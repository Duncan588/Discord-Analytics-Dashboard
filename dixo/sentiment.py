"""Message sentiment store (Phase 3).

One SQLite file per guild next to the main DB: data/servers/<gid>/sentiment.db
  msg_sentiment(message_id, author_id, day, label, p_neg, p_pos)  label: -1 neg / 0 neu / 1 pos
  daily_guild(day, pos, neu, neg, score_sum)        score = p_pos - p_neg
  daily_user(author_id, day, pos, neu, neg, score_sum)
  meta(key, value)                                  model, max_id (largest scored snowflake)

Model: cardiffnlp/twitter-xlm-roberta-base-sentiment (labels neg/neu/pos).
Messages that were never scored (bots, empty, <2 chars, link-only) are simply absent = NA.

CLI (stdlib only, safe to run on prod):
  python -m dixo.sentiment import <scores.csv.gz> --guild <gid>   # columns id,author_id,day,label,p_neg,p_pos
  python -m dixo.sentiment rebuild --guild <gid>                  # recompute daily aggregates
  python -m dixo.sentiment status --guild <gid>
"""
import argparse
import csv
import gzip
import os
import re
import sqlite3
import sys
from datetime import date, datetime, timedelta, timezone

MODEL = "cardiffnlp/twitter-xlm-roberta-base-sentiment"
LABELS = {"neg": -1, "neu": 0, "pos": 1, "-1": -1, "0": 0, "1": 1}
DISCORD_EPOCH_MS = 1420070400000

SCHEMA = """
CREATE TABLE IF NOT EXISTS msg_sentiment(message_id TEXT PRIMARY KEY, author_id TEXT, day TEXT,
  label INTEGER NOT NULL, p_neg REAL, p_pos REAL) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS daily_guild(day TEXT PRIMARY KEY, pos INTEGER DEFAULT 0, neu INTEGER DEFAULT 0,
  neg INTEGER DEFAULT 0, score_sum REAL DEFAULT 0);
CREATE TABLE IF NOT EXISTS daily_user(author_id TEXT, day TEXT, pos INTEGER DEFAULT 0, neu INTEGER DEFAULT 0,
  neg INTEGER DEFAULT 0, score_sum REAL DEFAULT 0, PRIMARY KEY(author_id, day)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT);
"""

# Same cleaning as the historical export (tools/sentiment_prepare.py / Colab input).
_URL = re.compile(r"https?://\S+|www\.\S+")
_EMOJI = re.compile(r"<a?:(\w+):\d+>")
_MENTION = re.compile(r"<[@#][!&]?\d+>|@everyone|@here")
_WS = re.compile(r"\s+")
_ALNUM = re.compile(r"\w", re.UNICODE)


def clean_text(s):
    s = _URL.sub(" ", s or "")
    s = _EMOJI.sub(r"\1", s)
    s = _MENTION.sub(" ", s)
    s = _WS.sub(" ", s).strip()
    if len(s) < 2 or not _ALNUM.search(s):
        return None
    return s[:512]


def path_for(main_db_path):
    return os.path.join(os.path.dirname(main_db_path), "sentiment.db")


def connect(path, readonly=False):
    if readonly:
        if not os.path.exists(path):
            return None
        c = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5, check_same_thread=False)
    else:
        c = sqlite3.connect(path, timeout=30)
        c.execute("PRAGMA journal_mode=WAL")
        c.executescript(SCHEMA)
    c.row_factory = sqlite3.Row
    return c


def snowflake_day(mid):
    ms = (int(mid) >> 22) + DISCORD_EPOCH_MS
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


def _meta_set(c, k, v):
    c.execute("INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (k, str(v)))


def meta(c, k, default=None):
    r = c.execute("SELECT value FROM meta WHERE key=?", (k,)).fetchone()
    return r[0] if r else default


def _bump_max(c, ids):
    if ids:
        cur = int(meta(c, "max_id", "0") or 0)
        m = max(int(i) for i in ids)
        if m > cur:
            _meta_set(c, "max_id", m)


def insert_scores(c, rows, update_aggregates=True):
    """rows: iterable of (message_id, author_id, day, label(int), p_neg, p_pos).
    Already-scored ids are ignored (idempotent). Returns number inserted."""
    n, ids = 0, []
    for mid, aid, day, lab, pn, pp in rows:
        ids.append(mid)
        cur = c.execute("INSERT OR IGNORE INTO msg_sentiment VALUES(?,?,?,?,?,?)", (str(mid), str(aid), day, int(lab), pn, pp))
        if cur.rowcount and update_aggregates:
            col = {1: "pos", 0: "neu", -1: "neg"}[int(lab)]
            s = (pp or 0) - (pn or 0)
            c.execute(f"INSERT INTO daily_guild(day,{col},score_sum) VALUES(?,1,?) ON CONFLICT(day) DO UPDATE SET "
                      f"{col}={col}+1, score_sum=score_sum+excluded.score_sum", (day, s))
            c.execute(f"INSERT INTO daily_user(author_id,day,{col},score_sum) VALUES(?,?,1,?) ON CONFLICT(author_id,day) "
                      f"DO UPDATE SET {col}={col}+1, score_sum=score_sum+excluded.score_sum", (str(aid), day, s))
        n += cur.rowcount
    _bump_max(c, ids)
    return n


def rebuild(c):
    c.execute("DELETE FROM daily_guild")
    c.execute("DELETE FROM daily_user")
    agg = ("SUM(label=1),SUM(label=0),SUM(label=-1),SUM(COALESCE(p_pos,0)-COALESCE(p_neg,0))")
    c.execute(f"INSERT INTO daily_guild SELECT day,{agg} FROM msg_sentiment GROUP BY day")
    c.execute(f"INSERT INTO daily_user SELECT author_id,day,{agg} FROM msg_sentiment GROUP BY author_id,day")
    m = c.execute("SELECT MAX(CAST(message_id AS INTEGER)) FROM msg_sentiment").fetchone()[0]
    if m:
        _meta_set(c, "max_id", m)
    c.commit()


# ---------------------------------------------------------------- queries
def _bucket(day, gran):
    if gran == "week":
        d = date.fromisoformat(day)
        return (d - timedelta(days=d.weekday())).isoformat()
    if gran == "month":
        return day[:7]
    return day


def series(c, author_ids=None, since=None, until=None, granularity="day"):
    """[{day,pos,neu,neg,n,pos_pct,neu_pct,neg_pct,mean}] oldest first. mean = avg(p_pos-p_neg) in [-1,1]."""
    where, args = [], []
    if since:
        where.append("day>=?"); args.append(since)
    if until:
        where.append("day<=?"); args.append(until)
    if author_ids:
        ph = ",".join("?" for _ in author_ids)
        where.append(f"author_id IN ({ph})"); args.extend(author_ids)
        sql = (f"SELECT day,SUM(pos) pos,SUM(neu) neu,SUM(neg) neg,SUM(score_sum) s FROM daily_user "
               f"{'WHERE ' + ' AND '.join(where) if where else ''} GROUP BY day ORDER BY day")
    else:
        sql = f"SELECT day,pos,neu,neg,score_sum s FROM daily_guild {'WHERE ' + ' AND '.join(where) if where else ''} ORDER BY day"
    out = {}
    for r in c.execute(sql, args):
        k = _bucket(r["day"], granularity)
        o = out.setdefault(k, [0, 0, 0, 0.0])
        o[0] += r["pos"] or 0; o[1] += r["neu"] or 0; o[2] += r["neg"] or 0; o[3] += r["s"] or 0.0
    res = []
    for k in sorted(out):
        p, u, g, s = out[k]
        n = p + u + g
        if not n:
            continue
        res.append({"day": k, "pos": p, "neu": u, "neg": g, "n": n,
                    "pos_pct": round(100 * p / n, 1), "neu_pct": round(100 * u / n, 1), "neg_pct": round(100 * g / n, 1),
                    "mean": round(s / n, 4)})
    return res


def status(c):
    if c is None:
        return {"ready": False}
    n = c.execute("SELECT COUNT(*) FROM daily_guild").fetchone()[0]
    r = c.execute("SELECT MIN(day),MAX(day),SUM(pos+neu+neg) FROM daily_guild").fetchone()
    return {"ready": bool(n), "model": meta(c, "model", MODEL), "max_id": meta(c, "max_id"),
            "first_day": r[0], "last_day": r[1], "scored": r[2] or 0}


# ---------------------------------------------------------------- CLI
def _main_db(gid):
    base = os.environ.get("DIXO_DATA_DIR") or os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "servers")
    return os.path.join(base, str(gid), "discord_data.db")


def cli(argv=None):
    ap = argparse.ArgumentParser(prog="python -m dixo.sentiment")
    ap.add_argument("cmd", choices=["import", "rebuild", "status"])
    ap.add_argument("file", nargs="?")
    ap.add_argument("--guild", required=True)
    ap.add_argument("--db", help="override sentiment.db path")
    a = ap.parse_args(argv)
    path = a.db or path_for(_main_db(a.guild))
    c = connect(path)
    if a.cmd == "status":
        print(status(c)); return
    if a.cmd == "rebuild":
        rebuild(c); print(status(c)); return
    op = gzip.open if a.file.endswith(".gz") else open
    c.execute("PRAGMA synchronous=OFF")
    total, batch = 0, []
    with op(a.file, "rt", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            batch.append((row["id"], row["author_id"], row["day"], LABELS[row["label"]],
                          float(row["p_neg"]), float(row["p_pos"])))
            if len(batch) >= 50000:
                total += insert_scores(c, batch, update_aggregates=False); c.commit(); batch = []
                print("imported", total, flush=True)
    total += insert_scores(c, batch, update_aggregates=False)
    _meta_set(c, "model", MODEL)
    c.commit()
    rebuild(c)
    print("done", total, status(c))


if __name__ == "__main__":
    cli(sys.argv[1:])
