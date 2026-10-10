"""Incremental summary tables (replace the homepage/profile pickle caches).

Layout: data/servers/<gid>/summary.db  (derived data only; safe to delete -> full rebuild)
The main discord_data.db is only READ, except for one tiny, additive change:
  table summary_dirty(gen) + AFTER DELETE triggers on messages/reactions that bump
  `gen`; consumers (summary.db, search.db) rebuild when the generation they saw changes.
messages/reactions are append-only (INSERT OR IGNORE / INSERT), so rowid is a
monotonic watermark: each refresh only reads rows with rowid > watermark.

CLI (pre-build before deploying, resumable):
    python -m dixo.summary data/servers/<gid>/discord_data.db [--budget 0]
"""
import collections
import json
import logging
import os
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone

from . import textutil

log = logging.getLogger("dixo.summary")
SCHEMA_VERSION = 1
LOCAL_TZ = timezone(timedelta(hours=8))  # dashboard shows Asia/Shanghai (same +8 as before)

SUMMARY_DDL = """
CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS msg_hour(day TEXT NOT NULL, hour INTEGER NOT NULL, c INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(day,hour)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS user_stats(user_id TEXT PRIMARY KEY, msg_count INTEGER NOT NULL DEFAULT 0,
    react_recv INTEGER NOT NULL DEFAULT 0, react_given INTEGER NOT NULL DEFAULT 0, first_ts TEXT, last_ts TEXT) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS idx_us_msg ON user_stats(msg_count DESC);
CREATE TABLE IF NOT EXISTS user_day(user_id TEXT NOT NULL, day TEXT NOT NULL, c INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(user_id,day)) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS idx_ud_day ON user_day(day);
CREATE TABLE IF NOT EXISTS user_hour(user_id TEXT NOT NULL, hour INTEGER NOT NULL, c INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(user_id,hour)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS thread_stats(thread_id TEXT PRIMARY KEY, msg_count INTEGER NOT NULL DEFAULT 0,
    op_msg_id TEXT, op_author TEXT, first_ts TEXT, last_ts TEXT) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS idx_ts_count ON thread_stats(msg_count DESC);
CREATE INDEX IF NOT EXISTS idx_ts_op ON thread_stats(op_author, first_ts DESC);
CREATE TABLE IF NOT EXISTS msg_react(message_id TEXT PRIMARY KEY, c INTEGER NOT NULL DEFAULT 0) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS idx_mr_c ON msg_react(c DESC);
CREATE TABLE IF NOT EXISTS user_emoji_recv(user_id TEXT NOT NULL, emoji_name TEXT NOT NULL, emoji_url TEXT, c INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(user_id,emoji_name)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS user_emoji_given(user_id TEXT NOT NULL, emoji_name TEXT NOT NULL, emoji_url TEXT, c INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(user_id,emoji_name)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS word(word TEXT PRIMARY KEY, c INTEGER NOT NULL DEFAULT 0) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS idx_word_c ON word(c DESC);
CREATE TABLE IF NOT EXISTS profile_extra(user_id TEXT PRIMARY KEY, wm INTEGER, built_at REAL, data TEXT) WITHOUT ROWID;
"""
DATA_TABLES = ("msg_hour", "user_stats", "user_day", "user_hour", "thread_stats", "msg_react",
               "user_emoji_recv", "user_emoji_given", "word", "profile_extra")

DIRTY_DDL = """
CREATE TABLE IF NOT EXISTS summary_dirty(id INTEGER PRIMARY KEY CHECK(id=1), gen INTEGER NOT NULL DEFAULT 0);
INSERT OR IGNORE INTO summary_dirty(id,gen) VALUES(1,0);
CREATE TRIGGER IF NOT EXISTS trg_summary_dirty_msg AFTER DELETE ON messages
  BEGIN UPDATE summary_dirty SET gen=gen+1 WHERE id=1; END;
CREATE TRIGGER IF NOT EXISTS trg_summary_dirty_react AFTER DELETE ON reactions
  BEGIN UPDATE summary_dirty SET gen=gen+1 WHERE id=1; END;
"""


def summary_path(main_db_path):
    return os.path.join(os.path.dirname(main_db_path), "summary.db")


def _parse_ts(value):
    if not value:
        return None
    try:
        s = str(value).replace("Z", "+00:00").replace(" ", "T", 1)
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)  # stored timestamps are UTC (K23)
        return dt.astimezone(LOCAL_TZ)
    except (TypeError, ValueError):
        return None


def connect_summary(path, readonly=False):
    if readonly:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=30, check_same_thread=False)
    else:
        conn = sqlite3.connect(path, timeout=30, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.executescript(SUMMARY_DDL)
    conn.row_factory = sqlite3.Row
    return conn


def _meta_get(conn, key, default=None, schema="s"):
    row = conn.execute(f"SELECT value FROM {schema}.meta WHERE key=?", (key,)).fetchone()
    return row[0] if row else default


def _meta_set(conn, key, value, schema="s"):
    conn.execute(f"INSERT INTO {schema}.meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                 (key, str(value)))


def _upsert_add(conn, table, keys, values_cols, rows):
    if not rows:
        return
    cols = keys + values_cols
    ph = ",".join("?" for _ in cols)
    upd = ",".join(f"{c}={c}+excluded.{c}" for c in values_cols)
    conn.executemany(
        f"INSERT INTO s.{table}({','.join(cols)}) VALUES({ph}) ON CONFLICT({','.join(keys)}) DO UPDATE SET {upd}", rows)


def _reset(conn):
    for t in DATA_TABLES:
        conn.execute(f"DELETE FROM s.{t}")
    _meta_set(conn, "msg_wm", 0)
    _meta_set(conn, "react_wm", 0)
    _meta_set(conn, "schema_version", SCHEMA_VERSION)
    _meta_set(conn, "stopwords_sig", _stopwords_sig())


def _stopwords_sig():
    return f"{len(textutil.ZH_STOP)}:{len(textutil.EN_STOP)}:v1"


def _process_messages(conn, rows):
    hour = collections.Counter(); uday = collections.Counter(); uhour = collections.Counter()
    ustat = {}; tstat = {}; words = collections.Counter()
    extra = textutil.extra_stopwords()
    for r in rows:
        author = r["author_id"]; tid = r["thread_id"]; ts_raw = r["timestamp"] or ""
        dt = _parse_ts(ts_raw)
        if dt is not None:
            day = dt.strftime("%Y-%m-%d"); h = dt.hour
            hour[(day, h)] += 1
            if author:
                uday[(author, day)] += 1; uhour[(author, h)] += 1
        if author:
            s = ustat.setdefault(author, [0, None, None])
            s[0] += 1
            if ts_raw and (s[1] is None or ts_raw < s[1]): s[1] = ts_raw
            if ts_raw and (s[2] is None or ts_raw > s[2]): s[2] = ts_raw
        if tid:
            t = tstat.setdefault(tid, [0, None, None, None, None])
            t[0] += 1
            key = (ts_raw or "\uffff", r["message_id"])
            if t[3] is None or key < (t[3] or "\uffff", t[1]):
                t[1], t[2], t[3] = r["message_id"], author, ts_raw
            if ts_raw and (t[4] is None or ts_raw > t[4]): t[4] = ts_raw
        if r["content"]:
            words.update(textutil.tokenize(r["content"], extra))
    _upsert_add(conn, "msg_hour", ["day", "hour"], ["c"], [(d, h, c) for (d, h), c in hour.items()])
    _upsert_add(conn, "user_day", ["user_id", "day"], ["c"], [(u, d, c) for (u, d), c in uday.items()])
    _upsert_add(conn, "user_hour", ["user_id", "hour"], ["c"], [(u, h, c) for (u, h), c in uhour.items()])
    _upsert_add(conn, "word", ["word"], ["c"], list(words.items()))
    conn.executemany(
        "INSERT INTO s.user_stats(user_id,msg_count,first_ts,last_ts) VALUES(?,?,?,?) ON CONFLICT(user_id) DO UPDATE SET "
        "msg_count=msg_count+excluded.msg_count,"
        "first_ts=CASE WHEN first_ts IS NULL OR (excluded.first_ts IS NOT NULL AND excluded.first_ts<first_ts) THEN excluded.first_ts ELSE first_ts END,"
        "last_ts=CASE WHEN last_ts IS NULL OR (excluded.last_ts IS NOT NULL AND excluded.last_ts>last_ts) THEN excluded.last_ts ELSE last_ts END",
        [(u, v[0], v[1], v[2]) for u, v in ustat.items()])
    conn.executemany(
        "INSERT INTO s.thread_stats(thread_id,msg_count,op_msg_id,op_author,first_ts,last_ts) VALUES(?,?,?,?,?,?) "
        "ON CONFLICT(thread_id) DO UPDATE SET msg_count=msg_count+excluded.msg_count,"
        "op_msg_id=CASE WHEN first_ts IS NULL OR excluded.first_ts<first_ts THEN excluded.op_msg_id ELSE op_msg_id END,"
        "op_author=CASE WHEN first_ts IS NULL OR excluded.first_ts<first_ts THEN excluded.op_author ELSE op_author END,"
        "first_ts=CASE WHEN first_ts IS NULL OR excluded.first_ts<first_ts THEN excluded.first_ts ELSE first_ts END,"
        "last_ts=CASE WHEN last_ts IS NULL OR excluded.last_ts>last_ts THEN excluded.last_ts ELSE last_ts END",
        [(t, v[0], v[1], v[2], v[3], v[4]) for t, v in tstat.items()])


def _process_reactions(conn, rows):
    mr = collections.Counter(); recv = collections.Counter(); given = collections.Counter()
    erecv = collections.Counter(); egiven = collections.Counter(); urls = {}
    for r in rows:
        name = r["emoji_name"] or "?"
        urls[name] = r["emoji_url"]
        mr[r["message_id"]] += 1
        if r["author_id"]:
            recv[r["author_id"]] += 1; erecv[(r["author_id"], name)] += 1
        if r["user_id"]:
            given[r["user_id"]] += 1; egiven[(r["user_id"], name)] += 1
    _upsert_add(conn, "msg_react", ["message_id"], ["c"], list(mr.items()))
    _upsert_add(conn, "user_stats", ["user_id"], ["react_recv"], list(recv.items()))
    _upsert_add(conn, "user_stats", ["user_id"], ["react_given"], list(given.items()))
    conn.executemany(
        "INSERT INTO s.user_emoji_recv(user_id,emoji_name,emoji_url,c) VALUES(?,?,?,?) ON CONFLICT(user_id,emoji_name) "
        "DO UPDATE SET c=c+excluded.c, emoji_url=COALESCE(excluded.emoji_url,emoji_url)",
        [(u, n, urls.get(n), c) for (u, n), c in erecv.items()])
    conn.executemany(
        "INSERT INTO s.user_emoji_given(user_id,emoji_name,emoji_url,c) VALUES(?,?,?,?) ON CONFLICT(user_id,emoji_name) "
        "DO UPDATE SET c=c+excluded.c, emoji_url=COALESCE(excluded.emoji_url,emoji_url)",
        [(u, n, urls.get(n), c) for (u, n), c in egiven.items()])


_TRIGGER_READY = set()


def ensure_dirty_trigger(main_conn, key=None):
    """Create summary_dirty + triggers once per process per DB (avoids a write txn on every refresh)."""
    if key is not None and key in _TRIGGER_READY:
        return
    main_conn.executescript(DIRTY_DDL)
    if key is not None:
        _TRIGGER_READY.add(key)


def delete_generation(main_conn):
    row = main_conn.execute("SELECT gen FROM summary_dirty WHERE id=1").fetchone()
    return row[0] if row else 0


def refresh(main_db_path, budget_seconds=20.0, batch=20000):
    """Advance summary.db towards the main DB. Returns dict(caught_up, msg_wm, react_wm, processed)."""
    started = time.monotonic()
    spath = summary_path(main_db_path)
    connect_summary(spath).close()  # create schema / WAL
    conn = sqlite3.connect(main_db_path, timeout=60, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    processed = 0
    try:
        conn.execute("PRAGMA busy_timeout=60000")
        ensure_dirty_trigger(conn, os.path.abspath(main_db_path))
        conn.execute("ATTACH DATABASE ? AS s", (spath,))
        gen = str(delete_generation(conn))
        if _meta_get(conn, "delete_gen") != gen or _meta_get(conn, "schema_version") != str(SCHEMA_VERSION) \
                or _meta_get(conn, "stopwords_sig") != _stopwords_sig():
            log.warning("summary full rebuild path=%s gen=%s", main_db_path, gen)
            with conn:
                _reset(conn)
                _meta_set(conn, "delete_gen", gen)
        msg_wm = int(_meta_get(conn, "msg_wm", 0)); react_wm = int(_meta_get(conn, "react_wm", 0))
        while True:
            rows = conn.execute(
                "SELECT rowid,message_id,thread_id,author_id,content,timestamp FROM messages WHERE rowid>? ORDER BY rowid LIMIT ?",
                (msg_wm, batch)).fetchall()
            if not rows:
                break
            with conn:
                _process_messages(conn, rows)
                msg_wm = rows[-1]["rowid"]
                _meta_set(conn, "msg_wm", msg_wm)
            processed += len(rows)
            if budget_seconds and time.monotonic() - started > budget_seconds:
                break
        while not (budget_seconds and time.monotonic() - started > budget_seconds):
            rows = conn.execute(
                "SELECT r.rowid rid,r.message_id,r.user_id,r.emoji_name,r.emoji_url,m.author_id FROM reactions r "
                "LEFT JOIN messages m ON m.message_id=r.message_id WHERE r.rowid>? ORDER BY r.rowid LIMIT ?",
                (react_wm, batch * 2)).fetchall()
            if not rows:
                break
            with conn:
                _process_reactions(conn, rows)
                react_wm = rows[-1]["rid"]
                _meta_set(conn, "react_wm", react_wm)
            processed += len(rows)
        max_m = conn.execute("SELECT MAX(rowid) FROM messages").fetchone()[0] or 0
        max_r = conn.execute("SELECT MAX(rowid) FROM reactions").fetchone()[0] or 0
        caught_up = msg_wm >= max_m and react_wm >= max_r
        with conn:
            _meta_set(conn, "refreshed_at", time.time())
            _meta_set(conn, "caught_up", int(caught_up))
        return {"caught_up": caught_up, "msg_wm": msg_wm, "react_wm": react_wm, "processed": processed,
                "max_msg": max_m, "max_react": max_r}
    finally:
        conn.close()


def state(summary_conn):
    rows = dict(summary_conn.execute("SELECT key,value FROM meta").fetchall())
    return {"msg_wm": int(rows.get("msg_wm", 0) or 0), "react_wm": int(rows.get("react_wm", 0) or 0),
            "caught_up": rows.get("caught_up") == "1", "refreshed_at": float(rows.get("refreshed_at", 0) or 0)}


def _main(argv):
    import argparse
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("db"); ap.add_argument("--budget", type=float, default=0, help="seconds, 0 = until caught up")
    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO)
    while True:
        res = refresh(a.db, budget_seconds=a.budget or 60)
        print(json.dumps(res))
        if res["caught_up"] or a.budget:
            return 0


if __name__ == "__main__":
    sys.exit(_main(sys.argv[1:]))
