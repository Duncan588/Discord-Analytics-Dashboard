"""Full-text search index for the AI API (FTS5, separate file: data/servers/<gid>/search.db).

Contentless FTS5 table: stores only the inverted index (no copy of message text);
hits are resolved back to discord_data.db by rowid. Chinese text is pre-segmented
with jieba (cut_for_search) so the compact unicode61 tokenizer works for zh + en.
Incremental by messages.rowid watermark; rebuilds when summary_dirty.gen changes.

CLI: python -m dixo.search data/servers/<gid>/discord_data.db
"""
import os
import re
import sqlite3
import sys
import time

from . import summary, textutil

_WORD_RE = re.compile(r"[\u4e00-\u9fff]+|[A-Za-z0-9_]+")


def search_path(main_db_path):
    return os.path.join(os.path.dirname(main_db_path), "search.db")


def segment(text):
    out = []
    for run in _WORD_RE.findall(text or ""):
        if "\u4e00" <= run[0] <= "\u9fff":
            jb = textutil._get_jieba()
            out.extend(t for t in jb.cut_for_search(run) if t.strip())
        else:
            out.append(run.lower())
    return " ".join(out)


def _ensure(conn):
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT);
    CREATE VIRTUAL TABLE IF NOT EXISTS msg_fts USING fts5(body, content='', tokenize='unicode61');
    """)


def _meta(conn, key, default=None):
    r = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    return r[0] if r else default


def _set(conn, key, value):
    conn.execute("INSERT OR REPLACE INTO meta(key,value) VALUES(?,?)", (key, str(value)))


def refresh(main_db_path, budget_seconds=20.0, batch=20000):
    spath = search_path(main_db_path)
    s = sqlite3.connect(spath, timeout=30)
    s.execute("PRAGMA journal_mode=WAL")
    _ensure(s)
    m = sqlite3.connect(main_db_path, timeout=60)
    started = time.monotonic()
    try:
        summary.ensure_dirty_trigger(m, os.path.abspath(main_db_path))
        gen = str(summary.delete_generation(m))
        if _meta(s, "delete_gen") != gen:
            with s:
                s.execute("DROP TABLE IF EXISTS msg_fts")
                _ensure(s)
                _set(s, "wm", 0); _set(s, "delete_gen", gen)
        wm = int(_meta(s, "wm", 0))
        while True:
            rows = m.execute("SELECT rowid, content FROM messages WHERE rowid>? ORDER BY rowid LIMIT ?", (wm, batch)).fetchall()
            if not rows:
                break
            with s:
                s.executemany("INSERT INTO msg_fts(rowid, body) VALUES(?,?)",
                              [(r[0], segment(r[1])) for r in rows if r[1]])
                wm = rows[-1][0]; _set(s, "wm", wm)
            if budget_seconds and time.monotonic() - started > budget_seconds:
                break
        max_m = m.execute("SELECT MAX(rowid) FROM messages").fetchone()[0] or 0
        return {"wm": wm, "max": max_m, "caught_up": wm >= max_m}
    finally:
        s.close(); m.close()


def query_to_match(q):
    toks = [t for t in segment(q).split() if t]
    if not toks:
        return None
    return " AND ".join('"%s"' % t.replace('"', '""') for t in toks[:12])


def search(main_conn, main_db_path, q, author_ids=None, limit=50, offset=0):
    match = query_to_match(q)
    path = search_path(main_db_path)
    if not match or not os.path.exists(path):
        return []
    s = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=10)
    try:
        # over-fetch when filtering by author (filter applied on main DB)
        want = (limit + offset) * (5 if author_ids else 1)
        rowids = [r[0] for r in s.execute("SELECT rowid FROM msg_fts WHERE msg_fts MATCH ? ORDER BY rank LIMIT ?", (match, want))]
    finally:
        s.close()
    if not rowids:
        return []
    ph = ",".join("?" for _ in rowids)
    sql = f"SELECT rowid,message_id,thread_id,author_id,content,timestamp FROM messages WHERE rowid IN ({ph})"
    args = list(rowids)
    if author_ids:
        sql += f" AND author_id IN ({','.join('?' for _ in author_ids)})"; args += list(author_ids)
    rows = {r["rowid"]: dict(r) for r in main_conn.execute(sql, args)}
    ordered = [rows[r] for r in rowids if r in rows]
    for d in ordered:
        d.pop("rowid", None)
    return ordered[offset:offset + limit]


if __name__ == "__main__":
    import json
    while True:
        res = refresh(sys.argv[1], budget_seconds=60)
        print(json.dumps(res))
        if res["caught_up"]:
            break
