"""Visitor tracking moved out of discord_data.db (ROADMAP S2 / KNOWN_ISSUES K28).

Writes to web_visitors/profile_views used to bump the 5 GB main DB's mtime on every
page view, invalidating caches. They now live in data/servers/<gid>/visitors.db.
On first use the old rows are COPIED (INSERT OR IGNORE); the old tables in the main
DB are left untouched (hard rule: never delete data).
"""
import os
import sqlite3
import threading

DDL = """
CREATE TABLE IF NOT EXISTS web_visitors(user_id TEXT PRIMARY KEY, username TEXT, nickname TEXT, avatar_url TEXT, last_visit DATETIME);
CREATE TABLE IF NOT EXISTS profile_views(id INTEGER PRIMARY KEY AUTOINCREMENT, target_user_id TEXT, viewer_user_id TEXT,
    viewer_name TEXT, viewer_avatar TEXT, timestamp DATETIME, UNIQUE(target_user_id, viewer_user_id));
CREATE INDEX IF NOT EXISTS idx_pv_target_time ON profile_views(target_user_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_wv_last ON web_visitors(last_visit DESC);
CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT);
"""
_migrated = set()
_lock = threading.Lock()


def visitors_path(main_db_path):
    return os.path.join(os.path.dirname(main_db_path), "visitors.db")


def connect(main_db_path, readonly=False):
    path = visitors_path(main_db_path)
    if readonly:
        if not os.path.exists(path):
            conn = sqlite3.connect(":memory:")
            conn.executescript(DDL)
        else:
            conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=10)
        conn.row_factory = sqlite3.Row
        return conn
    conn = sqlite3.connect(path, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(DDL)
    _migrate_once(conn, main_db_path)
    return conn


def _migrate_once(conn, main_db_path):
    if main_db_path in _migrated:
        return
    with _lock:
        if main_db_path in _migrated:
            return
        done = conn.execute("SELECT value FROM meta WHERE key='migrated_from_main'").fetchone()
        if not done and os.path.exists(main_db_path):
            src = sqlite3.connect(f"file:{main_db_path}?mode=ro", uri=True, timeout=30)
            try:
                for table, cols in (("web_visitors", "user_id,username,nickname,avatar_url,last_visit"),
                                    ("profile_views", "target_user_id,viewer_user_id,viewer_name,viewer_avatar,timestamp")):
                    try:
                        rows = src.execute(f"SELECT {cols} FROM {table}").fetchall()
                    except sqlite3.OperationalError:
                        rows = []
                    ph = ",".join("?" for _ in cols.split(","))
                    conn.executemany(f"INSERT OR IGNORE INTO {table}({cols}) VALUES({ph})", rows)
            finally:
                src.close()
            conn.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('migrated_from_main','1')")
            conn.commit()
        _migrated.add(main_db_path)


def record_site_visit(conn, u, now):
    conn.execute(
        "INSERT INTO web_visitors(user_id,username,nickname,avatar_url,last_visit) VALUES(?,?,?,?,?) "
        "ON CONFLICT(user_id) DO UPDATE SET username=excluded.username,nickname=excluded.nickname,"
        "avatar_url=excluded.avatar_url,last_visit=excluded.last_visit",
        (u["id"], u["username"], u.get("nickname") or u["username"], u.get("avatar"), now))
    conn.commit()


def recent_site_visitors(conn, limit=200):
    return conn.execute("SELECT * FROM web_visitors ORDER BY last_visit DESC LIMIT ?", (limit,)).fetchall()


def record_profile_view(conn, target, visitor, now):
    conn.execute(
        "INSERT INTO profile_views(target_user_id,viewer_user_id,viewer_name,viewer_avatar,timestamp) VALUES(?,?,?,?,?) "
        "ON CONFLICT(target_user_id,viewer_user_id) DO UPDATE SET timestamp=excluded.timestamp",
        (target, visitor["id"], visitor["username"], visitor.get("avatar"), now))
    conn.commit()


def profile_view_stats(conn, target):
    count = conn.execute("SELECT count(*) FROM profile_views WHERE target_user_id=?", (target,)).fetchone()[0]
    recent = conn.execute("SELECT viewer_name,viewer_avatar,timestamp FROM profile_views WHERE target_user_id=? "
                          "ORDER BY timestamp DESC LIMIT 20", (target,)).fetchall()
    return count, recent
