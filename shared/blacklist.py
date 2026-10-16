"""Global blacklist (portal.db: global_blacklist). Blacklisted users are invisible everywhere:
web pages/APIs and the MCP server. Enforcement is central:

* apply(conn, kind): installs TEMP VIEWs that shadow the user-bearing tables of a read connection
  (unqualified names resolve temp -> main), so every existing query is filtered without edits.
  Only use on READ connections (writers must keep using plain connections).
* scrub(obj): recursively drops dicts that belong to a blacklisted user from JSON payloads.
"""
import os
import sqlite3
import threading
import time

_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PORTAL_DB = os.getenv("PORTAL_DB") or os.path.join(_ROOT, "data", "portal.db")
_lock = threading.Lock()
_cache = {"at": 0.0, "ids": frozenset()}
TTL = 10.0

DDL = """CREATE TABLE IF NOT EXISTS global_blacklist(
  user_id TEXT PRIMARY KEY, note TEXT, added_by TEXT, created_at TEXT)"""


def ensure(conn):
    conn.execute(DDL)


def ids(force=False):
    now = time.monotonic()
    if not force and now - _cache["at"] < TTL:
        return _cache["ids"]
    with _lock:
        try:
            c = sqlite3.connect(PORTAL_DB, timeout=10)
            try:
                ensure(c)
                s = frozenset(str(r[0]) for r in c.execute("SELECT user_id FROM global_blacklist"))
            finally:
                c.close()
        except Exception:
            s = _cache["ids"]
        _cache.update(at=now, ids=s)
        return s


def invalidate():
    _cache["at"] = 0.0


def list_all(conn):
    ensure(conn)
    return [dict(user_id=r[0], note=r[1], added_by=r[2], created_at=r[3]) for r in
            conn.execute("SELECT user_id,note,added_by,created_at FROM global_blacklist ORDER BY created_at")]


def add(conn, uid, note="", by=""):
    ensure(conn)
    conn.execute("INSERT OR REPLACE INTO global_blacklist(user_id,note,added_by,created_at) VALUES(?,?,?,datetime('now'))",
                 (str(uid), note or "", str(by or "")))
    conn.commit(); invalidate()


def remove(conn, uid):
    ensure(conn)
    conn.execute("DELETE FROM global_blacklist WHERE user_id=?", (str(uid),))
    conn.commit(); invalidate()


def is_blocked(uid):
    return str(uid) in ids()


def _lit(s):
    return "(" + ",".join("'%s'" % i for i in sorted(s) if str(i).isdigit()) + ")"


_VIEWS = {
    "main": {
        "users": "SELECT * FROM main.users WHERE user_id NOT IN {L}",
        "messages": "SELECT rowid AS rowid, * FROM main.messages WHERE author_id NOT IN {L}",
        "reactions": ("SELECT rowid AS rowid, * FROM main.reactions WHERE user_id NOT IN {L} AND message_id NOT IN "
                      "(SELECT message_id FROM main.messages WHERE author_id IN {L})"),
        "mentions": "SELECT * FROM main.mentions WHERE author_id NOT IN {L} AND mentioned_user_id NOT IN {L}",
        "attachments": ("SELECT * FROM main.attachments WHERE message_id NOT IN "
                        "(SELECT message_id FROM main.messages WHERE author_id IN {L})"),
        "user_stats": "SELECT * FROM main.user_stats WHERE user_id NOT IN {L}",
    },
    "summary": {
        "user_stats": "SELECT * FROM main.user_stats WHERE user_id NOT IN {L}",
        "user_day": "SELECT * FROM main.user_day WHERE user_id NOT IN {L}",
        "user_hour": "SELECT * FROM main.user_hour WHERE user_id NOT IN {L}",
        "user_emoji_recv": "SELECT * FROM main.user_emoji_recv WHERE user_id NOT IN {L}",
        "user_emoji_given": "SELECT * FROM main.user_emoji_given WHERE user_id NOT IN {L}",
    },
    "insights": {
        "member_circle": "SELECT * FROM main.member_circle WHERE user_id NOT IN {L}",
        "member_year": "SELECT * FROM main.member_year WHERE user_id NOT IN {L}",
        "dossiers": "SELECT * FROM main.dossiers WHERE user_id NOT IN {L}",
    },
    "sentiment": {
        "msg_sentiment": "SELECT * FROM main.msg_sentiment WHERE author_id NOT IN {L}",
        "daily_user": "SELECT * FROM main.daily_user WHERE author_id NOT IN {L}",
    },
}

KIND_BY_FILE = {"discord_data.db": "main", "summary.db": "summary", "insights.db": "insights", "sentiment.db": "sentiment"}


def kind_for_path(path):
    return KIND_BY_FILE.get(os.path.basename(str(path or "")))


def apply(conn, kind):
    """Shadow user-bearing tables with filtered TEMP VIEWs. No-op when the blacklist is empty."""
    if kind not in _VIEWS:
        return conn
    s = ids()
    if not s:
        return conn
    L = _lit(s)
    try:
        have = {r[0] for r in conn.execute("SELECT name FROM main.sqlite_master WHERE type='table'")}
        for name, sql in _VIEWS[kind].items():
            if name in have:
                conn.execute(f"DROP VIEW IF EXISTS temp.{name}")
                conn.execute(f"CREATE TEMP VIEW {name} AS " + sql.replace("{L}", L))
    except sqlite3.Error:
        pass
    return conn


_ID_KEYS = ("user_id", "author_id", "owner_id", "requester_id", "target_id", "op_author", "first_author_id", "id")


def scrub(obj, s=None):
    """Remove blacklisted users from a JSON-like structure."""
    s = ids() if s is None else s
    if not s:
        return obj
    return _scrub(obj, s)


def _hit(d, s):
    for k in ("user_id", "author_id", "owner_id", "requester_id", "target_id", "op_author"):
        v = d.get(k)
        if v is not None and str(v) in s:
            return True
    # graph nodes ({"id": uid, "name": ...}) and links ({"source"/"target"} or {"s"/"t"})
    if "id" in d and ("name" in d or "w" in d) and str(d.get("id")) in s:
        return True
    for a, b in (("source", "target"), ("s", "t"), ("from", "to")):
        if a in d and b in d and (str(d[a]) in s or str(d[b]) in s):
            return True
    return False


def _scrub(o, s):
    if isinstance(o, list):
        return [_scrub(x, s) for x in o if not (isinstance(x, dict) and _hit(x, s))
                and not (isinstance(x, (str, int)) and str(x) in s)]
    if isinstance(o, dict):
        if o.get("first_author_id") is not None and str(o["first_author_id"]) in s:
            o = dict(o, first_author_id=None, first_author=None, first_text=None)
        return {k: (None if isinstance(v, dict) and _hit(v, s) else _scrub(v, s)) for k, v in o.items()}
    return o
