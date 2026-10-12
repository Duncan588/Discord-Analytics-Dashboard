"""Write path shared by the gateway listener and the backfill downloader.

Keeps discord_data.db's schema unchanged and the summary's rowid watermarks valid:

* New messages / reactions are plain INSERTs (append-only -> picked up by the
  incremental summary at the next refresh).
* Removals (message delete, reaction remove, whole-thread re-import) first write
  an ``ingest_tombstones`` row describing what is being removed, then DELETE.
  The ``summary_dirty`` triggers ignore deletes that have a matching tombstone,
  so they no longer force a full summary rebuild; instead the summary subtracts
  the tombstoned rows incrementally (dixo/summary.py).
* Deletes without a tombstone (manual SQL, old tools) still bump
  ``summary_dirty.gen`` -> full shadow rebuild, as before.
"""
import sqlite3
from datetime import datetime, timezone

DIRTY_DDL = """
CREATE TABLE IF NOT EXISTS summary_dirty(id INTEGER PRIMARY KEY CHECK(id=1), gen INTEGER NOT NULL DEFAULT 0);
INSERT OR IGNORE INTO summary_dirty(id,gen) VALUES(1,0);
CREATE TABLE IF NOT EXISTS ingest_tombstones(
  id INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL, src_rowid INTEGER NOT NULL,
  message_id TEXT, thread_id TEXT, author_id TEXT, content TEXT, timestamp TEXT,
  user_id TEXT, emoji_name TEXT, emoji_url TEXT, created_at TEXT);
CREATE INDEX IF NOT EXISTS idx_tomb_src ON ingest_tombstones(kind, src_rowid);
DROP TRIGGER IF EXISTS trg_summary_dirty_msg;
DROP TRIGGER IF EXISTS trg_summary_dirty_react;
CREATE TRIGGER IF NOT EXISTS trg_summary_dirty_msg2 AFTER DELETE ON messages
  WHEN NOT EXISTS (SELECT 1 FROM ingest_tombstones t WHERE t.kind='message' AND t.src_rowid=OLD.rowid AND t.message_id=OLD.message_id)
  BEGIN UPDATE summary_dirty SET gen=gen+1 WHERE id=1; END;
CREATE TRIGGER IF NOT EXISTS trg_summary_dirty_react2 AFTER DELETE ON reactions
  WHEN NOT EXISTS (SELECT 1 FROM ingest_tombstones t WHERE t.kind='reaction' AND t.src_rowid=OLD.rowid AND t.message_id=OLD.message_id)
  BEGIN UPDATE summary_dirty SET gen=gen+1 WHERE id=1; END;
"""

BASE_DDL = """
CREATE TABLE IF NOT EXISTS users(user_id TEXT PRIMARY KEY,username TEXT,nickname TEXT,avatar_url TEXT,is_bot BOOLEAN);
CREATE TABLE IF NOT EXISTS threads(thread_id TEXT PRIMARY KEY,category_id TEXT,name TEXT,exported_at TEXT,guild_id TEXT,last_active_at TEXT);
CREATE TABLE IF NOT EXISTS messages(message_id TEXT PRIMARY KEY,thread_id TEXT,author_id TEXT,content TEXT,timestamp DATETIME,reply_to_msg_id TEXT);
CREATE TABLE IF NOT EXISTS reactions(id INTEGER PRIMARY KEY AUTOINCREMENT,message_id TEXT,user_id TEXT,emoji_name TEXT,emoji_url TEXT);
CREATE TABLE IF NOT EXISTS attachments(id INTEGER PRIMARY KEY AUTOINCREMENT,message_id TEXT,url TEXT,filename TEXT,size_bytes INTEGER);
CREATE TABLE IF NOT EXISTS mentions(id INTEGER PRIMARY KEY AUTOINCREMENT,message_id TEXT,mentioned_user_id TEXT,author_id TEXT);
"""

_READY = set()


def ensure(conn, key=None):
    if key is not None and key in _READY:
        return
    conn.executescript(DIRTY_DDL)
    if key is not None:
        _READY.add(key)


def connect(path):
    conn = sqlite3.connect(str(path), timeout=120)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=120000")
    conn.executescript(BASE_DDL)
    ensure(conn, str(path))
    return conn


def iso_ms(dt):
    """Same shape as DiscordChatExporter timestamps: 2026-09-23T16:11:19.894+00:00"""
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}+00:00"


def _now():
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------- removals

def tombstone_delete_messages(conn, message_ids):
    """Tombstone + delete messages (and their reactions/attachments/mentions)."""
    ids = [str(i) for i in message_ids if i]
    if not ids:
        return 0
    now = _now()
    removed = 0
    for i in range(0, len(ids), 500):
        chunk = ids[i:i + 500]
        ph = ",".join("?" for _ in chunk)
        authors = {r["message_id"]: r["author_id"] for r in conn.execute(
            f"SELECT message_id,author_id FROM messages WHERE message_id IN ({ph})", chunk)}
        conn.executemany(
            "INSERT INTO ingest_tombstones(kind,src_rowid,message_id,user_id,emoji_name,emoji_url,author_id,created_at) "
            "VALUES('reaction',?,?,?,?,?,?,?)",
            [(r["id"], r["message_id"], r["user_id"], r["emoji_name"], r["emoji_url"], authors.get(r["message_id"]), now)
             for r in conn.execute(f"SELECT id,message_id,user_id,emoji_name,emoji_url FROM reactions WHERE message_id IN ({ph})", chunk)])
        conn.executemany(
            "INSERT INTO ingest_tombstones(kind,src_rowid,message_id,thread_id,author_id,content,timestamp,created_at) "
            "VALUES('message',?,?,?,?,?,?,?)",
            [(r["rowid"], r["message_id"], r["thread_id"], r["author_id"], r["content"], r["timestamp"], now)
             for r in conn.execute(f"SELECT rowid,message_id,thread_id,author_id,content,timestamp FROM messages WHERE message_id IN ({ph})", chunk)])
        for table in ("reactions", "attachments", "mentions"):
            conn.execute(f"DELETE FROM {table} WHERE message_id IN ({ph})", chunk)
        removed += conn.execute(f"DELETE FROM messages WHERE message_id IN ({ph})", chunk).rowcount
    return removed


def tombstone_delete_reactions(conn, where_sql, args):
    rows = conn.execute(
        f"SELECT r.id,r.message_id,r.user_id,r.emoji_name,r.emoji_url,m.author_id FROM reactions r "
        f"LEFT JOIN messages m ON m.message_id=r.message_id WHERE {where_sql}", args).fetchall()
    if not rows:
        return 0
    now = _now()
    conn.executemany(
        "INSERT INTO ingest_tombstones(kind,src_rowid,message_id,user_id,emoji_name,emoji_url,author_id,created_at) "
        "VALUES('reaction',?,?,?,?,?,?,?)",
        [(r["id"], r["message_id"], r["user_id"], r["emoji_name"], r["emoji_url"], r["author_id"], now) for r in rows])
    conn.executemany("DELETE FROM reactions WHERE id=?", [(r["id"],) for r in rows])
    return len(rows)


def remove_thread(conn, thread_id):
    """Whole-thread removal before a re-import (backfill). No full summary rebuild."""
    ids = [r[0] for r in conn.execute("SELECT message_id FROM messages WHERE thread_id=?", (str(thread_id),))]
    tombstone_delete_messages(conn, ids)
    conn.execute("DELETE FROM threads WHERE thread_id=?", (str(thread_id),))


# ---------------------------------------------------------------- gateway writes

def upsert_user(conn, user_id, username, nickname, avatar_url, is_bot):
    if not user_id:
        return
    conn.execute(
        "INSERT INTO users(user_id,username,nickname,avatar_url,is_bot) VALUES(?,?,?,?,?) "
        "ON CONFLICT(user_id) DO UPDATE SET username=excluded.username,"
        "nickname=COALESCE(NULLIF(excluded.nickname,''),nickname),avatar_url=COALESCE(NULLIF(excluded.avatar_url,''),avatar_url),"
        "is_bot=excluded.is_bot",
        (str(user_id), username or "", nickname or "", avatar_url or "", bool(is_bot)))


def touch_thread(conn, thread_id, guild_id, parent_id, name, last_active_at):
    conn.execute(
        "INSERT OR IGNORE INTO threads(thread_id,category_id,name,exported_at,guild_id,last_active_at) VALUES(?,?,?,?,?,?)",
        (str(thread_id), str(parent_id or ""), name or "", _now(), str(guild_id or ""), last_active_at))
    conn.execute(
        "UPDATE threads SET name=COALESCE(NULLIF(?,''),name), "
        "last_active_at=CASE WHEN last_active_at IS NULL OR ?>last_active_at THEN ? ELSE last_active_at END WHERE thread_id=?",
        (name or "", last_active_at, last_active_at, str(thread_id)))


def insert_message(conn, msg):
    """msg: dict(message_id, thread_id, author_id, content, timestamp, reply_to, attachments[(url,name,size)], mentions[ids])."""
    cur = conn.execute(
        "INSERT OR IGNORE INTO messages(message_id,thread_id,author_id,content,timestamp,reply_to_msg_id) VALUES(?,?,?,?,?,?)",
        (str(msg["message_id"]), str(msg["thread_id"]), str(msg["author_id"] or ""), msg.get("content") or "",
         msg.get("timestamp"), msg.get("reply_to")))
    if cur.rowcount == 0:
        return False
    for url, name, size in msg.get("attachments") or []:
        conn.execute("INSERT INTO attachments(message_id,url,filename,size_bytes) VALUES(?,?,?,?)",
                     (str(msg["message_id"]), url, name, size))
    for mid in msg.get("mentions") or []:
        conn.execute("INSERT INTO mentions(message_id,mentioned_user_id,author_id) VALUES(?,?,?)",
                     (str(msg["message_id"]), str(mid), str(msg["author_id"] or "")))
    return True


def edit_message(conn, message_id, content):
    return conn.execute("UPDATE messages SET content=? WHERE message_id=?", (content or "", str(message_id))).rowcount


def emoji_url_for(conn, name, custom_url=None):
    if custom_url:
        return custom_url
    r = conn.execute("SELECT emoji_url FROM reactions WHERE emoji_name=? AND emoji_url IS NOT NULL LIMIT 1", (name,)).fetchone()
    return r[0] if r else None


def add_reaction(conn, message_id, user_id, emoji_name, emoji_url):
    exists = conn.execute("SELECT 1 FROM reactions WHERE message_id=? AND user_id=? AND emoji_name=? LIMIT 1",
                          (str(message_id), str(user_id), emoji_name)).fetchone()
    if exists:
        return False
    conn.execute("INSERT INTO reactions(message_id,user_id,emoji_name,emoji_url) VALUES(?,?,?,?)",
                 (str(message_id), str(user_id), emoji_name, emoji_url))
    return True


def remove_reaction(conn, message_id, user_id, emoji_name):
    return tombstone_delete_reactions(conn, "r.message_id=? AND r.user_id=? AND r.emoji_name=?",
                                      (str(message_id), str(user_id), emoji_name))


def clear_reactions(conn, message_id, emoji_name=None):
    if emoji_name is None:
        return tombstone_delete_reactions(conn, "r.message_id=?", (str(message_id),))
    return tombstone_delete_reactions(conn, "r.message_id=? AND r.emoji_name=?", (str(message_id), emoji_name))
