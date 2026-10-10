"""Portal-backed block list + thread favorites (shared with whitelist_bot.py).

Tables live in portal.db (same schema the Discord bot already uses):
  user_blocks(blocker_id, blocked_id, created_at) PK(blocker, blocked)
  favorites(user_id, guild_id, thread_id, created_at) PK(user_id, thread_id)
  block_interception_counts(...)  — written only by the bot; read by the UI

Web UI may create/delete rows; interception counts stay bot-owned.
"""
from . import summary as _summary  # for utc formatting if needed


DDL = """
CREATE TABLE IF NOT EXISTS user_blocks (
    blocker_id TEXT NOT NULL,
    blocked_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (blocker_id, blocked_id)
);
CREATE INDEX IF NOT EXISTS idx_user_blocks_blocker ON user_blocks(blocker_id);
CREATE TABLE IF NOT EXISTS favorites (
    user_id TEXT NOT NULL,
    guild_id TEXT NOT NULL,
    thread_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (user_id, thread_id)
);
CREATE INDEX IF NOT EXISTS idx_favorites_user_created ON favorites(user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_favorites_guild_thread ON favorites(guild_id, thread_id);
CREATE TABLE IF NOT EXISTS block_interception_counts (
    guild_id TEXT NOT NULL,
    sender_id TEXT NOT NULL,
    target_id TEXT NOT NULL,
    count INTEGER NOT NULL DEFAULT 0,
    last_blocked_at TEXT NOT NULL,
    PRIMARY KEY (guild_id, sender_id, target_id)
);
"""


def ensure_tables(portal_conn):
    portal_conn.executescript(DDL)
    portal_conn.commit()


def block_user(portal, blocker_id, blocked_id, now):
    blocker_id, blocked_id = str(blocker_id), str(blocked_id)
    if blocker_id == blocked_id:
        return False, "cannot_block_self"
    ensure_tables(portal)
    portal.execute(
        "INSERT OR IGNORE INTO user_blocks(blocker_id,blocked_id,created_at) VALUES(?,?,?)",
        (blocker_id, blocked_id, now),
    )
    portal.commit()
    return True, "blocked"


def unblock_user(portal, blocker_id, blocked_id):
    ensure_tables(portal)
    cur = portal.execute(
        "DELETE FROM user_blocks WHERE blocker_id=? AND blocked_id=?",
        (str(blocker_id), str(blocked_id)),
    )
    portal.commit()
    return cur.rowcount > 0


def is_blocked(portal, blocker_id, blocked_id):
    ensure_tables(portal)
    return bool(portal.execute(
        "SELECT 1 FROM user_blocks WHERE blocker_id=? AND blocked_id=?",
        (str(blocker_id), str(blocked_id)),
    ).fetchone())


def add_favorite(portal, user_id, guild_id, thread_id, now):
    ensure_tables(portal)
    portal.execute(
        "INSERT OR IGNORE INTO favorites(user_id,guild_id,thread_id,created_at) VALUES(?,?,?,?)",
        (str(user_id), str(guild_id), str(thread_id), now),
    )
    portal.commit()
    return True


def remove_favorite(portal, user_id, thread_id, guild_id=None):
    ensure_tables(portal)
    if guild_id:
        cur = portal.execute(
            "DELETE FROM favorites WHERE user_id=? AND thread_id=? AND guild_id=?",
            (str(user_id), str(thread_id), str(guild_id)),
        )
    else:
        cur = portal.execute(
            "DELETE FROM favorites WHERE user_id=? AND thread_id=?",
            (str(user_id), str(thread_id)),
        )
    portal.commit()
    return cur.rowcount > 0


def is_favorited(portal, user_id, thread_id):
    ensure_tables(portal)
    return bool(portal.execute(
        "SELECT 1 FROM favorites WHERE user_id=? AND thread_id=?",
        (str(user_id), str(thread_id)),
    ).fetchone())
