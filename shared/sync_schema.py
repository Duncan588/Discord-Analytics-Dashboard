"""Schema for per-guild sync (shared by downloader and admin routes)."""
from shared.sqlite_utils import add_columns


def ensure_sync_schema(conn):
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS guild_sync (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        config_id INTEGER UNIQUE, guild_id TEXT NOT NULL, forum_channel_id TEXT NOT NULL,
        owner_user_id TEXT, state TEXT DEFAULT 'backfilling', paused INTEGER DEFAULT 0,
        backfill_done INTEGER DEFAULT 0, backfill_done_at TEXT, current_run_id INTEGER,
        run_kind TEXT, threads_done INTEGER DEFAULT 0, messages_ingested INTEGER DEFAULT 0,
        oldest_reached TEXT, last_error TEXT, retry_count INTEGER DEFAULT 0, next_retry_at TEXT,
        failing_since TEXT, fail_notified INTEGER DEFAULT 0, backfill_notified INTEGER DEFAULT 0,
        last_live_started_at TEXT, last_live_done_at TEXT, next_live_at TEXT,
        restart_requested INTEGER DEFAULT 0, notes TEXT, created_at TEXT, updated_at TEXT
    );
    """)
    add_columns(conn, "download_tasks", {"sync_id": "INTEGER", "archived": "INTEGER DEFAULT 0", "live_stop_at": "TEXT"})
    conn.commit()
