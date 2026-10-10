CREATE TABLE portal_users (
        user_id TEXT PRIMARY KEY,
        username TEXT NOT NULL,
        nickname TEXT,
        avatar_url TEXT,
        last_login DATETIME
    );
CREATE TABLE servers (
        server_id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        icon_url TEXT,
        owner_user_id TEXT,
        db_path TEXT NOT NULL,
        created_at DATETIME NOT NULL,
        updated_at DATETIME NOT NULL,
        source_task_id INTEGER
    );
CREATE TABLE user_server_presence (
        user_id TEXT NOT NULL,
        server_id TEXT NOT NULL,
        first_seen DATETIME NOT NULL,
        last_seen DATETIME NOT NULL,
        PRIMARY KEY(user_id, server_id)
    );
CREATE INDEX idx_presence_user ON user_server_presence(user_id);
CREATE TABLE member_sync_requests (
        id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id TEXT NOT NULL, requested_by TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending', created_at DATETIME NOT NULL,
        finished_at DATETIME, error TEXT
    );
CREATE INDEX idx_member_sync_requests_status ON member_sync_requests(status, created_at);
CREATE TABLE whitelist_users (
        user_id TEXT PRIMARY KEY,
        username TEXT,
        added_by TEXT,
        created_at DATETIME NOT NULL
    );
CREATE TABLE user_server_access (
        user_id TEXT NOT NULL,
        server_id TEXT NOT NULL,
        granted_by TEXT,
        created_at DATETIME NOT NULL,
        PRIMARY KEY(user_id, server_id)
    );
CREATE TABLE download_bots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        owner_user_id TEXT NOT NULL,
        name TEXT NOT NULL,
        token TEXT NOT NULL,
        token_type TEXT NOT NULL DEFAULT 'bot',
        created_at DATETIME NOT NULL
    );
CREATE TABLE download_servers (
        server_id TEXT PRIMARY KEY,
        owner_user_id TEXT NOT NULL,
        guild_id TEXT NOT NULL,
        forum_channel_id TEXT NOT NULL,
        bot_id INTEGER,
        enabled INTEGER DEFAULT 1,
        use_default_bot INTEGER DEFAULT 0,
        updated_at DATETIME NOT NULL
    );
CREATE TABLE download_server_bots (
        server_id TEXT NOT NULL,
        bot_id INTEGER NOT NULL,
        PRIMARY KEY(server_id, bot_id)
    );
CREATE TABLE download_configs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        server_id TEXT NOT NULL,
        owner_user_id TEXT NOT NULL,
        guild_id TEXT NOT NULL,
        forum_channel_id TEXT NOT NULL,
        guild_name TEXT,
        forum_name TEXT,
        enabled INTEGER DEFAULT 1,
        use_default_bot INTEGER DEFAULT 0,
        scheduler_interval INTEGER DEFAULT 250,
        download_interval_ms INTEGER DEFAULT 0,
        update_enabled INTEGER DEFAULT 0,
        updated_at DATETIME NOT NULL,
        UNIQUE(guild_id, forum_channel_id)
    );
CREATE TABLE download_config_bots (
        config_id INTEGER NOT NULL,
        bot_id INTEGER NOT NULL,
        PRIMARY KEY(config_id, bot_id)
    );
CREATE INDEX idx_download_configs_owner ON download_configs(owner_user_id);
CREATE INDEX idx_download_configs_guild ON download_configs(guild_id, enabled);
CREATE TABLE download_tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id TEXT NOT NULL, forum_channel_id TEXT NOT NULL,
        created_by TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending', total INTEGER DEFAULT 0, completed INTEGER DEFAULT 0,
        started_at DATETIME, finished_at DATETIME, estimated_seconds INTEGER DEFAULT 0, elapsed_seconds INTEGER DEFAULT 0,
        message TEXT, error TEXT, notified_at DATETIME, created_at DATETIME NOT NULL
    , phase TEXT DEFAULT 'queued', scan_discovered INTEGER DEFAULT 0, scan_processed INTEGER DEFAULT 0, scan_total_estimate INTEGER DEFAULT 0, scan_started_at DATETIME, scan_finished_at DATETIME, scan_cursor TEXT, scan_completed INTEGER DEFAULT 0, active_bots INTEGER DEFAULT 0, failed_count INTEGER DEFAULT 0, speed REAL DEFAULT 0, heartbeat_at DATETIME, delete_requested INTEGER DEFAULT 0, config_id INTEGER, guild_name TEXT, forum_name TEXT, scheduler_interval INTEGER DEFAULT 250, download_interval_ms INTEGER DEFAULT 0, update_enabled INTEGER DEFAULT 0, mode TEXT DEFAULT 'initial', scan_bot_name TEXT, active_started_at DATETIME, last_active_at TEXT, recent_speed REAL DEFAULT 0);
CREATE TABLE download_task_items (
        task_id INTEGER NOT NULL,
        thread_id TEXT NOT NULL,
        thread_name TEXT,
        status TEXT NOT NULL DEFAULT 'pending',
        filename TEXT,
        bot_name TEXT,
        error TEXT,
        downloaded_at DATETIME, last_active_at TEXT,
        PRIMARY KEY(task_id,thread_id)
    );
CREATE INDEX idx_download_items_task_status ON download_task_items(task_id,status);
CREATE TABLE server_download_quota (
        user_id TEXT PRIMARY KEY,
        quota INTEGER DEFAULT 1
    );
CREATE TABLE server_visitors (
        server_id TEXT NOT NULL,
        user_id TEXT NOT NULL,
        username TEXT,
        avatar_url TEXT,
        last_visit DATETIME,
        PRIMARY KEY(server_id, user_id)
    );
CREATE TABLE demo_identities (
        real_user_id TEXT PRIMARY KEY,
        demo_user_id TEXT NOT NULL,
        demo_server_id TEXT NOT NULL,
        created_at DATETIME NOT NULL
    );
CREATE INDEX idx_download_tasks_status_created ON download_tasks(status,created_at);
CREATE INDEX idx_download_tasks_guild_status ON download_tasks(guild_id,status);
CREATE TABLE favorite_bot_users (
                    user_id INTEGER PRIMARY KEY,
                    first_seen_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')),
                    last_seen_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')),
                    help_dm_sent INTEGER NOT NULL DEFAULT 0
                );
CREATE TABLE favorites (
                    user_id INTEGER NOT NULL,
                    guild_id INTEGER NOT NULL,
                    thread_id INTEGER NOT NULL,
                    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')),
                    PRIMARY KEY (user_id, thread_id)
                );
CREATE INDEX idx_favorites_guild_thread ON favorites (guild_id, thread_id);
CREATE INDEX idx_favorites_user_guild_created ON favorites (user_id, guild_id, created_at DESC);
CREATE INDEX idx_favorites_guild_created ON favorites (guild_id, created_at DESC);
CREATE TABLE user_blocks (
                    blocker_id INTEGER NOT NULL,
                    blocked_id INTEGER NOT NULL,
                    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')),
                    PRIMARY KEY (blocker_id, blocked_id)
                );
CREATE TABLE block_interception_counts (
                    guild_id INTEGER NOT NULL,
                    sender_id INTEGER NOT NULL,
                    target_id INTEGER NOT NULL,
                    count INTEGER NOT NULL DEFAULT 0,
                    last_blocked_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')),
                    PRIMARY KEY (guild_id, sender_id, target_id)
                );
CREATE INDEX idx_user_blocks_blocker ON user_blocks (blocker_id);
CREATE TABLE thread_scan_state (guild_id TEXT NOT NULL, thread_id TEXT NOT NULL, name TEXT, last_active_at TEXT, scanned_at TEXT, PRIMARY KEY(guild_id, thread_id));
