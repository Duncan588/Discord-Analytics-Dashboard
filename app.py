import os
import re
import csv
import json
import math
import pickle
import sqlite3
import collections
import sys
import subprocess
import threading
import signal
import stat
import hashlib
import logging
import time
import uuid
from datetime import datetime, timedelta, timezone
from functools import wraps
from logging.handlers import RotatingFileHandler

import requests
from flask import Flask, render_template, request, g, redirect, session, url_for, jsonify, flash
from urllib.parse import urlencode
from werkzeug.middleware.proxy_fix import ProxyFix
from werkzeug.exceptions import HTTPException

from Preparation_Before_Use.discordDB import (
    add_column_if_missing,
    import_json_to_db,
    inspect_json,
    is_missing_table_error,
    rebuild_user_stats,
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
_startup_problems = []


class NoServerSelectedError(RuntimeError):
    pass


class ServerDataNotFoundError(RuntimeError):
    pass


def load_local_env(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip("\"'")
                if key and key not in os.environ:
                    os.environ[key] = value
    except FileNotFoundError:
        return
    except OSError as exc:
        _startup_problems.append(f"读取本地环境文件失败 path={path} error={exc}")

load_local_env(os.path.join(BASE_DIR, ".env"))

def _env_keys(path):
    keys = set()
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            for raw in f:
                line = raw.strip()
                if line and not line.startswith("#") and "=" in line:
                    keys.add(line.split("=", 1)[0].strip())
    return keys

def validate_env_contract():
    example = os.path.join(BASE_DIR, ".env.example")
    actual = os.path.join(BASE_DIR, ".env")
    expected = _env_keys(example)
    present = _env_keys(actual)
    missing = sorted(expected - present)
    extra = sorted(present - expected)
    if missing:
        raise RuntimeError(".env 缺少 .env.example 中定义的配置项: " + ", ".join(missing))
    if extra:
        raise RuntimeError(".env 包含 .env.example 未定义的配置项: " + ", ".join(extra))
    return expected

ENV_KEYS = validate_env_contract()
DATA_DIR = os.path.join(BASE_DIR, "data", "servers")
CACHE_DIR = os.path.join(BASE_DIR, "data", "cache")
PORTAL_DB = os.getenv("PORTAL_DB", os.path.join(BASE_DIR, "data", "portal.db"))
if not os.path.isabs(PORTAL_DB):
    PORTAL_DB = os.path.join(BASE_DIR, PORTAL_DB)
LEGACY_DB = os.path.join(BASE_DIR, "discord_data.db")
ITEMS_PER_PAGE = 100
ADMIN_IDS = {x.strip() for x in os.getenv("ADMIN_IDS", "891196284998930522").split(",") if x.strip()}
DISCORD_CLIENT_ID = os.getenv("DISCORD_CLIENT_ID", "").strip()
DISCORD_CLIENT_SECRET = os.getenv("DISCORD_CLIENT_SECRET", "").strip()
API_BASE_URL = "https://discord.com/api/v10"
SECRET_KEY = os.getenv("FLASK_SECRET_KEY", "").strip()
if not SECRET_KEY:
    secret_file = os.path.join(BASE_DIR, "data", ".flask_secret")
    try:
        os.makedirs(os.path.dirname(secret_file), exist_ok=True)
    except OSError as exc:
        _startup_problems.append(
            f"准备持久化 SECRET_KEY 目录失败 path={os.path.dirname(secret_file)} error={exc}；会话不会在重启后保留"
        )
    try:
        with open(secret_file, "r", encoding="utf-8") as f:
            SECRET_KEY = f.read().strip()
    except OSError as exc:
        _startup_problems.append(f"读取持久化 SECRET_KEY 失败 path={secret_file} error={exc}")
        SECRET_KEY = ""
    if not SECRET_KEY:
        import secrets
        SECRET_KEY = secrets.token_urlsafe(48)
        try:
            with open(secret_file, "w", encoding="utf-8") as f:
                f.write(SECRET_KEY)
        except OSError as exc:
            _startup_problems.append(
                f"写入持久化 SECRET_KEY 失败 path={secret_file} error={exc}；会话不会在重启后保留"
            )
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").strip().rstrip("/")

app = Flask(__name__)
app.secret_key = SECRET_KEY
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_port=1)
app.permanent_session_lifetime = timedelta(days=30)
app.config["MAX_CONTENT_LENGTH"] = int(os.getenv("MAX_UPLOAD_BYTES", str(2 * 1024 * 1024 * 1024)))

# 静态资源通过查询参数携带发布版本（例如 ?v=20260821-1）。版本化资源
# 内容不会在同一版本内变化，可以安全地交给 Activity/浏览器长期复用。
VERSIONED_STATIC_MAX_AGE = 365 * 24 * 60 * 60

# Discord 活动（Activities）模式下，网站会被 Discord 客户端以跨站 iframe 形式加载
# （地址类似 https://<app_id>.discordsays.com/ 通过 Discord 的代理转发到本站）。
# 跨站 iframe 场景下，浏览器默认的 SameSite=Lax Cookie 不会被携带，登录状态会丢失，
# 因此这里在部署为 https 时把 Session Cookie 切换成 SameSite=None; Secure。
# 本地 http 调试（PUBLIC_BASE_URL 未配置或不是 https）时保持 Lax，避免开发环境下
# Cookie 因为没有 https 被浏览器整个丢弃。
_IS_HTTPS_DEPLOY = PUBLIC_BASE_URL.startswith("https://")
app.config["SESSION_COOKIE_SAMESITE"] = "None" if _IS_HTTPS_DEPLOY else "Lax"
app.config["SESSION_COOKIE_SECURE"] = _IS_HTTPS_DEPLOY
# Discord Activity 是跨站 iframe；Partitioned Cookie 兼容浏览器对第三方 Cookie
# 的限制，同时仍将会话隔离在当前顶层 Discord 页面下。Flask 3.1+ 会自动附加
# `Partitioned` 属性并要求 `Secure`，本地 HTTP 调试则保持关闭。
app.config["SESSION_COOKIE_PARTITIONED"] = _IS_HTTPS_DEPLOY

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(os.path.join(BASE_DIR, "data", "uploads"), exist_ok=True)
LOG_DIR = os.path.join(BASE_DIR, "data", "logs")
os.makedirs(LOG_DIR, exist_ok=True)


def configure_logging():
    """配置门户日志；日志文件可直接提供给排障，不包含 OAuth 密钥或 token。"""
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )

    app.logger.handlers.clear()
    app.logger.setLevel(level)
    app.logger.propagate = False

    file_handler = RotatingFileHandler(
        os.path.join(LOG_DIR, "app.log"),
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    app.logger.addHandler(file_handler)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    app.logger.addHandler(console_handler)


configure_logging()
for _problem in _startup_problems:
    app.logger.warning(_problem)
_startup_problems.clear()


def _json_error_request():
    accepts = request.accept_mimetypes
    return request.path.startswith("/api/") or (
        accepts.accept_json and not accepts.accept_html
    )


@app.errorhandler(Exception)
def handle_unhandled_exception(error):
    if isinstance(error, HTTPException):
        return error
    app.logger.exception("未处理的请求异常 request_id=%s", _request_id())
    if _json_error_request():
        return jsonify({"ok": False, "error": "服务器内部错误"}), 500
    return "服务器内部错误", 500


def _handle_server_state_error(error):
    app.logger.warning("请求需要服务器数据 request_id=%s error=%s", _request_id(), error)
    if _json_error_request():
        return jsonify({"ok": False, "error": str(error)}), 400
    return redirect(url_for("welcome"))


@app.errorhandler(NoServerSelectedError)
def handle_no_server_selected(error):
    return _handle_server_state_error(error)


@app.errorhandler(ServerDataNotFoundError)
def handle_server_data_not_found(error):
    return _handle_server_state_error(error)


def _request_id():
    try:
        return getattr(g, "request_id", "-")
    except RuntimeError:
        # 允许独立测试/启动阶段调用 Discord 辅助函数。
        return "-"


@app.before_request
def log_request_start():
    supplied = (request.headers.get("X-Request-ID") or "").strip()
    if not re.fullmatch(r"[A-Za-z0-9._:-]{1,80}", supplied):
        supplied = uuid.uuid4().hex
    g.request_id = supplied
    g.request_started_at = time.perf_counter()
    app.logger.info(
        "request.start request_id=%s method=%s path=%s remote=%s user_agent=%s",
        g.request_id,
        request.method,
        request.path,
        request.headers.get("X-Forwarded-For", request.remote_addr or "-"),
        (request.user_agent.string or "-")[:160],
    )


@app.after_request
def log_request_end(response):
    started_at = getattr(g, "request_started_at", None)
    elapsed_ms = (time.perf_counter() - started_at) * 1000 if started_at else 0
    response.headers["X-Request-ID"] = _request_id()
    if request.path.startswith("/static/") and request.args.get("v"):
        # Flask may have already added no-cache for static responses; replace
        # the whole directive so versioned assets are actually reusable.
        response.headers["Cache-Control"] = (
            f"public, max-age={VERSIONED_STATIC_MAX_AGE}, immutable"
        )
    app.logger.info(
        "request.end request_id=%s status=%s elapsed_ms=%.1f",
        _request_id(), response.status_code, elapsed_ms,
    )
    return response


def db_connect(path):
    conn = sqlite3.connect(path, timeout=60)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=60000")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def get_portal_db():
    if not hasattr(g, "portal_db"):
        g.portal_db = db_connect(PORTAL_DB)
    return g.portal_db


def current_server_id():
    return str(session.get("server_id")) if session.get("server_id") else None


def server_db_path(server_id):
    return os.path.join(DATA_DIR, str(server_id), "discord_data.db")


def get_db():
    sid = current_server_id()
    if not sid:
        raise NoServerSelectedError("未选择服务器")
    if not hasattr(g, "analytics_db"):
        path = server_db_path(sid)
        if not os.path.exists(path):
            raise ServerDataNotFoundError("服务器数据不存在")
        g.analytics_db = db_connect(path)
    return g.analytics_db


@app.teardown_appcontext
def close_connections(exception=None):
    for name in ("portal_db", "analytics_db"):
        conn = getattr(g, name, None)
        if conn is not None:
            conn.close()


def init_portal_db():
    conn = db_connect(PORTAL_DB)
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS portal_users (
        user_id TEXT PRIMARY KEY,
        username TEXT NOT NULL,
        nickname TEXT,
        avatar_url TEXT,
        last_login DATETIME
    );
    CREATE TABLE IF NOT EXISTS servers (
        server_id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        icon_url TEXT,
        owner_user_id TEXT,
        db_path TEXT NOT NULL,
        created_at DATETIME NOT NULL,
        updated_at DATETIME NOT NULL,
        source_task_id INTEGER
    );
    CREATE TABLE IF NOT EXISTS user_server_presence (
        user_id TEXT NOT NULL,
        server_id TEXT NOT NULL,
        first_seen DATETIME NOT NULL,
        last_seen DATETIME NOT NULL,
        PRIMARY KEY(user_id, server_id)
    );
    CREATE INDEX IF NOT EXISTS idx_presence_user ON user_server_presence(user_id);
    CREATE TABLE IF NOT EXISTS member_sync_requests (
        id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id TEXT NOT NULL, requested_by TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending', created_at DATETIME NOT NULL,
        finished_at DATETIME, error TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_member_sync_requests_status ON member_sync_requests(status, created_at);
    CREATE TABLE IF NOT EXISTS whitelist_users (
        user_id TEXT PRIMARY KEY,
        username TEXT,
        added_by TEXT,
        created_at DATETIME NOT NULL
    );
    CREATE TABLE IF NOT EXISTS user_server_access (
        user_id TEXT NOT NULL,
        server_id TEXT NOT NULL,
        granted_by TEXT,
        created_at DATETIME NOT NULL,
        PRIMARY KEY(user_id, server_id)
    );
    CREATE TABLE IF NOT EXISTS download_bots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        owner_user_id TEXT NOT NULL,
        name TEXT NOT NULL,
        token TEXT NOT NULL,
        created_at DATETIME NOT NULL
    );
    CREATE TABLE IF NOT EXISTS download_servers (
        server_id TEXT PRIMARY KEY,
        owner_user_id TEXT NOT NULL,
        guild_id TEXT NOT NULL,
        forum_channel_id TEXT NOT NULL,
        bot_id INTEGER,
        enabled INTEGER DEFAULT 1,
        use_default_bot INTEGER DEFAULT 0,
        updated_at DATETIME NOT NULL
    );
    CREATE TABLE IF NOT EXISTS download_server_bots (
        server_id TEXT NOT NULL,
        bot_id INTEGER NOT NULL,
        PRIMARY KEY(server_id, bot_id)
    );
    CREATE TABLE IF NOT EXISTS download_configs (
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
    CREATE TABLE IF NOT EXISTS download_config_bots (
        config_id INTEGER NOT NULL,
        bot_id INTEGER NOT NULL,
        PRIMARY KEY(config_id, bot_id)
    );
    CREATE INDEX IF NOT EXISTS idx_download_configs_owner ON download_configs(owner_user_id);
    CREATE INDEX IF NOT EXISTS idx_download_configs_guild ON download_configs(guild_id, enabled);
    CREATE TABLE IF NOT EXISTS download_tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id TEXT NOT NULL, forum_channel_id TEXT NOT NULL,
        created_by TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending', total INTEGER DEFAULT 0, completed INTEGER DEFAULT 0,
        started_at DATETIME, finished_at DATETIME, estimated_seconds INTEGER DEFAULT 0, elapsed_seconds INTEGER DEFAULT 0,
        message TEXT, error TEXT, notified_at DATETIME, created_at DATETIME NOT NULL
    );
    CREATE TABLE IF NOT EXISTS download_task_items (
        task_id INTEGER NOT NULL,
        thread_id TEXT NOT NULL,
        thread_name TEXT,
        status TEXT NOT NULL DEFAULT 'pending',
        filename TEXT,
        bot_name TEXT,
        error TEXT,
        downloaded_at DATETIME,
        PRIMARY KEY(task_id,thread_id)
    );
    CREATE INDEX IF NOT EXISTS idx_download_items_task_status ON download_task_items(task_id,status);
    CREATE TABLE IF NOT EXISTS server_download_quota (
        user_id TEXT PRIMARY KEY,
        quota INTEGER DEFAULT 1
    );
    CREATE TABLE IF NOT EXISTS server_visitors (
        server_id TEXT NOT NULL,
        user_id TEXT NOT NULL,
        username TEXT,
        avatar_url TEXT,
        last_visit DATETIME,
        PRIMARY KEY(server_id, user_id)
    );
    """)
    task_migrations = {
        "phase": "TEXT DEFAULT 'queued'",
        "scan_discovered": "INTEGER DEFAULT 0",
        "scan_processed": "INTEGER DEFAULT 0",
        "scan_total_estimate": "INTEGER DEFAULT 0",
        "scan_started_at": "DATETIME",
        "scan_finished_at": "DATETIME",
        "scan_cursor": "TEXT",
        "scan_completed": "INTEGER DEFAULT 0",
        "active_bots": "INTEGER DEFAULT 0",
        "failed_count": "INTEGER DEFAULT 0",
        "speed": "REAL DEFAULT 0",
        "heartbeat_at": "DATETIME",
        "delete_requested": "INTEGER DEFAULT 0",
        "config_id": "INTEGER",
        "guild_name": "TEXT",
        "forum_name": "TEXT",
        "scheduler_interval": "INTEGER DEFAULT 250",
        "download_interval_ms": "INTEGER DEFAULT 0",
        "update_enabled": "INTEGER DEFAULT 0",
        "mode": "TEXT DEFAULT 'initial'",
        "scan_bot_name": "TEXT"
    }
    for col, typ in task_migrations.items():
        add_column_if_missing(conn, "download_tasks", col, typ)
    add_column_if_missing(conn, "download_servers", "use_default_bot", "INTEGER DEFAULT 0")
    add_column_if_missing(conn, "servers", "source_task_id", "INTEGER")
    add_column_if_missing(conn, "download_configs", "update_enabled", "INTEGER DEFAULT 0")
    add_column_if_missing(conn, "download_configs", "download_interval_ms", "INTEGER DEFAULT 0")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_download_tasks_status_created ON download_tasks(status,created_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_download_tasks_guild_status ON download_tasks(guild_id,status)")
    conn.commit()
    # 旧版本数据库兼容：为已有白名单用户补充默认 1 台服务器额度。
    conn.execute("INSERT OR IGNORE INTO server_download_quota(user_id,quota) SELECT user_id,1 FROM whitelist_users")
    # 兼容旧的单机器人配置；新配置允许同一服务器选择多个机器人。
    conn.execute("INSERT OR IGNORE INTO download_server_bots(server_id,bot_id) SELECT server_id,bot_id FROM download_servers WHERE bot_id IS NOT NULL")
    # 兼容旧版：把“每服务器一个论坛”的配置迁移到可多论坛配置表。
    old_rows = conn.execute("SELECT * FROM download_servers WHERE enabled=1").fetchall()
    for old in old_rows:
        exists_cfg = conn.execute("SELECT id FROM download_configs WHERE guild_id=? AND forum_channel_id=?", (str(old["guild_id"]), str(old["forum_channel_id"]))).fetchone()
        if exists_cfg:
            continue
        server_row = conn.execute("SELECT name FROM servers WHERE server_id=?", (str(old["guild_id"]),)).fetchone()
        guild_name = server_row["name"] if server_row else str(old["guild_id"])
        cur = conn.execute("INSERT INTO download_configs(server_id,owner_user_id,guild_id,forum_channel_id,guild_name,forum_name,enabled,use_default_bot,scheduler_interval,updated_at) VALUES(?,?,?,?,?,?,1,?,250,?)",
            (f'{old["guild_id"]}:{old["forum_channel_id"]}', old["owner_user_id"], old["guild_id"], old["forum_channel_id"], guild_name, old["forum_channel_id"], int(old["use_default_bot"] or 0), old["updated_at"]))
        cfg_id = cur.lastrowid
        for b in conn.execute("SELECT bot_id FROM download_server_bots WHERE server_id=?", (str(old["server_id"]),)).fetchall():
            conn.execute("INSERT OR IGNORE INTO download_config_bots(config_id,bot_id) VALUES(?,?)", (cfg_id, b["bot_id"]))
    conn.commit()
    conn.close()


def init_server_db(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = db_connect(path)
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS users (
        user_id TEXT PRIMARY KEY, username TEXT, nickname TEXT, avatar_url TEXT, is_bot BOOLEAN
    );
    CREATE TABLE IF NOT EXISTS threads (
        thread_id TEXT PRIMARY KEY, category_id TEXT, name TEXT, exported_at TEXT, guild_id TEXT, last_active_at TEXT
    );
    CREATE TABLE IF NOT EXISTS thread_scan_state (
        thread_id TEXT PRIMARY KEY, name TEXT, last_active_at TEXT, scanned_at TEXT, guild_id TEXT
    );
    CREATE TABLE IF NOT EXISTS messages (
        message_id TEXT PRIMARY KEY, thread_id TEXT, author_id TEXT, content TEXT,
        timestamp DATETIME, reply_to_msg_id TEXT
    );
    CREATE TABLE IF NOT EXISTS reactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT, message_id TEXT, user_id TEXT,
        emoji_name TEXT, emoji_url TEXT
    );
    CREATE TABLE IF NOT EXISTS attachments (
        id INTEGER PRIMARY KEY AUTOINCREMENT, message_id TEXT, url TEXT, filename TEXT, size_bytes INTEGER
    );
    CREATE TABLE IF NOT EXISTS mentions (
        id INTEGER PRIMARY KEY AUTOINCREMENT, message_id TEXT, mentioned_user_id TEXT, author_id TEXT
    );
    CREATE TABLE IF NOT EXISTS user_stats (
        user_id TEXT PRIMARY KEY, msg_count INTEGER DEFAULT 0,
        reaction_received_count INTEGER DEFAULT 0, interaction_score INTEGER DEFAULT 0,
        first_msg_at DATETIME, last_msg_at DATETIME
    );
    CREATE TABLE IF NOT EXISTS user_merges (
        target_id TEXT PRIMARY KEY, parent_id TEXT, created_at DATETIME
    );
    CREATE TABLE IF NOT EXISTS claim_requests_v2 (
        id INTEGER PRIMARY KEY AUTOINCREMENT, requester_id TEXT, target_id TEXT,
        target_name TEXT, status INTEGER DEFAULT 0, created_at DATETIME,
        UNIQUE(requester_id, target_id)
    );
    CREATE TABLE IF NOT EXISTS web_visitors (
        user_id TEXT PRIMARY KEY, username TEXT, nickname TEXT, avatar_url TEXT, last_visit DATETIME
    );
    CREATE TABLE IF NOT EXISTS profile_views (
        id INTEGER PRIMARY KEY AUTOINCREMENT, target_user_id TEXT, viewer_user_id TEXT,
        viewer_name TEXT, viewer_avatar TEXT, timestamp DATETIME,
        UNIQUE(target_user_id, viewer_user_id)
    );
    CREATE INDEX IF NOT EXISTS idx_msg_author ON messages(author_id);
    CREATE INDEX IF NOT EXISTS idx_msg_timestamp ON messages(timestamp);
    CREATE INDEX IF NOT EXISTS idx_msg_thread ON messages(thread_id);
    CREATE INDEX IF NOT EXISTS idx_react_user ON reactions(user_id);
    CREATE INDEX IF NOT EXISTS idx_react_msg ON reactions(message_id);
    CREATE INDEX IF NOT EXISTS idx_stats_count ON user_stats(msg_count);
    """)
    add_column_if_missing(conn, "threads", "last_active_at", "TEXT")
    conn.commit()
    conn.close()


def migrate_legacy_db():
    if not os.path.exists(LEGACY_DB):
        return
    conn = db_connect(PORTAL_DB)
    exists = conn.execute("SELECT 1 FROM servers LIMIT 1").fetchone()
    if exists:
        conn.close()
        return
    legacy_server_id = os.getenv("LEGACY_SERVER_ID", "915249444721668096")
    target_dir = os.path.join(DATA_DIR, legacy_server_id)
    target = server_db_path(legacy_server_id)
    if not os.path.exists(target):
        os.makedirs(target_dir, exist_ok=True)
        with open(LEGACY_DB, "rb") as src, open(target, "wb") as dst:
            dst.write(src.read())
    init_server_db(target)
    now = datetime.now(timezone.utc).isoformat()
    conn.execute("""INSERT OR REPLACE INTO servers(
        server_id,name,icon_url,owner_user_id,db_path,created_at,updated_at,source_task_id
    ) VALUES (?,?,?,?,?,?,?,NULL)""", (
        legacy_server_id, "Legacy Discord Server", None, None, target, now, now
    ))
    conn.commit()
    conn.close()


def format_time(seconds):
    seconds = max(0, int(seconds))
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    return f"{h:d}:{m:02d}:{s:02d}" if h else f"{m:02d}:{s:02d}"


def parse_and_convert(time_str):
    if not time_str:
        return None
    try:
        value = str(time_str).replace("Z", "+00:00")
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone(timedelta(hours=8)))
    except (TypeError, ValueError):
        return None


@app.template_filter("datetimeformat")
def datetimeformat_filter(value, format="%Y-%m-%d %H:%M"):
    dt = parse_and_convert(value)
    return dt.strftime(format) if dt else value


@app.template_filter("raw_datetime")
def raw_datetime_filter(value, format="%Y-%m-%d %H:%M"):
    return datetimeformat_filter(value, format)


def is_pure_chinese(word):
    return bool(word) and re.fullmatch(r"[\u4e00-\u9fa5]+", str(word)) is not None


def get_word_cloud_counter(text_list):
    text = " ".join(str(t) for t in text_list if t)
    text = text[:5000000]
    words = re.findall(r"[\u4e00-\u9fa5]{2,}", text)
    stop_words = set("什么 这个 那个 怎么 可以 因为 所以 但是 就是 这就 感觉 时候 现在 还是 没有 一样 知道 觉得 出来 其实 这种 那样 一下 然后 虽然 不是 还有 这里 那里 今天 明天 真的 可能 图片 表情 回复 一个 自己 只是 非常 不能 不要 需要 如果 以及 我们 你们 他们 看到 不过 确实 已经 大家 为什么 不会 这样 这么 那么 那些 是不是 有没有".split())
    return collections.Counter(w for w in words if w not in stop_words)


def format_word_cloud(counter, limit=None):
    items = [(k, v) for k, v in counter.items() if is_pure_chinese(k)]
    items.sort(key=lambda x: x[1], reverse=True)
    if limit:
        items = items[:limit]
    return [{"text": k, "weight": v} for k, v in items]


def process_messages(conn, rows):
    rows = [dict(x) for x in rows]
    if not rows:
        return []
    ids = [str(x["message_id"]) for x in rows if x.get("message_id")]
    if not ids:
        return rows
    placeholders = ",".join("?" for _ in ids)
    cursor = conn.execute(
        f"SELECT message_id, emoji_name, emoji_url, count(*) count FROM reactions WHERE message_id IN ({placeholders}) GROUP BY message_id, emoji_name ORDER BY message_id, count DESC",
        ids,
    )
    reaction_map = {}
    for r in cursor.fetchall():
        reaction_map.setdefault(r["message_id"], []).append(dict(r))
    for row in rows:
        row["detailed_reactions"] = reaction_map.get(row["message_id"], [])[:3]
    return rows


class DataEngine:
    def __init__(self):
        self.cache = {}
        # 下载器会持续写入数据库。首页数据允许短暂使用上一份快照，避免每条新消息
        # 都让 Activity 请求同步重建词云、榜单和趋势数据。
        self._refresh_lock = threading.Lock()
        self._refreshing = set()
        self._last_refresh_started = {}
        self._refresh_interval = 10.0

    def _cache_file(self, sid):
        return os.path.join(CACHE_DIR, f"{sid}.pkl")

    def _load_cache(self, sid):
        if sid in self.cache:
            return self.cache[sid]
        path = self._cache_file(sid)
        try:
            with open(path, "rb") as f:
                value = pickle.load(f)
                self.cache[sid] = value
                return value
        except FileNotFoundError:
            app.logger.debug("首页缓存不存在 sid=%s path=%s", sid, path)
        except (OSError, EOFError, pickle.UnpicklingError, TypeError, ValueError) as exc:
            app.logger.warning("首页缓存读取失败 sid=%s path=%s error=%s", sid, path, exc)
        value = {"last_msg_count": -1, "global_word_counter": collections.Counter(), "homepage": {}, "merges": {}}
        self.cache[sid] = value
        return value

    def _save_cache(self, sid):
        path = self._cache_file(sid)
        tmp = path + ".tmp"
        with open(tmp, "wb") as f:
            pickle.dump(self.cache[sid], f)
        os.replace(tmp, path)

    def _refresh(self, sid):
        """Rebuild one homepage snapshot outside the request thread."""
        try:
            cache = self._load_cache(sid)
            db_path = server_db_path(sid)
            conn = db_connect(db_path)
            cur = conn.cursor()
            count = cur.execute("SELECT count(*) FROM messages").fetchone()[0]
            try:
                db_mtime = os.path.getmtime(db_path)
            except OSError:
                db_mtime = 0
            texts = [
                row[0]
                for row in cur.execute(
                    "SELECT content FROM messages WHERE content IS NOT NULL AND content != ''"
                )
            ]
            cache["global_word_counter"] = get_word_cloud_counter(texts)
            cache["homepage"] = self._homepage(cur, cache["global_word_counter"])
            cache["last_msg_count"] = count
            cache["db_mtime"] = db_mtime
            cache["homepage_version"] = 4
            cache["merges"] = {
                str(r["target_id"]): str(r["parent_id"])
                for r in cur.execute("SELECT target_id,parent_id FROM user_merges")
            }
            conn.close()
            self._save_cache(sid)
        except Exception:
            app.logger.exception("刷新首页缓存失败: %s", sid)
        finally:
            with self._refresh_lock:
                self._refreshing.discard(str(sid))

    def _schedule_refresh(self, sid):
        sid = str(sid)
        with self._refresh_lock:
            now = time.monotonic()
            if sid in self._refreshing or now - self._last_refresh_started.get(sid, 0) < self._refresh_interval:
                return
            self._refreshing.add(sid)
            self._last_refresh_started[sid] = now
        # 给当前请求留出完成响应的时间，避免后台词云重算立刻和首屏争抢
        # Python CPU/SQLite 资源。快照仍会在很短时间内自动更新。
        thread = threading.Timer(0.25, self._refresh, args=(sid,))
        thread.name = f"homepage-cache-{sid}"
        thread.daemon = True
        thread.start()

    def load_or_compute(self, sid):
        cache = self._load_cache(sid)
        db_path = server_db_path(sid)
        conn = db_connect(db_path)
        cur = conn.cursor()
        count = cur.execute("SELECT count(*) FROM messages").fetchone()[0]
        try:
            db_mtime = os.path.getmtime(db_path)
        except OSError:
            db_mtime = 0
        # Rebuild once after changing the homepage ranking schema so stale cache
        # data cannot keep the old (possibly incomplete) lists on screen. When a
        # valid snapshot already exists, refresh it in the background: this is
        # important while the downloader is importing messages continuously.
        cache_needs_refresh = (
            count != cache.get("last_msg_count")
            or db_mtime != cache.get("db_mtime")
            or cache.get("homepage_version") != 4
            or not cache.get("homepage")
        )
        if cache_needs_refresh and cache.get("homepage"):
            self._schedule_refresh(sid)
        elif cache_needs_refresh:
            # There is no usable first snapshot yet, so the first request must
            # wait for one. Subsequent requests use the cheap stale-while-refresh
            # path above.
            conn.close()
            self._refresh(sid)
            cache = self._load_cache(sid)
        conn.close()
        return cache["homepage"]

    def _homepage(self, cur, counter):
        data = {}
        data["total_msgs"] = cur.execute("SELECT count(*) FROM messages").fetchone()[0]
        data["total_threads"] = cur.execute("SELECT count(*) FROM threads").fetchone()[0]
        data["total_users"] = cur.execute("SELECT count(*) FROM users").fetchone()[0]
        data["chart_daily"] = [dict(r) for r in cur.execute("SELECT substr(timestamp,1,10) day,count(*) c FROM messages GROUP BY day ORDER BY day")]
        hourly = {r["hour"]: r["c"] for r in cur.execute("SELECT strftime('%H',timestamp) hour,count(*) c FROM messages GROUP BY hour")}
        hours = [0] * 24
        for h, c in hourly.items():
            if h is not None:
                hours[(int(h) + 8) % 24] += c
        data["chart_hourly"] = [{"hour": f"{i:02d}:00", "c": v} for i, v in enumerate(hours)]
        data["server_word_cloud"] = format_word_cloud(counter, 60)
        data["server_word_rank"] = data["server_word_cloud"][:15]
        top_users = []
        rows = cur.execute("SELECT u.*,count(m.message_id) msg_count FROM users u JOIN messages m ON u.user_id=m.author_id GROUP BY u.user_id ORDER BY msg_count DESC LIMIT 10").fetchall()
        for u in rows:
            d = dict(u)
            d["top_emojis"] = [dict(r) for r in cur.execute("SELECT r.emoji_url,r.emoji_name,count(*) c FROM reactions r JOIN messages m ON r.message_id=m.message_id WHERE m.author_id=? GROUP BY r.emoji_name ORDER BY c DESC LIMIT 3", (u["user_id"],))]
            top_users.append(d)
        data["top_users"] = top_users
        top_threads = []
        thread_rows = cur.execute("SELECT thread_id,count(*) c FROM messages GROUP BY thread_id ORDER BY c DESC,thread_id LIMIT 10").fetchall()
        for rank, r in enumerate(thread_rows, 1):
            t = cur.execute("SELECT * FROM threads WHERE thread_id=?", (r["thread_id"],)).fetchone()
            if not t:
                continue
            d = dict(t)
            d["msg_count"] = r["c"]
            d["rank"] = rank
            first = cur.execute("SELECT message_id,content,timestamp FROM messages WHERE thread_id=? ORDER BY timestamp,message_id LIMIT 1", (d["thread_id"],)).fetchone()
            d["first_content"] = first["content"] if first else ""
            d["op_msg_id"] = first["message_id"] if first else ""
            d["created_at"] = first["timestamp"] if first and first["timestamp"] else d.get("exported_at")
            op = cur.execute("SELECT u.username,u.nickname,u.avatar_url FROM users u JOIN messages m ON u.user_id=m.author_id WHERE m.thread_id=? ORDER BY m.timestamp LIMIT 1", (d["thread_id"],)).fetchone()
            d["op_user"] = dict(op) if op else {"username": "Unknown", "nickname": "", "avatar_url": ""}
            top_threads.append(d)
        data["top_threads"] = top_threads
        hot = []
        hot_rows = cur.execute("SELECT message_id,count(*) c FROM reactions GROUP BY message_id ORDER BY c DESC,message_id LIMIT 10").fetchall()
        for rank, r in enumerate(hot_rows, 1):
            m = cur.execute("SELECT * FROM messages WHERE message_id=?", (r["message_id"],)).fetchone()
            if not m:
                continue
            d = dict(m)
            d["rank"] = rank
            auth = cur.execute("SELECT username,nickname,avatar_url FROM users WHERE user_id=?", (d["author_id"],)).fetchone()
            d["author"] = dict(auth) if auth else {"username": "Unknown", "nickname": "", "avatar_url": ""}
            tn = cur.execute("SELECT name FROM threads WHERE thread_id=?", (d["thread_id"],)).fetchone()
            d["thread_name"] = tn["name"] if tn else "Unknown"
            d["detailed_reactions"] = [dict(x) for x in cur.execute("SELECT emoji_url,emoji_name,count(*) count FROM reactions WHERE message_id=? GROUP BY emoji_name ORDER BY count DESC LIMIT 3", (d["message_id"],))]
            hot.append(d)
        data["top_hot_msgs"] = hot
        return data

    def get_merged_ids(self, sid, uid):
        cache = self._load_cache(sid)
        if not cache.get("merges"):
            conn = db_connect(server_db_path(sid))
            cache["merges"] = {str(r["target_id"]): str(r["parent_id"]) for r in conn.execute("SELECT target_id,parent_id FROM user_merges")}
            conn.close()
        children = [k for k, v in cache["merges"].items() if v == str(uid)]
        return [str(uid)] + children


data_engine = DataEngine()


def admin_level(user_id):
    uid = str(user_id)
    if uid in ADMIN_IDS:
        return 1
    row = get_portal_db().execute("SELECT 1 FROM whitelist_users WHERE user_id=?", (uid,)).fetchone()
    return 2 if row else 0


def get_download_quota(user_id):
    row = get_portal_db().execute("SELECT quota FROM server_download_quota WHERE user_id=?", (str(user_id),)).fetchone()
    return int(row["quota"]) if row else 1


def _parse_clamped_int(value, default, lower, upper):
    if value is None or value == "":
        return default
    try:
        return max(lower, min(upper, int(value)))
    except (TypeError, ValueError):
        return None


def sync_server_users_to_portal(server_id):
    """把服务器分析库中的普通用户同步到门户数据库，避免普通登录用户被误判为无数据。"""
    sid = str(server_id)
    path = server_db_path(sid)
    if not os.path.exists(path):
        return 0
    now = datetime.now(timezone.utc).isoformat()
    portal = get_portal_db()
    conn = db_connect(path)
    rows = conn.execute("SELECT user_id,username,nickname,avatar_url FROM users WHERE user_id IS NOT NULL AND user_id!=''").fetchall()
    conn.close()
    for row in rows:
        uid = str(row["user_id"])
        portal.execute(
            """INSERT INTO portal_users(user_id,username,nickname,avatar_url,last_login)
               VALUES(?,?,?,?,COALESCE((SELECT last_login FROM portal_users WHERE user_id=?),NULL))
               ON CONFLICT(user_id) DO UPDATE SET
               username=CASE WHEN excluded.username!='' THEN excluded.username ELSE portal_users.username END,
               nickname=CASE WHEN excluded.nickname!='' THEN excluded.nickname ELSE portal_users.nickname END,
               avatar_url=CASE WHEN excluded.avatar_url!='' THEN excluded.avatar_url ELSE portal_users.avatar_url END""",
            (uid, row["username"] or uid, row["nickname"] or row["username"] or uid, row["avatar_url"], uid)
        )
        portal.execute(
            """INSERT INTO user_server_presence(user_id,server_id,first_seen,last_seen)
               VALUES(?,?,?,?) ON CONFLICT(user_id,server_id) DO UPDATE SET last_seen=excluded.last_seen""",
            (uid, sid, now, now)
        )
    portal.commit()
    return len(rows)


def sync_all_server_users():
    try:
        portal = get_portal_db()
        # 首次升级旧项目时回填一次；之后由导入/下载流程实时维护，避免每次启动扫描所有服务器数据库。
        if portal.execute("SELECT 1 FROM user_server_presence LIMIT 1").fetchone():
            return
        servers = portal.execute("SELECT server_id FROM servers").fetchall()
        for row in servers:
            try:
                sync_server_users_to_portal(row["server_id"])
            except Exception:
                app.logger.exception("同步服务器普通用户失败: %s", row["server_id"])
    except Exception:
        app.logger.exception("初始化普通用户数据失败")


def get_servers_for_user(user_id):
    uid = str(user_id)
    portal = get_portal_db()
    rows = portal.execute("SELECT * FROM servers ORDER BY name").fetchall()
    result = []
    level = admin_level(uid)
    for server in rows:
        sid = str(server["server_id"])
        path = server["db_path"]
        if not os.path.exists(path):
            continue
        if level == 1:
            result.append(server)
            continue
        found = portal.execute("SELECT 1 FROM user_server_access WHERE user_id=? AND server_id=?", (uid, sid)).fetchone()
        present = portal.execute("SELECT 1 FROM user_server_presence WHERE user_id=? AND server_id=?", (uid, sid)).fetchone()
        # 对历史数据库即时校验 users 主键；无需把 20 万帖子重新同步到 portal.db。
        if not present:
            try:
                adb = db_connect(path)
                present = adb.execute("SELECT user_id,username,nickname,avatar_url FROM users WHERE user_id=? LIMIT 1", (uid,)).fetchone()
                adb.close()
                if present:
                    timestamp = datetime.now(timezone.utc).isoformat()
                    portal.execute("INSERT INTO portal_users(user_id,username,nickname,avatar_url,last_login) VALUES(?,?,?,?,NULL) ON CONFLICT(user_id) DO UPDATE SET username=CASE WHEN excluded.username!='' THEN excluded.username ELSE portal_users.username END,nickname=CASE WHEN excluded.nickname!='' THEN excluded.nickname ELSE portal_users.nickname END,avatar_url=CASE WHEN excluded.avatar_url!='' THEN excluded.avatar_url ELSE portal_users.avatar_url END", (uid, present["username"] or uid, present["nickname"] or present["username"] or uid, present["avatar_url"]))
                    portal.execute("INSERT INTO user_server_presence(user_id,server_id,first_seen,last_seen) VALUES(?,?,?,?) ON CONFLICT(user_id,server_id) DO UPDATE SET last_seen=excluded.last_seen", (uid, sid, timestamp, timestamp))
                    portal.commit()
            except Exception:
                app.logger.exception("检查普通用户服务器数据失败: %s", sid)
        if found or present or str(server["owner_user_id"] or "") == uid:
            if not found:
                portal.execute("INSERT OR IGNORE INTO user_server_access(user_id,server_id,granted_by,created_at) VALUES(?,?,?,?)", (uid, sid, uid, datetime.now(timezone.utc).isoformat()))
                portal.commit()
            result.append(server)
    return result

def user_has_server_data(user_id):
    return get_servers_for_user(user_id)


def login_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if "user" not in session:
            return redirect(url_for("welcome"))
        return f(*args, **kwargs)
    return wrapper


def is_discord_activity_request():
    """判断当前请求是否带有 Discord Embedded Activity 的上下文参数。"""
    return bool(
        request.args.get("frame_id")
        and request.args.get("instance_id")
    )


def server_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if "user" not in session:
            return redirect(url_for("welcome"))
        servers = get_servers_for_user(session["user"]["id"])
        allowed = {str(x["server_id"]) for x in servers}
        sid = current_server_id()
        if not sid or sid not in allowed:
            session.pop("server_id", None)
            if len(servers) == 1:
                session["server_id"] = servers[0]["server_id"]
            elif len(servers) > 1:
                return redirect(url_for("servers"))
            else:
                return redirect(url_for("welcome"))
        return f(*args, **kwargs)
    return wrapper


def admin_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if "user" not in session:
            return redirect(url_for("welcome"))
        if admin_level(session["user"]["id"]) == 0:
            # 管理页不是公开资源；普通登录用户回首页而不是暴露 403 页面。
            return redirect(url_for("index"))
        return f(*args, **kwargs)
    return wrapper


def level1_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if "user" not in session:
            return redirect(url_for("welcome"))
        if admin_level(session["user"]["id"]) != 1:
            return redirect(url_for("admin_panel"))
        return f(*args, **kwargs)
    return wrapper


def whitelist_allowed(user_id):
    return admin_level(user_id) in (1, 2)

def register_server(server_id, name, icon_url=None, owner_user_id=None, db_path=None):
    server_id = str(server_id)
    db_path = db_path or server_db_path(server_id)
    init_server_db(db_path)
    now = datetime.now(timezone.utc).isoformat()
    conn = get_portal_db()
    conn.execute("INSERT INTO servers(server_id,name,icon_url,owner_user_id,db_path,created_at,updated_at) VALUES(?,?,?,?,?,?,?) ON CONFLICT(server_id) DO UPDATE SET name=excluded.name,icon_url=excluded.icon_url,db_path=excluded.db_path,updated_at=excluded.updated_at", (server_id, name or f"Discord Server {server_id}", icon_url, owner_user_id, db_path, now, now))
    conn.commit()


@app.route("/login")
def login():
    if "user" in session:
        servers = get_servers_for_user(session["user"]["id"])
        if len(servers) == 1:
            session["server_id"] = servers[0]["server_id"]
            return redirect(url_for("index"))
        if len(servers) > 1:
            return redirect(url_for("servers"))
        return redirect(url_for("welcome"))
    return render_template("login.html", current_user=None, activity_mode=is_discord_activity_request())


def oauth_config_error():
    if not DISCORD_CLIENT_ID or not DISCORD_CLIENT_ID.isdigit():
        return "Discord OAuth Client ID 未配置或不是数字。请在项目目录的 .env 中填写 DISCORD_CLIENT_ID。"
    if not DISCORD_CLIENT_SECRET:
        return "Discord OAuth Client Secret 未配置。请在项目目录的 .env 中填写 DISCORD_CLIENT_SECRET。"
    return None


def managed_guilds_from_session():
    """Return guilds the logged-in user can manage, as supplied by Discord OAuth."""
    result = []
    for guild in session.get("discord_guilds", []):
        try:
            permissions = int(guild.get("permissions", "0"))
        except (TypeError, ValueError):
            permissions = 0
        # Administrator (0x8) or Manage Guild (0x20).
        if guild.get("owner") or permissions & 0x8 or permissions & 0x20:
            result.append(guild)
    for guild in result:
        icon = guild.get("icon")
        if icon and guild.get("id"):
            ext = "gif" if str(icon).startswith("a_") else "png"
            guild["icon_url"] = f"https://cdn.discordapp.com/icons/{guild.get('id')}/{icon}.{ext}?size=64"
        else:
            guild["icon_url"] = "https://cdn.discordapp.com/embed/avatars/0.png"
    return result


def bot_identity(token):
    """Validate a bot token and return Discord's authoritative bot name."""
    response = requests.get(f"{API_BASE_URL}/users/@me", headers={"Authorization": f"Bot {token}"}, timeout=12)
    response.raise_for_status()
    data = response.json()
    return data.get("global_name") or data.get("username") or "Discord Bot"



def _discord_bot_headers(token):
    return {"Authorization": f"Bot {token}", "User-Agent": "Discord-Analytics-Dashboard/1.0"}


def check_bot_forum_access(token, guild_id, forum_id):
    """Validate that a bot is in the guild and can View Channel + Read Message History on a Forum."""
    headers = _discord_bot_headers(token)
    try:
        me = requests.get(f"{API_BASE_URL}/users/@me", headers=headers, timeout=12)
        if me.status_code in (401, 403):
            return False, "Token 无效或机器人未获准使用此 Token"
        me.raise_for_status()
        bot = me.json()
        bot_id = str(bot.get("id"))

        member = requests.get(f"{API_BASE_URL}/guilds/{guild_id}/members/{bot_id}", headers=headers, timeout=12)
        if member.status_code == 404:
            return False, "机器人不在此服务器"
        if member.status_code in (401, 403):
            return False, "无法读取机器人在服务器中的成员信息"
        member.raise_for_status()
        member_data = member.json()

        channels = requests.get(f"{API_BASE_URL}/guilds/{guild_id}/channels", headers=headers, timeout=12)
        if channels.status_code == 403:
            return False, "机器人无法读取服务器频道列表"
        if channels.status_code == 404:
            return False, "机器人不在此服务器"
        channels.raise_for_status()
        channel = next((c for c in channels.json() if str(c.get("id")) == str(forum_id)), None)
        if not channel:
            return False, "找不到目标 Forum，或机器人无法访问该频道"
        if int(channel.get("type", -1)) != 15:
            return False, "目标频道不是 Forum 频道"

        # Compute effective permissions from @everyone + role + member overwrites.
        roles_resp = requests.get(f"{API_BASE_URL}/guilds/{guild_id}/roles", headers=headers, timeout=12)
        roles_resp.raise_for_status()
        roles = roles_resp.json()
        role_map = {str(r["id"]): r for r in roles}
        everyone = role_map.get(str(guild_id), {})
        permissions = int(everyone.get("permissions", 0))
        member_role_ids = {str(x) for x in member_data.get("roles", [])}
        for rid in member_role_ids:
            permissions |= int(role_map.get(rid, {}).get("permissions", 0))

        ADMINISTRATOR = 0x8
        VIEW_CHANNEL = 0x400
        READ_HISTORY = 0x10000
        if permissions & ADMINISTRATOR:
            return True, "可访问（Administrator）"

        overwrites = channel.get("permission_overwrites") or []
        # Discord permission overwrite algorithm: @everyone, then roles, then member.
        everyone_ow = next((x for x in overwrites if str(x.get("id")) == str(guild_id)), None)
        if everyone_ow:
            permissions &= ~int(everyone_ow.get("deny", 0))
            permissions |= int(everyone_ow.get("allow", 0))

        role_allow = role_deny = 0
        for ow in overwrites:
            if ow.get("type") == 0 and str(ow.get("id")) in member_role_ids and str(ow.get("id")) != str(guild_id):
                role_deny |= int(ow.get("deny", 0))
                role_allow |= int(ow.get("allow", 0))
        permissions &= ~role_deny
        permissions |= role_allow

        member_ow = next((x for x in overwrites if str(x.get("id")) == bot_id and str(x.get("type")) == "1"), None)
        if member_ow:
            permissions &= ~int(member_ow.get("deny", 0))
            permissions |= int(member_ow.get("allow", 0))

        missing=[]
        if not (permissions & VIEW_CHANNEL): missing.append("查看频道")
        if not (permissions & READ_HISTORY): missing.append("读取历史消息")
        if missing:
            return False, "缺少权限：" + "、".join(missing)
        return True, "可访问"
    except requests.RequestException as exc:
        return False, f"Discord API 检查失败：{exc}"


def selected_download_bots(portal, uid, bot_ids, use_default):
    rows = []
    owned = {str(r["id"]): dict(r) for r in portal.execute(
        "SELECT id,name,token FROM download_bots WHERE owner_user_id=? ORDER BY id", (uid,)
    ).fetchall()}
    if len(bot_ids) > 5:
        raise ValueError("最多添加 5 个自定义下载机器人；另外还可以使用 1 个默认下载机器人，因此最多 6 个机器人并行下载")
    if not set(bot_ids).issubset(owned):
        raise PermissionError("只能选择自己的下载机器人")
    for bid in bot_ids:
        rows.append(owned[bid])
    if use_default:
        token = (os.getenv("DISCORD_DOWNLOADER_TOKEN") or os.getenv("DISCORD_DOWNLOADER") or "").strip()
        if not token:
            raise ValueError("已勾选默认下载机器人，但服务器未配置 DISCORD_DOWNLOADER_TOKEN")
        rows.append({"id": "default", "name": "默认下载机器人", "token": token})
    # Avoid duplicate tokens if a custom bot happens to use the same token as default.
    unique=[]; seen=set()
    for row in rows:
        if row["token"] not in seen:
            unique.append(row); seen.add(row["token"])
    return unique

def fetch_discord_user(user_id):
    """Look up a Discord user's authoritative username by ID, same logic as bot_identity():
    don't trust manually typed names, ask Discord directly. Uses any available bot token
    (the site's own default token, falling back to the downloader's), since /users/{id}
    works for any user regardless of shared servers. Returns None if no token is configured
    or the ID doesn't resolve, so callers can fall back to the raw ID."""
    token = (os.getenv("DISCORD_BOT_TOKEN") or os.getenv("DISCORD_DOWNLOADER_TOKEN") or os.getenv("DISCORD_DOWNLOADER") or "").strip()
    if not token:
        return None
    response = requests.get(f"{API_BASE_URL}/users/{user_id}", headers={"Authorization": f"Bot {token}"}, timeout=12)
    if response.status_code == 404:
        return None
    response.raise_for_status()
    data = response.json()
    username = data.get("global_name") or data.get("username")
    if not username:
        return None
    avatar_hash = data.get("avatar")
    avatar_url = f"https://cdn.discordapp.com/avatars/{user_id}/{avatar_hash}.png?size=64" if avatar_hash else None
    return {"username": username, "avatar_url": avatar_url}


def oauth_redirect_uri():
    if PUBLIC_BASE_URL:
        return f"{PUBLIC_BASE_URL}/callback"
    return url_for("callback", _external=True)


@app.route("/auth/discord")
def auth_discord():
    config_error = oauth_config_error()
    if config_error:
        return render_template("login.html", error=config_error, current_user=None), 500
    redirect_uri = oauth_redirect_uri()
    params = urlencode({
        "client_id": DISCORD_CLIENT_ID,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": "identify guilds"
    })
    return redirect(f"{API_BASE_URL}/oauth2/authorize?{params}")


def _discord_rate_limit_info(response):
    """读取 Discord 429 的 JSON/header，兼容 retry_after 是数字或字符串。"""
    body = {}
    try:
        parsed = response.json()
        if isinstance(parsed, dict):
            body = parsed
    except (ValueError, requests.exceptions.JSONDecodeError):
        pass

    raw_retry_after = body.get("retry_after", response.headers.get("Retry-After"))
    try:
        retry_after = max(0.0, float(raw_retry_after))
    except (TypeError, ValueError):
        retry_after = None
    return retry_after, bool(body.get("global", False)), body.get("message")


def _discord_rate_limit_error(response, stage):
    retry_after, is_global, discord_message = _discord_rate_limit_info(response)
    wait_text = f"{retry_after:.1f}" if retry_after is not None else "稍后"
    app.logger.warning(
        "discord.rate_limit request_id=%s stage=%s status=%s retry_after=%s global=%s message=%s",
        _request_id(), stage, response.status_code,
        retry_after if retry_after is not None else "unknown",
        is_global, (discord_message or "")[:200],
    )
    return {
        "error": f"Discord API 当前受到速率限制，请等待 {wait_text} 秒后再登录。请不要重复点击登录。",
        "rate_limited": True,
        "retry_after": retry_after,
        "global": is_global,
        "stage": stage,
    }


def exchange_discord_code(code, redirect_uri=None, source="unknown", fetch_guilds=True):
    """用 OAuth2 授权 code 换取 access_token，拉取用户资料并写入 session。

    普通网页登录（/callback，走完整重定向）和 Discord 活动模式
    （/api/activity/token，走 Embedded App SDK 的 authorize 命令）
    都复用这一个函数。两种登录都验证用户身份，但只有普通网页登录
    需要提前拉取完整的 Discord 服务器列表；Activity 只需要本地门户
    根据 User ID 判断数据权限，避免额外的 Discord API 请求拖慢启动。

    redirect_uri 为 None 时不传给 Discord（活动模式的 authorize 不是重定向流程，
    不需要也不应该带 redirect_uri）。

    返回 (ok: bool, payload: dict)：
    - 成功: (True, {"user": {...}, "access_token": "..."})
    - 失败: (False, {"error": "..."})
    """
    code_fingerprint = hashlib.sha256(code.encode("utf-8")).hexdigest()[:12]
    app.logger.info(
        "oauth.exchange.start request_id=%s source=%s code_fingerprint=%s redirect_uri=%s",
        _request_id(), source, code_fingerprint, bool(redirect_uri),
    )
    try:
        data = {
            "client_id": DISCORD_CLIENT_ID,
            "client_secret": DISCORD_CLIENT_SECRET,
            "grant_type": "authorization_code",
            "code": code,
        }
        if redirect_uri:
            data["redirect_uri"] = redirect_uri
        token_response = requests.post(f"{API_BASE_URL}/oauth2/token", data=data, timeout=20)
        if token_response.status_code == 429:
            return False, _discord_rate_limit_error(token_response, "oauth_token")
        token_response.raise_for_status()
        token = token_response.json()
        access_token = token.get("access_token")
        if not access_token:
            raise RuntimeError(f"Discord Token 响应异常: {token}")
        r = requests.get(f"{API_BASE_URL}/users/@me", headers={"Authorization": f"Bearer {access_token}"}, timeout=20)
        if r.status_code == 429:
            return False, _discord_rate_limit_error(r, "current_user")
        r.raise_for_status()
        u = r.json()
        guilds = []
        if fetch_guilds:
            guild_response = requests.get(f"{API_BASE_URL}/users/@me/guilds", headers={"Authorization": f"Bearer {access_token}"}, timeout=20)
            if guild_response.status_code == 429:
                return False, _discord_rate_limit_error(guild_response, "user_guilds")
            guild_response.raise_for_status()
            guilds = guild_response.json()
        avatar = f"https://cdn.discordapp.com/avatars/{u['id']}/{u['avatar']}.png" if u.get("avatar") else "https://cdn.discordapp.com/embed/avatars/0.png"
        portal = get_portal_db()
        portal.execute("INSERT INTO portal_users(user_id,username,nickname,avatar_url,last_login) VALUES(?,?,?,?,?) ON CONFLICT(user_id) DO UPDATE SET username=excluded.username,nickname=excluded.nickname,avatar_url=excluded.avatar_url,last_login=excluded.last_login", (u["id"], u["username"], u.get("global_name") or u["username"], avatar, datetime.now(timezone.utc).isoformat()))
        portal.commit()
        session.clear()
        session["user"] = {"id": u["id"], "username": u["username"], "avatar": avatar}
        session["discord_guilds"] = guilds
        session.permanent = True
        app.logger.info(
            "oauth.exchange.success request_id=%s source=%s discord_user_id=%s guild_count=%s fetch_guilds=%s",
            _request_id(), source, u.get("id", "-"), len(session["discord_guilds"]), fetch_guilds,
        )
        return True, {"user": session["user"], "access_token": access_token}
    except requests.HTTPError as e:
        detail = e.response.text[:1000] if e.response is not None else str(e)
        app.logger.exception(
            "oauth.exchange.http_error request_id=%s source=%s code_fingerprint=%s status=%s detail=%s",
            _request_id(), source, code_fingerprint,
            e.response.status_code if e.response is not None else "unknown", detail,
        )
        return False, {"error": f"Discord OAuth 登录失败: {detail}"}
    except Exception as e:
        app.logger.exception(
            "oauth.exchange.error request_id=%s source=%s code_fingerprint=%s error=%s",
            _request_id(), source, code_fingerprint, e,
        )
        return False, {"error": f"登录失败: {e}"}


@app.route("/callback")
def callback():
    config_error = oauth_config_error()
    if config_error:
        return render_template("login.html", error=config_error, current_user=None), 500
    error_code = request.args.get("error")
    if error_code:
        description = request.args.get("error_description") or error_code
        return render_template("login.html", error=f"Discord 登录被取消或失败: {description}", current_user=None), 400
    code = request.args.get("code")
    if not code:
        return render_template("login.html", error="Discord 没有返回授权 code，请重新登录。", current_user=None), 400

    ok, payload = exchange_discord_code(code, redirect_uri=oauth_redirect_uri(), source="web_oauth")
    if not ok:
        return render_template("login.html", error=payload["error"], current_user=None), 429 if payload.get("rate_limited") else 400

    servers = get_servers_for_user(payload["user"]["id"])
    if len(servers) == 1:
        session["server_id"] = servers[0]["server_id"]
        return redirect(url_for("index"))
    if len(servers) > 1:
        return redirect(url_for("servers"))
    return redirect(url_for("my_profile"))


@app.route("/api/activity/token", methods=["POST"])
def activity_token():
    """Discord 活动模式（Activities / 小活动）专用登录接口。

    网页在 Discord 客户端内以 iframe 形式运行时，不能走普通的整页跳转 OAuth，
    而是用 Embedded App SDK 的 discordSdk.commands.authorize() 拿到一次性 code，
    再由前端 POST 到这个接口，在服务器端（可以安全使用 client_secret）换取
    access_token 并写入 Flask session，实现"无需在浏览器打开也能登录"。

    参考：static/js/discord-activity.js
    """
    config_error = oauth_config_error()
    if config_error:
        return jsonify({"ok": False, "error": config_error}), 500

    body = request.get_json(silent=True) or {}
    code = (body.get("code") or "").strip()
    if not code:
        app.logger.warning("activity.token.invalid request_id=%s reason=missing_code", _request_id())
        return jsonify({"ok": False, "error": "缺少 code 参数"}), 400

    # 活动模式下的 authorize() 不是重定向流程，不传 redirect_uri。
    app.logger.info("activity.token.start request_id=%s", _request_id())
    # Activity 登录只需确认 Discord User ID。服务器数据权限由门户本地
    # 数据库判断；不要像浏览器 OAuth 一样再请求完整 guild 列表。
    ok, payload = exchange_discord_code(
        code, redirect_uri=None, source="activity", fetch_guilds=False
    )
    if not ok:
        app.logger.warning(
            "activity.token.failed request_id=%s rate_limited=%s stage=%s",
            _request_id(), payload.get("rate_limited", False), payload.get("stage", "unknown"),
        )
        result = {"ok": False, "error": payload["error"]}
        for key in ("rate_limited", "retry_after", "global", "stage"):
            if key in payload:
                result[key] = payload[key]
        return jsonify(result), 429 if payload.get("rate_limited") else 400

    app.logger.info("activity.token.success request_id=%s discord_user_id=%s", _request_id(), payload["user"].get("id", "-"))
    return jsonify({
        "ok": True,
        "user": payload["user"],
        # 前端需要这个 access_token 调用 discordSdk.commands.authenticate()，
        # 完成 Embedded App SDK 侧的认证握手；仅在响应中一次性返回，不落库。
        "access_token": payload["access_token"],
    })


@app.route("/api/activity/status")
def activity_status():
    """确认 Activity 代理是否实际保存了 Flask session cookie。"""
    user = session.get("user")
    authenticated = bool(user and user.get("id"))
    app.logger.info(
        "activity.status request_id=%s authenticated=%s discord_user_id=%s",
        _request_id(), authenticated, user.get("id", "-") if authenticated else "-",
    )
    return jsonify({"ok": True, "authenticated": authenticated})


@app.route("/api/activity/log", methods=["POST"])
def activity_log():
    """接收 Activity 前端的脱敏诊断事件，便于 Discord iframe 内排障。"""
    if request.content_length and request.content_length > 16 * 1024:
        return jsonify({"ok": False, "error": "日志请求过大"}), 413
    body = request.get_json(silent=True) or {}
    event = str(body.get("event") or "unknown")[:80]
    if not re.fullmatch(r"[A-Za-z0-9_.:-]+", event):
        event = "invalid_event"
    client_request_id = str(body.get("request_id") or "-")[:80]
    if not re.fullmatch(r"[A-Za-z0-9._:-]{1,80}", client_request_id):
        client_request_id = "-"
    details = body.get("details")
    if not isinstance(details, dict):
        details = {"value": str(details)[:500]} if details is not None else {}
    # 前端只应发送诊断字段；截断并序列化后再写入日志，避免日志被任意内容撑爆。
    try:
        details_text = json.dumps(details, ensure_ascii=False, separators=(",", ":"))[:2000]
    except (TypeError, ValueError):
        details_text = "{}"
    app.logger.info(
        "activity.client event=%s request_id=%s client_request_id=%s details=%s",
        event, _request_id(), client_request_id, details_text,
    )
    return jsonify({"ok": True})


@app.route("/me")
@login_required
def my_profile():
    servers = get_servers_for_user(session["user"]["id"])
    if len(servers) == 1:
        session["server_id"] = servers[0]["server_id"]
        return redirect(url_for("user_profile", user_id=session["user"]["id"]))
    if len(servers) > 1:
        return redirect(url_for("servers"))
    return render_template("welcome.html", can_upload=whitelist_allowed(session["user"]["id"]), current_user=session["user"], no_data=True)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("welcome"))


@app.route("/welcome")
def welcome():
    current_user = session.get("user")
    if not current_user and is_discord_activity_request():
        return redirect(url_for("login", **request.args.to_dict()))
    if current_user:
        servers = get_servers_for_user(current_user["id"])
        if len(servers) == 1:
            return redirect(url_for("index"))
        if len(servers) > 1:
            return redirect(url_for("servers"))
        can_upload = whitelist_allowed(current_user["id"])
    else:
        can_upload = False
    return render_template("welcome.html", can_upload=can_upload, current_user=current_user, no_data=bool(current_user and not servers))


@app.route("/upload-json", methods=["POST"])
@login_required
def upload_json():
    uid = str(session["user"]["id"])
    if not whitelist_allowed(uid):
        return "你没有 JSON 导入权限", 403
    file = request.files.get("json_file")
    if not file or not file.filename.lower().endswith(".json"):
        return "请选择 JSON 文件", 400
    temp = os.path.join(BASE_DIR, "data", "uploads", f"{uid}_{os.urandom(8).hex()}.json")
    try:
        file.save(temp)
        meta = inspect_json(temp)
        sid = str(meta["server_id"])
        target = server_db_path(sid)
        # 权限 2 用户只能新建/管理自己拥有访问权的服务器，且下载服务器数量受 quota 限制。
        existing = get_portal_db().execute("SELECT 1 FROM servers WHERE server_id=?", (sid,)).fetchone()
        if not existing and admin_level(uid) == 2:
            used = get_portal_db().execute("SELECT count(*) FROM user_server_access WHERE user_id=?", (uid,)).fetchone()[0]
            if used >= get_download_quota(uid):
                return "已达到服务器配额，请联系权限1管理员增加配额", 403
        import_json_to_db(temp, target, server_id=sid)
        register_server(sid, meta.get("server_name") or f"Discord Server {sid}", meta.get("icon_url"), owner_user_id=uid, db_path=target)
        get_portal_db().execute("INSERT OR IGNORE INTO user_server_access(user_id,server_id,granted_by,created_at) VALUES(?,?,?,?)", (uid, sid, uid, datetime.now(timezone.utc).isoformat()))
        get_portal_db().commit()
        sync_server_users_to_portal(sid)
        session["server_id"] = sid
        return redirect(url_for("index"))
    except Exception as e:
        app.logger.exception("JSON import failed")
        return render_template("welcome.html", can_upload=True, current_user=session["user"], error=f"导入失败: {e}"), 400
    finally:
        if os.path.exists(temp):
            try:
                os.remove(temp)
            except OSError as exc:
                app.logger.warning("清理 JSON 临时文件失败 path=%s error=%s", temp, exc)


@app.route("/servers")
@login_required
def servers():
    rows = get_servers_for_user(session["user"]["id"])
    return render_template("servers.html", servers=rows, current_user=session["user"])


@app.route("/server/<server_id>")
@login_required
def select_server(server_id):
    allowed = {str(x["server_id"]) for x in get_servers_for_user(session["user"]["id"])}
    if str(server_id) not in allowed:
        return "403 Access Denied", 403
    session["server_id"] = str(server_id)
    return redirect(url_for("index"))


@app.route("/")
def index():
    if "user" not in session:
        if is_discord_activity_request():
            return redirect(url_for("login", **request.args.to_dict()))
        return redirect(url_for("welcome"))
    servers = get_servers_for_user(session["user"]["id"])
    if not current_server_id():
        if len(servers) == 1:
            session["server_id"] = servers[0]["server_id"]
        elif len(servers) > 1:
            return redirect(url_for("servers"))
        else:
            return redirect(url_for("welcome"))
    elif str(current_server_id()) not in {str(x["server_id"]) for x in servers}:
        session.pop("server_id", None)
        if len(servers) == 1:
            session["server_id"] = servers[0]["server_id"]
        elif len(servers) > 1:
            return redirect(url_for("servers"))
        else:
            return redirect(url_for("welcome"))
    sid = current_server_id()
    conn = get_db()
    u = session["user"]
    now = datetime.now(timezone.utc).isoformat()
    conn.execute("INSERT INTO web_visitors(user_id,username,nickname,avatar_url,last_visit) VALUES(?,?,?,?,?) ON CONFLICT(user_id) DO UPDATE SET username=excluded.username,nickname=excluded.nickname,avatar_url=excluded.avatar_url,last_visit=excluded.last_visit", (u["id"], u["username"], u["username"], u["avatar"], now))
    conn.commit()
    data = data_engine.load_or_compute(sid)
    visitors = conn.execute("SELECT * FROM web_visitors ORDER BY last_visit DESC").fetchall()
    full_leaderboard = []
    for row in conn.execute("SELECT u.*,count(m.message_id) msg_count FROM users u JOIN messages m ON u.user_id=m.author_id GROUP BY u.user_id ORDER BY msg_count DESC LIMIT 50"):
        d = dict(row)
        d["top_emojis"] = [dict(x) for x in conn.execute("SELECT r.emoji_url,r.emoji_name,count(*) c FROM reactions r JOIN messages m ON r.message_id=m.message_id WHERE m.author_id=? GROUP BY r.emoji_name ORDER BY c DESC LIMIT 3", (row["user_id"],))]
        full_leaderboard.append(d)
    server = get_portal_db().execute("SELECT * FROM servers WHERE server_id=?", (sid,)).fetchone()
    return render_template("index.html", server=server, server_id=sid, current_user=u, site_visitors=visitors, full_leaderboard=full_leaderboard, **data)


@app.route("/api/leaderboard")
@server_required
def api_leaderboard():
    page = max(1, int(request.args.get("page", 1)))
    offset = (page - 1) * 50
    conn = get_db()
    users = []
    for i, row in enumerate(conn.execute("SELECT u.*,count(m.message_id) msg_count FROM users u JOIN messages m ON u.user_id=m.author_id GROUP BY u.user_id ORDER BY msg_count DESC LIMIT 50 OFFSET ?", (offset,))):
        d = dict(row)
        d["rank"] = offset + i + 1
        d["top_emojis"] = [dict(x) for x in conn.execute("SELECT emoji_url,emoji_name,count(*) c FROM reactions r JOIN messages m ON r.message_id=m.message_id WHERE m.author_id=? GROUP BY emoji_name ORDER BY c DESC LIMIT 3", (row["user_id"],))]
        users.append(d)
    return jsonify(users)


@app.route("/search")
@server_required
def search():
    query = request.args.get("q", "").strip()
    conn = get_db()
    results = conn.execute("SELECT * FROM users WHERE user_id=? OR username LIKE ? OR nickname LIKE ? LIMIT 20", (query, f"%{query}%", f"%{query}%")).fetchall()
    data = data_engine.load_or_compute(current_server_id())
    return render_template("index.html", server_id=current_server_id(), current_user=session["user"], search_results=results, query=query, site_visitors=[], full_leaderboard=[], **data)


@app.route("/user/<user_id>")
@server_required
def user_profile(user_id):
    sid = current_server_id()
    conn = get_db()
    merge = conn.execute("SELECT parent_id FROM user_merges WHERE target_id=?", (str(user_id),)).fetchone()
    if merge:
        return redirect(url_for("user_profile", user_id=merge["parent_id"]))
    user = conn.execute("SELECT * FROM users WHERE user_id=?", (user_id,)).fetchone()
    if not user:
        return "User Not Found", 404
    visitor = session["user"]
    if visitor["id"] != str(user_id):
        conn.execute("INSERT INTO profile_views(target_user_id,viewer_user_id,viewer_name,viewer_avatar,timestamp) VALUES(?,?,?,?,?) ON CONFLICT(target_user_id,viewer_user_id) DO UPDATE SET timestamp=excluded.timestamp", (user_id, visitor["id"], visitor["username"], visitor["avatar"], datetime.now(timezone.utc).isoformat()))
        conn.commit()
    view_count = conn.execute("SELECT count(*) FROM profile_views WHERE target_user_id=?", (user_id,)).fetchone()[0]
    recent_viewers = conn.execute("SELECT viewer_name,viewer_avatar,timestamp FROM profile_views WHERE target_user_id=? ORDER BY timestamp DESC LIMIT 20", (user_id,)).fetchall()
    merged_ids = data_engine.get_merged_ids(sid, user_id)
    ph = ",".join("?" for _ in merged_ids)
    msg_count = conn.execute(f"SELECT count(DISTINCT message_id) FROM messages WHERE author_id IN ({ph})", merged_ids).fetchone()[0]
    reaction_received_count = conn.execute(f"SELECT count(*) FROM reactions r JOIN messages m ON r.message_id=m.message_id WHERE m.author_id IN ({ph})", merged_ids).fetchone()[0]
    sort_by = request.args.get("sort", "hot")
    page = max(1, int(request.args.get("page", 1)))
    offset = (page - 1) * ITEMS_PER_PAGE
    order = "ORDER BY total_reactions DESC,m.timestamp DESC" if sort_by == "hot" else "ORDER BY m.timestamp DESC"
    messages = process_messages(conn, conn.execute(f"SELECT m.*,t.name thread_name,(SELECT count(*) FROM reactions WHERE message_id=m.message_id) total_reactions FROM messages m JOIN threads t ON m.thread_id=t.thread_id WHERE m.author_id IN ({ph}) {order} LIMIT ? OFFSET ?", (*merged_ids, ITEMS_PER_PAGE, offset)).fetchall())
    thread_count = conn.execute(f"SELECT count(DISTINCT thread_id) FROM messages WHERE author_id IN ({ph})", merged_ids).fetchone()[0]
    my_threads = []
    for row in conn.execute(f"SELECT t.thread_id,t.name,m.timestamp created_at,(SELECT count(*) FROM messages WHERE thread_id=t.thread_id) reply_count,(SELECT content FROM messages WHERE thread_id=t.thread_id ORDER BY timestamp LIMIT 1) first_content,(SELECT message_id FROM messages WHERE thread_id=t.thread_id ORDER BY timestamp LIMIT 1) op_msg_id FROM threads t JOIN messages m ON t.thread_id=m.thread_id WHERE m.author_id IN ({ph}) AND m.timestamp=(SELECT min(timestamp) FROM messages WHERE thread_id=t.thread_id) ORDER BY reply_count DESC LIMIT ? OFFSET ?", (*merged_ids, ITEMS_PER_PAGE, offset)):
        d = dict(row); d["op_user"] = dict(user)
        emoji = conn.execute("SELECT emoji_url,count(*) c FROM reactions WHERE message_id=? GROUP BY emoji_name ORDER BY c DESC LIMIT 1", (d["op_msg_id"],)).fetchone()
        d["top_emoji_url"] = emoji["emoji_url"] if emoji else None
        d["top_emoji_count"] = emoji["c"] if emoji else 0
        my_threads.append(d)
    top_emojis_given = [dict(r) for r in conn.execute(f"SELECT emoji_url,emoji_name,count(*) c FROM reactions WHERE user_id IN ({ph}) GROUP BY emoji_name ORDER BY c DESC LIMIT 3", merged_ids)]
    top_emojis_received = [dict(r) for r in conn.execute(f"SELECT r.emoji_url,r.emoji_name,count(*) c FROM reactions r JOIN messages m ON r.message_id=m.message_id WHERE m.author_id IN ({ph}) GROUP BY r.emoji_name ORDER BY c DESC LIMIT 3", merged_ids)]
    incoming = [dict(r) for r in conn.execute(
        f"SELECT u.user_id,u.nickname,u.username,u.avatar_url,count(*) score FROM (SELECT author_id source_id FROM messages WHERE message_id IN (SELECT message_id FROM messages WHERE author_id IN ({ph})) UNION ALL SELECT r.user_id source_id FROM reactions r JOIN messages m ON r.message_id=m.message_id WHERE m.author_id IN ({ph})) raw JOIN users u ON raw.source_id=u.user_id WHERE u.user_id NOT IN ({ph}) GROUP BY u.user_id ORDER BY score DESC LIMIT 5",
        (*merged_ids, *merged_ids, *merged_ids))]
    outgoing = [dict(r) for r in conn.execute(
        f"SELECT u.user_id,u.nickname,u.username,u.avatar_url,count(*) score FROM (SELECT mentioned_user_id target_id FROM mentions WHERE author_id IN ({ph}) UNION ALL SELECT m.author_id target_id FROM reactions r JOIN messages m ON r.message_id=m.message_id WHERE r.user_id IN ({ph})) raw JOIN users u ON raw.target_id=u.user_id WHERE u.user_id NOT IN ({ph}) GROUP BY u.user_id ORDER BY score DESC LIMIT 5",
        (*merged_ids, *merged_ids, *merged_ids))]
    daily = [dict(r) for r in conn.execute(f"SELECT substr(timestamp,1,10) day,count(*) c FROM messages WHERE author_id IN ({ph}) GROUP BY day ORDER BY day", merged_ids)]
    hourly = {r["hour"]: r["c"] for r in conn.execute(f"SELECT strftime('%H',timestamp) hour,count(*) c FROM messages WHERE author_id IN ({ph}) GROUP BY hour", merged_ids)}
    hours = [0] * 24
    for h, c in hourly.items():
        if h is not None: hours[(int(h) + 8) % 24] += c
    chart_hourly = [{"hour": f"{i:02d}:00", "c": v} for i, v in enumerate(hours)]
    text_list = [r[0] for r in conn.execute(f"SELECT content FROM messages WHERE author_id IN ({ph}) ORDER BY timestamp DESC LIMIT 2000", merged_ids)]
    return render_template("user.html", user=user, messages=messages, my_threads=my_threads, view_count=view_count, recent_viewers=recent_viewers, server_id=sid, current_sort=sort_by, current_page=page, total_msg_pages=math.ceil(msg_count / ITEMS_PER_PAGE), total_thread_pages=math.ceil(thread_count / ITEMS_PER_PAGE), current_user=session["user"], msg_count=msg_count, reaction_received_count=reaction_received_count, top_emojis_given=top_emojis_given, top_emojis_received=top_emojis_received, interactions_incoming=incoming, interactions_outgoing=outgoing, chart_daily=daily, chart_hourly=chart_hourly, word_cloud_data=format_word_cloud(get_word_cloud_counter(text_list), 50))


@app.route("/claim_account", methods=["POST"])
@server_required
def claim_account():
    target_id = request.form.get("target_id", "").strip()
    requester_id = str(session["user"]["id"])
    conn = get_db()
    target = conn.execute("SELECT nickname FROM users WHERE user_id=?", (target_id,)).fetchone()
    if not target or target_id == requester_id:
        return "无效请求", 400
    try:
        conn.execute("INSERT INTO claim_requests_v2(requester_id,target_id,target_name,created_at) VALUES(?,?,?,?)", (requester_id, target_id, target["nickname"], datetime.now(timezone.utc).isoformat()))
        conn.commit(); flash("认领申请已提交，请等待管理员审核。")
    except sqlite3.IntegrityError:
        flash("申请已存在")
    return redirect(url_for("user_profile", user_id=target_id))


@app.route("/admin")
@login_required
@admin_required
def admin_panel():
    portal = get_portal_db(); sid = current_server_id()
    conn = get_db() if sid and os.path.exists(server_db_path(sid)) else None
    reqs = conn.execute("SELECT r.*,u.username req_name,u.avatar_url req_avatar FROM claim_requests_v2 r LEFT JOIN users u ON r.requester_id=u.user_id WHERE r.status=0").fetchall() if conn else []
    merges = conn.execute("SELECT m.*,u.username parent_name FROM user_merges m LEFT JOIN users u ON m.parent_id=u.user_id").fetchall() if conn else []
    whitelist = portal.execute("SELECT w.*,COALESCE(q.quota,1) quota FROM whitelist_users w LEFT JOIN server_download_quota q ON q.user_id=w.user_id ORDER BY w.created_at DESC").fetchall()
    all_servers = portal.execute("SELECT * FROM servers ORDER BY name").fetchall()
    level = admin_level(session["user"]["id"]); uid = str(session["user"]["id"])
    if level == 1:
        servers = all_servers
        accesses = portal.execute("SELECT a.*,w.username FROM user_server_access a LEFT JOIN whitelist_users w ON w.user_id=a.user_id ORDER BY a.user_id,a.server_id").fetchall()
        cfg_rows = portal.execute("SELECT c.*,s.name server_display_name,s.icon_url FROM download_configs c LEFT JOIN servers s ON s.server_id=c.guild_id WHERE c.enabled=1 ORDER BY COALESCE(s.name,c.guild_name),c.forum_name,c.id").fetchall()
        tasks = portal.execute("SELECT t.*,COALESCE(p.username,t.created_by) creator_name,COALESCE(c.guild_name,s.name,t.guild_id) server_display_name,COALESCE(c.forum_name,t.forum_name,t.forum_channel_id) forum_display_name FROM download_tasks t LEFT JOIN portal_users p ON p.user_id=t.created_by LEFT JOIN download_configs c ON c.id=t.config_id LEFT JOIN servers s ON s.server_id=t.guild_id ORDER BY t.id DESC LIMIT 100").fetchall()
    else:
        allowed = {str(x["server_id"]) for x in get_servers_for_user(uid)}
        servers = [x for x in all_servers if str(x["server_id"]) in allowed]
        accesses = portal.execute("SELECT a.*,w.username FROM user_server_access a LEFT JOIN whitelist_users w ON w.user_id=a.user_id WHERE a.user_id=? ORDER BY a.server_id", (uid,)).fetchall()
        cfg_rows = portal.execute("SELECT c.*,s.name server_display_name,s.icon_url FROM download_configs c LEFT JOIN servers s ON s.server_id=c.guild_id WHERE c.enabled=1 AND c.owner_user_id=? ORDER BY COALESCE(s.name,c.guild_name),c.forum_name,c.id", (uid,)).fetchall()
        cfg_ids = [int(x["id"]) for x in cfg_rows]
        if cfg_ids:
            ph = ','.join('?' * len(cfg_ids))
            tasks = portal.execute(f"SELECT t.*,COALESCE(p.username,t.created_by) creator_name,COALESCE(c.guild_name,s.name,t.guild_id) server_display_name,COALESCE(c.forum_name,t.forum_name,t.forum_channel_id) forum_display_name FROM download_tasks t LEFT JOIN portal_users p ON p.user_id=t.created_by LEFT JOIN download_configs c ON c.id=t.config_id LEFT JOIN servers s ON s.server_id=t.guild_id WHERE t.created_by=? OR t.config_id IN ({ph}) ORDER BY t.id DESC LIMIT 100", (uid,*cfg_ids)).fetchall()
        else:
            tasks = portal.execute("SELECT t.*,COALESCE(p.username,t.created_by) creator_name,COALESCE(c.guild_name,s.name,t.guild_id) server_display_name,COALESCE(c.forum_name,t.forum_name,t.forum_channel_id) forum_display_name FROM download_tasks t LEFT JOIN portal_users p ON p.user_id=t.created_by LEFT JOIN download_configs c ON c.id=t.config_id LEFT JOIN servers s ON s.server_id=t.guild_id WHERE t.created_by=? ORDER BY t.id DESC LIMIT 100", (uid,)).fetchall()
    bot_rows = portal.execute("SELECT id,name,owner_user_id,created_at FROM download_bots WHERE owner_user_id=? ORDER BY id", (uid,)).fetchall()
    bot_map = {str(row["id"]): row["name"] for row in portal.execute("SELECT id,name FROM download_bots")}
    linked = collections.defaultdict(list)
    for row in portal.execute("SELECT config_id,bot_id FROM download_config_bots"):
        linked[int(row["config_id"])].append(bot_map.get(str(row["bot_id"]), f"机器人 #{row['bot_id']}"))
    guild_icon_map = {str(g.get("id")): g.get("icon_url") for g in managed_guilds_from_session()}
    configs = []
    for row in cfg_rows:
        d = dict(row)
        d["icon_url"] = d.get("icon_url") or guild_icon_map.get(str(row["guild_id"]))
        d["bot_names"] = linked.get(int(row["id"]), [])
        if row["use_default_bot"]:
            d["bot_names"].append("默认下载机器人")
        configs.append(d)
    return render_template("admin.html", pending_requests=reqs, active_merges=merges, whitelist=whitelist, servers=servers, accesses=accesses, bots=bot_rows, download_servers=configs, tasks=tasks, admin_level=level, current_user=session["user"], user_quota=get_download_quota(uid))

def _admin_server_scope():
    uid = str(session["user"]["id"])
    level = admin_level(uid)
    if level == 1:
        return None
    sid = current_server_id()
    if not sid:
        return "", 403
    allowed = {str(x["server_id"]) for x in get_servers_for_user(uid)}
    if sid not in allowed:
        return "403 Access Denied", 403
    return sid


@app.route("/admin/approve/<int:req_id>")
@login_required
@admin_required
def admin_approve(req_id):
    scope = _admin_server_scope()
    if isinstance(scope, tuple):
        return scope
    sid = scope
    conn = get_db()
    req = conn.execute("SELECT * FROM claim_requests_v2 WHERE id=? AND status=0", (req_id,)).fetchone()
    if req:
        conn.execute("UPDATE claim_requests_v2 SET status=1 WHERE id=?", (req_id,))
        conn.execute("INSERT OR REPLACE INTO user_merges(target_id,parent_id,created_at) VALUES(?,?,?)", (req["target_id"], req["requester_id"], datetime.now(timezone.utc).isoformat()))
        conn.commit()
    return redirect(url_for("admin_panel"))


@app.route("/admin/unmerge/<target_id>")
@login_required
@admin_required
def admin_unmerge(target_id):
    scope = _admin_server_scope()
    if isinstance(scope, tuple):
        return scope
    conn = get_db(); conn.execute("DELETE FROM user_merges WHERE target_id=?", (target_id,)); conn.commit(); return redirect(url_for("admin_panel"))


@app.route("/admin/reset_all_claims")
@login_required
@admin_required
def admin_reset_all():
    scope = _admin_server_scope()
    if isinstance(scope, tuple):
        return scope
    conn = get_db(); conn.execute("DELETE FROM claim_requests_v2"); conn.execute("DELETE FROM user_merges"); conn.commit(); return redirect(url_for("admin_panel"))


@app.route("/admin/whitelist/add", methods=["POST"])
@login_required
@level1_required
def admin_whitelist_add():
    uid = str(request.form.get("user_id", "")).strip()
    if not uid.isdigit(): return "无效 Discord User ID", 400
    portal = get_portal_db()
    # 与 bot_identity() 相同的原则：不使用手动填写的名称，用 Discord API 权威查询，
    # 避免用户名和真实 Discord 账号对不上；查询失败（未配置 token 或用户不存在）时
    # 先用 ID 占位，不阻断白名单添加。
    try:
        info = fetch_discord_user(uid)
    except requests.RequestException as exc:
        app.logger.warning("查询白名单用户信息失败 user_id=%s，使用 ID 占位 error=%s", uid, exc)
        info = None
    name = info["username"] if info else uid
    portal.execute("INSERT OR IGNORE INTO whitelist_users(user_id,username,added_by,created_at) VALUES(?,?,?,?)", (uid, name, session["user"]["id"], datetime.now(timezone.utc).isoformat()))
    portal.execute("INSERT OR IGNORE INTO server_download_quota(user_id,quota) VALUES(?,1)", (uid,))
    portal.commit()
    return redirect(url_for("admin_panel"))

@app.route("/admin/whitelist/delete/<user_id>", methods=["POST"])
@login_required
@level1_required
def admin_whitelist_delete(user_id):
    portal=get_portal_db(); portal.execute("DELETE FROM whitelist_users WHERE user_id=?", (str(user_id),)); portal.execute("DELETE FROM server_download_quota WHERE user_id=?", (str(user_id),)); portal.execute("DELETE FROM user_server_access WHERE user_id=?", (str(user_id),)); portal.commit(); return redirect(url_for("admin_panel"))

@app.route("/admin/quota", methods=["POST"])
@login_required
@level1_required
def admin_quota():
    uid=str(request.form.get("user_id","")); quota=_parse_clamped_int(request.form.get("quota","1"), 1, 1, 100)
    if quota is None: return "配额必须是数字", 400
    portal=get_portal_db(); portal.execute("INSERT INTO server_download_quota(user_id,quota) VALUES(?,?) ON CONFLICT(user_id) DO UPDATE SET quota=excluded.quota",(uid,quota)); portal.commit(); return redirect(url_for("admin_panel"))

@app.route("/admin/access", methods=["POST"])
@login_required
@level1_required
def admin_access():
    uid=str(request.form.get("user_id","")); sid=str(request.form.get("server_id",""))
    if uid and sid and get_portal_db().execute("SELECT 1 FROM servers WHERE server_id=?",(sid,)).fetchone():
        p=get_portal_db(); p.execute("INSERT OR IGNORE INTO user_server_access(user_id,server_id,granted_by,created_at) VALUES(?,?,?,?)",(uid,sid,session["user"]["id"],datetime.now(timezone.utc).isoformat())); p.commit()
    return redirect(url_for("admin_panel"))

@app.route("/admin/access/delete", methods=["POST"])
@login_required
@level1_required
def admin_access_delete():
    p=get_portal_db(); p.execute("DELETE FROM user_server_access WHERE user_id=? AND server_id=?",(str(request.form.get("user_id")),str(request.form.get("server_id")))); p.commit(); return redirect(url_for("admin_panel"))

@app.route("/admin/bot/add", methods=["POST"])
@login_required
@admin_required
def admin_bot_add():
    p=get_portal_db(); owner=str(session["user"]["id"]); count=p.execute("SELECT count(*) FROM download_bots WHERE owner_user_id=?",(owner,)).fetchone()[0]
    if count>=5: return "最多添加 5 个下载机器人",400
    token=request.form.get("token","").strip()
    if not token: return "Token 不能为空",400
    try:
        # 不相信手动填写的名称，避免后续配置显示错误机器人。
        name=bot_identity(token)
    except requests.RequestException:
        return "无法验证机器人密钥，请确认 Token 有效后重试",400
    p.execute("INSERT INTO download_bots(owner_user_id,name,token,created_at) VALUES(?,?,?,?)",(owner,name,token,datetime.now(timezone.utc).isoformat())); p.commit(); return redirect(url_for("admin_panel"))

@app.route("/admin/bot/delete/<int:bot_id>", methods=["POST"])
@login_required
@admin_required
def admin_bot_delete(bot_id):
    p=get_portal_db(); owner=str(session["user"]["id"])
    if p.execute("SELECT 1 FROM download_bots WHERE id=? AND owner_user_id=?",(bot_id,owner)).fetchone():
        p.execute("DELETE FROM download_server_bots WHERE bot_id=?",(bot_id,))
        p.execute("DELETE FROM download_config_bots WHERE bot_id=?",(bot_id,))
        p.execute("UPDATE download_servers SET bot_id=NULL WHERE bot_id=?",(bot_id,))
        p.execute("DELETE FROM download_bots WHERE id=?",(bot_id,))
        p.commit()
    return redirect(url_for("admin_panel"))

@app.route("/admin/download-server", methods=["POST"])
@login_required
@admin_required
def admin_download_server():
    p=get_portal_db(); uid=str(session["user"]["id"])
    guild=str(request.form.get("guild_id","")).strip(); forum=str(request.form.get("forum_channel_id","")).strip()
    bot_ids=[str(x) for x in request.form.getlist("bot_ids") if str(x).isdigit()]; use_default=request.form.get("use_default_bot")=="1"
    scheduler_interval=_parse_clamped_int(request.form.get("scheduler_interval"), 250, 50, 60000)
    download_interval_ms=_parse_clamped_int(request.form.get("download_interval_ms"), 0, 0, 60000)
    if scheduler_interval is None: return "调度间隔必须是数字", 400
    if download_interval_ms is None: return "下载间隔必须是数字", 400
    update_enabled=1 if request.form.get("update_enabled")=="1" else 0
    if not guild.isdigit() or not forum.isdigit(): return "Guild ID 和 Forum Channel ID 必须是数字",400
    if admin_level(uid)==2:
        allowed={str(x.get("id")) for x in managed_guilds_from_session()}
        if guild not in allowed: return "你只能配置自己有管理权限的服务器",403
    if not bot_ids and not use_default:
        flash("请至少选择一个下载机器人或默认下载机器人", "error"); return redirect(url_for("admin_panel"))
    try:
        selected=selected_download_bots(p,uid,bot_ids,use_default)
    except (ValueError,PermissionError) as exc:
        flash(str(exc),"error"); return redirect(url_for("admin_panel"))
    failures=[]
    for bot in selected:
        ok,reason=check_bot_forum_access(bot["token"],guild,forum)
        if not ok: failures.append(f"{bot['name']}：{reason}")
    if failures:
        flash("下载配置未保存。以下机器人没有目标 Forum 的访问权限：\n"+"\n".join(failures),"error"); return redirect(url_for("admin_panel"))
    managed={str(x.get("id")):x for x in managed_guilds_from_session()}
    guild_name=(managed.get(guild) or {}).get("name") or (p.execute("SELECT name FROM servers WHERE server_id=?",(guild,)).fetchone() or {"name":guild})["name"]
    forum_name=forum
    try:
        # 由用户的 OAuth guild/channel 资源获取权威 Forum 名称。
        for bot in selected[:1]:
            resp=requests.get(f"{API_BASE_URL}/guilds/{guild}/channels",headers={"Authorization":f"Bot {bot['token']}"},timeout=12)
            if resp.ok:
                ch=next((x for x in resp.json() if str(x.get("id"))==forum),None)
                if ch: forum_name=ch.get("name") or forum
                break
    except requests.RequestException as exc:
        app.logger.warning("查询 Forum 名称失败 guild_id=%s forum_id=%s，使用 ID 占位 error=%s", guild, forum, exc)
    server_key=f"{guild}:{forum}"
    p.execute("INSERT INTO download_configs(server_id,owner_user_id,guild_id,forum_channel_id,guild_name,forum_name,enabled,use_default_bot,scheduler_interval,download_interval_ms,update_enabled,updated_at) VALUES(?,?,?,?,?,?,1,?,?,?,?,?) ON CONFLICT(guild_id,forum_channel_id) DO UPDATE SET owner_user_id=excluded.owner_user_id,server_id=excluded.server_id,guild_name=excluded.guild_name,forum_name=excluded.forum_name,enabled=1,use_default_bot=excluded.use_default_bot,scheduler_interval=excluded.scheduler_interval,download_interval_ms=excluded.download_interval_ms,update_enabled=excluded.update_enabled,updated_at=excluded.updated_at",(server_key,uid,guild,forum,guild_name,forum_name,1 if use_default else 0,scheduler_interval,download_interval_ms,update_enabled,datetime.now(timezone.utc).isoformat()))
    cfg=p.execute("SELECT id FROM download_configs WHERE guild_id=? AND forum_channel_id=?",(guild,forum)).fetchone(); cfg_id=int(cfg["id"])
    p.execute("DELETE FROM download_config_bots WHERE config_id=?",(cfg_id,))
    p.executemany("INSERT INTO download_config_bots(config_id,bot_id) VALUES(?,?)",[(cfg_id,x["id"]) for x in selected if x["id"]!="default"])
    p.commit(); flash(f"下载配置已保存：{guild_name} / {forum_name}，{len(selected)} 个机器人。","success")
    return redirect(url_for("admin_panel"))

@app.route("/admin/download-server/delete/<int:config_id>",methods=["POST"])
@login_required
@admin_required
def admin_download_server_delete(config_id):
    p=get_portal_db(); row=p.execute("SELECT owner_user_id FROM download_configs WHERE id=?",(config_id,)).fetchone()
    if not row: return "配置不存在",404
    if admin_level(session["user"]["id"])==2 and str(row["owner_user_id"])!=str(session["user"]["id"]): return "403 Access Denied",403
    p.execute("DELETE FROM download_config_bots WHERE config_id=?",(config_id,)); p.execute("DELETE FROM download_configs WHERE id=?",(config_id,)); p.commit(); return redirect(url_for("admin_panel"))

@app.route("/api/downloader-config")
@login_required
def downloader_config():
    uid=str(session["user"]["id"]); level=admin_level(uid); p=get_portal_db()
    if level==1: rows=p.execute("SELECT * FROM download_configs WHERE enabled=1 ORDER BY id").fetchall()
    else: rows=p.execute("SELECT * FROM download_configs WHERE enabled=1 AND owner_user_id=? ORDER BY id",(uid,)).fetchall()
    return jsonify({"servers":[dict(r) for r in rows],"bots":[dict(r) for r in p.execute("SELECT id,name FROM download_bots WHERE owner_user_id=? ORDER BY id",(uid,)).fetchall()],"default_token":os.getenv("DISCORD_DOWNLOADER_TOKEN","") if level==1 else ""})

@app.route("/api/managed-discord-resources")
@login_required
@admin_required
def managed_discord_resources():
    """Populate the download form from Discord instead of asking for IDs."""
    uid = str(session["user"]["id"])
    guilds = managed_guilds_from_session()
    bots = [dict(row) for row in get_portal_db().execute("SELECT id,name,token FROM download_bots WHERE owner_user_id=?", (uid,)).fetchall()]
    default_token = (os.getenv("DISCORD_DOWNLOADER_TOKEN") or os.getenv("DISCORD_DOWNLOADER") or "").strip()
    if default_token:
        bots.append({"id": "default", "name": "默认下载机器人", "token": default_token})
    forums = {}
    warnings = []
    seen_forums = set()
    for bot in bots:
        try:
            for guild in guilds:
                response = requests.get(f"{API_BASE_URL}/guilds/{guild['id']}/channels", headers={"Authorization": f"Bot {bot['token']}"}, timeout=12)
                if response.status_code in (401, 403, 404): continue
                response.raise_for_status()
                for channel in response.json():
                    if channel.get("type") == 15:
                        key=(str(guild["id"]),str(channel["id"]))
                        if key in seen_forums: continue
                        seen_forums.add(key)
                        forums.setdefault(str(guild["id"]), []).append({"id": channel["id"], "name": channel.get("name") or channel["id"]})
        except requests.RequestException as exc:
            warnings.append(f"{bot['name']} 无法读取频道：{exc}")
    return jsonify({"guilds": guilds, "forums": forums, "warnings": warnings})

@app.route("/report")
@server_required
def report():
    sid = current_server_id(); conn = get_db(); uid = str(session["user"]["id"])
    db_user = conn.execute("SELECT * FROM users WHERE user_id=?", (uid,)).fetchone()
    if not db_user:
        return redirect(url_for("welcome"))
    ids = data_engine.get_merged_ids(sid, uid); ph = ",".join("?" for _ in ids)
    result = {"join_date": "未知", "most_active_day": None, "latest_msg": None, "most_replied_thread": None, "most_active_topic": None, "most_liked_msg": None, "top_friend_incoming": None, "top_friend_outgoing": None}
    row = conn.execute(f"SELECT min(timestamp) joined FROM messages WHERE author_id IN ({ph})", ids).fetchone()
    if row and row["joined"]: result["join_date"] = datetimeformat_filter(row["joined"], "%Y-%m-%d")
    result["most_active_day"] = dict(conn.execute(f"SELECT substr(timestamp,1,10) day,count(*) c FROM messages WHERE author_id IN ({ph}) GROUP BY day ORDER BY c DESC LIMIT 1", ids).fetchone() or {}) or None
    late = conn.execute(f"SELECT * FROM messages WHERE author_id IN ({ph}) AND strftime('%H',timestamp) IN ('16','17','18','19','20','21') ORDER BY timestamp DESC LIMIT 1", ids).fetchone()
    if late:
        d = dict(late); t = conn.execute("SELECT name FROM threads WHERE thread_id=?", (d["thread_id"],)).fetchone(); d["thread_name"] = t["name"] if t else "Unknown"; result["latest_msg"] = d
    row = conn.execute(f"SELECT t.thread_id,t.name,count(*) reply_count FROM threads t JOIN messages m ON t.thread_id=m.thread_id WHERE m.author_id IN ({ph}) GROUP BY t.thread_id ORDER BY reply_count DESC LIMIT 1", ids).fetchone()
    if row: result["most_replied_thread"] = dict(row); result["most_replied_thread"]["op_user"] = dict(db_user)
    row = conn.execute(f"SELECT t.thread_id,t.name,count(*) c FROM messages m JOIN threads t ON m.thread_id=t.thread_id WHERE m.author_id IN ({ph}) GROUP BY t.thread_id ORDER BY c DESC LIMIT 1", ids).fetchone()
    result["most_active_topic"] = dict(row) if row else None
    row = conn.execute(f"SELECT m.*,t.name thread_name,count(r.id) rc FROM messages m JOIN threads t ON m.thread_id=t.thread_id LEFT JOIN reactions r ON r.message_id=m.message_id WHERE m.author_id IN ({ph}) GROUP BY m.message_id ORDER BY rc DESC LIMIT 1", ids).fetchone()
    if row:
        d = dict(row); d["detailed_reactions"] = [dict(x) for x in conn.execute("SELECT emoji_url,count(*) count FROM reactions WHERE message_id=? GROUP BY emoji_name ORDER BY count DESC LIMIT 3", (d["message_id"],))]; result["most_liked_msg"] = d
    texts = [r[0] for r in conn.execute(f"SELECT content FROM messages WHERE author_id IN ({ph}) ORDER BY timestamp DESC LIMIT 2000", ids)]
    return render_template("report.html", user=db_user, server_id=sid, word_cloud_data=format_word_cloud(get_word_cloud_counter(texts), 50), percentile=95, **result)


@app.route("/chouxiangpai")
@server_required
def chouxiangpai_page():
    path = os.path.join(BASE_DIR, "templates", "chouxiangpai.html")
    if os.path.exists(path):
        return open(path, encoding="utf-8").read()
    return ""


def _downloader_process_alive():
    for name,proc in _service_processes:
        if name=="discord_downloader" and proc.poll() is None: return True
    return False

def ensure_downloader_process():
    if not _downloader_process_alive():
        _start_child([sys.executable,os.path.join(BASE_DIR,"Preparation_Before_Use","discord_downloader.py")],"discord_downloader")

def request_member_sync(guild_id, requested_by):
    p=get_portal_db()
    active=p.execute("SELECT id FROM member_sync_requests WHERE guild_id=? AND status IN ('pending','running') ORDER BY id DESC LIMIT 1",(str(guild_id),)).fetchone()
    if active:
        return int(active["id"])
    cur=p.execute("INSERT INTO member_sync_requests(guild_id,requested_by,status,created_at) VALUES(?,?, 'pending', ?)",(str(guild_id),str(requested_by),datetime.now(timezone.utc).isoformat()))
    p.commit()
    return int(cur.lastrowid)

@app.route("/admin/member-sync", methods=["POST"])
@login_required
@admin_required
def admin_member_sync():
    uid=str(session["user"]["id"]); guild=str(request.form.get("guild_id","")).strip()
    if not guild.isdigit(): return "服务器 ID 无效",400
    if admin_level(uid)==2:
        allowed={str(x.get("id")) for x in managed_guilds_from_session()}
        if guild not in allowed:
            p=get_portal_db()
            owned=p.execute("SELECT 1 FROM download_configs WHERE guild_id=? AND owner_user_id=? AND enabled=1 LIMIT 1",(guild,uid)).fetchone()
            if not owned: return "你没有权限更新该服务器成员名单",403
    request_member_sync(guild,uid)
    flash(f"服务器 {guild} 的成员名单更新已加入队列。", "success")
    return redirect(url_for("admin_panel"))

@app.route("/admin/download-task",methods=["POST"])
@login_required
@admin_required
def admin_download_task_add():
    uid=str(session["user"]["id"]); config_id=str(request.form.get("config_id","")).strip()
    if not config_id.isdigit(): return "下载配置无效",400
    p=get_portal_db(); cfg=p.execute("SELECT * FROM download_configs WHERE id=? AND enabled=1",(int(config_id),)).fetchone()
    if not cfg: return "下载配置不存在或已禁用",400
    if admin_level(uid)==2:
        if str(cfg["owner_user_id"])!=uid: return "你只能为自己有管理权限的下载配置创建任务",403
        quota=get_download_quota(uid)
        active=p.execute("SELECT COUNT(DISTINCT guild_id) FROM download_tasks WHERE created_by=? AND status IN ('pending','running')",(uid,)).fetchone()[0]
        if active>=quota: return f"已达到下载服务器配额 {quota}",403
    nowv=datetime.now(timezone.utc).isoformat()
    mode="update" if int(cfg["update_enabled"] or 0) and os.path.exists(server_db_path(str(cfg["guild_id"]))) else "initial"
    p.execute("INSERT INTO download_tasks(guild_id,forum_channel_id,created_by,status,total,completed,created_at,message,config_id,guild_name,forum_name,scheduler_interval,download_interval_ms,mode) VALUES(?,?,?,?,0,0,?,?,?,?,?,?,?,?)",(cfg["guild_id"],cfg["forum_channel_id"],uid,"pending",nowv,f"等待下载器启动 · {'更新模式' if mode=='update' else '首次下载模式'}",cfg["id"],cfg["guild_name"] or cfg["guild_id"],cfg["forum_name"] or cfg["forum_channel_id"],cfg["scheduler_interval"] if int(cfg["scheduler_interval"] or 0) >= 50 else int(cfg["scheduler_interval"] or 1) * 1000,int(cfg["download_interval_ms"] or 0),mode))
    p.commit();
    request_member_sync(str(cfg["guild_id"]), uid)
    ensure_downloader_process(); return redirect(url_for("admin_panel"))

def download_task_scope(task):
    uid = str(session["user"]["id"])
    if admin_level(uid) == 1:
        return True
    if str(task["created_by"]) == uid:
        return True
    config_id = task["config_id"]
    if config_id:
        row = get_portal_db().execute(
            "SELECT owner_user_id FROM download_configs WHERE id=?",
            (config_id,)
        ).fetchone()
        if row and str(row["owner_user_id"]) == uid:
            return True
    return False

def _task_action(task_id,action):
    p=get_portal_db(); task=p.execute("SELECT * FROM download_tasks WHERE id=?",(task_id,)).fetchone()
    if not task or not download_task_scope(task): return "403 Access Denied",403
    nowv=datetime.now(timezone.utc).isoformat()
    if action=="pause" and task["status"] in ("pending","running"):
        p.execute("UPDATE download_tasks SET status='paused',phase='paused',message='任务已暂停，可继续下载',finished_at=NULL WHERE id=?",(task_id,))
    elif action=="resume" and task["status"]=="paused":
        p.execute("UPDATE download_tasks SET status='pending',phase='queued',message='等待下载器继续执行',finished_at=NULL,delete_requested=0 WHERE id=?",(task_id,)); p.commit(); ensure_downloader_process(); return redirect(url_for("admin_panel"))
    elif action=="cancel" and task["status"] in ("pending","running","paused"):
        p.execute("UPDATE download_tasks SET status='cancelled',phase='cancelled',message='任务已取消',finished_at=?,delete_requested=0 WHERE id=?",(nowv,task_id))
    p.commit(); return redirect(url_for("admin_panel"))

@app.route("/admin/download-task/<int:task_id>/pause",methods=["POST"])
@login_required
@admin_required
def admin_download_task_pause(task_id): return _task_action(task_id,"pause")
@app.route("/admin/download-task/<int:task_id>/resume",methods=["POST"])
@login_required
@admin_required
def admin_download_task_resume(task_id): return _task_action(task_id,"resume")
@app.route("/admin/download-task/<int:task_id>/cancel",methods=["POST"])
@login_required
@admin_required
def admin_download_task_cancel(task_id): return _task_action(task_id,"cancel")

def _delete_task_data_from_server(p, task_id, guild_id):
    db_path=server_db_path(guild_id)
    if not os.path.exists(db_path): return
    task_threads=[str(r[0]) for r in p.execute("SELECT thread_id FROM download_task_items WHERE task_id=?",(task_id,)).fetchall()]
    if not task_threads: return
    other=set(str(r[0]) for r in p.execute("SELECT DISTINCT thread_id FROM download_task_items WHERE task_id<>? AND status='downloaded'",(task_id,)).fetchall())
    remove=[x for x in task_threads if x not in other]
    if not remove: return
    conn=db_connect(db_path); ph=','.join('?'*len(remove))
    msg_ids=[str(r[0]) for r in conn.execute(f"SELECT message_id FROM messages WHERE thread_id IN ({ph})",remove).fetchall()]
    if msg_ids:
        mph=','.join('?'*len(msg_ids))
        for table,col in (("reactions","message_id"),("attachments","message_id"),("mentions","message_id")):
            conn.execute(f"DELETE FROM {table} WHERE {col} IN ({mph})",msg_ids)
        conn.execute(f"DELETE FROM messages WHERE message_id IN ({mph})",msg_ids)
    conn.execute(f"DELETE FROM threads WHERE thread_id IN ({ph})",remove)
    conn.commit(); conn.close()
    # 扫描缓存也属于该任务的数据。否则删除后再次开启“更新模式”时，
    # 旧的 last_active_at 会让这些已删除帖子被误判为无需重新下载。
    try:
        p.execute(f"DELETE FROM thread_scan_state WHERE guild_id=? AND thread_id IN ({ph})",(str(guild_id),*remove))
        p.commit()
    except sqlite3.OperationalError as exc:
        if not is_missing_table_error(exc):
            raise
        app.logger.warning("删除任务扫描缓存时表不存在 task_id=%s guild_id=%s", task_id, guild_id)
    try:
        rebuild_user_stats(db_path)
    except Exception as exc:
        app.logger.warning("删除下载任务后重建用户统计失败 task_id=%s error=%s", task_id, exc)

@app.route("/admin/download-task/<int:task_id>/delete",methods=["POST"])
@login_required
@admin_required
def admin_download_task_delete(task_id):
    import shutil
    p=get_portal_db(); task=p.execute("SELECT * FROM download_tasks WHERE id=?",(task_id,)).fetchone()
    if not task:
        return "下载任务不存在", 404
    if not download_task_scope(task):
        flash("你没有权限删除这个下载任务。", "error")
        return redirect(url_for("admin_panel"))
    guild_id=str(task["guild_id"]); task_root=os.path.join(BASE_DIR,"raw",guild_id,"tasks",str(task_id))
    was_running = task["status"] == "running"
    if was_running:
        # 先让后台 Worker 看到取消状态，再做数据库清理。任务行随后立即删除，
        # 避免下载器进程异常退出时留下“隐藏任务”和未清理的分析数据。
        p.execute("UPDATE download_tasks SET status='cancelled',phase='cancelled',delete_requested=1,message='正在停止并清理任务…',finished_at=? WHERE id=?",(datetime.now(timezone.utc).isoformat(),task_id)); p.commit()

    # 这一步必须在删除 download_task_items 之前执行；这些 item 是分析库中
    # 帖子归属的唯一任务索引。后台导入线程还会在写入后检查 cancelled 状态，
    # 因此即使删除与导入并发，也不会把帖子永久写回数据库。
    _delete_task_data_from_server(p,task_id,guild_id)
    p.execute("DELETE FROM download_task_items WHERE task_id=?",(task_id,))
    p.execute("DELETE FROM download_tasks WHERE id=?",(task_id,))
    remaining=p.execute("SELECT COUNT(*) FROM download_tasks WHERE guild_id=? AND status='completed'",(guild_id,)).fetchone()[0]
    server_db = None
    if remaining==0:
        # 该服务器已没有任何完成任务时，清理空的服务器门户记录和访问缓存。
        server=p.execute("SELECT db_path FROM servers WHERE server_id=?",(guild_id,)).fetchone()
        server_db = server["db_path"] if server and server["db_path"] else server_db_path(guild_id)
        p.execute("DELETE FROM user_server_access WHERE server_id=?",(guild_id,)); p.execute("DELETE FROM user_server_presence WHERE server_id=?",(guild_id,)); p.execute("DELETE FROM servers WHERE server_id=?",(guild_id,))
        try:
            p.execute("DELETE FROM thread_scan_state WHERE guild_id=?",(guild_id,))
        except sqlite3.OperationalError as exc:
            if not is_missing_table_error(exc):
                raise
            app.logger.warning("删除服务器扫描缓存时表不存在 guild_id=%s", guild_id)
    p.commit()
    if remaining==0 and server_db and os.path.exists(server_db):
        try: os.remove(server_db)
        except OSError as exc:
            app.logger.warning("删除服务器数据库失败 path=%s error=%s", server_db, exc)
    # Unix 下删除仍被 DCE 打开的文件是安全的；这样运行中删除也不会
    # 遗留 raw 目录。后台线程随后会因任务行不存在而停止处理。
    if os.path.exists(task_root):
        shutil.rmtree(task_root,ignore_errors=True)
    return redirect(url_for("admin_panel"))

@app.route("/admin/download-tasks/status")
@login_required
@admin_required
def admin_download_tasks_status():
    p=get_portal_db(); rows=p.execute("SELECT t.*,COALESCE(c.guild_name,s.name,t.guild_id) server_display_name,COALESCE(c.forum_name,t.forum_name,t.forum_channel_id) forum_display_name FROM download_tasks t LEFT JOIN download_configs c ON c.id=t.config_id LEFT JOIN servers s ON s.server_id=t.guild_id WHERE t.delete_requested=0 ORDER BY t.id DESC LIMIT 100").fetchall()
    result=[]
    for x in rows:
        if not download_task_scope(x): continue
        d=dict(x)
        if d.get("status") in ("pending", "running") and d.get("started_at"):
            try:
                started_dt=datetime.fromisoformat(str(d["started_at"]).replace("Z", "+00:00"))
                d["elapsed_seconds"]=max(int(d.get("elapsed_seconds") or 0), int((datetime.now(timezone.utc)-started_dt).total_seconds()))
            except (TypeError, ValueError) as exc:
                app.logger.warning("解析下载任务开始时间失败 task_id=%s started_at=%s error=%s", x["id"], d.get("started_at"), exc)
        item=p.execute("SELECT COUNT(*) total_items,SUM(CASE WHEN status='downloaded' THEN 1 ELSE 0 END) downloaded_items,SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) failed_items,SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END) pending_items FROM download_task_items WHERE task_id=?",(x["id"],)).fetchone()
        d.update({k:int(v or 0) for k,v in dict(item).items()})
        bots=p.execute("SELECT bot_name,COUNT(*) count FROM download_task_items WHERE task_id=? AND bot_name IS NOT NULL GROUP BY bot_name ORDER BY count DESC",(x["id"],)).fetchall()
        d["bot_progress"]=[{"name":r["bot_name"],"count":int(r["count"] or 0)} for r in bots]
        result.append(d)
    return jsonify(result)

@app.context_processor
def inject_globals():
    sid = current_server_id()
    server = None
    if sid and os.path.exists(PORTAL_DB):
        try:
            server = get_portal_db().execute("SELECT * FROM servers WHERE server_id=?", (sid,)).fetchone()
        except Exception as exc:
            app.logger.warning("注入服务器全局变量失败 sid=%s error=%s", sid, exc)
    user = session.get("user")
    return {
        "current_server": server,
        "current_user": user,
        "admin_level": admin_level(user["id"]) if user else 0,
        # Client ID 不是敏感信息（会出现在 OAuth 授权链接里），可以放心暴露给前端，
        # Discord 活动模式的 discord-activity.js 需要用它初始化 Embedded App SDK。
        "discord_client_id": DISCORD_CLIENT_ID,
    }


init_portal_db()
migrate_legacy_db()
with app.app_context():
    sync_all_server_users()

_service_processes = []
_service_stop = threading.Event()

def _start_child(command, name):
    child_env = os.environ.copy()
    # 子服务的 /restart 需要通知真正的 Flask 主进程，而不是只 exec 自己。
    # 只把这个 PID 注入由 app.py 管理的子进程，单独运行机器人时仍保留
    # 原来的自重启兜底行为。
    child_env["V20_APP_PID"] = str(os.getpid())
    restart_signal = getattr(signal, "SIGUSR1", signal.SIGTERM)
    child_env["V20_RESTART_SIGNAL"] = str(int(restart_signal))
    kwargs = {"cwd": BASE_DIR, "env": child_env, "stdout": None, "stderr": None}
    if os.name == "nt":
        kwargs["creationflags"] = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
    try:
        proc = subprocess.Popen(command, **kwargs)
        _service_processes.append((name, proc))
        app.logger.info("服务启动成功 name=%s pid=%s", name, proc.pid)
        return proc
    except Exception as exc:
        app.logger.exception("服务启动失败 name=%s error=%s", name, exc)
        return None

def start_background_services():
    if os.getenv("DISCORD_BOT_TOKEN", "").strip():
        _start_child([sys.executable, os.path.join(BASE_DIR, "Preparation_Before_Use", "whitelist_bot.py")], "whitelist_bot")
    else:
        app.logger.info("服务未启动 name=whitelist_bot：DISCORD_BOT_TOKEN 为空")
    try:
        conn=db_connect(PORTAL_DB); row=conn.execute("SELECT 1 FROM download_tasks WHERE status IN ('pending','running') AND delete_requested=0 LIMIT 1").fetchone(); conn.close()
        if row: ensure_downloader_process()
        else: app.logger.info("服务未启动 name=discord_downloader：没有下载任务")
    except Exception as exc:
        app.logger.exception("检查下载任务状态失败，无法启动 discord_downloader：%s", exc)

def stop_background_services():
    _service_stop.set()
    for name, proc in list(_service_processes):
        if proc.poll() is None:
            try:
                proc.terminate()
                proc.wait(timeout=8)
            except Exception as exc:
                app.logger.exception("停止服务失败 name=%s pid=%s，尝试强制终止：%s", name, proc.pid, exc)
                try:
                    proc.kill()
                except Exception as kill_exc:
                    app.logger.exception("无法强制停止服务 name=%s pid=%s：%s", name, proc.pid, kill_exc)
    _service_processes.clear()


def restart_application(signum, frame):
    """由管理机器人触发，完整替换 Flask 主进程并重新拉起全部服务。"""
    app.logger.warning("收到完整应用重启信号 signum=%s，正在停止子服务并重启 app.py", signum)
    stop_background_services()
    # Werkzeug 的开发服务器监听 socket 可能在 execv 后继续被旧进程继承，
    # 导致新 app.py 报 Address already in use。仅关闭 POSIX socket，不碰
    # 日志和数据库文件描述符；生产环境由外部 WSGI/进程管理器接管时也安全。
    if os.name == "posix":
        try:
            for fd_name in os.listdir("/proc/self/fd"):
                try:
                    fd = int(fd_name)
                except ValueError:
                    continue
                if fd <= 2:
                    continue
                try:
                    if stat.S_ISSOCK(os.fstat(fd).st_mode):
                        os.close(fd)
                except (OSError, ValueError):
                    pass
        except OSError:
            pass
    os.execv(sys.executable, [sys.executable, os.path.abspath(__file__)])

if __name__ == "__main__":
    restart_signal = getattr(signal, "SIGUSR1", signal.SIGTERM)
    signal.signal(restart_signal, restart_application)
    start_background_services()
    try:
        app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=os.getenv("FLASK_DEBUG", "0") == "1", use_reloader=False)
    finally:
        stop_background_services()
