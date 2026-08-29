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
import hmac
import logging
import secrets
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from functools import wraps
from logging.handlers import RotatingFileHandler

import requests
from flask import Flask, render_template, request, g, redirect, session, url_for, jsonify, flash
from urllib.parse import urlencode
from werkzeug.middleware.proxy_fix import ProxyFix
from werkzeug.exceptions import HTTPException

from Preparation_Before_Use.discordDB import import_json_to_db, inspect_json, rebuild_user_stats
from shared.discord_api import (
    API_BASE_URL,
    any_bot_token,
    bearer_headers,
    bot_get,
    bot_headers,
    default_downloader_token,
    discord_get,
    discord_headers,
    guild_icon_url,
    user_avatar_url,
)
from shared.env import env_keys, load_local_env
from shared.portal import touch_user_presence, upsert_portal_user
from shared.sqlite_utils import add_columns, connect_sqlite, is_missing_table_error
from shared.task_timing import calculate_task_timing
from shared.timeutil import parse_utc_datetime, to_local_datetime, utc_now_iso

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
_startup_problems = []


class NoServerSelectedError(RuntimeError):
    pass


class ServerDataNotFoundError(RuntimeError):
    pass


try:
    load_local_env(os.path.join(BASE_DIR, ".env"))
except OSError as exc:
    _startup_problems.append(f"读取本地环境文件失败 path={os.path.join(BASE_DIR, '.env')} error={exc}")

def validate_env_contract():
    example = os.path.join(BASE_DIR, ".env.example")
    actual = os.path.join(BASE_DIR, ".env")
    expected = env_keys(example)
    present = env_keys(actual)
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
ADMIN_IDS = {x.strip() for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()}
DISCORD_CLIENT_ID = os.getenv("DISCORD_CLIENT_ID", "").strip()
DISCORD_CLIENT_SECRET = os.getenv("DISCORD_CLIENT_SECRET", "").strip()
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


# Activity 登录握手由 Discord 客户端跨站发起，没有页面上下文可携带 CSRF Token；
# 这些接口不修改已登录用户的数据，因此单独豁免。
CSRF_EXEMPT_ENDPOINTS = {"activity_token", "activity_log"}


def request_page(default=1):
    """读取分页参数；非法输入回退到首页而不是 500。"""
    raw = request.args.get("page", default)
    try:
        return max(1, min(10 ** 6, int(raw)))
    except (TypeError, ValueError):
        return default


def csrf_token():
    token = session.get("_csrf_token")
    if not token:
        token = secrets.token_urlsafe(32)
        session["_csrf_token"] = token
    return token


@app.before_request
def csrf_protect():
    """所有写操作都必须带上会话内的 CSRF Token。

    Activity 模式下会话 Cookie 是 SameSite=None，浏览器会在跨站请求里携带它，
    因此仅靠 Cookie 无法区分请求来源。
    """
    if request.method in ("GET", "HEAD", "OPTIONS", "TRACE"):
        return None
    if request.endpoint in CSRF_EXEMPT_ENDPOINTS:
        return None
    expected = session.get("_csrf_token", "")
    supplied = request.form.get("csrf_token") or request.headers.get("X-CSRF-Token", "")
    if not expected or not hmac.compare_digest(str(supplied), str(expected)):
        app.logger.warning(
            "csrf.reject request_id=%s endpoint=%s", _request_id(), request.endpoint or "-"
        )
        if request.path.startswith("/api/") or request.accept_mimetypes.best == "application/json":
            return jsonify({"ok": False, "error": "CSRF 校验失败，请刷新页面后重试"}), 400
        return "CSRF 校验失败，请刷新页面后重试", 400
    return None


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
    return connect_sqlite(path)


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
        token_type TEXT NOT NULL DEFAULT 'bot',
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
    CREATE TABLE IF NOT EXISTS demo_identities (
        real_user_id TEXT PRIMARY KEY,
        demo_user_id TEXT NOT NULL,
        demo_server_id TEXT NOT NULL,
        created_at DATETIME NOT NULL
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
        "scan_bot_name": "TEXT",
        "active_started_at": "DATETIME"
    }
    add_columns(conn, "download_tasks", task_migrations)
    add_columns(conn, "download_servers", {"use_default_bot": "INTEGER DEFAULT 0"})
    add_columns(conn, "servers", {"source_task_id": "INTEGER"})
    add_columns(conn, "download_configs", {
        "update_enabled": "INTEGER DEFAULT 0",
        "download_interval_ms": "INTEGER DEFAULT 0",
    })
    add_columns(conn, "download_bots", {"token_type": "TEXT NOT NULL DEFAULT 'bot'"})
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
    CREATE INDEX IF NOT EXISTS idx_msg_author_message ON messages(author_id,message_id);
    CREATE INDEX IF NOT EXISTS idx_msg_author_thread ON messages(author_id,thread_id);
    CREATE INDEX IF NOT EXISTS idx_msg_timestamp ON messages(timestamp);
    CREATE INDEX IF NOT EXISTS idx_msg_thread ON messages(thread_id);
    CREATE INDEX IF NOT EXISTS idx_react_user ON reactions(user_id);
    CREATE INDEX IF NOT EXISTS idx_react_user_message ON reactions(user_id,message_id);
    CREATE INDEX IF NOT EXISTS idx_react_msg ON reactions(message_id);
    CREATE INDEX IF NOT EXISTS idx_stats_count ON user_stats(msg_count);
    CREATE INDEX IF NOT EXISTS idx_mentions_author ON mentions(author_id);
    CREATE INDEX IF NOT EXISTS idx_profile_views_target_time ON profile_views(target_user_id,timestamp DESC);
    """)
    add_columns(conn, "threads", {"last_active_at": "TEXT"})
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
    now = utc_now_iso()
    conn.execute("""INSERT OR REPLACE INTO servers(
        server_id,name,icon_url,owner_user_id,db_path,created_at,updated_at,source_task_id
    ) VALUES (?,?,?,?,?,?,?,NULL)""", (
        legacy_server_id, "Legacy Discord Server", None, None, target, now, now
    ))
    conn.commit()
    conn.close()


def ensure_server_indexes():
    """Apply read-path indexes to databases created before the split layout."""
    portal = db_connect(PORTAL_DB)
    try:
        server_rows = portal.execute("SELECT db_path FROM servers").fetchall()
    finally:
        portal.close()
    for row in server_rows:
        path = row["db_path"]
        if not path or not os.path.exists(path):
            continue
        conn = db_connect(path)
        try:
            conn.executescript("""
            CREATE INDEX IF NOT EXISTS idx_msg_author_message ON messages(author_id,message_id);
            CREATE INDEX IF NOT EXISTS idx_msg_author_thread ON messages(author_id,thread_id);
            CREATE INDEX IF NOT EXISTS idx_react_user_message ON reactions(user_id,message_id);
            CREATE INDEX IF NOT EXISTS idx_mentions_author ON mentions(author_id);
            """)
            conn.commit()
        finally:
            conn.close()


def format_time(seconds):
    seconds = max(0, int(seconds))
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    return f"{h:d}:{m:02d}:{s:02d}" if h else f"{m:02d}:{s:02d}"


def parse_and_convert(time_str):
    return to_local_datetime(time_str)


@app.template_filter("datetimeformat")
def datetimeformat_filter(value, format="%Y-%m-%d %H:%M"):
    dt = parse_and_convert(value)
    return dt.strftime(format) if dt else value


@app.template_filter("raw_datetime")
def raw_datetime_filter(value, format="%Y-%m-%d %H:%M"):
    return datetimeformat_filter(value, format)


def is_pure_chinese(word):
    return bool(word) and re.fullmatch(r"[\u4e00-\u9fa5]+", str(word)) is not None


def _is_valid_word(word):
    """中文词或长度>=3 的英文小写词（排除 the/and 等短停用词由停用词表处理）。"""
    if not word:
        return False
    return bool(re.fullmatch(r"[\u4e00-\u9fa5]+|[a-zA-Z][a-zA-Z'-]{2,}", str(word)))


# 英文停用词：过滤后剩余的都是有实义的讨论词汇
_EN_STOP_WORDS = set(
    """the a an and or but if then else for to of in on at by with from as is are was were be been being
    it its it's this that these those i you he she they we me him her them my your his their our
    not no yes so do does did done doing have has had having will would can could should shall may might must
    what which who whom when where why how all any both each few more most other some such only own same than too very
    just about into over under again further once here there out up down off why how
    don't doesn't didn't won't wouldn't can't couldn't shouldn't isn't aren't wasn't weren't i'm you're we're they're
    i've you've we've they've i'll you'll he'll she'll we'll they'll i'd you'd he'd she'd we'd they'd
    got get getting gonna wanna yeah yep nope hey hi hello ok okay u ur ur's like well really even also still way
    one two thing things something anything nothing everything someone anyone everyone time now new make made making
    go going went come coming took take taken see seen saw know known knew think thought say said let lets us am
    because sure thanks thank good great nice lol lmao haha hmm btw fyi""".split()
)


def get_word_cloud_counter(text_list):
    text = " ".join(str(t) for t in text_list if t)
    text = text[:5000000]
    stop_words = set("什么 这个 那个 怎么 可以 因为 所以 但是 就是 这就 感觉 时候 现在 还是 没有 一样 知道 觉得 出来 其实 这种 那样 一下 然后 虽然 不是 还有 这里 那里 今天 明天 真的 可能 图片 表情 回复 一个 自己 只是 非常 不能 不要 需要 如果 以及 我们 你们 他们 看到 不过 确实 已经 大家 为什么 不会 这样 这么 那么 那些 是不是 有没有".split())
    # 中文按整段提取；英文按单词切分并归一为小写，两路合并计数
    words = re.findall(r"[\u4e00-\u9fa5]{2,}", text)
    en_words = (w.lower() for w in re.findall(r"[a-zA-Z][a-zA-Z'-]*", text))
    counter = collections.Counter(w for w in words if w not in stop_words)
    counter.update(w for w in en_words if w not in _EN_STOP_WORDS)
    return counter


def format_word_cloud(counter, limit=None):
    items = [(k, v) for k, v in counter.items() if is_pure_chinese(k) or re.fullmatch(r"[a-z][a-z'-]{2,}", k)]
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
        """Rebuild one homepage snapshot outside the request thread.

        大库（4.6M+ 消息）冷启动时单次 SELECT content + Python 端分词
        会跑几分钟。改为增量分批处理，每批 10 万条，多次 commit 缓存，
        后续进程若被中断也能从已保存的快照继续（不会阻塞访问）。
        """
        try:
            cache = self._load_cache(sid)
            db_path = server_db_path(sid)
            conn = db_connect(db_path)
            cur = conn.cursor()
            count = cur.execute("SELECT MAX(rowid) FROM messages").fetchone()[0]
            try:
                db_mtime = os.path.getmtime(db_path)
            except OSError:
                db_mtime = 0
            # 已经统计过的消息数（增量分批）
            processed = cache.get("global_word_processed", 0)
            counter = cache.get("global_word_counter") or collections.Counter()
            BATCH = 100_000
            for off in range(processed, count, BATCH):
                texts = [
                    row[0] for row in cur.execute(
                        "SELECT content FROM messages WHERE content IS NOT NULL AND content != '' "
                        "ORDER BY timestamp DESC LIMIT ? OFFSET ?",
                        (BATCH, off),
                    )
                ]
                counter.update(get_word_cloud_counter(texts))
                processed += len(texts)
                cache["global_word_counter"] = counter
                cache["global_word_processed"] = processed
                self._save_cache(sid)
            cache["homepage"] = self._homepage(cur, counter)
            cache["last_msg_count"] = count
            cache["db_mtime"] = db_mtime
            cache["homepage_version"] = 5
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
        # 用 MAX(rowid) 代替 count(*) —— 460 万行全表扫秒级 vs 10 秒。
        # count(*) 必须全表扫描；rowid 是 btree 索引，最后一行秒级。
        count = cur.execute("SELECT MAX(rowid) FROM messages").fetchone()[0]
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
            or cache.get("homepage_version") != 5
            or not cache.get("homepage")
        )
        if cache_needs_refresh and cache.get("homepage"):
            self._schedule_refresh(sid)
        elif cache_needs_refresh:
            # 冷启动 / 首次访问 / 数据库变化：永远在后台异步构建
            # —— 避免首页一次 11 秒（4.6M 消息分词耗时）。
            # 先给前端一个空快照占位，词云和榜单会随后台进度更新。
            if not cache.get("homepage"):
                cache["homepage"] = {
                    "total_msgs": count, "total_threads": 0, "total_users": 0,
                    "chart_daily": [], "word_cloud_data": [], "server_word_rank": [],
                    "active_members": [], "popular_replies": [], "popular_discussions": [],
                }
                cache["last_msg_count"] = count
                cache["homepage_version"] = 5
                self._save_cache(sid)
            self._schedule_refresh(sid)
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
        # Build the complete leaderboard while the snapshot is refreshed in the
        # background. The request path must never repeat the expensive per-user
        # reaction queries for the modal's first 50 entries.
        leaderboard = []
        rows = cur.execute("SELECT u.*,count(m.message_id) msg_count FROM users u JOIN messages m ON u.user_id=m.author_id GROUP BY u.user_id ORDER BY msg_count DESC LIMIT 50").fetchall()
        leaderboard_ids = [row["user_id"] for row in rows]
        emoji_map = collections.defaultdict(list)
        if leaderboard_ids:
            placeholders = ",".join("?" for _ in leaderboard_ids)
            emoji_rows = cur.execute(
                f"SELECT m.author_id,r.emoji_url,r.emoji_name,count(*) c FROM reactions r "
                f"JOIN messages m ON r.message_id=m.message_id WHERE m.author_id IN ({placeholders}) "
                f"GROUP BY m.author_id,r.emoji_name ORDER BY m.author_id,c DESC",
                leaderboard_ids,
            ).fetchall()
            for emoji in emoji_rows:
                if len(emoji_map[emoji["author_id"]]) < 3:
                    emoji_map[emoji["author_id"]].append({
                        "emoji_url": emoji["emoji_url"],
                        "emoji_name": emoji["emoji_name"],
                        "c": emoji["c"],
                    })
        for u in rows:
            d = dict(u)
            d["top_emojis"] = emoji_map.get(u["user_id"], [])
            leaderboard.append(d)
        data["full_leaderboard"] = leaderboard
        data["top_users"] = leaderboard[:10]
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


class ProfileEngine:
    """Cached, stale-while-refresh profile aggregates.

    A profile contains several statistics over the whole analytics database.
    Those statistics do not change during a page navigation, so calculating
    them in every request is particularly expensive for large servers.  Keep
    them in a small per-user snapshot, just like the homepage snapshot, while
    leaving the current page of messages live.
    """

    VERSION = 1

    def __init__(self):
        self._memory = {}
        self._refresh_lock = threading.Lock()
        self._refreshing = set()
        self._last_refresh_started = {}
        self._refresh_interval = 10.0

    def _cache_file(self, sid, uid):
        return os.path.join(CACHE_DIR, f"profile-{sid}-{uid}.pkl")

    def _load(self, sid, uid):
        key = (str(sid), str(uid))
        if key in self._memory:
            return self._memory[key]
        try:
            with open(self._cache_file(*key), "rb") as f:
                value = pickle.load(f)
                if isinstance(value, dict):
                    self._memory[key] = value
                    return value
        except FileNotFoundError:
            pass
        except (OSError, EOFError, pickle.UnpicklingError, TypeError, ValueError) as exc:
            app.logger.warning("个人主页缓存读取失败 sid=%s uid=%s error=%s", sid, uid, exc)
        value = {}
        self._memory[key] = value
        return value

    def _save(self, sid, uid):
        key = (str(sid), str(uid))
        path = self._cache_file(*key)
        temp = path + ".tmp"
        try:
            with open(temp, "wb") as f:
                pickle.dump(self._memory[key], f)
            os.replace(temp, path)
        finally:
            if os.path.exists(temp):
                try:
                    os.remove(temp)
                except OSError:
                    pass

    @staticmethod
    def _db_signature(path):
        """Use the main DB and WAL metadata without scanning a million rows."""
        result = []
        for candidate in (path, path + "-wal"):
            try:
                stat_result = os.stat(candidate)
                result.append((candidate, stat_result.st_mtime_ns, stat_result.st_size))
            except OSError:
                result.append((candidate, 0, 0))
        return tuple(result)

    def _build(self, sid, uid):
        path = server_db_path(sid)
        conn = db_connect(path)
        try:
            merged_ids = data_engine.get_merged_ids(sid, uid)
            placeholders = ",".join("?" for _ in merged_ids)
            args = tuple(merged_ids)
            data = {"merged_ids": merged_ids}
            data["msg_count"] = conn.execute(
                f"SELECT count(*) FROM messages WHERE author_id IN ({placeholders})", args
            ).fetchone()[0]
            data["reaction_received_count"] = conn.execute(
                f"SELECT count(*) FROM reactions r JOIN messages m ON r.message_id=m.message_id "
                f"WHERE m.author_id IN ({placeholders})", args
            ).fetchone()[0]
            data["thread_count"] = conn.execute(
                f"SELECT count(*) FROM (SELECT thread_id FROM messages "
                f"WHERE author_id IN ({placeholders}) GROUP BY thread_id)", args
            ).fetchone()[0]
            data["top_emojis_given"] = [dict(row) for row in conn.execute(
                f"SELECT emoji_url,emoji_name,count(*) c FROM reactions "
                f"WHERE user_id IN ({placeholders}) GROUP BY emoji_name ORDER BY c DESC LIMIT 3", args
            )]
            data["top_emojis_received"] = [dict(row) for row in conn.execute(
                f"SELECT r.emoji_url,r.emoji_name,count(*) c FROM reactions r "
                f"JOIN messages m ON r.message_id=m.message_id "
                f"WHERE m.author_id IN ({placeholders}) GROUP BY r.emoji_name ORDER BY c DESC LIMIT 3", args
            )]

            # The first branch in the old query selected the author IDs of the
            # user's own messages and then excluded those same IDs. It could
            # never contribute a row, while making SQLite repeatedly resolve a
            # large message-id subquery.
            data["interactions_incoming"] = [dict(row) for row in conn.execute(
                f"SELECT u.user_id,u.nickname,u.username,u.avatar_url,count(*) score "
                f"FROM reactions r JOIN messages m ON r.message_id=m.message_id "
                f"JOIN users u ON r.user_id=u.user_id "
                f"WHERE m.author_id IN ({placeholders}) AND r.user_id NOT IN ({placeholders}) "
                f"GROUP BY u.user_id ORDER BY score DESC LIMIT 5", args + args
            )]
            data["interactions_outgoing"] = [dict(row) for row in conn.execute(
                f"SELECT u.user_id,u.nickname,u.username,u.avatar_url,count(*) score FROM ("
                f"SELECT mentioned_user_id target_id FROM mentions WHERE author_id IN ({placeholders}) "
                f"UNION ALL SELECT m.author_id target_id FROM reactions r "
                f"JOIN messages m ON r.message_id=m.message_id WHERE r.user_id IN ({placeholders})"
                f") raw JOIN users u ON raw.target_id=u.user_id "
                f"WHERE u.user_id NOT IN ({placeholders}) GROUP BY u.user_id ORDER BY score DESC LIMIT 5",
                args + args + args,
            )]
            data["chart_daily"] = [dict(row) for row in conn.execute(
                f"SELECT substr(timestamp,1,10) day,count(*) c FROM messages "
                f"WHERE author_id IN ({placeholders}) GROUP BY day ORDER BY day", args
            )]
            hourly = {row["hour"]: row["c"] for row in conn.execute(
                f"SELECT strftime('%H',timestamp) hour,count(*) c FROM messages "
                f"WHERE author_id IN ({placeholders}) GROUP BY hour", args
            )}
            hours = [0] * 24
            for hour, count in hourly.items():
                if hour is not None:
                    hours[(int(hour) + 8) % 24] += count
            data["chart_hourly"] = [{"hour": f"{i:02d}:00", "c": value} for i, value in enumerate(hours)]
            texts = [row[0] for row in conn.execute(
                f"SELECT content FROM messages WHERE author_id IN ({placeholders}) "
                f"ORDER BY timestamp DESC LIMIT 2000", args
            )]
            data["word_cloud_data"] = format_word_cloud(get_word_cloud_counter(texts), 50)
            data["signature"] = self._db_signature(path)
            data["version"] = self.VERSION
            data["complete"] = True
            return data
        finally:
            conn.close()

    def _build_fast(self, sid, uid):
        """Build enough data for the first paint without large joins.

        The expensive interaction and received-reaction aggregates are filled
        by the background refresh. A profile should be usable immediately,
        especially when a user has tens of thousands of messages.
        """
        path = server_db_path(sid)
        conn = db_connect(path)
        try:
            merged_ids = data_engine.get_merged_ids(sid, uid)
            placeholders = ",".join("?" for _ in merged_ids)
            args = tuple(merged_ids)
            texts = [row[0] for row in conn.execute(
                f"SELECT content FROM messages WHERE author_id IN ({placeholders}) "
                f"ORDER BY timestamp DESC LIMIT 2000", args
            )]
            data = {
                "merged_ids": merged_ids,
                "msg_count": conn.execute(
                    f"SELECT count(*) FROM messages WHERE author_id IN ({placeholders})", args
                ).fetchone()[0],
                "reaction_received_count": 0,
                "thread_count": conn.execute(
                    f"SELECT count(*) FROM (SELECT thread_id FROM messages "
                    f"WHERE author_id IN ({placeholders}) GROUP BY thread_id)", args
                ).fetchone()[0],
                "top_emojis_given": [dict(row) for row in conn.execute(
                    f"SELECT emoji_url,emoji_name,count(*) c FROM reactions "
                    f"WHERE user_id IN ({placeholders}) GROUP BY emoji_name ORDER BY c DESC LIMIT 3", args
                )],
                "top_emojis_received": [],
                "interactions_incoming": [],
                "interactions_outgoing": [],
                "chart_daily": [dict(row) for row in conn.execute(
                    f"SELECT substr(timestamp,1,10) day,count(*) c FROM messages "
                    f"WHERE author_id IN ({placeholders}) GROUP BY day ORDER BY day", args
                )],
                "word_cloud_data": format_word_cloud(get_word_cloud_counter(texts), 50),
                "signature": self._db_signature(path),
                "version": self.VERSION,
                "complete": False,
            }
            hourly = {row["hour"]: row["c"] for row in conn.execute(
                f"SELECT strftime('%H',timestamp) hour,count(*) c FROM messages "
                f"WHERE author_id IN ({placeholders}) GROUP BY hour", args
            )}
            hours = [0] * 24
            for hour, count in hourly.items():
                if hour is not None:
                    hours[(int(hour) + 8) % 24] += count
            data["chart_hourly"] = [{"hour": f"{i:02d}:00", "c": value} for i, value in enumerate(hours)]
            return data
        finally:
            conn.close()

    def _refresh(self, sid, uid):
        key = (str(sid), str(uid))
        try:
            self._memory[key] = self._build(*key)
            self._save(*key)
        except Exception:
            app.logger.exception("刷新个人主页缓存失败 sid=%s uid=%s", sid, uid)
        finally:
            with self._refresh_lock:
                self._refreshing.discard(key)

    def _schedule_refresh(self, sid, uid):
        key = (str(sid), str(uid))
        with self._refresh_lock:
            now = time.monotonic()
            if key in self._refreshing or now - self._last_refresh_started.get(key, 0) < self._refresh_interval:
                return
            self._refreshing.add(key)
            self._last_refresh_started[key] = now
        # Let the initial profile response release its SQLite read before the
        # full aggregate refresh starts competing for CPU and disk bandwidth.
        thread = threading.Timer(2.0, self._refresh, args=key)
        thread.name = f"profile-cache-{sid}-{uid}"
        thread.daemon = True
        thread.start()

    def load_or_compute(self, sid, uid):
        value = self._load(sid, uid)
        stale = (
            value.get("version") != self.VERSION
            or not value.get("signature")
            or value.get("signature") != self._db_signature(server_db_path(sid))
            or not value.get("merged_ids")
        )
        if stale and value.get("msg_count") is not None:
            self._schedule_refresh(sid, uid)
        elif stale:
            # The first visit gets a lightweight snapshot immediately. The
            # full interaction/word-cloud joins run after the response.
            self._memory[(str(sid), str(uid))] = self._build_fast(str(sid), str(uid))
            self._save(sid, uid)
            self._schedule_refresh(sid, uid)
            value = self._load(sid, uid)
        return value


profile_engine = ProfileEngine()


def admin_level(user_id):
    uid = str(user_id)
    try:
        cached = getattr(g, "admin_levels", {})
        if uid in cached:
            return cached[uid]
    except RuntimeError:
        cached = None
    if uid in ADMIN_IDS:
        level = 1
    else:
        row = get_portal_db().execute("SELECT 1 FROM whitelist_users WHERE user_id=?", (uid,)).fetchone()
        level = 2 if row else 0
    if cached is not None:
        cached[uid] = level
        g.admin_levels = cached
    return level


def get_download_quota(user_id):
    row = get_portal_db().execute("SELECT quota FROM server_download_quota WHERE user_id=?", (str(user_id),)).fetchone()
    return int(row["quota"]) if row else 1


def sync_server_users_to_portal(server_id):
    """把服务器分析库中的普通用户同步到门户数据库，避免普通登录用户被误判为无数据。"""
    sid = str(server_id)
    path = server_db_path(sid)
    if not os.path.exists(path):
        return 0
    now = utc_now_iso()
    portal = get_portal_db()
    conn = db_connect(path)
    rows = conn.execute("SELECT user_id,username,nickname,avatar_url FROM users WHERE user_id IS NOT NULL AND user_id!=''").fetchall()
    conn.close()
    for row in rows:
        uid = str(row["user_id"])
        upsert_portal_user(portal, uid, row["username"], row["nickname"], row["avatar_url"])
        touch_user_presence(portal, uid, sid, now)
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
    try:
        cached = getattr(g, "servers_for_user", {})
        if uid in cached:
            return cached[uid]
    except RuntimeError:
        cached = None
    portal = get_portal_db()
    rows = portal.execute("SELECT * FROM servers ORDER BY name").fetchall()
    result = []
    level = admin_level(uid)
    access = {
        str(row["server_id"])
        for row in portal.execute("SELECT server_id FROM user_server_access WHERE user_id=?", (uid,))
    }
    presence = {
        str(row["server_id"])
        for row in portal.execute("SELECT server_id FROM user_server_presence WHERE user_id=?", (uid,))
    }
    changed = False
    for server in rows:
        sid = str(server["server_id"])
        path = server["db_path"]
        if not os.path.exists(path):
            continue
        if level == 1:
            result.append(server)
            continue
        found = sid in access
        present = sid in presence
        # 对历史数据库即时校验 users 主键；无需把 20 万帖子重新同步到 portal.db。
        if not present:
            try:
                adb = db_connect(path)
                present = adb.execute("SELECT user_id,username,nickname,avatar_url FROM users WHERE user_id=? LIMIT 1", (uid,)).fetchone()
                adb.close()
                if present:
                    upsert_portal_user(portal, uid, present["username"], present["nickname"], present["avatar_url"])
                    touch_user_presence(portal, uid, sid)
                    presence.add(sid)
                    changed = True
            except Exception:
                app.logger.exception("检查普通用户服务器数据失败: %s", sid)
        if found or present or str(server["owner_user_id"] or "") == uid:
            if not found:
                portal.execute("INSERT OR IGNORE INTO user_server_access(user_id,server_id,granted_by,created_at) VALUES(?,?,?,?)", (uid, sid, uid, utc_now_iso()))
                access.add(sid)
                changed = True
            result.append(server)
    if changed:
        portal.commit()
    if cached is not None:
        cached[uid] = result
        g.servers_for_user = cached
    # ---- Demo 服务器兜底（2026-08-26 加入）----
    # 用户在任何真实服务器中都没有数据时，将其引入演示服务器以便体验；
    # 演示服务器绝不授予 admin/whitelist 权限，且一旦用户在真实服务器出现数据，
    # 就从可见列表中移除演示服务器。
    # 无数据的用户会被分配一个演示库中的假身份（demo_identities 表），
    # 这样看板/个人主页/年度报告都能以该假用户的视角完整体验。
    demo_id = str(os.getenv("DEMO_SERVER_ID", "900000000000000001"))
    has_real_data = any(str(s["server_id"]) != demo_id for s in result)
    if not has_real_data and level == 0:
        demo = portal.execute("SELECT * FROM servers WHERE server_id=?", (demo_id,)).fetchone()
        if demo is not None and os.path.exists(demo["db_path"]):
            portal.execute(
                "INSERT OR IGNORE INTO user_server_access(user_id,server_id,granted_by,created_at) VALUES(?,?,?,?)",
                (uid, demo_id, "demo-fallback", utc_now_iso()),
            )
            _assign_demo_identity(portal, uid, demo_id)
    elif level >= 1:
        # admin 切换到 demo 浏览时也分配假身份（不写 user_server_access，
        # 不影响其 admin 权限与真实服务器可见性）。
        demo = portal.execute("SELECT * FROM servers WHERE server_id=?", (demo_id,)).fetchone()
        if demo is not None and os.path.exists(demo["db_path"]):
            _assign_demo_identity(portal, uid, demo_id)
    elif has_real_data and level == 0:
        changed2 = False
        for row in portal.execute(
            "SELECT granted_by FROM user_server_access WHERE user_id=? AND server_id=?", (uid, demo_id)
        ).fetchall():
            if row["granted_by"] == "demo-fallback":
                portal.execute(
                    "DELETE FROM user_server_access WHERE user_id=? AND server_id=? AND granted_by='demo-fallback'",
                    (uid, demo_id),
                )
                changed2 = True
        if changed2:
            portal.commit()
            session.pop("demo_identity", None)
            result = [s for s in result if str(s["server_id"]) != demo_id]
    return result


def _assign_demo_identity(portal, uid, demo_id):
    """为无数据用户随机绑定一个演示库中的假成员，结果持久化，保证每次登录看到同一个身份。"""
    # 该函数可能从 get_servers_for_user() 的演示服务器兜底分支直接调用，
    # 不能依赖 get_display_user() 后面才执行的初始化逻辑。
    _ensure_demo_identity_table()
    existing = portal.execute(
        "SELECT demo_user_id FROM demo_identities WHERE real_user_id=?", (str(uid),)
    ).fetchone()
    if existing:
        return str(existing["demo_user_id"])
    conn = db_connect(server_db_path(demo_id))
    try:
        candidates = [r[0] for r in conn.execute(
            "SELECT user_id FROM users WHERE is_bot=0 AND user_id NOT IN "
            "(SELECT target_id FROM user_merges) ORDER BY RANDOM() LIMIT 5"
        )]
    finally:
        conn.close()
    if not candidates:
        return None
    chosen = candidates[0]
    portal.execute(
        "INSERT INTO demo_identities(real_user_id,demo_user_id,demo_server_id,created_at) VALUES(?,?,?,?)",
        (str(uid), str(chosen), str(demo_id), utc_now_iso()),
    )
    portal.commit()
    app.logger.info("为用户 %s 分配演示身份 %s（服务器 %s）", uid, chosen, demo_id)
    return str(chosen)


DEMO_IDENTITY_TABLE_READY = False

def _ensure_demo_identity_table():
    global DEMO_IDENTITY_TABLE_READY
    if DEMO_IDENTITY_TABLE_READY:
        return
    portal = get_portal_db()
    portal.execute("""
    CREATE TABLE IF NOT EXISTS demo_identities(
        real_user_id TEXT PRIMARY KEY,
        demo_user_id TEXT NOT NULL,
        demo_server_id TEXT NOT NULL,
        created_at DATETIME NOT NULL
    );
    """)
    portal.commit()
    DEMO_IDENTITY_TABLE_READY = True


def get_display_user():
    """返回当前会话用于展示的身份。

    - 有真实数据的用户：返回其 Discord 身份本身
    - 无数据的 demo 用户：返回被分配的假成员身份（并同步进 session）
    """
    u = session.get("user")
    if not u:
        return None
    demo_id = str(os.getenv("DEMO_SERVER_ID", "900000000000000001"))
    # 按当前选中的服务器判断：只要正浏览的是演示服务器，就用假身份展示
    # （admin 切到 demo 时同样生效；真实服务器中仍用本人身份）。
    if str(current_server_id() or "") != demo_id:
        return u
    _ensure_demo_identity_table()
    portal = get_portal_db()
    row = portal.execute(
        "SELECT d.demo_user_id, p.username, p.nickname, p.avatar_url FROM demo_identities d "
        "LEFT JOIN portal_users p ON p.user_id=d.demo_user_id WHERE d.real_user_id=?",
        (str(u["id"]),),
    ).fetchone()
    if not row:
        return u
    display = {
        "id": str(row["demo_user_id"]),
        "username": row["username"] or f"demo_{row['demo_user_id'][-4:]}",
        "avatar": row["avatar_url"] or u.get("avatar"),
        "nickname": row["nickname"],
        "_is_demo_identity": True,
    }
    session["display_user"] = display
    return display

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
    now = utc_now_iso()
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
            result.append(dict(guild))
    for guild in result:
        guild["icon_url"] = guild_icon_url(guild.get("id"), guild.get("icon"))
    return result


_managed_resources_cache = {}
_managed_resources_cache_lock = threading.Lock()
_MANAGED_RESOURCES_TTL = 60.0


def bot_identity(token, token_type="bot"):
    """Validate a download credential and return Discord's authoritative name."""
    response = discord_get("/users/@me", token, token_type, timeout=12)
    response.raise_for_status()
    data = response.json()
    return data.get("global_name") or data.get("username") or "Discord Bot"


def check_bot_forum_access(token, guild_id, forum_id, token_type="bot"):
    """Validate that a credential can see the target Forum.

    Bot credentials get the full role/overwrite permission check. User
    credentials are checked by the channel endpoint itself; requiring a user
    token to read the guild member/role endpoints would reject valid users
    that can nevertheless export the Forum through DiscordChatExporter.
    """
    headers = discord_headers(token, token_type)
    try:
        me = requests.get(f"{API_BASE_URL}/users/@me", headers=headers, timeout=12)
        if me.status_code in (401, 403):
            return False, "Token 无效或 Discord 拒绝了此凭据"
        me.raise_for_status()
        bot = me.json()
        bot_id = str(bot.get("id"))

        channels = requests.get(f"{API_BASE_URL}/guilds/{guild_id}/channels", headers=headers, timeout=12)
        if channels.status_code in (401, 403):
            return False, "凭据无法读取服务器频道列表，或不在此服务器"
        if channels.status_code == 404:
            return False, "不在此服务器"
        channels.raise_for_status()
        channel = next((c for c in channels.json() if str(c.get("id")) == str(forum_id)), None)
        if not channel:
            return False, "找不到目标 Forum，或凭据无法访问该频道"
        if int(channel.get("type", -1)) != 15:
            return False, "目标频道不是 Forum 频道"
        if str(token_type or "bot").lower() == "user":
            return True, "可访问（user token）"

        member = requests.get(f"{API_BASE_URL}/guilds/{guild_id}/members/{bot_id}", headers=headers, timeout=12)
        if member.status_code == 404:
            return False, "机器人不在此服务器"
        if member.status_code in (401, 403):
            return False, "无法读取机器人在服务器中的成员信息"
        member.raise_for_status()
        member_data = member.json()

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
        "SELECT id,name,token,token_type FROM download_bots WHERE owner_user_id=? ORDER BY id", (uid,)
    ).fetchall()}
    if not set(bot_ids).issubset(owned):
        raise PermissionError("只能选择自己的下载机器人")
    for bid in bot_ids:
        rows.append(owned[bid])
    if use_default:
        token = default_downloader_token()
        if not token:
            raise ValueError("已勾选默认下载机器人，但服务器未配置 DISCORD_DOWNLOADER_TOKEN")
        rows.append({"id": "default", "name": "默认下载机器人", "token": token, "token_type": "bot"})
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
    token = any_bot_token()
    if not token:
        return None
    response = bot_get(f"/users/{user_id}", token)
    if response.status_code == 404:
        return None
    response.raise_for_status()
    data = response.json()
    username = data.get("global_name") or data.get("username")
    if not username:
        return None
    return {"username": username, "avatar_url": user_avatar_url(user_id, data.get("avatar"), size=64, default=None)}


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
        r = requests.get(f"{API_BASE_URL}/users/@me", headers=bearer_headers(access_token), timeout=20)
        if r.status_code == 429:
            return False, _discord_rate_limit_error(r, "current_user")
        r.raise_for_status()
        u = r.json()
        guilds = []
        if fetch_guilds:
            guild_response = requests.get(f"{API_BASE_URL}/users/@me/guilds", headers=bearer_headers(access_token), timeout=20)
            if guild_response.status_code == 429:
                return False, _discord_rate_limit_error(guild_response, "user_guilds")
            guild_response.raise_for_status()
            guilds = guild_response.json()
        avatar = user_avatar_url(u["id"], u.get("avatar"))
        portal = get_portal_db()
        upsert_portal_user(
            portal, u["id"], u["username"], u.get("global_name") or u["username"], avatar, last_login=utc_now_iso()
        )
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
    if servers:
        get_display_user()  # demo 用户在此分配假身份并写入 session
    if len(servers) == 1:
        session["server_id"] = servers[0]["server_id"]
        return redirect(url_for("index"))
    if len(servers) > 1:
        return redirect(url_for("servers"))
    return redirect(url_for("welcome"))


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
    if servers:
        display_user = get_display_user() or session["user"]
    else:
        return render_template("welcome.html", can_upload=whitelist_allowed(session["user"]["id"]), current_user=session["user"], no_data=True)
    if len(servers) == 1:
        session["server_id"] = servers[0]["server_id"]
        return redirect(url_for("user_profile", user_id=display_user["id"]))
    if len(servers) > 1:
        # 管理员可以访问所有服务器；个人主页仍需绑定一个分析库。
        # 优先沿用当前选择，避免从 Admin/服务器列表点击“个人主页”时循环回 /servers。
        allowed = {str(server["server_id"]) for server in servers}
        sid = current_server_id()
        if sid not in allowed:
            sid = str(servers[0]["server_id"])
            session["server_id"] = sid
        return redirect(url_for("user_profile", user_id=display_user["id"]))
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
        # server_id 来自上传文件内容，会被拼进数据目录路径，必须限制为纯数字 ID。
        if not sid.isdigit():
            return "JSON 中的 server_id 无效", 400
        target = server_db_path(sid)
        # 权限 2 用户只能新建/管理自己拥有访问权的服务器，且下载服务器数量受 quota 限制。
        existing = get_portal_db().execute("SELECT 1 FROM servers WHERE server_id=?", (sid,)).fetchone()
        if not existing and admin_level(uid) == 2:
            used = get_portal_db().execute("SELECT count(*) FROM user_server_access WHERE user_id=?", (uid,)).fetchone()[0]
            if used >= get_download_quota(uid):
                return "已达到服务器配额，请联系权限1管理员增加配额", 403
        import_json_to_db(temp, target, server_id=sid)
        register_server(sid, meta.get("server_name") or f"Discord Server {sid}", meta.get("icon_url"), owner_user_id=uid, db_path=target)
        get_portal_db().execute("INSERT OR IGNORE INTO user_server_access(user_id,server_id,granted_by,created_at) VALUES(?,?,?,?)", (uid, sid, uid, utc_now_iso()))
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


@app.after_request
def _no_cache_html(response):
    # 动态页面禁止启发式缓存，防止切换服务器后浏览器/CDN 仍展示旧服务器内容。
    ct = response.headers.get("Content-Type", "")
    if ct.startswith("text/html"):
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        response.headers["Pragma"] = "no-cache"
        response.headers["Vary"] = "Cookie"
    return response


@app.route("/")
def index():
    if "user" not in session:
        if is_discord_activity_request():
            return redirect(url_for("login", **request.args.to_dict()))
        return redirect(url_for("welcome"))
    display_user = get_display_user() or session["user"]
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
    u = display_user
    now = utc_now_iso()
    conn.execute("INSERT INTO web_visitors(user_id,username,nickname,avatar_url,last_visit) VALUES(?,?,?,?,?) ON CONFLICT(user_id) DO UPDATE SET username=excluded.username,nickname=excluded.nickname,avatar_url=excluded.avatar_url,last_visit=excluded.last_visit", (u["id"], u["username"], u.get("nickname") or u["username"], u.get("avatar"), now))
    conn.commit()
    data = data_engine.load_or_compute(sid)
    visitors = conn.execute("SELECT * FROM web_visitors ORDER BY last_visit DESC").fetchall()
    server = get_portal_db().execute("SELECT * FROM servers WHERE server_id=?", (sid,)).fetchone()
    data = dict(data)
    full_leaderboard = data.pop("full_leaderboard", [])
    return render_template("index.html", server=server, server_id=sid, current_user=u, site_visitors=visitors, full_leaderboard=full_leaderboard, **data)


@app.route("/api/leaderboard")
@server_required
def api_leaderboard():
    page = request_page()
    offset = (page - 1) * 50
    # Page one is already part of the homepage snapshot. This endpoint is
    # opened from that page, so querying the same 50 users again only adds
    # latency (and used to run one reaction query per user).
    # Avoid even the cheap freshness check here: the homepage request just
    # served this snapshot and the cache refresh is already stale-while-refresh.
    snapshot = data_engine._load_cache(current_server_id()).get("homepage") or {}
    if page != 1 or not snapshot.get("full_leaderboard"):
        snapshot = data_engine.load_or_compute(current_server_id())
    if page == 1 and snapshot.get("full_leaderboard"):
        return jsonify([
            dict(user, rank=index)
            for index, user in enumerate(snapshot["full_leaderboard"], 1)
        ])
    conn = get_db()
    rows = conn.execute(
        "SELECT u.*,count(m.message_id) msg_count FROM users u JOIN messages m "
        "ON u.user_id=m.author_id GROUP BY u.user_id ORDER BY msg_count DESC LIMIT 50 OFFSET ?",
        (offset,),
    ).fetchall()
    ids = [row["user_id"] for row in rows]
    emoji_map = collections.defaultdict(list)
    # The homepage snapshot supplies emoji badges for its first page. For
    # subsequent lazy-loaded pages, avoid another large reaction aggregation;
    # the badges are decorative and an empty list keeps the API responsive.
    if ids and page == 1:
        placeholders = ",".join("?" for _ in ids)
        emoji_rows = conn.execute(
            f"SELECT m.author_id,r.emoji_url,r.emoji_name,count(*) c FROM reactions r "
            f"JOIN messages m ON r.message_id=m.message_id WHERE m.author_id IN ({placeholders}) "
            f"GROUP BY m.author_id,r.emoji_name ORDER BY m.author_id,c DESC", ids
        ).fetchall()
        for emoji in emoji_rows:
            if len(emoji_map[emoji["author_id"]]) < 3:
                emoji_map[emoji["author_id"]].append(dict(emoji))
    users = []
    for i, row in enumerate(rows):
        d = dict(row)
        d["rank"] = offset + i + 1
        d["top_emojis"] = emoji_map.get(row["user_id"], [])
        users.append(d)
    return jsonify(users)


@app.route("/search")
@server_required
def search():
    query = request.args.get("q", "").strip()
    conn = get_db()
    results = conn.execute("SELECT * FROM users WHERE user_id=? OR username LIKE ? OR nickname LIKE ? LIMIT 20", (query, f"%{query}%", f"%{query}%")).fetchall()
    data = dict(data_engine.load_or_compute(current_server_id()))
    data["full_leaderboard"] = []
    return render_template("index.html", server_id=current_server_id(), current_user=session["user"], search_results=results, query=query, site_visitors=[], **data)


@app.route("/user/<user_id>")
@server_required
def user_profile(user_id):
    sid = current_server_id()
    conn = get_db()
    user_id = str(user_id)
    merge = conn.execute("SELECT parent_id FROM user_merges WHERE target_id=?", (user_id,)).fetchone()
    if merge:
        return redirect(url_for("user_profile", user_id=merge["parent_id"]))
    user = conn.execute("SELECT * FROM users WHERE user_id=?", (user_id,)).fetchone()
    if not user:
        return "User Not Found", 404
    visitor = session["user"]
    if visitor["id"] != str(user_id):
        conn.execute("INSERT INTO profile_views(target_user_id,viewer_user_id,viewer_name,viewer_avatar,timestamp) VALUES(?,?,?,?,?) ON CONFLICT(target_user_id,viewer_user_id) DO UPDATE SET timestamp=excluded.timestamp", (user_id, visitor["id"], visitor["username"], visitor["avatar"], utc_now_iso()))
        conn.commit()
    view_count = conn.execute("SELECT count(*) FROM profile_views WHERE target_user_id=?", (user_id,)).fetchone()[0]
    recent_viewers = conn.execute("SELECT viewer_name,viewer_avatar,timestamp FROM profile_views WHERE target_user_id=? ORDER BY timestamp DESC LIMIT 20", (user_id,)).fetchall()
    profile = profile_engine.load_or_compute(sid, user_id)
    merged_ids = profile["merged_ids"]
    ph = ",".join("?" for _ in merged_ids)
    # Time order can use the author/timestamp index and keeps the first paint
    # responsive. Hot order remains available on demand for users who need it.
    sort_by = request.args.get("sort", "time")
    active_tab = request.args.get("tab", "threads") if request.args.get("tab") in ("threads", "messages") else "threads"
    page = request_page()
    offset = (page - 1) * ITEMS_PER_PAGE
    order = "ORDER BY total_reactions DESC,m.timestamp DESC" if sort_by == "hot" else "ORDER BY m.timestamp DESC"
    messages = []
    if active_tab == "messages":
        if sort_by == "hot":
            message_query = (
                f"SELECT m.*,t.name thread_name,(SELECT count(*) FROM reactions "
                f"WHERE message_id=m.message_id) total_reactions FROM messages m "
                f"JOIN threads t ON m.thread_id=t.thread_id WHERE m.author_id IN ({ph}) "
                f"ORDER BY total_reactions DESC,m.timestamp DESC LIMIT ? OFFSET ?"
            )
        else:
            # Correlated reaction counts are evaluated only for the limited
            # newest rows. A LEFT JOIN + GROUP BY forces SQLite to aggregate
            # every message before it can apply this timestamp order.
            message_query = (
                f"SELECT m.*,t.name thread_name,(SELECT count(*) FROM reactions "
                f"WHERE message_id=m.message_id) total_reactions FROM messages m "
                f"JOIN threads t ON m.thread_id=t.thread_id WHERE m.author_id IN ({ph}) "
                f"ORDER BY m.timestamp DESC LIMIT ? OFFSET ?"
            )
        messages = process_messages(conn, conn.execute(
            message_query, (*merged_ids, ITEMS_PER_PAGE, offset)
        ).fetchall())
    # Keep the thread lookup bounded to the user's own candidate messages;
    # reaction icons are fetched in one batch below instead of one query per
    # returned thread.
    my_threads = []
    if active_tab == "threads":
        thread_rows = conn.execute(
            f"SELECT t.thread_id,t.name,m.timestamp created_at,"
            f"(SELECT count(*) FROM messages WHERE thread_id=t.thread_id) reply_count,"
            f"(SELECT content FROM messages WHERE thread_id=t.thread_id ORDER BY timestamp,message_id LIMIT 1) first_content,"
            f"(SELECT message_id FROM messages WHERE thread_id=t.thread_id ORDER BY timestamp,message_id LIMIT 1) op_msg_id "
            f"FROM threads t JOIN messages m ON t.thread_id=m.thread_id "
            f"WHERE m.author_id IN ({ph}) AND m.timestamp=(SELECT min(timestamp) FROM messages WHERE thread_id=t.thread_id) "
            f"ORDER BY {('reply_count DESC' if sort_by == 'hot' else 'm.timestamp DESC')} LIMIT ? OFFSET ?",
            (*merged_ids, ITEMS_PER_PAGE, offset),
        ).fetchall()
        op_message_ids = [row["op_msg_id"] for row in thread_rows]
        emoji_map = {}
        if op_message_ids:
            message_ph = ",".join("?" for _ in op_message_ids)
            for emoji in conn.execute(
                f"SELECT message_id,emoji_url,emoji_name,count(*) c FROM reactions WHERE message_id IN ({message_ph}) "
                f"GROUP BY message_id,emoji_name ORDER BY message_id,c DESC", op_message_ids
            ):
                emoji_map.setdefault(emoji["message_id"], emoji)
        for row in thread_rows:
            d = dict(row)
            d["op_user"] = dict(user)
            emoji = emoji_map.get(row["op_msg_id"])
            d["top_emoji_url"] = emoji["emoji_url"] if emoji else None
            d["top_emoji_name"] = emoji["emoji_name"] if emoji else None
            d["top_emoji_count"] = emoji["c"] if emoji else 0
            my_threads.append(d)
    msg_count = profile["msg_count"]
    thread_count = profile["thread_count"]
    return render_template(
        "user.html", user=user, messages=messages, my_threads=my_threads,
        view_count=view_count, recent_viewers=recent_viewers, server_id=sid,
        current_sort=sort_by, current_page=page, active_tab=active_tab,
        total_msg_pages=math.ceil(msg_count / ITEMS_PER_PAGE),
        total_thread_pages=math.ceil(thread_count / ITEMS_PER_PAGE),
        current_user=session["user"], msg_count=msg_count,
        reaction_received_count=profile["reaction_received_count"],
        profile_ready=profile.get("complete", False),
        top_emojis_given=profile["top_emojis_given"],
        top_emojis_received=profile["top_emojis_received"],
        interactions_incoming=profile["interactions_incoming"],
        interactions_outgoing=profile["interactions_outgoing"],
        chart_daily=profile["chart_daily"], chart_hourly=profile["chart_hourly"],
        word_cloud_data=profile["word_cloud_data"],
    )


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
        conn.execute("INSERT INTO claim_requests_v2(requester_id,target_id,target_name,created_at) VALUES(?,?,?,?)", (requester_id, target_id, target["nickname"], utc_now_iso()))
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
    # 时间字段由后端统一计算；模板和后续轮询接口只负责展示结果。
    task_views = []
    for task in tasks:
        view = dict(task)
        calculated_at = datetime.now(timezone.utc)
        timing = calculate_task_timing(view, calculated_at)
        view.update(timing)
        task_views.append(view)
    tasks = task_views

    bot_rows = portal.execute("SELECT id,name,token_type,owner_user_id,created_at FROM download_bots WHERE owner_user_id=? ORDER BY id", (uid,)).fetchall()
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


@app.route("/admin/approve/<int:req_id>", methods=["POST"])
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
        conn.execute("INSERT OR REPLACE INTO user_merges(target_id,parent_id,created_at) VALUES(?,?,?)", (req["target_id"], req["requester_id"], utc_now_iso()))
        conn.commit()
    return redirect(url_for("admin_panel"))


@app.route("/admin/unmerge/<target_id>", methods=["POST"])
@login_required
@admin_required
def admin_unmerge(target_id):
    scope = _admin_server_scope()
    if isinstance(scope, tuple):
        return scope
    conn = get_db(); conn.execute("DELETE FROM user_merges WHERE target_id=?", (target_id,)); conn.commit(); return redirect(url_for("admin_panel"))


@app.route("/admin/reset_all_claims", methods=["POST"])
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
    portal.execute("INSERT OR IGNORE INTO whitelist_users(user_id,username,added_by,created_at) VALUES(?,?,?,?)", (uid, name, session["user"]["id"], utc_now_iso()))
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
    uid=str(request.form.get("user_id",""))
    try:
        quota=max(1,min(100,int(request.form.get("quota","1"))))
    except (TypeError, ValueError):
        return "配额必须是数字",400
    portal=get_portal_db(); portal.execute("INSERT INTO server_download_quota(user_id,quota) VALUES(?,?) ON CONFLICT(user_id) DO UPDATE SET quota=excluded.quota",(uid,quota)); portal.commit(); return redirect(url_for("admin_panel"))

@app.route("/admin/access", methods=["POST"])
@login_required
@level1_required
def admin_access():
    uid=str(request.form.get("user_id","")); sid=str(request.form.get("server_id",""))
    if uid and sid and get_portal_db().execute("SELECT 1 FROM servers WHERE server_id=?",(sid,)).fetchone():
        p=get_portal_db(); p.execute("INSERT OR IGNORE INTO user_server_access(user_id,server_id,granted_by,created_at) VALUES(?,?,?,?)",(uid,sid,session["user"]["id"],utc_now_iso())); p.commit()
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
    p=get_portal_db(); owner=str(session["user"]["id"])
    token=request.form.get("token","").strip()
    if not token: return "Token 不能为空",400
    token_type=str(request.form.get("token_type", "bot")).strip().lower()
    if token_type not in ("bot", "user"):
        return "账号类型无效",400
    try:
        # 不相信手动填写的名称，避免后续配置显示错误账号。
        name=bot_identity(token, token_type)
    except requests.RequestException:
        return "无法验证机器人密钥，请确认 Token 有效后重试",400
    p.execute("INSERT INTO download_bots(owner_user_id,name,token,token_type,created_at) VALUES(?,?,?,?,?)",(owner,name,token,token_type,utc_now_iso())); p.commit(); return redirect(url_for("admin_panel"))

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
    try:
        scheduler_interval=max(50,min(60000,int(request.form.get("scheduler_interval") or 250)))
        download_interval_ms=max(0,min(60000,int(request.form.get("download_interval_ms") or 0)))
    except (TypeError,ValueError):
        return "调度间隔和下载间隔必须是数字",400
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
        ok,reason=check_bot_forum_access(bot["token"],guild,forum,bot.get("token_type", "bot"))
        if not ok: failures.append(f"{bot['name']}：{reason}")
    if failures:
        flash("下载配置未保存。以下机器人没有目标 Forum 的访问权限：\n"+"\n".join(failures),"error"); return redirect(url_for("admin_panel"))
    managed={str(x.get("id")):x for x in managed_guilds_from_session()}
    guild_name=(managed.get(guild) or {}).get("name") or (p.execute("SELECT name FROM servers WHERE server_id=?",(guild,)).fetchone() or {"name":guild})["name"]
    forum_name=forum
    try:
        # 由用户的 OAuth guild/channel 资源获取权威 Forum 名称。
        for bot in selected[:1]:
            resp=discord_get(f"/guilds/{guild}/channels",bot["token"],bot.get("token_type", "bot"),timeout=12)
            if resp.ok:
                ch=next((x for x in resp.json() if str(x.get("id"))==forum),None)
                if ch: forum_name=ch.get("name") or forum
                break
    except requests.RequestException as exc:
        app.logger.warning("查询 Forum 名称失败 guild_id=%s forum_id=%s，使用 ID 占位 error=%s", guild, forum, exc)
    server_key=f"{guild}:{forum}"
    p.execute("INSERT INTO download_configs(server_id,owner_user_id,guild_id,forum_channel_id,guild_name,forum_name,enabled,use_default_bot,scheduler_interval,download_interval_ms,update_enabled,updated_at) VALUES(?,?,?,?,?,?,1,?,?,?,?,?) ON CONFLICT(guild_id,forum_channel_id) DO UPDATE SET owner_user_id=excluded.owner_user_id,server_id=excluded.server_id,guild_name=excluded.guild_name,forum_name=excluded.forum_name,enabled=1,use_default_bot=excluded.use_default_bot,scheduler_interval=excluded.scheduler_interval,download_interval_ms=excluded.download_interval_ms,update_enabled=excluded.update_enabled,updated_at=excluded.updated_at",(server_key,uid,guild,forum,guild_name,forum_name,1 if use_default else 0,scheduler_interval,download_interval_ms,update_enabled,utc_now_iso()))
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
    # 机器人 Token 只在服务端使用；不要通过 HTTP 接口回传给浏览器。
    return jsonify({"servers":[dict(r) for r in rows],"bots":[dict(r) for r in p.execute("SELECT id,name FROM download_bots WHERE owner_user_id=? ORDER BY id",(uid,)).fetchall()]})

@app.route("/api/managed-discord-resources")
@login_required
@admin_required
def managed_discord_resources():
    """Populate the download form from Discord instead of asking for IDs."""
    uid = str(session["user"]["id"])
    guilds = managed_guilds_from_session()
    bots = [dict(row) for row in get_portal_db().execute("SELECT id,name,token,token_type FROM download_bots WHERE owner_user_id=?", (uid,)).fetchall()]
    default_token = default_downloader_token()
    if default_token:
        bots.append({"id": "default", "name": "默认下载机器人", "token": default_token})

    cache_key = (uid, tuple(sorted(str(guild.get("id")) for guild in guilds)), tuple(sorted(str(bot.get("id")) for bot in bots)))
    now = time.monotonic()
    with _managed_resources_cache_lock:
        cached = _managed_resources_cache.get(cache_key)
        if cached and now - cached["created_at"] < _MANAGED_RESOURCES_TTL:
            return jsonify(cached["payload"])

    forums = {}
    warnings = []
    seen_forums = set()

    def fetch_forums(bot, guild):
        try:
            response = discord_get(f"/guilds/{guild['id']}/channels", bot["token"], bot.get("token_type", "bot"), timeout=12)
            if response.status_code in (401, 403, 404):
                return guild["id"], [], None
            response.raise_for_status()
            channels = [
                {"id": channel["id"], "name": channel.get("name") or channel["id"]}
                for channel in response.json() if channel.get("type") == 15
            ]
            return guild["id"], channels, None
        except requests.RequestException as exc:
            return guild["id"], [], f"{bot['name']} 无法读取频道：{exc}"

    # Channel lists are independent requests. Running them concurrently keeps
    # an admin page with several managed guilds/bots from waiting on a long
    # serial chain of Discord round trips.
    jobs = []
    with ThreadPoolExecutor(max_workers=min(8, max(1, len(bots) * len(guilds)))) as executor:
        for bot in bots:
            for guild in guilds:
                jobs.append(executor.submit(fetch_forums, bot, guild))
        for job in as_completed(jobs):
            guild_id, channels, warning = job.result()
            if warning:
                warnings.append(warning)
            for channel in channels:
                key=(str(guild_id),str(channel["id"]))
                if key in seen_forums:
                    continue
                seen_forums.add(key)
                forums.setdefault(str(guild_id), []).append(channel)
    # Tokens are used only on the server; never expose them to the browser.
    payload = {"guilds": guilds, "forums": forums, "warnings": warnings}
    with _managed_resources_cache_lock:
        _managed_resources_cache[cache_key] = {"created_at": time.monotonic(), "payload": payload}
    return jsonify(payload)

# ---- 年度报告缓存（stale-while-revalidate）----
# 单次 query 30+ 个 460 万行聚合，磁盘满时 5-10s。缓存 5 分钟供快速二次访问。
_REPORT_CACHE = {}
_REPORT_TTL = 300  # seconds

@app.route("/report")
@server_required
def report():
    sid = current_server_id(); conn = get_db()
    u = get_display_user() or session["user"]
    uid = str(u["id"])
    cache_key = f"{sid}:{uid}"
    now_ts = time.time()
    cached = _REPORT_CACHE.get(cache_key)
    if cached and now_ts - cached["ts"] < _REPORT_TTL:
        # 缓存命中，瞬时返回
        return render_template("report.html", user=cached["db_user"], server_id=sid,
                               word_cloud_data=cached["word_cloud_data"], percentile=95, **cached["result"])
    db_user = conn.execute("SELECT * FROM users WHERE user_id=?", (uid,)).fetchone()
    if not db_user:
        return redirect(url_for("welcome"))
    ids = data_engine.get_merged_ids(sid, uid); ph = ",".join("?" for _ in ids)
    result = {"join_date": "未知", "most_active_day": None, "latest_msg": None, "most_replied_thread": None, "most_active_topic": None, "most_liked_msg": None, "top_friend_incoming": None, "top_friend_outgoing": None}
    row = conn.execute(f"SELECT min(timestamp) joined FROM messages WHERE author_id IN ({ph})", ids).fetchone()
    if row and row["joined"]: result["join_date"] = datetimeformat_filter(row["joined"], "%Y-%m-%d")
    result["most_active_day"] = dict(conn.execute(f"SELECT substr(timestamp,1,10) day,count(*) c FROM messages WHERE author_id IN ({ph}) GROUP BY day ORDER BY c DESC LIMIT 1", ids).fetchone() or {}) or None
    # 之前: strftime('%H', timestamp) 走全表扫 (4s+)。改为：取最近 200 条消息后，在 Python 端按小时过滤 16-21 点
    late = conn.execute(
        f"SELECT * FROM messages WHERE author_id IN ({ph}) ORDER BY timestamp DESC LIMIT 200", ids
    ).fetchall()
    late = next((dict(m) for m in late if 16 <= int((m["timestamp"] or "T00:00:00")[11:13]) <= 21), None) if late else None
    if late:
        t = conn.execute("SELECT name FROM threads WHERE thread_id=?", (late["thread_id"],)).fetchone(); late["thread_name"] = t["name"] if t else "Unknown"; result["latest_msg"] = late
    row = conn.execute(f"SELECT t.thread_id,t.name,count(*) reply_count FROM threads t JOIN messages m ON t.thread_id=m.thread_id WHERE m.author_id IN ({ph}) GROUP BY t.thread_id ORDER BY reply_count DESC LIMIT 1", ids).fetchone()
    if row: result["most_replied_thread"] = dict(row); result["most_replied_thread"]["op_user"] = dict(db_user)
    row = conn.execute(f"SELECT t.thread_id,t.name,count(*) c FROM messages m JOIN threads t ON m.thread_id=t.thread_id WHERE m.author_id IN ({ph}) GROUP BY t.thread_id ORDER BY c DESC LIMIT 1", ids).fetchone()
    result["most_active_topic"] = dict(row) if row else None
    # 之前: LEFT JOIN reactions r 全表扫。改为先按 author 索引取最近 N 条消息，再按 message_id 查反应
    top_msg_row = conn.execute(
        f"SELECT m.message_id, m.content, m.timestamp, m.thread_id, t.name AS thread_name "
        f"FROM messages m LEFT JOIN threads t ON t.thread_id=m.thread_id "
        f"WHERE m.author_id IN ({ph}) "
        f"ORDER BY m.timestamp DESC LIMIT 2000", ids
    ).fetchall()
    if top_msg_row:
        ids_msgs = tuple(m["message_id"] for m in top_msg_row)
        ph_m = ",".join("?" * len(ids_msgs))
        rx_rows = conn.execute(
            f"SELECT message_id, count(*) rc FROM reactions WHERE message_id IN ({ph_m}) GROUP BY message_id",
            ids_msgs
        ).fetchall()
        rx_map = {r["message_id"]: r["rc"] for r in rx_rows}
        top_msg = max(top_msg_row, key=lambda m: rx_map.get(m["message_id"], 0))
        d = dict(top_msg)
        d["detailed_reactions"] = [dict(x) for x in conn.execute(
            "SELECT emoji_url,emoji_name,count(*) count FROM reactions WHERE message_id=? GROUP BY emoji_name ORDER BY count DESC LIMIT 3",
            (d["message_id"],))]
        result["most_liked_msg"] = d
        texts = [m["content"] for m in top_msg_row]
    else:
        texts = []
    # 互动好友：双向统计 —— outgoing=我提及/回复的人；incoming=提及/回复我的人
    ph_ids = list(ids)
    out_rows = conn.execute(
        f"SELECT mentioned_user_id target_id, count(*) score FROM mentions WHERE author_id IN ({ph}) "
        "AND mentioned_user_id NOT IN (" + ",".join("?" for _ in ph_ids) + ") GROUP BY target_id ORDER BY score DESC LIMIT 1",
        ph_ids + ph_ids,
    ).fetchone()
    if not out_rows or not out_rows["score"]:
        # 没有 mention 数据时退化用回复关系
        out_rows = conn.execute(
            f"SELECT m2.author_id target_id, count(*) score FROM messages m1 JOIN messages m2 "
            f"ON m1.reply_to_msg_id=m2.message_id AND m2.author_id NOT IN ({ph}) "
            f"WHERE m1.author_id IN ({ph}) GROUP BY m2.author_id ORDER BY score DESC LIMIT 1",
            ph_ids * 2,
        ).fetchone()
    in_rows = conn.execute(
        f"SELECT m.author_id target_id, count(*) score FROM mentions mn JOIN messages m ON mn.message_id=m.message_id "
        "WHERE mn.mentioned_user_id IN (" + ",".join("?" for _ in ph_ids) + ") "
        "AND m.author_id NOT IN (" + ",".join("?" for _ in ph_ids) + ") GROUP BY m.author_id ORDER BY score DESC LIMIT 1",
        ph_ids * 2,
    ).fetchone()
    if not in_rows or not in_rows["score"]:
        in_rows = conn.execute(
            f"SELECT m1.author_id target_id, count(*) score FROM messages m2 JOIN messages m1 "
            f"ON m2.reply_to_msg_id=m1.message_id AND m1.author_id NOT IN ({ph}) "
            f"WHERE m2.author_id IN ({ph}) GROUP BY m1.author_id ORDER BY score DESC LIMIT 1",
            ph_ids * 2,
        ).fetchone()

    def _friend_row(row):
        if not row:
            return None
        u = conn.execute("SELECT * FROM users WHERE user_id=?", (row["target_id"],)).fetchone()
        if not u:
            return None
        d = dict(u)
        d["score"] = row["score"]
        return d

    result["top_friend_outgoing"] = _friend_row(out_rows)
    result["top_friend_incoming"] = _friend_row(in_rows)
    _REPORT_CACHE[cache_key] = {
        "ts": time.time(), "db_user": db_user,
        "word_cloud_data": format_word_cloud(get_word_cloud_counter(texts), 50),
        "result": result,
    }
    return render_template("report.html", user=db_user, server_id=sid, word_cloud_data=format_word_cloud(get_word_cloud_counter(texts), 50), percentile=95, **result)


@app.route("/chouxiangpai")
@server_required
def chouxiangpai_page():
    path = os.path.join(BASE_DIR, "templates", "chouxiangpai.html")
    if os.path.exists(path):
        return open(path, encoding="utf-8").read()
    return ""


def _downloader_process_alive():
    with _service_lock:
        live = []
        found = False
        for name, proc in _service_processes:
            if proc.poll() is None:
                live.append((name, proc))
                if name == "discord_downloader":
                    found = True
        _service_processes[:] = live
        return found

def ensure_downloader_process():
    with _service_lock:
        if any(name == "discord_downloader" and proc.poll() is None for name, proc in _service_processes):
            return
        _start_child([sys.executable,os.path.join(BASE_DIR,"Preparation_Before_Use","discord_downloader.py")],"discord_downloader")

def request_member_sync(guild_id, requested_by):
    p=get_portal_db()
    active=p.execute("SELECT id FROM member_sync_requests WHERE guild_id=? AND status IN ('pending','running') ORDER BY id DESC LIMIT 1",(str(guild_id),)).fetchone()
    if active:
        return int(active["id"])
    cur=p.execute("INSERT INTO member_sync_requests(guild_id,requested_by,status,created_at) VALUES(?,?, 'pending', ?)",(str(guild_id),str(requested_by),utc_now_iso()))
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
    nowv=utc_now_iso()
    mode="update" if int(cfg["update_enabled"] or 0) and os.path.exists(server_db_path(str(cfg["guild_id"]))) else "initial"
    p.execute("INSERT INTO download_tasks(guild_id,forum_channel_id,created_by,status,total,completed,created_at,message,config_id,guild_name,forum_name,scheduler_interval,download_interval_ms,mode) VALUES(?,?,?,?,0,0,?,?,?,?,?,?,?,?)",(cfg["guild_id"],cfg["forum_channel_id"],uid,"pending",nowv,f"等待下载器启动 · {'更新模式' if mode=='update' else '首次下载模式'}",cfg["id"],cfg["guild_name"] or cfg["guild_id"],cfg["forum_name"] or cfg["forum_channel_id"],cfg["scheduler_interval"] if int(cfg["scheduler_interval"] or 0) >= 50 else int(cfg["scheduler_interval"] or 1) * 1000,int(cfg["download_interval_ms"] or 0),mode))
    p.commit();
    request_member_sync(str(cfg["guild_id"]), uid)
    ensure_downloader_process(); return redirect(url_for("admin_panel"))

@app.route("/admin/download-task/<int:task_id>/bot/add", methods=["POST"])
@login_required
@admin_required
def admin_download_task_bot_add(task_id):
    """把一个已验证的下载机器人加入正在运行的任务配置。"""
    p=get_portal_db(); uid=str(session["user"]["id"]); task=p.execute("SELECT * FROM download_tasks WHERE id=?",(task_id,)).fetchone()
    wants_json=request.headers.get("X-Requested-With")=="XMLHttpRequest" or request.accept_mimetypes.best=="application/json"
    def response(message, status=200):
        if wants_json:
            return jsonify({"ok":status < 400, "message":message}), status
        flash(message, "error" if status >= 400 else "success")
        return redirect(url_for("admin_panel"))

    if not task or not download_task_scope(task):
        return response("你没有权限操作这个下载任务",403)
    if task["status"] not in ("pending","running"):
        return response("只有排队中或下载中的任务可以加入机器人",400)
    config_id=task["config_id"]
    cfg=p.execute("SELECT * FROM download_configs WHERE id=? AND enabled=1",(config_id,)).fetchone() if config_id else None
    if not cfg:
        return response("下载配置不存在或已禁用",400)
    bot_id=str(request.form.get("bot_id","")).strip()
    if bot_id=="default":
        token=default_downloader_token()
        if not token:
            return response("服务器未配置默认下载机器人",400)
        bot={"id":"default","name":"默认下载机器人","token":token,"token_type":"bot"}
        already=bool(cfg["use_default_bot"])
    elif bot_id.isdigit():
        query="SELECT id,name,token,token_type,owner_user_id FROM download_bots WHERE id=?"
        row=p.execute(query,(int(bot_id),)).fetchone()
        if not row:
            return response("下载机器人不存在",404)
        if admin_level(uid)!=1 and str(row["owner_user_id"])!=uid:
            return response("只能加入自己拥有的下载机器人",403)
        bot=dict(row); already=bool(p.execute("SELECT 1 FROM download_config_bots WHERE config_id=? AND bot_id=?",(config_id,int(bot_id))).fetchone())
    else:
        return response("下载机器人选择无效",400)
    if already:
        return response(f"{bot['name']} 已在此下载配置中")
    configured_count=p.execute("SELECT COUNT(*) FROM download_config_bots WHERE config_id=?",(config_id,)).fetchone()[0] + int(cfg["use_default_bot"] or 0)
    ok,reason=check_bot_forum_access(bot["token"],str(task["guild_id"]),str(task["forum_channel_id"]),bot.get("token_type", "bot"))
    if not ok:
        return response(f"{bot['name']} 无法访问目标 Forum：{reason}",400)
    if bot_id=="default":
        p.execute("UPDATE download_configs SET use_default_bot=1,updated_at=? WHERE id=?",(utc_now_iso(),config_id))
    else:
        p.execute("INSERT OR IGNORE INTO download_config_bots(config_id,bot_id) VALUES(?,?)",(config_id,int(bot_id)))
    p.commit(); ensure_downloader_process()
    return response(f"已加入 {bot['name']}。下载器会在当前批次结束后启动它。")

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
    nowv=utc_now_iso()
    if action=="pause" and task["status"] in ("pending","running"):
        timing = calculate_task_timing(task)
        p.execute("UPDATE download_tasks SET status='paused',phase='paused',message='任务已暂停，可继续下载',finished_at=NULL,active_started_at=NULL,elapsed_seconds=?,estimated_seconds=?,speed=? WHERE id=?",(timing["elapsed_seconds"],timing["estimated_seconds"],timing["speed"],task_id))
    elif action=="resume" and task["status"]=="paused":
        p.execute("UPDATE download_tasks SET status='pending',phase='queued',message='等待下载器继续执行',finished_at=NULL,active_started_at=NULL,delete_requested=0 WHERE id=?",(task_id,)); p.commit(); ensure_downloader_process(); return redirect(url_for("admin_panel"))
    elif action=="cancel" and task["status"] in ("pending","running","paused"):
        timing = calculate_task_timing(task)
        p.execute("UPDATE download_tasks SET status='cancelled',phase='cancelled',message='任务已取消',finished_at=?,delete_requested=0,active_started_at=NULL,elapsed_seconds=?,estimated_seconds=?,speed=? WHERE id=?",(nowv,timing["elapsed_seconds"],timing["estimated_seconds"],timing["speed"],task_id))
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
        p.execute("UPDATE download_tasks SET status='cancelled',phase='cancelled',delete_requested=1,message='正在停止并清理任务…',finished_at=? WHERE id=?",(utc_now_iso(),task_id)); p.commit()

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
    p=get_portal_db()
    uid=str(session["user"]["id"]); level=admin_level(uid)
    scope_sql = ""
    scope_args = ()
    if level != 1:
        scope_sql = " AND (t.created_by=? OR EXISTS (SELECT 1 FROM download_configs scoped_c WHERE scoped_c.id=t.config_id AND scoped_c.owner_user_id=?))"
        scope_args = (uid, uid)
    rows=p.execute(
        "SELECT t.*,COALESCE(c.guild_name,s.name,t.guild_id) server_display_name,"
        "COALESCE(c.forum_name,t.forum_name,t.forum_channel_id) forum_display_name,"
        "COALESCE(c.use_default_bot,0) config_use_default_bot "
        "FROM download_tasks t LEFT JOIN download_configs c ON c.id=t.config_id "
        "LEFT JOIN servers s ON s.server_id=t.guild_id WHERE t.delete_requested=0" + scope_sql +
        " ORDER BY t.id DESC LIMIT 100", scope_args
    ).fetchall()
    result=[]
    task_ids = [x["id"] for x in rows]
    item_stats = {}
    bot_progress = collections.defaultdict(list)
    if task_ids:
        task_ph = ",".join("?" for _ in task_ids)
        for item in p.execute(
            f"SELECT task_id,COUNT(*) total_items,"
            f"SUM(CASE WHEN status='downloaded' THEN 1 ELSE 0 END) downloaded_items,"
            f"SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) failed_items,"
            f"SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END) pending_items "
            f"FROM download_task_items WHERE task_id IN ({task_ph}) GROUP BY task_id", task_ids
        ):
            item_stats[item["task_id"]] = dict(item)
        for bot in p.execute(
            f"SELECT task_id,bot_name,COUNT(*) count FROM download_task_items "
            f"WHERE task_id IN ({task_ph}) AND bot_name IS NOT NULL "
            f"GROUP BY task_id,bot_name ORDER BY task_id,count DESC", task_ids
        ):
            bot_progress[bot["task_id"]].append({"name": bot["bot_name"], "count": int(bot["count"] or 0)})
    available_rows = p.execute(
        "SELECT id,name FROM download_bots" + (" WHERE owner_user_id=?" if level != 1 else "") + " ORDER BY id",
        (uid,) if level != 1 else (),
    ).fetchall()
    available = [{"id": str(row["id"]), "name": row["name"]} for row in available_rows]
    cfg_ids = [x["config_id"] for x in rows if x["config_id"]]
    configured = collections.defaultdict(set)
    if cfg_ids:
        cfg_ph = ",".join("?" for _ in cfg_ids)
        for config_bot in p.execute(
            f"SELECT config_id,bot_id FROM download_config_bots WHERE config_id IN ({cfg_ph})", cfg_ids
        ):
            configured[config_bot["config_id"]].add(str(config_bot["bot_id"]))
    has_default_token = bool(default_downloader_token())
    for x in rows:
        item = item_stats.get(x["id"], {})
        d=dict(x)
        d.update({"total_items": 0, "downloaded_items": 0, "failed_items": 0, "pending_items": 0})
        d.update({k:int(v or 0) for k,v in item.items()})
        if d["total_items"]:
            d["total"] = max(int(d.get("total") or 0), d["total_items"])
            d["completed"] = max(0, d["total_items"] - d["pending_items"] - d["failed_items"])
        calculated_at = datetime.now(timezone.utc)
        timing = calculate_task_timing(d, calculated_at)
        d.update(timing)
        d["bot_progress"] = bot_progress.get(x["id"], [])
        configured_ids = configured.get(x["config_id"], set()).copy()
        if x["config_use_default_bot"]:
            configured_ids.add("default")
        d["configured_bot_ids"]=sorted(configured_ids)
        d["available_bots"]=[bot for bot in available if bot["id"] not in configured_ids]
        if "default" not in configured_ids and has_default_token:
            d["available_bots"].insert(0,{"id":"default","name":"默认下载机器人"})
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
        "csrf_token": csrf_token,
    }


init_portal_db()
migrate_legacy_db()
with app.app_context():
    ensure_server_indexes()
    sync_all_server_users()

_service_processes = []
_service_stop = threading.Event()
_service_lock = threading.RLock()

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
        with _service_lock:
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


def _has_active_download_tasks():
    conn = db_connect(PORTAL_DB)
    try:
        return bool(conn.execute(
            "SELECT 1 FROM download_tasks WHERE status IN ('pending','running') AND delete_requested=0 LIMIT 1"
        ).fetchone())
    finally:
        conn.close()


def _ensure_whitelist_process():
    with _service_lock:
        if any(name == "whitelist_bot" and proc.poll() is None for name, proc in _service_processes):
            return
    if os.getenv("DISCORD_BOT_TOKEN", "").strip():
        _start_child([sys.executable, os.path.join(BASE_DIR, "Preparation_Before_Use", "whitelist_bot.py")], "whitelist_bot")


def _service_watchdog():
    """Keep child services available after an unexpected process exit."""
    while not _service_stop.wait(5):
        try:
            if _has_active_download_tasks() and not _downloader_process_alive():
                app.logger.warning("检测到下载任务仍在运行但下载器进程已退出，自动恢复下载器")
                ensure_downloader_process()
            _ensure_whitelist_process()
        except Exception:
            app.logger.exception("后台服务看门狗检查失败")

def stop_background_services():
    _service_stop.set()
    with _service_lock:
        processes = list(_service_processes)
        _service_processes.clear()
    for name, proc in processes:
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
    threading.Thread(target=_service_watchdog, name="service-watchdog", daemon=True).start()
    try:
        app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=os.getenv("FLASK_DEBUG", "0") == "1", use_reloader=False)
    finally:
        stop_background_services()
