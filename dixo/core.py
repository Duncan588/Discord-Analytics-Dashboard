"""Application core: config, Flask app object, hooks, DB helpers, engines, auth decorators,
Discord helpers and background-service management (moved verbatim from app.py).

Routes live in dixo/routes/*.py (Blueprints); app.py is the thin entry point.
"""
import os

import re

import json

import math

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

from shared.env import apply_local_env, env_keys, load_local_env

from shared.portal import touch_user_presence, upsert_portal_user

from shared.sqlite_utils import add_columns, connect_sqlite, is_missing_table_error

from shared.task_timing import calculate_task_timing

from shared.timeutil import parse_utc_datetime, to_local_datetime, utc_now_iso

from shared.cloud_db import cloud_enabled, remote_exists, remote_file_stat

from dixo import textutil as dixo_textutil

from dixo import visitors as dixo_visitors

from dixo import assets as dixo_assets

from dixo import i18n as dixo_i18n

from types import SimpleNamespace

from dixo.api import create_ai_api, create_dashboard_api

from dixo.engines import ProfileEngine as DixoProfileEngine, SummaryEngine

# dixo/core.py 位于仓库下一层；BASE_DIR 仍指仓库根（.env / data / templates 所在）。
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
APP_ENTRY = os.path.join(BASE_DIR, "app.py")

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

app = Flask("app", root_path=BASE_DIR)

app.secret_key = SECRET_KEY

app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_port=1)

app.permanent_session_lifetime = timedelta(days=30)

dixo_i18n.init_app(app)

dixo_assets.init_app(app)

app.config["MAX_CONTENT_LENGTH"] = int(os.getenv("MAX_UPLOAD_BYTES", str(2 * 1024 * 1024 * 1024)))

# 版本化静态资源（?v=xxx）内容不变，可交给浏览器长期缓存
VERSIONED_STATIC_MAX_AGE = 365 * 24 * 60 * 60

# Discord Activity 以跨站 iframe 加载，SameSite=Lax 的 Cookie 不会被携带，
# 所以 https 部署时必须用 SameSite=None; Secure，本地 http 调试保持 Lax。
_IS_HTTPS_DEPLOY = PUBLIC_BASE_URL.startswith("https://")

app.config["SESSION_COOKIE_SAMESITE"] = "None" if _IS_HTTPS_DEPLOY else "Lax"

app.config["SESSION_COOKIE_SECURE"] = _IS_HTTPS_DEPLOY

# Activity 是跨站 iframe：Partitioned Cookie 兼容浏览器对第三方 Cookie 的限制。
# Flask 3.1+ 会自动附加 `Partitioned` 属性并要求 `Secure`，本地 HTTP 调试关闭。
app.config["SESSION_COOKIE_PARTITIONED"] = _IS_HTTPS_DEPLOY

# 云端数据模式下数据留在生产主机上（见 shared/cloud_db.py），本地不建 data/servers。
if not cloud_enabled():
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
    return redirect(url_for("pages.welcome"))

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
CSRF_EXEMPT_ENDPOINTS = {"activity.activity_token", "activity.activity_log"}

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

def db_file_signature(path):
    """数据库文件的 (mtime_ns, size)。

    云端数据模式下本地没有这些文件，必须问生产主机，否则缓存签名恒为 0，
    每次请求都会重算（实测 /user/<uid> 因此从缓存命中退化到 20 秒）。
    """
    if cloud_enabled():
        return remote_file_stat(path) or (0, 0)
    try:
        info = os.stat(path)
        return info.st_mtime_ns, info.st_size
    except OSError:
        return 0, 0

def db_mtime_seconds(path):
    return db_file_signature(path)[0] / 1e9

def get_portal_db():
    if not hasattr(g, "portal_db"):
        g.portal_db = db_connect(PORTAL_DB)
    return g.portal_db

def current_server_id():
    return str(session.get("server_id")) if session.get("server_id") else None

def server_db_path(server_id):
    return os.path.join(DATA_DIR, str(server_id), "discord_data.db")

def resolve_server_db_path(server_id, stored_path=None):
    """服务器库应当在本地用哪个路径打开。

    portal.db 里的 ``db_path`` 在生产上是绝对路径（例如
    ``/root/v2/V20/data/servers/<gid>/discord_data.db``），跨机器不可用，
    所以云端数据模式一律回到规范布局；本地模式保持原有行为。
    """
    if cloud_enabled() or not stored_path:
        return server_db_path(server_id)
    return stored_path

def server_db_exists(server_id, stored_path=None):
    """服务器库是否存在：云端数据模式下问生产主机，否则看本地文件。"""
    if cloud_enabled():
        return bool(remote_exists(server_db_path(server_id)))
    return os.path.exists(resolve_server_db_path(server_id, stored_path))

def get_db():
    sid = current_server_id()
    if not sid:
        raise NoServerSelectedError("未选择服务器")
    if not hasattr(g, "analytics_db"):
        path = server_db_path(sid)
        if not server_db_exists(sid):
            raise ServerDataNotFoundError("服务器数据不存在")
        g.analytics_db = db_connect(path)
    return g.analytics_db

@app.teardown_appcontext
def close_connections(exception=None):
    for name in ("portal_db", "analytics_db", "visitors_db"):
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
    CREATE TABLE IF NOT EXISTS block_interception_counts (
        guild_id TEXT NOT NULL,
        sender_id TEXT NOT NULL,
        target_id TEXT NOT NULL,
        count INTEGER NOT NULL DEFAULT 0,
        last_blocked_at TEXT NOT NULL,
        PRIMARY KEY (guild_id, sender_id, target_id)
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
        "active_started_at": "DATETIME",
        "recent_speed": "REAL DEFAULT 0"
    }
    add_columns(conn, "servers", {"member_count": "INTEGER"})
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
    CREATE INDEX IF NOT EXISTS idx_msg_author_message ON messages(author_id,message_id);
    CREATE INDEX IF NOT EXISTS idx_msg_author_thread ON messages(author_id,thread_id);
    CREATE INDEX IF NOT EXISTS idx_msg_timestamp ON messages(timestamp);
    CREATE INDEX IF NOT EXISTS idx_msg_thread_timestamp ON messages(thread_id,timestamp);
    CREATE INDEX IF NOT EXISTS idx_react_user_message ON reactions(user_id,message_id);
    CREATE INDEX IF NOT EXISTS idx_react_msg_user ON reactions(message_id,user_id);
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
    return bool(re.fullmatch(r"[\u4e00-\u9fff]+", word or ""))

# 词云分词/过滤统一走 dixo.textutil（jieba + 中英停用词 + URL/提及/表情/代码/数字剥离）。
# 下面两个名字保留给 /report 等旧调用点。
def get_word_cloud_counter(text_list):
    return dixo_textutil.count_words(text_list)

def format_word_cloud(counter, limit=None):
    return dixo_textutil.format_word_cloud(counter, limit)

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

class _MergeIndex:
    """user_merges 映射（小表），按服务器内存缓存 30 秒。"""

    def __init__(self):
        self._cache = {}

    def get_merged_ids(self, sid, uid):
        sid = str(sid)
        hit = self._cache.get(sid)
        if not hit or time.monotonic() - hit[0] > 30:
            conn = db_connect(server_db_path(sid))
            try:
                merges = {str(r["target_id"]): str(r["parent_id"]) for r in conn.execute("SELECT target_id,parent_id FROM user_merges")}
            finally:
                conn.close()
            hit = (time.monotonic(), merges)
            self._cache[sid] = hit
        children = sorted(k for k, v in hit[1].items() if v == str(uid))
        return [str(uid)] + children

    def invalidate(self, sid=None):
        if sid is None:
            self._cache.clear()
        else:
            self._cache.pop(str(sid), None)

# data_engine 名字保留：/report 等旧代码通过它取合并账号。
data_engine = _MergeIndex()

def _after_summary_refresh(sid):
    # AI API 启用时顺带推进全文索引（search.db，增量）。
    if os.getenv("AI_API_TOKENS", "").strip():
        from dixo import search as dixo_search
        res = dixo_search.refresh(server_db_path(sid), budget_seconds=20)
        app.logger.info("search.refresh sid=%s %s", sid, res)

summary_engine = SummaryEngine(server_db_path, db_connect, cloud_enabled, app.logger, after_refresh=_after_summary_refresh)

profile_engine = DixoProfileEngine(summary_engine, app.logger)

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
    if not server_db_exists(sid):
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
        path = resolve_server_db_path(sid, server["db_path"])
        if not server_db_exists(sid, server["db_path"]):
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
    # Demo 服务器兜底：无真实数据的普通用户被引入演示服务器并绑定假身份
    # 用户在任何真实服务器中都没有数据时，将其引入演示服务器以便体验；
    # 演示服务器绝不授予 admin/whitelist 权限，且一旦用户在真实服务器出现数据，
    # 就从可见列表中移除演示服务器。
    # 无数据的用户会被分配一个演示库中的假身份（demo_identities 表），
    # 这样看板/个人主页/年度报告都能以该假用户的视角完整体验。
    demo_id = str(os.getenv("DEMO_SERVER_ID", "900000000000000001"))
    has_real_data = any(str(s["server_id"]) != demo_id for s in result)
    if not has_real_data and level == 0:
        demo = portal.execute("SELECT * FROM servers WHERE server_id=?", (demo_id,)).fetchone()
        if demo is not None and server_db_exists(demo_id, demo["db_path"]):
            portal.execute(
                "INSERT OR IGNORE INTO user_server_access(user_id,server_id,granted_by,created_at) VALUES(?,?,?,?)",
                (uid, demo_id, "demo-fallback", utc_now_iso()),
            )
            _assign_demo_identity(portal, uid, demo_id)
    elif level >= 1:
        # admin 切换到 demo 浏览时也分配假身份（不写 user_server_access，
        # 不影响其 admin 权限与真实服务器可见性）。
        demo = portal.execute("SELECT * FROM servers WHERE server_id=?", (demo_id,)).fetchone()
        if demo is not None and server_db_exists(demo_id, demo["db_path"]):
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

def login_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if "user" not in session:
            return redirect(url_for("pages.welcome"))
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
            return redirect(url_for("pages.welcome"))
        servers = get_servers_for_user(session["user"]["id"])
        allowed = {str(x["server_id"]) for x in servers}
        sid = current_server_id()
        if not sid or sid not in allowed:
            session.pop("server_id", None)
            if len(servers) == 1:
                session["server_id"] = servers[0]["server_id"]
            elif len(servers) > 1:
                return redirect(url_for("pages.servers"))
            else:
                return redirect(url_for("pages.welcome"))
        return f(*args, **kwargs)
    return wrapper

def admin_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if "user" not in session:
            return redirect(url_for("pages.welcome"))
        if admin_level(session["user"]["id"]) == 0:
            # 管理页不是公开资源；普通登录用户回首页而不是暴露 403 页面。
            return redirect(url_for("pages.index"))
        return f(*args, **kwargs)
    return wrapper

def level1_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if "user" not in session:
            return redirect(url_for("pages.welcome"))
        if admin_level(session["user"]["id"]) != 1:
            return redirect(url_for("admin.admin_panel"))
        return f(*args, **kwargs)
    return wrapper

def whitelist_allowed(user_id):
    return admin_level(user_id) in (1, 2)

def register_server(server_id, name, icon_url=None, owner_user_id=None, db_path=None):
    server_id = str(server_id)
    db_path = db_path or server_db_path(server_id)
    if not cloud_enabled():
        init_server_db(db_path)
    now = utc_now_iso()
    conn = get_portal_db()
    conn.execute("INSERT INTO servers(server_id,name,icon_url,owner_user_id,db_path,created_at,updated_at) VALUES(?,?,?,?,?,?,?) ON CONFLICT(server_id) DO UPDATE SET name=excluded.name,icon_url=excluded.icon_url,db_path=excluded.db_path,updated_at=excluded.updated_at", (server_id, name or f"Discord Server {server_id}", icon_url, owner_user_id, db_path, now, now))
    conn.commit()

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
    return url_for("auth.callback", _external=True)

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
        # Session 是客户端签名字符串 Cookie：完整 guild 对象（含 features/description
        # 等长数组字段）会把 302 响应头撑到几十 KB，超过 nginx 默认 header 缓冲，
        # 直接 502（upstream sent too big header）。只保留下游实际用到的字段。
        session["discord_guilds"] = [
            {
                "id": g.get("id"),
                "name": g.get("name"),
                "icon": g.get("icon"),
                "owner": g.get("owner"),
                "permissions": g.get("permissions"),
            }
            for g in guilds
        ]
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

@app.after_request
def _no_cache_html(response):
    # 动态页面禁止启发式缓存，防止切换服务器后浏览器/CDN 仍展示旧服务器内容。
    ct = response.headers.get("Content-Type", "")
    if ct.startswith("text/html"):
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        response.headers["Pragma"] = "no-cache"
        response.headers["Vary"] = "Cookie"
    return response

def get_visitors_db():
    """访客库（data/servers/<gid>/visitors.db），与 5GB 主库分离，避免每次访问改主库 mtime（K28）。"""
    sid = current_server_id()
    if not hasattr(g, "visitors_db"):
        g.visitors_db = dixo_visitors.connect(server_db_path(sid), readonly=cloud_enabled())
    return g.visitors_db

def _safe_visitor_write(fn, *args):
    try:
        fn(get_visitors_db(), *args)
    except sqlite3.Error as exc:  # 只读（云端模式）或锁冲突时不影响页面
        app.logger.warning("访客记录写入失败 error=%s", exc)

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

# ---- 年度报告缓存（stale-while-revalidate）----
# 单次 query 30+ 个 460 万行聚合，磁盘满时 5-10s。缓存 5 分钟供快速二次访问。
_REPORT_CACHE = {}

_REPORT_TTL = 300  # 秒

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
    """Start discord_downloader if not running. With update_enabled configs it stays alive polling."""

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
        p.execute("UPDATE download_tasks SET status='pending',phase='queued',message='等待下载器继续执行',finished_at=NULL,active_started_at=NULL,delete_requested=0 WHERE id=?",(task_id,)); p.commit(); ensure_downloader_process(); return redirect(url_for("admin.admin_panel"))
    elif action=="cancel" and task["status"] in ("pending","running","paused"):
        timing = calculate_task_timing(task)
        p.execute("UPDATE download_tasks SET status='cancelled',phase='cancelled',message='任务已取消',finished_at=?,delete_requested=0,active_started_at=NULL,elapsed_seconds=?,estimated_seconds=?,speed=? WHERE id=?",(nowv,timing["elapsed_seconds"],timing["estimated_seconds"],timing["speed"],task_id))
    p.commit(); return redirect(url_for("admin.admin_panel"))

def _delete_task_data_from_server(p, task_id, guild_id):
    db_path=server_db_path(guild_id)
    if not server_db_exists(guild_id): return
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

@app.context_processor
def inject_globals():
    sid = current_server_id()
    server = None
    if sid and (cloud_enabled() or os.path.exists(PORTAL_DB)):
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

# ---- JSON API（dixo/api.py）----
def _list_servers_for_ai():
    conn = db_connect(PORTAL_DB)
    try:
        rows = conn.execute("SELECT server_id,name,icon_url FROM servers ORDER BY name").fetchall()
    finally:
        conn.close()
    return [dict(r) for r in rows if server_db_exists(r["server_id"])]

_api_ctx = SimpleNamespace(
    server_required=server_required, get_db=get_db, current_server_id=current_server_id,
    summary_engine=summary_engine, profile_engine=profile_engine, merged_ids=data_engine.get_merged_ids,
    server_db_path=server_db_path, server_db_exists=server_db_exists, connect=db_connect,
    list_servers=_list_servers_for_ai,
    export_root=lambda gid: os.path.join(BASE_DIR, "data", "exports", str(gid), "members"),
)

app.register_blueprint(create_dashboard_api(_api_ctx))

app.register_blueprint(create_ai_api(_api_ctx))

if cloud_enabled():
    app.logger.warning(
        "云端数据模式已启用：数据库读取自 %s，跳过本地建表/迁移；"
        "写操作是否放行取决于 CLOUD_DB_READONLY 与生产端 CLOUD_DB_AGENT_ALLOW_WRITE。",
        os.getenv("CLOUD_DB_SSH_TARGET", "?"),
    )
else:
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
        conn=db_connect(PORTAL_DB)
        row=conn.execute("SELECT 1 FROM download_tasks WHERE status IN ('pending','running') AND delete_requested=0 LIMIT 1").fetchone()
        poll=conn.execute("SELECT 1 FROM download_configs WHERE enabled=1 AND update_enabled=1 LIMIT 1").fetchone()
        conn.close()
        if row or poll:
            ensure_downloader_process()
        else:
            app.logger.info("服务未启动 name=discord_downloader：没有下载任务")
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

def _has_update_poll_configs():
    conn = db_connect(PORTAL_DB)
    try:
        return bool(conn.execute(
            "SELECT 1 FROM download_configs WHERE enabled=1 AND update_enabled=1 LIMIT 1"
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
            if (_has_active_download_tasks() or _has_update_poll_configs()) and not _downloader_process_alive():
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
    # 用户可能刚改过 .env。os.execv 原地替换进程不会触发 systemd 重新读取
    # EnvironmentFile，而启动时的 load_local_env 只填充缺失 key，所以这里
    # 必须用 override=True 把 .env 强制刷进进程环境，否则新配置不生效。
    try:
        apply_local_env(os.path.join(BASE_DIR, ".env"), override=True)
        app.logger.warning("重启前已用 override 模式重新加载 .env")
    except Exception:
        app.logger.exception("重启前重新加载 .env 失败，沿用旧环境继续重启")
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
    os.execv(sys.executable, [sys.executable, APP_ENTRY])
