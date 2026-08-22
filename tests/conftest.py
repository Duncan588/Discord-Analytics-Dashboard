"""共享测试环境。

测试不依赖真实的 Discord API、真实 .env 或仓库里的 data/ 数据：
所有数据库都建在 pytest 的临时目录里。
"""
import json
import os
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

ADMIN_ID = "1000000000000000001"

TEST_ENV = {
    "ADMIN_IDS": ADMIN_ID,
    "DISCORD_CLIENT_ID": "123456789012345678",
    "DISCORD_CLIENT_SECRET": "test-client-secret",
    "DISCORD_BOT_TOKEN": "",
    "DISCORD_DOWNLOADER_TOKEN": "",
    "DISCORD_DOWNLOADER": "",
    "FLASK_SECRET_KEY": "test-secret-key",
    "PUBLIC_BASE_URL": "",
    "PORT": "5000",
    "MAX_UPLOAD_BYTES": "1048576",
    "FLASK_DEBUG": "0",
    "LOG_LEVEL": "CRITICAL",
}


class FakeResponse:
    """requests.Response 的最小替身。"""

    def __init__(self, status_code=200, payload=None, headers=None, text=None):
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}
        self.text = text if text is not None else json.dumps(payload)

    def json(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        if self._payload is None:
            raise ValueError("no json body")
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            import requests

            raise requests.HTTPError(f"HTTP {self.status_code}", response=self)


@pytest.fixture(scope="session")
def app_module(tmp_path_factory):
    """导入 app.py。

    app.py 在导入时会校验 .env 与 .env.example 的键完全一致，并初始化门户数据库。
    这里先把关键配置写进 os.environ（load_local_env 不会覆盖已有变量），
    需要时用 .env.example 生成一个临时 .env，导入完成后立刻删除。
    """
    base = tmp_path_factory.mktemp("app_module")
    os.environ.update(TEST_ENV)
    os.environ["PORTAL_DB"] = str(base / "portal.db")

    env_path = REPO_ROOT / ".env"
    created_env = not env_path.exists()
    if created_env:
        env_path.write_text(
            (REPO_ROOT / ".env.example").read_text(encoding="utf-8"), encoding="utf-8"
        )
    try:
        import app
    finally:
        if created_env:
            env_path.unlink()

    app.DATA_DIR = str(base / "servers")
    app.CACHE_DIR = str(base / "cache")
    os.makedirs(app.DATA_DIR, exist_ok=True)
    os.makedirs(app.CACHE_DIR, exist_ok=True)
    app.app.config.update(TESTING=True)
    return app


@pytest.fixture
def flask_app(app_module, tmp_path, monkeypatch):
    """每个测试一套全新的门户数据库、服务器数据目录和首页缓存目录。"""
    monkeypatch.setattr(app_module, "PORTAL_DB", str(tmp_path / "portal.db"))
    monkeypatch.setattr(app_module, "DATA_DIR", str(tmp_path / "servers"))
    monkeypatch.setattr(app_module, "CACHE_DIR", str(tmp_path / "cache"))
    monkeypatch.setattr(app_module, "ADMIN_IDS", {ADMIN_ID})
    monkeypatch.setattr(app_module, "data_engine", app_module.DataEngine())
    os.makedirs(app_module.DATA_DIR, exist_ok=True)
    os.makedirs(app_module.CACHE_DIR, exist_ok=True)
    app_module.init_portal_db()
    return app_module


@pytest.fixture
def client(flask_app):
    with flask_app.app.test_client() as test_client:
        yield test_client


def login(test_client, user_id, username="tester"):
    with test_client.session_transaction() as session:
        session["user"] = {
            "id": str(user_id),
            "username": username,
            "avatar": "https://cdn.discordapp.com/embed/avatars/0.png",
        }


@pytest.fixture
def admin_id():
    return ADMIN_ID


def portal_conn(flask_app):
    """直接打开门户数据库。

    测试客户端用作上下文管理器时会保留最后一次请求的上下文，
    其中缓存的 flask.g 连接已经关闭，所以断言不要复用 get_portal_db()。
    """
    return flask_app.db_connect(flask_app.PORTAL_DB)


def build_server(flask_app, server_id, name="Test Server", owner_user_id=None, users=(), messages=()):
    """创建一个服务器分析库并注册到门户数据库。"""
    path = flask_app.server_db_path(server_id)
    flask_app.init_server_db(path)
    conn = flask_app.db_connect(path)
    for user in users:
        conn.execute(
            "INSERT OR REPLACE INTO users(user_id,username,nickname,avatar_url,is_bot) VALUES(?,?,?,?,0)",
            (str(user["user_id"]), user.get("username", ""), user.get("nickname", ""), user.get("avatar_url", "")),
        )
    for message in messages:
        conn.execute(
            "INSERT OR REPLACE INTO threads(thread_id,category_id,name,exported_at,guild_id) VALUES(?,?,?,?,?)",
            (str(message["thread_id"]), "cat", message.get("thread_name", "thread"), None, str(server_id)),
        )
        conn.execute(
            "INSERT OR REPLACE INTO messages(message_id,thread_id,author_id,content,timestamp,reply_to_msg_id)"
            " VALUES(?,?,?,?,?,NULL)",
            (
                str(message["message_id"]),
                str(message["thread_id"]),
                str(message["author_id"]),
                message.get("content", ""),
                message.get("timestamp", "2026-01-01T00:00:00+00:00"),
            ),
        )
    conn.commit()
    conn.close()

    now = "2026-01-01T00:00:00+00:00"
    portal = flask_app.db_connect(flask_app.PORTAL_DB)
    portal.execute(
        "INSERT OR REPLACE INTO servers(server_id,name,icon_url,owner_user_id,db_path,created_at,updated_at)"
        " VALUES(?,?,?,?,?,?,?)",
        (str(server_id), name, None, owner_user_id, path, now, now),
    )
    portal.commit()
    portal.close()
    return path
