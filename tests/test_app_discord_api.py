"""app.py 与 Discord API 交互的部分：不发真实请求，只替换 requests。"""
import pytest
import requests

from tests.conftest import FakeResponse


def test_oauth_config_error_messages(app_module, monkeypatch):
    monkeypatch.setattr(app_module, "DISCORD_CLIENT_ID", "")
    assert "Client ID" in app_module.oauth_config_error()
    monkeypatch.setattr(app_module, "DISCORD_CLIENT_ID", "not-digits")
    assert "Client ID" in app_module.oauth_config_error()
    monkeypatch.setattr(app_module, "DISCORD_CLIENT_ID", "123")
    monkeypatch.setattr(app_module, "DISCORD_CLIENT_SECRET", "")
    assert "Client Secret" in app_module.oauth_config_error()
    monkeypatch.setattr(app_module, "DISCORD_CLIENT_SECRET", "secret")
    assert app_module.oauth_config_error() is None


def test_oauth_redirect_uri_prefers_public_base_url(app_module, monkeypatch):
    monkeypatch.setattr(app_module, "PUBLIC_BASE_URL", "https://example.test")
    with app_module.app.test_request_context("/"):
        assert app_module.oauth_redirect_uri() == "https://example.test/callback"
    monkeypatch.setattr(app_module, "PUBLIC_BASE_URL", "")
    with app_module.app.test_request_context("/"):
        assert app_module.oauth_redirect_uri().endswith("/callback")


def test_managed_guilds_from_session_filters_and_builds_icons(app_module):
    with app_module.app.test_request_context("/"):
        from flask import session

        session["discord_guilds"] = [
            {"id": "1", "permissions": "8", "icon": "hash"},
            {"id": "2", "permissions": "32", "icon": "a_hash"},
            {"id": "3", "owner": True, "icon": None},
            {"id": "4", "permissions": "0"},
            {"id": "5", "permissions": None},
        ]
        guilds = app_module.managed_guilds_from_session()

    assert [g["id"] for g in guilds] == ["1", "2", "3"]
    assert guilds[0]["icon_url"].endswith("hash.png?size=64")
    assert guilds[1]["icon_url"].endswith("a_hash.gif?size=64")
    assert guilds[2]["icon_url"] == "https://cdn.discordapp.com/embed/avatars/0.png"


def test_bot_identity_prefers_global_name(app_module, monkeypatch):
    monkeypatch.setattr(
        app_module.requests, "get", lambda *a, **k: FakeResponse(payload={"global_name": "G", "username": "u"})
    )
    assert app_module.bot_identity("tok") == "G"
    monkeypatch.setattr(app_module.requests, "get", lambda *a, **k: FakeResponse(payload={"username": "u"}))
    assert app_module.bot_identity("tok") == "u"
    monkeypatch.setattr(app_module.requests, "get", lambda *a, **k: FakeResponse(payload={}))
    assert app_module.bot_identity("tok") == "Discord Bot"


def test_bot_identity_raises_for_invalid_token(app_module, monkeypatch):
    monkeypatch.setattr(app_module.requests, "get", lambda *a, **k: FakeResponse(status_code=401, payload={}))
    with pytest.raises(requests.HTTPError):
        app_module.bot_identity("bad")


def test_fetch_discord_user(app_module, monkeypatch):
    monkeypatch.setenv("DISCORD_BOT_TOKEN", "tok")
    monkeypatch.setattr(
        app_module.requests,
        "get",
        lambda *a, **k: FakeResponse(payload={"username": "u", "global_name": "G", "avatar": "hash"}),
    )
    assert app_module.fetch_discord_user("42") == {
        "username": "G",
        "avatar_url": "https://cdn.discordapp.com/avatars/42/hash.png?size=64",
    }

    monkeypatch.setattr(app_module.requests, "get", lambda *a, **k: FakeResponse(payload={"username": "u"}))
    assert app_module.fetch_discord_user("42") == {"username": "u", "avatar_url": None}

    monkeypatch.setattr(app_module.requests, "get", lambda *a, **k: FakeResponse(status_code=404, payload={}))
    assert app_module.fetch_discord_user("42") is None

    monkeypatch.setattr(app_module.requests, "get", lambda *a, **k: FakeResponse(payload={}))
    assert app_module.fetch_discord_user("42") is None


def test_fetch_discord_user_without_token(app_module, monkeypatch):
    for key in ("DISCORD_BOT_TOKEN", "DISCORD_DOWNLOADER_TOKEN", "DISCORD_DOWNLOADER"):
        monkeypatch.setenv(key, "")
    assert app_module.fetch_discord_user("42") is None


class _ApiStub:
    """按 URL 后缀返回预设响应，模拟 Discord REST API。"""

    def __init__(self, responses):
        self.responses = responses

    def __call__(self, url, headers=None, timeout=None):
        for suffix, response in self.responses.items():
            if url.endswith(suffix):
                return response
        raise AssertionError(f"unexpected url {url}")


def _forum_channel(**overrides):
    channel = {"id": "20", "type": 15, "permission_overwrites": []}
    channel.update(overrides)
    return channel


def _access_stub(**overrides):
    responses = {
        "/users/@me": FakeResponse(payload={"id": "5"}),
        "/guilds/10/members/5": FakeResponse(payload={"roles": []}),
        "/guilds/10/channels": FakeResponse(payload=[_forum_channel()]),
        "/guilds/10/roles": FakeResponse(payload=[{"id": "10", "permissions": 0}]),
    }
    responses.update(overrides)
    return _ApiStub(responses)


def test_check_bot_forum_access_administrator(app_module, monkeypatch):
    stub = _access_stub(**{"/guilds/10/roles": FakeResponse(payload=[{"id": "10", "permissions": 0x8}])})
    monkeypatch.setattr(app_module.requests, "get", stub)
    assert app_module.check_bot_forum_access("tok", "10", "20") == (True, "可访问（Administrator）")


def test_check_bot_forum_access_with_required_permissions(app_module, monkeypatch):
    stub = _access_stub(
        **{"/guilds/10/roles": FakeResponse(payload=[{"id": "10", "permissions": 0x400 | 0x10000}])}
    )
    monkeypatch.setattr(app_module.requests, "get", stub)
    assert app_module.check_bot_forum_access("tok", "10", "20") == (True, "可访问")


def test_check_bot_forum_access_lists_missing_permissions(app_module, monkeypatch):
    monkeypatch.setattr(app_module.requests, "get", _access_stub())
    ok, reason = app_module.check_bot_forum_access("tok", "10", "20")
    assert ok is False
    assert reason == "缺少权限：查看频道、读取历史消息"


def test_check_bot_forum_access_applies_overwrites(app_module, monkeypatch):
    channel = _forum_channel(
        permission_overwrites=[
            {"id": "10", "type": 0, "allow": 0x400, "deny": 0x10000},
            {"id": "11", "type": 0, "allow": 0x10000, "deny": 0},
            {"id": "5", "type": "1", "allow": 0, "deny": 0},
        ]
    )
    stub = _access_stub(
        **{
            "/guilds/10/members/5": FakeResponse(payload={"roles": ["11"]}),
            "/guilds/10/channels": FakeResponse(payload=[channel]),
        }
    )
    monkeypatch.setattr(app_module.requests, "get", stub)
    assert app_module.check_bot_forum_access("tok", "10", "20") == (True, "可访问")


def test_check_bot_forum_access_member_overwrite_can_deny(app_module, monkeypatch):
    channel = _forum_channel(
        permission_overwrites=[{"id": "5", "type": "1", "allow": 0, "deny": 0x400 | 0x10000}]
    )
    stub = _access_stub(
        **{
            "/guilds/10/roles": FakeResponse(payload=[{"id": "10", "permissions": 0x400 | 0x10000}]),
            "/guilds/10/channels": FakeResponse(payload=[channel]),
        }
    )
    monkeypatch.setattr(app_module.requests, "get", stub)
    ok, reason = app_module.check_bot_forum_access("tok", "10", "20")
    assert (ok, reason) == (False, "缺少权限：查看频道、读取历史消息")


@pytest.mark.parametrize(
    "overrides,expected",
    [
        ({"/users/@me": FakeResponse(status_code=401, payload={})}, "Token 无效或机器人未获准使用此 Token"),
        ({"/guilds/10/members/5": FakeResponse(status_code=404, payload={})}, "机器人不在此服务器"),
        ({"/guilds/10/members/5": FakeResponse(status_code=403, payload={})}, "无法读取机器人在服务器中的成员信息"),
        ({"/guilds/10/channels": FakeResponse(status_code=403, payload={})}, "机器人无法读取服务器频道列表"),
        ({"/guilds/10/channels": FakeResponse(status_code=404, payload={})}, "机器人不在此服务器"),
        ({"/guilds/10/channels": FakeResponse(payload=[])}, "找不到目标 Forum，或机器人无法访问该频道"),
        ({"/guilds/10/channels": FakeResponse(payload=[_forum_channel(type=0)])}, "目标频道不是 Forum 频道"),
    ],
)
def test_check_bot_forum_access_error_paths(app_module, monkeypatch, overrides, expected):
    monkeypatch.setattr(app_module.requests, "get", _access_stub(**overrides))
    assert app_module.check_bot_forum_access("tok", "10", "20") == (False, expected)


def test_check_bot_forum_access_network_failure(app_module, monkeypatch):
    def boom(*args, **kwargs):
        raise requests.ConnectionError("offline")

    monkeypatch.setattr(app_module.requests, "get", boom)
    ok, reason = app_module.check_bot_forum_access("tok", "10", "20")
    assert ok is False
    assert reason.startswith("Discord API 检查失败")


def test_selected_download_bots(flask_app, tmp_path, monkeypatch):
    conn = flask_app.db_connect(flask_app.PORTAL_DB)
    conn.execute("INSERT INTO download_bots(id,owner_user_id,name,token,created_at) VALUES(1,'u','A','tok-a','t')")
    conn.execute("INSERT INTO download_bots(id,owner_user_id,name,token,created_at) VALUES(2,'u','B','tok-b','t')")
    conn.execute("INSERT INTO download_bots(id,owner_user_id,name,token,created_at) VALUES(3,'other','C','tok-c','t')")
    conn.commit()

    assert [b["name"] for b in flask_app.selected_download_bots(conn, "u", ["1", "2"], False)] == ["A", "B"]

    monkeypatch.setenv("DISCORD_DOWNLOADER_TOKEN", "tok-default")
    with_default = flask_app.selected_download_bots(conn, "u", ["1"], True)
    assert [b["id"] for b in with_default] == [1, "default"]

    # 默认机器人和自定义机器人共用同一个 token 时只保留一个。
    monkeypatch.setenv("DISCORD_DOWNLOADER_TOKEN", "tok-a")
    assert len(flask_app.selected_download_bots(conn, "u", ["1"], True)) == 1

    monkeypatch.setenv("DISCORD_DOWNLOADER_TOKEN", "")
    monkeypatch.setenv("DISCORD_DOWNLOADER", "")
    with pytest.raises(ValueError, match="DISCORD_DOWNLOADER_TOKEN"):
        flask_app.selected_download_bots(conn, "u", ["1"], True)

    with pytest.raises(PermissionError):
        flask_app.selected_download_bots(conn, "u", ["3"], False)

    with pytest.raises(ValueError, match="最多添加 5 个"):
        flask_app.selected_download_bots(conn, "u", ["1", "2", "1", "2", "1", "2"], False)
    conn.close()


@pytest.mark.parametrize(
    "payload,headers,expected",
    [
        ({"retry_after": 1.5, "global": True, "message": "slow"}, {}, (1.5, True, "slow")),
        ({"retry_after": "2"}, {}, (2.0, False, None)),
        ({}, {"Retry-After": "3"}, (3.0, False, None)),
        ({"retry_after": "abc"}, {}, (None, False, None)),
        ({"retry_after": -5}, {}, (0.0, False, None)),
        (None, {}, (None, False, None)),
        ("not-a-dict", {}, (None, False, None)),
    ],
)
def test_discord_rate_limit_info(app_module, payload, headers, expected):
    response = FakeResponse(status_code=429, payload=payload, headers=headers)
    with app_module.app.test_request_context("/"):
        assert app_module._discord_rate_limit_info(response) == expected


def test_discord_rate_limit_error_payload(app_module):
    response = FakeResponse(status_code=429, payload={"retry_after": 2.25, "global": False})
    with app_module.app.test_request_context("/"):
        payload = app_module._discord_rate_limit_error(response, "oauth_token")
    assert payload["rate_limited"] is True
    assert payload["retry_after"] == 2.25
    assert payload["stage"] == "oauth_token"
    assert "2.2 秒" in payload["error"]


def test_discord_rate_limit_error_without_retry_after(app_module):
    response = FakeResponse(status_code=429, payload={})
    with app_module.app.test_request_context("/"):
        payload = app_module._discord_rate_limit_error(response, "current_user")
    assert payload["retry_after"] is None
    assert "稍后" in payload["error"]


def _oauth_stub(flask_app, monkeypatch, token_response=None, user_response=None, guilds_response=None):
    monkeypatch.setattr(
        flask_app.requests, "post", lambda *a, **k: token_response or FakeResponse(payload={"access_token": "tok"})
    )

    def fake_get(url, headers=None, timeout=None):
        if url.endswith("/users/@me/guilds"):
            return guilds_response or FakeResponse(payload=[{"id": "1"}])
        return user_response or FakeResponse(payload={"id": "42", "username": "u", "avatar": "hash"})

    monkeypatch.setattr(flask_app.requests, "get", fake_get)


def test_exchange_discord_code_success(flask_app, monkeypatch):
    _oauth_stub(flask_app, monkeypatch)
    with flask_app.app.test_request_context("/"):
        from flask import session

        ok, payload = flask_app.exchange_discord_code("code", redirect_uri="https://x/callback", source="web")
        assert ok is True
        assert payload["user"]["id"] == "42"
        assert payload["access_token"] == "tok"
        assert session["discord_guilds"] == [{"id": "1"}]
        stored = flask_app.get_portal_db().execute("SELECT avatar_url FROM portal_users WHERE user_id='42'").fetchone()
        assert stored["avatar_url"].endswith("/avatars/42/hash.png")


def test_exchange_discord_code_without_guilds(flask_app, monkeypatch):
    _oauth_stub(flask_app, monkeypatch, user_response=FakeResponse(payload={"id": "42", "username": "u"}))
    with flask_app.app.test_request_context("/"):
        from flask import session

        ok, _ = flask_app.exchange_discord_code("code", fetch_guilds=False)
        assert ok is True
        assert session["discord_guilds"] == []
        assert session["user"]["avatar"].endswith("embed/avatars/0.png")


@pytest.mark.parametrize("stage", ["token", "user", "guilds"])
def test_exchange_discord_code_reports_rate_limits(flask_app, monkeypatch, stage):
    limited = FakeResponse(status_code=429, payload={"retry_after": 1})
    kwargs = {
        "token": {"token_response": limited},
        "user": {"user_response": limited},
        "guilds": {"guilds_response": limited},
    }[stage]
    _oauth_stub(flask_app, monkeypatch, **kwargs)
    with flask_app.app.test_request_context("/"):
        ok, payload = flask_app.exchange_discord_code("code")
    assert ok is False
    assert payload["rate_limited"] is True


def test_exchange_discord_code_without_access_token(flask_app, monkeypatch):
    _oauth_stub(flask_app, monkeypatch, token_response=FakeResponse(payload={"error": "invalid_grant"}))
    with flask_app.app.test_request_context("/"):
        ok, payload = flask_app.exchange_discord_code("code")
    assert ok is False
    assert "登录失败" in payload["error"]


def test_exchange_discord_code_reports_http_errors(flask_app, monkeypatch):
    _oauth_stub(flask_app, monkeypatch, token_response=FakeResponse(status_code=400, payload={}, text="bad code"))
    with flask_app.app.test_request_context("/"):
        ok, payload = flask_app.exchange_discord_code("code")
    assert ok is False
    assert "bad code" in payload["error"]
