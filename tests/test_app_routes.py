"""用 Flask 测试客户端覆盖鉴权装饰器、活动模式接口和管理后台路由。"""
import io
import json

import pytest

from tests.conftest import build_server, login, portal_conn


@pytest.fixture
def server(flask_app, admin_id):
    build_server(
        flask_app,
        "100",
        name="Main",
        users=[
            {"user_id": admin_id, "username": "admin", "nickname": "Admin"},
            {"user_id": "u2", "username": "member", "nickname": "Member"},
        ],
        messages=[
            {"message_id": "m1", "thread_id": "t1", "author_id": admin_id, "content": "数据分析 数据分析"},
            {"message_id": "m2", "thread_id": "t1", "author_id": "u2", "content": "你好"},
        ],
    )
    return "100"


def test_anonymous_visitor_is_sent_to_welcome(client):
    assert client.get("/").headers["Location"].endswith("/welcome")
    assert client.get("/me").headers["Location"].endswith("/welcome")
    assert client.get("/admin").headers["Location"].endswith("/welcome")
    assert client.get("/search").headers["Location"].endswith("/welcome")
    assert client.get("/admin/whitelist/add").status_code in (302, 405)


def test_activity_request_is_routed_to_login(client):
    response = client.get("/?frame_id=f&instance_id=i")
    assert "/login" in response.headers["Location"]
    assert client.get("/welcome?frame_id=f&instance_id=i").status_code == 302


def test_welcome_and_login_render_for_user_without_data(client, admin_id):
    login(client, admin_id)
    assert client.get("/welcome").status_code == 200
    assert client.get("/me").status_code == 200
    assert client.get("/login").headers["Location"].endswith("/welcome")


def test_login_page_for_anonymous_user(client):
    assert client.get("/login").status_code == 200


def test_single_server_is_auto_selected(client, server, admin_id):
    login(client, admin_id)
    assert client.get("/login").headers["Location"] in ("/", "http://localhost/")
    assert client.get("/").status_code == 200


def test_multiple_servers_redirect_to_picker(client, flask_app, server, admin_id):
    build_server(flask_app, "101", name="Second")
    login(client, admin_id)
    assert client.get("/").headers["Location"].endswith("/servers")
    assert client.get("/servers").status_code == 200
    assert client.get("/me").headers["Location"].endswith("/servers")
    assert client.get("/search").headers["Location"].endswith("/servers")


def test_select_server_rejects_servers_without_access(client, server):
    login(client, "outsider")
    assert client.get("/server/100").status_code == 403


def test_select_server_sets_session(client, server, admin_id):
    login(client, admin_id)
    assert client.get("/server/100").headers["Location"] in ("/", "http://localhost/")
    with client.session_transaction() as session:
        assert session["server_id"] == "100"


def test_index_records_visitor_and_renders(client, flask_app, server, admin_id):
    login(client, admin_id)
    client.get("/server/100")
    assert client.get("/").status_code == 200
    conn = flask_app.db_connect(flask_app.server_db_path("100"))
    assert conn.execute("SELECT count(*) FROM web_visitors").fetchone()[0] == 1
    conn.close()


def test_index_drops_stale_server_selection(client, server, admin_id):
    login(client, admin_id)
    with client.session_transaction() as session:
        session["server_id"] = "does-not-exist"
    assert client.get("/").status_code == 200
    with client.session_transaction() as session:
        assert session["server_id"] == "100"


def test_api_leaderboard_is_paginated(client, server, admin_id):
    login(client, admin_id)
    client.get("/server/100")
    rows = client.get("/api/leaderboard").get_json()
    assert [r["rank"] for r in rows] == [1, 2]
    assert client.get("/api/leaderboard?page=2").get_json() == []


def test_search_finds_users(client, server, admin_id):
    login(client, admin_id)
    client.get("/server/100")
    body = client.get("/search?q=member").get_data(as_text=True)
    assert "Member" in body


def test_user_profile_renders_and_counts_views(client, flask_app, server, admin_id):
    login(client, admin_id)
    client.get("/server/100")
    assert client.get("/user/u2").status_code == 200
    assert client.get("/user/u2?sort=new&page=1").status_code == 200
    conn = flask_app.db_connect(flask_app.server_db_path("100"))
    assert conn.execute("SELECT count(*) FROM profile_views WHERE target_user_id='u2'").fetchone()[0] == 1
    conn.close()


def test_user_profile_unknown_user_is_404(client, server, admin_id):
    login(client, admin_id)
    client.get("/server/100")
    assert client.get("/user/nobody").status_code == 404


def test_user_profile_follows_merge_target(client, flask_app, server, admin_id):
    conn = flask_app.db_connect(flask_app.server_db_path("100"))
    conn.execute("INSERT INTO user_merges(target_id,parent_id,created_at) VALUES('u2',?, 't')", (admin_id,))
    conn.commit()
    conn.close()
    login(client, admin_id)
    client.get("/server/100")
    assert f"/user/{admin_id}" in client.get("/user/u2").headers["Location"]


def test_claim_account_flow(client, flask_app, server, admin_id):
    login(client, admin_id)
    client.get("/server/100")
    assert client.post("/claim_account", data={"target_id": "u2"}).status_code == 302
    # 重复提交不会 500，只是提示已存在。
    assert client.post("/claim_account", data={"target_id": "u2"}).status_code == 302
    assert client.post("/claim_account", data={"target_id": admin_id}).status_code == 400
    assert client.post("/claim_account", data={"target_id": "ghost"}).status_code == 400
    conn = flask_app.db_connect(flask_app.server_db_path("100"))
    assert conn.execute("SELECT count(*) FROM claim_requests_v2").fetchone()[0] == 1
    conn.close()


def test_admin_panel_requires_admin(client, server):
    login(client, "u2")
    assert client.get("/admin").headers["Location"] in ("/", "http://localhost/")


def test_admin_panel_renders_for_level1(client, server, admin_id):
    login(client, admin_id)
    client.get("/server/100")
    with client.session_transaction() as session:
        session["discord_guilds"] = [{"id": "100", "name": "Main", "owner": True, "icon": "a_hash"}]
    assert client.get("/admin").status_code == 200


def test_admin_panel_renders_for_level2(client, flask_app, server):
    portal = portal_conn(flask_app)
    portal.execute("INSERT INTO whitelist_users(user_id,username,added_by,created_at) VALUES('u2','m','a','t')")
    portal.execute(
        "INSERT INTO user_server_access(user_id,server_id,granted_by,created_at) VALUES('u2','100','a','t')"
    )
    portal.commit()
    portal.close()
    login(client, "u2")
    client.get("/server/100")
    assert client.get("/admin").status_code == 200


def test_admin_claim_approval_and_unmerge(client, flask_app, server, admin_id):
    login(client, admin_id)
    client.get("/server/100")
    client.post("/claim_account", data={"target_id": "u2"})
    conn = flask_app.db_connect(flask_app.server_db_path("100"))
    req_id = conn.execute("SELECT id FROM claim_requests_v2").fetchone()["id"]
    conn.close()

    assert client.get(f"/admin/approve/{req_id}").status_code == 302
    conn = flask_app.db_connect(flask_app.server_db_path("100"))
    assert conn.execute("SELECT count(*) FROM user_merges WHERE target_id='u2'").fetchone()[0] == 1
    conn.close()

    client.get("/admin/unmerge/u2")
    conn = flask_app.db_connect(flask_app.server_db_path("100"))
    assert conn.execute("SELECT count(*) FROM user_merges").fetchone()[0] == 0
    conn.close()

    client.get("/admin/reset_all_claims")
    conn = flask_app.db_connect(flask_app.server_db_path("100"))
    assert conn.execute("SELECT count(*) FROM claim_requests_v2").fetchone()[0] == 0
    conn.close()


def test_level2_admin_scope_is_limited_to_its_server(client, flask_app, server):
    portal = portal_conn(flask_app)
    portal.execute("INSERT INTO whitelist_users(user_id,username,added_by,created_at) VALUES('u2','m','a','t')")
    portal.commit()
    portal.close()
    build_server(flask_app, "102", name="Other")
    login(client, "u2")
    with client.session_transaction() as session:
        session["server_id"] = "102"
    assert client.get("/admin/reset_all_claims").status_code == 403

    with client.session_transaction() as session:
        session.pop("server_id", None)
    assert client.get("/admin/reset_all_claims").status_code == 403


def test_whitelist_quota_and_access_management(client, flask_app, server, admin_id, monkeypatch):
    monkeypatch.setattr(flask_app, "fetch_discord_user", lambda uid: {"username": "resolved"})
    login(client, admin_id)

    assert client.post("/admin/whitelist/add", data={"user_id": "abc"}).status_code == 400
    client.post("/admin/whitelist/add", data={"user_id": "42"})
    client.post("/admin/quota", data={"user_id": "42", "quota": "500"})
    client.post("/admin/access", data={"user_id": "42", "server_id": "100"})
    client.post("/admin/access", data={"user_id": "42", "server_id": "missing"})

    portal = portal_conn(flask_app)
    assert portal.execute("SELECT username FROM whitelist_users WHERE user_id='42'").fetchone()[0] == "resolved"
    # quota 被夹在 1..100 之间。
    assert portal.execute("SELECT quota FROM server_download_quota WHERE user_id='42'").fetchone()[0] == 100
    assert portal.execute("SELECT count(*) FROM user_server_access WHERE user_id='42'").fetchone()[0] == 1
    portal.close()

    client.post("/admin/access/delete", data={"user_id": "42", "server_id": "100"})
    client.post("/admin/whitelist/delete/42")
    portal = portal_conn(flask_app)
    assert portal.execute("SELECT count(*) FROM whitelist_users").fetchone()[0] == 0
    assert portal.execute("SELECT count(*) FROM server_download_quota").fetchone()[0] == 0
    portal.close()


def test_whitelist_add_survives_discord_api_failure(client, flask_app, admin_id, monkeypatch):
    import requests

    def boom(uid):
        raise requests.ConnectionError("offline")

    monkeypatch.setattr(flask_app, "fetch_discord_user", boom)
    login(client, admin_id)
    client.post("/admin/whitelist/add", data={"user_id": "77"})
    portal = portal_conn(flask_app)
    # Discord 查询失败时用 ID 占位，不阻断白名单添加。
    assert portal.execute("SELECT username FROM whitelist_users WHERE user_id='77'").fetchone()[0] == "77"
    portal.close()


def test_download_bot_add_and_delete(client, flask_app, admin_id, monkeypatch):
    monkeypatch.setattr(flask_app, "bot_identity", lambda token: "Bot A")
    login(client, admin_id)

    assert client.post("/admin/bot/add", data={"token": ""}).status_code == 400
    assert client.post("/admin/bot/add", data={"token": "tok-1"}).status_code == 302
    portal = portal_conn(flask_app)
    bot_id = portal.execute("SELECT id FROM download_bots").fetchone()["id"]
    portal.close()

    assert client.post(f"/admin/bot/delete/{bot_id}").status_code == 302
    portal = portal_conn(flask_app)
    assert portal.execute("SELECT count(*) FROM download_bots").fetchone()[0] == 0
    portal.close()


def test_download_bot_add_limit(client, flask_app, admin_id, monkeypatch):
    monkeypatch.setattr(flask_app, "bot_identity", lambda token: "Bot")
    portal = portal_conn(flask_app)
    for i in range(5):
        portal.execute(
            "INSERT INTO download_bots(owner_user_id,name,token,created_at) VALUES(?,?,?,'t')",
            (admin_id, f"b{i}", f"tok{i}"),
        )
    portal.commit()
    portal.close()
    login(client, admin_id)
    assert client.post("/admin/bot/add", data={"token": "tok-x"}).status_code == 400


def test_download_bot_add_rejects_invalid_token(client, flask_app, admin_id, monkeypatch):
    import requests

    def boom(token):
        raise requests.ConnectionError("offline")

    monkeypatch.setattr(flask_app, "bot_identity", boom)
    login(client, admin_id)
    assert client.post("/admin/bot/add", data={"token": "bad"}).status_code == 400


def test_download_server_config_requires_numeric_ids(client, admin_id):
    login(client, admin_id)
    response = client.post("/admin/download-server", data={"guild_id": "abc", "forum_channel_id": "1"})
    assert response.status_code == 400


def test_download_server_config_created_when_bot_has_access(client, flask_app, admin_id, monkeypatch):
    monkeypatch.setattr(flask_app, "check_bot_forum_access", lambda token, guild, forum: (True, "可访问"))
    monkeypatch.setattr(flask_app.requests, "get", lambda *a, **k: _FakeForumResponse())
    portal = portal_conn(flask_app)
    portal.execute(
        "INSERT INTO download_bots(id,owner_user_id,name,token,created_at) VALUES(9,?,'B','tok','t')",
        (admin_id,),
    )
    portal.commit()
    portal.close()
    login(client, admin_id)

    response = client.post(
        "/admin/download-server",
        data={"guild_id": "555", "forum_channel_id": "666", "bot_ids": ["9"], "scheduler_interval": "10"},
    )
    assert response.status_code == 302
    portal = portal_conn(flask_app)
    config = portal.execute("SELECT * FROM download_configs").fetchone()
    portal.close()
    # scheduler_interval 有最小值保护。
    assert config["scheduler_interval"] == 50
    assert config["forum_channel_id"] == "666"

    assert client.post(f"/admin/download-server/delete/{config['id']}").status_code == 302
    portal = portal_conn(flask_app)
    assert portal.execute("SELECT count(*) FROM download_configs WHERE id=?", (config["id"],)).fetchone()[0] == 0
    portal.close()


def test_download_server_config_reports_bot_failures(client, flask_app, admin_id, monkeypatch):
    monkeypatch.setattr(flask_app, "check_bot_forum_access", lambda token, guild, forum: (False, "缺少权限"))
    portal = portal_conn(flask_app)
    portal.execute(
        "INSERT INTO download_bots(id,owner_user_id,name,token,created_at) VALUES(8,?,'B','tok','t')",
        (admin_id,),
    )
    portal.commit()
    portal.close()
    login(client, admin_id)
    response = client.post(
        "/admin/download-server",
        data={"guild_id": "555", "forum_channel_id": "666", "bot_ids": ["8"]},
    )
    assert response.status_code == 302
    portal = portal_conn(flask_app)
    assert portal.execute("SELECT count(*) FROM download_configs").fetchone()[0] == 0
    portal.close()


def test_download_server_config_requires_a_bot(client, admin_id):
    login(client, admin_id)
    response = client.post("/admin/download-server", data={"guild_id": "1", "forum_channel_id": "2"})
    assert response.status_code == 302


def test_downloader_config_endpoint(client, flask_app, admin_id):
    portal = portal_conn(flask_app)
    portal.execute(
        "INSERT INTO download_bots(id,owner_user_id,name,token,created_at) VALUES(5,?,'B','tok','t')", (admin_id,)
    )
    portal.execute(
        "INSERT INTO download_configs(server_id,owner_user_id,guild_id,forum_channel_id,enabled,updated_at)"
        " VALUES('1:2',?, '1','2',1,'t')",
        (admin_id,),
    )
    portal.commit()
    portal.close()
    login(client, admin_id)
    payload = client.get("/api/downloader-config").get_json()
    assert [row["guild_id"] for row in payload["servers"]] == ["1"]
    assert payload["bots"] == [{"id": 5, "name": "B"}]


def test_downloader_config_hides_other_owners_for_level2(client, flask_app):
    portal = portal_conn(flask_app)
    portal.execute("INSERT INTO whitelist_users(user_id,username,added_by,created_at) VALUES('u2','m','a','t')")
    portal.execute(
        "INSERT INTO download_configs(server_id,owner_user_id,guild_id,forum_channel_id,enabled,updated_at)"
        " VALUES('1:2','someone-else','1','2',1,'t')"
    )
    portal.commit()
    portal.close()
    login(client, "u2")
    payload = client.get("/api/downloader-config").get_json()
    assert payload["servers"] == []
    assert payload["default_token"] == ""


def test_managed_discord_resources_uses_session_guilds(client, flask_app, admin_id, monkeypatch):
    monkeypatch.setattr(flask_app.requests, "get", lambda *a, **k: _FakeForumResponse())
    portal = portal_conn(flask_app)
    portal.execute(
        "INSERT INTO download_bots(id,owner_user_id,name,token,created_at) VALUES(3,?,'B','tok','t')", (admin_id,)
    )
    portal.commit()
    portal.close()
    login(client, admin_id)
    with client.session_transaction() as session:
        session["discord_guilds"] = [
            {"id": "555", "name": "Managed", "permissions": "8", "icon": None},
            {"id": "556", "name": "NoPerm", "permissions": "0"},
        ]
    payload = client.get("/api/managed-discord-resources").get_json()
    assert [g["id"] for g in payload["guilds"]] == ["555"]
    assert payload["forums"]["555"] == [{"id": "1", "name": "forum-a"}]
    assert payload["warnings"] == []


def test_managed_discord_resources_reports_api_errors(client, flask_app, admin_id, monkeypatch):
    import requests

    def boom(*args, **kwargs):
        raise requests.ConnectionError("offline")

    monkeypatch.setattr(flask_app.requests, "get", boom)
    portal = portal_conn(flask_app)
    portal.execute(
        "INSERT INTO download_bots(id,owner_user_id,name,token,created_at) VALUES(4,?,'B','tok','t')", (admin_id,)
    )
    portal.commit()
    portal.close()
    login(client, admin_id)
    with client.session_transaction() as session:
        session["discord_guilds"] = [{"id": "555", "name": "Managed", "permissions": "8"}]
    payload = client.get("/api/managed-discord-resources").get_json()
    assert payload["forums"] == {}
    assert "offline" in payload["warnings"][0]


class _FakeForumResponse:
    status_code = 200
    headers = {}
    text = "[]"
    ok = True

    def json(self):
        return [
            {"id": "1", "name": "forum-a", "type": 15},
            {"id": "2", "name": "text", "type": 0},
        ]

    def raise_for_status(self):
        return None


def test_activity_status_reflects_session(client, admin_id):
    assert client.get("/api/activity/status").get_json() == {"ok": True, "authenticated": False}
    login(client, admin_id)
    assert client.get("/api/activity/status").get_json()["authenticated"] is True


def test_activity_log_sanitizes_input(client):
    assert client.post("/api/activity/log", json={"event": "sdk ready!", "details": "boom"}).get_json() == {"ok": True}
    assert client.post("/api/activity/log", json={"event": "sdk.ready", "request_id": "x" * 200}).status_code == 200
    assert client.post("/api/activity/log", data="{}", content_type="application/json").status_code == 200
    big = client.post(
        "/api/activity/log",
        data=json.dumps({"event": "x", "details": {"blob": "y" * 20000}}),
        content_type="application/json",
    )
    assert big.status_code == 413


def test_activity_token_requires_code(client):
    response = client.post("/api/activity/token", json={})
    assert response.status_code == 400
    assert response.get_json()["error"] == "缺少 code 参数"


def test_activity_token_logs_in_user(client, flask_app, monkeypatch):
    monkeypatch.setattr(
        flask_app,
        "exchange_discord_code",
        lambda code, redirect_uri=None, source="", fetch_guilds=True: (
            True,
            {"user": {"id": "1", "username": "u", "avatar": "a"}, "access_token": "tok"},
        ),
    )
    payload = client.post("/api/activity/token", json={"code": "abc"}).get_json()
    assert payload == {"ok": True, "user": {"id": "1", "username": "u", "avatar": "a"}, "access_token": "tok"}


def test_activity_token_propagates_rate_limit(client, flask_app, monkeypatch):
    monkeypatch.setattr(
        flask_app,
        "exchange_discord_code",
        lambda *a, **k: (False, {"error": "限速", "rate_limited": True, "retry_after": 3.0, "stage": "oauth_token"}),
    )
    response = client.post("/api/activity/token", json={"code": "abc"})
    assert response.status_code == 429
    assert response.get_json()["retry_after"] == 3.0


def test_auth_discord_redirects_to_discord(client):
    location = client.get("/auth/discord").headers["Location"]
    assert location.startswith("https://discord.com/api/v10/oauth2/authorize?")
    assert "client_id=123456789012345678" in location


def test_callback_reports_discord_errors(client):
    assert client.get("/callback?error=access_denied").status_code == 400
    assert client.get("/callback").status_code == 400


def test_callback_logs_in_and_selects_server(client, flask_app, server, admin_id, monkeypatch):
    def fake_exchange(code, redirect_uri=None, source="unknown", fetch_guilds=True):
        from flask import session

        session["user"] = {"id": admin_id, "username": "admin", "avatar": "a"}
        return True, {"user": session["user"], "access_token": "tok"}

    monkeypatch.setattr(flask_app, "exchange_discord_code", fake_exchange)
    assert client.get("/callback?code=abc").headers["Location"] in ("/", "http://localhost/")


def test_callback_without_server_data_goes_to_profile(client, flask_app, monkeypatch):
    monkeypatch.setattr(
        flask_app,
        "exchange_discord_code",
        lambda *a, **k: (True, {"user": {"id": "solo", "username": "s", "avatar": "a"}, "access_token": "t"}),
    )
    assert client.get("/callback?code=abc").headers["Location"].endswith("/me")


def test_callback_rate_limited_returns_429(client, flask_app, monkeypatch):
    monkeypatch.setattr(
        flask_app, "exchange_discord_code", lambda *a, **k: (False, {"error": "限速", "rate_limited": True})
    )
    assert client.get("/callback?code=abc").status_code == 429


def test_logout_clears_session(client, admin_id):
    login(client, admin_id)
    client.get("/logout")
    with client.session_transaction() as session:
        assert "user" not in session


def test_upload_json_requires_permission(client, server):
    login(client, "u2")
    assert client.post("/upload-json").status_code == 403


def test_upload_json_validates_file_type(client, admin_id):
    login(client, admin_id)
    assert client.post("/upload-json").status_code == 400
    data = {"json_file": (io.BytesIO(b"x"), "notes.txt")}
    assert client.post("/upload-json", data=data, content_type="multipart/form-data").status_code == 400


def test_upload_json_imports_server(client, flask_app, admin_id):
    payload = {
        "guild": {"id": "900", "name": "Uploaded", "iconUrl": "http://icon"},
        "channel": {"id": "t900", "name": "thread", "categoryId": "c"},
        "messages": [
            {
                "id": "m900",
                "timestamp": "2026-01-01T00:00:00+00:00",
                "content": "hello",
                "author": {"id": "a1", "name": "a", "nickname": "A", "avatarUrl": "", "isBot": False},
            }
        ],
    }
    login(client, admin_id)
    data = {"json_file": (io.BytesIO(json.dumps(payload).encode("utf-8")), "export.json")}
    response = client.post("/upload-json", data=data, content_type="multipart/form-data")
    assert response.status_code == 302
    portal = portal_conn(flask_app)
    assert portal.execute("SELECT name FROM servers WHERE server_id='900'").fetchone()["name"] == "Uploaded"
    portal.close()


def test_upload_json_reports_bad_payload(client, admin_id):
    login(client, admin_id)
    data = {"json_file": (io.BytesIO(b"not json"), "export.json")}
    response = client.post("/upload-json", data=data, content_type="multipart/form-data")
    assert response.status_code == 400
    assert "导入失败" in response.get_data(as_text=True)


def test_upload_json_enforces_quota_for_level2(client, flask_app):
    portal = portal_conn(flask_app)
    portal.execute("INSERT INTO whitelist_users(user_id,username,added_by,created_at) VALUES('u2','m','a','t')")
    portal.execute("INSERT INTO server_download_quota(user_id,quota) VALUES('u2',1)")
    portal.execute(
        "INSERT INTO user_server_access(user_id,server_id,granted_by,created_at) VALUES('u2','999','a','t')"
    )
    portal.commit()
    portal.close()
    payload = {
        "guild": {"id": "901", "name": "Quota"},
        "channel": {"id": "t901", "name": "thread"},
        "messages": [],
    }
    login(client, "u2")
    data = {"json_file": (io.BytesIO(json.dumps(payload).encode("utf-8")), "export.json")}
    response = client.post("/upload-json", data=data, content_type="multipart/form-data")
    assert response.status_code == 403


def test_report_and_chouxiangpai_pages(client, server, admin_id):
    login(client, admin_id)
    client.get("/server/100")
    assert client.get("/report").status_code == 200
    assert client.get("/chouxiangpai").status_code == 200
