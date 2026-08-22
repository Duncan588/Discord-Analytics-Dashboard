"""app.py 中与请求无关的纯逻辑：时间格式化、词云、缓存引擎、权限判断。"""
import collections
import os
from datetime import datetime, timedelta, timezone

import pytest

from tests.conftest import build_server


def test_load_local_env_only_fills_missing_keys(app_module, tmp_path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text(
        "# comment\n"
        "\n"
        "NO_EQUALS_LINE\n"
        "TEST_NEW_KEY='quoted-value'\n"
        "TEST_EXISTING_KEY=from-file\n",
        encoding="utf-8",
    )
    monkeypatch.delenv("TEST_NEW_KEY", raising=False)
    monkeypatch.setenv("TEST_EXISTING_KEY", "from-environ")

    app_module.load_local_env(str(env_file))

    assert os.environ["TEST_NEW_KEY"] == "quoted-value"
    assert os.environ["TEST_EXISTING_KEY"] == "from-environ"
    os.environ.pop("TEST_NEW_KEY", None)


def test_load_local_env_ignores_missing_file(app_module, tmp_path):
    app_module.load_local_env(str(tmp_path / "absent.env"))


def test_env_keys_skips_comments_and_blank_lines(app_module, tmp_path):
    path = tmp_path / ".env"
    path.write_text("# c\n\nA=1\nB=\nJUNK\n", encoding="utf-8")
    assert app_module._env_keys(str(path)) == {"A", "B"}


def test_env_keys_for_missing_file_is_empty(app_module, tmp_path):
    assert app_module._env_keys(str(tmp_path / "nope")) == set()


@pytest.mark.parametrize(
    "seconds,expected",
    [(0, "00:00"), (-30, "00:00"), (59, "00:59"), (61, "01:01"), (3600, "1:00:00"), (3725, "1:02:05")],
)
def test_format_time(app_module, seconds, expected):
    assert app_module.format_time(seconds) == expected


def test_parse_and_convert_uses_shanghai_offset(app_module):
    parsed = app_module.parse_and_convert("2026-01-01T00:00:00Z")
    assert parsed == datetime(2026, 1, 1, 8, 0, tzinfo=timezone(timedelta(hours=8)))


def test_parse_and_convert_assumes_utc_for_naive_input(app_module):
    assert app_module.parse_and_convert("2026-01-01T00:00:00").hour == 8


@pytest.mark.parametrize("value", [None, "", "not-a-date"])
def test_parse_and_convert_returns_none_for_invalid_input(app_module, value):
    assert app_module.parse_and_convert(value) is None


def test_datetime_filters(app_module):
    assert app_module.datetimeformat_filter("2026-01-01T00:00:00Z") == "2026-01-01 08:00"
    assert app_module.datetimeformat_filter("2026-01-01T00:00:00Z", "%Y") == "2026"
    # 无法解析时原样返回，模板不会因为脏数据报错。
    assert app_module.datetimeformat_filter("garbage") == "garbage"
    assert app_module.raw_datetime_filter("2026-01-01T00:00:00Z") == "2026-01-01 08:00"


@pytest.mark.parametrize(
    "word,expected",
    [("中文", True), ("中文abc", False), ("", False), (None, False), ("123", False)],
)
def test_is_pure_chinese(app_module, word, expected):
    assert app_module.is_pure_chinese(word) is expected


def test_get_word_cloud_counter_drops_stopwords_and_single_chars(app_module):
    counter = app_module.get_word_cloud_counter(["数据 数据 什么 这个 好", None, "数据分析"])
    assert counter["数据"] == 2
    assert counter["数据分析"] == 1
    assert "什么" not in counter
    assert "好" not in counter


def test_format_word_cloud_sorts_and_limits(app_module):
    counter = collections.Counter({"数据": 5, "分析": 3, "mixed中文": 9})
    result = app_module.format_word_cloud(counter, limit=1)
    assert result == [{"text": "数据", "weight": 5}]
    assert [x["text"] for x in app_module.format_word_cloud(counter)] == ["数据", "分析"]


def test_process_messages_attaches_top_three_reactions(flask_app, tmp_path):
    path = tmp_path / "analytics.db"
    flask_app.init_server_db(str(path))
    conn = flask_app.db_connect(str(path))
    for emoji, count in (("a", 3), ("b", 2), ("c", 1), ("d", 1)):
        for i in range(count):
            conn.execute(
                "INSERT INTO reactions(message_id,user_id,emoji_name,emoji_url) VALUES('m1',?,?,'url')",
                (f"u{emoji}{i}", emoji),
            )
    conn.commit()
    rows = [{"message_id": "m1"}, {"message_id": "m2"}]

    result = flask_app.process_messages(conn, rows)

    assert len(result[0]["detailed_reactions"]) == 3
    assert result[0]["detailed_reactions"][0]["emoji_name"] == "a"
    assert result[1]["detailed_reactions"] == []
    conn.close()


def test_process_messages_short_circuits_without_rows(flask_app, tmp_path):
    path = tmp_path / "analytics.db"
    flask_app.init_server_db(str(path))
    conn = flask_app.db_connect(str(path))
    assert flask_app.process_messages(conn, []) == []
    assert flask_app.process_messages(conn, [{"message_id": None}]) == [{"message_id": None}]
    conn.close()


def test_server_db_path_is_per_server(flask_app):
    assert flask_app.server_db_path("42") == os.path.join(flask_app.DATA_DIR, "42", "discord_data.db")


def test_db_connect_returns_row_objects(flask_app, tmp_path):
    conn = flask_app.db_connect(str(tmp_path / "plain.db"))
    conn.execute("CREATE TABLE t(a TEXT)")
    conn.execute("INSERT INTO t VALUES('x')")
    assert conn.execute("SELECT a FROM t").fetchone()["a"] == "x"
    conn.close()


def test_init_server_db_creates_expected_tables(flask_app, tmp_path):
    path = str(tmp_path / "nested" / "discord_data.db")
    flask_app.init_server_db(path)
    conn = flask_app.db_connect(path)
    tables = {r["name"] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    conn.close()
    assert {"users", "threads", "messages", "reactions", "user_stats", "user_merges"} <= tables


def test_init_portal_db_migrates_legacy_download_servers(flask_app):
    conn = flask_app.db_connect(flask_app.PORTAL_DB)
    conn.execute(
        "INSERT INTO download_servers(server_id,owner_user_id,guild_id,forum_channel_id,bot_id,enabled,"
        "use_default_bot,updated_at) VALUES('g:f','owner','g','f',7,1,1,'2026-01-01T00:00:00+00:00')"
    )
    conn.execute("INSERT INTO whitelist_users(user_id,username,added_by,created_at) VALUES('u1','n','a','t')")
    conn.commit()
    conn.close()

    flask_app.init_portal_db()

    conn = flask_app.db_connect(flask_app.PORTAL_DB)
    config = conn.execute("SELECT * FROM download_configs WHERE guild_id='g'").fetchone()
    quota = conn.execute("SELECT quota FROM server_download_quota WHERE user_id='u1'").fetchone()
    bots = conn.execute("SELECT bot_id FROM download_config_bots WHERE config_id=?", (config["id"],)).fetchall()
    conn.close()
    assert config["forum_channel_id"] == "f"
    assert config["use_default_bot"] == 1
    assert quota["quota"] == 1
    assert [row["bot_id"] for row in bots] == [7]


def test_migrate_legacy_db_imports_root_database(flask_app, tmp_path, monkeypatch):
    legacy = tmp_path / "discord_data.db"
    flask_app.init_server_db(str(legacy))
    monkeypatch.setattr(flask_app, "LEGACY_DB", str(legacy))
    monkeypatch.setenv("LEGACY_SERVER_ID", "555")

    flask_app.migrate_legacy_db()

    conn = flask_app.db_connect(flask_app.PORTAL_DB)
    row = conn.execute("SELECT * FROM servers WHERE server_id='555'").fetchone()
    conn.close()
    assert row["name"] == "Legacy Discord Server"
    assert os.path.exists(flask_app.server_db_path("555"))

    # 已经有服务器数据时不再重复迁移。
    flask_app.migrate_legacy_db()


def test_migrate_legacy_db_without_legacy_file_is_noop(flask_app, tmp_path, monkeypatch):
    monkeypatch.setattr(flask_app, "LEGACY_DB", str(tmp_path / "missing.db"))
    flask_app.migrate_legacy_db()
    conn = flask_app.db_connect(flask_app.PORTAL_DB)
    assert conn.execute("SELECT count(*) FROM servers").fetchone()[0] == 0
    conn.close()


def test_admin_level_and_quota(flask_app, admin_id):
    with flask_app.app.test_request_context("/"):
        assert flask_app.admin_level(admin_id) == 1
        assert flask_app.admin_level("999") == 0
        assert flask_app.get_download_quota("999") == 1

        portal = flask_app.get_portal_db()
        portal.execute(
            "INSERT INTO whitelist_users(user_id,username,added_by,created_at) VALUES('999','n','a','t')"
        )
        portal.execute("INSERT INTO server_download_quota(user_id,quota) VALUES('999',4)")
        portal.commit()

        assert flask_app.admin_level("999") == 2
        assert flask_app.get_download_quota("999") == 4
        assert flask_app.whitelist_allowed("999") is True
        assert flask_app.whitelist_allowed("1234") is False


def test_get_db_requires_selected_and_existing_server(flask_app):
    with flask_app.app.test_request_context("/"):
        with pytest.raises(RuntimeError, match="未选择服务器"):
            flask_app.get_db()

    with flask_app.app.test_request_context("/") as ctx:
        from flask import session

        session["server_id"] = "404"
        with pytest.raises(RuntimeError, match="服务器数据不存在"):
            flask_app.get_db()
        assert ctx  # 请求上下文保持有效


def test_get_db_reuses_connection(flask_app):
    build_server(flask_app, "77")
    with flask_app.app.test_request_context("/"):
        from flask import session

        session["server_id"] = "77"
        assert flask_app.current_server_id() == "77"
        assert flask_app.get_db() is flask_app.get_db()


def test_register_server_upserts(flask_app):
    with flask_app.app.test_request_context("/"):
        flask_app.register_server("31", "First", icon_url="i", owner_user_id="u")
        flask_app.register_server("31", "Renamed")
        rows = flask_app.get_portal_db().execute("SELECT * FROM servers").fetchall()
    assert len(rows) == 1
    assert rows[0]["name"] == "Renamed"


def test_sync_server_users_to_portal(flask_app):
    build_server(
        flask_app,
        "88",
        users=[{"user_id": "u1", "username": "one", "nickname": "One"}, {"user_id": "u2", "username": "two"}],
    )
    with flask_app.app.test_request_context("/"):
        assert flask_app.sync_server_users_to_portal("88") == 2
        portal = flask_app.get_portal_db()
        assert portal.execute("SELECT count(*) FROM portal_users").fetchone()[0] == 2
        assert portal.execute("SELECT count(*) FROM user_server_presence WHERE server_id='88'").fetchone()[0] == 2
        # 缺少数据库文件时安全返回 0。
        assert flask_app.sync_server_users_to_portal("does-not-exist") == 0


def test_sync_all_server_users_backfills_once(flask_app):
    build_server(flask_app, "89", users=[{"user_id": "u9", "username": "nine"}])
    with flask_app.app.test_request_context("/"):
        flask_app.sync_all_server_users()
        portal = flask_app.get_portal_db()
        assert portal.execute("SELECT count(*) FROM user_server_presence").fetchone()[0] == 1
        portal.execute("DELETE FROM portal_users")
        portal.commit()
        # presence 已有数据，第二次调用直接返回，不再全库扫描。
        flask_app.sync_all_server_users()
        assert portal.execute("SELECT count(*) FROM portal_users").fetchone()[0] == 0


def test_get_servers_for_user_grants_access_by_presence(flask_app, admin_id):
    build_server(flask_app, "10", name="Ten", users=[{"user_id": "u1", "username": "one"}])
    build_server(flask_app, "11", name="Eleven")
    with flask_app.app.test_request_context("/"):
        # 权限 1 管理员能看到全部服务器。
        assert {str(x["server_id"]) for x in flask_app.get_servers_for_user(admin_id)} == {"10", "11"}
        # 普通用户只能看到分析库里出现过的服务器，并自动补 access 记录。
        assert [str(x["server_id"]) for x in flask_app.get_servers_for_user("u1")] == ["10"]
        portal = flask_app.get_portal_db()
        assert portal.execute("SELECT 1 FROM user_server_access WHERE user_id='u1'").fetchone()
        assert flask_app.user_has_server_data("nobody") == []


def test_get_servers_for_user_skips_missing_database_file(flask_app, admin_id, tmp_path):
    with flask_app.app.test_request_context("/"):
        portal = flask_app.get_portal_db()
        portal.execute(
            "INSERT INTO servers(server_id,name,icon_url,owner_user_id,db_path,created_at,updated_at)"
            " VALUES('12','Gone',NULL,NULL,?,'t','t')",
            (str(tmp_path / "missing.db"),),
        )
        portal.commit()
        assert flask_app.get_servers_for_user(admin_id) == []


def test_get_servers_for_user_includes_owner(flask_app):
    build_server(flask_app, "13", owner_user_id="owner-1")
    with flask_app.app.test_request_context("/"):
        assert [str(x["server_id"]) for x in flask_app.get_servers_for_user("owner-1")] == ["13"]


def test_data_engine_builds_and_caches_homepage(flask_app):
    build_server(
        flask_app,
        "20",
        users=[{"user_id": "u1", "username": "one", "nickname": "One"}],
        messages=[
            {"message_id": "m1", "thread_id": "t1", "author_id": "u1", "content": "数据分析 数据分析"},
            {"message_id": "m2", "thread_id": "t1", "author_id": "u1", "content": "数据分析"},
        ],
    )
    conn = flask_app.db_connect(flask_app.server_db_path("20"))
    conn.execute("INSERT INTO reactions(message_id,user_id,emoji_name,emoji_url) VALUES('m1','u1','ok','url')")
    conn.execute(
        "INSERT INTO user_merges(target_id,parent_id,created_at) VALUES('child','u1','2026-01-01T00:00:00+00:00')"
    )
    conn.commit()
    conn.close()

    engine = flask_app.DataEngine()
    data = engine.load_or_compute("20")

    assert data["total_msgs"] == 2
    assert data["total_threads"] == 1
    assert data["total_users"] == 1
    assert data["server_word_cloud"][0] == {"text": "数据分析", "weight": 3}
    assert data["top_users"][0]["msg_count"] == 2
    assert data["top_threads"][0]["msg_count"] == 2
    assert data["top_hot_msgs"][0]["message_id"] == "m1"
    assert data["chart_daily"][0]["c"] == 2
    assert len(data["chart_hourly"]) == 24
    assert engine.get_merged_ids("20", "u1") == ["u1", "child"]

    # 第二次读取命中磁盘缓存，不再重算。
    assert os.path.exists(engine._cache_file("20"))
    fresh_engine = flask_app.DataEngine()
    assert fresh_engine.load_or_compute("20")["total_msgs"] == 2


def test_data_engine_schedules_background_refresh(flask_app, monkeypatch):
    build_server(
        flask_app,
        "21",
        users=[{"user_id": "u1", "username": "one"}],
        messages=[{"message_id": "m1", "thread_id": "t1", "author_id": "u1", "content": "hi"}],
    )
    engine = flask_app.DataEngine()
    engine.load_or_compute("21")

    scheduled = []
    monkeypatch.setattr(engine, "_schedule_refresh", lambda sid: scheduled.append(sid))
    engine.cache["21"]["last_msg_count"] = 0
    engine.load_or_compute("21")
    assert scheduled == ["21"]


def test_data_engine_refresh_throttles_and_swallows_errors(flask_app):
    engine = flask_app.DataEngine()
    engine._schedule_refresh("missing-server")
    assert "missing-server" in engine._refreshing
    engine._schedule_refresh("missing-server")

    # 不存在的服务器数据库不会让刷新线程抛出异常。
    engine._refreshing.add("missing-server")
    with flask_app.app.app_context():
        engine._refresh("missing-server")
    assert "missing-server" not in engine._refreshing


def test_data_engine_load_cache_falls_back_to_empty_snapshot(flask_app):
    engine = flask_app.DataEngine()
    cache = engine._load_cache("no-cache-file")
    assert cache["last_msg_count"] == -1
    assert cache["homepage"] == {}
    assert engine._load_cache("no-cache-file") is cache


def test_data_engine_get_merged_ids_reads_database_when_cache_empty(flask_app):
    build_server(flask_app, "22")
    conn = flask_app.db_connect(flask_app.server_db_path("22"))
    conn.execute("INSERT INTO user_merges(target_id,parent_id,created_at) VALUES('c1','p1','t')")
    conn.commit()
    conn.close()
    engine = flask_app.DataEngine()
    assert engine.get_merged_ids("22", "p1") == ["p1", "c1"]


def test_configure_logging_is_idempotent(flask_app):
    flask_app.configure_logging()
    handlers = len(flask_app.app.logger.handlers)
    flask_app.configure_logging()
    assert len(flask_app.app.logger.handlers) == handlers


def test_request_id_outside_request_context(app_module):
    assert app_module._request_id() == "-"
