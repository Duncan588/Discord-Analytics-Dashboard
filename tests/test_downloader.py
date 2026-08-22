"""Preparation_Before_Use/discord_downloader.py 中不需要 Discord 网络连接的部分。

门户数据库复用 app.init_portal_db() 建出的真实结构（flask_app fixture），
但所有文件都落在 pytest 临时目录里。
"""
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from Preparation_Before_Use import discord_downloader as dd


@pytest.fixture
def downloader(flask_app, tmp_path, monkeypatch):
    monkeypatch.setattr(dd, "PORTAL_DB", Path(flask_app.PORTAL_DB))
    monkeypatch.setattr(dd, "SERVER_DATA_DIR", tmp_path / "servers")
    monkeypatch.setattr(dd, "RAW_DIR", tmp_path / "raw")
    conn = dd.db()
    dd.ensure_schema(conn)
    conn.close()
    return dd


def make_task(downloader, guild_id="10", status="pending", **values):
    conn = downloader.db()
    columns = {
        "guild_id": guild_id,
        "forum_channel_id": "20",
        "created_by": "u1",
        "status": status,
        "created_at": downloader.now(),
    }
    columns.update(values)
    names = ",".join(columns)
    placeholders = ",".join("?" for _ in columns)
    cursor = conn.execute(f"INSERT INTO download_tasks({names}) VALUES({placeholders})", tuple(columns.values()))
    conn.commit()
    task_id = int(cursor.lastrowid)
    conn.close()
    return task_id


def task_row(downloader, task_id):
    conn = downloader.db()
    row = conn.execute("SELECT * FROM download_tasks WHERE id=?", (task_id,)).fetchone()
    conn.close()
    return row


def test_load_local_env_only_sets_missing_keys(tmp_path, monkeypatch):
    env = tmp_path / ".env"
    env.write_text("# c\nDL_TEST_A=\"1\"\nDL_TEST_B=2\nbroken\n", encoding="utf-8")
    monkeypatch.delenv("DL_TEST_A", raising=False)
    monkeypatch.setenv("DL_TEST_B", "keep")
    dd.load_local_env(env)
    import os

    assert os.environ["DL_TEST_A"] == "1"
    assert os.environ["DL_TEST_B"] == "keep"
    os.environ.pop("DL_TEST_A", None)
    dd.load_local_env(tmp_path / "missing.env")


def test_db_requires_existing_portal_database(tmp_path, monkeypatch):
    monkeypatch.setattr(dd, "PORTAL_DB", tmp_path / "absent.db")
    with pytest.raises(FileNotFoundError):
        dd.db()


def test_ensure_schema_is_idempotent(downloader):
    conn = downloader.db()
    downloader.ensure_schema(conn)
    tables = {r["name"] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    columns = {r["name"] for r in conn.execute("PRAGMA table_info(download_tasks)")}
    conn.close()
    assert {"download_tasks", "download_task_items", "download_configs", "download_config_bots"} <= tables
    assert {"mode", "scan_cursor", "delete_requested", "scheduler_interval"} <= columns


def test_snowflake_from_ms():
    assert dd.snowflake_from_ms(dd.DISCORD_EPOCH_MS) == 0
    assert dd.snowflake_from_ms(dd.DISCORD_EPOCH_MS + 1) == 1 << 22
    # Discord 纪元之前的时间不会产生负数 ID。
    assert dd.snowflake_from_ms(0) == 0


@pytest.mark.parametrize("bot_count", [1, 3, 5])
def test_scan_partitions_cover_the_whole_range(bot_count):
    parts = dd.scan_partitions(bot_count)
    assert len(parts) == bot_count
    assert all(lower < upper for lower, upper in parts)
    for (_, upper), (lower, _) in zip(parts, parts[1:]):
        assert upper == lower


def test_scan_partitions_never_returns_empty():
    assert len(dd.scan_partitions(0)) == 1
    assert len(dd.scan_partitions(-3)) == 1


def test_bot_download_delay_ms(downloader):
    assert dd.bot_download_delay_ms() == 0

    task_id = make_task(downloader, download_interval_ms=1500)
    assert dd.bot_download_delay_ms(task_row(downloader, task_id)) == 1500

    task_id = make_task(downloader, download_interval_ms=999999)
    assert dd.bot_download_delay_ms(task_row(downloader, task_id)) == 60000

    task_id = make_task(downloader, download_interval_ms=None)
    assert dd.bot_download_delay_ms(task_row(downloader, task_id)) == 0

    # 行里没有该列时（旧结构）也返回 0。
    conn = downloader.db()
    row = conn.execute("SELECT id FROM download_tasks LIMIT 1").fetchone()
    conn.close()
    assert dd.bot_download_delay_ms(row) == 0


def test_update_task_sets_heartbeat(downloader):
    task_id = make_task(downloader)
    dd.update_task(task_id)  # 空更新直接返回
    dd.update_task(task_id, status="running", message="working")
    row = task_row(downloader, task_id)
    assert (row["status"], row["message"]) == ("running", "working")
    assert row["heartbeat_at"]

    dd.update_task(task_id, heartbeat_at="2020-01-01T00:00:00+00:00")
    assert task_row(downloader, task_id)["heartbeat_at"] == "2020-01-01T00:00:00+00:00"


def test_task_status_defaults_to_cancelled_for_missing_task(downloader):
    task_id = make_task(downloader, status="running")
    assert dd.task_status(task_id) == "running"
    assert dd.task_status(99999) == "cancelled"


def test_pending_tasks_skips_delete_requested(downloader):
    first = make_task(downloader)
    make_task(downloader, status="running")
    make_task(downloader, delete_requested=1)
    rows = dd.pending_tasks(5)
    assert [int(r["id"]) for r in rows] == [first]
    assert int(dd.pending_task()["id"]) == first


def test_pending_task_returns_none_when_queue_is_empty(downloader):
    assert dd.pending_task() is None


def test_recover_stale_tasks_requeues_running_tasks(downloader):
    running = make_task(downloader, status="running")
    deleting = make_task(downloader, status="running", delete_requested=1)
    dd.recover_stale_tasks()
    assert task_row(downloader, running)["status"] == "pending"
    assert task_row(downloader, running)["phase"] == "queued"
    assert task_row(downloader, deleting)["status"] == "running"


def test_task_items_lifecycle(downloader):
    task_id = make_task(downloader)
    dd.upsert_task_items(task_id, [])
    dd.upsert_task_items(task_id, [{"id": 1, "name": "First"}, {"id": 2}])
    rows = dd.item_rows(task_id)
    assert [(r["thread_id"], r["thread_name"], r["status"], r["filename"]) for r in rows] == [
        ("1", "First", "pending", "1.json"),
        ("2", "thread", "pending", "2.json"),
    ]

    # 再次 upsert 只更新名称和文件名，不重置状态。
    dd.mark_item(task_id, 1, status="downloaded", bot_name="A")
    dd.mark_item(task_id, 1)
    dd.upsert_task_items(task_id, [{"id": 1, "name": "Renamed"}])
    row = next(r for r in dd.item_rows(task_id) if r["thread_id"] == "1")
    assert (row["thread_name"], row["status"], row["bot_name"]) == ("Renamed", "downloaded", "A")


def test_set_scan_progress_accumulates_and_computes_speed(downloader):
    started = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
    task_id = make_task(downloader, scan_started_at=started)

    dd._set_scan_progress(task_id, discovered_delta=5, processed_delta=2)
    dd._set_scan_progress(task_id, discovered_delta=1, processed_delta=1, active_bots=3, message="扫描中")

    row = task_row(downloader, task_id)
    assert (row["scan_discovered"], row["scan_processed"]) == (6, 3)
    assert (row["active_bots"], row["message"]) == (3, "扫描中")
    assert row["speed"] > 0

    dd._set_scan_progress(99999, discovered_delta=1)


def test_set_scan_progress_tolerates_bad_started_at(downloader):
    task_id = make_task(downloader, scan_started_at="not-a-timestamp")
    dd._set_scan_progress(task_id, processed_delta=1)
    assert task_row(downloader, task_id)["speed"] == 0


def test_get_token_prefers_configured_bot(downloader, monkeypatch):
    conn = dd.db()
    conn.execute("INSERT INTO download_bots(id,owner_user_id,name,token,created_at) VALUES(7,'u','Bot A','tok-a','t')")
    conn.commit()
    conn.close()

    assert dd.get_token(7) == {"name": "Bot A", "token": "tok-a"}

    monkeypatch.setenv("DISCORD_DOWNLOADER_TOKEN", "tok-env")
    assert dd.get_token() == {"name": "env-default", "token": "tok-env"}
    assert dd.get_token(999) == {"name": "env-default", "token": "tok-env"}

    monkeypatch.setenv("DISCORD_DOWNLOADER_TOKEN", "")
    monkeypatch.setenv("DISCORD_DOWNLOADER", "")
    assert dd.get_token() is None


def test_get_server_bots(downloader, monkeypatch):
    monkeypatch.setenv("DISCORD_DOWNLOADER_TOKEN", "tok-env")
    conn = dd.db()
    conn.execute("INSERT INTO download_bots(id,owner_user_id,name,token,created_at) VALUES(1,'u','A','tok-a','t')")
    conn.execute(
        "INSERT INTO download_configs(id,server_id,owner_user_id,guild_id,forum_channel_id,enabled,use_default_bot,"
        "updated_at) VALUES(1,'10:20','u','10','20',1,1,'t')"
    )
    conn.execute("INSERT INTO download_config_bots(config_id,bot_id) VALUES(1,1)")
    conn.commit()
    conn.close()

    by_config = dd.get_server_bots(config_id=1)
    assert [b["name"] for b in by_config] == ["A", "默认下载机器人"]
    assert [b["name"] for b in dd.get_server_bots(guild_id="10")] == ["A", "默认下载机器人"]
    # 没有配置时退回默认机器人。
    assert [b["name"] for b in dd.get_server_bots(guild_id="999")] == ["默认下载机器人"]

    monkeypatch.setenv("DISCORD_DOWNLOADER_TOKEN", "")
    monkeypatch.setenv("DISCORD_DOWNLOADER", "")
    assert dd.get_server_bots() == []


def test_authenticate_download_bots_keeps_valid_tokens(downloader, monkeypatch):
    assert dd.authenticate_download_bots(1, []) == []

    async def fake_login(token):
        if token == "bad":
            raise RuntimeError("401 Unauthorized")
        return "555"

    monkeypatch.setattr(dd, "_login_bot_token", fake_login)
    bots = [{"name": "good", "token": "ok"}, {"name": "bad", "token": "bad"}]
    assert [b["name"] for b in dd.authenticate_download_bots(1, bots)] == ["good"]


def test_notify_task_started_sends_direct_message(downloader, monkeypatch):
    monkeypatch.setenv("DISCORD_BOT_TOKEN", "tok")
    calls = []

    class _Response:
        def __init__(self, status_code=200, payload=None):
            self.status_code = status_code
            self._payload = payload or {}

        def json(self):
            return self._payload

    def fake_post(url, headers=None, json=None, timeout=None):
        calls.append((url, json))
        if url.endswith("/users/@me/channels"):
            return _Response(200, {"id": "dm1"})
        return _Response(200, {})

    monkeypatch.setattr(dd.requests, "post", fake_post)
    task_id = make_task(downloader)
    dd.notify_task_started(task_row(downloader, task_id), 2, resumed=True)
    assert len(calls) == 2
    assert "已继续" in calls[1][1]["content"]


def test_notify_task_started_survives_api_errors(downloader, monkeypatch):
    monkeypatch.setenv("DISCORD_BOT_TOKEN", "tok")

    def boom(*args, **kwargs):
        raise dd.requests.ConnectionError("offline")

    monkeypatch.setattr(dd.requests, "post", boom)
    task_id = make_task(downloader)
    dd.notify_task_started(task_row(downloader, task_id), 1, resumed=False)


def test_notify_task_started_skipped_without_token(downloader, monkeypatch):
    monkeypatch.setenv("DISCORD_BOT_TOKEN", "")

    def fail(*args, **kwargs):
        raise AssertionError("不应该发起请求")

    monkeypatch.setattr(dd.requests, "post", fail)
    task_id = make_task(downloader)
    dd.notify_task_started(task_row(downloader, task_id), 1, resumed=False)


def _build_analytics_db(downloader, guild_id="10", threads=(("t1", "m1"),)):
    from Preparation_Before_Use import discordDB

    path = downloader.server_db_for(guild_id)
    conn = discordDB.connect(str(path))
    cur = conn.cursor()
    discordDB.create_tables(cur)
    for thread_id, message_id in threads:
        cur.execute("INSERT INTO threads(thread_id,name,guild_id) VALUES(?,?,?)", (thread_id, "T", guild_id))
        cur.execute(
            "INSERT INTO messages(message_id,thread_id,author_id,content,timestamp) VALUES(?,?,?,?,?)",
            (message_id, thread_id, "a1", "hi", "2026-01-01T00:00:00+00:00"),
        )
        cur.execute(
            "INSERT INTO reactions(message_id,user_id,emoji_name,emoji_url) VALUES(?,?,?,?)",
            (message_id, "a2", "x", "u"),
        )
    cur.execute("INSERT INTO users(user_id,username,nickname,avatar_url,is_bot) VALUES('a1','one','One','av',0)")
    conn.commit()
    conn.close()
    return path


def test_server_db_for_path(downloader):
    assert dd.server_db_for("10").name == "discord_data.db"
    assert dd.server_db_for("10").parent.name == "10"


def test_remove_thread_from_server_db(downloader, tmp_path):
    path = _build_analytics_db(downloader)
    dd._remove_thread_from_server_db(path, "t1")
    conn = sqlite3.connect(path)
    assert conn.execute("SELECT count(*) FROM messages").fetchone()[0] == 0
    assert conn.execute("SELECT count(*) FROM reactions").fetchone()[0] == 0
    assert conn.execute("SELECT count(*) FROM threads").fetchone()[0] == 0
    conn.close()
    dd._remove_thread_from_server_db(tmp_path / "missing.db", "t1")


def test_existing_thread_active_prefers_analytics_database(downloader):
    path = _build_analytics_db(downloader)
    conn = sqlite3.connect(path)
    conn.execute("UPDATE threads SET last_active_at='2026-02-01' WHERE thread_id='t1'")
    conn.commit()
    conn.close()
    assert dd.existing_thread_active("10", "t1") == "2026-02-01"


def test_existing_thread_active_falls_back_to_scan_state(downloader):
    assert dd.existing_thread_active("10", "t1") is None
    dd.record_scanned_thread("10", {"id": "t1", "name": "T", "last_active_at": "2026-03-01"})
    assert dd.existing_thread_active("10", "t1") == "2026-03-01"
    # 没有 last_active_at 时退回 created_at。
    dd.record_scanned_thread("10", {"id": "t2", "created_at": "2026-04-01"})
    assert dd.existing_thread_active("10", "t2") == "2026-04-01"


def test_import_one_json_replaces_thread(downloader, tmp_path):
    payload = {
        "guild": {"id": "10", "name": "Guild"},
        "channel": {"id": "t1", "name": "Thread"},
        "messages": [
            {
                "id": "m2",
                "timestamp": "2026-01-02T00:00:00+00:00",
                "content": "new",
                "author": {"id": "a1", "name": "one"},
            }
        ],
    }
    _build_analytics_db(downloader)
    export = tmp_path / "t1.json"
    export.write_text(json.dumps(payload), encoding="utf-8")

    db_path, result = dd.import_one_json(export, "10", "t1", "2026-01-02T00:00:00+00:00")

    assert result["messages"] == 1
    conn = sqlite3.connect(db_path)
    assert [r[0] for r in conn.execute("SELECT message_id FROM messages")] == ["m2"]
    assert conn.execute("SELECT last_active_at FROM threads WHERE thread_id='t1'").fetchone()[0] == (
        "2026-01-02T00:00:00+00:00"
    )
    conn.close()


def test_import_downloaded_task(downloader, tmp_path):
    task_id = make_task(downloader)
    dd.upsert_task_items(task_id, [{"id": "t1", "name": "Thread"}])
    with pytest.raises(RuntimeError, match="没有可导入的帖子数据"):
        dd.import_downloaded_task(tmp_path / "task", "10", task_id)

    dd.mark_item(task_id, "t1", status="downloaded")
    task_root = tmp_path / "task"
    (task_root / "backup").mkdir(parents=True)
    with pytest.raises(RuntimeError, match="任务文件缺失"):
        dd.import_downloaded_task(task_root, "10", task_id)

    (task_root / "backup" / "t1.json").write_text(
        json.dumps(
            {
                "guild": {"id": "10", "name": "Guild", "iconUrl": "http://icon"},
                "channel": {"id": "t1", "name": "Thread"},
                "messages": [
                    {
                        "id": "m1",
                        "timestamp": "2026-01-01T00:00:00+00:00",
                        "author": {"id": "a1", "name": "one"},
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    db_path, server_name, icon_url, total = dd.import_downloaded_task(task_root, "10", task_id)
    assert (server_name, icon_url, total) == ("Guild", "http://icon", 1)
    assert Path(db_path).exists()
    # 导入后删除临时 JSON，避免占用磁盘。
    assert not (task_root / "backup" / "t1.json").exists()
    assert task_row(downloader, task_id)["phase"] == "importing"


def test_import_downloaded_task_stops_when_cancelled(downloader, tmp_path):
    task_id = make_task(downloader, status="cancelled")
    dd.upsert_task_items(task_id, [{"id": "t1"}])
    dd.mark_item(task_id, "t1", status="downloaded")
    with pytest.raises(dd.TaskCancelled):
        dd.import_downloaded_task(tmp_path / "task", "10", task_id)


def test_delete_task_server_data_keeps_shared_threads(downloader):
    _build_analytics_db(downloader, threads=(("t1", "m1"), ("t2", "m2")))
    first = make_task(downloader)
    second = make_task(downloader)
    dd.upsert_task_items(first, [{"id": "t1"}, {"id": "t2"}])
    dd.upsert_task_items(second, [{"id": "t2"}])
    dd.mark_item(second, "t2", status="downloaded")

    dd.delete_task_server_data(first, "10")

    conn = sqlite3.connect(dd.server_db_for("10"))
    assert [r[0] for r in conn.execute("SELECT thread_id FROM threads")] == ["t2"]
    conn.close()


def test_delete_task_server_data_without_items(downloader):
    task_id = make_task(downloader)
    dd.delete_task_server_data(task_id, "10")


def test_finalize_delete_if_requested(downloader, tmp_path):
    db_path = _build_analytics_db(downloader)
    conn = dd.db()
    conn.execute(
        "INSERT INTO servers(server_id,name,db_path,created_at,updated_at) VALUES('10','G',?, 't','t')",
        (str(db_path),),
    )
    conn.execute("INSERT INTO user_server_access(user_id,server_id,granted_by,created_at) VALUES('u','10','a','t')")
    conn.commit()
    conn.close()

    task_id = make_task(downloader, status="cancelled")
    dd.upsert_task_items(task_id, [{"id": "t1"}])
    task_root = tmp_path / "raw" / "10" / "tasks" / str(task_id)
    task_root.mkdir(parents=True)

    # 没有删除请求时什么都不做。
    dd.finalize_delete_if_requested(task_id, task_root)
    assert task_row(downloader, task_id) is not None

    dd.update_task(task_id, delete_requested=1)
    dd.finalize_delete_if_requested(task_id, task_root)

    conn = dd.db()
    assert conn.execute("SELECT count(*) FROM download_tasks WHERE id=?", (task_id,)).fetchone()[0] == 0
    assert conn.execute("SELECT count(*) FROM download_task_items").fetchone()[0] == 0
    assert conn.execute("SELECT count(*) FROM servers").fetchone()[0] == 0
    assert conn.execute("SELECT count(*) FROM user_server_access").fetchone()[0] == 0
    conn.close()
    assert not db_path.exists()
    assert not task_root.exists()

    dd.finalize_delete_if_requested(99999)


def test_sync_portal_users(downloader):
    db_path = _build_analytics_db(downloader)
    task_id = make_task(downloader)
    dd.sync_portal_users("10", str(db_path), task_id)

    conn = dd.db()
    user = conn.execute("SELECT * FROM portal_users WHERE user_id='a1'").fetchone()
    presence = conn.execute("SELECT * FROM user_server_presence WHERE user_id='a1'").fetchone()
    conn.close()
    assert (user["username"], user["nickname"]) == ("one", "One")
    assert presence["server_id"] == "10"


def test_find_dce_requires_bundled_exporter(monkeypatch, tmp_path):
    monkeypatch.setattr(dd, "BASE_DIR", tmp_path)
    with pytest.raises(FileNotFoundError, match="DiscordChatExporter"):
        dd.find_dce()


def test_find_dce_returns_executable(monkeypatch, tmp_path):
    monkeypatch.setattr(dd, "BASE_DIR", tmp_path)
    exporter = tmp_path / "DiscordChatExporter.Cli.linux-x64" / "DiscordChatExporter.Cli"
    exporter.parent.mkdir(parents=True)
    exporter.write_text("#!/bin/sh\n", encoding="utf-8")
    assert dd.find_dce() == str(exporter)
