"""下载任务与成员同步：只操作门户数据库，不启动下载器子进程。"""
import os

import pytest

from tests.conftest import build_server, login, portal_conn


@pytest.fixture(autouse=True)
def no_child_processes(flask_app, monkeypatch):
    monkeypatch.setattr(flask_app, "ensure_downloader_process", lambda: None)


@pytest.fixture
def config(flask_app, admin_id):
    portal = portal_conn(flask_app)
    portal.execute(
        "INSERT INTO download_configs(id,server_id,owner_user_id,guild_id,forum_channel_id,guild_name,forum_name,"
        "enabled,use_default_bot,scheduler_interval,download_interval_ms,update_enabled,updated_at)"
        " VALUES(1,'10:20',?, '10','20','Guild','Forum',1,1,250,0,0,'t')",
        (admin_id,),
    )
    portal.commit()
    portal.close()
    return 1


def _task_row(flask_app, task_id):
    portal = portal_conn(flask_app)
    row = portal.execute("SELECT * FROM download_tasks WHERE id=?", (task_id,)).fetchone()
    portal.close()
    return row


def test_member_sync_request_is_deduplicated(client, flask_app, admin_id):
    login(client, admin_id)
    assert client.post("/admin/member-sync", data={"guild_id": "abc"}).status_code == 400
    assert client.post("/admin/member-sync", data={"guild_id": "10"}).status_code == 302
    client.post("/admin/member-sync", data={"guild_id": "10"})
    portal = portal_conn(flask_app)
    assert portal.execute("SELECT count(*) FROM member_sync_requests WHERE guild_id='10'").fetchone()[0] == 1
    portal.close()


def test_member_sync_rejects_unmanaged_guild_for_level2(client, flask_app):
    portal = portal_conn(flask_app)
    portal.execute("INSERT INTO whitelist_users(user_id,username,added_by,created_at) VALUES('u2','m','a','t')")
    portal.commit()
    portal.close()
    login(client, "u2")
    assert client.post("/admin/member-sync", data={"guild_id": "10"}).status_code == 403


def test_member_sync_allows_level2_owner_of_config(client, flask_app, config):
    portal = portal_conn(flask_app)
    portal.execute("INSERT INTO whitelist_users(user_id,username,added_by,created_at) VALUES('u2','m','a','t')")
    portal.execute("UPDATE download_configs SET owner_user_id='u2' WHERE id=1")
    portal.commit()
    portal.close()
    login(client, "u2")
    assert client.post("/admin/member-sync", data={"guild_id": "10"}).status_code == 302


def test_download_task_validates_config(client, admin_id):
    login(client, admin_id)
    assert client.post("/admin/download-task", data={"config_id": "x"}).status_code == 400
    assert client.post("/admin/download-task", data={"config_id": "999"}).status_code == 400


def test_download_task_created_in_initial_mode(client, flask_app, config, admin_id):
    login(client, admin_id)
    assert client.post("/admin/download-task", data={"config_id": "1"}).status_code == 302
    portal = portal_conn(flask_app)
    task = portal.execute("SELECT * FROM download_tasks").fetchone()
    sync = portal.execute("SELECT count(*) FROM member_sync_requests").fetchone()[0]
    portal.close()
    assert task["mode"] == "initial"
    assert task["status"] == "pending"
    assert task["guild_name"] == "Guild"
    assert sync == 1


def test_download_task_uses_update_mode_when_data_exists(client, flask_app, config, admin_id):
    build_server(flask_app, "10")
    portal = portal_conn(flask_app)
    portal.execute("UPDATE download_configs SET update_enabled=1, scheduler_interval=2 WHERE id=1")
    portal.commit()
    portal.close()
    login(client, admin_id)
    client.post("/admin/download-task", data={"config_id": "1"})
    task = _task_row(flask_app, 1)
    assert task["mode"] == "update"
    # 小于 50 的旧配置按秒解释，转换成毫秒。
    assert task["scheduler_interval"] == 2000


def test_download_task_quota_for_level2(client, flask_app, config):
    portal = portal_conn(flask_app)
    portal.execute("INSERT INTO whitelist_users(user_id,username,added_by,created_at) VALUES('u2','m','a','t')")
    portal.execute("UPDATE download_configs SET owner_user_id='u2' WHERE id=1")
    portal.commit()
    portal.close()
    login(client, "u2")
    assert client.post("/admin/download-task", data={"config_id": "1"}).status_code == 302
    # 配额默认为 1，第二个不同服务器的任务被拒绝。
    assert client.post("/admin/download-task", data={"config_id": "1"}).status_code == 403


def test_download_task_rejects_other_owners_config_for_level2(client, flask_app, config):
    portal = portal_conn(flask_app)
    portal.execute("INSERT INTO whitelist_users(user_id,username,added_by,created_at) VALUES('u2','m','a','t')")
    portal.commit()
    portal.close()
    login(client, "u2")
    assert client.post("/admin/download-task", data={"config_id": "1"}).status_code == 403


def test_task_pause_resume_cancel(client, flask_app, config, admin_id):
    login(client, admin_id)
    client.post("/admin/download-task", data={"config_id": "1"})

    client.post("/admin/download-task/1/pause")
    assert _task_row(flask_app, 1)["status"] == "paused"

    client.post("/admin/download-task/1/resume")
    assert _task_row(flask_app, 1)["status"] == "pending"

    client.post("/admin/download-task/1/cancel")
    assert _task_row(flask_app, 1)["status"] == "cancelled"

    # 已取消的任务不会被再次暂停。
    client.post("/admin/download-task/1/pause")
    assert _task_row(flask_app, 1)["status"] == "cancelled"

    assert client.post("/admin/download-task/999/cancel").status_code == 403


def test_task_actions_denied_for_unrelated_admin(client, flask_app, config, admin_id):
    login(client, admin_id)
    client.post("/admin/download-task", data={"config_id": "1"})
    portal = portal_conn(flask_app)
    portal.execute("INSERT INTO whitelist_users(user_id,username,added_by,created_at) VALUES('u3','m','a','t')")
    portal.commit()
    portal.close()

    login(client, "u3")
    assert client.post("/admin/download-task/1/cancel").status_code == 403
    response = client.post("/admin/download-task/1/delete")
    assert response.status_code == 302
    assert _task_row(flask_app, 1) is not None


def test_task_delete_removes_threads_and_server_record(client, flask_app, config, admin_id):
    build_server(
        flask_app,
        "10",
        users=[{"user_id": "u1", "username": "one"}],
        messages=[{"message_id": "m1", "thread_id": "t1", "author_id": "u1", "content": "hi"}],
    )
    server_db = flask_app.server_db_path("10")
    conn = flask_app.db_connect(server_db)
    conn.execute("INSERT INTO reactions(message_id,user_id,emoji_name,emoji_url) VALUES('m1','u1','x','u')")
    conn.commit()
    conn.close()

    login(client, admin_id)
    client.post("/admin/download-task", data={"config_id": "1"})
    portal = portal_conn(flask_app)
    portal.execute(
        "INSERT INTO download_task_items(task_id,thread_id,status) VALUES(1,'t1','downloaded')"
    )
    portal.execute("UPDATE download_tasks SET status='running' WHERE id=1")
    portal.commit()
    portal.close()

    assert client.post("/admin/download-task/1/delete").status_code == 302
    portal = portal_conn(flask_app)
    assert portal.execute("SELECT count(*) FROM download_tasks").fetchone()[0] == 0
    assert portal.execute("SELECT count(*) FROM download_task_items").fetchone()[0] == 0
    assert portal.execute("SELECT count(*) FROM servers WHERE server_id='10'").fetchone()[0] == 0
    portal.close()
    # 没有已完成任务时连服务器分析库一起清理。
    assert not os.path.exists(server_db)


def test_task_delete_keeps_threads_shared_with_other_tasks(client, flask_app, config, admin_id):
    build_server(
        flask_app,
        "10",
        messages=[{"message_id": "m1", "thread_id": "t1", "author_id": "u1", "content": "hi"}],
    )
    login(client, admin_id)
    client.post("/admin/download-task", data={"config_id": "1"})
    portal = portal_conn(flask_app)
    portal.execute(
        "INSERT INTO download_tasks(id,guild_id,forum_channel_id,created_by,status,total,completed,created_at)"
        " VALUES(2,'10','20',?, 'completed',0,0,'t')",
        (admin_id,),
    )
    portal.execute(
        "INSERT INTO download_task_items(task_id,thread_id,status) VALUES(1,'t1','downloaded')"
    )
    portal.execute(
        "INSERT INTO download_task_items(task_id,thread_id,status) VALUES(2,'t1','downloaded')"
    )
    portal.commit()
    portal.close()

    client.post("/admin/download-task/1/delete")

    conn = flask_app.db_connect(flask_app.server_db_path("10"))
    assert conn.execute("SELECT count(*) FROM messages WHERE thread_id='t1'").fetchone()[0] == 1
    conn.close()
    portal = portal_conn(flask_app)
    # 仍有已完成任务，服务器记录保留。
    assert portal.execute("SELECT count(*) FROM servers WHERE server_id='10'").fetchone()[0] == 1
    portal.close()


def test_task_delete_missing_task_is_404(client, admin_id):
    login(client, admin_id)
    assert client.post("/admin/download-task/123/delete").status_code == 404


def test_task_status_endpoint_reports_progress(client, flask_app, config, admin_id):
    login(client, admin_id)
    client.post("/admin/download-task", data={"config_id": "1"})
    portal = portal_conn(flask_app)
    portal.execute(
        "UPDATE download_tasks SET status='running', started_at='2020-01-01T00:00:00+00:00' WHERE id=1"
    )
    portal.execute(
        "INSERT INTO download_task_items(task_id,thread_id,status,bot_name)"
        " VALUES(1,'t1','downloaded','A')"
    )
    portal.execute(
        "INSERT INTO download_task_items(task_id,thread_id,status,bot_name)"
        " VALUES(1,'t2','failed','A')"
    )
    portal.execute(
        "INSERT INTO download_task_items(task_id,thread_id,status) VALUES(1,'t3','pending')"
    )
    portal.commit()
    portal.close()

    payload = client.get("/admin/download-tasks/status").get_json()
    assert len(payload) == 1
    task = payload[0]
    assert (task["total_items"], task["downloaded_items"], task["failed_items"], task["pending_items"]) == (3, 1, 1, 1)
    assert task["bot_progress"] == [{"name": "A", "count": 2}]
    assert task["elapsed_seconds"] > 0


def test_task_status_endpoint_hides_other_users_tasks(client, flask_app, config, admin_id):
    login(client, admin_id)
    client.post("/admin/download-task", data={"config_id": "1"})
    portal = portal_conn(flask_app)
    portal.execute("INSERT INTO whitelist_users(user_id,username,added_by,created_at) VALUES('u4','m','a','t')")
    portal.commit()
    portal.close()
    login(client, "u4")
    assert client.get("/admin/download-tasks/status").get_json() == []


def test_delete_task_data_ignores_missing_server_db(flask_app):
    with flask_app.app.test_request_context("/"):
        portal = flask_app.get_portal_db()
        flask_app._delete_task_data_from_server(portal, 1, "no-such-guild")


def test_downloader_process_helpers(flask_app, monkeypatch):
    monkeypatch.setattr(flask_app, "_service_processes", [])
    assert flask_app._downloader_process_alive() is False

    class _Proc:
        def poll(self):
            return None

    monkeypatch.setattr(flask_app, "_service_processes", [("discord_downloader", _Proc())])
    assert flask_app._downloader_process_alive() is True
