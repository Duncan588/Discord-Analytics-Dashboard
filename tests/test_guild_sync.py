"""Guild sync supervisor: adopt old tasks, retry with backoff from checkpoint, never die."""
import importlib
import sqlite3
from datetime import datetime, timedelta, timezone


def _load(tmp_path, monkeypatch):
    db = tmp_path / "portal.db"
    sqlite3.connect(db).executescript(
        "CREATE TABLE download_bots(id INTEGER PRIMARY KEY, owner_user_id TEXT, name TEXT, token TEXT, created_at TEXT);"
        "CREATE TABLE servers(server_id TEXT PRIMARY KEY);")
    monkeypatch.setenv("PORTAL_DB", str(db))
    import Preparation_Before_Use.discord_downloader as d
    d = importlib.reload(d)
    monkeypatch.setattr(d, "PORTAL_DB", db)
    monkeypatch.setattr(d, "SERVER_DATA_DIR", tmp_path / "servers")
    sent = []
    monkeypatch.setattr(d, "_sync_notify", lambda s, t: sent.append(t))
    c = d.db(); d.ensure_schema(c); d.ensure_sync_schema(c)
    c.execute("INSERT INTO download_configs(id,server_id,owner_user_id,guild_id,forum_channel_id,enabled,updated_at) VALUES(1,'g','u','g','f',1,'x')")
    for i, st, cur in [(1, "completed", None), (2, "failed", "2025-11-11T06:03:19+00:00")]:
        c.execute("INSERT INTO download_tasks(id,guild_id,forum_channel_id,created_by,status,created_at,scan_cursor,mode,config_id) VALUES(?,?,?,?,?,?,?,?,1)",
                  (i, "g", "f", "u", st, "x", cur, "update"))
    c.commit(); c.close()
    return d, sent


def q(d, sql, *a):
    c = d.db(); r = c.execute(sql, a).fetchone(); c.close(); return r


def test_adopt_retry_and_complete(tmp_path, monkeypatch):
    d, sent = _load(tmp_path, monkeypatch)
    d.supervise_syncs(set())
    s = q(d, "SELECT * FROM guild_sync")
    assert s["current_run_id"] == 2 and s["run_kind"] == "backfill"
    assert q(d, "SELECT archived FROM download_tasks WHERE id=1")[0] == 1
    run = q(d, "SELECT status,scan_cursor,sync_id FROM download_tasks WHERE id=2")
    assert run["status"] == "pending" and run["scan_cursor"].startswith("2025-11-11") and run["sync_id"] == s["id"]
    # run fails -> retrying with backoff, cursor preserved
    c = d.db(); c.execute("UPDATE download_tasks SET status='failed',error='ConnectionResetError' WHERE id=2"); c.commit(); c.close()
    d.supervise_syncs(set())
    s = q(d, "SELECT * FROM guild_sync")
    assert s["state"] == "retrying" and s["retry_count"] == 1 and s["next_retry_at"]
    # time passes -> requeued from checkpoint
    c = d.db(); c.execute("UPDATE guild_sync SET next_retry_at=?", ((datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat(),)); c.commit(); c.close()
    d.supervise_syncs(set())
    assert q(d, "SELECT status FROM download_tasks WHERE id=2")[0] == "pending"
    assert q(d, "SELECT scan_cursor FROM download_tasks WHERE id=2")[0].startswith("2025-11-11")
    assert not sent  # transient: no notification
    # completes -> backfill done, live, notify once
    c = d.db(); c.execute("UPDATE download_tasks SET status='completed' WHERE id=2"); c.commit(); c.close()
    d.supervise_syncs(set())
    s = q(d, "SELECT * FROM guild_sync")
    assert s["backfill_done"] == 1 and s["state"] == "live" and s["retry_count"] == 0 and len(sent) == 1
    # live pass due -> new hidden run with stop point
    c = d.db(); c.execute("UPDATE guild_sync SET next_live_at=?", ((datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat(),)); c.commit(); c.close()
    d.supervise_syncs(set())
    s = q(d, "SELECT * FROM guild_sync")
    run = q(d, "SELECT * FROM download_tasks WHERE id=?", s["current_run_id"])
    assert s["run_kind"] == "live" and run["sync_id"] == s["id"] and run["live_stop_at"] and run["notified_at"]


def test_persistent_failure_notifies_once(tmp_path, monkeypatch):
    d, sent = _load(tmp_path, monkeypatch)
    d.supervise_syncs(set())
    old = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    future = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
    c = d.db(); c.execute("UPDATE download_tasks SET status='failed' WHERE id=2")
    c.execute("UPDATE guild_sync SET state='retrying',failing_since=?,next_retry_at=?,retry_count=5", (old, future)); c.commit(); c.close()
    d.supervise_syncs(set()); d.supervise_syncs(set())
    assert len(sent) == 1


def test_pause_and_notify_policy(tmp_path, monkeypatch):
    d, _ = _load(tmp_path, monkeypatch)
    d.supervise_syncs(set())
    c = d.db(); c.execute("UPDATE guild_sync SET paused=1"); c.commit(); c.close()
    d.supervise_syncs(set())
    assert q(d, "SELECT status FROM download_tasks WHERE id=2")[0] == "paused"
    from shared.notify_policy import should_notify_finish
    assert should_notify_finish({"sync_id": 1, "mode": "update", "status": "failed"}) is False
