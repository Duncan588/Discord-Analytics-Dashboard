"""R2 restore helpers: confirm gate, companion matching, dry-run without wiping DBs."""
import json
from pathlib import Path

import pytest


def test_start_restore_requires_confirm(monkeypatch, tmp_path):
    from dixo import r2_backup as r2

    monkeypatch.setattr(r2, "load_env", lambda: {"RESTIC_REPOSITORY": "s3:https://x.r2.cloudflarestorage.com/b", "AWS_ACCESS_KEY_ID": "a", "AWS_SECRET_ACCESS_KEY": "b"})
    monkeypatch.setattr(r2, "read_restore_status", lambda: {"running": False})
    monkeypatch.setattr(r2, "systemd_timer_status", lambda: {"service_active": "inactive"})
    monkeypatch.setattr(r2, "_restore_script", lambda: tmp_path / "missing.sh")
    # wrong confirm
    bad = r2.start_restore("abc123", confirm="yes", dry_run=False)
    assert bad["ok"] is False and "确认恢复" in bad["error"]


def test_find_companions_window(monkeypatch):
    from dixo import r2_backup as r2

    snaps = {
        "ok": True,
        "snapshots": [
            {"id": "small1", "time": "2026-10-24T04:18:40Z", "tags": ["small-dbs"]},
            {"id": "data1", "time": "2026-10-24T04:18:51Z", "tags": ["discord-data"]},
            {"id": "data_old", "time": "2026-10-20T04:18:51Z", "tags": ["discord-data"]},
        ],
        "count": 3,
    }
    monkeypatch.setattr(r2, "restic_snapshots", lambda env, limit=40: snaps)
    out = r2.find_companion_data_snapshots("small1", env={"RESTIC_REPOSITORY": "x"})
    assert out["ok"] is True
    ids = [c["id"] for c in out["companions"]]
    assert ids == ["data1"]


def test_dry_run_lists_without_popen(monkeypatch):
    from dixo import r2_backup as r2

    monkeypatch.setattr(r2, "load_env", lambda: {"RESTIC_REPOSITORY": "s3:https://x.r2.cloudflarestorage.com/b"})
    monkeypatch.setattr(
        r2,
        "find_companion_data_snapshots",
        lambda sid, env=None, window_sec=1800: {
            "ok": True,
            "target": {"id": sid, "tags": ["small-dbs"], "time": "2026-10-24T04:18:40Z"},
            "companions": [{"id": "d1", "tags": ["discord-data"], "time": "2026-10-24T04:18:51Z"}],
        },
    )

    calls = []

    def fake_run(cmd, env=None, timeout=60):
        calls.append(cmd)
        if cmd[:2] == ["restic", "ls"]:
            return 0, "/var/tmp/db-backup-stage/portal.db\n", ""
        return 1, "", "unexpected"

    monkeypatch.setattr(r2, "_run", fake_run)
    out = r2.dry_run_restore("small1", include_data=True)
    assert out["ok"] is True
    assert any(c[:2] == ["restic", "ls"] for c in calls)
    assert "portal.db" in "\n".join(out["files_sample"])


def test_start_restore_dry_detached(monkeypatch, tmp_path):
    from dixo import r2_backup as r2

    script = tmp_path / "db-restore.sh"
    script.write_text("#!/bin/bash\necho ok\n")
    script.chmod(0o755)
    status = tmp_path / "status.json"
    envp = tmp_path / "env"
    monkeypatch.setattr(r2, "RESTORE_STATUS_PATH", status)
    monkeypatch.setattr(r2, "RESTORE_ENV_PATH", envp)
    monkeypatch.setattr(r2, "RESTORE_LOG_PATH", tmp_path / "restore.log")
    monkeypatch.setattr(r2, "load_env", lambda: {"RESTIC_REPOSITORY": "s3:https://x.r2.cloudflarestorage.com/b", "AWS_ACCESS_KEY_ID": "a", "AWS_SECRET_ACCESS_KEY": "b"})
    monkeypatch.setattr(r2, "read_restore_status", lambda: {"running": False})
    monkeypatch.setattr(r2, "systemd_timer_status", lambda: {"service_active": "inactive"})
    monkeypatch.setattr(r2, "_restore_script", lambda: script)
    monkeypatch.setattr(
        r2,
        "find_companion_data_snapshots",
        lambda sid, env=None, window_sec=1800: {"ok": True, "target": {"id": sid, "tags": ["small-dbs"]}, "companions": []},
    )

    class FakePopen:
        def __init__(self, *a, **k):
            self.pid = 4242

    monkeypatch.setattr(r2.subprocess, "Popen", FakePopen)
    out = r2.start_restore("small1", confirm="", dry_run=True, requested_by="tester")
    assert out["ok"] is True and out["dry_run"] is True
    assert "RESTORE_DRY_RUN=1" in envp.read_text(encoding="utf-8")
    assert json.loads(status.read_text(encoding="utf-8"))["phase"] == "queued"
