import os
import shutil
import sqlite3
import subprocess
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
DEMO_GUILD = "900000000000000001"


@pytest.fixture(scope="session")
def demo_db():
    """Fake-data server DB (generate_fake_data.py, fixed seed). Local data only."""
    path = os.path.join(ROOT, "data", "servers", DEMO_GUILD, "discord_data.db")
    if not os.path.exists(path):
        subprocess.check_call([sys.executable, os.path.join(ROOT, "generate_fake_data.py")], cwd=ROOT)
    return path


@pytest.fixture()
def tmp_copy(demo_db, tmp_path):
    """Writable copy of the demo DB so tests can insert/delete freely."""
    dst = tmp_path / "servers" / DEMO_GUILD
    dst.mkdir(parents=True)
    src = sqlite3.connect(demo_db)
    out = sqlite3.connect(str(dst / "discord_data.db"))
    src.backup(out)
    src.close(); out.close()
    return str(dst / "discord_data.db")


_summary_ready = False

@pytest.fixture
def client(demo_db):
    global _summary_ready
    if not os.path.exists(os.path.join(ROOT, ".env")):
        shutil.copy(os.path.join(ROOT, ".env.example"), os.path.join(ROOT, ".env"))
    import app as dashboard
    from dixo import summary
    if not _summary_ready:
        summary.refresh(demo_db, budget_seconds=0)
        _summary_ready = True
    c = dashboard.app.test_client()
    admin = sorted(dashboard.ADMIN_IDS)[0] if dashboard.ADMIN_IDS else "891196284998930522"
    with c.session_transaction() as s:
        s["user"] = {"id": admin, "username": "admin", "avatar": None}
        s["server_id"] = DEMO_GUILD
    c.dashboard = dashboard
    return c
