import secrets
import sqlite3
from datetime import datetime, timedelta, timezone

from shared import liveness as lv

NOW = datetime(2026, 10, 13, 12, 0, tzinfo=timezone.utc)
G = 915249444721668096


def conn():
    c = sqlite3.connect(":memory:"); c.row_factory = sqlite3.Row
    lv.ensure(c)
    return c


def test_enqueue_skips_bots_and_open_checks():
    c = conn()
    n, sk = lv.enqueue(c, G, [{"user_id": "1"}, {"user_id": "2", "is_bot": True}], "admin", NOW)
    assert n == 1 and sk == [("2", "bot")]
    n, sk = lv.enqueue(c, G, [{"user_id": "1"}], "admin", NOW)
    assert n == 0 and sk == [("1", "pending")]
    row = c.execute("SELECT * FROM liveness_checks").fetchone()
    assert row["status"] == "queued" and len(row["token"]) == 24


def test_lifecycle_pending_expired_alive():
    c = conn()
    lv.enqueue(c, G, [{"user_id": "1"}], "a", NOW)
    cid = c.execute("SELECT id FROM liveness_checks").fetchone()[0]
    lv.mark_sent(c, cid, NOW)
    r = c.execute("SELECT * FROM liveness_checks").fetchone()
    assert r["status"] == "pending" and r["deadline"].startswith("2026-10-20")
    assert lv.due_for_expiry(c, NOW + timedelta(days=6)) == []
    due = lv.due_for_expiry(c, NOW + timedelta(days=7, minutes=1))
    assert [d["id"] for d in due] == [cid]
    lv.mark_expired(c, cid, True, None, NOW + timedelta(days=7))
    assert lv.mark_alive(c, "1", G, role_removed=True, now=NOW + timedelta(days=9)) == 1
    r = c.execute("SELECT * FROM liveness_checks").fetchone()
    assert r["status"] == "alive" and r["role_added_at"] and r["role_removed_at"] and r["clicked_at"]
    # a new check is allowed again after it was resolved
    assert lv.enqueue(c, G, [{"user_id": "1"}], "a", NOW)[0] == 1


def test_dm_failed_is_not_open():
    c = conn()
    lv.enqueue(c, G, [{"user_id": "1"}], "a", NOW)
    lv.mark_failed(c, 1, "DM closed")
    assert c.execute("SELECT status,error FROM liveness_checks").fetchone()[:] == ("dm_failed", "DM closed")
    assert lv.enqueue(c, G, [{"user_id": "1"}], "a", NOW)[0] == 1


def test_dm_text_and_months():
    assert lv.months_since("2026-03-05T10:00:00.000+00:00", NOW) == 7
    t = lv.dm_text("2026-03-05T10:00:00.000+00:00", "抽象派", NOW)
    assert "7 个月" in t and "2026-03-05" in t and "我还在" in t
    assert "几乎没有发言" in lv.dm_text(None, "抽象派", NOW)


def _csrf(client):
    with client.session_transaction() as s:
        tok = s.get("_csrf_token") or secrets.token_urlsafe(32)
        s["_csrf_token"] = tok
    return tok


def test_admin_liveness_page_and_send(client, monkeypatch):
    html = client.get("/admin/liveness").get_data(as_text=True)
    assert "活跃确认" in html and "lv-all" in html
    # not the configured guild -> refused
    r = client.post("/admin/liveness/send", data={"user_ids": ["1"], "csrf_token": _csrf(client)})
    assert r.status_code == 302
    from tests.conftest import DEMO_GUILD
    monkeypatch.setattr(lv, "LIVENESS_GUILD_ID", int(DEMO_GUILD))
    from dixo.routes import admin as admin_routes
    fake = [{"user_id": "111", "name": "a", "tag": "faded", "msg_count": 80, "last_message_at": "2026-01-02T00:00:00+00:00",
             "months_idle": 9, "is_bot": False},
            {"user_id": "222", "name": "b", "tag": "rare", "msg_count": 1, "last_message_at": None,
             "months_idle": None, "is_bot": False}]
    monkeypatch.setattr(admin_routes, "_liveness_candidates", lambda sid, as_of=None, tags=("faded", "rare"): (fake, "2026-10"))
    html = client.get("/admin/liveness?tag=all").get_data(as_text=True)
    import re
    ids = re.findall(r'name="user_ids" value="(\d+)"', html)
    assert ids == ["111", "222"]
    r = client.post("/admin/liveness/send", data={"user_ids": ids + ["123"], "csrf_token": _csrf(client)})
    assert r.status_code == 302
    portal = client.dashboard.app.config.get("PORTAL_DB")
    from dixo import core
    c = sqlite3.connect(core.PORTAL_DB)
    try:
        got = {r[0] for r in c.execute("SELECT user_id FROM liveness_checks WHERE guild_id=? AND status='queued'", (DEMO_GUILD,))}
        assert set(ids) <= got and "123" not in got   # arbitrary ids never queued
        c.execute("DELETE FROM liveness_checks WHERE guild_id=?", (DEMO_GUILD,)); c.commit()
    finally:
        c.close()
