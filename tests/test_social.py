import pytest


def _csrf(client):
    import secrets
    with client.session_transaction() as s:
        tok = s.get("_csrf_token") or secrets.token_urlsafe(32)
        s["_csrf_token"] = tok
        return tok


def test_block_unblock_roundtrip(client):
    token = _csrf(client)
    target = "900000000000000102"
    r = client.post("/api/social/block", data={"user_id": target, "csrf_token": token},
                    headers={"X-Requested-With": "XMLHttpRequest"})
    assert r.status_code == 200 and r.get_json()["ok"] is True
    # profile of target shows blocked state for viewer
    html = client.get(f"/user/{target}").get_data(as_text=True)
    assert "/api/social/unblock" in html
    r = client.post("/api/social/unblock", data={"user_id": target, "csrf_token": token},
                    headers={"X-Requested-With": "XMLHttpRequest"})
    assert r.status_code == 200 and r.get_json()["blocked"] is False
    html = client.get(f"/user/{target}").get_data(as_text=True)
    assert "/api/social/block" in html


def test_cannot_block_self(client):
    token = _csrf(client)
    me = "891196284998930522"
    r = client.post("/api/social/block", data={"user_id": me, "csrf_token": token},
                    headers={"X-Requested-With": "XMLHttpRequest"})
    assert r.status_code == 400


def test_favorite_unfavorite_roundtrip(client, demo_db):
    import sqlite3
    tid = sqlite3.connect(demo_db).execute("SELECT thread_id FROM threads LIMIT 1").fetchone()[0]
    token = _csrf(client)
    r = client.post("/api/social/favorite",
                    data={"thread_id": tid, "guild_id": "900000000000000001", "csrf_token": token},
                    headers={"X-Requested-With": "XMLHttpRequest"})
    assert r.status_code == 200 and r.get_json()["favorited"] is True
    me = "891196284998930522"
    html = client.get(f"/user/{me}").get_data(as_text=True)
    assert "/api/social/unfavorite" in html
    r = client.post("/api/social/unfavorite",
                    data={"thread_id": tid, "guild_id": "900000000000000001", "csrf_token": token},
                    headers={"X-Requested-With": "XMLHttpRequest"})
    assert r.status_code == 200 and r.get_json()["favorited"] is False


def test_analytics_api_shape(client):
    r = client.get("/api/v1/analytics")
    assert r.status_code == 200
    data = r.get_json()["data"]
    for k in ("messages_day", "messages_week", "messages_month", "messages_year",
              "active_users_day", "thread_growth_month", "reply_rate_week", "top_emoji", "heatmap"):
        assert k in data, k
    assert isinstance(data["messages_day"], list)
