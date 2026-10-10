import os

import pytest

PAGES = ["/", "/api/leaderboard", "/api/leaderboard?page=2", "/search?q=nova", "/admin", "/servers",
         "/api/v1/summary", "/api/v1/timeseries", "/api/v1/heatmap", "/api/v1/wordcloud", "/api/v1/leaderboard",
         "/api/v1/top-threads", "/api/v1/hot-messages", "/api/v1/status", "/api/v1/analytics", "/privacy", "/terms"]


@pytest.mark.parametrize("url", PAGES)
def test_pages_ok(client, url):
    r = client.get(url)
    assert r.status_code == 200, (url, r.status_code)


def test_profile_pages(client):
    uid = "900000000000000101"
    for url in (f"/user/{uid}", f"/user/{uid}?tab=messages", f"/user/{uid}?tab=threads&sort=hot", f"/api/v1/users/{uid}/profile"):
        assert client.get(url).status_code == 200, url


def test_visit_does_not_touch_main_db(client, demo_db):
    before = os.stat(demo_db).st_mtime_ns
    client.get("/"); client.get("/user/900000000000000101")
    assert os.stat(demo_db).st_mtime_ns == before
    assert os.path.exists(os.path.join(os.path.dirname(demo_db), "visitors.db"))


def test_etag_304(client):
    r = client.get("/api/v1/summary")
    r2 = client.get("/api/v1/summary", headers={"If-None-Match": r.headers["ETag"]})
    assert r2.status_code == 304


def test_ai_api_disabled_without_tokens(client, monkeypatch):
    monkeypatch.setenv("AI_API_TOKENS", "")
    assert client.get("/api/ai/v1/guilds").status_code == 404


def test_ai_api_auth_and_endpoints(client, monkeypatch, demo_db):
    monkeypatch.setenv("AI_API_TOKENS", "testtoken123")
    assert client.get("/api/ai/v1/guilds").status_code == 401
    assert client.get("/api/ai/v1/guilds", headers={"Authorization": "Bearer wrong"}).status_code == 401
    h = {"Authorization": "Bearer testtoken123"}
    gid = "900000000000000001"
    assert gid in [g["server_id"] for g in client.get("/api/ai/v1/guilds", headers=h).get_json()["data"]]
    assert client.get(f"/api/ai/v1/guilds/{gid}/members?limit=5", headers=h).get_json()["data"]
    m = client.get(f"/api/ai/v1/guilds/{gid}/members/900000000000000101/messages?limit=3", headers=h).get_json()["data"]
    assert len(m["items"]) == 3 and m["next_before_rowid"]
    m2 = client.get(f"/api/ai/v1/guilds/{gid}/members/900000000000000101/messages?limit=3&before_rowid={m['next_before_rowid']}", headers=h).get_json()["data"]
    assert m2["items"][0]["cursor"] < m["items"][-1]["cursor"]
    assert client.get(f"/api/ai/v1/guilds/../members", headers=h).status_code == 404
    assert client.get(f"/api/ai/v1/guilds/{gid}/members/1/markdown/..%2F..%2F.env", headers=h).status_code == 404
    # write methods are not routed
    assert client.post(f"/api/ai/v1/guilds", headers=h).status_code in (400, 405)


def test_search_fts(client, monkeypatch, demo_db):
    from dixo import search
    search.refresh(demo_db, budget_seconds=0)
    monkeypatch.setenv("AI_API_TOKENS", "testtoken123")
    h = {"Authorization": "Bearer testtoken123"}
    import sqlite3
    word = sqlite3.connect(demo_db).execute("SELECT content FROM messages WHERE length(content)>20 LIMIT 1").fetchone()[0].split()[1]
    r = client.get(f"/api/ai/v1/guilds/900000000000000001/search?q={word}", headers=h).get_json()
    assert r["indexed"] and r["data"] and word.lower() in r["data"][0]["content"].lower()


def test_public_auth_pages_when_logged_out(client):
    with client.session_transaction() as s:
        s.clear()
    for url in ("/welcome", "/login", "/privacy", "/terms"):
        assert client.get(url).status_code == 200, url
