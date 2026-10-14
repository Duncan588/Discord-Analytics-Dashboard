import csv
import gzip
import os
import sqlite3

from dixo import sentiment as ds


def test_clean_text():
    assert ds.clean_text("https://x.y/z") is None
    assert ds.clean_text("<@123> 好") is None
    assert ds.clean_text("<:pepe_sad:123> 太难了") == "pepe_sad 太难了"


def test_import_series_and_rebuild(tmp_path):
    f = tmp_path / "s.csv.gz"
    with gzip.open(f, "wt", newline="") as fh:
        w = csv.writer(fh); w.writerow(["id", "author_id", "day", "label", "p_neg", "p_pos"])
        w.writerow(["100", "a", "2026-01-01", "pos", "0.1", "0.8"])
        w.writerow(["101", "a", "2026-01-01", "neg", "0.9", "0.05"])
        w.writerow(["102", "b", "2026-01-02", "neu", "0.2", "0.2"])
    db = str(tmp_path / "sentiment.db")
    ds.cli(["import", str(f), "--guild", "1", "--db", db])
    c = ds.connect(db)
    g = ds.series(c)
    assert [x["n"] for x in g] == [2, 1] and g[0]["pos_pct"] == 50.0
    assert abs(g[0]["mean"] - (0.7 - 0.85) / 2) < 1e-6
    assert ds.series(c, ["b"])[0]["neu"] == 1
    assert ds.series(c, granularity="month")[0]["n"] == 3
    assert ds.meta(c, "max_id") == "102"
    # incremental insert is idempotent
    assert ds.insert_scores(c, [("102", "b", "2026-01-02", 0, .2, .2), ("103", "b", "2026-01-02", 1, 0, 1)]) == 1
    assert ds.series(c, ["b"])[0]["n"] == 2 and ds.meta(c, "max_id") == "103"


def test_dashboard_and_scorer_api(client, demo_db, monkeypatch):
    sp = ds.path_for(demo_db)
    for p in (sp, sp + "-wal", sp + "-shm"):
        if os.path.exists(p):
            os.remove(p)
    r = client.get("/api/v1/sentiment").get_json()
    assert r["ok"] and r["data"]["ready"] is False
    gid = os.path.basename(os.path.dirname(demo_db))
    assert client.get(f"/api/sentiment/v1/guilds/{gid}/pending").status_code == 404  # disabled without tokens
    monkeypatch.setenv("SENTIMENT_API_TOKENS", "tok")
    h = {"Authorization": "Bearer tok"}
    assert client.get(f"/api/sentiment/v1/guilds/{gid}/pending").status_code == 401
    assert client.get(f"/api/sentiment/v1/guilds/{gid}/pending", headers=h).status_code == 409  # no baseline
    m = sqlite3.connect(demo_db)
    ids = [r[0] for r in m.execute("SELECT message_id FROM messages ORDER BY CAST(message_id AS INTEGER) LIMIT 50")]
    m.close()
    try:
        j = client.get(f"/api/sentiment/v1/guilds/{gid}/pending?after_id={int(ids[0]) - 1}&limit=20", headers=h).get_json()
        items = j["data"]["items"]
        assert items and items[0]["id"] == ids[0]
        sc = [{"id": x["id"], "label": "pos", "p_neg": 0.1, "p_pos": 0.8} for x in items if x["text"]]
        r = client.post(f"/api/sentiment/v1/guilds/{gid}/scores", json={"scores": sc, "cursor": j["data"]["cursor"]}, headers=h).get_json()
        assert r["ok"] and r["inserted"] == len(sc) and r["max_id"] == j["data"]["cursor"]
        d = client.get("/api/v1/sentiment").get_json()["data"]
        assert d["ready"] and sum(x["n"] for x in d["daily"]) == len(sc)
        nxt = client.get(f"/api/sentiment/v1/guilds/{gid}/pending?limit=5", headers=h).get_json()["data"]
        assert nxt["after_id"] == j["data"]["cursor"]
        assert client.get("/", follow_redirects=True).status_code == 200
    finally:
        for p in (sp, sp + "-wal", sp + "-shm"):
            if os.path.exists(p):
                os.remove(p)
