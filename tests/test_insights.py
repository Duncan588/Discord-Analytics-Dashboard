import os
import sqlite3

from dixo import insights as di


def test_insights_empty_and_ready(client, demo_db):
    p = di.path_for(demo_db)
    if os.path.exists(p):
        os.remove(p)
    assert client.get("/api/v1/insights/topics").get_json()["ready"] is False
    assert client.get("/insights").status_code == 200
    c = sqlite3.connect(p); c.executescript(di.SCHEMA)
    c.execute("INSERT INTO meta VALUES('built_at','x')")
    c.execute("INSERT INTO topics VALUES(0,'游戏','d',1,10,0.1,'[]')"); c.execute("INSERT INTO topic_month VALUES(0,'2026-01',10)")
    c.execute("INSERT INTO memes VALUES('顺飞','term','m','o','u',5,3,'2023-01-01','a','1','t','2','x','2023-01','{}','[]')")
    c.commit(); c.close()
    try:
        d = client.get("/api/v1/insights/topics").get_json()
        assert d["ready"] and d["data"]["topics"][0]["name"] == "游戏"
        assert client.get("/api/v1/insights/memes?q=顺").get_json()["data"][0]["term"] == "顺飞"
        assert client.get("/insights").status_code == 200
        assert client.get("/api/insights/v1/guilds/1/export/users").status_code == 404  # disabled without token
    finally:
        os.remove(p)


def test_nan_month_is_valid_json():
    import sqlite3
    from dixo import insights as I
    c = sqlite3.connect(":memory:"); c.row_factory = sqlite3.Row; c.executescript(I.SCHEMA)
    c.execute("INSERT INTO server_month VALUES('2026-03','{\"sentiment\": {\"mean\": NaN}}','')")
    import json
    assert json.loads(json.dumps(I.months(c), allow_nan=False))[0]["data"]["sentiment"]["mean"] is None
