"""Fast 'hot' messages path (summary.msg_react) must match the exact correlated query."""
import sqlite3


def test_hot_fast_path_matches_exact(client, demo_db, monkeypatch):
    from dixo.routes import pages
    sid = "900000000000000001"
    conn = sqlite3.connect(demo_db); conn.row_factory = sqlite3.Row
    uid = conn.execute("SELECT author_id FROM messages GROUP BY author_id ORDER BY count(*) DESC LIMIT 1").fetchone()[0]
    exact = conn.execute(
        "SELECT m.message_id,(SELECT count(*) FROM reactions WHERE message_id=m.message_id) c FROM messages m "
        "WHERE m.author_id=? ORDER BY c DESC LIMIT 10", (uid,)).fetchall()
    monkeypatch.setattr(pages, "HOT_FAST_MIN_MSGS", 0)
    with client.application.app_context():
        fast = pages._hot_message_ids(sid, conn, [str(uid)], 10, 0)
    assert fast is not None
    counts = dict(conn.execute("SELECT message_id,count(*) FROM reactions GROUP BY message_id").fetchall())
    got = [counts.get(i, 0) for i in fast]
    want = [r["c"] for r in exact if r["c"] > 0]
    assert got == want[:len(got)] and len(got) == len(want[:10])


def test_profile_messages_hot_renders(client):
    import sqlite3 as s3
    r = client.get("/user/900000000000000104?tab=messages&sort=hot&page=1")
    assert r.status_code == 200
