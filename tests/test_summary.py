import sqlite3

from dixo import dashboard_data, summary


def _truth(path):
    c = sqlite3.connect(path)
    t = {
        "msgs": c.execute("SELECT count(*) FROM messages").fetchone()[0],
        "reacts": c.execute("SELECT count(*) FROM reactions").fetchone()[0],
        "per_user": dict(c.execute("SELECT author_id,count(*) FROM messages GROUP BY author_id")),
        "per_thread": dict(c.execute("SELECT thread_id,count(*) FROM messages GROUP BY thread_id")),
        "recv": dict(c.execute("SELECT m.author_id,count(*) FROM reactions r JOIN messages m ON m.message_id=r.message_id GROUP BY m.author_id")),
        "hot": c.execute("SELECT message_id,count(*) c FROM reactions GROUP BY message_id ORDER BY c DESC,message_id LIMIT 1").fetchone(),
    }
    c.close()
    return t


def _summ(path):
    return summary.connect_summary(summary.summary_path(path))


def test_full_build_matches_sql_ground_truth(tmp_copy):
    res = summary.refresh(tmp_copy, budget_seconds=0)
    assert res["caught_up"]
    t = _truth(tmp_copy); s = _summ(tmp_copy)
    assert s.execute("SELECT SUM(msg_count) FROM user_stats").fetchone()[0] == t["msgs"]
    assert s.execute("SELECT SUM(c) FROM msg_react").fetchone()[0] == t["reacts"]
    assert dict(s.execute("SELECT user_id,msg_count FROM user_stats WHERE msg_count>0").fetchall()) == t["per_user"]
    assert dict(s.execute("SELECT thread_id,msg_count FROM thread_stats").fetchall()) == t["per_thread"]
    assert dict(s.execute("SELECT user_id,react_recv FROM user_stats WHERE react_recv>0").fetchall()) == t["recv"]
    top = s.execute("SELECT message_id,c FROM msg_react ORDER BY c DESC,message_id LIMIT 1").fetchone()
    assert tuple(top) == tuple(t["hot"])


def test_thread_op_is_first_message(tmp_copy):
    summary.refresh(tmp_copy, budget_seconds=0)
    s = _summ(tmp_copy); c = sqlite3.connect(tmp_copy)
    for tid, op in s.execute("SELECT thread_id,op_msg_id FROM thread_stats LIMIT 50"):
        first = c.execute("SELECT message_id FROM messages WHERE thread_id=? ORDER BY timestamp,message_id LIMIT 1", (tid,)).fetchone()[0]
        assert op == first


def test_incremental_equals_full(tmp_copy):
    summary.refresh(tmp_copy, budget_seconds=0)
    c = sqlite3.connect(tmp_copy)
    uid = c.execute("SELECT user_id FROM users LIMIT 1").fetchone()[0]
    tid = c.execute("SELECT thread_id FROM threads LIMIT 1").fetchone()[0]
    c.executemany("INSERT OR IGNORE INTO messages(message_id,thread_id,author_id,content,timestamp) VALUES(?,?,?,?,?)",
                  [(f"t-{i}", tid, uid, "增量 incremental test", "2026-10-01T03:00:00+00:00") for i in range(25)])
    c.execute("INSERT INTO reactions(message_id,user_id,emoji_name,emoji_url) VALUES('t-1',?,'👍','')", (uid,))
    c.commit()
    res = summary.refresh(tmp_copy, budget_seconds=0)
    assert res["processed"] == 26
    t = _truth(tmp_copy); s = _summ(tmp_copy)
    assert dict(s.execute("SELECT user_id,msg_count FROM user_stats WHERE msg_count>0").fetchall()) == t["per_user"]
    # local time bucket: 03:00 UTC -> 11:00 UTC+8
    assert s.execute("SELECT c FROM msg_hour WHERE day='2026-10-01' AND hour=11").fetchone()[0] >= 25


def test_delete_triggers_full_rebuild(tmp_copy):
    summary.refresh(tmp_copy, budget_seconds=0)
    c = sqlite3.connect(tmp_copy)
    victim = c.execute("SELECT author_id FROM messages GROUP BY author_id ORDER BY count(*) DESC LIMIT 1").fetchone()[0]
    c.execute("DELETE FROM messages WHERE author_id=?", (victim,)); c.commit()
    summary.refresh(tmp_copy, budget_seconds=0)
    s = _summ(tmp_copy)
    assert s.execute("SELECT SUM(msg_count) FROM user_stats").fetchone()[0] == _truth(tmp_copy)["msgs"]
    row = s.execute("SELECT msg_count FROM user_stats WHERE user_id=?", (victim,)).fetchone()
    assert row is None or row[0] == 0  # row may remain for reactions the user gave


def test_homepage_payload_shape(tmp_copy):
    summary.refresh(tmp_copy, budget_seconds=0)
    main = sqlite3.connect(tmp_copy); main.row_factory = sqlite3.Row
    data = dashboard_data.homepage(main, _summ(tmp_copy))
    for k in ("total_msgs", "total_threads", "total_users", "chart_daily", "chart_hourly", "heatmap",
              "server_word_cloud", "top_users", "top_threads", "top_hot_msgs", "full_leaderboard"):
        assert k in data, k
    assert len(data["heatmap"]) == 7 and len(data["heatmap"][0]) == 24
    assert sum(map(sum, data["heatmap"])) == data["total_msgs"]
    assert data["top_users"][0]["msg_count"] >= data["top_users"][-1]["msg_count"]
