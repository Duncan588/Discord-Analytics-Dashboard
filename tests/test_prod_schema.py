"""Run summary + social helpers against a DB built from the live production DDL (unslimmed)."""
import os, sqlite3
from dixo import summary, dashboard_data, social

HERE = os.path.dirname(__file__)


def _prod_db(tmp_path, demo_db):
    path = tmp_path / "discord_data.db"
    con = sqlite3.connect(str(path))
    con.executescript(open(os.path.join(HERE, "fixtures", "prod_main_schema.sql"), encoding="utf-8").read())
    con.execute("ATTACH ? AS d", (demo_db,))
    for t in ("users", "threads", "messages", "reactions", "attachments", "mentions"):
        cols = [r[1] for r in con.execute(f"PRAGMA main.table_info({t})")]
        c = ",".join(cols)
        con.execute(f"INSERT INTO main.{t}({c}) SELECT {c} FROM d.{t}")
    con.commit(); con.execute("DETACH d"); con.close()
    return str(path)


def test_summary_on_prod_schema(tmp_path, demo_db):
    db = _prod_db(tmp_path, demo_db)
    summary.refresh(db, budget_seconds=0)
    s = sqlite3.connect(summary.summary_path(db))
    a = dashboard_data.analytics_bundle(s)
    n = sqlite3.connect(db).execute("SELECT COUNT(*) FROM messages").fetchone()[0]
    assert sum(x["c"] for x in a["messages_day"]) == n
    assert a["messages_month"] and a["active_users_day"]


def test_social_on_prod_portal_integer_ids(tmp_path):
    con = sqlite3.connect(str(tmp_path / "portal.db"))
    con.executescript(open(os.path.join(HERE, "fixtures", "prod_portal_schema.sql"), encoding="utf-8").read())
    social.ensure_tables(con)  # must be a no-op on existing prod tables
    ok, _ = social.block_user(con, "891196284998930522", "900000000000000102", "2026-10-11T00:00:00+00:00")
    assert ok and social.is_blocked(con, "891196284998930522", "900000000000000102")
    assert con.execute("SELECT typeof(blocked_id) FROM user_blocks").fetchone()[0] == "integer"
    assert social.unblock_user(con, 891196284998930522, 900000000000000102)
    social.add_favorite(con, "1", "915249444721668096", "1234567890123", "2026-10-11T00:00:00+00:00")
    assert social.is_favorited(con, 1, 1234567890123)
    assert social.remove_favorite(con, "1", "1234567890123", "915249444721668096")


def test_migrate_skips_equivalent_prod_indexes(tmp_path, demo_db, capsys):
    from dixo import migrate
    db = _prod_db(tmp_path, demo_db)
    migrate.migrate_db(db)
    out = capsys.readouterr().out
    assert "idx_msg_author_timestamp exists, skipped" in out
    assert "idx_msg_thread_timestamp exists, skipped" in out
    names = {r[0] for r in sqlite3.connect(db).execute("SELECT name FROM sqlite_master WHERE type='index'")}
    assert "idx_msg_author_ts" not in names and "idx_msg_thread_ts" not in names
    assert "idx_react_msg_emoji" in names


def test_active_users_sql_matches_python(tmp_path, demo_db):
    import collections
    db = _prod_db(tmp_path, demo_db)
    summary.refresh(db, budget_seconds=0)
    s = sqlite3.connect(summary.summary_path(db))
    for grain, bucket in dashboard_data._BUCKETS.items():
        sets = collections.defaultdict(set)
        for day, uid in s.execute("SELECT day,user_id FROM user_day"):
            sets[bucket(day)].add(uid)
        expect = [{"t": k, "c": len(sets[k])} for k in sorted(sets)]
        assert dashboard_data.series_active_users(s, grain) == expect, grain


def test_analytics_bundle_cache_invalidates_on_watermark(tmp_path, demo_db):
    db = _prod_db(tmp_path, demo_db)
    summary.refresh(db, budget_seconds=0)
    s = sqlite3.connect(summary.summary_path(db))
    a = dashboard_data.analytics_bundle(s, cache_key="t")
    assert dashboard_data.analytics_bundle(s, cache_key="t") is a
    s.execute("UPDATE meta SET value=value||'x' WHERE key='msg_wm'"); s.commit()
    assert dashboard_data.analytics_bundle(s, cache_key="t") is not a
