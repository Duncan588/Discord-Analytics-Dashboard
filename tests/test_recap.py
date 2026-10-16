import json
import sqlite3
from datetime import date

from dixo import recap


def test_parse_period_kinds():
    t = date(2026, 10, 16)
    y = recap.parse_period("year", "2025", t)
    assert (y["start"], y["end"], y["prev_start"], y["open"]) == (date(2025, 1, 1), date(2026, 1, 1), date(2024, 1, 1), False)
    m = recap.parse_period("month", "2026-01", t)
    assert m["start"] == date(2026, 1, 1) and m["end"] == date(2026, 2, 1) and m["prev_start"] == date(2025, 12, 1)
    w = recap.parse_period("week", "2026-W42", t)
    assert w["start"] == date(2026, 10, 12) and w["end"] == date(2026, 10, 19) and w["open"]
    a = recap.parse_period("all", None, t)
    assert a["start"] is None and a["label"] == "全部"
    assert recap.parse_period("year", "bogus", t)["value"] == "2026"


def test_utc_boundary_is_shanghai_midnight():
    assert recap._utc(date(2026, 1, 1)) == "2025-12-31T16:00:00"


def test_ego_network_only_own_edges(monkeypatch):
    monkeypatch.setattr(recap.blacklist, "ids", lambda force=False: frozenset({"3"}))
    ic = sqlite3.connect(":memory:")
    ic.execute("CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT)")
    ic.execute("INSERT INTO meta VALUES('network',?)", (json.dumps({"edges": [["1", "2", 10], ["3", "1", 50], ["2", "4", 99], ["5", "1", 4]]}),))
    c = sqlite3.connect(":memory:")
    c.execute("CREATE TABLE users(user_id TEXT, username TEXT, nickname TEXT, avatar_url TEXT)")
    c.executemany("INSERT INTO users VALUES(?,?,?,?)", [(str(i), f"u{i}", None, None) for i in range(1, 6)])
    d = recap.ego_network(ic, c, ["1"])
    assert d["center"]["user_id"] == "1"
    assert [p["user_id"] for p in d["partners"]] == ["2", "5"]  # 3 is blacklisted, 2-4 edge is not ours
