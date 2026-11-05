"""Fair lottery: eligibility, opt-in, SystemRandom draw, audit hash, guild commands.

Manual check on the production guild after restarting v20:
1. /抽奖 with a title, prize, a Beijing time a few minutes ahead, and winner count 1.
   The public panel shows 奖品 / 中奖人数 / 开奖时间 / 发起人 / 当前参与 0 / 条件.
2. An account with no history clicks 参与 and gets the eligibility failure. Count stays 0.
3. An eligible account joins, sees the count go to 1, joins again (already in), leaves, rejoins.
4. Someone who is not the creator clicks 提前开奖 and 取消抽奖 and is refused. There is no pause button.
5. The creator confirms 提前开奖. Too few people leaves the lottery open; enough people draws.
6. A second lottery is left until its time; the panel flips to the result on its own.
7. /抽奖结果 with that id shows the same 名单校验 hash and winners. A bad id says 没有找到. A cancelled id says 已取消.
8. Restart the bot; the old panel buttons still join and leave.
"""
import hashlib
import os
import sqlite3
import sys
from datetime import datetime, timedelta, timezone

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from shared import lottery

NOW = datetime(2026, 11, 5, 4, 0, tzinfo=timezone.utc)  # 12:00 Beijing
GUILD = "915249444721668096"
CREATOR = "100"
OTHER = "200"


def _db(tmp_path):
    return str(tmp_path / "lottery.db")


def _servers(tmp_path):
    return str(tmp_path / "servers")


def _write_main(tmp_path, messages, users=(), merges=()):
    path = os.path.join(_servers(tmp_path), GUILD, "discord_data.db")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if os.path.exists(path):
        os.remove(path)
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE messages(message_id TEXT, author_id TEXT, timestamp TEXT)")
    conn.execute("CREATE TABLE users(user_id TEXT, is_bot INTEGER)")
    conn.execute("CREATE TABLE user_merges(target_id TEXT, parent_id TEXT)")
    conn.executemany("INSERT INTO messages VALUES(?,?,?)", messages)
    conn.executemany("INSERT INTO users VALUES(?,?)", users)
    conn.executemany("INSERT INTO user_merges VALUES(?,?)", merges)
    conn.commit()
    conn.close()
    return path


def _write_summary(tmp_path, stats, days):
    path = os.path.join(_servers(tmp_path), GUILD, "summary.db")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE user_stats(user_id TEXT PRIMARY KEY, msg_count INTEGER)")
    conn.execute("CREATE TABLE user_day(user_id TEXT, day TEXT, c INTEGER)")
    conn.executemany("INSERT INTO user_stats VALUES(?,?)", stats)
    conn.executemany("INSERT INTO user_day VALUES(?,?,?)", days)
    conn.commit()
    conn.close()
    return path


def _open(tmp_path, creator=CREATOR, winners=1, when=None, title="周末福利"):
    when = when or (NOW + timedelta(hours=5))
    return lottery.create_lottery(
        _db(tmp_path), GUILD, "555", creator, "host", title, "贴纸一套", winners, when, NOW,
    )


def test_constants_and_copy():
    assert lottery.RECENT_DAYS == 30
    assert lottery.MIN_HISTORY == 1000
    assert lottery.RULE == "近 30 天内有发言，或历史消息数 ≥ 1000"
    assert "1000" in lottery.ELIGIBILITY_FAIL and "近 30 天" in lottery.ELIGIBILITY_FAIL
    assert (lottery.BTN_JOIN, lottery.BTN_LEAVE, lottery.BTN_DRAW, lottery.BTN_CANCEL) == ("参与", "退出", "提前开奖", "取消抽奖")
    assert "暂停" not in lottery.OPEN_DESC
    assert lottery.FIELD_HASH == "名单校验"


def test_hash_is_numeric_order_without_trailing_newline():
    digest = lottery.participants_hash(["100", "20", "3", "20"])
    assert digest == hashlib.sha256("3\n20\n100".encode("utf-8")).hexdigest()
    assert digest == lottery.participants_hash(["3", "100", "20"])


def test_pick_winners_uses_system_random_on_sorted_pool(monkeypatch):
    seen = {}

    class Spy(lottery.secrets.SystemRandom):
        def sample(self, population, k):
            seen["population"] = list(population)
            seen["k"] = k
            seen["cls"] = type(self)
            return list(population)[:k]

    monkeypatch.setattr(lottery.secrets, "SystemRandom", Spy)
    winners = lottery.pick_winners(["100", "20", "3", "100"], 2)
    assert seen["population"] == ["3", "20", "100"]
    assert seen["k"] == 2
    assert issubclass(seen["cls"], lottery.secrets.SystemRandom)
    assert winners == ["3", "20"]


def test_real_system_random_is_without_replacement():
    pool = ["10", "20", "30", "40", "50"]
    for _ in range(40):
        winners = lottery.pick_winners(pool, 2)
        assert len(winners) == len(set(winners)) == 2
        assert set(winners) <= set(pool)


def test_parse_beijing_time_and_reject_past():
    parsed = lottery.parse_draw_at("2026-11-06 21：00", NOW)
    assert lottery.utc_stamp(parsed) == "2026-11-06T13:00:00+00:00"
    assert lottery.format_when(lottery.utc_stamp(parsed)).startswith("2026-11-06 21:00（北京时间）")
    zulu = lottery.parse_draw_at("2026-11-06T13:00:00Z", NOW)
    assert lottery.utc_stamp(zulu) == "2026-11-06T13:00:00+00:00"
    unix = lottery.parse_draw_at(str(int(parsed.timestamp())), NOW)
    assert lottery.utc_stamp(unix) == "2026-11-06T13:00:00+00:00"
    with pytest.raises(lottery.TimeParseError, match="晚于现在"):
        lottery.parse_draw_at("2026-11-05 11:00", NOW)
    with pytest.raises(lottery.TimeParseError, match="无法识别"):
        lottery.parse_draw_at("下周", NOW)
    with pytest.raises(lottery.TimeParseError, match="366"):
        lottery.parse_draw_at("2028-11-06 21:00", NOW)


def test_create_does_not_enroll_creator_and_has_no_cap(tmp_path):
    first = _open(tmp_path)
    second = _open(tmp_path, title="第二场")
    assert first["id"] != second["id"]
    assert first["participant_ids"] == []
    assert first["participant_count"] == 0
    assert first["creator_id"] == CREATOR
    assert lottery.load(_db(tmp_path), first["id"])["status"] == "open"
    panel = lottery.panel_payload(first)
    assert [name for name, _value, _inline in panel["fields"]] == ["奖品", "中奖人数", "开奖时间", "发起人", "当前参与", "条件"]
    assert panel["fields"][4][1] == "0 人"
    assert "北京时间" in panel["fields"][2][1]
    assert "点「参与」加入" in panel["description"]
    lottery.set_message(_db(tmp_path), first["id"], "999")
    assert lottery.load(_db(tmp_path), first["id"])["message_id"] == "999"
    ghost = _open(tmp_path, title="没发出去")
    lottery.abort_unposted(_db(tmp_path), ghost["id"], NOW)
    assert lottery.load(_db(tmp_path), ghost["id"])["status"] == "cancelled"
    lottery.abort_unposted(_db(tmp_path), first["id"], NOW)
    assert lottery.load(_db(tmp_path), first["id"])["status"] == "open"
    with pytest.raises(lottery.LotteryError):
        _open(tmp_path, title="   ")
    with pytest.raises(lottery.LotteryError):
        lottery.create_lottery(_db(tmp_path), GUILD, "555", CREATOR, "h", "标题", "奖", 0, NOW + timedelta(hours=1), NOW)


def test_eligibility_recent_history_merge_and_summary_lag(tmp_path, monkeypatch):
    monkeypatch.setattr(lottery, "MIN_HISTORY", 3)
    old = "2020-01-01T00:00:00+00:00"
    recent = "2026-11-01T00:00:00+00:00"
    _write_main(tmp_path, [
        ("1", "10", old),
        ("2", "10", old),
        ("3", "11", recent),
        ("4", "12", old),
        ("5", "12", old),
        ("6", "12", old),
        ("7", "30", recent),
    ], users=[("99", 1)], merges=[("30", "40")])
    servers = _servers(tmp_path)
    assert lottery.eligibility(servers, GUILD, "11", NOW).ok  # spoke within 30 days
    assert lottery.eligibility(servers, GUILD, "11", NOW).reason == "recent"
    assert not lottery.eligibility(servers, GUILD, "10", NOW).ok  # two old messages
    assert lottery.eligibility(servers, GUILD, "12", NOW).reason == "history"
    assert lottery.eligibility(servers, GUILD, "40", NOW).ok  # merged alt spoke recently
    assert lottery.eligibility(servers, GUILD, "99", NOW).reason == "bot"
    assert lottery.eligibility(servers, GUILD, "404", NOW).reason == "neither"
    # different guild has no database
    assert lottery.eligibility(servers, "1", "11", NOW).reason == "no_db"

    # summary says inactive, live log has a new message
    _write_summary(tmp_path, [("50", 1)], [("50", "2020-01-01", 1)])
    _write_main(tmp_path, [("9", "50", recent)])
    # rewrite would wipe merges; write_main replaces the file. Re-add the recent row only.
    assert lottery.eligibility(servers, GUILD, "50", NOW).reason == "recent"

    # summary alone is enough when the raw log is absent
    only = tmp_path / "summary-only" / "servers"
    gid = only / GUILD
    gid.mkdir(parents=True)
    conn = sqlite3.connect(gid / "summary.db")
    conn.execute("CREATE TABLE user_stats(user_id TEXT, msg_count INTEGER)")
    conn.execute("CREATE TABLE user_day(user_id TEXT, day TEXT, c INTEGER)")
    conn.execute("INSERT INTO user_stats VALUES('7', 3)")
    conn.commit()
    conn.close()
    assert lottery.eligibility(str(only), GUILD, "7", NOW).reason == "history"

    broken = tmp_path / "broken" / "servers" / GUILD
    broken.mkdir(parents=True)
    (broken / "summary.db").write_text("not a database", encoding="utf-8")
    assert lottery.eligibility(str(tmp_path / "broken" / "servers"), GUILD, "7", NOW).reason == "unavailable"


def test_millisecond_timestamps_match_exporter_format(tmp_path):
    # discord_data timestamps look like 2026-11-04T08:00:00.894+00:00.
    # NOW is 2026-11-05 04:00Z, so the 30-day cutoff is 2026-10-06 04:00Z.
    _write_main(tmp_path, [
        ("1", "60", "2026-10-06T04:00:00.001+00:00"),
        ("2", "61", "2026-10-06T03:59:59.999+00:00"),
    ])
    servers = _servers(tmp_path)
    assert lottery.eligibility(servers, GUILD, "60", NOW).reason == "recent"
    assert lottery.eligibility(servers, GUILD, "61", NOW).reason == "neither"


def test_join_leave_eligibility_and_opt_in(tmp_path):
    snap = _open(tmp_path)
    lid = snap["id"]
    db, servers = _db(tmp_path), _servers(tmp_path)
    missing = lottery.handle_join(db, servers, lid, "11", "neo", False, NOW)
    assert missing["code"] == "no_db" and missing["refresh"] is False
    assert lottery.load(db, lid)["participant_count"] == 0

    _write_main(tmp_path, [("1", "11", "2026-11-04T00:00:00+00:00"), ("2", "12", "2020-01-01T00:00:00+00:00")])
    blocked = lottery.handle_join(db, servers, lid, "12", "old", False, NOW)
    assert blocked["code"] == "ineligible"
    assert lottery.ELIGIBILITY_FAIL in blocked["text"]
    assert lottery.load(db, lid)["participant_ids"] == []

    bot = lottery.handle_join(db, servers, lid, "11", "bot", True, NOW)
    assert bot["code"] == "bot" and lottery.BOT_REJECT in bot["text"]
    assert lottery.load(db, lid)["participant_count"] == 0

    joined = lottery.handle_join(db, servers, lid, "11", "neo", False, NOW)
    assert joined["code"] == "joined" and joined["text"] == "已加入抽奖。当前参与：1 人。"
    again = lottery.handle_join(db, servers, lid, "11", "neo", False, NOW)
    assert again["code"] == "already" and "已经在参与名单里" in again["text"]
    assert lottery.load(db, lid)["participant_count"] == 1

    absent = lottery.handle_leave(db, lid, "12")
    assert absent["code"] == "absent" and absent["text"] == lottery.NOT_IN
    left = lottery.handle_leave(db, lid, "11")
    assert left["code"] == "left" and left["text"] == "已退出抽奖。当前参与：0 人。"
    assert lottery.load(db, lid)["participant_ids"] == []


def test_draw_cancel_permissions_and_audit(tmp_path):
    snap = _open(tmp_path, winners=2)
    lid = snap["id"]
    db = _db(tmp_path)
    _write_main(tmp_path, [
        ("1", "11", "2026-11-04T00:00:00+00:00"),
        ("2", "12", "2026-11-04T00:00:00+00:00"),
        ("3", "13", "2026-11-04T00:00:00+00:00"),
    ])
    servers = _servers(tmp_path)
    for uid in ("11", "12", "13"):
        assert lottery.handle_join(db, servers, lid, uid, uid, False, NOW)["code"] == "joined"

    class Boom:
        def sample(self, population, k):
            raise AssertionError("must not draw")

    denied = lottery.commit_draw(db, lid, "early", OTHER, NOW, Boom())
    assert denied["code"] == "forbidden" and denied["text"] == lottery.ONLY_DRAW
    assert denied["refresh"] is False
    assert lottery.load(db, lid)["status"] == "open"

    early_short = _open(tmp_path, winners=2, title="人不够")
    one = lottery.handle_join(db, servers, early_short["id"], "11", "11", False, NOW)
    assert one["code"] == "joined"
    stayed = lottery.commit_draw(db, early_short["id"], "early", CREATOR, NOW, Boom())
    assert stayed["code"] == "insufficient" and stayed["changed"] is False
    assert "抽奖仍会继续" in stayed["text"]
    assert lottery.load(db, early_short["id"])["status"] == "open"

    preview = lottery.preview_action(db, lid, OTHER, "cancel")
    assert preview["confirm"] is False and preview["text"] == lottery.ONLY_CANCEL
    preview_ok = lottery.preview_action(db, lid, CREATOR, "draw")
    assert preview_ok["confirm"] is True and "确认现在提前开奖" in preview_ok["text"]

    drawn = lottery.commit_draw(db, lid, "early", CREATOR, NOW)
    assert drawn["code"] == "drawn" and drawn["changed"] is True
    assert drawn["text"] == lottery.DRAWN_EARLY_OK
    assert len(drawn["winners"]) == len(set(drawn["winners"])) == 2
    assert set(drawn["winners"]) <= {"11", "12", "13"}
    report = lottery.audit(db, lid)
    assert report["ok"] is True
    assert report["stored_hash"] == report["hash"] == lottery.participants_hash(["11", "12", "13"])
    panel = lottery.panel_payload(lottery.load(db, lid))
    names = [name for name, _value, _inline in panel["fields"]]
    assert names[:6] == ["奖品", "中奖人数", "开奖时间", "发起人", "当前参与", "条件"]
    assert "中奖名单" in names and "名单校验" in names and "开奖方式" in names
    assert f"`{report['hash']}`" in [value for _n, value, _i in panel["fields"]]
    assert panel["fields"][5][1] == lottery.RULE
    assert "发起人提前开奖" in [value for _n, value, _i in panel["fields"]]
    assert "抽奖ID：" in panel["footer"] and "SHA-256" in panel["footer"]

    again = lottery.commit_draw(db, lid, "early", CREATOR, NOW)
    assert again["code"] == "closed" and lottery.load(db, lid)["winners"] == drawn["winners"]
    joined_late = lottery.handle_join(db, servers, lid, "11", "11", False, NOW)
    assert joined_late["code"] == "closed" and "已经开奖" in joined_late["text"]

    other = _open(tmp_path, winners=1, title="取消这场")
    oid = other["id"]
    lottery.handle_join(db, servers, oid, "11", "11", False, NOW)
    cancelled = lottery.commit_cancel(db, oid, CREATOR, NOW)
    assert cancelled["code"] == "cancelled" and cancelled["text"] == lottery.CANCELLED_OK
    assert lottery.load(db, oid)["status"] == "cancelled"
    assert "已取消" in lottery.panel_payload(lottery.load(db, oid))["description"]
    assert lottery.handle_join(db, servers, oid, "12", "12", False, NOW)["code"] == "closed"
    looked = lottery.lookup(db, oid, GUILD)
    assert "已取消" in looked["text"]
    assert lottery.lookup(db, oid, "999")["text"] == lottery.not_found_text(oid)
    assert lottery.lookup(db, 99999, GUILD)["text"] == lottery.not_found_text(99999)


def test_auto_draw_insufficient_and_due(tmp_path):
    db = _db(tmp_path)
    past = _open(tmp_path, winners=2, when=NOW - timedelta(minutes=1), title="过点")
    # create_lottery does not reject a past time; the scheduler does.
    future = _open(tmp_path, winners=1, when=NOW + timedelta(days=1), title="以后")
    assert lottery.due_ids(db, NOW) == [past["id"]]
    _write_main(tmp_path, [("1", "11", "2026-11-04T00:00:00+00:00")])
    lottery.handle_join(db, _servers(tmp_path), past["id"], "11", "11", False, NOW)
    closed = lottery.commit_draw(db, past["id"], "auto", None, NOW)
    assert closed["code"] == "insufficient" and closed["changed"] is True
    assert "抽奖仍会继续" not in closed["text"]
    snap = lottery.load(db, past["id"])
    assert snap["status"] == "insufficient" and snap["winners"] == []
    assert lottery.audit(db, past["id"])["ok"] is True
    assert "未能开奖" in lottery.panel_payload(snap)["description"]
    assert lottery.due_ids(db, NOW) == []
    assert lottery.load(db, future["id"])["status"] == "open"
    banner = lottery.lookup(db, past["id"], GUILD)["text"]
    assert "参与人数不足" in banner and "至少 2" in banner and "实际 1" in banner


def test_full_auto_draw_matches_hash(tmp_path):
    db = _db(tmp_path)
    snap = _open(tmp_path, winners=1, when=NOW - timedelta(seconds=5), title="到点")
    _write_main(tmp_path, [
        ("1", "11", "2026-11-04T00:00:00+00:00"),
        ("2", "12", "2026-11-04T00:00:00+00:00"),
    ])
    servers = _servers(tmp_path)
    lottery.handle_join(db, servers, snap["id"], "12", "b", False, NOW)
    lottery.handle_join(db, servers, snap["id"], "11", "a", False, NOW)
    result = lottery.commit_draw(db, snap["id"], "auto", None, NOW)
    assert result["code"] == "drawn" and result["winners"] in (["11"], ["12"])
    assert lottery.audit(db, snap["id"])["ok"] is True
    mode = [value for name, value, _i in lottery.panel_payload(lottery.load(db, snap["id"]))["fields"] if name == "开奖方式"]
    assert mode == ["到点自动开奖"]
    assert "尚未开奖" not in lottery.lookup(db, snap["id"], GUILD)["text"]
    assert "开奖结果" in lottery.lookup(db, snap["id"], GUILD)["text"]


def test_buttons_and_prod_guild_commands():
    sys.path.insert(0, os.path.join(ROOT, "Preparation_Before_Use"))
    from Preparation_Before_Use.lottery_ui import LotteryButton
    import whitelist_bot

    for action in ("join", "leave", "draw", "cancel"):
        button = LotteryButton(42, action)
        match = button.template.fullmatch(button.custom_id)
        assert match.group("action") == action and match.group("lid") == "42"
        assert button.item.label == {"join": "参与", "leave": "退出", "draw": "提前开奖", "cancel": "取消抽奖"}[action]
    assert LotteryButton(1, "join").template.fullmatch("lottery:draw_yes:1") is None

    guild = __import__("discord").Object(id=whitelist_bot.lottery_ui.prod_guild_id())
    names = sorted(cmd.name for cmd in whitelist_bot.bot.tree.get_commands(guild=guild))
    assert names == ["抽奖", "抽奖结果"]
    assert whitelist_bot.bot.tree.get_command("抽奖") is None
    created = whitelist_bot.bot.tree.get_command("抽奖", guild=guild)
    payload = created.to_dict(whitelist_bot.bot.tree)
    assert [opt["name"] for opt in payload["options"]] == ["标题", "奖品内容", "开奖时间", "中奖人数"]
    assert all(opt["required"] for opt in payload["options"])
    assert payload["dm_permission"] is False
    result = whitelist_bot.bot.tree.get_command("抽奖结果", guild=guild).to_dict(whitelist_bot.bot.tree)
    assert [opt["name"] for opt in result["options"]] == ["抽奖id"]
    assert whitelist_bot.lottery_ui.prod_guild_id() == 915249444721668096
