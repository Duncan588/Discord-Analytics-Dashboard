"""Preparation_Before_Use/whitelist_bot.py 中与 Discord 网关无关的部分。"""
import asyncio
import sqlite3
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from Preparation_Before_Use import whitelist_bot as wb


@pytest.fixture
def bot_db(tmp_path, monkeypatch):
    monkeypatch.setattr(wb, "DB", str(tmp_path / "portal.db"))
    monkeypatch.setattr(wb, "ADMINS", {"admin1"})
    conn = wb.db()
    conn.execute(
        "CREATE TABLE IF NOT EXISTS download_configs(id INTEGER PRIMARY KEY AUTOINCREMENT,guild_id TEXT,"
        "owner_user_id TEXT,enabled INTEGER DEFAULT 1)"
    )
    conn.commit()
    conn.close()
    return wb


def fake_member(member_id="u1", name="user", display_name="User", avatar_url="http://avatar"):
    avatar = SimpleNamespace(url=avatar_url) if avatar_url else None
    return SimpleNamespace(id=member_id, name=name, display_name=display_name, display_avatar=avatar)


def test_load_env_only_fills_missing_keys(tmp_path, monkeypatch):
    monkeypatch.setattr(wb, "BASE_DIR", str(tmp_path))
    (tmp_path / ".env").write_text("WB_TEST_KEY='abc'\n#comment\nnoequals\n", encoding="utf-8")
    monkeypatch.delenv("WB_TEST_KEY", raising=False)
    wb.load_env()
    import os

    assert os.environ["WB_TEST_KEY"] == "abc"
    os.environ.pop("WB_TEST_KEY", None)

    monkeypatch.setattr(wb, "BASE_DIR", str(tmp_path / "empty"))
    wb.load_env()


def test_db_creates_expected_tables(bot_db):
    conn = wb.db()
    tables = {r["name"] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    conn.close()
    assert {
        "whitelist_users",
        "server_download_quota",
        "user_server_access",
        "portal_users",
        "user_server_presence",
        "member_sync_requests",
        "download_tasks",
    } <= tables


def test_admin_and_whitelist_checks(bot_db):
    assert wb.admin("admin1") is True
    assert wb.admin(999) is False
    assert wb.is_whitelist("u1") is False

    conn = wb.db()
    conn.execute("INSERT INTO whitelist_users(user_id,username,added_by,created_at) VALUES('u1','one','a','t')")
    conn.commit()
    conn.close()
    assert wb.is_whitelist("u1") is True


def test_can_sync_requires_access_or_owned_config(bot_db):
    assert wb.can_sync("admin1", "10") is True
    assert wb.can_sync("u1", "10") is False

    conn = wb.db()
    conn.execute("INSERT INTO whitelist_users(user_id,username,added_by,created_at) VALUES('u1','one','a','t')")
    conn.commit()
    conn.close()
    # 白名单但没有该服务器权限时仍然不允许。
    assert wb.can_sync("u1", "10") is False

    conn = wb.db()
    conn.execute("INSERT INTO user_server_access(user_id,server_id,granted_by,created_at) VALUES('u1','10','a','t')")
    conn.commit()
    conn.close()
    assert wb.can_sync("u1", "10") is True

    # 非白名单用户拥有该服务器的下载配置时也允许。
    conn = wb.db()
    conn.execute("INSERT INTO download_configs(guild_id,owner_user_id,enabled) VALUES('20','u2',1)")
    conn.commit()
    conn.close()
    assert wb.can_sync("u2", "20") is True
    assert wb.can_sync("u2", "30") is False


def test_upsert_member_keeps_previous_avatar(bot_db):
    wb.upsert_member("10", fake_member())
    wb.upsert_member("10", fake_member(name="renamed", display_name="Renamed", avatar_url=None))

    conn = wb.db()
    user = conn.execute("SELECT * FROM portal_users WHERE user_id='u1'").fetchone()
    presence = conn.execute("SELECT * FROM user_server_presence WHERE user_id='u1'").fetchall()
    conn.close()
    assert (user["username"], user["nickname"], user["avatar_url"]) == ("renamed", "Renamed", "http://avatar")
    assert len(presence) == 1


def test_sync_guild_members_marks_request_completed(bot_db):
    conn = wb.db()
    cursor = conn.execute(
        "INSERT INTO member_sync_requests(guild_id,requested_by,status,created_at) VALUES('10','admin1','pending','t')"
    )
    request_id = cursor.lastrowid
    conn.commit()
    conn.close()

    class _Guild:
        id = "10"

        def fetch_members(self, limit=None):
            async def gen():
                for i in range(2):
                    yield fake_member(f"u{i}")

            return gen()

    assert asyncio.run(wb.sync_guild_members(_Guild(), request_id)) == 2

    conn = wb.db()
    row = conn.execute("SELECT * FROM member_sync_requests WHERE id=?", (request_id,)).fetchone()
    assert (row["status"], row["error"]) == ("completed", None)
    assert conn.execute("SELECT count(*) FROM portal_users").fetchone()[0] == 2
    conn.close()


def test_sync_guild_members_records_failure(bot_db):
    conn = wb.db()
    request_id = conn.execute(
        "INSERT INTO member_sync_requests(guild_id,requested_by,status,created_at) VALUES('10','admin1','pending','t')"
    ).lastrowid
    conn.commit()
    conn.close()

    class _Guild:
        id = "10"

        def fetch_members(self, limit=None):
            async def gen():
                raise RuntimeError("missing intents")
                yield  # pragma: no cover

            return gen()

    with pytest.raises(RuntimeError):
        asyncio.run(wb.sync_guild_members(_Guild(), request_id))

    conn = wb.db()
    row = conn.execute("SELECT * FROM member_sync_requests WHERE id=?", (request_id,)).fetchone()
    conn.close()
    assert row["status"] == "failed"
    assert "missing intents" in row["error"]


def test_thread_url():
    assert wb.thread_url(1, 2) == "https://discord.com/channels/1/2"


def test_is_private_context():
    assert wb.is_private_context(SimpleNamespace(guild=None)) is False
    assert wb.is_private_context(SimpleNamespace(guild=object())) is True


def test_help_embed_mentions_commands():
    embed = wb.help_embed()
    assert "收藏" in embed.title
    assert "/favorites" in embed.description


@pytest.mark.parametrize(
    "value,expected",
    [
        ("2026-01-02T03:04:05+00:00", datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)),
        ("2026-01-02T03:04:05Z", datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)),
        ("2026-01-02 03:04:05", datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)),
        ("2026-01-02T03:04:05", datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)),
        (datetime(2026, 1, 2, 3, 4, 5), datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)),
        (datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc), datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)),
    ],
)
def test_parse_db_datetime(value, expected):
    assert wb._parse_db_datetime(value) == expected


def test_parse_db_datetime_defaults_to_now():
    parsed = wb._parse_db_datetime(None)
    assert (datetime.now(timezone.utc) - parsed).total_seconds() < 5


def test_favorites_embed_without_rows():
    embed = wb.favorites_embed([], 0, 0, 1)
    assert "还没有收藏" in embed.fields[0].value
    assert embed.footer.text == "第 1 / 1 页"


def test_favorites_embed_lists_rows_with_index_offset():
    rows = [{"guild_id": 1, "thread_id": 2, "created_at": "2026-01-02T03:04:05+00:00"}]
    embed = wb.favorites_embed(rows, 11, 1, 2)
    assert "共 **11**" in embed.description
    assert "**11.** https://discord.com/channels/1/2" in embed.fields[0].value
    assert "2026-01-02 03:04 UTC" in embed.fields[0].value
    assert embed.footer.text == "第 2 / 2 页"


def test_favorites_embed_splits_long_values_into_multiple_fields():
    rows = [
        {"guild_id": 10**18 + i, "thread_id": 10**18 + i, "created_at": "2026-01-02T03:04:05+00:00"}
        for i in range(wb.PER_PAGE)
    ]
    embed = wb.favorites_embed(rows, len(rows), 0, 1)
    assert len(embed.fields) > 1
    assert all(len(field.value) <= 1024 for field in embed.fields)


def test_sqlite_connection_wrapper(tmp_path):
    async def scenario():
        monkey_db = tmp_path / "async.db"
        import Preparation_Before_Use.whitelist_bot as module

        original = module.DB
        module.DB = str(monkey_db)
        try:
            pool = await module.open_database()
        finally:
            module.DB = original
        async with pool.acquire() as conn:
            await conn.execute("CREATE TABLE favorites(user_id TEXT, created_at TEXT, flag INTEGER)")
            result = await conn.execute(
                "INSERT INTO favorites VALUES(?,?,?)",
                "u1",
                datetime(2026, 1, 1, tzinfo=timezone.utc),
                True,
            )
            assert result.rowcount == 1
        async with pool.acquire() as conn:
            assert await conn.fetchval("SELECT count(*) FROM favorites") == 1
            assert await conn.fetchval("SELECT user_id FROM favorites WHERE user_id='missing'") is None
            row = await conn.fetchrow("SELECT * FROM favorites")
            assert (row["user_id"], row["created_at"], row["flag"]) == ("u1", "2026-01-01T00:00:00+00:00", 1)
            assert len(await conn.fetch("SELECT * FROM favorites")) == 1
        # 异常时回滚，写入不会保留。
        with pytest.raises(sqlite3.OperationalError):
            async with pool.acquire() as conn:
                await conn.execute("INSERT INTO favorites VALUES('u2','t',0)")
                await conn.execute("INSERT INTO nope VALUES(1)")
        async with pool.acquire() as conn:
            assert await conn.fetchval("SELECT count(*) FROM favorites") == 1
        await pool.close()

    asyncio.run(scenario())
