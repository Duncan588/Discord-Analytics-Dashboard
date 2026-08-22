"""Preparation_Before_Use/discordDB.py：JSON -> SQLite 导入器。"""
import json
import sqlite3

import pytest

from Preparation_Before_Use import discordDB


def write_json(tmp_path, payload, name="export.json"):
    path = tmp_path / name
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return str(path)


def message(msg_id="m1", author_id="a1", **overrides):
    msg = {
        "id": msg_id,
        "timestamp": "2026-01-01T10:00:00+00:00",
        "content": "hello",
        "author": {
            "id": author_id,
            "name": "author",
            "nickname": "Author",
            "avatarUrl": "http://avatar",
            "isBot": False,
        },
    }
    msg.update(overrides)
    return msg


FLAT_EXPORT = {
    "guild": {"id": "500", "name": "Guild Name", "iconUrl": "http://icon"},
    "channel": {"id": "c1", "name": "general", "categoryId": "cat1"},
    "messages": [message()],
}

THREAD_EXPORT = {
    "guild": {"id": "600", "name": "Threaded", "icon_url": "http://icon2"},
    "threads": [
        {
            "channel": {"id": "t1", "name": "Thread One", "categoryId": "cat"},
            "exportedAt": "2026-01-02T00:00:00+00:00",
            "messages": [message("m1"), message("m2", author_id="a2")],
        },
        {"channel": {"id": "", "name": "broken"}, "messages": [message("m3")]},
    ],
}


def test_connect_creates_parent_directory(tmp_path):
    conn = discordDB.connect(str(tmp_path / "nested" / "dir" / "data.db"))
    assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    conn.close()


def test_inspect_json_flat_export(tmp_path):
    meta = discordDB.inspect_json(write_json(tmp_path, FLAT_EXPORT))
    assert meta == {
        "server_id": "500",
        "server_name": "Guild Name",
        "icon_url": "http://icon",
        "format": "messages",
    }


def test_inspect_json_thread_export(tmp_path):
    meta = discordDB.inspect_json(write_json(tmp_path, THREAD_EXPORT))
    assert meta["server_id"] == "600"
    assert meta["format"] == "threads"
    assert meta["icon_url"] == "http://icon2"


def test_inspect_json_falls_back_to_channel_guild_id(tmp_path):
    payload = {"channel": {"id": "c", "guildId": "777"}, "messages": []}
    meta = discordDB.inspect_json(write_json(tmp_path, payload))
    assert meta["server_id"] == "777"
    assert meta["server_name"] == "Imported Discord Server"


def test_inspect_json_without_guild_information(tmp_path):
    meta = discordDB.inspect_json(write_json(tmp_path, {"messages": []}))
    assert meta["server_id"] == "unknown"


def test_inspect_json_pure_python_fallback(tmp_path, monkeypatch):
    """没有安装 ijson 时退回 json.load 分支。"""
    monkeypatch.setattr(discordDB, "ijson", None)
    meta = discordDB.inspect_json(write_json(tmp_path, THREAD_EXPORT))
    assert meta["server_id"] == "600"
    assert meta["format"] == "threads"


def test_has_key(tmp_path):
    path = write_json(tmp_path, {"threads": [], "guild": {"id": "1"}})
    assert discordDB._has_key(path, "threads") is True
    assert discordDB._has_key(path, "guild") is True
    assert discordDB._has_key(path, "missing") is False


def test_iter_messages_without_ijson(tmp_path, monkeypatch):
    monkeypatch.setattr(discordDB, "ijson", None)
    flat = list(discordDB._iter_messages(write_json(tmp_path, FLAT_EXPORT), "messages"))
    assert flat[0][0]["id"] == "c1"
    assert len(flat[0][1]) == 1

    threaded = list(discordDB._iter_messages(write_json(tmp_path, THREAD_EXPORT, "t.json"), "threads"))
    assert [item[0].get("id") for item in threaded] == ["t1", ""]


def test_import_flat_export(tmp_path):
    db = str(tmp_path / "out.db")
    result = discordDB.import_json_to_db(write_json(tmp_path, FLAT_EXPORT), db)

    assert result == {
        "server_id": "500",
        "server_name": "Guild Name",
        "icon_url": "http://icon",
        "messages": 1,
        "threads": 1,
    }
    conn = discordDB.connect(db)
    conn.row_factory = sqlite3.Row
    thread = conn.execute("SELECT * FROM threads").fetchone()
    assert (thread["thread_id"], thread["name"], thread["guild_id"]) == ("c1", "general", "500")
    user = conn.execute("SELECT * FROM users").fetchone()
    assert (user["username"], user["nickname"], user["is_bot"]) == ("author", "Author", 0)
    stats = conn.execute("SELECT * FROM user_stats WHERE user_id='a1'").fetchone()
    assert stats["msg_count"] == 1
    assert stats["first_msg_at"] == stats["last_msg_at"] == "2026-01-01T10:00:00+00:00"
    conn.close()


def test_import_thread_export_skips_threads_without_id(tmp_path):
    db = str(tmp_path / "out.db")
    result = discordDB.import_json_to_db(write_json(tmp_path, THREAD_EXPORT), db)
    assert (result["threads"], result["messages"]) == (1, 2)
    conn = discordDB.connect(db)
    assert conn.execute("SELECT count(*) FROM messages").fetchone()[0] == 2
    assert conn.execute("SELECT exported_at FROM threads").fetchone()[0] == "2026-01-02T00:00:00+00:00"
    conn.close()


def test_import_normalizes_nested_message_data(tmp_path):
    payload = {
        "guild": {"id": "700", "name": "G"},
        "channel": {"id": "c1", "name": "general"},
        "messages": [
            message(
                "m1",
                reference={"messageId": "m0"},
                attachments=[
                    {"url": "http://a", "fileName": "a.png", "fileSizeBytes": 10},
                    {"url": "http://b", "filename": "b.png", "size_bytes": 20},
                ],
                reactions=[
                    {
                        "emoji": {"name": "ok", "imageUrl": "http://emoji"},
                        "users": [
                            {"id": "r1", "username": "reactor", "globalName": "R"},
                            {"id": "", "username": "ignored"},
                        ],
                    },
                    {"emoji": {"name": "no-users"}, "users": []},
                ],
                mentions=[{"id": "men1", "name": "mentioned"}, {"id": ""}],
            ),
            {"author": {"id": "a1"}},  # 没有 id 的消息被忽略
            {"message_id": "m2", "author": {"id": "a1", "name": "author", "bot": True}},
        ],
    }
    db = str(tmp_path / "out.db")
    result = discordDB.import_json_to_db(write_json(tmp_path, payload), db)
    assert result["messages"] == 2

    conn = discordDB.connect(db)
    conn.row_factory = sqlite3.Row
    assert conn.execute("SELECT reply_to_msg_id FROM messages WHERE message_id='m1'").fetchone()[0] == "m0"
    assert {r["filename"] for r in conn.execute("SELECT filename FROM attachments")} == {"a.png", "b.png"}
    reaction = conn.execute("SELECT * FROM reactions").fetchone()
    assert (reaction["user_id"], reaction["emoji_name"], reaction["emoji_url"]) == ("r1", "ok", "http://emoji")
    assert conn.execute("SELECT nickname FROM users WHERE user_id='r1'").fetchone()[0] == "R"
    mention = conn.execute("SELECT * FROM mentions").fetchone()
    assert (mention["mentioned_user_id"], mention["author_id"]) == ("men1", "a1")
    # 作者信息以 messages 中的 author 为准，后出现的 bot 标记会覆盖同一用户。
    assert conn.execute("SELECT is_bot FROM users WHERE user_id='a1'").fetchone()[0] == 1
    assert conn.execute("SELECT count(*) FROM reactions WHERE emoji_name='no-users'").fetchone()[0] == 0
    conn.close()


def test_import_replaces_analytics_but_keeps_website_data(tmp_path):
    db = str(tmp_path / "out.db")
    discordDB.import_json_to_db(write_json(tmp_path, FLAT_EXPORT), db)
    conn = discordDB.connect(db)
    conn.execute("INSERT INTO user_merges(target_id,parent_id,created_at) VALUES('x','a1','t')")
    conn.execute("INSERT INTO messages(message_id,thread_id,author_id) VALUES('stale','c1','a1')")
    conn.commit()
    conn.close()

    discordDB.import_json_to_db(write_json(tmp_path, FLAT_EXPORT), db)

    conn = discordDB.connect(db)
    assert conn.execute("SELECT count(*) FROM messages").fetchone()[0] == 1
    assert conn.execute("SELECT count(*) FROM user_merges").fetchone()[0] == 1
    conn.close()


def test_import_incremental_keeps_existing_rows(tmp_path):
    db = str(tmp_path / "out.db")
    discordDB.import_json_to_db(write_json(tmp_path, FLAT_EXPORT), db)
    second = {
        "guild": {"id": "500", "name": "Guild Name"},
        "channel": {"id": "c2", "name": "other"},
        "messages": [message("m9", author_id="a9")],
    }
    discordDB.import_json_incremental(write_json(tmp_path, second, "second.json"), db)

    conn = discordDB.connect(db)
    assert conn.execute("SELECT count(*) FROM messages").fetchone()[0] == 2
    # rebuild_stats=False，统计表保持上一次导入的结果。
    assert conn.execute("SELECT count(*) FROM user_stats").fetchone()[0] == 1
    conn.close()


def test_import_uses_channel_id_fallback_for_thread_id(tmp_path):
    payload = {"guild": {"id": "800", "name": "G"}, "channel": {}, "messages": [message()]}
    db = str(tmp_path / "out.db")
    discordDB.import_json_to_db(write_json(tmp_path, payload), db)
    conn = discordDB.connect(db)
    assert conn.execute("SELECT thread_id,name FROM threads").fetchone() == ("imported-channel", "Imported Messages")
    conn.close()


def test_import_accepts_explicit_server_id(tmp_path):
    payload = {"messages": [message()], "channel": {"id": "c1"}}
    db = str(tmp_path / "out.db")
    result = discordDB.import_json_to_db(write_json(tmp_path, payload), db, server_id="999")
    assert result["server_id"] == "999"


def test_import_requires_server_id(tmp_path):
    with pytest.raises(ValueError, match="guild.id"):
        discordDB.import_json_to_db(write_json(tmp_path, {"messages": []}), str(tmp_path / "out.db"))


def test_import_requires_ijson(tmp_path, monkeypatch):
    monkeypatch.setattr(discordDB, "ijson", None)
    with pytest.raises(RuntimeError, match="ijson"):
        discordDB.import_json_to_db(write_json(tmp_path, FLAT_EXPORT), str(tmp_path / "out.db"))


def test_small_batch_size_flushes_repeatedly(tmp_path):
    payload = {
        "guild": {"id": "900", "name": "G"},
        "channel": {"id": "c1"},
        "messages": [message(f"m{i}", author_id=f"a{i}") for i in range(5)],
    }
    db = str(tmp_path / "out.db")
    result = discordDB.import_json_to_db(write_json(tmp_path, payload), db, batch_size=1)
    assert result["messages"] == 5
    conn = discordDB.connect(db)
    assert conn.execute("SELECT count(*) FROM users").fetchone()[0] == 5
    conn.close()


def test_rebuild_user_stats_counts_received_reactions(tmp_path):
    db = str(tmp_path / "stats.db")
    conn = discordDB.connect(db)
    cur = conn.cursor()
    discordDB.create_tables(cur)
    cur.execute("INSERT INTO messages(message_id,thread_id,author_id,timestamp) VALUES('m1','t','a1','2026-01-01')")
    cur.execute("INSERT INTO messages(message_id,thread_id,author_id,timestamp) VALUES('m2','t','a1','2026-01-03')")
    cur.execute("INSERT INTO messages(message_id,thread_id,author_id,timestamp) VALUES('m3','t','','2026-01-03')")
    cur.executemany(
        "INSERT INTO reactions(message_id,user_id,emoji_name,emoji_url) VALUES(?,?,?,?)",
        [("m1", "b", "x", "u"), ("m2", "c", "x", "u")],
    )
    conn.commit()
    conn.close()

    discordDB.rebuild_user_stats(db)

    conn = discordDB.connect(db)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM user_stats").fetchall()
    assert len(rows) == 1
    assert (rows[0]["user_id"], rows[0]["msg_count"], rows[0]["reaction_received_count"]) == ("a1", 2, 2)
    assert (rows[0]["first_msg_at"], rows[0]["last_msg_at"]) == ("2026-01-01", "2026-01-03")
    conn.close()


def test_main_prints_import_result(tmp_path, monkeypatch, capsys):
    path = write_json(tmp_path, FLAT_EXPORT)
    db = str(tmp_path / "cli.db")
    monkeypatch.setattr("sys.argv", ["discordDB.py", path, "--db", db])
    discordDB.main()
    assert json.loads(capsys.readouterr().out)["messages"] == 1


def test_main_requires_existing_file(tmp_path, monkeypatch):
    monkeypatch.setattr("sys.argv", ["discordDB.py", str(tmp_path / "missing.json")])
    with pytest.raises(SystemExit, match="找不到 JSON"):
        discordDB.main()
