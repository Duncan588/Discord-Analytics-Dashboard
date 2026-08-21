"""Discord JSON -> SQLite importer.

兼容两种输入：
1. DiscordChatExporter 的单频道/合并 JSON，根节点直接包含 messages
2. 本项目的新格式，根节点包含 threads，每个 thread 包含 messages

旧版 merged_final_clean.json 属于第一种格式，旧版脚本只读取 threads.item，
因此会得到空数据库。这一版自动识别两种结构，并从 guild.id 获取服务器 ID。
"""
import argparse
import json
import os
import sqlite3
from datetime import datetime, timezone

try:
    import ijson
except ImportError:
    ijson = None

DEFAULT_DB = "discord_data.db"
BATCH_SIZE = 5000


def connect(path):
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    conn = sqlite3.connect(path, timeout=120)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=120000")
    return conn


def create_tables(cur):
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS users(user_id TEXT PRIMARY KEY,username TEXT,nickname TEXT,avatar_url TEXT,is_bot BOOLEAN);
    CREATE TABLE IF NOT EXISTS threads(thread_id TEXT PRIMARY KEY,category_id TEXT,name TEXT,exported_at TEXT,guild_id TEXT,last_active_at TEXT);
    CREATE TABLE IF NOT EXISTS thread_scan_state(thread_id TEXT PRIMARY KEY,name TEXT,last_active_at TEXT,scanned_at TEXT,guild_id TEXT);
    CREATE TABLE IF NOT EXISTS messages(message_id TEXT PRIMARY KEY,thread_id TEXT,author_id TEXT,content TEXT,timestamp DATETIME,reply_to_msg_id TEXT);
    CREATE TABLE IF NOT EXISTS reactions(id INTEGER PRIMARY KEY AUTOINCREMENT,message_id TEXT,user_id TEXT,emoji_name TEXT,emoji_url TEXT);
    CREATE TABLE IF NOT EXISTS attachments(id INTEGER PRIMARY KEY AUTOINCREMENT,message_id TEXT,url TEXT,filename TEXT,size_bytes INTEGER);
    CREATE TABLE IF NOT EXISTS mentions(id INTEGER PRIMARY KEY AUTOINCREMENT,message_id TEXT,mentioned_user_id TEXT,author_id TEXT);
    CREATE TABLE IF NOT EXISTS user_stats(user_id TEXT PRIMARY KEY,msg_count INTEGER DEFAULT 0,reaction_received_count INTEGER DEFAULT 0,interaction_score INTEGER DEFAULT 0,first_msg_at DATETIME,last_msg_at DATETIME);
    CREATE TABLE IF NOT EXISTS user_merges(target_id TEXT PRIMARY KEY,parent_id TEXT,created_at DATETIME);
    CREATE TABLE IF NOT EXISTS claim_requests_v2(id INTEGER PRIMARY KEY AUTOINCREMENT,requester_id TEXT,target_id TEXT,target_name TEXT,status INTEGER DEFAULT 0,created_at DATETIME,UNIQUE(requester_id,target_id));
    CREATE TABLE IF NOT EXISTS web_visitors(user_id TEXT PRIMARY KEY,username TEXT,nickname TEXT,avatar_url TEXT,last_visit DATETIME);
    CREATE TABLE IF NOT EXISTS profile_views(id INTEGER PRIMARY KEY AUTOINCREMENT,target_user_id TEXT,viewer_user_id TEXT,viewer_name TEXT,viewer_avatar TEXT,timestamp DATETIME,UNIQUE(target_user_id,viewer_user_id));
    CREATE INDEX IF NOT EXISTS idx_msg_author ON messages(author_id);
    CREATE INDEX IF NOT EXISTS idx_msg_timestamp ON messages(timestamp);
    CREATE INDEX IF NOT EXISTS idx_msg_thread ON messages(thread_id);
    CREATE INDEX IF NOT EXISTS idx_msg_author_timestamp ON messages(author_id,timestamp);
    CREATE INDEX IF NOT EXISTS idx_msg_thread_timestamp ON messages(thread_id,timestamp);
    CREATE INDEX IF NOT EXISTS idx_react_user ON reactions(user_id);
    CREATE INDEX IF NOT EXISTS idx_react_msg ON reactions(message_id);
    CREATE INDEX IF NOT EXISTS idx_react_msg_user ON reactions(message_id,user_id);
    CREATE INDEX IF NOT EXISTS idx_mentions_user ON mentions(mentioned_user_id);
    CREATE INDEX IF NOT EXISTS idx_stats_count ON user_stats(msg_count);
    """)


def inspect_json(filename):
    if not ijson:
        with open(filename, encoding="utf-8") as f:
            data = json.load(f)
        guild = data.get("guild") or {}
        channel = data.get("channel") or {}
        return {"server_id": str(guild.get("id") or channel.get("guildId") or channel.get("guild_id") or "unknown"), "server_name": guild.get("name") or "Imported Discord Server", "icon_url": guild.get("iconUrl"), "format": "threads" if data.get("threads") is not None else "messages"}

    # 注意：每个键的提取都必须使用独立的文件句柄。ijson 的迭代器会按内部缓冲区
    # （而非按逻辑读取量）预读文件，如果多次调用 ijson.items() 共享同一个文件对象，
    # 第二次调用会从被预读跳过的、非法的中间位置开始解析，从而抛出
    # "parse error: premature EOF" / "invalid char in json text" 等异常。
    # 这正是"下载好的 JSON 无法导入"报错的根本原因。
    with open(filename, "rb") as f:
        guild = next(ijson.items(f, "guild"), {}) or {}
    with open(filename, "rb") as f:
        channel = next(ijson.items(f, "channel"), {}) or {}
    return {
        "server_id": str(guild.get("id") or channel.get("guildId") or channel.get("guild_id") or "unknown"),
        "server_name": guild.get("name") or "Imported Discord Server",
        "icon_url": guild.get("iconUrl") or guild.get("icon_url"),
        "format": "threads" if _has_key(filename, "threads") else "messages",
    }


def _has_key(filename, key):
    with open(filename, "rb") as f:
        for prefix, event, value in ijson.parse(f):
            if prefix == key and event == "start_array":
                return True
            if prefix == key and event == "start_map":
                return True
    return False


def _iter_messages(datafile, fmt):
    if not ijson:
        data = json.load(open(datafile, encoding="utf-8"))
        if fmt == "threads":
            for thread in data.get("threads", []):
                yield thread.get("channel", {}), thread.get("messages", []), thread
        else:
            yield data.get("channel", {}), data.get("messages", []), None
        return
    if fmt == "threads":
        with open(datafile, "rb") as f:
            for thread in ijson.items(f, "threads.item"):
                yield thread.get("channel", {}), thread.get("messages", []), thread
        return
    # 与 inspect_json 相同的原因：channel 和 messages 不能共享同一个文件句柄读取，
    # 否则第二次解析会从被预读跳过的位置开始，触发 "premature EOF" 解析错误。
    with open(datafile, "rb") as f:
        channel = next(ijson.items(f, "channel"), {}) or {}
    with open(datafile, "rb") as f:
        messages = ijson.items(f, "messages.item")
        yield channel, messages, None


def _insert_message(cur, msg, thread_id, users, attachments, reactions, mentions):
    m_id = str(msg.get("id") or msg.get("message_id") or "")
    if not m_id:
        return False
    auth = msg.get("author") or {}
    author_id = str(auth.get("id") or "")
    if author_id:
        users[author_id] = (author_id, auth.get("name") or auth.get("username") or "", auth.get("nickname") or auth.get("globalName") or "", auth.get("avatarUrl") or auth.get("avatar_url") or "", bool(auth.get("isBot", auth.get("bot", False))))
    ref = msg.get("reference") or {}
    cur.execute("INSERT OR IGNORE INTO messages(message_id,thread_id,author_id,content,timestamp,reply_to_msg_id) VALUES(?,?,?,?,?,?)", (m_id, str(thread_id), author_id, msg.get("content") or "", msg.get("timestamp"), ref.get("messageId") or ref.get("message_id")))
    for att in msg.get("attachments") or []:
        attachments.append((m_id, att.get("url"), att.get("fileName") or att.get("filename"), att.get("fileSizeBytes") or att.get("size_bytes")))
    for reaction in msg.get("reactions") or []:
        emoji = reaction.get("emoji") or {}
        name = emoji.get("name")
        image = emoji.get("imageUrl") or emoji.get("url")
        for reactor in reaction.get("users") or []:
            rid = str(reactor.get("id") or "")
            if rid:
                users.setdefault(rid, (rid, reactor.get("name") or reactor.get("username") or "", reactor.get("nickname") or reactor.get("globalName") or "", reactor.get("avatarUrl") or reactor.get("avatar_url") or "", bool(reactor.get("isBot", False))))
                reactions.append((m_id, rid, name, image))
    for mention in msg.get("mentions") or []:
        mid = str(mention.get("id") or "")
        if mid:
            users.setdefault(mid, (mid, mention.get("name") or mention.get("username") or "", mention.get("nickname") or "", mention.get("avatarUrl") or "", False))
            mentions.append((m_id, mid, author_id))
    return True


def import_json_to_db(filename, db_filename, server_id=None, batch_size=BATCH_SIZE, replace=True, rebuild_stats=True):
    if ijson is None:
        raise RuntimeError("缺少 ijson，请先执行 pip install ijson；大型 Discord JSON 不建议使用普通 json.load")
    meta = inspect_json(filename)
    sid = str(server_id or meta["server_id"])
    if sid == "unknown":
        raise ValueError("JSON 中找不到 guild.id，无法确定服务器")
    init_new = not os.path.exists(db_filename)
    conn = connect(db_filename)
    cur = conn.cursor()
    create_tables(cur)
    if not init_new and replace:
        # 同一服务器完整重建时替换分析数据，保留账号合并、访问记录等网站数据。
        for table in ("reactions", "attachments", "mentions", "messages", "threads", "users", "user_stats"):
            cur.execute(f"DELETE FROM {table}")
        conn.commit()

    fmt = meta["format"]
    users = {}
    attachments, reactions, mentions = [], [], []
    msg_count = 0
    thread_count = 0

    pending_rows = 0
    for item in _iter_messages(filename, fmt):
        channel, messages, thread_data = item
        if fmt == "threads":
            thread_id = str(channel.get("id") or "")
            if not thread_id:
                continue
            category_id = channel.get("categoryId") or channel.get("category_id")
            name = channel.get("name") or "Unnamed Thread"
            exported_at = thread_data.get("exportedAt") if thread_data else None
        else:
            thread_id = str(channel.get("id") or channel.get("categoryId") or "imported-channel")
            category_id = channel.get("categoryId") or channel.get("id")
            name = channel.get("name") or "Imported Messages"
            exported_at = None
        try:
            cur.execute("ALTER TABLE threads ADD COLUMN last_active_at TEXT")
        except sqlite3.OperationalError:
            pass
        cur.execute("INSERT OR IGNORE INTO threads(thread_id,category_id,name,exported_at,guild_id,last_active_at) VALUES(?,?,?,?,?,NULL)", (thread_id, category_id, name, exported_at, sid))
        cur.execute("UPDATE threads SET category_id=?,name=?,exported_at=?,guild_id=? WHERE thread_id=?", (category_id,name,exported_at,sid,thread_id))
        thread_count += 1
        for msg in messages:
            if _insert_message(cur, msg, thread_id, users, attachments, reactions, mentions):
                msg_count += 1
            if len(users) >= batch_size or len(attachments) >= batch_size or len(reactions) >= batch_size or len(mentions) >= batch_size:
                _flush_side_tables(cur, users, attachments, reactions, mentions)
        _flush_side_tables(cur, users, attachments, reactions, mentions)
        pending_rows += 1
        if pending_rows >= 500:
            conn.commit()
            pending_rows = 0

    _flush_side_tables(cur, users, attachments, reactions, mentions)
    if rebuild_stats:
        rebuild_user_stats_conn(cur)
    conn.commit()
    conn.close()
    return {"server_id": sid, "server_name": meta["server_name"], "icon_url": meta["icon_url"], "messages": msg_count, "threads": thread_count}



def rebuild_user_stats_conn(cur):
    cur.execute("DELETE FROM user_stats")
    cur.execute(
        """INSERT INTO user_stats(user_id,msg_count,first_msg_at,last_msg_at)
           SELECT author_id,count(*),min(timestamp),max(timestamp)
           FROM messages
           WHERE author_id IS NOT NULL AND author_id!=''
           GROUP BY author_id"""
    )
    cur.execute(
        """UPDATE user_stats SET reaction_received_count=COALESCE(
           (SELECT count(*) FROM reactions r JOIN messages m ON r.message_id=m.message_id
            WHERE m.author_id=user_stats.user_id),0)"""
    )


def rebuild_user_stats(db_filename):
    conn = connect(db_filename)
    cur = conn.cursor()
    create_tables(cur)
    rebuild_user_stats_conn(cur)
    conn.commit()
    conn.close()


def import_json_incremental(filename, db_filename, server_id=None, batch_size=BATCH_SIZE):
    """将单个 DiscordChatExporter JSON 增量写入 SQLite，不创建长期 JSON 数据集。"""
    return import_json_to_db(
        filename,
        db_filename,
        server_id=server_id,
        batch_size=batch_size,
        replace=False,
        rebuild_stats=False,
    )

def _flush_side_tables(cur, users, attachments, reactions, mentions):
    if users:
        cur.executemany("INSERT OR REPLACE INTO users(user_id,username,nickname,avatar_url,is_bot) VALUES(?,?,?,?,?)", list(users.values()))
        users.clear()
    if attachments:
        cur.executemany("INSERT INTO attachments(message_id,url,filename,size_bytes) VALUES(?,?,?,?)", attachments); attachments.clear()
    if reactions:
        cur.executemany("INSERT INTO reactions(message_id,user_id,emoji_name,emoji_url) VALUES(?,?,?,?)", reactions); reactions.clear()
    if mentions:
        cur.executemany("INSERT INTO mentions(message_id,mentioned_user_id,author_id) VALUES(?,?,?)", mentions); mentions.clear()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("json", nargs="?", default="merged_final_clean.json")
    p.add_argument("--db", default=DEFAULT_DB)
    p.add_argument("--server-id", default=None)
    args = p.parse_args()
    if not os.path.exists(args.json):
        raise SystemExit(f"找不到 JSON: {args.json}")
    result = import_json_to_db(args.json, args.db, args.server_id)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
