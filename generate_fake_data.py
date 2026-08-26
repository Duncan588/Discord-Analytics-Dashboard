# -*- coding: utf-8 -*-
"""生成逼真的英文假数据用于测试 Discord Analytics Dashboard。

生成内容：
- portal.db: servers 注册、portal_users、user_server_access/presence（自动授权）
- data/servers/<guild_id>/discord_data.db: users / threads / messages /
  reactions / attachments(仅文件名) / mentions / user_stats
约 20 位成员、200 个帖子、1000 条消息，全部英文拟真讨论。
"""
import os
import random
import sqlite3
import sys
from datetime import datetime, timedelta, timezone

BASE = os.path.dirname(os.path.abspath(__file__))
PORTAL = os.path.join(BASE, "data", "portal.db")
GUILD_ID = "900000000000000001"
GUILD_NAME = "PixelForge Studio"
SERVER_DIR = os.path.join(BASE, "data", "servers", GUILD_ID)
SERVER_DB = os.path.join(SERVER_DIR, "discord_data.db")
ADMIN_ID = "891196284998930522"

random.seed(20260825)

# ---------------- 20 位成员 ----------------
MEMBERS = [
    ("891196284998930522", "admin_piaoc", "Admin"),
    ("900000000000000101", "novaforge", "Nova"),
    ("900000000000000102", "pixelpanda", "Panda"),
    ("900000000000000103", "glitchwitch", "Riley"),
    ("900000000000000104", "bytebandit", "Marcus"),
    ("900000000000000105", "lunarbloom", "Luna"),
    ("900000000000000106", "voidrunner", "Kai"),
    ("900000000000000107", "sircircuit", "Owen"),
    ("900000000000000108", "mintchipdev", "Mint"),
    ("900000000000000109", "ghostpixel", "Elena"),
    ("900000000000000110", "turbo_toast", "Theo"),
    ("900000000000000111", "nebula_nina", "Nina"),
    ("900000000000000112", "crashoverride", "Zoe"),
    ("900000000000000113", "hexhound", "Dmitri"),
    ("900000000000000114", "softserve", "Amara"),
    ("900000000000000115", "quantumquokka", "Quinn"),
    ("900000000000000116", "rustbucket", "Frank"),
    ("900000000000000117", "aurorabay", "Skye"),
    ("900000000000000118", "modemnoise", "Jonas"),
    ("900000000000000119", "velvetfog", "Iris"),
]
MEMBER_IDS = [m[0] for m in MEMBERS]

# ---------------- 帖子模板（论坛分类 + 标题） ----------------
CATEGORIES = {
    "1200000000000001": "General Chat",
    "1200000000000002": "Dev Logs",
    "1200000000000003": "Help & Support",
    "1200000000000004": "Showcase",
    "1200000000000005": "Off Topic",
}
TITLE_BANK = {
    "1200000000000001": [
        "Welcome new members! Introduce yourself here",
        "Community rules refresh - please read",
        "What are you working on this week?",
        "Weekly hangout thread",
        "Server suggestions megathread",
        "Anyone up for a game night this weekend?",
        "Meme thread - keep it civil",
        "How did you find this community?",
        "Poll: best time for voice chats",
        "Monthly community update",
    ],
    "1200000000000002": [
        "Dev Log #12: rewrote the physics engine",
        "Progress update on the inventory system",
        "Shipped the new renderer, benchmarks inside",
        "Dev Log #13: netcode adventures",
        "Refactoring the save system, lessons learned",
        "Week 3 of building my roguelike",
        "Finally fixed that memory leak (hopefully)",
        "Dev Log #14: audio pipeline overhaul",
        "Pathfinding improvements and stress tests",
        "Postmortem: the great database corruption of Tuesday",
    ],
    "1200000000000003": [
        "Shader compiles but renders black - any ideas?",
        "Unity vs Godot for a 2D platformer?",
        "Getting NaNs from my quaternion math",
        "Why is my coroutine not yielding?",
        "Best way to version control large binary assets?",
        "Collision detection flickering at high speeds",
        "Git LFS setup help",
        "My build works on Windows but crashes on Linux",
        "How to profile GPU bottlenecks?",
        "Blender export scale is wrong every time",
    ],
    "1200000000000004": [
        "[WIP] Cyberpunk alley scene - feedback welcome",
        "I made a tiny pixel art city in 48 hours",
        "Screenshot Saturday: post your latest work",
        "Released my first itch.io demo!",
        "Made a lo-fi synthwave track for my game",
        "3D printed a model from our game",
        "My entry for the monthly jam",
        "Redesigned the main menu UI",
        "First attempt at voxel art",
        "Trailer for our steam page - thoughts?",
    ],
    "1200000000000005": [
        "Coffee appreciation thread",
        "What keyboard are you using?",
        "Cats of the community (post yours)",
        "Hot take: tabs are better than spaces",
        "Favorite programming language and why",
        "Any book recommendations?",
        "Mechanical keyboard group buy interest check",
        "What's everyone listening to while coding?",
        "Desk setup battlestation thread",
        "The great pineapple pizza debate",
    ],
}

# 消息语料：按语境分桶，组合后像真实讨论
OPENERS = [
    "just pushed the fix for this", "okay so I've been digging into this all morning",
    "can confirm, same issue on my end", "this is actually really clean, nice work",
    "wait hold on let me test something", "has anyone tried this on the latest build?",
    "quick question before I break everything again", "finally got some time to look at this",
    "sorry for the late reply, was heads down all day", "posting here so I don't lose the link later",
]
BODIES = [
    "the issue turned out to be a race condition in the update loop, classic",
    "if you set the flag to false it stops repro-ing immediately, not sure why it defaults to true",
    "benchmarks show a ~30% improvement after switching to the spatial hash",
    "I think the docs are outdated on this, the API changed two versions ago",
    "works fine on my machine but Jenkins keeps failing with an OOM error",
    "the profiler says we're spending most of our time in garbage collection",
    "you can just cast it to the base type and it picks the right overload",
    "there's a known workaround, pin the dependency to 4.2.1 until they patch it",
    "honestly the simplest fix was deleting the cache folder, go figure",
    "the new pipeline cuts build times from 12 min down to about 4",
    "make sure you regenerate the bindings or nothing will link",
    "I'd suggest profiling first before rewriting anything, learned that the hard way",
    "the visual glitch only happens at exactly 144hz which took me way too long to figure out",
    "we could batch these calls together instead of doing one per frame",
    "reading through the source now, give me like 10 minutes",
]
REPLIES = [
    "@{other} yeah that matches what I saw too",
    "oh good catch @{other}, that would explain it",
    "thanks, that solved it. you're a lifesaver",
    "hm interesting, I'll try that tonight and report back",
    "agreed, let's go with that approach then",
    "lol same thing happened to me last week",
    "+1 to what {other} said",
    "that's brilliant actually, why didn't I think of that",
    "I owe you a coffee @{other}",
    "fair point, though I worry about edge cases",
    "update: still broken but differently now 😅",
    "never mind, user error as usual",
]
CLOSERS = [
    "anyway I'll keep this thread updated", "closing the loop on this, everything's green now",
    "feel free to ping me if it breaks again", "adding this to the wiki so we stop re-deriving it",
    "thanks everyone, this community is genuinely helpful",
]
EMOJIS = ["🔥", "😂", "👀", "💯", "🚀", "❤️", "🤔", "🎉"]

now = datetime.now(timezone.utc)
start = now - timedelta(days=90)


def ts_random():
    return (start + timedelta(seconds=random.randint(0, int((now - start).total_seconds())))).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "+00:00"


def init_db(path, script):
    conn = sqlite3.connect(path)
    conn.executescript(script)
    return conn


# ---------- portal.db ----------
portal = init_db(PORTAL, """
CREATE TABLE IF NOT EXISTS servers(server_id TEXT PRIMARY KEY,name TEXT,icon_url TEXT,owner_user_id TEXT,
 db_path TEXT,created_at TEXT,updated_at TEXT,source_task_id INTEGER);
CREATE TABLE IF NOT EXISTS portal_users(user_id TEXT PRIMARY KEY,username TEXT NOT NULL,nickname TEXT,
 avatar_url TEXT,last_login TEXT);
CREATE TABLE IF NOT EXISTS whitelist_users(user_id TEXT PRIMARY KEY,username TEXT,added_by TEXT,created_at DATETIME NOT NULL);
CREATE TABLE IF NOT EXISTS user_server_access(user_id TEXT NOT NULL,server_id TEXT NOT NULL,granted_by TEXT,
 created_at DATETIME NOT NULL, PRIMARY KEY(user_id,server_id));
CREATE TABLE IF NOT EXISTS user_server_presence(user_id TEXT NOT NULL,server_id TEXT NOT NULL,first_seen DATETIME NOT NULL,
 last_seen DATETIME NOT NULL, PRIMARY KEY(user_id,server_id));
CREATE TABLE IF NOT EXISTS server_visitors(user_id TEXT PRIMARY KEY,server_id TEXT,username TEXT,nickname TEXT,
 avatar_url TEXT,last_visit DATETIME);
""")
db_path_abs = SERVER_DB.replace("\\", "/")
portal.execute(
    "INSERT INTO servers(server_id,name,icon_url,owner_user_id,db_path,created_at,updated_at) "
    "VALUES(?,?,?,?,?,?,?) ON CONFLICT(server_id) DO UPDATE SET name=excluded.name,db_path=excluded.db_path,updated_at=excluded.updated_at",
    (GUILD_ID, GUILD_NAME, None, MEMBER_IDS[0], SERVER_DB, utc := ts_random(), utc),
)
for uid, uname, nick in MEMBERS:
    last = ts_random()
    portal.execute("INSERT INTO portal_users(user_id,username,nickname,avatar_url,last_login) VALUES(?,?,?,?,?) "
                   "ON CONFLICT(user_id) DO UPDATE SET username=excluded.username",
                   (uid, uname, nick, None, last))
    portal.execute("INSERT OR IGNORE INTO user_server_access VALUES(?,?,?,?)", (uid, GUILD_ID, ADMIN_ID, last))
    first = ts_random()
    portal.execute("INSERT OR REPLACE INTO user_server_presence VALUES(?,?,?,?)",
                   (uid, GUILD_ID, min(first, last), max(first, last)))
portal.commit()
portal.close()

# ---------- server db ----------
os.makedirs(SERVER_DIR, exist_ok=True)
srv = init_db(SERVER_DB, """
CREATE TABLE IF NOT EXISTS users (user_id TEXT PRIMARY KEY, username TEXT, nickname TEXT, avatar_url TEXT, is_bot BOOLEAN);
CREATE TABLE IF NOT EXISTS threads (thread_id TEXT PRIMARY KEY, category_id TEXT, name TEXT, exported_at TEXT, guild_id TEXT, last_active_at TEXT);
CREATE TABLE IF NOT EXISTS thread_scan_state (thread_id TEXT PRIMARY KEY, name TEXT, last_active_at TEXT, scanned_at TEXT, guild_id TEXT);
CREATE TABLE IF NOT EXISTS messages (message_id TEXT PRIMARY KEY, thread_id TEXT, author_id TEXT, content TEXT, timestamp DATETIME, reply_to_msg_id TEXT);
CREATE TABLE IF NOT EXISTS reactions (id INTEGER PRIMARY KEY AUTOINCREMENT, message_id TEXT, user_id TEXT, emoji_name TEXT, emoji_url TEXT);
CREATE TABLE IF NOT EXISTS attachments (id INTEGER PRIMARY KEY AUTOINCREMENT, message_id TEXT, url TEXT, filename TEXT, size_bytes INTEGER);
CREATE TABLE IF NOT EXISTS mentions (id INTEGER PRIMARY KEY AUTOINCREMENT, message_id TEXT, mentioned_user_id TEXT, author_id TEXT);
CREATE TABLE IF NOT EXISTS user_stats (user_id TEXT PRIMARY KEY, msg_count INTEGER DEFAULT 0,
 reaction_received_count INTEGER DEFAULT 0, interaction_score INTEGER DEFAULT 0, first_msg_at DATETIME, last_msg_at DATETIME);
CREATE TABLE IF NOT EXISTS user_merges (target_id TEXT PRIMARY KEY, parent_id TEXT, created_at DATETIME);
CREATE TABLE IF NOT EXISTS claim_requests_v2 (id INTEGER PRIMARY KEY AUTOINCREMENT, requester_id TEXT, target_id TEXT,
 target_name TEXT, status INTEGER DEFAULT 0, created_at DATETIME, UNIQUE(requester_id, target_id));
CREATE TABLE IF NOT EXISTS web_visitors (user_id TEXT PRIMARY KEY, username TEXT, nickname TEXT, avatar_url TEXT, last_visit DATETIME);
CREATE TABLE IF NOT EXISTS profile_views (id INTEGER PRIMARY KEY AUTOINCREMENT, target_user_id TEXT, viewer_user_id TEXT,
 viewer_name TEXT, viewer_avatar TEXT, timestamp DATETIME, UNIQUE(target_user_id, viewer_user_id));
CREATE INDEX IF NOT EXISTS idx_msg_author ON messages(author_id);
CREATE INDEX IF NOT EXISTS idx_msg_thread ON messages(thread_id);
""")

for uid, uname, nick in MEMBERS:
    srv.execute("INSERT OR IGNORE INTO users VALUES(?,?,?,?,?)", (uid, uname, nick, None, 0))

# 权重：让少数成员更活跃，更像真实社区
weights = [5 if i == 0 else random.choice([1, 1, 2, 2, 3, 4]) for i in range(len(MEMBER_IDS))]

threads_meta, msg_rows = [], []
mid_counter = 910000000000000000

for t in range(200):
    cat = random.choice(list(CATEGORIES))
    title = random.choice(TITLE_BANK[cat])
    tid = f"920{t:017d}"
    created = start + timedelta(seconds=random.randint(0, int((now - start).total_seconds() * 0.9)))
    threads_meta.append((tid, cat, title, created))

    # 每帖消息数：长尾分布（多数短，少数长），总计约 1000 条
    n_msgs = max(1, min(25, int(random.paretovariate(1.6) * 3)))
    participants = random.sample(MEMBER_IDS, k=min(len(MEMBER_IDS), random.randint(2, 7)))
    prev_msg_id = None
    last_ts = created
    for _ in range(n_msgs):
        mid_counter += random.randint(3, 40)
        author = random.choices(participants, weights=[random.random() + 0.3 for _ in participants])[0]
        roll = random.random()
        other = random.choice([m for m in participants if m != author] or participants)
        if prev_msg_id and roll < 0.35:
            content = random.choice(REPLIES).format(other=random.choice(MEMBERS)[1])
        elif roll < 0.45:
            content = random.choice(REPLIES).format(other=random.choice([m[1] for m in MEMBERS]))
        else:
            parts = [random.choice(OPENERS), random.choice(BODIES)]
            if random.random() < 0.15:
                parts.append(random.choice(CLOSERS))
            content = ". ".join(p[0].upper() + p[1:] for p in parts)
            content = content.replace("i ", "I ")
        if random.random() < 0.08:
            content += " " + random.choice(EMOJIS)
        offset = timedelta(minutes=random.randint(1, 60 * 72))
        last_ts = min(now, last_ts + offset)
        mts = last_ts.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "+00:00"
        msg_rows.append([str(mid_counter), tid, author, content, mts, prev_msg_id if random.random() < 0.4 else None])
        prev_msg_id = str(mid_counter)

for tid, cat, name, created in threads_meta:
    last_active = max((r[4] for r in msg_rows if r[1] == tid), default=None)
    srv.execute("INSERT OR IGNORE INTO threads VALUES(?,?,?,?,?,?)",
                (tid, cat, name, created.strftime("%Y-%m-%dT%H:%M:%S+00:00"), GUILD_ID, last_active))
    srv.execute("INSERT OR REPLACE INTO thread_scan_state VALUES(?,?,?,?,?)",
                (tid, name, last_active, created.strftime("%Y-%m-%dT%H:%M:%S+00:00"), GUILD_ID))

msg_ids = set()
for r in msg_rows:
    srv.execute("INSERT OR IGNORE INTO messages VALUES(?,?,?,?,?,?)", r)
    msg_ids.add(r[0])
    # 提及
    import re
    for m in MEMBERS:
        if "@" + m[1] + "" in r[3]:
            srv.execute("INSERT OR IGNORE INTO mentions(message_id,mentioned_user_id,author_id) VALUES(?,?,?)",
                        (r[0], m[0], r[2]))

# 反应
for r in random.sample(msg_rows, k=min(len(msg_rows), 350)):
    for _ in range(random.randint(1, 4)):
        srv.execute("INSERT INTO reactions(message_id,user_id,emoji_name,emoji_url) VALUES(?,?,?,NULL)",
                    (r[0], random.choice(MEMBER_IDS), random.choice(EMOJIS)))

# 附件（仅记录元数据，不生成真实文件）
for r in random.sample(msg_rows, k=min(len(msg_rows), 60)):
    fname = random.choice(["screenshot_2026", "error_log", "benchmark_results", "wireframe_v2", "profile_capture"]) + \
        random.choice([".png", ".txt", ".csv", ".blend"])
    srv.execute("INSERT INTO attachments(message_id,url,filename,size_bytes) VALUES(?,?,?,?)",
                (r[0], None, fname, random.randint(50_000, 8_000_000)))

# user_stats 聚合
srv.execute("""
INSERT INTO user_stats(user_id,msg_count,reaction_received_count,interaction_score,first_msg_at,last_msg_at)
SELECT m.author_id, COUNT(*), 0, COUNT(*)*2, MIN(m.timestamp), MAX(m.timestamp)
FROM messages m GROUP BY m.author_id
ON CONFLICT(user_id) DO UPDATE SET msg_count=excluded.msg_count
""")
srv.execute("""
UPDATE user_stats SET reaction_received_count=(
  SELECT COUNT(*) FROM reactions r JOIN messages m ON r.message_id=m.message_id WHERE m.author_id=user_stats.user_id)
""")
srv.commit()

counts = {}
for tbl in ["users", "threads", "messages", "reactions", "attachments", "mentions", "user_stats"]:
    counts[tbl] = srv.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
print("server db:", counts)
pc = sqlite3.connect(PORTAL)
print("portal:", {t: pc.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t in ["servers","portal_users","user_server_access"]})
