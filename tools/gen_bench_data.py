# -*- coding: utf-8 -*-
"""Generate a large synthetic server DB for latency benchmarks.

Usage: python tools/gen_bench_data.py [--messages 500000] [--guild 900000000000000002]
Creates data/servers/<guild>/discord_data.db (12 business tables, same DDL as
app.init_server_db) and registers it in data/portal.db for ADMIN_IDS users.
Never touches other guild databases. Deterministic (fixed seed).
"""
import argparse
import os
import random
import sqlite3
import sys
from datetime import datetime, timedelta, timezone

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE)

ZH = ["今天这个版本更新了吗", "我觉得这个模型效果不错", "有没有人一起打游戏", "服务器又卡了",
      "这个插件怎么安装", "哈哈哈哈哈太好笑了", "价格有点贵但是值得", "周末去爬山吗",
      "代码终于跑通了", "数据库优化之后快多了", "推荐一个好用的编辑器", "猫咪真的太可爱了",
      "明天开会讨论需求", "这个bug复现不了", "求一个低价会员渠道", "教程写得很清楚",
      "显卡价格又涨了", "大家晚安", "早上好呀", "这首歌好听"]
EN = ["just pushed the new build", "anyone tried the latest patch", "this dashboard looks great",
      "server lag is terrible tonight", "check the roadmap please", "great work on the release",
      "the wordcloud needs filtering", "python vs rust debate again", "who wants to play later",
      "database migration finished"]
NOISE = ["https://example.com/post/{n}", "<@{u}>", "<:pepe:{n}>", "<a:dance:{n}>", "😂😂", "🔥",
         "`npm install`", "```py\nprint({n})\n```", "{n}", "www.test{n}.cn", "@everyone", ":smile:"]
EMOJIS = ["👍", "😂", "🔥", "❤️", "🎉", "😭", "pepe", "kekw"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--messages", type=int, default=500_000)
    ap.add_argument("--guild", default="900000000000000002")
    ap.add_argument("--users", type=int, default=2000)
    ap.add_argument("--threads", type=int, default=20000)
    args = ap.parse_args()
    random.seed(42)
    import app as dashboard  # reuse DDL + portal init (local mode only)
    path = dashboard.server_db_path(args.guild)
    if os.path.exists(path):
        print("exists, refusing to overwrite:", path)
        return 1
    dashboard.init_server_db(path)
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    users = [str(800000000000000000 + i) for i in range(args.users)]
    conn.executemany("INSERT INTO users(user_id,username,nickname,avatar_url,is_bot) VALUES(?,?,?,?,0)",
                     [(u, f"user{i}", f"成员{i}" if i % 2 else f"Member{i}", "") for i, u in enumerate(users)])
    threads = [str(700000000000000000 + i) for i in range(args.threads)]
    conn.executemany("INSERT INTO threads(thread_id,category_id,name,exported_at,guild_id) VALUES(?,?,?,?,?)",
                     [(t, "1", f"帖子 Thread {i}", "2026-01-01T00:00:00+00:00", args.guild) for i, t in enumerate(threads)])
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    span = 600 * 86400
    weights = [1.0 / (i + 1) ** 0.8 for i in range(len(users))]
    batch, reacts, ments = [], [], []
    for n in range(args.messages):
        mid = str(600000000000000000 + n)
        au = random.choices(users, weights)[0]
        parts = [random.choice(ZH if random.random() < 0.6 else EN)]
        if random.random() < 0.35:
            parts.append(random.choice(NOISE).format(n=n, u=random.choice(users)))
        ts = (start + timedelta(seconds=int(span * n / args.messages) + random.randint(0, 3600))).isoformat()
        batch.append((mid, random.choice(threads), au, " ".join(parts), ts))
        for _ in range(random.choice([0, 0, 0, 1, 1, 2, 4])):
            e = random.choice(EMOJIS)
            reacts.append((mid, random.choice(users), e, ""))
        if random.random() < 0.05:
            ments.append((mid, random.choice(users), au))
        if len(batch) >= 50_000:
            conn.executemany("INSERT OR IGNORE INTO messages(message_id,thread_id,author_id,content,timestamp) VALUES(?,?,?,?,?)", batch)
            conn.executemany("INSERT INTO reactions(message_id,user_id,emoji_name,emoji_url) VALUES(?,?,?,?)", reacts)
            conn.executemany("INSERT INTO mentions(message_id,mentioned_user_id,author_id) VALUES(?,?,?)", ments)
            conn.commit(); batch, reacts, ments = [], [], []
    if batch:
        conn.executemany("INSERT OR IGNORE INTO messages(message_id,thread_id,author_id,content,timestamp) VALUES(?,?,?,?,?)", batch)
        conn.executemany("INSERT INTO reactions(message_id,user_id,emoji_name,emoji_url) VALUES(?,?,?,?)", reacts)
        conn.executemany("INSERT INTO mentions(message_id,mentioned_user_id,author_id) VALUES(?,?,?)", ments)
    conn.commit(); conn.close()
    portal = sqlite3.connect(dashboard.PORTAL_DB)
    now = datetime.now(timezone.utc).isoformat()
    portal.execute("INSERT OR REPLACE INTO servers(server_id,name,db_path,created_at,updated_at) VALUES(?,?,?,?,?)",
                   (args.guild, "Bench Server", path, now, now))
    for admin in dashboard.ADMIN_IDS:
        portal.execute("INSERT OR IGNORE INTO user_server_access(user_id,server_id,created_at) VALUES(?,?,?)", (admin, args.guild, now))
    portal.commit(); portal.close()
    print("ok", path, os.path.getsize(path) // 1024 // 1024, "MB")
    return 0


if __name__ == "__main__":
    sys.exit(main())
