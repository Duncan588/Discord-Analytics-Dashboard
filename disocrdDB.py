import sqlite3
import os
import time

try:
    import ijson
except ImportError:
    ijson = None

# ================= 配置 =================
JSON_FILENAME = "抽象派 - 日常冲浪区🏄 - 1019924310665728022.json"
DB_FILENAME = "discord_data.db"
SERVER_ID = "915249444721668096"
BATCH_SIZE = 5000  # 每处理多少条消息写入一次硬盘 (防止内存爆炸)


# =======================================

def create_tables(cursor):
    cursor.execute('''CREATE TABLE IF NOT EXISTS users (
        user_id TEXT PRIMARY KEY, username TEXT, nickname TEXT, avatar_url TEXT, is_bot BOOLEAN)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS threads (
        thread_id TEXT PRIMARY KEY, category_id TEXT, name TEXT, exported_at TEXT, guild_id TEXT)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS messages (
        message_id TEXT PRIMARY KEY, thread_id TEXT, author_id TEXT, content TEXT, 
        timestamp DATETIME, reply_to_msg_id TEXT)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS reactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT, message_id TEXT, user_id TEXT, 
        emoji_name TEXT, emoji_url TEXT)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS attachments (
        id INTEGER PRIMARY KEY AUTOINCREMENT, message_id TEXT, url TEXT, filename TEXT, size_bytes INTEGER)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS mentions (
        id INTEGER PRIMARY KEY AUTOINCREMENT, message_id TEXT, mentioned_user_id TEXT, author_id TEXT)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS user_stats (
        user_id TEXT PRIMARY KEY, msg_count INTEGER DEFAULT 0,
        reaction_received_count INTEGER DEFAULT 0, interaction_score INTEGER DEFAULT 0,
        first_msg_at DATETIME, last_msg_at DATETIME)''')


def create_indexes(cursor):
    print(">> 正在创建索引 (加速查询)...")
    idx_list = [
        "CREATE INDEX IF NOT EXISTS idx_msg_author ON messages(author_id)",
        "CREATE INDEX IF NOT EXISTS idx_msg_timestamp ON messages(timestamp)",
        "CREATE INDEX IF NOT EXISTS idx_msg_thread ON messages(thread_id)",
        "CREATE INDEX IF NOT EXISTS idx_react_user ON reactions(user_id)",
        "CREATE INDEX IF NOT EXISTS idx_react_msg ON reactions(message_id)",
        "CREATE INDEX IF NOT EXISTS idx_stats_count ON user_stats(msg_count)"
    ]
    for sql in idx_list:
        cursor.execute(sql)


def process_data():
    if ijson is None:
        raise ImportError("缺少 ijson 库。请运行: pip install ijson")
    if not os.path.exists(JSON_FILENAME):
        raise FileNotFoundError(f"错误: 找不到文件 {JSON_FILENAME}")

    # 重建数据库
    if os.path.exists(DB_FILENAME):
        try:
            os.remove(DB_FILENAME)
        except OSError as exc:
            raise RuntimeError(
                f"错误: 无法删除旧数据库 {DB_FILENAME}，请关闭占用该文件的程序后重试"
            ) from exc

    conn = sqlite3.connect(DB_FILENAME)
    cursor = conn.cursor()
    cursor.execute("PRAGMA synchronous = OFF")  # 极速写入模式
    cursor.execute("PRAGMA journal_mode = MEMORY")
    create_tables(cursor)

    print(f"🚀 开始流式处理文件: {JSON_FILENAME}")
    print(f"ℹ️  内存保护模式已开启，每 {BATCH_SIZE} 条消息写入一次...")

    # 缓冲区
    buffers = {
        'users': {},  # 用字典去重
        'threads': [],
        'messages': [],
        'attachments': [],
        'reactions': [],
        'mentions': []
    }

    counters = {'msg': 0, 'thread': 0}
    start_time = time.time()

    def flush_buffers():
        """将缓冲区写入数据库并清空"""
        if not buffers['messages'] and not buffers['threads']: return

        cursor.executemany('INSERT OR IGNORE INTO users VALUES (?,?,?,?,?)', buffers['users'].values())
        cursor.executemany('INSERT OR IGNORE INTO threads VALUES (?,?,?,?,?)', buffers['threads'])
        cursor.executemany('INSERT OR IGNORE INTO messages VALUES (?,?,?,?,?,?)', buffers['messages'])
        cursor.executemany('INSERT INTO attachments (message_id, url, filename, size_bytes) VALUES (?,?,?,?)',
                           buffers['attachments'])
        cursor.executemany('INSERT INTO reactions (message_id, user_id, emoji_name, emoji_url) VALUES (?,?,?,?)',
                           buffers['reactions'])
        cursor.executemany('INSERT INTO mentions (message_id, mentioned_user_id, author_id) VALUES (?,?,?)',
                           buffers['mentions'])
        conn.commit()

        # 清空
        buffers['users'].clear()
        buffers['threads'].clear()
        buffers['messages'].clear()
        buffers['attachments'].clear()
        buffers['reactions'].clear()
        buffers['mentions'].clear()

        print(f"   -> 已存入 {counters['msg']} 条消息...", end='\r')

    # --- 流式读取核心逻辑 ---
    with open(JSON_FILENAME, 'rb') as f:  # ijson 需要二进制模式打开
        # 'threads.item' 表示遍历 JSON 根对象中 'threads' 数组的每一个元素
        # 这样每次内存里只有 1 个 thread 的数据，而不是整个文件
        threads_stream = ijson.items(f, 'threads.item')

        for thread_data in threads_stream:
            counters['thread'] += 1

            # 1. 处理帖子
            ch = thread_data.get('channel', {})
            t_id = ch.get('id')
            buffers['threads'].append((
                t_id, ch.get('categoryId'), ch.get('name'), thread_data.get('exportedAt'), SERVER_ID
            ))

            # 2. 处理消息
            for msg in thread_data.get('messages', []):
                counters['msg'] += 1
                m_id = msg.get('id')
                auth = msg.get('author', {})
                author_id = auth.get('id')

                # 缓存用户
                if author_id not in buffers['users']:
                    buffers['users'][author_id] = (
                        author_id, auth.get('name', ''), auth.get('nickname', ''),
                        auth.get('avatarUrl', ''), auth.get('isBot', False)
                    )

                # 缓存消息
                ref_id = msg.get('reference', {}).get('messageId')
                buffers['messages'].append((
                    m_id, t_id, author_id, msg.get('content', ''), msg.get('timestamp'), ref_id
                ))

                # 缓存附件
                for att in msg.get('attachments', []):
                    buffers['attachments'].append((m_id, att.get('url'), att.get('fileName'), att.get('fileSizeBytes')))

                # 缓存反应
                for r in msg.get('reactions', []):
                    emoji_name = r.get('emoji', {}).get('name')
                    emoji_url = r.get('emoji', {}).get('imageUrl')
                    for u in r.get('users', []):
                        u_id = u.get('id')
                        if u_id not in buffers['users']:
                            buffers['users'][u_id] = (u_id, u.get('name', ''), u.get('nickname', ''),
                                                      u.get('avatarUrl', ''), False)
                        buffers['reactions'].append((m_id, u_id, emoji_name, emoji_url))

                # 缓存提及
                for m_user in msg.get('mentions', []):
                    mu_id = m_user.get('id')
                    if mu_id not in buffers['users']:
                        buffers['users'][mu_id] = (mu_id, m_user.get('name', ''), m_user.get('nickname', ''),
                                                   m_user.get('avatarUrl', ''), False)
                    buffers['mentions'].append((m_id, mu_id, author_id))

            # 检测是否需要写入硬盘
            if len(buffers['messages']) >= BATCH_SIZE:
                flush_buffers()

    # 最后一次写入
    flush_buffers()
    print(f"\n✅ 原始数据导入完成！共处理 {counters['msg']} 条消息。")

    # --- 统计计算 ---
    create_indexes(cursor)

    print(">> 正在生成统计数据 (预计算)...")
    print("   [1/2] 统计用户发言...")
    cursor.execute('''
        INSERT OR REPLACE INTO user_stats (user_id, msg_count, first_msg_at, last_msg_at)
        SELECT author_id, COUNT(*), MIN(timestamp), MAX(timestamp)
        FROM messages GROUP BY author_id
    ''')

    print("   [2/2] 统计用户获赞...")
    cursor.execute('''
        SELECT m.author_id, COUNT(r.id) FROM messages m
        JOIN reactions r ON m.message_id = r.message_id
        GROUP BY m.author_id
    ''')
    reaction_counts = cursor.fetchall()

    # 批量更新获赞数
    if reaction_counts:
        cursor.executemany('UPDATE user_stats SET reaction_received_count = ? WHERE user_id = ?',
                           [(cnt, uid) for uid, cnt in reaction_counts])

    conn.commit()
    conn.close()

    print(f"🎉 全部完成！耗时: {time.time() - start_time:.2f} 秒")


if __name__ == "__main__":
    try:
        process_data()
    except ImportError:
        print("错误: 缺少 ijson 库。请运行: pip install ijson")
        raise SystemExit(1)
    except Exception as e:
        print(f"\n❌ 发生错误: {e}")
        import traceback

        traceback.print_exc()
        raise SystemExit(1)