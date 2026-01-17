from flask import Flask, render_template, request, g, redirect, session, url_for, make_response, jsonify, flash, \
    send_from_directory
import sqlite3
from datetime import datetime, timedelta, timezone
import collections
import math
import requests
import csv
import os
import re
import pickle
import sys
import time
from multiprocessing import Pool, cpu_count

app = Flask(__name__)
app.secret_key = 'YOUR_SUPER_SECRET_KEY_CHANGE_THIS'

# ================= 配置区 =================
DB_DATABASE = 'discord_data.db'
SERVER_ID = "频道id"
ITEMS_PER_PAGE = 100
CSV_FILENAME = 'members.csv'
CACHE_FILE = 'cache_data_full.pkl'
CHECKPOINT_INTERVAL = 50

# 管理员 ID
ADMIN_IDS = ["discord admin ID"]

DISCORD_CLIENT_ID = "Client ID"  # <--- 填入你的 Client ID
DISCORD_CLIENT_SECRET = "Client Secret"  # <--- 填入你的 Client Secret
API_BASE_URL = 'https://discord.com/api/v10'


# =========================================

# --- 0. 工具函数 ---
def format_time(seconds):
    if seconds < 0: seconds = 0
    m, s = divmod(int(seconds), 60);
    h, m = divmod(m, 60)
    if h > 0: return f"{h:d}:{m:02d}:{s:02d}"
    return f"{m:02d}:{s:02d}"


def print_progress_bar(iteration, total, start_time, prefix='', length=30):
    if total == 0: total = 1
    percent = ("{0:.1f}").format(100 * (iteration / float(total)))
    filled_length = int(length * iteration // total)
    bar = '█' * filled_length + '-' * (length - filled_length)
    elapsed_time = time.time() - start_time
    speed = iteration / elapsed_time if elapsed_time > 0 and iteration > 0 else 0
    eta_seconds = (total - iteration) / speed if speed > 0 else 0
    sys.stdout.write(
        f'\r{prefix} |{bar}| {percent}% ({iteration}/{total}) [{speed:.1f} it/s] ETA: {format_time(eta_seconds)}')
    sys.stdout.flush()
    if iteration == total: print()


def log_step(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


# --- 1. 数据库 ---
def get_db():
    db = getattr(g, '_database', None)
    if db is None: db = g._database = sqlite3.connect(DB_DATABASE); db.row_factory = sqlite3.Row
    return db


@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None: db.close()


def init_db_structure():
    if not os.path.exists(DB_DATABASE): return
    conn = sqlite3.connect(DB_DATABASE);
    cur = conn.cursor()
    try:
        cur.execute("ALTER TABLE users ADD COLUMN visited_report INTEGER DEFAULT 0")
    except:
        pass
    try:
        cur.execute("ALTER TABLE users ADD COLUMN last_visit DATETIME")
    except:
        pass
    try:
        cur.execute(
            """CREATE TABLE IF NOT EXISTS profile_views (id INTEGER PRIMARY KEY AUTOINCREMENT, target_user_id TEXT, viewer_user_id TEXT, viewer_name TEXT, viewer_avatar TEXT, timestamp DATETIME, UNIQUE(target_user_id, viewer_user_id))""")
    except:
        pass
    try:
        cur.execute(
            """CREATE TABLE IF NOT EXISTS web_visitors (user_id TEXT PRIMARY KEY, username TEXT, nickname TEXT, avatar_url TEXT, last_visit DATETIME)""")
    except:
        pass
    cur.execute(
        """CREATE TABLE IF NOT EXISTS claim_requests (id INTEGER PRIMARY KEY AUTOINCREMENT, requester_id TEXT, target_id TEXT, target_name TEXT, status INTEGER DEFAULT 0, created_at DATETIME, UNIQUE(requester_id, target_id))""")
    cur.execute(
        """CREATE TABLE IF NOT EXISTS user_merges (target_id TEXT PRIMARY KEY, parent_id TEXT, created_at DATETIME)""")
    conn.commit();
    conn.close()


def optimize_database():
    if not os.path.exists(DB_DATABASE): return
    log_step("🔧 正在检查索引...")
    conn = sqlite3.connect(DB_DATABASE);
    cur = conn.cursor()
    try:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_msg_author ON messages(author_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_msg_timestamp ON messages(timestamp)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_msg_thread ON messages(thread_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_react_user ON reactions(user_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_react_msg ON reactions(message_id)")
        try:
            cur.execute("CREATE INDEX IF NOT EXISTS idx_stats_count ON user_stats(msg_count)")
        except:
            pass
        conn.commit()
    except:
        pass
    finally:
        conn.close()


MEMBER_JOIN_MAP = {}


def load_member_csv():
    global MEMBER_JOIN_MAP
    if not os.path.exists(CSV_FILENAME): return
    try:
        with open(CSV_FILENAME, mode='r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                uid = row.get('用户ID', '').strip()
                if uid: MEMBER_JOIN_MAP[uid] = row.get('加入服务器时间(UTC)', '').strip()
    except:
        pass


def parse_and_convert(time_str):
    if not time_str: return None
    try:
        clean_val = time_str.split('+')[0].split('.')[0]
        if 'T' in clean_val:
            fmt = "%Y-%m-%dT%H:%M:%S"
        else:
            fmt = "%Y-%m-%d %H:%M:%S"
        dt_utc = datetime.strptime(clean_val, fmt).replace(tzinfo=timezone.utc)
        return dt_utc.astimezone(timezone(timedelta(hours=8)))
    except:
        return None


@app.template_filter('datetimeformat')
def datetimeformat_filter(value, format='%Y-%m-%d %H:%M'):
    dt = parse_and_convert(value)
    return dt.strftime(format) if dt else value


@app.template_filter('raw_datetime')
def raw_datetime_filter(value, format='%Y-%m-%d %H:%M'):
    dt = parse_and_convert(value)
    return dt.strftime(format) if dt else value


def is_pure_chinese(word):
    if not word: return False
    return re.match(r'^[\u4e00-\u9fa5]+$', word) is not None


def get_word_cloud_counter(text_list):
    text = " ".join([str(t) for t in text_list if t])
    if len(text) > 5000000: text = text[:5000000]
    words = re.findall(r'[\u4e00-\u9fa5]{2,}', text)
    stop_words = {'什么', '这个', '那个', '怎么', '可以', '因为', '所以', '但是', '就是', '这就', '感觉', '时候',
                  '现在', '还是', '没有', '一样', '知道', '觉得', '出来', '其实', '这种', '那样', '一下', '然后',
                  '虽然', '不是', '还有', '这里', '那里', '今天', '明天', '真的', '可能', '图片', '表情', '回复',
                  '一个', '一下', '自己', '只是', '非常', '不能', '不要', '需要', '如果', '以及', '我们', '你们',
                  '他们', '看到', '不过', '确实', '已经', '大家', '为什么', '不会', '不是', '这样', '那个', '这么',
                  '那么', '那些'}
    filtered_words = [w for w in words if w not in stop_words]
    return collections.Counter(filtered_words)


def merge_counters(old_counter, new_counter):
    if not old_counter: return new_counter
    if not new_counter: return old_counter
    return old_counter + new_counter


def format_word_cloud(counter, limit=None):
    if not counter: return []
    valid_items = [(k, v) for k, v in counter.items() if is_pure_chinese(k)]
    valid_items.sort(key=lambda x: x[1], reverse=True)
    if limit: valid_items = valid_items[:limit]
    return [{'text': word, 'weight': count} for word, count in valid_items]


def process_messages(cur, raw_messages):
    messages = []
    if raw_messages:
        msg_ids = [m['message_id'] for m in raw_messages];
        placeholders = ','.join(['?'] * len(msg_ids))
        cur.execute(
            f"SELECT message_id, emoji_name, emoji_url, count(*) as count FROM reactions WHERE message_id IN ({placeholders}) GROUP BY message_id, emoji_name",
            msg_ids);
        all_reactions = cur.fetchall();
        r_map = {};
        for r in all_reactions:
            if r['message_id'] not in r_map: r_map[r['message_id']] = []
            r_map[r['message_id']].append(r)
        for msg in raw_messages: d = dict(msg); d['detailed_reactions'] = r_map.get(msg['message_id'],
                                                                                    []); messages.append(d)
    return messages


# --- Workers ---
def analyze_message_chunk(args):
    db_path, start_id, end_id = args
    conn = sqlite3.connect(db_path);
    cur = conn.cursor()
    try:
        cur.execute("SELECT content FROM messages WHERE message_id > ? AND message_id <= ?", (start_id, end_id))
        contents = [r[0] for r in cur.fetchall()]
        counter = get_word_cloud_counter(contents)
        conn.close();
        return counter
    except:
        conn.close(); return collections.Counter()


# --- Data Engine ---
class DataEngine:
    def __init__(self):
        self.cache = {"homepage": {}, "users": {}, "last_msg_id": 0, "global_word_counter": collections.Counter(),
                      "merges": {}}

    def save_to_disk(self):
        try:
            temp_file = CACHE_FILE + '.tmp'
            with open(temp_file, 'wb') as f:
                pickle.dump(self.cache, f)
            if os.path.exists(CACHE_FILE): os.remove(CACHE_FILE)
            os.rename(temp_file, CACHE_FILE)
        except Exception as e:
            print(f"❌ 保存失败: {e}")

    def load_merges(self, cur):
        cur.execute("SELECT target_id, parent_id FROM user_merges")
        self.cache['merges'] = {row['target_id']: row['parent_id'] for row in cur.fetchall()}
        return self.cache['merges']

    def get_merged_ids(self, uid):
        # 获取所有孩子 + 自己
        children = [k for k, v in self.cache.get('merges', {}).items() if v == str(uid)]
        return [str(uid)] + children

    def force_clean_cache(self):
        if not self.cache.get("global_word_counter"): return
        log_step("🧹 词云清洗...")
        cleaned = collections.Counter(
            {k: v for k, v in self.cache["global_word_counter"].items() if is_pure_chinese(k)})
        self.cache["global_word_counter"] = cleaned

    def load_or_compute(self):
        log_step(">> 初始化引擎...")
        conn = sqlite3.connect(DB_DATABASE);
        conn.row_factory = sqlite3.Row;
        cur = conn.cursor()
        try:
            cur.execute("SELECT MAX(message_id) FROM messages"); db_max_id = cur.fetchone()[0]
        except:
            db_max_id = 0
        if db_max_id is None: db_max_id = 0

        self.load_merges(cur)

        loaded = False
        if os.path.exists(CACHE_FILE):
            try:
                log_step(">> 读取缓存...")
                with open(CACHE_FILE, 'rb') as f:
                    disk_cache = pickle.load(f)
                    if 'merges' not in disk_cache: disk_cache['merges'] = {}
                    disk_cache['merges'] = self.cache['merges']
                    self.cache = disk_cache
                    self.force_clean_cache()
                    if str(disk_cache.get("last_msg_id")) == str(db_max_id):
                        loaded = True
            except:
                pass

        if loaded:
            log_step("✅ 缓存有效")
            # 即使缓存有效，也刷新一次首页，确保排行榜和访客数据最新
            self.refresh_homepage_stats(cur)
            conn.close();
            return

        log_step(f"🚀 增量计算...")
        min_id = self.cache.get("last_msg_id", 0)
        cur.execute("SELECT count(*) FROM messages WHERE message_id > ?", (min_id,))
        if cur.fetchone()[0] > 0:
            cur.execute("SELECT message_id FROM messages WHERE message_id > ? ORDER BY message_id", (min_id,))
            all_ids = [r[0] for r in cur.fetchall()]
            chunk_size = math.ceil(len(all_ids) / (cpu_count() * 4));
            chunks = []
            for i in range(0, len(all_ids), chunk_size):
                chunk_ids = all_ids[i:i + chunk_size]
                if chunk_ids: chunks.append((DB_DATABASE, int(chunk_ids[0]) - 1, int(chunk_ids[-1])))

            new_counter = collections.Counter()
            with Pool(processes=cpu_count()) as pool:
                for res in pool.imap_unordered(analyze_message_chunk, chunks): new_counter += res
            self.cache["global_word_counter"] = merge_counters(self.cache.get("global_word_counter"), new_counter)
            self.force_clean_cache()

        self.refresh_homepage_stats(cur)
        self.cache["last_msg_id"] = db_max_id
        self.save_to_disk()
        conn.close()

    def refresh_homepage_stats(self, cur):
        # 实时计算首页数据，确保不为空
        server_word_cloud = format_word_cloud(self.cache["global_word_counter"], 60)
        server_word_rank = server_word_cloud[:15]

        cur.execute("SELECT COUNT(*) FROM messages");
        total_msgs = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM threads");
        total_threads = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM users");
        total_users = cur.fetchone()[0]

        cur.execute("SELECT substr(timestamp, 1, 10) as day, count(*) as c FROM messages GROUP BY day ORDER BY day")
        chart_daily = [dict(zip(['day', 'c'], r)) for r in cur.fetchall()]
        cur.execute("SELECT strftime('%H', timestamp) as hour, count(*) as c FROM messages GROUP BY hour")
        hourly_raw = {r[0]: r[1] for r in cur.fetchall()};
        chart_hourly = [0] * 24
        for h, c in hourly_raw.items():
            try:
                chart_hourly[(int(h) + 8) % 24] += c
            except:
                pass
        chart_hourly = [{'hour': f"{h:02d}:00", 'c': c} for h, c in enumerate(chart_hourly)]

        cur.execute(
            "SELECT u.user_id, u.username, u.nickname, u.avatar_url, count(m.message_id) as msg_count FROM users u JOIN messages m ON u.user_id = m.author_id GROUP BY u.user_id ORDER BY msg_count DESC LIMIT 12")
        top_users = []
        for u in cur.fetchall():
            d = dict(u);
            cur.execute(
                "SELECT r.emoji_url, r.emoji_name, count(*) as c FROM reactions r JOIN messages m ON r.message_id = m.message_id WHERE m.author_id = ? GROUP BY r.emoji_name ORDER BY c DESC LIMIT 5",
                (u['user_id'],));
            d['top_emojis'] = [dict(r) for r in cur.fetchall()];
            top_users.append(d)

        cur.execute("SELECT thread_id, count(*) as c FROM messages GROUP BY thread_id ORDER BY c DESC LIMIT 10")
        top_threads = []
        for r in cur.fetchall():
            cur.execute("SELECT * FROM threads WHERE thread_id=?", (r['thread_id'],));
            t = cur.fetchone()
            if t:
                d = dict(t);
                d['msg_count'] = r['c'];
                cur.execute(
                    "SELECT username, avatar_url FROM users WHERE user_id=(SELECT author_id FROM messages WHERE thread_id=? LIMIT 1)",
                    (d['thread_id'],));
                op = cur.fetchone()
                d['op_user'] = dict(op) if op else {'username': 'Unknown', 'avatar_url': ''}
                cur.execute(
                    "SELECT emoji_url, count(*) as c FROM reactions r JOIN messages m ON r.message_id = m.message_id WHERE m.thread_id=? GROUP BY emoji_name ORDER BY c DESC LIMIT 1",
                    (d['thread_id'],))
                emoji = cur.fetchone()
                d['top_emoji_url'] = emoji['emoji_url'] if emoji else None
                d['top_emoji_count'] = emoji['c'] if emoji else 0
                top_threads.append(d)

        cur.execute("SELECT message_id, count(*) as c FROM reactions GROUP BY message_id ORDER BY c DESC LIMIT 10")
        top_hot_msgs = []
        for r in cur.fetchall():
            cur.execute("SELECT * FROM messages WHERE message_id=?", (r['message_id'],));
            m = cur.fetchone()
            if m:
                d = dict(m);
                cur.execute("SELECT username, nickname, avatar_url FROM users WHERE user_id=?", (d['author_id'],));
                auth = cur.fetchone()
                d['author'] = dict(auth) if auth else {'username': 'Unknown', 'avatar_url': ''}
                cur.execute("SELECT name FROM threads WHERE thread_id=?", (d['thread_id'],));
                tn = cur.fetchone()
                d['thread_name'] = tn['name'] if tn else 'Unknown'
                cur.execute("SELECT emoji_url, count(*) as count FROM reactions WHERE message_id=? GROUP BY emoji_name",
                            (d['message_id'],));
                d['detailed_reactions'] = [dict(row) for row in cur.fetchall()]
                top_hot_msgs.append(d)

        self.cache["homepage"] = {'total_threads': total_threads, 'total_users': total_users, 'total_msgs': total_msgs,
                                  'chart_daily': chart_daily, 'chart_hourly': chart_hourly,
                                  'server_word_cloud': server_word_cloud, 'server_word_rank': server_word_rank,
                                  'top_users': top_users, 'top_threads': top_threads, 'top_hot_msgs': top_hot_msgs}

    def get_user_data(self, user_id):
        return {}, {}


data_engine = DataEngine()


# --- Routes ---
def login_required(f):
    def wrapper(*args, **kwargs):
        if 'user' not in session: return redirect(url_for('login'))
        return f(*args, **kwargs)

    wrapper.__name__ = f.__name__
    return wrapper


def admin_required(f):
    def wrapper(*args, **kwargs):
        if 'user' not in session or str(session['user']['id']) not in ADMIN_IDS: return "403 Access Denied", 403
        return f(*args, **kwargs)

    wrapper.__name__ = f.__name__
    return wrapper


@app.route('/login')
def login(): return render_template('login.html')


@app.route('/auth/discord')
def auth_discord(): return redirect(
    f"{API_BASE_URL}/oauth2/authorize?client_id={DISCORD_CLIENT_ID}&redirect_uri={url_for('callback', _external=True)}&response_type=code&scope=identify")


@app.route('/callback')
def callback():
    code = request.args.get('code')
    if not code: return redirect(url_for('login'))
    data = {'client_id': DISCORD_CLIENT_ID, 'client_secret': DISCORD_CLIENT_SECRET, 'grant_type': 'authorization_code',
            'code': code, 'redirect_uri': url_for('callback', _external=True)}
    try:
        r = requests.post(f'{API_BASE_URL}/oauth2/token', data=data,
                          headers={'Content-Type': 'application/x-www-form-urlencoded'});
        r.raise_for_status();
        token = r.json()
        r = requests.get(f'{API_BASE_URL}/users/@me', headers={'Authorization': f"Bearer {token['access_token']}"});
        r.raise_for_status();
        user_data = r.json()

        conn = get_db();
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO users (user_id, username, nickname, avatar_url, is_bot) VALUES (?, ?, ?, ?, ?) ON CONFLICT(user_id) DO UPDATE SET username=excluded.username, nickname=excluded.nickname, avatar_url=excluded.avatar_url",
            (user_data['id'], user_data['username'], user_data.get('global_name', ''),
             f"https://cdn.discordapp.com/avatars/{user_data['id']}/{user_data['avatar']}.png", 0))
        conn.commit()

        session['user'] = {'id': user_data['id'], 'username': user_data['username'],
                           'avatar': f"https://cdn.discordapp.com/avatars/{user_data['id']}/{user_data['avatar']}.png"}

        # 【跳转逻辑】
        if request.cookies.get('has_seen_report'):
            return redirect(url_for('index'))
        else:
            return redirect(url_for('report'))
    except Exception as e:
        print(e); return render_template('login.html', error="登录错误")


@app.route('/logout')
def logout(): session.pop('user', None); return redirect(url_for('login'))


@app.route('/')
@login_required
def index():
    conn = get_db();
    cur = conn.cursor()
    u = session['user']
    now_str = datetime.now(timezone.utc).isoformat()
    # 【修复访客记录】UPSERT
    try:
        cur.execute(
            "INSERT INTO web_visitors (user_id, username, nickname, avatar_url, last_visit) VALUES (?, ?, ?, ?, ?) ON CONFLICT(user_id) DO UPDATE SET last_visit=excluded.last_visit, avatar_url=excluded.avatar_url",
            (u['id'], u['username'], u['username'], u['avatar'], now_str))
        conn.commit()
    except Exception as e:
        print(f"Vis Error: {e}")

    # 【修复首页空白】如果 Cache 没有数据，现场生成
    data = data_engine.cache.get("homepage")
    if not data or not data.get('total_msgs'):
        print("Homepage cache empty, refreshing...")
        data_engine.refresh_homepage_stats(cur, 0)
        data = data_engine.cache.get("homepage", {})

    cur.execute("SELECT user_id, nickname, username, avatar_url, last_visit FROM web_visitors ORDER BY last_visit DESC")
    site_visitors = cur.fetchall()

    cur.execute(
        "SELECT u.user_id, u.username, u.nickname, u.avatar_url, count(m.message_id) as msg_count FROM users u JOIN messages m ON u.user_id = m.author_id GROUP BY u.user_id ORDER BY msg_count DESC LIMIT 50")
    full_leaderboard = []
    for u in cur.fetchall():
        d = dict(u);
        cur.execute(
            "SELECT emoji_url FROM reactions r JOIN messages m ON r.message_id=m.message_id WHERE m.author_id=? GROUP BY r.emoji_name ORDER BY count(*) DESC LIMIT 3",
            (u['user_id'],))
        d['top_emojis'] = [r['emoji_url'] for r in cur.fetchall()]
        full_leaderboard.append(d)

    return render_template('index.html', server_id=SERVER_ID, current_user=session['user'], site_visitors=site_visitors,
                           full_leaderboard=full_leaderboard, **data)


@app.route('/chouxiangpai')
@login_required
def chouxiangpai_page(): return send_from_directory('templates', 'chouxiangpai.html')


@app.route('/api/leaderboard')
@login_required
def api_leaderboard():
    page = int(request.args.get('page', 1));
    offset = (page - 1) * 50
    conn = get_db();
    conn.row_factory = sqlite3.Row;
    cur = conn.cursor()
    cur.execute(
        "SELECT u.user_id, u.username, u.nickname, u.avatar_url, count(m.message_id) as msg_count FROM users u JOIN messages m ON u.user_id = m.author_id GROUP BY u.user_id ORDER BY msg_count DESC LIMIT 50 OFFSET ?",
        (offset,))
    users = []
    for u in cur.fetchall():
        d = dict(u);
        cur.execute(
            "SELECT emoji_url FROM reactions r JOIN messages m ON r.message_id=m.message_id WHERE m.author_id=? GROUP BY r.emoji_name ORDER BY count(*) DESC LIMIT 3",
            (u['user_id'],))
        d['top_emojis'] = [r['emoji_url'] for r in cur.fetchall()]
        users.append(d)
    return jsonify(users)


@app.route('/search')
@login_required
def search():
    query = request.args.get('q', '').strip();
    cur = get_db().cursor()
    cur.execute("SELECT * FROM users WHERE user_id = ? OR username LIKE ? OR nickname LIKE ? LIMIT 20",
                (query, f'%{query}%', f'%{query}%'))
    data = data_engine.cache.get("homepage", {})
    return render_template('index.html', search_results=cur.fetchall(), query=query, server_id=SERVER_ID,
                           current_user=session['user'], site_visitors=[], full_leaderboard=[], **data)


@app.route('/user/<user_id>')
@login_required
def user_profile(user_id):
    # 防止死循环：如果 A->B, B->A，或者 Parent==Target
    parent_id = data_engine.cache.get('merges', {}).get(str(user_id))
    if parent_id and str(parent_id) != str(user_id):
        flash(f"账号 {user_id} 已合并至 {parent_id}");
        return redirect(url_for('user_profile', user_id=parent_id))

    conn = get_db();
    cur = conn.cursor()
    cur.execute("SELECT * FROM users WHERE user_id = ?", (user_id,));
    user = cur.fetchone()
    if not user: return "User Not Found", 404

    visitor = session.get('user')
    if visitor and str(visitor['id']) != str(user_id):
        try:
            cur.execute(
                "INSERT INTO profile_views (target_user_id, viewer_user_id, viewer_name, viewer_avatar, timestamp) VALUES (?, ?, ?, ?, ?) ON CONFLICT(target_user_id, viewer_user_id) DO UPDATE SET timestamp=excluded.timestamp",
                (user_id, visitor['id'], visitor['username'], visitor['avatar'],
                 datetime.now(timezone.utc).isoformat())); conn.commit()
        except:
            pass
    cur.execute("SELECT COUNT(*) as c FROM profile_views WHERE target_user_id = ?", (user_id,));
    view_count = cur.fetchone()['c']
    cur.execute(
        "SELECT viewer_name, viewer_avatar, timestamp FROM profile_views WHERE target_user_id = ? ORDER BY timestamp DESC LIMIT 20",
        (user_id,));
    recent_viewers = cur.fetchall()

    merged_ids = data_engine.get_merged_ids(user_id)
    ids_ph = ','.join(['?'] * len(merged_ids))

    cur.execute(f"SELECT count(DISTINCT message_id) as c FROM messages WHERE author_id IN ({ids_ph})", merged_ids)
    msg_count = cur.fetchone()['c']
    cur.execute(
        f"SELECT count(*) as c FROM reactions r JOIN messages m ON r.message_id = m.message_id WHERE m.author_id IN ({ids_ph})",
        merged_ids)
    reaction_received_count = cur.fetchone()['c']

    sort_by = request.args.get('sort', 'hot');
    page = int(request.args.get('page', 1));
    offset = (page - 1) * ITEMS_PER_PAGE
    msg_order = "ORDER BY total_reactions DESC, m.timestamp DESC" if sort_by == 'hot' else "ORDER BY m.timestamp DESC"
    cur.execute(
        f"SELECT m.*, t.name as thread_name, (SELECT count(*) FROM reactions WHERE message_id = m.message_id) as total_reactions FROM messages m JOIN threads t ON m.thread_id = t.thread_id WHERE m.author_id IN ({ids_ph}) {msg_order} LIMIT ? OFFSET ?",
        (*merged_ids, ITEMS_PER_PAGE, offset))
    messages = process_messages(cur, cur.fetchall())

    cur.execute(
        f"""SELECT count(DISTINCT t.thread_id) as c FROM threads t JOIN messages m ON t.thread_id = m.thread_id WHERE m.author_id IN ({ids_ph}) AND m.timestamp = (SELECT min(timestamp) FROM messages WHERE thread_id = t.thread_id)""",
        merged_ids);
    thread_count = cur.fetchone()['c']
    total_thread_pages = math.ceil(thread_count / ITEMS_PER_PAGE)
    cur.execute(
        f"""SELECT t.thread_id, t.name, m.timestamp as created_at, (SELECT count(*) FROM messages WHERE thread_id = t.thread_id) as reply_count, (SELECT content FROM messages WHERE thread_id = t.thread_id ORDER BY timestamp ASC LIMIT 1) as first_content, (SELECT message_id FROM messages WHERE thread_id = t.thread_id ORDER BY timestamp ASC LIMIT 1) as op_msg_id FROM threads t JOIN messages m ON t.thread_id = m.thread_id WHERE m.author_id IN ({ids_ph}) AND m.timestamp = (SELECT min(timestamp) FROM messages WHERE thread_id = t.thread_id) ORDER BY reply_count DESC LIMIT ? OFFSET ?""",
        (*merged_ids, ITEMS_PER_PAGE, offset))
    raw_my_threads = cur.fetchall();
    my_threads = []
    for t in raw_my_threads:
        d = dict(t);
        d['op_user'] = user
        if d['op_msg_id']:
            cur.execute(
                "SELECT emoji_url FROM reactions WHERE message_id = ? GROUP BY emoji_name ORDER BY count(*) DESC LIMIT 1",
                (d['op_msg_id'],));
            emoji_row = cur.fetchone();
            d['top_emoji_url'] = emoji_row['emoji_url'] if emoji_row else None
            cur.execute(
                "SELECT count(*) as c FROM reactions WHERE message_id = ? GROUP BY emoji_name ORDER BY c DESC LIMIT 1",
                (d['op_msg_id'],));
            count_row = cur.fetchone();
            d['top_emoji_count'] = count_row['c'] if count_row else None
        my_threads.append(d)

    cur.execute(
        f"SELECT emoji_url, emoji_name, count(*) as c FROM reactions WHERE user_id IN ({ids_ph}) GROUP BY emoji_name ORDER BY c DESC LIMIT 8",
        merged_ids);
    top_emojis_given = [dict(r) for r in cur.fetchall()]
    cur.execute(
        f"SELECT r.emoji_url, r.emoji_name, count(*) as c FROM reactions r JOIN messages m ON r.message_id = m.message_id WHERE m.author_id IN ({ids_ph}) GROUP BY r.emoji_name ORDER BY c DESC LIMIT 8",
        merged_ids);
    top_emojis_received = [dict(r) for r in cur.fetchall()]
    cur.execute(
        f"SELECT u.user_id, u.nickname, u.username, u.avatar_url, COUNT(*) as score FROM (SELECT author_id as source_id FROM mentions WHERE mentioned_user_id IN ({ids_ph}) UNION ALL SELECT r.user_id as source_id FROM reactions r JOIN messages m ON r.message_id = m.message_id WHERE m.author_id IN ({ids_ph})) raw JOIN users u ON raw.source_id = u.user_id WHERE u.user_id NOT IN ({ids_ph}) GROUP BY u.user_id ORDER BY score DESC LIMIT 5",
        (*merged_ids, *merged_ids, *merged_ids));
    interactions_incoming = [dict(r) for r in cur.fetchall()]
    cur.execute(
        f"SELECT u.user_id, u.nickname, u.username, u.avatar_url, COUNT(*) as score FROM (SELECT mentioned_user_id as target_id FROM mentions WHERE author_id IN ({ids_ph}) UNION ALL SELECT m.author_id as target_id FROM reactions r JOIN messages m ON r.message_id = m.message_id WHERE r.user_id IN ({ids_ph})) raw JOIN users u ON raw.target_id = u.user_id WHERE u.user_id NOT IN ({ids_ph}) GROUP BY u.user_id ORDER BY score DESC LIMIT 5",
        (*merged_ids, *merged_ids, *merged_ids));
    interactions_outgoing = [dict(r) for r in cur.fetchall()]

    cur.execute(
        f"SELECT substr(timestamp, 1, 10) as day, count(*) as c FROM messages WHERE author_id IN ({ids_ph}) GROUP BY day",
        merged_ids);
    chart_daily = [dict(r) for r in sorted(cur.fetchall(), key=lambda x: x['day'])]
    cur.execute(
        f"SELECT strftime('%H', timestamp) as hour, count(*) as c FROM messages WHERE author_id IN ({ids_ph}) GROUP BY hour",
        merged_ids)
    hourly_raw = {row['hour']: row['c'] for row in cur.fetchall()};
    chart_hourly = [0] * 24
    for h, c in hourly_raw.items():
        try:
            chart_hourly[(int(h) + 8) % 24] += c
        except:
            pass
    chart_hourly = [{'hour': f"{h:02d}:00", 'c': c} for h, c in enumerate(chart_hourly)]

    cur.execute(f"SELECT content FROM messages WHERE author_id IN ({ids_ph}) ORDER BY timestamp DESC LIMIT 2000",
                merged_ids)
    text_list = [r['content'] for r in cur.fetchall()]
    word_cloud_data = format_word_cloud(get_word_cloud_counter(text_list), 50)

    return render_template('user.html', user=user, messages=messages, my_threads=my_threads, view_count=view_count,
                           recent_viewers=recent_viewers, server_id=SERVER_ID, current_sort=sort_by, current_page=page,
                           total_msg_pages=math.ceil(msg_count / ITEMS_PER_PAGE), total_thread_pages=total_thread_pages,
                           current_user=session['user'],
                           msg_count=msg_count, reaction_received_count=reaction_received_count,
                           top_emojis_given=top_emojis_given, top_emojis_received=top_emojis_received,
                           interactions_incoming=interactions_incoming, interactions_outgoing=interactions_outgoing,
                           chart_daily=chart_daily, chart_hourly=chart_hourly, word_cloud_data=word_cloud_data)


@app.route('/claim_account', methods=['POST'])
@login_required
def claim_account():
    target_id = request.form.get('target_id').strip();
    requester_id = str(session['user']['id'])
    if target_id == requester_id: return "无效请求", 400
    conn = get_db();
    cur = conn.cursor()
    print(f"[CLAIM DEBUG] {requester_id} claiming {target_id}")
    cur.execute("SELECT nickname FROM users WHERE user_id=?", (target_id,));
    t = cur.fetchone()
    if not t: return "目标不存在", 404
    try:
        cur.execute("INSERT INTO claim_requests (requester_id, target_id, target_name, created_at) VALUES (?,?,?,?)",
                    (requester_id, target_id, t['nickname'], datetime.now()));
        conn.commit()
        print(f"[CLAIM SUCCESS]")
        flash("认领申请已提交，请等待管理员审核。")
    except:
        flash("申请已存在")
    return redirect(url_for('user_profile', user_id=target_id))


@app.route('/admin')
@admin_required
def admin_panel():
    conn = get_db();
    cur = conn.cursor()
    cur.execute(
        "SELECT r.*, u.username as req_name, u.avatar_url as req_avatar FROM claim_requests r LEFT JOIN users u ON r.requester_id=u.user_id WHERE r.status=0")
    reqs = cur.fetchall()
    cur.execute("SELECT m.*, u.username as parent_name FROM user_merges m LEFT JOIN users u ON m.parent_id=u.user_id")
    merges = cur.fetchall()
    return render_template('admin.html', pending_requests=reqs, active_merges=merges)


@app.route('/admin/approve/<int:req_id>')
@admin_required
def admin_approve(req_id):
    conn = get_db();
    cur = conn.cursor()
    cur.execute("SELECT * FROM claim_requests WHERE id=?", (req_id,));
    req = cur.fetchone()
    if req:
        cur.execute("UPDATE claim_requests SET status=1 WHERE id=?", (req_id,))
        cur.execute("INSERT OR REPLACE INTO user_merges (target_id, parent_id, created_at) VALUES (?,?,?)",
                    (req['target_id'], req['requester_id'], datetime.now()))
        conn.commit();
        data_engine.cache['merges'][req['target_id']] = req['requester_id']
    return redirect(url_for('admin_panel'))


@app.route('/admin/unmerge/<target_id>')
@admin_required
def admin_unmerge(target_id):
    conn = get_db();
    cur = conn.cursor()
    cur.execute("DELETE FROM user_merges WHERE target_id=?", (target_id,))
    conn.commit()
    if target_id in data_engine.cache['merges']: del data_engine.cache['merges'][target_id]
    return redirect(url_for('admin_panel'))


@app.route('/admin/reset_all_claims')
@admin_required
def admin_reset_all():
    conn = get_db();
    cur = conn.cursor()
    cur.execute("DELETE FROM claim_requests")
    cur.execute("DELETE FROM user_merges")
    conn.commit()
    data_engine.cache['merges'] = {}
    return redirect(url_for('admin_panel'))


@app.route('/report')
@login_required
def report():
    try:
        user_id = session['user']['id'];
        conn = get_db();
        cur = conn.cursor()
        cur.execute("SELECT * FROM users WHERE user_id = ?", (user_id,));
        db_user = cur.fetchone()

        # 补全
        if not db_user:
            db_user = {'user_id': user_id, 'username': session['user']['username'],
                       'avatar_url': session['user']['avatar'], 'nickname': session['user']['username']}

        merged_ids = data_engine.get_merged_ids(user_id);
        ids_ph = ','.join(['?'] * len(merged_ids))

        report = {}
        cur.execute(f"SELECT min(timestamp) as joined FROM messages WHERE author_id IN ({ids_ph})", merged_ids)
        joined_row = cur.fetchone();
        report['join_date'] = datetimeformat_filter(joined_row['joined'], '%Y-%m-%d') if joined_row and joined_row[
            'joined'] else "未知"

        cur.execute(
            f"SELECT substr(timestamp, 1, 10) as day, count(*) as c FROM messages WHERE author_id IN ({ids_ph}) GROUP BY day ORDER BY c DESC LIMIT 1",
            merged_ids)
        row = cur.fetchone();
        report['most_active_day'] = dict(row) if row else None

        cur.execute(
            f"SELECT * FROM messages WHERE author_id IN ({ids_ph}) AND strftime('%H', timestamp) IN ('16','17','18','19','20','21') ORDER BY timestamp DESC LIMIT 1",
            merged_ids)
        late = cur.fetchone()
        if late:
            d = dict(late);
            cur.execute("SELECT name FROM threads WHERE thread_id=?", (d['thread_id'],));
            tn = cur.fetchone()
            d['thread_name'] = tn['name'] if tn else 'Unknown';
            report['latest_msg'] = d
        else:
            report['latest_msg'] = None

        cur.execute(
            f"SELECT t.thread_id, t.name, (SELECT count(*) FROM messages WHERE thread_id = t.thread_id) as reply_count FROM threads t JOIN messages m ON t.thread_id = m.thread_id WHERE m.author_id IN ({ids_ph}) AND m.timestamp = (SELECT min(timestamp) FROM messages WHERE thread_id = t.thread_id) ORDER BY reply_count DESC LIMIT 1",
            merged_ids)
        row = cur.fetchone();
        if row:
            d = dict(row); d['op_user'] = db_user; report['most_replied_thread'] = d
        else:
            report['most_replied_thread'] = None

        cur.execute(
            f"SELECT t.thread_id, t.name, count(*) as c FROM messages m JOIN threads t ON m.thread_id = t.thread_id WHERE m.author_id IN ({ids_ph}) GROUP BY t.thread_id ORDER BY c DESC LIMIT 1",
            merged_ids)
        row = cur.fetchone();
        report['most_active_topic'] = dict(row) if row else None

        cur.execute(
            f"SELECT m.*, t.name as thread_name, (SELECT count(*) FROM reactions WHERE message_id = m.message_id) as rc FROM messages m JOIN threads t ON m.thread_id = t.thread_id WHERE m.author_id IN ({ids_ph}) ORDER BY rc DESC LIMIT 1",
            merged_ids)
        row = cur.fetchone()
        if row:
            d = dict(row);
            cur.execute("SELECT emoji_url, count(*) as count FROM reactions WHERE message_id = ? GROUP BY emoji_name",
                        (d['message_id'],));
            d['detailed_reactions'] = [dict(r) for r in cur.fetchall()];
            report['most_liked_msg'] = d
        else:
            report['most_liked_msg'] = None

        cur.execute(
            f"SELECT u.nickname, u.username, u.avatar_url, COUNT(*) as score FROM (SELECT author_id as source_id FROM mentions WHERE mentioned_user_id IN ({ids_ph}) UNION ALL SELECT r.user_id as source_id FROM reactions r JOIN messages m ON r.message_id = m.message_id WHERE m.author_id IN ({ids_ph})) raw JOIN users u ON raw.source_id = u.user_id WHERE u.user_id NOT IN ({ids_ph}) GROUP BY u.user_id ORDER BY score DESC LIMIT 1",
            (*merged_ids, *merged_ids, *merged_ids))
        row = cur.fetchone();
        report['top_friend_incoming'] = dict(row) if row else None

        cur.execute(
            f"SELECT u.nickname, u.username, u.avatar_url, COUNT(*) as score FROM (SELECT mentioned_user_id as target_id FROM mentions WHERE author_id IN ({ids_ph}) UNION ALL SELECT m.author_id as target_id FROM reactions r JOIN messages m ON r.message_id = m.message_id WHERE r.user_id IN ({ids_ph})) raw JOIN users u ON raw.target_id = u.user_id WHERE u.user_id NOT IN ({ids_ph}) GROUP BY u.user_id ORDER BY score DESC LIMIT 1",
            (*merged_ids, *merged_ids, *merged_ids))
        row = cur.fetchone();
        report['top_friend_outgoing'] = dict(row) if row else None

        cur.execute(f"SELECT content FROM messages WHERE author_id IN ({ids_ph}) ORDER BY timestamp DESC LIMIT 2000",
                    merged_ids)
        text_list = [r['content'] for r in cur.fetchall()]
        wc = format_word_cloud(get_word_cloud_counter(text_list), 50)

        resp = make_response(
            render_template('report.html', user=db_user, server_id=SERVER_ID, word_cloud_data=wc, percentile=95,
                            **report))
        resp.set_cookie('has_seen_report', '1', max_age=60 * 60 * 24 * 365)
        return resp
    except Exception as e:
        return render_template('login.html', error=f"生成报告失败: {e}")


if __name__ == '__main__':
    if os.environ.get('WERKZEUG_RUN_MAIN') == 'true':
        init_db_structure()
        optimize_database()
        load_member_csv()
        data_engine.load_or_compute()
    app.run(host='0.0.0.0', port=5000, debug=True)