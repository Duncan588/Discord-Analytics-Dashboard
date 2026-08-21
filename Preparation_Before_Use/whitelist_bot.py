"""白名单管理机器人 + 服务器成员名单同步 + Forum 帖子收藏机器人。

单个 Discord Bot 进程，同一个 Token，同一个 CommandTree，合并了原来两个
独立机器人的全部功能（原 favorite_bot.py 已合并进本文件，不再单独运行）：

白名单 / 成员同步部分：
- 创建下载任务后，网站写入 member_sync_requests，机器人自动同步目标服务器成员
- 新成员加入服务器时实时写入 portal.db
- 成员资料变化时实时更新
- Admin 或白名单用户可在后台或私聊执行 /members_sync server_id 手动同步
- /whitelist_add /whitelist_remove /whitelist_list /server_access /quota /restart

收藏部分（原 favorite_bot.py）：
- 右键 Forum 帖子内的消息 -> Apps -> 📌 收藏帖子 / 📕 取消收藏
- /favorites 查看我的收藏（服务器内看当前服务器，私信看全部服务器）
- /top /top30 查看收藏排行榜
- /help 查看收藏机器人使用说明

收藏数据与白名单数据共用同一个 portal.db（PORTAL_DB），不再使用单独的
discord_favorites.db 文件，避免维护两份数据库连接配置。
"""
import os
import sys
import sqlite3
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

import aiosqlite
import discord
from discord import app_commands
from discord.ext import tasks

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def load_env():
    p = os.path.join(BASE_DIR, '.env')
    if os.path.exists(p):
        for raw in open(p, encoding='utf-8'):
            line = raw.strip()
            if line and not line.startswith('#') and '=' in line:
                k, v = line.split('=', 1)
                os.environ.setdefault(k.strip(), v.strip().strip('"\''))


load_env()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("discord-bot")

TOKEN = os.getenv('DISCORD_BOT_TOKEN', '').strip()
DB = os.getenv('PORTAL_DB', os.path.join(BASE_DIR, 'data', 'portal.db'))
if not os.path.isabs(DB):
    DB = os.path.join(BASE_DIR, DB)
ADMINS = {x.strip() for x in os.getenv('ADMIN_IDS', '').split(',') if x.strip()}

PER_PAGE = 10

intents = discord.Intents.default()
intents.guilds = True
intents.members = True


# ---------------------------------------------------------------------------
# 同步 SQLite（白名单 / 成员同步 / 下载任务通知），沿用原 whitelist_bot 实现
# ---------------------------------------------------------------------------

def db():
    c = sqlite3.connect(DB, timeout=60)
    c.row_factory = sqlite3.Row
    c.execute('PRAGMA busy_timeout=60000')
    c.execute('PRAGMA journal_mode=WAL')
    c.executescript('''
    CREATE TABLE IF NOT EXISTS whitelist_users(
        user_id TEXT PRIMARY KEY, username TEXT, added_by TEXT, created_at DATETIME NOT NULL
    );
    CREATE TABLE IF NOT EXISTS server_download_quota(
        user_id TEXT PRIMARY KEY, quota INTEGER DEFAULT 1
    );
    CREATE TABLE IF NOT EXISTS user_server_access(
        user_id TEXT NOT NULL, server_id TEXT NOT NULL, granted_by TEXT,
        created_at DATETIME NOT NULL, PRIMARY KEY(user_id,server_id)
    );
    CREATE TABLE IF NOT EXISTS portal_users(
        user_id TEXT PRIMARY KEY, username TEXT NOT NULL, nickname TEXT,
        avatar_url TEXT, last_login DATETIME
    );
    CREATE TABLE IF NOT EXISTS user_server_presence(
        user_id TEXT NOT NULL, server_id TEXT NOT NULL,
        first_seen DATETIME NOT NULL, last_seen DATETIME NOT NULL,
        PRIMARY KEY(user_id,server_id)
    );
    CREATE TABLE IF NOT EXISTS member_sync_requests(
        id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id TEXT NOT NULL,
        requested_by TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
        created_at DATETIME NOT NULL, finished_at DATETIME, error TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_member_sync_requests_status
        ON member_sync_requests(status,created_at);
    CREATE TABLE IF NOT EXISTS download_tasks(
        id INTEGER PRIMARY KEY AUTOINCREMENT,guild_id TEXT NOT NULL,forum_channel_id TEXT NOT NULL,
        created_by TEXT NOT NULL,status TEXT NOT NULL DEFAULT 'pending',total INTEGER DEFAULT 0,
        completed INTEGER DEFAULT 0,started_at DATETIME,finished_at DATETIME,estimated_seconds INTEGER DEFAULT 0,
        elapsed_seconds INTEGER DEFAULT 0,message TEXT,error TEXT,notified_at DATETIME,created_at DATETIME NOT NULL
    );
    ''')
    return c


def admin(uid):
    return str(uid) in ADMINS


def is_whitelist(uid):
    c = db()
    row = c.execute('SELECT 1 FROM whitelist_users WHERE user_id=?', (str(uid),)).fetchone()
    c.close()
    return bool(row)


def can_sync(uid, guild_id):
    if admin(uid):
        return True
    c = db()
    row = c.execute('''SELECT 1 FROM whitelist_users w
                       WHERE w.user_id=? AND EXISTS(
                           SELECT 1 FROM user_server_access a
                           WHERE a.user_id=w.user_id AND a.server_id=?
                       )''', (str(uid), str(guild_id))).fetchone()
    if row:
        c.close()
        return True
    row = c.execute('SELECT 1 FROM download_configs WHERE guild_id=? AND owner_user_id=? AND enabled=1 LIMIT 1', (str(guild_id), str(uid))).fetchone()
    c.close()
    return bool(row)


def upsert_member(guild_id, member):
    now = datetime.now(timezone.utc).isoformat()
    username = str(member.name or member.id)
    nickname = str(member.display_name or member.name or member.id)
    avatar = None
    try:
        avatar = member.display_avatar.url if member.display_avatar else None
    except Exception:
        avatar = None
    c = db()
    c.execute('''INSERT INTO portal_users(user_id,username,nickname,avatar_url,last_login)
                 VALUES(?,?,?,?,NULL)
                 ON CONFLICT(user_id) DO UPDATE SET
                 username=excluded.username,
                 nickname=excluded.nickname,
                 avatar_url=COALESCE(excluded.avatar_url,portal_users.avatar_url)''',
              (str(member.id), username, nickname, avatar))
    c.execute('''INSERT INTO user_server_presence(user_id,server_id,first_seen,last_seen)
                 VALUES(?,?,?,?)
                 ON CONFLICT(user_id,server_id) DO UPDATE SET last_seen=excluded.last_seen''',
              (str(member.id), str(guild_id), now, now))
    c.commit()
    c.close()


async def sync_guild_members(guild, request_id=None):
    if request_id:
        c = db()
        c.execute("UPDATE member_sync_requests SET status='running',error=NULL WHERE id=?", (request_id,))
        c.commit(); c.close()
    count = 0
    try:
        # fetch_members 使用 Members Intent 分页获取完整成员名单，不依赖本地缓存。
        async for member in guild.fetch_members(limit=None):
            upsert_member(guild.id, member)
            count += 1
            if count % 500 == 0:
                print(f'[members] guild={guild.id} synced={count}')
                await asyncio.sleep(0)
        if request_id:
            c = db()
            c.execute("UPDATE member_sync_requests SET status='completed',finished_at=? WHERE id=?", (datetime.now(timezone.utc).isoformat(), request_id))
            c.commit(); c.close()
        print(f'[members] guild={guild.id} sync completed count={count}')
        return count
    except Exception as exc:
        if request_id:
            c = db()
            c.execute("UPDATE member_sync_requests SET status='failed',finished_at=?,error=? WHERE id=?", (datetime.now(timezone.utc).isoformat(), str(exc), request_id))
            c.commit(); c.close()
        print(f'[members] guild={guild.id} sync failed: {exc}')
        raise


# ---------------------------------------------------------------------------
# 异步 SQLite（收藏功能，原 favorite_bot.py），与白名单共用同一个 portal.db 文件
# ---------------------------------------------------------------------------

class SQLiteResult:
    """给数据库 execute 调用提供 rowcount。"""

    def __init__(self, rowcount: int):
        self.rowcount = rowcount


class SQLiteConnection:
    """对 aiosqlite 连接做一层很小的兼容封装。"""

    def __init__(self, conn: aiosqlite.Connection):
        self.conn = conn

    @staticmethod
    def _params(params):
        converted = []
        for value in params:
            if isinstance(value, datetime):
                converted.append(value.astimezone(timezone.utc).isoformat())
            elif isinstance(value, bool):
                converted.append(1 if value else 0)
            else:
                converted.append(value)
        return tuple(converted)

    async def execute(self, sql: str, *params):
        cursor = await self.conn.execute(sql, self._params(params))
        return SQLiteResult(cursor.rowcount)

    async def fetchrow(self, sql: str, *params):
        cursor = await self.conn.execute(sql, self._params(params))
        return await cursor.fetchone()

    async def fetchval(self, sql: str, *params):
        cursor = await self.conn.execute(sql, self._params(params))
        row = await cursor.fetchone()
        return row[0] if row is not None else None

    async def fetch(self, sql: str, *params):
        cursor = await self.conn.execute(sql, self._params(params))
        return await cursor.fetchall()


class _SQLiteAcquire:
    def __init__(self, pool):
        self.pool = pool

    async def __aenter__(self):
        return self.pool.conn_wrapper

    async def __aexit__(self, exc_type, exc, tb):
        if exc_type is None:
            await self.pool.conn.commit()
        else:
            await self.pool.conn.rollback()


class SQLitePool:
    """轻量 SQLite 连接池兼容层；本项目使用单个 SQLite 连接即可。"""

    def __init__(self, conn: aiosqlite.Connection):
        self.conn = conn
        self.conn_wrapper = SQLiteConnection(conn)

    def acquire(self):
        return _SQLiteAcquire(self)

    async def close(self):
        await self.conn.close()


async def open_database() -> SQLitePool:
    os.makedirs(os.path.dirname(os.path.abspath(DB)), exist_ok=True)

    conn = await aiosqlite.connect(DB)
    conn.row_factory = aiosqlite.Row

    await conn.execute("PRAGMA foreign_keys = ON")
    await conn.execute("PRAGMA journal_mode = WAL")
    await conn.execute("PRAGMA busy_timeout = 60000")
    await conn.commit()

    log.info("收藏数据库已打开（与 portal.db 共用）：%s", DB)
    return SQLitePool(conn)


def thread_url(guild_id: int, thread_id: int) -> str:
    return f"https://discord.com/channels/{guild_id}/{thread_id}"


async def is_forum_thread(message: discord.Message) -> bool:
    """
    可靠判断消息是否位于 Discord Forum 帖子。

    不只依赖本地缓存的 message.channel.parent：
    - 优先使用当前 channel 对象；
    - 如果 parent 信息缺失，则通过 guild.fetch_channel() 重新获取 Thread；
    - 再通过 parent_id 获取真正的 Forum Channel。

    这样同一个机器人可以同时处理多个服务器中的 Forum。
    """
    channel = message.channel

    if isinstance(channel, discord.Thread):
        parent = channel.parent

        if parent is not None and parent.type == discord.ChannelType.forum:
            return True

        parent_id = getattr(channel, "parent_id", None)
        if parent_id and message.guild is not None:
            try:
                parent_channel = message.guild.get_channel(parent_id)
                if parent_channel is None:
                    parent_channel = await message.guild.fetch_channel(parent_id)

                if parent_channel is not None:
                    return parent_channel.type == discord.ChannelType.forum
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                pass

        if message.guild is not None:
            try:
                fresh_channel = await message.guild.fetch_channel(channel.id)
                if isinstance(fresh_channel, discord.Thread):
                    parent_id = getattr(fresh_channel, "parent_id", None)

                    if fresh_channel.parent is not None:
                        return (
                            fresh_channel.parent.type
                            == discord.ChannelType.forum
                        )

                    if parent_id:
                        parent_channel = message.guild.get_channel(parent_id)
                        if parent_channel is None:
                            parent_channel = await message.guild.fetch_channel(
                                parent_id
                            )

                        return (
                            parent_channel is not None
                            and parent_channel.type == discord.ChannelType.forum
                        )
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                pass

    return False


def is_private_context(interaction: discord.Interaction) -> bool:
    return interaction.guild is not None


def help_embed() -> discord.Embed:
    embed = discord.Embed(
        title="📌 Discord 收藏机器人",
        description=(
            "用于收藏 Discord Forum 论坛帖子。\n\n"
            "**📌 收藏帖子**\n"
            "在论坛帖子中的任意消息上右键 → Apps → 📌 收藏帖子。\n\n"
            "**📕 取消收藏**\n"
            "右键帖子中的任意消息 → Apps → 📕 取消收藏。\n\n"
            "**📚 我的收藏**\n"
            "`/favorites`：服务器内查看当前服务器收藏；"
            "私信机器人时查看全部服务器收藏。\n\n"
            "**🏆 排行榜**\n"
            "`/top`：历史累计 Top 10。\n"
            "`/top30`：最近 30 天 Top 10。\n\n"
            "收藏关系不会公开给其他用户，排行榜只显示收藏数量。"
        ),
        color=discord.Color.blurple(),
    )
    embed.set_footer(text="简体中文 · English localization reserved")
    return embed


# ---------------------------------------------------------------------------
# 合并后的 Bot：一个 discord.Client + 一个 CommandTree，承载全部功能
# ---------------------------------------------------------------------------

class Bot(discord.Client):
    def __init__(self):
        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(self)
        self.db: Optional[SQLitePool] = None  # 收藏功能使用的异步连接

    async def setup_hook(self):
        # 收藏功能建表（与白名单共用 portal.db，白名单表由同步 db() 负责建表）
        self.db = await open_database()
        await self.init_favorites_db()

        # Discord Activities 应用会自动拥有一个 PRIMARY_ENTRY_POINT（Entry Point）命令。
        # discord.py 的 CommandTree 不会把这个 type=4 命令放进 bulk upsert payload，
        # 而 Discord 从 2026 年起禁止 bulk update 隐式删除 Entry Point，因此直接
        # await self.tree.sync() 会抛 HTTP 400 / 50240，导致整个 Bot 无法启动。
        #
        # 这里保留现有 Entry Point，不让它阻塞 Bot 启动。普通 slash/context
        # commands 仍按原方式同步；如果 Discord 已经存在 Entry Point，Discord 会
        # 保持它不变。
        try:
            await self.tree.sync()
            log.info("Discord 应用命令同步完成")
        except discord.HTTPException as exc:
            code = getattr(exc, "code", None)
            status = getattr(exc, "status", None)
            if code == 50240:
                log.warning(
                    "Discord 返回 50240：检测到 Activity Entry Point 命令。"
                    "已跳过本次全局 bulk sync，保留现有 Entry Point；"
                    "Bot 继续启动。若新增/修改了 slash 命令，请在 Discord 开发者后台"
                    "确认 Entry Point 后再重启同步。"
                )
                # bulk overwrite 会尝试删除 Activity 的 PRIMARY_ENTRY_POINT，
                # 因而 Discord 返回 50240。单独 upsert 普通命令不会触碰它，
                # 这里补注册新命令，确保 /restart 仍能在已有 Activity 应用中出现。
                await self._upsert_restart_command_without_entry_point()
            elif status == 429:
                retry_after = None
                try:
                    retry_after = exc.response.headers.get("Retry-After")
                except Exception:
                    pass
                log.warning(
                    "Discord API 当前受到速率限制，命令同步暂时跳过。Retry-After=%s。"
                    "Bot 不会因此退出。",
                    retry_after or "unknown",
                )
            else:
                log.exception("Discord 应用命令同步失败，但继续启动 Bot：%s", exc)

        self.member_sync_worker.start()
        self.download_notifications.start()

    async def _upsert_restart_command_without_entry_point(self) -> None:
        command = self.tree.get_command("restart")
        if command is None or self.application_id is None:
            return
        try:
            payload = (
                await command.get_translated_payload(self.tree, self.tree.translator)
                if self.tree.translator
                else command.to_dict(self.tree)
            )
            await self.tree._http.upsert_global_command(
                self.application_id, payload=payload
            )
            log.info("已单独注册 /restart（保留 Activity Entry Point）")
        except discord.HTTPException:
            log.exception("单独注册 /restart 失败")

    async def init_favorites_db(self) -> None:
        assert self.db is not None
        async with self.db.acquire() as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS favorite_bot_users (
                    user_id INTEGER PRIMARY KEY,
                    first_seen_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')),
                    last_seen_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')),
                    help_dm_sent INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS favorites (
                    user_id INTEGER NOT NULL,
                    guild_id INTEGER NOT NULL,
                    thread_id INTEGER NOT NULL,
                    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')),
                    PRIMARY KEY (user_id, thread_id)
                )
                """
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_favorites_guild_thread ON favorites (guild_id, thread_id)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_favorites_user_guild_created ON favorites (user_id, guild_id, created_at DESC)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_favorites_guild_created ON favorites (guild_id, created_at DESC)"
            )
        log.info("收藏数据表初始化完成")

    async def ensure_favorite_user(self, user: discord.abc.User) -> None:
        assert self.db is not None
        async with self.db.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT help_dm_sent FROM favorite_bot_users WHERE user_id=?",
                user.id,
            )
            if row is None:
                await conn.execute(
                    "INSERT INTO favorite_bot_users (user_id) VALUES (?)",
                    user.id,
                )
                first_use = True
            else:
                await conn.execute(
                    "UPDATE favorite_bot_users SET last_seen_at=strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now') WHERE user_id=?",
                    user.id,
                )
                first_use = False

        if first_use:
            try:
                await user.send(embed=help_embed())
                async with self.db.acquire() as conn:
                    await conn.execute(
                        "UPDATE favorite_bot_users SET help_dm_sent=1 WHERE user_id=?",
                        user.id,
                    )
            except (discord.Forbidden, discord.HTTPException) as exc:
                log.info("无法向首次使用用户 %s 发送 DM: %s", user.id, exc)

    async def on_ready(self):
        print(f'[bot] 已登录: {self.user} ({self.user.id}) | guilds={len(self.guilds)} | Members Intent={intents.members}')

    async def on_member_join(self, member):
        try:
            upsert_member(member.guild.id, member)
            print(f'[members] new member guild={member.guild.id} user={member.id}')
        except Exception as exc:
            print(f'[members] on_member_join failed: {exc}')

    async def on_member_update(self, before, after):
        try:
            upsert_member(after.guild.id, after)
        except Exception as exc:
            print(f'[members] on_member_update failed: {exc}')

    async def close(self):
        if self.db is not None:
            await self.db.close()
        await super().close()

    @tasks.loop(seconds=2)
    async def member_sync_worker(self):
        c = db()
        rows = c.execute("SELECT * FROM member_sync_requests WHERE status='pending' ORDER BY id LIMIT 2").fetchall()
        c.close()
        for row in rows:
            guild = self.get_guild(int(row['guild_id']))
            if guild is None:
                c = db(); c.execute("UPDATE member_sync_requests SET status='failed',finished_at=?,error=? WHERE id=?", (datetime.now(timezone.utc).isoformat(), '白名单机器人未加入该服务器', row['id'])); c.commit(); c.close()
                continue
            try:
                await sync_guild_members(guild, int(row['id']))
            except Exception:
                pass

    @tasks.loop(seconds=5)
    async def download_notifications(self):
        c = db()
        rows = c.execute("SELECT * FROM download_tasks WHERE status IN ('completed','failed') AND notified_at IS NULL ORDER BY id LIMIT 20").fetchall()
        for r in rows:
            try:
                user = await self.fetch_user(int(r['created_by']))
                if r['status'] == 'completed':
                    text = f"Discord 数据下载任务 #{r['id']} 已完成。\n服务器：{r['guild_id']}\n共下载：{r['completed']}/{r['total']} 个帖子。\n耗时：{r['elapsed_seconds'] or 0} 秒。"
                else:
                    text = f"Discord 数据下载任务 #{r['id']} 下载失败。\n服务器：{r['guild_id']}\n原因：{r['error'] or '未知错误'}"
                await user.send(text)
                c.execute("UPDATE download_tasks SET notified_at=? WHERE id=?", (datetime.now(timezone.utc).isoformat(), r['id']))
            except Exception as exc:
                c.execute("UPDATE download_tasks SET notified_at=? WHERE id=?", (datetime.now(timezone.utc).isoformat(), r['id']))
                print(f'[notify] task {r["id"]} failed: {exc}')
        c.commit(); c.close()


bot = Bot()


# ---------------------------------------------------------------------------
# 白名单 / 成员同步 / 下载通知 相关斜杠命令
# ---------------------------------------------------------------------------

@bot.tree.command(name='whitelist_add', description='添加 JSON 分析白名单用户')
async def whitelist_add(interaction: discord.Interaction, user: discord.User):
    if not admin(interaction.user.id):
        return await interaction.response.send_message('无权限', ephemeral=True)
    c = db()
    c.execute('INSERT OR REPLACE INTO whitelist_users(user_id,username,added_by,created_at) VALUES(?,?,?,?)', (str(user.id), str(user), str(interaction.user.id), datetime.now(timezone.utc).isoformat()))
    c.execute('INSERT OR IGNORE INTO server_download_quota(user_id,quota) VALUES(?,1)', (str(user.id),))
    c.commit(); c.close()
    await interaction.response.send_message(f'已加入白名单 {user}，默认服务器配额 1', ephemeral=True)


@bot.tree.command(name='whitelist_remove', description='移除 JSON 分析白名单用户')
async def whitelist_remove(interaction: discord.Interaction, user: discord.User):
    if not admin(interaction.user.id):
        return await interaction.response.send_message('无权限', ephemeral=True)
    c = db(); c.execute('DELETE FROM whitelist_users WHERE user_id=?', (str(user.id),)); c.execute('DELETE FROM server_download_quota WHERE user_id=?', (str(user.id),)); c.execute('DELETE FROM user_server_access WHERE user_id=?', (str(user.id),)); c.commit(); c.close()
    await interaction.response.send_message(f'已移除 {user}', ephemeral=True)


@bot.tree.command(name='whitelist_list', description='查看白名单')
async def whitelist_list(interaction: discord.Interaction):
    if not admin(interaction.user.id):
        return await interaction.response.send_message('无权限', ephemeral=True)
    c = db(); rows = c.execute('SELECT w.user_id,w.username,COALESCE(q.quota,1) FROM whitelist_users w LEFT JOIN server_download_quota q ON q.user_id=w.user_id ORDER BY w.created_at DESC').fetchall(); c.close()
    text = '\n'.join(f'{r[1]} `{r[0]}` 配额 {r[2]}' for r in rows) or '空'
    await interaction.response.send_message(text[:1900], ephemeral=True)


@bot.tree.command(name='server_access', description='给白名单用户授权服务器')
@app_commands.describe(user='白名单用户', server_id='Discord Server ID')
async def server_access(interaction: discord.Interaction, user: discord.User, server_id: str):
    if not admin(interaction.user.id):
        return await interaction.response.send_message('无权限', ephemeral=True)
    if not server_id.isdigit():
        return await interaction.response.send_message('服务器 ID 无效', ephemeral=True)
    c = db()
    if not c.execute('SELECT 1 FROM whitelist_users WHERE user_id=?', (str(user.id),)).fetchone():
        c.close(); return await interaction.response.send_message('该用户不在白名单，请先执行 /whitelist_add', ephemeral=True)
    c.execute('INSERT OR REPLACE INTO user_server_access(user_id,server_id,granted_by,created_at) VALUES(?,?,?,?)', (str(user.id), server_id, str(interaction.user.id), datetime.now(timezone.utc).isoformat()))
    c.commit(); c.close()
    await interaction.response.send_message(f'已授权 {user} 使用已导入服务器 {server_id}', ephemeral=True)


@bot.tree.command(name='quota', description='修改白名单用户服务器配额')
async def quota(interaction: discord.Interaction, user: discord.User, quota: int):
    if not admin(interaction.user.id):
        return await interaction.response.send_message('无权限', ephemeral=True)
    quota = max(1, min(100, quota)); c = db(); c.execute('INSERT INTO server_download_quota(user_id,quota) VALUES(?,?) ON CONFLICT(user_id) DO UPDATE SET quota=excluded.quota', (str(user.id), quota)); c.commit(); c.close()
    await interaction.response.send_message(f'{user} 配额已设置为 {quota}', ephemeral=True)


@bot.tree.command(name='restart', description='重启主机器人（仅 .env 管理员）')
async def restart(interaction: discord.Interaction):
    if not admin(interaction.user.id):
        return await interaction.response.send_message('无权限：只有 .env 中的 ADMIN_IDS 可以重启主机器人。', ephemeral=True)

    await interaction.response.send_message('主机器人将在 1 秒后重启。', ephemeral=True)
    log.warning('管理员 %s 请求重启主机器人', interaction.user.id)
    await asyncio.sleep(1)
    app_pid = os.getenv("V20_APP_PID", "").strip()
    restart_signal = os.getenv("V20_RESTART_SIGNAL", "").strip()
    if app_pid and restart_signal:
        try:
            os.kill(int(app_pid), int(restart_signal))
            log.warning('已请求主应用完整重启 app_pid=%s signal=%s', app_pid, restart_signal)
            return
        except (ValueError, ProcessLookupError, PermissionError, OSError):
            log.exception('通知主应用完整重启失败，将回退为仅重启机器人')
    try:
        os.execv(sys.executable, [sys.executable, os.path.abspath(__file__)])
    except Exception:
        log.exception('主机器人重启失败')
        await interaction.followup.send('重启失败，请查看机器人日志。', ephemeral=True)


@bot.tree.command(name='members_sync', description='更新服务器成员名单')
@app_commands.describe(server_id='Discord Server ID')
async def members_sync(interaction: discord.Interaction, server_id: str):
    if not server_id.isdigit():
        return await interaction.response.send_message('服务器 ID 无效', ephemeral=True)
    if not can_sync(interaction.user.id, server_id):
        return await interaction.response.send_message('你没有权限更新该服务器成员名单', ephemeral=True)
    guild = bot.get_guild(int(server_id))
    if guild is None:
        return await interaction.response.send_message('白名单机器人未加入该服务器', ephemeral=True)
    c = db()
    active = c.execute("SELECT id FROM member_sync_requests WHERE guild_id=? AND status IN ('pending','running') LIMIT 1", (server_id,)).fetchone()
    if active:
        c.close(); return await interaction.response.send_message(f'该服务器已有成员更新任务 #{active["id"]} 正在执行。', ephemeral=True)
    c.execute("INSERT INTO member_sync_requests(guild_id,requested_by,status,created_at) VALUES(?,?, 'pending', ?)", (server_id, str(interaction.user.id), datetime.now(timezone.utc).isoformat()))
    c.commit(); c.close()
    await interaction.response.send_message(f'服务器 {guild.name} 的成员名单更新已加入队列。', ephemeral=True)


# ---------------------------------------------------------------------------
# 收藏功能（原 favorite_bot.py）
# ---------------------------------------------------------------------------

async def ensure_favorite_user(interaction: discord.Interaction) -> None:
    await bot.ensure_favorite_user(interaction.user)


async def favorite_message(
    interaction: discord.Interaction,
    message: discord.Message,
) -> None:
    await ensure_favorite_user(interaction)
    ephemeral = is_private_context(interaction)

    # 先 defer，避免 is_forum_thread() 的网络请求 + 数据库查询
    # 耗时超过 3 秒导致 interaction token 失效（Unknown interaction）。
    await interaction.response.defer(ephemeral=ephemeral, thinking=True)

    if not await is_forum_thread(message):
        await interaction.followup.send(
            "❌ 这个消息不属于 Discord Forum 帖子。",
            ephemeral=ephemeral,
        )
        return

    if interaction.guild is None:
        await interaction.followup.send(
            "❌ 收藏帖子必须来自服务器。",
            ephemeral=True,
        )
        return

    assert bot.db is not None
    guild_id = interaction.guild.id
    thread_id = message.channel.id

    async with bot.db.acquire() as conn:
        result = await conn.execute(
            """
            INSERT INTO favorites (user_id, guild_id, thread_id)
            VALUES (?, ?, ?)
            ON CONFLICT (user_id, thread_id) DO NOTHING
            """,
            interaction.user.id,
            guild_id,
            thread_id,
        )

        count = await conn.fetchval(
            """
            SELECT COUNT(*)
            FROM favorites
            WHERE guild_id=? AND thread_id=?
            """,
            guild_id,
            thread_id,
        )

    url = thread_url(guild_id, thread_id)

    if result.rowcount == 1:
        title = "✅ 收藏成功"
        text = f"{url}\n\n📌 当前收藏：**{count}**"
        color = discord.Color.green()
    else:
        title = "📌 已经收藏"
        text = (
            f"你已经收藏过这个帖子。\n\n"
            f"{url}\n\n"
            f"📌 当前收藏：**{count}**"
        )
        color = discord.Color.blurple()

    await interaction.followup.send(
        embed=discord.Embed(
            title=title,
            description=text,
            color=color,
        ),
        ephemeral=ephemeral,
    )


async def unfavorite_message(
    interaction: discord.Interaction,
    message: discord.Message,
) -> None:
    await ensure_favorite_user(interaction)
    ephemeral = is_private_context(interaction)

    await interaction.response.defer(ephemeral=ephemeral, thinking=True)

    if not await is_forum_thread(message):
        await interaction.followup.send(
            "❌ 这个消息不属于 Discord Forum 帖子。",
            ephemeral=ephemeral,
        )
        return

    if interaction.guild is None:
        await interaction.followup.send(
            "❌ 取消收藏必须来自服务器。",
            ephemeral=True,
        )
        return

    assert bot.db is not None

    async with bot.db.acquire() as conn:
        result = await conn.execute(
            """
            DELETE FROM favorites
            WHERE user_id=? AND guild_id=? AND thread_id=?
            """,
            interaction.user.id,
            interaction.guild.id,
            message.channel.id,
        )

    text = (
        "📕 已取消收藏。"
        if result.rowcount == 1
        else "ℹ️ 你还没有收藏这个帖子。"
    )

    await interaction.followup.send(
        text,
        ephemeral=ephemeral,
    )


@app_commands.context_menu(name="📌 收藏帖子")
async def favorite_post(
    interaction: discord.Interaction,
    message: discord.Message,
) -> None:
    await favorite_message(interaction, message)


@app_commands.context_menu(name="📕 取消收藏")
async def unfavorite_post(
    interaction: discord.Interaction,
    message: discord.Message,
) -> None:
    await unfavorite_message(interaction, message)


bot.tree.add_command(favorite_post)
bot.tree.add_command(unfavorite_post)


class FavoritesView(discord.ui.View):
    def __init__(
        self,
        owner_id: int,
        guild_id: Optional[int],
        page: int = 0,
    ) -> None:
        super().__init__(timeout=180)
        self.owner_id = owner_id
        self.guild_id = guild_id
        self.page = page

    async def interaction_check(
        self,
        interaction: discord.Interaction,
    ) -> bool:
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message(
                "❌ 这个收藏列表不是你的。",
                ephemeral=True,
            )
            return False
        return True

    async def load(self):
        assert bot.db is not None
        offset = self.page * PER_PAGE

        async with bot.db.acquire() as conn:
            if self.guild_id is None:
                total = await conn.fetchval(
                    "SELECT COUNT(*) FROM favorites WHERE user_id=?",
                    self.owner_id,
                )
                rows = await conn.fetch(
                    """
                    SELECT guild_id, thread_id, created_at
                    FROM favorites
                    WHERE user_id=?
                    ORDER BY created_at DESC
                    LIMIT ? OFFSET ?
                    """,
                    self.owner_id,
                    PER_PAGE,
                    offset,
                )
            else:
                total = await conn.fetchval(
                    """
                    SELECT COUNT(*)
                    FROM favorites
                    WHERE user_id=? AND guild_id=?
                    """,
                    self.owner_id,
                    self.guild_id,
                )
                rows = await conn.fetch(
                    """
                    SELECT guild_id, thread_id, created_at
                    FROM favorites
                    WHERE user_id=? AND guild_id=?
                    ORDER BY created_at DESC
                    LIMIT ? OFFSET ?
                    """,
                    self.owner_id,
                    self.guild_id,
                    PER_PAGE,
                    offset,
                )

        pages = max(1, (total + PER_PAGE - 1) // PER_PAGE)
        return total, rows, pages

    async def refresh(self, interaction: discord.Interaction) -> None:
        total, rows, pages = await self.load()
        self.page = min(self.page, pages - 1)

        self.prev.disabled = self.page <= 0
        self.next.disabled = self.page >= pages - 1

        await interaction.response.edit_message(
            embed=favorites_embed(rows, total, self.page, pages),
            view=self,
        )

    @discord.ui.button(
        label="⬅",
        style=discord.ButtonStyle.secondary,
    )
    async def prev(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button,
    ) -> None:
        self.page -= 1
        await self.refresh(interaction)

    @discord.ui.button(
        label="➡",
        style=discord.ButtonStyle.secondary,
    )
    async def next(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button,
    ) -> None:
        self.page += 1
        await self.refresh(interaction)


def favorites_embed(
    rows,
    total: int,
    page: int,
    pages: int,
) -> discord.Embed:
    embed = discord.Embed(
        title="📚 我的收藏",
        description=f"共 **{total}** 个收藏",
        color=discord.Color.blurple(),
    )

    if not rows:
        embed.add_field(
            name="",
            value="📭 你还没有收藏任何帖子。",
            inline=False,
        )
    else:
        lines = []

        for index, row in enumerate(
            rows,
            start=page * PER_PAGE + 1,
        ):
            url = thread_url(
                row["guild_id"],
                row["thread_id"],
            )
            created = _parse_db_datetime(row["created_at"]).astimezone(
                timezone.utc
            ).strftime("%Y-%m-%d %H:%M UTC")

            lines.append(
                f"**{index}.** {url}\n"
                f"　收藏时间：`{created}`"
            )

        # Discord 单个 embed field 的 value 上限是 1024 字符。
        # PER_PAGE 条目拼在一起可能超出该限制，因此这里按长度
        # 拆分成多个 field，而不是硬塞进同一个 field 导致 400。
        MAX_FIELD_LEN = 1024
        chunk_lines: list[str] = []
        chunk_len = 0

        def flush_chunk() -> None:
            if chunk_lines:
                embed.add_field(
                    name="",
                    value="\n\n".join(chunk_lines),
                    inline=False,
                )

        for line in lines:
            # +2 对应拼接时使用的 "\n\n"
            added_len = len(line) + (2 if chunk_lines else 0)

            if chunk_len + added_len > MAX_FIELD_LEN:
                flush_chunk()
                chunk_lines = []
                chunk_len = 0
                added_len = len(line)

            chunk_lines.append(line)
            chunk_len += added_len

        flush_chunk()

    embed.set_footer(text=f"第 {page + 1} / {pages} 页")
    return embed


def _parse_db_datetime(value) -> datetime:
    """把 SQLite 中保存的时间字符串转换成带时区的 datetime。"""
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value

    if not value:
        return datetime.now(timezone.utc)

    text = str(value).strip()
    try:
        result = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        # 兼容 SQLite CURRENT_TIMESTAMP 产生的旧格式
        result = datetime.strptime(text, "%Y-%m-%d %H:%M:%S").replace(
            tzinfo=timezone.utc
        )

    if result.tzinfo is None:
        result = result.replace(tzinfo=timezone.utc)
    return result


@bot.tree.command(
    name="favorites",
    description="查看我的收藏",
)
async def favorites(
    interaction: discord.Interaction,
) -> None:
    await ensure_favorite_user(interaction)

    guild_id = (
        interaction.guild.id
        if interaction.guild
        else None
    )

    view = FavoritesView(
        owner_id=interaction.user.id,
        guild_id=guild_id,
    )

    total, rows, pages = await view.load()

    view.prev.disabled = True
    view.next.disabled = pages <= 1

    await interaction.response.send_message(
        embed=favorites_embed(rows, total, 0, pages),
        view=view,
        ephemeral=is_private_context(interaction),
    )


async def ranking(
    interaction: discord.Interaction,
    days: Optional[int],
) -> None:
    await ensure_favorite_user(interaction)

    if interaction.guild is None:
        await interaction.response.send_message(
            "❌ 排行榜只能在服务器中使用。",
            ephemeral=True,
        )
        return

    assert bot.db is not None
    guild_id = interaction.guild.id

    async with bot.db.acquire() as conn:
        if days is None:
            rows = await conn.fetch(
                """
                SELECT thread_id, COUNT(*) AS favorite_count
                FROM favorites
                WHERE guild_id=?
                GROUP BY thread_id
                ORDER BY favorite_count DESC, thread_id ASC
                LIMIT 10
                """,
                guild_id,
            )
            title = "🏆 服务器历史累计收藏 Top 10"
        else:
            since = datetime.now(timezone.utc) - timedelta(days=days)

            rows = await conn.fetch(
                """
                SELECT thread_id, COUNT(*) AS favorite_count
                FROM favorites
                WHERE guild_id=?
                  AND created_at >= ?
                GROUP BY thread_id
                ORDER BY favorite_count DESC, thread_id ASC
                LIMIT 10
                """,
                guild_id,
                since,
            )
            title = "🔥 服务器最近 30 天收藏 Top 10"

    embed = discord.Embed(
        title=title,
        color=discord.Color.gold(),
    )

    if not rows:
        embed.description = "📭 当前没有收藏数据。"
    else:
        medals = ["🥇", "🥈", "🥉"]
        lines = []

        for index, row in enumerate(rows, start=1):
            prefix = (
                medals[index - 1]
                if index <= 3
                else f"**{index}.**"
            )

            url = thread_url(
                guild_id,
                row["thread_id"],
            )

            lines.append(
                f"{prefix} {url}\n"
                f"　📌 **{row['favorite_count']}** 人收藏"
            )

        embed.description = "\n\n".join(lines)

    # 排行榜是服务器公开统计，因此不使用 ephemeral。
    await interaction.response.send_message(embed=embed)


@bot.tree.command(
    name="top",
    description="查看服务器历史累计收藏 Top 10",
)
async def top(
    interaction: discord.Interaction,
) -> None:
    await ranking(interaction, None)


@bot.tree.command(
    name="top30",
    description="查看服务器最近 30 天收藏 Top 10",
)
async def top30(
    interaction: discord.Interaction,
) -> None:
    await ranking(interaction, 30)


@bot.tree.command(
    name="help",
    description="查看收藏机器人帮助",
)
async def help_command(
    interaction: discord.Interaction,
) -> None:
    await ensure_favorite_user(interaction)

    await interaction.response.send_message(
        embed=help_embed(),
        ephemeral=is_private_context(interaction),
    )


@bot.tree.error
async def on_app_command_error(
    interaction: discord.Interaction,
    error: app_commands.AppCommandError,
) -> None:
    log.error(
        "Application Command Error: %r",
        error,
        exc_info=(type(error), error, error.__traceback__),
    )

    try:
        if interaction.response.is_done():
            await interaction.followup.send(
                "❌ 操作失败，请稍后再试。",
                ephemeral=is_private_context(interaction),
            )
        else:
            await interaction.response.send_message(
                "❌ 操作失败，请稍后再试。",
                ephemeral=is_private_context(interaction),
            )
    except discord.HTTPException:
        pass


if __name__ == '__main__':
    if not TOKEN:
        raise SystemExit('请设置 DISCORD_BOT_TOKEN')
    bot.run(TOKEN)
