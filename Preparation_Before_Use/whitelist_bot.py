"""白名单管理机器人 + 服务器成员名单同步 + Forum 帖子收藏 + 黑名单拦截机器人。

单个 Discord Bot 进程，同一个 Token，同一个 CommandTree，包含四部分功能：

白名单 / 成员同步：
- 创建下载任务后，网站写入 member_sync_requests，机器人自动同步目标服务器成员
- 新成员加入服务器时实时写入 portal.db
- 成员资料变化时实时更新
- Admin 或白名单用户可在后台或私聊执行 /members_sync server_id 手动同步
- /whitelist_add /whitelist_remove /whitelist_list /server_access /quota /restart

收藏：
- 右键 Forum 帖子内的消息 -> Apps -> 📌 收藏帖子 / 📕 取消收藏
- /favorites 查看我的收藏（服务器内看当前服务器，私信看全部服务器）
- /top /top30 查看收藏排行榜
- /help 查看收藏机器人使用说明

黑名单：
- /block /unblock 拉黑 / 解除用户
- 右键消息 -> Apps -> 拉黑此发言者 / 解除拉黑此发言者
- 双向拦截：被拉黑后双方不能互相回复、@提及，或在 Forum 跟帖中互动，
  触发的消息会被自动删除，并通过 DM 通知发送者
- 拦截次数会记录在 portal.db 中；/list 在展示黑名单的同时会显示每个用户
  被本机器人拦截的消息次数
- 同时支持「直接 Reply」和「Forum 帖子内的普通跟帖（默认目标 = 楼主）」两种
  触发场景

三类功能数据共用同一个 portal.db（PORTAL_DB）。
"""
import os
import sys
import asyncio
import logging
import signal
import subprocess
from datetime import datetime, timedelta, timezone
from typing import Optional

import aiosqlite
import discord
from discord import app_commands
from discord.ext import tasks

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from shared.env import load_local_env
from shared.portal import touch_user_presence, upsert_portal_user
from shared.sqlite_utils import connect_sqlite
from shared.timeutil import parse_utc_datetime, utc_now_iso
from shared.notify_policy import should_notify_finish, is_auto_poll
from shared.gateway_ingest import GatewayIngest
from shared import block_policy
from shared import liveness

load_local_env(os.path.join(BASE_DIR, '.env'))

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
# 黑名单拦截需要读取消息内容来识别「引用回复 / Forum 跟帖」并把原文 DM 回给发送者。
intents.message_content = True


# ---------------------------------------------------------------------------
# 同步 SQLite（白名单 / 成员同步 / 下载任务通知）
# ---------------------------------------------------------------------------

def db():
    c = connect_sqlite(DB, synchronous=None)
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
    now = utc_now_iso()
    username = str(member.name or member.id)
    nickname = str(member.display_name or member.name or member.id)
    avatar = None
    try:
        avatar = member.display_avatar.url if member.display_avatar else None
    except Exception as exc:
        log.warning("读取成员头像失败 guild=%s user=%s error=%s", guild_id, member.id, exc)
        avatar = None
    c = db()
    upsert_portal_user(c, member.id, username, nickname, avatar)
    touch_user_presence(c, member.id, guild_id, now)
    c.commit()
    c.close()


async def sync_guild_members(guild, request_id=None):
    if request_id:
        c = db()
        c.execute("UPDATE member_sync_requests SET status='running',error=NULL WHERE id=?", (request_id,))
        c.commit(); c.close()
    count = 0
    seen = []
    try:
        # fetch_members 使用 Members Intent 分页获取完整成员名单，不依赖本地缓存。
        async for member in guild.fetch_members(limit=None):
            upsert_member(guild.id, member)
            seen.append(str(member.id))
            count += 1
            if count % 500 == 0:
                log.info("成员同步中 guild=%s synced=%s", guild.id, count)
                await asyncio.sleep(0)
        # 以 Discord 实时名单为准：删掉已退服的 presence，并写入 servers.member_count。
        c = db()
        try:
            c.execute("ALTER TABLE servers ADD COLUMN member_count INTEGER")
        except Exception:
            pass
        if seen:
            # chunk NOT IN deletes for large guilds
            stale = c.execute("SELECT user_id FROM user_server_presence WHERE server_id=?", (str(guild.id),)).fetchall()
            keep = set(seen)
            drop = [r[0] for r in stale if str(r[0]) not in keep]
            for i in range(0, len(drop), 400):
                chunk = drop[i:i+400]
                ph = ",".join("?" for _ in chunk)
                c.execute(f"DELETE FROM user_server_presence WHERE server_id=? AND user_id IN ({ph})", (str(guild.id), *chunk))
            c.execute(
                "UPDATE servers SET member_count=?, updated_at=COALESCE(updated_at, ?) WHERE server_id=?",
                (count, utc_now_iso(), str(guild.id)),
            )
            # 若 servers 行尚不存在，不强制插入（下载完成后才会有）；presence 已是权威名单。
        c.commit()
        c.close()
        if request_id:
            c = db()
            c.execute("UPDATE member_sync_requests SET status='completed',finished_at=? WHERE id=?", (utc_now_iso(), request_id))
            c.commit(); c.close()
        log.info("成员同步完成 guild=%s count=%s pruned=%s", guild.id, count, len(drop) if seen else 0)
        return count
    except Exception as exc:
        if request_id:
            c = db()
            c.execute("UPDATE member_sync_requests SET status='failed',finished_at=?,error=? WHERE id=?", (utc_now_iso(), str(exc), request_id))
            c.commit(); c.close()
        log.exception("成员同步失败 guild=%s error=%s", guild.id, exc)
        raise


# ---------------------------------------------------------------------------
# 异步 SQLite（收藏功能），与白名单共用同一个 portal.db 文件
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
            except (discord.NotFound, discord.Forbidden, discord.HTTPException) as exc:
                log.debug("获取 Forum 父频道失败 guild=%s channel=%s error=%s", message.guild.id, parent_id, exc)

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
            except (discord.NotFound, discord.Forbidden, discord.HTTPException) as exc:
                log.debug("刷新 Forum 帖子频道失败 guild=%s channel=%s error=%s", message.guild.id, channel.id, exc)

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

# ---------------------------------------------------------------------------
# 「还在吗？」活跃确认（shared/liveness.py；portal.db liveness_checks）
# ---------------------------------------------------------------------------

class AliveButton(discord.ui.DynamicItem[discord.ui.Button], template=r'liveness:(?P<token>[0-9a-f]{8,64})'):
    """持久化按钮：custom_id 带 token，timeout=None；setup_hook 中 add_dynamic_items 注册，重启后仍可点击。"""

    def __init__(self, token: str):
        super().__init__(discord.ui.Button(label='我还在', style=discord.ButtonStyle.success,
                                           emoji='👋', custom_id=f'liveness:{token}'))
        self.token = token

    @classmethod
    async def from_custom_id(cls, interaction, item, match):
        return cls(match['token'])

    async def callback(self, interaction: discord.Interaction):
        c = db()
        try:
            row = liveness.by_token(c, self.token)
        finally:
            c.close()
        if row is None:
            return await interaction.response.send_message('该确认已失效。', ephemeral=True)
        if str(row['user_id']) != str(interaction.user.id):
            return await interaction.response.send_message('这个按钮不是发给你的。', ephemeral=True)
        await interaction.response.defer(ephemeral=True, thinking=True)
        text = await interaction.client.liveness_confirm(int(row['guild_id']), interaction.user.id)
        await interaction.followup.send(text, ephemeral=True)


def liveness_view(token: str) -> discord.ui.View:
    view = discord.ui.View(timeout=None)
    view.add_item(AliveButton(token))
    return view


class Bot(discord.Client):
    def __init__(self):
        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(self)
        self.db: Optional[SQLitePool] = None  # 收藏功能使用的异步连接
        # 网关实时入库：on_message/编辑/删除/反应直接写 data/servers/<gid>/discord_data.db
        self.ingest = GatewayIngest(BASE_DIR, DB)

    async def setup_hook(self):
        # 收藏建表（与白名单共用 portal.db，白名单表由同步 db() 建表）
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
                except Exception as retry_exc:
                    log.debug("读取命令同步限流响应头失败 error=%s", retry_exc)
                log.warning(
                    "Discord API 当前受到速率限制，命令同步暂时跳过。Retry-After=%s。"
                    "Bot 不会因此退出。",
                    retry_after or "unknown",
                )
            else:
                log.exception("Discord 应用命令同步失败，但继续启动 Bot：%s", exc)

        self.member_sync_worker.start()
        self.download_notifications.start()
        c = db()
        try:
            liveness.ensure(c)
        finally:
            c.close()
        self.add_dynamic_items(AliveButton)  # 重启后旧私信里的「我还在」按钮仍然有效
        self.liveness_sender.start()
        self.liveness_expirer.start()

    async def _upsert_restart_command_without_entry_point(self) -> None:
        """Discord 50240 时单独 upsert 一组命令。

        当 Activity Entry Point 命令存在时，bulk sync() 会被 Discord 拒绝
        （会尝试隐式删除 Entry Point）。但单个命令的 upsert 不会触碰 Entry Point，
        因此这里把所有普通 slash/context 命令单独 upsert 一遍，保证新增的
        `/block` `/unblock` `/list` 等不会被遗漏。
        """
        if self.application_id is None:
            return

        # 使用 discord.py 内部的 _get_all_commands() 取得全部待注册命令
        # （包括 context menus 和 Group 下的子命令），并跳过 Activity Entry Point
        # （type=4 = AppCommandType.chat_input 但通过 _context_menus 注册的那一类）。
        all_commands = self.tree._get_all_commands(guild=None)
        # 排除 Activity Entry Point（PRIMARY_ENTRY_POINT）。Entry Point 在
        # discord.py 中以 type=4 注册在 _context_menus 中，但 bot.tree.sync()
        # 不负责维护它——它由 Activity SDK 维护。这里用 to_dict() 后看 type
        # 字段来排除 type==4 的项。
        commands = []
        for cmd in all_commands:
            try:
                payload = cmd.to_dict(self.tree)
            except Exception:
                payload = None
            if payload and payload.get("type") == 4:
                continue
            commands.append(cmd)

        success = 0
        for command in commands:
            try:
                cmd_payload = (
                    await command.get_translated_payload(self.tree, self.tree.translator)
                    if self.tree.translator
                    else command.to_dict(self.tree)
                )
                await self.tree._http.upsert_global_command(
                    self.application_id, payload=cmd_payload
                )
                success += 1
            except discord.HTTPException as exc:
                log.exception(
                    "单独注册命令 %s 失败 error=%s",
                    getattr(command, "name", "?"),
                    exc,
                )
        log.info(
            "已跳过 Activity Entry Point，单独 upsert %s/%s 条普通命令",
            success,
            len(commands),
        )

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
            # 黑名单关系表 + 拦截次数计数
            # 使用 portal.db 同一文件，不再单独维护 blocklist.db。
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS user_blocks (
                    blocker_id INTEGER NOT NULL,
                    blocked_id INTEGER NOT NULL,
                    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')),
                    PRIMARY KEY (blocker_id, blocked_id)
                )
                """
            )
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS block_interception_counts (
                    guild_id INTEGER NOT NULL,
                    sender_id INTEGER NOT NULL,
                    target_id INTEGER NOT NULL,
                    count INTEGER NOT NULL DEFAULT 0,
                    last_blocked_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')),
                    PRIMARY KEY (guild_id, sender_id, target_id)
                )
                """
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_user_blocks_blocker ON user_blocks (blocker_id)"
            )
        log.info("收藏数据表初始化完成（含黑名单表）")

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

    # ---- 权限自检：登录后检查各服务器权限与特权 Intent，缺失则写日志并通知 ADMIN_IDS ----

    async def check_permissions(self):
        """检查每个已加入服务器的关键权限；缺失时记录日志并私信通知管理员。"""
        problems = []
        for guild in self.guilds:
            me = guild.me
            if me is None:
                continue
            perms = me.guild_permissions
            lacking = []
            if not perms.view_channel: lacking.append("view_channel")
            if not perms.read_message_history: lacking.append("read_message_history")
            if not perms.send_messages: lacking.append("send_messages")
            if not perms.manage_messages: lacking.append("manage_messages（拉黑拦截删除消息需要）")
            if guild.id == liveness.LIVENESS_GUILD_ID:
                role = guild.get_role(liveness.LIVENESS_ROLE_ID)
                if not perms.manage_roles:
                    lacking.append("manage_roles（不活跃身份组需要）")
                if role is None:
                    lacking.append(f"不活跃身份组 {liveness.LIVENESS_ROLE_ID} 不存在")
                elif me.top_role <= role:
                    lacking.append(f"机器人最高身份组需高于不活跃身份组 {liveness.LIVENESS_ROLE_ID}")
            if lacking:
                problems.append((guild, lacking))
                log.warning("权限不足 guild=%s(%s) 缺少: %s", guild.name, guild.id, ", ".join(lacking))
        # 特权 Intent 检查：尝试拉取一个成员列表验证 members intent 实际生效
        for guild in self.guilds:
            try:
                async for _ in guild.fetch_members(limit=2).map(lambda m: m):
                    break
            except discord.Forbidden:
                log.error(
                    "Members Intent 未在开发者后台启用或机器人无成员查看权限 guild=%s(%s)。"
                    "请到 https://discord.com/developers/applications/ -> Bot -> Privileged Gateway Intents 开启 SERVER MEMBERS INTENT。",
                    guild.name, guild.id,
                )
                problems.append((guild, ["SERVER MEMBERS INTENT 未启用"]))
            except Exception:
                pass  # 网络/限流等其他错误不作为权限问题报告
            break  # 只测第一个服务器即可代表 token 级 intent 配置
        await self.notify_admins_permission_problems(problems)

    async def notify_admins_permission_problems(self, problems):
        if not problems or not ADMINS:
            return
        lines = ["⚠️ 白名单机器人权限自检发现问题："]
        for guild, lacking in problems:
            lines.append(f"- {guild.name} (`{guild.id}`)：缺少 {'、'.join(lacking)}")
        lines.append("请到 Discord 开发者后台 -> Bot -> Privileged Gateway Intents 检查，或在服务器设置中补全角色权限。")
        content = "\n".join(lines)
        for aid in ADMINS:
            try:
                user = self.get_user(int(aid)) or await self.fetch_user(int(aid))
                if user:
                    await user.send(content[:1900])
            except Exception as exc:
                log.info("权限问题通知管理员失败 admin=%s error=%s", aid, exc)

    async def on_ready(self):
        print(f'[bot] 已登录: {self.user} ({self.user.id}) | guilds={len(self.guilds)} | Members Intent={intents.members}')
        try:
            await self.check_permissions()
        except Exception:
            log.exception("权限自检执行失败")
        try:
            for line in await asyncio.to_thread(self.ingest.presence_report, self):
                log.info("网关入库自检 %s | message_content=%s", line, intents.message_content)
            # on_ready 在首次登录和无法 RESUME 的重连后触发：补齐离线期间的数据（静默任务）。
            await asyncio.to_thread(self.ingest.enqueue_backfill)
            log.info("thread_meta sync active threads=%s", await self.ingest.sync_active_threads(self))
        except Exception:
            log.exception("网关入库自检/补齐入队失败")

    async def on_raw_message_edit(self, payload):
        await self.ingest.on_raw_message_edit(payload, self)
        try:
            await self._enforce_blocks_on_edit(payload)
        except Exception:
            log.exception("编辑消息拉黑拦截失败 message=%s", payload.message_id)

    async def on_raw_message_delete(self, payload):
        await self.ingest.on_raw_message_delete(payload, self)

    async def on_thread_create(self, thread):
        await self.ingest.on_thread_upsert(thread)

    async def on_thread_update(self, before, after):
        await self.ingest.on_thread_upsert(after)

    async def on_raw_thread_delete(self, payload):
        await self.ingest.on_raw_thread_delete(payload)

    async def on_raw_bulk_message_delete(self, payload):
        await self.ingest.on_raw_bulk_message_delete(payload, self)

    async def on_raw_reaction_add(self, payload):
        await self.ingest.on_raw_reaction_add(payload, self)

    async def on_raw_reaction_remove(self, payload):
        await self.ingest.on_raw_reaction_remove(payload, self)

    async def on_raw_reaction_clear(self, payload):
        await self.ingest.on_raw_reaction_clear(payload, self)

    async def on_raw_reaction_clear_emoji(self, payload):
        await self.ingest.on_raw_reaction_clear_emoji(payload, self)

    async def on_member_join(self, member):
        try:
            upsert_member(member.guild.id, member)
            c = db()
            try:
                c.execute("ALTER TABLE servers ADD COLUMN member_count INTEGER")
            except Exception:
                pass
            c.execute("UPDATE servers SET member_count=COALESCE(member_count,0)+1 WHERE server_id=?", (str(member.guild.id),))
            c.commit(); c.close()
            log.info("新增成员已同步 guild=%s user=%s", member.guild.id, member.id)
        except Exception as exc:
            log.exception("新增成员同步失败 guild=%s user=%s error=%s", member.guild.id, member.id, exc)

    async def on_member_remove(self, member):
        try:
            c = db()
            c.execute("DELETE FROM user_server_presence WHERE user_id=? AND server_id=?", (str(member.id), str(member.guild.id)))
            try:
                c.execute("ALTER TABLE servers ADD COLUMN member_count INTEGER")
            except Exception:
                pass
            c.execute("UPDATE servers SET member_count=CASE WHEN COALESCE(member_count,0)>0 THEN member_count-1 ELSE 0 END WHERE server_id=?", (str(member.guild.id),))
            c.commit(); c.close()
            log.info("成员离开已同步 guild=%s user=%s", member.guild.id, member.id)
        except Exception as exc:
            log.exception("成员离开同步失败 guild=%s user=%s error=%s", member.guild.id, member.id, exc)

    async def on_member_update(self, before, after):
        try:
            upsert_member(after.guild.id, after)
        except Exception as exc:
            log.exception("成员更新同步失败 guild=%s user=%s error=%s", after.guild.id, after.id, exc)

    # ---- 黑名单拦截（原 Blockbot.py 合并进来）----

    async def _increment_block_count(self, guild_id: int, sender_id: int, target_id: int) -> None:
        """拦截触发后写入 / 自增计数，供 /list 展示。"""
        if self.db is None:
            return
        try:
            async with self.db.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO block_interception_counts (guild_id, sender_id, target_id, count, last_blocked_at)
                    VALUES (?, ?, ?, 1, strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now'))
                    ON CONFLICT (guild_id, sender_id, target_id) DO UPDATE SET
                        count = count + 1,
                        last_blocked_at = strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')
                    """,
                    guild_id,
                    sender_id,
                    target_id,
                )
        except Exception as exc:
            log.exception("写入黑名单拦截次数失败 guild=%s sender=%s target=%s error=%s",
                          guild_id, sender_id, target_id, exc)

    @staticmethod
    def _check_block_relation(rows, sender_id: int, recipient_id: int):
        """根据 user_blocks 查询结果判定双向拦截关系。

        返回：
          - 'sender_blocked_recipient': 发送者主动拉黑了接收者
          - 'recipient_blocked_sender': 接收者拉黑了发送者
          - 'mutual': 双方互为拉黑
          - None: 无任何拉黑关系
        """
        if not rows:
            return None
        blockers = {row[0] for row in rows}
        if sender_id in blockers and recipient_id in blockers:
            return "mutual"
        if sender_id in blockers:
            return "sender_blocked_recipient"
        return "recipient_blocked_sender"

    async def _block_relation_ids(self, user_a: int, user_b: int):
        """查询 user_a 与 user_b 之间任意方向的拉黑关系。

        返回 'sender_blocked_recipient' / 'recipient_blocked_sender' / 'mutual' / None。
        """
        if self.db is None:
            return None
        try:
            async with self.db.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT blocker_id FROM user_blocks
                    WHERE (blocker_id = ? AND blocked_id = ?)
                       OR (blocker_id = ? AND blocked_id = ?)
                    """,
                    user_a,
                    user_b,
                    user_b,
                    user_a,
                )
        except Exception as exc:
            log.exception("查询黑名单关系失败 error=%s", exc)
            return None
        return self._check_block_relation(rows, user_a, user_b)

    async def _handle_third_party_bot_message(self, message: discord.Message) -> None:
        """处理第三方机器人消息：若由与帖子楼主存在拉黑关系的用户触发，删除该回复。

        Discord 上 slash 命令调用本身不是消息、无法拦截；但第三方 bot 的
        （非 ephemeral）回复消息带有 interaction 元数据，可以定位到触发者。
        这里利用它实现「被拉黑者在楼主帖内调用其他机器人时，bot 回复被自动删除」。
        """
        # 我们自己的消息（拦截通知等）永不删除，避免自删循环
        if message.author.id == self.user.id:
            return
        if not isinstance(message.channel, discord.Thread):
            return
        parent = message.channel.parent
        if parent is None or parent.type != discord.ChannelType.forum:
            return

        # 定位触发命令的用户
        invoker = None
        meta = getattr(message, "interaction_metadata", None)
        if meta is not None:
            invoker = getattr(meta, "user", None)
        if invoker is None:
            legacy = getattr(message, "interaction", None)  # 旧版字段兜底
            if legacy is not None:
                invoker = getattr(legacy, "user", None)
        if invoker is None or invoker.bot:
            return

        owner_id = message.channel.owner_id
        targets = block_policy.collect_targets(
            invoker.id, owner_id=owner_id, reply_author_id=await self._reply_author_id(message),
            mention_ids=[u.id for u in message.mentions if not u.bot])
        hit = block_policy.pick(await self._block_pairs(invoker.id, targets), invoker.id, targets)
        if not hit:
            return
        target_id, relation = hit
        who = "帖子楼主" if target_id == owner_id else "对方"

        channel_mention = message.channel.mention
        if relation == "sender_blocked_recipient":
            reason = f"你已将{who}拉黑，机器人命令的执行结果被自动删除。"
        elif relation == "recipient_blocked_sender":
            reason = f"{who}已将你拉黑，机器人命令的执行结果被自动删除。"
        else:
            reason = f"你与{who}处于相互拉黑状态，机器人命令的执行结果被自动删除。"

        ok = await self._delete_intercepted(message, "third-party-bot")
        log.info("拉黑拦截(第三方bot) guild=%s channel=%s invoker=%s target=%s relation=%s deleted=%s",
                 message.guild.id if message.guild else 0, message.channel.id, invoker.id, target_id, relation, ok)

        if message.guild:
            await self._increment_block_count(message.guild.id, invoker.id, target_id)

        try:
            dm = await invoker.create_dm()
            await dm.send(
                f"⚠️ **机器人命令执行结果已被拦截**\n"
                f"你在帖子 {channel_mention} 中调用的机器人命令，其回复已被自动删除。\n"
                f"**拦截原因：** {reason}"
            )
        except discord.Forbidden:
            pass
        except discord.HTTPException as exc:
            log.debug("向第三方 bot 触发者发送 DM 失败 user=%s error=%s", invoker.id, exc)

    async def on_message(self, message: discord.Message):
        # 私信不走拦截
        if not message.guild:
            return
        # 先入库（被拦截删除的消息随后由 on_raw_message_delete 以墓碑方式移除）
        await self.ingest.on_message(message)

        # 第三方 bot 消息：检查是否由被拉黑用户触发（在楼主帖内调用其他机器人）
        if message.author.bot:
            await self._handle_third_party_bot_message(message)
            return

        await self._enforce_blocks(message)

    # ------------------------------------------------------------------ 拉黑拦截
    async def _block_pairs(self, sender_id: int, targets):
        if self.db is None or not targets:
            return []
        sql, args = block_policy.query(sender_id, targets)
        try:
            async with self.db.acquire() as conn:
                return [(r[0], r[1]) for r in await conn.fetch(sql, *args)]
        except Exception as exc:
            log.exception("查询黑名单关系失败 sender=%s error=%s", sender_id, exc)
            return []

    async def _reply_author_id(self, message: discord.Message):
        if not (message.reference and message.reference.message_id):
            return None
        ref = message.reference.resolved
        if isinstance(ref, discord.Message):
            return ref.author.id
        try:
            ref = await message.channel.fetch_message(message.reference.message_id)
            return ref.author.id
        except Exception:
            return None

    @staticmethod
    def _forum_owner_id(channel):
        if isinstance(channel, discord.Thread):
            parent = channel.parent
            if parent is not None and parent.type == discord.ChannelType.forum:
                return channel.owner_id
        return None

    async def _delete_intercepted(self, message: discord.Message, what: str) -> bool:
        try:
            await message.delete()
            return True
        except discord.NotFound:
            return True
        except discord.Forbidden as exc:
            log.warning("拦截删除失败（缺少 Manage Messages 权限）%s guild=%s channel=%s message=%s error=%s",
                        what, message.guild.id if message.guild else 0, message.channel.id, message.id, exc)
        except discord.HTTPException as exc:
            log.warning("拦截删除失败 %s guild=%s channel=%s message=%s error=%s",
                        what, message.guild.id if message.guild else 0, message.channel.id, message.id, exc)
        return False

    async def _enforce_blocks(self, message: discord.Message, edited: bool = False) -> None:
        """拦截与任一目标（楼主 > 被回复者 > 所有 @ 对象）存在拉黑关系的发言。"""
        sender = message.author
        owner_id = self._forum_owner_id(message.channel)
        targets = block_policy.collect_targets(
            sender.id,
            owner_id=owner_id,
            reply_author_id=await self._reply_author_id(message),
            mention_ids=[u.id for u in message.mentions],
        )
        hit = block_policy.pick(await self._block_pairs(sender.id, targets), sender.id, targets)
        if not hit:
            return
        target_id, relation = hit
        who = "帖子楼主" if owner_id and target_id == owner_id else "对方"
        if relation == "sender_blocked_recipient":
            reason = f"你已将{who}加入黑名单，无法对其进行回复。"
        elif relation == "recipient_blocked_sender":
            reason = f"{who}已将你拉黑，回复被自动拦截。"
        else:
            reason = f"你与{who}处于相互拉黑状态，回复已被自动删除。"
        deleted_content = message.content
        channel_mention = message.channel.mention
        ok = await self._delete_intercepted(message, "edit" if edited else "message")
        log.info("拉黑拦截 guild=%s channel=%s sender=%s target=%s relation=%s edited=%s deleted=%s",
                 message.guild.id, message.channel.id, sender.id, target_id, relation, edited, ok)
        # 不论是否删成功都计数，便于在 /list 中发现权限问题
        await self._increment_block_count(message.guild.id, sender.id, target_id)
        try:
            dm = await sender.create_dm()
            await dm.send(
                f"⚠️ **消息发送已被拦截**\n"
                f"你在频道 {channel_mention} 中的{'编辑后' if edited else ''}回复已被自动删除。\n"
                f"**拦截原因：** {reason}\n\n"
                f"**你的回复原文：**\n"
                f"> {deleted_content if deleted_content else '[包含附件/多媒体/非纯文本内容]'}"
            )
        except discord.Forbidden:
            pass
        except discord.HTTPException as exc:
            log.debug("向被拦截用户发送 DM 失败 user=%s error=%s", sender.id, exc)

    async def _enforce_blocks_on_edit(self, payload) -> None:
        data = payload.data or {}
        if payload.guild_id is None or "content" not in data:
            return
        author = data.get("author") or {}
        if author.get("bot") or (self.user and str(author.get("id")) == str(self.user.id)):
            return
        msg = getattr(payload, "message", None)
        if msg is None:
            ch = self.get_channel(payload.channel_id)
            if ch is None:
                return
            try:
                msg = await ch.fetch_message(payload.message_id)
            except Exception:
                return
        if msg.guild is None or msg.author.bot:
            return
        await self._enforce_blocks(msg, edited=True)

    # ------------------------------------------------------------------ 活跃确认
    async def _inactive_role(self, guild_id: int, user_id: int, add: bool):
        """添加/移除不活跃身份组。返回 (ok, error)。"""
        guild = self.get_guild(int(guild_id))
        if guild is None:
            return False, 'bot not in guild'
        role = guild.get_role(liveness.LIVENESS_ROLE_ID)
        if role is None:
            return False, f'role {liveness.LIVENESS_ROLE_ID} not found'
        try:
            member = guild.get_member(int(user_id)) or await guild.fetch_member(int(user_id))
        except discord.NotFound:
            return False, 'member left guild'
        except discord.HTTPException as exc:
            return False, f'fetch member: {exc}'
        try:
            if add and role not in member.roles:
                await member.add_roles(role, reason='活跃确认超时')
            elif not add and role in member.roles:
                await member.remove_roles(role, reason='用户确认我还在')
            return True, None
        except discord.Forbidden as exc:
            log.warning('修改不活跃身份组失败（缺少 Manage Roles 或身份组层级不足）guild=%s user=%s add=%s error=%s',
                        guild_id, user_id, add, exc)
            return False, f'forbidden: {exc}'
        except discord.HTTPException as exc:
            return False, f'http: {exc}'

    async def liveness_confirm(self, guild_id: int, user_id: int) -> str:
        """按钮或 /alive：标记 alive，若已加身份组则移除。"""
        ok, err = await self._inactive_role(guild_id, user_id, add=False)
        c = db()
        try:
            n = liveness.mark_alive(c, user_id, guild_id, role_removed=ok)
        finally:
            c.close()
        log.info('活跃确认 guild=%s user=%s updated=%s role_removed=%s err=%s', guild_id, user_id, n, ok, err)
        if err and err not in ('member left guild',):
            return '已记录你的确认，谢谢！不过移除身份组时出了点问题，请联系管理员。'
        return '✅ 谢谢确认！你仍然是活跃成员。'

    @tasks.loop(seconds=20)
    async def liveness_sender(self):
        c = db()
        try:
            rows = c.execute("SELECT * FROM liveness_checks WHERE status='queued' ORDER BY id LIMIT 5").fetchall()
        finally:
            c.close()
        for row in rows:
            cid, uid, gid = row['id'], int(row['user_id']), int(row['guild_id'])
            error = None
            try:
                user = self.get_user(uid) or await self.fetch_user(uid)
                guild = self.get_guild(gid)
                if user.bot:
                    error = 'bot account'
                elif guild is not None and guild.get_member(uid) is None:
                    try:
                        await guild.fetch_member(uid)
                    except discord.NotFound:
                        error = 'not in guild'
                if error is None:
                    await user.send(liveness.dm_text(row['last_message_at'], guild.name if guild else '服务器'),
                                    view=liveness_view(row['token']))
            except discord.Forbidden as exc:
                error = f'DM closed: {exc}'
            except discord.HTTPException as exc:
                error = f'http: {exc}'
            except Exception as exc:
                error = f'{type(exc).__name__}: {exc}'
            c = db()
            try:
                if error:
                    liveness.mark_failed(c, cid, error)
                else:
                    liveness.mark_sent(c, cid)
            finally:
                c.close()
            log.info('活跃确认私信 check=%s user=%s ok=%s error=%s', cid, uid, error is None, error)
            await asyncio.sleep(liveness.SEND_INTERVAL_SEC)

    @tasks.loop(minutes=10)
    async def liveness_expirer(self):
        c = db()
        try:
            rows = liveness.due_for_expiry(c)
        finally:
            c.close()
        for row in rows:
            uid, gid = int(row['user_id']), int(row['guild_id'])
            ok, err = await self._inactive_role(gid, uid, add=True)
            c = db()
            try:
                liveness.mark_expired(c, row['id'], ok, err)
            finally:
                c.close()
            log.info('活跃确认超时 check=%s user=%s role_added=%s err=%s', row['id'], uid, ok, err)
            if ok:
                try:
                    user = self.get_user(uid) or await self.fetch_user(uid)
                    await user.send(liveness.EXPIRED_TEXT.format(days=liveness.DEADLINE_DAYS),
                                    view=liveness_view(row['token']))
                except discord.HTTPException as exc:
                    log.info('超时通知私信失败 user=%s error=%s', uid, exc)
            await asyncio.sleep(1)

    @liveness_sender.before_loop
    @liveness_expirer.before_loop
    async def _liveness_wait_ready(self):
        await self.wait_until_ready()

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
                c = db(); c.execute("UPDATE member_sync_requests SET status='failed',finished_at=?,error=? WHERE id=?", (utc_now_iso(), '白名单机器人未加入该服务器', row['id'])); c.commit(); c.close()
                continue
            try:
                await sync_guild_members(guild, int(row['id']))
            except Exception as exc:
                log.exception("成员同步失败 guild=%s request_id=%s：%s", guild.id, row["id"], exc)

    @tasks.loop(seconds=5)
    async def download_notifications(self):
        c = db()
        rows = c.execute("SELECT * FROM download_tasks WHERE status IN ('completed','failed') AND notified_at IS NULL ORDER BY id LIMIT 20").fetchall()
        for r in rows:
            prev = c.execute(
                "SELECT status FROM download_tasks WHERE guild_id=? AND forum_channel_id=? AND id<? ORDER BY id DESC LIMIT 1",
                (r['guild_id'], r['forum_channel_id'], r['id'])).fetchone()
            if not should_notify_finish(r, prev['status'] if prev else None):
                # 自动轮询任务（mode=update）静默：只记已处理，不发私信
                c.execute("UPDATE download_tasks SET notified_at=? WHERE id=?", (utc_now_iso(), r['id']))
                continue
            try:
                user = await self.fetch_user(int(r['created_by']))
                if r['status'] == 'completed':
                    text = f"Discord 数据下载任务 #{r['id']} 已完成。\n服务器：{r['guild_id']}\n共下载：{r['completed']}/{r['total']} 个帖子。\n耗时：{r['elapsed_seconds'] or 0} 秒。"
                else:
                    text = ("[自动轮询] " if is_auto_poll(r) else "") + f"Discord 数据下载任务 #{r['id']} 下载失败。\n服务器：{r['guild_id']}\n原因：{r['error'] or '未知错误'}"
                await user.send(text)
                c.execute("UPDATE download_tasks SET notified_at=? WHERE id=?", (utc_now_iso(), r['id']))
            except Exception as exc:
                c.execute("UPDATE download_tasks SET notified_at=? WHERE id=?", (utc_now_iso(), r['id']))
                log.exception("下载任务通知失败 task_id=%s error=%s", r["id"], exc)
        c.commit(); c.close()


bot = Bot()


# ---------------------------------------------------------------------------
# 白名单 / 成员同步 / 下载通知 相关斜杠命令
# ---------------------------------------------------------------------------

@bot.tree.command(name='whitelist_add', description='添加白名单 · Add whitelist user')
async def whitelist_add(interaction: discord.Interaction, user: discord.User):
    if not admin(interaction.user.id):
        return await interaction.response.send_message('无权限', ephemeral=True)
    c = db()
    c.execute('INSERT OR REPLACE INTO whitelist_users(user_id,username,added_by,created_at) VALUES(?,?,?,?)', (str(user.id), str(user), str(interaction.user.id), utc_now_iso()))
    c.execute('INSERT OR IGNORE INTO server_download_quota(user_id,quota) VALUES(?,1)', (str(user.id),))
    c.commit(); c.close()
    await interaction.response.send_message(f'已加入白名单 {user}，默认服务器配额 1', ephemeral=True)


@bot.tree.command(name='whitelist_remove', description='移除白名单 · Remove whitelist user')
async def whitelist_remove(interaction: discord.Interaction, user: discord.User):
    if not admin(interaction.user.id):
        return await interaction.response.send_message('无权限', ephemeral=True)
    c = db(); c.execute('DELETE FROM whitelist_users WHERE user_id=?', (str(user.id),)); c.execute('DELETE FROM server_download_quota WHERE user_id=?', (str(user.id),)); c.execute('DELETE FROM user_server_access WHERE user_id=?', (str(user.id),)); c.commit(); c.close()
    await interaction.response.send_message(f'已移除 {user}', ephemeral=True)


@bot.tree.command(name='whitelist_list', description='查看白名单 · Show whitelist')
async def whitelist_list(interaction: discord.Interaction):
    if not admin(interaction.user.id):
        return await interaction.response.send_message('无权限', ephemeral=True)
    c = db(); rows = c.execute('SELECT w.user_id,w.username,COALESCE(q.quota,1) FROM whitelist_users w LEFT JOIN server_download_quota q ON q.user_id=w.user_id ORDER BY w.created_at DESC').fetchall(); c.close()
    text = '\n'.join(f'{r[1]} `{r[0]}` 配额 {r[2]}' for r in rows) or '空'
    await interaction.response.send_message(text[:1900], ephemeral=True)


@bot.tree.command(name='server_access', description='授权访问服务器 · Grant server access')
@app_commands.describe(user='白名单用户', server_id='Discord Server ID')
async def server_access(interaction: discord.Interaction, user: discord.User, server_id: str):
    if not admin(interaction.user.id):
        return await interaction.response.send_message('无权限', ephemeral=True)
    if not server_id.isdigit():
        return await interaction.response.send_message('服务器 ID 无效', ephemeral=True)
    c = db()
    if not c.execute('SELECT 1 FROM whitelist_users WHERE user_id=?', (str(user.id),)).fetchone():
        c.close(); return await interaction.response.send_message('该用户不在白名单，请先执行 /whitelist_add', ephemeral=True)
    c.execute('INSERT OR REPLACE INTO user_server_access(user_id,server_id,granted_by,created_at) VALUES(?,?,?,?)', (str(user.id), server_id, str(interaction.user.id), utc_now_iso()))
    c.commit(); c.close()
    await interaction.response.send_message(f'已授权 {user} 使用已导入服务器 {server_id}', ephemeral=True)


@bot.tree.command(name='quota', description='设置下载配额 · Set download quota')
async def quota(interaction: discord.Interaction, user: discord.User, quota: int):
    if not admin(interaction.user.id):
        return await interaction.response.send_message('无权限', ephemeral=True)
    quota = max(1, min(100, quota)); c = db(); c.execute('INSERT INTO server_download_quota(user_id,quota) VALUES(?,?) ON CONFLICT(user_id) DO UPDATE SET quota=excluded.quota', (str(user.id), quota)); c.commit(); c.close()
    await interaction.response.send_message(f'{user} 配额已设置为 {quota}', ephemeral=True)


def request_full_application_restart() -> bool:
    """Ask the V20 application/service manager to restart the whole stack."""
    app_pid = os.getenv("V20_APP_PID", "").strip()
    restart_signal = os.getenv("V20_RESTART_SIGNAL", "").strip()
    if app_pid and restart_signal:
        try:
            os.kill(int(app_pid), int(restart_signal))
            log.warning("已请求 V20 主应用完整重启 app_pid=%s signal=%s", app_pid, restart_signal)
            return True
        except (ValueError, ProcessLookupError, PermissionError, OSError):
            log.exception("通知 V20 主应用完整重启失败")

    # Older/manual deployments may not inject the child-process environment.
    # Never exec app.py here: that would replace this bot with Flask and leave
    # the rest of the stack running. Prefer the systemd unit that owns app.py.
    service_name = os.getenv("V20_SERVICE_NAME", "v20.service").strip() or "v20.service"
    if os.name == "posix":
        try:
            subprocess.Popen(
                ["systemctl", "restart", service_name],
                start_new_session=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                close_fds=True,
            )
            log.warning("已请求 systemd 重启完整 V20 服务 service=%s", service_name)
            return True
        except (FileNotFoundError, PermissionError, OSError):
            log.exception("请求 systemd 重启 V20 服务失败 service=%s", service_name)
    return False


@bot.tree.command(name='alive', description='我还在 · 确认活跃并移除不活跃身份组')
async def alive_cmd(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True, thinking=True)
    gid = interaction.guild.id if interaction.guild else liveness.LIVENESS_GUILD_ID
    text = await bot.liveness_confirm(gid, interaction.user.id)
    await interaction.followup.send(text, ephemeral=True)


@bot.tree.command(name='restart', description='重启机器人 · Restart bot')
async def restart(interaction: discord.Interaction):
    if not admin(interaction.user.id):
        return await interaction.response.send_message('无权限：只有 .env 中的 ADMIN_IDS 可以重启完整 V20 服务。', ephemeral=True)

    await interaction.response.send_message('完整 V20 服务将在 1 秒后重启。', ephemeral=True)
    log.warning('管理员 %s 请求重启完整 V20 服务', interaction.user.id)
    await asyncio.sleep(1)
    if not request_full_application_restart():
        log.error('完整 V20 服务重启失败 user_id=%s', interaction.user.id)
        await interaction.followup.send('完整服务重启失败，请检查 V20_APP_PID 或 systemd 权限。', ephemeral=True)


@bot.tree.command(name='members_sync', description='同步成员名单 · Sync members')
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
    c.execute("INSERT INTO member_sync_requests(guild_id,requested_by,status,created_at) VALUES(?,?, 'pending', ?)", (server_id, str(interaction.user.id), utc_now_iso()))
    c.commit(); c.close()
    await interaction.response.send_message(f'服务器 {guild.name} 的成员名单更新已加入队列。', ephemeral=True)


# ---------------------------------------------------------------------------
# 收藏功能
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

    # 拉黑拦截：与帖子楼主存在任意方向拉黑关系时，禁止收藏该帖子。
    # （解除收藏不受限，允许用户随时清掉历史收藏。）
    owner_id = getattr(message.channel, "owner_id", None)
    if owner_id and owner_id != interaction.user.id:
        relation = await bot._block_relation_ids(interaction.user.id, owner_id)
        if relation:
            await interaction.followup.send(
                "❌ 你与帖子楼主存在拉黑关系，无法收藏该帖子。",
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
    return parse_utc_datetime(value) or datetime.now(timezone.utc)


@bot.tree.command(
    name="favorites",
    description="查看我的收藏 · My favorites",
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
    description="收藏排行 · Top favorites",
)
async def top(
    interaction: discord.Interaction,
) -> None:
    await ranking(interaction, None)


@bot.tree.command(
    name="top30",
    description="近期收藏排行 · Recent top favorites",
)
async def top30(
    interaction: discord.Interaction,
) -> None:
    await ranking(interaction, 30)


@bot.tree.command(
    name="help",
    description="使用帮助 · Help",
)
async def help_command(
    interaction: discord.Interaction,
) -> None:
    await ensure_favorite_user(interaction)

    await interaction.response.send_message(
        embed=help_embed(),
        ephemeral=is_private_context(interaction),
    )


# ---------------------------------------------------------------------------
# 黑名单功能：斜杠命令 / 右键 Apps
# ---------------------------------------------------------------------------


def _parse_block_target(input_val: str) -> Optional[int]:
    """支持纯数字 ID 或 `@用户` 形式输入。"""
    cleaned = (input_val or "").strip().strip("<@!> ")
    return int(cleaned) if cleaned.isdigit() else None


async def _block_user(blocker_id: int, blocked_id: int) -> bool:
    """在 portal.db.user_blocks 中写入拉黑关系。blocker 与 blocked 不能相同。"""
    if blocker_id == blocked_id:
        return False
    assert bot.db is not None
    async with bot.db.acquire() as conn:
        result = await conn.execute(
            """
            INSERT INTO user_blocks (blocker_id, blocked_id)
            VALUES (?, ?)
            ON CONFLICT (blocker_id, blocked_id) DO NOTHING
            """,
            blocker_id,
            blocked_id,
        )
    return result.rowcount == 1


async def _unblock_user(blocker_id: int, blocked_id: int) -> bool:
    assert bot.db is not None
    async with bot.db.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM user_blocks WHERE blocker_id=? AND blocked_id=?",
            blocker_id,
            blocked_id,
        )
    return result.rowcount == 1


async def _get_blocked_list_with_counts(blocker_id: int, guild_id: Optional[int]) -> list[dict]:
    """返回当前用户的黑名单列表，每条附带：

    - blocked_id:  被拉黑的用户 ID
    - count:       本机器人在 guild_id 内拦截「blocker -> blocked」方向的消息次数
                   若 guild_id 为 None（DM 中调用），则统计全服务器累计次数
    - last_blocked_at: 最近一次拦截时间（UTC ISO 字符串）

    按拦截次数降序排列，便于用户优先看到「被拦截最多的」目标。
    """
    assert bot.db is not None
    async with bot.db.acquire() as conn:
        if guild_id is None:
            rows = await conn.fetch(
                """
                SELECT
                    b.blocked_id AS blocked_id,
                    COALESCE(SUM(c.count), 0) AS total_count,
                    MAX(c.last_blocked_at) AS last_blocked_at
                FROM user_blocks b
                LEFT JOIN block_interception_counts c
                    ON c.sender_id = b.blocker_id AND c.target_id = b.blocked_id
                WHERE b.blocker_id = ?
                GROUP BY b.blocked_id
                ORDER BY total_count DESC, b.created_at DESC
                """,
                blocker_id,
            )
        else:
            rows = await conn.fetch(
                """
                SELECT
                    b.blocked_id AS blocked_id,
                    COALESCE(c.count, 0) AS total_count,
                    c.last_blocked_at AS last_blocked_at
                FROM user_blocks b
                LEFT JOIN block_interception_counts c
                    ON c.guild_id = ?
                   AND c.sender_id = b.blocker_id
                   AND c.target_id = b.blocked_id
                WHERE b.blocker_id = ?
                ORDER BY total_count DESC, b.created_at DESC
                """,
                guild_id,
                blocker_id,
            )
    return [dict(r) for r in rows]


@bot.tree.command(
    name="block",
    description="拉黑用户 · Block user",
)
@app_commands.describe(user_id="输入对方的 User ID，或直接 @提及 对方")
async def block_command(interaction: discord.Interaction, user_id: str):
    target_id = _parse_block_target(user_id)
    if not target_id:
        await interaction.response.send_message(
            "❌ 请输入正确的纯数字 ID 或直接 @提及 用户。",
            ephemeral=True,
        )
        return
    if target_id == interaction.user.id:
        await interaction.response.send_message("❌ 你不能拉黑你自己。", ephemeral=True)
        return

    success = await _block_user(interaction.user.id, target_id)
    if success:
        await interaction.response.send_message(
            f"✅ 已成功拉黑用户 ID: `{target_id}`，双方互相回复均会被自动拦截。",
            ephemeral=True,
        )
    else:
        await interaction.response.send_message(
            f"ℹ️ 该用户 (`{target_id}`) 已在你的黑名单中。",
            ephemeral=True,
        )


@bot.tree.command(
    name="unblock",
    description="解除拉黑 · Unblock user",
)
@app_commands.describe(user_id="输入要解除的 User ID，或直接 @提及 对方")
async def unblock_command(interaction: discord.Interaction, user_id: str):
    target_id = _parse_block_target(user_id)
    if not target_id:
        await interaction.response.send_message(
            "❌ 请输入正确的纯数字 ID 或直接 @提及 用户。",
            ephemeral=True,
        )
        return

    success = await _unblock_user(interaction.user.id, target_id)
    if success:
        await interaction.response.send_message(
            f"✅ 已解除对用户 ID: `{target_id}` 的拉黑限制。",
            ephemeral=True,
        )
    else:
        await interaction.response.send_message(
            f"ℹ️ 用户 ID (`{target_id}`) 不在你的黑名单中。",
            ephemeral=True,
        )


@bot.tree.command(
    name="list",
    description="查看拉黑列表 · Show blocklist",
)
async def list_command(interaction: discord.Interaction):
    """黑名单列表 + 拦截次数。

    - 在服务器内调用时，仅统计本服务器内的拦截次数
    - 在私聊中调用时，统计所有服务器的累计拦截次数
    """
    guild_id = interaction.guild.id if interaction.guild else None
    rows = await _get_blocked_list_with_counts(interaction.user.id, guild_id)

    if not rows:
        await interaction.response.send_message(
            "📋 你的黑名单目前为空。\n使用 `/block <用户>` 可以拉黑一个用户。",
            ephemeral=True,
        )
        return

    scope_text = "当前服务器" if guild_id else "全部服务器"

    lines: list[str] = []
    total_intercepted = 0
    for index, row in enumerate(rows, start=1):
        target_id = row["blocked_id"]
        count = int(row["total_count"] or 0)
        total_intercepted += count

        user = bot.get_user(target_id)
        name_str = f"**{user.name}**" if user else "已离线/未缓存用户"
        last_blocked = row["last_blocked_at"]
        last_blocked_text = ""
        if last_blocked:
            parsed = parse_utc_datetime(last_blocked)
            if parsed:
                last_blocked_text = parsed.astimezone(
                    timezone(timedelta(hours=8))
                ).strftime("%Y-%m-%d %H:%M CST")

        line = (
            f"**{index}.** {name_str} (`{target_id}`)\n"
            f"　　🛡️ 机器人替你拦截消息：**{count}** 次"
        )
        if last_blocked_text:
            line += f"\n　　⏱️ 最近拦截：`{last_blocked_text}`"
        lines.append(line)

    header = (
        f"**黑名单列表（{scope_text}）**\n"
        f"共 **{len(rows)}** 个用户，累计拦截 **{total_intercepted}** 条消息。\n\n"
    )
    content = header + "\n\n".join(lines)

    if len(content) > 2000:
        content = content[:1990] + "\n…（内容过长已截断）"

    await interaction.response.send_message(content, ephemeral=True)


@app_commands.context_menu(name="拉黑此发言者")
async def context_block(interaction: discord.Interaction, message: discord.Message):
    target = message.author
    if target.id == interaction.user.id:
        await interaction.response.send_message("❌ 不能拉黑自己。", ephemeral=True)
        return
    if target.bot:
        await interaction.response.send_message("❌ 无法拉黑机器人。", ephemeral=True)
        return

    success = await _block_user(interaction.user.id, target.id)
    msg = (
        f"✅ 已将 **{target.name}** 加入黑名单（双方均不可互相回复）。"
        if success
        else f"ℹ️ **{target.name}** 已在黑名单中。"
    )
    await interaction.response.send_message(msg, ephemeral=True)


@app_commands.context_menu(name="解除拉黑此发言者")
async def context_unblock(interaction: discord.Interaction, message: discord.Message):
    target = message.author
    success = await _unblock_user(interaction.user.id, target.id)
    msg = (
        f"✅ 已解除对 **{target.name}** 的拉黑限制。"
        if success
        else f"ℹ️ **{target.name}** 不在黑名单中。"
    )
    await interaction.response.send_message(msg, ephemeral=True)


bot.tree.add_command(context_block)
bot.tree.add_command(context_unblock)


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
    except discord.HTTPException as exc:
        log.debug("发送应用命令错误提示失败 user_id=%s error=%s", interaction.user.id, exc)


if __name__ == '__main__':
    if not TOKEN:
        raise SystemExit('请设置 DISCORD_BOT_TOKEN')
    bot.run(TOKEN)
