"""Discord 数据下载器。

下载过程使用数据库记录任务/帖子状态；原始 JSON 只作为短生命周期的导入临时文件，
成功导入 SQLite 后立即删除。帖子文件名只使用 Discord Thread ID，不再使用帖子标题，
从根源上避免 Linux/Windows 的 File name too long。
"""
import csv
import json
import os
import sqlite3
import subprocess
import sys
import time
import threading
import shutil
import asyncio
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from queue import Queue, Empty
from datetime import datetime, timezone, timedelta
from pathlib import Path


# 该脚本位于 Preparation_Before_Use，确保可以导入根目录项目模块/配置。
BASE_DIR = Path(__file__).resolve().parent.parent
def load_local_env(path):
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip("\"'"))


load_local_env(BASE_DIR / ".env")
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import discord
from Preparation_Before_Use.discordDB import import_json_incremental, rebuild_user_stats

PORTAL_DB = Path(os.getenv("PORTAL_DB", "data/portal.db"))
if not PORTAL_DB.is_absolute():
    PORTAL_DB = BASE_DIR / PORTAL_DB
RAW_DIR = BASE_DIR / "raw"
SERVER_DATA_DIR = BASE_DIR / "data" / "servers"
LOG_FILE = BASE_DIR / "discord_downloader.log"

logger = logging.getLogger("discord_downloader")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(threadName)s | %(message)s"))
    logger.addHandler(handler)
    stream = logging.StreamHandler(sys.stdout)
    stream.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(threadName)s | %(message)s"))
    logger.addHandler(stream)
logger.propagate = False

def log_step(message, *args):
    logger.info(message, *args)


class TaskPaused(Exception):
    pass


class TaskCancelled(Exception):
    pass


def now():
    return datetime.now(timezone.utc).isoformat()


def db():
    if not PORTAL_DB.exists():
        raise FileNotFoundError(f"找不到 {PORTAL_DB}")
    conn = sqlite3.connect(PORTAL_DB, timeout=60)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=60000")
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def ensure_schema(conn):
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS download_servers (
        server_id TEXT PRIMARY KEY, owner_user_id TEXT NOT NULL, guild_id TEXT NOT NULL,
        forum_channel_id TEXT NOT NULL, bot_id INTEGER, enabled INTEGER DEFAULT 1,
        use_default_bot INTEGER DEFAULT 0, updated_at DATETIME NOT NULL
    );
    CREATE TABLE IF NOT EXISTS download_tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        guild_id TEXT NOT NULL, forum_channel_id TEXT NOT NULL, created_by TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending', total INTEGER DEFAULT 0, completed INTEGER DEFAULT 0,
        started_at DATETIME, finished_at DATETIME, estimated_seconds INTEGER DEFAULT 0,
        elapsed_seconds INTEGER DEFAULT 0, message TEXT, error TEXT, notified_at DATETIME,
        created_at DATETIME NOT NULL, phase TEXT DEFAULT 'queued',
        scan_discovered INTEGER DEFAULT 0, scan_processed INTEGER DEFAULT 0,
        scan_total_estimate INTEGER DEFAULT 0, scan_started_at DATETIME, scan_finished_at DATETIME,
        scan_cursor TEXT, scan_completed INTEGER DEFAULT 0,
        active_bots INTEGER DEFAULT 0, failed_count INTEGER DEFAULT 0, speed REAL DEFAULT 0,
        heartbeat_at DATETIME, delete_requested INTEGER DEFAULT 0, download_interval_ms INTEGER DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS download_task_items (
        task_id INTEGER NOT NULL,
        thread_id TEXT NOT NULL,
        thread_name TEXT,
        status TEXT NOT NULL DEFAULT 'pending',
        filename TEXT,
        bot_name TEXT,
        error TEXT,
        downloaded_at DATETIME,
        last_active_at TEXT,
        PRIMARY KEY(task_id, thread_id)
    );
    CREATE TABLE IF NOT EXISTS download_configs (
        id INTEGER PRIMARY KEY AUTOINCREMENT, server_id TEXT NOT NULL, owner_user_id TEXT NOT NULL,
        guild_id TEXT NOT NULL, forum_channel_id TEXT NOT NULL, guild_name TEXT, forum_name TEXT,
        enabled INTEGER DEFAULT 1, use_default_bot INTEGER DEFAULT 0, scheduler_interval INTEGER DEFAULT 250, download_interval_ms INTEGER DEFAULT 0, update_enabled INTEGER DEFAULT 0, updated_at DATETIME NOT NULL,
        UNIQUE(guild_id, forum_channel_id)
    );
    CREATE TABLE IF NOT EXISTS download_config_bots (config_id INTEGER NOT NULL, bot_id INTEGER NOT NULL, PRIMARY KEY(config_id,bot_id));
    CREATE INDEX IF NOT EXISTS idx_download_items_task_status
        ON download_task_items(task_id, status);
    """)
    migrations = {
        "phase": "TEXT DEFAULT 'queued'",
        "scan_discovered": "INTEGER DEFAULT 0",
        "scan_processed": "INTEGER DEFAULT 0",
        "scan_total_estimate": "INTEGER DEFAULT 0",
        "scan_started_at": "DATETIME",
        "scan_finished_at": "DATETIME",
        "scan_cursor": "TEXT",
        "scan_completed": "INTEGER DEFAULT 0",
        "active_bots": "INTEGER DEFAULT 0",
        "failed_count": "INTEGER DEFAULT 0",
        "speed": "REAL DEFAULT 0",
        "heartbeat_at": "DATETIME",
        "delete_requested": "INTEGER DEFAULT 0",
        "download_interval_ms": "INTEGER DEFAULT 0",
        "config_id": "INTEGER",
        "guild_name": "TEXT",
        "forum_name": "TEXT",
        "scheduler_interval": "INTEGER DEFAULT 1000",
        "update_enabled": "INTEGER DEFAULT 0",
        "scan_bot_name": "TEXT",
        "mode": "TEXT DEFAULT 'initial'",
        "last_active_at": "TEXT"
    }
    for col, typ in migrations.items():
        try:
            conn.execute(f"ALTER TABLE download_tasks ADD COLUMN {col} {typ}")
        except sqlite3.OperationalError:
            pass
    for col, typ in {"last_active_at":"TEXT"}.items():
        try:
            conn.execute(f"ALTER TABLE download_task_items ADD COLUMN {col} {typ}")
        except sqlite3.OperationalError:
            pass
    try:
        conn.execute("ALTER TABLE servers ADD COLUMN source_task_id INTEGER")
    except sqlite3.OperationalError:
        pass
    conn.execute("CREATE INDEX IF NOT EXISTS idx_download_tasks_status_created ON download_tasks(status, created_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_download_tasks_guild_status ON download_tasks(guild_id, status)")
    conn.commit()


def get_token(bot_id=None):
    conn = db()
    row = None
    if bot_id:
        row = conn.execute("SELECT name,token FROM download_bots WHERE id=?", (bot_id,)).fetchone()
    default_token = (os.getenv("DISCORD_DOWNLOADER_TOKEN") or os.getenv("DISCORD_DOWNLOADER") or "").strip()
    conn.close()
    if row and row["token"]:
        return {"name": row["name"], "token": row["token"]}
    if default_token:
        return {"name": "env-default", "token": default_token}
    return None


def get_server_bots(config_id=None, guild_id=None):
    conn=db(); result=[]
    if config_id:
        cfg=conn.execute("SELECT * FROM download_configs WHERE id=? AND enabled=1",(int(config_id),)).fetchone()
    elif guild_id:
        cfg=conn.execute("SELECT * FROM download_configs WHERE guild_id=? AND enabled=1 ORDER BY id LIMIT 1",(str(guild_id),)).fetchone()
    else:
        cfg=None
    if cfg:
        rows=conn.execute("SELECT b.id,b.name,b.token FROM download_config_bots cb JOIN download_bots b ON b.id=cb.bot_id WHERE cb.config_id=? ORDER BY b.id",(cfg["id"],)).fetchall()
        result=[dict(r) for r in rows]
        if cfg["use_default_bot"]:
            default=get_token()
            if default: result.append({"id":None,"name":"默认下载机器人","token":default["token"]})
    if not result and cfg is None:
        default=get_token()
        if default: result.append({"id":None,"name":"默认下载机器人","token":default["token"]})
    conn.close(); return result


async def _login_bot_token(token):
    """只建立 Discord HTTP 登录会话验证令牌，不启动 Gateway 常驻连接。"""
    intents = discord.Intents.none()
    client = discord.Client(intents=intents)
    try:
        await client.login(token)
        user = client.user
        return str(user.id) if user else "unknown"
    finally:
        await client.close()


def authenticate_download_bots(task_id, bots):
    """并行验证全部下载机器人，让无效令牌在任务开始时立即暴露。"""
    if not bots:
        return []

    def login_one(bot):
        try:
            user_id = asyncio.run(_login_bot_token(bot["token"]))
            log_step("任务 #%s | 下载机器人登录成功 | name=%s discord_id=%s", task_id, bot["name"], user_id)
            return bot
        except Exception as exc:
            log_step("任务 #%s | 下载机器人登录失败 | name=%s | error=%s", task_id, bot["name"], exc)
            return None

    valid = []
    with ThreadPoolExecutor(max_workers=len(bots), thread_name_prefix=f"bot-login-{task_id}") as pool:
        futures = [pool.submit(login_one, bot) for bot in bots]
        for future in futures:
            result = future.result()
            if result:
                valid.append(result)
    return valid


def notify_task_started(task, bot_count, resumed):
    """使用主程序机器人（DISCORD_BOT_TOKEN）向任务创建者发送开始通知。"""
    token = (os.getenv("DISCORD_BOT_TOKEN") or "").strip()
    user_id = str(task["created_by"] or "").strip()
    if not token or not user_id:
        log_step("任务 #%s | 跳过开始通知 | DISCORD_BOT_TOKEN 或用户 ID 未配置", task["id"])
        return

    text = (
        f"Discord 数据下载任务 #{task['id']} 已{'继续' if resumed else '开始'}。\n"
        f"服务器：{task['guild_id']}\n"
        f"下载机器人：{bot_count} 个\n"
        f"扫描状态：{'已完成，恢复未完成帖子' if resumed else '扫描中，扫描完成后全部机器人加入下载池'}"
    )
    headers = {"Authorization": f"Bot {token}", "Content-Type": "application/json"}
    try:
        channel_response = requests.post(
            "https://discord.com/api/v10/users/@me/channels",
            headers=headers, json={"recipient_id": user_id}, timeout=15,
        )
        if channel_response.status_code not in (200, 201):
            raise RuntimeError(f"创建私信频道失败 HTTP {channel_response.status_code}")
        channel_id = channel_response.json().get("id")
        if not channel_id:
            raise RuntimeError("Discord 没有返回私信频道 ID")
        message_response = requests.post(
            f"https://discord.com/api/v10/channels/{channel_id}/messages",
            headers=headers, json={"content": text}, timeout=15,
        )
        if message_response.status_code not in (200, 201):
            raise RuntimeError(f"发送开始通知失败 HTTP {message_response.status_code}")
        log_step("任务 #%s | 主程序机器人已发送开始通知 | user=%s", task["id"], user_id)
    except Exception as exc:
        # 通知失败不能阻断下载任务本身。
        log_step("任务 #%s | 主程序机器人开始通知失败 | error=%s", task["id"], exc)

def update_task(task_id, **values):
    if not values:
        return
    conn = db()
    values = dict(values)
    if "heartbeat_at" not in values:
        values["heartbeat_at"] = now()
    fields = ", ".join(f"{k}=?" for k in values)
    conn.execute(f"UPDATE download_tasks SET {fields} WHERE id=?", (*values.values(), task_id))
    conn.commit()
    conn.close()


def task_status(task_id):
    conn = db()
    row = conn.execute("SELECT status FROM download_tasks WHERE id=?", (task_id,)).fetchone()
    conn.close()
    return row["status"] if row else "cancelled"


def pending_tasks(limit=1):
    conn = db()
    ensure_schema(conn)
    rows = conn.execute(
        "SELECT * FROM download_tasks WHERE status='pending' AND delete_requested=0 ORDER BY id LIMIT ?",
        (int(limit),)
    ).fetchall()
    conn.close()
    return rows

def pending_task():
    rows = pending_tasks(1)
    return rows[0] if rows else None

def recover_stale_tasks():
    conn = db()
    ensure_schema(conn)
    # This function only runs when a new downloader process starts. Any task
    # left in running state belongs to the previous process and must be queued
    # again immediately; waiting five minutes leaves the restarted downloader
    # with no pending work while the task appears stuck as running.
    conn.execute(
        "UPDATE download_tasks SET status='pending', phase='queued', message='下载器重启后恢复排队', heartbeat_at=? "
        "WHERE status='running' AND delete_requested=0",
        (now(),)
    )
    conn.commit()
    conn.close()


class Enumerator(discord.Client):
    def __init__(self, guild_id, forum_id, token, task_id, lower_id=0, upper_id=None, progress_cb=None, row_cb=None, scan_before=None, cursor_cb=None, initial_seen=None, **kwargs):
        super().__init__(**kwargs)
        self.guild_id = int(guild_id)
        self.forum_id = int(forum_id)
        self.token = token
        self.task_id = task_id
        self.lower_id = int(lower_id or 0)
        self.upper_id = int(upper_id) if upper_id else None
        self.progress_cb = progress_cb
        self.row_cb = row_cb
        self.scan_before = scan_before
        self.cursor_cb = cursor_cb
        self.initial_seen = {int(x) for x in (initial_seen or ())}
        self.rows = []
        self.error = None
        self.processed = 0

    async def on_ready(self):
        try:
            log_step("任务 #%s | Discord 扫描客户端已连接 | guild=%s forum=%s", self.task_id, self.guild_id, self.forum_id)
            guild = self.get_guild(self.guild_id)
            if not guild:
                self.error = f"找不到服务器 {self.guild_id}"
                log_step("任务 #%s | 扫描失败 | %s", self.task_id, self.error)
                return
            channel = guild.get_channel(self.forum_id)
            if not isinstance(channel, discord.ForumChannel):
                self.error = f"{self.forum_id} 不是论坛频道，或机器人没有权限"
                log_step("任务 #%s | 扫描失败 | %s", self.task_id, self.error)
                return
            # 断点恢复时，活跃帖子接口会再次返回已经扫描过的帖子；
            # 预填 seen，避免重复计数和重复加入下载队列。
            seen = set(self.initial_seen)

            # channel.threads 只是 Gateway 缓存，不能用于获取 Forum 的全部活跃帖子。
            # 必须调用 Discord 的 Get Active Threads API。
            active_threads = await guild.active_threads()
            active_threads = [t for t in active_threads if int(getattr(t, "parent_id", 0) or 0) == self.forum_id]
            log_step("任务 #%s | REST 活跃帖子扫描完成 | Forum=%s | 数量=%s", self.task_id, self.forum_id, len(active_threads))
            for t in active_threads:
                if t.id in seen or not self._in_range(t.id):
                    continue
                seen.add(t.id)
                row = self.row(t)
                if self.row_cb: self.row_cb(row)
                else: self.rows.append(row)
                self._tick()

            async def fetch_archived_page(cursor):
                """读取一个完整 API 页。

                Discord 的历史接口偶发 5xx/429。扫描不能把这种临时错误
                当作“没有更多帖子”，否则任务会静默停在某个分页（例如
                13969）。只要任务没有被暂停/取消，就继续重试；成功返回后
                游标才会推进。
                """
                attempt = 0
                while True:
                    page_threads = []
                    try:
                        async for thread in channel.archived_threads(limit=100, before=cursor):
                            # 必须完整消费本次 API 页。不能按过滤后的帖子数
                            # 提前 break，否则 oldest archive timestamp 可能错误。
                            page_threads.append(thread)
                        return page_threads
                    except discord.HTTPException as exc:
                        status_code = int(getattr(exc, "status", 0) or 0)
                        if status_code not in (429, 500, 502, 503, 504):
                            raise
                        current_status = task_status(self.task_id)
                        if current_status in ("paused", "cancelled"):
                            raise TaskCancelled()
                        attempt += 1
                        retry_after = getattr(exc, "retry_after", None)
                        try:
                            wait_seconds = max(1.0, min(90.0, float(retry_after)))
                        except (TypeError, ValueError):
                            wait_seconds = min(90.0, 2.0 ** min(attempt - 1, 6))
                        log_step(
                            "任务 #%s | 历史扫描临时失败，将重试 | status=%s attempt=%s wait=%ss",
                            self.task_id, status_code, attempt, wait_seconds,
                        )
                        await asyncio.sleep(wait_seconds)

            # Discord 的 Public Archived Threads API 使用 archive_timestamp 分页。
            # 每轮严格按 API 返回的原始页移动游标，不能按“过滤后新帖子数”决定结束。
            before = None
            if self.scan_before:
                try:
                    before = datetime.fromisoformat(str(self.scan_before))
                    if before.tzinfo is None:
                        before = before.replace(tzinfo=timezone.utc)
                except (TypeError, ValueError):
                    log_step("任务 #%s | 扫描游标无效，将从历史起点开始 | cursor=%s", self.task_id, self.scan_before)
            page = 0
            while True:
                status = task_status(self.task_id)
                if status in ('paused', 'cancelled'):
                    log_step("任务 #%s | 扫描被用户状态中断 | status=%s", self.task_id, status)
                    return

                page += 1
                raw_page = await fetch_archived_page(before)
                if not raw_page:
                    log_step("任务 #%s | Forum 历史扫描结束 | page=%s | 累计=%s", self.task_id, page, self.processed)
                    break

                oldest_archive_at = None
                for t in raw_page:
                    archive_at = getattr(t, "archive_timestamp", None)
                    if archive_at is not None and (oldest_archive_at is None or archive_at < oldest_archive_at):
                        oldest_archive_at = archive_at
                    if t.id not in seen and self._in_range(t.id):
                        seen.add(t.id)
                        row = self.row(t)
                        if self.row_cb: self.row_cb(row)
                        else: self.rows.append(row)
                        self._tick()

                log_step("任务 #%s | Forum 历史扫描第 %s 页完成 | API本页=%s | 累计=%s | before=%s", self.task_id, page, len(raw_page), self.processed, before.isoformat() if before else None)
                if oldest_archive_at is None:
                    # 极少数情况下 Thread 没有 archive_timestamp，使用 created_at 作为保底游标。
                    oldest = min(raw_page, key=lambda x: int(x.id))
                    oldest_archive_at = getattr(oldest, "created_at", None)
                if oldest_archive_at is None:
                    raise RuntimeError(f"历史扫描第 {page} 页没有可用分页游标")

                # 必须向过去移动，否则 Discord 会重复返回同一页。
                next_before = oldest_archive_at - timedelta(microseconds=1)
                if before is not None and next_before >= before:
                    raise RuntimeError(
                        f"历史扫描分页游标未向前移动：before={before.isoformat()} next={next_before.isoformat()}"
                    )
                before = next_before
                if self.cursor_cb:
                    self.cursor_cb(before.isoformat())
        except Exception as exc:
            self.error = str(exc)
            log_step("任务 #%s | 扫描异常 | %s", self.task_id, self.error)
        finally:
            log_step("任务 #%s | 扫描客户端关闭 | processed=%s", self.task_id, self.processed)
            await self.close()

    def _in_range(self, tid):
        return int(tid) > self.lower_id and (self.upper_id is None or int(tid) <= self.upper_id)

    def _tick(self):
        self.processed += 1
        if self.progress_cb and (self.processed == 1 or self.processed % 50 == 0):
            self.progress_cb(self.processed)

    @staticmethod
    def row(t):
        last_active = None
        last_message_id = getattr(t, "last_message_id", None)
        if last_message_id:
            try:
                last_active = discord.utils.snowflake_time(int(last_message_id)).isoformat()
            except Exception:
                last_active = None
        if not last_active:
            last_active = t.created_at.isoformat() if t.created_at else ""
        return {
            "id": t.id, "name": t.name,
            "created_at": t.created_at.isoformat() if t.created_at else "",
            "last_active_at": last_active,
            "archived": t.archived, "locked": t.locked,
            "message_count": getattr(t, "message_count", ""),
            "member_count": getattr(t, "member_count", ""),
            "owner_id": getattr(t, "owner_id", "")
        }


def enumerate_server(guild_id, forum_id, token, task_id, lower_id=0, upper_id=None, progress_cb=None, row_cb=None, scan_before=None, cursor_cb=None, initial_seen=None):
    intents = discord.Intents.default(); intents.guilds = True
    client = Enumerator(guild_id, forum_id, token, task_id, lower_id, upper_id, progress_cb, row_cb=row_cb, scan_before=scan_before, cursor_cb=cursor_cb, initial_seen=initial_seen, intents=intents)
    client.run(token)
    if client.error: raise RuntimeError(client.error)
    return client.rows


def _export_one(dce, token, tid, out, task_id, bot_name=""):
    """使用一个机器人启动一个独立 DCE 进程下载一个帖子。"""
    log_step("任务 #%s | 机器人=%s | 开始 DCE 下载 | 帖子=%s", task_id, bot_name, tid)
    cmd = [dce, "export", "-t", token, "-c", str(tid), "-f", "Json", "-o", str(out)]
    if os.getenv("DCE_MARKDOWN", "false").strip().lower() not in ("1", "true", "yes", "on"):
        cmd.extend(["--markdown", "false"])
    safe_cmd = list(cmd)
    try:
        token_index = safe_cmd.index("-t") + 1
        safe_cmd[token_index] = "<TOKEN>"
    except (ValueError, IndexError):
        pass
    if os.getenv("DCE_LOG_COMMAND", "false").strip().lower() in ("1", "true", "yes", "on"):
        log_step("任务 #%s | 机器人=%s | DCE 命令=%s", task_id, bot_name, " ".join(safe_cmd))
    proc = subprocess.Popen(
        cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        cwd=BASE_DIR, env=os.environ.copy(), start_new_session=True
    )
    log_step("任务 #%s | 机器人=%s | DCE PID=%s 已启动 | 帖子=%s", task_id, bot_name, proc.pid, tid)
    try:
        while proc.poll() is None:
            state = task_status(task_id)
            if state == "paused":
                log_step("任务 #%s | 机器人=%s | 检测到暂停，终止 DCE PID=%s | 帖子=%s", task_id, bot_name, proc.pid, tid)
                proc.terminate()
                try:
                    proc.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    proc.kill(); proc.wait()
                out.unlink(missing_ok=True)
                raise TaskPaused()
            if state == "cancelled":
                log_step("任务 #%s | 机器人=%s | 检测到取消，终止 DCE PID=%s | 帖子=%s", task_id, bot_name, proc.pid, tid)
                proc.terminate()
                try:
                    proc.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    proc.kill(); proc.wait()
                out.unlink(missing_ok=True)
                raise TaskCancelled()
            try:
                poll_ms = max(100, min(2000, int(os.getenv("DCE_PROCESS_POLL_MS", "500"))))
            except ValueError:
                poll_ms = 500
            time.sleep(poll_ms / 1000.0)
        proc.wait()
    except Exception:
        if proc.poll() is None:
            proc.kill(); proc.wait()
        raise
    if proc.returncode != 0 or not out.exists() or out.stat().st_size == 0:
        log_step("任务 #%s | 机器人=%s | DCE 失败 | 帖子=%s | returncode=%s | output_exists=%s", task_id, bot_name, tid, proc.returncode, out.exists())
        raise RuntimeError(f"帖子 {tid} 下载失败，DCE 返回码 {proc.returncode}")
    log_step("任务 #%s | 机器人=%s | DCE 下载完成 | 帖子=%s | 文件大小=%s bytes", task_id, bot_name, tid, out.stat().st_size)


def upsert_task_items(task_id, rows):
    conn = db()
    for row in rows:
        tid = str(row["id"])
        filename = f"{tid}.json"
        conn.execute(
            """INSERT INTO download_task_items(task_id,thread_id,thread_name,status,filename)
               VALUES(?,?,?,?,?)
               ON CONFLICT(task_id,thread_id) DO UPDATE SET
               thread_name=excluded.thread_name,filename=excluded.filename""",
            (task_id, tid, str(row.get("name") or "thread"), "pending", filename)
        )
    conn.commit()
    conn.close()


def item_rows(task_id):
    conn = db()
    rows = conn.execute(
        "SELECT * FROM download_task_items WHERE task_id=? ORDER BY rowid", (task_id,)
    ).fetchall()
    conn.close()
    return rows


def mark_item(task_id, tid, **values):
    if not values:
        return
    conn = db()
    fields = ", ".join(f"{k}=?" for k in values)
    conn.execute(
        f"UPDATE download_task_items SET {fields} WHERE task_id=? AND thread_id=?",
        (*values.values(), task_id, str(tid))
    )
    conn.commit()
    conn.close()


def sync_portal_users(guild_id, db_path, task_id):
    conn = db()
    rows = sqlite3.connect(db_path)
    rows.row_factory = sqlite3.Row
    users = rows.execute(
        "SELECT user_id,username,nickname,avatar_url FROM users WHERE user_id IS NOT NULL AND user_id!=''"
    ).fetchall()
    rows.close()
    timestamp = now()
    for u in users:
        uid = str(u["user_id"])
        conn.execute(
            """INSERT INTO portal_users(user_id,username,nickname,avatar_url,last_login)
               VALUES(?,?,?,?,NULL)
               ON CONFLICT(user_id) DO UPDATE SET
               username=CASE WHEN excluded.username!='' THEN excluded.username ELSE portal_users.username END,
               nickname=CASE WHEN excluded.nickname!='' THEN excluded.nickname ELSE portal_users.nickname END,
               avatar_url=CASE WHEN excluded.avatar_url!='' THEN excluded.avatar_url ELSE portal_users.avatar_url END""",
            (uid, u["username"] or uid, u["nickname"] or u["username"] or uid, u["avatar_url"])
        )
        conn.execute(
            """INSERT INTO user_server_presence(user_id,server_id,first_seen,last_seen)
               VALUES(?,?,?,?) ON CONFLICT(user_id,server_id) DO UPDATE SET last_seen=excluded.last_seen""",
            (uid, str(guild_id), timestamp, timestamp)
        )
    conn.commit()
    conn.close()


DISCORD_EPOCH_MS = 1420070400000

def snowflake_from_ms(ms):
    return max(0, (int(ms) - DISCORD_EPOCH_MS) << 22)


def scan_partitions(bot_count):
    # 论坛数据通常集中在最近几年；用最近 8 年作为初始时间范围，避免把 2015~现在的
    # Snowflake 全时间轴平均切分后造成 5 个机器人扫描空时间片。
    import time as _time
    now_ms = int(_time.time() * 1000)
    start_ms = max(DISCORD_EPOCH_MS, now_ms - 8 * 365 * 24 * 60 * 60 * 1000)
    start = snowflake_from_ms(start_ms)
    end = snowflake_from_ms(now_ms + 60_000)
    span = max(1, end - start)
    count = max(1, int(bot_count))
    parts = []
    for i in range(count):
        lower = start + (span * i // count)
        upper = end if i == count - 1 else start + (span * (i + 1) // count)
        parts.append((lower, upper))
    return parts


def _set_scan_progress(task_id, discovered_delta=0, processed_delta=0, active_bots=None, message=None):
    conn = db()
    row = conn.execute("SELECT scan_discovered,scan_processed,scan_started_at FROM download_tasks WHERE id=?", (task_id,)).fetchone()
    if not row:
        conn.close(); return
    discovered = int(row["scan_discovered"] or 0) + int(discovered_delta or 0)
    processed = int(row["scan_processed"] or 0) + int(processed_delta or 0)
    started = row["scan_started_at"]
    speed = 0.0
    if started:
        try:
            elapsed = max(1.0, (datetime.now(timezone.utc) - datetime.fromisoformat(started)).total_seconds())
            speed = processed / elapsed * 60.0
        except Exception:
            pass
    values = [discovered, processed, speed, now(), task_id]
    sql = "UPDATE download_tasks SET scan_discovered=?,scan_processed=?,speed=?,heartbeat_at=?"
    if active_bots is not None:
        sql += ",active_bots=?"
        values.insert(-1, int(active_bots))
    if message is not None:
        sql += ",message=?"
        values.insert(-1, message)
    sql += " WHERE id=?"
    conn.execute(sql, values)
    conn.commit(); conn.close()


def upsert_task_items(task_id, rows):
    if not rows:
        return
    conn = db()
    conn.executemany(
        """INSERT INTO download_task_items(task_id,thread_id,thread_name,status,filename)
           VALUES(?,?,?,?,?)
           ON CONFLICT(task_id,thread_id) DO UPDATE SET
           thread_name=excluded.thread_name,filename=excluded.filename""",
        [(task_id, str(r["id"]), str(r.get("name") or "thread"), "pending", f"{r['id']}.json") for r in rows]
    )
    conn.commit(); conn.close()


def item_rows(task_id):
    conn = db()
    rows = conn.execute("SELECT * FROM download_task_items WHERE task_id=? ORDER BY rowid", (task_id,)).fetchall()
    conn.close(); return rows


def mark_item(task_id, tid, **values):
    if not values: return
    conn = db(); fields = ", ".join(f"{k}=?" for k in values)
    conn.execute(f"UPDATE download_task_items SET {fields} WHERE task_id=? AND thread_id=?", (*values.values(), task_id, str(tid)))
    conn.commit(); conn.close()


def import_downloaded_task(task_root, guild_id, task_id):
    db_path = SERVER_DATA_DIR / str(guild_id) / "discord_data.db"
    rows = item_rows(task_id)
    downloaded = [r for r in rows if r["status"] == "downloaded"]
    if not downloaded:
        raise RuntimeError("没有可导入的帖子数据")
    total_messages = 0; server_name = f"Discord Server {guild_id}"; icon_url = None
    for idx, row in enumerate(downloaded, 1):
        if task_status(task_id) in ('paused','cancelled'):
            raise TaskCancelled()
        path = task_root / "backup" / (row["filename"] or f"{row['thread_id']}.json")
        if not path.exists():
            raise RuntimeError(f"任务文件缺失：{row['thread_id']}")
        result = import_json_incremental(str(path), str(db_path), server_id=guild_id)
        total_messages += int(result.get("messages", 0))
        server_name = result.get("server_name") or server_name
        icon_url = result.get("icon_url") or icon_url
        path.unlink(missing_ok=True)
        if idx % 20 == 0 or idx == len(downloaded):
            update_task(task_id, phase="importing", message=f"正在导入 SQLite：{idx}/{len(downloaded)}", completed=min(int(row["task_id"] or 0) if False else idx, len(downloaded)))
    rebuild_user_stats(str(db_path))
    return db_path, server_name, icon_url, total_messages



def finalize_delete_if_requested(task_id, task_root=None):
    conn = db()
    row = conn.execute("SELECT delete_requested,guild_id FROM download_tasks WHERE id=?", (task_id,)).fetchone()
    if not row:
        conn.close(); return
    if not int(row["delete_requested"] or 0):
        conn.close(); return
    guild_id = row["guild_id"]
    conn.close()
    delete_task_server_data(task_id, guild_id)
    conn = db()
    conn.execute("DELETE FROM download_task_items WHERE task_id=?", (task_id,))
    conn.execute("DELETE FROM download_tasks WHERE id=?", (task_id,))
    remaining = conn.execute(
        "SELECT COUNT(*) FROM download_tasks WHERE guild_id=? AND status='completed'",
        (str(guild_id),),
    ).fetchone()[0]
    if remaining == 0:
        # Web 请求在运行任务删除时不能立即删任务行，最终清理由这里完成。
        # 同步清掉门户引用，避免分析库已空但网站仍显示旧服务器。
        server = conn.execute("SELECT db_path FROM servers WHERE server_id=?", (str(guild_id),)).fetchone()
        conn.execute("DELETE FROM user_server_access WHERE server_id=?", (str(guild_id),))
        conn.execute("DELETE FROM user_server_presence WHERE server_id=?", (str(guild_id),))
        conn.execute("DELETE FROM servers WHERE server_id=?", (str(guild_id),))
        try:
            conn.execute("DELETE FROM thread_scan_state WHERE guild_id=?", (str(guild_id),))
        except sqlite3.OperationalError:
            pass
    conn.commit(); conn.close()
    if remaining == 0:
        db_path = server["db_path"] if server and server["db_path"] else str(server_db_for(guild_id))
        try:
            Path(db_path).unlink(missing_ok=True)
        except OSError:
            pass
    if task_root:
        import shutil
        shutil.rmtree(task_root, ignore_errors=True)


def server_db_for(guild_id):
    return SERVER_DATA_DIR / str(guild_id) / "discord_data.db"


def delete_task_server_data(task_id, guild_id):
    """删除任务已写入分析库的帖子及其关联数据；删除请求由后台收尾时调用。"""
    conn = db()
    task_threads = [str(r[0]) for r in conn.execute(
        "SELECT thread_id FROM download_task_items WHERE task_id=?", (task_id,)
    ).fetchall()]
    other = {str(r[0]) for r in conn.execute(
        "SELECT DISTINCT thread_id FROM download_task_items WHERE task_id<>? AND status='downloaded'", (task_id,)
    ).fetchall()}
    conn.close()
    remove = [thread_id for thread_id in task_threads if thread_id not in other]
    db_path = server_db_for(guild_id)
    if not remove or not db_path.exists():
        return

    analytics = sqlite3.connect(db_path, timeout=120)
    try:
        placeholders = ','.join('?' * len(remove))
        message_ids = [str(r[0]) for r in analytics.execute(
            f"SELECT message_id FROM messages WHERE thread_id IN ({placeholders})", remove
        ).fetchall()]
        if message_ids:
            message_placeholders = ','.join('?' * len(message_ids))
            for table in ('reactions', 'attachments', 'mentions', 'messages'):
                analytics.execute(
                    f"DELETE FROM {table} WHERE message_id IN ({message_placeholders})", message_ids
                )
        analytics.execute(f"DELETE FROM threads WHERE thread_id IN ({placeholders})", remove)
        analytics.commit()
    finally:
        analytics.close()
    try:
        rebuild_user_stats(str(db_path))
    except Exception as exc:
        log_step("任务 #%s | 删除后重建用户统计失败 | error=%s", task_id, exc)
    try:
        conn = db()
        placeholders = ','.join('?' * len(remove))
        conn.execute(f"DELETE FROM thread_scan_state WHERE guild_id=? AND thread_id IN ({placeholders})", (str(guild_id), *remove))
        conn.commit()
        conn.close()
    except sqlite3.OperationalError:
        pass


def _remove_thread_from_server_db(db_path, thread_id):
    if not db_path.exists():
        return
    conn=sqlite3.connect(db_path,timeout=120)
    try:
        ids=[r[0] for r in conn.execute("SELECT message_id FROM messages WHERE thread_id=?",(str(thread_id),)).fetchall()]
        if ids:
            ph=','.join('?'*len(ids))
            for table in ('reactions','attachments','mentions'):
                conn.execute(f"DELETE FROM {table} WHERE message_id IN ({ph})",ids)
            conn.execute(f"DELETE FROM messages WHERE message_id IN ({ph})",ids)
        conn.execute("DELETE FROM threads WHERE thread_id=?",(str(thread_id),))
        conn.commit()
    finally:
        conn.close()


def import_one_json(path, guild_id, thread_id, last_active_at):
    db_path = server_db_for(guild_id)
    # 更新帖子时整帖替换，避免 Discord 删除/编辑消息后旧数据残留。
    _remove_thread_from_server_db(db_path, thread_id)
    result = import_json_incremental(str(path), str(db_path), server_id=guild_id)
    conn = sqlite3.connect(db_path, timeout=120)
    try:
        try:
            conn.execute("ALTER TABLE threads ADD COLUMN last_active_at TEXT")
        except sqlite3.OperationalError:
            pass
        conn.execute("UPDATE threads SET last_active_at=? WHERE thread_id=?", (last_active_at, str(thread_id)))
        conn.commit()
    finally:
        conn.close()
    return db_path, result


def existing_thread_active(guild_id, thread_id):
    # 更新比较必须以“已成功写入分析数据库”的最后活跃时间为准。
    # 不能只看扫描状态，否则某个帖子下载失败后，下一次更新会被误判为“无变化”。
    db_path = server_db_for(guild_id)
    if db_path.exists():
        try:
            conn = sqlite3.connect(db_path, timeout=60)
            row = conn.execute("SELECT last_active_at FROM threads WHERE thread_id=? LIMIT 1", (str(thread_id),)).fetchone()
            conn.close()
            if row and row[0]:
                return row[0]
        except sqlite3.Error:
            pass
    conn = db()
    try:
        conn.execute("CREATE TABLE IF NOT EXISTS thread_scan_state (guild_id TEXT NOT NULL, thread_id TEXT NOT NULL, name TEXT, last_active_at TEXT, scanned_at TEXT, PRIMARY KEY(guild_id, thread_id))")
        row = conn.execute("SELECT last_active_at FROM thread_scan_state WHERE guild_id=? AND thread_id=?", (str(guild_id), str(thread_id))).fetchone()
        return row[0] if row else None
    finally:
        conn.commit(); conn.close()


def record_scanned_thread(guild_id, row):
    conn = db()
    try:
        conn.execute("CREATE TABLE IF NOT EXISTS thread_scan_state (guild_id TEXT NOT NULL, thread_id TEXT NOT NULL, name TEXT, last_active_at TEXT, scanned_at TEXT, PRIMARY KEY(guild_id, thread_id))")
        conn.execute("INSERT INTO thread_scan_state(guild_id,thread_id,name,last_active_at,scanned_at) VALUES(?,?,?,?,?) ON CONFLICT(guild_id,thread_id) DO UPDATE SET name=excluded.name,last_active_at=excluded.last_active_at,scanned_at=excluded.scanned_at", (str(guild_id), str(row['id']), str(row.get('name') or 'thread'), str(row.get('last_active_at') or row.get('created_at') or ''), now()))
        conn.commit()
    finally:
        conn.close()



def bot_download_delay_ms(task=None):
    raw = task["download_interval_ms"] if task is not None and "download_interval_ms" in task.keys() else 0
    try:
        return max(0, min(60000, int(raw or 0)))
    except (ValueError, TypeError):
        return 0


def run_task(task, dce):
    task_id=int(task["id"])
    log_step("任务 #%s | 开始处理", task_id)
    guild_id=str(task["guild_id"])
    forum_id=str(task["forum_channel_id"])
    config_id=task["config_id"]
    conn=db(); ensure_schema(conn)
    cfg=conn.execute("SELECT * FROM download_configs WHERE id=? AND enabled=1",(config_id,)).fetchone() if config_id else conn.execute("SELECT * FROM download_configs WHERE guild_id=? AND forum_channel_id=? AND enabled=1",(guild_id,forum_id)).fetchone()
    conn.close()
    bots=get_server_bots(config_id=cfg["id"] if cfg else None,guild_id=guild_id)
    log_step("任务 #%s | 配置读取完成 | guild=%s forum=%s mode=%s | 机器人=%s", task_id, guild_id, forum_id, task["mode"] if "mode" in task.keys() else "initial", ", ".join(str(b.get("name")) for b in bots))
    if not bots:
        update_task(task_id,status='failed',phase='failed',message='没有可用的下载机器人',error='下载机器人未配置',finished_at=now())
        return

    bots=authenticate_download_bots(task_id, bots)
    if not bots:
        update_task(task_id,status='failed',phase='failed',message='下载机器人登录失败',error='所有配置的下载机器人都无法登录 Discord',finished_at=now())
        return

    scan_bot=bots[0]
    other_bots=bots[1:]
    task_root=RAW_DIR/guild_id/'tasks'/str(task_id)
    backup=task_root/'backup'
    backup.mkdir(parents=True,exist_ok=True)
    started=time.time()
    mode=str(task['mode'] or 'initial') if 'mode' in task.keys() else 'initial'
    # 只使用持久化完成标记判断扫描状态；时间字段只用于展示/审计。
    # 扫描完成后，继续任务只恢复未完成帖子；扫描中暂停则依据 scan_cursor
    # 接着历史分页，不重新从 Discord 的历史起点开始扫描。
    resume=bool(task['scan_completed'])
    scan_resume=not resume and bool(task['scan_cursor'])

    notify_task_started(task, len(bots), resume)

    # 中央任务队列：每个 Worker 一次只拿一个帖子。Worker 使用自己的 Token 和独立 DCE 进程。
    q=Queue()
    workers=[]
    stop_event=threading.Event()
    scan_finished=threading.Event()
    progress_lock=threading.Lock()
    failed_posts=[]
    retry_counts={}
    scanner_error=None
    scan_processed=int(task['scan_processed'] or 0) if scan_resume else 0
    scan_seen=set()
    if scan_resume:
        c=db(); scan_seen={int(r[0]) for r in c.execute("SELECT thread_id FROM download_task_items WHERE task_id=?",(task_id,)).fetchall()}; c.close()

    def total_items():
        c=db()
        try: return int(c.execute('SELECT COUNT(*) FROM download_task_items WHERE task_id=?',(task_id,)).fetchone()[0] or 0)
        finally: c.close()

    def done_items():
        c=db()
        try: return int(c.execute("SELECT COUNT(*) FROM download_task_items WHERE task_id=? AND status IN ('downloaded','skipped')",(task_id,)).fetchone()[0] or 0)
        finally: c.close()

    def upsert_scan_row(row,status='pending'):
        c=db()
        c.execute('''INSERT INTO download_task_items(task_id,thread_id,thread_name,status,filename,last_active_at)
                     VALUES(?,?,?,?,?,?)
                     ON CONFLICT(task_id,thread_id) DO UPDATE SET
                     thread_name=excluded.thread_name,filename=excluded.filename,last_active_at=excluded.last_active_at''',
                  (task_id,str(row['id']),str(row.get('name') or 'thread'),status,f"{row['id']}.json",row.get('last_active_at') or row.get('created_at') or ''))
        c.commit(); c.close()

    def scan_row(row):
        nonlocal scan_processed
        scan_processed+=1
        tid=str(row['id'])
        active=str(row.get('last_active_at') or row.get('created_at') or '')
        old=existing_thread_active(guild_id,tid)
        record_scanned_thread(guild_id,row)
        if mode=='update':
            if old is not None and str(old)==active:
                upsert_scan_row(row,'skipped')
                update_task(task_id,completed=done_items(),total=total_items(),scan_discovered=total_items(),scan_processed=scan_processed,message=f'更新扫描：帖子 {tid} 无变化，跳过下载')
                return
        upsert_scan_row(row,'pending')
        q.put(dict(row))
        log_step("任务 #%s | 扫描发现帖子=%s | last_active_at=%s | 队列长度=%s | status=%s", task_id, tid, active, q.qsize(), "skip" if mode=="update" and old is not None and str(old)==active else "pending")
        if scan_processed==1 or scan_processed%20==0:
            update_task(task_id,scan_processed=scan_processed,scan_discovered=total_items(),completed=done_items(),total=total_items(),active_bots=len(bots),message=f'扫描中：已发现 {total_items()} 个帖子，下载队列 {q.qsize()}')

    def worker(bot):
        log_step("任务 #%s | Worker 启动 | 机器人=%s", task_id, bot["name"])
        try:
            while not stop_event.is_set():
                try:
                    state=task_status(task_id)
                except Exception as exc:
                    log_step("任务 #%s | Worker=%s | 读取任务状态失败，继续保持 Worker | error=%s", task_id, bot["name"], exc)
                    time.sleep(0.5)
                    continue
                if state in ('paused','cancelled'):
                    return
                try:
                    row=q.get(timeout=0.2)
                except Empty:
                    if scan_finished.is_set() and q.unfinished_tasks == 0:
                        return
                    continue
                tid=str(row['id'])
                out=backup/f'{tid}.json'
                log_step("任务 #%s | 机器人=%s | 从队列取出帖子=%s | 剩余队列=%s", task_id, bot["name"], tid, q.qsize())
                try:
                    c=db(); st=c.execute('SELECT status FROM download_task_items WHERE task_id=? AND thread_id=?',(task_id,tid)).fetchone(); c.close()
                    if st and st[0] in ('downloaded','skipped'):
                        continue
                    if task_status(task_id) != 'running':
                        raise TaskCancelled()
                    _export_one(dce,bot['token'],tid,out,task_id,bot["name"])
                    if task_status(task_id) != 'running':
                        out.unlink(missing_ok=True)
                        raise TaskCancelled()
                    log_step("任务 #%s | 机器人=%s | 开始写入数据库 | 帖子=%s", task_id, bot["name"], tid)
                    db_path,result=import_one_json(out,guild_id,tid,str(row.get('last_active_at') or row.get('created_at') or ''))
                    # 删除请求可能与导入同时发生。导入完成后再次检查，
                    # 防止取消任务的帖子在 Web 端清理之后被写回分析库。
                    if task_status(task_id) != 'running':
                        _remove_thread_from_server_db(db_path, tid)
                        out.unlink(missing_ok=True)
                        raise TaskCancelled()
                    # 每个帖子导入完成后立即刷新门户服务器信息，网站无需等待整个任务结束。
                    portal=db(); ts=now()
                    portal.execute("INSERT INTO servers(server_id,name,icon_url,owner_user_id,db_path,created_at,updated_at,source_task_id) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(server_id) DO UPDATE SET name=COALESCE(excluded.name,servers.name),icon_url=COALESCE(excluded.icon_url,servers.icon_url),db_path=excluded.db_path,updated_at=excluded.updated_at,source_task_id=excluded.source_task_id",(guild_id,result.get('server_name') or f'Discord Server {guild_id}',result.get('icon_url'),str(task['created_by']),str(db_path),ts,ts,task_id))
                    portal.commit(); portal.close()
                    out.unlink(missing_ok=True)
                    log_step("任务 #%s | 机器人=%s | 数据库写入完成 | 帖子=%s", task_id, bot["name"], tid)
                    mark_item(task_id,tid,status='downloaded',bot_name=bot['name'],downloaded_at=now(),last_active_at=str(row.get('last_active_at') or row.get('created_at') or ''),error=None)
                    done=done_items(); total=total_items()
                    elapsed=max(1,int(time.time()-started))
                    update_task(task_id,phase='downloading',completed=done,total=total,elapsed_seconds=elapsed,active_bots=active_worker_count(),message=f'实时写入数据库：{tid} · {done}/{total} · {bot["name"]}')
                    log_step("任务 #%s | 机器人=%s | 帖子完成 | %s/%s", task_id, bot["name"], done, total)
                    delay_ms = bot_download_delay_ms(task)
                    if delay_ms:
                        log_step("任务 #%s | 机器人=%s | 下载速度控制等待 %sms", task_id, bot["name"], delay_ms)
                        time.sleep(delay_ms / 1000.0)
                except (TaskPaused,TaskCancelled):
                    return
                except Exception as exc:
                    log_step("任务 #%s | 机器人=%s | 帖子=%s | 下载流程异常：%s", task_id, bot["name"], tid, exc)
                    retry_counts[tid]=retry_counts.get(tid,0)+1
                    if retry_counts[tid] < 2 and task_status(task_id) not in ('paused','cancelled'):
                        q.put(row)
                    else:
                        mark_item(task_id,tid,status='failed',bot_name=bot['name'],error=str(exc))
                        failed_posts.append(f'帖子 {tid}: {exc}')
                finally:
                    q.task_done()
        except Exception as exc:
            # 不让单个机器人线程的未预期异常拖垮整个任务；主循环会重新拉起它。
            log_step("任务 #%s | Worker=%s | 线程异常退出 | error=%s", task_id, bot["name"], exc)

    worker_threads={}
    def worker_key(bot):
        return str(bot.get("id") or bot.get("name"))

    def launch_worker(bot):
        key=worker_key(bot)
        current=worker_threads.get(key)
        if current and current.is_alive():
            return current
        t=threading.Thread(target=worker,args=(bot,),name=f'dce-{bot["name"]}',daemon=True)
        worker_threads[key]=t
        workers.append(t)
        t.start()
        return t

    def active_worker_count():
        return sum(1 for t in worker_threads.values() if t.is_alive())

    monitor_stop = threading.Event()
    def scan_monitor():
        while not monitor_stop.wait(2):
            if task_status(task_id) != 'running':
                return
            if scan_finished.is_set():
                return
            try:
                update_task(task_id, scan_processed=scan_processed, scan_discovered=total_items(), total=total_items(), completed=done_items(), elapsed_seconds=max(0,int(time.time()-started)), active_bots=active_worker_count(), message=f'扫描中：已扫描 {scan_processed} · 已发现 {total_items()} · 下载队列 {q.qsize()} · 活跃下载机器人 {active_worker_count()}')
                log_step("任务 #%s | 扫描心跳 | processed=%s discovered=%s queue=%s active_workers=%s", task_id, scan_processed, total_items(), q.qsize(), active_worker_count())
            except Exception as exc:
                log_step("任务 #%s | 扫描心跳更新失败 | %s", task_id, exc)
    threading.Thread(target=scan_monitor, name=f'scan-monitor-{task_id}', daemon=True).start()

    try:
        update_task(task_id,status='running',phase='downloading' if resume else 'scanning' ,mode=mode,started_at=task['started_at'] or now(),scan_started_at=task['scan_started_at'] or now(),scan_bot_name=scan_bot['name'],active_bots=len(other_bots) if not resume else len(bots),message='继续下载：跳过已完成帖子，只处理未完成帖子' if resume else ('更新模式：扫描帖子 ID 与最后活跃时间' if mode=='update' else '首次模式：扫描帖子 ID 与最后活跃时间'))

        # 恢复时不再重新扫描，直接把未完成帖子放入队列。
        if resume:
            c=db(); rows=c.execute("SELECT thread_id,thread_name,last_active_at FROM download_task_items WHERE task_id=? AND status='pending' ORDER BY rowid",(task_id,)).fetchall(); c.close()
            for r in rows:
                q.put({'id':r['thread_id'],'name':r['thread_name'],'last_active_at':r['last_active_at']})
            scan_finished.set()
        else:
            # 扫描机器人在扫描完成前必须专职扫描；其他机器人先从中央队列下载。
            # 扫描完成后再把 scan_bot 动态加入 Worker 池，四个机器人才能全部下载。
            # 如果上次是在扫描中暂停/失败，之前已发现但尚未下载的帖子
            # 仍然要恢复到队列；历史分页会从 scan_cursor 继续，不会重复扫描。
            c=db(); pending_scan_rows=c.execute("SELECT thread_id,thread_name,last_active_at FROM download_task_items WHERE task_id=? AND status='pending' ORDER BY rowid",(task_id,)).fetchall(); c.close()
            for r in pending_scan_rows:
                q.put({'id':r['thread_id'],'name':r['thread_name'],'last_active_at':r['last_active_at']})
            for bot in other_bots:
                launch_worker(bot)

            def scanner_thread():
                try:
                    log_step("任务 #%s | 扫描线程启动 | 机器人=%s | 下载 Worker=%s", task_id, scan_bot["name"], len(workers))
                    enumerate_server(
                        guild_id, forum_id, scan_bot['token'], task_id,
                        progress_cb=lambda n: update_task(
                            task_id, scan_processed=scan_processed, scan_discovered=total_items(),
                            completed=done_items(), active_bots=active_worker_count(),
                            message=f'扫描中：已扫描 {scan_processed} · 已发现 {total_items()} · 下载队列 {q.qsize()} · 活跃机器人 {active_worker_count()}'
                        ),
                        row_cb=scan_row,
                        scan_before=task['scan_cursor'] if scan_resume else None,
                        cursor_cb=lambda cursor: update_task(task_id, scan_cursor=cursor),
                        initial_seen=scan_seen
                    )
                    log_step("任务 #%s | 扫描机器人返回 | 已处理=%s | 队列=%s", task_id, scan_processed, q.qsize())
                except Exception as exc:
                    scanner_error=str(exc)
                    failed_posts.append(f'扫描失败: {exc}')
                    log_step("任务 #%s | 扫描线程异常 | %s", task_id, exc)
                finally:
                    state_after_scan=task_status(task_id)
                    scan_failed=bool(scanner_error)
                    if state_after_scan not in ('paused','cancelled') and not scan_failed:
                        # 先启动扫描机器人对应的下载 Worker，再释放 scan_finished；
                        # 这样其他 Worker 不会在交接瞬间误判队列为空而退出。
                        launch_worker(scan_bot)
                        scan_finished.set()
                        update_task(
                            task_id, phase='downloading', scan_finished_at=now(), scan_completed=1,
                            scan_cursor=None,
                            scan_discovered=total_items(), completed=done_items(),
                            total=total_items(), active_bots=len(workers),
                            message=f'扫描完成：共发现 {total_items()} 个帖子，扫描机器人加入下载池'
                        )
                        log_step("任务 #%s | 扫描完成 | 全部下载 Worker 已参与 | 机器人=%s", task_id, len(workers))
                    else:
                        # 让主循环结束等待，暂停时保留已保存的扫描游标。
                        scan_finished.set()

            scanner=threading.Thread(target=scanner_thread,name=f'scanner-{task_id}',daemon=True)
            scanner.start()
            log_step("任务 #%s | 扫描与下载已同时启动", task_id)

        # 恢复任务不需要扫描，此处让全部机器人直接处理未完成帖子。
        if resume and not workers:
            for bot in bots:
                launch_worker(bot)
        elif resume:
            pass

        while True:
            state=task_status(task_id)
            if state in ('paused','cancelled'):
                while True:
                    try:
                        q.get_nowait(); q.task_done()
                    except Empty:
                        break
                break
            # Worker 发生未预期异常时自动补起；扫描阶段只维持其他下载机器人，
            # 扫描完成后则确保所有已登录机器人都能参与下载。
            if scan_finished.is_set():
                if q.unfinished_tasks > 0:
                    for bot in bots:
                        launch_worker(bot)
            else:
                for bot in other_bots:
                    launch_worker(bot)
            # 扫描尚未结束时，即使当前队列暂时为空，也不能结束任务。
            if scan_finished.is_set() and q.unfinished_tasks == 0:
                break
            time.sleep(0.2)
        monitor_stop.set()
        stop_event.set()
        for t in workers:
            t.join(timeout=5)

        state=task_status(task_id)
        if state in ('paused','cancelled'):
            finalize_delete_if_requested(task_id,task_root)
            return
        total=total_items(); done=done_items()
        if failed_posts:
            raise RuntimeError('以下帖子下载失败：'+'；'.join(failed_posts[:10]))
        db_path=server_db_for(guild_id)
        conn=sqlite3.connect(db_path,timeout=120)
        total_messages=int(conn.execute('SELECT COUNT(*) FROM messages').fetchone()[0] or 0)
        conn.close()
        portal=db(); ts=now()
        portal.execute('''INSERT INTO servers(server_id,name,icon_url,owner_user_id,db_path,created_at,updated_at,source_task_id)
                          VALUES(?,?,?,?,?,?,?,?)
                          ON CONFLICT(server_id) DO UPDATE SET db_path=excluded.db_path,updated_at=excluded.updated_at,source_task_id=excluded.source_task_id''',
                       (guild_id,f'Discord Server {guild_id}',None,str(task['created_by']),str(db_path),ts,ts,task_id))
        portal.commit(); portal.close()
        sync_portal_users(guild_id,db_path,task_id)
        shutil.rmtree(task_root,ignore_errors=True)
        elapsed=int(time.time()-started)
        update_task(task_id,status='completed',phase='completed',completed=done,total=total,elapsed_seconds=elapsed,estimated_seconds=0,active_bots=0,speed=0,message=f"{'更新' if mode=='update' else '首次下载'}完成：{done}/{total}，每个帖子下载后已实时写入数据库，共 {total_messages} 条消息",finished_at=now())
    except TaskCancelled:
        monitor_stop.set()
        update_task(task_id,status='cancelled',phase='cancelled',active_bots=0,message='任务已取消',finished_at=now()); finalize_delete_if_requested(task_id,task_root)
    except TaskPaused:
        monitor_stop.set()
        update_task(task_id,status='paused',phase='paused',active_bots=0,message='任务已暂停；已下载帖子保留，继续时不会重新下载这些帖子',finished_at=None)
    except Exception as exc:
        monitor_stop.set()
        update_task(task_id,status='failed',phase='failed',active_bots=0,error=str(exc),message='任务失败：已完成帖子已经写入数据库，未完成帖子状态保留',finished_at=now()); finalize_delete_if_requested(task_id,task_root)

def find_dce():
    names = ["DiscordChatExporter.Cli.linux-x64", "DiscordChatExporter.Cli.win-x64"]
    candidates = []
    for folder in names:
        root = BASE_DIR / folder
        if root.exists():
            candidates.extend(root.rglob("DiscordChatExporter.Cli.exe"))
            candidates.extend(root.rglob("DiscordChatExporter.Cli"))
    if os.name == "nt":
        candidates.sort(key=lambda p: 0 if p.name.lower().endswith(".exe") else 1)
    if not candidates:
        raise FileNotFoundError("同目录没有找到 DiscordChatExporter.Cli.linux-x64 或 DiscordChatExporter.Cli.win-x64")
    path = candidates[0]
    if os.name != "nt":
        try:
            path.chmod(path.stat().st_mode | 0o111)
        except OSError:
            pass
    return str(path)


def main_once():
    recover_stale_tasks()
    dce = find_dce()
    max_tasks = max(1, int(os.getenv("DOWNLOAD_MAX_CONCURRENT_TASKS", "3")))
    active = {}
    log_step("下载器启动 | 任务并发数=%s | DCE=%s | 机器人下载间隔=%sms", max_tasks, dce, 0)
    while True:
        for tid, future in list(active.items()):
            if future.done():
                try: future.result()
                except Exception as exc: log_step("任务 #%s | 调度 Worker 异常：%s", tid, exc)
                active.pop(tid, None)
        slots = max_tasks - len(active)
        if slots > 0:
            candidates = pending_tasks(slots)
            for task in candidates:
                tid = int(task['id'])
                if tid in active: continue
                # 原始任务若已有删除请求，直接清理，不再启动。
                if int(task['delete_requested'] or 0):
                    continue
                log_step("任务 #%s | 进入调度队列", tid)
                update_task(tid, status='running', phase='queued', message='已进入下载调度队列')
                pool = getattr(main_once, '_pool', None)
                if pool is None:
                    main_once._pool = ThreadPoolExecutor(max_workers=max_tasks, thread_name_prefix='task-worker')
                    pool = main_once._pool
                active[tid] = pool.submit(run_task, task, dce)
        if not active and not pending_tasks(1):
            log_step("没有剩余下载任务，下载器退出")
            return False
        active_intervals=[]
        for _tid in active:
            try:
                c=db(); rr=c.execute("SELECT scheduler_interval FROM download_tasks WHERE id=?",(_tid,)).fetchone(); c.close()
                if rr:
                    raw=int(rr["scheduler_interval"] or 1000)
                    # 兼容旧版以秒保存的 1~49 值；新版最小单位为毫秒。
                    interval_ms = raw * 1000 if raw < 50 else raw
                    active_intervals.append(max(50,min(60000,interval_ms)))
            except Exception: pass
        time.sleep((min(active_intervals) if active_intervals else 1000) / 1000.0)


if __name__ == "__main__":
    main_once()
