"""Real-time ingestion: Discord gateway events -> data/servers/<gid>/discord_data.db.

Used by Preparation_Before_Use/whitelist_bot.py. Only threads whose parent forum is
an enabled download_configs entry are written. All SQLite work is synchronous and
runs via asyncio.to_thread so the gateway loop never blocks.
"""
import asyncio
import logging
import os
import sqlite3
import subprocess
import sys
import json
import threading
import time

from shared import ingest_db

log = logging.getLogger("gateway-ingest")


def _emoji(emoji):
    """(name, custom_url) in the same shape DiscordChatExporter stores."""
    name = getattr(emoji, "name", None) or str(emoji)
    url = None
    if getattr(emoji, "id", None):
        url = str(getattr(emoji, "url", "") or "") or None
    return name, url


def _avatar(user):
    try:
        a = getattr(user, "display_avatar", None)
        return str(a.url) if a else ""
    except Exception:
        return ""


class GatewayIngest:
    def __init__(self, base_dir, portal_db):
        self.servers_dir = os.path.join(base_dir, "data", "servers")
        self.portal_db = portal_db
        self.base_dir = base_dir
        self._forums = {}  # guild_id -> set(forum ids)
        self._forums_at = 0.0
        self._lock = threading.Lock()  # one writer per process
        self.stats = {"messages": 0, "edits": 0, "deletes": 0, "react_add": 0, "react_remove": 0}

    # ---------------------------------------------------------- config
    def forums(self):
        if time.monotonic() - self._forums_at > 60:
            out = {}
            try:
                c = sqlite3.connect(self.portal_db, timeout=30)
                for gid, fid in c.execute("SELECT guild_id, forum_channel_id FROM download_configs WHERE enabled=1"):
                    out.setdefault(str(gid), set()).add(str(fid))
                c.close()
                self._forums = out
            except Exception as exc:
                log.warning("读取 download_configs 失败：%s", exc)
            self._forums_at = time.monotonic()
        return self._forums

    def tracked(self, guild_id, parent_id):
        return guild_id is not None and parent_id is not None and str(parent_id) in self.forums().get(str(guild_id), ())

    def db_path(self, guild_id):
        return os.path.join(self.servers_dir, str(guild_id), "discord_data.db")

    def _run(self, guild_id, fn):
        path = self.db_path(guild_id)
        if not os.path.exists(path):
            return None  # never create a server DB from the gateway; the first full download does that
        with self._lock:
            conn = ingest_db.connect(path)
            try:
                r = fn(conn)
                conn.commit()
                return r
            finally:
                conn.close()

    async def _call(self, guild_id, fn):
        try:
            return await asyncio.to_thread(self._run, guild_id, fn)
        except Exception:
            log.exception("网关写入失败 guild=%s", guild_id)

    def _thread_of(self, channel):
        parent = getattr(channel, "parent_id", None)
        return parent if getattr(channel, "type", None) is not None and "thread" in str(channel.type) else None

    # ---------------------------------------------------------- events
    async def on_message(self, message):
        ch = message.channel
        if message.guild is None or not self.tracked(message.guild.id, self._thread_of(ch)):
            return
        a = message.author
        data = {
            "message_id": message.id, "thread_id": ch.id, "author_id": a.id,
            "content": message.content or "", "timestamp": ingest_db.iso_ms(message.created_at),
            "reply_to": str(message.reference.message_id) if message.reference and message.reference.message_id else None,
            "attachments": [(x.url, x.filename, x.size) for x in message.attachments],
            "mentions": [u.id for u in message.mentions],
        }
        users = [(a.id, a.name, getattr(a, "nick", None) or getattr(a, "global_name", None) or "", _avatar(a), a.bot)]
        users += [(u.id, u.name, getattr(u, "nick", None) or getattr(u, "global_name", None) or "", _avatar(u), u.bot)
                  for u in message.mentions]
        gid, tid, pid, name, ts = message.guild.id, ch.id, ch.parent_id, getattr(ch, "name", ""), data["timestamp"]

        def fn(conn):
            for u in users:
                ingest_db.upsert_user(conn, *u)
            ingest_db.touch_thread(conn, tid, gid, pid, name, ts)
            return ingest_db.insert_message(conn, data)
        if await self._call(gid, fn):
            self.stats["messages"] += 1
            log.info("网关写入消息 guild=%s thread=%s msg=%s", gid, tid, message.id)

    # ---------------------------------------------------------- forum posts (thread metadata)
    def _thread_meta(self, th):
        tags = [t.name for t in (getattr(th, "applied_tags", None) or [])]
        created = getattr(th, "created_at", None)
        return (th.id, th.parent_id, getattr(th, "owner_id", None),
                ingest_db.iso_ms(created) if created else None,
                json.dumps(tags, ensure_ascii=False), getattr(th, "archived", False), getattr(th, "locked", False))

    async def on_thread_upsert(self, thread):
        """THREAD_CREATE / THREAD_UPDATE: owner, created_at, tags, title rename, archived/locked."""
        if thread.guild is None or not self.tracked(thread.guild.id, thread.parent_id):
            return
        meta = self._thread_meta(thread)
        gid, name = thread.guild.id, getattr(thread, "name", "")
        parent = getattr(thread, "parent", None)
        ftags = [(t.id, t.name, str(t.emoji) if t.emoji else "") for t in (getattr(parent, "available_tags", None) or [])]

        def fn(conn):
            ingest_db.upsert_thread_meta(conn, *meta)
            if ftags:
                ingest_db.upsert_forum_tags(conn, thread.parent_id, ftags)
            if name:
                conn.execute("UPDATE threads SET name=? WHERE thread_id=?", (name, str(thread.id)))
        await self._call(gid, fn)

    async def on_raw_thread_delete(self, payload):
        gid = getattr(payload, "guild_id", None)
        if gid is None or not self.tracked(gid, getattr(payload, "parent_id", None)):
            return
        await self._call(gid, lambda conn: ingest_db.mark_thread_deleted(conn, payload.thread_id))

    async def sync_active_threads(self, bot):
        """On ready: record metadata of all cached (active) threads in tracked forums."""
        n = 0
        for g in bot.guilds:
            for th in list(getattr(g, "threads", []) or []):
                if self.tracked(g.id, th.parent_id):
                    await self.on_thread_upsert(th)
                    n += 1
        return n

    def _guild_thread(self, payload, bot):
        gid = getattr(payload, "guild_id", None)
        if gid is None:
            return None
        ch = bot.get_channel(payload.channel_id)
        if ch is None:
            return gid  # unknown (uncached) thread: rows only exist if we tracked it -> safe to act by message_id
        return gid if self.tracked(gid, self._thread_of(ch)) else None

    async def on_raw_message_edit(self, payload, bot):
        gid = self._guild_thread(payload, bot)
        data = getattr(payload, "data", None) or {}
        if gid is None or "content" not in data:
            return
        if await self._call(gid, lambda c: ingest_db.edit_message(c, payload.message_id, data.get("content"))):
            self.stats["edits"] += 1

    async def on_raw_message_delete(self, payload, bot):
        gid = self._guild_thread(payload, bot)
        if gid is None:
            return
        n = await self._call(gid, lambda c: ingest_db.tombstone_delete_messages(c, [payload.message_id]))
        if n:
            self.stats["deletes"] += n
            log.info("网关删除消息 guild=%s msg=%s", gid, payload.message_id)

    async def on_raw_bulk_message_delete(self, payload, bot):
        gid = self._guild_thread(payload, bot)
        if gid is None:
            return
        n = await self._call(gid, lambda c: ingest_db.tombstone_delete_messages(c, list(payload.message_ids)))
        if n:
            self.stats["deletes"] += n

    async def on_raw_reaction_add(self, payload, bot):
        gid = self._guild_thread(payload, bot)
        if gid is None:
            return
        name, custom = _emoji(payload.emoji)
        member = getattr(payload, "member", None)

        def fn(conn):
            if not conn.execute("SELECT 1 FROM messages WHERE message_id=?", (str(payload.message_id),)).fetchone():
                return False  # message not in DB (untracked / not yet backfilled)
            if member is not None:
                ingest_db.upsert_user(conn, member.id, member.name, getattr(member, "nick", None) or member.global_name or "",
                                      _avatar(member), member.bot)
            return ingest_db.add_reaction(conn, payload.message_id, payload.user_id, name,
                                          ingest_db.emoji_url_for(conn, name, custom))
        if await self._call(gid, fn):
            self.stats["react_add"] += 1

    async def on_raw_reaction_remove(self, payload, bot):
        gid = self._guild_thread(payload, bot)
        if gid is None:
            return
        name, _ = _emoji(payload.emoji)
        if await self._call(gid, lambda c: ingest_db.remove_reaction(c, payload.message_id, payload.user_id, name)):
            self.stats["react_remove"] += 1

    async def on_raw_reaction_clear(self, payload, bot):
        gid = self._guild_thread(payload, bot)
        if gid is not None:
            await self._call(gid, lambda c: ingest_db.clear_reactions(c, payload.message_id))

    async def on_raw_reaction_clear_emoji(self, payload, bot):
        gid = self._guild_thread(payload, bot)
        if gid is not None:
            name, _ = _emoji(payload.emoji)
            await self._call(gid, lambda c: ingest_db.clear_reactions(c, payload.message_id, name))

    # ---------------------------------------------------------- backfill
    def enqueue_backfill(self):
        """Queue one offline-gap backfill task per enabled update config (downloader CLI)."""
        script = os.path.join(self.base_dir, "Preparation_Before_Use", "discord_downloader.py")
        try:
            out = subprocess.run([sys.executable, script, "--enqueue-backfill"], cwd=self.base_dir,
                                 capture_output=True, text=True, timeout=120)
            log.info("网关上线补齐入队 rc=%s created=%s", out.returncode, (out.stdout or "").strip()[-20:])
            if out.returncode:
                log.warning("补齐入队失败：%s", (out.stderr or "")[-500:])
        except Exception:
            log.exception("补齐入队失败")

    def presence_report(self, bot):
        lines = []
        guild_ids = {str(g.id) for g in bot.guilds}
        for gid, forums in self.forums().items():
            g = bot.get_guild(int(gid))
            lines.append("guild=%s in_guild=%s forums=%s" % (gid, gid in guild_ids,
                         ",".join("%s:%s" % (f, bool(g and g.get_channel(int(f)))) for f in sorted(forums))))
        return lines
