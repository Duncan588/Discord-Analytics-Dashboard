"""Discord forum/thread context for AstrBot (Discord Analytics MCP + Discord API fallback)."""
import asyncio
import json
import random
import time

import aiohttp

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.provider import ProviderRequest
from astrbot.api.star import Context, Star, register

try:
    import discord
except Exception:  # pragma: no cover
    discord = None

MARK = "[Discord 上下文]"
UA = "AstrBot-discord-context/0.1 (+aiohttp)"  # Cloudflare blocks the default Python-urllib UA


def _clip(s, n):
    s = (s or "").replace("\n", " ").strip()
    return s if len(s) <= n else s[: max(0, n - 1)] + "…"


@register("astrbot_plugin_discord_context", "Duncan588", "Discord 论坛帖上下文注入 (MCP)", "0.2.0")
class DiscordContext(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.cfg = config
        self._session = None
        self._cache = {}
        self._last_proactive = 0.0
        self._rpc_id = 0
        self._typing = {}  # session key -> (generation, task)
        self._typing_gen = 0

    # ------------------------------------------------------------------ MCP
    async def _http(self):
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(headers={"User-Agent": UA})
        return self._session

    async def mcp(self, tool, args):
        token = (self.cfg.get("mcp_token") or "").strip()
        url = (self.cfg.get("mcp_url") or "").strip()
        if not token or not url:
            raise RuntimeError("mcp_url/mcp_token not configured")
        self._rpc_id += 1
        body = {"jsonrpc": "2.0", "id": self._rpc_id, "method": "tools/call",
                "params": {"name": tool, "arguments": args}}
        s = await self._http()
        async with s.post(url, json=body, timeout=aiohttp.ClientTimeout(total=float(self.cfg.get("timeout_sec", 6))),
                          headers={"Authorization": f"Bearer {token}", "Accept": "application/json, text/event-stream"}) as r:
            if r.status != 200:
                raise RuntimeError(f"MCP HTTP {r.status}")
            text = await r.text()
        if text.lstrip().startswith("event:") or text.lstrip().startswith("data:"):
            text = next((ln[5:].strip() for ln in text.splitlines() if ln.startswith("data:")), "{}")
        data = json.loads(text)
        if "error" in data:
            raise RuntimeError(f"MCP error {data['error']}")
        res = data.get("result") or {}
        if res.get("isError"):
            raise RuntimeError("MCP tool error: " + "".join(c.get("text", "") for c in res.get("content", []))[:200])
        if res.get("structuredContent") is not None:
            sc = res["structuredContent"]
            return sc.get("result", sc) if isinstance(sc, dict) and set(sc) == {"result"} else sc
        parts = [c.get("text", "") for c in res.get("content", []) if c.get("type") == "text"]
        try:
            return json.loads(parts[0]) if len(parts) == 1 else [json.loads(p) for p in parts]
        except ValueError:
            return "\n".join(parts)

    async def _cached(self, key, coro_fn):
        ttl = int(self.cfg.get("cache_ttl_sec", 20))
        hit = self._cache.get(key)
        if hit and time.monotonic() - hit[0] < ttl:
            return hit[1]
        val = await coro_fn()
        self._cache[key] = (time.monotonic(), val)
        if len(self._cache) > 200:
            self._cache.pop(next(iter(self._cache)))
        return val

    # ------------------------------------------------------------------ formatting
    def _fmt_msgs(self, msgs, n):
        out = []
        for m in msgs[-n:]:
            who = m.get("author") or m.get("author_id")
            line = f"- {who}: {_clip(m.get('content'), int(self.cfg.get('msg_max_chars', 200)))}"
            rc = m.get("reply_to_context")
            if rc and not rc.get("missing"):
                line += f"  (回复 {rc.get('author')}: {_clip(rc.get('snippet'), 40)})"
            out.append(line)
        return out

    def _fmt_thread(self, ctx):
        t = ctx.get("thread") or {}
        lines = [f"当前所在论坛帖：《{t.get('title')}》 楼主 {t.get('author')}，发帖 {str(t.get('created_at'))[:16]}，"
                 f"{t.get('replies')} 条回复 / {t.get('participants')} 人参与"
                 + (f"，标签 {','.join(t.get('tags') or [])}" if t.get("tags") else "")]
        sp = ctx.get("starter_post")
        if sp:
            lines.append("楼主正文：" + _clip(sp.get("content"), int(self.cfg.get("starter_max_chars", 1200))))
        msgs = ctx.get("recent_messages") or []
        if msgs:
            lines.append(f"最近 {len(msgs)} 条回复（旧→新）：")
            lines += self._fmt_msgs(msgs, len(msgs))
        k = int(self.cfg.get("include_participants", 5))
        if k > 0 and ctx.get("participants"):
            ps = [f"{p.get('name')}({p.get('messages_in_thread')}条{','+p['activity_tier'] if p.get('activity_tier') else ''})"
                  for p in ctx["participants"][:k]]
            lines.append("本帖活跃者：" + "、".join(ps))
        return lines

    def _fmt_profile(self, p, display):
        if not isinstance(p, dict):
            return []
        bits = [f"提问者 {display or p.get('nickname') or p.get('username')}"]
        if p.get("msg_count") is not None:
            bits.append(f"累计发言 {p['msg_count']} 条(排名 {p.get('rank_by_messages')})")
        if p.get("first_message"):
            bits.append(f"首次发言 {str(p['first_message'])[:10]}")
        if p.get("activity_tier_label"):
            bits.append(f"活跃分层 {p['activity_tier_label']}")
        chans = [c.get("channel") for c in (p.get("top_channels") or [])[:3] if c.get("channel")]
        if chans:
            bits.append("常去的帖：" + "、".join(_clip(c, 20) for c in chans))
        return ["；".join(bits)]

    # ------------------------------------------------------------------ discord helpers
    @staticmethod
    def _raw(event):
        return getattr(getattr(event, "message_obj", None), "raw_message", None)

    @staticmethod
    def _is_thread(ch):
        return discord is not None and isinstance(ch, discord.Thread)

    async def _discord_history(self, ch, n, skip_id=None):
        out = []
        async for m in ch.history(limit=n + 1):
            if skip_id and str(m.id) == str(skip_id):
                continue
            out.append({"author": getattr(m.author, "display_name", None) or m.author.name, "content": m.content or
                        ("[附件]" if m.attachments else "")})
        return list(reversed(out))[-n:]

    async def build_context(self, event):
        raw = self._raw(event)
        ch = getattr(raw, "channel", None)
        if ch is None or getattr(raw, "guild", None) is None:
            return None
        gid = str(raw.guild.id)
        uid = str(raw.author.id)
        lines, src = [], "mcp"
        if self._is_thread(ch):
            tid = str(ch.id)
            try:
                ctx = await self._cached(("t", tid), lambda: self.mcp("get_thread_context", {
                    "thread_id": tid, "guild_id": gid, "last_n": int(self.cfg.get("thread_last_n", 20))}))
                lines += self._fmt_thread(ctx)
            except Exception as e:
                logger.warning(f"[discord_context] MCP get_thread_context failed: {e}")
                if not self.cfg.get("fallback_discord_api", True):
                    return None
                src = "discord"
                lines.append(f"当前所在帖子/子区：《{getattr(ch, 'name', '')}》")
                try:
                    starter = await ch.fetch_message(ch.id)  # forum post starter shares the thread id
                except Exception:
                    starter = None
                if starter:
                    lines.append(f"楼主 {starter.author.display_name} 正文：" +
                                 _clip(starter.content, int(self.cfg.get("starter_max_chars", 1200))))
                hist = await self._discord_history(ch, int(self.cfg.get("thread_last_n", 20)), skip_id=raw.id)
                lines.append(f"最近 {len(hist)} 条消息（旧→新）：")
                lines += self._fmt_msgs(hist, len(hist))
        else:
            src = "discord"
            hist = await self._discord_history(ch, int(self.cfg.get("channel_last_n", 15)), skip_id=raw.id)
            lines.append(f"当前频道 #{getattr(ch, 'name', '')} 最近 {len(hist)} 条消息（旧→新）：")
            lines += self._fmt_msgs(hist, len(hist))
        if self.cfg.get("include_user_profile", True):
            try:
                p = await self._cached(("u", uid), lambda: self.mcp("get_member_profile", {
                    "user_id": uid, "guild_id": gid, "recent": 0}))
                lines += self._fmt_profile(p, getattr(raw.author, "display_name", None))
            except Exception as e:
                logger.debug(f"[discord_context] profile failed: {e}")
        text = "\n".join(lines)
        mx = int(self.cfg.get("max_chars", 3500))
        if len(text) > mx:
            text = text[: mx - 1] + "…"
        return text, src

    # ------------------------------------------------------------------ typing keepalive
    # Ported from astrbot_plugin_stoprushingiamtyping (Melantilla.): keep Discord's "typing…" indicator alive
    # from LLM request until the reply is sent, refreshed every N seconds, capped by a max window.
    async def _typing_loop(self, key, gen, channel):
        loop = asyncio.get_running_loop()
        start = loop.time()
        every = max(3.0, float(self.cfg.get("typing_keepalive_seconds", 8.0)))
        window = max(10.0, float(self.cfg.get("typing_max_window_seconds", 120.0)))
        try:
            while loop.time() - start < window:
                cur = self._typing.get(key)
                if not cur or cur[0] != gen:
                    return
                await channel.trigger_typing()
                await asyncio.sleep(every)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            if self.cfg.get("debug_log"):
                logger.info(f"[discord_context] trigger_typing failed: {e}")
        finally:
            cur = self._typing.get(key)
            if cur and cur[0] == gen:
                self._typing.pop(key, None)

    async def _typing_start(self, event):
        if not self.cfg.get("typing_enable", True):
            return
        ch = getattr(self._raw(event), "channel", None)
        if ch is None or not hasattr(ch, "trigger_typing"):
            return
        key = event.unified_msg_origin
        cur = self._typing.get(key)
        if cur and not cur[1].done():
            return
        self._typing_gen += 1
        gen = self._typing_gen
        self._typing[key] = (gen, asyncio.create_task(self._typing_loop(key, gen, ch)))

    async def _typing_stop(self, key):
        cur = self._typing.pop(key, None)
        if cur and not cur[1].done():
            cur[1].cancel()
            try:
                await cur[1]
            except (asyncio.CancelledError, Exception):
                pass

    @filter.after_message_sent()
    async def after_sent(self, event: AstrMessageEvent):
        await self._typing_stop(event.unified_msg_origin)

    # ------------------------------------------------------------------ hooks
    @filter.on_llm_request()
    async def inject(self, event: AstrMessageEvent, req: ProviderRequest):
        if event.get_platform_name() != "discord":
            return
        await self._typing_start(event)
        if not self.cfg.get("enable", True):
            return
        try:
            res = await asyncio.wait_for(self.build_context(event), timeout=float(self.cfg.get("timeout_sec", 6)) * 2 + 2)
        except Exception as e:
            logger.warning(f"[discord_context] build failed: {e}")
            return
        if not res:
            return
        text, src = res
        block = (f"\n\n{MARK}（来源:{src}；以下为背景资料，仅供理解对话，不要逐条复述）\n{text}\n"
                 "回答时结合帖子主题和上下文，自然地接话。")
        req.system_prompt = (req.system_prompt or "") + block
        logger.info(f"[discord_context] injected {len(text)} chars via {src} session={event.session_id}")
        if self.cfg.get("debug_log"):
            logger.info(block)

    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def proactive(self, event: AstrMessageEvent):
        if not self.cfg.get("proactive_enable", False) or event.get_platform_name() != "discord":
            return
        if event.is_at_or_wake_command:
            return
        raw = self._raw(event)
        ch = getattr(raw, "channel", None)
        if raw is None or not self._is_thread(ch) or raw.author.bot or str(raw.id) != str(ch.id):
            return  # only the starter message of a new forum post
        forums = [str(x) for x in (self.cfg.get("proactive_forum_ids") or [])]
        if forums and str(ch.parent_id) not in forums:
            return
        if len(raw.content or "") < int(self.cfg.get("proactive_min_chars", 15)):
            return
        now = time.monotonic()
        if now - self._last_proactive < int(self.cfg.get("proactive_cooldown_sec", 600)):
            return
        if random.random() > float(self.cfg.get("proactive_probability", 0.2)):
            return
        self._last_proactive = now
        logger.info(f"[discord_context] proactive reply in thread {ch.id}")
        yield event.request_llm(prompt=self.cfg.get("proactive_prompt") + "\n\n帖子标题：" + (ch.name or "") +
                                "\n正文：" + _clip(raw.content, 800))

    async def terminate(self):
        for k in list(self._typing):
            await self._typing_stop(k)
        if self._session and not self._session.closed:
            await self._session.close()
