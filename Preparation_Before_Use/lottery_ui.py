"""Discord UI for /抽奖 and /抽奖结果.

Commands are registered on the production guild (LEGACY_SERVER_ID, the same
guild as the liveness checks) so they appear as soon as this bot process
starts. Draws run on the bot's task loop; buttons survive restarts via
dynamic custom_ids.
"""
from __future__ import annotations

import asyncio
import logging
import os
from collections import deque

import discord
from discord import app_commands
from discord.ext import tasks

from shared import liveness
from shared import lottery

log = logging.getLogger("discord-bot.lottery")

_SEEN = deque(maxlen=4096)
_SEEN_SET = set()
_LOCKS = {}
_REFRESH = {}
_PENDING = set()
MENTIONS = discord.AllowedMentions(everyone=False, users=True, roles=False, replied_user=False)


def prod_guild_id():
    raw = (os.getenv("LEGACY_SERVER_ID") or "").strip()
    if raw.isdigit():
        return int(raw)
    return int(liveness.LIVENESS_GUILD_ID)


def _db():
    return lottery.default_db_path()


def _servers():
    return lottery.servers_dir()


def _claim(interaction_id):
    if interaction_id in _SEEN_SET:
        return False
    if _SEEN.maxlen and len(_SEEN) == _SEEN.maxlen:
        old = _SEEN.popleft()
        _SEEN_SET.discard(old)
    _SEEN.append(interaction_id)
    _SEEN_SET.add(interaction_id)
    return True


def _lock(lottery_id):
    lock = _LOCKS.get(lottery_id)
    if lock is None:
        lock = asyncio.Lock()
        _LOCKS[lottery_id] = lock
    return lock


def to_embed(payload):
    embed = discord.Embed(title=payload["title"], description=payload["description"] or None, color=payload["color"])
    for name, value, inline in payload["fields"]:
        embed.add_field(name=name, value=value or "（无）", inline=inline)
    if payload.get("footer"):
        embed.set_footer(text=payload["footer"])
    return embed


class LotteryButton(discord.ui.DynamicItem[discord.ui.Button], template=r"lottery:(?P<action>join|leave|draw|cancel):(?P<lid>[0-9]+)"):
    _LABELS = {
        "join": (lottery.BTN_JOIN, discord.ButtonStyle.success),
        "leave": (lottery.BTN_LEAVE, discord.ButtonStyle.secondary),
        "draw": (lottery.BTN_DRAW, discord.ButtonStyle.primary),
        "cancel": (lottery.BTN_CANCEL, discord.ButtonStyle.danger),
    }

    def __init__(self, lottery_id: int, action: str):
        label, style = self._LABELS[action]
        super().__init__(discord.ui.Button(label=label, style=style, custom_id=f"lottery:{action}:{int(lottery_id)}"))
        self.lottery_id = int(lottery_id)
        self.action = action

    @classmethod
    async def from_custom_id(cls, interaction, item, match):
        return cls(int(match["lid"]), match["action"])

    async def callback(self, interaction: discord.Interaction):
        if not _claim(interaction.id):
            return
        if self.action in ("join", "leave"):
            await self._membership(interaction)
        else:
            await self._preview(interaction, self.action)

    async def _membership(self, interaction: discord.Interaction):
        # thinking=True so a button ack stays ephemeral. A plain defer() would
        # edit the public panel (deferred message update).
        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            if self.action == "join":
                result = await asyncio.to_thread(
                    lottery.handle_join, _db(), _servers(), self.lottery_id,
                    interaction.user.id, interaction.user.name, bool(interaction.user.bot),
                )
            else:
                result = await asyncio.to_thread(lottery.handle_leave, _db(), self.lottery_id, interaction.user.id)
        except Exception:
            log.exception("抽奖参与操作失败 id=%s action=%s", self.lottery_id, self.action)
            await interaction.followup.send("操作失败，请稍后再试。", ephemeral=True)
            return
        try:
            await interaction.followup.send(result["text"], ephemeral=True)
        finally:
            if result.get("refresh"):
                schedule_refresh(interaction.client, self.lottery_id)

    async def _preview(self, interaction: discord.Interaction, kind: str):
        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            result = await asyncio.to_thread(lottery.preview_action, _db(), self.lottery_id, interaction.user.id, kind)
        except Exception:
            log.exception("抽奖预览失败 id=%s kind=%s", self.lottery_id, kind)
            await interaction.followup.send("操作失败，请稍后再试。", ephemeral=True)
            return
        kwargs = {"ephemeral": True}
        if result.get("confirm"):
            kwargs["view"] = ConfirmView(self.lottery_id, kind)
        await interaction.followup.send(result["text"], **kwargs)


class ConfirmView(discord.ui.View):
    def __init__(self, lottery_id: int, kind: str):
        super().__init__(timeout=180)
        self.lottery_id = int(lottery_id)
        self.kind = kind
        label = lottery.BTN_CONFIRM_DRAW if kind == "draw" else lottery.BTN_CONFIRM_CANCEL
        button = discord.ui.Button(label=label, style=discord.ButtonStyle.danger, custom_id=f"lotconf:{kind}:{lottery_id}")
        button.callback = self._confirm
        self.add_item(button)

    async def _confirm(self, interaction: discord.Interaction):
        if not _claim(interaction.id):
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            if self.kind == "draw":
                result = await asyncio.to_thread(lottery.commit_draw, _db(), self.lottery_id, "early", interaction.user.id)
            else:
                result = await asyncio.to_thread(lottery.commit_cancel, _db(), self.lottery_id, interaction.user.id)
        except Exception:
            log.exception("抽奖确认失败 id=%s kind=%s", self.lottery_id, self.kind)
            await interaction.followup.send("操作失败，请稍后再试。", ephemeral=True)
            return
        if self.kind == "draw" and result.get("code") == "drawn":
            log.info(
                "抽奖提前开奖 id=%s actor=%s participants=%s winners=%s hash=%s",
                self.lottery_id, interaction.user.id, result.get("count"), result.get("winners"), result.get("hash"),
            )
        elif result.get("changed"):
            log.info("抽奖取消 id=%s actor=%s", self.lottery_id, interaction.user.id)
        if result.get("refresh"):
            await edit_panel(interaction.client, self.lottery_id)
        await interaction.followup.send(result["text"], ephemeral=True)


def panel_view(lottery_id: int) -> discord.ui.View:
    view = discord.ui.View(timeout=None)
    for action in ("join", "leave", "draw", "cancel"):
        view.add_item(LotteryButton(lottery_id, action))
    return view


def schedule_refresh(bot, lottery_id: int):
    task = _REFRESH.get(lottery_id)
    if task is not None and not task.done():
        _PENDING.add(lottery_id)
        return
    _REFRESH[lottery_id] = asyncio.create_task(_refresh_soon(bot, lottery_id))


async def _refresh_soon(bot, lottery_id: int):
    try:
        await asyncio.sleep(0.8)
        await edit_panel(bot, lottery_id)
    except Exception:
        log.exception("刷新抽奖面板失败 id=%s", lottery_id)
    finally:
        _REFRESH.pop(lottery_id, None)
        if lottery_id in _PENDING:
            _PENDING.discard(lottery_id)
            schedule_refresh(bot, lottery_id)


async def edit_panel(bot, lottery_id: int):
    async with _lock(lottery_id):
        snap = await asyncio.to_thread(lottery.load, _db(), lottery_id)
        if not snap or not snap.get("message_id") or not snap.get("channel_id"):
            return
        embed = to_embed(lottery.panel_payload(snap))
        view = panel_view(lottery_id) if snap["status"] == lottery.OPEN else None
        try:
            channel = bot.get_channel(int(snap["channel_id"]))
            if channel is None:
                channel = await bot.fetch_channel(int(snap["channel_id"]))
            message = await channel.fetch_message(int(snap["message_id"]))
            await message.edit(embed=embed, view=view, allowed_mentions=MENTIONS)
        except discord.NotFound:
            log.info("抽奖原消息不存在 id=%s", lottery_id)
        except discord.Forbidden:
            log.warning("无权编辑抽奖消息 id=%s channel=%s", lottery_id, snap["channel_id"])
        except discord.HTTPException:
            log.exception("编辑抽奖消息失败 id=%s", lottery_id)


async def draw_due(bot):
    ids = await asyncio.to_thread(lottery.due_ids, _db())
    for lid in ids:
        try:
            result = await asyncio.to_thread(lottery.commit_draw, _db(), lid, "auto", None)
        except Exception:
            log.exception("抽奖自动开奖失败 id=%s", lid)
            continue
        if not result.get("changed"):
            continue
        log.info(
            "抽奖到点结束 id=%s code=%s participants=%s winners=%s hash=%s",
            lid, result.get("code"), result.get("count"), result.get("winners"), result.get("hash"),
        )
        await edit_panel(bot, lid)


def start_draw_loop(bot):
    if getattr(bot, "lottery_draw_loop", None) is not None:
        return

    @tasks.loop(seconds=15)
    async def lottery_draw_loop():
        try:
            await draw_due(bot)
        except Exception:
            log.exception("抽奖到点扫描失败")

    @lottery_draw_loop.before_loop
    async def _wait_ready():
        await bot.wait_until_ready()

    lottery_draw_loop.start()
    bot.lottery_draw_loop = lottery_draw_loop


async def sync_guild_commands(bot):
    """Upsert /抽奖 and /抽奖结果 onto the production guild.

    Guild upsert shows the commands immediately and does not bulk-replace the
    guild command list, so commands added outside this repo stay registered.
    Global commands are untouched (the Activity entry-point bulk sync stays in
    whitelist_bot.setup_hook).
    """
    guild_id = prod_guild_id()
    guild = discord.Object(id=guild_id)
    if bot.application_id is None:
        log.warning("没有 application_id，跳过抽奖命令同步")
        return
    commands = bot.tree._get_all_commands(guild=guild)
    ok = 0
    for command in commands:
        try:
            payload = command.to_dict(bot.tree)
            await bot.tree._http.upsert_guild_command(bot.application_id, guild_id, payload=payload)
            ok += 1
        except discord.HTTPException:
            log.exception("注册服务器命令失败 name=%s", getattr(command, "name", "?"))
    log.info("生产服务器命令已同步 guild=%s upserted=%s/%s names=%s", guild_id, ok, len(commands), [getattr(c, "name", "?") for c in commands])


async def setup(bot):
    bot.add_dynamic_items(LotteryButton)
    try:
        await sync_guild_commands(bot)
    finally:
        start_draw_loop(bot)


def register(bot):
    guild = discord.Object(id=prod_guild_id())

    @bot.tree.command(
        name="抽奖",
        description="发起一场公平抽奖（按钮参与，到点或提前开奖）",
        guild=guild,
        auto_locale_strings=False,
    )
    @app_commands.guild_only()
    @app_commands.rename(title="标题", prize="奖品内容", draw_time="开奖时间", winner_count="中奖人数")
    @app_commands.describe(
        title="抽奖标题",
        prize="奖品内容",
        draw_time="开奖时间（北京时间），例如 2026-11-06 21:00",
        winner_count="中奖人数（1 到 40）",
    )
    async def lottery_cmd(
        interaction: discord.Interaction,
        title: str,
        prize: str,
        draw_time: str,
        winner_count: app_commands.Range[int, 1, lottery.MAX_WINNERS],
    ):
        await _create(interaction, title, prize, draw_time, int(winner_count))

    @bot.tree.command(
        name="抽奖结果",
        description="按抽奖 ID 查看结果或当前状态",
        guild=guild,
        auto_locale_strings=False,
    )
    @app_commands.rename(lottery_id="抽奖id")
    @app_commands.describe(lottery_id="面板底部的抽奖 ID")
    async def lottery_result_cmd(
        interaction: discord.Interaction,
        lottery_id: app_commands.Range[int, 1, 2_000_000_000],
    ):
        await _result(interaction, int(lottery_id))


async def _create(interaction: discord.Interaction, title: str, prize: str, draw_time: str, winner_count: int):
    if interaction.guild is None or interaction.channel_id is None:
        await interaction.response.send_message(lottery.GUILD_ONLY, ephemeral=True)
        return
    now = lottery.utc_now()
    try:
        when = lottery.parse_draw_at(draw_time, now)
    except lottery.TimeParseError as exc:
        await interaction.response.send_message(str(exc), ephemeral=True)
        return
    try:
        snap = lottery.create_lottery(
            _db(), interaction.guild.id, interaction.channel_id, interaction.user.id,
            interaction.user.name, title, prize, winner_count, when, now,
        )
    except lottery.LotteryError as exc:
        await interaction.response.send_message(str(exc), ephemeral=True)
        return
    except Exception:
        log.exception("创建抽奖失败 guild=%s user=%s", interaction.guild.id, interaction.user.id)
        await interaction.response.send_message("发起抽奖失败，请稍后再试。", ephemeral=True)
        return
    lid = int(snap["id"])
    embed = to_embed(lottery.panel_payload(snap))
    try:
        await interaction.response.send_message(embed=embed, view=panel_view(lid), allowed_mentions=MENTIONS)
    except discord.HTTPException:
        log.exception("发送抽奖面板失败 id=%s", lid)
        await asyncio.to_thread(lottery.abort_unposted, _db(), lid)
        return
    try:
        message = await interaction.original_response()
        await asyncio.to_thread(lottery.set_message, _db(), lid, message.id)
    except Exception:
        log.exception("抽奖已发出，但没有记下消息 ID id=%s", lid)
    log.info("抽奖创建 id=%s guild=%s creator=%s draw_at=%s winners=%s", lid, interaction.guild.id, interaction.user.id, snap["draw_at"], winner_count)


async def _result(interaction: discord.Interaction, lottery_id: int):
    guild_id = interaction.guild.id if interaction.guild else None
    try:
        found = await asyncio.to_thread(lottery.lookup, _db(), lottery_id, guild_id)
    except Exception:
        log.exception("查询抽奖结果失败 id=%s", lottery_id)
        await interaction.response.send_message("操作失败，请稍后再试。", ephemeral=True)
        return
    embed = to_embed(found["embed"]) if found.get("embed") else None
    await interaction.response.send_message(found["text"], embed=embed, ephemeral=True, allowed_mentions=MENTIONS)
