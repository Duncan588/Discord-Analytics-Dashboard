"""Small Discord REST helpers shared by the web app and workers."""

import os

import requests


API_BASE_URL = "https://discord.com/api/v10"
_DEFAULT_USER_AGENT = "Discord-Analytics-Dashboard/1.0"


def bot_headers(token):
    return {"Authorization": f"Bot {token}", "User-Agent": _DEFAULT_USER_AGENT}


def discord_headers(token, token_type="bot"):
    """Build headers for an official bot token or a user token.

    DiscordChatExporter accepts both token kinds, but Discord's REST API does
    not: a user token must be sent as the raw Authorization value while a bot
    token uses the ``Bot`` scheme.
    """
    if str(token_type or "bot").lower() == "user":
        return {"Authorization": str(token), "User-Agent": _DEFAULT_USER_AGENT}
    return bot_headers(token)


def bearer_headers(token):
    return {"Authorization": f"Bearer {token}", "User-Agent": _DEFAULT_USER_AGENT}


def bot_get(path, token, **kwargs):
    return requests.get(f"{API_BASE_URL}{path}", headers=bot_headers(token), **kwargs)


def discord_get(path, token, token_type="bot", **kwargs):
    return requests.get(
        f"{API_BASE_URL}{path}",
        headers=discord_headers(token, token_type),
        **kwargs,
    )


def default_downloader_token():
    return (os.getenv("DISCORD_DOWNLOADER_TOKEN") or os.getenv("DISCORD_DOWNLOADER") or "").strip()


def any_bot_token():
    return (
        os.getenv("DISCORD_BOT_TOKEN")
        or default_downloader_token()
        or ""
    ).strip()


def guild_icon_url(guild_id, icon_hash, size=64):
    if guild_id and icon_hash:
        extension = "gif" if str(icon_hash).startswith("a_") else "png"
        return f"https://cdn.discordapp.com/icons/{guild_id}/{icon_hash}.{extension}?size={size}"
    return "https://cdn.discordapp.com/embed/avatars/0.png"


def user_avatar_url(user_id, avatar_hash, size=64, default="https://cdn.discordapp.com/embed/avatars/0.png"):
    if user_id and avatar_hash:
        extension = "gif" if str(avatar_hash).startswith("a_") else "png"
        return f"https://cdn.discordapp.com/avatars/{user_id}/{avatar_hash}.{extension}?size={size}"
    return default


def send_direct_message(token, user_id, content):
    """Open a Discord DM channel and send one text message."""
    response = requests.post(
        f"{API_BASE_URL}/users/@me/channels",
        headers=bot_headers(token),
        json={"recipient_id": str(user_id)},
        timeout=20,
    )
    response.raise_for_status()
    channel_id = response.json().get("id")
    if not channel_id:
        raise RuntimeError("Discord DM channel response did not contain an id")
    message = requests.post(
        f"{API_BASE_URL}/channels/{channel_id}/messages",
        headers=bot_headers(token),
        json={"content": str(content)},
        timeout=20,
    )
    message.raise_for_status()
    return message
