"""Discord REST API 常量与请求工具。"""
import os

import requests

API_BASE_URL = "https://discord.com/api/v10"
CDN_BASE_URL = "https://cdn.discordapp.com"
USER_AGENT = "Discord-Analytics-Dashboard/1.0"
DEFAULT_AVATAR_URL = f"{CDN_BASE_URL}/embed/avatars/0.png"


def bot_headers(token, json_content=False):
    headers = {"Authorization": f"Bot {token}", "User-Agent": USER_AGENT}
    if json_content:
        headers["Content-Type"] = "application/json"
    return headers


def bearer_headers(access_token):
    return {"Authorization": f"Bearer {access_token}", "User-Agent": USER_AGENT}


def bot_get(path, token, timeout=12, **kwargs):
    """以机器人身份 GET Discord API；path 为 /users/@me 这样的相对路径。"""
    return requests.get(f"{API_BASE_URL}{path}", headers=bot_headers(token), timeout=timeout, **kwargs)


def bot_post(path, token, timeout=15, **kwargs):
    return requests.post(
        f"{API_BASE_URL}{path}", headers=bot_headers(token, json_content=True), timeout=timeout, **kwargs
    )


def default_downloader_token():
    """默认下载机器人令牌（服务器环境变量）。"""
    return (os.getenv("DISCORD_DOWNLOADER_TOKEN") or os.getenv("DISCORD_DOWNLOADER") or "").strip()


def any_bot_token():
    """任意可用的机器人令牌：优先主程序机器人，其次默认下载机器人。"""
    return (os.getenv("DISCORD_BOT_TOKEN") or "").strip() or default_downloader_token()


def user_avatar_url(user_id, avatar_hash, size=None, default=DEFAULT_AVATAR_URL):
    if not avatar_hash or not user_id:
        return default
    suffix = f"?size={size}" if size else ""
    return f"{CDN_BASE_URL}/avatars/{user_id}/{avatar_hash}.png{suffix}"


def guild_icon_url(guild_id, icon_hash, size=64, default=DEFAULT_AVATAR_URL):
    if not icon_hash or not guild_id:
        return default
    ext = "gif" if str(icon_hash).startswith("a_") else "png"
    suffix = f"?size={size}" if size else ""
    return f"{CDN_BASE_URL}/icons/{guild_id}/{icon_hash}.{ext}{suffix}"


def send_direct_message(token, user_id, content, timeout=15):
    """以机器人身份给用户发私信；失败时抛出 RuntimeError。"""
    channel_response = bot_post("/users/@me/channels", token, timeout=timeout, json={"recipient_id": str(user_id)})
    if channel_response.status_code not in (200, 201):
        raise RuntimeError(f"创建私信频道失败 HTTP {channel_response.status_code}")
    channel_id = channel_response.json().get("id")
    if not channel_id:
        raise RuntimeError("Discord 没有返回私信频道 ID")
    message_response = bot_post(
        f"/channels/{channel_id}/messages", token, timeout=timeout, json={"content": content}
    )
    if message_response.status_code not in (200, 201):
        raise RuntimeError(f"发送消息失败 HTTP {message_response.status_code}")
    return channel_id
