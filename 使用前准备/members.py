#此脚本用于下载成员信息
# requirements:
# pip install -U discord.py

import csv
import discord
from datetime import timezone

TOKEN = "机器人密钥"
GUILD_ID = 服务器id
CSV_FILE = "members.csv"

intents = discord.Intents.default()
intents.members = True  # 必须开启

client = discord.Client(intents=intents)

@client.event
async def on_ready():
    print(f"已登录: {client.user} ({client.user.id})")

    guild = client.get_guild(GUILD_ID)
    if guild is None:
        print("未找到指定服务器，请确认机器人已加入该服务器")
        await client.close()
        return

    with open(CSV_FILE, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow([
            "用户名",
            "用户ID",
            "账号创建时间(UTC)",
            "加入服务器时间(UTC)"
        ])

        for member in guild.members:
            writer.writerow([
                f"{member.name}#{member.discriminator}",
                member.id,
                member.created_at.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                member.joined_at.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                if member.joined_at else ""
            ])

    print(f"成员信息已导出到 {CSV_FILE}")
    await client.close()  # 导出完成后自动退出

client.run(TOKEN)
