#获取论坛帖子ID用于DiscordChatExporter进行备份
import os
import csv
import discord
from zoneinfo import ZoneInfo

FORUM_CHANNEL_ID = 1019924310665728022 #这是你的论坛ID
OUTPUT_FILE = "all_threads.csv"
TZ = ZoneInfo("Asia/Shanghai")

def to_shanghai(dt):
    if not dt:
        return ""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZoneInfo("UTC"))
    return dt.astimezone(TZ).isoformat()

def snowflake_time_to_shanghai(snowflake_id):
    if not snowflake_id:
        return ""
    return to_shanghai(discord.utils.snowflake_time(int(snowflake_id)))

class MyClient(discord.Client):
    async def on_ready(self):
        print(f"已登录: {self.user}")

        channel = self.get_channel(FORUM_CHANNEL_ID)
        if not isinstance(channel, discord.ForumChannel):
            print("错误：该 ID 不是论坛频道或无权访问")
            await self.close()
            return

        print(f"开始抓取论坛 {channel.name} 的帖子，含活跃与归档")

        fieldnames = [
            "id",
            "name",
            "created_at_shanghai",
            "last_message_at_shanghai",
            "archived",
            "locked",
            "message_count",
            "member_count",
            "owner_id",
        ]

        count = 0
        seen = set()

        with open(OUTPUT_FILE, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            def write_thread(t: discord.Thread):
                nonlocal count
                if t.id in seen:
                    return
                seen.add(t.id)

                writer.writerow({
                    "id": t.id,
                    "name": t.name,
                    "created_at_shanghai": to_shanghai(t.created_at),
                    "last_message_at_shanghai": snowflake_time_to_shanghai(t.last_message_id),
                    "archived": t.archived,
                    "locked": t.locked,
                    "message_count": getattr(t, "message_count", ""),
                    "member_count": getattr(t, "member_count", ""),
                    "owner_id": getattr(t, "owner_id", ""),
                })
                f.flush()
                count += 1
                print(f"[{count}] {'归档' if t.archived else '活跃'}: {t.name}")

            for t in channel.threads:
                write_thread(t)

            async for t in channel.archived_threads(limit=None):
                write_thread(t)

            try:
                async for t in channel.archived_threads(limit=None, private=True):
                    write_thread(t)
            except TypeError:
                pass

        print(f"完成，总计 {count} 个帖子，已输出到 {OUTPUT_FILE}")
        await self.close()

intents = discord.Intents.default()
intents.guilds = True

token = "DISCORD_TOKEN" #在此处设置你的机器人密钥
if not token:
    raise SystemExit("请先设置环境变量 DISCORD_TOKEN")

MyClient(intents=intents).run(token)
