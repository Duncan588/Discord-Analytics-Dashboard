import os
import aiosqlite
import discord
from discord import app_commands

intents = discord.Intents.default()
intents.message_content = True  # 必须开启，用于检测引用与消息内容
intents.guilds = True

client = discord.Client(intents=intents)
tree = app_commands.CommandTree(client)

DB_PATH = "blocklist.db"

# ==================== 数据库模块 ====================

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS user_blocks (
                blocker_id INTEGER NOT NULL,
                blocked_id INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (blocker_id, blocked_id)
            )
        """)
        await db.commit()

async def block_user(blocker_id: int, blocked_id: int) -> bool:
    if blocker_id == blocked_id:
        return False
    async with aiosqlite.connect(DB_PATH) as db:
        try:
            await db.execute(
                "INSERT INTO user_blocks (blocker_id, blocked_id) VALUES (?, ?)",
                (blocker_id, blocked_id)
            )
            await db.commit()
            return True
        except aiosqlite.IntegrityError:
            return False

async def unblock_user(blocker_id: int, blocked_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "DELETE FROM user_blocks WHERE blocker_id = ? AND blocked_id = ?",
            (blocker_id, blocked_id)
        )
        await db.commit()
        return cursor.rowcount > 0

async def check_mutual_block(sender_id: int, recipient_id: int) -> str | None:
    """
    检查双方是否存在拉黑关系：
    - 返回 'sender_blocked_recipient': 发送者主动拉黑了接收者
    - 返回 'recipient_blocked_sender': 接收者拉黑了发送者
    - 返回 'mutual': 双方互为拉黑
    - 返回 None: 无任何拉黑关系
    """
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            SELECT blocker_id FROM user_blocks 
            WHERE (blocker_id = ? AND blocked_id = ?) 
               OR (blocker_id = ? AND blocked_id = ?)
            """,
            (sender_id, recipient_id, recipient_id, sender_id)
        )
        rows = await cursor.fetchall()
        if not rows:
            return None

        blockers = {row[0] for row in rows}
        if sender_id in blockers and recipient_id in blockers:
            return "mutual"
        elif sender_id in blockers:
            return "sender_blocked_recipient"
        else:
            return "recipient_blocked_sender"

async def get_blocked_list(blocker_id: int) -> list[int]:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT blocked_id FROM user_blocks WHERE blocker_id = ?",
            (blocker_id,)
        )
        rows = await cursor.fetchall()
        return [row[0] for row in rows]

def parse_user_id(input_val: str) -> int | None:
    cleaned = input_val.strip("<@!> ")
    return int(cleaned) if cleaned.isdigit() else None

# ==================== 生命周期与双向拦截 ====================

@client.event
async def on_ready():
    await init_db()
    synced = await tree.sync()
    print(f"全局指令同步完成（{len(synced)} 个）。Bot 身份: {client.user}")

@client.event
async def on_message(message: discord.Message):
    if message.author.bot or not message.guild:
        return

    recipient_id = None

    # 1. 判定直接引用回复 (Reply)
    if message.reference and message.reference.message_id:
        try:
            ref_msg = message.reference.resolved
            if not isinstance(ref_msg, discord.Message):
                ref_msg = await message.channel.fetch_message(message.reference.message_id)
            if ref_msg:
                recipient_id = ref_msg.author.id
        except Exception:
            pass

    # 2. 判定论坛模式 (Forum Thread) 帖内普通跟帖 -> 默认目标为发帖楼主
    if not recipient_id and isinstance(message.channel, discord.Thread):
        if message.channel.parent and message.channel.parent.type == discord.ChannelType.forum:
            if message.channel.owner_id and message.channel.owner_id != message.author.id:
                recipient_id = message.channel.owner_id

    # 3. 双向拦截触发判定
    if recipient_id and recipient_id != message.author.id:
        block_relation = await check_mutual_block(sender_id=message.author.id, recipient_id=recipient_id)

        if block_relation:
            deleted_content = message.content
            channel_mention = message.channel.mention

            # 根据拉黑关系定制私聊提醒语
            if block_relation == "sender_blocked_recipient":
                reason = "你已将对方加入黑名单，无法对该用户进行回复。"
            elif block_relation == "recipient_blocked_sender":
                reason = "对方已将你拉黑，回复被自动拦截。"
            else:
                reason = "你与该用户处于相互拉黑状态，回复已被自动删除。"

            try:
                # 删除原消息
                await message.delete()

                # 私信将被删除的内容发还给发言者
                dm = await message.author.create_dm()
                await dm.send(
                    f"⚠️ **消息发送已被拦截**\n"
                    f"你在频道 {channel_mention} 中的回复已被自动删除。\n"
                    f"**拦截原因：** {reason}\n\n"
                    f"**你的回复原文：**\n"
                    f"> {deleted_content if deleted_content else '[包含附件/多媒体/非纯文本内容]'}"
                )
            except discord.Forbidden:
                pass  # 发言者关闭了服务器私信权限或 Bot 无删帖权限
            except Exception as e:
                print(f"处理拦截消息出错: {e}")

# ==================== 斜杠指令 (Slash Commands) ====================

@tree.command(name="block", description="拉黑用户（双方将无法在任何频道/论坛中互相回复）")
@app_commands.describe(user_id="输入对方的 User ID，或直接 @提及 对方")
async def block_slash(interaction: discord.Interaction, user_id: str):
    target_id = parse_user_id(user_id)
    if not target_id:
        await interaction.response.send_message("❌ 请输入正确的纯数字 ID 或直接 @提及 用户。", ephemeral=True)
        return

    if target_id == interaction.user.id:
        await interaction.response.send_message("❌ 你不能拉黑你自己。", ephemeral=True)
        return

    success = await block_user(interaction.user.id, target_id)
    if success:
        await interaction.response.send_message(f"✅ 已成功拉黑用户 ID: `{target_id}`，双方互相回复均会被自动拦截。", ephemeral=True)
    else:
        await interaction.response.send_message(f"ℹ️ 该用户 (`{target_id}`) 已在你的黑名单中。", ephemeral=True)

@tree.command(name="unblock", description="解除对指定用户的拉黑")
@app_commands.describe(user_id="输入要解除的 User ID，或直接 @提及 对方")
async def unblock_slash(interaction: discord.Interaction, user_id: str):
    target_id = parse_user_id(user_id)
    if not target_id:
        await interaction.response.send_message("❌ 请输入正确的纯数字 ID 或直接 @提及 用户。", ephemeral=True)
        return

    success = await unblock_user(interaction.user.id, target_id)
    if success:
        await interaction.response.send_message(f"✅ 已解除对用户 ID: `{target_id}` 的拉黑限制。", ephemeral=True)
    else:
        await interaction.response.send_message(f"ℹ️ 用户 ID (`{target_id}`) 不在你的黑名单中。", ephemeral=True)

@tree.command(name="list", description="查看你拉黑的用户列表")
async def list_slash(interaction: discord.Interaction):
    blocked_ids = await get_blocked_list(interaction.user.id)
    if not blocked_ids:
        await interaction.response.send_message("📋 你的黑名单目前为空。", ephemeral=True)
        return

    items = []
    for uid in blocked_ids:
        user = client.get_user(uid)
        name_str = f"**{user.name}**" if user else "已离线/未缓存用户"
        items.append(f"- {name_str} (`{uid}`)")

    content = "**当前黑名单列表：**\n" + "\n".join(items)
    if len(content) > 2000:
        content = content[:1990] + "\n..."
    await interaction.response.send_message(content, ephemeral=True)

# ==================== 右键/长按 Apps 选项 ====================

@tree.context_menu(name="拉黑此发言者")
async def context_block(interaction: discord.Interaction, message: discord.Message):
    target = message.author
    if target.id == interaction.user.id:
        await interaction.response.send_message("❌ 不能拉黑自己。", ephemeral=True)
        return
    if target.bot:
        await interaction.response.send_message("❌ 无法拉黑机器人。", ephemeral=True)
        return

    success = await block_user(interaction.user.id, target.id)
    msg = f"✅ 已将 **{target.name}** 加入黑名单（双方均不可互相回复）。" if success else f"ℹ️ **{target.name}** 已在黑名单中。"
    await interaction.response.send_message(msg, ephemeral=True)

@tree.context_menu(name="解除拉黑此发言者")
async def context_unblock(interaction: discord.Interaction, message: discord.Message):
    target = message.author
    success = await unblock_user(interaction.user.id, target.id)
    msg = f"✅ 已解除对 **{target.name}** 的拉黑限制。" if success else f"ℹ️ **{target.name}** 不在黑名单中。"
    await interaction.response.send_message(msg, ephemeral=True)

# ==================== 启动 ====================

if __name__ == "__main__":
    # 整合到 whitelist_bot.py 后，本文件保留为参考实现，**不再单独运行**。
    # 若要本地调试，需通过环境变量提供令牌，禁止将令牌硬编码进源码。
    TOKEN = os.getenv("DISCORD_BOT_TOKEN", "")
    if not TOKEN:
        raise SystemExit("请设置 DISCORD_BOT_TOKEN 环境变量后再运行。")
    client.run(TOKEN)