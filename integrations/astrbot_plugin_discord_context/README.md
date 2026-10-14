# astrbot_plugin_discord_context

AstrBot plugin: when the bot is triggered on Discord (forum post/thread or any channel), it appends a compact
context block to the LLM system prompt:

- forum post/thread: `get_thread_context` from the Discord Analytics MCP server (title, tags, starter post,
  last N replies with reply-to snippets, active participants)
- other channels: last N messages via the bot's own Discord client
- asking user: `get_member_profile` (message count/rank, first seen, activity tier, favourite threads)
- MCP down → falls back to Discord API (thread starter + history)
- optional proactive mode (default off): reply to new forum posts with probability + global cooldown

Budget: `max_chars` (default 3500), per-message clip `msg_max_chars`, starter clip `starter_max_chars`.
Token: set `mcp_token` in the plugin config (WebUI) — never commit it. Uses a custom User-Agent because
Cloudflare rejects `Python-urllib` (error 1010).

Install: copy this folder to `AstrBot/data/plugins/` and restart/reload. Test: @bot in a forum post; logs show
`[discord_context] injected N chars via mcp`.

## Typing keepalive (ported from astrbot_plugin_stoprushingiamtyping)

From `on_llm_request` until `after_message_sent`, the plugin calls py-cord `channel.trigger_typing()` every
`typing_keepalive_seconds` (default 8, min 3), capped at `typing_max_window_seconds` (default 120, min 10).
Toggle with `typing_enable`. No `discord.py` dependency — the original plugin's `requirements.txt` pulled in
`discord.py`, which overwrote AstrBot's bundled py-cord and broke the Discord adapter.
