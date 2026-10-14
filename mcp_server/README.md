# Read-only MCP server

Streamable HTTP MCP server over `data/servers/<gid>/{discord_data,summary}.db` and `data/portal.db`,
all opened with `mode=ro`. Row cap 200 per call, 8 s query timeout, message text clipped to 1500 chars.

- Auth: `Authorization: Bearer <token>`; tokens in prod `.env` as `MCP_API_TOKENS` (comma separated, never committed).
- Prod: separate venv `/root/v2/mcp-venv` (`pip install -r mcp_server/requirements.txt`), unit `mcp_server/v20-mcp.service`
  -> `/etc/systemd/system/v20-mcp.service`, listens on 127.0.0.1:5055; nginx `location /mcp` on
  discordanalytics.monster6324.me proxies to it (HTTPS via Cloudflare).
- Tools: list_servers, server_overview, search_members, get_member_profile, get_member_messages,
  search_messages, get_top_members, get_activity_tiers, get_channel_stats,
  list_threads, get_thread, get_thread_messages, get_thread_context, search_threads, get_hot_threads.
- Hot score: `replies*1 + unique_participants*3 + reactions*0.5` over threads created in range.
- Thread owner/tags/archived come from `thread_meta` (gateway THREAD_CREATE/UPDATE, since 2026-10-15);
  older threads fall back to the first-message author and snowflake creation time; tags empty.
- Smoke test: `TOK=... python mcp_server/selftest.py https://discordanalytics.monster6324.me/mcp`

AstrBot config:

```json
{
  "transport": "streamable_http",
  "url": "https://discordanalytics.monster6324.me/mcp",
  "headers": {"Authorization": "Bearer <token>"},
  "timeout": 30
}
```
