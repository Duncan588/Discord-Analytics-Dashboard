"""MCP smoke test: TOK=<token> python mcp_server/selftest.py https://host/mcp"""
import asyncio, json, os, sys
from mcp import ClientSession
try:
    from mcp.client.streamable_http import streamablehttp_client as c
except ImportError:
    from mcp.client.streamable_http import streamable_http_client as c
async def main(url, tok):
    async with c(url, headers={"Authorization": "Bearer " + tok}) as (r, w, _):
        async with ClientSession(r, w) as s:
            await s.initialize()
            tools = await s.list_tools()
            print("TOOLS", [t.name for t in tools.tools])
            for name, args in [("list_servers", {}), ("server_overview", {}), ("get_top_members", {"limit": 3}),
                               ("search_members", {"query": "柠檬", "limit": 2}), ("get_activity_tiers", {"tier": "faded", "limit": 2}),
                               ("search_messages", {"keyword": "hello", "limit": 2}), ("get_channel_stats", {"limit": 2}),
                               ("get_member_profile", {"user_id": os.getenv("UID", "1210794053361475654"), "recent": 1}),
                               ("get_member_messages", {"user_id": os.getenv("UID", "1210794053361475654"), "since": "2026-10-01", "limit": 2}),
                               ("list_threads", {"sort": "replies", "limit": 2}), ("get_hot_threads", {"limit": 3}),
                               ("search_threads", {"keyword": "拼团", "limit": 2}), ("get_thread", {"thread_id": "1558372038035636305"}),
                               ("get_thread_messages", {"thread_id": "1558372038035636305", "limit": 3}),
                               ("get_thread_context", {"thread_id": "1558372038035636305", "last_n": 5})]:
                res = await s.call_tool(name, args)
                txt = "".join(getattr(x, "text", "") for x in res.content)
                print(name, "ERR" if res.isError else "OK", txt[:300].replace("\n", " "))
asyncio.run(main(sys.argv[1], os.environ["TOK"]))
