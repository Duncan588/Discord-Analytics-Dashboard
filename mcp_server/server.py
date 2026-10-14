"""Read-only MCP server over the Discord-Analytics-Dashboard databases.

Transport: Streamable HTTP (stateless, JSON responses) at /mcp, bearer-token auth.
All SQLite files are opened with mode=ro URIs; every query has a row cap and a
wall-clock timeout (sqlite progress handler).

Run:  python -m mcp_server.server   (env: MCP_API_TOKENS, MCP_HOST, MCP_PORT, V20_ROOT)
"""
import contextlib
import hmac
import json
import os
import re
import sqlite3
import time
from datetime import datetime, timedelta, timezone

from mcp.server.fastmcp import FastMCP

ROOT = os.path.abspath(os.getenv("V20_ROOT") or os.path.join(os.path.dirname(__file__), ".."))
DATA = os.path.join(ROOT, "data")
PORTAL_DB = os.getenv("PORTAL_DB") or os.path.join(DATA, "portal.db")
MAX_ROWS = 200
QUERY_TIMEOUT_S = float(os.getenv("MCP_QUERY_TIMEOUT", "8"))
CONTENT_MAX = 1500
_ID = re.compile(r"^\d{5,25}$")


def _load_env():
    path = os.path.join(ROOT, ".env")
    if not os.path.exists(path):
        return
    for raw in open(path, encoding="utf-8"):
        line = raw.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip("'\""))


_load_env()


# ---------------------------------------------------------------- sqlite helpers
def _ro(path):
    if not os.path.exists(path):
        raise ValueError("database not found")
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=1")
    deadline = time.monotonic() + QUERY_TIMEOUT_S
    conn.set_progress_handler(lambda: 1 if time.monotonic() > deadline else 0, 10000)
    return conn


@contextlib.contextmanager
def _db(path):
    conn = _ro(path)
    try:
        yield conn
    except sqlite3.OperationalError as e:
        if "interrupted" in str(e):
            raise ValueError(f"query exceeded {QUERY_TIMEOUT_S:.0f}s timeout; narrow the filters") from None
        raise
    finally:
        conn.close()


def _gid(guild_id):
    g = str(guild_id or "").strip()
    if not g:
        g = (os.getenv("MCP_DEFAULT_GUILD") or "").strip()
        if not g:
            servers = list_servers()
            if not servers:
                raise ValueError("no servers")
            g = max(servers, key=lambda s: s.get("member_count") or 0)["guild_id"]
    if not _ID.match(g):
        raise ValueError("invalid guild_id")
    d = os.path.join(DATA, "servers", g)
    if not os.path.exists(os.path.join(d, "discord_data.db")):
        raise ValueError("unknown guild_id")
    return g, os.path.join(d, "discord_data.db"), os.path.join(d, "summary.db")


def _uid(user_id):
    u = str(user_id or "").strip().strip("<@!>")
    if not _ID.match(u):
        raise ValueError("invalid user_id (numeric Discord id expected; use search_members first)")
    return u


def _lim(limit, default=20):
    try:
        n = int(limit if limit is not None else default)
    except (TypeError, ValueError):
        n = default
    return max(1, min(MAX_ROWS, n))


def _clip(text):
    text = text or ""
    return text if len(text) <= CONTENT_MAX else text[:CONTENT_MAX] + "…"


def _merged_ids(main, uid):
    try:
        kids = [r[0] for r in main.execute("SELECT target_id FROM user_merges WHERE parent_id=?", (uid,))]
    except sqlite3.Error:
        kids = []
    return [uid] + [str(k) for k in kids if str(k) != uid]


def _names(main, ids):
    ids = list({str(i) for i in ids if i})
    out = {}
    for i in range(0, len(ids), 500):
        chunk = ids[i:i + 500]
        q = ",".join("?" * len(chunk))
        for r in main.execute(f"SELECT user_id,username,nickname FROM users WHERE user_id IN ({q})", chunk):
            out[str(r[0])] = r["nickname"] or r["username"]
    return out


def _msg_rows(main, rows):
    names = _names(main, [r["author_id"] for r in rows])
    return [{"message_id": r["message_id"], "timestamp": r["timestamp"], "author_id": r["author_id"],
             "author": names.get(str(r["author_id"])), "channel_id": r["thread_id"],
             "channel": r["thread_name"], "content": _clip(r["content"]),
             "reply_to": r["reply_to_msg_id"]} for r in rows]


_MSG_SELECT = ("SELECT m.rowid rid,m.message_id,m.thread_id,t.name thread_name,m.author_id,m.content,"
               "m.timestamp,m.reply_to_msg_id FROM messages m LEFT JOIN threads t ON t.thread_id=m.thread_id ")


def _date(s, end=False):
    if not s:
        return None
    s = str(s).strip()
    if not re.match(r"^\d{4}-\d{2}(-\d{2})?([T ][\d:.+Z-]*)?$", s):
        raise ValueError("dates must be ISO format, e.g. 2026-10-01")
    if len(s) == 7:
        s += "-01"
    if end and len(s) == 10:
        return (datetime.fromisoformat(s) + timedelta(days=1)).strftime("%Y-%m-%d")
    return s


# ---------------------------------------------------------------- tiers
TIER_LABELS = {"always_active": "一贯活跃", "returned": "消失后回归", "faded": "活跃后消失",
               "rare": "几乎不活跃", "new": "近两月新活跃"}


def _tiers(summ_path):
    try:
        from dixo import dashboard_data as dd
    except Exception:
        return None
    with _db(summ_path) as summ:
        return dd.member_activity_tags(summ, sample_per_tag=0)


_TIER_CACHE = {}


def _tier_map(gid, summ_path):
    try:
        mt = os.path.getmtime(summ_path)
    except OSError:
        return {}, None
    hit = _TIER_CACHE.get(gid)
    if hit and hit[0] == mt and time.time() - hit[1] < 600:
        return hit[2], hit[3]
    res = _tiers(summ_path) or {}
    m = {}
    for tag, recs in (res.get("by_tag") or {}).items():
        for r in recs:
            m[str(r["user_id"])] = tag
    _TIER_CACHE[gid] = (mt, time.time(), m, res)
    return m, res


# ---------------------------------------------------------------- MCP
mcp = FastMCP(
    "discord-analytics",
    instructions=("Read-only access to the Discord Analytics Dashboard (抽象派 archive). "
                  "guild_id is optional and defaults to the main server (抽象派); call list_servers for others. "
                  "Use search_members to resolve names to user ids. Timestamps are UTC."),
    stateless_http=True,
    json_response=True,
    streamable_http_path="/mcp",
)
try:  # behind nginx with bearer auth; Host header is the public domain
    from mcp.server.transport_security import TransportSecuritySettings
    mcp.settings.transport_security = TransportSecuritySettings(enable_dns_rebinding_protection=False)
except Exception:
    pass


@mcp.tool()
def list_servers() -> list[dict]:
    """List Discord servers (guilds) available in the archive."""
    out = []
    try:
        with _db(PORTAL_DB) as p:
            for r in p.execute("SELECT server_id,name,member_count,updated_at FROM servers ORDER BY created_at"):
                if os.path.exists(os.path.join(DATA, "servers", str(r[0]), "discord_data.db")):
                    out.append({"guild_id": str(r[0]), "name": r[1], "member_count": r[2], "updated_at": r[3]})
    except Exception:
        pass
    if not out:
        base = os.path.join(DATA, "servers")
        for g in sorted(os.listdir(base)) if os.path.isdir(base) else []:
            if os.path.exists(os.path.join(base, g, "discord_data.db")):
                out.append({"guild_id": g, "name": None})
    return out


@mcp.tool()
def server_overview(guild_id: str = "") -> dict:
    """Totals for a server: messages, members, channels, date range, last 30 days activity, data freshness."""
    gid, main_p, summ_p = _gid(guild_id)
    with _db(main_p) as main, _db(summ_p) as summ:
        last = main.execute("SELECT MAX(timestamp) FROM messages WHERE rowid>(SELECT MAX(rowid)-2000 FROM messages)").fetchone()[0]
        us = summ.execute("SELECT COUNT(*),COALESCE(SUM(msg_count),0),MIN(first_ts),MAX(last_ts),COALESCE(SUM(react_recv),0) FROM user_stats WHERE msg_count>0").fetchone()
        since = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")
        d30 = summ.execute("SELECT COALESCE(SUM(c),0),COUNT(DISTINCT user_id) FROM user_day WHERE day>=?", (since,)).fetchone()
        return {"guild_id": gid, "members_with_messages": us[0], "total_messages": us[1],
                "total_reactions": us[4], "first_message": us[2], "last_message": last or us[3],
                "channels": main.execute("SELECT COUNT(*) FROM threads").fetchone()[0],
                "known_users": main.execute("SELECT COUNT(*) FROM users").fetchone()[0],
                "last_30_days": {"messages": d30[0], "active_members": d30[1]}}


@mcp.tool()
def search_members(query: str, guild_id: str = "", limit: int = 20) -> list[dict]:
    """Find members by username / nickname (substring) or exact user id. Sorted by message count."""
    gid, main_p, summ_p = _gid(guild_id)
    q = (query or "").strip()
    if not q:
        raise ValueError("query required")
    with _db(main_p) as main:
        if _ID.match(q.strip("<@!>")):
            rows = main.execute("SELECT user_id,username,nickname,is_bot FROM users WHERE user_id=?", (q.strip("<@!>"),)).fetchall()
        else:
            like = "%" + q.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"
            rows = main.execute("SELECT user_id,username,nickname,is_bot FROM users WHERE username LIKE ? ESCAPE '\\' "
                                "OR nickname LIKE ? ESCAPE '\\' LIMIT 500", (like, like)).fetchall()
    ids = [str(r[0]) for r in rows]
    stats = {}
    if ids:
        with _db(summ_p) as summ:
            for i in range(0, len(ids), 500):
                c = ids[i:i + 500]
                for s in summ.execute(f"SELECT user_id,msg_count,last_ts FROM user_stats WHERE user_id IN ({','.join('?'*len(c))})", c):
                    stats[str(s[0])] = (s[1], s[2])
    out = [{"user_id": str(r[0]), "username": r[1], "nickname": r[2], "is_bot": bool(r[3]),
            "msg_count": stats.get(str(r[0]), (0, None))[0], "last_message": stats.get(str(r[0]), (0, None))[1]} for r in rows]
    out.sort(key=lambda x: -(x["msg_count"] or 0))
    return out[:_lim(limit)]


@mcp.tool()
def get_member_profile(user_id: str, guild_id: str = "", recent: int = 10) -> dict:
    """Member stats: message/reaction counts, first/last message, activity tier, active months,
    busiest hours (UTC), top channels and most recent messages."""
    gid, main_p, summ_p = _gid(guild_id)
    uid = _uid(user_id)
    with _db(main_p) as main, _db(summ_p) as summ:
        u = main.execute("SELECT user_id,username,nickname,avatar_url,is_bot FROM users WHERE user_id=?", (uid,)).fetchone()
        if not u:
            raise ValueError("user not found")
        ids = _merged_ids(main, uid)
        ph = ",".join("?" * len(ids))
        st = summ.execute(f"SELECT COALESCE(SUM(msg_count),0),COALESCE(SUM(react_recv),0),COALESCE(SUM(react_given),0),MIN(first_ts),MAX(last_ts) FROM user_stats WHERE user_id IN ({ph})", ids).fetchone()
        rank = summ.execute("SELECT COUNT(*)+1 FROM user_stats WHERE msg_count>?", (st[0],)).fetchone()[0]
        months = summ.execute(f"SELECT substr(day,1,7) m,SUM(c) FROM user_day WHERE user_id IN ({ph}) GROUP BY m ORDER BY m", ids).fetchall()
        hours = summ.execute(f"SELECT hour,SUM(c) n FROM user_hour WHERE user_id IN ({ph}) GROUP BY hour ORDER BY n DESC LIMIT 3", ids).fetchall()
        chans = main.execute(f"SELECT m.thread_id,t.name,COUNT(*) n FROM messages m LEFT JOIN threads t ON t.thread_id=m.thread_id "
                             f"WHERE m.author_id IN ({ph}) GROUP BY m.thread_id ORDER BY n DESC LIMIT 5", ids).fetchall()
        rec = main.execute(_MSG_SELECT + f"WHERE m.author_id IN ({ph}) ORDER BY m.timestamp DESC LIMIT ?", (*ids, min(50, max(0, int(recent or 0))))).fetchall()
        recent_msgs = _msg_rows(main, rec)
    tmap, _ = _tier_map(gid, summ_p)
    tier = tmap.get(uid)
    return {"user_id": uid, "username": u["username"], "nickname": u["nickname"], "is_bot": bool(u["is_bot"]),
            "merged_ids": ids[1:], "msg_count": st[0], "rank_by_messages": rank, "reactions_received": st[1],
            "reactions_given": st[2], "first_message": st[3], "last_message": st[4],
            "activity_tier": tier, "activity_tier_label": TIER_LABELS.get(tier),
            "monthly_messages": [{"month": m[0], "messages": m[1]} for m in months][-24:],
            "top_hours_utc": [{"hour": h[0], "messages": h[1]} for h in hours],
            "top_channels": [{"channel_id": c[0], "channel": c[1], "messages": c[2]} for c in chans],
            "recent_messages": recent_msgs}


@mcp.tool()
def get_member_messages(user_id: str, guild_id: str = "", since: str = "", until: str = "",
                        limit: int = 50, offset: int = 0, oldest_first: bool = False) -> dict:
    """A member's messages, paged (limit<=200), optional ISO date range [since, until]."""
    gid, main_p, _ = _gid(guild_id)
    uid = _uid(user_id)
    lim, off = _lim(limit, 50), max(0, int(offset or 0))
    with _db(main_p) as main:
        ids = _merged_ids(main, uid)
        where, args = [f"m.author_id IN ({','.join('?'*len(ids))})"], list(ids)
        if since:
            where.append("m.timestamp>=?"); args.append(_date(since))
        if until:
            where.append("m.timestamp<?"); args.append(_date(until, end=True))
        order = "ASC" if oldest_first else "DESC"
        rows = main.execute(_MSG_SELECT + f"WHERE {' AND '.join(where)} ORDER BY m.timestamp {order} LIMIT ? OFFSET ?", (*args, lim + 1, off)).fetchall()
        items = _msg_rows(main, rows[:lim])
    return {"items": items, "next_offset": off + lim if len(rows) > lim else None}


@mcp.tool()
def search_messages(keyword: str = "", guild_id: str = "", channel_id: str = "", author_id: str = "",
                    since: str = "", until: str = "", limit: int = 30, offset: int = 0) -> dict:
    """Search messages by keyword (substring), channel/thread id, author id and ISO date range.
    At least one filter is required. Newest first."""
    gid, main_p, _ = _gid(guild_id)
    lim, off = _lim(limit, 30), max(0, int(offset or 0))
    kw = (keyword or "").strip()
    if not (kw or channel_id or author_id or since):
        raise ValueError("give at least one of keyword, channel_id, author_id, since")
    with _db(main_p) as main:
        where, args = [], []
        if kw:
            where.append("m.content LIKE ? ESCAPE '\\'")
            args.append("%" + kw.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%")
        if channel_id:
            where.append("m.thread_id=?"); args.append(_uid(channel_id))
        if author_id:
            ids = _merged_ids(main, _uid(author_id))
            where.append(f"m.author_id IN ({','.join('?'*len(ids))})"); args += ids
        if since:
            where.append("m.timestamp>=?"); args.append(_date(since))
        if until:
            where.append("m.timestamp<?"); args.append(_date(until, end=True))
        rows = main.execute(_MSG_SELECT + f"WHERE {' AND '.join(where)} ORDER BY m.timestamp DESC LIMIT ? OFFSET ?", (*args, lim + 1, off)).fetchall()
        items = _msg_rows(main, rows[:lim])
    return {"items": items, "next_offset": off + lim if len(rows) > lim else None}


@mcp.tool()
def get_top_members(guild_id: str = "", since: str = "", until: str = "", limit: int = 20,
                    offset: int = 0, include_bots: bool = False) -> list[dict]:
    """Leaderboard by message count. Optional ISO date range (uses daily totals); default all-time."""
    gid, main_p, summ_p = _gid(guild_id)
    lim, off = _lim(limit), max(0, int(offset or 0))
    with _db(summ_p) as summ, _db(main_p) as main:
        bots = set() if include_bots else {str(r[0]) for r in main.execute("SELECT user_id FROM users WHERE is_bot=1")}
        if since or until:
            where, args = [], []
            if since:
                where.append("day>=?"); args.append(_date(since)[:10])
            if until:
                where.append("day<?"); args.append(_date(until, end=True)[:10])
            rows = summ.execute(f"SELECT user_id,SUM(c) n FROM user_day WHERE {' AND '.join(where)} GROUP BY user_id ORDER BY n DESC LIMIT ?", (*args, lim + off + len(bots))).fetchall()
        else:
            rows = summ.execute("SELECT user_id,msg_count n FROM user_stats ORDER BY msg_count DESC LIMIT ?", (lim + off + len(bots),)).fetchall()
        rows = [r for r in rows if str(r[0]) not in bots][off:off + lim]
        names = _names(main, [r[0] for r in rows])
    return [{"rank": off + i + 1, "user_id": str(r[0]), "name": names.get(str(r[0])), "messages": r[1]} for i, r in enumerate(rows)]


@mcp.tool()
def get_activity_tiers(guild_id: str = "", tier: str = "", limit: int = 30, offset: int = 0) -> dict:
    """Member activity tiers (成员活跃分层): counts per tier, or the members of one tier
    (always_active / returned / faded / rare / new), paged."""
    gid, main_p, summ_p = _gid(guild_id)
    tmap, res = _tier_map(gid, summ_p)
    if not res:
        raise ValueError("tiers unavailable")
    counts = res.get("counts") or {}
    out = {"as_of": res.get("as_of"), "labels": TIER_LABELS, "counts": counts}
    if tier:
        if tier not in TIER_LABELS:
            raise ValueError("tier must be one of " + ", ".join(TIER_LABELS))
        lim, off = _lim(limit, 30), max(0, int(offset or 0))
        users = (res.get("by_tag") or {}).get(tier) or []
        page = [dict(r) for r in users[off:off + lim]]
        with _db(main_p) as main:
            names = _names(main, [r["user_id"] for r in page])
        for r in page:
            r["name"] = names.get(str(r["user_id"]))
        out["tier"] = tier
        out["total"] = len(users)
        out["members"] = page
        out["next_offset"] = off + lim if len(users) > off + lim else None
    return out


@mcp.tool()
def get_channel_stats(guild_id: str = "", channel_id: str = "", limit: int = 20) -> list[dict] | dict:
    """Channel/thread statistics. With channel_id: one channel's totals and top posters;
    otherwise the most active channels/threads."""
    gid, main_p, summ_p = _gid(guild_id)
    with _db(main_p) as main, _db(summ_p) as summ:
        if channel_id:
            cid = _uid(channel_id)
            t = main.execute("SELECT thread_id,name,category_id,last_active_at FROM threads WHERE thread_id=?", (cid,)).fetchone()
            s = summ.execute("SELECT msg_count,op_author,first_ts,last_ts FROM thread_stats WHERE thread_id=?", (cid,)).fetchone()
            if not t and not s:
                raise ValueError("channel not found")
            top = main.execute("SELECT author_id,COUNT(*) n FROM messages WHERE thread_id=? GROUP BY author_id ORDER BY n DESC LIMIT 10", (cid,)).fetchall()
            names = _names(main, [r[0] for r in top] + ([s[1]] if s else []))
            return {"channel_id": cid, "name": t["name"] if t else None, "parent_id": t["category_id"] if t else None,
                    "messages": s[0] if s else None, "created_by": names.get(str(s[1])) if s else None,
                    "creator_id": s[1] if s else None, "first_message": s[2] if s else None,
                    "last_message": s[3] if s else None,
                    "top_posters": [{"user_id": r[0], "name": names.get(str(r[0])), "messages": r[1]} for r in top]}
        rows = summ.execute("SELECT thread_id,msg_count,last_ts FROM thread_stats ORDER BY msg_count DESC LIMIT ?", (_lim(limit),)).fetchall()
        ids = [r[0] for r in rows]
        nm = {}
        if ids:
            nm = {r[0]: r[1] for r in main.execute(f"SELECT thread_id,name FROM threads WHERE thread_id IN ({','.join('?'*len(ids))})", ids)}
        return [{"channel_id": r[0], "name": nm.get(r[0]), "messages": r[1], "last_message": r[2]} for r in rows]


# ---------------------------------------------------------------- auth + app
def _tokens():
    return [t.strip() for t in os.getenv("MCP_API_TOKENS", "").split(",") if t.strip()]


class BearerAuth:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http":
            hdr = dict(scope.get("headers") or [])
            auth = hdr.get(b"authorization", b"").decode("latin-1")
            sup = auth[7:].strip() if auth.lower().startswith("bearer ") else ""
            valid = _tokens()
            if not valid or not sup or not any(hmac.compare_digest(sup, t) for t in valid):
                body = json.dumps({"error": "unauthorized"}).encode()
                await send({"type": "http.response.start", "status": 401,
                            "headers": [(b"content-type", b"application/json"), (b"www-authenticate", b"Bearer")]})
                await send({"type": "http.response.body", "body": body})
                return
        await self.app(scope, receive, send)


def build_app():
    return BearerAuth(mcp.streamable_http_app())


def main():
    import uvicorn
    uvicorn.run(build_app(), host=os.getenv("MCP_HOST", "127.0.0.1"), port=int(os.getenv("MCP_PORT", "5055")),
                log_level="info", proxy_headers=True)


if __name__ == "__main__":
    main()
