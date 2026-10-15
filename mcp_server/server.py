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



# ---------------------------------------------------------------- forum threads
DISCORD_EPOCH_MS = 1420070400000
SNIPPET = 200


def _snow_ts(sid):
    try:
        ms = (int(sid) >> 22) + DISCORD_EPOCH_MS
        return datetime.fromtimestamp(ms / 1000, timezone.utc).isoformat(timespec="seconds")
    except (TypeError, ValueError):
        return None


def _snow_from(date_s, end=False):
    d = _date(date_s, end=end)
    if not d:
        return None
    dt = datetime.fromisoformat(d.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return (int(dt.timestamp() * 1000) - DISCORD_EPOCH_MS) << 22


def _has_table(conn, name):
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone() is not None


def _thread_meta_map(main, ids):
    out = {}
    if not ids or not _has_table(main, "thread_meta"):
        return out
    for i in range(0, len(ids), 500):
        c = ids[i:i + 500]
        for r in main.execute(f"SELECT thread_id,owner_id,created_at,tags,archived,locked,deleted_at FROM thread_meta "
                              f"WHERE thread_id IN ({','.join('?'*len(c))})", c):
            d = dict(r)
            try:
                d["tags"] = json.loads(d["tags"]) if d["tags"] else []
            except ValueError:
                d["tags"] = []
            out[str(r[0])] = d
    return out


def _thread_stats_map(summ, ids):
    out = {}
    for i in range(0, len(ids), 500):
        c = ids[i:i + 500]
        for r in summ.execute(f"SELECT thread_id,msg_count,op_msg_id,op_author,first_ts,last_ts FROM thread_stats "
                              f"WHERE thread_id IN ({','.join('?'*len(c))})", c):
            out[str(r[0])] = r
    return out


def _participants(main, tid):
    return main.execute("SELECT COUNT(DISTINCT author_id) FROM messages WHERE thread_id=?", (tid,)).fetchone()[0]


def _thread_reactions(main, summ_path, tid):
    ids = [r[0] for r in main.execute("SELECT message_id FROM messages WHERE thread_id=?", (tid,))]
    if not ids:
        return 0
    tot = 0
    with _db(summ_path) as summ:
        for i in range(0, len(ids), 500):
            c = ids[i:i + 500]
            tot += summ.execute(f"SELECT COALESCE(SUM(c),0) FROM msg_react WHERE message_id IN ({','.join('?'*len(c))})", c).fetchone()[0]
    return tot


def _thread_card(tid, t, st, meta, names):
    owner = (meta or {}).get("owner_id") or (st["op_author"] if st else None)
    msgs = st["msg_count"] if st else 0
    return {"thread_id": tid, "title": t["name"] if t else None, "forum_id": t["category_id"] if t else None,
            "author_id": owner, "author": names.get(str(owner)) if owner else None,
            "created_at": (meta or {}).get("created_at") or _snow_ts(tid),
            "tags": (meta or {}).get("tags") or [], "messages": msgs, "replies": max(0, msgs - 1),
            "last_activity": (st["last_ts"] if st else None) or (t["last_active_at"] if t else None),
            "deleted": bool((meta or {}).get("deleted_at")),
            "url": f"https://discord.com/channels/{t['guild_id']}/{tid}" if t and t["guild_id"] else None}


def _reply_ctx(main, rows):
    ids = list({r["reply_to_msg_id"] for r in rows if r["reply_to_msg_id"]})
    out = {}
    for i in range(0, len(ids), 500):
        c = ids[i:i + 500]
        for r in main.execute(f"SELECT message_id,author_id,content FROM messages WHERE message_id IN ({','.join('?'*len(c))})", c):
            out[str(r[0])] = r
    names = _names(main, [r["author_id"] for r in out.values()])
    return {k: {"message_id": k, "author_id": v["author_id"], "author": names.get(str(v["author_id"])),
                "snippet": (v["content"] or "")[:SNIPPET]} for k, v in out.items()}


def _thread_msgs(main, rows):
    items = _msg_rows(main, rows)
    ctx = _reply_ctx(main, rows)
    for it in items:
        it.pop("channel", None); it.pop("channel_id", None)
        if it.get("reply_to"):
            it["reply_to_context"] = ctx.get(str(it["reply_to"])) or {"message_id": it["reply_to"], "missing": True}
    return items


@mcp.tool()
def list_threads(guild_id: str = "", forum_id: str = "", created_since: str = "", created_until: str = "",
                 tag: str = "", sort: str = "recency", limit: int = 20, offset: int = 0) -> dict:
    """List forum posts/threads. Filters: forum_id, created date range (ISO), tag name.
    sort: recency (last activity), created (newest first), replies, participants, activity
    (messages in the last 7 days). participants/activity are computed over the top 500 candidates by replies."""
    gid, main_p, summ_p = _gid(guild_id)
    lim, off = _lim(limit), max(0, int(offset or 0))
    if sort not in ("recency", "created", "replies", "participants", "activity"):
        raise ValueError("sort must be recency|created|replies|participants|activity")
    with _db(main_p) as main:
        where, args = ["1=1"], []
        if forum_id:
            where.append("t.category_id=?"); args.append(_uid(forum_id))
        lo, hi = _snow_from(created_since), _snow_from(created_until, end=True)
        if lo:
            where.append("CAST(t.thread_id AS INTEGER)>=?"); args.append(lo)
        if hi:
            where.append("CAST(t.thread_id AS INTEGER)<?"); args.append(hi)
        if tag:
            if not _has_table(main, "thread_meta"):
                raise ValueError("tag data not collected yet")
            where.append("t.thread_id IN (SELECT thread_id FROM thread_meta WHERE tags LIKE ?)")
            args.append('%"' + tag.replace('"', '') + '"%')
        rows = main.execute(f"SELECT t.* FROM threads t WHERE {' AND '.join(where)}", args).fetchall()
        tmap = {str(r["thread_id"]): r for r in rows}
        ids = list(tmap)
        with _db(summ_p) as summ:
            st = _thread_stats_map(summ, ids)
        extra = {}
        if sort == "created":
            ids.sort(key=lambda x: -int(x))
        elif sort == "recency":
            ids.sort(key=lambda x: ((st[x]["last_ts"] if x in st else None) or tmap[x]["last_active_at"] or ""), reverse=True)
        else:
            ids.sort(key=lambda x: -(st[x]["msg_count"] if x in st else 0))
            if sort in ("participants", "activity"):
                cand = ids[:500]
                if sort == "participants":
                    extra = {x: _participants(main, x) for x in cand}
                else:
                    since = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")
                    extra = {x: main.execute("SELECT COUNT(*) FROM messages WHERE thread_id=? AND timestamp>=?", (x, since)).fetchone()[0] for x in cand}
                ids = sorted(cand, key=lambda x: -extra[x])
        total = len(ids)
        page = ids[off:off + lim]
        meta = _thread_meta_map(main, page)
        names = _names(main, [(meta.get(x) or {}).get("owner_id") or (st[x]["op_author"] if x in st else None) for x in page])
        items = []
        for x in page:
            c = _thread_card(x, tmap[x], st.get(x), meta.get(x), names)
            if sort == "participants":
                c["participants"] = extra[x]
            elif sort == "activity":
                c["messages_last_7d"] = extra[x]
            items.append(c)
    return {"total": total, "items": items, "next_offset": off + lim if total > off + lim else None}


@mcp.tool()
def get_thread(thread_id: str, guild_id: str = "") -> dict:
    """One forum post/thread: title, author, tags, created time, full starter post, stats
    (messages, replies, participants, reactions, first/last activity) and top participants."""
    gid, main_p, summ_p = _gid(guild_id)
    tid = _uid(thread_id)
    with _db(main_p) as main:
        t = main.execute("SELECT * FROM threads WHERE thread_id=?", (tid,)).fetchone()
        with _db(summ_p) as summ:
            st = _thread_stats_map(summ, [tid]).get(tid)
        if not t and not st:
            raise ValueError("thread not found")
        meta = _thread_meta_map(main, [tid]).get(tid)
        starter_id = tid if main.execute("SELECT 1 FROM messages WHERE message_id=?", (tid,)).fetchone() else (st["op_msg_id"] if st else None)
        starter = main.execute(_MSG_SELECT + "WHERE m.message_id=?", (starter_id,)).fetchone() if starter_id else None
        top = main.execute("SELECT author_id,COUNT(*) n FROM messages WHERE thread_id=? GROUP BY author_id ORDER BY n DESC", (tid,)).fetchall()
        att = main.execute("SELECT url,filename FROM attachments WHERE message_id=?", (starter_id,)).fetchall() if starter_id else []
        owner = (meta or {}).get("owner_id") or (st["op_author"] if st else None)
        names = _names(main, [r[0] for r in top[:15]] + [owner])
        card = _thread_card(tid, t, st, meta, names)
        card.update({
            "participants": len(top), "reactions": _thread_reactions(main, summ_p, tid),
            "first_message": st["first_ts"] if st else None,
            "archived": bool((meta or {}).get("archived")), "locked": bool((meta or {}).get("locked")),
            "starter_post": None if not starter else {
                "message_id": starter["message_id"], "author_id": starter["author_id"],
                "author": names.get(str(starter["author_id"])) or _names(main, [starter["author_id"]]).get(str(starter["author_id"])),
                "timestamp": starter["timestamp"], "content": starter["content"] or "",
                "attachments": [{"url": a[0], "filename": a[1]} for a in att]},
            "starter_missing": starter is None,
            "top_participants": [{"user_id": r[0], "name": names.get(str(r[0])), "messages": r[1]} for r in top[:15]],
        })
    return card


@mcp.tool()
def get_thread_messages(thread_id: str, guild_id: str = "", limit: int = 50, before_message_id: str = "",
                        after_message_id: str = "", around_message_id: str = "") -> dict:
    """Messages of a thread in chronological order (oldest→newest), each with reply-to context.
    Default: latest `limit` messages. Page backwards with before_message_id, forwards with
    after_message_id, or center on around_message_id."""
    gid, main_p, _ = _gid(guild_id)
    tid = _uid(thread_id)
    lim = _lim(limit, 50)
    with _db(main_p) as main:
        def ts_of(mid):
            r = main.execute("SELECT timestamp FROM messages WHERE message_id=?", (_uid(mid),)).fetchone()
            if not r:
                raise ValueError("message not found")
            return r[0]
        base = _MSG_SELECT + "WHERE m.thread_id=? "
        if around_message_id:
            ts = ts_of(around_message_id); half = lim // 2
            older = main.execute(base + "AND m.timestamp<? ORDER BY m.timestamp DESC LIMIT ?", (tid, ts, half)).fetchall()
            newer = main.execute(base + "AND m.timestamp>=? ORDER BY m.timestamp ASC LIMIT ?", (tid, ts, lim - half)).fetchall()
            rows = list(reversed(older)) + list(newer)
        elif after_message_id:
            rows = main.execute(base + "AND m.timestamp>? ORDER BY m.timestamp ASC LIMIT ?", (tid, ts_of(after_message_id), lim)).fetchall()
        else:
            args = [tid]
            q = base
            if before_message_id:
                q += "AND m.timestamp<? "; args.append(ts_of(before_message_id))
            rows = list(reversed(main.execute(q + "ORDER BY m.timestamp DESC LIMIT ?", (*args, lim)).fetchall()))
        items = _thread_msgs(main, rows)
    return {"thread_id": tid, "items": items,
            "older_cursor": items[0]["message_id"] if items else None,
            "newer_cursor": items[-1]["message_id"] if items else None}


@mcp.tool()
def get_thread_context(thread_id: str, guild_id: str = "", last_n: int = 30) -> dict:
    """Compact digest for an AI about to reply in a thread: title/tags/author, full starter post,
    the last N messages (oldest→newest, with reply-to snippets) and active participants with brief
    profiles (messages in this thread, server-wide totals, activity tier, last seen)."""
    gid, main_p, summ_p = _gid(guild_id)
    tid = _uid(thread_id)
    info = get_thread(tid, gid)
    n = max(1, min(100, int(last_n or 30)))
    msgs = get_thread_messages(tid, gid, limit=n)["items"]
    if info.get("starter_post"):
        msgs = [m for m in msgs if m["message_id"] != info["starter_post"]["message_id"]]
    ids = [p["user_id"] for p in info["top_participants"][:10]]
    tmap, _ = _tier_map(gid, summ_p)
    prof = {}
    with _db(summ_p) as summ:
        if ids:
            for r in summ.execute(f"SELECT user_id,msg_count,first_ts,last_ts FROM user_stats WHERE user_id IN ({','.join('?'*len(ids))})", ids):
                prof[str(r[0])] = r
    parts = []
    for p in info["top_participants"][:10]:
        r = prof.get(str(p["user_id"]))
        tier = tmap.get(str(p["user_id"]))
        parts.append({"user_id": p["user_id"], "name": p["name"], "messages_in_thread": p["messages"],
                      "messages_total": r[1] if r else None, "member_since": r[2] if r else None,
                      "last_seen": r[3] if r else None, "activity_tier": TIER_LABELS.get(tier, tier)})
    for m in msgs:
        m["content"] = (m["content"] or "")[:600]
    return {"thread": {k: info[k] for k in ("thread_id", "title", "author", "author_id", "tags", "created_at",
                                             "messages", "replies", "participants", "reactions", "last_activity", "url")},
            "starter_post": info.get("starter_post"), "recent_messages": msgs, "participants": parts}


@mcp.tool()
def search_threads(keyword: str, guild_id: str = "", forum_id: str = "", in_starter: bool = True,
                   limit: int = 20, offset: int = 0) -> dict:
    """Find forum posts by keyword in the title (and, by default, the starter post text). Newest first."""
    gid, main_p, summ_p = _gid(guild_id)
    kw = (keyword or "").strip()
    if not kw:
        raise ValueError("keyword required")
    lim, off = _lim(limit), max(0, int(offset or 0))
    like = "%" + kw.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"
    with _db(main_p) as main:
        fcond, fargs = ("AND t.category_id=? ", [_uid(forum_id)]) if forum_id else ("", [])
        # title hits: whole history (threads table is small)
        hits = {str(r["thread_id"]): r for r in main.execute(
            f"SELECT t.*,NULL starter FROM threads t WHERE t.name LIKE ? ESCAPE '\\' {fcond}", (like, *fargs))}
        searched_back_to = None
        if in_starter:
            # starter posts: scan newest-first in chunks within a time budget
            all_ids = [str(r[0]) for r in main.execute(f"SELECT t.thread_id FROM threads t WHERE 1=1 {fcond}", fargs)]
            all_ids.sort(key=lambda x: -int(x))
            t0 = time.monotonic()
            for i in range(0, len(all_ids), 2000):
                if time.monotonic() - t0 > QUERY_TIMEOUT_S * 0.6 or len(hits) >= off + lim + 1 and i > 0:
                    searched_back_to = _snow_ts(all_ids[i - 1])
                    break
                c = all_ids[i:i + 2000]
                for r in main.execute(f"SELECT t.*,m.content starter FROM messages m JOIN threads t ON t.thread_id=m.thread_id "
                                      f"WHERE m.message_id IN ({','.join('?'*len(c))}) AND m.content LIKE ? ESCAPE '\\'", (*c, like)):
                    hits[str(r["thread_id"])] = r
        rows = sorted(hits.values(), key=lambda r: -int(r["thread_id"]))[off:off + lim + 1]
        page = rows[:lim]
        ids = [str(r["thread_id"]) for r in page]
        with _db(summ_p) as summ:
            st = _thread_stats_map(summ, ids)
        meta = _thread_meta_map(main, ids)
        names = _names(main, [(meta.get(x) or {}).get("owner_id") or (st[x]["op_author"] if x in st else None) for x in ids])
        items = []
        for r in page:
            x = str(r["thread_id"])
            c = _thread_card(x, r, st.get(x), meta.get(x), names)
            c["starter_snippet"] = (r["starter"] or "")[:SNIPPET]
            items.append(c)
    return {"items": items, "next_offset": off + lim if len(rows) > lim else None,
            "starter_search_partial_back_to": searched_back_to}


HOT_W_REPLIES, HOT_W_PARTICIPANTS, HOT_W_REACTIONS = 1.0, 3.0, 0.5


@mcp.tool()
def get_hot_threads(guild_id: str = "", since: str = "", until: str = "", forum_id: str = "", limit: int = 10) -> dict:
    """Hottest forum posts CREATED within [since, until] (ISO dates; default last 7 days).
    score = replies*1 + unique_participants*3 + reactions*0.5, where replies = messages-1,
    unique_participants = distinct authors, reactions = all reactions on all messages of the thread.
    Scored over the top 300 created-in-range threads by replies."""
    gid, main_p, summ_p = _gid(guild_id)
    if not since:
        since = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")
    lo, hi = _snow_from(since), _snow_from(until, end=True) if until else None
    lim = max(1, min(50, int(limit or 10)))
    with _db(main_p) as main:
        where, args = ["CAST(t.thread_id AS INTEGER)>=?"], [lo]
        if hi:
            where.append("CAST(t.thread_id AS INTEGER)<?"); args.append(hi)
        if forum_id:
            where.append("t.category_id=?"); args.append(_uid(forum_id))
        rows = main.execute(f"SELECT t.* FROM threads t WHERE {' AND '.join(where)}", args).fetchall()
        tmap = {str(r["thread_id"]): r for r in rows}
        with _db(summ_p) as summ:
            st = _thread_stats_map(summ, list(tmap))
        cand = sorted(tmap, key=lambda x: -(st[x]["msg_count"] if x in st else 0))[:300]
        scored = []
        for x in cand:
            replies = max(0, (st[x]["msg_count"] if x in st else 0) - 1)
            parts = _participants(main, x)
            reacts = _thread_reactions(main, summ_p, x)
            score = replies * HOT_W_REPLIES + parts * HOT_W_PARTICIPANTS + reacts * HOT_W_REACTIONS
            scored.append((score, x, replies, parts, reacts))
        scored.sort(reverse=True)
        top = scored[:lim]
        meta = _thread_meta_map(main, [x for _, x, *_ in top])
        names = _names(main, [(meta.get(x) or {}).get("owner_id") or (st[x]["op_author"] if x in st else None) for _, x, *_ in top])
        items = []
        for score, x, replies, parts, reacts in top:
            c = _thread_card(x, tmap[x], st.get(x), meta.get(x), names)
            c.update({"score": round(score, 1), "participants": parts, "reactions": reacts})
            items.append(c)
    return {"since": since, "until": until or None, "threads_created_in_range": len(tmap),
            "formula": "score = replies*1 + unique_participants*3 + reactions*0.5", "items": items}

@mcp.tool()
def get_sentiment(guild_id: str = "", user_id: str = "", since: str = "", until: str = "", granularity: str = "day") -> dict:
    """Message sentiment over time for a guild, or one member (incl. merged alt accounts).
    since/until: ISO dates (default: last 90 days). granularity: day | week | month.
    Each bucket: pos/neu/neg counts, their % share, and mean = average(p_pos - p_neg) in [-1, 1].
    Model cardiffnlp/twitter-xlm-roberta-base-sentiment; bot/empty/link-only messages are not scored.
    Returns ready=false until the historical import has run."""
    from dixo import sentiment as dsent
    gid, main_p, _ = _gid(guild_id)
    gran = granularity if granularity in ("day", "week", "month") else "day"
    if not since:
        since = (datetime.now(timezone.utc) - timedelta(days=90)).strftime("%Y-%m-%d")
    for d in (since, until):
        if d and not re.match(r"^\d{4}-\d{2}-\d{2}$", d):
            raise ValueError("dates must be YYYY-MM-DD")
    sp = dsent.path_for(main_p)
    if not os.path.exists(sp):
        return {"ready": False, "items": []}
    ids = None
    if user_id:
        uid = _uid(user_id)
        with _db(main_p) as main:
            ids = _merged_ids(main, uid)
    with _db(sp) as c:
        st = dsent.status(c)
        items = dsent.series(c, ids, since, until or None, gran) if st["ready"] else []
    if len(items) > MAX_ROWS:
        items = items[-MAX_ROWS:]
    tot = [sum(x[k] for x in items) for k in ("pos", "neu", "neg")]
    n = sum(tot)
    overall = {"n": n, "pos_pct": round(100 * tot[0] / n, 1) if n else None, "neu_pct": round(100 * tot[1] / n, 1) if n else None,
               "neg_pct": round(100 * tot[2] / n, 1) if n else None,
               "mean": round(sum(x["mean"] * x["n"] for x in items) / n, 4) if n else None}
    return {"ready": st["ready"], "guild_id": gid, "user_id": user_id or None, "since": since, "until": until or None,
            "granularity": gran, "model": st.get("model"), "scored_through": st.get("last_day"),
            "overall": overall, "items": [{k: v for k, v in x.items()} for x in items]}

def _ins(guild_id):
    from dixo import insights as dins
    gid, main_p, _ = _gid(guild_id)
    p = dins.path_for(main_p)
    if not os.path.exists(p):
        raise ValueError("insights not generated yet")
    return gid, main_p, p, dins


@mcp.tool()
def get_topics(guild_id: str = "", months: int = 24) -> dict:
    """Topic map: LLM-labelled forum topics (threads clustered by embedding) with thread/message counts,
    trend (share in last 3 months vs previous 12, relative) and monthly message counts for the last `months` months."""
    gid, _, p, dins = _ins(guild_id)
    with _db(p) as c:
        d = dins.topics(c, max(1, min(120, int(months or 24))))
    return {"guild_id": gid, **d}


@mcp.tool()
def get_circles(guild_id: str = "", user_id: str = "") -> dict:
    """Relationship circles (Louvain communities on the reply+mention graph): name, description, size, core members,
    favourite threads, typical words. With user_id: only that member's circle."""
    gid, main_p, p, dins = _ins(guild_id)
    with _db(p) as c:
        d = dins.network(c)
        cs = d["circles"]
        if user_id:
            r = c.execute("SELECT circle FROM member_circle WHERE user_id=?", (_uid(user_id),)).fetchone()
            cs = [x for x in cs if r and x["id"] == r[0]]
    return {"guild_id": gid, "graph_size": {"nodes": len((d["graph"] or {}).get("nodes", []))}, "circles": cs[:MAX_ROWS]}


@mcp.tool()
def search_memes(query: str = "", kind: str = "", guild_id: str = "", limit: int = 30) -> dict:
    """Community meme glossary (群梗词典): term, meaning, origin (first use: who/when/which thread), usage, spread
    (total uses, distinct users, peak month, monthly counts). kind: term | emoji | phrase. Empty query = most widespread."""
    gid, _, p, dins = _ins(guild_id)
    with _db(p) as c:
        items = dins.memes(c, (query or "")[:40], kind or "", _lim(limit, 30))
    return {"guild_id": gid, "items": items}


@mcp.tool()
def get_server_report(month: str = "", guild_id: str = "") -> dict:
    """Server monthly report (YYYY-MM; default latest): messages, active/new members, hot threads, top posters,
    rising terms, sentiment, most-reacted message, and an AI-written summary."""
    gid, _, p, dins = _ins(guild_id)
    with _db(p) as c:
        if month:
            return {"guild_id": gid, "report": dins.months(c, month)}
        ms = dins.months(c)
        return {"guild_id": gid, "report": ms[0] if ms else None, "available": [m["ym"] for m in ms]}


@mcp.tool()
def get_member_report(user_id: str, year: int = 0, guild_id: str = "") -> dict:
    """Per-member annual summary: messages & rank, typical phrases, best buddies, busiest / happiest / gloomiest day,
    top threads, sentiment, most-reacted message; plus the member's circle. year=0 -> all years."""
    gid, main_p, p, dins = _ins(guild_id)
    uid = _uid(user_id)
    with _db(main_p) as main:
        ids = _merged_ids(main, uid)
    with _db(p) as c:
        d = dins.member(c, ids, int(year) if year else None)
    d.pop("dossier", None)
    return {"guild_id": gid, "user_id": uid, **d}


@mcp.tool()
def get_member_dossier(user_id: str, guild_id: str = "") -> dict:
    """AI-written member dossier (Markdown): who they are, topics, speaking style, catchphrases, partners,
    notable moments, persona summary. Available for active members; generated from history with a free model."""
    gid, main_p, p, dins = _ins(guild_id)
    uid = _uid(user_id)
    with _db(main_p) as main:
        ids = _merged_ids(main, uid)
    with _db(p) as c:
        d = dins.member(c, ids)
    return {"guild_id": gid, "user_id": uid, "dossier": d["dossier"], "circle": d["circle"]}


@mcp.tool()
def get_community_overview(guild_id: str = "") -> dict:
    """AI-written community overview (history, circles, culture & memes, mood) plus build metadata."""
    gid, _, p, dins = _ins(guild_id)
    with _db(p) as c:
        return {"guild_id": gid, "overview": dins.doc(c, "overview"), "meta": dins.status(c)}

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
