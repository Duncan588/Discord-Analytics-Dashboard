"""Build homepage / profile / API payloads from summary.db (+ a few indexed lookups on the main DB).

Every function takes open connections: `main` (discord_data.db) and `summ` (summary.db).
No full-table scans on messages/reactions happen here.
"""
import collections
from datetime import datetime, timedelta

from . import textutil
from .summary import LOCAL_TZ

UNKNOWN_USER = {"username": "Unknown", "nickname": "", "avatar_url": ""}


def _users_by_id(main, ids):
    ids = [str(i) for i in ids if i]
    if not ids:
        return {}
    out = {}
    for i in range(0, len(ids), 500):
        chunk = ids[i:i + 500]
        ph = ",".join("?" for _ in chunk)
        for r in main.execute(f"SELECT * FROM users WHERE user_id IN ({ph})", chunk):
            out[str(r["user_id"])] = dict(r)
    return out


def _top_emojis(summ, table, ids, per=3):
    ids = [str(i) for i in ids]
    out = collections.defaultdict(list)
    if not ids:
        return out
    ph = ",".join("?" for _ in ids)
    for r in summ.execute(f"SELECT user_id,emoji_name,emoji_url,c FROM {table} WHERE user_id IN ({ph}) ORDER BY user_id,c DESC", ids):
        if len(out[r["user_id"]]) < per:
            out[r["user_id"]].append({"emoji_name": r["emoji_name"], "emoji_url": r["emoji_url"], "c": r["c"]})
    return out


def leaderboard(main, summ, limit=50, offset=0):
    rows = summ.execute("SELECT user_id,msg_count,react_recv,react_given,first_ts,last_ts FROM user_stats "
                        "WHERE msg_count>0 ORDER BY msg_count DESC,user_id LIMIT ? OFFSET ?", (limit, offset)).fetchall()
    ids = [r["user_id"] for r in rows]
    users = _users_by_id(main, ids)
    emojis = _top_emojis(summ, "user_emoji_recv", ids)
    out = []
    for rank, r in enumerate(rows, offset + 1):
        u = users.get(r["user_id"]) or {"user_id": r["user_id"], **UNKNOWN_USER}
        d = dict(u)
        d.update(msg_count=r["msg_count"], reaction_received_count=r["react_recv"], reaction_given_count=r["react_given"],
                 first_msg_at=r["first_ts"], last_msg_at=r["last_ts"], rank=rank, top_emojis=emojis.get(r["user_id"], []))
        out.append(d)
    return out


def daily_series(summ):
    return [{"day": r[0], "c": r[1]} for r in summ.execute("SELECT day,SUM(c) FROM msg_hour GROUP BY day ORDER BY day")]


def hourly_series(summ):
    hours = [0] * 24
    for h, c in summ.execute("SELECT hour,SUM(c) FROM msg_hour GROUP BY hour"):
        hours[int(h)] = c
    return [{"hour": f"{i:02d}:00", "c": v} for i, v in enumerate(hours)]


def heatmap(summ, days=None):
    """7x24 matrix, rows Monday..Sunday (local time)."""
    grid = [[0] * 24 for _ in range(7)]
    sql = "SELECT day,hour,c FROM msg_hour"
    args = ()
    if days:
        since = (datetime.now(LOCAL_TZ) - timedelta(days=days)).strftime("%Y-%m-%d")
        sql += " WHERE day>=?"; args = (since,)
    for day, hour, c in summ.execute(sql, args):
        try:
            wd = datetime.strptime(day, "%Y-%m-%d").weekday()
        except ValueError:
            continue
        grid[wd][int(hour)] += c
    return grid


def kpis(main, summ):
    """Headline numbers with week-over-week deltas, relative to the latest active day in the data."""
    last = summ.execute("SELECT MAX(day) FROM msg_hour").fetchone()[0]
    total = summ.execute("SELECT COALESCE(SUM(msg_count),0) FROM user_stats").fetchone()[0]
    res = {"total_msgs": total,
           "total_threads": main.execute("SELECT count(*) FROM threads").fetchone()[0],
           "total_users": main.execute("SELECT count(*) FROM users").fetchone()[0],
           "total_reactions": summ.execute("SELECT COALESCE(SUM(c),0) FROM msg_react").fetchone()[0],
           "last_day": last}
    if last:
        end = datetime.strptime(last, "%Y-%m-%d")
        d7 = (end - timedelta(days=6)).strftime("%Y-%m-%d"); d14 = (end - timedelta(days=13)).strftime("%Y-%m-%d")
        cur = summ.execute("SELECT COALESCE(SUM(c),0) FROM msg_hour WHERE day>=?", (d7,)).fetchone()[0]
        prev = summ.execute("SELECT COALESCE(SUM(c),0) FROM msg_hour WHERE day>=? AND day<?", (d14, d7)).fetchone()[0]
        au = summ.execute("SELECT count(DISTINCT user_id) FROM user_day WHERE day>=?", (d7,)).fetchone()[0]
        aup = summ.execute("SELECT count(DISTINCT user_id) FROM user_day WHERE day>=? AND day<?", (d14, d7)).fetchone()[0]
        res.update(msgs_7d=cur, msgs_prev_7d=prev, active_users_7d=au, active_users_prev_7d=aup,
                   msgs_7d_delta=_delta(cur, prev), active_users_7d_delta=_delta(au, aup))
    return res


def _delta(cur, prev):
    if not prev:
        return None
    return round((cur - prev) * 100.0 / prev, 1)


def word_cloud(summ, limit=60):
    rows = summ.execute("SELECT word,c FROM word ORDER BY c DESC LIMIT ?", (limit * 4 + 50,)).fetchall()
    return textutil.format_word_cloud([(r[0], r[1]) for r in rows], limit)


def top_threads(main, summ, limit=10):
    rows = summ.execute("SELECT * FROM thread_stats ORDER BY msg_count DESC,thread_id LIMIT ?", (limit,)).fetchall()
    tids = [r["thread_id"] for r in rows]
    threads = {}
    if tids:
        ph = ",".join("?" for _ in tids)
        threads = {str(t["thread_id"]): dict(t) for t in main.execute(f"SELECT * FROM threads WHERE thread_id IN ({ph})", tids)}
    ops = {}
    op_ids = [r["op_msg_id"] for r in rows if r["op_msg_id"]]
    if op_ids:
        ph = ",".join("?" for _ in op_ids)
        ops = {str(m["message_id"]): m["content"] for m in main.execute(f"SELECT message_id,content FROM messages WHERE message_id IN ({ph})", op_ids)}
    users = _users_by_id(main, [r["op_author"] for r in rows])
    out = []
    for rank, r in enumerate(rows, 1):
        t = threads.get(str(r["thread_id"]))
        if not t:
            continue
        d = dict(t)
        u = users.get(str(r["op_author"])) or UNKNOWN_USER
        d.update(msg_count=r["msg_count"], rank=rank, first_content=ops.get(str(r["op_msg_id"]), ""),
                 op_msg_id=r["op_msg_id"] or "", created_at=r["first_ts"] or d.get("exported_at"),
                 last_active=r["last_ts"],
                 op_user={"username": u.get("username"), "nickname": u.get("nickname"), "avatar_url": u.get("avatar_url")})
        out.append(d)
    return out


def hot_messages(main, summ, limit=10):
    rows = summ.execute("SELECT message_id,c FROM msg_react ORDER BY c DESC,message_id LIMIT ?", (limit,)).fetchall()
    mids = [r["message_id"] for r in rows]
    if not mids:
        return []
    ph = ",".join("?" for _ in mids)
    msgs = {str(m["message_id"]): dict(m) for m in main.execute(f"SELECT * FROM messages WHERE message_id IN ({ph})", mids)}
    users = _users_by_id(main, [m["author_id"] for m in msgs.values()])
    tids = list({m["thread_id"] for m in msgs.values() if m["thread_id"]})
    tnames = {}
    if tids:
        tph = ",".join("?" for _ in tids)
        tnames = {str(t[0]): t[1] for t in main.execute(f"SELECT thread_id,name FROM threads WHERE thread_id IN ({tph})", tids)}
    reacts = collections.defaultdict(list)
    for r in main.execute(f"SELECT message_id,emoji_url,emoji_name,count(*) count FROM reactions WHERE message_id IN ({ph}) "
                          f"GROUP BY message_id,emoji_name ORDER BY message_id,count DESC", mids):
        if len(reacts[r["message_id"]]) < 3:
            reacts[r["message_id"]].append({"emoji_url": r["emoji_url"], "emoji_name": r["emoji_name"], "count": r["count"]})
    out = []
    for rank, r in enumerate(rows, 1):
        m = msgs.get(str(r["message_id"]))
        if not m:
            continue
        u = users.get(str(m["author_id"])) or UNKNOWN_USER
        m.update(rank=rank, total_reactions=r["c"], thread_name=tnames.get(str(m["thread_id"]), "Unknown"),
                 author={"username": u.get("username"), "nickname": u.get("nickname"), "avatar_url": u.get("avatar_url")},
                 detailed_reactions=reacts.get(m["message_id"], []))
        out.append(m)
    return out


def homepage(main, summ):
    data = kpis(main, summ)
    data["chart_daily"] = daily_series(summ)
    data["chart_hourly"] = hourly_series(summ)
    data["heatmap"] = heatmap(summ)
    data["server_word_cloud"] = word_cloud(summ, 60)
    data["server_word_rank"] = data["server_word_cloud"][:15]
    lb = leaderboard(main, summ, 50, 0)
    data["full_leaderboard"] = lb
    data["top_users"] = lb[:10]
    data["top_threads"] = top_threads(main, summ, 10)
    data["top_hot_msgs"] = hot_messages(main, summ, 10)
    return data


# ---------------- profile ----------------

def profile_base(main, summ, ids):
    ids = [str(i) for i in ids]
    ph = ",".join("?" for _ in ids)
    s = summ.execute(f"SELECT COALESCE(SUM(msg_count),0) m, COALESCE(SUM(react_recv),0) rr, COALESCE(SUM(react_given),0) rg, "
                     f"MIN(first_ts) f, MAX(last_ts) l FROM user_stats WHERE user_id IN ({ph})", ids).fetchone()
    thread_count = main.execute(f"SELECT count(*) FROM (SELECT DISTINCT thread_id FROM messages WHERE author_id IN ({ph}))", ids).fetchone()[0]
    started = summ.execute(f"SELECT count(*) FROM thread_stats WHERE op_author IN ({ph})", ids).fetchone()[0]
    hours = [0] * 24
    for h, c in summ.execute(f"SELECT hour,SUM(c) FROM user_hour WHERE user_id IN ({ph}) GROUP BY hour", ids):
        hours[int(h)] = c
    daily = []
    for day, c in summ.execute(f"SELECT day,SUM(c) FROM user_day WHERE user_id IN ({ph}) GROUP BY day ORDER BY day", ids):
        daily.append({"day": day, "c": c})

    def emo(table):
        return [dict(r) for r in summ.execute(
            f"SELECT emoji_name,MAX(emoji_url) emoji_url,SUM(c) c FROM {table} WHERE user_id IN ({ph}) GROUP BY emoji_name ORDER BY c DESC LIMIT 3", ids)]
    rank_row = summ.execute("SELECT count(*)+1 FROM user_stats WHERE msg_count>?", (s["m"],)).fetchone()
    return {"merged_ids": ids, "msg_count": s["m"], "reaction_received_count": s["rr"], "reaction_given_count": s["rg"],
            "first_msg_at": s["f"], "last_msg_at": s["l"], "thread_count": thread_count, "threads_started": started,
            "rank": rank_row[0] if s["m"] else None, "active_days": len(daily),
            "chart_daily": daily, "chart_hourly": [{"hour": f"{i:02d}:00", "c": v} for i, v in enumerate(hours)],
            "top_emojis_given": emo("user_emoji_given"), "top_emojis_received": emo("user_emoji_recv")}


def profile_extra(main, ids, recent_limit=2000):
    """Heavier per-user aggregates (interactions + personal word cloud); cached in summary.profile_extra."""
    ids = [str(i) for i in ids]
    ph = ",".join("?" for _ in ids)
    args = tuple(ids)
    incoming = [dict(r) for r in main.execute(
        f"SELECT u.user_id,u.nickname,u.username,u.avatar_url,count(*) score FROM reactions r "
        f"JOIN messages m ON r.message_id=m.message_id JOIN users u ON r.user_id=u.user_id "
        f"WHERE m.author_id IN ({ph}) AND r.user_id NOT IN ({ph}) GROUP BY u.user_id ORDER BY score DESC LIMIT 5", args + args)]
    outgoing = [dict(r) for r in main.execute(
        f"SELECT u.user_id,u.nickname,u.username,u.avatar_url,count(*) score FROM ("
        f"SELECT mentioned_user_id target_id FROM mentions WHERE author_id IN ({ph}) "
        f"UNION ALL SELECT m.author_id target_id FROM reactions r JOIN messages m ON r.message_id=m.message_id WHERE r.user_id IN ({ph})"
        f") raw JOIN users u ON raw.target_id=u.user_id WHERE u.user_id NOT IN ({ph}) GROUP BY u.user_id ORDER BY score DESC LIMIT 5",
        args + args + args)]
    texts = [r[0] for r in main.execute(
        f"SELECT content FROM messages WHERE author_id IN ({ph}) ORDER BY timestamp DESC LIMIT ?", args + (recent_limit,))]
    words = textutil.count_words(texts)
    return {"interactions_incoming": incoming, "interactions_outgoing": outgoing,
            "word_cloud_data": textutil.format_word_cloud(words, 50)}
