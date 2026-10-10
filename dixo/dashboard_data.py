"""Build homepage / profile / API payloads from summary.db (+ a few indexed lookups on the main DB).

Every function takes open connections: `main` (discord_data.db) and `summ` (summary.db).
No full-table scans on messages/reactions happen here.
"""
import collections
import time
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


def _read_last_good(summ):
    try:
        rows = dict(summ.execute("SELECT key,value FROM meta WHERE key LIKE 'last_good_%'").fetchall())
    except Exception:
        return None
    if "last_good_msgs" not in rows:
        return None
    return {
        "total_msgs": int(float(rows.get("last_good_msgs") or 0)),
        "total_reactions": int(float(rows.get("last_good_reacts") or 0)),
        "total_threads": int(float(rows.get("last_good_threads") or 0)),
        "last_good_at": rows.get("last_good_at"),
    }


def save_last_good_kpis(summ, total_msgs, total_reactions, total_threads):
    """Call when summary is caught up so mid-rebuild UI can keep showing real totals."""
    try:
        for k, v in (("last_good_msgs", total_msgs), ("last_good_reacts", total_reactions),
                     ("last_good_threads", total_threads), ("last_good_at", str(time.time()))):
            summ.execute("INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                         (k, str(v)))
        summ.commit()
    except Exception:
        pass


def kpis(main, summ, member_count=None):
    """Headline numbers with week-over-week deltas, relative to the latest active day in the data.

    member_count: live Discord roster size (portal presence / servers.member_count).
    Falls back to COUNT(users) which includes leavers and anyone who ever posted — often higher.
    """
    last = summ.execute("SELECT MAX(day) FROM msg_hour").fetchone()[0]
    total = summ.execute("SELECT COALESCE(SUM(msg_count),0) FROM user_stats").fetchone()[0]
    users_seen = main.execute("SELECT count(*) FROM users").fetchone()[0]
    members = int(member_count) if member_count is not None else users_seen
    res = {"total_msgs": total,
           "total_threads": main.execute("SELECT count(*) FROM threads").fetchone()[0],
           "total_users": members,
           "users_seen": users_seen,
           "total_reactions": summ.execute("SELECT COALESCE(SUM(c),0) FROM msg_react").fetchone()[0],
           "last_day": last,
           "msgs_7d": 0, "msgs_prev_7d": 0, "active_users_7d": 0, "active_users_prev_7d": 0,
           "msgs_7d_delta": None, "active_users_7d_delta": None}
    if last:
        end = datetime.strptime(last, "%Y-%m-%d")
        d7 = (end - timedelta(days=6)).strftime("%Y-%m-%d"); d14 = (end - timedelta(days=13)).strftime("%Y-%m-%d")
        cur = summ.execute("SELECT COALESCE(SUM(c),0) FROM msg_hour WHERE day>=?", (d7,)).fetchone()[0]
        prev = summ.execute("SELECT COALESCE(SUM(c),0) FROM msg_hour WHERE day>=? AND day<?", (d14, d7)).fetchone()[0]
        au = summ.execute("SELECT count(DISTINCT user_id) FROM user_day WHERE day>=?", (d7,)).fetchone()[0]
        aup = summ.execute("SELECT count(DISTINCT user_id) FROM user_day WHERE day>=? AND day<?", (d14, d7)).fetchone()[0]
        res.update(msgs_7d=cur, msgs_prev_7d=prev, active_users_7d=au, active_users_prev_7d=aup,
                   msgs_7d_delta=_delta(cur, prev), active_users_7d_delta=_delta(au, aup))
    # If summary was wiped mid-rebuild, totals can be absurdly low — prefer last-good snapshot.
    try:
        st_caught = summ.execute("SELECT value FROM meta WHERE key='caught_up'").fetchone()
        caught = st_caught and st_caught[0] == "1"
    except Exception:
        caught = False
    good = _read_last_good(summ)
    if good and not caught and good["total_msgs"] > (res["total_msgs"] or 0) * 2:
        res["total_msgs"] = good["total_msgs"]
        res["total_reactions"] = good["total_reactions"]
        # threads from main are already live-accurate; keep res total_threads
        res["kpis_from_last_good"] = True
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
    # Prefer summary.thread_stats (indexed); avoid DISTINCT scan on 5GB messages.
    started = summ.execute(f"SELECT count(*) FROM thread_stats WHERE op_author IN ({ph})", ids).fetchone()[0]
    thread_count = started
    hours = [0] * 24
    for h, c in summ.execute(f"SELECT hour,SUM(c) FROM user_hour WHERE user_id IN ({ph}) GROUP BY hour", ids):
        hours[int(h)] = c
    daily = [{"day": day, "c": c} for day, c in summ.execute(
        f"SELECT day,SUM(c) FROM user_day WHERE user_id IN ({ph}) GROUP BY day ORDER BY day", ids)]
    active_days = summ.execute(f"SELECT count(DISTINCT day) FROM user_day WHERE user_id IN ({ph})", ids).fetchone()[0]

    def emo(table):
        return [dict(r) for r in summ.execute(
            f"SELECT emoji_name,MAX(emoji_url) emoji_url,SUM(c) c FROM {table} WHERE user_id IN ({ph}) GROUP BY emoji_name ORDER BY c DESC LIMIT 3", ids)]
    rank_row = summ.execute("SELECT count(*)+1 FROM user_stats WHERE msg_count>?", (s["m"],)).fetchone()
    msg_count = s["m"]
    try:
        caught = summ.execute("SELECT value FROM meta WHERE key='caught_up'").fetchone()
        if not (caught and caught[0] == "1"):
            # live_msg fallback — author index exists; prefer truth over mid-rebuild crumbs
            live_n = main.execute(f"SELECT COUNT(*) FROM messages WHERE author_id IN ({ph})", ids).fetchone()[0]
            if live_n and live_n > (msg_count or 0):
                msg_count = live_n
    except Exception:
        pass
    return {"merged_ids": ids, "msg_count": msg_count, "reaction_received_count": s["rr"], "reaction_given_count": s["rg"],
            "first_msg_at": s["f"], "last_msg_at": s["l"], "thread_count": thread_count, "threads_started": started,
            "rank": rank_row[0] if msg_count else None, "active_days": active_days,
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


def _bucket_day(day):
    return day

def _bucket_week(day):
    try:
        d = datetime.strptime(day, "%Y-%m-%d")
        # Monday as week start (ISO)
        monday = d - timedelta(days=d.weekday())
        return monday.strftime("%Y-%m-%d")
    except ValueError:
        return day

def _bucket_month(day):
    return day[:7] if day and len(day) >= 7 else day

def _bucket_year(day):
    return day[:4] if day and len(day) >= 4 else day


_BUCKETS = {"day": _bucket_day, "week": _bucket_week, "month": _bucket_month, "year": _bucket_year}


def _filter_days(summ, since=None, until=None):
    sql = "SELECT day, SUM(c) c FROM msg_hour"
    where, args = [], []
    if since:
        where.append("day>=?"); args.append(since)
    if until:
        where.append("day<=?"); args.append(until)
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " GROUP BY day ORDER BY day"
    return [(r[0], r[1]) for r in summ.execute(sql, args)]


def series_messages(summ, grain="day", since=None, until=None):
    grain = grain if grain in _BUCKETS else "day"
    bucket = _BUCKETS[grain]
    agg = collections.Counter()
    for day, c in _filter_days(summ, since, until):
        agg[bucket(day)] += c
    return [{"t": k, "c": agg[k]} for k in sorted(agg)]


_SQL_BUCKET = {
    "day": "day",
    "week": "date(day, 'weekday 0', '-6 days')",   # ISO Monday of that week
    "month": "substr(day,1,7)",
    "year": "substr(day,1,4)",
}


def series_active_users(summ, grain="day", since=None, until=None):
    """Distinct active members per bucket, aggregated in SQL (user_day is unique per (user, day))."""
    grain = grain if grain in _SQL_BUCKET else "day"
    expr = _SQL_BUCKET[grain]
    agg = "COUNT(*)" if grain == "day" else "COUNT(DISTINCT user_id)"
    where, args = [], []
    if since:
        where.append("day>=?"); args.append(since)
    if until:
        where.append("day<=?"); args.append(until)
    sql = f"SELECT {expr} t, {agg} c FROM user_day"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " GROUP BY t ORDER BY t"
    return [{"t": t, "c": c} for t, c in summ.execute(sql, args) if t]


def series_thread_growth(summ, grain="month", since=None, until=None):
    """New threads by first_ts (OP timestamp), cumulative optional via client."""
    grain = grain if grain in _BUCKETS else "month"
    bucket = _BUCKETS[grain]
    sql = "SELECT substr(first_ts,1,10) day, count(*) c FROM thread_stats WHERE first_ts IS NOT NULL"
    args = []
    if since:
        sql += " AND substr(first_ts,1,10)>=?"; args.append(since)
    if until:
        sql += " AND substr(first_ts,1,10)<=?"; args.append(until)
    sql += " GROUP BY day ORDER BY day"
    agg = collections.Counter()
    for day, c in summ.execute(sql, args):
        if day:
            agg[bucket(day)] += c
    out, running = [], 0
    for k in sorted(agg):
        running += agg[k]
        out.append({"t": k, "c": agg[k], "cum": running})
    return out


def series_reply_rate(summ, grain="week", since=None, until=None):
    """Approximate reply rate = (messages - new threads) / messages per bucket."""
    msgs = {x["t"]: x["c"] for x in series_messages(summ, grain, since, until)}
    threads = {x["t"]: x["c"] for x in series_thread_growth(summ, grain, since, until)}
    keys = sorted(set(msgs) | set(threads))
    out = []
    for k in keys:
        m = msgs.get(k, 0); t = threads.get(k, 0)
        replies = max(0, m - t)
        out.append({"t": k, "messages": m, "threads": t, "replies": replies,
                    "reply_rate": round(replies / m, 4) if m else 0})
    return out


def top_emoji(summ, limit=20):
    rows = summ.execute(
        "SELECT emoji_name, MAX(emoji_url) emoji_url, SUM(c) c FROM user_emoji_given "
        "GROUP BY emoji_name ORDER BY c DESC LIMIT ?", (limit,)).fetchall()
    return [{"emoji_name": r[0], "emoji_url": r[1], "c": r[2]} for r in rows]


# Activity retention tags (monthly grain). Used for cleanup / engagement prep.
TAG_ALWAYS = "always_active"      # 一贯活跃
TAG_FADED = "faded"               # 活跃后消失
TAG_RARE = "rare"                 # 几乎不活跃
TAG_RETURNED = "returned"         # 消失后回归
TAG_NEW = "new"                   # 近两月新活跃


def _month_range(start_ym, end_ym):
    """Inclusive YYYY-MM range."""
    if not start_ym or not end_ym or start_ym > end_ym:
        return []
    y, m = int(start_ym[:4]), int(start_ym[5:7])
    ey, em = int(end_ym[:4]), int(end_ym[5:7])
    out = []
    while (y, m) <= (ey, em):
        out.append(f"{y:04d}-{m:02d}")
        m += 1
        if m > 12:
            m = 1
            y += 1
    return out


def classify_member_activity(active_months, first_ym, last_ym, as_of_ym):
    """Classify one member from their set of active YYYY-MM keys.

    Tiers (priority order):
      new       — first activity within last 2 months of as_of
      returned  — ≥2 consecutive inactive months in the middle, then active again near as_of
      always_active — active in ≥70% of months since first, and active in last 2 months
      faded     — had activity earlier, silent in last 2 months, ≥2 active months historically
      rare      — otherwise (≤2 active months or <15% coverage)
    """
    if not active_months or not first_ym:
        return TAG_RARE
    months = _month_range(first_ym, as_of_ym)
    if not months:
        return TAG_RARE
    active = set(active_months)
    n = len(months)
    n_active = sum(1 for m in months if m in active)
    recent = months[-2:] if len(months) >= 2 else months
    recent_hit = sum(1 for m in recent if m in active)
    # new
    span_from_first = _month_range(first_ym, as_of_ym)
    if len(span_from_first) <= 2:
        return TAG_NEW
    # returned: gap of ≥2 inactive then activity in last 2 months
    if recent_hit >= 1 and n >= 4:
        # find longest trailing gap before last active streak
        bits = [1 if m in active else 0 for m in months]
        # look for 0,0 pattern then later 1 near end
        gap = False
        for i in range(len(bits) - 2):
            if bits[i] == 0 and bits[i + 1] == 0:
                # was there activity before this gap?
                if any(bits[:i]):
                    gap = True
                    break
        if gap and recent_hit >= 1 and bits[-1] == 1:
            return TAG_RETURNED
    ratio = n_active / n if n else 0
    if recent_hit >= 1 and ratio >= 0.70 and n_active >= 3:
        return TAG_ALWAYS
    if recent_hit == 0 and n_active >= 2:
        return TAG_FADED
    if n_active <= 2 or ratio < 0.15:
        return TAG_RARE
    if recent_hit == 0:
        return TAG_FADED
    return TAG_ALWAYS if ratio >= 0.5 else TAG_RARE


def member_activity_tags(summ, as_of=None, sample_per_tag=12):
    """Tag every member with monthly activity pattern; return summary + full by_tag lists.

    as_of: YYYY-MM-DD or YYYY-MM; defaults to max(user_day.day).
    samples: first sample_per_tag per tier (compat); by_tag: ALL members per tier.
    """
    row = summ.execute("SELECT MAX(day) FROM user_day").fetchone()
    max_day = (row[0] if row else None) or as_of
    if as_of:
        as_of_ym = as_of[:7]
    else:
        as_of_ym = (max_day or "")[:7]
    if not as_of_ym:
        return {"as_of": None, "counts": {}, "samples": {}, "tags": []}

    # active months per user
    by_user = collections.defaultdict(set)
    for uid, day in summ.execute("SELECT user_id, day FROM user_day"):
        if day and day[:7] <= as_of_ym:
            by_user[str(uid)].add(day[:7])

    # first/last from user_stats when available
    first_last = {}
    try:
        for r in summ.execute("SELECT user_id, substr(first_ts,1,7), substr(last_ts,1,7) FROM user_stats"):
            first_last[str(r[0])] = (r[1], r[2])
    except Exception:
        pass

    counts = collections.Counter()
    samples = {TAG_ALWAYS: [], TAG_FADED: [], TAG_RARE: [], TAG_RETURNED: [], TAG_NEW: []}
    by_tag = {TAG_ALWAYS: [], TAG_FADED: [], TAG_RARE: [], TAG_RETURNED: [], TAG_NEW: []}
    for uid, months in by_user.items():
        fl = first_last.get(uid)
        first_ym = (fl[0] if fl and fl[0] else min(months))
        last_ym = (fl[1] if fl and fl[1] else max(months))
        if first_ym > as_of_ym:
            continue
        tag = classify_member_activity(months, first_ym, min(last_ym, as_of_ym), as_of_ym)
        counts[tag] += 1
        rec = {"user_id": uid, "tag": tag, "first_month": first_ym, "last_month": last_ym,
               "active_months": len(months), "months_span": len(_month_range(first_ym, as_of_ym))}
        by_tag[tag].append(rec)
        if len(samples[tag]) < sample_per_tag:
            samples[tag].append(rec)

    order = [TAG_ALWAYS, TAG_RETURNED, TAG_FADED, TAG_RARE, TAG_NEW]
    # Sort each tier: most active months first
    for k in order:
        by_tag[k].sort(key=lambda r: (-r.get("active_months", 0), r.get("user_id") or ""))
        samples[k] = by_tag[k][:sample_per_tag]
    return {
        "as_of": as_of_ym,
        "counts": {k: counts.get(k, 0) for k in order},
        "labels": {
            TAG_ALWAYS: {"zh": "一贯活跃", "en": "Always active"},
            TAG_RETURNED: {"zh": "消失后回归", "en": "Returned after gap"},
            TAG_FADED: {"zh": "活跃后消失", "en": "Active then gone"},
            TAG_RARE: {"zh": "几乎不活跃", "en": "Rarely active"},
            TAG_NEW: {"zh": "近两月新活跃", "en": "New (≤2 months)"},
        },
        "samples": samples,
        "by_tag": by_tag,
        "total_tagged": sum(counts.values()),
    }


def activity_tag_monthly_series(summ, since=None, until=None):
    """For each month in range, count of members by tier as-of that month-end.

    Month axis = every YYYY-MM with any user_day (or msg_hour) activity in [since, until].
    No artificial month cap — full selected range.
    """
    days = _filter_days(summ, since, until)
    month_ends = sorted({d[:7] for d, _ in days}) if days else []
    if not month_ends:
        # Fall back to user_day span so chart still fills when msg_hour empty
        q = "SELECT DISTINCT substr(day,1,7) FROM user_day WHERE 1=1"
        args = []
        if since:
            q += " AND day>=?"; args.append(since)
        if until:
            q += " AND day<=?"; args.append(until)
        q += " ORDER BY 1"
        month_ends = [r[0] for r in summ.execute(q, args) if r[0]]
    if not month_ends:
        return []
    # Preload all user months once
    by_user = collections.defaultdict(set)
    for uid, day in summ.execute("SELECT user_id, day FROM user_day"):
        if day:
            by_user[str(uid)].add(day[:7])
    first_last = {}
    try:
        for r in summ.execute("SELECT user_id, substr(first_ts,1,7), substr(last_ts,1,7) FROM user_stats"):
            first_last[str(r[0])] = (r[1], r[2])
    except Exception:
        pass
    out = []
    for ym in month_ends:
        counts = collections.Counter()
        for uid, months in by_user.items():
            sub = {m for m in months if m <= ym}
            if not sub:
                continue
            fl = first_last.get(uid)
            first_ym = (fl[0] if fl and fl[0] else min(sub))
            if first_ym > ym:
                continue
            last_ym = min(max(sub), ym)
            tag = classify_member_activity(sub, first_ym, last_ym, ym)
            counts[tag] += 1
        out.append({
            "t": ym,
            "always_active": counts[TAG_ALWAYS],
            "returned": counts[TAG_RETURNED],
            "faded": counts[TAG_FADED],
            "rare": counts[TAG_RARE],
            "new": counts[TAG_NEW],
        })
    return out


_BUNDLE_CACHE = {}


def _summary_version(summ):
    try:
        row = summ.execute("SELECT group_concat(key||'='||value, ';') FROM meta").fetchone()
        return row[0] if row else ""
    except Exception:
        return None


def analytics_bundle(summ, since=None, until=None, cache_key=None):
    """All overview analytics series. Cached per (cache_key, summary watermark, range)."""
    ver = _summary_version(summ) if cache_key is not None else None
    if cache_key is not None and ver is not None:
        hit = _BUNDLE_CACHE.get((cache_key, since, until))
        if hit and hit[0] == ver:
            return hit[1]
    data = _analytics_bundle(summ, since, until)
    if cache_key is not None and ver is not None:
        if len(_BUNDLE_CACHE) > 64:
            _BUNDLE_CACHE.clear()
        _BUNDLE_CACHE[(cache_key, since, until)] = (ver, data)
    return data


def _analytics_bundle(summ, since=None, until=None):
    until_ym = (until or "")[:7] or None
    tags = member_activity_tags(summ, as_of=until or since)
    return {
        "messages_day": series_messages(summ, "day", since, until),
        "messages_week": series_messages(summ, "week", since, until),
        "messages_month": series_messages(summ, "month", since, until),
        "messages_year": series_messages(summ, "year", since, until),
        "active_users_day": series_active_users(summ, "day", since, until),
        "active_users_week": series_active_users(summ, "week", since, until),
        "active_users_month": series_active_users(summ, "month", since, until),
        "thread_growth_month": series_thread_growth(summ, "month", since, until),
        "reply_rate_week": series_reply_rate(summ, "week", since, until),
        "top_emoji": top_emoji(summ, 20),
        "heatmap": heatmap(summ),
        "word_cloud": word_cloud(summ, 40),
        "activity_tags": tags,
        "activity_tags_monthly": activity_tag_monthly_series(summ, None, None),  # full history; trend presets do not clip this
    }
