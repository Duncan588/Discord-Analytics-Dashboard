"""服务器总结 / 个人总结（年 / 月 / 周 / 全部）。

数据来自主库（按时间区间扫描 messages）+ summary.db（user_day / msg_react / thread_stats）+ insights.db（梗）。
计算放在后台线程里，结果按 (服务器, 范围, 周期) 缓存到 data/servers/<gid>/recap/*.json：
已结束的周期永久缓存，进行中的周期 / 全部 1 小时过期。读连接都走 ctx.connect（全局黑名单 TEMP VIEW），
输出再经 blacklist.scrub 兜底。时间按 Asia/Shanghai（与 summary.db 的 day 一致），库里时间戳是 UTC。
"""
import collections
import json
import os
import re
import threading
import time
from datetime import date, datetime, timedelta, timezone

from shared import blacklist

TZ = timezone(timedelta(hours=8))
TTL_OPEN = 3600
_lock = threading.Lock()
_jobs = {}          # key -> {"state": "building"|"error", "at": ts, "error": str}
_mem = {}           # key -> (ts, data)
_MEM_MAX = 256


# ------------------------------------------------------------------ periods
def parse_period(kind, value=None, today=None):
    """-> dict(kind, value, start, end, prev_start, prev_end, label, open). start/end are local dates [start, end)."""
    today = today or datetime.now(TZ).date()
    kind = (kind or "year").lower()
    if kind == "all":
        return {"kind": "all", "value": "all", "start": None, "end": None, "prev_start": None, "prev_end": None,
                "label": "全部", "open": True}
    if kind == "month":
        m = re.match(r"^(\d{4})-(\d{1,2})$", str(value or ""))
        y, mo = (int(m.group(1)), int(m.group(2))) if m else (today.year, today.month)
        mo = min(12, max(1, mo))
        start = date(y, mo, 1)
        end = date(y + (mo == 12), mo % 12 + 1, 1)
        pe = start; ps = date(start.year - (start.month == 1), (start.month - 2) % 12 + 1, 1)
        return _p("month", f"{y}-{mo:02d}", start, end, ps, pe, f"{y} 年 {mo} 月", today)
    if kind == "week":
        m = re.match(r"^(\d{4})-W(\d{1,2})$", str(value or ""), re.I)
        if m:
            start = date.fromisocalendar(int(m.group(1)), max(1, min(53, int(m.group(2)))), 1)
        else:
            start = today - timedelta(days=today.weekday())
        iy, iw, _ = start.isocalendar()
        end = start + timedelta(days=7)
        return _p("week", f"{iy}-W{iw:02d}", start, end, start - timedelta(days=7), start,
                  f"{iy} 年第 {iw} 周（{start:%m/%d}–{end - timedelta(days=1):%m/%d}）", today)
    m = re.match(r"^(\d{4})$", str(value or ""))
    y = int(m.group(1)) if m else today.year
    return _p("year", str(y), date(y, 1, 1), date(y + 1, 1, 1), date(y - 1, 1, 1), date(y, 1, 1), f"{y} 年", today)


def _p(kind, value, start, end, ps, pe, label, today):
    return {"kind": kind, "value": value, "start": start, "end": end, "prev_start": ps, "prev_end": pe,
            "label": label, "open": end > today}


def _utc(d):
    """local date -> UTC ISO prefix comparable with stored timestamps."""
    return datetime(d.year, d.month, d.day, tzinfo=TZ).astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


def _local(ts):
    try:
        s = str(ts).replace("Z", "+00:00").replace(" ", "T", 1)
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(TZ)
    except (TypeError, ValueError):
        return None


def _day_clause(p, col="day", prev=False):
    s, e = (p["prev_start"], p["prev_end"]) if prev else (p["start"], p["end"])
    if s is None:
        return "1=1", ()
    return f"{col}>=? AND {col}<?", (s.isoformat(), e.isoformat())


def _ts_clause(p, col="timestamp", prev=False):
    s, e = (p["prev_start"], p["prev_end"]) if prev else (p["start"], p["end"])
    if s is None:
        return "1=1", ()
    return f"{col}>=? AND {col}<?", (_utc(s), _utc(e))


def periods_available(sc):
    r = sc.execute("SELECT MIN(day), MAX(day) FROM user_day").fetchone()
    if not r or not r[0]:
        y = datetime.now(TZ).year
        return {"years": [y], "first_day": None, "last_day": None}
    a, b = int(r[0][:4]), int(r[1][:4])
    return {"years": list(range(b, a - 1, -1)), "first_day": r[0], "last_day": r[1]}


# ------------------------------------------------------------------ helpers
def _users(conn, ids):
    ids = [str(i) for i in ids if i]
    out = {}
    for i in range(0, len(ids), 500):
        ch = ids[i:i + 500]
        for r in conn.execute(f"SELECT user_id,username,nickname,avatar_url FROM users WHERE user_id IN ({','.join('?' * len(ch))})", ch):
            out[r[0]] = {"user_id": r[0], "name": r[2] or r[1] or r[0], "username": r[1], "avatar_url": r[3]}
    return out


def _u(umap, uid, **extra):
    d = dict(umap.get(str(uid)) or {"user_id": str(uid), "name": str(uid), "avatar_url": None})
    d.update(extra)
    return d


def _pct(cur, prev):
    if not prev:
        return None
    return round((cur - prev) * 100.0 / prev, 1)


def _thread_names(conn, tids):
    tids = [str(t) for t in tids]
    if not tids:
        return {}
    return {r[0]: r[1] for r in conn.execute(f"SELECT thread_id,name FROM threads WHERE thread_id IN ({','.join('?' * len(tids))})", tids)}


def _memes_in(ic, p, texts=None, n=10):
    """Top memes from insights (monthly counts within the period); or count in given texts."""
    if ic is None:
        return []
    try:
        rows = ic.execute("SELECT term,meaning,monthly,total FROM memes").fetchall()
    except Exception:
        return []
    out = []
    for term, meaning, monthly, total in rows:
        if texts is not None:
            c = sum(t.count(term) for t in texts if t)
        elif p["start"] is None:
            c = int(total or 0)
        else:
            try:
                mm = json.loads(monthly or "{}")
            except ValueError:
                mm = {}
            s, e = p["start"].strftime("%Y-%m"), (p["end"] - timedelta(days=1)).strftime("%Y-%m")
            c = sum(int(v or 0) for k, v in mm.items() if s <= k <= e)
        if c > 0:
            out.append({"term": term, "meaning": meaning, "count": c})
    out.sort(key=lambda x: -x["count"])
    return out[:n]


def _words(texts, n=30):
    from dixo import textutil
    extra = textutil.extra_stopwords()
    c = collections.Counter()
    for t in texts:
        if t:
            c.update(textutil.tokenize(t, extra))
    return [{"word": w, "count": k} for w, k in c.most_common(n)]


# ------------------------------------------------------------------ server recap
def server_recap(conn, sc, ic, p):
    out = {"period": _pjson(p), "generated_at": datetime.now(TZ).isoformat(timespec="seconds")}
    dc, da = _day_clause(p)
    pc, pa = _day_clause(p, prev=True)
    per_user = {r[0]: r[1] for r in sc.execute(f"SELECT user_id,SUM(c) FROM user_day WHERE {dc} GROUP BY user_id", da)}
    prev_user = {r[0]: r[1] for r in sc.execute(f"SELECT user_id,SUM(c) FROM user_day WHERE {pc} GROUP BY user_id", pa)} if p["prev_start"] else {}
    total = sum(per_user.values()); prev_total = sum(prev_user.values())
    days = sc.execute(f"SELECT day,SUM(c) c FROM user_day WHERE {dc} GROUP BY day ORDER BY c DESC", da).fetchall()
    first = {r[0]: r[1] for r in sc.execute("SELECT user_id,MIN(day) FROM user_day GROUP BY user_id")}
    in_range = (lambda d: d and (p["start"] is None or p["start"].isoformat() <= d < p["end"].isoformat()))
    newcomers = sorted([u for u, d in first.items() if in_range(d) and u in per_user], key=lambda u: -per_user[u])
    # hours (local) from msg_hour
    hours = [0] * 24
    for h, c in sc.execute(f"SELECT hour,SUM(c) FROM msg_hour WHERE {dc} GROUP BY hour", da):
        hours[int(h) % 24] += int(c or 0)

    # one pass over the period's messages for threads / superlatives / words sample
    tc, ta = _ts_clause(p, "m.timestamp")
    thr = {}
    night = collections.Counter(); early = collections.Counter(); long_msgs = collections.Counter(); replies = collections.Counter()
    sample = []
    for mid, tid, aid, ts, ln, rep, content in conn.execute(
            f"SELECT m.message_id,m.thread_id,m.author_id,m.timestamp,length(m.content),m.reply_to_msg_id,"
            f"CASE WHEN abs(random())%20=0 THEN m.content END FROM messages m WHERE {tc}", ta):
        t = thr.get(tid)
        if t is None:
            t = thr[tid] = [0, set(), ts, ts]
        t[0] += 1; t[1].add(aid)
        if ts and ts < t[2]: t[2] = ts
        if ts and ts > t[3]: t[3] = ts
        lt = _local(ts)
        if lt is not None and aid:
            if 0 <= lt.hour < 5: night[aid] += 1
            elif 5 <= lt.hour < 8: early[aid] += 1
        if aid and (ln or 0) >= 200: long_msgs[aid] += 1
        if aid and rep: replies[aid] += 1
        if content and len(sample) < 60000: sample.append(content)
    hot = sorted(thr.items(), key=lambda kv: (-kv[1][0], -len(kv[1][1])))[:10]
    by_people = sorted(thr.items(), key=lambda kv: (-len(kv[1][1]), -kv[1][0]))[:5]
    def span(v):
        a, b = _local(v[2]), _local(v[3])
        return (b - a).total_seconds() if a and b else 0
    longest = sorted([kv for kv in thr.items() if kv[1][0] >= 20], key=lambda kv: -span(kv[1]))[:5]
    names = _thread_names(conn, [k for k, _ in hot + by_people + longest])

    # hot messages by reactions
    hot_msgs = conn.execute(
        f"SELECT m.message_id,m.thread_id,m.author_id,m.content,m.timestamp,r.c FROM messages m JOIN s.msg_react r ON r.message_id=m.message_id "
        f"WHERE {tc} ORDER BY r.c DESC LIMIT 10", ta).fetchall() if _has_attached(conn) else []
    hm_ids = [r[0] for r in hot_msgs]
    emo = {}
    if hm_ids:
        for r in conn.execute(f"SELECT message_id,emoji_name,emoji_url,COUNT(*) c FROM reactions WHERE message_id IN ({','.join('?' * len(hm_ids))}) GROUP BY message_id,emoji_name ORDER BY c DESC", hm_ids):
            emo.setdefault(r[0], []).append({"emoji_name": r[1], "emoji_url": r[2], "count": r[3]})
    reacts_total = 0; top_emojis = []
    if _has_attached(conn):
        reacts_total = conn.execute(f"SELECT COALESCE(SUM(r.c),0) FROM messages m JOIN s.msg_react r ON r.message_id=m.message_id WHERE {tc}", ta).fetchone()[0]
    if p["start"] is None:
        top_emojis = [{"emoji_name": r[0], "emoji_url": r[1], "count": r[2]} for r in sc.execute(
            "SELECT emoji_name,MAX(emoji_url),SUM(c) c FROM user_emoji_recv GROUP BY emoji_name ORDER BY c DESC LIMIT 12")]
    else:
        top_emojis = [{"emoji_name": r[0], "emoji_url": r[1], "count": r[2]} for r in conn.execute(
            f"SELECT x.emoji_name,MAX(x.emoji_url),COUNT(*) c FROM reactions x JOIN messages m ON m.message_id=x.message_id WHERE {tc} "
            f"GROUP BY x.emoji_name ORDER BY c DESC LIMIT 12", ta)]

    # thread starters
    sc_clause, sc_args = _ts_clause(p, "first_ts")
    starters = sc.execute(f"SELECT op_author,COUNT(*) c FROM thread_stats WHERE {sc_clause} AND op_author IS NOT NULL GROUP BY op_author ORDER BY c DESC LIMIT 8", sc_args).fetchall()
    threads_new = sc.execute(f"SELECT COUNT(*) FROM thread_stats WHERE {sc_clause}", sc_args).fetchone()[0]
    pc2, pa2 = _ts_clause(p, "first_ts", prev=True)
    threads_new_prev = sc.execute(f"SELECT COUNT(*) FROM thread_stats WHERE {pc2}", pa2).fetchone()[0] if p["prev_start"] else None

    # member status changes
    ms = {"joined": 0, "left": 0, "deleted": 0}
    try:
        jc, ja = _ts_clause(p, "joined_at"); lc, la = _ts_clause(p, "left_at")
        ms["joined"] = conn.execute(f"SELECT COUNT(*) FROM member_status WHERE joined_at IS NOT NULL AND {jc}", ja).fetchone()[0]
        ms["left"] = conn.execute(f"SELECT COUNT(*) FROM member_status WHERE in_guild=0 AND deleted=0 AND left_at IS NOT NULL AND {lc}", la).fetchone()[0]
        ms["deleted"] = conn.execute(f"SELECT COUNT(*) FROM member_status WHERE deleted=1 AND (left_at IS NULL OR {lc})", la).fetchone()[0]
    except Exception:
        ms = None

    top_posters = sorted(per_user.items(), key=lambda kv: -kv[1])[:10]
    rising = sorted([(u, c - prev_user.get(u, 0)) for u, c in per_user.items() if c >= 20 and u not in newcomers[:0]],
                    key=lambda kv: -kv[1])[:6] if p["prev_start"] else []
    ids = set(u for u, _ in top_posters) | set(u for u, _ in rising) | set(newcomers[:8]) | {r[0] for r in starters} \
        | {r[2] for r in hot_msgs} | set(u for u, _ in night.most_common(1)) | set(u for u, _ in early.most_common(1)) \
        | set(u for u, _ in long_msgs.most_common(1)) | set(u for u, _ in replies.most_common(1))
    um = _users(conn, ids)
    tl = lambda kv: {"thread_id": kv[0], "name": names.get(kv[0]) or "未命名帖子", "messages": kv[1][0], "people": len(kv[1][1]),
                     "first": kv[1][2], "last": kv[1][3], "span_days": round(span(kv[1]) / 86400, 1)}
    peak_hour = max(range(24), key=lambda h: hours[h]) if any(hours) else None
    sup = []
    for title, counter, unit in (("夜猫子", night, "条凌晨 0–5 点的消息"), ("早起鸟", early, "条早上 5–8 点的消息"),
                                 ("长文作家", long_msgs, "条 200 字以上的长消息"), ("回复狂人", replies, "次回复别人")):
        if counter:
            u, c = counter.most_common(1)[0]
            sup.append({"title": title, "user": _u(um, u), "value": c, "unit": unit})
    if by_people:
        kv = by_people[0]; sup.append({"title": "最热闹的广场", "thread": tl(kv), "value": len(kv[1][1]), "unit": "人参与"})
    out.update({
        "totals": {"messages": total, "messages_prev": prev_total if p["prev_start"] else None, "messages_change": _pct(total, prev_total),
                   "active_members": len(per_user), "active_prev": len(prev_user) if p["prev_start"] else None,
                   "active_change": _pct(len(per_user), len(prev_user)), "threads_new": threads_new, "threads_new_prev": threads_new_prev,
                   "threads_change": _pct(threads_new, threads_new_prev or 0), "threads_active": len(thr), "reactions": reacts_total,
                   "newcomers": len(newcomers), "active_days": len(days)},
        "peak_day": {"day": days[0][0], "messages": days[0][1]} if days else None,
        "peak_hour": {"hour": peak_hour, "messages": hours[peak_hour]} if peak_hour is not None else None,
        "hours": hours,
        "daily": sorted([[d, c] for d, c in days]),
        "hot_threads": [tl(kv) for kv in hot],
        "longest_threads": [tl(kv) for kv in longest],
        "hot_messages": [{"message_id": r[0], "thread_id": r[1], "author": _u(um, r[2]), "author_id": r[2], "content": (r[3] or "")[:280],
                          "timestamp": r[4], "reactions": r[5], "emojis": emo.get(r[0], [])[:4]} for r in hot_msgs],
        "top_posters": [_u(um, u, messages=c, share=round(c * 100.0 / total, 1) if total else 0) for u, c in top_posters],
        "rising": [_u(um, u, delta=d, messages=per_user[u]) for u, d in rising if d > 0],
        "newcomers": [_u(um, u, messages=per_user[u], first_day=first.get(u)) for u in newcomers[:8]],
        "starters": [_u(um, r[0], threads=r[1]) for r in starters],
        "top_emojis": top_emojis,
        "memes": _memes_in(ic, p),
        "words": _words(sample, 30),
        "member_changes": ms,
        "superlatives": sup,
    })
    return out


def _has_attached(conn):
    try:
        return any(r[1] == "s" for r in conn.execute("PRAGMA database_list"))
    except Exception:
        return False


def _pjson(p):
    return {k: (v.isoformat() if isinstance(v, date) else v) for k, v in p.items()}


# ------------------------------------------------------------------ personal recap
def personal_recap(conn, sc, ic, p, ids):
    ids = [str(i) for i in ids]
    ph = ",".join("?" * len(ids))
    dc, da = _day_clause(p); pc, pa = _day_clause(p, prev=True)
    per_user = collections.Counter({r[0]: r[1] for r in sc.execute(f"SELECT user_id,SUM(c) FROM user_day WHERE {dc} GROUP BY user_id", da)})
    mine_ids = set(ids)
    my = sum(per_user[i] for i in ids)
    others = sorted([c for u, c in per_user.items() if u not in mine_ids], reverse=True)
    rank = 1 + sum(1 for c in others if c > my) if my else None
    prev = sc.execute(f"SELECT COALESCE(SUM(c),0) FROM user_day WHERE user_id IN ({ph}) AND {pc}", ids + list(pa)).fetchone()[0] if p["prev_start"] else None
    days = [r[0] for r in sc.execute(f"SELECT DISTINCT day FROM user_day WHERE user_id IN ({ph}) AND {dc} AND c>0 ORDER BY day", ids + list(da))]
    prev_days = sc.execute(f"SELECT COUNT(DISTINCT day) FROM user_day WHERE user_id IN ({ph}) AND {pc}", ids + list(pa)).fetchone()[0] if p["prev_start"] else None
    best, cur, last = 0, 0, None
    for d in days:
        dd = date.fromisoformat(d)
        cur = cur + 1 if last and (dd - last).days == 1 else 1
        best = max(best, cur); last = dd
    tc, ta = _ts_clause(p, "m.timestamp")
    hours = [0] * 24; wd = [0] * 7; thr = collections.Counter(); texts = []; msg_ids = []
    for mid, tid, ts, content in conn.execute(f"SELECT m.message_id,m.thread_id,m.timestamp,m.content FROM messages m WHERE m.author_id IN ({ph}) AND {tc}", ids + list(ta)):
        lt = _local(ts)
        if lt: hours[lt.hour] += 1; wd[lt.weekday()] += 1
        thr[tid] += 1
        if content and len(texts) < 20000: texts.append(content)
        msg_ids.append(mid)
    names = _thread_names(conn, [t for t, _ in thr.most_common(6)])
    hot = []
    if _has_attached(conn):
        hot = conn.execute(f"SELECT m.message_id,m.thread_id,m.content,m.timestamp,r.c FROM messages m JOIN s.msg_react r ON r.message_id=m.message_id "
                           f"WHERE m.author_id IN ({ph}) AND {tc} ORDER BY r.c DESC LIMIT 5", ids + list(ta)).fetchall()
    hnames = _thread_names(conn, [r[1] for r in hot])
    emo = {}
    if hot:
        hid = [r[0] for r in hot]
        for r in conn.execute(f"SELECT message_id,emoji_name,emoji_url,COUNT(*) c FROM reactions WHERE message_id IN ({','.join('?' * len(hid))}) GROUP BY message_id,emoji_name ORDER BY c DESC", hid):
            emo.setdefault(r[0], []).append({"emoji_name": r[1], "emoji_url": r[2], "count": r[3]})
    recv = collections.Counter(); recv_url = {}; recv_from = collections.Counter()
    for i in range(0, len(msg_ids), 900):
        ch = msg_ids[i:i + 900]
        for name, url, uid in conn.execute(f"SELECT emoji_name,emoji_url,user_id FROM reactions WHERE message_id IN ({','.join('?' * len(ch))})", ch):
            recv[name] += 1; recv_url.setdefault(name, url)
            if uid and uid not in mine_ids: recv_from[uid] += 1
    given = collections.Counter(); given_url = {}; given_to = collections.Counter()
    for name, url, aid in conn.execute(f"SELECT x.emoji_name,x.emoji_url,m.author_id FROM reactions x JOIN messages m ON m.message_id=x.message_id "
                                       f"WHERE x.user_id IN ({ph}) AND {tc}", ids + list(ta)):
        given[name] += 1; given_url.setdefault(name, url)
        if aid and aid not in mine_ids: given_to[aid] += 1
    # interaction partners: replies both ways in period + reactions exchanged
    partners = collections.Counter()
    for (aid,) in conn.execute(f"SELECT m2.author_id FROM messages m1 JOIN messages m2 ON m1.reply_to_msg_id=m2.message_id WHERE m1.author_id IN ({ph}) AND {tc.replace('m.', 'm1.')}", ids + list(ta)):
        if aid and aid not in mine_ids: partners[aid] += 2
    for (aid,) in conn.execute(f"SELECT m1.author_id FROM messages m2 JOIN messages m1 ON m1.reply_to_msg_id=m2.message_id WHERE m2.author_id IN ({ph}) AND {tc.replace('m.', 'm1.')}", ids + list(ta)):
        if aid and aid not in mine_ids: partners[aid] += 2
    partners.update(recv_from); partners.update(given_to)
    top_p = partners.most_common(6)
    um = _users(conn, [u for u, _ in top_p] + ids)
    me = um.get(ids[0]) or {"user_id": ids[0], "name": ids[0]}
    peak_h = max(range(24), key=lambda h: hours[h]) if any(hours) else None
    return {
        "period": _pjson(p), "user": me, "generated_at": datetime.now(TZ).isoformat(timespec="seconds"),
        "messages": my, "messages_prev": prev, "messages_change": _pct(my, prev or 0) if prev is not None else None,
        "rank": rank, "ranked_of": len(per_user), "share": round(my * 100.0 / sum(per_user.values()), 2) if per_user and my else 0,
        "active_days": len(days), "active_days_prev": prev_days, "longest_streak": best,
        "first_day": days[0] if days else None, "last_day": days[-1] if days else None,
        "hours": hours, "weekdays": wd, "peak_hour": peak_h,
        "top_threads": [{"thread_id": t, "name": names.get(t) or "未命名帖子", "messages": c} for t, c in thr.most_common(6)],
        "hot_messages": [{"message_id": r[0], "thread_id": r[1], "thread_name": hnames.get(r[1]), "content": (r[2] or "")[:280], "timestamp": r[3],
                          "reactions": r[4], "emojis": emo.get(r[0], [])[:4]} for r in hot],
        "reactions_received": sum(recv.values()), "reactions_given": sum(given.values()),
        "top_received": [{"emoji_name": k, "emoji_url": recv_url.get(k), "count": v} for k, v in recv.most_common(8)],
        "top_given": [{"emoji_name": k, "emoji_url": given_url.get(k), "count": v} for k, v in given.most_common(8)],
        "partners": [_u(um, u, score=c) for u, c in top_p],
        "words": _words(texts, 30),
        "memes": _memes_in(ic, p, texts=texts, n=8),
    }


# ------------------------------------------------------------------ cache + background build
def _cache_dir(ctx, sid):
    d = os.path.join(os.path.dirname(ctx.server_db_path(sid)), "recap")
    os.makedirs(d, exist_ok=True)
    return d


def _key(sid, scope, p):
    return f"{sid}:{scope}:{p['kind']}:{p['value']}"


def _fresh(p, at):
    return (not p["open"]) or (time.time() - at < TTL_OPEN)


def get(ctx, sid, scope, p, ids=None):
    """-> ("ready", data) | ("building", None) | ("error", msg). Starts a build when needed."""
    key = _key(sid, scope, p)
    hit = _mem.get(key)
    if hit and _fresh(p, hit[0]):
        return "ready", blacklist.scrub(hit[1])
    path = os.path.join(_cache_dir(ctx, sid), re.sub(r"[^0-9A-Za-z_-]", "_", key) + ".json")
    if os.path.exists(path):
        at = os.path.getmtime(path)
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            _remember(key, at, data)
            if _fresh(p, at):
                return "ready", blacklist.scrub(data)
            stale = data
        except (OSError, ValueError):
            stale = None
    else:
        stale = None
    with _lock:
        job = _jobs.get(key)
        if job and job["state"] == "building":
            return ("ready", blacklist.scrub(stale)) if stale else ("building", None)
        if job and job["state"] == "error" and time.time() - job["at"] < 60:
            return "error", job["error"]
        _jobs[key] = {"state": "building", "at": time.time()}
    threading.Thread(target=_build, args=(ctx, sid, scope, p, ids, key, path), daemon=True, name="recap-build").start()
    return ("ready", blacklist.scrub(stale)) if stale else ("building", None)


def _remember(key, at, data):
    if len(_mem) > _MEM_MAX:
        _mem.clear()
    _mem[key] = (at, data)


def open_conns(ctx, sid):
    from dixo import summary as _summary
    from dixo import insights as _ins
    main_path = ctx.server_db_path(sid)
    conn = ctx.connect(main_path)
    spath = _summary.summary_path(main_path)
    try:
        conn.execute("ATTACH DATABASE ? AS s", (spath,))
    except Exception:
        pass
    sc = ctx.summary_engine.summary_conn(sid)
    ic = _ins.connect_ro(_ins.path_for(main_path))
    return conn, sc, ic


def _build(ctx, sid, scope, p, ids, key, path):
    t0 = time.time()
    conn = sc = ic = None
    try:
        conn, sc, ic = open_conns(ctx, sid)
        data = server_recap(conn, sc, ic, p) if scope == "server" else personal_recap(conn, sc, ic, p, ids)
        data["build_seconds"] = round(time.time() - t0, 1)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        os.replace(tmp, path)
        _remember(key, time.time(), data)
        with _lock:
            _jobs.pop(key, None)
    except Exception as e:  # noqa: BLE001
        import logging
        logging.getLogger("app").exception("recap build failed key=%s", key)
        with _lock:
            _jobs[key] = {"state": "error", "at": time.time(), "error": str(e)[:300]}
    finally:
        for c in (conn, sc, ic):
            try:
                c and c.close()
            except Exception:
                pass


def invalidate():
    _mem.clear()


def warm(ctx, sid):
    """Precompute the common server periods (current year/month/week + all)."""
    for kind in ("year", "month", "week", "all"):
        try:
            get(ctx, sid, "server", parse_period(kind))
        except Exception:
            pass


# ------------------------------------------------------------------ ego network (profile bubble)
def ego_network(ic, conn, ids, limit=14):
    ids = {str(i) for i in ids}
    if ic is None:
        return None
    g = ic.execute("SELECT value FROM meta WHERE key='network'").fetchone()
    if not g:
        return None
    try:
        net = json.loads(g[0])
    except ValueError:
        return None
    w = collections.Counter()
    for e in net.get("edges") or []:
        a, b, k = str(e[0]), str(e[1]), e[2] if len(e) > 2 else 1
        if a in ids and b not in ids: w[b] += k
        elif b in ids and a not in ids: w[a] += k
    blocked = blacklist.ids()
    top = [(u, k) for u, k in w.most_common(limit * 2) if u not in blocked][:limit]
    if not top:
        return {"center": None, "partners": []}
    um = _users(conn, [u for u, _ in top] + list(ids))
    center = next((um[i] for i in ids if i in um), {"user_id": next(iter(ids)), "name": ""})
    return {"center": center, "partners": [_u(um, u, weight=k) for u, k in top if u in um]}


# ------------------------------------------------------------------ blueprint
def create_recap(ctx):
    from flask import Blueprint, jsonify, render_template, request, session

    bp = Blueprint("recap", __name__)

    def _period():
        return parse_period(request.args.get("p") or "year", request.args.get("v"))

    def _available(sid):
        sc = ctx.summary_engine.summary_conn(sid)
        try:
            return periods_available(sc)
        finally:
            sc.close()

    @bp.route("/recap")
    @ctx.server_required
    def recap_page():
        sid = ctx.current_server_id()
        return render_template("recap.html", scope="server", avail=_available(sid), current_user=session.get("user"))

    @bp.route("/api/v1/recap/server")
    @ctx.server_required
    def api_server():
        sid = ctx.current_server_id(); p = _period()
        state, data = get(ctx, sid, "server", p)
        return jsonify(ok=state != "error", state=state, data=data, error=data if state == "error" else None)

    @bp.route("/api/v1/recap/me")
    @ctx.server_required
    def api_me():
        sid = ctx.current_server_id(); p = _period()
        from dixo import core
        u = core.get_display_user() or session["user"]
        uid = str(u["id"])  # 只能看自己的个人总结
        if blacklist.is_blocked(uid):
            return jsonify(ok=False, state="error", error="not found"), 404
        ids = ctx.merged_ids(sid, uid)
        state, data = get(ctx, sid, f"user-{uid}", p, ids)
        return jsonify(ok=state != "error", state=state, data=data, error=data if state == "error" else None)

    @bp.route("/api/v1/ego/<uid>")
    @ctx.server_required
    def api_ego(uid):
        sid = ctx.current_server_id()
        if blacklist.is_blocked(uid):
            return jsonify(ok=True, data=None)
        from dixo import insights as _ins
        conn = ctx.connect(ctx.server_db_path(sid))
        ic = _ins.connect_ro(_ins.path_for(ctx.server_db_path(sid)))
        try:
            return jsonify(ok=True, data=ego_network(ic, conn, ctx.merged_ids(sid, uid)))
        finally:
            conn.close()
            ic and ic.close()

    return bp
