"""Community insights (topic map, relationship circles, meme glossary, monthly/yearly reports, member dossiers).

Built off-box by tools/insights (box CPU + Colab T4, free models) into data/servers/<gid>/insights.db,
copied to prod by tools/insights/run_monthly.sh (scp). The web app only reads it.
"""
import gzip
import hmac
import json
import os
import sqlite3


SCHEMA = """
CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS topics(id INTEGER PRIMARY KEY, name TEXT, desc TEXT, threads INTEGER, msgs INTEGER, trend REAL, examples TEXT);
CREATE TABLE IF NOT EXISTS topic_month(topic INTEGER, ym TEXT, msgs INTEGER, PRIMARY KEY(topic, ym));
CREATE TABLE IF NOT EXISTS thread_topic(thread_id TEXT PRIMARY KEY, topic INTEGER);
CREATE TABLE IF NOT EXISTS circles(id INTEGER PRIMARY KEY, name TEXT, desc TEXT, size INTEGER, core TEXT, threads TEXT, words TEXT);
CREATE TABLE IF NOT EXISTS member_circle(user_id TEXT PRIMARY KEY, circle INTEGER);
CREATE TABLE IF NOT EXISTS memes(term TEXT PRIMARY KEY, kind TEXT, meaning TEXT, origin TEXT, usage TEXT, total INTEGER, authors INTEGER,
  first_day TEXT, first_author TEXT, first_author_id TEXT, first_thread TEXT, first_thread_id TEXT, first_text TEXT, peak_ym TEXT, monthly TEXT, examples TEXT);
CREATE TABLE IF NOT EXISTS member_year(user_id TEXT, year INTEGER, data TEXT, PRIMARY KEY(user_id, year));
CREATE TABLE IF NOT EXISTS server_month(ym TEXT PRIMARY KEY, data TEXT, narrative TEXT);
CREATE TABLE IF NOT EXISTS dossiers(user_id TEXT PRIMARY KEY, name TEXT, md TEXT);
CREATE TABLE IF NOT EXISTS docs(key TEXT PRIMARY KEY, md TEXT);
CREATE TABLE IF NOT EXISTS corpus(id TEXT PRIMARY KEY, text TEXT, reacts INTEGER, thread TEXT, day TEXT);
"""


def path_for(main_db_path):
    return os.path.join(os.path.dirname(main_db_path), "insights.db")


def connect_ro(path):
    if not os.path.exists(path):
        return None
    c = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5, check_same_thread=False)
    c.row_factory = sqlite3.Row
    return c


def _rows(c, sql, args=()):
    return [dict(r) for r in c.execute(sql, args)]


def _j(v):
    try:
        # NaN/Infinity (e.g. a month with no scored messages) would make jsonify emit invalid JSON and blank the page
        return json.loads(v, parse_constant=lambda _: None) if v else None
    except ValueError:
        return None


# ---------------------------------------------------------------- queries (shared by web + MCP)
def topics(c, months=0):
    ts = _rows(c, "SELECT id,name,desc,threads,msgs,trend,examples FROM topics ORDER BY msgs DESC")
    for t in ts:
        t["examples"] = _j(t["examples"])
    q = "SELECT topic,ym,msgs FROM topic_month"
    yms = [r[0] for r in c.execute("SELECT DISTINCT ym FROM topic_month ORDER BY ym")]
    if months:
        yms = yms[-months:]
    series = {}
    for r in c.execute(q + (" WHERE ym>=?" if yms else ""), (yms[0],) if yms else ()):
        series.setdefault(r["topic"], {})[r["ym"]] = r["msgs"]
    return {"months": yms, "topics": ts, "series": series}


def network(c):
    g = c.execute("SELECT value FROM meta WHERE key='network'").fetchone()
    cs = _rows(c, "SELECT id,name,desc,size,core,threads,words FROM circles ORDER BY id")
    for x in cs:
        for k in ("core", "threads", "words"):
            x[k] = _j(x[k])
    return {"graph": _j(g[0]) if g else None, "circles": cs}


def memes(c, q="", kind="", limit=300):
    sql, args = "SELECT * FROM memes", []
    w = []
    if q:
        w.append("(term LIKE ? OR meaning LIKE ?)"); args += [f"%{q}%", f"%{q}%"]
    if kind:
        w.append("kind=?"); args.append(kind)
    if w:
        sql += " WHERE " + " AND ".join(w)
    out = _rows(c, sql + " ORDER BY authors DESC LIMIT ?", (*args, limit))
    for x in out:
        x["monthly"] = _j(x["monthly"]); x["examples"] = _j(x["examples"])
    return out


def months(c, ym=None):
    if ym:
        r = c.execute("SELECT ym,data,narrative FROM server_month WHERE ym=?", (ym,)).fetchone()
        return dict(r, data=_j(r["data"])) if r else None
    return [dict(ym=r[0], data=_j(r[1]), narrative=r[2]) for r in c.execute("SELECT ym,data,narrative FROM server_month ORDER BY ym DESC")]


def member(c, ids, year=None):
    ph = ",".join("?" for _ in ids)
    yrs = [dict(year=r[1], **_j(r[2])) for r in c.execute(
        f"SELECT user_id,year,data FROM member_year WHERE user_id IN ({ph})" + (" AND year=?" if year else "") + " ORDER BY year DESC",
        (*ids, year) if year else tuple(ids))]
    d = c.execute(f"SELECT name,md FROM dossiers WHERE user_id IN ({ph})", ids).fetchone()
    ci = c.execute(f"SELECT c.id,c.name,c.desc FROM member_circle m JOIN circles c ON c.id=m.circle WHERE m.user_id IN ({ph})", ids).fetchone()
    return {"years": yrs, "dossier": dict(d) if d else None, "circle": dict(ci) if ci else None}


def doc(c, key):
    r = c.execute("SELECT md FROM docs WHERE key=?", (key,)).fetchone()
    return r[0] if r else None


def status(c):
    if c is None:
        return {"ready": False}
    return {"ready": True, **{r[0]: r[1] for r in c.execute("SELECT key,value FROM meta WHERE key<>'network'")}}


# ---------------------------------------------------------------- Flask
def create_insights(ctx):
    from flask import Blueprint, abort, jsonify, render_template, request, session

    bp = Blueprint("insights", __name__)

    def conn(sid=None):
        sid = sid or ctx.current_server_id()
        return connect_ro(path_for(ctx.server_db_path(sid)))

    def wrap(fn):
        c = conn()
        if c is None:
            return jsonify(ok=True, ready=False, data=None)
        try:
            return jsonify(ok=True, ready=True, data=fn(c))
        finally:
            c.close()

    @bp.route("/insights")
    @ctx.server_required
    def insights_page():
        c = conn()
        st = status(c)
        if c:
            c.close()
        return render_template("insights.html", st=st, current_user=session.get("user"))

    @bp.route("/api/v1/insights/topics")
    @ctx.server_required
    def api_topics():
        return wrap(lambda c: topics(c, request.args.get("months", 0, type=int)))

    @bp.route("/api/v1/insights/network")
    @ctx.server_required
    def api_network():
        return wrap(network)

    @bp.route("/api/v1/insights/memes")
    @ctx.server_required
    def api_memes():
        return wrap(lambda c: memes(c, (request.args.get("q") or "")[:40], request.args.get("kind") or ""))

    @bp.route("/api/v1/insights/months")
    @ctx.server_required
    def api_months():
        return wrap(lambda c: {"months": months(c), "overview": doc(c, "overview")})

    @bp.route("/api/v1/insights/member/<uid>")
    @ctx.server_required
    def api_member(uid):
        if not uid.isdigit():
            abort(404)
        sid = ctx.current_server_id()
        return wrap(lambda c: member(c, ctx.merged_ids(sid, uid)))

    # ---- machine API for the off-box monthly pipeline (Bearer INSIGHTS_API_TOKENS; empty = 404)
    @bp.before_request
    def _machine_auth():
        if not request.path.startswith("/api/insights/v1/"):
            return None
        valid = [t.strip() for t in os.getenv("INSIGHTS_API_TOKENS", "").split(",") if t.strip()]
        if not valid:
            abort(404)
        auth = request.headers.get("Authorization", "")
        sup = auth[7:].strip() if auth.lower().startswith("bearer ") else ""
        if not sup or not any(hmac.compare_digest(sup, t) for t in valid):
            return jsonify(ok=False, error="unauthorized"), 401
        return None

    def _gid(gid):
        if not str(gid).isdigit() or not ctx.server_db_exists(gid):
            abort(404)
        return ctx.server_db_path(gid)

    @bp.route("/api/insights/v1/guilds/<gid>/export/<kind>")
    def export(gid, kind):
        """Incremental raw export for the pipeline. kind: messages (?after_id=&limit=) | users | threads |
        reply_edges (?after_id=) | mention_edges (?after_id=) | reactions (?after_id=). JSON rows, read-only."""
        main = ctx.connect(_gid(gid))
        after = int(request.args.get("after_id", "0") or 0)
        lim = max(1, min(200000, request.args.get("limit", 100000, type=int)))
        q = {
            "messages": ("SELECT message_id,author_id,thread_id,timestamp,content,reply_to_msg_id FROM messages "
                         "WHERE CAST(message_id AS INTEGER)>? ORDER BY CAST(message_id AS INTEGER) LIMIT ?", (after, lim)),
            "users": ("SELECT user_id,username,nickname,COALESCE(is_bot,0) is_bot,avatar_url FROM users", ()),
            "threads": ("SELECT thread_id,name,category_id FROM threads", ()),
            "reactions": ("SELECT message_id,COUNT(*) n FROM reactions WHERE CAST(message_id AS INTEGER)>? GROUP BY 1", (after,)),
            "mentions": ("SELECT message_id,author_id src,mentioned_user_id dst FROM mentions WHERE CAST(message_id AS INTEGER)>?", (after,)),
        }.get(kind)
        if not q:
            abort(404)
        try:
            rows = [list(r) for r in main.execute(*q)]
        finally:
            main.close()
        body = gzip.compress(json.dumps(rows, ensure_ascii=False).encode())
        return body, 200, {"Content-Type": "application/json", "Content-Encoding": "gzip"}

    return bp
