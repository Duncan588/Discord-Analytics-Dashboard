"""JSON APIs.

/api/v1/*     dashboard data for the logged-in session (same auth as pages: server_required).
/api/ai/v1/*  read-only API for AI bots, Bearer-token auth (env AI_API_TOKENS, comma separated;
              empty = API disabled -> 404). Never writes to any database.
"""
import hmac
import os
import threading
import time
from collections import defaultdict, deque

from flask import Blueprint, abort, jsonify, request, send_file

from . import dashboard_data, search as dsearch


def _etag_response(payload, etag):
    if etag and request.headers.get("If-None-Match") == etag:
        resp = jsonify(ok=True); resp.status_code = 304; resp.set_data(b"")
    else:
        resp = jsonify(payload)
    if etag:
        resp.headers["ETag"] = etag
        resp.headers["Cache-Control"] = "private, max-age=0, must-revalidate"
    return resp


def create_dashboard_api(ctx):
    bp = Blueprint("api_v1", __name__, url_prefix="/api/v1")

    def _with(fn):
        sid = ctx.current_server_id()
        main = ctx.get_db()
        summ = ctx.summary_engine.summary_conn(sid)
        try:
            st = ctx.summary_engine.freshness(main, summ)
            if st["stale"]:
                ctx.summary_engine.schedule(sid)
            etag = f'W/"{sid}-{st["msg_wm"]}-{st["react_wm"]}-{request.full_path}"'
            if request.headers.get("If-None-Match") == etag:
                return _etag_response(None, etag)
            return _etag_response({"ok": True, "state": st, "data": fn(main, summ)}, etag)
        finally:
            summ.close()

    @bp.route("/summary")
    @ctx.server_required
    def summary_kpis():
        return _with(lambda m, s: dashboard_data.kpis(m, s))

    @bp.route("/timeseries")
    @ctx.server_required
    def timeseries():
        return _with(lambda m, s: {"daily": dashboard_data.daily_series(s), "hourly": dashboard_data.hourly_series(s)})

    @bp.route("/heatmap")
    @ctx.server_required
    def heatmap():
        days = request.args.get("days", type=int)
        return _with(lambda m, s: dashboard_data.heatmap(s, days))

    @bp.route("/wordcloud")
    @ctx.server_required
    def wordcloud():
        limit = max(1, min(300, request.args.get("limit", 60, type=int)))
        return _with(lambda m, s: dashboard_data.word_cloud(s, limit))

    @bp.route("/leaderboard")
    @ctx.server_required
    def leaderboard():
        page = max(1, request.args.get("page", 1, type=int))
        size = max(1, min(100, request.args.get("size", 50, type=int)))
        return _with(lambda m, s: dashboard_data.leaderboard(m, s, size, (page - 1) * size))

    @bp.route("/top-threads")
    @ctx.server_required
    def top_threads():
        return _with(lambda m, s: dashboard_data.top_threads(m, s, min(50, request.args.get("limit", 10, type=int))))

    @bp.route("/hot-messages")
    @ctx.server_required
    def hot_messages():
        return _with(lambda m, s: dashboard_data.hot_messages(m, s, min(50, request.args.get("limit", 10, type=int))))

    @bp.route("/users/<user_id>/profile")
    @ctx.server_required
    def profile(user_id):
        sid = ctx.current_server_id()
        ids = ctx.merged_ids(sid, user_id)
        return jsonify(ok=True, data=ctx.profile_engine.load(sid, ids, ctx.get_db()))

    @bp.route("/analytics")
    @ctx.server_required
    def analytics():
        since = request.args.get("since")
        until = request.args.get("until")

        def build(m, s):
            data = dashboard_data.analytics_bundle(s, since, until, cache_key=ctx.current_server_id())
            tags = data.get("activity_tags") or {}
            samples = tags.get("samples") or {}
            ids = []
            for rows in samples.values():
                ids.extend(r["user_id"] for r in rows)
            if ids:
                ph = ",".join("?" for _ in ids)
                users = {str(u["user_id"]): dict(u) for u in m.execute(
                    f"SELECT user_id,username,nickname,avatar_url FROM users WHERE user_id IN ({ph})", ids)}
                for rows in samples.values():
                    for r in rows:
                        u = users.get(str(r["user_id"])) or {}
                        r["username"] = u.get("username")
                        r["nickname"] = u.get("nickname")
                        r["avatar_url"] = u.get("avatar_url")
            return data

        return _with(build)

    @bp.route("/status")
    @ctx.server_required
    def status():
        sid = ctx.current_server_id()
        summ = ctx.summary_engine.summary_conn(sid)
        try:
            st = ctx.summary_engine.freshness(ctx.get_db(), summ)
        finally:
            summ.close()
        st["refreshing"] = ctx.summary_engine.is_refreshing(sid)
        return jsonify(ok=True, data=st)

    return bp


class _RateLimiter:
    def __init__(self, per_minute=120):
        self.per_minute = per_minute
        self.hits = defaultdict(deque)
        self.lock = threading.Lock()

    def allow(self, key):
        now = time.monotonic()
        with self.lock:
            q = self.hits[key]
            while q and now - q[0] > 60:
                q.popleft()
            if len(q) >= self.per_minute:
                return False
            q.append(now)
            return True


def create_ai_api(ctx):
    bp = Blueprint("ai_api_v1", __name__, url_prefix="/api/ai/v1")
    limiter = _RateLimiter()

    def tokens():
        return [t.strip() for t in os.getenv("AI_API_TOKENS", "").split(",") if t.strip()]

    @bp.before_request
    def _auth():
        valid = tokens()
        if not valid:
            abort(404)
        auth = request.headers.get("Authorization", "")
        supplied = auth[7:].strip() if auth.lower().startswith("bearer ") else ""
        if not supplied or not any(hmac.compare_digest(supplied, t) for t in valid):
            return jsonify(ok=False, error="unauthorized"), 401
        if not limiter.allow(supplied[:8]):
            return jsonify(ok=False, error="rate limited"), 429
        return None

    def open_guild(gid):
        gid = str(gid)
        if not gid.isdigit() or not ctx.server_db_exists(gid):
            abort(404)
        return ctx.connect(ctx.server_db_path(gid))

    def page_args(default=50, maximum=500):
        return max(1, min(maximum, request.args.get("limit", default, type=int))), max(0, request.args.get("offset", 0, type=int))

    @bp.route("/guilds")
    def guilds():
        return jsonify(ok=True, data=ctx.list_servers())

    @bp.route("/guilds/<gid>/members")
    def members(gid):
        limit, offset = page_args(100, 1000)
        main = open_guild(gid); summ = ctx.summary_engine.summary_conn(gid)
        try:
            return jsonify(ok=True, data=dashboard_data.leaderboard(main, summ, limit, offset))
        finally:
            main.close(); summ.close()

    @bp.route("/guilds/<gid>/members/<uid>")
    def member(gid, uid):
        main = open_guild(gid)
        try:
            u = main.execute("SELECT * FROM users WHERE user_id=?", (str(uid),)).fetchone()
            if not u:
                abort(404)
            prof = ctx.profile_engine.load(gid, ctx.merged_ids(gid, uid), main, wait=2.0)
            return jsonify(ok=True, data={"user": dict(u), "profile": prof})
        finally:
            main.close()

    def _messages(main, where, args, limit):
        before = request.args.get("before_rowid", type=int)
        if before:
            where += " AND m.rowid<?"; args = (*args, before)
        rows = main.execute(
            f"SELECT m.rowid cursor,m.message_id,m.thread_id,t.name thread_name,m.author_id,m.content,m.timestamp,m.reply_to_msg_id "
            f"FROM messages m LEFT JOIN threads t ON t.thread_id=m.thread_id WHERE {where} ORDER BY m.rowid DESC LIMIT ?",
            (*args, limit)).fetchall()
        data = [dict(r) for r in rows]
        return {"items": data, "next_before_rowid": data[-1]["cursor"] if len(data) == limit else None}

    @bp.route("/guilds/<gid>/members/<uid>/messages")
    def member_messages(gid, uid):
        limit, _ = page_args(100, 1000)
        ids = ctx.merged_ids(gid, uid)
        main = open_guild(gid)
        try:
            ph = ",".join("?" for _ in ids)
            return jsonify(ok=True, data=_messages(main, f"m.author_id IN ({ph})", tuple(ids), limit))
        finally:
            main.close()

    @bp.route("/guilds/<gid>/threads/<tid>/messages")
    def thread_messages(gid, tid):
        limit, _ = page_args(100, 1000)
        main = open_guild(gid)
        try:
            return jsonify(ok=True, data=_messages(main, "m.thread_id=?", (str(tid),), limit))
        finally:
            main.close()

    @bp.route("/guilds/<gid>/search")
    def search(gid):
        q = (request.args.get("q") or "").strip()
        if not q:
            return jsonify(ok=False, error="q required"), 400
        limit, offset = page_args(50, 200)
        author = request.args.get("author")
        main = open_guild(gid)
        try:
            ids = ctx.merged_ids(gid, author) if author else None
            hits = dsearch.search(main, ctx.server_db_path(gid), q, ids, limit, offset)
            indexed = os.path.exists(dsearch.search_path(ctx.server_db_path(gid)))
            return jsonify(ok=True, indexed=indexed, data=hits)
        finally:
            main.close()

    @bp.route("/guilds/<gid>/members/<uid>/markdown")
    @bp.route("/guilds/<gid>/members/<uid>/markdown/<name>")
    def member_markdown(gid, uid, name="README.md"):
        open_guild(gid).close()
        base = os.path.join(ctx.export_root(gid), str(uid))
        if not (name == "README.md" or (name.startswith("history-") and name.endswith(".md") and "/" not in name and ".." not in name)):
            abort(404)
        path = os.path.join(base, name)
        if not os.path.isfile(path):
            return jsonify(ok=False, error="not exported yet; run python -m dixo.export_md"), 404
        return send_file(path, mimetype="text/markdown; charset=utf-8")

    return bp
