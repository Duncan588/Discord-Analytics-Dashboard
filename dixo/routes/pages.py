"""User-facing pages: welcome, legal, servers, overview, search, profile, report, upload (moved verbatim from app.py; endpoint = "pages.<function name>")."""
from flask import Blueprint

from dixo import core as _core

# Route bodies were written against app.py's module globals; expose the same names here.
globals().update({k: v for k, v in vars(_core).items() if not k.startswith("__")})

bp = Blueprint("pages", __name__)



def _member_count_for(sid):
    """Live Discord roster size for the homepage KPI.

    Prefer servers.member_count (whitelist bot sync/prune). If the whitelist bot is
    not in the guild, fall back to Discord GET /guilds/{id}?with_counts=true using a
    download-bot / default downloader token, and cache onto servers.member_count.
    Analytics `users` table is NOT used (includes leavers / historical authors).
    """
    sid = str(sid)
    portal = get_portal_db()
    try:
        row = portal.execute("SELECT member_count, updated_at FROM servers WHERE server_id=?", (sid,)).fetchone()
    except Exception:
        row = None
    cached = None
    if row is not None:
        try:
            cached = int(row[0]) if row[0] is not None else None
        except (TypeError, ValueError):
            cached = None
    # Refresh from Discord at most every 6 hours when we only have API approx counts.
    need_fetch = cached is None or cached <= 0
    if not need_fetch:
        return cached
    token = None
    token_type = "bot"
    try:
        bots = portal.execute(
            """SELECT b.token, b.token_type FROM download_bots b
               JOIN download_config_bots cb ON cb.bot_id=b.id
               JOIN download_configs c ON c.id=cb.config_id
               WHERE c.guild_id=? AND c.enabled=1 ORDER BY b.id LIMIT 1""",
            (sid,),
        ).fetchone()
        if bots:
            token, token_type = bots[0], (bots[1] or "bot")
    except Exception:
        pass
    if not token:
        try:
            from dixo.core import default_downloader_token
            token = default_downloader_token()
            token_type = "bot"
        except Exception:
            token = None
    if not token:
        return cached
    try:
        import urllib.request, json as _json
        req = urllib.request.Request(
            f"https://discord.com/api/v10/guilds/{sid}?with_counts=true",
            headers={
                "Authorization": (token if str(token_type).lower() == "user" else f"Bot {token}"),
                "User-Agent": "DixoMemberCount/1.0",
            },
        )
        with urllib.request.urlopen(req, timeout=8) as resp:
            data = _json.loads(resp.read().decode("utf-8"))
        n = data.get("approximate_member_count") or data.get("member_count")
        if n is None:
            return cached
        n = int(n)
        try:
            portal.execute("UPDATE servers SET member_count=?, updated_at=? WHERE server_id=?", (n, utc_now_iso(), sid))
            portal.commit()
        except Exception:
            pass
        return n
    except Exception as exc:
        try:
            app.logger.warning("member_count fetch failed guild=%s err=%s", sid, exc)
        except Exception:
            pass
        return cached

@bp.route("/me")
@login_required
def my_profile():
    servers = get_servers_for_user(session["user"]["id"])
    if servers:
        display_user = get_display_user() or session["user"]
    else:
        return render_template("welcome.html", can_upload=whitelist_allowed(session["user"]["id"]), current_user=session["user"], no_data=True)
    if len(servers) == 1:
        session["server_id"] = servers[0]["server_id"]
        return redirect(url_for("pages.user_profile", user_id=display_user["id"]))
    if len(servers) > 1:
        # 管理员可以访问所有服务器；个人主页仍需绑定一个分析库。
        # 优先沿用当前选择，避免从 Admin/服务器列表点击“个人主页”时循环回 /servers。
        allowed = {str(server["server_id"]) for server in servers}
        sid = current_server_id()
        if sid not in allowed:
            sid = str(servers[0]["server_id"])
            session["server_id"] = sid
        return redirect(url_for("pages.user_profile", user_id=display_user["id"]))
    return render_template("welcome.html", can_upload=whitelist_allowed(session["user"]["id"]), current_user=session["user"], no_data=True)


@bp.route("/welcome")
def welcome():
    current_user = session.get("user")
    if not current_user and is_discord_activity_request():
        return redirect(url_for("auth.login", **request.args.to_dict()))
    if current_user:
        servers = get_servers_for_user(current_user["id"])
        if len(servers) == 1:
            return redirect(url_for("pages.index"))
        if len(servers) > 1:
            return redirect(url_for("pages.servers"))
        can_upload = whitelist_allowed(current_user["id"])
    else:
        can_upload = False
    return render_template("welcome.html", can_upload=can_upload, current_user=current_user, no_data=bool(current_user and not servers))


@bp.route("/privacy")
def privacy():
    return render_template("privacy.html", current_user=session.get("user"), discord_client_id=DISCORD_CLIENT_ID)


@bp.route("/terms")
def terms():
    return render_template("terms.html", current_user=session.get("user"), discord_client_id=DISCORD_CLIENT_ID)


@bp.route("/upload-json", methods=["POST"])
@login_required
def upload_json():
    uid = str(session["user"]["id"])
    if not whitelist_allowed(uid):
        return "你没有 JSON 导入权限", 403
    file = request.files.get("json_file")
    if not file or not file.filename.lower().endswith(".json"):
        return "请选择 JSON 文件", 400
    temp = os.path.join(BASE_DIR, "data", "uploads", f"{uid}_{os.urandom(8).hex()}.json")
    try:
        file.save(temp)
        meta = inspect_json(temp)
        sid = str(meta["server_id"])
        # server_id 来自上传文件内容，会被拼进数据目录路径，必须限制为纯数字 ID。
        if not sid.isdigit():
            return "JSON 中的 server_id 无效", 400
        target = server_db_path(sid)
        # 权限 2 用户只能新建/管理自己拥有访问权的服务器，且下载服务器数量受 quota 限制。
        existing = get_portal_db().execute("SELECT 1 FROM servers WHERE server_id=?", (sid,)).fetchone()
        if not existing and admin_level(uid) == 2:
            used = get_portal_db().execute("SELECT count(*) FROM user_server_access WHERE user_id=?", (uid,)).fetchone()[0]
            if used >= get_download_quota(uid):
                return "已达到服务器配额，请联系权限1管理员增加配额", 403
        import_json_to_db(temp, target, server_id=sid)
        register_server(sid, meta.get("server_name") or f"Discord Server {sid}", meta.get("icon_url"), owner_user_id=uid, db_path=target)
        get_portal_db().execute("INSERT OR IGNORE INTO user_server_access(user_id,server_id,granted_by,created_at) VALUES(?,?,?,?)", (uid, sid, uid, utc_now_iso()))
        get_portal_db().commit()
        sync_server_users_to_portal(sid)
        session["server_id"] = sid
        return redirect(url_for("pages.index"))
    except Exception as e:
        app.logger.exception("JSON import failed")
        return render_template("welcome.html", can_upload=True, current_user=session["user"], error=f"导入失败: {e}"), 400
    finally:
        if os.path.exists(temp):
            try:
                os.remove(temp)
            except OSError as exc:
                app.logger.warning("清理 JSON 临时文件失败 path=%s error=%s", temp, exc)


@bp.route("/servers")
@login_required
def servers():
    rows = get_servers_for_user(session["user"]["id"])
    return render_template("servers.html", servers=rows, current_user=session["user"])


@bp.route("/server/<server_id>")
@login_required
def select_server(server_id):
    allowed = {str(x["server_id"]) for x in get_servers_for_user(session["user"]["id"])}
    if str(server_id) not in allowed:
        return "403 Access Denied", 403
    session["server_id"] = str(server_id)
    return redirect(url_for("pages.index"))


@bp.route("/")
def index():
    if "user" not in session:
        if is_discord_activity_request():
            return redirect(url_for("auth.login", **request.args.to_dict()))
        return redirect(url_for("pages.welcome"))
    display_user = get_display_user() or session["user"]
    servers = get_servers_for_user(session["user"]["id"])
    if not current_server_id():
        if len(servers) == 1:
            session["server_id"] = servers[0]["server_id"]
        elif len(servers) > 1:
            return redirect(url_for("pages.servers"))
        else:
            return redirect(url_for("pages.welcome"))
    elif str(current_server_id()) not in {str(x["server_id"]) for x in servers}:
        session.pop("server_id", None)
        if len(servers) == 1:
            session["server_id"] = servers[0]["server_id"]
        elif len(servers) > 1:
            return redirect(url_for("pages.servers"))
        else:
            return redirect(url_for("pages.welcome"))
    sid = current_server_id()
    conn = get_db()
    u = display_user
    _safe_visitor_write(dixo_visitors.record_site_visit, u, utc_now_iso())
    data = summary_engine.homepage(sid, conn)
    mc = _member_count_for(sid)
    if mc is not None:
        data["users_seen"] = data.get("total_users")
        data["total_users"] = mc
    visitors = dixo_visitors.recent_site_visitors(get_visitors_db())
    server = get_portal_db().execute("SELECT * FROM servers WHERE server_id=?", (sid,)).fetchone()
    full_leaderboard = data.pop("full_leaderboard", [])
    return render_template("index.html", server=server, server_id=sid, current_user=u, site_visitors=visitors, full_leaderboard=full_leaderboard, **data)


@bp.route("/api/leaderboard")
@server_required
def api_leaderboard():
    page = request_page()
    from dixo import dashboard_data
    summ = summary_engine.summary_conn(current_server_id())
    try:
        users = dashboard_data.leaderboard(get_db(), summ, 50, (page - 1) * 50)
    finally:
        summ.close()
    return jsonify(users)


@bp.route("/search")
@server_required
def search():
    query = request.args.get("q", "").strip()
    conn = get_db()
    results = conn.execute("SELECT * FROM users WHERE user_id=? OR username LIKE ? OR nickname LIKE ? LIMIT 20", (query, f"%{query}%", f"%{query}%")).fetchall()
    data = summary_engine.homepage(current_server_id(), conn)
    mc = _member_count_for(current_server_id())
    if mc is not None:
        data["users_seen"] = data.get("total_users")
        data["total_users"] = mc
    data.pop("full_leaderboard", None)
    return render_template("index.html", server_id=current_server_id(), current_user=session["user"], search_results=results, query=query, site_visitors=[], **data)


@bp.route("/user/<user_id>")
@server_required
def user_profile(user_id):
    sid = current_server_id()
    conn = get_db()
    user_id = str(user_id)
    merge = conn.execute("SELECT parent_id FROM user_merges WHERE target_id=?", (user_id,)).fetchone()
    if merge:
        return redirect(url_for("pages.user_profile", user_id=merge["parent_id"]))
    user = conn.execute("SELECT * FROM users WHERE user_id=?", (user_id,)).fetchone()
    if not user:
        return "User Not Found", 404
    visitor = session["user"]
    if visitor["id"] != str(user_id):
        _safe_visitor_write(dixo_visitors.record_profile_view, user_id, visitor, utc_now_iso())
    view_count, recent_viewers = dixo_visitors.profile_view_stats(get_visitors_db(), user_id)
    profile = profile_engine.load(sid, data_engine.get_merged_ids(sid, user_id), conn, wait=0.3)
    merged_ids = profile["merged_ids"]
    ph = ",".join("?" for _ in merged_ids)
    # Time order can use the author/timestamp index and keeps the first paint
    # responsive. Hot order remains available on demand for users who need it.
    sort_by = request.args.get("sort", "time")
    active_tab = request.args.get("tab", "threads") if request.args.get("tab") in ("threads", "messages") else "threads"
    page = request_page()
    offset = (page - 1) * ITEMS_PER_PAGE
    order = "ORDER BY total_reactions DESC,m.timestamp DESC" if sort_by == "hot" else "ORDER BY m.timestamp DESC"
    messages = []
    if active_tab == "messages":
        if sort_by == "hot":
            message_query = (
                f"SELECT m.*,t.name thread_name,(SELECT count(*) FROM reactions "
                f"WHERE message_id=m.message_id) total_reactions FROM messages m "
                f"JOIN threads t ON m.thread_id=t.thread_id WHERE m.author_id IN ({ph}) "
                f"ORDER BY total_reactions DESC,m.timestamp DESC LIMIT ? OFFSET ?"
            )
        else:
            # Correlated reaction counts are evaluated only for the limited
            # newest rows. A LEFT JOIN + GROUP BY forces SQLite to aggregate
            # every message before it can apply this timestamp order.
            message_query = (
                f"SELECT m.*,t.name thread_name,(SELECT count(*) FROM reactions "
                f"WHERE message_id=m.message_id) total_reactions FROM messages m "
                f"JOIN threads t ON m.thread_id=t.thread_id WHERE m.author_id IN ({ph}) "
                f"ORDER BY m.timestamp DESC LIMIT ? OFFSET ?"
            )
        messages = process_messages(conn, conn.execute(
            message_query, (*merged_ids, ITEMS_PER_PAGE, offset)
        ).fetchall())
    # Keep the thread lookup bounded to the user's own candidate messages;
    # reaction icons are fetched in one batch below instead of one query per
    # returned thread.
    my_threads = []
    if active_tab == "threads":
        # 楼主帖列表来自 summary.thread_stats（op_author 索引），不再对每个帖子做相关子查询。
        summ = summary_engine.summary_conn(sid)
        try:
            order_sql = "msg_count DESC" if sort_by == "hot" else "first_ts DESC"
            stat_rows = summ.execute(
                f"SELECT thread_id,msg_count,op_msg_id,first_ts FROM thread_stats WHERE op_author IN ({ph}) "
                f"ORDER BY {order_sql} LIMIT ? OFFSET ?", (*merged_ids, ITEMS_PER_PAGE, offset)).fetchall()
        finally:
            summ.close()
        tids = [r["thread_id"] for r in stat_rows]
        names, firsts = {}, {}
        if tids:
            tph = ",".join("?" for _ in tids)
            names = {str(r[0]): r[1] for r in conn.execute(f"SELECT thread_id,name FROM threads WHERE thread_id IN ({tph})", tids)}
            oids = [r["op_msg_id"] for r in stat_rows if r["op_msg_id"]]
            if oids:
                oph = ",".join("?" for _ in oids)
                firsts = {str(r[0]): r[1] for r in conn.execute(f"SELECT message_id,content FROM messages WHERE message_id IN ({oph})", oids)}
        thread_rows = [{"thread_id": r["thread_id"], "name": names.get(str(r["thread_id"]), ""), "created_at": r["first_ts"],
                        "reply_count": r["msg_count"], "first_content": firsts.get(str(r["op_msg_id"]), ""),
                        "op_msg_id": r["op_msg_id"]} for r in stat_rows]
        op_message_ids = [row["op_msg_id"] for row in thread_rows]
        emoji_map = {}
        if op_message_ids:
            message_ph = ",".join("?" for _ in op_message_ids)
            for emoji in conn.execute(
                f"SELECT message_id,emoji_url,emoji_name,count(*) c FROM reactions WHERE message_id IN ({message_ph}) "
                f"GROUP BY message_id,emoji_name ORDER BY message_id,c DESC", op_message_ids
            ):
                emoji_map.setdefault(emoji["message_id"], emoji)
        for row in thread_rows:
            d = dict(row)
            d["op_user"] = dict(user)
            emoji = emoji_map.get(row["op_msg_id"])
            d["top_emoji_url"] = emoji["emoji_url"] if emoji else None
            d["top_emoji_name"] = emoji["emoji_name"] if emoji else None
            d["top_emoji_count"] = emoji["c"] if emoji else 0
            my_threads.append(d)
    msg_count = profile["msg_count"]
    thread_count = profile.get("threads_started", profile["thread_count"])

    # ---- 机器人用户功能（仅本人主页显示）----
    is_self = session["user"]["id"] == user_id
    from dixo import social as dixo_social
    dixo_social.ensure_tables(get_portal_db())
    i_blocked = (not is_self) and dixo_social.is_blocked(get_portal_db(), session["user"]["id"], user_id)
    my_blocks = []
    my_favorites = []
    blk_page = fav_page = 1
    total_blk_pages = total_fav_pages = 1
    if is_self:
        portal = get_portal_db()
        uid_str = str(user_id)
        # 拉黑列表：每页 10 条，按拦截次数排序
        blk_total = portal.execute("SELECT COUNT(*) FROM user_blocks WHERE blocker_id=?", (user_id,)).fetchone()[0]
        total_blk_pages = max(1, math.ceil(blk_total / 10))
        try:
            blk_page = max(1, min(total_blk_pages, int(request.args.get("blk_page", 1))))
        except (TypeError, ValueError):
            blk_page = 1
        for r in portal.execute(
            """
            SELECT b.blocked_id, b.created_at,
                   COALESCE(SUM(c.count),0) intercepted,
                   MAX(c.last_blocked_at) last_blocked_at
            FROM user_blocks b
            LEFT JOIN block_interception_counts c
                   ON (c.sender_id=b.blocker_id AND c.target_id=b.blocked_id)
                   OR (c.sender_id=b.blocked_id AND c.target_id=b.blocker_id)
            WHERE b.blocker_id=?
            GROUP BY b.blocked_id, b.created_at
            ORDER BY intercepted DESC, b.created_at DESC
            LIMIT 10 OFFSET ?
            """,
            (user_id, (blk_page - 1) * 10),
        ).fetchall():
            bid = str(r["blocked_id"])
            pu = portal.execute("SELECT username,nickname FROM portal_users WHERE user_id=?", (bid,)).fetchone()
            gu = conn.execute("SELECT nickname,username,avatar_url FROM users WHERE user_id=?", (bid,)).fetchone()
            if gu:
                name = gu["nickname"] or gu["username"] or bid
                avatar = gu["avatar_url"] or ""
            elif pu:
                name = pu["nickname"] or pu["username"] or bid
                avatar = ""
            else:
                name, avatar = bid, ""
            my_blocks.append({
                "blocked_id": bid, "name": name, "avatar_url": avatar,
                "created_at": (r["created_at"] or "")[:16].replace("T", " "),
                "intercepted": int(r["intercepted"] or 0),
                "last_blocked_at": ((r["last_blocked_at"] or "")[:16].replace("T", " ")) or "—",
            })
        # 收藏列表：每页 5 条，跨服务器，解析帖子标题
        fav_total = portal.execute("SELECT COUNT(*) FROM favorites WHERE user_id=?", (user_id,)).fetchone()[0]
        total_fav_pages = max(1, math.ceil(fav_total / 5))
        try:
            fav_page = max(1, min(total_fav_pages, int(request.args.get("fav_page", 1))))
        except (TypeError, ValueError):
            fav_page = 1
        fav_rows = portal.execute(
            """
            SELECT f.guild_id, f.thread_id, f.created_at,
                   COALESCE(s.name, f.guild_id) guild_name
            FROM favorites f
            LEFT JOIN servers s ON s.server_id=f.guild_id
            WHERE f.user_id=?
            ORDER BY f.created_at DESC
            LIMIT 5 OFFSET ?
            """,
            (user_id, (fav_page - 1) * 5),
        ).fetchall()
        titles_by_guild = {}
        for r in fav_rows:
            gid = str(r["guild_id"])
            if gid in titles_by_guild:
                continue
            db_path = server_db_path(gid)
            if not server_db_exists(gid):
                titles_by_guild[gid] = {}
                continue
            try:
                g = db_connect(server_db_path(gid))
                titles_by_guild[gid] = {
                    str(row["thread_id"]): row["name"]
                    for row in g.execute("SELECT thread_id,name FROM threads")
                }
                g.close()
            except Exception:
                app.logger.exception("读取服务器帖子标题失败 guild=%s", gid)
                titles_by_guild[gid] = {}
        for r in fav_rows:
            gid = str(r["guild_id"])
            tid = str(r["thread_id"])
            my_favorites.append({
                "guild_id": gid,
                "guild_name": r["guild_name"],
                "thread_id": tid,
                "thread_name": titles_by_guild.get(gid, {}).get(tid) or f"帖子 {tid}",
                "created_at": (r["created_at"] or "")[:16].replace("T", " "),
            })

    return render_template(
        "user.html", user=user, messages=messages, my_threads=my_threads,
        view_count=view_count, recent_viewers=recent_viewers, server_id=sid,
        current_sort=sort_by, current_page=page, active_tab=active_tab,
        total_msg_pages=math.ceil(msg_count / ITEMS_PER_PAGE),
        total_thread_pages=math.ceil(thread_count / ITEMS_PER_PAGE),
        current_user=session["user"], msg_count=msg_count,
        reaction_received_count=profile["reaction_received_count"],
        profile_ready=profile.get("complete", False),
        top_emojis_given=profile["top_emojis_given"],
        top_emojis_received=profile["top_emojis_received"],
        interactions_incoming=profile["interactions_incoming"],
        interactions_outgoing=profile["interactions_outgoing"],
        chart_daily=profile["chart_daily"], chart_hourly=profile["chart_hourly"],
        word_cloud_data=profile["word_cloud_data"], profile=profile,
        is_self=is_self, i_blocked=i_blocked, my_blocks=my_blocks, my_favorites=my_favorites,
        blk_page=blk_page, total_blk_pages=total_blk_pages,
        fav_page=fav_page, total_fav_pages=total_fav_pages,
    )


@bp.route("/claim_account", methods=["POST"])
@server_required
def claim_account():
    target_id = request.form.get("target_id", "").strip()
    requester_id = str(session["user"]["id"])
    conn = get_db()
    target = conn.execute("SELECT nickname FROM users WHERE user_id=?", (target_id,)).fetchone()
    if not target or target_id == requester_id:
        return "无效请求", 400
    try:
        conn.execute("INSERT INTO claim_requests_v2(requester_id,target_id,target_name,created_at) VALUES(?,?,?,?)", (requester_id, target_id, target["nickname"], utc_now_iso()))
        conn.commit(); flash("认领申请已提交，请等待管理员审核。")
    except sqlite3.IntegrityError:
        flash("申请已存在")
    return redirect(url_for("pages.user_profile", user_id=target_id))


@bp.route("/report")
@server_required
def report():
    sid = current_server_id(); conn = get_db()
    u = get_display_user() or session["user"]
    uid = str(u["id"])
    cache_key = f"{sid}:{uid}"
    now_ts = time.time()
    cached = _REPORT_CACHE.get(cache_key)
    if cached and now_ts - cached["ts"] < _REPORT_TTL:
        return render_template("report.html", user=cached["db_user"], server_id=sid,
                               word_cloud_data=cached["word_cloud_data"], percentile=95, **cached["result"])
    db_user = conn.execute("SELECT * FROM users WHERE user_id=?", (uid,)).fetchone()
    if not db_user:
        return redirect(url_for("pages.welcome"))
    ids = data_engine.get_merged_ids(sid, uid); ph = ",".join("?" for _ in ids)
    result = {"join_date": "未知", "most_active_day": None, "latest_msg": None, "most_replied_thread": None, "most_active_topic": None, "most_liked_msg": None, "top_friend_incoming": None, "top_friend_outgoing": None}
    row = conn.execute(f"SELECT min(timestamp) joined FROM messages WHERE author_id IN ({ph})", ids).fetchone()
    if row and row["joined"]: result["join_date"] = datetimeformat_filter(row["joined"], "%Y-%m-%d")
    result["most_active_day"] = dict(conn.execute(f"SELECT substr(timestamp,1,10) day,count(*) c FROM messages WHERE author_id IN ({ph}) GROUP BY day ORDER BY c DESC LIMIT 1", ids).fetchone() or {}) or None
    # 小时分布：先按 author 索引取最近 200 条，再在 Python 端按小时过滤，避免全表扫
    late = conn.execute(
        f"SELECT * FROM messages WHERE author_id IN ({ph}) ORDER BY timestamp DESC LIMIT 200", ids
    ).fetchall()
    late = next((dict(m) for m in late if 16 <= int((m["timestamp"] or "T00:00:00")[11:13]) <= 21), None) if late else None
    if late:
        t = conn.execute("SELECT name FROM threads WHERE thread_id=?", (late["thread_id"],)).fetchone(); late["thread_name"] = t["name"] if t else "Unknown"; result["latest_msg"] = late
    row = conn.execute(f"SELECT t.thread_id,t.name,count(*) reply_count FROM threads t JOIN messages m ON t.thread_id=m.thread_id WHERE m.author_id IN ({ph}) GROUP BY t.thread_id ORDER BY reply_count DESC LIMIT 1", ids).fetchone()
    if row: result["most_replied_thread"] = dict(row); result["most_replied_thread"]["op_user"] = dict(db_user)
    row = conn.execute(f"SELECT t.thread_id,t.name,count(*) c FROM messages m JOIN threads t ON m.thread_id=t.thread_id WHERE m.author_id IN ({ph}) GROUP BY t.thread_id ORDER BY c DESC LIMIT 1", ids).fetchone()
    result["most_active_topic"] = dict(row) if row else None
    # 先取作者最近 2000 条消息，再按 message_id 批量查反应，避免 reactions 全表聚合
    top_msg_row = conn.execute(
        f"SELECT m.message_id, m.content, m.timestamp, m.thread_id, t.name AS thread_name "
        f"FROM messages m LEFT JOIN threads t ON t.thread_id=m.thread_id "
        f"WHERE m.author_id IN ({ph}) "
        f"ORDER BY m.timestamp DESC LIMIT 2000", ids
    ).fetchall()
    if top_msg_row:
        ids_msgs = tuple(m["message_id"] for m in top_msg_row)
        ph_m = ",".join("?" * len(ids_msgs))
        rx_rows = conn.execute(
            f"SELECT message_id, count(*) rc FROM reactions WHERE message_id IN ({ph_m}) GROUP BY message_id",
            ids_msgs
        ).fetchall()
        rx_map = {r["message_id"]: r["rc"] for r in rx_rows}
        top_msg = max(top_msg_row, key=lambda m: rx_map.get(m["message_id"], 0))
        d = dict(top_msg)
        d["detailed_reactions"] = [dict(x) for x in conn.execute(
            "SELECT emoji_url,emoji_name,count(*) count FROM reactions WHERE message_id=? GROUP BY emoji_name ORDER BY count DESC LIMIT 3",
            (d["message_id"],))]
        result["most_liked_msg"] = d
        texts = [m["content"] for m in top_msg_row]
    else:
        texts = []
    # 互动好友：双向统计 —— outgoing=我提及/回复的人；incoming=提及/回复我的人
    ph_ids = list(ids)
    out_rows = conn.execute(
        f"SELECT mentioned_user_id target_id, count(*) score FROM mentions WHERE author_id IN ({ph}) "
        "AND mentioned_user_id NOT IN (" + ",".join("?" for _ in ph_ids) + ") GROUP BY target_id ORDER BY score DESC LIMIT 1",
        ph_ids + ph_ids,
    ).fetchone()
    if not out_rows or not out_rows["score"]:
        # 没有 mention 数据时退化用回复关系
        out_rows = conn.execute(
            f"SELECT m2.author_id target_id, count(*) score FROM messages m1 JOIN messages m2 "
            f"ON m1.reply_to_msg_id=m2.message_id AND m2.author_id NOT IN ({ph}) "
            f"WHERE m1.author_id IN ({ph}) GROUP BY m2.author_id ORDER BY score DESC LIMIT 1",
            ph_ids * 2,
        ).fetchone()
    in_rows = conn.execute(
        f"SELECT m.author_id target_id, count(*) score FROM mentions mn JOIN messages m ON mn.message_id=m.message_id "
        "WHERE mn.mentioned_user_id IN (" + ",".join("?" for _ in ph_ids) + ") "
        "AND m.author_id NOT IN (" + ",".join("?" for _ in ph_ids) + ") GROUP BY m.author_id ORDER BY score DESC LIMIT 1",
        ph_ids * 2,
    ).fetchone()
    if not in_rows or not in_rows["score"]:
        in_rows = conn.execute(
            f"SELECT m1.author_id target_id, count(*) score FROM messages m2 JOIN messages m1 "
            f"ON m2.reply_to_msg_id=m1.message_id AND m1.author_id NOT IN ({ph}) "
            f"WHERE m2.author_id IN ({ph}) GROUP BY m1.author_id ORDER BY score DESC LIMIT 1",
            ph_ids * 2,
        ).fetchone()

    def _friend_row(row):
        if not row:
            return None
        u = conn.execute("SELECT * FROM users WHERE user_id=?", (row["target_id"],)).fetchone()
        if not u:
            return None
        d = dict(u)
        d["score"] = row["score"]
        return d

    result["top_friend_outgoing"] = _friend_row(out_rows)
    result["top_friend_incoming"] = _friend_row(in_rows)
    _REPORT_CACHE[cache_key] = {
        "ts": time.time(), "db_user": db_user,
        "word_cloud_data": format_word_cloud(get_word_cloud_counter(texts), 50),
        "result": result,
    }
    return render_template("report.html", user=db_user, server_id=sid, word_cloud_data=format_word_cloud(get_word_cloud_counter(texts), 50), percentile=95, **result)
