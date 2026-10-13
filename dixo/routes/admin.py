"""Admin panel, whitelist/quota/access, bots, download configs + tasks (moved verbatim from app.py; endpoint = "admin.<function name>")."""
from flask import Blueprint, abort

from dixo import core as _core

# Route bodies were written against app.py's module globals; expose the same names here.
globals().update({k: v for k, v in vars(_core).items() if not k.startswith("__")})

bp = Blueprint("admin", __name__)


@bp.route("/admin")
@login_required
@admin_required
def admin_panel():
    portal = get_portal_db(); sid = current_server_id()
    conn = get_db() if sid and server_db_exists(sid) else None
    reqs = conn.execute("SELECT r.*,u.username req_name,u.avatar_url req_avatar FROM claim_requests_v2 r LEFT JOIN users u ON r.requester_id=u.user_id WHERE r.status=0").fetchall() if conn else []
    merges = conn.execute("SELECT m.*,u.username parent_name FROM user_merges m LEFT JOIN users u ON m.parent_id=u.user_id").fetchall() if conn else []
    whitelist = portal.execute("SELECT w.*,COALESCE(q.quota,1) quota FROM whitelist_users w LEFT JOIN server_download_quota q ON q.user_id=w.user_id ORDER BY w.created_at DESC").fetchall()
    all_servers = portal.execute("SELECT * FROM servers ORDER BY name").fetchall()
    level = admin_level(session["user"]["id"]); uid = str(session["user"]["id"])
    if level == 1:
        servers = all_servers
        accesses = portal.execute("SELECT a.*,w.username FROM user_server_access a LEFT JOIN whitelist_users w ON w.user_id=a.user_id ORDER BY a.user_id,a.server_id").fetchall()
        cfg_rows = portal.execute("SELECT c.*,s.name server_display_name,s.icon_url FROM download_configs c LEFT JOIN servers s ON s.server_id=c.guild_id WHERE c.enabled=1 ORDER BY COALESCE(s.name,c.guild_name),c.forum_name,c.id").fetchall()
        tasks = portal.execute("SELECT t.*,COALESCE(p.username,t.created_by) creator_name,COALESCE(c.guild_name,s.name,t.guild_id) server_display_name,COALESCE(c.forum_name,t.forum_name,t.forum_channel_id) forum_display_name FROM download_tasks t LEFT JOIN portal_users p ON p.user_id=t.created_by LEFT JOIN download_configs c ON c.id=t.config_id LEFT JOIN servers s ON s.server_id=t.guild_id ORDER BY t.id DESC LIMIT 100").fetchall()
    else:
        allowed = {str(x["server_id"]) for x in get_servers_for_user(uid)}
        servers = [x for x in all_servers if str(x["server_id"]) in allowed]
        accesses = portal.execute("SELECT a.*,w.username FROM user_server_access a LEFT JOIN whitelist_users w ON w.user_id=a.user_id WHERE a.user_id=? ORDER BY a.server_id", (uid,)).fetchall()
        cfg_rows = portal.execute("SELECT c.*,s.name server_display_name,s.icon_url FROM download_configs c LEFT JOIN servers s ON s.server_id=c.guild_id WHERE c.enabled=1 AND c.owner_user_id=? ORDER BY COALESCE(s.name,c.guild_name),c.forum_name,c.id", (uid,)).fetchall()
        cfg_ids = [int(x["id"]) for x in cfg_rows]
        if cfg_ids:
            ph = ','.join('?' * len(cfg_ids))
            tasks = portal.execute(f"SELECT t.*,COALESCE(p.username,t.created_by) creator_name,COALESCE(c.guild_name,s.name,t.guild_id) server_display_name,COALESCE(c.forum_name,t.forum_name,t.forum_channel_id) forum_display_name FROM download_tasks t LEFT JOIN portal_users p ON p.user_id=t.created_by LEFT JOIN download_configs c ON c.id=t.config_id LEFT JOIN servers s ON s.server_id=t.guild_id WHERE t.created_by=? OR t.config_id IN ({ph}) ORDER BY t.id DESC LIMIT 100", (uid,*cfg_ids)).fetchall()
        else:
            tasks = portal.execute("SELECT t.*,COALESCE(p.username,t.created_by) creator_name,COALESCE(c.guild_name,s.name,t.guild_id) server_display_name,COALESCE(c.forum_name,t.forum_name,t.forum_channel_id) forum_display_name FROM download_tasks t LEFT JOIN portal_users p ON p.user_id=t.created_by LEFT JOIN download_configs c ON c.id=t.config_id LEFT JOIN servers s ON s.server_id=t.guild_id WHERE t.created_by=? ORDER BY t.id DESC LIMIT 100", (uid,)).fetchall()
    # 时间字段由后端统一计算；模板和后续轮询接口只负责展示结果。
    task_views = []
    for task in tasks:
        view = dict(task)
        calculated_at = datetime.now(timezone.utc)
        timing = calculate_task_timing(view, calculated_at)
        view.update(timing)
        task_views.append(view)
    tasks = task_views

    bot_rows = portal.execute("SELECT id,name,token_type,owner_user_id,created_at FROM download_bots WHERE owner_user_id=? ORDER BY id", (uid,)).fetchall()
    bot_map = {str(row["id"]): row["name"] for row in portal.execute("SELECT id,name FROM download_bots")}
    linked = collections.defaultdict(list)
    for row in portal.execute("SELECT config_id,bot_id FROM download_config_bots"):
        linked[int(row["config_id"])].append(bot_map.get(str(row["bot_id"]), f"机器人 #{row['bot_id']}"))
    guild_icon_map = {str(g.get("id")): g.get("icon_url") for g in managed_guilds_from_session()}
    configs = []
    for row in cfg_rows:
        d = dict(row)
        d["icon_url"] = d.get("icon_url") or guild_icon_map.get(str(row["guild_id"]))
        d["bot_names"] = linked.get(int(row["id"]), [])
        if row["use_default_bot"]:
            d["bot_names"].append("默认下载机器人")
        configs.append(d)
    return render_template("admin.html", pending_requests=reqs, active_merges=merges, whitelist=whitelist, servers=servers, accesses=accesses, bots=bot_rows, download_servers=configs, tasks=tasks, admin_level=level, current_user=session["user"], user_quota=get_download_quota(uid))


@bp.route("/admin/bot-features")
@login_required
@admin_required
def admin_bot_features():
    """机器人用户功能面板：拉黑列表 / 收藏列表。

    机器人在 portal.db 新增用户功能数据表时，在这里补一个查询块即可同步到面板。
    """
    portal = get_portal_db()

    # ---- 拉黑列表（含双向拦截次数）----
    blocklist = portal.execute(
        """
        SELECT b.blocker_id, b.blocked_id, b.created_at,
               COALESCE(SUM(c.count), 0) AS intercepted,
               MAX(c.last_blocked_at) AS last_blocked_at
        FROM user_blocks b
        LEFT JOIN block_interception_counts c
               ON (c.sender_id = b.blocker_id AND c.target_id = b.blocked_id)
               OR (c.sender_id = b.blocked_id AND c.target_id = b.blocker_id)
        GROUP BY b.blocker_id, b.blocked_id
        ORDER BY intercepted DESC, b.created_at DESC
        LIMIT 200
        """
    ).fetchall()

    def _display_name(user_id):
        row = portal.execute(
            "SELECT username, nickname FROM portal_users WHERE user_id=?", (str(user_id),)
        ).fetchone()
        if row:
            return row["nickname"] or row["username"] or str(user_id)
        return str(user_id)

    blocks = []
    for r in blocklist:
        blocks.append({
            "blocker": _display_name(r["blocker_id"]),
            "blocked": _display_name(r["blocked_id"]),
            "created_at": (r["created_at"] or "")[:16].replace("T", " "),
            "intercepted": int(r["intercepted"] or 0),
            "last_blocked_at": ((r["last_blocked_at"] or "")[:16].replace("T", " ")) or "—",
        })

    # ---- 收藏列表（最近 200 条，附帖子标题）----
    fav_rows = portal.execute(
        """
        SELECT f.user_id, f.guild_id, f.thread_id, f.created_at,
               COALESCE(s.name, f.guild_id) AS guild_name
        FROM favorites f
        LEFT JOIN servers s ON s.server_id = f.guild_id
        ORDER BY f.created_at DESC
        LIMIT 200
        """
    ).fetchall()
    thread_ids_by_guild = {}
    for r in fav_rows:
        thread_ids_by_guild.setdefault(str(r["guild_id"]), set()).add(str(r["thread_id"]))
    thread_names = {}
    for gid, tids in thread_ids_by_guild.items():
        db_path = server_db_path(gid)
        if not server_db_exists(gid):
            continue
        try:
            g = db_connect(db_path)
            ph = ",".join("?" * len(tids))
            for row in g.execute(
                f"SELECT thread_id, name FROM threads WHERE thread_id IN ({ph})", tuple(tids)
            ):
                thread_names[str(row["thread_id"])] = row["name"]
            g.close()
        except Exception:
            app.logger.exception("读取服务器帖子标题失败 guild=%s", gid)
    favorites = []
    for r in fav_rows:
        tid = str(r["thread_id"])
        favorites.append({
            "user": _display_name(r["user_id"]),
            "guild_name": r["guild_name"],
            "thread_name": thread_names.get(tid) or f"帖子 {tid}",
            "created_at": (r["created_at"] or "")[:16].replace("T", " "),
        })

    return render_template(
        "bot_features.html",
        blocks=blocks,
        favorites=favorites,
        current_user=session["user"],
        discord_client_id=DISCORD_CLIENT_ID,
    )


@bp.route("/admin/approve/<int:req_id>", methods=["POST"])
@login_required
@admin_required
def admin_approve(req_id):
    scope = _admin_server_scope()
    if isinstance(scope, tuple):
        return scope
    sid = scope
    conn = get_db()
    req = conn.execute("SELECT * FROM claim_requests_v2 WHERE id=? AND status=0", (req_id,)).fetchone()
    if req:
        conn.execute("UPDATE claim_requests_v2 SET status=1 WHERE id=?", (req_id,))
        conn.execute("INSERT OR REPLACE INTO user_merges(target_id,parent_id,created_at) VALUES(?,?,?)", (req["target_id"], req["requester_id"], utc_now_iso())); data_engine.invalidate()
        conn.commit()
    return redirect(url_for("admin.admin_panel"))


@bp.route("/admin/unmerge/<target_id>", methods=["POST"])
@login_required
@admin_required
def admin_unmerge(target_id):
    scope = _admin_server_scope()
    if isinstance(scope, tuple):
        return scope
    conn = get_db(); conn.execute("DELETE FROM user_merges WHERE target_id=?", (target_id,)); data_engine.invalidate(); conn.commit(); return redirect(url_for("admin.admin_panel"))


@bp.route("/admin/reset_all_claims", methods=["POST"])
@login_required
@admin_required
def admin_reset_all():
    scope = _admin_server_scope()
    if isinstance(scope, tuple):
        return scope
    conn = get_db(); conn.execute("DELETE FROM claim_requests_v2"); conn.execute("DELETE FROM user_merges"); data_engine.invalidate(); conn.commit(); return redirect(url_for("admin.admin_panel"))


@bp.route("/admin/whitelist/add", methods=["POST"])
@login_required
@level1_required
def admin_whitelist_add():
    uid = str(request.form.get("user_id", "")).strip()
    if not uid.isdigit(): return "无效 Discord User ID", 400
    portal = get_portal_db()
    # 与 bot_identity() 相同的原则：不使用手动填写的名称，用 Discord API 权威查询，
    # 避免用户名和真实 Discord 账号对不上；查询失败（未配置 token 或用户不存在）时
    # 先用 ID 占位，不阻断白名单添加。
    try:
        info = fetch_discord_user(uid)
    except requests.RequestException as exc:
        app.logger.warning("查询白名单用户信息失败 user_id=%s，使用 ID 占位 error=%s", uid, exc)
        info = None
    name = info["username"] if info else uid
    portal.execute("INSERT OR IGNORE INTO whitelist_users(user_id,username,added_by,created_at) VALUES(?,?,?,?)", (uid, name, session["user"]["id"], utc_now_iso()))
    portal.execute("INSERT OR IGNORE INTO server_download_quota(user_id,quota) VALUES(?,1)", (uid,))
    portal.commit()
    return redirect(url_for("admin.admin_panel"))


@bp.route("/admin/whitelist/delete/<user_id>", methods=["POST"])
@login_required
@level1_required
def admin_whitelist_delete(user_id):
    portal=get_portal_db(); portal.execute("DELETE FROM whitelist_users WHERE user_id=?", (str(user_id),)); portal.execute("DELETE FROM server_download_quota WHERE user_id=?", (str(user_id),)); portal.execute("DELETE FROM user_server_access WHERE user_id=?", (str(user_id),)); portal.commit(); return redirect(url_for("admin.admin_panel"))


@bp.route("/admin/quota", methods=["POST"])
@login_required
@level1_required
def admin_quota():
    uid=str(request.form.get("user_id",""))
    try:
        quota=max(1,min(100,int(request.form.get("quota","1"))))
    except (TypeError, ValueError):
        return "配额必须是数字",400
    portal=get_portal_db(); portal.execute("INSERT INTO server_download_quota(user_id,quota) VALUES(?,?) ON CONFLICT(user_id) DO UPDATE SET quota=excluded.quota",(uid,quota)); portal.commit(); return redirect(url_for("admin.admin_panel"))


@bp.route("/admin/access", methods=["POST"])
@login_required
@level1_required
def admin_access():
    uid=str(request.form.get("user_id","")); sid=str(request.form.get("server_id",""))
    if uid and sid and get_portal_db().execute("SELECT 1 FROM servers WHERE server_id=?",(sid,)).fetchone():
        p=get_portal_db(); p.execute("INSERT OR IGNORE INTO user_server_access(user_id,server_id,granted_by,created_at) VALUES(?,?,?,?)",(uid,sid,session["user"]["id"],utc_now_iso())); p.commit()
    return redirect(url_for("admin.admin_panel"))


@bp.route("/admin/access/delete", methods=["POST"])
@login_required
@level1_required
def admin_access_delete():
    p=get_portal_db(); p.execute("DELETE FROM user_server_access WHERE user_id=? AND server_id=?",(str(request.form.get("user_id")),str(request.form.get("server_id")))); p.commit(); return redirect(url_for("admin.admin_panel"))


@bp.route("/admin/bot/add", methods=["POST"])
@login_required
@admin_required
def admin_bot_add():
    p=get_portal_db(); owner=str(session["user"]["id"])
    token=request.form.get("token","").strip()
    if not token: return "Token 不能为空",400
    token_type=str(request.form.get("token_type", "bot")).strip().lower()
    if token_type not in ("bot", "user"):
        return "账号类型无效",400
    try:
        name=bot_identity(token, token_type)
    except requests.RequestException:
        return "无法验证机器人密钥，请确认 Token 有效后重试",400
    p.execute("INSERT INTO download_bots(owner_user_id,name,token,token_type,created_at) VALUES(?,?,?,?,?)",(owner,name,token,token_type,utc_now_iso())); p.commit(); return redirect(url_for("admin.admin_panel"))


@bp.route("/admin/bot/delete/<int:bot_id>", methods=["POST"])
@login_required
@admin_required
def admin_bot_delete(bot_id):
    p=get_portal_db(); owner=str(session["user"]["id"])
    if p.execute("SELECT 1 FROM download_bots WHERE id=? AND owner_user_id=?",(bot_id,owner)).fetchone():
        p.execute("DELETE FROM download_server_bots WHERE bot_id=?",(bot_id,))
        p.execute("DELETE FROM download_config_bots WHERE bot_id=?",(bot_id,))
        p.execute("UPDATE download_servers SET bot_id=NULL WHERE bot_id=?",(bot_id,))
        p.execute("DELETE FROM download_bots WHERE id=?",(bot_id,))
        p.commit()
    return redirect(url_for("admin.admin_panel"))


@bp.route("/admin/download-server", methods=["POST"])
@login_required
@admin_required
def admin_download_server():
    p=get_portal_db(); uid=str(session["user"]["id"])
    guild=str(request.form.get("guild_id","")).strip(); forum=str(request.form.get("forum_channel_id","")).strip()
    bot_ids=[str(x) for x in request.form.getlist("bot_ids") if str(x).isdigit()]; use_default=request.form.get("use_default_bot")=="1"
    try:
        scheduler_interval=max(50,min(60000,int(request.form.get("scheduler_interval") or 250)))
        download_interval_ms=max(0,min(60000,int(request.form.get("download_interval_ms") or 0)))
    except (TypeError,ValueError):
        return "调度间隔和下载间隔必须是数字",400
    update_enabled=1 if request.form.get("update_enabled")=="1" else 0
    if not guild.isdigit() or not forum.isdigit(): return "Guild ID 和 Forum Channel ID 必须是数字",400
    if admin_level(uid)==2:
        allowed={str(x.get("id")) for x in managed_guilds_from_session()}
        if guild not in allowed: return "你只能配置自己有管理权限的服务器",403
    if not bot_ids and not use_default:
        flash("请至少选择一个下载机器人或默认下载机器人", "error"); return redirect(url_for("admin.admin_panel"))
    try:
        selected=selected_download_bots(p,uid,bot_ids,use_default)
    except (ValueError,PermissionError) as exc:
        flash(str(exc),"error"); return redirect(url_for("admin.admin_panel"))
    failures=[]
    for bot in selected:
        ok,reason=check_bot_forum_access(bot["token"],guild,forum,bot.get("token_type", "bot"))
        if not ok: failures.append(f"{bot['name']}：{reason}")
    if failures:
        flash("下载配置未保存。以下机器人没有目标 Forum 的访问权限：\n"+"\n".join(failures),"error"); return redirect(url_for("admin.admin_panel"))
    managed={str(x.get("id")):x for x in managed_guilds_from_session()}
    guild_name=(managed.get(guild) or {}).get("name") or (p.execute("SELECT name FROM servers WHERE server_id=?",(guild,)).fetchone() or {"name":guild})["name"]
    forum_name=forum
    try:
        # 由用户的 OAuth guild/channel 资源获取权威 Forum 名称。
        for bot in selected[:1]:
            resp=discord_get(f"/guilds/{guild}/channels",bot["token"],bot.get("token_type", "bot"),timeout=12)
            if resp.ok:
                ch=next((x for x in resp.json() if str(x.get("id"))==forum),None)
                if ch: forum_name=ch.get("name") or forum
                break
    except requests.RequestException as exc:
        app.logger.warning("查询 Forum 名称失败 guild_id=%s forum_id=%s，使用 ID 占位 error=%s", guild, forum, exc)
    server_key=f"{guild}:{forum}"
    p.execute("INSERT INTO download_configs(server_id,owner_user_id,guild_id,forum_channel_id,guild_name,forum_name,enabled,use_default_bot,scheduler_interval,download_interval_ms,update_enabled,updated_at) VALUES(?,?,?,?,?,?,1,?,?,?,?,?) ON CONFLICT(guild_id,forum_channel_id) DO UPDATE SET owner_user_id=excluded.owner_user_id,server_id=excluded.server_id,guild_name=excluded.guild_name,forum_name=excluded.forum_name,enabled=1,use_default_bot=excluded.use_default_bot,scheduler_interval=excluded.scheduler_interval,download_interval_ms=excluded.download_interval_ms,update_enabled=excluded.update_enabled,updated_at=excluded.updated_at",(server_key,uid,guild,forum,guild_name,forum_name,1 if use_default else 0,scheduler_interval,download_interval_ms,update_enabled,utc_now_iso()))
    cfg=p.execute("SELECT id FROM download_configs WHERE guild_id=? AND forum_channel_id=?",(guild,forum)).fetchone(); cfg_id=int(cfg["id"])
    p.execute("DELETE FROM download_config_bots WHERE config_id=?",(cfg_id,))
    p.executemany("INSERT INTO download_config_bots(config_id,bot_id) VALUES(?,?)",[(cfg_id,x["id"]) for x in selected if x["id"]!="default"])
    p.commit(); flash(f"下载配置已保存：{guild_name} / {forum_name}，{len(selected)} 个机器人。","success")
    return redirect(url_for("admin.admin_panel"))


@bp.route("/admin/download-server/delete/<int:config_id>",methods=["POST"])
@login_required
@admin_required
def admin_download_server_delete(config_id):
    p=get_portal_db(); row=p.execute("SELECT owner_user_id FROM download_configs WHERE id=?",(config_id,)).fetchone()
    if not row: return "配置不存在",404
    if admin_level(session["user"]["id"])==2 and str(row["owner_user_id"])!=str(session["user"]["id"]): return "403 Access Denied",403
    p.execute("DELETE FROM download_config_bots WHERE config_id=?",(config_id,)); p.execute("DELETE FROM download_configs WHERE id=?",(config_id,)); p.commit(); return redirect(url_for("admin.admin_panel"))


@bp.route("/api/downloader-config")
@login_required
def downloader_config():
    uid=str(session["user"]["id"]); level=admin_level(uid); p=get_portal_db()
    if level==1: rows=p.execute("SELECT * FROM download_configs WHERE enabled=1 ORDER BY id").fetchall()
    else: rows=p.execute("SELECT * FROM download_configs WHERE enabled=1 AND owner_user_id=? ORDER BY id",(uid,)).fetchall()
    # 机器人 Token 只在服务端使用；不要通过 HTTP 接口回传给浏览器。
    return jsonify({"servers":[dict(r) for r in rows],"bots":[dict(r) for r in p.execute("SELECT id,name FROM download_bots WHERE owner_user_id=? ORDER BY id",(uid,)).fetchall()]})


@bp.route("/api/managed-discord-resources")
@login_required
@admin_required
def managed_discord_resources():
    """Populate the download form from Discord instead of asking for IDs."""
    uid = str(session["user"]["id"])
    guilds = managed_guilds_from_session()
    bots = [dict(row) for row in get_portal_db().execute("SELECT id,name,token,token_type FROM download_bots WHERE owner_user_id=?", (uid,)).fetchall()]
    default_token = default_downloader_token()
    if default_token:
        bots.append({"id": "default", "name": "默认下载机器人", "token": default_token})

    cache_key = (uid, tuple(sorted(str(guild.get("id")) for guild in guilds)), tuple(sorted(str(bot.get("id")) for bot in bots)))
    now = time.monotonic()
    with _managed_resources_cache_lock:
        cached = _managed_resources_cache.get(cache_key)
        if cached and now - cached["created_at"] < _MANAGED_RESOURCES_TTL:
            return jsonify(cached["payload"])

    forums = {}
    warnings = []
    seen_forums = set()

    def fetch_forums(bot, guild):
        try:
            response = discord_get(f"/guilds/{guild['id']}/channels", bot["token"], bot.get("token_type", "bot"), timeout=12)
            if response.status_code in (401, 403, 404):
                return guild["id"], [], None
            response.raise_for_status()
            channels = [
                {"id": channel["id"], "name": channel.get("name") or channel["id"]}
                for channel in response.json() if channel.get("type") == 15
            ]
            return guild["id"], channels, None
        except requests.RequestException as exc:
            return guild["id"], [], f"{bot['name']} 无法读取频道：{exc}"

    # Channel lists are independent requests. Running them concurrently keeps
    # an admin page with several managed guilds/bots from waiting on a long
    # serial chain of Discord round trips.
    jobs = []
    with ThreadPoolExecutor(max_workers=min(8, max(1, len(bots) * len(guilds)))) as executor:
        for bot in bots:
            for guild in guilds:
                jobs.append(executor.submit(fetch_forums, bot, guild))
        for job in as_completed(jobs):
            guild_id, channels, warning = job.result()
            if warning:
                warnings.append(warning)
            for channel in channels:
                key=(str(guild_id),str(channel["id"]))
                if key in seen_forums:
                    continue
                seen_forums.add(key)
                forums.setdefault(str(guild_id), []).append(channel)
    # Tokens are used only on the server; never expose them to the browser.
    payload = {"guilds": guilds, "forums": forums, "warnings": warnings}
    with _managed_resources_cache_lock:
        _managed_resources_cache[cache_key] = {"created_at": time.monotonic(), "payload": payload}
    return jsonify(payload)


@bp.route("/admin/member-sync", methods=["POST"])
@login_required
@admin_required
def admin_member_sync():
    uid=str(session["user"]["id"]); guild=str(request.form.get("guild_id","")).strip()
    if not guild.isdigit(): return "服务器 ID 无效",400
    if admin_level(uid)==2:
        allowed={str(x.get("id")) for x in managed_guilds_from_session()}
        if guild not in allowed:
            p=get_portal_db()
            owned=p.execute("SELECT 1 FROM download_configs WHERE guild_id=? AND owner_user_id=? AND enabled=1 LIMIT 1",(guild,uid)).fetchone()
            if not owned: return "你没有权限更新该服务器成员名单",403
    request_member_sync(guild,uid)
    flash(f"服务器 {guild} 的成员名单更新已加入队列。", "success")
    return redirect(url_for("admin.admin_panel"))


@bp.route("/admin/download-task",methods=["POST"])
@login_required
@admin_required
def admin_download_task_add():
    uid=str(session["user"]["id"]); config_id=str(request.form.get("config_id","")).strip()
    if not config_id.isdigit(): return "下载配置无效",400
    p=get_portal_db(); cfg=p.execute("SELECT * FROM download_configs WHERE id=? AND enabled=1",(int(config_id),)).fetchone()
    if not cfg: return "下载配置不存在或已禁用",400
    if admin_level(uid)==2:
        if str(cfg["owner_user_id"])!=uid: return "你只能为自己有管理权限的下载配置创建任务",403
        quota=get_download_quota(uid)
        active=p.execute("SELECT COUNT(DISTINCT guild_id) FROM download_tasks WHERE created_by=? AND status IN ('pending','running')",(uid,)).fetchone()[0]
        if active>=quota: return f"已达到下载服务器配额 {quota}",403
    nowv=utc_now_iso()
    mode="update" if int(cfg["update_enabled"] or 0) and server_db_exists(str(cfg["guild_id"])) else "initial"
    p.execute("INSERT INTO download_tasks(guild_id,forum_channel_id,created_by,status,total,completed,created_at,message,config_id,guild_name,forum_name,scheduler_interval,download_interval_ms,mode) VALUES(?,?,?,?,0,0,?,?,?,?,?,?,?,?)",(cfg["guild_id"],cfg["forum_channel_id"],uid,"pending",nowv,f"等待下载器启动 · {'更新模式' if mode=='update' else '首次下载模式'}",cfg["id"],cfg["guild_name"] or cfg["guild_id"],cfg["forum_name"] or cfg["forum_channel_id"],cfg["scheduler_interval"] if int(cfg["scheduler_interval"] or 0) >= 50 else int(cfg["scheduler_interval"] or 1) * 1000,int(cfg["download_interval_ms"] or 0),mode))
    p.commit();
    request_member_sync(str(cfg["guild_id"]), uid)
    ensure_downloader_process(); return redirect(url_for("admin.admin_panel"))


@bp.route("/admin/download-task/<int:task_id>/bot/add", methods=["POST"])
@login_required
@admin_required
def admin_download_task_bot_add(task_id):
    """把一个已验证的下载机器人加入正在运行的任务配置。"""
    p=get_portal_db(); uid=str(session["user"]["id"]); task=p.execute("SELECT * FROM download_tasks WHERE id=?",(task_id,)).fetchone()
    wants_json=request.headers.get("X-Requested-With")=="XMLHttpRequest" or request.accept_mimetypes.best=="application/json"
    def response(message, status=200):
        if wants_json:
            return jsonify({"ok":status < 400, "message":message}), status
        flash(message, "error" if status >= 400 else "success")
        return redirect(url_for("admin.admin_panel"))

    if not task or not download_task_scope(task):
        return response("你没有权限操作这个下载任务",403)
    if task["status"] not in ("pending","running"):
        return response("只有排队中或下载中的任务可以加入机器人",400)
    config_id=task["config_id"]
    cfg=p.execute("SELECT * FROM download_configs WHERE id=? AND enabled=1",(config_id,)).fetchone() if config_id else None
    if not cfg:
        return response("下载配置不存在或已禁用",400)
    bot_id=str(request.form.get("bot_id","")).strip()
    if bot_id=="default":
        token=default_downloader_token()
        if not token:
            return response("服务器未配置默认下载机器人",400)
        bot={"id":"default","name":"默认下载机器人","token":token,"token_type":"bot"}
        already=bool(cfg["use_default_bot"])
    elif bot_id.isdigit():
        query="SELECT id,name,token,token_type,owner_user_id FROM download_bots WHERE id=?"
        row=p.execute(query,(int(bot_id),)).fetchone()
        if not row:
            return response("下载机器人不存在",404)
        if admin_level(uid)!=1 and str(row["owner_user_id"])!=uid:
            return response("只能加入自己拥有的下载机器人",403)
        bot=dict(row); already=bool(p.execute("SELECT 1 FROM download_config_bots WHERE config_id=? AND bot_id=?",(config_id,int(bot_id))).fetchone())
    else:
        return response("下载机器人选择无效",400)
    if already:
        return response(f"{bot['name']} 已在此下载配置中")
    configured_count=p.execute("SELECT COUNT(*) FROM download_config_bots WHERE config_id=?",(config_id,)).fetchone()[0] + int(cfg["use_default_bot"] or 0)
    ok,reason=check_bot_forum_access(bot["token"],str(task["guild_id"]),str(task["forum_channel_id"]),bot.get("token_type", "bot"))
    if not ok:
        return response(f"{bot['name']} 无法访问目标 Forum：{reason}",400)
    if bot_id=="default":
        p.execute("UPDATE download_configs SET use_default_bot=1,updated_at=? WHERE id=?",(utc_now_iso(),config_id))
    else:
        p.execute("INSERT OR IGNORE INTO download_config_bots(config_id,bot_id) VALUES(?,?)",(config_id,int(bot_id)))
    p.commit(); ensure_downloader_process()
    return response(f"已加入 {bot['name']}。下载器会在当前批次结束后启动它。")


@bp.route("/admin/download-task/<int:task_id>/pause",methods=["POST"])
@login_required
@admin_required
def admin_download_task_pause(task_id): return _task_action(task_id,"pause")


@bp.route("/admin/download-task/<int:task_id>/resume",methods=["POST"])
@login_required
@admin_required
def admin_download_task_resume(task_id): return _task_action(task_id,"resume")


@bp.route("/admin/download-task/<int:task_id>/cancel",methods=["POST"])
@login_required
@admin_required
def admin_download_task_cancel(task_id): return _task_action(task_id,"cancel")


@bp.route("/admin/download-task/<int:task_id>/delete",methods=["POST"])
@login_required
@admin_required
def admin_download_task_delete(task_id):
    import shutil
    p=get_portal_db(); task=p.execute("SELECT * FROM download_tasks WHERE id=?",(task_id,)).fetchone()
    if not task:
        return "下载任务不存在", 404
    if not download_task_scope(task):
        flash("你没有权限删除这个下载任务。", "error")
        return redirect(url_for("admin.admin_panel"))
    guild_id=str(task["guild_id"]); task_root=os.path.join(BASE_DIR,"raw",guild_id,"tasks",str(task_id))
    was_running = task["status"] == "running"
    if was_running:
        # 先让后台 Worker 看到取消状态，再做数据库清理。任务行随后立即删除，
        # 避免下载器进程异常退出时留下“隐藏任务”和未清理的分析数据。
        p.execute("UPDATE download_tasks SET status='cancelled',phase='cancelled',delete_requested=1,message='正在停止并清理任务…',finished_at=? WHERE id=?",(utc_now_iso(),task_id)); p.commit()

    # 这一步必须在删除 download_task_items 之前执行；这些 item 是分析库中
    # 帖子归属的唯一任务索引。后台导入线程还会在写入后检查 cancelled 状态，
    # 因此即使删除与导入并发，也不会把帖子永久写回数据库。
    _delete_task_data_from_server(p,task_id,guild_id)
    p.execute("DELETE FROM download_task_items WHERE task_id=?",(task_id,))
    p.execute("DELETE FROM download_tasks WHERE id=?",(task_id,))
    remaining=p.execute("SELECT COUNT(*) FROM download_tasks WHERE guild_id=? AND status='completed'",(guild_id,)).fetchone()[0]
    server_db = None
    if remaining==0:
        # 该服务器已没有任何完成任务时，清理空的服务器门户记录和访问缓存。
        server=p.execute("SELECT db_path FROM servers WHERE server_id=?",(guild_id,)).fetchone()
        server_db = server["db_path"] if server and server["db_path"] else server_db_path(guild_id)
        p.execute("DELETE FROM user_server_access WHERE server_id=?",(guild_id,)); p.execute("DELETE FROM user_server_presence WHERE server_id=?",(guild_id,)); p.execute("DELETE FROM servers WHERE server_id=?",(guild_id,))
        try:
            p.execute("DELETE FROM thread_scan_state WHERE guild_id=?",(guild_id,))
        except sqlite3.OperationalError as exc:
            if not is_missing_table_error(exc):
                raise
            app.logger.warning("删除服务器扫描缓存时表不存在 guild_id=%s", guild_id)
    p.commit()
    if remaining==0 and server_db and not cloud_enabled() and os.path.exists(server_db):
        try: os.remove(server_db)
        except OSError as exc:
            app.logger.warning("删除服务器数据库失败 path=%s error=%s", server_db, exc)
    # Unix 下删除仍被 DCE 打开的文件是安全的；这样运行中删除也不会
    # 遗留 raw 目录。后台线程随后会因任务行不存在而停止处理。
    if os.path.exists(task_root):
        shutil.rmtree(task_root,ignore_errors=True)
    return redirect(url_for("admin.admin_panel"))


@bp.route("/admin/download-tasks/status")
@login_required
@admin_required
def admin_download_tasks_status():
    p=get_portal_db()
    uid=str(session["user"]["id"]); level=admin_level(uid)
    scope_sql = ""
    scope_args = ()
    if level != 1:
        scope_sql = " AND (t.created_by=? OR EXISTS (SELECT 1 FROM download_configs scoped_c WHERE scoped_c.id=t.config_id AND scoped_c.owner_user_id=?))"
        scope_args = (uid, uid)
    rows=p.execute(
        "SELECT t.*,COALESCE(c.guild_name,s.name,t.guild_id) server_display_name,"
        "COALESCE(c.forum_name,t.forum_name,t.forum_channel_id) forum_display_name,"
        "COALESCE(c.use_default_bot,0) config_use_default_bot "
        "FROM download_tasks t LEFT JOIN download_configs c ON c.id=t.config_id "
        "LEFT JOIN servers s ON s.server_id=t.guild_id WHERE t.delete_requested=0" + scope_sql +
        " ORDER BY t.id DESC LIMIT 100", scope_args
    ).fetchall()
    result=[]
    task_ids = [x["id"] for x in rows]
    item_stats = {}
    bot_progress = collections.defaultdict(list)
    if task_ids:
        task_ph = ",".join("?" for _ in task_ids)
        for item in p.execute(
            f"SELECT task_id,COUNT(*) total_items,"
            f"SUM(CASE WHEN status='downloaded' THEN 1 ELSE 0 END) downloaded_items,"
            f"SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) failed_items,"
            f"SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END) pending_items "
            f"FROM download_task_items WHERE task_id IN ({task_ph}) GROUP BY task_id", task_ids
        ):
            item_stats[item["task_id"]] = dict(item)
        for bot in p.execute(
            f"SELECT task_id,bot_name,COUNT(*) count FROM download_task_items "
            f"WHERE task_id IN ({task_ph}) AND bot_name IS NOT NULL "
            f"GROUP BY task_id,bot_name ORDER BY task_id,count DESC", task_ids
        ):
            bot_progress[bot["task_id"]].append({"name": bot["bot_name"], "count": int(bot["count"] or 0)})
    available_rows = p.execute(
        "SELECT id,name FROM download_bots" + (" WHERE owner_user_id=?" if level != 1 else "") + " ORDER BY id",
        (uid,) if level != 1 else (),
    ).fetchall()
    available = [{"id": str(row["id"]), "name": row["name"]} for row in available_rows]
    cfg_ids = [x["config_id"] for x in rows if x["config_id"]]
    configured = collections.defaultdict(set)
    if cfg_ids:
        cfg_ph = ",".join("?" for _ in cfg_ids)
        for config_bot in p.execute(
            f"SELECT config_id,bot_id FROM download_config_bots WHERE config_id IN ({cfg_ph})", cfg_ids
        ):
            configured[config_bot["config_id"]].add(str(config_bot["bot_id"]))
    has_default_token = bool(default_downloader_token())
    for x in rows:
        item = item_stats.get(x["id"], {})
        d=dict(x)
        d.update({"total_items": 0, "downloaded_items": 0, "failed_items": 0, "pending_items": 0})
        d.update({k:int(v or 0) for k,v in item.items()})
        if d["total_items"]:
            d["total"] = max(int(d.get("total") or 0), d["total_items"])
            d["completed"] = max(0, d["total_items"] - d["pending_items"] - d["failed_items"])
        calculated_at = datetime.now(timezone.utc)
        timing = calculate_task_timing(d, calculated_at)
        d.update(timing)
        d["bot_progress"] = bot_progress.get(x["id"], [])
        configured_ids = configured.get(x["config_id"], set()).copy()
        if x["config_use_default_bot"]:
            configured_ids.add("default")
        d["configured_bot_ids"]=sorted(configured_ids)
        d["available_bots"]=[bot for bot in available if bot["id"] not in configured_ids]
        if "default" not in configured_ids and has_default_token:
            d["available_bots"].insert(0,{"id":"default","name":"默认下载机器人"})
        result.append(d)
    return jsonify(result)


# ---------------------------------------------------------------------------
# 「还在吗？」活跃确认（shared/liveness.py；私信由 whitelist_bot 发送）
# ---------------------------------------------------------------------------
def _liveness_candidates(sid, as_of=None, tags=("faded", "rare")):
    from dixo import dashboard_data
    from shared import liveness
    summ = summary_engine.summary_conn(sid)
    try:
        res = dashboard_data.member_activity_tags(summ, as_of=as_of or None)
        rows = [dict(r) for t in tags for r in res.get("by_tag", {}).get(t, [])]
        last = {}
        ids = [r["user_id"] for r in rows]
        for i in range(0, len(ids), 500):
            chunk = ids[i:i + 500]
            ph = ",".join("?" * len(chunk))
            for r in summ.execute(f"SELECT user_id,last_ts FROM user_stats WHERE user_id IN ({ph})", chunk):
                last[str(r[0])] = r[1]
    finally:
        summ.close()
    names, bots = {}, set()
    main = get_db()
    for i in range(0, len(rows), 500):
        chunk = [r["user_id"] for r in rows[i:i + 500]]
        ph = ",".join("?" * len(chunk))
        for r in main.execute(f"SELECT user_id,username,nickname,is_bot FROM users WHERE user_id IN ({ph})", chunk):
            names[str(r["user_id"])] = r["nickname"] or r["username"] or str(r["user_id"])
            if r["is_bot"]:
                bots.add(str(r["user_id"]))
    for r in rows:
        uid = r["user_id"]
        r["name"] = names.get(uid, uid)
        r["is_bot"] = uid in bots
        r["last_message_at"] = last.get(uid)
        r["months_idle"] = liveness.months_since(last.get(uid))
    return [r for r in rows if not r["is_bot"]], res.get("as_of")


@bp.route("/admin/liveness")
@login_required
@admin_required
def admin_liveness():
    from shared import liveness
    if admin_level(session["user"]["id"]) != 1:
        abort(403)
    sid = current_server_id()
    portal = get_portal_db()
    liveness.ensure(portal)
    as_of = (request.args.get("as_of") or "").strip()[:7]
    tag = request.args.get("tag") or "all"
    tags = ("faded",) if tag == "faded" else ("rare",) if tag == "rare" else ("faded", "rare")
    candidates, as_of_ym = ([], None)
    if sid and server_db_exists(sid):
        candidates, as_of_ym = _liveness_candidates(sid, as_of, tags)
    latest = {}
    for r in portal.execute("SELECT * FROM liveness_checks WHERE guild_id=? ORDER BY id", (str(sid or ""),)):
        latest[str(r["user_id"])] = dict(r)
    for c in candidates:
        c["check"] = latest.get(c["user_id"])
    checks = [dict(r) for r in portal.execute(
        "SELECT * FROM liveness_checks WHERE guild_id=? ORDER BY id DESC LIMIT 500", (str(sid or ""),))]
    name_of = {c["user_id"]: c["name"] for c in candidates}
    for ch in checks:
        ch["name"] = name_of.get(str(ch["user_id"]), str(ch["user_id"]))
    counts = collections.Counter(ch["status"] for ch in checks)
    return render_template("liveness.html", candidates=candidates[:3000], total=len(candidates), checks=checks,
                           counts=counts, as_of=as_of_ym, tag=tag, sid=sid,
                           enabled=str(sid) == str(liveness.LIVENESS_GUILD_ID),
                           role_id=liveness.LIVENESS_ROLE_ID, deadline_days=liveness.DEADLINE_DAYS,
                           current_user=session["user"])


@bp.route("/admin/liveness/send", methods=["POST"])
@login_required
@admin_required
def admin_liveness_send():
    from shared import liveness
    if admin_level(session["user"]["id"]) != 1:
        abort(403)
    sid = current_server_id()
    if str(sid) != str(liveness.LIVENESS_GUILD_ID):
        flash("当前服务器未启用活跃确认 · Liveness checks are not enabled for this server", "error")
        return redirect(url_for("admin.admin_liveness"))
    picked = {x for x in request.form.getlist("user_ids") if x.isdigit()}
    if not picked:
        flash("请先选择成员 · Select members first", "error")
        return redirect(url_for("admin.admin_liveness", as_of=request.form.get("as_of") or None))
    cands, _ = _liveness_candidates(sid, request.form.get("as_of") or None)
    by_id = {c["user_id"]: c for c in cands}
    users = [by_id[u] for u in picked if u in by_id]  # only real faded/rare members, never arbitrary ids
    portal = get_portal_db()
    created, skipped = liveness.enqueue(portal, sid, users, session["user"]["id"])
    app.logger.info("liveness.enqueue admin=%s created=%s skipped=%s", session["user"]["id"], created, len(skipped))
    flash(f"已加入发送队列 {created} 人（机器人约每 3 秒发送一条）；跳过 {len(skipped)} 人（已有待确认或为机器人）。"
          f" · Queued {created}, skipped {len(skipped)}.", "success")
    return redirect(url_for("admin.admin_liveness", as_of=request.form.get("as_of") or None))
