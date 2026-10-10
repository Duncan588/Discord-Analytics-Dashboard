"""OAuth login / callback / logout (moved verbatim from app.py; endpoint = "auth.<function name>")."""
from flask import Blueprint

from dixo import core as _core

# Route bodies were written against app.py's module globals; expose the same names here.
globals().update({k: v for k, v in vars(_core).items() if not k.startswith("__")})

bp = Blueprint("auth", __name__)


@bp.route("/login")
def login():
    if "user" in session:
        servers = get_servers_for_user(session["user"]["id"])
        if len(servers) == 1:
            session["server_id"] = servers[0]["server_id"]
            return redirect(url_for("pages.index"))
        if len(servers) > 1:
            return redirect(url_for("pages.servers"))
        return redirect(url_for("pages.welcome"))
    return render_template("login.html", current_user=None, activity_mode=is_discord_activity_request())


@bp.route("/auth/discord")
def auth_discord():
    config_error = oauth_config_error()
    if config_error:
        return render_template("login.html", error=config_error, current_user=None), 500
    redirect_uri = oauth_redirect_uri()
    params = urlencode({
        "client_id": DISCORD_CLIENT_ID,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": "identify guilds"
    })
    return redirect(f"{API_BASE_URL}/oauth2/authorize?{params}")


@bp.route("/callback")
def callback():
    config_error = oauth_config_error()
    if config_error:
        return render_template("login.html", error=config_error, current_user=None), 500
    error_code = request.args.get("error")
    if error_code:
        description = request.args.get("error_description") or error_code
        return render_template("login.html", error=f"Discord 登录被取消或失败: {description}", current_user=None), 400
    code = request.args.get("code")
    if not code:
        return render_template("login.html", error="Discord 没有返回授权 code，请重新登录。", current_user=None), 400

    ok, payload = exchange_discord_code(code, redirect_uri=oauth_redirect_uri(), source="web_oauth")
    if not ok:
        return render_template("login.html", error=payload["error"], current_user=None), 429 if payload.get("rate_limited") else 400

    servers = get_servers_for_user(payload["user"]["id"])
    if servers:
        get_display_user()  # demo 用户在此分配假身份并写入 session
    if len(servers) == 1:
        session["server_id"] = servers[0]["server_id"]
        return redirect(url_for("pages.index"))
    if len(servers) > 1:
        return redirect(url_for("pages.servers"))
    return redirect(url_for("pages.welcome"))


@bp.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("pages.welcome"))
