"""Discord Embedded Activity handshake endpoints (moved verbatim from app.py; endpoint = "activity.<function name>")."""
from flask import Blueprint

from dixo import core as _core

# Route bodies were written against app.py's module globals; expose the same names here.
globals().update({k: v for k, v in vars(_core).items() if not k.startswith("__")})

bp = Blueprint("activity", __name__)


@bp.route("/api/activity/token", methods=["POST"])
def activity_token():
    """Discord 活动模式（Activities / 小活动）专用登录接口。

    网页在 Discord 客户端内以 iframe 形式运行时，不能走普通的整页跳转 OAuth，
    而是用 Embedded App SDK 的 discordSdk.commands.authorize() 拿到一次性 code，
    再由前端 POST 到这个接口，在服务器端（可以安全使用 client_secret）换取
    access_token 并写入 Flask session，实现"无需在浏览器打开也能登录"。

    参考：static/js/discord-activity.js
    """
    config_error = oauth_config_error()
    if config_error:
        return jsonify({"ok": False, "error": config_error}), 500

    body = request.get_json(silent=True) or {}
    code = (body.get("code") or "").strip()
    if not code:
        app.logger.warning("activity.token.invalid request_id=%s reason=missing_code", _request_id())
        return jsonify({"ok": False, "error": "缺少 code 参数"}), 400

    # 活动模式下的 authorize() 不是重定向流程，不传 redirect_uri。
    app.logger.info("activity.token.start request_id=%s", _request_id())
    # Activity 登录只需确认 Discord User ID。服务器数据权限由门户本地
    # 数据库判断；不要像浏览器 OAuth 一样再请求完整 guild 列表。
    ok, payload = exchange_discord_code(
        code, redirect_uri=None, source="activity", fetch_guilds=False
    )
    if not ok:
        app.logger.warning(
            "activity.token.failed request_id=%s rate_limited=%s stage=%s",
            _request_id(), payload.get("rate_limited", False), payload.get("stage", "unknown"),
        )
        result = {"ok": False, "error": payload["error"]}
        for key in ("rate_limited", "retry_after", "global", "stage"):
            if key in payload:
                result[key] = payload[key]
        return jsonify(result), 429 if payload.get("rate_limited") else 400

    app.logger.info("activity.token.success request_id=%s discord_user_id=%s", _request_id(), payload["user"].get("id", "-"))
    return jsonify({
        "ok": True,
        "user": payload["user"],
        # 前端需要这个 access_token 调用 discordSdk.commands.authenticate()，
        # 完成 Embedded App SDK 侧的认证握手；仅在响应中一次性返回，不落库。
        "access_token": payload["access_token"],
    })


@bp.route("/api/activity/status")
def activity_status():
    """确认 Activity 代理是否实际保存了 Flask session cookie。"""
    user = session.get("user")
    authenticated = bool(user and user.get("id"))
    app.logger.info(
        "activity.status request_id=%s authenticated=%s discord_user_id=%s",
        _request_id(), authenticated, user.get("id", "-") if authenticated else "-",
    )
    return jsonify({"ok": True, "authenticated": authenticated})


@bp.route("/api/activity/log", methods=["POST"])
def activity_log():
    """接收 Activity 前端的脱敏诊断事件，便于 Discord iframe 内排障。"""
    if request.content_length and request.content_length > 16 * 1024:
        return jsonify({"ok": False, "error": "日志请求过大"}), 413
    body = request.get_json(silent=True) or {}
    event = str(body.get("event") or "unknown")[:80]
    if not re.fullmatch(r"[A-Za-z0-9_.:-]+", event):
        event = "invalid_event"
    client_request_id = str(body.get("request_id") or "-")[:80]
    if not re.fullmatch(r"[A-Za-z0-9._:-]{1,80}", client_request_id):
        client_request_id = "-"
    details = body.get("details")
    if not isinstance(details, dict):
        details = {"value": str(details)[:500]} if details is not None else {}
    # 前端只应发送诊断字段；截断并序列化后再写入日志，避免日志被任意内容撑爆。
    try:
        details_text = json.dumps(details, ensure_ascii=False, separators=(",", ":"))[:2000]
    except (TypeError, ValueError):
        details_text = "{}"
    app.logger.info(
        "activity.client event=%s request_id=%s client_request_id=%s details=%s",
        event, _request_id(), client_request_id, details_text,
    )
    return jsonify({"ok": True})
