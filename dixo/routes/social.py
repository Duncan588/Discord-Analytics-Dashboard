"""Web APIs for block / unblock and thread favorites (portal.db)."""
from flask import Blueprint, jsonify, request

from dixo import core as _core
from dixo import social

globals().update({k: v for k, v in vars(_core).items() if not k.startswith("__")})

bp = Blueprint("social", __name__)


def _body():
    data = {}
    if request.form:
        data.update(request.form.to_dict())
    if request.is_json:
        data.update(request.get_json(silent=True) or {})
    return data


def _wants_json():
    return (
        request.path.startswith("/api/")
        or request.headers.get("X-Requested-With") == "XMLHttpRequest"
        or request.accept_mimetypes.best == "application/json"
        or request.is_json
    )


def _respond(ok, error=None, next_url=None, **extra):
    if _wants_json():
        payload = {"ok": ok, **extra}
        if error:
            payload["error"] = error
        return jsonify(payload), (200 if ok else 400)
    from flask import redirect, url_for
    return redirect(next_url or url_for("pages.index"))


@bp.route("/api/social/block", methods=["POST"])
@login_required
def api_block():
    social.ensure_tables(get_portal_db())
    body = _body()
    target = str(body.get("user_id") or "").strip()
    if not target.isdigit():
        return _respond(False, "invalid_user")
    me = session["user"]["id"]
    ok, code = social.block_user(get_portal_db(), me, target, utc_now_iso())
    return _respond(ok, None if ok else code, body.get("next") or url_for("pages.user_profile", user_id=target),
                    blocked=True, user_id=target)


@bp.route("/api/social/unblock", methods=["POST"])
@login_required
def api_unblock():
    social.ensure_tables(get_portal_db())
    body = _body()
    target = str(body.get("user_id") or "").strip()
    if not target:
        return _respond(False, "invalid_user")
    me = session["user"]["id"]
    done = social.unblock_user(get_portal_db(), me, target)
    return _respond(done, None if done else "not_blocked",
                    body.get("next") or url_for("pages.user_profile", user_id=me),
                    blocked=False, user_id=target)


@bp.route("/api/social/favorite", methods=["POST"])
@login_required
@server_required
def api_favorite():
    social.ensure_tables(get_portal_db())
    body = _body()
    tid = str(body.get("thread_id") or "").strip()
    gid = str(body.get("guild_id") or current_server_id() or "").strip()
    if not tid or not gid:
        return _respond(False, "invalid_thread")
    social.add_favorite(get_portal_db(), session["user"]["id"], gid, tid, utc_now_iso())
    return _respond(True, None, body.get("next") or url_for("pages.user_profile", user_id=session["user"]["id"]),
                    favorited=True, thread_id=tid, guild_id=gid)


@bp.route("/api/social/unfavorite", methods=["POST"])
@login_required
def api_unfavorite():
    social.ensure_tables(get_portal_db())
    body = _body()
    tid = str(body.get("thread_id") or "").strip()
    gid = str(body.get("guild_id") or "").strip() or None
    if not tid:
        return _respond(False, "invalid_thread")
    done = social.remove_favorite(get_portal_db(), session["user"]["id"], tid, gid)
    return _respond(done, None if done else "not_favorited",
                    body.get("next") or url_for("pages.user_profile", user_id=session["user"]["id"]),
                    favorited=False, thread_id=tid)
