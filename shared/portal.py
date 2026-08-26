"""Portal database upsert helpers shared by the app and Discord services."""

from .timeutil import utc_now_iso


def upsert_portal_user(conn, user_id, username, nickname=None, avatar_url=None, last_login=None):
    uid = str(user_id)
    name = str(username or uid)
    nick = nickname or name
    conn.execute(
        """INSERT INTO portal_users(user_id,username,nickname,avatar_url,last_login)
           VALUES(?,?,?,?,?)
           ON CONFLICT(user_id) DO UPDATE SET
             username=CASE WHEN excluded.username!='' THEN excluded.username ELSE portal_users.username END,
             nickname=CASE WHEN excluded.nickname!='' THEN excluded.nickname ELSE portal_users.nickname END,
             avatar_url=CASE WHEN excluded.avatar_url IS NOT NULL AND excluded.avatar_url!='' THEN excluded.avatar_url ELSE portal_users.avatar_url END,
             last_login=COALESCE(excluded.last_login, portal_users.last_login)""",
        (uid, name, str(nick), avatar_url, last_login),
    )


def touch_user_presence(conn, user_id, server_id, timestamp=None):
    value = timestamp or utc_now_iso()
    conn.execute(
        """INSERT INTO user_server_presence(user_id,server_id,first_seen,last_seen)
           VALUES(?,?,?,?)
           ON CONFLICT(user_id,server_id) DO UPDATE SET last_seen=excluded.last_seen""",
        (str(user_id), str(server_id), value, value),
    )

