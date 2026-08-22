"""portal.db 中用户资料/在场记录的共用写入逻辑。

网站登录、服务器数据同步和白名单机器人都会写入同一批用户行，写法必须一致：
非空字段覆盖旧值，空值保留旧值，避免机器人或同步任务把 OAuth 登录时拿到的
头像、昵称清掉。
"""
from .timeutil import utc_now_iso

_UPSERT_PORTAL_USER_SQL = """
INSERT INTO portal_users(user_id,username,nickname,avatar_url,last_login)
VALUES(?,?,?,?,?)
ON CONFLICT(user_id) DO UPDATE SET
    username=CASE WHEN COALESCE(excluded.username,'')!='' THEN excluded.username ELSE portal_users.username END,
    nickname=CASE WHEN COALESCE(excluded.nickname,'')!='' THEN excluded.nickname ELSE portal_users.nickname END,
    avatar_url=CASE WHEN COALESCE(excluded.avatar_url,'')!='' THEN excluded.avatar_url ELSE portal_users.avatar_url END,
    last_login=COALESCE(excluded.last_login,portal_users.last_login)
"""

_UPSERT_PRESENCE_SQL = """
INSERT INTO user_server_presence(user_id,server_id,first_seen,last_seen)
VALUES(?,?,?,?) ON CONFLICT(user_id,server_id) DO UPDATE SET last_seen=excluded.last_seen
"""


def upsert_portal_user(conn, user_id, username=None, nickname=None, avatar_url=None, last_login=None):
    """写入门户用户资料；last_login 为 None 时保留原有登录时间。"""
    uid = str(user_id)
    conn.execute(
        _UPSERT_PORTAL_USER_SQL,
        (uid, username or uid, nickname or username or uid, avatar_url, last_login),
    )


def touch_user_presence(conn, user_id, server_id, timestamp=None):
    """记录用户在某服务器出现过（首次时间保留，最后一次时间刷新）。"""
    stamp = timestamp or utc_now_iso()
    conn.execute(_UPSERT_PRESENCE_SQL, (str(user_id), str(server_id), stamp, stamp))
