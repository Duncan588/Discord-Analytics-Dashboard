"""'Still here?' liveness checks for inactive members (portal.db: liveness_checks).

Flow: admin page enqueues rows (status=queued) -> whitelist_bot sends the DM with a
persistent '我还在' button (status=pending, deadline=+7d, or dm_failed) -> click or
/alive marks alive (and removes the inactive role if it was added) -> after the
deadline the bot adds the inactive role (status=expired) and DMs a notice.
"""
import secrets
from datetime import datetime, timedelta, timezone

LIVENESS_GUILD_ID = 915249444721668096
LIVENESS_ROLE_ID = 1425661375472074884
DEADLINE_DAYS = 7
SEND_INTERVAL_SEC = 3.0          # >= 3s between DMs (~20/min) to stay well under DM rate limits
STATUSES = ("queued", "pending", "alive", "expired", "dm_failed")
OPEN_STATUSES = ("queued", "pending")

DDL = """
CREATE TABLE IF NOT EXISTS liveness_checks(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  guild_id TEXT NOT NULL,
  user_id TEXT NOT NULL,
  token TEXT NOT NULL UNIQUE,
  status TEXT NOT NULL DEFAULT 'queued',
  tag TEXT,
  last_message_at TEXT,
  created_by TEXT,
  created_at TEXT NOT NULL,
  sent_at TEXT,
  deadline TEXT,
  clicked_at TEXT,
  role_added_at TEXT,
  role_removed_at TEXT,
  error TEXT
);
CREATE INDEX IF NOT EXISTS idx_liveness_user ON liveness_checks(user_id, status);
CREATE INDEX IF NOT EXISTS idx_liveness_status ON liveness_checks(status, deadline);
"""


def now_iso(now=None):
    return (now or datetime.now(timezone.utc)).isoformat()


def ensure(conn):
    conn.executescript(DDL)


def _parse(ts):
    if not ts:
        return None
    try:
        d = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def months_since(last_ts, now=None):
    d = _parse(last_ts)
    if d is None:
        return None
    now = now or datetime.now(timezone.utc)
    return max(0, (now.year - d.year) * 12 + now.month - d.month - (1 if now.day < d.day else 0))


def enqueue(conn, guild_id, users, created_by, now=None):
    """users: iterable of dict(user_id, tag, last_message_at, is_bot). Returns (created, skipped)."""
    ensure(conn)
    created, skipped = 0, []
    for u in users:
        uid = str(u["user_id"])
        if u.get("is_bot"):
            skipped.append((uid, "bot")); continue
        open_row = conn.execute(
            "SELECT 1 FROM liveness_checks WHERE user_id=? AND guild_id=? AND status IN ('queued','pending') LIMIT 1",
            (uid, str(guild_id))).fetchone()
        if open_row:
            skipped.append((uid, "pending")); continue
        conn.execute(
            "INSERT INTO liveness_checks(guild_id,user_id,token,status,tag,last_message_at,created_by,created_at) "
            "VALUES(?,?,?,?,?,?,?,?)",
            (str(guild_id), uid, secrets.token_hex(12), "queued", u.get("tag"), u.get("last_message_at"),
             str(created_by or ""), now_iso(now)))
        created += 1
    conn.commit()
    return created, skipped


def dm_text(last_message_at, guild_name="服务器", now=None):
    g = guild_name
    n = months_since(last_message_at, now)
    d = _parse(last_message_at)
    if d is None:
        head = f"我们发现你在服务器「{g}」里几乎没有发言记录。"
    elif n and n >= 1:
        head = f"我们发现你已经 {n} 个月没有在服务器「{g}」发言了（最后一次发言：{d.astimezone(timezone(timedelta(hours=8))):%Y-%m-%d}）。"
    else:
        head = f"我们发现你最近很少在服务器「{g}」发言（最后一次发言：{d.astimezone(timezone(timedelta(hours=8))):%Y-%m-%d}）。"
    return (f"👋 **你还在吗？**\n{head}\n\n"
            f"如果你还在，请在 {DEADLINE_DAYS} 天内点击下方「我还在」按钮。\n"
            f"超过期限未确认，你会被移入「不活跃」身份组；之后随时可以点按钮或在服务器里使用 /alive 命令恢复。")


EXPIRED_TEXT = ("⏰ 你在 {days} 天内没有确认「我还在」，已被移入不活跃身份组。\n"
                "如果你还在，随时点击之前私信里的「我还在」按钮，或在服务器里使用 /alive 命令即可移除该身份组。")


def mark_sent(conn, check_id, now=None):
    now = now or datetime.now(timezone.utc)
    conn.execute("UPDATE liveness_checks SET status='pending', sent_at=?, deadline=?, error=NULL WHERE id=?",
                 (now_iso(now), now_iso(now + timedelta(days=DEADLINE_DAYS)), check_id))
    conn.commit()


def mark_failed(conn, check_id, error):
    conn.execute("UPDATE liveness_checks SET status='dm_failed', error=? WHERE id=?", (str(error)[:300], check_id))
    conn.commit()


def due_for_expiry(conn, now=None):
    return conn.execute("SELECT * FROM liveness_checks WHERE status='pending' AND deadline<=? ORDER BY id LIMIT 50",
                        (now_iso(now),)).fetchall()


def mark_expired(conn, check_id, role_added, error=None, now=None):
    conn.execute("UPDATE liveness_checks SET status='expired', role_added_at=?, error=? WHERE id=?",
                 (now_iso(now) if role_added else None, error, check_id))
    conn.commit()


def mark_alive(conn, user_id, guild_id, token=None, role_removed=False, now=None):
    """Mark open/expired checks of the user alive. Returns rows updated."""
    ts = now_iso(now)
    q = ("UPDATE liveness_checks SET status='alive', clicked_at=COALESCE(clicked_at,?), "
         "role_removed_at=CASE WHEN ? AND role_added_at IS NOT NULL THEN ? ELSE role_removed_at END "
         "WHERE user_id=? AND guild_id=? AND status IN ('queued','pending','expired')")
    args = [ts, 1 if role_removed else 0, ts, str(user_id), str(guild_id)]
    n = conn.execute(q, args).rowcount
    conn.commit()
    return n


def by_token(conn, token):
    return conn.execute("SELECT * FROM liveness_checks WHERE token=?", (token,)).fetchone()
