"""Fair Discord lottery stored in data/lottery.db (beside portal.db).

Participation is opt-in. Eligibility is read from the guild analytics
databases and is not decided here by a cooldown or a per-user cap.

Audit hash: SHA-256 hex of the UTF-8 bytes of the participant user IDs,
sorted by integer value ascending, joined with ``\\n`` and no trailing
newline. Winners are a ``secrets.SystemRandom`` sample of that list,
without replacement.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
import sqlite3
from datetime import datetime, timedelta, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SHANGHAI = timezone(timedelta(hours=8))

RECENT_DAYS = 30
MIN_HISTORY = 1000
MAX_WINNERS = 40
MAX_TITLE = 80
MAX_PRIZE = 500
MAX_HORIZON_DAYS = 366

OPEN, DRAWN, CANCELLED, INSUFFICIENT = "open", "drawn", "cancelled", "insufficient"

RULE = "近 30 天内有发言，或历史消息数 ≥ 1000"
ELIGIBILITY_FAIL = "你暂不符合参与条件。需要满足以下任一条件：近 30 天内有发言，或历史消息数达到 1000 条。"
NO_DB = "这个服务器还没有消息分析库，暂时无法核对参与条件。"
UNAVAILABLE = "暂时无法读取消息记录，请稍后再试。"
BOT_REJECT = "机器人不能参与抽奖。"
MISSING = "这场抽奖不存在或已失效。"
ONLY_DRAW = "只有发起人可以提前开奖。"
ONLY_CANCEL = "只有发起人可以取消抽奖。"
CANCELLED_OK = "已取消这场抽奖。"
CANCELLED_STATE = "这场抽奖已取消，不再接受参与。"
DRAWN_EARLY_OK = "已提前开奖。中奖名单见原消息，也可使用 /抽奖结果。"
OPEN_DESC = "点「参与」加入，点「退出」离开。到点自动开奖，发起人也可以提前开奖或取消。"
DRAWN_DESC = "已开奖。"
TIME_FORMAT = "开奖时间格式无法识别。请用北京时间，例如 2026-11-06 21:00。"
TIME_PAST = "开奖时间必须晚于现在。请用北京时间，例如 2026-11-06 21:00。"
TIME_FAR = "开奖时间不能超过 366 天。"
TITLE_BAD = "标题不能为空，且不超过 80 个字。"
PRIZE_BAD = "奖品内容不能为空，且不超过 500 个字。"
WINNERS_BAD = "中奖人数需要在 1 到 40 之间。"
GUILD_ONLY = "请在服务器频道里使用 /抽奖，不要在私信里发起。"
ENDED = "这场抽奖已经结束。"
NOT_IN = "你不在参与名单里。"
MODE_AUTO = "到点自动开奖"
MODE_EARLY = "发起人提前开奖"
HASH_NOTE = "名单按用户 ID 升序拼接后取 SHA-256"
INSUFFICIENT_STAYS_TAIL = "抽奖仍会继续，到点若人数仍不足将自动结束。"

BTN_JOIN, BTN_LEAVE = "参与", "退出"
BTN_DRAW, BTN_CANCEL = "提前开奖", "取消抽奖"
BTN_CONFIRM_DRAW, BTN_CONFIRM_CANCEL = "确认开奖", "确认取消"

FIELD_PRIZE = "奖品"
FIELD_WINNER_COUNT = "中奖人数"
FIELD_WHEN = "开奖时间"
FIELD_HOST = "发起人"
FIELD_COUNT = "当前参与"
FIELD_RULE = "条件"
FIELD_WINNERS = "中奖名单"
FIELD_HASH = "名单校验"
FIELD_MODE = "开奖方式"

COLOR_OPEN = 0x5865F2
COLOR_DRAWN = 0x57F287
COLOR_CANCELLED = 0x99AAB5
COLOR_INSUFFICIENT = 0xFEE75C

_DDL = """
CREATE TABLE IF NOT EXISTS lotteries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    guild_id TEXT NOT NULL,
    channel_id TEXT NOT NULL,
    message_id TEXT,
    creator_id TEXT NOT NULL,
    creator_name TEXT,
    title TEXT NOT NULL,
    prize TEXT NOT NULL,
    winner_count INTEGER NOT NULL,
    draw_at TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    closed_at TEXT,
    draw_mode TEXT,
    participants_hash TEXT,
    winners_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_lottery_due ON lotteries(status, draw_at);
CREATE TABLE IF NOT EXISTS lottery_entries (
    lottery_id INTEGER NOT NULL REFERENCES lotteries(id) ON DELETE CASCADE,
    user_id TEXT NOT NULL,
    username TEXT,
    joined_at TEXT NOT NULL,
    PRIMARY KEY (lottery_id, user_id)
);
"""


class LotteryError(ValueError):
    pass


class TimeParseError(LotteryError):
    pass


class Eligibility:
    def __init__(self, ok, reason, msg_count=0, recent=False):
        self.ok = ok
        self.reason = reason
        self.msg_count = msg_count
        self.recent = recent


def default_db_path():
    return os.path.join(ROOT, "data", "lottery.db")


def servers_dir(root=None):
    return os.path.join(root or ROOT, "data", "servers")


def utc_now():
    return datetime.now(timezone.utc)


def utc_stamp(dt=None):
    dt = dt or utc_now()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00")


def connect(path=None):
    path = path or default_db_path()
    parent = os.path.dirname(os.path.abspath(path))
    if parent:
        os.makedirs(parent, exist_ok=True)
    conn = sqlite3.connect(path, timeout=60, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=60000")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA cache_size=-2000")
    conn.executescript(_DDL)
    return conn


class _Tx:
    def __init__(self, conn):
        self.conn = conn
        self.done = False

    def __enter__(self):
        self.conn.execute("BEGIN IMMEDIATE")
        return self

    def finish(self, commit):
        if self.done:
            return
        self.conn.execute("COMMIT" if commit else "ROLLBACK")
        self.done = True

    def __exit__(self, exc_type, exc, tb):
        if not self.done:
            self.conn.execute("ROLLBACK" if exc_type else "COMMIT")
            self.done = True
        return False


def _id_key(value):
    text = str(value)
    if text.isdigit():
        return (0, int(text))
    return (1, text)


def participants_hash(user_ids):
    ids = sorted({str(i) for i in user_ids}, key=_id_key)
    return hashlib.sha256("\n".join(ids).encode("utf-8")).hexdigest()


def pick_winners(user_ids, k, rng=None):
    """Sample ``k`` ids without replacement. Default RNG is SystemRandom."""
    pool = sorted({str(i) for i in user_ids}, key=_id_key)
    if k < 1 or len(pool) < k:
        raise LotteryError("insufficient")
    rng = secrets.SystemRandom() if rng is None else rng
    winners = list(rng.sample(pool, k))
    if len(winners) != len(set(winners)) or len(winners) != k:
        raise LotteryError("draw failed")
    if not set(winners) <= set(pool):
        raise LotteryError("draw failed")
    return winners


def parse_draw_at(text, now):
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    now = now.astimezone(timezone.utc)
    raw = (text or "").strip().replace("：", ":").replace("／", "/")
    raw = re.sub(r"\s+", " ", raw)
    for suffix in ("（北京时间）", "(北京时间)"):
        if raw.endswith(suffix):
            raw = raw[: -len(suffix)].strip()
    if not raw:
        raise TimeParseError(TIME_FORMAT)
    if re.fullmatch(r"\d{10}|\d{13}", raw):
        seconds = int(raw) / (1000.0 if len(raw) == 13 else 1.0)
        dt = datetime.fromtimestamp(seconds, timezone.utc)
    else:
        dt = _parse_clock(raw)
    dt = dt.astimezone(timezone.utc).replace(microsecond=0)
    if dt <= now:
        raise TimeParseError(TIME_PAST)
    if dt > now + timedelta(days=MAX_HORIZON_DAYS):
        raise TimeParseError(TIME_FAR)
    return dt


def _parse_clock(raw):
    candidate = raw.replace("/", "-")
    if candidate.endswith("Z"):
        candidate = candidate[:-1] + "+00:00"
    if re.search(r"(?:Z|[+-]\d{2}:?\d{2})$", raw) or re.search(r"[+-]\d{2}:?\d{2}$", candidate):
        try:
            parsed = datetime.fromisoformat(candidate.replace(" ", "T"))
        except ValueError as exc:
            raise TimeParseError(TIME_FORMAT) from exc
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=SHANGHAI)
        return parsed
    naive = candidate.replace("T", " ")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(naive, fmt).replace(tzinfo=SHANGHAI)
        except ValueError:
            continue
    raise TimeParseError(TIME_FORMAT)


def format_when(stamp):
    try:
        dt = datetime.strptime(str(stamp), "%Y-%m-%dT%H:%M:%S+00:00").replace(tzinfo=timezone.utc)
    except ValueError:
        return str(stamp)
    local = dt.astimezone(SHANGHAI)
    return f"{local.strftime('%Y-%m-%d %H:%M')}（北京时间） · <t:{int(dt.timestamp())}:F>"


def _clean_title(title):
    return " ".join((title or "").replace("\n", " ").split())


def _validate_fields(title, prize, winner_count):
    title = _clean_title(title)
    prize = (prize or "").strip()
    if not title or len(title) > MAX_TITLE:
        raise LotteryError(TITLE_BAD)
    if not prize or len(prize) > MAX_PRIZE:
        raise LotteryError(PRIZE_BAD)
    if isinstance(winner_count, bool) or not isinstance(winner_count, int) or not 1 <= winner_count <= MAX_WINNERS:
        raise LotteryError(WINNERS_BAD)
    return title, prize, winner_count


def _count(conn, lottery_id):
    return int(conn.execute("SELECT COUNT(*) FROM lottery_entries WHERE lottery_id=?", (lottery_id,)).fetchone()[0])


def _participant_ids(conn, lottery_id):
    rows = conn.execute("SELECT user_id FROM lottery_entries WHERE lottery_id=?", (lottery_id,)).fetchall()
    return sorted((str(r["user_id"]) for r in rows), key=_id_key)


def _winners(raw):
    if not raw:
        return []
    try:
        data = json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        return []
    if not isinstance(data, list):
        return []
    return [str(i) for i in data]


def _row_dict(conn, lottery_id):
    row = conn.execute("SELECT * FROM lotteries WHERE id=?", (lottery_id,)).fetchone()
    if row is None:
        return None
    snap = dict(row)
    snap["participant_count"] = _count(conn, lottery_id)
    snap["participant_ids"] = _participant_ids(conn, lottery_id)
    snap["winners"] = _winners(snap.get("winners_json"))
    return snap


def load(db_path, lottery_id):
    conn = connect(db_path)
    try:
        return _row_dict(conn, lottery_id)
    finally:
        conn.close()


def has_entry(conn, lottery_id, user_id):
    row = conn.execute(
        "SELECT 1 FROM lottery_entries WHERE lottery_id=? AND user_id=?",
        (lottery_id, str(user_id)),
    ).fetchone()
    return row is not None


def create_lottery(db_path, guild_id, channel_id, creator_id, creator_name, title, prize, winner_count, draw_at, now=None):
    title, prize, winner_count = _validate_fields(title, prize, winner_count)
    if not isinstance(draw_at, datetime):
        raise LotteryError(TIME_FORMAT)
    now = now or utc_now()
    conn = connect(db_path)
    try:
        cur = conn.execute(
            """INSERT INTO lotteries(
                guild_id, channel_id, creator_id, creator_name, title, prize, winner_count,
                draw_at, status, created_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?)""",
            (
                str(guild_id), str(channel_id), str(creator_id), (creator_name or "")[:80],
                title, prize, winner_count, utc_stamp(draw_at), OPEN, utc_stamp(now),
            ),
        )
        return _row_dict(conn, cur.lastrowid)
    finally:
        conn.close()


def set_message(db_path, lottery_id, message_id):
    conn = connect(db_path)
    try:
        conn.execute("UPDATE lotteries SET message_id=? WHERE id=?", (str(message_id), lottery_id))
    finally:
        conn.close()


def abort_unposted(db_path, lottery_id, now=None):
    now = now or utc_now()
    conn = connect(db_path)
    try:
        with _Tx(conn) as tx:
            row = conn.execute("SELECT status, message_id FROM lotteries WHERE id=?", (lottery_id,)).fetchone()
            if row and row["status"] == OPEN and not row["message_id"]:
                conn.execute(
                    "UPDATE lotteries SET status=?, closed_at=?, draw_mode=? WHERE id=? AND status=?",
                    (CANCELLED, utc_stamp(now), "abort", lottery_id, OPEN),
                )
            tx.finish(True)
    finally:
        conn.close()


def due_ids(db_path, now=None):
    now = now or utc_now()
    conn = connect(db_path)
    try:
        rows = conn.execute(
            "SELECT id FROM lotteries WHERE status=? AND draw_at<=? ORDER BY draw_at, id",
            (OPEN, utc_stamp(now)),
        ).fetchall()
        return [int(r["id"]) for r in rows]
    finally:
        conn.close()


def _closed_text(status, action):
    if status == DRAWN:
        return "这场抽奖已经开奖，不能再参与。" if action == "join" else (
            "这场抽奖已经开奖，不能再退出。" if action == "leave" else "这场抽奖已经开奖。"
        )
    if status == CANCELLED:
        return "这场抽奖已取消，不能再参与。" if action in ("join", "leave") else "这场抽奖已取消。"
    if status == INSUFFICIENT:
        return "这场抽奖已因参与人数不足结束。"
    return ENDED


def joined_text(count):
    return f"已加入抽奖。当前参与：{count} 人。"


def already_text(count):
    return f"你已经在参与名单里。当前参与：{count} 人。"


def left_text(count):
    return f"已退出抽奖。当前参与：{count} 人。"


def insufficient_text(need, count, stays_open):
    text = f"参与人数不足，无法开奖。需要至少 {need} 人，当前 {count} 人。"
    if stays_open:
        text += INSUFFICIENT_STAYS_TAIL
    return text


def confirm_draw_text(count, need):
    text = f"确认现在提前开奖？当前参与 {count} 人，中奖人数 {need}。开奖后不能再加入或退出。"
    if count < need:
        text += "目前人数不足，确认后不会开奖，抽奖会继续。"
    return text


def confirm_cancel_text(count):
    return f"确认取消这场抽奖？取消后不能恢复。当前参与 {count} 人。"


def not_found_text(lottery_id):
    return f"没有找到抽奖 `{lottery_id}`。"


def lookup_banner(snap):
    lid = snap["id"]
    n = snap["participant_count"]
    status = snap["status"]
    if status == OPEN:
        text = f"这场抽奖尚未开奖。当前参与：{n} 人。"
    elif status == CANCELLED:
        text = f"抽奖 `{lid}` 已取消。"
    elif status == INSUFFICIENT:
        text = f"抽奖 `{lid}` 因参与人数不足未能开奖。需要至少 {snap['winner_count']} 人，实际 {n} 人。"
    elif status == DRAWN:
        text = f"抽奖 `{lid}` 的开奖结果："
    else:
        text = f"抽奖 `{lid}` 状态：{status}。"
    url = jump_url(snap)
    if url:
        text += "\n" + url
    return text


def jump_url(snap):
    if not snap.get("message_id"):
        return None
    return f"https://discord.com/channels/{snap['guild_id']}/{snap['channel_id']}/{snap['message_id']}"


def handle_join(db_path, server_dir, lottery_id, user_id, username, is_bot, now=None):
    """``server_dir`` is ``data/servers`` (one subdirectory per guild)."""
    now = now or utc_now()
    conn = connect(db_path)
    try:
        snap = _row_dict(conn, lottery_id)
        if snap is None:
            return {"code": "missing", "text": MISSING, "refresh": False}
        if snap["status"] != OPEN:
            return {"code": "closed", "text": _closed_text(snap["status"], "join"), "refresh": False, "status": snap["status"]}
        if has_entry(conn, lottery_id, user_id):
            return {"code": "already", "text": already_text(snap["participant_count"]), "refresh": False, "count": snap["participant_count"]}
        guild_id = snap["guild_id"]
    finally:
        conn.close()
    if is_bot:
        return {"code": "bot", "text": BOT_REJECT, "refresh": False}
    elig = eligibility(server_dir, guild_id, user_id, now)
    if elig.reason in {"unavailable"}:
        return {"code": "unavailable", "text": UNAVAILABLE, "refresh": False}
    if elig.reason == "no_db":
        return {"code": "no_db", "text": NO_DB, "refresh": False}
    if elig.reason == "bot":
        return {"code": "bot", "text": BOT_REJECT, "refresh": False}
    if not elig.ok:
        return {"code": "ineligible", "text": ELIGIBILITY_FAIL, "refresh": False}
    conn = connect(db_path)
    try:
        result = _try_join(conn, lottery_id, user_id, username, now)
    finally:
        conn.close()
    code = result["code"]
    if code == "joined":
        text = joined_text(result["count"])
    elif code == "already":
        text = already_text(result["count"])
    elif code == "closed":
        text = _closed_text(result.get("status"), "join")
    else:
        text = MISSING
    return {"code": code, "text": text, "refresh": code == "joined", "count": result.get("count")}


def handle_leave(db_path, lottery_id, user_id, now=None):
    now = now or utc_now()
    conn = connect(db_path)
    try:
        result = _try_leave(conn, lottery_id, user_id)
    finally:
        conn.close()
    code = result["code"]
    if code == "left":
        text = left_text(result["count"])
    elif code == "absent":
        text = NOT_IN
    elif code == "closed":
        text = _closed_text(result.get("status"), "leave")
    else:
        text = MISSING
    return {"code": code, "text": text, "refresh": code == "left", "count": result.get("count")}


def preview_action(db_path, lottery_id, actor_id, kind):
    snap = load(db_path, lottery_id)
    if snap is None:
        return {"confirm": False, "text": MISSING}
    if str(actor_id) != str(snap["creator_id"]):
        return {"confirm": False, "text": ONLY_DRAW if kind == "draw" else ONLY_CANCEL}
    if snap["status"] != OPEN:
        action = "draw" if kind == "draw" else "cancel"
        return {"confirm": False, "text": _closed_text(snap["status"], action)}
    if kind == "draw":
        text = confirm_draw_text(snap["participant_count"], snap["winner_count"])
    else:
        text = confirm_cancel_text(snap["participant_count"])
    return {"confirm": True, "text": text}


def commit_draw(db_path, lottery_id, mode, actor_id, now=None, rng=None):
    now = now or utc_now()
    conn = connect(db_path)
    try:
        result = _try_draw(conn, lottery_id, mode, actor_id, now, rng)
    finally:
        conn.close()
    result["text"] = _draw_text(result)
    result["refresh"] = bool(result.get("changed"))
    return result


def commit_cancel(db_path, lottery_id, actor_id, now=None):
    now = now or utc_now()
    conn = connect(db_path)
    try:
        result = _try_cancel(conn, lottery_id, actor_id, now)
    finally:
        conn.close()
    code = result["code"]
    if code == "cancelled":
        text = CANCELLED_OK
    elif code == "forbidden":
        text = ONLY_CANCEL
    elif code == "closed":
        text = _closed_text(result.get("status"), "cancel")
    else:
        text = MISSING
    result["text"] = text
    result["refresh"] = bool(result.get("changed"))
    return result


def _draw_text(result):
    code = result["code"]
    if code == "drawn":
        return DRAWN_EARLY_OK
    if code == "insufficient":
        return insufficient_text(result["need"], result["count"], stays_open=not result.get("changed"))
    if code == "forbidden":
        return ONLY_DRAW
    if code == "closed":
        return _closed_text(result.get("status"), "draw")
    if code == "missing":
        return MISSING
    return ENDED


def _try_join(conn, lottery_id, user_id, username, now):
    with _Tx(conn) as tx:
        row = conn.execute("SELECT status FROM lotteries WHERE id=?", (lottery_id,)).fetchone()
        if row is None:
            tx.finish(False)
            return {"code": "missing", "count": 0, "status": None}
        if row["status"] != OPEN:
            count = _count(conn, lottery_id)
            status = row["status"]
            tx.finish(False)
            return {"code": "closed", "count": count, "status": status}
        try:
            conn.execute(
                "INSERT INTO lottery_entries(lottery_id, user_id, username, joined_at) VALUES(?,?,?,?)",
                (lottery_id, str(user_id), (username or "")[:80], utc_stamp(now)),
            )
            code = "joined"
        except sqlite3.IntegrityError:
            code = "already"
        count = _count(conn, lottery_id)
        tx.finish(True)
        return {"code": code, "count": count, "status": OPEN}


def _try_leave(conn, lottery_id, user_id):
    with _Tx(conn) as tx:
        row = conn.execute("SELECT status FROM lotteries WHERE id=?", (lottery_id,)).fetchone()
        if row is None:
            tx.finish(False)
            return {"code": "missing", "count": 0, "status": None}
        if row["status"] != OPEN:
            tx.finish(False)
            return {"code": "closed", "count": _count(conn, lottery_id), "status": row["status"]}
        deleted = conn.execute(
            "DELETE FROM lottery_entries WHERE lottery_id=? AND user_id=?",
            (lottery_id, str(user_id)),
        ).rowcount
        count = _count(conn, lottery_id)
        tx.finish(True)
        return {"code": "left" if deleted else "absent", "count": count, "status": OPEN}


def _try_cancel(conn, lottery_id, actor_id, now):
    with _Tx(conn) as tx:
        row = conn.execute("SELECT * FROM lotteries WHERE id=?", (lottery_id,)).fetchone()
        if row is None:
            tx.finish(False)
            return {"code": "missing", "changed": False, "status": None}
        if str(actor_id) != str(row["creator_id"]):
            tx.finish(False)
            return {"code": "forbidden", "changed": False, "status": row["status"]}
        if row["status"] != OPEN:
            tx.finish(False)
            return {"code": "closed", "changed": False, "status": row["status"]}
        conn.execute(
            "UPDATE lotteries SET status=?, closed_at=? WHERE id=? AND status=?",
            (CANCELLED, utc_stamp(now), lottery_id, OPEN),
        )
        tx.finish(True)
        return {"code": "cancelled", "changed": True, "status": CANCELLED, "count": _count(conn, lottery_id)}


def _try_draw(conn, lottery_id, mode, actor_id, now, rng):
    with _Tx(conn) as tx:
        row = conn.execute("SELECT * FROM lotteries WHERE id=?", (lottery_id,)).fetchone()
        if row is None:
            tx.finish(False)
            return {"code": "missing", "changed": False, "status": None}
        if mode == "early":
            if actor_id is None or str(actor_id) != str(row["creator_id"]):
                tx.finish(False)
                return {"code": "forbidden", "changed": False, "status": row["status"]}
        elif mode != "auto":
            tx.finish(False)
            raise LotteryError("unknown draw mode")
        if row["status"] != OPEN:
            tx.finish(False)
            return {"code": "closed", "changed": False, "status": row["status"], "need": int(row["winner_count"])}
        ids = _participant_ids(conn, lottery_id)
        need = int(row["winner_count"])
        digest = participants_hash(ids)
        if len(ids) < need:
            if mode == "auto":
                conn.execute(
                    """UPDATE lotteries SET status=?, closed_at=?, draw_mode=?, participants_hash=?, winners_json=?
                       WHERE id=? AND status=?""",
                    (INSUFFICIENT, utc_stamp(now), "auto", digest, "[]", lottery_id, OPEN),
                )
                tx.finish(True)
                return {"code": "insufficient", "changed": True, "status": INSUFFICIENT, "count": len(ids), "need": need, "hash": digest, "winners": [], "mode": mode}
            tx.finish(False)
            return {"code": "insufficient", "changed": False, "status": OPEN, "count": len(ids), "need": need, "mode": mode}
        winners = pick_winners(ids, need, rng)
        conn.execute(
            """UPDATE lotteries SET status=?, closed_at=?, draw_mode=?, participants_hash=?, winners_json=?
               WHERE id=? AND status=?""",
            (DRAWN, utc_stamp(now), mode, digest, json.dumps(winners), lottery_id, OPEN),
        )
        tx.finish(True)
        return {
            "code": "drawn", "changed": True, "status": DRAWN, "count": len(ids), "need": need,
            "hash": digest, "winners": winners, "mode": mode,
        }


def lookup(db_path, lottery_id, guild_id=None):
    snap = load(db_path, lottery_id)
    if snap is None or (guild_id is not None and str(snap["guild_id"]) != str(guild_id)):
        return {"text": not_found_text(lottery_id), "embed": None, "snap": None}
    return {"text": lookup_banner(snap), "embed": panel_payload(snap), "snap": snap}


def audit(db_path, lottery_id):
    snap = load(db_path, lottery_id)
    if snap is None:
        return {"ok": False, "reason": "missing"}
    ids = snap["participant_ids"]
    recomputed = participants_hash(ids)
    winners = snap["winners"]
    winners_ok = len(winners) == len(set(winners)) and set(winners) <= set(ids)
    if snap["status"] == DRAWN:
        hash_ok = snap["participants_hash"] == recomputed and bool(snap["participants_hash"])
        winners_ok = winners_ok and len(winners) == int(snap["winner_count"])
    elif snap["status"] == INSUFFICIENT:
        hash_ok = snap["participants_hash"] == recomputed
        winners_ok = winners == []
    elif snap["status"] == OPEN:
        hash_ok = not snap["participants_hash"] and not winners
    else:
        hash_ok = True
        winners_ok = not winners
    return {
        "ok": hash_ok and winners_ok,
        "hash": recomputed,
        "stored_hash": snap["participants_hash"],
        "winners": winners,
        "participants": ids,
        "status": snap["status"],
    }


def panel_payload(snap):
    status = snap["status"]
    n = int(snap["participant_count"])
    k = int(snap["winner_count"])
    if status == DRAWN:
        description, color = DRAWN_DESC, COLOR_DRAWN
    elif status == CANCELLED:
        description, color = CANCELLED_STATE, COLOR_CANCELLED
    elif status == INSUFFICIENT:
        description = f"参与人数不足，未能开奖。需要至少 {k} 人，当前 {n} 人。"
        color = COLOR_INSUFFICIENT
    else:
        description, color = OPEN_DESC, COLOR_OPEN
    fields = [
        (FIELD_PRIZE, snap["prize"], False),
        (FIELD_WINNER_COUNT, f"{k} 人", True),
        (FIELD_WHEN, format_when(snap["draw_at"]), True),
        (FIELD_HOST, f"<@{snap['creator_id']}>", True),
        (FIELD_COUNT, f"{n} 人", True),
        (FIELD_RULE, RULE, False),
    ]
    if status == DRAWN:
        fields.append((FIELD_WINNERS, _format_winners(snap["winners"]), False))
        fields.append((FIELD_HASH, f"`{snap['participants_hash']}`" if snap.get("participants_hash") else "（无）", False))
        fields.append((FIELD_MODE, MODE_EARLY if snap.get("draw_mode") == "early" else MODE_AUTO, True))
    footer = f"抽奖ID：{snap['id']}"
    if status == DRAWN:
        footer += f" · {HASH_NOTE}"
    return {
        "title": str(snap["title"])[:256],
        "description": description,
        "color": color,
        "fields": [(name, _clip(value), inline) for name, value, inline in fields],
        "footer": footer[:2048],
    }


def _format_winners(ids):
    if not ids:
        return "（无）"
    lines = [f"<@{i}>" for i in ids]
    text = "\n".join(lines)
    if len(text) <= 1024:
        return text
    kept = []
    for line in lines:
        trial = "\n".join(kept + [line, "…"])
        if len(trial) > 1024:
            break
        kept.append(line)
    return "\n".join(kept + ["…"]) if kept else "…"


def _clip(value, limit=1024):
    text = "" if value is None else str(value)
    if len(text) <= limit:
        return text
    return text[: limit - 1] + "…"


def eligibility(server_dir, guild_id, user_id, now=None):
    """True when the last 30 days contain a message, or history >= 1000.

    ``summary.db`` is the analytics aggregate. A negative answer there still
    checks ``discord_data.db``, so a speaker not rolled into the summary yet
    is not rejected. A read error is ``unavailable``, not ineligible.
    """
    now = now or utc_now()
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    gid = str(guild_id)
    uid = str(user_id)
    main = os.path.join(server_dir, gid, "discord_data.db")
    summary = os.path.join(server_dir, gid, "summary.db")
    main_exists = os.path.exists(main)
    summary_exists = os.path.exists(summary)
    if not main_exists and not summary_exists:
        return Eligibility(False, "no_db")
    ids = [uid]
    if main_exists:
        try:
            if _user_is_bot(main, uid):
                return Eligibility(False, "bot")
            ids = _merged_ids(main, uid)
        except sqlite3.Error:
            ids = [uid]
    cutoff = now.astimezone(timezone.utc) - timedelta(days=RECENT_DAYS)
    cutoff_iso = cutoff.strftime("%Y-%m-%dT%H:%M:%S+00:00")
    cutoff_day = cutoff.strftime("%Y-%m-%d")
    summary_state = None
    if summary_exists:
        try:
            summary_state = _summary_activity(summary, ids, cutoff_day)
        except sqlite3.Error:
            summary_state = "error"
        else:
            if summary_state["recent"] or summary_state["total"] >= MIN_HISTORY:
                return Eligibility(True, "recent" if summary_state["recent"] else "history", summary_state["total"], summary_state["recent"])
    if main_exists:
        try:
            live = _live_activity(main, ids, cutoff_iso)
        except sqlite3.Error:
            if summary_state == "error" or summary_state is None:
                return Eligibility(False, "unavailable")
            # Summary said no, but it can lag the live log. Don't reject on a failed live read.
            return Eligibility(False, "unavailable")
        total = live["total"]
        if summary_state and summary_state != "error":
            total = max(total, summary_state["total"])
        if live["recent"] or total >= MIN_HISTORY:
            return Eligibility(True, "recent" if live["recent"] else "history", total, live["recent"])
        return Eligibility(False, "neither", total, False)
    if summary_state == "error":
        return Eligibility(False, "unavailable")
    total = 0 if not summary_state or summary_state == "error" else summary_state["total"]
    return Eligibility(False, "neither", total, False)


def _ro(path):
    conn = sqlite3.connect(path, timeout=30)
    try:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only=ON")
        conn.execute("PRAGMA busy_timeout=30000")
    except sqlite3.Error:
        conn.close()
        raise
    return conn


def _user_is_bot(path, user_id):
    conn = _ro(path)
    try:
        row = conn.execute("SELECT is_bot FROM users WHERE user_id=?", (str(user_id),)).fetchone()
    except sqlite3.OperationalError:
        return False
    finally:
        conn.close()
    if row is None or row["is_bot"] is None:
        return False
    return str(row["is_bot"]).strip().lower() in {"1", "true", "yes"}


def _merged_ids(path, user_id):
    """Same cluster the analytics profile uses: parent plus merged alts."""
    uid = str(user_id)
    conn = _ro(path)
    try:
        rows = conn.execute("SELECT target_id, parent_id FROM user_merges").fetchall()
    except sqlite3.OperationalError:
        return [uid]
    finally:
        conn.close()
    parent_of = {str(r["target_id"]): str(r["parent_id"]) for r in rows}
    root = uid
    seen = set()
    while root in parent_of and root not in seen:
        seen.add(root)
        root = parent_of[root]
    ids = {root}
    for child, parent in parent_of.items():
        if parent == root:
            ids.add(child)
    return sorted(ids, key=_id_key)


def _summary_activity(path, ids, cutoff_day):
    conn = _ro(path)
    try:
        marks = ",".join("?" for _ in ids)
        total = conn.execute(
            f"SELECT COALESCE(SUM(msg_count),0) FROM user_stats WHERE user_id IN ({marks})",
            ids,
        ).fetchone()[0]
        recent = conn.execute(
            f"SELECT COALESCE(SUM(c),0) FROM user_day WHERE user_id IN ({marks}) AND day>=?",
            (*ids, cutoff_day),
        ).fetchone()[0]
    finally:
        conn.close()
    return {"total": int(total or 0), "recent": int(recent or 0) > 0}


def _live_activity(path, ids, cutoff_iso):
    conn = _ro(path)
    try:
        recent = False
        total = 0
        for uid in ids:
            if not recent:
                hit = conn.execute(
                    "SELECT 1 FROM messages WHERE author_id=? AND timestamp>=? LIMIT 1",
                    (uid, cutoff_iso),
                ).fetchone()
                recent = hit is not None
            if total < MIN_HISTORY:
                got = conn.execute(
                    "SELECT COUNT(*) FROM (SELECT 1 FROM messages WHERE author_id=? LIMIT ?)",
                    (uid, MIN_HISTORY - total),
                ).fetchone()[0]
                total += int(got or 0)
            if recent and total >= MIN_HISTORY:
                break
    finally:
        conn.close()
    return {"total": total, "recent": recent}
