"""Guild membership status per server DB (table member_status in discord_data.db).

in_guild=1 member, 0 left (退群). deleted=1 account deleted (销号; Discord renames to deleted_user_xxx).
Maintained by: gateway on_member_join/remove (whitelist_bot) + daily full resync (cron:
`python -m shared.member_status resync <guild_id>`). Never hides anyone; display-only markers.
"""
import json
import os
import sqlite3
import sys
import time
import urllib.request
from datetime import datetime, timezone

_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

DDL = """CREATE TABLE IF NOT EXISTS member_status(
  user_id TEXT PRIMARY KEY, in_guild INTEGER, joined_at TEXT, left_at TEXT,
  deleted INTEGER DEFAULT 0, last_checked TEXT)"""
DELETED_SQL = "(lower(username) LIKE 'deleted_user%' OR lower(username) LIKE 'deleted user%' OR lower(nickname) LIKE 'deleted user%')"


def now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def db_path(gid):
    return os.path.join(_ROOT, "data", "servers", str(gid), "discord_data.db")


def connect(gid):
    c = sqlite3.connect(db_path(gid), timeout=120)
    c.execute("PRAGMA busy_timeout=120000")
    c.execute(DDL)
    return c


def on_join(gid, uid, joined_at=None):
    if not os.path.exists(db_path(gid)):
        return
    c = connect(gid)
    try:
        c.execute("INSERT INTO member_status(user_id,in_guild,joined_at,left_at,deleted,last_checked) VALUES(?,1,?,NULL,0,?) "
                  "ON CONFLICT(user_id) DO UPDATE SET in_guild=1,joined_at=COALESCE(excluded.joined_at,joined_at),left_at=NULL,last_checked=excluded.last_checked",
                  (str(uid), joined_at, now()))
        c.commit()
    finally:
        c.close()


def on_leave(gid, uid):
    if not os.path.exists(db_path(gid)):
        return
    c = connect(gid)
    try:
        t = now()
        c.execute("INSERT INTO member_status(user_id,in_guild,left_at,deleted,last_checked) VALUES(?,0,?,0,?) "
                  "ON CONFLICT(user_id) DO UPDATE SET in_guild=0,left_at=excluded.left_at,last_checked=excluded.last_checked",
                  (str(uid), t, t))
        c.commit()
    finally:
        c.close()


def fetch_members(gid, token):
    out, after = {}, "0"
    while True:
        req = urllib.request.Request(f"https://discord.com/api/v10/guilds/{gid}/members?limit=1000&after={after}",
                                     headers={"Authorization": f"Bot {token}", "User-Agent": "DiscordBot (dixo, 1.0)"})
        for attempt in range(5):
            try:
                with urllib.request.urlopen(req, timeout=30) as r:
                    page = json.load(r)
                break
            except urllib.error.HTTPError as e:
                if e.code == 429:
                    time.sleep(float(json.loads(e.read() or b"{}").get("retry_after", 2)) + 0.5); continue
                raise
        else:
            raise RuntimeError("rate limited")
        for m in page:
            out[str(m["user"]["id"])] = m.get("joined_at")
        if len(page) < 1000:
            return out
        after = page[-1]["user"]["id"]
        time.sleep(0.5)


def resync(gid, token):
    members = fetch_members(gid, token)
    if len(members) < 10:
        raise RuntimeError(f"suspicious member list size {len(members)}; aborting")
    c = connect(gid)
    t = now()
    try:
        prev = {r[0]: r[1] for r in c.execute("SELECT user_id,in_guild FROM member_status")}
        known = [r[0] for r in c.execute("SELECT user_id FROM users")]
        deleted = {r[0] for r in c.execute(f"SELECT user_id FROM users WHERE {DELETED_SQL}")}
        rows = []
        for uid, j in members.items():
            rows.append((uid, 1, j, None, 1 if uid in deleted else 0, t))
        for uid in known:
            if uid in members:
                continue
            # left_at only known when we observe the transition (was in guild before); backfill leaves it NULL
            left = t if prev.get(uid) == 1 else None
            rows.append((uid, 0, None, left, 1 if uid in deleted else 0, t))
        c.executemany("INSERT INTO member_status(user_id,in_guild,joined_at,left_at,deleted,last_checked) VALUES(?,?,?,?,?,?) "
                      "ON CONFLICT(user_id) DO UPDATE SET in_guild=excluded.in_guild,"
                      "joined_at=COALESCE(excluded.joined_at,joined_at),"
                      "left_at=CASE WHEN excluded.in_guild=1 THEN NULL ELSE COALESCE(left_at,excluded.left_at) END,"
                      "deleted=excluded.deleted,last_checked=excluded.last_checked", rows)
        c.commit()
        stat = c.execute("SELECT SUM(in_guild=1),SUM(in_guild=0 AND deleted=0),SUM(deleted=1) FROM member_status "
                         "WHERE user_id IN (SELECT user_id FROM users)").fetchone()
        return {"guild_members": len(members), "db_users_in_guild": stat[0], "left": stat[1], "deleted": stat[2]}
    finally:
        c.close()


def status_map(conn, ids):
    """{uid: 'deleted'|'left'|'member'} for ids that have a row. conn: any connection to discord_data.db."""
    ids = [str(i) for i in ids if str(i).isdigit()]
    out = {}
    if not ids:
        return out
    try:
        for i in range(0, len(ids), 500):
            ch = ids[i:i + 500]; ph = ",".join("?" * len(ch))
            for r in conn.execute(f"SELECT user_id,in_guild,deleted,joined_at,left_at FROM main.member_status WHERE user_id IN ({ph})", ch):
                out[str(r[0])] = {"status": "deleted" if r[2] else ("member" if r[1] else "left"),
                                  "joined_at": r[3], "left_at": r[4]}
        # deleted accounts not yet synced: detect by name
        miss = [i for i in ids if i not in out]
        for i in range(0, len(miss), 500):
            ch = miss[i:i + 500]; ph = ",".join("?" * len(ch))
            for r in conn.execute(f"SELECT user_id FROM main.users WHERE user_id IN ({ph}) AND {DELETED_SQL}", ch):
                out[str(r[0])] = {"status": "deleted", "joined_at": None, "left_at": None}
    except sqlite3.Error:
        pass
    return out


LABELS = {"deleted": "已销号", "left": "已退群", "member": "在群"}


def _env():
    p = os.path.join(_ROOT, ".env")
    for raw in open(p, encoding="utf-8") if os.path.exists(p) else []:
        line = raw.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip("'\""))


if __name__ == "__main__":
    _env()
    if len(sys.argv) >= 3 and sys.argv[1] == "resync":
        print(json.dumps(resync(sys.argv[2], os.environ["DISCORD_BOT_TOKEN"]), ensure_ascii=False))
    else:
        print("usage: python -m shared.member_status resync <guild_id>")
