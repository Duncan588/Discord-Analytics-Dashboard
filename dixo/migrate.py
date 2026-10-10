"""One-shot, additive production migration (never deletes or rewrites existing rows).

    python -m dixo.migrate                # all servers registered in portal.db
    python -m dixo.migrate --guild <gid>  # one server
    python -m dixo.migrate --dry-run      # only print the plan + EXPLAIN checks

Steps per server DB:
 1. read-path indexes (CREATE INDEX IF NOT EXISTS; each costs disk ≈ 2-4% of the DB, minutes on 5 GB):
      idx_msg_author_ts   messages(author_id, timestamp)    profile message tab / exports
      idx_msg_thread_ts   messages(thread_id, timestamp)    thread first-post / AI thread API
      idx_react_msg_emoji reactions(message_id, emoji_name) per-message emoji badges
 2. summary_dirty table + 2 AFTER DELETE triggers (tiny)
 3. copy web_visitors/profile_views into visitors.db (old tables kept)
 4. build summary.db (resumable; ~5 min per 5M messages on one core)
 5. optional --search: build search.db FTS index (needed only for /api/ai/v1/.../search)
Run `PRAGMA integrity_check` / table counts before & after as usual (.ai hard rule 3).
"""
import argparse
import os
import shutil
import sqlite3
import sys
import time

from . import search, summary, visitors

INDEXES = [
    ("idx_msg_author_ts", "messages(author_id, timestamp)"),
    ("idx_msg_thread_ts", "messages(thread_id, timestamp)"),
    ("idx_react_msg_emoji", "reactions(message_id, emoji_name)"),
]


def server_dbs(portal_path, guild=None):
    conn = sqlite3.connect(portal_path)
    try:
        rows = conn.execute("SELECT server_id, db_path FROM servers").fetchall()
    finally:
        conn.close()
    base = os.path.dirname(os.path.abspath(portal_path))
    out = []
    for sid, path in rows:
        if guild and str(sid) != str(guild):
            continue
        cand = path if path and os.path.exists(path) else os.path.join(base, "servers", str(sid), "discord_data.db")
        if os.path.exists(cand):
            out.append((str(sid), cand))
    return out


def migrate_db(path, dry_run=False, with_search=False):
    print(f"== {path}  ({os.path.getsize(path) / 1e9:.2f} GB, free disk {shutil.disk_usage(os.path.dirname(path)).free / 1e9:.1f} GB)")
    conn = sqlite3.connect(path, timeout=120)
    conn.execute("PRAGMA busy_timeout=120000")
    have = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='index'")}
    for name, spec in INDEXES:
        if name in have:
            print(f"   index {name}: exists")
            continue
        print(f"   index {name}: {'WOULD CREATE' if dry_run else 'creating'} ON {spec}")
        if not dry_run:
            t = time.time(); conn.execute(f"CREATE INDEX IF NOT EXISTS {name} ON {spec}"); conn.commit()
            print(f"      done in {time.time() - t:.1f}s")
    if dry_run:
        conn.close(); return
    summary.ensure_dirty_trigger(conn); conn.commit(); conn.close()
    visitors.connect(path).close()
    print("   visitors.db ready")
    while True:
        res = summary.refresh(path, budget_seconds=120)
        print("   summary", res, flush=True)
        if res["caught_up"]:
            break
    if with_search:
        while True:
            res = search.refresh(path, budget_seconds=120)
            print("   search", res, flush=True)
            if res["caught_up"]:
                break


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--portal", default=os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "portal.db"))
    ap.add_argument("--guild"); ap.add_argument("--dry-run", action="store_true"); ap.add_argument("--search", action="store_true")
    a = ap.parse_args(argv)
    for sid, path in server_dbs(a.portal, a.guild):
        migrate_db(path, a.dry_run, a.search)
    return 0


if __name__ == "__main__":
    sys.exit(main())
