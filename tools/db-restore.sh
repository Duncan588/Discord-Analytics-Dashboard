#!/bin/bash
# Restore Discord Analytics DBs from Cloudflare R2 (restic).
# Invoked by Admin (level-1) via dixo.r2_backup.start_restore — runs outside v20 so
# stopping the app does not kill the restore. Secrets: /root/.config/r2-backup.env + restic-r2.pass.
#
# Env (written to /run/db-restore.env by the Admin API):
#   RESTORE_SMALL_ID   required restic short/long id for tag small-dbs
#   RESTORE_DATA_IDS   optional comma-separated discord-data snapshot ids
#   RESTORE_DRY_RUN    1 = list/verify only, do not stop services or write DBs
#   RESTORE_SAFETY     1 = copy current portal.db (+ blocklist) under /var/tmp/pre-restore-<ts>
#   RESTORE_INCLUDE_DATA 1 = also restore companion discord-data dumps (default 1)
set -Eeuo pipefail
BASE="${DIXO_BASE:-/root/v2/V20}"
STAGE=/var/tmp/db-restore-stage
STATUS=/run/db-restore-status.json
LOG=/var/log/db-restore.log
LOCK=/run/db-restore.lock
export PATH=/usr/local/bin:/usr/bin:/bin

exec 9>"$LOCK"
flock -n 9 || { echo "$(date -Is) another restore is running, skip" >>"$LOG"; exit 0; }

ts() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }
write_status() {
  local phase="$1" msg="$2" ok="${3:-}"
  python3 - "$STATUS" "$phase" "$msg" "$ok" <<'PY'
import json, sys, os, time
path, phase, msg, ok = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
data = {
  "phase": phase,
  "message": msg,
  "ok": (True if ok == "1" else False if ok == "0" else None),
  "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
  "pid": os.getpid(),
}
# preserve job fields if present
try:
  prev = json.load(open(path, encoding="utf-8"))
  for k in ("small_id", "data_ids", "dry_run", "safety", "started_at", "requested_by"):
    if k in prev and k not in data:
      data[k] = prev[k]
  if "started_at" in prev:
    data["started_at"] = prev["started_at"]
except Exception:
  pass
tmp = path + ".tmp"
with open(tmp, "w", encoding="utf-8") as f:
  json.dump(data, f, ensure_ascii=False)
os.replace(tmp, path)
PY
}

on_err() {
  local rc=$? line=$1
  echo "$(date -Is) FAILED rc=$rc line=$line" >>"$LOG"
  write_status "failed" "恢复失败（见 /var/log/db-restore.log line=$line rc=$rc）" 0
  # best-effort restart app if we stopped it
  systemctl start v20.service >/dev/null 2>&1 || true
  exit $rc
}
trap 'on_err $LINENO' ERR

[ -r /root/.config/r2-backup.env ] || { echo "missing r2-backup.env" >>"$LOG"; false; }
set -a; . /root/.config/r2-backup.env; set +a
[ -n "${AWS_ACCESS_KEY_ID:-}" ] && [ -n "${AWS_SECRET_ACCESS_KEY:-}" ] || { echo "R2 credentials not set" >>"$LOG"; false; }

# Load job env if present
if [ -f /run/db-restore.env ]; then
  set -a; . /run/db-restore.env; set +a
fi

SMALL_ID="${RESTORE_SMALL_ID:-}"
DRY="${RESTORE_DRY_RUN:-0}"
SAFETY="${RESTORE_SAFETY:-1}"
INCLUDE_DATA="${RESTORE_INCLUDE_DATA:-1}"
DATA_IDS_RAW="${RESTORE_DATA_IDS:-}"

echo "$(date -Is) restore start small=$SMALL_ID dry=$DRY safety=$SAFETY data=$DATA_IDS_RAW" >>"$LOG"
[ -n "$SMALL_ID" ] || { write_status "failed" "缺少 RESTORE_SMALL_ID" 0; exit 1; }

write_status "checking" "校验快照 $SMALL_ID …"

# Verify snapshot exists and looks like small-dbs
meta=$(restic snapshots "$SMALL_ID" --json 2>>"$LOG")
python3 - "$meta" <<'PY'
import json,sys
snaps=json.loads(sys.argv[1] or "[]")
if not snaps: raise SystemExit("snapshot not found")
s=snaps[0] if isinstance(snaps,list) else snaps
tags=set(s.get("tags") or [])
if "small-dbs" not in tags and "discord-data" in tags:
    raise SystemExit("所选快照是 discord-data；请选择 small-dbs 快照作为主恢复点")
print("ok", s.get("short_id"), ",".join(sorted(tags)))
PY

# List files (also used as dry-run proof)
write_status "listing" "列出快照文件…"
restic ls "$SMALL_ID" >>"$LOG" 2>&1

if [ "$DRY" = "1" ]; then
  if [ "$INCLUDE_DATA" = "1" ] && [ -n "$DATA_IDS_RAW" ]; then
    IFS=',' read -ra IDS <<< "$DATA_IDS_RAW"
    for id in "${IDS[@]}"; do
      id=$(echo "$id" | tr -d ' ')
      [ -z "$id" ] && continue
      echo "$(date -Is) dry-run ls discord-data $id" >>"$LOG"
      restic ls "$id" >>"$LOG" 2>&1 || true
    done
  fi
  write_status "dry_ok" "演练成功：快照可读，未改动生产库" 1
  echo "$(date -Is) dry-run ok" >>"$LOG"
  exit 0
fi

# Safety local copy of small DBs (not full multi-GB discord_data)
if [ "$SAFETY" = "1" ]; then
  write="/var/tmp/pre-restore-$(date +%Y%m%d%H%M%S)"
  write_status "safety" "写入本地安全快照 $SAFE …"
  mkdir -p "$SAFE/servers"
  sqlite3 "$BASE/data/portal.db" ".timeout 120000" ".backup '$SAFE/portal.db'"
  [ -f "$BASE/blocklist.db" ] && sqlite3 "$BASE/blocklist.db" ".timeout 120000" ".backup '$SAFE/blocklist.db'" || true
  for d in "$BASE"/data/servers/*/; do
    gid=$(basename "$d")
    for f in summary.db visitors.db; do
      [ -f "$d$f" ] && mkdir -p "$SAFE/servers/$gid" && sqlite3 "$d$f" ".timeout 120000" ".backup '$SAFE/servers/$gid/$f'" || true
    done
  done
  echo "$(date -Is) safety snapshot at $SAFE" >>"$LOG"
fi

write_status "stopping" "停止 v20（网站将短暂下线）…"
systemctl stop v20.service
# children may linger briefly
pkill -f '/root/v2/V20/Preparation_Before_Use/discord_downloader.py' >/dev/null 2>&1 || true
pkill -f '/root/v2/V20/Preparation_Before_Use/whitelist_bot.py' >/dev/null 2>&1 || true
sleep 2

write_status "restoring_small" "恢复 portal / summary / visitors …"
rm -rf "$STAGE"
mkdir -p "$STAGE"
# Snapshot paths are absolute (/var/tmp/db-backup-stage/...); restore into STAGE then remap.
restic restore "$SMALL_ID" --target "$STAGE" >>"$LOG" 2>&1

# Find restored portal.db wherever restic placed it
PORTAL_SRC=$(find "$STAGE" -type f -name 'portal.db' | head -n1)
[ -n "$PORTAL_SRC" ] || { echo "portal.db missing in restore" >>"$LOG"; false; }
STAGE_ROOT=$(dirname "$PORTAL_SRC")
# If path ends with .../db-backup-stage, use that; if portal is directly under a folder, use it
if [ -d "$STAGE_ROOT/servers" ] || [ -f "$STAGE_ROOT/portal.db" ]; then
  :
else
  STAGE_ROOT=$(dirname "$STAGE_ROOT")
fi

mkdir -p "$BASE/data"
cp -f "$PORTAL_SRC" "$BASE/data/portal.db"
# clear WAL sidecars so SQLite opens the restored main file cleanly
rm -f "$BASE/data/portal.db-wal" "$BASE/data/portal.db-shm"
if [ -f "$STAGE_ROOT/blocklist.db" ]; then
  cp -f "$STAGE_ROOT/blocklist.db" "$BASE/blocklist.db"
  rm -f "$BASE/blocklist.db-wal" "$BASE/blocklist.db-shm"
fi
if [ -d "$STAGE_ROOT/servers" ]; then
  for d in "$STAGE_ROOT"/servers/*/; do
    gid=$(basename "$d")
    mkdir -p "$BASE/data/servers/$gid"
    for f in summary.db visitors.db; do
      if [ -f "$d$f" ]; then
        cp -f "$d$f" "$BASE/data/servers/$gid/$f"
        rm -f "$BASE/data/servers/$gid/$f-wal" "$BASE/data/servers/$gid/$f-shm"
      fi
    done
  done
fi

sqlite3 "$BASE/data/portal.db" "PRAGMA integrity_check;" | grep -qx ok

if [ "$INCLUDE_DATA" = "1" ] && [ -n "$DATA_IDS_RAW" ]; then
  write_status "restoring_data" "恢复 discord_data 转储…"
  IFS=',' read -ra IDS <<< "$DATA_IDS_RAW"
  for id in "${IDS[@]}"; do
    id=$(echo "$id" | tr -d ' ')
    [ -z "$id" ] && continue
    # discover sql filename inside snapshot
    fname=$(restic ls "$id" --json 2>/dev/null | python3 -c 'import json,sys
for line in sys.stdin:
  try: o=json.loads(line)
  except Exception: continue
  n=o.get("name") or ""
  if n.endswith(".sql") or "discord_data" in n:
    print(o.get("path") or n); break
' || true)
    if [ -z "$fname" ]; then
      # fallback non-json ls
      fname=$(restic ls "$id" 2>/dev/null | awk '/discord_data/ {print $NF; exit}')
    fi
    [ -n "$fname" ] || { echo "no sql in snapshot $id" >>"$LOG"; false; }
    gid=$(echo "$fname" | sed -n 's/.*discord_data-\([0-9][0-9]*\)\.sql.*/\1/p')
    [ -n "$gid" ] || { echo "cannot parse guild id from $fname" >>"$LOG"; false; }
    mkdir -p "$BASE/data/servers/$gid"
    tmpdb="$BASE/data/servers/$gid/discord_data.db.restoring"
    rm -f "$tmpdb"
    echo "$(date -Is) restoring discord_data guild=$gid from $id path=$fname" >>"$LOG"
    restic dump "$id" "$fname" | sqlite3 "$tmpdb"
    sqlite3 "$tmpdb" "PRAGMA integrity_check;" | grep -qx ok
    mv -f "$tmpdb" "$BASE/data/servers/$gid/discord_data.db"
    rm -f "$BASE/data/servers/$gid/discord_data.db-wal" "$BASE/data/servers/$gid/discord_data.db-shm"
  done
fi

write_status "starting" "启动 v20…"
systemctl start v20.service
sleep 2
systemctl is-active --quiet v20.service

write_status "done" "恢复完成 small=$SMALL_ID" 1
echo "$(date -Is) restore ok small=$SMALL_ID" >>"$LOG"
rm -rf "$STAGE"
