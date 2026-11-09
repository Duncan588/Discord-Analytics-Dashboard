#!/bin/bash
# jojo-incr-v2 (2026-11-09): small DBs -> restic (chunk dedupe); discord_data.db -> INCREMENTAL only
# (new/changed/deleted rows via backup_changelog triggers, zstd -19) -> restic --tag discord-incr.
# Full discord_data baselines are NOT automatic (traffic): run once by hand with FULL_BASELINE=1.
# Every step runs under /root/bin/stallwatch.py: killed + reported if no CPU/IO progress for 10s
# (no total runtime cap). A failed step is reported (DM to ADMIN_IDS) and the rest still runs.
# Secrets: /root/.config/r2-backup.env, /root/.config/restic-r2.pass (chmod 600). Never printed.
set -Eeuo pipefail
BASE=/root/v2/V20
STAGE=/var/tmp/db-backup-stage
INCR_STAGE=/var/tmp/db-incr-stage
LOG=/var/log/db-backup.log
LOCAL_INCR=/var/backups/db-incr          # local copy of every incremental file (tiny)
FIRST_SINCE="${FIRST_SINCE:-2026-11-09T00:00:00}"   # first export window (overlaps baseline 7569311b)
PY=/root/v2/venv/bin/python
STALL=10
export PATH=/usr/local/bin:/usr/bin:/bin
export HOME="${HOME:-/root}"
export XDG_CACHE_HOME="${XDG_CACHE_HOME:-/root/.cache}"

exec 9>/run/db-backup.lock
flock -n 9 || { echo "$(date -Is) another backup is running, skip" >>"$LOG"; exit 0; }

notify_fail() {
  local msg="$1"
  /root/bin/stallwatch.py -s "$STALL" -- /root/v2/venv/bin/python - "$msg" <<'PY' || true
import json, sys, urllib.request
env = {}
for line in open('/root/v2/V20/.env', encoding='utf-8'):
    if '=' in line and not line.lstrip().startswith('#'):
        k, v = line.split('=', 1); env[k.strip()] = v.strip().strip('"\'')
tok = env.get('DISCORD_BOT_TOKEN', '')
ids = [x.strip() for x in env.get('ADMIN_IDS', '').split(',') if x.strip()]
def call(path, data):
    req = urllib.request.Request('https://discord.com/api/v10' + path, data=json.dumps(data).encode(), method='POST',
        headers={'Authorization': 'Bot ' + tok, 'Content-Type': 'application/json', 'User-Agent': 'DiscordBot (db-backup,1)'})
    with urllib.request.urlopen(req, timeout=20) as r: return json.load(r)
for uid in ids:
    try:
        ch = call('/users/@me/channels', {'recipient_id': uid})
        call(f"/channels/{ch['id']}/messages", {'content': '⚠️ 数据库备份失败 (db-backup.sh)\n' + sys.argv[1][:1800]})
    except Exception as e:
        print('notify failed', uid, e, file=sys.stderr)
PY
}
on_err() {
  local rc=$? line=$1
  echo "$(date -Is) FAILED rc=$rc line=$line" >>"$LOG"
  notify_fail "host $(hostname) rc=$rc line=$line
$(tail -n 15 "$LOG" | cut -c1-200)"
  exit $rc
}
trap 'on_err $LINENO' ERR
trap 'rm -rf "$STAGE" "$INCR_STAGE"' EXIT

FAILS=""
# step NAME CMD...: run under the stall watchdog, log rc + duration, never abort the script.
step() {
  local name="$1"; shift
  local t0 rc=0; t0=$(date +%s)
  echo "$(date -Is) step $name begin" >>"$LOG"
  /root/bin/stallwatch.py -s "$STALL" -- "$@" >>"$LOG" 2>&1 || rc=$?
  echo "$(date -Is) step $name rc=$rc $(( $(date +%s) - t0 ))s" >>"$LOG"
  if [ "$rc" -ne 0 ]; then
    [ "$rc" -eq 124 ] && FAILS+="$name: 卡死(${STALL}s 无进展)被杀; " || FAILS+="$name: rc=$rc; "
  fi
  return "$rc"
}

[ -r /root/.config/r2-backup.env ] || { echo "missing /root/.config/r2-backup.env" >>"$LOG"; false; }
set -a; . /root/.config/r2-backup.env; set +a
[ -n "${AWS_ACCESS_KEY_ID:-}" ] && [ -n "${AWS_SECRET_ACCESS_KEY:-}" ] || { echo "R2 credentials not set" >>"$LOG"; false; }
export RESTIC_PASSWORD_FILE="${RESTIC_PASSWORD_FILE:-/root/.config/restic-r2.pass}"

start=$(date +%s)
echo "$(date -Is) start jojo-incr-v2 FULL_BASELINE=${FULL_BASELINE:-0}" >>"$LOG"
rm -rf "$STAGE" "$INCR_STAGE"; mkdir -p "$STAGE" "$INCR_STAGE" "$LOCAL_INCR"; chmod 700 "$STAGE" "$INCR_STAGE" "$LOCAL_INCR"

step unlock restic unlock || true

# ---------------------------------------------------------------- small DBs (unchanged logic)
snap() {
  local src="$1" dst="$2"
  mkdir -p "$(dirname "$dst")"
  step "snap:${src#$BASE/}" sqlite3 "$src" ".timeout 10000" ".backup '$dst'" || true
}
[ -f "$BASE/blocklist.db" ] && snap "$BASE/blocklist.db" "$STAGE/blocklist.db"
if [ "${SKIP_PORTAL:-0}" != "1" ] && [ -f "$BASE/data/portal.db" ]; then snap "$BASE/data/portal.db" "$STAGE/portal.db"; fi
[ -f "$BASE/data/lottery.db" ] && snap "$BASE/data/lottery.db" "$STAGE/lottery.db"
shopt -s nullglob
for d in "$BASE"/data/servers/*/; do
  gid=$(basename "$d")
  for f in summary.db visitors.db; do
    [ -f "$d$f" ] && snap "$d$f" "$STAGE/servers/$gid/$f"
  done
done
if [ -n "$(find "$STAGE" -type f 2>/dev/null | head -1)" ]; then
  step restic-small-dbs restic backup --host prod-v20 --tag small-dbs "$STAGE" || true
  rm -rf "$STAGE"; mkdir -p "$STAGE"; chmod 700 "$STAGE"
else
  FAILS+="small-dbs: nothing staged; "
fi

# ---------------------------------------------------------------- discord_data.db: incremental only
exported=()
for d in "$BASE"/data/servers/*/; do
  gid=$(basename "$d"); db="${d}discord_data.db"
  [ -f "$db" ] || continue
  step "install:$gid" "$PY" /root/bin/db-incr.py install "$db" || continue
  since=(); [ -f "/root/.local/state/db-incr/$gid.json" ] || since=(--since "$FIRST_SINCE")
  if step "export:$gid" nice -n 19 "$PY" /root/bin/db-incr.py export "$db" "$INCR_STAGE/$gid" "${since[@]}"; then
    exported+=("$gid")
  fi
done
if [ "${#exported[@]}" -gt 0 ]; then
  if step restic-discord-incr restic backup --host prod-v20 --tag discord-incr "$INCR_STAGE"; then
    for gid in "${exported[@]}"; do
      mkdir -p "$LOCAL_INCR/$gid"; cp -p "$INCR_STAGE/$gid"/*.zst "$LOCAL_INCR/$gid/"
      step "commit:$gid" "$PY" /root/bin/db-incr.py commit "$BASE/data/servers/$gid/discord_data.db" || true
    done
  fi   # upload failed -> state not committed -> next run re-exports the same range
fi

# ---------------------------------------------------------------- manual full baseline (FULL_BASELINE=1 only)
if [ "${FULL_BASELINE:-0}" = "1" ]; then
  for d in "$BASE"/data/servers/*/; do
    gid=$(basename "$d"); [ -f "${d}discord_data.db" ] || continue
    mkdir -p "$STAGE/servers/$gid"
    step "baseline-copy:$gid" "$PY" -c 'import sqlite3,sys
s=sqlite3.connect("file:"+sys.argv[1]+"?mode=ro",uri=True,timeout=10); d=sqlite3.connect(sys.argv[2])
s.backup(d,pages=-1); d.close(); s.close()' "${d}discord_data.db" "$STAGE/servers/$gid/discord_data.db" &&
    step "baseline-upload:$gid" restic backup --host prod-v20 --tag discord-data --tag baseline "$STAGE/servers/$gid/discord_data.db" || true
    rm -f "$STAGE/servers/$gid/discord_data.db"
  done
fi

# baseline-tagged full snapshots and all discord-incr snapshots are never forgotten (incremental chain).
step forget-prune restic forget --host prod-v20 --keep-daily 7 --keep-weekly 4 --keep-monthly 3 \
  --keep-tag baseline --keep-tag discord-incr --prune || true
if [ "$(date +%u)" = 7 ] || [ "${FORCE_CHECK:-0}" = 1 ]; then
  step check restic check --read-data-subset=5% || true
fi

if [ -n "$FAILS" ]; then
  echo "$(date -Is) FAILED steps: $FAILS" >>"$LOG"
  notify_fail "host $(hostname) 失败步骤: $FAILS
$(tail -n 12 "$LOG" | cut -c1-200)"
  exit 1
fi
echo "$(date -Is) ok in $(( $(date +%s) - start ))s" >>"$LOG"
