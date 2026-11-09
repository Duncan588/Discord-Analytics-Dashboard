# 增量数据库备份（restic → Cloudflare R2）

生产机路径：脚本装在 `/root/bin/`，systemd 单元在 `/etc/systemd/system/`。本目录是仓库里的副本，便于版本管理与恢复演练；部署时复制到上述位置即可。

**原则：不跑定时 DCE，只用 gateway 实时写入。** 备份只读 SQLite / restic，不会触发 DiscordChatExporter。

## 架构

| 数据 | 策略 | restic tag |
| --- | --- | --- |
| 小库（`portal.db`、`blocklist.db`、`lottery.db`、各服 `summary.db` / `visitors.db`） | 每天 `sqlite3 .backup` 后整包上传；restic 内容寻址，重复块不占流量 | `small-dbs` |
| 大库 `data/servers/<gid>/discord_data.db` | **仅增量**：`backup_changelog` 表 + 每表 AFTER INSERT/UPDATE/DELETE 触发器记录变更；`db-incr.py export` 导出变更行的当前版本 + 删除列表 → `zstd -19` 成 `.sqlite.zst` → 上传 | `discord-incr` |
| 大库全量基线 | **不自动跑**（省流量）。手动：`FULL_BASELINE=1 /root/bin/db-backup.sh` | `discord-data` + `baseline` |

配套路径：

- 暂存：`/var/tmp/db-incr-stage`（跑完清）
- 本地增量副本：`/var/backups/db-incr/<gid>/`
- 水位状态：`/root/.local/state/db-incr/<gid>.json`（上传成功后 `commit` 才推进；失败则下次重导同一区间）
- 日志：`/var/log/db-backup.log`
- 密钥（chmod 600，**永不入库**）：`/root/.config/r2-backup.env`、`/root/.config/restic-r2.pass`；失败私信还读 `/root/v2/V20/.env` 里的 `DISCORD_BOT_TOKEN` / `ADMIN_IDS`

`forget --prune` 保留：`--keep-daily 7 --keep-weekly 4 --keep-monthly 3`，并 **永远保留** `--keep-tag baseline`、`--keep-tag discord-incr`。

### 当前基线快照（prod-v20）

| 库 | restic short id | tag |
| --- | --- | --- |
| 大库 `915249444721668096/discord_data.db` | `0f3a1b61` | `baseline` |
| 小库（small-dbs 链上的基线） | `a648c1c3` | `baseline` |

> VACUUM 会改 rowid，增量按 rowid 对齐，**VACUUM 后必须再跑一次 `FULL_BASELINE=1`**。

## 脚本

| 文件 | 作用 |
| --- | --- |
| `db-backup.sh` | 总控（jojo-incr-v2）：小库 → 增量 → 可选全量基线 → forget/prune；每步经 stallwatch |
| `db-incr.py` | `install` / `export` / `commit` |
| `db-incr-restore.py` | 把增量按 seq 叠到基线副本上（先删后 INSERT OR REPLACE，保留原 rowid） |
| `stallwatch.py` | 看门狗：连续 **10 秒** CPU+IO（含网络）无进展就杀进程组并退出 124；**不限总时长** |
| `db-backup.service` | oneshot：`Nice=19`、`IOSchedulingClass=idle`、`CPUWeight=20`、`TimeoutStartSec=4h` |
| `db-backup.timer` | `OnCalendar=*-*-* 04:17:00 UTC`，`RandomizedDelaySec=5m` |

失败步骤会汇总，用 Bot 私信 `ADMIN_IDS`；某步失败不中断后续步骤，整次退出码非 0。

### 环境变量（脚本读，不写死密钥）

```bash
# /root/.config/r2-backup.env （示例，勿提交真实值）
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_DEFAULT_REGION=auto
RESTIC_REPOSITORY=s3:https://<CF_ACCOUNT_ID>.r2.cloudflarestorage.com/<bucket>
RESTIC_PASSWORD_FILE=/root/.config/restic-r2.pass

# 可选运行时
FULL_BASELINE=1   # 仅手动全量基线
FORCE_CHECK=1     # 强制 restic check --read-data-subset=5%
SKIP_PORTAL=1     # 跳过 portal.db
FIRST_SINCE=2026-11-09T00:00:00  # 首次 export 窗口（无水位时）
```

## 部署

```bash
sudo install -m 700 -o root -g root ops/backup/db-backup.sh ops/backup/db-incr.py \
  ops/backup/db-incr-restore.py ops/backup/stallwatch.py /root/bin/
sudo install -m 644 ops/backup/db-backup.service ops/backup/db-backup.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now db-backup.timer
# 首次大库：装触发器 + 确认已有 baseline 快照后，再让 timer 跑增量
# 不要擅自启动备份；需要时由人工执行：
#   sudo systemctl start db-backup.service
#   # 或 FULL_BASELINE=1 sudo /root/bin/db-backup.sh
```

依赖：`restic`、`sqlite3`、`zstd`、项目 venv（`/root/v2/venv/bin/python`）。

## 恢复步骤（大库 discord_data）

Admin 卡片的 `tools/db-restore.sh` 目前覆盖 **small-dbs**（portal / summary / visitors 等）。大库增量链需手工：

```bash
# 0. 加载凭证（勿 echo）
set -a; . /root/.config/r2-backup.env; set +a
export RESTIC_PASSWORD_FILE=/root/.config/restic-r2.pass

GID=915249444721668096
WORKDIR=/var/tmp/discord-restore-$GID
mkdir -p "$WORKDIR" && chmod 700 "$WORKDIR"
cd "$WORKDIR"

# 1. 拉基线（约 4GB 流量；可 ionice/nice）
nice -n 19 ionice -c3 restic restore 0f3a1b61 --target "$WORKDIR/baseline" --include "/servers/$GID/discord_data.db"
# 若快照内路径不同，先：restic ls 0f3a1b61 | head

BASE="$WORKDIR/baseline/servers/$GID/discord_data.db"   # 按 ls 结果调整
test -f "$BASE"

# 2. 按时间拉全部 discord-incr，并按文件名里的 seq 排序
nice -n 19 ionice -c3 restic restore --tag discord-incr --host prod-v20 --target "$WORKDIR/incr"
# 或逐个 snapshot id：restic restore <id> --target "$WORKDIR/incr"

# 3. 解压并叠增量（db-incr-restore 接受 .zst）
mapfile -t INCRS < <(find "$WORKDIR/incr" -name "${GID}-*.sqlite.zst" | sort)
/root/v2/venv/bin/python /root/bin/db-incr-restore.py "$BASE" "${INCRS[@]}"

# 4. 完整性
sqlite3 "$BASE" 'PRAGMA integrity_check;'
# 期望：ok

# 5. 停服务 → 替换（注意 WAL/SHM）→ 启动
sudo systemctl stop v20.service
# 可选安全副本：
#   cp -a /root/v2/V20/data/servers/$GID/discord_data.db* /var/tmp/pre-restore-$(date +%s)/
sudo rm -f /root/v2/V20/data/servers/$GID/discord_data.db-wal \
           /root/v2/V20/data/servers/$GID/discord_data.db-shm
sudo install -m 644 -o root -g root "$BASE" /root/v2/V20/data/servers/$GID/discord_data.db
sudo systemctl start v20.service

# 6. 验证网页 / Admin KPI；确认 gateway 在写：
#   sqlite3 .../discord_data.db 'SELECT count(*), max(id) FROM backup_changelog;'
```

小库恢复仍可用 Admin「☁️ Cloudflare 备份」或 `tools/db-restore.sh`（`RESTORE_SMALL_ID`）。

## 已验证 / 未验证

| 项 | 状态 |
| --- | --- |
| 小库：从 R2 基线 + 增量恢复，14 张表行数与内容与线上一致 | ✅ |
| 大库 `915249444721668096`（约 4.1GB）完整拉回 + 恢复实测 | ❌ 未做（需约 4GB 下载；需人工批准） |
| 圭亚那时间 2026-11-09 18:31–18:48 对旧消息的编辑 | ❌ 触发器安装前，changelog 未覆盖 |

## 限制

- 增量依赖 rowid；**VACUUM / 重建库后必须新基线**。
- 首次 `export`（无 state）用 `--since` / `FIRST_SINCE` 补基线到触发器安装之间的窗口；更早的「只改旧行」仍可能漏。
- 不要无故 `systemctl start db-backup`；流量与 R2 写入都有成本。
