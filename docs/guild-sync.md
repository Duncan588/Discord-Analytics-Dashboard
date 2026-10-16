# 服务器统一同步 / Guild sync

- 表 `guild_sync`：每个启用的 `download_configs` 一行，状态 `backfilling | live | retrying | paused`。
- 执行单元仍是 `download_tasks` 行（run），带 `sync_id`，在任务列表中隐藏；旧任务标记 `archived=1`（不删除）。
- 回填 run：全量扫描归档帖，`scan_cursor` 每页落盘；失败后同一 run 回到 pending，从断点继续（已下载帖子不重下）。
- 增量 run：回填完成后每 `SYNC_LIVE_INTERVAL_SEC` 秒一次，扫描到 `live_stop_at`（上次扫描起点前 1 小时）即停；实时消息由 whitelist_bot 网关写入，网关重连会立即触发一次增量。
- 重试：30s 起指数退避，上限 `SYNC_MAX_BACKOFF_SEC`，无限次；扫描分页对 429/5xx 以及 aiohttp 断线/截断响应会就地重试。下载器主循环崩溃 10 秒后自恢复，服务启动时自动恢复未完成 run。
- 写库：分析库导入在进程内单写者锁下串行，遇到 `database is locked` 退避重试；连接均为 WAL + busy_timeout。
- 通知：同步 run 不再逐次私信；持续失败超过 `SYNC_NOTIFY_FAIL_SEC` 通知一次，回填完成通知一次。
- 接口：`GET /admin/guild-sync/status`，`POST /admin/guild-sync/<id>/(pause|resume|restart)`。
