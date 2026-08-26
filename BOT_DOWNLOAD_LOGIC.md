# Discord 机器人下载与运行逻辑

## 角色

- `DISCORD_BOT_TOKEN` 是主程序机器人令牌，负责网站功能、成员同步、任务完成/失败通知，以及任务开始/继续时向任务创建者发送私信。
- 下载配置中的账号令牌来自 `download_bots` 和 `download_config_bots`，并由 `download_bots.token_type` 标记为 `bot` 或 `user`。Bot Token 使用 `Bot` Authorization；User Token 使用 Discord 原始 Authorization，并通过 REST 扫描 Forum。任务启动时，下载器会并行验证所有已选令牌；验证失败的账号会写入 `discord_downloader.log`，不会静默计入下载池。
- 每个有效下载机器人对应一个独立 Worker。Worker 使用自己的令牌启动 DiscordChatExporter（DCE），每个进程按小批次处理多个 Forum 帖子，并按帖子 ID 分别输出文件。

## 任务流程

1. 下载器从 `download_tasks` 取出任务，并读取任务配置中的全部下载机器人。
2. 所有机器人先进行 HTTP 登录校验。至少一个机器人成功才会继续任务。
3. 下载器使用 `DISCORD_BOT_TOKEN` 向任务创建者发送“开始下载”或“继续下载”私信；通知失败不会阻断下载。
4. `scan_completed=0` 时，第一台有效机器人专职扫描 Forum；其他有效机器人立即从中央队列下载已发现的帖子。
5. 扫描每完成一页就保存 `scan_cursor`。扫描成功后将 `scan_completed` 设为 `1`，清空游标，然后把扫描机器人加入下载池。
6. `scan_completed=1` 的继续任务不会重新扫描，只把 `download_task_items.status='pending'` 的帖子重新放入队列。
7. `scan_completed=0` 的继续任务会从 `scan_cursor` 继续扫描；没有游标时才从扫描起点开始。
8. 每个帖子下载成功后立即导入服务器 SQLite，并更新任务项目状态和使用的机器人名称。
9. 任务处于 `pending` 或 `running` 时，管理后台可以加入尚未配置的下载账号；下载器会在轮询发现配置变化后校验账号并启动新的 Worker。

## Worker 守护逻辑

- 扫描期间，除扫描机器人外的所有有效机器人都必须保持 Worker 在线，即使暂时没有队列任务也不能退出。
- 扫描完成后，所有有效机器人都可以消费中央队列。
- 主循环会检查 Worker；发现 Worker 因未预期异常退出时，在仍有未完成队列时自动重新启动对应机器人。
- Flask 主进程会看守下载器子进程；下载器意外退出而数据库仍有活动任务时，会自动重新拉起，重新启动时会从持久化状态恢复。
- 暂停或取消任务会让 Worker 停止当前处理；继续任务只恢复未完成项目，不重复下载已完成项目。

## 状态字段

| 字段 | 含义 |
| --- | --- |
| `scan_completed` | 持久化扫描完成标记，`1` 表示可以跳过扫描 |
| `scan_cursor` | Forum 历史分页断点，用于未完成扫描的续扫 |
| `scan_finished_at` | 扫描完成时间，供展示和审计使用 |
| `download_task_items.status` | 帖子级状态：`pending`、`downloaded`、`skipped` 或 `failed` |
| `download_task_items.bot_name` | 最后成功处理该帖子的下载机器人 |

## 通知

- 开始/继续：下载器直接使用 `DISCORD_BOT_TOKEN` 调用 Discord API 创建私信并发送。
- 完成/失败：主程序机器人后台任务读取 `download_tasks.notified_at`，向任务创建者发送结果并标记已通知。
