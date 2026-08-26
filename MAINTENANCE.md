# V30 项目维护文档（AI 读取版）

> 本文件供 AI 助手快速理解项目结构与运行方式。项目路径：`E:\Discord-Analytics-Dashboard-main\V30 - 副本`

---

## 1. 项目是什么

**Discord Analytics Dashboard** —— 基于 Flask 的 Discord 论坛数据分析平台：

- Flask Web 看板（服务器概览、排行榜、用户主页、词云等）
- 多机器人并发下载器（调用外部工具 **DiscordChatExporter / DCE** 抓取 Forum 帖子）
- SQLite 存储（`data/portal.db` 主库 + `data/servers/` 每服务器独立库）
- 白名单机器人（兼收藏指令 `/favorites` `/top` `/top30`）
- Discord OAuth2 登录 + 常规用户自动授权（ID 出现在已采集数据即自动放行）
- 可选 Discord Activity 内嵌模式

## 2. 目录结构

```text
V30 - 副本/
├── app.py                             # 主入口（约2900行）：Flask 应用，自动拉起子进程
├── requirements.txt                   # Flask>=3.1, requests, ijson, PyNaCl, discord.py>=2.4, aiosqlite
├── README.md                          # 项目介绍（中英双语）
├── BOT_DOWNLOAD_LOGIC.md              # 下载任务状态机与通知机制（中文，权威说明）
├── MAINTENANCE.md                     # 本文件：AI 维护文档
├── .env                               # 环境变量（含令牌，勿提交/勿外传）
├── shared/                            # 公共工具模块
│   ├── env.py                         #   读取 .env
│   ├── portal.py                      #   portal.db 用户/在线状态写入
│   ├── discord_api.py                 #   Discord REST API 封装
│   ├── sqlite_utils.py                #   SQLite 连接工具
│   ├── timeutil.py                    #   时间处理（统一入口）
│   └── task_timing.py                 #   任务耗时统计
├── Preparation_Before_Use/
│   ├── whitelist_bot.py               # 白名单+收藏机器人（discord.py，~1200行，app.py 自动拉起）
│   ├── discord_downloader.py          # 多机器人下载器 Worker（~1550行，有任务时自动拉起）
│   ├── all_threads.py                 # 旧版脚本
│   ├── clean_json.py                  # 旧版脚本
│   ├── merge_script.py                # 旧版脚本
│   ├── discordDB.py                   # 旧版脚本
│   └── backup_2025.py                 # 旧版备份脚本
├── templates/                         # Jinja2 网页模板
├── static/                            # 本地托管前端资源（Tailwind/Chart.js/Alpine.js/TagCloud/Discord SDK）
├── DiscordChatExporter.Cli.win-x64/   # DCE 可执行文件（外部依赖，Windows x64 版）
└── data/
    ├── portal.db (+ -wal / -shm)      # 主数据库（WAL 模式）
    ├── servers/                       # 各服务器导出数据入库
    ├── logs/app.log                   # 应用日志（轮转保留5份，不记令牌）
    ├── cache/                         # 缓存
    └── uploads/                       # 上传文件
```

## 3. 进程模型（重要）

`app.py` 是**父进程**，负责看守并拉起子进程：

1. **whitelist_bot**：`DISCORD_BOT_TOKEN` 非空即由 app.py 用 `subprocess.Popen` 启动（app.py 约 L2804）。
2. **discord_downloader**：仅当数据库存在活动下载任务时启动（L2453、L2810），意外退出且仍有活动任务时自动重新拉起。
3. 子进程使用 `sys.executable`（必须用项目 venv 的 Python 启动 app.py，否则缺依赖）。
4. Windows 下以 `CREATE_NEW_PROCESS_GROUP` 创建。

## 4. 关键环境变量（.env）

| 变量 | 用途 |
| --- | --- |
| `DISCORD_CLIENT_ID` / `DISCORD_CLIENT_SECRET` | OAuth2 登录 |
| `ADMIN_IDS` | 管理员 User ID（逗号分隔） |
| `DISCORD_BOT_TOKEN` | 白名单/主程序机器人 |
| `DISCORD_DOWNLOADER_TOKEN` | 默认下载机器人 |
| `PUBLIC_BASE_URL` | 生产 HTTPS 域名（Activity 模式必需） |
| `DOWNLOAD_MAX_CONCURRENT_TASKS` | 并发下载任务数 |
| `DCE_*`（MARKDOWN / PROCESS_POLL_MS / LOG_COMMAND / BATCH_SIZE） | DCE 调优 |

自定义下载账号存于 DB 表 `download_bots`（`token_type` 为 bot/user）。User Token 自动化违反 Discord ToS，生产建议 Bot Token。

## 5. 下载任务状态机（详见 BOT_DOWNLOAD_LOGIC.md）

- 任务表 `download_tasks`；帖子级表 `download_task_items.status ∈ {pending, downloaded, skipped, failed}`，含 `bot_name`。
- 字段：`scan_completed`（1=跳过扫描）、`scan_cursor`（续扫断点）、`scan_finished_at`。
- 第一台有效机器人扫描 Forum，其余并行消费中央队列；扫描完成后全员入池。
- 每帖下载成功立即导入该服务器 SQLite 并更新状态 → 支持暂停/继续不重复下载。
- 开始/继续通知由下载器直接调 API 发私信；完成/失败由主程序读 `notified_at` 发送。

## 6. 运行方式

```bash
python -m venv venv
venv\Scripts\pip install -r requirements.txt
venv\Scripts\python app.py        # 不要用系统 Python 直接跑
```

单独调试：
```bash
venv\Scripts\python Preparation_Before_Use\whitelist_bot.py
venv\Scripts\python Preparation_Before_Use\discord_downloader.py
```

## 7. 已知问题排查

### discord.py 反复 "session has been invalidated" + ClientConnectionResetError
日志形态：identify 时 websocket transport 已关闭 → 重连循环。
**（2026-08-25 已修复：根因是开发者后台未开启 SERVER MEMBERS INTENT，开启后恢复正常。）**
常见原因按优先级排查：
1. **令牌无效/已重置**——`DISCORD_BOT_TOKEN` 错误或已在开发者平台重置。验证：`curl -H "Authorization: Bot <token>" https://discord.com/api/v10/users/@me`
2. **网络被阻断**——中国大陆网络下 `gateway.discord.gg` 直连不稳定，需为进程设置代理（`HTTPS_PROXY=http://127.0.0.1:port`），或在代码中给 `bot.run()` 传 `proxy=`。
3. **多实例抢登录同一 token**——旧进程未退出导致会话互踢（本机出现两个 python 进程同时跑 whitelist_bot 时典型）。检查：`tasklist | findstr python`。
4. **IP 被 Discord 风控**——换出口 IP 或稍后重试。

### PrivilegedIntentsRequired
`whitelist_bot.py` 请求了 `intents.members = True`，必须在 Discord 开发者后台 -> Bot -> Privileged Gateway Intents 打开 **SERVER MEMBERS INTENT** 才能启动。（2026-08-25 已在后台开启，问题已解决。）

## 9. 权限自检机制（2026-08-25 加入）

位置：`whitelist_bot.py` 中 `Bot.check_permissions()`，在每次 `on_ready` 时自动运行。

行为：
1. 遍历所有已加入服务器，检查机器人角色是否具备 `view_channel` / `read_message_history` / `send_messages`；缺失时写 WARNING 日志（格式 `权限不足 guild=... 缺少: ...`）。
2. 用 `guild.fetch_members(limit=2)` 实测 Members Intent 是否真正生效；收到 `Forbidden` 说明 SERVER MEMBERS INTENT 未在后台启用，写 ERROR 日志。
3. 所有发现的问题汇总后通过私信发送给 `.env` 中 `ADMIN_IDS` 的每一位管理员；私信失败只记 debug 日志，不影响运行。

排查时先看 `data/logs/app.log` 中的「权限不足」「Members Intent」「权限自检」关键字。

### 其他
- 页面空白 / ModuleNotFoundError flask → 未用 venv Python 启动 app.py。
- 下载卡住 → 看 `data/logs/` 与下载器日志；确认 DCE 可执行文件在 `DiscordChatExporter.Cli.win-x64/`。

## 8. 维护注意事项

- 所有时间处理统一走 `shared/timeutil.py`。
- 日志自动轮转保留 5 份，禁止向日志写入任何 token。
- 数据库 WAL 模式（portal.db-wal/shm 属正常现象，勿手工删）。
- 修改下载逻辑前先阅读 `BOT_DOWNLOAD_LOGIC.md`，状态字段是持久化契约。
