# Discord Analytics Dashboard

> Discord 论坛数据分析、常规用户自动识别与下载管理平台
> A Discord forum data analytics, regular-user auto-detection, and download-management platform.

🔗 **在线演示 / Live Demo：** [https://dome.monster6324.me/](https://dome.monster6324.me/)

---

## 目录 / Table of Contents

- [项目简介 / Overview](#项目简介--overview)
- [在线演示 / Live Demo](#在线演示--live-demo)
- [核心功能 / Features](#核心功能--features)
- [技术栈 / Tech Stack](#技术栈--tech-stack)
- [项目结构 / Project Structure](#项目结构--project-structure)
- [快速开始 / Quick Start](#快速开始--quick-start)
- [环境变量 / Environment Variables](#环境变量--environment-variables)
- [下载机器人工作原理 / Download Bot Logic](#下载机器人工作原理--download-bot-logic)
- [Discord Activity 内嵌模式 / Discord Activity Mode](#discord-activity-内嵌模式--discord-activity-mode)
- [鸣谢与依赖引用 / Credits & Attribution](#鸣谢与依赖引用--credits--attribution)
- [打赏支持 / Donation](#打赏支持--donation)
- [许可证 / License](#许可证--license)

---

## 项目简介 / Overview

**中文：**
Discord Analytics Dashboard 是一个基于 Flask 的 Web 平台，用于对 Discord 论坛（Forum）频道的数据进行采集、导入与可视化分析。项目内置多机器人并发下载器（基于 [DiscordChatExporter](https://github.com/Tyrrrz/DiscordChatExporter)）、SQLite 数据存储、Discord OAuth2 登录、常规用户自动授权，以及可直接在 Discord 客户端内运行的 Activity（内嵌应用）模式。

**English:**
Discord Analytics Dashboard is a Flask-based web platform for collecting, importing, and visualizing data from Discord Forum channels. It includes a concurrent multi-bot downloader (built on [DiscordChatExporter](https://github.com/Tyrrrz/DiscordChatExporter)), SQLite-based storage, Discord OAuth2 login, automatic access provisioning for regular members, and a Discord Activity (embedded app) mode that runs directly inside the Discord client.

---

## 在线演示 / Live Demo

**中文：** 测试站点：**[https://dome.monster6324.me/](https://dome.monster6324.me/)**（仅供功能演示，数据可能随时重置）。

**English:** Test site: **[https://dome.monster6324.me/](https://dome.monster6324.me/)** (for demonstration purposes only — data may be reset at any time).

---

## 核心功能 / Features

**中文：**
- 数据看板：服务器概览、排行榜、用户主页、抽象派（图云/词云等）展示
- Forum 帖子多机器人并发扫描与下载，支持断点续传、暂停/继续/取消/删除；任务运行期间可以继续加入下载账号
- 单服务器支持最多 5 个自定义下载机器人 + 1 个默认机器人，最多 6 个并发
- 常规用户自动识别：只要 Discord User ID 出现在已采集服务器数据中即可自动获得访问权限，无需手动白名单
- 白名单机器人内置 Forum 帖子收藏功能（`/favorites`、`/top`、`/top30` 等指令）
- Discord Activity（内嵌应用）模式，可直接在 Discord 客户端语音频道“火箭图标”中打开
- 前端依赖（Tailwind、Chart.js、Alpine.js、TagCloud、Discord Embedded App SDK）全部本地托管，避免 CDN 不可用问题
- 移动端自适应布局
- 详细的运行日志与 Activity 鉴权诊断日志（自动轮转，最多保留 5 份，且不记录敏感令牌）

**English:**
- Analytics dashboard: server overview, leaderboards, user profile pages, word-cloud style visualizations
- Multi-bot concurrent Forum scanning and downloading, with resumable, pause/resume/cancel/delete task controls
- Up to 5 custom download bots + 1 default bot per server (up to 6 concurrent workers)
- Automatic regular-user access: any Discord User ID found in an already-collected server dataset is granted access automatically, no manual whitelisting required
- The whitelist bot doubles as a Forum-post favorites bot (`/favorites`, `/top`, `/top30`, etc.)
- Discord Activity (embedded app) mode, launchable directly from the voice-channel "rocket" activity menu inside Discord
- Frontend dependencies (Tailwind, Chart.js, Alpine.js, TagCloud, Discord Embedded App SDK) are all hosted locally to avoid CDN failures
- Mobile-responsive layouts
- Detailed application logs and Activity authentication diagnostics (auto-rotated, up to 5 files retained, tokens never logged)

---

## 技术栈 / Tech Stack

**中文 / English：**

| 类别 Category | 说明 Details |
| --- | --- |
| 后端 Backend | Python, Flask ≥ 3.1 |
| 数据库 Database | SQLite（`data/portal.db` + 各服务器独立数据库） |
| Discord 交互 Discord | discord.py ≥ 2.4, PyNaCl, Discord OAuth2, Discord Embedded App SDK 2.5.0 |
| 数据导出 Exporter | [DiscordChatExporter](https://github.com/Tyrrrz/DiscordChatExporter)（外部依赖，需自行下载） |
| 前端 Frontend | Tailwind CSS, Chart.js, Alpine.js, TagCloud（均本地托管） |
| 异步/工具 Async & Utils | aiosqlite, requests, ijson |

---

## 项目结构 / Project Structure

```text
Discord-Analytics-Dashboard-main/
├── app.py                     # 主应用入口 / Main application entry point
├── requirements.txt
├── .env.example
├── shared/                    # 通用工具模块 / Shared utility modules
├── templates/                 # Flask 网页模板 / Web templates
├── static/                    # 前端静态资源 / Frontend static assets
├── Preparation_Before_Use/    # 下载器、白名单机器人、旧版处理脚本
│                               # Downloader, whitelist bot, legacy scripts
├── tests/ / test/             # 测试用例 / Test suites
├── data/                      # 运行时生成的数据（数据库、日志）
│                               # Runtime-generated data (DB, logs)
└── raw/                       # 临时下载文件 / Temporary download files
```

---

## 快速开始 / Quick Start

**中文：**

1. 复制 `.env.example` 为 `.env` 并填入你的 Discord 应用/机器人凭据。
2. 在**虚拟环境**中安装依赖：

   ```bash
   python -m venv venv
   venv/bin/pip install -r requirements.txt
   ```

3. 启动主程序：

   ```bash
   venv/bin/python app.py
   ```

   下载器与白名单机器人默认由 `app.py` 自动拉起，通常无需单独运行。如需单独调试：

   ```bash
   venv/bin/python Preparation_Before_Use/discord_downloader.py
   venv/bin/python Preparation_Before_Use/whitelist_bot.py
   ```

> ⚠️ **不要**直接使用系统 Python 运行 `app.py`。若使用 `nohup`、systemd、Supervisor 等进程管理工具，可执行文件必须指向项目的 `venv/bin/python`，否则可能出现 `ModuleNotFoundError: No module named 'flask'`，页面表现为空白。

4. 本项目的下载功能依赖外部工具 **DiscordChatExporter**，请自行前往其官方仓库下载可执行文件（见下方“鸣谢与依赖引用”）。

**English:**

1. Copy `.env.example` to `.env` and fill in your Discord application/bot credentials.
2. Install dependencies inside a **virtual environment**:

   ```bash
   python -m venv venv
   venv/bin/pip install -r requirements.txt
   ```

3. Start the main application:

   ```bash
   venv/bin/python app.py
   ```

   The downloader and whitelist bot are normally started automatically by `app.py`, so you don't need to run them manually. For standalone debugging:

   ```bash
   venv/bin/python Preparation_Before_Use/discord_downloader.py
   venv/bin/python Preparation_Before_Use/whitelist_bot.py
   ```

> ⚠️ **Do not** run `app.py` with the system Python interpreter. If you use `nohup`, systemd, Supervisor, or another process manager, the executable must point to the project's `venv/bin/python`, or you may see `ModuleNotFoundError: No module named 'flask'` and a blank page.

4. The downloader depends on the external tool **DiscordChatExporter**. Download the executable from its official repository yourself (see "Credits & Attribution" below).

---

## 环境变量 / Environment Variables

**中文：** 主要变量（完整列表见 `.env.example`）：

| 变量 Variable | 说明 Description |
| --- | --- |
| `DISCORD_CLIENT_ID` / `DISCORD_CLIENT_SECRET` | Discord OAuth2 应用凭据 |
| `ADMIN_IDS` | 管理员 Discord User ID，逗号分隔 |
| `DISCORD_BOT_TOKEN` | 主程序/白名单机器人令牌 |
| `DISCORD_DOWNLOADER_TOKEN` | 默认下载机器人令牌 |
| `PUBLIC_BASE_URL` | 生产环境访问域名（HTTPS，Activity 模式必需） |
| `DOWNLOAD_MAX_CONCURRENT_TASKS` | 同时运行的独立下载任务数量 |
| `DCE_MARKDOWN` / `DCE_PROCESS_POLL_MS` / `DCE_LOG_COMMAND` / `DCE_BATCH_SIZE` | DiscordChatExporter 相关性能与批量导出参数 |

自定义下载账号可以在管理后台选择 `Bot Token` 或 `User Token`。User Token 会按 Discord 的原始 Authorization 方式用于 REST 扫描和 DiscordChatExporter 导出；DiscordChatExporter 官方文档明确提示自动化用户账号可能违反 Discord 服务条款，因此生产环境建议使用 Bot Token。所有下载账号令牌仅保存在服务端，不会返回浏览器或写入日志。

**English:** Key variables (see `.env.example` for the full list):

| Variable | Description |
| --- | --- |
| `DISCORD_CLIENT_ID` / `DISCORD_CLIENT_SECRET` | Discord OAuth2 application credentials |
| `ADMIN_IDS` | Comma-separated admin Discord User IDs |
| `DISCORD_BOT_TOKEN` | Main app / whitelist bot token |
| `DISCORD_DOWNLOADER_TOKEN` | Default download bot token |
| `PUBLIC_BASE_URL` | Production HTTPS base URL (required for Activity mode) |
| `DOWNLOAD_MAX_CONCURRENT_TASKS` | Number of concurrent independent download tasks |
| `DCE_MARKDOWN` / `DCE_PROCESS_POLL_MS` / `DCE_LOG_COMMAND` / `DCE_BATCH_SIZE` | DiscordChatExporter performance and batch-export tuning |

---

## 下载机器人工作原理 / Download Bot Logic

**中文：**
每个有效下载机器人对应一个独立 Worker，使用自己的令牌启动 DiscordChatExporter 进程；每个 DCE 进程按小批次导出多个帖子，并按帖子 ID 分别输出文件。第一个配置的机器人负责扫描 Forum，其余机器人在扫描进行的同时并行下载已发现的帖子；扫描完成后，扫描机器人也会加入下载池。扫描进度（`scan_cursor`）与帖子级下载状态均持久化到数据库，支持暂停/继续而不重复下载。详细状态字段与通知机制说明见项目内 [`BOT_DOWNLOAD_LOGIC.md`](./BOT_DOWNLOAD_LOGIC.md)（中文）。

**English:**
Each valid download bot runs its own Worker and starts an independent DiscordChatExporter process using its own token; each DCE process exports a small batch of threads and writes separate files keyed by thread ID. The first configured bot is responsible for scanning the Forum, while the remaining bots download discovered threads concurrently while the scan is still running; once scanning finishes, the scanning bot also joins the download pool. Scan progress (`scan_cursor`) and per-thread download status are persisted to the database, allowing pause/resume without re-downloading completed threads. See [`BOT_DOWNLOAD_LOGIC.md`](./BOT_DOWNLOAD_LOGIC.md) (Chinese) in the repository for the full state-field and notification breakdown.

---

## Discord Activity 内嵌模式 / Discord Activity Mode

**中文：**
该网站可作为 Discord Activity 直接在 Discord 客户端内以内嵌应用形式打开，无需用户单独打开外部浏览器。前端脚本会自动检测 Activity 环境并调用官方 Discord Embedded App SDK 完成授权，后端复用现有 OAuth 令牌交换逻辑完成登录。需要在 [Discord 开发者平台](https://discord.com/developers/applications) 中手动启用 Activities、配置 URL Mapping 与 OAuth2 设置，详细步骤见 [`English Documentation.md`](./English%20Documentation.md)。

**English:**
The website can run as a Discord Activity, opening directly inside the Discord client as an embedded application without requiring an external browser. The frontend automatically detects the Activity environment and uses the official Discord Embedded App SDK to authorize; the backend reuses the existing OAuth token-exchange logic to complete login. Activities, URL Mapping, and OAuth2 settings must be configured manually in the [Discord Developer Portal](https://discord.com/developers/applications) — see [`English Documentation.md`](./English%20Documentation.md) for the full setup steps.

---

## 鸣谢与依赖引用 / Credits & Attribution

**中文：**
本项目的数据下载功能构建于开源工具 **[DiscordChatExporter](https://github.com/Tyrrrz/DiscordChatExporter)**（作者：[Tyrrrz](https://github.com/Tyrrrz)，许可证：GPL-3.0-only）之上。DiscordChatExporter 本身**未包含**在本仓库中，请前往其 [Releases 页面](https://github.com/Tyrrrz/DiscordChatExporter/releases) 自行下载对应平台的可执行文件，并遵守其许可证条款与项目仓库中声明的使用条件（包括 Discord 官方服务条款关于自动化用户账号的限制——建议使用机器人令牌而非用户令牌进行导出）。

**English:**
This project's data-download functionality is built on top of the open-source tool **[DiscordChatExporter](https://github.com/Tyrrrz/DiscordChatExporter)** (author: [Tyrrrz](https://github.com/Tyrrrz), licensed under GPL-3.0-only). DiscordChatExporter itself is **not bundled** in this repository — please download the appropriate executable for your platform from its [Releases page](https://github.com/Tyrrrz/DiscordChatExporter/releases) and comply with its license terms and the usage conditions stated in its repository (including Discord's Terms of Service restrictions on automating user accounts — using a bot token rather than a user token for exporting is recommended).

---

## 打赏支持 / Donation

**中文：** 如果这个项目对你有帮助，欢迎打赏支持后续开发与维护：

**English:** If this project has been useful to you, donations to support continued development are greatly appreciated:

**EVM / 以太坊兼容地址 · EVM / Ethereum-compatible address:**

```text
0xB1D6e9f2706085007eD506DD3e9b6697D16D3903
```

**Solana 地址 · Solana address:**

```text
3ZUSMkMAnBZK9e7VeP6JgRk846T1KH4zaDAod7hwAGqe
```

---

## 许可证 / License

**中文：** 本仓库的具体开源许可证请以仓库中实际的 `LICENSE` 文件为准；本项目所依赖的 DiscordChatExporter 采用 **GPL-3.0-only** 许可证，使用时请遵守其条款。

**English:** Please refer to the `LICENSE` file in this repository for its actual license terms. The DiscordChatExporter dependency used by this project is licensed under **GPL-3.0-only**; please comply with its terms when using it.
