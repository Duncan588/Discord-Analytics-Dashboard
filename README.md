# Dixo — Discord Forum Analytics

> Discord 论坛数据分析、成员活跃分层与下载管理平台
> Analytics, member-activity tiers and archive downloads for Discord **Forum** channels.

🔗 **在线演示 / Live Demo：** [https://discordanalytics.monster6324.me/](https://discordanalytics.monster6324.me/)

🤖 **添加机器人 / Add Bot：** [邀请 Dixo Bot / Invite Dixo Bot](https://discord.com/oauth2/authorize?client_id=1460293619616317607)

<p align="center">
  <img src="docs/images/home-light-desktop.png" width="68%" alt="Dashboard (light)">
  <img src="docs/images/home-dark-mobile.png" width="22%" alt="Dashboard (dark, mobile)">
</p>

---

## 目录 / Contents

- [简介 / Overview](#简介--overview)
- [功能 / Features](#功能--features)
- [截图 / Screenshots](#截图--screenshots)
- [快速开始 / Quick Start](#快速开始--quick-start)
- [生产部署 / Deployment](#生产部署--deployment)
- [配置 / Configuration](#配置--configuration)
- [AI 只读 API / AI API](#ai-只读-api--ai-api)
- [项目结构 / Project Layout](#项目结构--project-layout)
- [鸣谢 / Credits](#鸣谢与依赖引用--credits--attribution)
- [打赏 / Donation](#打赏支持--donation)

---

## 简介 / Overview

**中文：** Dixo 把 Discord 论坛频道的帖子、回复与表情反应持续采集进 SQLite，并以苹果风格的浅色 / 深色仪表盘呈现：趋势、热力图、词云、排行榜、成员主页，以及按月判定的成员活跃分层。它既能在浏览器里使用，也能作为 **Discord Activity** 直接在 Discord 客户端（含手机）内打开。

**English:** Dixo continuously ingests Discord Forum threads, replies and reactions into SQLite and presents them as an Apple-style light/dark dashboard — trends, heatmaps, word clouds, leaderboards, member profiles and monthly member-activity tiers. Use it in the browser or open it inside the Discord client (desktop & mobile) as a **Discord Activity**.

---

## 功能 / Features

| | 中文 | English |
| --- | --- | --- |
| 📈 **数据分析** | 消息 / 活跃成员 / 新帖 / 回复率，按日·周·月·年，30 天 / 90 天 / 1 年 / 全部 / 自定义区间；星期×小时热力图、表情排行、jieba 中文词云（含停用词） | Messages, active members, new threads and reply rate by day/week/month/year with 30d/90d/1Y/all/custom ranges; weekday×hour heatmap, emoji ranking, jieba-based Chinese word cloud with stopwords |
| 🧭 **成员活跃分层** | 一贯活跃 / 消失后回归 / 活跃后消失 / 几乎不活跃 / 近两月新活跃；按「最后一次发言」计算沉默月数，全历史月度堆叠图 + 分页全员名单（按消息数排序），用于留存盘点与清理 | Always active / returned / faded / rare / new, measured from the *last* message; full-history monthly stacked chart and paginated member lists (sorted by message count) for retention reviews and cleanup |
| 👤 **成员主页** | 发言、获赞、送出反应、发帖、活跃天数，发帖/发言记录（最新·最热），互动对象、个人词云；首屏走汇总表，重内容异步加载 | Messages, reactions received/given, threads, active days; thread/message history (newest·hottest), interaction partners and personal word cloud; summary-table first paint, heavy parts filled async |
| ⭐ **收藏 / 拉黑** | 收藏帖子、拉黑成员，在主页分页管理 | Favourite threads and block members, managed with pagination on the profile |
| 🔄 **持续下载轮询** | 多 Bot 并行导出（DiscordChatExporter），首轮全量后常驻轮询新帖/新回复/反应，未变化帖子自动跳过；扫描游标与帖子级状态持久化，可暂停续传 | Multi-bot parallel export via DiscordChatExporter; after the first full pass it keeps polling for new threads/replies/reactions and skips unchanged threads; scan cursor and per-thread state persisted for pause/resume |
| 🤖 **AI 只读 API** | Bearer Token 保护的 `/api/ai/v1`：成员、消息、帖子、全文搜索，以及**单个成员发言历史的 Markdown 导出**，方便喂给 LLM 做人设/总结 | Bearer-token `/api/ai/v1`: members, messages, threads, full-text search and **per-member Markdown export** of message history for LLM personas/summaries |
| 🌗 **浅色 / 深色** | 一键切换并记忆；中英双语界面；统一紧凑数字（1.2K、5M、1B） | One-tap theme toggle (remembered), zh/en UI, compact numbers (1.2K, 5M, 1B) |
| 📱 **Discord Activity** | Embedded App SDK 自动登录；手机端安全区适配、点击区域修正、服务器选择服务端记忆 | Embedded App SDK auto-login; mobile safe-area insets, fixed tap targets, server choice remembered server-side |
| ⚡ **性能** | 增量 `summary.db`（影子重建 + 原子替换，重建期间显示上次正确值）、水位缓存、精简索引；百万级消息首页亚秒级 | Incremental `summary.db` (shadow rebuild + atomic swap, last-good KPIs during rebuilds), watermark caches, lean indexes; sub-second home page on millions of messages |
| 🛡️ **管理后台** | 白名单与配额、下载 Bot、服务器配置、下载任务、成员同步、账号合并 | Whitelist & quotas, download bots, server configs, tasks, member sync, account merges |

---

## 截图 / Screenshots

> 截图来自内置的假数据演示库（`generate_fake_data.py`），非真实社区数据。
> Screenshots use the bundled fake demo database, not real community data.

| | 浅色 Light | 深色 Dark |
| --- | --- | --- |
| 首页 Home | ![](docs/images/home-light-desktop.png) | ![](docs/images/home-dark-desktop.png) |
| 深度分析 Analytics | ![](docs/images/analytics-light-desktop.png) | ![](docs/images/analytics-dark-desktop.png) |
| 成员主页 Profile | ![](docs/images/profile-light-desktop.png) | ![](docs/images/profile-dark-desktop.png) |

<p align="center">
  <img src="docs/images/home-light-mobile.png" width="19%" alt="">
  <img src="docs/images/home-dark-mobile.png" width="19%" alt="">
  <img src="docs/images/analytics-dark-mobile.png" width="19%" alt="">
  <img src="docs/images/profile-light-mobile.png" width="19%" alt="">
  <img src="docs/images/profile-dark-mobile.png" width="19%" alt="">
</p>

---

## 快速开始 / Quick Start

```bash
python -m venv venv
venv/bin/pip install -r requirements.txt
cp .env.example .env              # 填入 Discord 应用 / Bot 凭据 · fill in Discord app/bot credentials
venv/bin/python generate_fake_data.py   # 可选：生成演示数据 · optional demo data
venv/bin/python app.py
```

- 下载器与机器人由 `app.py` 自动拉起；单独调试可运行 `Preparation_Before_Use/discord_downloader.py` / `whitelist_bot.py`。
  The downloader and bot are started by `app.py`; run them standalone from `Preparation_Before_Use/` for debugging.
- 下载功能需要自行下载 **DiscordChatExporter**（见鸣谢）。Downloads require **DiscordChatExporter** (see Credits).
- 测试 / Tests: `venv/bin/python -m pytest -q`（浏览器点击回归 `tests/e2e/` 需 `playwright install chromium`）。

> ⚠️ 始终使用 `venv/bin/python`（systemd / Supervisor / nohup 同理），否则会出现 `No module named 'flask'` 与空白页。
> Always run with `venv/bin/python`, including under systemd/Supervisor/nohup.

---

## 生产部署 / Deployment

1. 在服务器上放置代码与 `venv`，`.env` 中设置 `PUBLIC_BASE_URL`（HTTPS）。
   Put the code + venv on the server and set `PUBLIC_BASE_URL` (HTTPS) in `.env`.
2. 用 systemd 运行 `venv/bin/python app.py`，前面挂 nginx 反向代理。
   Run `venv/bin/python app.py` under systemd behind an nginx reverse proxy.
3. 在管理后台添加下载 Bot、服务器 Forum 配置并开启「持续更新」，下载器会常驻轮询。
   In the admin console add download bots and a forum config with *update enabled*; the downloader then polls continuously.
4. （可选）Discord Activity：在 [Discord 开发者平台](https://discord.com/developers/applications) 启用 Activities，配置 URL Mapping（`/` → 你的 `PUBLIC_BASE_URL` 域名）与 OAuth2 回调；运维细节见 [`MAINTENANCE.md`](./MAINTENANCE.md)。
   (Optional) Discord Activity: enable Activities, URL Mapping (`/` → your `PUBLIC_BASE_URL` host) and OAuth2 in the Developer Portal; ops notes in [`MAINTENANCE.md`](./MAINTENANCE.md).

> 大库提示：SQLite `VACUUM` 需要约等于数据库大小的空闲磁盘；汇总表会在后台增量 / 影子重建，期间首页显示上次正确值。
> Large DBs: `VACUUM` needs free disk roughly equal to the DB size; summaries rebuild in the background while the last-good values stay on screen.

---

## 配置 / Configuration

完整列表见 `.env.example`（`.env` 与 `.env.example` 的键必须保持一致）。Full list in `.env.example` (keys must match `.env`).

| 变量 Variable | 说明 Description |
| --- | --- |
| `DISCORD_CLIENT_ID` / `DISCORD_CLIENT_SECRET` | OAuth2 应用凭据 · OAuth2 app credentials |
| `ADMIN_IDS` | 管理员 User ID，逗号分隔 · comma-separated admin IDs |
| `DISCORD_BOT_TOKEN` | 主 Bot 令牌 · main bot token |
| `DISCORD_DOWNLOADER_TOKEN` | 默认下载 Bot（也用于读取服务器成员数）· default download bot (also used for member counts) |
| `PUBLIC_BASE_URL` | 生产 HTTPS 域名（Activity 必需）· production HTTPS URL (required for Activity) |
| `DOWNLOAD_MAX_CONCURRENT_TASKS` | 并发下载任务数 · concurrent download tasks |
| `DOWNLOAD_POLL_FOREVER` / `DOWNLOAD_POLL_IDLE_SEC` | 常驻轮询开关（默认 1）与空闲间隔秒数（默认 60）· keep polling (default 1) and idle interval (default 60s) |
| `DCE_MARKDOWN` / `DCE_PROCESS_POLL_MS` / `DCE_LOG_COMMAND` / `DCE_BATCH_SIZE` | DiscordChatExporter 调优 · exporter tuning |
| `AI_API_TOKENS` | AI API 的 Bearer Token，逗号分隔；留空即关闭 · AI API bearer tokens; empty = disabled |

下载账号可选 Bot Token 或 User Token；令牌仅保存在服务端。DiscordChatExporter 提示自动化用户账号可能违反 Discord 条款，生产环境建议使用 Bot Token。
Download accounts may use a Bot or User token (stored server-side only); Bot tokens are recommended in production.

---

## AI 只读 API / AI API

设置 `AI_API_TOKENS` 后启用，请求头 `Authorization: Bearer <token>`。Enabled when `AI_API_TOKENS` is set.

| 路径 Path (`/api/ai/v1`) | 用途 Purpose |
| --- | --- |
| `GET /guilds` | 可访问的服务器 · guilds |
| `GET /guilds/<gid>/members` · `/members/<uid>` | 成员列表 / 详情 · members |
| `GET /guilds/<gid>/members/<uid>/messages` | 成员消息分页 · member messages |
| `GET /guilds/<gid>/members/<uid>/markdown` | 成员完整发言历史 Markdown 导出 · per-member Markdown export |
| `GET /guilds/<gid>/threads/<tid>/messages` | 帖子消息 · thread messages |
| `GET /guilds/<gid>/search?q=` | 全文搜索（增量 `search.db`）· full-text search |

站内仪表盘另有会话鉴权的 `/api/v1/*`（`summary`、`analytics`、`timeseries`、`heatmap`、`leaderboard`、`wordcloud`、`users/<id>/profile`）。
The dashboard itself uses session-authenticated `/api/v1/*` endpoints. 完整说明 / full reference: [`docs/AI_API.md`](./docs/AI_API.md).

---

## 项目结构 / Project Layout

```text
app.py                  # 入口（薄）· thin entry point
dixo/                   # Flask Blueprints、汇总引擎、分析、API · blueprints, summary engine, analytics, API
  routes/               # pages / admin / auth …
  summary.py            # 增量 + 影子重建汇总库 · incremental/shadow summary DB
  dashboard_data.py     # KPI、序列、活跃分层 · KPIs, series, activity tiers
  api.py                # /api/v1 与 /api/ai/v1
templates/ static/      # Jinja + Chart.js / Alpine.js（本地托管 · vendored）
Preparation_Before_Use/ # 下载器与白名单 Bot · downloader & whitelist bot
tests/                  # pytest（含 e2e 浏览器点击回归 · incl. e2e click audit）
docs/images/            # README 截图 · screenshots
data/                   # 运行时数据库与日志（不入库）· runtime DBs/logs (not committed)
```

技术栈 / Stack: Python · Flask ≥ 3.1 · SQLite · discord.py ≥ 2.4 · Discord Embedded App SDK · Chart.js · Alpine.js · jieba · DiscordChatExporter.

下载器的详细状态字段见 [`BOT_DOWNLOAD_LOGIC.md`](./BOT_DOWNLOAD_LOGIC.md)。Downloader state details: [`BOT_DOWNLOAD_LOGIC.md`](./BOT_DOWNLOAD_LOGIC.md).

---

## 鸣谢与依赖引用 / Credits & Attribution

**中文：**
本项目的数据下载功能构建于开源工具 **[DiscordChatExporter](https://github.com/Tyrrrz/DiscordChatExporter)**（作者：[Tyrrrz](https://github.com/Tyrrrz)，许可证：GPL-3.0-only）之上。DiscordChatExporter 本身**未包含**在本仓库中，请前往其 [Releases 页面](https://github.com/Tyrrrz/DiscordChatExporter/releases) 自行下载对应平台的可执行文件，并遵守其许可证条款与项目仓库中声明的使用条件（包括 Discord 官方服务条款关于自动化用户账号的限制——建议使用机器人令牌而非用户令牌进行导出）。

**English:**
This project's data-download functionality is built on top of the open-source tool **[DiscordChatExporter](https://github.com/Tyrrrz/DiscordChatExporter)** (author: [Tyrrrz](https://github.com/Tyrrrz), licensed under GPL-3.0-only). DiscordChatExporter itself is **not included** in this repository. Please download the appropriate executable from its [Releases page](https://github.com/Tyrrrz/DiscordChatExporter/releases) and comply with its license and the conditions stated in the project repository (including Discord's Terms of Service restrictions on automated user accounts — Bot Tokens are recommended for production exports).

---

## 打赏支持 / Donation

感谢支持本项目。

ETH
   ```
0x3a1d27e8540dfc6c563841f7d620d834dc368323
   ```
SOLANA
   ```
Fucz37Pz8uhHiY7CV98za4jb3gLrLTjUQ2jmZdv9n8Ta
   ```

---

## 许可证 / License

本项目许可证请以仓库中的 LICENSE 文件为准。
