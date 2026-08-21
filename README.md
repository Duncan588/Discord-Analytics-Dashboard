# Discord Analytics Dashboard

Discord 数据分析、普通用户自动识别与 Forum 数据下载平台。

## 运行

网站主程序：

```bash
venv/bin/python app.py
```

下载器和白名单机器人已经统一移动到 `Preparation_Before_Use/`，正常情况下由 `app.py` 自动启动，无需手动运行。

如果需要单独调试：

```bash
venv/bin/python Preparation_Before_Use/discord_downloader.py
venv/bin/python Preparation_Before_Use/whitelist_bot.py
```

不要直接使用系统的 `python app.py`。项目依赖安装在 `venv/` 中；如果使用
`nohup` 或进程管理器，也必须把执行文件配置为项目目录下的
`venv/bin/python`。否则会出现 `ModuleNotFoundError: No module named 'flask'`，
Activity 内通常只显示空白页面。

## 项目目录

根目录只保留启动和配置文件：

- `app.py`：主程序
- `README.md`
- `.env`
- `.env.example`
- `requirements.txt`
- `config.example.json`
- `templates/`：网页模板
- `Preparation_Before_Use/`：下载器、数据库导入器、白名单机器人以及旧版数据处理脚本

运行产生的数据统一进入 `data/`，下载临时文件进入 `raw/`。

## 数据库与普通用户

门户数据库为 `data/portal.db`。

数据库会保存：

- Discord OAuth 登录用户
- 普通用户与服务器数据的关联
- 白名单用户
- 服务器访问权限
- 下载机器人
- 下载服务器配置
- 下载任务和每个帖子任务状态

普通用户不再要求白名单。

服务器数据导入或下载完成后，系统会扫描服务器 SQLite 中的 `users` 表，将 Discord User ID 同步到门户数据库。普通用户登录后，只要自己的 User ID 出现在已收录服务器数据中，就会自动获得该服务器访问权限。

这样解决了“只有管理员/白名单用户能正常进入网站，普通服务器成员登录后却回到介绍页”的逻辑缺陷。

## 下载任务

下载任务现在使用数据库记录帖子状态，不再使用 `processed_ids.txt` 作为任务进度数据库。

帖子导出的 JSON 只作为临时文件：

1. DiscordChatExporter 导出到临时 JSON
2. 数据库记录该 Thread 已下载
3. 所有帖子完成后逐个增量导入服务器 SQLite
4. 重建用户统计
5. 成功后立即删除原始 JSON 和任务临时目录

文件名只使用 Discord Thread ID，例如：

`1539162014532894730.json`

不再把 Discord 帖子标题拼进文件名，因此不会因为超长标题触发：

`[Errno 36] File name too long`

## 下载控制

网页后台支持：

- 暂停下载：停止当前 DiscordChatExporter 子进程，已完成帖子状态保留
- 继续下载：从数据库任务状态继续，已经下载的帖子不会重复下载
- 取消任务：停止任务，但保留临时数据
- 删除下载：删除任务临时目录；如果该任务是当前服务器分析数据库的来源，则同时删除对应服务器分析数据库、服务器访问记录和门户服务器记录

删除操作是彻底删除，不会留下已经下载好的任务数据。

## 多机器人

单个服务器最多：

- 5 个自定义下载机器人
- 1 个环境变量默认下载机器人

最多 6 个机器人并行下载。

每个机器人都会先检查 Forum 访问权限。权限检查失败不会开始下载。

## 白名单机器人（已合并收藏功能）

`Preparation_Before_Use/whitelist_bot.py` 现在是唯一需要运行的通用 Discord 机器人，
原来独立的 `favorite_bot.py`（Forum 帖子收藏机器人）已经合并进来，两者共用同一个
Bot Token（`DISCORD_BOT_TOKEN`）、同一个 CommandTree、同一个 `portal.db`：

- 白名单 / 权限：`/whitelist_add` `/whitelist_remove` `/whitelist_list` `/server_access` `/quota`
- 主机器人控制：`/restart`，仅 `.env` 中 `ADMIN_IDS` 的用户可执行
- 成员名单同步：`/members_sync`，新成员加入、资料变化时自动同步
- 下载完成 / 失败私信通知
- 收藏功能：右键 Forum 帖子内消息 → Apps → 📌 收藏帖子 / 📕 取消收藏
- 收藏查询：`/favorites` `/top` `/top30` `/help`

不再需要单独部署或运行 `favorite_bot.py`；收藏数据表（`favorites`、`favorite_bot_users`）
直接建在 `portal.db` 里，不再单独维护 `discord_favorites.db` 文件。

如果你之前单独跑过 `favorite_bot.py` 并且 `discord_favorites.db` 里已经积累了收藏数据，
需要迁移到 `portal.db` 时可以告诉我，我可以给你写一个一次性迁移脚本。

## Discord 活动模式（Activities / “小活动”）

网站现在支持作为 Discord 的 Activity（语音频道里的“小活动”）直接在 Discord 客户端内
以 iframe 形式打开，不需要用户额外打开浏览器访问。

原理：

1. 前端新增 `static/js/discord-activity.js`，只有页面被 Discord 当作 Activity 加载
   （URL 带 `frame_id` / `instance_id`，或者处于跨域 iframe 中）时才会触发，普通浏览器
   直接访问网站完全不受影响。
2. 该脚本通过 Discord 官方 Embedded App SDK 调用 `authorize()` 拿到一次性 `code`，
   POST 给新增的 `/api/activity/token` 接口。
3. 后端复用原来 `/callback` 的 OAuth 换取逻辑（已抽成 `exchange_discord_code()`），
   用 `code` 换 `access_token`，写入 Flask session，前端刷新页面即完成登录。
4. Session Cookie 在部署为 `https`（即 `.env` 中 `PUBLIC_BASE_URL` 以 `https://` 开头）
   时会自动切换成 `SameSite=None; Secure; Partitioned`，这是 Discord 跨站 iframe
   场景下 Cookie 能正常保留登录态的必要条件；本地 `http` 调试环境不受影响，仍然
   是 `SameSite=Lax`。

部署为 Discord 活动，需要在 [Discord Developer Portal](https://discord.com/developers/applications)
里做以下配置（这部分是控制台操作，代码无法代劳）：

1. 打开对应 Application → **Activities** → 启用 Activities。
2. **URL Mappings** 里把 Root Mapping（`/`）指向 `.env` 中配置的 `PUBLIC_BASE_URL` 域名，
   必须是 `https`。
3. **OAuth2** 页面确认 Client ID / Client Secret 与 `.env` 中的 `DISCORD_CLIENT_ID` /
   `DISCORD_CLIENT_SECRET` 一致，Scopes 至少包含 `identify` `guilds`。
4. 在测试阶段，把要测试的 Discord 账号加入 Application 的 **Team** 或该服务器的
   **测试服务器（Test Servers）**列表，这样不用经过 Discord 审核也能在语音频道的
   火箭图标里看到并启动这个活动；面向所有用户公开需要提交 Discord 审核。
5. 机器人本身需要以正常方式加入目标服务器（`applications.commands` + `bot` scope），
   这一步和现有 OAuth 邀请流程一样，不需要额外操作。

Activity 使用项目本地静态目录中的 Discord Embedded App SDK 2.5.0，不依赖
jsDelivr。Discord Activity 的网络代理可能无法访问第三方 CDN；本地托管 SDK
可以保证 `ready()` 和 `authorize()` 在 Activity 内正常执行。

页面布局所需的 Tailwind 静态 CSS、Chart.js、Alpine.js 和 TagCloud 也已放入
本站静态目录并由模板从本站加载；Activity 不再在浏览器端运行 Tailwind 编译器，
不会因外部 CDN 不可用或运行时编译延迟而出现首屏错版。

### Activity 登录排障日志

门户日志默认写入 `data/logs/app.log`，单个文件超过 10 MB 后自动轮转并保留 5 份。
Activity 登录会记录前端 SDK 加载、`authorize`、后端 token 换取、用户资料/guild
请求、限流阶段和 `retry_after`；授权 code 及 access token 不会写入日志。前端遇到
Discord 限流时会暂停重复授权并显示倒计时，冷却结束后再获取新的授权 code。

前端诊断事件通过 `/api/activity/log` 写入同一日志文件，日志中可用每次 Activity
生成的 `request_id` 对照完整链路。若需要更详细的门户日志，可在启动进程环境中设置
`LOG_LEVEL=DEBUG`。

如果 Activity 授权后仍是空白页，先检查 `data/logs/app.log` 中的
`activity.status authenticated=`。如果是 `False`，通常是代理没有保存 Secure
Session Cookie，需确认生产地址使用 HTTPS 且 Discord URL Mapping 指向同一个域名。

## 移动端

网页模板统一加入移动端 viewport，并重新调整：

- 管理后台按钮
- 下载配置表单
- 下载任务操作区
- 机器人管理
- 白名单管理
- 服务器选择
- 首页卡片
- 用户页面

手机端按钮会自动堆叠，不再出现横向撑爆页面或按钮过长的问题。

## V2 并发与多 Forum 优化说明

- 基于上一版 V20 optimized 继续修改，未回退到最初版本。
- `download_configs` 支持同一 Discord 服务器配置多个 Forum；多个 Forum 最终导入同一个服务器 SQLite 数据库，因此网站展示会自动合并。
- 下载任务增加 `config_id`、服务器名称、Forum 名称和调度检查间隔。
- 扫描与下载分离：第一个配置机器人负责扫描；其余机器人负责下载。只有一个机器人时，该机器人扫描结束后继续下载。
- Forum 历史扫描使用 `archived_threads(limit=None)`，不再限制约 100 个归档帖子。
- 扫描结果分批写入 `download_task_items`，避免一次性把几十万帖子全部保存在 Python 内存中。
- 不同下载任务使用 Worker Pool 并行执行，`DOWNLOAD_MAX_CONCURRENT_TASKS` 控制同时运行的任务数量。
- `DOWNLOADER_INTERVAL` 已移除；每个下载配置在 Admin 的“调度检查间隔”中单独设置。
- 普通用户登录时会直接检查各服务器 SQLite 的 `users` 主键，即使历史数据库没有提前同步到 `user_server_presence`，也能正常识别所属服务器。
- 运行中的删除任务由后台 Worker 负责结束 DCE 进程后清理，避免删除任务后残留 Worker 导致下一任务卡死。
- 同一服务器多个 Forum 删除单个任务时，仅删除该任务对应的帖子数据；如果其他已完成任务仍引用同一帖子，则保留该帖子。


## 下载速度说明

Admin 的“任务调度检查间隔”只控制下载器多久检查一次新的待处理任务，不控制 Forum 扫描频率，也不控制 DiscordChatExporter 的下载速度。正在运行的任务不会因为这个值变慢。

当前下载结构为每个机器人独立运行一个 DiscordChatExporter 进程。扫描机器人负责扫描 Forum，其他机器人在扫描过程中立即从队列获取帖子并并行导出；扫描完成后扫描机器人也加入下载池。
