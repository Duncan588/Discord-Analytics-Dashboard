# Discord Analytics Dashboard V20

Discord data analytics, regular-user auto-identification, and Forum data download platform.

## Running

Main web application:

```bash
venv/bin/python app.py
```

The downloader and whitelist bot have been moved into `Preparation_Before_Use/`. Under normal circumstances, they are automatically started by `app.py`, so you do not need to run them manually.

For standalone debugging:

```bash
venv/bin/python Preparation_Before_Use/discord_downloader.py
venv/bin/python Preparation_Before_Use/whitelist_bot.py
```

**Do not run `python app.py` directly using the system Python interpreter.**

Project dependencies are installed inside `venv/`. If you use `nohup`, systemd, Supervisor, or another process manager, the executable must also point to the project's:

```text
venv/bin/python
```

Otherwise, you may encounter:

```text
ModuleNotFoundError: No module named 'flask'
```

and the Activity may only display a blank page.

---

## Project Structure

The project root keeps only the main startup and configuration files:

- `app.py` — Main application
- `README.md`
- `.env`
- `.env.example`
- `requirements.txt`
- `config.example.json`
- `templates/` — Web templates
- `Preparation_Before_Use/` — Downloader, database importer, whitelist bot, and legacy data-processing scripts

Runtime-generated data is stored in:

```text
data/
```

Temporary download files are stored in:

```text
raw/
```

---

## Database and Regular Users

The portal database is:

```text
data/portal.db
```

The database stores:

- Discord OAuth users
- Relationships between users and servers
- Whitelisted users
- Server access permissions
- Download bots
- Download server configurations
- Download tasks
- Per-thread download status

### Regular User Access

Regular users no longer need to be manually whitelisted.

After server data is imported or downloaded, the system scans the `users` table of the server's SQLite database and synchronizes Discord User IDs into the portal database.

When a regular user logs in, if their Discord User ID exists in any collected server dataset, they automatically receive access to that server.

This fixes the previous issue where only administrators or manually whitelisted users could enter the website normally, while regular server members were redirected back to the introduction page.

---

## Download Tasks

Download tasks now use database records to track thread status.

`processed_ids.txt` is no longer used as the task progress database.

The JSON exported for each thread is only treated as a temporary file.

The workflow is:

1. DiscordChatExporter exports the thread to a temporary JSON file.
2. The database records that the thread has been downloaded.
3. After all threads are completed, they are incrementally imported into the server SQLite database.
4. User statistics are rebuilt.
5. After successful processing, the original JSON files and temporary task directory are immediately deleted.

File names use only the Discord Thread ID, for example:

```text
1539162014532894730.json
```

Discord thread titles are no longer included in file names.

This prevents errors such as:

```text
[Errno 36] File name too long
```

---

## Download Controls

The web administration panel supports:

- **Pause Download** — Stops the current DiscordChatExporter subprocess while preserving completed thread statuses.
- **Resume Download** — Continues from the database task status. Already completed threads will not be downloaded again.
- **Cancel Task** — Stops the task while preserving temporary data.
- **Delete Download** — Deletes the task's temporary directory. If the task is the source of the current server analytics database, the corresponding server analytics database, server access records, and portal server record are also deleted.

Deletion is permanent. Downloaded task data will not be retained after deletion.

---

## Multiple Download Bots

Each Discord server supports:

- Up to **5 custom download bots**
- **1 default download bot** configured through the environment variable

Therefore, a maximum of **6 download bots** can operate simultaneously.

Each bot first checks whether it has permission to access the target Forum.

If the permission check fails, that bot will not start downloading.

---

## Whitelist Bot

The whitelist bot has been merged with the favorites functionality.

`Preparation_Before_Use/whitelist_bot.py` is now the only general-purpose Discord bot that needs to be running.

The previous standalone `favorite_bot.py` has been merged into it.

Both functions share:

- The same Bot Token: `DISCORD_BOT_TOKEN`
- The same CommandTree
- The same `portal.db`

### Available Commands

Whitelist and permission management:

```text
/whitelist_add
/whitelist_remove
/whitelist_list
/server_access
/quota
```

Main bot control:

```text
/restart
```

Only users listed in `.env` under `ADMIN_IDS` can execute `/restart`.

Member synchronization:

```text
/members_sync
```

The bot also automatically synchronizes new members and profile changes.

Other functions include:

- Download completion notifications via DM
- Download failure notifications via DM
- Forum post favorites

### Favorites

Users can right-click a Forum post message and select:

```text
Apps → 📌 Favorite Post
Apps → 📕 Unfavorite Post
```

Favorite-related commands:

```text
/favorites
/top
/top30
/help
```

The standalone `favorite_bot.py` is no longer required and should not be deployed or run separately.

Favorite data is stored directly inside:

```text
portal.db
```

The following tables are used:

```text
favorites
favorite_bot_users
```

There is no longer a separate:

```text
discord_favorites.db
```

If you previously ran `favorite_bot.py` and already have favorite data stored in `discord_favorites.db`, a one-time migration script can be created to migrate the existing data into `portal.db`.

---

# Discord Activity Mode

The website can now operate as a Discord Activity and open directly inside the Discord client as an embedded application.

Users do not need to open the website separately in an external browser.

## How It Works

### 1. Activity Detection

The frontend includes:

```text
static/js/discord-activity.js
```

The script only activates when the page is loaded as a Discord Activity.

It detects Activity-related parameters such as:

```text
frame_id
instance_id
```

or detects that the page is running inside a cross-origin iframe.

Normal browser access is unaffected.

### 2. Discord Authorization

The script uses the official Discord Embedded App SDK to call:

```text
authorize()
```

The resulting one-time authorization code is sent to:

```text
/api/activity/token
```

### 3. Backend Authentication

The backend reuses the OAuth token exchange logic previously used by `/callback`.

The token exchange logic has been extracted into:

```text
exchange_discord_code()
```

The server exchanges the authorization code for an access token and stores the authenticated user in the Flask session.

The frontend then reloads the page and completes the login process.

### 4. Activity Session Cookies

When the application is deployed over HTTPS, meaning:

```text
PUBLIC_BASE_URL=https://...
```

the session cookie automatically uses:

```text
SameSite=None
Secure
Partitioned
```

These settings are required for the session cookie to work correctly in Discord's cross-site iframe environment.

Local HTTP development remains unchanged and continues to use:

```text
SameSite=Lax
```

---

# Discord Developer Portal Configuration

The following configuration must be performed manually in the Discord Developer Portal.

The code cannot automatically configure these settings.

[Discord Developer Portal](https://discord.com/developers/applications?utm_source=chatgpt.com)

## 1. Enable Activities

Open the corresponding Discord Application.

Go to:

```text
Activities
```

Enable Activities.

## 2. Configure URL Mappings

Under:

```text
URL Mappings
```

configure the Root Mapping `/` to point to the domain configured by:

```text
PUBLIC_BASE_URL
```

The URL must use:

```text
https://
```

## 3. Configure OAuth2

Under:

```text
OAuth2
```

make sure the Client ID and Client Secret match:

```text
DISCORD_CLIENT_ID
DISCORD_CLIENT_SECRET
```

The OAuth scopes should include at least:

```text
identify
guilds
```

## 4. Testing

During development and testing, add the Discord accounts that need to test the Activity to the application's:

- Team
- Test Servers

This allows the Activity to appear in the rocket/activity interface of the voice channel without requiring public Discord approval.

For public distribution, Discord's Activity review process may be required.

## 5. Discord Bot

The bot itself still needs to be invited to the target Discord server normally.

The required scopes are:

```text
applications.commands
bot
```

No additional special bot configuration is required for Activity mode.

---

# Local Discord Embedded App SDK

The project includes Discord Embedded App SDK **2.5.0** inside the local static directory.

It does not depend on jsDelivr or another third-party CDN.

This is important because the network environment used by Discord Activities may not reliably access external CDNs.

Local hosting ensures that:

```text
ready()
authorize()
```

can load correctly inside Discord Activities.

---

# Frontend Static Assets

The following frontend dependencies are also hosted locally:

- Tailwind CSS
- Chart.js
- Alpine.js
- TagCloud

The Activity no longer relies on the browser compiling Tailwind CSS at runtime.

This avoids problems caused by:

- External CDN failures
- Runtime Tailwind compilation delays
- Incorrect first-screen rendering
- Activity network restrictions

---

# Activity Authentication Troubleshooting

The portal log is stored at:

```text
data/logs/app.log
```

When a single log file exceeds **10 MB**, it is automatically rotated.

The system retains up to **5** rotated log files.

Activity authentication logs include:

- Frontend SDK loading
- `authorize()` execution
- Backend token exchange
- User profile requests
- Guild requests
- Rate-limit events
- `retry_after` values

Authorization codes and access tokens are **never written to the logs**.

---

## Frontend Activity Diagnostics

Frontend diagnostic events are sent to:

```text
/api/activity/log
```

and written to the same application log.

Each Activity session generates a:

```text
request_id
```

which can be used to trace the complete authentication flow.

For more detailed application logs, set:

```text
LOG_LEVEL=DEBUG
```

in the process environment.

---

## Discord Rate Limits

If Discord returns a rate-limit response, the frontend will:

1. Stop repeated authorization attempts.
2. Display a countdown.
3. Wait for the cooldown period.
4. Request a new authorization code after the cooldown expires.

This prevents repeated Activity authorization requests from creating unnecessary Discord API rate limits.

---

## Blank Activity Page Troubleshooting

If the Activity still displays a blank page after authorization, check:

```text
data/logs/app.log
```

Look for:

```text
activity.status authenticated=
```

If the value is:

```text
False
```

the most common cause is that the production environment failed to preserve the Secure session cookie.

Check the following:

1. The production website uses HTTPS.
2. `PUBLIC_BASE_URL` uses the same HTTPS domain.
3. Discord URL Mapping points to the same domain.
4. The browser/Discord Activity is not switching between different hostnames.
5. The session cookie is being returned and preserved correctly.

---

# Mobile Support

All web templates now include a mobile viewport configuration and responsive layouts.

The following areas have been optimized for mobile devices:

- Administration buttons
- Download configuration forms
- Download task controls
- Bot management
- Whitelist management
- Server selection
- Homepage cards
- User profile pages

On mobile devices, buttons automatically stack vertically when necessary.

This prevents:

- Horizontal overflow
- Oversized buttons
- Broken forms
- Content being pushed outside the viewport

---

# V2 Concurrency and Multiple Forum Optimization

This version continues development from the previous **V20 Optimized** version and does not roll back to the original implementation.

## Multiple Forums per Server

`download_configs` now supports multiple Forums under the same Discord server.

Multiple Forums are imported into the same server SQLite database.

Therefore, website analytics automatically merge the data from all configured Forums.

---

## Download Task Improvements

Download tasks now record:

- `config_id`
- Server name
- Forum name
- Scheduler check interval

This makes it easier to identify the source and configuration of each download task.

---

# Scan and Download Separation

Scanning and downloading are now separated.

The first configured bot is responsible for scanning.

The remaining bots are responsible for downloading.

If only one bot is configured, that bot will continue downloading after it finishes scanning.

This allows other download bots to start processing discovered threads while the Forum scan is still running.

---

# Historical Forum Scanning

Historical Forum scanning now uses:

```python
archived_threads(limit=None)
```

The previous approximately 100-thread archive limitation has been removed.

This allows the downloader to discover the complete available archive instead of stopping after a small number of archived threads.

---

# Incremental Scan Results

Scan results are written to:

```text
download_task_items
```

in batches.

The system no longer keeps tens or hundreds of thousands of thread IDs inside Python memory at once.

This significantly reduces memory usage when processing large Forums.

---

# Parallel Download Workers

Different download tasks can run concurrently using a Worker Pool.

The maximum number of simultaneously running download tasks is controlled by:

```text
DOWNLOAD_MAX_CONCURRENT_TASKS
```

Each downloader bot can run its own DiscordChatExporter process.

This means multiple bots are able to perform downloads concurrently rather than waiting for one bot to finish before another starts.

---

# Scheduler Check Interval

The old:

```text
DOWNLOADER_INTERVAL
```

configuration has been removed.

Each download configuration now has its own:

```text
Scheduler Check Interval
```

which can be configured in the Admin panel.

This allows different servers or Forums to use different scheduling intervals.

---

# Regular User Server Detection

When a normal user logs in, the system directly checks the primary key of the `users` table in each server's SQLite database.

Therefore, even if an older database was not previously synchronized into:

```text
user_server_presence
```

the system can still correctly identify which servers the user belongs to.

This improves compatibility with historical server databases.

---

# Safe Task Deletion

When a running task is deleted, the backend Worker is responsible for terminating the active DiscordChatExporter process before cleaning up the task.

This prevents leftover Workers or DCE processes from remaining alive after deletion and blocking future tasks.

---

# Multiple Forum Deletion

If a server has multiple Forum download tasks, deleting one Forum task only removes the post data associated with that specific task.

If another completed task still references the same post, that post is preserved.

This prevents deleting one Forum configuration from accidentally removing shared data required by another completed Forum task.

---

# Download Speed and Scheduling

The Admin panel's:

```text
Task Scheduler Check Interval
```

only controls how frequently the downloader checks for new pending tasks.

It does **not** control:

- Forum scanning speed
- DiscordChatExporter download speed
- The speed of an already-running download

Therefore, changing this value will not intentionally slow down an active download.

---

# Current Download Architecture

Each download bot runs its own independent DiscordChatExporter process.

The general workflow is:

```text
Forum Scanner
      │
      ▼
Discover Threads
      │
      ▼
Download Task Queue
      │
      ├──────────────┬──────────────┬──────────────┐
      ▼              ▼              ▼              ▼
   Bot #2          Bot #3         Bot #4         Bot #5
      │              │              │              │
      ▼              ▼              ▼              ▼
 DiscordChatExporter processes running concurrently
      │              │              │              │
      └──────────────┴──────────────┴──────────────┘
                         │
                         ▼
                  SQLite Import
                         │
                         ▼
                  User Statistics
```

The scanning bot immediately places discovered threads into the download queue.

Other bots can begin downloading discovered threads while scanning is still in progress.

After scanning is complete, the scanning bot also joins the download pool.

With multiple download bots configured, the system therefore supports concurrent downloading instead of:

```text
Bot 1 → finish → Bot 2 → finish → Bot 3
```

The intended behavior is:

```text
Bot 1 ────────────────┐
Bot 2 ────────────────┤
Bot 3 ────────────────┤
Bot 4 ────────────────┤ → Concurrent Downloads
Bot 5 ────────────────┤
Bot 6 ────────────────┘
```

---

# Donation / Support

If this project has been useful to you and you would like to support its continued development, donations are greatly appreciated.

**EVM / Ethereum-compatible address:**

```text
0xB1D6e9f2706085007eD506DD3e9b6697D16D3903
```

**Solana address:**

```text
3ZUSMkMAnBZK9e7VeP6JgRk846T1KH4zaDAod7hwAGqe
```

Thank you for supporting the continued development and maintenance of Discord Analytics Dashboard V20.