# 🌊 Discord Analytics Dashboard / Discord 服务器数据分析看板

[English](#english) | [中文](#chinese)

---

<a name="english"></a>
## 🇬🇧 English Description

A high-performance, visualization-rich Discord server analytics dashboard built with **Python (Flask)**. It provides detailed insights into server activity, member engagement, and annual personalized reports.

### ✨ Features

* **📊 Server Overview**: Real-time stats on threads, active members, and visual activity charts (Daily/Hourly).
* **🏆 Leaderboards**: Top active members, hot threads, trending replies, and server-wide word clouds (3D).
* **👤 User Profile**: Detailed personal stats, message history, frequent emojis, interaction network, and profile view tracking.
* **📑 Annual Report**: A "Spotify Wrapped" style scrolling report for members, showing their join date rank, late-night activity, and social highlights.
* **🚀 High Performance**: Built-in caching mechanism to handle millions of messages with zero latency.
* **🔐 Secure Login**: Integrated Discord OAuth2 authentication.

### 🛠️ Prerequisites

* Python 3.9+ or Docker
* A Discord Developer Application (Client ID & Secret)
* `discord_data.db`: A SQLite database containing your Discord chat logs (Schema expected: `messages`, `users`, `threads`, `reactions`).
* `members.csv` (Optional): A CSV file for accurate join dates (Columns: `用户ID`, `加入服务器时间(UTC)`).

### 🚀 Deployment Guide

#### Method 1: Using Docker (Recommended)

1.  **Clone the repository**
    ```bash
    git clone [https://github.com/yourusername/discord-dashboard.git](https://github.com/yourusername/discord-dashboard.git)
    cd discord-dashboard
    ```

2.  **Configure Credentials**
    Open `app.py` and fill in your Discord App credentials:
    ```python
    DISCORD_CLIENT_ID = "YOUR_CLIENT_ID"
    DISCORD_CLIENT_SECRET = "YOUR_CLIENT_SECRET"
    ```
    *Note: In Discord Developer Portal, set Redirect URI to `http://YOUR_SERVER_IP:5000/callback`.*

3.  **Prepare Data**
    Place your `discord_data.db` and `members.csv` in the project root directory.

4.  **Run with Docker Compose**
    ```bash
    docker-compose up -d
    ```
    Access the dashboard at `http://localhost:5000`.

#### Method 2: Manual Installation

1.  **Install Dependencies**
    ```bash
    pip install -r requirements.txt
    ```

2.  **Run the App**
    ```bash
    python app.py
    ```

---

<a name="chinese"></a>
## 🇨🇳 中文介绍

一个基于 **Python (Flask)** 构建的高性能 Discord 服务器数据可视化分析看板。它提供了服务器活跃度、成员互动详情以及年度个人总结报告等深度数据洞察。

### ✨ 核心功能

* **📊 服务器概览**：实时收录帖子数、活跃成员数，以及每日/每小时活跃度趋势图表。
* **🏆 丰富榜单**：活跃成员排行、热门讨论区、高赞回复排行以及服务器 3D 词云。
* **👤 个人档案**：详细的发言记录、发帖历史、常用表情分析、社交互动关系网以及主页浏览量记录。
* **📑 年度报告**：类似 "网易云年度听歌报告" 的全屏滑动式总结，包含入群排名、熬夜记录、高光时刻等。
* **🚀 极速响应**：内置多线程缓存机制，毫秒级加载百万条消息数据。
* **🔐 安全登录**：集成 Discord OAuth2 官方授权登录。

### 🛠️ 准备工作

* Python 3.9+ 或 Docker 环境
* Discord 开发者应用 (获取 Client ID 和 Secret)
* `discord_data.db`：包含 Discord 聊天记录的 SQLite 数据库（需包含 `messages`, `users`, `threads`, `reactions` 表）。
* `members.csv` (可选)：用于更精确的入群时间排名（列名：`用户ID`, `加入服务器时间(UTC)`）。

### 🚀 部署教程

#### 方法一：使用 Docker 部署 (推荐)

这是最简单的方法，无需配置 Python 环境。

1.  **下载源码**
    ```bash
    git clone [https://github.com/yourusername/discord-dashboard.git](https://github.com/yourusername/discord-dashboard.git)
    cd discord-dashboard
    ```

2.  **配置参数**
    打开 `app.py` 文件，修改配置区：
    ```python
    DISCORD_CLIENT_ID = "你的CLIENT_ID"
    DISCORD_CLIENT_SECRET = "你的CLIENT_SECRET"
    ```
    *注意：请务必在 Discord Developer Portal 的 OAuth2 设置中，将 Redirects 添加为 `http://你的服务器IP:5000/callback`。*

3.  **放入数据文件**
    将你的 `discord_data.db` 和 `members.csv` 文件放入当前目录。

4.  **一键启动**
    ```bash
    docker-compose up -d
    ```
    启动后访问 `http://localhost:5000` (或服务器 IP:5000) 即可使用。

#### 方法二：手动安装

1.  **安装依赖库**
    ```bash
    pip install -r requirements.txt
    ```

2.  **启动应用**
    ```bash
    python app.py
    ```

### 📂 数据库结构说明 (Database Schema)

本项目依赖 `discord_data.db`，核心表结构如下：
* **users**: `user_id`, `username`, `nickname`, `avatar_url`...
* **messages**: `message_id`, `author_id`, `content`, `timestamp`, `thread_id`...
* **threads**: `thread_id`, `name`...
* **reactions**: `message_id`, `emoji_name`, `emoji_url`...

### 📄 License

MIT License
