"""Run the dashboard locally against the cloud databases, web only.

Why not just ``python app.py``: ``app.py``'s ``__main__`` block calls
``start_background_services()``, which spawns ``whitelist_bot.py`` and
``discord_downloader.py``. Those child processes log in to Discord with the
same ``DISCORD_BOT_TOKEN`` as the production deployment, and two live gateway
sessions on one token kick each other offline (see KNOWN_ISSUES K18). This
launcher imports the Flask app and serves it without starting any child
service, so the local instance is only the web front end talking to the
production SQLite files over the cloud-DB bridge.

Usage::

    venv\\Scripts\\python run_local_web.py
"""

import os
import sys

import app as dashboard


def main():
    if os.getenv("DISCORD_BOT_TOKEN", "").strip():
        print("[run_local_web] 注意：本地不会启动 whitelist_bot / discord_downloader，"
              "避免与线上抢同一个 bot token（见 KNOWN_ISSUES K18）。")
    host = os.getenv("LOCAL_WEB_HOST", "127.0.0.1")
    port = int(os.getenv("PORT", "5000"))
    print("[run_local_web] 云端数据模式 =", dashboard.cloud_enabled())
    print("[run_local_web] 监听 http://%s:%d/" % (host, port))
    dashboard.app.run(host=host, port=port, debug=False, use_reloader=False, threaded=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
