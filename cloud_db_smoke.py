"""Smoke test for the cloud-DB bridge: read production over SSH, no download.

Run it with the same environment the app uses::

    set CLOUD_DB=1
    set CLOUD_DB_SSH_TARGET=root@<host>
    set CLOUD_DB_SSH_KEY=C:\\path\\to\\id_key
    venv\\Scripts\\python cloud_db_smoke.py

Part 1 proves the transport: it reads counts out of the production
``portal.db``, checks a 19-digit snowflake survives the round trip, and
confirms a write is refused while ``CLOUD_DB_READONLY=1``.

Part 2 proves the wiring: it imports the real Flask ``app`` and asks it, in
cloud mode, for the server list and a page render.
"""

import os
import sqlite3
import sys

from shared.cloud_db import cloud_enabled, maybe_connect, remote_stat
from shared.env import load_local_env

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PORTAL = os.path.join(BASE_DIR, "data", "portal.db")
SERVER_DB = os.path.join(BASE_DIR, "data", "servers", "915249444721668096", "discord_data.db")


def main():
    load_local_env(os.path.join(BASE_DIR, ".env"))
    if not cloud_enabled():
        print("CLOUD_DB is not set to 1 -- nothing to test.")
        return 1

    print("portal.db   ->", remote_stat(PORTAL))
    print("main server ->", remote_stat(SERVER_DB))

    conn = maybe_connect(PORTAL)
    if conn is None:
        print("portal.db 未映射到云端")
        return 1
    print("read_only =", conn.read_only, "remote =", conn.remote_path)

    row = conn.execute("SELECT count(*) AS n FROM portal_users").fetchone()
    print("portal_users =", row["n"], "(positional:", row[0], ")")

    snowflake = conn.execute("SELECT 1542114525183082497 AS n").fetchone()[0]
    print("snowflake round trip =", snowflake, "lossless =", snowflake == 1542114525183082497)

    try:
        conn.execute("CREATE TABLE _cloud_smoke_should_fail(x)")
    except sqlite3.OperationalError as exc:
        print("write refused as expected:", exc)
    else:
        print("!! write was NOT refused -- check CLOUD_DB_READONLY / agent gate")

    conn.close()

    print()
    print("--- app wiring ---")
    import app as dashboard

    print("app.cloud_enabled() =", dashboard.cloud_enabled())
    print("main guild db exists =", dashboard.server_db_exists("915249444721668096"))
    print("demo guild db exists =", dashboard.server_db_exists("900000000000000001"))

    server_conn = dashboard.db_connect(dashboard.server_db_path("915249444721668096"))
    threads = server_conn.execute("SELECT count(*) FROM threads").fetchone()[0]
    print("main server threads =", threads)
    server_conn.close()

    client = dashboard.app.test_client()
    response = client.get("/login")
    print("GET /login ->", response.status_code)

    admin = (os.getenv("ADMIN_IDS", "").split(",") or [""])[0].strip()
    with client.session_transaction() as sess:
        sess["user"] = {"id": admin, "username": "smoke", "avatar_url": None}
        sess["server_id"] = "915249444721668096"
    for path in ("/", "/report", "/user/" + admin):
        response = client.get(path)
        print("GET %s -> %s" % (path, response.status_code))

    return 0


if __name__ == "__main__":
    sys.exit(main())
