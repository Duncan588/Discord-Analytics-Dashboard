"""Run the app on the fake demo DB with a pre-signed admin session (E2E only)."""
import os, sys
ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT); os.chdir(ROOT)
GUILD = "900000000000000001"
import app as dashboard  # noqa: E402
from dixo import summary  # noqa: E402
summary.refresh(os.path.join(ROOT, "data", "servers", GUILD, "discord_data.db"), budget_seconds=0)
a = dashboard.app
admin = sorted(dashboard.ADMIN_IDS)[0] if dashboard.ADMIN_IDS else "891196284998930522"
ser = a.session_interface.get_signing_serializer(a)
cookie = ser.dumps({"user": {"id": admin, "username": "admin", "avatar": None}, "server_id": GUILD})
with open(sys.argv[2], "w") as fh:
    fh.write(a.config.get("SESSION_COOKIE_NAME", "session") + "\n" + cookie)
a.run(port=int(sys.argv[1]), threaded=True, use_reloader=False)
