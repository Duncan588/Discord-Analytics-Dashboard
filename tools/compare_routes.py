import sys; sys.path.insert(0, ".")
import sys, importlib, json
mod = importlib.import_module(sys.argv[1])
urls = ["/", "/welcome", "/privacy", "/terms", "/login", "/me", "/servers", "/server/900000000000000001", "/search?q=a",
        "/user/900000000000000101", "/user/900000000000000101?tab=messages&sort=hot&page=2", "/user/1", "/report", "/admin", "/admin/bot-features",
        "/admin/download-tasks/status", "/api/downloader-config", "/api/leaderboard?page=2", "/api/activity/status", "/callback", "/auth/discord", "/logout"]
out = {}
for who in ("anon", "admin", "member"):
    c = mod.app.test_client()
    if who != "anon":
        with c.session_transaction() as s:
            s["user"] = {"id": "891196284998930522" if who == "admin" else "900000000000000101", "username": who, "avatar": None}
            s["server_id"] = "900000000000000001"
    for u in urls:
        r = c.get(u)
        out[f"{who} {u}"] = [r.status_code, r.headers.get("Location", "")]
    r = c.post("/admin/quota", data={"x": 1}); out[f"{who} POST nocsrf"] = [r.status_code, ""]
print(json.dumps(out))
