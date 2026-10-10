"""Latency benchmark via Flask test client (local fake DB only).

python tools/bench.py [--guild 900000000000000002] [--user 800000000000000000]
Measures: cold homepage, time until homepage data is complete, warm homepage
(median of 10), profile cold/warm, /api/leaderboard.
"""
import argparse, glob, os, statistics, sys, time
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--incremental", action="store_true")
    ap.add_argument("--guild", default="900000000000000002")
    ap.add_argument("--user", default="800000000000000000")
    a = ap.parse_args()
    for f in glob.glob(os.path.join(BASE, "data", "cache", f"*{a.guild}*")):
        os.remove(f)
    t0 = time.perf_counter()
    import app as dashboard
    print(f"import app: {time.perf_counter()-t0:.2f}s")
    admin = sorted(dashboard.ADMIN_IDS)[0]
    c = dashboard.app.test_client()
    with c.session_transaction() as s:
        s["user"] = {"id": admin, "username": "admin", "avatar": None}
        s["server_id"] = a.guild

    def hit(url):
        t = time.perf_counter(); r = c.get(url); return (time.perf_counter() - t) * 1000, r

    ms, r = hit("/"); print(f"home cold: {ms:.0f}ms status={r.status_code} bytes={len(r.data)}")
    # wait until the homepage contains leaderboard data
    t = time.perf_counter(); ready = None
    while time.perf_counter() - t < 900:
        _, r = hit("/")
        body = r.get_data(as_text=True)
        if f"/user/{a.user}" in body:
            ready = time.perf_counter() - t; break
        time.sleep(0.5)
    print(f"home data ready after: {ready if ready is None else round(ready,1)}s")
    time.sleep(1)
    warm = [hit("/")[0] for _ in range(10)]
    print(f"home warm median: {statistics.median(warm):.1f}ms  max={max(warm):.1f}ms")
    ms, r = hit("/api/leaderboard"); print(f"api/leaderboard: {ms:.1f}ms status={r.status_code}")
    ms, r = hit("/api/leaderboard?page=3"); print(f"api/leaderboard p3: {ms:.1f}ms status={r.status_code}")
    ms, r = hit(f"/user/{a.user}"); print(f"profile cold: {ms:.0f}ms status={r.status_code}")
    time.sleep(8)
    pw = [hit(f"/user/{a.user}")[0] for _ in range(5)]
    print(f"profile warm median: {statistics.median(pw):.1f}ms")
    for url in ("/api/v1/summary", "/api/v1/wordcloud", "/api/v1/heatmap"):
        ms, r = hit(url); print(f"{url}: {ms:.1f}ms status={r.status_code}")
    size = sum(os.path.getsize(f) for f in glob.glob(os.path.join(BASE, "data", "cache", f"*{a.guild}*")))
    print(f"pickle cache bytes for guild: {size}")


if __name__ == "__main__" and "--incremental" not in sys.argv:
    main()


def incremental_check(guild="900000000000000002"):
    """After-refactor only: cost of absorbing 1,000 new messages + full rebuild cost."""
    import sqlite3
    from dixo import summary
    import app as dashboard
    path = dashboard.server_db_path(guild)
    c = sqlite3.connect(path)
    base = c.execute("SELECT MAX(rowid) FROM messages").fetchone()[0]
    c.executemany("INSERT OR IGNORE INTO messages(message_id,thread_id,author_id,content,timestamp) VALUES(?,?,?,?,?)",
                  [(f"bench-{base}-{i}", "700000000000000001", "800000000000000001", "增量 测试 incremental bench", "2025-08-01T00:00:00+00:00") for i in range(1000)])
    c.commit()
    t = time.perf_counter(); r = summary.refresh(path); print(f"incremental refresh (+1000 msgs): {(time.perf_counter()-t)*1000:.0f}ms {r['caught_up']}")
    t = time.perf_counter(); r = summary.refresh(path); print(f"no-op refresh: {(time.perf_counter()-t)*1000:.1f}ms")
    c.execute("DELETE FROM messages WHERE message_id LIKE 'bench-%'"); c.commit(); c.close()
    t = time.perf_counter(); r = summary.refresh(path, budget_seconds=0); print(f"full rebuild after delete: {time.perf_counter()-t:.1f}s")


if __name__ == "__main__" and "--incremental" in sys.argv:
    incremental_check()
