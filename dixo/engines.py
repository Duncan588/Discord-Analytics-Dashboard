"""Request-side engines on top of summary.db (replaces DataEngine/ProfileEngine pickles).

- Freshness check = MAX(rowid) of messages/reactions vs summary watermarks (no file mtime: K28).
- Refresh runs in a background thread per server (stale-while-refresh), bounded by a time budget,
  and re-schedules itself until caught up.
- Homepage payload is memoised in-process per (server, watermarks) — building it is ~ms anyway.
"""
import json
import os
import threading
import time

from . import dashboard_data, summary


class SummaryEngine:
    HOMEPAGE_VERSION = 7

    def __init__(self, server_db_path, connect, cloud_enabled, logger, refresh_interval=10.0, budget=20.0, after_refresh=None):
        self.server_db_path = server_db_path
        self.connect = connect            # bridge-aware sqlite connect (shared.sqlite_utils.connect_sqlite)
        self.cloud_enabled = cloud_enabled
        self.log = logger
        self.refresh_interval = refresh_interval
        self.budget = budget
        self._lock = threading.Lock()
        self._running = set()
        self._last_start = {}
        self._memo = {}
        self.after_refresh = after_refresh  # e.g. search-index refresh

    # ---- connections ----
    def summary_conn(self, sid):
        path = summary.summary_path(self.server_db_path(sid))
        if not self.cloud_enabled() and not os.path.exists(path):
            summary.connect_summary(path).close()   # create schema
        return self.connect(path)

    # ---- refresh ----
    def _refresh(self, sid):
        try:
            res = summary.refresh(self.server_db_path(sid), budget_seconds=self.budget)
            self.log.info("summary.refresh sid=%s %s", sid, res)
            again = not res["caught_up"]
            if res["caught_up"]:
                try:
                    sc = self.summary_conn(sid)
                    try:
                        msgs = sc.execute("SELECT COALESCE(SUM(msg_count),0) FROM user_stats").fetchone()[0]
                        reacts = sc.execute("SELECT COALESCE(SUM(c),0) FROM msg_react").fetchone()[0]
                        threads = sc.execute("SELECT COUNT(*) FROM thread_stats").fetchone()[0]
                        dashboard_data.save_last_good_kpis(sc, msgs, reacts, threads)
                    finally:
                        sc.close()
                    self._memo.pop(str(sid), None)
                except Exception:
                    self.log.exception("save_last_good_kpis failed sid=%s", sid)
                if self.after_refresh:
                    self.after_refresh(sid)
        except Exception:
            self.log.exception("summary refresh failed sid=%s", sid)
            again = False
        finally:
            with self._lock:
                self._running.discard(sid)
        if again:
            self.schedule(sid, force=True)

    def schedule(self, sid, force=False):
        if self.cloud_enabled():
            return  # production host owns the refresh; local cloud mode is read-only
        sid = str(sid)
        with self._lock:
            now = time.monotonic()
            if sid in self._running or (not force and now - self._last_start.get(sid, 0) < self.refresh_interval):
                return
            self._running.add(sid)
            self._last_start[sid] = now
        t = threading.Thread(target=self._refresh, args=(sid,), name=f"summary-{sid}", daemon=True)
        t.start()

    def is_refreshing(self, sid):
        with self._lock:
            return str(sid) in self._running

    def freshness(self, main_conn, summ_conn):
        st = summary.state(summ_conn)
        max_m = main_conn.execute("SELECT MAX(rowid) FROM messages").fetchone()[0] or 0
        max_r = main_conn.execute("SELECT MAX(rowid) FROM reactions").fetchone()[0] or 0
        st.update(max_msg=max_m, max_react=max_r, stale=(st["msg_wm"] < max_m or st["react_wm"] < max_r))
        # Prefer shadow-build watermarks for the progress banner while a full rebuild runs.
        build_path = None
        try:
            for row in summ_conn.execute("PRAGMA database_list"):
                if row[1] == "main" and row[2]:
                    build_path = row[2] + ".build"
                    break
        except Exception:
            build_path = None
        if build_path and os.path.exists(build_path):
            try:
                bc = summary.connect_summary(build_path, readonly=True)
                try:
                    bst = summary.state(bc)
                    if bst["msg_wm"] >= st["msg_wm"]:
                        st["msg_wm"] = bst["msg_wm"]
                        st["react_wm"] = bst["react_wm"]
                        st["shadow"] = True
                        st["stale"] = True
                finally:
                    bc.close()
            except Exception:
                pass
        st["progress"] = round(100.0 * st["msg_wm"] / max_m, 1) if max_m else 100.0
        return st

    # ---- payloads ----
    def homepage(self, sid, main_conn):
        sid = str(sid)
        summ = self.summary_conn(sid)
        try:
            st = self.freshness(main_conn, summ)
            if st["stale"]:
                self.schedule(sid)
            key = (sid, st["msg_wm"], st["react_wm"], self.HOMEPAGE_VERSION)
            memo = self._memo.get(sid)
            if memo and memo[0] == key:
                data = memo[1]
            else:
                data = dashboard_data.homepage(main_conn, summ)
                # Do not memoize mid-rebuild crumbs (e.g. wiped summary at 1% progress).
                if not st.get("stale") or st.get("progress", 100) >= 95:
                    self._memo[sid] = (key, data)
            out = dict(data)
            out["summary_state"] = st
            return out
        finally:
            summ.close()

    def invalidate(self, sid):
        self._memo.pop(str(sid), None)


class ProfileEngine:
    """Profile = cheap base numbers from summary (always live) + cached heavy extras."""
    EXTRA_VERSION = 3
    EXTRA_MAX_AGE = 3600.0

    def __init__(self, summary_engine, logger):
        self.se = summary_engine
        self.log = logger
        self._lock = threading.Lock()
        self._running = set()
        self._mem = {}

    def _key(self, sid, ids):
        return f"{sid}:{','.join(sorted(ids))}:v{self.EXTRA_VERSION}"

    def _build_extra(self, sid, ids):
        key = self._key(sid, ids)
        try:
            main = self.se.connect(self.se.server_db_path(sid))
            try:
                data = dashboard_data.profile_extra(main, ids)
            finally:
                main.close()
            self._mem[key] = (time.time(), data)
            if not self.se.cloud_enabled():
                summ = self.se.summary_conn(sid)
                try:
                    wm = summary.state(summ)["msg_wm"]
                    summ.execute("INSERT OR REPLACE INTO profile_extra(user_id,wm,built_at,data) VALUES(?,?,?,?)",
                                 (key, wm, time.time(), json.dumps(data, ensure_ascii=False)))
                    summ.commit()
                finally:
                    summ.close()
        except Exception:
            self.log.exception("profile extra build failed sid=%s ids=%s", sid, ids)
        finally:
            with self._lock:
                self._running.discard(key)

    def _schedule(self, sid, ids):
        key = self._key(sid, ids)
        with self._lock:
            if key in self._running:
                return
            self._running.add(key)
        threading.Thread(target=self._build_extra, args=(sid, ids), daemon=True, name=f"profile-{key[:40]}").start()

    def load(self, sid, ids, main_conn, wait=0.0):
        sid = str(sid); ids = [str(i) for i in ids]
        summ = self.se.summary_conn(sid)
        try:
            base = dashboard_data.profile_base(main_conn, summ, ids)
            key = self._key(sid, ids)
            cached = self._mem.get(key)
            if cached is None:
                try:
                    row = summ.execute("SELECT built_at,data FROM profile_extra WHERE user_id=?", (key,)).fetchone()
                except Exception:
                    row = None
                if row:
                    cached = (row[0], json.loads(row[1]))
                    self._mem[key] = cached
        finally:
            summ.close()
        if cached is None:
            self._schedule(sid, ids)
            if wait:
                deadline = time.monotonic() + wait
                while time.monotonic() < deadline and self._key(sid, ids) not in self._mem:
                    time.sleep(0.05)
                cached = self._mem.get(self._key(sid, ids))
        elif time.time() - cached[0] > self.EXTRA_MAX_AGE:
            self._schedule(sid, ids)
        extra = cached[1] if cached else {"interactions_incoming": [], "interactions_outgoing": [], "word_cloud_data": []}
        base.update(extra)
        base["complete"] = cached is not None
        return base
