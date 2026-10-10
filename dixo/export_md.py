"""Per-member Markdown export of chat history (for AI bots / RAG).

Layout (default root data/exports/<gid>/members/):
    <user_id>/README.md         summary header (YAML front-matter + stats); rewritten every run
    <user_id>/history-001.md    messages grouped by thread, then by local date; append-only
    <user_id>/history-002.md    ... new part once a part exceeds --max-bytes
    _state.json                 per-user rowid watermark -> incremental runs only append new messages

Merged accounts (user_merges) are exported under the parent id. Blocked/opted-out users can be
skipped with --exclude-file (one id per line).

CLI:
    python -m dixo.export_md data/servers/<gid>/discord_data.db [--user ID] [--out DIR] [--max-bytes 500000]
"""
import argparse
import collections
import json
import os
import re
import sqlite3
import sys
from datetime import datetime, timezone

from . import dashboard_data, summary, textutil

_MENTION_RE = re.compile(r"<@!?(\d+)>")
_EMOJI_RE = re.compile(r"<a?:(\w+):\d+>")
_CHANNEL_RE = re.compile(r"<#(\d+)>")


def _connect(path):
    c = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=30)
    c.row_factory = sqlite3.Row
    return c


def _fmt_ts(ts):
    dt = summary._parse_ts(ts)
    return (dt.strftime("%Y-%m-%d"), dt.strftime("%H:%M")) if dt else ("unknown-date", "--:--")


def _clean(content, names):
    content = _MENTION_RE.sub(lambda m: "@" + names.get(m.group(1), m.group(1)), content or "")
    content = _EMOJI_RE.sub(r":\1:", content)
    content = _CHANNEL_RE.sub(r"#\1", content)
    return content.strip().replace("\r", "")


def _yaml_str(v):
    return json.dumps("" if v is None else str(v), ensure_ascii=False)


class Exporter:
    def __init__(self, main_db_path, out_dir=None, max_bytes=500_000, exclude=()):
        self.db_path = main_db_path
        gid = os.path.basename(os.path.dirname(main_db_path))
        root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(main_db_path))))  # data/
        self.out = out_dir or os.path.join(root, "exports", gid, "members")
        self.gid = gid
        self.max_bytes = max_bytes
        self.exclude = set(exclude)
        os.makedirs(self.out, exist_ok=True)
        self.state_path = os.path.join(self.out, "_state.json")
        try:
            with open(self.state_path, encoding="utf-8") as fh:
                self.state = json.load(fh)
        except (OSError, ValueError):
            self.state = {}
        self.main = _connect(main_db_path)
        spath = summary.summary_path(main_db_path)
        self.summ = _connect(spath) if os.path.exists(spath) else None
        self.merges = {str(r[0]): str(r[1]) for r in self.main.execute("SELECT target_id,parent_id FROM user_merges")}
        self._names = None

    def names(self):
        if self._names is None:
            self._names = {str(r[0]): (r[2] or r[1] or str(r[0])) for r in self.main.execute("SELECT user_id,username,nickname FROM users")}
        return self._names

    def member_ids(self):
        if self.summ:
            ids = [r[0] for r in self.summ.execute("SELECT user_id FROM user_stats WHERE msg_count>0 ORDER BY msg_count DESC")]
        else:
            ids = [r[0] for r in self.main.execute("SELECT DISTINCT author_id FROM messages")]
        return [i for i in ids if i and str(i) not in self.merges and str(i) not in self.exclude]

    def merged_ids(self, uid):
        return [str(uid)] + sorted(k for k, v in self.merges.items() if v == str(uid))

    def _thread_names(self, tids):
        out = {}
        tids = list(tids)
        for i in range(0, len(tids), 500):
            chunk = tids[i:i + 500]
            ph = ",".join("?" for _ in chunk)
            out.update({str(r[0]): r[1] for r in self.main.execute(f"SELECT thread_id,name FROM threads WHERE thread_id IN ({ph})", chunk)})
        return out

    def _reply_snippets(self, ids):
        out = {}
        ids = [i for i in ids if i]
        for i in range(0, len(ids), 500):
            chunk = ids[i:i + 500]
            ph = ",".join("?" for _ in chunk)
            for r in self.main.execute(f"SELECT message_id,author_id,content FROM messages WHERE message_id IN ({ph})", chunk):
                out[str(r[0])] = (r[1], (r[2] or "").replace("\n", " ")[:80])
        return out

    def export_user(self, uid):
        uid = str(uid)
        ids = self.merged_ids(uid)
        st = self.state.get(uid, {"wm": 0, "part": 1})
        ph = ",".join("?" for _ in ids)
        rows = self.main.execute(
            f"SELECT rowid,message_id,thread_id,content,timestamp,reply_to_msg_id FROM messages "
            f"WHERE author_id IN ({ph}) AND rowid>? ORDER BY thread_id,timestamp,message_id", (*ids, st["wm"])).fetchall()
        udir = os.path.join(self.out, uid)
        os.makedirs(udir, exist_ok=True)
        if rows:
            names = self.names()
            tnames = self._thread_names({r["thread_id"] for r in rows})
            replies = self._reply_snippets({r["reply_to_msg_id"] for r in rows})
            by_thread = collections.OrderedDict()
            for r in rows:
                by_thread.setdefault(r["thread_id"], []).append(r)
            # threads ordered by their first message time
            ordered = sorted(by_thread.items(), key=lambda kv: kv[1][0]["timestamp"] or "")
            chunks = []
            continued = st["wm"] > 0
            for tid, msgs in ordered:
                lines = [f"\n## 帖子 / Thread: {tnames.get(str(tid), tid)}{' (续 / cont.)' if continued else ''}\n",
                         f"<!-- thread_id: {tid} -->\n"]
                cur_day = None
                for m in msgs:
                    day, hm = _fmt_ts(m["timestamp"])
                    if day != cur_day:
                        lines.append(f"\n### {day}\n\n"); cur_day = day
                    text = _clean(m["content"], names) or "_(无文本 / no text: attachment or embed)_"
                    text = text.replace("\n", "\n  ")
                    rep = replies.get(str(m["reply_to_msg_id"])) if m["reply_to_msg_id"] else None
                    prefix = f"↪ 回复 @{names.get(str(rep[0]), rep[0])}: “{rep[1]}” — " if rep else ""
                    lines.append(f"- **{hm}** {prefix}{text}\n")
                chunks.append("".join(lines))
            self._append(udir, st, chunks)
            st["wm"] = max(r["rowid"] for r in rows)
        self._write_readme(uid, ids, udir, st)
        self.state[uid] = st
        return len(rows)

    def _part_path(self, udir, n):
        return os.path.join(udir, f"history-{n:03d}.md")

    def _append(self, udir, st, chunks):
        path = self._part_path(udir, st["part"])
        size = os.path.getsize(path) if os.path.exists(path) else 0
        fh = open(path, "a", encoding="utf-8")
        try:
            if size == 0:
                fh.write(f"# 聊天记录 / Chat history — part {st['part']}\n")
            for chunk in chunks:
                b = len(chunk.encode("utf-8"))
                if size and size + b > self.max_bytes:
                    fh.close()
                    st["part"] += 1
                    path = self._part_path(udir, st["part"])
                    fh = open(path, "a", encoding="utf-8")
                    fh.write(f"# 聊天记录 / Chat history — part {st['part']}\n")
                    size = 0
                fh.write(chunk); size += b
        finally:
            fh.close()

    def _write_readme(self, uid, ids, udir, st):
        u = self.main.execute("SELECT * FROM users WHERE user_id=?", (uid,)).fetchone()
        u = dict(u) if u else {"username": uid, "nickname": ""}
        prof = dashboard_data.profile_base(self.main, self.summ, ids) if self.summ else {}
        ph = ",".join("?" for _ in ids)
        texts = [r[0] for r in self.main.execute(
            f"SELECT content FROM messages WHERE author_id IN ({ph}) ORDER BY timestamp DESC LIMIT 2000", ids)]
        words = [w["text"] for w in textutil.format_word_cloud(textutil.count_words(texts), 20)]
        top_threads = self.main.execute(
            f"SELECT m.thread_id, t.name, count(*) c FROM messages m LEFT JOIN threads t ON t.thread_id=m.thread_id "
            f"WHERE m.author_id IN ({ph}) GROUP BY m.thread_id ORDER BY c DESC LIMIT 10", ids).fetchall()
        hours = prof.get("chart_hourly") or []
        peak = max(hours, key=lambda h: h["c"])["hour"] if hours and any(h["c"] for h in hours) else None
        parts = [f"history-{n:03d}.md" for n in range(1, st["part"] + 1) if os.path.exists(self._part_path(udir, n))]
        fm = [
            "---",
            f"user_id: {_yaml_str(uid)}",
            f"username: {_yaml_str(u.get('username'))}",
            f"display_name: {_yaml_str(u.get('nickname') or u.get('username'))}",
            f"merged_ids: {json.dumps(ids)}",
            f"guild_id: {_yaml_str(self.gid)}",
            f"message_count: {prof.get('msg_count', len(texts))}",
            f"reactions_received: {prof.get('reaction_received_count', 0)}",
            f"reactions_given: {prof.get('reaction_given_count', 0)}",
            f"threads_participated: {prof.get('thread_count', 0)}",
            f"threads_started: {prof.get('threads_started', 0)}",
            f"active_days: {prof.get('active_days', 0)}",
            f"first_message_at: {_yaml_str(prof.get('first_msg_at'))}",
            f"last_message_at: {_yaml_str(prof.get('last_msg_at'))}",
            f"peak_hour_local: {_yaml_str(peak)}",
            f"top_words: {json.dumps(words, ensure_ascii=False)}",
            f"history_files: {json.dumps(parts)}",
            f"generated_at: {_yaml_str(datetime.now(timezone.utc).isoformat(timespec='seconds'))}",
            "timezone: \"Asia/Shanghai (UTC+8)\"",
            "---", "",
            f"# {u.get('nickname') or u.get('username')} (@{u.get('username')})", "",
            "## 概览 / Summary", "",
            f"- 消息 / Messages: **{prof.get('msg_count', len(texts))}**，收到反应 / reactions received: {prof.get('reaction_received_count', 0)}",
            f"- 活跃时间 / Active: {prof.get('first_msg_at') or '?'} → {prof.get('last_msg_at') or '?'}（{prof.get('active_days', 0)} 天），高峰 {peak or '?'}",
            f"- 常用词 / Top words: {', '.join(words) or '—'}", "",
            "## 最常发言的帖子 / Most active threads", "",
        ]
        fm += [f"- {r['name'] or r['thread_id']} — {r['c']} 条" for r in top_threads]
        fm += ["", "## 聊天记录文件 / History files", ""] + [f"- [{p}]({p})" for p in parts] + [""]
        with open(os.path.join(udir, "README.md"), "w", encoding="utf-8") as fh:
            fh.write("\n".join(fm))

    def save_state(self):
        tmp = self.state_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(self.state, fh)
        os.replace(tmp, self.state_path)

    def run(self, users=None):
        total = 0
        for i, uid in enumerate(users or self.member_ids(), 1):
            total += self.export_user(uid)
            if i % 50 == 0:
                self.save_state()
        self.save_state()
        return total


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("db"); ap.add_argument("--user", action="append"); ap.add_argument("--out")
    ap.add_argument("--max-bytes", type=int, default=500_000); ap.add_argument("--exclude-file")
    a = ap.parse_args(argv)
    exclude = []
    if a.exclude_file:
        with open(a.exclude_file, encoding="utf-8") as fh:
            exclude = [l.strip() for l in fh if l.strip()]
    ex = Exporter(a.db, a.out, a.max_bytes, exclude)
    n = ex.run(a.user)
    print(json.dumps({"exported_messages": n, "out": ex.out}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
