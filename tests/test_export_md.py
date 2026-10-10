import json
import os
import sqlite3

from dixo import export_md, summary


def test_export_incremental_and_split(tmp_copy, tmp_path):
    summary.refresh(tmp_copy, budget_seconds=0)
    out = str(tmp_path / "md")
    ex = export_md.Exporter(tmp_copy, out, max_bytes=4000)
    uid = ex.member_ids()[0]
    n1 = ex.export_user(uid); ex.save_state()
    assert n1 > 0
    udir = os.path.join(out, uid)
    readme = open(os.path.join(udir, "README.md"), encoding="utf-8").read()
    assert readme.startswith("---") and f'user_id: "{uid}"' in readme and "message_count:" in readme
    parts = sorted(p for p in os.listdir(udir) if p.startswith("history-"))
    assert len(parts) >= 2  # split by size
    body = "".join(open(os.path.join(udir, p), encoding="utf-8").read() for p in parts)
    assert "## 帖子 / Thread:" in body and "### " in body
    assert body.count("\n- **") == n1
    # incremental: nothing new -> nothing appended
    ex2 = export_md.Exporter(tmp_copy, out, max_bytes=4000)
    assert ex2.export_user(uid) == 0
    c = sqlite3.connect(tmp_copy)
    tid = c.execute("SELECT thread_id FROM threads LIMIT 1").fetchone()[0]
    c.execute("INSERT INTO messages(message_id,thread_id,author_id,content,timestamp) VALUES('new-1',?,?,?,?)",
              (tid, uid, "hello <@900000000000000102> <:pepe:1>", "2026-10-10T10:00:00+00:00")); c.commit()
    assert ex2.export_user(uid) == 1
    last = open(os.path.join(udir, sorted(p for p in os.listdir(udir) if p.startswith("history-"))[-1]), encoding="utf-8").read()
    assert "(续 / cont.)" in last and ":pepe:" in last and "<@" not in last
    assert json.load(open(os.path.join(out, "_state.json")))[uid]["wm"] > 0
