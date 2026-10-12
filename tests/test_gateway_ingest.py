import os
import sqlite3
import tempfile
import unittest

from dixo import summary
from shared import ingest_db


def counts(spath):
    c = sqlite3.connect(spath)
    try:
        return {
            "users": dict(c.execute("SELECT user_id,msg_count FROM user_stats WHERE msg_count>0")),
            "recv": dict(c.execute("SELECT user_id,react_recv FROM user_stats WHERE react_recv>0")),
            "threads": dict(c.execute("SELECT thread_id,msg_count FROM thread_stats")),
            "hours": sum(r[0] for r in c.execute("SELECT c FROM msg_hour")),
            "words": dict(c.execute("SELECT word,c FROM word")),
            "react": dict(c.execute("SELECT message_id,c FROM msg_react")),
            "gen": c.execute("SELECT value FROM meta WHERE key='delete_gen'").fetchone()[0],
        }
    finally:
        c.close()


def msg(i, tid="t1", author="u1", content="hello world"):
    return {"message_id": str(i), "thread_id": tid, "author_id": author, "content": content,
            "timestamp": "2026-10-0%dT10:00:00.000+00:00" % (1 + i % 5), "reply_to": None,
            "attachments": [], "mentions": []}


class IngestSummaryTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.path = os.path.join(self.dir, "discord_data.db")
        self.spath = summary.summary_path(self.path)

    def write(self, fn):
        c = ingest_db.connect(self.path)
        try:
            r = fn(c); c.commit(); return r
        finally:
            c.close()

    def rebuild_reference(self):
        """Counts from a fresh full build for comparison."""
        for f in os.listdir(self.dir):
            if f.startswith("summary.db"):
                os.remove(os.path.join(self.dir, f))
        summary.refresh(self.path, budget_seconds=0)
        return counts(self.spath)

    def test_incremental_matches_full_rebuild(self):
        self.write(lambda c: [ingest_db.insert_message(c, msg(i, author="u%d" % (i % 3))) for i in range(1, 11)])
        self.write(lambda c: [ingest_db.add_reaction(c, str(i), "u9", "👍", None) for i in range(1, 6)])
        summary.refresh(self.path, budget_seconds=0)
        gen0 = counts(self.spath)["gen"]
        # gateway removals + the max rowid deleted and re-used
        self.write(lambda c: ingest_db.tombstone_delete_messages(c, ["10", "3"]))
        self.write(lambda c: ingest_db.remove_reaction(c, "1", "u9", "👍"))
        self.write(lambda c: ingest_db.clear_reactions(c, "2"))
        summary.refresh(self.path, budget_seconds=0)
        self.write(lambda c: ingest_db.insert_message(c, msg(11, tid="t2", author="u7", content="new words here")))
        # backfill re-import of t1 (thread removed then re-inserted)
        self.write(lambda c: ingest_db.remove_thread(c, "t1"))
        self.write(lambda c: [ingest_db.insert_message(c, msg(i, author="u%d" % (i % 3))) for i in (1, 2, 4, 5)])
        self.write(lambda c: ingest_db.add_reaction(c, "4", "u8", "🔥", None))
        summary.refresh(self.path, budget_seconds=0)
        inc = counts(self.spath)
        self.assertEqual(inc["gen"], gen0, "tombstoned deletes must not force a full rebuild")
        ref = self.rebuild_reference()
        for k in ("users", "recv", "threads", "hours", "words", "react"):
            self.assertEqual(inc[k], ref[k], k)

    def test_plain_delete_still_bumps_gen(self):
        self.write(lambda c: ingest_db.insert_message(c, msg(1)))
        c = ingest_db.connect(self.path)
        g0 = summary.delete_generation(c)
        c.execute("DELETE FROM messages"); c.commit()
        self.assertEqual(summary.delete_generation(c), g0 + 1)
        c.close()

    def test_iso_ms_and_duplicates(self):
        from datetime import datetime, timezone
        self.assertEqual(ingest_db.iso_ms(datetime(2026, 9, 23, 16, 11, 19, 894123, tzinfo=timezone.utc)),
                         "2026-09-23T16:11:19.894+00:00")
        self.assertTrue(self.write(lambda c: ingest_db.insert_message(c, msg(1))))
        self.assertFalse(self.write(lambda c: ingest_db.insert_message(c, msg(1))))
        self.assertTrue(self.write(lambda c: ingest_db.add_reaction(c, "1", "u2", "x", None)))
        self.assertFalse(self.write(lambda c: ingest_db.add_reaction(c, "1", "u2", "x", None)))


if __name__ == "__main__":
    unittest.main()
