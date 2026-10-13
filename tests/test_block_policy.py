import sqlite3
import unittest

from shared import block_policy as bp

OWNER, U, OTHER, M2 = 1423645729490210970, 1021053983642636289, 1343848322603028480, 1020844413833981962


class BlockPolicyTest(unittest.TestCase):
    def test_reply_to_self_mentioning_owner_is_caught(self):
        # the prod bypass: U replies to own message, @mentions the owner, in owner's thread
        t = bp.collect_targets(U, owner_id=OWNER, reply_author_id=U, mention_ids=[OWNER])
        self.assertEqual(t, [OWNER])
        self.assertEqual(bp.pick([(OWNER, U)], U, t), (OWNER, "recipient_blocked_sender"))

    def test_reply_to_third_party_in_owner_thread(self):
        t = bp.collect_targets(U, owner_id=OWNER, reply_author_id=OTHER, mention_ids=[OTHER])
        self.assertEqual(bp.pick([(OWNER, U)], U, t)[0], OWNER)

    def test_owner_wins_and_multi_mentions(self):
        t = bp.collect_targets(U, owner_id=OWNER, mention_ids=[M2, OTHER])
        self.assertEqual(t, [OWNER, M2, OTHER])
        self.assertEqual(bp.pick([(M2, U), (OWNER, U)], U, t), (OWNER, "recipient_blocked_sender"))
        # no owner relation -> any mentioned user still triggers (multi-mention used to be skipped)
        self.assertEqual(bp.pick([(U, OTHER)], U, t), (OTHER, "sender_blocked_recipient"))

    def test_mutual_and_none(self):
        self.assertEqual(bp.relation([(U, OWNER), (OWNER, U)], U, OWNER), "mutual")
        self.assertIsNone(bp.pick([(OTHER, M2)], U, [OWNER]))
        self.assertEqual(bp.collect_targets(OWNER, owner_id=OWNER, mention_ids=[OWNER]), [])

    def test_query_matches_both_directions(self):
        c = sqlite3.connect(":memory:")
        c.execute("CREATE TABLE user_blocks(blocker_id INTEGER, blocked_id INTEGER, created_at TEXT)")
        c.executemany("INSERT INTO user_blocks VALUES(?,?,'x')", [(OWNER, U), (U, M2), (OTHER, M2)])
        sql, args = bp.query(U, [OWNER, M2, OTHER])
        self.assertEqual(sorted(c.execute(sql, args).fetchall()), sorted([(OWNER, U), (U, M2)]))


if __name__ == "__main__":
    unittest.main()
