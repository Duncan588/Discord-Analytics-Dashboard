"""Drives whitelist_bot.Bot._enforce_blocks with fakes (no Discord connection)."""
import asyncio
import os
import sys
import types
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "Preparation_Before_Use"))
os.environ.setdefault("DISCORD_BOT_TOKEN", "")
import whitelist_bot as wb  # noqa: E402

OWNER, U = 1423645729490210970, 1021053983642636289


class FakeSelf:
    _enforce_blocks = wb.Bot._enforce_blocks

    def __init__(self, pairs, owner):
        self.pairs, self.owner, self.deleted, self.counted = pairs, owner, [], []

    def _forum_owner_id(self, ch):
        return self.owner

    async def _reply_author_id(self, m):
        return m.reply_author

    async def _block_pairs(self, sender, targets):
        return [p for p in self.pairs if sender in p and (p[0] in targets or p[1] in targets)]

    async def _delete_intercepted(self, m, what):
        self.deleted.append((m.id, what)); return True

    async def _increment_block_count(self, g, s, t):
        self.counted.append((s, t))


def msg(author, reply_author=None, mentions=()):
    async def create_dm():
        async def send(text): pass
        return types.SimpleNamespace(send=send)
    a = types.SimpleNamespace(id=author, create_dm=create_dm)
    return types.SimpleNamespace(id=1, author=a, reply_author=reply_author, content="hi",
                                 mentions=[types.SimpleNamespace(id=i) for i in mentions],
                                 guild=types.SimpleNamespace(id=9), channel=types.SimpleNamespace(id=7, mention="#t"))


class EnforceTest(unittest.TestCase):
    def run_(self, fake, m, edited=False):
        asyncio.run(fake._enforce_blocks(m, edited=edited))

    def test_prod_bypass_now_deleted(self):
        f = FakeSelf([(OWNER, U)], OWNER)
        self.run_(f, msg(U, reply_author=U, mentions=[OWNER]))
        self.assertEqual(f.deleted, [(1, "message")]); self.assertEqual(f.counted, [(U, OWNER)])

    def test_edit_enforced(self):
        f = FakeSelf([(OWNER, U)], OWNER)
        self.run_(f, msg(U, reply_author=555), edited=True)
        self.assertEqual(f.deleted, [(1, "edit")])

    def test_unrelated_untouched(self):
        f = FakeSelf([(OWNER, 42)], OWNER)
        self.run_(f, msg(U, reply_author=U))
        self.assertEqual(f.deleted, [])


if __name__ == "__main__":
    unittest.main()
