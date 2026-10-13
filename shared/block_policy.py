"""Pure decision logic for bot-side block enforcement (user_blocks in portal.db).

A message is intercepted when its sender has a block relation (either direction)
with ANY target: the thread owner (forum threads), the author of the replied-to
message, or any mentioned user. The sender is never a target. The thread owner is
checked first, so "owner blocked you" wins over any other relation.
"""


def collect_targets(sender_id, owner_id=None, reply_author_id=None, mention_ids=()):
    out = []
    for t in [owner_id, reply_author_id, *mention_ids]:
        if t is None:
            continue
        t = int(t)
        if t != int(sender_id) and t not in out:
            out.append(t)
    return out


def relation(pairs, sender_id, target_id):
    """pairs: iterable of (blocker_id, blocked_id). Returns relation string or None."""
    s, t = int(sender_id), int(target_id)
    p = {(int(a), int(b)) for a, b in pairs}
    sb, tb = (s, t) in p, (t, s) in p
    if sb and tb:
        return "mutual"
    if sb:
        return "sender_blocked_recipient"
    if tb:
        return "recipient_blocked_sender"
    return None


def pick(pairs, sender_id, targets):
    """First (target, relation) in priority order (owner first), or None."""
    pairs = list(pairs)
    for t in targets:
        rel = relation(pairs, sender_id, t)
        if rel:
            return t, rel
    return None


def query(sender_id, targets):
    """SQL + args returning (blocker_id, blocked_id) rows between sender and targets."""
    ph = ",".join("?" for _ in targets)
    sql = (f"SELECT blocker_id, blocked_id FROM user_blocks WHERE "
           f"(blocker_id=? AND blocked_id IN ({ph})) OR (blocked_id=? AND blocker_id IN ({ph}))")
    return sql, [int(sender_id), *map(int, targets), int(sender_id), *map(int, targets)]
