from dixo.dashboard_data import (
    classify_member_activity, TAG_ALWAYS, TAG_FADED, TAG_RARE, TAG_RETURNED, TAG_NEW,
)


def test_new_member():
    assert classify_member_activity({"2026-09", "2026-10"}, "2026-09", "2026-10", "2026-10") == TAG_NEW


def test_always_active():
    months = {f"2025-{m:02d}" for m in range(1, 13)} | {f"2026-{m:02d}" for m in range(1, 10)}
    assert classify_member_activity(months, "2025-01", "2026-09", "2026-09") == TAG_ALWAYS


def test_faded():
    assert classify_member_activity({"2025-01", "2025-02", "2025-03"}, "2025-01", "2025-03", "2026-09") == TAG_FADED


def test_returned():
    act = {"2025-01", "2025-02", "2026-08", "2026-09"}
    assert classify_member_activity(act, "2025-01", "2026-09", "2026-09") == TAG_RETURNED


def test_rare():
    assert classify_member_activity({"2025-06"}, "2025-01", "2025-06", "2026-09") == TAG_RARE
