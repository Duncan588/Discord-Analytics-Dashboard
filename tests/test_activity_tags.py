from dixo.dashboard_data import (
    activity_profile, classify_member_activity, TAG_ALWAYS, TAG_FADED, TAG_RARE, TAG_RETURNED, TAG_NEW,
)


def _months(a, b):
    from dixo.dashboard_data import _month_range
    return set(_month_range(a, b))


def test_new_member():
    assert classify_member_activity({"2026-09", "2026-10"}, "2026-09", "2026-10", "2026-10") == TAG_NEW


def test_always_active():
    assert classify_member_activity(_months("2025-01", "2026-09"), "2025-01", "2026-09", "2026-10") == TAG_ALWAYS


def test_faded_counts_from_last_message():
    tag, info = activity_profile({"2025-01", "2025-02", "2025-03"}, "2025-01", "2026-09")
    assert tag == TAG_FADED and info["months_since_last"] == 18


def test_returned_reports_streak():
    act = _months("2022-09", "2025-12") | {"2026-08", "2026-09"}
    tag, info = activity_profile(act, "2022-09", "2026-10")
    assert tag == TAG_RETURNED and info["returned_months"] == 2 and info["months_since_last"] == 1


def test_rare():
    assert classify_member_activity({"2025-06"}, "2025-01", "2025-06", "2026-09") == TAG_RARE
