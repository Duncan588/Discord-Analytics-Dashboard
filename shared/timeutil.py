"""UTC timestamp helpers used by all V20 processes."""

from datetime import datetime, timedelta, timezone


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def parse_utc_datetime(value):
    if not value:
        return None
    try:
        text = str(value).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        result = datetime.fromisoformat(text)
        if result.tzinfo is None:
            result = result.replace(tzinfo=timezone.utc)
        return result.astimezone(timezone.utc)
    except (TypeError, ValueError):
        return None


def to_local_datetime(value, offset_hours=8):
    parsed = parse_utc_datetime(value)
    return parsed.astimezone(timezone(timedelta(hours=offset_hours))) if parsed else None

