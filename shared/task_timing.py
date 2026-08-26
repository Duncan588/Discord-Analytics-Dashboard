"""Backend-only timing calculations for download tasks."""

import math
from datetime import datetime, timezone

from shared.timeutil import parse_utc_datetime


def _get(task, key, default=None):
    try:
        value = task[key]
    except (KeyError, IndexError, TypeError):
        return default
    return default if value is None else value


def _number(value, default=0):
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return default


def calculate_task_timing(task, current=None):
    """Return elapsed seconds and ETA calculated on the server.

    ``elapsed_seconds`` is accumulated active time.  ``active_started_at`` is
    only set while a task is actually running, so time spent queued or paused
    is excluded.  ``heartbeat_at`` is a safe fallback for tasks created by an
    older version that has no active start marker.
    """
    current = current or datetime.now(timezone.utc)
    status = str(_get(task, "status", "") or "")
    phase = str(_get(task, "phase", "") or "")
    elapsed = max(0, _number(_get(task, "elapsed_seconds")))

    active_at = parse_utc_datetime(_get(task, "active_started_at"))
    if status == "running":
        if active_at is None:
            active_at = parse_utc_datetime(_get(task, "heartbeat_at"))
            if active_at is None and elapsed == 0:
                active_at = parse_utc_datetime(_get(task, "started_at"))
        if active_at is not None:
            elapsed += max(0, int((current - active_at).total_seconds()))

    total = max(0, _number(_get(task, "total")))
    completed = max(0, _number(_get(task, "completed")))
    if phase == "scanning":
        # The final thread count is unknown while Discord history is scanned.
        eta = 0
    elif total > completed and completed > 0 and elapsed >= 5:
        eta = max(0, math.ceil((total - completed) * elapsed / completed))
    else:
        eta = 0

    speed = (completed / elapsed * 60.0) if elapsed > 0 else 0.0
    return {
        "elapsed_seconds": elapsed,
        "estimated_seconds": eta,
        "speed": round(speed, 3),
    }
