"""Cloud-DB mode (remote SQLite over SSH) was removed in v1.2.

The production-side agent (``cloud_db_agent.py``) is gone, so this module is a
safe stub that keeps the old import surface: every helper reports "not cloud"
and callers fall through to local SQLite files. ``CLOUD_DB=1`` is ignored with
a one-time warning. The ``CLOUD_DB_SSH_*`` keys in ``.env`` are still used by
deploy tooling for plain SSH/scp, not by the app.
"""

import logging
import os

_warned = False


class CloudDbError(RuntimeError):
    """Kept for import compatibility."""


def cloud_enabled():
    global _warned
    if os.getenv("CLOUD_DB", "0") == "1" and not _warned:
        _warned = True
        logging.getLogger(__name__).warning(
            "CLOUD_DB=1 ignored: cloud-DB mode was removed in v1.2; using local data/ files.")
    return False


def maybe_connect(path, row_factory=True, write=None):
    return None


def remote_stat(path):
    return None


def remote_exists(path):
    return None


def remote_file_stat(path):
    return None
