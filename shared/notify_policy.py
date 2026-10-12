"""When to DM the task owner about download tasks.

Automatic poll tasks (mode='update', created by the continuous downloader every
cycle) must stay silent: real-time ingest would otherwise DM the owner at every
start and finish. Manual tasks (mode='initial' / anything else) keep both DMs.
A failing poll is reported once per failure streak (not every cycle).
"""


def is_auto_poll(task):
    try:
        return str(task["mode"] or "").lower() == "update"
    except (KeyError, IndexError, TypeError):
        return False


def should_notify_start(task):
    return not is_auto_poll(task)


def should_notify_finish(task, prev_same_forum_status=None):
    """prev_same_forum_status: status of the previous task for the same guild/forum."""
    if not is_auto_poll(task):
        return True
    if task["status"] != "failed":
        return False
    return prev_same_forum_status != "failed"
