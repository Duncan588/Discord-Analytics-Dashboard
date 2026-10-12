from shared.notify_policy import should_notify_start, should_notify_finish


def t(mode, status="completed"):
    return {"mode": mode, "status": status}


def test_manual_tasks_notify():
    assert should_notify_start(t("initial"))
    assert should_notify_finish(t("initial"))
    assert should_notify_finish(t(None, "failed"))


def test_poll_tasks_silent():
    assert not should_notify_start(t("update"))
    assert not should_notify_finish(t("update"), "completed")


def test_poll_failure_once_per_streak():
    assert should_notify_finish(t("update", "failed"), "completed")
    assert not should_notify_finish(t("update", "failed"), "failed")
