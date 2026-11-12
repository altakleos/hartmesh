"""Interruption wording distinguishes lost work, unstarted work and unknowns."""

import pytest


@pytest.mark.parametrize("locale", ["en", "zh-CN"])
@pytest.mark.parametrize(
    "reason,kept,not_started",
    [
        ("container-absent", True, False),
        ("run-orphaned", True, False),
        ("run-never-started", False, True),
        ("start-failed", False, True),
        ("outcome-uncertain", True, False),
        ("idle", True, False),
        ("lifecycle", True, False),
        ("storage-recovery", False, False),
        ("operator", True, False),
        ("host-replaced", False, False),
        ("a-new-reason", False, False),
    ],
)
def test_interruption_wording_and_protected_form(locale, reason, kept, not_started):
    from app.telegram_bots.work_news import present_news

    values = {"objective": "Private objective " * 20, "name": "Maya", "reason": reason, "supervisor": "Sam"}
    view = present_news("interrupted", values=values, locale=locale, protected=True)
    text = "".join(part["visible_text"] for part in view.parts)
    assert len(text.splitlines()[0]) <= 60
    assert (("Work so far is kept." if locale == "en" else "已完成的工作已保留。") in text) is kept
    assert text.startswith(("Did not start:" if not_started else "Interrupted:") if locale == "en" else ("未启动：" if not_started else "已中断："))
    teaser = present_news("interrupted", values=values, locale=locale, protected=True, concealed=True, outbox_id="a" * 32)
    assert "Private" not in str(teaser) and "Maya" not in str(teaser)
    assert teaser.keyboard[0][0]["kind"] == "reveal"
    if locale == "en":
        assert teaser.parts[0]["visible_text"] == ("An assignment did not start." if not_started else "An assignment was interrupted.")


@pytest.mark.parametrize("reason", ["busy", "run-limit", "restarting", "starting-elsewhere", "start-failed", "files-full", "storage-busy", "unknown"])
def test_failure_wording_uses_closed_reason_and_reader_form(reason):
    from app.telegram_bots.work_news import present_news

    values = {"objective": "Check facts", "name": "Maya", "reason": reason, "supervisor": "Sam", "reader_is_manager": False}
    text = present_news("attempt.failed", values=values, locale="en", protected=False).parts[0]["visible_text"]
    assert text.startswith('Failed: "Check facts"' if reason == "unknown" else 'Did not start: "Check facts"')
    if reason == "files-full":
        assert "Ask Sam to delete files" in text
        managed = present_news("attempt.failed", values={**values, "reader_is_manager": True}, locale="en", protected=False).parts[0]["visible_text"]
        assert "Delete files on the web" in managed


def test_no_result_keeps_full_progress_and_bounds_only_verified_excerpt():
    from app.telegram_bots.work_news import present_news

    values = {"objective": "Check facts", "name": "Maya", "progress": "Full <progress> " * 400, "excerpt": "Verified <output> " * 100}
    view = present_news("attempt.no_result", values=values, locale="en", protected=False)
    text = "".join(part["visible_text"] for part in view.parts)
    assert values["progress"] in text
    assert "If Maya asked something below" in text
    excerpt = text.split("Maya wrote last:\n", 1)[1]
    assert len(excerpt) <= 600 and excerpt.endswith("…")
    assert "&lt;output&gt;" in "".join(part["html"] for part in view.parts)
    plain = present_news("attempt.no_result", values={**values, "progress": "", "excerpt": None}, locale="en", protected=False).parts[0]["visible_text"]
    assert "Latest:" not in plain and "wrote last:" not in plain and "conversation on the web" in plain
