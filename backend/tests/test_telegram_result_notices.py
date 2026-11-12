"""Non-review result cards retain the whole outcome without action controls."""

from copy import deepcopy

import pytest
from test_telegram_work_result_files import result_source


@pytest.mark.parametrize("key", ["result.done", "result.waiting_review"])
def test_result_notice_keeps_files_statement_suggestion_and_no_work_actions(key):
    from app.telegram_bots.result_notices import present_result

    source = result_source(["outputs/first.png", "outputs/second.png", "outputs/report.pdf"])
    work = deepcopy(source.work)
    work["suggestion"] = {"id": "e" * 32, "statement": "Check <older> reports."}
    view = present_result(work, key=key, name="Assistant", person="Reviewer", protected=False, locale="en", capacity=300, max_bytes=100)
    assert [part["kind"] for part in view.parts] == ["album", "document", "text"]
    assert not view.keyboard
    assert "All findings are included." in view.parts[-1]["visible_text"]
    assert "Assistant suggests next: Check <older> reports." in view.parts[-1]["visible_text"]
    assert "&lt;older&gt;" in view.parts[-1]["html"]
    assert ("Waiting for Reviewer to review." in view.parts[-1]["visible_text"]) == (key == "result.waiting_review")
    assert view.parts[-1]["visible_text"].endswith("Files above:\nfirst.png\nsecond.png\nreport.pdf")


def test_long_result_notice_is_complete_and_protected_teaser_has_no_private_values():
    from app.telegram_bots.result_notices import present_result

    work = deepcopy(result_source(["private/" + "report" * 40 + ".pdf"]).work)
    work["objective"] = "Private objective " * 30
    work["outcome"]["statement"] = "Evidence & details. " * 200
    arguments = dict(key="result.done", name="Private assistant", person=None, protected=True, locale="en", capacity=300, max_bytes=100)
    hidden = present_result(work, concealed=True, outbox_id="a" * 32, **arguments)
    assert len(hidden.parts) == 1
    assert hidden.parts[0]["visible_text"] == "An assignment is finished."
    assert "Private" not in str(hidden) and "report.pdf" not in str(hidden) and "Evidence" not in str(hidden)
    assert hidden.keyboard[0][0]["kind"] == "reveal"
    shown = present_result(work, **arguments)
    text = "".join(part["visible_text"] for part in shown.parts if part["kind"] == "text")
    assert text.count("Evidence & details.") == 200
    assert len(shown.parts) > 2 and not shown.keyboard
    assert all(part["protect_content"] for part in shown.parts)


def test_empty_result_has_no_file_trailer_or_suggestion():
    from app.telegram_bots.result_notices import present_result

    work = result_source([]).work
    shown = present_result(work, key="result.done", name="Assistant", person=None, protected=False, locale="en", capacity=300, max_bytes=100)
    assert len(shown.parts) == 1
    assert "Files above" not in shown.parts[0]["visible_text"]
    assert "suggests next" not in shown.parts[0]["visible_text"]


@pytest.mark.parametrize("key,person", [("review.ready", "Reviewer"), ("result.waiting_review", None)])
def test_result_notice_requires_known_kind_and_resolved_reviewer(key, person):
    from app.telegram_bots.result_notices import present_result

    with pytest.raises(ValueError):
        present_result(result_source([]).work, key=key, name="Assistant", person=person, protected=False, locale="en", capacity=300, max_bytes=100)
