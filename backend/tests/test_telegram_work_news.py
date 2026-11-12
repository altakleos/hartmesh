"""Colleague notices retain full quoted content and expose no Work commands."""

import pytest


@pytest.mark.parametrize("locale", ["en", "zh-CN"])
@pytest.mark.parametrize(
    "key",
    [
        "request.answered",
        "decision.recorded",
        "result.accepted",
        "result.changes",
        "work.cancelled",
        "work.changed",
        "work.reopened",
        "attempt.stopped",
        "attempt.stopped_changed",
        "attempt.stopped_withdrawn",
        "work.proposed",
        "remind.ready_to_resume",
    ],
)
def test_news_first_line_and_protected_teaser_do_not_disclose_written_content(locale, key):
    from app.telegram_bots.work_news import present_news

    values = dict(
        objective="Private assignment " * 20,
        objective_full="PRIVATE FULL OBJECTIVE",
        success_criteria="PRIVATE CRITERIA",
        name="Private assistant",
        person="Colleague " * 20,
        decision="PRIVATE DECISION",
        note="PRIVATE NOTE",
        text="PRIVATE COMMENT",
        time="12:34",
    )
    shown = present_news(key, values=values, locale=locale, protected=True)
    first = shown.parts[0]["visible_text"].splitlines()[0]
    assert len(first) <= 60
    assert not any(word in first for word in ("DECISION", "NOTE", "COMMENT", "CRITERIA", "FULL OBJECTIVE"))
    assert not shown.keyboard
    concealed = present_news(key, values=values, locale=locale, protected=True, concealed=True, outbox_id="a" * 32)
    assert len(concealed.parts) == 1 and concealed.keyboard[0][0]["kind"] == "reveal"
    assert "Private" not in str(concealed) and "PRIVATE" not in str(concealed) and "Colleague" not in str(concealed)
    assert all(part["protect_content"] for part in shown.parts + concealed.parts)


def test_decision_and_comment_are_complete_and_optional_note_is_omitted():
    from app.telegram_bots.work_news import present_news

    values = dict(objective="Check facts", person="A colleague", decision="Use <the> archive", note="Evidence & context. " * 250)
    view = present_news("decision.recorded", values=values, locale="en", protected=False)
    assert len(view.parts) >= 2
    joined = "".join(part["visible_text"] for part in view.parts)
    assert joined.count("Evidence & context.") == 250
    assert "Use &lt;the&gt; archive" in view.parts[0]["html"]
    empty = present_news("decision.recorded", values={**values, "note": None}, locale="en", protected=False)
    assert "Note:" not in empty.parts[0]["visible_text"]
    proposal = present_news("work.proposed", values={"objective_full": "Complete detail " * 100, "success_criteria": "All checks"}, locale="en", protected=False)
    assert "Complete detail " * 100 in "".join(part["visible_text"] for part in proposal.parts)


def test_news_rejects_unowned_keys_and_unbound_show():
    from app.telegram_bots.work_news import present_news

    with pytest.raises(ValueError):
        present_news("review.ready", values={}, locale="en", protected=False)
    with pytest.raises(ValueError):
        present_news("work.changed", values={}, locale="en", protected=True, concealed=True)
