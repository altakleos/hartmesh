"""Complete assignment previews keep consent controls after all canonical text."""

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from app.telegram_bots.work_news_sources import WorkNewsRead


def card(*, source="command", objective="Review the guide", criteria="List verified gaps", phase="draft", due=None):
    now = datetime(2026, 1, 2, tzinfo=UTC)
    return WorkNewsRead(
        None,
        "assignment.card",
        {"name": "Maya", "due": due},
        True,
        {
            "phase": phase,
            "draft_id": "d" * 32,
            "draft": {"objective": objective, "success_criteria": criteria, "due_at": due, "due_text": None},
            "source": source,
            "due_note": False,
            "can_save": True,
            "can_start": True,
            "expires_at": (now + timedelta(hours=24)).isoformat(),
        },
        "a" * 64,
        "b" * 64,
    )


@pytest.mark.parametrize("locale", ["en", "zh-CN"])
def test_assignment_card_preserves_full_escaped_canonical_fields(locale):
    from app.telegram_bots.assignment_cards import present_assignment
    from app.telegram_bots.split import visible_text

    objective, criteria = "<&>" + "O" * 4093, "<&>" + "C" * 4093
    view = present_assignment(card(objective=objective, criteria=criteria), {"locale": locale}, SimpleNamespace())
    html = "".join(part["html"] for part in view.parts)
    assert objective in visible_text(html) and criteria in visible_text(html)
    assert "&lt;&amp;&gt;" in html and len(view.parts) >= 3
    assert all(part["protect_content"] for part in view.parts)
    assert [[button["kind"] for button in line] for line in view.keyboard] == [["assign_start", "assign_save"], ["assign_edit", "assign_discard"]]
    assert all(button["target_id"] == "d" * 32 and button["expires_at"] == card().work["expires_at"] for line in view.keyboard for button in line)


def test_tool_proposal_offers_just_answer_instead_of_discard():
    from app.telegram_bots.assignment_cards import present_assignment

    view = present_assignment(card(source="tool"), {"locale": "en"}, SimpleNamespace())
    assert "Assignment proposed by Maya" in view.parts[0]["visible_text"]
    assert [button["kind"] for button in view.keyboard[-1]] == ["assign_edit", "assign_answer"]


@pytest.mark.parametrize("phase", ["discarded", "replaced", "answering", "expired"])
def test_closed_draft_never_reoffers_consent_or_private_words(phase):
    from app.telegram_bots.assignment_cards import present_assignment

    view = present_assignment(card(phase=phase), {"locale": "en"}, SimpleNamespace())
    assert not view.keyboard
    assert "Review the guide" not in "".join(part["visible_text"] for part in view.parts)
