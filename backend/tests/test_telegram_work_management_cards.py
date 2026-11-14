"""Saved-work confirmation controls follow the whole immutable preview."""

from types import SimpleNamespace

import pytest

from app.telegram_bots.work_news_sources import WorkNewsRead


def source(*, key="management.edit", phase="draft", objective="Review the guide", criteria="List verified corrections", running=False):
    return WorkNewsRead(
        "w" * 32,
        key,
        {"name": "Maya", "due": "2026-12-02 18:00"},
        True,
        {"phase": phase, "draft_id": "d" * 32, "draft": {"objective": objective, "success_criteria": criteria}, "objective": objective, "running": running, "expires_at": "2026-12-01T18:00:00+00:00"},
        "a" * 64,
        "b" * 64,
    )


@pytest.mark.parametrize("locale", ["en", "zh-CN"])
def test_edit_preview_keeps_full_fields_and_only_save_edit_discard(locale):
    from app.telegram_bots.split import visible_text
    from app.telegram_bots.work_management_cards import present_management

    objective, criteria = "<&>" + "O" * 4093, "<&>" + "C" * 4093
    data = source(objective=objective, criteria=criteria)
    view = present_management(data, {"locale": locale}, SimpleNamespace())
    html = "".join(part["html"] for part in view.parts)
    assert objective in visible_text(html) and criteria in visible_text(html)
    assert "&lt;&amp;&gt;" in html and len(view.parts) >= 3
    assert all(part["protect_content"] for part in view.parts)
    assert [[button["kind"] for button in row] for row in view.keyboard] == [["work_change_save"], ["assign_edit", "assign_discard"]]
    assert all(button["target_id"] == "d" * 32 and button["expires_at"] == data.work["expires_at"] for row in view.keyboard for button in row)


@pytest.mark.parametrize("locale", ["en", "zh-CN"])
@pytest.mark.parametrize("running", [False, True])
def test_withdrawal_has_a_distinct_second_tap_and_back(locale, running):
    from app.telegram_bots.work_management_cards import present_management

    view = present_management(source(key="management.withdraw", phase="confirm", running=running), {"locale": locale}, SimpleNamespace())
    assert "Review the guide" in "".join(part["visible_text"] for part in view.parts)
    assert [[button["kind"] for button in row] for row in view.keyboard] == [["work_withdraw_confirm", "back"]]
    assert all(button["target_kind"] == "work" and button["target_id"] == "w" * 32 for row in view.keyboard for button in row)


@pytest.mark.parametrize("phase", ["discarded", "expired", "replaced"])
def test_retired_edit_card_does_not_reveal_private_words_or_effect_controls(phase):
    from app.telegram_bots.work_management_cards import present_management

    view = present_management(source(phase=phase), {"locale": "en"}, SimpleNamespace())
    assert not view.keyboard and "Review the guide" not in "".join(part["visible_text"] for part in view.parts)
