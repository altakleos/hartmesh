"""Digest navigation stays bounded and never substitutes for an action preview."""

import pytest


@pytest.mark.parametrize("locale", ["en-US", "zh-CN"])
def test_digest_navigation_has_bounded_labels_and_current_member_identifiers(locale):
    from app.telegram_bots.split import utf16_units, visible_text
    from app.telegram_bots.work_digest_text import DigestEntry, present_digest

    entries = tuple(DigestEntry(f"{index:032x}", "question", kind, "<Private title> " + "🙂" * 100) for index, kind in enumerate(("question", "decision", "review"), 1))
    shown = present_digest(entries, locale=locale, protected=False, outbox_id="a" * 32)
    assert len(shown.parts) == 1 and utf16_units(visible_text(shown.parts[0]["html"])) <= 4096
    assert "<Private title>" not in shown.parts[0]["html"]
    assert len(shown.keyboard) == 3 and all(len(row) == 1 for row in shown.keyboard)
    for entry, row in zip(entries, shown.keyboard, strict=True):
        button = row[0]
        assert button["kind"] == "digest_item" and button["target_id"] == entry.outbox_id
        assert len(button["label_values"]["label"]) <= 32
        assert button["payload"] == {"digest_id": "a" * 32, "outbox_id": entry.outbox_id}


def test_digest_concealed_form_exposes_no_titles_or_item_buttons():
    from app.telegram_bots.work_digest_text import DigestEntry, present_digest

    entries = [DigestEntry("b" * 32, "question", "question", "Private title")]
    shown = present_digest(entries, locale="en-US", protected=True, concealed=True, outbox_id="a" * 32)
    assert shown.parts[0]["html"] == "1 things need you."
    assert "Private" not in str(shown)
    assert shown.keyboard[0][0]["kind"] == "reveal" and shown.keyboard[0][0]["target_id"] == "a" * 32


def test_digest_mixed_news_uses_first_line_and_preserves_order():
    from app.telegram_bots.work_digest_text import DigestEntry, present_digest

    entries = [DigestEntry("b" * 32, "question", "question", "Supplier comparison"), DigestEntry("c" * 32, "interrupt", "news", first_line="A colleague stopped work\nHidden detail")]
    shown = present_digest(entries, locale="en-US", protected=False, outbox_id="a" * 32)
    assert shown.parts[0]["html"] == "2 new messages:\na question for &quot;Supplier comparison&quot;\nA colleague stopped work"
    assert [row[0]["target_id"] for row in shown.keyboard] == [entry.outbox_id for entry in entries]


@pytest.mark.parametrize("damage", ["mixed", "reply", "status", "duplicate", "oversize"])
def test_digest_rejects_invalid_membership(damage):
    from app.telegram_bots.work_digest_text import DigestEntry, present_digest

    entries = [DigestEntry("b" * 32, "question", "question", "Question")]
    if damage == "duplicate":
        entries *= 2
    elif damage == "oversize":
        entries = [DigestEntry(f"{index:032x}", "question", "question", "Question") for index in range(11)]
    else:
        entries.append(DigestEntry("c" * 32, {"mixed": "info", "reply": "reply", "status": "progress"}[damage], "news", first_line="Other"))
    with pytest.raises(ValueError):
        present_digest(entries, locale="en-US", protected=False, outbox_id="a" * 32)
