"""Existing renderer controls and explicit missing frozen-presentation scaffolds."""

import hashlib
from copy import deepcopy
from dataclasses import FrozenInstanceError

import pytest

from app.telegram_bots import producer
from app.telegram_bots.markdown import to_html
from app.telegram_bots.split import split_html, utf16_units, visible_text


@pytest.mark.parametrize(
    "source",
    [
        "**visible & escaped** with [a link](https://example.invalid/path)",
        "# Heading\n\n" + "😀e\u0301 & <literal> " * 700,
        "```python\n" + "print('😀 & <ok>')\n" * 400 + "```",
        "> quote\n>\n> **more quote**\n\n" + "a long paragraph " * 800,
    ],
    ids=["entities-and-link", "astral-codepoints", "code-fence", "quotation"],
)
def test_current_renderer_visible_basis_is_lossless_and_split_units_are_utf16(source):
    rendered = to_html(source)
    assert rendered.wide_tables == []
    chunks = split_html(rendered.html)
    assert chunks
    assert "".join(visible_text(chunk) for chunk in chunks) == rendered.plain
    assert all(0 < utf16_units(visible_text(chunk)) <= 4096 for chunk in chunks)
    cursor = 0
    for chunk in chunks:
        text = visible_text(chunk)
        assert rendered.plain[cursor : cursor + len(text)] == text
        cursor += len(text)
    assert cursor == len(rendered.plain)


def test_current_renderer_source_digest_is_distinct_from_rendered_visible_basis():
    source = "**a genuine source** & [link](https://example.invalid/path)"
    rendered = to_html(source)
    assert rendered.plain != source
    assert hashlib.sha256(source.encode("utf-8")).digest() != hashlib.sha256(rendered.plain.encode("utf-8")).digest()


def _freeze(source):
    return producer.freeze_reply_presentation(source, text_sha256=hashlib.sha256(source.encode("utf-8")).hexdigest())


def test_frozen_presentation_keeps_original_digest_and_visible_codepoint_ranges():
    source = "**😀e\u0301 & visible**\n\n" + "[link](https://example.invalid/path) 😀\n" * 800
    plan = _freeze(source)
    assert plan.renderer_schema == "telegram-html-v1"
    assert plan.text_sha256 == hashlib.sha256(source.encode("utf-8")).hexdigest()
    assert len(plan.chunks) > 1
    visible = "".join(chunk.visible_text for chunk in plan.chunks)
    assert visible == to_html(source).plain
    assert plan.visible_sha256 == hashlib.sha256(visible.encode("utf-8")).hexdigest()
    assert plan.visible_sha256 != plan.text_sha256
    cursor = 0
    for chunk in plan.chunks:
        assert (chunk.start, chunk.end) == (cursor, cursor + len(chunk.visible_text))
        assert visible[chunk.start : chunk.end] == visible_text(chunk.html) == chunk.visible_text
        assert chunk.visible_sha256 == hashlib.sha256(chunk.visible_text.encode("utf-8")).hexdigest()
        assert utf16_units(chunk.visible_text) <= 4096
        cursor = chunk.end
    with pytest.raises(FrozenInstanceError):
        plan.text_sha256 = "f" * 64
    with pytest.raises(FrozenInstanceError):
        plan.chunks[0].start = 9


def test_frozen_presentation_json_roundtrip_is_detached():
    plan = _freeze("**visible answer** & 😀")
    encoded = plan.to_json()
    restored = producer.FrozenReplyPresentation.from_json(encoded)
    assert restored == plan
    encoded["chunks"][0]["html"] = "a different source"
    assert restored == plan and plan.to_json() != encoded


@pytest.mark.parametrize("change", ["extra", "renderer", "source-type", "chunk-extra", "bool-start", "hash", "html", "range"])
def test_frozen_presentation_closed_parser_refuses_corrupt_saved_basis(change):
    encoded = deepcopy(_freeze("**visible answer** & 😀").to_json())
    if change == "extra":
        encoded["unexpected"] = True
    elif change == "renderer":
        encoded["renderer_schema"] = "unknown-renderer"
    elif change == "source-type":
        encoded["text_sha256"] = True
    elif change == "chunk-extra":
        encoded["chunks"][0]["message_ids"] = [88]
    elif change == "bool-start":
        encoded["chunks"][0]["start"] = False
    elif change == "hash":
        encoded["visible_sha256"] = "f" * 64
    elif change == "html":
        encoded["chunks"][0]["html"] = "<b>different</b>"
    else:
        encoded["chunks"][0]["end"] += 1
    with pytest.raises(ValueError, match="publication-presentation"):
        producer.FrozenReplyPresentation.from_json(encoded)


@pytest.mark.parametrize("text,digest", [("answer", "f" * 64), ("", hashlib.sha256(b"").hexdigest()), (True, "f" * 64), ("answer", True)])
def test_frozen_presentation_refuses_noncanonical_source(text, digest):
    with pytest.raises(ValueError, match="publication-presentation"):
        producer.freeze_reply_presentation(text, text_sha256=digest)


def test_frozen_presentation_holds_wide_table_without_dropping_its_content():
    source = "| first | second | third | fourth |\n| --- | --- | --- | --- |\n| one | two | three | four |"
    assert to_html(source).wide_tables
    with pytest.raises(ValueError, match="publication-presentation"):
        _freeze(source)
