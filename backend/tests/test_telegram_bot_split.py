"""Lossless, socket-free Telegram HTML boundary tests."""

from html import unescape
from html.parser import HTMLParser

import pytest

from app.telegram_bots.split import clip_draft, split_html


class _Parsed(HTMLParser):
    def __init__(self, source: str):
        super().__init__(convert_charrefs=True)
        self.stack: list[str] = []
        self.text: list[str] = []
        self.tags: list[tuple[str, dict]] = []
        self.feed(source)
        assert not self.stack

    def handle_starttag(self, tag, attrs):
        assert tag in {"b", "i", "s", "code", "pre", "a", "blockquote"}
        self.tags.append((tag, dict(attrs)))
        self.stack.append(tag)

    def handle_endtag(self, tag):
        assert self.stack.pop() == tag

    def handle_data(self, data):
        self.text.append(data)


def _visible(source):
    return "".join(_Parsed(source).text)


def _units(source):
    return len(_visible(source).encode("utf-16-le")) // 2


def _assert_partition(source, limit):
    chunks = split_html(source, limit=limit)
    assert "".join(_visible(chunk) for chunk in chunks) == _visible(source)
    assert all(_visible(chunk).strip() and _units(chunk) <= limit for chunk in chunks)
    return chunks


def test_short_message_is_one_chunk():
    assert split_html("<b>Hello</b> &amp; goodbye") == ["<b>Hello</b> &amp; goodbye"]


def test_limit_counts_utf16_units_after_entity_parsing():
    assert split_html("<b>12345</b>", limit=5) == ["<b>12345</b>"]


def test_non_bmp_character_counts_two_units():
    assert split_html("😀😀a", limit=4) == ["😀😀", "a"]


def test_escaped_ampersand_counts_one_unit():
    assert split_html("&amp;" * 4, limit=2) == ["&amp;&amp;", "&amp;&amp;"]


def test_cuts_at_blank_line():
    chunks = _assert_partition("First\n\nsecond line third", 15)
    assert chunks[0] == "First\n\n"


def test_cuts_at_line_break_without_blank_line():
    assert _assert_partition("First\nsecond line third", 15)[0] == "First\n"


def test_cuts_at_space_without_line_break():
    assert _assert_partition("First secondword", 10)[0] == "First "


def test_hard_cut_never_splits_a_surrogate_pair():
    assert _assert_partition("abc😀def😀ghi", 4) == ["abc", "😀de", "f😀g", "hi"]


def test_never_cuts_inside_a_tag():
    chunks = _assert_partition('<a href="https://example.test/long/address">abcdefgh</a>', 4)
    assert chunks == ['<a href="https://example.test/long/address">abcd</a>', '<a href="https://example.test/long/address">efgh</a>']


def test_never_cuts_inside_an_escape():
    assert _assert_partition("ab&lt;cd&amp;ef", 3) == ["ab&lt;", "cd&amp;", "ef"]


def test_open_tags_closed_at_cut_and_reopened():
    assert _assert_partition("<b><i>abcdefgh</i></b>", 4) == ["<b><i>abcd</i></b>", "<b><i>efgh</i></b>"]


def test_pre_keeps_language_class_on_both_parts():
    chunks = _assert_partition('<pre><code class="language-python">abcdefgh</code></pre>', 4)
    assert chunks == ['<pre><code class="language-python">abcd</code></pre>', '<pre><code class="language-python">efgh</code></pre>']


def test_anchor_moves_whole_to_next_chunk():
    chunks = _assert_partition('abc <a href="https://example.test">link name</a> tail', 12)
    assert chunks[0] == "abc "
    assert chunks[1] == '<a href="https://example.test">link name</a> '


def test_every_chunk_is_accepted_markup():
    _assert_partition('<blockquote expandable><b>one &amp; two</b></blockquote>\n\n<pre><code class="language-py">😀long_code</code></pre>', 9)


def test_chunks_join_to_the_original_visible_text():
    _assert_partition("<b>  Intro\n\n</b>middle &lt;word&gt;\n  😀 ending  ", 12)


def test_no_empty_or_whitespace_chunk():
    assert split_html("  \n ") == []
    assert split_html("<b></b>") == []
    _assert_partition("A" + " " * 8 + "B  ", 7)
    _assert_partition("12345\n\n", 5)


def test_caption_limit_1024_uses_the_same_rules():
    _assert_partition("<b>" + "😀" * 1100 + "</b>", 1024)


def test_draft_preview_is_clipped_with_ellipsis():
    assert clip_draft("<b>abcdefghij</b>", 5) == "<b>abcd…</b>"
    assert clip_draft("<b>abc</b>", 5) == "<b>abc</b>"
    assert clip_draft("😀😀😀", 4) == "😀…"


@pytest.mark.parametrize("source", ["a &amp; b\n\nc 😀d ef\n" * 20, "<b>a <i>b &lt;c&gt;</i> 😀d</b> " * 20, '<a href="https://example.test">a long link</a> ' * 20])
@pytest.mark.parametrize("limit", [13, 32, 1024])
def test_partition_preserves_mixed_markup_and_whitespace(source, limit):
    _assert_partition(source, limit)


@pytest.mark.parametrize("limit", [0, -1, True, 1.5])
def test_invalid_limit_is_refused(limit):
    with pytest.raises(ValueError):
        split_html("abc", limit=limit)


def test_impossible_limit_does_not_drop_a_character():
    with pytest.raises(ValueError):
        split_html("😀", limit=1)
    with pytest.raises(ValueError):
        split_html("a" + " " * 20 + "b", limit=3)


def test_numeric_entities_remain_atomic():
    chunks = _assert_partition("&#128512;&#x1F600;&quot;&gt;", 4)
    assert chunks == ["&#128512;&#x1F600;", "&quot;&gt;"]
    assert unescape("".join(chunks)) == '😀😀">'


def test_generated_plain_boundaries_preserve_every_feasible_partition():
    # Independent reachability oracle distinguishes truly impossible whitespace
    # budgets from a splitter that makes a poor early paragraph choice.
    import random
    from html import escape

    random_source = random.Random(417)
    for _ in range(500):
        source = "".join(random_source.choices("ab \n😀", k=random_source.randrange(1, 65)))
        limit = random_source.randrange(2, 13)
        reachable = {0}
        for start in range(len(source)):
            if start not in reachable:
                continue
            for end in range(start + 1, len(source) + 1):
                if len(source[start:end].encode("utf-16-le")) // 2 > limit:
                    break
                if source[start:end].strip():
                    reachable.add(end)
        if len(source) in reachable:
            _assert_partition(escape(source), limit)


def test_draft_preview_uses_the_first_paragraph_part():
    assert clip_draft("<b>First\n\nsecond line third</b>", 15) == "<b>First\n\n…</b>"
    assert clip_draft('abc <a href="https://example.test">link name</a> tail', 13) == "abc …"


def test_draft_preview_can_reserve_the_entire_budget_for_ellipsis():
    assert clip_draft("😀😀", 1) == "…"
    assert clip_draft("😀😀", 2) == "…"
