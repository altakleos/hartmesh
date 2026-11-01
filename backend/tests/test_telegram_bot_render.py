"""Offline Markdown presentation contracts for managed Telegram bots."""

from dataclasses import FrozenInstanceError
from html.parser import HTMLParser

import pytest

from app.telegram_bots.markdown import Rendered, to_html


class _Markup(HTMLParser):
    def __init__(self, source: str):
        super().__init__(convert_charrefs=True)
        self.tags: list[tuple[str, dict[str, str | None]]] = []
        self.stack: list[str] = []
        self.feed(source)
        assert not self.stack

    def handle_starttag(self, tag, attrs):
        assert tag in {"b", "i", "s", "code", "pre", "a", "blockquote"}
        assert not (tag in {"code", "pre"} and any(item != "pre" for item in self.stack))
        assert not (tag == "a" and any(item in {"a", "blockquote"} for item in self.stack))
        assert not (tag == "blockquote" and tag in self.stack)
        self.tags.append((tag, dict(attrs)))
        self.stack.append(tag)

    def handle_endtag(self, tag):
        assert self.stack.pop() == tag


def test_rendered_is_a_frozen_value():
    rendered = to_html("hello")
    assert isinstance(rendered, Rendered)
    with pytest.raises(FrozenInstanceError):
        rendered.html = "changed"


def test_escapes_angle_brackets_and_ampersand():
    rendered = to_html("A < B > C & D")
    assert rendered.html == "A &lt; B &gt; C &amp; D"
    assert rendered.plain == "A < B > C & D"


def test_entity_text_in_source_is_shown_literally():
    rendered = to_html("&amp; &#65; &lt;")
    assert rendered.html == "&amp;amp; &amp;#65; &amp;lt;"
    assert rendered.plain == "&amp; &#65; &lt;"


def test_html_in_source_is_escaped_not_passed_through():
    rendered = to_html("<script>x</script>\n\n<b>model text</b>")
    assert "<script>" not in rendered.html
    assert "<b>" not in rendered.html
    assert rendered.plain == "<script>x</script>\n\n<b>model text</b>"


def test_bold_italic_strikethrough_map_to_b_i_s():
    assert to_html("**bold** *italic* ~~gone~~").html == "<b>bold</b> <i>italic</i> <s>gone</s>"


def test_inline_code_maps_to_code_with_escaped_content():
    assert to_html("`<x> &amp;`").html == "<code>&lt;x&gt; &amp;amp;</code>"


def test_fenced_code_maps_to_pre_with_language_class():
    assert to_html("```python\nprint('<&>')\n```").html == '<pre><code class="language-python">print(&#x27;&lt;&amp;&gt;&#x27;)\n</code></pre>'


def test_fenced_code_without_language_maps_to_pre():
    assert to_html("```\n<&>\n```").html == "<pre>&lt;&amp;&gt;\n</pre>"


def test_unclosed_fence_in_partial_text_is_closed():
    html = to_html("```python\nx < 2", partial=True).html
    assert html == '<pre><code class="language-python">x &lt; 2</code></pre>'
    _Markup(html)


def test_unbalanced_emphasis_in_partial_text_renders_as_text():
    assert to_html("**still writing", partial=True).html == "**still writing"


def test_heading_becomes_bold_line():
    assert to_html("## Heading\n\nBody").html == "<b>Heading</b>\n\nBody"


def test_bullet_and_numbered_lists_become_lines():
    assert to_html("- One\n- Two\n\n3. Three\n4. Four").plain == "• One\n• Two\n\n3. Three\n4. Four"


def test_nested_list_is_indented():
    assert to_html("- Parent\n  - Child\n    - Grandchild\n- Next").plain == "• Parent\n  • Child\n    • Grandchild\n• Next"


def test_task_list_items_get_box_characters():
    assert to_html("- [ ] Todo\n- [x] Done\n- [X] Also done").plain == "☐ Todo\n☑ Done\n☑ Also done"


def test_link_maps_to_anchor_with_escaped_href():
    assert to_html("[site](https://example.test/?x=1&y=2)").html == '<a href="https://example.test/?x=1&amp;y=2">site</a>'


def test_link_with_other_scheme_is_plain_text():
    assert to_html("[download](ftp://example.test/file)").html == "download"
    assert "<a" not in to_html("[bad](javascript:alert(1))").html


def test_image_becomes_link():
    assert to_html("![Chart](https://example.test/chart.png)").html == '<a href="https://example.test/chart.png">Chart</a>'


def test_blockquote_maps_to_blockquote():
    assert to_html("> quoted\n> again").html == "<blockquote>quoted\nagain</blockquote>"


def test_nested_blockquote_is_flattened():
    html = to_html("> outer\n>\n>> inner\n>\n> after").html
    assert html.count("<blockquote>") == 1
    assert "outer" in html and "inner" in html and "after" in html
    _Markup(html)


def test_details_becomes_expandable_blockquote():
    html = to_html("<details>\n<summary>Details</summary>\n\n**Body** <unsafe>\n</details>").html
    assert html == "<blockquote expandable>Details\n\n<b>Body</b> &lt;unsafe&gt;</blockquote>"
    _Markup(html)


def test_code_inside_bold_closes_bold_first():
    assert to_html("**before `code` after**").html == "<b>before </b><code>code</code><b> after</b>"


def test_table_of_three_columns_becomes_aligned_pre():
    html = to_html("| Name | N | OK |\n| --- | --- | --- |\n| A | 10 | yes |\n| Long | 2 | no |").html
    assert html == "<pre>Name | N  | OK \nA    | 10 | yes\nLong | 2  | no </pre>"


def test_a_table_that_fits_a_phone_stays_a_table_at_any_number_of_columns():
    html = to_html("| A | B | C | D |\n| --- | --- | --- | --- |\n| 1 | 2 | 3 | 4 |").html
    assert html == "<pre>A | B | C | D\n1 | 2 | 3 | 4</pre>"


def test_a_table_is_aligned_up_to_the_width_of_a_phone_and_no_further():
    from app.telegram_bots.markdown import PHONE_WIDTH

    fits = "A" * (PHONE_WIDTH - 4)
    assert to_html(f"| {fits} | B |\n| --- | --- |\n| 1 | 2 |").html.startswith("<pre>")
    assert not to_html(f"| {fits}A | B |\n| --- | --- |\n| 1 | 2 |").html.startswith("<pre>")


def test_a_label_is_written_once_and_only_when_there_is_one():
    long = "a value that is much too wide for one line on a phone"
    assert to_html(f"| Total: | Note |\n| --- | --- |\n| 12 | {long} |").plain == f"Total: 12\nNote: {long}"
    assert to_html(f"|  | Note |\n| --- | --- |\n| 12 | {long} |").plain == f"12\nNote: {long}"


def test_a_picture_in_a_narrow_table_keeps_its_address():
    html = to_html("| Chart |\n| --- |\n| ![Sales](https://example.test/s.png) |").html
    assert '<a href="https://example.test/s.png">Sales</a>' in html


def test_a_link_in_a_heading_keeps_its_address():
    long = "a value that is much too wide for one line on a phone"
    assert '<a href="https://example.test/h">Plan</a>' in to_html(f"| [Plan](https://example.test/h) | Note |\n| --- | --- |\n| 1 | {long} |").html
    assert '<a href="https://example.test/h">Plan</a>' in to_html(f"| [Plan](https://example.test/h) | {long} |\n| --- | --- |").html


def test_a_table_wider_than_a_phone_becomes_labelled_lines():
    table = "| Customer | Contact | Note |\n| --- | --- | --- |\n| Acme Holdings | Jo | Renewal due at the end of the quarter |\n| Beta | Sam | |"
    rendered = to_html("Before\n\n" + table + "\n\nAfter")
    assert rendered.plain == "Before\n\nCustomer: Acme Holdings\nContact: Jo\nNote: Renewal due at the end of the quarter\n\nCustomer: Beta\nContact: Sam\n\nAfter"
    assert "<pre>" not in rendered.html


def test_a_link_in_a_table_keeps_its_address():
    html = to_html("| Invoice | Where |\n| --- | --- |\n| 12 | [open it](https://example.test/12) |").html
    assert html == 'Invoice: 12\nWhere: <a href="https://example.test/12">open it</a>'


@pytest.mark.parametrize("mark", ["<br>", "<br/>", "</br>", "<BR />"])
def test_a_break_in_a_cell_is_a_space(mark):
    assert to_html(f"| A | B |\n| --- | --- |\n| one{mark}two | three |").plain == "A       | B    \none two | three"


def test_a_wide_table_with_no_rows_still_shows_its_heads():
    plain = to_html(f"| {'long heading ' * 3} | {'another heading ' * 3} |\n| --- | --- |").plain
    assert "long heading" in plain and "another heading" in plain and "```" not in plain


@pytest.mark.parametrize(
    "source, expected",
    [
        ("Write to [the office](mailto:office@example.test) today.", "Write to the office (office@example.test) today."),
        ("Call [us](tel:+15550100) before noon.", "Call us (+15550100) before noon."),
        ("[office@example.test](mailto:office@example.test)", "office@example.test"),
        ("<office@example.test>", "office@example.test"),
        ("[a form](mailto:office@example.test?subject=Hello)", "a form (office@example.test)"),
    ],
)
def test_a_mail_or_phone_link_keeps_what_it_points_at(source, expected):
    assert to_html(source).plain == expected


@pytest.mark.parametrize("source", ["[](mailto:office@example.test)", "| A | B |\n| --- | --- |\n| [](mailto:office@example.test) | 2 |"])
def test_a_contact_link_with_no_label_still_shows_its_address(source):
    assert "office@example.test" in to_html(source).plain


def test_a_picture_with_no_description_shows_its_address():
    assert to_html("![](https://example.test/chart.png)").html == '<a href="https://example.test/chart.png">https://example.test/chart.png</a>'
    assert "https://example.test/a.png" in to_html("[![](https://example.test/a.png)](https://example.test/full)").plain


@pytest.mark.parametrize(
    "source, title",
    [
        ("[citation:City Report](https://example.test/a)", "City Report"),
        ("[Citation: City Report](https://example.test/a)", "City Report"),
        ("[**citation:City Report**](https://example.test/a)", "City Report"),
        ("[citation:](https://example.test/a)", "citation:"),
    ],
)
def test_a_citation_shows_its_title_as_the_web_does(source, title):
    assert to_html(source).plain == title


def test_the_word_citation_stays_outside_a_link():
    assert to_html("the word citation:here stays").plain == "the word citation:here stays"


def test_output_contains_only_allowed_tags():
    _Markup(to_html("# Title\n\n**bold *italic* ~~gone~~**\n\n> quote\n\n`code` [a](https://example.test)\n\n<div>raw</div>").html)


def test_a_long_answer_keeps_every_link_past_the_hundredth():
    # Telegram ignores entities past 100 in one message, so any limit belongs to a message and not to an answer.
    rendered = to_html("\n\n".join(f"[guide {i}](https://example.test/{i}) " + "word " * 12 for i in range(120)))
    assert rendered.html.count("<a href=") == 120


def test_plain_fallback_strips_tags_and_unescapes():
    rendered = to_html("**R&D** `a < b` [name](https://example.test)")
    assert rendered.plain == "R&D a < b name"


def test_status_line_is_italic_and_last():
    # The streaming caller appends the product-owned activity text. Conversion
    # must retain that position and formatting while the answer is partial.
    rendered = to_html("Partial answer\n\n*Reading report.csv…*", partial=True)
    assert rendered.html.endswith("\n\n<i>Reading report.csv…</i>")


@pytest.mark.parametrize("source", ["**a `b` c** " * 110, "> a\n>\n> ```py\n> b\n> ```", "<details>\n> quoted\n</details>"])
def test_complex_nesting_remains_valid(source):
    _Markup(to_html(source, partial=True).html)


def test_code_block_separators_stay_outside_pre():
    assert to_html("Intro\n\n```\ncode\n```\n\nAfter").html == "Intro\n\n<pre>code\n</pre>\n\nAfter"


def test_language_info_cannot_supply_html_attributes():
    html = to_html('```python" onclick="x\ncode\n```').html
    assert html == "<pre>code\n</pre>"


def test_unicode_table_alignment_uses_display_width():
    assert to_html("| 名 | N |\n| --- | --- |\n| abc | 1 |").plain == "名  | N\nabc | 1"


def test_image_inside_link_keeps_only_the_outer_anchor():
    assert to_html("[![Chart](https://example.test/chart.png)](https://example.test/page)").html == '<a href="https://example.test/page">Chart</a>'


def test_code_in_quote_closes_the_quote_before_code():
    rendered = to_html("> before `code` after")
    assert rendered.html == "<blockquote>before </blockquote><code>code</code><blockquote> after</blockquote>"
    assert rendered.plain == "before code after"


def test_link_in_quote_closes_quote_around_the_anchor():
    rendered = to_html("> before [link](https://example.test) after")
    assert rendered.html == '<blockquote>before </blockquote><a href="https://example.test">link</a><blockquote> after</blockquote>'
    _Markup(rendered.html)


@pytest.mark.parametrize("source", ["<details>Body</details> required conclusion", "<details>\nBody\n</details> required conclusion"])
def test_details_preserves_text_after_the_closing_tag(source):
    rendered = to_html(source)
    assert rendered.html == "<blockquote expandable>Body</blockquote> required conclusion"
    assert rendered.plain == "Body required conclusion"


@pytest.mark.parametrize(
    "source",
    [
        "> <details>\n> Body\n\nOutside",
        "> <details>\n> Body\n\nOutside\n</details>",
        "- <details>\n  Body\n\nOutside",
        "- <details>\n  Body\n\nOutside\n</details>",
    ],
)
def test_unclosed_details_cannot_capture_content_outside_its_container(source):
    rendered = to_html(source, partial=True)
    assert rendered.plain.count("Outside") == 1
    assert rendered.plain.count("Body") == 1
    if source.endswith("</details>"):
        assert "&lt;/details&gt;" in rendered.html
    _Markup(rendered.html)


@pytest.mark.parametrize(
    "source",
    [
        "<details>\n```html\n</details>\n```\nConclusion\n</details>",
        "<details>\n`</details>`\nConclusion\n</details>",
        "<details>\n`<details>`\nConclusion\n</details>",
        "<details>\n```html\n<details>\n```\nConclusion\n</details>",
    ],
)
def test_details_delimiters_in_code_fall_back_to_literal_source(source):
    rendered = to_html(source)
    assert rendered.plain == source
    assert "<blockquote" not in rendered.html
    _Markup(rendered.html)


def test_details_delimiter_in_indented_code_preserves_code_and_conclusion():
    rendered = to_html("<details>\n\n    </details>\n\nConclusion\n</details>")
    assert "<pre>&lt;/details&gt;\n</pre>" in rendered.html
    assert rendered.plain.count("</details>") == 2
    assert rendered.plain.count("Conclusion") == 1
    assert "<blockquote" not in rendered.html
    _Markup(rendered.html)


def test_details_delimiter_in_image_alt_code_is_literal():
    source = "<details>\n![`</details>`](https://example.test/image.png)\nConclusion\n</details>"
    assert to_html(source).plain == source


def test_code_after_a_complete_details_block_does_not_disable_the_container():
    rendered = to_html("<details>Body</details>\n\n`</details>`")
    assert rendered.html == "<blockquote expandable>Body</blockquote>\n\n<code>&lt;/details&gt;</code>"


@pytest.mark.parametrize(
    "body",
    [
        "<!-- </details> -->",
        "<!-- first line\n</details>\nlast line -->",
        '<div title="</details>">Body</div>',
        "<div title='a > </details> b'>Body</div>",
        '<div title="<details>">Body</div>',
        '<script>"</details>"</script>',
        '<style>"</details>"</style>',
    ],
)
def test_details_uses_html_lexical_boundaries_for_literal_markup(body):
    rendered = to_html(f"<details>\n{body}\nConclusion\n</details>")
    assert rendered.html.startswith("<blockquote expandable>")
    assert rendered.html.endswith("</blockquote>")
    assert body in rendered.plain
    assert rendered.plain.count("Conclusion") == 1
    assert rendered.plain.count("</details>") == body.count("</details>")
    assert rendered.plain.count("<details>") == body.count("<details>")
    _Markup(rendered.html)


@pytest.mark.parametrize("delimiter", [r"\</details>", r"\<details>"])
def test_escaped_details_delimiters_fall_back_without_losing_text(delimiter):
    source = f"<details>\n{delimiter}\nConclusion\n</details>"
    rendered = to_html(source)
    assert rendered.plain == source
    assert "<blockquote" not in rendered.html
    _Markup(rendered.html)


@pytest.mark.parametrize(
    "label",
    [
        "Literal `</summary>` token",
        "Literal `<summary>` token",
        "Literal \\</summary> token",
        "Literal \\<summary> token",
    ],
)
def test_ambiguous_summary_delimiters_preserve_the_literal_label(label):
    summary = f"<summary>{label}</summary>"
    rendered = to_html(f"<details>\n{summary}\nConclusion\n</details>")
    assert summary in rendered.plain
    assert rendered.plain.count("Conclusion") == 1
    _Markup(rendered.html)


@pytest.mark.parametrize(
    "label",
    [
        '<span title="</summary>">Visible</span>',
        "<span title='a > </summary> b'>Visible</span>",
        '<span title="<summary>">Visible</span>',
        "<!-- </summary> -->Visible",
        "<!-- first line\n</summary>\nlast line -->Visible",
        '<script>"</summary>"</script>Visible',
    ],
)
def test_summary_uses_html_lexical_boundaries_without_losing_literal_markup(label):
    rendered = to_html(f"<details>\n<summary>{label}</summary>\nConclusion\n</details>")
    assert label in rendered.plain
    assert rendered.plain.count("Conclusion") == 1
    assert rendered.plain.count("</summary>") == label.count("</summary>")
    assert rendered.plain.count("<summary>") == label.count("<summary>")
    _Markup(rendered.html)
