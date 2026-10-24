"""Markdown answers rendered into Telegram's deliberately small HTML vocabulary."""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from html import escape
from html.parser import HTMLParser
from urllib.parse import urlsplit

from markdown_it import MarkdownIt
from markdown_it.rules_block import StateBlock
from markdown_it.token import Token


@dataclass(frozen=True)
class Rendered:
    html: str
    plain: str
    wide_tables: list[str]


@dataclass(frozen=True)
class _Format:
    tag: str
    attributes: str = ""
    identity: int = 0


@dataclass(frozen=True)
class _Run:
    text: str
    formats: tuple[_Format, ...]


class _Writer:
    def __init__(self):
        self.runs: list[_Run] = []
        self.formats: list[_Format] = []
        self.pending = 0
        self.serial = 0

    def write(self, text: str, *, isolated: tuple[_Format, ...] | None = None) -> None:
        if not text:
            return
        formats = tuple(self.formats) if isolated is None else isolated
        if self.pending and self.runs:
            self.runs.append(_Run("\n" * self.pending, formats))
        self.pending = 0
        self.runs.append(_Run(text, formats))

    def gap(self, lines: int = 2) -> None:
        self.pending = max(self.pending, lines)

    def push(self, tag: str, attributes: str = "") -> _Format | None:
        if tag in {"blockquote", "a"} and any(item.tag == tag for item in self.formats):
            return None
        # A block separator belongs before its opening tag, not inside it.
        if self.pending and self.runs:
            self.runs.append(_Run("\n" * self.pending, tuple(self.formats)))
            self.pending = 0
        self.serial += 1
        item = _Format(tag, attributes, self.serial)
        self.formats.append(item)
        return item

    def pop(self, item: _Format | None) -> None:
        if item is not None:
            assert self.formats.pop() == item

    def code(self, text: str, language: str | None = None, *, block: bool = False) -> None:
        # Telegram forbids code/pre inside emphasis or links. The next ordinary
        # run resumes those formats only if it has actual text to display.
        if self.pending and self.runs:
            self.runs.append(_Run("\n" * self.pending, tuple(self.formats)))
            self.pending = 0
        formats: tuple[_Format, ...] = ()
        self.serial += 1
        if block:
            formats += (_Format("pre", identity=self.serial),)
            if language:
                self.serial += 1
                formats += (_Format("code", f' class="language-{escape(language, quote=True)}"', self.serial),)
        else:
            formats += (_Format("code", identity=self.serial),)
        self.write(text, isolated=formats)

    def rendered(self) -> tuple[str, str]:
        parts: list[str] = []
        active: tuple[_Format, ...] = ()
        entities = 0
        for run in self.runs:
            # Quotes and links cannot contain each other. Keep the innermost
            # semantic span and resume the outer span when its text continues.
            exclusive = next((item for item in reversed(run.formats) if item.tag in {"a", "blockquote"}), None)
            formats = tuple(item for item in run.formats if item.tag not in {"a", "blockquote"} or item == exclusive)
            common = 0
            while common < min(len(active), len(formats)) and active[common] == formats[common]:
                common += 1
            parts.extend(f"</{item.tag}>" for item in reversed(active[common:]))
            opened: list[_Format] = []
            for item in formats[common:]:
                if entities == 100:
                    break
                parts.append(f"<{item.tag}{item.attributes}>")
                entities += 1
                opened.append(item)
            active = (*active[:common], *opened)
            parts.append(escape(run.text, quote=True))
        parts.extend(f"</{item.tag}>" for item in reversed(active))
        return "".join(parts), "".join(run.text for run in self.runs)


class _ContainerBoundary(HTMLParser):
    """Find real container tags without interpreting or rendering source HTML."""

    _RAW_TEXT = {"script", "style", "textarea", "title", "xmp", "iframe", "noembed", "noframes", "plaintext"}

    def __init__(self, source: str, *, tag: str):
        super().__init__(convert_charrefs=False)
        self.source = source
        self.tag = tag
        self.line_offsets = [0, *(index + 1 for index, char in enumerate(source) if char == "\n")]
        self.depth = 1
        self.closing: tuple[int, int] | None = None
        self.ambiguous = False
        self.raw_text: str | None = None
        try:
            self.feed(source)
            self.close()
        except (AssertionError, ValueError):
            # Malformed lexical declarations are displayed as ordinary source.
            self.ambiguous = True

    def _offset(self) -> int:
        line, column = self.getpos()
        return self.line_offsets[line - 1] + column

    def _escaped(self, offset: int) -> bool:
        cursor = offset
        while cursor and self.source[cursor - 1] == "\\":
            cursor -= 1
        return (offset - cursor) % 2 == 1

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if self.closing is not None:
            return
        if tag == self.tag:
            if self.raw_text or self._escaped(self._offset()):
                self.ambiguous = True
            else:
                self.depth += 1
        elif self.raw_text is None and tag in self._RAW_TEXT:
            self.raw_text = tag

    def handle_endtag(self, tag: str) -> None:
        if self.closing is not None:
            return
        if self.raw_text:
            if tag == self.raw_text:
                self.raw_text = None
            elif tag == self.tag:
                # HTMLParser handles script/style raw text itself. Other HTML
                # text-only elements need conservative literal rendering when
                # its callbacks expose a would-be container delimiter inside them.
                self.ambiguous = True
            return
        if tag == self.tag:
            offset = self._offset()
            marker = re.compile(r"</" + re.escape(self.tag) + r"\s*>", re.IGNORECASE).match(self.source, offset)
            if marker is None or self._escaped(offset):
                self.ambiguous = True
                return
            self.depth -= 1
            if self.depth == 0:
                self.closing = (offset, marker.end())

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if self.closing is None and (tag == self.tag or tag in self._RAW_TEXT):
            self.ambiguous = True


def _code_has_container_tag(source: str, *, tag: str, through: int) -> bool:
    """Detect ambiguous Markdown code with the same parser as ordinary answers."""
    # HTML tokenization cannot distinguish a delimiter from one inside Markdown
    # code. Disable HTML so an opener cannot conceal subsequent code tokens.
    # This parser has no container rule, so the preflight cannot recurse here.
    tokens = MarkdownIt("commonmark", {"html": False}).parse(source)
    delimiter = re.compile(r"</?" + re.escape(tag) + r"(?:\s[^>]*)?>", re.IGNORECASE)
    closing_line = source[:through].count("\n")
    for token in tokens:
        if token.map and token.map[0] > closing_line:
            continue
        pending = [token]
        while pending:
            candidate = pending.pop()
            if candidate.type in {"code_inline", "fence", "code_block"} and delimiter.search(candidate.content):
                return True
            pending.extend(candidate.children or [])
    return False


def _summary_text(source: str) -> str:
    """Remove a real summary wrapper; ambiguous labels keep their literal source."""
    opening = re.match(r"^\s*<summary\s*>", source, re.IGNORECASE)
    if opening is None:
        return source
    start = opening.end()
    boundary = _ContainerBoundary(source[start:], tag="summary")
    if boundary.ambiguous or boundary.closing is None:
        return source
    end, after = (start + position for position in boundary.closing)
    if _code_has_container_tag(source, tag="summary", through=after):
        return source
    # Preserve body indentation; it may introduce an intentional code block.
    return source[start:end] + "\n\n" + source[after:].lstrip("\r\n")


def _details(state: StateBlock, start: int, end: int, silent: bool) -> bool:
    """Recognize only the specified details container; all other HTML is literal."""
    begin = state.bMarks[start] + state.tShift[start]
    opening = re.match(r"<details(?:\s+open(?:=(?:\"open\"|'open'|open))?)?\s*>", state.src[begin:], re.IGNORECASE)
    if opening is None or state.sCount[start] - state.blkIndent >= 4:
        return False
    if silent:
        return True
    # Blockquote rules pass a bounded line range, while list rules additionally
    # use indentation to fence their current item. Read the logical lines through
    # those boundaries: slicing state.src directly would restore quote markers
    # and could consume a later closing tag outside this container.
    block_end = start + 1
    while block_end < end:
        position = state.bMarks[block_end] + state.tShift[block_end]
        if position < state.eMarks[block_end] and state.sCount[block_end] < state.blkIndent:
            break
        block_end += 1
    source = state.getLines(start, block_end, state.blkIndent, False)
    opening = re.match(r"[ \t]*<details(?:\s+open(?:=(?:\"open\"|'open'|open))?)?\s*>", source, re.IGNORECASE)
    assert opening is not None
    content_start = opening.end()
    boundary = _ContainerBoundary(source[content_start:], tag="details")
    if boundary.ambiguous:
        return False
    content_end, after = (content_start + position for position in boundary.closing) if boundary.closing else (len(source), len(source))
    if _code_has_container_tag(source, tag="details", through=after):
        return False
    token = state.push("details", "blockquote", 0)
    token.content = source[content_start:content_end].strip("\n")
    next_line = min(start + source[:after].count("\n") + 1, block_end)
    token.map = [start, next_line]
    # The block rule consumes the closing line. Its suffix is ordinary visible
    # source, even when a model puts a conclusion after </details> on that line.
    token.meta["tail"] = source[after:].split("\n", 1)[0]
    state.line = next_line
    return True


def _parser() -> MarkdownIt:
    # Disable entity decoding so an answer containing literal "&amp;" displays
    # those five characters. HTML tokens are escaped by our renderer below.
    parser = MarkdownIt("commonmark").enable(("table", "strikethrough")).disable("entity")
    parser.block.ruler.before("html_block", "telegram_details", _details)
    return parser


def _http_address(address: str) -> bool:
    try:
        return urlsplit(address).scheme.lower() in {"http", "https"}
    except ValueError:
        return False


def _display_width(text: str) -> int:
    return sum(0 if unicodedata.combining(char) else 2 if unicodedata.east_asian_width(char) in {"F", "W"} else 1 for char in text)


class _Renderer:
    def __init__(self, parser: MarkdownIt):
        self.parser = parser
        self.writer = _Writer()
        self.wide_tables: list[str] = []

    def inline(self, tokens: list[Token], *, task: bool = False) -> None:
        frames: list[_Format | None] = []
        first = True
        for token in tokens:
            kind = token.type
            if kind in {"text", "html_inline"}:
                text = token.content
                if first and task:
                    text = re.sub(r"^\[([ xX])\]\s+", lambda match: "☐ " if match[1] == " " else "☑ ", text)
                self.writer.write(text)
            elif kind in {"softbreak", "hardbreak"}:
                self.writer.write("\n")
            elif kind in {"strong_open", "em_open", "s_open"}:
                frames.append(self.writer.push({"strong_open": "b", "em_open": "i", "s_open": "s"}[kind]))
            elif kind in {"strong_close", "em_close", "s_close", "link_close"}:
                self.writer.pop(frames.pop())
            elif kind == "code_inline":
                self.writer.code(token.content)
            elif kind == "link_open":
                address = token.attrGet("href") or ""
                frames.append(self.writer.push("a", f' href="{escape(address, quote=True)}"') if _http_address(address) else None)
            elif kind == "image":
                address = token.attrGet("src") or ""
                item = self.writer.push("a", f' href="{escape(address, quote=True)}"') if _http_address(address) else None
                self.inline(token.children or [])
                self.writer.pop(item)
            else:
                self.writer.write(token.content)
            if token.content:
                first = False

    def blocks(self, tokens: list[Token], source: str) -> None:
        lists: list[dict] = []
        items: list[bool] = []
        quotes: list[_Format | None] = []
        headings: list[_Format | None] = []
        index = 0
        while index < len(tokens):
            token = tokens[index]
            kind = token.type
            if kind == "inline":
                task = bool(items and items[-1])
                self.inline(token.children or [], task=task)
                if items:
                    items[-1] = False
            elif kind == "paragraph_close":
                self.writer.gap(1 if lists else 2)
            elif kind == "heading_open":
                headings.append(self.writer.push("b"))
            elif kind == "heading_close":
                self.writer.pop(headings.pop())
                self.writer.gap()
            elif kind in {"bullet_list_open", "ordered_list_open"}:
                if not lists:
                    self.writer.gap()
                lists.append({"ordered": kind == "ordered_list_open", "number": token.attrGet("start") or 1})
            elif kind == "list_item_open":
                self.writer.gap(1)
                marker = f"{lists[-1]['number']}. " if lists[-1]["ordered"] else "• "
                # Task checkboxes replace the ordinary bullet marker.
                lookahead = tokens[index + 2] if index + 2 < len(tokens) else None
                task = bool(lookahead and lookahead.type == "inline" and re.match(r"^\[[ xX]\]\s", lookahead.content))
                self.writer.write("  " * (len(lists) - 1) + ("" if task else marker))
                lists[-1]["number"] += 1
                items.append(task)
            elif kind == "list_item_close":
                items.pop()
                self.writer.gap(1)
            elif kind in {"bullet_list_close", "ordered_list_close"}:
                lists.pop()
                self.writer.gap(1 if lists else 2)
            elif kind == "blockquote_open":
                quotes.append(self.writer.push("blockquote"))
            elif kind == "blockquote_close":
                self.writer.pending = 0
                self.writer.pop(quotes.pop())
                self.writer.gap()
            elif kind in {"fence", "code_block"}:
                self.writer.gap()
                info = token.info.strip().split(maxsplit=1)
                language = info[0] if info and re.fullmatch(r"[A-Za-z0-9_+.\-]+", info[0]) else None
                self.writer.code(token.content, language, block=True)
                self.writer.gap()
            elif kind == "html_block":
                self.writer.write(token.content.rstrip("\n"))
                self.writer.gap()
            elif kind == "details":
                item = self.writer.push("blockquote", " expandable")
                content = _summary_text(token.content)
                self.blocks(self.parser.parse(content), content)
                self.writer.pending = 0
                self.writer.pop(item)
                self.writer.write(token.meta.get("tail", ""))
                self.writer.gap()
            elif kind == "table_open":
                finish = index + 1
                while tokens[finish].type != "table_close":
                    finish += 1
                self.table(tokens[index : finish + 1], source)
                index = finish
            elif kind == "hr":
                self.writer.write("—")
                self.writer.gap()
            index += 1

    def table(self, tokens: list[Token], source: str) -> None:
        rows: list[list[str]] = []
        for token in tokens:
            if token.type == "tr_open":
                rows.append([])
            elif token.type == "inline":
                cell = _Renderer(self.parser)
                cell.inline(token.children or [])
                _, plain = cell.writer.rendered()
                rows[-1].append(plain.replace("\n", " "))
        columns = max(map(len, rows), default=0)
        if columns > 3:
            first, last = tokens[0].map
            self.wide_tables.append("\n".join(source.splitlines()[first:last]))
            return
        widths = [max(_display_width(row[column]) for row in rows) for column in range(columns)]
        aligned = "\n".join(" | ".join(cell + " " * (widths[column] - _display_width(cell)) for column, cell in enumerate(row)) for row in rows)
        self.writer.gap()
        self.writer.code(aligned, block=True)
        self.writer.gap()


def to_html(text: str, *, partial: bool = False) -> Rendered:
    """Render complete or growing Markdown, always emitting balanced safe HTML.

    CommonMark already closes unfinished fences and retains unmatched emphasis
    as literal text. Activity text is appended by the streaming caller, which
    knows the current product-owned status and reserves its separate budget.
    """
    parser = _parser()
    renderer = _Renderer(parser)
    renderer.blocks(parser.parse(text), text)
    html, plain = renderer.writer.rendered()
    return Rendered(html=html, plain=plain, wide_tables=renderer.wide_tables)
