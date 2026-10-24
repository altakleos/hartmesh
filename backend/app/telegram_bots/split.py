"""Lossless boundaries for the constrained HTML used by managed Telegram bots."""

from __future__ import annotations

from bisect import bisect_right
from dataclasses import dataclass
from html import unescape
from html.parser import HTMLParser


@dataclass(frozen=True)
class _Tag:
    name: str
    opening: str
    identity: int


@dataclass(frozen=True)
class _Atom:
    raw: str
    text: str
    tags: tuple[_Tag, ...]


def utf16_units(text: str) -> int:
    """Count Telegram's conservative visible-text budget, without encoding copies."""
    return sum(2 if ord(char) > 0xFFFF else 1 for char in text)


class _HTML(HTMLParser):
    def __init__(self, source: str):
        super().__init__(convert_charrefs=False)
        self.atoms: list[_Atom] = []
        self.stack: list[_Tag] = []
        self.serial = 0
        self.feed(source)
        self.close()
        if self.stack:
            raise ValueError("Telegram HTML must have balanced tags")

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        allowed = {"b": set(), "i": set(), "s": set(), "pre": set(), "code": {"class"}, "a": {"href"}, "blockquote": {"expandable"}}
        if tag not in allowed or any(name not in allowed[tag] for name, _ in attrs):
            raise ValueError("Unsupported Telegram HTML tag or attribute")
        self.serial += 1
        self.stack.append(_Tag(tag, self.get_starttag_text(), self.serial))

    def handle_endtag(self, tag: str) -> None:
        if not self.stack or self.stack[-1].name != tag:
            raise ValueError("Telegram HTML must have balanced tags")
        self.stack.pop()

    def handle_data(self, data: str) -> None:
        tags = tuple(self.stack)
        self.atoms.extend(_Atom(char, char, tags) for char in data)

    def handle_entityref(self, name: str) -> None:
        raw = f"&{name};"
        self.atoms.append(_Atom(raw, unescape(raw), tuple(self.stack)))

    def handle_charref(self, name: str) -> None:
        raw = f"&#{name};"
        self.atoms.append(_Atom(raw, unescape(raw), tuple(self.stack)))

    def handle_comment(self, data: str) -> None:
        raise ValueError("Unsupported Telegram HTML comment")

    def handle_decl(self, decl: str) -> None:
        raise ValueError("Unsupported Telegram HTML declaration")


def visible_text(html: str) -> str:
    """Decode constrained, balanced Telegram HTML into its exact visible string."""
    return "".join(atom.text for atom in _HTML(html).atoms)


def _render(atoms: list[_Atom]) -> str:
    parts: list[str] = []
    active: tuple[_Tag, ...] = ()
    for atom in atoms:
        common = 0
        while common < min(len(active), len(atom.tags)) and active[common] == atom.tags[common]:
            common += 1
        parts.extend(f"</{tag.name}>" for tag in reversed(active[common:]))
        parts.extend(tag.opening for tag in atom.tags[common:])
        parts.append(atom.raw)
        active = atom.tags
    parts.extend(f"</{tag.name}>" for tag in reversed(active))
    return "".join(parts)


def _validate_limit(limit: int) -> None:
    if type(limit) is not int or limit < 1:
        raise ValueError("Telegram text limit must be a positive integer")


def _prefix_units(atoms: list[_Atom]) -> list[int]:
    prefix = [0]
    for atom in atoms:
        prefix.append(prefix[-1] + utf16_units(atom.text))
    return prefix


def split_html(html: str, *, limit: int = 4096) -> list[str]:
    """Split balanced HTML, retaining every visible character and each attribute.

    A link that fits the budget moves whole. An oversized link is closed and
    reopened like any other span. Impossible budgets (a two-unit character with
    limit one, or whitespace that cannot accompany any visible word) raise rather
    than silently discard canonical text.
    """
    _validate_limit(limit)
    atoms = _HTML(html).atoms
    if not any(atom.text.strip() for atom in atoms):
        return []
    prefix = _prefix_units(atoms)
    if prefix[-1] <= limit:
        return [html]

    # Spans carry an identity so adjacent links with the same URL stay distinct.
    anchors: dict[int, tuple[int, int]] = {}
    for index, atom in enumerate(atoms):
        for tag in atom.tags:
            if tag.name == "a":
                start, _ = anchors.get(tag.identity, (index, index))
                anchors[tag.identity] = (start, index + 1)

    # Some whitespace layouts cannot be split without a blank-only message.
    # Compute suffix reachability before preferring paragraphs, so a local cut
    # cannot strand whitespace that a later part cannot carry. Fitting anchors
    # are indivisible; oversized anchors use ordinary balanced boundaries.
    forbidden: set[int] = set()
    for left, right in anchors.values():
        if prefix[right] - prefix[left] <= limit:
            forbidden.update(range(left + 1, right))
    size = len(atoms)
    next_nonblank = [size] * (size + 1)
    for index in range(size - 1, -1, -1):
        next_nonblank[index] = index if atoms[index].text.strip() else next_nonblank[index + 1]
    feasible = [False] * size + [True]
    next_feasible = [size + 1] * size + [size]
    for index in range(size - 1, -1, -1):
        maximum = bisect_right(prefix, prefix[index] + limit) - 1
        first = next_nonblank[index]
        if index not in forbidden and first < size and next_feasible[first + 1] <= maximum:
            feasible[index] = True
        next_feasible[index] = index if feasible[index] else next_feasible[index + 1]
    if not feasible[0]:
        raise ValueError("Text cannot be partitioned into nonblank Telegram messages without splitting a fitting anchor")
    previous_feasible = [0] * (size + 1)
    for index in range(1, size + 1):
        previous_feasible[index] = index if feasible[index] else previous_feasible[index - 1]

    chunks: list[str] = []
    start = 0
    while start < size:
        maximum = bisect_right(prefix, prefix[start] + limit) - 1
        if maximum == size:
            end = maximum
        else:
            ends: list[list[int]] = [[], [], []]
            first = next_nonblank[start]
            for index in range(first, maximum):
                if not feasible[index + 1]:
                    continue
                text = atoms[index].text
                if text == "\n" and index > start and atoms[index - 1].text == "\n":
                    ends[0].append(index + 1)
                if text == "\n":
                    ends[1].append(index + 1)
                if text.isspace():
                    ends[2].append(index + 1)
            end = next((candidates[-1] for candidates in ends if candidates), previous_feasible[maximum])
        chunks.append(_render(atoms[start:end]))
        start = end
    return chunks


def clip_draft(html: str, limit: int = 4096) -> str:
    """Clip a disposable preview with a balanced, budgeted trailing ellipsis."""
    _validate_limit(limit)
    atoms = _HTML(html).atoms
    prefix = _prefix_units(atoms)
    if prefix[-1] <= limit:
        return html
    if limit == 1:
        return "…"
    try:
        chunks = split_html(html, limit=limit - 1)
        first = _HTML(chunks[0]).atoms if chunks else []
    except ValueError:
        # A disposable prefix still fits when the remaining source has no
        # feasible nonblank partition (or its first emoji needs two units).
        end = bisect_right(prefix, limit - 1) - 1
        first = atoms[:end]
    tags = first[-1].tags if first else ()
    return _render([*first, _Atom("…", "…", tags)])
