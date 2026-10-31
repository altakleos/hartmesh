"""Pure fixed-text rendering and complete preview presentation plans.

Plans contain presentation only. The future outbox owns durable acknowledgements,
binding/revision checks and callback authority; a caller's message IDs are never
authorization to execute an action.
"""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from html import escape
from string import Formatter
from types import MappingProxyType
from typing import Any

from .texts import FIRST_KEYS, catalog, forms, words

FIRST_LINE_MAX = 60
FIRST_LINE_NAME_MAX = 30
OBJECTIVE_TITLE_MAX = 60

_REFERENCES = frozenset({"disk-too-small", "storage-missing", "storage-unreadable", "server-not-ready", "server-check-failed", "server-moved", "files-unchecked"})
_WITH_ENDINGS = frozenset({"file.own_unchecked", "file.own_unavailable", "unavailable.moved", "unavailable.files_unchecked", "unavailable.fix", "add.unavailable"})


def _clip(text: str, limit: int, *, at_word: bool = False) -> str:
    if len(text) <= limit:
        return text
    if limit <= 0:
        return ""
    prefix = text[: limit - 1]
    if at_word and " " in prefix:
        prefix = prefix.rsplit(" ", 1)[0]
    return prefix.rstrip() + "…"


def _format(template: str, values: Mapping[str, Any], *, html: bool) -> str:
    # Escape inserted values once, without interpreting their braces as templates.
    return template.format_map({key: escape(str(value), quote=True) if html else str(value) for key, value in values.items()})


def _first_line(template: str, values: dict[str, Any]) -> dict[str, Any]:
    first_values = values.copy()
    for key in ("person", "name"):
        if key in first_values:
            first_values[key] = _clip(str(first_values[key]), FIRST_LINE_NAME_MAX)
    # Objectives shrink first; brand names are also navigation text, so a long
    # deployment brand cannot make a product-owned notification line overflow.
    names = [name for _, name, _, _ in Formatter().parse(template) if name]
    for name in dict.fromkeys(["objective", *names]):
        if name not in first_values:
            continue
        while len(_format(template, first_values, html=False)) > FIRST_LINE_MAX and len(str(first_values[name])) > 1:
            excess = len(_format(template, first_values, html=False)) - FIRST_LINE_MAX
            first_values[name] = _clip(str(first_values[name]), max(1, len(str(first_values[name])) - excess), at_word=name == "objective")
    return first_values


def _render(
    key: str,
    locale: str,
    *,
    html: bool,
    reader_is_supervisor: bool,
    reader_is_administrator: bool,
    custody: str | None,
    topics_hint: bool,
    values: dict[str, Any],
) -> str:
    texts = catalog(locale)
    template = texts[key]  # Missing keys are always an error, including gated lines.
    variants = forms(locale).get(key, {})
    vocabulary = words(locale)
    if custody not in {None, "personal", "company"}:
        raise ValueError("custody must be personal, company or None")
    if custody is not None:
        template = variants.get(custody, template)
    if reader_is_administrator:
        template = variants.get(f"{custody}.administrator", variants.get("administrator", template))
    if "supervisor" in values and values["supervisor"] is None:
        template = variants.get("no_supervisor", template)
        # Templates needing a replacement sentence supply a no_supervisor form.
        # A standalone value still has a readable, safe fallback.
        values["supervisor"] = vocabulary["administrator"]
    elif reader_is_supervisor:
        values["supervisor"] = vocabulary["manager"]
    if topics_hint:
        template += variants.get("topics_hint", "")

    values["provider"] = values.get("provider") or vocabulary["provider"]
    values["support"] = values["provider"] if reader_is_administrator else vocabulary["administrator"]
    if values.get("person", True) is None:
        colleague = vocabulary["colleague"]
        values["person"] = colleague[0].upper() + colleague[1:] if template.lstrip().startswith("{person}") else colleague
    if "objective" in values:
        values["objective"] = _clip(str(values["objective"]), OBJECTIVE_TITLE_MAX, at_word=True)

    if key == "unavailable.next" and not reader_is_administrator:
        template = texts["unavailable.ask_admin"]
    if key == "unavailable.reference" and not reader_is_administrator:
        return ""
    if key == "file.own_unchecked":
        values["code"] = "files-unchecked"
    if reader_is_administrator and (key in _WITH_ENDINGS or key == "unavailable.reference") and values.get("code") is not None and values["code"] not in _REFERENCES:
        raise ValueError("code must be a public support reference word")
    if key in _WITH_ENDINGS:
        template += " " + texts["unavailable.next" if reader_is_administrator else "unavailable.ask_admin"]
        if reader_is_administrator and values.get("code"):
            template += "\n" + texts["unavailable.reference"]

    if key == "stop.answer":
        if values.get("written") is False:
            template = variants["unwritten"]
        if values.get("waiting"):
            template += " " + variants["waiting"]
    if key == "privacy.full" and values.get("transcriber") is not None:
        template += "\n\n" + texts["privacy.voice"]
    if key == "chat.restarted" and values.get("kept") is False:
        template = variants["no_message"]
    if values.get("voice_enabled") is False:
        template = variants.get("no_voice", template)

    # A role may be absent. Only product-owned punctuation and its empty line go.
    if values.get("role", True) in (None, ""):
        template = template.replace(" ({role})", "").replace(": {role}", "")
        values["role"] = ""
    if values.get("role_description", True) in (None, ""):
        template = template.replace("{role_description}\n", "").replace("\n{role_description}", "")
        values["role_description"] = ""

    if key in FIRST_KEYS:
        # This service notice is both a reply and a proactive text. Its catalog
        # keeps the exact two sentences; layout gives each sentence its own line
        # so neither is clipped merely to satisfy the notification's first line.
        if key == "delivery.failed":
            for boundary in (". ", "。"):
                before, found, after = template.partition(boundary)
                if found:
                    template = before + found.rstrip() + "\n" + after
                    break
        first, separator, rest = template.partition("\n")
        first = _format(first, _first_line(first, values), html=html)
        return first + (separator + _format(rest, values, html=html) if separator else "")
    return _format(template, values, html=html)


def render(
    key: str,
    locale: str,
    *,
    reader_is_supervisor: bool = False,
    reader_is_administrator: bool = False,
    custody: str | None = None,
    topics_hint: bool = False,
    **values: Any,
) -> str:
    """Render a complete product template, escaping every inserted HTML value."""
    return _render(key, locale, html=True, reader_is_supervisor=reader_is_supervisor, reader_is_administrator=reader_is_administrator, custody=custody, topics_hint=topics_hint, values=values)


def render_plain(
    key: str,
    locale: str,
    *,
    reader_is_supervisor: bool = False,
    reader_is_administrator: bool = False,
    custody: str | None = None,
    topics_hint: bool = False,
    **values: Any,
) -> str:
    """Apply the same forms and title limits, preserving literal value characters."""
    return _render(key, locale, html=False, reader_is_supervisor=reader_is_supervisor, reader_is_administrator=reader_is_administrator, custody=custody, topics_hint=topics_hint, values=values)


def _freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    if isinstance(value, (tuple, list)):
        return tuple(_freeze(item) for item in value)
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise TypeError("preview basis must contain JSON-compatible values")


@dataclass(frozen=True)
class PreviewPart:
    html: str
    silent: bool
    protected: bool
    controls_allowed: bool


@dataclass(frozen=True)
class PreviewPlan:
    """Stable presentation snapshot for a future durable outbox writer.

    ``basis`` holds the canonical basis/revisions supplied by its owner. ``parts``
    has no keyboards. ``next_part`` consumes that owner's trusted saved prefix
    acknowledgements; it checks shape only and confers no callback authority.
    Persist the plan and receipt mapping before transport; failed parts reuse it.
    """

    parts: tuple[str, ...]
    basis: Mapping[str, Any]
    silent: bool
    protected: bool

    def next_part(self, acknowledged_message_ids: Sequence[int]) -> PreviewPart | None:
        if len(acknowledged_message_ids) > len(self.parts) or any(type(message_id) is not int or message_id <= 0 for message_id in acknowledged_message_ids) or len(set(acknowledged_message_ids)) != len(acknowledged_message_ids):
            raise ValueError("expected an ordered prefix of saved part acknowledgements")
        index = len(acknowledged_message_ids)
        if index == len(self.parts):
            return None
        final = index == len(self.parts) - 1
        return PreviewPart(self.parts[index], self.silent if final else True, self.protected, final)


def render_preview(
    key: str,
    locale: str,
    *,
    heading: str,
    basis: Mapping[str, Any],
    silent: bool = False,
    protected: bool = False,
    limit: int = 4096,
    **values: Any,
) -> PreviewPlan:
    """Plan the full escaped preview, reserving each repeated header's UTF-16 units.

    Canonical fields have no presentation cap. The heading alone is navigation
    text. Earlier parts are silent and only the final part may carry controls.
    Outbox/callback code must still verify the saved basis, delivery and binding.
    """
    from .split import split_html, utf16_units, visible_text

    if type(limit) is not int or not 0 < limit <= 4096:
        raise ValueError("preview limit must be between 1 and 4096 UTF-16 units")
    body = render(key, locale, **values)
    if not body.strip():
        raise ValueError("a complete preview must have visible content")
    heading = heading.split("\n", 1)[0]
    # The complete count affects header width. Increase the reservation until it
    # covers the actual count; no canonical value is cut to obtain convergence.
    expected_parts = 1
    while True:
        suffix = render_plain("preview.part_header", locale, heading="", part=expected_parts, parts=expected_parts)
        title = _clip(heading, max(0, FIRST_LINE_MAX - len(suffix)), at_word=True)
        header = render("preview.part_header", locale, heading=title, part=expected_parts, parts=expected_parts)
        budget = limit - utf16_units(visible_text(header)) - 1
        if budget <= 0:
            raise ValueError("preview limit cannot fit the product heading and content")
        chunks = split_html(body, limit=budget)
        count = len(chunks)
        if count <= expected_parts:
            break
        expected_parts = count
    parts = tuple(render("preview.part_header", locale, heading=title, part=index, parts=count) + "\n" + chunk for index, chunk in enumerate(chunks, start=1))
    return PreviewPlan(parts, _freeze(basis), silent, protected)
