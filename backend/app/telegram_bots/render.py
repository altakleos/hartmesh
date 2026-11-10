"""Pure fixed-text rendering. The outbox owns acknowledgements and callback authority."""

from collections.abc import Mapping
from html import escape
from string import Formatter
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
    if key == "refused.files_full" and values.get("reader_is_manager") is not True:
        template = texts["refused.files_full_ask"]
    variants = forms(locale).get(key, {})
    vocabulary = words(locale)
    if custody not in {None, "personal", "company"}:
        raise ValueError("custody must be personal, company or None")
    if custody is not None:
        template = variants.get(custody, template)
    if key == "hello.assistant" and values.get("owner"):
        template = variants["colleague"]
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
            template += variants["waiting"]  # Chinese sentences take no space between them.
    if key in {"hello.assistant", "hello.employee"} and values.get("read_by_others"):
        first, rest = template.split("\n\n", 1)
        template = first + " " + texts["privacy.company"] + "\n\n" + rest
    if key == "privacy.full":
        if values.get("read_by_others"):
            template += "\n\n" + texts["privacy.company"]
        if values.get("protected"):
            template += "\n\n" + texts["privacy.protected"]
        if values.get("transcriber") is not None:
            template += "\n\n" + texts["privacy.voice"]
    if values.get("voice_enabled") is False:
        template = variants.get(f"{custody}.no_voice", variants.get("no_voice", template))

    # A role may be absent. Only product-owned punctuation and its empty line go.
    if values.get("role", True) in (None, ""):
        template = template.replace(" ({role})", "").replace(": {role}", "").replace("（{role}）", "").replace("：{role}", "")
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
