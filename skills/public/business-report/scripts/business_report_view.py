"""Skill-owned passive display; calculations and ordinary exports stay unchanged."""

from __future__ import annotations

import json
import unicodedata
from pathlib import PurePosixPath

from business_report_common import cell_format, checks_line, format_value, is_color

VIEW_SUFFIX = ".view.json"
MAX_BYTES = 1024 * 1024
LABELS = {
    ".pdf": "PDF report",
    ".docx": "Editable Word report",
    ".xlsx": "Excel workbook",
    ".html": "HTML report",
}
TONES = {
    "pass": "positive",
    "warn": "warning",
    "fail": "negative",
    "not_checked": "neutral",
}
OMISSION = "Some preview content is omitted because it exceeds presentation limits. The original report and available exports retain the complete content."


class _PreviewLimit(ValueError):
    """A display budget cannot invalidate an independently rendered format."""


def _text(value: str, limit: int) -> str:
    if not isinstance(value, str):
        raise ValueError("Preview text must be authored text.")
    if len(value) > limit:
        raise _PreviewLimit("Preview text exceeds its display limit.")
    return value


def _short(value: str, limit: int) -> str:
    return value if len(value) <= limit else value[: limit - 1] + "…"


def _reference(name: str, *, sibling: bool = False) -> str:
    if (
        not isinstance(name, str)
        or not name
        or len(name) > 1024
        or any(char in "\\:%?#" or unicodedata.category(char) == "Cc" for char in name)
    ):
        raise ValueError("Preview references must be literal local filenames.")
    if any(not part or part.startswith(".") for part in name.split("/")):
        raise ValueError("Preview references cannot escape their directory.")
    if sibling and ("/" in name or name.lower().endswith(VIEW_SUFFIX)):
        raise ValueError("The primary source must be one ordinary sibling filename.")
    name.encode("utf-8")
    return name


def _base(report: dict, source_name: str, names: list[str]) -> dict:
    if len(names) > 16 or len(set(names)) != len(names):
        raise ValueError("Preview exports must be explicit and unique.")
    meta = report["meta"]
    subtitle = f"{meta['period']['label']} · {meta['currency']['code']} · Draft {meta['draft']}"
    if meta.get("company"):
        subtitle = f"{meta['company']} · {subtitle}"
    view = {
        "format": "hartmesh.artifact-view",
        "version": 1,
        "title": meta["title"],
        "subtitle": subtitle,
        "primary_source": {"path": _reference(source_name, sibling=True)},
        "destination": {"collection": "Reports"},
        "blocks": [],
        "exports": [
            {
                "path": _reference(name),
                "label": LABELS.get(PurePosixPath(name).suffix, f"Download {name}"),
            }
            for name in names
        ],
    }
    accent = meta.get("brand", {}).get("primary")
    if is_color(accent):
        view["accent"] = accent
    return view


def _fact(kpi: dict, currency: str) -> dict:
    item = {
        "label": _text(kpi["label"], 256),
        "value": _text(format_value(kpi["value"], kpi["format"], currency), 4096),
    }
    delta = kpi.get("delta")
    if delta and delta.get("pct") is not None:
        item["detail"] = _text(
            f"{'+' if delta['pct'] > 0 else ''}{format_value(delta['pct'], 'percent')} vs {delta['vs']}",
            4096,
        )
    if not item["label"]:
        raise ValueError("A preview fact needs its authored label.")
    return item


def _checks(report: dict, *, concise: bool = False) -> list[dict]:
    result = []
    checks = report["checks"]
    if concise:
        # Preserve warnings/failures before passing details if the model is huge.
        checks = sorted(
            checks,
            key=lambda check: {"fail": 0, "warn": 1, "not_checked": 2, "pass": 3}[
                check["status"]
            ],
        )[:32]
    summary = checks_line(report)
    if summary:
        tone = next(
            (
                TONES[status]
                for status in ("fail", "warn", "not_checked", "pass")
                if any(check["status"] == status for check in checks)
            ),
            "neutral",
        )
        result.append(
            {
                "type": "notice",
                "heading": "Checks",
                "tone": tone,
                "text": _short(summary, 4096) if concise else _text(summary, 4096),
                "attribution": "Report script",
            }
        )
    for check in checks:
        result.append(
            {
                "type": "notice",
                "tone": TONES[check["status"]],
                "text": _short(check["text"], 4096)
                if concise
                else _text(check["text"], 4096),
                "attribution": "Report script",
            }
        )
    return result


def _table(table: dict, heading: str, currency: str) -> dict:
    width = len(table["columns"])
    if not 1 <= width <= 12 or len(table["rows"]) > 200:
        raise _PreviewLimit("The complete table does not fit passive presentation.")
    if any(len(row) != width for row in table["rows"]) or (
        table.get("totals") is not None and len(table["totals"]) != width
    ):
        raise ValueError("Report table dimensions do not agree.")
    result = {
        "type": "table",
        "heading": _text(heading, 256),
        "columns": [
            {
                "label": _text(label, 256),
                "align": "start"
                if table["formats"][index] in ("text", "date")
                else "end",
            }
            for index, label in enumerate(table["columns"])
        ],
        "rows": [
            [
                _text(
                    format_value(
                        value, cell_format(table, row_index, column_index), currency
                    ),
                    4096,
                )
                for column_index, value in enumerate(row)
            ]
            for row_index, row in enumerate(table["rows"])
        ],
    }
    if table.get("totals") is not None:
        result["footer"] = [
            _text(format_value(value, cell_format(table, None, index), currency), 4096)
            for index, value in enumerate(table["totals"])
        ]
    if any(not column["label"] for column in result["columns"]):
        raise ValueError("Table columns require their authored labels.")
    return result


def _full(view: dict, report: dict) -> dict:
    _text(view["title"], 256)
    _text(view["subtitle"], 512)
    currency = report["meta"]["currency"]["code"]
    cells = images = 0

    def add(block: dict) -> None:
        nonlocal cells, images
        if block["type"] == "table":
            cells += len(block["columns"]) * (
                1 + len(block["rows"]) + (1 if "footer" in block else 0)
            )
        if block["type"] == "image":
            images += 1
        if len(view["blocks"]) >= 64 or cells > 5000 or images > 32:
            raise _PreviewLimit("Preview document resource budget exceeded.")
        view["blocks"].append(block)

    if report["kpis"]:
        if len(report["kpis"]) > 32:
            raise _PreviewLimit("Too many preview facts.")
        add(
            {"type": "facts", "items": [_fact(kpi, currency) for kpi in report["kpis"]]}
        )
    for block in _checks(report):
        add(block)
    for section in report["sections"]:
        heading = _text(section["heading"], 256)
        for key, kind, limit in (("paragraphs", "text", 16), ("bullets", "list", 32)):
            items = section.get(key, [])
            if not items:
                continue
            if len(items) > limit:
                raise _PreviewLimit("Preview prose list exceeds its display limit.")
            add(
                {
                    "type": kind,
                    "heading": heading,
                    "paragraphs" if kind == "text" else "items": [
                        _text(item, 4096) for item in items
                    ],
                }
            )
        if section.get("table"):
            add(_table(section["table"], heading, currency))
        if section.get("note"):
            add(
                {
                    "type": "notice",
                    "tone": "neutral",
                    "text": _text(section["note"], 4096),
                }
            )
    for chart in report["charts"]:
        title = chart.get("spec", {}).get("title") or chart["id"]
        add(
            {
                "type": "image",
                "path": _reference(chart["png"]),
                "alt": _text(title, 256),
                "heading": _text(title, 256),
            }
        )
    if report["notes"]:
        if len(report["notes"]) > 32:
            raise _PreviewLimit("Too many preview caveats.")
        add(
            {
                "type": "list",
                "heading": "Not included",
                "items": [_text(note, 4096) for note in report["notes"]],
            }
        )
    inputs = report["meta"].get("inputs", [])
    if inputs:
        if len(inputs) > 32:
            raise _PreviewLimit("Too many preview inputs.")
        add(
            {
                "type": "list",
                "heading": "Inputs",
                "items": [
                    _text(
                        f"{item['name']} (uploaded {item['uploaded']}, {format_value(item['rows'], 'integer')} rows)",
                        4096,
                    )
                    for item in inputs
                ],
            }
        )
    return view


def _concise(view: dict, report: dict) -> dict:
    view["title"] = _short(view["title"], 256)
    view["subtitle"] = _short(view["subtitle"], 512)
    view["blocks"] = [{"type": "notice", "tone": "warning", "text": OMISSION}]
    items = []
    for kpi in report["kpis"][:32]:
        try:
            items.append(_fact(kpi, report["meta"]["currency"]["code"]))
        except _PreviewLimit:
            # Never truncate a displayed number into a different amount.
            continue
    if items:
        view["blocks"].append({"type": "facts", "items": items})
    view["blocks"].extend(_checks(report, concise=True))
    return view


def _encode(view: dict) -> bytes:
    if not view["title"]:
        raise ValueError("A preview needs its report title.")
    return (
        json.dumps(view, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
        + "\n"
    ).encode("utf-8")


def serialize_view(report: dict, source_name: str, names: list[str]) -> bytes:
    """Return bounded UTF-8 bytes from existing authored/formatter output only."""
    base = _base(report, source_name, names)
    try:
        content = _encode(_full(dict(base, blocks=[]), report))
        if len(content) > MAX_BYTES:
            raise _PreviewLimit("Serialized preview exceeds one MiB.")
        return content
    except _PreviewLimit:
        content = _encode(_concise(dict(base, blocks=[]), report))
        if len(content) > MAX_BYTES:
            raise _PreviewLimit("Even the concise preview exceeds one MiB.")
        return content
