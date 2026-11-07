"""Retained draw values; redraw presentation without business reanalysis."""

from __future__ import annotations

import hashlib
import json
import math
import re

from business_report_common import CHART_PNG_PATTERN, InputError
from business_report_product import _canonical, _object, _read, _unique

MEMBER = "chart-data.json"
MAX_BYTES = 4 * 1024 * 1024
FIELDS = {"type", "x", "y", "title", "format", "labels", "values"}
UNAVAILABLE = "Saved chart draw data is unavailable or incompatible. Rebuild explicitly from the source inputs before changing its presentation."


def validate_data(raw, report):
    try:
        if len(raw) > MAX_BYTES:
            raise ValueError("bounds")
        record = _object(
            json.loads(raw.decode("utf-8"), object_pairs_hook=_unique),
            {"schema_version", "charts"},
        )
        if (
            type(record["schema_version"]) is not int
            or record["schema_version"] != 1
            or not isinstance(record["charts"], dict)
            or len(record["charts"]) > 64
        ):
            raise ValueError("version/charts")
        charts = record["charts"]
        if len(report["charts"]) != len(charts) or set(charts) != {
            c["id"] for c in report["charts"]
        }:
            raise ValueError("chart identity")
        for entry in report["charts"]:
            chart_id = entry["id"]
            data = _object(charts[chart_id], FIELDS)
            if (
                not re.fullmatch(r"[a-z0-9_]+", chart_id)
                or entry["png"] != f"charts/{chart_id}.png"
                or {k: v for k, v in data.items() if k not in {"labels", "values"}}
                != entry["spec"]
            ):
                raise ValueError("chart spec")
            if data["type"] not in {"bar", "barh"} or data["format"] not in {
                "currency",
                "integer",
                "number",
                "percent",
            }:
                raise ValueError("chart renderer")
            if any(
                not isinstance(data[k], str) or len(data[k]) > 32767
                for k in ("x", "y", "title")
            ):
                raise ValueError("chart text")
            if (
                not isinstance(data["labels"], list)
                or not isinstance(data["values"], list)
                or len(data["labels"]) != len(data["values"])
                or len(data["values"]) > 4096
            ):
                raise ValueError("chart values")
            if any(
                not isinstance(v, str) or len(v) > 32767 for v in data["labels"]
            ) or any(
                type(v) not in {int, float} or not math.isfinite(v)
                for v in data["values"]
            ):
                raise ValueError("chart scalar")
        return charts
    except (OSError, ValueError, TypeError, KeyError, RecursionError, OverflowError):
        raise InputError(UNAVAILABLE) from None


def write_data(stage, charts, report):
    raw = _canonical({"schema_version": 1, "charts": charts}) + b"\n"
    validate_data(raw, report)
    (stage / MEMBER).write_bytes(raw)


def stage_charts(report, source, stage, selected, manifest, previous, draw):
    for chart in report["charts"]:
        if (
            not isinstance(chart, dict)
            or not isinstance(chart.get("png"), str)
            or not CHART_PNG_PATTERN.fullmatch(chart["png"])
        ):
            raise InputError("Chart paths must use charts/<id>.png inside this report.")
    if not report["charts"]:
        write_data(stage, {}, report)
        return
    if not (source.parent / MEMBER).is_file() or (
        manifest is not None and MEMBER not in manifest["sha256"]
    ):
        raise InputError(UNAVAILABLE)
    raw = _read(str(source.parent.resolve() / MEMBER), MAX_BYTES)
    if (
        manifest is not None
        and hashlib.sha256(raw).hexdigest() != manifest["sha256"][MEMBER]
    ):
        raise InputError(UNAVAILABLE)
    data = validate_data(raw, report)
    (stage / MEMBER).write_bytes(raw)
    if (
        manifest is None
        or previous is None
        or previous["palette"]["primary"] != selected.primary
    ):
        draw(data, stage, selected.brand(), report["meta"]["currency"]["code"])
        return
    # Charts contain this palette and these retained values, not tenant names.
    # Their verified pixels can survive an identity-only presentation change.
    for chart in report["charts"]:
        name = chart["png"]
        raw = _read(str(source.parent.resolve() / name), 8 * 1024 * 1024)
        if hashlib.sha256(raw).hexdigest() != manifest["sha256"].get(name):
            raise InputError(
                "A retained chart changed before staging; nothing was published."
            )
        destination = stage / name
        destination.parent.mkdir(exist_ok=True)
        destination.write_bytes(raw)
