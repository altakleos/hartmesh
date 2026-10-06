"""The skill formats passive presentation without changing computed figures."""

from __future__ import annotations

import copy
import importlib
import json
import sys
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

ROOT = Path(__file__).resolve().parents[4]
SCRIPT_DIR = ROOT / "skills/public/business-report/scripts"
VIEW_SCHEMA = json.loads((ROOT / "contracts/artifact_view/view.schema.json").read_text(encoding="utf-8"))


@pytest.fixture
def serializer(monkeypatch):
    monkeypatch.syspath_prepend(str(SCRIPT_DIR))
    previous = sys.dont_write_bytecode
    sys.dont_write_bytecode = True
    try:
        return importlib.import_module("business_report_view")
    finally:
        sys.dont_write_bytecode = previous


@pytest.fixture
def document():
    return {
        "meta": {
            "title": "Monthly review",
            "company": "Example",
            "period": {"label": "September 2026"},
            "draft": 2,
            "currency": {"code": "USD"},
            "brand": {"primary": "#123456"},
            "inputs": [{"name": "export.csv", "uploaded": "2026-10-01", "rows": 3}],
        },
        "kpis": [{"label": "Revenue", "value": 1250.005, "format": "currency", "delta": {"pct": -0.04, "vs": "August"}}, {"label": "Jobs", "value": 3, "format": "integer"}],
        "sections": [
            {
                "heading": "Summary",
                "paragraphs": ["Review the source before acting."],
                "bullets": ["Check the flagged rows."],
                "table": {"columns": ["Item", "Value"], "formats": ["text", "currency"], "rows": [["Share", 12.25], ["Amount", -1.005]], "row_formats": [["text", "percent"], ["text", "currency"]], "totals": ["Total", 1250.005]},
                "note": "Amounts retain source precision.",
            }
        ],
        "charts": [{"id": "revenue", "png": "charts/revenue.png", "spec": {"title": "Revenue by period"}}],
        "checks": [
            {"id": "totals_reconcile", "status": "pass", "text": "Totals match your file."},
            {"id": "rows_used", "status": "warn", "text": "One row needs review."},
            {"id": "external_links", "status": "not_checked", "text": "External links were not checked."},
        ],
        "notes": ["Previous-year comparison is not included."],
        "rows": {"raw": "Supporting data stays in the original report."},
    }


def decode(serializer, document, exports=None):
    raw = serializer.serialize_view(document, "review.report.json", exports or ["review.pdf", "review.docx", "review.xlsx"])
    assert len(raw) <= 1024 * 1024
    view = json.loads(raw)
    Draft202012Validator(VIEW_SCHEMA).validate(view)
    return view


def test_values_row_overrides_footer_and_negative_zero_use_existing_python_formatting(serializer, document):
    before = copy.deepcopy(document)
    view = decode(serializer, document)
    facts = next(block for block in view["blocks"] if block["type"] == "facts")
    assert facts["items"][0] == {"label": "Revenue", "value": "$1,250.01", "detail": "-0.0% vs August"}
    assert facts["items"][1]["value"] == "3"
    table = next(block for block in view["blocks"] if block["type"] == "table")
    assert table["rows"] == [["Share", "12.3%"], ["Amount", "-$1.01"]]
    assert table["footer"] == ["Total", "$1,250.01"]
    assert document == before


def test_source_exports_charts_checks_notes_and_input_provenance_are_explicit(serializer, document):
    view = decode(serializer, document, ["review.xlsx"])
    assert view["primary_source"] == {"path": "review.report.json"}
    assert view["destination"] == {"collection": "Reports"}
    assert view["exports"] == [{"path": "review.xlsx", "label": "Excel workbook"}]
    assert next(block for block in view["blocks"] if block["type"] == "image")["path"] == "charts/revenue.png"
    notices = [block for block in view["blocks"] if block["type"] == "notice"]
    assert any(block["tone"] == "warning" and block["text"] == "One row needs review." for block in notices)
    assert any(block["tone"] == "neutral" and block["text"] == "External links were not checked." for block in notices)
    assert any(block.get("attribution") == "Report script" for block in notices)
    text = json.dumps(view)
    assert "Previous-year comparison is not included." in text
    assert "export.csv" in text and "2026-10-01" in text
    assert "Supporting data" not in text


@pytest.mark.parametrize("overflow", ["columns", "rows", "cells", "blocks", "text", "utf8_bytes", "images"])
def test_over_budget_content_gets_an_explicit_concise_omission_with_exports(serializer, document, overflow):
    table = document["sections"][0]["table"]
    if overflow == "columns":
        table["columns"] *= 7
    elif overflow == "rows":
        table["rows"] *= 101
    elif overflow == "cells":
        large = {"columns": [f"Value {index}" for index in range(12)], "formats": ["integer"] * 12, "rows": [list(range(12))] * 200, "totals": [0] * 12}
        document["sections"] = [{"heading": "Items", "table": large}] * 3
    elif overflow == "blocks":
        document["sections"] *= 65
    elif overflow == "text":
        document["sections"][0]["paragraphs"] = ["x" * 4097]
    elif overflow == "utf8_bytes":
        document["sections"] = [{"heading": "Notes", "paragraphs": ["界" * 4096] * 16}] * 6
    else:
        document["charts"] *= 33
    view = decode(serializer, document)
    assert any(block["type"] == "notice" and "omitted" in block["text"].lower() for block in view["blocks"])
    assert len(view["exports"]) == 3
    assert view["primary_source"]["path"] == "review.report.json"
    assert not any(block["type"] == "table" for block in view["blocks"])
    assert "One row needs review." in json.dumps(view)


@pytest.mark.parametrize("source", ["../source.json", "/source.json", "other.view.json", ".hidden.json", "bad%20name.json"])
def test_source_relation_never_names_an_unsafe_or_other_view_path(serializer, document, source):
    with pytest.raises(ValueError):
        serializer.serialize_view(document, source, ["review.pdf"])


def test_empty_exports_do_not_invent_formats(serializer, document):
    view = json.loads(serializer.serialize_view(document, "review.report.json", []))
    assert view["exports"] == []
    Draft202012Validator(VIEW_SCHEMA).validate(view)


def test_browser_fixture_is_exact_current_serializer_output(serializer):
    fixtures = ROOT / "frontend-hm/tests/fixtures/business-report"
    source = json.loads((fixtures / "2026-08-business-review.report.json").read_text(encoding="utf-8"))
    assert json.loads((fixtures / "generated-passive.view.json").read_text(encoding="utf-8")) == json.loads(serializer.serialize_view(source, "source.json", ["summary.pdf", "summary.docx"]))
