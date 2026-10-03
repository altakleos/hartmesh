"""Money parsing and currency consistency, with independently stated expected amounts."""

from __future__ import annotations

import csv
import importlib.util
import math
import sys
from decimal import Decimal
from pathlib import Path

import pytest

for _library in ("pandas", "openpyxl", "docx", "jinja2", "matplotlib", "xlsxwriter"):
    pytest.importorskip(_library)

SCRIPT = Path(__file__).resolve().parents[4] / "skills/public/business-report/scripts/report.py"


@pytest.fixture(scope="module")
def report():
    spec = importlib.util.spec_from_file_location("business_report_amounts_script", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    previous = sys.dont_write_bytecode
    sys.dont_write_bytecode = True
    try:
        spec.loader.exec_module(module)
    finally:
        sys.dont_write_bytecode = previous
    return module


def _source(tmp_path, name, amounts, header="Amount", currencies=None):
    path = tmp_path / name
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["Date", header, *(["Currency"] if currencies is not None else [])])
        for index, amount in enumerate(amounts):
            writer.writerow(["2026-09-01", amount, *([currencies[index]] if currencies is not None else [])])
    return str(path)


def _prepare(report, sources, currency=None):
    return report.prepare(sources, "2026-09", report.BuildOptions(currency=currency), None, report.load_profile(None, None))


@pytest.mark.parametrize(
    "raw,style,expected",
    [
        ("1e3", None, "1000"),
        ("1.25E+3", "us", "1250"),
        ("1,25e3", "eu", "1250"),
        ("1.234e3", "eu", "1234"),
        ("1,234e3", "us", "1234"),
        ("-2e-2", None, "-0.02"),
        ("$-100.00", "us", "-100"),
        ("USD −100.00", "us", "-100"),
        ("−100.00", "us", "-100"),
        ("-$100.00", "us", "-100"),
        ("(USD 100.00)", "us", "-100"),
        ("100.00-", "us", "-100"),
        ("100.00 USD-", "us", "-100"),
        ("USD100", None, "100"),
        ("+£100.00", "us", "100"),
        ("$1,234.56", "us", "1234.56"),
        ("1.234,56 €", "eu", "1234.56"),
        ("1 234,56", "eu", "1234.56"),
        ("1\u202f234,56", "eu", "1234.56"),
        ("CHF 1'234.56", "us", "1234.56"),
        ("CA$100.00", "us", "100"),
        ("A$100.00", "us", "100"),
        ("NZ$100.00", "us", "100"),
        (".50", "us", "0.50"),
        (",50", "eu", "0.50"),
        (1.25, "eu", "1.25"),
    ],
)
def test_money_forms_preserve_their_value(report, raw, style, expected):
    assert report.parse_amount(raw, style) == float(expected)
    assert report._decimal_from_cell(raw, style) == Decimal(expected)


@pytest.mark.parametrize("style", [None, "us", "eu"])
@pytest.mark.parametrize("raw", ["invoice 123", "1a2", "1e", "1-2", "100%", "1,2,3", "1.2.3", "(100", "100)", "--100", "(-100)", "NaN", "Infinity", "1e309", "1e-400", float("inf"), float("-inf"), True])
def test_invalid_amounts_never_become_invented_numbers(report, raw, style):
    assert math.isnan(report.parse_amount(raw, style))
    assert report._decimal_from_cell(raw, style) is None


@pytest.mark.parametrize("raw,expected", [("1e3", 1000), ("$-100.00", -100), ("−100.00", -100)])
def test_report_and_reconciliation_use_the_actual_amount(report, tmp_path, raw, expected):
    ctx = _prepare(report, [_source(tmp_path, "amount.csv", [raw])])
    built, _ = report.build_report(ctx, 0, report.compute_checks)
    assert built["kpis"][0]["value"] == expected
    assert built["rows"]["rows"][0][1] == expected
    assert next(check for check in built["checks"] if check["id"] == "totals_reconcile")["status"] == "pass"


def test_reconciliation_detects_a_corrupted_numeric_conversion(report, tmp_path):
    ctx = _prepare(report, [_source(tmp_path, "amount.csv", ["1e3", "$-100.00"])])
    ctx.all_rows.loc[:, "amount"] = [13, 100]
    built, _ = report.build_report(ctx, 0, report.compute_checks)
    assert next(check for check in built["checks"] if check["id"] == "totals_reconcile")["status"] == "fail"


def test_rejected_amounts_are_visible_in_checks(report, tmp_path):
    ctx = _prepare(report, [_source(tmp_path, "amount.csv", ["100", "invoice 123", "1-2", "Infinity"])])
    built, _ = report.build_report(ctx, 0, report.compute_checks)
    assert built["kpis"][0]["value"] == 100
    check = next(check for check in built["checks"] if check["id"] == "unparsed_amounts")
    assert check["status"] == "warn"
    assert "3 rows" in check["text"]
    assert "invoice 123" in check["text"]


@pytest.mark.parametrize("currency", [None, "USD", "EUR"])
@pytest.mark.parametrize("scenario", ["rows", "late_row", "header", "headers", "files", "same_cell", "currency_column"])
def test_mixed_currency_evidence_stops_aggregation(report, tmp_path, currency, scenario):
    if scenario == "rows":
        sources = [_source(tmp_path, "mixed.csv", ["USD 100", "EUR 100"])]
    elif scenario == "late_row":
        sources = [_source(tmp_path, "mixed.csv", ["USD 100"] * 205 + ["EUR 100"])]
    elif scenario == "header":
        sources = [_source(tmp_path, "mixed.csv", ["EUR 100"], header="Amount (USD)")]
    elif scenario == "headers":
        sources = [_source(tmp_path, "dollars.csv", ["100"], header="Amount (USD)"), _source(tmp_path, "euros.csv", ["100"], header="Amount (EUR)")]
    elif scenario == "files":
        sources = [_source(tmp_path, "dollars.csv", ["USD 100"]), _source(tmp_path, "euros.csv", ["EUR 100"])]
    elif scenario == "same_cell":
        sources = [_source(tmp_path, "mixed.csv", ["USD €100"])]
    else:
        sources = [_source(tmp_path, "mixed.csv", ["100", "100"], currencies=["USD", "EUR"])]
    with pytest.raises(report.InputError, match="currenc") as error:
        _prepare(report, sources, currency)
    assert "USD" in str(error.value) and "EUR" in str(error.value)
    assert "convert" in str(error.value).lower() or "separate" in str(error.value).lower()


def test_currency_option_cannot_relabel_explicit_amounts(report, tmp_path):
    sources = [_source(tmp_path, "usd.csv", ["USD 100"])]
    with pytest.raises(report.InputError, match="currenc"):
        _prepare(report, sources, "EUR")


@pytest.mark.parametrize("raw,header,expected", [("EUR100", "Amount", "EUR"), ("CA$100", "Amount", "CAD"), ("A$100", "Amount", "AUD"), ("NZ$100", "Amount", "NZD"), ("$100", "Amount (CAD)", "CAD")])
def test_currency_tokens_and_header_context_are_preserved(report, tmp_path, raw, header, expected):
    ctx = _prepare(report, [_source(tmp_path, "money.csv", [raw], header=header)])
    assert ctx.currency == expected
    assert ctx.all_rows["amount"].tolist() == [100]


def test_currency_evidence_in_later_file_applies_to_unlabeled_amounts(report, tmp_path):
    ctx = _prepare(report, [_source(tmp_path, "plain.csv", ["100"]), _source(tmp_path, "euros.csv", ["EUR 100"])])
    assert ctx.currency == "EUR"
    assert ctx.currency_source == "values"


def test_currency_option_resolves_unlabeled_amounts(report, tmp_path):
    ctx = _prepare(report, [_source(tmp_path, "plain.csv", ["100"])], "EUR")
    assert (ctx.currency, ctx.currency_source) == ("EUR", "preferences")


def test_same_currency_files_keep_their_own_number_style_for_checks(report, tmp_path):
    sources = [_source(tmp_path, "us.csv", ["EUR 1,234.50"]), _source(tmp_path, "eu.csv", ["EUR 2.000,50"])]
    ctx = _prepare(report, sources)
    built, _ = report.build_report(ctx, 0, report.compute_checks)
    assert built["kpis"][0]["value"] == 3235
    assert next(check for check in built["checks"] if check["id"] == "totals_reconcile")["status"] == "pass"


def test_cli_withholds_mixed_currency_artifacts(report, tmp_path, capsys):
    source = _source(tmp_path, "mixed.csv", ["USD 100", "EUR 100"])
    output = tmp_path / "output"
    code = report.main(["build", source, "--period", "2026-09", "--out", str(output), "--currency", "USD"])
    assert code == report.EXIT_WITHHELD
    assert "currenc" in capsys.readouterr().err
    assert not list(output.glob("*.report.json"))


def test_long_fraction_disambiguates_dot_decimal_amounts(report, tmp_path):
    ctx = _prepare(report, [_source(tmp_path, "precise.csv", ["2.675", "1.005", "-0.335", "0.0005", "0"])])
    assert ctx.all_rows["amount"].tolist() == [2.675, 1.005, -0.335, 0.0005, 0]


def test_large_finite_scientific_amount_can_be_formatted(report, tmp_path):
    ctx = _prepare(report, [_source(tmp_path, "scientific.csv", ["1e100"])])
    built, _ = report.build_report(ctx, 0, report.compute_checks)
    assert float(built["kpis"][0]["value"]) == 1e100
    assert report.format_value(built["kpis"][0]["value"], "currency").endswith(".00")


@pytest.mark.parametrize("currency", ["XYZ", "dollars", "USD / EUR"])
def test_unknown_currency_metadata_never_becomes_an_assumed_currency(report, tmp_path, currency):
    with pytest.raises(report.InputError, match="currency"):
        _prepare(report, [_source(tmp_path, "metadata.csv", ["100"], currencies=[currency])])


def test_subcent_table_corruption_is_withheld(report, tmp_path):
    ctx = _prepare(report, [_source(tmp_path, "amount.csv", ["100"])])
    section = {"id": "by_category", "heading": "By service", "table": {"columns": ["Revenue"], "rows": [[100.004]], "totals": [100]}}
    checks = report.compute_checks(ctx, ctx.all_rows, 100, 1, [section])
    assert next(check for check in checks if check["id"] == "totals_reconcile")["status"] == "fail"


def test_malformed_grouping_cannot_change_how_valid_amounts_are_read(report, tmp_path):
    ctx = _prepare(report, [_source(tmp_path, "amount.csv", ["1.234", "1.23,45"])])
    assert ctx.all_rows["amount"].iloc[0] == 1.234
    assert math.isnan(ctx.all_rows["amount"].iloc[1])


def test_independent_sum_retains_small_amounts_amid_large_cancelling_values(report):
    assert report.independent_amount_total(["1e100", "1", "-1e100"]) == 1
    assert report.independent_amount_total(["1e100", "1", "-1e100"], styles=["us", "us", "eu"]) == 1


def test_report_retains_small_amount_amid_large_cancelling_values(report, tmp_path):
    ctx = _prepare(report, [_source(tmp_path, "amount.csv", ["1e100", "1", "-1e100"])])
    built, _ = report.build_report(ctx, 0, report.compute_checks)
    assert built["kpis"][0]["value"] == 1
    assert next(check for check in built["checks"] if check["id"] == "totals_reconcile")["status"] == "pass"


def test_aggregate_overflow_is_an_actionable_input_error(report, tmp_path):
    ctx = _prepare(report, [_source(tmp_path, "overflow.csv", ["1e308", "1e308"])])
    with pytest.raises(report.InputError, match="range|large|finite"):
        report.build_report(ctx, 0, report.compute_checks)


@pytest.mark.parametrize("scientific", ["1.234e3", "1,234e3"])
def test_scientific_mantissa_is_not_mistaken_for_thousands_grouping(report, tmp_path, scientific):
    ctx = _prepare(report, [_source(tmp_path, "scientific.csv", [scientific, "850"])])
    assert ctx.all_rows["amount"].tolist() == [1234, 850]


@pytest.mark.parametrize("raw", ["1,234.56e2", "1.234,56e2", "1 234e2"])
def test_grouped_scientific_mantissas_are_unreadable(report, raw):
    assert math.isnan(report.parse_amount(raw))
