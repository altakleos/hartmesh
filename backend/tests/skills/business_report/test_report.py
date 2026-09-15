"""Contracts for the public business-report skill script.

The script turns a tabular export (CSV, XLSX, XLS) into one ``report.json`` and
renders that document to HTML, PDF, DOCX and XLSX inside the sandbox image. It
runs with the libraries the image ships, installs nothing, never shells out
and never modifies an input. Expected figures below are recomputed here with
the csv module and Decimal, independently of pandas.
"""

from __future__ import annotations

import ast
import csv
import datetime as dt
import importlib.util
import json
import re
import sys
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

import openpyxl
import pytest
from docx import Document
from jsonschema import Draft202012Validator, FormatChecker
from pypdf import PdfReader

REPO_ROOT = Path(__file__).resolve().parents[4]
SKILL_DIR = REPO_ROOT / "skills" / "public" / "business-report"
SCRIPT = SKILL_DIR / "scripts" / "report.py"
SKILL_DOC = SKILL_DIR / "SKILL.md"
PROFILE = SKILL_DIR / "profiles" / "services-generic.json"
SCHEMA = REPO_ROOT / "contracts" / "business_report" / "report.schema.json"
FIXTURES = Path(__file__).resolve().parent / "fixtures"
LARGE_CSV = FIXTURES / "example_services_export.csv"
LARGE_XLSX = FIXTURES / "example_services_export.xlsx"
SMALL_CSV = FIXTURES / "example_services_export_small.csv"
SMALL_XLS = FIXTURES / "example_services_export_small.xls"


def _load_script():
    spec = importlib.util.spec_from_file_location("business_report_script", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    # Never leave a __pycache__ inside the public skill package: the skill
    # reviewer treats every file there as package content.
    previous = sys.dont_write_bytecode
    sys.dont_write_bytecode = True
    try:
        spec.loader.exec_module(module)
    finally:
        sys.dont_write_bytecode = previous
    return module


@pytest.fixture(scope="module")
def report():
    return _load_script()


def _run(report, capsys, *argv: str) -> tuple[int, str, str]:
    code = report.main(list(argv))
    captured = capsys.readouterr()
    return code, captured.out, captured.err


def _build(report, capsys, out_dir: Path, *argv: str) -> tuple[int, str, str]:
    return _run(report, capsys, "build", *argv, "--out", str(out_dir))


def _report_path(out_dir: Path) -> Path:
    paths = sorted(out_dir.glob("*.report.json"))
    assert len(paths) == 1, paths
    return paths[0]


def _read_report(out_dir: Path) -> dict:
    return json.loads(_report_path(out_dir).read_text(encoding="utf-8"))


def _money(text: str) -> Decimal:
    return Decimal(text.replace("$", "").replace(",", ""))


def _expected_for_period(path: Path, start: dt.date, end: dt.date, amount_column: str = "Total") -> dict:
    """Independent totals: csv + Decimal, no pandas."""

    revenue = Decimal("0")
    jobs = 0
    per_person: dict[str, Decimal] = {}
    per_category: dict[str, Decimal] = {}
    unassigned = 0
    warranty = 0
    with path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            date = dt.date.fromisoformat(row["Completed On"])
            if not start <= date <= end:
                continue
            amount = _money(row[amount_column])
            jobs += 1
            revenue += amount
            person = row["Assigned To"] or "Unassigned"
            per_person[person] = per_person.get(person, Decimal("0")) + amount
            per_category[row["Service Type"]] = per_category.get(row["Service Type"], Decimal("0")) + amount
            if not row["Assigned To"]:
                unassigned += 1
            if row["Service Type"] == "Warranty":
                warranty += 1
    average = (revenue / jobs).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP) if jobs else None
    return {
        "revenue": revenue,
        "jobs": jobs,
        "average": average,
        "per_person": per_person,
        "per_category": per_category,
        "unassigned": unassigned,
        "warranty": warranty,
    }


AUGUST = (dt.date(2026, 8, 1), dt.date(2026, 8, 31))
JULY = (dt.date(2026, 7, 1), dt.date(2026, 7, 31))
LAST_AUGUST = (dt.date(2025, 8, 1), dt.date(2025, 8, 31))


@pytest.fixture(scope="module")
def august_report(report, tmp_path_factory) -> tuple[Path, dict]:
    """One build of the large fixture, shared by the read-only tests."""

    out_dir = tmp_path_factory.mktemp("august")
    code = report.main(["build", str(LARGE_CSV), "--period", "2026-08", "--out", str(out_dir)])
    assert code == 0
    return out_dir, _read_report(out_dir)


@pytest.fixture(scope="module")
def small_report(report, tmp_path_factory) -> tuple[Path, dict]:
    out_dir = tmp_path_factory.mktemp("small")
    code = report.main(["build", str(SMALL_CSV), "--period", "2026-08", "--out", str(out_dir)])
    assert code == 0
    return out_dir, _read_report(out_dir)


def _write_csv(path: Path, columns: list[str], rows: list[list[object]]) -> Path:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(columns)
        writer.writerows(rows)
    return path


# --- the script itself -------------------------------------------------------


def test_script_installs_nothing_never_shells_out_and_runs_on_the_image_python() -> None:
    source = SCRIPT.read_text(encoding="utf-8")

    ast.parse(source, feature_version=(3, 10))
    assert "subprocess" not in source
    assert "os.system" not in source
    assert not re.search(r"\bpip\b", source)
    assert not re.search(r"\b(urllib|requests|http\.client|socket)\b", source)
    assert "__import__" not in source
    assert "importlib" not in source


def test_missing_document_library_exits_with_a_message_naming_the_image(tmp_path, capsys) -> None:
    shadow = tmp_path / "shadow"
    shadow.mkdir()
    (shadow / "docx.py").write_text("raise ImportError('shadowed for the test')\n", encoding="utf-8")
    sys.path.insert(0, str(shadow))
    for name in [key for key in sys.modules if key == "docx" or key.startswith("docx.")]:
        sys.modules.pop(name)
    try:
        spec = importlib.util.spec_from_file_location("business_report_shadowed", SCRIPT)
        module = importlib.util.module_from_spec(spec)
        previous = sys.dont_write_bytecode
        sys.dont_write_bytecode = True
        try:
            with pytest.raises(SystemExit) as raised:
                spec.loader.exec_module(module)
        finally:
            sys.dont_write_bytecode = previous
    finally:
        sys.path.remove(str(shadow))
        for name in [key for key in sys.modules if key == "docx" or key.startswith("docx.")]:
            sys.modules.pop(name)

    assert raised.value.code == 2
    assert "docker/sandbox/Dockerfile" in capsys.readouterr().err


def test_profile_and_skill_doc_stay_in_lockstep_with_the_script(report) -> None:
    profile = json.loads(PROFILE.read_text(encoding="utf-8"))
    doc = SKILL_DOC.read_text(encoding="utf-8")

    assert set(profile["aliases"]) == set(report.ROLES)
    assert profile["name"] == report.DEFAULT_PROFILE
    for section in profile["sections"]:
        assert section in report.SECTION_BUILDERS
    assert "/mnt/skills/public/business-report/scripts/report.py" in doc
    assert report.DEFAULT_REPORTS_DIR in doc
    assert "preferences.json" in doc
    for command in ("inspect", "build", "prose", "render", "checks"):
        assert f"report.py {command}" in doc or f"report.py \\\n  {command}" in doc
    # The lasting-preference rule and the never-modify-inputs rule are the two
    # the agent must follow without being asked.
    assert "from now on" in doc
    assert "never modif" in doc.lower() or "never change" in doc.lower()


# --- family 1: column roles --------------------------------------------------


def test_generic_export_headers_map_to_every_role(report) -> None:
    table = report.read_table(str(LARGE_CSV))
    profile = report.load_profile(None, None)
    mapping = report.suggest_mapping(table.frame, profile)

    assert mapping.roles == {
        "date": "Completed On",
        "amount": "Total",
        "id": "Job #",
        "customer": "Customer Name",
        "category": "Service Type",
        "person": "Assigned To",
        "status": "Invoice Status",
        "source": "Source",
        "quantity": "Quantity",
        "location": "City",
    }
    assert mapping.ambiguous == {}
    assert mapping.missing == []
    assert all(level == "high" for level in mapping.confidence.values())


def test_two_matching_date_columns_are_ambiguous_until_the_mapping_settles_it(report, tmp_path, capsys) -> None:
    path = _write_csv(
        tmp_path / "two_dates.csv",
        ["Created", "Completed On", "Total"],
        [["2026-08-01", "2026-08-03", "10"], ["2026-07-30", "2026-08-05", "20"]],
    )
    profile = report.load_profile(None, None)
    mapping = report.suggest_mapping(report.read_table(str(path)).frame, profile)
    assert mapping.ambiguous == {"date": ["Created", "Completed On"]}
    assert mapping.roles["date"] is None

    code, out, err = _build(report, capsys, tmp_path / "out", str(path), "--period", "2026-08")
    assert code == report.EXIT_DECISION_NEEDED
    assert "Created" in err and "Completed On" in err
    assert not list((tmp_path / "out").glob("*.report.json"))

    mapping_file = tmp_path / "m.json"
    mapping_file.write_text(json.dumps({"date": "Completed On"}), encoding="utf-8")
    code, out, err = _build(report, capsys, tmp_path / "out", str(path), "--period", "2026-08", "--mapping", str(mapping_file))
    assert code == 0, err
    built = _read_report(tmp_path / "out")
    assert built["kpis"][0]["value"] == 30
    assert built["kpis"][1]["value"] == 2


def test_adversarial_export_blank_headers_mixed_types_and_formula_text(report, tmp_path) -> None:
    path = _write_csv(
        tmp_path / "messy.csv",
        ["", "Job #", "Completed On", "Total (USD)", "Notes", "Unnamed: 5"],
        [
            ["x", "A-1", "2026-08-02", "$1,234.50", "=SUM(A1:A2)", ""],
            ["y", "A-2", "08/03/2026", "(120.00)", "plain", ""],
            ["z", "A-3", "2026-08-04", "n/a", "", ""],
            ["w", "A-3", "not a date", "15", "", ""],
        ],
    )
    table = report.read_table(str(path))
    profile = report.load_profile(None, None)
    mapping = report.suggest_mapping(table.frame, profile)
    assert mapping.roles["date"] == "Completed On"
    assert mapping.roles["amount"] == "Total (USD)"
    assert mapping.roles["id"] == "Job #"
    assert "" not in mapping.roles.values()
    assert "Unnamed: 5" not in mapping.roles.values()

    clean = report.apply_mapping(table.frame, mapping.roles)
    amounts = clean.frame["amount"].tolist()
    assert amounts[0] == 1234.5
    assert amounts[1] == -120.0
    assert amounts[2] != amounts[2]  # NaN: unparsable amount
    assert amounts[3] == 15.0
    assert clean.unparsed_amounts == 1
    assert clean.unparsed_dates == 1
    assert report.detect_currency(table.frame, "Total (USD)") == ("USD", "header")


def test_amount_falls_back_to_the_only_unassigned_numeric_column(report, tmp_path) -> None:
    path = _write_csv(
        tmp_path / "fallback.csv",
        ["Job #", "When", "Charge"],
        [["10001", "2026-08-01", "12.5"], ["10002", "2026-08-02", "20"]],
    )
    profile = report.load_profile(None, None)
    mapping = report.suggest_mapping(report.read_table(str(path)).frame, profile)

    assert mapping.roles["id"] == "Job #"
    assert mapping.roles["date"] == "When"
    assert mapping.confidence["date"] == "low"
    assert mapping.roles["amount"] == "Charge"
    assert mapping.confidence["amount"] == "low"


def test_missing_required_role_is_reported_not_guessed(report, tmp_path) -> None:
    path = _write_csv(tmp_path / "no_amount.csv", ["Job #", "Completed On", "Customer"], [["1", "2026-08-01", "A"]])
    profile = report.load_profile(None, None)
    mapping = report.suggest_mapping(report.read_table(str(path)).frame, profile)

    assert mapping.roles["amount"] is None
    assert mapping.missing == ["amount"]


def test_workbook_sheet_is_chosen_by_content_and_addressed_by_workbook_and_name(report) -> None:
    table = report.read_table(str(LARGE_XLSX))
    assert table.sheet == "Jobs"
    assert table.rows == 5000

    named = report.read_table(f"{LARGE_XLSX}::Jobs")
    assert named.sheet == "Jobs"
    with pytest.raises(report.InputError, match="Notes"):
        report.read_table(f"{LARGE_XLSX}::Notes")
    with pytest.raises(report.InputError, match="Missing"):
        report.read_table(f"{LARGE_XLSX}::Missing")


def test_legacy_xls_reads_with_the_same_mapping(report) -> None:
    table = report.read_table(str(SMALL_XLS))
    mapping = report.suggest_mapping(table.frame, report.load_profile(None, None))

    assert table.rows == 300
    assert mapping.roles["date"] == "Completed On"
    assert mapping.roles["amount"] == "Total"
    assert mapping.missing == []


# --- family 2: metrics -------------------------------------------------------


def test_kpis_match_independent_totals_for_the_period(august_report) -> None:
    _out_dir, built = august_report
    expected = _expected_for_period(LARGE_CSV, *AUGUST)
    kpis = {kpi["id"]: kpi for kpi in built["kpis"]}

    assert Decimal(str(kpis["revenue"]["value"])).quantize(Decimal("0.01")) == expected["revenue"]
    assert kpis["revenue"]["format"] == "currency"
    assert kpis["jobs"]["value"] == expected["jobs"]
    assert Decimal(str(kpis["average_ticket"]["value"])).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP) == expected["average"]
    assert built["meta"]["period"] == {"start": "2026-08-01", "end": "2026-08-31", "label": "August 2026", "key": "2026-08"}
    assert built["meta"]["inputs"][0]["rows"] == 5000
    assert re.fullmatch(r"[0-9a-f]{64}", built["meta"]["inputs"][0]["sha256"])


def test_section_tables_match_independent_totals(august_report) -> None:
    _out_dir, built = august_report
    expected = _expected_for_period(LARGE_CSV, *AUGUST)
    sections = {section["id"]: section for section in built["sections"]}

    by_person = sections["by_person"]["table"]
    revenue_column = by_person["columns"].index("Revenue")
    got = {row[0]: Decimal(str(row[revenue_column])).quantize(Decimal("0.01")) for row in by_person["rows"]}
    assert got == expected["per_person"]
    assert Decimal(str(by_person["totals"][revenue_column])).quantize(Decimal("0.01")) == expected["revenue"]

    by_category = sections["by_category"]["table"]
    revenue_column = by_category["columns"].index("Revenue")
    got = {row[0]: Decimal(str(row[revenue_column])).quantize(Decimal("0.01")) for row in by_category["rows"]}
    assert got == expected["per_category"]


def test_period_boundaries_are_inclusive_and_rows_outside_are_counted(report, tmp_path, capsys) -> None:
    path = _write_csv(
        tmp_path / "edges.csv",
        ["Completed On", "Total"],
        [["2026-07-31", "1"], ["2026-08-01", "2"], ["2026-08-31 23:15", "4"], ["2026-09-01", "8"], ["", "16"]],
    )
    code, out, err = _build(report, capsys, tmp_path / "out", str(path), "--period", "2026-08")
    assert code == 0, err
    built = _read_report(tmp_path / "out")

    assert built["kpis"][0]["value"] == 6
    assert built["kpis"][1]["value"] == 2
    rows_used = next(check for check in built["checks"] if check["id"] == "rows_used")
    assert "2 of 5" in rows_used["text"]
    assert "1 had no usable date" in rows_used["text"]


def test_comparison_with_the_previous_period_and_the_same_period_last_year(august_report) -> None:
    _out_dir, built = august_report
    august = _expected_for_period(LARGE_CSV, *AUGUST)
    july = _expected_for_period(LARGE_CSV, *JULY)
    last_august = _expected_for_period(LARGE_CSV, *LAST_AUGUST)
    kpis = {kpi["id"]: kpi for kpi in built["kpis"]}

    expected_pct = ((august["revenue"] - july["revenue"]) / july["revenue"] * 100).quantize(Decimal("0.1"), rounding=ROUND_HALF_UP)
    assert kpis["revenue"]["delta"]["vs"] == "July 2026"
    assert Decimal(str(kpis["revenue"]["delta"]["pct"])).quantize(Decimal("0.1"), rounding=ROUND_HALF_UP) == expected_pct

    comparison = next(section for section in built["sections"] if section["id"] == "comparison")["table"]
    assert comparison["columns"] == ["Metric", "August 2026", "July 2026", "Change", "August 2025", "Change vs last year"]
    revenue_row = next(row for row in comparison["rows"] if row[0] == "Revenue")
    assert Decimal(str(revenue_row[2])).quantize(Decimal("0.01")) == july["revenue"]
    assert Decimal(str(revenue_row[4])).quantize(Decimal("0.01")) == last_august["revenue"]


def test_sections_the_data_cannot_support_are_listed_not_faked(report, tmp_path, capsys) -> None:
    path = _write_csv(tmp_path / "thin.csv", ["Completed On", "Total"], [["2026-08-01", "10"], ["2026-08-02", "20"]])
    code, out, err = _build(report, capsys, tmp_path / "out", str(path), "--period", "2026-08")
    assert code == 0, err
    built = _read_report(tmp_path / "out")
    section_ids = {section["id"] for section in built["sections"]}

    assert {"summary", "by_period", "actions"} <= section_ids
    assert not {"by_person", "by_category", "customers", "status", "source", "comparison"} & section_ids
    assert any("technician" in note and "no" in note for note in built["notes"])
    assert any("July 2026" in note for note in built["notes"])
    assert "Not included" in out


def test_exclusions_remove_rows_and_say_so(report, capsys, tmp_path) -> None:
    expected = _expected_for_period(LARGE_CSV, *AUGUST)
    code, out, err = _build(report, capsys, tmp_path / "out", str(LARGE_CSV), "--period", "2026-08", "--exclude", "category=Warranty")
    assert code == 0, err
    built = _read_report(tmp_path / "out")
    kpis = {kpi["id"]: kpi for kpi in built["kpis"]}

    assert kpis["jobs"]["value"] == expected["jobs"] - expected["warranty"]
    exclusions = next(check for check in built["checks"] if check["id"] == "exclusions")
    assert exclusions["status"] == "pass"
    assert f"Excluded {expected['warranty']} jobs where service = Warranty ($0)" in exclusions["text"]


def test_new_customers_need_earlier_rows_to_mean_anything(report, capsys, tmp_path) -> None:
    code, _out, err = _build(report, capsys, tmp_path / "august", str(SMALL_CSV), "--period", "2026-08")
    assert code == 0, err
    august = _read_report(tmp_path / "august")
    assert any(kpi["id"] == "new_customers" for kpi in august["kpis"])

    code, _out, err = _build(report, capsys, tmp_path / "july", str(SMALL_CSV), "--period", "2026-07")
    assert code == 0, err
    july = _read_report(tmp_path / "july")
    assert not any(kpi["id"] == "new_customers" for kpi in july["kpis"])
    assert any("earlier" in note for note in july["notes"])


def test_blank_person_rows_are_unassigned_and_in_the_checks_line(report, august_report) -> None:
    _out_dir, built = august_report
    expected = _expected_for_period(LARGE_CSV, *AUGUST)
    by_person = next(section for section in built["sections"] if section["id"] == "by_person")["table"]

    assert any(row[0] == "Unassigned" for row in by_person["rows"])
    unmapped = next(check for check in built["checks"] if check["id"] == "unmapped_rows")
    assert unmapped["status"] == "warn"
    assert f"{expected['unassigned']} jobs had no technician" in unmapped["text"]
    assert unmapped["text"] in report.checks_line(built)


# --- family 3: renderers -----------------------------------------------------


def test_report_json_validates_against_the_contract(august_report) -> None:
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    validator = Draft202012Validator(schema, format_checker=FormatChecker())

    _out_dir, built = august_report
    errors = [error.message for error in validator.iter_errors(built)]
    assert errors == []


def test_charts_are_drawn_locally_at_two_times_resolution(report, august_report) -> None:
    out_dir, built = august_report
    assert {chart["id"] for chart in built["charts"]} == {"revenue_by_period", "revenue_by_category", "jobs_by_person"}
    for chart in built["charts"]:
        png = out_dir / chart["png"]
        assert png.is_file()
        header = png.read_bytes()[:24]
        width = int.from_bytes(header[16:20], "big")
        assert header[:8] == b"\x89PNG\r\n\x1a\n"
        assert width >= 1200


def test_html_is_standalone_and_branded(report, small_report, tmp_path, capsys) -> None:
    out_dir, built = small_report
    tenant = tmp_path / "tenant"
    tenant.mkdir()
    logo = tenant / "logo.png"
    logo.write_bytes((out_dir / built["charts"][0]["png"]).read_bytes())
    (tenant / "brand.json").write_text(json.dumps({"company_name": "Example Services Co.", "logo": "logo.png", "colors": {"primary": "#0a6b3d", "secondary": "#9ccdb4"}}), encoding="utf-8")

    code, out, err = _run(report, capsys, "render", str(_report_path(out_dir)), "--to", "html", "--tenant", str(tenant))
    assert code == 0, err
    html_path = _report_path(out_dir).with_name(_report_path(out_dir).name.replace(".report.json", ".html"))
    html = html_path.read_text(encoding="utf-8")

    assert "#0a6b3d" in html
    assert "Example Services Co." in html
    assert "data:image/png;base64," in html
    assert "http://" not in html and "https://" not in html
    assert report.format_value(built["kpis"][0]["value"], "currency") in html
    assert 'lang="en-US"' in html


def test_docx_has_headings_tables_and_embedded_charts(report, small_report, capsys) -> None:
    out_dir, built = small_report
    code, out, err = _run(report, capsys, "render", str(_report_path(out_dir)), "--to", "docx")
    assert code == 0, err
    docx_path = _report_path(out_dir).with_name(_report_path(out_dir).name.replace(".report.json", ".docx"))
    document = Document(str(docx_path))

    headings = [paragraph.text for paragraph in document.paragraphs if paragraph.style.name.startswith("Heading")]
    for section in built["sections"]:
        assert section["heading"] in headings
    assert len(document.inline_shapes) == len(built["charts"])
    by_person = next(section for section in built["sections"] if section["id"] == "by_person")["table"]
    matching = [table for table in document.tables if [cell.text for cell in table.rows[0].cells] == by_person["columns"]]
    assert len(matching) == 1
    first_row = [cell.text for cell in matching[0].rows[1].cells]
    assert first_row[0] == by_person["rows"][0][0]
    assert first_row[2] == report.format_value(by_person["rows"][0][2], "currency")


def test_pdf_has_selectable_text_and_a_plausible_page_count(report, small_report, capsys) -> None:
    try:
        import weasyprint  # noqa: F401
    except (ImportError, OSError) as error:  # missing wheel, or missing pango on this host
        pytest.skip(f"weasyprint unavailable here: {error}")
    out_dir, built = small_report
    code, out, err = _run(report, capsys, "render", str(_report_path(out_dir)), "--to", "pdf")
    assert code == 0, err
    pdf_path = _report_path(out_dir).with_name(_report_path(out_dir).name.replace(".report.json", ".pdf"))
    reader = PdfReader(str(pdf_path))
    text = "\n".join(page.extract_text() for page in reader.pages)

    assert 2 <= len(reader.pages) <= 8
    assert built["meta"]["title"] in text
    assert report.format_value(built["kpis"][0]["value"], "currency") in text


def test_xlsx_totals_are_live_formulas_and_text_stays_text(report, tmp_path, capsys) -> None:
    path = _write_csv(
        tmp_path / "formula_text.csv",
        ["Job #", "Completed On", "Assigned To", "Total", "Notes"],
        [["1", "2026-08-01", "A", "10.5", "=SUM(A1:A2)"], ["2", "2026-08-02", "B", "20", "+1"], ["3", "2026-08-03", "A", "30", "@cmd"]],
    )
    code, out, err = _build(report, capsys, tmp_path / "out", str(path), "--period", "2026-08")
    assert code == 0, err
    code, out, err = _run(report, capsys, "render", str(_report_path(tmp_path / "out")), "--to", "xlsx")
    assert code == 0, err
    xlsx_path = _report_path(tmp_path / "out").with_name(_report_path(tmp_path / "out").name.replace(".report.json", ".xlsx"))
    workbook = openpyxl.load_workbook(xlsx_path)

    summary = workbook["Summary"]
    revenue_cell = next(summary.cell(row=row, column=2) for row in range(1, 20) if summary.cell(row=row, column=1).value == "Revenue")
    assert isinstance(revenue_cell.value, str) and revenue_cell.value.startswith("=SUM(Rows!")
    rows = workbook["Rows"]
    referenced = re.fullmatch(r"=SUM\(Rows!([A-Z]+)2:([A-Z]+)(\d+)\)", revenue_cell.value)
    assert referenced and referenced.group(1) == referenced.group(2)
    column, last = referenced.group(1), int(referenced.group(3))
    assert sum(rows[f"{column}{row}"].value for row in range(2, last + 1)) == 60.5

    by_person = workbook["By technician"]
    total_row = next(row for row in range(1, 20) if by_person.cell(row=row, column=1).value == "Total")
    assert str(by_person.cell(row=total_row, column=3).value).startswith("=SUM(")

    notes_column = next(cell.column for cell in rows[1] if cell.value == "Notes")
    for row in range(2, 5):
        cell = rows.cell(row=row, column=notes_column)
        assert cell.data_type == "s", (cell.value, cell.data_type)
    assert rows.cell(row=2, column=notes_column).value == "=SUM(A1:A2)"


def test_the_same_numbers_appear_in_every_render(report, small_report, capsys) -> None:
    out_dir, built = small_report
    for target in ("html", "docx", "xlsx"):
        code, out, err = _run(report, capsys, "render", str(_report_path(out_dir)), "--to", target)
        assert code == 0, err
    base = _report_path(out_dir).name.replace(".report.json", "")
    revenue = built["kpis"][0]["value"]
    formatted = report.format_value(revenue, "currency")

    assert formatted in (out_dir / f"{base}.html").read_text(encoding="utf-8")
    document = Document(str(out_dir / f"{base}.docx"))
    assert any(formatted in cell.text for table in document.tables for row in table.rows for cell in row.cells)
    workbook = openpyxl.load_workbook(out_dir / f"{base}.xlsx")
    by_person = workbook["By technician"]
    total_row = next(row for row in range(1, 30) if by_person.cell(row=row, column=1).value == "Total")
    referenced = re.fullmatch(r"=SUM\(C2:C(\d+)\)", str(by_person.cell(row=total_row, column=3).value))
    assert referenced
    assert round(sum(by_person.cell(row=row, column=3).value for row in range(2, int(referenced.group(1)) + 1)), 2) == round(revenue, 2)


def test_render_never_rebuilds_and_never_touches_inputs(report, small_report, capsys) -> None:
    out_dir, built = small_report
    before = SMALL_CSV.read_bytes()
    report_before = _report_path(out_dir).read_bytes()
    code, out, err = _run(report, capsys, "render", str(_report_path(out_dir)), "--to", "html")
    assert code == 0, err

    assert SMALL_CSV.read_bytes() == before
    assert _report_path(out_dir).read_bytes() == report_before
    assert built["meta"]["draft"] == 1


# --- family 4: checks --------------------------------------------------------


def test_a_failed_reconciliation_withholds_the_report(report, monkeypatch, tmp_path, capsys) -> None:
    monkeypatch.setattr(report, "independent_amount_total", lambda *args, **kwargs: 1.0)
    code, out, err = _build(report, capsys, tmp_path / "out", str(SMALL_CSV), "--period", "2026-08")

    assert code == report.EXIT_WITHHELD
    assert not list((tmp_path / "out").glob("*.report.json"))
    checks = json.loads((tmp_path / "out" / "checks.json").read_text(encoding="utf-8"))
    totals = next(check for check in checks if check["id"] == "totals_reconcile")
    assert totals["status"] == "fail"
    assert "withheld" in err.lower()


def test_every_other_failure_produces_the_report_with_the_line(report, tmp_path, capsys) -> None:
    path = _write_csv(
        tmp_path / "dupes.csv",
        ["Job #", "Completed On", "Assigned To", "Total"],
        [["1", "2026-08-01", "", "10"], ["1", "2026-08-02", "A", "20"], ["2", "2026-08-03", "A", "n/a"]],
    )
    code, out, err = _build(report, capsys, tmp_path / "out", str(path), "--period", "2026-08")
    assert code == 0, err
    built = _read_report(tmp_path / "out")
    statuses = {check["id"]: check["status"] for check in built["checks"]}

    assert statuses["totals_reconcile"] == "pass"
    assert statuses["duplicate_ids"] == "warn"
    assert statuses["unmapped_rows"] == "warn"
    assert statuses["unparsed_amounts"] == "warn"
    assert statuses["currency"] == "warn"
    line = report.checks_line(built)
    assert "Totals match your file" in line
    assert "1 job ID appears more than once" in line
    assert "assumed to be in USD" in line
    assert line in out


def test_checks_can_be_rerun_from_the_report_and_the_inputs(report, small_report, capsys) -> None:
    out_dir, built = small_report
    (out_dir / "checks.json").unlink()
    code, out, err = _run(report, capsys, "checks", str(_report_path(out_dir)), str(SMALL_CSV))
    assert code == 0, err
    rerun = json.loads((out_dir / "checks.json").read_text(encoding="utf-8"))

    assert [(check["id"], check["status"]) for check in rerun] == [(check["id"], check["status"]) for check in built["checks"]]
    assert "Totals match your file" in out


def test_currency_comes_from_values_or_is_stated_as_assumed(report, small_report, august_report) -> None:
    _small_dir, small = small_report
    _august_dir, august = august_report

    assert small["meta"]["currency"] == {"code": "USD", "source": "header"}
    assert next(check for check in small["checks"] if check["id"] == "currency")["status"] == "pass"
    assert august["meta"]["currency"] == {"code": "USD", "source": "assumed"}
    currency = next(check for check in august["checks"] if check["id"] == "currency")
    assert currency["status"] == "warn"
    assert "assumed" in currency["text"]


def test_workbook_inputs_state_that_external_links_were_not_checked(report, tmp_path, capsys) -> None:
    code, out, err = _build(report, capsys, tmp_path / "out", str(SMALL_XLS), "--period", "2026-08")
    assert code == 0, err
    built = _read_report(tmp_path / "out")
    external = next(check for check in built["checks"] if check["id"] == "external_links")
    assert external["status"] == "not_checked"


# --- family 5: prose ---------------------------------------------------------


def test_prose_numbers_are_verified_against_the_report(report, august_report) -> None:
    _out_dir, built = august_report
    revenue = report.format_value(built["kpis"][0]["value"], "currency")
    jobs = report.format_value(built["kpis"][1]["value"], "integer")
    pct = next(kpi for kpi in built["kpis"] if kpi["id"] == "revenue")["delta"]["pct"]
    text = f"August 2026 brought {revenue} across {jobs} jobs. Revenue was up {abs(pct):.1f}% on July. Warranty work reached $999,999 this month. Three technicians handled most of the work."

    clean, removed = report.verify_prose_numbers(built, text)

    assert removed == ["$999,999"]
    assert "999,999" not in clean
    assert revenue in clean and jobs in clean
    assert f"{abs(pct):.1f}%" in clean
    assert "Three technicians handled most of the work." in clean


def test_prose_accepts_rounded_and_formatted_variants_of_report_numbers(report, august_report) -> None:
    _out_dir, built = august_report
    revenue = built["kpis"][0]["value"]
    average = next(kpi for kpi in built["kpis"] if kpi["id"] == "average_ticket")["value"]
    rounded_thousands = f"${revenue / 1000:,.0f}k"
    text = f"Revenue reached about {rounded_thousands}, or {report.format_value(round(revenue), 'currency')} exactly, at an average of ${average:.0f} per job in 2026."

    clean, removed = report.verify_prose_numbers(built, text)

    assert removed == []
    assert clean == text


def test_prose_command_updates_the_report_and_bumps_the_draft(report, capsys, tmp_path) -> None:
    code, _out, err = _build(report, capsys, tmp_path / "out", str(SMALL_CSV), "--period", "2026-08")
    assert code == 0, err
    prose = tmp_path / "prose.json"
    built = _read_report(tmp_path / "out")
    revenue = report.format_value(built["kpis"][0]["value"], "currency")
    prose.write_text(
        json.dumps({"summary": [f"August closed at {revenue}. Bookings hit 987,654 last week."], "actions": ["Chase unpaid invoices.", "Book 7,777 more inspections."]}),
        encoding="utf-8",
    )

    code, out, err = _run(report, capsys, "prose", str(_report_path(tmp_path / "out")), "--from", str(prose))
    assert code == 0, err
    updated = _read_report(tmp_path / "out")
    summary = next(section for section in updated["sections"] if section["id"] == "summary")
    actions = next(section for section in updated["sections"] if section["id"] == "actions")

    assert updated["meta"]["draft"] == 2
    assert summary["paragraphs"] == [f"August closed at {revenue}."]
    assert actions["bullets"] == ["Chase unpaid invoices."]
    prose_check = next(check for check in updated["checks"] if check["id"] == "prose_numbers")
    assert prose_check["status"] == "warn"
    assert "987,654" in prose_check["text"] and "7,777" in prose_check["text"]
    assert "Removed" in out


# --- family 6: preferences ---------------------------------------------------


def test_preferences_merge_into_the_build_options_and_list_what_applied(report) -> None:
    options = report.BuildOptions()
    prefs = {
        "version": 1,
        "brand": {"primary": "#0a6b3d"},
        "exclusions": [{"role": "category", "equals": "Warranty"}],
        "summary_length": "short",
        "charts": ["revenue_by_period"],
        "unknown_key": True,
    }

    merged, applied = report.apply_preferences(options, prefs)

    assert applied == ["brand.primary", "exclusions[0]", "summary_length", "charts"]
    assert merged.brand["primary"] == "#0a6b3d"
    assert merged.exclusions == [{"role": "category", "equals": "Warranty"}]
    assert merged.summary_length == "short"
    assert merged.charts == ["revenue_by_period"]
    assert options.exclusions == []  # pure: the input is untouched


def test_preferences_apply_on_the_next_build_and_are_recorded(report, capsys, tmp_path) -> None:
    prefs = tmp_path / "preferences.json"
    prefs.write_text(json.dumps({"version": 1, "brand": {"primary": "#0a6b3d"}, "exclusions": [{"role": "category", "equals": "Warranty"}], "summary_length": "short"}), encoding="utf-8")
    expected = _expected_for_period(LARGE_CSV, *AUGUST)

    code, out, err = _build(report, capsys, tmp_path / "out", str(LARGE_CSV), "--period", "2026-08", "--prefs", str(prefs))
    assert code == 0, err
    built = _read_report(tmp_path / "out")

    assert built["meta"]["preferences_applied"] == ["brand.primary", "exclusions[0]", "summary_length"]
    assert built["kpis"][1]["value"] == expected["jobs"] - expected["warranty"]
    code, out, err = _run(report, capsys, "render", str(_report_path(tmp_path / "out")), "--to", "html")
    assert code == 0, err
    html = _report_path(tmp_path / "out").with_name(_report_path(tmp_path / "out").name.replace(".report.json", ".html")).read_text(encoding="utf-8")
    assert "#0a6b3d" in html


# --- the command surface -----------------------------------------------------


def test_inspect_reports_the_mapping_period_and_currency_as_json(report, capsys) -> None:
    code, out, err = _run(report, capsys, "inspect", str(LARGE_XLSX))
    assert code == 0, err
    inspected = json.loads(out)
    table = inspected["files"][0]["tables"][0]

    assert inspected["files"][0]["name"] == LARGE_XLSX.name
    assert table["sheet"] == "Jobs"
    assert table["rows"] == 5000
    assert table["mapping"]["date"] == "Completed On"
    assert table["ambiguous"] == {}
    assert table["missing"] == []
    assert table["period_suggestion"]["key"] == "2026-08"
    assert table["period_suggestion"]["label"] == "August 2026"
    assert table["date_range"] == {"start": "2025-08-01", "end": "2026-08-31"}
    assert table["currency"] == {"code": "USD", "source": "assumed"}
    assert [sheet["sheet"] for sheet in inspected["files"][0]["skipped_sheets"]] == ["Notes"]


def test_build_numbers_drafts_in_the_same_directory(report, capsys, tmp_path) -> None:
    code, out, _err = _build(report, capsys, tmp_path / "out", str(SMALL_CSV), "--period", "2026-08")
    assert code == 0
    assert "draft 1" in out
    code, out, _err = _build(report, capsys, tmp_path / "out", str(SMALL_CSV), "--period", "2026-08", "--exclude", "category=Warranty")
    assert code == 0
    assert "draft 2" in out
    assert _read_report(tmp_path / "out")["meta"]["draft"] == 2


def test_period_forms_and_labels(report) -> None:
    assert report.parse_period("2026-08").label == "August 2026"
    assert report.parse_period("2026-Q3").start == dt.date(2026, 7, 1)
    assert report.parse_period("2026-Q3").end == dt.date(2026, 9, 30)
    assert report.parse_period("2026-Q3").label == "Q3 2026"
    assert report.parse_period("2026").label == "2026"
    custom = report.parse_period("2026-08-01..2026-08-15")
    assert (custom.start, custom.end, custom.label) == (dt.date(2026, 8, 1), dt.date(2026, 8, 15), "1 to 15 August 2026")
    assert report.parse_period("2026-08-20..2026-09-05").label == "20 August to 5 September 2026"
    assert report.previous_period(report.parse_period("2026-01")).label == "December 2025"
    assert report.same_period_last_year(report.parse_period("2026-Q1")).label == "Q1 2025"
    with pytest.raises(report.InputError):
        report.parse_period("August")


def test_format_value_is_the_one_formatting_function(report) -> None:
    assert report.format_value(186420, "currency") == "$186,420"
    assert report.format_value(153.55, "currency") == "$153.55"
    assert report.format_value(-120, "currency") == "-$120"
    assert report.format_value(1214, "integer") == "1,214"
    assert report.format_value(8.21, "percent") == "8.2%"
    assert report.format_value(None, "currency") == "—"
    assert report.format_value(1234.5, "currency", "EUR") == "€1,234.50"
    assert report.format_value(1234.5, "currency", "CHF") == "CHF 1,234.50"
    assert report.format_value("Unassigned", "text") == "Unassigned"
