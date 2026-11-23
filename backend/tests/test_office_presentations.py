"""Office previews over ordinary files produced by the application's document libraries."""

import hashlib
import io
import json
import os
import tomllib
from datetime import date, time, timedelta
from pathlib import Path
from zipfile import ZipFile

import pytest
import yaml
from _office_fixture import W, archive, document, relationships, workbook, zip_parts
from _projection_support import installed_or_checkout_package, ready_projection_host
from _router_auth_helpers import make_authed_test_app
from fastapi.testclient import TestClient
from openpyxl import Workbook
from openpyxl.chart import BarChart, Reference
from openpyxl.utils.datetime import CALENDAR_MAC_1904
from pptx import Presentation

from app.gateway.routers import artifacts
from deerflow.config import paths as paths_module
from deerflow.extensions.loader import ExtensionSpec, load_extensions
from deerflow.runtime.user_context import get_effective_user_id

ROOT = Path(__file__).resolve().parents[2]
ENTRY = "hartmesh_office_preview:install"
GOLDEN = ROOT / "frontend-hm/tests/fixtures/office"


@pytest.fixture(scope="module", autouse=True)
def office_package():
    checkout = ROOT / "backend/extensions/sources/hartmesh-office-preview"
    with installed_or_checkout_package("hartmesh_office_preview", checkout):
        yield


def saved(document):
    stream = io.BytesIO()
    document.save(stream)
    return stream.getvalue()


def deck_sample():
    slides = Presentation()
    slides.core_properties.author = "Example"
    slides.core_properties.last_modified_by = "Example"
    slide = slides.slides.add_slide(slides.slide_layouts[1])
    slide.shapes.title.text = "Overview"
    slide.placeholders[1].text = "Milestones"
    slide.notes_slide.notes_text_frame.text = "Discuss the schedule."
    with ZipFile(io.BytesIO(saved(slides))) as template:
        return zip_parts({name: template.read(name) for name in template.namelist()})


@pytest.fixture
def projections():
    loaded, diagnostics = load_extensions([ExtensionSpec(use=ENTRY, required=True)])
    assert not diagnostics
    ((source, plugin),) = loaded.plugins
    assert source == ENTRY and plugin.namespace == "hartmesh.office-preview"
    assert {item.id: (item.suffixes, item.projection_marker) for item in plugin.artifacts} == {
        "workbook": ((".xlsx",), "office-workbook-v1"),
        "document": ((".docx",), "office-document-v1"),
        "deck": ((".pptx",), "office-deck-v1"),
    }
    return {item.id: item.project for item in plugin.artifacts}


def notice(view):
    return " ".join(block["text"] for block in view["blocks"] if block["type"] == "notice").lower()


@pytest.mark.parametrize(("kind", "suffix", "sample"), [("workbook", "xlsx", workbook), ("document", "docx", document), ("deck", "pptx", deck_sample)])
def test_passive_documents_match_the_browser_corpus(projections, kind, suffix, sample):
    raw = (GOLDEN / ("sample." + suffix)).read_bytes()
    # Compression implementations may differ. Compare the actual Office parts.
    with ZipFile(io.BytesIO(raw)) as recorded, ZipFile(io.BytesIO(sample())) as generated:
        assert set(recorded.namelist()) == set(generated.namelist())
        for name in generated.namelist():
            assert recorded.read(name) == generated.read(name)
    assert json.loads(projections[kind](raw)) == json.loads((GOLDEN / (kind + ".json")).read_bytes())


@pytest.mark.skipif(os.name == "nt", reason="descriptor-relative artifact reads require POSIX")
@pytest.mark.parametrize(("owner", "shared_formula"), [(True, False), (False, False), (True, True)])
def test_workbook_via_authorized_artifact_route(monkeypatch, tmp_path, owner, shared_formula):
    paths = paths_module.Paths(tmp_path)
    monkeypatch.setattr(paths_module, "_paths", paths)
    directory = paths.sandbox_outputs_dir("office-chat", user_id=get_effective_user_id())
    directory.mkdir(parents=True)
    source = workbook(cells='<c r="A1"><f t="shared" si="0" ref="A1:B1">C1+$D$2</f></c><c r="B1"><f t="shared" si="0"/></c>') if shared_formula else workbook()
    (directory / "sample.xlsx").write_bytes(source)
    app = make_authed_test_app(owner_check_passes=owner)
    app.state.extensions, diagnostics = load_extensions([ExtensionSpec(use=ENTRY, required=True)])
    assert not diagnostics
    app.include_router(artifacts.router)
    app.router.lifespan_context = ready_projection_host
    url = "/api/threads/office-chat/artifacts/mnt/user-data/outputs/sample.xlsx"
    with TestClient(app) as client:
        preview = client.get(url, params={"preview": "hartmesh.office-preview/workbook"})
        download = client.get(url, params={"download": True})
    if not owner:
        assert preview.status_code in {403, 404} and download.status_code in {403, 404}
        return
    assert preview.status_code == 200 and download.status_code == 200
    assert preview.headers["x-artifact-projection"] == "office-workbook-v1"
    assert preview.headers["etag"] == '"' + hashlib.sha256(source).hexdigest() + '"'
    assert int(preview.headers["x-artifact-source-bytes"]) == len(source)
    if shared_formula:
        assert preview.json()["blocks"][0]["rows"] == [["=C1+$D$2", "=D1+$D$2"]]
    else:
        assert preview.json() == json.loads((GOLDEN / "workbook.json").read_bytes())
    assert download.content == source


def test_workbook_preserves_coordinates_dates_and_formulas(projections):
    book = Workbook()
    sheet = book.active
    sheet.title = "Values"
    sheet["A1"] = "Item"
    sheet["C1"] = "Stored"
    sheet["B2"] = date(2024, 2, 29)
    sheet["C2"] = 0.25
    sheet["C2"].number_format = "0%"
    sheet["C3"] = "=1+1"
    hidden = book.create_sheet("Private sheet")
    hidden.sheet_state = "hidden"
    hidden["A1"] = "Hidden contents"
    source = saved(book)
    encoded = projections["workbook"](source)
    assert projections["workbook"](source) == encoded
    view = json.loads(encoded)
    (table,) = (block for block in view["blocks"] if block["type"] == "table")
    assert table["heading"] == "Sheet: Values"
    assert table["rows"] == [["Item", "", "Stored"], ["", "2024-02-29", "25%"], ["", "", "=1+1"]]
    assert b"Hidden contents" not in encoded
    assert "formula" in notice(view) and "download" in notice(view)


def test_sheet_limits_and_aggregate_cell_count(projections):
    book = Workbook()
    for title in ["First", "Second", "Third"]:
        sheet = book.active if title == "First" else book.create_sheet()
        sheet.title = title
        for i in range(205):
            sheet.append([i] * 14)
    view = json.loads(projections["workbook"](saved(book)))
    tables = [block for block in view["blocks"] if block["type"] == "table"]
    assert len(tables) == 3
    assert all(len(block["columns"]) == 12 and len(block["rows"]) <= 200 for block in tables)
    assert sum(len(block["columns"]) * (len(block["rows"]) + 1) for block in tables) <= 5000
    assert "download" in notice(view)


def test_document_reading_order_and_omissions(projections):
    raw = projections["document"](document())
    view = json.loads(raw)
    assert raw.index(b"Field notes") < raw.index(b"Opening") < raw.index(b"Remember") < raw.index(b"Left") < raw.index(b"Closing")
    assert view["blocks"][2]["type"] == "list"
    assert view["blocks"][3]["type"] == "table"
    assert b"Omitted heading" not in raw and "header" in notice(view)


def test_document_groups_ordinary_numbered_list_items(projections):
    media_type = "application/vnd.openxmlformats-officedocument.wordprocessingml."
    items = "".join(f'<w:p><w:pPr><w:numPr><w:ilvl w:val="0"/><w:numId w:val="5"/></w:numPr></w:pPr><w:r><w:t>{word}</w:t></w:r></w:p>' for word in ("First", "Second"))
    source = archive(
        "word/document.xml",
        media_type + "document.main+xml",
        {
            "word/document.xml": f'<w:document xmlns:w="{W}"><w:body>{items}</w:body></w:document>',
            "word/_rels/document.xml.rels": relationships(("rId1", "numbering", "numbering.xml")),
            "word/numbering.xml": (
                f'<w:numbering xmlns:w="{W}"><w:abstractNum w:abstractNumId="1">'
                '<w:lvl w:ilvl="0"><w:start w:val="1"/><w:numFmt w:val="decimal"/><w:lvlText w:val="%1."/></w:lvl></w:abstractNum>'
                '<w:num w:numId="5"><w:abstractNumId w:val="1"/></w:num></w:numbering>'
            ),
        },
        {"word/numbering.xml": media_type + "numbering+xml"},
    )
    lists = [block for block in json.loads(projections["document"](source))["blocks"] if block["type"] == "list"]
    assert lists == [{"type": "list", "ordered": True, "items": ["First", "Second"]}]


def test_deck_respects_order_visibility_and_reads_notes(projections):
    deck = Presentation()
    first = deck.slides.add_slide(deck.slide_layouts[1])
    first.shapes.title.text = "Launch"
    first.placeholders[1].text = "Agenda"
    first.notes_slide.notes_text_frame.text = "Presenter notes"
    hidden = deck.slides.add_slide(deck.slide_layouts[1])
    hidden._element.set("show", "0")
    hidden.shapes.title.text = "Hidden heading"
    raw = projections["deck"](saved(deck))
    view = json.loads(raw)
    assert view["blocks"][0]["heading"] == "Slide 1: Launch"
    assert "Agenda" in view["blocks"][0]["paragraphs"]
    assert b"Presenter notes" in raw and b"Hidden heading" not in raw
    assert "hidden" in notice(view)


def test_chart_sheet_merged_cells_and_1904_dates(projections):
    book = Workbook()
    book.epoch = CALENDAR_MAC_1904
    sheet = book.active
    sheet["A1"] = "Wide label"
    sheet.merge_cells("A1:B1")
    sheet["C1"] = date(2024, 2, 29)
    chart = BarChart()
    chart.add_data(Reference(sheet, min_col=3, min_row=1, max_row=1))
    book.create_chartsheet("Chart").add_chart(chart)
    view = json.loads(projections["workbook"](saved(book)))
    assert view["blocks"][0]["rows"] == [["Wide label", "", "2024-02-29"]]
    assert all(word in notice(view) for word in ("chart", "merged", "download"))


def test_shared_rich_text_and_zero_cached_formula(projections):
    source = workbook(cells='<c r="A1" t="s"><v>1</v></c><c r="B1"><f>1-1</f><v>0</v></c>', shared="<si><t>Unused string</t></si><si><r><t>Sea</t></r><r><t>weed</t></r></si>")
    raw = projections["workbook"](source)
    assert json.loads(raw)["blocks"][0]["rows"] == [["Seaweed", "0"]]
    assert b"Unused" not in raw and b"1-1" not in raw


def test_inline_phonetic_guide_does_not_change_the_cell_value(projections):
    source = workbook(cells='<c r="A1" t="inlineStr"><is><t>青海</t><rPh sb="0" eb="2"><t>Aomi</t></rPh></is></c>')
    assert json.loads(projections["workbook"](source))["blocks"][0]["rows"] == [["青海"]]


@pytest.mark.parametrize("mac_epoch", [False, True])
def test_ordinary_times_and_elapsed_durations_are_independent_of_the_epoch(projections, mac_epoch):
    book = Workbook()
    if mac_epoch:
        book.epoch = CALENDAR_MAC_1904
    book.active["A1"] = timedelta(hours=36)
    book.active["B1"] = time(12, 0)
    assert json.loads(projections["workbook"](saved(book)))["blocks"][0]["rows"] == [["36:00:00", "12:00:00"]]


@pytest.mark.parametrize(
    ("setting", "limit", "message"),
    [
        ("MAX_MEMBERS", 2, "ZIP directory"),
        ("MAX_DEPTH", 2, "XML depth"),
        ("MAX_PART", 128, "Expanded document"),
        ("MAX_TOTAL", 128, "Expanded document"),
    ],
)
def test_normal_document_respects_reduced_input_limits(projections, monkeypatch, setting, limit, message):
    from hartmesh_office_preview import package

    monkeypatch.setattr(package, setting, limit)
    with pytest.raises(ValueError, match=message):
        projections["document"](document())


def test_output_retains_a_notice_at_byte_budget(projections, monkeypatch):
    from hartmesh_office_preview import view as limits

    book = Workbook()
    for row in range(1, 9):
        book.active.cell(row, 1, "🎈" * 256)
    monkeypatch.setattr(limits, "MAX_BYTES", 12 * 1024)
    raw = projections["workbook"](saved(book))
    preview = json.loads(raw)
    assert len(raw) <= limits.MAX_BYTES
    assert 1 <= len(preview["blocks"][0]["rows"]) < 8
    assert "additional" in notice(preview) and "download" in notice(preview)


def test_projection_cuts_unicode_on_whole_code_points_including_formula_prefix(projections):
    book = Workbook()
    book.active["A1"] = "=" + "🎈" * 4096
    raw = projections["workbook"](saved(book))
    preview = json.loads(raw)
    assert preview["blocks"][0]["rows"] == [["=" + "🎈" * 4095]]
    assert "long text" in notice(preview)


def test_package_installed_and_template_entry_optional():
    project = tomllib.loads((ROOT / "backend/pyproject.toml").read_text())
    lock = tomllib.loads((ROOT / "backend/uv.lock").read_text())
    name = "hartmesh-office-preview"
    directory = "extensions/sources/" + name
    assert name in project["dependency-groups"]["extensions"]
    assert project["tool"]["uv"]["sources"][name] == {"path": directory}
    assert any(package["name"] == name and package["source"] == {"directory": directory} for package in lock["package"])
    template = yaml.safe_load((ROOT / "deploy/compose/config.yaml").read_text())
    entries = [item for item in template["plugins"] if item.get("package") == name or item["use"] == ENTRY]
    assert len(entries) <= 1
    for entry in entries:
        spec = ExtensionSpec.model_validate(entry)
        assert spec.enabled and not spec.required and spec.use == ENTRY and spec.package == name


def test_shared_formula_followers_display_their_own_expression(projections):
    source = workbook(
        rows=(
            '<row r="1"><c r="A1"><f t="shared" si="0" ref="A1:C2">SUM(A1:$C$4,$B1,A$1)</f></c>'
            '<c r="B1"><f t="shared" si="0"/><v/></c><c r="C1"><f t="shared" si="0"/><v>0</v></c></row>'
            '<row r="2"><c r="B2"><f t="shared" si="0"/></c><c r="C2"><f>1+2</f></c></row>'
        )
    )
    view = json.loads(projections["workbook"](source))
    assert view["blocks"][0]["rows"] == [
        ["=SUM(A1:$C$4,$B1,A$1)", "=SUM(B1:$C$4,$B1,B$1)", "0"],
        ["", "=SUM(B2:$C$4,$B2,B$1)", "=1+2"],
    ]
    assert "formula" in notice(view)


def test_hidden_shared_formula_anchor_and_reference_syntax(projections):
    source = workbook(
        rows=(
            '<row r="1" hidden="1"><c r="A1"><f t="shared" si="3" ref="A1:B2">'
            """SUM(A:A,$2:3,'Q1''s A1'!A1,Table1[[Q1]:[A1]],"A1",LOG10(A1),Rate_A1,Rate?A1,CaféA1,A1:B1!C1)"""
            '</f></c></row><row r="2"><c r="B2"><f t="shared" si="3"/></c></row>'
        )
    )
    view = json.loads(projections["workbook"](source))
    assert view["blocks"][0]["rows"] == [["", ""], ["", """=SUM(B:B,$2:4,'Q1''s A1'!B2,Table1[[Q1]:[A1]],"A1",LOG10(B2),Rate_A1,Rate?A1,CaféA1,A1:B1!D2)"""]]


def test_missing_shared_formula_anchor_is_reported(projections):
    with pytest.raises(ValueError, match="shared formula"):
        projections["workbook"](workbook(cells='<c r="A1"><f t="shared" si="3"/></c>'))


def test_shared_formula_saved_string_and_defined_name(projections):
    source = workbook(cells=('<c r="A1" t="str"><f t="shared" si="01" ref="A1:C1">SUM(A1:Tax)</f><v/></c><c r="B1" t="str"><f t="shared" si="1"/><v/></c><c r="C1" t="s"><f t="shared" si="1"/></c>'))
    assert json.loads(projections["workbook"](source))["blocks"][0]["rows"] == [["", "", "=SUM(C1:Tax)"]]


def test_long_shared_formula_is_translated_before_display_truncation(projections):
    expression = '"A1 ' + "x" * 4100 + '"'
    source = workbook(cells=f'<c r="A1"><f t="shared" si="0" ref="A1:B1">{expression}</f></c><c r="B1"><f t="shared" si="0"/></c>')
    view = json.loads(projections["workbook"](source))
    assert view["blocks"][0]["rows"] == [[("=" + expression)[:4096]] * 2]
    assert "long text" in notice(view)


def test_shared_formula_grid_edges_and_intersections(projections):
    source = workbook(cells='<c r="A1"><f t="shared" si="0" ref="A1:B1">SUM(XFD9,XFE9,A1 (B2:C3),A1 [@Amount],D : E)</f></c><c r="B1"><f t="shared" si="0"/></c>')
    view = json.loads(projections["workbook"](source))
    assert view["blocks"][0]["rows"][0][1] == "=SUM(#REF!,XFE9,B1 (C2:D3),B1 [@Amount],E : F)"
