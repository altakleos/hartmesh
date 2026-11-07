"""Actual report commands preserve facts while selecting a new complete presentation."""

from __future__ import annotations

import copy
import importlib.util
import json
import sys
from pathlib import Path

import pytest

for _library in ("pandas", "docx", "pypdf", "jinja2", "matplotlib", "weasyprint"):
    pytest.importorskip(_library)

from .test_presentation import SCRIPTS, canonical, png, selected  # noqa: E402
from .test_presentation import modules as modules  # noqa: E402
from .test_publication import public_bytes  # noqa: E402

FIXTURE = Path(__file__).parent / "fixtures/example_services_export_small.csv"


@pytest.fixture
def report(modules):
    spec = importlib.util.spec_from_file_location("report_presentation_commands", SCRIPTS / "report.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def current(root):
    return root / json.loads((root / ".cache/business-report" / f"{root.name}.current.json").read_text())["report"]


def build(report, root, *extra):
    assert report.main(["build", str(FIXTURE), "--period", "2026-08", "--company", "Example Report Subject", "--out", str(root), "--bundle-id", "a", *extra]) == 0
    return current(root)


def test_current_partial_render_drops_incompatible_formats_and_preserves_old_bytes(report, tmp_path, monkeypatch):
    selected(tmp_path, monkeypatch)
    root = tmp_path / "out"
    old = build(report, root, "--render", "html,pdf,docx,xlsx")
    retained = public_bytes(old.parent)
    facts = json.loads(old.read_text())
    selected(tmp_path, monkeypatch, name="Example Tenant Revised", raw=png("blue"))
    assert report.main(["render", str(old), "--to", "html", "--bundle-id", "b"]) == 0
    new = current(root)
    assert json.loads(new.read_text()) == facts
    assert public_bytes(old.parent) == retained
    manifest = json.loads((new.parent / "renders.json").read_text())
    assert manifest["files"] == ["out.html", "out.xlsx"]
    assert "presentation.json" in manifest["sha256"] and "presentation/tenant.png" in manifest["sha256"]
    assert "chart-data.json" in manifest["sha256"]
    html = (new.parent / "out.html").read_text()
    assert "Example Tenant Revised" in html and "Example Tenant One" not in html
    assert "Example Report Subject" in html and "Product" in html and "Provider" in html
    view = json.loads((new.parent / "out.view.json").read_text())
    assert "accent" not in view
    assert {v["path"] for v in view["exports"]} == {"out.html", "out.xlsx"}


@pytest.mark.parametrize("command", ["render", "prose", "export"])
def test_one_capture_survives_input_removal_between_real_formats(report, tmp_path, monkeypatch, command):
    projection, _ = selected(tmp_path, monkeypatch)
    root = tmp_path / "out"
    old = build(report, root)
    original = report.render
    observed = []

    def removing(document, path, target, out_path, tenant, **kwargs):
        observed.append(kwargs["presentation"])
        projection.unlink(missing_ok=True)
        (tmp_path / "product.json").unlink(missing_ok=True)
        return original(document, path, target, out_path, tenant, **kwargs)

    monkeypatch.setattr(report, "render", removing)
    if command == "export":
        destination = tmp_path / "single.html"
        args = ["render", str(old), "--to", "html", "--out", str(destination)]
    elif command == "prose":
        args = ["prose", str(old), "--summary", "Review the figures.", "--render", "html,pdf,docx", "--bundle-id", "b"]
    else:
        args = ["render", str(old), "--to", "html,pdf,docx", "--bundle-id", "b"]
    assert report.main(args) == 0
    assert len(observed) == (1 if command == "export" else 3)
    assert all(item is observed[0] for item in observed)
    path = destination if command == "export" else current(root).parent / "out.html"
    assert "Example Tenant One" in path.read_text()
    assert report.main(["render", str(current(root)), "--to", "html"]) == 1


def test_regeneration_uses_verified_historical_preferences_without_current_profile_reads(report, tmp_path, monkeypatch):
    selected(tmp_path, monkeypatch)
    preferences = tmp_path / "preferences.json"
    preferences.write_bytes(b'{"version":1, "brand":{"primary":"#0a6b3d"}, "currency":"USD"}\n')
    root = tmp_path / "out"
    old = build(report, root, "--prefs", str(preferences))
    recorded = (old.parent / "preferences-used.json").read_bytes()
    preferences.write_bytes(b'{"version":1,"brand":{"primary":"#ffffff"},"currency":"EUR"}\n')
    current_preferences = preferences.read_bytes()
    selected(tmp_path, monkeypatch, name="Example Tenant Revised", raw=png("blue"))

    def never_analyse(*args, **kwargs):
        raise AssertionError("Branding-only operation must not reanalyse or reload the profile")

    monkeypatch.setattr(report, "prepare", never_analyse)
    monkeypatch.setattr(report, "load_profile", never_analyse)
    assert report.main(["render", str(old), "--to", "html,pdf", "--bundle-id", "b"]) == 0
    new = current(root)
    assert (new.parent / "preferences-used.json").read_bytes() == recorded
    assert preferences.read_bytes() == current_preferences
    assert json.loads(new.read_text())["meta"]["currency"]["code"] == "USD"
    presentation = json.loads((new.parent / "presentation.json").read_text())
    assert presentation["palette"]["primary"] == "#0a6b3d"


def test_legacy_palette_change_redraws_retained_chart_values_without_reanalysis(report, tmp_path, monkeypatch):
    tenant = tmp_path / "tenant"
    tenant.mkdir()

    def activate(name, color):
        (tenant / "brand.json").write_text(json.dumps({"company_name": name, "colors": {"primary": color}}))

    activate("Example Tenant One", "#123456")
    root = tmp_path / "out"
    old = build(report, root, "--tenant", str(tenant), "--render", "html,docx,xlsx")
    data = (old.parent / "chart-data.json").read_bytes()
    first_chart = json.loads(old.read_text())["charts"][0]["png"]
    original_png = (old.parent / first_chart).read_bytes()
    activate("Example Tenant Revised", "#006633")

    def never_analyse(*args, **kwargs):
        raise AssertionError("No new business analysis")

    monkeypatch.setattr(report, "prepare", never_analyse)
    assert report.main(["render", str(old), "--tenant", str(tenant), "--to", "html", "--bundle-id", "b"]) == 0
    new = current(root)
    assert (new.parent / "chart-data.json").read_bytes() == data
    assert (new.parent / first_chart).read_bytes() != original_png
    assert json.loads(new.read_text()) == json.loads(old.read_text())
    assert json.loads((new.parent / "renders.json").read_text())["files"] == ["out.html", "out.xlsx"]


def test_legacy_ambiguous_labels_remain_content_separate_from_current_tenant(report, tmp_path, monkeypatch):
    selected(tmp_path, monkeypatch)
    built = build(report, tmp_path / "original")
    legacy = copy.deepcopy(json.loads(built.read_text()))
    legacy["meta"]["brand"]["company"] = "Example Saved Label"
    legacy["charts"] = []
    for section in legacy["sections"]:
        section.pop("charts", None)
    legacy_root = tmp_path / "legacy"
    legacy_root.mkdir()
    source = legacy_root / "legacy.report.json"
    source.write_bytes(canonical(legacy))
    before = source.read_bytes()
    selected(tmp_path, monkeypatch, name="Example Tenant Revised")
    assert report.main(["render", str(source), "--to", "html,pdf", "--bundle-id", "b"]) == 0
    new = current(legacy_root)
    assert source.read_bytes() == before
    assert json.loads(new.read_text()) == legacy
    html = (new.parent / "legacy.html").read_text()
    assert all(name in html for name in ("Example Report Subject", "Example Saved Label", "Example Tenant Revised"))
    view = json.loads((new.parent / "legacy.view.json").read_text())
    assert "Saved label: Example Saved Label" in view["subtitle"]


@pytest.mark.parametrize("interrupted", [False, True])
def test_second_format_failure_preserves_previous_current_and_retained_members(report, tmp_path, monkeypatch, interrupted):
    selected(tmp_path, monkeypatch)
    root = tmp_path / "out"
    old = build(report, root, "--render", "html,pdf")
    before = public_bytes(old.parent)
    selected(tmp_path, monkeypatch, name="Example Tenant Revised")
    original = report.render

    def fail(document, path, target, out_path, tenant, **kwargs):
        if target == "html":
            raise KeyboardInterrupt if interrupted else RuntimeError("controlled second format failure")
        return original(document, path, target, out_path, tenant, **kwargs)

    monkeypatch.setattr(report, "render", fail)
    with pytest.raises(KeyboardInterrupt if interrupted else RuntimeError):
        report.main(["render", str(old), "--to", "html,pdf", "--bundle-id", "b"])
    assert current(root) == old
    assert public_bytes(old.parent) == before
    assert not (root / "drafts/b").exists()


def test_unaffected_explicit_xlsx_export_does_not_require_legacy_chart_pixels(report, tmp_path, monkeypatch):
    selected(tmp_path, monkeypatch)
    old = build(report, tmp_path / "original")
    legacy = tmp_path / "legacy"
    legacy.mkdir()
    source = legacy / "out.report.json"
    source.write_bytes(old.read_bytes())
    before = source.read_bytes()
    destination = tmp_path / "facts.xlsx"
    assert report.main(["render", str(source), "--to", "xlsx", "--out", str(destination)]) == 0
    assert destination.is_file() and source.read_bytes() == before


@pytest.mark.parametrize("command", ["build", "prose", "render", "export"])
def test_invalid_current_tenant_projection_preserves_old_current_and_exports(report, tmp_path, monkeypatch, command):
    projection, _ = selected(tmp_path, monkeypatch)
    root = tmp_path / "out"
    old = build(report, root, "--render", "html")
    before = public_bytes(old.parent)
    projection.write_bytes(b'{"schema_version":2}')
    destination = tmp_path / "existing.html"
    destination.write_bytes(b"keep previous export")
    if command == "build":
        args = ["build", str(FIXTURE), "--period", "2026-08", "--out", str(root), "--bundle-id", "b", "--render", "html"]
    elif command == "prose":
        args = ["prose", str(old), "--summary", "Review the facts.", "--render", "html", "--bundle-id", "b"]
    else:
        args = ["render", str(old), "--to", "html", *(["--out", str(destination)] if command == "export" else ["--bundle-id", "b"])]
    assert report.main(args) == 1
    assert current(root) == old and public_bytes(old.parent) == before
    assert destination.read_bytes() == b"keep previous export"
    assert not (root / "drafts/b").exists()


def test_strict_unreadable_primary_preferences_refuse_before_publishing(report, tmp_path, monkeypatch):
    selected(tmp_path, monkeypatch)
    root = tmp_path / "out"
    old = build(report, root, "--render", "html")
    before = public_bytes(old.parent)
    prefs = tmp_path / "preferences.json"
    raw = b'{"version":1,"brand":{"primary":"#ffffff"}}\n'
    prefs.write_bytes(raw)
    assert report.main(["build", str(FIXTURE), "--period", "2026-08", "--prefs", str(prefs), "--out", str(root), "--bundle-id", "b", "--render", "html"]) == 1
    assert current(root) == old and public_bytes(old.parent) == before
    assert prefs.read_bytes() == raw and not (root / "drafts/b").exists()


def test_legacy_missing_draw_values_refuses_affected_regeneration_truthfully(report, tmp_path, monkeypatch):
    selected(tmp_path, monkeypatch)
    old = build(report, tmp_path / "original")
    legacy = tmp_path / "legacy"
    legacy.mkdir()
    source = legacy / "out.report.json"
    source.write_bytes(old.read_bytes())
    before = source.read_bytes()
    assert report.main(["render", str(source), "--to", "html,pdf", "--bundle-id", "refused"]) == 1
    assert source.read_bytes() == before and not (legacy / "drafts/refused").exists()


def test_verified_old_spreadsheet_is_not_carried_when_report_content_changes(report, tmp_path, monkeypatch):
    selected(tmp_path, monkeypatch)
    root = tmp_path / "out"
    old = build(report, root, "--render", "xlsx")
    before = public_bytes(old.parent)
    original = report._source_report

    def changed(path):
        document, source_root, base, expected = original(path)
        document["kpis"][0]["value"] += 1
        return document, source_root, base, expected

    monkeypatch.setattr(report, "_source_report", changed)
    assert report.main(["render", str(old), "--to", "html", "--bundle-id", "b"]) == 0
    new = current(root)
    assert json.loads((new.parent / "renders.json").read_text())["files"] == ["out.html"]
    assert public_bytes(old.parent) == before
