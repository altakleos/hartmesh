#!/usr/bin/env python3
"""Qualify complete presentations using real offline commands and shipped renderers.

Test subprocesses run Python only; production report code starts no subprocess.
Run in the sandbox image with networking disabled and read-only skill/fixtures.
"""

from __future__ import annotations

import argparse
import base64
import csv
import importlib.util
import io
import json
import os
import re
import subprocess
import sys
from decimal import Decimal
from pathlib import Path
from zipfile import ZipFile

import fitz
import openpyxl
import PIL
import weasyprint
from check_product_reports_on_image import assert_pdf, current, retained, sha
from docx import Document
from PIL import Image


def compact(text):
    return re.sub(r"\s+", "", text)


def pixels(data):
    image = Image.open(io.BytesIO(data)).convert("RGB")
    return image.size, image.tobytes()


def assert_tenant_pdf(path, projection, image_dir):
    doc = fitz.open(path)
    text = " ".join(page.get_text() for page in doc)
    tenant = projection["tenant"]
    if tenant is None:
        assert "Example Tenant" not in text
        return
    assert "Workspace" in text and compact(tenant["name"]) in compact(text)
    expected = pixels(base64.b64decode(tenant["logo"].split(",", 1)[1]))
    images = {entry[0] for page in doc for entry in page.get_images(full=True)}
    assert any(pixels(doc.extract_image(xref)["image"]) == expected for xref in images), "actual PDF is missing the selected tenant pixels"
    # The affected tenant text is fixed black on white, independent of palette.
    assert any(span["color"] == 0 for page in doc for block in page.get_text("dict")["blocks"] for line in block.get("lines", []) for span in line["spans"] if "Workspace" in span["text"])
    image_dir.mkdir(parents=True, exist_ok=True)
    doc[0].get_pixmap(matrix=fitz.Matrix(1.3, 1.3)).save(str(image_dir / (path.stem + "-page-1.png")))


def assert_docx(path, projection):
    doc = Document(path)
    text = " ".join(p.text for p in doc.paragraphs)
    assert "Example Report Subject" in text
    assert "Example Provider" not in text
    tenant = projection["tenant"]
    if tenant:
        assert tenant["name"] in text
        assert doc.inline_shapes[0].width.inches <= 1.501 and doc.inline_shapes[0].height.inches <= 0.601
        expected = pixels(base64.b64decode(tenant["logo"].split(",", 1)[1]))
        with ZipFile(path) as archive:
            assert any(pixels(archive.read(name)) == expected for name in archive.namelist() if name.startswith("word/media/"))
    else:
        assert "Example Tenant" not in text


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--skill", required=True, type=Path)
    parser.add_argument("--fixtures", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    assert importlib.util.find_spec("deerflow") is None, "Do not borrow Gateway dependencies"
    profiles = args.fixtures / "tenant-presentation"
    assert os.statvfs(profiles).f_flag & os.ST_RDONLY
    assert os.statvfs(args.skill).f_flag & os.ST_RDONLY
    fixture_hashes = {p.name: sha(p) for p in profiles.glob("*.json")}
    source = args.fixtures / "example_services_export_small.csv"
    source_hash = sha(source)
    with source.open(encoding="utf-8", newline="") as stream:
        rows = [row for row in csv.DictReader(stream) if row["Completed On"].startswith("2026-08-")]
    total = sum((Decimal(row["Total (USD)"].replace("$", "").replace(",", "")) for row in rows), Decimal(0))
    product_path = args.fixtures / "product-identity/one.json"
    product = json.loads(product_path.read_text(encoding="utf-8"))
    base_env = {k: v for k, v in os.environ.items() if not k.startswith("HARTMESH_") and k != "PYTHONPATH"}
    base_env["PYTHONDONTWRITEBYTECODE"] = "1"
    env = dict(base_env)
    script = args.skill / "scripts/report.py"
    commands = 0

    def run(*command, expected=0, selected_env=None):
        nonlocal commands
        commands += 1
        result = subprocess.run([sys.executable, str(script), *map(str, command)], env=selected_env or env, cwd=args.output, capture_output=True, text=True, encoding="utf-8", timeout=180)
        with (args.output / "commands.log").open("a", encoding="utf-8") as log:
            log.write(result.stdout + result.stderr)
        assert result.returncode == expected, (result.returncode, result.stdout, result.stderr)
        return result

    def activate(name, *, default=False):
        projection_path = profiles / f"{name}.json"
        projection = json.loads(projection_path.read_text(encoding="utf-8"))
        shared_path = profiles / "default-product.json" if default else product_path
        shared = json.loads(shared_path.read_text(encoding="utf-8"))
        env.update(HARTMESH_REPORT_PRODUCT_IDENTITY=str(shared_path), HARTMESH_REPORT_PRODUCT_SHA256=shared["sha256"], HARTMESH_REPORT_TENANT_PRESENTATION=str(projection_path), HARTMESH_REPORT_TENANT_SHA256=projection["snapshot_id"])
        return projection

    def facts(path):
        document = json.loads(path.read_text(encoding="utf-8"))
        values = {k["id"]: k["value"] for k in document["kpis"]}
        assert Decimal(str(values["revenue"])) == total and values["jobs"] == len(rows)
        assert document["meta"]["company"] == "Example Report Subject"
        return document

    def inspection(path):
        folder = args.output / "inspection" / (path.parent.parent.parent.name + "-" + path.parent.name if path.parent.parent.name == "drafts" else "exports")
        folder.mkdir(parents=True, exist_ok=True)
        return folder

    def complete(path, selected, shared=product):
        base = path.name.removesuffix(".report.json")
        html = path.with_name(base + ".html").read_text(encoding="utf-8")
        assert "Example Report Subject" in html
        if selected["tenant"]:
            assert selected["tenant"]["name"] in html and selected["tenant"]["logo"] in html
            assert 'aria-label="Workspace:' in html
        else:
            assert "Example Tenant" not in html
        shared_attribution = shared if shared and shared["identity"]["mode"] == "configured" else None
        assert_pdf(path.with_name(base + ".pdf"), shared_attribution, "Example Report Subject", image_dir=inspection(path))
        assert_tenant_pdf(path.with_name(base + ".pdf"), selected, inspection(path))
        if path.with_name(base + ".docx").exists():
            assert_docx(path.with_name(base + ".docx"), selected)
        if path.with_name(base + ".xlsx").exists():
            workbook = openpyxl.load_workbook(path.with_name(base + ".xlsx"), data_only=True)
            assert Decimal(str(workbook["Summary"]["B2"].value)) == total
        view = json.loads(path.with_name(base + ".view.json").read_text(encoding="utf-8"))
        manifest = json.loads((path.parent / "renders.json").read_text(encoding="utf-8"))
        assert "accent" not in view
        assert {entry["path"] for entry in view["exports"]} == set(manifest["files"])
        assert "presentation.json" in manifest["sha256"] and "chart-data.json" in manifest["sha256"]
        facts(path)

    # Default legacy still renders without inventing tenant or provider identity.
    legacy = args.output / "unconfigured"
    run("build", source, "--period", "2026-08", "--company", "Example Report Subject", "--out", legacy, "--bundle-id", "a", "--render", "html,pdf,docx,xlsx")
    none = {"tenant": None}
    complete(current(legacy), none, None)
    print("unconfigured legacy: real HTML/PDF/DOCX/XLSX passed", flush=True)

    a = activate("a")
    root = args.output / "report"
    prefs = args.output / "preferences.json"
    prefs.write_text('{"version":1,"brand":{"primary":"#0a6b3d"},"currency":"USD"}\n', encoding="utf-8")
    run("build", source, "--period", "2026-08", "--company", "Example Report Subject", "--prefs", prefs, "--out", root, "--bundle-id", "a", "--render", "html,pdf,docx,xlsx")
    first = current(root)
    report = facts(first)
    first_bytes = retained(first.parent)
    preference_bytes = (first.parent / "preferences-used.json").read_bytes()
    chart_bytes = (first.parent / "chart-data.json").read_bytes()
    complete(first, a)
    prefs.write_text('{"version":1,"currency":"EUR","brand":{"primary":"#ffffff"}}\n', encoding="utf-8")
    current_prefs = prefs.read_bytes()
    b = activate("b")
    run("render", first, "--to", "html", "--bundle-id", "b-one")
    partial = current(root)
    assert facts(partial) == report
    assert json.loads((partial.parent / "renders.json").read_text(encoding="utf-8"))["files"] == ["report.html", "report.xlsx"]
    assert not partial.with_name("report.pdf").exists() and not partial.with_name("report.docx").exists()
    run("render", partial, "--to", "all", "--bundle-id", "b-all")
    second = current(root)
    complete(second, b)
    assert (second.parent / "preferences-used.json").read_bytes() == preference_bytes
    assert (second.parent / "chart-data.json").read_bytes() == chart_bytes
    assert prefs.read_bytes() == current_prefs and retained(first.parent) == first_bytes
    assert facts(second) == report
    print("same tenant A→B: complete current selection, incompatible carry omitted, retained facts/preferences/bytes passed", flush=True)

    # Prose changes only the requested words; restoration keeps current settings.
    run("prose", second, "--summary", "Review the report findings.", "--render", "html,pdf,docx", "--bundle-id", "prose-b")
    prose = current(root)
    complete(prose, b)
    after_prose = facts(prose)
    assert all(after_prose[k] == report[k] for k in ("kpis", "rows", "charts", "notes"))
    assert after_prose["meta"]["inputs"] == report["meta"]["inputs"]
    activate("a")
    run("render", prose, "--to", "html,pdf,docx,xlsx", "--bundle-id", "restored-a")
    restored = current(root)
    complete(restored, a)
    assert facts(restored) == after_prose
    assert prefs.read_bytes() == current_prefs and retained(first.parent) == first_bytes
    exported = args.output / "single.pdf"
    before_export = retained(root)
    run("render", restored, "--to", "pdf", "--out", exported)
    assert_pdf(exported, product, "Example Report Subject", image_dir=inspection(exported))
    assert_tenant_pdf(exported, a, inspection(exported))
    assert retained(root) == before_export and current(root) == restored
    print("prose, A restoration, explicit PDF export and immutable old downloads passed", flush=True)

    only = activate("only")
    run("render", restored, "--to", "html,pdf,docx,xlsx", "--bundle-id", "product-only")
    current_only = current(root)
    complete(current_only, only)
    assert not (current_only.parent / "presentation/tenant.png").exists()
    two = activate("two")
    isolated = args.output / "isolated"
    run("build", source, "--period", "2026-08", "--company", "Example Report Subject", "--out", isolated, "--bundle-id", "a", "--render", "html,pdf,docx,xlsx")
    complete(current(isolated), two)
    default = activate("default-tenant", default=True)
    default_root = args.output / "default-product"
    run("build", source, "--period", "2026-08", "--company", "Example Report Subject", "--out", default_root, "--bundle-id", "a", "--render", "html,pdf,docx,xlsx")
    complete(current(default_root), default, json.loads((profiles / "default-product.json").read_text(encoding="utf-8")))
    print("product-only, isolated tenant and strict/default-product compatibility passed", flush=True)

    activate("only")
    preserved = retained(root)
    invalid = dict(env, HARTMESH_REPORT_TENANT_SHA256="0" * 64)
    failed = run("render", current_only, "--to", "html,pdf", "--bundle-id", "refused", selected_env=invalid, expected=1)
    assert "Rendered" not in failed.stdout
    assert retained(root) == preserved and current(root) == current_only
    assert sha(source) == source_hash
    assert {p.name: sha(p) for p in profiles.glob("*.json")} == fixture_hashes
    result = {
        "python": sys.version,
        "pillow": PIL.__version__,
        "weasyprint": weasyprint.__version__,
        "commands": commands,
        "revenue": str(total),
        "rows": len(rows),
        "required_cases_skipped": 0,
        "build_presentation": a["snapshot_id"],
        "revised_presentation": b["snapshot_id"],
        "input_sha256": source_hash,
    }
    (args.output / "qualification.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
