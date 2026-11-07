#!/usr/bin/env python3
"""Qualify real offline report commands with the sandbox's shipped libraries.

Mount skill/tests read-only and run this on the sandbox image with --network none.
This test harness may launch Python commands; production report code never does.
"""

from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import importlib.util
import io
import json
import os
import re
import subprocess
import sys
from decimal import Decimal
from pathlib import Path

import fitz
import openpyxl
import PIL
import weasyprint
from docx import Document
from PIL import Image


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def retained(root):
    return {p.relative_to(root).as_posix(): sha(p) for p in root.rglob("*") if p.is_file() and ".cache" not in p.relative_to(root).parts}


def current(root):
    pointer = root / ".cache/business-report" / f"{root.name}.current.json"
    return root / json.loads(pointer.read_text(encoding="utf-8"))["report"]


def assert_pdf(path, record, company, *, image_dir=None):
    document = fitz.open(path)
    text = " ".join(page.get_text() for page in document)
    assert company in text
    assert 2 <= len(document) <= 12, len(document)
    identity = record["identity"] if record else None
    images = set()
    links = []
    for page in document:
        for block in page.get_text("dict")["blocks"]:
            for line in block.get("lines", []):
                for span in line["spans"]:
                    x0, y0, x1, y1 = span["bbox"]
                    assert x0 >= 0 and y0 >= 0 and x1 <= page.rect.width + 1 and y1 <= page.rect.height + 1, (span["text"], span["bbox"])
        for entry in page.get_images(full=True):
            images.add(entry[0])
            for rect in page.get_image_rects(entry[0]):
                assert page.rect.contains(rect), rect
        links.extend(link.get("uri") for link in page.get_links())
    assert images, "The actual PDF must contain report charts"
    if identity:
        for fields in (identity["product"], identity["provider"]):
            assert fields["name"] in re.sub(r"\s+", " ", text)
            expected = Image.open(io.BytesIO(base64.b64decode(fields["logo"].split(",", 1)[1]))).convert("RGB")
            found = False
            for xref in images:
                actual = Image.open(io.BytesIO(document.extract_image(xref)["image"])).convert("RGB")
                if actual.size == expected.size and actual.tobytes() == expected.tobytes():
                    found = True
                    break
            assert found, f"Missing embedded logo pixels: {fields['name']}"
        assert identity["provider"]["support_url"] in re.sub(r"\s+", "", text)
        assert identity["provider"]["support_url"] in links
        assert "Product" in text and "Provider" in text
        # Retain actual rendered end matter for inspection, including pagination.
        destination = image_dir or path.parent
        destination.mkdir(parents=True, exist_ok=True)
        for index in range(max(0, len(document) - 2), len(document)):
            document[index].get_pixmap(matrix=fitz.Matrix(1.3, 1.3)).save(str(destination / f"attribution-page-{index + 1}.png"))
    else:
        assert "Example Provider" not in text
    return len(document)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--skill", required=True, type=Path)
    parser.add_argument("--fixtures", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    assert importlib.util.find_spec("deerflow") is None, "Qualification must not borrow Gateway packages"
    source = args.fixtures / "example_services_export_small.csv"
    before_input = sha(source)
    with source.open(encoding="utf-8", newline="") as stream:
        rows = [row for row in csv.DictReader(stream) if row["Completed On"].startswith("2026-08-")]
    revenue = sum((Decimal(row["Total (USD)"].replace("$", "").replace(",", "")) for row in rows), Decimal(0))
    profiles = args.fixtures / "product-identity"
    assert os.statvfs(profiles).f_flag & os.ST_RDONLY, "Offline projection must be mounted read-only"
    profile_hashes = {p.name: sha(p) for p in profiles.glob("*.json")}
    script = args.skill / "scripts/report.py"
    base_env = {k: v for k, v in os.environ.items() if not k.startswith("HARTMESH_") and k != "PYTHONPATH"}
    base_env["PYTHONDONTWRITEBYTECODE"] = "1"
    results = []
    for profile in (None, "one", "two"):
        label = profile or "default"
        root = args.output / label
        env = dict(base_env)
        record = None
        if profile:
            projection = profiles / f"{profile}.json"
            record = json.loads(projection.read_text(encoding="utf-8"))
            env.update(HARTMESH_REPORT_PRODUCT_IDENTITY=str(projection), HARTMESH_REPORT_PRODUCT_SHA256=record["sha256"])

        def run(*command, expected=0, selected_env=None):
            completed = subprocess.run([sys.executable, str(script), *map(str, command)], cwd=args.output, env=selected_env or env, capture_output=True, text=True, encoding="utf-8", timeout=120)
            with (args.output / f"{label}-commands.log").open("a", encoding="utf-8") as log:
                log.write(completed.stdout + completed.stderr)
            assert completed.returncode == expected, (completed.returncode, completed.stdout, completed.stderr)
            return completed

        run("build", source, "--period", "2026-08", "--company", "Example Report Subject", "--out", root, "--bundle-id", "initial", "--render", "html,pdf,docx,xlsx")
        first = current(root)
        report = json.loads(first.read_text(encoding="utf-8"))
        assert report["meta"]["company"] == "Example Report Subject"
        kpis = {k["id"]: k["value"] for k in report["kpis"]}
        assert Decimal(str(kpis["revenue"])) == revenue and kpis["jobs"] == len(rows)
        assert report["charts"]
        html = first.with_name(f"{label}.html").read_text(encoding="utf-8")
        assert "Example Report Subject" in html
        if record:
            assert record["identity"]["product"]["name"] in html and record["identity"]["provider"]["name"] in html
            assert record["identity"]["provider"]["support_url"] in html
            assert json.loads((first.parent / "product-identity.json").read_text(encoding="utf-8")) == record
        else:
            assert 'id="shared-attribution"' not in html
        pages = assert_pdf(first.with_name(f"{label}.pdf"), record, "Example Report Subject")
        word = " ".join(p.text for p in Document(first.with_name(f"{label}.docx")).paragraphs)
        assert "Example Report Subject" in word and "Example Provider" not in word
        workbook = openpyxl.load_workbook(first.with_name(f"{label}.xlsx"), data_only=True)
        assert Decimal(str(workbook["Summary"]["B2"].value)) == revenue
        view = first.with_name(f"{label}.view.json").read_text(encoding="utf-8")
        assert "Example Provider" not in view
        before = retained(first.parent)
        run("prose", first, "--summary", "Review the report findings.", "--bundle-id", "prose", "--render", "html,pdf")
        second = current(root)
        updated = json.loads(second.read_text(encoding="utf-8"))
        assert updated["kpis"] == report["kpis"] and updated["rows"] == report["rows"]
        run("render", second, "--to", "html,pdf", "--bundle-id", "rendered")
        third = current(root)
        exported = args.output / f"{label}-export.pdf"
        run("render", third, "--to", "pdf", "--out", exported)
        assert_pdf(exported, record, "Example Report Subject")
        assert retained(first.parent) == before
        before_all = retained(root)
        invalid = dict(env, HARTMESH_REPORT_PRODUCT_IDENTITY="/missing/identity.json", HARTMESH_REPORT_PRODUCT_SHA256="0" * 64)
        failed = run("render", third, "--to", "html,pdf", "--bundle-id", "refused", expected=1, selected_env=invalid)
        assert "Rendered" not in failed.stdout
        assert retained(root) == before_all and current(root) == third
        results.append({"profile": label, "revenue": str(revenue), "rows": len(rows), "pages": pages, "snapshot": record["sha256"] if record else None})
        print(f"{label}: real build/prose/render/export HTML/PDF, DOCX/XLSX/view compatibility and failure/retention passed", flush=True)
    assert sha(source) == before_input
    assert {p.name: sha(p) for p in profiles.glob("*.json")} == profile_hashes
    result = {"python": sys.version, "pillow": PIL.__version__, "weasyprint": weasyprint.__version__, "cases": results, "input_sha256": before_input, "projection_sha256": profile_hashes}
    (args.output / "qualification.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
