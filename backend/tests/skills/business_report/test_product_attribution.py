"""Shared identity is offline, complete and independent of report business context."""

from __future__ import annotations

import base64
import copy
import hashlib
import importlib.util
import io
import json
import sys
from pathlib import Path

import pytest

# Optional in default backend installs; the dedicated skill job asserts these
# dependencies and the real-image qualification requires every HTML/PDF case.
for _library in ("pandas", "docx", "pypdf", "jinja2", "matplotlib", "weasyprint"):
    pytest.importorskip(_library)

from PIL import Image  # noqa: E402
from pypdf import PdfReader  # noqa: E402

ROOT = Path(__file__).resolve().parents[4]
SCRIPTS = ROOT / "skills/public/business-report/scripts"
FIXTURE = Path(__file__).parent / "fixtures/example_services_export_small.csv"


def load_script(name):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    previous = sys.dont_write_bytecode
    sys.dont_write_bytecode = True
    try:
        spec.loader.exec_module(module)
    finally:
        sys.dont_write_bytecode = previous
    return module


@pytest.fixture
def reader(monkeypatch):
    monkeypatch.syspath_prepend(str(SCRIPTS))
    for key in ("HARTMESH_REPORT_PRODUCT_IDENTITY", "HARTMESH_REPORT_PRODUCT_SHA256", "HARTMESH_PRODUCT_PAYLOAD"):
        monkeypatch.delenv(key, raising=False)
    return load_script("business_report_product")


def png(color, size=(64, 64)):
    out = io.BytesIO()
    Image.new("RGBA", size, color).save(out, format="PNG")
    return out.getvalue()


def uri(data):
    return "data:image/png;base64," + base64.b64encode(data).decode("ascii")


def identity(profile="One"):
    return {
        "schema_version": 1,
        "mode": "configured",
        "name": f"Example Product {profile}",
        "product": {"name": f"Example Product {profile}", "logo": uri(png("navy" if profile == "One" else "teal")), "browser_icon": uri(png("navy" if profile == "One" else "teal"))},
        "provider": {"name": f"Example Provider {profile}", "logo": uri(png("maroon" if profile == "One" else "purple")), "support_url": f"https://support.example.test/{profile.lower()}"},
    }


def projection(value):
    digest = hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode("ascii")).hexdigest()
    return {"schema_version": 1, "sha256": digest, "identity": value}


def configure(tmp_path, monkeypatch, value=None):
    record = projection(value or identity())
    path = tmp_path / "product.json"
    path.write_text(json.dumps(record), encoding="utf-8")
    monkeypatch.setenv("HARTMESH_REPORT_PRODUCT_IDENTITY", str(path))
    monkeypatch.setenv("HARTMESH_REPORT_PRODUCT_SHA256", record["sha256"])
    return path, record


def test_default_is_legacy_but_partial_configuration_never_defaults(reader, monkeypatch):
    assert reader.load_product_identity() is None
    for key in ("HARTMESH_REPORT_PRODUCT_IDENTITY", "HARTMESH_REPORT_PRODUCT_SHA256", "HARTMESH_PRODUCT_PAYLOAD"):
        monkeypatch.setenv(key, "/missing")
        with pytest.raises(ValueError, match="identity"):
            reader.load_product_identity()
        monkeypatch.delenv(key)


@pytest.mark.parametrize("profile", ["One", "Two"])
def test_validated_snapshot_keeps_complete_bytes_after_source_disappears(reader, tmp_path, monkeypatch, profile):
    path, record = configure(tmp_path, monkeypatch, identity(profile))
    snapshot = reader.load_product_identity()
    path.unlink()
    assert snapshot.product_name == f"Example Product {profile}"
    assert snapshot.provider_name == f"Example Provider {profile}"
    assert snapshot.product_logo == record["identity"]["product"]["logo"]
    assert snapshot.provider_logo == record["identity"]["provider"]["logo"]
    assert json.loads(snapshot.record) == record
    with pytest.raises(ValueError, match="identity"):
        reader.load_product_identity()


@pytest.mark.parametrize(
    "mutation",
    [
        lambda v: v.update(schema_version=True),
        lambda v: v.update(tenant="unapproved"),
        lambda v: v.update(mode="default"),
        lambda v: v.update(name="Different"),
        lambda v: v["product"].update(name="X" * 41),
        lambda v: v["product"].update(name="Bad\u200dName"),
        lambda v: v["product"].update(name="Bad\nName"),
        lambda v: v["product"].update(logo="../logo.png"),
        lambda v: v["provider"].update(logo="https://example.test/logo.png"),
        lambda v: v["provider"].update(support_url="javascript:alert(1)"),
        lambda v: v["provider"].update(support_url="https://user:secret@example.test"),
        lambda v: v["provider"].update(support_url="https://ex%61mple.test"),
        lambda v: v["provider"].update(support_url="https://example.test/\\x"),
        lambda v: v["provider"].update(support_url="https://example.test/\u200b"),
        lambda v: v["provider"].update(extra="unapproved"),
        lambda v: v.pop("provider"),
        lambda v: v["product"].update(logo=uri(b"\x89PNG\r\n\x1a\nnot-an-image")),
        lambda v: v["product"].update(logo=uri(png("red") + b"<script>")),
        lambda v: v["product"].update(logo=uri(png("red", (1025, 1)))),
        lambda v: v["product"].update(browser_icon=uri(png("red", (31, 31)))),
    ],
    ids=[
        "bool-version",
        "unknown",
        "default-projection",
        "alias-conflict",
        "long-name",
        "format-control",
        "newline",
        "traversal",
        "remote-logo",
        "active-link",
        "userinfo",
        "encoded-host",
        "backslash",
        "url-control",
        "unknown-provider",
        "incomplete",
        "fake-png",
        "trailing",
        "large-image",
        "small-icon",
    ],
)
def test_projection_rejects_even_correctly_rehashed_unsafe_input(reader, tmp_path, monkeypatch, mutation):
    value = identity()
    mutation(value)
    configure(tmp_path, monkeypatch, value)
    with pytest.raises(ValueError, match="identity"):
        reader.load_product_identity()


def test_projection_checks_both_binding_and_operator_expected_digest(reader, tmp_path, monkeypatch):
    path, record = configure(tmp_path, monkeypatch)
    record["identity"]["provider"]["name"] = "Changed"
    path.write_text(json.dumps(record), encoding="utf-8")
    with pytest.raises(ValueError, match="identity"):
        reader.load_product_identity()
    configure(tmp_path, monkeypatch)
    monkeypatch.setenv("HARTMESH_REPORT_PRODUCT_SHA256", "0" * 64)
    with pytest.raises(ValueError, match="identity"):
        reader.load_product_identity()


def test_projection_rejects_duplicate_keys_oversize_and_links(reader, tmp_path, monkeypatch):
    path, record = configure(tmp_path, monkeypatch)
    path.write_text(json.dumps(record).replace('"schema_version": 1', '"schema_version": 1, "schema_version": 1', 1), encoding="utf-8")
    with pytest.raises(ValueError):
        reader.load_product_identity()
    path.write_bytes(b" " * (2 * 1024 * 1024 + 1))
    with pytest.raises(ValueError):
        reader.load_product_identity()
    configure(tmp_path, monkeypatch)
    linked = tmp_path / "linked.json"
    linked.symlink_to(path)
    monkeypatch.setenv("HARTMESH_REPORT_PRODUCT_IDENTITY", str(linked))
    with pytest.raises(ValueError):
        reader.load_product_identity()


def report_module(monkeypatch):
    monkeypatch.syspath_prepend(str(SCRIPTS))
    return load_script("report")


def current(root):
    return root / json.loads(next((root / ".cache/business-report").glob("*.current.json")).read_text(encoding="utf-8"))["report"]


def public_bytes(root):
    return {str(p.relative_to(root)): p.read_bytes() for p in root.rglob("*") if p.is_file() and ".cache" not in p.relative_to(root).parts}


def test_real_html_pdf_keep_business_context_and_capture_once(reader, tmp_path, monkeypatch):
    path, record = configure(tmp_path, monkeypatch)
    report = report_module(monkeypatch)
    root = tmp_path / "out"
    original = report.render
    calls = []

    def remove_after_first(document, report_path, target, out_path, tenant, **kwargs):
        calls.append(kwargs["product_identity"])
        result = original(document, report_path, target, out_path, tenant, **kwargs)
        path.unlink(missing_ok=True)
        return result

    monkeypatch.setattr(report, "render", remove_after_first)
    assert report.main(["build", str(FIXTURE), "--period", "2026-08", "--company", "Example Report Subject", "--out", str(root), "--bundle-id", "initial", "--render", "html,pdf"]) == 0
    assert len(calls) == 2 and calls[0] is calls[1]
    source = current(root)
    html = source.with_name("out.html").read_text(encoding="utf-8")
    pdf = PdfReader(source.with_name("out.pdf"))
    text = "\n".join(page.extract_text() for page in pdf.pages)
    for expected in ("Example Report Subject", "Example Product One", "Example Provider One", "https://support.example.test/one", "Product", "Provider"):
        assert expected in html and expected in text
    assert len([image for page in pdf.pages for image in page.images]) >= 2
    assert json.loads((source.parent / "product-identity.json").read_text(encoding="utf-8")) == record
    manifest = json.loads((source.parent / "renders.json").read_text(encoding="utf-8"))
    assert "product-identity.json" in manifest["sha256"]
    assert json.loads(source.read_text(encoding="utf-8"))["meta"]["company"] == "Example Report Subject"
    before = public_bytes(root)
    assert report.main(["render", str(source), "--to", "html,pdf", "--bundle-id", "invalid"]) == 1
    assert public_bytes(root) == before and current(root) == source


@pytest.mark.parametrize("command", ["build", "prose", "render", "export"])
def test_invalid_identity_preserves_retained_output_and_current(reader, tmp_path, monkeypatch, command):
    report = report_module(monkeypatch)
    root = tmp_path / "out"
    assert report.main(["build", str(FIXTURE), "--period", "2026-08", "--out", str(root), "--render", "html,pdf"]) == 0
    source = current(root)
    before = public_bytes(root)
    configure(tmp_path, monkeypatch)
    monkeypatch.setenv("HARTMESH_REPORT_PRODUCT_SHA256", "0" * 64)
    args = {
        "build": ["build", str(FIXTURE), "--period", "2026-08", "--out", str(root), "--render", "html,pdf"],
        "prose": ["prose", str(source), "--summary", "Business context.", "--render", "html,pdf"],
        "render": ["render", str(source), "--to", "html,pdf"],
        "export": ["render", str(source), "--to", "pdf", "--out", str(tmp_path / "export.pdf")],
    }
    assert report.main(args[command]) == 1
    assert public_bytes(root) == before and current(root) == source
    assert not (tmp_path / "export.pdf").exists()


def test_default_and_other_formats_keep_legacy_behavior(reader, tmp_path, monkeypatch):
    import openpyxl
    from docx import Document

    report = report_module(monkeypatch)
    root = tmp_path / "out"
    assert report.main(["build", str(FIXTURE), "--period", "2026-08", "--out", str(root), "--render", "html,docx,xlsx"]) == 0
    source = current(root)
    assert 'id="shared-attribution"' not in source.with_name("out.html").read_text(encoding="utf-8")
    assert not (source.parent / "product-identity.json").exists()
    before = copy.deepcopy(json.loads(source.read_text(encoding="utf-8")))
    configure(tmp_path, monkeypatch, identity("Two"))
    assert report.main(["render", str(source), "--to", "html,docx,xlsx"]) == 0
    revised = current(root)
    assert json.loads(revised.read_text(encoding="utf-8")) == before
    assert "Example Provider Two" not in " ".join(p.text for p in Document(revised.with_name("out.docx")).paragraphs)
    workbook = openpyxl.load_workbook(revised.with_name("out.xlsx"), data_only=True)
    assert workbook["Summary"]["B2"].value == before["kpis"][0]["value"]
    view = revised.with_name("out.view.json").read_text(encoding="utf-8")
    assert "Example Provider Two" not in view


@pytest.mark.parametrize("profile", ["one", "two"])
def test_image_fixtures_are_exact_gateway_exports_and_accepted_offline(reader, tmp_path, monkeypatch, profile):
    from types import SimpleNamespace

    from deerflow.config.product_identity import mounted_identity
    from deerflow.config.product_identity_export import offline_product_identity
    from deerflow.config.ui_config import UiConfig

    record = json.loads((FIXTURE.parent / "product-identity" / f"{profile}.json").read_text(encoding="utf-8"))
    projected = record["identity"]
    assets = tmp_path / "payload" / "assets"
    assets.mkdir(parents=True)
    product = dict(projected["product"])
    provider = dict(projected["provider"])
    for fields, key, name in [(product, "logo", "product"), (product, "browser_icon", "icon"), (provider, "logo", "provider")]:
        (assets / f"{name}.png").write_bytes(base64.b64decode(fields[key].split(",", 1)[1]))
        fields[key] = f"assets/{name}.png"
    (assets.parent / "product.json").write_text(json.dumps({"schema_version": 1, "public": True, "product": product, "provider": provider}), encoding="utf-8")
    monkeypatch.setenv("HARTMESH_PRODUCT_PAYLOAD", str(assets.parent))
    mounted_identity.cache_clear()
    try:
        exported = offline_product_identity(SimpleNamespace(ui=UiConfig(), tenant_bundle=SimpleNamespace(path=None)))
        assert exported == record
    finally:
        mounted_identity.cache_clear()
    configure(tmp_path, monkeypatch, record["identity"])
    assert reader.load_product_identity().digest == exported["sha256"]


def test_attribution_display_text_is_escaped_and_tenant_preferences_cannot_override(reader, tmp_path, monkeypatch):
    report = report_module(monkeypatch)
    value = identity()
    value["name"] = value["product"]["name"] = '<script src="x">Example</script>'
    configure(tmp_path, monkeypatch, value)
    tenant = tmp_path / "tenant"
    tenant.mkdir()
    (tenant / "brand.json").write_text(json.dumps({"company_name": "Unrelated Tenant"}), encoding="utf-8")
    prefs = tmp_path / "preferences.json"
    prefs.write_text(json.dumps({"version": 1, "brand": {"company": "Preference Company"}}), encoding="utf-8")
    root = tmp_path / "out"
    command = ["build", str(FIXTURE), "--period", "2026-08", "--company", "Business Company", "--tenant", str(tenant), "--prefs", str(prefs), "--out", str(root), "--render", "html"]
    assert report.main(command) == 1  # Unknown preference identity fields refuse.
    prefs.write_text(json.dumps({"version": 1, "brand": {"primary": "#123456"}}), encoding="utf-8")
    assert report.main(command) == 0
    html = current(root).with_name("out.html").read_text(encoding="utf-8")
    assert '<script src="x">' not in html
    assert "&lt;script src=&#34;x&#34;&gt;" in html
    assert "Business Company" in html and "Example Provider One" in html


def test_changed_shared_identity_never_carries_an_old_branded_format(reader, tmp_path, monkeypatch):
    report = report_module(monkeypatch)
    configure(tmp_path, monkeypatch)
    root = tmp_path / "out"
    assert report.main(["build", str(FIXTURE), "--period", "2026-08", "--out", str(root), "--render", "html,pdf,docx,xlsx"]) == 0
    old = current(root)
    before = public_bytes(old.parent)
    configure(tmp_path, monkeypatch, identity("Two"))
    assert report.main(["render", str(old), "--to", "html"]) == 0
    revised = current(root)
    files = json.loads((revised.parent / "renders.json").read_text(encoding="utf-8"))["files"]
    assert files == ["out.docx", "out.html", "out.xlsx"]
    assert not revised.with_name("out.pdf").exists()
    assert "Example Provider Two" in revised.with_name("out.html").read_text(encoding="utf-8")
    assert public_bytes(old.parent) == before


@pytest.mark.parametrize("interrupted", [False, True])
def test_configured_second_format_failure_preserves_earlier_bundle(reader, tmp_path, monkeypatch, interrupted):
    report = report_module(monkeypatch)
    configure(tmp_path, monkeypatch)
    root = tmp_path / "out"
    assert report.main(["build", str(FIXTURE), "--period", "2026-08", "--out", str(root), "--render", "html,pdf"]) == 0
    source = current(root)
    before = public_bytes(root)
    original = report.render

    def fail(document, report_path, target, out_path, tenant, **kwargs):
        if target == "html":
            assert report_path.with_name("out.pdf").exists()
            if interrupted:
                raise KeyboardInterrupt
            raise RuntimeError("controlled second-format failure")
        return original(document, report_path, target, out_path, tenant, **kwargs)

    monkeypatch.setattr(report, "render", fail)
    with pytest.raises(KeyboardInterrupt if interrupted else RuntimeError):
        report.main(["render", str(source), "--to", "html,pdf"])
    assert public_bytes(root) == before and current(root) == source


@pytest.mark.parametrize("source_change", ["remove", "replace"])
def test_prose_and_explicit_export_capture_before_input_replacement(reader, tmp_path, monkeypatch, source_change):
    report = report_module(monkeypatch)
    path, _ = configure(tmp_path, monkeypatch)
    root = tmp_path / "out"
    assert report.main(["build", str(FIXTURE), "--period", "2026-08", "--out", str(root)]) == 0
    source = current(root)
    original = report.render
    observed = []

    def replace_input(document, report_path, target, out_path, tenant, **kwargs):
        observed.append(kwargs["product_identity"])
        if source_change == "remove":
            path.unlink(missing_ok=True)
        else:
            # Neither metadata nor image fields from a new complete source may
            # enter this already admitted operation, even at the same filename.
            path.write_text(json.dumps(projection(identity("Two"))), encoding="utf-8")
        return original(document, report_path, target, out_path, tenant, **kwargs)

    monkeypatch.setattr(report, "render", replace_input)
    assert report.main(["prose", str(source), "--summary", "Review the figures.", "--render", "html,pdf"]) == 0
    revised = current(root)
    html = revised.with_name("out.html").read_text(encoding="utf-8")
    assert "Example Provider One" in html and "Example Provider Two" not in html
    assert len(observed) == 2 and observed[0] is observed[1]
    configure(tmp_path, monkeypatch)
    exported = tmp_path / "single.html"
    assert report.main(["render", str(revised), "--to", "html", "--out", str(exported)]) == 0
    assert "Example Provider One" in exported.read_text(encoding="utf-8")


def test_projection_rejects_hardlinks_ancestor_links_ancillary_data_and_asset_byte_limits(reader, tmp_path, monkeypatch):
    import os
    import random

    from PIL.PngImagePlugin import PngInfo

    path, _ = configure(tmp_path, monkeypatch)
    os.link(path, tmp_path / "hardlinked.json")
    with pytest.raises(ValueError):
        reader.load_product_identity()
    (tmp_path / "hardlinked.json").unlink()
    parent_link = tmp_path / "linked-parent"
    parent_link.symlink_to(tmp_path, target_is_directory=True)
    monkeypatch.setenv("HARTMESH_REPORT_PRODUCT_IDENTITY", str(parent_link / path.name))
    with pytest.raises(ValueError):
        reader.load_product_identity()
    metadata = PngInfo()
    metadata.add_text("unapproved", "<script>active</script>")
    out = io.BytesIO()
    Image.new("RGBA", (64, 64), "navy").save(out, format="PNG", pnginfo=metadata)
    value = identity()
    value["product"]["logo"] = uri(out.getvalue())
    configure(tmp_path, monkeypatch, value)
    with pytest.raises(ValueError):
        reader.load_product_identity()
    out = io.BytesIO()
    Image.frombytes("RGBA", (300, 300), random.Random(1).randbytes(300 * 300 * 4)).save(out, format="PNG")
    assert len(out.getvalue()) > 256 * 1024
    value["product"]["logo"] = uri(out.getvalue())
    configure(tmp_path, monkeypatch, value)
    with pytest.raises(ValueError):
        reader.load_product_identity()


def test_carried_html_cannot_use_a_projection_changed_after_manifest_verification(reader, tmp_path, monkeypatch):
    report = report_module(monkeypatch)
    import business_report_publish as publication
    from business_report_common import InputError

    configure(tmp_path, monkeypatch)
    root = tmp_path / "out"
    assert report.main(["build", str(FIXTURE), "--period", "2026-08", "--out", str(root), "--render", "html"]) == 0
    source = current(root)
    document = json.loads(source.read_text(encoding="utf-8"))
    old_record = source.parent / "product-identity.json"
    original_bytes = old_record.read_bytes()
    before = public_bytes(root)
    expected = publication.current_identity(root, "out")
    configure(tmp_path, monkeypatch, identity("Two"))
    selected = reader.load_product_identity()
    manifest = publication.bundle_manifest(source)
    charts = {chart["png"]: (source.parent / chart["png"]).read_bytes() for chart in document["charts"]}

    def prepare(stage):
        for name, content in charts.items():
            destination = stage / name
            destination.parent.mkdir(exist_ok=True)
            destination.write_bytes(content)

    changed = False
    import business_report_presentation as presentation

    real_read = presentation._read

    def race(path, *args):
        nonlocal changed
        if Path(path) == old_record and not changed:
            changed = True
            old_record.write_bytes(selected.record)
            captured = real_read(path, *args)
            old_record.write_bytes(original_bytes)
            return captured
        return real_read(path, *args)

    # Freeze the already verified manifest, then change/restore bytes exactly at
    # the subsequent compatibility read. The final current check alone cannot
    # detect this race: its original bytes have already been restored.
    monkeypatch.setattr(publication, "bundle_manifest", lambda *args, **kwargs: manifest)
    monkeypatch.setattr(presentation, "_read", race)
    with pytest.raises(InputError, match="shared identity changed"):
        publication.publish_bundle(
            root,
            "out",
            document,
            [],
            None,
            report.render,
            prepare,
            bundle_id="raced",
            expected_current=expected,
            source=source,
            carry=["out.html"],
            product_identity=selected,
            presentation=presentation.capture_presentation(None, product_identity=selected),
        )
    assert changed and public_bytes(root) == before
