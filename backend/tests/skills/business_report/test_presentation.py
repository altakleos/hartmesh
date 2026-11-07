"""Complete offline presentation capture and retained bundle integrity."""

from __future__ import annotations

import base64
import copy
import hashlib
import importlib
import io
import json
import sys
from pathlib import Path

import pytest
from PIL import Image

ROOT = Path(__file__).resolve().parents[4]
SCRIPTS = ROOT / "skills/public/business-report/scripts"
PRODUCT = Path(__file__).parent / "fixtures/product-identity/one.json"


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode("ascii")


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def png(color="red", size=(64, 64)):
    stream = io.BytesIO()
    Image.new("RGBA", size, color).save(stream, format="PNG")
    return stream.getvalue()


def uri(raw):
    return "data:image/png;base64," + base64.b64encode(raw).decode("ascii")


@pytest.fixture
def modules(monkeypatch):
    monkeypatch.syspath_prepend(str(SCRIPTS))
    for key in ("HARTMESH_REPORT_PRODUCT_IDENTITY", "HARTMESH_REPORT_PRODUCT_SHA256", "HARTMESH_PRODUCT_PAYLOAD", "HARTMESH_REPORT_TENANT_PRESENTATION", "HARTMESH_REPORT_TENANT_SHA256"):
        monkeypatch.delenv(key, raising=False)
    previous = sys.dont_write_bytecode
    sys.dont_write_bytecode = True
    try:
        yield
    finally:
        sys.dont_write_bytecode = previous


def selected(tmp_path, monkeypatch, *, mode="tenant", name="Example Tenant One", raw=None, product=None):
    product = product or json.loads(PRODUCT.read_text(encoding="utf-8"))
    path = tmp_path / "product.json"
    path.write_bytes(canonical(product))
    monkeypatch.setenv("HARTMESH_REPORT_PRODUCT_IDENTITY", str(path))
    monkeypatch.setenv("HARTMESH_REPORT_PRODUCT_SHA256", product["sha256"])
    body = {"schema_version": 1, "mode": mode, "product_sha256": product["sha256"], "tenant": {"name": name, "logo": uri(raw or png())} if mode == "tenant" else None}
    body["snapshot_id"] = digest(body)
    tenant = tmp_path / "tenant.json"
    tenant.write_bytes(canonical(body))
    monkeypatch.setenv("HARTMESH_REPORT_TENANT_PRESENTATION", str(tenant))
    monkeypatch.setenv("HARTMESH_REPORT_TENANT_SHA256", body["snapshot_id"])
    return tenant, body


def test_current_legacy_selection_never_overlays_stored_company_with_live_assets(modules, tmp_path):
    pytest.importorskip("docx")
    render = importlib.import_module("business_report_render")
    tenant = tmp_path / "tenant"
    tenant.mkdir()
    (tenant / "logo.png").write_bytes(png("blue"))
    (tenant / "brand.json").write_text(json.dumps({"company_name": "Example Tenant Revised", "logo": "logo.png", "colors": {"primary": "#123456"}}), encoding="utf-8")
    report = {"meta": {"company": "Example Report Subject", "brand": {"company": "Example Tenant Original", "primary": "#654321"}}}
    original = copy.deepcopy(report)
    brand = render.effective_brand(report, tmp_path / "report.json", str(tenant))
    assert brand["company"] == "Example Tenant Revised"
    assert brand["primary"] == "#123456"
    assert brand["logo_bytes"] == png("blue")
    assert report == original


@pytest.mark.parametrize("mode", ["tenant", "product-only"])
def test_strict_capture_owns_one_complete_projection_after_source_loss(modules, tmp_path, monkeypatch, mode):
    reader = importlib.import_module("business_report_presentation")
    path, body = selected(tmp_path, monkeypatch, mode=mode)
    snapshot = reader.capture_presentation(None)
    path.unlink()
    (tmp_path / "product.json").unlink()
    assert snapshot.mode == mode
    assert snapshot.source_id == body["snapshot_id"]
    assert snapshot.product_identity.digest == body["product_sha256"]
    assert snapshot.brand()["company"] == ("Example Tenant One" if mode == "tenant" else "")
    assert snapshot.brand()["logo_bytes"] == (png() if mode == "tenant" else None)
    with pytest.raises(ValueError, match="presentation|identity"):
        reader.capture_presentation(None)


@pytest.mark.parametrize(
    "mutation",
    [
        lambda p: p.update(schema_version=True),
        lambda p: p.update(schema_version=2),
        lambda p: p.update(extra="unsupported"),
        lambda p: p.update(mode="legacy"),
        lambda p: p.update(tenant=None),
        lambda p: p["tenant"].update(name="X" * 81),
        lambda p: p["tenant"].update(name="Bad\u200dName"),
        lambda p: p["tenant"].update(name=" Bad"),
        lambda p: p["tenant"].update(logo="../logo.png"),
        lambda p: p["tenant"].update(logo=uri(png(size=(1025, 1)))),
        lambda p: p["tenant"].update(logo=uri(png() + b"extra")),
        lambda p: p["tenant"].update(primary="#123456"),
        lambda p: p.update(product_sha256="0" * 64),
    ],
    ids=["bool-version", "version", "unknown", "implicit-mode", "incomplete", "long-name", "control", "trimmed-name", "path", "dimensions", "trailing", "palette-conflict", "product-binding"],
)
def test_even_correctly_rehashed_unsafe_projections_refuse(modules, tmp_path, monkeypatch, mutation):
    reader = importlib.import_module("business_report_presentation")
    path, body = selected(tmp_path, monkeypatch)
    body.pop("snapshot_id")
    mutation(body)
    body["snapshot_id"] = digest(body)
    path.write_bytes(canonical(body))
    monkeypatch.setenv("HARTMESH_REPORT_TENANT_SHA256", body["snapshot_id"])
    with pytest.raises(ValueError, match="presentation"):
        reader.capture_presentation(None)


@pytest.mark.parametrize("case", ["stale", "missing", "duplicate", "oversize", "symlink", "hardlink", "partial", "missing-product"])
def test_unavailable_and_unbound_current_inputs_never_become_defaults(modules, tmp_path, monkeypatch, case):
    reader = importlib.import_module("business_report_presentation")
    path, body = selected(tmp_path, monkeypatch)
    if case == "stale":
        monkeypatch.setenv("HARTMESH_REPORT_TENANT_SHA256", "0" * 64)
    elif case == "missing":
        path.unlink()
    elif case == "duplicate":
        path.write_bytes(canonical(body).replace(b'"schema_version":1', b'"schema_version":1,"schema_version":1'))
    elif case == "oversize":
        path.write_bytes(b" " * (384 * 1024 + 1))
    elif case == "symlink":
        linked = tmp_path / "link.json"
        linked.symlink_to(path)
        monkeypatch.setenv("HARTMESH_REPORT_TENANT_PRESENTATION", str(linked))
    elif case == "hardlink":
        import os

        os.link(path, tmp_path / "link.json")
    elif case == "partial":
        monkeypatch.delenv("HARTMESH_REPORT_TENANT_SHA256")
    elif case == "missing-product":
        monkeypatch.delenv("HARTMESH_REPORT_PRODUCT_IDENTITY")
        monkeypatch.delenv("HARTMESH_REPORT_PRODUCT_SHA256")
    with pytest.raises(ValueError, match="presentation|identity"):
        reader.capture_presentation(None)


def test_effective_preferences_are_complete_bounded_and_historical(modules, tmp_path, monkeypatch):
    reader = importlib.import_module("business_report_presentation")
    selected(tmp_path, monkeypatch)
    original = reader.capture_presentation(None)
    chosen = original.with_preferences({"version": 1, "brand": {"primary": "#0a6b3d", "secondary": "#9ccdb4"}})
    assert chosen.brand()["primary"] == "#0a6b3d"
    assert original.brand()["primary"] == "#1F4E79"
    assert chosen.digest != original.digest
    for brand in ({"primary": "#ffffff"}, {"tertiary": "#123456"}, {"primary": "red"}, {"company": "Override"}):
        with pytest.raises(ValueError, match="palette|preferences"):
            original.with_preferences({"version": 1, "brand": brand})


def test_retained_snapshot_uses_hashed_relative_assets_and_refuses_tampering(modules, tmp_path, monkeypatch):
    reader = importlib.import_module("business_report_presentation")
    selected(tmp_path, monkeypatch, name="L" * 80, raw=png(size=(1024, 1)))
    snapshot = reader.capture_presentation(None)
    stage = tmp_path / "bundle"
    stage.mkdir()
    snapshot.write(stage)
    (stage / "product-identity.json").write_bytes(snapshot.product_identity.record)
    members = {p.relative_to(stage).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest() for p in stage.rglob("*") if p.is_file()}
    record = reader.validate_retained(stage, members)
    assert record["snapshot_id"] == snapshot.digest
    assert record["tenant"]["logo"] == "presentation/tenant.png"
    (stage / "presentation/tenant.png").write_bytes(png("blue"))
    with pytest.raises(ValueError, match="presentation"):
        reader.validate_retained(stage, members)


def test_strict_default_product_binding_never_invents_provider_attribution(modules, tmp_path, monkeypatch):
    reader = importlib.import_module("business_report_presentation")
    identity = {"schema_version": 1, "mode": "default", "name": "Example Shared Product", "product": {"name": "Example Shared Product", "logo": None, "browser_icon": None}, "provider": None}
    selected(tmp_path, monkeypatch, product={"schema_version": 1, "identity": identity, "sha256": digest(identity)})
    snapshot = reader.capture_presentation(None)
    assert snapshot.product_identity.digest == digest(identity)
    assert snapshot.product_identity.configured is False
    assert snapshot.product_identity.provider_name == ""
    assert snapshot.brand()["company"] == "Example Tenant One"


@pytest.mark.parametrize("profile", ["a", "b", "two", "only", "default-tenant"])
def test_static_offline_fixtures_are_exact_current_gateway_projections(modules, tmp_path, profile):
    from deerflow.config.tenant_presentation import capture_presentation, content_digest

    fixture = Path(__file__).parent / "fixtures/tenant-presentation" / f"{profile}.json"
    expected = json.loads(fixture.read_text(encoding="utf-8"))
    product_path = fixture.with_name("default-product.json") if profile == "default-tenant" else PRODUCT
    product = json.loads(product_path.read_text(encoding="utf-8"))["identity"]
    manifest = {"schema_version": 1, "mode": expected["mode"], "product_sha256": expected["product_sha256"], "tenant": None}
    if expected["tenant"]:
        raw = base64.b64decode(expected["tenant"]["logo"].split(",", 1)[1])
        (tmp_path / "logo.png").write_bytes(raw)
        manifest["tenant"] = {"name": expected["tenant"]["name"], "logo": {"path": "logo.png", "sha256": hashlib.sha256(raw).hexdigest()}}
    (tmp_path / "presentation.json").write_bytes(canonical(manifest))
    assert capture_presentation(str(tmp_path), content_digest(manifest), product).projection() == expected


def test_admitted_legacy_name_remains_readable_from_its_new_retained_bundle(modules, tmp_path):
    reader = importlib.import_module("business_report_presentation")
    tenant = tmp_path / "tenant"
    tenant.mkdir()
    (tenant / "brand.json").write_text(json.dumps({"company_name": "Example\u200dTenant"}), encoding="utf-8")
    snapshot = reader.capture_presentation(str(tenant))
    stage = tmp_path / "bundle"
    stage.mkdir()
    snapshot.write(stage)
    members = {"presentation.json": hashlib.sha256((stage / "presentation.json").read_bytes()).hexdigest()}
    assert reader.validate_retained(stage, members)["tenant"]["name"] == "Example\u200dTenant"


@pytest.mark.parametrize("case", ["version", "path", "missing", "binding"])
def test_retained_semantics_refuse_even_when_outer_hashes_are_updated(modules, tmp_path, monkeypatch, case):
    reader = importlib.import_module("business_report_presentation")
    selected(tmp_path, monkeypatch)
    snapshot = reader.capture_presentation(None)
    stage = tmp_path / "bundle"
    stage.mkdir()
    snapshot.write(stage)
    (stage / "product-identity.json").write_bytes(snapshot.product_identity.record)
    record = json.loads((stage / "presentation.json").read_text(encoding="utf-8"))
    if case == "version":
        record["schema_version"] = 2
    elif case == "path":
        record["tenant"]["logo"] = "../outside.png"
    elif case == "missing":
        (stage / "presentation/tenant.png").unlink()
    elif case == "binding":
        record["source_id"] = "0" * 64
    record.pop("snapshot_id")
    record["snapshot_id"] = digest(record)
    (stage / "presentation.json").write_bytes(canonical(record))
    members = {p.relative_to(stage).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest() for p in stage.rglob("*") if p.is_file()}
    with pytest.raises(ValueError, match="presentation"):
        reader.validate_retained(stage, members)


def test_legacy_malformed_color_collection_degrades_before_freezing(modules, tmp_path):
    reader = importlib.import_module("business_report_presentation")
    tenant = tmp_path / "tenant"
    tenant.mkdir()
    (tenant / "brand.json").write_text(json.dumps({"company_name": "Example Tenant One", "colors": ["unsupported"]}), encoding="utf-8")
    snapshot = reader.capture_presentation(str(tenant))
    assert snapshot.tenant_name == "Example Tenant One"
    assert snapshot.primary == "#1F4E79"
