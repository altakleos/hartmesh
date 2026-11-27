"""Complete offline presentation capture and retained bundle integrity."""

from __future__ import annotations

import copy
import hashlib
import importlib
import io
import json
import shutil
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


def historical_members(stage, mode):
    """Copy members written by the previous report publisher for each retired mode."""
    fixtures = Path(__file__).parent / "fixtures"
    shutil.copytree(fixtures / "historical-presentation" / mode, stage)
    shutil.copyfile(PRODUCT, stage / "product-identity.json")
    return {p.relative_to(stage).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest() for p in stage.rglob("*") if p.is_file()}


def test_standalone_capture_refuses_retired_selectors_by_presence(modules, monkeypatch):
    reader = importlib.import_module("business_report_presentation")
    for key in ("HARTMESH_REPORT_TENANT_PRESENTATION", "HARTMESH_REPORT_TENANT_SHA256"):
        with monkeypatch.context() as changes:
            changes.setenv(key, "")
            with pytest.raises(reader.InputError, match=key):
                reader.capture_presentation(None)


def test_explicit_default_offline_product_preserves_tenant_brand_without_provider(modules, tmp_path, monkeypatch):
    reader = importlib.import_module("business_report_presentation")
    product = PRODUCT.with_name("default.json")
    digest = json.loads(product.read_bytes())["sha256"]
    monkeypatch.setenv("HARTMESH_REPORT_PRODUCT_IDENTITY", str(product))
    monkeypatch.setenv("HARTMESH_REPORT_PRODUCT_SHA256", digest)
    (tmp_path / "brand.json").write_text('{"company_name":"Example Workspace"}', encoding="utf-8")
    snapshot = reader.capture_presentation(str(tmp_path))
    assert snapshot.product_identity.digest == digest and snapshot.product_identity.configured is False
    assert snapshot.product_identity.provider_name == ""
    assert snapshot.brand()["company"] == "Example Workspace"


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
def test_historical_presentation_records_still_validate(modules, tmp_path, mode):
    reader = importlib.import_module("business_report_presentation")
    stage = tmp_path / "bundle"
    members = historical_members(stage, mode)
    record = reader.validate_retained(stage, members)
    assert record["mode"] == mode
    assert record["product_sha256"] == json.loads(PRODUCT.read_bytes())["sha256"]
    if mode == "tenant":
        assert record["tenant"]["name"] == "Example Tenant One"
        (stage / "presentation/tenant.png").write_bytes(png("blue"))
        with pytest.raises(ValueError, match="presentation"):
            reader.validate_retained(stage, members)
    else:
        assert record["tenant"] is None


def test_effective_preferences_are_complete_bounded_and_historical(modules):
    reader = importlib.import_module("business_report_presentation")
    original = reader.capture_presentation(None)
    chosen = original.with_preferences({"version": 1, "brand": {"primary": "#ffffff", "secondary": "#9ccdb4"}})
    assert chosen.brand()["primary"] == "#ffffff"
    assert original.brand()["primary"] == "#1F4E79"
    assert chosen.digest != original.digest
    for brand in ({"tertiary": "#123456"}, {"primary": "red"}, {"company": "Override"}):
        with pytest.raises(ValueError, match="palette|preferences"):
            original.with_preferences({"version": 1, "brand": brand})


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
    stage = tmp_path / "bundle"
    historical_members(stage, "tenant")
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
