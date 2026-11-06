"""Public deployment identity is a complete, bounded snapshot, never tenant data."""

import copy
import json
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from PIL import Image

from app.gateway.deps import get_config
from app.gateway.routers.product import router
from deerflow.config.product_identity import identity_for, mounted_identity
from deerflow.config.ui_config import UiConfig, product_name


@pytest.fixture(autouse=True)
def isolated(monkeypatch):
    monkeypatch.delenv("HARTMESH_PRODUCT_PAYLOAD", raising=False)
    mounted_identity.cache_clear()
    yield
    mounted_identity.cache_clear()


@pytest.fixture
def payload(tmp_path, monkeypatch):
    assets = tmp_path / "assets"
    assets.mkdir()
    Image.new("RGB", (64, 64), "navy").save(assets / "mark.png")
    body = dict(
        schema_version=1,
        public=True,
        product=dict(name="Example Product One", logo="assets/mark.png", browser_icon="assets/mark.png"),
        provider=dict(name="Example Provider One", logo="assets/mark.png", support_url="https://support.example.test/one"),
    )

    def save(data=body):
        (tmp_path / "product.json").write_text(json.dumps(data), encoding="utf-8")

    save()
    monkeypatch.setenv("HARTMESH_PRODUCT_PAYLOAD", str(tmp_path))
    return tmp_path, body, save


def config(**kwargs):
    return SimpleNamespace(ui=UiConfig(**kwargs), tenant_bundle=SimpleNamespace(path=None))


def test_default_and_legacy_name():
    assert identity_for(config())["mode"] == "default"
    assert identity_for(config(product_name="Legacy"))["name"] == "Legacy"


@pytest.mark.parametrize("number", ["One", "Two"])
def test_complete_public_profile(payload, number):
    root, body, save = payload
    body["product"]["name"] = f"Example Product {number}"
    body["provider"]["name"] = f"Example Provider {number}"
    save()
    result = identity_for(config())
    assert result["name"] == body["product"]["name"]
    assert result["provider"]["name"] == body["provider"]["name"]
    assert result["product"]["logo"].startswith("data:image/png;base64,")
    assert product_name(config()) == result["name"]
    (root / "product.json").unlink()
    assert identity_for(config()) == result  # process snapshot survives mount loss
    result["provider"]["name"] = "mutated"
    assert identity_for(config())["provider"]["name"] == body["provider"]["name"]


@pytest.mark.parametrize(
    "mutation",
    [
        lambda b: b.update(schema_version=2),
        lambda b: b.update(secret="credential"),
        lambda b: b.update(public=False),
        lambda b: b["product"].update(name=" "),
        lambda b: b["product"].update(name="x\u0085y"),
        lambda b: b["product"].update(name="x\ud800y"),
        lambda b: b["product"].update(logo="../mark.png"),
        lambda b: b["product"].update(logo="/etc/passwd"),
        lambda b: b["provider"].update(support_url="javascript:alert(1)"),
        lambda b: b["provider"].update(support_url="https://user:pass@example.test"),
        lambda b: b["provider"].update(tenant="private"),
    ],
)
def test_invalid_contract_rejects(payload, mutation):
    _, body, save = payload
    changed = copy.deepcopy(body)
    mutation(changed)
    save(changed)
    with pytest.raises(ValueError):
        identity_for(config())


@pytest.mark.parametrize("case", ["svg", "symlink", "hardlink", "oversize", "wide", "animation", "trailing", "trailing-iend", "duplicate", "unknown-file"])
def test_invalid_assets_and_json(payload, case):
    root, _, _ = payload
    mark = root / "assets/mark.png"
    if case == "svg":
        mark.write_text("<svg><script>alert(1)</script></svg>")
    elif case == "symlink":
        mark.rename(root.parent / "outside.png")
        mark.symlink_to(root.parent / "outside.png")
    elif case == "hardlink":
        import os

        os.link(mark, root.parent / "copy.png")
    elif case == "oversize":
        mark.write_bytes(b"x" * (256 * 1024 + 1))
    elif case == "wide":
        Image.new("RGB", (2049, 1)).save(mark)
    elif case == "animation":
        Image.new("RGB", (64, 64), "red").save(mark, save_all=True, append_images=[Image.new("RGB", (64, 64), "blue")])
    elif case == "trailing":
        mark.write_bytes(mark.read_bytes() + b"<script>bad</script>")
    elif case == "trailing-iend":
        mark.write_bytes(mark.read_bytes() + b"<script>bad</script>" + mark.read_bytes()[-12:])
    elif case == "unknown-file":
        (root / "credentials.json").write_text("{}")
    else:
        p = root / "product.json"
        p.write_text(p.read_text().replace('"schema_version": 1', '"schema_version": 1, "schema_version": 1'))
    with pytest.raises(ValueError):
        identity_for(config())


def test_fixed_fields_reject_legacy_and_tenant_conflicts(payload, tmp_path):
    with pytest.raises(ValueError, match="fixed"):
        identity_for(config(product_name="Other"))
    assert identity_for(config(product_name="Example Product One"))["mode"] == "configured"
    (tmp_path / "provider.json").write_text('{"display_name":"Unrelated"}')
    cfg = config()
    cfg.tenant_bundle.path = str(tmp_path)
    with pytest.raises(ValueError, match="provider.json"):
        identity_for(cfg)


def test_public_api_does_not_project_tenant_and_failure_is_explicit(payload):
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_config] = lambda: config()
    with TestClient(app) as client:
        r = client.get("/api/v1/auth/product-identity")
        assert r.status_code == 200
        assert r.headers["cache-control"] == "no-store"
        assert "tenant" not in r.text
        assert "path" not in r.text
        assert client.get("/api/v1/auth/product").json() == {"name": "Example Product One"}
        mounted_identity.cache_clear()
        (payload[0] / "product.json").unlink()
        r = client.get("/api/v1/auth/product-identity")
        assert r.status_code == 503
        assert r.json() == {"detail": "Service identity unavailable"}


def test_actual_middleware_keeps_tenant_data_protected(payload):
    from app.gateway.app import create_app

    app = create_app()
    app.dependency_overrides[get_config] = lambda: config()
    client = TestClient(app)
    assert client.get("/api/v1/auth/product-identity").status_code == 200
    for path in ["/api/features", "/api/branding/logo", "/api/v1/auth/product-identity/tenant", "/api/v1/auth/me"]:
        assert client.get(path).status_code == 401


def test_app_config_rejects_fixed_fields_on_all_validation_paths(payload, monkeypatch):
    from deerflow.config.app_config import AppConfig

    base = {"sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"}}
    assert product_name(AppConfig.model_validate(base)) == "Example Product One"
    with pytest.raises(ValueError, match="fixed"):
        AppConfig.model_validate({**base, "ui": {"product_name": "Other"}})
    monkeypatch.setenv("HARTMESH_PRODUCT_NAME", "Other")
    with pytest.raises(ValueError, match="fixed"):
        AppConfig.model_validate(base)


@pytest.mark.parametrize("field,value", [("schema_version", True), ("public", 1)])
def test_contract_literals_do_not_coerce_boolean_and_integer(payload, field, value):
    _, body, save = payload
    body[field] = value
    save()
    with pytest.raises(ValueError):
        identity_for(config())


def test_supported_defaults_are_independent_of_fixed_identity(payload):
    from deerflow.config.app_config import AppConfig

    cfg = AppConfig.model_validate({"sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"}, "ui": {"profile": "business", "starters": []}})
    assert cfg.ui.profile == "business"
    assert cfg.ui.starters == []
    assert identity_for(cfg)["name"] == "Example Product One"


def test_read_only_payload_needs_no_image_or_source_writes(payload):
    root, _, _ = payload
    paths = [root / "product.json", root / "assets/mark.png", root / "assets", root]
    original = {p: p.stat().st_mode & 0o777 for p in paths}
    before = {p: p.read_bytes() for p in paths[:2]}
    try:
        for p in paths:
            p.chmod(0o555 if p.is_dir() else 0o444)
        assert identity_for(config())["name"] == "Example Product One"
        assert {p: p.read_bytes() for p in paths[:2]} == before
    finally:
        for p in reversed(paths):
            p.chmod(original[p])


def test_rejected_payload_diagnostics_do_not_echo_unknown_private_values(payload):
    import traceback

    _, body, save = payload
    body["unexpected"] = "synthetic-sensitive-value"
    save()
    with pytest.raises(ValueError) as caught:
        identity_for(config())
    assert "synthetic-sensitive-value" not in "".join(traceback.format_exception(caught.value))


def test_unconfigured_identity_keeps_legacy_unicode_joiners():
    value = "Example\u200dAssist"
    cfg = config(product_name=value)
    assert identity_for(cfg)["name"] == value
    assert identity_for(cfg)["product"]["name"] == value


def test_admitted_runtime_name_does_not_read_tenant_files_or_copy_assets(payload, monkeypatch):
    import os

    cfg = config()
    cfg.tenant_bundle.path = str(payload[0])
    identity_for(cfg)  # configuration admission occurs before runtime reads

    def forbidden(*args, **kwargs):
        raise AssertionError("runtime name lookup must not perform file I/O or copy image bytes")

    with monkeypatch.context() as patch:
        patch.setattr(os.path, "lexists", forbidden)
        patch.setattr(copy, "deepcopy", forbidden)
        assert product_name(cfg) == "Example Product One"
