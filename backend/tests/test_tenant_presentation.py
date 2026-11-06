"""Complete tenant capture, independent of mutable feature/settings readers."""

from __future__ import annotations

import base64
import hashlib
import json
import os
from io import BytesIO

import pytest
from PIL import Image

from deerflow.config.tenant_bundle import TenantBundleConfig, load_tenant_bundle
from deerflow.config.tenant_presentation import PresentationReader, capture_presentation, content_digest

PRODUCT = {"schema_version": 1, "mode": "default", "name": "HartMesh", "product": {"name": "HartMesh", "logo": None, "browser_icon": None}, "provider": None}


def png(color="red", size=(32, 32)):
    output = BytesIO()
    Image.new("RGBA", size, color).save(output, format="PNG")
    return output.getvalue()


def stage(root, *, name="Example Tenant One", picture=None, mode="tenant"):
    root.mkdir(exist_ok=True)
    picture = picture or png()
    (root / "logo.png").write_bytes(picture)
    manifest = {"schema_version": 1, "mode": mode, "product_sha256": content_digest(PRODUCT), "tenant": {"name": name, "logo": {"path": "logo.png", "sha256": hashlib.sha256(picture).hexdigest()}} if mode == "tenant" else None}
    (root / "presentation.json").write_text(json.dumps(manifest), encoding="utf-8")
    return manifest


def capture(root, manifest):
    return capture_presentation(str(root), content_digest(manifest), PRODUCT)


def test_capture_binds_names_product_and_sanitized_bytes_and_survives_source_loss(tmp_path):
    manifest = stage(tmp_path)
    snapshot = capture(tmp_path, manifest)
    body = snapshot.projection()
    assert body["tenant"]["name"] == "Example Tenant One"
    assert body["product_sha256"] == content_digest(PRODUCT)
    assert body["snapshot_id"] == content_digest({k: v for k, v in body.items() if k != "snapshot_id"})
    with Image.open(BytesIO(base64.b64decode(body["tenant"]["logo"].split(",")[1]))) as image:
        assert image.getpixel((0, 0)) == (255, 0, 0, 255)
    for path in tmp_path.iterdir():
        path.unlink()
    assert snapshot.projection() == body
    body["tenant"]["name"] = "corrupted caller"
    assert snapshot.projection()["tenant"]["name"] == "Example Tenant One"


def test_product_only_requires_explicit_manifest_and_preserves_unrelated_files(tmp_path):
    manifest = stage(tmp_path, mode="product-only")
    (tmp_path / "starters.json").write_text("[]")
    (tmp_path / "roles.json").write_text("[]")
    (tmp_path / "report-profile.json").write_text('{"independent":true}')
    before = {p.name: p.read_bytes() for p in tmp_path.iterdir()}
    assert capture(tmp_path, manifest).projection()["tenant"] is None
    assert load_tenant_bundle(tmp_path).starters == ()
    assert load_tenant_bundle(tmp_path).roles == ()
    assert before == {p.name: p.read_bytes() for p in tmp_path.iterdir()}
    (tmp_path / "presentation.json").unlink()
    with pytest.raises(ValueError, match="Tenant presentation unavailable"):
        capture(tmp_path, manifest)


@pytest.mark.parametrize("size", [(1024, 1024), (1024, 1), (1, 1024)])
def test_boundary_logo_dimensions_and_full_length_name_are_admitted_together(tmp_path, size):
    manifest = stage(tmp_path, name="X" * 80, picture=png(size=size))
    projection = capture(tmp_path, manifest).projection()
    assert projection["tenant"]["name"] == "X" * 80
    with Image.open(BytesIO(base64.b64decode(projection["tenant"]["logo"].split(",")[1]))) as image:
        assert image.size == size


@pytest.mark.parametrize(
    "change",
    [
        "version",
        "boolean-version",
        "missing-mode",
        "unknown",
        "product-override",
        "missing-name",
        "blank-name",
        "trim-name",
        "long-name",
        "control-name",
        "surrogate-name",
        "missing-logo",
        "product-mismatch",
        "asset-mismatch",
        "traversal",
        "absolute",
        "backslash",
        "extra-logo",
        "colors",
        "product-only-with-tenant",
        "old-schema",
    ],
)
def test_invalid_strict_manifest_never_admits_partial_presentation(tmp_path, change):
    value = stage(tmp_path)
    if change == "version":
        value["schema_version"] = 2
    elif change == "boolean-version":
        value["schema_version"] = True
    elif change == "missing-mode":
        del value["mode"]
    elif change == "unknown":
        value["private-field"] = "private-value"
    elif change == "product-override":
        value["product"] = {"name": "private-value"}
    elif change == "missing-name":
        del value["tenant"]["name"]
    elif change == "blank-name":
        value["tenant"]["name"] = ""
    elif change == "trim-name":
        value["tenant"]["name"] = " Example "
    elif change == "long-name":
        value["tenant"]["name"] = "X" * 81
    elif change == "control-name":
        value["tenant"]["name"] = "private\u202evalue"
    elif change == "surrogate-name":
        value["tenant"]["name"] = "\ud800"
    elif change == "missing-logo":
        value["tenant"]["logo"] = None
    elif change == "product-mismatch":
        value["product_sha256"] = "0" * 64
    elif change == "asset-mismatch":
        value["tenant"]["logo"]["sha256"] = "0" * 64
    elif change in ("traversal", "absolute", "backslash"):
        value["tenant"]["logo"]["path"] = {"traversal": "../logo.png", "absolute": "/logo.png", "backslash": "..\\logo.png"}[change]
    elif change == "extra-logo":
        value["tenant"]["logo"]["unknown"] = True
    elif change == "colors":
        value["tenant"]["colors"] = {"primary": "#fff"}
    elif change == "product-only-with-tenant":
        value["mode"] = "product-only"
    elif change == "old-schema":
        value = {"company_name": "private-value"}
    (tmp_path / "presentation.json").write_text(json.dumps(value))
    with pytest.raises(ValueError, match="^Tenant presentation unavailable or invalid$") as error:
        capture(tmp_path, value)
    assert "private" not in str(error.value)


@pytest.mark.parametrize("picture", [b"<svg onload='alert(1)'/>", b"\x89PNG\r\n\x1a\n", png() + b"trailing", png(size=(1025, 1)), b"x" * (256 * 1024 + 1)], ids=["active", "header", "trailing", "dimensions", "bytes"])
def test_invalid_assets_refuse(tmp_path, picture):
    manifest = stage(tmp_path, picture=picture)
    with pytest.raises(ValueError):
        capture(tmp_path, manifest)


@pytest.mark.skipif(os.name != "posix", reason="strict descriptor traversal is POSIX")
@pytest.mark.parametrize("kind", ["file-symlink", "root-symlink", "ancestor-symlink", "hardlink", "manifest-symlink", "fifo"])
def test_no_follow_sources(tmp_path, kind):
    root = tmp_path / "tenant"
    manifest = stage(root)
    path = root / "logo.png"
    if kind == "file-symlink":
        original = tmp_path / "outside.png"
        path.rename(original)
        path.symlink_to(original)
    elif kind == "manifest-symlink":
        path = root / "presentation.json"
        original = tmp_path / "outside.json"
        path.rename(original)
        path.symlink_to(original)
    elif kind == "root-symlink":
        alias = tmp_path / "alias"
        alias.symlink_to(root, target_is_directory=True)
        root = alias
    elif kind == "ancestor-symlink":
        alias = tmp_path / "alias"
        alias.symlink_to(tmp_path, target_is_directory=True)
        root = alias / "tenant"
    elif kind == "hardlink":
        os.link(path, tmp_path / "alias.png")
    elif kind == "fifo":
        path.unlink()
        os.mkfifo(path)
    with pytest.raises(ValueError):
        capture(root, manifest)


def test_manifest_digest_is_independently_pinned_and_duplicate_keys_refuse(tmp_path):
    manifest = stage(tmp_path)
    with pytest.raises(ValueError):
        capture_presentation(str(tmp_path), "0" * 64, PRODUCT)
    text = json.dumps(manifest)
    (tmp_path / "presentation.json").write_text(text[:-1] + ',"mode":"tenant"}')
    with pytest.raises(ValueError):
        capture(tmp_path, manifest)


def test_reader_freezes_success_and_failed_reader_can_recover_only_complete_input(tmp_path):
    manifest = stage(tmp_path)
    settings = TenantBundleConfig(path=str(tmp_path), presentation="strict-v1", presentation_sha256=content_digest(manifest))
    reader = PresentationReader(settings, PRODUCT)
    admitted = reader.read()
    stage(tmp_path, name="Example Tenant Revised", picture=png("blue"))
    assert reader.read() == admitted
    fresh = PresentationReader(settings, PRODUCT)
    with pytest.raises(ValueError):
        fresh.read()
    stage(tmp_path)
    assert fresh.read() == admitted


def test_legacy_is_explicit_compatibility_default_not_inferred_from_a_directory(tmp_path):
    assert TenantBundleConfig().presentation == "legacy"
    assert PresentationReader(TenantBundleConfig(path=str(tmp_path)), PRODUCT).read() == {"schema_version": 1, "mode": "legacy"}
    for settings in ({"presentation": "strict-v1"}, {"presentation": "strict-v1", "path": str(tmp_path)}, {"presentation_sha256": "0" * 64}):
        with pytest.raises(ValueError):
            TenantBundleConfig(**settings)
