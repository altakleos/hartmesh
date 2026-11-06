"""The logo route serves the picture the bundle loader resolved, to signed-in people only."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient
from test_tenant_presentation import PRODUCT, content_digest, png, stage

from app.gateway.auth_middleware import _is_public
from app.gateway.deps import get_config
from app.gateway.routers import branding
from deerflow.config.tenant_bundle import TenantBundleConfig
from deerflow.config.tenant_presentation import PresentationReader

PNG = b"\x89PNG\r\n\x1a\n" + b"\0" * 16


def _app(bundle_path: str | None) -> FastAPI:
    app = FastAPI()
    app.include_router(branding.router)
    app.dependency_overrides[get_config] = lambda: SimpleNamespace(tenant_bundle=SimpleNamespace(path=bundle_path))
    return app


def _bundle(tmp_path: Path, *, logo: str | None = "logo.png", picture: bytes = PNG) -> str:
    root = tmp_path / "tenant"
    root.mkdir()
    if logo:
        (root / logo).write_bytes(picture)
    (root / "brand.json").write_text(json.dumps({"company_name": "Example Services Co.", "logo": logo}), encoding="utf-8")
    return str(root)


def test_the_logo_is_served_with_its_own_type_and_a_short_private_cache(tmp_path: Path) -> None:
    with TestClient(_app(_bundle(tmp_path))) as client:
        response = client.get("/api/branding/logo")

    assert response.status_code == 200
    assert response.headers["content-type"] == "image/png"
    assert response.headers["cache-control"] == "private, max-age=300"
    assert response.content == PNG


def test_a_jpeg_logo_is_a_jpeg(tmp_path: Path) -> None:
    with TestClient(_app(_bundle(tmp_path, logo="mark.jpg", picture=b"\xff\xd8\xff" + b"\0" * 8))) as client:
        response = client.get("/api/branding/logo")

    assert response.status_code == 200
    assert response.headers["content-type"] == "image/jpeg"


def test_no_logo_is_404_not_a_broken_picture(tmp_path: Path) -> None:
    for bundle_path in (None, _bundle(tmp_path, logo=None)):
        with TestClient(_app(bundle_path)) as client:
            assert client.get("/api/branding/logo").status_code == 404


def test_a_logo_the_loader_refuses_is_404_here(tmp_path: Path) -> None:
    """The route never resolves a path of its own; what the loader would not name, it does not serve."""
    with TestClient(_app(_bundle(tmp_path, logo="logo.svg", picture=b"<svg/>"))) as client:
        assert client.get("/api/branding/logo").status_code == 404


def test_the_brand_is_delivered_after_sign_in() -> None:
    """The login page stays the product's own; a tenant's name and picture are for the people it signed in."""
    assert not _is_public("/api/branding/logo")
    assert not _is_public("/api/features")
    assert not _is_public("/api/branding/presentation")


def strict_app(root, manifest, *, auth_source="session"):
    app = _app(str(root))
    app.state.tenant_presentation_reader = PresentationReader(TenantBundleConfig(path=str(root), presentation="strict-v1", presentation_sha256=content_digest(manifest)), PRODUCT)

    @app.middleware("http")
    async def route_unit_provenance(request, call_next):
        # Route behavior only; the live browser suite proves real authentication.
        request.state.auth_source = auth_source
        return await call_next(request)

    return app


def test_strict_route_delivers_one_bound_snapshot_with_private_no_store_headers(tmp_path):
    manifest = stage(tmp_path)
    with TestClient(strict_app(tmp_path, manifest)) as client:
        first = client.get("/api/branding/presentation")
        assert first.status_code == 200
        assert first.headers["cache-control"] == "private, no-store"
        assert first.headers["vary"] == "Cookie, Authorization"
        assert first.headers["x-content-type-options"] == "nosniff"
        stage(tmp_path, name="Example Tenant Revised", picture=png("blue"))
        assert client.get("/api/branding/presentation").json() == first.json()
        assert client.get("/api/branding/logo").status_code == 409


def test_fresh_reader_refuses_changed_bytes_and_recovers_complete_original(tmp_path):
    manifest = stage(tmp_path)
    (tmp_path / "logo.png").write_bytes(png("blue"))
    with TestClient(strict_app(tmp_path, manifest)) as client:
        failed = client.get("/api/branding/presentation")
        assert failed.status_code == 503
        assert failed.headers["cache-control"] == "private, no-store"
        assert "Example" not in failed.text
        stage(tmp_path)
        assert client.get("/api/branding/presentation").json()["tenant"]["name"] == "Example Tenant One"


def test_strict_route_never_delivers_protected_data_under_auth_disabled_or_internal_provenance(tmp_path):
    manifest = stage(tmp_path)
    for source in ("auth_disabled", "internal", None):
        with TestClient(strict_app(tmp_path, manifest, auth_source=source)) as client:
            refused = client.get("/api/branding/presentation")
            assert refused.status_code == 403
            assert "Example" not in refused.text
            assert refused.headers["cache-control"] == "private, no-store"
