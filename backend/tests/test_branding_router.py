"""The logo route serves the picture the bundle loader resolved, to signed-in people only."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.gateway.auth_middleware import _is_public
from app.gateway.deps import get_config
from app.gateway.routers import branding

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
    """The login page stays the product's own; a customer's name and picture are for the people it signed in."""
    assert not _is_public("/api/branding/logo")
    assert not _is_public("/api/features")
