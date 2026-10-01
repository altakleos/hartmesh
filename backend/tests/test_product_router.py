"""The product's name is read before sign-in: it heads the sign-in page and titles the tab."""

from __future__ import annotations

from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.gateway.auth_middleware import _is_public
from app.gateway.deps import get_config
from app.gateway.routers import product
from deerflow.config.ui_config import UiConfig


def _app(ui: UiConfig) -> FastAPI:
    app = FastAPI()
    app.include_router(product.router)
    app.dependency_overrides[get_config] = lambda: SimpleNamespace(ui=ui)
    return app


def test_the_name_is_the_one_the_operator_configured() -> None:
    with TestClient(_app(UiConfig(product_name="Acme Assist"))) as client:
        response = client.get("/api/product")

    assert response.status_code == 200
    assert response.json() == {"name": "Acme Assist"}


def test_a_deployment_that_names_nothing_is_hartmesh() -> None:
    with TestClient(_app(UiConfig())) as client:
        assert client.get("/api/product").json() == {"name": "HartMesh"}


def test_the_name_is_public_and_nothing_next_to_it_is() -> None:
    assert _is_public("/api/product")
    assert not _is_public("/api/product/logo")
    assert not _is_public("/api/features")


def test_the_gateway_mounts_the_route() -> None:
    # The router tests above build their own app; this one asks the Gateway's own
    # factory, so the route cannot be left unmounted while they still pass.
    from app.gateway.app import create_app

    routes = [route for route in create_app().routes if getattr(route, "path", None) == "/api/product"]
    assert [sorted(route.methods) for route in routes] == [["GET"]]
    assert routes[0].endpoint is product.get_product
