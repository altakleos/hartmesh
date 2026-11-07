"""Strict presentation capture must read/decode files outside the event loop."""

import asyncio
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI
from test_tenant_presentation import PRODUCT, content_digest, stage

from app.gateway.deps import get_config
from app.gateway.routers import branding
from deerflow.config.tenant_bundle import TenantBundleConfig
from deerflow.config.tenant_presentation import PresentationReader


@pytest.fixture
def app(tmp_path):
    manifest = stage(tmp_path)
    config = SimpleNamespace(tenant_bundle=TenantBundleConfig(path=str(tmp_path), presentation="strict-v1", presentation_sha256=content_digest(manifest)))
    application = FastAPI()
    application.include_router(branding.router)
    application.state.tenant_presentation_reader = PresentationReader(config.tenant_bundle, PRODUCT)
    application.dependency_overrides[get_config] = lambda: config

    @application.middleware("http")
    async def route_unit_provenance(request, call_next):
        request.state.auth_source = "session"
        return await call_next(request)

    return application


@pytest.mark.asyncio
async def test_real_file_capture_and_repeated_projection_do_not_block(app):
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gateway.test") as client:
        response = await client.get("/api/branding/presentation")
        assert response.status_code == 200
        assert response.json()["tenant"]["name"] == "Example Tenant One"
        second = await client.get("/api/branding/presentation")
        assert second.content == response.content


@pytest.mark.asyncio
async def test_control_proves_inline_file_capture_is_detected(app):
    from blockbuster import BlockingError

    reader = app.state.tenant_presentation_reader
    with pytest.raises(BlockingError):
        reader.read()
    assert (await asyncio.to_thread(reader.read))["tenant"]["name"] == "Example Tenant One"
