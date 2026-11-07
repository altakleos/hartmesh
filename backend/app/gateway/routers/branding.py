"""Authenticated tenant presentation over the existing mounted interface.

Strict delivery returns one content-bound in-memory name/logo snapshot and
requires verified session provenance. The separate legacy logo route retains
its tolerant live-file behavior. Neither accepts source paths from requests;
both stay behind ordinary authentication, outside public product identity.
"""

import mimetypes

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse

from app.gateway.deps import get_config
from deerflow.config.app_config import AppConfig
from deerflow.config.tenant_bundle import configured_tenant_bundle
from deerflow.config.tenant_presentation import PresentationReader
from deerflow.utils.file_io import run_file_io

router = APIRouter(prefix="/api/branding", tags=["branding"])
PRIVATE_HEADERS = {"Cache-Control": "private, no-store", "Vary": "Cookie, Authorization", "X-Content-Type-Options": "nosniff"}


def create_presentation_reader(config) -> PresentationReader:
    from deerflow.config.product_identity import identity_for
    from deerflow.config.tenant_bundle import TenantBundleConfig

    settings = getattr(config, "tenant_bundle", None) or TenantBundleConfig()
    if not isinstance(settings, TenantBundleConfig):
        settings = TenantBundleConfig(path=settings.path)
    return PresentationReader(settings, identity_for(config))


async def presentation_reader(request: Request, config) -> PresentationReader:
    # Production lifespan installs the startup selection. Minimal router apps
    # can initialize it lazily; there is no process-global protected response cache.
    reader = getattr(request.app.state, "tenant_presentation_reader", None)
    if reader is None:
        reader = await run_file_io(create_presentation_reader, config)
        request.app.state.tenant_presentation_reader = reader
    return reader


@router.get("/presentation", summary="Complete authenticated workspace presentation")
async def get_presentation(request: Request, config: AppConfig = Depends(get_config)) -> JSONResponse:
    try:
        reader = await presentation_reader(request, config)
        if reader.settings.presentation == "strict-v1":
            from app.gateway.auth_disabled import AUTH_SOURCE_SESSION

            if getattr(request.state, "auth_source", None) != AUTH_SOURCE_SESSION:
                return JSONResponse({"detail": "Workspace presentation requires a verified session"}, status_code=403, headers=PRIVATE_HEADERS)
        projection = await run_file_io(reader.read)
        return JSONResponse(projection, headers=PRIVATE_HEADERS)
    except ValueError:
        return JSONResponse({"detail": "Tenant presentation unavailable or invalid"}, status_code=503, headers=PRIVATE_HEADERS)


@router.get(
    "/logo",
    summary="Tenant logo",
    description="The picture the tenant bundle names as its logo; 404 when the bundle names none.",
    response_class=FileResponse,
)
async def get_logo(request: Request, config: AppConfig = Depends(get_config)) -> FileResponse:
    reader = await presentation_reader(request, config)
    if reader.settings.presentation == "strict-v1":
        raise HTTPException(409, "Use the complete workspace presentation", headers=PRIVATE_HEADERS)
    logo = (await run_file_io(configured_tenant_bundle, config.tenant_bundle.path)).logo
    if logo is None:
        raise HTTPException(status_code=404, detail="no logo")
    media_type, _ = mimetypes.guess_type(logo.name)
    # A short private cache: the operator can replace the file, and the header
    # picks the new one up on the next page load rather than the next day.
    return FileResponse(logo, media_type=media_type or "application/octet-stream", headers={"Cache-Control": "private, max-age=300"})
