"""Authenticated access to the live tenant bundle logo."""

import mimetypes

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse

from app.gateway.deps import get_config
from deerflow.config.app_config import AppConfig
from deerflow.config.tenant_bundle import configured_tenant_bundle
from deerflow.utils.file_io import run_file_io

router = APIRouter(prefix="/api/branding", tags=["branding"])


@router.get(
    "/logo",
    summary="Tenant logo",
    description="The picture the tenant bundle names as its logo; 404 when the bundle names none.",
    response_class=FileResponse,
)
async def get_logo(config: AppConfig = Depends(get_config)) -> FileResponse:
    logo = (await run_file_io(configured_tenant_bundle, config.tenant_bundle.path)).logo
    if logo is None:
        raise HTTPException(status_code=404, detail="no logo")
    media_type, _ = mimetypes.guess_type(logo.name)
    # Keep the private cache short so replaced images reach clients promptly.
    return FileResponse(logo, media_type=media_type or "application/octet-stream", headers={"Cache-Control": "private, max-age=300"})
