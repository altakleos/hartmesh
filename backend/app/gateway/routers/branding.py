"""The tenant bundle's logo, served to signed-in people.

``GET /api/features`` says whether there is one; this is where the picture
comes from. It is the file the bundle loader already resolved -- a PNG or JPEG
inside the bundle directory, the same rule the report skill applies -- so
nothing here takes a path from the request or opens anything the loader did
not name. It sits behind the ordinary auth middleware on purpose: a
tenant's brand is delivered after sign-in, and the login page stays the
product's own.
"""

import mimetypes

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse

from app.gateway.deps import get_config
from deerflow.config.app_config import AppConfig
from deerflow.config.tenant_bundle import configured_tenant_bundle

router = APIRouter(prefix="/api/branding", tags=["branding"])


@router.get(
    "/logo",
    summary="Tenant logo",
    description="The picture the tenant bundle names as its logo; 404 when the bundle names none.",
    response_class=FileResponse,
)
async def get_logo(config: AppConfig = Depends(get_config)) -> FileResponse:
    logo = configured_tenant_bundle(config.tenant_bundle.path).logo
    if logo is None:
        raise HTTPException(status_code=404, detail="no logo")
    media_type, _ = mimetypes.guess_type(logo.name)
    # A short private cache: the operator can replace the file, and the header
    # picks the new one up on the next page load rather than the next day.
    return FileResponse(logo, media_type=media_type or "application/octet-stream", headers={"Cache-Control": "private, max-age=300"})
