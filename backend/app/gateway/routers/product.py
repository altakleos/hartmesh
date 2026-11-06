"""Public deployment product/provider identity and the legacy name-only reader.

Only explicitly approved shared fields and sanitized raster assets are public.
Tenant branding and configuration remain behind their existing authentication.
"""

from fastapi import APIRouter, Depends, HTTPException, Response
from pydantic import BaseModel, Field

from app.gateway.deps import get_config
from deerflow.config.app_config import AppConfig
from deerflow.config.product_identity import identity_for
from deerflow.config.ui_config import MAX_PRODUCT_NAME_CHARS, product_name

# Beside the other reads a sign-in page makes before anyone is signed in
# (``/api/v1/auth/providers``, ``/api/v1/auth/setup-status``). A public path
# directly under ``/api`` would be one an extension's own ``/api/{name}`` route
# could enter, and the extension host refuses such routes.
router = APIRouter(prefix="/api/v1/auth", tags=["product"])


class ProductResponse(BaseModel):
    name: str = Field(..., max_length=MAX_PRODUCT_NAME_CHARS, description="What the product is called; `ui.product_name`, HartMesh when unset")


@router.get("/product", response_model=ProductResponse, summary="Product name")
def get_product(config: AppConfig = Depends(get_config)) -> ProductResponse:
    try:
        return ProductResponse(name=product_name(config))
    except ValueError:
        raise HTTPException(503, "Service identity unavailable") from None


@router.get("/product-identity", summary="Public product and provider identity")
def get_product_identity(response: Response, config: AppConfig = Depends(get_config)) -> dict:
    response.headers["Cache-Control"] = "no-store"
    try:
        return identity_for(config)
    except ValueError:
        raise HTTPException(503, "Service identity unavailable", headers={"Cache-Control": "no-store"}) from None
