"""The product's name, for pages that render before anyone signs in.

The sign-in and setup pages carry it as their heading and every page as its
tab title, so this is public -- unlike ``/api/features`` and the tenant logo,
which are for people the deployment signed in. It says only what the
deployment calls itself; a company's own brand is still delivered after sign-in.
"""

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field

from app.gateway.deps import get_config
from deerflow.config.app_config import AppConfig
from deerflow.config.ui_config import MAX_PRODUCT_NAME_CHARS, product_name

router = APIRouter(prefix="/api", tags=["product"])


class ProductResponse(BaseModel):
    name: str = Field(..., max_length=MAX_PRODUCT_NAME_CHARS, description="What the product is called; `ui.product_name`, HartMesh when unset")


@router.get("/product", response_model=ProductResponse, summary="Product name")
async def get_product(config: AppConfig = Depends(get_config)) -> ProductResponse:
    return ProductResponse(name=product_name(config))
