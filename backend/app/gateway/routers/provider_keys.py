"""Provider keys an administrator manages in the product.

``GET /api/provider-keys`` lists every provider in the release's catalog with
where its key comes from -- ``product``, ``environment`` or ``none`` -- and
when it last changed; ``PUT /api/provider-keys/{provider}`` with
``{"key": "..."}`` adds or replaces one, ``DELETE`` removes it, and
``GET /api/provider-keys/events`` is the record of who did which, and when.
An administrator's interactive session only, as for adding a person. No
response, log line or error carries a key: the body is parsed here rather
than by a model whose validation errors would quote what was sent. How a change takes effect is ``app.gateway.provider_keys.service``.
"""

from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request, Response

from app.gateway.deps import require_admin_user
from app.gateway.provider_keys.service import KEY_MAX_LENGTH, ProviderKeyService, ProviderKeysRefused
from app.gateway.routers.auth import require_session_source

# Every route, reads included: an administrator's personal access token carries no admin capability.
router = APIRouter(prefix="/api/provider-keys", tags=["provider-keys"], dependencies=[Depends(require_session_source)])

_ADMIN_DETAIL = "Only an administrator manages provider keys"
# Room for the longest key JSON-escaped, and no more before it is parsed.
_BODY_MAX_BYTES = 6 * KEY_MAX_LENGTH + 64
_BODY_MESSAGE = 'the body must be the JSON object {"key": "<the provider key>"} and nothing else'
_NOT_AVAILABLE = {
    "code": "not_available",
    "message": "This deployment does not manage provider keys in the product; its provider keys are set where it is deployed",
}


def _service(request: Request) -> ProviderKeyService | None:
    return getattr(request.app.state, "provider_keys", None)


def _refused(refusal: ProviderKeysRefused) -> HTTPException:
    return HTTPException(status_code=refusal.status, detail={"code": refusal.code, "message": refusal.message})


def _require_service(request: Request) -> ProviderKeyService:
    service = _service(request)
    if service is None:
        raise HTTPException(status_code=409, detail=_NOT_AVAILABLE)
    return service


def _body_invalid() -> HTTPException:
    # Fixed, whatever arrived: a key sent under the wrong name is still a key.
    return HTTPException(status_code=422, detail={"code": "body_invalid", "message": _BODY_MESSAGE})


async def _key_from_body(request: Request) -> str:
    raw = bytearray()
    async for chunk in request.stream():
        raw += chunk
        if len(raw) > _BODY_MAX_BYTES:
            raise _body_invalid()
    try:
        body = json.loads(raw)
    except (ValueError, RecursionError):
        body = None
    if not isinstance(body, dict) or set(body) != {"key"} or not isinstance(body["key"], str):
        raise _body_invalid()
    return body["key"]


@router.get("")
async def list_provider_keys(request: Request) -> dict[str, Any]:
    await require_admin_user(request, detail=_ADMIN_DETAIL)
    service = _service(request)
    if service is None:
        return {"available": False, "refusal": _NOT_AVAILABLE, "wrapping_key": None, "providers": []}
    return await service.status()


@router.get("/events")
async def list_provider_key_events(request: Request, limit: int = 50) -> dict[str, Any]:
    await require_admin_user(request, detail=_ADMIN_DETAIL)
    service = _require_service(request)
    if not 1 <= limit <= 200:
        raise HTTPException(status_code=422, detail={"code": "limit_invalid", "message": "limit must be between 1 and 200"})
    return {"events": await service.events(limit=limit)}


@router.put("/{provider}")
async def put_provider_key(provider: str, request: Request, response: Response) -> dict[str, Any]:
    admin = await require_admin_user(request, detail=_ADMIN_DETAIL)
    service = _require_service(request)
    key = await _key_from_body(request)
    try:
        result = await service.put(provider, key, actor_id=str(admin.id), actor_email=getattr(admin, "email", None))
    except ProviderKeysRefused as refusal:
        raise _refused(refusal) from None
    response.headers["Cache-Control"] = "no-store"
    return result


@router.delete("/{provider}")
async def delete_provider_key(provider: str, request: Request) -> dict[str, Any]:
    admin = await require_admin_user(request, detail=_ADMIN_DETAIL)
    service = _require_service(request)
    try:
        return await service.remove(provider, actor_id=str(admin.id), actor_email=getattr(admin, "email", None))
    except ProviderKeysRefused as refusal:
        raise _refused(refusal) from None
