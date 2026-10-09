"""Attention and Work-backed human input, with no execution side effects."""

from typing import Literal

from fastapi import APIRouter, Query, Request, Response

from app.gateway.authz import require_permission
from app.gateway.routers.agent_instances import InstanceRoute, _service
from app.gateway.routers.spaces import _actor
from deerflow.agent_instances.human_input import HumanInput
from deerflow.agent_instances.human_input_contract import CreateRequest, RequestCommand, Respond
from deerflow.agent_instances.work import AgentWork
from deerflow.agent_instances.work_contract import Revision, StrictModel

router = APIRouter(prefix="/api", tags=["human-input"], route_class=InstanceRoute)


async def service(request):
    return HumanInput(AgentWork(await _service(request)))


def _private(response):
    response.headers["Cache-Control"] = "no-store"


@router.get("/human-input")
@require_permission("agents", "read")
async def attention(
    request: Request,
    response: Response,
    view: Literal["pending", "answered", "routing", "all"] = "pending",
    instance_id: str | None = None,
    work_id: str | None = None,
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0, le=100000),
):
    _private(response)
    return await (await service(request)).list(actor=await _actor(request), view=view, instance_id=instance_id, work_id=work_id, limit=limit, offset=offset, can_write=await can_write(request))


async def can_write(request):
    from app.gateway.authz import get_auth_context, resolve_route_permissions_for_request
    from app.gateway.deps import get_current_user_from_request

    context = get_auth_context(request)
    if context is not None:
        return context.has_permission("agents", "write")
    permissions = await resolve_route_permissions_for_request(request, await get_current_user_from_request(request))
    return "agents:write" in permissions


@router.get("/human-input/{request_id}")
@require_permission("agents", "read")
async def get_request(request_id: str, request: Request, response: Response):
    _private(response)
    return await (await service(request)).get(actor=await _actor(request), request_id=request_id, can_write=await can_write(request))


@router.get("/human-input/{request_id}/responses")
@require_permission("agents", "read")
async def responses(request_id: str, request: Request, response: Response, limit: int = Query(50, ge=1, le=100), offset: int = Query(0, ge=0, le=100000)):
    _private(response)
    return await (await service(request)).responses(actor=await _actor(request), request_id=request_id, limit=limit, offset=offset)


@router.get("/human-input/{request_id}/history")
@require_permission("agents", "read")
async def history(request_id: str, request: Request, response: Response, limit: int = Query(50, ge=1, le=100), offset: int = Query(0, ge=0, le=100000)):
    _private(response)
    return await (await service(request)).history(actor=await _actor(request), request_id=request_id, limit=limit, offset=offset)


@router.post("/agent-instances/{instance_id}/work/{work_id}/requests", status_code=201)
@require_permission("agents", "read")
@require_permission("agents", "write")
async def create_request(instance_id: str, work_id: str, body: CreateRequest, request: Request):
    return await (await service(request)).create(actor=await _actor(request), instance_id=instance_id, work_id=work_id, request=body)


@router.post("/human-input/{request_id}/responses")
@require_permission("agents", "read")
@require_permission("agents", "write")
async def respond(request_id: str, body: Respond, request: Request):
    return await (await service(request)).respond(actor=await _actor(request), request_id=request_id, request=body)


@router.post("/human-input/{request_id}/commands")
@require_permission("agents", "read")
@require_permission("agents", "write")
async def command(request_id: str, body: RequestCommand, request: Request):
    return await (await service(request)).command(actor=await _actor(request), request_id=request_id, request=body)


class MarkRead(StrictModel):
    revision: Revision


@router.post("/human-input/{request_id}/read")
@require_permission("agents", "read")
async def mark_read(request_id: str, body: MarkRead, request: Request):
    return await (await service(request)).mark_read(actor=await _actor(request), request_id=request_id, revision=body.revision)
