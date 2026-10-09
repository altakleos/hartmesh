"""Authenticated human Work management; no model or activation endpoints."""

from fastapi import APIRouter, Query, Request

from app.gateway.authz import require_permission
from app.gateway.routers.agent_instances import InstanceRoute, _service
from app.gateway.routers.spaces import _actor
from deerflow.agent_instances.work import AgentWork
from deerflow.agent_instances.work_contract import DelegateWork, WorkCommand

router = APIRouter(prefix="/api/agent-instances", tags=["agent-work"], route_class=InstanceRoute)


@router.get("/{instance_id}/work")
@require_permission("agents", "read")
async def list_work(instance_id: str, request: Request, limit: int = Query(50, ge=1, le=100), offset: int = Query(0, ge=0, le=100000)):
    records = await AgentWork(await _service(request)).list(actor=await _actor(request), instance_id=instance_id, limit=limit, offset=offset)
    return {"work": records, "availability": "records_only", "execution_available": False}


@router.post("/{instance_id}/work", status_code=201)
@require_permission("agents", "write")
@require_permission("agents", "read")
async def delegate_work(instance_id: str, body: DelegateWork, request: Request):
    return await AgentWork(await _service(request)).delegate(actor=await _actor(request), instance_id=instance_id, request=body)


@router.get("/{instance_id}/work/{work_id}")
@require_permission("agents", "read")
async def get_work(instance_id: str, work_id: str, request: Request):
    return await AgentWork(await _service(request)).get(actor=await _actor(request), instance_id=instance_id, work_id=work_id)


@router.get("/{instance_id}/work/{work_id}/history")
@require_permission("agents", "read")
async def work_history(instance_id: str, work_id: str, request: Request, limit: int = Query(50, ge=1, le=100), offset: int = Query(0, ge=0, le=100000)):
    events = await AgentWork(await _service(request)).history(actor=await _actor(request), instance_id=instance_id, work_id=work_id, limit=limit, offset=offset)
    return {"events": events}


@router.post("/{instance_id}/work/{work_id}/commands")
@require_permission("agents", "write")
@require_permission("agents", "read")
async def command_work(instance_id: str, work_id: str, body: WorkCommand, request: Request):
    return await AgentWork(await _service(request)).command(actor=await _actor(request), instance_id=instance_id, work_id=work_id, request=body)
