"""Authenticated human Work management and explicit host run activation."""

from fastapi import APIRouter, Query, Request

from app.gateway.authz import require_permission
from app.gateway.routers.agent_instances import InstanceRoute, _service
from app.gateway.routers.spaces import _actor
from deerflow.agent_instances.work import AgentWork
from deerflow.agent_instances.work_contract import DelegateWork, WorkCommand
from deerflow.agent_instances.work_execution_contract import ActivateWork

router = APIRouter(prefix="/api/agent-instances", tags=["agent-work"], route_class=InstanceRoute)


@router.get("/{instance_id}/work")
@require_permission("agents", "read")
async def list_work(instance_id: str, request: Request, limit: int = Query(50, ge=1, le=100), offset: int = Query(0, ge=0, le=100000)):
    records = await AgentWork(await _service(request)).list(actor=await _actor(request), instance_id=instance_id, limit=limit, offset=offset)
    return {"work": records, "availability": "explicit_activation", "execution_available": True}


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
    from app.gateway.work_execution import dispatch_work_stop, run_cancel_allowed

    work, actor = AgentWork(await _service(request)), await _actor(request)
    can_stop = await run_cancel_allowed(request)
    result = await work.command(actor=actor, instance_id=instance_id, work_id=work_id, request=body, can_stop=can_stop)
    if can_stop:
        await dispatch_work_stop(request, work, actor=actor, instance_id=instance_id, work_id=work_id)
    return result


@router.post("/{instance_id}/work/{work_id}/activate")
@require_permission("agents", "write")
@require_permission("agents", "read")
@require_permission("runs", "create")
async def activate_work(instance_id: str, work_id: str, body: ActivateWork, request: Request):
    from app.gateway.agent_conversations import execution_for_run
    from app.gateway.run_models import RunCreateRequest
    from app.gateway.services import start_run
    from deerflow.agent_instances.contract import AgentDenied
    from deerflow.agent_instances.work_execution import WorkExecution

    work = AgentWork(await _service(request))
    execution = await execution_for_run(request, body.thread_id)
    if execution is None or execution.instance.id != instance_id:
        raise AgentDenied("Activation requires this AI employee's bound conversation")
    scope = await WorkExecution(work).reserve(execution=execution, work_id=work_id, request=body)
    if not scope.retired:
        record = await start_run(
            RunCreateRequest(input={"messages": [{"role": "user", "content": "Work on the explicitly activated assignment. Read current Work context, follow the adopted mandate, and report progress, blockers or an outcome."}]}),
            body.thread_id,
            request,
            idempotency_key="agent-work:" + scope.attempt_id,
            require_existing_thread=True,
            work_attempt=scope,
        )
        await scope.observe_run(execution, record)
    return {**await work.get(actor=execution.requester, instance_id=instance_id, work_id=work_id), "activation_receipt": {"operation_id": body.operation_id, "attempt_id": scope.attempt_id}}
