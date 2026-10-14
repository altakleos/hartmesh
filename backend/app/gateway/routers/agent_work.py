"""Authenticated human Work management and explicit host run activation."""

from fastapi import APIRouter, Query, Request

from app.gateway.authz import require_permission
from app.gateway.routers.agent_instances import InstanceRoute, _service
from app.gateway.routers.spaces import _actor
from deerflow.agent_instances.work import AgentWork
from deerflow.agent_instances.work_contract import DelegateWork, WorkCommand
from deerflow.agent_instances.work_execution_contract import ActivateWork
from deerflow.utils.file_io import run_file_io

router = APIRouter(prefix="/api/agent-instances", tags=["agent-work"], route_class=InstanceRoute)


async def _work_capacity(agents, actor):
    from sqlalchemy import or_, select

    from deerflow.agent_instances.contract import AgentPermission
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow, AgentInstanceRow
    from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkRow

    residents = agents.residents
    if residents is None:
        from app.gateway.deps import get_config
        from deerflow.community.warm_pool_lifecycle import DEFAULT_REPLICAS

        replicas = (await run_file_io(get_config)).sandbox.replicas
        return {"max": max(1, (replicas or DEFAULT_REPLICAS) - 1), "in_use": 0, "running": []}
    attempts = residents.attempts_in_flight()
    capacity = {"max": residents.work_max(), "in_use": len(attempts), "running": []}
    if not attempts:
        return capacity
    attempt_ids = {item["attempt_id"] for item in attempts if item.get("attempt_id") is not None}
    work_ids = {item["work_id"] for item in attempts if item.get("work_id") is not None}
    async with agents._sf() as session:
        rows = (
            await session.execute(
                select(AgentWorkRow.id, AgentWorkRow.instance_id, AgentInstanceRow.name, AgentWorkRow.objective, AgentWorkAttemptRow.id, AgentWorkAttemptRow.requester_id, AgentWorkAttemptRow.created_at)
                .join(AgentInstanceRow, AgentInstanceRow.id == AgentWorkRow.instance_id)
                .join(AgentInstanceGrantRow, AgentInstanceGrantRow.instance_id == AgentInstanceRow.id)
                .outerjoin(AgentWorkAttemptRow, AgentWorkAttemptRow.work_id == AgentWorkRow.id)
                .where(
                    AgentInstanceGrantRow.user_id == actor.subject_id,
                    AgentInstanceGrantRow.permissions.between(1, 7),
                    AgentInstanceGrantRow.permissions.op("&")(int(AgentPermission.INSPECT)) != 0,
                    or_(AgentWorkRow.id.in_(work_ids), AgentWorkAttemptRow.id.in_(attempt_ids)),
                )
            )
        ).all()
    by_attempt = {row[4]: row for row in rows if row[4] is not None}
    by_work = {row[0]: row for row in rows}
    for item in attempts:
        row = by_attempt.get(item.get("attempt_id")) if item.get("attempt_id") is not None else by_work.get(item.get("work_id"))
        if row is None or row[1] != item.get("instance_id"):
            continue
        # Task facts are authoritative, even when missing or falsey. A bound
        # launch reservation may fill facts only from its exact attempt.
        reservation = item.get("run_id") is None and item.get("attempt_id") is not None
        capacity["running"].append(
            {"instance_id": row[1], "instance_name": row[2], "work_id": row[0], "objective": row[3], "started_at": row[6] if reservation else item.get("started_at"), "requester_id": row[5] if reservation else item.get("requester_id")}
        )
    return capacity


@router.get("/{instance_id}/work")
@require_permission("agents", "read")
async def list_work(instance_id: str, request: Request, limit: int = Query(50, ge=1, le=100), offset: int = Query(0, ge=0, le=100000)):
    agents = await _service(request)
    actor = await _actor(request)
    records = await AgentWork(agents).list(actor=actor, instance_id=instance_id, limit=limit, offset=offset)
    state = await agents.execution_probe(instance_id) if agents.execution_probe is not None else None
    available = state is None or state.execution == "ready"
    result = {"work": records, "availability": "explicit_activation" if available else "unavailable", "execution_available": available, "work_capacity": await _work_capacity(agents, actor)}
    if state is not None:
        result["execution_reason"] = state.reason
    return result


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
    from app.gateway.work_execution import activate_work_run
    from deerflow.agent_instances.contract import AgentDenied
    from deerflow.agent_instances.work_execution import WorkExecution

    work = AgentWork(await _service(request))
    service = WorkExecution(work)
    replay = await service.activation_exists(work_id, body.operation_id)
    execution = await execution_for_run(request, body.thread_id, require_ready=not replay)
    if execution is None or execution.instance.id != instance_id:
        raise AgentDenied("Activation requires this AI employee's bound conversation")
    scope = await activate_work_run(request, service, execution, work_id, body)
    return {**await work.get(actor=execution.requester, instance_id=instance_id, work_id=work_id), "activation_receipt": {"operation_id": body.operation_id, "attempt_id": scope.attempt_id}}
