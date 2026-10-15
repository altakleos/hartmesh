"""Gateway adapter: existing run admission and cancellation own all execution."""

import asyncio
import logging

from sqlalchemy import select

from app.gateway.authz import get_auth_context, resolve_route_permissions_for_request
from app.gateway.deps import get_current_user_from_request, get_run_manager
from deerflow.agent_instances.contract import AgentDenied
from deerflow.utils.file_io import await_drained

logger = logging.getLogger(__name__)


class _NewActivationRequired(Exception):
    """Private authoritative preflight: no new durable attempt is permitted."""


async def activate_work_run(request, service, execution, work_id, body):
    """Own capacity, durable reservation, run launch and observation together."""
    from app.gateway.run_models import RunCreateRequest
    from app.gateway.services import start_run
    from deerflow.agent_instances.contract import Refusal
    from deerflow.agent_instances.runtime import admit_storage

    residents = service.agents.residents

    def replay_only(_attempt_id):
        raise _NewActivationRequired

    async def exact_replay():
        return await admit_storage(service.agents, execution.instance, lambda: service.reserve(execution=execution, work_id=work_id, request=body, before_admit=replay_only))

    async def perform():
        reservation = None
        promises = []
        owner = asyncio.current_task()

        def retain(promise):
            residents.retain_launch(promise, owner)
            if promise not in promises:
                promises.append(promise)

        def bind(attempt_id):
            residents.bind(reservation, attempt_id, owner=owner)

        try:
            if not await service.activation_exists(work_id, body.operation_id):
                await execution.authority.require_ready(execution)
            if residents is None:
                scope = await admit_storage(service.agents, execution.instance, lambda: service.reserve(execution=execution, work_id=work_id, request=body))
            else:
                try:
                    scope = await exact_replay()
                except _NewActivationRequired:
                    await execution.authority.require_ready(execution)
                    try:
                        reservation = await residents.admit(execution.instance, work=True)
                    except Refusal as refusal:
                        # Capacity may have yielded while a peer committed this
                        # request. Revalidate every replay grant and field.
                        try:
                            scope = await exact_replay()
                        except _NewActivationRequired:
                            raise refusal
                    else:
                        retain(reservation)
                        await execution.authority.require_ready(execution)
                        scope = await admit_storage(
                            service.agents,
                            execution.instance,
                            lambda: service.reserve(
                                execution=execution,
                                work_id=work_id,
                                request=body,
                                before_admit=bind,
                                on_rollback=lambda: residents.cancel(reservation),
                            ),
                        )
                        if reservation.attempt_id != scope.attempt_id:
                            residents.cancel(reservation)
                            reservation = None
                if not scope.retired and scope.run_id is None:
                    # An unresolved replay without a run still needs a launch
                    # promise. Reuse its exact retained one or recover it.
                    async with residents.entry(execution.instance).lock:
                        promise = residents.reservation_for(execution.instance, scope.attempt_id)
                        if promise is None:
                            reservation = await residents.admit(execution.instance, work=True)
                            retain(reservation)
                            residents.bind(reservation, scope.attempt_id, owner=owner)
                        else:
                            retain(promise)
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
            elif residents is not None and reservation is not None:
                residents.cancel(reservation)
            return scope
        except BaseException:
            # A bound promise belongs to a possibly committed attempt. Only a
            # confirmed rollback or the worker's exact handoff can release it.
            if residents is not None and reservation is not None and reservation.attempt_id is None:
                residents.cancel(reservation)
            raise
        finally:
            for promise in promises:
                residents.release_launch(promise, owner)

    runtime = getattr(request.app.state, "resident_runtime", None)
    return await (runtime.own_operation(perform()) if runtime is not None else await_drained(perform()))


async def run_cancel_allowed(request):
    context = get_auth_context(request)
    if context is not None:
        return context.has_permission("runs", "cancel")
    permissions = await resolve_route_permissions_for_request(request, await get_current_user_from_request(request))
    return "runs:cancel" in permissions


async def dispatch_work_stop(request, work, *, actor, instance_id, work_id, expected_attempt_id=None):
    """Retry the recorded exact stop intent; a cancellation receipt is not settlement."""
    current = await work.get(actor=actor, instance_id=instance_id, work_id=work_id)
    attempt = current["attempt"]
    if expected_attempt_id is not None and (attempt is None or attempt["id"] != expected_attempt_id):
        return
    if not attempt or attempt["status"] != "stopping" or not attempt["run_id"]:
        return
    manager = get_run_manager(request)
    record = await manager.get(attempt["run_id"], raise_on_store_error=True)
    if record is None:
        return
    if record.thread_id != attempt["thread_id"] or (record.metadata or {}).get("agent_work_attempt_id") != attempt["id"]:
        raise AgentDenied("Work stop binding is unavailable")
    await manager.cancel(record.run_id)


async def contain_orphaned_work(app, recovered_runs):
    """Request an exact attempt recycle; maintenance owns physical closure."""
    from deerflow.persistence.agent_instances.model import AgentInstanceRow
    from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkRow

    runtime = getattr(app.state, "resident_runtime", None)
    if runtime is None:
        return
    for record in recovered_runs:
        try:
            instance_id = (record.metadata or {}).get("agent_instance_id")
            if not isinstance(instance_id, str):
                continue
            attachment_id = None
            async with app.state.agent_instances._sf() as session:
                parent = await session.get(AgentInstanceRow, instance_id)
                if parent is None or parent.home_id is None:
                    continue
                home_id = parent.home_id
                attempt_id = (record.metadata or {}).get("agent_work_attempt_id")
                if attempt_id is not None:
                    bound = (await session.execute(select(AgentWorkAttemptRow, AgentWorkRow.instance_id).join(AgentWorkRow, AgentWorkRow.id == AgentWorkAttemptRow.work_id).where(AgentWorkAttemptRow.id == attempt_id))).one_or_none()
                    if bound is None:
                        continue
                    attempt, owner = bound
                    if (owner, attempt.run_id, attempt.thread_id, attempt.requester_id) != (instance_id, record.run_id, record.thread_id, record.user_id):
                        continue
                    attachment_id = attempt.attachment_id
            rows = await runtime.inventory_runs()
            if any(row["run_id"] in runtime.foreign_run_blocks and row.get("metadata", {}).get("agent_instance_id") == instance_id for row in rows):
                continue
            if attachment_id is not None:
                state = await runtime.attachments.unfenced(home_id)
                if state is not None and state.id == attachment_id:
                    await runtime.registry.request_recycle(instance_id, home_id, attachment_id, "run-orphaned", defer=True)
            await runtime.settle(instance_id=instance_id, reason="run-orphaned", attachment_id=attachment_id)
        except Exception:
            logger.exception("agents: recovered run %s retains its pending Work containment", record.run_id)
    wake = getattr(app.state, "storage_recheck_wake", None)
    if recovered_runs and wake is not None:
        wake.set()
