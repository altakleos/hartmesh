"""Gateway adapter: existing run admission and cancellation own all execution."""

from app.gateway.authz import get_auth_context, resolve_route_permissions_for_request
from app.gateway.deps import get_current_user_from_request, get_run_manager
from deerflow.agent_instances.contract import AgentDenied


async def run_cancel_allowed(request):
    context = get_auth_context(request)
    if context is not None:
        return context.has_permission("runs", "cancel")
    permissions = await resolve_route_permissions_for_request(request, await get_current_user_from_request(request))
    return "runs:cancel" in permissions


async def dispatch_work_stop(request, work, *, actor, instance_id, work_id):
    """Retry the recorded exact stop intent; a cancellation receipt is not settlement."""
    current = await work.get(actor=actor, instance_id=instance_id, work_id=work_id)
    attempt = current["attempt"]
    if not attempt or attempt["status"] != "stopping" or not attempt["run_id"]:
        return
    manager = get_run_manager(request)
    record = await manager.get(attempt["run_id"], raise_on_store_error=True)
    if record is None:
        return
    if record.thread_id != attempt["thread_id"] or (record.metadata or {}).get("agent_work_attempt_id") != attempt["id"]:
        raise AgentDenied("Work stop binding is unavailable")
    await manager.cancel(record.run_id)
