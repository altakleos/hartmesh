"""Instance conversation authority is mandatory alongside route ceilings."""

from collections.abc import Mapping

from fastapi import HTTPException

from app.gateway.request_path import get_request_route_path
from app.gateway.routers.spaces import _actor
from deerflow.agent_instances.contract import AgentDenied, AgentPermission
from deerflow.agent_instances.conversations import AgentConversations


def _authority(request):
    # Minimal legacy Request fixtures need not have an ASGI app entry. Real
    # Gateway requests always do; a property lookup on missing scope raises.
    scope = getattr(request, "scope", None)
    app = scope.get("app") if isinstance(scope, Mapping) else getattr(request, "app", None)
    return getattr(getattr(app, "state", None), "agent_conversations", None)


async def require_conversation(request, thread_id, *, permission=AgentPermission.INSPECT):
    authority = _authority(request)
    if not isinstance(authority, AgentConversations) or await authority.binding(thread_id) is None:
        return False
    actor = await _actor(request)
    if not await authority.allowed(actor=actor, thread_id=thread_id, permission=permission):
        raise HTTPException(404, "Agent conversation is unavailable")
    request.state.agent_conversation_thread_id = thread_id
    return True


async def admit_request(request):
    """Cover all thread/evidence routes, including routes without owner decorators."""
    path = get_request_route_path(request)
    parts = path.strip("/").split("/")
    if parts[:1] == ["threads"]:
        parts.insert(0, "api")
    if len(parts) < 3 or parts[:2] != ["api", "threads"] or parts[2] in {"search", "count"}:
        return
    thread_id = parts[2]
    permission = AgentPermission.INSPECT if request.method in {"GET", "HEAD"} else AgentPermission.MANAGE
    suffix = parts[3:]
    if request.method == "POST" and suffix in (["runs"], ["runs", "stream"], ["runs", "wait"]):
        permission = AgentPermission.USE
    if (request.method == "POST" and len(suffix) == 3 and suffix[0] == "runs" and suffix[2] == "cancel") or (len(suffix) == 3 and suffix[0] == "runs" and suffix[2] == "stream"):
        permission = AgentPermission.INSPECT
    if not await require_conversation(request, thread_id, permission=permission):
        return
    if permission == AgentPermission.USE:
        await require_conversation(request, thread_id)
    # These legacy paths resolve requester-owned directories, browser sessions,
    # external publications or scheduler identities. They are not instance views.
    if suffix and suffix[0] in {"files", "uploads", "workspace", "browser", "share", "publish", "publications", "scheduled-tasks"}:
        raise HTTPException(501, "Use the agent's Home Space for instance resources")
    if (suffix[:1] == ["artifacts"] and request.method not in {"GET", "HEAD"}) or suffix[-2:] == ["artifacts", "archive"]:
        raise HTTPException(501, "Use the agent's Home Space for editing and downloads")


async def require_run_cancellation(request, record):
    if not await require_conversation(request, record.thread_id):
        return
    actor = await _actor(request)
    permission = AgentPermission.USE if record.user_id == actor.subject_id else AgentPermission.MANAGE
    await require_conversation(request, record.thread_id, permission=permission)


def guard_response(request, response):
    thread_id = getattr(request.state, "agent_conversation_thread_id", None)
    iterator = getattr(response, "body_iterator", None)
    if thread_id is None or iterator is None or not response.headers.get("content-type", "").startswith("text/event-stream"):
        return response

    async def admitted_body():
        try:
            async for chunk in iterator:
                try:
                    await require_conversation(request, thread_id)
                except HTTPException:
                    return
                yield chunk
        finally:
            close = getattr(iterator, "aclose", None)
            if close is not None:
                await close()

    response.body_iterator = admitted_body()
    return response


async def execution_for_run(request, thread_id):
    authority = _authority(request)
    if not isinstance(authority, AgentConversations) or await authority.binding(thread_id) is None:
        return None
    try:
        return await authority.execution(actor=await _actor(request), thread_id=thread_id)
    except AgentDenied:
        raise HTTPException(404, "Agent conversation is unavailable") from None


async def evidence_user_id(request, thread_id, fallback):
    # None only removes the requester attribution filter after mandatory
    # current conversation admission. It never authenticates the reader.
    return None if await require_conversation(request, thread_id) else fallback


async def inspection_for_read(request, thread_id, *, user_id=None):
    authority = _authority(request)
    if not isinstance(authority, AgentConversations) or await authority.binding(thread_id) is None:
        return None
    from deerflow.spaces.contract import PrincipalRef

    actor = PrincipalRef("human", user_id) if user_id is not None else await _actor(request)
    try:
        return await authority.inspection(actor=actor, thread_id=thread_id)
    except AgentDenied:
        raise HTTPException(404, "Agent conversation is unavailable") from None


async def reject_legacy_binding(request, thread_id):
    if await require_conversation(request, thread_id):
        raise HTTPException(501, "This legacy feature has no instance adapter; use Home Space resources")
