"""Build a conversation's sandbox ahead of its first turn.

Mounted on the threads prefix (``POST /api/threads/{thread_id}/workspace/prewarm``).
The build itself is the sandbox provider's ``prewarm_async``; this module is
the route alone.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, BackgroundTasks, Request
from pydantic import BaseModel

from app.gateway.authz import require_permission
from app.gateway.utils import sanitize_log_param
from deerflow.runtime.user_context import get_effective_user_id
from deerflow.sandbox.sandbox_provider import get_sandbox_provider
from deerflow.utils.thread_id import ThreadId

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/threads", tags=["threads"])


class ThreadWorkspacePrewarmResponse(BaseModel):
    thread_id: str
    #: Whether a build was handed to the background. ``False`` is not an
    #: error: the first turn builds its own sandbox, as it always did.
    scheduled: bool
    reason: str | None = None


async def _prewarm_thread_workspace(prewarm, thread_id: str, user_id: str) -> None:
    """Background half of the route: a prewarm is never a surfaced failure."""
    try:
        parked = await prewarm(thread_id, user_id=user_id)
    except Exception:
        logger.warning(
            "Prewarm for thread %s failed; its first turn builds the sandbox instead",
            sanitize_log_param(thread_id),
            exc_info=True,
        )
        return
    if parked is None:
        logger.info("Prewarm for thread %s built nothing; its first turn builds the sandbox instead", sanitize_log_param(thread_id))


@router.post("/{thread_id}/workspace/prewarm", response_model=ThreadWorkspacePrewarmResponse, status_code=202)
@require_permission("threads", "write", owner_check=True)
async def prewarm_thread_workspace(thread_id: ThreadId, request: Request, background: BackgroundTasks) -> ThreadWorkspacePrewarmResponse:
    """Build the thread's sandbox now, while the person is still typing.

    The client calls this the moment it mints a new thread id. The container
    the first turn would build depends on ``(user, thread)`` and nothing the
    person is about to say, so building it here moves the cold start -- the
    container's create plus its readiness wait -- off the turn. The user id is
    the request's own, the same identity the run carries, so the parked
    container is the one the turn's acquisition looks for.

    Always answers 202: the build runs after the response and its outcome is
    the provider's log, never this call's. ``scheduled: false`` says the
    deployment's provider cannot prewarm, and costs nothing.
    """
    del request
    try:
        prewarm = getattr(get_sandbox_provider(), "prewarm_async", None)
    except Exception:
        logger.info("Sandbox provider unavailable for prewarm of thread %s", sanitize_log_param(thread_id), exc_info=True)
        return ThreadWorkspacePrewarmResponse(thread_id=thread_id, scheduled=False, reason="unavailable")
    if prewarm is None:
        return ThreadWorkspacePrewarmResponse(thread_id=thread_id, scheduled=False, reason="unsupported")
    user_id = get_effective_user_id()
    if not user_id:
        return ThreadWorkspacePrewarmResponse(thread_id=thread_id, scheduled=False, reason="anonymous")
    background.add_task(_prewarm_thread_workspace, prewarm, thread_id, user_id)
    return ThreadWorkspacePrewarmResponse(thread_id=thread_id, scheduled=True)
