"""One run's delivery verdict, for a client that was not listening when it was given.

Mounted on the threads prefix (``GET /api/threads/{thread_id}/runs/{run_id}/delivery``).
The projection is ``deerflow.runtime.runs.delivery``; this module is the route
alone.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request

from app.gateway.authz import require_permission
from app.gateway.deps import get_run_event_store, get_run_manager
from app.gateway.routers.thread_runs import _require_run_visible_to_scope, _run_scope_user_id
from deerflow.runtime.runs.delivery import get_run_delivery_response
from deerflow.utils.thread_id import ThreadId

router = APIRouter(prefix="/api/threads", tags=["runs"])


@router.get("/{thread_id}/runs/{run_id}/delivery")
@require_permission("runs", "read", owner_check=True)
async def get_run_delivery(thread_id: ThreadId, run_id: str, request: Request) -> dict:
    """Return the delivery verdict recorded for one run.

    A run that produced files and handed none of them over ends as an error
    whose stop reason says so, and a live client hears which files in one
    ``custom`` frame. That frame is page-local state: a client that reloads,
    gaps, or never asked for ``custom`` has no way back to it. This is that
    way back, read from the run's own record and its ``run.delivery`` receipt.
    """
    await _require_run_visible_to_scope(run_id, thread_id, request)
    record = await get_run_manager(request).get(run_id, user_id=await _run_scope_user_id(request, thread_id))
    if record is None or record.thread_id != thread_id:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")
    return await get_run_delivery_response(get_run_event_store(request), thread_id, run_id, stop_reason=record.stop_reason)
