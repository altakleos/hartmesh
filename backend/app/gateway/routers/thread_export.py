"""Download one conversation's transcript: what the page shows, as Markdown or JSON.

Mounted on the threads prefix (``GET /api/threads/{thread_id}/export``). The
reading and rendering are ``app.gateway.transcript``; this module is the route
alone.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Literal

from fastapi import APIRouter, HTTPException, Query, Request, Response

from app.gateway import transcript
from app.gateway.authz import require_permission
from app.gateway.deps import get_current_user, get_thread_store
from app.gateway.routers.threads import _CHECKPOINT_MODE_ERRORS, _checkpoint_mode_http_error
from app.gateway.utils import sanitize_log_param
from deerflow.utils.thread_id import ThreadId

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/threads", tags=["threads"])


@router.get("/{thread_id}/export")
@require_permission("threads", "read", owner_check=True)
async def export_thread(thread_id: ThreadId, request: Request, export_format: Literal["markdown", "json"] = Query(alias="format")) -> Response:
    """Download one conversation's transcript: what the page shows, as Markdown or JSON (``app.gateway.transcript``)."""
    record = await get_thread_store(request).get(thread_id)
    try:
        from app.gateway.agent_conversations import evidence_user_id

        conversation = await transcript.read_conversation(request, thread_id, record, user_id=await evidence_user_id(request, thread_id, await get_current_user(request)))
    except _CHECKPOINT_MODE_ERRORS as exc:
        raise _checkpoint_mode_http_error(exc, thread_id) from exc
    except Exception:
        logger.exception("Failed to read thread %s for export", sanitize_log_param(thread_id))
        raise HTTPException(status_code=500, detail="Failed to export thread")
    if conversation is None:
        raise HTTPException(status_code=404, detail=f"Thread {thread_id} not found")
    extension, media_type = transcript.FORMATS[export_format]
    exported_at = datetime.now(UTC)
    if export_format == "markdown":
        body = transcript.transcript_markdown(title=conversation.title, created_at=conversation.created_at, messages=conversation.messages, exported_at=exported_at)
    else:
        body = transcript.transcript_json(title=conversation.title, thread_id=thread_id, created_at=conversation.created_at, messages=conversation.messages, exported_at=exported_at)
    return Response(
        content=body.encode("utf-8"),
        media_type=media_type,
        headers={"Content-Disposition": transcript.content_disposition(transcript.transcript_stem(conversation.title), extension), "Cache-Control": "no-store"},
    )
