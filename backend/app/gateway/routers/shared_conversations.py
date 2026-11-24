"""Authenticated access to frozen company conversation publications."""

from typing import Annotated

from fastapi import APIRouter, HTTPException, Query, Request, Response
from pydantic import Field

from app.gateway.authz import require_permission
from app.gateway.conversation_snapshots import SharedSnapshot, SnapshotFolder, read_collection, read_snapshot
from app.gateway.snapshot_file_response import SnapshotMemberResponse
from app.gateway.snapshot_manifest import Digest, SnapshotModel
from app.gateway.snapshot_publication import ShareRequest, draft, publish_snapshot, remove_snapshot
from deerflow.utils.thread_id import ThreadId

router = APIRouter(prefix="/api/shared-conversations", tags=["shared"])


class UndoRequest(SnapshotModel):
    publication_ids: list[Annotated[str, Field(min_length=1, max_length=128)]] = Field(min_length=1, max_length=66)
    undo_receipt: Digest


@router.get("/draft")
@require_permission("threads", "write")
@require_permission("threads", "read")
@require_permission("runs", "read")
async def share_draft(thread_id: ThreadId, request: Request, response: Response):
    response.headers["Cache-Control"] = "private, no-store"
    return await draft(request, thread_id)


@router.post("")
@require_permission("threads", "write")
@require_permission("threads", "read")
@require_permission("runs", "read")
async def share(body: ShareRequest, request: Request, response: Response):
    result = await publish_snapshot(request, body)
    response.status_code = 200 if result["already_shared"] else 201
    return result


@router.get("")
async def snapshots(request: Request, response: Response, limit: int = Query(100, ge=1, le=200), before: str | None = Query(None, min_length=1, max_length=128)):
    response.headers["Cache-Control"] = "private, no-store"
    return await read_collection(request=request, limit=limit, before=before)


@router.get("/{publication_id}", response_model=SharedSnapshot)
async def snapshot(publication_id: str, request: Request, response: Response):
    response.headers["Cache-Control"] = "private, no-store"
    detail = await read_snapshot(request=request, publication_id=publication_id, allow_folder=True)
    if isinstance(detail, SnapshotFolder):
        raise HTTPException(404, {"code": "snapshot-folder-available", **detail.model_dump()}, headers={"Cache-Control": "private, no-store"})
    return detail


@router.get("/{publication_id}/members/{member_id}", response_class=Response)
@require_permission("threads", "read")
async def snapshot_member(publication_id: str, member_id: str, request: Request, download: bool = False):
    return SnapshotMemberResponse(request, publication_id, member_id, download)


@router.delete("/{publication_id}")
@require_permission("threads", "delete")
async def remove(publication_id: str, request: Request):
    return await remove_snapshot(request, publication_id)


@router.post("/{publication_id}/undo")
@require_permission("threads", "delete")
async def undo(publication_id: str, body: UndoRequest, request: Request):
    return await remove_snapshot(request, publication_id, undo=body)
