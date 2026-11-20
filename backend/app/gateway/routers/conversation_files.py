"""Pick presented files and attach an independent copy to a conversation."""

from pathlib import PurePosixPath

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from pydantic import BaseModel

from app.gateway.authz import require_permission
from app.gateway.conversation_file_import import FileSource, PresentedFileSelection, admit_presented_source, conversation_scope, read_presented, receipt_paths, require_scope
from app.gateway.conversation_file_upload import import_into_chat
from app.gateway.deps import get_config, get_run_manager
from app.gateway.routers.thread_runs import _parse_run_page_created_at
from deerflow.config.app_config import AppConfig
from deerflow.runtime.runs.schemas import RunStatus
from deerflow.runtime.runs.store.base import format_run_cursor_created_at
from deerflow.utils.thread_id import ThreadId

router = APIRouter(prefix="/api/threads", tags=["uploads"])


class FromConversationRequest(PresentedFileSelection):
    create_destination: bool = False


class PresentedFile(BaseModel):
    run_id: str
    path: str
    filename: str
    size: int | None
    modified: float | None
    available: bool


class PresentedFilesPage(BaseModel):
    files: list[PresentedFile]
    has_more: bool
    next_before_created_at: str | None
    next_before_run_id: str | None


class ImportedAttachment(BaseModel):
    filename: str
    size: int
    sha256: str
    virtual_path: str
    artifact_url: str
    source: FileSource
    original_filename: str | None = None
    markdown_file: str | None = None
    markdown_virtual_path: str | None = None
    markdown_artifact_url: str | None = None


@router.get("/{thread_id}/presented-files", response_model=PresentedFilesPage)
@require_permission("threads", "read", owner_check=True, require_existing=True)
@require_permission("runs", "read")
async def list_presented_files(
    thread_id: ThreadId, request: Request, response: Response, limit: int = Query(20, ge=1, le=50), before_created_at: str | None = Query(None, min_length=1), before_run_id: str | None = Query(None, min_length=1, max_length=256)
):
    response.headers["Cache-Control"] = "private, no-store"
    if (before_created_at is None) != (before_run_id is None):
        raise HTTPException(422, "Both run cursor fields are required together")
    scope = await conversation_scope(request, thread_id)
    await admit_presented_source(request, thread_id, scope)
    user_id = None if scope[0] == "agent" else scope[1]
    cursor = _parse_run_page_created_at(before_created_at) if before_created_at is not None else None
    try:
        records = await get_run_manager(request).list_by_thread(thread_id, user_id=user_id, limit=limit + 1, before_created_at=cursor, before_run_id=before_run_id, require_store=True, raise_on_store_error=True)
    except Exception:
        raise HTTPException(503, "Conversation files are temporarily unavailable") from None
    files = []
    for run in records[:limit]:
        if run.thread_id != thread_id or (user_id is not None and run.user_id != user_id) or run.status not in (RunStatus.success, RunStatus.error, RunStatus.timeout, RunStatus.interrupted) or getattr(run, "operation_kind", "run") != "run":
            continue
        for path in await receipt_paths(request, thread_id, run.run_id, scope):
            try:
                size, modified = await read_presented(request, thread_id, path)
            except HTTPException as exc:
                if exc.status_code != 404:
                    raise
                size, modified = None, None
            files.append({"run_id": run.run_id, "path": path, "filename": PurePosixPath(path).name, "size": size, "modified": modified, "available": size is not None})
    await require_scope(request, thread_id, scope)
    await admit_presented_source(request, thread_id, scope)
    last = records[limit - 1] if len(records) > limit else None
    return {"files": files, "has_more": last is not None, "next_before_created_at": format_run_cursor_created_at(last.created_at) if last else None, "next_before_run_id": last.run_id if last else None}


@router.post("/{thread_id}/uploads/from-conversation", response_model=ImportedAttachment, response_model_exclude_none=True)
@require_permission("threads", "write", owner_check=True)
@require_permission("threads", "read")
@require_permission("runs", "read")
async def import_presented_file(thread_id: ThreadId, body: FromConversationRequest, request: Request, config: AppConfig = Depends(get_config)):
    return await import_into_chat(request=request, thread_id=thread_id, source=body.as_source(), config=config, create_destination=body.create_destination)
