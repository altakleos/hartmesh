"""Files API: the person's own files, kept across conversations.

``/api/files`` lists, streams and removes the caller's files; they live under
``{base_dir}/users/{user_id}/files`` and every sandbox of that user mounts the
same directory at ``/mnt/user-data/files``, so what the person keeps here is
on the disk of their next conversation. ``POST /api/threads/{id}/files`` keeps
one of a conversation's files (an upload or an output) by copying its exact
bytes; the artifact stays where it was. The routes are per person: there is no
way to name another owner, and a trusted internal caller acts for the owner it
carries, as the memory router does.
"""

from __future__ import annotations

import asyncio
import logging
import mimetypes
import os
import stat
from pathlib import Path

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import FileResponse, Response
from pydantic import BaseModel, Field

from app.gateway.authz import require_permission
from app.gateway.internal_auth import get_trusted_internal_owner_user_id
from app.gateway.path_utils import resolve_thread_virtual_path
from app.gateway.routers.artifacts import ACTIVE_CONTENT_MIME_TYPES, _build_attachment_headers, _build_content_disposition, is_text_file_by_content
from deerflow.config.paths import VIRTUAL_PATH_PREFIX, get_paths, make_safe_user_id
from deerflow.files import UserFile, UserFileError, delete_user_file, keep_file, list_user_files, resolve_user_file
from deerflow.runtime.user_context import get_effective_user_id
from deerflow.utils.thread_id import ThreadId

logger = logging.getLogger(__name__)

router = APIRouter(tags=["files"])

#: What a conversation may keep: what the person gave it and what it made for
#: them. The workspace is scratch, and the person's files are already theirs.
_KEEPABLE_PREFIXES = (f"{VIRTUAL_PATH_PREFIX}/uploads/", f"{VIRTUAL_PATH_PREFIX}/outputs/")

__all__ = ["get_paths", "router"]


class UserFileInfo(BaseModel):
    """One of the person's files."""

    path: str
    name: str
    size: int
    modified: float
    virtual_path: str
    url: str

    @classmethod
    def of(cls, entry: UserFile) -> UserFileInfo:
        return cls(path=entry.path, name=entry.name, size=entry.size, modified=entry.modified, virtual_path=entry.virtual_path, url=entry.url)


class UserFileListResponse(BaseModel):
    files: list[UserFileInfo]
    count: int
    # The listing stopped at its ceiling; what it holds is a prefix, not the whole.
    truncated: bool = False


class DeleteUserFileResponse(BaseModel):
    success: bool
    message: str


class KeepFileRequest(BaseModel):
    """Keep one of this conversation's files in the caller's own files."""

    # The virtual path as the sandbox sees it, under uploads or outputs.
    path: str = Field(min_length=1, max_length=4096)
    # A folder in the person's files to put it in; the root when omitted.
    folder: str | None = Field(default=None, max_length=1024)


def _files_user_id(request: Request) -> str:
    """The owner these files belong to: the trusted internal owner, else the caller."""
    raw_owner = get_trusted_internal_owner_user_id(request)
    if raw_owner:
        return make_safe_user_id(raw_owner)
    return get_effective_user_id()


def _existing_regular_file(user_id: str, path: str) -> Path:
    """Worker-thread body: the host path of one of the person's files, or the HTTP reason it is not."""
    try:
        actual = resolve_user_file(user_id, path)
    except UserFileError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None
    try:
        metadata = os.lstat(actual)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"File not found: {path}") from None
    if stat.S_ISLNK(metadata.st_mode):
        raise HTTPException(status_code=400, detail=f"Not a file: {path}")
    if not stat.S_ISREG(metadata.st_mode):
        raise HTTPException(status_code=404, detail=f"File not found: {path}")
    return actual


def _response_plan(actual: Path, download: bool) -> tuple[bool, str | None]:
    """Worker-thread body: whether to force a download, and the media type."""
    mime_type, _ = mimetypes.guess_type(actual)
    if download or mime_type in ACTIVE_CONTENT_MIME_TYPES:
        return True, mime_type
    if mime_type is None and is_text_file_by_content(actual):
        mime_type = "text/plain"
    return False, mime_type


@router.get("/api/files", response_model=UserFileListResponse, summary="List My Files")
@require_permission("files", "read")
async def list_files(request: Request) -> UserFileListResponse:
    """Every file the caller has kept, across all their conversations."""
    entries, truncated = await asyncio.to_thread(list_user_files, _files_user_id(request))
    return UserFileListResponse(files=[UserFileInfo.of(entry) for entry in entries], count=len(entries), truncated=truncated)


@router.get("/api/files/{path:path}", summary="Get One Of My Files")
@require_permission("files", "read")
async def get_file(path: str, request: Request, download: bool = False) -> Response:
    """Stream one of the caller's files, inline where the browser can show it.

    Active content (HTML, XHTML, SVG) is always a download, as the artifact
    route does, so nothing generated runs in the application origin.
    """
    actual = await asyncio.to_thread(_existing_regular_file, _files_user_id(request), path)
    force_download, mime_type = await asyncio.to_thread(_response_plan, actual, download)
    if force_download:
        return FileResponse(path=actual, filename=actual.name, media_type=mime_type, headers=_build_attachment_headers(actual.name))
    return FileResponse(
        path=actual,
        media_type=mime_type,
        headers={"Content-Disposition": _build_content_disposition("inline", actual.name)},
    )


@router.delete("/api/files/{path:path}", response_model=DeleteUserFileResponse, summary="Remove One Of My Files")
@require_permission("files", "delete")
async def delete_file(path: str, request: Request) -> DeleteUserFileResponse:
    """Remove one of the caller's files. Folders stay."""
    user_id = _files_user_id(request)
    try:
        await asyncio.to_thread(delete_user_file, user_id, path)
    except UserFileError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"File not found: {path}") from None
    return DeleteUserFileResponse(success=True, message=f"Deleted {path}")


def _keepable_source(thread_id: str, virtual_path: str, user_id: str) -> Path:
    """Worker-thread body: the host file a conversation may keep, or the HTTP reason it may not."""
    normalized = "/" + virtual_path.lstrip("/")
    if not normalized.startswith(_KEEPABLE_PREFIXES):
        raise HTTPException(status_code=400, detail=f"Only files under {' or '.join(prefix.rstrip('/') for prefix in _KEEPABLE_PREFIXES)} can be kept")
    actual = resolve_thread_virtual_path(thread_id, normalized, user_id=user_id)
    try:
        metadata = os.lstat(actual)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"File not found: {virtual_path}") from None
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise HTTPException(status_code=400, detail=f"Not a file: {virtual_path}")
    return actual


@router.post("/api/threads/{thread_id}/files", response_model=UserFileInfo, status_code=201, summary="Keep A Conversation's File")
@require_permission("files", "write")
@require_permission("threads", "read", owner_check=True)
async def keep_thread_file(thread_id: ThreadId, body: KeepFileRequest, request: Request) -> UserFileInfo:
    """Copy one of this conversation's uploads or outputs into the caller's files.

    The exact bytes are copied; a name already taken is kept beside the new
    one with the next free ``_N`` suffix, nothing is overwritten, and the
    conversation's own file is untouched.
    """
    user_id = _files_user_id(request)
    source = await asyncio.to_thread(_keepable_source, thread_id, body.path, user_id)
    try:
        kept = await asyncio.to_thread(keep_file, user_id, source, name=Path(body.path).name, folder=body.folder)
    except UserFileError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None
    logger.info("Kept %s from thread %s as %s", body.path, thread_id, kept.path)
    return UserFileInfo.of(kept)
