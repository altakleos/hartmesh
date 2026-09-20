"""Shared API: what anyone at the company published, readable by everyone.

``/api/shared`` lists and streams the company's Shared area; it lives under
``{base_dir}/shared`` and every sandbox of every person mounts it read-only at
``/mnt/user-data/shared``. ``POST /api/shared/publish`` copies the exact bytes
of something the caller already has -- one of their own files, or one of a
conversation's uploads or outputs -- into Shared and records who published
it, when, and from where; a name already there keeps both, nothing is
overwritten. ``DELETE`` is the publisher's or an admin's, and leaves the
record with who removed it and when. The routes carry the same ``threads:*``
authorities as the person's own files, for the same reason: the authority
universe is capped, and a role that names threads names what is theirs to
read, keep and hand out.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import FileResponse, Response
from pydantic import BaseModel, Field

from app.gateway.authz import require_permission
from app.gateway.deps import get_shared_publications_repo, get_thread_store, get_user_repo, is_admin_user
from app.gateway.routers._file_http import acting_user_id, existing_regular_file, response_plan
from app.gateway.routers.artifacts import _build_attachment_headers, _build_content_disposition
from app.gateway.routers.files import _keepable_source
from deerflow.config.paths import USER_FILES_VIRTUAL_PREFIX, VIRTUAL_PATH_PREFIX
from deerflow.files import SharedFile, SharedFileError, list_shared_files, normalize_relative_path, publish_file, remove_shared_file, resolve_shared_file, resolve_user_file
from deerflow.utils.thread_id import validate_thread_id

logger = logging.getLogger(__name__)

router = APIRouter(tags=["shared"])

#: Nothing served from this directory may be sniffed into a type the browser would run.
_NOSNIFF = {"X-Content-Type-Options": "nosniff"}
_CONVERSATION_PREFIXES = (f"{VIRTUAL_PATH_PREFIX}/uploads/", f"{VIRTUAL_PATH_PREFIX}/outputs/")

__all__ = ["router"]


class SharedFileInfo(BaseModel):
    """One published file, with who put it there."""

    path: str
    name: str
    size: int
    modified: float
    virtual_path: str
    url: str
    # The publication record, when there is one: a file an operator placed by
    # hand is listed with no publisher rather than hidden.
    # Who put it here, as a colleague would recognise them. Null when the
    # record does not know (a file placed on the disk by hand) or when the
    # account behind the record is gone.
    published_by: str | None = None
    published_at: datetime | None = None
    from_thread_id: str | None = None
    # Whether the caller may take this one out: its publisher, or an admin.
    # Decided here, per caller, so the page never reasons about roles.
    can_remove: bool = False

    @classmethod
    def of(cls, entry: SharedFile, record: dict | None, *, can_remove: bool = False, publisher: str | None = None) -> SharedFileInfo:
        return cls(
            path=entry.path,
            name=entry.name,
            size=entry.size,
            modified=entry.modified,
            virtual_path=entry.virtual_path,
            url=entry.url,
            published_by=publisher,
            published_at=None if record is None else record.get("published_at"),
            from_thread_id=None if record is None else record.get("from_thread_id"),
            can_remove=can_remove,
        )


class SharedFileListResponse(BaseModel):
    files: list[SharedFileInfo]
    count: int
    truncated: bool = False


class PublishRequest(BaseModel):
    """Publish one of the caller's files, or one of a conversation's, to Shared."""

    # The virtual path as the sandbox sees it: under the person's files, or
    # under a conversation's uploads or outputs (then ``thread_id`` names it).
    path: str = Field(min_length=1, max_length=4096)
    thread_id: str | None = Field(default=None, max_length=128)
    # A folder in Shared to put it in; the root when omitted.
    folder: str | None = Field(default=None, max_length=1024)


class RemoveSharedFileResponse(BaseModel):
    success: bool
    message: str


def _published_file(path: str) -> Path:
    """Worker-thread body: the host path of one published file, or the HTTP reason it is not."""
    return existing_regular_file(lambda: resolve_shared_file(path), label=path)


async def _publisher_names(records: list[dict | None]) -> dict[str, str]:
    """A recognisable name for each distinct publisher in *records*.

    The record stores the user id, which means nothing to the colleague
    reading the page. Ids the users table cannot resolve are simply absent,
    so a listing still renders when an account is gone.
    """
    ids = {record["published_by"] for record in records if record and record.get("published_by")}
    if not ids:
        return {}
    repo = get_user_repo()
    if repo is None:
        return {}
    names: dict[str, str] = {}
    for user_id in ids:
        try:
            user = await repo.get_user_by_id(user_id)
        except Exception:
            logger.warning("Could not look up the publisher %s for the Shared listing", user_id, exc_info=True)
            continue
        if user is not None:
            names[user_id] = user.email
    return names


def _may_remove(record: dict | None, *, user_id: str, admin: bool) -> bool:
    """The publisher may take it back, and an admin may take anything back; a file with no record is an admin's."""
    if admin:
        return True
    return record is not None and record.get("published_by") == user_id


def _own_file_source(user_id: str, virtual_path: str) -> Path:
    """Worker-thread body: the host path of one of the caller's own files, or the HTTP reason it is not."""
    relative = virtual_path[len(USER_FILES_VIRTUAL_PREFIX) :].lstrip("/")
    # A folder offered as the source of a publish is a bad request, not a miss.
    return existing_regular_file(lambda: resolve_user_file(user_id, relative), label=virtual_path, non_regular_is_missing=False)


@router.get("/api/shared", response_model=SharedFileListResponse, summary="List Shared Files")
@require_permission("threads", "read")
async def list_shared(request: Request) -> SharedFileListResponse:
    """Everything anyone at the company published, with who and when."""
    repo = get_shared_publications_repo(request)
    user_id = acting_user_id(request)
    admin = await is_admin_user(request)
    entries, truncated = await asyncio.to_thread(list_shared_files)
    records = await repo.live_publications()
    names = await _publisher_names([records.get(entry.path) for entry in entries])
    files = []
    for entry in entries:
        record = records.get(entry.path)
        publisher = None if record is None else names.get(record.get("published_by", ""))
        files.append(SharedFileInfo.of(entry, record, can_remove=_may_remove(record, user_id=user_id, admin=admin), publisher=publisher))
    return SharedFileListResponse(files=files, count=len(entries), truncated=truncated)


@router.post("/api/shared/publish", response_model=SharedFileInfo, status_code=201, summary="Publish A File To Shared")
@require_permission("threads", "write")
async def publish(body: PublishRequest, request: Request) -> SharedFileInfo:
    """Copy one of the caller's files, or one of their conversation's, into Shared.

    The exact bytes are copied and the record says who, when and from where.
    A name already taken is kept beside the new one with the next free
    ``_N`` suffix; nothing is overwritten, and the source is untouched.
    """
    repo = get_shared_publications_repo(request)
    user_id = acting_user_id(request)
    normalized = "/" + body.path.lstrip("/")
    thread_id: str | None = None
    if normalized.startswith(USER_FILES_VIRTUAL_PREFIX + "/"):
        source = await asyncio.to_thread(_own_file_source, user_id, normalized)
    elif normalized.startswith(_CONVERSATION_PREFIXES):
        if body.thread_id is None:
            raise HTTPException(status_code=400, detail="A conversation's file needs its thread_id")
        try:
            thread_id = validate_thread_id(body.thread_id)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from None
        # The conversation must be the caller's, and must exist: this hands
        # its bytes to everyone at the company. ``get(user_id=...)`` is the
        # owner-filtered read, not ``check_access``, which also admits a row
        # whose owner is null ("shared / pre-auth data"). For a personal copy
        # that permissiveness is harmless; here it would let any signed-in
        # caller publish a legacy conversation's outputs company-wide.
        if await get_thread_store(request).get(thread_id, user_id=user_id) is None:
            raise HTTPException(status_code=404, detail="Thread not found")
        source = await asyncio.to_thread(_keepable_source, thread_id, normalized, user_id)
    else:
        raise HTTPException(status_code=400, detail=f"Only files under {USER_FILES_VIRTUAL_PREFIX}, {' or '.join(prefix.rstrip('/') for prefix in _CONVERSATION_PREFIXES)} can be published")
    try:
        published = await asyncio.to_thread(publish_file, source, name=Path(normalized).name, folder=body.folder)
    except SharedFileError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None
    try:
        record = await repo.record_publication(
            path=published.path,
            size=published.size,
            sha256=published.sha256 or "",
            published_by=user_id,
            from_thread_id=thread_id,
            from_path=normalized,
        )
    except Exception:
        # The bytes are already in Shared. A file nobody can be shown as the
        # publisher of is a file only an admin can remove and nobody can
        # account for, so the copy goes back out rather than outliving its
        # record. (A path too long for the column is one way here; so is the
        # database being briefly unavailable.)
        logger.exception("Could not record the publication of %s; taking the copy back out of Shared", published.path)
        try:
            await asyncio.to_thread(remove_shared_file, published.path)
        except Exception:
            logger.exception("Could not take %s back out of Shared; it is there with no publication record", published.path)
        raise HTTPException(status_code=503, detail="Could not record the publication; nothing was shared") from None
    logger.info("Published %s to Shared as %s", normalized, published.path)
    return SharedFileInfo.of(published, record, can_remove=True, publisher=(await _publisher_names([record])).get(user_id))


@router.get("/api/shared/{path:path}", summary="Get One Shared File")
@require_permission("threads", "read")
async def get_shared_file(path: str, request: Request, download: bool = False) -> Response:
    """Stream one published file, inline where the browser can show it.

    Active content (HTML, XHTML, SVG) is always a download, as the artifact
    route does, so nothing published runs in the application origin.
    """
    actual = await asyncio.to_thread(_published_file, path)
    force_download, mime_type = await asyncio.to_thread(response_plan, actual, download)
    if force_download:
        return FileResponse(path=actual, filename=actual.name, media_type=mime_type, headers=_build_attachment_headers(actual.name, _NOSNIFF))
    return FileResponse(path=actual, media_type=mime_type, headers={"Content-Disposition": _build_content_disposition("inline", actual.name), **_NOSNIFF})


@router.delete("/api/shared/{path:path}", response_model=RemoveSharedFileResponse, summary="Remove One Shared File")
@require_permission("threads", "delete")
async def remove_shared(path: str, request: Request) -> RemoveSharedFileResponse:
    """Take one file out of Shared. The publisher may, and an admin may; the record stays."""
    repo = get_shared_publications_repo(request)
    user_id = acting_user_id(request)
    await asyncio.to_thread(_published_file, path)
    # The record is keyed by the path the store settled on, not the spelling
    # the caller sent: `/Reports//r.pdf` names the same file as `Reports/r.pdf`
    # and must find the same record, or the publisher is refused their own file
    # and an admin's removal leaves the row live forever.
    try:
        relative = normalize_relative_path(path)
    except SharedFileError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None
    record = await repo.live_publication(relative)
    if not _may_remove(record, user_id=user_id, admin=await is_admin_user(request)):
        raise HTTPException(status_code=403, detail="Only the person who published this, or an admin, can remove it")
    try:
        await asyncio.to_thread(remove_shared_file, relative)
    except SharedFileError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"File not found: {path}") from None
    if record is not None:
        await repo.record_removal(record["publication_id"], removed_by=user_id)
    logger.info("Removed %s from Shared", relative)
    return RemoveSharedFileResponse(success=True, message=f"Removed {relative}")
