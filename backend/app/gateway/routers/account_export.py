"""Download all my data: the caller's own export, from their own browser session.

There is no user id anywhere in these routes: every one acts for the person
the session belongs to, so an administrator, a personal access token or an
internal caller cannot start, follow or download anyone's export
(``require_session_source``), and a person turned off cannot start one.
The archive and its limits are ``app.gateway.account_export``.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from fastapi.responses import JSONResponse, StreamingResponse

from app.gateway.account_export import AccountExportService, ExportRefused
from app.gateway.auth.mode import require_live_account
from app.gateway.routers.auth import require_session_source

router = APIRouter(prefix="/api/account/export", tags=["account-export"], dependencies=[Depends(require_session_source)])

_CHUNK_BYTES = 1024 * 1024


def _service(request: Request) -> AccountExportService:
    service = getattr(request.app.state, "account_export", None)
    if service is None:
        # More than one Gateway process serves this deployment (``runs_in_this_process``).
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Downloading all your data is not available on this deployment")
    return service


def _person(request: Request) -> Any:
    user = getattr(request.state, "user", None)
    if user is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentication required")
    require_live_account(user)
    return user


@router.post("", status_code=status.HTTP_202_ACCEPTED)
async def start_export(request: Request) -> Any:
    """Start preparing the caller's export, or answer with the one they already have."""
    person = _person(request)
    try:
        job = _service(request).start(person)
    except ExportRefused as exc:
        return JSONResponse(status_code=status.HTTP_429_TOO_MANY_REQUESTS, content={"detail": exc.detail, "code": exc.code}, headers={"Retry-After": "60"})
    return job.document()


@router.get("")
async def export_status(request: Request) -> Any:
    """How the caller's export is going, and its parts once ready."""
    person = _person(request)
    job = _service(request).status(str(person.id))
    if job is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No export in progress")
    return job.document()


class _PartResponse(StreamingResponse):
    """A part's download, which ends the download however the response ends, even before its first byte."""

    def __init__(self, content: Any, *, finish: Callable[[], None], **kwargs: Any) -> None:
        super().__init__(content, **kwargs)
        self._finish = finish

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        try:
            await super().__call__(scope, receive, send)
        finally:
            self._finish()


@router.get("/parts/{number}")
async def download_part(number: int, request: Request) -> StreamingResponse:
    """One part of the caller's prepared export; it can be downloaded again until the export is deleted."""
    person = _person(request)
    service = _service(request)
    download = service.open_part(str(person.id), number)
    if download is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No such part to download")
    try:
        handle = await asyncio.to_thread(download.path.open, "rb")
    except BaseException as exc:
        service.close_part(download, complete=False)
        if isinstance(exc, FileNotFoundError):
            # Deleted since it was found: discarded, or its time ran out.
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No such part to download") from None
        raise
    complete = finished = False

    async def _stream():
        nonlocal complete
        while chunk := await asyncio.to_thread(handle.read, _CHUNK_BYTES):
            yield chunk
        # Reached only once every byte went out: a dropped download keeps its part.
        complete = True

    def _finish() -> None:
        nonlocal finished
        if not finished:
            finished = True
            handle.close()
            service.close_part(download, complete=complete)

    count = len(download.job.parts)
    stamp = datetime.now(UTC).strftime("%Y-%m-%d")
    suffix = f"-part-{number}-of-{count}" if count > 1 else ""
    return _PartResponse(
        _stream(),
        finish=_finish,
        media_type="application/zip",
        headers={
            "Content-Disposition": f'attachment; filename="account-export-{stamp}{suffix}.zip"',
            "Content-Length": str(download.size),
            "Cache-Control": "no-store",
        },
    )


@router.delete("", status_code=status.HTTP_204_NO_CONTENT)
async def discard_export(request: Request) -> Response:
    """Stop the caller's export if it is being prepared, and delete it."""
    person = _person(request)
    _service(request).discard(str(person.id))
    return Response(status_code=status.HTTP_204_NO_CONTENT)
