"""What the two file routers do identically: who is asking, and how a path becomes a file.

``/api/files`` (the person's own) and ``/api/shared`` (the company's) differ
in who may write and who may remove. They do not differ in how a caller is
identified, how a virtual path is turned into a host path, or how that file
is handed back to the browser — so those live here once, and both routers
import them. The filesystem layer underneath is shared the same way, in
:mod:`deerflow.files.store`.
"""

from __future__ import annotations

import asyncio
import mimetypes
import os
import stat
from collections.abc import Callable
from pathlib import Path
from secrets import token_hex

from fastapi import HTTPException, Request
from starlette.datastructures import MutableHeaders
from starlette.responses import FileResponse
from starlette.types import Receive, Scope, Send

from app.gateway.internal_auth import get_trusted_internal_owner_user_id
from app.gateway.routers.artifacts import _build_content_disposition
from deerflow.config.paths import make_safe_user_id
from deerflow.files.store import StoreError, open_regular_source
from deerflow.runtime.user_context import get_effective_user_id
from deerflow.utils.file_io import await_drained
from deerflow.utils.text_detection import _is_active_content_mime_type

__all__ = ["DescriptorFileResponse", "acting_user_id", "existing_regular_file"]


def acting_user_id(request: Request) -> str:
    """Who is acting: the trusted internal owner, else the caller."""
    raw_owner = get_trusted_internal_owner_user_id(request)
    if raw_owner:
        return make_safe_user_id(raw_owner)
    return get_effective_user_id()


def existing_regular_file(resolve: Callable[[], Path], *, label: str, non_regular_is_missing: bool = True) -> Path:
    """Worker-thread body: the host path *resolve* names, or the HTTP reason it is not a file.

    A link is always ``400`` — the sandbox can plant one, and following it is
    how a file area leaks. What a directory means depends on the question:
    asking to *read* one is a miss (``404``), while offering one as the
    source of a publish is a bad request (``400``), which
    *non_regular_is_missing* selects.
    """
    try:
        actual = resolve()
    except StoreError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None
    try:
        metadata = os.lstat(actual)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"File not found: {label}") from None
    if stat.S_ISLNK(metadata.st_mode):
        raise HTTPException(status_code=400, detail=f"Not a file: {label}")
    if not stat.S_ISREG(metadata.st_mode):
        if non_regular_is_missing:
            raise HTTPException(status_code=404, detail=f"File not found: {label}")
        raise HTTPException(status_code=400, detail=f"Not a file: {label}")
    return actual


class DescriptorFileResponse(FileResponse):
    """Serve the file opened through no-follow directory descriptors.

    Opening happens inside ASGI ownership, not while constructing a response
    which might never be sent. Metadata, sniffing and all ranges share this
    descriptor. Draining each worker before close prevents an abandoned read
    from using a descriptor number that another request has since acquired.
    """

    def __init__(self, path: Path, *, download: bool = False) -> None:
        super().__init__(path, media_type="application/octet-stream", headers={"X-Content-Type-Options": "nosniff"})
        self._download = download
        self._fd: int | None = None

    def _prepare(self) -> None:
        try:
            self._fd = open_regular_source(Path(self.path))
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail=f"File not found: {Path(self.path).name}") from None
        except StoreError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from None
        # Publish ownership before any subsequent operation can fail. The
        # enclosing ASGI finally also closes after preparation failure.
        self.stat_result = os.fstat(self._fd)
        self.set_stat_headers(self.stat_result)
        mime_type, _ = mimetypes.guess_type(self.path)
        force_download = self._download or _is_active_content_mime_type(mime_type)
        if mime_type is None and not force_download and b"\x00" not in os.read(self._fd, 8192):
            mime_type = "text/plain"
        self.media_type = mime_type or "application/octet-stream"
        self.headers["content-type"] = self.media_type + (f"; charset={self.charset}" if self.media_type.startswith("text/") else "")
        self.headers["content-disposition"] = _build_content_disposition("attachment" if force_download else "inline", Path(self.path).name)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        try:
            # _prepare stores the fd itself: cancellation cannot discard an
            # acquired descriptor as an unobserved worker return value.
            await await_drained(asyncio.to_thread(self._prepare))
            await super().__call__(scope, receive, send)
        finally:
            if self._fd is not None:
                fd, self._fd = self._fd, None
                await await_drained(asyncio.to_thread(os.close, fd))

    def _read(self, offset: int, size: int) -> bytes:
        assert self._fd is not None
        os.lseek(self._fd, offset, os.SEEK_SET)
        return os.read(self._fd, size)

    async def _send_range(self, send: Send, start: int, end: int) -> None:
        while start < end:
            chunk = await await_drained(asyncio.to_thread(self._read, start, min(self.chunk_size, end - start)))
            if not chunk:
                # An agent can truncate an open regular file. Never spin on
                # EOF while attempting to finish the original byte range.
                break
            start += len(chunk)
            await send({"type": "http.response.body", "body": chunk, "more_body": True})

    async def _handle_simple(self, send: Send, send_header_only: bool, send_pathsend: bool) -> None:
        await send({"type": "http.response.start", "status": self.status_code, "headers": self.raw_headers})
        if not send_header_only:
            assert self.stat_result is not None
            await self._send_range(send, 0, self.stat_result.st_size)
        # Never delegate pathsend: that would reopen an untrusted path.
        await send({"type": "http.response.body", "body": b"", "more_body": False})

    async def _handle_single_range(self, send: Send, start: int, end: int, file_size: int, send_header_only: bool) -> None:
        headers = MutableHeaders(raw=list(self.raw_headers))
        headers["content-range"] = f"bytes {start}-{end - 1}/{file_size}"
        headers["content-length"] = str(end - start)
        await send({"type": "http.response.start", "status": 206, "headers": headers.raw})
        if not send_header_only:
            await self._send_range(send, start, end)
        await send({"type": "http.response.body", "body": b"", "more_body": False})

    async def _handle_multiple_ranges(self, send: Send, ranges: list[tuple[int, int]], file_size: int, send_header_only: bool) -> None:
        boundary = token_hex(13)
        content_length, header = self.generate_multipart(ranges, boundary, file_size, self.headers["content-type"])
        headers = MutableHeaders(raw=list(self.raw_headers))
        headers["content-type"] = f"multipart/byteranges; boundary={boundary}"
        headers["content-length"] = str(content_length)
        await send({"type": "http.response.start", "status": 206, "headers": headers.raw})
        if send_header_only:
            await send({"type": "http.response.body", "body": b"", "more_body": False})
            return
        for start, end in ranges:
            await send({"type": "http.response.body", "body": header(start, end), "more_body": True})
            await self._send_range(send, start, end)
            await send({"type": "http.response.body", "body": b"\r\n", "more_body": True})
        await send({"type": "http.response.body", "body": f"--{boundary}--".encode("latin-1"), "more_body": False})
