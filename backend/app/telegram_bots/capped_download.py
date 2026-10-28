"""Bounded file bodies on the managed bot's existing normal request client."""

from __future__ import annotations

import asyncio
import io
import traceback
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Literal

from telegram.request import HTTPXRequest

from deerflow.utils.file_io import await_drained


class CappedDownloadError(Exception):
    """Closed diagnostics; never retain remote bodies, URLs or exception text."""

    def __init__(self, reason: Literal["too_large", "overrun", "not_saved", "status"], *, status: int | None = None):
        if reason not in {"too_large", "overrun", "not_saved", "status"} or (type(status) is not int or not 100 <= status <= 599 if reason == "status" else status is not None):
            raise ValueError("invalid-capped-download-error") from None
        super().__init__("capped-download-failed")
        self.reason, self.status = reason, status


@dataclass(eq=False)
class _FailedClose:
    stream: object
    release: Callable[[], Awaitable[None]] | None
    stream_closed: bool = False


class ManagedHTTPXRequest(HTTPXRequest):
    """The adapter owns/drains whole getFile+read operations before shutdown.

    Only failed-close custody is retained here. Save a release callback once and
    query its identity after the operation has drained, even after cancellation.
    Successful reads close their response before transferring bytes and never
    invoke that callback. Failed close transfers its capacity release to shutdown.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._failed_closes: list[_FailedClose] = []
        self._cleanup_lock = asyncio.Lock()
        self._download_stopping = False

    def retains_failed_close(self, callback) -> bool:
        return any(record.release is callback for record in self._failed_closes)

    async def initialize(self) -> None:
        if self._failed_closes:
            raise CappedDownloadError("not_saved") from None
        await super().initialize()
        self._download_stopping = False

    async def _close_response(self, response, release):
        failed = False
        try:
            await response.aclose()
        except BaseException as error:
            traceback.clear_frames(error.__traceback__)
            failed = True
        if failed:
            # HTTPX marks is_closed before awaiting its stream. Repeating only
            # Response.aclose would skip a stream whose first close failed.
            self._failed_closes.append(_FailedClose(response.stream, release))
            raise CappedDownloadError("not_saved") from None

    async def read_file_capped(self, url, *, ceiling: int, max_bytes: int, before_dispatch: Callable[[], Awaitable[None]], on_failed_close: Callable[[], Awaitable[None]] | None = None) -> bytes:
        if type(ceiling) is not int or ceiling < 0 or type(max_bytes) is not int or max_bytes <= 0 or not callable(before_dispatch) or on_failed_close is not None and not callable(on_failed_close):
            raise CappedDownloadError("not_saved") from None
        if ceiling > max_bytes:
            raise CappedDownloadError("too_large") from None
        response = None
        request = chunk = None
        buffer = io.BytesIO()
        failed = False
        try:
            try:
                await before_dispatch()
                if self._download_stopping or self._client.is_closed:
                    raise CappedDownloadError("not_saved")
                request = self._client.build_request("GET", url, headers={"Accept-Encoding": "identity", "User-Agent": self.USER_AGENT})
                response = await self._client.send(request, stream=True, follow_redirects=False)
                if response.status_code != 200:
                    raise CappedDownloadError("status", status=response.status_code)
                if response.headers.get("content-encoding", "identity").strip().lower() != "identity":
                    raise CappedDownloadError("not_saved")
                length = response.headers.get("content-length")
                if length is not None:
                    if len(length) > 20 or not length.isascii() or not length.isdecimal():
                        raise CappedDownloadError("not_saved")
                    length = int(length)
                    if length > max_bytes:
                        raise CappedDownloadError("too_large")
                    if length > ceiling:
                        raise CappedDownloadError("overrun")
                size = 0
                # Raw public stream avoids both decompression and aiter_raw's
                # implicit close, whose failure would otherwise be ambiguous.
                async for chunk in response.stream:
                    size += len(chunk)
                    if size > max_bytes:
                        raise CappedDownloadError("too_large")
                    if size > ceiling:
                        raise CappedDownloadError("overrun")
                    buffer.write(chunk)
                if length is not None and length != size:
                    raise CappedDownloadError("not_saved")
            except (asyncio.CancelledError, CappedDownloadError):
                raise
            except Exception as error:
                traceback.clear_frames(error.__traceback__)
                failed = True
            finally:
                if response is not None:
                    await await_drained(self._close_response(response, on_failed_close))
            if failed:
                raise CappedDownloadError("not_saved") from None
            # BytesIO exposes its immutable buffer without a retained list of
            # chunks plus a second joined whole-file allocation.
            return buffer.getvalue()
        finally:
            request = response = chunk = None
            buffer.close()

    async def _shutdown_downloads(self):
        async with self._cleanup_lock:
            for record in tuple(self._failed_closes):
                failed = False
                try:
                    if not record.stream_closed:
                        await record.stream.aclose()
                        record.stream_closed = True
                    if record.release is not None:
                        await record.release()
                except BaseException as error:
                    traceback.clear_frames(error.__traceback__)
                    failed = True
                if failed:
                    raise CappedDownloadError("not_saved") from None
                self._failed_closes.remove(record)
            await super().shutdown()

    async def shutdown(self) -> None:
        self._download_stopping = True
        await await_drained(self._shutdown_downloads())
