"""Every open connection is held under its owner, so turning the owner off closes it.

A request is refused at its next read of the account; a connection that
authenticated once is not read again. An SSE stream, a streaming download
and the browser WebSocket would otherwise keep flowing after the account was
turned off, until the client dropped them. This middleware registers each
connection under the account that opened it -- once it starts its response,
or accepts its socket, by which time the authenticating layer has stamped
``state.user`` -- and releases it when it finishes. The refusal watch
(``app.gateway.refusal_watch``) ends what a refused owner holds.

Ending one looks, to the application, exactly like the client going away:
its ``receive`` answers with a disconnect, which every streaming response
already listens for and unwinds from through its own cleanup, and nothing it
sends afterwards leaves the process. To the client it is a cut: an HTTP
body is never completed, so the server drops the connection -- a download
cut short never reads as a finished one -- and a socket is closed with
``4401`` (sign in again). An application that does not read ``receive`` is
cancelled after :data:`UNWIND_GRACE_SECONDS`.

A route that authenticates the connection itself -- a WebSocket route, which
``AuthMiddleware`` does not see -- stamps ``state.user`` before it accepts;
``routers.browser._authenticate_ws`` does it for every socket that uses it.
A connection with no ``state.user`` is not held and not reported.

It is the outermost middleware on purpose: a ``BaseHTTPMiddleware`` outside
it would complete a response whose application returned mid-stream, and the
cut would reach the client as a clean end.

A connection is judged by the later of two reads of the account: its own,
made after it arrived, and the refusal watch's last look. One that arrived
after that look read the refusals is not cut by it, so someone turned back
on is not cut by a look made before they were.

A request that has not started its response is not held: it is either
refused at its own read of the account, or it will hold itself once it
starts streaming, and cutting a write mid-way is not what a refusal means.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time

import anyio
from starlette.datastructures import Headers
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from deerflow.runtime.owner_holdings import Holding, OwnerHoldings, get_owner_holdings

logger = logging.getLogger(__name__)

#: The close code a refused socket gets: the credential no longer holds.
REFUSED_SOCKET_CLOSE_CODE = 4401

#: How long an ended connection's application has to unwind as from a client
#: disconnect before it is cancelled where it stands.
UNWIND_GRACE_SECONDS = 5.0


def _owner(scope: Scope) -> str | None:
    user = (scope.get("state") or {}).get("user")
    owner = getattr(user, "id", None)
    return str(owner) if owner is not None else None


def _surface(scope: Scope, message: Message) -> str:
    if scope["type"] == "websocket":
        return "websockets"
    content_type = Headers(raw=message.get("headers") or []).get("content-type", "")
    return "sse_streams" if content_type.startswith("text/event-stream") else "downloads"


class OwnerConnectionsMiddleware:
    """Hold each authenticated connection under its owner while it is open."""

    def __init__(self, app: ASGIApp, holdings: OwnerHoldings | None = None, *, unwind_grace_seconds: float = UNWIND_GRACE_SECONDS) -> None:
        self.app = app
        self._holdings = holdings
        self._grace = unwind_grace_seconds

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] not in ("http", "websocket"):
            await self.app(scope, receive, send)
            return
        holdings = self._holdings or get_owner_holdings()
        # No later than any read of the account made for this connection.
        arrived_at = time.monotonic()
        loop = asyncio.get_running_loop()
        ended = asyncio.Event()
        holding: Holding | None = None
        # One read of the real ``receive`` at a time, kept across calls and
        # never cancelled while the connection is open: cancelling the
        # server's socket read mid-frame closes the socket as a server error.
        reading: asyncio.Future[Message] | None = None
        disconnect: Message = {"type": "websocket.disconnect", "code": REFUSED_SOCKET_CLOSE_CODE} if scope["type"] == "websocket" else {"type": "http.disconnect"}

        with anyio.CancelScope() as cancel_scope:

            def _on_end() -> None:
                ended.set()
                loop.call_later(self._grace, cancel_scope.cancel)

            def _end() -> None:
                # The watch may run on another thread; the connection belongs to this loop.
                loop.call_soon_threadsafe(_on_end)

            async def held_receive() -> Message:
                nonlocal reading
                if ended.is_set():
                    return disconnect
                if reading is None:
                    reading = asyncio.ensure_future(receive())
                read = reading
                refusing = asyncio.ensure_future(ended.wait())
                try:
                    await asyncio.wait((read, refusing), return_when=asyncio.FIRST_COMPLETED)
                finally:
                    refusing.cancel()
                if read.done():
                    if reading is read:
                        reading = None
                    return read.result()
                return disconnect

            async def held_send(message: Message) -> None:
                nonlocal holding
                if ended.is_set():
                    # Gone, as far as the application can tell: nothing more leaves.
                    return
                kind = message["type"]
                if holding is None and kind in ("http.response.start", "websocket.accept"):
                    owner = _owner(scope)
                    if owner is not None:
                        holding = holdings.hold(owner, _surface(scope, message), _end, authorized_at=arrived_at)
                        if holding.ended:
                            # Refused as it began: the start goes out, so the
                            # client sees a cut rather than a server error, and
                            # nothing after it does.
                            await send(message)
                            ended.set()
                            return
                await send(message)
                if holding is not None and ((kind == "http.response.body" and not message.get("more_body", False)) or kind == "websocket.close"):
                    holding.release()

            try:
                await self.app(scope, held_receive, held_send)
            except Exception:
                # Told the client went away, an application may raise on its
                # way out (a socket route that does not catch the disconnect,
                # say); that is the ending, not a fault to report.
                if not ended.is_set():
                    raise
                logger.debug("An ended connection's application raised while unwinding", exc_info=True)
            finally:
                if holding is not None:
                    holding.release()

        if ended.is_set() and scope["type"] == "websocket":
            with contextlib.suppress(Exception):
                await send({"type": "websocket.close", "code": REFUSED_SOCKET_CLOSE_CODE})
        if reading is not None and not reading.done():
            reading.cancel()
            with contextlib.suppress(BaseException):
                await reading


__all__ = ["OwnerConnectionsMiddleware", "REFUSED_SOCKET_CLOSE_CODE", "UNWIND_GRACE_SECONDS"]
