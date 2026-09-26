"""A connection that authenticated once is closed by the deployment when its owner is refused.

Served by a real uvicorn on a loopback socket: whether a stream the server
cut is seen as cut by the client is a property of the server, not of an
in-memory transport. The app puts a ``BaseHTTPMiddleware`` between the
connection middleware and the routes, as the Gateway's ``AuthMiddleware``
does, so the cancellation crosses one.
"""

from __future__ import annotations

import asyncio
import socket
import threading
import time
from collections.abc import AsyncIterator, Iterator
from types import SimpleNamespace

import httpx
import pytest
import uvicorn
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse, StreamingResponse
from starlette.routing import Mount, Route, WebSocketRoute
from starlette.websockets import WebSocket
from websockets.exceptions import ConnectionClosed
from websockets.sync.client import connect as ws_connect

from app.gateway.owner_connections import OwnerConnectionsMiddleware
from deerflow.runtime.owner_holdings import OwnerHoldings


class _StampUser(BaseHTTPMiddleware):
    """What ``AuthMiddleware`` does for this test: the caller's id, on ``request.state.user``."""

    async def dispatch(self, request, call_next):
        who = request.headers.get("x-user")
        if who:
            request.state.user = SimpleNamespace(id=who)
        return await call_next(request)


async def _ticks(content: bytes) -> AsyncIterator[bytes]:
    while True:
        yield content
        await asyncio.sleep(0.05)


async def _sse(request):
    return StreamingResponse(_ticks(b"data: tick\n\n"), media_type="text/event-stream")


async def _download(request):
    return StreamingResponse(_ticks(b"x" * 1024), media_type="application/zip")


async def _quick(request):
    return JSONResponse({"ok": True})


HEARD: list[str] = []


async def _listening(scope, receive, send):
    """An application that streams and listens, as the Gateway's SSE consumer does: what does it hear?"""
    await send({"type": "http.response.start", "status": 200, "headers": [(b"content-type", b"text/event-stream")]})
    await send({"type": "http.response.body", "body": b"data: tick\n\n", "more_body": True})
    try:
        message = await receive()
        while message["type"] == "http.request":  # the (empty) request body comes first
            message = await receive()
        HEARD.append(message["type"])
    except BaseException as exc:
        HEARD.append(type(exc).__name__)
        raise


async def _deaf(scope, receive, send):
    """An application that never reads ``receive``: only the grace's cancellation ends it."""
    await send({"type": "http.response.start", "status": 200, "headers": [(b"content-type", b"text/event-stream")]})
    while True:
        await send({"type": "http.response.body", "body": b"data: tick\n\n", "more_body": True})
        await asyncio.sleep(0.05)


async def _socket(websocket: WebSocket):
    # The browser route authenticates itself and stamps the same state.
    websocket.state.user = SimpleNamespace(id=websocket.query_params["who"])
    await websocket.accept()
    while True:
        await websocket.receive_text()


def _free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


@pytest.fixture
def served() -> Iterator[SimpleNamespace]:
    holdings = OwnerHoldings()
    app = Starlette(
        routes=[Route("/sse", _sse), Route("/download", _download), Route("/quick", _quick), Mount("/listening", app=_listening), Mount("/deaf", app=_deaf), WebSocketRoute("/ws", _socket)],
        middleware=[Middleware(OwnerConnectionsMiddleware, holdings=holdings, unwind_grace_seconds=0.5), Middleware(_StampUser)],
    )
    port = _free_port()
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning", lifespan="off"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 10
    while not server.started:
        assert time.monotonic() < deadline, "server did not start"
        time.sleep(0.02)
    try:
        yield SimpleNamespace(base=f"http://127.0.0.1:{port}", ws=f"ws://127.0.0.1:{port}", holdings=holdings, loop=None)
    finally:
        server.should_exit = True
        thread.join(timeout=10)


def _wait_for(predicate, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, "condition not reached"
        time.sleep(0.02)


def _read_until_cut(response: httpx.Response, outcome: dict[str, object]) -> None:
    try:
        for _ in response.iter_raw():
            pass
        outcome["ended"] = "completed"
    except httpx.HTTPError as exc:
        outcome["ended"] = type(exc).__name__


def test_ending_an_owner_cuts_their_sse_stream_and_download_and_leaves_another_owner_s(served) -> None:
    with httpx.Client(timeout=10) as client:
        with client.stream("GET", served.base + "/sse", headers={"x-user": "pat"}) as sse, client.stream("GET", served.base + "/download", headers={"x-user": "pat"}) as download:
            with httpx.Client(timeout=10) as other_client, other_client.stream("GET", served.base + "/sse", headers={"x-user": "sam"}) as other:
                outcomes: dict[str, dict[str, object]] = {"sse": {}, "download": {}}
                readers = [threading.Thread(target=_read_until_cut, args=(sse, outcomes["sse"])), threading.Thread(target=_read_until_cut, args=(download, outcomes["download"]))]
                for reader in readers:
                    reader.start()
                _wait_for(lambda: served.holdings.owners() == {"pat", "sam"})

                counts = asyncio.run(served.holdings.end_owner("pat"))

                for reader in readers:
                    reader.join(timeout=5)
                assert counts == {"sse_streams": 1, "downloads": 1}
                assert outcomes["sse"]["ended"] != "completed" and outcomes["download"]["ended"] != "completed", "a cut stream must not read as a finished one"
                # The other owner's stream is still flowing.
                chunk = next(other.iter_raw())
                assert chunk.startswith(b"data: tick")
                assert served.holdings.owners() == {"sam"}


def test_ending_an_owner_closes_their_websocket_with_the_signed_out_code(served) -> None:
    with ws_connect(served.ws + "/ws?who=pat") as pat_socket, ws_connect(served.ws + "/ws?who=sam") as sam_socket:
        _wait_for(lambda: served.holdings.owners() == {"pat", "sam"})

        counts = asyncio.run(served.holdings.end_owner("pat"))

        assert counts == {"websockets": 1}
        with pytest.raises(ConnectionClosed) as closed:
            pat_socket.recv(timeout=5)
        assert closed.value.rcvd is not None and closed.value.rcvd.code == 4401
        sam_socket.send("still here")
        assert served.holdings.owners() == {"sam"}


def test_a_finished_response_holds_nothing_and_an_unauthenticated_one_is_never_held(served) -> None:
    with httpx.Client(timeout=10) as client:
        assert client.get(served.base + "/quick", headers={"x-user": "pat"}).json() == {"ok": True}
        with client.stream("GET", served.base + "/sse") as anonymous:
            next(anonymous.iter_raw())
            assert served.holdings.owners() == set()
    _wait_for(lambda: served.holdings.owners() == set())


def test_a_stream_opened_by_an_owner_already_refused_is_cut_at_once(served) -> None:
    """Authorized before the look read the refusals (a read stamped later than the request stands for that)."""
    served.holdings.set_refused({"pat"}, read_at=time.monotonic() + 3600)
    outcome: dict[str, object] = {}
    with httpx.Client(timeout=10) as client, client.stream("GET", served.base + "/sse", headers={"x-user": "pat"}) as sse:
        _read_until_cut(sse, outcome)
    assert outcome["ended"] != "completed"
    assert served.holdings.drain_late_endings() == {"pat": {"sse_streams": 1}}


def test_a_stream_that_arrived_after_the_look_read_the_refusals_is_judged_by_its_own_read(served) -> None:
    """Turned back on after the last look, and signed in before the next one: the stream flows."""
    served.holdings.set_refused({"pat"})
    with httpx.Client(timeout=10) as client, client.stream("GET", served.base + "/sse", headers={"x-user": "pat"}) as sse:
        next(sse.iter_raw())
        assert served.holdings.owners() == {"pat"}
    assert served.holdings.drain_late_endings() == {}


def test_to_the_application_an_ending_is_exactly_a_client_that_went_away(served) -> None:
    """The refusal takes the path every stream already handles for a dropped client, whatever it does there."""
    HEARD.clear()
    with httpx.Client(timeout=10) as client, client.stream("GET", served.base + "/listening/", headers={"x-user": "pat"}) as dropped:
        next(dropped.iter_raw())
    _wait_for(lambda: len(HEARD) == 1)
    client_went_away = list(HEARD)

    HEARD.clear()
    outcome: dict[str, object] = {}
    with httpx.Client(timeout=10) as client, client.stream("GET", served.base + "/listening/", headers={"x-user": "pat"}) as sse:
        reader = threading.Thread(target=_read_until_cut, args=(sse, outcome))
        reader.start()
        _wait_for(lambda: served.holdings.owners() == {"pat"})
        asyncio.run(served.holdings.end_owner("pat"))
        reader.join(timeout=5)
    _wait_for(lambda: len(HEARD) == 1)

    assert outcome["ended"] != "completed"
    assert HEARD == client_went_away, (HEARD, client_went_away)


def test_an_application_that_never_reads_receive_is_cut_after_the_grace(served) -> None:
    outcome: dict[str, object] = {}
    with httpx.Client(timeout=10) as client, client.stream("GET", served.base + "/deaf/", headers={"x-user": "pat"}) as deaf:
        reader = threading.Thread(target=_read_until_cut, args=(deaf, outcome))
        reader.start()
        _wait_for(lambda: served.holdings.owners() == {"pat"})
        started = time.monotonic()
        asyncio.run(served.holdings.end_owner("pat"))
        reader.join(timeout=5)
    assert outcome["ended"] != "completed" and time.monotonic() - started < 3


def test_two_readers_of_receive_at_once_both_hear_the_ending() -> None:
    """ASGI forbids it, but a middleware must not crash when a layer does it."""

    async def _scenario() -> list[str]:
        holdings = OwnerHoldings()
        heard: list[str] = []
        never = asyncio.Event()

        async def _receive():
            await never.wait()
            return {"type": "http.request"}

        async def _send(message):
            return None

        async def _app(scope, receive, send):
            await send({"type": "http.response.start", "status": 200, "headers": []})

            async def _listen():
                heard.append((await receive())["type"])

            await asyncio.gather(_listen(), _listen())

        middleware = OwnerConnectionsMiddleware(_app, holdings=holdings)
        serving = asyncio.ensure_future(middleware({"type": "http", "state": {"user": SimpleNamespace(id="pat")}}, _receive, _send))
        while holdings.owners() != {"pat"}:
            await asyncio.sleep(0.01)
        await holdings.end_owner("pat")
        await asyncio.wait_for(serving, timeout=5)
        return heard

    assert asyncio.run(_scenario()) == ["http.disconnect", "http.disconnect"]


def test_the_browser_socket_s_own_authentication_stamps_its_owner(monkeypatch: pytest.MonkeyPatch) -> None:
    """Every socket that authenticates through the shared helper is held; one that did not stamp would silently report nothing."""
    from app.gateway import auth_disabled
    from app.gateway.routers import browser

    user = SimpleNamespace(id="pat")
    monkeypatch.setattr(auth_disabled, "is_auth_disabled", lambda: True)
    monkeypatch.setattr(auth_disabled, "get_auth_disabled_user", lambda: user)
    socket_ = SimpleNamespace(cookies={}, state=SimpleNamespace())

    assert asyncio.run(browser._authenticate_ws(socket_)) is user
    assert socket_.state.user is user


def test_a_response_that_fails_mid_stream_holds_nothing_afterwards() -> None:
    async def _scenario() -> set[str]:
        holdings = OwnerHoldings()

        async def _receive():
            await asyncio.Event().wait()

        async def _send(message):
            return None

        async def _app(scope, receive, send):
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"part", "more_body": True})
            raise RuntimeError("the archive could not be read")

        with pytest.raises(RuntimeError):
            await OwnerConnectionsMiddleware(_app, holdings=holdings)({"type": "http", "state": {"user": SimpleNamespace(id="pat")}}, _receive, _send)
        return holdings.owners()

    assert asyncio.run(_scenario()) == set()
