"""The loopback-only guard other tests rely on to never leave the machine."""

from __future__ import annotations

import socket
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread

import httpx
import pytest
from support import loopback_only


class _Hello(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802 - http.server naming
        self.send_response(200)
        self.send_header("Content-Length", "5")
        self.end_headers()
        self.wfile.write(b"hello")

    def log_message(self, *args: object) -> None:
        pass


@pytest.fixture
def local_server():
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Hello)
    thread = Thread(target=server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True)
    thread.start()
    yield server.server_port
    server.shutdown()
    server.server_close()
    thread.join(timeout=5)


def test_a_connect_outside_loopback_raises_and_is_recorded(monkeypatch, local_server):
    guard = loopback_only.install(monkeypatch)

    with socket.socket() as plain, pytest.raises(OSError, match="loopback"):
        plain.connect(("8.8.8.8", 9))
    with socket.socket() as plain:
        assert plain.connect_ex(("127.0.0.1", local_server)) == 0
    with pytest.raises(httpx.ConnectError):
        httpx.get("http://10.0.0.5:9/", trust_env=False)

    assert guard.blocked == [("8.8.8.8", 9), ("10.0.0.5", 9)]
    with pytest.raises(AssertionError, match="outside the loopback interface"):
        guard.assert_nothing_blocked()


@pytest.mark.asyncio
async def test_an_asynchronous_connect_outside_loopback_is_blocked_too(monkeypatch, local_server):
    guard = loopback_only.install(monkeypatch)

    async with httpx.AsyncClient(trust_env=False) as client:
        assert (await client.get(f"http://127.0.0.1:{local_server}/")).text == "hello"
        with pytest.raises(httpx.ConnectError):
            await client.get("http://8.8.8.8:9/")

    assert guard.blocked == [("8.8.8.8", 9)]


def test_names_are_answered_from_a_script_or_not_at_all(monkeypatch):
    guard = loopback_only.install(monkeypatch)
    guard.names["records.example.test"] = ["8.8.8.8", "127.0.0.1"]

    answers = [socket.getaddrinfo("records.example.test", 80, type=socket.SOCK_STREAM)[0][4][0] for _ in range(3)]
    assert answers == ["8.8.8.8", "127.0.0.1", "127.0.0.1"]
    assert guard.answers("records.example.test") == answers
    with pytest.raises(socket.gaierror):
        socket.getaddrinfo("unscripted.example.test", 80)
    # A scripted answer is resolved in the family that was asked for, never fabricated.
    with pytest.raises(socket.gaierror):
        socket.getaddrinfo("records.example.test", 80, socket.AF_INET6)
    assert socket.getaddrinfo("127.0.0.1", 80, type=socket.SOCK_STREAM)[0][4][0] == "127.0.0.1"


def test_a_routed_public_address_lands_on_a_loopback_endpoint(monkeypatch, local_server):
    guard = loopback_only.install(monkeypatch)
    guard.routes[("8.8.8.8", 8443)] = ("127.0.0.1", local_server)

    assert httpx.get("http://8.8.8.8:8443/", trust_env=False).text == "hello"

    assert guard.routed == [("8.8.8.8", 8443)]
    guard.assert_nothing_blocked()
