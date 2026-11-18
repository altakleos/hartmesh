"""Loopback MCP and token endpoints that record every request they receive.

Each stub runs on its own threads and binds ``127.0.0.1:0`` only. It speaks
just enough of the streamable HTTP and SSE transports for a real MCP client to
initialize, list one tool and call it, and it answers an OAuth token request.

- ``POST /mcp`` (or ``/mcp/``) is the streamable HTTP endpoint (JSON responses, no session).
- ``GET /sse`` opens an SSE session whose messages are posted to ``/messages``.
- ``POST /token`` returns a fixed access token.

Scripted departures let a test observe what a client does with them:
``unauthorized_once`` answers the next MCP request with 401 and a pointer to an
authorization document, ``sse_post_endpoint_once`` makes the next SSE session
announce that address as the place to post messages, ``redirect_to`` answers
every request with a 307 to that address, and ``redirects`` does so for single
paths.
"""

from __future__ import annotations

import json
import queue
import select
import socket
import threading
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, NamedTuple
from urllib.parse import parse_qs, urlsplit

TOOL_NAME = "echo"
ACCESS_TOKEN = "stub-access-token"


class RecordedRequest(NamedTuple):
    method: str
    path: str
    headers: dict[str, str]
    body: bytes


class EndpointStub:
    def __init__(self) -> None:
        self.requests: list[RecordedRequest] = []
        self.connections = 0
        self.unauthorized_once: str | None = None
        self.sse_post_endpoint_once: str | None = None
        self.redirect_to: str | None = None
        self.redirects: dict[str, str] = {}
        self._lock = threading.Lock()
        self._sessions: dict[str, queue.Queue[dict[str, Any]]] = {}
        self._closing = threading.Event()
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), self._handler())
        self._thread = threading.Thread(target=self._server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True)

    def __enter__(self) -> EndpointStub:
        self._thread.start()
        return self

    def __exit__(self, *exc_info: object) -> None:
        self._closing.set()
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)

    @property
    def port(self) -> int:
        return self._server.server_port

    @property
    def host_port(self) -> str:
        return f"127.0.0.1:{self.port}"

    def url(self, path: str, *, host: str = "127.0.0.1") -> str:
        return f"http://{host}:{self.port}{path}"

    def calls(self) -> list[tuple[str, str]]:
        """Method and path of every request received so far, in arrival order."""
        with self._lock:
            return [(request.method, request.path) for request in self.requests]

    def header_values(self, name: str, *, path: str | None = None) -> list[str]:
        with self._lock:
            return [request.headers[name.lower()] for request in self.requests if name.lower() in request.headers and (path is None or urlsplit(request.path).path == path)]

    def bodies(self, path: str) -> list[bytes]:
        with self._lock:
            return [request.body for request in self.requests if urlsplit(request.path).path == path]

    def received_text(self) -> str:
        """Everything received, as one string, for "this value never arrived" checks."""
        with self._lock:
            return "\n".join(f"{request.method} {request.path}\n{json.dumps(request.headers)}\n{request.body.decode('utf-8', 'replace')}" for request in self.requests)

    def _take(self, attribute: str) -> str | None:
        with self._lock:
            value = getattr(self, attribute)
            setattr(self, attribute, None)
            return value

    @staticmethod
    def _answer(message: Any) -> dict[str, Any] | None:
        if not isinstance(message, dict) or message.get("id") is None:
            return None
        method = message.get("method")
        params = message.get("params") or {}
        if method == "initialize":
            result: dict[str, Any] = {"protocolVersion": params.get("protocolVersion", "2025-03-26"), "capabilities": {"tools": {}}, "serverInfo": {"name": "endpoint-stub", "version": "1"}}
        elif method == "tools/list":
            result = {"tools": [{"name": TOOL_NAME, "description": "Return the given text.", "inputSchema": {"type": "object", "properties": {"text": {"type": "string"}}}}]}
        elif method == "tools/call":
            result = {"content": [{"type": "text", "text": f"echo:{(params.get('arguments') or {}).get('text', '')}"}], "isError": False}
        elif method == "ping":
            result = {}
        else:
            return {"jsonrpc": "2.0", "id": message["id"], "error": {"code": -32601, "message": "Method not found"}}
        return {"jsonrpc": "2.0", "id": message["id"], "result": result}

    def _handler(self) -> type[BaseHTTPRequestHandler]:
        stub = self

        class Handler(BaseHTTPRequestHandler):
            def setup(self) -> None:
                super().setup()
                with stub._lock:
                    stub.connections += 1

            def log_message(self, *args: object) -> None:
                pass

            def _record(self) -> bytes:
                length = int(self.headers.get("Content-Length") or 0)
                body = self.rfile.read(length) if length else b""
                with stub._lock:
                    stub.requests.append(RecordedRequest(self.command, self.path, {name.lower(): value for name, value in self.headers.items()}, body))
                return body

            def _send(self, status: int, payload: Any = None, *, headers: dict[str, str] | None = None) -> None:
                data = b"" if payload is None else json.dumps(payload).encode("utf-8")
                self.send_response(status)
                if payload is not None:
                    self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                for name, value in (headers or {}).items():
                    self.send_header(name, value)
                self.end_headers()
                self.wfile.write(data)

            def _scripted(self, *, mcp_request: bool) -> bool:
                location = stub.redirect_to or stub.redirects.get(urlsplit(self.path).path)
                if location is not None:
                    self._send(307, headers={"Location": location})
                    return True
                if mcp_request and (document := stub._take("unauthorized_once")) is not None:
                    self._send(401, headers={"WWW-Authenticate": f'Bearer resource_metadata="{document}"'})
                    return True
                return False

            def do_POST(self) -> None:  # noqa: N802 - http.server naming
                body = self._record()
                target = urlsplit(self.path)
                if self._scripted(mcp_request=target.path in {"/mcp", "/mcp/"}):
                    return
                if target.path == "/token":
                    self._send(200, {"access_token": ACCESS_TOKEN, "token_type": "Bearer", "expires_in": 3600})
                elif target.path in {"/mcp", "/mcp/"}:
                    answer = stub._answer(json.loads(body or b"null"))
                    self._send(202) if answer is None else self._send(200, answer)
                elif target.path == "/messages":
                    session = stub._sessions.get(parse_qs(target.query).get("session_id", [""])[0])
                    if session is None:
                        self._send(404)
                        return
                    answer = stub._answer(json.loads(body or b"null"))
                    if answer is not None:
                        session.put(answer)
                    self._send(202)
                else:
                    self._send(404)

            def do_GET(self) -> None:  # noqa: N802 - http.server naming
                self._record()
                target = urlsplit(self.path)
                if self._scripted(mcp_request=target.path == "/sse"):
                    return
                if target.path != "/sse":
                    self._send(405 if target.path == "/mcp" else 404)
                    return
                session_id = uuid.uuid4().hex
                messages: queue.Queue[dict[str, Any]] = queue.Queue()
                stub._sessions[session_id] = messages
                endpoint = stub._take("sse_post_endpoint_once") or f"/messages?session_id={session_id}"
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                try:
                    self._event("endpoint", endpoint)
                    while not stub._closing.is_set() and not self._peer_closed():
                        try:
                            self._event("message", json.dumps(messages.get(timeout=0.05)))
                        except queue.Empty:
                            continue
                except OSError:
                    pass
                finally:
                    stub._sessions.pop(session_id, None)

            def do_DELETE(self) -> None:  # noqa: N802 - http.server naming
                self._record()
                self._send(405)

            def _event(self, name: str, data: str) -> None:
                self.wfile.write(f"event: {name}\ndata: {data}\n\n".encode())
                self.wfile.flush()

            def _peer_closed(self) -> bool:
                readable, _, _ = select.select([self.connection], [], [], 0)
                if not readable:
                    return False
                try:
                    return self.connection.recv(1, socket.MSG_PEEK) == b""
                except OSError:
                    return True

        return Handler
