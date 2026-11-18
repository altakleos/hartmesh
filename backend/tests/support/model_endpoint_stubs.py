"""A loopback OpenAI-compatible model endpoint that records every request.

The stub runs on its own threads and binds ``127.0.0.1:0`` only. It speaks
HTTP/1.1 with keep-alive, so a client that pools connections keeps one open and
``open_connections`` shows it.

- ``POST /v1/chat/completions`` answers with one message, or, for a streamed
  request, with one ``connection_check`` tool call.
- ``GET /v1/models`` lists one model.

``redirect_to`` answers every request with a 307 to that address instead.
"""

from __future__ import annotations

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, NamedTuple
from urllib.parse import urlsplit

MODEL_ID = "stub-model"
ANSWER = "stub answer"


class RecordedRequest(NamedTuple):
    method: str
    path: str
    headers: dict[str, str]
    body: bytes


class ModelEndpointStub:
    def __init__(self) -> None:
        self.requests: list[RecordedRequest] = []
        self.connections = 0
        self.open_connections = 0
        self.redirect_to: str | None = None
        self._lock = threading.Lock()
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), self._handler())
        self._thread = threading.Thread(target=self._server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True)

    def __enter__(self) -> ModelEndpointStub:
        self._thread.start()
        return self

    def __exit__(self, *exc_info: object) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)

    @property
    def port(self) -> int:
        return self._server.server_port

    @property
    def host_port(self) -> str:
        return f"127.0.0.1:{self.port}"

    def base_url(self, *, host: str = "127.0.0.1") -> str:
        return f"http://{host}:{self.port}/v1"

    def calls(self) -> list[tuple[str, str]]:
        with self._lock:
            return [(request.method, urlsplit(request.path).path) for request in self.requests]

    def received_text(self) -> str:
        with self._lock:
            return "\n".join(f"{request.method} {request.path}\n{json.dumps(request.headers)}\n{request.body.decode('utf-8', 'replace')}" for request in self.requests)

    def settled_open_connections(self, *, timeout: float = 3.0) -> int:
        """Connections still open once the server has seen every close a client sent."""
        deadline = time.monotonic() + timeout
        while self.open_connections and time.monotonic() < deadline:
            time.sleep(0.02)
        return self.open_connections

    def _handler(self) -> type[BaseHTTPRequestHandler]:
        stub = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def setup(self) -> None:
                super().setup()
                with stub._lock:
                    stub.connections += 1
                    stub.open_connections += 1

            def finish(self) -> None:
                try:
                    super().finish()
                finally:
                    with stub._lock:
                        stub.open_connections -= 1

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

            def _redirected(self) -> bool:
                if stub.redirect_to is None:
                    return False
                self._send(307, headers={"Location": stub.redirect_to})
                return True

            def do_GET(self) -> None:  # noqa: N802 - http.server naming
                self._record()
                if self._redirected():
                    return
                if urlsplit(self.path).path == "/v1/models":
                    self._send(200, {"object": "list", "data": [{"id": MODEL_ID, "object": "model"}]})
                else:
                    self._send(404)

            def do_POST(self) -> None:  # noqa: N802 - http.server naming
                body = self._record()
                if self._redirected():
                    return
                if urlsplit(self.path).path != "/v1/chat/completions":
                    self._send(404)
                    return
                request = json.loads(body or b"{}")
                if not request.get("stream"):
                    message = {"role": "assistant", "content": ANSWER}
                    usage = {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}
                    self._send(200, {"id": "chatcmpl-stub", "object": "chat.completion", "created": 0, "model": MODEL_ID, "choices": [{"index": 0, "message": message, "finish_reason": "stop"}], "usage": usage})
                    return
                call = {"index": 0, "id": "call_stub", "type": "function", "function": {"name": "connection_check", "arguments": "{}"}}
                chunks = [
                    {"index": 0, "delta": {"role": "assistant", "content": None, "tool_calls": [call]}, "finish_reason": None},
                    {"index": 0, "delta": {}, "finish_reason": "tool_calls"},
                ]
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Connection", "close")
                self.end_headers()
                self.close_connection = True
                for choice in chunks:
                    event = {"id": "chatcmpl-stub", "object": "chat.completion.chunk", "created": 0, "model": MODEL_ID, "choices": [choice]}
                    self.wfile.write(f"data: {json.dumps(event)}\n\n".encode())
                self.wfile.write(b"data: [DONE]\n\n")
                self.wfile.flush()

        return Handler
