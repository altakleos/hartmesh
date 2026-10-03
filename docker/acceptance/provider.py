"""Synthetic OpenAI protocol fixture; never calls a model or external service."""

from __future__ import annotations

import json
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

UPLOAD_MARKER = "UPLOAD-ACCEPTANCE-726"
ARTIFACT = "Synthetic acceptance artifact.\nUploaded text: UPLOAD-ACCEPTANCE-726\n"
FINAL = "Acceptance complete. Your synthetic artifact is ready."


def reply(body: dict[str, Any]) -> dict[str, Any]:
    messages = body.get("messages", [])
    if not body.get("tools"):
        return {"content": "[]"}  # Optional suggestion calls use the same fixture.
    latest = max(
        (i for i, message in enumerate(messages) if message.get("role") == "user"),
        default=0,
    )
    turn = messages[latest:]
    if "acceptance:file" not in str(turn[0].get("content")):
        raise ValueError("unrecognized acceptance prompt")
    results = [message for message in turn if message.get("role") == "tool"]
    if not results:
        name, arguments = "read_file", {"path": "/mnt/user-data/uploads/input.txt"}
    elif len(results) == 1:
        if UPLOAD_MARKER not in str(results[-1].get("content")):
            raise ValueError("uploaded fixture was not read by the real file tool")
        name, arguments = (
            "write_file",
            {"path": "/mnt/user-data/outputs/acceptance.txt", "content": ARTIFACT},
        )
    elif len(results) == 2:
        name, arguments = (
            "present_files",
            {"filepaths": ["/mnt/user-data/outputs/acceptance.txt"]},
        )
    else:
        return {"content": FINAL}
    offered = {tool.get("function", {}).get("name") for tool in body["tools"]}
    if name not in offered:
        raise ValueError(f"required real tool not offered: {name}")
    return {
        "tool_calls": [
            {
                "id": f"call_acceptance_{len(results)}",
                "type": "function",
                "function": {"name": name, "arguments": json.dumps(arguments)},
            }
        ]
    }


class Handler(BaseHTTPRequestHandler):
    def log_message(self, _format: str, *args: object) -> None:
        pass  # Never print request bodies or authentication headers.

    def do_GET(self) -> None:
        self.send_response(200 if self.path == "/health" else 404)
        self.end_headers()

    def do_POST(self) -> None:
        if self.path != "/v1/chat/completions":
            self.send_error(404)
            return
        size = int(self.headers.get("Content-Length", "0"))
        if not 0 < size <= 2 * 1024 * 1024:
            self.send_error(413)
            return
        try:
            body = json.loads(self.rfile.read(size))
            answer = reply(body)
        except (ValueError, KeyError, TypeError) as error:
            self.send_error(422, str(error))
            return
        finish = "tool_calls" if "tool_calls" in answer else "stop"
        base = {"id": f"chatcmpl-{uuid.uuid4().hex}", "created": 1, "model": "acceptance-model"}
        if not body.get("stream"):
            result = {
                **base,
                "object": "chat.completion",
                "choices": [
                    {
                        "index": 0,
                        "message": {"role": "assistant", **answer},
                        "finish_reason": finish,
                    }
                ],
                "usage": {
                    "prompt_tokens": 10,
                    "completion_tokens": 10,
                    "total_tokens": 20,
                },
            }
            payload = json.dumps(result).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        if "tool_calls" in answer:
            answer["tool_calls"][0]["index"] = 0
            deltas = [{"role": "assistant", **answer}]
        else:
            content = answer["content"]
            deltas = [
                {"role": "assistant", "content": content[:12]},
                {"content": content[12:]},
            ]
        try:
            for delta in deltas:
                event = {
                    **base,
                    "object": "chat.completion.chunk",
                    "choices": [{"index": 0, "delta": delta, "finish_reason": None}],
                }
                self.wfile.write(f"data: {json.dumps(event)}\n\n".encode())
                self.wfile.flush()
                time.sleep(0.05)
            end = {
                **base,
                "object": "chat.completion.chunk",
                "choices": [{"index": 0, "delta": {}, "finish_reason": finish}],
            }
            self.wfile.write(f"data: {json.dumps(end)}\n\ndata: [DONE]\n\n".encode())
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
