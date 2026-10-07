"""Synthetic OpenAI protocol fixture; never calls a model or external service."""

from __future__ import annotations

import json
import re
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
        system = " ".join(str(message.get("content", "")) for message in messages if message.get("role") == "system")
        if "You are a security reviewer for AI agent skills." in system:
            content = " ".join(str(message.get("content", "")) for message in messages if message.get("role") == "user")
            allowed = "skill-result-acceptance-fixture" in content
            return {
                "content": json.dumps(
                    {
                        "decision": "allow" if allowed else "block",
                        "reason": "Deterministic decision for registered synthetic fixture" if allowed else "Unregistered fixture content",
                    }
                )
            }
        if "You are generating follow-up questions" in system:
            return {"content": "[]"}
        raise ValueError("unrecognized optional acceptance prompt")
    latest = max(
        (i for i, message in enumerate(messages) if message.get("role") == "user"),
        default=0,
    )
    turn = messages[latest:]
    if not turn:
        raise ValueError("missing acceptance turn")
    skill_match = re.search(
        r"acceptance:skill:(supplier-comparison|procedure-summary)(?:\s|$)",
        str(turn[0].get("content")),
    )
    if skill_match:
        return skill_reply(body, turn, skill_match.group(1))
    if "acceptance:fallback" in str(turn[0].get("content")):
        return fallback_reply(body, turn)
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


def skill_reply(body, turn, package):
    results = [message for message in turn if message.get("role") == "tool"]
    filename = "quotes.json" if package == "supplier-comparison" else "procedure.json"
    if not results:
        name, arguments = (
            "read_file",
            {"path": f"/mnt/skills/custom/{package}/SKILL.md"},
        )
    elif len(results) == 1:
        if "skill-result-acceptance-fixture" not in str(results[-1].get("content")):
            raise ValueError("real skill instructions were not read")
        name, arguments = "read_file", {"path": f"/mnt/user-data/uploads/{filename}"}
    elif len(results) == 2:
        expected = "unit_price" if package == "supplier-comparison" else "Inspection interval not supplied"
        if expected not in str(results[-1].get("content")):
            raise ValueError("real uploaded skill input was not read")
        name, arguments = (
            "bash",
            {"command": f"python3 -B /mnt/skills/custom/{package}/scripts/build.py --input /mnt/user-data/uploads/{filename} --output /mnt/user-data/outputs/skill-results"},
        )
    elif len(results) == 3:
        content = str(results[-1].get("content"))
        decoded = None
        for offset, character in enumerate(content):
            if character != "{":
                continue
            try:
                value, _ = json.JSONDecoder().raw_decode(content[offset:])
                if isinstance(value, dict) and isinstance(value.get("files"), list):
                    decoded = value
                    break
            except ValueError:
                continue
        if decoded is None or not 1 <= len(decoded["files"]) <= 4:
            raise ValueError("real producer did not return a bounded file manifest")
        for path in decoded["files"]:
            if not isinstance(path, str) or not re.fullmatch(
                r"(?:comparison|procedure)-[0-9a-f]{12}/(?:comparison|procedure)\.(?:view\.json|json|pdf|xlsx|docx)",
                path,
            ):
                raise ValueError("producer returned an unexpected path")
        name, arguments = (
            "present_files",
            {"filepaths": ["/mnt/user-data/outputs/skill-results/" + path for path in decoded["files"]]},
        )
    else:
        return {"content": f"Skill result ready: {package}."}
    offered = {tool.get("function", {}).get("name") for tool in body["tools"]}
    if name not in offered:
        raise ValueError(f"required real tool not offered: {name}")
    return {
        "tool_calls": [
            {
                "id": f"call_skill_{len(results)}",
                "type": "function",
                "function": {"name": name, "arguments": json.dumps(arguments)},
            }
        ]
    }


def fallback_reply(body, turn):
    results = [message for message in turn if message.get("role") == "tool"]
    if not results:
        name, arguments = (
            "bash",
            {"command": "cp /mnt/user-data/files/Comparisons/comparison.pdf /mnt/user-data/outputs/plain.pdf"},
        )
    elif len(results) == 1:
        name, arguments = (
            "write_file",
            {
                "path": "/mnt/user-data/outputs/broken.view.json",
                "content": json.dumps(
                    {
                        "format": "hartmesh.artifact-view",
                        "version": 999,
                        "title": "Unsupported fixture view",
                        "blocks": [],
                        "exports": [],
                    }
                ),
            },
        )
    elif len(results) == 2:
        name, arguments = (
            "present_files",
            {
                "filepaths": [
                    "/mnt/user-data/outputs/broken.view.json",
                    "/mnt/user-data/outputs/plain.pdf",
                ]
            },
        )
    else:
        return {"content": "Ordinary fallback files ready."}
    offered = {tool.get("function", {}).get("name") for tool in body["tools"]}
    if name not in offered:
        raise ValueError(f"required real tool not offered: {name}")
    return {
        "tool_calls": [
            {
                "id": f"call_fallback_{len(results)}",
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
        base = {
            "id": f"chatcmpl-{uuid.uuid4().hex}",
            "created": 1,
            "model": "acceptance-model",
        }
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
