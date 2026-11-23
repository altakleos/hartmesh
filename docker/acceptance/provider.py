"""Synthetic OpenAI protocol fixture; never calls a model or external service."""

from __future__ import annotations

import hashlib
import json
import os
import re
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

UPLOAD_MARKER = "UPLOAD-ACCEPTANCE-726"
ARTIFACT = "Synthetic acceptance artifact.\nUploaded text: UPLOAD-ACCEPTANCE-726\n"
FINAL = "Acceptance complete. Your synthetic artifact is ready."


def final_observation(token):
    directory = os.environ.get("HARTMESH_MODEL_BARRIER_DIR")
    if not directory:
        raise ValueError("model observation requires the hosted driver")
    armed = Path(directory) / ("armed-" + token)
    if not armed.is_file():
        raise ValueError("model observation was not armed")
    (Path(directory) / ("waiting-" + token)).touch()
    deadline = time.monotonic() + 180
    while armed.exists():
        if time.monotonic() >= deadline:
            raise ValueError("final model call exceeded its qualification deadline")
        time.sleep(0.05)
    return {"content": "Final observed response " + hashlib.sha256(token.encode("ascii")).hexdigest()}


def tool_result_content(value: object, *, allow_preflight_failure: bool = False) -> str:
    """Consume one provider-facing envelope; this fixture grants no authority."""
    if not isinstance(value, str):
        raise ValueError("invalid acceptance tool result text")
    try:
        envelope = json.loads(value)
    except ValueError:
        return value
    if not isinstance(envelope, dict) or "deerflow_tool_meta" not in envelope:
        return value
    metadata = envelope["deerflow_tool_meta"]
    transforms = envelope.get("content_transforms", [])
    if (
        not set(envelope) <= {"deerflow_tool_meta", "content", "content_transforms"}
        or not isinstance(metadata, dict)
        or set(metadata) != {"status", "source"}
        or metadata.get("status") not in (("success", "partial_success", "error") if allow_preflight_failure else ("success", "partial_success"))
        or metadata.get("source") not in ("tool_return", "exception", "progress_middleware")
        or not isinstance(envelope.get("content"), str)
        or not isinstance(transforms, list)
        or len(transforms) > 2
        or any(transform not in ("truncated", "externalized") for transform in transforms)
    ):
        raise ValueError("invalid or failed acceptance tool result envelope")
    return envelope["content"]


NOTICE_STATEMENT = "Read the updated notice before the connection was taken away."
WORK_ACTIVATION = (
    "--- BEGIN USER INPUT ---\nWork on the explicitly activated assignment. Read current Work context, "
    "follow the adopted mandate, and report progress, blockers or an outcome.\n--- END USER INPUT ---"
)


def scripted_tool(assistant, result, call_id):
    calls = assistant.get("tool_calls", [])
    if result.get("tool_call_id") != call_id or assistant.get("role") != "assistant" or len(calls) != 1 or calls[0].get("id") != call_id:
        return None
    return calls[0].get("function", {}).get("name")


def notice_result_block(assistant, result, expected_id, expected_name):
    """Read the one native MCP text block emitted by the scripted notice services."""
    if scripted_tool(assistant, result, expected_id) != expected_name:
        raise ValueError("Unexpected notice result identity")
    for text in ("Notice from notices-a.", "Notice from notices-b."):
        if result["content"] == [{"type": "text", "text": text}]:
            return text
    raise ValueError("Unexpected notice result block")


def receipt_text(assistant, result, call_id, name, status, source):
    if scripted_tool(assistant, result, call_id) != name:
        return None
    try:
        envelope = json.loads(result["content"])
    except (TypeError, ValueError):
        return None
    if isinstance(envelope, dict) and envelope.get("deerflow_tool_meta") == {"status": status, "source": source}:
        return envelope.get("content")
    return None


def is_notice_assignment(context):
    try:
        objective = json.loads(context)["work"]["objective"]
    except (TypeError, KeyError, ValueError):
        return False
    return isinstance(objective, str) and re.fullmatch(r"acceptance:work:notices:[0-9a-f]{32}", objective) is not None


def continued_report(messages, latest, token, offered, user_text):
    """Confirm an actual continued employee run has the published words and file."""
    earlier = messages[:latest]
    soul = "Use the available file tools for the requested report."
    if not any(m.get("role") == "system" and soul in str(m.get("content")) for m in earlier):
        raise ValueError("The continued turn did not receive the adopted employee identity")
    first = "acceptance:file:shared-" + token
    if not any(m.get("role") == "user" and first in str(m.get("content")) for m in earlier):
        raise ValueError("The continued turn did not receive the published human message")
    if not any(m.get("role") == "assistant" and FINAL in str(m.get("content")) for m in earlier):
        raise ValueError("The continued turn did not receive the published employee reply")
    if len(messages) == latest + 1:
        if "read_file" not in offered:
            raise ValueError("The continued employee cannot read files")
        sections = re.findall(r"<current_uploads>(.*?)</current_uploads>", user_text, re.S)
        paths = re.findall(r"^  Path: (.+)$", sections[0], re.M) if len(sections) == 1 else []
        if len(paths) != 1 or not re.fullmatch(r"/mnt/user-data/uploads/acceptance(?: \([2-9][0-9]*\))?\.txt", paths[0]):
            raise ValueError("The continued message did not present the copied report")
        function = {"name": "read_file", "arguments": json.dumps({"path": paths[0]})}
        return {"tool_calls": [{"id": "call_shared_upload", "type": "function", "function": function}]}
    result = messages[-1]
    text = tool_result_content(result.get("content"))
    if scripted_tool(messages[-2], result, "call_shared_upload") != "read_file" or UPLOAD_MARKER not in text or "Synthetic acceptance artifact." not in text:
        raise ValueError("The continued employee did not read the copied Home file")
    return {"content": "I read the shared report in my files."}


def message_text(message):
    content = message.get("content")
    if isinstance(content, list):
        return "\n".join(block["text"] for block in content if isinstance(block, dict) and block.get("type") == "text" and isinstance(block.get("text"), str))
    return content if isinstance(content, str) else ""


def linked_report(messages, latest, offered):
    """Discover source selectors and copied bytes solely through the tools."""
    hint = re.search(r'Read-only conversation references for this run: (\["[a-f0-9-]{36}"\])', message_text(messages[latest]))
    if not hint:
        return None
    if "acceptance:linked-report" not in message_text(messages[latest]):
        older = next((m for m in reversed(messages[:latest]) if m.get("role") == "user"), {})
        if "acceptance:linked-report" not in message_text(older):
            return None
    source, = json.loads(hint[1])
    tools = ("read_conversation", "list_conversation_files", "import_conversation_file", "read_file")
    turn = messages[latest + 1:]
    results = []
    for i, message in enumerate(turn):
        if message.get("role") == "tool":
            n = len(results)
            if n >= len(tools) or i == 0 or scripted_tool(turn[i-1], message, f"call_linked_{n}") != tools[n]:
                raise ValueError("Unexpected linked report tool result")
            results.append(tool_result_content(message.get("content")))
    if len(results) == 4:
        if results[3].rstrip("\n") != ARTIFACT.rstrip("\n"):
            raise ValueError("Unexpected imported bytes")
        return {"content": "I imported and read the linked report."}
    name = tools[len(results)]
    arguments = {"thread_id": source}
    if len(results) == 1:
        page = json.loads(results[0])
        if page.get("thread_id") != source or not any(m.get("role") == "assistant" and FINAL in m.get("text", "") for m in page["messages"]):
            raise ValueError("The reader did not return the linked report conversation")
    elif len(results) == 2:
        file, = [f for f in json.loads(results[1])["files"] if f["available"] and f["filename"] == "acceptance.txt"]
        arguments.update(run_id=file["run_id"], path=file["path"])
    elif len(results) == 3:
        receipt, = json.loads(results[2])["imported_files"]
        arguments = {"path": receipt["virtual_path"]}
    if name not in offered:
        raise ValueError(f"Linked report tool unavailable: {name}")
    return {"tool_calls": [{"id": f"call_linked_{len(results)}", "type": "function", "function": {"name": name, "arguments": json.dumps(arguments)}}]}


def reply(body: dict[str, Any]) -> dict[str, Any]:
    messages = body.get("messages", [])
    if not body.get("tools"):
        latest_user = next((message.get("content") for message in reversed(messages) if message.get("role") == "user"), None)
        if latest_user in ("acceptance:notice:helper", "--- BEGIN USER INPUT ---\nacceptance:notice:helper\n--- END USER INPUT ---"):
            return {"content": "Helper saw no notice connection."}
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
    offered = {tool.get("function", {}).get("name") for tool in body["tools"]}
    work_turn = "report_work" in offered
    wrap_up = False
    if work_turn and messages and str(messages[latest].get("content", "")).startswith("[LOOP DETECTED] You are repeating the same tool calls. Stop repeating them and wrap up now."):
        previous = next((n for n in range(latest - 1, -1, -1) if messages[n].get("role") == "user"), None)
        if previous is not None and messages[previous].get("content") == WORK_ACTIVATION:
            latest = previous
            wrap_up = True
    user_text = message_text(messages[latest])
    if "Read-only conversation references for this run:" in user_text:
        imported = linked_report(messages, latest, offered)
        if imported is not None:
            return imported
    continuation = re.search(r"\bacceptance:continued-report:([0-9a-f]{32})\b", user_text) if isinstance(user_text, str) else None
    if continuation:
        return continued_report(messages, latest, continuation[1], offered, user_text)
    turn = messages[latest:]
    if not turn:
        raise ValueError("missing acceptance turn")
    skill_match = re.search(r"acceptance:skill:(supplier-comparison|procedure-summary)(?:\s|$)", str(turn[0].get("content")))
    file_turn = not work_turn and skill_match is None and "acceptance:file" in str(turn[0].get("content"))
    notice_read = "acceptance:notice:read" in str(turn[0].get("content"))
    notice_delegate = "acceptance:notice:delegate" in str(turn[0].get("content"))
    notice_work = False
    notice_report = None
    normalized = []
    tool_index = 0
    notice_revoked = False
    for message in turn:
        if message.get("role") == "tool":
            # The OpenAI tool reply identifies its assistant-issued call by ID.
            # Allow an error only for the final scripted revocation observation.
            previous = normalized[-1]
            content = message.get("content")
            if notice_delegate:
                completed = receipt_text(previous, message, "call_notice", "task", "success", "tool_return")
                if tool_index != 0 or "task" not in offered or completed != "Task Succeeded. Result: Helper saw no notice connection.":
                    raise ValueError("Missing confirmed result from the helper")
            if notice_work and tool_index == 3:
                refused = receipt_text(previous, message, "call_work_3", "notices_read_notice", "error", "exception")
                notice_revoked = refused == "Error: Tool 'notices_read_notice' failed: A manager took away this connection. Ask a manager if it is needed again."
            if notice_work and tool_index == 4:
                notice_report = receipt_text(previous, message, "call_work_4", "report_work", "success", "tool_return")
            if isinstance(content, list) and notice_work and tool_index in (1, 2) and "notices_read_notice" in offered:
                content = notice_result_block(previous, message, f"call_work_{tool_index}", "notices_read_notice")
            elif isinstance(content, list) and notice_read and tool_index == 0:
                assigned = offered.intersection({"notices_read_notice", "bulletins_read_notice"})
                if len(assigned) != 1:
                    raise ValueError("Expected exactly one granted notice tool")
                content = notice_result_block(previous, message, "call_notice", assigned.pop())
            content = tool_result_content(content, allow_preflight_failure=(file_turn and tool_index == 1) or (tool_index == 3 and notice_revoked))
            if work_turn and tool_index == 0:
                notice_work = is_notice_assignment(content)
            message = {**message, "content": content}
            tool_index += 1
        normalized.append(message)
    turn = normalized
    if work_turn:
        return work_reply(body, turn, notice_revoked=notice_revoked, notice_report=notice_report, wrap_up=wrap_up)
    if "acceptance:notice:" in str(turn[0].get("content")):
        return notice_reply(body, turn)
    if skill_match:
        return skill_reply(body, turn, skill_match.group(1))
    if "acceptance:preview" in str(turn[0].get("content")):
        return preview_reply(body, turn)
    if "acceptance:timing" in str(turn[0].get("content")):
        return {"content": "Timing sample complete."}
    if "acceptance:fallback" in str(turn[0].get("content")):
        return fallback_reply(body, turn)
    results = [message for message in turn if message.get("role") == "tool"]
    if "acceptance:attachments" in str(turn[0].get("content")):
        attachments = (("acceptance.txt", ARTIFACT), ("local.txt", "Local attachment."))
        if len(results) > len(attachments):
            raise ValueError("Unexpected extra attachment read")
        for result, (_, expected) in zip(results, attachments, strict=False):
            if result["content"].rstrip("\n") != expected.rstrip("\n"):
                raise ValueError("The receiving conversation read unexpected attachment content")
        if len(results) == len(attachments):
            return {"content": "Both attachments were read."}
        name, arguments = "read_file", {"path": "/mnt/user-data/uploads/" + attachments[len(results)][0]}
    elif "acceptance:file" not in str(turn[0].get("content")):
        raise ValueError("unrecognized acceptance prompt")
    elif not results:
        name, arguments = "read_file", {"path": "/mnt/user-data/uploads/input.txt"}
    elif len(results) == 1:
        if UPLOAD_MARKER not in str(results[-1].get("content")):
            raise ValueError("uploaded fixture was not read by the real file tool")
        name, arguments = "read_file", {"path": "/mnt/user-data/outputs/acceptance.txt"}
    elif len(results) == 2:
        name, arguments = "write_file", {"path": "/mnt/user-data/outputs/acceptance.txt", "content": ARTIFACT}
    elif len(results) == 3:
        name, arguments = (
            "present_files",
            {"filepaths": ["/mnt/user-data/outputs/acceptance.txt"]},
        )
    else:
        observed = re.search(r"acceptance:file:final:([a-f0-9]{32})\b", str(turn[0].get("content")))
        return final_observation(observed[1]) if observed else {"content": FINAL}
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


def notice_reply(body, turn):
    offered = {entry.get("function", {}).get("name") for entry in body["tools"]}
    request = str(turn[0].get("content"))
    results = [str(message["content"]) for message in turn if message.get("role") == "tool"]
    if "acceptance:notice:helper" in request or "acceptance:notice:unassigned" in request:
        if any(isinstance(name, str) and name.endswith("_read_notice") for name in offered):
            raise ValueError("An unassigned notice tool was offered")
        message = "Helper saw no notice connection." if "acceptance:notice:helper" in request else "No notice connection is available."
        return {"content": message}
    if "acceptance:notice:delegate" in request:
        if results:
            if "Helper saw no notice connection." not in results[-1]:
                raise ValueError("Missing result from the helper")
            return {"content": "Helper confirmed its offered tools."}
        name, arguments = "task", {"subagent_type": "general-purpose", "description": "Check available notices", "prompt": "acceptance:notice:helper"}
    elif "acceptance:notice:read" in request:
        if results:
            for service in ("notices-a", "notices-b"):
                text = f"Notice from {service}."
                if text in results[-1]:
                    return {"content": text}
            raise ValueError("Missing notice from the MCP service")
        assigned = offered.intersection({"notices_read_notice", "bulletins_read_notice"})
        if len(assigned) != 1:
            raise ValueError("Expected exactly one granted notice tool")
        name, arguments = assigned.pop(), {}
    else:
        raise ValueError("Unknown notice scenario")
    if name not in offered:
        raise ValueError("Expected notice scenario tool was not offered")
    return {"tool_calls": [{"id": "call_notice", "type": "function", "function": {"name": name, "arguments": json.dumps(arguments)}}]}


def work_reply(body, turn, *, notice_revoked, notice_report, wrap_up):
    """Stateless script of real Work tools; no application inference bypass."""
    offered = {tool.get("function", {}).get("name") for tool in body["tools"]}
    system = " ".join(str(message.get("content", "")) for message in body.get("messages", []) if message.get("role") == "system")
    if "ask_clarification" in offered or "- This run works on an assignment, and no person is waiting in this conversation." not in system:
        raise ValueError("Work requires its question rule and must not offer ask_clarification")
    results = [message for message in turn if message.get("role") == "tool"]
    if not results:
        name, arguments = "read_work_context", {}
    else:
        try:
            work = json.loads(results[0]["content"])["work"]
            work_id, attempt_id = work["id"], work["attempt"]["id"]
            revision, objective = work["revision"], work["objective"]
            if (
                not isinstance(work_id, str)
                or not re.fullmatch(r"[0-9a-f]{32}", work_id)
                or not isinstance(attempt_id, str)
                or not re.fullmatch(r"[0-9a-f]{32}", attempt_id)
                or type(revision) is not int
                or not 1 <= revision <= 2**31 - 1
                or not isinstance(objective, str)
            ):
                raise ValueError
        except (KeyError, TypeError, ValueError):
            raise ValueError("invalid acceptance work context") from None
        if "acceptance:work:outcome" in objective:
            if len(results) == 1:
                name, arguments = "bash", {"command": "mkdir -p /mnt/user-data/outputs && printf 'WORK-ACCEPTANCE-PROOF\\n' > /mnt/user-data/outputs/proof.txt"}
            elif len(results) == 2:
                name, arguments = (
                    "report_work",
                    {
                        "report": {
                            "operation_id": hashlib.sha256((work_id + ":" + attempt_id).encode("utf-8")).hexdigest()[:32],
                            "expected_revision": revision,
                            "action": "outcome",
                            "statement": "Acceptance outcome: outputs/proof.txt written.",
                        }
                    },
                )
            else:
                return {"content": "Work acceptance complete."}
        elif notices := re.fullmatch(r"acceptance:work:notices:([a-f0-9]{32})", objective):
            if wrap_up and len(results) < 4:
                raise ValueError("The host stopped the repeated calls before the notice sequence was observed")
            # The test driver changes the registry between real tool responses.
            # Distinct call IDs are supplied below for each turn of the model.
            name, arguments = "notices_read_notice", {}
            if len(results) >= 2 and "Notice from notices-a." not in results[1]["content"]:
                raise ValueError("First call did not reach the original service")
            if len(results) == 2:
                final_observation(notices[1] + "-edit")
            if len(results) >= 3 and "Notice from notices-b." not in results[2]["content"]:
                raise ValueError("Second call did not reach the replacement service")
            if len(results) == 3:
                final_observation(notices[1] + "-revoke")
            elif len(results) == 4:
                if not notice_revoked:
                    raise ValueError("The notice call did not confirm the manager’s revocation")
                name, arguments = "report_work", {"report": {
                    "operation_id": hashlib.sha256((work_id + ":" + attempt_id).encode()).hexdigest()[:32], "expected_revision": revision,
                    "action": "outcome", "statement": NOTICE_STATEMENT,
                }}
            elif len(results) > 4:
                try:
                    work = json.loads(notice_report)
                    attempt = work["attempt"]
                    candidate = attempt["candidate"]
                    complete = (len(results) == 5 and notice_revoked and work["id"] == work_id and attempt["id"] == attempt_id
                                and candidate["attempt_id"] == attempt_id and candidate["statement"] == NOTICE_STATEMENT
                                and re.fullmatch(r"[a-f0-9]{32}", candidate["id"]))
                except (TypeError, KeyError, ValueError):
                    complete = False
                if not complete:
                    raise ValueError("Missing the notice outcome receipt for this Work attempt")
                return {"content": "Notice assignment complete."}
        elif observed := re.fullmatch(r"acceptance:work:final:([a-f0-9]{32})", objective):
            return final_observation(observed[1])
        elif "acceptance:work:hold" in objective:
            if len(results) == 1:
                name, arguments = "bash", {"command": "sleep 300"}
            else:
                return {"content": "Work hold finished."}
        elif "acceptance:work:denied" in objective:
            if len(results) == 1:
                name, arguments = "bash", {"command": "python3 -c \"import urllib.request; urllib.request.urlopen('https://example.org/', timeout=20)\"; echo exit $?"}
            elif len(results) == 2:
                name, arguments = (
                    "report_work",
                    {
                        "report": {
                            "operation_id": hashlib.sha256((work_id + ":" + attempt_id).encode("utf-8")).hexdigest()[:32],
                            "expected_revision": revision,
                            "action": "outcome",
                            "statement": "Acceptance outcome: blocked connection recorded.",
                        }
                    },
                )
            else:
                return {"content": "Work acceptance complete."}
        elif "acceptance:work:ask" in objective:
            if len(results) == 1:
                name, arguments = (
                    "report_work",
                    {
                        "report": {
                            "operation_id": hashlib.sha256((work_id + ":" + attempt_id).encode("utf-8")).hexdigest()[:32],
                            "expected_revision": revision,
                            "action": "request_input",
                            "statement": "Which input should I use for this assignment?",
                            "purpose": "information",
                            "reason": "The required input is unspecified.",
                            "expected_response": "Identify the input to use.",
                        }
                    },
                )
            else:
                return {"content": "Work question recorded."}
        else:
            raise ValueError("unrecognized acceptance work objective")
    offered = {tool.get("function", {}).get("name") for tool in body["tools"]}
    if name not in offered:
        raise ValueError(f"required real tool not offered: {name}")
    return {"tool_calls": [{"id": f"call_work_{len(results)}", "type": "function", "function": {"name": name, "arguments": json.dumps(arguments)}}]}


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


def preview_reply(body, turn):
    """Publish only the fixed, synthetic samples in this disposable chat."""
    results = [message for message in turn if message.get("role") == "tool"]
    output = "/mnt/user-data/outputs"
    files = ("sample.report.json", "sample.xlsx", "sample.docx", "sample.pptx")
    if not results:
        name = "bash"
        sources = " ".join(f"/mnt/user-data/uploads/{filename}" for filename in files)
        arguments = {"command": (
            f"cp {sources} {output}/ && "
            f"cd {output} && cp sample.report.json busy.report.json && printf ' ' >> busy.report.json && "
            "printf oversize > large.sample && printf invalid > invalid.sample && "
            "printf interrupted > interrupted.sample && printf deadline > deadline.sample && printf address-limit > address.sample && "
            "printf recovered > recovery.sample && "
            "for i in 0 1 2 3 4 5 6 7 8 9 10 11; do printf 'sample-%s' \"$i\" > \"delay-$i.sample\"; done && "
            "for i in 0 1 2 3 4; do printf 'turn-%s' \"$i\" > \"turn-$i.sample\"; done"
        )}
    elif len(results) == 1:
        name, arguments = "present_files", {"filepaths": [f"{output}/{filename}" for filename in (*files, "busy.report.json")]}
    else:
        return {"content": "Preview files ready."}
    if name not in {tool.get("function", {}).get("name") for tool in body["tools"]}:
        raise ValueError(f"Required preview fixture tool unavailable: {name}")
    return {"tool_calls": [{"id": f"call_preview_{len(results)}", "type": "function",
                            "function": {"name": name, "arguments": json.dumps(arguments)}}]}


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
    streaming_usage = False

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
            if self.streaming_usage:
                end["usage"] = {"prompt_tokens": 10, "completion_tokens": 10, "total_tokens": 20}
            self.wfile.write(f"data: {json.dumps(end)}\n\ndata: [DONE]\n\n".encode())
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
