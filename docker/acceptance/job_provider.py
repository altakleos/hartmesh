"""Synthetic model for the disposable employee job qualification."""

import json
import os
import re
from http.server import ThreadingHTTPServer
from pathlib import Path

import provider

MARK = re.compile(r"acceptance:job:(employee|personal|cut|off|third|replacement):(submit|item|second|final):([a-f0-9]{32})\b")


def call(name, index, args, offered):
    if name not in offered:
        raise ValueError("The expected job tool is unavailable")
    return {"tool_calls": [{"id": "job_" + str(index), "type": "function", "function": {"name": name, "arguments": json.dumps(args)}}]}


def answer(body, fallback):
    messages = body["messages"]
    for index in range(len(messages) - 1, -1, -1):
        message = messages[index]
        if message.get("role") == "user" and (match := MARK.search(provider.message_text(message))):
            break
    else:
        return fallback(body)
    kind, stage, nonce = match.groups()
    offered = {tool["function"]["name"] for tool in body.get("tools", [])}
    turn = messages[index + 1 :]
    receipts = []
    for pos, result in enumerate(turn):
        if result.get("role") == "tool":
            expected = "job_" + str(len(receipts))
            preceding = turn[pos - 1] if pos else {}
            name = provider.scripted_tool(preceding, result, expected)
            if not name or name not in offered:
                raise ValueError("Unexpected job tool result")
            receipts.append((name, result, preceding))
    if kind == "third":
        return {"content": "The third chat is ready."}
    if stage == "submit":
        if not receipts:
            items = [{"key": "report", "prompt": f"acceptance:job:{kind}:item:{nonce}"}]
            if kind == "replacement":
                items.append({"key": "remaining", "depends_on": ["report"], "prompt": f"acceptance:job:{kind}:second:{nonce}"})
            return call(
                "batch_task",
                0,
                {
                    "title": "Review sample " + kind + " " + nonce,
                    "items": items,
                    "subagent_type": "general-purpose",
                    "max_running_items": 1,
                    "continuation": f"acceptance:job:{kind}:final:{nonce}",
                },
                offered,
            )
        name, result, _ = receipts[0]
        if len(receipts) != 1 or name != "batch_task" or not re.search(r"(?:Job|Batch) [a-zA-Z0-9-]+ (?:accepted|submitted)", provider.tool_result_content(result["content"]), re.I):
            raise ValueError("The job was not accepted")
        return {"content": "The requested check has been submitted."}
    if stage in ("item", "second"):
        if any(n.endswith("_read_notice") for n in offered):
            raise ValueError("An item received an employee connection")
        if kind in ("employee", "personal"):
            provider.final_observation(nonce + "-" + kind)
        if stage == "second":
            marker = Path(os.environ["HARTMESH_MODEL_BARRIER_DIR"]) / ("armed-" + nonce + "-replacement")
            if marker.is_file():
                provider.final_observation(nonce + "-replacement")
            elif not marker.with_name("released-" + nonce + "-replacement").is_file():
                raise ValueError("The replacement check was never armed")
            return {"content": "Checked remaining " + nonce}
        return {"content": "Checked sample " + nonce}
    # The server, not this fixture, supplies the job ID in its final-turn input.
    scope = re.search(r'"batch_id"\s*:\s*"([a-zA-Z0-9-]+)"', provider.message_text(messages[index]))
    if scope is None:
        raise ValueError("Missing admitted job ID")
    expected = ["Checked sample " + nonce]
    if kind == "replacement":
        expected.append("Checked remaining " + nonce)
    for position, text in enumerate(expected):
        if len(receipts) == position:
            return call("read_batch_result", position, {"batch_id": scope[1], "position": position}, offered)
        name, result, _ = receipts[position]
        if name != "read_batch_result":
            raise ValueError("Missing stored item result")
        window = json.loads(provider.tool_result_content(result["content"]))
        document = json.loads(window["content"])
        if (
            window.get("status") != "ok"
            or window.get("position") != position
            or window.get("offset") != 0
            or window.get("next_offset") is not None
            or document.get("position") != position
            or document.get("result") != text
            or document.get("status") != "succeeded"
        ):
            raise ValueError("Missing confirmed stored item result")
    if kind in ("employee", "cut", "replacement"):
        name = "updated_read_notice" if kind == "replacement" else "notices_read_notice"
        if kind == "replacement" and "notices_read_notice" in offered:
            raise ValueError("Replacement retained an old grant")
        i = len(expected)
        if len(receipts) == i:
            return call(name, i, {}, offered)
        observed, result, preceding = receipts[i]
        notice = "Notice from notices-b." if kind == "replacement" else "Notice from notices-a."
        if len(receipts) != i + 1 or observed != name or provider.notice_result_block(preceding, result, "job_" + str(i), name) != notice:
            raise ValueError("Missing confirmed company notice")
        if kind == "cut":
            provider.final_observation(nonce + "-final")
    elif kind == "off" and any(n.endswith("_read_notice") for n in offered):
        raise ValueError("The disabled connection was offered")
    return {"content": "Completed " + kind + " checks " + nonce}


if __name__ == "__main__":
    normal = provider.reply
    provider.reply = lambda body: answer(body, normal)
    provider.Handler.streaming_usage = True
    ThreadingHTTPServer(("0.0.0.0", 18080), provider.Handler).serve_forever()
