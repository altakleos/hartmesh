"""Exercise continued conversation requests in both native OpenAI message forms."""

import json

import pytest

from provider import ARTIFACT, FINAL, reply


@pytest.mark.parametrize("browser", [False, True], ids=["plain-text", "browser-text-blocks"])
def test_continued_report_dispatch_requires_the_original_words_and_copied_file(browser):
    nonce = "5e5c8158f3254dd4bd5507cfa883cab1"
    saved = "/mnt/user-data/uploads/acceptance (2).txt"
    uploads = f"<current_uploads>\n  Path: {saved}\n</current_uploads>"
    words = f"--- BEGIN USER INPUT ---\nacceptance:continued-report:{nonce}\n--- END USER INPUT ---"
    content = [{"type": "text", "text": uploads}, {"type": "text", "text": words}] if browser else f"{uploads}\n\n{words}"
    messages = [
        {"role": "system", "content": "Use the available file tools for the requested report."},
        {"role": "user", "content": f"acceptance:file:shared-{nonce}"},
        {"role": "assistant", "content": FINAL},
        {"role": "user", "content": content},
    ]
    request = {"messages": messages, "tools": [{"type": "function", "function": {"name": "read_file"}}]}
    action = reply(request)
    (call,) = action["tool_calls"]
    assert call["function"]["name"] == "read_file"
    assert json.loads(call["function"]["arguments"]) == {"path": saved}
    messages.extend([{"role": "assistant", **action}, {"role": "tool", "tool_call_id": call["id"], "content": "different file"}])
    with pytest.raises(ValueError, match="did not read the copied Home file"):
        reply(request)
    messages[-1]["content"] = ARTIFACT
    assert reply(request) == {"content": "I read the shared report in my files."}


@pytest.mark.parametrize("separate_hint", [True, False])
def test_import_uses_the_presented_run_and_reads_the_receipt_path(separate_hint):
    source = "31a9d42e-8002-4b13-b0f9-b91dd3635d5f"
    run = "0fcaa0a1-d806-4479-8cf8-8a37938cd8f5"
    output = "/mnt/user-data/outputs/nested/acceptance.txt"
    upload = "/mnt/user-data/uploads/acceptance_1.txt"
    hint = 'Read-only conversation references for this run: ' + json.dumps([source])
    marker = {"type": "text", "text": "acceptance:linked-report — bring the report here."}
    messages = [{"role": "user", "content": [marker]}]
    if separate_hint:
        messages.append({"role": "user", "content": hint})
    else:
        messages[0]["content"].append({"type": "text", "text": hint})
    tools = ["read_conversation", "list_conversation_files", "import_conversation_file", "read_file"]
    body = {"messages": messages, "tools": [{"type": "function", "function": {"name": name}} for name in tools]}
    steps = [
        ({"thread_id": source}, json.dumps({"status": "ok", "thread_id": source, "messages": [{"role": "assistant", "text": FINAL}]})),
        ({"thread_id": source}, json.dumps({"files": [{"run_id": run, "path": output, "filename": "acceptance.txt", "available": True}]})),
        ({"thread_id": source, "run_id": run, "path": output}, json.dumps({"imported_files": [{"virtual_path": upload, "filename": "acceptance_1.txt"}]})),
        ({"path": upload}, ARTIFACT),
    ]
    for name, (expected, result) in zip(tools, steps, strict=True):
        action = reply(body)
        call, = action["tool_calls"]
        assert call["function"]["name"] == name and json.loads(call["function"]["arguments"]) == expected
        messages.extend([{"role": "assistant", **action}, {"role": "tool", "tool_call_id": call["id"], "content": result}])
    assert reply(body) == {"content": "I imported and read the linked report."}
    messages[-1]["content"] = "Other bytes"
    with pytest.raises(ValueError, match="imported bytes"):
        reply(body)
