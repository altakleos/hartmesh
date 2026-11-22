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
