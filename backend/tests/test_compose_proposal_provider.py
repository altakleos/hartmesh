"""The fixture accepts a proposal only after a successful tool result from this turn."""

import json
import runpy
from pathlib import Path

import pytest

reply = runpy.run_path(str(Path(__file__).resolve().parents[2] / "docker/acceptance/provider.py"))["reply"]
FORM = "ext_hm_work_proposals_propose_tracked_work_0123456789ab"
TOKEN = "a" * 32


def body(mode, tools):
    return {"tools": [{"function": {"name": name}} for name in tools], "messages": [{"role": "user", "content": f"acceptance:proposal:{mode}:{TOKEN}"}]}


def test_scripted_employee_uses_the_offered_form_and_exact_success_receipt():
    request = body("enable", [FORM])
    call = reply(request)["tool_calls"][0]
    assert call["function"]["name"] == FORM and json.loads(call["function"]["arguments"]) == {"enabled": True}
    request["messages"].extend([{"role": "assistant", "tool_calls": [call]}, {"role": "tool", "tool_call_id": call["id"], "content": "The proposal to enable tracked assignments was submitted for review in the employee’s Settings."}])
    assert reply(request) == {"content": "Submitted the enable proposal for review."}
    request["messages"][-1]["tool_call_id"] = "different-call"
    with pytest.raises(ValueError):
        reply(request)


def test_fresh_turn_observes_actual_work_and_optional_form_inventory():
    assert reply(body("fresh-on", [FORM, "propose_assignment"])) == {"content": "Tracked assignment proposals are available."}
    assert reply(body("fresh-off", [FORM])) == {"content": "Tracked assignment proposals are unavailable."}
    assert reply(body("package-off", ["show_web_page"])) == {"content": "The configuration form is unavailable."}
    for mode, tools in [("enable", ["show_web_page"]), ("fresh-off", [FORM, "propose_assignment"]), ("fresh-on", [FORM]), ("package-off", [FORM])]:
        with pytest.raises(ValueError):
            reply(body(mode, tools))
