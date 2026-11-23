"""The pinned job fixture advances only on correlated real tool receipts."""

import importlib
import json
from pathlib import Path

import pytest
import yaml


def test_model_uses_offered_tools_and_real_item_and_notice_receipts(monkeypatch):
    root = Path(__file__).resolve().parents[2]
    monkeypatch.syspath_prepend(str(root / "docker/acceptance"))
    module = importlib.import_module("job_provider")
    nonce = "b" * 32

    def reply(messages, names):
        body = {"messages": messages, "tools": [{"function": {"name": n}} for n in names]}
        return module.answer(body, lambda _: pytest.fail("unexpected default"))

    def turn(kind, stage):
        return [{"role": "user", "content": [{"type": "text", "text": json.dumps({"batch_id": "sample", "follow_up": f"acceptance:job:{kind}:{stage}:{nonce}"})}]}]

    def receive(messages, call, content):
        messages.extend([{"role": "assistant", **call}, {"role": "tool", "tool_call_id": call["tool_calls"][0]["id"], "content": content}])

    messages = turn("employee", "submit")
    create = reply(messages, ["batch_task"])
    spec = json.loads(create["tool_calls"][0]["function"]["arguments"])
    assert spec["subagent_type"] == "general-purpose" and nonce in spec["items"][0]["prompt"] and nonce in spec["continuation"]
    receive(messages, create, "Job sample accepted with 1 checks.")
    assert "submitted" in reply(messages, ["batch_task"])["content"]
    names = ["read_batch_result", "notices_read_notice"]
    with pytest.raises(ValueError):
        reply(turn("employee", "item"), names)
    final = turn("employee", "final")
    read = reply(final, names)
    assert json.loads(read["tool_calls"][0]["function"]["arguments"]) == {"batch_id": "sample", "position": 0}
    receive(final, read, "Checked sample " + nonce)
    notice = reply(final, names)
    receive(final, notice, [{"type": "text", "text": "Notice from notices-a."}])
    assert reply(final, names) == {"content": "Completed checks " + nonce}
    final[-1]["tool_call_id"] = "stale"
    with pytest.raises(ValueError):
        reply(final, names)
    off = turn("off", "final")
    receive(off, reply(off, names), "Checked sample " + nonce)
    with pytest.raises(ValueError):
        reply(off, names)
    assert "Completed" in reply(off, names[:1])["content"]


def test_dedicated_mounted_job_shares_candidate_verification_and_preserves_bundle_default():
    root = Path(__file__).resolve().parents[2]
    workflow = yaml.safe_load((root / ".github/workflows/compose-instances.yml").read_text())
    steps = {s.get("name"): s for s in workflow["jobs"]["bundle-instances"]["steps"]}
    signed = steps["Verify the signed workspace browser pair"]
    assert "inputs.verify_agent_jobs" in signed["if"] and "verify-candidate" in signed["run"] and "deploy/compose/images.txt" in signed["run"]
    assert "HARTMESH_BATCHES=on" in steps["Install the connection qualification fixture"]["run"]
    fixture = steps["Qualify employee jobs and final-turn recovery"]
    assert fixture["if"] == "inputs.verify_agent_jobs" and "employee-jobs.xml" in fixture["run"] and "employee-jobs.json" in fixture["run"]
