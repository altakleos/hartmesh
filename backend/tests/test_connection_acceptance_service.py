"""The connection fixture reports only protocol traffic counts and a fixed notice."""

import importlib.util
from pathlib import Path

from starlette.testclient import TestClient


def load_service():
    path = Path(__file__).resolve().parents[2] / "docker/acceptance/connections/notice_service.py"
    spec = importlib.util.spec_from_file_location("notice_service", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_services_record_discovery_and_calls_separately_and_never_collect_arguments():
    service = load_service()
    headers = {"Accept": "application/json, text/event-stream"}
    with (
        TestClient(service.create_app("notices-a"), base_url="https://notices-a.example.test:8080") as first,
        TestClient(service.create_app("notices-b"), base_url="https://notices-b.example.test:8080") as second,
    ):
        empty = {"requests": 0, "discoveries": 0, "calls": 0}
        assert first.get("/observations").json() == second.get("/observations").json() == empty

        def exchange(method, params):
            response = first.post("/mcp", json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params}, headers=headers)
            assert response.status_code == 200
            return response.json()["result"]

        exchange("initialize", {"protocolVersion": "2025-03-26", "capabilities": {}, "clientInfo": {"name": "acceptance", "version": "1"}})
        tools = exchange("tools/list", {})["tools"]
        assert [tool["name"] for tool in tools] == ["read_notice"]
        assert first.get("/observations").json() == {"requests": 2, "discoveries": 1, "calls": 0}
        answer = exchange("tools/call", {"name": "read_notice", "arguments": {}})
        assert answer["content"] == [{"type": "text", "text": "Notice from notices-a."}]
        assert first.get("/observations").json() == {"requests": 3, "discoveries": 1, "calls": 1}
        assert second.get("/observations").json() == empty


def test_model_requires_a_real_notice_and_an_independent_helper_observation():
    import runpy

    import pytest

    provider = Path(__file__).resolve().parents[2] / "docker/acceptance/provider.py"
    reply = runpy.run_path(str(provider))["reply"]

    def turn(prompt, names, *results):
        return reply(
            {
                "tools": [{"function": {"name": name}} for name in names],
                "messages": [{"role": "user", "content": prompt}, *({"role": "tool", "content": value} for value in results)],
            }
        )

    offered = ["notices_read_notice", "task"]
    assert turn("acceptance:notice:read", offered)["tool_calls"][0]["function"]["name"] == "notices_read_notice"
    assert turn("acceptance:notice:read", offered, "Notice from notices-b.") == {"content": "Notice from notices-b."}
    with pytest.raises(ValueError):
        turn("acceptance:notice:read", offered, "missing")
    with pytest.raises(ValueError):
        turn("acceptance:notice:unassigned", offered)
    assert turn("acceptance:notice:unassigned", ["read_file"]) == {"content": "No notice connection is available."}
    child = turn("acceptance:notice:delegate", offered)["tool_calls"][0]["function"]
    assert child["name"] == "task"
    assert "acceptance:notice:helper" in child["arguments"]
    with pytest.raises(ValueError):
        turn("acceptance:notice:helper", offered)
    assert turn("acceptance:notice:helper", ["read_file"]) == {"content": "Helper saw no notice connection."}
    with pytest.raises(ValueError):
        turn("acceptance:notice:delegate", offered, "Helper not started")


def test_assignment_waits_for_each_new_address_and_for_a_confirmed_refusal(monkeypatch):
    import json
    import runpy

    import pytest

    provider = Path(__file__).resolve().parents[2] / "docker/acceptance/provider.py"
    reply = runpy.run_path(str(provider))["reply"]
    waits = []
    monkeypatch.setitem(reply.__globals__, "final_observation", waits.append)
    nonce = "c" * 32
    context = json.dumps({"work": {"id": "a" * 32, "attempt": {"id": "b" * 32}, "revision": 2, "objective": "acceptance:work:notices:" + nonce}})

    def next_tool(*results, last_name="notices_read_notice", last_id=None):
        from langchain_core.messages import AIMessage, ToolMessage
        from langchain_openai.chat_models.base import _convert_message_to_dict

        names = ["read_work_context", "report_work", "notices_read_notice"]
        messages = [
            {"role": "system", "content": "- This run works on an assignment, and no person is waiting in this conversation."},
            {"role": "user", "content": "Execute the work."},
        ]
        for index, content in enumerate((context, *results)):
            name = "read_work_context" if index == 0 else last_name if index == len(results) else "notices_read_notice"
            call_id = f"call_work_{index}"
            assistant = AIMessage(content="", tool_calls=[{"id": call_id, "name": name, "args": {}}])
            tool = ToolMessage(name=name, content=content, tool_call_id=last_id if last_id and index == len(results) else call_id)
            messages.extend((_convert_message_to_dict(assistant), _convert_message_to_dict(tool)))
        reply_data = reply({"tools": [{"function": {"name": name}} for name in names], "messages": messages})
        return reply_data["tool_calls"][0]["function"]["name"]

    first = "Notice from notices-a."
    second = "Notice from notices-b."
    assert next_tool() == "notices_read_notice"
    assert next_tool(first) == "notices_read_notice"
    assert waits == [nonce + "-edit"]
    assert next_tool(first, second) == "notices_read_notice"
    assert waits == [nonce + "-edit", nonce + "-revoke"]
    def envelope(text, status="error", source="exception"):
        return json.dumps({"deerflow_tool_meta": {"status": status, "source": source}, "content": text})

    guidance = "Error: Tool 'notices_read_notice' failed: A manager took away this connection. Ask a manager if it is needed again."
    assert next_tool(first, second, envelope(guidance)) == "report_work"
    for unrelated in (guidance, "Server responded normally.", envelope(guidance, "success", "tool_return"), envelope("Some other dependency timed out.")):
        with pytest.raises(ValueError):
            next_tool(first, second, unrelated)
    with pytest.raises(ValueError):
        next_tool(first, second, envelope(guidance), last_name="report_work")
    with pytest.raises(ValueError):
        next_tool(first, second, envelope(guidance), last_id="other-call")
    with pytest.raises(ValueError):
        next_tool(first, first)
    with pytest.raises(ValueError):
        next_tool(first, second, second)


def test_hosted_qualification_verifies_both_images_before_boot_and_uses_the_shipped_floor():
    import yaml

    root = Path(__file__).resolve().parents[2]
    workflow = yaml.safe_load((root / ".github/workflows/compose-instances.yml").read_text())
    inputs = workflow[True]["workflow_dispatch"]["inputs"]
    assert inputs["connections"]["options"] == ["skip", "explicit", "bundle-default"]
    steps = workflow["jobs"]["bundle-instances"]["steps"]
    verification = next(step for step in steps if step.get("name") == "Verify connection candidate images")
    boot = next(step for step in steps if step.get("name", "").startswith("11 Start"))
    assert steps.index(verification) < steps.index(boot)
    assert "verify-candidate" in verification["run"] and "GITHUB_SHA" in verification["run"]
    assert "frontend" in verification["run"] and "backend" in verification["run"]
    older = next(step for step in steps if step.get("name") == "Install connection rollback bundles")
    for commit in ("d9d1a21a4451b13d0919c665b51b7635aba9fc91", "40aa905acbb4e7d890b0a4840a9915de0a710ea9", "b2548de9ae0ee664c526fcbad294aec5f1ad75f9"):
        assert commit in older["run"]
    rehearsal = next(step for step in steps if step.get("name") == "Exercise company connections with mounted employees")
    assert "test_compose_agent_connections.py" in rehearsal["run"]
    assert "pytest.importorskip" not in (root / "backend/tests/test_compose_agent_connections.py").read_text()


def test_explicit_activation_preserves_template_and_updates_one_option(tmp_path):
    import runpy

    import yaml

    module = runpy.run_path(str(Path(__file__).resolve().parents[2] / "docker/acceptance/connections/template.py"))
    path = tmp_path / "template.yaml"
    for initial in ({}, {"agent_connections": {"enabled": False, "packages": ["calendar"]}}):
        original = {"other": {"enabled": False}, **initial}
        path.write_text(yaml.safe_dump(original))
        for enabled in (True, False, True):
            module["apply_option"](path, enabled)
            written = path.read_text()
            assert written.count("agent_connections:") == 1
            assert yaml.safe_load(written) == {**original, "agent_connections": {"enabled": enabled, "packages": initial.get("agent_connections", {}).get("packages", [])}}
