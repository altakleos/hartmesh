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

        # Exercise the same SDK adapter, model-facing middleware and OpenAI
        # serializer that send the hosted fixture's result to the scripted model.
        import runpy

        import pytest
        from langchain_core.messages import AIMessage, ToolMessage
        from langchain_mcp_adapters.tools import _convert_call_tool_result
        from langchain_openai.chat_models.base import _convert_message_to_dict
        from mcp.types import CallToolResult

        from deerflow.agents.middlewares.tool_result_meta import normalize_tool_message, tool_result_for_model

        blocks, artifact = _convert_call_tool_result(CallToolResult.model_validate(answer))
        tool = ToolMessage(content=blocks, artifact=artifact, tool_call_id="call_notice", name="notices_read_notice")
        wire = _convert_message_to_dict(tool_result_for_model(normalize_tool_message(tool)))
        assert wire["content"] == [{"type": "text", "text": "Notice from notices-a."}]
        provider = Path(__file__).resolve().parents[2] / "docker/acceptance/provider.py"
        reply = runpy.run_path(str(provider))["reply"]

        def send(result, name="notices_read_notice"):
            assistant = AIMessage(content="", tool_calls=[{"name": name, "args": {}, "id": "call_notice"}])
            return reply({"tools": [{"function": {"name": "notices_read_notice"}}], "messages": [{"role": "user", "content": "acceptance:notice:read"}, _convert_message_to_dict(assistant), result]})

        assert send(wire) == {"content": "Notice from notices-a."}
        for other in ([{"type": "text", "text": "Something else"}], [*wire["content"], {"type": "image_url", "image_url": {"url": "test"}}], [{"type": "text", "text": "A manager took away this connection."}]):
            with pytest.raises(ValueError):
                send({**wire, "content": other})
        with pytest.raises(ValueError):
            send({**wire, "tool_call_id": "other"})
        for absent in ("another_tool", "bulletins_read_notice"):
            with pytest.raises(ValueError):
                send(wire, name=absent)


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
    from langchain_core.messages import ToolMessage
    from langchain_openai.chat_models.base import _convert_message_to_dict

    from deerflow.agents.middlewares.tool_result_meta import normalize_tool_message, tool_result_for_model
    from deerflow.subagents.status_contract import format_subagent_result_message

    child_reply = reply({"messages": [{"role": "system", "content": "Helper"}, {"role": "user", "content": "Saved context"}, {"role": "user", "content": "--- BEGIN USER INPUT ---\nacceptance:notice:helper\n--- END USER INPUT ---"}]})
    assert child_reply == {"content": "Helper saw no notice connection."}
    parent_result, _ = format_subagent_result_message("completed", result=child_reply["content"])
    task = ToolMessage(name="task", tool_call_id="call_notice", content=parent_result)
    parent_wire = _convert_message_to_dict(tool_result_for_model(normalize_tool_message(task)))

    def complete_parent(wire, name="task"):
        declared = {"role": "assistant", "tool_calls": [{"id": "call_notice", "function": {"name": name}}]}
        return reply({"tools": [{"function": {"name": n}} for n in offered], "messages": [{"role": "user", "content": "acceptance:notice:delegate"}, declared, wire]})

    assert complete_parent(parent_wire) == {"content": "Helper confirmed its offered tools."}
    with pytest.raises(ValueError):
        complete_parent({**parent_wire, "content": "Task failed. Helper saw no notice connection."})
    with pytest.raises(ValueError):
        complete_parent(parent_wire, name="read_file")
    with pytest.raises(ValueError):
        complete_parent({**parent_wire, "tool_call_id": "different"})
    with pytest.raises(ValueError):
        reply({"messages": [{"role": "user", "content": "acceptance:notice:helper"}, {"role": "user", "content": "A different task."}]})
    for prompt in ("unrecognized task", "prefix: acceptance:notice:helper", "acceptance:notice:read"):
        with pytest.raises(ValueError):
            turn(prompt, [])
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

    def next_tool(*results, last_name=None, last_id=None, nudge=False):
        from langchain_core.messages import AIMessage, ToolMessage
        from langchain_openai.chat_models.base import _convert_message_to_dict

        from app.gateway.work_execution import ACTIVATION_MESSAGE
        from deerflow.agents.middlewares.loop_detection_middleware import _WARNING_MSG

        names = ["read_work_context", "report_work", "notices_read_notice"]
        messages = [
            {"role": "system", "content": "- This run works on an assignment, and no person is waiting in this conversation."},
            {"role": "user", "content": f"--- BEGIN USER INPUT ---\n{ACTIVATION_MESSAGE}\n--- END USER INPUT ---"},
        ]
        for index, content in enumerate((context, *results)):
            name = ["read_work_context", "notices_read_notice", "notices_read_notice", "notices_read_notice", "report_work"][index]
            if index == len(results) and last_name:
                name = last_name
            call_id = f"call_work_{index}"
            assistant = AIMessage(content="", tool_calls=[{"id": call_id, "name": name, "args": {}}])
            tool = ToolMessage(name=name, content=content, tool_call_id=last_id if last_id and index == len(results) else call_id)
            messages.extend((_convert_message_to_dict(assistant), _convert_message_to_dict(tool)))
        if nudge:
            messages.append({"role": "user", "content": _WARNING_MSG})
        reply_data = reply({"tools": [{"function": {"name": name}} for name in names], "messages": messages})
        return reply_data["content"] if "content" in reply_data else reply_data["tool_calls"][0]["function"]["name"]

    first = [{"type": "text", "text": "Notice from notices-a."}]
    second = [{"type": "text", "text": "Notice from notices-b."}]
    assert next_tool() == "notices_read_notice"
    assert next_tool(first) == "notices_read_notice"
    assert waits == [nonce + "-edit"]
    assert next_tool(first, second) == "notices_read_notice"
    assert waits == [nonce + "-edit", nonce + "-revoke"]

    def envelope(text, status="error", source="exception"):
        return json.dumps({"deerflow_tool_meta": {"status": status, "source": source}, "content": text})

    guidance = "Error: Tool 'notices_read_notice' failed: A manager took away this connection. Ask a manager if it is needed again."
    assert next_tool(first, second, envelope(guidance)) == "report_work"
    assert next_tool(first, second, envelope(guidance), nudge=True) == "report_work"
    statement = "Read the updated notice before the connection was taken away."
    candidate = {"id": "c" * 32, "attempt_id": "b" * 32, "statement": statement}
    report = {"id": "a" * 32, "attempt": {"id": "b" * 32, "candidate": candidate}}

    def receipt(value):
        return envelope(json.dumps(value), "success", "tool_return")

    assert next_tool(first, second, envelope(guidance), receipt(report)) == "Notice assignment complete."
    for bad in (json.dumps(report), receipt({**report, "id": "d" * 32}), receipt({**report, "attempt": {"id": "b" * 32, "candidate": {**candidate, "statement": "Another assignment"}}})):
        with pytest.raises(ValueError):
            next_tool(first, second, envelope(guidance), bad)
    with pytest.raises(ValueError):
        next_tool(first, second, envelope(guidance), receipt(report), last_name="read_work_context")
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
