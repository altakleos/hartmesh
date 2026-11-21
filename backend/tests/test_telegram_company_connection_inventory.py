"""A paired Telegram employee finishes a captured turn with and without a company tool."""

import asyncio
import json

import pytest
from _config_singleton_guard import restore_config_singletons  # noqa: F401 -- autouse fixture
from _run_output_capture_test_support import capture_environment as capture_environment
from _run_output_capture_test_support import output_store as output_store
from _telegram_compose_turn_support import chat_completions_stream, install_provider_wire, render_compose_config
from _telegram_work_worker_support import WorkerProviderWire, installed_worker, propose
from sqlalchemy import select
from support.mcp_endpoint_stubs import EndpointStub
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging

from deerflow.agent_instances import connections
from deerflow.config.app_config import agent_connections_enabled
from deerflow.persistence.models.run_event import RunEventRow
from deerflow.persistence.run.model import RunRow
from deerflow.runtime.runs.output import TextOutputReceipt, receipt_from_kwargs
from deerflow.runtime.runs.output_inventory import EventInventoryEvidence, inventory_evidence_from_kwargs
from deerflow.runtime.runs.output_inventory_source import read_durable_inventory_source


@pytest.mark.asyncio
@pytest.mark.parametrize("granted", [False, True], ids=["no-grants", "issued-call"])
async def test_telegram_employee_connection_inventory(capture_environment, monkeypatch, tmp_path, granted):
    # Source checkouts preceding the bundle switch turn the same runtime setting on in YAML.
    await asyncio.to_thread(render_compose_config, tmp_path, monkeypatch, keys={"OPENAI_API_KEY": "sk-synthetic", "HARTMESH_AGENT_CONNECTIONS": "on"}, tweak=lambda config: config.setdefault("agent_connections", {"enabled": True}))
    assert await asyncio.to_thread(agent_connections_enabled)
    marker = "verified-guide"
    final_text = "The guide review is complete."
    wire = WorkerProviderWire([], chat_completions_stream(text_value="Guide review"))
    if granted:
        wire.replies.append(chat_completions_stream(tool=("records_echo", {"text": marker})))
    wire.replies.append(chat_completions_stream(text_value=final_text, identity="chatcmpl-result"))

    with EndpointStub() as endpoint:
        install_provider_wire(monkeypatch, wire, loopback_port=endpoint.port)
        settings = {"mcpServers": {"records": {"type": "http", "url": endpoint.url("/mcp"), "tool_name_prefix": True}}}
        (tmp_path / "extensions_config.json").write_text(json.dumps(settings), encoding="utf-8")
        async with installed_worker(capture_environment, monkeypatch, tmp_path, custody="company") as worker:
            [choice] = await asyncio.to_thread(connections.list_connections, worker.instance.id, manager=True)
            assert choice["name"] == "records" and choice["can_give"] and not choice["given"]
            if granted:
                await asyncio.to_thread(connections.change_grant, worker.instance.id, choice["id"], worker.link.owner_user_id, give=True)

            turn = await propose(worker)
            async with worker.e.sf() as reader:
                row = await reader.get(RunRow, turn.run_id)
                assert row.status == "success", row.error
                receipt = receipt_from_kwargs(row.kwargs_json)
                assert isinstance(receipt, TextOutputReceipt)
                assert (receipt.run_id, receipt.thread_id, receipt.owner_user_id) == (turn.run_id, row.thread_id, worker.link.owner_user_id)
                evidence = inventory_evidence_from_kwargs(row.kwargs_json)
                assert isinstance(evidence, EventInventoryEvidence)
                events = await reader.scalars(select(RunEventRow.content).where(RunEventRow.run_id == turn.run_id, RunEventRow.event_type == "middleware:connection_call").order_by(RunEventRow.seq))
                calls = [json.loads(content)["changes"] for content in events]
            audit = await read_durable_inventory_source(worker.e.event_store, evidence=evidence, output_receipt=receipt)
            assert audit.state == "matched" and audit.document.coverage == "complete"
            assert any(final_text in call.parameters.get("text", "") for call in worker.normal.calls if call.endpoint in {"sendMessage", "editMessageText"})
            offered = {tool["function"]["name"] for tool in json.loads(wire.lead_bodies[0])["tools"]}
            assert ("records_echo" in offered) == granted

        requests = [json.loads(body) for body in endpoint.bodies("/mcp") if body]
        issued = [message["params"] for message in requests if message.get("method") == "tools/call"]
        assert [(params["name"], params["arguments"]) for params in issued] == ([("echo", {"text": marker})] if granted else [])
        assert [call["outcome"] for call in calls] == (["sending", "made"] if granted else [])
        if granted:
            assert calls[0]["call_id"] == calls[1]["call_id"]
            assert all(call["instance_id"] == worker.instance.id and call["origin"] == "message" for call in calls)
            assert any("echo:" + marker in body for body in wire.lead_bodies[1:])
