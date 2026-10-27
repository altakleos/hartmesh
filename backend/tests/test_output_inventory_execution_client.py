"""Inventory negotiation over the one actual owner; synthetic HTTP proof only."""

import asyncio
import json

import httpx
import pytest
from test_channel_execution_client import PUBLICATION, Admission, creating
from test_channel_execution_output import projection
from test_channel_http_owner import RecordingStream
from test_output_inventory import inventory_document

from app.channels.execution import ChannelExecutionProtocolError, ChannelExecutionScope
from app.channels.execution_client import ChannelExecutionFacade
from app.telegram_bots.gateway_client import GatewayClientOwner
from deerflow.runtime.runs.output_inventory import build_event_inventory_evidence, output_receipt_digest

pytestmark = pytest.mark.asyncio
HEADER = "X-DeerFlow-Require-Output-Inventory"
ACK = {"X-DeerFlow-Channel-Execution-Version": "1", "X-DeerFlow-Output-Inventory-Version": "1"}


def inventory_projection():
    output = projection()
    receipt = output["output_evidence"]
    document = inventory_document(owner_user_id="human", input_message_id="actual-human", output_receipt_sha256=output_receipt_digest(receipt))
    evidence = build_event_inventory_evidence(receipt, status="success", inventory_event_seq=10, document=document)
    return {"version": 2, "output": output, "inventory": {"state": "matched", "evidence": evidence.model_dump(mode="json"), "document": document, "reason": None}}


def client_for(respond, *, capture_inventory=True, capture_output=True):
    owner = GatewayClientOwner(gateway_url="https://gateway.example", langgraph_url="https://runtime.example/prefix/api", transport=httpx.MockTransport(respond))
    scope = ChannelExecutionScope(PUBLICATION, "human", "connection", "chat", "thread", owner.endpoint_id, "lead_agent", None)
    admission = Admission()
    client = ChannelExecutionFacade(owner, scope=scope, admission=admission, capture_output=capture_output, capture_inventory=capture_inventory)
    return client, owner, admission


@pytest.mark.parametrize("streaming", [False, True])
@pytest.mark.parametrize("inventory", [False, True])
async def test_mode_controls_detached_creating_intent_and_never_retries(streaming, inventory):
    calls = []
    response = RecordingStream(block=False)

    def respond(request):
        calls.append(request)
        return httpx.Response(200, headers={"Content-Type": "text/event-stream"}, stream=response) if streaming else httpx.Response(200, json={})

    client, owner, _ = client_for(respond, capture_inventory=inventory)
    try:
        if streaming:
            stream = client.runs.stream("thread", "lead_agent", **creating())
            try:
                await anext(stream)
            finally:
                await stream.aclose()
        else:
            await client.runs.wait("thread", "lead_agent", **creating())
        assert len(calls) == 1 and calls[0].method == "POST"
        assert json.loads(calls[0].content)["output_capture"] == {"version": 2 if inventory else 1}
    finally:
        await owner.stop()


@pytest.mark.parametrize("value", [None, 0, 1, "true", {}, []])
async def test_inventory_flag_is_literal_and_does_not_create_transport(value):
    calls = []
    with pytest.raises(ChannelExecutionProtocolError):
        client_for(lambda request: calls.append(request) or httpx.Response(200), capture_inventory=value)
    assert not calls


async def test_inventory_requires_capture_and_is_readonly():
    with pytest.raises(ChannelExecutionProtocolError):
        client_for(lambda request: httpx.Response(200), capture_output=False)
    client, owner, _ = client_for(lambda request: httpx.Response(200))
    try:
        with pytest.raises(AttributeError):
            client.capture_inventory = False
    finally:
        await owner.stop()


async def test_real_owner_negotiates_exact_read_under_retained_source_authority():
    calls = []
    client, owner, admission = client_for(lambda request: calls.append(request) or httpx.Response(200, headers=ACK, json=inventory_projection()))
    try:
        owner.close_run_admission()
        admission.permitted.remove("run_create")
        result = await client.get_run_output("run")
        assert result.version == 2 and result.output.state == "complete" and result.inventory.document.items == ()
        assert admission.calls == ["source_read", "source_read"]
        assert calls[0].headers[HEADER] == "1"
        assert calls[0].headers["X-DeerFlow-Owner-User-Id"] == "human"
        assert str(calls[0].url) == "https://runtime.example/prefix/api/threads/thread/runs/run/output"
    finally:
        await owner.stop()


@pytest.mark.parametrize(
    "status,headers,body,missing",
    [(404, ACK, {}, True), (404, {"X-DeerFlow-Channel-Execution-Version": "1"}, {}, False), (200, {"X-DeerFlow-Channel-Execution-Version": "1"}, inventory_projection(), False), (200, ACK, projection(), False), (200, ACK, None, False)],
)
async def test_old_or_unacknowledged_peer_never_becomes_inventory_proof(status, headers, body, missing):
    calls = []
    client, owner, _ = client_for(lambda request: calls.append(request) or httpx.Response(status, headers=headers, json=body))
    try:
        if missing:
            assert await client.get_run_output("run") is None
        else:
            with pytest.raises(ChannelExecutionProtocolError):
                await client.get_run_output("run")
        assert len(calls) == 1 and calls[0].method == "GET"
    finally:
        await owner.stop()


@pytest.mark.parametrize("status", [422, 503])
async def test_refused_v2_creating_call_has_no_downgrade(status):
    calls = []
    client, owner, _ = client_for(lambda request: calls.append(request) or httpx.Response(status, json={"detail": "unsupported"}))
    try:
        with pytest.raises(ChannelExecutionProtocolError):
            await client.runs.wait("thread", "lead_agent", **creating())
        assert len(calls) == 1 and json.loads(calls[0].content)["output_capture"] == {"version": 2}
    finally:
        await owner.stop()


async def test_source_authority_withdrawn_during_real_response_refuses():
    entered, release = asyncio.Event(), asyncio.Event()

    async def respond(request):
        entered.set()
        await release.wait()
        return httpx.Response(200, headers=ACK, json=inventory_projection())

    client, owner, admission = client_for(respond)
    operation = asyncio.create_task(client.get_run_output("run"))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        admission.permitted.remove("source_read")
        release.set()
        with pytest.raises(ChannelExecutionProtocolError):
            await operation
    finally:
        release.set()
        await asyncio.gather(operation, return_exceptions=True)
        await owner.stop()


@pytest.mark.parametrize("headers", [{HEADER: "3"}, {HEADER: "1 "}, {"X-DeerFlow-Owner-User-Id": "other"}, {"Authorization": "private"}, {"X-Unrelated-Proof": "1"}])
async def test_actual_owner_header_extension_does_not_admit_other_values_or_authority(headers):
    calls = []
    client, owner, admission = client_for(lambda request: calls.append(request) or httpx.Response(200, headers=ACK, json=inventory_projection()))
    try:
        with pytest.raises(ChannelExecutionProtocolError):
            await owner.request_json(operation="source_read", owner_user_id="human", method="GET", path="threads/thread/runs/run/output", headers=headers, before_dispatch=lambda: admission.authorize("source_read"))
        assert not calls
    finally:
        await owner.stop()


@pytest.mark.parametrize(
    "path,value",
    [
        (("version",), True),
        (("inventory", "evidence", "version"), True),
        (("inventory", "evidence", "input_message_id"), None),
        (("inventory", "evidence", "output_receipt_sha256"), "0" * 64),
        (("inventory", "document", "scan", "after_root"), "missing"),
        (("inventory", "document", "coverage"), "incomplete"),
        (("inventory", "document", "observed_items"), []),
        (("output", "channel_execution", "publication_id"), "b" * 32),
    ],
)
async def test_nested_inventory_scope_and_closed_coverage_refuse(path, value):
    body = inventory_projection()
    target = body
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value
    calls = []
    client, owner, _ = client_for(lambda request: calls.append(request) or httpx.Response(200, headers=ACK, json=body))
    try:
        with pytest.raises(ChannelExecutionProtocolError):
            await client.get_run_output("run")
        assert len(calls) == 1
    finally:
        await owner.stop()


async def test_incomplete_observed_empty_remains_unknown_to_consumer():
    body = inventory_projection()
    doc = body["inventory"]["document"]
    doc.update(coverage="incomplete", reason="scan_incomplete", observed_items=[], scan=None)
    del doc["items"]
    body["inventory"]["evidence"] = build_event_inventory_evidence(body["output"]["output_evidence"], status="success", inventory_event_seq=10, document=doc).model_dump(mode="json")
    client, owner, _ = client_for(lambda request: httpx.Response(200, headers=ACK, json=body))
    try:
        result = await client.get_run_output("run")
        assert result.output.state == "complete" and result.inventory.state == "matched"
        assert result.inventory.document.coverage == "incomplete" and result.inventory.document.observed_items == ()
        assert not hasattr(result.inventory.document, "items")
    finally:
        await owner.stop()
