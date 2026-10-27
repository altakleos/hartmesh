"""Explicit format negotiation; SQL/HTTP seeds test protocol, not host attestation."""

import json
from types import SimpleNamespace

import httpx
import pytest
from _run_output_capture_test_support import capture_environment as capture_environment  # noqa: F401
from _run_output_capture_test_support import output_store as output_store  # noqa: F401
from _run_output_capture_test_support import put_messages
from langchain_core.messages import AIMessage, HumanMessage
from test_channel_execution_client import PUBLICATION, Admission, creating
from test_output_inventory import inventory_document
from test_output_inventory_execution_client import inventory_projection
from test_run_output_capture_admission import admission as admission  # noqa: F401
from test_run_output_capture_admission import launched, ordinary
from test_run_output_route import ACK, HEADER, OWNER, TEXT
from test_run_output_route import route_environment as route_environment  # noqa: F401

from app.channels.execution import ChannelExecutionProtocolError, ChannelExecutionScope
from app.channels.execution_client import ChannelExecutionFacade
from app.gateway import services
from app.telegram_bots.gateway_client import GatewayClientOwner
from deerflow.runtime.runs.manager import _new_run_kwargs
from deerflow.runtime.runs.output import OutputCaptureIntent, TextOutputReceipt, canonical_text_digest
from deerflow.runtime.runs.output_inventory import build_event_inventory_evidence, output_receipt_digest
from deerflow.runtime.runs.output_view import OutputViewQualifier
from deerflow.runtime.runs.schemas import RunStatus

REQUIRE = "X-DeerFlow-Require-Output-Inventory"
VERSION = "X-DeerFlow-Output-Inventory-Version"
INTENT = {"version": 2, "inventory_document_version": 2}


def test_admission_preserves_the_canonical_selector_without_caller_proof():
    supplied = {"output_capture": INTENT, "inventory_evidence": {"forged": True}, "output_inventory_profile": "forged"}
    actual = _new_run_kwargs(supplied, on_disconnect="continue")
    assert actual == {"output_capture": INTENT, "output_capture_on_disconnect": "continue"}
    assert actual["output_capture"] is not INTENT


@pytest.mark.parametrize("qualifier", [None, object(), SimpleNamespace(__call__=lambda: None)])
def test_document_two_requires_actual_host_support_before_preparation(qualifier):
    from fastapi import HTTPException

    manager = SimpleNamespace(supports_output_receipts=True, supports_output_inventory=True)
    events = SimpleNamespace(supports_durable_output_evidence=True, supports_durable_output_inventory=True)
    with pytest.raises(HTTPException) as failure:
        services._output_capture_preflight(INTENT, run_manager=manager, event_store=events, qualify_output_view=qualifier)
    assert failure.value.status_code == 503


def test_static_support_does_not_issue_a_no_view_claim():
    async def never_issue(request):
        pytest.fail("preflight must not issue a run lease")

    qualifier = OutputViewQualifier(never_issue)
    actual = services._output_capture_preflight(
        INTENT,
        run_manager=SimpleNamespace(supports_output_receipts=True, supports_output_inventory=True),
        event_store=SimpleNamespace(supports_durable_output_evidence=True, supports_durable_output_inventory=True),
        qualify_output_view=qualifier,
    )
    assert actual.document_version == 2


def test_caller_qualifier_claim_is_removed_from_both_serialized_config_sections(monkeypatch):
    monkeypatch.setattr(services, "_resolve_gateway_recursion_limits", lambda: (25, 100))
    config = services.build_run_config(thread_id="thread", request_config={"context": {"qualify_output_view": "forged", "legitimate": 7}, "configurable": {"qualify_output_view": "forged"}}, metadata=None)
    assert config["context"]["legitimate"] == 7
    assert "qualify_output_view" not in config["context"]
    assert "qualify_output_view" not in config["configurable"]


@pytest.mark.parametrize("stored,requested", [({"version": 2}, INTENT), (INTENT, {"version": 2})])
def test_idempotency_never_reinterprets_a_saved_document_selector(stored, requested):
    from fastapi import HTTPException

    with pytest.raises(HTTPException) as failure:
        services._require_same_output_capture({"output_capture": stored}, OutputCaptureIntent.model_validate(requested))
    assert failure.value.status_code == 409


@pytest.mark.asyncio
async def test_actual_creating_service_refuses_missing_qualifier_before_preparation(admission):
    from fastapi import HTTPException

    e = admission
    with pytest.raises(HTTPException) as failure:
        await launched(e, ordinary(output_capture=INTENT))
    assert failure.value.status_code == 503
    assert e.preparations == e.inputs == []
    assert await e.repository.list_by_thread(e.thread_id, user_id=OWNER) == []


@pytest.mark.asyncio
async def test_actual_creating_service_retains_intent_and_original_host_qualifier(admission):
    e = admission

    async def not_a_worker_proof(request):
        pytest.fail("admission must not invoke the actual worker's qualifier")

    qualifier = OutputViewQualifier(not_a_worker_proof)
    e.request.app.state.qualify_output_view = qualifier
    record = await launched(e, ordinary(output_capture=INTENT))
    saved = await e.repository.get(record.run_id, user_id=OWNER)
    assert saved["kwargs"]["output_capture"] == INTENT
    assert e.inputs[0]["ctx"].qualify_output_view is qualifier
    assert len(e.preparations) == len(e.inputs) == 1


def client_for(respond, *, document_version=2, capture_inventory=True):
    owner = GatewayClientOwner(gateway_url="https://gateway.example", langgraph_url="https://runtime.example/prefix/api", transport=httpx.MockTransport(respond))
    scope = ChannelExecutionScope(PUBLICATION, "human", "connection", "chat", "thread", owner.endpoint_id, "lead_agent", None)
    client = ChannelExecutionFacade(owner, scope=scope, admission=Admission(), capture_output=True, capture_inventory=capture_inventory, inventory_document_version=document_version)
    return client, owner


@pytest.mark.asyncio
@pytest.mark.parametrize("version", [None, True, False, 0, 2.0, "2", 3])
async def test_facade_selector_is_literal_and_cannot_create_transport(version):
    calls = []
    with pytest.raises(ChannelExecutionProtocolError):
        client_for(lambda request: calls.append(request), document_version=version)
    assert not calls


@pytest.mark.asyncio
async def test_selector_two_requires_inventory_and_is_readonly():
    with pytest.raises(ChannelExecutionProtocolError):
        client_for(lambda request: httpx.Response(200), capture_inventory=False)
    client, owner = client_for(lambda request: httpx.Response(200))
    try:
        assert client.inventory_document_version == 2
        with pytest.raises(AttributeError):
            client.inventory_document_version = 1
    finally:
        await owner.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [200, 422, 503])
async def test_explicit_creation_is_one_original_post_without_downgrade(status):
    calls = []
    client, owner = client_for(lambda request: calls.append(request) or httpx.Response(status, json={}))
    try:
        if status == 200:
            await client.runs.wait("thread", "lead_agent", **creating())
        else:
            with pytest.raises(ChannelExecutionProtocolError):
                await client.runs.wait("thread", "lead_agent", **creating())
        assert len(calls) == 1 and calls[0].method == "POST"
        assert json.loads(calls[0].content)["output_capture"] == INTENT
    finally:
        await owner.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("ack", [None, "1", "2"])
@pytest.mark.parametrize("status", [200, 404])
async def test_explicit_read_requires_two_ack_even_for_supported_missing(ack, status):
    calls = []
    headers = {ACK: "1", **({VERSION: ack} if ack else {})}
    client, owner = client_for(lambda request: calls.append(request) or httpx.Response(status, headers=headers, json=inventory_projection()))
    try:
        if ack == "2":
            result = await client.get_run_output("run")
            assert result is None if status == 404 else result.inventory.document.version == 1
        else:
            with pytest.raises(ChannelExecutionProtocolError):
                await client.get_run_output("run")
        assert len(calls) == 1 and calls[0].headers[REQUIRE] == "2"
    finally:
        await owner.stop()


@pytest.mark.asyncio
async def test_protocol_one_facade_refuses_a_mislabelled_document_two():
    body = inventory_projection()
    body["inventory"]["document"] = document_two(body["inventory"]["document"])
    body["inventory"]["evidence"] = build_event_inventory_evidence(body["output"]["output_evidence"], status="success", inventory_event_seq=10, document=body["inventory"]["document"]).model_dump(mode="json")
    client, owner = client_for(lambda request: httpx.Response(200, headers={ACK: "1", VERSION: "1"}, json=body), document_version=1)
    try:
        with pytest.raises(ChannelExecutionProtocolError):
            await client.get_run_output("run")
    finally:
        await owner.stop()


def document_two(document):
    result = dict(document, version=2)
    result.pop("scan")
    result["output_view"] = {"kind": "no_admitted_output_view", "scope": {"version": 1, "kind": "managed_output_routing", "virtual_root": "/mnt/user-data/outputs"}}
    return result


async def seed_pair(e, *, selector=2, document_version=2, terminal="success"):
    intent = INTENT if selector == 2 else {"version": 2}
    record = await e.manager.create_or_reject(
        "thread",
        "lead_agent",
        user_id=OWNER,
        kwargs={
            "output_capture": intent,
            "channel_execution": {"version": 1, "publication_id": PUBLICATION, "owner_user_id": OWNER, "thread_id": "thread", "assistant_id": "lead_agent", "agent_name": None},
        },
    )
    if terminal == "pending":
        return record
    if terminal == "failed":
        assert await e.manager.fail_start_if_pending(record.run_id, error="controlled admission failure")
        return record
    assert await e.manager.try_start(record.run_id)
    rows = await put_messages(e, [HumanMessage(id="human", content="input", additional_kwargs={"channel_turn_id": PUBLICATION}).model_dump(), AIMessage(id="actual-ai", content=TEXT).model_dump()], owner=OWNER, run_id=record.run_id)
    receipt = TextOutputReceipt(
        version=1, kind="text", owner_user_id=OWNER, thread_id="thread", run_id=record.run_id, input_message_id="human", assistant_message_id="actual-ai", event_seq=rows[-1]["seq"], text_sha256=canonical_text_digest(TEXT)
    )
    document = inventory_document(owner_user_id=OWNER, run_id=record.run_id, output_receipt_sha256=output_receipt_digest(receipt))
    if document_version == 2:
        document = document_two(document)
    from test_output_inventory_api import put_inventory

    event, _ = await put_inventory(e, document, run_id=record.run_id, owner=OWNER)
    evidence = build_event_inventory_evidence(receipt, status="success", inventory_event_seq=event["seq"], document=document)
    result = await e.manager.finalize_output(record.run_id, RunStatus.success, output_receipt=receipt, inventory_evidence=evidence)
    assert result.output_receipt_confirmed and result.inventory_evidence_confirmed
    return record


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal", ["pending", "failed", "success"])
async def test_protocol_one_refuses_saved_selector_two_without_ack(route_environment, terminal):
    e = route_environment
    record = await seed_pair(e, terminal=terminal)
    response = await e.client.get(f"/api/threads/thread/runs/{record.run_id}/output", headers={HEADER: "1", REQUIRE: "1"})
    assert response.status_code == 503 and VERSION not in response.headers and ACK not in response.headers
    # The original text-only API remains available without inventory negotiation.
    ordinary = await e.client.get(f"/api/threads/thread/runs/{record.run_id}/output", headers={HEADER: "1"})
    assert ordinary.status_code == 200 and VERSION not in ordinary.headers


@pytest.mark.asyncio
@pytest.mark.parametrize("selector,document_version", [(1, 1), (2, 2), (1, 2), (2, 1)])
async def test_protocol_two_joins_document_to_actual_saved_selector(route_environment, selector, document_version):
    e = route_environment
    record = await seed_pair(e, selector=selector, document_version=document_version)
    response = await e.client.get(f"/api/threads/thread/runs/{record.run_id}/output", headers={HEADER: "1", REQUIRE: "2"})
    if selector == document_version:
        assert response.status_code == 200 and response.headers[VERSION] == "2"
        assert response.json()["inventory"]["document"]["version"] == document_version
    else:
        assert response.status_code == 503 and VERSION not in response.headers and ACK not in response.headers


@pytest.mark.asyncio
async def test_protocol_two_supported_missing_requires_real_inventory_backend(route_environment):
    e = route_environment
    response = await e.client.get("/api/threads/thread/runs/missing/output", headers={HEADER: "1", REQUIRE: "2"})
    assert response.status_code == 404 and response.headers[VERSION] == "2"
    from deerflow.runtime.events.store.memory import MemoryRunEventStore

    e.app.state.run_event_store = MemoryRunEventStore()
    response = await e.client.get("/api/threads/thread/runs/missing/output", headers={HEADER: "1", REQUIRE: "2"})
    assert response.status_code == 503 and VERSION not in response.headers and ACK not in response.headers
