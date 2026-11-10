"""The generic capability facade over the actual owner and recorded HTTP."""

import asyncio
import json
import threading
from dataclasses import replace
from types import SimpleNamespace

import httpx
import pytest
from _channel_execution_test_support import gateway_environment as gateway_environment
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_channel_http_owner import RecordingStream
from test_gateway_channel_execution_reads import environment as environment

from app.channels.execution import ChannelExecutionBusyError, ChannelExecutionProtocolError, ChannelExecutionScope
from app.telegram_bots.gateway_client import GatewayClientOwner

pytestmark = pytest.mark.asyncio

PUBLICATION = "a" * 32
STAMP = "2026-10-25T00:00:00Z"
ACK = {"X-DeerFlow-Channel-Execution-Version": "1"}


class Admission:
    def __init__(self):
        self.permitted = {"thread_read", "run_create", "run_read", "source_read"}
        self.calls = []

    async def authorize(self, operation):
        self.calls.append(operation)
        if operation not in self.permitted:
            raise ChannelExecutionProtocolError()

    def current(self, operation):
        return operation in self.permitted


def facade(respond):
    from app.channels.execution_client import ChannelExecutionFacade

    owner = GatewayClientOwner(gateway_url="https://gateway.example", langgraph_url="https://runtime.example/prefix/api", transport=httpx.MockTransport(respond))
    scope = ChannelExecutionScope(PUBLICATION, "human", "connection", "chat", "thread", owner.endpoint_id, "lead_agent", None)
    admission = Admission()
    return ChannelExecutionFacade(owner, scope=scope, admission=admission), owner, admission, scope


def row(identity="run", *, publication=PUBLICATION, **changes):
    return {
        "run_id": identity,
        "thread_id": "thread",
        "assistant_id": "lead_agent",
        "status": "success",
        "created_at": STAMP,
        "kwargs": {"channel_execution": {"version": 1, "publication_id": publication, "owner_user_id": "human", "thread_id": "thread", "assistant_id": "lead_agent", "agent_name": None}},
        **changes,
    }


def creating():
    return dict(input={"messages": [{"role": "user", "content": "synthetic input"}]}, metadata={"channel_turn_id": PUBLICATION}, channel_execution={"version": 1, "publication_id": PUBLICATION}, headers={"Idempotency-Key": PUBLICATION})


def answered():
    return httpx.Response(200, headers={"Content-Type": "text/event-stream"}, content=b'event: values\ndata: {"part":1}\n\n')


@pytest.mark.parametrize("binding", ["instance", "other", None])
async def test_instance_scope_requires_authoritative_thread_binding(binding):
    from app.channels.execution_client import ChannelExecutionFacade

    response = {"thread_id": "thread", "owner_user_id": "human", "agent_instance": {"id": binding} if binding else None, "metadata": {"agent_instance_id": "instance"}}
    client, owner, admission, scope = facade(lambda request: httpx.Response(200, json=response))
    try:
        instance = ChannelExecutionFacade(owner, scope=replace(scope, instance_id="instance"), admission=admission)
        if binding == "instance":
            assert await instance.threads.get("thread") == response
        else:
            with pytest.raises(ChannelExecutionProtocolError):
                await instance.threads.get("thread")
        # An ordinary scope cannot adopt an instance just because the receipt's
        # custom-agent slot is also None.
        with pytest.raises(ChannelExecutionProtocolError):
            await client.threads.get("thread")
    finally:
        await owner.stop()


async def create(client, assistant="lead_agent", **payload):
    """One creating POST, read to its first frame as the manager reads it."""
    stream = client.runs.stream("thread", assistant, **(payload or creating()))
    try:
        return await anext(stream)
    finally:
        await stream.aclose()


@pytest.mark.parametrize("assistant,agent,supported", [("custom-agent", None, False), ("lead_agent", None, True), ("lead_agent", "custom-agent", True)])
async def test_v1_facade_requires_a_supported_canonical_factory_before_any_post(assistant, agent, supported):
    from app.channels.execution_client import ChannelExecutionFacade

    calls = []
    owner = GatewayClientOwner(gateway_url="https://gateway.example", langgraph_url="https://runtime.example/prefix/api", transport=httpx.MockTransport(lambda request: calls.append(request) or answered()))
    scope = ChannelExecutionScope(PUBLICATION, "human", "connection", "chat", "thread", owner.endpoint_id, assistant, agent)

    async def execute():
        client = ChannelExecutionFacade(owner, scope=scope, admission=Admission())
        return await create(client, assistant)

    try:
        if supported:
            await execute()
            assert len(calls) == 1
            wire = json.loads(calls[0].content)
            assert wire["assistant_id"] == "lead_agent" and wire["context"]["agent_name"] == agent
        else:
            with pytest.raises(ChannelExecutionProtocolError):
                await execute()
            assert not calls
    finally:
        await owner.stop()


async def test_creating_stream_sends_typed_snapshot_with_original_target_and_explicit_retention_policy():
    calls = []

    def respond(request):
        calls.append(request)
        return answered()

    client, owner, admission, scope = facade(respond)
    try:
        assert (await create(client)).data == {"part": 1}
        (request,) = calls
        body = json.loads(request.content)
        assert body["channel_execution"] == {"version": 1, "publication_id": PUBLICATION}
        assert body["on_disconnect"] == "continue" and body.get("on_completion") is None
        assert body["assistant_id"] == "lead_agent"
        assert request.headers["Idempotency-Key"] == PUBLICATION
        assert request.url.path == "/prefix/api/threads/thread/runs/stream"
        assert admission.calls == ["run_create", "run_read"]
    finally:
        await owner.stop()


@pytest.mark.parametrize("status,headers", [(200, {}), (404, {}), (404, {"X-DeerFlow-Channel-Execution-Version": "2"}), (503, ACK)])
async def test_unacknowledged_or_failed_reads_never_prove_absence(status, headers):
    client, owner, _, _ = facade(lambda request: httpx.Response(status, json=row(), headers=headers))
    try:
        with pytest.raises(ChannelExecutionProtocolError):
            await client.get_run("run")
    finally:
        await owner.stop()


async def test_only_scoped_acknowledged_exact_miss_returns_none():
    calls = []
    client, owner, _, _ = facade(lambda request: calls.append(request) or httpx.Response(404, json={"detail": "missing"}, headers=ACK))
    try:
        assert await client.get_run("run") is None
        assert calls[0].headers["X-DeerFlow-Require-Channel-Execution"] == "1"
    finally:
        await owner.stop()


async def test_acknowledged_200_null_is_not_an_exact_miss():
    client, owner, _, _ = facade(lambda request: httpx.Response(200, content=b"null", headers=ACK))
    try:
        with pytest.raises(ChannelExecutionProtocolError):
            await client.get_run("run")
    finally:
        await owner.stop()


async def test_scope_is_readonly_after_the_facade_is_constructed():
    client, owner, _, scope = facade(lambda request: httpx.Response(200, json={}))
    try:
        with pytest.raises(AttributeError):
            client.scope = replace(scope, owner_user_id="other-human")
    finally:
        await owner.stop()


@pytest.mark.parametrize("changes", [{"thread_id": "other"}, {"run_id": "other"}, {"kwargs": {"channel_execution": None}}, {"created_at": "bad"}])
async def test_exact_run_rejects_malformed_or_retargeted_server_evidence(changes):
    client, owner, _, _ = facade(lambda request: httpx.Response(200, json=row(**changes), headers=ACK))
    try:
        with pytest.raises(ChannelExecutionProtocolError):
            await client.get_run("run")
    finally:
        await owner.stop()


@pytest.mark.parametrize(
    "payload",
    [
        {"data": [], "has_more": True, "next_before_created_at": STAMP, "next_before_run_id": "a"},
        {"data": [row("a"), row("a")], "has_more": False},
        {"data": [row("a"), row("b")], "has_more": False},
        {"data": [row()], "has_more": True, "next_before_created_at": STAMP, "next_before_run_id": "other"},
        {"data": [], "has_more": 1},
    ],
)
async def test_bad_run_cursor_or_page_cannot_be_exhausted_as_absence(payload):
    client, owner, _, _ = facade(lambda request: httpx.Response(200, json=payload, headers=ACK))
    try:
        with pytest.raises(ChannelExecutionProtocolError):
            await client.list_runs_page()
    finally:
        await owner.stop()


@pytest.mark.parametrize("detail,busy", [({"detail": {"code": "thread_busy"}}, True), ({"detail": "busy"}, False), ({"detail": {"code": "idempotency_mismatch"}}, False), ({"detail": {"code": "thread_busy", "extra": True}}, False)])
async def test_only_exact_typed_conflict_is_busy(detail, busy):
    calls = []
    client, owner, _, _ = facade(lambda request: calls.append(request) or httpx.Response(409, json=detail))
    try:
        with pytest.raises(ChannelExecutionBusyError if busy else ChannelExecutionProtocolError):
            await create(client)
        assert len(calls) == 1
    finally:
        await owner.stop()


async def test_dispatched_stream_uses_read_authority_after_new_run_admission_closes():
    response = RecordingStream()
    calls = []
    client, owner, admission, _ = facade(lambda request: calls.append(request) or httpx.Response(200, headers={"Content-Type": "text/event-stream"}, stream=response))
    stream = client.runs.stream("thread", "lead_agent", **creating())
    try:
        part = await anext(stream)
        owner.close_run_admission()
        admission.permitted.remove("run_create")
        assert part.event == "values"
        assert admission.calls == ["run_create", "run_read"]
        assert len(calls) == 1
        await stream.aclose()
        assert response.closed
    finally:
        await stream.aclose()
        await owner.stop()


async def test_a_queued_stream_part_rechecks_current_read_authority_before_handoff():
    response = RecordingStream(data=b'event: values\ndata: {"part":1}\n\nevent: values\ndata: {"part":2}\n\n')
    client, owner, admission, _ = facade(lambda request: httpx.Response(200, headers={"Content-Type": "text/event-stream"}, stream=response))
    stream = client.runs.stream("thread", "lead_agent", **creating())
    try:
        assert (await anext(stream)).data == {"part": 1}
        # The pump has authorized and queued part two before the transport asks
        # for more bytes. Revocation must still win at the consumer handoff.
        await asyncio.wait_for(response.entered.wait(), 3)
        admission.permitted.remove("run_read")
        with pytest.raises(ChannelExecutionProtocolError):
            await anext(stream)
        assert response.closed
    finally:
        await stream.aclose()
        await owner.stop()


async def test_frames_of_one_second_share_one_full_check(monkeypatch):
    from app.channels import execution_client

    monkeypatch.setattr(execution_client, "time", SimpleNamespace(monotonic=lambda: 100.0))
    response = RecordingStream(data=b"".join(b'event: values\ndata: {"part":%d}\n\n' % number for number in range(1, 6)))
    client, owner, admission, _ = facade(lambda request: httpx.Response(200, headers={"Content-Type": "text/event-stream"}, stream=response))
    stream = client.runs.stream("thread", "lead_agent", **creating())
    try:
        assert [(await anext(stream)).data["part"] for _ in range(5)] == [1, 2, 3, 4, 5]
        assert admission.calls == ["run_create", "run_read"]
    finally:
        await stream.aclose()
        await owner.stop()


async def test_a_revocation_only_the_database_knows_stops_frames_within_a_second(monkeypatch):
    from app.channels import execution_client

    clock = [100.0]
    monkeypatch.setattr(execution_client, "time", SimpleNamespace(monotonic=lambda: clock[0]))
    client, owner, admission, _ = facade(lambda request: httpx.Response(200))

    async def revoked(operation):
        raise ChannelExecutionProtocolError()

    try:
        await client._authorize_frame()
        admission.authorize = revoked
        clock[0] += 0.9
        await client._authorize_frame()
        clock[0] += 0.1
        with pytest.raises(ChannelExecutionProtocolError):
            await client._authorize_frame()
    finally:
        await owner.stop()


async def test_a_revocation_held_in_memory_stops_the_next_frame():
    client, owner, admission, _ = facade(lambda request: httpx.Response(200))
    try:
        await client._authorize_frame()
        admission.permitted.remove("run_read")
        with pytest.raises(ChannelExecutionProtocolError):
            await client._authorize_frame()
    finally:
        await owner.stop()


@pytest.mark.parametrize("snapshot_finished", [False, True])
@pytest.mark.parametrize("field,original_value,changed_value", [("agent_name", None, "other-target"), ("owner_user_id", "human", "other-human")])
async def test_mutable_caller_target_cannot_replace_the_validated_wire_snapshot(monkeypatch, snapshot_finished, field, original_value, changed_value):
    from app.channels import execution_client as module

    entered, release = threading.Event(), threading.Event()
    actual = module._snapshot

    def snapshot(value):
        private = actual(value) if snapshot_finished else None
        entered.set()
        release.wait(10)
        return private if snapshot_finished else actual(value)

    monkeypatch.setattr(module, "_snapshot", snapshot)
    calls = []
    client, owner, _, _ = facade(lambda request: calls.append(request) or answered())
    payload = creating() | {"context": {field: original_value}}
    operation = asyncio.create_task(create(client, **payload))
    try:
        assert await asyncio.to_thread(entered.wait, 3)
        payload["context"][field] = changed_value
        release.set()
        if snapshot_finished:
            await operation
            assert len(calls) == 1
            sent = json.loads(calls[0].content)
            assert sent["context"][field] == original_value
            assert sent["channel_execution"]["publication_id"] == PUBLICATION
        else:
            with pytest.raises(ChannelExecutionProtocolError):
                await operation
            assert not calls
    finally:
        release.set()
        await asyncio.gather(operation, return_exceptions=True)
        await owner.stop()


async def test_read_authority_is_rechecked_after_the_actual_json_decoder_wait(monkeypatch):
    from app.telegram_bots import gateway_client as module

    entered, release = threading.Event(), threading.Event()
    actual = module._decode

    def decode(value):
        entered.set()
        release.wait(10)
        return actual(value)

    monkeypatch.setattr(module, "_decode", decode)
    client, owner, admission, _ = facade(lambda request: httpx.Response(200, json=row(), headers=ACK))
    operation = asyncio.create_task(client.get_run("run"))
    try:
        assert await asyncio.to_thread(entered.wait, 3)
        admission.permitted.remove("run_read")
        release.set()
        with pytest.raises(ChannelExecutionProtocolError):
            await operation
    finally:
        release.set()
        await asyncio.gather(operation, return_exceptions=True)
        await owner.stop()


def event(seq, **changes):
    return dict(thread_id="thread", run_id="run", seq=seq, category="message", event_type="llm.ai.response", content={"id": "actual-message", "type": "ai", "content": "synthetic"}, metadata={}, created_at=STAMP, **changes)


@pytest.mark.parametrize("kind", ["valid", "boolean-seq", "wrong-run", "descending", "empty-more", "outside-before", "missing-metadata"])
async def test_actual_event_envelopes_and_exclusive_sequence_bounds(kind):
    data = [event(2), event(3)]
    more = True
    if kind == "boolean-seq":
        data[0]["seq"] = True
    elif kind == "wrong-run":
        data[0]["run_id"] = "other"
    elif kind == "descending":
        data.reverse()
    elif kind == "empty-more":
        data = []
    elif kind == "outside-before":
        data[-1]["seq"] = 4
    elif kind == "missing-metadata":
        data[0].pop("metadata")
    client, owner, _, _ = facade(lambda request: httpx.Response(200, json={"data": data, "has_more": more}, headers=ACK))
    try:
        if kind == "valid":
            page = await client.list_run_messages("run", before_seq=4, after_seq=1)
            assert page.next_before_seq == 2 and page.next_after_seq == 3
            assert len(page.data) == 2
        else:
            with pytest.raises(ChannelExecutionProtocolError):
                await client.list_run_messages("run", before_seq=4, after_seq=1)
    finally:
        await owner.stop()


async def test_facade_reads_actual_strict_routes_and_stores_with_session_auth_standin(environment):
    from test_gateway_channel_execution_reads import OWNER, THREAD

    from app.channels.execution_client import ChannelExecutionFacade

    e = environment
    owner = GatewayClientOwner(gateway_url="http://gateway.test", langgraph_url="http://runtime.test/api", transport=httpx.ASGITransport(app=e.app))
    scope = ChannelExecutionScope(PUBLICATION, OWNER, None, "chat", THREAD, owner.endpoint_id, "lead_agent", None)
    receipt = dict(version=1, publication_id=PUBLICATION, owner_user_id=OWNER, thread_id=THREAD, assistant_id="lead_agent", agent_name=None)
    await e.store.put("actual-run", thread_id=THREAD, user_id=OWNER, assistant_id="lead_agent", status="success", kwargs={"channel_execution": receipt}, created_at=STAMP)
    await e.app.state.run_event_store.put(
        thread_id=THREAD, run_id="actual-run", event_type="llm.ai.response", category="message", content={"id": "stored-message", "type": "ai", "content": "synthetic stored output"}, metadata={}, created_at=STAMP
    )
    admission = Admission()
    client = ChannelExecutionFacade(owner, scope=scope, admission=admission)
    try:
        exact = await client.get_run("actual-run")
        assert exact.receipt.publication_id == PUBLICATION
        assert (await client.list_runs_page()).data[0].row["run_id"] == "actual-run"
        page = await client.list_run_messages("actual-run")
        assert len(page.data) == 1 and page.data[0]["content"]["id"] == "stored-message"
        assert await client.get_run("missing-run") is None
    finally:
        await owner.stop()


async def test_conflicting_target_on_the_config_root_is_refused_before_http():
    calls = []
    client, owner, _, _ = facade(lambda request: calls.append(request) or answered())
    try:
        with pytest.raises(ChannelExecutionProtocolError):
            await create(client, **(creating() | {"config": {"agent_name": "other-target"}}))
        assert not calls
    finally:
        await owner.stop()


@pytest.mark.parametrize("field", ["owner_user_id", "user_id"])
async def test_additive_row_owner_fields_cannot_contradict_the_scoped_acknowledgement(field):
    client, owner, _, _ = facade(lambda request: httpx.Response(200, json=row(kwargs={}, **{field: "other-human"}), headers=ACK))
    try:
        with pytest.raises(ChannelExecutionProtocolError):
            await client.get_run("run")
    finally:
        await owner.stop()


@pytest.mark.parametrize("field,value", [("user_id", "other-human"), ("owner_user_id", "other-human"), ("connection_id", "other-connection"), ("chat_id", "other-chat"), ("endpoint_id", "other-endpoint"), ("is_bootstrap", True)])
@pytest.mark.parametrize("nested", [False, True])
async def test_detached_creating_tree_refuses_conflicting_human_and_routing_scope(field, value, nested):
    calls = []
    client, owner, _, _ = facade(lambda request: calls.append(request) or answered())
    carrier = {field: value}
    payload = creating() | ({"config": {"context": {"metadata": carrier}}} if nested else {"context": carrier})
    try:
        with pytest.raises(ChannelExecutionProtocolError):
            await create(client, **payload)
        assert not calls
    finally:
        await owner.stop()


@pytest.mark.parametrize("alias,valid", [(None, True), ("lead_agent", True), ("custom-agent", True), (" Custom_Agent ", True), ("other-agent", False)])
async def test_canonical_custom_receipt_accepts_only_recognized_requested_aliases(alias, valid):
    from app.channels.execution_client import ChannelExecutionFacade

    data = row(assistant_id=alias)
    data["kwargs"]["channel_execution"]["agent_name"] = "custom-agent"
    calls = []

    def respond(request):
        calls.append(request)
        return httpx.Response(200, json={"data": [data], "has_more": False} if request.url.path.endswith("/page") else data, headers=ACK)

    _, owner, admission, scope = facade(respond)
    client = ChannelExecutionFacade(owner, scope=replace(scope, agent_name="custom-agent"), admission=admission)
    try:
        if valid:
            assert (await client.get_run("run")).receipt.agent_name == "custom-agent"
            assert (await client.list_runs_page()).data[0].row["run_id"] == "run"
        else:
            with pytest.raises(ChannelExecutionProtocolError):
                await client.get_run("run")
        assert all(request.method == "GET" for request in calls)
    finally:
        await owner.stop()


async def test_custom_scope_fills_missing_wire_selection_from_its_captured_target():
    from app.channels.execution_client import ChannelExecutionFacade

    calls = []
    _, owner, admission, scope = facade(lambda request: calls.append(request) or answered())
    client = ChannelExecutionFacade(owner, scope=replace(scope, agent_name="custom-agent"), admission=admission)
    try:
        await create(client)
        assert json.loads(calls[0].content)["context"] == {"agent_name": "custom-agent"}
    finally:
        await owner.stop()


async def test_manager_owned_http_actual_gateway_admission_and_checkpoint_without_model(gateway_environment, monkeypatch, tmp_path):
    from fastapi import FastAPI
    from test_channel_execution_checkpoint import _checkpoint_captured_input
    from test_threads_router import _RawStateAccessor

    from app.channels.execution import ChannelExecutionContext
    from app.channels.execution_client import ChannelExecutionFacade
    from app.channels.manager import ChannelManager
    from app.channels.message_bus import InboundMessage, MessageBus
    from app.channels.store import JsonChannelStore
    from app.gateway.auth_middleware import AuthMiddleware
    from app.gateway.csrf_middleware import CSRFMiddleware
    from app.gateway.routers import thread_runs, threads

    e = gateway_environment
    monkeypatch.setenv("DEER_FLOW_AUTH_DISABLED", "0")
    await e.thread_store.update_metadata(e.thread_id, {"channel_agent_name": "lead_agent"}, user_id=e.owner_id)
    app = FastAPI()
    app.add_middleware(CSRFMiddleware)
    app.add_middleware(AuthMiddleware)
    for name, value in vars(e.request.app.state).items():
        setattr(app.state, name, value)
    app.state.run_store = e.store
    app.include_router(threads.router)
    app.include_router(thread_runs.router)

    async def read_checkpoint(request, *, thread_id, assistant_id=None, checkpoint_id=None):
        # This is the existing real-checkpointer/no-model test accessor. The
        # creating route, live human, binding SQL and start_run stay unpatched.
        return _RawStateAccessor(e.checkpointer), {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}

    monkeypatch.setattr(threads, "abuild_checkpoint_state_accessor", read_checkpoint)
    monkeypatch.setattr(thread_runs, "abuild_checkpoint_state_accessor", read_checkpoint)
    calls = []

    class RecordingASGI(httpx.ASGITransport):
        async def handle_async_request(self, request):
            calls.append(request)
            return await super().handle_async_request(request)

    owner = GatewayClientOwner(gateway_url="http://gateway.test", langgraph_url="http://runtime.test/api", transport=RecordingASGI(app=app))
    scope = ChannelExecutionScope(PUBLICATION, e.owner_id, None, "chat", e.thread_id, owner.endpoint_id, "lead_agent", None)
    admission = Admission()
    client = ChannelExecutionFacade(owner, scope=scope, admission=admission)
    bus = MessageBus()
    manager = ChannelManager(bus, JsonChannelStore(str(tmp_path / "manager.json")))

    def no_default():
        raise AssertionError("A required publication cannot select the ordinary client")

    monkeypatch.setattr(manager, "_get_client", no_default)
    msg = InboundMessage(channel_name="slack", chat_id="chat", user_id="sender", text="synthetic publication input", owner_user_id=e.owner_id, execution_required=True, execution_context=ChannelExecutionContext(scope, client, admission))
    try:
        await manager.start()
        await bus.publish_inbound(msg)
        await asyncio.wait_for(bus.join_inbound(), 5)
        assert len(e.worker_inputs) == 1
        (captured,) = e.worker_inputs
        record = captured["record"]
        assert record.user_id == e.owner_id
        assert record.kwargs["channel_execution"] == dict(version=1, publication_id=PUBLICATION, owner_user_id=e.owner_id, thread_id=e.thread_id, assistant_id="lead_agent", agent_name=None)
        assert captured["graph_input"]["messages"][0].additional_kwargs["channel_turn_id"] == PUBLICATION
        assert sum(request.method == "POST" for request in calls) == 1
        creating_call = next(request for request in calls if request.method == "POST")
        assert creating_call.headers["Idempotency-Key"] == PUBLICATION
        assert creating_call.headers["Cookie"] == "csrf_token=" + creating_call.headers["X-CSRF-Token"]
        assert creating_call.url.path.endswith("/stream")
        assert (await client.list_runs_page()).data[0].row["run_id"] == record.run_id
        snapshot = await _checkpoint_captured_input(e, captured)
        visible = [message for message in snapshot.values["messages"] if message.type == "human" and not message.additional_kwargs.get("hide_from_ui")]
        assert len(visible) == 1 and visible[0].additional_kwargs["channel_turn_id"] == PUBLICATION
        # The recording worker creates no AI output. This bridge proves actual
        # admission/checkpoint provenance, never model-final completeness.
    finally:
        await manager.stop()
        await owner.stop()


@pytest.mark.parametrize("cancel_close", [False, True])
@pytest.mark.parametrize("streaming", [False, True])
async def test_close_owns_the_actual_post_response_sql_admission_before_any_late_result(db, streaming, cancel_close):
    entered, release, sql_closed = asyncio.Event(), asyncio.Event(), asyncio.Event()
    response = RecordingStream(block=False)

    def respond(request):
        if streaming:
            return httpx.Response(200, headers={"Content-Type": "text/event-stream"}, stream=response)
        return httpx.Response(200, json=row(), headers=ACK)

    client, owner, admission, _ = facade(respond)
    actual = admission.authorize
    reads = []

    async def authorize(operation):
        await actual(operation)
        reads.append(operation)
        # A plain read authorizes before it dispatches too. The admission under test comes after the response.
        if operation == "run_read" and (streaming or reads.count("run_read") == 2):
            async with db.sessions() as session:
                await session.execute(select(1))
                entered.set()
                await release.wait()
            sql_closed.set()

    admission.authorize = authorize
    stream = client.runs.stream("thread", "lead_agent", **creating()) if streaming else None
    reading = asyncio.create_task(anext(stream) if streaming else client.get_run("run"))
    closing = None
    try:
        await asyncio.wait_for(entered.wait(), 3)
        closing = asyncio.create_task(stream.aclose() if streaming else owner.stop())
        done, _ = await asyncio.wait({closing}, timeout=0.1)
        assert not done, "Close cannot leave the admitted SQL guard live outside its ownership"
        assert not sql_closed.is_set()
        if cancel_close:
            closing.cancel()
            await asyncio.sleep(0)
            closing.cancel()
            await asyncio.sleep(0)
            if not streaming:
                # Owner stop waiters are cancellable; its retained cleanup is
                # shared by the next waiter. Stream close drains before exit.
                await asyncio.gather(closing, return_exceptions=True)
                closing = asyncio.create_task(owner.stop())
            done, _ = await asyncio.wait({closing}, timeout=0.1)
            assert not done
            assert not sql_closed.is_set()
        release.set()
        (closed,) = await asyncio.wait_for(asyncio.gather(closing, return_exceptions=True), 3)
        assert closed is None or cancel_close and isinstance(closed, asyncio.CancelledError)
        assert sql_closed.is_set()
        (result,) = await asyncio.gather(reading, return_exceptions=True)
        assert isinstance(result, (StopAsyncIteration, asyncio.CancelledError, ChannelExecutionProtocolError)), "A result cannot escape after its owning close"
    finally:
        release.set()
        if closing is not None:
            await asyncio.gather(closing, return_exceptions=True)
        await asyncio.gather(reading, return_exceptions=True)
        if stream is not None:
            await stream.aclose()
        await owner.stop()


async def test_facade_close_joins_an_already_started_snapshot_worker(monkeypatch):
    from app.channels import execution_client as module

    entered, release = threading.Event(), threading.Event()
    actual = module._snapshot

    def snapshot(value):
        entered.set()
        release.wait(10)
        return actual(value)

    monkeypatch.setattr(module, "_snapshot", snapshot)
    calls = []
    client, owner, _, _ = facade(lambda request: calls.append(request) or httpx.Response(200, json={}))
    stream = client.runs.stream("thread", "lead_agent", **creating())
    reading = asyncio.create_task(anext(stream))
    closing = None
    try:
        assert await asyncio.to_thread(entered.wait, 3)
        closing = asyncio.create_task(stream.aclose())
        done, _ = await asyncio.wait({closing}, timeout=0.1)
        assert not done, "The stream cannot detach its active private snapshot worker"
        release.set()
        await asyncio.wait_for(closing, 3)
        (result,) = await asyncio.gather(reading, return_exceptions=True)
        assert isinstance(result, (asyncio.CancelledError, StopAsyncIteration, ChannelExecutionProtocolError))
        assert not calls
    finally:
        release.set()
        if closing is not None:
            await asyncio.gather(closing, return_exceptions=True)
        await asyncio.gather(reading, return_exceptions=True)
        await stream.aclose()
        await owner.stop()
