"""Host capability admission at the actual channel-manager entry point."""

import asyncio
import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import httpx
import pytest

from app.channels.execution import ChannelExecutionBusyError, ChannelExecutionContext, ChannelExecutionProtocolError, ChannelExecutionScope
from app.channels.manager import ChannelManager, _human_input_message
from app.channels.message_bus import InboundMessage, InboundMessageType, MessageBus
from app.channels.run_policy import CHANNEL_RUN_POLICY, ChannelRunPolicy
from app.channels.store import ChannelStore


@pytest.mark.asyncio
@pytest.mark.parametrize("required,context", [(True, None), (True, {}), (False, {}), (1, None), (None, None)])
async def test_required_or_malformed_host_context_refuses_before_ordinary_client(tmp_path, monkeypatch, required, context):
    manager = ChannelManager(MessageBus(), ChannelStore(str(tmp_path / "channels.json")))
    message = InboundMessage(channel_name="slack", chat_id="chat", user_id="sender", text="hello")
    # The pre-change dataclass permits these host attributes, but the actual
    # manager ignores them. This is a behavioral red, not a missing-API probe.
    message.execution_required = required
    message.execution_context = context
    ordinary = Mock(side_effect=RuntimeError("Unexpected ordinary client selection"))
    monkeypatch.setattr(manager, "_get_client", ordinary)

    with pytest.raises(RuntimeError, match="^Channel execution protocol refused$"):
        await manager._handle_chat(message)

    ordinary.assert_not_called()


class Admission:
    def __init__(self):
        self.allowed = True
        self.calls = []

    async def authorize(self, operation):
        self.calls.append(operation)
        if not self.allowed:
            raise RuntimeError("Synthetic private authority detail")

    def current(self, operation):
        return self.allowed


class OwnedStream:
    def __init__(self, parts=()):
        self.parts = iter(parts)
        self.closed = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self.parts)
        except StopIteration:
            raise StopAsyncIteration from None

    async def aclose(self):
        self.closed = True


@pytest.fixture
def setup(tmp_path, monkeypatch):
    bus = MessageBus()
    outbounds = []

    async def outbound(message):
        outbounds.append(message)

    bus.subscribe_outbound(outbound)
    manager = ChannelManager(bus, ChannelStore(str(tmp_path / "channels.json")))
    scope = ChannelExecutionScope("a" * 32, "owner", "connection", "chat", "original-thread", "original-endpoint", "lead_agent", None)
    admission = Admission()
    stream = OwnedStream([SimpleNamespace(event="values", data={"messages": [{"type": "ai", "content": "answer"}]}, id=None)])
    client = SimpleNamespace(
        threads=SimpleNamespace(get=AsyncMock(return_value={"thread_id": scope.thread_id})), runs=SimpleNamespace(wait=AsyncMock(return_value={"messages": [{"type": "ai", "content": "answer"}]}), stream=Mock(return_value=stream))
    )
    context = ChannelExecutionContext(scope, client, admission)
    message = InboundMessage(channel_name="slack", chat_id="chat", user_id="sender", text="hello", owner_user_id="owner", connection_id="connection", execution_required=True, execution_context=context)
    ordinary = Mock(side_effect=AssertionError("No ordinary client fallback"))
    monkeypatch.setattr(manager, "_get_client", ordinary)
    monkeypatch.setattr(manager, "_channel_supports_streaming", lambda _channel: False)
    return SimpleNamespace(manager=manager, message=message, context=context, client=client, admission=admission, stream=stream, outbounds=outbounds, ordinary=ordinary)


@pytest.mark.asyncio
@pytest.mark.parametrize("required", [False, True])
@pytest.mark.parametrize("streaming", [False, True])
async def test_valid_host_context_uses_original_thread_and_typed_creating_path(setup, monkeypatch, required, streaming):
    e = setup
    e.message.execution_required = required
    # Neither the current map, remembered target nor ordinary metadata PATCH is
    # authority for this already-prepared original publication.
    monkeypatch.setattr(e.manager, "_lookup_thread_id", AsyncMock(side_effect=AssertionError("No latest mapping")))
    e.manager._thread_agent_names[e.context.thread_id] = "different-agent"
    monkeypatch.setattr(e.manager, "_channel_supports_streaming", lambda _channel: streaming)
    await e.manager._handle_chat(e.message)
    e.ordinary.assert_not_called()
    e.client.threads.get.assert_awaited_once_with(e.context.thread_id)
    invocation = (e.client.runs.stream if streaming else e.client.runs.wait).call_args
    assert invocation.args == (e.context.thread_id, e.context.assistant_id)
    assert invocation.kwargs["channel_execution"] == {"version": 1, "publication_id": e.context.publication_id}
    assert invocation.kwargs["metadata"] == {"channel_turn_id": e.context.publication_id}
    assert invocation.kwargs["headers"] == {"Idempotency-Key": e.context.publication_id}
    assert invocation.kwargs["on_disconnect"] == "continue" and invocation.kwargs["on_completion"] is None
    (human,) = invocation.kwargs["input"]["messages"]
    assert human["additional_kwargs"]["channel_turn_id"] == e.context.publication_id
    assert e.outbounds[-1].thread_id == e.context.thread_id
    assert e.outbounds[-1].connection_id == e.context.connection_id
    assert e.stream.closed is streaming


@pytest.mark.asyncio
@pytest.mark.parametrize("attribute,value", [("owner_user_id", "other"), ("connection_id", "other"), ("chat_id", "other")])
async def test_host_scope_mismatch_refuses_before_thread_or_run(setup, attribute, value):
    e = setup
    setattr(e.message, attribute, value)
    with pytest.raises(ChannelExecutionProtocolError):
        await e.manager._handle_chat(e.message)
    e.client.threads.get.assert_not_called()
    e.client.runs.wait.assert_not_called()
    e.ordinary.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", ["get", "wait", "stream", "async-stream"])
async def test_incomplete_or_wrong_shape_facade_refuses_before_any_http(setup, missing):
    e = setup
    if missing == "get":
        e.client.threads.get = None
    elif missing == "async-stream":
        e.client.runs.stream = AsyncMock()
    else:
        setattr(e.client.runs, missing, None)
    with pytest.raises(ChannelExecutionProtocolError):
        await e.manager._handle_chat(e.message)
    e.ordinary.assert_not_called()
    if missing != "get":
        e.client.threads.get.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "carrier",
    [
        "metadata-assistant",
        "metadata-agent",
        "default-assistant",
        "channel-assistant",
        "user-assistant",
        "default-context",
        "default-config-context",
        "default-config-configurable",
        "extra-context",
        "bootstrap",
        "publication",
        "human-publication",
    ],
)
async def test_every_explicit_target_or_publication_conflict_refuses_before_http(setup, carrier):
    e = setup
    extra = None
    if carrier == "metadata-assistant":
        e.message.metadata["assistant_id"] = "other"
    elif carrier == "metadata-agent":
        e.message.metadata["agent_name"] = "other"
    elif carrier == "default-assistant":
        e.manager._default_session["assistant_id"] = "other"
    elif carrier == "channel-assistant":
        e.manager._channel_sessions["slack"] = {"assistant_id": "other"}
    elif carrier == "user-assistant":
        e.manager._channel_sessions["slack"] = {"users": {"sender": {"assistant_id": "other"}}}
    elif carrier == "default-context":
        e.manager._default_session["context"] = {"agent_name": "other"}
    elif carrier == "default-config-context":
        e.manager._default_session["config"] = {"context": {"agent_name": "other"}}
    elif carrier == "default-config-configurable":
        e.manager._default_session["config"] = {"configurable": {"agent_name": "other"}}
    elif carrier == "extra-context":
        extra = {"agent_name": "other"}
    elif carrier == "bootstrap":
        e.manager._default_session["context"] = {"is_bootstrap": True}
    elif carrier == "publication":
        e.message.metadata["channel_turn_id"] = "b" * 32
    else:
        e.message.metadata["human_message_kwargs"] = {"channel_turn_id": "b" * 32}
    with pytest.raises(ChannelExecutionProtocolError):
        await e.manager._handle_chat(e.message, extra_context=extra)
    e.client.threads.get.assert_not_called()
    e.client.runs.wait.assert_not_called()


@pytest.mark.asyncio
async def test_required_command_context_cannot_escape_to_ordinary_command_handler(setup, monkeypatch, caplog):
    e = setup
    e.message.msg_type = InboundMessageType.COMMAND
    e.message.text = "/new"
    await e.manager._handle_message(e.message)
    e.ordinary.assert_not_called()
    assert e.outbounds[-1].text == "Channel execution protocol refused"
    assert f"Channel execution refused channel={e.message.channel_name} publication={e.message.execution_context.publication_id} cause=ChannelExecutionProtocolError:" in caplog.text


@pytest.mark.asyncio
async def test_a_refusal_for_a_malformed_context_still_answers_and_releases_the_message(setup, monkeypatch, caplog):
    e = setup
    released = []

    async def release(message):
        released.append(message)

    monkeypatch.setattr(e.manager, "_release_inbound_dedupe_key", release)
    e.message.execution_context = object()
    await e.manager._handle_message(e.message)
    assert released == [e.message]
    assert e.outbounds[-1].text == "Channel execution protocol refused"
    assert "publication=None cause=ChannelExecutionProtocolError:" in caplog.text


@pytest.mark.asyncio
async def test_creation_policy_without_correlated_capability_refuses_before_http(setup, monkeypatch):
    e = setup
    monkeypatch.setitem(CHANNEL_RUN_POLICY, "slack", ChannelRunPolicy(fire_and_forget=True))
    with pytest.raises(ChannelExecutionProtocolError):
        await e.manager._handle_chat(e.message)
    e.client.threads.get.assert_not_called()


def test_human_extras_preserve_forwarding_and_actual_original_content_and_files():
    extra = {"channel_turn_id": "a" * 32, "forwarded": [{"from": "sender"}], "original_user_content": "caller", "files": [{"filename": "retained"}]}
    message = _human_input_message("prepared", original_content="actual original", files=[{"filename": "uploaded"}], extra_kwargs=extra)
    assert message["additional_kwargs"] == extra | {"original_user_content": "actual original", "files": [{"filename": "retained"}, {"filename": "uploaded"}]}
    assert extra["original_user_content"] == "caller" and extra["files"] == [{"filename": "retained"}]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("channel_agent_name", "other"),
        ("agent_instance", {"id": "instance"}),
        ("agent_instance_id", "instance"),
        ("user_id", "other"),
        ("thread_id", "new-thread"),
        ("owner_user_id", "other"),
        ("connection_id", "other"),
        ("chat_id", "other"),
        ("endpoint_id", "other"),
    ],
)
async def test_additional_canonical_target_and_scope_carriers_refuse_before_http(setup, field, value):
    e = setup
    e.manager._default_session["context"] = {field: value}
    with pytest.raises(ChannelExecutionProtocolError):
        await e.manager._handle_chat(e.message)
    e.client.threads.get.assert_not_called()


@pytest.mark.asyncio
async def test_file_preparation_keeps_the_original_destination_and_human_extras(setup, monkeypatch):
    from app.channels import manager as module
    from app.channels import service

    e = setup
    e.message.files = [{"filename": "incoming"}]
    e.message.metadata["human_message_kwargs"] = {"forwarded": [{"from": "sender"}], "original_user_content": "supplied", "files": [{"filename": "retained"}]}
    e.client.threads.upload = AsyncMock()

    async def receive(msg, thread, *, user_id):
        assert thread == e.context.thread_id and user_id == e.context.owner_user_id
        return replace(msg, text="downloaded", owner_user_id="other", connection_id="other", chat_id="other", execution_context=None)

    async def ingest(thread, msg, *, user_id, upload, saved_files):
        assert (msg.chat_id, msg.connection_id, msg.owner_user_id) == ("chat", "connection", "owner")
        assert msg.execution_context is e.context
        assert upload is e.client.threads.upload, "A managed chat's files take the route the web uses"
        assert saved_files is e.context.saved_files, "Whoever makes the attempts keeps what they saved"
        msg.text = "prepared with uploaded path"
        return [{"filename": "uploaded"}]

    monkeypatch.setattr(service, "get_channel_service", lambda: SimpleNamespace(get_channel=lambda _name: SimpleNamespace(receive_file=receive)))
    monkeypatch.setattr(module, "_ingest_inbound_files", ingest)
    await e.manager._handle_chat(e.message)
    (human,) = e.client.runs.wait.call_args.kwargs["input"]["messages"]
    assert human["content"] == "prepared with uploaded path"
    assert human["additional_kwargs"] == {"channel_turn_id": e.context.publication_id, "forwarded": [{"from": "sender"}], "original_user_content": "downloaded", "files": [{"filename": "retained"}, {"filename": "uploaded"}]}
    assert (e.outbounds[-1].chat_id, e.outbounds[-1].connection_id, e.outbounds[-1].owner_user_id) == ("chat", "connection", "owner")


@pytest.mark.asyncio
async def test_actual_worker_reports_required_context_refusal_without_default_client(setup):
    e = setup
    e.message.execution_context = None
    await e.manager.start()
    try:
        await e.manager.bus.publish_inbound(e.message)
        await asyncio.wait_for(e.manager.bus.join_inbound(), 2)
        e.ordinary.assert_not_called()
        assert [message.text for message in e.outbounds] == ["Channel execution protocol refused"]
    finally:
        await e.manager.stop()


@pytest.mark.asyncio
async def test_worker_captures_the_context_before_async_dedupe_preparation(setup, monkeypatch):
    e = setup
    e.message.workspace_id = "workspace"
    e.message.metadata["event_id"] = "event"
    entered, release = asyncio.Event(), asyncio.Event()

    async def record(_key):
        entered.set()
        await release.wait()
        return False

    monkeypatch.setattr(e.manager._inbound_dedupe_store, "try_record", record)
    await e.manager.start()
    try:
        await e.manager.bus.publish_inbound(e.message)
        await asyncio.wait_for(entered.wait(), 2)
        e.message.execution_context = replace(e.context, scope=replace(e.context.scope, publication_id="b" * 32), admission=Admission())
        e.admission.allowed = False
        release.set()
        await asyncio.wait_for(e.manager.bus.join_inbound(), 2)
        e.client.threads.get.assert_not_called()
        e.client.runs.wait.assert_not_called()
        assert [message.text for message in e.outbounds] == ["Channel execution protocol refused"]
    finally:
        release.set()
        await e.manager.stop()


@pytest.mark.asyncio
async def test_context_capture_precedes_a_bound_identity_await(setup, monkeypatch):
    e = setup
    entered, release = asyncio.Event(), asyncio.Event()

    async def bound(_message):
        entered.set()
        await release.wait()

    monkeypatch.setattr(e.manager, "_get_bound_identity_rejection", bound)
    task = asyncio.create_task(e.manager._handle_chat(e.message))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        e.message.execution_context = replace(e.context, admission=Admission())
        e.admission.allowed = False
        release.set()
        with pytest.raises(ChannelExecutionProtocolError):
            await task
        e.client.threads.get.assert_not_called()
        e.client.runs.wait.assert_not_called()
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_bound_identity_refusal_does_not_redirect_the_saved_publication_to_another_owner(setup, monkeypatch):
    e = setup
    monkeypatch.setattr(e.manager, "_get_bound_identity_rejection", AsyncMock(return_value=SimpleNamespace(message="refused", outbound_connection_id="new-connection", outbound_owner_user_id="new-owner")))
    with pytest.raises(ChannelExecutionProtocolError):
        await e.manager._handle_chat(e.message)
    assert not e.outbounds
    e.client.threads.get.assert_not_called()


@pytest.mark.asyncio
async def test_serialized_wait_refreshes_the_original_admission_before_a_second_create(setup, monkeypatch):
    e = setup
    monkeypatch.setitem(CHANNEL_RUN_POLICY, "slack", ChannelRunPolicy(serialize_thread_runs=True))
    entered, release, queued = asyncio.Event(), asyncio.Event(), asyncio.Event()
    begin = e.manager._begin_serialized_thread_run

    def observe(**kwargs):
        result = begin(**kwargs)
        if result[1]:
            queued.set()
        return result

    async def waiting(*_args, **_kwargs):
        entered.set()
        await release.wait()
        return {"messages": []}

    monkeypatch.setattr(e.manager, "_begin_serialized_thread_run", observe)
    e.client.runs.wait.side_effect = waiting
    second_admission = Admission()
    second = replace(e.message, execution_context=replace(e.context, scope=replace(e.context.scope, publication_id="b" * 32), admission=second_admission))
    first_task = asyncio.create_task(e.manager._handle_chat(e.message))
    second_task = None
    try:
        await asyncio.wait_for(entered.wait(), 2)
        second_task = asyncio.create_task(e.manager._handle_chat(second))
        await asyncio.wait_for(queued.wait(), 2)
        second_admission.allowed = False
        release.set()
        await first_task
        with pytest.raises(ChannelExecutionProtocolError):
            await second_task
        assert e.client.runs.wait.await_count == 1
        assert not e.manager._serialized_thread_runs
    finally:
        release.set()
        await asyncio.gather(first_task, *([second_task] if second_task else []), return_exceptions=True)


@pytest.mark.asyncio
async def test_policy_await_cannot_replace_the_canonical_target_or_escape_late_refusal(setup, monkeypatch):
    e = setup
    entered, release = asyncio.Event(), asyncio.Event()

    async def policy(_msg, context):
        entered.set()
        await release.wait()
        context["agent_name"] = "other"

    monkeypatch.setattr(e.manager, "_apply_channel_policy", policy)
    task = asyncio.create_task(e.manager._handle_chat(e.message))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        release.set()
        with pytest.raises(ChannelExecutionProtocolError):
            await task
        e.client.runs.wait.assert_not_called()
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_literal_current_check_cannot_be_replaced_by_a_truthy_grant(setup, monkeypatch):
    e = setup
    monkeypatch.setattr(e.admission, "current", lambda operation: 1)
    with pytest.raises(ChannelExecutionProtocolError):
        await e.manager._handle_chat(e.message)
    e.client.threads.get.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure,busy", [(ChannelExecutionBusyError(), True), (RuntimeError("409 thread busy private detail"), False)])
async def test_only_the_typed_busy_exception_is_a_busy_manager_outcome(setup, failure, busy):
    e = setup
    e.client.runs.wait.side_effect = failure
    if busy:
        await e.manager._handle_chat(e.message)
        assert "already processing" in e.outbounds[-1].text
    else:
        with pytest.raises(ChannelExecutionProtocolError):
            await e.manager._handle_chat(e.message)
        assert not e.outbounds
    assert e.client.runs.wait.await_count == 1


@pytest.mark.asyncio
async def test_repeated_cancellation_drains_the_owned_stream_close_without_a_final_hint(setup, monkeypatch):
    e = setup
    entered, closing, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    close_calls = 0

    class Stream(OwnedStream):
        async def __anext__(self):
            entered.set()
            await asyncio.Event().wait()

        async def aclose(self):
            nonlocal close_calls
            close_calls += 1
            closing.set()
            await release.wait()
            self.closed = True

    stream = Stream()
    e.client.runs.stream.return_value = stream
    monkeypatch.setattr(e.manager, "_channel_supports_streaming", lambda _channel: True)
    task = asyncio.create_task(e.manager._handle_chat(e.message))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        task.cancel()
        await asyncio.wait_for(closing.wait(), 2)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done() and not e.outbounds
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert stream.closed and close_calls == 1 and not e.outbounds
    finally:
        release.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_repeated_cancel_keeps_the_started_admission_owned_before_any_http(setup, monkeypatch):
    e = setup
    entered, release, finished = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def authorize(_operation):
        entered.set()
        await release.wait()
        finished.set()

    monkeypatch.setattr(e.admission, "authorize", authorize)
    task = asyncio.create_task(e.manager._handle_chat(e.message))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done() and not finished.is_set()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert finished.is_set()
        e.client.threads.get.assert_not_called()
        assert not e.outbounds
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_false_none_keeps_the_ordinary_client_and_ignores_metadata_capability_spoofs(setup, monkeypatch):
    e = setup
    e.message.execution_required = False
    e.message.execution_context = None
    e.message.metadata = {"execution_required": True, "execution_context": {"scope": {}}, "channel_turn_id": "b" * 32}
    monkeypatch.setattr(e.manager, "_get_client", Mock(return_value=e.client))
    monkeypatch.setattr(e.manager, "_get_or_create_thread", AsyncMock(return_value=("ordinary-thread", True)))
    monkeypatch.setattr("app.channels.manager._channel_storage_user_id", lambda _message: "owner")
    monkeypatch.setattr("app.channels.manager._owner_headers", lambda _message: {"X-Legacy": "retained"})
    await e.manager._handle_chat(e.message)
    assert e.client.runs.wait.call_args.args[0] == "ordinary-thread"
    assert "channel_execution" not in e.client.runs.wait.call_args.kwargs
    assert e.client.runs.wait.call_args.kwargs["headers"] == {"X-Legacy": "retained"}


@pytest.mark.asyncio
@pytest.mark.parametrize("streaming", [False, True])
@pytest.mark.parametrize("agent", [None, "custom-agent"])
async def test_actual_facade_and_one_http_owner_receive_the_managers_canonical_creating_body(setup, monkeypatch, streaming, agent):
    from app.channels.execution_client import ChannelExecutionFacade
    from app.telegram_bots.gateway_client import GatewayClientOwner

    e = setup
    calls = []

    def respond(request):
        calls.append(request)
        if request.method == "GET":
            return httpx.Response(200, json={"thread_id": "original-thread", "owner_user_id": "owner", "agent_instance": None, "metadata": {"channel_agent_name": agent or "lead_agent", **({"agent_name": agent} if agent else {})}})
        if streaming:
            return httpx.Response(200, headers={"content-type": "text/event-stream"}, content=b'event: values\ndata: {"messages":[{"type":"ai","content":"answer"}]}\n\n')
        return httpx.Response(200, json={"messages": [{"type": "ai", "content": "answer"}]})

    owner = GatewayClientOwner(gateway_url="https://gateway.example", langgraph_url="https://runtime.example/prefix/api", transport=httpx.MockTransport(respond))
    scope = replace(e.context.scope, endpoint_id=owner.endpoint_id, agent_name=agent)
    client = ChannelExecutionFacade(owner, scope=scope, admission=e.admission)
    e.message.execution_context = ChannelExecutionContext(scope, client, e.admission)
    e.message.metadata["human_message_kwargs"] = {"forwarded": [{"from": "sender"}], "original_user_content": "retained original"}
    monkeypatch.setattr(e.manager, "_channel_supports_streaming", lambda _channel: streaming)
    try:
        await e.manager._handle_chat(e.message)
        assert [call.method for call in calls] == ["GET", "POST"]
        request = calls[-1]
        assert str(request.url).startswith("https://runtime.example/prefix/api/threads/original-thread/runs/")
        body = json.loads(request.content)
        assert body["channel_execution"] == {"version": 1, "publication_id": scope.publication_id}
        assert body["metadata"] == {"channel_turn_id": scope.publication_id}
        assert body["assistant_id"] == scope.assistant_id
        assert body["context"].get("agent_name") == agent and body["context"]["user_id"] == "owner"
        assert body["config"]["configurable"].get("agent_name") == agent
        (human,) = body["input"]["messages"]
        assert human["additional_kwargs"]["channel_turn_id"] == scope.publication_id
        assert human["additional_kwargs"]["forwarded"] == [{"from": "sender"}]
        assert human["additional_kwargs"]["original_user_content"] == "retained original"
        assert request.headers["Idempotency-Key"] == scope.publication_id
        assert "execution_context" not in request.content.decode() and "execution_required" not in request.content.decode()
        assert e.outbounds[-1].text == "answer"
    finally:
        await owner.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("streaming", [False, True])
async def test_actual_manager_drain_retains_a_dispatched_result_after_new_run_admission_closes(setup, monkeypatch, streaming):
    from app.channels.execution_client import ChannelExecutionFacade
    from app.telegram_bots.gateway_client import GatewayClientOwner

    e = setup
    calls = []
    dispatched, release, refused = asyncio.Event(), asyncio.Event(), asyncio.Event()

    class RetainedAdmission:
        creating = True

        async def authorize(self, operation):
            if not self.current(operation):
                raise ChannelExecutionProtocolError()

        def current(self, operation):
            return operation != "run_create" or self.creating

    class ResponseStream(httpx.AsyncByteStream):
        closed = False

        async def __aiter__(self):
            await release.wait()
            yield b'event: values\ndata: {"messages":[{"type":"ai","content":"retained answer"}]}\n\n'

        async def aclose(self):
            self.closed = True

    response_stream = ResponseStream()

    async def respond(request):
        calls.append(request)
        if request.method == "GET":
            return httpx.Response(200, json={"thread_id": "original-thread", "owner_user_id": "owner", "agent_instance": None, "metadata": {"channel_agent_name": "lead_agent"}})
        dispatched.set()
        if streaming:
            return httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=response_stream)
        await release.wait()
        return httpx.Response(200, json={"messages": [{"type": "ai", "content": "retained answer"}]})

    async def observe(message):
        if message.text == "Channel execution protocol refused":
            refused.set()

    owner = GatewayClientOwner(gateway_url="https://gateway.example", langgraph_url="https://runtime.example/prefix/api", transport=httpx.MockTransport(respond))
    admission = RetainedAdmission()
    scope = replace(e.context.scope, endpoint_id=owner.endpoint_id)
    e.message.execution_context = ChannelExecutionContext(scope, ChannelExecutionFacade(owner, scope=scope, admission=admission), admission)
    next_scope = replace(scope, publication_id="b" * 32)
    next_message = replace(e.message, execution_context=ChannelExecutionContext(next_scope, ChannelExecutionFacade(owner, scope=next_scope, admission=admission), admission))
    monkeypatch.setattr(e.manager, "_channel_supports_streaming", lambda _channel: streaming)
    e.manager.bus.subscribe_outbound(observe)
    stop = None
    await e.manager.start()
    try:
        await e.manager.bus.publish_inbound(e.message)
        await asyncio.wait_for(dispatched.wait(), 2)
        owner.close_run_admission()
        admission.creating = False
        await e.manager.bus.publish_inbound(next_message)
        await asyncio.wait_for(refused.wait(), 2)
        stop = asyncio.create_task(e.manager.stop())
        await asyncio.sleep(0)
        assert not stop.done() and not owner.closed
        release.set()
        await asyncio.wait_for(stop, 2)
        assert [call.method for call in calls] == ["GET", "POST", "GET"]
        assert [message.text for message in e.outbounds] == ["Channel execution protocol refused", "retained answer"]
        assert not owner.closed and (response_stream.closed if streaming else True)
        assert not e.manager._worker_tasks
    finally:
        release.set()
        if stop is not None:
            await asyncio.gather(stop, return_exceptions=True)
        await e.manager.stop()
        await owner.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("streaming", [False, True])
async def test_actual_worker_transport_ambiguity_never_requeues_or_retries_a_model_post(setup, monkeypatch, streaming):
    from app.channels.execution_client import ChannelExecutionFacade
    from app.telegram_bots.gateway_client import GatewayClientOwner

    e = setup
    calls = []

    def respond(request):
        calls.append(request)
        if request.method == "GET":
            return httpx.Response(200, json={"thread_id": "original-thread", "owner_user_id": "owner", "agent_instance": None, "metadata": {"channel_agent_name": "lead_agent"}})
        raise httpx.ReadError("synthetic transport ambiguity", request=request)

    owner = GatewayClientOwner(gateway_url="https://gateway.example", langgraph_url="https://runtime.example/prefix/api", transport=httpx.MockTransport(respond))
    scope = replace(e.context.scope, endpoint_id=owner.endpoint_id)
    e.message.execution_context = ChannelExecutionContext(scope, ChannelExecutionFacade(owner, scope=scope, admission=e.admission), e.admission)
    e.message.workspace_id = "workspace"
    e.message.metadata["event_id"] = "event"
    monkeypatch.setattr(e.manager, "_channel_supports_streaming", lambda _channel: streaming)
    await e.manager.start()
    try:
        await e.manager.bus.publish_inbound(e.message)
        await asyncio.wait_for(e.manager.bus.join_inbound(), 2)
        assert [request.method for request in calls] == ["GET", "POST"]
        assert [message.text for message in e.outbounds] == ["Channel execution protocol refused"]
        assert e.manager.bus.inbound_queue.empty()
        e.ordinary.assert_not_called()
    finally:
        await e.manager.stop()
        await owner.stop()
