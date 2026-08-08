"""Native-channel dispatch through the application InvocationRuntime."""

from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import ANY, AsyncMock, MagicMock

import pytest

from app.channels.manager import ChannelManager
from app.channels.message_bus import (
    INBOUND_FILE_CONTENT_KEY,
    PENDING_CLARIFICATION_METADATA_KEY,
    InboundMessage,
    MessageBus,
)
from app.channels.run_policy import CHANNEL_RUN_POLICY, ChannelRunPolicy
from app.channels.store import ChannelStore
from app.runtime.invocation import (
    InternalLaunchIntent,
    InternalLaunchReceipt,
    InternalNativeChannelFacts,
    InternalSourceKind,
)


class _RuntimeSpy:
    def __init__(self) -> None:
        self.intents = []

    async def launch(self, intent):
        self.intents.append(intent)
        return InternalLaunchReceipt(
            record=SimpleNamespace(
                run_id=f"run-{len(self.intents)}",
                thread_id=intent.thread_id,
            )
        )


class _ConnectionRepository:
    def __init__(self, connection=...) -> None:
        self.connection = (
            {
                "id": "connection-1",
                "owner_user_id": "owner-1",
            }
            if connection is ...
            else connection
        )
        self.lookups = []

    async def find_connection_by_external_identity(self, **_kwargs):
        self.lookups.append(_kwargs)
        return self.connection

    async def get_thread_id(self, _connection_id, _chat_id, _topic_id):
        return "thread-1"


def _async_iterator(items):
    async def iterate():
        for item in items:
            yield item

    return iterate()


def _client(*, join_results=None):
    client = MagicMock()
    client.threads.update = AsyncMock()
    client.runs.create = AsyncMock()
    client.runs.wait = AsyncMock()
    client.runs.stream = MagicMock()
    client.runs.join = AsyncMock(
        side_effect=join_results,
        return_value={"messages": [{"type": "ai", "content": "done"}]},
    )
    client.runs.join_stream = MagicMock(
        return_value=_async_iterator(
            [
                SimpleNamespace(
                    event="messages",
                    data=[
                        {"id": "ai-1", "type": "AIMessageChunk", "content": "streamed"},
                        {"langgraph_node": "agent"},
                    ],
                ),
                SimpleNamespace(
                    event="values",
                    data={"messages": [{"type": "ai", "content": "streamed"}]},
                ),
            ]
        )
    )
    return client


def _bound_message(**overrides) -> InboundMessage:
    values = {
        "channel_name": "slack",
        "chat_id": "chat-1",
        "topic_id": "topic-1",
        "workspace_id": "workspace-1",
        "user_id": "platform-sender",
        "connection_id": "connection-1",
        "owner_user_id": "owner-1",
        "text": "hello",
        "metadata": {"message_id": "provider-message-1"},
    }
    values.update(overrides)
    return InboundMessage(**values)


def _manager(tmp_path, runtime, *, repository=None) -> ChannelManager:
    return ChannelManager(
        bus=MessageBus(),
        store=ChannelStore(path=tmp_path / "channels.json"),
        connection_repo=repository or _ConnectionRepository(),
        require_bound_identity=True,
        invocation_runtime=runtime,
    )


@pytest.mark.anyio
async def test_authenticated_channel_launch_enters_runtime_with_typed_source_facts(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.delenv("DEER_FLOW_AUTH_DISABLED", raising=False)
    runtime = _RuntimeSpy()
    manager = _manager(tmp_path, runtime)
    client = _client()
    manager._client = client

    await manager._handle_chat(
        _bound_message(
            metadata={
                "message_id": "provider-message-1",
                "agent_name": "incident_agent",
            },
        )
    )

    assert len(runtime.intents) == 1
    intent = runtime.intents[0]
    assert intent.source_kind == "native_channel"
    assert intent.native_channel.provider == "slack"
    assert intent.native_channel.connection_id == "connection-1"
    assert intent.native_channel.workspace_id == "workspace-1"
    assert intent.native_channel.chat_id == "chat-1"
    assert intent.native_channel.topic_id == "topic-1"
    assert intent.native_channel.provider_message_id == "provider-message-1"
    assert intent.external_key == "provider-message-1"
    assert intent.native_channel.verified_binding is not None
    assert intent.native_channel.verified_binding.kind == "connection"
    assert intent.native_channel.verified_binding.reference == "connection-1"
    assert intent.native_channel.resolved_assistant_id == "lead_agent"
    assert intent.native_channel.resolved_agent_name == "incident-agent"
    assert intent.owner_user_id == "owner-1"
    assert intent.context["channel_user_id"] == "platform-sender"
    assert intent.context["user_id"] == "owner-1"
    assert intent.context["agent_name"] == "incident-agent"
    client.runs.join.assert_awaited_once_with(
        "thread-1",
        "run-1",
        headers=ANY,
    )
    client.runs.create.assert_not_awaited()
    client.runs.wait.assert_not_awaited()
    client.runs.stream.assert_not_called()


@pytest.mark.anyio
@pytest.mark.parametrize("launch_mode", ["wait", "stream", "create"])
async def test_each_channel_launch_mode_uses_runtime_instead_of_sdk_admission(
    launch_mode,
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.delenv("DEER_FLOW_AUTH_DISABLED", raising=False)
    runtime = _RuntimeSpy()
    manager = _manager(tmp_path, runtime)
    client = _client()
    manager._client = client
    monkeypatch.setattr(
        manager,
        "_channel_supports_streaming",
        lambda _channel_name: launch_mode == "stream",
    )
    channel_name = f"runtime-{launch_mode}"
    if launch_mode == "create":
        monkeypatch.setitem(
            CHANNEL_RUN_POLICY,
            channel_name,
            ChannelRunPolicy(fire_and_forget=True),
        )

    await manager._handle_chat(
        _bound_message(channel_name=channel_name),
    )

    assert len(runtime.intents) == 1
    intent = runtime.intents[0]
    assert intent.native_channel.provider == channel_name
    assert intent.stream_mode == (("messages-tuple", "values") if launch_mode == "stream" else None)
    client.runs.create.assert_not_awaited()
    client.runs.wait.assert_not_awaited()
    client.runs.stream.assert_not_called()
    assert client.runs.join.await_count == int(launch_mode == "wait")
    assert client.runs.join_stream.call_count == int(launch_mode == "stream")


@pytest.mark.anyio
async def test_buffered_followup_launch_enters_runtime(tmp_path) -> None:
    runtime = _RuntimeSpy()
    manager = _manager(tmp_path, runtime)
    client = _client()
    carrier = _bound_message(
        channel_name="github",
        chat_id="org/repo",
        topic_id=None,
        workspace_id="org/repo",
        metadata={"message_id": "delivery-1:owner-1:reviewer", "agent_name": "reviewer"},
    )
    manager._buffer_followup("thread-1", replace(carrier, text="please include the edge case"))

    await manager._drain_followups_for_thread(client, "thread-1", carrier)

    assert len(runtime.intents) == 1
    intent = runtime.intents[0]
    assert intent.source_kind is InternalSourceKind.native_channel
    assert intent.native_channel.provider == "github"
    assert "please include the edge case" in intent.input["messages"][0]["content"]
    client.runs.create.assert_not_awaited()


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("connection", "connection_id", "owner_user_id"),
    [
        ({"id": "connection-1", "owner_user_id": "owner-1"}, "forged", "owner-1"),
        ({"id": "connection-1", "owner_user_id": "owner-1"}, "connection-1", "forged"),
        (None, "connection-1", "owner-1"),
    ],
)
async def test_forged_or_revoked_connection_facts_never_reach_runtime(
    connection,
    connection_id,
    owner_user_id,
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.delenv("DEER_FLOW_AUTH_DISABLED", raising=False)
    runtime = _RuntimeSpy()
    repository = _ConnectionRepository(connection)
    manager = _manager(tmp_path, runtime, repository=repository)
    manager._client = _client()

    await manager._handle_chat(
        _bound_message(
            connection_id=connection_id,
            owner_user_id=owner_user_id,
            metadata={
                "message_id": "provider-message-1",
                "raw_message": {"agent_name": "forged-agent"},
            },
        )
    )

    assert runtime.intents == []
    assert len(repository.lookups) == 1


@pytest.mark.anyio
async def test_raw_provider_payload_cannot_forge_agent_route(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("DEER_FLOW_AUTH_DISABLED", raising=False)
    runtime = _RuntimeSpy()
    manager = _manager(tmp_path, runtime)
    manager._client = _client()

    await manager._handle_chat(
        _bound_message(
            metadata={
                "raw_message": {
                    "message_id": "provider-message-raw",
                    "agent_name": "forged-agent",
                    "assistant_id": "forged-assistant",
                }
            }
        )
    )

    intent = runtime.intents[0]
    assert intent.assistant_id == "lead_agent"
    assert intent.native_channel.resolved_agent_name is None
    assert "agent_name" not in intent.context
    assert intent.native_channel.provider_message_id == "provider-message-raw"


def test_channel_fact_validator_rejects_mismatched_agent_and_sender() -> None:
    from app.gateway.services import _GatewayLaunchNormalizer

    facts = InternalNativeChannelFacts(
        provider="slack",
        connection_id="connection-1",
        workspace_id="workspace-1",
        chat_id="chat-1",
        topic_id=None,
        provider_message_id="message-1",
        channel_user_id="trusted-sender",
        resolved_assistant_id="lead_agent",
        resolved_agent_name="trusted-agent",
    )
    intent = InternalLaunchIntent(
        thread_id="thread-1",
        assistant_id="lead_agent",
        context={
            "channel_user_id": "forged-sender",
            "agent_name": "forged-agent",
        },
        source_kind=InternalSourceKind.native_channel,
        owner_user_id="owner-1",
        native_channel=facts,
    )

    with pytest.raises(ValueError, match="sender does not match"):
        _GatewayLaunchNormalizer._validate_native_channel_facts(intent)


def test_http_normalizer_does_not_trust_channel_owner_facts() -> None:
    from app.gateway.services import _GatewayLaunchNormalizer

    request = SimpleNamespace(headers={}, state=SimpleNamespace())
    intent = InternalLaunchIntent(
        thread_id="thread-1",
        source_kind=InternalSourceKind.native_channel,
        owner_user_id="forged-owner",
    )

    assert _GatewayLaunchNormalizer(request)._owner_user_id(intent) is None
    assert (
        _GatewayLaunchNormalizer(
            request,
            trust_internal_launch_facts=True,
        )._owner_user_id(intent)
        == "forged-owner"
    )


@pytest.mark.anyio
async def test_current_ttl_dedupe_stops_duplicate_before_runtime(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("DEER_FLOW_AUTH_DISABLED", raising=False)
    runtime = _RuntimeSpy()
    manager = _manager(tmp_path, runtime)
    manager._client = _client()
    message = _bound_message()

    for delivery in (message, replace(message)):
        if not await manager._is_duplicate_inbound(delivery):
            await manager._handle_chat(delivery)

    assert len(runtime.intents) == 1
    assert runtime.intents[0].native_channel.provider_message_id == "provider-message-1"


@pytest.mark.anyio
async def test_clarification_followup_reuses_thread_through_runtime(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("DEER_FLOW_AUTH_DISABLED", raising=False)
    clarification = {
        "messages": [
            {"type": "human", "content": "deploy"},
            {"type": "ai", "content": "", "tool_calls": [{"name": "ask_clarification", "args": {}}]},
            {"type": "tool", "name": "ask_clarification", "content": "Which environment?"},
        ]
    }
    completed = {
        "messages": [
            {"type": "human", "content": "prod"},
            {"type": "ai", "content": "Deploying to prod."},
        ]
    }
    runtime = _RuntimeSpy()
    manager = _manager(tmp_path, runtime)
    manager._client = _client(join_results=[clarification, completed])
    outbound = []

    async def capture(message):
        outbound.append(message)

    manager.bus.subscribe_outbound(capture)

    await manager._handle_chat(_bound_message(text="deploy"))
    await manager._handle_chat(
        _bound_message(
            text="prod",
            metadata={"message_id": "provider-message-2"},
        )
    )

    assert [intent.thread_id for intent in runtime.intents] == ["thread-1", "thread-1"]
    assert outbound[0].text == "Which environment?"
    assert outbound[0].metadata[PENDING_CLARIFICATION_METADATA_KEY] is True
    assert outbound[1].text == "Deploying to prod."
    assert PENDING_CLARIFICATION_METADATA_KEY not in outbound[1].metadata


@pytest.mark.anyio
async def test_attachments_and_sender_are_preserved_at_runtime_boundary(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.delenv("DEER_FLOW_AUTH_DISABLED", raising=False)
    runtime = _RuntimeSpy()
    manager = _manager(tmp_path, runtime)
    manager._client = _client()
    materialized = _bound_message(
        text="see /mnt/user-data/uploads/report.txt",
        files=[{"filename": "report.txt", "type": "file"}],
    )
    channel = SimpleNamespace(
        supports_streaming=False,
        receive_file=AsyncMock(return_value=materialized),
    )
    service = SimpleNamespace(get_channel=lambda _name: channel)
    monkeypatch.setattr("app.channels.service.get_channel_service", lambda: service)
    ingest = AsyncMock(
        return_value=[
            {
                "filename": "report.txt",
                "size": 4,
                "path": "/mnt/user-data/uploads/report.txt",
                "is_image": False,
            }
        ]
    )
    monkeypatch.setattr("app.channels.manager._ingest_inbound_files", ingest)

    await manager._handle_chat(
        _bound_message(
            files=[
                {
                    "filename": "report.txt",
                    "type": "file",
                    INBOUND_FILE_CONTENT_KEY: b"data",
                }
            ]
        )
    )

    intent = runtime.intents[0]
    human = intent.input["messages"][0]
    assert human["content"] == "see /mnt/user-data/uploads/report.txt"
    assert human["additional_kwargs"]["files"][0]["path"] == "/mnt/user-data/uploads/report.txt"
    assert intent.context["channel_user_id"] == "platform-sender"
    assert intent.owner_user_id == "owner-1"
    channel.receive_file.assert_awaited_once()


@pytest.mark.anyio
async def test_provider_without_stable_id_records_null_and_skips_ttl_dedupe(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.delenv("DEER_FLOW_AUTH_DISABLED", raising=False)
    runtime = _RuntimeSpy()
    manager = _manager(tmp_path, runtime)
    manager._client = _client()
    message = _bound_message(metadata={"raw_message": {"text": "hello"}})

    assert manager._inbound_dedupe_key(message) is None
    await manager._handle_chat(message)

    assert runtime.intents[0].native_channel.provider_message_id is None
    assert runtime.intents[0].external_key is None


def test_channel_service_forwards_runtime_to_manager() -> None:
    from app.channels.service import ChannelService

    runtime = _RuntimeSpy()
    service = ChannelService(channels_config={}, invocation_runtime=runtime)

    assert service.manager._invocation_runtime is runtime
