"""Verified native bindings for interactive connections and signed webhooks."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
import sqlalchemy as sa
import yaml
from fastapi import FastAPI
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.store.memory import InMemoryStore
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.channels.connection_identity import attach_connection_identity
from app.channels.inbound_receipts import InboundReceiptProcessor, SqlInboundReceiptStore
from app.channels.manager import ChannelManager
from app.channels.message_bus import InboundMessage, MessageBus
from app.channels.store import ChannelStore
from app.gateway.auth_disabled import AUTH_SOURCE_INTERNAL
from app.gateway.github.dispatcher import VerifiedGitHubWebhookRequest, fanout_event
from app.gateway.github.registry import _invalidate_cache
from app.gateway.routers import github_webhooks
from app.gateway.services import _GatewayLaunchNormalizer, build_channel_invocation_runtime
from app.runtime import (
    InternalLaunchIntent,
    InternalLaunchReceipt,
    InternalNativeChannelFacts,
    InternalSourceKind,
    InternalVerifiedNativeBinding,
    InternalVerifiedNativeBindingKind,
)
from app.runtime.idempotency import scope_for_channel
from deerflow.config.agents_config import GitHubAgentConfig
from deerflow.config.app_config import AppConfig, reset_app_config, set_app_config
from deerflow.persistence.base import Base
from deerflow.persistence.inbound_receipt.model import InboundReceiptRow
from deerflow.persistence.thread_meta.memory import MemoryThreadMetaStore
from deerflow.runtime.accepted_invocation import (
    InvocationOrigin,
    ResolvedAgentMaterialV1,
    ResolvedAgentRevision,
    canonical_digest,
)
from deerflow.runtime.events.store.memory import MemoryRunEventStore
from deerflow.runtime.runs.manager import RunManager
from deerflow.runtime.runs.schemas import RunStatus
from deerflow.runtime.runs.store.base import LifecycleType
from deerflow.runtime.runs.store.memory import MemoryRunStore
from deerflow.runtime.tenant_identity import TenantIdentityV1, tenant_admission_scope

_SECRET = "verified-binding-test-secret"
_TEST_TENANT_IDENTITY = TenantIdentityV1.from_canonical_id("local")
_TEST_TENANT = _TEST_TENANT_IDENTITY.to_persisted_reference()


def _verified_request(
    delivery_id: str,
    *,
    event: str = "pull_request",
    body: bytes = b"verified-test-event",
) -> VerifiedGitHubWebhookRequest:
    return VerifiedGitHubWebhookRequest.attest(
        delivery_id,
        event=event,
        body=body,
    )


class _ReadyAdmissionFence:
    async def ready_for_admission(self) -> bool:
        return True


class _RecordingRuntime:
    def __init__(self, runtime) -> None:
        self.runtime = runtime
        self.receipts = []
        self.intents = []

    async def launch(self, intent):
        self.intents.append(intent)
        receipt = await self.runtime.launch(intent)
        self.receipts.append(receipt)
        return receipt


class _LoseCreatedResponseRuntime(_RecordingRuntime):
    """Commit one selected launch, then model loss of its first response."""

    def __init__(self, runtime, *, external_key: str) -> None:
        super().__init__(runtime)
        self.external_key = external_key
        self.response_lost = False

    async def launch(self, intent):
        self.intents.append(intent)
        receipt = await self.runtime.launch(intent)
        self.receipts.append(receipt)
        if intent.external_key == self.external_key and receipt.created and not self.response_lost:
            self.response_lost = True
            raise RuntimeError("simulated response loss after committed admission")
        return receipt


def _write_github_agent(
    base: Path,
    *,
    owner: str = "owner-1",
    name: str = "reviewer",
    installation_id: int | None = 1234,
    recursion_limit: int = 100,
) -> None:
    agent_dir = base / "users" / owner / "agents" / name
    agent_dir.mkdir(parents=True, exist_ok=True)
    (agent_dir / "config.yaml").write_text(
        yaml.safe_dump(
            {
                "name": name,
                "github": {
                    "installation_id": installation_id,
                    "recursion_limit": recursion_limit,
                    "bindings": [
                        {
                            "repo": "acme/widgets",
                            "triggers": {
                                "pull_request": {"actions": ["opened"]},
                            },
                        }
                    ],
                },
            }
        ),
        encoding="utf-8",
    )


def _github_payload() -> dict[str, object]:
    return {
        "action": "opened",
        "number": 7,
        "pull_request": {
            "number": 7,
            "title": "Verify source binding",
            "body": "Please review.",
            "user": {"login": "octocat"},
        },
        "repository": {"full_name": "acme/widgets"},
        "installation": {"id": 1234},
        "sender": {"login": "octocat"},
        "owner_user_id": "attacker",
        # Payload attempts to mint authority are ignored by the dispatcher.
        "verified_source_binding": {
            "kind": "connection",
            "reference": "forged",
        },
    }


def _signature(body: bytes) -> str:
    return "sha256=" + hmac.new(_SECRET.encode(), body, hashlib.sha256).hexdigest()


@pytest.mark.anyio
async def test_real_normalizer_accepts_keyed_verified_webhook_route(monkeypatch) -> None:
    async def owner_user(*_args, **_kwargs):
        return SimpleNamespace(
            id="owner-1",
            system_role="user",
            oauth_provider=None,
            oauth_id=None,
        )

    monkeypatch.setattr(
        "app.gateway.services.resolve_trusted_internal_owner_for_attribution",
        owner_user,
    )
    normalizer = _GatewayLaunchNormalizer(
        SimpleNamespace(
            state=SimpleNamespace(
                user=SimpleNamespace(id="internal", system_role="internal"),
                auth_source=AUTH_SOURCE_INTERNAL,
            ),
            app=SimpleNamespace(
                state=SimpleNamespace(tenant_identity=_TEST_TENANT_IDENTITY),
            ),
        ),
        trust_internal_launch_facts=True,
    )
    binding = InternalVerifiedNativeBinding(
        kind=InternalVerifiedNativeBindingKind.webhook_route,
        reference="route:v1:sha256:" + "a" * 64,
    )
    identity = await normalizer.identify(
        InternalLaunchIntent(
            thread_id="thread-github",
            assistant_id="lead_agent",
            context={
                "channel_user_id": "octocat",
                "channel_name": "github",
                "agent_name": "reviewer",
            },
            source_kind=InternalSourceKind.native_channel,
            owner_user_id="owner-1",
            external_key="delivery-1:owner-1:reviewer",
            native_channel=InternalNativeChannelFacts(
                provider="github",
                connection_id=None,
                workspace_id="acme/widgets",
                chat_id="acme/widgets",
                topic_id="7:reviewer",
                provider_message_id="delivery-1:owner-1:reviewer",
                channel_user_id="octocat",
                resolved_assistant_id="lead_agent",
                resolved_agent_name="reviewer",
                verified_binding=binding,
            ),
        )
    )

    assert identity is not None
    assert identity.external_scope == tenant_admission_scope(
        _TEST_TENANT_IDENTITY.to_persisted_reference(),
        scope_for_channel(
            "github",
            binding.reference,
            "acme/widgets",
            "acme/widgets",
            binding_kind=InternalVerifiedNativeBindingKind.webhook_route,
        ),
    )


def test_connection_scope_remains_backward_compatible() -> None:
    assert (
        scope_for_channel(
            "slack",
            "connection-1",
            "workspace-1",
            "chat-1",
        )
        == "channel:v1:sha256:431a0b57a0122016df5bbd8f03608466ca1665b5c498613e1411210eca334f12"
    )


@pytest.mark.anyio
async def test_verified_dispatch_requires_matching_trusted_installation(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("DEER_FLOW_HOME", str(tmp_path))
    from deerflow.config import paths as paths_module

    monkeypatch.setattr(paths_module, "_paths", None)
    _invalidate_cache()
    _write_github_agent(tmp_path)
    payload = _github_payload()

    mismatched_bus = MessageBus()
    mismatched = dict(payload)
    mismatched["installation"] = {"id": 9999}
    result = await fanout_event(
        mismatched_bus,
        "pull_request",
        "delivery-1",
        mismatched,
        verified_request=_verified_request("delivery-1"),
    )
    assert result["fired_agents"] == []
    assert result["skipped"] == [{"agent": "reviewer", "reason": "installation_mismatch"}]
    assert mismatched_bus.inbound_queue.empty()

    bus = MessageBus()
    result = await fanout_event(
        bus,
        "pull_request",
        "delivery-1",
        payload,
        verified_request=_verified_request("delivery-1"),
    )
    assert result["fired_agents"] == ["reviewer"]
    message = await bus.get_inbound()
    assert message.connection_id is None
    assert message.owner_user_id == "owner-1"
    assert message.verified_source_binding is not None
    assert message.verified_source_binding.kind == "webhook_route"
    assert message.verified_source_binding.reference != "forged"

    malformed_bus = MessageBus()
    malformed = dict(payload)
    malformed["installation"] = {"id": True}
    result = await fanout_event(
        malformed_bus,
        "pull_request",
        "delivery-2",
        malformed,
        verified_request=_verified_request("delivery-2"),
    )
    assert result["fired_agents"] == []
    assert result["skipped"] == [{"agent": "reviewer", "reason": "installation_mismatch"}]
    assert malformed_bus.inbound_queue.empty()

    _write_github_agent(tmp_path, installation_id=None)
    _invalidate_cache()
    unconfigured_bus = MessageBus()
    result = await fanout_event(
        unconfigured_bus,
        "pull_request",
        "delivery-3",
        payload,
        verified_request=_verified_request("delivery-3"),
    )
    assert result["fired_agents"] == []
    assert result["skipped"] == [{"agent": "reviewer", "reason": "installation_unconfigured"}]
    assert unconfigured_bus.inbound_queue.empty()


@pytest.mark.anyio
async def test_verified_dispatch_scopes_conversation_to_each_owner_binding(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("DEER_FLOW_HOME", str(tmp_path))
    from deerflow.config import paths as paths_module

    monkeypatch.setattr(paths_module, "_paths", None)
    _invalidate_cache()
    _write_github_agent(tmp_path, owner="alice")
    _write_github_agent(tmp_path, owner="bob")
    bus = MessageBus()

    result = await fanout_event(
        bus,
        "pull_request",
        "delivery-shared",
        _github_payload(),
        verified_request=_verified_request("delivery-shared"),
    )

    assert result["fired_agents"] == ["reviewer", "reviewer"]
    messages = [await bus.get_inbound(), await bus.get_inbound()]
    by_owner = {message.owner_user_id: message for message in messages}
    assert set(by_owner) == {"alice", "bob"}
    assert by_owner["alice"].topic_id != by_owner["bob"].topic_id
    assert by_owner["alice"].metadata["preferred_thread_id"] != by_owner["bob"].metadata["preferred_thread_id"]
    assert by_owner["alice"].verified_source_binding != by_owner["bob"].verified_source_binding


@pytest.mark.anyio
async def test_verified_dispatch_rejects_ambiguous_registry_match(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("DEER_FLOW_HOME", str(tmp_path))
    from app.gateway.github import dispatcher
    from app.gateway.github.registry import build_github_agent_registry, lookup_agents
    from deerflow.config import paths as paths_module

    monkeypatch.setattr(paths_module, "_paths", None)
    _invalidate_cache()
    _write_github_agent(tmp_path)
    registry = build_github_agent_registry()
    match = lookup_agents(registry, "acme/widgets", "pull_request")[0]
    monkeypatch.setattr(dispatcher, "build_github_agent_registry", lambda: registry)
    monkeypatch.setattr(
        dispatcher,
        "lookup_agents",
        lambda _registry, _repo, event: [match, match] if event == "pull_request" else [],
    )
    bus = MessageBus()

    result = await fanout_event(
        bus,
        "pull_request",
        "delivery-1",
        _github_payload(),
        verified_request=_verified_request("delivery-1"),
    )

    assert result["fired_agents"] == []
    assert all(item["reason"] == "ambiguous_route_binding" for item in result["skipped"])
    assert bus.inbound_queue.empty()


def test_route_binding_coordinates_have_independent_collision_domains() -> None:
    from app.runtime import build_verified_webhook_route_binding

    base = dict(
        provider="github",
        installation_reference=1234,
        owner_user_id="owner-1",
        agent_id="reviewer",
        repository_reference="acme/widgets",
    )
    bindings = (
        build_verified_webhook_route_binding(**base),
        build_verified_webhook_route_binding(**{**base, "owner_user_id": "owner-2"}),
        build_verified_webhook_route_binding(**{**base, "agent_id": "coder"}),
        build_verified_webhook_route_binding(**{**base, "repository_reference": "acme/other"}),
    )
    references = {binding.reference for binding in bindings}
    assert len(references) == 4
    scopes = {
        scope_for_channel(
            "github",
            binding.reference,
            "acme/widgets",
            "acme/widgets",
            binding_kind=binding.kind,
        )
        for binding in bindings
    }
    assert len(scopes) == 4


@pytest.mark.anyio
async def test_real_normalizer_rejects_missing_or_conflicting_binding(monkeypatch) -> None:
    async def owner_user(*_args, **_kwargs):
        return SimpleNamespace(
            id="owner-1",
            system_role="user",
            oauth_provider=None,
            oauth_id=None,
        )

    monkeypatch.setattr(
        "app.gateway.services.resolve_trusted_internal_owner_for_attribution",
        owner_user,
    )
    normalizer = _GatewayLaunchNormalizer(
        SimpleNamespace(
            state=SimpleNamespace(
                user=SimpleNamespace(id="internal", system_role="internal"),
                auth_source=AUTH_SOURCE_INTERNAL,
            ),
            app=SimpleNamespace(
                state=SimpleNamespace(tenant_identity=_TEST_TENANT_IDENTITY),
            ),
        ),
        trust_internal_launch_facts=True,
    )

    def intent(binding, *, connection_id=None):
        return InternalLaunchIntent(
            thread_id="thread-github",
            assistant_id="lead_agent",
            context={
                "channel_user_id": "octocat",
                "channel_name": "github",
                "agent_name": "reviewer",
            },
            source_kind=InternalSourceKind.native_channel,
            owner_user_id="owner-1",
            external_key="delivery-1",
            native_channel=InternalNativeChannelFacts(
                provider="github",
                connection_id=connection_id,
                workspace_id="acme/widgets",
                chat_id="acme/widgets",
                topic_id="7:reviewer",
                provider_message_id="delivery-1",
                channel_user_id="octocat",
                resolved_assistant_id="lead_agent",
                resolved_agent_name="reviewer",
                verified_binding=binding,
            ),
        )

    with pytest.raises(ValueError, match="verified source binding"):
        await normalizer.identify(intent(None))

    route_binding = InternalVerifiedNativeBinding(
        kind=InternalVerifiedNativeBindingKind.webhook_route,
        reference="route:v1:sha256:" + "b" * 64,
    )
    with pytest.raises(ValueError, match="conflicting verified binding sources"):
        await normalizer.identify(intent(route_binding, connection_id="forged-connection"))

    connection_binding = InternalVerifiedNativeBinding(
        kind=InternalVerifiedNativeBindingKind.connection,
        reference="connection-2",
    )
    with pytest.raises(ValueError, match="connection binding conflicts"):
        await normalizer.identify(intent(connection_binding, connection_id="connection-1"))


@pytest.mark.anyio
async def test_signed_route_reaches_real_runtime_and_redelivery_replays(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Exercise HMAC route -> dispatcher -> bus -> manager -> real normalizer."""

    monkeypatch.setenv("GITHUB_WEBHOOK_SECRET", _SECRET)
    monkeypatch.delenv("DEER_FLOW_ALLOW_UNVERIFIED_GITHUB_WEBHOOKS", raising=False)
    monkeypatch.delenv("DEER_FLOW_AUTH_DISABLED", raising=False)
    monkeypatch.setenv("DEER_FLOW_HOME", str(tmp_path))
    from deerflow.config import paths as paths_module

    monkeypatch.setattr(paths_module, "_paths", None)
    _invalidate_cache()
    _write_github_agent(tmp_path, owner="alice")
    _write_github_agent(tmp_path, owner="bob")

    graph_store = InMemoryStore()
    run_manager = RunManager(store=MemoryRunStore(), tenant=_TEST_TENANT)
    app = FastAPI()
    app.state.tenant_identity = _TEST_TENANT_IDENTITY
    app.include_router(github_webhooks.router)
    app.state.runtime_readiness = _ReadyAdmissionFence()
    app.state.stream_bridge = SimpleNamespace()
    app.state.run_manager = run_manager
    app.state.checkpointer = InMemorySaver()
    app.state.store = graph_store
    app.state.run_event_store = MemoryRunEventStore()
    app.state.run_events_config = None
    app.state.thread_store = MemoryThreadMetaStore(graph_store)

    bus = MessageBus()

    class _ChannelService:
        def __init__(self, receipt_processor) -> None:
            self.bus = bus
            self._receipt_processor = receipt_processor

        def is_channel_enabled(self, name: str) -> bool:
            return name == "github"

        def get_channel_config(self, _name: str) -> dict[str, object]:
            return {"enabled": True}

        def get_channel(self, _name: str):
            return None

        async def accept_verified_inbound_batch(self, messages) -> None:
            await self._receipt_processor.receive_batch(messages)

    import app.channels.service as service_module

    async def owner_user(_request, owner_user_id):
        return SimpleNamespace(
            id=owner_user_id,
            system_role="user",
            oauth_provider=None,
            oauth_id=None,
        )

    monkeypatch.setattr(
        "app.gateway.services.resolve_trusted_internal_owner_for_attribution",
        owner_user,
    )
    revision = ResolvedAgentRevision.from_material(
        ResolvedAgentMaterialV1(
            agent_id="reviewer",
            storage_source="file",
            storage_version="test",
            agent_config={"name": "reviewer"},
            soul="test",
            model_profile={},
        )
    )
    set_app_config(AppConfig.model_validate({"sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"}}))

    client = MagicMock()
    client.threads.create = AsyncMock(side_effect=lambda **kwargs: {"thread_id": kwargs["thread_id"]})
    client.threads.get = AsyncMock()
    client.threads.update = AsyncMock()
    client.runs.create = AsyncMock()
    client.runs.wait = AsyncMock()
    client.runs.stream = MagicMock()
    client.runs.join = AsyncMock()

    runtime = build_channel_invocation_runtime(app)
    recording_runtime = _RecordingRuntime(runtime)

    class _InteractiveConnectionRepositoryMustNotBeUsed:
        def __getattr__(self, name: str):
            raise AssertionError(f"webhook touched interactive connection repository: {name}")

    manager = ChannelManager(
        bus=bus,
        store=ChannelStore(path=tmp_path / "channels.json"),
        connection_repo=_InteractiveConnectionRepositoryMustNotBeUsed(),
        require_bound_identity=True,
        invocation_runtime=recording_runtime,
        get_stream_bridge=lambda: None,
    )
    manager._client = client
    receipt_engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'receipts.db'}")
    async with receipt_engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all, tables=[InboundReceiptRow.__table__])
    receipt_sessions = async_sessionmaker(receipt_engine, expire_on_commit=False)
    receipt_processor = InboundReceiptProcessor(
        store=SqlInboundReceiptStore(receipt_sessions),
        publish_wakeup=bus.publish_receipt_wakeup,
        process_message=manager.process_inbound_receipt_message,
        lease_owner="gateway-test",
    )
    manager.set_inbound_receipt_processor(receipt_processor)
    service = _ChannelService(receipt_processor)
    monkeypatch.setattr(service_module, "get_channel_service", lambda: service)
    body = json.dumps(_github_payload()).encode()
    headers = {
        "X-GitHub-Event": "pull_request",
        "X-GitHub-Delivery": "delivery-1",
        "X-Hub-Signature-256": _signature(body),
        "Content-Type": "application/json",
    }

    try:
        with (
            patch("app.gateway.services.resolve_agent_revision", return_value=revision),
            patch("app.gateway.services.resolve_agent_factory", return_value=object()),
            patch(
                "app.gateway.github.app_auth.mint_installation_token",
                new_callable=AsyncMock,
                return_value="transient-test-token",
            ),
            patch("app.gateway.services.run_agent", new_callable=AsyncMock) as run_agent,
        ):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test",
            ) as http:
                await manager.start()
                await receipt_processor.start()
                response = await http.post(
                    "/api/webhooks/github",
                    content=body,
                    headers=headers,
                )
                assert response.status_code == 200
                for _ in range(100):
                    if len(recording_runtime.receipts) == 2:
                        break
                    await asyncio.sleep(0.01)
                assert len(recording_runtime.receipts) == 2
                first_receipts = tuple(recording_runtime.receipts)
                for receipt in first_receipts:
                    assert receipt.record.task is not None
                    await receipt.record.task
                for _ in range(100):
                    async with receipt_sessions() as session:
                        stored_rows = tuple(await session.scalars(sa.select(InboundReceiptRow).where(InboundReceiptRow.provider_delivery_id == "delivery-1")))
                    if len(stored_rows) == 2 and all(stored.state == "completed" for stored in stored_rows):
                        break
                    await asyncio.sleep(0.01)
                assert len(stored_rows) == 2
                assert all(stored.state == "completed" for stored in stored_rows)
                assert all(stored.provider_event_digest is not None for stored in stored_rows)

                # The verified provider event is unchanged, but live execution
                # policy has changed since the first receipt. Redelivery must
                # retain the original accepted envelope instead of conflicting
                # or silently rewriting its policy.
                _write_github_agent(tmp_path, owner="alice", recursion_limit=200)
                _write_github_agent(tmp_path, owner="bob", recursion_limit=200)
                _invalidate_cache()

                redelivery = await http.post(
                    "/api/webhooks/github",
                    content=body,
                    headers=headers,
                )
                assert redelivery.status_code == 200
                await asyncio.sleep(0)

            assert len(recording_runtime.receipts) == 2
            assert all(receipt.created is True for receipt in first_receipts)
            assert {stored.run_id for stored in stored_rows} == {receipt.record.run_id for receipt in first_receipts}
            assert all(stored.payload_json["policy_metadata"]["github"]["recursion_limit"] == 100 for stored in stored_rows)
            assert run_agent.await_count == 2

            from deerflow.extensions.mcp import McpInvocationFacts

            principals: set[str] = set()
            thread_ids: set[str] = set()
            bindings: set[str] = set()
            for receipt in first_receipts:
                accepted = receipt.record.accepted_invocation
                assert accepted is not None
                principals.add(accepted.principal.user_id)
                thread_ids.add(receipt.record.thread_id)
                binding_reference = accepted.origin.references["binding_reference"]
                bindings.add(binding_reference)
                assert accepted.origin.references["binding_kind"] == "webhook_route"
                assert binding_reference.startswith("route:v1:sha256:")
                assert accepted.trusted_context is not None
                trusted_binding = next(reference.value for reference in accepted.trusted_context.origin.references if reference.key == "binding_reference")
                assert trusted_binding == binding_reference
                mcp_facts = McpInvocationFacts.from_accepted(
                    accepted,
                    run_id=receipt.record.run_id,
                )
                mcp_binding = next(reference.value for reference in mcp_facts.origin.references if reference.key == "binding_reference")
                assert mcp_binding == binding_reference
                assert "transient-test-token" not in json.dumps(accepted.to_persisted())
            assert principals == {"alice", "bob"}
            assert len(thread_ids) == 2
            assert len(bindings) == 2
            assert client.threads.create.await_count == 2

    finally:
        await receipt_processor.stop()
        await manager.stop()
        await receipt_engine.dispose()
        reset_app_config()


@pytest.mark.anyio
async def test_buffered_followup_uses_its_own_delivery_key_through_real_runtime(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """K2 must not be replayed as changed caller intent under K1's key."""

    monkeypatch.delenv("DEER_FLOW_AUTH_DISABLED", raising=False)
    monkeypatch.setenv("DEER_FLOW_HOME", str(tmp_path))
    from deerflow.config import paths as paths_module
    from deerflow.runtime import MemoryStreamBridge

    monkeypatch.setattr(paths_module, "_paths", None)

    async def owner_user(*_args, **_kwargs):
        return SimpleNamespace(
            id="owner-1",
            system_role="user",
            oauth_provider=None,
            oauth_id=None,
        )

    monkeypatch.setattr(
        "app.gateway.services.resolve_trusted_internal_owner_for_attribution",
        owner_user,
    )
    bridge = MemoryStreamBridge()
    graph_store = InMemoryStore()
    run_manager = RunManager(store=MemoryRunStore(), tenant=_TEST_TENANT)
    app = FastAPI()
    app.state.tenant_identity = _TEST_TENANT_IDENTITY
    app.state.runtime_readiness = _ReadyAdmissionFence()
    app.state.stream_bridge = bridge
    app.state.run_manager = run_manager
    app.state.checkpointer = InMemorySaver()
    app.state.store = graph_store
    app.state.run_event_store = MemoryRunEventStore()
    app.state.run_events_config = None
    app.state.thread_store = MemoryThreadMetaStore(graph_store)

    channel_store = ChannelStore(path=tmp_path / "channels.json")
    thread_id = "github-followup-thread"
    channel_store.set_thread_id(
        "github",
        "acme/widgets",
        thread_id,
        topic_id="7:reviewer",
        user_id="octocat",
    )
    client = MagicMock()
    client.threads.update = AsyncMock()
    client.runs.create = AsyncMock()
    runtime = _LoseCreatedResponseRuntime(
        build_channel_invocation_runtime(app),
        external_key="delivery-k2",
    )
    manager = ChannelManager(
        bus=MessageBus(),
        store=channel_store,
        require_bound_identity=True,
        invocation_runtime=runtime,
        get_stream_bridge=lambda: bridge,
    )
    manager._client = client

    binding = InternalVerifiedNativeBinding(
        kind=InternalVerifiedNativeBindingKind.webhook_route,
        reference="route:v1:sha256:" + "c" * 64,
    )

    def delivery(
        message_id: str,
        text: str,
        *,
        sender: str = "octocat",
    ) -> InboundMessage:
        return InboundMessage(
            channel_name="github",
            chat_id="acme/widgets",
            topic_id="7:reviewer",
            workspace_id="acme/widgets",
            user_id=sender,
            owner_user_id="owner-1",
            text=text,
            verified_source_binding=binding,
            metadata={
                "message_id": message_id,
                "agent_name": "reviewer",
                "github": {
                    "delivery_id": message_id,
                    "installation_id": 1234,
                    "recursion_limit": 50,
                },
            },
        )

    revision = ResolvedAgentRevision.from_material(
        ResolvedAgentMaterialV1(
            agent_id="reviewer",
            storage_source="file",
            storage_version="test",
            agent_config={"name": "reviewer"},
            soul="test",
            model_profile={},
        )
    )
    first_release = asyncio.Event()
    graph_starts: list[str] = []

    async def controlled_run_agent(*args, **_kwargs):
        run_id = args[2].run_id
        await run_manager.try_start(run_id)
        graph_starts.append(run_id)
        if len(graph_starts) == 1:
            await first_release.wait()
        await run_manager.set_status(
            run_id,
            RunStatus.success,
            lifecycle_type=LifecycleType.succeeded,
        )
        await bridge.publish_end(run_id)

    async def wait_until(predicate, *, timeout: float = 2.0) -> None:
        async with asyncio.timeout(timeout):
            while not predicate():
                await asyncio.sleep(0.01)

    set_app_config(AppConfig.model_validate({"sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"}}))
    try:
        with (
            patch("app.gateway.services.resolve_agent_revision", return_value=revision),
            patch("app.gateway.services.resolve_agent_factory", return_value=object()),
            patch(
                "app.gateway.github.app_auth.mint_installation_token",
                new_callable=AsyncMock,
                return_value="transient-test-token",
            ),
            patch("app.gateway.services.run_agent", side_effect=controlled_run_agent),
        ):
            await manager._handle_chat(delivery("delivery-k1", "first work"))
            await wait_until(lambda: len(graph_starts) == 1)

            await manager._handle_chat(delivery("delivery-k2", "follow-up work"))
            await manager._handle_chat(
                delivery(
                    "delivery-k3",
                    "second follow-up work",
                    sender="hubot",
                )
            )
            assert len(manager._followup_buffers[thread_id]) == 2
            retained_k2 = next(iter(manager._followup_buffers[thread_id].values()))
            assert "github_token" not in retained_k2.run_context
            assert "transient-test-token" not in repr(retained_k2)

            first_release.set()
            await wait_until(lambda: len(graph_starts) == 2)
            await wait_until(lambda: runtime.response_lost)
            assert next(iter(manager._followup_buffers[thread_id].values())) is retained_k2

            # A later drain replays the exact K2 entry. The known receipt
            # watches K2's retained terminal marker, which then chains K3.
            await manager._drain_followups_for_thread(client, thread_id)
            await wait_until(lambda: len(graph_starts) == 3)
            await wait_until(lambda: thread_id not in manager._followup_buffers)

        assert len(runtime.receipts) == 4
        first, followup, replay, second_followup = runtime.receipts
        assert len(graph_starts) == 3
        assert len({first.record.run_id, followup.record.run_id, second_followup.record.run_id}) == 3
        assert first.record.external_key == "raw:delivery-k1"
        assert followup.record.external_key == "raw:delivery-k2"
        assert second_followup.record.external_key == "raw:delivery-k3"
        assert replay.created is False
        assert replay.record.run_id == followup.record.run_id
        assert first.record.accepted_invocation.origin.references["provider_message_id"] == "delivery-k1"
        assert followup.record.accepted_invocation.origin.references["provider_message_id"] == "delivery-k2"
        assert second_followup.record.accepted_invocation.origin.references["provider_message_id"] == "delivery-k3"
        for receipt in (followup, second_followup):
            accepted = receipt.record.accepted_invocation
            assert accepted.origin.references["binding_kind"] == "webhook_route"
            assert accepted.origin.references["binding_reference"] == binding.reference
            trusted_binding = next(reference.value for reference in accepted.trusted_context.origin.references if reference.key == "binding_reference")
            assert trusted_binding == binding.reference
        assert followup.record.accepted_invocation.principal.channel_user_id == "octocat"
        assert second_followup.record.accepted_invocation.principal.channel_user_id == "hubot"
    finally:
        first_release.set()
        await manager.stop()
        reset_app_config()


@pytest.mark.anyio
async def test_unverified_development_webhook_stays_unkeyed(tmp_path: Path) -> None:
    class _Runtime:
        def __init__(self) -> None:
            self.intents = []

        async def launch(self, intent):
            self.intents.append(intent)
            return InternalLaunchReceipt(
                record=SimpleNamespace(
                    run_id="run-1",
                    thread_id=intent.thread_id,
                )
            )

    runtime = _Runtime()
    manager = ChannelManager(
        bus=MessageBus(),
        store=ChannelStore(path=tmp_path / "channels.json"),
        require_bound_identity=True,
        invocation_runtime=runtime,
        get_stream_bridge=lambda: None,
    )
    manager._client = MagicMock()
    manager._client.threads.create = AsyncMock(return_value={"thread_id": "thread-1"})
    message = InboundMessage(
        channel_name="github",
        chat_id="acme/widgets",
        user_id="octocat",
        owner_user_id="owner-1",
        workspace_id="acme/widgets",
        topic_id="7:reviewer",
        text="review",
        metadata={"message_id": "delivery-1", "agent_name": "reviewer"},
    )

    with patch(
        "app.gateway.github.app_auth.mint_installation_token",
        new_callable=AsyncMock,
    ):
        await manager._handle_chat(message)

    assert len(runtime.intents) == 1
    assert runtime.intents[0].external_key is None
    assert runtime.intents[0].native_channel.verified_binding is None


@pytest.mark.anyio
async def test_buzz_repository_binding_retains_durable_event_key(tmp_path: Path) -> None:
    """Buzz's adapter-owned repository lookup is a verified connection path."""

    class _ConnectionRepository:
        async def find_connection_by_external_identity(self, **kwargs):
            assert kwargs == {
                "provider": "buzz",
                "external_account_id": "pubkey-1",
                "workspace_id": "relay.example",
            }
            return {
                "id": "connection-1",
                "owner_user_id": "owner-1",
                "workspace_id": "relay.example",
            }

    class _Runtime:
        def __init__(self) -> None:
            self.intents = []

        async def launch(self, intent):
            self.intents.append(intent)
            return InternalLaunchReceipt(
                record=SimpleNamespace(
                    run_id="run-1",
                    thread_id=intent.thread_id,
                )
            )

    message = InboundMessage(
        channel_name="buzz",
        chat_id="channel-1",
        user_id="pubkey-1",
        text="review",
        workspace_id="relay.example",
        metadata={"event_id": "event-1"},
    )
    message = await attach_connection_identity(
        message,
        repo=_ConnectionRepository(),
        provider="buzz",
        workspace_id="relay.example",
    )
    runtime = _Runtime()
    manager = ChannelManager(
        bus=MessageBus(),
        store=ChannelStore(path=tmp_path / "channels.json"),
        require_bound_identity=True,
        invocation_runtime=runtime,
        get_stream_bridge=lambda: None,
    )
    manager._client = MagicMock()
    manager._client.threads.create = AsyncMock(return_value={"thread_id": "thread-1"})

    await manager._handle_chat(message)

    assert len(runtime.intents) == 1
    intent = runtime.intents[0]
    assert intent.external_key == "event-1"
    assert intent.native_channel.verified_binding is not None
    assert intent.native_channel.verified_binding.kind == "connection"
    assert intent.native_channel.verified_binding.reference == "connection-1"


@pytest.mark.anyio
async def test_authenticated_github_request_without_delivery_id_is_rejected_before_dispatch(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("DEER_FLOW_HOME", str(tmp_path))
    from deerflow.config import paths as paths_module

    monkeypatch.setattr(paths_module, "_paths", None)
    _invalidate_cache()
    _write_github_agent(tmp_path)
    bus = MessageBus()

    class _ChannelService:
        def __init__(self) -> None:
            self.bus = bus

        def is_channel_enabled(self, name: str) -> bool:
            return name == "github"

        def get_channel_config(self, _name: str) -> dict[str, object]:
            return {"enabled": True}

    import app.channels.service as service_module

    monkeypatch.setattr(
        service_module,
        "get_channel_service",
        lambda: _ChannelService(),
    )
    monkeypatch.setenv("GITHUB_WEBHOOK_SECRET", _SECRET)
    body = json.dumps(_github_payload()).encode()
    app = FastAPI()
    app.state.tenant_identity = _TEST_TENANT_IDENTITY
    app.include_router(github_webhooks.router)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as http:
        response = await http.post(
            "/api/webhooks/github",
            content=body,
            headers={
                "X-GitHub-Event": "pull_request",
                "X-Hub-Signature-256": _signature(body),
                "Content-Type": "application/json",
            },
        )

    assert response.status_code == 400
    assert response.json() == {"detail": "Invalid verified GitHub delivery identity"}
    assert bus.inbound_queue.empty()


@pytest.mark.anyio
async def test_legacy_connection_origin_replays_with_redundant_verified_binding(
    monkeypatch,
) -> None:
    async def owner_user(*_args, **_kwargs):
        return SimpleNamespace(
            id="owner-1",
            system_role="user",
            oauth_provider=None,
            oauth_id=None,
        )

    monkeypatch.setattr(
        "app.gateway.services.resolve_trusted_internal_owner_for_attribution",
        owner_user,
    )
    normalizer = _GatewayLaunchNormalizer(
        SimpleNamespace(
            state=SimpleNamespace(
                user=SimpleNamespace(id="internal", system_role="internal"),
                auth_source=AUTH_SOURCE_INTERNAL,
            ),
            app=SimpleNamespace(
                state=SimpleNamespace(tenant_identity=_TEST_TENANT_IDENTITY),
            ),
        ),
        trust_internal_launch_facts=True,
    )
    intent = InternalLaunchIntent(
        thread_id="thread-slack",
        assistant_id="lead_agent",
        context={
            "channel_user_id": "sender-1",
            "channel_name": "slack",
            "agent_name": None,
        },
        source_kind=InternalSourceKind.native_channel,
        owner_user_id="owner-1",
        external_key="message-1",
        native_channel=InternalNativeChannelFacts(
            provider="slack",
            connection_id="connection-1",
            workspace_id="workspace-1",
            chat_id="chat-1",
            topic_id=None,
            provider_message_id="message-1",
            channel_user_id="sender-1",
            resolved_assistant_id="lead_agent",
            resolved_agent_name=None,
            verified_binding=InternalVerifiedNativeBinding(
                kind=InternalVerifiedNativeBindingKind.connection,
                reference="connection-1",
            ),
        ),
    )
    identity = await normalizer.identify(intent)
    assert identity is not None and identity.caller_intent is not None
    legacy_origin = InvocationOrigin(
        source_kind="native_channel",
        references={
            "provider": "slack",
            "connection_id": "connection-1",
            "workspace_id": "workspace-1",
            "chat_id": "chat-1",
            "topic_id": None,
            "provider_message_id": "message-1",
            "channel_user_id": "sender-1",
        },
    )
    accepted = SimpleNamespace(
        principal_digest=identity.principal_digest,
        base_origin_digest=canonical_digest({"version": 1, "origin": legacy_origin.base_json()}),
        origin=legacy_origin,
    )
    record = SimpleNamespace(
        accepted_invocation=accepted,
        thread_id="thread-slack",
        caller_intent_json=identity.caller_intent.to_persisted(),
        caller_intent_digest_version=identity.caller_intent.digest_version,
        caller_intent_digest=identity.caller_intent.digest,
    )

    await normalizer.validate_replay(intent, identity, record)


@pytest.mark.parametrize("installation_id", [0, -1, True, "1234"])
def test_github_installation_id_requires_strict_positive_integer(
    installation_id,
) -> None:
    with pytest.raises(ValidationError):
        GitHubAgentConfig(installation_id=installation_id)
