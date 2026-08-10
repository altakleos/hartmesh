"""Accepted invocation sealing, launch-source trust, and pinned revisions."""

from __future__ import annotations

import asyncio
import threading
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from deerflow_extension_api import (
    ActingServiceV1,
    EffectiveSubjectV1,
    InvocationIdentityV1,
    SafeContextReferenceV1,
    SealedOriginV1,
)

from app.runtime.invocation import (
    InternalLaunchIntent,
    InternalNativeChannelFacts,
    InternalSourceKind,
)
from deerflow.runtime.accepted_invocation import (
    INVOCATION_IDENTITY_CONTEXT_KEY,
    INVOCATION_ORIGIN_CONTEXT_KEY,
    AcceptedInvocation,
    InvocationOrigin,
    PrincipalProjection,
    ResolvedAgentMaterialV1,
    ResolvedAgentRevision,
    canonical_digest,
)
from deerflow.runtime.agent_revision import (
    RESOLVED_AGENT_MATERIAL_CONTEXT_KEY,
    assert_agent_config_projection_complete,
    assert_app_config_projection_complete,
)
from deerflow.runtime.runs.manager import RunManager, ThreadOperationKind
from deerflow.runtime.runs.schemas import RunStatus
from deerflow.runtime.runs.store.memory import MemoryRunStore
from deerflow.runtime.runs.worker import RunContext, run_agent


def _material(*, soul: str = "steady") -> ResolvedAgentMaterialV1:
    return ResolvedAgentMaterialV1(
        agent_id="reviewer",
        storage_source="file",
        storage_version="v1",
        agent_config={"name": "reviewer", "skills": ["code-review"]},
        soul=soul,
        model_profile={"name": "default", "model": "gpt-test"},
        tool_groups=("coding",),
        tools=("bash", "read_file"),
        skills=({"name": "code-review", "manifest_digest": "a" * 64, "content_digest": "b" * 64},),
        runtime_defaults={
            "thinking_enabled": True,
            "reasoning_effort": None,
            "is_plan_mode": False,
            "subagent_enabled": False,
            "max_concurrent_subagents": 3,
            "max_total_subagents": 6,
        },
    )


def test_material_and_accepted_digests_are_stable_and_mutation_safe() -> None:
    config = {"name": "reviewer", "skills": ["code-review"]}
    material = replace(_material(), agent_config=config)
    revision = ResolvedAgentRevision.from_material(material)
    config["skills"].append("forged")

    assert revision.digest == ResolvedAgentRevision.from_material(_material()).digest
    assert revision.material is material

    principal = PrincipalProjection(user_id="u1", role="member")
    origin = InvocationOrigin(source_kind="http", references={"request_id": "req-1"})
    accepted = AcceptedInvocation.seal(
        principal=principal,
        origin=origin,
        thread_id="thread-1",
        context_references={"non_interactive": False},
        agent_revision=revision,
        normalized_input={"messages": [{"role": "user", "content": "hello"}]},
        execution_options={"multitask_strategy": "reject"},
        extension_generation=7,
        contributor_execution_digest=canonical_digest({"version": 1, "execution": []}),
    )

    assert len(accepted.principal_digest) == 64
    assert len(accepted.base_origin_digest) == 64
    assert len(accepted.runtime_identity_digest) == 64
    assert accepted.agent_revision.digest == revision.digest
    assert accepted.to_persisted()["decision_evidence_json"] == {"version": 1, "decisions": []}


def test_revision_digest_changes_with_execution_material_and_storage_version() -> None:
    base = _material()
    same_execution = replace(base, storage_version="v2")
    changed_execution = replace(base, soul="different")

    assert ResolvedAgentRevision.from_material(base).digest != ResolvedAgentRevision.from_material(same_execution).digest
    assert ResolvedAgentRevision.from_material(base).digest != ResolvedAgentRevision.from_material(changed_execution).digest


def test_legacy_persisted_run_without_accepted_fields_is_readable() -> None:
    assert AcceptedInvocation.from_persisted({}) is None


def test_agent_config_projector_classifies_every_factory_field() -> None:
    assert_agent_config_projection_complete()
    assert_app_config_projection_complete()


@pytest.mark.asyncio
@pytest.mark.parametrize("agent_id", ["1bot", "a" * 65, "a" * 128])
async def test_full_agent_identifier_domain_seals_into_trusted_context(
    monkeypatch,
    agent_id: str,
) -> None:
    from app.gateway import services

    material = replace(
        _material(),
        agent_id=agent_id,
        agent_config={"name": agent_id, "skills": ["code-review"]},
    )
    revision = ResolvedAgentRevision.from_material(material)
    monkeypatch.setattr(services, "resolve_agent_revision", lambda *_args, **_kwargs: revision)
    request = SimpleNamespace(
        state=SimpleNamespace(user=SimpleNamespace(id="u1", system_role="member")),
        app=SimpleNamespace(
            state=SimpleNamespace(
                extensions=SimpleNamespace(generation=1),
                capability_manifest=SimpleNamespace(digest="f" * 64),
                contributor_host=None,
            )
        ),
    )
    intent = InternalLaunchIntent(
        thread_id="_thread",
        assistant_id=agent_id,
        input={"messages": []},
    )

    accepted = await services._seal_accepted_invocation(
        request=request,
        intent=intent,
        config={"context": {}},
        graph_input={"messages": []},
        owner_user_id="u1",
        run_ctx=SimpleNamespace(app_config=object()),
    )

    assert accepted.agent_revision.agent_id == agent_id
    assert accepted.trusted_context is not None
    assert accepted.trusted_context.agent_revision.agent_id == agent_id


@pytest.mark.asyncio
async def test_cancelled_revision_resolution_releases_late_process_material(
    monkeypatch,
) -> None:
    from app.gateway import services

    material = _material()
    revision = ResolvedAgentRevision.from_material(material)
    resolver_started = threading.Event()
    resolver_continue = threading.Event()
    material_released = threading.Event()
    release_calls: list[ResolvedAgentMaterialV1] = []

    def blocking_resolver(*_args, **_kwargs):
        resolver_started.set()
        assert resolver_continue.wait(timeout=2)
        return revision

    def release_process_material(self: ResolvedAgentMaterialV1) -> None:
        release_calls.append(self)
        material_released.set()

    monkeypatch.setattr(services, "resolve_agent_revision", blocking_resolver)
    monkeypatch.setattr(ResolvedAgentMaterialV1, "release_process_material", release_process_material)
    request = SimpleNamespace(
        state=SimpleNamespace(user=SimpleNamespace(id="u1", system_role="member")),
        app=SimpleNamespace(
            state=SimpleNamespace(
                extensions=SimpleNamespace(generation=1),
                capability_manifest=SimpleNamespace(digest="f" * 64),
                contributor_host=None,
            )
        ),
    )
    config = {"context": {}}
    sealing = asyncio.create_task(
        services._seal_accepted_invocation(
            request=request,
            intent=InternalLaunchIntent(thread_id="thread-1", input={"messages": []}),
            config=config,
            graph_input={"messages": []},
            owner_user_id="u1",
            run_ctx=SimpleNamespace(app_config=object()),
        )
    )
    assert await asyncio.to_thread(resolver_started.wait, 1)

    sealing.cancel()
    resolver_continue.set()
    with pytest.raises(asyncio.CancelledError):
        await sealing

    assert await asyncio.to_thread(material_released.wait, 1)
    assert release_calls == [material]
    assert RESOLVED_AGENT_MATERIAL_CONTEXT_KEY not in config["context"]


@pytest.mark.asyncio
async def test_cancelled_run_context_contribution_releases_published_material(
    monkeypatch,
) -> None:
    from app.gateway import services

    material = _material()
    revision = ResolvedAgentRevision.from_material(material)
    contribution_started = asyncio.Event()
    material_released = threading.Event()
    release_calls: list[ResolvedAgentMaterialV1] = []
    empty_digest = canonical_digest({"version": 1, "execution": []})

    class BlockingContributorHost:
        async def contribute_origin(self, _request):
            return SimpleNamespace(
                persistable=(),
                runtime_only=(),
                secret_handles=(),
                execution_digest=empty_digest,
                diagnostics=(),
            )

        async def contribute_run_context(self, _request):
            contribution_started.set()
            await asyncio.Event().wait()

    def release_process_material(self: ResolvedAgentMaterialV1) -> None:
        release_calls.append(self)
        material_released.set()

    monkeypatch.setattr(services, "resolve_agent_revision", lambda *_args, **_kwargs: revision)
    monkeypatch.setattr(ResolvedAgentMaterialV1, "release_process_material", release_process_material)
    request = SimpleNamespace(
        state=SimpleNamespace(user=SimpleNamespace(id="u1", system_role="member")),
        app=SimpleNamespace(
            state=SimpleNamespace(
                extensions=SimpleNamespace(generation=1),
                capability_manifest=SimpleNamespace(digest="f" * 64),
                contributor_host=BlockingContributorHost(),
            )
        ),
    )
    config = {"context": {}}
    sealing = asyncio.create_task(
        services._seal_accepted_invocation(
            request=request,
            intent=InternalLaunchIntent(thread_id="thread-1", input={"messages": []}),
            config=config,
            graph_input={"messages": []},
            owner_user_id="u1",
            run_ctx=SimpleNamespace(app_config=object()),
        )
    )
    await asyncio.wait_for(contribution_started.wait(), timeout=1)

    sealing.cancel()
    with pytest.raises(asyncio.CancelledError):
        await sealing

    assert await asyncio.to_thread(material_released.wait, 1)
    assert release_calls == [material]
    assert RESOLVED_AGENT_MATERIAL_CONTEXT_KEY not in config["context"]


def _accepted(material: ResolvedAgentMaterialV1) -> AcceptedInvocation:
    return AcceptedInvocation.seal(
        principal=PrincipalProjection(user_id="u1"),
        origin=InvocationOrigin(source_kind="http"),
        thread_id="thread-worker",
        context_references={},
        agent_revision=ResolvedAgentRevision.from_material(material),
        normalized_input={},
        execution_options={},
        extension_generation=3,
        contributor_execution_digest=canonical_digest({"version": 1, "execution": []}),
    )


def _bridge() -> SimpleNamespace:
    return SimpleNamespace(
        publish=AsyncMock(),
        publish_end=AsyncMock(),
        cleanup=AsyncMock(),
    )


@pytest.mark.asyncio
async def test_restart_drift_fails_before_graph_construction_or_model_work() -> None:
    accepted = _accepted(_material())
    persisted_revision = replace(accepted.agent_revision, material=None)
    accepted = replace(accepted, agent_revision=persisted_revision)
    manager = RunManager()
    record = await manager.create_or_reject(
        "thread-worker",
        accepted_invocation=accepted,
    )
    factory_called = False

    def factory(*, config):
        nonlocal factory_called
        factory_called = True
        raise AssertionError("graph construction must not run after drift")

    await run_agent(
        _bridge(),
        manager,
        record,
        ctx=RunContext(
            checkpointer=None,
            agent_revision_resolver=lambda _record, _config: ResolvedAgentRevision.from_material(_material(soul="drifted")),
        ),
        agent_factory=factory,
        graph_input={},
        config={},
    )

    assert factory_called is False
    assert record.status is RunStatus.error
    assert record.stop_reason == "agent_revision_drift"


@pytest.mark.asyncio
async def test_restart_equality_uses_the_exact_resolved_object_once() -> None:
    material = _material()
    accepted = _accepted(material)
    accepted = replace(accepted, agent_revision=replace(accepted.agent_revision, material=None))
    manager = RunManager()
    record = await manager.create_or_reject(
        "thread-worker-equal",
        accepted_invocation=accepted,
    )
    calls = 0
    seen = None

    class _Agent:
        async def astream(self, *_args, **_kwargs):
            yield {"messages": []}

    def resolver(_record, _config):
        nonlocal calls
        calls += 1
        return ResolvedAgentRevision.from_material(material)

    def factory(*, config):
        nonlocal seen
        seen = config["context"][RESOLVED_AGENT_MATERIAL_CONTEXT_KEY]
        return _Agent()

    await run_agent(
        _bridge(),
        manager,
        record,
        ctx=RunContext(checkpointer=None, agent_revision_resolver=resolver),
        agent_factory=factory,
        graph_input={},
        config={},
    )

    assert calls == 1
    assert seen is material
    assert record.status is RunStatus.success


@pytest.mark.asyncio
async def test_pinned_material_replaces_mutated_factory_context_after_digest_check() -> None:
    material = replace(
        _material(),
        runtime_defaults={
            **dict(_material().runtime_defaults),
            "agent_name": "sealed-target",
            "is_bootstrap": True,
            "non_interactive": False,
            "channel_name": None,
        },
    )
    manager = RunManager()
    record = await manager.create_or_reject(
        "thread-worker-mutation",
        accepted_invocation=_accepted(material),
    )
    seen: dict[str, object] = {}

    class _Agent:
        async def astream(self, *_args, **_kwargs):
            yield {"messages": []}

    def factory(*, config):
        seen.update(config["context"])
        return _Agent()

    await run_agent(
        _bridge(),
        manager,
        record,
        ctx=RunContext(checkpointer=None),
        agent_factory=factory,
        graph_input={},
        config={"context": {"agent_name": "mutated-target", "is_bootstrap": False}},
    )

    assert seen["agent_name"] == "sealed-target"
    assert seen["is_bootstrap"] is True
    assert record.status is RunStatus.success


@pytest.mark.asyncio
async def test_worker_replaces_forged_runtime_identity_with_accepted_facts() -> None:
    material = _material()
    identity = InvocationIdentityV1(
        effective_subject=EffectiveSubjectV1(kind="human", subject_id="owner-1"),
        acting_service=ActingServiceV1(service_id="channel:telegram"),
    )
    accepted = AcceptedInvocation.seal(
        principal=PrincipalProjection(identity=identity, channel_user_id="telegram-user"),
        origin=InvocationOrigin(
            source_kind="native_channel",
            references={"provider": "telegram", "chat_id": "chat-1"},
        ),
        thread_id="thread-worker-identity",
        context_references={},
        agent_revision=ResolvedAgentRevision.from_material(material),
        normalized_input={},
        execution_options={},
        extension_generation=3,
        contributor_execution_digest=canonical_digest({"version": 1, "execution": []}),
    )
    manager = RunManager()
    record = await manager.create_or_reject(
        "thread-worker-identity",
        accepted_invocation=accepted,
    )
    seen: dict[str, object] = {}

    class _Agent:
        async def astream(self, *_args, **_kwargs):
            yield {"messages": []}

    def factory(*, config):
        seen.update(config["context"])
        return _Agent()

    forged_identity = InvocationIdentityV1(effective_subject=EffectiveSubjectV1(kind="service", subject_id="forged-root"))
    forged_origin = SealedOriginV1(source_kind="service", digest="f" * 64)
    await run_agent(
        _bridge(),
        manager,
        record,
        ctx=RunContext(checkpointer=None),
        agent_factory=factory,
        graph_input={},
        config={
            "context": {
                INVOCATION_IDENTITY_CONTEXT_KEY: forged_identity,
                INVOCATION_ORIGIN_CONTEXT_KEY: forged_origin,
                "is_internal": True,
            }
        },
    )

    assert seen[INVOCATION_IDENTITY_CONTEXT_KEY] is identity
    origin = seen[INVOCATION_ORIGIN_CONTEXT_KEY]
    assert isinstance(origin, SealedOriginV1)
    assert origin.source_kind == "native_channel"
    assert origin.digest == accepted.base_origin_digest
    assert seen["user_id"] == "owner-1"
    assert seen["is_internal"] is False
    assert record.status is RunStatus.success


@pytest.mark.asyncio
async def test_normal_rows_persist_safe_facts_and_auxiliary_rows_do_not() -> None:
    store = MemoryRunStore()
    manager = RunManager(store=store)
    accepted = _accepted(_material())
    record = await manager.create_or_reject(
        "thread-persisted",
        accepted_invocation=accepted,
    )
    row = await store.get(record.run_id, user_id=None)

    assert row["origin_json"]["source_kind"] == "http"
    assert row["principal_projection_json"]["user_id"] == "u1"
    assert row["agent_revision_digest"] == accepted.agent_revision.digest
    assert "soul" not in row["agent_revision_json"]

    await manager.cancel(record.run_id)
    async with manager.reserve_thread_operation(
        "thread-persisted",
        kind=ThreadOperationKind.checkpoint_write,
        user_id=None,
    ):
        auxiliary = next(item for item in await store.list_inflight() if item["operation_kind"] == ThreadOperationKind.checkpoint_write.value)
        assert auxiliary["origin_json"] is None
        assert auxiliary["agent_revision_digest"] is None


@pytest.mark.asyncio
async def test_store_reconstruction_rejects_corrupt_accepted_evidence_before_recovery() -> None:
    store = MemoryRunStore()
    writer = RunManager(store=store)
    record = await writer.create_or_reject(
        "thread-corrupt-reconstruction",
        user_id="u1",
        accepted_invocation=_accepted(_material()),
    )
    store._runs[record.run_id]["principal_projection_digest"] = "0" * 64

    reconstructed = RunManager(store=store)
    assert await reconstructed.get(record.run_id, user_id="u1") is None
    assert (
        await reconstructed.reconcile_orphaned_inflight_runs(
            error="worker disappeared",
        )
        == []
    )
    persisted = await store.get(record.run_id, user_id=None)
    assert persisted is not None
    assert persisted["status"] == "pending"


@pytest.mark.asyncio
async def test_external_replay_lookup_rejects_corrupt_accepted_evidence_with_stable_error() -> None:
    store = MemoryRunStore()
    writer = RunManager(store=store)
    accepted = _accepted(_material())
    admission = await writer.ensure_or_reject(
        "thread-corrupt-replay",
        external_scope="http:user:u1",
        external_key="request-1",
        request_digest="1" * 64,
        request_digest_version="sha256-canonical-json-v1",
        caller_intent_json={"version": 1, "fields": {}},
        caller_intent_digest="2" * 64,
        caller_intent_digest_version="sha256-canonical-json-v1",
        user_id="u1",
        accepted_invocation=accepted,
    )
    store._runs[admission.record.run_id]["principal_projection_digest"] = "0" * 64

    reconstructed = RunManager(store=store)
    with pytest.raises(RuntimeError, match="^accepted_evidence_invalid$"):
        await reconstructed.get_by_external_identity(
            "http:user:u1",
            "request-1",
            user_id="u1",
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("intent", "expected"),
    [
        (
            InternalLaunchIntent(thread_id="thread-http"),
            {"source_kind": "http"},
        ),
        (
            InternalLaunchIntent(
                thread_id="thread-task",
                source_kind=InternalSourceKind.scheduled_task,
                trusted_task_id="task-1",
                task_run_id="occurrence-1",
                scheduled_trigger="scheduled",
                owner_user_id="u1",
            ),
            {"source_kind": "scheduled_task", "task_id": "task-1", "task_run_id": "occurrence-1"},
        ),
        (
            InternalLaunchIntent(
                thread_id="thread-channel",
                assistant_id="lead_agent",
                source_kind=InternalSourceKind.native_channel,
                owner_user_id="u1",
                native_channel=InternalNativeChannelFacts(
                    provider="slack",
                    connection_id="connection-1",
                    workspace_id="workspace-1",
                    chat_id="chat-1",
                    topic_id=None,
                    provider_message_id=None,
                    channel_user_id="platform-user",
                    resolved_assistant_id="lead_agent",
                    resolved_agent_name=None,
                ),
            ),
            {"source_kind": "native_channel", "provider": "slack", "provider_message_id": None},
        ),
    ],
)
async def test_every_launch_source_is_sealed_with_host_selected_origin(
    monkeypatch,
    intent: InternalLaunchIntent,
    expected: dict[str, object],
) -> None:
    from app.gateway import services

    revision = ResolvedAgentRevision.from_material(_material())
    monkeypatch.setattr(services, "resolve_agent_revision", lambda *_args, **_kwargs: revision)

    class _ContributorSpy:
        def __init__(self) -> None:
            self.origin_requests = []
            self.context_requests = []

        async def contribute_origin(self, contributor_request):
            self.origin_requests.append(contributor_request)
            return SimpleNamespace(
                persistable=(
                    SimpleNamespace(
                        contribution_id="audit",
                        namespace="audit",
                        reference=SafeContextReferenceV1(
                            key="tenant",
                            value="tenant-1",
                            storage_class="persistable",
                            purpose="correlation",
                        ),
                    ),
                ),
                runtime_only=(),
                secret_handles=(),
                execution_digest=canonical_digest({"version": 1, "execution": []}),
                diagnostics=(),
            )

        async def contribute_run_context(self, contributor_request):
            self.context_requests.append(contributor_request)
            return SimpleNamespace(
                persistable=(),
                runtime_only=(
                    SimpleNamespace(
                        contribution_id="routing",
                        namespace="routing",
                        reference=SafeContextReferenceV1(
                            key="target",
                            value="ephemeral-route",
                            storage_class="runtime_only",
                            purpose="execution",
                        ),
                    ),
                ),
                secret_handles=(
                    SimpleNamespace(
                        contribution_id="routing",
                        namespace="routing",
                        reference=SafeContextReferenceV1(
                            key="credential",
                            value="vault://tenant/api",
                            storage_class="persistable",
                            purpose="secret_handle",
                        ),
                    ),
                ),
                execution_digest=canonical_digest({"version": 1, "execution": []}),
                diagnostics=(),
            )

    contributor_spy = _ContributorSpy()
    request = SimpleNamespace(
        state=SimpleNamespace(
            user=SimpleNamespace(id="u1", system_role="member"),
        ),
        app=SimpleNamespace(
            state=SimpleNamespace(
                extensions=SimpleNamespace(generation=9),
                capability_manifest=SimpleNamespace(digest="f" * 64),
                contributor_host=contributor_spy,
            )
        ),
    )
    config = {
        "context": {
            "user_id": "forged-body-user",
            "user_role": "member",
            "channel_user_id": (intent.native_channel.channel_user_id if intent.native_channel is not None else None),
        }
    }
    accepted = await services._seal_accepted_invocation(
        request=request,
        intent=intent,
        config=config,
        graph_input={"messages": []},
        owner_user_id=intent.owner_user_id,
        run_ctx=SimpleNamespace(app_config=object()),
    )

    persisted = accepted.origin.to_json()
    assert persisted["source_kind"] == expected["source_kind"]
    for key, value in expected.items():
        if key != "source_kind":
            assert persisted["references"][key] == value
    assert accepted.principal.user_id == "u1"
    assert accepted.principal.role == "member"
    assert accepted.extension_generation == 9
    assert accepted.extension_manifest_digest == "f" * 64
    assert accepted.agent_revision.material is revision.material
    assert accepted.trusted_context is not None
    assert accepted.trusted_context.origin is contributor_spy.context_requests[0].origin
    assert accepted.trusted_context.persistable_references[0].fully_qualified_key == "audit.tenant"
    assert accepted.trusted_context.runtime_only_references[0].reference.value == "ephemeral-route"
    assert accepted.trusted_context.secret_handles[0].reference.value == "vault://tenant/api"
    accepted_wire = repr(accepted.to_persisted())
    assert "tenant-1" in accepted_wire
    assert "vault://tenant/api" in accepted_wire
    assert "ephemeral-route" not in accepted_wire
    assert [item.source_kind for item in contributor_spy.origin_requests] == [expected["source_kind"]]
    assert [item.thread_id for item in contributor_spy.context_requests] == [intent.thread_id]
    assert contributor_spy.context_requests[0].agent_revision.digest == revision.digest
