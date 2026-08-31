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
    canonical_effective_execution_digest,
)
from deerflow.runtime.agent_revision import (
    RESOLVED_AGENT_MATERIAL_CONTEXT_KEY,
    assert_agent_config_projection_complete,
    assert_app_config_projection_complete,
)
from deerflow.runtime.events.store.memory import MemoryRunEventStore
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
    assert accepted.to_persisted()["decision_evidence_json"] == {
        "version": 1,
        "decisions": [],
        "tool_receipts": {"version": 1},
    }
    assert accepted.tool_receipt_evidence_version == 1
    legacy = replace(
        accepted,
        decision_evidence={"version": 1, "decisions": []},
    )
    assert legacy.tool_receipt_evidence_version is None


def test_persisted_effective_execution_restores_frozen_execution_options() -> None:
    execution_options = {
        "multitask_strategy": "reject",
        "interrupt_before": None,
        "interrupt_after": None,
        "checkpoint_id": None,
        "recursion_limit": 1000,
    }
    accepted = AcceptedInvocation.seal(
        principal=PrincipalProjection(user_id="u1", role="member"),
        origin=InvocationOrigin(source_kind="http"),
        thread_id="thread-persisted-options",
        context_references={},
        agent_revision=ResolvedAgentRevision.from_material(_material()),
        normalized_input={},
        execution_options=execution_options,
        extension_generation=7,
        contributor_execution_digest=canonical_digest(
            {"version": 1, "execution": []},
        ),
    )
    effective_projection = {
        "accepted_digest_semantics": "canonical_execution_v2",
        "thread_id": accepted.thread_id,
        "agent_selector": "default",
        "agent_revision_digest": accepted.agent_revision.digest,
        "principal_digest": accepted.principal_digest,
        "base_origin_digest": accepted.base_origin_digest,
        "accepted_context_digest": accepted.accepted_context_digest,
        "runtime_identity_digest": accepted.runtime_identity_digest,
        "contributor_execution_digest": accepted.contributor_execution_digest,
        "extension_generation": accepted.extension_generation,
        "input": {},
        "command": None,
        "multitask_strategy": "reject",
        "checkpoint": {},
        "interrupt_before": None,
        "interrupt_after": None,
        "execution_context": {},
        "recursion_limit": 1000,
    }
    persisted = {
        **accepted.to_persisted(),
        "thread_id": accepted.thread_id,
        "kwargs": {
            "__accepted_request_projection_v1": effective_projection,
        },
        "request_digest": canonical_effective_execution_digest(
            effective_projection,
        ),
        "request_digest_version": "sha256-canonical-json-v1",
    }

    restored = AcceptedInvocation.from_persisted(persisted)

    assert restored is not None
    assert dict(restored.execution_options) == execution_options


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
async def test_legacy_nonterminal_revision_fails_before_resolution_or_graph_work() -> None:
    accepted = _accepted(_material())
    accepted = replace(
        accepted,
        agent_revision=replace(
            accepted.agent_revision,
            subagent_catalog=None,
            skill_scopes=None,
            legacy_live_catalog=True,
            material=None,
        ),
    )
    manager = RunManager()
    record = await manager.create_or_reject(
        "thread-worker-legacy-catalog",
        accepted_invocation=accepted,
    )
    resolver = AsyncMock(side_effect=AssertionError("legacy rows must not resolve live material"))
    factory = AsyncMock(side_effect=AssertionError("legacy rows must not construct a graph"))

    await run_agent(
        _bridge(),
        manager,
        record,
        ctx=RunContext(
            checkpointer=None,
            agent_revision_resolver=resolver,
        ),
        agent_factory=factory,
        graph_input={},
        config={},
    )

    resolver.assert_not_called()
    factory.assert_not_called()
    assert record.status is RunStatus.error
    assert record.stop_reason == "subagent_catalog_unavailable"


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
async def test_restart_rebinds_persisted_catalog_instead_of_live_managed_state() -> None:
    from deerflow.runtime.subagent_snapshot import (
        ResolvedSkillScopesV1,
        ResolvedSubagentCatalogV1,
        resolved_subagent_definition,
    )

    entry = resolved_subagent_definition(
        name="planner",
        source_kind="managed",
        source_version="source-v1",
        description="Accepted planner",
        system_prompt="Accepted prompt",
        model=None,
        model_settings={},
        tool_names=(),
        skill_names=(),
        max_turns=8,
        timeout_seconds=30,
        inherits_tools=True,
    )
    catalog = ResolvedSubagentCatalogV1.from_entries(
        (entry,),
        allowed_names=("planner",),
    )
    material = replace(
        _material(),
        subagent_catalog=catalog,
        skill_scopes=ResolvedSkillScopesV1.from_scopes({"lead": (), "subagent:planner": ()}),
    )
    accepted = _accepted(material)
    accepted = replace(
        accepted,
        agent_revision=replace(accepted.agent_revision, material=None),
    )
    live_candidate = replace(
        material,
        subagent_catalog=ResolvedSubagentCatalogV1.empty(),
        skill_scopes=ResolvedSkillScopesV1.empty(),
    )
    manager = RunManager()
    record = await manager.create_or_reject(
        "thread-worker-catalog-recovery",
        accepted_invocation=accepted,
    )
    seen = None

    class _Agent:
        async def astream(self, *_args, **_kwargs):
            yield {"messages": []}

    def factory(*, config):
        nonlocal seen
        seen = config["context"][RESOLVED_AGENT_MATERIAL_CONTEXT_KEY]
        return _Agent()

    await run_agent(
        _bridge(),
        manager,
        record,
        ctx=RunContext(
            checkpointer=None,
            agent_revision_resolver=lambda _record, _config: ResolvedAgentRevision.from_material(live_candidate),
        ),
        agent_factory=factory,
        graph_input={},
        config={},
    )

    assert seen is not None
    assert seen.subagent_catalog == catalog
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


def _durable_test_descriptor(material: ResolvedAgentMaterialV1, *, prompt: str = "accepted prompt"):
    from deerflow.agents.assembly_descriptor import (
        build_assembly_descriptor,
        subagent_release_policy,
    )

    defaults = material.runtime_defaults
    return build_assembly_descriptor(
        namespace="deerflow",
        agent_name=str(defaults.get("agent_name") or "lead-agent"),
        requested_model=None,
        effective_model=str(material.model_profile["name"]),
        model_config=SimpleNamespace(),
        thinking_enabled=bool(defaults.get("thinking_enabled", True)),
        reasoning_effort=defaults.get("reasoning_effort"),
        rendered_base_prompt=prompt,
        tools=[],
        middlewares=[],
        deferred_names=frozenset(),
        enabled_skills=list(material.enabled_skill_objects),
        effective_policies={
            "bootstrap": False,
            "non_interactive": bool(defaults.get("non_interactive", False)),
            "plan_mode": bool(defaults.get("is_plan_mode", False)),
            "recursion_limit": "framework-default",
            "subagents": subagent_release_policy(
                material.app_config,
                enabled=False,
                max_concurrent=int(defaults.get("max_concurrent_subagents", 3)),
                max_total=int(defaults.get("max_total_subagents", 6)),
                resolved_subagent_catalog=material.subagent_catalog,
            ),
        },
    )


@pytest.mark.asyncio
async def test_accepted_durable_bare_graph_fails_before_graph_invocation() -> None:
    from deerflow.runtime.assembly_evidence import assembly_evidence_is_required

    store = MemoryRunStore()
    manager = RunManager(store=store, worker_id="worker-assembly")
    record = await manager.create_or_reject(
        "thread-worker-bare",
        accepted_invocation=_accepted(_material()),
    )
    calls = {"factory": 0, "astream": 0}

    class Agent:
        async def astream(self, *_args, **_kwargs):
            calls["astream"] += 1
            yield {"messages": []}

    def factory(*, config):
        calls["factory"] += 1
        assert assembly_evidence_is_required(config["context"])
        return Agent()

    await run_agent(
        _bridge(),
        manager,
        record,
        ctx=RunContext(checkpointer=None),
        agent_factory=factory,
        graph_input={},
        config={},
    )

    assert calls == {"factory": 1, "astream": 0}
    assert record.status is RunStatus.error
    assert record.stop_reason == "assembly_evidence_unavailable"
    row = await store.get(record.run_id)
    assert row is not None
    assert row["assembly_evidence_json"] is None
    assert row["assembly_evidence_digest"] is None


@pytest.mark.asyncio
async def test_accepted_durable_evidence_is_bound_before_checkpoint_access_and_astream(
    monkeypatch,
) -> None:
    from deerflow.agents.lead_agent.agent import LeadAgentAssembly

    material = _material()
    store = MemoryRunStore()
    manager = RunManager(store=store, worker_id="worker-assembly")
    record = await manager.create_or_reject(
        "thread-worker-evidence",
        accepted_invocation=_accepted(material),
    )
    observed_bound_evidence = False
    checkpoint_preflight_saw_bound_evidence = False

    async def compatibility_check(_checkpointer, _config, _mode):
        nonlocal checkpoint_preflight_saw_bound_evidence
        row = await store.get(record.run_id)
        checkpoint_preflight_saw_bound_evidence = bool(row and row["status"] == "running" and row["assembly_evidence_json"] and row["assembly_evidence_digest"])

    monkeypatch.setattr(
        "deerflow.runtime.runs.worker.aensure_checkpoint_mode_compatible",
        compatibility_check,
    )

    class Agent:
        async def astream(self, *_args, **_kwargs):
            nonlocal observed_bound_evidence
            row = await store.get(record.run_id)
            observed_bound_evidence = bool(row and row["status"] == "running" and row["assembly_evidence_json"] and row["assembly_evidence_digest"])
            yield {"messages": []}

    def factory(*, config):
        return LeadAgentAssembly(
            graph=Agent(),
            descriptor=_durable_test_descriptor(material),
        )

    await run_agent(
        _bridge(),
        manager,
        record,
        ctx=RunContext(
            checkpointer=object(),
            event_store=MemoryRunEventStore(run_store=store),
        ),
        agent_factory=factory,
        graph_input={},
        config={},
    )

    assert checkpoint_preflight_saw_bound_evidence is True
    assert observed_bound_evidence is True
    assert record.status is RunStatus.success
    row = await store.get(record.run_id)
    assert row is not None
    assert row["status"] == "success"
    assert row["assembly_evidence_json"] == record.assembly_evidence_json
    assert row["assembly_evidence_digest"] == record.assembly_evidence_digest


@pytest.mark.asyncio
async def test_legacy_accepted_run_binds_assembly_without_starting_receipt_tail() -> None:
    from deerflow.agents.lead_agent.agent import LeadAgentAssembly

    material = _material()
    legacy = replace(
        _accepted(material),
        decision_evidence={"version": 1, "decisions": []},
    )
    store = MemoryRunStore()
    manager = RunManager(store=store, worker_id="worker-legacy-receipts")
    record = await manager.create_or_reject(
        "thread-worker-legacy-receipts",
        accepted_invocation=legacy,
    )
    astream_calls = 0

    class Agent:
        async def astream(self, *_args, **_kwargs):
            nonlocal astream_calls
            astream_calls += 1
            yield {"messages": []}

    await run_agent(
        _bridge(),
        manager,
        record,
        ctx=RunContext(checkpointer=None, event_store=None),
        agent_factory=lambda **_kwargs: LeadAgentAssembly(
            graph=Agent(),
            descriptor=_durable_test_descriptor(material),
        ),
        graph_input={},
        config={},
    )

    assert astream_calls == 1
    assert record.status is RunStatus.success


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal_outcome", ["failure", "cancellation", "timeout"])
async def test_worker_retains_bound_evidence_on_terminal_outcome(
    terminal_outcome: str,
) -> None:
    from deerflow.agents.lead_agent.agent import LeadAgentAssembly
    from deerflow.runtime.runs.store.base import LifecycleType

    material = _material()
    store = MemoryRunStore()
    manager = RunManager(store=store, worker_id="worker-terminal-evidence")
    record = await manager.create_or_reject(
        f"thread-terminal-{terminal_outcome}",
        accepted_invocation=_accepted(material),
    )
    stream_started = asyncio.Event()
    release_stream = asyncio.Event()

    class Agent:
        async def astream(self, *_args, **_kwargs):
            stream_started.set()
            if terminal_outcome == "failure":
                raise RuntimeError("expected model failure")
            await release_stream.wait()
            if False:
                yield {"messages": []}

    task = asyncio.create_task(
        run_agent(
            _bridge(),
            manager,
            record,
            ctx=RunContext(
                checkpointer=None,
                event_store=MemoryRunEventStore(run_store=store),
            ),
            agent_factory=lambda **_kwargs: LeadAgentAssembly(
                graph=Agent(),
                descriptor=_durable_test_descriptor(material),
            ),
            graph_input={},
            config={},
        )
    )
    record.task = task
    await asyncio.wait_for(stream_started.wait(), timeout=1)

    if terminal_outcome == "cancellation":
        await manager.cancel(record.run_id)
    elif terminal_outcome == "timeout":
        await manager.set_status(
            record.run_id,
            RunStatus.timeout,
            error="Run timed out",
            lifecycle_type=LifecycleType.timed_out,
        )
        release_stream.set()

    await asyncio.wait_for(task, timeout=1)

    row = await store.get(record.run_id)
    assert row is not None
    assert (
        row["status"]
        == {
            "failure": "error",
            "cancellation": "interrupted",
            "timeout": "timeout",
        }[terminal_outcome]
    )
    assert row["assembly_evidence_json"] is not None
    assert row["assembly_evidence_digest"] is not None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changed_dimension",
    ["identical", "model", "prompt", "tool_authorization", "middleware", "skills", "policy"],
)
async def test_recovered_assembly_must_match_original_before_astream(
    changed_dimension: str,
) -> None:
    from deerflow_extension_api import MiddlewareDescriptor, ToolDescriptor

    from deerflow.agents.assembly_descriptor import skill_catalog_digest
    from deerflow.agents.lead_agent.agent import LeadAgentAssembly
    from deerflow.runtime.assembly_evidence import (
        AcceptedAssemblyAnchors,
        assembly_evidence_digest,
        build_assembly_evidence,
        canonical_durable_policy_digest,
        canonical_skillset_digest,
    )
    from deerflow.runtime.runs.store.base import BindAssemblyEvidenceOutcome

    material = _material()
    accepted = _accepted(material)
    original_descriptor = _durable_test_descriptor(material, prompt="original prompt")
    if changed_dimension == "identical":
        changed_descriptor = original_descriptor
    elif changed_dimension == "model":
        changed_descriptor = replace(original_descriptor, effective_model="changed-model")
    elif changed_dimension == "prompt":
        changed_descriptor = replace(original_descriptor, base_prompt_hash="f" * 64)
    elif changed_dimension == "tool_authorization":
        changed_descriptor = replace(
            original_descriptor,
            tools=(
                ToolDescriptor(
                    name="newly-authorized-tool",
                    description_hash="1" * 64,
                    schema_hash="2" * 64,
                    source="builtin",
                ),
            ),
        )
    elif changed_dimension == "middleware":
        changed_descriptor = replace(
            original_descriptor,
            middlewares=(
                MiddlewareDescriptor(
                    name="NewPolicyMiddleware",
                    module="tests.assembly",
                    policy_parameters={"enabled": True},
                ),
            ),
        )
    elif changed_dimension == "skills":
        changed_descriptor = replace(original_descriptor, enabled_skills=("forged-skill",))
    else:
        changed_descriptor = replace(
            original_descriptor,
            effective_policies={
                **original_descriptor.effective_policies,
                "recursion_limit": 999,
            },
        )
    original_evidence = build_assembly_evidence(
        original_descriptor,
        anchors=AcceptedAssemblyAnchors(
            run_id="placeholder",
            expected_namespace="deerflow",
            expected_agent_name="lead-agent",
            expected_effective_model="default",
            expected_skillset_digest=canonical_skillset_digest(
                (),
                catalog_digest=skill_catalog_digest([]),
            ),
            expected_policy_digest=canonical_durable_policy_digest(original_descriptor.effective_policies),
            agent_revision_digest=accepted.agent_revision.digest,
            extension_generation=accepted.extension_generation,
        ),
    )

    class RecoveredStore(MemoryRunStore):
        durable_lifecycle = True

        async def start_run(self, run_id, **kwargs):
            started = await super().start_run(run_id, **kwargs)
            assert started is True
            row = await self.get(run_id)
            assert row is not None
            outcome = await super().bind_assembly_evidence(
                run_id,
                owner_id=row["owner_worker_id"],
                lease_epoch=row["state_version"],
                evidence_json=original_evidence.to_persisted_json(),
                evidence_digest=assembly_evidence_digest(original_evidence),
            )
            assert outcome is BindAssemblyEvidenceOutcome.bound
            return True

    store = RecoveredStore()
    manager = RunManager(store=store, worker_id="worker-assembly")
    record = await manager.create_or_reject(
        "thread-worker-recovered",
        accepted_invocation=accepted,
    )
    astream_calls = 0

    class Agent:
        async def astream(self, *_args, **_kwargs):
            nonlocal astream_calls
            astream_calls += 1
            yield {"messages": []}

    await run_agent(
        _bridge(),
        manager,
        record,
        ctx=RunContext(
            checkpointer=None,
            event_store=MemoryRunEventStore(run_store=store),
        ),
        agent_factory=lambda **_kwargs: LeadAgentAssembly(
            graph=Agent(),
            descriptor=changed_descriptor,
        ),
        graph_input={},
        config={},
    )

    if changed_dimension == "identical":
        assert astream_calls == 1
        assert record.status is RunStatus.success
        assert record.stop_reason is None
    else:
        assert astream_calls == 0
        assert record.status is RunStatus.error
        assert record.stop_reason == "agent_assembly_drift"
    row = await store.get(record.run_id)
    assert row is not None
    assert row["assembly_evidence_json"] == original_evidence.to_persisted_json()


@pytest.mark.asyncio
async def test_broken_assembly_observer_is_fail_open_and_evidence_still_binds() -> None:
    from deerflow.agents.lead_agent.agent import LeadAgentAssembly
    from deerflow.extensions.notify import notify_agent_assembled
    from deerflow.extensions.registry import ExtensionRegistry

    material = _material()
    store = MemoryRunStore()
    manager = RunManager(store=store, worker_id="worker-observer")
    record = await manager.create_or_reject(
        "thread-worker-observer",
        accepted_invocation=_accepted(material),
    )
    observer_calls = 0

    class BrokenObserver:
        def on_agent_assembled(self, _app_store, _descriptor):
            nonlocal observer_calls
            observer_calls += 1
            raise RuntimeError("private observer failure")

    registry = ExtensionRegistry()
    with registry.attributed_to("broken-observer"):
        registry.agent_assembly_observer(BrokenObserver())
    extensions = registry.build()

    class Agent:
        async def astream(self, *_args, **_kwargs):
            yield {"messages": []}

    def factory(**_kwargs):
        descriptor = _durable_test_descriptor(material)
        notify_agent_assembled(descriptor)
        return LeadAgentAssembly(graph=Agent(), descriptor=descriptor)

    await run_agent(
        _bridge(),
        manager,
        record,
        ctx=RunContext(
            checkpointer=None,
            event_store=MemoryRunEventStore(run_store=store),
            extensions=extensions,
        ),
        agent_factory=factory,
        graph_input={},
        config={},
    )

    row = await store.get(record.run_id)
    assert observer_calls == 1
    assert record.status is RunStatus.success
    assert row is not None
    assert row["assembly_evidence_json"] is not None
    assert row["assembly_evidence_digest"] is not None
    assert "private observer failure" not in str(row)


@pytest.mark.asyncio
async def test_ownership_loss_during_evidence_bind_does_not_terminalize_new_owner() -> None:
    from deerflow.agents.lead_agent.agent import LeadAgentAssembly
    from deerflow.runtime.runs.store.base import BindAssemblyEvidenceOutcome

    material = _material()

    class OwnershipTransferredStore(MemoryRunStore):
        durable_lifecycle = True

        async def bind_assembly_evidence(self, run_id, **_kwargs):
            row = self._runs[run_id]
            row["owner_worker_id"] = "worker-new-owner"
            return BindAssemblyEvidenceOutcome.ownership_lost

    store = OwnershipTransferredStore()
    manager = RunManager(store=store, worker_id="worker-old-owner")
    record = await manager.create_or_reject(
        "thread-worker-transfer",
        accepted_invocation=_accepted(material),
    )
    astream_calls = 0

    class Agent:
        async def astream(self, *_args, **_kwargs):
            nonlocal astream_calls
            astream_calls += 1
            yield {"messages": []}

    await run_agent(
        _bridge(),
        manager,
        record,
        ctx=RunContext(checkpointer=None),
        agent_factory=lambda **_kwargs: LeadAgentAssembly(
            graph=Agent(),
            descriptor=_durable_test_descriptor(material),
        ),
        graph_input={},
        config={},
    )

    row = await store.get(record.run_id)
    assert astream_calls == 0
    assert record.ownership_lost is True
    assert row is not None
    assert row["owner_worker_id"] == "worker-new-owner"
    assert row["status"] == "running"
    assert row.get("stop_reason") is None


@pytest.mark.asyncio
@pytest.mark.parametrize("store_behavior", ["malformed", "raises"])
async def test_unexpected_evidence_bind_failure_stops_before_model_work(
    store_behavior: str,
) -> None:
    from deerflow.agents.lead_agent.agent import LeadAgentAssembly

    material = _material()

    class UnexpectedBindStore(MemoryRunStore):
        durable_lifecycle = True

        async def bind_assembly_evidence(self, _run_id, **_kwargs):
            if store_behavior == "raises":
                raise RuntimeError("private persistence failure")
            return object()

    store = UnexpectedBindStore()
    manager = RunManager(store=store, worker_id="worker-bind-failure")
    record = await manager.create_or_reject(
        f"thread-bind-{store_behavior}",
        accepted_invocation=_accepted(material),
    )
    bridge = _bridge()
    astream_calls = 0

    class Agent:
        async def astream(self, *_args, **_kwargs):
            nonlocal astream_calls
            astream_calls += 1
            yield {"messages": []}

    await run_agent(
        bridge,
        manager,
        record,
        ctx=RunContext(checkpointer=None),
        agent_factory=lambda **_kwargs: LeadAgentAssembly(
            graph=Agent(),
            descriptor=_durable_test_descriptor(material),
        ),
        graph_input={},
        config={},
    )

    row = await store.get(record.run_id)
    assert astream_calls == 0
    assert record.ownership_lost is False
    assert row is not None
    assert row["status"] == "error"
    assert row["assembly_evidence_json"] is None
    assert row["stop_reason"] == "agent_assembly_drift"
    assert bridge.publish.await_count >= 1
    assert bridge.publish.await_args.args[1:] == (
        "error",
        {
            "message": "Agent assembly does not match the accepted durable execution",
            "name": "AssemblyEvidenceError",
        },
    )
    assert "private persistence failure" not in str(bridge.publish.await_args_list)


@pytest.mark.asyncio
async def test_rollback_cancellation_before_evidence_bind_does_not_access_checkpoint() -> None:
    material = _material()
    store = MemoryRunStore()
    manager = RunManager(store=store, worker_id="worker-prebind-cancel")
    record = await manager.create_or_reject(
        "thread-prebind-cancel",
        accepted_invocation=_accepted(material),
    )
    record.abort_action = "rollback"
    checkpointer = SimpleNamespace(
        aget_tuple=AsyncMock(return_value=None),
        adelete_thread=AsyncMock(),
    )

    def cancelled_factory(**_kwargs):
        raise asyncio.CancelledError

    await run_agent(
        _bridge(),
        manager,
        record,
        ctx=RunContext(checkpointer=checkpointer),
        agent_factory=cancelled_factory,
        graph_input={},
        config={},
    )

    row = await store.get(record.run_id)
    assert row is not None
    assert row["status"] == "error"
    assert row["assembly_evidence_json"] is None
    checkpointer.aget_tuple.assert_not_awaited()
    checkpointer.adelete_thread.assert_not_awaited()


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
    store._runs[record.run_id]["agent_revision_json"]["version"] = 99

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
    assert persisted["status"] == "error"
    assert persisted["error"] == "Agent assembly evidence is unavailable"
    assert persisted["stop_reason"] == "assembly_evidence_unavailable"


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
