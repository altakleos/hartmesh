"""Trusted contributor context survives admission without leaking secret values."""

from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from deerflow_extension_api import (
    ORIGIN_CONTRIBUTOR_CAPABILITY_API_VERSION,
    ORIGIN_CONTRIBUTOR_KIND,
    ActingServiceV1,
    EffectiveSubjectV1,
    InvocationIdentityV1,
    NamespacedContextReferenceV1,
    OriginContributionRequestV1,
    OriginContributionV1,
    OriginContributorFactory,
    ResolvedAgentRevisionReferenceV1,
    ResolvedProfileRevisionReferenceV1,
    SafeContextReferenceV1,
    SealedOriginV1,
    TrustedRunContextV1,
)
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.runtime.idempotency import (
    REQUEST_DIGEST_VERSION,
    canonical_request_digest,
)
from app.runtime.invocation import InternalLaunchIntent
from deerflow.extensions.contributors import ContributorHost, ContributorIndeterminateError
from deerflow.extensions.registry import ExtensionRegistry
from deerflow.persistence.base import Base
from deerflow.persistence.run.model import RunRow
from deerflow.persistence.run.sql import RunRepository
from deerflow.runtime.accepted_invocation import (
    TRUSTED_RUN_CONTEXT_KEY,
    AcceptedInvocation,
    InvocationOrigin,
    PrincipalProjection,
    ResolvedAgentMaterialV1,
    ResolvedAgentRevision,
    canonical_digest,
)
from deerflow.runtime.runs.manager import RunManager
from deerflow.runtime.runs.schemas import RunStatus
from deerflow.runtime.runs.store.memory import MemoryRunStore
from deerflow.runtime.runs.worker import RunContext, run_agent


def _trusted_context(
    *,
    origin_references: dict[str, object] | None = None,
) -> TrustedRunContextV1:
    identity = InvocationIdentityV1(
        effective_subject=EffectiveSubjectV1(kind="human", subject_id="user-1", role="member"),
        acting_service=ActingServiceV1(service_id="channel:telegram"),
    )
    persisted = NamespacedContextReferenceV1(
        capability_id="run_context_contributor:tenant",
        namespace="tenant",
        reference=SafeContextReferenceV1(
            key="region",
            value="us-central",
            storage_class="persistable",
            purpose="execution",
        ),
    )
    ephemeral = NamespacedContextReferenceV1(
        capability_id="run_context_contributor:tenant",
        namespace="tenant",
        reference=SafeContextReferenceV1(
            key="route",
            value="runtime-route",
            storage_class="runtime_only",
            purpose="execution",
        ),
    )
    handle = NamespacedContextReferenceV1(
        capability_id="run_context_contributor:tenant",
        namespace="tenant",
        reference=SafeContextReferenceV1(
            key="credential",
            value="vault://tenant/api",
            storage_class="persistable",
            purpose="secret_handle",
        ),
    )
    safe_origin_references = tuple(
        SafeContextReferenceV1(
            key=key,
            value=value,
            storage_class="persistable",
            purpose="correlation",
        )
        for key, value in sorted((origin_references or {}).items())
    )
    origin_digest = canonical_digest(
        {
            "version": 1,
            "source_kind": "native_channel",
            "references": [
                {
                    "key": reference.key,
                    "value": reference.value,
                    "storage_class": reference.storage_class,
                    "purpose": reference.purpose,
                }
                for reference in safe_origin_references
            ],
            "contributor_references": [],
        }
    )
    return TrustedRunContextV1(
        identity=identity,
        origin=SealedOriginV1(
            source_kind="native_channel",
            references=safe_origin_references,
            digest=origin_digest,
        ),
        thread_id="thread-1",
        external_key_reference="raw:event-1",
        agent_revision=ResolvedAgentRevisionReferenceV1(agent_id="lead-agent", digest="b" * 64),
        profile_revision=ResolvedProfileRevisionReferenceV1(profile_id="default", digest="c" * 64),
        extension_generation=7,
        extension_manifest_digest="d" * 64,
        persistable_references=(persisted,),
        runtime_only_references=(ephemeral,),
        secret_handles=(handle,),
    )


def test_trusted_run_context_is_immutable_and_persists_no_runtime_value() -> None:
    trusted = _trusted_context()

    with pytest.raises((AttributeError, TypeError)):
        trusted.runtime_only_references[0].reference.value = "forged"  # type: ignore[misc]

    persisted = trusted.to_persisted_json()
    rendered = repr(persisted)
    assert "us-central" in rendered
    assert "vault://tenant/api" in rendered
    assert "runtime-route" not in rendered

    recovered = TrustedRunContextV1.from_persisted_json(persisted)
    assert recovered.persistable_references == trusted.persistable_references
    assert recovered.secret_handles == trusted.secret_handles
    assert recovered.runtime_only_references == ()
    assert recovered.runtime_state_complete is False
    assert recovered.digest == trusted.digest


def test_mcp_facts_alias_is_removed_by_context_redaction() -> None:
    from deerflow_extension_api import PrincipalProjectionV1

    from deerflow.extensions.mcp import MCP_INVOCATION_FACTS_CONTEXT_KEY, McpInvocationFacts
    from deerflow.runtime.secret_context import redact_secret_context_keys

    trusted = _trusted_context().bind_run("run-1")
    facts = McpInvocationFacts(
        principal=PrincipalProjectionV1(identity=trusted.identity),
        origin=trusted.origin,
        thread_id=trusted.thread_id,
        run_id="run-1",
        agent_revision=trusted.agent_revision,
        extension_generation=trusted.extension_generation,
        trusted_context=trusted,
    )

    redacted = redact_secret_context_keys(
        {
            TRUSTED_RUN_CONTEXT_KEY: trusted,
            "__deerflow_invocation_origin": trusted.origin,
            MCP_INVOCATION_FACTS_CONTEXT_KEY: facts,
            "ordinary": "visible",
        }
    )
    assert redacted == {"ordinary": "visible"}
    assert "runtime-route" not in repr(redacted)


def test_correlation_is_evidence_bound_without_changing_execution_identity() -> None:
    trusted = replace(
        _trusted_context(),
        persistable_references=(
            NamespacedContextReferenceV1(
                capability_id="run_context_contributor:audit",
                namespace="audit",
                reference=SafeContextReferenceV1(
                    key="trace",
                    value="trace-1",
                    storage_class="persistable",
                    purpose="correlation",
                ),
            ),
        ),
    )
    changed = replace(
        trusted,
        persistable_references=(
            replace(
                trusted.persistable_references[0],
                reference=replace(trusted.persistable_references[0].reference, value="trace-2"),
            ),
        ),
    )

    assert trusted.digest != changed.digest
    assert trusted.execution_digest == changed.execution_digest

    def seal(context: TrustedRunContextV1) -> AcceptedInvocation:
        return AcceptedInvocation.seal(
            principal=PrincipalProjection(identity=context.identity),
            origin=InvocationOrigin(source_kind=context.origin.source_kind),
            thread_id=context.thread_id,
            context_references={},
            agent_revision=ResolvedAgentRevision(
                agent_id=context.agent_revision.agent_id,
                digest=context.agent_revision.digest,
                storage_source="builtin",
                storage_version="v1",
            ),
            normalized_input={"messages": []},
            execution_options={},
            extension_generation=context.extension_generation,
            extension_manifest_digest=context.extension_manifest_digest,
            contributor_execution_digest="f" * 64,
            trusted_context=context,
        )

    accepted = seal(trusted)
    accepted_changed = seal(changed)
    assert accepted.accepted_context_digest == accepted_changed.accepted_context_digest
    assert accepted.to_persisted()["decision_evidence_json"]["trusted_run_context"]["evidence_digest"] != accepted_changed.to_persisted()["decision_evidence_json"]["trusted_run_context"]["evidence_digest"]


def test_accepted_invocation_persists_trusted_safe_evidence_and_recovers_fail_closed() -> None:
    trusted = _trusted_context()
    accepted = AcceptedInvocation.seal(
        principal=PrincipalProjection(identity=trusted.identity),
        origin=InvocationOrigin(source_kind="native_channel"),
        thread_id=trusted.thread_id,
        context_references={},
        agent_revision=ResolvedAgentRevision(
            agent_id=trusted.agent_revision.agent_id,
            digest=trusted.agent_revision.digest,
            storage_source="file",
            storage_version="v1",
        ),
        normalized_input={"messages": []},
        execution_options={},
        extension_generation=trusted.extension_generation,
        extension_manifest_digest=trusted.extension_manifest_digest,
        contributor_execution_digest=canonical_digest({"version": 1, "execution": []}),
        trusted_context=trusted,
    )

    persisted = accepted.to_persisted()
    assert persisted["decision_evidence_json"]["trusted_run_context"]["persistable_references"]
    assert "runtime-route" not in repr(persisted)

    recovered = AcceptedInvocation.from_persisted({"thread_id": trusted.thread_id, **persisted})
    assert recovered is not None
    assert recovered.trusted_context is not None
    assert recovered.trusted_context.runtime_state_complete is False
    assert recovered.accepted_context_digest == accepted.accepted_context_digest

    tampered = accepted.to_persisted()
    tampered["decision_evidence_json"]["trusted_run_context"]["persistable_references"][0]["reference"]["value"] = "forged-region"
    with pytest.raises(ValueError, match="evidence digest"):
        AcceptedInvocation.from_persisted({"thread_id": trusted.thread_id, **tampered})


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (
            lambda row: row.__setitem__("principal_projection_digest", "0" * 64),
            "principal projection digest",
        ),
        (
            lambda row: row["origin_json"]["references"].__setitem__("provider", "forged"),
            "base Origin digest",
        ),
        (
            lambda row: row["agent_revision_json"].__setitem__("digest", "0" * 64),
            "agent revision digest",
        ),
        (
            lambda row: row.__setitem__("extension_generation", 8),
            "generation",
        ),
        (
            lambda row: row["decision_evidence_json"]["capability_manifest"].__setitem__("digest", "0" * 64),
            "extension manifest",
        ),
    ],
)
def test_accepted_invocation_hydration_rejects_contradictory_evidence(
    mutate,
    message: str,
) -> None:
    trusted = _trusted_context(origin_references={"provider": "telegram"})
    accepted = AcceptedInvocation.seal(
        principal=PrincipalProjection(identity=trusted.identity),
        origin=InvocationOrigin(
            source_kind="native_channel",
            references={"provider": "telegram"},
        ),
        thread_id=trusted.thread_id,
        context_references={},
        agent_revision=ResolvedAgentRevision(
            agent_id=trusted.agent_revision.agent_id,
            digest=trusted.agent_revision.digest,
            storage_source="file",
            storage_version="v1",
        ),
        normalized_input={"messages": []},
        execution_options={},
        extension_generation=trusted.extension_generation,
        extension_manifest_digest=trusted.extension_manifest_digest,
        contributor_execution_digest=canonical_digest({"version": 1, "execution": []}),
        trusted_context=trusted,
    )
    persisted = {"thread_id": trusted.thread_id, **accepted.to_persisted()}
    mutate(persisted)

    with pytest.raises(ValueError, match=message):
        AcceptedInvocation.from_persisted(copy.deepcopy(persisted))


def _accepted_row_with_effective_projection() -> dict[str, object]:
    trusted = _trusted_context(origin_references={"provider": "telegram"})
    execution_options = {
        "multitask_strategy": "reject",
        "interrupt_before": None,
        "interrupt_after": None,
        "checkpoint_id": None,
        "recursion_limit": 100,
    }
    accepted = AcceptedInvocation.seal(
        principal=PrincipalProjection(identity=trusted.identity),
        origin=InvocationOrigin(
            source_kind="native_channel",
            references={"provider": "telegram"},
        ),
        thread_id=trusted.thread_id,
        context_references={"max_total_subagents": 2},
        agent_revision=ResolvedAgentRevision(
            agent_id=trusted.agent_revision.agent_id,
            digest=trusted.agent_revision.digest,
            storage_source="file",
            storage_version="v1",
        ),
        normalized_input={"messages": []},
        execution_options=execution_options,
        extension_generation=trusted.extension_generation,
        extension_manifest_digest=trusted.extension_manifest_digest,
        contributor_execution_digest=canonical_digest({"version": 1, "execution": []}),
        trusted_context=trusted,
    )
    effective = {
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
        "input": {"messages": []},
        "command": None,
        "multitask_strategy": "reject",
        "checkpoint": {},
        "interrupt_before": None,
        "interrupt_after": None,
        "execution_context": {"max_total_subagents": 2},
        "recursion_limit": 100,
    }
    return {
        "run_id": "run-1",
        "thread_id": trusted.thread_id,
        "external_key": trusted.external_key_reference,
        "request_digest": canonical_request_digest(effective),
        "request_digest_version": REQUEST_DIGEST_VERSION,
        "kwargs": {"__accepted_request_projection_v1": effective},
        **accepted.to_persisted(),
    }


def _forge_self_consistent_trusted_origin_digest(row: dict[str, object]) -> None:
    trusted = row["decision_evidence_json"]["trusted_run_context"]
    trusted["origin"]["digest"] = "0" * 64
    projection = copy.deepcopy(trusted)
    projection.pop("evidence_digest")
    trusted["evidence_digest"] = hashlib.sha256(
        json.dumps(
            projection,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()


def _mutate_effective_projection(
    row: dict[str, object],
    field_name: str,
    value: object,
) -> None:
    projection = row["kwargs"]["__accepted_request_projection_v1"]
    projection[field_name] = value
    row["request_digest"] = canonical_request_digest(projection)


def _forge_self_consistent_accepted_context_digest(row: dict[str, object]) -> None:
    row["accepted_context_digest"] = "0" * 64
    _mutate_effective_projection(row, "accepted_context_digest", "0" * 64)


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (
            lambda row: row.__setitem__("request_digest", "0" * 64),
            "effective execution digest",
        ),
        (
            lambda row: _mutate_effective_projection(row, "runtime_identity_digest", "0" * 64),
            "runtime identity digest",
        ),
        (
            _forge_self_consistent_accepted_context_digest,
            "accepted context digest",
        ),
        (
            _forge_self_consistent_trusted_origin_digest,
            "trusted Origin digest",
        ),
    ],
)
def test_hydration_recomputes_bound_effective_and_trusted_evidence(
    mutate,
    message: str,
) -> None:
    row = _accepted_row_with_effective_projection()
    mutate(row)

    with pytest.raises(ValueError, match=message):
        AcceptedInvocation.from_persisted(copy.deepcopy(row))


def _worker_accepted(*, persisted_recovery: bool = False) -> tuple[AcceptedInvocation, ResolvedAgentMaterialV1]:
    material = ResolvedAgentMaterialV1(
        agent_id="lead-agent",
        storage_source="builtin",
        storage_version="v1",
        agent_config=None,
        soul="steady",
        model_profile={"name": "default", "model": "test"},
    )
    revision = ResolvedAgentRevision.from_material(material)
    trusted = replace(
        _trusted_context(),
        agent_revision=ResolvedAgentRevisionReferenceV1(
            agent_id=revision.agent_id,
            digest=revision.digest,
        ),
        profile_revision=ResolvedProfileRevisionReferenceV1(
            profile_id="default",
            digest=canonical_digest({"version": 1, "model_profile": material.model_profile}),
        ),
    )
    accepted = AcceptedInvocation.seal(
        principal=PrincipalProjection(identity=trusted.identity),
        origin=InvocationOrigin(source_kind=trusted.origin.source_kind),
        thread_id=trusted.thread_id,
        context_references={},
        agent_revision=revision,
        normalized_input={"messages": []},
        execution_options={},
        extension_generation=trusted.extension_generation,
        extension_manifest_digest=trusted.extension_manifest_digest,
        contributor_execution_digest=canonical_digest({"version": 1, "execution": []}),
        trusted_context=trusted,
    )
    if persisted_recovery:
        recovered = AcceptedInvocation.from_persisted({"thread_id": trusted.thread_id, **accepted.to_persisted()})
        assert recovered is not None
        accepted = recovered
    return accepted, material


def _bridge() -> SimpleNamespace:
    return SimpleNamespace(publish=AsyncMock(), publish_end=AsyncMock(), cleanup=AsyncMock())


@pytest.mark.asyncio
async def test_worker_carries_one_trusted_context_without_parallel_attributes_path() -> None:
    accepted, _material = _worker_accepted()
    manager = RunManager()
    record = await manager.create_or_reject(accepted.thread_id, accepted_invocation=accepted)
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
        config={"context": {"authz_attributes": {"forged": True}}},
    )

    trusted = seen[TRUSTED_RUN_CONTEXT_KEY]
    assert isinstance(trusted, TrustedRunContextV1)
    assert trusted.run_id == record.run_id
    assert trusted.runtime_only_references[0].reference.value == "runtime-route"
    assert "authz_attributes" not in seen
    assert record.status is RunStatus.success


@pytest.mark.asyncio
async def test_process_recovery_without_ephemeral_context_fails_before_graph_start() -> None:
    accepted, material = _worker_accepted(persisted_recovery=True)
    manager = RunManager()
    record = await manager.create_or_reject(accepted.thread_id, accepted_invocation=accepted)
    factory_called = False

    def factory(*, config):
        nonlocal factory_called
        del config
        factory_called = True
        raise AssertionError("graph construction must not run")

    await run_agent(
        _bridge(),
        manager,
        record,
        ctx=RunContext(
            checkpointer=None,
            agent_revision_resolver=lambda _record, _config: ResolvedAgentRevision.from_material(material),
        ),
        agent_factory=factory,
        graph_input={},
        config={},
    )

    assert factory_called is False
    assert record.status is RunStatus.error
    assert record.stop_reason == "trusted_context_unavailable"


@pytest.mark.asyncio
async def test_runtime_only_values_never_enter_store_lifecycle_or_public_response() -> None:
    from app.gateway.routers.thread_runs import _record_to_response

    accepted, _material = _worker_accepted()
    store = MemoryRunStore()
    manager = RunManager(store=store)

    record = await manager.create_or_reject(
        accepted.thread_id,
        user_id="user-1",
        accepted_invocation=accepted,
    )

    row = await store.get(record.run_id, user_id="user-1")
    lifecycle = await store.list_lifecycle_events(run_id=record.run_id)
    public_response = _record_to_response(record)
    assert row is not None
    assert "us-central" in repr(row)
    assert "vault://tenant/api" in repr(row)
    assert "runtime-route" not in repr(row)
    assert "runtime-route" not in repr(lifecycle)
    assert "vault://tenant/api" not in repr(lifecycle)
    assert "runtime-route" not in repr(public_response)
    assert "vault://tenant/api" not in repr(public_response)


@pytest.mark.asyncio
async def test_runtime_only_values_never_enter_sql_store_or_lifecycle(tmp_path) -> None:
    accepted, _material = _worker_accepted()
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'trusted-context.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    store = RunRepository(session_factory)
    manager = RunManager(store=store)
    try:
        record = await manager.create_or_reject(
            accepted.thread_id,
            user_id="user-1",
            accepted_invocation=accepted,
        )

        row = await store.get(record.run_id, user_id="user-1")
        lifecycle = await store.list_lifecycle_events(run_id=record.run_id)
        async with session_factory() as session:
            stored_evidence = (await session.execute(select(RunRow.decision_evidence_json).where(RunRow.run_id == record.run_id))).scalar_one()
        assert row is not None
        assert "us-central" in repr(row)
        assert "vault://tenant/api" in repr(row)
        assert "runtime-route" not in repr(row)
        assert "us-central" in repr(stored_evidence)
        assert "vault://tenant/api" in repr(stored_evidence)
        assert "runtime-route" not in repr(stored_evidence)
        assert "runtime-route" not in repr(lifecycle)
        assert "vault://tenant/api" not in repr(lifecycle)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_composition_returns_persistable_runtime_only_and_stable_handle_products() -> None:
    class _Context:
        async def contribute(self, _request):
            return OriginContributionV1(
                namespace="tenant",
                references=(
                    SafeContextReferenceV1(
                        key="region",
                        value="us-central",
                        storage_class="persistable",
                        purpose="execution",
                    ),
                    SafeContextReferenceV1(
                        key="routing_hint",
                        value="ephemeral-route",
                        storage_class="runtime_only",
                        purpose="execution",
                    ),
                    SafeContextReferenceV1(
                        key="credential",
                        value="vault://tenant/api",
                        storage_class="runtime_only",
                        purpose="secret_handle",
                    ),
                ),
            )

    registry = ExtensionRegistry()
    with registry.attributed_to("tenant:install"):
        registry.origin_contributor(
            OriginContributorFactory(
                contribution_id="tenant-context",
                capability_api_version=ORIGIN_CONTRIBUTOR_CAPABILITY_API_VERSION,
                factory=_Context,
                kind=ORIGIN_CONTRIBUTOR_KIND,
            )
        )

    composed = await ContributorHost(registry.build()).contribute_origin(OriginContributionRequestV1(source_kind="http"))

    assert [item.reference.key for item in composed.persistable] == ["region"]
    assert [item.reference.key for item in composed.runtime_only] == ["routing_hint"]
    assert [item.reference.key for item in composed.secret_handles] == ["credential"]


@pytest.mark.asyncio
async def test_host_rejects_runtime_only_correlation_with_no_approved_consumer() -> None:
    class _Unsupported:
        async def contribute(self, _request):
            return OriginContributionV1(
                namespace="tenant",
                references=(
                    SafeContextReferenceV1(
                        key="unused",
                        value="must-not-float-through-runtime",
                        storage_class="runtime_only",
                        purpose="correlation",
                    ),
                ),
            )

    registry = ExtensionRegistry()
    with registry.attributed_to("tenant:install"):
        registry.origin_contributor(
            OriginContributorFactory(
                contribution_id="tenant-context",
                capability_api_version=ORIGIN_CONTRIBUTOR_CAPABILITY_API_VERSION,
                factory=_Unsupported,
                kind=ORIGIN_CONTRIBUTOR_KIND,
            )
        )

    with pytest.raises(ContributorIndeterminateError, match="unsupported_reference_policy"):
        await ContributorHost(registry.build()).contribute_origin(OriginContributionRequestV1(source_kind="http"))


@pytest.mark.asyncio
async def test_aggregate_reference_limit_fails_closed_across_valid_contributors() -> None:
    class _Many:
        def __init__(self, namespace: str) -> None:
            self.namespace = namespace

        async def contribute(self, _request):
            return OriginContributionV1(
                namespace=self.namespace,
                references=tuple(
                    SafeContextReferenceV1(
                        key=f"value_{index}",
                        value=index,
                        storage_class="persistable",
                        purpose="execution",
                    )
                    for index in range(20)
                ),
            )

    registry = ExtensionRegistry()
    for contribution_id in ("alpha", "beta"):
        with registry.attributed_to(f"{contribution_id}:install"):
            registry.origin_contributor(
                OriginContributorFactory(
                    contribution_id=contribution_id,
                    capability_api_version=ORIGIN_CONTRIBUTOR_CAPABILITY_API_VERSION,
                    factory=lambda contribution_id=contribution_id: _Many(contribution_id),
                    kind=ORIGIN_CONTRIBUTOR_KIND,
                )
            )

    with pytest.raises(ContributorIndeterminateError, match="aggregate_reference_limit"):
        await ContributorHost(registry.build()).contribute_origin(OriginContributionRequestV1(source_kind="http"))


@pytest.mark.asyncio
async def test_duplicate_fully_qualified_reference_key_fails_deterministically() -> None:
    class _Duplicate:
        async def contribute(self, _request):
            return OriginContributionV1(
                namespace="shared",
                references=(
                    SafeContextReferenceV1(
                        key="tenant",
                        value="one",
                        storage_class="persistable",
                        purpose="execution",
                    ),
                ),
            )

    registry = ExtensionRegistry()
    for contribution_id in ("alpha", "beta"):
        with registry.attributed_to(f"{contribution_id}:install"):
            registry.origin_contributor(
                OriginContributorFactory(
                    contribution_id=contribution_id,
                    capability_api_version=ORIGIN_CONTRIBUTOR_CAPABILITY_API_VERSION,
                    factory=_Duplicate,
                    kind=ORIGIN_CONTRIBUTOR_KIND,
                )
            )

    with pytest.raises(ContributorIndeterminateError, match="duplicate_fully_qualified_key"):
        await ContributorHost(registry.build()).contribute_origin(OriginContributionRequestV1(source_kind="http"))


@pytest.mark.asyncio
async def test_malicious_optional_contributor_error_is_reduced_to_safe_diagnostic() -> None:
    class _Malicious:
        async def contribute(self, _request):
            raise RuntimeError("credential=resolved-secret-value")

    registry = ExtensionRegistry()
    with registry.attributed_to("tenant:install"):
        registry.origin_contributor(
            OriginContributorFactory(
                contribution_id="tenant-context",
                capability_api_version=ORIGIN_CONTRIBUTOR_CAPABILITY_API_VERSION,
                factory=_Malicious,
                kind=ORIGIN_CONTRIBUTOR_KIND,
            )
        )

    composed = await ContributorHost(registry.build()).contribute_origin(OriginContributionRequestV1(source_kind="http"))

    diagnostic = composed.diagnostics[0]
    assert diagnostic.capability_id == "origin_contributor:tenant-context"
    assert diagnostic.contribution_id == "tenant-context"
    assert diagnostic.diagnostic_code == "contribution_failed"
    assert diagnostic.error_class == "RuntimeError"
    assert len(diagnostic.correlation_id) == 32
    assert "resolved-secret-value" not in repr(diagnostic)


@pytest.mark.asyncio
async def test_gateway_logs_only_redacted_contributor_diagnostic(
    monkeypatch,
    caplog,
) -> None:
    from app.gateway import services

    class _Malicious:
        async def contribute(self, _request):
            raise RuntimeError("credential=resolved-secret-value")

    registry = ExtensionRegistry()
    with registry.attributed_to("tenant:install"):
        registry.origin_contributor(
            OriginContributorFactory(
                contribution_id="tenant-context",
                capability_api_version=ORIGIN_CONTRIBUTOR_CAPABILITY_API_VERSION,
                factory=_Malicious,
                kind=ORIGIN_CONTRIBUTOR_KIND,
            )
        )
    accepted, _material = _worker_accepted()
    monkeypatch.setattr(services, "resolve_agent_revision", lambda *_args, **_kwargs: accepted.agent_revision)
    request = SimpleNamespace(
        state=SimpleNamespace(user=SimpleNamespace(id="user-1", system_role="member")),
        app=SimpleNamespace(
            state=SimpleNamespace(
                extensions=SimpleNamespace(generation=7),
                capability_manifest=SimpleNamespace(digest="d" * 64),
                contributor_host=ContributorHost(registry.build()),
            )
        ),
    )

    with caplog.at_level("WARNING", logger="app.gateway.services"):
        await services._seal_accepted_invocation(
            request=request,
            intent=InternalLaunchIntent(thread_id="thread-1"),
            config={"context": {}},
            graph_input={"messages": []},
            owner_user_id=None,
            run_ctx=SimpleNamespace(app_config=object()),
        )

    rendered = caplog.text
    assert "tenant-context" in rendered
    assert "diagnostic_code=contribution_failed" in rendered
    assert "error_class=RuntimeError" in rendered
    assert "correlation_id=" in rendered
    assert "resolved-secret-value" not in rendered
