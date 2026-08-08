"""Version-two restrictive invocation constraints and enforcement fences."""

from __future__ import annotations

import dataclasses
from contextlib import asynccontextmanager, contextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from deerflow_extension_api import (
    INVOCATION_CONSTRAINTS_CAPABILITY_API_VERSION_V2,
    INVOCATION_CONSTRAINTS_KIND,
    INVOCATION_CONSTRAINTS_REQUIRED_CAPABILITY_V2,
    INVOCATION_CONSTRAINTS_V2_SUPPORTED_OBLIGATIONS,
    ActingServiceV1,
    CapabilityHealthResult,
    ConstraintIndeterminate,
    ConstraintProjectionRequestV2,
    ConstraintProjectionV2,
    EffectiveSubjectV1,
    InvocationConstraintsProviderFactory,
    InvocationIdentityV1,
    NamespacedContextReferenceV1,
    ResolvedAgentRevisionReferenceV1,
    ResolvedProfileRevisionReferenceV1,
    SafeContextReferenceV1,
    SealedOriginV1,
    TrustedRunContextV1,
)

from app.runtime.constraints import ProviderInvocationConstraints
from app.runtime.invocation import (
    DurableAdmission,
    InternalAdmissionIdentity,
    InternalAuthorizationDecision,
    InternalConstraintDecision,
    InternalLaunchIntent,
    InvocationPrincipal,
    InvocationRuntime,
    PreparedLaunch,
)
from deerflow.extensions.capabilities import CapabilityHealthMonitor, build_capability_manifest
from deerflow.extensions.constraints import ConstraintStartupError, InvocationConstraintsHost
from deerflow.extensions.registry import ExtensionRegistry
from deerflow.runtime import DisconnectMode, RunRecord, RunStatus
from deerflow.runtime.accepted_invocation import (
    AcceptedInvocation,
    InvocationOrigin,
    PrincipalProjection,
    ResolvedAgentMaterialV1,
    ResolvedAgentRevision,
    canonical_digest,
)
from deerflow.runtime.constraints import (
    INVOCATION_CONSTRAINTS_CONTEXT_KEY,
    SUBAGENT_RESERVATION_CONTEXT_KEY,
)
from deerflow.runtime.runs.manager import RunManager
from deerflow.runtime.runs.store.base import AdmissionOutcome
from deerflow.runtime.runs.worker import RunContext, run_agent

_DIGESTS = tuple(character * 64 for character in "abcdef")


def _request_v2(**overrides: object) -> ConstraintProjectionRequestV2:
    values = {
        "identity": InvocationIdentityV1(
            effective_subject=EffectiveSubjectV1(kind="human", subject_id="owner-1", role="member"),
            acting_service=ActingServiceV1(service_id="channel-worker"),
        ),
        "origin": SealedOriginV1(
            source_kind="native_channel",
            references=(
                SafeContextReferenceV1(
                    key="provider",
                    value="telegram",
                    storage_class="persistable",
                    purpose="correlation",
                ),
            ),
            digest=_DIGESTS[0],
        ),
        "policy_lookup_references": (
            NamespacedContextReferenceV1(
                capability_id="run_context_contributor:tenant",
                namespace="tenant",
                reference=SafeContextReferenceV1(
                    key="region",
                    value="us-east",
                    storage_class="persistable",
                    purpose="correlation",
                ),
            ),
        ),
        "thread_id": "thread-1",
        "external_key_reference": "raw:event-1",
        "agent_revision": ResolvedAgentRevisionReferenceV1(
            agent_id="default",
            digest=_DIGESTS[1],
        ),
        "profile_revision": ResolvedProfileRevisionReferenceV1(
            profile_id="default",
            digest=_DIGESTS[2],
        ),
        "request_digest": _DIGESTS[3],
        "trusted_context_digest": _DIGESTS[4],
        "extension_manifest_digest": _DIGESTS[5],
        "extension_generation": 7,
        "host_max_total_subagents": 3,
    }
    values.update(overrides)
    return ConstraintProjectionRequestV2(**values)


def test_v2_contract_is_explicit_and_does_not_widen_v1() -> None:
    assert INVOCATION_CONSTRAINTS_CAPABILITY_API_VERSION_V2 == "2.0"
    assert INVOCATION_CONSTRAINTS_REQUIRED_CAPABILITY_V2 == "invocation_constraints.v2"
    assert INVOCATION_CONSTRAINTS_V2_SUPPORTED_OBLIGATIONS == frozenset({"max_total_subagents"})
    assert {field.name for field in dataclasses.fields(ConstraintProjectionRequestV2)} == {
        "identity",
        "origin",
        "policy_lookup_references",
        "thread_id",
        "external_key_reference",
        "agent_revision",
        "profile_revision",
        "request_digest",
        "trusted_context_digest",
        "extension_manifest_digest",
        "extension_generation",
        "host_max_total_subagents",
    }
    assert {field.name for field in dataclasses.fields(ConstraintProjectionV2)} == {
        "request_digest",
        "trusted_context_digest",
        "thread_id",
        "agent_revision_digest",
        "profile_revision_digest",
        "extension_manifest_digest",
        "extension_generation",
        "projection_revision",
        "issued_at",
        "valid_until",
        "evidence_id",
        "evidence_digest",
        "mandatory_obligations",
        "max_total_subagents",
    }


def test_v2_projection_allows_zero_subagents_as_an_enforceable_obligation() -> None:
    now = datetime(2026, 8, 8, 12, tzinfo=UTC)
    projection = ConstraintProjectionV2(
        request_digest=_DIGESTS[0],
        trusted_context_digest=_DIGESTS[1],
        thread_id="thread-1",
        agent_revision_digest=_DIGESTS[2],
        profile_revision_digest=_DIGESTS[3],
        extension_manifest_digest=_DIGESTS[4],
        extension_generation=7,
        projection_revision="policy-8",
        issued_at=now,
        valid_until=now + timedelta(minutes=5),
        evidence_id="evidence-8",
        evidence_digest=_DIGESTS[5],
        mandatory_obligations=("max_total_subagents",),
        max_total_subagents=0,
    )

    assert projection.max_total_subagents == 0
    assert projection.mandatory_obligations == ("max_total_subagents",)

    with pytest.raises(ValueError, match="at most 16"):
        dataclasses.replace(
            projection,
            mandatory_obligations=tuple(f"future_limit_{index}" for index in range(17)),
            max_total_subagents=None,
        )


def test_v2_policy_lookup_references_are_bounded_and_correlation_only() -> None:
    base = _request_v2()
    references = tuple(
        NamespacedContextReferenceV1(
            capability_id=f"run_context_contributor:lookup-{index}",
            namespace=f"lookup_{index}",
            reference=SafeContextReferenceV1(
                key="value",
                value=index,
                storage_class="persistable",
                purpose="correlation",
            ),
        )
        for index in range(33)
    )
    with pytest.raises(ValueError, match="at most 32"):
        dataclasses.replace(base, policy_lookup_references=references)

    execution_reference = dataclasses.replace(
        base.policy_lookup_references[0].reference,
        purpose="execution",
    )
    with pytest.raises(ValueError, match="correlation"):
        dataclasses.replace(
            base,
            policy_lookup_references=(
                dataclasses.replace(
                    base.policy_lookup_references[0],
                    reference=execution_reference,
                ),
            ),
        )


def _v2_projection(request: ConstraintProjectionRequestV2, **overrides: object) -> ConstraintProjectionV2:
    now = datetime(2026, 8, 8, 12, tzinfo=UTC)
    values = {
        "request_digest": request.request_digest,
        "trusted_context_digest": request.trusted_context_digest,
        "thread_id": request.thread_id,
        "agent_revision_digest": request.agent_revision.digest,
        "profile_revision_digest": request.profile_revision.digest,
        "extension_manifest_digest": request.extension_manifest_digest,
        "extension_generation": request.extension_generation,
        "projection_revision": "policy-8",
        "issued_at": now,
        "valid_until": now + timedelta(minutes=5),
        "evidence_id": "evidence-8",
        "evidence_digest": _DIGESTS[0],
        "mandatory_obligations": ("max_total_subagents",),
        "max_total_subagents": 5,
    }
    values.update(overrides)
    return ConstraintProjectionV2(**values)


def _v2_extensions(provider: object):
    registry = ExtensionRegistry()
    with registry.attributed_to("constraints_v2:install"):
        registry.invocation_constraints(
            InvocationConstraintsProviderFactory(
                contribution_id="constraints-v2",
                capability_api_version=INVOCATION_CONSTRAINTS_CAPABILITY_API_VERSION_V2,
                factory=lambda: provider,
                kind=INVOCATION_CONSTRAINTS_KIND,
            )
        )
    return registry.build()


def _v2_host(provider: object) -> InvocationConstraintsHost:
    return InvocationConstraintsHost(
        _v2_extensions(provider),
        required_capabilities=(INVOCATION_CONSTRAINTS_REQUIRED_CAPABILITY_V2,),
        clock=lambda: datetime(2026, 8, 8, 12, tzinfo=UTC),
    )


@pytest.mark.asyncio
async def test_v2_host_selects_the_version_and_never_raises_the_host_ceiling() -> None:
    request = _request_v2()

    class _Provider:
        async def project(self, received: ConstraintProjectionRequestV2):
            assert received is request
            return _v2_projection(received, max_total_subagents=5)

    host = _v2_host(_Provider())
    result = await host.project(request, runtime_enforceable=True)

    assert host.initialized_capability_ids == frozenset({"invocation_constraints.v2"})
    assert isinstance(result, ConstraintProjectionV2)
    assert result.max_total_subagents == 3


@pytest.mark.asyncio
async def test_v2_host_preserves_a_zero_local_ceiling() -> None:
    request = _request_v2(host_max_total_subagents=0)

    class _Provider:
        async def project(self, received: ConstraintProjectionRequestV2):
            return _v2_projection(received, max_total_subagents=5)

    result = await _v2_host(_Provider()).project(request)

    assert isinstance(result, ConstraintProjectionV2)
    assert result.max_total_subagents == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "overrides",
    [
        {"request_digest": "0" * 64},
        {"trusted_context_digest": "1" * 64},
        {"thread_id": "other-thread"},
        {"agent_revision_digest": "2" * 64},
        {"profile_revision_digest": "3" * 64},
        {"extension_manifest_digest": "4" * 64},
        {"extension_generation": 8},
    ],
    ids=(
        "request",
        "trusted-context",
        "thread",
        "agent-revision",
        "profile-revision",
        "manifest",
        "extension-generation",
    ),
)
async def test_v2_host_rejects_projection_for_different_bound_material(
    overrides: dict[str, object],
) -> None:
    request = _request_v2()

    class _Provider:
        async def project(self, received):
            return _v2_projection(received, **overrides)

    result = await _v2_host(_Provider()).project(request)

    assert isinstance(result, ConstraintIndeterminate)


@pytest.mark.asyncio
async def test_unknown_mandatory_obligation_fails_closed() -> None:
    request = _request_v2()

    class _Provider:
        async def project(self, received):
            return _v2_projection(
                received,
                mandatory_obligations=("future_resource_limit",),
                max_total_subagents=None,
            )

    result = await _v2_host(_Provider()).project(request)

    assert isinstance(result, ConstraintIndeterminate)


@pytest.mark.asyncio
@pytest.mark.parametrize("provider_result", [ConstraintIndeterminate(), object()])
async def test_v2_indeterminate_or_malformed_result_fails_closed(provider_result: object) -> None:
    request = _request_v2()

    class _Provider:
        async def project(self, _received):
            return provider_result

    result = await _v2_host(_Provider()).project(request)

    assert isinstance(result, ConstraintIndeterminate)


@pytest.mark.asyncio
async def test_v2_expired_projection_fails_closed() -> None:
    request = _request_v2()

    class _Provider:
        async def project(self, received):
            now = datetime(2026, 8, 8, 12, tzinfo=UTC)
            return _v2_projection(
                received,
                issued_at=now - timedelta(minutes=2),
                valid_until=now,
            )

    result = await _v2_host(_Provider()).project(request)

    assert isinstance(result, ConstraintIndeterminate)


def test_v1_registration_cannot_satisfy_a_v2_operator_requirement() -> None:
    registry = ExtensionRegistry()
    with registry.attributed_to("constraints_v1:install"):
        registry.invocation_constraints(
            InvocationConstraintsProviderFactory(
                contribution_id="constraints-v1",
                capability_api_version="1.0",
                factory=lambda: object(),
                kind=INVOCATION_CONSTRAINTS_KIND,
            )
        )

    with pytest.raises(ConstraintStartupError, match="invocation_constraints.v2"):
        InvocationConstraintsHost(
            registry.build(),
            required_capabilities=(INVOCATION_CONSTRAINTS_REQUIRED_CAPABILITY_V2,),
        )


def test_required_v2_initialization_failure_stops_startup_without_raw_error_text() -> None:
    registry = ExtensionRegistry()

    def _broken_factory():
        raise RuntimeError("credential-like-private-text")

    with registry.attributed_to("constraints_v2:install"):
        registry.invocation_constraints(
            InvocationConstraintsProviderFactory(
                contribution_id="constraints-v2",
                capability_api_version=INVOCATION_CONSTRAINTS_CAPABILITY_API_VERSION_V2,
                factory=_broken_factory,
                kind=INVOCATION_CONSTRAINTS_KIND,
            )
        )

    with pytest.raises(ConstraintStartupError) as captured:
        InvocationConstraintsHost(
            registry.build(),
            required_capabilities=(INVOCATION_CONSTRAINTS_REQUIRED_CAPABILITY_V2,),
        )
    assert "RuntimeError" in str(captured.value)
    assert "credential-like-private-text" not in str(captured.value)


def test_v2_registration_has_distinct_manifest_and_required_health_identity() -> None:
    extensions = _v2_extensions(object())
    manifest = build_capability_manifest(
        extensions,
        required_capabilities=(INVOCATION_CONSTRAINTS_REQUIRED_CAPABILITY_V2,),
        initialized_capability_ids=(INVOCATION_CONSTRAINTS_REQUIRED_CAPABILITY_V2,),
    )

    entry = next(item for item in manifest.capabilities if item.capability_type == "invocation_constraints")
    assert entry.capability_id == INVOCATION_CONSTRAINTS_REQUIRED_CAPABILITY_V2
    assert entry.capability_api_version == INVOCATION_CONSTRAINTS_CAPABILITY_API_VERSION_V2
    assert entry.operator_required is True
    assert entry.initialization_status == "initialized"


@pytest.mark.asyncio
async def test_v2_required_health_uses_the_v2_capability_identity() -> None:
    async def _health() -> CapabilityHealthResult:
        return CapabilityHealthResult(status="healthy")

    registry = ExtensionRegistry()
    with registry.attributed_to("constraints_v2:install"):
        registry.invocation_constraints(
            InvocationConstraintsProviderFactory(
                contribution_id="constraints-v2",
                capability_api_version=INVOCATION_CONSTRAINTS_CAPABILITY_API_VERSION_V2,
                factory=lambda: object(),
                kind=INVOCATION_CONSTRAINTS_KIND,
                health_probe=_health,
            )
        )
    extensions = registry.build()
    manifest = build_capability_manifest(
        extensions,
        required_capabilities=(INVOCATION_CONSTRAINTS_REQUIRED_CAPABILITY_V2,),
        initialized_capability_ids=(INVOCATION_CONSTRAINTS_REQUIRED_CAPABILITY_V2,),
    )

    readiness = await CapabilityHealthMonitor(manifest, extensions).readiness()

    assert readiness.status == "ready"
    constraint_health = next(item for item in readiness.health if item.capability_id == INVOCATION_CONSTRAINTS_REQUIRED_CAPABILITY_V2)
    assert constraint_health.status == "healthy"


def _accepted_v2() -> AcceptedInvocation:
    request = _request_v2()
    material = ResolvedAgentMaterialV1(
        agent_id="default",
        storage_source="config",
        storage_version="v1",
        agent_config=None,
        soul="steady",
        model_profile={"name": "default"},
        runtime_defaults={
            "subagent_enabled": True,
            "max_concurrent_subagents": 3,
            "max_total_subagents": request.host_max_total_subagents,
        },
    )
    revision = ResolvedAgentRevision.from_material(material)
    trusted_context = TrustedRunContextV1(
        identity=request.identity,
        origin=request.origin,
        thread_id=request.thread_id,
        external_key_reference=request.external_key_reference,
        agent_revision=ResolvedAgentRevisionReferenceV1(
            agent_id=revision.agent_id,
            digest=revision.digest,
        ),
        profile_revision=request.profile_revision,
        extension_generation=request.extension_generation,
        extension_manifest_digest=request.extension_manifest_digest,
        persistable_references=request.policy_lookup_references,
    )
    return AcceptedInvocation.seal(
        principal=PrincipalProjection(
            user_id="owner-1",
            role="member",
            identity=request.identity,
        ),
        origin=InvocationOrigin(source_kind=request.origin.source_kind),
        thread_id=request.thread_id,
        context_references={"max_total_subagents": request.host_max_total_subagents},
        agent_revision=revision,
        normalized_input={"messages": [{"role": "user", "content": "hello"}]},
        execution_options={},
        extension_generation=request.extension_generation,
        extension_manifest_digest=request.extension_manifest_digest,
        contributor_execution_digest=canonical_digest({"version": 1, "execution": []}),
        trusted_context=trusted_context,
    )


@pytest.mark.asyncio
async def test_application_adapter_projects_v2_from_only_host_sealed_facts() -> None:
    accepted = _accepted_v2()
    seen: dict[str, object] = {}

    class _Host:
        capability_api_version = INVOCATION_CONSTRAINTS_CAPABILITY_API_VERSION_V2

        async def project(self, request, **kwargs):
            seen["request"] = request
            seen["kwargs"] = kwargs
            return _v2_projection(
                request,
                max_total_subagents=0,
            )

    async def _worker(_record):
        return None

    launch = PreparedLaunch(
        thread_id=accepted.thread_id,
        assistant_id="default",
        on_disconnect=DisconnectMode.cancel,
        metadata={},
        kwargs={},
        multitask_strategy="reject",
        model_name=None,
        user_id="owner-1",
        worker=_worker,
        accepted_invocation=accepted,
        request_digest=_DIGESTS[3],
        request_digest_version="sha256-canonical-json-v1",
    )

    decision = await ProviderInvocationConstraints(_Host()).project(launch)

    request = seen["request"]
    assert isinstance(request, ConstraintProjectionRequestV2)
    assert request.identity is accepted.trusted_context.identity
    assert request.origin is accepted.trusted_context.origin
    assert request.thread_id == accepted.thread_id
    assert request.agent_revision.digest == accepted.agent_revision.digest
    assert request.profile_revision == accepted.trusted_context.profile_revision
    assert request.policy_lookup_references == accepted.trusted_context.persistable_references
    assert request.trusted_context_digest == accepted.trusted_context.digest
    assert request.host_max_total_subagents == 3
    assert seen["kwargs"] == {"runtime_enforceable": True}
    assert decision.outcome.value == "allowed"
    assert decision.evidence["constraints"]["version"] == 2
    assert decision.evidence["constraints"]["max_total_subagents"] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    (
        "health_status",
        "generation_offset",
        "expected_outcome",
        "expected_host_calls",
    ),
    [
        ("healthy", 0, "allowed", 1),
        ("healthy", 1, "indeterminate", 0),
        ("unhealthy", 0, "indeterminate", 0),
        ("unknown", 0, "indeterminate", 0),
        (None, 0, "indeterminate", 0),
    ],
)
async def test_required_v2_health_fences_projection(
    health_status: str | None,
    generation_offset: int,
    expected_outcome: str,
    expected_host_calls: int,
) -> None:
    accepted = _accepted_v2()
    host_calls = 0

    class _Host:
        capability_api_version = INVOCATION_CONSTRAINTS_CAPABILITY_API_VERSION_V2
        required_capability_id = INVOCATION_CONSTRAINTS_REQUIRED_CAPABILITY_V2

        async def project(self, request, **_kwargs):
            nonlocal host_calls
            host_calls += 1
            return _v2_projection(request)

    class _Health:
        async def health_for(self, capability_ids, *, refresh):
            assert capability_ids == {INVOCATION_CONSTRAINTS_REQUIRED_CAPABILITY_V2}
            assert refresh is True
            if health_status is None:
                return ()
            return (
                SimpleNamespace(
                    capability_id=INVOCATION_CONSTRAINTS_REQUIRED_CAPABILITY_V2,
                    status=health_status,
                    extension_generation=(accepted.extension_generation + generation_offset),
                ),
            )

    async def _worker(_record):
        return None

    launch = PreparedLaunch(
        thread_id=accepted.thread_id,
        assistant_id="default",
        on_disconnect=DisconnectMode.cancel,
        metadata={},
        kwargs={},
        multitask_strategy="reject",
        model_name=None,
        user_id="owner-1",
        worker=_worker,
        accepted_invocation=accepted,
        request_digest=_DIGESTS[3],
        request_digest_version="sha256-canonical-json-v1",
    )

    decision = await ProviderInvocationConstraints(_Host(), _Health()).project(launch)

    assert decision.outcome.value == expected_outcome
    assert host_calls == expected_host_calls


@pytest.mark.asyncio
async def test_replay_reuses_v2_evidence_while_a_new_invocation_gets_a_new_projection() -> None:
    accepted = _accepted_v2()
    provider_calls = 0

    class _Host:
        capability_api_version = INVOCATION_CONSTRAINTS_CAPABILITY_API_VERSION_V2

        async def project(self, request, **_kwargs):
            nonlocal provider_calls
            provider_calls += 1
            return _v2_projection(request, projection_revision=f"policy-{provider_calls}")

    class _Normalizer:
        @contextmanager
        def scope(self, _intent):
            yield

        async def identify(self, intent):
            return InternalAdmissionIdentity(
                external_scope="service:v1:sha256:scope",
                external_key=f"raw:{intent.external_key}",
                principal_digest=accepted.principal_digest,
                base_origin_digest=accepted.base_origin_digest,
                thread_id=accepted.thread_id,
                requested_agent_id=accepted.agent_revision.agent_id,
                user_id="owner-1",
                principal=InvocationPrincipal(user_id="owner-1"),
            )

        async def validate_replay(self, _intent, _identity, _record):
            return None

        async def normalize(self, intent):
            async def _worker(_record):
                return None

            return PreparedLaunch(
                thread_id=accepted.thread_id,
                assistant_id="default",
                on_disconnect=DisconnectMode.cancel,
                metadata={},
                kwargs={},
                multitask_strategy="reject",
                model_name=None,
                user_id="owner-1",
                worker=_worker,
                accepted_invocation=accepted,
                external_scope="service:v1:sha256:scope",
                external_key=f"raw:{intent.external_key}",
                request_digest=_DIGESTS[3],
                request_digest_version="sha256-canonical-json-v1",
            )

    class _Runs:
        def __init__(self):
            self.records: dict[str, RunRecord] = {}

        async def find_by_external_identity(self, identity):
            return self.records.get(identity.external_key)

        @asynccontextmanager
        async def admission_scope(self, _thread_id):
            yield

        async def prepare_admission(self, _launch):
            return None

        async def admit(self, launch):
            now = datetime.now(UTC).isoformat()
            record = RunRecord(
                run_id=f"run-{len(self.records) + 1}",
                thread_id=launch.thread_id,
                assistant_id=launch.assistant_id,
                status=RunStatus.pending,
                on_disconnect=launch.on_disconnect,
                user_id=launch.user_id,
                created_at=now,
                updated_at=now,
                request_digest=launch.request_digest,
                accepted_invocation=launch.accepted_invocation,
            )
            self.records[launch.external_key] = record
            return DurableAdmission(record, AdmissionOutcome.created)

        async def observe(self, run_id, _principal):
            return next((record for record in self.records.values() if record.run_id == run_id), None)

        async def fail_start(self, *_args):
            raise AssertionError("worker attachment should not fail")

    class _Authorization:
        async def authorize_start(self, _launch):
            return InternalAuthorizationDecision.allowed()

        async def authorize_observe(self, *_args):
            return InternalAuthorizationDecision.allowed()

    runs = _Runs()
    runtime = InvocationRuntime(
        normalizer=_Normalizer(),
        runs=runs,
        authorization=_Authorization(),
        constraints=ProviderInvocationConstraints(_Host()),
        task_factory=lambda worker: worker.close(),
    )

    first = await runtime.launch(InternalLaunchIntent(thread_id="thread-1", external_key="key-1"))
    first_evidence = first.record.accepted_invocation.decision_evidence["constraints"]
    replay = await runtime.launch(InternalLaunchIntent(thread_id="thread-1", external_key="key-1"))
    second = await runtime.launch(InternalLaunchIntent(thread_id="thread-1", external_key="key-2"))

    assert first.created is True
    assert replay.created is False
    assert replay.record is first.record
    assert replay.record.accepted_invocation.decision_evidence["constraints"] == first_evidence
    assert second.created is True
    assert provider_calls == 2
    assert first_evidence["projection_revision"] == "policy-1"
    assert second.record.accepted_invocation.decision_evidence["constraints"]["projection_revision"] == "policy-2"


@pytest.mark.asyncio
async def test_worker_enforces_zero_ceiling_before_any_subagent_dispatch() -> None:
    accepted = _accepted_v2()
    trusted = accepted.trusted_context
    assert trusted is not None
    now = datetime(2026, 8, 8, 12, tzinfo=UTC)
    projection = ConstraintProjectionV2(
        request_digest=_DIGESTS[3],
        trusted_context_digest=trusted.digest,
        thread_id=accepted.thread_id,
        agent_revision_digest=accepted.agent_revision.digest,
        profile_revision_digest=trusted.profile_revision.digest,
        extension_manifest_digest=_DIGESTS[5],
        extension_generation=accepted.extension_generation,
        projection_revision="policy-8",
        issued_at=now,
        valid_until=now + timedelta(minutes=5),
        evidence_id="evidence-8",
        evidence_digest=_DIGESTS[0],
        mandatory_obligations=("max_total_subagents",),
        max_total_subagents=0,
    )
    decision_evidence = dict(accepted.decision_evidence)
    decision_evidence["constraints"] = InternalConstraintDecision.projected(projection).evidence["constraints"]
    accepted = dataclasses.replace(accepted, decision_evidence=decision_evidence)
    manager = RunManager()
    record = await manager.create_or_reject("thread-1", accepted_invocation=accepted)
    record.request_digest = _DIGESTS[3]
    seen: dict[str, object] = {}
    stream_calls = 0

    class _Agent:
        async def astream(self, *_args, **_kwargs):
            nonlocal stream_calls
            stream_calls += 1
            yield {"messages": []}

    def _factory(*, config):
        seen.update(config["context"])
        return _Agent()

    bridge = SimpleNamespace(
        publish=AsyncMock(),
        publish_end=AsyncMock(),
        cleanup=AsyncMock(),
    )
    await run_agent(
        bridge,
        manager,
        record,
        ctx=RunContext(checkpointer=None, constraint_clock=lambda: now),
        agent_factory=_factory,
        graph_input={},
        config={},
    )

    assert record.status is RunStatus.success
    assert stream_calls == 1
    assert isinstance(seen[INVOCATION_CONSTRAINTS_CONTEXT_KEY], ConstraintProjectionV2)
    reservation = seen[SUBAGENT_RESERVATION_CONTEXT_KEY]
    assert reservation.limit == 0
    assert reservation.reserve("dispatch-1") is False


@pytest.mark.asyncio
async def test_worker_rejects_unknown_mandatory_obligation_before_graph_construction() -> None:
    accepted = _accepted_v2()
    trusted = accepted.trusted_context
    assert trusted is not None
    now = datetime(2026, 8, 8, 12, tzinfo=UTC)
    projection = ConstraintProjectionV2(
        request_digest=_DIGESTS[3],
        trusted_context_digest=trusted.digest,
        thread_id=accepted.thread_id,
        agent_revision_digest=accepted.agent_revision.digest,
        profile_revision_digest=trusted.profile_revision.digest,
        extension_manifest_digest=_DIGESTS[5],
        extension_generation=accepted.extension_generation,
        projection_revision="policy-8",
        issued_at=now,
        valid_until=now + timedelta(minutes=5),
        evidence_id="evidence-8",
        evidence_digest=_DIGESTS[0],
        mandatory_obligations=("future_resource_limit",),
    )
    evidence = dict(accepted.decision_evidence)
    evidence["constraints"] = InternalConstraintDecision.projected(projection).evidence["constraints"]
    accepted = dataclasses.replace(accepted, decision_evidence=evidence)
    manager = RunManager()
    record = await manager.create_or_reject("thread-1", accepted_invocation=accepted)
    record.request_digest = _DIGESTS[3]
    factory_calls = 0

    def _factory(*, config):
        nonlocal factory_calls
        factory_calls += 1
        raise AssertionError("graph construction must not run")

    bridge = SimpleNamespace(
        publish=AsyncMock(),
        publish_end=AsyncMock(),
        cleanup=AsyncMock(),
    )
    await run_agent(
        bridge,
        manager,
        record,
        ctx=RunContext(checkpointer=None, constraint_clock=lambda: now),
        agent_factory=_factory,
        graph_input={},
        config={},
    )

    assert factory_calls == 0
    assert record.status is RunStatus.error
    assert record.stop_reason == "constraint_evidence_mismatch"
