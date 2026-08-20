"""Invocation-operation authorization at the durable runtime boundary."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager, contextmanager
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
import yaml
from deerflow_extension_api import AuthzDecision, AuthzReason
from fastapi import HTTPException

from app.runtime.authorization import (
    ProviderInvocationAuthorization,
    validate_invocation_authorization_startup,
)
from app.runtime.invocation import (
    DurableAdmission,
    InternalAuthorizationDecision,
    InternalCancelRequest,
    InternalLaunchIntent,
    InvocationAuthorizationOutcome,
    InvocationPrincipal,
    InvocationRuntime,
    NotFoundOrInvisible,
    PreparedLaunch,
)
from deerflow.config.authorization_config import AuthorizationConfig, InvocationOperationsAuthorizationConfig
from deerflow.runtime import CancelOutcome, DisconnectMode, RunRecord, RunStatus
from deerflow.runtime.accepted_invocation import (
    AcceptedInvocation,
    InvocationOrigin,
    PrincipalProjection,
    ResolvedAgentRevision,
)
from deerflow.runtime.runs.manager import IdempotencyConflictError
from deerflow.runtime.runs.store.base import AdmissionOutcome


def test_invocation_operation_authorization_is_a_complete_opt_in() -> None:
    config = AuthorizationConfig()

    assert config.invocation_operations.start_enabled is False
    assert config.invocation_operations.observe_enabled is False
    assert config.invocation_operations.cancel_enabled is False
    assert config.invocation_operations.timeout_seconds == 2.0


def test_invocation_operation_config_is_operator_only_and_versioned() -> None:
    root = Path(__file__).parents[2]
    example = yaml.safe_load((root / "config.example.yaml").read_text())

    assert example["config_version"] == 41
    assert example["authorization"]["invocation_operations"] == {
        "start_enabled": False,
        "observe_enabled": False,
        "cancel_enabled": False,
        "timeout_seconds": 2.0,
    }
    assert "invocation_operations" not in (root / "extensions_config.example.json").read_text()


def _record(*, run_id: str = "run-1") -> RunRecord:
    now = datetime.now(UTC).isoformat()
    return RunRecord(
        run_id=run_id,
        thread_id="thread-1",
        assistant_id="agent-1",
        status=RunStatus.pending,
        on_disconnect=DisconnectMode.cancel,
        user_id="owner-1",
        created_at=now,
        updated_at=now,
        state_version=1,
    )


class _Normalizer:
    def __init__(self, events: list[str], *, keyed: bool = False) -> None:
        self.events = events
        self.keyed = keyed

    @contextmanager
    def scope(self, _intent: InternalLaunchIntent):
        yield

    async def identify(self, _intent):
        if not self.keyed:
            return None
        from app.runtime.invocation import InternalAdmissionIdentity

        return InternalAdmissionIdentity(
            external_scope="http:v1:sha256:scope",
            external_key="raw:key",
            principal_digest="a" * 64,
            base_origin_digest="b" * 64,
            thread_id="thread-1",
            requested_agent_id="agent-1",
            user_id="owner-1",
        )

    async def validate_replay(self, _intent, _identity, _record):
        self.events.append("validate_replay")

    async def normalize(self, intent: InternalLaunchIntent) -> PreparedLaunch:
        self.events.append("normalize")

        async def worker(_record: RunRecord) -> None:
            self.events.append("worker")

        return PreparedLaunch(
            thread_id=intent.thread_id,
            assistant_id="agent-1",
            on_disconnect=DisconnectMode.cancel,
            metadata={},
            kwargs={},
            multitask_strategy="reject",
            model_name=None,
            user_id="owner-1",
            worker=worker,
            request_digest="c" * 64,
            request_digest_version="sha256-canonical-json-v1",
        )


class _Runs:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.record = _record()
        self.existing: RunRecord | None = None
        self.admission_outcome = AdmissionOutcome.created
        self.cancelled = False

    @asynccontextmanager
    async def admission_scope(self, _thread_id: str):
        yield

    async def prepare_admission(self, _launch):
        self.events.append("prepare")

    async def admit(self, _launch, *, candidate_run_id):
        self.events.append("admit")
        self.record.run_id = candidate_run_id
        return DurableAdmission(self.record, self.admission_outcome)

    async def attach_worker(self, record, worker, task_factory):
        record.task = task_factory(worker)
        return record.task

    async def find_by_external_identity(self, _identity):
        self.events.append("lookup")
        return self.existing

    async def fail_start(self, _record, _error):
        raise AssertionError("not expected")

    async def observe(self, run_id, _principal):
        self.events.append(f"visible:{run_id}")
        if run_id == self.record.run_id:
            return self.record
        return None

    async def cancel(self, _request):
        self.events.append("cancel")
        self.cancelled = True
        return CancelOutcome.cancelled


class _Authorization:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.start = InternalAuthorizationDecision.allowed()
        self.observe = InternalAuthorizationDecision.allowed()
        self.cancel = InternalAuthorizationDecision.allowed()

    async def authorize_start(self, _launch):
        self.events.append("authorize:start")
        return self.start

    async def authorize_observe(self, _record, _principal, *, target_kind="run"):
        self.events.append(f"authorize:observe:{target_kind}")
        return self.observe

    async def authorize_cancel(self, _record, _principal):
        self.events.append("authorize:cancel")
        return self.cancel


@pytest.mark.anyio
async def test_start_denial_stops_before_admission_and_worker_work() -> None:
    events: list[str] = []
    authorization = _Authorization(events)
    authorization.start = InternalAuthorizationDecision.denied()
    runs = _Runs(events)
    runtime = InvocationRuntime(
        normalizer=_Normalizer(events),
        runs=runs,
        authorization=authorization,
    )

    result = await runtime.launch(InternalLaunchIntent(thread_id="thread-1"))

    assert result is InvocationAuthorizationOutcome.denied
    assert events == ["normalize", "authorize:start"]
    assert runs.record.task is None


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("outcome", "status_code"),
    [
        (InvocationAuthorizationOutcome.denied, 403),
        (InvocationAuthorizationOutcome.indeterminate, 503),
    ],
)
async def test_http_start_facade_maps_finite_authorization_failures(
    monkeypatch,
    outcome,
    status_code,
) -> None:
    from app.gateway import services
    from app.gateway.run_models import RunCreateRequest

    runtime = SimpleNamespace(launch=AsyncMock(return_value=outcome))
    request = SimpleNamespace(headers={})
    monkeypatch.setattr(services, "build_invocation_runtime", lambda _request: runtime)

    with pytest.raises(HTTPException) as exc_info:
        await services.start_run(
            RunCreateRequest(input={"messages": [{"role": "user", "content": "hello"}]}),
            "thread-1",
            request,
        )

    assert exc_info.value.status_code == status_code


@pytest.mark.anyio
async def test_known_replay_uses_fresh_observe_authorization_and_bypasses_start() -> None:
    events: list[str] = []
    authorization = _Authorization(events)
    runs = _Runs(events)
    runs.existing = runs.record
    runtime = InvocationRuntime(
        normalizer=_Normalizer(events, keyed=True),
        runs=runs,
        authorization=authorization,
    )

    receipt = await runtime.launch(InternalLaunchIntent(thread_id="thread-1", external_key="key"))

    assert receipt.record is runs.record
    assert receipt.created is False
    assert events == ["lookup", "authorize:observe:run", "validate_replay"]


@pytest.mark.anyio
async def test_gateway_visibility_rechecks_owner_for_process_local_record() -> None:
    from app.gateway.services import _GatewayDurableRuns

    record = _record()
    run_manager = SimpleNamespace(get=AsyncMock(return_value=record))
    request = SimpleNamespace(
        app=SimpleNamespace(state=SimpleNamespace(run_manager=run_manager)),
    )

    visible = await _GatewayDurableRuns(request).observe(
        record.run_id,
        InvocationPrincipal(user_id="intruder"),
    )

    assert visible is None
    run_manager.get.assert_awaited_once_with(record.run_id, user_id="intruder")


@pytest.mark.anyio
async def test_enabled_run_feed_observes_the_run_before_policy(
    monkeypatch,
) -> None:
    from app.gateway.routers import thread_runs

    record = _record()
    principal = InvocationPrincipal(
        user_id="owner-1",
        visibility_prevalidated=True,
    )
    runtime = SimpleNamespace(observe_run=AsyncMock(return_value=record))
    request = SimpleNamespace()
    monkeypatch.setattr(
        thread_runs,
        "invocation_observation_enabled",
        lambda _request: True,
    )
    monkeypatch.setattr(
        thread_runs,
        "build_invocation_runtime",
        lambda _request: runtime,
    )
    monkeypatch.setattr(
        thread_runs,
        "_invocation_principal",
        AsyncMock(return_value=principal),
    )

    await thread_runs._authorize_run_feed(
        request,
        thread_id=record.thread_id,
        run_id=record.run_id,
    )

    runtime.observe_run.assert_awaited_once_with(record.run_id, principal)


@pytest.mark.anyio
async def test_known_changed_digest_conflicts_only_after_fresh_observe_authorization() -> None:
    events: list[str] = []
    authorization = _Authorization(events)
    runs = _Runs(events)
    runs.existing = runs.record

    class ConflictingNormalizer(_Normalizer):
        async def validate_replay(self, _intent, _identity, _record):
            self.events.append("validate_replay")
            raise IdempotencyConflictError("changed digest")

    runtime = InvocationRuntime(
        normalizer=ConflictingNormalizer(events, keyed=True),
        runs=runs,
        authorization=authorization,
    )

    with pytest.raises(IdempotencyConflictError, match="changed digest"):
        await runtime.launch(
            InternalLaunchIntent(thread_id="thread-1", external_key="key"),
        )

    assert events == ["lookup", "authorize:observe:run", "validate_replay"]


@pytest.mark.anyio
async def test_observe_and_cancel_apply_visibility_before_policy_and_mutation() -> None:
    events: list[str] = []
    authorization = _Authorization(events)
    runs = _Runs(events)
    runtime = InvocationRuntime(
        normalizer=_Normalizer(events),
        runs=runs,
        authorization=authorization,
    )
    principal = InvocationPrincipal(user_id="owner-1")

    assert await runtime.observe_run("hidden", principal) is NotFoundOrInvisible.not_found_or_invisible
    assert events == ["visible:hidden"]

    events.clear()
    authorization.observe = InternalAuthorizationDecision.indeterminate()
    result = await runtime.observe_run("run-1", principal)
    assert result is InvocationAuthorizationOutcome.indeterminate
    assert events == ["visible:run-1", "authorize:observe:run"]

    events.clear()
    authorization.cancel = InternalAuthorizationDecision.denied()
    result = await runtime.cancel_run(
        InternalCancelRequest(run_id="run-1", principal=principal),
    )
    assert result is InvocationAuthorizationOutcome.denied
    assert events == ["visible:run-1", "authorize:cancel"]
    assert runs.cancelled is False


@pytest.mark.anyio
async def test_concurrent_first_callers_consult_start_but_only_creator_attaches() -> None:
    events: list[str] = []
    authorization = _Authorization(events)
    normalizer = _Normalizer(events, keyed=True)

    class RacingRuns(_Runs):
        def __init__(self, recorded_events):
            super().__init__(recorded_events)
            self.lock = asyncio.Lock()
            self.created = False

        @asynccontextmanager
        async def admission_scope(self, _thread_id):
            async with self.lock:
                yield

        async def admit(self, _launch, *, candidate_run_id):
            self.events.append("admit")
            if self.created:
                return DurableAdmission(self.record, AdmissionOutcome.known_same)
            self.created = True
            self.record.run_id = candidate_run_id
            return DurableAdmission(self.record, AdmissionOutcome.created)

    runs = RacingRuns(events)
    runtime = InvocationRuntime(
        normalizer=normalizer,
        runs=runs,
        authorization=authorization,
    )

    first, second = await asyncio.gather(
        runtime.launch(InternalLaunchIntent(thread_id="thread-1", external_key="key")),
        runtime.launch(InternalLaunchIntent(thread_id="thread-1", external_key="key")),
    )

    assert sorted((first.created, second.created)) == [False, True]
    assert events.count("authorize:start") == 2
    assert events.count("authorize:observe:run") == 1
    assert runs.record.task is not None
    await runs.record.task
    assert events.count("worker") == 1


@pytest.mark.anyio
async def test_allowed_start_evidence_is_attached_before_durable_admission() -> None:
    events: list[str] = []
    accepted = _accepted()
    evidence = {
        "version": 1,
        "decisions": [
            {
                "authorization_generation": 3,
                "policy_id": "policy.v1",
                "reason_codes": ["allow"],
                "evidence_digest": "f" * 64,
            }
        ],
    }

    class AcceptedNormalizer(_Normalizer):
        async def normalize(self, intent):
            launch = await super().normalize(intent)
            from dataclasses import replace

            return replace(launch, accepted_invocation=accepted)

    class EvidenceAuthorization(_Authorization):
        async def authorize_start(self, _launch):
            self.events.append("authorize:start")
            return InternalAuthorizationDecision.allowed(evidence=evidence)

    class CapturingRuns(_Runs):
        async def admit(self, launch, *, candidate_run_id):
            self.events.append("admit")
            assert launch.accepted_invocation is not accepted
            assert launch.accepted_invocation.to_persisted()["decision_evidence_json"] == evidence
            self.record.run_id = candidate_run_id
            return DurableAdmission(self.record, AdmissionOutcome.created)

    runs = CapturingRuns(events)
    runtime = InvocationRuntime(
        normalizer=AcceptedNormalizer(events),
        runs=runs,
        authorization=EvidenceAuthorization(events),
    )

    receipt = await runtime.launch(InternalLaunchIntent(thread_id="thread-1"))

    assert receipt.created is True
    await receipt.record.task


def _accepted(
    *,
    source_kind: str = "native_channel",
    source_references: dict | None = None,
) -> AcceptedInvocation:
    if source_references is None:
        source_references = {"provider": "slack", "chat_id": "chat-1"}
    return AcceptedInvocation.seal(
        principal=PrincipalProjection(
            user_id="owner-1",
            role="member",
            oauth_provider="oidc",
            oauth_id="subject-1",
            channel_user_id="sender-1",
        ),
        origin=InvocationOrigin(
            source_kind=source_kind,
            references=source_references,
        ),
        thread_id="thread-1",
        context_references={"non_interactive": False},
        agent_revision=ResolvedAgentRevision(
            agent_id="agent-1",
            digest="d" * 64,
            storage_source="database",
            storage_version="7",
        ),
        normalized_input={"messages": ["secret prompt is represented only by the request digest"]},
        execution_options={"multitask_strategy": "reject"},
        extension_generation=9,
        contributor_execution_digest="e" * 64,
    )


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("source_kind", "source_references"),
    [
        ("http", {}),
        ("scheduled_task", {"task_id": "task-1", "task_run_id": "occurrence-1"}),
        ("native_channel", {"provider": "slack", "chat_id": "chat-1"}),
    ],
)
async def test_every_origin_uses_sealed_source_evidence_not_caller_forgery(
    source_kind,
    source_references,
) -> None:
    provider = _Provider(AuthzDecision(allow=True))
    authorization = _provider_authorization(provider, start_enabled=True)
    accepted = _accepted(
        source_kind=source_kind,
        source_references=source_references,
    )
    launch = SimpleNamespace(
        accepted_invocation=accepted,
        external_scope=None,
        external_key=None,
        request_digest="c" * 64,
        request_digest_version="sha256-canonical-json-v1",
        metadata={
            "source_kind": "forged",
            "owner_user_id": "forged-owner",
            "agent_id": "forged-agent",
        },
    )

    result = await authorization.authorize_start(launch)

    assert result.outcome is InvocationAuthorizationOutcome.allowed
    request = provider.requests[0]
    assert request.context["origin"]["source_kind"] == source_kind
    assert request.context["origin"]["references"] == source_references
    assert "forged" not in repr(request.context)


class _Provider:
    name = "test-provider"

    def __init__(self, result) -> None:
        self.result = result
        self.requests = []

    def authorize(self, request):
        self.requests.append(request)
        return self.result

    async def aauthorize(self, request):
        self.requests.append(request)
        if isinstance(self.result, BaseException):
            raise self.result
        return self.result

    def filter_resources(self, _principal, _resource_type, candidates):
        return list(candidates)


def _provider_authorization(
    provider,
    **settings,
) -> ProviderInvocationAuthorization:
    config = InvocationOperationsAuthorizationConfig(**settings)
    return ProviderInvocationAuthorization(
        config,
        lambda: SimpleNamespace(generation=12, provider=provider),
    )


@pytest.mark.anyio
async def test_start_request_uses_only_sealed_host_facts_and_returns_bounded_evidence() -> None:
    provider = _Provider(
        AuthzDecision(
            allow=True,
            policy_id="policy.start.v1",
            reasons=[AuthzReason(code="allowed", message="provider detail is digested")],
            metadata={"rule": "agent-owner"},
        )
    )
    authorization = _provider_authorization(provider, start_enabled=True)
    accepted = _accepted()
    launch = PreparedLaunch(
        thread_id="thread-1",
        assistant_id="caller-forged-agent",
        on_disconnect=DisconnectMode.cancel,
        metadata={"caller_claim": "must-not-enter-policy"},
        kwargs={"input": "must-not-enter-policy"},
        multitask_strategy="reject",
        model_name=None,
        user_id="owner-1",
        worker=lambda _record: None,  # type: ignore[arg-type,return-value]
        accepted_invocation=accepted,
        external_scope="channel:v1:sha256:" + "a" * 64,
        external_key="raw:event-1",
        request_digest="c" * 64,
        request_digest_version="sha256-canonical-json-v1",
    )

    decision = await authorization.authorize_start(launch)

    assert decision.outcome is InvocationAuthorizationOutcome.allowed
    assert decision.evidence == {
        "version": 1,
        "decisions": (
            {
                "authorization_generation": 12,
                "policy_id": "policy.start.v1",
                "reason_codes": ("allowed",),
                "evidence_digest": decision.evidence["decisions"][0]["evidence_digest"],
            },
        ),
    }
    assert len(decision.evidence["decisions"][0]["evidence_digest"]) == 64
    request = provider.requests[0]
    assert (request.resource, request.action, request.target) == (
        "invocation",
        "start",
        "agent:agent-1@sha256:" + "d" * 64,
    )
    assert request.principal.user_id == "owner-1"
    assert set(request.context) == {
        "principal",
        "origin",
        "bound_context",
        "external_key",
        "request_digest",
        "request_digest_version",
        "revision",
        "safe_source_evidence",
    }
    assert "caller_claim" not in repr(request.context)
    assert "secret prompt" not in repr(request.context)


@pytest.mark.anyio
@pytest.mark.parametrize(
    "provider_result",
    [
        RuntimeError("provider unavailable"),
        object(),
        AuthzDecision(allow="yes"),
        AuthzDecision(allow=True, reasons=[AuthzReason(code="bad code")]),
    ],
)
async def test_provider_failure_or_malformed_decision_is_indeterminate(provider_result) -> None:
    provider = _Provider(provider_result)
    authorization = _provider_authorization(provider, start_enabled=True)
    launch = SimpleNamespace(accepted_invocation=_accepted(), external_scope=None, external_key=None, request_digest=None, request_digest_version=None)

    result = await authorization.authorize_start(launch)

    assert result.outcome is InvocationAuthorizationOutcome.indeterminate
    assert result.evidence is None


@pytest.mark.anyio
async def test_provider_timeout_is_indeterminate() -> None:
    class SlowProvider(_Provider):
        async def aauthorize(self, request):
            import asyncio

            self.requests.append(request)
            await asyncio.sleep(1)
            return AuthzDecision(allow=True)

    provider = SlowProvider(AuthzDecision(allow=True))
    authorization = _provider_authorization(
        provider,
        start_enabled=True,
        timeout_seconds=0.001,
    )
    launch = SimpleNamespace(accepted_invocation=_accepted(), external_scope=None, external_key=None, request_digest=None, request_digest_version=None)

    result = await authorization.authorize_start(launch)

    assert result.outcome is InvocationAuthorizationOutcome.indeterminate


@pytest.mark.anyio
async def test_provider_exception_is_fail_closed_and_never_logged_verbatim(
    caplog,
) -> None:
    marker = "authorization-provider-secret-marker"
    provider = _Provider(RuntimeError(marker))
    authorization = _provider_authorization(provider, start_enabled=True)
    launch = SimpleNamespace(
        accepted_invocation=_accepted(),
        external_scope=None,
        external_key=None,
        request_digest=None,
        request_digest_version=None,
    )

    with caplog.at_level("WARNING", logger="app.runtime.authorization"):
        result = await authorization.authorize_start(launch)

    assert result.outcome is InvocationAuthorizationOutcome.indeterminate
    assert marker not in caplog.text
    record = next(item for item in caplog.records if getattr(item, "diagnostic_code", None) == "authorization_indeterminate")
    assert record.exception_class == "RuntimeError"
    assert len(record.correlation_id) == 32


@pytest.mark.anyio
async def test_observe_context_and_cancel_requests_have_distinct_targets() -> None:
    provider = _Provider(AuthzDecision(allow=False, reasons=[AuthzReason(code="denied")]))
    authorization = _provider_authorization(
        provider,
        observe_enabled=True,
        cancel_enabled=True,
    )
    record = _record()
    record.accepted_invocation = _accepted()
    principal = InvocationPrincipal(user_id="viewer-1", role="auditor")

    observed = await authorization.authorize_observe(record, principal)
    context = await authorization.authorize_context_observe("thread-1", principal)
    cancelled = await authorization.authorize_cancel(record, principal)

    assert observed.outcome is InvocationAuthorizationOutcome.denied
    assert context.outcome is InvocationAuthorizationOutcome.denied
    assert cancelled.outcome is InvocationAuthorizationOutcome.denied
    assert [(item.action, item.target) for item in provider.requests] == [
        ("observe", "run:run-1"),
        ("observe", "context:thread-1"),
        ("cancel", "run:run-1"),
    ]
    assert all(item.resource == "invocation" for item in provider.requests)


@pytest.mark.anyio
async def test_observe_context_and_cancel_provider_failures_are_indeterminate() -> None:
    provider = _Provider(RuntimeError("policy backend unavailable"))
    authorization = _provider_authorization(
        provider,
        observe_enabled=True,
        cancel_enabled=True,
    )
    record = _record()
    principal = InvocationPrincipal(user_id="viewer-1")

    decisions = [
        await authorization.authorize_observe(record, principal),
        await authorization.authorize_context_observe(record.thread_id, principal),
        await authorization.authorize_cancel(record, principal),
    ]

    assert [decision.outcome for decision in decisions] == [
        InvocationAuthorizationOutcome.indeterminate,
        InvocationAuthorizationOutcome.indeterminate,
        InvocationAuthorizationOutcome.indeterminate,
    ]


@pytest.mark.anyio
async def test_disabled_operations_never_resolve_or_call_a_provider() -> None:
    def fail_resolution():
        raise AssertionError("disabled operation resolved a provider")

    authorization = ProviderInvocationAuthorization(
        InvocationOperationsAuthorizationConfig(),
        fail_resolution,
    )
    record = _record()
    principal = InvocationPrincipal(user_id="owner-1")

    assert (await authorization.authorize_start(SimpleNamespace())).outcome is InvocationAuthorizationOutcome.allowed
    assert (await authorization.authorize_observe(record, principal)).outcome is InvocationAuthorizationOutcome.allowed
    assert (await authorization.authorize_context_observe("thread-1", principal)).outcome is InvocationAuthorizationOutcome.allowed
    assert (await authorization.authorize_cancel(record, principal)).outcome is InvocationAuthorizationOutcome.allowed


def test_enabled_operation_requires_enabled_initialized_provider_at_startup() -> None:
    enabled_start = AuthorizationConfig(
        invocation_operations={"start_enabled": True},
    )
    with pytest.raises(ValueError, match="authorization.enabled=true"):
        validate_invocation_authorization_startup(
            enabled_start,
            SimpleNamespace(provider=None),
        )

    missing_provider = AuthorizationConfig(
        enabled=True,
        invocation_operations={"cancel_enabled": True},
    )
    with pytest.raises(ValueError, match="initialized authorization provider"):
        validate_invocation_authorization_startup(
            missing_provider,
            SimpleNamespace(provider=None),
        )

    validate_invocation_authorization_startup(
        AuthorizationConfig(),
        SimpleNamespace(provider=None),
    )


def test_startup_only_invocation_flags_do_not_replace_legacy_provider_generation() -> None:
    from app.gateway.authorization import AuthorizationProviderResolver
    from deerflow.config.authorization_config import AuthorizationProviderConfig
    from deerflow.extensions.registry import ExtensionRegistry

    resolver = AuthorizationProviderResolver(
        ExtensionRegistry().build(generation=4),
        AuthorizationConfig(
            enabled=True,
            provider=AuthorizationProviderConfig(
                use="extension_test_fixtures.demo_extensions:CountingAuthorizationProvider",
                config={"label": "shared"},
            ),
        ),
    )
    initial = resolver.snapshot()
    changed_startup_only_setting = resolver.resolve(
        AuthorizationConfig(
            enabled=True,
            provider=AuthorizationProviderConfig(
                use="extension_test_fixtures.demo_extensions:CountingAuthorizationProvider",
                config={"label": "shared"},
            ),
            invocation_operations={"start_enabled": True},
        )
    )

    assert changed_startup_only_setting is initial
    assert changed_startup_only_setting.provider is initial.provider


@pytest.mark.anyio
async def test_invocation_authorization_uses_the_resolver_provider_identity() -> None:
    from app.gateway.authorization import AuthorizationProviderResolver
    from deerflow.config.authorization_config import AuthorizationProviderConfig
    from deerflow.extensions.registry import ExtensionRegistry

    config = AuthorizationConfig(
        enabled=True,
        provider=AuthorizationProviderConfig(
            use="extension_test_fixtures.demo_extensions:CountingAuthorizationProvider",
            config={"label": "coherent"},
        ),
        invocation_operations={"start_enabled": True},
    )
    resolver = AuthorizationProviderResolver(
        ExtensionRegistry().build(generation=4),
        config,
    )
    snapshot = resolver.snapshot()
    calls = []
    original = snapshot.provider.aauthorize

    async def record_call(request):
        calls.append(request)
        return await original(request)

    snapshot.provider.aauthorize = record_call
    authorization = ProviderInvocationAuthorization(
        config.invocation_operations,
        lambda: resolver.resolve(config),
    )
    launch = SimpleNamespace(
        accepted_invocation=_accepted(),
        external_scope=None,
        external_key=None,
        request_digest="c" * 64,
        request_digest_version="sha256-canonical-json-v1",
    )

    result = await authorization.authorize_start(launch)

    assert result.outcome is InvocationAuthorizationOutcome.allowed
    assert resolver.snapshot().provider is snapshot.provider
    assert calls[0].action == "start"
