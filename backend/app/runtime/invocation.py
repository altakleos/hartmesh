"""Application-layer ownership of durable invocation sequencing."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine, Mapping
from contextlib import AbstractAsyncContextManager, AbstractContextManager, asynccontextmanager
from dataclasses import dataclass, field, replace
from enum import StrEnum
from types import MappingProxyType
from typing import Any, Literal, Protocol

from deerflow_extension_api import ConstraintProjectionV1, ConstraintProjectionV2, InvocationIdentityV1

from app.runtime.idempotency import CanonicalCallerIntent
from deerflow.runtime import CancelOutcome, DisconnectMode, RunRecord
from deerflow.runtime.accepted_invocation import AcceptedInvocation
from deerflow.runtime.runs.lifecycle_query import LifecyclePage, LifecycleQuery
from deerflow.runtime.runs.store.base import (
    AdmissionOutcome,
    CancellationRequestOutcome,
    lifecycle_owner_scope,
)

WorkerCoroutine = Coroutine[Any, Any, None]
WorkerFactory = Callable[[RunRecord], WorkerCoroutine]
TaskFactory = Callable[[WorkerCoroutine], asyncio.Task[None]]


def _freeze_host_value(value: Any) -> Any:
    """Snapshot nested host values without retaining caller-owned containers."""

    if isinstance(value, Mapping):
        return MappingProxyType({key: _freeze_host_value(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_host_value(item) for item in value)
    if isinstance(value, (set, frozenset)):
        return frozenset(_freeze_host_value(item) for item in value)
    return value


def thaw_host_value(value: Any) -> Any:
    """Return a fresh mutable copy of a frozen host-internal value."""

    if isinstance(value, Mapping):
        return {key: thaw_host_value(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [thaw_host_value(item) for item in value]
    if isinstance(value, frozenset):
        return {thaw_host_value(item) for item in value}
    return value


class InternalSourceKind(StrEnum):
    http = "http"
    scheduled_task = "scheduled_task"
    native_channel = "native_channel"
    service = "service"


@dataclass(frozen=True)
class InternalNativeChannelFacts:
    """Authenticated native-channel facts carried only inside the host."""

    provider: str
    connection_id: str | None
    workspace_id: str | None
    chat_id: str
    topic_id: str | None
    provider_message_id: str | None
    channel_user_id: str
    resolved_assistant_id: str
    resolved_agent_name: str | None


@dataclass(frozen=True)
class InternalLaunchIntent:
    """Finite host request that snapshots every caller-owned container."""

    thread_id: str
    assistant_id: str | None = None
    input: Mapping[str, Any] | None = None
    command: Mapping[str, Any] | None = None
    metadata: Mapping[str, Any] | None = None
    config: Mapping[str, Any] | None = None
    context: Mapping[str, Any] | None = None
    checkpoint_id: str | None = None
    checkpoint: Mapping[str, Any] | None = None
    interrupt_before: tuple[str, ...] | Literal["*"] | None = None
    interrupt_after: tuple[str, ...] | Literal["*"] | None = None
    stream_mode: tuple[str, ...] | str | None = None
    stream_subgraphs: bool = False
    on_disconnect: Literal["cancel", "continue"] = "cancel"
    multitask_strategy: Literal["reject", "rollback", "interrupt"] = "reject"
    source_kind: InternalSourceKind = InternalSourceKind.http
    trusted_task_id: str | None = None
    task_run_id: str | None = None
    scheduled_trigger: Literal["scheduled", "manual"] | None = None
    owner_user_id: str | None = None
    native_channel: InternalNativeChannelFacts | None = None
    trusted_service_id: str | None = None
    external_key: str | None = None
    scheduled_system_owned: bool = False
    thread_id_explicit: bool = True

    def __post_init__(self) -> None:
        for name in ("input", "command", "metadata", "config", "context", "checkpoint"):
            value = getattr(self, name)
            if value is not None:
                if not isinstance(value, Mapping):
                    raise TypeError(f"{name} must be a mapping or None")
                object.__setattr__(self, name, _freeze_host_value(value))
        for name in ("interrupt_before", "interrupt_after", "stream_mode"):
            value = getattr(self, name)
            if isinstance(value, (list, tuple)):
                object.__setattr__(self, name, tuple(_freeze_host_value(item) for item in value))
            elif value is not None and not isinstance(value, str):
                raise TypeError(f"{name} must be a sequence, string, or None")


@dataclass(frozen=True)
class InternalAdmissionIdentity:
    """Host-authenticated identity available before expensive normalization."""

    external_scope: str
    external_key: str
    principal_digest: str
    base_origin_digest: str
    thread_id: str | None
    requested_agent_id: str
    caller_intent: CanonicalCallerIntent | None = None
    user_id: str | None = None
    principal: InvocationPrincipal = field(default_factory=lambda: InvocationPrincipal())


@dataclass(frozen=True)
class PreparedLaunch:
    """Sealed admission data plus one deferred worker factory.

    Metadata, persisted kwargs, callbacks, and caller-intent evidence are
    defensive immutable snapshots. Adapters thaw fresh copies only at the
    mutable RunManager/persistence boundary.
    """

    thread_id: str
    assistant_id: str | None
    on_disconnect: DisconnectMode
    metadata: Mapping[str, Any]
    kwargs: Mapping[str, Any]
    multitask_strategy: str
    model_name: str | None
    user_id: str | None
    worker: WorkerFactory = field(repr=False)
    accepted_invocation: AcceptedInvocation | None = field(default=None, repr=False)
    external_scope: str | None = None
    external_key: str | None = None
    request_digest: str | None = None
    request_digest_version: str | None = None
    caller_intent_json: Mapping[str, Any] | None = None
    caller_intent_digest: str | None = None
    caller_intent_digest_version: str | None = None
    principal: InvocationPrincipal = field(default_factory=lambda: InvocationPrincipal())

    def __post_init__(self) -> None:
        for name in ("metadata", "kwargs"):
            value = getattr(self, name)
            if not isinstance(value, Mapping):
                raise TypeError(f"{name} must be a mapping")
            object.__setattr__(self, name, _freeze_host_value(value))
        if self.caller_intent_json is not None:
            if not isinstance(self.caller_intent_json, Mapping):
                raise TypeError("caller_intent_json must be a mapping or None")
            object.__setattr__(self, "caller_intent_json", _freeze_host_value(self.caller_intent_json))


@dataclass(frozen=True)
class DurableAdmission:
    record: RunRecord
    outcome: AdmissionOutcome = AdmissionOutcome.created


@dataclass(frozen=True)
class InternalLaunchReceipt:
    record: RunRecord
    created: bool = True


@dataclass(frozen=True)
class InvocationPrincipal:
    user_id: str | None = None
    role: str | None = None
    oauth_provider: str | None = None
    oauth_id: str | None = None
    channel_user_id: str | None = None
    is_internal: bool = False
    visibility_prevalidated: bool = False
    identity: InvocationIdentityV1 | None = None

    def __post_init__(self) -> None:
        identity = self.identity
        if identity is None:
            if self.is_internal and (self.channel_user_id is not None or self.role not in {"internal", "service"}):
                object.__setattr__(self, "is_internal", False)
            return
        if not isinstance(identity, InvocationIdentityV1):
            raise TypeError("identity must be InvocationIdentityV1 or None")
        subject = identity.effective_subject
        object.__setattr__(self, "user_id", subject.subject_id)
        object.__setattr__(self, "role", subject.role)
        object.__setattr__(self, "oauth_provider", subject.oauth_provider)
        object.__setattr__(self, "oauth_id", subject.oauth_id)
        object.__setattr__(self, "is_internal", subject.kind == "service")


class InvocationAuthorizationOutcome(StrEnum):
    """Finite invocation-operation authorization outcomes."""

    allowed = "allowed"
    denied = "denied"
    indeterminate = "indeterminate"


class InvocationConstraintOutcome(StrEnum):
    """Finite restrictive-projection outcomes."""

    allowed = "allowed"
    denied = "denied"
    indeterminate = "indeterminate"


def _constraint_projection_evidence(projection: ConstraintProjectionV1 | ConstraintProjectionV2) -> dict[str, Any]:
    from deerflow.runtime.accepted_invocation import canonical_digest

    if isinstance(projection, ConstraintProjectionV2):
        normalized = {
            "version": 2,
            "request_digest": projection.request_digest,
            "trusted_context_digest": projection.trusted_context_digest,
            "thread_id": projection.thread_id,
            "agent_revision_digest": projection.agent_revision_digest,
            "profile_revision_digest": projection.profile_revision_digest,
            "extension_manifest_digest": projection.extension_manifest_digest,
            "extension_generation": projection.extension_generation,
            "projection_revision": projection.projection_revision,
            "issued_at": projection.issued_at.isoformat(),
            "valid_until": projection.valid_until.isoformat(),
            "evidence_id": projection.evidence_id,
            "evidence_digest": projection.evidence_digest,
            "mandatory_obligations": projection.mandatory_obligations,
            "max_total_subagents": projection.max_total_subagents,
        }
    else:
        normalized = {
            "version": 1,
            "request_digest": projection.request_digest,
            "agent_revision_digest": projection.agent_revision_digest,
            "projection_revision": projection.projection_revision,
            "issued_at": projection.issued_at.isoformat(),
            "valid_until": projection.valid_until.isoformat(),
            "evidence_id": projection.evidence_id,
            "evidence_digest": projection.evidence_digest,
            "max_total_subagents": projection.max_total_subagents,
        }
    normalized["projection_digest"] = canonical_digest(normalized)
    return {"version": 1, "constraints": normalized}


@dataclass(frozen=True)
class InternalConstraintDecision:
    outcome: InvocationConstraintOutcome
    evidence: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        if self.evidence is not None:
            if not isinstance(self.evidence, Mapping):
                raise TypeError("constraint evidence must be a mapping or None")
            object.__setattr__(self, "evidence", _freeze_host_value(self.evidence))

    @classmethod
    def absent(cls) -> InternalConstraintDecision:
        return cls(InvocationConstraintOutcome.allowed)

    @classmethod
    def projected(cls, projection: ConstraintProjectionV1 | ConstraintProjectionV2) -> InternalConstraintDecision:
        return cls(
            InvocationConstraintOutcome.allowed,
            evidence=_constraint_projection_evidence(projection),
        )

    @classmethod
    def projected_evidence(cls, evidence: Mapping[str, Any]) -> InternalConstraintDecision:
        return cls(InvocationConstraintOutcome.allowed, evidence=evidence)

    @classmethod
    def denied(cls) -> InternalConstraintDecision:
        return cls(InvocationConstraintOutcome.denied)

    @classmethod
    def indeterminate(cls) -> InternalConstraintDecision:
        return cls(InvocationConstraintOutcome.indeterminate)


@dataclass(frozen=True)
class InternalAuthorizationDecision:
    """Host-owned authorization result and optional safe persisted evidence."""

    outcome: InvocationAuthorizationOutcome
    evidence: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        if self.evidence is not None:
            if not isinstance(self.evidence, Mapping):
                raise TypeError("authorization evidence must be a mapping or None")
            object.__setattr__(self, "evidence", _freeze_host_value(self.evidence))

    @classmethod
    def allowed(cls, *, evidence: Mapping[str, Any] | None = None) -> InternalAuthorizationDecision:
        return cls(InvocationAuthorizationOutcome.allowed, evidence=evidence)

    @classmethod
    def denied(cls) -> InternalAuthorizationDecision:
        return cls(InvocationAuthorizationOutcome.denied)

    @classmethod
    def indeterminate(cls) -> InternalAuthorizationDecision:
        return cls(InvocationAuthorizationOutcome.indeterminate)


class NotFoundOrInvisible(StrEnum):
    not_found_or_invisible = "not_found_or_invisible"


@dataclass(frozen=True)
class InternalCancelRequest:
    run_id: str
    action: Literal["interrupt", "rollback"] = "interrupt"
    principal: InvocationPrincipal = field(default_factory=InvocationPrincipal)
    thread_id: str | None = None
    expected_state_version: int | None = None


@dataclass(frozen=True)
class InternalCancelReceipt:
    outcome: CancelOutcome | CancellationRequestOutcome
    record: RunRecord | None = None


@dataclass(frozen=True)
class InternalInvocationLifecycleQuery:
    run_id: str
    principal: InvocationPrincipal
    cursor: str | None = None
    limit: int = 100
    include_snapshot: bool = True


@dataclass(frozen=True)
class InternalContextLifecycleQuery:
    thread_id: str
    principal: InvocationPrincipal
    cursor: str | None = None
    limit: int = 100
    include_snapshot: bool = True
    source_kind: str | None = None


@dataclass(frozen=True)
class InternalLifecycleObservation:
    record: RunRecord | None
    page: LifecyclePage
    authoritative_snapshot: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        if self.authoritative_snapshot is not None:
            if not isinstance(self.authoritative_snapshot, Mapping):
                raise TypeError("authoritative_snapshot must be a mapping or None")
            object.__setattr__(self, "authoritative_snapshot", _freeze_host_value(self.authoritative_snapshot))


class LaunchNormalizer(Protocol):
    def scope(self, intent: InternalLaunchIntent) -> AbstractContextManager[None]: ...

    async def normalize(self, intent: InternalLaunchIntent) -> PreparedLaunch: ...

    async def identify(self, intent: InternalLaunchIntent) -> InternalAdmissionIdentity | None: ...

    async def validate_replay(
        self,
        intent: InternalLaunchIntent,
        identity: InternalAdmissionIdentity,
        record: RunRecord,
    ) -> None: ...


class DurableRuns(Protocol):
    def admission_scope(self, thread_id: str) -> AbstractAsyncContextManager[None]: ...

    async def prepare_admission(self, launch: PreparedLaunch) -> None: ...

    async def admit(self, launch: PreparedLaunch) -> DurableAdmission | RunRecord: ...

    async def find_by_external_identity(
        self,
        identity: InternalAdmissionIdentity,
    ) -> RunRecord | None: ...

    async def fail_start(self, record: RunRecord, error: str) -> None: ...

    async def observe(self, run_id: str, principal: InvocationPrincipal) -> RunRecord | None: ...

    async def context_visible(
        self,
        thread_id: str,
        principal: InvocationPrincipal,
    ) -> bool: ...

    async def query_lifecycle(self, query: LifecycleQuery) -> LifecyclePage: ...

    async def cancel(
        self,
        request: InternalCancelRequest,
    ) -> CancelOutcome | CancellationRequestOutcome: ...


class InvocationAuthorization(Protocol):
    async def authorize_start(
        self,
        launch: PreparedLaunch,
    ) -> InternalAuthorizationDecision: ...

    async def authorize_observe(
        self,
        record: RunRecord,
        principal: InvocationPrincipal,
        *,
        target_kind: Literal["run", "context"] = "run",
    ) -> InternalAuthorizationDecision: ...

    async def authorize_cancel(
        self,
        record: RunRecord,
        principal: InvocationPrincipal,
    ) -> InternalAuthorizationDecision: ...

    async def authorize_context_observe(
        self,
        thread_id: str,
        principal: InvocationPrincipal,
    ) -> InternalAuthorizationDecision: ...


class InvocationConstraints(Protocol):
    async def project(self, launch: PreparedLaunch) -> InternalConstraintDecision: ...


class InvocationAdmissionFence(Protocol):
    """Fail-closed deployment-safety check for genuinely new admissions."""

    async def ready_for_admission(self) -> bool: ...

    def admission_permit(self) -> AbstractAsyncContextManager[bool]: ...


class _DisabledInvocationAuthorization:
    async def authorize_start(self, _launch: PreparedLaunch) -> InternalAuthorizationDecision:
        return InternalAuthorizationDecision.allowed()

    async def authorize_observe(
        self,
        _record: RunRecord,
        _principal: InvocationPrincipal,
        *,
        target_kind: Literal["run", "context"] = "run",
    ) -> InternalAuthorizationDecision:
        return InternalAuthorizationDecision.allowed()

    async def authorize_cancel(
        self,
        _record: RunRecord,
        _principal: InvocationPrincipal,
    ) -> InternalAuthorizationDecision:
        return InternalAuthorizationDecision.allowed()

    async def authorize_context_observe(
        self,
        _thread_id: str,
        _principal: InvocationPrincipal,
    ) -> InternalAuthorizationDecision:
        return InternalAuthorizationDecision.allowed()


class _AbsentInvocationConstraints:
    async def project(self, _launch: PreparedLaunch) -> InternalConstraintDecision:
        return InternalConstraintDecision.absent()


def _merge_decision_evidence(
    accepted: AcceptedInvocation,
    evidence: Mapping[str, Any] | None,
) -> AcceptedInvocation:
    if evidence is None:
        return accepted
    current = dict(accepted.decision_evidence)
    incoming = dict(evidence)
    current_decisions = list(current.pop("decisions", ()) or ())
    incoming_decisions = list(incoming.pop("decisions", ()) or ())
    merged = {
        "version": 1,
        "decisions": [*current_decisions, *incoming_decisions],
        **current,
        **incoming,
    }
    return replace(accepted, decision_evidence=merged)


class InvocationRuntime:
    """Deep application module for launch, observation, and cancellation."""

    def __init__(
        self,
        *,
        normalizer: LaunchNormalizer,
        runs: DurableRuns,
        authorization: InvocationAuthorization | None = None,
        constraints: InvocationConstraints | None = None,
        admission_fence: InvocationAdmissionFence | None = None,
        task_factory: TaskFactory = asyncio.create_task,
    ) -> None:
        self._normalizer = normalizer
        self._runs = runs
        self._authorization = authorization or _DisabledInvocationAuthorization()
        self._constraints = constraints or _AbsentInvocationConstraints()
        self._admission_fence = admission_fence
        self._task_factory = task_factory

    @staticmethod
    def _rejection(
        decision: InternalAuthorizationDecision,
    ) -> InvocationAuthorizationOutcome | None:
        if decision.outcome is InvocationAuthorizationOutcome.allowed:
            return None
        return decision.outcome

    @asynccontextmanager
    async def _admission_permit(self):
        fence = self._admission_fence
        if fence is None:
            yield True
            return
        permit = getattr(fence, "admission_permit", None)
        if callable(permit):
            async with permit() as allowed:
                yield allowed
            return
        # Compatibility for host test doubles and older embedded adapters.
        yield await fence.ready_for_admission()

    async def _launch_absent(
        self,
        intent: InternalLaunchIntent,
        identity: InternalAdmissionIdentity | None,
        validate_replay,
    ) -> InternalLaunchReceipt | NotFoundOrInvisible | InvocationAuthorizationOutcome:
        launch = await self._normalizer.normalize(intent)
        start_decision = await self._authorization.authorize_start(launch)
        if rejection := self._rejection(start_decision):
            return rejection
        if start_decision.evidence is not None and launch.accepted_invocation is not None:
            launch = replace(
                launch,
                accepted_invocation=_merge_decision_evidence(
                    launch.accepted_invocation,
                    start_decision.evidence,
                ),
            )
        constraint_decision = await self._constraints.project(launch)
        if constraint_decision.outcome is InvocationConstraintOutcome.denied:
            return InvocationAuthorizationOutcome.denied
        if constraint_decision.outcome is InvocationConstraintOutcome.indeterminate:
            return InvocationAuthorizationOutcome.indeterminate
        if constraint_decision.evidence is not None and launch.accepted_invocation is not None:
            launch = replace(
                launch,
                accepted_invocation=_merge_decision_evidence(
                    launch.accepted_invocation,
                    constraint_decision.evidence,
                ),
            )
        async with self._runs.admission_scope(launch.thread_id):
            await self._runs.prepare_admission(launch)
            admitted = await self._runs.admit(launch)
            if isinstance(admitted, DurableAdmission):
                record = admitted.record
                if admitted.outcome is not AdmissionOutcome.created:
                    visible = await self._runs.observe(
                        record.run_id,
                        launch.principal,
                    )
                    if visible is None:
                        return NotFoundOrInvisible.not_found_or_invisible
                    observation = await self._authorization.authorize_observe(
                        visible,
                        launch.principal,
                    )
                    if rejection := self._rejection(observation):
                        return rejection
                    if identity is not None and callable(validate_replay):
                        await validate_replay(intent, identity, record)
                    return InternalLaunchReceipt(record=visible, created=False)
            else:
                record = admitted
            # Real-pod qualification barriers are inert unless the dedicated
            # test image is started with its explicit environment gate.
            from deerflow.runtime.kubernetes_qualification import (
                qualification_barrier,
                qualification_counter,
            )

            await qualification_barrier("accepted_before_worker_start", record)
            worker = launch.worker(record)
            try:
                record.task = self._task_factory(worker)
            except Exception as exc:
                close = getattr(worker, "close", None)
                if callable(close):
                    close()
                await self._runs.fail_start(
                    record,
                    f"Failed to attach run worker: {exc}",
                )
                raise
            await qualification_counter("worker_attachments", record)
            await qualification_barrier("accepted_before_client_response", record)
        return InternalLaunchReceipt(record=record, created=True)

    async def launch(
        self,
        intent: InternalLaunchIntent,
    ) -> InternalLaunchReceipt | NotFoundOrInvisible | InvocationAuthorizationOutcome:
        with self._normalizer.scope(intent):
            identity: InternalAdmissionIdentity | None = None
            identify = getattr(self._normalizer, "identify", None)
            find_existing = getattr(self._runs, "find_by_external_identity", None)
            validate_replay = getattr(self._normalizer, "validate_replay", None)
            if callable(identify) and callable(find_existing):
                identity = await identify(intent)
                if identity is not None:
                    existing = await find_existing(identity)
                    if existing is not None:
                        observation = await self._authorization.authorize_observe(
                            existing,
                            identity.principal,
                        )
                        if rejection := self._rejection(observation):
                            return rejection
                        if callable(validate_replay):
                            await validate_replay(intent, identity, existing)
                        return InternalLaunchReceipt(record=existing, created=False)

            async with self._admission_permit() as permitted:
                if not permitted:
                    return InvocationAuthorizationOutcome.indeterminate
                return await self._launch_absent(intent, identity, validate_replay)

    async def observe_run(
        self,
        run_id: str,
        principal: InvocationPrincipal,
    ) -> RunRecord | NotFoundOrInvisible | InvocationAuthorizationOutcome:
        record = await self._runs.observe(run_id, principal)
        if record is None:
            return NotFoundOrInvisible.not_found_or_invisible
        decision = await self._authorization.authorize_observe(record, principal)
        if rejection := self._rejection(decision):
            return rejection
        return record

    @staticmethod
    def _owner_scope(principal: InvocationPrincipal) -> str | None:
        if principal.visibility_prevalidated or principal.role == "admin":
            return None
        return lifecycle_owner_scope(principal.user_id)

    async def observe_invocation_lifecycle(
        self,
        query: InternalInvocationLifecycleQuery,
    ) -> InternalLifecycleObservation | NotFoundOrInvisible | InvocationAuthorizationOutcome:
        record = await self._runs.observe(query.run_id, query.principal)
        if record is None:
            return NotFoundOrInvisible.not_found_or_invisible
        decision = await self._authorization.authorize_observe(record, query.principal)
        if rejection := self._rejection(decision):
            return rejection
        page = await self._runs.query_lifecycle(
            LifecycleQuery(
                run_id=query.run_id,
                owner_scope=self._owner_scope(query.principal),
                cursor=query.cursor,
                limit=query.limit,
                # A singular observation always needs state from the same
                # read snapshot as its fence, even when the caller omits the
                # snapshot collection from the public response.
                include_snapshot=True,
            )
        )
        authoritative_snapshot = next(
            (snapshot for snapshot in page.snapshots if snapshot.get("run_id") == query.run_id),
            None,
        )
        if authoritative_snapshot is None:
            return NotFoundOrInvisible.not_found_or_invisible
        if not query.include_snapshot:
            page = replace(page, snapshots=(), summaries=())
        return InternalLifecycleObservation(
            record=record,
            page=page,
            authoritative_snapshot=authoritative_snapshot,
        )

    async def observe_context_lifecycle(
        self,
        query: InternalContextLifecycleQuery,
    ) -> InternalLifecycleObservation | NotFoundOrInvisible | InvocationAuthorizationOutcome:
        if not await self._runs.context_visible(query.thread_id, query.principal):
            return NotFoundOrInvisible.not_found_or_invisible
        decision = await self._authorization.authorize_context_observe(
            query.thread_id,
            query.principal,
        )
        if rejection := self._rejection(decision):
            return rejection
        page = await self._runs.query_lifecycle(
            LifecycleQuery(
                thread_id=query.thread_id,
                owner_scope=self._owner_scope(query.principal),
                cursor=query.cursor,
                limit=query.limit,
                include_snapshot=query.include_snapshot,
                source_kind=query.source_kind,
            )
        )
        return InternalLifecycleObservation(record=None, page=page)

    async def cancel_run(
        self,
        request: InternalCancelRequest,
    ) -> InternalCancelReceipt | NotFoundOrInvisible | InvocationAuthorizationOutcome:
        record = await self._runs.observe(request.run_id, request.principal)
        if record is None or (request.thread_id is not None and record.thread_id != request.thread_id):
            return NotFoundOrInvisible.not_found_or_invisible
        decision = await self._authorization.authorize_cancel(record, request.principal)
        if rejection := self._rejection(decision):
            return rejection
        outcome = await self._runs.cancel(request)
        fresh = await self._runs.observe(request.run_id, request.principal) if request.expected_state_version is not None else None
        return InternalCancelReceipt(outcome=outcome, record=fresh or record)
