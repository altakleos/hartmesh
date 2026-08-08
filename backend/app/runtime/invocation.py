"""Application-layer ownership of durable invocation sequencing."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine
from contextlib import AbstractAsyncContextManager, AbstractContextManager
from dataclasses import dataclass, field, replace
from enum import StrEnum
from typing import Any, Literal, Protocol

from deerflow_extension_api import ConstraintProjectionV1

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
    """Finite, host-internal request for one invocation."""

    thread_id: str
    assistant_id: str | None = None
    input: dict[str, Any] | None = None
    command: dict[str, Any] | None = None
    metadata: dict[str, Any] | None = None
    config: dict[str, Any] | None = None
    context: dict[str, Any] | None = None
    checkpoint_id: str | None = None
    checkpoint: dict[str, Any] | None = None
    interrupt_before: list[str] | Literal["*"] | None = None
    interrupt_after: list[str] | Literal["*"] | None = None
    stream_mode: list[str] | str | None = None
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
    """Normalized admission data plus the deferred worker factory."""

    thread_id: str
    assistant_id: str | None
    on_disconnect: DisconnectMode
    metadata: dict[str, Any]
    kwargs: dict[str, Any]
    multitask_strategy: str
    model_name: str | None
    user_id: str | None
    worker: WorkerFactory = field(repr=False)
    accepted_invocation: AcceptedInvocation | None = field(default=None, repr=False)
    external_scope: str | None = None
    external_key: str | None = None
    request_digest: str | None = None
    request_digest_version: str | None = None
    caller_intent_json: dict[str, Any] | None = None
    caller_intent_digest: str | None = None
    caller_intent_digest_version: str | None = None
    principal: InvocationPrincipal = field(default_factory=lambda: InvocationPrincipal())


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


def _constraint_projection_evidence(projection: ConstraintProjectionV1) -> dict[str, Any]:
    from deerflow.runtime.accepted_invocation import canonical_digest

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
    evidence: dict[str, Any] | None = None

    @classmethod
    def absent(cls) -> InternalConstraintDecision:
        return cls(InvocationConstraintOutcome.allowed)

    @classmethod
    def projected(cls, projection: ConstraintProjectionV1) -> InternalConstraintDecision:
        return cls(
            InvocationConstraintOutcome.allowed,
            evidence=_constraint_projection_evidence(projection),
        )

    @classmethod
    def projected_evidence(cls, evidence: dict[str, Any]) -> InternalConstraintDecision:
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
    evidence: dict[str, Any] | None = None

    @classmethod
    def allowed(cls, *, evidence: dict[str, Any] | None = None) -> InternalAuthorizationDecision:
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


@dataclass(frozen=True)
class InternalLifecycleObservation:
    record: RunRecord | None
    page: LifecyclePage
    authoritative_snapshot: dict[str, Any] | None = None


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
    evidence: dict[str, Any] | None,
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
        task_factory: TaskFactory = asyncio.create_task,
    ) -> None:
        self._normalizer = normalizer
        self._runs = runs
        self._authorization = authorization or _DisabledInvocationAuthorization()
        self._constraints = constraints or _AbsentInvocationConstraints()
        self._task_factory = task_factory

    @staticmethod
    def _rejection(
        decision: InternalAuthorizationDecision,
    ) -> InvocationAuthorizationOutcome | None:
        if decision.outcome is InvocationAuthorizationOutcome.allowed:
            return None
        return decision.outcome

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
                        # A concurrent creator may have bound server-resolved
                        # facts (notably a generated stateless thread) that made
                        # the provisional digest differ. Re-project against the
                        # winning row before deciding replay versus conflict.
                        if identity is not None and callable(validate_replay):
                            await validate_replay(intent, identity, record)
                        return InternalLaunchReceipt(record=visible, created=False)
                else:
                    # Compatibility for host adapters predating keyed ensure.
                    record = admitted
                # Keep attachment adjacent to durable admission: no await may
                # separate a successful admit from installing its worker task.
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
        return InternalLaunchReceipt(record=record, created=True)

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
            page = replace(page, snapshots=())
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
