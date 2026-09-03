"""Abstract interface for run metadata storage.

RunManager depends on this interface. Implementations:
- MemoryRunStore: in-memory dict (development, tests)
- Future: RunRepository backed by SQLAlchemy ORM

All methods accept an optional user_id for user isolation.
When user_id is None, no user filtering is applied (single-user mode).
"""

from __future__ import annotations

import abc
import hashlib
import json
import re
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Literal

from deerflow_extension_api import TenantReferenceV1

from deerflow.runtime.runs.lifecycle_query import (
    LifecyclePage,
    LifecycleQuery,
    LifecycleVisibilityScope,
)

_LIFECYCLE_EVIDENCE_KEY = re.compile(r"^[A-Za-z_][A-Za-z0-9_.-]{0,63}$")
_LIFECYCLE_SAFE_REASONS = {
    "agent_assembly_drift",
    "agent_revision_drift",
    "assembly_evidence_unavailable",
    "accepted_skill_execution_fence_failed",
    "accepted_skill_execution_lease_unavailable",
    "constraint_evidence_mismatch",
    "constraint_expired_before_start",
    "loop_capped",
    "model_length_capped",
    "orphan_recovered",
    "recovery_checkpoint_unavailable",
    "recovery_tool_attempt_indeterminate",
    "replacement",
    "rollback",
    "safety_capped",
    "scheduled_task_orphan_recovered",
    "subagent_limit_capped",
    "tenant_identity_mismatch",
    "token_capped",
    "worker_attachment_failed",
}


def tenant_store_columns(
    configured: TenantReferenceV1 | None,
    supplied: TenantReferenceV1 | None,
) -> tuple[str | None, str | None]:
    """Validate one typed tenant anchor and project it to storage columns."""

    if configured is not None and not isinstance(configured, TenantReferenceV1):
        raise TypeError("configured tenant must be TenantReferenceV1 or None")
    if supplied is not None and not isinstance(supplied, TenantReferenceV1):
        raise TypeError("supplied tenant must be TenantReferenceV1 or None")
    if configured is not None and supplied is not None and supplied != configured:
        from deerflow.runtime.tenant_identity import TenantIdentityError

        raise TenantIdentityError(
            "tenant_identity_mismatch",
            "run tenant differs from the process tenant identity",
        )
    effective = configured or supplied
    if effective is None:
        return None, None
    return effective.public_ref, effective.digest


@dataclass(frozen=True)
class EditReplayVisibility:
    hidden_source_run_ids: set[str] = field(default_factory=set)
    hidden_attempt_run_ids: set[str] = field(default_factory=set)


class LeaseClockAuthority(StrEnum):
    """Clock domain that mints durable run-lease deadlines."""

    process_v1 = "process_v1"
    database_v1 = "database_v1"


def validate_lease_deadline_request(
    *,
    lease_expires_at: str | None,
    lease_duration_seconds: int | None,
    required: bool,
) -> None:
    """Validate one absolute-or-duration lease deadline request."""

    if lease_expires_at is not None and lease_duration_seconds is not None:
        raise ValueError("lease_expires_at and lease_duration_seconds are mutually exclusive")
    if lease_duration_seconds is not None and (isinstance(lease_duration_seconds, bool) or not isinstance(lease_duration_seconds, int) or lease_duration_seconds <= 0):
        raise ValueError("lease_duration_seconds must be a positive integer")
    if required and lease_expires_at is None and lease_duration_seconds is None:
        raise ValueError("one of lease_expires_at or lease_duration_seconds is required")


@dataclass(frozen=True)
class LeaseRenewal:
    """Result of renewing a run lease.

    ``cancel_action`` carries a durable cancellation request to the owning
    worker without transferring lease ownership.
    """

    renewed: bool
    cancel_action: str | None = None
    lease_expires_at: str | None = None


@dataclass(frozen=True)
class StatusFinalization:
    """Result of completing a run only if cancellation has not won."""

    finalized: bool
    cancel_action: str | None = None


class AdmissionOutcome(StrEnum):
    """Durable keyed-admission outcome."""

    created = "created"
    known_same = "known_same"
    key_conflict = "key_conflict"


class RecoveryPolicy(StrEnum):
    """Server-owned recovery behavior frozen with one admitted run."""

    terminalize_v1 = "terminalize_v1"
    exact_two_takeover_v1 = "exact_two_takeover_v1"


class ExecutionTakeoverOutcome(StrEnum):
    """Finite result of attempting to transfer one expired execution lease."""

    claimed = "claimed"
    not_eligible = "not_eligible"


@dataclass(frozen=True)
class ExecutionTakeoverClaim:
    """A takeover decision and the authoritative row observed under its CAS."""

    outcome: ExecutionTakeoverOutcome
    row: dict[str, Any] | None = None


class BindAssemblyEvidenceOutcome(StrEnum):
    """Finite outcome of binding evidence under the current execution fence."""

    bound = "bound"
    already_matching = "already_matching"
    mismatch = "mismatch"
    ownership_lost = "ownership_lost"
    not_found = "not_found"


class DuplicateRunIdentityError(RuntimeError):
    """Raised when a candidate reuses an existing durable run identity."""

    def __init__(self, run_id: str) -> None:
        super().__init__(f"duplicate durable run identity: {run_id}")


class RecoveryPayloadIntegrityError(RuntimeError):
    """Raised when snapshot repair contradicts admission-owned recovery input."""

    def __init__(self) -> None:
        super().__init__("recovery_payload_integrity_conflict")


class ThreadOperationReleaseOutcome(StrEnum):
    """Finite result of an exact, owner-fenced auxiliary release."""

    released = "released"
    absent = "absent"
    inactive = "inactive"
    ownership_lost = "ownership_lost"
    identity_mismatch = "identity_mismatch"
    unsupported = "unsupported"


@dataclass(frozen=True)
class ThreadOperationReleaseResult:
    """Result of releasing one non-invocation thread reservation."""

    outcome: ThreadOperationReleaseOutcome


class LifecycleType(StrEnum):
    """The complete v1 authoritative invocation lifecycle vocabulary."""

    accepted = "accepted"
    started = "started"
    cancellation_requested = "cancellation_requested"
    cancelled = "cancelled"
    succeeded = "succeeded"
    failed = "failed"
    timed_out = "timed_out"
    interrupted = "interrupted"


_LIFECYCLE_TYPE_BY_STATUS = {
    "running": LifecycleType.started,
    "success": LifecycleType.succeeded,
    "error": LifecycleType.failed,
    "timeout": LifecycleType.timed_out,
    "interrupted": LifecycleType.interrupted,
}
_LIFECYCLE_STATUSES_BY_TYPE = {
    LifecycleType.accepted: frozenset({"pending"}),
    LifecycleType.started: frozenset({"running"}),
    LifecycleType.cancellation_requested: frozenset({"pending", "running"}),
    LifecycleType.cancelled: frozenset({"error", "interrupted"}),
    LifecycleType.succeeded: frozenset({"success"}),
    LifecycleType.failed: frozenset({"error"}),
    LifecycleType.timed_out: frozenset({"timeout"}),
    LifecycleType.interrupted: frozenset({"error", "interrupted"}),
}


def lifecycle_type_for_status(status: str) -> LifecycleType:
    """Return the ordinary lifecycle type for one persisted run status."""

    try:
        return _LIFECYCLE_TYPE_BY_STATUS[status]
    except KeyError as exc:
        raise ValueError(f"No lifecycle mapping for run status {status!r}") from exc


def validate_lifecycle_transition(transition: LifecycleTransition) -> None:
    """Reject evidence whose lifecycle meaning contradicts its row status."""

    allowed_statuses = _LIFECYCLE_STATUSES_BY_TYPE[transition.lifecycle_type]
    if transition.status not in allowed_statuses:
        raise ValueError(f"Lifecycle type {transition.lifecycle_type.value!r} cannot produce status {transition.status!r}")


@dataclass(frozen=True)
class LifecycleTransition:
    """One safe state mutation and its matching lifecycle evidence."""

    lifecycle_type: LifecycleType
    status: str
    error: str | None = None
    stop_reason: str | None = None
    reason: str | None = None
    evidence: dict[str, str | int | bool | None | list[str | int | bool | None]] = field(default_factory=dict)
    execution_evidence_json: dict[str, Any] | None = None
    execution_evidence_digest: str | None = None


@dataclass(frozen=True)
class LifecycleTransitionResult:
    applied: bool
    row: dict[str, Any] | None = None
    event: dict[str, Any] | None = None


class CancellationRequestOutcome(StrEnum):
    requested = "requested"
    already_requested = "already_requested"
    already_terminal = "already_terminal"
    stale = "stale"
    not_found_or_invisible = "not_found_or_invisible"


@dataclass(frozen=True)
class CancellationRequestResult:
    outcome: CancellationRequestOutcome
    row: dict[str, Any] | None = None
    event: dict[str, Any] | None = None


def lifecycle_owner_scope(user_id: str | None) -> str:
    """Create a bounded, non-identifying access scope for lifecycle evidence."""

    if user_id is None:
        return "unscoped"
    return f"user:sha256:{hashlib.sha256(user_id.encode('utf-8')).hexdigest()}"


def build_lifecycle_payload(transition: LifecycleTransition) -> dict[str, Any]:
    """Validate and project the deliberately small lifecycle v1 payload."""

    validate_lifecycle_transition(transition)
    payload: dict[str, Any] = {"version": 1}
    if (transition.execution_evidence_json is None) != (transition.execution_evidence_digest is None):
        raise ValueError("execution evidence and digest must be supplied together")
    if transition.execution_evidence_json is not None:
        if transition.lifecycle_type is not LifecycleType.started:
            raise ValueError("execution evidence is supported only for started")
        evidence = transition.execution_evidence_json
        neutral_evidence_digest: str | None = None
        if isinstance(evidence, dict):
            from deerflow.sandbox.accepted_material import (
                decode_accepted_execution_evidence,
            )

            try:
                neutral_evidence_digest = decode_accepted_execution_evidence(
                    evidence,
                ).digest
            except (TypeError, ValueError):
                pass
        v1_fields = {
            "version",
            "profile",
            "attempt_id",
            "snapshot_id",
            "run_id",
            "generation",
            "pod_uid",
            "lease_uid",
            "runtime_image_ids_digest",
            "verifier_receipt_digest",
            "materialization_evidence_digest",
        }
        v2_fields = v1_fields | {
            "pod_isolation_digest",
            "network_policy_uid",
            "network_policy_spec_digest",
            "evidence_secret_uid",
            "evidence_secret_digest",
            "capability_secret_uid",
            "capability_secret_digest",
            "sandbox_image_digest",
            "accepted_skill_runtime_image_digest",
        }
        evidence_fields = frozenset(evidence) if isinstance(evidence, dict) else frozenset()
        allowed_fields = {
            frozenset(v1_fields),
            frozenset(v2_fields),
        }
        legacy_evidence = evidence_fields in allowed_fields and type(evidence.get("version")) is int and evidence.get("version") in {1, 2}
        if not isinstance(evidence, dict) or (neutral_evidence_digest is None and not legacy_evidence):
            raise ValueError("execution evidence has invalid fields")
        encoded_evidence = json.dumps(
            evidence,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        if len(encoded_evidence) > 4096:
            raise ValueError("execution evidence exceeds 4096 UTF-8 bytes")
        evidence_digest = neutral_evidence_digest or hashlib.sha256(encoded_evidence).hexdigest()
        if evidence_digest != transition.execution_evidence_digest:
            raise ValueError("execution evidence digest mismatch")
        payload["execution_evidence_digest"] = evidence_digest
    if transition.reason is not None:
        if transition.reason not in _LIFECYCLE_SAFE_REASONS:
            raise ValueError(f"unsupported lifecycle reason: {transition.reason!r}")
        payload["reason"] = transition.reason
    if transition.evidence:
        if transition.lifecycle_type != LifecycleType.cancellation_requested:
            raise ValueError("lifecycle evidence is only supported for cancellation_requested")
        if len(transition.evidence) > 16:
            raise ValueError("lifecycle evidence exceeds 16 references")
        safe_evidence: dict[str, str | int | bool | None | list[str | int | bool | None]] = {}
        for key, value in transition.evidence.items():
            if not isinstance(key, str) or not _LIFECYCLE_EVIDENCE_KEY.fullmatch(key):
                raise ValueError(f"invalid lifecycle evidence key: {key!r}")
            if key != "action":
                raise ValueError(f"unsupported lifecycle evidence key: {key!r}")
            values = value if isinstance(value, list) else [value]
            for item in values:
                if not isinstance(item, (str, int, bool, type(None))):
                    raise ValueError("lifecycle evidence values must be safe scalars or lists")
                if isinstance(item, str) and len(item.encode("utf-8")) > 256:
                    raise ValueError("lifecycle evidence string exceeds 256 UTF-8 bytes")
            if value not in ("interrupt", "rollback"):
                raise ValueError("lifecycle action evidence must be interrupt or rollback")
            safe_evidence[key] = list(values) if isinstance(value, list) else value
        payload["evidence"] = safe_evidence
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    if len(encoded) > 4096:
        raise ValueError("lifecycle payload exceeds 4096 UTF-8 bytes")
    return payload


def validate_execution_evidence_run(
    run_id: str,
    evidence: dict[str, Any] | None,
) -> None:
    """Reject relationally valid evidence bound to a different run row."""

    if evidence is not None and evidence.get("run_id") != run_id:
        raise ValueError("execution evidence belongs to a different run")


@dataclass(frozen=True)
class RunEnsureResult:
    outcome: AdmissionOutcome
    row: dict[str, Any]
    claimed: tuple[dict[str, Any], ...] = ()


@dataclass(frozen=True)
class LifecycleReadiness:
    """Bounded structural health result for authoritative lifecycle storage."""

    ready: bool
    reason_code: Literal[
        "ready",
        "lifecycle_cursor_missing",
        "admission_cursor_state_invalid",
        "lifecycle_pruning_invalid",
        "lifecycle_event_cardinality_invalid",
        "lifecycle_event_bounds_invalid",
        "lifecycle_event_sequence_invalid",
        "lifecycle_store_unavailable",
    ] = "ready"

    def __post_init__(self) -> None:
        allowed = {
            "ready",
            "lifecycle_cursor_missing",
            "admission_cursor_state_invalid",
            "lifecycle_pruning_invalid",
            "lifecycle_event_cardinality_invalid",
            "lifecycle_event_bounds_invalid",
            "lifecycle_event_sequence_invalid",
            "lifecycle_store_unavailable",
        }
        if self.reason_code not in allowed:
            raise ValueError("invalid lifecycle readiness reason code")
        if self.ready != (self.reason_code == "ready"):
            raise ValueError("lifecycle readiness status and reason disagree")


class RunIdempotencyConflict(RuntimeError):
    """A run with the requested process-wide idempotency key already exists."""

    def __init__(self, existing: dict[str, Any]) -> None:
        super().__init__(f"Run idempotency key already belongs to {existing.get('run_id')}")
        self.existing = existing


class RunStore(abc.ABC):
    # Custom stores retain their compatibility behavior until they implement
    # the same row+journal transaction and explicitly override this flag.
    durable_lifecycle = False
    lease_clock_authority = LeaseClockAuthority.process_v1

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        # Inheriting implementation code is not proof that a custom store
        # preserves its backing system's row+journal transaction. Every
        # lifecycle-capable implementation opts in on its own class body.
        if "durable_lifecycle" not in cls.__dict__:
            cls.durable_lifecycle = False

    async def initialize_lifecycle(self) -> None:
        """Validate or initialize lifecycle cursor ordering state."""

    async def lifecycle_ready(self) -> bool:
        """Report whether lifecycle ordering metadata is internally coherent."""

        return (await self.lifecycle_readiness()).ready

    async def lifecycle_readiness(self) -> LifecycleReadiness:
        """Return one safe bounded structural readiness result."""

        return LifecycleReadiness(ready=True)

    async def list_lifecycle_events(
        self,
        *,
        run_id: str | None = None,
        thread_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """Internal inspection seam; no public cursor API is implied."""

        raise NotImplementedError

    async def query_lifecycle(self, query: LifecycleQuery) -> LifecyclePage:
        """Read one authorized lifecycle page from one consistent snapshot."""

        raise NotImplementedError

    async def context_visible_in_scope(
        self,
        thread_id: str,
        scope: LifecycleVisibilityScope,
    ) -> bool:
        """Return whether one exact context has a row inside a finite scope."""

        raise NotImplementedError

    async def prune_lifecycle_through(self, cursor: str) -> str:
        """Administratively prune a committed global lifecycle prefix."""

        raise NotImplementedError

    async def transition_run_atomic(
        self,
        run_id: str,
        *,
        expected_state_version: int,
        expected_statuses: tuple[str, ...] | None,
        transition: LifecycleTransition,
        user_id: str | None = None,
    ) -> LifecycleTransitionResult:
        """Compare-and-set one normal run and append its evidence atomically."""

        raise NotImplementedError

    async def transition_owned_run_atomic(
        self,
        run_id: str,
        *,
        expected_state_version: int,
        expected_statuses: tuple[str, ...] | None,
        transition: LifecycleTransition,
        expected_owner_worker_id: str,
        require_unexpired_lease: bool,
        user_id: str | None = None,
    ) -> LifecycleTransitionResult:
        """Transition only while the expected worker still owns the run."""

        raise NotImplementedError

    @asynccontextmanager
    async def hold_execution_fence(
        self,
        run_id: str,
        *,
        owner_worker_id: str,
        state_version: int,
        terminal_state_version: int | None = None,
        allowed_active_statuses: tuple[str, ...] = ("running",),
    ) -> AsyncIterator[bool]:
        """Hold an ownership fence across one external durable mutation.

        Implementations must serialize ownership changes with the protected
        mutation for the full context lifetime. Stores without that guarantee
        fail closed so a stale process never receives mutation authority.
        """

        del run_id, owner_worker_id, state_version, terminal_state_version, allowed_active_statuses
        yield False

    async def bind_assembly_evidence(
        self,
        run_id: str,
        *,
        owner_id: str,
        lease_epoch: int,
        evidence_json: Mapping[str, object],
        evidence_digest: str,
    ) -> BindAssemblyEvidenceOutcome:
        """Bind or compare one V1 record under the active execution fence.

        Custom stores are not assumed to implement the atomicity contract.
        The compatibility default therefore fails closed instead of attempting
        a worker-side read followed by a write.
        """

        del run_id, owner_id, lease_epoch, evidence_json, evidence_digest
        return BindAssemblyEvidenceOutcome.ownership_lost

    async def request_cancel_fenced(
        self,
        run_id: str,
        *,
        action: str,
        expected_state_version: int,
        user_id: str | None = None,
    ) -> CancellationRequestResult:
        """Record a version-fenced cancellation request with precise precedence."""

        raise NotImplementedError

    async def request_cancel_compat(
        self,
        run_id: str,
        *,
        action: str,
        user_id: str | None = None,
    ) -> CancellationRequestResult:
        """Atomically record the first request for the existing unfenced API."""

        winning = await self.request_cancel(run_id, action=action)
        if winning is None:
            return CancellationRequestResult(CancellationRequestOutcome.already_terminal)
        outcome = CancellationRequestOutcome.requested if winning == action else CancellationRequestOutcome.stale
        return CancellationRequestResult(outcome)

    async def request_cancel_owned(
        self,
        run_id: str,
        *,
        action: str,
        expected_owner_worker_id: str,
        require_unexpired_lease: bool,
        user_id: str | None = None,
    ) -> CancellationRequestResult:
        """Request cancellation only while the expected worker owns the run."""

        raise NotImplementedError

    @abc.abstractmethod
    async def put(
        self,
        run_id: str,
        *,
        thread_id: str,
        assistant_id: str | None = None,
        user_id: str | None = None,
        model_name: str | None = None,
        status: str = "pending",
        operation_kind: str = "run",
        multitask_strategy: str = "reject",
        metadata: dict[str, Any] | None = None,
        kwargs: dict[str, Any] | None = None,
        error: str | None = None,
        stop_reason: str | None = None,
        created_at: str | None = None,
        owner_worker_id: str | None = None,
        lease_expires_at: str | None = None,
        origin_json: dict[str, Any] | None = None,
        principal_projection_json: dict[str, Any] | None = None,
        principal_projection_digest: str | None = None,
        base_origin_digest: str | None = None,
        accepted_context_digest: str | None = None,
        tenant: TenantReferenceV1 | None = None,
        agent_revision_json: dict[str, Any] | None = None,
        agent_revision_digest: str | None = None,
        extension_generation: int | None = None,
        decision_evidence_json: dict[str, Any] | None = None,
        external_scope: str | None = None,
        external_key: str | None = None,
        request_digest: str | None = None,
        request_digest_version: str | None = None,
        caller_intent_json: dict[str, Any] | None = None,
        caller_intent_digest: str | None = None,
        caller_intent_digest_version: str | None = None,
        idempotency_key: str | None = None,
        recovery_policy: RecoveryPolicy = RecoveryPolicy.terminalize_v1,
        recovery_payload_json: dict[str, Any] | None = None,
    ) -> None:
        pass

    @abc.abstractmethod
    async def get(
        self,
        run_id: str,
        *,
        user_id: str | None = None,
    ) -> dict[str, Any] | None:
        pass

    async def authoritative_get(self, run_id: str) -> dict[str, Any] | None:
        """Return global primary-key truth for trusted integrity recovery.

        This privileged lookup deliberately bypasses owner visibility. Normal
        request, observation, and authorization paths must use :meth:`get`.
        Stores that cannot provide global primary-key truth fail closed by
        leaving post-commit integrity quarantine unresolved.
        """

        raise NotImplementedError

    async def thread_projection_authorized(
        self,
        *,
        run_id: str,
        thread_id: str,
        run_status: str,
        owner_worker_id: str,
        active_state_version: int,
        terminal_state_version: int | None = None,
    ) -> bool:
        """Validate one current run authority for a derived thread projection.

        Compatibility stores fail closed. The projection store remains
        responsible for making its own write atomic with this decision when
        cross-process correctness is required.
        """

        del (
            run_id,
            thread_id,
            run_status,
            owner_worker_id,
            active_state_version,
            terminal_state_version,
        )
        return False

    @asynccontextmanager
    async def hold_thread_projection_authority(
        self,
        *,
        run_id: str,
        thread_id: str,
        run_status: str,
        owner_worker_id: str,
        active_state_version: int,
        terminal_state_version: int | None = None,
    ) -> AsyncIterator[bool]:
        """Hold a process-local projection decision through its derived write.

        Cross-process repositories make the decision and projection in one
        database transaction instead. Compatibility stores fail closed so a
        caller cannot accidentally turn a read-only authorization check into
        an atomicity claim.
        """

        del (
            run_id,
            thread_id,
            run_status,
            owner_worker_id,
            active_state_version,
            terminal_state_version,
        )
        yield False

    async def get_by_external_identity(
        self,
        external_scope: str,
        external_key: str,
    ) -> dict[str, Any] | None:
        """Return the normal run bound to one normalized external identity."""

        raise NotImplementedError

    async def get_by_idempotency_key(
        self,
        idempotency_key: str,
    ) -> dict[str, Any] | None:
        """Return tenant-local truth for one process-wide idempotency key."""

        raise NotImplementedError

    @abc.abstractmethod
    async def list_by_thread(
        self,
        thread_id: str,
        *,
        user_id: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        pass

    async def list_successful_regenerate_sources(
        self,
        thread_id: str,
        *,
        user_id: str | None = None,
    ) -> set[str]:
        """Return source run IDs superseded by successful regenerations.

        Implementations must inspect the complete thread and must not apply the
        normal bounded run-list limit.
        """
        raise NotImplementedError

    async def list_edit_regenerate_runs(
        self,
        thread_id: str,
        *,
        user_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """Return all edit-regenerate attempt runs for one thread, oldest first."""
        raise NotImplementedError

    async def get_many_by_thread(
        self,
        thread_id: str,
        run_ids: set[str],
        *,
        user_id: str | None = None,
    ) -> dict[str, dict[str, Any]]:
        """Batch-load selected runs belonging to one thread."""
        raise NotImplementedError

    @abc.abstractmethod
    async def update_status(
        self,
        run_id: str,
        status: str,
        *,
        error: str | None = None,
        stop_reason: str | None = None,
    ) -> bool | None:
        """Update a run status.

        Returns ``False`` when the store can prove no row was updated. Older or
        lightweight stores may return ``None`` when they cannot report rowcount.
        """
        pass

    @abc.abstractmethod
    async def start_run(
        self,
        run_id: str,
        *,
        execution_evidence_json: dict[str, Any] | None = None,
        execution_evidence_digest: str | None = None,
    ) -> bool:
        """Atomically transition a pending run to running.

        Returns ``False`` when the row is missing or no longer pending.
        """
        pass

    @abc.abstractmethod
    async def delete(self, run_id: str) -> None:
        pass

    async def delete_thread_operation(self, run_id: str, *, user_id: str | None) -> None:
        """Release an admitted thread operation for its recorded owner.

        The default keeps legacy stores compatible: older implementations only
        accepted ``run_id``. User-aware stores should override this method so
        cleanup never depends on ambient request context.
        """
        await self.delete(run_id)

    async def release_thread_operation_owned(
        self,
        run_id: str,
        *,
        thread_id: str,
        operation_kind: str,
        user_id: str | None,
        expected_owner_worker_id: str,
        require_unexpired_lease: bool,
    ) -> ThreadOperationReleaseResult:
        """Release one exact auxiliary reservation without a legacy delete fallback.

        Stores that cannot enforce every supplied identity and ownership fence fail
        closed with ``unsupported``.  In particular, this default never delegates to
        :meth:`delete_thread_operation`, whose legacy contract is too weak for
        post-commit compensation.
        """

        del (
            run_id,
            thread_id,
            operation_kind,
            user_id,
            expected_owner_worker_id,
            require_unexpired_lease,
        )
        return ThreadOperationReleaseResult(
            outcome=ThreadOperationReleaseOutcome.unsupported,
        )

    @abc.abstractmethod
    async def update_model_name(
        self,
        run_id: str,
        model_name: str | None,
        *,
        expected_owner_worker_id: str | None = None,
        expected_state_version: int | None = None,
        require_unexpired_lease: bool = False,
    ) -> bool | None:
        """Update ``model_name`` under an optional owner/epoch fence.

        ``expected_owner_worker_id`` and ``expected_state_version`` form one
        authority capability and must be supplied together. Implementations
        return ``False`` when a supplied fence no longer matches. Omitting the
        fence preserves the legacy contract only for a row with no lease;
        actively leased rows fail closed. Compatibility stores may return
        ``None`` when they cannot report application.
        """
        pass

    @abc.abstractmethod
    async def update_run_completion(
        self,
        run_id: str,
        *,
        status: str,
        expected_owner_worker_id: str | None = None,
        expected_active_state_version: int | None = None,
        expected_terminal_state_version: int | None = None,
        total_input_tokens: int = 0,
        total_output_tokens: int = 0,
        total_tokens: int = 0,
        llm_call_count: int = 0,
        lead_agent_tokens: int = 0,
        subagent_tokens: int = 0,
        middleware_tokens: int = 0,
        token_usage_by_model: dict[str, dict[str, int]] | None = None,
        message_count: int = 0,
        last_ai_message: str | None = None,
        first_human_message: str | None = None,
        error: str | None = None,
    ) -> bool | None:
        """Project completion fields onto an already-terminal row.

        Implementations must never transition status.  The three expected
        values form one terminal authority capability and must be supplied
        together for a row terminalized by an owned durable lifecycle CAS.
        Returns ``False`` when the row or exact terminal projection differs.
        """
        pass

    async def update_run_progress(
        self,
        run_id: str,
        *,
        expected_owner_worker_id: str | None = None,
        expected_state_version: int | None = None,
        require_unexpired_lease: bool = False,
        total_input_tokens: int | None = None,
        total_output_tokens: int | None = None,
        total_tokens: int | None = None,
        llm_call_count: int | None = None,
        lead_agent_tokens: int | None = None,
        subagent_tokens: int | None = None,
        middleware_tokens: int | None = None,
        token_usage_by_model: dict[str, dict[str, int]] | None = None,
        message_count: int | None = None,
        last_ai_message: str | None = None,
        first_human_message: str | None = None,
    ) -> bool | None:
        """Persist a running snapshot under an optional owner/epoch fence.

        ``expected_owner_worker_id`` and ``expected_state_version`` form one
        authority capability and must be supplied together. Implementations
        return ``False`` when a supplied fence no longer matches. Omitting the
        fence preserves the legacy contract only for a row with no lease;
        actively leased rows fail closed. Compatibility stores may return
        ``None`` when they cannot report application.
        """
        return None

    @abc.abstractmethod
    async def list_pending(self, *, before: str | None = None) -> list[dict[str, Any]]:
        pass

    @abc.abstractmethod
    async def list_inflight(self, *, before: str | None = None) -> list[dict[str, Any]]:
        """Return persisted runs that are still ``pending`` or ``running``."""
        pass

    @abc.abstractmethod
    async def aggregate_tokens_by_thread(self, thread_id: str, *, include_active: bool = False) -> dict[str, Any]:
        """Aggregate token usage for completed runs in a thread.

        Returns a dict with keys: total_tokens, total_input_tokens,
        total_output_tokens, total_runs, by_model (model_name → {tokens, runs}),
        by_caller ({lead_agent, subagent, middleware}).
        """
        pass

    @abc.abstractmethod
    async def update_lease(
        self,
        run_id: str,
        *,
        owner_worker_id: str,
        lease_expires_at: str | None = None,
        lease_duration_seconds: int | None = None,
    ) -> bool:
        """Renew the lease on an active run. Returns ``False`` when no row matched."""
        pass

    async def renew_lease(
        self,
        run_id: str,
        *,
        owner_worker_id: str,
        lease_expires_at: str | None = None,
        lease_duration_seconds: int | None = None,
    ) -> LeaseRenewal:
        """Renew ownership and return any durable cancellation request.

        The default wraps the legacy ``update_lease`` method and returns no
        cancellation action, so third-party stores remain source-compatible
        without adding a background read. Stores that support multi-process
        cancellation must override this method to renew and observe the
        request atomically.
        """
        validate_lease_deadline_request(
            lease_expires_at=lease_expires_at,
            lease_duration_seconds=lease_duration_seconds,
            required=True,
        )
        if lease_duration_seconds is None:
            renewed = await self.update_lease(
                run_id,
                owner_worker_id=owner_worker_id,
                lease_expires_at=lease_expires_at,
            )
        else:
            renewed = await self.update_lease(
                run_id,
                owner_worker_id=owner_worker_id,
                lease_duration_seconds=lease_duration_seconds,
            )
        return LeaseRenewal(
            renewed=renewed,
            lease_expires_at=lease_expires_at if renewed else None,
        )

    async def execution_owner_authorized(
        self,
        run_id: str,
        *,
        owner_worker_id: str,
        state_version: int,
    ) -> bool:
        """Return whether one execution owner epoch is currently authoritative."""

        del run_id, owner_worker_id, state_version
        return False

    async def request_cancel(self, run_id: str, *, action: str) -> str | None:
        """Persist the first cancellation action for an active run.

        Implementations must update only ``pending`` or ``running`` rows and
        return the winning action, or ``None`` when no active row matched.
        """
        raise NotImplementedError

    async def finalize_if_not_cancelled(
        self,
        run_id: str,
        *,
        status: str,
        error: str | None = None,
        stop_reason: str | None = None,
        expected_owner_worker_id: str | None = None,
        expected_state_version: int | None = None,
        require_unexpired_lease: bool = False,
    ) -> StatusFinalization:
        """Atomically finalize an active run unless cancellation won.

        The compatibility default is safe for stores that do not implement
        durable cancellation.
        """
        if expected_owner_worker_id is not None or expected_state_version is not None:
            return StatusFinalization(finalized=False)
        updated = await self.update_status(
            run_id,
            status,
            error=error,
            stop_reason=stop_reason,
        )
        return StatusFinalization(finalized=updated is not False)

    @abc.abstractmethod
    async def claim_for_takeover(
        self,
        run_id: str,
        *,
        grace_seconds: int,
        error: str,
        stop_reason: str | None = None,
        expected_state_version: int | None = None,
    ) -> bool:
        """Atomically mark an expired-lease active run as ``error``.

        Only ``terminalize_v1`` rows whose lease has expired past
        *grace_seconds* (or whose lease is NULL — pre-ownership data) are
        updated. ``exact_two_takeover_v1`` rows fail closed and use the
        dedicated execution-takeover primitive instead. The conditional
        policy, status, lease, and optional version fence close races with
        heartbeat, cancellation, and terminal writers. When provided,
        *stop_reason* is persisted in the same atomic update.

        Returns ``False`` when:
          - the run is no longer ``pending`` / ``running``,
          - the run's immutable policy is ``exact_two_takeover_v1``,
          - the lease is still valid (owner heartbeat is alive), or
          - the row doesn't exist.
        """
        pass

    async def claim_for_execution_takeover(
        self,
        run_id: str,
        *,
        new_owner_worker_id: str,
        lease_expires_at: str | None = None,
        lease_duration_seconds: int | None = None,
        grace_seconds: int,
        expected_state_version: int,
    ) -> ExecutionTakeoverClaim:
        """Transfer an expired exact-two run lease without terminalizing it.

        Compatibility stores fail closed. Implementations must atomically
        verify the admitted recovery policy, active status, expired lease,
        absent cancellation, and expected epoch before assigning the new owner
        and incrementing ``state_version``.
        """

        del (
            run_id,
            new_owner_worker_id,
            lease_expires_at,
            lease_duration_seconds,
            grace_seconds,
            expected_state_version,
        )
        return ExecutionTakeoverClaim(ExecutionTakeoverOutcome.not_eligible)

    @abc.abstractmethod
    async def list_inflight_with_expired_lease(
        self,
        *,
        before: str | None = None,
        grace_seconds: int = 10,
    ) -> list[dict[str, Any]]:
        """Return active runs whose lease has expired (or is NULL for pre-ownership rows)."""
        pass

    async def create_thread_operation_atomic(
        self,
        run_id: str,
        *,
        thread_id: str,
        owner_worker_id: str,
        lease_expires_at: str | None = None,
        lease_duration_seconds: int | None = None,
        operation_kind: str = "run",
        multitask_strategy: str = "reject",
        assistant_id: str | None = None,
        user_id: str | None = None,
        model_name: str | None = None,
        metadata: dict[str, Any] | None = None,
        kwargs: dict[str, Any] | None = None,
        created_at: str | None = None,
        grace_seconds: int = 10,
        origin_json: dict[str, Any] | None = None,
        principal_projection_json: dict[str, Any] | None = None,
        principal_projection_digest: str | None = None,
        base_origin_digest: str | None = None,
        accepted_context_digest: str | None = None,
        tenant: TenantReferenceV1 | None = None,
        agent_revision_json: dict[str, Any] | None = None,
        agent_revision_digest: str | None = None,
        extension_generation: int | None = None,
        decision_evidence_json: dict[str, Any] | None = None,
        external_scope: str | None = None,
        external_key: str | None = None,
        request_digest: str | None = None,
        request_digest_version: str | None = None,
        caller_intent_json: dict[str, Any] | None = None,
        caller_intent_digest: str | None = None,
        caller_intent_digest_version: str | None = None,
        idempotency_key: str | None = None,
        recovery_policy: RecoveryPolicy = RecoveryPolicy.terminalize_v1,
        recovery_payload_json: dict[str, Any] | None = None,
        require_predecessor_inactive: bool = False,
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        """Atomically create an active thread operation with cross-process uniqueness.

        The default implementation preserves compatibility with stores that
        still implement the former ``create_run_atomic`` interface. Legacy
        stores support only normal run rows; internal operation kinds require
        an implementation of this method.

        Returns ``(new_run_dict, claimed_run_dicts)``.
        Raises ``IntegrityError`` on conflict for ``reject`` strategy.
        When ``require_predecessor_inactive`` is true, replacement strategies
        must reject an active predecessor instead of terminalizing it. This is
        the fail-closed boundary used when predecessor delivery evidence lives
        in a separate transaction domain.
        """
        legacy_impl = type(self).create_run_atomic
        if legacy_impl is RunStore.create_run_atomic:
            raise NotImplementedError("RunStore must implement create_thread_operation_atomic() or create_run_atomic()")
        if operation_kind != "run":
            raise NotImplementedError("Legacy RunStore.create_run_atomic() cannot create non-run thread operations")
        if lease_duration_seconds is not None:
            raise NotImplementedError("Legacy RunStore.create_run_atomic() cannot mint duration-based leases")
        if require_predecessor_inactive and multitask_strategy in {
            "interrupt",
            "rollback",
        }:
            from deerflow.runtime.runs.manager import ConflictError

            raise ConflictError(
                f"Thread {thread_id} requires explicit predecessor cancellation",
            )
        if any(
            value is not None
            for value in (
                origin_json,
                principal_projection_json,
                principal_projection_digest,
                base_origin_digest,
                accepted_context_digest,
                tenant,
                agent_revision_json,
                agent_revision_digest,
                extension_generation,
                decision_evidence_json,
                external_scope,
                external_key,
                request_digest,
                request_digest_version,
                caller_intent_json,
                caller_intent_digest,
                caller_intent_digest_version,
                idempotency_key,
                recovery_payload_json,
            )
        ):
            raise NotImplementedError("Legacy RunStore.create_run_atomic() cannot persist accepted invocation or idempotency facts")
        return await self.create_run_atomic(
            run_id,
            thread_id=thread_id,
            owner_worker_id=owner_worker_id,
            lease_expires_at=lease_expires_at,
            multitask_strategy=multitask_strategy,
            assistant_id=assistant_id,
            user_id=user_id,
            model_name=model_name,
            metadata=metadata,
            kwargs=kwargs,
            created_at=created_at,
            grace_seconds=grace_seconds,
        )

    async def ensure_run_atomic(
        self,
        run_id: str,
        *,
        thread_id: str,
        owner_worker_id: str,
        lease_expires_at: str | None = None,
        lease_duration_seconds: int | None = None,
        external_scope: str,
        external_key: str,
        request_digest: str,
        request_digest_version: str,
        caller_intent_json: dict[str, Any],
        caller_intent_digest: str,
        caller_intent_digest_version: str,
        multitask_strategy: str = "reject",
        assistant_id: str | None = None,
        user_id: str | None = None,
        model_name: str | None = None,
        metadata: dict[str, Any] | None = None,
        kwargs: dict[str, Any] | None = None,
        created_at: str | None = None,
        grace_seconds: int = 10,
        origin_json: dict[str, Any] | None = None,
        principal_projection_json: dict[str, Any] | None = None,
        principal_projection_digest: str | None = None,
        base_origin_digest: str | None = None,
        accepted_context_digest: str | None = None,
        tenant: TenantReferenceV1 | None = None,
        agent_revision_json: dict[str, Any] | None = None,
        agent_revision_digest: str | None = None,
        extension_generation: int | None = None,
        decision_evidence_json: dict[str, Any] | None = None,
        recovery_policy: RecoveryPolicy = RecoveryPolicy.terminalize_v1,
        recovery_payload_json: dict[str, Any] | None = None,
        require_predecessor_inactive: bool = False,
    ) -> RunEnsureResult:
        """Atomically ensure one keyed normal run.

        Implementations return only the three external-identity outcomes.
        Independent active-thread conflicts remain exceptions.
        """

        raise NotImplementedError

    async def create_run_atomic(
        self,
        run_id: str,
        *,
        thread_id: str,
        owner_worker_id: str,
        lease_expires_at: str | None = None,
        lease_duration_seconds: int | None = None,
        multitask_strategy: str = "reject",
        assistant_id: str | None = None,
        user_id: str | None = None,
        model_name: str | None = None,
        metadata: dict[str, Any] | None = None,
        kwargs: dict[str, Any] | None = None,
        created_at: str | None = None,
        grace_seconds: int = 10,
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        """Deprecated compatibility alias for normal-run admission."""
        operation_impl = type(self).create_thread_operation_atomic
        if operation_impl is RunStore.create_thread_operation_atomic:
            raise NotImplementedError("RunStore must implement create_thread_operation_atomic() or create_run_atomic()")
        call_kwargs: dict[str, Any] = {
            "thread_id": thread_id,
            "owner_worker_id": owner_worker_id,
            "lease_expires_at": lease_expires_at,
            "operation_kind": "run",
            "multitask_strategy": multitask_strategy,
            "assistant_id": assistant_id,
            "user_id": user_id,
            "model_name": model_name,
            "metadata": metadata,
            "kwargs": kwargs,
            "created_at": created_at,
            "grace_seconds": grace_seconds,
        }
        if lease_duration_seconds is not None:
            call_kwargs["lease_duration_seconds"] = lease_duration_seconds
        return await self.create_thread_operation_atomic(run_id, **call_kwargs)
