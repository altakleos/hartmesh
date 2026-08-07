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
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from deerflow.runtime.runs.lifecycle_query import LifecyclePage, LifecycleQuery

_LIFECYCLE_EVIDENCE_KEY = re.compile(r"^[A-Za-z_][A-Za-z0-9_.-]{0,63}$")
_LIFECYCLE_SAFE_REASONS = {
    "agent_revision_drift",
    "constraint_evidence_mismatch",
    "constraint_expired_before_start",
    "loop_capped",
    "model_length_capped",
    "orphan_recovered",
    "replacement",
    "rollback",
    "safety_capped",
    "subagent_limit_capped",
    "token_capped",
    "worker_attachment_failed",
}


@dataclass(frozen=True)
class EditReplayVisibility:
    hidden_source_run_ids: set[str] = field(default_factory=set)
    hidden_attempt_run_ids: set[str] = field(default_factory=set)


@dataclass(frozen=True)
class LeaseRenewal:
    """Result of renewing a run lease.

    ``cancel_action`` carries a durable cancellation request to the owning
    worker without transferring lease ownership.
    """

    renewed: bool
    cancel_action: str | None = None


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


@dataclass(frozen=True)
class RunEnsureResult:
    outcome: AdmissionOutcome
    row: dict[str, Any]
    claimed: tuple[dict[str, Any], ...] = ()


class RunStore(abc.ABC):
    # Custom stores retain their compatibility behavior until they implement
    # the same row+journal transaction and explicitly override this flag.
    durable_lifecycle = False

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        # Inheriting implementation code is not proof that a custom store
        # preserves its backing system's row+journal transaction. Every
        # lifecycle-capable implementation opts in on its own class body.
        if "durable_lifecycle" not in cls.__dict__:
            cls.durable_lifecycle = False

    async def initialize_lifecycle(self) -> None:
        """Validate or initialize lifecycle cursor ordering state."""

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
        agent_revision_json: dict[str, Any] | None = None,
        agent_revision_digest: str | None = None,
        extension_generation: int | None = None,
        decision_evidence_json: dict[str, Any] | None = None,
        external_scope: str | None = None,
        external_key: str | None = None,
        request_digest: str | None = None,
        request_digest_version: str | None = None,
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

    async def get_by_external_identity(
        self,
        external_scope: str,
        external_key: str,
    ) -> dict[str, Any] | None:
        """Return the normal run bound to one normalized external identity."""

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
    async def start_run(self, run_id: str) -> bool:
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

    @abc.abstractmethod
    async def update_model_name(
        self,
        run_id: str,
        model_name: str | None,
    ) -> None:
        """Update the model_name field for an existing run."""
        pass

    @abc.abstractmethod
    async def update_run_completion(
        self,
        run_id: str,
        *,
        status: str,
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
        """Persist final completion fields.

        Implementations must not replace a different terminal status. Returns
        ``False`` when the row is missing or already has a conflicting terminal
        outcome.
        """
        pass

    async def update_run_progress(
        self,
        run_id: str,
        *,
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
    ) -> None:
        """Persist a best-effort running snapshot without changing run status."""
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
        lease_expires_at: str,
    ) -> bool:
        """Renew the lease on an active run. Returns ``False`` when no row matched."""
        pass

    async def renew_lease(
        self,
        run_id: str,
        *,
        owner_worker_id: str,
        lease_expires_at: str,
    ) -> LeaseRenewal:
        """Renew ownership and return any durable cancellation request.

        The default wraps the legacy ``update_lease`` method and returns no
        cancellation action, so third-party stores remain source-compatible
        without adding a background read. Stores that support multi-process
        cancellation must override this method to renew and observe the
        request atomically.
        """
        renewed = await self.update_lease(
            run_id,
            owner_worker_id=owner_worker_id,
            lease_expires_at=lease_expires_at,
        )
        return LeaseRenewal(renewed=renewed)

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
    ) -> StatusFinalization:
        """Atomically finalize an active run unless cancellation won.

        The compatibility default is safe for stores that do not implement
        durable cancellation.
        """
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

        Only rows whose lease has expired past *grace_seconds* (or whose
        lease is NULL — pre-ownership data) are updated. The conditional
        status, lease, and optional version fence close races with heartbeat,
        cancellation, and terminal writers. When provided, *stop_reason* is
        persisted in the same atomic update.

        Returns ``False`` when:
          - the run is no longer ``pending`` / ``running``,
          - the lease is still valid (owner heartbeat is alive), or
          - the row doesn't exist.
        """
        pass

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
        lease_expires_at: str | None,
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
        agent_revision_json: dict[str, Any] | None = None,
        agent_revision_digest: str | None = None,
        extension_generation: int | None = None,
        decision_evidence_json: dict[str, Any] | None = None,
        external_scope: str | None = None,
        external_key: str | None = None,
        request_digest: str | None = None,
        request_digest_version: str | None = None,
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        """Atomically create an active thread operation with cross-process uniqueness.

        The default implementation preserves compatibility with stores that
        still implement the former ``create_run_atomic`` interface. Legacy
        stores support only normal run rows; internal operation kinds require
        an implementation of this method.

        Returns ``(new_run_dict, claimed_run_dicts)``.
        Raises ``IntegrityError`` on conflict for ``reject`` strategy.
        """
        legacy_impl = type(self).create_run_atomic
        if legacy_impl is RunStore.create_run_atomic:
            raise NotImplementedError("RunStore must implement create_thread_operation_atomic() or create_run_atomic()")
        if operation_kind != "run":
            raise NotImplementedError("Legacy RunStore.create_run_atomic() cannot create non-run thread operations")
        if any(
            value is not None
            for value in (
                origin_json,
                principal_projection_json,
                principal_projection_digest,
                base_origin_digest,
                accepted_context_digest,
                agent_revision_json,
                agent_revision_digest,
                extension_generation,
                decision_evidence_json,
                external_scope,
                external_key,
                request_digest,
                request_digest_version,
            )
        ):
            raise NotImplementedError("Legacy RunStore.create_run_atomic() cannot persist accepted invocation facts")
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
        lease_expires_at: str | None,
        external_scope: str,
        external_key: str,
        request_digest: str,
        request_digest_version: str,
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
        agent_revision_json: dict[str, Any] | None = None,
        agent_revision_digest: str | None = None,
        extension_generation: int | None = None,
        decision_evidence_json: dict[str, Any] | None = None,
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
        lease_expires_at: str | None,
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
        return await self.create_thread_operation_atomic(
            run_id,
            thread_id=thread_id,
            owner_worker_id=owner_worker_id,
            lease_expires_at=lease_expires_at,
            operation_kind="run",
            multitask_strategy=multitask_strategy,
            assistant_id=assistant_id,
            user_id=user_id,
            model_name=model_name,
            metadata=metadata,
            kwargs=kwargs,
            created_at=created_at,
            grace_seconds=grace_seconds,
        )
