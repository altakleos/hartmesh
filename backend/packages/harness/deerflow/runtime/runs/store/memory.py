"""In-memory RunStore. Used when database.backend=memory (default) and in tests.

Equivalent to the original RunManager._runs dict behavior.
"""

from __future__ import annotations

import asyncio
import copy
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from functools import wraps
from typing import Any

from deerflow_extension_api import (
    TenantReferenceV1,
    validate_model_profile_identifier,
    validate_thread_identifier,
)

from deerflow.runtime.assembly_evidence import (
    AssemblyEvidenceError,
    AssemblyEvidenceV1,
    assembly_evidence_binding_matches,
    assembly_evidence_digest,
)
from deerflow.runtime.runs.lifecycle_query import (
    CursorAhead,
    LifecyclePage,
    LifecycleQuery,
    LifecycleVisibilityScope,
    build_invocation_summary,
    decode_lifecycle_cursor,
    encode_lifecycle_cursor,
    invocation_source_kind,
    validate_cursor_window,
)
from deerflow.runtime.runs.store.base import (
    AdmissionOutcome,
    BindAssemblyEvidenceOutcome,
    CancellationRequestOutcome,
    CancellationRequestResult,
    DuplicateRunIdentityError,
    ExecutionTakeoverClaim,
    ExecutionTakeoverOutcome,
    LeaseClockAuthority,
    LeaseRenewal,
    LifecycleTransition,
    LifecycleTransitionResult,
    LifecycleType,
    RecoveryPayloadIntegrityError,
    RecoveryPolicy,
    RunEnsureResult,
    RunIdempotencyConflict,
    RunStore,
    StatusFinalization,
    ThreadOperationReleaseOutcome,
    ThreadOperationReleaseResult,
    build_lifecycle_payload,
    lifecycle_owner_scope,
    lifecycle_type_for_status,
    tenant_store_columns,
    validate_execution_evidence_run,
    validate_lease_deadline_request,
)
from deerflow.runtime.tenant_identity import TenantIdentityError
from deerflow.utils.time import is_lease_expired

_TERMINAL_STATUSES = {"success", "error", "timeout", "interrupted"}


def _lease_expired_at(
    lease_expires_at: str | None,
    *,
    observed_at: datetime,
    grace_seconds: int,
) -> bool:
    if lease_expires_at is None:
        return True
    try:
        deadline = datetime.fromisoformat(lease_expires_at)
    except (TypeError, ValueError):
        return True
    if deadline.tzinfo is None:
        deadline = deadline.replace(tzinfo=UTC)
    return deadline <= observed_at - timedelta(seconds=grace_seconds)


def _process_lease_deadline(
    *,
    lease_expires_at: str | None,
    lease_duration_seconds: int | None,
    observed_at: datetime,
    required: bool,
) -> str | None:
    validate_lease_deadline_request(
        lease_expires_at=lease_expires_at,
        lease_duration_seconds=lease_duration_seconds,
        required=required,
    )
    if lease_duration_seconds is not None:
        return (observed_at + timedelta(seconds=lease_duration_seconds)).isoformat()
    return lease_expires_at


class _ReentrantMutationLock:
    """Task-reentrant lock shared by run mutation and derived projections."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._owner: asyncio.Task[Any] | None = None
        self._depth = 0

    @asynccontextmanager
    async def hold(self) -> AsyncIterator[None]:
        task = asyncio.current_task()
        if task is None:
            raise RuntimeError("memory run mutation requires an asyncio task")
        if self._owner is task:
            self._depth += 1
        else:
            await self._lock.acquire()
            self._owner = task
            self._depth = 1
        try:
            yield
        finally:
            self._depth -= 1
            if self._depth == 0:
                self._owner = None
                self._lock.release()


def _atomic_memory_mutation[**P, R](method: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
    @wraps(method)
    async def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
        self = args[0]
        async with self._mutation_lock.hold():
            snapshot = (
                copy.deepcopy(self._runs),
                copy.deepcopy(self._runs_by_thread),
                copy.deepcopy(self._runs_by_external_identity),
                copy.deepcopy(self._lifecycle_events),
                self._lifecycle_cursor,
                self._lifecycle_pruned_through,
                self._admission_cursor,
            )
            try:
                # Match the SQL repository boundary: callers receive a
                # detached materialization, never a reference into the
                # authoritative in-memory row or its nested evidence.
                return copy.deepcopy(await method(*args, **kwargs))
            except BaseException:
                (
                    self._runs,
                    self._runs_by_thread,
                    self._runs_by_external_identity,
                    self._lifecycle_events,
                    self._lifecycle_cursor,
                    self._lifecycle_pruned_through,
                    self._admission_cursor,
                ) = snapshot
                raise

    return wrapped


def _locked_memory_mutation[**P, R](method: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
    """Serialize one simple mutation with held execution/projection fences."""

    @wraps(method)
    async def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
        self = args[0]
        async with self._mutation_lock.hold():
            return copy.deepcopy(await method(*args, **kwargs))

    return wrapped


class MemoryRunStore(RunStore):
    durable_lifecycle = True
    lease_clock_authority = LeaseClockAuthority.process_v1

    def __init__(self, *, tenant: TenantReferenceV1 | None = None) -> None:
        if tenant is not None and not isinstance(tenant, TenantReferenceV1):
            raise TypeError("tenant must be TenantReferenceV1 or None")
        self._tenant = tenant
        self._runs: dict[str, dict[str, Any]] = {}
        # Secondary index: thread_id -> insertion-ordered run_id set (a dict is
        # used as an ordered set), maintained in lockstep with ``_runs`` so
        # per-thread queries avoid O(total in-memory runs) full scans. Mirrors
        # the index ``RunManager`` keeps over its own in-memory records.
        self._runs_by_thread: dict[str, dict[str, None]] = {}
        self._runs_by_external_identity: dict[tuple[str, str], str] = {}
        self._lifecycle_events: list[dict[str, Any]] = []
        self._lifecycle_cursor = 0
        self._lifecycle_pruned_through = 0
        self._admission_cursor = 0
        self._mutation_lock = _ReentrantMutationLock()

    def _next_admission_cursor(self) -> int:
        self._admission_cursor += 1
        return self._admission_cursor

    def _tenant_visible(self, row: Mapping[str, Any]) -> bool:
        return self._tenant is None or row.get("tenant_digest") == self._tenant.digest

    def _visible_run(self, run_id: str) -> dict[str, Any] | None:
        row = self._runs.get(run_id)
        return row if row is not None and self._tenant_visible(row) else None

    async def initialize_lifecycle(self) -> None:
        return None

    async def list_lifecycle_events(
        self,
        *,
        run_id: str | None = None,
        thread_id: str | None = None,
    ) -> list[dict[str, Any]]:
        return [copy.deepcopy(event) for event in self._lifecycle_events if self._tenant_visible(event) and (run_id is None or event["run_id"] == run_id) and (thread_id is None or event["thread_id"] == thread_id)]

    async def query_lifecycle(self, query: LifecycleQuery) -> LifecyclePage:
        requested = validate_cursor_window(
            query.cursor,
            pruned_through=self._lifecycle_pruned_through,
            last_cursor=self._lifecycle_cursor,
        )
        window: list[dict[str, Any]] = []
        start_index = requested - self._lifecycle_pruned_through
        for index in range(start_index, len(self._lifecycle_events)):
            event = self._lifecycle_events[index]
            if not (
                self._tenant_visible(event)
                and requested < event["cursor"] <= self._lifecycle_cursor
                and (query.run_id is None or event["run_id"] == query.run_id)
                and (query.thread_id is None or event["thread_id"] == query.thread_id)
                and (query.owner_scope is None or event["owner_scope"] == query.owner_scope)
                and (query.source_kind is None or invocation_source_kind(self._runs.get(event["run_id"], {})) == query.source_kind)
                and (
                    query.visibility_scope is None
                    or query.visibility_scope.permits(
                        run_id=event["run_id"],
                        owner_id=self._runs.get(event["run_id"], {}).get("user_id"),
                        source_kind=invocation_source_kind(self._runs.get(event["run_id"], {})),
                    )
                )
            ):
                continue
            window.append(copy.deepcopy(event))
            if len(window) > query.limit:
                break
        has_more = len(window) > query.limit
        events = window[: query.limit]
        summaries: list[dict[str, Any]] = []
        snapshots: list[dict[str, Any]] = []
        if query.include_snapshot:
            if query.run_id is not None:
                summary_run_ids = (query.run_id,)
            else:
                summary_run_ids = tuple(dict.fromkeys(event["run_id"] for event in events))
            for run_id in summary_run_ids:
                row = self._visible_run(run_id)
                if row is None or row.get("operation_kind", "run") != "run":
                    continue
                if query.owner_scope is not None and lifecycle_owner_scope(row.get("user_id")) != query.owner_scope:
                    continue
                if query.source_kind is not None and invocation_source_kind(row) != query.source_kind:
                    continue
                summary = build_invocation_summary(row)
                if summary is not None:
                    summaries.append(copy.deepcopy(summary))
                snapshots.append(
                    {
                        "run_id": row["run_id"],
                        "thread_id": row["thread_id"],
                        "status": row["status"],
                        "state_version": row["state_version"],
                    }
                )
        next_value = events[-1]["cursor"] if has_more else self._lifecycle_cursor
        return LifecyclePage(
            snapshots=tuple(snapshots),
            events=tuple(events),
            next_cursor=encode_lifecycle_cursor(next_value),
            minimum_available_cursor=encode_lifecycle_cursor(self._lifecycle_pruned_through),
            read_fence_cursor=encode_lifecycle_cursor(self._lifecycle_cursor),
            summaries=tuple(summaries),
        )

    async def context_visible_in_scope(
        self,
        thread_id: str,
        scope: LifecycleVisibilityScope,
    ) -> bool:
        if scope.thread_id != thread_id:
            raise ValueError("lifecycle visibility scope is bound to another context")
        for run_id in self._runs_by_thread.get(thread_id, {}):
            row = self._visible_run(run_id)
            if row is None or row.get("operation_kind", "run") != "run":
                continue
            if scope.permits(
                run_id=run_id,
                owner_id=row.get("user_id"),
                source_kind=invocation_source_kind(row),
            ):
                return True
        return False

    @_atomic_memory_mutation
    async def prune_lifecycle_through(self, cursor: str) -> str:
        requested = decode_lifecycle_cursor(cursor)
        if requested > self._lifecycle_cursor:
            raise CursorAhead(encode_lifecycle_cursor(self._lifecycle_cursor))
        if requested > self._lifecycle_pruned_through:
            self._lifecycle_events = [event for event in self._lifecycle_events if event["cursor"] > requested]
            self._lifecycle_pruned_through = requested
        return encode_lifecycle_cursor(self._lifecycle_pruned_through)

    def _append_lifecycle_event(
        self,
        row: dict[str, Any],
        transition: LifecycleTransition,
        *,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self._lifecycle_cursor += 1
        event = {
            "event_id": str(uuid.uuid4()),
            "cursor": self._lifecycle_cursor,
            "run_id": row["run_id"],
            "thread_id": row["thread_id"],
            "owner_scope": lifecycle_owner_scope(row.get("user_id")),
            "tenant_ref": row.get("tenant_ref"),
            "tenant_digest": row.get("tenant_digest"),
            "lifecycle_type": transition.lifecycle_type,
            "state_version": row["state_version"],
            "status": row["status"],
            "created_at": datetime.now(UTC).isoformat(),
            "payload": payload if payload is not None else build_lifecycle_payload(transition),
        }
        self._lifecycle_events.append(event)
        return event

    @_atomic_memory_mutation
    async def transition_run_atomic(
        self,
        run_id: str,
        *,
        expected_state_version: int,
        expected_statuses: tuple[str, ...] | None,
        transition: LifecycleTransition,
        user_id: str | None = None,
    ) -> LifecycleTransitionResult:
        return self._transition_run_atomic(
            run_id,
            expected_state_version=expected_state_version,
            expected_statuses=expected_statuses,
            transition=transition,
            user_id=user_id,
        )

    @_atomic_memory_mutation
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
        return self._transition_run_atomic(
            run_id,
            expected_state_version=expected_state_version,
            expected_statuses=expected_statuses,
            transition=transition,
            user_id=user_id,
            expected_owner_worker_id=expected_owner_worker_id,
            require_unexpired_lease=require_unexpired_lease,
        )

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
        """Hold process-local ownership authority across one external mutation."""

        if not allowed_active_statuses or any(status not in {"pending", "running"} for status in allowed_active_statuses):
            raise ValueError("allowed_active_statuses must contain active run states")
        async with self._mutation_lock.hold():
            row = self._visible_run(run_id)
            active = bool(
                row is not None
                and row.get("operation_kind", "run") == "run"
                and terminal_state_version is not None
                and row.get("state_version") == terminal_state_version
                and row.get("status") in _TERMINAL_STATUSES
                and row.get("terminal_projection_owner_worker_id") == owner_worker_id
                and row.get("terminal_projection_active_state_version") == state_version
                and row.get("owner_worker_id") is None
                and row.get("lease_expires_at") is None
            )
            if row is not None and row.get("operation_kind", "run") == "run" and row.get("status") in allowed_active_statuses and row.get("owner_worker_id") == owner_worker_id and row.get("state_version") == state_version:
                lease_expires_at = row.get("lease_expires_at")
                active = lease_expires_at is None or not is_lease_expired(
                    lease_expires_at,
                    grace_seconds=0,
                )
            yield active

    def _transition_run_atomic(
        self,
        run_id: str,
        *,
        expected_state_version: int,
        expected_statuses: tuple[str, ...] | None,
        transition: LifecycleTransition,
        user_id: str | None,
        expected_owner_worker_id: str | None = None,
        require_unexpired_lease: bool = False,
    ) -> LifecycleTransitionResult:
        payload = build_lifecycle_payload(transition)
        row = self._visible_run(run_id)
        if row is None or row.get("operation_kind", "run") != "run":
            return LifecycleTransitionResult(applied=False)
        if user_id is not None and row.get("user_id") != user_id:
            return LifecycleTransitionResult(applied=False)
        if expected_owner_worker_id is not None and not self._owned_run_fence_matches(
            row,
            expected_owner_worker_id=expected_owner_worker_id,
            require_unexpired_lease=require_unexpired_lease,
        ):
            return LifecycleTransitionResult(applied=False, row=row)
        if row["state_version"] != expected_state_version:
            return LifecycleTransitionResult(applied=False, row=row)
        if expected_statuses is not None and row["status"] not in expected_statuses:
            return LifecycleTransitionResult(applied=False, row=row)
        if transition.status in _TERMINAL_STATUSES:
            row["terminal_projection_owner_worker_id"] = row.get("owner_worker_id")
            row["terminal_projection_active_state_version"] = row["state_version"] if row.get("owner_worker_id") is not None else None
        else:
            row["terminal_projection_owner_worker_id"] = None
            row["terminal_projection_active_state_version"] = None
        row["status"] = transition.status
        row["state_version"] += 1
        if transition.status in _TERMINAL_STATUSES:
            row["owner_worker_id"] = None
            row["lease_expires_at"] = None
        if transition.error is not None:
            row["error"] = transition.error
        if transition.stop_reason is not None:
            row["stop_reason"] = transition.stop_reason
        if transition.execution_evidence_json is not None:
            row["execution_evidence_json"] = transition.execution_evidence_json
            row["execution_evidence_digest"] = transition.execution_evidence_digest
        row["updated_at"] = datetime.now(UTC).isoformat()
        event = self._append_lifecycle_event(row, transition, payload=payload)
        return LifecycleTransitionResult(applied=True, row=row, event=event)

    @staticmethod
    def _owned_run_fence_matches(
        row: dict[str, Any],
        *,
        expected_owner_worker_id: str,
        require_unexpired_lease: bool,
    ) -> bool:
        if row.get("owner_worker_id") != expected_owner_worker_id:
            return False
        if not require_unexpired_lease:
            return True
        lease_expires_at = row.get("lease_expires_at")
        if not isinstance(lease_expires_at, str):
            return False
        try:
            deadline = datetime.fromisoformat(lease_expires_at)
        except ValueError:
            return False
        if deadline.tzinfo is None:
            deadline = deadline.replace(tzinfo=UTC)
        return deadline > datetime.now(UTC)

    @_atomic_memory_mutation
    async def bind_assembly_evidence(
        self,
        run_id: str,
        *,
        owner_id: str,
        lease_epoch: int,
        evidence_json: Mapping[str, object],
        evidence_digest: str,
    ) -> BindAssemblyEvidenceOutcome:
        actual = AssemblyEvidenceV1.from_persisted_json(evidence_json)
        if assembly_evidence_digest(actual) != evidence_digest:
            raise AssemblyEvidenceError("assembly_descriptor_invalid")
        normalized = actual.to_persisted_json()

        row = self._visible_run(run_id)
        if row is None or row.get("operation_kind", "run") != "run":
            return BindAssemblyEvidenceOutcome.not_found
        if (
            row.get("status") != "running"
            or row.get("state_version") != lease_epoch
            or not self._owned_run_fence_matches(
                row,
                expected_owner_worker_id=owner_id,
                require_unexpired_lease=row.get("lease_expires_at") is not None,
            )
        ):
            return BindAssemblyEvidenceOutcome.ownership_lost

        stored_json = row.get("assembly_evidence_json")
        stored_digest = row.get("assembly_evidence_digest")
        if stored_json is None and stored_digest is None:
            row["assembly_evidence_json"] = copy.deepcopy(normalized)
            row["assembly_evidence_digest"] = evidence_digest
            row["updated_at"] = datetime.now(UTC).isoformat()
            return BindAssemblyEvidenceOutcome.bound
        if stored_json is None or stored_digest is None:
            return BindAssemblyEvidenceOutcome.mismatch
        if assembly_evidence_binding_matches(
            actual,
            actual_digest=evidence_digest,
            persisted_json=stored_json,
            persisted_digest=stored_digest,
        ):
            return BindAssemblyEvidenceOutcome.already_matching
        return BindAssemblyEvidenceOutcome.mismatch

    @_atomic_memory_mutation
    async def request_cancel_fenced(
        self,
        run_id: str,
        *,
        action: str,
        expected_state_version: int,
        user_id: str | None = None,
    ) -> CancellationRequestResult:
        return self._request_cancel_atomic(
            run_id,
            action=action,
            expected_state_version=expected_state_version,
            user_id=user_id,
        )

    @_atomic_memory_mutation
    async def request_cancel_compat(
        self,
        run_id: str,
        *,
        action: str,
        user_id: str | None = None,
    ) -> CancellationRequestResult:
        return self._request_cancel_atomic(run_id, action=action, user_id=user_id)

    @_atomic_memory_mutation
    async def request_cancel_owned(
        self,
        run_id: str,
        *,
        action: str,
        expected_owner_worker_id: str,
        require_unexpired_lease: bool,
        user_id: str | None = None,
    ) -> CancellationRequestResult:
        return self._request_cancel_atomic(
            run_id,
            action=action,
            user_id=user_id,
            expected_owner_worker_id=expected_owner_worker_id,
            require_unexpired_lease=require_unexpired_lease,
        )

    def _request_cancel_atomic(
        self,
        run_id: str,
        *,
        action: str,
        expected_state_version: int | None = None,
        user_id: str | None = None,
        expected_owner_worker_id: str | None = None,
        require_unexpired_lease: bool = False,
    ) -> CancellationRequestResult:
        if action not in ("interrupt", "rollback"):
            raise ValueError(f"Unsupported cancellation action: {action}")
        row = self._visible_run(run_id)
        if row is None or row.get("operation_kind", "run") != "run" or (user_id is not None and row.get("user_id") != user_id):
            return CancellationRequestResult(CancellationRequestOutcome.not_found_or_invisible)
        if expected_owner_worker_id is not None and not self._owned_run_fence_matches(
            row,
            expected_owner_worker_id=expected_owner_worker_id,
            require_unexpired_lease=require_unexpired_lease,
        ):
            return CancellationRequestResult(CancellationRequestOutcome.stale)
        if row.get("cancel_action") == action:
            return CancellationRequestResult(CancellationRequestOutcome.already_requested, row=row)
        if row["status"] in _TERMINAL_STATUSES:
            return CancellationRequestResult(CancellationRequestOutcome.already_terminal, row=row)
        if row.get("cancel_action") is not None or (expected_state_version is not None and row["state_version"] != expected_state_version):
            return CancellationRequestResult(CancellationRequestOutcome.stale, row=row)
        row["cancel_action"] = action
        row["cancel_requested_at"] = datetime.now(UTC).isoformat()
        row["state_version"] += 1
        row["updated_at"] = datetime.now(UTC).isoformat()
        transition = LifecycleTransition(
            lifecycle_type=LifecycleType.cancellation_requested,
            status=row["status"],
            evidence={"action": action},
        )
        event = self._append_lifecycle_event(row, transition)
        return CancellationRequestResult(CancellationRequestOutcome.requested, row=row, event=event)

    def _index_run(self, run_id: str, thread_id: str) -> None:
        """Register *run_id* under *thread_id* in the secondary index."""
        self._runs_by_thread.setdefault(thread_id, {})[run_id] = None

    def _unindex_run(self, run_id: str, thread_id: str) -> None:
        """Drop *run_id* from the *thread_id* bucket, removing the bucket when empty."""
        bucket = self._runs_by_thread.get(thread_id)
        if bucket is not None:
            bucket.pop(run_id, None)
            if not bucket:
                self._runs_by_thread.pop(thread_id, None)

    @_atomic_memory_mutation
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
        thread_id = validate_thread_identifier(thread_id)
        tenant_ref, tenant_digest = tenant_store_columns(self._tenant, tenant)
        if model_name is not None:
            model_name = validate_model_profile_identifier(model_name, field_name="run model_name profile identifier")
        now = datetime.now(UTC).isoformat()
        recovery_policy = RecoveryPolicy(recovery_policy)
        if operation_kind != "run" and recovery_policy is not RecoveryPolicy.terminalize_v1:
            raise ValueError("execution recovery policy applies only to normal runs")
        if (recovery_policy is RecoveryPolicy.exact_two_takeover_v1) != (recovery_payload_json is not None):
            raise ValueError("execution recovery policy and payload must be admitted together")
        existing = self._runs.get(run_id)
        if existing is not None and not self._tenant_visible(existing):
            raise TenantIdentityError(
                "tenant_identity_mismatch",
                "run identity is already bound to a different tenant",
            )
        normalized_recovery_payload = copy.deepcopy(recovery_payload_json) if operation_kind == "run" else None
        if existing is not None and existing.get("operation_kind", "run") == "run":
            if (
                existing.get(
                    "recovery_policy",
                    RecoveryPolicy.terminalize_v1.value,
                )
                != recovery_policy.value
                or existing.get("recovery_payload_json") != normalized_recovery_payload
            ):
                raise RecoveryPayloadIntegrityError()
        lifecycle_row = operation_kind == "run" and status is not None
        terminal_statuses = {
            "success",
            "error",
            "timeout",
            "interrupted",
        }
        terminal_status = status in terminal_statuses
        new_row = {
            "run_id": run_id,
            "admission_cursor": (existing.get("admission_cursor") if existing is not None else self._next_admission_cursor()),
            "thread_id": thread_id,
            "assistant_id": assistant_id,
            "user_id": user_id,
            "model_name": model_name,
            "status": "pending" if lifecycle_row else status,
            "operation_kind": operation_kind,
            "recovery_policy": recovery_policy.value,
            "recovery_payload_json": (normalized_recovery_payload),
            "multitask_strategy": multitask_strategy,
            "metadata": metadata or {},
            "kwargs": kwargs or {},
            "error": error,
            "stop_reason": stop_reason,
            "tenant_ref": tenant_ref,
            "tenant_digest": tenant_digest,
            "created_at": created_at or now,
            "updated_at": now,
            "owner_worker_id": owner_worker_id,
            "lease_expires_at": lease_expires_at,
            "terminal_projection_owner_worker_id": (existing.get("terminal_projection_owner_worker_id") if existing else None),
            "terminal_projection_active_state_version": (existing.get("terminal_projection_active_state_version") if existing else None),
            "origin_json": origin_json if operation_kind == "run" else None,
            "principal_projection_json": principal_projection_json if operation_kind == "run" else None,
            "principal_projection_digest": principal_projection_digest if operation_kind == "run" else None,
            "base_origin_digest": base_origin_digest if operation_kind == "run" else None,
            "accepted_context_digest": accepted_context_digest if operation_kind == "run" else None,
            "agent_revision_json": agent_revision_json if operation_kind == "run" else None,
            "agent_revision_digest": agent_revision_digest if operation_kind == "run" else None,
            "extension_generation": extension_generation if operation_kind == "run" else None,
            "decision_evidence_json": decision_evidence_json if operation_kind == "run" else None,
            "external_scope": external_scope if operation_kind == "run" else None,
            "external_key": external_key if operation_kind == "run" else None,
            "request_digest": request_digest if operation_kind == "run" else None,
            "request_digest_version": request_digest_version if operation_kind == "run" else None,
            "caller_intent_json": caller_intent_json if operation_kind == "run" else None,
            "caller_intent_digest": caller_intent_digest if operation_kind == "run" else None,
            "caller_intent_digest_version": caller_intent_digest_version if operation_kind == "run" else None,
            "execution_evidence_json": None,
            "execution_evidence_digest": None,
            "assembly_evidence_json": existing.get("assembly_evidence_json") if existing else None,
            "assembly_evidence_digest": existing.get("assembly_evidence_digest") if existing else None,
            "idempotency_key": idempotency_key,
            # ``put`` is an idempotent snapshot write. Preserve a cancellation
            # request that may have raced a retry of an earlier snapshot.
            "cancel_action": existing.get("cancel_action") if existing else None,
            "cancel_requested_at": existing.get("cancel_requested_at") if existing else None,
            "state_version": existing.get("state_version", 0) if existing else (1 if lifecycle_row else 0),
        }
        if existing is not None:
            # Snapshot repair must never overwrite authoritative lifecycle
            # state. Dedicated transition primitives own status/version.
            new_row["status"] = existing["status"]
            new_row["state_version"] = existing.get("state_version", 0)
            if existing.get("operation_kind", "run") == "run":
                new_row["error"] = existing.get("error")
                new_row["stop_reason"] = existing.get("stop_reason")
                new_row["owner_worker_id"] = existing.get("owner_worker_id")
                new_row["lease_expires_at"] = existing.get("lease_expires_at")
                new_row["recovery_policy"] = existing.get(
                    "recovery_policy",
                    RecoveryPolicy.terminalize_v1.value,
                )
                new_row["recovery_payload_json"] = copy.deepcopy(existing.get("recovery_payload_json"))
        self._runs[run_id] = new_row
        self._index_run(run_id, thread_id)
        if operation_kind == "run" and external_scope is not None and external_key is not None:
            self._runs_by_external_identity[(external_scope, external_key)] = run_id
        if existing is None and lifecycle_row:
            self._append_lifecycle_event(
                new_row,
                LifecycleTransition(lifecycle_type=LifecycleType.accepted, status="pending"),
            )
            if status != "pending":
                if terminal_status:
                    new_row["terminal_projection_owner_worker_id"] = new_row.get("owner_worker_id")
                    new_row["terminal_projection_active_state_version"] = new_row["state_version"] if new_row.get("owner_worker_id") is not None else None
                new_row["status"] = status
                new_row["state_version"] += 1
                if terminal_status:
                    new_row["owner_worker_id"] = None
                    new_row["lease_expires_at"] = None
                self._append_lifecycle_event(
                    new_row,
                    LifecycleTransition(
                        lifecycle_type=lifecycle_type_for_status(status),
                        status=status,
                        error=error,
                        stop_reason=stop_reason,
                        reason=stop_reason,
                    ),
                )
        elif existing is not None and existing["status"] != status:
            if operation_kind != "run":
                new_row["status"] = status
                if terminal_status:
                    new_row["owner_worker_id"] = None
                    new_row["lease_expires_at"] = None
        elif existing is None and terminal_status:
            new_row["owner_worker_id"] = None
            new_row["lease_expires_at"] = None
        if new_row["status"] in terminal_statuses:
            new_row["owner_worker_id"] = None
            new_row["lease_expires_at"] = None

    async def get(self, run_id, *, user_id=None):
        run = self._visible_run(run_id)
        if run is None:
            return None
        if user_id is not None and run.get("user_id") != user_id:
            return None
        return copy.deepcopy(run)

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
        row = self._visible_run(run_id)
        if row is None or row.get("operation_kind", "run") != "run" or row.get("thread_id") != thread_id or row.get("status") != run_status:
            return False
        normal_rows = [candidate for candidate in self._runs.values() if self._tenant_visible(candidate) and candidate.get("operation_kind", "run") == "run" and candidate.get("thread_id") == thread_id]
        if (
            not normal_rows
            or any(candidate.get("admission_cursor") is None for candidate in normal_rows)
            or row.get("admission_cursor") is None
            or max(
                normal_rows,
                key=lambda candidate: candidate["admission_cursor"],
            ).get("run_id")
            != run_id
        ):
            return False
        if terminal_state_version is None:
            return bool(
                run_status == "running"
                and row.get("owner_worker_id") == owner_worker_id
                and row.get("state_version") == active_state_version
                and (
                    row.get("lease_expires_at") is None
                    or not is_lease_expired(
                        row.get("lease_expires_at"),
                        grace_seconds=0,
                    )
                )
            )
        return bool(
            run_status in _TERMINAL_STATUSES
            and terminal_state_version > active_state_version
            and row.get("state_version") == terminal_state_version
            and row.get("terminal_projection_owner_worker_id") == owner_worker_id
            and row.get("terminal_projection_active_state_version") == active_state_version
            and row.get("owner_worker_id") is None
            and row.get("lease_expires_at") is None
        )

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
        async with self._mutation_lock.hold():
            yield await self.thread_projection_authorized(
                run_id=run_id,
                thread_id=thread_id,
                run_status=run_status,
                owner_worker_id=owner_worker_id,
                active_state_version=active_state_version,
                terminal_state_version=terminal_state_version,
            )

    async def authoritative_get(self, run_id: str) -> dict[str, Any] | None:
        """Return one row by primary identity without applying owner scope."""

        return copy.deepcopy(self._visible_run(run_id))

    async def get_by_external_identity(
        self,
        external_scope: str,
        external_key: str,
    ) -> dict[str, Any] | None:
        run_id = self._runs_by_external_identity.get((external_scope, external_key))
        return copy.deepcopy(self._visible_run(run_id)) if run_id is not None else None

    async def get_by_idempotency_key(
        self,
        idempotency_key: str,
    ) -> dict[str, Any] | None:
        for row in self._runs.values():
            if self._tenant_visible(row) and row.get("idempotency_key") == idempotency_key:
                return copy.deepcopy(row)
        return None

    async def list_by_thread(self, thread_id, *, user_id=None, limit=100):
        # Use the thread index for an O(runs-in-thread) lookup instead of
        # scanning every run. ``self._runs.get`` is defense-in-depth: it drops a
        # stale id still in the index but already gone from ``_runs``.
        run_ids = self._runs_by_thread.get(thread_id)
        if not run_ids:
            return []
        results = [run for run_id in run_ids if (run := self._visible_run(run_id)) is not None and run.get("operation_kind", "run") == "run" and (user_id is None or run.get("user_id") == user_id)]
        results.sort(key=lambda r: r["created_at"], reverse=True)
        return copy.deepcopy(results[:limit])

    async def list_successful_regenerate_sources(self, thread_id, *, user_id=None):
        run_ids = self._runs_by_thread.get(thread_id) or ()
        sources: set[str] = set()
        for run_id in run_ids:
            run = self._visible_run(run_id)
            if run is None or run.get("operation_kind", "run") != "run" or run.get("status") != "success":
                continue
            if user_id is not None and run.get("user_id") != user_id:
                continue
            source = (run.get("metadata") or {}).get("regenerate_from_run_id")
            if isinstance(source, str) and source:
                sources.add(source)
        return sources

    async def list_edit_regenerate_runs(self, thread_id, *, user_id=None):
        run_ids = self._runs_by_thread.get(thread_id) or ()
        results = []
        for run_id in run_ids:
            run = self._visible_run(run_id)
            if run is None:
                continue
            if user_id is not None and run.get("user_id") != user_id:
                continue
            metadata = run.get("metadata") or {}
            source = metadata.get("regenerate_from_run_id")
            if metadata.get("replay_kind") == "edit" and isinstance(source, str) and source:
                results.append(run)
        results.sort(key=lambda r: r["created_at"])
        return copy.deepcopy(results)

    async def get_many_by_thread(self, thread_id, run_ids, *, user_id=None):
        thread_run_ids = self._runs_by_thread.get(thread_id) or ()
        return copy.deepcopy(
            {run_id: run for run_id in thread_run_ids if run_id in run_ids and (run := self._visible_run(run_id)) is not None and run.get("operation_kind", "run") == "run" and (user_id is None or run.get("user_id") == user_id)}
        )

    @_locked_memory_mutation
    async def update_status(self, run_id, status, *, error=None, stop_reason=None):
        run = self._visible_run(run_id)
        if run is None:
            return False
        # Guard: only transition rows that are still active. ``interrupted``
        # is included for the rollback path (``interrupted → error`` finalize).
        if run["status"] not in ("pending", "running", "interrupted"):
            return False
        if run.get("operation_kind", "run") != "run":
            run["status"] = status
            if error is not None:
                run["error"] = error
            if stop_reason is not None:
                run["stop_reason"] = stop_reason
            run["updated_at"] = datetime.now(UTC).isoformat()
            return True
        lifecycle_type = lifecycle_type_for_status(status)
        result = await self.transition_run_atomic(
            run_id,
            expected_state_version=run["state_version"],
            expected_statuses=("pending", "running", "interrupted"),
            transition=LifecycleTransition(
                lifecycle_type=lifecycle_type,
                status=status,
                error=error,
                stop_reason=stop_reason,
                reason=stop_reason,
            ),
        )
        return result.applied

    @_locked_memory_mutation
    async def start_run(
        self,
        run_id: str,
        *,
        execution_evidence_json: dict[str, Any] | None = None,
        execution_evidence_digest: str | None = None,
    ) -> bool:
        validate_execution_evidence_run(run_id, execution_evidence_json)
        run = self._visible_run(run_id)
        if run is None or run["status"] != "pending" or run.get("cancel_action") is not None:
            return False
        result = await self.transition_run_atomic(
            run_id,
            expected_state_version=run["state_version"],
            expected_statuses=("pending",),
            transition=LifecycleTransition(
                lifecycle_type=LifecycleType.started,
                status="running",
                execution_evidence_json=execution_evidence_json,
                execution_evidence_digest=execution_evidence_digest,
            ),
        )
        return result.applied

    @staticmethod
    def _owned_observation_fence_matches(
        run: Mapping[str, Any],
        *,
        expected_owner_worker_id: str | None,
        expected_state_version: int | None,
        require_unexpired_lease: bool,
    ) -> bool:
        fenced = expected_owner_worker_id is not None or expected_state_version is not None or require_unexpired_lease
        lease_expires_at = run.get("lease_expires_at")
        if not fenced:
            return lease_expires_at is None
        if expected_owner_worker_id is None or expected_state_version is None:
            return False
        if run.get("owner_worker_id") != expected_owner_worker_id or run.get("state_version") != expected_state_version:
            return False
        if lease_expires_at is None:
            return not require_unexpired_lease
        if not require_unexpired_lease:
            return False
        return not is_lease_expired(
            lease_expires_at,
            grace_seconds=0,
        )

    @_locked_memory_mutation
    async def update_model_name(
        self,
        run_id,
        model_name,
        *,
        expected_owner_worker_id=None,
        expected_state_version=None,
        require_unexpired_lease=False,
    ):
        if model_name is not None:
            model_name = validate_model_profile_identifier(model_name, field_name="run model_name profile identifier")
        run = self._visible_run(run_id)
        if run is None or not self._owned_observation_fence_matches(
            run,
            expected_owner_worker_id=expected_owner_worker_id,
            expected_state_version=expected_state_version,
            require_unexpired_lease=require_unexpired_lease,
        ):
            return False
        run["model_name"] = model_name
        run["updated_at"] = datetime.now(UTC).isoformat()
        return True

    @_locked_memory_mutation
    async def delete(self, run_id, *, user_id=None):
        run = self._visible_run(run_id)
        if run is not None:
            self._runs.pop(run_id, None)
            self._unindex_run(run_id, run["thread_id"])
            scope = run.get("external_scope")
            key = run.get("external_key")
            if scope is not None and key is not None:
                self._runs_by_external_identity.pop((scope, key), None)
            self._lifecycle_events = [event for event in self._lifecycle_events if event["run_id"] != run_id]

    @_locked_memory_mutation
    async def update_run_completion(
        self,
        run_id,
        *,
        status,
        expected_owner_worker_id=None,
        expected_active_state_version=None,
        expected_terminal_state_version=None,
        **kwargs,
    ):
        run = self._visible_run(run_id)
        capability = (
            expected_owner_worker_id,
            expected_active_state_version,
            expected_terminal_state_version,
        )
        if any(value is not None for value in capability) and not all(value is not None for value in capability):
            raise ValueError(
                "terminal completion authority must be supplied together",
            )
        if run is None or status not in _TERMINAL_STATUSES or run.get("status") != status:
            return False
        if all(value is not None for value in capability):
            if (
                run.get("owner_worker_id") is not None
                or run.get("lease_expires_at") is not None
                or run.get("terminal_projection_owner_worker_id") != expected_owner_worker_id
                or run.get("terminal_projection_active_state_version") != expected_active_state_version
                or run.get("state_version") != expected_terminal_state_version
            ):
                return False
        else:
            # Compatibility applies only to legacy/lease-less terminal rows;
            # omitting the capability can never edit a peer-owned projection.
            if run.get("terminal_projection_owner_worker_id") is not None or run.get("terminal_projection_active_state_version") is not None:
                return False
        for key, value in kwargs.items():
            if value is not None:
                run[key] = value
        run["updated_at"] = datetime.now(UTC).isoformat()
        return True

    @_locked_memory_mutation
    async def update_run_progress(
        self,
        run_id,
        *,
        expected_owner_worker_id=None,
        expected_state_version=None,
        require_unexpired_lease=False,
        **kwargs,
    ):
        run = self._visible_run(run_id)
        if (
            run is None
            or run.get("status") != "running"
            or not self._owned_observation_fence_matches(
                run,
                expected_owner_worker_id=expected_owner_worker_id,
                expected_state_version=expected_state_version,
                require_unexpired_lease=require_unexpired_lease,
            )
        ):
            return False
        for key, value in kwargs.items():
            if value is not None:
                run[key] = value
        run["updated_at"] = datetime.now(UTC).isoformat()
        return True

    async def list_pending(self, *, before=None):
        now = before or datetime.now(UTC).isoformat()
        results = [r for r in self._runs.values() if self._tenant_visible(r) and r.get("operation_kind", "run") == "run" and r["status"] == "pending" and r["created_at"] <= now]
        results.sort(key=lambda r: r["created_at"])
        return copy.deepcopy(results)

    async def list_inflight(self, *, before=None):
        now = before or datetime.now(UTC).isoformat()
        results = [r for r in self._runs.values() if self._tenant_visible(r) and r["status"] in ("pending", "running") and r["created_at"] <= now]
        results.sort(key=lambda r: r["created_at"])
        return copy.deepcopy(results)

    async def aggregate_tokens_by_thread(self, thread_id: str, *, include_active: bool = False) -> dict[str, Any]:
        statuses = ("success", "error", "running") if include_active else ("success", "error")
        # Use the thread index for an O(runs-in-thread) lookup instead of
        # scanning every run in the process (mirrors ``list_by_thread``).
        run_ids = self._runs_by_thread.get(thread_id) or ()
        completed = [run for run_id in run_ids if (run := self._visible_run(run_id)) is not None and run.get("operation_kind", "run") == "run" and run.get("status") in statuses]
        by_model: dict[str, dict] = {}
        for r in completed:
            usage_by_model = r.get("token_usage_by_model") or {}
            if usage_by_model:
                for model, usage in usage_by_model.items():
                    entry = by_model.setdefault(model, {"tokens": 0, "runs": 0})
                    entry["tokens"] += usage.get("total_tokens", 0)
                    entry["runs"] += 1
            else:
                # Fallback for rows written before per-model accounting landed:
                # attribute the whole run to its single ``model_name``. Keeps
                # the legacy lead-only behavior for old data instead of
                # silently dropping it.
                model = r.get("model_name") or "unknown"
                entry = by_model.setdefault(model, {"tokens": 0, "runs": 0})
                entry["tokens"] += r.get("total_tokens", 0)
                entry["runs"] += 1
        return {
            "total_tokens": sum(r.get("total_tokens", 0) for r in completed),
            "total_input_tokens": sum(r.get("total_input_tokens", 0) for r in completed),
            "total_output_tokens": sum(r.get("total_output_tokens", 0) for r in completed),
            "total_runs": len(completed),
            "by_model": by_model,
            "by_caller": {
                "lead_agent": sum(r.get("lead_agent_tokens", 0) for r in completed),
                "subagent": sum(r.get("subagent_tokens", 0) for r in completed),
                "middleware": sum(r.get("middleware_tokens", 0) for r in completed),
            },
        }

    # ------------------------------------------------------------------
    # Multi-worker run ownership methods
    # ------------------------------------------------------------------

    @_locked_memory_mutation
    async def update_lease(
        self,
        run_id: str,
        *,
        owner_worker_id: str,
        lease_expires_at: str | None = None,
        lease_duration_seconds: int | None = None,
    ) -> bool:
        observed_at = datetime.now(UTC)
        new_expiry = _process_lease_deadline(
            lease_expires_at=lease_expires_at,
            lease_duration_seconds=lease_duration_seconds,
            observed_at=observed_at,
            required=True,
        )
        run = self._visible_run(run_id)
        if run is None:
            return False
        if run["status"] not in ("pending", "running"):
            return False
        if run.get("owner_worker_id") != owner_worker_id:
            return False
        current_lease = run.get("lease_expires_at")
        if current_lease is not None and _lease_expired_at(
            current_lease,
            observed_at=observed_at,
            grace_seconds=0,
        ):
            return False
        run["owner_worker_id"] = owner_worker_id
        run["lease_expires_at"] = new_expiry
        run["updated_at"] = observed_at.isoformat()
        return True

    @_locked_memory_mutation
    async def renew_lease(
        self,
        run_id: str,
        *,
        owner_worker_id: str,
        lease_expires_at: str | None = None,
        lease_duration_seconds: int | None = None,
    ) -> LeaseRenewal:
        observed_at = datetime.now(UTC)
        new_expiry = _process_lease_deadline(
            lease_expires_at=lease_expires_at,
            lease_duration_seconds=lease_duration_seconds,
            observed_at=observed_at,
            required=True,
        )
        run = self._visible_run(run_id)
        if (
            run is None
            or run["status"] not in ("pending", "running")
            or run.get("owner_worker_id") != owner_worker_id
            or (
                run.get("lease_expires_at") is not None
                and _lease_expired_at(
                    run["lease_expires_at"],
                    observed_at=observed_at,
                    grace_seconds=0,
                )
            )
        ):
            return LeaseRenewal(renewed=False)
        run["lease_expires_at"] = new_expiry
        run["updated_at"] = observed_at.isoformat()
        return LeaseRenewal(
            renewed=True,
            cancel_action=run.get("cancel_action"),
            lease_expires_at=new_expiry,
        )

    @_locked_memory_mutation
    async def execution_owner_authorized(
        self,
        run_id: str,
        *,
        owner_worker_id: str,
        state_version: int,
    ) -> bool:
        observed_at = datetime.now(UTC)
        run = self._visible_run(run_id)
        return bool(
            run is not None
            and run.get("operation_kind", "run") == "run"
            and run.get("status") in ("pending", "running")
            and run.get("owner_worker_id") == owner_worker_id
            and run.get("state_version") == state_version
            and (
                run.get("lease_expires_at") is None
                or not _lease_expired_at(
                    run["lease_expires_at"],
                    observed_at=observed_at,
                    grace_seconds=0,
                )
            )
        )

    @_locked_memory_mutation
    async def request_cancel(self, run_id: str, *, action: str) -> str | None:
        result = await self.request_cancel_compat(run_id, action=action)
        if result.outcome in (
            CancellationRequestOutcome.requested,
            CancellationRequestOutcome.already_requested,
            CancellationRequestOutcome.stale,
        ):
            return result.row.get("cancel_action") if result.row is not None else None
        return None

    @_locked_memory_mutation
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
        run = self._visible_run(run_id)
        if run is None:
            return StatusFinalization(finalized=False)
        if run.get("cancel_action") is not None:
            return StatusFinalization(
                finalized=False,
                cancel_action=run["cancel_action"],
            )
        if run["status"] not in ("pending", "running"):
            return StatusFinalization(finalized=False)
        if (expected_owner_worker_id is None) != (expected_state_version is None):
            return StatusFinalization(finalized=False)
        if expected_owner_worker_id is not None and (
            run["state_version"] != expected_state_version
            or not self._owned_run_fence_matches(
                run,
                expected_owner_worker_id=expected_owner_worker_id,
                require_unexpired_lease=require_unexpired_lease,
            )
        ):
            return StatusFinalization(finalized=False)
        lifecycle_type = lifecycle_type_for_status(status)
        transition = LifecycleTransition(
            lifecycle_type=lifecycle_type,
            status=status,
            error=error,
            stop_reason=stop_reason,
            reason=stop_reason,
        )
        if expected_owner_worker_id is not None:
            result = await self.transition_owned_run_atomic(
                run_id,
                expected_state_version=expected_state_version,
                expected_statuses=("pending", "running"),
                transition=transition,
                expected_owner_worker_id=expected_owner_worker_id,
                require_unexpired_lease=require_unexpired_lease,
            )
        else:
            result = await self.transition_run_atomic(
                run_id,
                expected_state_version=run["state_version"],
                expected_statuses=("pending", "running"),
                transition=transition,
            )
        return StatusFinalization(finalized=result.applied)

    @_locked_memory_mutation
    async def claim_for_takeover(
        self,
        run_id: str,
        *,
        grace_seconds: int,
        error: str,
        stop_reason: str | None = None,
        expected_state_version: int | None = None,
    ) -> bool:
        from deerflow.utils.time import is_lease_expired

        run = self._visible_run(run_id)
        if run is None:
            return False
        if run["status"] not in ("pending", "running"):
            return False
        if expected_state_version is not None and run["state_version"] != expected_state_version:
            return False
        if run.get("recovery_policy", RecoveryPolicy.terminalize_v1.value) == RecoveryPolicy.exact_two_takeover_v1.value:
            return False
        lease = run.get("lease_expires_at")
        if not is_lease_expired(lease, grace_seconds=grace_seconds):
            return False
        if run.get("operation_kind", "run") != "run":
            run["status"] = "error"
            run["error"] = error
            run["owner_worker_id"] = None
            run["lease_expires_at"] = None
            if stop_reason is not None:
                run["stop_reason"] = stop_reason
            run["updated_at"] = datetime.now(UTC).isoformat()
            return True
        result = await self.transition_run_atomic(
            run_id,
            expected_state_version=run["state_version"],
            expected_statuses=("pending", "running"),
            transition=LifecycleTransition(
                lifecycle_type=LifecycleType.failed,
                status="error",
                error=error,
                stop_reason=stop_reason,
                reason=stop_reason,
            ),
        )
        return result.applied

    @_atomic_memory_mutation
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
        observed_at = datetime.now(UTC)
        new_expiry = _process_lease_deadline(
            lease_expires_at=lease_expires_at,
            lease_duration_seconds=lease_duration_seconds,
            observed_at=observed_at,
            required=True,
        )
        run = self._visible_run(run_id)
        if run is None:
            return ExecutionTakeoverClaim(ExecutionTakeoverOutcome.not_eligible)
        eligible = (
            run.get("operation_kind", "run") == "run"
            and run.get("recovery_policy", RecoveryPolicy.terminalize_v1.value) == RecoveryPolicy.exact_two_takeover_v1.value
            and run.get("status") in ("pending", "running")
            and run.get("cancel_action") is None
            and run.get("state_version") == expected_state_version
            and _lease_expired_at(
                run.get("lease_expires_at"),
                observed_at=observed_at,
                grace_seconds=grace_seconds,
            )
        )
        if not eligible:
            return ExecutionTakeoverClaim(
                ExecutionTakeoverOutcome.not_eligible,
                copy.deepcopy(run),
            )
        run["owner_worker_id"] = new_owner_worker_id
        run["lease_expires_at"] = new_expiry
        run["state_version"] += 1
        run["updated_at"] = observed_at.isoformat()
        return ExecutionTakeoverClaim(
            ExecutionTakeoverOutcome.claimed,
            copy.deepcopy(run),
        )

    async def list_inflight_with_expired_lease(
        self,
        *,
        before: str | None = None,
        grace_seconds: int = 10,
    ) -> list[dict[str, Any]]:
        observed_at = datetime.now(UTC)
        now_dt = datetime.fromisoformat(before) if before else observed_at
        cutoff = observed_at - timedelta(seconds=grace_seconds)
        results = []
        for r in self._runs.values():
            if not self._tenant_visible(r):
                continue
            if r["status"] not in ("pending", "running"):
                continue
            created_at = r.get("created_at", "")
            if not created_at:
                continue
            try:
                created_dt = datetime.fromisoformat(created_at)
            except (ValueError, TypeError):
                continue
            if created_dt > now_dt:
                continue
            lease = r.get("lease_expires_at")
            if lease is None:
                # Pre-ownership rows: no lease means orphaned
                results.append(copy.deepcopy(r))
            else:
                try:
                    lease_dt = datetime.fromisoformat(lease)
                    # Treat naive values as UTC — same convention as
                    # ``coerce_iso`` in the SQL store, so the comparison
                    # against the aware ``cutoff`` does not raise
                    # ``TypeError`` when heartbeat is enabled on SQLite
                    # (which drops tzinfo on read).
                    if lease_dt.tzinfo is None:
                        lease_dt = lease_dt.replace(tzinfo=UTC)
                    if lease_dt <= cutoff:
                        results.append(copy.deepcopy(r))
                except (ValueError, TypeError):
                    results.append(copy.deepcopy(r))
        results.sort(key=lambda r: r["created_at"])
        return results

    @_atomic_memory_mutation
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
        from deerflow.runtime.runs.manager import ConflictError

        if run_id in self._runs:
            raise DuplicateRunIdentityError(run_id)

        thread_id = validate_thread_identifier(thread_id)
        tenant_ref, tenant_digest = tenant_store_columns(self._tenant, tenant)
        if model_name is not None:
            model_name = validate_model_profile_identifier(model_name, field_name="run model_name profile identifier")
        observed_at = datetime.now(UTC)
        now = observed_at.isoformat()
        lease_expires_at = _process_lease_deadline(
            lease_expires_at=lease_expires_at,
            lease_duration_seconds=lease_duration_seconds,
            observed_at=observed_at,
            required=False,
        )
        recovery_policy = RecoveryPolicy(recovery_policy)
        if operation_kind != "run" and recovery_policy is not RecoveryPolicy.terminalize_v1:
            raise ValueError("execution recovery policy applies only to normal runs")
        if (recovery_policy is RecoveryPolicy.exact_two_takeover_v1) != (recovery_payload_json is not None):
            raise ValueError("execution recovery policy and payload must be admitted together")
        cutoff = observed_at - timedelta(seconds=grace_seconds)

        if idempotency_key is not None:
            for existing in self._runs.values():
                if not self._tenant_visible(existing):
                    continue
                if existing.get("idempotency_key") == idempotency_key:
                    raise RunIdempotencyConflict(existing)

        # For reject: check if any active run exists
        if multitask_strategy == "reject":
            for r in self._runs.values():
                if not self._tenant_visible(r):
                    continue
                if r["thread_id"] == thread_id and r["status"] in ("pending", "running"):
                    raise ConflictError(f"Thread {thread_id} already has an active run")

        # For interrupt/rollback: claim inflight runs only for the legacy
        # receiptless contract. Evidence-requiring callers fail closed below.
        # Two-pass so the memory path mirrors the SQL store's transactional
        # semantics — if any candidate is a live run owned by another worker
        # we must raise ConflictError WITHOUT having already mutated earlier
        # candidates. Mutating inline would leave the store in a half-
        # interrupted state on raise, diverging from SQL where a raise rolls
        # the whole transaction back.
        claimed = []
        if multitask_strategy in ("interrupt", "rollback"):
            candidates: list[dict[str, Any]] = []
            for r in self._runs.values():
                if not self._tenant_visible(r):
                    continue
                if r["thread_id"] != thread_id:
                    continue
                if r["status"] not in ("pending", "running"):
                    continue
                if r.get("recovery_policy") == RecoveryPolicy.exact_two_takeover_v1.value:
                    # Exact-two execution authority may move only through its
                    # dedicated, qualified takeover primitive. Replacement is
                    # a generic terminalization path, even when the lease is
                    # expired or the candidate reports the same worker ID.
                    raise ConflictError(
                        f"Thread {thread_id} has an active exact-two run",
                        active_run_id=r["run_id"],
                    )
                if require_predecessor_inactive:
                    raise ConflictError(
                        f"Thread {thread_id} requires explicit predecessor cancellation",
                        active_run_id=r["run_id"],
                    )
                lease_expired = False
                existing_lease = r.get("lease_expires_at")
                if existing_lease is not None:
                    try:
                        lease_dt = datetime.fromisoformat(existing_lease)
                        # Treat naive values as UTC — same convention as
                        # the SQL store and ``coerce_iso``, so the
                        # comparison against the aware ``cutoff`` does not
                        # raise ``TypeError``.
                        if lease_dt.tzinfo is None:
                            lease_dt = lease_dt.replace(tzinfo=UTC)
                        lease_expired = lease_dt <= cutoff
                        if lease_dt > cutoff and r.get("owner_worker_id") != owner_worker_id:
                            # Live run owned by another worker — cannot
                            # interrupt, and the partial unique index would
                            # reject the INSERT anyway. Surface as ConflictError
                            # so the caller gets a clean signal. Raise before
                            # any mutation so the store is left untouched.
                            raise ConflictError(f"Thread {thread_id} already has an active run owned by another worker")
                    except (ValueError, TypeError):
                        pass
                if r.get("operation_kind", "run") != "run" and not lease_expired:
                    raise ConflictError(f"Thread {thread_id} has an active checkpoint write")
                candidates.append(r)
            candidates.sort(key=lambda row: (row["created_at"], row["run_id"]))
            for r in candidates:
                replacement_status = "error" if multitask_strategy == "rollback" else "interrupted"
                replacement_error = "Rolled back by user" if multitask_strategy == "rollback" else "Cancelled by newer run"
                if r.get("operation_kind", "run") == "run":
                    r["terminal_projection_owner_worker_id"] = r.get("owner_worker_id")
                    r["terminal_projection_active_state_version"] = r["state_version"] if r.get("owner_worker_id") is not None else None
                else:
                    r["terminal_projection_owner_worker_id"] = None
                    r["terminal_projection_active_state_version"] = None
                r["status"] = replacement_status
                r["error"] = replacement_error
                r["owner_worker_id"] = None
                r["lease_expires_at"] = None
                r["updated_at"] = now
                if r.get("operation_kind", "run") == "run":
                    r["state_version"] += 1
                    self._append_lifecycle_event(
                        r,
                        LifecycleTransition(
                            lifecycle_type=LifecycleType.interrupted,
                            status=replacement_status,
                            error=replacement_error,
                            reason="rollback" if multitask_strategy == "rollback" else "replacement",
                        ),
                    )
                claimed.append(r)

        new_row = {
            "run_id": run_id,
            "admission_cursor": self._next_admission_cursor(),
            "thread_id": thread_id,
            "assistant_id": assistant_id,
            "user_id": user_id,
            "model_name": model_name,
            "status": "pending",
            "operation_kind": operation_kind,
            "recovery_policy": recovery_policy.value,
            "recovery_payload_json": (copy.deepcopy(recovery_payload_json) if operation_kind == "run" else None),
            "multitask_strategy": multitask_strategy,
            "metadata": metadata or {},
            "kwargs": kwargs or {},
            "error": None,
            "owner_worker_id": owner_worker_id,
            "lease_expires_at": lease_expires_at,
            "terminal_projection_owner_worker_id": None,
            "terminal_projection_active_state_version": None,
            "idempotency_key": idempotency_key,
            "cancel_action": None,
            "cancel_requested_at": None,
            "created_at": created_at or now,
            "updated_at": now,
            "tenant_ref": tenant_ref,
            "tenant_digest": tenant_digest,
            "origin_json": origin_json if operation_kind == "run" else None,
            "principal_projection_json": principal_projection_json if operation_kind == "run" else None,
            "principal_projection_digest": principal_projection_digest if operation_kind == "run" else None,
            "base_origin_digest": base_origin_digest if operation_kind == "run" else None,
            "accepted_context_digest": accepted_context_digest if operation_kind == "run" else None,
            "agent_revision_json": agent_revision_json if operation_kind == "run" else None,
            "agent_revision_digest": agent_revision_digest if operation_kind == "run" else None,
            "extension_generation": extension_generation if operation_kind == "run" else None,
            "decision_evidence_json": decision_evidence_json if operation_kind == "run" else None,
            "external_scope": external_scope if operation_kind == "run" else None,
            "external_key": external_key if operation_kind == "run" else None,
            "request_digest": request_digest if operation_kind == "run" else None,
            "request_digest_version": request_digest_version if operation_kind == "run" else None,
            "caller_intent_json": caller_intent_json if operation_kind == "run" else None,
            "caller_intent_digest": caller_intent_digest if operation_kind == "run" else None,
            "caller_intent_digest_version": caller_intent_digest_version if operation_kind == "run" else None,
            "execution_evidence_json": None,
            "execution_evidence_digest": None,
            "assembly_evidence_json": None,
            "assembly_evidence_digest": None,
            "state_version": 1 if operation_kind == "run" else 0,
        }
        self._runs[run_id] = new_row
        self._index_run(run_id, thread_id)
        if operation_kind == "run" and external_scope is not None and external_key is not None:
            self._runs_by_external_identity[(external_scope, external_key)] = run_id
        if operation_kind == "run":
            self._append_lifecycle_event(
                new_row,
                LifecycleTransition(lifecycle_type=LifecycleType.accepted, status="pending"),
            )
        return new_row, claimed

    @_atomic_memory_mutation
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
        """Release one exact auxiliary row under its captured ownership fence."""

        row = self._visible_run(run_id)
        if row is None:
            return ThreadOperationReleaseResult(
                outcome=ThreadOperationReleaseOutcome.absent,
            )
        actual_kind = row.get("operation_kind", "run")
        if actual_kind == "run" or actual_kind != operation_kind or row.get("thread_id") != thread_id or row.get("user_id") != user_id:
            return ThreadOperationReleaseResult(
                outcome=ThreadOperationReleaseOutcome.identity_mismatch,
            )
        if row.get("status") not in {"pending", "running"}:
            return ThreadOperationReleaseResult(
                outcome=ThreadOperationReleaseOutcome.inactive,
            )
        if row.get("owner_worker_id") != expected_owner_worker_id:
            return ThreadOperationReleaseResult(
                outcome=ThreadOperationReleaseOutcome.ownership_lost,
            )
        if require_unexpired_lease:
            from deerflow.utils.time import is_lease_expired

            if is_lease_expired(
                row.get("lease_expires_at"),
                grace_seconds=0,
            ):
                return ThreadOperationReleaseResult(
                    outcome=ThreadOperationReleaseOutcome.ownership_lost,
                )
        self._runs.pop(run_id, None)
        self._unindex_run(run_id, thread_id)
        return ThreadOperationReleaseResult(
            outcome=ThreadOperationReleaseOutcome.released,
        )

    @_locked_memory_mutation
    async def ensure_run_atomic(
        self,
        run_id: str,
        *,
        external_scope: str,
        external_key: str,
        request_digest: str,
        request_digest_version: str,
        caller_intent_json: dict[str, Any],
        caller_intent_digest: str,
        caller_intent_digest_version: str,
        **kwargs: Any,
    ) -> RunEnsureResult:
        existing = await self.get_by_external_identity(external_scope, external_key)
        if existing is not None:
            same_intent = existing.get("caller_intent_json") == caller_intent_json and existing.get("caller_intent_digest") == caller_intent_digest and existing.get("caller_intent_digest_version") == caller_intent_digest_version
            outcome = AdmissionOutcome.known_same if same_intent else AdmissionOutcome.key_conflict
            return RunEnsureResult(outcome=outcome, row=existing)

        row, claimed = await self.create_thread_operation_atomic(
            run_id,
            operation_kind="run",
            external_scope=external_scope,
            external_key=external_key,
            request_digest=request_digest,
            request_digest_version=request_digest_version,
            caller_intent_json=caller_intent_json,
            caller_intent_digest=caller_intent_digest,
            caller_intent_digest_version=caller_intent_digest_version,
            **kwargs,
        )
        return RunEnsureResult(outcome=AdmissionOutcome.created, row=row, claimed=tuple(claimed))
