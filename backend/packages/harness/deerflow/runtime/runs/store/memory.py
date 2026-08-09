"""In-memory RunStore. Used when database.backend=memory (default) and in tests.

Equivalent to the original RunManager._runs dict behavior.
"""

from __future__ import annotations

import copy
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from functools import wraps
from typing import Any

from deerflow_extension_api import (
    validate_model_profile_identifier,
    validate_thread_identifier,
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
    CancellationRequestOutcome,
    CancellationRequestResult,
    LeaseRenewal,
    LifecycleTransition,
    LifecycleTransitionResult,
    LifecycleType,
    RunEnsureResult,
    RunStore,
    StatusFinalization,
    build_lifecycle_payload,
    lifecycle_owner_scope,
    lifecycle_type_for_status,
)

_TERMINAL_STATUSES = {"success", "error", "timeout", "interrupted"}


def _atomic_memory_mutation[**P, R](method: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
    @wraps(method)
    async def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
        self = args[0]
        snapshot = (
            copy.deepcopy(self._runs),
            copy.deepcopy(self._runs_by_thread),
            copy.deepcopy(self._runs_by_external_identity),
            copy.deepcopy(self._lifecycle_events),
            self._lifecycle_cursor,
            self._lifecycle_pruned_through,
        )
        try:
            return await method(*args, **kwargs)
        except BaseException:
            (
                self._runs,
                self._runs_by_thread,
                self._runs_by_external_identity,
                self._lifecycle_events,
                self._lifecycle_cursor,
                self._lifecycle_pruned_through,
            ) = snapshot
            raise

    return wrapped


class MemoryRunStore(RunStore):
    durable_lifecycle = True

    def __init__(self) -> None:
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

    async def initialize_lifecycle(self) -> None:
        return None

    async def list_lifecycle_events(
        self,
        *,
        run_id: str | None = None,
        thread_id: str | None = None,
    ) -> list[dict[str, Any]]:
        return [dict(event) for event in self._lifecycle_events if (run_id is None or event["run_id"] == run_id) and (thread_id is None or event["thread_id"] == thread_id)]

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
                requested < event["cursor"] <= self._lifecycle_cursor
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
                row = self._runs.get(run_id)
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
            row = self._runs.get(run_id)
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
        payload = build_lifecycle_payload(transition)
        row = self._runs.get(run_id)
        if row is None or row.get("operation_kind", "run") != "run":
            return LifecycleTransitionResult(applied=False)
        if user_id is not None and row.get("user_id") != user_id:
            return LifecycleTransitionResult(applied=False)
        if row["state_version"] != expected_state_version:
            return LifecycleTransitionResult(applied=False, row=row)
        if expected_statuses is not None and row["status"] not in expected_statuses:
            return LifecycleTransitionResult(applied=False, row=row)
        row["status"] = transition.status
        row["state_version"] += 1
        if transition.error is not None:
            row["error"] = transition.error
        if transition.stop_reason is not None:
            row["stop_reason"] = transition.stop_reason
        row["updated_at"] = datetime.now(UTC).isoformat()
        event = self._append_lifecycle_event(row, transition, payload=payload)
        return LifecycleTransitionResult(applied=True, row=row, event=event)

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

    def _request_cancel_atomic(
        self,
        run_id: str,
        *,
        action: str,
        expected_state_version: int | None = None,
        user_id: str | None = None,
    ) -> CancellationRequestResult:
        if action not in ("interrupt", "rollback"):
            raise ValueError(f"Unsupported cancellation action: {action}")
        row = self._runs.get(run_id)
        if row is None or row.get("operation_kind", "run") != "run" or (user_id is not None and row.get("user_id") != user_id):
            return CancellationRequestResult(CancellationRequestOutcome.not_found_or_invisible)
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
        run_id,
        *,
        thread_id,
        assistant_id=None,
        user_id=None,
        model_name=None,
        status="pending",
        operation_kind="run",
        multitask_strategy="reject",
        metadata=None,
        kwargs=None,
        error=None,
        stop_reason=None,
        created_at=None,
        owner_worker_id=None,
        lease_expires_at=None,
        origin_json=None,
        principal_projection_json=None,
        principal_projection_digest=None,
        base_origin_digest=None,
        accepted_context_digest=None,
        agent_revision_json=None,
        agent_revision_digest=None,
        extension_generation=None,
        decision_evidence_json=None,
        external_scope=None,
        external_key=None,
        request_digest=None,
        request_digest_version=None,
        caller_intent_json=None,
        caller_intent_digest=None,
        caller_intent_digest_version=None,
    ):
        thread_id = validate_thread_identifier(thread_id)
        if model_name is not None:
            model_name = validate_model_profile_identifier(model_name, field_name="run model_name profile identifier")
        now = datetime.now(UTC).isoformat()
        existing = self._runs.get(run_id)
        lifecycle_row = operation_kind == "run" and status is not None
        new_row = {
            "run_id": run_id,
            "thread_id": thread_id,
            "assistant_id": assistant_id,
            "user_id": user_id,
            "model_name": model_name,
            "status": "pending" if lifecycle_row else status,
            "operation_kind": operation_kind,
            "multitask_strategy": multitask_strategy,
            "metadata": metadata or {},
            "kwargs": kwargs or {},
            "error": error,
            "stop_reason": stop_reason,
            "created_at": created_at or now,
            "updated_at": now,
            "owner_worker_id": owner_worker_id,
            "lease_expires_at": lease_expires_at,
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
                new_row["status"] = status
                new_row["state_version"] += 1
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

    async def get(self, run_id, *, user_id=None):
        run = self._runs.get(run_id)
        if run is None:
            return None
        if user_id is not None and run.get("user_id") != user_id:
            return None
        return run

    async def get_by_external_identity(self, external_scope: str, external_key: str):
        run_id = self._runs_by_external_identity.get((external_scope, external_key))
        return self._runs.get(run_id) if run_id is not None else None

    async def list_by_thread(self, thread_id, *, user_id=None, limit=100):
        # Use the thread index for an O(runs-in-thread) lookup instead of
        # scanning every run. ``self._runs.get`` is defense-in-depth: it drops a
        # stale id still in the index but already gone from ``_runs``.
        run_ids = self._runs_by_thread.get(thread_id)
        if not run_ids:
            return []
        results = [run for run_id in run_ids if (run := self._runs.get(run_id)) is not None and run.get("operation_kind", "run") == "run" and (user_id is None or run.get("user_id") == user_id)]
        results.sort(key=lambda r: r["created_at"], reverse=True)
        return results[:limit]

    async def list_successful_regenerate_sources(self, thread_id, *, user_id=None):
        run_ids = self._runs_by_thread.get(thread_id) or ()
        sources: set[str] = set()
        for run_id in run_ids:
            run = self._runs.get(run_id)
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
            run = self._runs.get(run_id)
            if run is None:
                continue
            if user_id is not None and run.get("user_id") != user_id:
                continue
            metadata = run.get("metadata") or {}
            source = metadata.get("regenerate_from_run_id")
            if metadata.get("replay_kind") == "edit" and isinstance(source, str) and source:
                results.append(run)
        results.sort(key=lambda r: r["created_at"])
        return results

    async def get_many_by_thread(self, thread_id, run_ids, *, user_id=None):
        thread_run_ids = self._runs_by_thread.get(thread_id) or ()
        return {run_id: run for run_id in thread_run_ids if run_id in run_ids and (run := self._runs.get(run_id)) is not None and run.get("operation_kind", "run") == "run" and (user_id is None or run.get("user_id") == user_id)}

    async def update_status(self, run_id, status, *, error=None, stop_reason=None):
        run = self._runs.get(run_id)
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

    async def start_run(self, run_id) -> bool:
        run = self._runs.get(run_id)
        if run is None or run["status"] != "pending" or run.get("cancel_action") is not None:
            return False
        result = await self.transition_run_atomic(
            run_id,
            expected_state_version=run["state_version"],
            expected_statuses=("pending",),
            transition=LifecycleTransition(lifecycle_type=LifecycleType.started, status="running"),
        )
        return result.applied

    async def update_model_name(self, run_id, model_name):
        if model_name is not None:
            model_name = validate_model_profile_identifier(model_name, field_name="run model_name profile identifier")
        if run_id in self._runs:
            self._runs[run_id]["model_name"] = model_name
            self._runs[run_id]["updated_at"] = datetime.now(UTC).isoformat()

    async def delete(self, run_id, *, user_id=None):
        run = self._runs.pop(run_id, None)
        if run is not None:
            self._unindex_run(run_id, run["thread_id"])
            scope = run.get("external_scope")
            key = run.get("external_key")
            if scope is not None and key is not None:
                self._runs_by_external_identity.pop((scope, key), None)
            self._lifecycle_events = [event for event in self._lifecycle_events if event["run_id"] != run_id]

    async def update_run_completion(self, run_id, *, status, **kwargs):
        run = self._runs.get(run_id)
        if run is None:
            return False
        current_status = run.get("status")
        allowed_sources = {"pending", "running", status}
        if status == "error":
            allowed_sources.add("interrupted")
        if current_status not in allowed_sources:
            return False
        if current_status != status and run.get("operation_kind", "run") == "run":
            lifecycle_type = lifecycle_type_for_status(status)
            result = await self.transition_run_atomic(
                run_id,
                expected_state_version=run["state_version"],
                expected_statuses=(current_status,),
                transition=LifecycleTransition(
                    lifecycle_type=lifecycle_type,
                    status=status,
                    error=kwargs.get("error"),
                    stop_reason=kwargs.get("stop_reason"),
                    reason=kwargs.get("stop_reason"),
                ),
            )
            if not result.applied:
                return False
        else:
            run["status"] = status
        for key, value in kwargs.items():
            if value is not None:
                run[key] = value
        run["updated_at"] = datetime.now(UTC).isoformat()
        return True

    async def update_run_progress(self, run_id, **kwargs):
        if run_id in self._runs and self._runs[run_id].get("status") == "running":
            for key, value in kwargs.items():
                if value is not None:
                    self._runs[run_id][key] = value
            self._runs[run_id]["updated_at"] = datetime.now(UTC).isoformat()

    async def list_pending(self, *, before=None):
        now = before or datetime.now(UTC).isoformat()
        results = [r for r in self._runs.values() if r.get("operation_kind", "run") == "run" and r["status"] == "pending" and r["created_at"] <= now]
        results.sort(key=lambda r: r["created_at"])
        return results

    async def list_inflight(self, *, before=None):
        now = before or datetime.now(UTC).isoformat()
        results = [r for r in self._runs.values() if r["status"] in ("pending", "running") and r["created_at"] <= now]
        results.sort(key=lambda r: r["created_at"])
        return results

    async def aggregate_tokens_by_thread(self, thread_id: str, *, include_active: bool = False) -> dict[str, Any]:
        statuses = ("success", "error", "running") if include_active else ("success", "error")
        # Use the thread index for an O(runs-in-thread) lookup instead of
        # scanning every run in the process (mirrors ``list_by_thread``).
        run_ids = self._runs_by_thread.get(thread_id) or ()
        completed = [run for run_id in run_ids if (run := self._runs.get(run_id)) is not None and run.get("operation_kind", "run") == "run" and run.get("status") in statuses]
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

    async def update_lease(
        self,
        run_id: str,
        *,
        owner_worker_id: str,
        lease_expires_at: str,
    ) -> bool:
        run = self._runs.get(run_id)
        if run is None:
            return False
        if run["status"] not in ("pending", "running"):
            return False
        if run.get("owner_worker_id") != owner_worker_id:
            return False
        run["owner_worker_id"] = owner_worker_id
        run["lease_expires_at"] = lease_expires_at
        run["updated_at"] = datetime.now(UTC).isoformat()
        return True

    async def renew_lease(
        self,
        run_id: str,
        *,
        owner_worker_id: str,
        lease_expires_at: str,
    ) -> LeaseRenewal:
        # Delegate through ``update_lease`` so lightweight subclasses and tests
        # that override the legacy primitive keep the same behavior.
        renewed = await self.update_lease(
            run_id,
            owner_worker_id=owner_worker_id,
            lease_expires_at=lease_expires_at,
        )
        if not renewed:
            return LeaseRenewal(renewed=False)
        run = self._runs.get(run_id)
        return LeaseRenewal(
            renewed=True,
            cancel_action=run.get("cancel_action") if run is not None else None,
        )

    async def request_cancel(self, run_id: str, *, action: str) -> str | None:
        result = await self.request_cancel_compat(run_id, action=action)
        if result.outcome in (
            CancellationRequestOutcome.requested,
            CancellationRequestOutcome.already_requested,
            CancellationRequestOutcome.stale,
        ):
            return result.row.get("cancel_action") if result.row is not None else None
        return None

    async def finalize_if_not_cancelled(
        self,
        run_id: str,
        *,
        status: str,
        error: str | None = None,
        stop_reason: str | None = None,
    ) -> StatusFinalization:
        run = self._runs.get(run_id)
        if run is None:
            return StatusFinalization(finalized=False)
        if run.get("cancel_action") is not None:
            return StatusFinalization(
                finalized=False,
                cancel_action=run["cancel_action"],
            )
        if run["status"] not in ("pending", "running"):
            return StatusFinalization(finalized=False)
        lifecycle_type = lifecycle_type_for_status(status)
        result = await self.transition_run_atomic(
            run_id,
            expected_state_version=run["state_version"],
            expected_statuses=("pending", "running"),
            transition=LifecycleTransition(
                lifecycle_type=lifecycle_type,
                status=status,
                error=error,
                stop_reason=stop_reason,
                reason=stop_reason,
            ),
        )
        return StatusFinalization(finalized=result.applied)

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

        run = self._runs.get(run_id)
        if run is None:
            return False
        if run["status"] not in ("pending", "running"):
            return False
        if expected_state_version is not None and run["state_version"] != expected_state_version:
            return False
        lease = run.get("lease_expires_at")
        if not is_lease_expired(lease, grace_seconds=grace_seconds):
            return False
        if run.get("operation_kind", "run") != "run":
            run["status"] = "error"
            run["error"] = error
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

    async def list_inflight_with_expired_lease(
        self,
        *,
        before: str | None = None,
        grace_seconds: int = 10,
    ) -> list[dict[str, Any]]:
        now_dt = datetime.fromisoformat(before) if before else datetime.now(UTC)
        cutoff = datetime.now(UTC) - timedelta(seconds=grace_seconds)
        results = []
        for r in self._runs.values():
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
                results.append(r)
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
                    if lease_dt < cutoff:
                        results.append(r)
                except (ValueError, TypeError):
                    results.append(r)
        results.sort(key=lambda r: r["created_at"])
        return results

    @_atomic_memory_mutation
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
        caller_intent_json: dict[str, Any] | None = None,
        caller_intent_digest: str | None = None,
        caller_intent_digest_version: str | None = None,
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        from deerflow.runtime.runs.manager import ConflictError

        thread_id = validate_thread_identifier(thread_id)
        if model_name is not None:
            model_name = validate_model_profile_identifier(model_name, field_name="run model_name profile identifier")
        now = datetime.now(UTC).isoformat()
        cutoff = datetime.now(UTC) - timedelta(seconds=grace_seconds)

        # For reject: check if any active run exists
        if multitask_strategy == "reject":
            for r in self._runs.values():
                if r["thread_id"] == thread_id and r["status"] in ("pending", "running"):
                    raise ConflictError(f"Thread {thread_id} already has an active run")

        # For interrupt/rollback: claim inflight runs.
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
                if r["thread_id"] != thread_id:
                    continue
                if r["status"] not in ("pending", "running"):
                    continue
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
                        lease_expired = lease_dt < cutoff
                        if lease_dt >= cutoff and r.get("owner_worker_id") != owner_worker_id:
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
                r["status"] = replacement_status
                r["error"] = replacement_error
                r["owner_worker_id"] = owner_worker_id
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
            "thread_id": thread_id,
            "assistant_id": assistant_id,
            "user_id": user_id,
            "model_name": model_name,
            "status": "pending",
            "operation_kind": operation_kind,
            "multitask_strategy": multitask_strategy,
            "metadata": metadata or {},
            "kwargs": kwargs or {},
            "error": None,
            "owner_worker_id": owner_worker_id,
            "lease_expires_at": lease_expires_at,
            "cancel_action": None,
            "cancel_requested_at": None,
            "created_at": created_at or now,
            "updated_at": now,
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
