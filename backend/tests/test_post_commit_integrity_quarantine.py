"""Fail-closed reconciliation contracts for contradictory post-commit evidence."""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import logging
import os
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal

import pytest

from deerflow.config.run_ownership_config import RunOwnershipConfig
from deerflow.runtime import DisconnectMode, RunManager, RunStatus, ThreadOperationKind
from deerflow.runtime.runs.lifecycle_query import LifecycleQuery
from deerflow.runtime.runs.manager import (
    RunRecord,
    _AdmissionTerminalDisposition,
    _UnresolvedAdmissionCandidate,
    _UnresolvedThreadOperationRelease,
)
from deerflow.runtime.runs.store.base import (
    LifecycleTransition,
    LifecycleType,
    lifecycle_owner_scope,
)
from deerflow.runtime.runs.store.memory import MemoryRunStore

_POSTGRES_URL = os.environ.get("DEERFLOW_TEST_POSTGRES_URL")

_RETAINED_USER = "owner-retained"
_DURABLE_USER = "owner-durable"
_WORKER = "worker-integrity-quarantine"


def _execution_evidence(run_id: str) -> tuple[dict[str, Any], str]:
    evidence = {
        "version": 1,
        "profile": "rwx_verified_copy_v1",
        "attempt_id": "attempt-1",
        "snapshot_id": "a" * 64,
        "run_id": run_id,
        "generation": 1,
        "pod_uid": "pod-1",
        "lease_uid": "lease-1",
        "runtime_image_ids_digest": "b" * 64,
        "verifier_receipt_digest": "c" * 64,
        "materialization_evidence_digest": "d" * 64,
    }
    encoded = json.dumps(
        evidence,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return evidence, hashlib.sha256(encoded).hexdigest()


class _ControlledAuthoritativeReadStore(MemoryRunStore):
    """Inject failures only into the trusted unscoped reconciliation read."""

    def __init__(self) -> None:
        super().__init__()
        self.authoritative_mode: Literal[
            "normal",
            "unavailable",
            "malformed",
            "unsupported-status",
        ] = "normal"
        self.read_scopes: list[str | None] = []
        self.renewal_calls = 0
        self.transition_calls = 0
        self.release_calls = 0

    async def authoritative_get(
        self,
        run_id: str,
    ) -> dict[str, Any] | Any | None:
        self.read_scopes.append(None)
        if self.authoritative_mode == "unavailable":
            raise ConnectionError("authoritative store unavailable")
        if self.authoritative_mode == "malformed":
            return object()
        if self.authoritative_mode == "unsupported-status":
            row = await super().authoritative_get(run_id)
            if row is None:
                return None
            return {**row, "status": "future-active-state"}
        return await super().authoritative_get(run_id)

    async def renew_lease(self, *args: Any, **kwargs: Any):
        self.renewal_calls += 1
        return await super().renew_lease(*args, **kwargs)

    async def transition_owned_run_atomic(self, *args: Any, **kwargs: Any):
        self.transition_calls += 1
        return await super().transition_owned_run_atomic(*args, **kwargs)

    async def release_thread_operation_owned(self, *args: Any, **kwargs: Any):
        self.release_calls += 1
        return await super().release_thread_operation_owned(*args, **kwargs)


class _ResolverRegistrationRaceStore(_ControlledAuthoritativeReadStore):
    """Pause ordinary resolver I/O around a contradictory registration."""

    durable_lifecycle = True

    def __init__(self) -> None:
        super().__init__()
        self.pause_get = False
        self.pause_release = False
        self.get_entered = asyncio.Event()
        self.get_continue = asyncio.Event()
        self.release_entered = asyncio.Event()
        self.release_continue = asyncio.Event()
        self.transition_entered = asyncio.Event()
        self.transition_continue = asyncio.Event()
        self.request_cancel_entered = asyncio.Event()
        self.request_cancel_continue = asyncio.Event()
        self.authoritative_entered = asyncio.Event()
        self.authoritative_continue = asyncio.Event()
        self.pause_authoritative = False
        self.pause_transition = False
        self.pause_request_cancel = False

    async def get(self, *args: Any, **kwargs: Any):
        if self.pause_get:
            self.get_entered.set()
            await self.get_continue.wait()
        return await super().get(*args, **kwargs)

    async def authoritative_get(self, run_id: str):
        if self.pause_authoritative:
            self.authoritative_entered.set()
            await self.authoritative_continue.wait()
        return await super().authoritative_get(run_id)

    async def release_thread_operation_owned(self, *args: Any, **kwargs: Any):
        self.release_calls += 1
        result = await MemoryRunStore.release_thread_operation_owned(
            self,
            *args,
            **kwargs,
        )
        if self.pause_release:
            self.release_entered.set()
            await self.release_continue.wait()
        return result

    async def transition_owned_run_atomic(self, *args: Any, **kwargs: Any):
        self.transition_calls += 1
        result = await MemoryRunStore.transition_owned_run_atomic(
            self,
            *args,
            **kwargs,
        )
        if self.pause_transition:
            self.transition_entered.set()
            await self.transition_continue.wait()
        return result

    async def request_cancel_owned(self, *args: Any, **kwargs: Any):
        result = await MemoryRunStore.request_cancel_owned(
            self,
            *args,
            **kwargs,
        )
        if self.pause_request_cancel:
            self.request_cancel_entered.set()
            await self.request_cancel_continue.wait()
        return result


def _normal_candidate(
    *,
    run_id: str,
    thread_id: str,
    commit_proven: bool,
    terminal_disposition: _AdmissionTerminalDisposition = _AdmissionTerminalDisposition.worker_attachment_failed,
    cancellation_action: str | None = None,
) -> _UnresolvedAdmissionCandidate:
    return _UnresolvedAdmissionCandidate(
        run_id=run_id,
        thread_id=thread_id,
        user_id=_RETAINED_USER,
        owner_worker_id=_WORKER,
        external_scope=None,
        external_key=None,
        caller_intent_digest=None,
        caller_intent_digest_version=None,
        commit_proven=commit_proven,
        terminal_disposition=terminal_disposition,
        cancellation_action=cancellation_action,
    )


def _auxiliary_obligation(
    *,
    run_id: str,
    thread_id: str,
) -> _UnresolvedThreadOperationRelease:
    return _UnresolvedThreadOperationRelease(
        run_id=run_id,
        thread_id=thread_id,
        operation_kind=ThreadOperationKind.checkpoint_write,
        user_id=_RETAINED_USER,
        owner_worker_id=_WORKER,
        require_unexpired_lease=False,
    )


def _install_local_phantom(
    manager: RunManager,
    *,
    run_id: str,
    thread_id: str,
    operation_kind: ThreadOperationKind,
) -> RunRecord:
    record = RunRecord(
        run_id=run_id,
        thread_id=thread_id,
        assistant_id=None,
        status=RunStatus.pending,
        on_disconnect=DisconnectMode.continue_,
        operation_kind=operation_kind,
        user_id=_RETAINED_USER,
        owner_worker_id=_WORKER,
        error="retained-local-evidence",
    )
    manager._runs[run_id] = record
    manager._index_run_locked(record)
    return record


async def _create_durable_row(
    store: MemoryRunStore,
    *,
    run_id: str,
    thread_id: str,
    operation_kind: ThreadOperationKind,
    user_id: str,
    status: RunStatus = RunStatus.pending,
) -> dict[str, Any]:
    row, _ = await store.create_thread_operation_atomic(
        run_id,
        thread_id=thread_id,
        owner_worker_id=_WORKER,
        lease_expires_at=None,
        operation_kind=operation_kind.value,
        user_id=user_id,
    )
    row["status"] = status.value
    row["error"] = "foreign-durable-evidence"
    if status not in {RunStatus.pending, RunStatus.running}:
        row["owner_worker_id"] = None
        row["lease_expires_at"] = None
    return row


async def _cancel_compensator(manager: RunManager) -> None:
    task = manager._admission_compensation_task
    if task is not None and not task.done():
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.anyio
@pytest.mark.parametrize("registration_order", ["admission-first", "auxiliary-first"])
async def test_cross_type_post_commit_registration_enters_read_only_quarantine(
    registration_order: Literal["admission-first", "auxiliary-first"],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A same-UUID type contradiction is retained before either resolver mutates."""

    store = _ControlledAuthoritativeReadStore()
    store.authoritative_mode = "unavailable"
    manager = RunManager(store=store, worker_id=_WORKER)
    run_id = f"cross-type-{registration_order}"
    thread_id = f"thread-cross-type-{registration_order}"
    candidate = _normal_candidate(
        run_id=run_id,
        thread_id=thread_id,
        commit_proven=True,
    )
    obligation = _auxiliary_obligation(run_id=run_id, thread_id=thread_id)
    caplog.set_level(logging.ERROR, logger="deerflow.runtime.runs.manager")
    try:
        if registration_order == "admission-first":
            manager._register_unresolved_admission(candidate)
            manager._register_unresolved_thread_operation_release(obligation)
        else:
            manager._register_unresolved_thread_operation_release(obligation)
            manager._register_unresolved_admission(candidate)

        await asyncio.sleep(0)

        assert manager._unresolved_admissions[run_id] is candidate
        assert manager._unresolved_thread_operation_releases[run_id] is obligation
        assert run_id in manager._quarantined_post_commit_obligations
        assert manager.post_commit_obligations_ready() is False
        assert store.transition_calls == 0
        assert store.release_calls == 0
        assert caplog.text.count("code=post_commit_obligation_type_collision") == 1
        assert run_id not in caplog.text
        assert await manager.shutdown(timeout=0.01) is False
    finally:
        await _cancel_compensator(manager)


@pytest.mark.anyio
async def test_cross_type_registration_during_admission_read_blocks_later_mutation() -> None:
    """A stale owner-scoped read cannot authorize mutation after quarantine."""

    store = _ResolverRegistrationRaceStore()
    store.pause_get = True
    manager = RunManager(store=store, worker_id=_WORKER)
    run_id = "cross-type-during-admission-read"
    thread_id = "thread-cross-type-during-admission-read"
    await _create_durable_row(
        store,
        run_id=run_id,
        thread_id=thread_id,
        operation_kind=ThreadOperationKind.run,
        user_id=_RETAINED_USER,
    )
    candidate = _normal_candidate(
        run_id=run_id,
        thread_id=thread_id,
        commit_proven=True,
    )
    obligation = _auxiliary_obligation(run_id=run_id, thread_id=thread_id)
    try:
        manager._register_unresolved_admission(candidate)
        await asyncio.wait_for(store.get_entered.wait(), timeout=1)
        manager._register_unresolved_thread_operation_release(obligation)
        store.authoritative_mode = "unavailable"
        store.get_continue.set()

        assert await manager.drain_post_commit_obligations(timeout=0.05) is False
        row = await store.authoritative_get(run_id) if store.authoritative_mode == "normal" else store._runs[run_id]
        assert row["status"] == RunStatus.pending.value
        assert store.transition_calls == 0
        assert manager._unresolved_admissions[run_id] is candidate
        assert manager._unresolved_thread_operation_releases[run_id] is obligation
        assert run_id in manager._quarantined_post_commit_obligations
        assert manager.post_commit_obligations_ready() is False
    finally:
        store.get_continue.set()
        await _cancel_compensator(manager)


@pytest.mark.anyio
async def test_cross_type_registration_during_dispatched_release_keeps_supervision_until_authoritative_followup() -> None:
    """A stale release response cannot clear collision ownership by itself."""

    store = _ResolverRegistrationRaceStore()
    store.pause_release = True
    store.pause_authoritative = True
    manager = RunManager(store=store, worker_id=_WORKER)
    run_id = "cross-type-during-auxiliary-release"
    thread_id = "thread-cross-type-during-auxiliary-release"
    await _create_durable_row(
        store,
        run_id=run_id,
        thread_id=thread_id,
        operation_kind=ThreadOperationKind.checkpoint_write,
        user_id=_RETAINED_USER,
    )
    candidate = _normal_candidate(
        run_id=run_id,
        thread_id=thread_id,
        commit_proven=True,
    )
    obligation = _auxiliary_obligation(run_id=run_id, thread_id=thread_id)
    try:
        manager._register_unresolved_thread_operation_release(obligation)
        await asyncio.wait_for(store.release_entered.wait(), timeout=1)
        manager._register_unresolved_admission(candidate)
        store.release_continue.set()
        await asyncio.wait_for(store.authoritative_entered.wait(), timeout=1)

        assert run_id not in store._runs
        assert manager._unresolved_admissions[run_id] is candidate
        assert manager._unresolved_thread_operation_releases[run_id] is obligation
        assert run_id in manager._quarantined_post_commit_obligations
        assert manager.post_commit_obligations_ready() is False
        assert store.release_calls == 1

        store.pause_authoritative = False
        store.authoritative_continue.set()
        manager._wake_admission_compensator(reset_backoff=True)
        assert await manager.drain_post_commit_obligations(timeout=1) is True
        assert manager.post_commit_obligations_ready() is True
    finally:
        store.release_continue.set()
        store.authoritative_continue.set()
        await _cancel_compensator(manager)


@pytest.mark.anyio
async def test_cross_type_registration_during_dispatched_transition_keeps_supervision_until_authoritative_followup() -> None:
    """A stale terminal response cannot clear collision ownership by itself."""

    store = _ResolverRegistrationRaceStore()
    store.pause_transition = True
    store.pause_authoritative = True
    manager = RunManager(store=store, worker_id=_WORKER)
    run_id = "cross-type-during-admission-transition"
    thread_id = "thread-cross-type-during-admission-transition"
    await _create_durable_row(
        store,
        run_id=run_id,
        thread_id=thread_id,
        operation_kind=ThreadOperationKind.run,
        user_id=_RETAINED_USER,
    )
    candidate = _normal_candidate(
        run_id=run_id,
        thread_id=thread_id,
        commit_proven=True,
    )
    obligation = _auxiliary_obligation(run_id=run_id, thread_id=thread_id)
    try:
        manager._register_unresolved_admission(candidate)
        await asyncio.wait_for(store.transition_entered.wait(), timeout=1)
        manager._register_unresolved_thread_operation_release(obligation)
        store.transition_continue.set()
        await asyncio.wait_for(store.authoritative_entered.wait(), timeout=1)

        assert store._runs[run_id]["status"] == RunStatus.error.value
        assert manager._unresolved_admissions[run_id] is candidate
        assert manager._unresolved_thread_operation_releases[run_id] is obligation
        assert run_id in manager._quarantined_post_commit_obligations
        assert manager.post_commit_obligations_ready() is False
        assert store.transition_calls == 1
        events = await store.list_lifecycle_events(run_id=run_id)
        assert [event["lifecycle_type"] for event in events] == [
            LifecycleType.accepted.value,
            LifecycleType.failed.value,
        ]

        store.pause_authoritative = False
        store.authoritative_continue.set()
        manager._wake_admission_compensator(reset_backoff=True)
        assert await manager.drain_post_commit_obligations(timeout=1) is True
        assert manager.post_commit_obligations_ready() is True
    finally:
        store.transition_continue.set()
        store.authoritative_continue.set()
        await _cancel_compensator(manager)


@pytest.mark.anyio
async def test_cross_type_registration_after_cancel_request_commit_remains_fail_closed_on_active_truth() -> None:
    """An intermediate cancel commit cannot authorize later quarantined mutation."""

    store = _ResolverRegistrationRaceStore()
    store.pause_request_cancel = True
    store.pause_authoritative = True
    manager = RunManager(store=store, worker_id=_WORKER)
    run_id = "cross-type-after-cancel-request-commit"
    thread_id = "thread-cross-type-after-cancel-request-commit"
    await _create_durable_row(
        store,
        run_id=run_id,
        thread_id=thread_id,
        operation_kind=ThreadOperationKind.run,
        user_id=_RETAINED_USER,
    )
    candidate = _normal_candidate(
        run_id=run_id,
        thread_id=thread_id,
        commit_proven=True,
        terminal_disposition=_AdmissionTerminalDisposition.cancelled,
        cancellation_action="interrupt",
    )
    obligation = _auxiliary_obligation(run_id=run_id, thread_id=thread_id)
    try:
        manager._register_unresolved_admission(candidate)
        await asyncio.wait_for(store.request_cancel_entered.wait(), timeout=1)
        manager._register_unresolved_thread_operation_release(obligation)
        store.request_cancel_continue.set()
        await asyncio.wait_for(store.authoritative_entered.wait(), timeout=1)

        row = store._runs[run_id]
        assert row["status"] == RunStatus.pending.value
        assert row["cancel_action"] == "interrupt"
        assert store.transition_calls == 0
        events = await store.list_lifecycle_events(run_id=run_id)
        assert [event["lifecycle_type"] for event in events] == [
            LifecycleType.accepted.value,
            LifecycleType.cancellation_requested.value,
        ]
        assert manager._unresolved_admissions[run_id] is candidate
        assert manager._unresolved_thread_operation_releases[run_id] is obligation
        assert run_id in manager._quarantined_post_commit_obligations
        assert manager.post_commit_obligations_ready() is False

        store.pause_authoritative = False
        store.authoritative_continue.set()
        manager._wake_admission_compensator(reset_backoff=True)
        assert await manager.drain_post_commit_obligations(timeout=0.05) is False
        assert manager.post_commit_obligations_ready() is False
    finally:
        store.request_cancel_continue.set()
        store.authoritative_continue.set()
        await _cancel_compensator(manager)


@pytest.mark.anyio
@pytest.mark.parametrize("resolver_kind", ["normal", "auxiliary"])
@pytest.mark.parametrize("authoritative_state", ["absent", "terminal"])
async def test_quarantine_rechecks_registration_token_inside_post_read_manager_lock(
    resolver_kind: Literal["normal", "auxiliary"],
    authoritative_state: Literal["absent", "terminal"],
) -> None:
    """A stale global read cannot evict or synchronize a newer local owner."""

    store = _ResolverRegistrationRaceStore()
    store.pause_authoritative = True
    manager = RunManager(store=store, worker_id=_WORKER)
    run_id = f"quarantine-lock-token-{resolver_kind}-{authoritative_state}"
    thread_id = f"thread-quarantine-lock-token-{resolver_kind}-{authoritative_state}"
    operation_kind = ThreadOperationKind.run if resolver_kind == "normal" else ThreadOperationKind.checkpoint_write
    if authoritative_state == "terminal":
        await _create_durable_row(
            store,
            run_id=run_id,
            thread_id=thread_id,
            operation_kind=operation_kind,
            user_id=_RETAINED_USER,
        )
        if operation_kind is ThreadOperationKind.run:
            transitioned = await store.transition_run_atomic(
                run_id,
                expected_state_version=1,
                expected_statuses=(RunStatus.pending.value,),
                transition=LifecycleTransition(
                    lifecycle_type=LifecycleType.failed,
                    status=RunStatus.error.value,
                    error="authoritative terminal evidence",
                ),
                user_id=_RETAINED_USER,
            )
            assert transitioned.applied is True
        else:
            store._runs[run_id]["status"] = RunStatus.error.value
    local = _install_local_phantom(
        manager,
        run_id=run_id,
        thread_id=thread_id,
        operation_kind=operation_kind,
    )
    candidate = _normal_candidate(
        run_id=run_id,
        thread_id=thread_id,
        commit_proven=True,
    )
    obligation = _auxiliary_obligation(run_id=run_id, thread_id=thread_id)
    manager._unresolved_admissions[run_id] = candidate
    manager._unresolved_thread_operation_releases[run_id] = obligation
    manager._quarantined_post_commit_obligations.add(run_id)
    original_token = manager._advance_post_commit_obligation_token(run_id)
    resolver = asyncio.create_task(manager._resolve_unresolved_admission(candidate) if resolver_kind == "normal" else manager._resolve_unresolved_thread_operation_release(obligation))
    manager._admission_compensation_task = resolver
    manager_lock_held = False
    try:
        await asyncio.wait_for(store.authoritative_entered.wait(), timeout=1)
        await manager._lock.acquire()
        manager_lock_held = True
        store.pause_authoritative = False
        store.authoritative_continue.set()
        await asyncio.sleep(0)
        assert resolver.done() is False

        if resolver_kind == "normal":
            manager._register_unresolved_admission(candidate)
        else:
            manager._register_unresolved_thread_operation_release(obligation)
        assert manager._post_commit_obligation_tokens[run_id] is not original_token
        manager._lock.release()
        manager_lock_held = False

        assert await asyncio.wait_for(resolver, timeout=1) is False
        assert manager._runs[run_id] is local
        assert local.error == "retained-local-evidence"
        assert manager.post_commit_obligations_ready() is False
    finally:
        if manager_lock_held:
            manager._lock.release()
        store.authoritative_continue.set()
        if not resolver.done():
            resolver.cancel()
            await asyncio.gather(resolver, return_exceptions=True)
        await _cancel_compensator(manager)


@pytest.mark.anyio
async def test_ordinary_terminal_read_rechecks_token_inside_sync_lock() -> None:
    """A newer registration invalidates terminal truth before local projection."""

    store = _ResolverRegistrationRaceStore()
    store.pause_get = True
    manager = RunManager(store=store, worker_id=_WORKER)
    run_id = "ordinary-terminal-lock-token"
    thread_id = "thread-ordinary-terminal-lock-token"
    await _create_durable_row(
        store,
        run_id=run_id,
        thread_id=thread_id,
        operation_kind=ThreadOperationKind.run,
        user_id=_RETAINED_USER,
    )
    transitioned = await store.transition_run_atomic(
        run_id,
        expected_state_version=1,
        expected_statuses=(RunStatus.pending.value,),
        transition=LifecycleTransition(
            lifecycle_type=LifecycleType.failed,
            status=RunStatus.error.value,
            error="authoritative terminal evidence",
        ),
        user_id=_RETAINED_USER,
    )
    assert transitioned.applied is True
    local = _install_local_phantom(
        manager,
        run_id=run_id,
        thread_id=thread_id,
        operation_kind=ThreadOperationKind.run,
    )
    candidate = _normal_candidate(
        run_id=run_id,
        thread_id=thread_id,
        commit_proven=True,
    )
    manager._unresolved_admissions[run_id] = candidate
    original_token = manager._advance_post_commit_obligation_token(run_id)
    resolver = asyncio.create_task(manager._resolve_unresolved_admission(candidate))
    manager._admission_compensation_task = resolver
    manager_lock_held = False
    try:
        await asyncio.wait_for(store.get_entered.wait(), timeout=1)
        await manager._lock.acquire()
        manager_lock_held = True
        store.get_continue.set()
        await asyncio.sleep(0)
        assert resolver.done() is False
        manager._register_unresolved_admission(candidate)
        assert manager._post_commit_obligation_tokens[run_id] is not original_token
        manager._lock.release()
        manager_lock_held = False

        assert await asyncio.wait_for(resolver, timeout=1) is False
        assert manager._runs[run_id] is local
        assert local.error == "retained-local-evidence"
    finally:
        if manager_lock_held:
            manager._lock.release()
        store.get_continue.set()
        if not resolver.done():
            resolver.cancel()
            await asyncio.gather(resolver, return_exceptions=True)
        await _cancel_compensator(manager)


@pytest.mark.anyio
@pytest.mark.parametrize("resolver_kind", ["normal", "auxiliary"])
async def test_no_store_compatibility_rechecks_token_inside_manager_lock(
    resolver_kind: Literal["normal", "auxiliary"],
) -> None:
    """Process-local compatibility cannot mutate after newer registration."""

    manager = RunManager(store=None, worker_id=_WORKER)
    run_id = f"no-store-lock-token-{resolver_kind}"
    thread_id = f"thread-no-store-lock-token-{resolver_kind}"
    operation_kind = ThreadOperationKind.run if resolver_kind == "normal" else ThreadOperationKind.checkpoint_write
    local = _install_local_phantom(
        manager,
        run_id=run_id,
        thread_id=thread_id,
        operation_kind=operation_kind,
    )
    candidate = _normal_candidate(
        run_id=run_id,
        thread_id=thread_id,
        commit_proven=False,
    )
    obligation = _auxiliary_obligation(run_id=run_id, thread_id=thread_id)
    if resolver_kind == "normal":
        manager._unresolved_admissions[run_id] = candidate
    else:
        manager._unresolved_thread_operation_releases[run_id] = obligation
    original_token = manager._advance_post_commit_obligation_token(run_id)
    await manager._lock.acquire()
    resolver = asyncio.create_task(manager._resolve_unresolved_admission(candidate) if resolver_kind == "normal" else manager._resolve_unresolved_thread_operation_release(obligation))
    try:
        await asyncio.sleep(0)
        assert resolver.done() is False
        manager._advance_post_commit_obligation_token(run_id)
        assert manager._post_commit_obligation_tokens[run_id] is not original_token
        manager._lock.release()

        assert await asyncio.wait_for(resolver, timeout=1) is False
        assert manager._runs[run_id] is local
        assert local.error == "retained-local-evidence"
    finally:
        if manager._lock.locked():
            manager._lock.release()
        if not resolver.done():
            resolver.cancel()
            await asyncio.gather(resolver, return_exceptions=True)


@pytest.mark.anyio
async def test_post_commit_status_and_logs_are_bounded_transition_only(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Operational evidence exposes counts, never retained identity fields."""

    marker = "secret-owner-and-run-marker"
    store = _ControlledAuthoritativeReadStore()
    store.authoritative_mode = "unavailable"
    manager = RunManager(store=store, worker_id=_WORKER)
    candidate = _normal_candidate(
        run_id=marker,
        thread_id="thread-post-commit-transition-log",
        commit_proven=False,
    )
    caplog.set_level(logging.INFO, logger="deerflow.runtime.runs.manager")
    try:
        manager._register_unresolved_admission(candidate)
        manager._register_unresolved_admission(candidate)
        status = manager.post_commit_obligation_status()

        assert status.pending_admissions == 1
        assert status.pending_thread_operation_releases == 0
        assert status.pending_quarantines == 0
        assert caplog.text.count("code=post_commit_obligations_pending") == 1
        assert marker not in caplog.text

        store.authoritative_mode = "normal"
        manager._wake_admission_compensator(reset_backoff=True)
        assert await manager.drain_post_commit_obligations(timeout=1) is True
        status = manager.post_commit_obligation_status()

        assert status.pending_admissions == 0
        assert status.resolved_admissions_since_start == 1
        assert caplog.text.count("code=post_commit_obligations_cleared") == 1
        assert marker not in caplog.text

        manager._resolved_admissions_since_start = 2_147_483_648
        assert manager.post_commit_obligation_status().resolved_admissions_since_start == 2_147_483_647
    finally:
        await _cancel_compensator(manager)


@pytest.mark.anyio
@pytest.mark.parametrize("resolver_kind", ["normal", "auxiliary"])
async def test_cross_type_quarantine_without_authoritative_store_stays_unresolved(
    resolver_kind: Literal["normal", "auxiliary"],
) -> None:
    """Compatibility mode cannot guess global absence for contradictory evidence."""

    manager = RunManager(store=None, worker_id=_WORKER)
    run_id = f"cross-type-no-store-{resolver_kind}"
    thread_id = f"thread-cross-type-no-store-{resolver_kind}"
    candidate = _normal_candidate(
        run_id=run_id,
        thread_id=thread_id,
        commit_proven=True,
    )
    obligation = _auxiliary_obligation(run_id=run_id, thread_id=thread_id)
    manager._unresolved_admissions[run_id] = candidate
    manager._unresolved_thread_operation_releases[run_id] = obligation
    manager._quarantined_post_commit_obligations.add(run_id)

    if resolver_kind == "normal":
        assert await manager._resolve_unresolved_admission(candidate) is False
    else:
        assert await manager._resolve_unresolved_thread_operation_release(obligation) is False

    assert manager.post_commit_obligations_ready() is False
    assert manager._unresolved_admissions[run_id] is candidate
    assert manager._unresolved_thread_operation_releases[run_id] is obligation


@pytest.mark.anyio
@pytest.mark.parametrize(
    "authoritative_kind",
    [ThreadOperationKind.run, ThreadOperationKind.checkpoint_write],
)
@pytest.mark.parametrize("authoritative_state", ["absent", "active", "terminal"])
async def test_cross_type_quarantine_uses_only_authoritative_primary_key_truth(
    authoritative_kind: ThreadOperationKind,
    authoritative_state: Literal["absent", "active", "terminal"],
) -> None:
    """Both obligation kinds remain read-only until global truth is safe."""

    store = _ControlledAuthoritativeReadStore()
    manager = RunManager(store=store, worker_id=_WORKER)
    run_id = f"cross-type-{authoritative_kind.value}-{authoritative_state}"
    thread_id = f"thread-cross-type-{authoritative_kind.value}-{authoritative_state}"
    if authoritative_state != "absent":
        await _create_durable_row(
            store,
            run_id=run_id,
            thread_id=thread_id,
            operation_kind=authoritative_kind,
            user_id=_RETAINED_USER,
        )
    if authoritative_state == "terminal":
        if authoritative_kind is ThreadOperationKind.run:
            transitioned = await store.transition_run_atomic(
                run_id,
                expected_state_version=1,
                expected_statuses=(RunStatus.pending.value,),
                transition=LifecycleTransition(
                    lifecycle_type=LifecycleType.failed,
                    status=RunStatus.error.value,
                    error="terminalized through lifecycle contract",
                ),
                user_id=_RETAINED_USER,
            )
            assert transitioned.applied is True
        else:
            # Auxiliary reservations intentionally have no lifecycle journal;
            # direct mutation here models store corruption/orphan takeover.
            store._runs[run_id]["status"] = RunStatus.error.value

    candidate = _normal_candidate(
        run_id=run_id,
        thread_id=thread_id,
        commit_proven=True,
    )
    obligation = _auxiliary_obligation(run_id=run_id, thread_id=thread_id)
    manager._unresolved_admissions[run_id] = candidate
    manager._unresolved_thread_operation_releases[run_id] = obligation
    manager._quarantined_post_commit_obligations.add(run_id)

    normal_resolved = await manager._resolve_unresolved_admission(candidate)
    auxiliary_resolved = await manager._resolve_unresolved_thread_operation_release(obligation)

    if authoritative_state == "active":
        assert normal_resolved is False
        assert auxiliary_resolved is False
        assert manager.post_commit_obligations_ready() is False
    else:
        assert normal_resolved is True
        assert auxiliary_resolved is True
        manager._unresolved_admissions.pop(run_id)
        manager._discard_resolved_post_commit_integrity(run_id)
        assert run_id in manager._quarantined_post_commit_obligations
        manager._unresolved_thread_operation_releases.pop(run_id)
        manager._discard_resolved_post_commit_integrity(run_id)
        assert manager.post_commit_obligations_ready() is True
    assert store.transition_calls == 0
    assert store.release_calls == 0


@pytest.mark.anyio
async def test_terminal_quarantine_reconciliation_preserves_lifecycle_transition_page() -> None:
    """Normal terminal truth is materialized through the lifecycle contract."""

    store = _ControlledAuthoritativeReadStore()
    manager = RunManager(store=store, worker_id=_WORKER)
    run_id = "quarantine-terminal-lifecycle-contract"
    thread_id = "thread-quarantine-terminal-lifecycle-contract"
    await _create_durable_row(
        store,
        run_id=run_id,
        thread_id=thread_id,
        operation_kind=ThreadOperationKind.run,
        user_id=_RETAINED_USER,
    )
    transitioned = await store.transition_run_atomic(
        run_id,
        expected_state_version=1,
        expected_statuses=(RunStatus.pending.value,),
        transition=LifecycleTransition(
            lifecycle_type=LifecycleType.failed,
            status=RunStatus.error.value,
            error="terminalized through lifecycle contract",
        ),
        user_id=_RETAINED_USER,
    )
    assert transitioned.applied is True
    before_row = copy.deepcopy(await store.authoritative_get(run_id))
    before_page = copy.deepcopy(
        await store.query_lifecycle(
            LifecycleQuery(
                run_id=run_id,
                owner_scope=lifecycle_owner_scope(_RETAINED_USER),
            )
        )
    )
    local = _install_local_phantom(
        manager,
        run_id=run_id,
        thread_id=thread_id,
        operation_kind=ThreadOperationKind.run,
    )
    candidate = _normal_candidate(
        run_id=run_id,
        thread_id=thread_id,
        commit_proven=True,
    )
    manager._quarantined_post_commit_obligations.add(run_id)

    assert await manager._resolve_unresolved_admission(candidate) is True

    after_row = await store.authoritative_get(run_id)
    after_page = await store.query_lifecycle(
        LifecycleQuery(
            run_id=run_id,
            owner_scope=lifecycle_owner_scope(_RETAINED_USER),
        )
    )
    assert after_row == before_row
    assert after_page == before_page
    assert [event["lifecycle_type"] for event in after_page.events] == [
        LifecycleType.accepted.value,
        LifecycleType.failed.value,
    ]
    assert after_page.snapshots[0]["state_version"] == 2
    assert local.status is RunStatus.error


@pytest.mark.anyio
@pytest.mark.parametrize("commit_proven", [False, True])
async def test_normal_quarantine_keeps_cross_owner_active_row_supervised(
    commit_proven: bool,
) -> None:
    """Owner-filtered invisibility cannot prove an exact UUID globally absent."""

    store = _ControlledAuthoritativeReadStore()
    manager = RunManager(store=store, worker_id=_WORKER)
    run_id = f"quarantine-normal-active-{commit_proven}"
    thread_id = f"thread-quarantine-normal-{commit_proven}"
    await _create_durable_row(
        store,
        run_id=run_id,
        thread_id=thread_id,
        operation_kind=ThreadOperationKind.run,
        user_id=_DURABLE_USER,
    )
    candidate = _normal_candidate(
        run_id=run_id,
        thread_id=thread_id,
        commit_proven=commit_proven,
    )
    manager._unresolved_admissions[run_id] = candidate
    manager._quarantined_post_commit_obligations.add(run_id)

    assert await manager._resolve_unresolved_admission(candidate) is False
    assert store.read_scopes == [None]
    assert manager.admission_compensations_ready() is False
    assert await manager.shutdown(timeout=0.01) is False
    retained = await MemoryRunStore.get(store, run_id)
    assert retained is not None
    assert retained["status"] == RunStatus.pending.value

    await _cancel_compensator(manager)


@pytest.mark.anyio
async def test_auxiliary_quarantine_keeps_cross_owner_active_row_supervised() -> None:
    store = _ControlledAuthoritativeReadStore()
    manager = RunManager(store=store, worker_id=_WORKER)
    run_id = "quarantine-auxiliary-active"
    thread_id = "thread-quarantine-auxiliary-active"
    await _create_durable_row(
        store,
        run_id=run_id,
        thread_id=thread_id,
        operation_kind=ThreadOperationKind.checkpoint_write,
        user_id=_DURABLE_USER,
    )
    obligation = _auxiliary_obligation(run_id=run_id, thread_id=thread_id)
    manager._unresolved_thread_operation_releases[run_id] = obligation
    manager._quarantined_post_commit_obligations.add(run_id)

    assert await manager._resolve_unresolved_thread_operation_release(obligation) is False
    assert store.read_scopes == [None]
    assert manager.admission_compensations_ready() is False
    assert await manager.shutdown(timeout=0.01) is False
    retained = await MemoryRunStore.get(store, run_id)
    assert retained is not None
    assert retained["status"] == RunStatus.pending.value

    await _cancel_compensator(manager)


@pytest.mark.anyio
@pytest.mark.parametrize("commit_proven", [False, True])
async def test_normal_quarantine_accepts_only_global_absence(
    commit_proven: bool,
) -> None:
    store = _ControlledAuthoritativeReadStore()
    manager = RunManager(store=store, worker_id=_WORKER)
    run_id = f"quarantine-normal-absent-{commit_proven}"
    thread_id = f"thread-quarantine-absent-{commit_proven}"
    candidate = _normal_candidate(
        run_id=run_id,
        thread_id=thread_id,
        commit_proven=commit_proven,
    )
    _install_local_phantom(
        manager,
        run_id=run_id,
        thread_id=thread_id,
        operation_kind=ThreadOperationKind.run,
    )
    manager._quarantined_post_commit_obligations.add(run_id)

    assert await manager._resolve_unresolved_admission(candidate) is True
    assert store.read_scopes == [None]
    assert run_id not in manager._runs
    assert run_id not in manager._runs_by_thread.get(thread_id, {})


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("operation_kind", "resolver_kind"),
    [
        pytest.param(ThreadOperationKind.run, "normal", id="normal"),
        pytest.param(
            ThreadOperationKind.checkpoint_write,
            "auxiliary",
            id="auxiliary",
        ),
    ],
)
@pytest.mark.parametrize("durable_user", [_RETAINED_USER, _DURABLE_USER])
async def test_quarantine_terminal_row_clears_without_cross_owner_sync(
    operation_kind: ThreadOperationKind,
    resolver_kind: Literal["normal", "auxiliary"],
    durable_user: str,
) -> None:
    store = _ControlledAuthoritativeReadStore()
    manager = RunManager(store=store, worker_id=_WORKER)
    suffix = f"{resolver_kind}-{'matching' if durable_user == _RETAINED_USER else 'conflicting'}"
    run_id = f"quarantine-terminal-{suffix}"
    thread_id = f"thread-quarantine-terminal-{suffix}"
    await _create_durable_row(
        store,
        run_id=run_id,
        thread_id=thread_id,
        operation_kind=operation_kind,
        user_id=durable_user,
        status=RunStatus.error,
    )
    local = _install_local_phantom(
        manager,
        run_id=run_id,
        thread_id=thread_id,
        operation_kind=operation_kind,
    )
    manager._quarantined_post_commit_obligations.add(run_id)

    if resolver_kind == "normal":
        result = await manager._resolve_unresolved_admission(
            _normal_candidate(
                run_id=run_id,
                thread_id=thread_id,
                commit_proven=True,
            )
        )
    else:
        result = await manager._resolve_unresolved_thread_operation_release(_auxiliary_obligation(run_id=run_id, thread_id=thread_id))

    assert result is True
    assert store.read_scopes == [None]
    if durable_user == _RETAINED_USER and resolver_kind == "normal":
        assert manager._runs[run_id].status is RunStatus.error
        assert manager._runs[run_id].error == "foreign-durable-evidence"
    else:
        assert run_id not in manager._runs
        assert local.user_id == _RETAINED_USER
        assert local.error == "retained-local-evidence"


@pytest.mark.anyio
@pytest.mark.parametrize("resolver_kind", ["normal", "auxiliary"])
@pytest.mark.parametrize(
    "authoritative_mode",
    ["unavailable", "malformed", "unsupported-status"],
)
async def test_quarantine_retains_obligation_for_indeterminate_authoritative_read(
    resolver_kind: Literal["normal", "auxiliary"],
    authoritative_mode: Literal[
        "unavailable",
        "malformed",
        "unsupported-status",
    ],
) -> None:
    store = _ControlledAuthoritativeReadStore()
    store.authoritative_mode = authoritative_mode
    manager = RunManager(store=store, worker_id=_WORKER)
    run_id = f"quarantine-{resolver_kind}-{authoritative_mode}"
    thread_id = f"thread-quarantine-{resolver_kind}-{authoritative_mode}"
    operation_kind = ThreadOperationKind.run if resolver_kind == "normal" else ThreadOperationKind.checkpoint_write
    await _create_durable_row(
        store,
        run_id=run_id,
        thread_id=thread_id,
        operation_kind=operation_kind,
        user_id=_RETAINED_USER,
    )
    local = _install_local_phantom(
        manager,
        run_id=run_id,
        thread_id=thread_id,
        operation_kind=operation_kind,
    )
    blocker = asyncio.Event()
    local.task = asyncio.create_task(blocker.wait())
    manager._quarantined_post_commit_obligations.add(run_id)

    if resolver_kind == "normal":
        result = await manager._resolve_unresolved_admission(
            _normal_candidate(
                run_id=run_id,
                thread_id=thread_id,
                commit_proven=True,
            )
        )
    else:
        result = await manager._resolve_unresolved_thread_operation_release(_auxiliary_obligation(run_id=run_id, thread_id=thread_id))

    assert result is False
    assert store.read_scopes == [None]
    assert local.abort_event.is_set()
    assert local.task.cancelled() or local.task.cancelling()

    await asyncio.gather(local.task, return_exceptions=True)


@pytest.mark.anyio
async def test_conflicting_terminal_truth_fences_local_task_before_eviction() -> None:
    store = _ControlledAuthoritativeReadStore()
    manager = RunManager(store=store, worker_id=_WORKER)
    run_id = "quarantine-terminal-live-task"
    thread_id = "thread-quarantine-terminal-live-task"
    await _create_durable_row(
        store,
        run_id=run_id,
        thread_id=thread_id,
        operation_kind=ThreadOperationKind.run,
        user_id=_DURABLE_USER,
        status=RunStatus.error,
    )
    local = _install_local_phantom(
        manager,
        run_id=run_id,
        thread_id=thread_id,
        operation_kind=ThreadOperationKind.run,
    )
    blocker = asyncio.Event()
    local.task = asyncio.create_task(blocker.wait())
    candidate = _normal_candidate(
        run_id=run_id,
        thread_id=thread_id,
        commit_proven=True,
    )
    manager._quarantined_post_commit_obligations.add(run_id)

    assert await manager._resolve_unresolved_admission(candidate) is False
    assert local.abort_event.is_set()
    assert local.task.cancelled() or local.task.cancelling()
    assert run_id in manager._runs

    await asyncio.gather(local.task, return_exceptions=True)
    assert await manager._resolve_unresolved_admission(candidate) is True
    assert run_id not in manager._runs


@pytest.mark.anyio
async def test_quarantined_taskless_phantom_cannot_renew_conflicting_row() -> None:
    store = _ControlledAuthoritativeReadStore()
    manager = RunManager(
        store=store,
        worker_id=_WORKER,
        run_ownership_config=RunOwnershipConfig(
            lease_seconds=30,
            grace_seconds=0,
            heartbeat_enabled=True,
        ),
    )
    run_id = "quarantine-taskless-no-renewal"
    thread_id = "thread-quarantine-taskless-no-renewal"
    await _create_durable_row(
        store,
        run_id=run_id,
        thread_id=thread_id,
        operation_kind=ThreadOperationKind.run,
        user_id=_DURABLE_USER,
    )
    future_expiry = (datetime.now(UTC) + timedelta(seconds=30)).isoformat()
    store._runs[run_id]["lease_expires_at"] = future_expiry
    local = _install_local_phantom(
        manager,
        run_id=run_id,
        thread_id=thread_id,
        operation_kind=ThreadOperationKind.run,
    )
    local.lease_expires_at = future_expiry
    local.attachment_supervised = True
    candidate = _normal_candidate(
        run_id=run_id,
        thread_id=thread_id,
        commit_proven=True,
    )
    manager._unresolved_admissions[run_id] = candidate
    manager._quarantined_post_commit_obligations.add(run_id)

    assert await manager._resolve_unresolved_admission(candidate) is False
    await manager._renew_leases()

    assert local.abort_event.is_set()
    assert store.renewal_calls == 0
    assert store._runs[run_id]["lease_expires_at"] == future_expiry


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("field_name", "malformed"),
    [
        pytest.param("state_version", object(), id="state-version-object"),
        pytest.param("state_version", True, id="state-version-bool"),
        pytest.param("state_version", -1, id="state-version-negative"),
        pytest.param("thread_id", "x" * 4097, id="thread-id-unbounded"),
        pytest.param("error", {}, id="error-mapping"),
        pytest.param("stop_reason", [], id="stop-reason-list"),
        pytest.param("lease_expires_at", 1, id="lease-integer"),
        pytest.param("updated_at", object(), id="updated-at-object"),
        pytest.param(
            "execution_evidence_digest",
            "a" * 64,
            id="orphan-evidence-digest",
        ),
    ],
)
async def test_matching_terminal_malformed_projection_stays_quarantined(
    field_name: str,
    malformed: Any,
) -> None:
    store = _ControlledAuthoritativeReadStore()
    manager = RunManager(store=store, worker_id=_WORKER)
    run_id = f"quarantine-malformed-terminal-{field_name}"
    thread_id = f"thread-quarantine-malformed-{field_name}"
    await _create_durable_row(
        store,
        run_id=run_id,
        thread_id=thread_id,
        operation_kind=ThreadOperationKind.run,
        user_id=_RETAINED_USER,
        status=RunStatus.error,
    )
    store._runs[run_id][field_name] = malformed
    local = _install_local_phantom(
        manager,
        run_id=run_id,
        thread_id=thread_id,
        operation_kind=ThreadOperationKind.run,
    )
    candidate = _normal_candidate(
        run_id=run_id,
        thread_id=thread_id,
        commit_proven=True,
    )
    manager._unresolved_admissions[run_id] = candidate
    manager._quarantined_post_commit_obligations.add(run_id)

    assert await manager._resolve_unresolved_admission(candidate) is False
    assert manager.post_commit_obligations_ready() is False
    assert local.status is RunStatus.pending
    assert local.state_version == 0
    assert local.error == "retained-local-evidence"


@pytest.mark.anyio
@pytest.mark.parametrize("malformation", ["wrong-run", "digest-mismatch"])
async def test_matching_terminal_rejects_relationally_corrupt_execution_evidence(
    malformation: Literal["wrong-run", "digest-mismatch"],
) -> None:
    store = _ControlledAuthoritativeReadStore()
    manager = RunManager(store=store, worker_id=_WORKER)
    run_id = f"quarantine-evidence-{malformation}"
    thread_id = f"thread-quarantine-evidence-{malformation}"
    await _create_durable_row(
        store,
        run_id=run_id,
        thread_id=thread_id,
        operation_kind=ThreadOperationKind.run,
        user_id=_RETAINED_USER,
        status=RunStatus.error,
    )
    evidence_run_id = "different-run" if malformation == "wrong-run" else run_id
    evidence, digest = _execution_evidence(evidence_run_id)
    store._runs[run_id]["execution_evidence_json"] = evidence
    store._runs[run_id]["execution_evidence_digest"] = "0" * 64 if malformation == "digest-mismatch" else digest
    local = _install_local_phantom(
        manager,
        run_id=run_id,
        thread_id=thread_id,
        operation_kind=ThreadOperationKind.run,
    )
    candidate = _normal_candidate(
        run_id=run_id,
        thread_id=thread_id,
        commit_proven=True,
    )
    manager._unresolved_admissions[run_id] = candidate
    manager._quarantined_post_commit_obligations.add(run_id)

    assert await manager._resolve_unresolved_admission(candidate) is False
    assert manager.post_commit_obligations_ready() is False
    assert local.execution_evidence_json is None
    assert local.execution_evidence_digest is None


@pytest.mark.anyio
@pytest.mark.parametrize("resolver_kind", ["normal", "auxiliary"])
async def test_sql_quarantine_uses_global_primary_key_truth(
    tmp_path: Path,
    resolver_kind: Literal["normal", "auxiliary"],
) -> None:
    """SQLite exercises the same unscoped repository seam as PostgreSQL."""

    from deerflow.persistence.engine import close_engine, get_session_factory, init_engine
    from deerflow.persistence.run import RunRepository

    await init_engine(
        "sqlite",
        url=f"sqlite+aiosqlite:///{tmp_path / 'quarantine.db'}",
        sqlite_dir=str(tmp_path),
    )
    store = RunRepository(get_session_factory())
    manager = RunManager(store=store, worker_id=_WORKER)
    operation_kind = ThreadOperationKind.run if resolver_kind == "normal" else ThreadOperationKind.artifact_write
    run_id = f"sql-quarantine-{resolver_kind}"
    thread_id = f"thread-sql-quarantine-{resolver_kind}"
    try:
        await store.create_thread_operation_atomic(
            run_id,
            thread_id=thread_id,
            owner_worker_id=_WORKER,
            lease_expires_at=None,
            operation_kind=operation_kind.value,
            user_id=_DURABLE_USER,
        )
        manager._quarantined_post_commit_obligations.add(run_id)

        if resolver_kind == "normal":
            resolved = await manager._resolve_unresolved_admission(
                _normal_candidate(
                    run_id=run_id,
                    thread_id=thread_id,
                    commit_proven=True,
                )
            )
        else:
            obligation = _UnresolvedThreadOperationRelease(
                run_id=run_id,
                thread_id=thread_id,
                operation_kind=operation_kind,
                user_id=_RETAINED_USER,
                owner_worker_id=_WORKER,
                require_unexpired_lease=False,
            )
            resolved = await manager._resolve_unresolved_thread_operation_release(obligation)

        assert resolved is False
        assert await store.get(run_id, user_id=_RETAINED_USER) is None
        authoritative = await store.authoritative_get(run_id)
        assert authoritative is not None
        assert authoritative["status"] == RunStatus.pending.value
        assert manager.post_commit_obligations_ready() is False
    finally:
        await _cancel_compensator(manager)
        await close_engine()


@pytest.mark.anyio
@pytest.mark.postgres_contract
@pytest.mark.skipif(
    not _POSTGRES_URL,
    reason="requires DEERFLOW_TEST_POSTGRES_URL for PostgreSQL quarantine qualification",
)
@pytest.mark.parametrize("resolver_kind", ["normal", "auxiliary"])
@pytest.mark.parametrize(
    "authoritative_state",
    ["absent", "active", "matching-terminal", "conflicting-terminal"],
)
async def test_postgres_quarantine_uses_global_primary_key_state_matrix(
    resolver_kind: Literal["normal", "auxiliary"],
    authoritative_state: Literal[
        "absent",
        "active",
        "matching-terminal",
        "conflicting-terminal",
    ],
) -> None:
    """The production store distinguishes global absence, activity, and terminal truth."""

    from sqlalchemy import delete, update
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
    from support.postgres import postgres_async_url

    from deerflow.persistence.base import Base
    from deerflow.persistence.run.model import RunRow
    from deerflow.persistence.run.sql import RunRepository

    assert _POSTGRES_URL is not None
    engine = create_async_engine(postgres_async_url(_POSTGRES_URL))
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    store = RunRepository(factory)
    manager = RunManager(store=store, worker_id=_WORKER)
    run_id = str(uuid.uuid4())
    thread_id = f"thread-quarantine-pg-{uuid.uuid4().hex}"
    operation_kind = ThreadOperationKind.run if resolver_kind == "normal" else ThreadOperationKind.artifact_write
    durable_user = _RETAINED_USER if authoritative_state == "matching-terminal" else _DURABLE_USER
    try:
        if authoritative_state != "absent":
            await store.create_thread_operation_atomic(
                run_id,
                thread_id=thread_id,
                owner_worker_id=_WORKER,
                lease_expires_at=None,
                operation_kind=operation_kind.value,
                user_id=durable_user,
            )
        lifecycle_before = None
        if authoritative_state.endswith("terminal"):
            if operation_kind is ThreadOperationKind.run:
                transitioned = await store.transition_run_atomic(
                    run_id,
                    expected_state_version=1,
                    expected_statuses=(RunStatus.pending.value,),
                    transition=LifecycleTransition(
                        lifecycle_type=LifecycleType.failed,
                        status=RunStatus.error.value,
                        error="terminalized through lifecycle contract",
                    ),
                    user_id=durable_user,
                )
                assert transitioned.applied is True
                lifecycle_before = await store.list_lifecycle_events(run_id=run_id)
            else:
                # Auxiliary reservations never emit invocation lifecycle rows;
                # this direct update is explicit corruption evidence only.
                async with factory() as session:
                    await session.execute(update(RunRow).where(RunRow.run_id == run_id).values(status=RunStatus.error.value, state_version=1))
                    await session.commit()

        _install_local_phantom(
            manager,
            run_id=run_id,
            thread_id=thread_id,
            operation_kind=operation_kind,
        )
        manager._quarantined_post_commit_obligations.add(run_id)
        if resolver_kind == "normal":
            candidate = _normal_candidate(
                run_id=run_id,
                thread_id=thread_id,
                commit_proven=True,
            )
            manager._unresolved_admissions[run_id] = candidate
            resolved = await manager._resolve_unresolved_admission(candidate)
        else:
            obligation = _UnresolvedThreadOperationRelease(
                run_id=run_id,
                thread_id=thread_id,
                operation_kind=operation_kind,
                user_id=_RETAINED_USER,
                owner_worker_id=_WORKER,
                require_unexpired_lease=False,
            )
            manager._unresolved_thread_operation_releases[run_id] = obligation
            resolved = await manager._resolve_unresolved_thread_operation_release(
                obligation,
            )

        if authoritative_state == "active":
            assert resolved is False
            assert manager.post_commit_obligations_ready() is False
            authoritative = await store.authoritative_get(run_id)
            assert authoritative is not None
            assert authoritative["status"] == RunStatus.pending.value
        else:
            assert resolved is True
            assert authoritative_state == "matching-terminal" or run_id not in manager._runs
        if lifecycle_before is not None:
            assert await store.list_lifecycle_events(run_id=run_id) == lifecycle_before
            assert [event["lifecycle_type"] for event in lifecycle_before] == [LifecycleType.accepted.value, LifecycleType.failed.value]
    finally:
        await _cancel_compensator(manager)
        async with factory() as session:
            await session.execute(delete(RunRow).where(RunRow.run_id == run_id))
            await session.commit()
        await engine.dispose()
