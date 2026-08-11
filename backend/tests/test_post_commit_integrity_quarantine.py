"""Fail-closed reconciliation contracts for contradictory post-commit evidence."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal

import pytest

from deerflow.config.run_ownership_config import RunOwnershipConfig
from deerflow.runtime import DisconnectMode, RunManager, RunStatus, ThreadOperationKind
from deerflow.runtime.runs.manager import (
    RunRecord,
    _UnresolvedAdmissionCandidate,
    _UnresolvedThreadOperationRelease,
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


def _normal_candidate(
    *,
    run_id: str,
    thread_id: str,
    commit_proven: bool,
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
    return row


async def _cancel_compensator(manager: RunManager) -> None:
    task = manager._admission_compensation_task
    if task is not None and not task.done():
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


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
        if authoritative_state.endswith("terminal"):
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
    finally:
        await _cancel_compensator(manager)
        async with factory() as session:
            await session.execute(delete(RunRow).where(RunRow.run_id == run_id))
            await session.commit()
        await engine.dispose()
