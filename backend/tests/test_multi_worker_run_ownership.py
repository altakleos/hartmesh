"""Tests for multi-worker run ownership (work items 2–3).

Coverage:
- create_or_reject with reject strategy blocks duplicate active runs
- create_or_reject with interrupt strategy claims and cancels old runs
- create_thread_operation_atomic refuses to interrupt a run owned by another live worker
- reconcile_orphaned_inflight_runs uses lease-based detection
- periodic reconciliation notifies Gateway recovery orchestration
- Worker reconciliation skips runs with unexpired leases
- Lease heartbeat renews active run leases
- GATEWAY_WORKERS=1 + heartbeat_enabled=false behaviour unchanged
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest

from deerflow.config.run_ownership_config import RunOwnershipConfig
from deerflow.runtime import ORPHAN_RECOVERY_STOP_REASON, RunManager, RunStatus, ThreadOperationKind
from deerflow.runtime.runs.manager import (
    CancelOutcome,
    ConflictError,
    RunStartupError,
    _generate_worker_id,
)
from deerflow.runtime.runs.store.base import (
    CancellationRequestOutcome,
    ExecutionTakeoverOutcome,
    LeaseClockAuthority,
    LifecycleTransition,
    LifecycleType,
    RecoveryPolicy,
)
from deerflow.runtime.runs.store.memory import MemoryRunStore

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _lease_config(**kwargs) -> RunOwnershipConfig:
    return RunOwnershipConfig(
        lease_seconds=kwargs.get("lease_seconds", 30),
        grace_seconds=kwargs.get("grace_seconds", 10),
        heartbeat_enabled=kwargs.get("heartbeat_enabled", False),
    )


def _timing_lease_config(*, lease_seconds: int = 1) -> RunOwnershipConfig:
    """Build a compact integer TTL for deterministic manager timing tests."""

    return RunOwnershipConfig.model_construct(
        lease_seconds=lease_seconds,
        grace_seconds=0,
        heartbeat_enabled=True,
    )


def _execution_recovery_payload(thread_id: str) -> dict[str, object]:
    return {
        "version": 1,
        "input_kind": "graph",
        "input_value": {"messages": []},
        "config": {"configurable": {"thread_id": thread_id}},
        "stream_modes": ["values"],
        "stream_subgraphs": False,
        "interrupt_before": None,
        "interrupt_after": None,
    }


def _make_manager(store=None, **kwargs) -> RunManager:
    return RunManager(
        store=store or MemoryRunStore(),
        run_ownership_config=kwargs.pop("run_ownership_config", _lease_config()),
        **kwargs,
    )


class CapturingDatabaseClockStore(MemoryRunStore):
    """Exercise the manager's database-clock protocol without a live Postgres."""

    # The topology boundary accepts the public wire value as well as StrEnum;
    # the manager must normalize it identically.
    lease_clock_authority = LeaseClockAuthority.database_v1.value

    def __init__(self) -> None:
        super().__init__()
        self.create_lease_arguments: list[tuple[str | None, int | None]] = []
        self.ensure_lease_arguments: list[tuple[str | None, int | None]] = []
        self.renew_lease_arguments: list[tuple[str | None, int | None]] = []
        self.renewal_delay_seconds = 0.0

    async def create_thread_operation_atomic(
        self,
        run_id,
        *,
        lease_expires_at=None,
        lease_duration_seconds=None,
        **kwargs,
    ):
        self.create_lease_arguments.append((lease_expires_at, lease_duration_seconds))
        return await super().create_thread_operation_atomic(
            run_id,
            lease_expires_at=lease_expires_at,
            lease_duration_seconds=lease_duration_seconds,
            **kwargs,
        )

    async def ensure_run_atomic(
        self,
        run_id,
        *,
        lease_expires_at=None,
        lease_duration_seconds=None,
        **kwargs,
    ):
        self.ensure_lease_arguments.append((lease_expires_at, lease_duration_seconds))
        return await super().ensure_run_atomic(
            run_id,
            lease_expires_at=lease_expires_at,
            lease_duration_seconds=lease_duration_seconds,
            **kwargs,
        )

    async def renew_lease(
        self,
        run_id,
        *,
        owner_worker_id,
        lease_expires_at=None,
        lease_duration_seconds=None,
    ):
        self.renew_lease_arguments.append((lease_expires_at, lease_duration_seconds))
        if self.renewal_delay_seconds:
            await asyncio.sleep(self.renewal_delay_seconds)
        return await super().renew_lease(
            run_id,
            owner_worker_id=owner_worker_id,
            lease_expires_at=lease_expires_at,
            lease_duration_seconds=lease_duration_seconds,
        )


class DurableCapturingDatabaseClockStore(CapturingDatabaseClockStore):
    """Opt into the row+journal lifecycle contract for terminal-sync tests."""

    durable_lifecycle = True


# ---------------------------------------------------------------------------
# create_or_reject — reject strategy
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_reject_blocks_when_active_run_exists():
    """reject strategy must raise ConflictError when thread has an active run."""
    store = MemoryRunStore()
    manager = _make_manager(store=store)
    await manager.create("thread-1")
    await manager.set_status((await manager.list_by_thread("thread-1"))[0].run_id, RunStatus.running)

    with pytest.raises(ConflictError, match="already has an active run"):
        await manager.create_or_reject("thread-1", multitask_strategy="reject")


@pytest.mark.anyio
async def test_reject_succeeds_when_no_active_run():
    """reject strategy must succeed when the thread has no active run."""
    store = MemoryRunStore()
    manager = _make_manager(store=store, run_ownership_config=_lease_config(heartbeat_enabled=True))
    record = await manager.create_or_reject("thread-1", multitask_strategy="reject")
    assert record is not None
    assert record.status == RunStatus.pending
    assert record.owner_worker_id is not None
    assert record.lease_expires_at is not None


@pytest.mark.anyio
async def test_checkpoint_write_reservation_rejects_nonowning_worker_while_run_is_active():
    """A durable run owned by worker A must block worker B's checkpoint writer."""
    store = MemoryRunStore()
    owner = _make_manager(store=store, worker_id="worker-a")
    non_owner = _make_manager(store=store, worker_id="worker-b")
    active = await owner.create_or_reject("thread-1")
    await owner.set_status(active.run_id, RunStatus.running)

    with pytest.raises(ConflictError, match="already has an active run"):
        async with non_owner.reserve_thread_operation("thread-1", kind=ThreadOperationKind.checkpoint_write):
            pytest.fail("the checkpoint mutation guard must not be acquired")

    stored = await store.get(active.run_id)
    assert stored is not None
    assert stored["status"] == "running"


@pytest.mark.anyio
async def test_checkpoint_write_reservation_blocks_new_runs_until_mutation_finishes():
    """The durable guard closes the check-then-write window in both directions."""
    store = MemoryRunStore()
    compaction_worker = _make_manager(store=store, worker_id="worker-a")
    run_worker = _make_manager(store=store, worker_id="worker-b")

    async with compaction_worker.reserve_thread_operation("thread-1", kind=ThreadOperationKind.checkpoint_write):
        inflight = await store.list_inflight()
        assert len(inflight) == 1
        assert inflight[0]["operation_kind"] == ThreadOperationKind.checkpoint_write
        assert inflight[0]["metadata"] == {}

        with pytest.raises(ConflictError, match="checkpoint write"):
            await run_worker.create_or_reject("thread-1", multitask_strategy="interrupt")

    assert await store.list_inflight() == []
    assert await store.list_by_thread("thread-1") == []

    admitted = await run_worker.create_or_reject("thread-1")
    assert admitted.status == RunStatus.pending


@pytest.mark.anyio
async def test_interrupt_reclaims_expired_checkpoint_write_reservation():
    """A dead checkpoint writer must not wait for periodic reconciliation."""
    store = MemoryRunStore()
    expired = (datetime.now(UTC) - timedelta(seconds=30)).isoformat()
    await store.put(
        "checkpoint-write-1",
        thread_id="thread-1",
        status="pending",
        operation_kind=ThreadOperationKind.checkpoint_write,
        owner_worker_id="dead-worker",
        lease_expires_at=expired,
        created_at=expired,
    )
    manager = _make_manager(
        store=store,
        worker_id="worker-b",
        run_ownership_config=_lease_config(grace_seconds=10),
    )

    admitted = await manager.create_or_reject("thread-1", multitask_strategy="interrupt")

    assert admitted.status == RunStatus.pending
    stale = await store.get("checkpoint-write-1")
    assert stale is not None
    assert stale["status"] == "interrupted"
    assert stale["owner_worker_id"] is None
    assert stale["lease_expires_at"] is None
    assert stale["terminal_projection_owner_worker_id"] is None
    assert stale["terminal_projection_active_state_version"] is None


@pytest.mark.anyio
async def test_reject_blocks_reentrant_same_thread_locally():
    """reject must also block when a local in-memory active run exists."""
    store = MemoryRunStore()
    manager = _make_manager(store=store)
    await manager.create_or_reject("thread-1", multitask_strategy="reject")

    with pytest.raises(ConflictError, match="already has an active run"):
        await manager.create_or_reject("thread-1", multitask_strategy="reject")


# ---------------------------------------------------------------------------
# create_or_reject — interrupt strategy
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_interrupt_cancels_old_run_and_creates_new():
    """interrupt must cancel the previous active run and create a new one."""
    store = MemoryRunStore()
    manager = _make_manager(store=store)
    old = await manager.create_or_reject("thread-1", multitask_strategy="reject")
    await manager.set_status(old.run_id, RunStatus.running)

    new = await manager.create_or_reject("thread-1", multitask_strategy="interrupt")

    assert new.run_id != old.run_id
    assert new.status == RunStatus.pending

    # Old run must be interrupted locally
    assert old.status == RunStatus.interrupted
    assert old.abort_event.is_set()

    # Old run must be marked interrupted in-store (persist_status after local cancel)
    old_after = await store.get(old.run_id)
    assert old_after["status"] == "interrupted"


@pytest.mark.anyio
async def test_interrupt_creates_new_when_old_completed():
    """interrupt must succeed when the previous run already reached a terminal status."""
    store = MemoryRunStore()
    manager = _make_manager(store=store)
    old = await manager.create_or_reject("thread-1")
    await manager.set_status(old.run_id, RunStatus.success)

    new = await manager.create_or_reject("thread-1", multitask_strategy="interrupt")
    assert new.run_id != old.run_id
    assert new.status == RunStatus.pending


@pytest.mark.anyio
async def test_interrupt_exhausted_retries_surface_as_conflict_error():
    """When all retry attempts collide with a unique violation, the loop must
    surface ConflictError (HTTP 409) — matching the reject branch — instead of
    leaking the raw IntegrityError (HTTP 500).

    Without the post-loop conversion, the last attempt's ``raise`` re-raises
    the IntegrityError, giving callers an inconsistent signal depending on
    which strategy they picked. The reject path already converts; this test
    pins the symmetric behaviour for interrupt/rollback.
    """
    import sqlite3

    class _AlwaysUniqueViolationStore(MemoryRunStore):
        """MemoryRunStore whose ``create_thread_operation_atomic`` always raises a
        real-flavoured unique-violation IntegrityError, simulating a worker
        that keeps losing the cross-worker race for the same thread."""

        def __init__(self):
            super().__init__()
            self.atomic_call_count = 0

        async def create_thread_operation_atomic(self, *args, **kwargs):
            self.atomic_call_count += 1
            err = sqlite3.IntegrityError("UNIQUE constraint failed: runs.uq_runs_thread_active")
            err.sqlite_errorcode = sqlite3.SQLITE_CONSTRAINT_UNIQUE
            raise err

    store = _AlwaysUniqueViolationStore()
    manager = _make_manager(store=store)

    with pytest.raises(ConflictError, match="already has an active run"):
        await manager.create_or_reject("thread-1", multitask_strategy="interrupt")

    # Sanity: the loop actually retried 3 times before giving up.
    assert store.atomic_call_count == 3


# ---------------------------------------------------------------------------
# create_or_reject — run ownership metadata
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_run_record_stores_owner_and_lease():
    """Newly created runs must carry owner_worker_id and lease_expires_at (when heartbeat is on)."""
    store = MemoryRunStore()
    manager = _make_manager(store=store, run_ownership_config=_lease_config(heartbeat_enabled=True))
    record = await manager.create_or_reject("thread-1")

    assert record.owner_worker_id == manager.worker_id
    assert isinstance(record.owner_worker_id, str) and len(record.owner_worker_id) > 0
    assert record.lease_expires_at is not None

    # Store row must also carry the fields
    stored = await store.get(record.run_id)
    assert stored is not None
    assert stored["owner_worker_id"] == manager.worker_id
    assert stored["lease_expires_at"] is not None
    assert stored["operation_kind"] == ThreadOperationKind.run


@pytest.mark.anyio
async def test_store_row_roundtrips_ownership_fields():
    """Records hydrated from the store must surface ownership fields."""
    store = MemoryRunStore()
    manager = _make_manager(store=store, run_ownership_config=_lease_config(heartbeat_enabled=True))
    record = await manager.create_or_reject("thread-1")

    hydrated = await manager.get(record.run_id)
    assert hydrated is not None
    assert hydrated.owner_worker_id == manager.worker_id
    assert hydrated.lease_expires_at is not None
    assert hydrated.operation_kind == ThreadOperationKind.run


@pytest.mark.anyio
async def test_reconciliation_releases_expired_internal_operation_without_reporting_run():
    """Expired internal reservations release admission without becoming failed runs."""
    store = MemoryRunStore()
    expired = (datetime.now(UTC) - timedelta(seconds=30)).isoformat()
    await store.put(
        "checkpoint-write-1",
        thread_id="thread-1",
        status="pending",
        operation_kind=ThreadOperationKind.checkpoint_write,
        owner_worker_id="dead-worker",
        lease_expires_at=expired,
        created_at=expired,
    )
    manager = _make_manager(
        store=store,
        run_ownership_config=_lease_config(heartbeat_enabled=True, grace_seconds=10),
    )

    recovered = await manager.reconcile_orphaned_inflight_runs(error="owner expired")

    assert recovered == []
    stored = await store.get("checkpoint-write-1")
    assert stored is not None
    assert stored["status"] == "error"


# ---------------------------------------------------------------------------
# reconcile_orphaned_inflight_runs — lease-based
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_reconciliation_claims_expired_lease_runs():
    """A run with an expired lease must be reclaimed as orphaned."""
    store = MemoryRunStore()
    manager = _make_manager(store=store)

    # Insert a run with an already-expired lease
    expired_lease = (datetime.now(UTC) - timedelta(seconds=60)).isoformat()
    await store.put(
        "expired-run",
        thread_id="thread-1",
        status="running",
        owner_worker_id="worker-dead",
        lease_expires_at=expired_lease,
        created_at=(datetime.now(UTC) - timedelta(seconds=120)).isoformat(),
    )

    recovered = await manager.reconcile_orphaned_inflight_runs(
        error="Gateway restarted before this run reached a durable final state.",
    )

    assert len(recovered) == 1
    assert recovered[0].run_id == "expired-run"
    assert recovered[0].status == RunStatus.error

    stored = await store.get("expired-run")
    assert stored["status"] == "error"


@pytest.mark.anyio
async def test_reconciliation_skips_active_lease_runs():
    """A run with a still-valid lease must NOT be reclaimed."""
    store = MemoryRunStore()
    manager = _make_manager(store=store)

    # Insert a run with a still-valid lease
    valid_lease = (datetime.now(UTC) + timedelta(seconds=60)).isoformat()
    await store.put(
        "live-run",
        thread_id="thread-1",
        status="running",
        owner_worker_id="worker-alive",
        lease_expires_at=valid_lease,
        created_at=(datetime.now(UTC) - timedelta(seconds=10)).isoformat(),
    )

    recovered = await manager.reconcile_orphaned_inflight_runs(
        error="Gateway restarted before this run reached a durable final state.",
    )

    # Live run's lease is still valid — must not be reclaimed
    assert all(r.run_id != "live-run" for r in recovered)

    stored = await store.get("live-run")
    assert stored["status"] == "running"


@pytest.mark.anyio
async def test_reconciliation_rechecks_live_lease_after_stale_scan():
    """A stale scan result cannot bypass the claim-time lease check."""
    store = MemoryRunStore()
    grace = 10
    valid_lease = (datetime.now(UTC) + timedelta(seconds=60)).isoformat()
    await store.put(
        "race-run",
        thread_id="thread-1",
        status="running",
        owner_worker_id="worker-alive",
        lease_expires_at=valid_lease,
        created_at=(datetime.now(UTC) - timedelta(seconds=120)).isoformat(),
    )

    async def stale_scan(*, before=None, grace_seconds=10):
        del before, grace_seconds
        stale = await store.get("race-run")
        assert stale is not None
        stale["lease_expires_at"] = (datetime.now(UTC) - timedelta(seconds=grace + 5)).isoformat()
        return [stale]

    store.list_inflight_with_expired_lease = stale_scan
    manager = _make_manager(
        store=store,
        run_ownership_config=_lease_config(heartbeat_enabled=True, grace_seconds=grace),
    )

    recovered = await manager.reconcile_orphaned_inflight_runs(
        error="Gateway restarted before this run reached a durable final state.",
    )

    assert recovered == []
    stored = await store.get("race-run")
    assert stored["status"] == "running"
    assert datetime.fromisoformat(stored["lease_expires_at"]) > datetime.now(UTC)


@pytest.mark.anyio
async def test_memory_expired_owner_cannot_resurrect_lease():
    """Once the deadline passes, only takeover may establish new authority."""

    store = MemoryRunStore()
    expired_lease = (datetime.now(UTC) - timedelta(minutes=1)).isoformat()
    await store.put(
        "expired-owner-run",
        thread_id="thread-expired-owner",
        status="running",
        owner_worker_id="worker-expired",
        lease_expires_at=expired_lease,
    )
    requested_lease = (datetime.now(UTC) + timedelta(minutes=5)).isoformat()

    updated = await store.update_lease(
        "expired-owner-run",
        owner_worker_id="worker-expired",
        lease_expires_at=requested_lease,
    )
    renewed = await store.renew_lease(
        "expired-owner-run",
        owner_worker_id="worker-expired",
        lease_expires_at=requested_lease,
    )

    assert updated is False
    assert renewed.renewed is False
    retained = await store.get("expired-owner-run")
    assert retained is not None
    assert retained["lease_expires_at"] == expired_lease


@pytest.mark.anyio
async def test_concurrent_reconcilers_report_one_successful_claim():
    """Two reapers scanning the same candidate must report one recovery."""
    store = MemoryRunStore()
    grace = 10
    run_id = "contended-orphan"
    await store.put(
        run_id,
        thread_id="thread-1",
        status="running",
        owner_worker_id="worker-dead",
        lease_expires_at=(datetime.now(UTC) - timedelta(seconds=grace + 5)).isoformat(),
        created_at=(datetime.now(UTC) - timedelta(seconds=120)).isoformat(),
    )

    original_scan = store.list_inflight_with_expired_lease
    both_scanned = asyncio.Event()
    scan_lock = asyncio.Lock()
    scan_count = 0

    async def synchronized_scan(*, before=None, grace_seconds=10):
        nonlocal scan_count
        rows = [dict(row) for row in await original_scan(before=before, grace_seconds=grace_seconds)]
        async with scan_lock:
            scan_count += 1
            if scan_count == 2:
                both_scanned.set()
        await asyncio.wait_for(both_scanned.wait(), timeout=1)
        return rows

    store.list_inflight_with_expired_lease = synchronized_scan
    managers = [
        _make_manager(store=store, run_ownership_config=_lease_config(heartbeat_enabled=True, grace_seconds=grace)),
        _make_manager(store=store, run_ownership_config=_lease_config(heartbeat_enabled=True, grace_seconds=grace)),
    ]

    results = await asyncio.gather(*(manager.reconcile_orphaned_inflight_runs(error="orphaned") for manager in managers))

    assert sorted(len(recovered) for recovered in results) == [0, 1]
    assert [record.run_id for recovered in results for record in recovered] == [run_id]
    row = await store.get(run_id)
    assert row is not None
    assert row["status"] == "error"


@pytest.mark.anyio
async def test_reconciliation_claims_null_lease_runs():
    """Pre-ownership rows (NULL lease) must be reclaimed."""
    store = MemoryRunStore()
    manager = _make_manager(store=store)

    await store.put(
        "legacy-run",
        thread_id="thread-1",
        status="running",
        created_at=(datetime.now(UTC) - timedelta(seconds=120)).isoformat(),
    )

    recovered = await manager.reconcile_orphaned_inflight_runs(
        error="Gateway restarted before this run reached a durable final state.",
    )

    assert len(recovered) == 1
    assert recovered[0].run_id == "legacy-run"


@pytest.mark.anyio
async def test_heartbeat_disabled_crashed_run_reclaimed_immediately():
    """Single-worker regression: when heartbeat is off, a crashed run must be
    reclaimed on the next restart without waiting for lease expiry.

    The run is created with lease_expires_at=NULL (no heartbeat => no lease),
    so reconciliation treats it as an orphan and reclaims it right away —
    preserving the pre-ownership recovery latency.
    """
    store = MemoryRunStore()
    # Worker A: heartbeat disabled (single-worker default)
    manager_a = _make_manager(store=store, run_ownership_config=_lease_config(heartbeat_enabled=False))
    record = await manager_a.create("thread-1")
    await manager_a.set_status(record.run_id, RunStatus.running)

    # Verify the run was stored WITHOUT a lease (heartbeat off)
    stored = await store.get(record.run_id)
    assert stored is not None
    assert stored["lease_expires_at"] is None

    # Simulate crash: drop manager_a's local state, build a fresh manager
    # (same store) as if Worker A restarted.
    manager_b = _make_manager(store=store, run_ownership_config=_lease_config(heartbeat_enabled=False))

    # Reconciliation must reclaim the run IMMEDIATELY — no lease to wait out.
    recovered = await manager_b.reconcile_orphaned_inflight_runs(
        error="Gateway restarted before this run reached a durable final state.",
    )

    assert len(recovered) == 1
    assert recovered[0].run_id == record.run_id
    assert recovered[0].status == RunStatus.error


@pytest.mark.anyio
async def test_reconciliation_skips_locally_active_runs():
    """An active local run (owned by this worker) must NOT be reclaimed even with an expired lease."""
    store = MemoryRunStore()
    manager = _make_manager(store=store)

    # Create a live local run
    record = await manager.create("thread-1")
    await manager.set_status(record.run_id, RunStatus.running)

    # Its lease hasn't expired yet, so this is mostly testing the local-ownership guard
    recovered = await manager.reconcile_orphaned_inflight_runs(
        error="Gateway restarted before this run reached a durable final state.",
    )

    assert all(r.run_id != record.run_id for r in recovered)


@pytest.mark.anyio
async def test_reconciliation_returns_empty_when_no_orphaned_runs():
    """Reconciliation must return empty when there are no orphaned runs."""
    store = MemoryRunStore()
    manager = _make_manager(store=store)

    recovered = await manager.reconcile_orphaned_inflight_runs(
        error="Gateway restarted before this run reached a durable final state.",
    )

    assert recovered == []


@pytest.mark.anyio
async def test_periodic_reconciliation_notifies_recovery_callback():
    """Periodic recovery must hand terminalized rows to Gateway orchestration."""
    store = MemoryRunStore()
    on_orphans_recovered = AsyncMock()
    manager = _make_manager(
        store=store,
        on_orphans_recovered=on_orphans_recovered,
    )
    expired_lease = (datetime.now(UTC) - timedelta(seconds=60)).isoformat()
    await store.put(
        "periodic-orphan",
        thread_id="thread-1",
        status="running",
        owner_worker_id="dead-worker",
        lease_expires_at=expired_lease,
        created_at=(datetime.now(UTC) - timedelta(seconds=120)).isoformat(),
    )

    await manager._reconcile_orphans_periodic()
    await asyncio.sleep(0)

    on_orphans_recovered.assert_awaited_once()
    recovered = on_orphans_recovered.await_args.args[0]
    assert [record.run_id for record in recovered] == ["periodic-orphan"]
    assert recovered[0].status == RunStatus.error
    assert recovered[0].stop_reason == ORPHAN_RECOVERY_STOP_REASON
    stored = await store.get("periodic-orphan")
    assert stored is not None
    assert stored["stop_reason"] == ORPHAN_RECOVERY_STOP_REASON


@pytest.mark.anyio
async def test_periodic_reconciliation_logs_recovered_run_ids_when_callback_fails(caplog):
    """Callback failures must identify every recovered run in the warning."""
    store = MemoryRunStore()
    on_orphans_recovered = AsyncMock(side_effect=RuntimeError("callback failed"))
    manager = _make_manager(
        store=store,
        on_orphans_recovered=on_orphans_recovered,
    )
    expired_lease = (datetime.now(UTC) - timedelta(seconds=60)).isoformat()
    created_at = (datetime.now(UTC) - timedelta(seconds=120)).isoformat()
    for run_id in ("periodic-orphan-1", "periodic-orphan-2"):
        await store.put(
            run_id,
            thread_id=f"thread-{run_id}",
            status="running",
            owner_worker_id="dead-worker",
            lease_expires_at=expired_lease,
            created_at=created_at,
        )

    with caplog.at_level("WARNING", logger="deerflow.runtime.runs.manager"):
        await manager._reconcile_orphans_periodic()
        await asyncio.sleep(0)

    assert "Periodic orphan recovery callback failed for 2 run(s)" in caplog.text
    assert "periodic-orphan-1" in caplog.text
    assert "periodic-orphan-2" in caplog.text


@pytest.mark.anyio
async def test_periodic_terminalization_does_not_block_lease_renewal_or_shutdown():
    """The real heartbeat loop must keep renewing during a slow callback."""
    store = MemoryRunStore()
    callback_started = asyncio.Event()
    callback_release = asyncio.Event()
    callback_finished = asyncio.Event()

    async def on_orphans_recovered(_recovered):
        callback_started.set()
        await callback_release.wait()
        callback_finished.set()

    manager = _make_manager(
        store=store,
        run_ownership_config=_lease_config(heartbeat_enabled=True, lease_seconds=5),
        on_orphans_recovered=on_orphans_recovered,
    )
    active = await manager.create_or_reject(
        "active-thread",
        candidate_run_id="11111111-1111-4111-8111-111111111111",
    )
    active_task = await manager.attach_worker_once(
        active.run_id,
        asyncio.sleep(3600),
        asyncio.create_task,
    )
    await manager.set_status(active.run_id, RunStatus.running)
    original_expiry = active.lease_expires_at
    expired_lease = (datetime.now(UTC) - timedelta(seconds=60)).isoformat()
    await store.put(
        "periodic-orphan",
        thread_id="orphan-thread",
        status="running",
        owner_worker_id="dead-worker",
        lease_expires_at=expired_lease,
        created_at=expired_lease,
    )

    await manager.start_heartbeat()
    await asyncio.wait_for(callback_started.wait(), timeout=4.5)
    expiry_during_callback = active.lease_expires_at
    await asyncio.sleep(1.2)

    assert expiry_during_callback != original_expiry
    assert active.lease_expires_at != expiry_during_callback
    assert callback_finished.is_set() is False

    shutdown_task = asyncio.create_task(manager.shutdown(timeout=1.0))
    await asyncio.sleep(0)
    assert shutdown_task.done() is False
    callback_release.set()
    await asyncio.wait_for(shutdown_task, timeout=1.0)
    assert callback_finished.is_set() is True
    active_task.cancel()
    await asyncio.gather(active_task, return_exceptions=True)


@pytest.mark.anyio
async def test_periodic_store_scan_does_not_block_real_heartbeat_loop():
    """A slow orphan scan must not stall later lease-renewal cycles."""

    class SlowScanStore(MemoryRunStore):
        def __init__(self):
            super().__init__()
            self.scan_started = asyncio.Event()
            self.scan_release = asyncio.Event()

        async def list_inflight_with_expired_lease(
            self,
            *,
            before=None,
            grace_seconds=10,
        ):
            self.scan_started.set()
            await self.scan_release.wait()
            return await super().list_inflight_with_expired_lease(
                before=before,
                grace_seconds=grace_seconds,
            )

    store = SlowScanStore()
    manager = _make_manager(
        store=store,
        run_ownership_config=_lease_config(heartbeat_enabled=True, lease_seconds=5),
    )
    active = await manager.create_or_reject(
        "active-thread",
        candidate_run_id="22222222-2222-4222-8222-222222222222",
    )
    active_task = await manager.attach_worker_once(
        active.run_id,
        asyncio.sleep(3600),
        asyncio.create_task,
    )
    await manager.set_status(active.run_id, RunStatus.running)

    await manager.start_heartbeat()
    await asyncio.wait_for(store.scan_started.wait(), timeout=4.5)
    expiry_during_scan = active.lease_expires_at
    await asyncio.sleep(1.2)

    assert active.lease_expires_at != expiry_during_scan

    store.scan_release.set()
    await manager.stop_heartbeat()
    await manager._drain_orphan_recovery_task(timeout=0.5)
    active_task.cancel()
    await asyncio.gather(active_task, return_exceptions=True)


@pytest.mark.anyio
async def test_periodic_recovery_is_single_flight():
    """A second trigger must not create another recovery pipeline."""
    store = MemoryRunStore()
    callback_started = asyncio.Event()
    callback_release = asyncio.Event()
    callback_calls = 0

    async def on_orphans_recovered(_recovered):
        nonlocal callback_calls
        callback_calls += 1
        callback_started.set()
        await callback_release.wait()

    manager = _make_manager(
        store=store,
        on_orphans_recovered=on_orphans_recovered,
    )
    expired_lease = (datetime.now(UTC) - timedelta(seconds=60)).isoformat()
    await store.put(
        "periodic-orphan",
        thread_id="orphan-thread",
        status="running",
        owner_worker_id="dead-worker",
        lease_expires_at=expired_lease,
        created_at=expired_lease,
    )

    manager._schedule_orphan_reconciliation()
    await asyncio.wait_for(callback_started.wait(), timeout=0.5)
    first_task = manager._orphan_recovery_task
    manager._schedule_orphan_reconciliation()

    assert manager._orphan_recovery_task is first_task
    assert callback_calls == 1

    callback_release.set()
    await asyncio.wait_for(first_task, timeout=0.5)


@pytest.mark.anyio
async def test_shutdown_cancels_recovery_that_exceeds_drain_budget():
    """The pending -> cancel -> gather branch must observe callback cancellation."""
    store = MemoryRunStore()
    callback_started = asyncio.Event()
    callback_cancelled = asyncio.Event()

    async def on_orphans_recovered(_recovered):
        callback_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            callback_cancelled.set()
            raise

    manager = _make_manager(
        store=store,
        on_orphans_recovered=on_orphans_recovered,
    )
    expired_lease = (datetime.now(UTC) - timedelta(seconds=60)).isoformat()
    await store.put(
        "periodic-orphan",
        thread_id="orphan-thread",
        status="running",
        owner_worker_id="dead-worker",
        lease_expires_at=expired_lease,
        created_at=expired_lease,
    )
    manager._schedule_orphan_reconciliation()
    await asyncio.wait_for(callback_started.wait(), timeout=0.5)

    await manager.shutdown(timeout=0.01)

    assert callback_cancelled.is_set()
    assert manager._orphan_recovery_task is None


@pytest.mark.anyio
async def test_shutdown_applies_shared_deadline_to_heartbeat_stop():
    """A stuck heartbeat must not receive a separate five-second budget."""
    manager = _make_manager(
        run_ownership_config=_lease_config(heartbeat_enabled=True),
    )
    heartbeat_cancelled = asyncio.Event()

    async def stuck_heartbeat():
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            heartbeat_cancelled.set()
            raise

    manager._heartbeat_stop = asyncio.Event()
    manager._heartbeat_task = asyncio.create_task(stuck_heartbeat())
    started = asyncio.get_running_loop().time()

    await manager.shutdown(timeout=0.01)

    elapsed = asyncio.get_running_loop().time() - started
    assert heartbeat_cancelled.is_set()
    assert elapsed < 0.5


# ---------------------------------------------------------------------------
# Lease heartbeat
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_heartbeat_renews_active_run_leases():
    """Heartbeat must extend the lease on active runs owned by this worker."""
    config = _lease_config(lease_seconds=30, heartbeat_enabled=True)
    store = MemoryRunStore()
    manager = _make_manager(store=store, run_ownership_config=config)

    record = await manager.create_or_reject(
        "thread-1",
        candidate_run_id="33333333-3333-4333-8333-333333333333",
    )
    task = await manager.attach_worker_once(
        record.run_id,
        asyncio.sleep(3600),
        asyncio.create_task,
    )
    await manager.set_status(record.run_id, RunStatus.running)

    original_lease = record.lease_expires_at
    assert original_lease is not None

    # Start heartbeat and let it tick once
    await manager.start_heartbeat()
    await asyncio.sleep(0.2)  # heartbeat interval = 10s, too long; manually renew

    await manager._renew_leases()
    await manager.stop_heartbeat()

    assert record.lease_expires_at is not None
    # Lease should have been extended
    assert record.lease_expires_at >= original_lease
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


@pytest.mark.anyio
async def test_database_clock_manager_uses_duration_and_monotonic_budget(
    monkeypatch: pytest.MonkeyPatch,
):
    """Pod wall-clock skew cannot mint or locally validate a DB lease."""

    from deerflow.runtime.runs import manager as run_manager_module

    actual_now = datetime.now(UTC)

    class FastProcessDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            value = actual_now + timedelta(hours=24)
            return value if tz is not None else value.replace(tzinfo=None)

    monkeypatch.setattr(run_manager_module, "datetime", FastProcessDateTime)
    config = _lease_config(lease_seconds=5, heartbeat_enabled=True)
    store = CapturingDatabaseClockStore()
    manager = _make_manager(
        store=store,
        worker_id="worker-database-clock",
        run_ownership_config=config,
    )

    record = await manager.create_or_reject(
        "thread-duration-unkeyed",
        candidate_run_id="77777777-7777-4777-8777-777777777777",
    )
    keyed = await manager.ensure_or_reject(
        "thread-duration-keyed",
        candidate_run_id="88888888-8888-4888-8888-888888888888",
        external_scope="test-duration",
        external_key="keyed",
        request_digest="a" * 64,
        request_digest_version="request-v1",
        caller_intent_json={"input": "test"},
        caller_intent_digest="b" * 64,
        caller_intent_digest_version="intent-v1",
    )

    # The keyed Memory compatibility implementation delegates its create to
    # the same atomic primitive, so both admissions are visible here.
    assert store.create_lease_arguments == [(None, 5), (None, 5)]
    assert store.ensure_lease_arguments == [(None, 5)]
    assert record.lease_expires_at is not None
    assert keyed.record.lease_expires_at is not None
    assert record.lease_safety_deadline_monotonic is not None

    await manager.set_status(record.run_id, RunStatus.running)
    store.renewal_delay_seconds = 0.1
    loop = asyncio.get_running_loop()
    renewal_started = loop.time()

    await manager._renew_leases()

    renewal_finished = loop.time()
    assert store.renew_lease_arguments == [(None, 5), (None, 5)]
    assert record.ownership_lost is False
    assert record.lease_safety_deadline_monotonic is not None
    assert record.lease_safety_deadline_monotonic <= renewal_started + 5.05
    assert record.lease_safety_deadline_monotonic < renewal_finished + 4.98
    persisted = await store.get(record.run_id)
    assert persisted is not None
    assert record.lease_expires_at == persisted["lease_expires_at"]
    assert record.created_at == persisted["created_at"]
    keyed_persisted = await store.get(keyed.record.run_id)
    assert keyed_persisted is not None
    assert keyed.record.created_at == keyed_persisted["created_at"]


@pytest.mark.anyio
async def test_heartbeat_renews_pending_run_before_task_is_spawned():
    """A run sitting in ``pending`` between ``create_thread_operation_atomic`` and task
    spawn must still have its lease renewed.

    Pre-fix the renewal filter required ``record.task is not None``, so a
    pending run with no task yet (the brief window after
    ``create_thread_operation_atomic`` inserts the row before the worker layer spawns
    the agent task) was silently skipped. If that window stretched past
    ``lease_seconds`` — e.g. event-loop saturation, slow checkpoint
    hydrate — peer reconciliation reclaimed the run as an orphan and
    marked it ``error`` even though this worker still intended to run it.
    """
    config = _lease_config(lease_seconds=30, heartbeat_enabled=True)
    store = MemoryRunStore()
    manager = _make_manager(store=store, run_ownership_config=config)

    record = await manager.create_or_reject(
        "thread-1",
        candidate_run_id="66666666-6666-4666-8666-666666666666",
    )
    assert record.status == RunStatus.pending
    # No task has been spawned — this is the regression sentinel.
    assert record.task is None

    original_lease = record.lease_expires_at
    assert original_lease is not None

    # Force a measurable gap so the renewed lease strictly post-dates the
    # original — without this the two timestamps land in the same
    # microsecond on fast hosts and the strict comparison fails trivially.
    await asyncio.sleep(0.001)

    store.renew_lease = AsyncMock(wraps=store.renew_lease)

    await manager._renew_leases()

    store.renew_lease.assert_awaited_once()
    assert record.lease_expires_at is not None
    assert record.lease_expires_at > original_lease


@pytest.mark.anyio
async def test_transient_renewal_exception_before_deadline_keeps_run_alive():
    """A renewal error is retryable while the last confirmed lease is valid."""
    config = _lease_config(lease_seconds=30, heartbeat_enabled=True)
    store = MemoryRunStore()
    manager = _make_manager(store=store, worker_id="worker-a", run_ownership_config=config)
    record = await manager.create("thread-1")
    await manager.set_status(record.run_id, RunStatus.running)
    record.task = asyncio.create_task(asyncio.sleep(3600))
    original_renew_lease = store.renew_lease
    attempts = 0

    async def fail_once_then_renew(*args, **kwargs):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise OSError("temporary database outage")
        return await original_renew_lease(*args, **kwargs)

    store.renew_lease = fail_once_then_renew
    original_expiry = record.lease_expires_at

    try:
        await manager._renew_leases()

        assert record.abort_event.is_set() is False
        assert record.task.done() is False
        assert record.lease_expires_at == original_expiry

        await manager._renew_leases()

        assert attempts == 2
        assert record.abort_event.is_set() is False
        assert record.task.done() is False
        assert record.lease_expires_at is not None
        assert original_expiry is not None
        assert record.lease_expires_at > original_expiry
    finally:
        record.task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await record.task


@pytest.mark.anyio
async def test_renewal_exception_through_confirmed_expiry_fail_stops_run():
    """The owner must stop once errors outlive its last confirmed lease."""
    config = _lease_config(lease_seconds=30, heartbeat_enabled=True)
    store = MemoryRunStore()
    manager = _make_manager(store=store, worker_id="worker-a", run_ownership_config=config)
    record = await manager.create("thread-1")
    await manager.set_status(record.run_id, RunStatus.running)
    record.task = asyncio.create_task(asyncio.sleep(3600))
    attempts = 0

    async def fail_renewal(*_args, **_kwargs):
        nonlocal attempts
        attempts += 1
        raise OSError("database unreachable")

    store.renew_lease = fail_renewal

    await manager._renew_leases()
    assert record.abort_event.is_set() is False
    assert record.task.done() is False

    expired = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()
    record.lease_expires_at = expired
    store._runs[record.run_id]["lease_expires_at"] = expired
    store._runs[record.run_id]["recovery_policy"] = RecoveryPolicy.exact_two_takeover_v1.value

    await manager._renew_leases()
    await asyncio.sleep(0)

    assert attempts == 1
    assert record.ownership_lost is True
    assert record.abort_event.is_set() is True
    assert record.task.cancelled()


@pytest.mark.anyio
async def test_hung_renewal_is_bounded_by_confirmed_lease_deadline():
    """A blocked store call cannot keep execution alive beyond the lease."""
    config = _lease_config(lease_seconds=30, heartbeat_enabled=True)
    store = MemoryRunStore()
    manager = _make_manager(store=store, worker_id="worker-a", run_ownership_config=config)
    record = await manager.create("thread-1")
    await manager.set_status(record.run_id, RunStatus.running)
    record.task = asyncio.create_task(asyncio.sleep(3600))
    near_expiry = (datetime.now(UTC) + timedelta(milliseconds=50)).isoformat()
    record.lease_expires_at = near_expiry
    store._runs[record.run_id]["lease_expires_at"] = near_expiry

    async def hang_renewal(*_args, **_kwargs):
        await asyncio.Event().wait()

    store.renew_lease = hang_renewal

    await asyncio.wait_for(manager._renew_leases(), timeout=1)
    await asyncio.sleep(0)

    assert record.ownership_lost is True
    assert record.abort_event.is_set() is True
    assert record.task.cancelled()


@pytest.mark.anyio
async def test_late_renewal_attempt_is_rejected_and_fences_local_run():
    """An owner cannot revive authority after its old deadline passes."""
    config = _lease_config(lease_seconds=30, heartbeat_enabled=True)
    store = MemoryRunStore()
    manager = _make_manager(store=store, worker_id="worker-a", run_ownership_config=config)
    record = await manager.create("thread-1")
    await manager.set_status(record.run_id, RunStatus.running)
    record.task = asyncio.create_task(asyncio.sleep(3600))
    near_expiry = (datetime.now(UTC) + timedelta(milliseconds=50)).isoformat()
    record.lease_expires_at = near_expiry
    store._runs[record.run_id]["lease_expires_at"] = near_expiry
    original_renew_lease = store.renew_lease

    async def renew_after_timeout_cancellation(*args, **kwargs):
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            # Simulate a store operation that reaches its authority check only
            # after the caller's deadline cancellation.
            pass
        return await original_renew_lease(*args, **kwargs)

    store.renew_lease = renew_after_timeout_cancellation

    await asyncio.wait_for(manager._renew_leases(), timeout=1)
    await asyncio.sleep(0)

    row = await store.get(record.run_id)
    assert row is not None
    assert row["lease_expires_at"] == near_expiry
    assert record.lease_expires_at == near_expiry
    assert record.ownership_lost is True
    assert record.abort_event.is_set() is True
    assert record.task.cancelled()


@pytest.mark.anyio
async def test_database_clock_renewal_failure_is_fenced_at_monotonic_deadline():
    """A late transient failure cannot defer fencing to the next heartbeat."""

    config = _timing_lease_config()
    store = CapturingDatabaseClockStore()
    manager = _make_manager(
        store=store,
        worker_id="worker-deadline-watchdog",
        run_ownership_config=config,
    )
    record = await manager.create_or_reject(
        "thread-deadline-watchdog",
        candidate_run_id="99999999-9999-4999-8999-999999999991",
    )
    task = await manager.attach_worker_once(
        record.run_id,
        asyncio.sleep(3600),
        asyncio.create_task,
    )
    await manager.set_status(record.run_id, RunStatus.running)

    try:
        # Move to a fresh confirmed deadline. The watchdog must move with it;
        # otherwise the admission timer becomes stale and no later timer fences
        # this capability.
        await manager._renew_leases()
        confirmed_deadline = record.lease_safety_deadline_monotonic
        assert confirmed_deadline is not None

        async def fail_just_before_deadline(*_args, **_kwargs):
            remaining = confirmed_deadline - asyncio.get_running_loop().time()
            await asyncio.sleep(max(0.0, remaining - 0.08))
            raise RuntimeError("transient renewal failure")

        store.renew_lease = fail_just_before_deadline
        await manager._renew_leases()
        failure_returned_at = asyncio.get_running_loop().time()
        await asyncio.sleep(
            max(
                0.0,
                confirmed_deadline - asyncio.get_running_loop().time() + 0.08,
            )
        )
        await asyncio.sleep(0)

        assert record.ownership_lost is True
        assert record.abort_event.is_set() is True
        assert task.cancelled()
        assert asyncio.get_running_loop().time() - failure_returned_at < 0.5
    finally:
        for watchdog in manager._lease_watchdogs.values():
            watchdog[3].cancel()
        manager._lease_watchdogs.clear()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.anyio
async def test_database_clock_attachment_validates_and_invokes_factory_in_one_lock_turn():
    """No queued contender may split deadline validation from task creation."""

    config = _timing_lease_config()
    store = CapturingDatabaseClockStore()
    manager = _make_manager(
        store=store,
        worker_id="worker-attachment-deadline",
        run_ownership_config=config,
    )
    record = await manager.create_or_reject(
        "thread-attachment-deadline",
        candidate_run_id="99999999-9999-4999-8999-999999999995",
    )
    manager._disarm_database_lease_watchdog(record.run_id, record=record)
    loop = asyncio.get_running_loop()
    worker = asyncio.sleep(0)
    factory_called_at = None
    factory_observed_lock = False
    factory_observed_contender = False
    spawned_tasks: list[asyncio.Task[None]] = []
    contender_acquired = asyncio.Event()
    release_contender = asyncio.Event()

    def task_factory(coroutine):
        nonlocal factory_called_at, factory_observed_lock, factory_observed_contender
        factory_called_at = loop.time()
        factory_observed_lock = manager._lock.locked()
        factory_observed_contender = contender_acquired.is_set()
        task = asyncio.create_task(coroutine)
        spawned_tasks.append(task)
        return task

    async def hold_attachment_lock() -> None:
        async with manager._lock:
            contender_acquired.set()
            await release_contender.wait()

    attach_task = None
    contender_task = None
    await manager._lock.acquire()
    try:
        record.lease_safety_deadline_monotonic = loop.time() + 0.1
        attach_task = asyncio.create_task(
            manager.attach_worker_once(
                record.run_id,
                worker,
                task_factory,
            )
        )
        await asyncio.sleep(0)
        contender_task = asyncio.create_task(hold_attachment_lock())
        await asyncio.sleep(0)
    finally:
        manager._lock.release()

    try:
        await asyncio.wait_for(contender_acquired.wait(), timeout=1)
        deadline = record.lease_safety_deadline_monotonic
        assert deadline is not None
        await asyncio.sleep(max(0.0, deadline - loop.time() + 0.02))
        release_contender.set()
        await asyncio.wait_for(contender_task, timeout=1)
        await asyncio.wait_for(attach_task, timeout=1)

        assert factory_called_at is not None
        assert factory_called_at < deadline
        assert factory_observed_lock is True
        assert factory_observed_contender is False
    finally:
        release_contender.set()
        if contender_task is not None and not contender_task.done():
            contender_task.cancel()
        if attach_task is not None and not attach_task.done():
            attach_task.cancel()
        await asyncio.gather(
            *(task for task in (contender_task, attach_task) if task is not None),
            return_exceptions=True,
        )
        for task in spawned_tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*spawned_tasks, return_exceptions=True)
        if worker.cr_frame is not None:
            worker.close()


@pytest.mark.anyio
async def test_database_clock_attachment_queued_past_deadline_never_calls_factory():
    """An attachment first acquiring the lock after expiry cannot start work."""

    config = _timing_lease_config()
    store = CapturingDatabaseClockStore()
    manager = _make_manager(
        store=store,
        worker_id="worker-attachment-expired-waiter",
        run_ownership_config=config,
    )
    record = await manager.create_or_reject(
        "thread-attachment-expired-waiter",
        candidate_run_id="99999999-9999-4999-8999-999999999994",
    )
    manager._disarm_database_lease_watchdog(record.run_id, record=record)
    loop = asyncio.get_running_loop()
    worker = asyncio.sleep(0)
    factory_called = False
    spawned_tasks: list[asyncio.Task[None]] = []

    def task_factory(coroutine):
        nonlocal factory_called
        factory_called = True
        task = asyncio.create_task(coroutine)
        spawned_tasks.append(task)
        return task

    await manager._lock.acquire()
    record.lease_safety_deadline_monotonic = loop.time() + 0.05
    attach_task = asyncio.create_task(
        manager.attach_worker_once(
            record.run_id,
            worker,
            task_factory,
        )
    )
    try:
        await asyncio.sleep(0)
        deadline = record.lease_safety_deadline_monotonic
        assert deadline is not None
        await asyncio.sleep(max(0.0, deadline - loop.time() + 0.02))
    finally:
        manager._lock.release()

    try:
        with pytest.raises(RunStartupError):
            await asyncio.wait_for(attach_task, timeout=1)
        assert factory_called is False
    finally:
        if not attach_task.done():
            attach_task.cancel()
        await asyncio.gather(attach_task, return_exceptions=True)
        for task in spawned_tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*spawned_tasks, return_exceptions=True)
        if worker.cr_frame is not None:
            worker.close()


@pytest.mark.anyio
async def test_stale_equal_deadline_watchdog_cannot_disarm_same_id_replacement():
    """Record identity, not deadline equality, owns a same-ID watchdog."""

    config = _lease_config(lease_seconds=30, heartbeat_enabled=True)
    store = CapturingDatabaseClockStore()
    manager = _make_manager(
        store=store,
        worker_id="worker-watchdog-replacement",
        run_ownership_config=config,
    )
    original = await manager.create_or_reject(
        "thread-watchdog-replacement",
        candidate_run_id="99999999-9999-4999-8999-999999999996",
    )
    original_watchdog = manager._lease_watchdogs[original.run_id]
    deadline = original.lease_safety_deadline_monotonic
    assert deadline is not None
    original_token = original_watchdog[2]

    replacement = type(original)(
        run_id=original.run_id,
        thread_id=original.thread_id,
        assistant_id=original.assistant_id,
        status=original.status,
        on_disconnect=original.on_disconnect,
        owner_worker_id=manager.worker_id,
        lease_expires_at=original.lease_expires_at,
        lease_safety_deadline_monotonic=deadline,
    )
    manager._runs[original.run_id] = replacement
    manager._arm_database_lease_watchdog(replacement)
    replacement_watchdog = manager._lease_watchdogs[replacement.run_id]

    try:
        assert replacement_watchdog[0] is replacement
        assert replacement_watchdog[1] == deadline

        await manager._fence_database_lease_at_deadline(
            original,
            deadline,
            original_token,
        )

        assert manager._lease_watchdogs[replacement.run_id] is replacement_watchdog
        assert replacement.ownership_lost is False
        assert replacement.abort_event.is_set() is False
    finally:
        manager._disarm_database_lease_watchdog(
            replacement.run_id,
            record=replacement,
        )


@pytest.mark.anyio
async def test_terminal_store_sync_disarms_database_clock_watchdog():
    """A confirmed terminal row cannot retain a live local lease timer."""

    config = _lease_config(lease_seconds=30, heartbeat_enabled=True)
    store = DurableCapturingDatabaseClockStore()
    manager = _make_manager(
        store=store,
        worker_id="worker-terminal-watchdog",
        run_ownership_config=config,
    )
    record = await manager.create_or_reject(
        "thread-terminal-watchdog",
        candidate_run_id="99999999-9999-4999-8999-999999999997",
    )
    watchdog = manager._lease_watchdogs[record.run_id]

    await manager.set_status(record.run_id, RunStatus.success)

    persisted = await store.get(record.run_id)
    assert persisted is not None
    assert persisted["status"] == RunStatus.success.value
    assert record.run_id not in manager._lease_watchdogs
    assert watchdog[3].cancelled()


@pytest.mark.anyio
async def test_database_clock_watchdog_fences_unpersisted_staged_terminal():
    """A local terminal stage cannot outlive its still-running durable lease."""

    config = _timing_lease_config()
    store = DurableCapturingDatabaseClockStore()
    manager = _make_manager(
        store=store,
        worker_id="worker-staged-terminal",
        run_ownership_config=config,
    )
    record = await manager.create_or_reject(
        "thread-staged-terminal",
        candidate_run_id="99999999-9999-4999-8999-999999999998",
    )
    task = await manager.attach_worker_once(
        record.run_id,
        asyncio.sleep(3600),
        asyncio.create_task,
    )
    await manager.set_status(record.run_id, RunStatus.running)
    record.lease_safety_deadline_monotonic = asyncio.get_running_loop().time() + 0.05
    manager._arm_database_lease_watchdog(record)
    await manager.set_status(record.run_id, RunStatus.success, persist=False)

    try:
        await asyncio.wait_for(record.abort_event.wait(), timeout=1)
        await asyncio.gather(task, return_exceptions=True)

        persisted = await store.get(record.run_id)
        assert persisted is not None
        assert persisted["status"] == RunStatus.running.value
        assert record.ownership_lost is True
        assert record.abort_event.is_set() is True
        assert task.cancelled()
    finally:
        for watchdog in manager._lease_watchdogs.values():
            watchdog[3].cancel()
        manager._lease_watchdogs.clear()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.anyio
async def test_duration_admission_exhausting_ttl_never_becomes_attachable():
    """A committed row returned after its safety budget cannot start work."""

    config = _timing_lease_config()
    store = CapturingDatabaseClockStore()
    original_create = store.create_thread_operation_atomic

    async def create_after_ttl(*args, **kwargs):
        await asyncio.sleep(config.lease_seconds + 0.05)
        return await original_create(*args, **kwargs)

    store.create_thread_operation_atomic = create_after_ttl
    manager = _make_manager(
        store=store,
        worker_id="worker-expired-admission",
        run_ownership_config=config,
    )
    run_id = "99999999-9999-4999-8999-999999999992"

    with pytest.raises(
        RunStartupError,
        match="Database-clock lease expired before worker attachment",
    ):
        await manager.create_or_reject(
            "thread-expired-admission",
            candidate_run_id=run_id,
        )

    record = manager._runs[run_id]
    assert record.ownership_lost is True
    assert record.abort_event.is_set() is True
    assert record.task is None
    worker = asyncio.sleep(0)
    factory_called = False

    def task_factory(coroutine):
        nonlocal factory_called
        factory_called = True
        coroutine.close()
        raise AssertionError("fenced admission must not invoke task_factory")

    try:
        with pytest.raises(RunStartupError):
            await manager.attach_worker_once(
                run_id,
                worker,
                task_factory,
            )
        assert factory_called is False
    finally:
        worker.close()


@pytest.mark.anyio
async def test_duration_takeover_exhausting_ttl_never_invokes_recovery_callback():
    """A slow winning CAS is not enough authority to dispatch recovery work."""

    config = _timing_lease_config()
    store = CapturingDatabaseClockStore()
    expired = (datetime.now(UTC) - timedelta(minutes=1)).isoformat()
    await store.put(
        "run-expired-duration-takeover",
        thread_id="thread-expired-duration-takeover",
        status="running",
        owner_worker_id="worker-dead",
        lease_expires_at=expired,
        created_at=expired,
        recovery_policy=RecoveryPolicy.exact_two_takeover_v1,
        recovery_payload_json=_execution_recovery_payload("thread-expired-duration-takeover"),
    )
    original_claim = store.claim_for_execution_takeover

    async def claim_after_ttl(*args, **kwargs):
        await asyncio.sleep(config.lease_seconds + 0.05)
        return await original_claim(*args, **kwargs)

    store.claim_for_execution_takeover = claim_after_ttl
    callback_calls = 0

    async def recovery_callback(_record):
        nonlocal callback_calls
        callback_calls += 1
        raise AssertionError("expired takeover must not invoke application recovery")

    manager = _make_manager(
        store=store,
        worker_id="worker-duration-takeover",
        run_ownership_config=config,
        on_execution_takeover=recovery_callback,
    )

    recovered = await manager.reconcile_orphaned_inflight_runs(error="orphaned")

    assert recovered == []
    assert callback_calls == 0


@pytest.mark.anyio
async def test_heartbeat_skips_runs_not_owned_by_this_worker():
    """Heartbeat must only renew leases for runs owned by this worker."""
    config = _lease_config(lease_seconds=30, heartbeat_enabled=True)
    store = MemoryRunStore()
    manager = _make_manager(store=store, run_ownership_config=config)

    # Create a run owned by a different worker
    old_lease = (datetime.now(UTC) + timedelta(seconds=5)).isoformat()
    await store.put(
        "other-worker-run",
        thread_id="thread-1",
        status="running",
        owner_worker_id="other-worker",
        lease_expires_at=old_lease,
        created_at=(datetime.now(UTC) - timedelta(seconds=10)).isoformat(),
    )

    await manager._renew_leases()

    stored = await store.get("other-worker-run")
    # Lease should be unchanged (other worker's run)
    assert stored["lease_expires_at"] == old_lease


@pytest.mark.anyio
async def test_heartbeat_not_started_when_disabled():
    """When heartbeat_enabled is False, start_heartbeat must be a no-op."""
    config = _lease_config(heartbeat_enabled=False)
    store = MemoryRunStore()
    manager = _make_manager(store=store, run_ownership_config=config)

    assert manager.heartbeat_enabled is False
    await manager.start_heartbeat()
    assert manager._heartbeat_task is None
    assert manager._heartbeat_stop is None


# ---------------------------------------------------------------------------
# cancel with cross-worker lease awareness
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_cancel_local_run_succeeds():
    """Cancel must succeed for a locally-owned active run."""
    store = MemoryRunStore()
    manager = _make_manager(store=store)
    record = await manager.create("thread-1")
    await manager.set_status(record.run_id, RunStatus.running)

    result = await manager.cancel(record.run_id)
    assert result == CancelOutcome.cancelled
    assert record.status == RunStatus.interrupted


@pytest.mark.anyio
async def test_cancel_unknown_run_returns_false():
    """Cancel must return not_active_locally for a run not known to this worker (heartbeat off)."""
    store = MemoryRunStore()
    manager = _make_manager(store=store)

    result = await manager.cancel("nonexistent-run")
    assert result == CancelOutcome.not_active_locally


@pytest.mark.anyio
async def test_cancel_idempotent():
    """Cancel must return cancelled when the run is already interrupted."""
    store = MemoryRunStore()
    manager = _make_manager(store=store)
    record = await manager.create("thread-1")
    await manager.set_status(record.run_id, RunStatus.interrupted)

    result = await manager.cancel(record.run_id)
    assert result == CancelOutcome.cancelled


# ---------------------------------------------------------------------------
# GATEWAY_WORKERS=1 backward compatibility
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_single_worker_default_config_behavior_unchanged():
    """With default config (heartbeat_enabled=False), behavior must match pre-ownership code."""
    config = _lease_config(heartbeat_enabled=False)
    store = MemoryRunStore()
    manager = _make_manager(store=store, run_ownership_config=config)

    # Create runs, cancel, create_or_reject — all must work
    r1 = await manager.create("thread-1")
    assert r1.owner_worker_id is not None

    r2 = await manager.create_or_reject("thread-2", multitask_strategy="reject")
    assert r2.owner_worker_id is not None

    await manager.cancel(r2.run_id)
    stored = await store.get(r2.run_id)
    assert stored["status"] == "interrupted"


@pytest.mark.anyio
async def test_manager_without_run_ownership_config():
    """Manager without run_ownership_config must still work (backward compat)."""
    store = MemoryRunStore()
    manager = RunManager(store=store)  # no run_ownership_config

    record = await manager.create_or_reject("thread-1")
    assert record is not None
    assert record.owner_worker_id is not None  # always set, even without config

    # Heartbeat must be a no-op without config
    assert manager.heartbeat_enabled is False
    await manager.start_heartbeat()
    assert manager._heartbeat_task is None


# ---------------------------------------------------------------------------
# worker_id uniqueness
# ---------------------------------------------------------------------------


def test_worker_id_is_generated():
    """worker_id must be a non-empty string containing hostname."""
    wid = _generate_worker_id()
    assert isinstance(wid, str)
    assert len(wid) > 0
    assert ":" in wid


def test_two_managers_have_different_default_ids():
    """Two managers without explicit worker_id must get unique ids."""
    m1 = RunManager()
    m2 = RunManager()
    assert m1.worker_id != m2.worker_id


# ---------------------------------------------------------------------------
# Store atomic methods
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_create_thread_operation_atomic_reject_prevents_duplicate():
    """Atomic thread-operation creation must reject a duplicate."""
    store = MemoryRunStore()
    config = _lease_config()

    store.create_thread_operation_atomic = AsyncMock(wraps=store.create_thread_operation_atomic)

    await store.create_thread_operation_atomic(
        run_id="run-1",
        thread_id="thread-1",
        owner_worker_id="w1",
        lease_expires_at=(datetime.now(UTC) + timedelta(seconds=30)).isoformat(),
        multitask_strategy="reject",
        grace_seconds=config.grace_seconds,
    )

    with pytest.raises(ConflictError, match="already has an active run"):
        await store.create_thread_operation_atomic(
            run_id="run-2",
            thread_id="thread-1",
            owner_worker_id="w2",
            lease_expires_at=(datetime.now(UTC) + timedelta(seconds=30)).isoformat(),
            multitask_strategy="reject",
            grace_seconds=config.grace_seconds,
        )


@pytest.mark.anyio
async def test_create_thread_operation_atomic_interrupt_claims_and_creates():
    """Atomic thread-operation creation with interrupt must claim and replace."""
    store = MemoryRunStore()
    config = _lease_config()
    # Create an active run with an expired lease (simulating a crashed worker)
    expired_lease = (datetime.now(UTC) - timedelta(seconds=60)).isoformat()

    await store.create_thread_operation_atomic(
        run_id="run-old",
        thread_id="thread-1",
        owner_worker_id="w1",
        lease_expires_at=expired_lease,
        multitask_strategy="reject",
        grace_seconds=config.grace_seconds,
    )

    new_row, claimed = await store.create_thread_operation_atomic(
        run_id="run-new",
        thread_id="thread-1",
        owner_worker_id="w2",
        lease_expires_at=(datetime.now(UTC) + timedelta(seconds=30)).isoformat(),
        multitask_strategy="interrupt",
        grace_seconds=config.grace_seconds,
    )

    assert new_row["run_id"] == "run-new"
    assert new_row["status"] == "pending"
    assert len(claimed) == 1
    assert claimed[0]["run_id"] == "run-old"

    # Old run must be interrupted in-store
    old_row = await store.get("run-old")
    assert old_row["status"] == "interrupted"


@pytest.mark.anyio
async def test_create_thread_operation_atomic_interrupt_rejects_other_worker_valid_lease():
    """Interrupt must raise ConflictError when a valid-lease run is owned by another worker.

    The partial unique index ``uq_runs_thread_active`` would reject the INSERT
    anyway; surfacing ConflictError here gives the caller a clean signal
    instead of a futile retry loop on IntegrityError.
    """
    store = MemoryRunStore()
    config = _lease_config(grace_seconds=10)
    valid_lease = (datetime.now(UTC) + timedelta(seconds=30)).isoformat()

    await store.create_thread_operation_atomic(
        run_id="valid-lease-run",
        thread_id="thread-1",
        owner_worker_id="other-worker",
        lease_expires_at=valid_lease,
        multitask_strategy="reject",
        grace_seconds=config.grace_seconds,
    )

    with pytest.raises(ConflictError, match="another worker"):
        await store.create_thread_operation_atomic(
            run_id="run-new",
            thread_id="thread-1",
            owner_worker_id="w2",
            lease_expires_at=(datetime.now(UTC) + timedelta(seconds=30)).isoformat(),
            multitask_strategy="interrupt",
            grace_seconds=config.grace_seconds,
        )

    # The valid-lease run must be untouched (transaction rolled back).
    old_row = await store.get("valid-lease-run")
    assert old_row["status"] == "pending"
    assert old_row["owner_worker_id"] == "other-worker"


@pytest.mark.anyio
async def test_create_thread_operation_atomic_interrupt_allows_self_owned_valid_lease():
    """Interrupt must succeed when the existing valid-lease run is owned by this worker."""
    store = MemoryRunStore()
    config = _lease_config(grace_seconds=10)
    valid_lease = (datetime.now(UTC) + timedelta(seconds=30)).isoformat()

    await store.create_thread_operation_atomic(
        run_id="self-run",
        thread_id="thread-1",
        owner_worker_id="w1",
        lease_expires_at=valid_lease,
        multitask_strategy="reject",
        grace_seconds=config.grace_seconds,
    )

    new_row, claimed = await store.create_thread_operation_atomic(
        run_id="run-new",
        thread_id="thread-1",
        owner_worker_id="w1",  # same worker
        lease_expires_at=(datetime.now(UTC) + timedelta(seconds=30)).isoformat(),
        multitask_strategy="interrupt",
        grace_seconds=config.grace_seconds,
    )

    assert new_row["run_id"] == "run-new"
    assert len(claimed) == 1
    assert claimed[0]["run_id"] == "self-run"
    assert claimed[0]["status"] == "interrupted"


@pytest.mark.anyio
async def test_create_thread_operation_atomic_interrupt_rolls_back_earlier_mutations_on_conflict():
    """Interrupt must not leave earlier candidates interrupted when a later
    candidate raises ConflictError.

    Mirrors the SQL store's transactional semantics: the whole interrupt pass
    is one transaction, so a raise on any candidate must roll back mutations
    already applied to earlier candidates. Without this, the memory store
    diverges from SQL (which the production path uses), and the
    test_multi_worker_run_ownership.py suite gives false confidence by
    passing against memory while SQL would behave differently.

    Setup: expired-lease run (interruptible) inserted FIRST, then a
    valid-lease run owned by another worker. Iteration order means the
    expired run is mutated before the valid-lease run raises — so a naive
    single-pass implementation would leave the expired run interrupted.
    """
    store = MemoryRunStore()
    config = _lease_config(grace_seconds=10)
    expired_lease = (datetime.now(UTC) - timedelta(seconds=60)).isoformat()
    valid_lease = (datetime.now(UTC) + timedelta(seconds=30)).isoformat()

    # Seed both active rows directly via ``put`` (bypassing atomic operation creation's
    # reject check, which would refuse the second row). Insert the
    # interruptible run first so dict iteration visits it first — that's the
    # ordering that exposes the half-interrupted divergence in a naive
    # single-pass implementation.
    await store.put(
        "expired-run",
        thread_id="thread-1",
        status="pending",
        owner_worker_id="old-worker",
        lease_expires_at=expired_lease,
    )
    await store.put(
        "valid-lease-run",
        thread_id="thread-1",
        status="pending",
        owner_worker_id="other-worker",
        lease_expires_at=valid_lease,
    )

    with pytest.raises(ConflictError, match="another worker"):
        await store.create_thread_operation_atomic(
            run_id="run-new",
            thread_id="thread-1",
            owner_worker_id="w1",
            lease_expires_at=(datetime.now(UTC) + timedelta(seconds=30)).isoformat(),
            multitask_strategy="interrupt",
            grace_seconds=config.grace_seconds,
        )

    # The expired run must be UNTOUCHED — the interrupt pass must roll back
    # on ConflictError, not leave a half-interrupted store.
    expired_row = await store.get("expired-run")
    assert expired_row["status"] == "pending"
    assert expired_row["owner_worker_id"] == "old-worker"
    assert expired_row["error"] is None

    # The valid-lease run that caused the conflict is also untouched.
    valid_row = await store.get("valid-lease-run")
    assert valid_row["status"] == "pending"
    assert valid_row["owner_worker_id"] == "other-worker"

    # The new run was never inserted.
    assert await store.get("run-new") is None


# ---------------------------------------------------------------------------
# update_lease
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_update_lease_renews_row():
    """update_lease must update the lease_expires_at on the stored row."""
    store = MemoryRunStore()
    old_lease = (datetime.now(UTC) + timedelta(seconds=5)).isoformat()
    await store.put(
        "run-1",
        thread_id="thread-1",
        status="running",
        owner_worker_id="w1",
        lease_expires_at=old_lease,
    )

    new_lease = (datetime.now(UTC) + timedelta(seconds=30)).isoformat()
    updated = await store.update_lease(
        "run-1",
        owner_worker_id="w1",
        lease_expires_at=new_lease,
    )
    assert updated is True

    stored = await store.get("run-1")
    assert stored["lease_expires_at"] == new_lease


@pytest.mark.anyio
async def test_update_lease_returns_false_for_terminal_run():
    """update_lease must return False when the run is not pending/running."""
    store = MemoryRunStore()
    await store.put(
        "run-1",
        thread_id="thread-1",
        status="success",
        owner_worker_id="w1",
        lease_expires_at=(datetime.now(UTC) + timedelta(seconds=5)).isoformat(),
    )

    new_lease = (datetime.now(UTC) + timedelta(seconds=30)).isoformat()
    updated = await store.update_lease(
        "run-1",
        owner_worker_id="w1",
        lease_expires_at=new_lease,
    )
    assert updated is False

    stored = await store.get("run-1")
    assert stored["status"] == "success"
    assert stored["owner_worker_id"] is None
    assert stored["lease_expires_at"] is None


@pytest.mark.anyio
async def test_update_lease_returns_false_for_wrong_owner():
    """update_lease must reject renewal when owner_worker_id does not match."""
    store = MemoryRunStore()
    old_lease = (datetime.now(UTC) + timedelta(seconds=5)).isoformat()
    await store.put(
        "run-1",
        thread_id="thread-1",
        status="running",
        owner_worker_id="w1",
        lease_expires_at=old_lease,
    )

    new_lease = (datetime.now(UTC) + timedelta(seconds=30)).isoformat()
    updated = await store.update_lease(
        "run-1",
        owner_worker_id="w2",  # different worker
        lease_expires_at=new_lease,
    )
    assert updated is False

    # The original lease must be untouched
    stored = await store.get("run-1")
    assert stored["owner_worker_id"] == "w1"
    assert stored["lease_expires_at"] == old_lease


# ---------------------------------------------------------------------------
# list_inflight_with_expired_lease
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_list_inflight_with_expired_lease_filters_correctly():
    """Only runs with expired or NULL leases must be returned."""
    store = MemoryRunStore()
    now = datetime.now(UTC)
    grace = 10

    # Expired lease
    expired = (now - timedelta(seconds=60)).isoformat()
    await store.put("expired-run", thread_id="t1", status="running", owner_worker_id="w1", lease_expires_at=expired, created_at=expired)

    # Valid lease
    valid = (now + timedelta(seconds=60)).isoformat()
    await store.put("valid-run", thread_id="t2", status="running", owner_worker_id="w2", lease_expires_at=valid, created_at=valid)

    # NULL lease (legacy)
    await store.put("null-lease-run", thread_id="t3", status="running", created_at=(now - timedelta(seconds=30)).isoformat())

    # Terminal status (should not appear)
    await store.put("success-run", thread_id="t4", status="success", created_at=(now - timedelta(seconds=60)).isoformat())

    results = await store.list_inflight_with_expired_lease(grace_seconds=grace)

    result_ids = {r["run_id"] for r in results}
    assert "expired-run" in result_ids
    assert "null-lease-run" in result_ids
    assert "valid-run" not in result_ids
    assert "success-run" not in result_ids


# ---------------------------------------------------------------------------
# MemoryRunStore — datetime comparison for created_at filtering
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_list_inflight_with_expired_lease_compares_created_at_as_datetime():
    """``before`` filter must use datetime comparison, not string lexical order.

    ISO-8601 strings compare lexically only when every component is zero-padded
    to the same width and the timezone suffix matches. Datetime parsing is
    order-safe regardless of format.
    """
    store = MemoryRunStore()
    now = datetime.now(UTC)
    grace = 10

    # A run created "now" — should be included when before=None (defaults to now).
    await store.put("recent-run", thread_id="t1", status="running", created_at=now.isoformat())
    # A run created far in the future — should be excluded by the before filter
    # even though the string "2300-01-01..." > "2025-..." lexically.
    far_future = "2300-01-01T00:00:00+00:00"
    await store.put("future-run", thread_id="t2", status="running", created_at=far_future)

    results = await store.list_inflight_with_expired_lease(before=now.isoformat(), grace_seconds=grace)
    result_ids = {r["run_id"] for r in results}
    assert "recent-run" in result_ids
    assert "future-run" not in result_ids


@pytest.mark.anyio
async def test_list_inflight_with_expired_lease_handles_malformed_created_at():
    """Malformed ``created_at`` values must not crash the listing."""
    store = MemoryRunStore()
    grace = 10

    store._runs["bad-run"] = {
        "run_id": "bad-run",
        "thread_id": "t1",
        "status": "running",
        "created_at": "not-a-datetime",
    }
    store._runs["empty-run"] = {
        "run_id": "empty-run",
        "thread_id": "t2",
        "status": "running",
        "created_at": "",
    }

    results = await store.list_inflight_with_expired_lease(grace_seconds=grace)
    # Both should be skipped because their created_at can't be parsed
    result_ids = {r["run_id"] for r in results}
    assert "bad-run" not in result_ids
    assert "empty-run" not in result_ids


@pytest.mark.anyio
async def test_list_inflight_with_expired_lease_datetime_aware_naive_handling():
    """Lease comparison must handle aware and naive datetimes.

    ``lease_expires_at`` stored with a trailing ``+00:00`` (aware) and without
    (naive) should both be comparable against the aware ``cutoff``. The MemoryRunStore
    uses ``datetime.fromisoformat`` which preserves the offset, so both paths
    must work.
    """
    store = MemoryRunStore()
    now = datetime.now(UTC)
    grace = 10

    # Naive datetime (no timezone suffix) — common on SQLite read-back
    naive_expired = (now - timedelta(seconds=60)).isoformat()  # "2025-01-01T00:00:00"
    await store.put("naive-run", thread_id="t1", status="running", lease_expires_at=naive_expired, created_at=naive_expired)

    # Aware datetime (with +00:00)
    aware_expired = (now - timedelta(seconds=60)).replace(tzinfo=UTC).isoformat()  # "2025-01-01T00:00:00+00:00"
    await store.put("aware-run", thread_id="t2", status="running", lease_expires_at=aware_expired, created_at=aware_expired)

    results = await store.list_inflight_with_expired_lease(grace_seconds=grace)
    result_ids = {r["run_id"] for r in results}
    # Both expired, both should be returned
    assert "naive-run" in result_ids
    assert "aware-run" in result_ids


@pytest.mark.anyio
async def test_list_inflight_with_expired_lease_null_lease_always_reclaimed():
    """NULL lease rows are always reclaimed regardless of created_at value."""
    store = MemoryRunStore()
    grace = 10

    # NULL lease is the single-worker mode default — every inflight row
    # must be returned so reconciliation can reclaim it.
    await store.put("null-run", thread_id="t1", status="running", created_at=datetime.now(UTC).isoformat())

    results = await store.list_inflight_with_expired_lease(grace_seconds=grace)
    result_ids = {r["run_id"] for r in results}
    assert "null-run" in result_ids


# ---------------------------------------------------------------------------
# claim_for_takeover — store primitive
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_claim_for_takeover_succeeds_with_expired_lease():
    """claim_for_takeover must succeed when the lease has passed the grace window."""
    store = MemoryRunStore()
    grace = 10
    expired_lease = (datetime.now(UTC) - timedelta(seconds=grace + 5)).isoformat()
    await store.put("run-1", thread_id="t1", status="running", created_at=datetime.now(UTC).isoformat(), owner_worker_id="w-a", lease_expires_at=expired_lease)

    ok = await store.claim_for_takeover(
        "run-1",
        grace_seconds=grace,
        error="claimed",
        stop_reason=ORPHAN_RECOVERY_STOP_REASON,
    )
    assert ok is True

    row = await store.get("run-1")
    assert row is not None
    assert row["status"] == "error"
    assert row["error"] == "claimed"
    assert row["stop_reason"] == ORPHAN_RECOVERY_STOP_REASON


@pytest.mark.anyio
async def test_claim_for_takeover_fails_with_valid_lease():
    """claim_for_takeover must return False when the lease is still valid."""
    store = MemoryRunStore()
    grace = 10
    valid_lease = (datetime.now(UTC) + timedelta(seconds=60)).isoformat()
    await store.put("run-1", thread_id="t1", status="running", created_at=datetime.now(UTC).isoformat(), owner_worker_id="w-a", lease_expires_at=valid_lease)

    ok = await store.claim_for_takeover("run-1", grace_seconds=grace, error="claimed")
    assert ok is False

    row = await store.get("run-1")
    assert row is not None
    assert row["status"] == "running"


@pytest.mark.anyio
async def test_claim_for_takeover_succeeds_with_null_lease():
    """NULL-lease rows (pre-ownership data) must be claimable."""
    store = MemoryRunStore()
    await store.put("run-null", thread_id="t1", status="running", created_at=datetime.now(UTC).isoformat())

    ok = await store.claim_for_takeover("run-null", grace_seconds=10, error="claimed")
    assert ok is True

    row = await store.get("run-null")
    assert row["status"] == "error"


@pytest.mark.anyio
async def test_claim_for_takeover_fails_on_terminal_status():
    """claim_for_takeover must return False for already-terminal runs."""
    store = MemoryRunStore()
    await store.put("run-done", thread_id="t1", status="success", created_at=datetime.now(UTC).isoformat())

    ok = await store.claim_for_takeover("run-done", grace_seconds=10, error="claimed")
    assert ok is False


@pytest.mark.anyio
async def test_claim_for_takeover_fails_for_nonexistent_run():
    """claim_for_takeover must return False when the run doesn't exist."""
    store = MemoryRunStore()
    ok = await store.claim_for_takeover("no-such-run", grace_seconds=10, error="claimed")
    assert ok is False


# ---------------------------------------------------------------------------
# cancel() cross-worker takeover — work item 4
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_cancel_takeover_from_crashed_worker():
    """cancel must take over (mark error) when lease is expired and owner is another worker."""
    store = MemoryRunStore()
    grace = 10
    expired_lease = (datetime.now(UTC) - timedelta(seconds=grace + 5)).isoformat()
    await store.put("run-expired", thread_id="t1", status="running", created_at=datetime.now(UTC).isoformat(), owner_worker_id="dead-worker", lease_expires_at=expired_lease)

    manager = _make_manager(store=store, run_ownership_config=_lease_config(heartbeat_enabled=True, grace_seconds=grace))
    outcome = await manager.cancel("run-expired")
    assert outcome == CancelOutcome.taken_over

    row = await store.get("run-expired")
    assert row is not None
    assert row["status"] == "error"


@pytest.mark.anyio
async def test_cancel_cannot_take_over_ineligible_exact_two_run():
    """An exact-two kill switch must govern non-local cancel takeover too."""
    store = MemoryRunStore()
    grace = 10
    expired_lease = (datetime.now(UTC) - timedelta(seconds=grace + 5)).isoformat()
    await store.put(
        "run-exact-two-ineligible",
        thread_id="t1",
        status="running",
        created_at=datetime.now(UTC).isoformat(),
        owner_worker_id="dead-worker",
        lease_expires_at=expired_lease,
        recovery_policy=RecoveryPolicy.exact_two_takeover_v1,
        recovery_payload_json=_execution_recovery_payload("t1"),
    )

    manager = _make_manager(
        store=store,
        run_ownership_config=_lease_config(
            heartbeat_enabled=True,
            grace_seconds=grace,
        ),
        execution_recovery_claims_enabled=True,
        execution_takeover_eligibility=lambda _record: False,
    )
    before = await store.get("run-exact-two-ineligible")

    outcome = await manager.cancel("run-exact-two-ineligible")
    row = await store.get("run-exact-two-ineligible")

    assert outcome == CancelOutcome.not_active_locally
    assert row == before


@pytest.mark.anyio
async def test_cancel_requests_active_lease_from_other_worker():
    """A cancel routed to a peer must durably notify the live owner."""
    store = MemoryRunStore()
    grace = 10
    valid_lease = (datetime.now(UTC) + timedelta(seconds=60)).isoformat()
    await store.put("run-alive", thread_id="t1", status="running", created_at=datetime.now(UTC).isoformat(), owner_worker_id="alive-worker", lease_expires_at=valid_lease)

    manager = _make_manager(store=store, run_ownership_config=_lease_config(heartbeat_enabled=True, grace_seconds=grace))
    outcome = await manager.cancel("run-alive")
    assert outcome == CancelOutcome.requested

    row = await store.get("run-alive")
    assert row is not None
    assert row["status"] == "running"
    assert row["cancel_action"] == "interrupt"
    assert row["cancel_requested_at"] is not None


@pytest.mark.anyio
async def test_non_owner_cancel_is_observed_by_owner_heartbeat():
    """Heartbeat signals the owner task without performing terminal writes."""
    store = MemoryRunStore()
    config = _lease_config(heartbeat_enabled=True, lease_seconds=30)
    owner = _make_manager(store=store, worker_id="worker-a", run_ownership_config=config)
    peer = _make_manager(store=store, worker_id="worker-b", run_ownership_config=config)

    record = await owner.create_or_reject("thread-1")
    await owner.set_status(record.run_id, RunStatus.running)
    record.task = asyncio.create_task(asyncio.sleep(3600))

    try:
        assert await peer.cancel(record.run_id, action="rollback") == CancelOutcome.requested
        # Match local idempotency: once accepted, a later action cannot change
        # whether the owner rolls back or merely interrupts.
        assert await peer.cancel(record.run_id, action="interrupt") == CancelOutcome.requested

        await owner._renew_leases()
        await asyncio.sleep(0)

        assert record.abort_event.is_set()
        assert record.abort_action == "rollback"
        assert record.status == RunStatus.running
        assert record.task.cancelled()

        stored = await store.get(record.run_id)
        assert stored is not None
        assert stored["status"] == "running"
        assert stored["cancel_action"] == "rollback"

        # The worker's existing cancellation path, not heartbeat, owns the
        # terminal status write and any rollback cleanup.
        await owner.set_status(
            record.run_id,
            RunStatus.error,
            error="Rolled back by user",
        )
        next_run = await peer.create_or_reject("thread-1")
        assert next_run.status == RunStatus.pending
    finally:
        if not record.task.done():
            record.task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await record.task


@pytest.mark.anyio
async def test_first_cancel_action_wins_when_retry_lands_on_owner():
    """Routing a retry to the owner must not replace the durable first action."""
    store = MemoryRunStore()
    config = _lease_config(heartbeat_enabled=True, lease_seconds=30)
    owner = _make_manager(store=store, worker_id="worker-a", run_ownership_config=config)
    peer = _make_manager(store=store, worker_id="worker-b", run_ownership_config=config)

    record = await owner.create_or_reject("thread-1")
    await owner.set_status(record.run_id, RunStatus.running)
    record.task = asyncio.create_task(asyncio.sleep(3600))

    try:
        assert await peer.cancel(record.run_id, action="rollback") == CancelOutcome.requested
        assert await owner.cancel(record.run_id, action="interrupt") == CancelOutcome.cancelled
        await asyncio.sleep(0)

        assert record.abort_action == "rollback"
        assert record.task.cancelled()
        stored = await store.get(record.run_id)
        assert stored is not None
        assert stored["cancel_action"] == "rollback"
    finally:
        if not record.task.done():
            record.task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await record.task


@pytest.mark.anyio
async def test_local_owner_cancel_falls_back_when_durable_request_fails():
    """A local owner can still abort its own task when durable cancel persistence fails."""

    class FailingCancelStore(MemoryRunStore):
        async def request_cancel(self, run_id: str, *, action: str) -> str | None:
            raise RuntimeError("store unavailable")

    store = FailingCancelStore()
    manager = _make_manager(
        store=store,
        worker_id="worker-a",
        run_ownership_config=_lease_config(heartbeat_enabled=True, lease_seconds=30),
    )

    record = await manager.create_or_reject("thread-1")
    await manager.set_status(record.run_id, RunStatus.running)
    record.task = asyncio.create_task(asyncio.sleep(3600))

    try:
        assert await manager.cancel(record.run_id, action="rollback") == CancelOutcome.cancelled
        assert record.abort_event.is_set()
        assert record.abort_action == "rollback"

        with pytest.raises(asyncio.CancelledError):
            await record.task

        stored = await store.get(record.run_id)
        assert stored is not None
        assert stored["status"] == "error"
        assert stored["error"] == "Rolled back by user"
        assert stored["cancel_action"] is None
    finally:
        if not record.task.done():
            record.task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await record.task


@pytest.mark.anyio
async def test_owner_cancel_retry_on_peer_is_accepted():
    """A peer must treat the owner's interrupted status as an accepted cancel."""
    store = MemoryRunStore()
    config = _lease_config(heartbeat_enabled=True, lease_seconds=30)
    owner = _make_manager(store=store, worker_id="worker-a", run_ownership_config=config)
    peer = _make_manager(store=store, worker_id="worker-b", run_ownership_config=config)

    record = await owner.create_or_reject("thread-1")
    await owner.set_status(record.run_id, RunStatus.running)

    assert await owner.cancel(record.run_id, action="rollback") == CancelOutcome.cancelled
    assert await peer.cancel(record.run_id, action="rollback") == CancelOutcome.requested

    stored = await store.get(record.run_id)
    assert stored is not None
    assert stored["status"] == "error"
    assert stored["cancel_action"] == "rollback"


@pytest.mark.anyio
async def test_owner_cancel_uses_store_while_terminal_status_is_staged_locally():
    """A staged local terminal status must not bypass the durable cancel CAS."""
    store = MemoryRunStore()
    manager = _make_manager(
        store=store,
        run_ownership_config=_lease_config(heartbeat_enabled=True),
    )
    record = await manager.create_or_reject("thread-1")
    await manager.set_status(record.run_id, RunStatus.running)

    # Event-store finalization stages success in memory before persisting it.
    record.status = RunStatus.success

    assert await manager.cancel(record.run_id, action="rollback") == CancelOutcome.cancelled
    stored = await store.get(record.run_id)
    assert stored is not None
    assert stored["status"] == "error"
    assert stored["cancel_action"] == "rollback"


@pytest.mark.anyio
async def test_cancel_returns_unknown_when_no_store():
    """cancel must return unknown when there's no store and the run is not in memory."""
    manager = _make_manager(run_ownership_config=_lease_config(heartbeat_enabled=True))
    outcome = await manager.cancel("no-such-run")
    assert outcome == CancelOutcome.unknown


@pytest.mark.anyio
async def test_cancel_returns_not_active_locally_when_heartbeat_disabled():
    """With heartbeat disabled, store-only runs must not be cancellable (old 409 path)."""
    store = MemoryRunStore()
    await store.put("store-only", thread_id="t1", status="running", created_at=datetime.now(UTC).isoformat())

    manager = _make_manager(store=store, run_ownership_config=_lease_config(heartbeat_enabled=False))
    outcome = await manager.cancel("store-only")
    assert outcome == CancelOutcome.not_active_locally


@pytest.mark.anyio
async def test_cancel_takeover_race_owner_renewed_lease():
    """When takeover loses to a renewal, cancellation must notify the live owner."""
    store = MemoryRunStore()
    grace = 10
    expired_lease = (datetime.now(UTC) - timedelta(seconds=grace + 5)).isoformat()
    await store.put("run-race", thread_id="t1", status="running", created_at=datetime.now(UTC).isoformat(), owner_worker_id="w-a", lease_expires_at=expired_lease)

    # Simulate the race: right before claim_for_takeover writes, another
    # heartbeat renews the lease.  We monkey-patch claim_for_takeover to
    # simulate the lease having been renewed.
    original = store.claim_for_takeover

    async def race_lost(run_id, *, grace_seconds, error, stop_reason=None, expected_state_version=None):
        # Simulate a heartbeat renewal between the read and the write
        run = store._runs.get(run_id)
        if run and run["status"] in ("pending", "running"):
            run["lease_expires_at"] = (datetime.now(UTC) + timedelta(seconds=60)).isoformat()
        return await original(
            run_id,
            grace_seconds=grace_seconds,
            error=error,
            stop_reason=stop_reason,
            expected_state_version=expected_state_version,
        )

    store.claim_for_takeover = race_lost
    manager = _make_manager(store=store, run_ownership_config=_lease_config(heartbeat_enabled=True, grace_seconds=grace))

    outcome = await manager.cancel("run-race")
    assert outcome == CancelOutcome.requested
    assert (await store.get("run-race"))["cancel_action"] == "interrupt"


@pytest.mark.anyio
async def test_cancel_takeover_respects_grace_seconds():
    """Within the grace window, cancellation must notify rather than take over."""
    store = MemoryRunStore()
    grace = 10
    # Lease expired, but only by 3s — still within the 10s grace window
    just_expired = (datetime.now(UTC) - timedelta(seconds=3)).isoformat()
    await store.put("run-grace", thread_id="t1", status="running", created_at=datetime.now(UTC).isoformat(), owner_worker_id="w-a", lease_expires_at=just_expired)

    manager = _make_manager(store=store, run_ownership_config=_lease_config(heartbeat_enabled=True, grace_seconds=grace))
    outcome = await manager.cancel("run-grace")
    assert outcome == CancelOutcome.requested
    assert (await store.get("run-grace"))["cancel_action"] == "interrupt"


@pytest.mark.anyio
async def test_cancel_not_cancellable_for_store_terminal_run():
    """cancel must return not_cancellable when the store run is already in a terminal state."""
    store = MemoryRunStore()
    await store.put("run-done", thread_id="t1", status="success", created_at=datetime.now(UTC).isoformat())

    manager = _make_manager(store=store, run_ownership_config=_lease_config(heartbeat_enabled=True))
    outcome = await manager.cancel("run-done")
    assert outcome == CancelOutcome.not_cancellable


# ---------------------------------------------------------------------------
# HTTP-level — cancel endpoint cross-worker responses
# ---------------------------------------------------------------------------


class _EndingCrossProcessBridge:
    supports_cross_process = True

    async def publish(self, run_id, event, data):
        return None

    async def publish_end(self, run_id):
        return None

    def subscribe(self, run_id, *, last_event_id=None, heartbeat_interval=15.0):
        from deerflow.runtime import END_SENTINEL

        async def events():
            yield END_SENTINEL

        return events()

    async def cleanup(self, run_id, *, delay=0):
        return None


def _make_cancel_test_app(mgr: RunManager, *, bridge=None):
    """Build a TestClient wired with the thread_runs router + memory bridge."""
    from _router_auth_helpers import make_authed_test_app
    from fastapi.testclient import TestClient

    from app.gateway.routers import thread_runs
    from deerflow.runtime import MemoryStreamBridge

    app = make_authed_test_app()
    app.include_router(thread_runs.router)
    app.state.run_manager = mgr
    app.state.stream_bridge = bridge or MemoryStreamBridge()
    return TestClient(app, raise_server_exceptions=False)


def test_http_cancel_non_owner_valid_lease_returns_202():
    """POST /cancel must not fail solely because routing chose a non-owner."""
    store = MemoryRunStore()
    grace = 10
    valid_lease = (datetime.now(UTC) + timedelta(seconds=60)).isoformat()
    asyncio.run(
        store.put(
            "run-alive",
            thread_id="t1",
            status="running",
            created_at=datetime.now(UTC).isoformat(),
            owner_worker_id="alive-worker",
            lease_expires_at=valid_lease,
        )
    )
    mgr = _make_manager(store=store, run_ownership_config=_lease_config(heartbeat_enabled=True, grace_seconds=grace))
    client = _make_cancel_test_app(mgr)

    resp = client.post("/api/threads/t1/runs/run-alive/cancel")
    assert resp.status_code == 202
    assert "Retry-After" not in resp.headers

    # The owner remains fenced, while the cancellation request is durable.
    row = asyncio.run(store.get("run-alive"))
    assert row["status"] == "running"
    assert row["cancel_action"] == "interrupt"


def test_http_stream_action_non_owner_without_shared_bridge_returns_202():
    """A peer cancel is accepted without subscribing to an unreachable local stream."""
    store = MemoryRunStore()
    valid_lease = (datetime.now(UTC) + timedelta(seconds=60)).isoformat()
    asyncio.run(
        store.put(
            "run-alive-stream",
            thread_id="t1",
            status="running",
            created_at=datetime.now(UTC).isoformat(),
            owner_worker_id="alive-worker",
            lease_expires_at=valid_lease,
        )
    )
    mgr = _make_manager(
        store=store,
        run_ownership_config=_lease_config(heartbeat_enabled=True),
    )
    client = _make_cancel_test_app(mgr)

    resp = client.post(
        "/api/threads/t1/runs/run-alive-stream/stream",
        params={"action": "interrupt"},
    )

    assert resp.status_code == 202
    row = asyncio.run(store.get("run-alive-stream"))
    assert row["status"] == "running"
    assert row["cancel_action"] == "interrupt"


def test_http_cancel_non_owner_wait_uses_shared_bridge():
    """wait=true observes remote owner finalization through the shared bridge."""
    store = MemoryRunStore()
    valid_lease = (datetime.now(UTC) + timedelta(seconds=60)).isoformat()
    asyncio.run(
        store.put(
            "run-alive-wait",
            thread_id="t1",
            status="running",
            created_at=datetime.now(UTC).isoformat(),
            owner_worker_id="alive-worker",
            lease_expires_at=valid_lease,
        )
    )
    mgr = _make_manager(
        store=store,
        run_ownership_config=_lease_config(heartbeat_enabled=True),
    )
    client = _make_cancel_test_app(mgr, bridge=_EndingCrossProcessBridge())

    resp = client.post(
        "/api/threads/t1/runs/run-alive-wait/cancel",
        params={"action": "rollback", "wait": "true"},
    )

    assert resp.status_code == 204
    row = asyncio.run(store.get("run-alive-wait"))
    assert row["cancel_action"] == "rollback"


def test_http_stream_action_non_owner_uses_shared_bridge():
    """The SDK stop path drains the remote owner's shared stream after accept."""
    store = MemoryRunStore()
    valid_lease = (datetime.now(UTC) + timedelta(seconds=60)).isoformat()
    asyncio.run(
        store.put(
            "run-alive-shared-stream",
            thread_id="t1",
            status="running",
            created_at=datetime.now(UTC).isoformat(),
            owner_worker_id="alive-worker",
            lease_expires_at=valid_lease,
        )
    )
    mgr = _make_manager(
        store=store,
        run_ownership_config=_lease_config(heartbeat_enabled=True),
    )
    client = _make_cancel_test_app(mgr, bridge=_EndingCrossProcessBridge())

    resp = client.post(
        "/api/threads/t1/runs/run-alive-shared-stream/stream",
        params={"action": "interrupt"},
    )

    assert resp.status_code == 200
    assert "event: end" in resp.text
    row = asyncio.run(store.get("run-alive-shared-stream"))
    assert row["cancel_action"] == "interrupt"


def test_http_cancel_non_owner_expired_lease_returns_202_takeover():
    """POST /cancel on a non-owning worker with an expired lease must return 202 (takeover)."""
    store = MemoryRunStore()
    grace = 10
    expired_lease = (datetime.now(UTC) - timedelta(seconds=grace + 30)).isoformat()
    asyncio.run(
        store.put(
            "run-dead",
            thread_id="t1",
            status="running",
            created_at=datetime.now(UTC).isoformat(),
            owner_worker_id="dead-worker",
            lease_expires_at=expired_lease,
        )
    )
    mgr = _make_manager(store=store, run_ownership_config=_lease_config(heartbeat_enabled=True, grace_seconds=grace))
    client = _make_cancel_test_app(mgr)

    resp = client.post("/api/threads/t1/runs/run-dead/cancel")
    assert resp.status_code == 202

    # Store row must be marked error
    row = asyncio.run(store.get("run-dead"))
    assert row["status"] == "error"


def test_http_stream_action_interrupt_takeover_returns_202_not_hang():
    """POST /stream?action=interrupt on a dead-owner run must return 202 immediately, not hang on SSE."""
    store = MemoryRunStore()
    grace = 10
    expired_lease = (datetime.now(UTC) - timedelta(seconds=grace + 30)).isoformat()
    asyncio.run(
        store.put(
            "run-dead-stream",
            thread_id="t1",
            status="running",
            created_at=datetime.now(UTC).isoformat(),
            owner_worker_id="dead-worker",
            lease_expires_at=expired_lease,
        )
    )
    mgr = _make_manager(store=store, run_ownership_config=_lease_config(heartbeat_enabled=True, grace_seconds=grace))
    client = _make_cancel_test_app(mgr)

    # This must NOT hang — the takeover path returns 202 before reaching StreamingResponse.
    resp = client.post("/api/threads/t1/runs/run-dead-stream/stream", params={"action": "interrupt"})
    assert resp.status_code == 202

    row = asyncio.run(store.get("run-dead-stream"))
    assert row["status"] == "error"


# ---------------------------------------------------------------------------
# Split-brain defences — update_status guard + heartbeat self-termination
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_update_status_rejects_terminal_row():
    """update_status must return False when the store row is already terminal
    (error/success), so a late writer cannot overwrite a peer's takeover or
    a completed run. interrupted is NOT terminal — the rollback path needs
    ``interrupted → error`` to finalize."""
    store = MemoryRunStore()
    # error (takeover) must stay locked
    await store.put("run-err", thread_id="t1", status="error", created_at=datetime.now(UTC).isoformat())
    assert await store.update_status("run-err", "success") is False
    assert (await store.get("run-err"))["status"] == "error"

    # success must stay locked
    await store.put("run-ok", thread_id="t1", status="success", created_at=datetime.now(UTC).isoformat())
    assert await store.update_status("run-ok", "error") is False
    assert (await store.get("run-ok"))["status"] == "success"

    # interrupted → error MUST pass (rollback finalize path)
    await store.put("run-rb", thread_id="t1", status="interrupted", created_at=datetime.now(UTC).isoformat())
    assert await store.update_status("run-rb", "error", error="Rolled back by user") is True
    row = await store.get("run-rb")
    assert row["status"] == "error"
    assert row["error"] == "Rolled back by user"


@pytest.mark.anyio
async def test_persist_status_skips_recovery_when_row_taken_over():
    """_persist_status must not recreate a row that was taken over by another worker.

    When update_status returns False, the recovery path checks whether the
    row still exists. A row that exists but is terminal (taken over) must
    be left alone — calling put() would overwrite the takeover."""
    store = MemoryRunStore()
    mgr = RunManager(store=store, run_ownership_config=_lease_config(heartbeat_enabled=True))

    # Simulate: this worker created and started a run, but a peer took it over.
    record = await mgr.create("thread-1")
    await mgr.set_status(record.run_id, RunStatus.running)
    # Peer takeover: directly flip the store row to error
    await store.update_status(record.run_id, "error")
    # Now simulate the original owner's task finishing and trying to write success
    ok = await mgr._persist_status(record, RunStatus.success)
    assert ok is False  # skipped recovery, row already exists and is terminal
    row = await store.get(record.run_id)
    assert row["status"] == "error"  # not overwritten


@pytest.mark.anyio
async def test_heartbeat_cancels_task_on_lease_loss():
    """Heartbeat must cancel the local asyncio task when update_lease returns False.

    If the store row was claimed by another worker (status no longer
    pending/running, or owner changed), the heartbeat tick must abort the
    local task so wasted CPU is bounded to ~10s instead of the full task
    lifetime."""
    store = MemoryRunStore()
    mgr = RunManager(store=store, run_ownership_config=_lease_config(heartbeat_enabled=True, lease_seconds=30))

    # Create a run that this worker owns
    record = await mgr.create("thread-1")
    await mgr.set_status(record.run_id, RunStatus.running)

    # Spawn a dummy task so cancel has something to stop
    loop = asyncio.get_running_loop()
    record.task = loop.create_task(asyncio.sleep(3600))

    # Simulate takeover: directly flip the store row to error
    await store.update_status(record.run_id, "error")

    # Run a single heartbeat tick — it should see update_lease return False
    # and cancel the task
    await mgr._renew_leases()

    # Let the event loop process the cancellation (task.cancel() schedules,
    # doesn't await).
    await asyncio.sleep(0)
    assert record.ownership_lost is True
    assert record.abort_event.is_set() is True
    assert record.task.cancelled()


@pytest.mark.anyio
async def test_cancel_returns_taken_over_when_peer_claims_during_local_cancel():
    """When a peer's claim_for_takeover flips the row to error between this
    worker's in-memory cancel and the guarded update_status, cancel() must
    surface taken_over (not cancelled) so the client sees a status consistent
    with the store."""
    store = MemoryRunStore()
    mgr = RunManager(store=store, run_ownership_config=_lease_config(heartbeat_enabled=True))

    record = await mgr.create("thread-1")
    await mgr.set_status(record.run_id, RunStatus.running)

    original = store.transition_run_atomic
    original_owned = store.transition_owned_run_atomic
    injected = False

    async def race_transition(run_id, **kwargs):
        nonlocal injected
        transition = kwargs["transition"]
        if not injected and transition.lifecycle_type == LifecycleType.cancelled:
            injected = True
            current = await store.get(run_id)
            await original(
                run_id,
                expected_state_version=current["state_version"],
                expected_statuses=("running",),
                transition=LifecycleTransition(
                    lifecycle_type=LifecycleType.failed,
                    status="error",
                    error="peer takeover",
                    reason=ORPHAN_RECOVERY_STOP_REASON,
                ),
            )
        return await original_owned(run_id, **kwargs)

    store.transition_owned_run_atomic = race_transition

    outcome = await mgr.cancel(record.run_id)
    assert outcome == CancelOutcome.taken_over

    # Store row must reflect the takeover, not the local cancel
    row = await store.get(record.run_id)
    assert row["status"] == "error"


@pytest.mark.anyio
async def test_cancel_action_rollback_finalizes_to_error_in_store():
    """action=rollback must end up as error in the store with the
    "Rolled back by user" message preserved.

    Regression guard: the update_status guard was originally
    ``status IN ('pending','running')`` which blocked the rollback path's
    ``interrupted → error`` transition — the store stayed interrupted and
    the rollback message was lost.
    """
    store = MemoryRunStore()
    mgr = RunManager(store=store, run_ownership_config=_lease_config(heartbeat_enabled=True))

    record = await mgr.create("thread-1")
    await mgr.set_status(record.run_id, RunStatus.running)

    # Cancellation request and rollback finalization are distinct journal
    # transitions, while the authoritative row lands directly at error.
    outcome = await mgr.cancel(record.run_id, action="rollback")
    assert outcome == CancelOutcome.cancelled
    row = await store.get(record.run_id)
    assert row["status"] == "error"
    assert row["error"] == "Rolled back by user"


@pytest.mark.anyio
async def test_peer_reconciliation_fences_late_success_and_completion():
    """A stale owner cannot overwrite a peer's terminal takeover."""
    store = MemoryRunStore()
    config = _lease_config(heartbeat_enabled=True, grace_seconds=0)
    owner = _make_manager(store=store, worker_id="worker-a", run_ownership_config=config)
    peer = _make_manager(store=store, worker_id="worker-b", run_ownership_config=config)
    record = await owner.create("thread-1")
    await owner.set_status(record.run_id, RunStatus.running)

    expired = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()
    record.lease_expires_at = expired
    store._runs[record.run_id]["lease_expires_at"] = expired
    recovered = await peer.reconcile_orphaned_inflight_runs(error="peer takeover")

    assert [recovered_record.run_id for recovered_record in recovered] == [record.run_id]
    assert (await store.get(record.run_id))["status"] == "error"

    await owner.set_status(record.run_id, RunStatus.success)
    await owner.update_run_completion(record.run_id, status=record.status.value, total_tokens=1)

    row = await store.get(record.run_id)
    assert record.ownership_lost is True
    assert record.status == RunStatus.error
    assert row["status"] == "error"
    assert row["error"] == "peer takeover"


@pytest.mark.anyio
async def test_unconfirmed_success_is_fenced_when_heartbeat_is_enabled():
    """A store outage cannot turn an unconfirmed terminal write into local success."""
    store = MemoryRunStore()
    manager = _make_manager(
        store=store,
        worker_id="worker-a",
        run_ownership_config=_lease_config(heartbeat_enabled=True),
    )
    record = await manager.create("thread-1")
    await manager.set_status(record.run_id, RunStatus.running)

    async def fail_status_write(*_args, **_kwargs):
        raise OSError("database unreachable")

    store.transition_owned_run_atomic = fail_status_write

    await manager.set_status(record.run_id, RunStatus.success)

    row = await store.get(record.run_id)
    assert record.ownership_lost is True
    assert record.status == RunStatus.error
    assert record.abort_event.is_set() is True
    assert row["status"] == "running"


@pytest.mark.anyio
@pytest.mark.parametrize("terminal_status", [RunStatus.error, RunStatus.interrupted])
async def test_every_unconfirmed_terminal_status_loses_ownership(
    terminal_status,
):
    store = MemoryRunStore()
    manager = _make_manager(
        store=store,
        worker_id="worker-a",
        run_ownership_config=_lease_config(heartbeat_enabled=True),
    )
    record = await manager.create("thread-terminal-fence")
    await manager.set_status(record.run_id, RunStatus.running)

    async def fail_status_write(*_args, **_kwargs):
        raise OSError("database unreachable")

    store.transition_owned_run_atomic = fail_status_write
    await manager.set_status(record.run_id, terminal_status)

    assert record.ownership_lost is True
    assert record.abort_event.is_set() is True
    assert (await store.get(record.run_id))["status"] == "running"


@pytest.mark.anyio
async def test_stale_attachment_failure_cannot_terminalize_takeover_owner() -> None:
    store = MemoryRunStore()
    config = _lease_config(heartbeat_enabled=True, grace_seconds=0)
    owner = _make_manager(
        store=store,
        worker_id="worker-a",
        run_ownership_config=config,
    )
    record = await owner.create("thread-attachment-takeover")
    expired = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()
    record.lease_expires_at = expired
    store._runs[record.run_id]["lease_expires_at"] = expired
    store._runs[record.run_id]["recovery_policy"] = RecoveryPolicy.exact_two_takeover_v1.value
    active_version = store._runs[record.run_id]["state_version"]
    takeover = await store.claim_for_execution_takeover(
        record.run_id,
        new_owner_worker_id="worker-b",
        lease_expires_at=(datetime.now(UTC) + timedelta(minutes=1)).isoformat(),
        grace_seconds=0,
        expected_state_version=active_version,
    )
    assert takeover.outcome is ExecutionTakeoverOutcome.claimed

    assert (
        await owner.fail_start_if_pending(
            record.run_id,
            error="thread metadata unavailable",
        )
        is False
    )

    stored = await store.get(record.run_id)
    assert stored is not None
    assert stored["status"] == "pending"
    assert stored["owner_worker_id"] == "worker-b"
    assert stored["state_version"] == active_version + 1
    assert record.ownership_lost is True


@pytest.mark.anyio
async def test_duplicate_fenced_cancel_does_not_interrupt_terminal_cleanup() -> None:
    store = MemoryRunStore()
    manager = _make_manager(store=store)
    record = await manager.create("thread-duplicate-cancel")
    cleanup_started = asyncio.Event()
    release_cleanup = asyncio.Event()
    second_cancel = asyncio.Event()

    async def worker() -> None:
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            async with manager._lock:
                record.status = RunStatus.interrupted
            cleanup_started.set()
            try:
                await release_cleanup.wait()
            except asyncio.CancelledError:
                second_cancel.set()
                raise

    task = asyncio.create_task(worker())
    await asyncio.sleep(0)
    async with manager._lock:
        record.task = task
    initial_version = record.state_version

    first = await manager.request_cancel_fenced(
        record.run_id,
        action="interrupt",
        expected_state_version=initial_version,
    )
    assert first is CancellationRequestOutcome.requested
    await cleanup_started.wait()
    assert record.status is RunStatus.interrupted

    duplicate = await manager.request_cancel_fenced(
        record.run_id,
        action="interrupt",
        expected_state_version=initial_version,
    )

    assert duplicate is CancellationRequestOutcome.already_requested
    assert record.status is RunStatus.interrupted
    assert second_cancel.is_set() is False
    release_cleanup.set()
    await task


@pytest.mark.anyio
async def test_unconfirmed_staged_success_is_fenced_on_deferred_persistence():
    """Receipt-ordered status persistence retains the success fail-close fence."""
    store = MemoryRunStore()
    manager = _make_manager(
        store=store,
        worker_id="worker-a",
        run_ownership_config=_lease_config(heartbeat_enabled=True),
    )
    record = await manager.create("thread-1")
    await manager.set_status(record.run_id, RunStatus.running)
    await manager.set_status(record.run_id, RunStatus.success, persist=False)

    async def fail_status_write(*_args, **_kwargs):
        raise OSError("database unreachable")

    store.transition_owned_run_atomic = fail_status_write

    persisted = await manager.persist_current_status(record.run_id)

    row = await store.get(record.run_id)
    assert persisted is False
    assert record.ownership_lost is True
    assert record.status == RunStatus.error
    assert record.abort_event.is_set() is True
    assert row["status"] == "running"


# ---------------------------------------------------------------------------
# cancel() claim_for_takeover False → re-read precision
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_cancel_claim_lost_to_terminal_returns_not_cancellable():
    """When cancel() reads the run as active but claim_for_takeover returns
    False because the row went terminal (run finished) between the read and
    the conditional UPDATE, the re-read must surface not_cancellable."""
    store = MemoryRunStore()
    mgr = _make_manager(store=store, run_ownership_config=_lease_config(heartbeat_enabled=True, grace_seconds=10))

    # Seed as running so cancel()'s first read passes the status guard.
    expired = (datetime.now(UTC) - timedelta(seconds=60)).isoformat()
    await store.put(
        "run-race",
        thread_id="t1",
        status="running",
        owner_worker_id="w-a",
        lease_expires_at=expired,
        created_at=datetime.now(UTC).isoformat(),
    )

    # Wrap claim_for_takeover: flip the row to success just before the
    # conditional UPDATE so it matches 0 rows.
    original = store.claim_for_takeover

    async def race_claim(run_id, *, grace_seconds, error, stop_reason=None, expected_state_version=None):
        await store.update_status(run_id, "success")
        return await original(
            run_id,
            grace_seconds=grace_seconds,
            error=error,
            stop_reason=stop_reason,
            expected_state_version=expected_state_version,
        )

    store.claim_for_takeover = race_claim

    outcome = await mgr.cancel("run-race")
    assert outcome == CancelOutcome.not_cancellable


@pytest.mark.anyio
async def test_cancel_claim_lost_to_takeover_returns_taken_over():
    """When cancel() reads the run as active but claim_for_takeover returns
    False because another worker already took it over (row is error), the
    re-read must surface taken_over."""
    store = MemoryRunStore()
    mgr = _make_manager(store=store, run_ownership_config=_lease_config(heartbeat_enabled=True, grace_seconds=10))

    expired = (datetime.now(UTC) - timedelta(seconds=60)).isoformat()
    await store.put(
        "run-race",
        thread_id="t1",
        status="running",
        owner_worker_id="w-a",
        lease_expires_at=expired,
        created_at=datetime.now(UTC).isoformat(),
    )

    # Wrap claim_for_takeover: flip the row to error before the conditional
    # UPDATE so it matches 0 rows (peer already took it over).
    original = store.claim_for_takeover

    async def race_takeover(run_id, *, grace_seconds, error, stop_reason=None, expected_state_version=None):
        await store.update_status(
            run_id,
            "error",
            error="peer claim",
            stop_reason=ORPHAN_RECOVERY_STOP_REASON,
        )
        return await original(
            run_id,
            grace_seconds=grace_seconds,
            error=error,
            stop_reason=stop_reason,
            expected_state_version=expected_state_version,
        )

    store.claim_for_takeover = race_takeover

    outcome = await mgr.cancel("run-race")
    assert outcome == CancelOutcome.taken_over


# ---------------------------------------------------------------------------
# _compute_retry_after unit tests
# ---------------------------------------------------------------------------


def test_compute_retry_after_null_lease_returns_none():
    from app.gateway.routers.thread_runs import _compute_retry_after

    assert _compute_retry_after(None, 10) is None


def test_compute_retry_after_unparseable_returns_none():
    from app.gateway.routers.thread_runs import _compute_retry_after

    assert _compute_retry_after("not-a-date", 10) is None


def test_compute_retry_after_normal():
    from app.gateway.routers.thread_runs import _compute_retry_after

    future = (datetime.now(UTC) + timedelta(seconds=45)).isoformat()
    val = _compute_retry_after(future, 10)
    assert val is not None
    # lease_expires_at is ~45s from now + grace_seconds 10 = ~55, within reason
    assert 40 <= val <= 65
