"""Contract tests for fenced auxiliary thread-operation release."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from deerflow.config.run_ownership_config import RunOwnershipConfig
from deerflow.runtime import RunManager, ThreadOperationKind
from deerflow.runtime.runs.manager import PersistenceRetryPolicy
from deerflow.runtime.runs.store import base as run_store_contract
from deerflow.runtime.runs.store.memory import MemoryRunStore

_OWNER = "worker-release-owner"
_USER = "user-release-owner"


def _release_contract() -> tuple[type[Any], type[Any]]:
    """Return the required public release result types without breaking collection."""

    outcome_type = getattr(
        run_store_contract,
        "ThreadOperationReleaseOutcome",
        None,
    )
    result_type = getattr(
        run_store_contract,
        "ThreadOperationReleaseResult",
        None,
    )
    assert outcome_type is not None, "RunStore must export ThreadOperationReleaseOutcome"
    assert result_type is not None, "RunStore must export ThreadOperationReleaseResult"
    return outcome_type, result_type


async def _create_auxiliary_row(
    store: MemoryRunStore,
    *,
    run_id: str,
    thread_id: str,
    kind: ThreadOperationKind,
    user_id: str | None = _USER,
    owner_worker_id: str = _OWNER,
    lease_expires_at: str | None = None,
) -> dict[str, Any]:
    row, claimed = await store.create_thread_operation_atomic(
        run_id,
        thread_id=thread_id,
        owner_worker_id=owner_worker_id,
        lease_expires_at=lease_expires_at,
        operation_kind=kind.value,
        user_id=user_id,
    )
    assert claimed == []
    assert row["state_version"] == 0
    return row


async def _release_owned(
    store: MemoryRunStore,
    *,
    run_id: str,
    thread_id: str,
    kind: ThreadOperationKind,
    user_id: str | None = _USER,
    owner_worker_id: str = _OWNER,
    require_unexpired_lease: bool = False,
) -> Any:
    release = getattr(store, "release_thread_operation_owned", None)
    assert release is not None, "RunStore must implement release_thread_operation_owned()"
    return await release(
        run_id,
        thread_id=thread_id,
        operation_kind=kind.value,
        user_id=user_id,
        expected_owner_worker_id=owner_worker_id,
        require_unexpired_lease=require_unexpired_lease,
    )


@pytest.mark.anyio
@pytest.mark.parametrize(
    "kind",
    [
        ThreadOperationKind.checkpoint_write,
        ThreadOperationKind.artifact_write,
    ],
)
async def test_memory_store_releases_exact_owned_auxiliary_operation_without_lifecycle(
    kind: ThreadOperationKind,
) -> None:
    outcome_type, result_type = _release_contract()
    store = MemoryRunStore()
    run_id = f"release-{kind.value}"
    thread_id = f"thread-{kind.value}"
    await _create_auxiliary_row(
        store,
        run_id=run_id,
        thread_id=thread_id,
        kind=kind,
    )

    released = await _release_owned(
        store,
        run_id=run_id,
        thread_id=thread_id,
        kind=kind,
    )

    assert isinstance(released, result_type)
    assert released.outcome is outcome_type.released
    assert await store.get(run_id, user_id=_USER) is None
    assert await store.list_lifecycle_events(run_id=run_id) == []

    repeated = await _release_owned(
        store,
        run_id=run_id,
        thread_id=thread_id,
        kind=kind,
    )
    assert repeated.outcome is outcome_type.absent


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("changed_field", "expected_outcome"),
    [
        pytest.param("thread_id", "identity_mismatch", id="thread"),
        pytest.param("operation_kind", "identity_mismatch", id="operation-kind"),
        pytest.param("user_id", "identity_mismatch", id="user"),
        pytest.param("expected_owner_worker_id", "ownership_lost", id="worker"),
    ],
)
async def test_memory_store_release_rejects_every_changed_identity_or_owner_fence(
    changed_field: str,
    expected_outcome: str,
) -> None:
    outcome_type, result_type = _release_contract()
    store = MemoryRunStore()
    run_id = f"release-mismatch-{changed_field}"
    thread_id = "thread-release-mismatch"
    await _create_auxiliary_row(
        store,
        run_id=run_id,
        thread_id=thread_id,
        kind=ThreadOperationKind.checkpoint_write,
    )
    arguments: dict[str, Any] = {
        "thread_id": thread_id,
        "operation_kind": ThreadOperationKind.checkpoint_write.value,
        "user_id": _USER,
        "expected_owner_worker_id": _OWNER,
        "require_unexpired_lease": False,
    }
    replacements: dict[str, Any] = {
        "thread_id": "thread-another",
        "operation_kind": ThreadOperationKind.artifact_write.value,
        "user_id": "user-another",
        "expected_owner_worker_id": "worker-another",
    }
    arguments[changed_field] = replacements[changed_field]

    release = getattr(store, "release_thread_operation_owned", None)
    assert release is not None, "RunStore must implement release_thread_operation_owned()"
    result = await release(run_id, **arguments)

    assert isinstance(result, result_type)
    assert result.outcome is getattr(outcome_type, expected_outcome)
    retained = await store.get(run_id, user_id=_USER)
    assert retained is not None
    assert retained["status"] == "pending"
    assert await store.list_lifecycle_events(run_id=run_id) == []


@pytest.mark.anyio
async def test_memory_store_expired_auxiliary_lease_loses_release_authority() -> None:
    outcome_type, result_type = _release_contract()
    store = MemoryRunStore()
    run_id = "release-expired-lease"
    thread_id = "thread-release-expired"
    expired = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()
    await _create_auxiliary_row(
        store,
        run_id=run_id,
        thread_id=thread_id,
        kind=ThreadOperationKind.artifact_write,
        lease_expires_at=expired,
    )

    result = await _release_owned(
        store,
        run_id=run_id,
        thread_id=thread_id,
        kind=ThreadOperationKind.artifact_write,
        require_unexpired_lease=True,
    )

    assert isinstance(result, result_type)
    assert result.outcome is outcome_type.ownership_lost
    assert await store.get(run_id, user_id=_USER) is not None
    assert await store.list_lifecycle_events(run_id=run_id) == []


@pytest.mark.anyio
async def test_memory_store_reports_inactive_after_auxiliary_orphan_terminalization() -> None:
    outcome_type, result_type = _release_contract()
    store = MemoryRunStore()
    run_id = "release-inactive-orphan"
    thread_id = "thread-release-inactive"
    expired = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()
    await _create_auxiliary_row(
        store,
        run_id=run_id,
        thread_id=thread_id,
        kind=ThreadOperationKind.checkpoint_write,
        lease_expires_at=expired,
    )
    assert await store.claim_for_takeover(
        run_id,
        grace_seconds=0,
        error="auxiliary owner was lost",
        stop_reason="orphan_recovered",
    )

    result = await _release_owned(
        store,
        run_id=run_id,
        thread_id=thread_id,
        kind=ThreadOperationKind.checkpoint_write,
    )

    assert isinstance(result, result_type)
    assert result.outcome is outcome_type.inactive
    retained = await store.get(run_id, user_id=_USER)
    assert retained is not None
    assert retained["status"] == "error"
    assert await store.list_lifecycle_events(run_id=run_id) == []


class _ReleaseUnavailableMemoryRunStore(MemoryRunStore):
    """Store whose exact auxiliary release can be restored by the test."""

    def __init__(self) -> None:
        super().__init__()
        self.release_available = False
        self.release_calls: list[tuple[Any, ...]] = []
        self.legacy_release_calls: list[tuple[str, str | None]] = []

    async def release_thread_operation_owned(
        self,
        run_id: str,
        *,
        thread_id: str,
        operation_kind: str,
        user_id: str | None,
        expected_owner_worker_id: str,
        require_unexpired_lease: bool,
    ) -> Any:
        self.release_calls.append(
            (
                run_id,
                thread_id,
                operation_kind,
                user_id,
                expected_owner_worker_id,
                require_unexpired_lease,
            )
        )
        if not self.release_available:
            raise ConnectionError("release store unavailable")
        release = getattr(super(), "release_thread_operation_owned", None)
        assert release is not None, "MemoryRunStore must implement exact owned release"
        return await release(
            run_id,
            thread_id=thread_id,
            operation_kind=operation_kind,
            user_id=user_id,
            expected_owner_worker_id=expected_owner_worker_id,
            require_unexpired_lease=require_unexpired_lease,
        )

    async def delete_thread_operation(
        self,
        run_id: str,
        *,
        user_id: str | None,
    ) -> None:
        self.legacy_release_calls.append((run_id, user_id))
        if not self.release_available:
            raise ConnectionError("legacy release store unavailable")
        await super().delete_thread_operation(run_id, user_id=user_id)


class _RenewalObservedReleaseUnavailableStore(_ReleaseUnavailableMemoryRunStore):
    """Unavailable release store that records or pauses lease renewal."""

    def __init__(self, *, block_renewal: bool = False) -> None:
        super().__init__()
        self.renewal_started = asyncio.Event()
        self.allow_renewal = asyncio.Event()
        self.renewal_calls = 0
        if not block_renewal:
            self.allow_renewal.set()

    async def renew_lease(
        self,
        run_id: str,
        *,
        owner_worker_id: str,
        lease_expires_at: str,
    ) -> Any:
        self.renewal_calls += 1
        self.renewal_started.set()
        await self.allow_renewal.wait()
        return await super().renew_lease(
            run_id,
            owner_worker_id=owner_worker_id,
            lease_expires_at=lease_expires_at,
        )


class _ReleaseResponseLostMemoryRunStore(MemoryRunStore):
    """Delete one exact row, then lose the response once."""

    def __init__(self) -> None:
        super().__init__()
        self.lose_response = True

    async def release_thread_operation_owned(self, *args: Any, **kwargs: Any) -> Any:
        result = await super().release_thread_operation_owned(*args, **kwargs)
        if self.lose_response:
            self.lose_response = False
            raise ConnectionError("release response lost after commit")
        return result


class _MalformedReleaseMemoryRunStore(_ReleaseUnavailableMemoryRunStore):
    """Custom store returning a value outside the exact release contract."""

    def __init__(self, malformed_result: Any) -> None:
        super().__init__()
        self.release_available = True
        self.malformed_result = malformed_result
        self.return_malformed = True

    async def release_thread_operation_owned(self, *args: Any, **kwargs: Any) -> Any:
        if self.return_malformed:
            return self.malformed_result
        return await super().release_thread_operation_owned(*args, **kwargs)


async def _exercise_reserved_operation(
    manager: RunManager,
    *,
    behavior: str,
    kind: ThreadOperationKind = ThreadOperationKind.checkpoint_write,
) -> BaseException | None:
    entered = asyncio.Event()
    block = asyncio.Event()

    async def operation() -> None:
        async with manager.reserve_thread_operation(
            "thread-release-compensation",
            kind=kind,
            user_id=_USER,
        ):
            entered.set()
            if behavior == "error":
                raise LookupError("body failure must survive release failure")
            if behavior == "cancel":
                await block.wait()

    task = asyncio.create_task(operation())
    await asyncio.wait_for(entered.wait(), timeout=1)
    if behavior == "cancel":
        task.cancel()
    try:
        await task
    except BaseException as exc:
        return exc
    return None


async def _cleanup_release_test(
    manager: RunManager,
    store: _ReleaseUnavailableMemoryRunStore,
) -> None:
    store.release_available = True
    if not manager.admission_compensations_ready():
        await manager.drain_admission_compensations(timeout=1)
    compensation = manager._admission_compensation_task
    if compensation is not None and not compensation.done():
        compensation.cancel()
        await asyncio.gather(compensation, return_exceptions=True)
    for row in await store.list_inflight():
        await MemoryRunStore.delete_thread_operation(
            store,
            row["run_id"],
            user_id=row.get("user_id"),
        )


@pytest.mark.anyio
@pytest.mark.parametrize(
    "kind",
    [
        ThreadOperationKind.checkpoint_write,
        ThreadOperationKind.artifact_write,
    ],
)
@pytest.mark.parametrize("behavior", ["success", "error", "cancel"])
async def test_manager_preserves_body_outcome_and_supervises_failed_auxiliary_release(
    behavior: str,
    kind: ThreadOperationKind,
) -> None:
    store = _ReleaseUnavailableMemoryRunStore()
    manager = RunManager(
        store=store,
        worker_id=_OWNER,
        persistence_retry_policy=PersistenceRetryPolicy(
            max_attempts=1,
            initial_delay=0,
        ),
    )
    try:
        outcome = await _exercise_reserved_operation(
            manager,
            behavior=behavior,
            kind=kind,
        )

        if behavior == "success":
            assert outcome is None
        elif behavior == "error":
            assert isinstance(outcome, LookupError)
            assert str(outcome) == "body failure must survive release failure"
        else:
            assert isinstance(outcome, asyncio.CancelledError)

        rows = await store.list_inflight()
        assert len(rows) == 1
        row = rows[0]
        assert row["thread_id"] == "thread-release-compensation"
        assert row["operation_kind"] == kind.value
        assert row["user_id"] == _USER
        assert await store.list_lifecycle_events(run_id=row["run_id"]) == []
        assert manager.admission_compensations_ready() is False
    finally:
        await _cleanup_release_test(manager, store)


@pytest.mark.anyio
async def test_manager_release_outage_blocks_shutdown_then_recovers_exact_row() -> None:
    store = _ReleaseUnavailableMemoryRunStore()
    manager = RunManager(
        store=store,
        worker_id=_OWNER,
        persistence_retry_policy=PersistenceRetryPolicy(
            max_attempts=1,
            initial_delay=0,
        ),
    )
    try:
        assert await _exercise_reserved_operation(manager, behavior="success") is None
        rows = await store.list_inflight()
        assert len(rows) == 1
        run_id = rows[0]["run_id"]

        assert manager.admission_compensations_ready() is False
        assert await manager.shutdown(timeout=0.01) is False

        store.release_available = True
        assert await manager.drain_admission_compensations(timeout=1) is True
        assert await store.get(run_id, user_id=_USER) is None
        assert await store.list_lifecycle_events(run_id=run_id) == []
        assert manager.admission_compensations_ready() is True

        expected_call = (
            run_id,
            "thread-release-compensation",
            ThreadOperationKind.checkpoint_write.value,
            _USER,
            _OWNER,
            False,
        )
        assert store.release_calls
        assert set(store.release_calls) == {expected_call}
        assert store.legacy_release_calls == []
    finally:
        await _cleanup_release_test(manager, store)


@pytest.mark.anyio
@pytest.mark.parametrize(
    "kind",
    [
        ThreadOperationKind.checkpoint_write,
        ThreadOperationKind.artifact_write,
    ],
)
async def test_unresolved_release_handoff_detaches_the_completed_body_task(
    kind: ThreadOperationKind,
) -> None:
    """The shared supervisor, not an unrelated caller continuation, owns release."""

    store = _ReleaseUnavailableMemoryRunStore()
    manager = RunManager(
        store=store,
        worker_id=_OWNER,
        persistence_retry_policy=PersistenceRetryPolicy(
            max_attempts=1,
            initial_delay=0,
        ),
    )
    body_exited = asyncio.Event()
    finish_caller = asyncio.Event()

    async def operation() -> None:
        async with manager.reserve_thread_operation(
            f"thread-detached-{kind.value}",
            kind=kind,
            user_id=_USER,
        ):
            pass
        body_exited.set()
        await finish_caller.wait()

    task = asyncio.create_task(operation())
    try:
        await asyncio.wait_for(body_exited.wait(), timeout=1)
        rows = await store.list_inflight()
        assert len(rows) == 1
        local = await manager.get(rows[0]["run_id"], user_id=_USER)
        assert local is not None
        assert local.task is None
        assert not task.done()
        assert manager.admission_compensations_ready() is False
    finally:
        finish_caller.set()
        await asyncio.gather(task, return_exceptions=True)
        await _cleanup_release_test(manager, store)


@pytest.mark.anyio
async def test_unresolved_release_does_not_renew_after_the_body_handoff() -> None:
    store = _RenewalObservedReleaseUnavailableStore()
    manager = RunManager(
        store=store,
        worker_id=_OWNER,
        run_ownership_config=RunOwnershipConfig(
            lease_seconds=30,
            grace_seconds=0,
            heartbeat_enabled=True,
        ),
        persistence_retry_policy=PersistenceRetryPolicy(
            max_attempts=1,
            initial_delay=0,
        ),
    )
    body_exited = asyncio.Event()
    finish_caller = asyncio.Event()

    async def operation() -> None:
        async with manager.reserve_thread_operation(
            "thread-no-post-body-renewal",
            kind=ThreadOperationKind.checkpoint_write,
            user_id=_USER,
        ):
            pass
        body_exited.set()
        await finish_caller.wait()

    task = asyncio.create_task(operation())
    try:
        await asyncio.wait_for(body_exited.wait(), timeout=1)
        await manager._renew_leases()
        assert store.renewal_calls == 0
    finally:
        finish_caller.set()
        await asyncio.gather(task, return_exceptions=True)
        await _cleanup_release_test(manager, store)


@pytest.mark.anyio
async def test_reservation_caller_can_request_shutdown_after_release_handoff() -> None:
    """Shutdown must not cancel or await the task whose reservation body ended."""

    store = _ReleaseUnavailableMemoryRunStore()
    manager = RunManager(
        store=store,
        worker_id=_OWNER,
        persistence_retry_policy=PersistenceRetryPolicy(
            max_attempts=1,
            initial_delay=0,
        ),
    )

    async def operation() -> bool:
        async with manager.reserve_thread_operation(
            "thread-same-task-shutdown",
            kind=ThreadOperationKind.artifact_write,
            user_id=_USER,
        ):
            pass
        return await manager.shutdown(timeout=0.01)

    task = asyncio.create_task(operation())
    try:
        assert await task is False
        assert not task.cancelled()
    finally:
        await _cleanup_release_test(manager, store)


@pytest.mark.anyio
async def test_renewal_response_after_release_handoff_does_not_refresh_local_lease() -> None:
    """A renewal started by the body task loses local authority at handoff."""

    store = _RenewalObservedReleaseUnavailableStore(block_renewal=True)
    manager = RunManager(
        store=store,
        worker_id=_OWNER,
        run_ownership_config=RunOwnershipConfig(
            lease_seconds=30,
            grace_seconds=0,
            heartbeat_enabled=True,
        ),
        persistence_retry_policy=PersistenceRetryPolicy(
            max_attempts=1,
            initial_delay=0,
        ),
    )
    body_entered = asyncio.Event()
    leave_body = asyncio.Event()
    body_exited = asyncio.Event()
    finish_caller = asyncio.Event()

    async def operation() -> None:
        async with manager.reserve_thread_operation(
            "thread-renewal-handoff-race",
            kind=ThreadOperationKind.checkpoint_write,
            user_id=_USER,
        ):
            body_entered.set()
            await leave_body.wait()
        body_exited.set()
        await finish_caller.wait()

    task = asyncio.create_task(operation())
    renewal_task: asyncio.Task[None] | None = None
    try:
        await asyncio.wait_for(body_entered.wait(), timeout=1)
        rows = await store.list_inflight()
        assert len(rows) == 1
        run_id = rows[0]["run_id"]
        local = await manager.get(run_id, user_id=_USER)
        assert local is not None
        original_expiry = local.lease_expires_at

        renewal_task = asyncio.create_task(manager._renew_leases())
        await asyncio.wait_for(store.renewal_started.wait(), timeout=1)
        leave_body.set()
        await asyncio.wait_for(body_exited.wait(), timeout=1)
        store.allow_renewal.set()
        await renewal_task

        assert local.task is None
        assert local.lease_expires_at == original_expiry
        assert manager.admission_compensations_ready() is False
    finally:
        store.allow_renewal.set()
        leave_body.set()
        finish_caller.set()
        if renewal_task is not None:
            await asyncio.gather(renewal_task, return_exceptions=True)
        await asyncio.gather(task, return_exceptions=True)
        await _cleanup_release_test(manager, store)


@pytest.mark.anyio
@pytest.mark.parametrize(
    "kind",
    [
        ThreadOperationKind.checkpoint_write,
        ThreadOperationKind.artifact_write,
    ],
)
async def test_release_response_loss_converges_on_absence_without_restart(
    kind: ThreadOperationKind,
) -> None:
    store = _ReleaseResponseLostMemoryRunStore()
    manager = RunManager(
        store=store,
        worker_id=_OWNER,
        persistence_retry_policy=PersistenceRetryPolicy(
            max_attempts=1,
            initial_delay=0,
        ),
    )

    assert (
        await _exercise_reserved_operation(
            manager,
            behavior="success",
            kind=kind,
        )
        is None
    )
    assert await manager.drain_admission_compensations(timeout=1) is True
    assert await store.list_inflight() == []
    assert manager.admission_compensations_ready() is True


@pytest.mark.anyio
@pytest.mark.parametrize("malformed_result", [None, object()])
@pytest.mark.parametrize("behavior", ["success", "error", "cancel"])
async def test_malformed_custom_release_result_preserves_body_and_stays_supervised(
    malformed_result: Any,
    behavior: str,
) -> None:
    """Malformed persistence output cannot escape cleanup or erase body outcome."""

    store = _MalformedReleaseMemoryRunStore(malformed_result)
    manager = RunManager(
        store=store,
        worker_id=_OWNER,
        persistence_retry_policy=PersistenceRetryPolicy(
            max_attempts=1,
            initial_delay=0,
        ),
    )
    try:
        outcome = await _exercise_reserved_operation(manager, behavior=behavior)

        if behavior == "success":
            assert outcome is None
        elif behavior == "error":
            assert isinstance(outcome, LookupError)
            assert str(outcome) == "body failure must survive release failure"
        else:
            assert isinstance(outcome, asyncio.CancelledError)

        assert manager.admission_compensations_ready() is False
        assert await manager.shutdown(timeout=0.01) is False
        task = manager._admission_compensation_task
        assert task is not None
        assert not task.done() or task.exception() is None
    finally:
        store.return_malformed = False
        await _cleanup_release_test(manager, store)


@pytest.mark.anyio
async def test_expired_auxiliary_release_waits_for_orphan_inactive_wakeup() -> None:
    outcome_type, _ = _release_contract()
    store = _ReleaseUnavailableMemoryRunStore()
    store.release_available = True
    manager = RunManager(
        store=store,
        worker_id=_OWNER,
        run_ownership_config=RunOwnershipConfig(
            lease_seconds=30,
            grace_seconds=0,
            heartbeat_enabled=True,
        ),
        persistence_retry_policy=PersistenceRetryPolicy(
            max_attempts=1,
            initial_delay=0,
        ),
    )
    entered = asyncio.Event()
    release_body = asyncio.Event()

    async def operation() -> None:
        async with manager.reserve_thread_operation(
            "thread-expired-release",
            kind=ThreadOperationKind.artifact_write,
            user_id=_USER,
        ):
            entered.set()
            await release_body.wait()

    task = asyncio.create_task(operation())
    try:
        await asyncio.wait_for(entered.wait(), timeout=1)
        rows = await store.list_inflight()
        assert len(rows) == 1
        run_id = rows[0]["run_id"]
        expired = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()
        rows[0]["lease_expires_at"] = expired
        store._runs[run_id]["lease_expires_at"] = expired
        local = await manager.get(run_id, user_id=_USER)
        assert local is not None
        local.lease_expires_at = expired

        release_body.set()
        await task

        assert store.release_calls
        assert store.release_calls[-1][-1] is True
        assert manager.admission_compensations_ready() is False

        recovered = await manager.reconcile_orphaned_inflight_runs(
            error="auxiliary reservation owner was lost",
            stop_reason="orphan_recovered",
        )
        assert recovered == []
        assert await manager.drain_admission_compensations(timeout=1) is True
        retained = await store.get(run_id, user_id=_USER)
        assert retained is not None
        assert retained["status"] == "error"
        assert await store.list_lifecycle_events(run_id=run_id) == []
        assert store.release_calls[-1]
        final = await _release_owned(
            store,
            run_id=run_id,
            thread_id="thread-expired-release",
            kind=ThreadOperationKind.artifact_write,
            require_unexpired_lease=False,
        )
        assert final.outcome is outcome_type.inactive
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await _cleanup_release_test(manager, store)
