"""Regressions for post-commit admission and finalizer ownership."""

import asyncio
import uuid
from collections.abc import Coroutine
from typing import Any

import pytest

from deerflow.runtime import RunManager, RunStatus
from deerflow.runtime.runs.manager import (
    PersistenceRetryPolicy,
    RunStartOutcome,
    RunStartupError,
)
from deerflow.runtime.runs.store.memory import MemoryRunStore


class LostReplacementResponseStore(MemoryRunStore):
    """Commit one replacement, then hide its exact row until recovery."""

    durable_lifecycle = True

    def __init__(self) -> None:
        super().__init__()
        self.lose_next_create_response = False
        self.candidate_reads_available = True

    async def create_thread_operation_atomic(self, run_id: str, **kwargs):
        result = await super().create_thread_operation_atomic(run_id, **kwargs)
        if self.lose_next_create_response:
            self.lose_next_create_response = False
            raise OSError("response lost after durable commit")
        return result

    async def get(self, run_id: str, *, user_id: str | None = None):
        if not self.candidate_reads_available:
            raise OSError("candidate lookup unavailable")
        return await super().get(run_id, user_id=user_id)

    async def authoritative_get(self, run_id: str):
        """Read the backing row without the injected availability failure."""

        return await super().get(run_id)


class CountingAtomicRunStore(MemoryRunStore):
    """Count atomic admissions without changing their behavior."""

    durable_lifecycle = True

    def __init__(self) -> None:
        super().__init__()
        self.atomic_admission_calls = 0

    async def create_thread_operation_atomic(self, run_id: str, **kwargs):
        self.atomic_admission_calls += 1
        return await super().create_thread_operation_atomic(run_id, **kwargs)


class HistoricalReadFailureStore(LostReplacementResponseStore):
    """Fail reads only for predecessors after exact candidate proof succeeds."""

    def __init__(self) -> None:
        super().__init__()
        self.failed_historical_ids: set[str] = set()
        self.historical_read_attempts: list[str] = []

    async def get(self, run_id: str, *, user_id: str | None = None):
        if run_id in self.failed_historical_ids:
            self.historical_read_attempts.append(run_id)
            raise OSError("historical finalizer lookup unavailable")
        return await super().get(run_id, user_id=user_id)


def _candidate_id(index: int) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"hartmesh-post-commit-{index}"))


async def _attach_cancellation_resistant_worker(
    manager: RunManager,
    run_id: str,
    *,
    release: asyncio.Event,
    tasks: list[asyncio.Task[None]],
) -> None:
    async def worker() -> None:
        while not release.is_set():
            try:
                await release.wait()
            except asyncio.CancelledError:
                continue

    worker_coroutine: Coroutine[Any, Any, None] = worker()
    task = await manager.attach_worker_once(
        run_id,
        worker_coroutine,
        asyncio.create_task,
    )
    tasks.append(task)
    assert await manager.try_start(run_id) is RunStartOutcome.started


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("strategy", "keyed"),
    [
        pytest.param("interrupt", False, id="unkeyed-interrupt"),
        pytest.param("rollback", True, id="keyed-rollback"),
    ],
)
async def test_many_finalizers_do_not_overflow_lost_replacement_compensation(
    strategy: str,
    keyed: bool,
) -> None:
    """Historical finalizers cannot prevent retaining the one new replacement."""

    store = LostReplacementResponseStore()
    manager = RunManager(
        store=store,
        persistence_retry_policy=PersistenceRetryPolicy(
            max_attempts=1,
            initial_delay=0,
        ),
    )
    release_workers = asyncio.Event()
    worker_tasks: list[asyncio.Task[None]] = []
    thread_id = f"thread-many-finalizers-{strategy}"

    try:
        current = await manager.create_or_reject(
            thread_id,
            candidate_run_id=_candidate_id(0),
        )
        await _attach_cancellation_resistant_worker(
            manager,
            current.run_id,
            release=release_workers,
            tasks=worker_tasks,
        )

        # Seventeen replacements leave seventeen already-fenced finalizers and
        # one genuinely actionable current predecessor in local history.
        for index in range(1, 18):
            current = await manager.create_or_reject(
                thread_id,
                candidate_run_id=_candidate_id(index),
                multitask_strategy=strategy,
            )
            await _attach_cancellation_resistant_worker(
                manager,
                current.run_id,
                release=release_workers,
                tasks=worker_tasks,
            )

        candidate_run_id = _candidate_id(18)
        store.lose_next_create_response = True
        store.candidate_reads_available = False

        if keyed:
            admission = manager.ensure_or_reject(
                thread_id,
                candidate_run_id=candidate_run_id,
                external_scope="scope-rapid-replacement",
                external_key="delivery-rapid-replacement",
                request_digest="a" * 64,
                request_digest_version="request-v1",
                caller_intent_json={"message": "replacement"},
                caller_intent_digest="b" * 64,
                caller_intent_digest_version="intent-v1",
                multitask_strategy=strategy,
            )
        else:
            admission = manager.create_or_reject(
                thread_id,
                candidate_run_id=candidate_run_id,
                multitask_strategy=strategy,
            )
        with pytest.raises(OSError, match="response lost after durable commit"):
            await admission

        retained = await store.authoritative_get(candidate_run_id)
        assert retained is not None
        assert retained["status"] == RunStatus.pending.value
        assert manager.admission_compensations_ready() is False
        assert await manager.shutdown(timeout=0.01) is False

        store.candidate_reads_available = True
        assert await manager.drain_admission_compensations(timeout=1) is True
        retained = await store.authoritative_get(candidate_run_id)
        assert retained is not None
        assert retained["status"] == RunStatus.error.value
        assert retained["stop_reason"] == "worker_attachment_failed"
    finally:
        store.candidate_reads_available = True
        release_workers.set()
        await asyncio.gather(*worker_tasks, return_exceptions=True)
        if not manager.admission_compensations_ready():
            await manager.drain_admission_compensations(timeout=1)


@pytest.mark.anyio
async def test_lost_replacement_retains_one_actionable_predecessor_until_exact_read() -> None:
    """The current predecessor stays live until the candidate commit is proven."""

    store = LostReplacementResponseStore()
    manager = RunManager(
        store=store,
        persistence_retry_policy=PersistenceRetryPolicy(
            max_attempts=1,
            initial_delay=0,
        ),
    )
    release_worker = asyncio.Event()
    worker_tasks: list[asyncio.Task[None]] = []
    predecessor = await manager.create_or_reject(
        "thread-single-predecessor",
        candidate_run_id=_candidate_id(30),
    )
    await _attach_cancellation_resistant_worker(
        manager,
        predecessor.run_id,
        release=release_worker,
        tasks=worker_tasks,
    )
    candidate_run_id = _candidate_id(31)
    store.lose_next_create_response = True
    store.candidate_reads_available = False

    try:
        with pytest.raises(OSError, match="response lost after durable commit"):
            await manager.create_or_reject(
                "thread-single-predecessor",
                candidate_run_id=candidate_run_id,
                multitask_strategy="interrupt",
            )

        assert predecessor.abort_event.is_set() is False
        assert manager.admission_compensations_ready() is False
        assert await manager.shutdown(timeout=0.01) is False

        store.candidate_reads_available = True
        assert await manager.drain_admission_compensations(timeout=1) is True
        assert predecessor.abort_event.is_set()
        retained = await store.authoritative_get(candidate_run_id)
        assert retained is not None
        assert retained["status"] == RunStatus.error.value
    finally:
        store.candidate_reads_available = True
        release_worker.set()
        await asyncio.gather(*worker_tasks, return_exceptions=True)
        if not manager.admission_compensations_ready():
            await manager.drain_admission_compensations(timeout=1)


@pytest.mark.anyio
@pytest.mark.parametrize("strategy", ["interrupt", "rollback"])
@pytest.mark.parametrize("keyed", [False, True])
async def test_exact_candidate_proof_does_not_depend_on_historical_finalizer_reads(
    strategy: str,
    keyed: bool,
) -> None:
    """A proven creator attaches despite unrelated historical-store outage."""

    store = HistoricalReadFailureStore()
    manager = RunManager(
        store=store,
        persistence_retry_policy=PersistenceRetryPolicy(
            max_attempts=1,
            initial_delay=0,
        ),
    )
    release_workers = asyncio.Event()
    worker_tasks: list[asyncio.Task[None]] = []
    thread_id = f"thread-proven-candidate-{strategy}-{keyed}"
    first = await manager.create_or_reject(
        thread_id,
        candidate_run_id=_candidate_id(60),
    )
    await _attach_cancellation_resistant_worker(
        manager,
        first.run_id,
        release=release_workers,
        tasks=worker_tasks,
    )
    current = await manager.create_or_reject(
        thread_id,
        candidate_run_id=_candidate_id(61),
        multitask_strategy="interrupt",
    )
    await _attach_cancellation_resistant_worker(
        manager,
        current.run_id,
        release=release_workers,
        tasks=worker_tasks,
    )

    candidate_run_id = _candidate_id(62)
    store.failed_historical_ids = {first.run_id}
    store.lose_next_create_response = True
    if keyed:
        admission = manager.ensure_or_reject(
            thread_id,
            candidate_run_id=candidate_run_id,
            external_scope="scope-proven-candidate",
            external_key=f"delivery-{strategy}",
            request_digest="c" * 64,
            request_digest_version="request-v1",
            caller_intent_json={"message": "proven replacement"},
            caller_intent_digest="d" * 64,
            caller_intent_digest_version="intent-v1",
            multitask_strategy=strategy,
        )
    else:
        admission = manager.create_or_reject(
            thread_id,
            candidate_run_id=candidate_run_id,
            multitask_strategy=strategy,
        )

    try:
        admitted = await admission
        record = admitted.record if hasattr(admitted, "record") else admitted
        assert record.run_id == candidate_run_id
        assert record.attachment_supervised is True
        assert store.historical_read_attempts == []
        await _attach_cancellation_resistant_worker(
            manager,
            record.run_id,
            release=release_workers,
            tasks=worker_tasks,
        )
        assert record.task is worker_tasks[-1]
        assert manager.admission_compensations_ready() is True
    finally:
        store.failed_historical_ids.clear()
        release_workers.set()
        await asyncio.gather(*worker_tasks, return_exceptions=True)
        if not manager.admission_compensations_ready():
            await manager.drain_admission_compensations(timeout=1)


@pytest.mark.anyio
async def test_multiple_actionable_predecessors_fail_before_store_admission() -> None:
    """Corrupt local one-active-run state is rejected before durable mutation."""

    store = CountingAtomicRunStore()
    manager = RunManager(store=store)
    first = await manager.create("thread-multiple-actionable")
    second = await manager.create("thread-multiple-actionable")
    assert await manager.try_start(first.run_id) is RunStartOutcome.started
    assert await manager.try_start(second.run_id) is RunStartOutcome.started

    with pytest.raises(RunStartupError):
        await manager.create_or_reject(
            "thread-multiple-actionable",
            candidate_run_id=_candidate_id(40),
            multitask_strategy="interrupt",
        )

    assert store.atomic_admission_calls == 0


@pytest.mark.anyio
async def test_shutdown_waits_for_terminal_finalizer_task() -> None:
    """Shutdown cannot report quiescence while post-cancel cleanup can write."""

    manager = RunManager(store=MemoryRunStore())
    record = await manager.create_or_reject(
        "thread-terminal-finalizer",
        candidate_run_id=_candidate_id(50),
    )
    worker_started = asyncio.Event()
    finalizer_started = asyncio.Event()
    release_finalizer = asyncio.Event()

    async def worker() -> None:
        worker_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            finalizer_started.set()
            while not release_finalizer.is_set():
                try:
                    await release_finalizer.wait()
                except asyncio.CancelledError:
                    continue
        finally:
            await manager.set_finalizing(record.run_id, False)

    task = await manager.attach_worker_once(
        record.run_id,
        worker(),
        asyncio.create_task,
    )
    assert await manager.try_start(record.run_id) is RunStartOutcome.started
    await asyncio.wait_for(worker_started.wait(), timeout=1)
    assert (await manager.cancel(record.run_id, action="interrupt")).value == "cancelled"
    await asyncio.wait_for(finalizer_started.wait(), timeout=1)

    try:
        assert await manager.shutdown(timeout=0.01) is False
        assert task.done() is False
    finally:
        release_finalizer.set()
        await asyncio.wait_for(task, timeout=1)
    assert await manager.shutdown(timeout=1) is True


@pytest.mark.anyio
async def test_shutdown_does_not_cancel_running_status_finalizer() -> None:
    """The explicit finalizing fence wins before terminal status is persisted."""

    manager = RunManager(store=MemoryRunStore())
    record = await manager.create_or_reject(
        "thread-running-finalizer",
        candidate_run_id=_candidate_id(51),
    )
    finalizer_started = asyncio.Event()
    cancellation_received = asyncio.Event()
    release_finalizer = asyncio.Event()

    async def worker() -> None:
        await manager.set_finalizing(record.run_id, True)
        finalizer_started.set()
        while not release_finalizer.is_set():
            try:
                await release_finalizer.wait()
            except asyncio.CancelledError:
                cancellation_received.set()
        await manager.set_finalizing(record.run_id, False)

    task = await manager.attach_worker_once(
        record.run_id,
        worker(),
        asyncio.create_task,
    )
    assert await manager.try_start(record.run_id) is RunStartOutcome.started
    await asyncio.wait_for(finalizer_started.wait(), timeout=1)
    assert record.status is RunStatus.running
    assert record.finalizing is True

    try:
        assert await manager.shutdown(timeout=0.01) is False
        assert cancellation_received.is_set() is False
        assert record.status is RunStatus.running
        assert task.done() is False
    finally:
        release_finalizer.set()
        await asyncio.wait_for(task, timeout=1)
    assert await manager.shutdown(timeout=1) is True
