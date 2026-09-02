"""Authority-fenced projections from durable runs into thread metadata."""

from __future__ import annotations

import asyncio
import os
import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from langgraph.store.memory import InMemoryStore
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from support.postgres import postgres_async_url

from deerflow.persistence.base import Base
from deerflow.persistence.run import RunRepository
from deerflow.persistence.run.model import RunRow
from deerflow.persistence.thread_meta import (
    MemoryThreadMetaStore,
    ThreadMetaRepository,
    ThreadMetaRunProjection,
)
from deerflow.runtime.runs.store.base import LifecycleTransition, LifecycleType, RunStore
from deerflow.runtime.runs.store.memory import MemoryRunStore
from deerflow.runtime.runs.worker import RunContext, run_agent

_POSTGRES_URL = os.environ.get("DEERFLOW_TEST_POSTGRES_URL")


class _PauseAfterNextThreadReadStore(InMemoryStore):
    """Pause one thread read after capturing its current value."""

    def __init__(self) -> None:
        super().__init__()
        self.pause_next_thread_read = False
        self.thread_read_captured = asyncio.Event()
        self.allow_thread_read = asyncio.Event()

    async def aget(self, namespace, key, **kwargs):
        item = await super().aget(namespace, key, **kwargs)
        if self.pause_next_thread_read and namespace == ("threads",):
            self.pause_next_thread_read = False
            self.thread_read_captured.set()
            await self.allow_thread_read.wait()
        return item


async def _admit_and_start(
    store: RunStore,
    *,
    run_id: str,
    thread_id: str,
    owner: str,
    created_at: str | None = None,
    lease_expires_at: str | None = None,
) -> dict:
    admitted, _ = await store.create_thread_operation_atomic(
        run_id,
        thread_id=thread_id,
        owner_worker_id=owner,
        lease_expires_at=lease_expires_at,
        created_at=created_at,
    )
    started = await store.transition_owned_run_atomic(
        run_id,
        expected_state_version=admitted["state_version"],
        expected_statuses=("pending",),
        transition=LifecycleTransition(
            lifecycle_type=LifecycleType.started,
            status="running",
        ),
        expected_owner_worker_id=owner,
        require_unexpired_lease=False,
    )
    assert started.applied
    assert started.row is not None
    return dict(started.row)


@pytest.fixture
async def sqlite_stores(tmp_path):
    from deerflow.persistence.engine import close_engine, get_session_factory, init_engine

    await init_engine(
        "sqlite",
        url=f"sqlite+aiosqlite:///{tmp_path / 'projection.db'}",
        sqlite_dir=str(tmp_path),
    )
    factory = get_session_factory()
    yield RunRepository(factory), ThreadMetaRepository(factory)
    await close_engine()


@pytest.mark.anyio
async def test_memory_projection_rejects_an_older_run_after_a_newer_admission() -> None:
    """The latest admitted normal run exclusively owns title/status projection."""

    runs = MemoryRunStore()
    threads = MemoryThreadMetaStore(InMemoryStore(), run_store=runs)
    await threads.create(
        "thread-1",
        display_name="Human title",
        user_id="test-user",
    )

    same_created_at = "2026-09-01T12:00:00+00:00"
    older = await _admit_and_start(
        runs,
        run_id="run-z-old",
        thread_id="thread-1",
        owner="worker-old",
        created_at=same_created_at,
    )
    completed = await runs.transition_owned_run_atomic(
        "run-z-old",
        expected_state_version=older["state_version"],
        expected_statuses=("running",),
        transition=LifecycleTransition(
            lifecycle_type=LifecycleType.succeeded,
            status="success",
        ),
        expected_owner_worker_id="worker-old",
        require_unexpired_lease=False,
    )
    assert completed.applied
    assert completed.row is not None

    await runs.create_thread_operation_atomic(
        "run-a-new",
        thread_id="thread-1",
        owner_worker_id="worker-new",
        lease_expires_at=None,
        created_at=same_created_at,
    )

    applied = await threads.project_run(
        ThreadMetaRunProjection(
            run_id="run-z-old",
            thread_id="thread-1",
            owner_worker_id="worker-old",
            active_state_version=older["state_version"],
            terminal_state_version=completed.row["state_version"],
            status="idle",
            display_name="Stale generated title",
        ),
        user_id="test-user",
    )

    assert applied is False
    current = await threads.get("thread-1", user_id="test-user")
    assert current is not None
    assert current["display_name"] == "Human title"
    assert current["status"] == "idle"


@pytest.mark.anyio
async def test_memory_projection_holds_authority_through_metadata_write() -> None:
    """Admission cannot interleave after the authority check but before write."""

    class PausingStore(InMemoryStore):
        def __init__(self) -> None:
            super().__init__()
            self.pause_thread_read = False
            self.thread_read_started = asyncio.Event()
            self.allow_thread_read = asyncio.Event()

        async def aget(self, namespace, key, **kwargs):
            if self.pause_thread_read and namespace == ("threads",):
                self.thread_read_started.set()
                await self.allow_thread_read.wait()
            return await super().aget(namespace, key, **kwargs)

    runs = MemoryRunStore()
    backing_store = PausingStore()
    threads = MemoryThreadMetaStore(backing_store, run_store=runs)
    await threads.create(
        "thread-1",
        display_name="Human title",
        user_id="test-user",
    )
    running = await _admit_and_start(
        runs,
        run_id="run-old",
        thread_id="thread-1",
        owner="worker-old",
    )
    completed = await runs.transition_owned_run_atomic(
        "run-old",
        expected_state_version=running["state_version"],
        expected_statuses=("running",),
        transition=LifecycleTransition(
            lifecycle_type=LifecycleType.succeeded,
            status="success",
        ),
        expected_owner_worker_id="worker-old",
        require_unexpired_lease=False,
    )
    assert completed.applied
    assert completed.row is not None
    projection = ThreadMetaRunProjection(
        run_id="run-old",
        thread_id="thread-1",
        owner_worker_id="worker-old",
        active_state_version=running["state_version"],
        terminal_state_version=completed.row["state_version"],
        status="idle",
        display_name="Generated title",
    )

    backing_store.pause_thread_read = True
    projection_task = asyncio.create_task(threads.project_run(projection, user_id="test-user"))
    await asyncio.wait_for(backing_store.thread_read_started.wait(), timeout=1)
    admission_task = asyncio.create_task(
        runs.create_thread_operation_atomic(
            "run-new",
            thread_id="thread-1",
            owner_worker_id="worker-new",
            lease_expires_at=None,
        )
    )
    await asyncio.sleep(0)

    try:
        assert admission_task.done() is False
    finally:
        backing_store.allow_thread_read.set()

    assert await projection_task is True
    await admission_task
    assert await threads.project_run(projection, user_id="test-user") is False


@pytest.mark.anyio
async def test_memory_projection_and_delete_serialize_without_resurrecting_thread() -> None:
    """A projection holding an old snapshot cannot recreate a deleted thread."""

    runs = MemoryRunStore()
    backing_store = _PauseAfterNextThreadReadStore()
    threads = MemoryThreadMetaStore(backing_store, run_store=runs)
    await threads.create(
        "thread-1",
        display_name="Human title",
        user_id="test-user",
    )
    running = await _admit_and_start(
        runs,
        run_id="run-1",
        thread_id="thread-1",
        owner="worker-1",
    )
    projection = ThreadMetaRunProjection(
        run_id="run-1",
        thread_id="thread-1",
        owner_worker_id="worker-1",
        active_state_version=running["state_version"],
        status="running",
        display_name="Generated title",
    )

    backing_store.pause_next_thread_read = True
    projection_task = asyncio.create_task(threads.project_run(projection, user_id="test-user"))
    await asyncio.wait_for(backing_store.thread_read_captured.wait(), timeout=1)
    delete_task = asyncio.create_task(threads.delete("thread-1", user_id="test-user"))
    await asyncio.sleep(0)
    delete_completed_while_projection_was_paused = delete_task.done()

    backing_store.allow_thread_read.set()
    assert await projection_task is True
    await delete_task

    assert delete_completed_while_projection_was_paused is False
    assert await threads.get("thread-1", user_id="test-user") is None


@pytest.mark.anyio
async def test_memory_projection_and_metadata_update_preserve_disjoint_fields() -> None:
    """Projection and metadata updates retain both callers' changes."""

    runs = MemoryRunStore()
    backing_store = _PauseAfterNextThreadReadStore()
    threads = MemoryThreadMetaStore(backing_store, run_store=runs)
    await threads.create(
        "thread-1",
        display_name="Human title",
        metadata={"retained": "original"},
        user_id="test-user",
    )
    running = await _admit_and_start(
        runs,
        run_id="run-1",
        thread_id="thread-1",
        owner="worker-1",
    )
    projection = ThreadMetaRunProjection(
        run_id="run-1",
        thread_id="thread-1",
        owner_worker_id="worker-1",
        active_state_version=running["state_version"],
        status="running",
        display_name="Generated title",
    )

    backing_store.pause_next_thread_read = True
    projection_task = asyncio.create_task(threads.project_run(projection, user_id="test-user"))
    await asyncio.wait_for(backing_store.thread_read_captured.wait(), timeout=1)
    metadata_task = asyncio.create_task(
        threads.update_metadata(
            "thread-1",
            {"admin": "preserved"},
            user_id="test-user",
        )
    )
    await asyncio.sleep(0)
    metadata_completed_while_projection_was_paused = metadata_task.done()

    backing_store.allow_thread_read.set()
    assert await projection_task is True
    await metadata_task

    assert metadata_completed_while_projection_was_paused is False
    current = await threads.get("thread-1", user_id="test-user")
    assert current is not None
    assert current["display_name"] == "Generated title"
    assert current["status"] == "running"
    assert current["metadata"] == {
        "admin": "preserved",
        "retained": "original",
    }


@pytest.mark.anyio
async def test_sql_projection_rejects_an_older_run_after_a_newer_admission(
    sqlite_stores,
) -> None:
    runs, threads = sqlite_stores
    await threads.create(
        "thread-1",
        display_name="Human title",
        user_id="test-user",
    )
    older = await _admit_and_start(
        runs,
        run_id="run-z-old",
        thread_id="thread-1",
        owner="worker-old",
        created_at="2026-09-01T12:00:00+00:00",
    )
    completed = await runs.transition_owned_run_atomic(
        "run-z-old",
        expected_state_version=older["state_version"],
        expected_statuses=("running",),
        transition=LifecycleTransition(
            lifecycle_type=LifecycleType.succeeded,
            status="success",
        ),
        expected_owner_worker_id="worker-old",
        require_unexpired_lease=False,
    )
    assert completed.applied
    assert completed.row is not None
    await runs.create_thread_operation_atomic(
        "run-a-new",
        thread_id="thread-1",
        owner_worker_id="worker-new",
        lease_expires_at=None,
        created_at="2026-09-01T12:00:00+00:00",
    )

    applied = await threads.project_run(
        ThreadMetaRunProjection(
            run_id="run-z-old",
            thread_id="thread-1",
            owner_worker_id="worker-old",
            active_state_version=older["state_version"],
            terminal_state_version=completed.row["state_version"],
            status="idle",
            display_name="Stale generated title",
        ),
        user_id="test-user",
    )

    assert applied is False
    current = await threads.get("thread-1", user_id="test-user")
    assert current is not None
    assert current["display_name"] == "Human title"
    assert current["status"] == "idle"


@pytest.mark.anyio
async def test_memory_running_projection_requires_the_exact_active_owner_epoch() -> None:
    runs = MemoryRunStore()
    threads = MemoryThreadMetaStore(InMemoryStore(), run_store=runs)
    await threads.create(
        "thread-1",
        display_name="Human title",
        user_id="test-user",
    )
    running = await _admit_and_start(
        runs,
        run_id="run-1",
        thread_id="thread-1",
        owner="worker-1",
    )

    stale = ThreadMetaRunProjection(
        run_id="run-1",
        thread_id="thread-1",
        owner_worker_id="worker-stale",
        active_state_version=running["state_version"],
        status="running",
    )
    assert await threads.project_run(stale, user_id="test-user") is False

    authoritative = ThreadMetaRunProjection(
        run_id="run-1",
        thread_id="thread-1",
        owner_worker_id="worker-1",
        active_state_version=running["state_version"],
        status="running",
    )
    assert await threads.project_run(authoritative, user_id="test-user") is True
    current = await threads.get("thread-1", user_id="test-user")
    assert current is not None
    assert current["display_name"] == "Human title"
    assert current["status"] == "running"


@pytest.mark.anyio
async def test_sql_running_projection_requires_the_exact_active_owner_epoch(
    sqlite_stores,
) -> None:
    runs, threads = sqlite_stores
    await threads.create(
        "thread-1",
        display_name="Human title",
        user_id="test-user",
    )
    running = await _admit_and_start(
        runs,
        run_id="run-1",
        thread_id="thread-1",
        owner="worker-1",
    )

    stale = ThreadMetaRunProjection(
        run_id="run-1",
        thread_id="thread-1",
        owner_worker_id="worker-1",
        active_state_version=running["state_version"] - 1,
        status="running",
    )
    assert await threads.project_run(stale, user_id="test-user") is False

    authoritative = ThreadMetaRunProjection(
        run_id="run-1",
        thread_id="thread-1",
        owner_worker_id="worker-1",
        active_state_version=running["state_version"],
        status="running",
    )
    assert await threads.project_run(authoritative, user_id="test-user") is True
    current = await threads.get("thread-1", user_id="test-user")
    assert current is not None
    assert current["display_name"] == "Human title"
    assert current["status"] == "running"


async def _running_projection_rejects_an_expired_lease(
    runs: RunStore,
    threads,
) -> None:
    await threads.create("thread-1", user_id="test-user")
    running = await _admit_and_start(
        runs,
        run_id="run-1",
        thread_id="thread-1",
        owner="worker-1",
        lease_expires_at="2020-01-01T00:00:00+00:00",
    )
    projection = ThreadMetaRunProjection(
        run_id="run-1",
        thread_id="thread-1",
        owner_worker_id="worker-1",
        active_state_version=running["state_version"],
        status="running",
    )

    assert (
        await runs.thread_projection_authorized(
            run_id="run-1",
            thread_id="thread-1",
            run_status="running",
            owner_worker_id="worker-1",
            active_state_version=running["state_version"],
        )
        is False
    )
    assert await threads.project_run(projection, user_id="test-user") is False
    current = await threads.get("thread-1", user_id="test-user")
    assert current is not None
    assert current["status"] == "idle"


@pytest.mark.anyio
async def test_memory_running_projection_rejects_an_expired_lease() -> None:
    runs = MemoryRunStore()
    threads = MemoryThreadMetaStore(InMemoryStore(), run_store=runs)
    await _running_projection_rejects_an_expired_lease(runs, threads)


@pytest.mark.anyio
async def test_sql_running_projection_rejects_an_expired_lease(
    sqlite_stores,
) -> None:
    await _running_projection_rejects_an_expired_lease(*sqlite_stores)


async def _terminal_projection_updates_title_and_status_together(
    runs: RunStore,
    threads,
) -> None:
    await threads.create(
        "thread-1",
        display_name="Human title",
        user_id="test-user",
    )
    running = await _admit_and_start(
        runs,
        run_id="run-1",
        thread_id="thread-1",
        owner="worker-1",
    )
    assert await threads.project_run(
        ThreadMetaRunProjection(
            run_id="run-1",
            thread_id="thread-1",
            owner_worker_id="worker-1",
            active_state_version=running["state_version"],
            status="running",
        ),
        user_id="test-user",
    )
    completed = await runs.transition_owned_run_atomic(
        "run-1",
        expected_state_version=running["state_version"],
        expected_statuses=("running",),
        transition=LifecycleTransition(
            lifecycle_type=LifecycleType.succeeded,
            status="success",
        ),
        expected_owner_worker_id="worker-1",
        require_unexpired_lease=False,
    )
    assert completed.applied
    assert completed.row is not None
    terminal_version = completed.row["state_version"]

    wrong_terminal = ThreadMetaRunProjection(
        run_id="run-1",
        thread_id="thread-1",
        owner_worker_id="worker-1",
        active_state_version=running["state_version"],
        terminal_state_version=terminal_version + 1,
        status="idle",
        display_name="Generated title",
    )
    assert await threads.project_run(wrong_terminal, user_id="test-user") is False
    before = await threads.get("thread-1", user_id="test-user")
    assert before is not None
    assert before["display_name"] == "Human title"
    assert before["status"] == "running"

    exact_terminal = ThreadMetaRunProjection(
        run_id="run-1",
        thread_id="thread-1",
        owner_worker_id="worker-1",
        active_state_version=running["state_version"],
        terminal_state_version=terminal_version,
        status="idle",
        display_name="Generated title",
    )
    assert await threads.project_run(exact_terminal, user_id="test-user") is True
    after = await threads.get("thread-1", user_id="test-user")
    assert after is not None
    assert after["display_name"] == "Generated title"
    assert after["status"] == "idle"


async def _terminal_projection_rejects_a_forged_prior_owner_epoch(
    runs: RunStore,
    threads,
) -> None:
    await threads.create(
        "thread-forged-terminal",
        display_name="Human title",
        user_id="test-user",
    )
    running = await _admit_and_start(
        runs,
        run_id="run-forged-terminal",
        thread_id="thread-forged-terminal",
        owner="worker-real",
    )
    completed = await runs.transition_owned_run_atomic(
        "run-forged-terminal",
        expected_state_version=running["state_version"],
        expected_statuses=("running",),
        transition=LifecycleTransition(
            lifecycle_type=LifecycleType.succeeded,
            status="success",
        ),
        expected_owner_worker_id="worker-real",
        require_unexpired_lease=False,
    )
    assert completed.applied
    assert completed.row is not None

    forged_owner = ThreadMetaRunProjection(
        run_id="run-forged-terminal",
        thread_id="thread-forged-terminal",
        owner_worker_id="worker-never-owned",
        active_state_version=running["state_version"],
        terminal_state_version=completed.row["state_version"],
        status="idle",
        display_name="Forged title",
    )
    assert await threads.project_run(forged_owner, user_id="test-user") is False

    stale_epoch = ThreadMetaRunProjection(
        run_id="run-forged-terminal",
        thread_id="thread-forged-terminal",
        owner_worker_id="worker-real",
        active_state_version=running["state_version"] - 1,
        terminal_state_version=completed.row["state_version"],
        status="idle",
        display_name="Stale title",
    )
    assert await threads.project_run(stale_epoch, user_id="test-user") is False
    current = await threads.get("thread-forged-terminal", user_id="test-user")
    assert current is not None
    assert current["display_name"] == "Human title"


async def _direct_terminal_put_preserves_the_prior_owner_epoch(
    runs: RunStore,
    threads,
) -> None:
    await threads.create(
        "thread-direct-terminal",
        display_name="Human title",
        user_id="test-user",
    )
    await runs.put(
        "run-direct-terminal",
        thread_id="thread-direct-terminal",
        user_id="test-user",
        owner_worker_id="worker-real",
        status="success",
    )
    completed = await runs.get("run-direct-terminal", user_id="test-user")
    assert completed is not None
    assert completed["state_version"] == 2
    assert completed["terminal_projection_owner_worker_id"] == "worker-real"
    assert completed["terminal_projection_active_state_version"] == 1

    exact_terminal = ThreadMetaRunProjection(
        run_id="run-direct-terminal",
        thread_id="thread-direct-terminal",
        owner_worker_id="worker-real",
        active_state_version=1,
        terminal_state_version=2,
        status="idle",
        display_name="Generated title",
    )
    assert await threads.project_run(exact_terminal, user_id="test-user") is True
    current = await threads.get("thread-direct-terminal", user_id="test-user")
    assert current is not None
    assert current["display_name"] == "Generated title"


async def _terminal_execution_fence_requires_the_prior_owner_epoch(
    runs: RunStore,
) -> None:
    running = await _admit_and_start(
        runs,
        run_id="run-terminal-execution-fence",
        thread_id="thread-terminal-execution-fence",
        owner="worker-real",
    )
    completed = await runs.transition_owned_run_atomic(
        "run-terminal-execution-fence",
        expected_state_version=running["state_version"],
        expected_statuses=("running",),
        transition=LifecycleTransition(
            lifecycle_type=LifecycleType.succeeded,
            status="success",
        ),
        expected_owner_worker_id="worker-real",
        require_unexpired_lease=False,
    )
    assert completed.applied
    assert completed.row is not None
    terminal_version = completed.row["state_version"]

    async with runs.hold_execution_fence(
        "run-terminal-execution-fence",
        owner_worker_id="worker-real",
        state_version=running["state_version"],
        terminal_state_version=terminal_version,
    ) as exact:
        assert exact is True
    async with runs.hold_execution_fence(
        "run-terminal-execution-fence",
        owner_worker_id="worker-never-owned",
        state_version=running["state_version"],
        terminal_state_version=terminal_version,
    ) as forged_owner:
        assert forged_owner is False
    async with runs.hold_execution_fence(
        "run-terminal-execution-fence",
        owner_worker_id="worker-real",
        state_version=running["state_version"] - 1,
        terminal_state_version=terminal_version,
    ) as stale_epoch:
        assert stale_epoch is False


async def _sqlite_subsecond_expired_lease() -> str:
    """Choose an expiry old SQLite's whole-second clock misclassifies."""

    while True:
        now = datetime.now(UTC)
        if 300_000 <= now.microsecond <= 500_000:
            return (now - timedelta(milliseconds=100)).isoformat()
        await asyncio.sleep(0.01)


@pytest.mark.anyio
async def test_memory_terminal_projection_requires_the_transition_owner_epoch() -> None:
    runs = MemoryRunStore()
    threads = MemoryThreadMetaStore(InMemoryStore(), run_store=runs)
    await _terminal_projection_rejects_a_forged_prior_owner_epoch(runs, threads)


@pytest.mark.anyio
async def test_sql_terminal_projection_requires_the_transition_owner_epoch(
    sqlite_stores,
) -> None:
    await _terminal_projection_rejects_a_forged_prior_owner_epoch(*sqlite_stores)


@pytest.mark.anyio
async def test_memory_direct_terminal_put_preserves_the_prior_owner_epoch() -> None:
    runs = MemoryRunStore()
    threads = MemoryThreadMetaStore(InMemoryStore(), run_store=runs)
    await _direct_terminal_put_preserves_the_prior_owner_epoch(runs, threads)


@pytest.mark.anyio
async def test_sql_direct_terminal_put_preserves_the_prior_owner_epoch(
    sqlite_stores,
) -> None:
    await _direct_terminal_put_preserves_the_prior_owner_epoch(*sqlite_stores)


@pytest.mark.anyio
async def test_memory_terminal_execution_fence_requires_the_transition_owner_epoch() -> None:
    await _terminal_execution_fence_requires_the_prior_owner_epoch(MemoryRunStore())


@pytest.mark.anyio
async def test_sql_terminal_execution_fence_requires_the_transition_owner_epoch(
    sqlite_stores,
) -> None:
    await _terminal_execution_fence_requires_the_prior_owner_epoch(sqlite_stores[0])


@pytest.mark.anyio
async def test_sql_running_authorities_reject_subsecond_expired_lease(
    sqlite_stores,
) -> None:
    runs, threads = sqlite_stores
    await threads.create(
        "thread-subsecond-expiry",
        display_name="Human title",
        user_id="test-user",
    )
    lease_expires_at = await _sqlite_subsecond_expired_lease()
    await runs.put(
        "run-subsecond-expiry",
        thread_id="thread-subsecond-expiry",
        user_id="test-user",
        owner_worker_id="worker-expired",
        lease_expires_at=lease_expires_at,
        status="running",
    )
    running = await runs.get("run-subsecond-expiry", user_id="test-user")
    assert running is not None
    projection = ThreadMetaRunProjection(
        run_id="run-subsecond-expiry",
        thread_id="thread-subsecond-expiry",
        owner_worker_id="worker-expired",
        active_state_version=running["state_version"],
        status="running",
    )

    assert await threads.project_run(projection, user_id="test-user") is False
    async with runs.hold_execution_fence(
        "run-subsecond-expiry",
        owner_worker_id="worker-expired",
        state_version=running["state_version"],
    ) as active:
        assert active is False


@pytest.mark.anyio
async def test_sql_run_projection_uses_database_clock_for_thread_recency(
    sqlite_stores,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from deerflow.persistence.thread_meta import sql as thread_meta_sql

    runs, threads = sqlite_stores
    await threads.create("thread-db-clock", user_id="test-user")
    running = await _admit_and_start(
        runs,
        run_id="run-db-clock",
        thread_id="thread-db-clock",
        owner="worker-skewed",
    )

    class _FastPodClock(datetime):
        @classmethod
        def now(cls, tz=None):
            observed = datetime(2099, 1, 1, tzinfo=UTC)
            return observed if tz is not None else observed.replace(tzinfo=None)

    monkeypatch.setattr(thread_meta_sql, "datetime", _FastPodClock)
    assert await threads.project_run(
        ThreadMetaRunProjection(
            run_id="run-db-clock",
            thread_id="thread-db-clock",
            owner_worker_id="worker-skewed",
            active_state_version=running["state_version"],
            status="running",
        ),
        user_id="test-user",
    )

    projected = await threads.get("thread-db-clock", user_id="test-user")
    assert projected is not None
    updated_at = projected["updated_at"]
    if isinstance(updated_at, str):
        updated_at = datetime.fromisoformat(updated_at)
    assert updated_at.year < 2099


@pytest.mark.anyio
async def test_memory_execution_fence_serializes_lease_mutation() -> None:
    runs = MemoryRunStore()
    running = await _admit_and_start(
        runs,
        run_id="run-memory-lease-fence",
        thread_id="thread-memory-lease-fence",
        owner="worker-current",
        lease_expires_at=(datetime.now(UTC) + timedelta(minutes=5)).isoformat(),
    )
    update_task: asyncio.Task[bool] | None = None

    async with runs.hold_execution_fence(
        "run-memory-lease-fence",
        owner_worker_id="worker-current",
        state_version=running["state_version"],
    ) as active:
        assert active is True
        update_task = asyncio.create_task(
            runs.update_lease(
                "run-memory-lease-fence",
                owner_worker_id="worker-current",
                lease_expires_at=(datetime.now(UTC) - timedelta(minutes=1)).isoformat(),
            )
        )
        await asyncio.sleep(0)
        assert update_task.done() is False

    assert update_task is not None
    assert await update_task is True


@pytest.mark.anyio
async def test_memory_projection_authority_serializes_run_deletion() -> None:
    runs = MemoryRunStore()
    running = await _admit_and_start(
        runs,
        run_id="run-memory-delete-fence",
        thread_id="thread-memory-delete-fence",
        owner="worker-current",
    )
    delete_task: asyncio.Task[None] | None = None

    async with runs.hold_thread_projection_authority(
        run_id="run-memory-delete-fence",
        thread_id="thread-memory-delete-fence",
        run_status="running",
        owner_worker_id="worker-current",
        active_state_version=running["state_version"],
    ) as active:
        assert active is True
        delete_task = asyncio.create_task(
            runs.delete("run-memory-delete-fence"),
        )
        await asyncio.sleep(0)
        assert delete_task.done() is False

    assert delete_task is not None
    await delete_task
    assert await runs.get("run-memory-delete-fence") is None


@pytest.mark.anyio
async def test_memory_terminal_projection_requires_the_exact_terminal_version() -> None:
    runs = MemoryRunStore()
    threads = MemoryThreadMetaStore(InMemoryStore(), run_store=runs)
    await _terminal_projection_updates_title_and_status_together(runs, threads)


@pytest.mark.anyio
async def test_sql_terminal_projection_requires_the_exact_terminal_version(
    sqlite_stores,
) -> None:
    await _terminal_projection_updates_title_and_status_together(*sqlite_stores)


@pytest.mark.anyio
@pytest.mark.parametrize("malformed_field", ["owner_worker_id", "lease_expires_at"])
async def test_sql_terminal_projection_rejects_partially_finalized_rows(
    sqlite_stores,
    malformed_field,
) -> None:
    runs, threads = sqlite_stores
    await threads.create(
        "thread-1",
        display_name="Human title",
        user_id="test-user",
    )
    running = await _admit_and_start(
        runs,
        run_id="run-1",
        thread_id="thread-1",
        owner="worker-1",
    )
    completed = await runs.transition_owned_run_atomic(
        "run-1",
        expected_state_version=running["state_version"],
        expected_statuses=("running",),
        transition=LifecycleTransition(
            lifecycle_type=LifecycleType.succeeded,
            status="success",
        ),
        expected_owner_worker_id="worker-1",
        require_unexpired_lease=False,
    )
    assert completed.applied
    assert completed.row is not None

    from deerflow.persistence.engine import get_session_factory

    malformed_value = "ghost-worker" if malformed_field == "owner_worker_id" else datetime.now(UTC) + timedelta(minutes=5)
    async with get_session_factory()() as session:
        await session.execute(update(RunRow).where(RunRow.run_id == "run-1").values(**{malformed_field: malformed_value}))
        await session.commit()

    projection = ThreadMetaRunProjection(
        run_id="run-1",
        thread_id="thread-1",
        owner_worker_id="worker-1",
        active_state_version=running["state_version"],
        terminal_state_version=completed.row["state_version"],
        status="idle",
        display_name="Generated title",
    )
    assert await threads.project_run(projection, user_id="test-user") is False
    current = await threads.get("thread-1", user_id="test-user")
    assert current is not None
    assert current["display_name"] == "Human title"


@pytest.mark.anyio
async def test_sql_projection_fails_closed_when_admission_order_is_missing(
    sqlite_stores,
) -> None:
    runs, threads = sqlite_stores
    await threads.create("thread-1", user_id="test-user")
    running = await _admit_and_start(
        runs,
        run_id="run-1",
        thread_id="thread-1",
        owner="worker-1",
    )
    from deerflow.persistence.engine import get_session_factory

    async with get_session_factory()() as session:
        await session.execute(update(RunRow).where(RunRow.run_id == "run-1").values(admission_cursor=None))
        await session.commit()

    projection = ThreadMetaRunProjection(
        run_id="run-1",
        thread_id="thread-1",
        owner_worker_id="worker-1",
        active_state_version=running["state_version"],
        status="running",
    )
    assert await threads.project_run(projection, user_id="test-user") is False
    current = await threads.get("thread-1", user_id="test-user")
    assert current is not None
    assert current["status"] == "idle"


@pytest.mark.anyio
@pytest.mark.postgres_contract
@pytest.mark.skipif(
    not _POSTGRES_URL,
    reason=("requires DEERFLOW_TEST_POSTGRES_URL for the admission-cursor serialization gate"),
)
async def test_postgres_replicas_serialize_admission_cursor_allocation() -> None:
    assert _POSTGRES_URL is not None
    engine = create_async_engine(postgres_async_url(_POSTGRES_URL))
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)

    unique = uuid.uuid4().hex
    run_ids = [
        f"projection-cursor-left-{unique}",
        f"projection-cursor-right-{unique}",
        f"projection-cursor-later-{unique}",
    ]
    repositories = [
        RunRepository(session_factory),
        RunRepository(session_factory),
    ]
    try:
        await asyncio.gather(
            repositories[0].put(
                run_ids[0],
                thread_id=f"projection-cursor-thread-left-{unique}",
                owner_worker_id="worker-left",
                user_id=None,
            ),
            repositories[1].put(
                run_ids[1],
                thread_id=f"projection-cursor-thread-right-{unique}",
                owner_worker_id="worker-right",
                user_id=None,
            ),
        )
        concurrent = [await repositories[index].get(run_id, user_id=None) for index, run_id in enumerate(run_ids[:2])]
        assert all(row is not None for row in concurrent)
        concurrent_cursors = {row["admission_cursor"] for row in concurrent if row is not None}
        assert len(concurrent_cursors) == 2
        assert all(type(cursor) is int and cursor > 0 for cursor in concurrent_cursors)

        await repositories[0].put(
            run_ids[2],
            thread_id=f"projection-cursor-thread-later-{unique}",
            owner_worker_id="worker-later",
            user_id=None,
        )
        later = await repositories[0].get(run_ids[2], user_id=None)
        assert later is not None
        assert later["admission_cursor"] > max(concurrent_cursors)
    finally:
        for run_id in reversed(run_ids):
            await repositories[0].delete(run_id, user_id=None)
        await engine.dispose()


@pytest.mark.anyio
@pytest.mark.postgres_contract
@pytest.mark.skipif(
    not _POSTGRES_URL,
    reason=("requires DEERFLOW_TEST_POSTGRES_URL for the projection/admission row-lock race gate"),
)
async def test_postgres_new_admission_committed_while_projection_waits_wins() -> None:
    """A cross-replica stale projection must recheck after its row lock wait."""

    assert _POSTGRES_URL is not None
    engine = create_async_engine(postgres_async_url(_POSTGRES_URL))
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)

    unique = uuid.uuid4().hex
    thread_id = f"projection-thread-{unique}"
    old_run_id = f"projection-z-old-{unique}"
    new_run_id = f"projection-a-new-{unique}"
    user_id = f"projection-user-{unique}"
    same_created_at = "2026-09-01T12:00:00+00:00"
    runs = RunRepository(session_factory)
    threads = ThreadMetaRepository(session_factory)
    projection_task: asyncio.Task[bool] | None = None

    try:
        await threads.create(
            thread_id,
            display_name="Human title",
            user_id=user_id,
        )
        await threads.update_status(thread_id, "running", user_id=user_id)
        await runs.put(
            old_run_id,
            thread_id=thread_id,
            user_id=user_id,
            owner_worker_id="worker-old",
            status="success",
            created_at=same_created_at,
        )
        old = await runs.get(old_run_id, user_id=user_id)
        assert old is not None
        assert old["state_version"] == 2

        stale_projection = ThreadMetaRunProjection(
            run_id=old_run_id,
            thread_id=thread_id,
            owner_worker_id="worker-old",
            active_state_version=1,
            terminal_state_version=old["state_version"],
            status="idle",
            display_name="Stale generated title",
        )

        # Hold the stale candidate row so the projection begins in one
        # repository/session but cannot make its authority decision yet.
        # A second public RunRepository commits a newer admission while the
        # projection waits. Once unblocked, project_run must observe the new
        # admission cursor and reject the stale write.
        async with session_factory.begin() as blocking_session:
            locked = await blocking_session.scalar(select(RunRow).where(RunRow.run_id == old_run_id).with_for_update())
            assert locked is not None
            projection_task = asyncio.create_task(threads.project_run(stale_projection, user_id=user_id))
            await asyncio.sleep(0.05)
            assert projection_task.done() is False

            await RunRepository(session_factory).put(
                new_run_id,
                thread_id=thread_id,
                user_id=user_id,
                owner_worker_id="worker-new",
                status="pending",
                created_at=same_created_at,
            )

        assert await asyncio.wait_for(projection_task, timeout=2) is False
        current = await threads.get(thread_id, user_id=user_id)
        assert current is not None
        assert current["display_name"] == "Human title"
        assert current["status"] == "running"
    finally:
        if projection_task is not None and not projection_task.done():
            projection_task.cancel()
            await asyncio.gather(projection_task, return_exceptions=True)
        await runs.delete(new_run_id, user_id=None)
        await runs.delete(old_run_id, user_id=None)
        await threads.delete(thread_id, user_id=None)
        await engine.dispose()


@pytest.mark.anyio
async def test_worker_projects_running_and_terminal_status_with_exact_fences() -> None:
    runs = MemoryRunStore()
    from deerflow.runtime import RunManager, RunStatus
    from deerflow.runtime.events import MemoryRunEventStore

    event_store = MemoryRunEventStore(run_store=runs)
    manager = RunManager(store=runs, event_store=event_store)
    record = await manager.create("thread-1")
    thread_store = SimpleNamespace(project_run=AsyncMock(return_value=True))
    bridge = SimpleNamespace(
        publish=AsyncMock(),
        publish_end=AsyncMock(),
        cleanup=AsyncMock(),
    )

    class SuccessfulAgent:
        metadata = {"model_name": "test-model"}
        checkpointer = None
        store = None
        interrupt_before_nodes = None
        interrupt_after_nodes = None

        async def astream(self, *args, **kwargs):
            del args, kwargs
            if False:
                yield None

    await run_agent(
        bridge,
        manager,
        record,
        ctx=RunContext(
            checkpointer=None,
            event_store=event_store,
            thread_store=thread_store,
        ),
        agent_factory=lambda **_kwargs: SuccessfulAgent(),
        graph_input={"messages": []},
        config={},
    )

    assert record.status is RunStatus.success
    assert thread_store.project_run.await_count == 2, (
        record.checkpoint_terminal_state_version,
        record.ownership_lost,
        record.state_version,
    )
    running_projection = thread_store.project_run.await_args_list[0].args[0]
    terminal_projection = thread_store.project_run.await_args_list[1].args[0]
    assert running_projection.status == "running"
    assert running_projection.terminal_state_version is None
    assert terminal_projection.status == "idle"
    assert terminal_projection.active_state_version == running_projection.active_state_version
    assert terminal_projection.terminal_state_version == record.checkpoint_terminal_state_version
