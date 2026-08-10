"""Authoritative invocation lifecycle-store contract tests."""

from __future__ import annotations

import asyncio
import os
import uuid

import pytest
from sqlalchemy import delete, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from support.postgres import postgres_async_url

from deerflow.persistence.base import Base
from deerflow.persistence.run.model import RunRow
from deerflow.persistence.run.sql import RunRepository
from deerflow.runtime.runs.store.base import (
    CancellationRequestOutcome,
    LifecycleTransition,
    LifecycleType,
    RunStore,
)
from deerflow.runtime.runs.store.memory import MemoryRunStore

_POSTGRES_URL = os.environ.get("DEERFLOW_TEST_POSTGRES_URL")


@pytest.mark.anyio
async def test_normal_admission_creates_version_one_and_accepted_event() -> None:
    store = MemoryRunStore()

    await store.put("run-1", thread_id="thread-1")

    row = await store.get("run-1")
    events = await store.list_lifecycle_events(run_id="run-1")
    assert row is not None
    assert row["state_version"] == 1
    assert [(event["lifecycle_type"], event["state_version"], event["status"]) for event in events] == [(LifecycleType.accepted, 1, "pending")]
    assert store.durable_lifecycle is True


@pytest.mark.anyio
async def test_nonpending_compatibility_insert_records_each_state_mapping() -> None:
    store = MemoryRunStore()

    await store.put("run-1", thread_id="thread-1", status="running")

    row = await store.get("run-1")
    events = await store.list_lifecycle_events(run_id="run-1")
    assert row is not None
    assert (row["status"], row["state_version"]) == ("running", 2)
    assert [event["lifecycle_type"] for event in events] == [
        LifecycleType.accepted,
        LifecycleType.started,
    ]


@pytest.mark.anyio
async def test_compare_and_set_failure_changes_neither_row_nor_journal() -> None:
    store = MemoryRunStore()
    await store.put("run-1", thread_id="thread-1")

    result = await store.transition_run_atomic(
        "run-1",
        expected_state_version=0,
        expected_statuses=("pending",),
        transition=LifecycleTransition(
            lifecycle_type=LifecycleType.started,
            status="running",
        ),
    )

    assert result.applied is False
    assert (await store.get("run-1"))["state_version"] == 1
    assert len(await store.list_lifecycle_events(run_id="run-1")) == 1


@pytest.mark.anyio
async def test_sql_row_and_lifecycle_event_commit_together(tmp_path) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'lifecycle.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    store = RunRepository(async_sessionmaker(engine, expire_on_commit=False))
    try:
        await store.put("run-1", thread_id="thread-1", user_id=None)
        started = await store.transition_run_atomic(
            "run-1",
            expected_state_version=1,
            expected_statuses=("pending",),
            transition=LifecycleTransition(
                lifecycle_type=LifecycleType.started,
                status="running",
            ),
        )

        assert started.applied is True
        row = await store.get("run-1", user_id=None)
        events = await store.list_lifecycle_events(run_id="run-1")
        assert row is not None
        assert (row["state_version"], row["status"]) == (2, "running")
        assert [(event["cursor"], event["lifecycle_type"], event["state_version"]) for event in events] == [
            (1, LifecycleType.accepted, 1),
            (2, LifecycleType.started, 2),
        ]
    finally:
        await engine.dispose()


@pytest.mark.anyio
async def test_auxiliary_rows_never_emit_lifecycle_events() -> None:
    store = MemoryRunStore()
    await store.put(
        "checkpoint-1",
        thread_id="thread-1",
        operation_kind="checkpoint_write",
    )

    row = await store.get("checkpoint-1")
    assert row is not None
    assert row["state_version"] == 0
    assert await store.list_lifecycle_events(run_id="checkpoint-1") == []
    assert RunStore.durable_lifecycle is False

    class CustomMemoryStore(MemoryRunStore):
        pass

    assert CustomMemoryStore.durable_lifecycle is False


@pytest.mark.anyio
async def test_fenced_cancellation_precedence_and_single_event() -> None:
    store = MemoryRunStore()
    await store.put("run-1", thread_id="thread-1")

    requested = await store.request_cancel_fenced(
        "run-1",
        action="interrupt",
        expected_state_version=1,
    )
    duplicate_with_stale_version = await store.request_cancel_fenced(
        "run-1",
        action="interrupt",
        expected_state_version=1,
    )
    different_action = await store.request_cancel_fenced(
        "run-1",
        action="rollback",
        expected_state_version=2,
    )

    assert requested.outcome == CancellationRequestOutcome.requested
    assert duplicate_with_stale_version.outcome == CancellationRequestOutcome.already_requested
    assert different_action.outcome == CancellationRequestOutcome.stale
    events = await store.list_lifecycle_events(run_id="run-1")
    assert [event["lifecycle_type"] for event in events] == [
        LifecycleType.accepted,
        LifecycleType.cancellation_requested,
    ]
    assert events[-1]["payload"] == {
        "version": 1,
        "evidence": {"action": "interrupt"},
    }


@pytest.mark.anyio
async def test_terminal_precedes_stale_for_fenced_cancellation() -> None:
    store = MemoryRunStore()
    await store.put("run-1", thread_id="thread-1")
    await store.update_status("run-1", "success")

    result = await store.request_cancel_fenced(
        "run-1",
        action="interrupt",
        expected_state_version=1,
    )

    assert result.outcome == CancellationRequestOutcome.already_terminal


@pytest.mark.anyio
async def test_stale_orphan_claim_changes_neither_row_nor_journal() -> None:
    store = MemoryRunStore()
    await store.put("run-1", thread_id="thread-1")
    before = await store.get("run-1")
    before_events = await store.list_lifecycle_events(run_id="run-1")

    claimed = await store.claim_for_takeover(
        "run-1",
        grace_seconds=0,
        error="worker disappeared",
        stop_reason="orphan_recovered",
        expected_state_version=0,
    )

    assert claimed is False
    assert await store.get("run-1") == before
    assert await store.list_lifecycle_events(run_id="run-1") == before_events


@pytest.mark.anyio
async def test_replacement_batch_orders_interrupt_before_acceptance() -> None:
    store = MemoryRunStore()
    await store.put("old", thread_id="thread-1", created_at="2026-01-01T00:00:00+00:00")
    await store.start_run("old")

    replacement, claimed = await store.create_thread_operation_atomic(
        "new",
        thread_id="thread-1",
        owner_worker_id="worker",
        lease_expires_at=None,
        multitask_strategy="interrupt",
        created_at="2026-01-01T00:00:01+00:00",
    )

    assert [(row["run_id"], row["status"], row["state_version"]) for row in claimed] == [("old", "interrupted", 3)]
    assert (replacement["status"], replacement["state_version"]) == ("pending", 1)
    events = await store.list_lifecycle_events(thread_id="thread-1")
    assert [(event["run_id"], event["lifecycle_type"]) for event in events] == [
        ("old", LifecycleType.accepted),
        ("old", LifecycleType.started),
        ("old", LifecycleType.interrupted),
        ("new", LifecycleType.accepted),
    ]


@pytest.mark.anyio
async def test_replacement_batch_failure_rolls_back_rows_events_and_cursor() -> None:
    class FailingStore(MemoryRunStore):
        durable_lifecycle = True

        def _append_lifecycle_event(
            self,
            row: dict,
            transition: LifecycleTransition,
            *,
            payload: dict | None = None,
        ) -> dict:
            if row["run_id"] == "new":
                raise RuntimeError("injected batch failure")
            return super()._append_lifecycle_event(row, transition, payload=payload)

    store = FailingStore()
    await store.put("old", thread_id="thread-1")
    before_events = await store.list_lifecycle_events(thread_id="thread-1")

    with pytest.raises(RuntimeError, match="injected batch failure"):
        await store.create_thread_operation_atomic(
            "new",
            thread_id="thread-1",
            owner_worker_id="worker",
            lease_expires_at=None,
            multitask_strategy="interrupt",
        )

    assert (await store.get("old"))["status"] == "pending"
    assert await store.get("new") is None
    assert await store.list_lifecycle_events(thread_id="thread-1") == before_events


@pytest.mark.anyio
async def test_lifecycle_payload_rejects_rich_or_oversize_evidence() -> None:
    store = MemoryRunStore()
    await store.put("run-1", thread_id="thread-1")

    with pytest.raises(ValueError, match="unsupported lifecycle evidence key"):
        await store.transition_run_atomic(
            "run-1",
            expected_state_version=1,
            expected_statuses=("pending",),
            transition=LifecycleTransition(
                lifecycle_type=LifecycleType.cancellation_requested,
                status="pending",
                evidence={"prompt": "secret"},
            ),
        )

    with pytest.raises(ValueError, match="safe scalars"):
        await store.transition_run_atomic(
            "run-1",
            expected_state_version=1,
            expected_statuses=("pending",),
            transition=LifecycleTransition(
                lifecycle_type=LifecycleType.cancellation_requested,
                status="pending",
                evidence={"action": {"messages": ["secret"]}},  # type: ignore[dict-item]
            ),
        )

    with pytest.raises(ValueError, match="256 UTF-8 bytes"):
        await store.transition_run_atomic(
            "run-1",
            expected_state_version=1,
            expected_statuses=("pending",),
            transition=LifecycleTransition(
                lifecycle_type=LifecycleType.cancellation_requested,
                status="pending",
                evidence={"action": "x" * 257},
            ),
        )

    with pytest.raises(ValueError, match="unsupported lifecycle reason"):
        await store.transition_run_atomic(
            "run-1",
            expected_state_version=1,
            expected_statuses=("pending",),
            transition=LifecycleTransition(
                lifecycle_type=LifecycleType.failed,
                status="error",
                reason="secret",
            ),
        )

    assert (await store.get("run-1"))["status"] == "pending"
    assert len(await store.list_lifecycle_events(run_id="run-1")) == 1


@pytest.mark.anyio
async def test_lifecycle_type_must_match_resulting_status() -> None:
    store = MemoryRunStore()
    await store.put("run-1", thread_id="thread-1")

    with pytest.raises(ValueError, match="cannot produce status"):
        await store.transition_run_atomic(
            "run-1",
            expected_state_version=1,
            expected_statuses=("pending",),
            transition=LifecycleTransition(
                lifecycle_type=LifecycleType.succeeded,
                status="error",
            ),
        )

    assert (await store.get("run-1"))["status"] == "pending"
    assert len(await store.list_lifecycle_events(run_id="run-1")) == 1


@pytest.mark.anyio
async def test_sql_pending_start_and_compat_cancel_serialize_without_conflict(tmp_path) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'cancel-race.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    store = RunRepository(async_sessionmaker(engine, expire_on_commit=False))
    try:
        await store.put("run-1", thread_id="thread-1", user_id=None)

        start_result, cancel_result = await asyncio.gather(
            store.start_run("run-1"),
            store.request_cancel_compat("run-1", action="interrupt"),
        )

        assert start_result in (True, False)
        assert cancel_result.outcome == CancellationRequestOutcome.requested
        row = await store.get("run-1", user_id=None)
        events = await store.list_lifecycle_events(run_id="run-1")
        assert row is not None
        assert row["status"] == ("running" if start_result else "pending")
        assert [event["lifecycle_type"] for event in events].count(LifecycleType.cancellation_requested) == 1
        assert events[-1]["state_version"] == row["state_version"]
        assert events[-1]["status"] == row["status"]
    finally:
        await engine.dispose()


@pytest.mark.anyio
async def test_sql_initialization_repairs_only_an_empty_missing_singleton(tmp_path) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'ordering.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
        await connection.execute(text("DELETE FROM run_lifecycle_cursor_state"))
    store = RunRepository(async_sessionmaker(engine, expire_on_commit=False))
    try:
        await store.initialize_lifecycle()
        await store.put("run-1", thread_id="thread-1", user_id=None)
        async with engine.begin() as connection:
            await connection.execute(text("DROP TABLE run_lifecycle_cursor_state"))
            await connection.run_sync(Base.metadata.create_all)
        with pytest.raises(RuntimeError, match="ordering state is corrupt"):
            await store.initialize_lifecycle()
    finally:
        await engine.dispose()


@pytest.mark.anyio
async def test_sql_replacement_failure_rolls_back_rows_events_and_cursor(tmp_path) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'batch.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    store = RunRepository(async_sessionmaker(engine, expire_on_commit=False))
    try:
        await store.put("old", thread_id="thread-1", user_id=None)
        async with engine.begin() as connection:
            await connection.execute(text("CREATE TRIGGER fail_new_lifecycle BEFORE INSERT ON run_lifecycle_events WHEN NEW.run_id = 'new' BEGIN SELECT RAISE(ABORT, 'injected'); END"))

        with pytest.raises(Exception, match="injected"):
            await store.create_thread_operation_atomic(
                "new",
                thread_id="thread-1",
                owner_worker_id="worker",
                lease_expires_at=None,
                multitask_strategy="interrupt",
                user_id=None,
            )

        old = await store.get("old", user_id=None)
        assert old is not None
        assert (old["status"], old["state_version"]) == ("pending", 1)
        assert await store.get("new", user_id=None) is None
        assert len(await store.list_lifecycle_events(thread_id="thread-1")) == 1
        async with engine.connect() as connection:
            assert await connection.scalar(text("SELECT last_cursor FROM run_lifecycle_cursor_state WHERE singleton_id=1")) == 1
    finally:
        await engine.dispose()


@pytest.mark.anyio
@pytest.mark.postgres_contract
@pytest.mark.skipif(
    not _POSTGRES_URL,
    reason="requires DEERFLOW_TEST_POSTGRES_URL for the lifecycle CAS race gate",
)
async def test_postgres_concurrent_cas_has_one_row_event_winner() -> None:
    assert _POSTGRES_URL is not None
    engine = create_async_engine(postgres_async_url(_POSTGRES_URL))
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    left = RunRepository(session_factory)
    right = RunRepository(session_factory)
    unique = uuid.uuid4().hex
    run_id = f"lifecycle-{unique}"
    thread_id = f"lifecycle-thread-{unique}"
    try:
        await left.put(run_id, thread_id=thread_id, user_id=None)
        results = await asyncio.gather(
            left.transition_run_atomic(
                run_id,
                expected_state_version=1,
                expected_statuses=("pending",),
                transition=LifecycleTransition(
                    lifecycle_type=LifecycleType.started,
                    status="running",
                ),
            ),
            right.transition_run_atomic(
                run_id,
                expected_state_version=1,
                expected_statuses=("pending",),
                transition=LifecycleTransition(
                    lifecycle_type=LifecycleType.failed,
                    status="error",
                    reason="orphan_recovered",
                ),
            ),
        )
        assert sorted(result.applied for result in results) == [False, True]
        row = await left.get(run_id, user_id=None)
        events = await left.list_lifecycle_events(run_id=run_id)
        assert row is not None
        assert len(events) == 2
        assert events[-1]["state_version"] == row["state_version"] == 2
        assert events[-1]["status"] == row["status"]
    finally:
        async with session_factory() as session:
            await session.execute(delete(RunRow).where(RunRow.run_id == run_id))
            await session.commit()
        await engine.dispose()


@pytest.mark.anyio
@pytest.mark.postgres_contract
@pytest.mark.skipif(
    not _POSTGRES_URL,
    reason="requires DEERFLOW_TEST_POSTGRES_URL for the global lifecycle cursor race gate",
)
async def test_postgres_independent_runs_allocate_unique_global_cursors() -> None:
    assert _POSTGRES_URL is not None
    engine = create_async_engine(postgres_async_url(_POSTGRES_URL))
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    left = RunRepository(session_factory)
    right = RunRepository(session_factory)
    unique = uuid.uuid4().hex
    run_ids = (f"cursor-left-{unique}", f"cursor-right-{unique}")
    try:
        await asyncio.gather(
            left.put(run_ids[0], thread_id=f"thread-left-{unique}", user_id=None),
            right.put(run_ids[1], thread_id=f"thread-right-{unique}", user_id=None),
        )
        transitions = await asyncio.gather(
            left.transition_run_atomic(
                run_ids[0],
                expected_state_version=1,
                expected_statuses=("pending",),
                transition=LifecycleTransition(
                    lifecycle_type=LifecycleType.started,
                    status="running",
                ),
            ),
            right.transition_run_atomic(
                run_ids[1],
                expected_state_version=1,
                expected_statuses=("pending",),
                transition=LifecycleTransition(
                    lifecycle_type=LifecycleType.failed,
                    status="error",
                    reason="orphan_recovered",
                ),
            ),
        )

        assert all(result.applied for result in transitions)
        transition_cursors = [result.event["cursor"] for result in transitions if result.event]
        assert len(transition_cursors) == len(set(transition_cursors)) == 2
        for run_id in run_ids:
            row = await left.get(run_id, user_id=None)
            events = await left.list_lifecycle_events(run_id=run_id)
            assert row is not None
            assert events[-1]["state_version"] == row["state_version"] == 2
            assert events[-1]["status"] == row["status"]
    finally:
        async with session_factory() as session:
            await session.execute(delete(RunRow).where(RunRow.run_id.in_(run_ids)))
            await session.commit()
        await engine.dispose()
