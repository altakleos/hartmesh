"""Restart-safe, access-filtered authoritative lifecycle query tests."""

from __future__ import annotations

import asyncio
import base64
import json
import os
import uuid
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import pytest
from sqlalchemy import delete, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from deerflow.persistence.base import Base
from deerflow.persistence.run.model import RunRow
from deerflow.persistence.run.sql import RunRepository
from deerflow.runtime.runs.store.base import LifecycleTransition, LifecycleType, lifecycle_owner_scope
from deerflow.runtime.runs.store.memory import MemoryRunStore

_POSTGRES_URL = os.environ.get("DEERFLOW_TEST_POSTGRES_URL")


def _postgres_async_url(url: str) -> str:
    if url.startswith("postgresql://"):
        url = "postgresql+asyncpg://" + url[len("postgresql://") :]
    parts = urlsplit(url)
    query = urlencode((key, value) for key, value in parse_qsl(parts.query, keep_blank_values=True) if key not in {"sslmode", "channel_binding"})
    return urlunsplit(parts._replace(query=query))


@pytest.mark.anyio
async def test_page_boundary_uses_last_returned_event_before_read_fence() -> None:
    from deerflow.runtime.runs.lifecycle_query import LifecycleQuery, decode_lifecycle_cursor

    store = MemoryRunStore()
    await store.put("run-1", thread_id="thread-1", user_id="owner-1")
    await store.transition_run_atomic(
        "run-1",
        expected_state_version=1,
        expected_statuses=("pending",),
        transition=LifecycleTransition(LifecycleType.started, "running"),
    )
    await store.transition_run_atomic(
        "run-1",
        expected_state_version=2,
        expected_statuses=("running",),
        transition=LifecycleTransition(LifecycleType.succeeded, "success"),
    )

    page = await store.query_lifecycle(
        LifecycleQuery(
            run_id="run-1",
            owner_scope=lifecycle_owner_scope("owner-1"),
            limit=1,
        )
    )

    assert [event["lifecycle_type"] for event in page.events] == [LifecycleType.accepted]
    assert decode_lifecycle_cursor(page.next_cursor) == 1
    assert decode_lifecycle_cursor(page.read_fence_cursor) == 3
    assert page.snapshots[0]["status"] == "success"
    assert page.snapshots[0]["state_version"] == 3


@pytest.mark.anyio
async def test_sql_query_reconstructs_snapshot_and_events_after_store_restart(tmp_path) -> None:
    from deerflow.runtime.runs.lifecycle_query import LifecycleQuery

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'query.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        writer = RunRepository(factory)
        await writer.put("run-1", thread_id="thread-1", user_id="owner-1")

        reader = RunRepository(factory)
        page = await reader.query_lifecycle(
            LifecycleQuery(
                run_id="run-1",
                owner_scope=lifecycle_owner_scope("owner-1"),
            )
        )

        assert [row["run_id"] for row in page.snapshots] == ["run-1"]
        assert [event["lifecycle_type"] for event in page.events] == [LifecycleType.accepted]
    finally:
        await engine.dispose()


@pytest.mark.anyio
async def test_filtered_empty_page_advances_to_global_read_fence() -> None:
    from deerflow.runtime.runs.lifecycle_query import LifecycleQuery, decode_lifecycle_cursor, encode_lifecycle_cursor

    store = MemoryRunStore()
    await store.put("owner-run", thread_id="thread-1", user_id="owner-1", status="success")
    owner_fence = 2
    await store.put("other-run", thread_id="thread-1", user_id="owner-2", status="success")

    page = await store.query_lifecycle(
        LifecycleQuery(
            thread_id="thread-1",
            owner_scope=lifecycle_owner_scope("owner-1"),
            cursor=encode_lifecycle_cursor(owner_fence),
        )
    )

    assert page.events == ()
    assert [row["run_id"] for row in page.snapshots] == ["owner-run"]
    assert decode_lifecycle_cursor(page.next_cursor) == 4
    assert page.next_cursor == page.read_fence_cursor


@pytest.mark.anyio
async def test_repeated_page_is_harmless_and_page_boundary_skips_nothing() -> None:
    from deerflow.runtime.runs.lifecycle_query import LifecycleQuery

    store = MemoryRunStore()
    await store.put("run-1", thread_id="thread-1", user_id="owner-1", status="success")
    query = LifecycleQuery(
        run_id="run-1",
        owner_scope=lifecycle_owner_scope("owner-1"),
        limit=1,
    )

    first = await store.query_lifecycle(query)
    duplicate = await store.query_lifecycle(query)
    second = await store.query_lifecycle(
        LifecycleQuery(
            run_id="run-1",
            owner_scope=lifecycle_owner_scope("owner-1"),
            cursor=first.next_cursor,
            limit=1,
        )
    )

    assert duplicate.events == first.events
    assert [event["cursor"] for event in (*first.events, *second.events)] == [1, 2]
    assert second.next_cursor == second.read_fence_cursor


@pytest.mark.anyio
async def test_malformed_ahead_and_pruned_cursors_are_typed() -> None:
    from deerflow.runtime.runs.lifecycle_query import CursorAhead, CursorGap, InvalidLifecycleCursor, LifecycleQuery, encode_lifecycle_cursor

    store = MemoryRunStore()
    await store.put("run-1", thread_id="thread-1", user_id="owner-1", status="success")

    wrong_version = base64.urlsafe_b64encode(json.dumps({"cursor": 0, "version": "other/v1"}).encode()).rstrip(b"=").decode()
    with pytest.raises(InvalidLifecycleCursor):
        LifecycleQuery(run_id="run-1", cursor=f"lc1.{wrong_version}")
    with pytest.raises(InvalidLifecycleCursor):
        LifecycleQuery(run_id="run-1", cursor="not-a-cursor")
    with pytest.raises(CursorAhead):
        await store.query_lifecycle(LifecycleQuery(run_id="run-1", cursor=encode_lifecycle_cursor(3)))

    minimum = await store.prune_lifecycle_through(encode_lifecycle_cursor(1))
    assert minimum == encode_lifecycle_cursor(1)
    assert await store.prune_lifecycle_through(encode_lifecycle_cursor(0)) == minimum
    with pytest.raises(CursorGap) as exc_info:
        await store.query_lifecycle(LifecycleQuery(run_id="run-1", cursor=encode_lifecycle_cursor(0)))
    assert exc_info.value.minimum_available_cursor == minimum

    resumed = await store.query_lifecycle(LifecycleQuery(run_id="run-1", cursor=minimum))
    fresh = await store.query_lifecycle(LifecycleQuery(run_id="run-1"))
    assert [event["cursor"] for event in resumed.events] == [2]
    assert fresh.events == resumed.events


@pytest.mark.anyio
async def test_context_query_excludes_other_owners_and_auxiliary_rows() -> None:
    from deerflow.runtime.runs.lifecycle_query import LifecycleQuery

    store = MemoryRunStore()
    await store.put("owner-run", thread_id="thread-1", user_id="owner-1", status="success")
    await store.put("other-run", thread_id="thread-1", user_id="owner-2", status="success")
    await store.put(
        "checkpoint-write",
        thread_id="thread-1",
        user_id="owner-1",
        operation_kind="checkpoint_write",
    )

    page = await store.query_lifecycle(
        LifecycleQuery(
            thread_id="thread-1",
            owner_scope=lifecycle_owner_scope("owner-1"),
        )
    )

    assert {row["run_id"] for row in page.snapshots} == {"owner-run"}
    assert {event["run_id"] for event in page.events} == {"owner-run"}


@pytest.mark.anyio
async def test_sql_prune_is_monotonic_and_cursor_state_corruption_fails_read(tmp_path) -> None:
    from deerflow.runtime.runs.lifecycle_query import LifecycleOrderingCorruption, LifecycleQuery, encode_lifecycle_cursor

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'prune.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    store = RunRepository(factory)
    try:
        await store.put("run-1", thread_id="thread-1", user_id="owner-1", status="success")
        assert await store.prune_lifecycle_through(encode_lifecycle_cursor(1)) == encode_lifecycle_cursor(1)
        assert await store.prune_lifecycle_through(encode_lifecycle_cursor(0)) == encode_lifecycle_cursor(1)
        page = await store.query_lifecycle(LifecycleQuery(run_id="run-1"))
        assert [event["cursor"] for event in page.events] == [2]

        async with engine.begin() as connection:
            await connection.execute(text("DELETE FROM run_lifecycle_cursor_state"))
        with pytest.raises(LifecycleOrderingCorruption):
            await store.query_lifecycle(LifecycleQuery(run_id="run-1"))
    finally:
        await engine.dispose()


@pytest.mark.anyio
@pytest.mark.skipif(
    not _POSTGRES_URL,
    reason="requires DEERFLOW_TEST_POSTGRES_URL for repeatable-read interleaving",
)
async def test_postgres_query_uses_one_repeatable_read_snapshot() -> None:
    """A commit between snapshot and fence reads cannot tear one page."""

    from deerflow.runtime.runs.lifecycle_query import LifecycleQuery, decode_lifecycle_cursor

    assert _POSTGRES_URL is not None
    engine = create_async_engine(_postgres_async_url(_POSTGRES_URL))
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    snapshot_read = asyncio.Event()
    continue_read = asyncio.Event()

    class PausedReader(RunRepository):
        async def _after_lifecycle_snapshot(self) -> None:
            snapshot_read.set()
            await continue_read.wait()

    writer = RunRepository(factory)
    reader = PausedReader(factory)
    unique = uuid.uuid4().hex
    run_id = f"query-repeatable-{unique}"
    thread_id = f"query-repeatable-thread-{unique}"
    try:
        await writer.put(run_id, thread_id=thread_id, user_id=None)
        page_task = asyncio.create_task(reader.query_lifecycle(LifecycleQuery(run_id=run_id)))
        await snapshot_read.wait()
        transition = await writer.transition_run_atomic(
            run_id,
            expected_state_version=1,
            expected_statuses=("pending",),
            transition=LifecycleTransition(LifecycleType.started, "running"),
        )
        assert transition.applied is True
        continue_read.set()
        page = await page_task

        assert [(row["status"], row["state_version"]) for row in page.snapshots] == [("pending", 1)]
        assert [event["lifecycle_type"] for event in page.events] == [LifecycleType.accepted]
        assert decode_lifecycle_cursor(page.read_fence_cursor) == page.events[-1]["cursor"]
    finally:
        continue_read.set()
        async with factory() as session:
            await session.execute(delete(RunRow).where(RunRow.run_id == run_id))
            await session.commit()
        await engine.dispose()
