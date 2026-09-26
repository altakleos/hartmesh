"""The record a Gateway process keeps of having looked for refused owners, and what it ended.

The account command runs in its own process and cannot reach into a
Gateway's memory. It asks for a check (one row, whose id orders it after the
refusal it just committed), and each live Gateway process records the
latest check it has acted on and what it ended for whom. The command waits
until every live process has acted on its check. Liveness is read on the
database's own clock, so two hosts' clocks never have to agree.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from types import SimpleNamespace

import pytest

from deerflow.persistence.refusal_sweeps import RefusalSweepRepository


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
def sweeps(tmp_path) -> Iterator[SimpleNamespace]:
    from deerflow.persistence.engine import close_engine, get_session_factory, init_engine

    asyncio.run(init_engine("sqlite", url=f"sqlite+aiosqlite:///{tmp_path}/sweeps.db", sqlite_dir=str(tmp_path)))
    session_factory = get_session_factory()
    assert session_factory is not None
    try:
        yield SimpleNamespace(repo=RefusalSweepRepository(session_factory))
    finally:
        asyncio.run(close_engine())


@pytest.mark.anyio
async def test_checks_are_ordered_and_the_latest_is_what_a_process_acts_on(sweeps) -> None:
    repo = sweeps.repo
    assert await repo.latest_check() == 0
    first = await repo.request_check()
    second = await repo.request_check()
    assert 0 < first < second
    assert await repo.latest_check() == second


@pytest.mark.anyio
async def test_a_registered_process_is_live_and_reports_the_check_it_acted_on(sweeps) -> None:
    repo = sweeps.repo
    check = await repo.request_check()
    await repo.register("gw-1")
    await repo.register("gw-2")
    await repo.beat("gw-1", checked_through=check)

    processes = {entry.process_id: entry for entry in await repo.live_processes(window_seconds=30)}
    assert set(processes) == {"gw-1", "gw-2"}
    assert processes["gw-1"].checked_through == check
    assert processes["gw-2"].checked_through == check, "a process registers at the latest check: it holds nothing from before it started"

    later = await repo.request_check()
    assert {entry.process_id: entry.checked_through for entry in await repo.live_processes(window_seconds=30)} == {"gw-1": check, "gw-2": check}
    await repo.unregister("gw-2")
    assert [entry.process_id for entry in await repo.live_processes(window_seconds=30)] == ["gw-1"]
    assert later > check


@pytest.mark.anyio
async def test_a_process_that_stopped_beating_is_not_live(sweeps) -> None:
    repo = sweeps.repo
    await repo.register("gw-gone")
    await asyncio.sleep(1.2)
    await repo.register("gw-here")
    assert [entry.process_id for entry in await repo.live_processes(window_seconds=1)] == ["gw-here"]


@pytest.mark.anyio
async def test_endings_are_recorded_per_owner_and_surface_and_read_back_from_a_check_on(sweeps) -> None:
    repo = sweeps.repo
    old = await repo.request_check()
    await repo.record_endings("gw-1", old, {"user-pat": {"sse_streams": 1}})
    check = await repo.request_check()
    await repo.record_endings("gw-1", check, {"user-pat": {"sse_streams": 2, "websockets": 1}, "user-sam": {"downloads": 1}})
    await repo.record_endings("gw-2", check, {"user-pat": {"sse_streams": 1}})
    await repo.record_endings("gw-2", check, {})

    endings = await repo.endings_for(["user-pat"], since_check=check)
    assert sorted((ending.process_id, ending.surface, ending.count) for ending in endings) == [("gw-1", "sse_streams", 2), ("gw-1", "websockets", 1), ("gw-2", "sse_streams", 1)]
    assert all(ending.ended_at.tzinfo is not None for ending in endings)
    assert await repo.endings_for([], since_check=check) == []


@pytest.mark.anyio
async def test_pruning_never_forgets_the_latest_check_and_a_pruned_process_that_beats_is_live_again(sweeps) -> None:
    repo = sweeps.repo
    await repo.request_check()
    latest = await repo.request_check()
    await repo.register("gw-stalled")
    await asyncio.sleep(0.05)
    await repo.prune(older_than_seconds=0)
    assert await repo.latest_check() == latest, "a process registering now must still start at the latest check"
    assert await repo.live_processes(window_seconds=30) == [], "pruned"

    await repo.beat("gw-stalled", checked_through=latest)
    assert [(entry.process_id, entry.checked_through) for entry in await repo.live_processes(window_seconds=30)] == [("gw-stalled", latest)]
