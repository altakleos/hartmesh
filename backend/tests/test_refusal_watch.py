"""Each Gateway process looks for refused owners among those it holds something for, ends it, and records it.

A look reads every refused account in one query -- the match each account's
own read makes -- and publishes that set before it ends anything, so a
refused owner cannot open something in between that the look misses. The
process beats every tick whether or not it can look: one that is alive but
failing reads to the command as alive and unconfirmed, never as gone.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Iterator

import pytest

from app.gateway.refusal_watch import RefusalWatch
from deerflow.persistence.refusal_sweeps import RefusalSweepRepository
from deerflow.runtime.owner_holdings import Ended, OwnerHoldings


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
def sweeps(tmp_path) -> Iterator[RefusalSweepRepository]:
    from deerflow.persistence.engine import close_engine, get_session_factory, init_engine

    asyncio.run(init_engine("sqlite", url=f"sqlite+aiosqlite:///{tmp_path}/watch.db", sqlite_dir=str(tmp_path)))
    try:
        yield RefusalSweepRepository(get_session_factory())
    finally:
        asyncio.run(close_engine())


def _watch(sweeps: RefusalSweepRepository, holdings: OwnerHoldings, refused: set[str], **kwargs) -> RefusalWatch:
    async def _refused_owners() -> set[str]:
        return set(refused)

    return RefusalWatch(sweeps, holdings, refused_owners=_refused_owners, process_id="gw-test", **kwargs)


async def _checked_through(sweeps: RefusalSweepRepository, window: float = 30) -> list[int]:
    return [live.checked_through for live in await sweeps.live_processes(window_seconds=window)]


@pytest.mark.anyio
async def test_a_check_makes_the_process_end_what_a_refused_owner_holds_and_record_it(sweeps) -> None:
    holdings = OwnerHoldings()
    ended: list[str] = []
    holdings.hold("pat", "sse_streams", lambda: ended.append("pat"))
    holdings.hold("sam", "sse_streams", lambda: ended.append("sam"))
    refused: set[str] = set()
    watch = _watch(sweeps, holdings, refused)
    await watch.register()

    refused.add("pat")
    check = await sweeps.request_check()
    await watch.tick()

    assert ended == ["pat"] and holdings.owners() == {"sam"}
    assert [(ending.user_id, ending.surface, ending.count) for ending in await sweeps.endings_for(["pat", "sam"], since_check=check)] == [("pat", "sse_streams", 1)]
    assert await _checked_through(sweeps) == [check]


@pytest.mark.anyio
async def test_an_owner_who_held_nothing_when_the_look_began_is_ended_the_moment_they_open_something(sweeps) -> None:
    """A download that does a second of work before its response starts, for a request that authenticated just before the refusal."""
    holdings = OwnerHoldings()
    watch = _watch(sweeps, holdings, {"pat"})
    await watch.register()
    check = await sweeps.request_check()
    authorized_at = time.monotonic()
    await watch.tick()

    ended: list[str] = []
    holdings.hold("pat", "downloads", lambda: ended.append("late"), authorized_at=authorized_at)
    assert ended == ["late"], "published by the look before it ended anything, so a later registration ends at once"
    await watch.tick()
    assert [(ending.surface, ending.count) for ending in await sweeps.endings_for(["pat"], since_check=check)] == [("downloads", 1)]


@pytest.mark.anyio
async def test_without_a_new_check_it_only_beats_until_its_periodic_look(sweeps) -> None:
    holdings = OwnerHoldings()
    ended: list[str] = []
    holdings.hold("pat", "downloads", lambda: ended.append("pat"))
    refused = {"pat"}
    watch = _watch(sweeps, holdings, refused, full_look_every_seconds=3600)
    await watch.register()

    await watch.tick()
    assert ended == [], "nothing asked it to look, and its periodic look is not due"

    watch_due = _watch(sweeps, holdings, refused, full_look_every_seconds=0)
    await watch_due.register()
    await watch_due.tick()
    assert ended == ["pat"], "a refusal that came without a check is still reached by the periodic look"


@pytest.mark.anyio
async def test_an_owner_enabled_again_is_no_longer_ended_on_registering(sweeps) -> None:
    holdings = OwnerHoldings()
    holdings.hold("pat", "sse_streams", lambda: None)
    refused = {"pat"}
    watch = _watch(sweeps, holdings, refused)
    await watch.register()
    await sweeps.request_check()
    await watch.tick()

    refused.clear()
    await sweeps.request_check()
    await watch.tick()
    holdings.hold("pat", "sse_streams", lambda: None)
    assert holdings.owners() == {"pat"}


@pytest.mark.anyio
async def test_a_process_whose_look_keeps_failing_keeps_beating_and_never_claims_the_check(sweeps) -> None:
    """Alive but unable to look is unconfirmed to the command -- never gone, which would read as nothing held."""
    holdings = OwnerHoldings()
    holdings.hold("pat", "sse_streams", lambda: None)

    async def _database_gone() -> set[str]:
        raise RuntimeError("database went away")

    watch = RefusalWatch(sweeps, holdings, refused_owners=_database_gone, process_id="gw-test", interval_seconds=0.1)
    await watch.start()
    try:
        registered_at = watch._checked
        check = await sweeps.request_check()
        await asyncio.sleep(1.5)
        assert await _checked_through(sweeps, window=1) == [registered_at], "beating within the last second, and still short of the check"
    finally:
        await watch.stop()
    assert registered_at < check and holdings.owners() == {"pat"}


@pytest.mark.anyio
async def test_a_look_waiting_on_a_slow_stop_does_not_stop_the_heartbeat(sweeps) -> None:
    """A container slow to stop must not make the process read as gone, and what it holds as ended."""
    import threading

    release = threading.Event()

    def _slow_stop(owners: frozenset[str]) -> dict[str, Ended]:
        release.wait(5)
        return {}

    holdings = OwnerHoldings(source_time_limit_seconds=10)
    holdings.add_source("sandboxes", _slow_stop, blocking=True)
    watch = _watch(sweeps, holdings, {"pat"}, interval_seconds=0.1)
    await watch.start()
    try:
        await sweeps.request_check()
        await asyncio.sleep(1.5)
        assert [live.process_id for live in await sweeps.live_processes(window_seconds=1)] == ["gw-test"], "still beating while the look waits"
    finally:
        release.set()
        await watch.stop()


@pytest.mark.anyio
async def test_what_a_look_ended_but_failed_to_record_is_recorded_by_the_next(sweeps) -> None:
    holdings = OwnerHoldings()
    holdings.hold("pat", "sse_streams", lambda: None)
    watch = _watch(sweeps, holdings, {"pat"})
    await watch.register()
    check = await sweeps.request_check()
    record = sweeps.record_endings
    calls: list[int] = []

    async def _fails_once(*args, **kwargs):
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("database went away")
        return await record(*args, **kwargs)

    sweeps.record_endings = _fails_once  # type: ignore[method-assign]
    with pytest.raises(RuntimeError):
        await watch.tick()
    assert holdings.owners() == set(), "the stream was ended before the record failed"
    assert await _checked_through(sweeps) == [check - 1], "a look whose record failed has not acted on the check"

    await watch.tick()
    assert [(ending.user_id, ending.surface, ending.count) for ending in await sweeps.endings_for(["pat"], since_check=check)] == [("pat", "sse_streams", 1)]
    assert await _checked_through(sweeps) == [check]


@pytest.mark.anyio
async def test_a_process_takes_the_check_its_row_was_registered_at(sweeps) -> None:
    """Its own count and its row never disagree, or it would sit one check behind until its periodic look."""
    await sweeps.request_check()
    await sweeps.request_check()
    watch = _watch(sweeps, OwnerHoldings(), set(), full_look_every_seconds=3600)
    await watch.register()
    await watch.tick()
    assert await _checked_through(sweeps) == [watch._checked] == [2]


@pytest.mark.anyio
async def test_start_and_stop_register_beat_and_unregister(sweeps) -> None:
    holdings = OwnerHoldings()
    watch = _watch(sweeps, holdings, set(), interval_seconds=0.05)
    await watch.start()
    try:
        await asyncio.sleep(0.2)
        assert [live.process_id for live in await sweeps.live_processes(window_seconds=30)] == ["gw-test"]
    finally:
        await watch.stop()
    assert await sweeps.live_processes(window_seconds=30) == []


@pytest.mark.anyio
async def test_a_tick_that_fails_does_not_stop_the_watch(sweeps) -> None:
    holdings = OwnerHoldings()
    holdings.hold("pat", "sse_streams", lambda: None)
    calls: list[int] = []

    async def _fails_once() -> set[str]:
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("database went away")
        return {"pat"}

    watch = RefusalWatch(sweeps, holdings, refused_owners=_fails_once, process_id="gw-test", interval_seconds=0.05)
    await watch.start()
    try:
        await sweeps.request_check()
        for _ in range(100):
            if holdings.owners() == set():
                break
            await asyncio.sleep(0.05)
    finally:
        await watch.stop()
    assert len(calls) >= 2 and holdings.owners() == set(), "the loop went on after the failed tick and ended the stream"


@pytest.mark.anyio
async def test_a_look_ends_what_each_subsystem_keeps_for_a_refused_owner_and_records_what_it_could_not(sweeps) -> None:
    holdings = OwnerHoldings()
    holdings.add_source("mcp_sessions", lambda owners: {"pat": Ended(2)} if "pat" in owners else {})
    holdings.add_source("sandboxes", lambda owners: {"pat": Ended(1, failed=1)} if "pat" in owners else {})
    watch = _watch(sweeps, holdings, {"pat"})
    await watch.register()
    check = await sweeps.request_check()
    await watch.tick()

    endings = sorted((ending.surface, ending.count, ending.failed) for ending in await sweeps.endings_for(["pat"], since_check=check))
    assert endings == [("mcp_sessions", 2, 0), ("sandboxes", 1, 1)]
    assert await _checked_through(sweeps) == [check], "it looked; what it could not end is in the record, not in its silence"


@pytest.mark.anyio
async def test_a_process_names_the_surfaces_it_cannot_reach(sweeps) -> None:
    watch = _watch(sweeps, OwnerHoldings(), set(), unreached=("sandboxes",))
    await watch.register()
    assert [live.unreached for live in await sweeps.live_processes(window_seconds=30)] == [("sandboxes",)]
