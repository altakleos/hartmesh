"""What a process holds open for an account, so a refusal of that account can end it.

A connection that authenticated once -- a browser WebSocket, an SSE stream,
a streaming download -- never asks again, so turning the account off must
reach it from outside. Every such holding registers under its owner here;
the refusal watch ends everything an owner holds once the owner is refused,
and anything registered for an owner already known to be refused is ended
the moment it registers.
"""

from __future__ import annotations

import asyncio

import pytest

from deerflow.runtime.owner_holdings import Ended, OwnerHoldings


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio
async def test_ending_an_owner_ends_everything_they_hold_and_nothing_anyone_else_holds() -> None:
    holdings = OwnerHoldings()
    ended: list[str] = []
    holdings.hold("pat", "sse_streams", lambda: ended.append("pat-sse-1"))
    holdings.hold("pat", "sse_streams", lambda: ended.append("pat-sse-2"))
    holdings.hold("pat", "websockets", lambda: ended.append("pat-ws"))
    holdings.hold("sam", "sse_streams", lambda: ended.append("sam-sse"))

    counts = await holdings.end_owner("pat")

    assert counts == {"sse_streams": 2, "websockets": 1}
    assert sorted(ended) == ["pat-sse-1", "pat-sse-2", "pat-ws"]
    assert holdings.owners() == {"sam"}


@pytest.mark.anyio
async def test_a_released_holding_is_not_ended_and_an_owner_with_none_left_is_not_listed() -> None:
    holdings = OwnerHoldings()
    ended: list[str] = []
    held = holdings.hold("pat", "downloads", lambda: ended.append("download"))
    held.release()
    held.release()  # idempotent: a response that finished and was then cancelled releases twice

    assert holdings.owners() == set()
    assert await holdings.end_owner("pat") == {}
    assert ended == []


@pytest.mark.anyio
async def test_an_async_ending_is_awaited_and_one_that_fails_does_not_stop_the_others() -> None:
    holdings = OwnerHoldings()
    ended: list[str] = []

    async def _close() -> None:
        await asyncio.sleep(0)
        ended.append("async")

    def _broken() -> None:
        raise RuntimeError("the socket was already gone")

    holdings.hold("pat", "websockets", _broken)
    holdings.hold("pat", "websockets", _close)

    assert await holdings.end_owner("pat") == {"websockets": 2}, "a holding whose end raised is still one it ended"
    assert ended == ["async"]


@pytest.mark.anyio
async def test_a_holding_registered_for_an_owner_known_to_be_refused_ends_at_once() -> None:
    """A stream that started a moment after the watch looked is not left open until the next look."""
    holdings = OwnerHoldings()
    ended: list[str] = []
    holdings.set_refused({"pat"})

    held = holdings.hold("pat", "sse_streams", lambda: ended.append("late"))
    await asyncio.sleep(0)

    assert ended == ["late"] and held.ended
    assert holdings.owners() == set()
    assert holdings.drain_late_endings() == {"pat": {"sse_streams": Ended(1)}}
    assert holdings.drain_late_endings() == {}, "each late ending is reported once"

    holdings.set_refused(set())
    holdings.hold("pat", "sse_streams", lambda: ended.append("after enable"))
    assert holdings.owners() == {"pat"} and ended == ["late"]


def test_a_holding_authorized_after_the_look_read_the_refusals_is_judged_by_its_own_read() -> None:
    """Whichever read is later wins: someone enabled again signs in before the next look and is not cut by the last one."""
    holdings = OwnerHoldings()
    ended: list[str] = []
    holdings.set_refused({"pat"}, read_at=100.0)

    holdings.hold("pat", "sse_streams", lambda: ended.append("authorized after"), authorized_at=100.5)
    assert ended == [] and holdings.owners() == {"pat"}
    assert holdings.drain_late_endings() == {}

    holdings.hold("pat", "downloads", lambda: ended.append("authorized before"), authorized_at=99.5)
    assert ended == ["authorized before"]
    assert holdings.drain_late_endings() == {"pat": {"downloads": Ended(1)}}


@pytest.mark.anyio
async def test_ending_owners_reaches_what_they_hold_and_what_each_subsystem_keeps_for_them() -> None:
    """State a subsystem keeps between requests is ended from its own record, asked once per look."""
    holdings = OwnerHoldings()
    holdings.hold("pat", "sse_streams", lambda: None)
    holdings.hold("sam", "sse_streams", lambda: None)
    asked: list[frozenset[str]] = []

    def _parked(owners: frozenset[str]) -> dict[str, Ended]:
        asked.append(owners)
        return {"pat": Ended(2)}

    async def _queued(owners: frozenset[str]) -> dict[str, Ended]:
        return {owner: Ended(1) for owner in owners if owner == "pat"}

    holdings.add_source("sandboxes", _parked, blocking=True)
    holdings.add_source("memory_updates", _queued)

    ended = await holdings.end_owners({"pat", "lee"})

    assert ended == {"pat": {"sse_streams": Ended(1), "sandboxes": Ended(2), "memory_updates": Ended(1)}}
    assert asked == [frozenset({"pat", "lee"})]
    assert holdings.owners() == {"sam"}
    assert await holdings.end_owners(set()) == {}, "nobody refused: no subsystem is asked"


@pytest.mark.anyio
async def test_a_subsystem_that_cannot_end_what_it_keeps_is_reported_not_ended_and_the_others_still_end() -> None:
    holdings = OwnerHoldings()

    def _broken(owners: frozenset[str]) -> dict[str, Ended]:
        raise RuntimeError("the container runtime is not answering")

    def _partial(owners: frozenset[str]) -> dict[str, Ended]:
        return {"pat": Ended(1, failed=1)}

    holdings.add_source("browser_sessions", _broken)
    holdings.add_source("sandboxes", _partial)

    ended = await holdings.end_owners({"pat", "lee"})

    assert ended["pat"] == {"sandboxes": Ended(1, failed=1)}
    assert ended["*"] == {"browser_sessions": Ended(0, failed=1)}, "whatever it keeps is not known ended, for any refused owner: one row, not one each"
    assert "lee" not in ended


@pytest.mark.anyio
async def test_a_source_that_answers_nonsense_is_not_ended_rather_than_breaking_the_look() -> None:
    holdings = OwnerHoldings()
    holdings.add_source("mcp_sessions", lambda owners: ["pat"])
    holdings.add_source("memory_updates", lambda owners: {"pat": Ended(1)})
    assert await holdings.end_owners({"pat"}) == {"*": {"mcp_sessions": Ended(0, failed=1)}, "pat": {"memory_updates": Ended(1)}}


def test_something_kept_for_an_owner_the_last_look_found_refused_can_be_ended_as_it_is_kept_and_is_recorded() -> None:
    holdings = OwnerHoldings()
    assert not holdings.is_refused("pat")
    holdings.set_refused({"pat"})
    assert holdings.is_refused("pat") and not holdings.is_refused("sam")
    holdings.note_late_ending("pat", "sandboxes", Ended(1))
    holdings.note_late_ending("pat", "sandboxes", Ended(0, failed=1))
    assert holdings.drain_late_endings() == {"pat": {"sandboxes": Ended(1, failed=1)}}


@pytest.mark.anyio
async def test_a_subsystem_slower_than_its_limit_is_not_ended_this_look_and_what_it_ended_is_carried_to_the_next() -> None:
    """A container that takes minutes to stop must not hold up the look, nor be reported ended before it has."""
    import threading

    holdings = OwnerHoldings(source_time_limit_seconds=0.2)
    release = threading.Event()
    calls: list[frozenset[str]] = []

    def _slow_stop(owners: frozenset[str]) -> dict[str, Ended]:
        calls.append(owners)
        if len(calls) == 1:
            release.wait(5)
            return {"pat": Ended(1)}
        return {}

    holdings.add_source("sandboxes", _slow_stop, blocking=True)
    holdings.add_source("memory_updates", lambda owners: {"pat": Ended(1)})

    first = await holdings.end_owners({"pat"})
    assert first == {"*": {"sandboxes": Ended(0, failed=1)}, "pat": {"memory_updates": Ended(1)}}, "the others end within the look"

    still = await holdings.end_owners({"pat"})
    assert still["*"]["sandboxes"] == Ended(0, failed=1) and len(calls) == 1, "not asked twice while the first stop runs"

    release.set()
    await asyncio.sleep(0.2)
    after = await holdings.end_owners({"pat"})
    assert after["pat"]["sandboxes"] == Ended(1) and len(calls) == 2, "the late stop's count is kept, and the source asked again"
