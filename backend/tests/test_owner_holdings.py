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

from deerflow.runtime.owner_holdings import OwnerHoldings


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
    assert holdings.drain_late_endings() == {"pat": {"sse_streams": 1}}
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
    assert holdings.drain_late_endings() == {"pat": {"downloads": 1}}
