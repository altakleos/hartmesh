"""Process byte budgets admit whole buffers without downloading partial batches."""

import asyncio
import importlib

import pytest


def budget_type():
    return importlib.import_module("app.telegram_bots.files").FileBytesBudget


@pytest.mark.asyncio
async def test_batch_charges_are_atomic_and_caller_cannot_mutate_ceilings():
    budget = budget_type()(10)
    ceilings = [4, 6]
    reservation = await budget.reserve_batch(ceilings, timeout=1)
    ceilings[0] = 1
    assert budget.capacity == budget.used == 10
    assert reservation.ceilings == reservation.charges == (4, 6)
    with pytest.raises((AttributeError, TypeError)):
        reservation.ceilings = (1, 1)
    await reservation.aclose()
    assert budget.used == 0 and reservation.charges == (0, 0)


@pytest.mark.asyncio
async def test_waiting_batch_holds_no_partial_charge_and_shrink_wakes_it():
    budget = budget_type()(10)
    active = await budget.reserve_batch([8], timeout=1)
    entered = asyncio.Event()

    async def waiting():
        entered.set()
        return await budget.reserve_batch([2, 2], timeout=15)

    task = asyncio.create_task(waiting())
    following = None
    try:
        await entered.wait()
        await asyncio.sleep(0)
        assert not task.done() and budget.used == 8
        await active.shrink(0, 6)
        following = await asyncio.wait_for(task, 15)
        assert active.ceilings == (8,) and active.charges == (6,)
        assert following.charges == (2, 2) and budget.used == 10
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        if following is not None:
            await following.aclose()
        await active.aclose()
    assert budget.used == 0


@pytest.mark.asyncio
async def test_timeout_and_cancel_waiters_never_charge_a_partial_batch():
    budget = budget_type()(10)
    active = await budget.reserve_batch([7], timeout=1)
    try:
        with pytest.raises(TimeoutError):
            await budget.reserve_batch([2, 2], timeout=0)
        task = asyncio.create_task(budget.reserve_batch([2, 2], timeout=15))
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert budget.used == 7
        spare = await budget.reserve_batch([3], timeout=1)
        await spare.aclose()
    finally:
        await active.aclose()
    assert budget.used == 0


@pytest.mark.asyncio
async def test_shrink_cannot_increase_or_change_another_member():
    budget = budget_type()(10)
    reservation = await budget.reserve_batch([4, 6], timeout=1)
    try:
        await reservation.shrink(0, 1)
        assert reservation.ceilings == (4, 6) and reservation.charges == (1, 6) and budget.used == 7
        for index, size in [(0, 2), (1, 7), (-1, 0), (2, 0), (True, 0), (0, -1), (0, True)]:
            with pytest.raises(ValueError):
                await reservation.shrink(index, size)
        assert budget.used == 7 and reservation.charges == (1, 6)
    finally:
        await reservation.aclose()
    with pytest.raises(ValueError):
        await reservation.shrink(0, 0)


@pytest.mark.asyncio
async def test_concurrent_repeated_close_survives_cancelled_waiter_without_double_release():
    budget = budget_type()(10)
    reservation = await budget.reserve_batch([10], timeout=1)
    # Hold the actual accounting lock so close cannot complete before cancellation.
    await budget._condition.acquire()
    first = asyncio.create_task(reservation.aclose())
    second = None
    try:
        await asyncio.sleep(0)
        first.cancel()
        await asyncio.sleep(0)
        first.cancel()
        second = asyncio.create_task(reservation.aclose())
        await asyncio.sleep(0)
        assert budget.used == 10 and not first.done() and not second.done()
    finally:
        budget._condition.release()
        results = await asyncio.gather(*[task for task in (first, second) if task is not None], return_exceptions=True)
    assert isinstance(results[0], asyncio.CancelledError) and results[1] is None
    await reservation.aclose()
    assert budget.used == 0
    other = await budget.reserve_batch([10], timeout=1)
    await other.aclose()


@pytest.mark.asyncio
async def test_known_zero_is_zero_and_impossible_or_malformed_batches_refuse():
    budget = budget_type()(10)
    empty_file = await budget.reserve_batch([0], timeout=0)
    assert empty_file.ceilings == empty_file.charges == (0,) and budget.used == 0
    await empty_file.aclose()
    for ceilings in [[11], [6, 5], [-1], [True], [None], "123"]:
        with pytest.raises(ValueError):
            await budget.reserve_batch(ceilings, timeout=1)
    for timeout in [-1, float("inf"), float("nan"), True]:
        with pytest.raises(ValueError):
            await budget.reserve_batch([1], timeout=timeout)
    assert budget.used == 0
