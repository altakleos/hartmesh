"""Shared batch capacity remains owned until transport and SDK sides drain."""

import asyncio

import pytest

from app.telegram_bots import files
from app.telegram_bots.files import FileBytesBudget


@pytest.mark.asyncio
@pytest.mark.parametrize("transport_first", [False, True])
async def test_last_transport_signal_and_owner_drain_jointly_release_batch(transport_first):
    budget = FileBytesBudget(8)
    batch = files.OutputBatch(await budget.reserve_batch([4, 4], timeout=1))
    first, second = batch.begin_transport(0), batch.begin_transport(1)
    one, two = b"abc", b"de"
    await batch.keep(0, one)
    await batch.keep(1, two)
    assert batch.data(0) is one and batch.data(1) is two and budget.used == 5
    await first()
    if transport_first:
        await second()
        assert budget.used == 5
        await batch.aclose()
    else:
        await batch.aclose()
        assert budget.used == 5 and batch.buffers == (None, None)
        await second()
    assert budget.used == 0 and batch.buffers == (None, None)
    await first()
    await second()
    await batch.aclose()
    assert budget.used == 0


@pytest.mark.asyncio
async def test_waiting_batch_observes_prior_buffers_destroyed_before_permit_reuse():
    budget = FileBytesBudget(4)
    batch = files.OutputBatch(await budget.reserve_batch([4], timeout=1))
    await batch.keep(0, b"data")
    observed = []

    async def later():
        reservation = await budget.reserve_batch([4], timeout=1)
        observed.append(batch.buffers)
        await reservation.aclose()

    waiter = asyncio.create_task(later())
    try:
        await asyncio.sleep(0)
        assert not waiter.done()
        await batch.aclose()
        await waiter
        assert observed == [(None,)] and budget.used == 0
        with pytest.raises(ValueError):
            await batch.keep(0, b"late")
    finally:
        await batch.aclose()
        waiter.cancel()
        await asyncio.gather(waiter, return_exceptions=True)


@pytest.mark.asyncio
async def test_batch_rejects_duplicate_member_start_and_overrun_without_growing_charge():
    budget = FileBytesBudget(4)
    batch = files.OutputBatch(await budget.reserve_batch([4], timeout=1))
    closed = batch.begin_transport(0)
    try:
        with pytest.raises(ValueError):
            batch.begin_transport(0)
        with pytest.raises(ValueError):
            await batch.keep(0, b"too long")
        assert budget.used == 4 and batch.buffers == (None,)
        await closed()
    finally:
        await batch.aclose()
    assert budget.used == 0


@pytest.mark.asyncio
async def test_repeated_cancellation_drains_release_before_capacity_reuse():
    budget = FileBytesBudget(4)
    reservation = await budget.reserve_batch([4], timeout=1)
    batch = files.OutputBatch(reservation)
    await batch.keep(0, b"data")
    await budget._condition.acquire()
    release = asyncio.create_task(batch.aclose())
    try:
        await asyncio.sleep(0)
        release.cancel()
        await asyncio.sleep(0)
        release.cancel()
        await asyncio.sleep(0)
        assert not release.done() and batch.buffers == (None,) and budget.used == 4
    finally:
        budget._condition.release()
    with pytest.raises(asyncio.CancelledError):
        await release
    assert budget.used == 0
