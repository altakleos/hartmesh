"""Startup recovery is bounded even if a driver takes time to cancel."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest


@pytest.mark.asyncio
async def test_listing_failure_is_logged_without_blocking_readiness(caplog):
    from app.subagent_batches.recovery import recover_unavailable_batches

    repo = SimpleNamespace(list_unavailable_bound_batches=AsyncMock(side_effect=OSError("private-row-sentinel")))
    await recover_unavailable_batches(repo)
    assert "listing" in caplog.text.lower()
    assert "OSError" in caplog.text
    assert "private-row-sentinel" not in caplog.text


@pytest.mark.asyncio
async def test_one_failed_batch_does_not_hide_the_next_batch_or_page():
    from app.subagent_batches.recovery import recover_unavailable_batches

    repo = SimpleNamespace(
        list_unavailable_bound_batches=AsyncMock(side_effect=[["one", "two"], ["z"], []]),
        cancel_unavailable_bound_batch=AsyncMock(side_effect=[ValueError("bad row"), True, False]),
    )
    await recover_unavailable_batches(repo)
    assert [call.args[0] for call in repo.cancel_unavailable_bound_batch.await_args_list] == ["one", "two", "z"]
    assert [call.kwargs["after"] for call in repo.list_unavailable_bound_batches.await_args_list] == [None, "two", "z"]


@pytest.mark.asyncio
async def test_slow_driver_cleanup_cannot_hold_gateway_startup():
    from app.subagent_batches.recovery import recover_unavailable_batches

    cleanup = asyncio.Event()
    finished = asyncio.Event()

    async def stuck(**kwargs):
        try:
            await asyncio.Event().wait()
        finally:
            await cleanup.wait()
            finished.set()

    repo = SimpleNamespace(list_unavailable_bound_batches=stuck)
    try:
        await asyncio.wait_for(recover_unavailable_batches(repo, time_budget=0.05, call_timeout=0.02), timeout=0.4)
        assert not finished.is_set()
    finally:
        cleanup.set()
    await asyncio.wait_for(finished.wait(), timeout=0.4)


@pytest.mark.asyncio
async def test_multiple_slow_calls_cannot_each_consume_the_full_startup_budget():
    from app.subagent_batches.recovery import recover_unavailable_batches

    attempted = []

    async def slow_cancel(batch_id):
        attempted.append(batch_id)
        await asyncio.Event().wait()

    repo = SimpleNamespace(list_unavailable_bound_batches=AsyncMock(return_value=list("abcdefghij")), cancel_unavailable_bound_batch=slow_cancel)
    await asyncio.wait_for(recover_unavailable_batches(repo, time_budget=0.12, call_timeout=0.04), timeout=0.6)
    assert 1 <= len(attempted) < 10
