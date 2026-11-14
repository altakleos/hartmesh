"""Actual codec cancellation and bounded pagination through the retained owner."""

import asyncio
import threading
from copy import deepcopy

import httpx
import pytest
from test_telegram_assignment_proposal_reader import reader_environment as reader_environment
from test_telegram_publication_observation import ACK


@pytest.mark.asyncio
@pytest.mark.parametrize("guarded", [False, True])
async def test_cancel_drains_actual_codec_before_releasing_reader(reader_environment, monkeypatch, guarded):
    from app.telegram_bots import assignment_proposal_source as source

    e = reader_environment
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()
    original = source.validate_proposal_basis

    def held(*args):
        value = original(*args)
        entered.set()
        try:
            assert release.wait(10)
            return value
        finally:
            finished.set()

    monkeypatch.setattr(source, "validate_proposal_basis", held)
    task = asyncio.create_task(e.reader.read_proposal(e.record, e.link, guard=e.guard if guarded else None))
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        assert not finished.is_set()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert finished.is_set()
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["pages", "messages"])
async def test_shared_pagination_limits_do_not_mint_source(reader_environment, boundary):
    e = reader_environment
    size = 1 if boundary == "pages" else 200
    pages = 0

    async def respond(request):
        nonlocal pages
        pages += 1
        after = int(request.url.params.get("after_seq", "0"))
        rows = []
        for index in range(size):
            row = deepcopy(e.basis["messages"][0])
            row["seq"] = after + index + 1
            rows.append(row)
        return httpx.Response(200, headers=ACK, json={"data": rows, "has_more": True})

    e.responder = respond
    with pytest.raises(ValueError):
        await e.reader.read_proposal(e.record, e.link)
    assert pages == (32 if boundary == "pages" else 21)
    assert e.calls == 1
