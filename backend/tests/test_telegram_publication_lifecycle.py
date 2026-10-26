"""The supervisor retains the sole HTTP owner through publication cleanup."""

import asyncio

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging
from test_telegram_supervisor import build

pytestmark = pytest.mark.asyncio


async def test_supervisor_composes_callable_publications_without_an_input_consumer(db, monkeypatch):
    from app.telegram_bots.model_source import ModelSource
    from app.telegram_bots.publication import PublicationCoordinator
    from app.telegram_bots.publication_authority import PublicationAttemptAuthority

    e = build(db, monkeypatch)
    try:
        await e.supervisor.check()
        await e.supervisor.start()
        assert isinstance(e.supervisor.publications, PublicationCoordinator)
        assert isinstance(e.supervisor.publication_authority, PublicationAttemptAuthority)
        assert isinstance(e.supervisor.model_source, ModelSource)
        client = e.supervisor.thread_client
        await e.supervisor.begin_shutdown()
        assert not client.closed
        await e.supervisor.stop()
        assert client.closed
        assert e.normal.calls == e.updates.calls == []
    finally:
        await e.supervisor.stop()


async def test_final_publication_cleanup_retains_owner_and_listener_through_repeated_cancellation(db, monkeypatch):
    e = build(db, monkeypatch)
    entered, release = asyncio.Event(), asyncio.Event()
    first = second = None
    calls = []
    try:
        await e.supervisor.check()
        await e.supervisor.start()
        client = e.supervisor.thread_client
        original = e.supervisor.publications.stop

        async def retained_stop():
            calls.append("stop")
            await original()
            entered.set()
            await release.wait()

        monkeypatch.setattr(e.supervisor.publications, "stop", retained_stop)
        await e.supervisor.begin_shutdown()
        assert not calls and not client.closed
        first = asyncio.create_task(e.supervisor.stop())
        await asyncio.wait_for(entered.wait(), 3)
        first.cancel()
        second = asyncio.create_task(e.supervisor.stop())
        await asyncio.sleep(0)
        second.cancel()
        assert not client.closed and e.supervisor._subscribed
        release.set()
        for task in (first, second):
            with pytest.raises(asyncio.CancelledError):
                await task
        await e.supervisor.stop()
        assert client.closed and not e.supervisor._subscribed and calls == ["stop"]
    finally:
        release.set()
        await asyncio.gather(*(task for task in (first, second) if task is not None), return_exceptions=True)
        await e.supervisor.stop()


async def test_failed_publication_cleanup_retains_owner_for_a_retry(db, monkeypatch):
    e = build(db, monkeypatch)
    try:
        await e.supervisor.check()
        await e.supervisor.start()
        client = e.supervisor.thread_client
        original = e.supervisor.publications.stop
        calls = []

        async def retryable():
            calls.append(True)
            if len(calls) == 1:
                raise RuntimeError("synthetic cleanup failure")
            await original()

        monkeypatch.setattr(e.supervisor.publications, "stop", retryable)
        with pytest.raises(RuntimeError, match="^telegram-publication-cleanup-failed$"):
            await e.supervisor.stop()
        assert not client.closed and e.supervisor._subscribed
        await e.supervisor.stop()
        assert client.closed and not e.supervisor._subscribed and len(calls) == 2
    finally:
        await e.supervisor.stop()
