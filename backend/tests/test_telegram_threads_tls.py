"""Stop owns the real pending startup TLS worker before any SDK owner exists."""

import asyncio
import threading

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging
from test_telegram_supervisor import build

pytestmark = pytest.mark.asyncio


async def test_stop_during_actual_tls_preparation_drains_worker_and_prevents_late_client(db, monkeypatch):
    from app.telegram_bots import gateway_client
    from app.telegram_bots import supervisor as module

    entered, release, finished = threading.Event(), threading.Event(), threading.Event()
    actual_tls, actual_client = module.httpx.create_ssl_context, gateway_client.GatewayClientOwner
    constructions = []

    def tls(*args, **kwargs):
        context = actual_tls(*args, **kwargs)
        entered.set()
        release.wait(10)
        finished.set()
        return context

    def construct(**kwargs):
        constructions.append(True)
        return actual_client(**kwargs)

    monkeypatch.setattr(module.httpx, "create_ssl_context", tls)
    monkeypatch.setattr(gateway_client, "GatewayClientOwner", construct)
    e = build(db, monkeypatch)
    starting, stopping, second = None, None, None
    try:
        await e.supervisor.check()
        starting = asyncio.create_task(e.supervisor.start())
        assert await asyncio.to_thread(entered.wait, 3)
        stopping = asyncio.create_task(e.supervisor.stop())
        while not e.supervisor._stopping:
            await asyncio.sleep(0)
        # A blocked native worker must retain the canceled start owner; a
        # shielded timeout observes it without canceling that owner again.
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(asyncio.shield(starting), 0.1)
        assert not e.supervisor._stop_task.done() and not finished.is_set()
        stopping.cancel()
        second = asyncio.create_task(e.supervisor.stop())
        await asyncio.sleep(0)
        second.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await starting
        await asyncio.gather(stopping, second, return_exceptions=True)
        await e.supervisor.stop()
        assert finished.is_set()
        assert not constructions and e.supervisor.thread_client is None
        assert not e.normal.calls and not e.updates.calls
    finally:
        release.set()
        await asyncio.gather(*(task for task in (starting, stopping, second) if task is not None), return_exceptions=True)
        await e.supervisor.stop()
