"""Cold lifecycle work uses the original loop without synchronous file reads."""

import asyncio
import threading
import traceback
from zoneinfo import ZoneInfo

import httpx
import pytest
from _telegram_repository_test_support import db as db
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging
from test_telegram_supervisor import build, running_one


@pytest.fixture
def trace_probe(tmp_path):
    path = tmp_path / "cold-stack.txt"
    path.write_text("cold traceback source", encoding="utf-8")
    return path


@pytest.mark.asyncio
async def test_actual_health_client_constructs_without_cold_ca_reads_on_loop(db, monkeypatch):
    owner = asyncio.get_running_loop()
    requests, clients = [], []

    class HealthClient(httpx.AsyncClient):
        def __init__(self, **kwargs):
            # Keep the actual default transport constructor, including TLS.
            # Only GET is intercepted, so no socket can leave the test.
            super().__init__(**kwargs)
            clients.append(self)

        async def get(self, url, **kwargs):
            assert asyncio.get_running_loop() is owner
            requests.append(url)
            return httpx.Response(200)

    e = build(db, monkeypatch)
    e.supervisor._health_client_factory = HealthClient
    try:
        await e.supervisor.check()
        await e.supervisor.start()
        assert e.supervisor.cause() is None
        assert requests == ["http://127.0.0.1:8001/health"]
        assert clients and all(client.is_closed for client in clients)
        assert e.normal.calls == e.updates.calls == []
    finally:
        await e.supervisor.stop()


@pytest.mark.asyncio
async def test_actual_poll_death_formats_cold_stack_off_loop(db, monkeypatch, trace_probe):
    await asyncio.to_thread(ZoneInfo, "America/New_York")
    owner_thread = threading.get_ident()
    threads = []
    original = traceback.format_tb

    def cold_format(tb, *args, **kwargs):
        trace_probe.read_text(encoding="utf-8")
        threads.append(threading.get_ident())
        return original(tb, *args, **kwargs)

    monkeypatch.setattr(traceback, "format_tb", cold_format)
    e, link, held = await running_one(db, monkeypatch)

    def unexpected():
        raise RuntimeError("Offline result callback failure")

    # Result callbacks are runtime code: unlike a refused update admission,
    # an unexpected callback failure terminates the actual poll task.
    e.supervisor.poll_task(link.id).on_result = unexpected
    try:
        held.release.set()

        async def observed():
            while not threads or not e.supervisor._deaths[(link.id, link.token_revision)]:
                await asyncio.sleep(0)

        await asyncio.wait_for(observed(), 3)
        assert all(thread != owner_thread for thread in threads)
        assert (await db.links.get(link.id)).health == "ok"
        assert len(e.supervisor._deaths[(link.id, link.token_revision)]) == 1
    finally:
        await e.supervisor.stop()
