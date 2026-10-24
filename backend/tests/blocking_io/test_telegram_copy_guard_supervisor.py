"""Copy-check file work is off-loop; Bot API and SQL owners keep one loop."""

import asyncio
import threading
from zoneinfo import ZoneInfo

import pytest
from _telegram_bot_test_support import RecordingRequest, update_payload
from _telegram_repository_test_support import bind_link
from _telegram_repository_test_support import db as db
from test_telegram_copy_guard_supervisor import conflict, environment, owned_link, poll_results, stop_environment, wait_until
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging

from app.telegram_bots.deployment import CopyGuard
from app.telegram_bots.inbox import InboxRepository

pytestmark = pytest.mark.asyncio


@pytest.fixture
def cold_probe(tmp_path):
    path = tmp_path / "copy-host-and-config"
    path.write_text("fixture-host", encoding="utf-8")
    return path


async def test_cold_host_and_configuration_callbacks_stay_off_gateway_loop(db, monkeypatch, cold_probe):
    from app.telegram_bots import supervisor as module

    await asyncio.to_thread(ZoneInfo, "America/New_York")
    e = environment(db, monkeypatch)
    owner = threading.get_ident()
    threads = []
    prepared = e.supervisor._app_config

    def read_host():
        threads.append(threading.get_ident())
        return cold_probe.read_text(encoding="utf-8")

    def read_config():
        threads.append(threading.get_ident())
        assert cold_probe.read_text(encoding="utf-8") == "fixture-host"
        return prepared

    monkeypatch.setattr(module, "get_app_config", read_config)
    e.supervisor._app_config = None
    e.supervisor.copy_guard = CopyGuard(db.sessions, host_reader=read_host, clock=e.clock.now)
    try:
        await e.supervisor.check()
        await e.supervisor.start()
        assert threads and all(thread != owner for thread in threads)
        assert e.supervisor.cause() is None and e.supervisor.send_allowed()
        assert e.supervisor.copy_guard.this_host_id == "fixture-host"
    finally:
        await stop_environment(e)


async def test_actual_sdk_false_decision_admission_and_confirmation_keep_original_loop(db, monkeypatch):
    import test_telegram_copy_guard_supervisor as fixtures

    await asyncio.to_thread(ZoneInfo, "America/New_York")
    owner = asyncio.get_running_loop()
    calls, accepted = [], []

    class LoopRequest(RecordingRequest):
        async def initialize(self):
            assert asyncio.get_running_loop() is owner
            await super().initialize()

        async def shutdown(self):
            assert asyncio.get_running_loop() is owner
            await super().shutdown()

        async def do_request(self, *args, **kwargs):
            assert asyncio.get_running_loop() is owner
            calls.append(True)
            return await super().do_request(*args, **kwargs)

    monkeypatch.setattr(fixtures, "RecordingRequest", LoopRequest)
    inbox = InboxRepository(db.sessions)

    async def handler(link, update):
        assert asyncio.get_running_loop() is owner
        await inbox.accept(link, update, sanitized_input={"text": "Synthetic input"})
        accepted.append(update.update_id)
        return True

    def script(link, normal, updates):
        updates.enqueue("getUpdates", result=[update_payload(80)])
        updates.enqueue("getUpdates", result=[], release=asyncio.Event())
        updates.enqueue("getUpdates", result=[])

    e = environment(db, monkeypatch, script=script, handler=handler)
    link, token = await owned_link(db, e, bot=9_000_002_001)
    link = await bind_link(db, link)
    try:
        await e.supervisor.check()
        await e.supervisor.start()
        await wait_until(lambda: e.supervisor.poll_task(link.id).offset == 81 and len(e.channels.created[link.id].updates.calls) == 2)
        assert accepted == [80] and e.supervisor.send_allowed()
        await stop_environment(e)
        assert e.channels.created[link.id].updates.calls[-1].parameters["timeout"] == 0
        assert calls
    finally:
        await stop_environment(e)


async def test_actual_broad_latch_and_shutdown_need_no_blocking_loop_work(db, monkeypatch):
    await asyncio.to_thread(ZoneInfo, "America/New_York")

    def script(link, normal, updates):
        conflict(normal, updates)

    e = environment(db, monkeypatch, script=script)
    links = []
    for i, owner in enumerate(["owner-a", "owner-b"]):
        link, token = await owned_link(db, e, bot=9_000_002_100 + i, owner=owner)
        links.append(link)
    await e.supervisor.check()
    starting = asyncio.create_task(e.supervisor.start())
    try:
        await poll_results(e, links)
        e.clock.advance(60)
        await asyncio.wait_for(starting, 4)
        assert e.supervisor.cause() == "polled-elsewhere"
        assert not e.supervisor.send_allowed()
        await stop_environment(e)
        assert all(record.normal.closed and record.updates.closed for record in e.channels.created.values())
    finally:
        await stop_environment(e)
        await asyncio.gather(starting, return_exceptions=True)
