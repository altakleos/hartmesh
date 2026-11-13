"""Default supervisor installs notification owners at the startup boundary."""

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging
from test_telegram_supervisor import build
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow, TelegramWorkCursorRow


@pytest.mark.asyncio
async def test_default_supervisor_seeds_before_releasing_updates(work_pages, monkeypatch):
    e = work_pages
    lifecycle = build(e.db, monkeypatch)
    host = lifecycle.supervisor
    host.app, host.authority, host._app_config = e.http_app, e.live.authority, e.live.config
    host.thread_client = e.http_client
    tasks = []
    monkeypatch.setattr(host, "_periodic", lambda callback, interval, name, **options: tasks.append((callback, interval, name, options)))
    released, recovered = [], []

    class HeldPoll:
        pending_drop = False

        async def release_held(self):
            async with e.db.sessions() as reader:
                row = await reader.get(TelegramBotLinkRow, e.link.id)
                assert row.work_baseline_generation == row.binding_generation
                assert (await reader.get(TelegramWorkCursorRow, (e.link.id, e.work["id"]))).revision == 2
            assert host.work_callbacks and host.work_text_inputs and host.message_settings
            assert recovered
            released.append(True)

        def wake(self):
            pass

    try:
        await host.check()
        assert host.cause() is None
        assert host.links.work_baseline_seeder is not None
        async with e.db.sessions() as reader:
            assert (await reader.get(TelegramBotLinkRow, e.link.id)).work_baseline_generation is None
        assert host._pending_copy_decision() is True
        assert not tasks
        original = host.notifications.recover_inputs

        async def recover(phase, **options):
            assert host.send_allowed() and host._pending_copy_decision()
            assert not tasks
            await original(phase, **options)
            recovered.append(True)

        monkeypatch.setattr(host.notifications, "recover_inputs", recover)
        host._polls[e.link.id] = HeldPoll()
        await host._coordinate_copy_start()
        assert released and host.sender.proactive is True
        assert host._pending_copy_decision() is False
        selected = {name: (callback, interval, options) for callback, interval, name, options in tasks}
        assert {"work-events", "summaries", "work-acts", "notification-recovery"} <= selected.keys()
        assert selected["work-events"][2]["refused_too"] is True
        assert selected["work-events"][1] == host.config.producer.poll_seconds
        assert selected["summaries"][1] == 60
        assert selected["work-acts"][1] == 5
        assert await host.notifications.events.tick() == 0
        host._cause = "channel-service-failed"
        assert await host.notifications.before_send() == 0
        assert await host.notifications.summaries.tick() == 0
    finally:
        host._polls.clear()
        await host.stop()
    assert host.notifications._stopping is True


@pytest.mark.asyncio
async def test_notification_tasks_stop_effects_on_refusal_and_drain_on_shutdown(work_pages, monkeypatch):
    import asyncio
    from collections import defaultdict

    e = work_pages
    host = build(e.db, monkeypatch).supervisor
    host.app, host.authority, host._app_config, host.thread_client = e.http_app, e.live.authority, e.live.config, e.http_client
    queues, finished, calls = defaultdict(asyncio.Queue), defaultdict(asyncio.Event), []
    selected = {"work-events", "summaries", "work-acts", "notification-recovery", "sender"}
    periodic = host._periodic

    async def sleep(_delay):
        await queues[asyncio.current_task().get_name()].get()

    def register(operation, seconds, name, **options):
        if name not in selected:
            return

        async def tick():
            await operation()
            calls.append(name)
            finished[name].set()

        periodic(tick, seconds, name, **options)

    monkeypatch.setattr(host, "_sleep", sleep)
    monkeypatch.setattr(host, "_periodic", register)
    try:
        await host.check()
        assert not calls and not host.notifications.initialized
        await host._coordinate_copy_start()
        for name in sorted(selected):
            queues["telegram-" + name].put_nowait(None)
            await asyncio.wait_for(finished[name].wait(), 10)
        assert set(calls) == selected
        finished["work-events"].clear()
        before = len(calls)
        host._cause = "channel-service-failed"
        for name in selected:
            queues["telegram-" + name].put_nowait(None)
        await asyncio.wait_for(finished["work-events"].wait(), 10)
        assert calls[before:] == ["work-events"]
        await host.begin_shutdown()
        assert not host._background and not host._sweeps
        assert host.notifications._stopping
        assert not host.notifications._operations
    finally:
        await host.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("failed_owner", ["worker", "operation"])
@pytest.mark.parametrize("cancel", [False, True])
async def test_notification_shutdown_drains_after_failure(failed_owner, cancel):
    import asyncio
    from types import SimpleNamespace

    from app.telegram_bots.notification_runtime import NotificationRuntime

    runtime = NotificationRuntime(SimpleNamespace(links=None))
    entered, release, failed, completed = (asyncio.Event() for _ in range(4))

    async def retained():
        entered.set()
        await release.wait()
        completed.set()

    async def failure():
        await entered.wait()
        failed.set()
        raise RuntimeError("Unavailable metadata")

    held = asyncio.create_task(retained())
    runtime._operations.add(held)
    if failed_owner == "worker":
        runtime.worker = SimpleNamespace(stop=failure)
    else:
        runtime._operations.add(asyncio.create_task(failure()))
    stopping = asyncio.create_task(runtime.stop())
    try:
        await failed.wait()
        for _ in range(5):
            await asyncio.sleep(0)
        if cancel:
            stopping.cancel()
            await asyncio.sleep(0)
        assert not stopping.done(), "shutdown returned before its retained operation"
        release.set()
        results = await asyncio.gather(stopping, return_exceptions=True)
        assert completed.is_set()
        assert isinstance(results[0], asyncio.CancelledError) if cancel else results == [None]
    finally:
        release.set()
        await asyncio.gather(stopping, *runtime._operations, return_exceptions=True)
