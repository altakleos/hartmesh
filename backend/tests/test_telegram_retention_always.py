"""What is kept has a promised end, and that holds whether or not Telegram runs."""

import asyncio
import logging
import secrets
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
import pytest_asyncio
from _telegram_bot_test_support import ManualClock
from _telegram_repository_test_support import add_link, rows
from _telegram_repository_test_support import db as db
from test_telegram_copy_guard_supervisor import environment, stop_environment
from test_telegram_supervisor import Health, credential

from app.channels.message_bus import MessageBus
from app.telegram_bots import supervisor as module
from app.telegram_bots.config import TelegramBotsConfig
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow, TelegramPendingFileRow

pytestmark = pytest.mark.asyncio

SWEPT = (TelegramInboundMarkRow, TelegramOutboxRow, TelegramPendingFileRow, TelegramCallbackRow)
SWEEP_TASKS = {"telegram-retention", "telegram-callback-sweep", "telegram-pending-file-sweep"}
GOOD = {"HARTMESH_PROVIDER_KEYS_SECRET": secrets.token_urlsafe(48)}
REFUSED = {
    "off": dict(config=TelegramBotsConfig(enabled=False), environ=GOOD),
    "secret-missing": dict(config=TelegramBotsConfig(), environ={}),
    "secret-invalid": dict(config=TelegramBotsConfig(), environ={"HARTMESH_PROVIDER_KEYS_SECRET": "short"}),
    "host-changed": dict(config=TelegramBotsConfig(), environ=GOOD, verdict="host_changed"),
    "polled-elsewhere": dict(config=TelegramBotsConfig(), environ=GOOD, verdict="polled_elsewhere"),
}


@pytest_asyncio.fixture(autouse=True)
async def retention_wait_diagnostics(request, capsys, caplog):
    """Keep coroutine waits visible when CI's thread dump only shows an idle loop."""
    phase = SimpleNamespace(name="setup")

    async def watch():
        while True:
            await asyncio.sleep(60)
            with capsys.disabled():
                print(f"\nRetention wait: {request.node.nodeid}; phase={phase.name}", flush=True)
                for task in asyncio.all_tasks():
                    if task is asyncio.current_task():
                        continue
                    # No task names, locals, object reprs or exception messages: those
                    # may carry caller input. Only code locations describe the wait.
                    current, seen = task.get_coro(), set()
                    locations = []
                    while current is not None and id(current) not in seen:
                        seen.add(id(current))
                        frame = getattr(current, "cr_frame", None) or getattr(current, "gi_frame", None)
                        if frame is not None:
                            locations.append(f"{frame.f_code.co_filename}:{frame.f_lineno}:{frame.f_code.co_name}")
                        current = getattr(current, "cr_await", None) or getattr(current, "gi_yieldfrom", None)
                    print("await: " + " -> ".join(locations), flush=True)
                failures = sum("copy_latch_write_failed" in record.getMessage() for record in caplog.records)
                print(f"copy_latch_write_failed count={failures}", flush=True)

    task = asyncio.create_task(watch())
    try:
        yield phase
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


class Verdict:
    this_host_id = "fixture-host"

    def __init__(self, verdict):
        self.verdict = verdict

    async def at_start(self):
        return self.verdict


def refused_supervisor(db, monkeypatch, clock, *, config, environ, verdict=None):
    """As the Gateway builds one: no injected sessions, and the real start-up checks."""
    monkeypatch.setattr(module, "get_session_factory", lambda: db.sessions)
    monkeypatch.setattr("app.telegram_bots.deployment.is_auth_disabled", lambda: False)
    manager = SimpleNamespace(connection_repo=db.connections, gateway_url="http://127.0.0.1:8001", langgraph_url="http://127.0.0.1:8001/api")
    return module.TelegramSupervisor(
        SimpleNamespace(state=SimpleNamespace()),
        config=config,
        channel_service=SimpleNamespace(bus=MessageBus(), manager=manager),
        app_config=SimpleNamespace(channel_connections=SimpleNamespace(enabled=True, require_bound_identity=True), run_events=SimpleNamespace(backend="db")),
        environ=environ,
        copy_guard=None if verdict is None else Verdict(verdict),
        clock=clock.now,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )


async def seed_aged_rows(db, link, now):
    old, chat = now - timedelta(days=40), link.chat_id or 7_000_000_001
    generation = link.binding_generation
    async with db.sessions() as session, session.begin():
        session.add(
            TelegramInboundMarkRow(
                link_id=link.id, message_id=1, update_id=1, chat_id=chat, binding_generation=generation, accepted_at=old, state="done", closed_at=old, route_complete=True, sanitized_input={"type": "text", "text": "something private"}
            )
        )
        session.add(
            TelegramOutboxRow(
                id=uuid4().hex,
                link_id=link.id,
                owner_user_id=link.owner_user_id,
                dedupe_key="old",
                class_="reply",
                binding_generation=generation,
                payload={"visible_text": "an old answer"},
                parts=[],
                state="sent",
                not_before=old,
                closed_at=old,
                created_at=old,
            )
        )
        session.add(
            TelegramPendingFileRow(
                link_id=link.id,
                message_id=2,
                chat_id=chat,
                binding_generation=generation,
                file_id="file",
                file_unique_id="unique",
                filename="private-name.pdf",
                mime_type="application/pdf",
                size=1,
                is_image=False,
                created_at=now - timedelta(days=3),
            )
        )
        session.add(TelegramCallbackRow(id=uuid4().hex, group_id=uuid4().hex, link_id=link.id, chat_id=chat, binding_generation=generation, kind="noop", payload={}, consumed_at=old, closed_reason="done", created_at=old))


async def swept_all(db):
    for _ in range(500):
        left = {table.__name__: len(await rows(db, table)) for table in SWEPT}
        if not any(left.values()):
            return
        await asyncio.sleep(0.01)
    raise AssertionError(left)


async def line_written(caplog, event):
    """The rows go a moment before the sweep says so."""
    for _ in range(500):
        if any(event in record.getMessage() for record in caplog.records):
            return
        await asyncio.sleep(0.01)
    raise AssertionError(event)


@pytest.mark.parametrize("cause", REFUSED)
async def test_rows_reach_their_end_while_telegram_is_refused(db, monkeypatch, caplog, cause):
    clock = ManualClock()
    link = await add_link(db)
    await seed_aged_rows(db, link, clock.now())
    supervisor = refused_supervisor(db, monkeypatch, clock, **REFUSED[cause])
    try:
        with caplog.at_level(logging.INFO, logger="app.telegram_bots.supervisor"):
            await supervisor.start()
            assert supervisor.cause() == cause
            await clock.wait_for_sleep(3)
            clock.advance(3600)
            await swept_all(db)
            await line_written(caplog, "retention_sweep")
        assert supervisor._background == set()
    finally:
        await supervisor.stop()
    assert not SWEEP_TASKS & {task.get_name() for task in asyncio.all_tasks()}


async def test_nothing_is_started_where_nothing_is_stored(db, monkeypatch):
    supervisor = refused_supervisor(db, monkeypatch, ManualClock(), **REFUSED["off"])
    monkeypatch.setattr(module, "get_session_factory", lambda: None)
    await supervisor.start()
    assert not SWEEP_TASKS & {task.get_name() for task in asyncio.all_tasks()}
    await supervisor.stop()


@pytest.mark.parametrize("cause", ["gateway-not-ready", "channel-service-failed"])
async def test_a_start_that_goes_wrong_stops_the_bots_and_not_the_sweeps(db, monkeypatch, cause):
    """The Gateway serves on, so what is stored still owes its end until that stops."""
    e = environment(db, monkeypatch, health=Health(result=False))
    failing = cause == "channel-service-failed"
    if failing:
        monkeypatch.setattr(e.supervisor, "_wait_health", AsyncMock(side_effect=OSError("down")))
    link, _token = await credential(db, e.env)
    await seed_aged_rows(db, link, e.clock.now())
    try:
        starting = asyncio.create_task(e.supervisor.start())
        await e.clock.wait_for_sleep(3 if failing else 4)  # The health wait is the fourth.
        e.clock.advance(60)
        await asyncio.wait_for(starting, 5)
        assert e.supervisor.cause() == cause
        e.clock.advance(3600)
        await swept_all(db)
    finally:
        await stop_environment(e)
    assert not SWEEP_TASKS & {task.get_name() for task in asyncio.all_tasks()}


async def test_a_copy_found_while_running_stops_the_bots_and_not_the_sweeps(db, monkeypatch, retention_wait_diagnostics):
    e = environment(db, monkeypatch)
    link, _token = await credential(db, e.env)
    await seed_aged_rows(db, link, e.clock.now())
    try:
        retention_wait_diagnostics.name = "start"
        await e.supervisor.start()
        retention_wait_diagnostics.name = "latch"
        await e.supervisor._latch_copy_conflict()
        retention_wait_diagnostics.name = "sweep"
        assert e.supervisor.cause() == "polled-elsewhere"
        e.clock.advance(3600)
        await swept_all(db)
    finally:
        retention_wait_diagnostics.name = "stop"
        await stop_environment(e)
    assert not SWEEP_TASKS & {task.get_name() for task in asyncio.all_tasks()}
