"""Actual managed adapter and SQL fences for poll/probe conflict transitions."""

import asyncio
import secrets
from contextlib import asynccontextmanager
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
from _telegram_bot_test_support import ManualClock, RecordingRequest, update_payload
from _telegram_repository_test_support import add_link, bind_link, code_of, welcome
from _telegram_repository_test_support import db as db
from sqlalchemy import event
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from app.telegram_bots.polling import PollRecord, PollTask
from app.telegram_bots.producer import Outbox
from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow

pytestmark = pytest.mark.asyncio


@asynccontextmanager
async def managed_rig(db, *, start=True, owner="owner-a", bot=9_000_000_001):
    from app.telegram_bots.adapter import TelegramBotChannel

    request, updates, clock = RecordingRequest(), RecordingRequest(), ManualClock()
    link = await add_link(db, owner=owner, bot=bot)
    if start:
        request.enqueue_get_me(bot_id=link.bot_id)

    def request_factory(*, connection_pool_size, httpx_kwargs):
        return request if connection_pool_size == 8 else updates

    services = SimpleNamespace(
        send_allowed=lambda: True, links=db.links, config=db.config, outbox=Outbox(db.sessions), settings=db.settings, tls_context=object(), request_factory=request_factory, protect_notice=AsyncMock(return_value=False)
    )
    token = str(secrets.randbelow(900000) + 100000) + ":" + secrets.token_urlsafe(30)
    channel = TelegramBotChannel(link=link, token=token, supervisor=services, monotonic=clock.monotonic, sleep=clock.sleep, now=clock.now)
    if start:
        await channel.start()
    changes = []

    async def health(cause, holds):
        transition = task.health_transition.as_dict() if task.health_transition else None
        changes.append((cause, holds, transition))
        await db.links.set_health(link.id, cause, holds=holds, transition=transition, expected_token_revision=task.transport_revision)

    task = PollTask(channel, on_health=health, record=PollRecord(), clock=clock.now, sleep=clock.sleep)
    try:
        yield SimpleNamespace(channel=channel, task=task, request=request, updates=updates, clock=clock, link=link, changes=changes)
    finally:
        await channel.stop()


@pytest_asyncio.fixture
async def rig(db):
    async with managed_rig(db) as value:
        yield value


def conflict(rig):
    return rig.updates.enqueue("getUpdates", status=409, body={"ok": False, "error_code": 409, "description": "Conflict: synthetic competing poll"})


def webhook(rig, url=""):
    return rig.request.enqueue("getWebhookInfo", result={"url": url, "has_custom_certificate": False, "pending_update_count": 0})


async def test_committed_conflict_fence_survives_task_recreation_below_health_threshold(db, rig):
    old_code = code_of(await db.links.mint_code(rig.link))
    conflict(rig)
    webhook(rig)
    assert await rig.task.probe() == "conflict"
    current = await db.links.get(rig.link.id)
    assert current.connect_conflict_epoch == 1 and current.health == "ok"
    recreated = PollTask(rig.channel, on_health=AsyncMock(), record=PollRecord(), clock=rig.clock.now, sleep=rig.clock.sleep)
    assert recreated.transport_revision == rig.task.transport_revision
    with pytest.raises(ValueError, match="expired"):
        await db.links.bind(
            current, telegram_user_id=7_000_000_001, chat_id=7_000_000_001, first_name="Fixture", username=None, start_identity={"message_id": 10, "update_id": 20, "chat_id": 7_000_000_001}, hello_parts=welcome(), code=old_code
        )


async def test_failed_conflict_fence_retries_before_count_callback_or_another_request(db, rig, monkeypatch):
    conflict(rig)
    webhook(rig)
    original = db.links.record_conflict
    failure = AsyncMock(side_effect=OperationalError("synthetic fence unavailable", {}, None))
    monkeypatch.setattr(db.links, "record_conflict", failure)
    results = []
    rig.task.on_result = lambda: results.append(rig.task.record.conflicts) or False
    assert await rig.task.probe() == "network"
    assert rig.task.record.polls == 0 and results == []
    assert len(rig.updates.calls) == 1 and [call.endpoint for call in rig.request.calls] == ["getMe"]
    monkeypatch.setattr(db.links, "record_conflict", original)
    assert await rig.task.probe() == "conflict"
    assert len(rig.updates.calls) == 1 and results == [1]
    assert (await db.links.get(rig.link.id)).connect_conflict_epoch == 1


async def test_stale_conflict_cannot_invalidate_successor_code_or_publish_a_result(db, rig):
    release = asyncio.Event()
    response = rig.updates.enqueue("getUpdates", status=409, body={"ok": False, "error_code": 409, "description": "Conflict"}, release=release)
    running = asyncio.create_task(rig.task.probe())
    try:
        await asyncio.wait_for(response.started.wait(), 2)
        rotated = await db.links.store_token(rig.link.id, "wrapped-new-token", actor_id=rig.link.owner_user_id, expected_revision=rig.link.token_revision, token_changed=True)
        fresh_code = code_of(await db.links.mint_code(rotated))
        release.set()
        assert await asyncio.wait_for(running, 2) == "invalid-token"
        assert rig.task.record.polls == 0 and rig.changes == []
        current = await db.links.get(rig.link.id)
        assert current.connect_conflict_epoch == rotated.connect_conflict_epoch
        bound = await db.links.bind(
            current, telegram_user_id=7_000_000_001, chat_id=7_000_000_001, first_name="Fixture", username=None, start_identity={"message_id": 10, "update_id": 20, "chat_id": 7_000_000_001}, hello_parts=welcome(), code=fresh_code
        )
        assert bound.state == "connected"
    finally:
        running.cancel()
        await asyncio.gather(running, return_exceptions=True)


async def test_conflict_needs_an_arriving_409_and_ends_without_clearing_pause(db, rig):
    conflict(rig)
    webhook(rig)
    assert await rig.task.probe() == "conflict"
    rig.clock.advance(61)
    assert (await db.links.get(rig.link.id)).health == "ok"
    conflict(rig)
    assert await rig.task.probe() == "conflict"
    current = await db.links.get(rig.link.id)
    assert current.health == "conflict" and current.conflict_since == rig.clock.now()
    assert current.probe_until == rig.clock.now() + timedelta(hours=24)
    pause = current.acts_paused_since
    rig.updates.enqueue("getUpdates", result=[])
    assert await rig.task.probe() == "ok"
    current = await db.links.get(rig.link.id)
    assert current.health == "ok" and current.conflict_since is current.probe_until is current.webhook_host is None
    assert current.acts_paused_since == pause
    assert [call.endpoint for call in rig.request.calls].count("getWebhookInfo") == 1


async def test_webhook_conflict_is_immediate_and_probes_only_poll_after_webhook_is_empty(db, rig, caplog):
    conflict(rig)
    webhook(rig, "https://fixture.invalid/private-webhook-secret")
    assert await rig.task.probe() == "conflict"
    current = await db.links.get(rig.link.id)
    assert current.health == "conflict" and current.webhook_host == "fixture.invalid" and rig.task.record.webhook
    webhook(rig, "https://fixture.invalid/private-webhook-secret")
    assert await rig.task.probe() == "webhook"
    assert len(rig.updates.calls) == 1
    webhook(rig)
    rig.updates.enqueue("getUpdates", result=[])
    assert await rig.task.probe() == "ok"
    assert rig.task.record.polls == 2 and rig.task.record.successes == 1
    assert "private-webhook-secret" not in caplog.text


async def test_probe_deadline_expires_until_explicit_probe_renews_it(db, rig):
    conflict(rig)
    webhook(rig, "https://fixture.invalid/path")
    await rig.task.probe()
    original = await db.links.get(rig.link.id)
    rig.clock.advance(24 * 3600 + 1)
    running = asyncio.create_task(rig.task.run())
    try:
        for _ in range(20):
            await asyncio.sleep(0)
        assert len(rig.updates.calls) == 1
    finally:
        running.cancel()
        await asyncio.gather(running, return_exceptions=True)
    webhook(rig, "https://fixture.invalid/path")
    assert await rig.task.probe() == "webhook"
    renewed = await db.links.get(rig.link.id)
    assert renewed.conflict_since == original.conflict_since
    assert renewed.probe_until == rig.clock.now() + timedelta(hours=24)


@pytest.mark.parametrize("operation", ["confirm_offset", "drop_pending"])
async def test_control_requests_commit_every_current_conflict(db, rig, operation):
    rig.task.offset = 50
    conflict(rig)
    webhook(rig)
    await getattr(rig.task, operation)()
    assert (await db.links.get(rig.link.id)).connect_conflict_epoch == 1
    assert rig.channel.application.running
    assert "deleteWebhook" not in [call.endpoint for call in rig.request.calls]


async def test_current_success_guard_fences_late_old_adapter_even_during_last_ok_throttle(db, rig):
    rig.updates.enqueue("getUpdates", result=[])
    await rig.task.probe()
    previous = (rig.task.record.polls, rig.task.record.successes)
    release = asyncio.Event()
    response = rig.updates.enqueue("getUpdates", result=[update_payload(20)], release=release)
    admit = AsyncMock(return_value=True)
    rig.channel.handle_update = admit
    running = asyncio.create_task(rig.task.probe())
    try:
        await asyncio.wait_for(response.started.wait(), 2)
        rotated = await db.links.store_token(rig.link.id, "wrapped-new-token", actor_id=rig.link.owner_user_id, expected_revision=rig.link.token_revision, token_changed=True)
        release.set()
        assert await asyncio.wait_for(running, 2) == "invalid-token"
        assert (rig.task.record.polls, rig.task.record.successes) == previous
        assert not admit.called and (await db.links.get(rig.link.id)).last_ok_at == rotated.last_ok_at
    finally:
        running.cancel()
        await asyncio.gather(running, return_exceptions=True)


async def test_same_transport_can_admit_after_binding_generation_refresh(db, rig):
    bound = await bind_link(db, rig.link)
    admitted = []

    async def admit(value):
        admitted.append((value.update_id, rig.channel.link.binding_generation, rig.channel.transport_revision))
        return True

    rig.channel.handle_update = admit
    rig.updates.enqueue("getUpdates", result=[update_payload(20)])
    assert await rig.task.probe() == "ok"
    assert admitted == [(20, bound.binding_generation, bound.token_revision)]


async def test_actual_shared_two_401_checker_pauses_run_and_probe_until_rejection(db, rig):
    rig.updates.enqueue("getUpdates", status=401, body={"ok": False, "error_code": 401, "description": "Unauthorized"})
    for _ in range(2):
        rig.request.enqueue("getMe", status=401, body={"ok": False, "error_code": 401, "description": "Unauthorized"})
    probe = asyncio.create_task(rig.task.probe())
    running = None
    try:
        await rig.clock.wait_for_sleep()
        assert rig.clock.sleeps == [30]
        running = asyncio.create_task(rig.task.run())
        for _ in range(10):
            await asyncio.sleep(0)
        assert len(rig.updates.calls) == 1
        assert [call.endpoint for call in rig.request.calls].count("getMe") == 2
        rig.clock.advance(30)
        assert await asyncio.wait_for(probe, 5) == "invalid-token"
        await asyncio.wait_for(running, 5)
        assert (await db.links.get(rig.link.id)).health == "token_rejected"
        assert len(rig.updates.calls) == 1
        assert [call.endpoint for call in rig.request.calls].count("getMe") == 3
    finally:
        probe.cancel()
        await asyncio.gather(probe, return_exceptions=True)
        if running is not None:
            running.cancel()
            await asyncio.gather(running, return_exceptions=True)


async def test_actual_404_verification_keeps_token_and_polling_has_no_own_thirty_second_wait(db, rig):
    rig.updates.enqueue("getUpdates", status=404, body={"ok": False, "error_code": 404, "description": "Not Found"})
    rig.request.enqueue("getMe", status=404, body={"ok": False, "error_code": 404, "description": "Not Found"})
    assert await rig.task.probe() == "network"
    assert rig.clock.sleeps == [] and (await db.links.get(rig.link.id)).health == "ok"
    rig.updates.enqueue("getUpdates", result=[])
    assert await rig.task.probe() == "ok"
    assert (await db.links.get(rig.link.id)).health == "ok"


@pytest.mark.parametrize("clearing", [False, True])
async def test_health_commit_failure_retries_exact_transition_before_more_transport(db, rig, clearing):
    if clearing:
        conflict(rig)
        webhook(rig, "https://fixture.invalid/path")
        await rig.task.probe()
        webhook(rig)
        rig.updates.enqueue("getUpdates", result=[])
    else:
        conflict(rig)
        webhook(rig, "https://fixture.invalid/path")
    committed = asyncio.Event()
    original = rig.task.on_health
    attempts = []

    async def health(cause, holds):
        target = cause == "conflict" and holds == (not clearing)
        if target:
            attempts.append((holds, rig.task.health_transition))
        await original(cause, holds)
        if target:
            committed.set()

    rig.task.on_health = health
    failed = False

    def fail_once(session):
        nonlocal failed
        if not failed and session.get_bind() is db.engine.sync_engine and any(isinstance(row, TelegramBotLinkRow) and row.id == rig.link.id and row.health == ("ok" if clearing else "conflict") for row in session.identity_map.values()):
            failed = True
            raise OperationalError("synthetic health commit failure", {}, None)

    event.listen(Session, "before_commit", fail_once)
    running = None
    try:
        try:
            await rig.task.probe()
        except OperationalError:
            pass
        assert failed and not committed.is_set()
        expected = (await db.links.get(rig.link.id)).health
        assert expected == ("conflict" if clearing else "ok")
        # A later API result must not erase the known, uncommitted transition.
        stalled = rig.updates.enqueue("getUpdates", result=[], release=asyncio.Event())
        rig.clock.advance(5)
        running = asyncio.create_task(rig.task.run())
        await asyncio.wait_for(committed.wait(), 3)
        assert len(attempts) == 2 and attempts[0] == attempts[1]
        current = await db.links.get(rig.link.id)
        assert current.health == ("ok" if clearing else "conflict")
        if not clearing:
            assert current.conflict_since == attempts[0][1].conflict_since
            assert current.probe_until == attempts[0][1].probe_until
            assert not stalled.started.is_set()
    finally:
        event.remove(Session, "before_commit", fail_once)
        if running is not None:
            running.cancel()
            await asyncio.gather(running, return_exceptions=True)


async def test_drop_fence_and_repeated_health_commit_failures_stay_owned_until_drop_completes(db, rig):
    conflict(rig)
    webhook(rig, "https://fixture.invalid/path")
    failed_fences = 0
    failed_health = 0
    attempts = []
    committed = asyncio.Event()
    original = rig.task.on_health

    async def health(cause, holds):
        if cause == "conflict" and holds:
            attempts.append(rig.task.health_transition)
        await original(cause, holds)
        if cause == "conflict" and holds:
            committed.set()

    rig.task.on_health = health

    def fail_commits(session):
        nonlocal failed_fences, failed_health
        if session.get_bind() is not db.engine.sync_engine:
            return
        for row in session.identity_map.values():
            if not isinstance(row, TelegramBotLinkRow) or row.id != rig.link.id:
                continue
            if row.health == "conflict" and failed_health < 2:
                failed_health += 1
                raise OperationalError("synthetic health commit unavailable", {}, None)
            if row.connect_conflict_epoch == 1 and not failed_fences:
                failed_fences += 1
                raise OperationalError("synthetic fence commit unavailable", {}, None)

    event.listen(Session, "before_commit", fail_commits)
    running = None
    try:
        await rig.task.drop_pending()
        assert failed_fences == 1 and (await db.links.get(rig.link.id)).connect_conflict_epoch == 0
        assert rig.task.record.polls == 0
        dropped = rig.updates.enqueue("getUpdates", result=[update_payload(40)])
        running = asyncio.create_task(rig.task.run())
        await rig.clock.wait_for_sleep()
        assert not running.done() and failed_health == 1
        assert (await db.links.get(rig.link.id)).connect_conflict_epoch == 1
        assert not dropped.started.is_set() and not rig.task._operation.locked()
        # A waiting link owns neither the database writer nor another poller.
        async with managed_rig(db, owner="owner-b", bot=9_000_000_002) as other:
            other.updates.enqueue("getUpdates", result=[])
            assert await other.task.probe() == "ok"
            assert other.task.record.successes == 1
        rig.clock.advance(1)
        await rig.clock.wait_for_sleep(2)
        assert not running.done() and failed_health == 2 and not dropped.started.is_set()
        assert (await db.links.get(rig.link.id)).health == "ok"
        rig.clock.advance(1.5)
        await asyncio.wait_for(committed.wait(), 3)
        await rig.clock.wait_for_sleep(3)
        assert not dropped.started.is_set() and not rig.task._operation.locked()
        rig.clock.advance(rig.clock.sleeps[-1])
        await asyncio.wait_for(dropped.started.wait(), 3)
        await rig.clock.wait_for_sleep(4)
        assert len(attempts) == 3 and attempts[0] is attempts[1] is attempts[2]
        assert (await db.links.get(rig.link.id)).connect_conflict_epoch == 1
        assert [call.parameters["offset"] for call in rig.updates.calls] == [-1, -1]
        assert rig.task.offset == 41 and rig.task.record.polls == 0
        # Once conflict probing permits transport, the completed drop's offset
        # is retained for the first ordinary request.
        rig.clock.advance(30)
        webhook(rig)
        ordinary = rig.updates.enqueue("getUpdates", result=[], release=asyncio.Event())
        await asyncio.wait_for(ordinary.started.wait(), 3)
        assert rig.updates.calls[-1].parameters["offset"] == 41
    finally:
        event.remove(Session, "before_commit", fail_commits)
        if running is not None:
            running.cancel()
            await asyncio.gather(running, return_exceptions=True)


async def test_startup_conflict_renewal_keeps_exact_health_intent_after_sql_failure(db, rig):
    transition = {"conflict_since": rig.clock.now() - timedelta(hours=2), "probe_until": rig.clock.now() + timedelta(hours=22), "webhook_host": "fixture.invalid"}
    await db.links.set_health(rig.link.id, "conflict", holds=True, transition=transition, expected_token_revision=rig.link.token_revision)
    await rig.channel.refresh_link()
    attempts = []
    committed = asyncio.Event()

    async def health(cause, holds):
        captured = task.health_transition
        attempts.append(captured)
        await db.links.set_health(rig.link.id, cause, holds=holds, transition=captured.as_dict(), expected_token_revision=task.transport_revision)
        committed.set()

    task = PollTask(rig.channel, on_health=health, record=PollRecord(), clock=rig.clock.now, sleep=rig.clock.sleep)
    failed = False

    def fail_once(session):
        nonlocal failed
        if (
            not failed
            and session.get_bind() is db.engine.sync_engine
            and any(isinstance(row, TelegramBotLinkRow) and row.id == rig.link.id and row.probe_until == rig.clock.now() + timedelta(hours=24) for row in session.identity_map.values())
        ):
            failed = True
            raise OperationalError("synthetic startup health commit unavailable", {}, None)

    event.listen(Session, "before_commit", fail_once)
    running = asyncio.create_task(task.run())
    try:
        await rig.clock.wait_for_sleep()
        assert failed and not running.done() and not task._operation.locked()
        assert (await db.links.get(rig.link.id)).probe_until == transition["probe_until"]
        rig.clock.advance(1)
        await asyncio.wait_for(committed.wait(), 3)
        assert len(attempts) == 2 and attempts[0] is attempts[1]
        current = await db.links.get(rig.link.id)
        assert current.conflict_since == transition["conflict_since"]
        assert current.probe_until == attempts[0].probe_until
        assert not rig.updates.calls
    finally:
        event.remove(Session, "before_commit", fail_once)
        running.cancel()
        await asyncio.gather(running, return_exceptions=True)


async def test_run_retries_top_level_sql_guard_before_any_transport(db, rig, monkeypatch):
    original = rig.channel.refresh_link
    reads = 0

    async def refresh():
        nonlocal reads
        reads += 1
        if reads == 1:
            raise OperationalError("synthetic current guard unavailable", {}, None)
        return await original()

    monkeypatch.setattr(rig.channel, "refresh_link", refresh)
    sent = rig.updates.enqueue("getUpdates", result=[], release=asyncio.Event())
    running = asyncio.create_task(rig.task.run())
    try:
        await rig.clock.wait_for_sleep()
        assert not running.done() and not sent.started.is_set() and not rig.task._operation.locked()
        rig.clock.advance(1)
        await asyncio.wait_for(sent.started.wait(), 3)
    finally:
        running.cancel()
        await asyncio.gather(running, return_exceptions=True)


@pytest.mark.parametrize("operation", ["run", "probe"])
async def test_token_rotation_after_committed_success_fences_counters_callback_and_admission(db, rig, operation, monkeypatch):
    from app.telegram_bots.links import BotLinkRepository

    original = rig.channel.record_poll_success
    published, release = asyncio.Event(), asyncio.Event()

    async def success(**kwargs):
        result = await original(**kwargs)
        published.set()
        await release.wait()
        return result

    monkeypatch.setattr(rig.channel, "record_poll_success", success)
    results = []
    rig.task.on_result = lambda: results.append(rig.task.record.polls) or False
    admit = AsyncMock(return_value=True)
    rig.channel.handle_update = admit
    rig.updates.enqueue("getUpdates", result=[update_payload(50)])
    running = asyncio.create_task(getattr(rig.task, operation)())
    try:
        await asyncio.wait_for(published.wait(), 3)
        successor = BotLinkRepository(db.sessions, db.connections, db.config)
        await successor.store_token(rig.link.id, "wrapped-new-token", actor_id=rig.link.owner_user_id, expected_revision=rig.link.token_revision, token_changed=True)
        release.set()
        result = await asyncio.wait_for(running, 3)
        assert result == ("invalid-token" if operation == "probe" else None)
        assert rig.task.record == PollRecord() and results == [] and not admit.called
        assert rig.task.offset is None
    finally:
        running.cancel()
        await asyncio.gather(running, return_exceptions=True)


async def test_actual_initialization_long_rate_limit_waits_full_deadline_before_retry_and_poll(db, monkeypatch):
    monkeypatch.setenv("PTB_TIMEDELTA", "1")
    async with managed_rig(db, start=False) as cold:
        limited = cold.request.enqueue("getMe", status=429, body={"ok": False, "error_code": 429, "description": "Too Many Requests", "parameters": {"retry_after": 70}})
        initialized = cold.request.enqueue_get_me(bot_id=cold.link.bot_id)
        polled = cold.updates.enqueue("getUpdates", result=[], release=asyncio.Event())
        running = asyncio.create_task(cold.task.run())
        try:
            await cold.clock.wait_for_sleep()
            assert limited.started.is_set() and not running.done()
            assert cold.request.closed and cold.updates.closed
            assert not initialized.started.is_set() and not polled.started.is_set()
            assert cold.clock.sleeps == [70] and not cold.task._operation.locked()
            cold.clock.advance(69)
            for _ in range(10):
                await asyncio.sleep(0)
            assert not initialized.started.is_set() and not polled.started.is_set()
            cold.clock.advance(1)
            await asyncio.wait_for(polled.started.wait(), 3)
            assert initialized.started.is_set() and cold.channel.application.running
        finally:
            running.cancel()
            await asyncio.gather(running, return_exceptions=True)


@pytest.mark.parametrize("guard", ["fence", "webhook"])
async def test_sql_guard_after_committed_conflict_does_not_repeat_epoch_or_result(db, rig, monkeypatch, guard):
    original_refresh = rig.channel.refresh_link
    original_fence = db.links.record_conflict
    original_webhook = rig.channel.bot.get_webhook_info
    armed = False
    failed = False

    async def refresh():
        nonlocal failed
        if armed and not failed:
            failed = True
            raise OperationalError("synthetic post-commit guard unavailable", {}, None)
        return await original_refresh()

    async def fence(*args, **kwargs):
        nonlocal armed
        result = await original_fence(*args, **kwargs)
        if guard == "fence":
            armed = True
        return result

    async def info(*args, **kwargs):
        nonlocal armed
        result = await original_webhook(*args, **kwargs)
        if guard == "webhook":
            armed = True
        return result

    monkeypatch.setattr(rig.channel, "refresh_link", refresh)
    monkeypatch.setattr(db.links, "record_conflict", fence)
    monkeypatch.setattr(type(rig.channel.bot), "get_webhook_info", lambda self, *args, **kwargs: info(*args, **kwargs))
    conflict(rig)
    webhook(rig)
    results = []
    rig.task.on_result = lambda: results.append(rig.task.record.conflicts) or False
    assert await rig.task.probe() == "network"
    assert failed and (await db.links.get(rig.link.id)).connect_conflict_epoch == 1
    assert rig.task.record.polls == 0 and results == []
    next_poll = rig.updates.enqueue("getUpdates", result=[], release=asyncio.Event())
    running = asyncio.create_task(rig.task.run())
    try:
        await asyncio.wait_for(next_poll.started.wait(), 3)
        assert (await db.links.get(rig.link.id)).connect_conflict_epoch == 1
        assert rig.task.record.conflicts == 1 and results == [1]
        assert [call.endpoint for call in rig.request.calls].count("getWebhookInfo") == 1
    finally:
        running.cancel()
        await asyncio.gather(running, return_exceptions=True)


@pytest.mark.parametrize("phase", ["initialize", "poll"])
async def test_known_rate_deadline_survives_sql_guard_failure_before_any_more_transport(db, monkeypatch, phase):
    monkeypatch.setenv("PTB_TIMEDELTA", "1")
    async with managed_rig(db, start=phase == "poll") as current:
        request = current.request if phase == "initialize" else current.updates
        limited = request.enqueue("getMe" if phase == "initialize" else "getUpdates", status=429, body={"ok": False, "error_code": 429, "description": "Too Many Requests", "parameters": {"retry_after": 70}})
        initialized = current.request.enqueue_get_me(bot_id=current.link.bot_id) if phase == "initialize" else None
        polled = current.updates.enqueue("getUpdates", result=[], release=asyncio.Event())
        failed = False

        def fail_guard(connection, cursor, statement, parameters, context, executemany):
            nonlocal failed
            if limited.started.is_set() and not failed and "FROM telegram_bot_links" in statement:
                failed = True
                raise OperationalError("synthetic post-rate SQL guard unavailable", {}, None)

        event.listen(db.engine.sync_engine, "before_cursor_execute", fail_guard)
        running = asyncio.create_task(current.task.run())
        try:
            await current.clock.wait_for_sleep()
            assert failed and not running.done() and current.clock.sleeps == [1]
            assert not polled.started.is_set()
            current.clock.advance(1)
            await current.clock.wait_for_sleep(2)
            remaining = 69 if phase == "initialize" else 69.5
            assert current.clock.sleeps[-1] == remaining
            assert not polled.started.is_set() and not current.task._operation.locked()
            if initialized is not None:
                assert not initialized.started.is_set()
            current.clock.advance(remaining - 0.25)
            for _ in range(10):
                await asyncio.sleep(0)
            assert not polled.started.is_set()
            if initialized is not None:
                assert not initialized.started.is_set()
            current.clock.advance(0.25)
            await asyncio.wait_for(polled.started.wait(), 3)
        finally:
            event.remove(db.engine.sync_engine, "before_cursor_execute", fail_guard)
            running.cancel()
            await asyncio.gather(running, return_exceptions=True)


@pytest.mark.parametrize("reason", ["webhook", "grace"])
async def test_conflict_health_intent_is_captured_before_post_result_sql_guard(db, rig, reason):
    if reason == "grace":
        conflict(rig)
        webhook(rig)
        assert await rig.task.probe() == "conflict"
        rig.clock.advance(60)
    conflict(rig)
    if reason == "webhook":
        webhook(rig, "https://fixture.invalid/path")
    observed = rig.clock.now()
    expected = 1 if reason == "webhook" else 2
    failed = False
    armed = False
    results = []

    def result():
        nonlocal armed
        results.append(rig.task.record.conflicts)
        armed = True
        return False

    def fail_guard(connection, cursor, statement, parameters, context, executemany):
        nonlocal failed
        if armed and not failed and "FROM telegram_bot_links" in statement:
            failed = True
            raise OperationalError("synthetic post-result SQL guard unavailable", {}, None)

    rig.task.on_result = result
    attempted, committed, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    original = rig.task.on_health
    transitions = []

    async def health(cause, holds):
        transitions.append(rig.task.health_transition)
        attempted.set()
        await release.wait()
        await original(cause, holds)
        committed.set()

    rig.task.on_health = health
    event.listen(db.engine.sync_engine, "before_cursor_execute", fail_guard)
    running = None
    watches = []
    try:
        assert await rig.task.probe() == "network"
        assert failed and (await db.links.get(rig.link.id)).health == "ok"
        assert rig.task.record.conflicts == expected and results == [expected]
        next_poll = rig.updates.enqueue("getUpdates", result=[], release=asyncio.Event())
        rig.clock.advance(5)
        running = asyncio.create_task(rig.task.run())
        watches = [asyncio.create_task(attempted.wait()), asyncio.create_task(next_poll.started.wait())]
        await asyncio.wait(watches, timeout=3, return_when=asyncio.FIRST_COMPLETED)
        assert attempted.is_set() and not next_poll.started.is_set()
        assert len(transitions) == 1
        transition = transitions[0]
        assert transition.conflict_since == observed and transition.probe_until == observed + timedelta(hours=24)
        assert (await db.links.get(rig.link.id)).connect_conflict_epoch == expected
        release.set()
        await asyncio.wait_for(committed.wait(), 3)
        await rig.clock.wait_for_sleep()
        saved = await db.links.get(rig.link.id)
        assert saved.health == "conflict" and saved.probe_until == transition.probe_until
        assert saved.connect_conflict_epoch == expected and rig.task.record.conflicts == expected and results == [expected]
        assert not next_poll.started.is_set()
    finally:
        event.remove(db.engine.sync_engine, "before_cursor_execute", fail_guard)
        if running is not None:
            running.cancel()
        for watch in watches:
            watch.cancel()
        await asyncio.gather(*watches, *([running] if running is not None else []), return_exceptions=True)
