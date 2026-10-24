"""Pinned socket-free polling with explicit batch, clock and request ownership."""

import asyncio
import logging
from copy import copy
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
from _telegram_bot_test_support import ManualClock, RecordingRequest, make_update, offline_bot, update_payload
from sqlalchemy.exc import OperationalError
from telegram.error import Conflict, NetworkError, TimedOut

from app.telegram_bots.polling import ALLOWED, PollRecord, PollTask

pytestmark = pytest.mark.asyncio


class Links:
    def __init__(self):
        self.current = SimpleNamespace(id="a" * 32, bot_id=123456, token_revision=1, binding_generation=1, state="connected", health="ok", last_ok_at=None, conflict_since=None, probe_until=None, webhook_host=None)
        self.fences = 0
        self.fence_error = None
        self.facts = []

    async def get(self, identity):
        return copy(self.current) if self.current is not None else None

    async def update_if_current(self, identity, *, token_revision, **facts):
        if self.current is None or self.current.token_revision != token_revision:
            return None
        self.facts.append(facts)
        for key, value in facts.items():
            setattr(self.current, key, value)
        return copy(self.current)

    async def record_conflict(self, identity, *, token_revision):
        if self.fence_error:
            raise self.fence_error
        if self.current is None or self.current.token_revision != token_revision:
            return False
        self.fences += 1
        return True


class Channel:
    def __init__(self, bot, clock):
        self.services = SimpleNamespace(links=Links(), config=SimpleNamespace(conflict_probe_seconds=30))
        self.link = copy(self.services.links.current)
        self.transport_revision = self.link.token_revision
        self.bot, self.clock = bot, clock
        self.application = SimpleNamespace(running=True)
        self.accepted = []
        self.admissions = {}
        self.verification = None
        self.verdict = SimpleNamespace(status="valid", user=None)
        self.verified = []

    async def refresh_link(self):
        current = await self.services.links.get(self.link.id)
        if current is None or current.token_revision != self.transport_revision:
            return None
        self.link = current
        return current

    async def start(self):
        self.application.running = True

    async def handle_update(self, value):
        self.accepted.append(value.update_id)
        result = self.admissions.get(value.update_id, True)
        if isinstance(result, Exception):
            raise result
        return result

    async def wait_token_verification(self):
        if self.verification is not None and not self.verification.is_set():
            await self.verification.wait()
            return self.verdict
        return None

    async def verify_token(self, error=None, *, from_get_me=False):
        self.verified.append((type(error).__name__, from_get_me))
        return self.verdict

    async def record_poll_success(self, *, recovered=False):
        current = await self.refresh_link()
        if current is None:
            return False
        facts = {}
        if recovered or current.last_ok_at is None or self.clock.now() - current.last_ok_at >= timedelta(seconds=60):
            facts["last_ok_at"] = self.clock.now()
        return await self.services.links.update_if_current(current.id, token_revision=self.transport_revision, **facts) is not None


@pytest_asyncio.fixture
async def env():
    request, updates, clock = RecordingRequest(), RecordingRequest(), ManualClock()
    request.enqueue_get_me()
    async with offline_bot(request, updates_request=updates) as bot:
        channel = Channel(bot, clock)
        health = []

        async def on_health(cause, holds):
            health.append((cause, holds))
            current = channel.services.links.current
            current.health = cause if holds else "ok"
            if cause == "conflict":
                if holds:
                    for key, value in task.health_transition.as_dict().items():
                        setattr(current, key, value)
                else:
                    current.conflict_since = current.probe_until = current.webhook_host = None

        task = PollTask(channel, on_health=on_health, record=PollRecord(), clock=clock.now, sleep=clock.sleep)
        yield SimpleNamespace(request=request, updates=updates, clock=clock, channel=channel, task=task, health=health, on_health=on_health)


async def cancel(task):
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def test_run_uses_exact_poll_contract_and_confirms_only_admitted_offset(env):
    env.updates.enqueue("getUpdates", result=[update_payload(10), update_payload(11)])
    held = env.updates.enqueue("getUpdates", result=[], release=asyncio.Event())
    running = asyncio.create_task(env.task.run())
    try:
        await asyncio.wait_for(held.started.wait(), 2)
        assert env.channel.accepted == [10, 11]
        first, second = env.updates.calls
        assert "offset" not in first.parameters
        assert first.parameters["timeout"] == 10 and first.parameters["allowed_updates"] == ALLOWED
        assert second.parameters["offset"] == 12
    finally:
        await cancel(running)
    env.updates.enqueue("getUpdates", result=[])
    await env.task.confirm_offset()
    assert env.updates.calls[-1].parameters == {"offset": 12, "timeout": 0, "allowed_updates": ALLOWED}


@pytest.mark.parametrize("failure", [False, RuntimeError("synthetic admission failure")])
async def test_failed_admission_retains_suffix_and_blocks_new_poll_and_confirmation(env, failure):
    env.channel.admissions[11] = failure
    env.updates.enqueue("getUpdates", result=[update_payload(10), update_payload(11), update_payload(12)])
    held = env.updates.enqueue("getUpdates", result=[], release=asyncio.Event())
    running = asyncio.create_task(env.task.run())
    try:
        await env.clock.wait_for_sleep()
        assert env.channel.accepted == [10, 11] and env.task.offset is None
        await env.task.confirm_offset()
        assert len(env.updates.calls) == 1
        env.channel.admissions[11] = True
        env.clock.advance(1)
        await asyncio.wait_for(held.started.wait(), 2)
        assert env.channel.accepted == [10, 11, 11, 12]
        assert env.updates.calls[-1].parameters["offset"] == 13
    finally:
        await cancel(running)


async def test_held_cap_releases_without_waiting_for_admission_and_advances_only_retained_prefix(env):
    gate = True
    env.task.on_result = lambda: gate
    env.channel.admissions[3] = False
    env.updates.enqueue("getUpdates", result=[update_payload(n) for n in range(1, 16)])
    next_poll = env.updates.enqueue("getUpdates", result=[], release=asyncio.Event())
    running = asyncio.create_task(env.task.run())
    try:
        for _ in range(100):
            if env.task.held_count():
                break
            await asyncio.sleep(0)
        assert env.task.held_count() == 10 and env.channel.accepted == [] and env.task.offset is None
        await env.task.confirm_offset()
        assert len(env.updates.calls) == 1
        gate = False
        await asyncio.wait_for(env.task.release_held(), 1)
        await env.clock.wait_for_sleep()
        assert env.channel.accepted == [1, 2, 3] and env.task.offset is None
        env.channel.admissions[3] = True
        env.clock.advance(1)
        await asyncio.wait_for(next_poll.started.wait(), 2)
        assert env.updates.calls[-1].parameters["offset"] == 11
        assert env.channel.accepted == [1, 2, 3, *range(3, 11)]
    finally:
        await cancel(running)


@pytest.mark.parametrize("wake_during_admission", [False, True])
async def test_transferred_release_wake_is_consumed_before_admission_but_new_wake_survives(env, wake_during_admission):
    gate = True
    current_entered, release_current, admission_entered, release_admission = (asyncio.Event() for _ in range(4))
    env.task.on_result = lambda: gate
    env.channel.admissions[2] = False
    env.updates.enqueue("getUpdates", result=[update_payload(n) for n in (1, 2, 3)])
    next_poll = env.updates.enqueue("getUpdates", result=[], release=asyncio.Event())
    refresh, handle = env.channel.refresh_link, env.channel.handle_update

    async def current():
        if env.task.held_count() and not current_entered.is_set():
            current_entered.set()
            await release_current.wait()
        return await refresh()

    async def admission(update):
        if update.update_id == 2 and not admission_entered.is_set():
            admission_entered.set()
            await release_admission.wait()
        return await handle(update)

    env.channel.refresh_link, env.channel.handle_update = current, admission
    running = asyncio.create_task(env.task.run())
    try:
        await asyncio.wait_for(current_entered.wait(), 2)
        gate = False
        await env.task.release_held()
        release_current.set()
        await asyncio.wait_for(admission_entered.wait(), 2)
        if wake_during_admission:
            env.task.wake()
        release_admission.set()
        await env.clock.wait_for_sleep()
        assert env.channel.accepted == ([1, 2, 2] if wake_during_admission else [1, 2])
        assert len(env.updates.calls) == 1 and env.task.offset is None
        env.channel.admissions[2] = True
        env.clock.advance(env.clock.sleeps[-1])
        await asyncio.wait_for(next_poll.started.wait(), 2)
        assert env.channel.accepted == ([1, 2, 2, 2, 3] if wake_during_admission else [1, 2, 2, 3])
        assert env.updates.calls[-1].parameters["offset"] == 4
    finally:
        release_current.set()
        release_admission.set()
        await cancel(running)


@pytest.mark.parametrize("operation", ["probe", "confirm_offset", "drop_pending"])
async def test_all_get_updates_operations_share_one_request_owner(env, operation):
    env.task.offset = 10
    release = asyncio.Event()
    long_poll = env.updates.enqueue("getUpdates", result=[], release=release)
    next_call = env.updates.enqueue("getUpdates", result=[], release=asyncio.Event())
    running = asyncio.create_task(env.task.run())
    other = None
    try:
        await asyncio.wait_for(long_poll.started.wait(), 2)
        other = asyncio.create_task(getattr(env.task, operation)())
        await asyncio.sleep(0)
        assert len(env.updates.calls) == 1
        release.set()
        await asyncio.wait_for(next_call.started.wait(), 2)
        assert len(env.updates.calls) == 2
    finally:
        await cancel(running)
        if other is not None:
            await cancel(other)


@pytest.mark.parametrize("operation", ["run", "probe"])
async def test_old_token_success_is_not_counted_or_admitted_even_when_status_writes_are_throttled(env, operation):
    # Establish a recent successful write first.
    env.updates.enqueue("getUpdates", result=[])
    await env.task.probe()
    before = (env.task.record.polls, env.task.record.successes)
    release = asyncio.Event()
    result = env.updates.enqueue("getUpdates", result=[update_payload(10)], release=release)
    running = asyncio.create_task(getattr(env.task, operation)())
    try:
        await asyncio.wait_for(result.started.wait(), 2)
        env.channel.services.links.current.token_revision = 2
        release.set()
        await asyncio.wait_for(running, 2)
        assert (env.task.record.polls, env.task.record.successes) == before
        assert env.channel.accepted == []
    finally:
        await cancel(running)


async def test_same_token_binding_refresh_keeps_poller_and_captured_revision(env):
    env.channel.services.links.current.binding_generation = 2
    env.updates.enqueue("getUpdates", result=[update_payload(10)])
    assert await env.task.probe() == "ok"
    assert env.channel.link.binding_generation == 2 and env.task.transport_revision == 1
    assert env.channel.accepted == [10]


async def test_current_revision_is_checked_before_each_durable_admission(env):
    original = env.channel.handle_update

    async def admit(value):
        result = await original(value)
        env.channel.services.links.current.token_revision = 2
        return result

    env.channel.handle_update = admit
    env.updates.enqueue("getUpdates", result=[update_payload(10), update_payload(11)])
    await env.task.probe()
    assert env.channel.accepted == [10] and env.task.offset is None


async def test_polling_pauses_for_adapter_shared_verification(env):
    env.channel.verification = asyncio.Event()
    result = env.updates.enqueue("getUpdates", result=[], release=asyncio.Event())
    running = asyncio.create_task(env.task.run())
    try:
        await asyncio.sleep(0)
        assert env.updates.calls == []
        env.channel.verification.set()
        await asyncio.wait_for(result.started.wait(), 2)
    finally:
        await cancel(running)


@pytest.mark.parametrize("verdict", ["valid", "unverified", "rejected", "retired"])
async def test_invalid_token_delegates_once_to_adapter_without_its_own_recheck_sleep(env, verdict):
    env.channel.verdict = SimpleNamespace(status=verdict, user=None)
    env.updates.enqueue("getUpdates", status=401, body={"ok": False, "error_code": 401, "description": "Unauthorized"})
    assert await env.task.probe() in {"network", "invalid-token"}
    assert env.channel.verified == [("InvalidToken", False)]
    assert env.clock.sleeps == []


async def test_timed_out_repolls_immediately_but_network_errors_use_bounded_backoff(env):
    blocked = asyncio.Event()
    called = asyncio.Event()
    failures = [TimedOut(), NetworkError("synthetic network error")]

    async def get_updates(**kwargs):
        if failures:
            raise failures.pop(0)
        called.set()
        await blocked.wait()
        return []

    env.channel.bot = SimpleNamespace(get_updates=get_updates)
    running = asyncio.create_task(env.task.run())
    try:
        await env.clock.wait_for_sleep()
        assert env.clock.sleeps == [1.0]
        env.clock.advance(1)
        await asyncio.wait_for(called.wait(), 2)
    finally:
        await cancel(running)


async def test_product_error_escapes_instead_of_becoming_a_network_retry(env):
    class BrokenBot:
        async def get_updates(self, **kwargs):
            raise RuntimeError("synthetic product fault")

    env.channel.bot = BrokenBot()
    with pytest.raises(RuntimeError, match="synthetic product fault"):
        await env.task.run()
    assert env.clock.sleeps == []


async def test_drop_pending_uses_negative_offset_and_empty_drop_restores_absent_offset(env):
    env.updates.enqueue("getUpdates", result=[update_payload(75)])
    await env.task.drop_pending()
    assert env.task.offset == 76
    assert env.updates.calls[-1].parameters == {"offset": -1, "limit": 1, "timeout": 0, "allowed_updates": ALLOWED}
    env.updates.enqueue("getUpdates", result=[])
    await env.task.drop_pending()
    assert env.task.offset is None and env.channel.accepted == []


async def test_last_ok_writes_first_recovery_and_otherwise_at_most_every_minute(env):
    for _ in range(3):
        env.updates.enqueue("getUpdates", result=[])
        await env.task.probe()
        env.clock.advance(20)
    assert len([facts for facts in env.channel.services.links.facts if "last_ok_at" in facts]) == 1
    env.updates.enqueue("getUpdates", result=[])
    await env.task.probe()
    assert len([facts for facts in env.channel.services.links.facts if "last_ok_at" in facts]) == 2


async def test_recovery_after_twenty_three_hours_is_exposed_once_to_health_sink(env):
    env.channel.services.links.current.last_ok_at = env.clock.now() - timedelta(hours=24)
    env.channel.bot = SimpleNamespace(get_updates=AsyncMock(side_effect=NetworkError("offline")))
    assert await env.task.probe() == "network"
    env.channel.bot = SimpleNamespace(get_updates=AsyncMock(return_value=[]))
    assert await env.task.probe() == "ok"
    assert env.task.back_online_due
    await env.task.probe()
    assert not env.task.back_online_due


async def test_delayed_webhook_response_from_retired_token_cannot_count_or_publish(env):
    env.updates.enqueue("getUpdates", status=409, body={"ok": False, "error_code": 409, "description": "Conflict"})
    release = asyncio.Event()
    webhook = env.request.enqueue("getWebhookInfo", result={"url": "https://fixture.invalid/path", "has_custom_certificate": False, "pending_update_count": 0}, release=release)
    result = asyncio.create_task(env.task.probe())
    try:
        await asyncio.wait_for(webhook.started.wait(), 2)
        env.channel.services.links.current.token_revision = 2
        release.set()
        assert await asyncio.wait_for(result, 2) == "invalid-token"
        assert env.task.record.polls == 0 and not env.task.record.webhook and env.health == []
    finally:
        await cancel(result)


async def test_failed_initialization_that_rejected_token_exits_without_retry_sleep(env):
    env.channel.application.running = False

    async def rejected_start():
        env.channel.services.links.current.health = "token_rejected"
        raise NetworkError("initialization incomplete")

    env.channel.start = rejected_start
    result = asyncio.create_task(env.task.run())
    try:
        await asyncio.wait_for(asyncio.shield(result), 0.2)
        assert env.clock.sleeps == []
    finally:
        await cancel(result)


async def test_new_conflict_waits_probe_interval_before_automatic_probe(env):
    env.updates.enqueue("getUpdates", status=409, body={"ok": False, "error_code": 409, "description": "Conflict"})
    env.request.enqueue("getWebhookInfo", result={"url": "https://fixture.invalid/path", "has_custom_certificate": False, "pending_update_count": 0})
    called = env.request.enqueue("getWebhookInfo", result={"url": "https://fixture.invalid/path", "has_custom_certificate": False, "pending_update_count": 0}, release=asyncio.Event())
    running = asyncio.create_task(env.task.run())
    try:
        await env.clock.wait_for_sleep()
        assert env.clock.sleeps == [30]
        assert not called.started.is_set()
        env.clock.advance(30)
        await asyncio.wait_for(called.started.wait(), 2)
    finally:
        await cancel(running)


async def test_fence_repair_is_not_postponed_by_conflict_probe_interval(env):
    env.channel.services.links.current.health = "conflict"
    env.channel.link.health = "conflict"
    env.task._conflicted = True
    env.task._probe_until = env.clock.now() + timedelta(hours=24)
    env.task._last_probe = env.clock.now()
    env.channel.services.links.fence_error = OperationalError("synthetic fence failure", {}, None)
    env.updates.enqueue("getUpdates", status=409, body={"ok": False, "error_code": 409, "description": "Conflict"})
    assert await env.task.probe() == "network"
    env.channel.services.links.fence_error = None
    env.request.enqueue("getWebhookInfo", result={"url": "", "has_custom_certificate": False, "pending_update_count": 0})
    running = asyncio.create_task(env.task.run())
    try:
        for _ in range(100):
            if env.channel.services.links.fences:
                break
            await asyncio.sleep(0)
        assert env.channel.services.links.fences == 1
    finally:
        await cancel(running)


async def test_failed_negative_offset_drop_is_retried_before_ordinary_polling(env):
    env.updates.enqueue("getUpdates", status=502, body={"ok": False, "error_code": 502, "description": "synthetic network failure"})
    await env.task.drop_pending()
    retried = env.updates.enqueue("getUpdates", result=[update_payload(70)], release=asyncio.Event())
    running = asyncio.create_task(env.task.run())
    try:
        await asyncio.wait_for(retried.started.wait(), 2)
        assert env.updates.calls[-1].parameters["offset"] == -1
        assert env.updates.calls[-1].parameters["limit"] == 1
    finally:
        await cancel(running)


async def test_run_racing_successful_drop_does_not_repeat_negative_offset_and_discard_new_inputs(env):
    release = asyncio.Event()
    dropped = env.updates.enqueue("getUpdates", result=[update_payload(70)], release=release)
    next_request = env.updates.enqueue("getUpdates", result=[update_payload(71)], release=asyncio.Event())
    control = asyncio.create_task(env.task.drop_pending())
    running = None
    try:
        await asyncio.wait_for(dropped.started.wait(), 2)
        running = asyncio.create_task(env.task.run())
        for _ in range(10):
            await asyncio.sleep(0)
        release.set()
        await asyncio.wait_for(next_request.started.wait(), 2)
        assert env.updates.calls[-1].parameters["offset"] == 71
        assert "limit" not in env.updates.calls[-1].parameters
    finally:
        await cancel(control)
        if running is not None:
            await cancel(running)


async def test_public_admission_keeps_owned_suffix_and_refuses_a_new_drop(env):
    env.channel.admissions[11] = False
    assert await env.task.admit_batch([make_update(10), make_update(11)]) == 1
    assert env.task.offset is None and await env.task.probe() == "network"
    with pytest.raises(ValueError, match="pending-admission"):
        await env.task.drop_pending()
    env.channel.admissions[11] = True
    assert await env.task.admit_batch([make_update(11)]) == 1
    assert env.task.offset == 12 and env.updates.calls == []


async def test_poll_429_uses_server_delay_plus_half_second(env, monkeypatch):
    monkeypatch.setenv("PTB_TIMEDELTA", "1")
    env.updates.enqueue("getUpdates", status=429, body={"ok": False, "error_code": 429, "description": "Retry later", "parameters": {"retry_after": 70}})
    next_poll = env.updates.enqueue("getUpdates", result=[], release=asyncio.Event())
    running = asyncio.create_task(env.task.run())
    try:
        await env.clock.wait_for_sleep()
        assert env.clock.sleeps == [70.5] and not next_poll.started.is_set()
        env.clock.advance(70.5)
        await asyncio.wait_for(next_poll.started.wait(), 2)
    finally:
        await cancel(running)


async def test_network_failure_marks_unreachable_at_three_hundred_seconds_and_success_clears_it(env):
    env.channel.bot = SimpleNamespace(get_updates=AsyncMock(side_effect=NetworkError("synthetic network failure")))
    assert await env.task.probe() == "network"
    env.clock.advance(299)
    await env.task.probe()
    assert ("unreachable", True) not in env.health
    env.clock.advance(1)
    await env.task.probe()
    assert ("unreachable", True) in env.health
    env.channel.bot = SimpleNamespace(get_updates=AsyncMock(return_value=[]))
    assert await env.task.probe() == "ok"
    assert env.health[-1] == ("unreachable", False)


async def test_poll_task_transport_revision_is_read_only(env):
    with pytest.raises(AttributeError):
        env.task.transport_revision = 2


async def test_poll_logs_first_success_recovery_and_only_safe_exception_classes(env, caplog):
    caplog.set_level("DEBUG", logger="app.telegram_bots.polling")
    env.channel.bot = SimpleNamespace(get_updates=AsyncMock(side_effect=NetworkError("synthetic private message must not be logged")))
    await env.task.probe()
    env.channel.bot = SimpleNamespace(get_updates=AsyncMock(return_value=[]))
    await env.task.probe()
    assert "synthetic private message" not in caplog.text
    assert "error=NetworkError" in caplog.text
    assert "poll_first_ok" in caplog.text and "poll_recovered" in caplog.text


async def test_automatic_probe_rechecks_expiry_after_waiting_for_verification(env):
    env.updates.enqueue("getUpdates", status=409, body={"ok": False, "error_code": 409, "description": "Conflict"})
    env.request.enqueue("getWebhookInfo", result={"url": "https://fixture.invalid/path", "has_custom_certificate": False, "pending_update_count": 0})
    await env.task.probe()
    env.clock.advance(24 * 3600 - 1)
    entered, release = asyncio.Event(), asyncio.Event()
    checks = 0

    async def verification():
        nonlocal checks
        checks += 1
        if checks == 2:
            entered.set()
            await release.wait()
        return None

    env.channel.wait_token_verification = verification
    unexpected = env.request.enqueue("getWebhookInfo", result={"url": "https://fixture.invalid/path", "has_custom_certificate": False, "pending_update_count": 0}, release=asyncio.Event())
    running = asyncio.create_task(env.task.run())
    try:
        await asyncio.wait_for(entered.wait(), 2)
        env.clock.advance(2)
        release.set()
        for _ in range(50):
            await asyncio.sleep(0)
        assert not unexpected.started.is_set()
    finally:
        await cancel(running)


async def test_checker_started_during_revision_cas_blocks_new_get_updates_dispatch(env):
    original = env.channel.refresh_link
    prepared = asyncio.Event()
    gate = asyncio.Event()
    reads = 0

    async def refresh():
        nonlocal reads
        current = await original()
        reads += 1
        if reads == 2:
            env.channel.verification = gate
            prepared.set()
        return current

    env.channel.refresh_link = refresh
    sent = env.updates.enqueue("getUpdates", result=[], release=asyncio.Event())
    running = asyncio.create_task(env.task.run())
    try:
        await asyncio.wait_for(prepared.wait(), 2)
        for _ in range(10):
            await asyncio.sleep(0)
        assert not sent.started.is_set()
        gate.set()
        await asyncio.wait_for(sent.started.wait(), 2)
    finally:
        await cancel(running)


async def test_public_probe_never_retries_a_current_rejected_token(env):
    env.channel.services.links.current.health = "token_rejected"
    assert await env.task.probe() == "invalid-token"
    assert env.updates.calls == []


async def test_recent_adapter_success_is_respected_by_poll_status_write_throttle(env):
    env.updates.enqueue("getUpdates", result=[])
    await env.task.probe()
    env.clock.advance(40)
    env.channel.services.links.current.last_ok_at = env.clock.now()
    env.clock.advance(20)
    env.updates.enqueue("getUpdates", result=[])
    await env.task.probe()
    assert len([facts for facts in env.channel.services.links.facts if "last_ok_at" in facts]) == 1


async def test_long_webhook_rate_wait_is_deferred_without_losing_committed_conflict(env):
    from app.telegram_bots.adapter import LongRetryAfter

    env.channel.bot = SimpleNamespace(get_updates=AsyncMock(side_effect=Conflict("synthetic conflict")), get_webhook_info=AsyncMock(side_effect=LongRetryAfter(70)))
    assert await env.task.probe() == "network"
    assert env.channel.services.links.fences == 1 and env.task.record.conflicts == 1
    env.channel.bot.get_updates = AsyncMock(return_value=[])
    assert await env.task.probe() == "network"
    assert not env.channel.bot.get_updates.called
    env.clock.advance(70)
    assert await env.task.probe() == "ok"


async def test_conflict_log_retains_closed_description_without_retaining_raw_error(env, caplog):
    env.updates.enqueue("getUpdates", status=409, body={"ok": False, "error_code": 409, "description": "Conflict: terminated by other getUpdates request; synthetic private suffix"})
    env.request.enqueue("getWebhookInfo", result={"url": "", "has_custom_certificate": False, "pending_update_count": 0})
    assert await env.task.probe() == "conflict"
    assert "desc=other-poller" in caplog.text
    assert "synthetic private suffix" not in caplog.text


async def test_pending_drop_logs_info_with_last_discarded_update(env, caplog):
    caplog.set_level(logging.INFO, logger="app.telegram_bots.polling")
    env.updates.enqueue("getUpdates", result=[update_payload(40)])
    await env.task.drop_pending()
    logs = [record for record in caplog.records if "pending_dropped" in record.getMessage()]
    assert len(logs) == 1 and logs[0].levelno == logging.INFO
    assert "through=40" in logs[0].getMessage()
