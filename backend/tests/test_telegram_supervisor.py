"""Supervisor ownership over actual managed transports and real SQL."""

import asyncio
import logging
import secrets
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from _telegram_bot_test_support import ManualClock, RecordingRequest
from _telegram_repository_test_support import add_link
from _telegram_repository_test_support import db as db

from app.channels.message_bus import MessageBus
from app.telegram_bots.cipher import token_cipher, unwrap_token, wrap_token
from app.telegram_bots.producer import DeliveryEligibility, Outbox

pytestmark = pytest.mark.asyncio


@pytest.fixture(autouse=True)
def _supervisor_logging():
    """Restore process logging, including cached effective-level decisions."""
    for name in ("telegram", "telegram.ext", "httpx", "httpcore"):
        logging.getLogger(name)
    loggers = {logging.getLogger()}
    loggers.update(logger for logger in list(logging.root.manager.loggerDict.values()) if isinstance(logger, logging.Logger))
    logger_state = {logger: (logger.level, list(logger.filters)) for logger in loggers}
    handlers = {handler for logger in loggers for handler in logger.handlers}
    handler_filters = {handler: list(handler.filters) for handler in handlers}
    try:
        yield
    finally:
        for logger, (level, filters) in logger_state.items():
            logger.filters = filters
            logger.setLevel(level)
        for handler, filters in handler_filters.items():
            handler.filters = filters


class ImmediateCopyDecision:
    """Keep unrelated lifecycle fixtures offline with an explicit false result.

    Dedicated CopyGuard tests supply the real SQL-backed guard instead.
    """

    this_host_id = "offline-lifecycle-fixture"

    async def at_start(self):
        return "running"

    def can_still_be_broad_conflict(self, started, results):
        return False

    def broad_conflict(self, polled, results):
        return False


class Authority:
    async def delivery(self, link, item, part=None):
        return DeliveryEligibility.allow()

    async def target(self, link):
        return "ok"

    async def protect_notice(self, context):
        return False


class Health:
    def __init__(self, release=None, result=True):
        self.release, self.result, self.called = release, result, asyncio.Event()
        self.closed = False
        self.urls = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        self.closed = True

    async def get(self, url, **kwargs):
        self.urls.append((url, kwargs))
        self.called.set()
        if self.release is not None:
            await self.release.wait()
        return SimpleNamespace(status_code=200 if self.result else 503)


def build(db, monkeypatch, *, health=None, clock=None, factory=None, environ=None, profiles=False):
    from app.telegram_bots import supervisor as module

    monkeypatch.setattr(module, "static_cause", lambda *args: None)
    if not profiles:
        # These cases script exact SDK calls; profile pushes have their own tests.
        monkeypatch.setattr(module.ProfileSync, "_attempt", AsyncMock(return_value=False))
    monkeypatch.setenv("PTB_TIMEDELTA", "true")
    clock = clock or ManualClock()
    normal, updates = RecordingRequest(), RecordingRequest()
    health = health or Health()
    env = environ or {"HARTMESH_PROVIDER_KEYS_SECRET": secrets.token_urlsafe(48)}
    manager = SimpleNamespace(connection_repo=db.connections, gateway_url="http://127.0.0.1:8001", langgraph_url="http://127.0.0.1:8001/api")
    service = SimpleNamespace(bus=MessageBus(), manager=manager)
    app = SimpleNamespace(state=SimpleNamespace())
    kwargs = {} if factory is None else {"channel_factory": factory}
    instance = module.TelegramSupervisor(
        app,
        config=db.config,
        channel_service=service,
        session_factory=db.sessions,
        links=db.links,
        outbox=Outbox(db.sessions),
        settings=db.settings,
        authority=Authority(),
        copy_guard=ImmediateCopyDecision(),
        app_config=object(),
        environ=env,
        request_factory=lambda **kw: normal if kw["connection_pool_size"] == 8 else updates,
        health_client_factory=lambda **kw: health,
        clock=clock.now,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
        **kwargs,
    )
    return SimpleNamespace(supervisor=instance, service=service, app=app, health=health, normal=normal, updates=updates, clock=clock, env=env)


async def credential(db, env, *, bot=9_000_000_001):
    link = await add_link(db, bot=bot)
    token = str(bot) + ":" + secrets.token_urlsafe(30)
    link = await db.links.store_token(link.id, wrap_token(token_cipher(env), bot, token), actor_id=link.owner_user_id)
    return link, token


async def test_check_has_no_bot_calls_and_publishes_current(db, monkeypatch):
    from app.telegram_bots.supervisor import current

    e = build(db, monkeypatch)
    await e.supervisor.check()
    assert current() is e.supervisor
    assert e.supervisor.cause() is None
    assert e.supervisor.reason() is None
    assert e.normal.calls == e.updates.calls == []
    assert logging.getLogger("telegram").level == logging.INFO
    assert logging.getLogger("httpx").level == logging.WARNING
    await e.supervisor.stop()
    assert current() is None


async def test_add_obeys_same_health_barrier_as_initial_scan(db, monkeypatch):
    release = asyncio.Event()
    e = build(db, monkeypatch, health=Health(release))
    link, token = await credential(db, e.env)
    e.normal.enqueue_get_me(bot_id=link.bot_id)
    poll = e.updates.enqueue("getUpdates", result=[], release=asyncio.Event())
    await e.supervisor.check()
    starting = asyncio.create_task(e.supervisor.start())
    await e.health.called.wait()
    adding = asyncio.create_task(e.supervisor.add(link, token))
    await asyncio.sleep(0)
    assert e.normal.calls == []
    assert e.supervisor.channel(link.id) is None
    release.set()
    try:
        await asyncio.wait_for(asyncio.gather(starting, adding), 3)
        await asyncio.wait_for(poll.started.wait(), 2)
        assert [c.endpoint for c in e.normal.calls] == ["getMe"]
        assert e.health.urls[0][0] == "http://127.0.0.1:8001/health"
        assert e.health.closed
    finally:
        await e.supervisor.begin_shutdown()
        await e.supervisor.stop()
    assert e.normal.closed and e.updates.closed


async def test_health_failure_unblocks_add_without_constructing_clients(db, monkeypatch):
    e = build(db, monkeypatch, health=Health(result=False))
    link, token = await credential(db, e.env)
    await e.supervisor.check()
    starting = asyncio.create_task(e.supervisor.start())
    await e.clock.wait_for_sleep()
    adding = asyncio.create_task(e.supervisor.add(link, token))
    e.clock.advance(60)
    await asyncio.wait_for(asyncio.gather(starting, adding), 2)
    assert e.supervisor.cause() == "gateway-not-ready"
    assert e.supervisor.reason() == "misconfigured"
    assert e.supervisor.status()["polling"] == 0
    assert e.normal.calls == e.updates.calls == []
    await e.supervisor.stop()


@pytest.mark.parametrize("missing", ["channel_service", "connections", "session_factory"])
async def test_missing_runtime_prerequisite_fails_closed_without_sdk(db, monkeypatch, missing):
    from app.telegram_bots import supervisor as module

    e = build(db, monkeypatch)
    if missing == "channel_service":
        e.supervisor.channel_service = None
    elif missing == "connections":
        e.service.manager.connection_repo = None
    else:
        e.supervisor.session_factory = None
        monkeypatch.setattr(module, "get_session_factory", lambda: None)
    await e.supervisor.check()
    await e.supervisor.start()
    assert e.supervisor.cause() == "channel-service-failed"
    assert e.normal.calls == e.updates.calls == []
    assert not e.health.called.is_set()
    await e.supervisor.stop()


async def test_begin_shutdown_owns_retained_start_before_coroutine_body(db, monkeypatch):
    e = build(db, monkeypatch)
    await e.supervisor.check()
    task = asyncio.create_task(e.supervisor.start())
    e.app.state.telegram_bots_start = task
    await e.supervisor.begin_shutdown()
    assert task.done() and task.cancelled()
    assert not e.health.called.is_set()
    await e.supervisor.stop()


async def test_actual_client_start_attempts_share_ten_permits_with_add(db, monkeypatch):
    e = build(db, monkeypatch)
    requests, normal, updates, gates = [], [], [], []

    # getMe helper intentionally accepts only API fields; use an explicitly
    # scripted body to gate the actual SDK initialization calls.
    def request_factory(**kwargs):
        request = RecordingRequest()
        requests.append(request)
        if kwargs["connection_pool_size"] == 8:
            gate = asyncio.Event()
            normal.append(request)
            gates.append(request.enqueue("getMe", result={"id": 123456, "is_bot": True, "first_name": "Fixture"}, release=gate))
        else:
            updates.append(request)
            request.enqueue("getUpdates", result=[], release=asyncio.Event())
        return request

    e.supervisor.request_factory = request_factory
    links = []
    cipher = token_cipher(e.env)
    for i in range(11):
        link = await add_link(db, owner=f"owner-{i}", bot=9_000_000_100 + i)
        token = str(link.bot_id) + ":" + secrets.token_urlsafe(30)
        link = await db.links.store_token(link.id, wrap_token(cipher, link.bot_id, token), actor_id=link.owner_user_id)
        links.append((link, token))
    await e.supervisor.check()
    starting = asyncio.create_task(e.supervisor.start())
    adding = asyncio.create_task(e.supervisor.add(*links[-1]))
    try:

        async def ten_started():
            while len(gates) < 10 or not all(g.started.is_set() for g in gates):
                await asyncio.sleep(0)

        await asyncio.wait_for(ten_started(), 5)
        await asyncio.sleep(0.02)
        assert len(normal) == 10
        gates[0].release.set()

        async def eleven_started():
            while len(gates) < 11 or not gates[-1].started.is_set():
                await asyncio.sleep(0)

        await asyncio.wait_for(eleven_started(), 3)
        assert len(normal) == 11
        for gate in gates:
            gate.release.set()
        await asyncio.wait_for(asyncio.gather(starting, adding), 5)
        assert len(e.supervisor._channels) == 11
    finally:
        await e.supervisor.begin_shutdown()
        await e.supervisor.stop()
        await asyncio.gather(starting, adding, return_exceptions=True)
    assert all(request.closed for request in requests)


async def test_cancelled_constructor_keeps_ownership_until_loop_cleanup(db, monkeypatch):
    import threading

    from app.telegram_bots.adapter import TelegramBotChannel

    gate, entered = threading.Event(), threading.Event()
    constructor_threads = []
    loop_thread = threading.get_ident()

    def factory(**kwargs):
        constructor_threads.append(threading.get_ident())
        entered.set()
        assert gate.wait(5)
        return TelegramBotChannel(**kwargs)

    e = build(db, monkeypatch, factory=factory)
    await e.supervisor.check()
    await e.supervisor.start()
    link, token = await credential(db, e.env)
    adding = asyncio.create_task(e.supervisor.add(link, token))
    try:
        await asyncio.wait_for(asyncio.to_thread(entered.wait, 3), 4)
        adding.cancel()
        await asyncio.sleep(0)
        adding.cancel()
        await asyncio.sleep(0.01)
        assert not adding.done()
        assert e.normal.calls == []
        gate.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(adding, 3)
        assert constructor_threads == [constructor_threads[0]]
        assert constructor_threads[0] != loop_thread
        assert e.normal.closed and e.updates.closed
        assert e.supervisor.channel(link.id) is None
    finally:
        gate.set()
        await e.supervisor.begin_shutdown()
        await e.supervisor.stop()
        await asyncio.gather(adding, return_exceptions=True)


async def running_one(db, monkeypatch, *, connected=False, profiles=False, prepare=None):
    from _telegram_repository_test_support import bind_link

    e = build(db, monkeypatch, profiles=profiles)
    link, token = await credential(db, e.env)
    if connected:
        link = await bind_link(db, link)
    e.normal.enqueue_get_me(bot_id=link.bot_id)
    if prepare is not None:
        prepare(e, link)
    held = e.updates.enqueue("getUpdates", result=[], release=asyncio.Event())
    await e.supervisor.check()
    await e.supervisor.start()
    await asyncio.wait_for(held.started.wait(), 2)
    return e, link, held


async def test_stale_same_revision_poll_callback_cannot_mutate_successor(db, monkeypatch):
    e, link, held = await running_one(db, monkeypatch)
    try:
        old = e.supervisor.poll_task(link.id)
        channel = e.supervisor.channel(link.id)
        # Exercise the production captured callback after a same-token restart.
        replacement = e.supervisor._make_poll(channel)
        e.supervisor._polls[link.id] = replacement
        await old.on_health("unreachable", True)
        assert (await db.links.get(link.id)).health == "ok"
        await replacement.on_health("unreachable", True)
        assert (await db.links.get(link.id)).health == "unreachable"
    finally:
        await e.supervisor.stop()


async def test_fifth_unexpected_poll_death_stops_without_resetting_history(db, monkeypatch):
    e, link, held = await running_one(db, monkeypatch)
    channel = e.supervisor.channel(link.id)
    try:
        for death in range(1, 6):
            poll = e.supervisor.poll_task(link.id)
            task = e.supervisor._runners[link.id]
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

            async def product_failure():
                raise RuntimeError("Synthetic product failure")

            failed = asyncio.create_task(product_failure())
            await asyncio.gather(failed, return_exceptions=True)
            e.supervisor._runners[link.id] = failed
            await e.supervisor._poll_done(channel, poll, failed)
            if death < 5:
                replacement_call = e.updates.enqueue("getUpdates", result=[], release=asyncio.Event())
                # Let the retained restart task establish its monotonic wait.
                await asyncio.sleep(0)
                e.clock.advance(5)
                await asyncio.wait_for(replacement_call.started.wait(), 2)
                assert e.supervisor.poll_task(link.id) is not poll
                await poll.on_health("unreachable", True)
                assert (await db.links.get(link.id)).health == "ok"
        assert (await db.links.get(link.id)).health == "stopped"
        assert len(e.supervisor._deaths[(link.id, link.token_revision)]) == 5
    finally:
        await e.supervisor.stop()


async def test_missing_row_reconcile_forgets_client_without_remote_notice(db, monkeypatch):
    e, link, held = await running_one(db, monkeypatch)
    try:
        initial = [call.endpoint for call in e.normal.calls]
        await db.links.remove(link.id, actor_id=link.owner_user_id)
        await e.supervisor.reconcile()
        assert e.supervisor.channel(link.id) is None
        assert e.supervisor.poll_task(link.id) is None
        assert [call.endpoint for call in e.normal.calls] == initial
        assert e.supervisor.status()["bots"] == 0
        assert e.normal.closed and e.updates.closed
    finally:
        await e.supervisor.stop()


async def test_unreadable_ciphertext_keeps_row_and_never_builds_client(db, monkeypatch):
    e = build(db, monkeypatch)
    link = await add_link(db)
    await e.supervisor.check()
    await e.supervisor.start()
    try:
        assert (await db.links.get(link.id)).health == "secret_unreadable"
        assert e.supervisor.channel(link.id) is None
        assert e.normal.calls == e.updates.calls == []
        assert e.supervisor.status() == {"bots": 1, "people": 1, "polling": 0, "failing": 1, "held": 0}
    finally:
        await e.supervisor.stop()


async def test_a_token_marked_unreadable_recovers_when_its_secret_returns(db, monkeypatch):
    e = build(db, monkeypatch)
    link, _token = await credential(db, e.env)
    await db.links.set_health(link.id, "secret_unreadable", holds=True)
    e.normal.enqueue_get_me(bot_id=link.bot_id)
    held = e.updates.enqueue("getUpdates", result=[], release=asyncio.Event())
    await e.supervisor.check()
    await e.supervisor.start()
    try:
        await asyncio.wait_for(held.started.wait(), 2)
        assert (await db.links.get(link.id)).health == "ok"
        assert e.supervisor.channel(link.id) is not None
    finally:
        await e.supervisor.stop()


async def test_an_unreadable_token_is_read_once_per_start(db, monkeypatch):
    from app.telegram_bots import supervisor as module

    e = build(db, monkeypatch)
    link = await add_link(db)
    reads = []
    original = module.unwrap_token

    def counted(*args):
        reads.append(args)
        return original(*args)

    monkeypatch.setattr(module, "unwrap_token", counted)
    await e.supervisor.check()
    await e.supervisor.start()
    try:
        await e.supervisor.reconcile()
        await e.supervisor.reconcile()
        assert len(reads) == 1
        assert (await db.links.get(link.id)).health == "secret_unreadable"
    finally:
        await e.supervisor.stop()


async def test_a_held_bot_follows_a_rotated_secret_without_a_client(db, monkeypatch):
    previous = {"HARTMESH_PROVIDER_KEYS_SECRET": secrets.token_urlsafe(48)}
    e = build(db, monkeypatch, environ={"HARTMESH_PROVIDER_KEYS_SECRET": secrets.token_urlsafe(48), "HARTMESH_PROVIDER_KEYS_SECRET_PREVIOUS": previous["HARTMESH_PROVIDER_KEYS_SECRET"]})
    link, token = await credential(db, previous)
    await db.links.update(link.id, state="held")
    stored, _revision = await db.links.token(link.id)
    await e.supervisor.check()
    await e.supervisor.start()
    try:
        after, _revision = await db.links.token(link.id)
        assert after != stored
        opened = unwrap_token(token_cipher(e.env), link.bot_id, after)
        assert opened.key == token and opened.under_previous is False
        assert e.supervisor.channel(link.id) is None
        revision = (await db.links.token(link.id))[1]
        await e.supervisor.reconcile()
        assert await db.links.token(link.id) == (after, revision), "one reading per start"
    finally:
        await e.supervisor.stop()


async def test_previous_key_rewrap_cas_loses_to_rotation_without_overwrite(db, monkeypatch):
    from app.telegram_bots.links import BotLinkRepository

    previous = {"HARTMESH_PROVIDER_KEYS_SECRET": secrets.token_urlsafe(48)}
    e = build(db, monkeypatch, environ={"HARTMESH_PROVIDER_KEYS_SECRET": secrets.token_urlsafe(48), "HARTMESH_PROVIDER_KEYS_SECRET_PREVIOUS": previous["HARTMESH_PROVIDER_KEYS_SECRET"]})
    link, old_token = await credential(db, previous)
    successor = str(link.bot_id) + ":" + secrets.token_urlsafe(30)
    encrypted = wrap_token(token_cipher(e.env), link.bot_id, successor)
    other = BotLinkRepository(db.sessions, db.connections, db.config)
    store = db.links.store_token
    rotated = []

    async def racing_store(identity, wrapped, **kwargs):
        if kwargs.get("action") == "rewrap" and not rotated:
            rotated.append(await other.store_token(identity, encrypted, actor_id=link.owner_user_id, expected_revision=link.token_revision, token_changed=True))
        return await store(identity, wrapped, **kwargs)

    monkeypatch.setattr(db.links, "store_token", racing_store)
    e.normal.enqueue_get_me(bot_id=link.bot_id)
    held = e.updates.enqueue("getUpdates", result=[], release=asyncio.Event())
    await e.supervisor.check()
    await e.supervisor.start()
    try:
        await asyncio.wait_for(held.started.wait(), 2)
        assert rotated
        saved_ciphertext, revision = await db.links.token(link.id)
        assert saved_ciphertext == encrypted
        assert revision == link.token_revision + 1
        assert e.supervisor.channel(link.id).transport_revision == revision
    finally:
        await e.supervisor.stop()


async def test_first_start_429_transfers_original_wait_before_delayed_guard(db, monkeypatch):
    e = build(db, monkeypatch)
    link, token = await credential(db, e.env)
    first = e.normal.enqueue("getMe", status=429, body={"ok": False, "error_code": 429, "description": "Synthetic limit", "parameters": {"retry_after": 90}})
    e.normal.enqueue_get_me(bot_id=link.bot_id)
    poll = e.updates.enqueue("getUpdates", result=[], release=asyncio.Event())
    original = db.links.update_if_current
    delayed = []

    async def delayed_guard(*args, **kwargs):
        if first.started.is_set() and not delayed:
            delayed.append(True)
            e.clock.advance(40)
        return await original(*args, **kwargs)

    monkeypatch.setattr(db.links, "update_if_current", delayed_guard)
    await e.supervisor.check()
    await e.supervisor.start()
    try:

        async def wait_for_server_delay():
            while 50 not in e.clock.sleeps:
                await asyncio.sleep(0)

        await asyncio.wait_for(wait_for_server_delay(), 2)
        assert len(e.normal.calls) == 1
        e.clock.advance(49)
        await asyncio.sleep(0)
        assert len(e.normal.calls) == 1
        e.clock.advance(1)
        await asyncio.wait_for(poll.started.wait(), 2)
        assert [c.endpoint for c in e.normal.calls] == ["getMe", "getMe"]
    finally:
        await e.supervisor.stop()


def wait_fixture():
    """Actual PollTask timer primitive; no transport or persistence is run."""
    from app.telegram_bots.polling import PollRecord, PollTask

    clock = ManualClock()
    channel = SimpleNamespace(transport_revision=1, link=SimpleNamespace(id="queued-wait", bot_id=9_000_000_001, webhook_host=None, health="ok", probe_until=None))

    async def no_health(*_args):
        pytest.fail("A timer primitive must not publish health")

    return PollTask(channel, on_health=no_health, record=PollRecord(), clock=clock.now, sleep=clock.sleep), clock


async def test_poll_wait_does_not_restart_duration_when_sleep_child_is_queued():
    poll, clock = wait_fixture()
    waiting = asyncio.create_task(poll._wait(1))
    try:
        # Parent queues its sleeper; this continuation runs before that child.
        await asyncio.sleep(0)
        assert clock.sleeps == []
        clock.advance(1)
        await asyncio.wait_for(waiting, 2)
        assert clock.sleeps == [0]
    finally:
        waiting.cancel()
        await asyncio.gather(waiting, return_exceptions=True)
    assert clock._waiters == []


async def test_poll_rate_wait_keeps_original_server_deadline_after_guard_elapsed():
    from app.telegram_bots.adapter import LongRetryAfter

    poll, clock = wait_fixture()
    poll.defer_start_retry(LongRetryAfter(90), observed_at=clock.now())
    clock.advance(40)
    assert poll._rate_pending() and poll._retry_after == 50
    # The absolute server deadline survives a later guard, even when the
    # relative retry value was observed before that guard completed.
    clock.advance(49)
    waiting = asyncio.create_task(poll._wait(poll._retry_after, until=poll._rate_until))
    try:
        await asyncio.sleep(0)
        assert clock.sleeps == []
        clock.advance(1)
        await asyncio.wait_for(waiting, 2)
        assert clock.sleeps == [0]
    finally:
        waiting.cancel()
        await asyncio.gather(waiting, return_exceptions=True)
    assert clock._waiters == []


@pytest.mark.parametrize("finish", ["wake", "cancel"])
async def test_poll_queued_sleep_still_drains_on_wake_or_cancellation(finish):
    poll, clock = wait_fixture()
    waiting = asyncio.create_task(poll._wait(1))
    try:
        await asyncio.sleep(0)
        assert clock.sleeps == []
        if finish == "wake":
            poll.wake()
            await asyncio.wait_for(waiting, 2)
        else:
            waiting.cancel()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(waiting, 2)
    finally:
        waiting.cancel()
        await asyncio.gather(waiting, return_exceptions=True)
    assert clock._waiters == []


def notice_reply(request, link, *, message=901):
    return request.enqueue("sendMessage", result={"message_id": message, "date": 1_893_456_000, "chat": {"id": link.chat_id, "type": "private"}, "text": "Fixed product notice"})


async def test_actual_sdk_conflict_notices_render_brand_variant_and_throttle(db, monkeypatch):
    e, link, held = await running_one(db, monkeypatch, connected=True)
    try:
        channel = e.supervisor.channel(link.id)
        notice_reply(e.normal, link)
        await e.supervisor._notice(channel, "link.conflict", throttle=True)
        await e.supervisor._notice(channel, "link.conflict", throttle=True)
        notice_reply(e.normal, link, message=902)
        await e.supervisor._notice(channel, "link.conflict_over", throttle=True)
        calls = [call for call in e.normal.calls if call.endpoint == "sendMessage"]
        assert len(calls) == 2
        assert "HartMesh" in calls[0].parameters["text"]
        assert "This chat works again" in calls[1].parameters["text"]
        assert "HartMesh" in calls[1].parameters["text"]
        assert all(call.parameters["chat_id"] == link.chat_id for call in calls)
        assert (await db.links.get(link.id)).notices.get("conflict")
    finally:
        await e.supervisor.stop()


async def test_access_off_forbidden_is_disposable_and_preserves_held_state(db, monkeypatch):
    e, link, held = await running_one(db, monkeypatch, connected=True)
    try:
        channel = e.supervisor.channel(link.id)
        saved = await db.links.hold_if_current(link.id, token_revision=link.token_revision, binding_generation=link.binding_generation)
        channel.link = saved
        e.normal.enqueue("sendMessage", status=403, body={"ok": False, "error_code": 403, "description": "Forbidden: bot was blocked by the user"})
        await e.supervisor._notice(channel, "access.off")
        current = await db.links.get(link.id)
        assert current.state == "held"
        assert current.binding_generation == saved.binding_generation
        assert [call.endpoint for call in e.normal.calls] == ["getMe", "sendMessage"]
        assert current.notices["access_off"]
    finally:
        await e.supervisor.stop()


async def test_unsupported_target_keeps_pending_reply_and_all_sql_facts(db, monkeypatch):
    from _telegram_repository_test_support import bind_link
    from sqlalchemy import select

    from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow, TelegramOutboxRow

    e = build(db, monkeypatch)
    link, token = await credential(db, e.env)
    link = await bind_link(db, link)
    async with db.sessions() as session, session.begin():
        row = await session.get(TelegramBotLinkRow, link.id)
        row.agent_kind, row.agent_ref = "instance", "a" * 32
    link = await db.links.get(link.id)

    async def unsupported(current):
        return "unsupported"

    e.supervisor.authority.target = unsupported
    await e.supervisor.check()
    await e.supervisor.start()
    try:
        await e.supervisor.reconcile()
        saved = await db.links.get(link.id)
        assert saved == link
        async with db.sessions() as session:
            outboxes = list((await session.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == link.id))).all())
        assert outboxes and all(row.state == "pending" and row.skip_reason is None for row in outboxes)
        assert saved.notices == {}
        assert e.normal.calls == e.updates.calls == []
        assert e.supervisor.channel(link.id) is None
        assert e.supervisor.status()["polling"] == 0
    finally:
        await e.supervisor.stop()


async def test_remove_uses_owned_actual_notice_and_profile_before_client_close(db, monkeypatch):
    e, link, held = await running_one(db, monkeypatch, connected=True)
    notice_reply(e.normal, link)
    e.normal.enqueue("setMyDescription")
    try:
        await e.supervisor.remove(link.id, actor_id=link.owner_user_id)
        assert await db.links.get(link.id) is None
        assert [call.endpoint for call in e.normal.calls] == ["getMe", "sendMessage", "setMyDescription"]
        assert "HartMesh" in e.normal.calls[1].parameters["text"]
        assert e.normal.closed and e.updates.closed
        assert e.supervisor.channel(link.id) is None
    finally:
        await e.supervisor.stop()


async def test_sounding_selection_prefers_ordinary_chat_then_recent_nonblocked(db, monkeypatch):
    from dataclasses import replace
    from datetime import timedelta

    e = build(db, monkeypatch)
    first = await add_link(db, bot=9_000_000_101)
    recent = await add_link(db, bot=9_000_000_102)
    ordinary = await add_link(db, bot=9_000_000_103)
    recent = replace(recent, last_update_at=e.clock.now())
    ordinary = replace(ordinary, agent_kind="lead_agent", agent_ref="lead_agent")
    notices = [(SimpleNamespace(link=first), "access.off", False), (SimpleNamespace(link=recent), "access.off", False), (SimpleNamespace(link=ordinary), "access.off", False)]
    assert e.supervisor._sounding_link(notices) == ordinary.id
    assert e.supervisor._sounding_link(notices[:-1]) == recent.id
    assert e.supervisor._sounding_link([(notices[0][0], "access.off", False), (notices[1][0], "access.off", True)]) == first.id
    same = replace(recent, last_update_at=None, created_at=first.created_at + timedelta(seconds=1))
    assert e.supervisor._sounding_link([notices[0], (SimpleNamespace(link=same), "access.off", False)]) == first.id
    await e.supervisor.stop()


def per_link_factory(records, *, construction_gates=None, construction_entered=None, restoring=False):
    """Own both real SDK request clients separately for every constructed bot."""
    from app.telegram_bots.adapter import TelegramBotChannel

    def construct(*, link, token, supervisor, **kwargs):
        if construction_entered is not None:
            construction_entered.append(link.id)
        if construction_gates is not None:
            assert construction_gates[link.id].wait(8)
        normal, updates = RecordingRequest(), RecordingRequest()
        normal.enqueue_get_me(bot_id=link.bot_id)
        if restoring:
            updates.enqueue("getUpdates", result=[])
        held = updates.enqueue("getUpdates", result=[], release=asyncio.Event())
        services = SimpleNamespace(
            config=supervisor.config,
            links=supervisor.links,
            outbox=supervisor.outbox,
            settings=supervisor.settings,
            tls_context=supervisor.tls_context,
            eligibility=supervisor.eligibility,
            send_allowed=supervisor.send_allowed,
            protect_notice=supervisor.protect_notice,
            channel=supervisor.channel,
            request_factory=lambda **kw: normal if kw["connection_pool_size"] == 8 else updates,
        )
        channel = TelegramBotChannel(link=link, token=token, supervisor=services, **kwargs)
        records[link.id] = SimpleNamespace(channel=channel, normal=normal, updates=updates, held=held)
        return channel

    return construct


async def test_constructor_cancel_does_not_release_tenth_permit_early(db, monkeypatch):
    import threading

    records, entered, first_calls = {}, [], {}
    construction_gate, construction_entered = threading.Event(), threading.Event()
    sdk_release = asyncio.Event()
    factory = per_link_factory(records)
    blocked_identity = None

    def capped_factory(*, link, **kwargs):
        entered.append(link.id)
        if link.id == blocked_identity:
            construction_entered.set()
            assert construction_gate.wait(8)
        channel = factory(link=link, **kwargs)
        # Nine first-start attempts wait asynchronously in the actual SDK; only
        # the constructor being cancelled occupies a physical worker thread.
        # Ten held startup permits must not require ten executor threads.
        first_get_me = records[link.id].normal.replies["getMe"][0]
        first_get_me.release = sdk_release
        first_calls[link.id] = first_get_me
        return channel

    e = build(db, monkeypatch, factory=capped_factory)
    cipher, links = token_cipher(e.env), []
    for i in range(10):
        link = await add_link(db, owner=f"owner-{i}", bot=9_000_000_200 + i)
        token = str(link.bot_id) + ":" + secrets.token_urlsafe(30)
        link = await db.links.store_token(link.id, wrap_token(cipher, link.bot_id, token), actor_id=link.owner_user_id)
        links.append((link, token))
    blocked_identity = links[0][0].id
    await e.supervisor.check()
    starting = asyncio.create_task(e.supervisor.start())
    adding = None
    try:

        async def ten_attempts_started():
            while not construction_entered.is_set() or any(link.id not in first_calls or not first_calls[link.id].started.is_set() for link, _ in links[1:]):
                await asyncio.sleep(0)

        await asyncio.wait_for(ten_attempts_started(), 4)
        # Add the eleventh link after the scan captured its ten rows, exercising
        # the shared route/startup limit without scheduling a duplicate attempt.
        extra = await add_link(db, owner="owner-10", bot=9_000_000_210)
        extra_token = str(extra.bot_id) + ":" + secrets.token_urlsafe(30)
        extra = await db.links.store_token(extra.id, wrap_token(cipher, extra.bot_id, extra_token), actor_id=extra.owner_user_id)
        adding = asyncio.create_task(e.supervisor.add(extra, extra_token))
        cancelled = next(task for task, identity in e.supervisor._operations.items() if identity == blocked_identity)
        cancelled.cancel()
        await asyncio.sleep(0.02)
        assert len(entered) == 10 and extra.id not in entered
        assert not cancelled.done()
        construction_gate.set()

        async def eleventh_attempt_started():
            while extra.id not in first_calls or not first_calls[extra.id].started.is_set():
                await asyncio.sleep(0)

        await asyncio.wait_for(eleventh_attempt_started(), 4)
        assert records[blocked_identity].normal.closed
        assert records[blocked_identity].updates.closed
        assert not records[blocked_identity].normal.calls
        sdk_release.set()
        await asyncio.wait_for(asyncio.gather(starting, adding), 6)
        assert e.supervisor.sender._recovered
        assert len(e.supervisor._channels) == 10
    finally:
        construction_gate.set()
        sdk_release.set()
        await e.supervisor.begin_shutdown()
        await e.supervisor.stop()
        await asyncio.gather(*(task for task in (starting, adding) if task is not None), return_exceptions=True)
    assert all(record.normal.closed and record.updates.closed for record in records.values())


async def test_hourly_getme_uses_shared_verifier_and_current_flags(db, monkeypatch):
    e, link, held = await running_one(db, monkeypatch)
    gate = asyncio.Event()
    call = e.normal.enqueue("getMe", result={"id": link.bot_id, "is_bot": True, "first_name": "Fixture", "can_manage_bots": True, "has_topics_enabled": True}, release=gate)
    refresh = asyncio.create_task(e.supervisor._refresh_flags())
    try:
        await asyncio.wait_for(call.started.wait(), 2)
        concurrent = asyncio.create_task(e.supervisor.channel(link.id).verify_token())
        await asyncio.sleep(0)
        gate.set()
        await asyncio.wait_for(asyncio.gather(refresh, concurrent), 2)
        assert [record.endpoint for record in e.normal.calls] == ["getMe", "getMe"]
        saved = await db.links.get(link.id)
        assert saved.can_manage_bots and saved.topics_enabled
    finally:
        gate.set()
        await e.supervisor.stop()
        await asyncio.gather(refresh, return_exceptions=True)


async def test_hold_restore_pass_sounds_once_and_drops_before_access_back(db, monkeypatch):
    from _telegram_repository_test_support import bind_link, code_of, welcome

    from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow

    records = {}
    e = build(db, monkeypatch, factory=per_link_factory(records))
    links = []
    for i in range(3):
        link, token = await credential(db, e.env, bot=9_000_000_300 + i)
        if i == 1:
            async with db.sessions() as session, session.begin():
                row = await session.get(TelegramBotLinkRow, link.id)
                row.agent_kind, row.agent_ref = "lead_agent", "lead_agent"
            link = await db.links.get(link.id)
        if i == 1:
            code = code_of(await db.links.mint_code(link))
            link = await db.links.bind(
                await db.links.get(link.id),
                telegram_user_id=7_000_000_001,
                chat_id=7_000_000_001,
                first_name="Fixture",
                username="fixture_person",
                code=code,
                start_identity={"update_id": 201, "message_id": 101, "chat_id": 7_000_000_001},
                hello_parts=welcome("hello.chat"),
            )
        else:
            link = await bind_link(db, link)
        links.append(link)
    await db.settings.update(links[0].owner_user_id, quiet_enabled=False)
    mode = ["ok"]

    async def target(link):
        return mode[0]

    e.supervisor.authority.target = target
    await e.supervisor.check()
    await e.supervisor.start()
    for link in links:
        await asyncio.wait_for(records[link.id].held.started.wait(), 2)
        notice_reply(records[link.id].normal, link)
    mode[0] = "held"
    try:
        await asyncio.wait_for(e.supervisor.reconcile(), 5)
        for index, link in enumerate(links):
            calls = [c for c in records[link.id].normal.calls if c.endpoint == "sendMessage"]
            assert len(calls) == 1
            assert calls[0].parameters["disable_notification"] is (index != 1)
            saved = await db.links.get(link.id)
            assert saved.state == "held" and saved.binding_generation == link.binding_generation + 1
            assert await db.connections.restore_held_connection(saved.connection_id, owner_user_id=saved.owner_user_id) == "restored"
        old_records = dict(records)
        e.supervisor._channel_factory = per_link_factory(records, restoring=True)
        mode[0] = "ok"
        # Prepare the direct fixed notice from the same per-link SDK factory,
        # before its client starts. No durable delivery is fabricated.
        factory = e.supervisor._channel_factory

        def restored_factory(**kwargs):
            channel = factory(**kwargs)
            notice_reply(records[channel.link.id].normal, channel.link)
            return channel

        e.supervisor._channel_factory = restored_factory
        await asyncio.wait_for(e.supervisor.reconcile(), 5)
        for index, link in enumerate(links):
            current = await db.links.get(link.id)
            assert current.state == "connected"
            calls = [c for c in records[link.id].normal.calls if c.endpoint == "sendMessage"]
            assert len(calls) == 1
            assert calls[0].parameters["disable_notification"] is (index != 1)
            updates = records[link.id].updates.calls
            assert updates[0].parameters["offset"] == -1
            assert updates[0].parameters["limit"] == 1
            assert sum(call.parameters.get("offset") == -1 for call in updates) == 1
            assert old_records[link.id].normal.closed
    finally:
        await e.supervisor.stop()


async def test_replacement_waits_for_old_sender_and_client_retirement(db, monkeypatch):
    records = {}
    e = build(db, monkeypatch, factory=per_link_factory(records))
    link, token = await credential(db, e.env)
    await e.supervisor.check()
    await e.supervisor.start()
    old = records[link.id]
    await asyncio.wait_for(old.held.started.wait(), 2)
    entered, release = asyncio.Event(), asyncio.Event()
    original = e.supervisor.sender.retire_link

    async def stalled_retirement(identity):
        entered.set()
        await release.wait()
        await original(identity)

    monkeypatch.setattr(e.supervisor.sender, "retire_link", stalled_retirement)
    retiring = asyncio.create_task(e.supervisor._retire(link.id, reason="token-stored"))
    adding = None
    try:
        await asyncio.wait_for(entered.wait(), 2)
        adding = asyncio.create_task(e.supervisor.add(link, token))
        await asyncio.sleep(0.02)
        assert not adding.done()
        assert records[link.id] is old
        assert not old.normal.closed
        release.set()
        await asyncio.wait_for(asyncio.gather(retiring, adding), 3)
        assert records[link.id] is not old
        assert old.normal.closed and old.updates.closed
        assert e.supervisor.channel(link.id) is records[link.id].channel
    finally:
        release.set()
        await e.supervisor.stop()
        await asyncio.gather(*(task for task in (retiring, adding) if task is not None), return_exceptions=True)


async def test_retention_runs_daily_after_an_hour_and_the_smaller_sweeps_hourly(db, monkeypatch):
    e = build(db, monkeypatch)
    periodic: dict = {}
    monkeypatch.setattr(e.supervisor, "_periodic", lambda operation, seconds, name, *, first_delay=None: periodic.__setitem__(name, (operation, seconds, first_delay)))
    await e.supervisor.start()
    try:
        assert periodic["publication-recovery"][0] == e.supervisor._tick_recovery
        assert periodic["retention"][1:] == (86400, 3600)
        assert periodic["callback-sweep"][1:] == (3600, None) and periodic["pending-file-sweep"][1:] == (3600, None)
        assert await periodic["callback-sweep"][0]() == 0
        assert await periodic["pending-file-sweep"][0]() == 0
    finally:
        await e.supervisor.stop()


async def test_retention_logs_one_line_of_counts(db, monkeypatch, caplog):
    e = build(db, monkeypatch)
    await credential(db, e.env)
    with caplog.at_level("INFO", logger="app.telegram_bots.supervisor"):
        await e.supervisor.sweep_retention()
    lines = [record.getMessage() for record in caplog.records if "retention_sweep" in record.getMessage()]
    assert len(lines) == 1 and all(f" {key}=" in lines[0] for key in ("inbound", "outbox", "acts", "events"))
    await e.supervisor.stop()


async def test_a_failed_job_says_which_job_and_why(db, monkeypatch, caplog):
    e = build(db, monkeypatch)
    failed = asyncio.Event()

    async def fail():
        failed.set()
        raise ValueError("private text")

    e.supervisor._periodic(fail, 3600, "sender", first_delay=0)
    await asyncio.wait_for(failed.wait(), 3)
    await asyncio.sleep(0)
    lines = [record.getMessage() for record in caplog.records if "maintenance_failed" in record.getMessage()]
    assert len(lines) == 1 and lines[0].startswith("telegram_bots maintenance_failed job=sender cause=ValueError:test_telegram_supervisor.py:")
    assert "private text" not in caplog.text
    await e.supervisor.stop()


async def test_recovery_says_when_its_held_count_moves(db, monkeypatch, caplog):
    from app.telegram_bots.recovery import RecoveryPass

    e = build(db, monkeypatch)
    e.supervisor.recovery = SimpleNamespace(tick=AsyncMock(side_effect=[RecoveryPass(scanned=3, held=2), RecoveryPass(scanned=3, held=2), RecoveryPass(scanned=1)]))
    with caplog.at_level("INFO", logger="app.telegram_bots.supervisor"):
        for _ in range(3):
            await e.supervisor._tick_recovery()
    lines = [(record.levelname, record.getMessage()) for record in caplog.records if "publication_recovery_pass" in record.getMessage()]
    assert lines == [("WARNING", "telegram_bots publication_recovery_pass held=2 scanned=3"), ("INFO", "telegram_bots publication_recovery_pass held=0 scanned=1")]
    e.supervisor.recovery = None
    await e.supervisor.stop()


async def test_event_retention_deletes_only_before_400_day_boundary(db, monkeypatch):
    from datetime import UTC, datetime, timedelta

    from sqlalchemy import select

    from deerflow.persistence.telegram_bots.model import TelegramBotLinkEventRow

    e = build(db, monkeypatch)
    link, token = await credential(db, e.env)
    now = datetime.now(UTC)
    async with db.sessions() as session, session.begin():
        events = list(await session.scalars(select(TelegramBotLinkEventRow).where(TelegramBotLinkEventRow.link_id == link.id)))
        assert len(events) == 2
        events[0].created_at = now - timedelta(days=401)
        events[1].created_at = now - timedelta(days=400)
        old_id, boundary_id = events[0].id, events[1].id
    counts = await e.supervisor.sweep_retention(now)
    assert counts == {"inbound": 0, "outbox": 0, "acts": 0, "events": 1}
    async with db.sessions() as session:
        assert await session.get(TelegramBotLinkEventRow, old_id) is None
        assert await session.get(TelegramBotLinkEventRow, boundary_id) is not None
    await e.supervisor.stop()
