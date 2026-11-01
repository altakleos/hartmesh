"""Copy startup observations gate actual SDK clients, not ownership claims."""

import asyncio
from types import SimpleNamespace

import pytest
from _telegram_bot_test_support import RecordingRequest, update_payload
from _telegram_repository_test_support import db as db
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging
from test_telegram_supervisor import build, credential

from app.telegram_bots.adapter import TelegramBotChannel
from app.telegram_bots.deployment import CopyGuard
from deerflow.persistence.telegram_bots.model import TelegramDeploymentRow

pytestmark = pytest.mark.asyncio


class Channels:
    def __init__(self, script=None, handler=None):
        self.script, self.handler, self.created = script, handler, {}

    def __call__(self, *, link, token, supervisor, **kwargs):
        normal, updates = RecordingRequest(), RecordingRequest()
        normal.enqueue_get_me(bot_id=link.bot_id)
        if self.script is not None:
            self.script(link, normal, updates)
        services = SimpleNamespace(
            config=supervisor.config,
            links=supervisor.links,
            outbox=supervisor.outbox,
            settings=supervisor.settings,
            tls_context=supervisor.tls_context,
            eligibility=supervisor.eligibility,
            protect_notice=supervisor.protect_notice,
            channel=supervisor.channel,
            send_allowed=supervisor.send_allowed,
            request_factory=lambda **kw: normal if kw["connection_pool_size"] == 8 else updates,
        )
        channel = TelegramBotChannel(link=link, token=token, supervisor=services, **kwargs)
        if self.handler is not None:

            async def handle(update):
                return await self.handler(link, update)

            channel.handle_update = handle
        self.created[link.id] = SimpleNamespace(channel=channel, normal=normal, updates=updates)
        return channel


def environment(db, monkeypatch, *, script=None, handler=None, host="fixture-host", health=None, environ=None):
    channels = Channels(script, handler)
    e = build(db, monkeypatch, factory=channels, health=health, environ=environ)
    e.supervisor.copy_guard = CopyGuard(db.sessions, host_reader=lambda: host, clock=e.clock.now)
    e.channels = channels
    return e


def release_queued_replies(e):
    """A safe-offset confirmation must not inherit an unused test-only wait."""
    for transport in e.channels.created.values():
        for request in (transport.normal, transport.updates):
            for replies in request.replies.values():
                for reply in replies:
                    if reply.release is not None:
                        reply.release.set()


async def stop_environment(e):
    release_queued_replies(e)
    await e.supervisor.stop()


async def wait_until(predicate):
    async def ready():
        while not predicate():
            # A spin here takes the GIL from the SQLite thread, so a bot's start
            # would slow with every statement until it missed this bound.
            await asyncio.sleep(0.01)

    await asyncio.wait_for(ready(), 15)


async def test_known_host_mismatch_refuses_before_health_or_bot_calls(db, monkeypatch):
    e = environment(db, monkeypatch)
    link, token = await credential(db, e.env)
    async with db.sessions() as session, session.begin():
        row = await session.get(TelegramDeploymentRow, 1)
        row.host_id = "host-before-copy"
    try:
        await e.supervisor.check()
        assert e.supervisor.cause() == "host-changed"
        await e.supervisor.start()
        assert e.supervisor.reason() == "host-changed"
        assert not e.health.called.is_set()
        assert e.channels.created == {}
    finally:
        await stop_environment(e)


async def test_one_initial_bot_never_holds_its_first_update(db, monkeypatch):
    accepted = []

    async def handler(link, update):
        accepted.append(update.update_id)
        return True

    def script(link, normal, updates):
        updates.enqueue("getUpdates", result=[update_payload(10)])
        updates.enqueue("getUpdates", result=[], release=asyncio.Event())

    e = environment(db, monkeypatch, script=script, handler=handler)
    link, token = await credential(db, e.env)
    try:
        await e.supervisor.check()
        await e.supervisor.start()
        await wait_until(lambda: accepted == [10])
        assert e.supervisor.poll_task(link.id).held_count() == 0
        assert e.supervisor.sender._recovered
    finally:
        await stop_environment(e)


async def test_earlier_result_waits_for_complete_first_attempt_cohort(db, monkeypatch):
    accepted, second_start = [], asyncio.Event()
    identities = []

    async def handler(link, update):
        accepted.append((link.id, update.update_id))
        return True

    def script(link, normal, updates):
        if link.id == identities[0]:
            updates.enqueue("getUpdates", result=[update_payload(10)])
            updates.enqueue("getUpdates", result=[], release=asyncio.Event())
        else:
            normal.replies["getMe"][0].release = second_start
            updates.enqueue("getUpdates", result=[], release=asyncio.Event())

    e = environment(db, monkeypatch, script=script, handler=handler)
    first, token = await credential(db, e.env, bot=9_000_000_101)
    second, token = await credential(db, e.env, bot=9_000_000_102)
    identities.extend([first.id, second.id])
    await e.supervisor.check()
    starting = asyncio.create_task(e.supervisor.start())
    try:
        await wait_until(lambda: e.supervisor.poll_task(first.id) is not None and e.supervisor.poll_task(first.id).record.polls == 1)
        assert accepted == []
        assert e.supervisor.poll_task(first.id).held_count() == 1
        assert not e.supervisor.sender._recovered
        e.clock.advance(61)
        await asyncio.sleep(0)
        assert not starting.done()
        assert accepted == []
        second_start.set()
        await asyncio.wait_for(starting, 4)
        await wait_until(lambda: accepted == [(first.id, 10)])
        assert e.supervisor.sender._recovered
    finally:
        second_start.set()
        await stop_environment(e)
        await asyncio.gather(starting, return_exceptions=True)


async def owned_link(db, e, *, bot, owner="owner-a"):
    import secrets

    from app.telegram_bots.cipher import token_cipher, wrap_token

    await db.settings.ensure(owner, timezone="America/New_York", locale="en-US")
    token = str(bot) + ":" + secrets.token_urlsafe(30)
    link = await db.links.add(owner_user_id=owner, agent_kind="custom_agent", agent_ref=f"target-{bot}", bot_id=bot, bot_username=f"fixture_{bot}_bot", encrypted_token=wrap_token(token_cipher(e.env), bot, token), actor_id=owner)
    return link, token


def conflict(normal, updates, *, count=1):
    normal.enqueue("getWebhookInfo", result={"url": "", "has_custom_certificate": False, "pending_update_count": 0})
    for _ in range(count):
        updates.enqueue("getUpdates", status=409, body={"ok": False, "error_code": 409, "description": "Conflict: terminated by other getUpdates request"})
    updates.enqueue("getUpdates", result=[], release=asyncio.Event())


async def poll_results(e, links):
    try:
        await wait_until(lambda: all(e.supervisor.poll_task(link.id) is not None and e.supervisor.poll_task(link.id).record.polls >= 1 for link in links))
    except TimeoutError:
        facts = {link.bot_id: None if e.supervisor.poll_task(link.id) is None else vars(e.supervisor.poll_task(link.id).record) for link in links}
        pytest.fail(f"Poll observations not ready: {facts}; cause={e.supervisor.cause()}; decision={e.supervisor._copy_decision}")


@pytest.mark.parametrize("fail_latch_write", [False, True])
async def test_latch_discards_held_without_chat_or_confirmation_and_owns_sql_retry(db, monkeypatch, fail_latch_write):
    from _telegram_repository_test_support import bind_link
    from sqlalchemy import event
    from sqlalchemy.exc import OperationalError

    accepted, conflict_ids = [], set()
    retry_sleeping = asyncio.Event()
    first_release, first_replies = asyncio.Event(), {}

    async def handler(link, update):
        accepted.append((link.id, update.update_id))
        return True

    def script(link, normal, updates):
        if link.id in conflict_ids:
            conflict(normal, updates)
        else:
            updates.enqueue("getUpdates", result=[update_payload(i) for i in range(100, 115)])
            updates.enqueue("getUpdates", result=[], release=asyncio.Event())
        first_reply = updates.replies["getUpdates"][0]
        first_reply.release = first_release
        first_replies[link.id] = first_reply

    e = environment(db, monkeypatch, script=script, handler=handler)
    links = []
    for i, owner in enumerate(["owner-a", "owner-b", "owner-b", "owner-c"]):
        link, token = await owned_link(db, e, bot=9_000_000_200 + i, owner=owner)
        if i < 3:
            conflict_ids.add(link.id)
        else:
            link = await bind_link(db, link)
        links.append(link)

    async def sleep(seconds):
        if e.supervisor.cause() == "polled-elsewhere" and seconds == 1:
            retry_sleeping.set()
        await e.clock.sleep(seconds)

    e.supervisor._sleep = sleep
    failed = []

    def fail_once(conn, cursor, statement, parameters, context, executemany):
        values = parameters.values() if isinstance(parameters, dict) else parameters
        if fail_latch_write and not failed and statement.upper().startswith("UPDATE TELEGRAM_DEPLOYMENT") and "polled_elsewhere" in values:
            failed.append(True)
            raise OperationalError("UPDATE telegram_deployment", {}, OSError("Offline fixture write failure"))

    event.listen(db.engine.sync_engine, "before_cursor_execute", fail_once)
    await e.supervisor.check()
    starting = asyncio.create_task(e.supervisor.start())
    try:
        # Finish initial SDK dispatch before poll-result SQL competes with the
        # remaining startup writes; the observation budget starts afterward.
        await wait_until(lambda: all(link.id in first_replies for link in links))
        await asyncio.wait_for(asyncio.gather(*(first_replies[link.id].started.wait() for link in links)), 3)
        first_release.set()
        await poll_results(e, links)
        assert not starting.done()
        assert accepted == []
        assert e.supervisor.poll_task(links[-1].id).held_count() == 10
        assert not e.supervisor.sender._recovered
        delivery = await e.supervisor.channel(links[-1].id).send_text("hello.privacy", source_id="a" * 32)
        assert delivery
        baseline = {link.id: await db.links.get(link.id) for link in links}
        e.clock.advance(60)
        if fail_latch_write:
            await asyncio.wait_for(retry_sleeping.wait(), 3)
            assert failed
            assert e.supervisor.cause() == "polled-elsewhere"
            assert not e.supervisor._send_ready
            async with db.sessions() as session:
                assert (await session.get(TelegramDeploymentRow, 1)).polling_state == "running"
            stopping = asyncio.create_task(e.supervisor.stop())
            await asyncio.sleep(0)
            stopping.cancel()
            with pytest.raises(asyncio.CancelledError):
                await stopping
            assert e.supervisor._latch_task is not None and not e.supervisor._latch_task.done()
            e.clock.advance(1)
            await asyncio.wait_for(e.supervisor.stop(), 4)
        else:
            await asyncio.wait_for(starting, 4)
        assert e.supervisor.cause() == "polled-elsewhere"
        assert accepted == []
        assert not e.supervisor.sender._recovered
        assert not e.supervisor._background
        for link in links:
            saved = await db.links.get(link.id)
            before = baseline[link.id]
            assert (saved.state, saved.health, saved.acts_paused_since, saved.notices, saved.binding_generation) == (before.state, before.health, before.acts_paused_since, before.notices, before.binding_generation)
            if link.id in conflict_ids:
                assert saved.connect_conflict_epoch >= 1
            transport = e.channels.created[link.id]
            assert all(call.endpoint in {"getMe", "getWebhookInfo"} for call in transport.normal.calls)
            assert all("offset" not in call.parameters for call in transport.updates.calls)
        async with db.sessions() as session:
            assert (await session.get(TelegramDeploymentRow, 1)).polling_state == "polled_elsewhere"
    finally:
        first_release.set()
        event.remove(db.engine.sync_engine, "before_cursor_execute", fail_once)
        e.clock.advance(1)
        await stop_environment(e)
        await asyncio.gather(starting, return_exceptions=True)


async def test_new_route_links_are_excluded_but_old_route_started_link_counts_once(db, monkeypatch):
    from test_telegram_supervisor import Health

    release = asyncio.Event()
    initial_ids = set()

    def script(link, normal, updates):
        if link.id in initial_ids:
            conflict(normal, updates)
        else:
            updates.enqueue("getUpdates", result=[])
            updates.enqueue("getUpdates", result=[], release=asyncio.Event())

    e = environment(db, monkeypatch, script=script, health=Health(release))
    first, first_token = await owned_link(db, e, bot=9_000_000_301, owner="owner-a")
    second, token = await owned_link(db, e, bot=9_000_000_302, owner="owner-b")
    initial_ids.update([first.id, second.id])
    await e.supervisor.check()
    starting = asyncio.create_task(e.supervisor.start())
    await e.health.called.wait()
    additions = [asyncio.create_task(e.supervisor.add(first, first_token))]
    all_links = [first, second]
    for i in range(3):
        new, token = await owned_link(db, e, bot=9_000_000_310 + i, owner="owner-c")
        all_links.append(new)
        additions.append(asyncio.create_task(e.supervisor.add(new, token)))
    release.set()
    await wait_until(lambda: e.supervisor._ready or 0.5 in e.clock.sleeps)
    if not e.supervisor._ready:
        # Fixture creation can outlast the health request budget; resume its
        # injected retry timer rather than changing that production deadline.
        e.clock.advance(0.5)
    try:
        await poll_results(e, all_links)
        await wait_until(lambda: e.supervisor._cohort_complete)
        assert not e.supervisor.sender._recovered
        assert {link.id for link in e.supervisor._startup_links} == initial_ids
        assert len(e.channels.created[first.id].normal.calls) == 2  # getMe + webhook, once each
        e.clock.advance(60)
        await asyncio.wait_for(starting, 4)
        assert e.supervisor.cause() == "polled-elsewhere"
        assert e.supervisor.copy_guard.broad_conflict_counts(e.supervisor._startup_links, e.supervisor._copy_results()) == {"polled": 2, "counted": 2, "people": 2}
    finally:
        await stop_environment(e)
        await asyncio.gather(starting, *additions, return_exceptions=True)


async def test_unstarted_rows_do_not_delay_single_actual_startup_poller(db, monkeypatch):
    from _telegram_repository_test_support import bind_link

    def script(link, normal, updates):
        conflict(normal, updates)

    e = environment(db, monkeypatch, script=script)
    live, token = await owned_link(db, e, bot=9_000_000_401)
    held, token = await owned_link(db, e, bot=9_000_000_402)
    held = await bind_link(db, held)
    held = await db.links.hold_if_current(held.id, token_revision=held.token_revision, binding_generation=held.binding_generation)
    rejected, token = await owned_link(db, e, bot=9_000_000_403)
    await db.links.set_health(rejected.id, "token_rejected", holds=True, expected_token_revision=rejected.token_revision)
    unreadable, token = await owned_link(db, e, bot=9_000_000_404)
    await db.links.store_token(unreadable.id, "unreadable-fixture", actor_id=unreadable.owner_user_id)
    try:
        await e.supervisor.check()
        await asyncio.wait_for(e.supervisor.start(), 4)
        assert set(e.channels.created) == {live.id}
        assert e.supervisor.sender._recovered
        assert e.supervisor.cause() is None
        assert e.clock.monotonic() == 0
    finally:
        await stop_environment(e)


async def test_same_token_restart_keeps_evidence_and_rejects_old_pair_callbacks(db, monkeypatch):
    second_start, second_result, restart_waiting = asyncio.Event(), asyncio.Event(), asyncio.Event()
    first_id = []

    def script(link, normal, updates):
        if link.id == first_id[0]:
            updates.enqueue("getUpdates", result=[])
            updates.enqueue("getUpdates", result=[], release=second_result)
            updates.enqueue("getUpdates", result=[], release=asyncio.Event())
        else:
            normal.replies["getMe"][0].release = second_start
            updates.enqueue("getUpdates", result=[], release=asyncio.Event())

    e = environment(db, monkeypatch, script=script)
    first, token = await owned_link(db, e, bot=9_000_000_501)
    second, token = await owned_link(db, e, bot=9_000_000_502)
    first_id.append(first.id)
    await e.supervisor.check()
    starting = asyncio.create_task(e.supervisor.start())
    try:
        await wait_until(lambda: e.supervisor.poll_task(first.id) is not None and e.supervisor.poll_task(first.id).record.successes == 1)
        previous = e.supervisor.poll_task(first.id)
        record = previous.record

        def unexpected():
            raise RuntimeError("Offline result callback failure")

        previous.on_result = unexpected

        async def sleep(seconds):
            if seconds == 5:
                restart_waiting.set()
            await e.clock.sleep(seconds)

        e.supervisor._sleep = sleep
        second_result.set()
        await asyncio.wait_for(restart_waiting.wait(), 3)
        assert record.successes == 2
        e.clock.advance(5)
        await wait_until(lambda: e.supervisor.poll_task(first.id) is not previous)
        current = e.supervisor.poll_task(first.id)
        assert current.record is record and current.record.successes == 2
        assert previous._result(conflict=True) is True
        assert record.polls == 2 and record.conflicts == 0
        await previous.on_health("unreachable", True)
        assert (await db.links.get(first.id)).health == "ok"
        assert e.supervisor._first_poll_at == 0
        second_start.set()
        await asyncio.wait_for(starting, 4)
        assert e.supervisor.sender._recovered
    finally:
        second_start.set()
        second_result.set()
        await stop_environment(e)
        await asyncio.gather(starting, return_exceptions=True)


async def test_successful_polling_can_release_both_deployments_without_election(db, monkeypatch):
    def script(link, normal, updates):
        updates.enqueue("getUpdates", result=[])
        updates.enqueue("getUpdates", result=[], release=asyncio.Event())

    first = environment(db, monkeypatch, script=script)
    second = environment(db, monkeypatch, script=script, environ=first.env)
    links = []
    for i in range(2):
        link, token = await owned_link(db, first, bot=9_000_000_600 + i)
        links.append(link)
    try:
        await first.supervisor.check()
        await second.supervisor.check()
        await asyncio.wait_for(asyncio.gather(first.supervisor.start(), second.supervisor.start()), 5)
        await poll_results(first, links)
        await poll_results(second, links)
        assert first.supervisor.sender._recovered and second.supervisor.sender._recovered
        assert first.supervisor.cause() is second.supervisor.cause() is None
        assert first.supervisor.send_allowed() and second.supervisor.send_allowed()
        async with db.sessions() as session:
            assert (await session.get(TelegramDeploymentRow, 1)).polling_state == "running"
    finally:
        await asyncio.gather(stop_environment(first), stop_environment(second))


async def pending_environment(db, monkeypatch):
    from _telegram_repository_test_support import bind_link

    results = asyncio.Event()

    def script(link, normal, updates):
        updates.enqueue("getUpdates", result=[], release=results)
        updates.enqueue("getUpdates", result=[])
        updates.enqueue("getUpdates", result=[], release=asyncio.Event())

    e = environment(db, monkeypatch, script=script)
    links = []
    for i in range(2):
        link, token = await owned_link(db, e, bot=9_000_000_700 + i)
        links.append(await bind_link(db, link))
    await e.supervisor.check()
    starting = asyncio.create_task(e.supervisor.start())
    await wait_until(lambda: e.supervisor._cohort_complete)
    assert not e.supervisor.send_allowed()
    return e, links, starting, results


async def test_pending_decision_gates_notice_claim_profile_takeover_and_negative_offset(db, monkeypatch):
    e, links, starting, results = await pending_environment(db, monkeypatch)
    first, removed = links
    channel = e.supervisor.channel(first.id)
    e.supervisor._schedule_notice(channel, "link.conflict", throttle=True)
    takeover = asyncio.create_task(e.supervisor.retry(first.id, take_over_webhook=True))
    drop = asyncio.create_task(e.supervisor.poll_task(first.id).drop_pending())
    try:
        await asyncio.sleep(0)
        assert (await db.links.get(first.id)).notices == {}
        await e.supervisor.remove(removed.id, actor_id=removed.owner_user_id)
        assert await db.links.get(removed.id) is None
        for link in links:
            transport = e.channels.created[link.id]
            assert [call.endpoint for call in transport.normal.calls] == ["getMe"]
            assert all("offset" not in call.parameters for call in transport.updates.calls)
        transport = e.channels.created[first.id]
        transport.normal.enqueue("deleteWebhook")
        transport.normal.enqueue("sendMessage", result={"message_id": 901, "date": 1_893_456_000, "chat": {"id": first.chat_id, "type": "private"}, "text": "Fixed notice"})
        results.set()
        await asyncio.wait_for(starting, 4)
        await wait_until(lambda: any(call.endpoint == "deleteWebhook" for call in transport.normal.calls) and any(call.endpoint == "sendMessage" for call in transport.normal.calls))
        assert (await db.links.get(first.id)).notices.get("conflict")
        await wait_until(lambda: any(call.parameters.get("offset") == -1 for call in transport.updates.calls))
        assert sum(call.parameters.get("offset") == -1 for call in transport.updates.calls) == 1
    finally:
        results.set()
        takeover.cancel()
        drop.cancel()
        await stop_environment(e)
        await asyncio.gather(starting, takeover, drop, return_exceptions=True)


@pytest.mark.parametrize("webhook", [False, True])
async def test_control_409_uses_pending_health_gate_without_counting_as_poll(db, webhook):
    from test_telegram_conflict import managed_rig

    from app.telegram_bots.polling import PollRecord, PollTask

    async with managed_rig(db) as rig:
        record = PollRecord()
        changes = []

        async def health(cause, holds):
            changes.append((cause, holds))
            await db.links.set_health(rig.link.id, cause, holds=holds, transition=task.health_transition.as_dict() if task.health_transition is not None else None, expected_token_revision=rig.link.token_revision)

        def forbidden_counter():
            raise AssertionError("A control result must not change startup counters")

        task = PollTask(rig.channel, on_health=health, record=record, on_result=forbidden_counter, decision_pending=lambda: True, clock=rig.clock.now, sleep=rig.clock.sleep)
        rig.request.enqueue("getWebhookInfo", result={"url": "https://offline.example.invalid/hook" if webhook else "", "has_custom_certificate": False, "pending_update_count": 0})
        for observed in range(2):
            rig.updates.enqueue("getUpdates", status=409, body={"ok": False, "error_code": 409, "description": "Conflict: terminated by other getUpdates request"})
            if observed:
                rig.clock.advance(61)
            await task.drop_pending()
        saved = await db.links.get(rig.link.id)
        assert saved.connect_conflict_epoch == rig.link.connect_conflict_epoch + 2
        assert record.polls == record.successes == record.conflicts == 0
        assert record.webhook is webhook
        assert saved.health == ("conflict" if webhook else "ok")
        assert bool(changes) is webhook
        assert (saved.acts_paused_since is not None) is webhook


@pytest.mark.parametrize("kind", ["notice", "part"])
async def test_shutdown_during_final_transport_recapture_prevents_raw_dispatch(db, monkeypatch, kind):
    from _telegram_repository_test_support import bind_link
    from sqlalchemy import select

    from app.telegram_bots.adapter import SendContext
    from app.telegram_bots.producer import DeliveryClaimRetired
    from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

    def script(link, normal, updates):
        updates.enqueue("getUpdates", result=[], release=asyncio.Event())

    e = environment(db, monkeypatch, script=script)
    link, token = await owned_link(db, e, bot=9_000_000_801)
    link = await bind_link(db, link)
    await e.supervisor.check()
    await e.supervisor.start()
    channel = e.supervisor.channel(link.id)
    e.channels.created[link.id].normal.enqueue("sendMessage", result={"message_id": 901, "date": 1_893_456_000, "chat": {"id": link.chat_id, "type": "private"}, "text": "Fixed text"})
    entered, release = asyncio.Event(), asyncio.Event()
    if kind == "notice":
        original = channel._notice_context_current
        calls = 0

        async def final_notice_guard(context):
            nonlocal calls
            calls += 1
            if calls == 2:
                entered.set()
                await release.wait()
            return await original(context)

        monkeypatch.setattr(channel, "_notice_context_current", final_notice_guard)
        pending = asyncio.create_task(channel.send_notice("link.conflict", context=SendContext(link.id, link.chat_id, link.binding_generation, link.token_revision, "optional"), product="HartMesh"))
    else:
        identity = await e.supervisor.outbox.plan_product(link, source_id="final-guard", parts=[{"kind": "text", "source_ref": {"key": "hello.privacy", "values": {}}}])
        async with db.sessions() as session, session.begin():
            row = await session.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.id == identity))
            row.state, row.claim_owner, row.claim_id = "sending", e.supervisor.run_id, "b" * 32
            parts = [dict(part) for part in row.parts]
            parts[0]["state"] = "sending"
            row.parts = parts
            part_id = parts[0]["part_id"]
        original = e.supervisor.outbox.capture_sending_part
        calls = 0

        async def final_part_guard(**scope):
            nonlocal calls
            calls += 1
            if calls == 3:
                entered.set()
                await release.wait()
            return await original(**scope)

        monkeypatch.setattr(e.supervisor.outbox, "capture_sending_part", final_part_guard)
        pending = asyncio.create_task(channel.send_part(outbox_id=identity, part_id=part_id, claim_owner=e.supervisor.run_id, claim_id="b" * 32))
    try:
        await asyncio.wait_for(entered.wait(), 3)
        await e.supervisor.begin_shutdown()
        assert channel.application.running
        release.set()
        if kind == "notice":
            assert await asyncio.wait_for(pending, 3) is None
        else:
            with pytest.raises(DeliveryClaimRetired):
                await asyncio.wait_for(pending, 3)
        assert not any(call.endpoint == "sendMessage" for call in e.channels.created[link.id].normal.calls)
    finally:
        release.set()
        await stop_environment(e)
        await asyncio.gather(pending, return_exceptions=True)


async def test_set_aside_success_counts_once_in_floor_until_grace_latch(db, monkeypatch):
    success_id = []

    def script(link, normal, updates):
        if link.id == success_id[0]:
            updates.enqueue("getUpdates", result=[])
            updates.enqueue("getUpdates", result=[], release=asyncio.Event())
        else:
            conflict(normal, updates)

    e = environment(db, monkeypatch, script=script)
    links = []
    for i, owner in enumerate(["owner-a", "owner-b", "owner-c"]):
        link, token = await owned_link(db, e, bot=9_000_000_900 + i, owner=owner)
        links.append(link)
    await db.links.set_health(links[0].id, "conflict", holds=True, expected_token_revision=links[0].token_revision)
    success_id.append(links[0].id)
    await e.supervisor.check()
    starting = asyncio.create_task(e.supervisor.start())
    try:
        await poll_results(e, links)
        await wait_until(lambda: e.supervisor._cohort_complete)
        assert not starting.done()
        assert e.supervisor.copy_guard.can_still_be_broad_conflict(e.supervisor._startup_links, e.supervisor._copy_results())
        assert e.supervisor.poll_task(links[0].id).record.successes == 1
        e.clock.advance(60)
        await asyncio.wait_for(starting, 4)
        assert e.supervisor.cause() == "polled-elsewhere"
    finally:
        await stop_environment(e)
        await asyncio.gather(starting, return_exceptions=True)


async def test_false_decision_does_not_reopen_after_new_token_observations(db, monkeypatch):
    import secrets

    mode = ["success"]

    def script(link, normal, updates):
        if mode[0] == "success":
            updates.enqueue("getUpdates", result=[])
            updates.enqueue("getUpdates", result=[], release=asyncio.Event())
        else:
            conflict(normal, updates)

    e = environment(db, monkeypatch, script=script)
    links = []
    for i, owner in enumerate(["owner-a", "owner-b"]):
        link, token = await owned_link(db, e, bot=9_000_001_000 + i, owner=owner)
        links.append(link)
    try:
        await e.supervisor.check()
        await asyncio.wait_for(e.supervisor.start(), 4)
        await poll_results(e, links)
        decision_task = e.supervisor._copy_task
        assert e.supervisor.send_allowed()
        mode[0] = "conflict"
        for link in links:
            await e.supervisor.replace_token(await db.links.get(link.id), str(link.bot_id) + ":" + secrets.token_urlsafe(30), actor_id=link.owner_user_id)
        await poll_results(e, links)
        assert e.supervisor.copy_guard.broad_conflict(e.supervisor._startup_links, e.supervisor._copy_results())
        assert e.supervisor._copy_task is decision_task and decision_task.done()
        assert e.supervisor.cause() is None and e.supervisor.send_allowed()
    finally:
        await stop_environment(e)


async def test_retired_held_buffer_is_discarded_while_current_suffix_admits(db, monkeypatch):
    from _telegram_repository_test_support import bind_link

    from app.telegram_bots.inbox import InboxRepository

    first_id, accepted = [], []
    other_start = asyncio.Event()
    inbox = InboxRepository(db.sessions)

    async def handler(link, update):
        await inbox.accept(link, update, sanitized_input={"text": "Synthetic input"})
        accepted.append((link.id, update.update_id))
        return True

    def script(link, normal, updates):
        if link.id == first_id[0]:
            updates.enqueue("getUpdates", result=[update_payload(42)])
            updates.enqueue("getUpdates", result=[], release=asyncio.Event())
        else:
            normal.replies["getMe"][0].release = other_start
            updates.enqueue("getUpdates", result=[], release=asyncio.Event())

    e = environment(db, monkeypatch, script=script, handler=handler)
    first, token = await owned_link(db, e, bot=9_000_001_101)
    first = await bind_link(db, first)
    other, other_token = await owned_link(db, e, bot=9_000_001_102)
    first_id.append(first.id)
    await e.supervisor.check()
    starting = asyncio.create_task(e.supervisor.start())
    try:
        await wait_until(lambda: e.supervisor.poll_task(first.id) is not None and e.supervisor.poll_task(first.id).held_count() == 1)
        retired = e.supervisor.poll_task(first.id)
        await e.supervisor._retire(first.id, reason="token-stored")
        await e.supervisor.add(first, token)
        current = e.supervisor.poll_task(first.id)
        await wait_until(lambda: current.held_count() == 1)
        assert retired.held_count() == 1 and accepted == []
        other_start.set()
        await asyncio.wait_for(starting, 4)
        await wait_until(lambda: accepted == [(first.id, 42)] and current.offset == 43)
        assert retired.held_count() == 0
        assert not e.supervisor._startup_polls
        assert current.offset == 43 and current.held_count() == 0
        e.channels.created[first.id].updates.enqueue("getUpdates", result=[])
        await wait_until(lambda: len(e.channels.created[first.id].updates.calls) == 2)
    finally:
        other_start.set()
        await stop_environment(e)
        await asyncio.gather(starting, return_exceptions=True)


async def test_false_decision_starts_sender_despite_failed_middle_held_admission(db, monkeypatch):
    from _telegram_repository_test_support import bind_link

    from app.telegram_bots.inbox import InboxRepository

    first_id, accepted = [], []
    other_start, permit_middle = asyncio.Event(), asyncio.Event()
    inbox = InboxRepository(db.sessions)

    async def handler(link, update):
        if update.update_id == 11 and not permit_middle.is_set():
            return False
        await inbox.accept(link, update, sanitized_input={"text": "Synthetic input"})
        accepted.append(update.update_id)
        return True

    def script(link, normal, updates):
        if link.id == first_id[0]:
            updates.enqueue("getUpdates", result=[update_payload(i) for i in range(10, 13)])
            updates.enqueue("getUpdates", result=[], release=asyncio.Event())
        else:
            normal.replies["getMe"][0].release = other_start
            updates.enqueue("getUpdates", result=[], release=asyncio.Event())

    e = environment(db, monkeypatch, script=script, handler=handler)
    first, token = await owned_link(db, e, bot=9_000_001_201)
    first = await bind_link(db, first)
    other, token = await owned_link(db, e, bot=9_000_001_202)
    first_id.append(first.id)
    await e.supervisor.check()
    starting = asyncio.create_task(e.supervisor.start())
    try:
        await wait_until(lambda: e.supervisor.poll_task(first.id) is not None and e.supervisor.poll_task(first.id).held_count() == 3)
        other_start.set()
        await asyncio.wait_for(starting, 4)
        await wait_until(lambda: accepted == [10])
        assert e.supervisor.sender._recovered and e.supervisor.send_allowed()
        poll = e.supervisor.poll_task(first.id)
        assert poll.offset is None
        assert len(e.channels.created[first.id].updates.calls) == 1
        e.channels.created[first.id].updates.enqueue("getUpdates", result=[])
        await poll.confirm_offset()
        assert len(e.channels.created[first.id].updates.calls) == 1
        permit_middle.set()
        poll.wake()
        await wait_until(lambda: accepted == [10, 11, 12] and poll.offset == 13)
        assert poll.offset == 13
        await wait_until(lambda: len(e.channels.created[first.id].updates.calls) == 2)
    finally:
        other_start.set()
        permit_middle.set()
        await stop_environment(e)
        await asyncio.gather(starting, return_exceptions=True)


async def test_grace_origin_waits_for_actual_poll_after_getme_and_sql(db, monkeypatch):
    get_me, sql_release = asyncio.Event(), asyncio.Event()
    sql_entered = set()

    def script(link, normal, updates):
        normal.replies["getMe"][0].release = get_me
        conflict(normal, updates)

    e = environment(db, monkeypatch, script=script)
    links = []
    for i, owner in enumerate(["owner-a", "owner-b"]):
        link, token = await owned_link(db, e, bot=9_000_001_300 + i, owner=owner)
        links.append(link)
    original = db.links.update_if_current

    async def delayed_status(identity, **facts):
        if "can_manage_bots" in facts and not sql_release.is_set():
            sql_entered.add(identity)
            await sql_release.wait()
        return await original(identity, **facts)

    monkeypatch.setattr(db.links, "update_if_current", delayed_status)
    await e.supervisor.check()
    starting = asyncio.create_task(e.supervisor.start())
    try:
        await wait_until(lambda: len(e.channels.created) == 2 and all(record.normal.calls for record in e.channels.created.values()))
        e.clock.advance(120)
        assert e.supervisor._first_poll_at is None and not starting.done()
        get_me.set()
        await wait_until(lambda: len(sql_entered) == 2)
        e.clock.advance(80)
        assert e.supervisor._first_poll_at is None and not starting.done()
        sql_release.set()
        await poll_results(e, links)
        assert e.supervisor._first_poll_at == 200
        e.clock.advance(59)
        await asyncio.sleep(0)
        assert e.supervisor.cause() is None and not starting.done()
        e.clock.advance(1)
        await asyncio.wait_for(starting, 4)
        assert e.supervisor.cause() == "polled-elsewhere"
    finally:
        get_me.set()
        sql_release.set()
        await stop_environment(e)
        await asyncio.gather(starting, return_exceptions=True)


@pytest.mark.parametrize("mode", ["drop", "confirm"])
async def test_control_gate_is_rechecked_after_delayed_sql_guard(db, monkeypatch, mode):
    from test_telegram_conflict import managed_rig

    from app.telegram_bots.polling import PollRecord, PollTask

    async with managed_rig(db) as rig:
        entered, release = asyncio.Event(), asyncio.Event()
        allowed = [True]
        original = rig.channel.refresh_link

        async def delayed_current():
            entered.set()
            await release.wait()
            return await original()

        monkeypatch.setattr(rig.channel, "refresh_link", delayed_current)

        async def health(cause, holds):
            raise AssertionError("No control response should be dispatched")

        task = PollTask(rig.channel, on_health=health, record=PollRecord(), control_allowed=lambda call: allowed[0], clock=rig.clock.now, sleep=rig.clock.sleep)
        if mode == "confirm":
            task.offset = 17
        rig.updates.enqueue("getUpdates", result=[])
        sending = asyncio.create_task(task.drop_pending() if mode == "drop" else task.confirm_offset())
        try:
            await asyncio.wait_for(entered.wait(), 3)
            allowed[0] = False
            release.set()
            await asyncio.wait_for(sending, 3)
            assert rig.updates.calls == []
            assert task.record.polls == 0
        finally:
            release.set()
            await asyncio.gather(sending, return_exceptions=True)


@pytest.mark.parametrize("kind", ["profile", "takeover"])
async def test_optional_control_rechecks_shutdown_after_its_last_await(db, monkeypatch, kind):
    from _telegram_repository_test_support import bind_link

    def script(link, normal, updates):
        updates.enqueue("getUpdates", result=[], release=asyncio.Event())

    e = environment(db, monkeypatch, script=script)
    link, token = await owned_link(db, e, bot=9_000_001_401)
    link = await bind_link(db, link)
    await e.supervisor.check()
    await e.supervisor.start()
    entered, release = asyncio.Event(), asyncio.Event()
    if kind == "profile":
        original = e.supervisor.settings.get

        async def delayed_settings(owner):
            entered.set()
            await release.wait()
            return await original(owner)

        monkeypatch.setattr(e.supervisor.settings, "get", delayed_settings)
        operation = asyncio.create_task(e.supervisor.remove(link.id, actor_id=link.owner_user_id, last_text=None))
    else:
        original = e.supervisor.links.update_if_current

        async def delayed_guard(identity, **facts):
            if asyncio.current_task().get_name() == "takeover-caller":
                entered.set()
                await release.wait()
            return await original(identity, **facts)

        monkeypatch.setattr(e.supervisor.links, "update_if_current", delayed_guard)
        operation = asyncio.create_task(e.supervisor.retry(link.id, take_over_webhook=True), name="takeover-caller")
    try:
        await asyncio.wait_for(entered.wait(), 3)
        await e.supervisor.begin_shutdown()
        release.set()
        await asyncio.wait_for(operation, 3)
        assert [call.endpoint for call in e.channels.created[link.id].normal.calls] == ["getMe"]
    finally:
        release.set()
        await stop_environment(e)
        await asyncio.gather(operation, return_exceptions=True)


async def test_resolved_false_shutdown_still_confirms_only_admitted_offset(db, monkeypatch):
    admitted = []

    def script(link, normal, updates):
        updates.enqueue("getUpdates", result=[update_payload(73)])
        updates.enqueue("getUpdates", result=[], release=asyncio.Event())
        updates.enqueue("getUpdates", result=[])

    async def handler(link, update):
        admitted.append(update.update_id)
        return True

    e = environment(db, monkeypatch, script=script, handler=handler)
    link, token = await owned_link(db, e, bot=9_000_001_501)
    try:
        await e.supervisor.check()
        await e.supervisor.start()
        await wait_until(lambda: e.supervisor.poll_task(link.id).offset == 74)
        await wait_until(lambda: len(e.channels.created[link.id].updates.calls) == 2)
        assert admitted == [73]
        await stop_environment(e)
        calls = e.channels.created[link.id].updates.calls
        assert calls[-1].parameters["offset"] == 74
        assert calls[-1].parameters["timeout"] == 0
        assert sum(call.parameters.get("timeout") == 0 for call in calls) == 1
    finally:
        await stop_environment(e)


@pytest.mark.parametrize("phase", ["backoff", "active-commit", "active-failure", "quiescence"])
async def test_expired_gateway_grace_retires_future_latch_work_and_drains_active_owner(db, monkeypatch, caplog, phase):
    import logging

    from sqlalchemy import event
    from sqlalchemy.exc import OperationalError

    def script(link, normal, updates):
        conflict(normal, updates)

    e = environment(db, monkeypatch, script=script)
    links = []
    for i, owner in enumerate(["owner-a", "owner-b"]):
        link, token = await owned_link(db, e, bot=9_000_001_600 + i, owner=owner)
        links.append(link)
    entered, release, backoff = asyncio.Event(), asyncio.Event(), asyncio.Event()
    attempts = []

    async def paused_sql(driver):
        entered.set()
        await release.wait()

    def intercept(conn, cursor, statement, parameters, context, executemany):
        values = parameters.values() if isinstance(parameters, dict) else parameters
        if not statement.upper().startswith("UPDATE TELEGRAM_DEPLOYMENT") or "polled_elsewhere" not in values:
            return
        attempts.append(True)
        if phase in {"active-commit", "active-failure"}:
            # An actual transaction and its deployment row lock are live here.
            # AdaptedConnection's public bridge pauses the asynchronous driver
            # without a second loop or an invented database implementation.
            conn.connection.dbapi_connection.run_async(paused_sql)
        if phase in {"backoff", "active-failure"}:
            raise OperationalError("UPDATE telegram_deployment", {}, OSError("Offline fixture write failure"))

    event.listen(db.engine.sync_engine, "before_cursor_execute", intercept)

    async def sleep(seconds):
        if e.supervisor.cause() == "polled-elsewhere" and seconds == 1:
            backoff.set()
        await e.clock.sleep(seconds)

    e.supervisor._sleep = sleep
    await e.supervisor.check()
    if phase == "quiescence":
        stop_sender = e.supervisor.sender.stop

        async def paused_quiescence():
            entered.set()
            await release.wait()
            await stop_sender()

        monkeypatch.setattr(e.supervisor.sender, "stop", paused_quiescence)
    caplog.set_level(logging.ERROR, logger="app.telegram_bots.supervisor")
    starting = asyncio.create_task(e.supervisor.start())
    retiring = None
    try:
        await poll_results(e, links)
        e.clock.advance(60)
        await asyncio.wait_for((backoff if phase == "backoff" else entered).wait(), 4)
        assert e.supervisor.cause() == "polled-elsewhere"
        assert not e.supervisor._latch_retired.is_set()
        retiring = asyncio.create_task(e.supervisor.drain_after_stop_timeout())
        if phase != "backoff":
            await asyncio.sleep(0.02)
            assert not retiring.done()
            retiring.cancel()
            await asyncio.sleep(0)
            retiring.cancel()
            await asyncio.sleep(0)
            assert not retiring.done()
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(retiring, 4)
        else:
            await asyncio.wait_for(retiring, 4)
        assert e.supervisor._stop_task.done() and e.supervisor._latch_task.done()
        assert e.supervisor._stopped
        assert e.supervisor.cause() == "polled-elsewhere"
        assert all(record.normal.closed and record.updates.closed for record in e.channels.created.values())
        expected = "polled_elsewhere" if phase == "active-commit" else "running"
        async with db.sessions() as session:
            assert (await session.get(TelegramDeploymentRow, 1)).polling_state == expected
        assert len(attempts) == (0 if phase == "quiescence" else 1)
        diagnostics = [record for record in caplog.records if record.getMessage() == "telegram_bots copy_latch_unpersisted"]
        assert len(diagnostics) == (0 if phase == "active-commit" else 1)
        assert all(record.levelno == logging.ERROR for record in diagnostics)
        e.clock.advance(1000)
        await asyncio.sleep(0)
        assert len(attempts) == (0 if phase == "quiescence" else 1)
    finally:
        release.set()
        await e.supervisor.drain_after_stop_timeout()
        await asyncio.gather(starting, *(task for task in (retiring,) if task is not None), return_exceptions=True)
        event.remove(db.engine.sync_engine, "before_cursor_execute", intercept)
