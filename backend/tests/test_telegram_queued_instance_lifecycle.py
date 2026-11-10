"""Queued instance inputs close only after SQL proves they were not published."""

import asyncio

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select, update
from test_telegram_instance_admission import admission as admission
from test_telegram_instance_lifecycle import lifecycle as lifecycle
from test_telegram_instance_publication import publication as publication
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging

from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow, AgentInstanceRow
from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow

pytestmark = pytest.mark.asyncio


async def withdraw(e, status):
    async with e.db.sessions.begin() as writer:
        if status == "use":
            await writer.execute(update(AgentInstanceGrantRow).values(permissions=2))
        else:
            await writer.execute(update(AgentInstanceRow).values(status=status))


async def closed(e):
    async with e.db.sessions() as reader:
        row = await reader.scalar(select(TelegramInboundMarkRow).where(TelegramInboundMarkRow.message_id == e.origin.message_id))
        assert row.route_complete and row.state == "done" and row.turn_id is None
    assert e.turn.input_closed and e.turn.record is None and e.bus.inbound_queue.empty()


@pytest.mark.parametrize("status,key", [("use", "access.agent_off"), ("suspended", "agent.paused"), ("archived", "agent.retired")])
@pytest.mark.parametrize("health", ["ok", "agent_unavailable"])
async def test_queued_input_closes_before_storage_or_execution(admission, monkeypatch, status, key, health):
    from app.gateway import storage_spaces

    e = admission
    notices = []

    async def notice(link):
        notices.append((await e.live.authority.instance_lifecycle(link))[0])

    async def forbidden(*args, **kwargs):
        raise AssertionError("Unavailable input must not reach storage or execution")

    e.channel._services.notice_instance_unavailable = notice
    monkeypatch.setattr(storage_spaces, "require_execution", forbidden)
    monkeypatch.setattr(storage_spaces, "storage_state", forbidden)
    await withdraw(e, status)
    if health != "ok":
        e.channel.link = await e.db.links.set_health(e.link.id, health, holds=True, captured_link=e.link, expected_token_revision=e.link.token_revision)
    assert await e.turns._prepare(e.turn) is False
    await closed(e)
    assert notices == [key]
    assert not (await e.inbox.prepublication_page(link_id=e.link.id)).entries


async def test_late_use_loss_during_thread_preparation_closes_original_input(admission):
    e = admission
    original = e.threads.ensure_thread
    notices = []

    async def prepare(*args, **kwargs):
        result = await original(*args, **kwargs)
        await withdraw(e, "use")
        return result

    async def notice(link):
        notices.append(link.id)

    e.threads.ensure_thread = prepare
    e.channel._services.notice_instance_unavailable = notice
    assert await e.turns._prepare(e.turn) is False
    await closed(e)
    assert notices == [e.link.id]


async def test_lost_reservation_response_cannot_close_a_publication(admission):
    e = admission
    inbox = e.channel._services.outbox.inbox
    original = inbox.reserve_publication

    async def reserve(*args, **kwargs):
        await original(*args, **kwargs)
        await withdraw(e, "use")
        raise RuntimeError("Synthetic lost committed reservation result")

    inbox.reserve_publication = reserve
    with pytest.raises(ValueError, match="input-already-published"):
        await e.turns._prepare(e.turn)
    assert not e.turn.input_closed and e.turn.record is None and e.bus.inbound_queue.empty()
    async with e.db.sessions() as reader:
        row = await reader.scalar(select(TelegramInboundMarkRow).where(TelegramInboundMarkRow.message_id == e.origin.message_id))
        assert row.turn_id is not None and not row.route_complete


async def test_capacity_wait_rechecks_lifecycle_before_retry(admission):
    from deerflow.agent_instances.contract import ResidentRunLimit

    e = admission
    calls, notices = [], []

    async def admit(view, *, work):
        calls.append(view.id)
        raise ResidentRunLimit(view.name)

    async def notice(link):
        notices.append(link.id)

    e.http_app.state.agent_instances.residents.admit = admit
    e.channel._services.notice_instance_unavailable = notice
    task = asyncio.create_task(e.turns._prepare(e.turn))
    try:
        for _ in range(300):
            if e.turn.capacity_notice_sent:
                break
            await asyncio.sleep(0.01)
        assert e.turn.capacity_notice_sent
        await withdraw(e, "use")
        e.channel._services.outbox.clock.__self__.advance(10)
        e.turn.wake.set()
        assert await asyncio.wait_for(task, 5) is False
        await closed(e)
        assert len(calls) == 1 and notices == [e.link.id]
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("published", [False, True])
async def test_callback_owned_retry_closes_only_without_its_own_publication(admission, published):
    from app.telegram_bots.inbox import CALLBACK_PUBLICATION_KEY
    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow

    e = admission
    callbacks = e.channel._services.callbacks
    wait_id = await e.channel.send_text("busy.followup", source_id=f"chat-wait:{e.link.chat_id}:{e.origin.message_id}", inbox_rows=[e.origin], _captured_link=e.link)
    wait_notice = await e.channel._services.outbox.get(e.link, wait_id)
    parent = await e.turns._input_outcome(e.turn, "refused.restarting", keyboard=e.turns._button("retry_message", str(e.origin.message_id), target_kind="request"))
    async with e.db.sessions() as reader:
        selected = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "retry_message"))
        identity, group = selected.id, selected.group_id
    assert await callbacks.adopt(e.link, group, 2401, chat_id=e.link.chat_id, message_thread_id=None, outbox_id=parent, presentation_version=0, check_current=lambda: True)
    guard = e.turns._guard(e.turn, repair=True)
    stage = await e.inbox.stage_callback_input(e.link, callback_id=identity, update_id=2402, chat_id=e.link.chat_id, message_id=2401, message_thread_id=None, callbacks=callbacks, guard=guard)
    e.turn.callback_id, e.turn.origins, e.turn.input_closed = identity, stage.origins, False
    assert e.turns.preview_allowed(e.channel, wait_notice) is False
    e.turn.input_closed = True
    assert e.turns.queue.admit_callback(e.link, e.turn.inbound, callback_id=identity) == "first-waiting"
    assert e.turns.preview_allowed(e.channel, wait_notice) is False
    e.turns.queue.cancel_wait(e.link, None)
    e.turn.input_closed = False
    if published:
        thread = await e.threads.ensure_callback_thread(e.link, callback_id=identity)
        await e.inbox.reserve_callback_publication(e.link, identity, thread_id=thread, endpoint_id=e.http_client.endpoint_id, guard=guard)
    await withdraw(e, "use")
    notices = []

    async def notice(link):
        notices.append(link.id)

    e.channel._services.notice_instance_unavailable = notice
    if published:
        with pytest.raises(ValueError, match="input-already-published"):
            await e.turns._prepare(e.turn)
    else:
        assert await e.turns._prepare(e.turn) is False
        assert await e.turns._prepare(e.turn) is False
    async with e.db.sessions() as reader:
        selected = await reader.get(TelegramCallbackRow, identity)
        data = selected.payload[CALLBACK_PUBLICATION_KEY]
        assert data["route_complete"] is (not published)
        assert (data["state"] == "done") is (not published)
        assert (selected.consumed_at is not None) is (not published)
    assert e.bus.inbound_queue.empty()
    assert bool(notices) is (not published)


@pytest.mark.parametrize("ending", ["cancel", "retire"])
async def test_closure_owns_final_writer_and_rechecks_original_lifetime(admission, ending):
    e = admission
    entered, release = asyncio.Event(), asyncio.Event()
    factory = e.db.links.input_closure_guard

    def guarded(*args, **kwargs):
        original = factory(*args, **kwargs)

        class Guard:
            captured_link = original.captured_link
            calls = 0

            async def check(self, writer):
                self.calls += 1
                if self.calls == 2:
                    entered.set()
                    await release.wait()
                return await original.check(writer)

        return Guard()

    e.db.links.input_closure_guard = guarded
    await withdraw(e, "use")
    task = asyncio.create_task(e.turns._prepare(e.turn))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        if ending == "cancel":
            task.cancel()
            await asyncio.sleep(0)
            task.cancel()
            await asyncio.sleep(0)
            assert not task.done()
        else:
            e.turn.active = False
        release.set()
        with pytest.raises(asyncio.CancelledError if ending == "cancel" else ValueError):
            await task
        async with e.db.sessions() as reader:
            row = await reader.scalar(select(TelegramInboundMarkRow).where(TelegramInboundMarkRow.message_id == e.origin.message_id))
            assert row.route_complete is (ending == "cancel")
            assert row.turn_id is None
    finally:
        release.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_queued_followups_close_through_supervisor_notice_and_do_not_restart(lifecycle):
    from test_telegram_bot_adapter import message as sent_message
    from test_telegram_bot_handlers import message

    e = lifecycle
    turns = e.supervisor.turns
    inbox = e.supervisor.outbox.inbox
    await e.supervisor.sender.stop()
    inputs = []
    for identity in (2511, 2512):
        incoming = message(e, identity, content="Queued question")
        row, _ = await inbox.accept(e.link, incoming, sanitized_input={"type": "text", "text": incoming.message.text})
        row = await inbox.route(row, kind="turn", target_id=e.link.agent_ref, basis={"input_profile": {"version": 1, "kind": "plain_text", "forwarded": False}}, operation_ids=[])
        inbound = turns._inbound(e.link, (row,))
        if inputs:
            await turns._admit_origins(e.channel, e.link, (row,))
        else:
            assert turns.queue.admit(e.link, inbound) == "run"
            turns._origins[(e.link.id, row.message_id)] = row
            turns._origin_channels[(e.link.id, row.message_id)] = e.channel
        inputs.append(inbound)
    from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

    async with e.db.sessions() as reader:
        notice = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == f"notice:chat-wait:{e.link.chat_id}:2512"))
        assert notice.state == "pending"
        notice_id = notice.id
    await withdraw(e, "use")
    sent_message(e.runtime.normal)
    turns._start(e.channel, e.link, inputs[0])
    for _ in range(500):
        if not turns._turns:
            break
        await asyncio.sleep(0.01)
    assert not turns._turns
    async with e.db.sessions() as reader:
        rows = list(await reader.scalars(select(TelegramInboundMarkRow).where(TelegramInboundMarkRow.message_id.in_([2511, 2512]))))
        assert len(rows) == 2 and all(row.route_complete and row.state == "done" and row.turn_id is None for row in rows)
    texts = [call.parameters["text"] for call in e.runtime.normal.calls if call.endpoint == "sendMessage"]
    assert len(texts) == 1 and "You no longer have access" in texts[0]
    async with e.db.sessions.begin() as writer:
        await writer.execute(update(AgentInstanceGrantRow).values(permissions=7))
    await e.supervisor.reconcile()
    assert not turns._turns and not (await inbox.prepublication_page(link_id=e.link.id)).entries
    assert e.runtime.service.bus.inbound_queue.empty()
    async with e.db.sessions() as reader:
        notice = await reader.get(TelegramOutboxRow, notice_id)
        assert (notice.state, notice.skip_reason, notice.claim_id) == ("skipped", "superseded", None)


@pytest.mark.parametrize("claimed", [False, True, "final_authority"])
async def test_closed_input_retires_wait_delivery_before_raw_send_after_restore(admission, claimed):
    from app.telegram_bots.producer import OutboxSender
    from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

    e = admission
    services = e.channel._services
    services.turns = e.turns
    services.eligibility = e.live.authority.delivery
    await e.db.links.end_pending(e.link.id, skip_reason="superseded")
    identity = await e.channel.send_text("busy.followup", source_id=f"chat-wait:{e.link.chat_id}:{e.origin.message_id}", inbox_rows=[e.origin], _captured_link=e.link)
    entered, release = asyncio.Event(), asyncio.Event()

    async def pause(delay):
        entered.set()
        await release.wait()
        e.channel.bot.rate_limiter._short_hold_until = 0

    async def raw(*, link_id, **kwargs):
        return await e.channel.send_part(**kwargs)

    async def notice(link):
        pass  # The real supervisor notice/throttle is exercised above.

    services.notice_instance_unavailable = notice
    final_checks = []

    async def close_and_restore():
        await withdraw(e, "use")
        assert await e.turns._prepare(e.turn) is False
        await closed(e)
        async with e.db.sessions.begin() as writer:
            await writer.execute(update(AgentInstanceGrantRow).values(permissions=7))

    if claimed == "final_authority":
        import inspect

        async def eligibility(*args):
            final = inspect.currentframe().f_back.f_code.co_name == "check_instance_part"
            verdict = await e.live.authority.delivery(*args)
            if final:
                assert verdict.decision == "allow"
                final_checks.append(True)
                await close_and_restore()
            return verdict

        services.eligibility = eligibility
    sender = OutboxSender(services.outbox, run_id="queued-fixture", deliver_part=raw, eligibility=e.live.authority.delivery, jitter=lambda: 0)
    try:
        if claimed is True:
            e.channel.bot.rate_limiter._short_hold_until = e.channel.bot.rate_limiter._time() + 10
            e.channel.bot.rate_limiter._sleep = pause
            await sender.tick()
            await asyncio.wait_for(entered.wait(), 5)
        if claimed != "final_authority":
            await close_and_restore()
        release.set()
        await sender.tick()
        await asyncio.wait_for(sender.drain(), 5)
        assert bool(final_checks) is (claimed == "final_authority")
        assert not [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
        async with e.db.sessions() as reader:
            notice = await reader.get(TelegramOutboxRow, identity)
            assert (notice.state, notice.skip_reason, notice.claim_id, notice.claim_owner) == ("skipped", "superseded", None, None)
    finally:
        release.set()
        await sender.stop()
