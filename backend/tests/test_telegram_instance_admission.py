"""Instance send refusals keep real SQL input and never publish an unavailable run."""

import asyncio

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_instance_publication import publication as publication

from app.telegram_bots.callbacks import CallbackRepository
from app.telegram_bots.turns import ChatTurns, _Turn
from deerflow.agent_instances.contract import ExecutionUnavailable, ResidentFilesBusy, ResidentRestarting, ResidentRunLimit
from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow, TelegramOutboxRow

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture
async def admission(publication):
    e = publication
    services = e.channel._services
    services.authority = e.live.authority
    services._app = e.http_app
    services._app_config = e.live.config
    services.thread_client = e.http_client
    services.threads = e.threads
    services.publication_authority = e.host
    services.publications = e.coordinator
    services.prepare_live_owner = e.live.authority.prepare_live_owner
    services.callbacks = CallbackRepository(e.db.sessions)

    async def values(link):
        return {"name": (await e.live.authority.instance_view(link)).name}

    services.prepare_ingress_values = values
    e.turns = ChatTurns(services)
    incoming = e.turns._inbound(e.link, (e.origin,))
    assert e.turns.queue.admit(e.link, incoming) == "run"
    e.turn = _Turn(e.channel, e.link, incoming, (e.origin,))
    e.turns._turns[(e.link.id, None)] = e.turn
    try:
        yield e
    finally:
        e.turns._turns.clear()
        await e.turns.stop()


@pytest.mark.parametrize(
    "reason,key",
    [
        ("agents-none", "unavailable.off"),
        ("operator-hold", "unavailable.paused"),
        ("host-replaced", "unavailable.moved"),
        ("space-unverified", "unavailable.files_unchecked"),
        ("storage-unavailable", "unavailable.fix"),
        ("unknown-code", "unavailable.fix"),
    ],
)
async def test_execution_refusal_plans_fixed_input_ending_and_starts_nothing(admission, monkeypatch, reason, key):
    from app.gateway import storage_spaces

    e = admission

    async def refuse(*args):
        raise ExecutionUnavailable(reason)

    async def storage(app):
        return storage_spaces.StorageState("unavailable", "inventory-unreadable")

    monkeypatch.setattr(storage_spaces, "require_execution", refuse)
    monkeypatch.setattr(storage_spaces, "storage_state", storage)
    assert await e.turns._prepare(e.turn) is False
    assert e.turn.input_closed and e.turn.record is None and e.bus.inbound_queue.empty()
    async with e.db.sessions() as reader:
        rows = list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == e.link.id)))
        reply = next(row for row in rows if row.parts[0]["source_ref"]["key"] == key)
        source = reply.parts[0]["source_ref"]
        if reason == "storage-unavailable":
            assert source["values"]["code"] == "storage-unreadable"
        marks = list(await reader.scalars(select(TelegramInboundMarkRow).where(TelegramInboundMarkRow.message_id == e.origin.message_id)))
        assert marks and all(mark.route_complete and mark.turn_id is None for mark in marks)


@pytest.mark.parametrize("error,key", [(ResidentRestarting, "refused.restarting"), (ResidentFilesBusy, "refused.files_busy")])
async def test_manual_send_refusal_is_not_retried(admission, error, key):
    e = admission
    calls = []

    async def refuse(view, *, work):
        calls.append((view.id, work))
        raise error(view.name, cause="lifecycle")

    e.http_app.state.agent_instances.residents.admit = refuse
    assert await e.turns._prepare(e.turn) is False
    assert calls == [(e.link.agent_ref, False)] and e.bus.inbound_queue.empty()
    async with e.db.sessions() as reader:
        rows = list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == e.link.id)))
        assert any(row.parts[0]["source_ref"]["key"] == key for row in rows)


@pytest.mark.parametrize("cancel", ["stop", "button"])
async def test_run_limit_wait_is_cancelled_before_any_publication(admission, cancel):
    e = admission
    entered = asyncio.Event()

    async def refuse(view, *, work):
        entered.set()
        raise ResidentRunLimit(view.name)

    e.http_app.state.agent_instances.residents.admit = refuse
    task = e.turn.task = asyncio.create_task(e.turns._prepare(e.turn))
    try:
        await asyncio.wait_for(entered.wait(), 3)
        for _ in range(100):
            if e.turn.capacity_notice_sent:
                break
            await asyncio.sleep(0.01)
        assert e.turn.capacity_waiting and e.turn.capacity_notice_sent
        assert not task.done() and e.bus.inbound_queue.empty()
        if cancel == "stop":
            await e.turns._stop_turn(e.turn, stop_id="fixture:stop")
        else:
            from deerflow.persistence.telegram_bots.model import TelegramCallbackRow

            callbacks = e.channel._services.callbacks
            async with e.db.sessions() as reader:
                saved = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.link_id == e.link.id, TelegramCallbackRow.kind == "cancel_wait"))
                identity, group, parent = saved.id, saved.group_id, saved.payload["anchor_outbox_id"]
            assert await callbacks.adopt(e.link, group, 2300, chat_id=e.link.chat_id, message_thread_id=None, outbox_id=parent, presentation_version=0, check_current=lambda: True)
            await e.turns.execute_callback(e.channel, await callbacks.get(identity), update_id=2301, query_id="fixture-cancel")
        assert await asyncio.wait_for(task, 3) is False
        assert e.turn.input_closed and e.turn.record is None and e.bus.inbound_queue.empty()
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("ending", ["ready", "gave_up"])
async def test_run_limit_wait_obeys_retry_deadline_and_publishes_at_most_once(admission, ending):
    e = admission
    allowed, entered = asyncio.Event(), asyncio.Event()

    async def admit(view, *, work):
        entered.set()
        if not allowed.is_set():
            raise ResidentRunLimit(view.name)

    e.http_app.state.agent_instances.residents.admit = admit
    task = e.turn.task = asyncio.create_task(e.turns._prepare(e.turn))
    try:
        await asyncio.wait_for(entered.wait(), 3)
        for _ in range(100):
            if e.turn.capacity_notice_sent:
                break
            await asyncio.sleep(0.01)
        assert e.turn.capacity_notice_sent and e.bus.inbound_queue.empty()
        if ending == "ready":
            allowed.set()
        e.channel._services.outbox.clock.__self__.advance(10 if ending == "ready" else 120)
        e.turn.wake.set()
        assert await asyncio.wait_for(task, 5) is (ending == "ready")
        assert e.bus.inbound_queue.qsize() == (1 if ending == "ready" else 0)
        assert e.turn.input_closed is (ending == "gave_up")
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_lost_use_never_reads_storage_diagnostic_after_readiness_refusal(admission, monkeypatch):
    from sqlalchemy import update

    from app.gateway import storage_spaces
    from app.telegram_bots.instance_refusals import describe_send_refusal
    from deerflow.agent_instances.contract import AgentDenied
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow

    e = admission
    async with e.db.sessions.begin() as writer:
        await writer.execute(update(AgentInstanceGrantRow).values(permissions=2))

    async def forbidden(app):
        raise AssertionError("Storage diagnostics cannot precede current Use")

    monkeypatch.setattr(storage_spaces, "storage_state", forbidden)
    with pytest.raises(AgentDenied):
        await describe_send_refusal(e.live.authority, e.link, ExecutionUnavailable("storage-unavailable"))


@pytest.mark.parametrize("reason", ["busy", "starting-elsewhere", "start-failed", "files-full", "work-limit"])
async def test_only_send_refusals_can_select_prepublication_retry(admission, reason):
    from app.telegram_bots.instance_refusals import describe_send_refusal
    from deerflow.agent_instances.contract import Refusal

    with pytest.raises(Refusal):
        await describe_send_refusal(admission.live.authority, admission.link, Refusal("Synthetic host refusal", reason=reason))


@pytest.mark.parametrize("custody,administrator", [("personal", False), ("company", False), ("personal", True), ("company", True)])
async def test_unavailable_presentation_names_kind_and_limits_support_reference(admission, custody, administrator):
    from sqlalchemy import update

    from app.telegram_bots.publication import fixed_source
    from app.telegram_bots.render import render_plain
    from deerflow.persistence.agent_instances.model import AgentInstanceRow
    from deerflow.persistence.user.model import UserRow

    e = admission
    async with e.db.sessions.begin() as writer:
        await writer.execute(update(AgentInstanceRow).values(custody=custody, owner_id=e.link.owner_user_id if custody == "personal" else None))
        await writer.execute(update(UserRow).where(UserRow.id == e.link.owner_user_id).values(system_role="admin" if administrator else "user"))
    source = await fixed_source(e.channel._services, e.link, "unavailable.off")
    text = render_plain(source["key"], "en", **source["values"])
    kind = "AI assistants" if custody == "personal" else "AI employees"
    assert text.startswith(kind + " are not offered here.") and "Reference" not in text
    assert (" to offer " + kind + " here.") in text if administrator else text.endswith("Ask an administrator.")
    source = await fixed_source(e.channel._services, e.link, "unavailable.moved")
    text = render_plain(source["key"], "en", code="server-moved", **source["values"])
    assert ("Reference for support: server-moved" in text) is administrator
    assert "host-replaced" not in text
    paused = await fixed_source(e.channel._services, e.link, "unavailable.paused")
    text = render_plain(paused["key"], "en", **paused["values"])
    assert "Ask " not in text and "Reference" not in text


@pytest.mark.parametrize("reason,key", [("restarting", "refused.restarting"), ("execution-unavailable", "unavailable.paused")])
async def test_refusal_after_thread_preparation_still_closes_input_before_reservation(admission, monkeypatch, reason, key):
    from app.gateway import storage_spaces

    e = admission
    original = e.threads.ensure_thread

    async def refuse(*args, **kwargs):
        if reason == "restarting":
            raise ResidentRestarting("Fixture instance")
        raise ExecutionUnavailable("operator-hold")

    async def prepared(*args, **kwargs):
        thread = await original(*args, **kwargs)
        if reason == "restarting":
            e.http_app.state.agent_instances.residents.admit = refuse
        else:
            monkeypatch.setattr(storage_spaces, "require_execution", refuse)
        return thread

    monkeypatch.setattr(e.threads, "ensure_thread", prepared)
    assert await e.turns._prepare(e.turn) is False
    assert e.turn.input_closed and e.turn.record is None and e.bus.inbound_queue.empty()
    async with e.db.sessions() as reader:
        rows = list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == e.link.id)))
        assert any(row.parts[0]["source_ref"]["key"] == key for row in rows)


@pytest.mark.parametrize("ending", ["alive", "ready", "cancelled", "limiter", "final_authority"])
async def test_preparation_wait_notice_cannot_dispatch_after_wait_ends(admission, ending):
    from test_telegram_bot_adapter import message
    from test_telegram_publication_primary import _primary

    from app.telegram_bots.producer import OutboxSender

    e = admission
    services = e.channel._services
    services.turns = e.turns
    services.eligibility = e.live.authority.delivery
    await e.db.links.end_pending(e.link.id, skip_reason="superseded")

    async def refuse(view, *, work):
        raise ResidentRunLimit(view.name)

    e.http_app.state.agent_instances.residents.admit = refuse
    task = e.turn.task = asyncio.create_task(e.turns._prepare(e.turn))
    try:
        for _ in range(150):
            if e.turn.capacity_notice_sent:
                break
            await asyncio.sleep(0.01)
        assert e.turn.capacity_notice_sent
        async with e.db.sessions() as reader:
            identity = await reader.scalar(select(TelegramOutboxRow.id).where(TelegramOutboxRow.dedupe_key.startswith("notice:preparation-capacity:")))
        assert identity
        request = e.channel.bot.request
        message(request)

        async def finish_wait(delay=0):
            e.turn.capacity_waiting = False
            e.channel.bot.rate_limiter._short_hold_until = 0

        if ending == "limiter":
            e.channel.bot.rate_limiter._short_hold_until = e.channel.bot.rate_limiter._time() + 1
            e.channel.bot.rate_limiter._sleep = finish_wait
        elif ending == "cancelled":
            e.turn.stop_requested = True
        elif ending == "ready":
            await finish_wait()

        final_checks = []
        if ending == "final_authority":
            import inspect

            async def final_authority(*args):
                final = inspect.currentframe().f_back.f_code.co_name == "check_instance_part"
                verdict = await e.live.authority.delivery(*args)
                if final:
                    final_checks.append(True)
                    await finish_wait()
                return verdict

            services.eligibility = final_authority

        errors = []

        async def raw(*, link_id, **kwargs):
            try:
                return await e.channel.send_part(**kwargs)
            except Exception as error:
                errors.append(repr(error))
                raise

        sender = OutboxSender(services.outbox, run_id="preparation-fixture", deliver_part=raw, eligibility=e.live.authority.delivery, jitter=lambda: 0)
        try:
            await sender.tick()
            await asyncio.wait_for(sender.drain(), 3)
            saved = await _primary(e.db, identity)
            if ending == "final_authority":
                assert final_checks
            assert bool([call for call in request.calls if call.endpoint == "sendMessage"]) is (ending == "alive"), (errors, saved.state, saved.payload, saved.parts)
            assert (saved.state == "sent") is (ending == "alive")
        finally:
            await sender.stop()
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
