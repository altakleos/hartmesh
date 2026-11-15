"""An old Start's canonical carrier remains proof throughout its fresh preview."""

import pytest
from _telegram_bot_test_support import ManualClock
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_agent_work_records import command
from test_telegram_bot_adapter import message
from test_telegram_callback_commands import press
from test_telegram_notification_runtime import runtime
from test_telegram_work_carriers import stopped_carrier
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_telegram_work_start import prepare_startable
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.agent_instances.work import AgentWorkEventRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", [False, True])
async def test_legacy_news_refresh_keeps_its_actual_canonical_event(work_delivery, monkeypatch, missing):
    e = work_delivery
    owner = await runtime(e)
    try:
        old = await stopped_carrier(e, "stop")
        current = await e.human.work.get(actor=e.actor, instance_id=e.link.agent_ref, work_id=old.target_id)
        await e.human.work.command(actor=e.actor, instance_id=e.link.agent_ref, work_id=old.target_id, request=command(current, "edit", objective="Current version of stopped work"))
        async with e.db.sessions.begin() as writer:
            callback = await writer.get(TelegramCallbackRow, old.id)
            callback.payload = {key: value for key, value in callback.payload.items() if key != "assignment_revision"}
            parent = await writer.get(TelegramOutboxRow, old.payload["anchor_outbox_id"])
            if missing:
                event = await writer.get(AgentWorkEventRow, parent.event_id)
                assert event is not None
                await writer.delete(event)
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(press(e, 3540, old.id, message_id=old.message_id))
        await e.channel._drain_ingress()
        async with e.db.sessions() as reader:
            views = [row for row in await reader.scalars(select(TelegramOutboxRow)) if row.payload.get("work_view_intent", {}).get("origin", {}).get("refusal")]
            ready = [view for view in views if view.parts]
            assert bool(ready) is not missing
            assert await reader.get(TelegramActRow, old.id) is None
            assert not list(await reader.scalars(select(TelegramActRow).where(TelegramActRow.kind == "activate")))
    finally:
        await owner.stop()


@pytest.mark.asyncio
async def test_repeated_actual_refreshes_keep_group_proofs_compact_through_native_entry(work_delivery, monkeypatch):
    from test_telegram_work_start_controls import start_control

    from app.telegram_bots.work_view_sources import WorkViewProjection

    e = work_delivery
    owner = await runtime(e)
    view_sources = owner.host.work_view_sources
    observed_rows, observed_widths = [], []
    try:
        _plans, _starts, old = await start_control(e, monkeypatch)
        owner.host.work_view_sources = view_sources
        checked, extended = WorkViewProjection.check_with_permissions, WorkViewProjection.extend

        def check(self, rows, held):
            observed_rows.append(len(rows))
            return checked(self, rows, held)

        def extend(self, query):
            result = extended(self, query)
            observed_widths.append(len(result.selected_columns))
            return result

        monkeypatch.setattr(WorkViewProjection, "check_with_permissions", check)
        monkeypatch.setattr(WorkViewProjection, "extend", extend)
        for generation in range(6):
            current = await e.human.work.get(actor=e.actor, instance_id=e.link.agent_ref, work_id=old.target_id)
            current = await e.human.work.command(actor=e.actor, instance_id=e.link.agent_ref, work_id=old.target_id, request=command(current, "edit", objective=f"Current objective {generation}"))
            e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
            assert await e.channel.handle_update(press(e, 3610 + generation * 10, old.id, message_id=old.message_id))
            await e.channel._drain_ingress()
            async with e.db.sessions() as reader:
                view = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_view_intent"]["origin"]["callback_id"].as_string() == old.id))
                assert view is not None and view.parts
            old = await send_refresh(e, view, 3611 + generation * 10)
            assert old is not None and old.payload["assignment_revision"] == current["assignment_revision"] and not e.launches
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(press(e, 3670, old.id, message_id=old.message_id))
        await e.channel._drain_ingress()
        assert len(e.launches) == 1
        assert observed_rows and max(observed_rows) <= 8
        assert observed_widths and max(observed_widths) < 1000
    finally:
        await owner.stop()


async def send_refresh(e, view, message_id):
    e.refresh_clock = ManualClock(e.sender.outbox.clock())
    e.sender.outbox.clock = e.refresh_clock.now
    async with e.db.sessions() as reader:
        original = await reader.get(TelegramCallbackRow, view.payload["work_view_intent"]["origin"]["callback_id"])
    message(e.channel.bot.request, "editMessageReplyMarkup", identity=original.message_id)
    for index in range(len(view.parts)):
        message(e.channel.bot.request, identity=message_id + index)
    for _ in range(len(view.parts) + 4):
        e.refresh_clock.advance(2)
        await e.sender.tick()
        await e.sender.drain()
        async with e.db.sessions() as reader:
            parent = await reader.get(TelegramOutboxRow, view.id)
            if parent.state == "sent":
                return await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == parent.parts[-1]["callback_group_id"], TelegramCallbackRow.kind == "work_start"))
    async with e.db.sessions() as reader:
        states = [(row.dedupe_key, row.state, row.failed_reason, row.skip_reason, row.not_before) for row in await reader.scalars(select(TelegramOutboxRow)) if row.state != "sent"]
    pytest.fail(f"The source-owned refreshed preview was not acknowledged: {states!r}; {e.dispatch_errors!r}")


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", [False, True])
async def test_new_start_rechecks_news_origin_after_refreshed_preview_ack(work_delivery, monkeypatch, missing):
    e = work_delivery
    owner = await runtime(e)
    try:
        old = await stopped_carrier(e, "stop")
        target = await e.human.work.get(actor=e.actor, instance_id=e.link.agent_ref, work_id=old.target_id)
        await prepare_startable(e, monkeypatch)
        e.work = target
        await e.human.work.command(actor=e.actor, instance_id=e.link.agent_ref, work_id=old.target_id, request=command(target, "edit", objective="Review the current stopped assignment"))
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(press(e, 3550, old.id, message_id=old.message_id))
        await e.channel._drain_ingress()
        async with e.db.sessions() as reader:
            view = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key.startswith("notice:work-start-refresh:")))
            assert view is not None and view.parts
        fresh = await send_refresh(e, view, 3551)
        assert fresh is not None and not e.launches
        if missing:
            async with e.db.sessions.begin() as writer:
                parent = await writer.get(TelegramOutboxRow, old.payload["anchor_outbox_id"])
                await writer.delete(await writer.get(AgentWorkEventRow, parent.event_id))
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(press(e, 3552, fresh.id, message_id=fresh.message_id))
        await e.channel._drain_ingress()
        assert len(e.launches) == (0 if missing else 1)
        async with e.db.sessions() as reader:
            actual = await reader.get(TelegramActRow, fresh.id)
            assert actual is None if missing else actual.state == "done"
    finally:
        await owner.stop()


@pytest.mark.asyncio
async def test_stale_capacity_offer_refresh_preserves_its_waiting_owner_transfer(work_delivery, monkeypatch):
    from test_telegram_capacity_offers import waiting
    from test_telegram_work_interaction_delivery import selected
    from test_telegram_work_preview_recovery import dispatch

    from app.gateway import work_execution

    e = work_delivery
    owner = await runtime(e)
    view_sources = owner.host.work_view_sources
    try:
        offers, waiting_act, real = await waiting(e, monkeypatch)
        owner.host.work_view_sources = view_sources
        e.identity = await offers.poll()
        await dispatch(e, 3560)
        old = await selected(e, "work_start")
        current = await e.human.work.command(actor=e.actor, instance_id=e.link.agent_ref, work_id=e.work["id"], request=command(e.work, "edit", objective="Review changed capacity work"))
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(press(e, 3561, old.id, message_id=3560))
        await e.channel._drain_ingress()
        async with e.db.sessions() as reader:
            view = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key.startswith("notice:work-start-refresh:")))
            assert view is not None and view.parts
            assert view.payload["work_view_intent"]["origin"]["waiting_act_id"] == waiting_act.id
        fresh = await send_refresh(e, view, 3562)
        assert fresh.payload["assignment_revision"] == current["assignment_revision"] and not e.launches
        e.human.agents.residents = None
        monkeypatch.setattr(work_execution, "activate_work", real)
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(press(e, 3563, fresh.id, message_id=fresh.message_id))
        await e.channel._drain_ingress()
        async with e.db.sessions() as reader:
            original = await reader.get(TelegramActRow, waiting_act.id)
            new = await reader.get(TelegramActRow, fresh.id)
            assert original.reason == "superseded" and original.result == {"capacity_transfer": {"outbox_id": e.identity, "operation_id": fresh.id}}
            assert new is not None and new.state == "done", None if new is None else (new.state, new.reason)
        assert len(e.launches) == 1
    finally:
        await owner.stop()
