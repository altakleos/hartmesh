"""Real row sweeps preserve only the refreshed Start's own complete source chain."""

from datetime import timedelta

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_agent_work_records import command
from test_telegram_callback_commands import press
from test_telegram_notification_runtime import runtime
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_telegram_work_start import prepare_startable
from test_telegram_work_start_controls import start_control
from test_telegram_work_start_refresh_families import family_control
from test_telegram_work_start_refresh_origins import send_refresh
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.retention import ACT_RETENTION_DAYS, OUTBOX_RETENTION_DAYS, sweep_rows
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramOutboxRow


async def current_preview(e, old):
    current = await e.human.work.get(actor=e.actor, instance_id=e.link.agent_ref, work_id=old.target_id)
    await e.human.work.command(actor=e.actor, instance_id=e.link.agent_ref, work_id=old.target_id, request=command(current, "edit", objective="Current assignment retained across collection"))
    e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
    assert await e.channel.handle_update(press(e, 3680, old.id, message_id=old.message_id))
    await e.channel._drain_ingress()
    async with e.db.sessions() as reader:
        view = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_view_intent"]["origin"]["callback_id"].as_string() == old.id))
        assert view is not None and view.parts
    return view, await send_refresh(e, view, 3681)


@pytest.mark.asyncio
async def test_live_refreshed_start_retains_its_actual_acknowledged_cleanup(work_delivery, monkeypatch):
    e = work_delivery
    owner = await runtime(e)
    view_sources = owner.host.work_view_sources
    try:
        _plans, _starts, old = await start_control(e, monkeypatch)
        owner.host.work_view_sources = view_sources
        view, fresh = await current_preview(e, old)
        cleanup_id = view.payload["work_view_intent"]["origin"]["cleanup_id"]
        later = e.sender.outbox.clock() + timedelta(days=OUTBOX_RETENTION_DAYS + 1)
        await owner.host.callbacks.sweep(later)
        await sweep_rows(e.db.sessions, later, callback_max_age_days=owner.host.callbacks.config.callbacks.max_age_days)
        async with e.db.sessions() as reader:
            cleanup = await reader.get(TelegramOutboxRow, cleanup_id)
            assert cleanup is not None and cleanup.state == "sent"
        e.refresh_clock.advance((OUTBOX_RETENTION_DAYS + 1) * 86400)
        owner.host.callbacks.clock = e.refresh_clock.now
        owner.chat.views.reader.clock = e.refresh_clock.now
        e.channel._now = e.refresh_clock.now
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(press(e, 3682, fresh.id, message_id=fresh.message_id))
        await e.channel._drain_ingress()
        assert len(e.launches) == 1
    finally:
        await owner.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("family", ["assignment", "outcome", "execution", "management-back", "management-saved"])
async def test_live_refresh_keeps_aged_original_journals_and_inputs_through_real_sweeps(work_delivery, monkeypatch, family):
    from app.gateway import work_execution

    e = work_delivery
    original_activation = work_execution.activate_work
    owner = await runtime(e, preserve=True)
    view_sources = owner.host.work_view_sources
    owner.host.callbacks.config.callbacks.max_age_days = 1000
    e.sender.outbox.config.callbacks.max_age_days = 1000
    try:
        old, _proof = await family_control(e, monkeypatch, family)
        owner.host.work_view_sources = view_sources
        target = await e.human.work.get(actor=e.actor, instance_id=e.link.agent_ref, work_id=old.target_id)
        await prepare_startable(e, monkeypatch)
        e.work = target
        view, fresh = await current_preview(e, old)
        async with e.db.sessions() as reader:
            originals = set(await reader.scalars(select(TelegramActRow.id)))
        later = e.sender.outbox.clock() + timedelta(days=ACT_RETENTION_DAYS + 1)
        await owner.host.callbacks.sweep(later)
        await sweep_rows(e.db.sessions, later, callback_max_age_days=1000)
        async with e.db.sessions() as reader:
            assert originals <= set(await reader.scalars(select(TelegramActRow.id)))
            assert await reader.get(TelegramOutboxRow, view.payload["work_view_intent"]["origin"]["cleanup_id"]) is not None
        if family == "execution":
            monkeypatch.setattr(work_execution, "activate_work", original_activation)
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(press(e, 3683, fresh.id, message_id=fresh.message_id))
        await e.channel._drain_ingress()
        assert len(e.launches) == 1
    finally:
        await owner.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["foreign-message", "topic"])
async def test_actual_foreign_cleanup_receipt_cannot_unlock_a_refreshed_start(work_delivery, monkeypatch, damage):
    from copy import deepcopy

    from test_telegram_bot_adapter import message

    from app.telegram_bots.producer import keyboard_cleanup_row
    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow

    e = work_delivery
    owner = await runtime(e)
    view_sources = owner.host.work_view_sources
    try:
        _plans, _starts, old = await start_control(e, monkeypatch)
        owner.host.work_view_sources = view_sources
        view, fresh = await current_preview(e, old)
        cleanup_id = view.payload["work_view_intent"]["origin"]["cleanup_id"]
        if damage == "foreign-message":
            async with e.db.sessions.begin() as writer:
                other = await writer.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "work_details", TelegramCallbackRow.message_id == 3501))
                assert other is not None and other.message_id != old.message_id
                foreign = keyboard_cleanup_row(other, owner_user_id=e.link.owner_user_id, now=e.sender.outbox.clock())
                foreign.payload = {**foreign.payload, "work_request_parent_id": view.id}
                writer.add(foreign)
                foreign_id = foreign.id
            message(e.channel.bot.request, "editMessageReplyMarkup", identity=3501)
            e.refresh_clock.advance(2)
            assert await e.sender.tick() == 1
            await e.sender.drain()
            async with e.db.sessions.begin() as writer:
                foreign = await writer.get(TelegramOutboxRow, foreign_id)
                assert foreign.state == "sent" and foreign.parts[0]["message_ids"] == [3501]
                own = await writer.get(TelegramOutboxRow, cleanup_id)
                own.parts, own.payload = deepcopy(foreign.parts), deepcopy(foreign.payload)
        else:
            async with e.db.sessions.begin() as writer:
                (await writer.get(TelegramOutboxRow, cleanup_id)).message_thread_id = 719
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(press(e, 3684, fresh.id, message_id=fresh.message_id))
        await e.channel._drain_ingress()
        assert not e.launches
        async with e.db.sessions() as reader:
            assert await reader.get(TelegramActRow, fresh.id) is None
    finally:
        await owner.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("family", ["assignment", "outcome", "execution"])
async def test_missing_public_source_control_holds_its_aged_body_without_a_new_effect(work_delivery, monkeypatch, family):
    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow

    e = work_delivery
    owner = await runtime(e, preserve=True)
    view_sources = owner.host.work_view_sources
    owner.host.callbacks.config.callbacks.max_age_days = 1000
    e.sender.outbox.config.callbacks.max_age_days = 1000
    try:
        old, _proof = await family_control(e, monkeypatch, family)
        owner.host.work_view_sources = view_sources
        target = await e.human.work.get(actor=e.actor, instance_id=e.link.agent_ref, work_id=old.target_id)
        await prepare_startable(e, monkeypatch)
        e.work = target
        _view, fresh = await current_preview(e, old)
        async with e.db.sessions.begin() as writer:
            originals = set(await writer.scalars(select(TelegramActRow.id)))
            await writer.delete(await writer.get(TelegramCallbackRow, old.id))
        later = e.sender.outbox.clock() + timedelta(days=ACT_RETENTION_DAYS + 1)
        await owner.host.callbacks.sweep(later)
        await sweep_rows(e.db.sessions, later, callback_max_age_days=1000)
        async with e.db.sessions() as reader:
            assert originals <= set(await reader.scalars(select(TelegramActRow.id)))
            assert await reader.get(TelegramOutboxRow, old.payload["anchor_outbox_id"]) is not None
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(press(e, 3685, fresh.id, message_id=fresh.message_id))
        await e.channel._drain_ingress()
        assert not e.launches
        async with e.db.sessions() as reader:
            assert await reader.get(TelegramActRow, fresh.id) is None
    finally:
        await owner.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("family,damage", [("view", "descriptor"), ("view", "generation"), ("outcome", "generation"), ("execution", "generation"), ("management-saved", "generation")])
async def test_owned_runless_admission_retains_its_historical_refresh_ancestry(work_delivery, monkeypatch, family, damage):
    from unittest.mock import AsyncMock

    from test_telegram_work_interaction_delivery import guard_for

    from app.gateway import work_execution
    from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow
    from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow

    e = work_delivery
    original_activation = work_execution.activate_work
    owner = await runtime(e, preserve=True)
    view_sources = owner.host.work_view_sources
    owner.host.callbacks.config.callbacks.max_age_days = 1000
    e.sender.outbox.config.callbacks.max_age_days = 1000
    try:
        if family == "view":
            _plans, _starts, old = await start_control(e, monkeypatch)
        else:
            old, _proof = await family_control(e, monkeypatch, family)
            target = await e.human.work.get(actor=e.actor, instance_id=e.link.agent_ref, work_id=old.target_id)
            await prepare_startable(e, monkeypatch)
            e.work = target
        owner.host.work_view_sources = view_sources
        view, fresh = await current_preview(e, old)
        if family == "execution":
            monkeypatch.setattr(work_execution, "activate_work", original_activation)
        async with e.db.sessions() as reader:
            originals = set(await reader.scalars(select(TelegramActRow.id)))
        staged = await owner.chat.starts.stage(e.link, fresh.id, chat_id=e.link.chat_id, message_id=fresh.message_id, message_thread_id=None, guard=await guard_for(e))
        monkeypatch.setattr("app.gateway.services.start_run", AsyncMock(side_effect=RuntimeError("Unknown offline admission")))
        await owner.host.acts.check(staged.id)
        async with e.db.sessions.begin() as writer:
            attempt = await writer.scalar(select(AgentWorkAttemptRow).where(AgentWorkAttemptRow.activation_id == staged.id))
            assert attempt is not None and attempt.run_id is None
            if damage == "descriptor":
                row = await writer.get(TelegramOutboxRow, view.id)
                row.payload = {key: value for key, value in row.payload.items() if key != "work_view_intent"}
            else:
                (await writer.get(TelegramBotLinkRow, e.link.id)).binding_generation += 1
        later = e.sender.outbox.clock() + timedelta(days=1001 if damage == "descriptor" else ACT_RETENTION_DAYS + 1)
        await owner.host.callbacks.sweep(later)
        await sweep_rows(e.db.sessions, later, callback_max_age_days=1000)
        async with e.db.sessions() as reader:
            saved = await reader.get(TelegramActRow, staged.id)
            assert saved is not None and saved.body == dict(staged.body)
            assert originals <= set(await reader.scalars(select(TelegramActRow.id)))
            assert await reader.get(TelegramOutboxRow, old.payload["anchor_outbox_id"]) is not None
        assert not e.launches
    finally:
        await owner.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["alternate-parent", "foreign-group", "malformed-assignment"])
async def test_pin_only_discovery_uses_the_real_gesture_and_contains_bad_parent_data(work_delivery, monkeypatch, damage):
    from copy import deepcopy

    from test_telegram_work_interaction_delivery import guard_for
    from test_telegram_work_preview_recovery import dispatch

    from app.telegram_bots.work_start_refresh_retention import refresh_dependencies
    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow

    e = work_delivery
    owner = await runtime(e, preserve=True)
    view_sources = owner.host.work_view_sources
    try:
        if damage in {"alternate-parent", "foreign-group"}:
            plans, _starts, old = await start_control(e, monkeypatch)
            async with e.db.sessions() as reader:
                detail = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "work_details", TelegramCallbackRow.target_id == old.target_id, TelegramCallbackRow.message_id == 3501))
            alternate_id = await plans.open(e.link, detail.id, query_id="another-actual-detail", chat_id=e.link.chat_id, message_id=3501, message_thread_id=None, guard=await guard_for(e))
            await dispatch(e, 3690)
            async with e.db.sessions() as reader:
                alternate = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.payload["anchor_outbox_id"].as_string() == alternate_id, TelegramCallbackRow.kind == "work_start"))
                assert alternate is not None and alternate.message_id == 3690
        else:
            old, _proof = await family_control(e, monkeypatch, "assignment")
        owner.host.work_view_sources = view_sources
        view, _fresh = await current_preview(e, old)
        async with e.db.sessions.begin() as writer:
            if damage == "alternate-parent":
                row = await writer.get(TelegramOutboxRow, view.id)
                payload = deepcopy(row.payload)
                payload["work_view_intent"]["origin"].update(callback_id=alternate.id, anchor_outbox_id=alternate_id)
                row.payload = payload
            elif damage == "foreign-group":
                row = await writer.get(TelegramOutboxRow, view.id)
                parts = deepcopy(row.parts)
                parts[-1]["callback_group_id"] = alternate.group_id
                row.parts = parts
            else:
                parent = await writer.get(TelegramOutboxRow, old.payload["anchor_outbox_id"])
                payload = deepcopy(parent.payload)
                payload["work_news_basis"]["source"]["work"]["phase"] = {"broken": True}
                parent.payload = payload
        async with e.db.sessions() as reader:
            held = await refresh_dependencies(reader, e.sender.outbox.clock(), max_age_days=180)
            assert view.id in held.deliveries
            if damage in {"alternate-parent", "foreign-group"}:
                assert old.payload["anchor_outbox_id"] in held.deliveries
                assert alternate_id not in held.deliveries and alternate.group_id not in held.groups
        later = e.sender.outbox.clock() + timedelta(days=OUTBOX_RETENTION_DAYS + 1)
        await owner.host.callbacks.sweep(later)
        await sweep_rows(e.db.sessions, later, callback_max_age_days=180)
        async with e.db.sessions() as reader:
            assert await reader.get(TelegramOutboxRow, view.id) is not None
    finally:
        await owner.stop()
