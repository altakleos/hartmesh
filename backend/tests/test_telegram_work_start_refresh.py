"""A changed assignment gets a new full preview before any new Start intent."""

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_agent_work_records import command
from test_telegram_bot_adapter import message
from test_telegram_callback_commands import press
from test_telegram_notification_runtime import runtime
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_telegram_work_start_controls import stage, start_control
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow


@pytest.mark.asyncio
@pytest.mark.parametrize("legacy", [False, "row", "stored"])
async def test_actual_stale_start_shows_current_assignment_and_needs_a_new_sdk_press(work_delivery, monkeypatch, legacy):
    e = work_delivery
    owner = await runtime(e)
    view_sources = owner.host.work_view_sources
    try:
        _plans, _starts, old = await start_control(e, monkeypatch)
        owner.host.work_view_sources = view_sources
        if legacy:
            from copy import deepcopy

            async with e.db.sessions.begin() as writer:
                control = await writer.get(TelegramCallbackRow, old.id)
                control.payload = {key: value for key, value in control.payload.items() if key != "assignment_revision"}
                if legacy == "stored":
                    parent = await writer.get(TelegramOutboxRow, old.payload["anchor_outbox_id"])
                    payload = deepcopy(parent.payload)
                    for keyboard in (payload["product_keyboard_basis"], payload["work_news_basis"]["keyboard"]):
                        for line in keyboard:
                            for button in line:
                                if button.get("kind") == "work_start":
                                    button["payload"].pop("assignment_revision", None)
                    parent.payload = payload
        current = await e.human.work.command(actor=e.actor, instance_id=e.link.agent_ref, work_id=e.work["id"], request=command(e.work, "edit", objective="A changed objective to review"))
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(press(e, 3510, old.id, message_id=3502))
        await e.channel._drain_ingress()
        async with e.db.sessions() as reader:
            assert await reader.get(TelegramActRow, old.id) is None
            views = [row for row in await reader.scalars(select(TelegramOutboxRow)) if row.payload.get("work_view_intent", {}).get("origin", {}).get("refusal") == "start.changed"]
            assert len(views) == 1 and views[0].parts
            assert (await reader.get(TelegramCallbackRow, old.id)).consumed_at is not None
            cleanup = [row for row in await reader.scalars(select(TelegramOutboxRow)) if row.state == "pending" and row.payload.get("kind") == "keyboard_cleanup"]
            assert len(cleanup) == 1
        assert not e.launches
        message(e.channel.bot.request, "editMessageReplyMarkup", identity=3502)
        message(e.channel.bot.request, identity=3511)
        for _ in range(4):
            e.clock.advance(2)
            await e.sender.tick()
            await e.sender.drain()
            async with e.db.sessions() as reader:
                if (await reader.get(TelegramOutboxRow, views[0].id)).state == "sent":
                    break
        sent = [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"][-1]
        assert sent.parameters["text"].startswith("This assignment changed.")
        assert current["objective"] in sent.parameters["text"] and current["success_criteria"] in sent.parameters["text"]
        async with e.db.sessions() as reader:
            fresh = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.payload["anchor_outbox_id"].as_string() == views[0].id, TelegramCallbackRow.kind == "work_start"))
            assert fresh is not None and fresh.message_id == 3511 and fresh.payload["assignment_revision"] == current["assignment_revision"]
        from app.telegram_bots.producer import DeliveryRetry

        assert not e.launches and all(isinstance(error, DeliveryRetry) for error in e.dispatch_errors), e.dispatch_errors
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(press(e, 3512, fresh.id, message_id=3511))
        await e.channel._drain_ingress()
        async with e.db.sessions() as reader:
            saved = await reader.get(TelegramActRow, fresh.id)
            assert saved.state == "done" and saved.body["expected_assignment_revision"] == current["assignment_revision"], (saved.state, saved.reason, saved.result)
        assert len(e.launches) == 1
    finally:
        await owner.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("evidence", ["event", "attempt"])
async def test_stale_start_rejects_actual_same_operation_evidence_bound_to_other_work(work_delivery, monkeypatch, evidence):
    from uuid import uuid4

    from deerflow.agent_instances.work_contract import DelegateWork
    from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow

    e = work_delivery
    owner = await runtime(e)
    try:
        _plans, _starts, old = await start_control(e, monkeypatch)
        other = await e.human.work.delegate(actor=e.actor, instance_id=e.link.agent_ref, request=DelegateWork(operation_id=uuid4().hex, objective="A separate assignment", success_criteria="Keep its own receipt"))
        if evidence == "event":
            request = command(other, "edit", objective="An actual different-target command").model_copy(update={"operation_id": old.id})
            await e.human.work.command(actor=e.actor, instance_id=e.link.agent_ref, work_id=other["id"], request=request)
        else:
            foreign = await e.acts.activate(e.link, operation_id=old.id, work_id=other["id"], assignment_revision=other["assignment_revision"])
            assert foreign.state == "done"
            async with e.db.sessions.begin() as writer:
                assert await writer.scalar(select(AgentWorkAttemptRow).where(AgentWorkAttemptRow.activation_id == old.id)) is not None
                # Lose the adapter journal and service event while retaining
                # the actual service-created native reservation elsewhere.
                await writer.delete(await writer.get(TelegramActRow, old.id))
                event = await writer.scalar(select(AgentWorkEventRow).where(AgentWorkEventRow.operation_id == old.id))
                await writer.delete(event)
        baseline = len(e.launches)
        await e.human.work.command(actor=e.actor, instance_id=e.link.agent_ref, work_id=e.work["id"], request=command(e.work, "edit", objective="Changed original assignment"))
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(press(e, 3580, old.id, message_id=3502))
        await e.channel._drain_ingress()
        async with e.db.sessions() as reader:
            assert not [row for row in await reader.scalars(select(TelegramOutboxRow)) if row.dedupe_key.startswith("notice:work-start-refresh:")]
        assert len(e.launches) == baseline
    finally:
        await owner.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("journal", ["missing", "malformed"])
async def test_stale_start_with_an_owned_reservation_cannot_create_a_fresh_preview(work_delivery, monkeypatch, journal):
    e = work_delivery
    owner = await runtime(e)
    view_sources = owner.host.work_view_sources
    try:
        _plans, starts, old = await start_control(e, monkeypatch)
        owner.host.work_view_sources = view_sources
        await stage(e, starts, old)
        async with e.db.sessions.begin() as writer:
            act = await writer.get(TelegramActRow, old.id)
            if journal == "missing":
                await writer.delete(act)
            else:
                act.state, act.result = "done", ["unqualified receipt"]
        await e.human.work.command(actor=e.actor, instance_id=e.link.agent_ref, work_id=e.work["id"], request=command(e.work, "edit", objective="New objective must not replace an owned launch"))
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(press(e, 3520, old.id, message_id=3502))
        await e.channel._drain_ingress()
        async with e.db.sessions() as reader:
            views = [row for row in await reader.scalars(select(TelegramOutboxRow)) if row.payload.get("work_view_intent", {}).get("origin", {}).get("refusal")]
            assert not views
        assert not e.launches
    finally:
        await owner.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["reservation", "ack"])
async def test_refresh_final_sdk_read_rechecks_original_start_proof(work_delivery, monkeypatch, damage):
    from copy import deepcopy

    e = work_delivery
    owner = await runtime(e)
    view_sources = owner.host.work_view_sources
    try:
        _plans, _starts, old = await start_control(e, monkeypatch)
        owner.host.work_view_sources = view_sources
        await e.human.work.command(actor=e.actor, instance_id=e.link.agent_ref, work_id=e.work["id"], request=command(e.work, "edit", objective="Current assignment"))
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(press(e, 3530, old.id, message_id=3502))
        await e.channel._drain_ingress()
        async with e.db.sessions() as reader:
            views = [row for row in await reader.scalars(select(TelegramOutboxRow)) if row.payload.get("work_view_intent", {}).get("origin", {}).get("refusal")]
            assert len(views) == 1 and views[0].parts
        message(e.channel.bot.request, "editMessageReplyMarkup", identity=3502)
        for _ in range(3):
            e.clock.advance(2)
            await e.sender.tick()
            await e.sender.drain()
            async with e.db.sessions() as reader:
                cleanup = await reader.get(TelegramOutboxRow, views[0].payload["work_view_intent"]["origin"]["cleanup_id"])
                if cleanup.state == "sent":
                    break
        assert cleanup.state == "sent"
        sources = owner.host.work_view_sources
        real, changed = sources.settings.get, []

        async def settings(*args, **kwargs):
            result = await real(*args, **kwargs)
            if not changed:
                async with e.db.sessions.begin() as writer:
                    if damage == "reservation":
                        control = await writer.get(TelegramCallbackRow, old.id)
                        control.payload = {**control.payload, "work_action_id": old.id}
                    else:
                        parent = await writer.get(TelegramOutboxRow, old.payload["anchor_outbox_id"])
                        parts = deepcopy(parent.parts)
                        parts[-1]["message_ids"] = []
                        parent.parts = parts
                changed.append(True)
            return result

        monkeypatch.setattr(sources.settings, "get", settings)
        before = len(e.channel.bot.request.calls)
        e.clock.advance(2)
        await e.sender.tick()
        await e.sender.drain()
        assert changed and not e.launches
        assert not [call for call in e.channel.bot.request.calls[before:] if call.endpoint == "sendMessage"]
        async with e.db.sessions() as reader:
            assert await reader.get(TelegramActRow, old.id) is None
    finally:
        await owner.stop()
