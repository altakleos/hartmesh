"""Derived assignments remain proposals; chat gestures never activate Work."""

from uuid import uuid4

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_bot_adapter import message
from test_telegram_work_news_delivery import prepare
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.producer import _item
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow


async def proposed(e):
    from test_telegram_work_event_intents import activate, setup

    from deerflow.agent_instances.contract import DefinitionSnapshot
    from deerflow.agent_instances.work_execution_contract import ReportWork
    from deerflow.persistence.agent_instances.model import AgentDefinitionRevisionRow, AgentInstanceRow

    e.channel._now = lambda: e.sender.outbox.clock()
    _peer, worker = await setup(e)
    async with e.db.sessions.begin() as writer:
        instance = await writer.get(AgentInstanceRow, e.instance.id)
        old = await writer.get(AgentDefinitionRevisionRow, instance.definition_revision)
        definition = DefinitionSnapshot.capture(owner_id=old.owner_id, config={**old.config, "work_policy": {**old.config.get("work_policy", {}), "enabled": True, "allow_derived": True, "max_derived_per_activation": 1}}, soul=old.soul)
        writer.add(AgentDefinitionRevisionRow(revision=definition.revision, owner_id=definition.owner_id, config=definition.config, soul=definition.soul))
        await writer.flush()
        instance.definition_revision = definition.revision
    parent, execution, scope, run = await activate(e)
    e.proposal_execution = execution
    e.proposal_scope, e.proposal_run = scope, run
    current = (await scope.context(execution))["work"]
    await scope.report(execution, ReportWork(operation_id=uuid4().hex, expected_revision=current["revision"], action="derive", statement="Check <all> references", success_criteria="List every broken reference & its source"))
    await worker.tick()
    async with e.db.sessions() as reader:
        row = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_event_intent"]["key"].as_string() == "work.proposed"))
        assert row is not None and row.work_id != parent["id"]
        return _item(row)


@pytest.mark.asyncio
async def test_proposal_delivers_complete_details_web_link_and_dismiss(work_delivery):
    e = work_delivery
    e.live.config.model_extra["telegram_bots"] = {"public_base_url": "https://app.example.test"}
    item = await proposed(e)
    await prepare(e, item=item)
    message(e.channel.bot.request, identity=2601)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not e.dispatch_errors, e.dispatch_errors
    calls = [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
    assert len(calls) == 1
    body = calls[0].parameters
    assert "Do: Check &lt;all&gt; references" in body["text"]
    assert "Done when: List every broken reference &amp; its source" in body["text"]
    assert "Not started." in body["text"]
    buttons = [button for row in body["reply_markup"]["inline_keyboard"] for button in row]
    assert [button["text"] for button in buttons] == ["Open assignment", "Not now"]
    assert buttons[0]["url"].startswith("https://app.example.test/")
    async with e.db.sessions() as reader:
        controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.payload["anchor_outbox_id"].as_string() == item.id)))
        assert [control.kind for control in controls] == ["dismiss"]
    current = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=item.work_id)
    assert current["attempt"] is None and current["revision"] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("protected", [False, True])
async def test_not_now_clears_only_the_acknowledged_keyboard_without_changing_work(work_delivery, protected):
    from datetime import UTC, datetime

    from _telegram_bot_test_support import ManualClock
    from test_telegram_work_callback_dispatch import install_dispatch, press_work
    from test_telegram_work_control_admission import admissions

    from app.telegram_bots.work_outcome_delivery import WorkOutcomePlans
    from deerflow.persistence.telegram_bots.model import TelegramInstancePolicyRow

    e = work_delivery
    clock = ManualClock(e.sender.outbox.clock())
    e.sender.outbox.clock = clock.now
    if protected:
        async with e.db.sessions.begin() as writer:
            writer.add(TelegramInstancePolicyRow(instance_id=e.link.agent_ref, policy="protected", updated_by=e.link.owner_user_id, updated_at=datetime.now(UTC)))
    item = await proposed(e)
    await prepare(e, item=item)
    install_dispatch(e, WorkOutcomePlans(e.plans, admissions(e).acts))
    message(e.channel.bot.request, identity=2611)
    clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()

    async def control():
        async with e.db.sessions() as reader:
            row = await reader.get(TelegramOutboxRow, item.id)
            return await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"]))

    if protected:
        show = await control()
        assert show.kind == "reveal"
        assert [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"][-1].parameters["text"] == "A new assignment is proposed."
        await press_work(e, show, message_id=2611)
        message(e.channel.bot.request, "editMessageText", identity=2611)
        clock.advance(2)
        assert await e.sender.tick() == 1
        await e.sender.drain()
    dismiss = await control()
    assert dismiss.kind == "dismiss"
    if protected:
        # Dismissal is presentation-only, including after a web withdrawal.
        await change_proposal(e, item, "cancel")
    before = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=item.work_id)
    await press_work(e, dismiss, message_id=2611)
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramCallbackRow, dismiss.id)).closed_reason == "dismissed"
    message(e.channel.bot.request, "editMessageReplyMarkup", identity=2611)
    clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not e.dispatch_errors, e.dispatch_errors
    cleaned = [call for call in e.channel.bot.request.calls if call.endpoint == "editMessageReplyMarkup"]
    assert len(cleaned) == 1 and cleaned[0].parameters["message_id"] == 2611
    assert not cleaned[0].parameters.get("reply_markup")
    assert await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=item.work_id) == before
    await press_work(e, dismiss, message_id=2611, update_id=44)
    clock.advance(2)
    assert await e.sender.tick() == 0


async def change_proposal(e, item, change):
    from dataclasses import replace

    from sqlalchemy.orm.attributes import flag_modified
    from test_agent_work_records import command
    from test_work_execution import activation

    from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow, AgentWorkRow

    if change in {"start", "cancel"}:
        current = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=item.work_id)
        if change == "cancel":
            await e.human.work.command(actor=e.actor, instance_id=e.instance.id, work_id=item.work_id, request=command(current, "cancel"))
        else:
            await e.proposal_scope.finish(e.proposal_execution, e.proposal_run, settled=True)
            execution = replace(e.proposal_execution, work=None)
            await e.proposal_scope.service.reserve(execution=execution, work_id=item.work_id, request=activation(execution, current))
        return
    async with e.db.sessions.begin() as writer:
        event = await writer.get(AgentWorkEventRow, item.event_id)
        if change == "actor":
            event.actor_id = "foreign-principal"
        elif change == "attempt":
            (await writer.get(AgentWorkAttemptRow, event.request["attempt_id"])).requester_id = "foreign-requester"
        elif change == "source":
            event.request = {**event.request, "source_work_id": item.work_id}
        elif change == "extra":
            event.request = {**event.request, "unused": "unverified"}
        elif change == "bool":
            event.result = {**event.result, "revision": True}
            flag_modified(event, "result")
        elif change == "criteria":
            (await writer.get(AgentWorkRow, item.work_id)).success_criteria = "A revised completion criterion"


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["actor", "attempt", "source", "extra", "bool", "start", "cancel"])
async def test_proposal_refuses_unqualified_provenance_or_a_started_child(work_delivery, change):
    from app.telegram_bots.producer import ProductSourceRetired

    e = work_delivery
    item = await proposed(e)
    await change_proposal(e, item, change)
    with pytest.raises(ProductSourceRetired):
        await prepare(e, item=item)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["actor", "criteria", "start"])
async def test_proposal_final_sdk_snapshot_rechecks_source_and_complete_preview(work_delivery, monkeypatch, change):
    from _telegram_bot_test_support import ManualClock

    e = work_delivery
    clock = ManualClock(e.sender.outbox.clock())
    e.sender.outbox.clock = clock.now
    item = await proposed(e)
    await prepare(e, item=item)
    sources = e.channel._services.work_news_sources
    original, changed = sources.settings.get, []

    async def drift(*args, **kwargs):
        settings = await original(*args, **kwargs)
        if not changed:
            await change_proposal(e, item, change)
            changed.append(True)
        return settings

    monkeypatch.setattr(sources.settings, "get", drift)
    clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert changed and not [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
    if change == "criteria":
        # The final fence defers; the next claimed attempt refreshes the saved
        # presentation before a subsequent attempt may dispatch it.
        message(e.channel.bot.request, identity=2621)
        for _ in range(2):
            clock.advance(2)
            assert await e.sender.tick() == 1
            await e.sender.drain()
        async with e.db.sessions() as reader:
            row = await reader.get(TelegramOutboxRow, item.id)
            assert row.state == "sent" and row.payload["work_news_basis"]["source"]["values"]["success_criteria"] == "A revised completion criterion"
        calls = [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
        assert len(calls) == 1 and "A revised completion criterion" in calls[0].parameters["text"]


@pytest.mark.asyncio
@pytest.mark.parametrize("corruption", ["ack", "group", "pending"])
async def test_proposal_dismiss_rejects_unacknowledged_or_substituted_controls(work_delivery, corruption):
    from copy import deepcopy

    from app.telegram_bots.callbacks import CallbackRefused

    e = work_delivery
    item = await proposed(e)
    await prepare(e, item=item)
    message(e.channel.bot.request, identity=2631)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, item.id)
        callback = await writer.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"]))
        identity = callback.id
        if corruption == "ack":
            row.parts = [{**part, "message_ids": [2632]} for part in row.parts]
        elif corruption == "pending":
            callback.payload = {**callback.payload, "presentation_pending": True}
        else:
            clone = {column.name: deepcopy(getattr(callback, column.name)) for column in TelegramCallbackRow.__table__.columns}
            identity = uuid4().hex
            writer.add(TelegramCallbackRow(**{**clone, "id": identity, "group_id": uuid4().hex}))
    owner = await e.live.authority.prepare_live_owner(e.link.owner_user_id)
    guard = e.db.links.publication_guard(e.link, check_live_owner=owner, check_attempt_current=lambda: True)
    with pytest.raises(CallbackRefused):
        await e.channel._services.work_news_sources.recovery.dismiss(e.link, identity, chat_id=e.link.chat_id, message_id=2631, message_thread_id=None, guard=guard)
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramCallbackRow, identity)).consumed_at is None
        assert not await reader.scalar(select(TelegramOutboxRow.id).where(TelegramOutboxRow.payload["kind"].as_string() == "keyboard_cleanup"))
