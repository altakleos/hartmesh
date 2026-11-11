"""Routing refresh and Show preserve complete source and operation ownership."""

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_bot_adapter import message
from test_telegram_outbox_workers import Clock
from test_telegram_work_interaction_delivery import guard_for
from test_telegram_work_routing_delivery import routing
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow


async def setup(e, monkeypatch, *, protected=False):
    from app.telegram_bots.work_routing_recovery import WorkRoutingRecovery

    plans, sources = await routing(e, monkeypatch, protected=protected)
    e.recovery = WorkRoutingRecovery(plans)
    sources.recovery = e.recovery
    e.clock = Clock()
    e.clock.now = datetime.now(UTC)
    e.sender.outbox.clock = e.clock
    return plans


async def send(e, *, identity=971, endpoint="sendMessage"):
    e.clock.advance(10)
    message(e.channel.bot.request, endpoint=endpoint, identity=identity)
    assert await e.sender.tick() == 1
    await e.sender.drain()


@pytest.mark.asyncio
async def test_routing_show_replaces_only_its_own_protected_preview(work_delivery, monkeypatch):
    e = work_delivery
    await setup(e, monkeypatch, protected=True)
    await send(e)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, e.identity)
        show = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"]))
        assert show.kind == "reveal"
    await e.recovery.reveal(e.link, show.id, chat_id=e.link.chat_id, message_id=971, message_thread_id=1, guard=await guard_for(e))
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, e.identity)
        assert row.payload["work_request_basis"]["version"] == 6
        assert row.payload["work_request_basis"]["reveal"] == show.id
        assert row.parts[-1]["target_message_id"] == 971
        control = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"]))
        assert control.kind == "route_to" and control.payload["presentation_pending"] is True
    await send(e, endpoint="editMessageText")
    assert not e.dispatch_errors, e.dispatch_errors
    call = next(call for call in e.channel.bot.request.calls if call.endpoint == "editMessageText")
    assert "another@example.com" in str(call.parameters["reply_markup"])
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, e.identity)
        assert row.state == "sent"
        assert (await reader.get(TelegramCallbackRow, control.id)).payload["presentation_pending"] is False


@pytest.mark.asyncio
async def test_pending_routing_refreshes_removed_recipient_before_any_send(work_delivery, monkeypatch):
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow

    e = work_delivery
    await setup(e, monkeypatch)
    async with e.db.sessions.begin() as writer:
        (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.recipient_id))).permissions = 1
    e.clock.advance(10)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not any(call.endpoint == "sendMessage" for call in e.channel.bot.request.calls)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, e.identity)
        assert row.state == "pending" and row.payload["work_request_basis"]["version"] == 6
        assert row.payload["work_request_basis"]["routing"]["recipients"] == []
    await send(e)
    call = next(call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage")
    assert "Nobody else can answer" in call.parameters["text"]


@pytest.mark.asyncio
async def test_wrong_person_followup_ends_after_routing_revision_changes(work_delivery, monkeypatch):
    from deerflow.agent_instances.contract import AgentConflict
    from deerflow.agent_instances.human_input_contract import RequestCommand

    e = work_delivery
    plans, _ = await routing(e, monkeypatch)
    current = await e.human.get(actor=e.actor, request_id=e.request["id"])
    await e.human.command(actor=e.actor, request_id=e.request["id"], request=RequestCommand(operation_id=uuid4().hex, action="route", expected_revision=current["revision"], expected_request_revision=1, recipient_id=e.recipient_id))
    value = await plans.reader.read_routing(e.link, e.request["id"])
    assert not value.after_wrong and not value.source.request["needs_routing"]
    assert e.actor.subject_id not in {person["user_id"] for person in value.recipients}
    with pytest.raises(AgentConflict):
        await plans.plan(e.link, request_id=e.request["id"], dedupe_key="after-completed-route")


@pytest.mark.asyncio
async def test_missing_routing_anchor_repairs_without_repeating_its_edit(work_delivery, monkeypatch):
    from test_telegram_work_control_admission import admissions

    from app.telegram_bots.work_routing_admission import WorkRoutingAdmissions

    e = work_delivery
    plans = await setup(e, monkeypatch)
    await send(e)
    await e.recovery.refresh_anchor(e.link, e.identity, expected_version=0, guard=await guard_for(e))
    e.channel.bot.request.enqueue("editMessageText", body={"ok": False, "error_code": 400, "description": "Bad Request: message to edit not found"}, status=400)
    e.clock.advance(10)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, e.identity)
        assert row.state == "failed" and row.failed_reason == "work-routing-refresh-required"
        original_ack = row.parts[0]
    assert await e.recovery.repair(e.link, e.identity, guard=await guard_for(e)) == e.identity
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, e.identity)
        assert row.parts[0] == original_ack
        assert row.parts[-1]["kind"] == "text"
        control = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"]))
    await send(e, identity=972)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, e.identity)
        assert row.state == "sent", (row.failed_reason, e.dispatch_errors)
        anchor = row.parts[-1]["message_ids"][0]
    controller = WorkRoutingAdmissions(plans, admissions(e).acts)
    staged = await controller.stage(e.link, control.id, chat_id=e.link.chat_id, message_id=anchor, message_thread_id=None, guard=await guard_for(e))
    assert staged.body["recipient_id"] == e.recipient_id
    assert (await controller.acts.check(staged.id)).state == "done"


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["revision", "language", "protection"])
async def test_routing_refresh_qualifies_current_display(work_delivery, monkeypatch, change):
    from deerflow.persistence.agent_instances.work import AgentWorkRow
    from deerflow.persistence.telegram_bots.model import TelegramInstancePolicyRow, TelegramPersonSettingsRow

    e = work_delivery
    await setup(e, monkeypatch)
    async with e.db.sessions.begin() as writer:
        if change == "revision":
            (await writer.get(AgentWorkRow, e.work["id"])).revision += 1
        elif change == "language":
            (await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)).locale = "zh-CN"
        else:
            writer.add(TelegramInstancePolicyRow(instance_id=e.link.agent_ref, policy="protected", updated_by=e.link.owner_user_id, updated_at=datetime.now(UTC)))
    e.clock.advance(10)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not any(call.endpoint == "sendMessage" for call in e.channel.bot.request.calls)
    await send(e)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, e.identity)
        assert row.state == "sent", (row.failed_reason, e.dispatch_errors)
        basis = row.payload["work_request_basis"]
        assert basis["version"] == 6
        if change == "language":
            assert basis["locale"] == "zh-CN"
        elif change == "protection":
            assert basis["concealed"]
            assert basis["parts"][0]["html"] == "A question is waiting."
        else:
            assert basis["source"]["seen"]["work_revision"] > 1


@pytest.mark.asyncio
async def test_routing_reveal_rolls_back_when_final_guard_changes_roster(work_delivery, monkeypatch):
    from app.telegram_bots.producer import ProductSourceRetired
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow

    e = work_delivery
    await setup(e, monkeypatch, protected=True)
    await send(e)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, e.identity)
        show = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"]))
    base = await guard_for(e)
    calls = []

    class Guard:
        current = base.current

        async def check(self, writer):
            result = await base.check(writer)
            calls.append(True)
            if len(calls) == 3:
                (await writer.get(AgentInstanceGrantRow, (e.link.agent_ref, e.recipient_id))).permissions = 1
                await writer.flush()
            return result

    with pytest.raises(ProductSourceRetired):
        await e.recovery.reveal(e.link, show.id, chat_id=e.link.chat_id, message_id=971, message_thread_id=None, guard=Guard())
    assert len(calls) == 3
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramCallbackRow, show.id)).consumed_at is None
        assert (await reader.get(TelegramOutboxRow, e.identity)).payload["presentation_version"] == 0


@pytest.mark.asyncio
async def test_routing_multipart_show_waits_for_teaser_cleanup_and_full_ack(work_delivery, monkeypatch):
    from deerflow.persistence.agent_instances.human_input import HumanInputRequestRow, HumanInputResponseRow

    e = work_delivery
    plans = await setup(e, monkeypatch, protected=True)
    await send(e)
    async with e.db.sessions.begin() as writer:
        request = await writer.get(HumanInputRequestRow, e.request["id"])
        for response in await writer.scalars(select(HumanInputResponseRow)):
            await writer.delete(response)
        request.recipient_id = None
        request.question = "Complete routing question. " * 230
        request.revision += 1
        row = await writer.get(TelegramOutboxRow, e.identity)
        show = await writer.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"]))
    await e.recovery.reveal(e.link, show.id, chat_id=e.link.chat_id, message_id=971, message_thread_id=None, guard=await guard_for(e))
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, e.identity)
        parts = [part for part in row.parts if part["presentation_version"] == 1]
        assert len(parts) > 1 and all(part["kind"] == "text" for part in parts)
        cleanup_id = row.payload["work_request_basis"]["cleanup_id"]
        assert cleanup_id
    await send(e, endpoint="editMessageReplyMarkup")
    assert len([call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]) == 1
    for index in range(len(parts)):
        await send(e, identity=980 + index)
        async with e.db.sessions() as reader:
            controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == parts[-1]["callback_group_id"])))
            assert all(control.payload["presentation_pending"] is (index < len(parts) - 1) for control in controls)
    calls = [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
    assert "".join(call.parameters["text"] for call in calls).count("Complete routing question.") == 230
    assert all(call.parameters["protect_content"] for call in calls)
    assert await plans.plan(e.link, request_id=e.request["id"], dedupe_key=(await e.sender.outbox.get(e.link, e.identity)).dedupe_key) == e.identity


@pytest.mark.asyncio
async def test_routing_refresh_keeps_unresolved_original_operation(work_delivery, monkeypatch):
    from test_telegram_work_control_admission import admissions

    from app.telegram_bots.producer import DeliveryRetry
    from app.telegram_bots.work_routing_admission import WorkRoutingAdmissions
    from deerflow.persistence.telegram_bots.model import TelegramActRow

    e = work_delivery
    plans = await setup(e, monkeypatch)
    await send(e)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, e.identity)
        before = row.payload
        control = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"]))
    controller = WorkRoutingAdmissions(plans, admissions(e).acts)
    staged = await controller.stage(e.link, control.id, chat_id=e.link.chat_id, message_id=971, message_thread_id=None, guard=await guard_for(e))
    with pytest.raises(DeliveryRetry):
        await e.recovery.refresh_anchor(e.link, e.identity, expected_version=0, guard=await guard_for(e))
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, e.identity)).payload == before
        journal = await reader.get(TelegramActRow, staged.id)
        assert journal.body == staged.body and journal.state == "pending" and journal.attempts == 0
        assert (await reader.get(TelegramCallbackRow, control.id)).payload["work_action_id"] == staged.id


@pytest.mark.asyncio
async def test_unacknowledged_routing_show_cannot_publish_private_content(work_delivery, monkeypatch):
    from app.telegram_bots.callbacks import CallbackRefused

    e = work_delivery
    await setup(e, monkeypatch, protected=True)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, e.identity)
        show = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"]))
    with pytest.raises(CallbackRefused):
        await e.recovery.reveal(e.link, show.id, chat_id=e.link.chat_id, message_id=971, message_thread_id=None, guard=await guard_for(e))
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, e.identity)
        assert row.payload["presentation_version"] == 0 and row.payload["work_request_basis"]["concealed"]
        assert (await reader.get(TelegramCallbackRow, show.id)).consumed_at is None


@pytest.mark.asyncio
async def test_explicit_routing_show_ignores_proactive_notification_preferences(work_delivery, monkeypatch):
    from deerflow.persistence.telegram_bots.model import TelegramPersonSettingsRow

    e = work_delivery
    await setup(e, monkeypatch, protected=True)
    await send(e)
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, e.identity)
        show = await writer.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"]))
        (await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)).notify = "none"
    e.sender.proactive = False
    await e.recovery.reveal(e.link, show.id, chat_id=e.link.chat_id, message_id=971, message_thread_id=None, guard=await guard_for(e))
    await send(e, endpoint="editMessageText")
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, e.identity)
        assert row.state == "sent", (row.state, row.skip_reason, e.dispatch_errors)
        assert row.class_ == "reply"
    assert "Who should answer instead?" in [call for call in e.channel.bot.request.calls if call.endpoint == "editMessageText"][-1].parameters["text"]
    # A later current-source refresh retains that direct input ownership.
    await e.recovery.refresh_anchor(e.link, e.identity, expected_version=1, guard=await guard_for(e))
    await send(e, endpoint="editMessageText")
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, e.identity)
        assert row.state == "sent" and row.class_ == "reply"
        assert row.payload["work_request_basis"]["reply_origin"] == show.id
        assert row.payload["work_request_basis"]["reveal"] is None
        assert not row.payload["work_request_basis"]["concealed"]


@pytest.mark.asyncio
async def test_routing_completion_rejects_missing_chunk_ack(work_delivery, monkeypatch):
    from copy import deepcopy

    from deerflow.persistence.agent_instances.human_input import HumanInputRequestRow, HumanInputResponseRow

    e = work_delivery
    await setup(e, monkeypatch)
    async with e.db.sessions.begin() as writer:
        for response in await writer.scalars(select(HumanInputResponseRow)):
            await writer.delete(response)
        request = await writer.get(HumanInputRequestRow, e.request["id"])
        request.recipient_id = None
        request.question = "Complete routing question. " * 230
        request.revision += 1
    e.clock.advance(10)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.sender.outbox._writer(e.link.id) as (writer, _binding):
        row = await writer.get(TelegramOutboxRow, e.identity, with_for_update=True)
        parts = deepcopy(row.parts)
        current = [part for part in parts if part["presentation_version"] == row.payload["presentation_version"]]
        assert len(current) > 1
        for index, part in enumerate(current):
            part.update(state="sent", message_ids=[980 + index], sent_at=e.clock().isoformat())
        current[0].update(state="unavailable", message_ids=[], sent_at=None)
        row.parts = parts
        await e.sender._complete(writer, row)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, e.identity)
        assert row.state == "failed" and row.failed_reason == "invalid-work-request-receipt"
        controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"])))
        assert controls and all(control.message_id is None and control.payload["presentation_pending"] for control in controls)


@pytest.mark.asyncio
async def test_failed_routing_repair_cannot_consume_another_presentation_group(work_delivery, monkeypatch):
    from copy import deepcopy

    from app.telegram_bots.producer import ProductSourceRetired

    e = work_delivery
    original = e.identity
    await setup(e, monkeypatch)
    async with e.db.sessions.begin() as writer:
        other = await writer.get(TelegramOutboxRow, original)
        foreign_group = other.parts[-1]["callback_group_id"]
        row = await writer.get(TelegramOutboxRow, e.identity)
        parts = deepcopy(row.parts)
        parts[-1]["callback_group_id"] = foreign_group
        row.parts = parts
        row.state, row.failed_reason = "failed", "work-routing-refresh-required"
    with pytest.raises(ProductSourceRetired):
        await e.recovery.repair(e.link, e.identity, guard=await guard_for(e))
    async with e.db.sessions() as reader:
        controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == foreign_group)))
        assert controls and all(control.consumed_at is None for control in controls)
        assert (await reader.get(TelegramOutboxRow, e.identity)).state == "failed"


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", [False, True])
async def test_routing_refresh_retains_and_requires_its_consumed_show(work_delivery, monkeypatch, missing):
    from app.telegram_bots.producer import ProductSourceRetired

    e = work_delivery
    await setup(e, monkeypatch, protected=True)
    await send(e)
    callbacks = e.channel._services.callbacks
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, e.identity)
        show = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"]))
    await e.recovery.reveal(e.link, show.id, chat_id=e.link.chat_id, message_id=971, message_thread_id=None, guard=await guard_for(e))
    await send(e, endpoint="editMessageText")
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, e.identity)
        before = row.payload
        group = row.parts[-1]["callback_group_id"]
    if missing:
        async with e.db.sessions.begin() as writer:
            await writer.delete(await writer.get(TelegramCallbackRow, show.id))
        with pytest.raises(ProductSourceRetired):
            await e.recovery.refresh_anchor(e.link, e.identity, expected_version=1, guard=await guard_for(e))
        async with e.db.sessions() as reader:
            assert (await reader.get(TelegramOutboxRow, e.identity)).payload == before
            assert all(control.consumed_at is None for control in await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == group)))
    else:
        e.clock.advance((callbacks.config.callbacks.sweep_after_days + 1) * 86400)
        await callbacks.sweep(e.clock())
        async with e.db.sessions() as reader:
            assert await reader.get(TelegramCallbackRow, show.id) is not None
        await e.recovery.refresh_anchor(e.link, e.identity, expected_version=1, guard=await guard_for(e))
        await send(e, endpoint="editMessageText")
        async with e.db.sessions() as reader:
            row = await reader.get(TelegramOutboxRow, e.identity)
            assert row.state == "sent", (row.failed_reason, e.dispatch_errors)
