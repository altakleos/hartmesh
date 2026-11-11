"""A manager's saved wrong-person response directly owns recipient choices."""

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_inbox import incoming
from test_telegram_work_input_admission import controller, stage_input
from test_telegram_work_input_capture import capture
from test_telegram_work_interaction_delivery import guard_for, open_view, selected
from test_telegram_work_preview_recovery import dispatch, install
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_telegram_work_typing_prompts import clear_old
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow


async def wrong_person(e, monkeypatch):
    from app.gateway.auth.models import User
    from app.gateway.routers import agent_instances
    from app.telegram_bots.inbox import InboxRepository
    from app.telegram_bots.work_routing import WorkRoutingReader
    from app.telegram_bots.work_routing_delivery import WorkRoutingPlans
    from deerflow.config import agents_api_config
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow

    monkeypatch.setattr(agents_api_config, "_agents_api_config", agents_api_config.AgentsApiConfig(enabled=True))
    e.http_app.include_router(agent_instances.router)
    person = await e.live.users.create_user(User(email="another@example.com"))
    e.recipient_id = str(person.id)
    async with e.db.sessions.begin() as writer:
        writer.add(AgentInstanceGrantRow(instance_id=e.instance.id, user_id=e.recipient_id, permissions=3))
    install(e)
    await dispatch(e, 801)
    e.identity = await open_view(e, await selected(e, "hi_cant_open"))
    await clear_old(e)
    await dispatch(e, 802)
    e.identity = await open_view(e, await selected(e, "hi_wrong"))
    await clear_old(e)
    await dispatch(e, 803)
    async with e.db.sessions() as reader:
        prompt = (await reader.get(TelegramOutboxRow, e.identity)).payload["work_request_basis"]["prompt_id"]
    inbox = InboxRepository(e.db.sessions)
    mark, _ = await inbox.accept(e.link, incoming(message=900, update_id=1900, account=e.link.chat_id, reply=803), sanitized_input={"text": "A colleague knows this."})
    await capture(e, inbox, mark, prompt)
    controls = controller(e, inbox)
    act = await stage_input(e, controls, mark)
    assert (await controls.controls.acts.check(act.id)).state == "done"
    reader = WorkRoutingReader(work=e.human.work, authority=e.live.authority, links=e.db.links, client=e.http_client)
    routing = WorkRoutingPlans(outbox=e.sender.outbox, reader=reader, callbacks=e.channel._services.callbacks)
    return controls.controls.acts, act, mark, routing


@pytest.mark.asyncio
async def test_saved_wrong_person_reply_is_the_acknowledged_routing_question(work_delivery, monkeypatch):
    from app.telegram_bots.work_outcome_delivery import WorkOutcomePlans, WorkOutcomeSources
    from app.telegram_bots.work_routing_admission import WorkRoutingAdmissions

    e = work_delivery
    acts, act, mark, routing = await wrong_person(e, monkeypatch)
    plans = WorkOutcomePlans(e.plans, acts, routing=routing)
    e.channel._services.work_outcome_sources = WorkOutcomeSources(plans)
    identity = await plans.plan(e.link, act.id, guard=await guard_for(e))
    await dispatch(e, 804)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "sent" and row.class_ == "reply", (row.failed_reason, e.dispatch_errors)
        assert "work_request_basis" not in row.payload
        control = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"]))
        assert control.kind == "route_to" and control.payload["recipient_id"] == e.recipient_id
        assert not control.payload["presentation_pending"]
        assert (await reader.get(TelegramInboundMarkRow, (mark.link_id, mark.message_id, mark.chat_id))).route_complete
    call = [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"][-1]
    assert call.parameters["text"] == "Noted. Who should answer instead?"
    staged = await WorkRoutingAdmissions(routing, acts).stage(e.link, control.id, chat_id=e.link.chat_id, message_id=804, message_thread_id=None, guard=await guard_for(e))
    assert staged.id != act.id
    assert (await acts.check(staged.id)).state == "done"


async def followup(e, monkeypatch, *, acknowledged=True):
    from app.telegram_bots.work_outcome_delivery import WorkOutcomePlans, WorkOutcomeSources
    from app.telegram_bots.work_routing_admission import WorkRoutingAdmissions

    acts, act, mark, routing = await wrong_person(e, monkeypatch)
    plans = WorkOutcomePlans(e.plans, acts, routing=routing)
    e.channel._services.work_outcome_sources = WorkOutcomeSources(plans)
    identity = await plans.plan(e.link, act.id, guard=await guard_for(e))
    if acknowledged:
        await dispatch(e, 804)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        control = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"]))
    return plans, WorkRoutingAdmissions(routing, acts), act, mark, row, control


async def route_followup(e, admission, control):
    return await admission.stage(e.link, control.id, chat_id=e.link.chat_id, message_id=804, message_thread_id=None, guard=await guard_for(e))


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["message_type", "timestamp", "part_identity", "basis_extra", "part_extra", "instance"])
async def test_followup_admission_requires_closed_typed_ack(work_delivery, monkeypatch, change):
    from sqlalchemy.orm.attributes import flag_modified

    from app.telegram_bots.producer import ProductSourceRetired
    from deerflow.persistence.telegram_bots.model import TelegramActRow

    e = work_delivery
    _plans, admission, _act, _mark, parent, control = await followup(e, monkeypatch)
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, parent.id)
        if change == "message_type":
            row.parts[0]["message_ids"] = [True]
        elif change == "timestamp":
            row.parts[0]["sent_at"] = "2026-01-01T00:00:00"
        elif change == "part_identity":
            row.parts[0]["part_id"] = "invalid"
        elif change == "basis_extra":
            row.payload["work_outcome_basis"]["unknown"] = True
        elif change == "part_extra":
            row.parts[0]["target_message_id"] = 999
        else:
            row.instance_id = "another-instance"
        flag_modified(row, "parts")
        flag_modified(row, "payload")
    with pytest.raises(ProductSourceRetired):
        await route_followup(e, admission, control)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramActRow, control.id) is None


@pytest.mark.asyncio
async def test_followup_before_ack_has_no_route_authority(work_delivery, monkeypatch):
    from app.telegram_bots.producer import ProductSourceRetired
    from deerflow.persistence.telegram_bots.model import TelegramActRow

    e = work_delivery
    _plans, admission, _act, _mark, _parent, control = await followup(e, monkeypatch, acknowledged=False)
    with pytest.raises(ProductSourceRetired):
        await route_followup(e, admission, control)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramActRow, control.id) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["receipt", "effect", "continuation", "candidate"])
async def test_followup_final_writer_reproves_origin_and_candidate(work_delivery, monkeypatch, change):
    from app.telegram_bots.producer import ProductSourceRetired
    from deerflow.agent_instances.contract import AgentDenied
    from deerflow.persistence.agent_instances.human_input import HumanInputEventRow
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
    from deerflow.persistence.agent_instances.work import AgentWorkEventRow
    from deerflow.persistence.telegram_bots.model import TelegramActRow

    e = work_delivery
    _plans, admission, act, _mark, _parent, control = await followup(e, monkeypatch)
    original = admission.acts._stage_work_act
    reached = []

    async def withdraw(*args, **kwargs):
        result = await original(*args, **kwargs)
        writer = kwargs["session"]
        reached.append(True)
        if change == "receipt":
            event = await writer.scalar(select(HumanInputEventRow).where(HumanInputEventRow.operation_id == act.id))
            event.receipt = {**event.receipt, "response_id": "another-response"}
        elif change == "effect":
            event = await writer.scalar(select(AgentWorkEventRow).where(AgentWorkEventRow.operation_id == act.id))
            event.actor_id = "another-actor"
        elif change == "continuation":
            (await writer.get(TelegramActRow, act.id)).continuation = None
        else:
            (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.recipient_id))).permissions = 1
        await writer.flush()
        return result

    monkeypatch.setattr(admission.acts, "_stage_work_act", withdraw)
    with pytest.raises((ProductSourceRetired, AgentDenied)):
        await route_followup(e, admission, control)
    assert reached
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramActRow, control.id) is None
        assert "work_action_id" not in (await reader.get(TelegramCallbackRow, control.id)).payload


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["manager", "empty", "protected"])
async def test_followup_uses_current_manager_roster_and_protection(work_delivery, monkeypatch, change):
    from app.telegram_bots.work_outcome_delivery import WorkOutcomePlans, WorkOutcomeSources
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
    from deerflow.persistence.telegram_bots.model import TelegramInstancePolicyRow, TelegramPersonSettingsRow

    e = work_delivery
    acts, act, _mark, routing = await wrong_person(e, monkeypatch)
    async with e.db.sessions.begin() as writer:
        if change == "manager":
            (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.actor.subject_id))).permissions = 3
        elif change == "empty":
            (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.recipient_id))).permissions = 1
        else:
            writer.add(TelegramInstancePolicyRow(instance_id=e.instance.id, policy="protected", updated_by=e.actor.subject_id, updated_at=e.sender.outbox.clock()))
            (await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)).notify = "none"
    e.sender.proactive = False
    plans = WorkOutcomePlans(e.plans, acts, routing=routing)
    e.channel._services.work_outcome_sources = WorkOutcomeSources(plans)
    identity = await plans.plan(e.link, act.id, guard=await guard_for(e))
    await dispatch(e, 804)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "sent", (row.failed_reason, e.dispatch_errors)
        if change == "manager":
            assert row.payload["work_outcome_basis"]["version"] == 1
            assert all(button.get("kind") != "route_to" for line in row.payload["product_keyboard_basis"] for button in line)
        else:
            assert row.payload["work_outcome_basis"]["version"] == 3
            people = row.payload["work_outcome_basis"]["routing"]["routing"]["recipients"]
            assert bool(people) == (change == "protected")
        if change == "protected":
            assert row.parts[0]["protect_content"] is True
    text = [call.parameters["text"] for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"][-1]
    assert (text == "Noted. Who should answer instead?") == (change == "protected")


@pytest.mark.asyncio
async def test_followup_route_replays_and_edits_only_its_selection(work_delivery, monkeypatch):
    from sqlalchemy import func
    from test_telegram_bot_adapter import message

    from deerflow.persistence.agent_instances.human_input import HumanInputEventRow

    e = work_delivery
    plans, admission, act, _mark, parent, control = await followup(e, monkeypatch)
    staged = await route_followup(e, admission, control)
    assert (await admission.acts.check(staged.id)).state == "done"
    replay = await route_followup(e, admission, control)
    assert replay.id == staged.id and replay.body == staged.body
    identity = await plans.plan(e.link, staged.id, guard=await guard_for(e))
    message(e.channel.bot.request, endpoint="editMessageText", identity=804)
    e.clock.advance(10)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "sent", (row.failed_reason, e.dispatch_errors)
        assert row.parts[0]["target_message_id"] == 804
        assert row.payload["work_outcome_basis"]["origin"]["anchor_outbox_id"] == parent.id
        assert await reader.scalar(select(func.count()).select_from(HumanInputEventRow).where(HumanInputEventRow.action == "route")) == 1
    assert await plans.plan(e.link, act.id, guard=await guard_for(e)) == parent.id


@pytest.mark.asyncio
async def test_followup_final_sdk_fence_rejects_candidate_withdrawn_after_read(work_delivery, monkeypatch):
    from app.telegram_bots.producer import ProductSourceRetired
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow

    e = work_delivery
    plans, _admission, _act, _mark, _parent, _control = await followup(e, monkeypatch, acknowledged=False)
    original = plans.routing.reader.read_routing
    reached = []
    calls_before = len(e.channel.bot.request.calls)

    async def withdraw(*args, **kwargs):
        result = await original(*args, **kwargs)
        if not reached:
            reached.append(True)
            async with e.db.sessions.begin() as writer:
                (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.recipient_id))).permissions = 1
        return result

    monkeypatch.setattr(plans.routing.reader, "read_routing", withdraw)
    e.clock.advance(10)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert reached and isinstance(e.dispatch_errors[-1], ProductSourceRetired), e.dispatch_errors
    assert not any(call.endpoint == "sendMessage" for call in e.channel.bot.request.calls[calls_before:])
