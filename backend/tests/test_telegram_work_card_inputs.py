"""Explicit card replies own a shown source and never become decision commands."""

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_inbox import incoming
from test_telegram_work_actions import management_request
from test_telegram_work_control_admission import admissions
from test_telegram_work_interaction_delivery import guard_for
from test_telegram_work_preview_recovery import dispatch, install
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.inbox import InboxRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow


async def card(e, purpose, *, long=False):
    install(e)
    request = e.request
    if purpose != "information":
        _work, request, _seen = await management_request(e, purpose)
        async with e.db.sessions.begin() as writer:
            (await writer.get(TelegramOutboxRow, e.identity)).state = "skipped"
        e.identity = await e.plans.plan(e.link, request_id=request["id"], dedupe_key="direct-card")
    if long:
        from deerflow.persistence.agent_instances.human_input import HumanInputRequestRow

        async with e.db.sessions.begin() as writer:
            row = await writer.get(HumanInputRequestRow, request["id"])
            row.question, row.reason = "Complete question. " * 180, "Complete context. " * 180
            (await writer.get(TelegramOutboxRow, e.identity)).state = "skipped"
        e.identity = await e.plans.plan(e.link, request_id=request["id"], dedupe_key="long-direct-card")
    async with e.db.sessions() as reader:
        count = len((await reader.get(TelegramOutboxRow, e.identity)).parts)
    for index in range(count):
        await dispatch(e, 801 + index)
    return request


async def accepted(e, *, reply=801, message=900):
    update = incoming(message=message, update_id=1900 + message, account=e.link.chat_id, reply=reply)
    return (await e.plans.outbox.inbox.accept(e.link, update, sanitized_input={"type": "text", "text": update.message.text}))[0]


@pytest.mark.asyncio
@pytest.mark.parametrize("purpose", ["information", "decision", "review"])
async def test_card_reply_captures_before_admission_and_saves_one_response(work_delivery, purpose):
    from app.telegram_bots.work_card_inputs import WorkCardInputs

    e = work_delivery
    request = await card(e, purpose)
    origin = await accepted(e)
    host = WorkCardInputs(admissions(e))
    captured = await host.capture(e.link, origin, source_outbox_id=e.identity, guard=await guard_for(e))
    assert captured.route_basis["work_input"]["purpose"] == ("answer" if purpose == "information" else "comment")
    async with e.db.sessions() as reader:
        assert not list(await reader.scalars(select(TelegramActRow)))
        assert all(row.consumed_at is not None for row in await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.payload["anchor_outbox_id"].as_string() == e.identity)))
    act = await host.stage(e.link, origin, guard=await guard_for(e))
    assert act.id == captured.operation_ids[0] and act.kind == "respond"
    assert act.body["text"] == origin.sanitized_input["text"]
    assert (await host.controls.acts.check(act.id)).state == "done"
    assert (await host.stage(e.link, origin, guard=await guard_for(e))).id == act.id
    responses = (await e.human.responses(actor=e.actor, request_id=request["id"]))["responses"]
    assert len(responses) == 1 and responses[0]["text"] == origin.sanitized_input["text"]
    assert (await e.human.get(actor=e.actor, request_id=request["id"]))["state"] == ("answered" if purpose == "information" else "pending")
    async with e.db.sessions() as reader:
        assert len(list(await reader.scalars(select(TelegramActRow)))) == 1
        mark = InboxRow.from_row(await reader.get(TelegramInboundMarkRow, (origin.link_id, origin.message_id, origin.chat_id)))
        assert mark.route_basis["work_action_id"] == act.id


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["reply", "topic", "media", "ack", "control", "reserved"])
async def test_card_capture_requires_exact_plain_reply_and_full_shown_group(work_delivery, damage):
    from app.telegram_bots.producer import PermanentDeliveryError, ProductSourceRetired
    from app.telegram_bots.work_card_inputs import WorkCardInputs

    e = work_delivery
    await card(e, "information")
    origin = await accepted(e, reply=999 if damage == "reply" else 801)
    async with e.db.sessions.begin() as writer:
        mark = await writer.get(TelegramInboundMarkRow, (origin.link_id, origin.message_id, origin.chat_id))
        parent = await writer.get(TelegramOutboxRow, e.identity)
        rows = list(await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == parent.parts[-1]["callback_group_id"])))
        if damage == "topic":
            mark.message_thread_id = 123
        elif damage == "media":
            mark.sanitized_input = {**mark.sanitized_input, "files": ["other"]}
        elif damage == "ack":
            parent.parts = [{**part, "message_ids": [True]} for part in parent.parts]
        elif damage == "control":
            for row in rows:
                row.message_id = 999
        elif damage == "reserved":
            for row in rows:
                row.payload = {**row.payload, "work_action_id": None}
    with pytest.raises((ValueError, ProductSourceRetired, PermanentDeliveryError)):
        await WorkCardInputs(admissions(e)).capture(e.link, origin, source_outbox_id=e.identity, guard=await guard_for(e))
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramInboundMarkRow, (origin.link_id, origin.message_id, origin.chat_id))).route_kind is None
        assert not list(await reader.scalars(select(TelegramActRow)))


@pytest.mark.asyncio
async def test_missing_original_card_reply_journal_cannot_be_recreated(work_delivery):
    from app.telegram_bots.callbacks import CallbackRefused
    from app.telegram_bots.work_card_inputs import WorkCardInputs

    e = work_delivery
    await card(e, "information")
    origin = await accepted(e)
    host = WorkCardInputs(admissions(e))
    await host.capture(e.link, origin, source_outbox_id=e.identity, guard=await guard_for(e))
    act = await host.stage(e.link, origin, guard=await guard_for(e))
    async with e.db.sessions.begin() as writer:
        await writer.delete(await writer.get(TelegramActRow, act.id))
    with pytest.raises(CallbackRefused):
        await host.stage(e.link, origin, guard=await guard_for(e))
    async with e.db.sessions() as reader:
        assert not list(await reader.scalars(select(TelegramActRow)))


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["input", "coherent_input", "callback", "grant", "body", "kind", "source"])
async def test_card_input_final_writer_rejects_late_drift(work_delivery, monkeypatch, damage):
    from app.telegram_bots.callbacks import CallbackRefused
    from app.telegram_bots.work_card_inputs import WorkCardInputs
    from deerflow.agent_instances.contract import AgentConflict, AgentDenied
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow

    e = work_delivery
    await card(e, "information")
    origin = await accepted(e)
    host = WorkCardInputs(admissions(e))
    captured = await host.capture(e.link, origin, source_outbox_id=e.identity, guard=await guard_for(e))
    original, reached = host.controls.acts._stage_work_act, []

    async def drift(*args, **kwargs):
        result = await original(*args, **kwargs)
        writer = kwargs["session"]
        if damage in {"input", "coherent_input"}:
            mark = await writer.get(TelegramInboundMarkRow, (origin.link_id, origin.message_id, origin.chat_id))
            mark.sanitized_input = {**mark.sanitized_input, "text": "Different at final fence"}
            if damage == "coherent_input":
                mark.route_basis = {**mark.route_basis, "work_input": {**mark.route_basis["work_input"], "text": "Different at final fence"}}
        elif damage == "callback":
            row = await writer.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == captured.route_basis["work_input"]["group_id"]))
            row.closed_reason = "command"
        elif damage == "grant":
            (await writer.get(AgentInstanceGrantRow, (e.link.agent_ref, e.link.owner_user_id))).permissions = 1
        elif damage == "source":
            from deerflow.persistence.agent_instances.human_input import HumanInputRequestRow

            row = await writer.get(HumanInputRequestRow, captured.route_basis["work_input"]["request_id"])
            row.request_revision += 1
            row.revision += 1
        else:
            row = await writer.get(TelegramActRow, result.id)
            if damage == "body":
                row.body = {**row.body, "text": "Altered command"}
            else:
                row.kind = "decide"
        await writer.flush()
        reached.append(True)
        return result

    monkeypatch.setattr(host.controls.acts, "_stage_work_act", drift)
    with pytest.raises((CallbackRefused, AgentDenied, AgentConflict)):
        await host.stage(e.link, origin, guard=await guard_for(e))
    assert reached
    async with e.db.sessions() as reader:
        assert not list(await reader.scalars(select(TelegramActRow)))
        mark = await reader.get(TelegramInboundMarkRow, (origin.link_id, origin.message_id, origin.chat_id))
        assert mark.sanitized_input == origin.sanitized_input and "work_action_id" not in mark.route_basis


@pytest.mark.asyncio
async def test_reply_to_first_part_requires_complete_multipart_card(work_delivery):
    from app.telegram_bots.work_card_inputs import WorkCardInputs

    e = work_delivery
    await card(e, "information", long=True)
    async with e.db.sessions() as reader:
        parent = await reader.get(TelegramOutboxRow, e.identity)
        assert len(parent.parts) > 1 and parent.parts[-1]["message_ids"] != [801]
    origin = await accepted(e)
    host = WorkCardInputs(admissions(e))
    captured = await host.capture(e.link, origin, source_outbox_id=e.identity, guard=await guard_for(e))
    assert captured.route_basis["work_input"]["reply_to_message_id"] == 801
    act = await host.stage(e.link, origin, guard=await guard_for(e))
    assert (await host.controls.acts.check(act.id)).state == "done"


@pytest.mark.asyncio
async def test_capture_keeps_original_input_across_its_own_delivery_registration(work_delivery, monkeypatch):
    from app.telegram_bots.callbacks import CallbackRefused
    from app.telegram_bots.work_card_inputs import WorkCardInputs

    e = work_delivery
    await card(e, "information")
    origin = await accepted(e)
    host = WorkCardInputs(admissions(e))
    original, changed = host.inbox.register_delivery, []

    async def drift(*args, **kwargs):
        result = await original(*args, **kwargs)
        writer = kwargs["session"]
        mark = await writer.get(TelegramInboundMarkRow, (origin.link_id, origin.message_id, origin.chat_id))
        mark.sanitized_input = {**mark.sanitized_input, "text": "Changed while capturing"}
        mark.route_basis = {**mark.route_basis, "work_input": {**mark.route_basis["work_input"], "text": "Changed while capturing"}}
        await writer.flush()
        changed.append(True)
        return result

    monkeypatch.setattr(host.inbox, "register_delivery", drift)
    with pytest.raises(CallbackRefused):
        await host.capture(e.link, origin, source_outbox_id=e.identity, guard=await guard_for(e))
    assert changed
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramInboundMarkRow, (origin.link_id, origin.message_id, origin.chat_id))).route_kind is None
        assert all(row.consumed_at is None for row in await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.payload["anchor_outbox_id"].as_string() == e.identity)))


@pytest.mark.asyncio
async def test_card_reply_admission_binds_request_to_current_instance(work_delivery):
    from uuid import uuid4

    from app.telegram_bots.work_card_inputs import WorkCardInputs
    from deerflow.agent_instances.contract import AgentDenied
    from deerflow.persistence.agent_instances.human_input import HumanInputRequestRow
    from deerflow.persistence.agent_instances.model import AgentInstanceRow

    e = work_delivery
    request = await card(e, "information")
    origin = await accepted(e)
    host = WorkCardInputs(admissions(e))
    await host.capture(e.link, origin, source_outbox_id=e.identity, guard=await guard_for(e))
    async with e.db.sessions.begin() as writer:
        current = await writer.get(AgentInstanceRow, e.link.agent_ref)
        values = {column.name: getattr(current, column.name) for column in AgentInstanceRow.__table__.columns}
        identity = uuid4().hex
        writer.add(AgentInstanceRow(**{**values, "id": identity, "principal_id": "agent:" + identity, "creation_id": uuid4().hex, "status": "provisioning", "home_id": None}))
        await writer.flush()
        (await writer.get(HumanInputRequestRow, request["id"])).instance_id = identity
    with pytest.raises(AgentDenied):
        await host.stage(e.link, origin, guard=await guard_for(e))
    async with e.db.sessions() as reader:
        assert not list(await reader.scalars(select(TelegramActRow)))


@pytest.mark.asyncio
@pytest.mark.parametrize("admitted", [False, True])
async def test_settled_card_input_cannot_start_a_new_operation(work_delivery, admitted):
    from app.telegram_bots.callbacks import CallbackRefused
    from app.telegram_bots.work_card_inputs import WorkCardInputs

    e = work_delivery
    await card(e, "information")
    origin = await accepted(e)
    host = WorkCardInputs(admissions(e))
    captured = await host.capture(e.link, origin, source_outbox_id=e.identity, guard=await guard_for(e))
    action = await host.stage(e.link, origin, guard=await guard_for(e)) if admitted else None
    async with e.db.sessions.begin() as writer:
        await host.inbox.finish_route(captured, session=writer)
    if admitted:
        assert (await host.stage(e.link, origin, guard=await guard_for(e))).id == action.id
    else:
        with pytest.raises(CallbackRefused):
            await host.stage(e.link, origin, guard=await guard_for(e))
    async with e.db.sessions() as reader:
        assert len(list(await reader.scalars(select(TelegramActRow)))) == int(admitted)
