"""Draft previews use actual Telegram acknowledgements and current SQL custody."""

import pytest
from _telegram_bot_test_support import ManualClock, make_update
from _telegram_repository_test_support import db as db
from sqlalchemy import func, select
from test_telegram_work_interaction_delivery import guard_for, selected
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.acts import Acts
from app.telegram_bots.assign import DraftAssignment
from app.telegram_bots.assignment_drafts import AssignmentDrafts
from deerflow.persistence.agent_instances.work import AgentWorkRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow


async def prepare_card(e, *, objective="Review the guide", criteria="List verified gaps", due_at=None, due_text=None):
    from app.telegram_bots.assignment_delivery import AssignmentPlans
    from app.telegram_bots.assignment_sources import AssignmentReader
    from app.telegram_bots.work_news_delivery import WorkNewsSources

    e.clock = ManualClock(e.sender.outbox.clock())
    e.sender.outbox.clock = e.clock.now
    e.channel._now = e.clock.now
    services = e.channel._services
    services.callbacks.clock = e.clock.now
    e.acts = Acts(e.db.sessions, settings=e.db.settings, callbacks=services.callbacks, outbox=e.sender.outbox, work=e.human.work, authority=e.live.authority, links=e.db.links)
    inbox, _ = await e.sender.outbox.inbox.accept(e.link, make_update(4801, account=e.link.chat_id, text="/assign " + objective), sanitized_input={"command": "assign", "text": objective})
    draft = await AssignmentDrafts(e.acts).create(e.link, inbox, draft=DraftAssignment(objective=objective, success_criteria=criteria, due_at=due_at, due_text=due_text), source="command")
    plans = AssignmentPlans(reader=AssignmentReader(e.acts), outbox=e.sender.outbox, callbacks=services.callbacks)
    services.assignment_sources = WorkNewsSources(plans)
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramOutboxRow, e.identity)).state = "skipped"
    e.identity = await plans.plan(e.link, draft.id, guard=await guard_for(e))
    return plans, draft, inbox


@pytest.mark.asyncio
async def test_draft_preview_is_a_durable_reply_and_creates_no_work(work_delivery):
    e = work_delivery
    plans, draft, _inbox = await prepare_card(e)
    assert await plans.plan(e.link, draft.id, guard=await guard_for(e)) == e.identity
    await dispatch(e, 4802)
    assert not e.dispatch_errors, e.dispatch_errors
    button = await selected(e, "assign_save")
    assert button.message_id == 4802 and not button.payload["presentation_pending"]
    calls = [call.parameters for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
    assert len(calls) == 1 and "Review the guide" in calls[0]["text"] and "List verified gaps" in calls[0]["text"]
    async with e.db.sessions() as reader:
        assert await reader.scalar(select(func.count()).select_from(AgentWorkRow)) == 1
        assert await reader.scalar(select(func.count()).select_from(TelegramActRow)) == 1
        row = await reader.get(TelegramOutboxRow, e.identity)
        assert row.state == "sent" and row.class_ == "reply"


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["draft", "input", "inspect"])
async def test_final_draft_sdk_fence_rejects_changed_source_or_grant(work_delivery, monkeypatch, change):
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
    from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow

    e = work_delivery
    _plans, draft, inbox = await prepare_card(e)
    sources = e.channel._services.assignment_sources
    get = sources.settings.get
    changed = []

    async def retire(*args, **kwargs):
        result = await get(*args, **kwargs)
        if not changed:
            changed.append(True)
            async with e.db.sessions.begin() as writer:
                if change == "draft":
                    row = await writer.get(TelegramActRow, draft.id)
                    row.body = {**row.body, "draft": {**row.body["draft"], "objective": "Other words"}}
                elif change == "input":
                    row = await writer.get(TelegramInboundMarkRow, (inbox.link_id, inbox.message_id, inbox.chat_id))
                    row.sanitized_input = {"command": "help"}
                else:
                    (await writer.get(AgentInstanceGrantRow, (e.link.agent_ref, e.link.owner_user_id))).permissions = 1
        return result

    monkeypatch.setattr(sources.settings, "get", retire)
    await dispatch(e, 4803)
    assert changed and not [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]


@pytest.mark.asyncio
async def test_unacknowledged_card_never_admits_save(work_delivery):
    from app.telegram_bots.assignment_controls import AssignmentAdmissions
    from app.telegram_bots.callbacks import CallbackRefused

    e = work_delivery
    plans, _draft, _inbox = await prepare_card(e)
    button = await selected(e, "assign_save")
    with pytest.raises(CallbackRefused):
        await AssignmentAdmissions(plans, e.acts).stage(e.link, button.id, chat_id=e.link.chat_id, message_id=4804, message_thread_id=None, guard=await guard_for(e))
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramActRow, button.id) is None
    assert not any(call.endpoint == "sendMessage" for call in e.channel.bot.request.calls)


async def save_card(e, plans, *, kind="assign_save"):
    from app.telegram_bots.assignment_controls import AssignmentAdmissions

    button = await selected(e, kind)
    admitted = await AssignmentAdmissions(plans, e.acts).stage(e.link, button.id, chat_id=e.link.chat_id, message_id=button.message_id, message_thread_id=None, guard=await guard_for(e))
    return button, admitted


@pytest.mark.asyncio
async def test_full_ack_save_owns_one_operation_and_current_saved_preview(work_delivery):
    e = work_delivery
    plans, draft, _inbox = await prepare_card(e)
    await dispatch(e, 4810)
    button, admitted = await save_card(e, plans)
    assert admitted.id == button.id and admitted.attempts == 0 and admitted.kind == "delegate"
    assert admitted.continuation["delegate_origin"]["anchor_outbox_id"] == e.identity
    _button, replay = await save_card(e, plans)
    assert replay.body == admitted.body
    async with e.db.sessions() as reader:
        root = await reader.get(TelegramActRow, draft.id)
        controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == button.group_id)))
        assert root.operation_ids == [button.id] and all(control.payload["work_action_id"] == button.id for control in controls)
        assert await reader.scalar(select(func.count()).select_from(AgentWorkRow)) == 1
    saved = await e.acts.check(admitted.id)
    assert saved.state == "done"
    assert await plans.observe(e.link, draft.id) == e.identity
    async with e.db.sessions() as reader:
        parent = await reader.get(TelegramOutboxRow, e.identity)
        source = parent.payload["work_news_basis"]["source"]
        assert source["work"]["phase"] == "saved"
        assert source["work"]["detail"]["record"]["id"] == saved.result["id"]
        assert source["work_id"] is None
        assert any(control["kind"] == "work_start" for line in parent.payload["product_keyboard_basis"] for control in line)
        assert await reader.scalar(select(func.count()).select_from(AgentWorkRow)) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["binding", "body", "late_message"])
async def test_save_replay_rechecks_original_journal_and_press(work_delivery, monkeypatch, change):
    from contextlib import asynccontextmanager

    from app.telegram_bots.assignment_controls import AssignmentAdmissions
    from app.telegram_bots.callbacks import CallbackRefused
    from deerflow.agent_instances.contract import AgentDenied

    e = work_delivery
    plans, _draft, _inbox = await prepare_card(e)
    await dispatch(e, 4820)
    button, admitted = await save_card(e, plans)
    if change in {"binding", "body"}:
        async with e.db.sessions.begin() as writer:
            row = await writer.get(TelegramActRow, admitted.id)
            if change == "binding":
                row.binding_generation += 1
            else:
                row.body = {**row.body, "objective": "Different words without consent"}
    else:
        real = plans.callbacks.writer

        @asynccontextmanager
        async def move_message(*args, **kwargs):
            async with e.db.sessions.begin() as writer:
                (await writer.get(TelegramCallbackRow, button.id)).message_id += 999
            async with real(*args, **kwargs) as writer:
                yield writer

        monkeypatch.setattr(plans.callbacks, "writer", move_message)
    with pytest.raises((CallbackRefused, AgentDenied)):
        await AssignmentAdmissions(plans, e.acts).stage(e.link, button.id, chat_id=e.link.chat_id, message_id=button.message_id, message_thread_id=button.message_thread_id, guard=await guard_for(e))


@pytest.mark.asyncio
async def test_draft_controls_must_name_the_actual_full_ack_message(work_delivery):
    from app.telegram_bots.assignment_controls import AssignmentAdmissions
    from app.telegram_bots.callbacks import CallbackRefused

    e = work_delivery
    plans, _draft, _inbox = await prepare_card(e)
    await dispatch(e, 4821)
    button = await selected(e, "assign_save")
    forged = button.message_id + 999
    async with e.db.sessions.begin() as writer:
        for control in await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == button.group_id)):
            control.message_id = forged
    with pytest.raises(CallbackRefused):
        await AssignmentAdmissions(plans, e.acts).stage(e.link, button.id, chat_id=e.link.chat_id, message_id=forged, message_thread_id=button.message_thread_id, guard=await guard_for(e))
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramActRow, button.id) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["message", "ack", "marker", "origin"])
async def test_save_effect_rechecks_full_original_consent(work_delivery, monkeypatch, damage):
    e = work_delivery
    plans, _draft, _inbox = await prepare_card(e)
    await dispatch(e, 4830)
    button, admitted = await save_card(e, plans)
    delegate = e.acts.work.delegate
    changed = []
    if damage == "origin":
        changed.append(True)
        async with e.db.sessions.begin() as writer:
            act = await writer.get(TelegramActRow, admitted.id)
            act.continuation = {key: value for key, value in act.continuation.items() if key != "delegate_origin"}

    async def move_consent(**kwargs):
        changed.append(True)
        async with e.db.sessions.begin() as writer:
            if damage == "message":
                (await writer.get(TelegramCallbackRow, button.id)).message_id += 999
            elif damage == "ack":
                parent = await writer.get(TelegramOutboxRow, e.identity)
                parent.parts = [{**part, "message_ids": [999999]} for part in parent.parts]
            elif damage == "marker":
                control = await writer.get(TelegramCallbackRow, button.id)
                control.payload = {key: value for key, value in control.payload.items() if key != "work_action_id"}
            else:
                act = await writer.get(TelegramActRow, admitted.id)
                act.continuation = {key: value for key, value in act.continuation.items() if key != "delegate_origin"}
        return await delegate(**kwargs)

    if damage != "origin":
        monkeypatch.setattr(e.acts.work, "delegate", move_consent)
    result = await e.acts.check(admitted.id)
    assert changed and result.state != "done"
    async with e.db.sessions() as reader:
        assert await reader.scalar(select(func.count()).select_from(AgentWorkRow)) == 1


@pytest.mark.asyncio
async def test_pending_save_cancel_never_creates_work_or_starts_a_retry(work_delivery):
    e = work_delivery
    plans, draft, _inbox = await prepare_card(e)
    await dispatch(e, 4840)
    _button, admitted = await save_card(e, plans)
    await plans.observe(e.link, draft.id)
    e.channel.bot.request.enqueue("editMessageText", body={"ok": True, "result": {"message_id": 4840, "date": 1, "chat": {"id": e.link.chat_id, "type": "private"}, "text": "Saving"}})
    e.clock.advance(10)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not e.dispatch_errors, e.dispatch_errors
    cancel = await selected(e, "cancel_wait")
    assert cancel.message_id == 4840
    result = await plans.cancel(e.link, cancel.id, chat_id=e.link.chat_id, message_id=cancel.message_id, message_thread_id=None, guard=await guard_for(e))
    assert result.cancel_requested_at is not None
    assert (await e.acts.check(admitted.id)).state != "done"
    async with e.db.sessions() as reader:
        assert await reader.scalar(select(func.count()).select_from(AgentWorkRow)) == 1


@pytest.mark.asyncio
async def test_long_draft_requires_every_ordered_ack_before_save(work_delivery):
    from app.telegram_bots.assignment_controls import AssignmentAdmissions
    from app.telegram_bots.callbacks import CallbackRefused

    e = work_delivery
    objective, criteria = "<>&Ω" * 1024, "✓<>&" * 1024
    plans, _draft, _inbox = await prepare_card(e, objective=objective, criteria=criteria)
    async with e.db.sessions() as reader:
        count = len((await reader.get(TelegramOutboxRow, e.identity)).parts)
    assert count > 1
    await dispatch(e, 4841)
    button = await selected(e, "assign_save")
    with pytest.raises(CallbackRefused):
        await AssignmentAdmissions(plans, e.acts).stage(e.link, button.id, chat_id=e.link.chat_id, message_id=4841, message_thread_id=None, guard=await guard_for(e))
    for ordinal in range(1, count):
        await dispatch(e, 4841 + ordinal)
    button, admitted = await save_card(e, plans)
    assert button.message_id == 4840 + count
    assert admitted.body["objective"] == objective and admitted.body["success_criteria"] == criteria


def run_ceiling(monkeypatch, permission):
    from app.gateway import authz

    resolve = authz.resolve_route_permissions

    async def current(*args, **kwargs):
        held = set(await resolve(*args, **kwargs))
        return (held - {"runs:create", "runs:write"}) | {permission}

    monkeypatch.setattr(authz, "resolve_route_permissions", current)


@pytest.mark.asyncio
@pytest.mark.parametrize("permission", ["runs:create", "runs:write"])
async def test_draft_preview_uses_actual_run_creation_ceiling(work_delivery, monkeypatch, permission):
    e = work_delivery
    run_ceiling(monkeypatch, permission)
    await prepare_card(e)
    await dispatch(e, 4842)
    assert (await selected(e, "assign_start") is not None) == (permission == "runs:create")


@pytest.mark.asyncio
@pytest.mark.parametrize("permission", ["runs:create", "runs:write"])
async def test_start_press_rechecks_actual_run_creation_ceiling(work_delivery, monkeypatch, permission):
    from app.telegram_bots.assignment_controls import AssignmentAdmissions
    from app.telegram_bots.authority import CeilingRefused
    from app.telegram_bots.callbacks import CallbackRefused

    e = work_delivery
    plans, _draft, _inbox = await prepare_card(e)
    await dispatch(e, 4843)
    button = await selected(e, "assign_start")
    assert button is not None
    run_ceiling(monkeypatch, permission)

    async def stage():
        return await AssignmentAdmissions(plans, e.acts).stage(e.link, button.id, chat_id=e.link.chat_id, message_id=button.message_id, message_thread_id=None, guard=await guard_for(e))

    if permission == "runs:create":
        assert (await stage()).continuation["delegate_source"]["start"] is True
    else:
        with pytest.raises((CeilingRefused, CallbackRefused)):
            await stage()
