"""Interrupted Save keeps canonical receipts and independent consent evidence."""

from datetime import timedelta

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import delete, func, select
from test_telegram_assignment_card_delivery import prepare_card, save_card
from test_telegram_work_interaction_delivery import guard_for, selected
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
from deerflow.persistence.agent_instances.work import AgentWorkRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow


@pytest.mark.asyncio
@pytest.mark.parametrize("witness", ["root", "callback", "presentation"])
async def test_callback_save_cannot_downgrade_itself_to_internal(work_delivery, witness):
    e = work_delivery
    plans, draft, _inbox = await prepare_card(e)
    await dispatch(e, 4901)
    button, admitted = await save_card(e, plans)
    async with e.db.sessions.begin() as writer:
        act = await writer.get(TelegramActRow, admitted.id)
        act.continuation = {**{key: value for key, value in act.continuation.items() if key != "delegate_origin"}, "delegate_source": {**act.continuation["delegate_source"], "origin_kind": "internal"}}
        for control in await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == button.group_id)):
            control.payload = {key: value for key, value in control.payload.items() if key != "work_action_id"}
        if witness != "root":
            (await writer.get(TelegramActRow, draft.id)).continuation = None
        if witness == "presentation":
            await writer.execute(delete(TelegramCallbackRow).where(TelegramCallbackRow.group_id == button.group_id))
    result = await e.acts.check(admitted.id)
    assert result.state != "done"
    async with e.db.sessions() as reader:
        assert await reader.scalar(select(func.count()).select_from(AgentWorkRow)) == 1


@pytest.mark.asyncio
async def test_no_due_child_cannot_substitute_unreviewed_deadline_words(work_delivery):
    e = work_delivery
    plans, _draft, _inbox = await prepare_card(e, due_at=e.sender.outbox.clock() + timedelta(days=1), due_text="the original deadline")
    await dispatch(e, 4902)
    _button, admitted = await save_card(e, plans)
    async with e.db.sessions.begin() as writer:
        act = await writer.get(TelegramActRow, admitted.id)
        act.continuation = {**act.continuation, "delegate_source": {**act.continuation["delegate_source"], "due_text": "unreviewed replacement deadline"}}
        (await writer.get(AgentInstanceGrantRow, (e.link.agent_ref, e.link.owner_user_id))).permissions = 3
    result = await e.acts.check(admitted.id)
    assert result.state != "done"
    async with e.db.sessions() as reader:
        assert await reader.scalar(select(func.count()).select_from(AgentWorkRow)) == 1


async def publish_saving(e, plans, draft, identity):
    await plans.observe(e.link, draft.id)
    e.channel.bot.request.enqueue("editMessageText", body={"ok": True, "result": {"message_id": identity, "date": 1, "chat": {"id": e.link.chat_id, "type": "private"}, "text": "Saving"}})
    e.clock.advance(10)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not e.dispatch_errors, e.dispatch_errors


@pytest.mark.asyncio
async def test_cancel_retains_its_fence_when_receipt_read_fails(work_delivery, monkeypatch):
    e = work_delivery
    plans, draft, _inbox = await prepare_card(e)
    await dispatch(e, 4903)
    _button, _admitted = await save_card(e, plans)
    await publish_saving(e, plans, draft, 4903)
    cancel = await selected(e, "cancel_wait")

    async def unavailable(_act):
        raise RuntimeError("synthetic receipt outage")

    monkeypatch.setattr(e.acts, "receipt_for", unavailable)
    result = await plans.cancel(e.link, cancel.id, chat_id=e.link.chat_id, message_id=cancel.message_id, message_thread_id=None, guard=await guard_for(e))
    assert result.cancel_requested_at is not None
    repeated = await plans.cancel(e.link, cancel.id, chat_id=e.link.chat_id, message_id=cancel.message_id, message_thread_id=None, guard=await guard_for(e))
    assert repeated.cancel_requested_at == result.cancel_requested_at
    assert repeated.state == result.state and repeated.attempts == result.attempts
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, e.identity)
        assert row.payload["work_news_basis"]["source"]["work"]["phase"] == "unconfirmed"
        assert await reader.scalar(select(func.count()).select_from(AgentWorkRow)) == 1


@pytest.mark.asyncio
async def test_lost_save_response_recovers_canonical_receipt_without_resubmission(work_delivery, monkeypatch):
    e = work_delivery
    plans, draft, _inbox = await prepare_card(e)
    await dispatch(e, 4904)
    _button, admitted = await save_card(e, plans)
    delegate, calls = e.acts.work.delegate, []

    async def lost(**kwargs):
        calls.append(kwargs)
        await delegate(**kwargs)
        raise OSError("synthetic lost response")

    monkeypatch.setattr(e.acts.work, "delegate", lost)
    assert (await e.acts.check(admitted.id)).state != "done"
    await plans.observe(e.link, draft.id)
    assert len(calls) == 1
    async with e.db.sessions() as reader:
        act = await reader.get(TelegramActRow, admitted.id)
        row = await reader.get(TelegramOutboxRow, e.identity)
        assert act.state == "done" and row.payload["work_news_basis"]["source"]["work"]["phase"] == "saved"
        assert await reader.scalar(select(func.count()).select_from(AgentWorkRow)) == 2


@pytest.mark.asyncio
async def test_observer_recovers_known_no_due_child_and_parent_atomically(work_delivery, monkeypatch):
    e = work_delivery
    plans, draft, _inbox = await prepare_card(e, due_at=e.sender.outbox.clock() + timedelta(days=1), due_text="the original deadline")
    await dispatch(e, 4905)
    _button, admitted = await save_card(e, plans)
    async with e.db.sessions.begin() as writer:
        (await writer.get(AgentInstanceGrantRow, (e.link.agent_ref, e.link.owner_user_id))).permissions = 3
    delegate, calls = e.acts.work.delegate, []

    async def lose_child(**kwargs):
        calls.append(kwargs["request"].operation_id)
        await delegate(**kwargs)
        raise RuntimeError("synthetic lost child reply")

    monkeypatch.setattr(e.acts.work, "delegate", lose_child)
    interrupted = await e.acts.check(admitted.id)
    assert interrupted.state == "pending" and len(interrupted.operation_ids) == 1
    child_id, before = interrupted.operation_ids[0], list(calls)
    await plans.observe(e.link, draft.id)
    assert calls == before
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramActRow, child_id)).state == "done"
        assert (await reader.get(TelegramActRow, admitted.id)).state == "done"
        row = await reader.get(TelegramOutboxRow, e.identity)
        source = row.payload["work_news_basis"]["source"]
        assert source["work"]["phase"] == "saved" and source["work"]["due_note"] is True
        assert "the original deadline" in source["work"]["detail"]["record"]["success_criteria"]


@pytest.mark.asyncio
async def test_observer_rechecks_root_custody_after_receipt_lookup(work_delivery, monkeypatch):
    from app.telegram_bots.authority import AuthorityUnavailable
    from app.telegram_bots.producer import ProductSourceRetired

    e = work_delivery
    plans, draft, _inbox = await prepare_card(e)
    await dispatch(e, 4906)
    _button, admitted = await save_card(e, plans)
    assert (await e.acts.check(admitted.id)).state == "done"
    async with e.db.sessions.begin() as writer:
        act = await writer.get(TelegramActRow, admitted.id)
        act.state, act.result = "pending", None
    receipt_for, changed = e.acts.receipt_for, []

    async def move_root(act):
        receipt = await receipt_for(act)
        if not changed:
            changed.append(True)
            async with e.db.sessions.begin() as writer:
                (await writer.get(TelegramActRow, draft.id)).operation_ids = ["e" * 32]
        return receipt

    monkeypatch.setattr(e.acts, "receipt_for", move_root)
    with pytest.raises((ProductSourceRetired, AuthorityUnavailable)):
        await plans.observe(e.link, draft.id)
    assert changed
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramActRow, admitted.id)).state == "pending"


@pytest.mark.asyncio
async def test_saved_full_preview_can_admit_a_distinct_explicit_start(work_delivery, monkeypatch):
    from test_telegram_work_start import prepare_startable

    from app.telegram_bots.work_start_controls import WorkStartAdmissions

    e = work_delivery
    await prepare_startable(e, monkeypatch)
    plans, draft, _inbox = await prepare_card(e)
    await dispatch(e, 4907)
    _button, delegated = await save_card(e, plans)
    saved = await e.acts.check(delegated.id)
    assert saved.state == "done" and not e.launches
    await publish_saving(e, plans, draft, 4907)
    start = await selected(e, "work_start")
    assert start is not None
    admitted = await WorkStartAdmissions(plans, e.acts).stage(e.link, start.id, chat_id=e.link.chat_id, message_id=start.message_id, message_thread_id=None, guard=await guard_for(e))
    assert admitted.id != delegated.id and admitted.work_id == saved.result["id"]
    assert not e.launches
    result = await e.acts.check(admitted.id)
    assert result.state == "done" and len(e.launches) == 1
    await plans.observe(e.link, draft.id)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, e.identity)
        current = [part for part in row.parts if part["presentation_version"] == row.payload["presentation_version"]]
        assert "saved and not started" not in "".join(part["visible_text"] for part in current)


@pytest.mark.asyncio
@pytest.mark.parametrize("stale_child", [False, True])
async def test_explicit_retry_owns_the_shown_no_due_child_counter(work_delivery, monkeypatch, stale_child):
    e = work_delivery
    plans, draft, _inbox = await prepare_card(e, due_at=e.sender.outbox.clock() + timedelta(days=1), due_text="the original deadline")
    await dispatch(e, 4908)
    _button, admitted = await save_card(e, plans)
    async with e.db.sessions.begin() as writer:
        (await writer.get(AgentInstanceGrantRow, (e.link.agent_ref, e.link.owner_user_id))).permissions = 3
    delegate, calls = e.acts.work.delegate, []

    async def fail_child(**kwargs):
        calls.append(kwargs["request"].operation_id)
        if kwargs["request"].due_at is not None:
            return await delegate(**kwargs)
        raise RuntimeError("synthetic unavailable child result")

    monkeypatch.setattr(e.acts.work, "delegate", fail_child)
    for _ in range(3):
        result = await e.acts.check(admitted.id)
    assert result.state == "uncertain" and len(result.operation_ids) == 1
    child_id = result.operation_ids[0]
    await publish_saving(e, plans, draft, 4908)
    retry = await selected(e, "act_check")
    assert retry is not None
    if stale_child:
        async with e.db.sessions.begin() as writer:
            (await writer.get(TelegramActRow, child_id)).attempts += 1
    else:
        monkeypatch.setattr(e.acts.work, "delegate", delegate)
    before = list(calls)
    recovered = await plans.retry(e.link, retry.id, chat_id=e.link.chat_id, message_id=retry.message_id, message_thread_id=None, guard=await guard_for(e))
    assert calls == before
    async with e.db.sessions() as reader:
        child = await reader.get(TelegramActRow, child_id)
        assert child.attempts == 4
        assert await reader.scalar(select(func.count()).select_from(AgentWorkRow)) == (1 if stale_child else 2)
    assert recovered.state == ("uncertain" if stale_child else "done")


@pytest.mark.asyncio
async def test_missing_forward_child_cannot_claim_unsaved_or_duplicate_the_save(work_delivery):
    from app.telegram_bots.producer import ProductSourceRetired

    e = work_delivery
    plans, draft, _inbox = await prepare_card(e, due_at=e.sender.outbox.clock() + timedelta(days=1), due_text="the original deadline")
    await dispatch(e, 4910)
    _button, admitted = await save_card(e, plans)
    async with e.db.sessions.begin() as writer:
        (await writer.get(AgentInstanceGrantRow, (e.link.agent_ref, e.link.owner_user_id))).permissions = 3
    assert (await e.acts.check(admitted.id)).state == "done"
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramActRow, admitted.id)
        row.operation_ids, row.state, row.reason, row.result = [], "uncertain", None, None
        (await writer.get(AgentInstanceGrantRow, (e.link.agent_ref, e.link.owner_user_id))).permissions = 7
    with pytest.raises(ProductSourceRetired):
        await plans.observe(e.link, draft.id)
    assert (await e.acts.check(admitted.id)).state != "done"
    async with e.db.sessions() as reader:
        assert await reader.scalar(select(func.count()).select_from(AgentWorkRow)) == 2


@pytest.mark.asyncio
async def test_lost_root_markers_never_reopen_an_already_admitted_draft(work_delivery):
    from uuid import uuid4

    from app.telegram_bots.producer import ProductSourceRetired

    e = work_delivery
    plans, draft, _inbox = await prepare_card(e)
    await dispatch(e, 4911)
    _button, admitted = await save_card(e, plans)
    async with e.db.sessions.begin() as writer:
        root = await writer.get(TelegramActRow, draft.id)
        root.operation_ids, root.continuation = [], None
    with pytest.raises(ProductSourceRetired):
        await plans.observe(e.link, draft.id)
    with pytest.raises(ProductSourceRetired):
        await e.acts.stage_delegate(e.link, draft.id, operation_id=uuid4().hex)
    async with e.db.sessions() as reader:
        assert await reader.scalar(select(func.count()).select_from(TelegramActRow).where(TelegramActRow.body["operation_id"].as_string().is_not(None))) == 1
        assert await reader.get(TelegramActRow, admitted.id) is not None
