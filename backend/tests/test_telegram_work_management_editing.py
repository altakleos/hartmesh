"""Saved edit typing owns a correction and never upgrades its original CAS."""

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import func, select
from test_telegram_assignment_prompt_inputs import accept_reply
from test_telegram_work_interaction_delivery import guard_for, selected
from test_telegram_work_management_delivery import open_card, prepare_management
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.assign import DraftAssignment
from deerflow.persistence.agent_instances.work import AgentWorkEventRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow


async def editing(e, *, unchanged=False):
    from app.telegram_bots.work_management_editing import WorkManagementEdits
    from app.telegram_bots.work_news_delivery import WorkNewsSources

    cards, button = await prepare_management(e, "work_change")
    await open_card(e, cards, button)
    if unchanged:
        from test_telegram_work_management_navigation import publish_edit

        save = await selected(e, "work_change_save")
        act = await cards.confirm(e.link, save.id, chat_id=e.link.chat_id, message_id=save.message_id, message_thread_id=None, guard=await guard_for(e))
        assert act.state == "refused" and act.reason == "unchanged"
        await cards.observe(e.link, e.identity)
        await publish_edit(e)
    old_card = e.identity
    edit = await selected(e, "assign_edit")
    edits = WorkManagementEdits(cards)
    e.channel._services.work_management_prompt_sources = WorkNewsSources(edits.prompts)
    scope = dict(chat_id=e.link.chat_id, message_id=edit.message_id, message_thread_id=None)
    prompt = await edits.open(e.link, edit.id, query_id="saved-edit", **scope, guard=await guard_for(e))
    assert await edits.open(e.link, edit.id, query_id="saved-edit", **scope, guard=await guard_for(e)) == prompt
    e.identity = prompt
    e.channel.bot.request.enqueue("editMessageReplyMarkup", body={"ok": True, "result": True})
    e.clock.advance(10)
    async with e.db.sessions() as reader:
        cleanup_id = (await reader.get(TelegramOutboxRow, prompt)).payload["management_prompt_cleanup_id"]
    for _ in range(8):
        claimed = await e.sender.tick()
        if claimed == 0:
            e.clock.advance(1)
            continue
        assert claimed == 1
        await e.sender.drain()
        async with e.db.sessions() as reader:
            if (await reader.get(TelegramOutboxRow, cleanup_id)).state == "sent":
                break
    else:
        pytest.fail("The actual old-keyboard cleanup was not acknowledged")
    from app.telegram_bots.producer import DeliveryRetry

    assert all(isinstance(error, DeliveryRetry) for error in e.dispatch_errors), e.dispatch_errors
    e.dispatch_errors.clear()
    await dispatch(e, 6301)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, prompt)
        assert row.state == "sent", (row.state, row.failed_reason, e.dispatch_errors)
        shown = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "prompt", TelegramCallbackRow.payload["anchor_outbox_id"].as_string() == prompt))
        assert shown.message_id == 6301 and shown.consumed_at is None
    return cards, edits, old_card, edit


@pytest.mark.asyncio
@pytest.mark.parametrize("unchanged", [False, True])
async def test_saved_edit_correction_extracts_once_and_saves_only_after_fresh_full_preview(work_delivery, monkeypatch, unchanged):
    e = work_delivery
    cards, edits, parent_id, edit = await editing(e, unchanged=unchanged)
    calls = []

    async def proposal(words, **kwargs):
        calls.append((words, kwargs["previous"]))
        return DraftAssignment(objective="Review the expanded guide", success_criteria="List verified corrections")

    monkeypatch.setattr("app.telegram_bots.work_management_editing.draft_assignment", proposal)
    incoming = await accept_reply(e, 6302, "Include the additional chapter", 6301)
    result = await edits.answer(e.link, incoming, guard=await guard_for(e))
    assert result == parent_id and await edits.answer(e.link, incoming, guard=await guard_for(e)) == result
    assert len(calls) == 1 and calls[0][0] == "Include the additional chapter" and calls[0][1].objective == e.work["objective"]
    async with e.db.sessions() as reader:
        original = await reader.get(TelegramActRow, edit.target_id)
        assert original.state == "refused" and original.reason == "replaced"
        parent = await reader.get(TelegramOutboxRow, parent_id)
        fresh_id = parent.payload["work_management_intent"]["draft_id"]
        fresh = await reader.get(TelegramActRow, fresh_id)
        assert fresh_id != original.id and fresh.work_id == e.work["id"]
        assert (fresh.body["expected_revision"], fresh.body["expected_assignment_revision"]) == (e.management_before["revision"], e.management_before["assignment_revision"])
        assert fresh.body["original"] == original.body["original"]
        assert await reader.scalar(select(func.count()).select_from(AgentWorkEventRow).where(AgentWorkEventRow.action == "edit")) == 0
    e.identity = parent_id
    e.clock.advance(10)
    e.channel.bot.request.enqueue("editMessageText", body={"ok": True, "result": {"message_id": 6103, "date": 1, "chat": {"id": e.link.chat_id, "type": "private"}, "text": "Corrected draft"}})
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not e.dispatch_errors, e.dispatch_errors
    rendered = [call.parameters.get("text", "") for call in e.channel.bot.request.calls if call.endpoint == "editMessageText"]
    assert "Review the expanded guide" in rendered[-1] and "List verified corrections" in rendered[-1]
    save = await selected(e, "work_change_save")
    assert save.target_id == fresh_id
    act = await cards.confirm(e.link, save.id, chat_id=e.link.chat_id, message_id=save.message_id, message_thread_id=None, guard=await guard_for(e))
    assert (await e.acts.check(act.id)).state == "done"
    current = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=e.work["id"])
    assert current["objective"] == "Review the expanded guide" and current["success_criteria"] == "List verified corrections"
    assert await edits.answer(e.link, incoming, guard=await guard_for(e)) == parent_id and len(calls) == 1


@pytest.mark.asyncio
async def test_failed_saved_edit_keeps_original_and_never_reextracts_on_replay(work_delivery, monkeypatch):
    e = work_delivery
    _cards, edits, _parent, edit = await editing(e)
    calls = []

    async def unavailable(words, **kwargs):
        calls.append(words)
        return None

    monkeypatch.setattr("app.telegram_bots.work_management_editing.draft_assignment", unavailable)
    incoming = await accept_reply(e, 6304, "Include the additional chapter", 6301)
    result = await edits.answer(e.link, incoming, guard=await guard_for(e))
    assert result.phase == "failed" and (await edits.answer(e.link, incoming, guard=await guard_for(e))).phase == "failed"
    assert calls == ["Include the additional chapter"]
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramActRow, edit.target_id)).state == "draft"
        prompt = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "prompt", TelegramCallbackRow.payload["purpose"].as_string() == "work_edit"))
        assert prompt.consumed_at is not None
    current = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=e.work["id"])
    assert current["revision"] == e.management_before["revision"]


@pytest.mark.asyncio
async def test_open_edit_retires_save_until_a_new_full_card_is_shown(work_delivery):
    from app.telegram_bots.producer import _item

    e = work_delivery
    cards, _edits, parent_id, _edit = await editing(e)
    async with e.db.sessions() as reader:
        item = _item(await reader.get(TelegramOutboxRow, parent_id))
    source = await cards.reader.read(e.link, item)
    assert source.work["phase"] == "editing"


@pytest.mark.asyncio
async def test_failed_edit_delivers_a_fresh_full_card_with_new_edit_controls(work_delivery, monkeypatch):
    from test_telegram_work_management_navigation import publish_edit

    e = work_delivery
    _cards, edits, parent_id, edit = await editing(e)

    async def unavailable(*args, **kwargs):
        return None

    monkeypatch.setattr("app.telegram_bots.work_management_editing.draft_assignment", unavailable)
    incoming = await accept_reply(e, 6312, "Include the extra chapter", 6301)
    assert (await edits.answer(e.link, incoming, guard=await guard_for(e))).phase == "failed"
    e.identity = parent_id
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, parent_id)).state == "pending"
    await publish_edit(e)
    fresh = await selected(e, "assign_edit")
    assert fresh is not None and fresh.id != edit.id and fresh.target_id == edit.target_id
    reopened = await edits.open(e.link, fresh.id, query_id="new-edit-after-failure", chat_id=e.link.chat_id, message_id=fresh.message_id, message_thread_id=None, guard=await guard_for(e))
    assert type(reopened) is str and reopened != incoming.outbox_id


@pytest.mark.asyncio
async def test_second_correction_uses_the_last_private_copy_and_original_revisions(work_delivery, monkeypatch):
    from test_telegram_work_management_navigation import publish_edit

    e = work_delivery
    cards, edits, parent_id, edit = await editing(e)
    calls = []

    async def proposal(words, **kwargs):
        calls.append((words, kwargs["previous"]))
        return DraftAssignment(objective="Corrected " + words, success_criteria="Verify the selected changes")

    monkeypatch.setattr("app.telegram_bots.work_management_editing.draft_assignment", proposal)
    first = await accept_reply(e, 6321, "first chapter", 6301)
    assert await edits.answer(e.link, first, guard=await guard_for(e)) == parent_id
    e.identity = parent_id
    await publish_edit(e)
    second_edit = await selected(e, "assign_edit")
    second_prompt = await edits.open(e.link, second_edit.id, query_id="second-correction", chat_id=e.link.chat_id, message_id=second_edit.message_id, message_thread_id=None, guard=await guard_for(e))
    e.identity = second_prompt
    e.channel.bot.request.enqueue("editMessageReplyMarkup", body={"ok": True, "result": True})
    e.clock.advance(10)
    async with e.db.sessions() as reader:
        cleanup_id = (await reader.get(TelegramOutboxRow, second_prompt)).payload["management_prompt_cleanup_id"]
    for _ in range(8):
        if await e.sender.tick() == 0:
            e.clock.advance(1)
            continue
        await e.sender.drain()
        async with e.db.sessions() as reader:
            if (await reader.get(TelegramOutboxRow, cleanup_id)).state == "sent":
                break
    else:
        pytest.fail("The second original-keyboard cleanup was not acknowledged")
    from app.telegram_bots.producer import DeliveryRetry

    assert all(isinstance(error, DeliveryRetry) for error in e.dispatch_errors), e.dispatch_errors
    e.dispatch_errors.clear()
    await dispatch(e, 6322)
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, second_prompt)).state == "sent"
    second = await accept_reply(e, 6323, "second chapter", 6322)
    assert await edits.answer(e.link, second, guard=await guard_for(e)) == parent_id
    assert calls[1][1].objective == "Corrected first chapter" and len(calls) == 2
    e.identity = parent_id
    await publish_edit(e)
    save = await selected(e, "work_change_save")
    async with e.db.sessions() as reader:
        original = await reader.get(TelegramActRow, edit.target_id)
        latest = await reader.get(TelegramActRow, save.target_id)
        assert latest.body["expected_revision"] == original.body["expected_revision"]
        assert latest.body["expected_assignment_revision"] == original.body["expected_assignment_revision"]
        assert latest.created_at == original.created_at
    act = await cards.confirm(e.link, save.id, chat_id=e.link.chat_id, message_id=save.message_id, message_thread_id=None, guard=await guard_for(e))
    assert (await e.acts.check(act.id)).state == "done"


@pytest.mark.asyncio
async def test_interrupted_extraction_recovers_without_another_model_call(work_delivery, monkeypatch):
    from app.telegram_bots.work_management_editing import WorkManagementEdits

    e = work_delivery
    cards, edits, parent_id, edit = await editing(e)
    calls = []

    async def interrupted(words, **kwargs):
        calls.append(words)
        raise RuntimeError("Synthetic process loss after the durable extraction claim")

    monkeypatch.setattr("app.telegram_bots.work_management_editing.draft_assignment", interrupted)
    incoming = await accept_reply(e, 6330, "Include the additional chapter", 6301)
    with pytest.raises(RuntimeError):
        await edits.answer(e.link, incoming, guard=await guard_for(e))
    replacement = WorkManagementEdits(cards)
    await replacement.recover(e.link, guard=await guard_for(e))
    assert (await replacement.answer(e.link, incoming, guard=await guard_for(e))).phase == "failed"
    assert calls == ["Include the additional chapter"]
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramActRow, edit.target_id)).state == "draft"
        assert (await reader.get(TelegramOutboxRow, parent_id)).state == "pending"


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["claim", "version"])
async def test_malformed_extraction_marker_never_becomes_a_known_failed_input(work_delivery, monkeypatch, damage):
    from app.telegram_bots.work_management_editing import WorkManagementEdits

    e = work_delivery
    cards, edits, _parent_id, _edit = await editing(e)

    async def interrupted(*args, **kwargs):
        raise RuntimeError("Synthetic process loss after the durable extraction claim")

    monkeypatch.setattr("app.telegram_bots.work_management_editing.draft_assignment", interrupted)
    incoming = await accept_reply(e, 6332, "Include the additional chapter", 6301)
    with pytest.raises(RuntimeError):
        await edits.answer(e.link, incoming, guard=await guard_for(e))
    async with e.db.sessions.begin() as writer:
        from sqlalchemy.orm.attributes import flag_modified

        prompt = await writer.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "prompt", TelegramCallbackRow.payload["purpose"].as_string() == "work_edit"))
        prompt_id = prompt.id
        marker = {**prompt.payload["management_input"], **({"claim_id": None} if damage == "claim" else {"version": True})}
        prompt.payload = {**prompt.payload, "management_input": marker}
        flag_modified(prompt, "payload")
    async with e.db.sessions() as reader:
        persisted = (await reader.get(TelegramCallbackRow, prompt_id)).payload["management_input"]
        assert (persisted["claim_id"] is None) if damage == "claim" else type(persisted["version"]) is bool
    await WorkManagementEdits(cards).recover(e.link, guard=await guard_for(e))
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramCallbackRow, prompt_id)).payload["management_input"] == marker


@pytest.mark.asyncio
async def test_edit_prompt_cannot_send_after_its_original_opening_gesture_is_lost(work_delivery):
    from app.telegram_bots.work_management_editing import WorkManagementEdits
    from app.telegram_bots.work_news_delivery import WorkNewsSources

    e = work_delivery
    cards, button = await prepare_management(e, "work_change")
    await open_card(e, cards, button)
    edit = await selected(e, "assign_edit")
    edits = WorkManagementEdits(cards)
    e.channel._services.work_management_prompt_sources = WorkNewsSources(edits.prompts)
    identity = await edits.open(e.link, edit.id, query_id="opening-custody", chat_id=e.link.chat_id, message_id=edit.message_id, message_thread_id=None, guard=await guard_for(e))
    async with e.db.sessions.begin() as writer:
        parent = await writer.get(TelegramOutboxRow, e.identity)
        gesture = parent.payload["work_management_intent"]["origin"]["gesture_id"]
        await writer.delete(await writer.get(TelegramCallbackRow, gesture))
    e.channel.bot.request.enqueue("editMessageReplyMarkup", body={"ok": True, "result": True})
    e.clock.advance(10)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    before = len([call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"])
    e.clock.advance(10)
    e.channel.bot.request.enqueue("sendMessage", body={"ok": True, "result": {"message_id": 6310, "date": 1, "chat": {"id": e.link.chat_id, "type": "private"}, "text": "Unowned prompt"}})
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert len([call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]) == before
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, identity)).state != "sent"
