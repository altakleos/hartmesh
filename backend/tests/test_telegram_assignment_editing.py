"""Editing uses one full acknowledged draft and a distinct accepted correction."""

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_assignment_card_delivery import prepare_card
from test_telegram_assignment_prompt_inputs import accept_reply
from test_telegram_work_interaction_delivery import guard_for, selected
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.assign import DraftAssignment
from app.telegram_bots.callbacks import CallbackRefused
from app.telegram_bots.producer import ProductSourceRetired
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramOutboxRow


async def editing(e):
    from app.telegram_bots.assignment_inputs import AssignmentInputs
    from app.telegram_bots.assignment_prompt_delivery import AssignmentPromptPlans
    from app.telegram_bots.assignment_prompt_sources import AssignmentPromptReader
    from app.telegram_bots.work_news_delivery import WorkNewsSources

    cards, draft, _ = await prepare_card(e)
    await dispatch(e, 5401)
    old_card = e.identity
    prompts = AssignmentPromptPlans(reader=AssignmentPromptReader(e.acts), outbox=e.sender.outbox, callbacks=cards.callbacks)
    e.channel._services.assignment_prompt_sources = WorkNewsSources(prompts)
    button = await selected(e, "assign_edit")
    scope = dict(chat_id=e.link.chat_id, message_id=button.message_id, message_thread_id=None, guard=await guard_for(e))
    return cards, prompts, AssignmentInputs(prompts, cards), draft, old_card, button, scope


@pytest.mark.asyncio
async def test_actual_edit_reply_replaces_one_draft_and_extracts_once(work_delivery, monkeypatch):
    e = work_delivery
    cards, prompts, inputs, draft, old_card, button, scope = await editing(e)
    e.identity = await prompts.edit(e.link, button.id, **scope)
    await dispatch(e, 5402)
    calls = []

    async def proposal(words, **kwargs):
        calls.append((words, kwargs["previous"]))
        return DraftAssignment(objective="Review the expanded guide", success_criteria="Record verified corrections")

    monkeypatch.setattr("app.telegram_bots.assignment_requests.draft_assignment", proposal)
    correction = await accept_reply(e, 5403, "Include the additional chapter", 5402)
    identity = await inputs.answer(e.link, correction, guard=await guard_for(e))
    assert type(identity) is str and await inputs.answer(e.link, correction, guard=await guard_for(e)) == identity
    assert len(calls) == 1 and calls[0][0] == "Include the additional chapter" and calls[0][1] == draft.draft
    async with e.db.sessions() as reader:
        old = await reader.get(TelegramActRow, draft.id)
        assert old.state == "refused" and old.reason == "replaced" and not old.operation_ids
        assert (await reader.get(TelegramOutboxRow, old_card)).payload["work_news_basis"]["source"]["work"]["phase"] == "replaced"
        fresh = await reader.get(TelegramOutboxRow, identity)
        assert fresh.payload["work_news_basis"]["source"]["work"]["source"] == "edit"


@pytest.mark.asyncio
async def test_changed_draft_cannot_open_an_edit_of_unseen_words(work_delivery):
    e = work_delivery
    _cards, prompts, _inputs, draft, _old, button, scope = await editing(e)
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramActRow, draft.id)
        row.body = {**row.body, "draft": {**row.body["draft"], "objective": "Changed unseen words"}}
    with pytest.raises((CallbackRefused, ProductSourceRetired)):
        await prompts.edit(e.link, button.id, **scope)


@pytest.mark.asyncio
async def test_save_of_original_during_extraction_prevents_replacement(work_delivery, monkeypatch):
    from test_telegram_assignment_card_delivery import save_card

    from app.telegram_bots.assignment_requests import DraftRequest

    e = work_delivery
    cards, prompts, inputs, draft, old_card, button, scope = await editing(e)
    e.identity = await prompts.edit(e.link, button.id, **scope)
    await dispatch(e, 5410)
    calls = []

    async def proposal(words, **kwargs):
        calls.append(words)
        current = e.identity
        e.identity = old_card
        try:
            await save_card(e, cards)
        finally:
            e.identity = current
        return DraftAssignment(objective="Unadopted edit", success_criteria="Must never replace saved original")

    monkeypatch.setattr("app.telegram_bots.assignment_requests.draft_assignment", proposal)
    correction = await accept_reply(e, 5411, "Include the extra chapter", 5410)
    result = await inputs.answer(e.link, correction, guard=await guard_for(e))
    assert isinstance(result, DraftRequest) and result.phase == "extracting"
    assert calls == ["Include the extra chapter"]
    async with e.db.sessions() as reader:
        original = await reader.get(TelegramActRow, draft.id)
        pending = await reader.get(TelegramActRow, result.id)
        assert original.state == "draft" and len(original.operation_ids) == 1
        assert pending.body["version"] == 2


@pytest.mark.asyncio
async def test_failed_edit_keeps_original_draft_and_never_reruns_extraction(work_delivery, monkeypatch):
    e = work_delivery
    _cards, prompts, inputs, draft, _old_card, button, scope = await editing(e)
    e.identity = await prompts.edit(e.link, button.id, **scope)
    await dispatch(e, 5412)
    calls = []

    async def unavailable(words, **kwargs):
        calls.append(words)
        return None

    monkeypatch.setattr("app.telegram_bots.assignment_requests.draft_assignment", unavailable)
    correction = await accept_reply(e, 5413, "Include the extra chapter", 5412)
    result = await inputs.answer(e.link, correction, guard=await guard_for(e))
    assert result.phase == "failed"
    assert (await inputs.answer(e.link, correction, guard=await guard_for(e))).phase == "failed"
    assert calls == ["Include the extra chapter"]
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramActRow, draft.id)).state == "draft"


@pytest.mark.asyncio
@pytest.mark.parametrize("disposition", ["save", "discard"])
async def test_replaced_input_replay_observes_its_new_card_after_disposition(work_delivery, monkeypatch, disposition):
    from app.telegram_bots.assignment_controls import AssignmentAdmissions
    from app.telegram_bots.assignment_navigation import AssignmentDraftNavigation

    e = work_delivery
    cards, prompts, inputs, _draft, _old_card, button, scope = await editing(e)
    e.identity = await prompts.edit(e.link, button.id, **scope)
    await dispatch(e, 5414)
    calls = []

    async def proposal(words, **kwargs):
        calls.append(words)
        return DraftAssignment(objective="Review the expanded guide", success_criteria="List verified changes")

    monkeypatch.setattr("app.telegram_bots.assignment_requests.draft_assignment", proposal)
    correction = await accept_reply(e, 5415, "Include the extra chapter", 5414)
    e.identity = await inputs.answer(e.link, correction, guard=await guard_for(e))
    # The replaced original card is an edit owed before the new card.
    e.channel.bot.request.enqueue("editMessageText", body={"ok": True, "result": {"message_id": 5401, "date": 1, "chat": {"id": e.link.chat_id, "type": "private"}, "text": "Replaced"}})
    e.clock.advance(10)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    await dispatch(e, 5416)
    selected_button = await selected(e, "assign_save" if disposition == "save" else "assign_discard")
    actual = dict(chat_id=e.link.chat_id, message_id=selected_button.message_id, message_thread_id=None, guard=await guard_for(e))
    if disposition == "save":
        admitted = await AssignmentAdmissions(cards, e.acts).stage(e.link, selected_button.id, **actual)
        assert (await e.acts.check(admitted.id)).state == "done"
    else:
        await AssignmentDraftNavigation(cards).discard(e.link, selected_button.id, **actual)
    assert await inputs.answer(e.link, correction, guard=await guard_for(e)) == e.identity
    assert calls == ["Include the extra chapter"]


@pytest.mark.asyncio
async def test_edit_prompt_replays_one_query_and_distinguishes_a_later_press(work_delivery):
    e = work_delivery
    _cards, prompts, _inputs, _draft, _old, button, scope = await editing(e)
    first = await prompts.edit(e.link, button.id, query_id="first-edit-query", **scope)
    assert await prompts.edit(e.link, button.id, query_id="first-edit-query", **scope) == first
    second = await prompts.edit(e.link, button.id, query_id="second-edit-query", **scope)
    assert second != first
    async with e.db.sessions() as reader:
        first_row, second_row = await reader.get(TelegramOutboxRow, first), await reader.get(TelegramOutboxRow, second)
        assert first_row.payload["assignment_prompt_intent"]["query_key"] != second_row.payload["assignment_prompt_intent"]["query_key"]


@pytest.mark.asyncio
async def test_new_acknowledged_card_edit_button_has_its_own_prompt_identity(work_delivery):
    from deerflow.persistence.telegram_bots.model import TelegramPersonSettingsRow

    e = work_delivery
    cards, prompts, _inputs, draft, old_card, button, scope = await editing(e)
    first = await prompts.edit(e.link, button.id, **scope)
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)).locale = "zh-CN"
    await cards.observe(e.link, draft.id)
    e.identity = old_card
    e.channel.bot.request.enqueue("editMessageText", body={"ok": True, "result": {"message_id": 5401, "date": 1, "chat": {"id": e.link.chat_id, "type": "private"}, "text": "Updated card"}})
    e.clock.advance(10)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    actual = await selected(e, "assign_edit")
    assert actual and actual.id != button.id and actual.message_id == 5401
    second = await prompts.edit(e.link, actual.id, chat_id=e.link.chat_id, message_id=actual.message_id, message_thread_id=None, guard=await guard_for(e))
    assert second != first
