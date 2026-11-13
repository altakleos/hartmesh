"""Shown assignment typing owns one durable extraction instead of ordinary chat."""

import pytest
from _telegram_bot_test_support import update_payload
from _telegram_repository_test_support import db as db
from sqlalchemy import func, select
from telegram import Update
from test_telegram_assignment_prompt_delivery import prepare_words
from test_telegram_work_interaction_delivery import guard_for
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.assign import DraftAssignment
from deerflow.persistence.agent_instances.work import AgentWorkRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow


async def accept_reply(e, identity, text, quoted):
    payload = update_payload(identity, account=e.link.chat_id, text=text)
    payload["message"]["reply_to_message"] = {
        "message_id": quoted,
        "date": payload["message"]["date"],
        "chat": payload["message"]["chat"],
        "from": {"id": e.link.bot_id, "is_bot": True, "first_name": "Fixture bot"},
        "text": "Assignment prompt",
    }
    return (await e.sender.outbox.inbox.accept(e.link, Update.de_json(payload, None), sanitized_input={"type": "text", "text": text, "reply_to_message_id": quoted}))[0]


@pytest.mark.asyncio
async def test_typed_prompt_answer_extracts_once_and_preserves_one_private_card(work_delivery, monkeypatch):
    from app.telegram_bots.assignment_delivery import AssignmentPlans
    from app.telegram_bots.assignment_inputs import AssignmentInputs
    from app.telegram_bots.assignment_sources import AssignmentReader

    e = work_delivery
    prompts, _ = await prepare_words(e)
    await dispatch(e, 5311)
    calls = []

    async def proposal(words, **kwargs):
        calls.append(words)
        return DraftAssignment(objective="Review the handbook", success_criteria="Record verified corrections")

    monkeypatch.setattr("app.telegram_bots.assignment_requests.draft_assignment", proposal)
    origin = await accept_reply(e, 5312, "Review the handbook", 5311)
    cards = AssignmentPlans(reader=AssignmentReader(e.acts), outbox=e.sender.outbox, callbacks=prompts.callbacks)
    inputs = AssignmentInputs(prompts, cards)
    first = await inputs.answer(e.link, origin, guard=await guard_for(e))
    replay = await inputs.answer(e.link, origin, guard=await guard_for(e))
    assert first == replay and calls == ["Review the handbook"]
    async with e.db.sessions() as reader:
        prompt = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "prompt"))
        assert prompt.consumed_at is not None
        assert await reader.scalar(select(func.count()).select_from(AgentWorkRow)) == 1
        assert await reader.scalar(select(func.count()).select_from(TelegramActRow)) == 2


@pytest.mark.asyncio
async def test_unseen_prompt_cannot_claim_a_typed_answer_or_call_the_model(work_delivery, monkeypatch):
    from app.telegram_bots.assignment_delivery import AssignmentPlans
    from app.telegram_bots.assignment_inputs import AssignmentInputs
    from app.telegram_bots.assignment_sources import AssignmentReader

    e = work_delivery
    prompts, _ = await prepare_words(e)
    calls = []

    async def proposal(*args, **kwargs):
        calls.append(True)
        raise AssertionError("unseen input")

    monkeypatch.setattr("app.telegram_bots.assignment_requests.draft_assignment", proposal)
    origin = await accept_reply(e, 5313, "Unseen answer", 5311)
    cards = AssignmentPlans(reader=AssignmentReader(e.acts), outbox=e.sender.outbox, callbacks=prompts.callbacks)
    assert await AssignmentInputs(prompts, cards).answer(e.link, origin, guard=await guard_for(e)) is None
    assert not calls


@pytest.mark.asyncio
async def test_manual_done_when_uses_the_actual_shown_fallback_without_another_model(work_delivery, monkeypatch):
    from app.telegram_bots.assignment_delivery import AssignmentPlans
    from app.telegram_bots.assignment_inputs import AssignmentInputs
    from app.telegram_bots.assignment_sources import AssignmentReader
    from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

    e = work_delivery
    prompts, _ = await prepare_words(e)
    await dispatch(e, 5320)
    calls = []

    async def unavailable(words, **kwargs):
        calls.append(words)
        return None

    monkeypatch.setattr("app.telegram_bots.assignment_requests.draft_assignment", unavailable)
    cards = AssignmentPlans(reader=AssignmentReader(e.acts), outbox=e.sender.outbox, callbacks=prompts.callbacks)
    inputs = AssignmentInputs(prompts, cards)
    words = await accept_reply(e, 5321, "Review the handbook", 5320)
    e.identity = await inputs.answer(e.link, words, guard=await guard_for(e))
    await dispatch(e, 5322)
    criteria = await accept_reply(e, 5323, "Record every verified correction", 5322)
    identity = await inputs.answer(e.link, criteria, guard=await guard_for(e))
    assert identity and calls == ["Review the handbook"]
    assert await inputs.answer(e.link, criteria, guard=await guard_for(e)) == identity
    async with e.db.sessions() as reader:
        card = await reader.get(TelegramOutboxRow, identity)
        draft = card.payload["work_news_basis"]["source"]["work"]["draft"]
        assert draft["objective"] == "Review the handbook" and draft["success_criteria"] == "Record every verified correction"
