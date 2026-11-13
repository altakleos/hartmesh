"""Assignment typing starts only after a complete recorded prompt acknowledgement."""

import pytest
from _telegram_bot_test_support import make_update
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_assignment_card_delivery import prepare_card
from test_telegram_work_interaction_delivery import guard_for
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.callbacks import Prompts
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow


async def prepare_words(e):
    from app.telegram_bots.assignment_prompt_delivery import AssignmentPromptPlans
    from app.telegram_bots.assignment_prompt_sources import AssignmentPromptReader
    from app.telegram_bots.work_news_delivery import WorkNewsSources

    cards, _, _ = await prepare_card(e)
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramOutboxRow, e.identity)).state = "skipped"
    plans = AssignmentPromptPlans(reader=AssignmentPromptReader(e.acts), outbox=e.sender.outbox, callbacks=cards.callbacks)
    e.channel._services.assignment_prompt_sources = WorkNewsSources(plans)
    inbox, _ = await e.sender.outbox.inbox.accept(e.link, make_update(5300, account=e.link.chat_id, text="/assign"), sanitized_input={"command": "assign"})
    e.identity = await plans.plan(e.link, inbox, purpose="assign_words", guard=await guard_for(e))
    return plans, inbox


@pytest.mark.asyncio
async def test_prompt_visibility_begins_at_its_actual_ack_without_a_model_or_work(work_delivery):
    e = work_delivery
    plans, inbox = await prepare_words(e)
    async with e.db.sessions() as reader:
        prompt = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "prompt"))
        assert prompt.message_id is None and prompt.expires_at is None
    assert await Prompts(plans.callbacks).current(e.link, e.link.chat_id, None, guard=await guard_for(e)) is None
    await dispatch(e, 5301)
    assert not e.dispatch_errors, e.dispatch_errors
    prompt = await Prompts(plans.callbacks).current(e.link, e.link.chat_id, None, guard=await guard_for(e))
    assert prompt and prompt.payload["purpose"] == "assign_words" and prompt.message_id == 5301
    calls = [call.parameters for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
    assert len(calls) == 1 and calls[0]["reply_markup"]["force_reply"]
    assert await plans.plan(e.link, inbox, purpose="assign_words", guard=await guard_for(e)) == e.identity


@pytest.mark.asyncio
async def test_prompt_final_sdk_fence_rejects_changed_original_input(work_delivery, monkeypatch):
    from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow

    e = work_delivery
    plans, inbox = await prepare_words(e)
    get, changed = plans.settings.get, []

    async def retire(*args, **kwargs):
        result = await get(*args, **kwargs)
        if not changed:
            changed.append(True)
            async with e.db.sessions.begin() as writer:
                (await writer.get(TelegramInboundMarkRow, (inbox.link_id, inbox.message_id, inbox.chat_id))).sanitized_input = {"command": "help"}
        return result

    monkeypatch.setattr(plans.settings, "get", retire)
    e.channel._services.assignment_prompt_sources.settings = plans.settings
    await dispatch(e, 5302)
    assert changed and not [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]


@pytest.mark.asyncio
async def test_queued_locale_refresh_arms_only_the_replacement_prompt_ack(work_delivery):
    from deerflow.persistence.telegram_bots.model import TelegramPersonSettingsRow

    e = work_delivery
    plans, _inbox = await prepare_words(e)
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)).locale = "zh-CN"
    await dispatch(e, 5330)
    assert not [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
    from app.telegram_bots.producer import DeliveryClaimRetired

    assert e.dispatch_errors and all(isinstance(error, DeliveryClaimRetired) for error in e.dispatch_errors)
    e.dispatch_errors.clear()
    e.clock.advance(10)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not e.dispatch_errors, e.dispatch_errors
    prompt = await Prompts(plans.callbacks).current(e.link, e.link.chat_id, None, guard=await guard_for(e))
    assert prompt and prompt.message_id == 5330 and prompt.payload["presentation_version"] == 1
