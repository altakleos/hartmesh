"""Draft-only navigation uses actual shown cards and never mutates Work."""

from datetime import timedelta

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import func, select
from test_telegram_assignment_card_delivery import prepare_card, save_card
from test_telegram_work_interaction_delivery import guard_for, selected
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.callbacks import CallbackRefused
from app.telegram_bots.producer import ProductSourceRetired
from deerflow.persistence.agent_instances.work import AgentWorkRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow


@pytest.mark.asyncio
async def test_discard_replays_its_exact_closed_draft_without_creating_work(work_delivery):
    from app.telegram_bots.assignment_navigation import AssignmentDraftNavigation

    e = work_delivery
    plans, draft, _ = await prepare_card(e)
    await dispatch(e, 5201)
    button = await selected(e, "assign_discard")
    navigation = AssignmentDraftNavigation(plans)
    scope = dict(chat_id=e.link.chat_id, message_id=button.message_id, message_thread_id=None, guard=await guard_for(e))
    result = await navigation.discard(e.link, button.id, **scope)
    again = await navigation.discard(e.link, button.id, **scope)
    assert result.id == again.id == draft.id and result.reason == again.reason == "discarded"
    assert again.updated_at == result.updated_at
    async with e.db.sessions() as reader:
        assert await reader.scalar(select(func.count()).select_from(AgentWorkRow)) == 1
        row = await reader.get(TelegramOutboxRow, e.identity)
        assert row.payload["work_news_basis"]["source"]["work"]["phase"] == "discarded"
        members = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == button.group_id)))
        assert members and all(member.consumed_at is not None for member in members)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["unacknowledged", "expired", "journal", "words"])
async def test_discard_cannot_reclassify_unseen_changed_or_admitted_draft(work_delivery, damage):
    from app.telegram_bots.assignment_navigation import AssignmentDraftNavigation

    e = work_delivery
    plans, draft, _ = await prepare_card(e)
    if damage != "unacknowledged":
        await dispatch(e, 5202)
    button = await selected(e, "assign_discard")
    if damage == "expired":
        e.clock.advance(timedelta(hours=25).total_seconds())
    elif damage == "journal":
        await save_card(e, plans)
    elif damage == "words":
        async with e.db.sessions.begin() as writer:
            row = await writer.get(TelegramActRow, draft.id)
            row.body = {**row.body, "draft": {**row.body["draft"], "objective": "Changed unseen words"}}
    with pytest.raises((CallbackRefused, ProductSourceRetired)):
        await AssignmentDraftNavigation(plans).discard(e.link, button.id, chat_id=e.link.chat_id, message_id=button.message_id or 5202, message_thread_id=None, guard=await guard_for(e))
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramActRow, draft.id)).state == "draft"
