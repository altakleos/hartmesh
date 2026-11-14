import asyncio

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_assignment_prompt_inputs import accept_reply
from test_telegram_work_interaction_delivery import guard_for
from test_telegram_work_management_editing import editing
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.assign import DraftAssignment
from app.telegram_bots.producer import ProductSourceRetired


@pytest.mark.asyncio
async def test_two_independent_owners_claim_the_same_saved_edit_only_once(work_delivery, monkeypatch):
    from app.telegram_bots import work_management_editing as module

    e = work_delivery
    cards, first, parent_id, control = await editing(e)
    second = module.WorkManagementEdits(cards)
    incoming = await accept_reply(e, 6493, "Include the final appendix", 6301)
    real = module.prepare_attention
    ready = asyncio.Event()
    prepared = []

    async def together(*args, **kwargs):
        current = await real(*args, **kwargs)
        prepared.append(True)
        if len(prepared) == 2:
            ready.set()
        await ready.wait()
        return current

    monkeypatch.setattr(module, "prepare_attention", together)
    calls = []

    async def proposal(*args, **kwargs):
        await kwargs["before_dispatch"]()
        calls.append(True)
        return DraftAssignment(objective="Review the full manual", success_criteria="Return verified corrections")

    monkeypatch.setattr(module, "draft_assignment", proposal)
    one = asyncio.create_task(first.answer(e.link, incoming, guard=await guard_for(e)))
    two = asyncio.create_task(second.answer(e.link, incoming, guard=await guard_for(e)))
    try:
        results = await asyncio.wait_for(asyncio.gather(one, two, return_exceptions=True), 60)
    finally:
        ready.set()
        await asyncio.gather(one, two, return_exceptions=True)
    assert calls == [True]
    assert sum(result == parent_id for result in results) == 1
    assert sum(isinstance(result, ProductSourceRetired) for result in results) == 1
