import pytest
from _telegram_repository_test_support import db as db
from test_telegram_assignment_prompt_inputs import accept_reply
from test_telegram_work_interaction_delivery import guard_for
from test_telegram_work_management_editing import editing
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.assign import DraftAssignment
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow


@pytest.mark.asyncio
@pytest.mark.parametrize("success", [False, True])
async def test_saved_final_extraction_replay_repairs_publication_without_another_model_call(work_delivery, monkeypatch, success):
    e = work_delivery
    cards, edits, parent_id, control = await editing(e)
    calls = []

    async def proposal(*args, **kwargs):
        calls.append(True)
        return DraftAssignment(objective="Review the complete manual", success_criteria="Return verified corrections") if success else None

    monkeypatch.setattr("app.telegram_bots.work_management_editing.draft_assignment", proposal)
    incoming = await accept_reply(e, 6491, "Include the appendix", 6301)
    observe = cards.observe

    async def unavailable(*args, **kwargs):
        raise OSError("Synthetic publication observation failure after durable extraction")

    monkeypatch.setattr(cards, "observe", unavailable)
    with pytest.raises(OSError):
        await edits.answer(e.link, incoming, guard=await guard_for(e))
    async with e.db.sessions() as reader:
        prompt = await reader.get(TelegramCallbackRow, (await reader.get(TelegramOutboxRow, e.identity)).payload["management_prompt_intent"]["prompt_id"])
        assert prompt.payload["management_input"]["phase"] == ("completed" if success else "failed")
    monkeypatch.setattr(cards, "observe", observe)
    result = await edits.answer(e.link, incoming, guard=await guard_for(e))
    assert result == parent_id if success else result.phase == "failed"
    assert len(calls) == 1
    async with e.db.sessions() as reader:
        parent = await reader.get(TelegramOutboxRow, parent_id)
        assert parent.state == "pending"
        mark = await reader.get(TelegramInboundMarkRow, (incoming.link_id, incoming.message_id, incoming.chat_id))
        assert mark.route_complete and mark.outbox_id == parent_id
