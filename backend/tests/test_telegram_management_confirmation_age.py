import pytest
from _telegram_repository_test_support import db as db
from test_telegram_work_interaction_delivery import guard_for, selected
from test_telegram_work_management_delivery import open_card, prepare_management
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.telegram_bots.model import TelegramActRow


@pytest.mark.asyncio
async def test_withdraw_expiry_crossed_in_final_authority_read_rolls_back_confirmation(work_delivery, monkeypatch):
    from app.telegram_bots import work_management_actions as module

    e = work_delivery
    plans, button = await prepare_management(e, "work_withdraw")
    await open_card(e, plans, button)
    control = await selected(e, "work_withdraw_confirm")
    prepare = module.prepare_attention
    changed = []

    async def expire_at_final(*args, **kwargs):
        current = await prepare(*args, **kwargs)

        async def final(*args, **kwargs):
            projection = kwargs.get("projection")
            if isinstance(projection, module.ManagementProjection) and projection.staged and projection.consent is not None and not projection.consent.admitting:
                changed.append(True)
                e.clock.advance(e.acts.outbox.config.callbacks.draft_ttl_hours * 3600 + 1)
            return await current(*args, **kwargs)

        return final

    monkeypatch.setattr(module, "prepare_attention", expire_at_final)
    with pytest.raises(Exception):
        await plans.confirm(e.link, control.id, chat_id=e.link.chat_id, message_id=control.message_id, message_thread_id=None, guard=await guard_for(e))
    assert changed
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramActRow, control.id) is None
