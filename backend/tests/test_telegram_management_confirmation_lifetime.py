import pytest
from _telegram_repository_test_support import db as db
from test_telegram_work_interaction_delivery import selected
from test_telegram_work_management_delivery import open_card, prepare_management
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow


@pytest.mark.asyncio
async def test_confirmation_owner_retires_at_last_authority_read(work_delivery, monkeypatch):
    from app.telegram_bots import work_management_actions as module

    e = work_delivery
    plans, button = await prepare_management(e, "work_withdraw")
    await open_card(e, plans, button)
    control = await selected(e, "work_withdraw_confirm")
    active = [True]
    owner = await e.live.authority.prepare_live_owner(e.link.owner_user_id)
    guard = e.db.links.publication_guard(e.link, check_live_owner=owner, check_attempt_current=lambda: active[0])
    prepare = module.prepare_attention

    async def retire_at_final(*args, **kwargs):
        current = await prepare(*args, **kwargs)

        async def final(*args, **kwargs):
            projection = kwargs.get("projection")
            result = await current(*args, **kwargs)
            if isinstance(projection, module.ManagementProjection) and projection.staged and projection.consent is not None and not projection.consent.admitting:
                active[0] = False
            return result

        return final

    monkeypatch.setattr(module, "prepare_attention", retire_at_final)
    with pytest.raises(ValueError, match="publication-stale"):
        await plans.confirm(e.link, control.id, chat_id=e.link.chat_id, message_id=control.message_id, message_thread_id=None, guard=guard)
    assert not active[0]
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramActRow, control.id) is None
        assert (await reader.get(TelegramCallbackRow, control.id)).payload.get("work_action_id") is None


@pytest.mark.asyncio
async def test_opening_owner_retires_at_final_private_draft_read(work_delivery, monkeypatch):
    from sqlalchemy import func, select

    from app.telegram_bots import work_edit_drafts as module

    e = work_delivery
    plans, control = await prepare_management(e, "work_change")
    active, reads = [True], []
    owner = await e.live.authority.prepare_live_owner(e.link.owner_user_id)
    guard = e.db.links.publication_guard(e.link, check_live_owner=owner, check_attempt_current=lambda: active[0])
    prepare = module.prepare_attention

    async def retire_at_final(*args, **kwargs):
        current = await prepare(*args, **kwargs)

        async def final(*args, **kwargs):
            result = await current(*args, **kwargs)
            if isinstance(kwargs.get("projection"), module.EditDraftProjection):
                reads.append(True)
                if len(reads) == 2:
                    active[0] = False
            return result

        return final

    monkeypatch.setattr(module, "prepare_attention", retire_at_final)
    with pytest.raises(ValueError, match="publication-stale"):
        await plans.open(e.link, control.id, query_id="retired-edit-copy", chat_id=e.link.chat_id, message_id=control.message_id, message_thread_id=None, guard=guard)
    assert not active[0] and len(reads) == 2
    async with e.db.sessions() as reader:
        assert await reader.scalar(select(func.count()).select_from(TelegramActRow).where(TelegramActRow.kind == "edit", TelegramActRow.work_id == e.work["id"])) == 0
