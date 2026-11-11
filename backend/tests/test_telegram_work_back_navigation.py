"""Back restores complete request choices without creating a Work action."""

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_work_control_admission import review_confirmation
from test_telegram_work_interaction_delivery import open_view, selected
from test_telegram_work_preview_recovery import dispatch, install
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_telegram_work_typing_prompts import clear_old
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramOutboxRow


@pytest.mark.asyncio
@pytest.mark.parametrize("purpose", ["information", "review"])
async def test_back_restores_request_controls_after_complete_ack(work_delivery, purpose):
    e = work_delivery
    if purpose == "review":
        await review_confirmation(e)
    else:
        install(e)
        await dispatch(e, 801)
        e.identity = await open_view(e, await selected(e, "hi_cant_open"))
        await clear_old(e)
        await dispatch(e, 802)
    identity = await open_view(e, await selected(e, "back"))
    await clear_old(e)
    await dispatch(e, 803)
    e.identity = identity
    assert await selected(e, "hi_answer" if purpose == "information" else "accept_open")
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, identity)).state == "sent"
        assert not list(await reader.scalars(select(TelegramActRow)))
