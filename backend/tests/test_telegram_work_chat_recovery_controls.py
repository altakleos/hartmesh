"""Installed recovery controls keep the original execution's saved body."""

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_callback_commands import press
from test_telegram_notification_runtime import runtime
from test_telegram_work_execution_outcomes import busy_notice, unknown_notice
from test_telegram_work_interaction_delivery import selected
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.telegram_bots.model import TelegramActRow


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["cancel_wait", "act_check"])
async def test_installed_execution_recovery_control_uses_original_journal(work_delivery, monkeypatch, kind):
    e = work_delivery
    if kind == "cancel_wait":
        _plans, original = await busy_notice(e, monkeypatch)
        restore = None
    else:
        _plans, original, restore = await unknown_notice(e, monkeypatch)
    owner = await runtime(e, preserve=True)
    try:
        await dispatch(e, 6261)
        button = await selected(e, kind)
        assert button is not None and button.target_id == original.id
        if restore is not None:
            monkeypatch.setattr("app.gateway.work_execution.activate_work", restore)
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        update = press(e, 6262, button.id, message_id=6261)
        assert await e.channel.handle_update(update)
        await e.channel._drain_ingress()
        async with e.db.sessions() as reader:
            current = await reader.get(TelegramActRow, original.id)
            assert current.body == original.body
            if kind == "cancel_wait":
                assert current.cancel_requested_at is not None
                assert not e.launches
            else:
                assert current.state == "done" and len(e.launches) == 1
        assert await e.channel.handle_update(update)
        await e.channel._drain_ingress()
        assert len(e.launches) == (0 if kind == "cancel_wait" else 1)
    finally:
        await owner.stop()
