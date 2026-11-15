"""Fresh acknowledged management gestures explain current permission loss."""

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_callback_commands import press
from test_telegram_work_chat_management_runtime import open_detail
from test_telegram_work_chat_runtime import installed_work_runtime
from test_telegram_work_interaction_delivery import selected
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.callbacks import _row
from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.render import render
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
from deerflow.persistence.agent_instances.work import AgentWorkRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow

MANAGEMENT_KINDS = [("work_change", "denied.change"), ("work_withdraw", "denied.withdraw")]
MANAGEMENT_LOSSES = ["manage", "inspect", "capability"]


@pytest.mark.asyncio
@pytest.mark.parametrize("kind,key", MANAGEMENT_KINDS)
@pytest.mark.parametrize("loss", MANAGEMENT_LOSSES)
async def test_management_press_reports_lost_manage(work_delivery, monkeypatch, kind, key, loss):
    e = work_delivery
    owner = await installed_work_runtime(e)
    try:
        await open_detail(e)
        button = await selected(e, kind)
        async with e.db.sessions.begin() as writer:
            (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 3 if loss == "manage" else 1 if loss == "inspect" else 7
            revision = (await writer.get(AgentWorkRow, e.work["id"])).revision
        if loss == "capability":
            from deerflow.config.authorization_config import AuthorizationConfig

            policy = {"routes": {"allow": "*", "deny": ["agents:write"]}}
            monkeypatch.setattr(
                e.live.config, "authorization", AuthorizationConfig.model_validate({"enabled": True, "provider": {"use": "deerflow.authz.rbac:RbacAuthorizationProvider", "config": {"roles": {"admin": policy, "user": policy}}}})
            )
        before = len(e.channel.bot.request.calls)
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(press(e, 6975, button.id, message_id=button.message_id))
        await e.channel._drain_ingress()
        await dispatch(e, 6976)
        sent = [call.parameters["text"] for call in e.channel.bot.request.calls[before:] if call.endpoint == "sendMessage"]
        assert render(key, "en-US") in sent
        async with e.db.sessions() as reader:
            assert await reader.get(TelegramActRow, button.id) is None
            assert (await reader.get(AgentWorkRow, e.work["id"])).revision == revision
    finally:
        await owner.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("surviving", ["opening", "gesture", "draft"])
async def test_management_denial_preserves_each_real_opening_owner(work_delivery, surviving):
    e = work_delivery
    owner = await installed_work_runtime(e)
    try:
        await open_detail(e)
        button = await selected(e, "work_change")
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(press(e, 6975, button.id, message_id=button.message_id))
        await e.channel._drain_ingress()
        async with e.db.sessions.begin() as writer:
            opening = await writer.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_management_intent"]["origin"]["callback_id"].as_string() == button.id))
            assert opening is not None
            intent = opening.payload["work_management_intent"]
            gesture = await writer.get(TelegramCallbackRow, intent["origin"]["gesture_id"])
            draft = await writer.get(TelegramActRow, intent["draft_id"])
            assert gesture is not None and draft is not None
            if surviving != "opening":
                await writer.delete(opening)
            if surviving != "gesture":
                await writer.delete(gesture)
            if surviving != "draft":
                await writer.delete(draft)
            (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 3
        async with e.db.sessions() as reader:
            actual = _row(await reader.get(TelegramCallbackRow, button.id))
        with pytest.raises(ProductSourceRetired):
            await owner.host.work_callback_denials.offer(e.link, actual, guard=owner.chat.guard(e.channel, e.link), query_id="query-6975")
        async with e.db.sessions() as reader:
            assert await reader.scalar(select(TelegramOutboxRow.id).where(TelegramOutboxRow.payload["work_callback_denial"]["callback"]["id"].as_string() == button.id)) is None
            assert await reader.get(TelegramActRow, button.id) is None
    finally:
        await owner.stop()
