"""Execution previews keep their genuine source owner during request navigation.

The launcher and submitted outcome are journal fixtures, not worker qualification.
"""

from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_agent_work_records import submit_fixture
from test_telegram_callback_commands import press
from test_telegram_notification_runtime import runtime
from test_telegram_work_interaction_delivery import selected
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_telegram_work_start_controls import stage, start_control
from test_work_attention_pages import work_pages as work_pages

from deerflow.agent_instances.contract import Refusal
from deerflow.agent_instances.human_input_contract import CreateRequest
from deerflow.persistence.telegram_bots.model import TelegramOutboxRow


@pytest.mark.asyncio
@pytest.mark.parametrize("purpose", ["information", "review"])
async def test_execution_full_preview_opens_current_request_using_its_real_carrier(work_delivery, monkeypatch, purpose):
    e = work_delivery
    _views, controller, button = await start_control(e, monkeypatch)
    act = await stage(e, controller, button)
    monkeypatch.setattr("app.gateway.work_execution.activate_work", AsyncMock(side_effect=Refusal("Workspace restarting", reason="restarting")))
    assert (await e.acts.check(act.id)).state == "refused"
    if purpose == "review":
        await submit_fixture(e.db.sessions, e.work)
    current = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=e.work["id"])
    request = await e.human.create(
        actor=e.actor,
        instance_id=e.instance.id,
        work_id=e.work["id"],
        request=CreateRequest(
            operation_id=uuid4().hex,
            expected_work_revision=current["revision"],
            expected_assignment_revision=current["assignment_revision"],
            purpose=purpose,
            question="Check the current guide",
            reason="A human check is required",
            expected_response="Answer or review",
            recipient_id=e.link.owner_user_id,
        ),
    )
    owner = await runtime(e, preserve=True)
    try:
        e.identity = await owner.chat.executions.observe(e.link, act.id, guard=owner.chat.guard(e.channel, e.link))
        await dispatch(e, 6611)
        assert not e.dispatch_errors, e.dispatch_errors
        async with e.db.sessions() as reader:
            card = await reader.get(TelegramOutboxRow, e.identity)
            assert card.payload["work_news_basis"]["source"]["work"]["notice"]["preview"] is True
        button = await selected(e, "accept_open" if purpose == "review" else "digest_item")
        assert button is not None and button.target_id == request["id"]
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(press(e, 6612, button.id, message_id=6611))
        await e.channel._drain_ingress()
        async with e.db.sessions() as reader:
            previews = list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.request_id == request["id"], TelegramOutboxRow.class_ == "reply")))
            assert len(previews) == 1 and previews[0].parts
            e.identity = previews[0].id
        if purpose == "review":
            e.channel.bot.request.enqueue("editMessageReplyMarkup", result=True)
            e.clock.advance(10)
            assert await e.sender.tick() == 1
            await e.sender.drain()
        await dispatch(e, 6613)
        assert not e.dispatch_errors, e.dispatch_errors
        async with e.db.sessions() as reader:
            assert (await reader.get(TelegramOutboxRow, e.identity)).state == "sent"
        assert not e.launches
    finally:
        await owner.stop()
