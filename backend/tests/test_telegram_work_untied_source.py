"""Ambiguous Work answers require the latest complete presentation in scope."""

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_inbox import incoming
from test_telegram_work_card_inputs import card
from test_telegram_work_interaction_delivery import guard_for
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow, TelegramOutboxRow


async def untied(e, *, message=900):
    return (await e.plans.outbox.inbox.accept(e.link, incoming(message=message, update_id=1900 + message, account=e.link.chat_id), sanitized_input={"type": "text", "text": "A possible answer"}))[0]


@pytest.mark.asyncio
@pytest.mark.parametrize("purpose", ["information", "review", "decision"])
async def test_latest_complete_work_question_can_offer_an_explicit_answer_choice(work_delivery, purpose):
    from app.telegram_bots.work_untied_source import WorkUntiedSource

    e = work_delivery
    request = await card(e, purpose)
    origin = await untied(e)
    source = await WorkUntiedSource(e.plans).find(e.link, origin, guard=await guard_for(e))
    assert source is not None and source.outbox_id == e.identity
    assert source.source.request["id"] == request["id"]
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, origin.message_id, e.link.chat_id))
        assert mark.route_kind is None and mark.operation_ids == []


@pytest.mark.asyncio
@pytest.mark.parametrize("later", ["bot", "human"])
async def test_intervening_message_prevents_ambiguous_question_capture(work_delivery, later):
    from app.telegram_bots.work_untied_source import WorkUntiedSource

    e = work_delivery
    await card(e, "information")
    if later == "human":
        earlier = await untied(e, message=850)
        await e.plans.outbox.inbox.route(earlier, kind="turn", target_id=e.link.agent_ref, basis={"input_profile": {"version": 1, "kind": "plain_text", "forwarded": False}}, operation_ids=[])
    else:
        e.identity = await e.plans.outbox.plan_product_keyboard(
            e.link,
            source_id="another-message",
            parts=[{"kind": "text", "source_ref": {"key": "prompt.discarded", "values": {}}}],
            keyboard=[],
            callbacks=e.plans.callbacks,
            guard=await guard_for(e),
        )
        await dispatch(e, 850)
    origin = await untied(e)
    assert await WorkUntiedSource(e.plans).find(e.link, origin, guard=await guard_for(e)) is None


@pytest.mark.asyncio
async def test_unacknowledged_or_newer_question_never_claims_earlier_text(work_delivery):
    from app.telegram_bots.work_untied_source import WorkUntiedSource

    e = work_delivery
    await card(e, "information")
    origin = await untied(e)
    async with e.db.sessions.begin() as writer:
        parent = await writer.get(TelegramOutboxRow, e.identity)
        parent.parts = [{**part, "message_ids": [901]} for part in parent.parts]
        parent.message_id = 901
    assert await WorkUntiedSource(e.plans).find(e.link, origin, guard=await guard_for(e)) is None


@pytest.mark.asyncio
async def test_locator_scans_past_a_full_page_of_unsent_history(work_delivery):
    from copy import deepcopy

    from app.telegram_bots.work_untied_source import WorkUntiedSource

    e = work_delivery
    await card(e, "information")
    async with e.db.sessions.begin() as writer:
        parent = await writer.get(TelegramOutboxRow, e.identity)
        fields = {column.key: deepcopy(getattr(parent, column.key)) for column in TelegramOutboxRow.__mapper__.column_attrs if column.key not in {"id", "dedupe_key", "parts", "state"}}
        for index in range(105):
            writer.add(TelegramOutboxRow(**fields, id=f"{index:032x}", dedupe_key=f"unsent-history:{index}", state="pending", parts=[]))
    origin = await untied(e)
    found = await WorkUntiedSource(e.plans).find(e.link, origin, guard=await guard_for(e))
    assert found is not None and found.outbox_id == e.identity


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["topic", "generation", "incomplete", "concealed", "spent", "expired"])
async def test_ineligible_question_does_not_offer_an_answer(work_delivery, change):
    from datetime import timedelta

    from app.telegram_bots.work_untied_source import WorkUntiedSource
    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow

    e = work_delivery
    await card(e, "information", long=True)
    async with e.db.sessions.begin() as writer:
        parent = await writer.get(TelegramOutboxRow, e.identity)
        if change == "topic":
            parent.message_thread_id = 17
        elif change == "generation":
            parent.binding_generation += 1
        elif change == "incomplete":
            parent.parts = [{**part, "state": "pending", "message_ids": []} if index == 0 else part for index, part in enumerate(parent.parts)]
            parent.state = "pending"
        elif change == "concealed":
            parent.payload = {**parent.payload, "work_request_basis": {**parent.payload["work_request_basis"], "concealed": True}}
        else:
            controls = list(await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == parent.parts[-1]["callback_group_id"])))
            for control in controls:
                if change == "spent":
                    control.consumed_at, control.closed_reason = e.clock(), "command"
                else:
                    control.created_at = e.clock() - timedelta(days=181)
    origin = await untied(e)
    assert await WorkUntiedSource(e.plans).find(e.link, origin, guard=await guard_for(e)) is None
