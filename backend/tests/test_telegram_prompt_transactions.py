"""Prompt routing uses actual acknowledged presentation without message adoption."""

from uuid import uuid4

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_callbacks import callbacks_environment as callbacks_environment
from test_telegram_publication_custody import publication_environment as publication_environment

from app.telegram_bots.callbacks import Prompts
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow


@pytest.mark.asyncio
async def test_prompt_and_required_presentation_share_rollback(callbacks_environment):
    e = callbacks_environment
    prompts, identity = Prompts(e.callbacks), uuid4().hex
    with pytest.raises(RuntimeError, match="required UI failed"):
        async with e.callbacks.writer(e.link, e.guard_factory()) as writer:
            await prompts.open(e.link, "clarify_other", {"thread_id": e.thread_id, "request_id": "request"}, chat_id=e.link.chat_id, message_thread_id=None, message_id=None, prompt_id=identity, guard=e.guard_factory(), session=writer)
            parent = await e.outbox.plan_product_keyboard(
                e.link, source_id="prompt-owned", parts=[{"kind": "text", "source_ref": {"key": "clarify.other_prompt", "values": {}}}], keyboard=[], callbacks=e.callbacks, guard=e.guard_factory(), session=writer
            )
            await prompts.attach(e.link, identity, outbox_id=parent, guard=e.guard_factory(), session=writer)
            raise RuntimeError("required UI failed")
    async with e.sessions() as reader:
        assert await reader.get(TelegramCallbackRow, identity) is None
        assert await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == "notice:prompt-owned")) is None


@pytest.mark.asyncio
async def test_prompt_reply_routes_only_actual_ack_without_adopting_identity(callbacks_environment):
    e = callbacks_environment
    prompts, identity = Prompts(e.callbacks), uuid4().hex
    async with e.callbacks.writer(e.link, e.guard_factory()) as writer:
        assert (
            await prompts.open(e.link, "clarify_other", {"thread_id": e.thread_id, "request_id": "request"}, chat_id=e.link.chat_id, message_thread_id=None, message_id=None, prompt_id=identity, guard=e.guard_factory(), session=writer)
            == identity
        )
        parent = await e.outbox.plan_product_keyboard(
            e.link, source_id="prompt-ack", parts=[{"kind": "text", "source_ref": {"key": "clarify.other_prompt", "values": {}}}], keyboard=[], callbacks=e.callbacks, guard=e.guard_factory(), session=writer
        )
        await prompts.attach(e.link, identity, outbox_id=parent, guard=e.guard_factory(), session=writer)
    assert await e.callbacks.for_message(e.link.id, 1902, chat_id=e.link.chat_id, message_thread_id=None, binding_generation=e.link.binding_generation) is None
    async with e.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, parent)
        row.state = "sent"
        row.parts = [{**part, "state": "sent", "message_ids": [1902]} for part in row.parts]
    route = await e.callbacks.for_message(e.link.id, 1902, chat_id=e.link.chat_id, message_thread_id=None, binding_generation=e.link.binding_generation)
    assert route is not None and route.retired_presentation is False and [row.id for row in route.rows] == [identity]
    assert (await prompts.get(e.link, identity, guard=e.guard_factory())).message_id is None
    assert await e.callbacks.for_message(e.link.id, 1903, chat_id=e.link.chat_id, message_thread_id=None, binding_generation=e.link.binding_generation) is None
    async with e.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, parent)
        row.payload = {**row.payload, "presentation_version": 1}
    retired = await e.callbacks.for_message(e.link.id, 1902, chat_id=e.link.chat_id, message_thread_id=None, binding_generation=e.link.binding_generation)
    assert retired is not None and retired.retired_presentation is True


@pytest.mark.asyncio
async def test_prompt_stable_replay_never_reopens_or_overwrites_a_later_scope_guard(callbacks_environment):
    e = callbacks_environment
    prompts = Prompts(e.callbacks)
    first, second = uuid4().hex, uuid4().hex

    async def open_prompt(identity, topic=None):
        return await prompts.open(e.link, "clarify_other", {"thread_id": e.thread_id, "request_id": "request"}, chat_id=e.link.chat_id, message_thread_id=topic, message_id=None, prompt_id=identity, guard=e.guard_factory())

    await open_prompt(first)
    await open_prompt(second)
    assert await open_prompt(first) == first
    assert (await prompts.current(e.link, e.link.chat_id, None, guard=e.guard_factory())).id == second
    await prompts.close(e.link, e.link.chat_id, None, reason="expired", guard=e.guard_factory())
    assert (await prompts.current(e.link, e.link.chat_id, None, guard=e.guard_factory())).closed_reason == "expired"
    await open_prompt(uuid4().hex, 99)
    assert (await prompts.current(e.link, e.link.chat_id, None, guard=e.guard_factory())).id == second
    await prompts.close(e.link, e.link.chat_id, 99, reason="command", guard=e.guard_factory())
    assert (await prompts.current(e.link, e.link.chat_id, None, guard=e.guard_factory())).id == second


@pytest.mark.asyncio
async def test_prompt_and_controls_with_different_acknowledged_parents_stay_ambiguous(callbacks_environment):
    e = callbacks_environment
    controls = await e.plan(message_id=1904, source="other-controls")
    prompts, identity = Prompts(e.callbacks), uuid4().hex
    async with e.callbacks.writer(e.link, e.guard_factory()) as writer:
        await prompts.open(e.link, "clarify_other", {"thread_id": e.thread_id, "request_id": "request"}, chat_id=e.link.chat_id, message_thread_id=None, message_id=None, prompt_id=identity, guard=e.guard_factory(), session=writer)
        parent = await e.outbox.plan_product_keyboard(
            e.link, source_id="other-prompt", parts=[{"kind": "text", "source_ref": {"key": "clarify.other_prompt", "values": {}}}], keyboard=[], callbacks=e.callbacks, guard=e.guard_factory(), session=writer
        )
        await prompts.attach(e.link, identity, outbox_id=parent, guard=e.guard_factory(), session=writer)
    # Explicit contradictory transport ACK seeds: two parents claim one message.
    async with e.sessions.begin() as writer:
        for outbox_id in (controls.outbox_id, parent):
            row = await writer.get(TelegramOutboxRow, outbox_id)
            row.state = "sent"
            row.parts = [{**part, "state": "sent", "message_ids": [1904]} for part in row.parts]
    route = await e.callbacks.for_message(e.link.id, 1904, chat_id=e.link.chat_id, message_thread_id=None, binding_generation=e.link.binding_generation)
    assert route is not None and route.retired_presentation is True
    assert {row.payload["anchor_outbox_id"] for row in route.rows} == {controls.outbox_id, parent}
