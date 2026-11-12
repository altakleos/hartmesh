"""The actual Telegram text route preserves explicit Work-card destinations."""

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_inbox import incoming
from test_telegram_work_card_inputs import card
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_telegram_work_typed_input import install_inputs
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.inbox import InboxRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramInboundMarkRow, TelegramOutboxRow


@pytest.mark.asyncio
@pytest.mark.parametrize("purpose", ["information", "decision", "review"])
async def test_actual_quoted_card_text_saves_one_answer_or_comment(work_delivery, purpose):
    e = work_delivery
    request = await card(e, purpose)
    from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow

    async with e.db.sessions.begin() as writer:
        for attempt in await writer.scalars(select(AgentWorkAttemptRow).where(AgentWorkAttemptRow.work_id == request["work_id"])):
            attempt.request = {"expected_revision": 1}
    _plans, inputs = install_inputs(e)
    update = incoming(message=900, update_id=2900, account=e.link.chat_id, reply=801)
    assert await e.channel.handle_update(update)
    await e.channel._drain_ingress()
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        assert mark is not None and mark.route_kind == "work_input"
        assert mark.route_basis["work_input"]["version"] == 2
        actions = list(await reader.scalars(select(TelegramActRow)))
        assert len(actions) == 1 and actions[0].kind == "respond" and actions[0].state == "done"
        assert actions[0].request_id == request["id"]
        assert actions[0].body["text"] == update.message.text
        origin = InboxRow.from_row(mark)
    assert await inputs.handle_text(e.channel, update, captured_link=e.link)
    assert await inputs.resume(e.channel, origin)
    responses = (await e.human.responses(actor=e.actor, request_id=request["id"]))["responses"]
    assert len(responses) == 1
    async with e.db.sessions() as reader:
        assert len(list(await reader.scalars(select(TelegramActRow)))) == 1
        outcome = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_outcome_basis"]["context"]["act"]["id"].as_string() == actions[0].id))
        assert outcome is not None
        expected = "Saved as your answer" if purpose == "information" else "Added as a comment"
        assert expected in "".join(part["html"] for part in outcome.parts)
        e.identity = outcome.id
    from test_telegram_work_input_refresh import deliver_refreshed

    await deliver_refreshed(e, 802)
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramInboundMarkRow, (origin.link_id, origin.message_id, origin.chat_id))).route_complete


@pytest.mark.asyncio
async def test_unknown_card_response_replay_never_submits_again(work_delivery, monkeypatch):
    from deerflow.agent_instances.human_input import HumanInput

    e = work_delivery
    await card(e, "information")
    _plans, inputs = install_inputs(e)
    calls = []

    async def unknown(*args, **kwargs):
        calls.append(kwargs["request"].operation_id)
        raise ConnectionError("Unknown response outcome")

    monkeypatch.setattr(HumanInput, "respond", unknown)
    update = incoming(message=900, update_id=2900, account=e.link.chat_id, reply=801)
    assert await e.channel.handle_update(update)
    await e.channel._drain_ingress()
    async with e.db.sessions() as reader:
        mark = InboxRow.from_row(await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id)))
        saved = await reader.get(TelegramActRow, mark.operation_ids[0])
        assert saved.state == "pending" and saved.attempts == 1 and saved.reason is None
    assert await inputs.handle_text(e.channel, update, captured_link=e.link)
    assert await inputs.resume(e.channel, mark)
    assert calls == [mark.operation_ids[0]]


@pytest.mark.asyncio
async def test_spent_card_reply_is_held_and_never_becomes_ordinary_text(work_delivery):
    e = work_delivery
    await card(e, "information")
    _plans, inputs = install_inputs(e)
    async with e.db.sessions.begin() as writer:
        from deerflow.persistence.telegram_bots.model import TelegramCallbackRow

        parent = await writer.get(TelegramOutboxRow, e.identity)
        for row in await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == parent.parts[-1]["callback_group_id"])):
            row.consumed_at, row.closed_reason = e.clock(), "command"
    update = incoming(message=900, update_id=2900, account=e.link.chat_id, reply=801)
    assert await e.channel.handle_update(update)
    await e.channel._drain_ingress()
    async with e.db.sessions() as reader:
        origin = InboxRow.from_row(await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id)))
        assert origin.route_kind == "work_card_hold" and not origin.route_complete
        assert origin.route_basis["source_outbox_id"] == e.identity
        assert not list(await reader.scalars(select(TelegramActRow)))
    assert await inputs.handle_text(e.channel, update, captured_link=e.link)
    assert await inputs.resume(e.channel, origin)


@pytest.mark.asyncio
async def test_card_reply_can_quote_first_part_of_complete_preview(work_delivery):
    e = work_delivery
    request = await card(e, "information", long=True)
    install_inputs(e)
    assert await e.channel.handle_update(incoming(message=900, update_id=2900, account=e.link.chat_id, reply=801))
    await e.channel._drain_ingress()
    responses = (await e.human.responses(actor=e.actor, request_id=request["id"]))["responses"]
    assert len(responses) == 1


@pytest.mark.asyncio
async def test_existing_ordinary_route_cannot_be_reinterpreted_as_card_reply(work_delivery):
    e = work_delivery
    await card(e, "information")
    _plans, inputs = install_inputs(e)
    update = incoming(message=900, update_id=2900, account=e.link.chat_id, reply=801)
    mark, _ = await inputs.inbox.accept(e.link, update, sanitized_input={"type": "text", "text": update.message.text})
    await inputs.inbox.route(mark, kind="turn", target_id=e.link.agent_ref, basis={"input_profile": {"version": 1, "kind": "plain_text", "forwarded": False}}, operation_ids=[])
    assert await inputs.handle_text(e.channel, update, captured_link=e.link) is False
    async with e.db.sessions() as reader:
        assert not list(await reader.scalars(select(TelegramActRow)))
        assert (await reader.get(TelegramInboundMarkRow, (mark.link_id, mark.message_id, mark.chat_id))).route_kind == "turn"


@pytest.mark.asyncio
async def test_reply_to_closed_work_prompt_stays_work_owned(work_delivery):
    from test_telegram_work_input_capture import prompt

    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow

    e = work_delivery
    _request, identity, _seen = await prompt(e)
    install_inputs(e)
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramCallbackRow, identity)
        row.consumed_at, row.closed_reason = e.clock(), "command"
    assert await e.channel.handle_update(incoming(message=900, update_id=2900, account=e.link.chat_id, reply=802))
    await e.channel._drain_ingress()
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        assert mark is not None and mark.route_kind == "work_card_hold"
        assert not list(await reader.scalars(select(TelegramActRow)))


@pytest.mark.asyncio
async def test_input_owned_card_reply_has_a_separate_comment_owner(work_delivery, monkeypatch):
    from test_telegram_work_retained_drafts import draft

    e = work_delivery
    _request, _inbox, _origin = await draft(e, monkeypatch)
    install_inputs(e)
    assert await e.channel.handle_update(incoming(message=901, update_id=2901, account=e.link.chat_id, reply=803))
    await e.channel._drain_ingress()
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 901, e.link.chat_id))
        assert mark is not None and mark.route_kind == "work_input" and mark.route_basis["work_input"]["purpose"] == "comment"
        actions = list(await reader.scalars(select(TelegramActRow)))
        assert len(actions) == 1 and actions[0].kind == "respond" and actions[0].state == "done"


@pytest.mark.asyncio
async def test_reply_to_first_part_of_work_prompt_uses_that_prompt(work_delivery):
    from datetime import timedelta

    from test_telegram_work_interaction_delivery import open_view, selected
    from test_telegram_work_preview_recovery import dispatch
    from test_telegram_work_typing_prompts import clear_old

    e = work_delivery
    request = await card(e, "information", long=True)
    e.identity = await open_view(e, await selected(e, "hi_answer"))
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, e.identity)
        count, deadline = len(row.parts), row.not_before
        row.not_before = e.clock() + timedelta(hours=1)
    assert count > 1
    await clear_old(e)
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramOutboxRow, e.identity)).not_before = deadline
    for index in range(count):
        await dispatch(e, 803 + index)
    install_inputs(e)
    assert await e.channel.handle_update(incoming(message=900, update_id=2900, account=e.link.chat_id, reply=803))
    await e.channel._drain_ingress()
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        assert mark is not None and mark.route_kind == "work_input" and mark.route_basis["work_input"]["version"] == 1
    assert len((await e.human.responses(actor=e.actor, request_id=request["id"]))["responses"]) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("timing", ["before_dispatch", "before_ack"])
async def test_card_reply_uses_presentation_dispatched_before_accepted_input(work_delivery, monkeypatch, timing):
    from datetime import datetime, timedelta

    e = work_delivery
    request = await card(e, "information")
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, e.identity)
        sent = datetime.fromisoformat(row.parts[0]["sent_at"])
        dispatched = sent - timedelta(seconds=2)
        row.parts = [{**part, "dispatched_at": dispatched.isoformat()} for part in row.parts]
    accepted_at = dispatched + timedelta(seconds=-1 if timing == "before_dispatch" else 1)
    monkeypatch.setattr("app.telegram_bots.inbox._now", lambda: accepted_at)
    install_inputs(e)
    assert await e.channel.handle_update(incoming(message=900, update_id=2900, account=e.link.chat_id, reply=801))
    await e.channel._drain_ingress()
    responses = (await e.human.responses(actor=e.actor, request_id=request["id"]))["responses"]
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        if timing == "before_dispatch":
            assert mark.route_kind == "work_card_hold"
            assert not responses
            assert not list(await reader.scalars(select(TelegramActRow)))
        else:
            assert mark.route_kind == "work_input"
            assert len(responses) == 1


@pytest.mark.asyncio
async def test_prompt_closing_during_card_delegation_keeps_reply_owned(work_delivery, monkeypatch):
    from test_telegram_work_input_capture import prompt

    from app.telegram_bots.callbacks import Prompts
    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow

    e = work_delivery
    _request, identity, _seen = await prompt(e)
    install_inputs(e)
    original, reached = Prompts.current, []

    async def closing(self, *args, **kwargs):
        result = await original(self, *args, **kwargs)
        if result is not None and not reached:
            row = await kwargs["session"].get(TelegramCallbackRow, identity)
            row.consumed_at, row.closed_reason = e.clock(), "command"
            await kwargs["session"].flush()
            reached.append(True)
        return result

    monkeypatch.setattr(Prompts, "current", closing)
    assert await e.channel.handle_update(incoming(message=900, update_id=2900, account=e.link.chat_id, reply=802))
    await e.channel._drain_ingress()
    assert reached
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        assert mark is not None and mark.route_kind == "work_card_hold"
        assert not list(await reader.scalars(select(TelegramActRow)))


@pytest.mark.asyncio
async def test_card_collected_after_identification_keeps_reply_owned(work_delivery, monkeypatch):
    e = work_delivery
    await card(e, "information")
    _plans, inputs = install_inputs(e)
    original = inputs.cards.find

    async def collected(*args, **kwargs):
        result = await original(*args, **kwargs)
        async with e.db.sessions.begin() as writer:
            await writer.delete(await writer.get(TelegramOutboxRow, result))
        return result

    monkeypatch.setattr(inputs.cards, "find", collected)
    assert await e.channel.handle_update(incoming(message=900, update_id=2900, account=e.link.chat_id, reply=801))
    await e.channel._drain_ingress()
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        assert mark is not None and mark.route_kind == "work_card_hold"
        assert not list(await reader.scalars(select(TelegramActRow)))


@pytest.mark.asyncio
@pytest.mark.parametrize("timing", ["before_dispatch", "before_ack"])
async def test_prompt_reply_uses_presentation_dispatched_before_accepted_input(work_delivery, monkeypatch, timing):
    from datetime import datetime, timedelta

    from test_telegram_work_input_capture import prompt

    e = work_delivery
    await prompt(e)
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, e.identity)
        sent = datetime.fromisoformat(row.parts[0]["sent_at"])
        dispatched = sent - timedelta(seconds=2)
        row.parts = [{**part, "dispatched_at": dispatched.isoformat()} for part in row.parts]
    accepted_at = dispatched + timedelta(seconds=-1 if timing == "before_dispatch" else 1)
    monkeypatch.setattr("app.telegram_bots.inbox._now", lambda: accepted_at)
    install_inputs(e)
    assert await e.channel.handle_update(incoming(message=900, update_id=2900, account=e.link.chat_id, reply=802))
    await e.channel._drain_ingress()
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        if timing == "before_dispatch":
            assert mark.route_kind == "work_card_hold"
            assert not list(await reader.scalars(select(TelegramActRow)))
        else:
            assert mark.route_kind == "work_input"
            assert len(list(await reader.scalars(select(TelegramActRow)))) == 1
