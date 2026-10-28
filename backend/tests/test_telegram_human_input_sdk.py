"""Real SQL and recorded SDK UI over explicitly seeded remote source proofs."""

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
import pytest_asyncio
from _telegram_human_input_test_support import request, source_basis
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_bot_handlers import message, sent
from test_telegram_callback_commands import drain, press, setup
from test_telegram_publication_observation import ACK, run_row
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging

from app.channels.message_bus import MessageBus
from app.telegram_bots.forms import Forms
from app.telegram_bots.gateway_client import GatewayClientOwner
from app.telegram_bots.human_input import HumanInputs
from app.telegram_bots.human_input_source import HumanInputSource
from app.telegram_bots.model_source import ModelSource
from app.telegram_bots.producer import DeliveryEligibility, OutboxSender
from app.telegram_bots.publication import PublicationCoordinator
from app.telegram_bots.publication_authority import PublicationAttemptAuthority
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow


@pytest_asyncio.fixture
async def ui(db, monkeypatch):
    e = await setup(db, monkeypatch)
    e.seed = None

    async def respond(incoming):
        if incoming.url.path == "/api/threads/request-thread":
            return httpx.Response(200, json={"thread_id": "request-thread", "owner_user_id": e.link.owner_user_id, "agent_instance": None, "metadata": {"channel_agent_name": "lead_agent"}})
        if incoming.url.path.endswith("/output"):
            return httpx.Response(200, headers=ACK, json=e.seed["projection"])
        if incoming.url.path.endswith("/messages"):
            return httpx.Response(200, headers=ACK, json={"data": e.seed["messages"], "has_more": False})
        return httpx.Response(200, headers=ACK, json={**run_row(e.record), "metadata": {"channel_turn_id": e.record.publication_id}, "status": "success"})

    http = GatewayClientOwner(gateway_url="http://gateway.test", langgraph_url="http://gateway.test/api", transport=httpx.MockTransport(respond))
    host = PublicationAttemptAuthority(links=db.links, authority=e.live.authority, client=http, channel=e.services.channel, create_allowed=e.services.send_allowed, read_allowed=e.services.send_allowed)
    coordinator = PublicationCoordinator(inbox=e.services.outbox.inbox, outbox=e.services.outbox, bus=MessageBus(), http_owner=http, capture_attempt=host.capture)
    e.services.thread_client, e.services.publications, e.services.publication_authority = http, coordinator, host
    reader = HumanInputSource(coordinator=coordinator, http_owner=http, capture_attempt=host.capture)
    e.services.model_source = ModelSource(read_output=coordinator.read_output, human_input_source=reader)
    e.services.forms = Forms(e.services.callbacks)
    e.services.turns = SimpleNamespace(accept_staged=AsyncMock(return_value=True), retire=AsyncMock(), message_sent=lambda *args, **kwargs: None)
    e.services.human_inputs = HumanInputs(e.services, source=reader)
    base_eligibility = e.services.eligibility

    async def eligibility(link, item, part=None):
        if item.payload.get("reply_basis", {}).get("version") == 5 and not await e.services.human_inputs.step_current(item):
            return DeliveryEligibility.skip("superseded")
        return await base_eligibility(link, item, part)

    await e.sender.stop()
    e.services.eligibility = eligibility
    e.sender = OutboxSender(e.services.outbox, run_id="human-input-ui", deliver_part=lambda **kw: e.channel.send_part(**{key: value for key, value in kw.items() if key != "link_id"}), eligibility=eligibility)

    async def offer(raw):
        origin, _ = await e.services.outbox.inbox.accept(e.link, message(e, 1401, content="Ask a question"), sanitized_input={"type": "text", "text": "Ask a question"})
        origin = await e.services.outbox.inbox.route(origin, kind="turn", target_id="lead_agent", basis={"input_profile": {"version": 1, "kind": "plain_text", "forwarded": False}}, operation_ids=[])
        e.record = await coordinator.dispatch(e.link, [origin], thread_id="request-thread")
        e.seed = source_basis(replace(e.record, run_id="coordinator-run"), payload=raw)
        identity = await e.services.human_inputs.plan_terminal(e.record, e.link)
        assert identity is not None
        return identity

    e.offer = offer
    try:
        yield e
    finally:
        await e.sender.stop()
        await coordinator.stop()
        await http.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_free_text_question_uses_actual_sdk_force_reply_without_a_prompt_row(ui):
    e = ui
    await e.offer(request(input_mode="free_text", options=[]))
    e.request.enqueue("sendMessage", result=sent(e, identity=701))
    await drain(e)
    call = e.request.calls[-1]
    assert call.endpoint == "sendMessage" and call.parameters["reply_markup"]["force_reply"] is True
    assert call.parameters["reply_parameters"]["message_id"] == 1401
    async with e.services.outbox.sessions() as session:
        assert not list(await session.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "prompt")))


@pytest.mark.asyncio
async def test_choice_card_saved_label_and_value_reach_the_actual_sdk_and_staged_answer(ui):
    e = ui
    await e.offer(request())
    e.request.enqueue("sendMessage", result=sent(e, identity=701))
    await drain(e)
    keyboard = e.request.calls[-1].parameters["reply_markup"]["inline_keyboard"]
    assert keyboard[0][0]["text"] == "First"
    callback = keyboard[0][0]["callback_data"].removeprefix("h1:")
    e.request.enqueue("answerCallbackQuery")
    e.request.enqueue("editMessageText", result=sent(e, identity=701))
    assert await e.channel.handle_update(press(e, 1402, callback, message_id=701))
    await drain(e)
    e.services.turns.accept_staged.assert_awaited_once()
    stage = e.services.turns.accept_staged.await_args.args[1]
    assert stage.text == "First" and stage.thread_id == "request-thread"
    assert stage.human_message_kwargs["human_input_response"]["option_id"] == "option-1"
    edits = [call for call in e.request.calls if call.endpoint == "editMessageText"]
    assert edits and "Your answer: First" in edits[-1].parameters["text"]


@pytest.mark.asyncio
async def test_first_required_form_field_and_prompt_are_committed_before_sdk_delivery(ui):
    e = ui
    raw = request(version=2, input_mode="form", fields=[{"name": "value", "label": "A value", "type": "text", "required": True}])
    raw.pop("options")
    await e.offer(raw)
    e.request.enqueue("sendMessage", result=sent(e, identity=701))
    await drain(e)
    assert "1 of 1: A value" in e.request.calls[-1].parameters["text"]
    assert e.request.calls[-1].parameters["reply_markup"]["force_reply"] is True
    async with e.services.outbox.sessions() as session:
        rows = list(await session.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.kind.in_(["form", "prompt"]))))
        assert {row.kind for row in rows} == {"form", "prompt"}
        prompt = next(row for row in rows if row.kind == "prompt")
        assert prompt.payload["purpose"] == "form_field" and prompt.payload["anchor_outbox_id"]


@pytest.mark.asyncio
async def test_retired_question_after_last_keyboard_read_cannot_send_through_actual_limiter(ui, monkeypatch):
    import asyncio

    e = ui
    identity = await e.offer(request())
    entered, release = asyncio.Event(), asyncio.Event()
    actual, calls = e.services.callbacks.read_keyboard, []

    async def held(captured):
        result = await actual(captured)
        calls.append(captured)
        if len(calls) == 3:
            entered.set()
            await release.wait()
        return result

    monkeypatch.setattr(e.services.callbacks, "read_keyboard", held)
    e.request.enqueue("sendMessage", result=sent(e, identity=701))
    try:
        e.clock.advance(2)
        await e.sender.tick()
        await asyncio.wait_for(entered.wait(), 8)
        item = await e.services.outbox.get(e.link, identity)
        attempt = await e.services.publication_authority.capture(e.link, thread_id="request-thread", endpoint_id=e.services.thread_client.endpoint_id)
        await e.services.callbacks.consume_group(item.parts[-1]["callback_group_id"], link=e.link, guard=attempt.read_guard)
        release.set()
        await e.sender.drain()
        assert not any(call.endpoint == "sendMessage" for call in e.request.calls)
        e.clock.advance(2)
        await e.sender.tick()
        await e.sender.drain()
        async with e.services.outbox.sessions() as session:
            from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

            saved = await session.get(TelegramOutboxRow, identity)
            assert saved.state == "skipped" and saved.skip_reason == "superseded"
    finally:
        release.set()
