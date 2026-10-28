"""Actual SQL/recorded SDK mixed UI; source/metadata and media ACK are protocol seams."""

import asyncio
from datetime import datetime, timedelta

import pytest
import pytest_asyncio
from _telegram_human_input_test_support import request
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_bot_handlers import message, sent
from test_telegram_callback_commands import drain, press
from test_telegram_human_input_sdk import ui as ui
from test_telegram_mixed_source import PATH, mixed_basis
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging

from app.telegram_bots.files import FileBytesBudget
from app.telegram_bots.gateway_client import ArtifactMetadata
from app.telegram_bots.model_source import ModelSource
from app.telegram_bots.producer import DeliveryEligibility, OutboxSender, SourceUnavailable
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramInboundMarkRow


@pytest_asyncio.fixture
async def mixed_ui(ui, monkeypatch):
    import test_telegram_human_input_sdk as seeds

    e = ui
    # Only the finite remote output/messages are seeded; HumanInputSource still
    # performs its real original-scope HTTP qualification and opaque issuance.
    monkeypatch.setattr(seeds, "source_basis", mixed_basis)
    e.services.file_bytes_budget = FileBytesBudget(50_000_000)
    e.services.callbacks.clock = e.clock.now
    e.services.outbox.clock = e.clock.now
    e.metadata_calls = []

    async def metadata(owner, thread, path, *, before_dispatch):
        await before_dispatch()
        assert (owner, thread, path) == (e.link.owner_user_id, "request-thread", PATH)
        e.metadata_calls.append(path)
        return ArtifactMetadata(path, "report.txt", 4, "text/plain")

    monkeypatch.setattr(e.services.thread_client, "describe_artifact", metadata)
    await e.sender.stop()
    e.media_entered, e.media_release = asyncio.Event(), asyncio.Event()
    e.hold_media = False
    e.media_receipts = []
    e.fail_media = False
    e.services.model_source = ModelSource(read_output=e.services.publications.read_output, human_input_source=e.services.human_inputs.source, outbox=e.services.outbox, http_owner=e.services.thread_client)

    async def eligibility(link, item, part=None):
        if part is not None and item.payload.get("reply_basis", {}).get("version") in {5, 7} and not await e.services.human_inputs.step_current(item, part=part):
            return DeliveryEligibility.skip("superseded")
        return await e.live.authority.delivery(link, item, part)

    async def deliver(**capture):
        values = {key: value for key, value in capture.items() if key != "link_id"}
        saved = await e.services.outbox.capture_sending_part(link_id=e.link.id, token_revision=e.channel.transport_revision, **values)
        if saved.part["kind"] in {"photo", "album", "document"}:
            e.media_entered.set()
            if e.hold_media:
                await e.media_release.wait()
            if e.fail_media:
                e.fail_media = False
                raise SourceUnavailable(source_ref={"key": "file.out_failed", "values": {"file": "report.txt"}})
            identity = 650 + len(e.media_receipts)
            e.media_receipts.append(identity)
            return [identity]
        return await e.channel.send_part(**values)

    e.services.eligibility = eligibility
    e.sender = OutboxSender(e.services.outbox, run_id="mixed-ui-protocol", deliver_part=deliver, eligibility=eligibility)
    try:
        yield e
    finally:
        e.media_release.set()
        await e.sender.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("required", [False, True])
async def test_unseen_form_preserves_plain_intent_and_ttl_begins_at_question_ack(mixed_ui, required):
    e = mixed_ui
    raw = request(version=2, input_mode="form", fields=[{"name": "note", "label": "A note", "type": "text", "required": required}])
    raw.pop("options")
    e.hold_media = True
    identity = await e.offer(raw)
    item = await e.services.outbox.get(e.link, identity)
    assert [part["kind"] for part in item.parts] == ["document", "text"]
    e.clock.advance(2)
    await e.sender.tick()
    await asyncio.wait_for(e.media_entered.wait(), 15)
    # The old wall-time TTL must not expire while the file still owns delivery.
    e.clock.advance(e.services.config.prompts.ttl_seconds + 10)
    await e.services.callbacks.sweep(e.clock.now())
    early = message(e, 2201, content="A new ordinary instruction")
    assert not await e.services.human_inputs.handle_text(e.channel, early)
    async with e.services.outbox.sessions() as reader:
        form = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "form"))
        prompt = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "prompt"))
        assert form.payload["values"] == {} and form.payload["state"] == "field"
        assert prompt.expires_at is None and prompt.consumed_at is None
        assert await reader.get(TelegramInboundMarkRow, (e.link.id, early.message.message_id, e.link.chat_id)) is None
    assert not any(call.endpoint == "sendMessage" for call in e.request.calls)
    e.request.enqueue("sendMessage", result=sent(e, identity=701))
    e.media_release.set()
    await e.sender.drain()
    await drain(e)
    async with e.services.outbox.sessions() as reader:
        prompt = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "prompt"))
        actual_expiry = prompt.expires_at
        if actual_expiry.tzinfo is None:
            actual_expiry = actual_expiry.replace(tzinfo=e.clock.now().tzinfo)
        current = await e.services.outbox.get(e.link, identity, session=reader)
        sent_at = datetime.fromisoformat(current.parts[-1]["sent_at"])
        assert prompt.message_id == 701 and actual_expiry == sent_at + timedelta(seconds=e.services.config.prompts.ttl_seconds)
    assert await e.services.human_inputs.handle_text(e.channel, message(e, 2202, content="The actual answer"))
    async with e.services.outbox.sessions() as reader:
        form = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "form"))
        assert form.payload["values"] == {"note": "The actual answer"} and form.payload["state"] == "review"


async def show_mixed(e, raw, *, failed_file=False):
    e.fail_media = failed_file
    identity = await e.offer(raw)
    if failed_file:
        e.request.enqueue("sendMessage", result=sent(e, identity=690))
    e.request.enqueue("sendMessage", result=sent(e, identity=701))
    for _ in range(3 if failed_file else 2):
        await drain(e)
    return identity


async def saved_button(e, kind):
    async with e.services.outbox.sessions() as reader:
        rows = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == kind, TelegramCallbackRow.consumed_at.is_(None))))
        assert len(rows) == 1
        return await e.services.callbacks.get(rows[0].id)


@pytest.mark.asyncio
async def test_media_warning_controls_do_not_keep_consumed_question_current(mixed_ui):
    e = mixed_ui
    identity = await show_mixed(e, request(), failed_file=True)
    question = await saved_button(e, "clarify_option")
    attempt = await e.services.publication_authority.capture(e.link, thread_id="request-thread", endpoint_id=e.services.thread_client.endpoint_id)
    await e.services.callbacks.consume_group(question.group_id, link=e.link, guard=attempt.read_guard)
    item = await e.services.outbox.get(e.link, identity)
    assert (await saved_button(e, "file_retry")).consumed_at is None
    assert not await e.services.human_inputs.step_current(item)
    for part in item.parts:
        if part["source_ref"].get("key") != "human_input.presentation":
            assert await e.services.human_inputs.step_current(item, part=part)


@pytest.mark.asyncio
async def test_question_other_after_media_retry_keeps_original_card_and_prompt_anchor(mixed_ui):
    e = mixed_ui
    identity = await show_mixed(e, request(), failed_file=True)
    retry = await saved_button(e, "file_retry")
    attempt = await e.services.publication_authority.capture(e.link, thread_id="request-thread", endpoint_id=e.services.thread_client.endpoint_id)
    assert await e.services.model_source.retry_file_callback(e.link, retry, callbacks=e.services.callbacks, guard=attempt.read_guard)
    e.request.enqueue("editMessageReplyMarkup", result=sent(e, identity=690))
    await drain(e)
    await drain(e)
    current = await e.services.outbox.get(e.link, identity)
    assert current.parts[-1]["message_ids"] == [e.media_receipts[-1]]
    assert e.media_receipts[-1] != 701
    other = await saved_button(e, "clarify_other")
    assert other.message_id == 701
    e.request.enqueue("answerCallbackQuery")
    e.request.enqueue("editMessageReplyMarkup", result=sent(e, identity=701))
    e.request.enqueue("sendMessage", result=sent(e, identity=702))
    assert await e.channel.handle_update(press(e, 2301, other.id, message_id=701))
    await drain(e)
    await drain(e)
    cleared = [call.parameters["message_id"] for call in e.request.calls if call.endpoint == "editMessageReplyMarkup"]
    assert cleared == [690, 701]
    async with e.services.outbox.sessions() as reader:
        prompt = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "prompt"))
        assert prompt.message_id == 702 and prompt.payload["purpose"] == "clarify_other"
    assert e.services.turns.accept_staged.await_count == 0


@pytest.mark.asyncio
async def test_attested_bad_network_details_keep_file_then_actionless_generic_error(mixed_ui, caplog):
    e = mixed_ui
    raw = request(
        source="sandbox_network",
        input_mode="single_choice",
        details={"host": None, "port": 443, "ttl_seconds": 300},
        options=[{"id": "allow_temporary", "label": "Allow", "value": "allow_temporary"}, {"id": "deny", "label": "Deny", "value": "deny"}],
    )
    identity = await show_mixed(e, raw)
    item = await e.services.outbox.get(e.link, identity)
    assert [part["state"] for part in item.parts] == ["sent", "sent"] and e.media_receipts
    question = next(call for call in reversed(e.request.calls) if call.endpoint == "sendMessage")
    assert question.parameters["text"].strip() and not question.parameters.get("reply_markup")
    async with e.services.outbox.sessions() as reader:
        assert not list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "network")))
    assert "human_input_invalid source=sandbox_network" in caplog.text


@pytest.mark.asyncio
async def test_reply_to_consumed_file_warning_is_plain_intent_not_old_question_guard(mixed_ui):
    from test_telegram_human_input_flows import quoted_message

    e = mixed_ui
    await show_mixed(e, request(), failed_file=True)
    warning = await saved_button(e, "file_retry")
    attempt = await e.services.publication_authority.capture(e.link, thread_id="request-thread", endpoint_id=e.services.thread_client.endpoint_id)
    await e.services.callbacks.consume_group(warning.group_id, link=e.link, guard=attempt.read_guard)
    incoming = quoted_message(e, 2302, "A new plain instruction", 690)
    assert not await e.services.human_inputs.handle_text(e.channel, incoming)
    async with e.services.outbox.sessions() as reader:
        assert not list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "prompt")))


@pytest.mark.asyncio
async def test_plain_publication_closes_question_after_media_retry_using_question_ack(mixed_ui):
    e = mixed_ui
    identity = await show_mixed(e, request(), failed_file=True)
    retry = await saved_button(e, "file_retry")
    attempt = await e.services.publication_authority.capture(e.link, thread_id="request-thread", endpoint_id=e.services.thread_client.endpoint_id)
    assert await e.services.model_source.retry_file_callback(e.link, retry, callbacks=e.services.callbacks, guard=attempt.read_guard)
    e.request.enqueue("editMessageReplyMarkup", result=sent(e, identity=690))
    await drain(e)
    await drain(e)
    incoming = message(e, 2303, content="A new unrelated instruction")
    origin, _ = await e.services.outbox.inbox.accept(e.link, incoming, sanitized_input={"type": "text", "text": incoming.message.text})
    await e.services.human_inputs.before_plain_publication(e.channel, e.link, [origin])
    e.request.enqueue("editMessageReplyMarkup", result=sent(e, identity=701))
    await drain(e)
    cleared = [call.parameters["message_id"] for call in e.request.calls if call.endpoint == "editMessageReplyMarkup"]
    assert cleared == [690, 701]
    assert e.media_receipts[-1] not in cleared
    async with e.services.outbox.sessions() as reader:
        question_rows = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.kind.in_(["clarify_option", "clarify_other"]))))
        assert question_rows and all(row.consumed_at is not None for row in question_rows)
    assert not await e.services.human_inputs.step_current(await e.services.outbox.get(e.link, identity))
