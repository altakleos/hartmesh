"""Actual SQL/card ownership and bus input; remote source proofs are explicit seeds."""

from copy import deepcopy

import httpx
import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from sqlalchemy import delete, select
from test_telegram_human_input_plans import parts, source
from test_telegram_publication_authority import attempt_environment as attempt_environment
from test_telegram_publication_coordinator import coordinator_environment as coordinator_environment

from app.telegram_bots.callbacks import CallbackRepository, Prompts
from app.telegram_bots.inbox import CALLBACK_PUBLICATION_KEY
from app.telegram_bots.threads import ThreadCoordinator
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramInboundMarkRow


@pytest_asyncio.fixture
async def submission_environment(coordinator_environment, request):
    e = coordinator_environment
    if getattr(request, "param", None) == "media_question":
        from datetime import UTC, datetime

        from telegram import Chat, Document, Message, Update, User

        incoming = Update(
            2801,
            message=Message(
                801,
                datetime.now(UTC),
                Chat(e.link.chat_id, "private"),
                from_user=User(e.link.telegram_user_id, "Actual human", False),
                caption="Question about this file",
                document=Document("question-file", "question-unique", file_name="question.pdf", mime_type="application/pdf", file_size=3),
            ),
        )
        file = {"type": "file", "file_id": "question-file", "file_unique_id": "question-unique", "filename": "question.pdf", "mime_type": "application/pdf", "size": 3, "message_id": 801, "telegram_bot_id": e.link.bot_id}
        row, _ = await e.inbox.accept(e.link, incoming, sanitized_input={"type": "media", "text": incoming.message.caption, "files": [file]})
        row = await e.inbox.route(row, kind="turn", target_id=e.link.agent_ref, basis={"input_profile": {"version": 2, "kind": "chat_input", "forwarded": False, "origin": None}}, operation_ids=[])
        e.origins = (row,)
    qualified, reader, _basis = await source(e)
    network = getattr(request, "param", None) in {"network", "network_mismatch"}
    form_request = getattr(request, "param", None) == "form"
    if network or form_request:
        from _telegram_human_input_test_support import request as request_payload
        from _telegram_human_input_test_support import source_basis
        from test_telegram_publication_observation import ACK

        payload = request_payload(
            source="sandbox_network",
            input_mode="single_choice",
            details={"host": "example.test", "port": 443, "ttl_seconds": 300},
            options=[{"id": "allow_temporary", "label": "Allow", "value": "Allow for five minutes"}, {"id": "deny", "label": "Keep blocked", "value": "Keep blocked"}],
        )
        if form_request:
            payload = request_payload(version=2, input_mode="form", fields=[{"name": "company", "label": "Company", "type": "text", "required": True}, {"name": "copy", "label": "Copy?", "type": "checkbox", "required": False}])
            payload.pop("options")
        network_basis = source_basis(qualified.record, payload=payload)
        previous_respond = e.responder

        async def network_respond(incoming):
            if incoming.url.path.endswith("/output"):
                return httpx.Response(200, headers=ACK, json=network_basis["projection"])
            if incoming.url.path.endswith("/messages"):
                return httpx.Response(200, headers=ACK, json={"data": network_basis["messages"], "has_more": False})
            return await previous_respond(incoming)

        e.responder = network_respond
        qualified = await reader.read_request(qualified.record, e.link)
    e.bus.get_inbound_nowait()
    e.bus.inbound_task_done()
    actual_respond = e.responder

    async def respond(request):
        if request.url.path.endswith("/" + e.thread_id):
            return httpx.Response(200, json={"thread_id": e.thread_id, "owner_user_id": e.link.owner_user_id, "agent_instance": None, "metadata": {"channel_agent_name": "lead_agent"}})
        return await actual_respond(request)

    e.responder = respond
    e.callbacks = CallbackRepository(e.db.sessions, config=e.db.config)
    e.prompts = Prompts(e.callbacks)
    e.parent = qualified.record
    e.qualified = qualified
    e.response = {"version": 1, "kind": "human_input_response", "source": "ask_clarification", "request_id": qualified.request["request_id"], "response_kind": "option", "option_id": "option-1", "value": "First"}
    attempt = await e.host.capture(e.link, thread_id=e.thread_id, endpoint_id=e.http.endpoint_id)
    e.guard = attempt.read_guard
    kind = "network" if network else "clarify_option"
    if network:
        e.response.update(source="sandbox_network", option_id="allow_temporary", value="Allow for five minutes")
    callback_payload = {"thread_id": e.thread_id, "request_id": e.response["request_id"], "option_id": e.response["option_id"], "value": e.response["value"]}
    if network:
        callback_payload.pop("value")
        callback_payload.update(host="example.test", port=443)
        if getattr(request, "param", None) == "network_mismatch":
            callback_payload["host"] = "different.example.test"
        e.network_body = {key: callback_payload[key] for key in ("thread_id", "request_id", "option_id", "host", "port")}
    if form_request:
        from uuid import uuid4

        from app.telegram_bots.forms import Forms, form_submission_value

        e.forms = Forms(e.callbacks)
        created = await e.forms.create(e.link, form_id=uuid4().hex, thread_id=e.thread_id, request=qualified.request, guard=e.guard)
        answered = await e.forms.transition(e.link, created.id, expected_revision=created.revision, update_id=3801, action="answer", field="company", value="Jones Ltd", guard=e.guard)
        e.form = await e.forms.transition(e.link, answered.id, expected_revision=answered.revision, update_id=3802, action="skip", field="copy", guard=e.guard)
        e.response = {key: value for key, value in e.response.items() if key != "option_id"}
        e.response.update(response_kind="text", value=form_submission_value(e.form.fields, e.form.values))
        kind, callback_payload = "form_send", {"form_id": e.form.id, "revision": e.form.revision}
    keyboard = [[{"label": "label.try_again", "kind": kind, "target_kind": "thread", "target_id": e.thread_id, "payload": callback_payload}]]
    e.card = await e.outbox.plan_human_input_reply(e.parent, qualified, parts=parts(e.parent), keyboard=keyboard, callbacks=e.callbacks, guard=e.guard)
    async with e.db.sessions() as reader:
        row = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == kind))
        e.callback_id, group = row.id, row.group_id
    assert await e.callbacks.adopt(e.link, group, 1901, chat_id=e.link.chat_id, message_thread_id=None, outbox_id=e.card, presentation_version=0, check_current=lambda: True)

    async def stage(**changes):
        if form_request:
            changes.setdefault("forms", e.forms)
        return await e.inbox.stage_submission(
            e.link, source_outbox_id=e.card, response=e.response, guard=e.guard, callback_id=e.callback_id, update_id=3901, chat_id=e.link.chat_id, message_id=1901, message_thread_id=None, callbacks=e.callbacks, **changes
        )

    e.stage = stage
    yield e


@pytest.mark.asyncio
@pytest.mark.parametrize("collected", [False, True])
async def test_callback_submission_dispatches_answer_not_old_question_and_survives_input_gc(submission_environment, collected):
    e = submission_environment
    if collected:
        async with e.db.sessions.begin() as writer:
            await writer.execute(delete(TelegramInboundMarkRow).where(TelegramInboundMarkRow.link_id == e.link.id))
    stage = await e.stage()
    assert stage.text == "First" and stage.human_message_kwargs == {"human_input_response": e.response}
    assert [row.message_id for row in stage.origins] == [701, 702]
    assert await e.stage() == stage
    callback = await e.callbacks.get(e.callback_id)
    assert callback.consumed_at is None
    assert callback.payload[CALLBACK_PUBLICATION_KEY]["version"] == 2
    record = await e.inbox.reserve_callback_publication(e.link, e.callback_id, thread_id=e.thread_id, endpoint_id=e.http.endpoint_id, guard=e.guard)
    assert record.input_sha256 != e.parent.input_sha256
    dispatched = await e.coordinator.dispatch_record(record, e.link)
    sent = e.bus.get_inbound_nowait()
    e.bus.inbound_task_done()
    assert sent.text == "First" and sent.text != "\n\n".join(row.sanitized_input["text"] for row in e.origins)
    assert sent.execution_context.thread_id == e.thread_id
    assert sent.metadata["human_message_kwargs"] == {"human_input_response": e.response, "channel_turn_id": record.publication_id}
    assert dispatched.dispatch_started and (await e.callbacks.get(e.callback_id)).consumed_at is not None
    assert (await e.coordinator.dispatch_record(record, e.link)).publication_id == record.publication_id
    assert e.bus.inbound_queue.empty()


@pytest.mark.asyncio
async def test_changed_stored_response_cannot_reuse_its_input_digest(submission_environment):
    e = submission_environment
    await e.stage()
    record = await e.inbox.reserve_callback_publication(e.link, e.callback_id, thread_id=e.thread_id, endpoint_id=e.http.endpoint_id, guard=e.guard)
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramCallbackRow, e.callback_id)
        payload = deepcopy(row.payload)
        payload[CALLBACK_PUBLICATION_KEY]["submission"]["text"] = "A different answer"
        row.payload = payload
    with pytest.raises(ValueError):
        await e.inbox.read_publication_submission(record, guard=e.guard)
    assert e.bus.inbound_queue.empty()


@pytest.mark.asyncio
async def test_typed_other_keeps_actual_new_message_and_original_thread_after_new_mapping(submission_environment):
    e = submission_environment
    origin = await e.add_origin(message_id=801, text="My actual typed answer")
    prompt = await e.prompts.open(e.link, "clarify_other", {"thread_id": e.thread_id, "request_id": e.response["request_id"]}, chat_id=e.link.chat_id, message_thread_id=None, message_id=None)
    async with e.callbacks.writer(e.link, e.guard) as writer:
        await e.prompts.attach(e.link, prompt, outbox_id=e.card, guard=e.guard, session=writer)
    response = {key: value for key, value in e.response.items() if key != "option_id"}
    response.update(response_kind="text", value="My actual typed answer")
    stage = await e.inbox.stage_submission(e.link, source_outbox_id=e.card, response=response, origin=origin, prompt_id=prompt, guard=e.guard)
    assert [row.message_id for row in stage.origins] == [801] and stage.callback_id is None
    await e.db.connections.set_thread_id(connection_id=e.link.connection_id, owner_user_id=e.link.owner_user_id, provider="telegram", external_conversation_id=str(e.link.chat_id), thread_id="newer-conversation")
    threads = ThreadCoordinator(links=e.db.links, authority=e.live.authority, client=e.http, channel=lambda _id: e.current[0], send_allowed=lambda: e.creating[0], outbox=e.outbox, product_name="HartMesh")
    try:
        assert await threads.ensure_submission_thread(e.link, stage) == e.thread_id
        grouped = await e.inbox.group_prepublication(stage.origins, guard=e.guard)
        record = await e.inbox.reserve_publication(e.link, grouped, thread_id=stage.thread_id, endpoint_id=e.http.endpoint_id, guard=e.guard)
        await e.coordinator.dispatch_record(record, e.link)
        sent = e.bus.get_inbound_nowait()
        e.bus.inbound_task_done()
        assert sent.text == origin.sanitized_input["text"] and sent.metadata["human_message_kwargs"]["human_input_response"] == response
        assert sent.execution_context.thread_id == e.thread_id
        assert not any(method == "POST" for method, _, _ in e.requests)
    finally:
        await threads.stop()


@pytest.mark.asyncio
async def test_typed_submission_cannot_launder_forwarded_origin_into_plain_input(submission_environment):
    e = submission_environment
    origin = await e.add_origin(message_id=811, text="Forwarded proposed answer", profile={"version": 1, "kind": "plain_text", "forwarded": True})
    prompt = await e.prompts.open(e.link, "clarify_other", {"thread_id": e.thread_id, "request_id": e.response["request_id"]}, chat_id=e.link.chat_id, message_thread_id=None, message_id=None)
    async with e.callbacks.writer(e.link, e.guard) as writer:
        await e.prompts.attach(e.link, prompt, outbox_id=e.card, guard=e.guard, session=writer)
    response = {key: value for key, value in e.response.items() if key != "option_id"}
    response.update(response_kind="text", value=origin.sanitized_input["text"])
    with pytest.raises(ValueError):
        await e.inbox.stage_submission(e.link, source_outbox_id=e.card, response=response, origin=origin, prompt_id=prompt, guard=e.guard)
    async with e.db.sessions() as reader:
        saved = await reader.get(TelegramInboundMarkRow, (e.link.id, 811, e.link.chat_id))
        assert saved.route_basis["input_profile"]["forwarded"] is True and "submission" not in saved.route_basis


@pytest.mark.asyncio
async def test_staged_callback_receipt_rechecks_authority_after_last_sql_read(submission_environment, monkeypatch):
    e = submission_environment
    await e.stage()
    actual = e.inbox.lock_callback_input

    async def retire_after_read(*args, **kwargs):
        result = await actual(*args, **kwargs)
        e.reading[0] = False
        return result

    monkeypatch.setattr(e.inbox, "lock_callback_input", retire_after_read)
    with pytest.raises(ValueError):
        await e.stage()


@pytest.mark.asyncio
@pytest.mark.parametrize("submission_environment", ["form"], indirect=True)
async def test_form_send_stages_immutable_values_and_consumes_form_atomically(submission_environment):
    e = submission_environment
    stage = await e.stage()
    assert stage.text == 'Company: Jones Ltd; Copy?: no [values: {"company":"Jones Ltd","copy":false}]'
    assert stage.human_message_kwargs == {"human_input_response": e.response}
    assert (await e.forms.read(e.link, e.form.id, guard=e.guard)).state == "submitted"
    assert await e.stage() == stage
    record = await e.inbox.reserve_callback_publication(e.link, e.callback_id, thread_id=e.thread_id, endpoint_id=e.http.endpoint_id, guard=e.guard)
    await e.coordinator.dispatch_record(record, e.link)
    sent = e.bus.get_inbound_nowait()
    e.bus.inbound_task_done()
    assert sent.text == stage.text and sent.metadata["human_message_kwargs"]["human_input_response"] == e.response
    assert sent.execution_context.thread_id == e.thread_id


@pytest.mark.asyncio
async def test_staged_callback_receipt_does_not_upgrade_a_different_chat(submission_environment):
    e = submission_environment
    await e.stage()
    with pytest.raises(ValueError):
        await e.inbox.stage_submission(
            e.link, source_outbox_id=e.card, response=e.response, guard=e.guard, callback_id=e.callback_id, update_id=3902, chat_id=e.link.chat_id + 1, message_id=1901, message_thread_id=None, callbacks=e.callbacks
        )


@pytest.mark.asyncio
async def test_explicit_retry_of_typed_answer_keeps_its_response_and_original_thread(submission_environment):
    e = submission_environment
    origin = await e.add_origin(message_id=821, text="Retained typed answer")
    prompt = await e.prompts.open(e.link, "clarify_other", {"thread_id": e.thread_id, "request_id": e.response["request_id"]}, chat_id=e.link.chat_id, message_thread_id=None, message_id=None)
    async with e.callbacks.writer(e.link, e.guard) as writer:
        await e.prompts.attach(e.link, prompt, outbox_id=e.card, guard=e.guard, session=writer)
    response = {key: value for key, value in e.response.items() if key != "option_id"}
    response.update(response_kind="text", value=origin.sanitized_input["text"])
    typed = await e.inbox.stage_submission(e.link, source_outbox_id=e.card, response=response, origin=origin, prompt_id=prompt, guard=e.guard)
    outcome = await e.outbox.plan_input_outcome(
        e.link,
        source_id="typed-prepublication-refused:821",
        source_ref={"key": "chat.files_full", "values": {}},
        keyboard=[[{"label": "label.try_again", "kind": "retry_message", "target_kind": "request", "target_id": "821"}]],
        callbacks=e.callbacks,
        guard=e.guard,
        inbox_rows=typed.origins,
    )
    async with e.db.sessions() as reader:
        button = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "retry_message"))
        identity, group = button.id, button.group_id
    assert await e.callbacks.adopt(e.link, group, 1905, chat_id=e.link.chat_id, message_thread_id=None, outbox_id=outcome, presentation_version=0, check_current=lambda: True)
    retried = await e.inbox.stage_callback_input(e.link, callback_id=identity, update_id=3905, chat_id=e.link.chat_id, message_id=1905, message_thread_id=None, callbacks=e.callbacks, guard=e.guard)
    assert retried.human_message_kwargs == {"human_input_response": response}
    assert retried.thread_id == e.thread_id
    record = await e.inbox.reserve_callback_publication(e.link, identity, thread_id=e.thread_id, endpoint_id=e.http.endpoint_id, guard=e.guard)
    await e.coordinator.dispatch_record(record, e.link)
    sent = e.bus.get_inbound_nowait()
    e.bus.inbound_task_done()
    assert sent.text == typed.text and sent.metadata["human_message_kwargs"]["human_input_response"] == response


@pytest.mark.asyncio
@pytest.mark.parametrize("submission_environment", ["media_question"], indirect=True)
async def test_new_clarification_answer_does_not_resend_historical_question_file(submission_environment):
    e = submission_environment
    assert e.origins[0].sanitized_input["files"][0]["file_id"] == "question-file"
    stage = await e.stage()
    record = await e.inbox.reserve_callback_publication(e.link, stage.callback_id, thread_id=e.thread_id, endpoint_id=e.http.endpoint_id, guard=e.guard)
    await e.coordinator.dispatch_record(record, e.link)
    sent = e.bus.get_inbound_nowait()
    e.bus.inbound_task_done()
    assert sent.text == "First" and sent.files == []
    assert sent.metadata["human_message_kwargs"]["human_input_response"] == e.response
