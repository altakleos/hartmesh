"""A stale captured note gets a complete fresh preview before new controls."""

from uuid import uuid4

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_work_control_admission import admissions, stage
from test_telegram_work_input_capture import accept, capture, prompt
from test_telegram_work_interaction_delivery import guard_for, selected
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.work_input_capture import WorkInputCaptures
from deerflow.agent_instances.human_input_contract import Respond
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramOutboxRow


async def previews(e, inbox, mark):
    from app.telegram_bots.work_input_previews import WorkInputPreviews

    host = WorkInputPreviews(e.plans, WorkInputCaptures(inbox, e.channel._services.callbacks))
    return await host.open(e.link, mark, guard=await guard_for(e))


@pytest.mark.asyncio
async def test_stale_note_is_kept_then_confirmed_against_complete_new_review(work_delivery):
    e = work_delivery
    request, prompt_id, old_seen = await prompt(e)
    inbox, mark = await accept(e)
    saved = await capture(e, inbox, mark, prompt_id)
    await e.human.respond(actor=e.actor, request_id=request["id"], request=Respond(operation_id=uuid4().hex, expected_request_revision=1, expected_assignment_revision=1, text="Check the revised reference first."))
    identity = await previews(e, inbox, mark)
    assert await previews(e, inbox, mark) == identity
    async with e.db.sessions() as reader:
        parent = await reader.get(TelegramOutboxRow, identity)
        assert parent.class_ == "reply" and parent.state == "pending"
        assert "Check the revised reference first." in "".join(part["html"] for part in parent.parts)
        assert saved.route_basis["work_input"]["text"] in "".join(part["html"] for part in parent.parts)
        assert not list(await reader.scalars(select(TelegramActRow)))
    await dispatch(e, 803)
    e.identity = identity
    callback = await selected(e, "changes_confirm")
    assert callback.message_id == 803 and callback.payload["seen"]["work_revision"] > old_seen["work_revision"]
    act = await stage(e, admissions(e), callback)
    assert act.id != saved.operation_ids[0] and act.body["note"] == saved.route_basis["work_input"]["text"]
    assert act.body["expected_revision"] == callback.payload["seen"]["work_revision"]
    assert (await admissions(e).acts.check(act.id)).state == "done"


@pytest.mark.asyncio
async def test_unchanged_change_note_does_not_gain_an_extra_confirmation(work_delivery):
    from app.telegram_bots.callbacks import CallbackRefused

    e = work_delivery
    _request, prompt_id, _seen = await prompt(e)
    inbox, mark = await accept(e)
    await capture(e, inbox, mark, prompt_id)
    with pytest.raises(CallbackRefused):
        await previews(e, inbox, mark)


@pytest.mark.asyncio
async def test_repeated_review_changes_retire_old_controls_and_preserve_note(work_delivery):
    from app.telegram_bots.callbacks import CallbackRefused
    from deerflow.agent_instances.contract import AgentConflict
    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow

    e = work_delivery
    request, prompt_id, _seen = await prompt(e)
    inbox, mark = await accept(e)
    await capture(e, inbox, mark, prompt_id)
    for text in ("First correction", "Second correction"):
        await e.human.respond(actor=e.actor, request_id=request["id"], request=Respond(operation_id=uuid4().hex, expected_request_revision=1, expected_assignment_revision=1, text=text))
        if text == "First correction":
            first = await previews(e, inbox, mark)
            await dispatch(e, 803)
            e.identity = first
            old = await selected(e, "changes_confirm")
    second = await previews(e, inbox, mark)
    assert first != second
    async with e.db.sessions() as reader:
        obsolete = await reader.get(TelegramCallbackRow, old.id)
        assert obsolete.consumed_at is not None and obsolete.closed_reason == "superseded"
        current = await reader.get(TelegramOutboxRow, second)
        text = "".join(part["html"] for part in current.parts)
        assert "Second correction" in text and "Keep this note" in text
        cleanups = list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["target_message_id"].as_integer() == 803)))
        assert any(row.payload.get("kind") == "keyboard_cleanup" for row in cleanups)
    with pytest.raises((CallbackRefused, AgentConflict)):
        await stage(e, admissions(e), old)


@pytest.mark.asyncio
async def test_typed_decision_is_delivered_before_explicit_confirmation(work_delivery):
    from test_telegram_inbox import incoming
    from test_telegram_work_actions import management_request
    from test_telegram_work_interaction_delivery import open_view
    from test_telegram_work_preview_recovery import install
    from test_telegram_work_typing_prompts import clear_old

    from app.telegram_bots.inbox import InboxRepository

    e = work_delivery
    install(e)
    _record, request, _seen = await management_request(e, "decision")
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramOutboxRow, e.identity)).state = "skipped"
    e.identity = await e.plans.plan(e.link, request_id=request["id"], dedupe_key="typed-decision")
    await dispatch(e, 801)
    e.identity = await open_view(e, await selected(e, "decide_own"))
    await clear_old(e)
    await dispatch(e, 802)
    async with e.db.sessions() as reader:
        prompt_id = (await reader.get(TelegramOutboxRow, e.identity)).payload["work_request_basis"]["prompt_id"]
    inbox = InboxRepository(e.db.sessions)
    mark, _ = await inbox.accept(e.link, incoming(message=900, update_id=1900, account=e.link.chat_id, reply=802), sanitized_input={"text": "Use the slower <reviewed> option."})
    await capture(e, inbox, mark, prompt_id)
    e.identity = await previews(e, inbox, mark)
    async with e.db.sessions() as reader:
        assert not list(await reader.scalars(select(TelegramActRow)))
    await dispatch(e, 803)
    assert not e.dispatch_errors, e.dispatch_errors
    call = [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"][-1]
    assert "Use the slower &lt;reviewed&gt; option." in call.parameters["text"]
    control = await selected(e, "decide_confirm")
    act = await stage(e, admissions(e), control)
    assert act.body["note"] == "Use the slower <reviewed> option."
    assert (await admissions(e).acts.check(act.id)).state == "done"


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["text", "done", "operation", "prompt", "topic", "voice", "files", "reply"])
async def test_final_sdk_input_proof_rejects_changed_accepted_origin(work_delivery, monkeypatch, damage):
    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramInboundMarkRow

    e = work_delivery
    request, prompt_id, _seen = await prompt(e)
    inbox, mark = await accept(e)
    await capture(e, inbox, mark, prompt_id)
    await e.human.respond(actor=e.actor, request_id=request["id"], request=Respond(operation_id=uuid4().hex, expected_request_revision=1, expected_assignment_revision=1, text="New review context"))
    identity = await previews(e, inbox, mark)
    sources = e.channel._services.work_request_sources
    original, changed = sources.settings.get, []

    async def mutate(*args, **kwargs):
        result = await original(*args, **kwargs)
        if not changed:
            changed.append(True)
            async with e.db.sessions.begin() as writer:
                row = await writer.get(TelegramInboundMarkRow, (mark.link_id, mark.message_id, mark.chat_id))
                if damage == "text":
                    row.sanitized_input = {"text": "different"}
                elif damage == "done":
                    row.route_complete = True
                elif damage == "operation":
                    row.operation_ids = [uuid4().hex]
                elif damage == "topic":
                    row.message_thread_id = 456
                elif damage == "voice":
                    row.sanitized_input = {**row.sanitized_input, "type": "voice"}
                elif damage == "files":
                    row.sanitized_input = {**row.sanitized_input, "files": ["unexpected"]}
                elif damage == "reply":
                    row.sanitized_input = {**row.sanitized_input, "reply_to_message_id": 999}
                else:
                    (await writer.get(TelegramCallbackRow, prompt_id)).closed_reason = "command"
        return result

    monkeypatch.setattr(sources.settings, "get", mutate)
    before = len(e.channel.bot.request.calls)
    e.clock.advance(10)
    await e.sender.tick()
    await e.sender.drain()
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, identity)).state == "skipped"
    assert changed and not [call for call in e.channel.bot.request.calls[before:] if call.endpoint == "sendMessage"]


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["done", "topic", "voice", "reply"])
async def test_finished_input_cannot_admit_a_new_confirmed_action(work_delivery, damage):
    from app.telegram_bots.producer import ProductSourceRetired
    from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow

    e = work_delivery
    request, prompt_id, _seen = await prompt(e)
    inbox, mark = await accept(e)
    await capture(e, inbox, mark, prompt_id)
    await e.human.respond(actor=e.actor, request_id=request["id"], request=Respond(operation_id=uuid4().hex, expected_request_revision=1, expected_assignment_revision=1, text="New context"))
    e.identity = await previews(e, inbox, mark)
    await dispatch(e, 803)
    control = await selected(e, "changes_confirm")
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramInboundMarkRow, (mark.link_id, mark.message_id, mark.chat_id))
        if damage == "done":
            row.route_complete = True
        elif damage == "topic":
            row.message_thread_id = 456
        elif damage == "voice":
            row.sanitized_input = {**row.sanitized_input, "type": "voice"}
        else:
            row.sanitized_input = {**row.sanitized_input, "reply_to_message_id": 999}
    with pytest.raises(ProductSourceRetired):
        await stage(e, admissions(e), control)
    async with e.db.sessions() as reader:
        assert not list(await reader.scalars(select(TelegramActRow)))


@pytest.mark.asyncio
@pytest.mark.parametrize("historical_id", [2**40, "malformed", None])
async def test_pending_input_action_uses_opaque_origin_without_narrow_json_casts(work_delivery, historical_id):
    from copy import deepcopy

    from app.telegram_bots.work_input_previews import blocked_input_actions, input_identity

    e = work_delivery
    request, prompt_id, _seen = await prompt(e)
    inbox, mark = await accept(e)
    captured = await capture(e, inbox, mark, prompt_id)
    await e.human.respond(actor=e.actor, request_id=request["id"], request=Respond(operation_id=uuid4().hex, expected_request_revision=1, expected_assignment_revision=1, text="New context"))
    e.identity = await previews(e, inbox, mark)
    await dispatch(e, 803)
    await stage(e, admissions(e), await selected(e, "changes_confirm"))
    async with e.db.sessions.begin() as writer:
        parent = await writer.get(TelegramOutboxRow, e.identity)
        payload = deepcopy(parent.payload)
        payload["work_request_basis"]["input"].update(chat_id=historical_id, message_id=historical_id)
        parent.payload = payload
    async with e.db.sessions() as reader:
        # This query must stay valid even for a malformed old JSON row. It
        # follows the reserved operation identity; it grants no send authority.
        assert await reader.scalar(select(blocked_input_actions(input_identity(captured))))
