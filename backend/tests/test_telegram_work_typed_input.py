"""Actual Telegram text retains its Work prompt and immutable action identity."""

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_inbox import incoming
from test_telegram_work_callback_dispatch import install_dispatch
from test_telegram_work_control_admission import admissions
from test_telegram_work_input_capture import prompt
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramInboundMarkRow, TelegramOutboxRow


def install_inputs(e):
    from app.telegram_bots.work_outcome_delivery import WorkOutcomePlans, WorkOutcomeSources
    from app.telegram_bots.work_text_inputs import WorkTextInputs

    plans = WorkOutcomePlans(e.plans, admissions(e).acts)
    e.channel._services.work_outcome_sources = WorkOutcomeSources(plans)
    callbacks = install_dispatch(e, plans)
    e.channel._services.work_text_inputs = WorkTextInputs(e.channel._services, callbacks)
    return plans, e.channel._services.work_text_inputs


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", [False, True])
async def test_actual_typed_changes_are_saved_once_or_retained_for_current_review(work_delivery, changed):
    from test_telegram_work_refusal_recovery import comment

    e = work_delivery
    request, _prompt_id, seen = await prompt(e)
    from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow

    async with e.db.sessions.begin() as writer:
        for attempt in await writer.scalars(select(AgentWorkAttemptRow).where(AgentWorkAttemptRow.work_id == request["work_id"])):
            attempt.request = {"expected_revision": 1}
    plans, _inputs = install_inputs(e)
    if changed:
        await comment(e, request)
    update = incoming(message=900, update_id=1900, account=e.link.chat_id, reply=802)
    assert await e.channel.handle_update(update)
    await e.channel._drain_ingress()
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        assert mark is not None and mark.route_kind == "work_input"
        assert mark.route_basis["work_input"]["text"] == update.message.text
        assert mark.route_basis["work_input"]["seen"] == seen
        actions = list(await reader.scalars(select(TelegramActRow)))
        if changed:
            assert actions == []
            row = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_request_basis"]["input"]["operation_id"].as_string() == mark.operation_ids[0]))
            assert row is not None and row.payload["work_request_basis"]["interaction"]["note"] == update.message.text
            assert "revised source" in "".join(part["html"] for part in row.parts)
        else:
            assert len(actions) == 1 and actions[0].state == "done" and actions[0].kind == "changes_requested"
            assert actions[0].body["note"] == update.message.text and actions[0].body["expected_revision"] == seen["work_revision"]
            row = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_outcome_basis"]["context"]["act"]["id"].as_string() == actions[0].id))
            assert row is not None and row.state == "pending"
    # Replaying the same accepted update keeps the original input and operation.
    await e.channel._services.work_text_inputs.handle_text(e.channel, update, captured_link=e.link)
    async with e.db.sessions() as reader:
        assert len(list(await reader.scalars(select(TelegramActRow)))) == (0 if changed else 1)

    from test_telegram_work_preview_recovery import dispatch

    await dispatch(e, 803)
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, row.id)).state == "sent", e.dispatch_errors
        saved_input = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        assert saved_input.route_complete is not changed


async def question_prompt(e, purpose):
    from test_telegram_work_interaction_delivery import open_view, selected
    from test_telegram_work_preview_recovery import dispatch, install
    from test_telegram_work_typing_prompts import clear_old

    install(e)
    await dispatch(e, 801)
    e.identity = await open_view(e, await selected(e, "hi_answer" if purpose == "answer" else "hi_cant_open"))
    await clear_old(e)
    await dispatch(e, 802)
    if purpose != "answer":
        e.identity = await open_view(e, await selected(e, "hi_cannot" if purpose == "cannot_provide" else "hi_wrong"))
        await clear_old(e)
        await dispatch(e, 803)
    return 802 if purpose == "answer" else 803


@pytest.mark.asyncio
@pytest.mark.parametrize("purpose", ["answer", "cannot_provide", "wrong_recipient"])
@pytest.mark.parametrize("quoted", [False, True])
async def test_actual_prompt_text_saves_one_response_without_starting_work(work_delivery, monkeypatch, purpose, quoted):
    from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow

    e = work_delivery
    message_id = await question_prompt(e, purpose)
    from app.telegram_bots import inbox

    monkeypatch.setattr(inbox, "_now", e.clock)
    install_inputs(e)
    async with e.db.sessions() as reader:
        before = list(await reader.scalars(select(AgentWorkAttemptRow.id)))
        assert list(await reader.scalars(select(TelegramActRow))) == []
    update = incoming(message=900, update_id=1900, account=e.link.chat_id, reply=message_id if quoted else None)
    assert await e.channel.handle_update(update)
    await e.channel._drain_ingress()
    async with e.db.sessions() as reader:
        actions = list(await reader.scalars(select(TelegramActRow)))
        assert len(actions) == 1 and actions[0].kind == "respond" and actions[0].state == "done"
        assert actions[0].body["text"] == update.message.text
        assert actions[0].body["disposition"] == ("supplied" if purpose == "answer" else purpose)
        assert list(await reader.scalars(select(AgentWorkAttemptRow.id))) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("note", [False, True])
@pytest.mark.parametrize("unknown", [False, True])
async def test_actual_typed_decision_waits_for_complete_preview_and_confirmation(work_delivery, monkeypatch, note, unknown):
    from test_telegram_work_actions import management_request
    from test_telegram_work_callback_dispatch import press_work
    from test_telegram_work_interaction_delivery import open_view, selected
    from test_telegram_work_preview_recovery import dispatch, install
    from test_telegram_work_typing_prompts import clear_old

    e = work_delivery
    install(e)
    _record, request, _seen = await management_request(e, "decision")
    from deerflow.persistence.agent_instances.human_input import HumanInputRequestRow

    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramOutboxRow, e.identity)).state = "skipped"
        if note:
            (await writer.get(HumanInputRequestRow, request["id"])).choices = ["Use the reviewed source"]
    e.identity = await e.plans.plan(e.link, request_id=request["id"], dedupe_key="actual-typed-decision")
    await dispatch(e, 801)
    e.identity = await open_view(e, await selected(e, "decide_choice" if note else "decide_own"))
    await clear_old(e)
    await dispatch(e, 802)
    if note:
        e.identity = await open_view(e, await selected(e, "decide_note"))
        await clear_old(e)
        await dispatch(e, 803)
    plans, _inputs = install_inputs(e)
    update = incoming(message=900, update_id=1900, account=e.link.chat_id, reply=803 if note else 802)
    assert await e.channel.handle_update(update)
    await e.channel._drain_ingress()
    async with e.db.sessions() as reader:
        assert list(await reader.scalars(select(TelegramActRow))) == []
        row = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_request_basis"]["input"]["message_id"].as_integer() == 900))
        assert row is not None and row.state == "pending"
        state = row.payload["work_request_basis"]["interaction"]
        assert state["note" if note else "decision"] == update.message.text
    e.identity = row.id
    await dispatch(e, 804)
    callback = await selected(e, "decide_confirm")
    if unknown:

        async def no_receipt(*args, **kwargs):
            raise TimeoutError()

        monkeypatch.setattr(plans.acts.work, "command", no_receipt)
    await press_work(e, callback, message_id=804)
    async with e.db.sessions() as reader:
        saved = await reader.get(TelegramActRow, callback.id)
        assert saved.state == ("pending" if unknown else "done") and saved.kind == "decide"
        expected = state["decision"] + ("\n\n" + state["note"] if note else "")
        assert saved.body["note"] == expected
        from app.telegram_bots.inbox import InboxRow

        origin = InboxRow.from_row(await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id)))
    assert await _inputs.resume(e.channel, origin)
    async with e.db.sessions() as reader:
        assert len(list(await reader.scalars(select(TelegramActRow)))) == 1
        assert (await reader.get(TelegramActRow, callback.id)).attempts == saved.attempts


@pytest.mark.asyncio
async def test_duplicate_text_after_unknown_result_reads_only_original_receipt(work_delivery, monkeypatch):
    from deerflow.agent_instances.human_input import HumanInput

    e = work_delivery
    message_id = await question_prompt(e, "answer")
    plans, inputs = install_inputs(e)
    calls = []

    async def unknown(*args, **kwargs):
        calls.append(kwargs["request"].operation_id)
        raise TimeoutError()

    monkeypatch.setattr(HumanInput, "respond", unknown)
    update = incoming(message=900, update_id=1900, account=e.link.chat_id, reply=message_id)
    assert await e.channel.handle_update(update)
    await e.channel._drain_ingress()
    assert await inputs.handle_text(e.channel, update, captured_link=e.link)
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        saved = await reader.get(TelegramActRow, mark.operation_ids[0])
        assert saved.state == "pending" and saved.attempts == 1
        assert calls == [saved.id] and not mark.route_complete


@pytest.mark.asyncio
async def test_retired_text_lifetime_cannot_start_an_admitted_action(work_delivery, monkeypatch):
    e = work_delivery
    message_id = await question_prompt(e, "answer")
    plans, inputs = install_inputs(e)
    original, reached = inputs.admissions.stage, []

    async def retire(*args, **kwargs):
        saved = await original(*args, **kwargs)
        reached.append(True)
        e.channel._stopping = True
        return saved

    monkeypatch.setattr(inputs.admissions, "stage", retire)
    update = incoming(message=900, update_id=1900, account=e.link.chat_id, reply=message_id)
    try:
        await e.channel.handle_update(update)
        await e.channel._drain_ingress()
    finally:
        e.channel._stopping = False
    assert reached
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        saved = await reader.get(TelegramActRow, mark.operation_ids[0])
        assert saved.state == "pending" and saved.attempts == 0
        assert not mark.route_complete


@pytest.mark.asyncio
async def test_existing_ordinary_input_does_not_switch_to_a_new_work_prompt(work_delivery):
    e = work_delivery
    update = incoming(message=900, update_id=1900, account=e.link.chat_id)
    mark, _ = await e.channel._services.outbox.inbox.accept(e.link, update, sanitized_input={"text": update.message.text})
    original = await e.channel._services.outbox.inbox.route(mark, kind="turn", target_id=e.link.agent_ref, basis={"input_profile": {"version": 1, "kind": "plain_text", "forwarded": False}}, operation_ids=[])
    await question_prompt(e, "answer")
    _plans, inputs = install_inputs(e)
    assert await inputs.handle_text(e.channel, update, captured_link=e.link) is False
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        assert mark.route_kind == "turn" and mark.route_basis == original.route_basis
        assert list(await reader.scalars(select(TelegramActRow))) == []


@pytest.mark.asyncio
async def test_reopened_input_owner_settles_late_receipt_without_resubmitting(work_delivery, monkeypatch):
    from app.telegram_bots.work_actions import request_for
    from app.telegram_bots.work_text_inputs import WorkTextInputs
    from deerflow.agent_instances.contract import PrincipalRef
    from deerflow.agent_instances.human_input import HumanInput

    e = work_delivery
    message_id = await question_prompt(e, "answer")
    plans, inputs = install_inputs(e)
    command = HumanInput.respond

    async def unknown(*args, **kwargs):
        raise TimeoutError()

    monkeypatch.setattr(HumanInput, "respond", unknown)
    update = incoming(message=900, update_id=1900, account=e.link.chat_id, reply=message_id)
    assert await e.channel.handle_update(update)
    await e.channel._drain_ingress()
    async with e.db.sessions() as reader:
        from app.telegram_bots.inbox import InboxRow

        origin = InboxRow.from_row(await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id)))
    original = await plans.acts._work_record(origin.operation_ids[0])
    await command(HumanInput(plans.acts.work), actor=PrincipalRef("human", e.link.owner_user_id), request_id=original.request_id, request=request_for(original.kind, original.body, identity=original.id))

    async def unexpected(*args, **kwargs):
        pytest.fail("A recovered canonical receipt must not execute a command")

    monkeypatch.setattr(HumanInput, "respond", unexpected)
    recovered = WorkTextInputs(e.channel._services, e.channel._services.work_callbacks)
    origin.route_basis["work_input"]["text"] = "A caller cannot replace the accepted text"
    assert await recovered.resume(e.channel, origin)
    saved = await plans.acts._work_record(original.id)
    assert saved.state == "done" and saved.attempts == original.attempts
    assert saved.body["text"] == update.message.text
