"""Work typing prompts become visible only after their complete source ACK."""

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_work_actions import management_request
from test_telegram_work_interaction_delivery import guard_for, open_view, selected
from test_telegram_work_preview_recovery import dispatch, install
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.callbacks import Prompts
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow


async def clear_old(e):
    e.channel.bot.request.enqueue("editMessageReplyMarkup", body={"ok": True, "result": True})
    e.clock.advance(10)
    await e.sender.tick()
    await e.sender.drain()


@pytest.mark.asyncio
@pytest.mark.parametrize("purpose,kind,prompt_purpose", [("information", "hi_answer", "answer"), ("review", "changes_open", "request_changes"), ("decision", "decide_own", "decide_own")])
async def test_prompt_requires_full_delivery_and_preserves_request_basis(work_delivery, purpose, kind, prompt_purpose):
    e = work_delivery
    install(e)
    if purpose != "information":
        _record, request, _seen = await management_request(e, purpose)
        async with e.db.sessions.begin() as writer:
            (await writer.get(TelegramOutboxRow, e.identity)).state = "skipped"
        e.identity = await e.plans.plan(e.link, request_id=request["id"], dedupe_key="typing:" + purpose)
    await dispatch(e, 801)
    callback = await selected(e, kind)
    identity = await open_view(e, callback)
    prompts = Prompts(e.channel._services.callbacks)
    guard = await guard_for(e)
    assert await prompts.current(e.link, e.link.chat_id, None, guard=guard) is None
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        basis = row.payload["work_request_basis"]
        prompt = await reader.get(TelegramCallbackRow, basis["prompt_id"])
        assert prompt.payload["purpose"] == prompt_purpose
        assert prompt.payload["seen"] == basis["source"]["seen"]
        assert prompt.payload["prompt_visibility"]["state"] == "pending"
        assert prompt.expires_at is None and prompt.message_id is None
        assert row.parts[-1]["force_reply"] and not row.parts[-1].get("stored_keyboard")
        assert not list(await reader.scalars(select(TelegramActRow)))
    await clear_old(e)
    await dispatch(e, 802)
    assert not e.dispatch_errors, e.dispatch_errors
    prompt = await prompts.current(e.link, e.link.chat_id, None, guard=guard)
    assert prompt and prompts.is_visible(prompt) and prompt.message_id == 802
    calls = [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
    assert calls[-1].parameters["reply_markup"] == {"force_reply": True, "selective": True}
    assert not e.dispatch_errors


@pytest.mark.asyncio
@pytest.mark.parametrize("kind,purpose", [("hi_cannot", "cannot_provide"), ("hi_wrong", "wrong_recipient")])
async def test_reason_prompt_from_cannot_menu_has_no_service_effect(work_delivery, kind, purpose):
    e = work_delivery
    install(e)
    await dispatch(e, 801)
    first = await open_view(e, await selected(e, "hi_cant_open"))
    await clear_old(e)
    await dispatch(e, 802)
    e.identity = first
    identity = await open_view(e, await selected(e, kind))
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        prompt = await reader.get(TelegramCallbackRow, row.payload["work_request_basis"]["prompt_id"])
        assert prompt.payload["purpose"] == purpose
        assert not list(await reader.scalars(select(TelegramActRow)))
    await clear_old(e)
    await dispatch(e, 803)
    assert not e.dispatch_errors, e.dispatch_errors
    prompt = await Prompts(e.channel._services.callbacks).current(e.link, e.link.chat_id, None, guard=await guard_for(e))
    assert prompt and prompt.message_id == 803
    assert not (await e.human.responses(actor=e.actor, request_id=e.request["id"]))["responses"]


@pytest.mark.asyncio
@pytest.mark.parametrize("when", ["queued", "final_snapshot"])
async def test_closed_prompt_retires_before_sdk_after_restart(work_delivery, monkeypatch, when):
    from app.telegram_bots.work_request_source import WorkRequestSources

    e = work_delivery
    install(e)
    await dispatch(e, 801)
    identity = await open_view(e, await selected(e, "hi_answer"))
    await clear_old(e)
    prompts = Prompts(e.channel._services.callbacks)
    guard = await guard_for(e)
    sources = WorkRequestSources(reader=e.reader, outbox=e.channel._services.outbox, callbacks=e.channel._services.callbacks)
    e.channel._services.work_request_sources = sources
    if when == "queued":
        await prompts.close(e.link, e.link.chat_id, None, reason="command", guard=guard)
    else:
        original = sources.settings.get

        async def close(*args, **kwargs):
            settings = await original(*args, **kwargs)
            await prompts.close(e.link, e.link.chat_id, None, reason="command", guard=guard)
            return settings

        monkeypatch.setattr(sources.settings, "get", close)
    before = len(e.channel.bot.request.calls)
    e.clock.advance(10)
    await e.sender.tick()
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "skipped", (row.state, e.dispatch_errors)
    assert not [call for call in e.channel.bot.request.calls[before:] if call.endpoint == "sendMessage"]


@pytest.mark.asyncio
async def test_multipart_prompt_waits_for_last_ack_after_reader_restart(work_delivery):
    from test_telegram_work_interaction_delivery import multipart_source

    e = work_delivery
    install(e)
    await multipart_source(e)
    identity = await open_view(e, await selected(e, "changes_open"))
    await clear_old(e)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        count = len(row.parts)
        prompt_id = row.payload["work_request_basis"]["prompt_id"]
    assert count > 2
    await dispatch(e, 901)
    prompts = Prompts(e.channel._services.callbacks)
    guard = await guard_for(e)
    assert await prompts.current(e.link, e.link.chat_id, None, guard=guard) is None
    async with e.db.sessions() as reader:
        prompt = await reader.get(TelegramCallbackRow, prompt_id)
        assert prompt.expires_at is None and prompt.message_id is None
    first = [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"][-1]
    assert not first.parameters.get("reply_markup")
    for index in range(1, count):
        await dispatch(e, 901 + index)
    prompt = await prompts.current(e.link, e.link.chat_id, None, guard=guard)
    assert prompt.id == prompt_id and prompts.is_visible(prompt) and prompt.message_id == 900 + count
    assert prompt.created_at == e.clock.now
    assert not e.dispatch_errors, e.dispatch_errors


@pytest.mark.asyncio
async def test_replacing_pending_work_prompt_cannot_revive_it(work_delivery):
    e = work_delivery
    install(e)
    await dispatch(e, 801)
    identity = await open_view(e, await selected(e, "hi_answer"))
    await clear_old(e)
    prompts = Prompts(e.channel._services.callbacks)
    guard = await guard_for(e)
    successor = await prompts.open(e.link, "unrelated", {}, chat_id=e.link.chat_id, message_id=804, message_thread_id=None, guard=guard)
    before = len(e.channel.bot.request.calls)
    e.clock.advance(10)
    await e.sender.tick()
    await e.sender.drain()
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, identity)).state == "skipped"
    current = await prompts.current(e.link, e.link.chat_id, None, guard=guard)
    assert current.id == successor
    assert not [call for call in e.channel.bot.request.calls[before:] if call.endpoint == "sendMessage"]


@pytest.mark.asyncio
async def test_protected_prompt_keeps_content_and_reply_bar_protected(work_delivery):
    from test_telegram_bot_adapter import message
    from test_telegram_work_preview_recovery import concealed, reveal

    e = work_delivery
    install(e)
    show = await concealed(e)
    await reveal(e, show)
    message(e.channel.bot.request, endpoint="editMessageText", identity=821)
    e.clock.advance(10)
    await e.sender.tick()
    await e.sender.drain()
    await open_view(e, await selected(e, "hi_answer"))
    await clear_old(e)
    await dispatch(e, 822)
    assert not e.dispatch_errors, e.dispatch_errors
    call = [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"][-1]
    assert call.parameters["protect_content"] and call.parameters["reply_markup"]["force_reply"]
    assert "Which guide?" in call.parameters["text"]


@pytest.mark.asyncio
async def test_decision_note_prompt_keeps_the_exact_unsubmitted_choice(work_delivery):
    from uuid import uuid4

    from deerflow.agent_instances.human_input_contract import CreateRequest
    from deerflow.agent_instances.work_contract import DelegateWork

    e = work_delivery
    install(e)
    work = await e.human.work.delegate(actor=e.actor, instance_id=e.instance.id, request=DelegateWork(operation_id=uuid4().hex, objective="Choose a plan", success_criteria="A decision"))
    request = await e.human.create(
        actor=e.actor,
        instance_id=e.instance.id,
        work_id=work["id"],
        request=CreateRequest(
            operation_id=uuid4().hex,
            expected_work_revision=1,
            expected_assignment_revision=1,
            purpose="decision",
            question="Which option?",
            reason="Choose a direction",
            expected_response="A decision",
            recipient_id=e.link.owner_user_id,
            choices=["Choose <A>"],
        ),
    )
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramOutboxRow, e.identity)).state = "skipped"
    e.identity = await e.plans.plan(e.link, request_id=request["id"], dedupe_key="decision-note")
    await dispatch(e, 801)
    preview = await open_view(e, await selected(e, "decide_choice"))
    await clear_old(e)
    await dispatch(e, 802)
    e.identity = preview
    await open_view(e, await selected(e, "decide_note"))
    await clear_old(e)
    await dispatch(e, 803)
    prompts = Prompts(e.channel._services.callbacks)
    prompt = await prompts.current(e.link, e.link.chat_id, None, guard=await guard_for(e))
    assert prompt and prompts.is_visible(prompt)
    assert prompt.payload["purpose"] == "decide_note"
    assert prompt.payload["interaction"]["decision"] == "Choose <A>"
    call = [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"][-1]
    assert "Choose &lt;A&gt;" in call.parameters["text"]
    async with e.db.sessions() as reader:
        assert not list(await reader.scalars(select(TelegramActRow)))


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["seen", "boolean_revision", "anchor", "part", "target", "message", "expiry"])
async def test_final_sdk_snapshot_refuses_changed_prompt_ownership(work_delivery, monkeypatch, damage):
    e = work_delivery
    install(e)
    await dispatch(e, 801)
    identity = await open_view(e, await selected(e, "hi_answer"))
    await clear_old(e)
    sources = e.channel._services.work_request_sources
    original = sources.settings.get
    changed = []

    async def damage_prompt(*args, **kwargs):
        settings = await original(*args, **kwargs)
        if not changed:
            changed.append(True)
            async with e.db.sessions.begin() as writer:
                row = await writer.get(TelegramOutboxRow, identity)
                prompt = await writer.get(TelegramCallbackRow, row.payload["work_request_basis"]["prompt_id"])
                if damage == "seen":
                    prompt.payload = {**prompt.payload, "seen": {**prompt.payload["seen"], "revision": 999}}
                elif damage == "boolean_revision":
                    from sqlalchemy.orm.attributes import flag_modified

                    prompt.payload = {**prompt.payload, "seen": {**prompt.payload["seen"], "revision": True}}
                    # JSON equality also treats True as 1 in SQLAlchemy's dirty
                    # comparison: force the malformed persisted fixture.
                    flag_modified(prompt, "payload")
                elif damage == "target":
                    prompt.target_kind, prompt.target_id = "request", e.request["id"]
                elif damage == "message":
                    prompt.message_id = 999
                elif damage == "expiry":
                    prompt.expires_at = e.clock.now
                elif damage == "anchor":
                    prompt.payload = {**prompt.payload, "anchor_outbox_id": e.identity}
                else:
                    prompt.payload = {**prompt.payload, "prompt_visibility": {**prompt.payload["prompt_visibility"], "part_id": "f" * 32}}
        return settings

    monkeypatch.setattr(sources.settings, "get", damage_prompt)
    before = len(e.channel.bot.request.calls)
    e.clock.advance(10)
    await e.sender.tick()
    await e.sender.drain()
    assert changed
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, identity)).state == "skipped"
    assert not [call for call in e.channel.bot.request.calls[before:] if call.endpoint == "sendMessage"]


@pytest.mark.asyncio
async def test_prompt_closed_during_sdk_response_is_not_revived_by_ack(work_delivery):
    e = work_delivery
    install(e)
    await dispatch(e, 801)
    identity = await open_view(e, await selected(e, "hi_answer"))
    await clear_old(e)
    prompts = Prompts(e.channel._services.callbacks)
    guard = await guard_for(e)
    original = e.sender.deliver_part

    async def close_after_sdk(**kwargs):
        receipt = await original(**kwargs)
        await prompts.close(e.link, e.link.chat_id, None, reason="command", guard=guard)
        return receipt

    e.sender.deliver_part = close_after_sdk
    await dispatch(e, 802)
    assert await prompts.current(e.link, e.link.chat_id, None, guard=guard) is None
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "sent"
        prompt = await reader.get(TelegramCallbackRow, row.payload["work_request_basis"]["prompt_id"])
        assert prompt.consumed_at and prompt.closed_reason == "command"
        assert prompt.message_id is None and prompt.payload["prompt_visibility"]["state"] == "pending"
