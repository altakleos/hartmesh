"""Acknowledged Work buttons own complete, durable interaction previews."""

from datetime import UTC, datetime

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_bot_adapter import message
from test_telegram_work_actions import management_request
from test_telegram_work_preview_recovery import dispatch, install
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow, TelegramPersonSettingsRow


async def guard_for(e):
    owner = await e.live.authority.prepare_live_owner(e.link.owner_user_id)
    return e.db.links.publication_guard(e.link, check_live_owner=owner, check_attempt_current=lambda: True)


async def selected(e, kind):
    async with e.db.sessions() as reader:
        parent = await reader.get(TelegramOutboxRow, e.identity)
        return await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == parent.parts[-1]["callback_group_id"], TelegramCallbackRow.kind == kind))


async def open_view(e, callback, **kwargs):
    from app.telegram_bots.work_interaction_delivery import WorkInteractionPlans

    planner = WorkInteractionPlans(e.plans)
    return await planner.open(e.link, callback.id, chat_id=e.link.chat_id, message_id=callback.message_id, message_thread_id=None, guard=await guard_for(e), **kwargs)


@pytest.mark.asyncio
@pytest.mark.parametrize("purpose,kind,view", [("information", "hi_cant_open", "cannot"), ("review", "accept_open", "accept")])
async def test_acknowledged_first_tap_creates_reply_and_never_an_act(work_delivery, purpose, kind, view):
    e = work_delivery
    install(e)
    if purpose == "review":
        _record, request, _seen = await management_request(e, purpose)
        async with e.db.sessions.begin() as writer:
            (await writer.get(TelegramOutboxRow, e.identity)).state = "skipped"
        e.identity = await e.plans.plan(e.link, request_id=request["id"], dedupe_key="review")
    await dispatch(e, 801)
    callback = await selected(e, kind)
    identity = await open_view(e, callback)
    assert await open_view(e, callback) == identity
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        basis = row.payload["work_request_basis"]
        assert row.class_ == "reply" and row.state == "pending"
        assert basis["interaction"] == {"view": view}
        assert basis["origin"]["callback_id"] == callback.id
        assert basis["cleanup_id"]
        assert not list(await reader.scalars(select(TelegramActRow)))
        controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"])))
        assert controls and all(control.payload["presentation_pending"] and control.message_id is None for control in controls)
    # Direct replies survive disabled proactive notifications, but must wait for
    # the exact old keyboard's acknowledged removal.
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)).notify = "none"
    e.channel.bot.request.enqueue("editMessageReplyMarkup", body={"ok": True, "result": True})
    e.clock.advance(10)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    await dispatch(e, 802)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "sent", (row.state, row.failed_reason, e.dispatch_errors)
        assert all(control.message_id == 802 and not control.payload["presentation_pending"] for control in await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"])))
    assert not e.dispatch_errors


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["unacknowledged", "message", "seen", "consumed", "source"])
async def test_first_tap_refuses_unproven_or_stale_origin(work_delivery, damage):
    from app.telegram_bots.callbacks import CallbackRefused
    from app.telegram_bots.producer import PermanentDeliveryError
    from deerflow.agent_instances.contract import AgentConflict

    e = work_delivery
    install(e)
    await dispatch(e, 801)
    callback = await selected(e, "hi_cant_open")
    async with e.db.sessions.begin() as writer:
        parent = await writer.get(TelegramOutboxRow, e.identity)
        row = await writer.get(TelegramCallbackRow, callback.id)
        if damage == "unacknowledged":
            parent.parts = [{**part, "state": "pending", "message_ids": []} for part in parent.parts]
        elif damage == "message":
            row.message_id = 999
        elif damage == "seen":
            row.payload = {**row.payload, "seen": {**row.payload["seen"], "revision": 999}}
        elif damage == "consumed":
            row.consumed_at, row.closed_reason = datetime.now(UTC), "superseded"
        else:
            parent.payload = {key: value for key, value in parent.payload.items() if key != "work_request_basis"}
    with pytest.raises((CallbackRefused, PermanentDeliveryError, AgentConflict)):
        await open_view(e, callback)
    async with e.db.sessions() as reader:
        assert not list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key.startswith("notice:work-interaction:"))))


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["origin", "receipt", "seen", "claim", "grant"])
async def test_final_sdk_snapshot_rechecks_origin_and_current_authority(work_delivery, monkeypatch, damage):
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow

    e = work_delivery
    install(e)
    await dispatch(e, 801)
    callback = await selected(e, "hi_cant_open")
    identity = await open_view(e, callback)
    e.channel.bot.request.enqueue("editMessageReplyMarkup", body={"ok": True, "result": True})
    e.clock.advance(10)
    await e.sender.tick()
    await e.sender.drain()
    sources = e.channel._services.work_request_sources
    original = sources.settings.get
    changed = []

    async def withdraw(*args, **kwargs):
        settings = await original(*args, **kwargs)
        if not changed:
            changed.append(True)
            async with e.db.sessions.begin() as writer:
                origin = await writer.get(TelegramCallbackRow, callback.id)
                parent = await writer.get(TelegramOutboxRow, e.identity)
                row = await writer.get(TelegramOutboxRow, identity)
                if damage == "origin":
                    origin.closed_reason = "superseded"
                elif damage == "receipt":
                    parent.parts = [{**part, "message_ids": [999]} for part in parent.parts]
                elif damage == "seen":
                    origin.payload = {**origin.payload, "seen": {**origin.payload["seen"], "revision": 999}}
                elif damage == "claim":
                    row.claim_id = "new-owner"
                else:
                    (await writer.get(AgentInstanceGrantRow, (e.link.agent_ref, e.link.owner_user_id))).permissions = 1
        return settings

    monkeypatch.setattr(sources.settings, "get", withdraw)
    before = len(e.channel.bot.request.calls)
    e.clock.advance(10)
    await e.sender.tick()
    await e.sender.drain()
    assert changed and e.dispatch_errors
    assert not [call for call in e.channel.bot.request.calls[before:] if call.endpoint == "sendMessage"]


@pytest.mark.asyncio
async def test_unfinished_interaction_retains_its_origin_receipts(work_delivery):
    from datetime import timedelta

    from app.telegram_bots.retention import sweep_rows

    e = work_delivery
    install(e)
    await dispatch(e, 801)
    callback = await selected(e, "hi_cant_open")
    identity = await open_view(e, callback)
    then = e.clock.now - timedelta(days=60)
    async with e.db.sessions.begin() as writer:
        parent = await writer.get(TelegramOutboxRow, e.identity)
        parent.closed_at = then
        for row in await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == callback.group_id)):
            row.consumed_at = row.created_at = then
    await sweep_rows(e.db.sessions, e.clock.now, callback_max_age_days=30)
    await e.channel._services.callbacks.sweep(e.clock.now)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramOutboxRow, e.identity) is not None
        assert await reader.get(TelegramCallbackRow, callback.id) is not None
        assert (await reader.get(TelegramOutboxRow, identity)).state == "pending"


@pytest.mark.asyncio
async def test_protected_first_tap_keeps_full_reply_protected(work_delivery):
    from test_telegram_work_preview_recovery import concealed, reveal

    e = work_delivery
    install(e)
    show = await concealed(e)
    await reveal(e, show)
    message(e.channel.bot.request, endpoint="editMessageText", identity=821)
    e.clock.advance(10)
    await e.sender.tick()
    await e.sender.drain()
    callback = await selected(e, "hi_cant_open")
    identity = await open_view(e, callback)
    e.channel.bot.request.enqueue("editMessageReplyMarkup", body={"ok": True, "result": True})
    e.clock.advance(10)
    await e.sender.tick()
    await e.sender.drain()
    await dispatch(e, 822)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "sent", (row.failed_reason, e.dispatch_errors)
        assert row.parts[0]["protect_content"]
    calls = [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
    assert "Which guide?" in calls[-1].parameters["text"] and calls[-1].parameters["protect_content"]


@pytest.mark.asyncio
async def test_multipart_confirmation_arms_only_after_every_ack_and_retry(work_delivery):
    from app.telegram_bots.callbacks import CallbackRefused

    e = work_delivery
    install(e)
    callback = await multipart_source(e)
    identity = await open_view(e, callback)
    e.channel.bot.request.enqueue("editMessageReplyMarkup", body={"ok": True, "result": True})
    e.clock.advance(10)
    await e.sender.tick()
    await e.sender.drain()
    await dispatch(e, 901)
    e.channel.bot.request.enqueue("sendMessage", status=503, body={"ok": False, "error_code": 503, "description": "unavailable"})
    e.clock.advance(10)
    await e.sender.tick()
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "pending" and row.parts[0]["state"] == "sent", (row.state, row.parts, e.dispatch_errors)
        group_id = row.parts[-1]["callback_group_id"]
        controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == group_id)))
        assert all(control.message_id is None and control.payload["presentation_pending"] for control in controls)
        unsent = sum(part["state"] != "sent" for part in row.parts)
    guard = await guard_for(e)
    async with e.channel._services.callbacks.writer(e.link, guard) as writer:
        with pytest.raises(CallbackRefused):
            await e.channel._services.callbacks.lock_current(e.link, controls[0].id, chat_id=e.link.chat_id, message_id=901, message_thread_id=None, guard=guard, session=writer)
    e.clock.advance(300)
    for index in range(unsent):
        await dispatch(e, 902 + index)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "sent", (row.failed_reason, e.dispatch_errors)
        assert row.parts[0]["message_ids"] == [901]
        assert all(control.message_id == 901 + unsent and not control.payload["presentation_pending"] for control in await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == group_id)))
        assert not list(await reader.scalars(select(TelegramActRow)))


@pytest.mark.asyncio
async def test_decision_preview_uses_saved_choice_without_recording_decision(work_delivery):
    from uuid import uuid4

    from deerflow.agent_instances.human_input_contract import CreateRequest

    e = work_delivery
    install(e)
    from deerflow.agent_instances.work_contract import DelegateWork

    work = await e.human.work.delegate(actor=e.actor, instance_id=e.instance.id, request=DelegateWork(operation_id=uuid4().hex, objective="Choose a plan", success_criteria="A decision"))
    request = await e.human.create(
        actor=e.actor,
        instance_id=e.instance.id,
        work_id=work["id"],
        request=CreateRequest(
            operation_id=uuid4().hex,
            expected_work_revision=work["revision"],
            expected_assignment_revision=work["assignment_revision"],
            purpose="decision",
            question="Which option?",
            reason="Choose a direction",
            expected_response="A decision",
            recipient_id=e.link.owner_user_id,
            choices=["Choose <A>", "Choose B"],
        ),
    )
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramOutboxRow, e.identity)).state = "skipped"
    e.identity = await e.plans.plan(e.link, request_id=request["id"], dedupe_key="decision-preview")
    await dispatch(e, 801)
    callback = await selected(e, "decide_choice")
    identity = await open_view(e, callback)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.payload["work_request_basis"]["interaction"] == {"view": "decision", "decision": callback.payload["decision"], "note": None}
        assert not list(await reader.scalars(select(TelegramActRow)))
    assert (await e.human.get(actor=e.actor, request_id=request["id"]))["state"] == "pending"


@pytest.mark.asyncio
@pytest.mark.parametrize("when", ["queued", "final_snapshot"])
async def test_locale_drift_retires_unsubmitted_reply_without_retry_loop(work_delivery, monkeypatch, when):
    from app.telegram_bots.work_request_source import WorkRequestSources

    e = work_delivery
    install(e)
    await dispatch(e, 801)
    callback = await selected(e, "hi_cant_open")
    identity = await open_view(e, callback)
    e.channel.bot.request.enqueue("editMessageReplyMarkup", body={"ok": True, "result": True})
    e.clock.advance(10)
    await e.sender.tick()
    await e.sender.drain()

    async def language():
        async with e.db.sessions.begin() as writer:
            (await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)).locale = "zh-CN"

    # Reconstruct the source reader as after a process restart; the draft and
    # consumed origin remain durable, but old-language text must not loop.
    sources = WorkRequestSources(reader=e.reader, outbox=e.channel._services.outbox, callbacks=e.channel._services.callbacks)
    e.channel._services.work_request_sources = sources
    if when == "queued":
        await language()
    else:
        original = sources.settings.get

        async def changed(*args, **kwargs):
            result = await original(*args, **kwargs)
            await language()
            return result

        monkeypatch.setattr(sources.settings, "get", changed)
    before = len(e.channel.bot.request.calls)
    e.clock.advance(10)
    await e.sender.tick()
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "skipped" and row.skip_reason == "superseded", (row.state, e.dispatch_errors)
        assert row.payload["work_request_basis"]["interaction"] == {"view": "cannot"}
    assert not [call for call in e.channel.bot.request.calls[before:] if call.endpoint == "sendMessage"]


async def multipart_source(e):
    from uuid import uuid4

    from deerflow.agent_instances.human_input_contract import Respond

    _record, request, _seen = await management_request(e, "review")
    for number in range(3):
        await e.human.respond(actor=e.actor, request_id=request["id"], request=Respond(operation_id=uuid4().hex, expected_request_revision=1, expected_assignment_revision=1, text=f"Comment {number}: " + "All details. " * 250))
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramOutboxRow, e.identity)).state = "skipped"
    e.identity = await e.plans.plan(e.link, request_id=request["id"], dedupe_key="multipart-review")
    async with e.db.sessions() as reader:
        count = len((await reader.get(TelegramOutboxRow, e.identity)).parts)
    assert count > 2
    for index in range(count):
        await dispatch(e, 801 + index)
    callback = await selected(e, "accept_open")
    return callback


@pytest.mark.asyncio
async def test_expired_multipart_confirmation_retires_before_first_content(work_delivery):
    from datetime import timedelta

    e = work_delivery
    install(e)
    callback = await multipart_source(e)
    identity = await open_view(e, callback)
    e.channel.bot.request.enqueue("editMessageReplyMarkup", body={"ok": True, "result": True})
    e.clock.advance(10)
    await e.sender.tick()
    await e.sender.drain()
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, identity)
        assert not row.parts[0].get("callback_group_id")
        for control in await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"])):
            control.created_at = e.clock.now - timedelta(days=e.channel._services.callbacks.config.callbacks.max_age_days + 1)
    before = len(e.channel.bot.request.calls)
    e.clock.advance(10)
    await e.sender.tick()
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "skipped" and row.skip_reason == "superseded", (row.state, e.dispatch_errors)
    assert not [call for call in e.channel.bot.request.calls[before:] if call.endpoint == "sendMessage"]
