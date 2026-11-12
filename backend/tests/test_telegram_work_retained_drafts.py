"""Retained review notes keep their age and survive explicit draft navigation."""

from datetime import timedelta

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_work_input_capture import accept, capture, prompt
from test_telegram_work_input_previews import previews
from test_telegram_work_interaction_delivery import selected
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_refusal_recovery import comment
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow


async def draft(e, monkeypatch, *, acknowledged=True):
    from app.telegram_bots import inbox

    request, prompt_id, _seen = await prompt(e)
    monkeypatch.setattr(inbox, "_now", e.clock)
    e.channel._services.callbacks.clock = e.clock
    e.clock.advance(30)
    accepted, mark = await accept(e)
    captured = await capture(e, accepted, mark, prompt_id)
    await comment(e, request)
    e.identity = await previews(e, accepted, mark)
    if acknowledged:
        await dispatch(e, 803)
    return request, accepted, captured


@pytest.mark.asyncio
async def test_review_note_controls_expire_from_original_input_even_after_refresh(work_delivery, monkeypatch):
    from app.telegram_bots.callbacks import CallbackRefused, _aware

    e = work_delivery
    request, inbox, origin = await draft(e, monkeypatch)
    e.clock.advance(20 * 3600)
    await comment(e, request)
    e.identity = await previews(e, inbox, origin)
    from test_telegram_work_typing_prompts import clear_old

    # The old keyboard removal and fresh preview are separate durable deliveries.
    async with e.db.sessions.begin() as writer:
        fresh = await writer.get(TelegramOutboxRow, e.identity)
        deadline = fresh.not_before
        fresh.not_before = e.clock() + timedelta(hours=1)
    await clear_old(e)
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramOutboxRow, e.identity)).not_before = deadline
    await dispatch(e, 804)
    control = await selected(e, "changes_confirm")
    assert _aware(control.expires_at) == origin.accepted_at + timedelta(hours=24)
    e.clock.now = origin.accepted_at + timedelta(hours=24, seconds=-1)
    e.channel._services.callbacks._usable(control)
    e.clock.now = origin.accepted_at + timedelta(hours=24, seconds=1)
    with pytest.raises(CallbackRefused):
        e.channel._services.callbacks._usable(control)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, e.identity)
        controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"])))
        assert {control.kind for control in controls} == {"changes_confirm", "changes_open", "dismiss"}
        for control in controls:
            with pytest.raises(CallbackRefused):
                e.channel._services.callbacks._usable(control)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["changes_open", "dismiss"])
async def test_draft_navigation_settles_old_input_without_submitting(work_delivery, monkeypatch, kind):
    from test_telegram_work_callback_dispatch import install_dispatch, press_work
    from test_telegram_work_control_admission import admissions
    from test_telegram_work_typing_prompts import clear_old

    from app.telegram_bots.work_outcome_delivery import WorkOutcomePlans
    from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramInboundMarkRow

    e = work_delivery
    _request, inbox, origin = await draft(e, monkeypatch)
    old_identity = e.identity
    control = await selected(e, kind)
    install_dispatch(e, WorkOutcomePlans(e.plans, admissions(e).acts))
    await press_work(e, control, message_id=803)
    async with e.db.sessions() as reader:
        saved = await reader.get(TelegramInboundMarkRow, (origin.link_id, origin.message_id, origin.chat_id))
        assert saved.route_complete
        assert saved.route_basis["work_input"]["text"] == origin.route_basis["work_input"]["text"]
        assert not list(await reader.scalars(select(TelegramActRow)))
        old = await reader.get(TelegramOutboxRow, old_identity)
        controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == old.parts[-1]["callback_group_id"])))
        assert all(row.consumed_at is not None for row in controls)
        fresh = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == "notice:work-interaction:" + control.id))
        if kind == "changes_open":
            assert fresh.payload["work_request_basis"]["interaction"] == {"view": "prompt", "purpose": "changes"}
            e.identity = fresh.id
        else:
            assert fresh is None
    await clear_old(e)
    if kind == "changes_open":
        await dispatch(e, 804)
        async with e.db.sessions() as reader:
            fresh = await reader.get(TelegramOutboxRow, e.identity)
            prompt_row = await reader.get(TelegramCallbackRow, fresh.payload["work_request_basis"]["prompt_id"])
            assert prompt_row.message_id == 804 and prompt_row.consumed_at is None
    # A replay cannot resurrect the old note after either navigation gesture.
    from app.telegram_bots.callbacks import CallbackRefused
    from app.telegram_bots.producer import ProductSourceRetired

    with pytest.raises((CallbackRefused, ProductSourceRetired)):
        await previews(e, inbox, origin)


@pytest.mark.asyncio
async def test_retained_note_preview_explains_the_review_changed(work_delivery, monkeypatch):
    from app.telegram_bots.render import render

    e = work_delivery
    await draft(e, monkeypatch)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, e.identity)
        html = "".join(part["html"] for part in row.parts)
        assert html.startswith(render("stale.changed_resend", "en") + "\n\n")


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["changes_open", "dismiss"])
@pytest.mark.parametrize("blocked", ["reserved", "in_flight", "lifetime"])
async def test_draft_navigation_refuses_when_original_ownership_is_unsettled(work_delivery, monkeypatch, kind, blocked):
    from uuid import uuid4

    from test_telegram_work_control_admission import admissions, stage
    from test_telegram_work_interaction_delivery import guard_for

    from app.telegram_bots.callbacks import CallbackRefused
    from app.telegram_bots.producer import DeliveryRetry, ProductSourceRetired
    from app.telegram_bots.work_retained_drafts import WorkRetainedDrafts
    from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow

    e = work_delivery
    _request, _inbox, origin = await draft(e, monkeypatch)
    control = await selected(e, kind)
    guard = await guard_for(e)
    if blocked == "reserved":
        await stage(e, admissions(e), await selected(e, "changes_confirm"))
    elif blocked == "in_flight":
        async with e.db.sessions.begin() as writer:
            (await writer.get(TelegramOutboxRow, e.identity)).claim_id = uuid4().hex
    else:
        from dataclasses import replace

        guard = replace(guard, _attempt=lambda: False)
    with pytest.raises((CallbackRefused, ProductSourceRetired, DeliveryRetry, ValueError)):
        await WorkRetainedDrafts(e.plans).navigate(e.link, control.id, chat_id=e.link.chat_id, message_id=803, message_thread_id=None, guard=guard)
    async with e.db.sessions() as reader:
        saved = await reader.get(TelegramInboundMarkRow, (origin.link_id, origin.message_id, origin.chat_id))
        assert not saved.route_complete
        current = await reader.get(TelegramCallbackRow, control.id)
        assert current.consumed_at is None


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["sdk", "admission"])
async def test_coherent_expiry_rewrite_cannot_extend_original_note(work_delivery, monkeypatch, boundary):
    from copy import deepcopy

    from test_telegram_work_control_admission import admissions, stage

    from app.telegram_bots.callbacks import _aware
    from app.telegram_bots.producer import ProductSourceRetired

    e = work_delivery
    await draft(e, monkeypatch, acknowledged=boundary == "admission")

    async def extend():
        async with e.db.sessions.begin() as writer:
            parent = await writer.get(TelegramOutboxRow, e.identity)
            payload = deepcopy(parent.payload)
            controls = list(await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == parent.parts[-1]["callback_group_id"])))
            for row in controls:
                row.expires_at = _aware(row.expires_at) + timedelta(hours=1)
                row.created_at = _aware(row.created_at) + timedelta(hours=1)
            for keyboard in (payload["product_keyboard_basis"], payload["work_request_basis"]["keyboard"]):
                for line in keyboard:
                    for control in line:
                        control["expires_at"] = _aware(controls[0].expires_at).isoformat()
            parent.payload = payload

    if boundary == "admission":
        await extend()
        with pytest.raises(ProductSourceRetired):
            await stage(e, admissions(e), await selected(e, "changes_confirm"))
    else:
        settings = e.channel._services.work_request_sources.settings
        original = settings.get
        changed = []

        async def mutate(*args, **kwargs):
            value = await original(*args, **kwargs)
            if not changed:
                changed.append(True)
                await extend()
            return value

        monkeypatch.setattr(settings, "get", mutate)
        before = len(e.channel.bot.request.calls)
        e.clock.advance(10)
        await e.sender.tick()
        await e.sender.drain()
        assert changed
        # Replacing the claimed payload first defers that claim. On the next
        # read the coherent new descriptors still fail the original input proof.
        e.clock.advance(10)
        await e.sender.tick()
        await e.sender.drain()
        assert not [call for call in e.channel.bot.request.calls[before:] if call.endpoint == "sendMessage"]
        async with e.db.sessions() as reader:
            assert (await reader.get(TelegramOutboxRow, e.identity)).state == "skipped"
