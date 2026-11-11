"""Refreshing an input-owned confirmation retains its text and operation lineage."""

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_work_callback_dispatch import install_dispatch, press_work
from test_telegram_work_control_admission import admissions, stage
from test_telegram_work_interaction_delivery import selected
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_refusal_recovery import comment
from test_telegram_work_retained_drafts import draft
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.work_outcome_delivery import WorkOutcomePlans
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["changes_confirm", "changes_open"])
async def test_changed_retained_note_press_refreshes_full_review_without_submitting(work_delivery, monkeypatch, kind):
    e = work_delivery
    request, _inbox, origin = await draft(e, monkeypatch)
    control = await selected(e, kind)
    old_identity = e.identity
    install_dispatch(e, WorkOutcomePlans(e.plans, admissions(e).acts))
    await comment(e, request)
    await press_work(e, control, message_id=803)
    async with e.db.sessions() as reader:
        row = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.id != old_identity, TelegramOutboxRow.payload["work_request_basis"]["input"]["operation_id"].as_string() == origin.operation_ids[0]))
        assert row is not None
        assert row.payload["work_request_basis"]["interaction"]["note"] == origin.route_basis["work_input"]["text"]
        assert row.payload["work_request_basis"]["source"]["seen"] != control.payload["seen"]
        assert (await reader.get(TelegramCallbackRow, control.id)).consumed_at is not None
        assert not (await reader.get(TelegramInboundMarkRow, (origin.link_id, origin.message_id, origin.chat_id))).route_complete
        assert not list(await reader.scalars(select(TelegramActRow)))
        e.identity = row.id
    # A full new preview must be acknowledged before its confirmation can act.
    fresh = await selected(e, "changes_confirm")
    from app.telegram_bots.callbacks import CallbackRefused

    with pytest.raises(CallbackRefused):
        await stage(e, admissions(e), fresh)
    await deliver_refreshed(e, 804)
    fresh = await selected(e, "changes_confirm")
    saved = await stage(e, admissions(e), fresh)
    assert saved.body["note"] == origin.route_basis["work_input"]["text"]
    assert (await admissions(e).acts.check(saved.id)).state == "done"


@pytest.mark.asyncio
async def test_unchanged_known_refusal_retries_the_same_typed_confirmation(work_delivery, monkeypatch):
    from test_telegram_work_callback_recovery import typed_refused

    e = work_delivery
    controls, _request, callback, captured, first = await typed_refused(e, monkeypatch)
    install_dispatch(e, WorkOutcomePlans(e.plans, controls.acts))
    await press_work(e, callback, message_id=803)
    async with e.db.sessions() as reader:
        saved = await reader.get(TelegramActRow, first.id)
        assert saved.state == "done" and saved.attempts == first.attempts + 1
        assert saved.body["note"] == captured.route_basis["work_input"]["text"]
        assert len(list(await reader.scalars(select(TelegramActRow)))) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("journal", ["unknown", "missing", "advanced", "body"])
async def test_input_refresh_never_reconstructs_unresolved_or_changed_journal(work_delivery, monkeypatch, journal):
    from test_telegram_work_callback_recovery import typed_refused
    from test_telegram_work_interaction_delivery import guard_for

    from app.telegram_bots.callbacks import CallbackRefused
    from app.telegram_bots.work_input_refresh import WorkInputRefresh

    e = work_delivery
    controls, request, callback, captured, first = await typed_refused(e, monkeypatch, unknown=journal == "unknown")
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramActRow, first.id)
        if journal == "missing":
            await writer.delete(row)
        elif journal == "advanced":
            row.state, row.reason = "waiting", None
            row.attempts += 1
        elif journal == "body":
            row.body = {**row.body, "note": "An altered decision"}
    await comment(e, request)
    from app.telegram_bots.producer import ProductSourceRetired

    with pytest.raises((CallbackRefused, ProductSourceRetired)):
        await WorkInputRefresh(controls).refresh(e.link, callback.id, chat_id=e.link.chat_id, message_id=803, message_thread_id=None, guard=await guard_for(e))
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramCallbackRow, callback.id)).consumed_at is None
        assert not (await reader.get(TelegramInboundMarkRow, (captured.link_id, captured.message_id, captured.chat_id))).route_complete
        assert not list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.id != e.identity, TelegramOutboxRow.payload["work_request_basis"]["input"]["operation_id"].as_string() == captured.operation_ids[0])))


async def deliver_refreshed(e, message_id):
    from datetime import timedelta

    from test_telegram_work_typing_prompts import clear_old

    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, e.identity)
        deadline = row.not_before
        row.not_before = e.clock() + timedelta(hours=1)
    await clear_old(e)
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramOutboxRow, e.identity)).not_before = deadline
    await dispatch(e, message_id)


@pytest.mark.asyncio
async def test_changed_typed_refusal_requires_full_new_ack_and_another_press(work_delivery, monkeypatch):
    from test_telegram_work_callback_recovery import typed_refused
    from test_telegram_work_interaction_delivery import guard_for

    from app.telegram_bots.callbacks import CallbackRefused
    from app.telegram_bots.work_input_refresh import WorkInputRefresh

    e = work_delivery
    controls, request, callback, captured, first = await typed_refused(e, monkeypatch)
    await comment(e, request)
    old_body = first.body
    e.identity = await WorkInputRefresh(controls).refresh(e.link, callback.id, chat_id=e.link.chat_id, message_id=803, message_thread_id=None, guard=await guard_for(e))
    with pytest.raises(CallbackRefused):
        await stage(e, controls, await selected(e, "decide_confirm"))
    await deliver_refreshed(e, 804)
    assert not e.dispatch_errors, e.dispatch_errors
    fresh = await stage(e, controls, await selected(e, "decide_confirm"))
    assert fresh.id != first.id and fresh.body["note"] == captured.route_basis["work_input"]["text"]
    assert (await controls.acts.check(fresh.id)).state == "done"
    from app.telegram_bots.work_actions import same_body

    previous = await controls.acts._work_record(first.id)
    assert previous.state == "conflict" and same_body(previous.body, old_body)


@pytest.mark.asyncio
async def test_changes_conflict_at_command_keeps_note_for_new_explicit_confirmation(work_delivery, monkeypatch):
    e = work_delivery
    request, _inbox, origin = await draft(e, monkeypatch)
    control = await selected(e, "changes_confirm")
    old_identity = e.identity
    controls = admissions(e)
    install_dispatch(e, WorkOutcomePlans(e.plans, controls.acts))
    original = controls.acts.work.command
    calls = []

    async def change_before_commit(*args, **kwargs):
        calls.append(kwargs["request"].operation_id)
        if len(calls) == 1:
            await comment(e, request)
        return await original(*args, **kwargs)

    monkeypatch.setattr(controls.acts.work, "command", change_before_commit)
    await press_work(e, control, message_id=803)
    async with e.db.sessions() as reader:
        act = await reader.get(TelegramActRow, control.id)
        assert act.state == "conflict" and act.attempts == 1
        row = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.id != old_identity, TelegramOutboxRow.payload["work_request_basis"]["input"]["operation_id"].as_string() == origin.operation_ids[0]))
        assert row is not None
        e.identity = row.id
    assert calls == [control.id]
    await deliver_refreshed(e, 804)
    fresh = await selected(e, "changes_confirm")
    await press_work(e, fresh, message_id=804, update_id=2902)
    async with e.db.sessions() as reader:
        saved = await reader.get(TelegramActRow, fresh.id)
        assert saved.state == "done" and saved.body["note"] == origin.route_basis["work_input"]["text"]
    assert calls == [control.id, fresh.id]


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["done", "text", "raw", "pending"])
async def test_final_guard_input_drift_rolls_back_entire_refresh(work_delivery, monkeypatch, damage):
    from test_telegram_work_callback_recovery import typed_refused
    from test_telegram_work_interaction_delivery import guard_for

    from app.telegram_bots.producer import DeliveryRetry, ProductSourceRetired
    from app.telegram_bots.work_input_refresh import WorkInputRefresh

    e = work_delivery
    controls, request, callback, captured, first = await typed_refused(e, monkeypatch)
    await comment(e, request)
    base = await guard_for(e)
    changed = []

    class Drift:
        def current(self):
            return base.current()

        async def check(self, writer):
            current = await base.check(writer)
            fresh = await writer.scalar(select(TelegramOutboxRow.id).where(TelegramOutboxRow.id != e.identity, TelegramOutboxRow.payload["work_request_basis"]["input"]["operation_id"].as_string() == captured.operation_ids[0]))
            if fresh is not None and not changed:
                changed.append(True)
                mark = await writer.get(TelegramInboundMarkRow, (captured.link_id, captured.message_id, captured.chat_id))
                if damage == "done":
                    mark.route_complete = True
                elif damage == "text":
                    mark.route_basis = {**mark.route_basis, "work_input": {**mark.route_basis["work_input"], "text": "Changed during the last guard await"}}
                elif damage == "raw":
                    mark.sanitized_input = {"text": "A different accepted message"}
                else:
                    saved = await writer.get(TelegramActRow, first.id)
                    saved.state, saved.reason, saved.cancel_requested_at = "pending", None, None
                await writer.flush()
            return current

    with pytest.raises((ProductSourceRetired, DeliveryRetry)):
        await WorkInputRefresh(controls).refresh(e.link, callback.id, chat_id=e.link.chat_id, message_id=803, message_thread_id=None, guard=Drift())
    assert changed
    async with e.db.sessions() as reader:
        saved = await reader.get(TelegramActRow, first.id)
        assert saved.state == "pending" and saved.reason == "attempt-unresolved" and saved.cancel_requested_at is None
        assert (await reader.get(TelegramCallbackRow, callback.id)).consumed_at is None
        assert not list(await reader.scalars(select(TelegramOutboxRow.id).where(TelegramOutboxRow.id != e.identity, TelegramOutboxRow.payload["work_request_basis"]["input"]["operation_id"].as_string() == captured.operation_ids[0])))
