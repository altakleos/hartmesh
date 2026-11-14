"""Original typed decision input survives combined confirmation and activation."""

from hashlib import sha256

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_inbox import incoming
from test_telegram_work_actions import management_request
from test_telegram_work_callback_dispatch import press_work
from test_telegram_work_interaction_delivery import open_view, selected
from test_telegram_work_preview_recovery import dispatch, install
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_telegram_work_start import prepare_startable
from test_telegram_work_typed_input import install_inputs
from test_telegram_work_typing_prompts import clear_old
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramInboundMarkRow, TelegramOutboxRow


async def typed_confirmation(e, monkeypatch):
    install(e)
    await prepare_startable(e, monkeypatch)
    e.reader.composites = True
    record, request, _seen = await management_request(e, "decision")
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramOutboxRow, e.identity)).state = "skipped"
    e.identity = await e.plans.plan(e.link, request_id=request["id"], dedupe_key="combined-typed-decision")
    await dispatch(e, 801)
    e.identity = await open_view(e, await selected(e, "decide_own"))
    await clear_old(e)
    await dispatch(e, 802)
    plans, _inputs = install_inputs(e)
    update = incoming(message=900, update_id=1900, account=e.link.chat_id, reply=802)
    assert await e.channel.handle_update(update)
    await e.channel._drain_ingress()
    async with e.db.sessions() as reader:
        row = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_request_basis"]["input"]["message_id"].as_integer() == 900))
        assert row is not None and row.state == "pending"
        e.identity = row.id
    await dispatch(e, 804)
    assert not e.dispatch_errors, e.dispatch_errors
    button = await selected(e, "decide_confirm_resume")
    assert button is not None
    return record, request, button, update, plans


@pytest.mark.asyncio
async def test_actual_typed_decision_retains_input_until_its_combined_child_finishes(work_delivery, monkeypatch):
    e = work_delivery
    record, _request, button, update, plans = await typed_confirmation(e, monkeypatch)
    await press_work(e, button, message_id=804)
    async with e.db.sessions() as reader:
        parent = await reader.get(TelegramActRow, button.id)
        assert parent.kind == "decide" and parent.state == "done"
        assert parent.body["note"] == update.message.text
        assert parent.continuation["request_activation"]["input"]["kind"] == "typed"
        assert parent.continuation["activation_continuation"]["phase"] == "done"
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
        assert mark.sanitized_input["text"] == update.message.text and mark.route_kind == "work_input"
        child = await reader.get(TelegramActRow, sha256((button.id + ":activate").encode()).hexdigest()[:32])
        assert child.state == "done" and child.work_id == record["id"]
    assert len(e.launches) == 1 and plans.acts is e.channel._services.work_callbacks.acts


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["route", "missing_prompt", "prompt_ack", "input"])
async def test_typed_resume_keeps_original_prompt_and_input_at_final_service_commit(work_delivery, monkeypatch, damage):
    from unittest.mock import AsyncMock

    from app.gateway import work_execution
    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow

    e = work_delivery
    _record, _request, button, _update, plans = await typed_confirmation(e, monkeypatch)
    original = work_execution.activate_work
    monkeypatch.setattr(work_execution, "activate_work", AsyncMock(side_effect=RuntimeError("Host admission unavailable")))
    await press_work(e, button, message_id=804)
    parent = await plans.acts._work_record(button.id)
    assert parent.state == "done" and not e.launches
    e.clock.advance(plans.acts.outbox.config.acts.retry_seconds)
    entered = []

    async def retire(*args, **kwargs):
        entered.append(True)
        async with e.db.sessions.begin() as writer:
            mark = await writer.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id))
            if damage == "route":
                mark.route_target_id = "f" * 32
            elif damage == "missing_prompt":
                await writer.delete(await writer.get(TelegramCallbackRow, mark.route_basis["work_input"]["prompt_id"]))
            elif damage == "prompt_ack":
                prompt = await writer.get(TelegramOutboxRow, mark.route_basis["work_input"]["source_outbox_id"])
                prompt.parts = [{**part, "message_ids": []} for part in prompt.parts]
            else:
                mark.sanitized_input = {**mark.sanitized_input, "text": "Changed after the wait"}
        return await original(*args, **kwargs)

    monkeypatch.setattr(work_execution, "activate_work", retire)
    await plans.acts.resume_continuation(button.id)
    assert entered and not e.launches
    assert (await plans.acts._work_record(parent.id)).state == "done"


@pytest.mark.asyncio
async def test_changed_combined_decision_transfers_only_the_refused_primary_to_new_preview(work_delivery, monkeypatch):
    from datetime import timedelta

    from test_telegram_work_control_admission import stage
    from test_telegram_work_interaction_delivery import guard_for
    from test_telegram_work_refusal_recovery import comment

    from app.telegram_bots.work_input_refresh import WorkInputRefresh
    from app.telegram_bots.work_outcome_transfer import transferred
    from app.telegram_bots.work_request_continuation_retention import request_dependencies

    e = work_delivery
    _record, request, button, update, plans = await typed_confirmation(e, monkeypatch)
    controls = e.channel._services.work_callbacks.controls
    saved = await stage(e, controls, button)
    await comment(e, request)
    refused = await plans.acts.check(saved.id)
    assert refused.state == "conflict" and not e.launches
    source = refused.continuation["request_activation"]
    fresh = await WorkInputRefresh(controls).refresh(e.link, button.id, chat_id=e.link.chat_id, message_id=804, message_thread_id=None, guard=await guard_for(e))
    assert fresh is not None
    async with e.db.sessions() as reader:
        old = await reader.get(TelegramActRow, saved.id)
        assert transferred(old) and old.continuation["request_activation"] == source
        assert old.continuation["activation_continuation"]["child_body"] is None
        assert await reader.get(TelegramActRow, sha256((saved.id + ":activate").encode()).hexdigest()[:32]) is None
        assert (await reader.get(TelegramOutboxRow, fresh)).payload["work_request_basis"]["interaction"]["decision"] == update.message.text
        held = await request_dependencies(reader, e.clock() + timedelta(days=401), max_age_days=180)
        assert old.id in held.acts, "the pending successor still owns its conclusive predecessor"
    # Controlled delivery retirement isolates known no-effect history expiry.
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, fresh)
        row.state, row.skip_reason, row.closed_at = "skipped", "superseded", e.clock()
    async with e.db.sessions() as reader:
        closed = await request_dependencies(reader, e.clock() + timedelta(days=401), max_age_days=180)
        assert old.id in closed.expired_history
