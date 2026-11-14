"""An actual spoken-answer confirmation owns one response and one activation."""

from hashlib import sha256

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_work_callback_dispatch import press_work
from test_telegram_work_interaction_delivery import selected
from test_telegram_work_outcome_delivery import clear_outcome_cleanup
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_telegram_work_start import prepare_startable
from test_telegram_work_voice_inputs import voice_answer
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramBotLinkRow, TelegramCallbackRow, TelegramOutboxRow


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["voice_save_resume", "voice_save_answer"])
@pytest.mark.parametrize("paused", [False, True])
async def test_actual_voice_save_and_resume_keeps_the_original_transcript_and_child(work_delivery, monkeypatch, kind, paused):
    e = work_delivery
    await prepare_startable(e, monkeypatch)
    e.reader.composites = True
    # The original displayed question can predate the new control profile.
    request, _notice = await voice_answer(e, monkeypatch, combined=True)
    await dispatch(e, 901)
    async with e.db.sessions() as reader:
        notice = await reader.get(TelegramOutboxRow, e.identity)
        assert notice.state == "sent", (notice.state, notice.skip_reason, notice.failed_reason, e.dispatch_errors)
    if paused:
        async with e.db.sessions.begin() as writer:
            (await writer.get(TelegramBotLinkRow, e.link.id)).acts_paused_since = e.clock()
    button = await selected(e, kind)
    assert button is not None and not e.launches
    await press_work(e, button, message_id=901)
    async with e.db.sessions() as reader:
        parent = await reader.get(TelegramActRow, button.id)
        assert parent is not None
        child_id = sha256((button.id + ":activate").encode()).hexdigest()[:32]
        child = await reader.get(TelegramActRow, child_id)
        assert parent.kind == "respond" and parent.state == "done"
        assert parent.body["text"] == "Use the current account."
        combined = kind == "voice_save_resume"
        if combined:
            assert parent.continuation["activation_continuation"]["phase"] == "done"
            assert child.kind == "activate" and child.state == "done" and child.work_id == request["work_id"]
        else:
            assert parent.continuation is None or "activation_continuation" not in parent.continuation
            assert child is None
        assert await reader.get(TelegramCallbackRow, child_id) is None
        assert len(list(await reader.scalars(select(TelegramActRow).where(TelegramActRow.kind.in_(("respond", "activate")))))) == (2 if combined else 1)
    assert len(e.launches) == (1 if combined else 0) and len(e.transcriber.calls) == 1
    await press_work(e, button, message_id=901, update_id=2902)
    assert len(e.launches) == (1 if combined else 0) and len(e.transcriber.calls) == 1


@pytest.mark.asyncio
async def test_primary_sdk_ack_keeps_voice_child_custody_through_restart(work_delivery, monkeypatch):
    from unittest.mock import AsyncMock

    from app.gateway import work_execution

    e = work_delivery
    await prepare_startable(e, monkeypatch)
    e.reader.composites = True
    _request, _notice = await voice_answer(e, monkeypatch, combined=True)
    await dispatch(e, 901)
    assert not e.dispatch_errors, e.dispatch_errors
    button = await selected(e, "voice_save_resume")
    original = work_execution.activate_work
    monkeypatch.setattr(work_execution, "activate_work", AsyncMock(side_effect=RuntimeError("Host admission unavailable")))
    await press_work(e, button, message_id=901)
    async with e.db.sessions() as reader:
        parent = await reader.get(TelegramActRow, button.id)
        assert parent.state == "done" and parent.continuation["activation_continuation"]["phase"] == "child_planned"
        e.outcome_identity = parent.continuation["request_activation"]["outcome_id"]
        origin = parent.continuation["request_activation"]
        child_id = parent.continuation["activation_continuation"]["child_id"]
    await clear_outcome_cleanup(e)
    await dispatch(e, 902)
    assert not e.dispatch_errors, e.dispatch_errors
    async with e.db.sessions() as reader:
        parent = await reader.get(TelegramActRow, button.id)
        assert (await reader.get(TelegramOutboxRow, e.outcome_identity)).state == "sent"
        assert parent.continuation["request_activation"] == origin
        assert parent.continuation["work_outcome"]["outbox_id"] == e.outcome_identity
        assert parent.continuation["activation_continuation"]["child_id"] == child_id
    monkeypatch.setattr(work_execution, "activate_work", original)
    acts = e.channel._services.work_callbacks.acts
    e.clock.advance(acts.outbox.config.acts.retry_seconds)
    completed = await acts.resume_continuation(button.id)
    assert completed.continuation["activation_continuation"]["phase"] == "done"
    assert len(e.launches) == 1 and len(e.transcriber.calls) == 1


async def pending_voice_child(e, monkeypatch, *, failure=None):
    from unittest.mock import AsyncMock

    from app.gateway import work_execution

    await prepare_startable(e, monkeypatch)
    e.reader.composites = True
    await voice_answer(e, monkeypatch, combined=True)
    await dispatch(e, 901)
    button = await selected(e, "voice_save_resume")
    original = work_execution.activate_work
    monkeypatch.setattr(work_execution, "activate_work", AsyncMock(side_effect=failure if failure is not None else RuntimeError("Host admission unavailable")))
    await press_work(e, button, message_id=901)
    acts = e.channel._services.work_callbacks.acts
    e.clock.advance(acts.outbox.config.acts.retry_seconds)
    parent = await acts._work_record(button.id)
    phase = "refused" if getattr(failure, "reason", None) == "work-limit" else "child_planned"
    assert parent.state == "done" and parent.continuation["activation_continuation"]["phase"] == phase
    return acts, parent, original


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["input", "route", "question", "voice_origin", "whole_ack", "response_event", "native_answer"])
async def test_voice_child_rechecks_input_and_primary_at_service_commit(work_delivery, monkeypatch, damage):
    from app.gateway import work_execution
    from deerflow.persistence.agent_instances.human_input import HumanInputEventRow, HumanInputResponseRow
    from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow

    e = work_delivery
    acts, parent, original = await pending_voice_child(e, monkeypatch)
    entered = []

    async def retire(*args, **kwargs):
        entered.append(True)
        identity = parent.continuation["request_activation"]["input"]
        async with e.db.sessions.begin() as writer:
            mark = await writer.get(TelegramInboundMarkRow, (identity["link_id"], identity["message_id"], identity["chat_id"]))
            notice = await writer.get(TelegramOutboxRow, parent.continuation["request_activation"]["origin"]["anchor_outbox_id"])
            if damage == "input":
                mark.sanitized_input = {**mark.sanitized_input, "text": "Changed after the wait"}
            elif damage == "route":
                mark.route_target_id = "f" * 32
            elif damage == "question":
                question = await writer.get(TelegramOutboxRow, mark.route_basis["work_answer"]["source_outbox_id"])
                question.request_id = "f" * 32
            elif damage == "voice_origin":
                basis = notice.payload["product_basis"]
                notice.payload = {**notice.payload, "product_basis": {**basis, "origins": [[e.link.id, identity["message_id"] + 1, e.link.chat_id]]}}
            elif damage == "whole_ack":
                notice.parts = [{**part, "message_ids": []} for part in notice.parts]
            elif damage == "response_event":
                event = await writer.scalar(select(HumanInputEventRow).where(HumanInputEventRow.operation_id == parent.id))
                event.receipt = {**event.receipt, "actor_id": "other-account"}
            else:
                response = await writer.scalar(select(HumanInputResponseRow).where(HumanInputResponseRow.request_id == parent.request_id))
                response.text = "Changed after the wait"
        return await original(*args, **kwargs)

    monkeypatch.setattr(work_execution, "activate_work", retire)
    await acts.resume_continuation(parent.id)
    assert entered and not e.launches
    assert (await acts._work_record(parent.id)).state == "done"
    assert len(e.transcriber.calls) == 1


@pytest.mark.asyncio
async def test_retired_callback_owner_leaves_saved_voice_ready_for_the_independent_worker(work_delivery, monkeypatch):
    e = work_delivery
    await prepare_startable(e, monkeypatch)
    e.reader.composites = True
    await voice_answer(e, monkeypatch, combined=True)
    await dispatch(e, 901)
    button = await selected(e, "voice_save_resume")
    callbacks = e.channel._services.work_callbacks
    original_plan, original_read = callbacks.outcomes.plan, callbacks.acts._work_record
    original_current = type(e.channel)._callback_current
    ready, current = [], [True]

    async def plan(*args, **kwargs):
        result = await original_plan(*args, **kwargs)
        ready.append(True)
        return result

    async def read(identity):
        result = await original_read(identity)
        if ready and identity == button.id:
            current[0] = False
        return result

    monkeypatch.setattr(type(e.channel), "_callback_current", lambda channel, link: current[0] and original_current(channel, link))
    monkeypatch.setattr(callbacks.outcomes, "plan", plan)
    monkeypatch.setattr(callbacks.acts, "_work_record", read)
    await press_work(e, button, message_id=901)
    parent = await original_read(button.id)
    assert ready and parent.state == "done"
    assert parent.continuation["activation_continuation"]["phase"] == "ready"
    assert not e.launches
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramActRow, parent.continuation["activation_continuation"]["child_id"]) is None
    monkeypatch.setattr(callbacks.acts, "_work_record", original_read)
    current[0] = True
    completed = await callbacks.acts.resume_continuation(parent.id)
    assert completed.continuation["activation_continuation"]["phase"] == "done" and len(e.launches) == 1
