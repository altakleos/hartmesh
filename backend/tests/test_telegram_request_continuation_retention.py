"""Pending request continuations retain their whole original confirmation."""

from datetime import timedelta

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_voice_continuations import pending_voice_child
from test_telegram_work_outcome_delivery import clear_outcome_cleanup
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.retention import sweep_rows
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", [None, "child_marker", "missing_child", "missing_primary", "missing_family"])
async def test_pending_voice_resume_retains_original_input_and_complete_consent(work_delivery, monkeypatch, damage):
    e = work_delivery
    acts, parent, _original = await pending_voice_child(e, monkeypatch)
    source = parent.continuation["request_activation"]
    child_id = parent.continuation["activation_continuation"]["child_id"]
    e.outcome_identity = source["outcome_id"]
    await clear_outcome_cleanup(e)
    await dispatch(e, 1101)
    assert not e.dispatch_errors, e.dispatch_errors
    identity = source["input"]
    input_key = (identity["link_id"], identity["message_id"], identity["chat_id"])
    async with e.db.sessions.begin() as writer:
        mark = await writer.get(TelegramInboundMarkRow, input_key)
        question_id = mark.route_basis["work_answer"]["source_outbox_id"]
        mark.state, mark.closed_at = "done", e.clock()
        question = await writer.get(TelegramOutboxRow, question_id)
        question.closed_at = e.clock()
        group = (await writer.get(TelegramCallbackRow, parent.id)).group_id
        question_group = mark.route_basis["work_answer"]["group_id"]
        if damage == "child_marker":
            (await writer.get(TelegramActRow, child_id)).continuation = None
        elif damage == "missing_child":
            await writer.delete(await writer.get(TelegramActRow, child_id))
        elif damage in {"missing_primary", "missing_family"}:
            await writer.delete(await writer.get(TelegramActRow, parent.id))
            if damage == "missing_family":
                await writer.delete(await writer.get(TelegramActRow, child_id))
    later = e.clock() + timedelta(days=401)
    await e.channel._services.callbacks.sweep(later)
    await sweep_rows(e.db.sessions, later, callback_max_age_days=180)
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramActRow, parent.id) is not None) == (damage not in {"missing_primary", "missing_family"})
        assert await reader.get(TelegramInboundMarkRow, input_key) is not None
        for outbox_id in (question_id, source["origin"]["anchor_outbox_id"], source["outcome_id"]):
            assert await reader.get(TelegramOutboxRow, outbox_id) is not None
        assert list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == group)))
        assert list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == question_group)))
    assert not e.launches
    if damage not in {"missing_primary", "missing_family"}:
        assert (await acts._work_record(parent.id)).state == "done"


@pytest.mark.asyncio
async def test_closed_request_family_expires_after_consent_and_primary_outbox_collection(work_delivery, monkeypatch):
    from app.gateway import work_execution
    from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow

    e = work_delivery
    acts, parent, original = await pending_voice_child(e, monkeypatch)
    monkeypatch.setattr(work_execution, "activate_work", original)
    completed = await acts.resume_continuation(parent.id)
    assert completed.continuation["activation_continuation"]["phase"] == "done"
    child_id = completed.continuation["activation_continuation"]["child_id"]
    e.outcome_identity = completed.continuation["request_activation"]["outcome_id"]
    await clear_outcome_cleanup(e)
    await dispatch(e, 1101)
    async with e.db.sessions.begin() as writer:
        saved = await writer.get(TelegramActRow, parent.id)
        notice = await writer.get(TelegramOutboxRow, e.outcome_identity)
        assert notice.state == "sent", (notice.state, notice.skip_reason, notice.failed_reason, e.dispatch_errors)
        assert "work_outcome" in saved.continuation, saved.continuation
        attempt = await writer.scalar(select(AgentWorkAttemptRow).where(AgentWorkAttemptRow.activation_id == child_id))
        attempt.status, attempt.settled_at = "cancelled", e.clock()
    later = e.clock() + timedelta(days=401)
    await e.channel._services.callbacks.sweep(later)
    await sweep_rows(e.db.sessions, later, callback_max_age_days=180)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramActRow, parent.id) is None
        assert await reader.get(TelegramActRow, child_id) is None
