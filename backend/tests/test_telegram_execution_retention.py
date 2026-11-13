"""Execution recovery retains its original consent and complete delivery proof."""

from datetime import timedelta

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_work_execution_outcomes import stop_notice
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.retention import ACT_RETENTION_DAYS, _sweep_acts, _sweep_outbox
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["requested", "final_reply_pending"])
async def test_original_stop_evidence_survives_age_while_followup_is_unfinished(work_delivery, phase):
    e = work_delivery
    plans, act, attempt_id, _manager = await stop_notice(e)
    e.identity = await plans.observe(e.link, act.id)
    await dispatch(e, 3920)
    async with e.db.sessions() as reader:
        source = await reader.get(TelegramCallbackRow, act.id)
        parent_id = source.payload["anchor_outbox_id"]
    if phase == "final_reply_pending":
        async with e.db.sessions.begin() as writer:
            attempt = await writer.get(AgentWorkAttemptRow, attempt_id)
            attempt.status, attempt.settled_at = "cancelled", e.clock.now()
        act = await e.acts.resume_stop_dispatch(act)
        assert act.stop_dispatch["state"] == "settled"
        await plans.observe(e.link, act.id)
    e.clock.advance(86400 * (ACT_RETENTION_DAYS + 1))
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramActRow, act.id)).created_at = e.clock.now() - timedelta(days=ACT_RETENTION_DAYS + 1)
    await e.channel._services.callbacks.sweep(e.clock.now())
    await _sweep_outbox(e.db.sessions, e.clock.now(), e.db.config.callbacks.max_age_days)
    await _sweep_acts(e.db.sessions, e.clock.now())
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramActRow, act.id) is not None
        assert await reader.get(TelegramCallbackRow, act.id) is not None
        assert await reader.get(TelegramOutboxRow, parent_id) is not None
        assert await reader.get(TelegramOutboxRow, e.identity) is not None
    assert await plans.observe(e.link, act.id) == e.identity


@pytest.mark.asyncio
async def test_settled_and_acknowledged_stop_evidence_expires_normally(work_delivery):
    from test_telegram_work_execution_outcomes import dispatch_edit

    e = work_delivery
    plans, act, attempt_id, _manager = await stop_notice(e)
    e.identity = await plans.observe(e.link, act.id)
    await dispatch(e, 3930)
    async with e.db.sessions.begin() as writer:
        original = await writer.get(AgentWorkAttemptRow, attempt_id)
        original.status, original.settled_at = "cancelled", e.clock.now()
    act = await e.acts.resume_stop_dispatch(act)
    await plans.observe(e.link, act.id)
    await dispatch_edit(e, 3930)
    e.clock.advance(86400 * (ACT_RETENTION_DAYS + 1))
    await e.channel._services.callbacks.sweep(e.clock.now())
    await _sweep_outbox(e.db.sessions, e.clock.now(), e.db.config.callbacks.max_age_days)
    await _sweep_acts(e.db.sessions, e.clock.now())
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramActRow, act.id) is None
        assert await reader.get(TelegramOutboxRow, e.identity) is None


@pytest.mark.asyncio
async def test_legacy_ended_follow_up_with_unsettled_original_keeps_its_journal(work_delivery):
    from app.telegram_bots.acts import _snapshot
    from app.telegram_bots.work_execution_retention import execution_dependencies

    e = work_delivery
    plans, act, attempt_id, manager = await stop_notice(e)
    # Earlier versions could end an acknowledged follow-up after a ceiling
    # withdrawal. Keep that persisted legacy shape as a retention fixture;
    # current recovery preserves requested instead of erasing the ACK.
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramActRow, act.id)
        row.stop_dispatch = {**row.stop_dispatch, "state": "ended"}
        await writer.flush()
        act = _snapshot(row)
    assert act.stop_dispatch["state"] == "ended"
    async with e.db.sessions() as reader:
        original = await reader.get(AgentWorkAttemptRow, attempt_id)
        assert original.status == "stopping" and original.settled_at is None
    e.identity = await plans.observe(e.link, act.id)
    await dispatch(e, 3950)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, e.identity)
        assert row.state == "sent"
        _groups, anchors, _held_acts = await execution_dependencies(reader)
        assert e.identity in anchors
    e.clock.advance(86400 * (ACT_RETENTION_DAYS + 1))
    await _sweep_acts(e.db.sessions, e.clock.now())
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramActRow, act.id) is not None
    manager.cancel.assert_awaited_once()
