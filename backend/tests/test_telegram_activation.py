"""Durable chat execution intents over real Work and bot SQL transactions."""

import asyncio
import hashlib
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from _storage_spaces_test_support import operation
from _telegram_repository_test_support import db as db
from sqlalchemy import func, select
from test_telegram_work_actions import actions as actions

from deerflow.agent_instances.work import AgentWork
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow, AgentWorkRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramBotLinkRow, TelegramOutboxRow
from deerflow.runtime.runs.manager import CancelOutcome


async def seeded_attempt(e, *, bound=True):
    """Journal fixture, not native worker or run-admission qualification."""
    attempt_id, thread_id, run_id = operation(), operation(), operation() if bound else None
    async with e.db.sessions.begin() as writer:
        writer.add(
            AgentWorkAttemptRow(
                id=attempt_id,
                work_id=e.work["id"],
                activation_id=operation(),
                requester_id=e.link.owner_user_id,
                assignment_revision=e.work["assignment_revision"],
                instance_generation=e.instance.generation,
                definition_revision=e.instance.definition_revision,
                status="running" if bound else "starting",
                thread_id=thread_id,
                run_id=run_id,
                request={},
            )
        )
    record = SimpleNamespace(run_id=run_id, thread_id=thread_id, user_id=e.link.owner_user_id, metadata={"agent_work_id": e.work["id"], "agent_work_attempt_id": attempt_id})
    manager = SimpleNamespace(get=AsyncMock(return_value=record if bound else None), cancel=AsyncMock(return_value=CancelOutcome.requested))
    e.http_app.state.run_manager = manager
    return attempt_id, record, manager


@pytest.mark.asyncio
async def test_command_receipt_does_not_lose_stop_dispatch(actions, monkeypatch):
    e = actions
    attempt_id, record, manager = await seeded_attempt(e)
    original, calls = AgentWork.command, []

    async def lose(self, **kwargs):
        calls.append(kwargs["request"].operation_id)
        await original(self, **kwargs)
        raise RuntimeError("lost command response")

    monkeypatch.setattr(AgentWork, "command", lose)
    identity = operation()
    pending = await e.acts.stop(e.link, operation_id=identity, work_id=e.work["id"], attempt_id=attempt_id)
    assert pending.state == "pending"
    manager.cancel.assert_not_awaited()
    recovered = await e.acts.check(identity)
    assert recovered.state == "done"
    assert recovered.stop_dispatch["attempt_id"] == attempt_id
    assert recovered.stop_dispatch["run_id"] == record.run_id
    assert recovered.stop_dispatch["state"] == "requested"
    assert calls == [identity]
    manager.cancel.assert_awaited_once_with(record.run_id)
    async with e.db.sessions() as reader:
        assert await reader.scalar(select(func.count()).select_from(AgentWorkEventRow).where(AgentWorkEventRow.action == "stop")) == 1
        assert (await reader.get(AgentWorkAttemptRow, attempt_id)).status == "stopping"


@pytest.mark.asyncio
async def test_requested_stop_only_reads_settlement(actions):
    e = actions
    attempt_id, record, manager = await seeded_attempt(e)
    result = await e.acts.stop(e.link, operation_id=operation(), work_id=e.work["id"], attempt_id=attempt_id)
    assert result.state == "done" and result.stop_dispatch["state"] == "requested"
    result = await e.acts.resume_stop_dispatch(result)
    assert result.stop_dispatch["state"] == "requested"
    manager.cancel.assert_awaited_once_with(record.run_id)
    async with e.db.sessions.begin() as writer:
        row = await writer.get(AgentWorkAttemptRow, attempt_id)
        row.status, row.settled_at = "cancelled", datetime.now(UTC)
    result = await e.acts.resume_stop_dispatch(result)
    assert result.stop_dispatch["state"] == "settled"
    manager.cancel.assert_awaited_once_with(record.run_id)


@pytest.mark.asyncio
async def test_worker_recovers_dispatch_on_a_done_act(actions):
    from app.telegram_bots.acts import ActsWorker

    e = actions
    attempt_id, record, manager = await seeded_attempt(e)
    manager.cancel.side_effect = [RuntimeError("dispatch response unavailable"), CancelOutcome.requested]
    result = await e.acts.stop(e.link, operation_id=operation(), work_id=e.work["id"], attempt_id=attempt_id)
    assert result.state == "done" and result.stop_dispatch["state"] == "ready"
    now = e.acts.outbox.clock() + timedelta(seconds=e.acts.outbox.config.acts.retry_seconds + 1)
    e.acts.outbox.clock = lambda: now
    worker = ActsWorker(e.acts)
    try:
        await worker.tick()
    finally:
        await worker.stop()
    manager.cancel.assert_awaited_with(record.run_id)
    assert manager.cancel.await_count == 2
    async with e.db.sessions() as reader:
        assert await reader.scalar(select(func.count()).select_from(AgentWorkEventRow).where(AgentWorkEventRow.action == "stop")) == 1


@pytest.mark.asyncio
async def test_stop_followup_waits_for_same_attempt_late_run_binding(actions):
    e = actions
    attempt_id, record, manager = await seeded_attempt(e, bound=False)
    result = await e.acts.stop(e.link, operation_id=operation(), work_id=e.work["id"], attempt_id=attempt_id)
    assert result.state == "done" and result.stop_dispatch["state"] == "ready"
    manager.cancel.assert_not_awaited()
    record.run_id = operation()
    async with e.db.sessions.begin() as writer:
        (await writer.get(AgentWorkAttemptRow, attempt_id)).run_id = record.run_id
    manager.get.return_value = record
    result = await e.acts.resume_stop_dispatch(result)
    assert result.stop_dispatch["state"] == "requested" and result.stop_dispatch["run_id"] == record.run_id
    manager.cancel.assert_awaited_once_with(record.run_id)


@pytest.mark.asyncio
async def test_failed_settlement_read_never_repeats_acknowledged_cancel(actions, monkeypatch):
    e = actions
    attempt_id, record, manager = await seeded_attempt(e)
    result = await e.acts.stop(e.link, operation_id=operation(), work_id=e.work["id"], attempt_id=attempt_id)
    original = e.acts.work.attempt_for_stop
    monkeypatch.setattr(e.acts.work, "attempt_for_stop", AsyncMock(side_effect=RuntimeError("settlement read unavailable")))
    result = await e.acts.resume_stop_dispatch(result)
    assert result.stop_dispatch["state"] == "requested"
    monkeypatch.setattr(e.acts.work, "attempt_for_stop", original)
    result = await e.acts.resume_stop_dispatch(result)
    assert result.stop_dispatch["state"] == "requested"
    manager.cancel.assert_awaited_once_with(record.run_id)


@pytest.mark.asyncio
async def test_stop_conflict_child_receipt_recovers_before_parent_update(actions, monkeypatch):
    e = actions
    attempt_id, record, manager = await seeded_attempt(e)
    identity = operation()
    child_id = hashlib.sha256((identity + ":stop:2").encode()).hexdigest()[:32]
    original, calls = AgentWork.command, []

    async def concurrent_progress_then_lost_child(self, **kwargs):
        body = kwargs["request"]
        calls.append(body.operation_id)
        if len(calls) == 1:
            async with e.db.sessions.begin() as writer:
                (await writer.get(AgentWorkRow, e.work["id"])).revision += 1
            return await original(self, **kwargs)
        async with e.db.sessions() as reader:
            parent = await reader.get(TelegramActRow, identity)
            child = await reader.get(TelegramActRow, child_id)
            assert parent.operation_ids == [child_id]
            assert child.body["operation_id"] == child_id and child.stop_dispatch["state"] == "awaiting_command"
        await original(self, **kwargs)
        raise RuntimeError("child committed before parent learned its receipt")

    monkeypatch.setattr(AgentWork, "command", concurrent_progress_then_lost_child)
    result = await e.acts.stop(e.link, operation_id=identity, work_id=e.work["id"], attempt_id=attempt_id)
    assert result.state == "pending" and result.operation_ids == (child_id,)
    result = await e.acts.check(identity)
    assert result.state == "done" and result.result["stop_child_id"] == child_id
    assert calls == [identity, child_id]
    manager.cancel.assert_awaited_once_with(record.run_id)
    async with e.db.sessions() as reader:
        parent, child = await reader.get(TelegramActRow, identity), await reader.get(TelegramActRow, child_id)
        assert child.state == "done" and child.stop_dispatch["state"] == "requested"
        assert parent.body["expected_revision"] + 1 == child.body["expected_revision"]
        assert await reader.scalar(select(func.count()).select_from(AgentWorkEventRow).where(AgentWorkEventRow.action == "stop")) == 1


@pytest.mark.asyncio
async def test_stop_reads_new_revision_for_three_distinct_conflicts(actions, monkeypatch):
    e = actions
    attempt_id, _record, manager = await seeded_attempt(e)
    original, calls = AgentWork.command, []
    identity = operation()

    async def progress_before_every_command(self, **kwargs):
        calls.append(kwargs["request"].model_dump(mode="json", exclude_unset=True))
        async with e.db.sessions.begin() as writer:
            (await writer.get(AgentWorkRow, e.work["id"])).revision += 1
        return await original(self, **kwargs)

    monkeypatch.setattr(AgentWork, "command", progress_before_every_command)
    result = await e.acts.stop(e.link, operation_id=identity, work_id=e.work["id"], attempt_id=attempt_id)
    assert result.state == "conflict" and result.reason == "stop-conflicts"
    assert [body["operation_id"] for body in calls] == [identity, *(hashlib.sha256((identity + f":stop:{n}").encode()).hexdigest()[:32] for n in (2, 3))]
    assert [body["expected_revision"] for body in calls] == sorted({body["expected_revision"] for body in calls})
    await e.acts.check(identity)
    assert len(calls) == 3
    manager.cancel.assert_not_awaited()


@pytest.mark.asyncio
async def test_late_binding_replacement_refuses_physical_stop(actions):
    e = actions
    attempt_id, record, manager = await seeded_attempt(e)

    async def fetched(*_args, **_kwargs):
        async with e.db.sessions.begin() as writer:
            (await writer.get(TelegramBotLinkRow, e.link.id)).binding_generation += 1
        return record

    manager.get.side_effect = fetched
    result = await e.acts.stop(e.link, operation_id=operation(), work_id=e.work["id"], attempt_id=attempt_id)
    assert result.state == "done" and result.stop_dispatch["state"] == "ended"
    manager.cancel.assert_not_awaited()


@pytest.mark.asyncio
async def test_manager_loss_after_original_read_refuses_physical_stop(actions, monkeypatch):
    e = actions
    attempt_id, record, manager = await seeded_attempt(e)
    async with e.db.sessions.begin() as writer:
        (await writer.get(AgentWorkAttemptRow, attempt_id)).requester_id = "past-requester"
    record.user_id = "past-requester"
    original, reads = e.acts.work.attempt_for_stop, []

    async def read(**kwargs):
        value = await original(**kwargs)
        reads.append(True)
        if len(reads) == 2:
            async with e.db.sessions.begin() as writer:
                (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 3
        return value

    monkeypatch.setattr(e.acts.work, "attempt_for_stop", read)
    result = await e.acts.stop(e.link, operation_id=operation(), work_id=e.work["id"], attempt_id=attempt_id)
    assert result.state == "done" and result.stop_dispatch["state"] == "ended"
    manager.cancel.assert_not_awaited()


@pytest.mark.asyncio
async def test_worker_shutdown_during_host_read_preserves_original_stop(actions):
    from app.telegram_bots.acts import ActsWorker

    e = actions
    attempt_id, record, manager = await seeded_attempt(e)
    manager.get.return_value = None
    first = await e.acts.stop(e.link, operation_id=operation(), work_id=e.work["id"], attempt_id=attempt_id)
    assert first.state == "done" and first.stop_dispatch["state"] == "ready"
    worker, stops = ActsWorker(e.acts), []

    async def fetched(*_args, **_kwargs):
        stops.append(asyncio.create_task(worker.stop()))
        await asyncio.sleep(0)
        assert worker._stopping
        return record

    manager.get.side_effect = fetched
    now = e.acts.outbox.clock() + timedelta(seconds=e.acts.outbox.config.acts.retry_seconds + 1)
    e.acts.outbox.clock = lambda: now
    try:
        await worker.tick()
        await asyncio.gather(*stops)
        manager.cancel.assert_not_awaited()
        retained = await e.acts._work_record(first.id)
        assert retained.stop_dispatch["state"] == "ready" and retained.stop_dispatch["attempts"] == first.stop_dispatch["attempts"]
        manager.get.side_effect, manager.get.return_value = None, record
        replacement = ActsWorker(e.acts)
        try:
            await replacement.tick()
            manager.cancel.assert_awaited_once_with(record.run_id)
        finally:
            await replacement.stop()
    finally:
        await worker.stop()
        await asyncio.gather(*stops, return_exceptions=True)


@pytest.mark.asyncio
async def test_duplicate_parent_retry_does_not_repeat_a_child_attempt(actions, monkeypatch):
    e = actions
    attempt_id, _record, manager = await seeded_attempt(e)
    original, calls = AgentWork.command, []

    async def conflict_then_unknown(self, **kwargs):
        calls.append(kwargs["request"].operation_id)
        if len(calls) == 1:
            async with e.db.sessions.begin() as writer:
                (await writer.get(AgentWorkRow, e.work["id"])).revision += 1
            return await original(self, **kwargs)
        raise RuntimeError("unknown child response")

    monkeypatch.setattr(AgentWork, "command", conflict_then_unknown)
    first = await e.acts.stop(e.link, operation_id=operation(), work_id=e.work["id"], attempt_id=attempt_id)
    assert first.state == "pending" and first.attempts == 2
    expected = {"id": first.id, "kind": first.kind, "body": dict(first.body), "attempts": first.attempts}
    second = await e.acts.check(first.id, expected=expected)
    assert second.state == "pending" and second.attempts == 3
    await e.acts.check(first.id, expected=expected)
    assert len(calls) == 3 and calls[1] == calls[2] == first.operation_ids[0]
    manager.cancel.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["ready", "requested", "settled", "malformed"])
async def test_retention_keeps_unresolved_stop_and_expires_valid_settlement(actions, phase):
    from app.telegram_bots.retention import ACT_RETENTION_DAYS, _sweep_acts

    e = actions
    attempt_id, _record, manager = await seeded_attempt(e)
    if phase == "ready":
        manager.get.return_value = None
    result = await e.acts.stop(e.link, operation_id=operation(), work_id=e.work["id"], attempt_id=attempt_id)
    if phase in {"settled", "malformed"}:
        async with e.db.sessions.begin() as writer:
            row = await writer.get(AgentWorkAttemptRow, attempt_id)
            row.status, row.settled_at = "cancelled", datetime.now(UTC)
        result = await e.acts.resume_stop_dispatch(result)
    now = datetime.now(UTC)
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramActRow, result.id)
        row.created_at = now - timedelta(days=ACT_RETENTION_DAYS + 1)
        if phase == "malformed":
            row.stop_dispatch = {**row.stop_dispatch, "attempt_id": operation()}
    await _sweep_acts(e.db.sessions, now)
    async with e.db.sessions() as reader:
        retained = await reader.get(TelegramActRow, result.id)
        assert (retained is None) == (phase == "settled")


@pytest.mark.asyncio
async def test_unreadable_expired_stop_receipt_becomes_uncertain_without_retry(actions, monkeypatch):
    from app.telegram_bots.acts import ActsWorker
    from app.telegram_bots.authority import AuthorityUnavailable

    e = actions
    attempt_id, _record, manager = await seeded_attempt(e)
    command_call = AsyncMock(side_effect=RuntimeError("unknown command result"))
    monkeypatch.setattr(AgentWork, "command", command_call)
    result = await e.acts.stop(e.link, operation_id=operation(), work_id=e.work["id"], attempt_id=attempt_id)
    assert result.state == "pending"
    monkeypatch.setattr(e.acts, "receipt_for", AsyncMock(side_effect=AuthorityUnavailable()))
    now = e.acts.outbox.clock() + timedelta(hours=1, seconds=1)
    e.acts.outbox.clock = lambda: now
    worker = ActsWorker(e.acts)
    try:
        await worker.tick()
    finally:
        await worker.stop()
    assert (await e.acts._work_record(result.id)).state == "uncertain"
    assert command_call.await_count == 1
    manager.cancel.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["requested", "settled", "pending_outcome", "missing_root_scope"])
async def test_retention_keeps_parent_child_group_until_followup_and_reply_finish(actions, monkeypatch, phase):
    from app.telegram_bots.retention import ACT_RETENTION_DAYS, _sweep_acts

    e = actions
    attempt_id, _record, _manager = await seeded_attempt(e)
    original, calls = AgentWork.command, []

    async def progress_before_first(self, **kwargs):
        calls.append(kwargs["request"].operation_id)
        if len(calls) == 1:
            async with e.db.sessions.begin() as writer:
                (await writer.get(AgentWorkRow, e.work["id"])).revision += 1
        return await original(self, **kwargs)

    monkeypatch.setattr(AgentWork, "command", progress_before_first)
    root = await e.acts.stop(e.link, operation_id=operation(), work_id=e.work["id"], attempt_id=attempt_id)
    child_id = root.operation_ids[0]
    if phase not in {"requested", "missing_root_scope"}:
        async with e.db.sessions.begin() as writer:
            attempt = await writer.get(AgentWorkAttemptRow, attempt_id)
            attempt.status, attempt.settled_at = "cancelled", datetime.now(UTC)
        await e.acts.resume_stop_dispatch(await e.acts._work_record(child_id))
    now = datetime.now(UTC)
    async with e.db.sessions.begin() as writer:
        for identity in (root.id, child_id):
            (await writer.get(TelegramActRow, identity)).created_at = now - timedelta(days=ACT_RETENTION_DAYS + 1)
        if phase == "missing_root_scope":
            (await writer.get(TelegramActRow, root.id)).stop_dispatch = None
        if phase == "pending_outcome":
            writer.add(
                TelegramOutboxRow(
                    id=operation(),
                    link_id=e.link.id,
                    owner_user_id=e.link.owner_user_id,
                    dedupe_key="stop-outcome:" + root.id,
                    class_="reply",
                    binding_generation=e.link.binding_generation,
                    payload={"work_outcome_basis": {"context": {"act": {"id": root.id}}}},
                    parts=[],
                    state="pending",
                    not_before=now,
                    created_at=now,
                )
            )
    await _sweep_acts(e.db.sessions, now)
    async with e.db.sessions() as reader:
        for identity in (root.id, child_id):
            assert ((await reader.get(TelegramActRow, identity)) is None) == (phase == "settled")


@pytest.mark.asyncio
async def test_own_reply_metadata_does_not_retire_pending_cancellation(actions):
    e = actions
    attempt_id, record, manager = await seeded_attempt(e)
    identity = operation()

    async def fetched(*_args, **_kwargs):
        # Separate reply-owner metadata, not a claim of a transport ACK.
        async with e.db.sessions.begin() as writer:
            (await writer.get(TelegramActRow, identity)).continuation = {"version": 1, "reply_acknowledged": True}
        return record

    manager.get.side_effect = fetched
    result = await e.acts.stop(e.link, operation_id=identity, work_id=e.work["id"], attempt_id=attempt_id)
    assert result.state == "done" and result.stop_dispatch["state"] == "requested"
    manager.cancel.assert_awaited_once_with(record.run_id)


@pytest.mark.asyncio
async def test_retention_expires_known_no_running_refusal_with_no_followup(actions):
    from app.telegram_bots.retention import ACT_RETENTION_DAYS, _sweep_acts

    e = actions
    result = await e.acts.stop(e.link, operation_id=operation(), work_id=e.work["id"], attempt_id=operation())
    assert result.state == "refused" and result.reason == "no-running-attempt" and result.attempts == 0 and result.stop_dispatch is None
    now = datetime.now(UTC)
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramActRow, result.id)).created_at = now - timedelta(days=ACT_RETENTION_DAYS + 1)
    await _sweep_acts(e.db.sessions, now)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramActRow, result.id) is None


@pytest.mark.asyncio
async def test_worker_resumes_child_planning_after_first_conflict_interruption(actions, monkeypatch):
    from app.telegram_bots.acts import ActsWorker

    e = actions
    attempt_id, record, manager = await seeded_attempt(e)
    identity = operation()
    original, calls = AgentWork.command, []

    async def progress_before_first(self, **kwargs):
        calls.append(kwargs["request"].operation_id)
        if len(calls) == 1:
            async with e.db.sessions.begin() as writer:
                (await writer.get(AgentWorkRow, e.work["id"])).revision += 1
        return await original(self, **kwargs)

    monkeypatch.setattr(AgentWork, "command", progress_before_first)
    plan = e.acts._plan_stop_child
    monkeypatch.setattr(e.acts, "_plan_stop_child", AsyncMock(side_effect=RuntimeError("retry planning interrupted")))
    with pytest.raises(RuntimeError, match="retry planning interrupted"):
        await e.acts.stop(e.link, operation_id=identity, work_id=e.work["id"], attempt_id=attempt_id)
    root = await e.acts._work_record(identity)
    assert root.state == "conflict" and root.reason == "changed" and not root.operation_ids
    monkeypatch.setattr(e.acts, "_plan_stop_child", plan)
    now = e.acts.outbox.clock() + timedelta(seconds=e.acts.outbox.config.acts.retry_seconds + 1)
    e.acts.outbox.clock = lambda: now
    worker = ActsWorker(e.acts)
    try:
        await worker.tick()
    finally:
        await worker.stop()
    result = await e.acts._work_record(identity)
    assert result.state == "done"
    assert calls == [identity, child_identity_for_test(identity, 2)]
    manager.cancel.assert_awaited_once_with(record.run_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("missing_child_scope", ["none", "scope", "scope_and_lineage"])
async def test_retention_never_treats_a_missing_child_scope_as_standalone_no_effect(actions, monkeypatch, missing_child_scope):
    from app.telegram_bots.retention import ACT_RETENTION_DAYS, _sweep_acts
    from deerflow.agent_instances.contract import Refusal

    e = actions
    attempt_id, _record, manager = await seeded_attempt(e)
    original, calls = AgentWork.command, []

    async def conflict_then_no_running(self, **kwargs):
        calls.append(kwargs["request"].operation_id)
        if len(calls) == 1:
            async with e.db.sessions.begin() as writer:
                (await writer.get(AgentWorkRow, e.work["id"])).revision += 1
            return await original(self, **kwargs)
        raise Refusal("No running attempt", reason="no-running-attempt", status=409)

    monkeypatch.setattr(AgentWork, "command", conflict_then_no_running)
    root = await e.acts.stop(e.link, operation_id=operation(), work_id=e.work["id"], attempt_id=attempt_id)
    assert root.state == "refused" and root.operation_ids
    child_id = root.operation_ids[0]
    now = datetime.now(UTC)
    async with e.db.sessions.begin() as writer:
        for identity in (root.id, child_id):
            (await writer.get(TelegramActRow, identity)).created_at = now - timedelta(days=ACT_RETENTION_DAYS + 1)
        if missing_child_scope != "none":
            child = await writer.get(TelegramActRow, child_id)
            child.stop_dispatch, child.attempts = None, 0
            if missing_child_scope == "scope_and_lineage":
                child.continuation = None
    await _sweep_acts(e.db.sessions, now)
    async with e.db.sessions() as reader:
        for identity in (root.id, child_id):
            assert ((await reader.get(TelegramActRow, identity)) is not None) == (missing_child_scope != "none")
    manager.cancel.assert_not_awaited()


def child_identity_for_test(identity, ordinal):
    return hashlib.sha256((identity + f":stop:{ordinal}").encode()).hexdigest()[:32]
