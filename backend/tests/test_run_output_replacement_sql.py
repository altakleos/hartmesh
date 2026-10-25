"""Direct atomic replacement/reuse of captured SQL predecessors."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from _run_output_store_test_support import RAW_OWNER
from _run_output_store_test_support import output_store as output_store
from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from deerflow.persistence.run.model import RunChangeClockRow, RunRow
from deerflow.runtime.runs.manager import ConflictError
from deerflow.runtime.runs.output import OUTPUT_CAPTURE_DISCONNECT_KEY, OutputEvidenceError, UnavailableOutputReceipt, receipt_from_kwargs
from deerflow.runtime.runs.store.base import RunIdempotencyConflict
from deerflow.runtime.runs.terminal import TerminalWriteFailure


async def predecessor(e, *, capture=True, lease="expired", owner="original-worker", key=None):
    run_id, thread_id = uuid4().hex, uuid4().hex
    deadline = None if lease == "null" else (datetime.now(UTC) + timedelta(hours=1) * (1 if lease == "live" else -1)).isoformat()
    kwargs = {"retained": {"nested": ["original"]}}
    if capture:
        kwargs.update(output_capture={"version": 1}, **{OUTPUT_CAPTURE_DISCONNECT_KEY: "continue"})
    await e.repository.put(run_id, thread_id=thread_id, user_id=RAW_OWNER, status="running", owner_worker_id=owner, lease_expires_at=deadline, kwargs=kwargs, idempotency_key=key, stop_reason="original_stop")
    return await e.repository.get(run_id, user_id=RAW_OWNER)


async def replace(e, row, *, strategy="interrupt", new_id=None, key=None, owner="new-worker"):
    return await e.repository.create_thread_operation_atomic(
        new_id or uuid4().hex, thread_id=row["thread_id"], owner_worker_id=owner, lease_expires_at=None, multitask_strategy=strategy, user_id=RAW_OWNER, kwargs={"successor": ["original"]}, idempotency_key=key
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("strategy", ["interrupt", "rollback"])
@pytest.mark.parametrize("owner", ["original-worker", "new-worker"])
async def test_live_capture_refuses_before_old_or_new_row_mutation(output_store, strategy, owner):
    e = output_store
    before = await predecessor(e, lease="live")
    async with e.sf() as session:
        clock = await session.scalar(select(RunChangeClockRow.value))
    with pytest.raises(ConflictError):
        await replace(e, before, strategy=strategy, owner=owner)
    assert await e.repository.get(before["run_id"], user_id=RAW_OWNER) == before
    assert len(await e.repository.list_by_thread(before["thread_id"], user_id=RAW_OWNER)) == 1
    async with e.sf() as session:
        assert await session.scalar(select(RunChangeClockRow.value)) == clock


@pytest.mark.asyncio
@pytest.mark.parametrize("strategy", ["interrupt", "rollback"])
@pytest.mark.parametrize("lease", ["expired", "null"])
async def test_expired_capture_failed_seal_and_successor_share_one_transaction(output_store, strategy, lease):
    e = output_store
    before = await predecessor(e, lease=lease)
    assert await e.repository.request_cancel(before["run_id"], action="rollback") == "rollback"
    before = await e.repository.get(before["run_id"], user_id=RAW_OWNER)
    created, claimed = await replace(e, before, strategy=strategy)
    after = await e.repository.get(before["run_id"], user_id=RAW_OWNER)
    assert after["status"] == "error", "Expiry proves lost ownership, not performed cancellation"
    expected = UnavailableOutputReceipt(version=1, kind="unavailable", owner_user_id=RAW_OWNER, thread_id=before["thread_id"], run_id=before["run_id"], input_message_id=None, reason="failed")
    assert receipt_from_kwargs(after["kwargs"]) == expected
    assert after["kwargs"] == {**before["kwargs"], "output_evidence": expected.model_dump(mode="json")}
    assert after["owner_worker_id"] == before["owner_worker_id"]
    assert after["cancel_action"] == "rollback"
    assert after["stop_reason"] == before["stop_reason"] == "original_stop"
    assert created["change_seq"] == after["change_seq"] > before["change_seq"]
    assert created["status"] == "pending" and created["owner_worker_id"] == "new-worker"
    assert len(claimed) == 1 and claimed[0] == after
    assert (await e.repository.get(created["run_id"], user_id=RAW_OWNER))["kwargs"] == {"successor": ["original"]}


@pytest.mark.asyncio
@pytest.mark.parametrize("strategy", ["interrupt", "rollback"])
async def test_peer_idempotent_reuse_precedes_the_new_live_capture_refusal(output_store, strategy):
    e = output_store
    key = "captured-key-" + uuid4().hex
    before = await predecessor(e, lease="live", key=key)
    with pytest.raises(RunIdempotencyConflict) as reused:
        await replace(e, before, strategy=strategy, key=key)
    assert reused.value.existing == before
    assert await e.repository.get(before["run_id"], user_id=RAW_OWNER) == before


@pytest.mark.asyncio
async def test_reject_idempotent_reuse_also_retains_existing_signal(output_store):
    e = output_store
    key = "captured-key-" + uuid4().hex
    before = await predecessor(e, lease="live", key=key)
    with pytest.raises(RunIdempotencyConflict) as reused:
        await replace(e, before, strategy="reject", key=key)
    assert reused.value.existing == before
    assert await e.repository.get(before["run_id"], user_id=RAW_OWNER) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("strategy", ["interrupt", "rollback"])
async def test_successor_insert_failure_rolls_back_predecessor_and_clock(output_store, strategy):
    e = output_store
    before = await predecessor(e)
    colliding = uuid4().hex
    await e.repository.put(colliding, thread_id=uuid4().hex, user_id=RAW_OWNER, status="success")
    async with e.sf() as session:
        clock = await session.scalar(select(RunChangeClockRow.value))
    with pytest.raises(IntegrityError):
        await replace(e, before, strategy=strategy, new_id=colliding)
    assert await e.repository.get(before["run_id"], user_id=RAW_OWNER) == before
    assert len(await e.repository.list_by_thread(before["thread_id"], user_id=RAW_OWNER)) == 1
    async with e.sf() as session:
        assert await session.scalar(select(RunChangeClockRow.value)) == clock


@pytest.mark.asyncio
@pytest.mark.parametrize("strategy", ["interrupt", "rollback"])
async def test_ordinary_same_owner_live_replacement_keeps_legacy_behavior(output_store, strategy):
    e = output_store
    before = await predecessor(e, capture=False, lease="live")
    created, claimed = await replace(e, before, strategy=strategy, owner="original-worker")
    after = await e.repository.get(before["run_id"], user_id=RAW_OWNER)
    assert after["status"] == "interrupted" and after["kwargs"] == before["kwargs"]
    assert created["status"] == "pending" and len(claimed) == 1


class ReplacementCommitResponseLost(RuntimeError):
    pass


@pytest.mark.asyncio
@pytest.mark.parametrize("strategy", ["interrupt", "rollback"])
async def test_lost_replacement_commit_response_preserves_seal_and_same_successor(output_store, monkeypatch, strategy):
    e = output_store
    before = await predecessor(e)
    key = "replacement-key-" + uuid4().hex
    commit = AsyncSession.commit
    observed = []

    async def lose_response(session):
        await commit(session)
        if session.bind is e.engine and not observed:
            observed.append(True)
            raise ReplacementCommitResponseLost("Fixed synthetic response loss")

    monkeypatch.setattr(AsyncSession, "commit", lose_response)
    with pytest.raises((ReplacementCommitResponseLost, TerminalWriteFailure)):
        await replace(e, before, strategy=strategy, key=key)
    assert observed
    after = await e.repository.get(before["run_id"], user_id=RAW_OWNER)
    saved = await e.repository.get_by_idempotency_key(key)
    assert after["status"] == "error" and receipt_from_kwargs(after["kwargs"]).reason == "failed"
    assert saved["status"] == "pending" and saved["run_id"] != before["run_id"]
    assert saved["change_seq"] == after["change_seq"]
    with pytest.raises(RunIdempotencyConflict) as reused:
        await replace(e, before, strategy=strategy, key=key)
    assert reused.value.existing == saved
    assert await e.repository.get(before["run_id"], user_id=RAW_OWNER) == after
    assert await e.repository.get(saved["run_id"], user_id=RAW_OWNER) == saved


@pytest.mark.asyncio
@pytest.mark.parametrize("bad", [None, [], "unknown"])
async def test_corrupt_kwargs_cannot_prove_ordinary_replacement(output_store, bad):
    e = output_store
    row = await predecessor(e)
    async with e.sf.begin() as session:
        await session.execute(update(RunRow).where(RunRow.run_id == row["run_id"]).values(kwargs_json=bad))
    before = await e.repository.get(row["run_id"], user_id=RAW_OWNER)
    assert type(before["kwargs"]) is type(bad) and before["kwargs"] == bad
    with pytest.raises((OutputEvidenceError, TerminalWriteFailure)):
        await replace(e, before)
    assert await e.repository.get(row["run_id"], user_id=RAW_OWNER) == before
    assert len(await e.repository.list_by_thread(row["thread_id"], user_id=RAW_OWNER)) == 1


@pytest.mark.asyncio
async def test_actual_empty_dict_is_still_ordinary_replacement(output_store):
    e = output_store
    row = await predecessor(e)
    async with e.sf.begin() as session:
        await session.execute(update(RunRow).where(RunRow.run_id == row["run_id"]).values(kwargs_json={}))
    created, _claimed = await replace(e, row)
    after = await e.repository.get(row["run_id"], user_id=RAW_OWNER)
    assert after["status"] == "interrupted" and after["kwargs"] == {}
    assert created["status"] == "pending"
