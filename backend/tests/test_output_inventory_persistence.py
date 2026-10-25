"""Capture2 pairs use the existing real SQL terminal authority.

Seeded receipts here qualify persistence only; real worker provenance is tested
by the integration owner. API-absence cases are new-feature scaffolds.
"""

import asyncio
from contextvars import ContextVar
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from _run_output_store_test_support import RAW_OWNER
from _run_output_store_test_support import output_store as output_store

from deerflow.runtime.runs.output import OutputEvidenceError


def output_receipt(row, *, reason=None, input_id="genuine-human"):
    from deerflow.runtime.runs.output import TextOutputReceipt, UnavailableOutputReceipt

    scope = dict(version=1, owner_user_id=row["user_id"], thread_id=row["thread_id"], run_id=row["run_id"], input_message_id=input_id)
    return UnavailableOutputReceipt(kind="unavailable", reason=reason, **scope) if reason else TextOutputReceipt(kind="text", assistant_message_id="legal-opaque-" + "a" * 100, event_seq=3, text_sha256="a" * 64, **scope)


def event_evidence(output):
    from deerflow.runtime.runs.output_inventory import EventInventoryEvidence, output_receipt_digest

    return EventInventoryEvidence(
        version=1,
        kind="event",
        owner_user_id=output.owner_user_id,
        thread_id=output.thread_id,
        run_id=output.run_id,
        input_message_id=output.input_message_id,
        output_receipt_sha256=output_receipt_digest(output),
        inventory_event_seq=5,
        inventory_sha256="f" * 64,
    )


async def seal(e, row, output, inventory):
    return await e.repository.finalize_owned(
        row["run_id"],
        owner_worker_id=row["owner_worker_id"],
        expected_change_seq=row["change_seq"],
        expected_status=row["status"],
        expected_cancel_action=row["cancel_action"],
        require_lease=row["lease_expires_at"] is not None,
        status="success" if output.kind == "text" else "interrupted" if output.reason == "cancelled" else "error",
        output_receipt=output,
        inventory_evidence=inventory,
    )


async def seed_run(e, *, version=2, status="running", lease=None, kwargs=None):
    run_id, thread_id = uuid4().hex, uuid4().hex
    values = {"retained": {"nested": ["original"]}}
    if version is not None:
        values.update(output_capture={"version": version}, output_capture_on_disconnect="continue")
    values.update(kwargs or {})
    await e.repository.put(run_id, thread_id=thread_id, user_id=RAW_OWNER, owner_worker_id="inventory-owner", lease_expires_at=lease, status=status, kwargs=values)
    return await e.repository.get(run_id, user_id=RAW_OWNER)


async def terminalize(e, row, method):
    repository, run_id = e.repository, row["run_id"]
    if method == "owned":
        return await repository.finalize_owned(
            run_id, owner_worker_id=row["owner_worker_id"], expected_change_seq=row["change_seq"], expected_status=row["status"], expected_cancel_action=row["cancel_action"], require_lease=False, status="success"
        )
    if method == "status":
        return await repository.update_status(run_id, "success")
    if method == "completion":
        return await repository.update_run_completion(run_id, status="success", total_tokens=37)
    if method == "uncancelled":
        return await repository.finalize_if_not_cancelled(run_id, status="success")
    return await repository.put(run_id, thread_id=row["thread_id"], user_id=RAW_OWNER, owner_worker_id=row["owner_worker_id"], status="success", kwargs=deepcopy(row["kwargs"]))


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["owned", "status", "completion", "uncancelled", "put"])
async def test_capture2_status_only_writers_cannot_terminalize(output_store, method):
    before = await seed_run(output_store)
    try:
        await terminalize(output_store, before, method)
    except OutputEvidenceError:
        pass
    assert await output_store.repository.get(before["run_id"], user_id=RAW_OWNER) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("version", [None, 1])
@pytest.mark.parametrize("method", ["owned", "status", "completion", "uncancelled", "put"])
async def test_ordinary_and_v1_status_writers_keep_their_contract(output_store, version, method):
    before = await seed_run(output_store, version=version)
    await terminalize(output_store, before, method)
    after = await output_store.repository.get(before["run_id"], user_id=RAW_OWNER)
    assert after["status"] == "success"
    assert after["kwargs"] == before["kwargs"]


@pytest.mark.asyncio
@pytest.mark.parametrize("transition", ["takeover", "replacement"])
async def test_capture2_real_lost_owner_transition_builds_both_proofs_without_events(output_store, transition):
    from deerflow.runtime.runs.output import receipt_from_kwargs

    before = await seed_run(output_store, lease=(datetime.now(UTC) - timedelta(hours=1)).isoformat())
    if transition == "takeover":
        assert await output_store.repository.claim_for_takeover(before["run_id"], grace_seconds=10, error="Owner lost")
    else:
        await output_store.repository.create_thread_operation_atomic(uuid4().hex, thread_id=before["thread_id"], user_id=RAW_OWNER, owner_worker_id="successor", lease_expires_at=None, multitask_strategy="interrupt")
    row = await output_store.repository.get(before["run_id"], user_id=RAW_OWNER)
    output = receipt_from_kwargs(row["kwargs"])
    assert output.reason == "failed" and row["status"] == "error"
    inventory = row["kwargs"]["inventory_evidence"]
    assert inventory["kind"] == "unavailable"
    assert inventory["reason"] == ("lease_takeover_not_enumerated" if transition == "takeover" else "replacement_not_enumerated")
    assert inventory["input_message_id"] is None
    assert row["owner_worker_id"] == before["owner_worker_id"]
    assert row["kwargs"]["retained"] == before["kwargs"]["retained"]


@pytest.mark.asyncio
async def test_pair_cas_preserves_locked_kwargs_and_both_proofs_during_metadata(output_store):
    from sqlalchemy import update

    from deerflow.persistence.run.model import RunRow
    from deerflow.runtime.runs.output import receipt_to_dict
    from deerflow.runtime.runs.output_inventory import inventory_evidence_to_dict

    before = await seed_run(output_store)
    fresh = {**before["kwargs"], "late": {"retained": [37]}}
    async with output_store.sf.begin() as session:
        await session.execute(update(RunRow).where(RunRow.run_id == before["run_id"]).values(kwargs_json=fresh))
    output = output_receipt(before)
    inventory = event_evidence(output)
    assert await seal(output_store, before, output, inventory)
    row = await output_store.repository.get(before["run_id"], user_id=RAW_OWNER)
    assert row["kwargs"] == {**fresh, "output_evidence": receipt_to_dict(output), "inventory_evidence": inventory_evidence_to_dict(inventory)}
    assert row["change_seq"] > before["change_seq"]
    await output_store.repository.update_run_progress(row["run_id"], total_tokens=37)
    await output_store.repository.update_run_completion(row["run_id"], status="success", total_tokens=41, expected_owner_worker_id=row["owner_worker_id"])
    assert (await output_store.repository.get(row["run_id"], user_id=RAW_OWNER))["kwargs"] == row["kwargs"]


@pytest.mark.asyncio
@pytest.mark.parametrize("saved", ["output_only", "inventory_only", "terminal_empty", "terminal_output_only"])
async def test_half_pairs_and_terminal_rows_cannot_acquire_missing_proof(output_store, saved):
    from sqlalchemy import update

    from deerflow.persistence.run.model import RunRow
    from deerflow.runtime.runs.output import receipt_to_dict
    from deerflow.runtime.runs.output_inventory import inventory_evidence_to_dict
    from deerflow.runtime.runs.terminal import TerminalWriteFailure

    row = await seed_run(output_store)
    output = output_receipt(row)
    inventory = event_evidence(output)
    values = deepcopy(row["kwargs"])
    if "output_only" in saved:
        values["output_evidence"] = receipt_to_dict(output)
    if saved == "inventory_only":
        values["inventory_evidence"] = inventory_evidence_to_dict(inventory)
    async with output_store.sf.begin() as session:
        await session.execute(update(RunRow).where(RunRow.run_id == row["run_id"]).values(kwargs_json=values, status="success" if saved.startswith("terminal") else "running"))
    before = await output_store.repository.get(row["run_id"], user_id=RAW_OWNER)
    try:
        assert await seal(output_store, before, output, inventory) is False
    except (OutputEvidenceError, TerminalWriteFailure):
        pass
    assert await output_store.repository.get(row["run_id"], user_id=RAW_OWNER) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("cause", ["prestart_not_enumerated", "lease_takeover_not_enumerated", "replacement_not_enumerated", "cancellation_after_inventory_not_enumerated"])
async def test_valid_unavailable_dto_alone_does_not_authorize_lifecycle(output_store, cause):
    from deerflow.runtime.runs.output_inventory import build_unavailable_inventory_evidence

    row = await seed_run(output_store)
    output = output_receipt(row, reason="cancelled" if cause.startswith("cancellation") else "failed")
    inventory = build_unavailable_inventory_evidence(output, status="interrupted" if output.reason == "cancelled" else "error", reason=cause)
    assert await seal(output_store, row, output, inventory) is False
    assert await output_store.repository.get(row["run_id"], user_id=RAW_OWNER) == row


@pytest.mark.asyncio
async def test_pair_sql_cancellation_owns_executed_query_until_transaction_exit(output_store):
    from sqlalchemy import event, update
    from sqlalchemy.util import await_only

    from deerflow.persistence.run.model import RunRow

    row = await seed_run(output_store)
    output = output_receipt(row)
    entered, release = asyncio.Event(), asyncio.Event()
    role = ContextVar("inventory_pair_native_read", default=False)

    def hold(_connection, _cursor, statement, _parameters, _context, _many):
        if role.get() and not entered.is_set() and statement.lstrip().upper().startswith("SELECT") and "FROM runs" in statement:
            entered.set()
            await_only(release.wait())

    async def finalize():
        token = role.set(True)
        try:
            return await seal(output_store, row, output, event_evidence(output))
        finally:
            role.reset(token)

    event.listen(output_store.engine.sync_engine, "after_cursor_execute", hold)
    task = asyncio.create_task(finalize())
    try:
        await asyncio.wait_for(entered.wait(), 5)
        task.cancel()
        task.cancel()
        done, _ = await asyncio.wait({task}, timeout=0.05)
        assert not done, "Caller escaped while its real pair SELECT was admitted"
        release.set()
        await asyncio.wait_for(asyncio.gather(task, return_exceptions=True), 5)
        assert task.cancelled()
        async with output_store.sf.begin() as session:
            await session.execute(update(RunRow).where(RunRow.run_id == row["run_id"]).values(total_tokens=37))
        saved = await output_store.repository.get(row["run_id"], user_id=RAW_OWNER)
        assert saved["total_tokens"] == 37
        assert saved["kwargs"]["inventory_evidence"]["inventory_sha256"] == "f" * 64
        assert output_store.engine.sync_engine.pool.checkedout() == 0
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        event.remove(output_store.engine.sync_engine, "after_cursor_execute", hold)


@pytest.mark.asyncio
@pytest.mark.parametrize("output_store", ["sqlite"], indirect=True)
@pytest.mark.parametrize("journal_mode", ["DELETE", "WAL"])
async def test_pair_native_sqlite_cursor_closes_before_cancelled_caller_returns(output_store, monkeypatch, journal_mode):
    import aiosqlite
    from sqlalchemy import text, update

    from deerflow.persistence.run.model import RunRow

    async with output_store.engine.connect() as connection:
        assert (await connection.execute(text(f"PRAGMA journal_mode={journal_mode}"))).scalar_one().upper() == journal_mode
    row = await seed_run(output_store)
    output = output_receipt(row)
    entered, release, closed = asyncio.Event(), asyncio.Event(), asyncio.Event()
    role = ContextVar("inventory_pair_native_cursor", default=False)
    execute = aiosqlite.Connection._execute
    cursors, connections = [], []

    async def native(connection, function, *args, **kwargs):
        result = await execute(connection, function, *args, **kwargs)
        if cursors and getattr(function, "__name__", "") == "close" and getattr(function, "__self__", None) is cursors[0]:
            closed.set()
        if role.get() and not cursors and getattr(function, "__name__", "") == "execute" and args and str(args[0]).lstrip().upper().startswith("SELECT") and "FROM runs" in str(args[0]):
            cursors.append(function.__self__)
            connections.append(connection)
            entered.set()
            await release.wait()  # Actual native execute returned, fetch/close has not.
        return result

    async def finalize():
        token = role.set(True)
        try:
            return await seal(output_store, row, output, event_evidence(output))
        finally:
            role.reset(token)

    monkeypatch.setattr(aiosqlite.Connection, "_execute", native)
    task = asyncio.create_task(finalize())
    try:
        await asyncio.wait_for(entered.wait(), 5)
        task.cancel()
        task.cancel()
        done, _ = await asyncio.wait({task}, timeout=0.05)
        assert not done and not closed.is_set()
        release.set()
        await asyncio.wait_for(asyncio.gather(task, return_exceptions=True), 5)
        assert task.cancelled() and closed.is_set()

        async def independent_commit():
            async with output_store.sf.begin() as session:
                await session.execute(update(RunRow).where(RunRow.run_id == row["run_id"]).values(total_tokens=37))

        await asyncio.wait_for(independent_commit(), 3)
        assert (await output_store.repository.get(row["run_id"], user_id=RAW_OWNER))["total_tokens"] == 37
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        if cursors and not closed.is_set():
            try:
                await execute(connections[0], cursors[0].close)
            except ValueError:
                if connections[0]._running:
                    raise
