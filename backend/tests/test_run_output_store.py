"""Optional output seals use the actual owned SQL terminal transaction."""

import asyncio
import sqlite3
from contextlib import asynccontextmanager
from contextvars import ContextVar
from copy import deepcopy
from datetime import UTC, datetime, timedelta

import pytest
from _run_output_store_test_support import RAW_OWNER, capture_run
from _run_output_store_test_support import output_store as output_store
from sqlalchemy import event, func, select, text, update

from deerflow.persistence.run.model import RunRow
from deerflow.runtime.runs.schemas import RunStatus


def receipt(record, *, unavailable=None):
    from deerflow.runtime.runs.output import validate_receipt

    fields = {"version": 1, "owner_user_id": record.user_id, "thread_id": record.thread_id, "run_id": record.run_id, "input_message_id": "human-source"}
    if unavailable:
        fields.update(kind="unavailable", reason=unavailable)
    else:
        fields.update(kind="text", assistant_message_id="actual-assistant-source", event_seq=2, text_sha256="a" * 64)
    return validate_receipt(fields)


@pytest.mark.asyncio
@pytest.mark.parametrize("creation", ["create", "create_or_reject"])
async def test_direct_new_admission_cannot_author_a_sealed_receipt(output_store, creation):
    from uuid import uuid4

    from deerflow.runtime.runs.manager import RunManager

    manager = RunManager(output_store.repository)
    caller_kwargs = {"output_capture": {"version": 1}, "output_evidence": {"kind": "text", "forged": "not-server-evidence"}, "input": {"messages": []}}
    record = await getattr(manager, creation)(uuid4().hex, user_id=RAW_OWNER, kwargs=caller_kwargs)
    row = await output_store.repository.get(record.run_id, user_id=RAW_OWNER)
    assert "output_evidence" not in row["kwargs"]
    assert "output_evidence" not in record.kwargs
    assert row["kwargs"]["output_capture"] == {"version": 1}
    assert caller_kwargs["output_evidence"] == {"kind": "text", "forged": "not-server-evidence"}


@pytest.mark.asyncio
async def test_direct_text_receipt_cannot_win_a_matching_durable_cancel(output_store):
    manager, record = await capture_run(output_store)
    await output_store.repository.request_cancel(record.run_id, action="interrupt")
    row = await output_store.repository.get(record.run_id, user_id=RAW_OWNER)
    result = await output_store.repository.finalize_owned(
        record.run_id,
        owner_worker_id=record.owner_worker_id,
        expected_change_seq=row["change_seq"],
        expected_status=row["status"],
        expected_cancel_action=row["cancel_action"],
        require_lease=False,
        status="success",
        output_receipt=receipt(record),
    )
    assert result is False
    assert await output_store.repository.get(record.run_id, user_id=RAW_OWNER) == row


class TerminalFault:
    """Fail an actual terminal UPDATE before or after its database commit."""

    def __init__(self, repository, *, after_commit):
        self.factory = repository._sf
        self.after_commit = after_commit
        self.failed = False
        self.writes = 0
        repository._sf = self.session

    @asynccontextmanager
    async def session(self):
        async with self.factory() as session:
            execute, commit = session.execute, session.commit
            terminal = False

            async def fault_execute(statement, *args, **kwargs):
                nonlocal terminal
                values = getattr(statement, "_values", None) or {}
                terminal = terminal or (getattr(statement, "is_update", False) and statement.table.name == "runs" and any(getattr(key, "name", key) == "status" for key in values))
                if terminal:
                    self.writes += 1
                    if not self.after_commit and not self.failed:
                        self.failed = True
                        raise ConnectionError("private-SQL-and-provider-output")
                return await execute(statement, *args, **kwargs)

            async def fault_commit():
                await commit()
                if terminal and self.after_commit and not self.failed:
                    self.failed = True
                    raise ConnectionError("private-SQL-and-provider-output")

            session.execute, session.commit = fault_execute, fault_commit
            yield session


@pytest.mark.asyncio
@pytest.mark.parametrize("heartbeat", [False, True])
async def test_seal_preserves_authoritative_kwargs_and_advances_change_position(output_store, heartbeat):
    from deerflow.runtime.runs.output import receipt_to_dict

    manager, record = await capture_run(output_store, heartbeat=heartbeat)
    original = await output_store.repository.get(record.run_id, user_id=RAW_OWNER)
    database_kwargs = {**deepcopy(original["kwargs"]), "database_only": {"nested": ["fresh"]}}
    async with output_store.sf() as session, session.begin():
        await session.execute(update(RunRow).where(RunRow.run_id == record.run_id).values(kwargs_json=database_kwargs))
    wanted = receipt(record)

    result = await manager.finalize_output(record.run_id, RunStatus.success, output_receipt=wanted)

    row = await output_store.repository.get(record.run_id, user_id=RAW_OWNER)
    assert result.finalized and result.output_receipt_confirmed
    assert row["status"] == "success" and row["change_seq"] > original["change_seq"]
    assert row["kwargs"] == {**database_kwargs, "output_evidence": receipt_to_dict(wanted)}
    assert record.kwargs["output_evidence"] == receipt_to_dict(wanted)
    await manager.update_run_completion(record.run_id, status="success", total_tokens=37)
    assert (await output_store.repository.get(record.run_id, user_id=RAW_OWNER))["kwargs"] == row["kwargs"]


@pytest.mark.asyncio
@pytest.mark.parametrize("heartbeat", [False, True])
@pytest.mark.parametrize("after_commit", [False, True])
async def test_receipt_transaction_recovery_reads_before_repeating(output_store, heartbeat, after_commit, caplog):
    from deerflow.runtime.runs.output import receipt_to_dict

    manager, record = await capture_run(output_store, heartbeat=heartbeat)
    wanted = receipt(record)
    fault = TerminalFault(output_store.repository, after_commit=after_commit)
    result = await manager.finalize_output(record.run_id, RunStatus.success, output_receipt=wanted)

    row = await output_store.repository.get(record.run_id, user_id=RAW_OWNER)
    assert result.finalized and result.output_receipt_confirmed
    assert row["status"] == "success" and row["kwargs"]["output_evidence"] == receipt_to_dict(wanted)
    assert fault.writes == (1 if after_commit else 2)
    assert "private-SQL-and-provider-output" not in caplog.text
    assert not any(item.exc_info for item in caplog.records if "terminal" in item.message.lower())


@pytest.mark.asyncio
@pytest.mark.parametrize("saved", ["missing", "different"])
async def test_terminal_status_never_promotes_a_missing_or_different_seal(output_store, saved):
    from deerflow.runtime.runs.output import receipt_to_dict

    manager, record = await capture_run(output_store)
    wanted = receipt(record)
    if saved == "different":
        kwargs = {**record.kwargs, "output_evidence": {**receipt_to_dict(wanted), "assistant_message_id": "other-source"}}
        async with output_store.sf() as session, session.begin():
            await session.execute(update(RunRow).where(RunRow.run_id == record.run_id).values(kwargs_json=kwargs))
    await manager.set_status_if_not_cancelled(record.run_id, RunStatus.success)
    before = await output_store.repository.get(record.run_id, user_id=RAW_OWNER)

    result = await manager.finalize_output(record.run_id, RunStatus.success, output_receipt=wanted)

    assert result.finalized and not result.output_receipt_confirmed
    assert await output_store.repository.get(record.run_id, user_id=RAW_OWNER) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("capture", [None, {"version": True}, {"version": 2}])
async def test_only_locked_supported_capture_intent_can_receive_a_seal(output_store, capture):
    manager, record = await capture_run(output_store)
    kwargs = deepcopy(record.kwargs)
    if capture is None:
        kwargs.pop("output_capture")
    else:
        kwargs["output_capture"] = capture
    async with output_store.sf() as session, session.begin():
        await session.execute(update(RunRow).where(RunRow.run_id == record.run_id).values(kwargs_json=kwargs))
    before = await output_store.repository.get(record.run_id, user_id=RAW_OWNER)

    result = await manager.finalize_output(record.run_id, RunStatus.success, output_receipt=receipt(record))

    assert not result.output_receipt_confirmed
    assert await output_store.repository.get(record.run_id, user_id=RAW_OWNER) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["interrupt", "rollback"])
async def test_cancellation_wins_before_success_receipt_update(output_store, monkeypatch, action):
    manager, record = await capture_run(output_store)
    entered, release = asyncio.Event(), asyncio.Event()
    original = output_store.repository.finalize_owned

    async def pause(*args, **kwargs):
        entered.set()
        await release.wait()
        return await original(*args, **kwargs)

    monkeypatch.setattr(output_store.repository, "finalize_owned", pause)
    task = asyncio.create_task(manager.finalize_output(record.run_id, RunStatus.success, output_receipt=receipt(record)))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        await output_store.repository.request_cancel(record.run_id, action=action)
        release.set()
        result = await asyncio.wait_for(task, 5)
        assert not result.finalized and not result.output_receipt_confirmed and result.cancel_action == action
        row = await output_store.repository.get(record.run_id, user_id=RAW_OWNER)
        assert row["status"] == "running" and "output_evidence" not in row["kwargs"]
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["worker", "version", "lease", "raw_owner", "thread", "capture"])
async def test_receipt_writer_recaptures_exact_scope_after_await(output_store, monkeypatch, change):
    manager, record = await capture_run(output_store, heartbeat=change == "lease")
    entered, release = asyncio.Event(), asyncio.Event()
    original = output_store.repository.finalize_owned

    async def pause(*args, **kwargs):
        entered.set()
        await release.wait()
        return await original(*args, **kwargs)

    monkeypatch.setattr(output_store.repository, "finalize_owned", pause)
    task = asyncio.create_task(manager.finalize_output(record.run_id, RunStatus.success, output_receipt=receipt(record)))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        values = {
            "worker": {"owner_worker_id": "successor-worker"},
            "version": {"change_seq": RunRow.change_seq + 1},
            "lease": {"lease_expires_at": datetime.now(UTC) - timedelta(seconds=1)},
            "raw_owner": {"user_id": "different-raw-owner"},
            "thread": {"thread_id": "different-original-thread"},
            "capture": {"kwargs_json": {"input": {"messages": []}}},
        }[change]
        async with output_store.sf() as session, session.begin():
            await session.execute(update(RunRow).where(RunRow.run_id == record.run_id).values(**values))
        expected = await output_store.repository.get(record.run_id, user_id=None)
        release.set()
        result = await asyncio.wait_for(task, 5)
        assert not result.finalized and not result.output_receipt_confirmed
        assert await output_store.repository.get(record.run_id, user_id=None) == expected
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_unavailable_receipt_survives_the_existing_owned_rollback_tail(output_store):
    from deerflow.runtime.runs.output import receipt_to_dict

    manager, record = await capture_run(output_store)
    await output_store.repository.request_cancel(record.run_id, action="rollback")
    record.abort_action = "rollback"
    record.abort_event.set()
    wanted = receipt(record, unavailable="cancelled")
    first = await manager.finalize_output(record.run_id, RunStatus.interrupted, output_receipt=wanted, respect_cancellation=False)
    second = await manager.finalize_output(record.run_id, RunStatus.error, output_receipt=wanted, respect_cancellation=False)
    row = await output_store.repository.get(record.run_id, user_id=RAW_OWNER)
    assert first.output_receipt_confirmed and second.output_receipt_confirmed
    assert row["status"] == "error" and row["kwargs"]["output_evidence"] == receipt_to_dict(wanted)


@pytest.mark.asyncio
@pytest.mark.parametrize("entry", ["sql", "manager"])
async def test_interrupted_unsealed_row_never_receives_a_new_rollback_seal(output_store, entry):
    manager, record = await capture_run(output_store)
    await output_store.repository.request_cancel(record.run_id, action="rollback")
    await manager.set_status(record.run_id, RunStatus.interrupted)
    record.abort_event.set()
    record.abort_action = "rollback"
    before = await output_store.repository.get(record.run_id, user_id=RAW_OWNER)
    assert before["status"] == "interrupted" and "output_evidence" not in before["kwargs"]
    wanted = receipt(record, unavailable="cancelled")
    if entry == "sql":
        result = await output_store.repository.finalize_owned(
            record.run_id,
            owner_worker_id=record.owner_worker_id,
            expected_change_seq=before["change_seq"],
            expected_status="interrupted",
            expected_cancel_action="rollback",
            require_lease=False,
            status="error",
            output_receipt=wanted,
        )
        assert result is False
    else:
        result = await manager.finalize_output(record.run_id, RunStatus.error, output_receipt=wanted, respect_cancellation=False)
        assert not result.output_receipt_confirmed
    assert await output_store.repository.get(record.run_id, user_id=RAW_OWNER) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("entry", ["sql", "manager"])
@pytest.mark.parametrize("action", [None, "interrupt", "rollback", "contradictory_receipt"])
async def test_only_an_actual_rollback_preserves_a_valid_interrupted_seal(output_store, entry, action):
    from deerflow.runtime.runs.output import receipt_to_dict

    manager, record = await capture_run(output_store)
    stored_action = "rollback" if action == "contradictory_receipt" else action
    if stored_action is not None:
        await output_store.repository.request_cancel(record.run_id, action=stored_action)
    wanted = receipt(record, unavailable="cancelled")
    await manager.finalize_output(record.run_id, RunStatus.interrupted, output_receipt=wanted, respect_cancellation=False)
    if action == "contradictory_receipt":
        wanted = receipt(record, unavailable="failed")
        kwargs = {**record.kwargs, "output_evidence": receipt_to_dict(wanted)}
        async with output_store.sf() as session, session.begin():
            await session.execute(update(RunRow).where(RunRow.run_id == record.run_id).values(kwargs_json=kwargs))
    record.abort_event.set()
    record.abort_action = "rollback"
    before = await output_store.repository.get(record.run_id, user_id=RAW_OWNER)
    if entry == "sql":
        accepted = await output_store.repository.finalize_owned(
            record.run_id,
            owner_worker_id=record.owner_worker_id,
            expected_change_seq=before["change_seq"],
            expected_status="interrupted",
            expected_cancel_action=stored_action,
            require_lease=False,
            status="error",
            output_receipt=wanted,
        )
    else:
        result = await manager.finalize_output(record.run_id, RunStatus.error, output_receipt=wanted, respect_cancellation=False)
        accepted = result.output_receipt_confirmed
    assert accepted is (action == "rollback")
    if action != "rollback":
        assert await output_store.repository.get(record.run_id, user_id=RAW_OWNER) == before


@pytest.mark.asyncio
async def test_idempotent_reuse_cannot_replace_an_existing_real_seal(output_store):
    from uuid import uuid4

    from deerflow.runtime.runs.manager import RunManager
    from deerflow.runtime.runs.output import receipt_to_dict

    manager = RunManager(output_store.repository)
    thread_id, key = uuid4().hex, uuid4().hex
    original = {"output_capture": {"version": 1}, "input": {"messages": []}}
    record = await manager.create_or_reject(thread_id, user_id=RAW_OWNER, kwargs=original, idempotency_key=key)
    wanted = receipt(record)
    await manager.finalize_output(record.run_id, RunStatus.success, output_receipt=wanted)
    before = await output_store.repository.get(record.run_id, user_id=RAW_OWNER)
    peer = RunManager(output_store.repository)
    replay = await peer.create_or_reject(thread_id, user_id=RAW_OWNER, kwargs={**original, "output_evidence": {"forged": True}}, idempotency_key=key)
    assert replay.run_id == record.run_id and replay.idempotency_reused
    assert replay.kwargs["output_evidence"] == receipt_to_dict(wanted)
    assert await output_store.repository.get(record.run_id, user_id=RAW_OWNER) == before


@pytest.mark.asyncio
async def test_actual_frozen_snapshot_composes_with_the_shared_receipt_parsers(output_store):
    from deerflow.runtime.runs.output import capture_from_kwargs, receipt_from_kwargs, receipt_to_dict

    manager, record = await capture_run(output_store)
    wanted = receipt(record)
    await manager.finalize_output(record.run_id, RunStatus.success, output_receipt=wanted)
    snapshot = await manager.get_output_snapshot(record.run_id, user_id=RAW_OWNER)
    assert capture_from_kwargs(snapshot.kwargs).version == 1
    assert receipt_from_kwargs(snapshot.kwargs) == wanted
    assert receipt_to_dict(receipt_from_kwargs(snapshot.kwargs)) == receipt_to_dict(wanted)


@pytest.mark.asyncio
async def test_clock_precedes_row_lock_and_lease_is_current_after_lock_wait(output_store):
    _manager, record = await capture_run(output_store, heartbeat=True)
    row = await output_store.repository.get(record.run_id, user_id=RAW_OWNER)
    role = ContextVar("output_receipt_lock_test", default=False)
    clock_entered, row_entered = asyncio.Event(), asyncio.Event()
    operations = []

    def observe(_connection, _cursor, statement, _parameters, _context, _many):
        if not role.get():
            return
        if "INTO run_change_clock" in statement:
            clock_entered.set()
        if statement.startswith("UPDATE run_change_clock"):
            operations.append("clock")
        if "FROM runs" in statement:
            operations.append("row")
            row_entered.set()

    async def seal():
        token = role.set(True)
        try:
            return await output_store.repository.finalize_owned(
                record.run_id, owner_worker_id=record.owner_worker_id, expected_change_seq=row["change_seq"], expected_status="running", expected_cancel_action=None, require_lease=True, status="success", output_receipt=receipt(record)
            )
        finally:
            role.reset(token)

    event.listen(output_store.engine.sync_engine, "before_cursor_execute", observe)
    task = None
    try:
        async with output_store.engine.connect() as holder:
            transaction = await holder.begin()
            try:
                if output_store.dialect == "sqlite":
                    await holder.execute(text("BEGIN IMMEDIATE"))
                else:
                    await holder.execute(select(RunRow.run_id).where(RunRow.run_id == record.run_id).with_for_update())
                task = asyncio.create_task(seal())
                await asyncio.wait_for((row_entered if output_store.dialect == "postgres" else clock_entered).wait(), 5)
                assert not task.done()
                # This database time is AFTER the proof transaction began. A
                # frozen PostgreSQL now() would wrongly admit the waiting CAS.
                clock = func.clock_timestamp() if output_store.dialect == "postgres" else func.current_timestamp()
                await holder.execute(update(RunRow).where(RunRow.run_id == record.run_id).values(lease_expires_at=clock))
                await transaction.commit()
                assert await asyncio.wait_for(task, 5) is False
            finally:
                if transaction.is_active:
                    await transaction.rollback()
                if task is not None:
                    await asyncio.gather(task, return_exceptions=True)
        assert operations.index("clock") < operations.index("row")
        stored = await output_store.repository.get(record.run_id, user_id=RAW_OWNER)
        assert stored["status"] == "running" and "output_evidence" not in stored["kwargs"]
    finally:
        event.remove(output_store.engine.sync_engine, "before_cursor_execute", observe)


@pytest.mark.asyncio
@pytest.mark.parametrize("output_store", ["sqlite"], indirect=True)
@pytest.mark.parametrize("entry,transactional", [("manager", False), ("manager", True), ("sql", True)], ids=["proof-read", "receipt-row-read", "direct-sql-row-read"])
async def test_cancelled_proof_retains_native_cursor_until_independent_commit(output_store, monkeypatch, entry, transactional):
    import aiosqlite

    manager, record = await capture_run(output_store)
    original_row = await output_store.repository.get(record.run_id, user_id=RAW_OWNER)
    entered, release, cursor_cancelled, cursor_closed = (asyncio.Event() for _ in range(4))
    role = ContextVar("output_native_cursor_role", default=False)
    native_cursor = []
    native_connection = []
    execute = aiosqlite.Connection._execute

    async def native_execute(connection, function, *args, **kwargs):
        result = await execute(connection, function, *args, **kwargs)
        if native_cursor and getattr(function, "__name__", "") == "close" and getattr(function, "__self__", None) is native_cursor[0]:
            cursor_closed.set()
        if not native_cursor and role.get() and getattr(function, "__name__", "") == "execute" and args and str(args[0]).lstrip().upper().startswith("SELECT") and connection.in_transaction is transactional:
            native_cursor.append(function.__self__)
            native_connection.append(connection)
            entered.set()
            try:
                await release.wait()
            except asyncio.CancelledError:
                cursor_cancelled.set()
                raise
        return result

    async def finalize():
        token = role.set(True)
        try:
            if entry == "sql":
                return await output_store.repository.finalize_owned(
                    record.run_id,
                    owner_worker_id=record.owner_worker_id,
                    expected_change_seq=original_row["change_seq"],
                    expected_status="running",
                    expected_cancel_action=None,
                    require_lease=False,
                    status="success",
                    output_receipt=receipt(record),
                )
            return await manager.finalize_output(record.run_id, RunStatus.success, output_receipt=receipt(record))
        finally:
            role.reset(token)

    monkeypatch.setattr(aiosqlite.Connection, "_execute", native_execute)
    task = asyncio.create_task(finalize())
    watcher = asyncio.create_task(cursor_cancelled.wait())
    try:
        await asyncio.wait_for(entered.wait(), 5)
        task.cancel()
        task.cancel()
        done, _ = await asyncio.wait({task, watcher}, timeout=0.05, return_when=asyncio.FIRST_COMPLETED)
        premature = bool(done)
        release.set()
        done, _ = await asyncio.wait({task}, timeout=5)
        assert task in done
        with pytest.raises(asyncio.CancelledError):
            await task

        async def commit():
            async with output_store.sf() as session, session.begin():
                await session.execute(update(RunRow).where(RunRow.run_id == record.run_id).values(total_tokens=37))

        await asyncio.wait_for(commit(), 3)
        assert (await output_store.repository.get(record.run_id, user_id=RAW_OWNER))["total_tokens"] == 37
        assert not premature, "proof canceled the active native SELECT (independent writer COMMIT/readback did succeed)"
        assert cursor_closed.is_set()
    finally:
        release.set()
        watcher.cancel()
        await asyncio.gather(task, watcher, return_exceptions=True)
        # Baseline evidence must not strand the deliberately observed native
        # cursor after its canceled wrapper lost ownership. Never hide a red
        # assertion by letting fixture disposal race that live statement.
        if native_cursor and not cursor_closed.is_set():
            try:
                await execute(native_connection[0], native_cursor[0].close)
            except ValueError:
                if native_connection[0]._running:
                    raise

                # SQLAlchemy may invalidate/stop the driver while the observed
                # native statement still owns its handle. The fixture explicitly
                # permits this final off-loop close after that driver has stopped.
                def close_stopped_cursor():
                    try:
                        native_cursor[0].close()
                    except sqlite3.ProgrammingError:
                        # A fully closed native connection has no surviving lock.
                        try:
                            native_cursor[0].connection.in_transaction
                        except sqlite3.ProgrammingError:
                            return
                        raise

                await asyncio.to_thread(close_stopped_cursor)
