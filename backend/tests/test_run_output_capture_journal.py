"""Capture-only journal ownership; real SQL writes, no source-proof stand-ins."""

import asyncio
from types import SimpleNamespace
from uuid import uuid4

import pytest
from _run_output_capture_test_support import capture_environment as capture_environment
from _run_output_capture_test_support import output_store as output_store
from _run_output_store_test_support import RAW_OWNER
from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.outputs import ChatGeneration, LLMResult
from sqlalchemy import event
from sqlalchemy.util import await_only

from deerflow.runtime.journal import RunJournal
from deerflow.runtime.runs.output_capture import OutputCaptureObserver
from deerflow.runtime.user_context import reset_current_user, set_current_user


def _journal(environment, *, progress=None):
    run_id = uuid4().hex
    observer = OutputCaptureObserver(owner_user_id=RAW_OWNER, thread_id="journal-thread", run_id=run_id)
    return RunJournal(run_id, "journal-thread", environment.event_store, flush_threshold=1, output_capture=observer, progress_reporter=progress, progress_flush_interval=0.01)


@pytest.mark.asyncio
async def test_fenced_close_drains_admitted_native_journal_write_without_starting_late_write(capture_environment):
    environment = capture_environment
    journal = _journal(environment)
    entered, release, cancelled = asyncio.Event(), asyncio.Event(), asyncio.Event()
    writes = []

    def pause(_connection, _cursor, statement, _parameters, _context, _many):
        if "INSERT INTO run_events" in statement:
            writes.append(statement.split(" ", 3)[:3])

            async def hold():
                entered.set()
                try:
                    await release.wait()
                except asyncio.CancelledError:
                    cancelled.set()
                    raise

            await_only(hold())

    token = set_current_user(SimpleNamespace(id=RAW_OWNER))
    event.listen(environment.engine.sync_engine, "after_cursor_execute", pause)
    close = None
    try:
        journal.record_middleware("synthetic", name="Synthetic", hook="before_model", action="record", changes={})
        await asyncio.wait_for(entered.wait(), 5)
        close = asyncio.create_task(journal.close(flush=False))
        await asyncio.sleep(0)
        assert journal._closed
        journal.record_middleware("synthetic", name="Synthetic", hook="after_model", action="late", changes={})
        close.cancel()
        close.cancel()
        done, _ = await asyncio.wait({close}, timeout=0.05)
        assert not done and not cancelled.is_set()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await close
        assert len(writes) == 1
        assert environment.engine.sync_engine.pool.checkedout() == 0
        rows = await environment.event_store.list_events("journal-thread", journal.run_id, user_id=RAW_OWNER)
        assert len(rows) == 1
        assert journal._store is None and not journal._pending_flush_tasks
    finally:
        release.set()
        if close is not None:
            await asyncio.gather(close, return_exceptions=True)
        await journal.close(flush=False)
        event.remove(environment.engine.sync_engine, "after_cursor_execute", pause)
        reset_current_user(token)


@pytest.mark.asyncio
@pytest.mark.parametrize("flush", [False, True])
async def test_dirty_admitted_progress_write_cannot_reschedule_after_capture_quiesce(capture_environment, flush):
    environment = capture_environment
    record = await environment.manager.create_or_reject("journal-thread", user_id=RAW_OWNER)
    await environment.manager.try_start(record.run_id)
    entered, release = asyncio.Event(), asyncio.Event()
    writes = []

    async def report(snapshot):
        writes.append(dict(snapshot))
        entered.set()
        await release.wait()
        await environment.manager.update_run_progress(record.run_id, **snapshot)

    journal = _journal(environment, progress=report)
    journal._schedule_progress_flush()
    await asyncio.wait_for(entered.wait(), 5)
    journal._progress_dirty = True
    close = asyncio.create_task(journal.close(flush=flush))
    try:
        await asyncio.sleep(0)
        assert journal._closed
        close.cancel()
        close.cancel()
        done, _ = await asyncio.wait({close}, timeout=0.05)
        assert not done
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await close
        # The real progress SQL committed; its dirty-bit tail cannot create a
        # new delayed task after the producer fence and SQL teardown.
        assert len(writes) == 1
        assert journal._pending_progress_task is None
        assert journal._progress_reporter is None
        assert environment.engine.sync_engine.pool.checkedout() == 0
    finally:
        release.set()
        await asyncio.gather(close, return_exceptions=True)
        await journal.close(flush=False)


@pytest.mark.asyncio
async def test_capture_quiesce_preserves_exact_frozen_completion_before_detaching(capture_environment):
    journal = _journal(capture_environment)
    token = set_current_user(SimpleNamespace(id=RAW_OWNER))
    try:
        human = HumanMessage("Original input", id="human")
        model_id = uuid4()
        journal.on_chat_model_start({}, [[human]], run_id=model_id)
        journal.on_llm_end(LLMResult(generations=[[ChatGeneration(message=AIMessage("Canonical summary", id="assistant", usage_metadata={"input_tokens": 3, "output_tokens": 4, "total_tokens": 7}))]]), run_id=model_id)
        await journal.quiesce_output_capture(flush=True)
        completion = journal.get_completion_data()
        assert completion["first_human_message"] == "Original input"
        assert completion["last_ai_message"] == "Canonical summary"
        assert completion["total_tokens"] == 7
        assert journal._store is None and journal._first_human_msg is None
        completion["token_usage_by_model"]["mutated"] = {"total_tokens": 999}
        assert "mutated" not in journal.get_completion_data()["token_usage_by_model"]
    finally:
        await journal.close(flush=False)
        reset_current_user(token)


@pytest.mark.asyncio
@pytest.mark.parametrize("queued", ["flush", "progress", "force_flush"])
async def test_fenced_capture_close_does_not_admit_queued_not_started_writer(capture_environment, queued):
    calls = []

    async def report(snapshot):
        calls.append(snapshot)

    journal = _journal(capture_environment, progress=report)
    token = set_current_user(SimpleNamespace(id=RAW_OWNER))
    force = None
    try:
        if queued == "flush":
            journal.record_middleware("synthetic", name="Synthetic", hook="before_model", action="queued", changes={})
        elif queued == "progress":
            journal._schedule_progress_flush()
        else:
            force = asyncio.create_task(journal.flush())
        # No yield has allowed either scheduled task to start. Fencing now
        # must drain that task without admitting a new store/reporter call.
        await journal.close(flush=False)
        if force is not None:
            await force
        rows = await capture_environment.event_store.list_events("journal-thread", journal.run_id, user_id=RAW_OWNER)
        assert rows == [] and calls == []
    finally:
        await journal.close(flush=False)
        if force is not None:
            await asyncio.gather(force, return_exceptions=True)
        reset_current_user(token)


@pytest.mark.asyncio
@pytest.mark.parametrize("fence_during_write", [False, True])
async def test_public_force_flush_and_fenced_close_retain_actual_native_write(capture_environment, fence_during_write):
    journal = _journal(capture_environment)
    journal._flush_threshold = 1000
    entered, release, cancelled = asyncio.Event(), asyncio.Event(), asyncio.Event()

    def pause(_connection, _cursor, statement, _parameters, _context, _many):
        if "INSERT INTO run_events" in statement:

            async def hold():
                entered.set()
                try:
                    await release.wait()
                except asyncio.CancelledError:
                    cancelled.set()
                    raise

            await_only(hold())

    token = set_current_user(SimpleNamespace(id=RAW_OWNER))
    event.listen(capture_environment.engine.sync_engine, "after_cursor_execute", pause)
    force = close = None
    try:
        journal.record_middleware("synthetic", name="Synthetic", hook="before_model", action="force", changes={})
        force = asyncio.create_task(journal.flush())
        await asyncio.wait_for(entered.wait(), 5)
        force.cancel()
        force.cancel()
        if fence_during_write:
            close = asyncio.create_task(journal.close(flush=False))
        done, _ = await asyncio.wait({task for task in (force, close) if task is not None}, timeout=0.05)
        assert not done and not cancelled.is_set()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await force
        if close is not None:
            await close
        else:
            assert not journal._closed and not journal._capture_settled
        assert not journal._capture_force_flush_tasks
        assert capture_environment.engine.sync_engine.pool.checkedout() == 0
        rows = await capture_environment.event_store.list_events("journal-thread", journal.run_id, user_id=RAW_OWNER)
        assert len(rows) == 1
    finally:
        release.set()
        await asyncio.gather(*(task for task in (force, close) if task is not None), return_exceptions=True)
        await journal.close(flush=False)
        event.remove(capture_environment.engine.sync_engine, "after_cursor_execute", pause)
        reset_current_user(token)
