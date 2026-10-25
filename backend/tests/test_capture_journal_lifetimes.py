"""Journal producers and finite I/O share their original host lifetime."""

import asyncio
import time
from types import SimpleNamespace
from uuid import uuid4

import pytest
import pytest_asyncio
from _run_output_capture_test_support import capture_environment as capture_environment
from _run_output_capture_test_support import output_store as output_store  # noqa: F401
from _run_output_store_test_support import RAW_OWNER
from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.outputs import ChatGeneration, LLMResult
from sqlalchemy import event
from sqlalchemy.util import await_only

from deerflow.runtime.journal import RunJournal
from deerflow.runtime.runs.capture_io import CaptureIOClosed, CaptureIOOwner, capture_io_scope
from deerflow.runtime.runs.output import OutputEvidenceError, canonical_text_digest
from deerflow.runtime.runs.output_capture import OutputCaptureObserver
from deerflow.runtime.runs.schemas import RunStatus
from deerflow.runtime.user_context import reset_current_user, set_current_user


@pytest_asyncio.fixture(autouse=True)
async def actual_user():
    token = set_current_user(SimpleNamespace(id=RAW_OWNER))
    try:
        yield
    finally:
        reset_current_user(token)


def _journal(environment, owner, *, capture=False, **kwargs):
    observer = OutputCaptureObserver(owner_user_id=RAW_OWNER, thread_id="journal-thread", run_id="journal-run") if capture else None
    with capture_io_scope(owner):
        journal = RunJournal("journal-run", "journal-thread", environment.event_store, output_capture=observer, **kwargs)
    return journal, observer


async def _messages(environment):
    return await environment.event_store.list_events("journal-thread", "journal-run", user_id=RAW_OWNER)


async def _cleanup(owner, journal, *tasks):
    owner.retire()
    await asyncio.gather(*tasks, return_exceptions=True)
    await journal.close(flush=False)
    await owner.drain()


async def _pending(task):
    done, _ = await asyncio.wait({task}, timeout=0.03)
    return not done


def _usage(journal, identity):
    journal.record_external_llm_usage_records([{"source_run_id": identity, "caller": "lead_agent", "model_name": "offline", "input_tokens": 2, "output_tokens": 3, "total_tokens": 5}])


@pytest.mark.asyncio
@pytest.mark.parametrize("capture", [False, True])
async def test_retirement_revokes_queued_journal_write_before_actual_store_call(capture_environment, monkeypatch, capture):
    e, owner, calls = capture_environment, CaptureIOOwner(), []
    original = e.event_store.put_batch

    async def recorded(batch):
        calls.append(batch)
        return await original(batch)

    monkeypatch.setattr(e.event_store, "put_batch", recorded)
    journal, observer = _journal(e, owner, capture=capture, flush_threshold=1)
    journal.record_delivery()  # The scheduled coroutine has not run yet.
    owner.retire()
    try:
        await owner.drain()
        await asyncio.gather(*tuple(journal._pending_flush_tasks), return_exceptions=True)
        rows = await _messages(e)
        assert calls == [] and rows == [], "Queued journal work reached SQL after retirement"
        assert observer is None or not observer.uncertain, "Retirement was misclassified as source write uncertainty"
    finally:
        await _cleanup(owner, journal)


@pytest.mark.asyncio
@pytest.mark.parametrize("capture", [False, True])
async def test_owner_drain_retains_actual_started_journal_insert_through_repeated_cancellation(capture_environment, capture):
    e, owner = capture_environment, CaptureIOOwner()
    entered, release, native_cancelled = asyncio.Event(), asyncio.Event(), asyncio.Event()
    journal, _ = _journal(e, owner, capture=capture, flush_threshold=1)

    def pause(_connection, _cursor, statement, _parameters, _context, _many):
        if "INSERT INTO run_events" in statement and not entered.is_set():

            async def held():
                entered.set()
                try:
                    await release.wait()
                except asyncio.CancelledError:
                    native_cancelled.set()
                    raise

            await_only(held())

    event.listen(e.engine.sync_engine, "after_cursor_execute", pause)
    drain = None
    try:
        journal.record_delivery()
        await asyncio.wait_for(entered.wait(), 5)
        owner.retire()
        drain = asyncio.create_task(owner.drain())
        retained_before_cancel = await _pending(drain)
        drain.cancel()
        retained_after_first = await _pending(drain)
        drain.cancel()
        retained_after_second = await _pending(drain)
        release.set()
        await asyncio.gather(drain, return_exceptions=True)
        await asyncio.gather(*tuple(journal._pending_flush_tasks), return_exceptions=True)
        rows = await _messages(e)
        # This is a real independent transaction after the admitted write settles,
        # not a claim that an after-execute event exposes a live native cursor.
        saved = await e.event_store.put(thread_id="writer-control", run_id="writer", event_type="control", category="trace", content="committed")
        controls = await e.event_store.list_events("writer-control", "writer", user_id=RAW_OWNER)
        assert saved["seq"] == controls[0]["seq"] and controls[0]["content"] == "committed"
        assert len(rows) == 1 and rows[0]["event_type"] == "run.delivery"
        assert retained_before_cancel and retained_after_first and retained_after_second, "Host drain detached an actual started journal write"
        assert not native_cancelled.is_set()
    finally:
        release.set()
        event.remove(e.engine.sync_engine, "after_cursor_execute", pause)
        await _cleanup(owner, journal, *([drain] if drain is not None else []))


@pytest.mark.asyncio
@pytest.mark.parametrize("ambient", ["none", "foreign"])
async def test_later_flush_uses_journals_original_owner_not_current_ambient_scope(capture_environment, ambient):
    e, owner, foreign = capture_environment, CaptureIOOwner(), CaptureIOOwner()
    journal, observer = _journal(e, owner, capture=True, flush_threshold=50)
    journal.record_delivery()
    owner.retire()
    failure = None
    try:
        with capture_io_scope(foreign if ambient == "foreign" else None):
            try:
                await journal.flush()
            except CaptureIOClosed as error:
                failure = error
        rows = await _messages(e)
        assert rows == [], "A later ambient context revived an originally retired journal"
        assert not observer.uncertain
        assert failure is None or isinstance(failure, CaptureIOClosed)
    finally:
        await _cleanup(owner, journal)
        foreign.retire()
        await foreign.drain()


@pytest.mark.asyncio
async def test_each_buffered_batch_requires_fresh_admission(capture_environment, monkeypatch):
    e, owner, calls = capture_environment, CaptureIOOwner(), []
    journal, observer = _journal(e, owner, capture=True, flush_threshold=20)
    for _ in range(3):
        journal.record_delivery()
    journal._flush_threshold = 1
    original = e.event_store.put_batch

    async def retire_after_commit(batch):
        rows = await original(batch)
        calls.append(rows)
        if len(calls) == 1:
            owner.retire()
        return rows

    monkeypatch.setattr(e.event_store, "put_batch", retire_after_commit)
    try:
        try:
            await journal.flush()
        except CaptureIOClosed:
            pass
        rows = await _messages(e)
        assert len(calls) == len(rows) == 1, "A whole flush admission allowed later batches after retirement"
        assert not observer.uncertain, "A refused later batch was labeled an unknown committed write"
    finally:
        await _cleanup(owner, journal)


@pytest.mark.asyncio
async def test_queued_progress_is_revoked_without_invoking_reporter(capture_environment):
    e, owner, calls = capture_environment, CaptureIOOwner(), []

    async def reporter(snapshot):
        calls.append(snapshot)

    journal, _ = _journal(e, owner, progress_reporter=reporter, progress_flush_interval=0)
    _usage(journal, "queued")
    owner.retire()
    try:
        await owner.drain()
        if journal._pending_progress_task is not None:
            await asyncio.gather(journal._pending_progress_task, return_exceptions=True)
        assert calls == [], "Queued progress performed a fresh reporter call after retirement"
    finally:
        await _cleanup(owner, journal)


@pytest.mark.asyncio
async def test_started_progress_is_retained_and_dirty_followup_is_revoked(capture_environment):
    e, owner, calls = capture_environment, CaptureIOOwner(), []
    entered, release, reporter_cancelled = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def reporter(snapshot):
        calls.append(snapshot)
        entered.set()
        try:
            await release.wait()
        except asyncio.CancelledError:
            reporter_cancelled.set()
            raise

    journal, _ = _journal(e, owner, progress_reporter=reporter, progress_flush_interval=0)
    drain = None
    try:
        _usage(journal, "started")
        await asyncio.wait_for(entered.wait(), 5)
        _usage(journal, "dirty-followup")
        owner.retire()
        drain = asyncio.create_task(owner.drain())
        retained = await _pending(drain)
        drain.cancel()
        first = await _pending(drain)
        drain.cancel()
        second = await _pending(drain)
        release.set()
        await asyncio.gather(drain, return_exceptions=True)
        if journal._pending_progress_task is not None:
            await asyncio.gather(journal._pending_progress_task, return_exceptions=True)
        # Any dirty reschedule must be settled without another reporter call.
        await asyncio.sleep(0)
        assert retained and first and second, "Host drain detached a started progress reporter"
        assert len(calls) == 1 and not reporter_cancelled.is_set()
    finally:
        release.set()
        await _cleanup(owner, journal, *([drain] if drain is not None else []))


@pytest.mark.asyncio
async def test_idle_delayed_progress_timer_is_settled_by_retirement(capture_environment):
    e, owner, calls = capture_environment, CaptureIOOwner(), []

    async def reporter(snapshot):
        calls.append(snapshot)

    journal, _ = _journal(e, owner, progress_reporter=reporter, progress_flush_interval=60)
    journal._last_progress_flush = time.monotonic()
    _usage(journal, "delayed")
    timer = journal._pending_progress_task
    try:
        await asyncio.sleep(0)  # Enter the actual delay, not a reporter operation.
        assert journal._pending_progress_delayed and timer is not None
        owner.retire()
        await owner.drain()
        assert timer.done() and calls == [], "Retirement left the journal progress timer alive"
    finally:
        await _cleanup(owner, journal)


@pytest.mark.asyncio
@pytest.mark.parametrize("producer", ["human", "external_usage", "model_start", "tool_end", "middleware", "skill"])
async def test_retirement_closes_summary_and_event_producers(capture_environment, producer):
    e, owner = capture_environment, CaptureIOOwner()
    journal, _ = _journal(e, owner, flush_threshold=50)
    before = journal.get_completion_data()
    owner.retire()
    try:
        if producer == "human":
            journal.set_first_human_message("late input")
        elif producer == "external_usage":
            _usage(journal, "late")
        elif producer == "model_start":
            journal.on_chat_model_start({}, [[HumanMessage(content="late input")]], run_id=uuid4(), tags=["lead_agent"])
        elif producer == "tool_end":
            from langchain_core.messages import ToolMessage

            journal.on_tool_end(ToolMessage(content="late tool", tool_call_id="late-tool"), run_id=uuid4())
        elif producer == "middleware":
            await asyncio.to_thread(journal.record_middleware, "late", name="Late", hook="after_model", action="record", changes={"late": True})
        else:
            journal.record_skill_usage({"path": "late", "content": "late content"})
            message = AIMessage(id="late-ai", content="late answer")
            journal.on_llm_end(LLMResult(generations=[[ChatGeneration(message=message)]]), run_id=uuid4())
        try:
            await journal.flush()
        except CaptureIOClosed:
            pass
        assert journal.get_completion_data() == before and await _messages(e) == [], "Retired producers mutated journal summaries or durable events"
    finally:
        await _cleanup(owner, journal)


@pytest.mark.asyncio
@pytest.mark.parametrize("capture", [False, True])
async def test_open_quiescence_preserves_final_buffer_summary_and_exact_durable_output(capture_environment, capture):
    e, owner = capture_environment, CaptureIOOwner()
    journal, observer = _journal(e, owner, capture=capture, flush_threshold=50)
    root, node, model = uuid4(), uuid4(), uuid4()
    human = HumanMessage(id="genuine-human", content="original input")
    answer = AIMessage(id="genuine-ai", content="  exact final answer\n")
    if observer is not None:
        observer.begin_epoch(root, {"messages": [human]}, pre_existing_ids=set())
    journal.on_chain_start({}, {"messages": [human]}, run_id=root)
    journal.on_chain_start({}, {"messages": [human]}, run_id=node, parent_run_id=root, name="model", metadata={"langgraph_node": "model"})
    journal.on_chat_model_start({}, [[human]], run_id=model, parent_run_id=node, tags=["lead_agent"], metadata={"langgraph_node": "model"})
    journal.on_llm_end(LLMResult(generations=[[ChatGeneration(message=answer)]]), run_id=model, parent_run_id=node, tags=["lead_agent"])
    journal.on_chain_end({"messages": [human, answer]}, run_id=root)
    try:
        if capture:
            await journal.quiesce_output_capture(flush=True)
        else:
            await journal.close(flush=True)
        rows = await _messages(e)
        answers = [row for row in rows if row["event_type"] == "llm.ai.response"]
        assert len(answers) == 1 and answers[0]["content"]["id"] == "genuine-ai"
        assert answers[0]["content"]["content"] == answer.content
        if capture:
            receipt = observer.receipt(status=RunStatus.success, cancelled=False)
            assert receipt.event_seq == answers[0]["seq"] and receipt.text_sha256 == canonical_text_digest(answer.content)
            completion = journal.get_completion_data()
            assert completion["first_human_message"] == "original input" and completion["last_ai_message"] == "exact final answer"
        assert journal._store is None and journal._progress_reporter is None
    finally:
        await _cleanup(owner, journal)


@pytest.mark.asyncio
async def test_ordinary_journal_remains_independent_of_an_unrelated_retired_owner(capture_environment):
    e, unrelated, calls = capture_environment, CaptureIOOwner(), []

    async def reporter(snapshot):
        calls.append(snapshot)

    journal = RunJournal("journal-run", "journal-thread", e.event_store, progress_reporter=reporter, progress_flush_interval=0)
    unrelated.retire()
    try:
        journal.set_first_human_message("ordinary input")
        _usage(journal, "ordinary")
        journal.record_delivery()
        await journal.flush()
        assert len(await _messages(e)) == 1 and len(calls) == 1
        assert journal.get_completion_data()["first_human_message"] == "ordinary input"
        assert journal.get_completion_data()["total_tokens"] == 5
    finally:
        await journal.close(flush=False)
        await unrelated.drain()


@pytest.mark.asyncio
async def test_open_owned_journal_close_preserves_explicit_retry_after_definite_write_failure(capture_environment, monkeypatch):
    e, owner, calls = capture_environment, CaptureIOOwner(), []
    journal, _ = _journal(e, owner, flush_threshold=50)
    original = e.event_store.put_batch

    async def fail_once(batch):
        calls.append(batch)
        if len(calls) == 1:
            raise RuntimeError("controlled definite failure")
        return await original(batch)

    monkeypatch.setattr(e.event_store, "put_batch", fail_once)
    try:
        journal.record_delivery()
        with pytest.raises(RuntimeError, match="controlled definite failure"):
            await journal.close(flush=True)
        retained = journal._store is e.event_store and bool(journal._buffer) and not journal._closed
        await journal.close(flush=True)
        rows = await _messages(e)
        assert retained and len(calls) == 2 and len(rows) == 1, "A failed normal close could not retry its original buffered tail"
    finally:
        owner.retire()
        # A failed close must still be observed and all finite work settled;
        # this baseline may report its completed cleanup failure honestly.
        await asyncio.gather(journal.close(flush=False), owner.drain(), return_exceptions=True)


@pytest.mark.asyncio
async def test_retired_nested_journal_preserves_final_summary_for_worker_completion(capture_environment):
    e, owner = capture_environment, CaptureIOOwner()
    journal, _ = _journal(e, owner, flush_threshold=50)
    journal.set_first_human_message("accepted input")
    message = AIMessage(id="accepted-ai", content="accepted answer")
    journal.on_llm_end(LLMResult(generations=[[ChatGeneration(message=message)]]), run_id=uuid4())
    # Finish the no-usage callback's existing summary coalescing before taking
    # the expected view; its message events remain buffered and must be dropped.
    journal._commit_pending_llm_response()
    before = journal.get_completion_data()
    try:
        owner.retire()
        await owner.drain()
        assert journal.get_completion_data() == before, "Nested journal detachment erased accepted summary data"
        assert await _messages(e) == []
        assert journal._store is None
    finally:
        await _cleanup(owner, journal)


@pytest.mark.asyncio
async def test_unknown_capture_commit_never_replays_the_written_batch(capture_environment, monkeypatch):
    e, owner, calls = capture_environment, CaptureIOOwner(), []
    journal, observer = _journal(e, owner, capture=True, flush_threshold=50)
    original = e.event_store.put_batch

    async def lose_committed_reply(batch):
        calls.append(batch)
        await original(batch)
        raise RuntimeError("controlled lost committed response")

    monkeypatch.setattr(e.event_store, "put_batch", lose_committed_reply)
    try:
        journal.record_delivery()
        with pytest.raises(RuntimeError, match="controlled lost committed response"):
            await journal.flush()
        with pytest.raises(OutputEvidenceError):
            await journal.flush()
        assert len(calls) == 1 and len(await _messages(e)) == 1
        assert observer.uncertain
    finally:
        await _cleanup(owner, journal)


@pytest.mark.asyncio
@pytest.mark.parametrize("capture", [False, True])
async def test_explicit_close_and_host_drain_share_one_native_write_owner(capture_environment, capture):
    e, owner = capture_environment, CaptureIOOwner()
    journal, observer = _journal(e, owner, capture=capture, flush_threshold=50)
    entered, release, native_cancelled = asyncio.Event(), asyncio.Event(), asyncio.Event()

    def pause(_connection, _cursor, statement, _parameters, _context, _many):
        if "INSERT INTO run_events" in statement and not entered.is_set():

            async def held():
                entered.set()
                try:
                    await release.wait()
                except asyncio.CancelledError:
                    native_cancelled.set()
                    raise

            await_only(held())

    event.listen(e.engine.sync_engine, "after_cursor_execute", pause)
    close = drain = None
    try:
        journal.record_delivery()
        journal.record_delivery()
        journal._flush_threshold = 1
        close = asyncio.create_task(journal.close(flush=True))
        await asyncio.wait_for(entered.wait(), 5)
        close.cancel()
        first = await _pending(close)
        close.cancel()
        second = await _pending(close)
        owner.retire()
        drain = asyncio.create_task(owner.drain())
        retained = await _pending(drain)
        release.set()
        result = await asyncio.gather(close, return_exceptions=True)
        await asyncio.wait_for(drain, 5)
        assert first and second and retained and isinstance(result[0], asyncio.CancelledError)
        rows = await _messages(e)
        assert len(rows) == 1 and not native_cancelled.is_set()
        assert journal._store is None and (observer is None or not observer.uncertain)
        assert e.engine.sync_engine.pool.checkedout() == 0
    finally:
        release.set()
        event.remove(e.engine.sync_engine, "after_cursor_execute", pause)
        await _cleanup(owner, journal, *[task for task in (close, drain) if task is not None])


@pytest.mark.asyncio
async def test_fenced_cleanup_after_capture_write_failure_does_not_repeat_the_failed_flush(capture_environment, monkeypatch):
    e, owner, calls = capture_environment, CaptureIOOwner(), []
    journal, observer = _journal(e, owner, capture=True, flush_threshold=50)

    async def fail(batch):
        calls.append(batch)
        raise RuntimeError("controlled capture failure")

    monkeypatch.setattr(e.event_store, "put_batch", fail)
    try:
        journal.record_delivery()
        with pytest.raises(RuntimeError, match="controlled capture failure"):
            await journal.close(flush=True)
        await journal.close(flush=False)
        owner.retire()
        await owner.drain()
        assert len(calls) == 1 and await _messages(e) == [] and observer.uncertain
        assert journal._store is None
    finally:
        owner.retire()
        await asyncio.gather(journal.close(flush=False), owner.drain(), return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("capture", [False, True])
@pytest.mark.parametrize("fail_after_ticket", [False, True])
async def test_public_force_flush_settles_bookkeeping_before_host_resource_detaches(capture_environment, monkeypatch, capture, fail_after_ticket):
    e, owner = capture_environment, CaptureIOOwner()
    journal, observer = _journal(e, owner, capture=capture, flush_threshold=50)
    entered, release = asyncio.Event(), asyncio.Event()
    original = journal._put_batch

    async def after_actual_ticket(store, batch):
        rows = await original(store, batch)  # Real SQL commit; its finite ticket has finished.
        entered.set()
        await release.wait()  # Hold only journal handoff/bookkeeping, not a native cursor.
        if fail_after_ticket:
            raise RuntimeError("controlled post-ticket response failure")
        return rows

    monkeypatch.setattr(journal, "_put_batch", after_actual_ticket)
    flushing = draining = None
    try:
        journal.record_delivery()
        flushing = asyncio.create_task(journal.flush())
        await asyncio.wait_for(entered.wait(), 5)
        assert len(await _messages(e)) == 1  # The backend write really committed.
        owner.retire()
        draining = asyncio.create_task(owner.drain())
        retained = await _pending(draining)
        attached = journal._store is e.event_store
        release.set()
        result = (await asyncio.gather(flushing, return_exceptions=True))[0]
        await asyncio.wait_for(draining, 5)
        if fail_after_ticket:
            assert isinstance(result, RuntimeError)
        else:
            assert result is None
        assert retained and attached, "Host drain detached a journal whose accepted force flush had not settled bookkeeping"
        assert journal._store is None and not journal._buffer
        assert observer is None or observer.uncertain is fail_after_ticket
        assert len(await _messages(e)) == 1
    finally:
        release.set()
        await _cleanup(owner, journal, *[task for task in (flushing, draining) if task is not None])
