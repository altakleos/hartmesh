"""A turn holds its chat only while its driver can still finish it, and every way out ends it."""

import asyncio

import httpx
import pytest
from _telegram_repository_test_support import db as db
from _telegram_turn_stack import bound_again, mark, only_turn, output_projection, past_the_limit, planned_keys, press_try_again, rebind, restart, send, stack, start_worker, stopped_output, until
from sqlalchemy import select
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging

from app.telegram_bots.turns import RUN_SHOWS_SECONDS
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow

pytestmark = pytest.mark.asyncio


def run_reads_fail(request):
    if request.method == "GET" and "/runs" in request.url.path:
        return httpx.Response(503, json={"detail": "scripted"})


async def test_a_failed_preparation_ends_at_once_and_the_next_message_runs(db, monkeypatch, tmp_path):
    s = await stack(db, monkeypatch, tmp_path)
    failing = [True]

    def fault(request):
        if failing[0] and request.method == "POST" and request.url.path.endswith("/threads"):
            return httpx.Response(500, json={"detail": "scripted"})

    s.gateway.fault = fault
    try:
        assert await send(s, 9101, "First") is True
        await until(lambda: not s.turns._turns)
        assert await planned_keys(s) == ["error.generic"]
        assert (await mark(s, 9101)).route_complete
        failing[0] = False
        assert await send(s, 9102, "Next") is True
        turn = only_turn(s)
        await until(lambda: turn.record is not None and turn.record.dispatch_started)
    finally:
        await s.close()


async def test_a_message_that_never_got_its_marker_ends_after_the_limit(db, monkeypatch, tmp_path):
    s = await stack(db, monkeypatch, tmp_path)
    reads = []

    def fault(request):
        if request.method == "GET" and request.url.path.count("/") == 3 and request.url.path.startswith("/api/threads/"):
            reads.append(1)
            if len(reads) == 2:
                return httpx.Response(503, json={"detail": "scripted"})

    s.gateway.fault = fault
    try:
        assert await send(s, 9201, "First") is True
        turn = only_turn(s)
        await until(lambda: turn.record is not None)
        assert not turn.record.dispatch_started
        await past_the_limit(s, turn)
        await until(lambda: not s.turns._turns)
        assert await planned_keys(s) == ["error.generic"]
        assert (await mark(s, 9201)).route_complete
        assert s.gateway.posts() == 0
        s.gateway.fault = None
        await press_try_again(s, turn.record.publication_id)
        again = only_turn(s)
        await until(lambda: again.record is not None and again.record.dispatch_started)
    finally:
        await s.close()


async def test_a_refused_post_ends_after_the_limit_and_try_again_runs_it(db, monkeypatch, tmp_path):
    s = await stack(db, monkeypatch, tmp_path)
    s.gateway.stream_status = 507
    try:
        assert await send(s, 9301, "First") is True
        turn = only_turn(s)
        await until(lambda: s.gateway.posts() == 1)
        assert await send(s, 9302, "Waiting behind it") is True
        s.gateway.stream_status = None
        await past_the_limit(s, turn, seconds=RUN_SHOWS_SECONDS + 1)
        await until(lambda: turn.done.is_set())
        assert await planned_keys(s) == ["busy.followup", "error.generic"]
        assert (await mark(s, 9301)).route_complete

        waiter = only_turn(s)
        assert waiter is not turn, "The message behind it runs now"
        await until(lambda: waiter.record is not None and waiter.record.publication_id in s.gateway.runs)
        s.gateway.finish(waiter.record.publication_id, status="error")
        await until(lambda: waiter.done.is_set())

        await press_try_again(s, turn.record.publication_id)
        again = only_turn(s)
        await until(lambda: again.record is not None and again.record.dispatch_started)
        assert again.record.publication_id != turn.record.publication_id
        assert again.record.scope.thread_id == turn.record.scope.thread_id
    finally:
        await s.close()


async def test_stop_ends_a_turn_with_no_run_within_seconds_and_its_waiters_with_it(db, monkeypatch, tmp_path):
    s = await stack(db, monkeypatch, tmp_path)
    s.gateway.stream_status = 503
    try:
        assert await send(s, 9401, "First") is True
        turn = only_turn(s)
        await until(lambda: s.gateway.posts() == 1)
        await asyncio.sleep(0.2)
        assert await send(s, 9403, "Waiting behind it") is True
        assert await send(s, 9402, "/stop") is True
        await past_the_limit(s, turn, seconds=RUN_SHOWS_SECONDS + 1)
        await until(lambda: turn.done.is_set())
        assert (await planned_keys(s)).count("busy.cancelled") == 2
        assert (await mark(s, 9402)).route_complete, "The /stop itself is closed too"
        assert (await mark(s, 9403)).route_complete and not s.turns._turns
    finally:
        await s.close()


async def test_a_message_waiting_for_a_worker_waits_and_then_runs(db, monkeypatch, tmp_path):
    s = await stack(db, monkeypatch, tmp_path, with_worker=False)
    try:
        assert await send(s, 9411, "All workers are busy") is True
        turn = only_turn(s)
        await until(lambda: turn.record is not None and turn.record.dispatch_started)
        await past_the_limit(s, turn)
        await asyncio.sleep(0.2)
        assert not turn.done.is_set() and await planned_keys(s) == []
        start_worker(s)
        await until(lambda: turn.record.publication_id in s.gateway.runs)
    finally:
        await s.close()


async def test_stop_takes_a_queued_message_out_of_line_and_it_never_runs(db, monkeypatch, tmp_path):
    s = await stack(db, monkeypatch, tmp_path, with_worker=False)
    try:
        assert await send(s, 9421, "All workers are busy") is True
        turn = only_turn(s)
        await until(lambda: turn.record is not None and turn.record.dispatch_started)
        await asyncio.sleep(0.2)
        assert await send(s, 9422, "/stop") is True
        start_worker(s)  # A worker that frees up the moment after Stop is the one the fence is for.
        await until(lambda: s.bus.inbound_queue.empty())
        await asyncio.sleep(0.3)
        await past_the_limit(s, turn, seconds=RUN_SHOWS_SECONDS + 1)
        await until(lambda: turn.done.is_set())
        assert await planned_keys(s) == ["busy.cancelled"]
        assert s.gateway.posts() == 0, "The person was told it did not run, so it must not"
    finally:
        await s.close()


async def test_a_post_already_on_the_wire_is_not_called_lost(db, monkeypatch, tmp_path):
    s = await stack(db, monkeypatch, tmp_path)
    s.gateway.post_gate = asyncio.Event()
    try:
        assert await send(s, 9431, "Slow to start") is True
        turn = only_turn(s)
        await until(lambda: s.gateway.posts() == 1)
        await past_the_limit(s, turn)
        await asyncio.sleep(0.2)
        assert not turn.done.is_set(), "A worker that has the message starts its run, however long that takes"
        result = await s.services.publications.observe(turn.record, turn.link, max_pages=2, closing="error.generic")
        assert result.state == "pending" and await planned_keys(s) == []
        s.gateway.post_gate.set()
        await until(lambda: turn.record.publication_id in s.gateway.runs)
    finally:
        s.gateway.post_gate.set()
        await s.close()


async def test_stop_waits_for_a_worker_that_has_the_message_and_then_cancels_the_run(db, monkeypatch, tmp_path):
    s = await stack(db, monkeypatch, tmp_path)
    s.gateway.post_gate = asyncio.Event()
    try:
        assert await send(s, 9433, "Slow to start") is True
        turn = only_turn(s)
        await until(lambda: s.gateway.posts() == 1)
        assert await send(s, 9434, "/stop") is True
        await past_the_limit(s, turn, seconds=RUN_SHOWS_SECONDS + 1)
        await asyncio.sleep(0.2)
        assert not turn.done.is_set() and await planned_keys(s) == [], "The POST is on the wire, so nothing true can be said yet"
        s.gateway.post_gate.set()
        await until(lambda: any(path.endswith("/cancel") for _method, path in s.gateway.calls))
    finally:
        s.gateway.post_gate.set()
        await s.close()


async def test_stop_gets_its_own_wait_and_not_what_was_left_of_the_last_one(db, monkeypatch, tmp_path):
    s = await stack(db, monkeypatch, tmp_path)
    s.gateway.stream_status = 503
    try:
        assert await send(s, 9435, "First") is True
        turn = only_turn(s)
        await until(lambda: s.gateway.posts() == 1)
        await until(lambda: turn.quiet_since is not None)
        s.e.clock.advance(RUN_SHOWS_SECONDS - 1)
        assert await send(s, 9436, "/stop") is True
        await until(lambda: turn.quiet_since is not None)
        s.e.clock.advance(RUN_SHOWS_SECONDS - 1)
        turn.wake.set()
        await asyncio.sleep(0.2)
        assert not turn.done.is_set()
        s.e.clock.advance(1)
        turn.wake.set()
        await until(lambda: turn.done.is_set())
    finally:
        await s.close()


async def test_a_run_the_watcher_has_not_found_yet_is_looked_for_once_more(db, monkeypatch, tmp_path):
    from app.telegram_bots.progress import RunObserver

    async def never_finds(*_args, **_kwargs):
        await asyncio.Event().wait()

    monkeypatch.setattr(RunObserver, "watch", never_finds)
    s = await stack(db, monkeypatch, tmp_path)
    try:
        assert await send(s, 9437, "First") is True
        turn = only_turn(s)
        await until(lambda: turn.record is not None and turn.record.publication_id in s.gateway.runs)
        await past_the_limit(s, turn, seconds=RUN_SHOWS_SECONDS + 1)
        await asyncio.sleep(0.3)
        assert not turn.done.is_set() and await planned_keys(s) == [], "The run exists, so nothing here is lost"
    finally:
        await s.close()


async def test_a_message_left_in_the_queue_by_a_retired_turn_is_ended_and_never_runs(db, monkeypatch, tmp_path):
    from app.telegram_bots.recovery import PublicationRecoveryService

    s = await stack(db, monkeypatch, tmp_path, with_worker=False)
    recovery = PublicationRecoveryService(s.services.outbox.inbox, s.services.outbox, s.services.publications, s.db.links, lambda: True, owned=lambda record, link: s.turns.owns_publication(record, link))
    try:
        assert await send(s, 9441, "All workers are busy") is True
        turn = only_turn(s)
        await until(lambda: turn.record is not None and turn.record.dispatch_started)
        s.turns._forget(turn)
        assert (await recovery.tick()).planned == 1
        assert await planned_keys(s) == ["chat.restarted"]
        start_worker(s)
        await until(lambda: s.bus.inbound_queue.empty())
        await asyncio.sleep(0.3)
        assert s.gateway.posts() == 0
        await recovery.stop()
    finally:
        await s.close()


async def test_blips_far_apart_in_a_long_run_do_not_add_up(db, monkeypatch, tmp_path):
    s = await stack(db, monkeypatch, tmp_path)
    try:
        assert await send(s, 9451, "Long answer") is True
        turn = only_turn(s)
        await until(lambda: turn.run_id is not None)
        for _ in range(2):
            s.gateway.fault = run_reads_fail
            turn.wake.set()
            await until(lambda: turn.quiet_since is not None)
            s.gateway.fault = None
            turn.wake.set()
            await until(lambda: turn.quiet_since is None)
            s.e.clock.advance(s.services.config.queue.max_wait_seconds + 1)
        await asyncio.sleep(0.2)
        assert not turn.done.is_set() and await planned_keys(s) == []
    finally:
        await s.close()


async def test_reads_that_keep_failing_let_the_chat_go_and_recovery_delivers_the_reply(db, monkeypatch, tmp_path, caplog):
    from app.telegram_bots.recovery import PublicationRecoveryService

    s = await stack(db, monkeypatch, tmp_path)
    recovery = PublicationRecoveryService(s.services.outbox.inbox, s.services.outbox, s.services.publications, s.db.links, lambda: True, owned=lambda record, link: s.turns.owns_publication(record, link))
    try:
        assert await send(s, 9461, "Long answer") is True
        turn = only_turn(s)
        await until(lambda: turn.run_id is not None)
        record = turn.record
        s.gateway.fault = run_reads_fail
        await past_the_limit(s, turn)
        await until(lambda: turn.done.is_set())
        assert not s.turns._turns and await planned_keys(s) == [], "Nothing was proved, so nothing is claimed"
        assert "chat_close_held" in caplog.text
        s.gateway.fault = None
        s.gateway.finish(record.publication_id, output=output_projection(record))
        assert (await recovery.tick()).planned == 1, "The reply it could not read still reaches the person"
        assert (await mark(s, 9461)).route_complete
        await recovery.stop()
    finally:
        await s.close()


async def test_a_finished_run_with_no_plan_still_lets_the_chat_go(db, monkeypatch, tmp_path):
    async def refuse(*_args, **_kwargs):
        raise RuntimeError("scripted")

    s = await stack(db, monkeypatch, tmp_path)
    try:
        assert await send(s, 9471, "First") is True
        turn = only_turn(s)
        await until(lambda: turn.run_id is not None)
        assert await send(s, 9472, "Behind it") is True
        monkeypatch.setattr(s.services.outbox, "plan_publication_outcome", refuse)
        s.gateway.finish(turn.record.publication_id, output=None)
        await until(lambda: turn.quiet_since is not None)
        for _ in range(4):
            s.e.clock.advance(s.services.config.queue.max_wait_seconds + 1)
            turn.wake.set()
            await asyncio.sleep(0.2)
        await until(lambda: turn.done.is_set())
        assert only_turn(s) is not turn, "The message behind it runs now"
    finally:
        await s.close()


@pytest.mark.parametrize("status", ["success", "error"])
async def test_a_final_pause_that_never_settles_lets_the_chat_go(db, monkeypatch, tmp_path, status):
    async def not_yet():
        return False

    s = await stack(db, monkeypatch, tmp_path)
    try:
        assert await send(s, 9481, "First") is True
        turn = only_turn(s)
        await until(lambda: turn.run_id is not None and turn.presentation is not None)
        turn.presentation.prepare_final = not_yet
        s.gateway.finish(turn.record.publication_id, status=status, output=output_projection(turn.record) if status == "success" else stopped_output(turn.record, status))
        turn.wake.set()
        await until(lambda: turn.quiet_since is not None)
        s.e.clock.advance(s.services.config.queue.max_wait_seconds + 1)
        turn.wake.set()
        await until(lambda: turn.done.is_set())
    finally:
        await s.close()


async def test_a_closing_turn_no_longer_owns_its_publication(db, monkeypatch, tmp_path):
    s = await stack(db, monkeypatch, tmp_path)
    s.gateway.stream_status = 503
    observe, owned = s.services.publications.observe, []

    async def noting(record, link, **options):
        owned.append(s.turns.owns_publication(record, link))
        return await observe(record, link, **options)

    s.services.publications.observe = noting
    try:
        assert await send(s, 9491, "First") is True
        turn = only_turn(s)
        await until(lambda: s.gateway.posts() == 1)
        assert s.turns.owns_publication(turn.record, turn.link)
        await past_the_limit(s, turn, seconds=RUN_SHOWS_SECONDS + 1)
        await until(lambda: turn.done.is_set())
        assert owned == [False], "Otherwise the planner holds back the very files it should deliver"
    finally:
        await s.close()


async def test_a_failed_capacity_retry_stays_in_the_wait_and_gives_up_on_time(db, monkeypatch, tmp_path):
    s = await stack(db, monkeypatch, tmp_path)
    try:
        assert await send(s, 9501, "Needs a sandbox") is True
        turn = only_turn(s)
        await until(lambda: s.gateway.runs)
        record = turn.record
        s.gateway.finish(record.publication_id, status="error", stop_reason="sandbox_capacity_exceeded", output=stopped_output(record, "error"))
        await until(lambda: turn.capacity_notice_sent)

        def fault(request):
            if request.method == "GET" and request.url.path.count("/") == 3 and request.url.path.startswith("/api/threads/"):
                return httpx.Response(503, json={"detail": "scripted blip"})

        s.gateway.fault = fault
        s.e.clock.advance(s.services.config.queue.retry_seconds + 1)
        turn.wake.set()
        await asyncio.sleep(0.2)
        assert not turn.task.done(), "A blip in the retry must not end the driver"
        s.gateway.fault = None
        s.e.clock.advance(s.services.config.queue.max_wait_seconds + 1)
        turn.wake.set()
        await until(lambda: turn.done.is_set())
        assert (await planned_keys(s))[-1] == "busy.gave_up"
        assert (await mark(s, 9501)).route_complete
    finally:
        await s.close()


async def test_disconnecting_during_a_turn_lets_the_next_message_run_after_reconnecting(db, monkeypatch, tmp_path):
    s = await stack(db, monkeypatch, tmp_path)
    try:
        assert await send(s, 9601, "Running") is True
        turn = only_turn(s)
        await until(lambda: s.gateway.runs)
        link = turn.link
        fresh = await rebind(s, link)
        await until(lambda: not s.turns._turns)
        await bound_again(s, link, fresh)
        assert await send(s, 9602, "After reconnecting") is True
        successor = only_turn(s)
        await until(lambda: successor.record is not None and successor.record.dispatch_started)
    finally:
        await s.close()


async def test_a_conversation_deleted_on_the_web_just_starts_a_new_one(db, monkeypatch, tmp_path):
    s = await stack(db, monkeypatch, tmp_path)
    try:
        assert await send(s, 9701, "First question") is True
        first = only_turn(s)
        await until(lambda: first.record is not None and first.record.publication_id in s.gateway.runs)
        s.gateway.finish(first.record.publication_id, output=output_projection(first.record))
        await until(lambda: first.done.is_set())
        s.gateway.deleted.add(first.record.scope.thread_id)

        assert await send(s, 9702, "Second question") is True
        second = only_turn(s)
        await until(lambda: second.record is not None and second.record.dispatch_started)
        assert second.record.scope.thread_id != first.record.scope.thread_id
    finally:
        await s.close()


async def test_one_failed_authority_read_does_not_end_a_turns_reads(db, monkeypatch, tmp_path):
    s = await stack(db, monkeypatch, tmp_path)
    try:
        assert await send(s, 9751, "Long answer") is True
        turn = only_turn(s)
        await until(lambda: turn.record is not None and turn.record.publication_id in s.gateway.runs)
        await until(lambda: s.turns.observer._states and next(iter(s.turns.observer._states.values())).attempt is not None)
        state = next(iter(s.turns.observer._states.values()))
        state.attempt.admission.retired = True  # What one failed read leaves behind.
        record = turn.record
        s.gateway.finish(record.publication_id, status="error")
        turn.wake.set()
        await until(lambda: turn.done.is_set())
        assert await planned_keys(s) == ["error.generic"]
    finally:
        await s.close()


@pytest.mark.parametrize(
    "status,reason,expected",
    [
        ("interrupted", "gateway_stopped", "chat.restarted"),
        ("error", "orphan_recovered", "refused.interrupted"),
        ("error", None, "error.generic"),
        ("error", "sandbox_capacity_exceeded", "busy.gave_up"),
    ],
)
async def test_a_run_that_ended_across_a_restart_gets_its_ending_and_try_again(db, monkeypatch, tmp_path, status, reason, expected):
    s = await stack(db, monkeypatch, tmp_path)
    try:
        assert await send(s, 9801, "Long answer") is True
        turn = only_turn(s)
        await until(lambda: s.gateway.runs)
        record = turn.record
        recovery = await restart(s)
        s.gateway.finish(record.publication_id, status=status, stop_reason=reason, output=stopped_output(record, status))
        result = await recovery.tick()
        assert result.planned == 1 and result.held == 0
        assert await planned_keys(s) == [expected]
        assert (await mark(s, 9801)).route_complete
        async with db.sessions() as session:
            control = await session.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "retry_message"))
            planned = await session.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == "reply:" + record.turn_id))
        assert control.target_id == record.publication_id, "It offers Try again"
        assert planned.parts[0]["source_ref"]["values"] == ({"name": "Fixture target"} if expected == "refused.interrupted" else {})
        quiet = await recovery.tick()
        assert (quiet.scanned, quiet.planned) == (0, 0), "An ended message is not read again"
        await recovery.stop()
    finally:
        await s.close()


async def test_a_message_that_lost_its_run_across_a_restart_is_told_so(db, monkeypatch, tmp_path):
    s = await stack(db, monkeypatch, tmp_path)
    s.gateway.stream_status = 503
    try:
        assert await send(s, 9851, "Lost") is True
        await until(lambda: s.gateway.posts() == 1)
        await asyncio.sleep(0.2)
        recovery = await restart(s)
        result = await recovery.tick()
        assert result.planned == 1
        assert await planned_keys(s) == ["chat.restarted"]
        assert (await mark(s, 9851)).route_complete
        await recovery.stop()
    finally:
        await s.close()


async def test_recovery_leaves_alone_a_message_a_live_turn_still_drives(db, monkeypatch, tmp_path):
    from app.telegram_bots.recovery import PublicationRecoveryService

    s = await stack(db, monkeypatch, tmp_path, with_worker=False)
    recovery = PublicationRecoveryService(s.services.outbox.inbox, s.services.outbox, s.services.publications, s.db.links, lambda: True, owned=lambda record, link: s.turns.owns_publication(record, link))
    try:
        assert await send(s, 9901, "Queued") is True
        turn = only_turn(s)
        await until(lambda: turn.record is not None and turn.record.dispatch_started)
        s.e.channel.link = await s.db.links.update_if_current(s.e.link.id, token_revision=s.e.link.token_revision, last_ok_at=s.e.clock.now())
        result = await recovery.tick()
        assert result.planned == 0 and await planned_keys(s) == [], "A poll's stamp must not end its ownership"
        assert not turn.done.is_set()
        await recovery.stop()
    finally:
        await s.close()
