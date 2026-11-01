"""Actual SQL/owner recovery plus explicitly controlled orchestration units.

Root's issuer helper uses a real Supervisor and opaque capabilities with a
synthetic startup phase. It is not proof of actual startup/copy ordering.
Remote output DTOs are scripted, not genuine worker/scanner provenance.
"""

import asyncio
from dataclasses import replace
from hashlib import sha256
from types import SimpleNamespace

import httpx
import pytest
import pytest_asyncio
from _telegram_recovery_test_support import issue_restart_epoch
from _telegram_repository_test_support import db as db
from test_telegram_publication_authority import attempt_environment as attempt_environment
from test_telegram_publication_coordinator import coordinator_environment as coordinator_environment
from test_telegram_publication_coordinator import saved_marks
from test_telegram_publication_observation import marked, output_projection, serve
from test_telegram_supervisor import build

from app.telegram_bots.publication import PublicationObservation
from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

pytestmark = pytest.mark.asyncio


async def unmarked(e):
    attempt = await e.host.capture(e.link, thread_id=e.thread_id, endpoint_id=e.http.endpoint_id)
    return await e.inbox.reserve_publication(e.link, e.origins, thread_id=e.thread_id, endpoint_id=e.http.endpoint_id, guard=attempt.create_guard)


@pytest_asyncio.fixture
async def recovery_environment(coordinator_environment):
    from app.telegram_bots.recovery import PublicationRecoveryService

    e = coordinator_environment
    e.runtime_allowed = [True]
    e.endpoint_id = e.http.endpoint_id
    e.service = PublicationRecoveryService(e.inbox, e.outbox, e.coordinator, e.db.links, lambda: e.runtime_allowed[0])
    try:
        yield e
    finally:
        await e.service.stop()


async def test_actual_marked_group_tick_plans_exact_v2_primary_without_new_dispatch(recovery_environment):
    e = recovery_environment
    record = await marked(e)
    await serve(e, record, output=output_projection(record))
    before = e.bus.inbound_queue.qsize()
    result = await e.service.tick()
    assert result.started == result.planned == 1
    marks = await saved_marks(e)
    assert all(mark.route_complete and mark.outbox_id for mark in marks)
    async with e.db.sessions() as session:
        primary = await session.get(TelegramOutboxRow, marks[0].outbox_id)
        assert primary.payload["reply_basis"]["version"] == 2
    again = await e.service.tick()
    assert again.started == 0 and e.bus.inbound_queue.qsize() == before
    assert not any(method == "POST" for method, _, _ in e.requests)


@pytest.mark.parametrize("case", ["pending", "incomplete", "nonempty", "failed", "missing"])
async def test_marked_unknown_or_unhandled_inventory_keeps_original_primary_free(recovery_environment, case):
    e = recovery_environment
    record = await marked(e)
    await serve(e, record, output=None if case == "missing" else output_projection(record, **{case: True}))
    result = await e.service.tick()
    assert result.started == 1 and result.planned == 0
    assert all(not row.route_complete and row.outbox_id is None for row in await saved_marks(e))
    assert not any(method == "POST" for method, _, _ in e.requests)


async def test_periodic_pass_never_closes_actual_current_unmarked_group(recovery_environment):
    e = recovery_environment
    record = await unmarked(e)
    before = tuple(e.requests)
    result = await e.service.tick()
    assert result.started == 0 and result.planned == 0
    assert all(not row.route_complete and row.outbox_id is None for row in await saved_marks(e))
    assert tuple(e.requests) == before and not record.dispatch_started


async def test_periodic_admission_requires_literal_true_before_metadata_read(recovery_environment, monkeypatch):
    e = recovery_environment
    reads = []

    async def forbidden(**kwargs):
        reads.append(kwargs)
        raise AssertionError("A closed service must not start a metadata read")

    monkeypatch.setattr(e.inbox, "recovery_page", forbidden)
    for value in (False, 1, None):
        e.runtime_allowed[0] = value
        result = await e.service.tick()
        assert result.started == 0
    assert not reads


async def test_startup_uses_issued_cohort_under_zero_private_ceilings_without_reselecting(recovery_environment, monkeypatch):
    from unittest.mock import AsyncMock

    e = recovery_environment
    record = await unmarked(e)
    e.runtime_allowed[0] = e.creating[0] = False
    host = issue_restart_epoch(e, monkeypatch, (record,))
    monkeypatch.setattr("app.gateway.authz.resolve_route_permissions", AsyncMock(return_value=[]))
    reads = []

    async def forbidden(**kwargs):
        reads.append(kwargs)
        raise AssertionError("Startup may only consume the host's preselected cohort")

    monkeypatch.setattr(e.inbox, "recovery_page", forbidden)
    before = tuple(e.requests)
    try:
        result = await e.service.recover_startup(host.epoch)
        assert result.started == result.planned == 1
        assert not reads and tuple(e.requests) == before and e.bus.inbound_queue.empty()
        rows = await saved_marks(e)
        assert all(row.route_complete and row.run_id is None and row.assistant_message_id is None for row in rows)
        async with e.db.sessions() as session:
            saved = await session.get(TelegramOutboxRow, rows[0].outbox_id)
            assert saved.payload["reply_basis"]["version"] == 3
        # The service cannot consume the same startup capability twice.
        assert (await e.service.recover_startup(host.epoch)).started == 0
    finally:
        await host.aclose()


async def test_empty_and_retired_real_startup_epochs_cannot_enroll_later_groups(recovery_environment, monkeypatch):
    e = recovery_environment
    record = await unmarked(e)
    host = issue_restart_epoch(e, monkeypatch, ())
    try:
        result = await e.service.recover_startup(host.epoch)
        assert result.started == 0
        assert all(row.outbox_id is None for row in await saved_marks(e))
    finally:
        await host.aclose()
    retired = issue_restart_epoch(e, monkeypatch, (record,))
    retired.epoch.retire()
    try:
        result = await e.service.recover_startup(retired.epoch)
        assert result.planned == 0 and all(row.outbox_id is None for row in await saved_marks(e))
    finally:
        await retired.aclose()


def synthetic_record(record, number, *, link_id=None):
    """Scheduler-only DTO; this is not another stored SQL publication."""
    turn = f"{number:032x}"
    scope = replace(record.scope, link_id=link_id or record.scope.link_id)
    origins = tuple(replace(origin, link_id=scope.link_id) for origin in record.origins)
    return replace(record, scope=scope, origins=origins, turn_id=turn, publication_id=sha256((turn + ":" + record.event_id).encode()).hexdigest()[:32], dispatch_started=True)


def controlled_service(e, *, records, observe, allowed=lambda: True):
    """Real A page DTOs, controlled orchestration; no SQL/source proof claim."""
    from app.telegram_bots.inbox import RecoveryCursor, RecoveryEntry, RecoveryPage
    from app.telegram_bots.recovery import PublicationRecoveryService

    reads = []

    async def page(*, after, limit, max_origins):
        reads.append((after, limit, max_origins))
        selected = records[:limit]
        entries = tuple(RecoveryEntry(RecoveryCursor(record.scope.link_id, record.turn_id), record, (), None) for record in selected)
        return RecoveryPage(entries, entries[-1].key if entries else None, False)

    async def link(identity):
        return replace(e.link, id=identity)

    service = PublicationRecoveryService(SimpleNamespace(recovery_page=page), SimpleNamespace(), SimpleNamespace(observe=observe), SimpleNamespace(get=link), allowed)
    return service, reads


async def test_controlled_units_preserve_four_global_one_link_slots_through_expiry(recovery_environment, monkeypatch):
    from app.telegram_bots import recovery

    e = recovery_environment
    original = await unmarked(e)
    records = [synthetic_record(original, number + 1, link_id=f"{number + 100:032x}") for number in range(6)]
    entered, cancelled, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    calls, deadlines, active = [], [], set()

    async def observe(record, link, *, max_pages, deadline):
        assert record.scope.link_id not in active
        active.add(record.scope.link_id)
        calls.append(record)
        deadlines.append(deadline)
        if len(active) == 4:
            entered.set()
        try:
            await release.wait()
        except asyncio.CancelledError:
            cancelled.set()
            # A controlled already-admitted unit retains its cleanup. Native
            # cursor qualification is separate, not inferred from this fake.
            while not release.is_set():
                try:
                    await release.wait()
                except asyncio.CancelledError:
                    cancelled.set()
        finally:
            active.remove(record.scope.link_id)
        return PublicationObservation("pending", record, reason="awaiting_output")

    service, reads = controlled_service(e, records=records, observe=observe)
    monkeypatch.setattr(recovery, "RECOVERY_TIMEOUT_SECONDS", 0.08)
    operation = asyncio.create_task(service.tick())
    stopping = None
    try:
        await asyncio.wait_for(entered.wait(), 5)
        result = await asyncio.wait_for(operation, 5)
        assert result.budget_exhausted and result.still_owned == 4
        assert len(calls) == 4 and len(set(deadlines)) == 1
        await asyncio.wait_for(cancelled.wait(), 5)
        before_reads = len(reads)
        assert (await service.tick()).started == 0
        assert len(reads) == before_reads
        service.begin_shutdown()
        assert (await service.tick()).started == 0
        stopping = asyncio.create_task(service.stop())
        done, _ = await asyncio.wait({stopping}, timeout=0.04)
        assert not done and len(active) == 4
        stopping.cancel()
        done, _ = await asyncio.wait({stopping}, timeout=0.04)
        assert not done
        stopping.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await stopping
        await service.stop()
        assert not active and len(calls) == 4 and reads
    finally:
        release.set()
        await asyncio.gather(operation, *([stopping] if stopping else []), return_exceptions=True)
        await service.stop()


async def test_controlled_same_link_waits_without_overlapping_or_reclaiming_live_unit(recovery_environment):
    e = recovery_environment
    original = await unmarked(e)
    records = [synthetic_record(original, number + 1) for number in range(3)]
    entered, release = asyncio.Event(), asyncio.Event()
    calls, active = [], []

    async def observe(record, link, *, max_pages, deadline):
        assert not active
        active.append(record)
        calls.append(record)
        try:
            if len(calls) == 1:
                entered.set()
                await release.wait()
        finally:
            active.clear()
        return PublicationObservation("pending", record, reason="awaiting_run")

    service, _ = controlled_service(e, records=records, observe=observe)
    operation = asyncio.create_task(service.tick())
    try:
        await asyncio.wait_for(entered.wait(), 5)
        assert len(calls) == 1
        release.set()
        result = await asyncio.wait_for(operation, 5)
        assert result.started == 3 and len(calls) == 3
    finally:
        release.set()
        await asyncio.gather(operation, return_exceptions=True)
        await service.stop()


async def test_a_failed_pass_counts_as_held_and_leaves_its_cause(recovery_environment, caplog):
    from app.telegram_bots.recovery import PublicationRecoveryService

    e = recovery_environment

    async def page(*, after, limit, max_origins):
        raise KeyError("private text")

    service = PublicationRecoveryService(SimpleNamespace(recovery_page=page), e.outbox, SimpleNamespace(), e.db.links, lambda: True)
    assert (await service.tick()).held == 1
    assert "telegram_bots publication_recovery_failed cause=KeyError:test_telegram_publication_recovery_service.py:" in caplog.text and "private text" not in caplog.text


async def test_controlled_cursor_advances_at_most_two_pages_and_wraps(recovery_environment):
    from app.telegram_bots.inbox import RecoveryCursor, RecoveryEntry, RecoveryPage
    from app.telegram_bots.recovery import PublicationRecoveryService

    e = recovery_environment
    original = await unmarked(e)
    first, second = (synthetic_record(original, number) for number in (1, 2))
    heads = [RecoveryCursor(record.scope.link_id, record.turn_id) for record in (first, second)]
    reads, calls = [], []

    async def page(*, after, limit, max_origins):
        reads.append(after)
        index = 0 if after is None else 1
        # A held malformed head advances too; no source/coordinator call.
        entry = RecoveryEntry(heads[index], None, (), "invalid_group")
        return RecoveryPage((entry,), heads[index], index == 0)

    async def observe(*args, **kwargs):
        calls.append(args)
        raise AssertionError("Held group must not be observed")

    service = PublicationRecoveryService(SimpleNamespace(recovery_page=page), e.outbox, SimpleNamespace(observe=observe), e.db.links, lambda: True)
    try:
        result = await service.tick()
        assert result.scanned == result.held == 2 and reads == [None, heads[0]]
        assert result.next_after is None and not calls
        await service.tick()
        assert reads == [None, heads[0], None, heads[0]]
    finally:
        await service.stop()


async def test_actual_failed_response_cleanup_keeps_supervisor_owner_and_listener_for_retry(recovery_environment, monkeypatch):
    """Real response owner retry; synthetic host wiring, no startup claim."""
    e = recovery_environment
    await marked(e)
    allow_close = [False]

    class FailingBody(httpx.AsyncByteStream):
        def __init__(self):
            self.attempts, self.closed = 0, False

        async def __aiter__(self):
            yield b"{}"

        async def aclose(self):
            self.attempts += 1
            if not allow_close[0]:
                raise RuntimeError("Synthetic response cleanup failed")
            self.closed = True

    body = FailingBody()

    async def respond(request):
        assert request.url.path.endswith("/runs/page")
        return httpx.Response(200, stream=body)

    e.responder = respond
    result = await e.service.tick()
    assert result.started == 1 and result.planned == 0 and body.attempts >= 2
    assert not body.closed and not e.http.closed
    boot = build(e.db, monkeypatch)
    host = boot.supervisor
    host.thread_client, host.publications, host.publication_authority, host.recovery = e.http, e.coordinator, e.host, e.service
    boot.service.bus.subscribe_outbound(host._listener)
    host._subscribed = True
    try:
        await e.service.stop()  # Business refusal is not a cleanup verdict.
        with pytest.raises(RuntimeError, match="telegram-thread-client-cleanup-failed"):
            await host.stop()
        assert host._subscribed and not e.http.closed and not body.closed
        assert host.thread_client is e.http and host.publications is e.coordinator
        allow_close[0] = True
        await host.stop()
        assert body.closed and e.http.closed and host._subscribed is False
    finally:
        allow_close[0] = True
        await host.stop()


async def test_queued_metadata_read_checks_original_pass_deadline_before_backend(recovery_environment, monkeypatch):
    from app.telegram_bots import recovery
    from app.telegram_bots.inbox import RecoveryPage
    from app.telegram_bots.recovery import PublicationRecoveryService

    e = recovery_environment
    queued, waiting, release, elapsed = (asyncio.Event() for _ in range(4))
    calls = []

    async def page(**kwargs):
        calls.append(kwargs)
        return RecoveryPage((), None, False)

    service = PublicationRecoveryService(SimpleNamespace(recovery_page=page), e.outbox, e.coordinator, e.db.links, lambda: True)
    actual_create, actual_wait = asyncio.create_task, asyncio.wait
    monkeypatch.setattr(recovery, "RECOVERY_TIMEOUT_SECONDS", 0.04)

    def delayed_page(coro, *, name=None, **kwargs):
        if name != "telegram-publication-recovery-page":
            return actual_create(coro, name=name, **kwargs)

        async def begin():
            queued.set()
            try:
                await release.wait()
                return await coro
            finally:
                coro.close()

        return actual_create(begin(), name=name, **kwargs)

    async def delayed_timer(tasks, *, timeout=None, **kwargs):
        if any(task.get_name() == "telegram-publication-recovery-page" for task in tasks):
            waiting.set()
            await release.wait()
            timeout = 1
        return await actual_wait(tasks, timeout=timeout, **kwargs)

    monkeypatch.setattr(asyncio, "create_task", delayed_page)
    monkeypatch.setattr(asyncio, "wait", delayed_timer)
    operation = actual_create(service.tick())
    timer = None
    try:
        await asyncio.wait_for(queued.wait(), 5)
        await asyncio.wait_for(waiting.wait(), 5)
        timer = asyncio.get_running_loop().call_later(0.06, elapsed.set)
        await asyncio.wait_for(elapsed.wait(), 5)
        release.set()
        result = await asyncio.wait_for(operation, 5)
        assert result.budget_exhausted and not calls
    finally:
        if timer is not None:
            timer.cancel()
        release.set()
        await asyncio.gather(operation, return_exceptions=True)
        await service.stop()


@pytest.mark.parametrize("change", ["confirm", "token", "retire_epoch", "expire_after_commit", "different_epoch"])
async def test_actual_committed_result_loss_only_confirms_fresh_pair_without_repeating_writer(recovery_environment, monkeypatch, change):
    e = recovery_environment
    record = await unmarked(e)
    e.runtime_allowed[0] = e.creating[0] = False
    deadline = asyncio.get_running_loop().time() + 5 if change == "expire_after_commit" else None
    host = issue_restart_epoch(e, monkeypatch, (record,), deadline=deadline)
    committed, release = asyncio.Event(), asyncio.Event()
    plans, reads = [], []
    actual_plan, actual_read = e.outbox.plan_restart_before_dispatch, e.inbox.read_restart_receipt

    async def lost_result(*args, **kwargs):
        plans.append(args[0])
        if change == "different_epoch":
            competitor = issue_restart_epoch(e, monkeypatch, (record,))
            try:
                other = await competitor.epoch.capture(record, e.link)
                assert other.ticket.startup_id != kwargs["ticket"].startup_id
                identity = await actual_plan(record, ticket=other.ticket, guard=other.fixed_guard)
            finally:
                await competitor.aclose()
            with pytest.raises(ValueError):
                await actual_plan(*args, **kwargs)
        else:
            identity = await actual_plan(*args, **kwargs)
        assert identity
        committed.set()
        if change == "token":
            await e.db.links.store_token(e.link.id, "replacement-wrapped-fixture", actor_id=e.link.owner_user_id)
        elif change == "retire_epoch":
            host.epoch.retire()
        elif change == "expire_after_commit":
            await release.wait()
        # The real SQL planner committed; only its result is lost here. This
        # does not claim a physical driver's ambiguous COMMIT was reproduced.
        raise RuntimeError("Synthetic committed planner result lost")

    async def read(*args, **kwargs):
        reads.append(set(kwargs))
        return await actual_read(*args, **kwargs)

    monkeypatch.setattr(e.outbox, "plan_restart_before_dispatch", lost_result)
    monkeypatch.setattr(e.inbox, "read_restart_receipt", read)
    before = tuple(e.requests)
    operation = asyncio.create_task(e.service.recover_startup(host.epoch))
    try:
        await asyncio.wait_for(committed.wait(), 5)
        result = await asyncio.wait_for(operation, 8)
        assert len(plans) == 1 and result.started == 1
        if change == "confirm":
            assert result.planned == 1 and reads == [{"captured_link", "check_live_owner", "check_current"}]
        elif change == "different_epoch":
            assert result.planned == 0 and reads == [{"captured_link", "check_live_owner", "check_current"}]
        else:
            assert result.planned == 0 and not reads
        if change == "expire_after_commit":
            assert result.budget_exhausted and result.still_owned == 1
        saved = await saved_marks(e)
        assert all(row.route_complete and row.outbox_id and row.run_id is None for row in saved)
        assert len({row.outbox_id for row in saved}) == 1
        assert tuple(e.requests) == before and e.bus.inbound_queue.empty()
        release.set()
        await e.service.stop()
        assert len(plans) == 1 and len(reads) == (1 if change in {"confirm", "different_epoch"} else 0)
    finally:
        release.set()
        await asyncio.gather(operation, return_exceptions=True)
        await e.service.stop()
        await host.aclose()


@pytest.mark.parametrize("retirement", ["epoch", "phase", "queued"])
async def test_fresh_startup_service_retirement_prevents_actual_link_sql(recovery_environment, monkeypatch, retirement):
    from sqlalchemy import event

    from app.telegram_bots.recovery import PublicationRecoveryService

    e = recovery_environment
    record = await unmarked(e)
    host = issue_restart_epoch(e, monkeypatch, (record,))
    service = PublicationRecoveryService(e.inbox, e.outbox, e.coordinator, e.db.links, lambda: False)
    entered, release = asyncio.Event(), asyncio.Event()
    calls, statements = [], []
    observing = [False]
    actual_get, actual_create = e.db.links.get, asyncio.create_task

    async def get(identity):
        calls.append(identity)
        observing[0] = True
        try:
            return await actual_get(identity)
        finally:
            observing[0] = False

    def sql(connection, cursor, statement, parameters, context, executemany):
        if observing[0] and statement.lstrip().lower().startswith("select") and "telegram_bot_links" in statement.lower():
            statements.append(True)

    def queue_unit(coro, *, name=None, **kwargs):
        if name != "telegram-publication-recovery-unit":
            return actual_create(coro, name=name, **kwargs)

        async def begin():
            entered.set()
            try:
                await release.wait()
                return await coro
            finally:
                coro.close()

        return actual_create(begin(), name=name, **kwargs)

    monkeypatch.setattr(e.db.links, "get", get)
    event.listen(e.db.engine.sync_engine, "before_cursor_execute", sql)
    operation = None
    try:
        if retirement == "queued":
            monkeypatch.setattr(asyncio, "create_task", queue_unit)
            operation = actual_create(service.recover_startup(host.epoch))
            await asyncio.wait_for(entered.wait(), 5)
            host.epoch.retire()
            release.set()
        else:
            if retirement == "epoch":
                host.epoch.retire()
            else:
                # Controlled host phase, as in the genuine issuer helper;
                # no ticket/registry field is forged or inspected.
                host.host._recovery_phase = None
            operation = actual_create(service.recover_startup(host.epoch))
        result = await asyncio.wait_for(operation, 5)
        assert result.planned == 0
        assert (len(calls), len(statements)) == (0, 0), "Retired startup cannot admit even a metadata link SELECT"
    finally:
        release.set()
        if operation is not None:
            await asyncio.gather(operation, return_exceptions=True)
        await service.stop()
        await host.aclose()
        event.remove(e.db.engine.sync_engine, "before_cursor_execute", sql)
