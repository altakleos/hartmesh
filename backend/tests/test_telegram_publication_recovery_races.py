"""Actual SQL/bus dispatch-versus-close races, with synthetic startup phase.

The old coordinator has an actual reservation and retained authority. The new
opaque epoch uses Root's controlled host helper. This is neither model-output
provenance nor a concurrent native database-lock qualification.
"""

import asyncio
from dataclasses import asdict

import pytest
from _telegram_recovery_test_support import issue_restart_epoch
from _telegram_repository_test_support import db as db
from test_telegram_publication_authority import attempt_environment as attempt_environment
from test_telegram_publication_coordinator import coordinator_environment as coordinator_environment
from test_telegram_publication_coordinator import saved_marks

from app.channels.message_bus import InboundMessage, InboundQueueFullError
from app.telegram_bots.publication import PublicationRefused

pytestmark = pytest.mark.asyncio


def free_capacity(bus, expected):
    reservations = []
    message = InboundMessage("capacity-control", "chat", "human", "offline capacity check")
    try:
        for _ in range(expected):
            reservations.append(bus.reserve_inbound(message))
        with pytest.raises(InboundQueueFullError):
            bus.reserve_inbound(message)
    finally:
        for reservation in reservations:
            reservation.release()


@pytest.mark.parametrize("winner", ["closure", "marker"])
async def test_actual_reserved_old_dispatch_and_startup_closure_have_one_atomic_winner(coordinator_environment, monkeypatch, winner):
    e = coordinator_environment
    e.endpoint_id = e.http.endpoint_id
    entered, release = asyncio.Event(), asyncio.Event()
    captured = []
    actual_mark = e.inbox.mark_dispatch_started

    async def paused(record, *, guard):
        captured.append(record)
        if winner == "marker":
            result = await actual_mark(record, guard=guard)
            assert result is True
        entered.set()
        await release.wait()
        return result if winner == "marker" else await actual_mark(record, guard=guard)

    monkeypatch.setattr(e.inbox, "mark_dispatch_started", paused)
    dispatch = asyncio.create_task(e.coordinator.dispatch(e.link, e.origins, thread_id=e.thread_id))
    host = None
    try:
        await asyncio.wait_for(entered.wait(), 5)
        record = captured[0]
        assert record.dispatch_started is False and record.run_id is None
        assert e.bus.inbound_queue.empty()
        free_capacity(e.bus, e.bus.inbound_queue_maxsize - 1)
        host = issue_restart_epoch(e, monkeypatch, (record,))
        attempt = await host.epoch.capture(record, e.link)
        if winner == "closure":
            identity = await e.outbox.plan_restart_before_dispatch(record, ticket=attempt.ticket, guard=attempt.fixed_guard)
        else:
            with pytest.raises(ValueError):
                await e.outbox.plan_restart_before_dispatch(record, ticket=attempt.ticket, guard=attempt.fixed_guard)
            identity = None
        release.set()
        if winner == "closure":
            with pytest.raises(PublicationRefused):
                await dispatch
            assert e.bus.inbound_queue.empty()
            free_capacity(e.bus, e.bus.inbound_queue_maxsize)
        else:
            result = await dispatch
            assert result.publication_id == record.publication_id and result.dispatch_started
            assert e.bus.inbound_queue.qsize() == 1
            free_capacity(e.bus, e.bus.inbound_queue_maxsize - 1)
            message = e.bus.get_inbound_nowait()
            e.bus.inbound_task_done()
            assert message.execution_context.publication_id == record.publication_id
            assert message.execution_context.scope.thread_id == record.scope.thread_id
            free_capacity(e.bus, e.bus.inbound_queue_maxsize)
        saved = await saved_marks(e)
        for row in saved:
            execution = row.route_basis["publications"][-1]["publication_execution"]
            assert execution["scope"] == asdict(record.scope)
            assert execution["input_sha256"] == record.input_sha256
            assert row.thread_id == record.scope.thread_id and row.run_id is None and row.assistant_message_id is None
            assert row.route_basis["current_publication_id"] == record.publication_id
            if winner == "closure":
                assert row.route_complete and row.outbox_id == identity
                assert row.route_basis["publication_recovery"]["publication_id"] == record.publication_id
                assert execution["dispatch_started"] is False
            else:
                assert not row.route_complete and row.outbox_id is None
                assert "publication_recovery" not in row.route_basis and execution["dispatch_started"] is True
        assert not any(method == "POST" for method, _, _ in e.requests)
    finally:
        release.set()
        await asyncio.gather(dispatch, return_exceptions=True)
        if host is not None:
            await host.aclose()
