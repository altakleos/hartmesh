"""Managed successor CAS over real SQL/bus with scripted acknowledged run rows."""

import httpx
import pytest
from _telegram_repository_test_support import db as db
from test_telegram_publication_authority import attempt_environment as attempt_environment
from test_telegram_publication_coordinator import coordinator_environment as coordinator_environment
from test_telegram_publication_observation import ACK, run_row

from app.channels.execution import ChannelExecutionBusyError
from app.channels.outcomes import capture_creation_refusal

pytestmark = pytest.mark.asyncio


async def responder(e, record, *, reason="sandbox_capacity_exceeded", status="error"):
    row = {**run_row(record, run_id="run-" + record.publication_id), "metadata": {"channel_turn_id": record.publication_id}, "status": status, "stop_reason": reason}

    async def respond(request):
        if "/runs/" in request.url.path:
            return httpx.Response(200, headers=ACK, json=row)
        return httpx.Response(200, json={"thread_id": e.thread_id, "owner_user_id": e.link.owner_user_id, "agent_instance": None, "metadata": {"channel_agent_name": e.link.agent_ref}})

    e.responder = respond
    return row


async def test_both_retries_hand_the_next_attempt_what_this_message_saved(coordinator_environment):
    e = coordinator_environment
    saved = {}
    old = await e.coordinator.dispatch(e.link, e.origins, thread_id=e.thread_id)
    first = e.bus.get_inbound_nowait()
    e.bus.inbound_task_done()
    refused = await e.coordinator.retry_refused(old, e.link, capture_creation_refusal(first.execution_context, ChannelExecutionBusyError()), saved_files=saved)
    assert e.bus.get_inbound_nowait().execution_context.saved_files is saved
    e.bus.inbound_task_done()
    await responder(e, refused)
    await e.coordinator.retry(refused, e.link, saved_files=saved)
    assert e.bus.get_inbound_nowait().execution_context.saved_files is saved
    e.bus.inbound_task_done()


async def test_capacity_successor_same_turn_exact_replay_and_stale_predecessor_refusal(coordinator_environment):
    e = coordinator_environment
    old = await e.coordinator.dispatch(e.link, e.origins, thread_id=e.thread_id)
    e.bus.get_inbound_nowait()
    e.bus.inbound_task_done()
    await responder(e, old)
    new = await e.coordinator.retry(old, e.link)
    assert new.turn_id == old.turn_id and new.event_id == "m701:r1"
    assert new.origins == old.origins and new.input_sha256 == old.input_sha256 and new.scope == old.scope
    assert new.publication_id != old.publication_id and new.dispatch_started
    assert e.bus.inbound_queue.qsize() == 1
    assert await e.coordinator.retry(old, e.link) == new
    assert e.bus.inbound_queue.qsize() == 1
    e.bus.get_inbound_nowait()
    e.bus.inbound_task_done()
    await responder(e, new)
    latest = await e.coordinator.retry(new, e.link)
    assert latest.event_id == "m701:r2"
    with pytest.raises((ValueError, RuntimeError)):
        await e.coordinator.retry(old, e.link)
    assert e.bus.inbound_queue.qsize() == 1


@pytest.mark.parametrize("kind", ["unknown", "running", "stop"])
async def test_retry_requires_closed_capacity_fact_and_no_exact_stop(coordinator_environment, kind):
    e = coordinator_environment
    old = await e.coordinator.dispatch(e.link, e.origins, thread_id=e.thread_id)
    e.bus.get_inbound_nowait()
    e.bus.inbound_task_done()
    await responder(e, old, reason=None if kind == "unknown" else "sandbox_capacity_exceeded", status="running" if kind == "running" else "error")
    if kind == "stop":
        attempt = await e.host.capture(e.link, thread_id=e.thread_id, endpoint_id=e.http.endpoint_id)
        await e.inbox.request_publication_stop(old, stop_id="command:stop", guard=attempt.create_guard)
    with pytest.raises((ValueError, RuntimeError)):
        await e.coordinator.retry(old, e.link)
    assert e.bus.inbound_queue.empty()


async def test_acknowledged_creation_refusal_is_scoped_not_bus_text(coordinator_environment):
    e = coordinator_environment
    old = await e.coordinator.dispatch(e.link, e.origins, thread_id=e.thread_id)
    message = e.bus.get_inbound_nowait()
    e.bus.inbound_task_done()
    refusal = capture_creation_refusal(message.execution_context, ChannelExecutionBusyError())
    new = await e.coordinator.retry_refused(old, e.link, refusal)
    assert new.turn_id == old.turn_id and new.event_id == "m701:r1" and new.dispatch_started
    assert e.bus.inbound_queue.qsize() == 1
    with pytest.raises((ValueError, RuntimeError)):
        await e.coordinator.retry_refused(new, e.link, refusal)


async def test_independent_retry_receivers_reserve_one_successor_and_old_bind_refuses(coordinator_environment):
    import asyncio

    e = coordinator_environment
    old = await e.coordinator.dispatch(e.link, e.origins, thread_id=e.thread_id)
    e.bus.get_inbound_nowait()
    e.bus.inbound_task_done()
    await responder(e, old)
    first, second = await asyncio.gather(e.coordinator.retry(old, e.link), e.coordinator.retry(old, e.link))
    assert first == second and e.bus.inbound_queue.qsize() == 1
    attempt = await e.host.capture(e.link, thread_id=e.thread_id, endpoint_id=e.http.endpoint_id)
    with pytest.raises(ValueError):
        await e.inbox.bind_publication_run(old, run_id="late-old-run", guard=attempt.read_guard)


@pytest.mark.parametrize("refused", [False, True])
async def test_retry_hands_off_exact_successor_before_stop_and_actual_marker(coordinator_environment, monkeypatch, refused):
    e = coordinator_environment
    old = await e.coordinator.dispatch(e.link, e.origins, thread_id=e.thread_id)
    message = e.bus.get_inbound_nowait()
    e.bus.inbound_task_done()
    refusal = capture_creation_refusal(message.execution_context, ChannelExecutionBusyError()) if refused else None
    await responder(e, old)
    current = [old]

    def pin(record):
        current[0] = record

    actual = e.inbox.mark_dispatch_started

    async def mark(record, *, guard):
        assert current[0].publication_id == record.publication_id, "controller must own successor before marker/enqueue"
        assert record.dispatch_started is False and record.run_id is None
        await e.inbox.request_publication_stop(record, stop_id="stop-at-retry-handoff", guard=guard)
        return await actual(record, guard=guard)

    monkeypatch.setattr(e.inbox, "mark_dispatch_started", mark)
    method = e.coordinator.retry_refused if refused else e.coordinator.retry
    args = (old, e.link, refusal) if refused else (old, e.link)
    kwargs = {"on_reserved": pin}
    with pytest.raises(RuntimeError):
        await method(*args, **kwargs)
    assert current[0].publication_id != old.publication_id and current[0].dispatch_started is False
    assert e.bus.inbound_queue.empty()
    attempt = await e.host.capture(e.link, thread_id=e.thread_id, endpoint_id=e.http.endpoint_id)
    fresh, _ = await e.inbox.read_publication_input(current[0], guard=attempt.read_guard)
    assert fresh == current[0] and fresh.dispatch_started is False


async def test_retry_handoff_never_awaits_a_callback_or_enqueues_after_malformed_result(coordinator_environment):
    e = coordinator_environment
    old = await e.coordinator.dispatch(e.link, e.origins, thread_id=e.thread_id)
    e.bus.get_inbound_nowait()
    e.bus.inbound_task_done()
    await responder(e, old)
    entered = []

    async def malformed(_record):
        entered.append("must-not-run")

    with pytest.raises(RuntimeError):
        await e.coordinator.retry(old, e.link, on_reserved=malformed)
    assert not entered and e.bus.inbound_queue.empty()
