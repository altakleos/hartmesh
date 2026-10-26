"""Original-loop service deadlines over actual custody and retained HTTP owner.

Remote response rows are scripted protocol fixtures, not model/output provenance.
Queue gates below delay scheduling without blocking the event loop.
"""

import asyncio
import math

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_publication_authority import attempt_environment as attempt_environment
from test_telegram_publication_coordinator import coordinator_environment as coordinator_environment
from test_telegram_publication_observation import marked, output_projection, serve

from app.telegram_bots import publication
from app.telegram_bots.publication import PublicationCoordinator, PublicationRefused

pytestmark = pytest.mark.asyncio


def recorded_coordinator(e, captured):
    async def capture(*args, **kwargs):
        captured.append(True)
        return await e.host.capture(*args, **kwargs)

    return PublicationCoordinator(inbox=e.inbox, outbox=e.outbox, bus=e.bus, http_owner=e.http, capture_attempt=capture)


@pytest.mark.parametrize("deadline", [True, False, math.inf, -math.inf, math.nan, "tomorrow", object()])
async def test_invalid_external_deadline_refuses_before_authority_or_http(coordinator_environment, deadline):
    e = coordinator_environment
    record = await marked(e)
    await serve(e, record, output=output_projection(record, pending=True))
    captured = []
    coordinator = recorded_coordinator(e, captured)
    before = tuple(e.requests)
    try:
        with pytest.raises(PublicationRefused):
            await coordinator.observe(record, e.link, max_pages=1, deadline=deadline)
        assert not captured and tuple(e.requests) == before
    finally:
        await coordinator.stop()


@pytest.mark.parametrize("offset", [-1.0, 0.0])
async def test_expired_service_deadline_admits_no_new_capture(coordinator_environment, offset):
    e = coordinator_environment
    record = await marked(e)
    await serve(e, record, output=output_projection(record, pending=True))
    captured = []
    coordinator = recorded_coordinator(e, captured)
    before = tuple(e.requests)
    try:
        result = await coordinator.observe(record, e.link, max_pages=1, deadline=asyncio.get_running_loop().time() + offset)
        assert (result.state, result.reason) == ("budget_exhausted", "budget_exhausted")
        assert result.record == record and not captured and tuple(e.requests) == before
    finally:
        await coordinator.stop()


@pytest.mark.parametrize("expire", [False, True])
async def test_queued_child_uses_service_deadline_before_parent_timer_registration(coordinator_environment, monkeypatch, expire):
    e = coordinator_environment
    record = await marked(e)
    await serve(e, record, output=output_projection(record, pending=True))
    captured = []
    coordinator = recorded_coordinator(e, captured)
    before = tuple(e.requests)
    loop = asyncio.get_running_loop()
    deadline = loop.time() + (0.04 if expire else 10)
    queued, waiting, release, elapsed = (asyncio.Event() for _ in range(4))
    actual_create, actual_wait = asyncio.create_task, asyncio.wait

    def queue_child(coro, *, name=None, **kwargs):
        if name != "telegram-publication-operation":
            return actual_create(coro, name=name, **kwargs)

        async def begin():
            queued.set()
            try:
                await release.wait()
                return await coro
            finally:
                coro.close()

        return actual_create(begin(), name=name, **kwargs)

    async def delay_parent(tasks, *, timeout=None, **kwargs):
        if any(task.get_name() == "telegram-publication-operation" for task in tasks):
            waiting.set()
            await release.wait()
            # Give the child an actual chance to execute before this timer can
            # retire it. Its own already-pinned D must prevent the new leaf.
            timeout = 1
        return await actual_wait(tasks, timeout=timeout, **kwargs)

    monkeypatch.setattr(asyncio, "create_task", queue_child)
    monkeypatch.setattr(asyncio, "wait", delay_parent)
    operation = actual_create(coordinator.observe(record, e.link, max_pages=1, deadline=deadline))
    timer = None
    try:
        await asyncio.wait_for(queued.wait(), 5)
        await asyncio.wait_for(waiting.wait(), 5)
        if expire:
            timer = loop.call_at(deadline + 0.01, elapsed.set)
            await asyncio.wait_for(elapsed.wait(), 5)
        release.set()
        result = await asyncio.wait_for(operation, 5)
        if expire:
            assert result.state == "budget_exhausted"
            assert not captured and tuple(e.requests) == before
        else:
            assert result.state == "pending" and result.record.run_id == "coordinator-run"
            assert len(captured) == 1 and len(e.requests) > len(before)
    finally:
        if timer is not None:
            timer.cancel()
        release.set()
        await asyncio.gather(operation, return_exceptions=True)
        await coordinator.stop()


@pytest.mark.parametrize("limit", ["default", "external", "internal"])
async def test_deadline_clamps_original_budget_and_cancels_actual_network_wait(coordinator_environment, monkeypatch, limit):
    e = coordinator_environment
    record = await marked(e)
    entered, cancelled = asyncio.Event(), asyncio.Event()

    async def held(request):
        assert request.url.path.endswith("/runs/page")
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    e.responder = held
    if limit == "internal":
        monkeypatch.setattr(publication, "OBSERVATION_TIMEOUT_SECONDS", 10)
    kwargs = {} if limit == "default" else {"deadline": asyncio.get_running_loop().time() + (10 if limit == "external" else 100)}
    actual_wait = asyncio.wait
    budgets = []

    async def deliver_timer_after_network_entry(tasks, *, timeout=None, **options):
        if any(task.get_name() == "telegram-publication-operation" for task in tasks):
            budgets.append(timeout)
            await asyncio.wait_for(entered.wait(), 5)
            # Controlled timer delivery after actual request admission. The
            # passed original-loop timeout still proves which D was clamped;
            # authority/SQL latency need not fit an artificial40ms fixture.
            return await actual_wait(tasks, timeout=0, **options)
        return await actual_wait(tasks, timeout=timeout, **options)

    monkeypatch.setattr(asyncio, "wait", deliver_timer_after_network_entry)
    operation = asyncio.create_task(e.coordinator.observe(record, e.link, max_pages=1, **kwargs))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        result = await asyncio.wait_for(operation, 5)
        assert result.state == "budget_exhausted" and cancelled.is_set()
        assert len(budgets) == 1 and (28 < budgets[0] <= 30 if limit == "default" else 8 < budgets[0] <= 10)
        assert not any(method == "POST" for method, _, _ in e.requests)
        assert e.http.closed is False
    finally:
        operation.cancel()
        await asyncio.gather(operation, return_exceptions=True)


async def test_actual_deadline_can_expire_during_authority_before_network(coordinator_environment):
    e = coordinator_environment
    record = await marked(e)
    entered, cancelled = asyncio.Event(), asyncio.Event()

    async def held_capture(*args, **kwargs):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    coordinator = PublicationCoordinator(inbox=e.inbox, outbox=e.outbox, bus=e.bus, http_owner=e.http, capture_attempt=held_capture)
    before = tuple(e.requests)
    operation = asyncio.create_task(coordinator.observe(record, e.link, max_pages=1, deadline=asyncio.get_running_loop().time() + 0.04))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        result = await asyncio.wait_for(operation, 5)
        assert result.state == "budget_exhausted" and cancelled.is_set()
        assert tuple(e.requests) == before
    finally:
        operation.cancel()
        await asyncio.gather(operation, return_exceptions=True)
        await coordinator.stop()
