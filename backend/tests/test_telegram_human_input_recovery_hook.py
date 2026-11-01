"""Periodic recovery delegates exact source-qualified questions without model retry."""

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_human_input_plans import parts, source
from test_telegram_publication_authority import attempt_environment as attempt_environment
from test_telegram_publication_coordinator import coordinator_environment as coordinator_environment


@pytest.mark.asyncio
async def test_saved_human_input_run_plans_via_optional_source_reader_without_live_turn(coordinator_environment):
    e = coordinator_environment
    qualified, reader, _basis = await source(e)
    e.bus.get_inbound_nowait()
    e.bus.inbound_task_done()
    original = qualified.record
    assert (await e.coordinator.observe(original, e.link, max_pages=1)).reason == "source_unavailable", "Waiting on a person, the run has no answer to publish"
    calls = []

    async def plan(record, link, *, guard=None):
        calls.append(record.publication_id)
        fresh = await reader.read_request(record, link, guard=guard)
        attempt = await e.host.capture(link, thread_id=record.scope.thread_id, endpoint_id=record.scope.endpoint_id)
        return await e.outbox.plan_human_input_reply(fresh.record, fresh, parts=parts(fresh.record), keyboard=[], callbacks=None, guard=attempt.read_guard)

    e.coordinator.human_input_planner = plan
    result = await e.coordinator.observe(original, e.link, max_pages=1)
    assert result.state == "planned" and result.outbox_id is not None
    replay = await e.coordinator.observe(original, e.link, max_pages=1)
    assert replay.outbox_id == result.outbox_id
    assert calls == [original.publication_id, original.publication_id]
    item = await e.outbox.get(e.link, result.outbox_id)
    assert item.payload["reply_basis"]["version"] == 5 and item.payload["reply_basis"]["finish_route"] is True
    assert e.bus.inbound_queue.empty()


@pytest.mark.asyncio
async def test_expired_original_observation_cannot_commit_a_late_question_plan(coordinator_environment, monkeypatch):
    import asyncio
    import importlib

    from sqlalchemy import select

    from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow, TelegramOutboxRow

    e = coordinator_environment
    qualified, _reader, _basis = await source(e)
    e.bus.get_inbound_nowait()
    e.bus.inbound_task_done()
    module = importlib.import_module("app.telegram_bots.publication")
    actual_control = module._Control
    clock = [asyncio.get_running_loop().time()]
    controls, guards = [], []
    entered, release = asyncio.Event(), asyncio.Event()

    def original_clock(**kwargs):
        control = actual_control(**kwargs)
        control.clock = lambda: clock[0]
        controls.append(control)
        return control

    monkeypatch.setattr(module, "_Control", original_clock)

    async def plan(record, link, *, guard=None):
        guards.append(guard)
        entered.set()
        await release.wait()
        # The old callback captured an independent source attempt; it could
        # commit with a still-live guard after its caller's deadline elapsed.
        attempt = await e.host.capture(link, thread_id=record.scope.thread_id, endpoint_id=record.scope.endpoint_id)
        return await e.outbox.plan_human_input_reply(record, qualified, parts=parts(record), keyboard=[], callbacks=None, guard=guard if guard is not None else attempt.read_guard)

    e.coordinator.human_input_planner = plan
    task = asyncio.create_task(e.coordinator.observe(qualified.record, e.link, max_pages=1))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        assert controls[0].deadline is not None
        clock[0] = controls[0].deadline + 1
        release.set()
        result = await asyncio.wait_for(task, 5)
        assert result.state == "budget_exhausted"
        async with e.db.sessions() as reader:
            primary = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == "reply:" + qualified.record.turn_id))
            rows = list(await reader.scalars(select(TelegramInboundMarkRow).where(TelegramInboundMarkRow.turn_id == qualified.record.turn_id)))
            assert primary is None, "An expired observation must fence the actual writer, not just its returned result"
            assert rows and all(not row.route_complete and row.outbox_id is None for row in rows)
        assert guards[0] is not None
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_real_human_input_service_keeps_original_recovery_fence_after_source_await(coordinator_environment, monkeypatch):
    import asyncio
    import importlib
    from types import SimpleNamespace

    from sqlalchemy import select

    from app.telegram_bots.callbacks import CallbackRepository
    from app.telegram_bots.human_input import HumanInputs
    from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow, TelegramOutboxRow

    e = coordinator_environment
    qualified, source_reader, _basis = await source(e)
    e.bus.get_inbound_nowait()
    e.bus.inbound_task_done()
    e.channel._stopping = False
    e.channel._same_scope = lambda left, right: left == right
    services = SimpleNamespace(
        publications=e.coordinator,
        thread_client=e.http,
        publication_authority=e.host,
        channel=lambda identity: e.current[0] if identity == e.link.id else None,
        settings=e.db.settings,
        callbacks=CallbackRepository(e.db.sessions, config=e.db.config),
        outbox=e.outbox,
        links=e.db.links,
        prepare_live_owner=e.live.authority.prepare_live_owner,
        publication_read_allowed=lambda: e.reading[0],
    )
    inputs = HumanInputs(services, source=source_reader)
    module = importlib.import_module("app.telegram_bots.publication")
    actual_control = module._Control
    controls, guards, profile_reads = [], [], []
    clock = [asyncio.get_running_loop().time()]
    entered, release = asyncio.Event(), asyncio.Event()
    actual_source = source_reader.read_request
    actual_settings = e.db.settings.get

    def original_clock(**kwargs):
        control = actual_control(**kwargs)
        control.clock = lambda: clock[0]
        controls.append(control)
        return control

    async def held_source(*args, **kwargs):
        result = await actual_source(*args, **kwargs)
        guards.append(kwargs.get("guard"))
        entered.set()
        await release.wait()
        return result

    async def counted_settings(*args, **kwargs):
        profile_reads.append(True)
        return await actual_settings(*args, **kwargs)

    monkeypatch.setattr(module, "_Control", original_clock)
    monkeypatch.setattr(source_reader, "read_request", held_source)
    monkeypatch.setattr(e.db.settings, "get", counted_settings)
    e.coordinator.human_input_planner = inputs.plan_terminal
    task = asyncio.create_task(e.coordinator.observe(qualified.record, e.link, max_pages=1))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        clock[0] = controls[0].deadline + 1
        release.set()
        result = await asyncio.wait_for(task, 5)
        assert result.state == "budget_exhausted"
        assert guards[0] is not None and not profile_reads
        async with e.db.sessions() as reader:
            primary = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == "reply:" + qualified.record.turn_id))
            rows = list(await reader.scalars(select(TelegramInboundMarkRow).where(TelegramInboundMarkRow.turn_id == qualified.record.turn_id)))
            assert primary is None and rows and all(not row.route_complete and row.outbox_id is None for row in rows)
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
