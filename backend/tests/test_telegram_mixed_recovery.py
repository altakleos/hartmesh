"""Real coordinator/service mixed recovery with explicit remote protocol seeds."""

from types import SimpleNamespace

import pytest
from _telegram_repository_test_support import db as db  # noqa: F401
from sqlalchemy import select
from test_telegram_mixed_source import mixed_environment
from test_telegram_publication_authority import attempt_environment as attempt_environment  # noqa: F401
from test_telegram_publication_coordinator import coordinator_environment as coordinator_environment  # noqa: F401

from app.telegram_bots.files import FileBytesBudget
from app.telegram_bots.human_input import HumanInputs
from app.telegram_bots.model_source import validate_model_reply_basis
from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow, TelegramOutboxRow


async def recovery_service(e):
    qualified, _guard, callbacks, basis = await mixed_environment(e)
    # Discard the one original model input. Recovery must never enqueue it again.
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
        callbacks=callbacks,
        outbox=e.outbox,
        links=e.db.links,
        prepare_live_owner=e.live.authority.prepare_live_owner,
        publication_read_allowed=lambda: e.reading[0],
        config=e.db.config,
        file_bytes_budget=FileBytesBudget(e.db.config.files.in_memory_max_bytes),
    )
    service = HumanInputs(services)
    e.coordinator.human_input_planner = service.plan_terminal
    return qualified.record, service, basis


@pytest.mark.asyncio
async def test_mixed_recovery_plans_real_service_primary_and_reuses_it_without_model_enqueue(coordinator_environment):
    e = coordinator_environment
    record, _service, basis = await recovery_service(e)
    result = await e.coordinator.observe(record, e.link, max_pages=1)
    async with e.db.sessions() as reader:
        rows = list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == "reply:" + record.turn_id)))
        assert len(rows) == 1, "Actual service must have planned the qualified mixed primary"
        saved_id = rows[0].id
    assert result.state == "planned" and result.outbox_id == saved_id
    replay = await e.coordinator.observe(record, e.link, max_pages=1)
    assert replay.state == "planned" and replay.outbox_id == saved_id
    item = await e.outbox.get(e.link, saved_id)
    proof = validate_model_reply_basis(item)
    assert proof.kind == "human_input_files" and proof.output.model_dump(mode="json") == basis["projection"]
    assert item.payload["reply_basis"]["version"] == 7 and item.payload["reply_basis"]["finish_route"] is True
    assert [part["kind"] for part in item.parts] == ["document", "text"]
    async with e.db.sessions() as reader:
        assert len(list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == "reply:" + record.turn_id)))) == 1
        marks = list(await reader.scalars(select(TelegramInboundMarkRow).where(TelegramInboundMarkRow.turn_id == record.turn_id)))
        assert marks and all(mark.route_complete and mark.outbox_id == saved_id for mark in marks)
    assert e.bus.inbound_queue.empty()


@pytest.mark.asyncio
async def test_mixed_recovery_does_not_accept_a_different_whole_projection_from_actual_planner(coordinator_environment):
    from deerflow.runtime.runs.output_inventory import build_event_inventory_evidence

    e = coordinator_environment
    record, service, basis = await recovery_service(e)
    actual_planner = service.plan_terminal

    async def changed_after_observation(original, link, *, guard):
        # The remote inventory changes after observe's first output snapshot.
        # The actual service qualifies and plans this newer complete source;
        # observe must still compare its original whole projection.
        document = basis["projection"]["inventory"]["document"]
        document["items"][0]["path"] = "/mnt/user-data/outputs/changed.txt"
        basis["projection"]["inventory"]["evidence"] = build_event_inventory_evidence(basis["projection"]["output"]["output_evidence"], status="success", inventory_event_seq=9, document=document).model_dump(mode="json")
        return await actual_planner(original, link, guard=guard)

    e.coordinator.human_input_planner = changed_after_observation
    result = await e.coordinator.observe(record, e.link, max_pages=1)
    assert result.state == "held" and result.outbox_id is None
    async with e.db.sessions() as reader:
        saved = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == "reply:" + record.turn_id))
        assert saved is not None
        item = await e.outbox.get(e.link, saved.id, session=reader)
        assert validate_model_reply_basis(item).output.model_dump(mode="json") == basis["projection"]
    assert e.bus.inbound_queue.empty()


@pytest.mark.asyncio
async def test_mixed_recovery_original_deadline_fences_actual_metadata_and_primary_writer(coordinator_environment, monkeypatch):
    import asyncio
    import importlib

    e = coordinator_environment
    record, _service, _basis = await recovery_service(e)
    module = importlib.import_module("app.telegram_bots.publication")
    actual_control = module._Control
    controls, heads = [], []
    clock = [asyncio.get_running_loop().time()]
    original_responder = e.responder

    def controlled_clock(**kwargs):
        control = actual_control(**kwargs)
        control.clock = lambda: clock[0]
        controls.append(control)
        return control

    async def expire_at_head(incoming):
        response = await original_responder(incoming)
        if incoming.method == "HEAD":
            heads.append(incoming.url.path)
            clock[0] = controls[0].deadline + 1
        return response

    monkeypatch.setattr(module, "_Control", controlled_clock)
    e.responder = expire_at_head
    result = await e.coordinator.observe(record, e.link, max_pages=1)
    assert result.state == "budget_exhausted" and len(heads) == 1
    async with e.db.sessions() as reader:
        assert await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == "reply:" + record.turn_id)) is None
        marks = list(await reader.scalars(select(TelegramInboundMarkRow).where(TelegramInboundMarkRow.turn_id == record.turn_id)))
        assert marks and all(not mark.route_complete and mark.outbox_id is None for mark in marks)
    assert e.bus.inbound_queue.empty()
