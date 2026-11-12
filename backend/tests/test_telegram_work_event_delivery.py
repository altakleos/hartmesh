"""A canonical question intent becomes a source-checked SDK presentation."""

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_bot_adapter import message
from test_telegram_work_event_intents import intents, request, setup
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow


async def queued(e, purpose="information"):
    peer, worker = await setup(e)
    async with e.db.sessions.begin() as writer:
        for row in await writer.scalars(select(TelegramOutboxRow)):
            row.state = "sent"
    work, req = await request(e, peer, purpose)
    await worker.tick()
    row = next(item for item in await intents(e) if item.work_id == work["id"])
    return row.id, req


@pytest.mark.asyncio
@pytest.mark.parametrize("purpose", ["information", "decision"])
async def test_question_intent_materializes_once_and_arms_only_after_complete_sdk_ack(work_delivery, purpose):
    from app.telegram_bots.work_event_delivery import WorkEventDelivery
    from app.telegram_bots.work_request_source import WorkRequestSources

    e = work_delivery
    identity, req = await queued(e, purpose)
    materializer = WorkEventDelivery(e.plans)
    assert await materializer.materialize(e.link, identity)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        parts = row.parts
        controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == parts[-1]["callback_group_id"])))
        assert controls and all(control.payload["presentation_pending"] for control in controls)
    assert await materializer.materialize(e.link, identity)
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, identity)).parts == parts
    e.channel._services.work_request_sources = WorkRequestSources(reader=e.reader, outbox=e.plans.outbox, callbacks=e.plans.callbacks)
    message(e.channel.bot.request, identity=880)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not e.dispatch_errors
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "sent" and row.request_id == req["id"]
        for control in controls:
            assert (await reader.get(TelegramCallbackRow, control.id)).payload["presentation_pending"] is False
    assert any(call.endpoint == "sendMessage" and "Which source?" in call.parameters["text"] for call in e.channel.bot.request.calls)


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", [1, 2])
async def test_materialization_final_writer_rejects_source_changed_during_provider_await(work_delivery, monkeypatch, phase):
    import app.gateway.authz as authz
    import app.telegram_bots.work_event_delivery as module
    from app.telegram_bots.producer import ProductSourceRetired
    from deerflow.persistence.agent_instances.work import AgentWorkEventRow

    e = work_delivery
    identity, _req = await queued(e)
    prepare, resolve = module.prepare_attention, authz.resolve_route_permissions
    active, changed = {}, []

    async def prepared(*args, **kwargs):
        admission = await prepare(*args, **kwargs)

        async def check(**options):
            active["writer"] = options["session"]
            active["phase"] = 2 if isinstance(options["projection"], module._MaterializationProjection) else 1
            try:
                return await admission(**options)
            finally:
                active.clear()

        return check

    async def provider(*args, **kwargs):
        held = await resolve(*args, **kwargs)
        if active.get("phase") == phase and not changed:
            writer = active["writer"]
            row = await writer.get(TelegramOutboxRow, identity)
            event = await writer.get(AgentWorkEventRow, row.event_id)
            event.actor_id = "different-event-actor"
            await writer.flush()
            changed.append(True)
        return held

    monkeypatch.setattr(module, "prepare_attention", prepared)
    monkeypatch.setattr(authz, "resolve_route_permissions", provider)
    with pytest.raises(ProductSourceRetired):
        await module.WorkEventDelivery(e.plans).materialize(e.link, identity)
    assert changed
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert not row.parts and "work_event_source" not in row.payload
        assert not list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.payload["anchor_outbox_id"].as_string() == identity)))


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["event", "receipt", "intent"])
async def test_sdk_final_snapshot_requires_original_event_after_preparation(work_delivery, monkeypatch, change):
    from app.telegram_bots.work_event_delivery import WorkEventDelivery
    from app.telegram_bots.work_request_source import WorkRequestSources
    from deerflow.persistence.agent_instances.human_input import HumanInputEventRow
    from deerflow.persistence.agent_instances.work import AgentWorkEventRow

    e = work_delivery
    identity, _req = await queued(e)
    await WorkEventDelivery(e.plans).materialize(e.link, identity)
    sources = WorkRequestSources(reader=e.reader, outbox=e.plans.outbox, callbacks=e.plans.callbacks)
    e.channel._services.work_request_sources = sources
    original, changed = sources.settings.get, []

    async def altered(*args, **kwargs):
        settings = await original(*args, **kwargs)
        if not changed:
            async with e.db.sessions.begin() as writer:
                row = await writer.get(TelegramOutboxRow, identity)
                event = await writer.get(AgentWorkEventRow, row.event_id)
                if change == "event":
                    await writer.delete(event)
                elif change == "receipt":
                    receipt = await writer.scalar(select(HumanInputEventRow).where(HumanInputEventRow.operation_id == event.operation_id))
                    receipt.actor_id = "different-person"
                else:
                    row.payload = {**row.payload, "work_event_intent": {**row.payload["work_event_intent"], "event_revision": 99}}
            changed.append(True)
        return settings

    monkeypatch.setattr(sources.settings, "get", altered)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert changed and e.dispatch_errors
    assert not any(call.endpoint == "sendMessage" for call in e.channel.bot.request.calls)


@pytest.mark.asyncio
async def test_routed_question_intent_cannot_refresh_into_a_new_notification(work_delivery):
    from uuid import uuid4

    from app.telegram_bots.producer import ProductSourceRetired
    from app.telegram_bots.work_event_delivery import WorkEventDelivery
    from deerflow.agent_instances.human_input_contract import RequestCommand

    e = work_delivery
    identity, req = await queued(e)
    await e.human.command(actor=e.actor, request_id=req["id"], request=RequestCommand(operation_id=uuid4().hex, action="route", expected_revision=1, expected_request_revision=1, recipient_id=e.actor.subject_id))
    with pytest.raises(ProductSourceRetired):
        await WorkEventDelivery(e.plans).materialize(e.link, identity)
    async with e.db.sessions() as reader:
        assert not (await reader.get(TelegramOutboxRow, identity)).parts


@pytest.mark.asyncio
async def test_protected_event_is_concealed_until_actual_show(work_delivery):
    from datetime import UTC, datetime

    from test_telegram_work_preview_recovery import dispatch, install, reveal

    from app.telegram_bots.work_event_delivery import WorkEventDelivery
    from deerflow.persistence.telegram_bots.model import TelegramInstancePolicyRow

    e = work_delivery
    install(e)
    identity, _req = await queued(e)
    e.identity = identity
    async with e.db.sessions.begin() as writer:
        writer.add(TelegramInstancePolicyRow(instance_id=e.link.agent_ref, policy="protected", updated_by=e.actor.subject_id, updated_at=datetime.now(UTC)))
    await WorkEventDelivery(e.plans).materialize(e.link, identity)
    await dispatch(e, 821)
    calls = [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
    assert len(calls) == 1 and calls[0].parameters["text"] == "A question is waiting."
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        show = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"]))
    await reveal(e, show.id)
    e.clock.advance(10)
    message(e.channel.bot.request, endpoint="editMessageText", identity=821)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not e.dispatch_errors
    calls = [call for call in e.channel.bot.request.calls if call.endpoint == "editMessageText"]
    assert len(calls) == 1 and "Which source?" in calls[0].parameters["text"]


@pytest.mark.asyncio
@pytest.mark.parametrize("settle", [False, True])
async def test_decision_waits_for_exact_attempt_or_original_hold_deadline(work_delivery, settle):
    from datetime import timedelta
    from uuid import uuid4

    from test_telegram_work_event_intents import activate

    from app.telegram_bots.notifications import aware
    from app.telegram_bots.work_event_delivery import WorkEventDelivery
    from deerflow.agent_instances.work_execution_contract import ReportWork

    e = work_delivery
    _peer, worker = await setup(e)
    work, execution, scope, run = await activate(e)
    current = (await scope.context(execution))["work"]
    await scope.report(
        execution, ReportWork(operation_id=uuid4().hex, expected_revision=current["revision"], action="request_input", statement="Choose a source", purpose="decision", reason="Need authorization", expected_response="A decision")
    )
    await worker.tick()
    row = next(item for item in await intents(e) if item.work_id == work["id"] and item.class_ == "question")
    moment = [aware(row.created_at) + timedelta(seconds=1)]
    e.plans.outbox.clock = lambda: moment[0]
    materializer = WorkEventDelivery(e.plans)
    assert await materializer.materialize(e.link, row.id) is False
    async with e.db.sessions() as reader:
        assert not (await reader.get(TelegramOutboxRow, row.id)).parts
    if settle:
        await scope.finish(execution, run, settled=True)
    else:
        moment[0] += timedelta(seconds=e.db.config.outbox.decision_hold_seconds)
    assert await materializer.materialize(e.link, row.id)


@pytest.mark.asyncio
async def test_intent_changed_before_sdk_capture_cannot_reuse_source_signature(work_delivery):
    from app.telegram_bots.work_event_delivery import WorkEventDelivery
    from app.telegram_bots.work_request_source import WorkRequestSources

    e = work_delivery
    identity, _req = await queued(e)
    await WorkEventDelivery(e.plans).materialize(e.link, identity)
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, identity)
        row.payload = {**row.payload, "work_event_intent": {**row.payload["work_event_intent"], "event_revision": 99}}
    e.channel._services.work_request_sources = WorkRequestSources(reader=e.reader, outbox=e.plans.outbox, callbacks=e.plans.callbacks)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert e.dispatch_errors
    assert not any(call.endpoint == "sendMessage" for call in e.channel.bot.request.calls)
