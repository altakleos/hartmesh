"""Canonical settlement notifications deliver review files before controls."""

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_bot_adapter import message
from test_telegram_work_file_exports import work_exports as work_exports
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow


async def queued(e):
    from _telegram_bot_test_support import ManualClock

    from app.telegram_bots.files import FileBytesBudget
    from app.telegram_bots.work_result_delivery import WorkReviewPlans
    from app.telegram_bots.work_result_source import WorkResultSources

    assert await e.event_worker.tick() >= 1
    async with e.db.sessions.begin() as writer:
        rows = list(await writer.scalars(select(TelegramOutboxRow)))
        review = next(row for row in rows if row.payload.get("work_event_intent", {}).get("key") == "review.ready")
        for row in rows:
            if row.id != review.id:
                row.state = "sent"
        identity = review.id
    services = e.channel._services
    e.delivery_clock = ManualClock(services.outbox.clock())
    services.outbox.clock = e.delivery_clock.now
    services.file_bytes_budget = FileBytesBudget(services.config.files.in_memory_max_bytes)
    services.work_result_sources = WorkResultSources(reader=e.reader, outbox=e.plans.outbox, callbacks=e.plans.callbacks)
    plans = WorkReviewPlans(outbox=e.plans.outbox, reader=e.reader, callbacks=e.plans.callbacks)
    return plans, identity


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["review_event", "review_empty_event"], indirect=True)
@pytest.mark.parametrize("routed", [False, True])
async def test_settled_review_event_sends_files_then_arms_review_controls(work_exports, routed):
    from app.telegram_bots.work_review_event_delivery import WorkReviewEventDelivery

    e = work_exports
    if routed:
        from uuid import uuid4

        from deerflow.agent_instances.human_input_contract import RequestCommand

        request = await e.reader.read_request(e.link, e.result["human_input_request_id"])
        await e.human.command(
            actor=e.actor,
            request_id=request.request["id"],
            request=RequestCommand(operation_id=uuid4().hex, action="route", expected_revision=request.seen["revision"], expected_request_revision=request.seen["request_revision"], recipient_id=e.actor.subject_id),
        )
    plans, identity = await queued(e)
    delivery = WorkReviewEventDelivery(plans)
    assert await delivery.materialize(e.link, identity)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        parts = row.parts
        controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == parts[-1]["callback_group_id"])))
        assert controls and all(control.payload["presentation_pending"] for control in controls)
        assert row.request_id == e.result["human_input_request_id"]
    assert await delivery.materialize(e.link, identity)
    message(e.channel.bot.request, "sendDocument", identity=951)
    message(e.channel.bot.request, identity=952)
    for _ in parts:
        assert await e.sender.tick() == 1
        await e.sender.drain()
        e.delivery_clock.advance(2)
    assert not e.dispatch_errors, e.dispatch_errors
    calls = [call for call in e.channel.bot.request.calls if call.endpoint in {"sendDocument", "sendMessage"}]
    assert [call.endpoint for call in calls] == (["sendDocument"] if e.result["outcome"]["sources"] else []) + ["sendMessage"]
    assert "The findings are verified." in calls[-1].parameters["text"]
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "sent" and len(row.parts) == len(parts)
        for control in controls:
            assert (await reader.get(TelegramCallbackRow, control.id)).payload["presentation_pending"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["review_event"], indirect=True)
@pytest.mark.parametrize("change", ["event", "outcome", "attempt", "claim"])
async def test_review_materialization_rechecks_event_and_result_after_provider(work_exports, monkeypatch, change):
    import app.gateway.authz as authz
    import app.telegram_bots.work_event_delivery as module
    from app.telegram_bots.producer import ProductSourceRetired
    from app.telegram_bots.work_review_event_delivery import WorkReviewEventDelivery, _ReviewMaterializationProjection
    from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow, AgentWorkRow

    e = work_exports
    plans, identity = await queued(e)
    prepare, resolve = module.prepare_attention, authz.resolve_route_permissions
    active, changed = {}, []

    async def prepared(*args, **kwargs):
        admission = await prepare(*args, **kwargs)

        async def check(**options):
            if isinstance(options["projection"], _ReviewMaterializationProjection):
                active["writer"] = options["session"]
            try:
                return await admission(**options)
            finally:
                active.clear()

        return check

    async def provider(*args, **kwargs):
        held = await resolve(*args, **kwargs)
        if active and not changed:
            writer = active["writer"]
            row = await writer.get(TelegramOutboxRow, identity)
            if change == "event":
                (await writer.get(AgentWorkEventRow, row.event_id)).actor_id = "changed-actor"
            elif change == "outcome":
                work = await writer.get(AgentWorkRow, row.work_id)
                work.outcome = {**work.outcome, "statement": "Different findings"}
            elif change == "attempt":
                (await writer.get(AgentWorkAttemptRow, row.payload["work_result_navigation"]["attempt_id"])).thread_id = None
            else:
                row.claim_id = "another-claim"
            await writer.flush()
            changed.append(True)
        return held

    monkeypatch.setattr(module, "prepare_attention", prepared)
    monkeypatch.setattr(authz, "resolve_route_permissions", provider)
    with pytest.raises(ProductSourceRetired):
        await WorkReviewEventDelivery(plans).materialize(e.link, identity)
    assert changed and not e.native_reads
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert not row.parts and "work_event_source" not in row.payload
        assert not list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.payload["anchor_outbox_id"].as_string() == identity)))


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["review_event"], indirect=True)
async def test_review_sdk_rejects_changed_canonical_event_before_file_bytes(work_exports, monkeypatch):
    from app.telegram_bots.work_review_event_delivery import WorkReviewEventDelivery
    from deerflow.persistence.agent_instances.work import AgentWorkEventRow

    e = work_exports
    plans, identity = await queued(e)
    await WorkReviewEventDelivery(plans).materialize(e.link, identity)
    sources = e.channel._services.work_result_sources
    original, changed = sources.settings.get, []

    async def altered(*args, **kwargs):
        settings = await original(*args, **kwargs)
        if not changed:
            async with e.db.sessions.begin() as writer:
                row = await writer.get(TelegramOutboxRow, identity)
                (await writer.get(AgentWorkEventRow, row.event_id)).actor_id = "changed-actor"
            changed.append(True)
        return settings

    monkeypatch.setattr(sources.settings, "get", altered)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert changed and e.dispatch_errors
    assert not e.native_reads
    assert not any(call.endpoint in {"sendDocument", "sendMessage"} for call in e.channel.bot.request.calls)


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["review_event"], indirect=True)
async def test_protected_review_event_requires_show_and_keyboard_cleanup_before_files(work_exports, monkeypatch):
    import test_telegram_work_result_recovery as recovery_tests

    from app.telegram_bots.work_review_event_delivery import WorkReviewEventDelivery

    async def prepare_event(e):
        plans, identity = await queued(e)
        assert await WorkReviewEventDelivery(plans).materialize(e.link, identity)
        return identity

    monkeypatch.setattr(recovery_tests, "prepare_delivery", prepare_event)
    await recovery_tests.test_show_clears_teaser_before_protected_files_and_final_controls(work_exports, "ok")
