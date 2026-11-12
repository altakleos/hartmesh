"""Recorded SDK delivery for canonical non-review result notifications."""

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_bot_adapter import message
from test_telegram_result_notice_sources import queued
from test_telegram_work_file_exports import work_exports as work_exports
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow


async def prepare(e):
    from _telegram_bot_test_support import ManualClock

    from app.telegram_bots.files import FileBytesBudget
    from app.telegram_bots.result_notice_delivery import ResultNoticePlans, ResultNoticeSources

    reader, item = await queued(e)
    services = e.channel._services
    async with e.db.sessions.begin() as writer:
        for row in await writer.scalars(select(TelegramOutboxRow)):
            if row.id != item.id:
                row.state = "sent"
    e.delivery_clock = ManualClock(services.outbox.clock())
    services.outbox.clock = e.delivery_clock.now
    services.file_bytes_budget = FileBytesBudget(services.config.files.in_memory_max_bytes)
    plans = ResultNoticePlans(reader=reader, outbox=services.outbox, callbacks=services.callbacks)
    services.result_notice_sources = ResultNoticeSources(plans)
    assert await plans.materialize(e.link, item.id)
    return plans, item.id


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["done_event", "waiting_event", "unrouted_event"], indirect=True)
async def test_result_notice_delivers_files_before_complete_card_without_review_controls(work_exports):
    e = work_exports
    plans, identity = await prepare(e)
    assert await plans.materialize(e.link, identity)
    sdk = e.channel.bot.request
    message(sdk, "sendDocument", identity=971)
    message(sdk, identity=972)
    for _ in range(2):
        assert await e.sender.tick() == 1
        await e.sender.drain()
        e.delivery_clock.advance(2)
    assert not e.dispatch_errors, e.dispatch_errors
    calls = [call for call in sdk.calls if call.endpoint in {"sendDocument", "sendMessage"}]
    assert [call.endpoint for call in calls] == ["sendDocument", "sendMessage"]
    assert calls[0].parameters["disable_notification"] is True
    assert "reply_to_message_id" not in calls[0].parameters
    assert "The findings are verified." in calls[-1].parameters["text"]
    assert "Files above:" in calls[-1].parameters["text"]
    if e.result["status"] == "submitted" and not hasattr(e, "reviewer"):
        assert "Waiting for Colleague to review." in calls[-1].parameters["text"]
    assert not calls[-1].parameters.get("reply_markup")
    assert e.native_reads == ["outputs/result.txt"]
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "sent" and [part["message_ids"] for part in row.parts] == [[971], [972]]
        assert not list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.payload["anchor_outbox_id"].as_string() == identity)))


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["done_event"], indirect=True)
@pytest.mark.parametrize("change", ["event", "outcome", "grant", "claim", "preferences"])
async def test_result_upload_final_snapshot_refuses_changes_after_settings_read(work_exports, monkeypatch, change):
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
    from deerflow.persistence.agent_instances.work import AgentWorkEventRow, AgentWorkRow
    from deerflow.persistence.telegram_bots.model import TelegramPersonSettingsRow

    e = work_exports
    _plans, identity = await prepare(e)
    sources = e.channel._services.result_notice_sources
    original, changed = sources.settings.get, []

    async def alter(*args, **kwargs):
        settings = await original(*args, **kwargs)
        if not changed:
            async with e.db.sessions.begin() as writer:
                row = await writer.get(TelegramOutboxRow, identity)
                if change == "event":
                    (await writer.get(AgentWorkEventRow, row.event_id)).actor_id = "changed-actor"
                elif change == "outcome":
                    work = await writer.get(AgentWorkRow, row.work_id)
                    work.outcome = {**work.outcome, "statement": "Changed result"}
                elif change == "grant":
                    (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.actor.subject_id))).permissions = 1
                elif change == "claim":
                    row.claim_id = "different-claim"
                else:
                    (await writer.get(TelegramPersonSettingsRow, e.actor.subject_id)).notify = "none"
            changed.append(True)
        return settings

    monkeypatch.setattr(sources.settings, "get", alter)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert changed and e.dispatch_errors
    assert not e.native_reads
    assert not any(call.endpoint in {"sendDocument", "sendMessage"} for call in e.channel.bot.request.calls)


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["done_event"], indirect=True)
async def test_result_notice_needs_read_and_export_but_no_review_write_ceiling(work_exports, monkeypatch):
    import app.gateway.authz as authz
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow

    e = work_exports
    original = authz.resolve_route_permissions

    async def read_only(*args, **kwargs):
        return set(await original(*args, **kwargs)) - {"agents:write"}

    monkeypatch.setattr(authz, "resolve_route_permissions", read_only)
    async with e.db.sessions.begin() as writer:
        (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.actor.subject_id))).permissions = 3
    await test_result_notice_delivers_files_before_complete_card_without_review_controls(e)


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["done_event"], indirect=True)
@pytest.mark.parametrize("failure", ["no_access", "too_large"])
async def test_result_notice_file_warning_precedes_card_with_qualified_navigation(work_exports, failure):
    from app.telegram_bots.producer import SourceUnavailable
    from deerflow.persistence.spaces.model import SpaceGrantRow

    e = work_exports
    e.live.config.model_extra["telegram_bots"] = {**(e.live.config.model_extra.get("telegram_bots") or {}), "public_base_url": "https://example.test"}
    _plans, identity = await prepare(e)
    if failure == "no_access":
        async with e.db.sessions.begin() as writer:
            (await writer.get(SpaceGrantRow, (e.home.id, "human", e.actor.subject_id))).permissions = 1
    else:
        e.channel._services.config.files.max_outbound_bytes = 3
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert len(e.dispatch_errors) == 1 and isinstance(e.dispatch_errors[0], SourceUnavailable)
    message(e.channel.bot.request, identity=991)
    message(e.channel.bot.request, identity=992)
    for _ in range(2):
        e.delivery_clock.advance(2)
        assert await e.sender.tick() == 1
        await e.sender.drain()
    assert len(e.dispatch_errors) == 1, e.dispatch_errors
    calls = [call for call in e.channel.bot.request.calls if call.endpoint in {"sendDocument", "sendMessage"}]
    assert [call.endpoint for call in calls] == ["sendMessage", "sendMessage"]
    assert "result.txt" in calls[0].parameters["text"]
    assert bool(calls[0].parameters.get("reply_markup")) == (failure == "too_large")
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, identity)).state == "sent"


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["done_event", "waiting_event", "unrouted_event"], indirect=True)
@pytest.mark.parametrize("retention", [False, True])
async def test_protected_result_notice_requires_actual_show_and_cleanup_before_files(work_exports, retention):
    from test_telegram_work_callback_dispatch import install_dispatch, press_work
    from test_telegram_work_control_admission import admissions

    from app.telegram_bots.work_outcome_delivery import WorkOutcomePlans
    from deerflow.persistence.telegram_bots.model import TelegramInstancePolicyRow

    e = work_exports
    async with e.db.sessions.begin() as writer:
        writer.add(TelegramInstancePolicyRow(instance_id=e.instance.id, policy="protected", updated_by=e.actor.subject_id, updated_at=e.sender.outbox.clock()))
    _plans, identity = await prepare(e)
    sdk = e.channel.bot.request
    message(sdk, identity=981)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not e.native_reads
    if e.dispatch_errors:
        raise e.dispatch_errors[0]
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "sent", e.dispatch_errors
        show = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"]))
    calls = [call for call in sdk.calls if call.endpoint == "sendMessage"]
    assert len(calls) == 1 and calls[0].parameters["text"] == "An assignment is finished."
    install_dispatch(e, WorkOutcomePlans(e.plans, admissions(e).acts))
    await press_work(e, show, message_id=981)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.payload["result_notice_basis"]["cleanup_id"]
        cleanup = await reader.get(TelegramOutboxRow, row.payload["result_notice_basis"]["cleanup_id"])
        assert len(cleanup.dedupe_key) <= TelegramOutboxRow.__table__.c.dedupe_key.type.length
        assert (await reader.get(TelegramCallbackRow, show.id)).consumed_at is not None
    e.delivery_clock.advance(2)
    message(sdk, "editMessageReplyMarkup", identity=981)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not e.native_reads
    if retention:
        from datetime import timedelta

        from app.telegram_bots.retention import OUTBOX_RETENTION_DAYS, _sweep_outbox

        async with e.db.sessions.begin() as writer:
            parent = await writer.get(TelegramOutboxRow, identity)
            cleanup_id = parent.payload["result_notice_basis"]["cleanup_id"]
            cleanup = await writer.get(TelegramOutboxRow, cleanup_id)
            cleanup.closed_at = e.delivery_clock.now() - timedelta(days=OUTBOX_RETENTION_DAYS + 1)
        await _sweep_outbox(e.db.sessions, e.delivery_clock.now(), e.channel._services.callbacks.config.callbacks.max_age_days)
        async with e.db.sessions() as reader:
            assert await reader.get(TelegramOutboxRow, cleanup_id) is not None
    message(sdk, "sendDocument", identity=982)
    message(sdk, identity=983)
    for _ in range(2):
        e.delivery_clock.advance(2)
        assert await e.sender.tick() == 1
        await e.sender.drain()
    assert not e.dispatch_errors, e.dispatch_errors
    calls = [call for call in sdk.calls if call.endpoint in {"sendDocument", "sendMessage"}]
    assert [call.endpoint for call in calls] == ["sendMessage", "sendDocument", "sendMessage"]
    assert all(call.parameters["protect_content"] for call in calls)
    if e.result["status"] == "submitted":
        assert all(call.parameters["disable_notification"] for call in calls)
    assert not calls[-1].parameters.get("reply_markup")
    if retention:
        await e.channel._services.callbacks.sweep(e.channel._services.callbacks.clock() + timedelta(days=e.channel._services.callbacks.config.callbacks.max_age_days + 1))
        async with e.db.sessions() as reader:
            assert await reader.get(TelegramCallbackRow, show.id) is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["done_event"], indirect=True)
async def test_protected_result_callback_survives_duplicate_ignored_account_facts(work_exports):
    from deerflow.persistence.user.access import DisabledIdentityRow

    e = work_exports
    async with e.db.sessions.begin() as writer:
        for issuer in ("https://first.example.test", "https://second.example.test"):
            writer.add(DisabledIdentityRow(issuer=issuer, subject=e.actor.subject_id, disabled_at=e.sender.outbox.clock()))
    await test_protected_result_notice_requires_actual_show_and_cleanup_before_files(e, False)
