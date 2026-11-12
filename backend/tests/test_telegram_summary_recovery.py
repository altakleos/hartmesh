"""A queued daily summary refreshes current content without losing its intent."""

import pytest
from _telegram_bot_test_support import ManualClock
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_bot_adapter import message
from test_telegram_summary_delivery import prepare
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow, TelegramPersonSettingsRow


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["title", "locale", "protection", "empty"])
async def test_queued_summary_refresh_preserves_daily_identity_and_original_control_age(work_delivery, change):
    from test_telegram_digest_item_families import protect

    from deerflow.persistence.agent_instances.human_input import HumanInputRequestRow
    from deerflow.persistence.agent_instances.work import AgentWorkRow

    e = work_delivery
    plans = await prepare(e)
    clock = ManualClock(plans.outbox.clock())
    plans.outbox.clock = clock.now
    e.channel._now = clock.now
    identity = await plans.plan(e.link)
    if change == "protection":
        await protect(e)
    else:
        async with e.db.sessions.begin() as writer:
            if change == "title":
                (await writer.get(AgentWorkRow, e.work["id"])).objective = "Current assignment title"
            elif change == "locale":
                (await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)).locale = "zh-CN"
            else:
                request = await writer.get(HumanInputRequestRow, e.request["id"])
                request.state, request.closed_reason = "closed", "withdrawn"
                work = await writer.get(AgentWorkRow, e.work["id"])
                work.status, work.blocker = "cancelled", None
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not any(call.endpoint == "sendMessage" for call in e.channel.bot.request.calls)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.payload["presentation_version"] == 1, (row.state, row.skip_reason, e.dispatch_errors)
        assert row.parts[0]["state"] == "retired"
        if change == "empty":
            assert row.state == "skipped" and row.skip_reason == "nothing-to-say"
            return
        assert row.state == "pending"
        controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"])))
        assert controls and all(control.created_at == row.created_at for control in controls)
    clock.advance(2)
    message(e.channel.bot.request, identity=7401)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "sent", e.dispatch_errors
        if change == "protection":
            assert row.parts[-1]["protect_content"] is True
        if change == "locale":
            assert row.payload["summary_basis"]["locale"] == "zh-CN"
        if change == "title":
            assert "Current assignment title" in row.parts[-1]["visible_text"]


@pytest.mark.asyncio
async def test_summary_held_repair_cannot_take_an_active_claim(work_delivery):
    e = work_delivery
    plans = await prepare(e)
    identity = await plans.plan(e.link)
    recovery = e.channel._services.summary_sources.recovery
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, identity)
        row.state, row.failed_reason, row.claim_id = "failed", "summary-refresh-required", "foreign"
    live = await e.live.authority.prepare_live_owner(e.link.owner_user_id)
    guard = e.db.links.publication_guard(e.link, check_live_owner=live, check_attempt_current=lambda: True)
    assert await recovery.repair(e.link, identity, guard=guard) is None
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramOutboxRow, identity)).claim_id = None
    assert await recovery.repair(e.link, identity, guard=guard) == identity
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "pending" and row.payload["presentation_version"] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["source", "claim", "group", "settings"])
async def test_summary_repair_rolls_back_when_provider_snapshot_changes(work_delivery, monkeypatch, change):
    from app.gateway import authz
    from app.telegram_bots.summary_sources import SummaryRead
    from deerflow.persistence.agent_instances.work import AgentWorkRow

    e = work_delivery
    plans = await prepare(e)
    identity = await plans.plan(e.link)
    async with e.db.sessions.begin() as writer:
        (await writer.get(AgentWorkRow, e.work["id"])).objective = "Fresh title before repair"
    authorize, resolve = SummaryRead.authorize, authz.resolve_route_permissions
    held, changed = {}, []

    async def guarded(self, **kwargs):
        held["writer"] = kwargs.get("session")
        try:
            return await authorize(self, **kwargs)
        finally:
            held.pop("writer", None)

    async def provider(*args, **kwargs):
        permissions = await resolve(*args, **kwargs)
        writer = held.get("writer")
        if writer is not None and not changed:
            row = await writer.get(TelegramOutboxRow, identity)
            if change == "source":
                (await writer.get(AgentWorkRow, e.work["id"])).objective = "Changed during final admission"
            elif change == "claim":
                row.claim_id = "foreign"
            elif change == "settings":
                (await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)).summary_enabled = False
            else:
                control = await writer.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"]))
                control.consumed_at, control.closed_reason = plans.outbox.clock(), "superseded"
            await writer.flush()
            changed.append(True)
        return permissions

    monkeypatch.setattr(SummaryRead, "authorize", guarded)
    monkeypatch.setattr(authz, "resolve_route_permissions", provider)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert changed
    assert not any(call.endpoint == "sendMessage" for call in e.channel.bot.request.calls)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.payload["presentation_version"] == 0


@pytest.mark.asyncio
async def test_summary_two_refreshes_keep_valid_history_for_next_days_cutoff(work_delivery):
    from datetime import timedelta

    from deerflow.persistence.agent_instances.work import AgentWorkRow

    e = work_delivery
    plans = await prepare(e)
    clock = ManualClock(plans.outbox.clock())
    plans.outbox.clock = clock.now
    e.channel._now = clock.now
    initial = clock.now()
    identity = await plans.plan(e.link)
    for revision in range(2):
        async with e.db.sessions.begin() as writer:
            (await writer.get(AgentWorkRow, e.work["id"])).objective = f"Changed title {revision}"
        clock.advance(2)
        assert await e.sender.tick() == 1
        await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.payload["presentation_version"] == 2
        assert len(row.payload["summary_basis"]["history"]) == 2 and len(row.parts) == 3
    message(e.channel.bot.request, identity=7402)
    clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, identity)).state == "sent", e.dispatch_errors
    clock.advance(timedelta(days=2).total_seconds())
    next_id = await plans.plan(e.link)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, next_id)
        assert row.payload["summary_basis"]["since"] == initial.isoformat()


@pytest.mark.asyncio
async def test_summary_held_repair_fences_final_failure_disposition(work_delivery, monkeypatch):
    from app.gateway import authz
    from app.telegram_bots.producer import ProductSourceRetired
    from app.telegram_bots.summary_sources import SummaryRead

    e = work_delivery
    plans = await prepare(e)
    identity = await plans.plan(e.link)
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, identity)
        row.state, row.failed_reason = "failed", "summary-refresh-required"
    authorize, resolve = SummaryRead.authorize, authz.resolve_route_permissions
    held, changed = {}, []

    async def guarded(self, **kwargs):
        held["writer"] = kwargs.get("session")
        try:
            return await authorize(self, **kwargs)
        finally:
            held.pop("writer", None)

    async def provider(*args, **kwargs):
        permissions = await resolve(*args, **kwargs)
        writer = held.get("writer")
        if writer is not None and not changed:
            row = await writer.get(TelegramOutboxRow, identity)
            if row.payload["presentation_version"] == 1:
                row.failed_reason = "invalid-summary-receipt"
                await writer.flush()
                changed.append(True)
        return permissions

    monkeypatch.setattr(SummaryRead, "authorize", guarded)
    monkeypatch.setattr(authz, "resolve_route_permissions", provider)
    live = await e.live.authority.prepare_live_owner(e.link.owner_user_id)
    guard = e.db.links.publication_guard(e.link, check_live_owner=live, check_attempt_current=lambda: True)
    with pytest.raises(ProductSourceRetired):
        await e.channel._services.summary_sources.recovery.repair(e.link, identity, guard=guard)
    assert changed
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.payload["presentation_version"] == 0 and row.failed_reason == "summary-refresh-required"
