"""Daily summary plans are durable, silent and source-qualified at dispatch."""

import traceback

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_bot_adapter import message
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow, TelegramPersonSettingsRow


async def prepare(e):
    from app.telegram_bots.summary_delivery import SummaryPlans, SummarySources
    from app.telegram_bots.summary_sources import SummaryReader

    async with e.db.sessions.begin() as writer:
        for row in await writer.scalars(select(TelegramOutboxRow)):
            row.state = "sent"
        preferences = await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)
        preferences.timezone, preferences.summary_minute, preferences.quiet_enabled = "UTC", 0, False
    reader = SummaryReader(work=e.human.work, authority=e.live.authority, links=e.db.links)
    plans = SummaryPlans(reader=reader, outbox=e.sender.outbox, callbacks=e.plans.callbacks)
    e.channel._services.summary_sources = SummarySources(plans)
    return plans


@pytest.mark.asyncio
async def test_daily_summary_is_deduplicated_and_delivered_silently(work_delivery):
    e = work_delivery
    plans = await prepare(e)
    identity = await plans.plan(e.link)
    assert identity and await plans.plan(e.link) == identity
    message(e.channel.bot.request, identity=7301)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "sent", "\n".join("".join(traceback.format_exception(error)) for error in e.dispatch_errors)
        assert row.dedupe_key == "summary:" + e.sender.outbox.clock().date().isoformat()
        controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"])))
        assert {control.kind for control in controls} == {"digest_item", "summary_off"}
        assert all(control.message_id == 7301 for control in controls)
    calls = [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
    assert len(calls) == 1 and calls[0].parameters["disable_notification"] is True


@pytest.mark.asyncio
async def test_empty_summary_persists_nothing_to_say_once(work_delivery):
    from deerflow.persistence.agent_instances.human_input import HumanInputRequestRow
    from deerflow.persistence.agent_instances.work import AgentWorkRow

    e = work_delivery
    plans = await prepare(e)
    async with e.db.sessions.begin() as writer:
        request = await writer.get(HumanInputRequestRow, e.request["id"])
        request.state, request.closed_reason = "closed", "withdrawn"
        work = await writer.get(AgentWorkRow, e.work["id"])
        work.status, work.blocker = "cancelled", None
    identity = await plans.plan(e.link)
    assert await plans.plan(e.link) == identity
    assert await e.sender.tick() == 0
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "skipped" and row.skip_reason == "nothing-to-say" and row.parts == []


@pytest.mark.asyncio
async def test_disabled_summary_is_not_planned(work_delivery):
    e = work_delivery
    plans = await prepare(e)
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)).summary_enabled = False
    assert await plans.plan(e.link) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["title", "request", "grant", "settings", "claim", "callback", "protection"])
async def test_summary_final_provider_fence_prevents_stale_sdk_dispatch(work_delivery, monkeypatch, change):
    from test_telegram_digest_item_families import protect

    from app.gateway import authz
    from deerflow.persistence.agent_instances.human_input import HumanInputRequestRow
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
    from deerflow.persistence.agent_instances.work import AgentWorkRow

    e = work_delivery
    plans = await prepare(e)
    identity = await plans.plan(e.link)
    owner = e.channel._services.summary_sources
    authorize, resolve = owner.authorize, authz.resolve_route_permissions
    armed, changed = [], []

    async def final(*args, **kwargs):
        armed.append(True)
        return await authorize(*args, **kwargs)

    async def provider(*args, **kwargs):
        held = await resolve(*args, **kwargs)
        if armed and not changed:
            if change == "protection":
                await protect(e)
            else:
                async with e.db.sessions.begin() as writer:
                    if change == "title":
                        (await writer.get(AgentWorkRow, e.work["id"])).objective = "Changed during final provider"
                    elif change == "request":
                        request = await writer.get(HumanInputRequestRow, e.request["id"])
                        request.state, request.closed_reason = "closed", "withdrawn"
                    elif change == "grant":
                        (await writer.get(AgentInstanceGrantRow, (e.link.agent_ref, e.link.owner_user_id))).permissions = 1
                    elif change == "settings":
                        (await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)).summary_enabled = False
                    elif change == "claim":
                        (await writer.get(TelegramOutboxRow, identity)).claim_id = "foreign"
                    else:
                        row = await writer.get(TelegramOutboxRow, identity)
                        control = await writer.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[0]["callback_group_id"]))
                        control.consumed_at, control.closed_reason = plans.outbox.clock(), "superseded"
            changed.append(True)
        return held

    monkeypatch.setattr(owner, "authorize", final)
    monkeypatch.setattr(authz, "resolve_route_permissions", provider)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert changed and e.dispatch_errors
    assert not any(call.endpoint == "sendMessage" for call in e.channel.bot.request.calls)


@pytest.mark.asyncio
async def test_protected_summary_sends_only_show_teaser(work_delivery):
    from test_telegram_digest_item_families import protect

    e = work_delivery
    plans = await prepare(e)
    await protect(e)
    identity = await plans.plan(e.link)
    message(e.channel.bot.request, identity=7302)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "sent", e.dispatch_errors
        controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[0]["callback_group_id"])))
        assert [control.kind for control in controls] == ["reveal"]
    call = next(call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage")
    assert call.parameters["protect_content"] is True and call.parameters["disable_notification"] is True
    assert e.work["objective"] not in call.parameters["text"]


@pytest.mark.asyncio
async def test_daily_summary_survives_restart_and_parallel_planners(work_delivery):
    import asyncio
    from datetime import timedelta

    from app.telegram_bots.summary_delivery import SummaryPlans

    e = work_delivery
    first = await prepare(e)
    restarted = SummaryPlans(reader=first.reader, outbox=first.outbox, callbacks=first.callbacks)
    identities = await asyncio.gather(first.plan(e.link), restarted.plan(e.link))
    assert identities[0] == identities[1]
    now = first.outbox.clock()
    first.outbox.clock = lambda: now + timedelta(days=1)
    next_id = await restarted.plan(e.link)
    assert next_id != identities[0]
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, next_id)
        previous = await reader.get(TelegramOutboxRow, identities[0])
        assert row.payload["summary_basis"]["since"] == previous.payload["summary_basis"]["as_of"]


@pytest.mark.asyncio
async def test_summary_rejects_malformed_complete_ack_before_controls_activate(work_delivery):
    from app.telegram_bots.producer import PartReceipt

    e = work_delivery
    plans = await prepare(e)
    identity = await plans.plan(e.link)

    async def malformed(**kwargs):
        return PartReceipt((7304, 7305), False)

    e.sender.deliver_part = malformed
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "failed" and row.failed_reason == "invalid-summary-receipt"
        controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[0]["callback_group_id"])))
        assert controls and all(control.message_id is None and control.payload.get("presentation_pending") is True for control in controls)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["date", "as_of", "sections", "silent", "control_age", "source"])
async def test_summary_rejects_changed_persisted_evidence(work_delivery, damage):
    from copy import deepcopy
    from datetime import timedelta

    e = work_delivery
    plans = await prepare(e)
    identity = await plans.plan(e.link)
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, identity)
        payload = deepcopy(row.payload)
        if damage == "date":
            payload["summary_basis"]["date"] = "2035-01-01"
        elif damage == "as_of":
            payload["summary_basis"]["as_of"] = (plans.outbox.clock() + timedelta(days=1)).isoformat()
        elif damage == "sections":
            payload["summary_basis"]["sections"] = None
        elif damage == "silent":
            row.silent = False
        elif damage == "control_age":
            control = await writer.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[0]["callback_group_id"]))
            control.created_at += timedelta(days=1)
        else:
            parts = deepcopy(row.parts)
            parts[0]["source_ref"]["values"]["outbox_id"] = "f" * 32
            row.parts = parts
        row.payload = payload
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert e.dispatch_errors
    assert not any(call.endpoint == "sendMessage" for call in e.channel.bot.request.calls)


@pytest.mark.asyncio
async def test_summary_ack_cannot_activate_replaced_wire_group(work_delivery):
    import asyncio

    e = work_delivery
    plans = await prepare(e)
    identity = await plans.plan(e.link)
    release = asyncio.Event()
    call = e.channel.bot.request.enqueue("sendMessage", release=release, result={"message_id": 7901, "date": 1893456000, "chat": {"id": e.link.chat_id, "type": "private"}, "text": "Summary"})
    await e.sender.tick()
    await asyncio.wait_for(call.started.wait(), 5)
    try:
        async with e.db.sessions.begin() as writer:
            row = await writer.get(TelegramOutboxRow, identity)
            old_group = row.parts[0]["callback_group_id"]
            group = await plans.callbacks.write_group(e.link, row.payload["summary_basis"]["keyboard"], anchor_outbox_id=row.id, presentation_version=0, chat_id=e.link.chat_id, message_id=None, message_thread_id=None, session=writer)
            assert group.group_id != old_group
            for control in await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == group.group_id)):
                control.payload = {**control.payload, "presentation_pending": True}
                control.created_at = row.created_at
            row.parts = [{**row.parts[0], "callback_group_id": group.group_id}]
    finally:
        release.set()
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == group.group_id)))
        assert row.state == "failed" and row.failed_reason == "invalid-summary-receipt", (row.state, row.failed_reason)
        assert controls and all(control.message_id is None and control.payload["presentation_pending"] is True for control in controls)
        assert row.payload["summary_dispatch_conflict"]["parts"][0]["callback_group_id"] == old_group
    from datetime import timedelta

    from app.telegram_bots.retention import _sweep_outbox

    later = plans.outbox.clock() + timedelta(days=plans.callbacks.config.callbacks.max_age_days + 1)
    await plans.callbacks.sweep(later)
    await _sweep_outbox(e.db.sessions, later, plans.callbacks.config.callbacks.max_age_days)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramOutboxRow, identity) is not None
        assert await reader.scalar(select(TelegramCallbackRow.id).where(TelegramCallbackRow.group_id == old_group)) is not None


@pytest.mark.asyncio
async def test_summary_keeps_valid_old_token_verification_continuation(work_delivery):
    e = work_delivery
    plans = await prepare(e)
    identity = await plans.plan(e.link)
    assert e.channel.transport_revision >= 1
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, identity)
        row.parts = [{**row.parts[0], "invalid_token_verification_pending": {"token_revision": e.channel.transport_revision - 1, "http_status": 401, "failure_logged": True}}]
    message(e.channel.bot.request, identity=7902)
    await e.sender.tick()
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "sent", (row.state, row.failed_reason, e.dispatch_errors)


@pytest.mark.asyncio
@pytest.mark.parametrize("version", [True, "1", "99999999999999999999999999999999999999999999", None])
async def test_summary_does_not_take_cutoff_from_malformed_prior_plan(work_delivery, version):
    from copy import deepcopy
    from datetime import timedelta

    e = work_delivery
    plans = await prepare(e)
    initial = plans.outbox.clock()
    plans.outbox.clock = lambda: initial
    identity = await plans.plan(e.link)
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, identity)
        body = deepcopy(row.payload)
        body["summary_basis"]["version"] = version
        row.payload = body
        from sqlalchemy.orm.attributes import flag_modified

        flag_modified(row, "payload")
    plans.outbox.clock = lambda: initial + timedelta(days=2)
    current = await plans.plan(e.link)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, current)
        assert row.payload["summary_basis"]["since"] == (initial + timedelta(days=1)).isoformat()
