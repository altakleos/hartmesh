"""Raw class labels cannot replace digest source authority."""

import asyncio
from copy import deepcopy

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_bot_adapter import message
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.digests import key_for
from app.telegram_bots.work_digest_delivery import recipes
from deerflow.persistence.telegram_bots.model import TelegramOutboxRow


@pytest.mark.asyncio
@pytest.mark.parametrize("key", ["digest.header", "summary.item.question", "concealed.digest_news"])
async def test_generic_reply_cannot_impersonate_a_digest(work_delivery, key):
    e = work_delivery
    async with e.db.sessions.begin() as writer:
        for row in await writer.scalars(select(TelegramOutboxRow)):
            row.state = "sent"
    await e.sender.outbox.plan_product(e.link, source_id="unqualified-digest", parts=[{"kind": "text", "source_ref": {"key": key, "values": {"n": 3, "objective": "Fabricated assignment"}}}])
    message(e.channel.bot.request, identity=5101)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]


async def reserved(e):
    from _telegram_bot_test_support import ManualClock
    from test_telegram_work_event_intents import intents, request, setup

    from app.telegram_bots.digests import DigestReservations
    from app.telegram_bots.work_digest_delivery import DigestPlans, DigestSources
    from app.telegram_bots.work_digest_members import DigestMembers
    from app.telegram_bots.work_event_delivery import WorkEventDelivery

    e.delivery_clock = ManualClock(e.sender.outbox.clock())
    e.sender.outbox.clock = e.delivery_clock.now
    peer, worker = await setup(e)
    async with e.db.sessions.begin() as writer:
        for row in await writer.scalars(select(TelegramOutboxRow)):
            row.state = "sent"
    for _ in range(3):
        await request(e, peer)
    await worker.tick()
    members = [row.id for row in await intents(e)]
    for identity in members:
        await WorkEventDelivery(e.plans).materialize(e.link, identity)
    from app.telegram_bots.notifications import aware

    async with e.db.sessions() as reader:
        due = max([aware((await reader.get(TelegramOutboxRow, member)).not_before) for member in members])
    e.delivery_clock.advance(max(1, (due - e.delivery_clock.now()).total_seconds() + 1))
    identity = await DigestReservations(e.sender.outbox).reserve(e.link, category="essential")
    assert identity is not None
    plans = DigestPlans(outbox=e.sender.outbox, callbacks=e.plans.callbacks, members=DigestMembers(requests=e.reader))
    e.channel._services.work_digest_sources = DigestSources(plans)
    e.channel._now = lambda: e.sender.outbox.clock()
    return plans, identity, members


@pytest.mark.asyncio
@pytest.mark.parametrize("protected", [False, True])
async def test_digest_sends_one_qualified_navigation_and_acks_members_together(work_delivery, protected):
    from datetime import UTC, datetime

    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramInstancePolicyRow

    e = work_delivery
    if protected:
        async with e.db.sessions.begin() as writer:
            writer.add(TelegramInstancePolicyRow(instance_id=e.link.agent_ref, policy="protected", updated_by=e.actor.subject_id, updated_at=datetime.now(UTC)))
    plans, identity, members = await reserved(e)
    assert await plans.materialize(e.link, identity)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        parts = row.parts
        assert len(parts) == 1
        assert all([(await reader.get(TelegramOutboxRow, member)).state == "pending" for member in members])
    assert await plans.materialize(e.link, identity)
    message(e.channel.bot.request, identity=5201)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    if e.dispatch_errors:
        raise e.dispatch_errors[0]
    calls = [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
    assert len(calls) == 1
    call = calls[0]
    assert call.parameters["protect_content"] is protected
    buttons = [button for line in call.parameters["reply_markup"]["inline_keyboard"] for button in line]
    assert len(buttons) == (1 if protected else 3)
    assert "Which source?" not in call.parameters["text"]
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "sent" and row.message_id == 5201
        for member in members:
            child = await reader.get(TelegramOutboxRow, member)
            assert (child.state, child.skip_reason) == ("skipped", "in-digest")
        controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[0]["callback_group_id"])))
        assert controls and all(control.payload["presentation_pending"] is False for control in controls)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["request", "member_claim", "member_source", "parent_claim", "locale", "callback", "grant"])
async def test_digest_final_provider_read_refuses_any_changed_member_or_authority(work_delivery, monkeypatch, change):
    from app.gateway import authz
    from app.telegram_bots import work_digest_delivery as module
    from deerflow.persistence.agent_instances.human_input import HumanInputRequestRow
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
    from deerflow.persistence.agent_instances.work import AgentWorkEventRow
    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramPersonSettingsRow

    e = work_delivery
    plans, identity, members = await reserved(e)
    await plans.materialize(e.link, identity)
    original_prepare, original_provider = module.prepare_attention, authz.resolve_route_permissions
    active, changed = [], []

    async def prepare(*args, **kwargs):
        admission = await original_prepare(*args, **kwargs)

        async def check(**options):
            projection = options.get("projection")
            if isinstance(projection, module._BatchProjection) and projection.captured is not None:
                active.append(True)
            try:
                return await admission(**options)
            finally:
                active.clear()

        return check

    async def provider(*args, **kwargs):
        held = await original_provider(*args, **kwargs)
        if active and not changed:
            async with e.db.sessions.begin() as writer:
                child = await writer.get(TelegramOutboxRow, members[-1])
                if change == "request":
                    request = await writer.get(HumanInputRequestRow, child.request_id)
                    request.state, request.closed_reason = "closed", "resolved"
                elif change == "member_claim":
                    child.claim_id = "foreign-claim"
                elif change == "member_source":
                    (await writer.get(AgentWorkEventRow, child.event_id)).actor_id = "changed-source"
                elif change == "parent_claim":
                    (await writer.get(TelegramOutboxRow, identity)).claim_id = "foreign-claim"
                elif change == "locale":
                    (await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)).locale = "zh-CN"
                elif change == "callback":
                    parent = await writer.get(TelegramOutboxRow, identity)
                    control = await writer.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == parent.parts[0]["callback_group_id"]))
                    control.target_id = "f" * 32
                else:
                    (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 1
            changed.append(True)
        return held

    monkeypatch.setattr(module, "prepare_attention", prepare)
    monkeypatch.setattr(authz, "resolve_route_permissions", provider)
    message(e.channel.bot.request, identity=5211)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert changed and e.dispatch_errors
    assert not [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
    async with e.db.sessions() as reader:
        for member in members:
            assert (await reader.get(TelegramOutboxRow, member)).skip_reason != "in-digest"


@pytest.mark.asyncio
async def test_digest_locale_retirement_releases_original_members_without_replacement(work_delivery):
    from deerflow.persistence.telegram_bots.model import TelegramPersonSettingsRow

    e = work_delivery
    plans, identity, members = await reserved(e)
    await plans.materialize(e.link, identity)
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)).locale = "zh-CN"
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
    async with e.db.sessions() as reader:
        parent = await reader.get(TelegramOutboxRow, identity)
        assert parent.state == "skipped" and parent.payload["member_ids"] == members
        for identity in members:
            child = await reader.get(TelegramOutboxRow, identity)
            assert child.state == "pending" and not child.payload.get("digest_id") and child.claim_id is None


@pytest.mark.asyncio
async def test_digest_transient_retry_preserves_membership_until_ack(work_delivery):
    from app.telegram_bots.notifications import aware

    e = work_delivery
    plans, identity, members = await reserved(e)
    await plans.materialize(e.link, identity)
    sdk = e.channel.bot.request
    sdk.enqueue("sendMessage", status=503, body={"ok": False, "error_code": 503, "description": "Service unavailable"})
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        parent = await reader.get(TelegramOutboxRow, identity)
        assert parent.state == "pending" and parent.payload["member_ids"] == members
        for member in members:
            child = await reader.get(TelegramOutboxRow, member)
            assert child.state == "pending" and child.payload["digest_id"] == identity and child.claim_id is None
        e.delivery_clock.advance(max(2, (aware(parent.not_before) - e.delivery_clock.now()).total_seconds() + 1))
    message(sdk, identity=5221)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, identity)).state == "sent"
        for member in members:
            assert (await reader.get(TelegramOutboxRow, member)).skip_reason == "in-digest"


@pytest.mark.asyncio
async def test_digest_forged_html_cannot_replace_its_qualified_navigation(work_delivery):
    e = work_delivery
    plans, identity, _ = await reserved(e)
    await plans.materialize(e.link, identity)
    async with e.db.sessions.begin() as writer:
        parent = await writer.get(TelegramOutboxRow, identity)
        parent.parts = [{**parent.parts[0], "html": "Forged digest text"}]
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]


@pytest.mark.asyncio
async def test_digest_does_not_keep_obsolete_member_sound_choices(work_delivery):
    e = work_delivery
    plans, identity, members = await reserved(e)
    await plans.materialize(e.link, identity)
    async with e.db.sessions.begin() as writer:
        for member in members:
            (await writer.get(TelegramOutboxRow, member)).silent = True
    message(e.channel.bot.request, identity=5241)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]


@pytest.mark.asyncio
async def test_digest_reminder_keeps_quiet_summary_suppression(work_delivery):
    from _telegram_bot_test_support import ManualClock
    from test_telegram_resume_reminders import reminder
    from test_telegram_work_news_delivery import prepare

    from app.telegram_bots.digests import DigestReservations
    from app.telegram_bots.work_digest_delivery import DigestPlans, DigestSources
    from app.telegram_bots.work_digest_members import DigestMembers
    from deerflow.persistence.telegram_bots.model import TelegramPersonSettingsRow

    e = work_delivery
    clock = ManualClock(e.sender.outbox.clock())
    e.sender.outbox.clock = clock.now
    item = await reminder(e)
    news, _ = await prepare(e, item=item)
    e.sender.outbox.config = e.sender.outbox.config.model_copy(update={"outbox": e.sender.outbox.config.outbox.model_copy(update={"digest_threshold": 1})})
    clock.advance(3610)
    identity = await DigestReservations(e.sender.outbox).reserve(e.link, category="essential")
    assert identity is not None
    plans = DigestPlans(outbox=e.sender.outbox, callbacks=e.plans.callbacks, members=DigestMembers(requests=e.reader, news=news.reader))
    e.channel._services.work_digest_sources = DigestSources(plans)
    await plans.materialize(e.link, identity)
    async with e.db.sessions.begin() as writer:
        settings = await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)
        settings.timezone = "UTC"
        settings.quiet_enabled, settings.quiet_start_minute, settings.quiet_end_minute = True, 0, 1439
        settings.summary_enabled = True
    message(e.channel.bot.request, identity=5251)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]


@pytest.mark.asyncio
@pytest.mark.parametrize("ack", [True, 0], ids=["boolean", "zero"])
async def test_digest_invalid_sdk_ack_does_not_settle_members(work_delivery, ack):
    e = work_delivery
    plans, identity, members = await reserved(e)
    await plans.materialize(e.link, identity)
    message(e.channel.bot.request, identity=ack)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, identity)).state != "sent"
        for member in members:
            assert (await reader.get(TelegramOutboxRow, member)).skip_reason != "in-digest"


@pytest.mark.asyncio
async def test_digest_shutdown_drains_sdk_and_preserves_ambiguous_reservation(work_delivery):
    import asyncio

    e = work_delivery
    plans, identity, members = await reserved(e)
    await plans.materialize(e.link, identity)
    release = asyncio.Event()
    call = e.channel.bot.request.enqueue("sendMessage", release=release, result={"message_id": 5261, "date": 1893456000, "chat": {"id": e.link.chat_id, "type": "private"}, "text": "Saved digest"})
    await e.sender.tick()
    await asyncio.wait_for(call.started.wait(), 5)
    stopping = asyncio.create_task(e.sender.stop())
    try:
        await asyncio.sleep(0.05)
        assert not stopping.done() and not call.cancelled.is_set()
    finally:
        release.set()
        await asyncio.wait_for(stopping, 5)
    assert not call.cancelled.is_set()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "pending" and row.claim_id is None and row.parts[0].get("dispatched_at")
        for member in members:
            child = await reader.get(TelegramOutboxRow, member)
            assert child.state == "pending" and child.claim_id is None and child.payload["digest_id"] == identity


@pytest.mark.asyncio
async def test_parent_sound_must_match_members(work_delivery):
    e = work_delivery
    plans, identity, members = await reserved(e)
    await plans.materialize(e.link, identity)
    async with e.db.sessions.begin() as writer:
        children = [await writer.get(TelegramOutboxRow, mid) for mid in members]
        assert any(not child.silent for child in children)
        (await writer.get(TelegramOutboxRow, identity)).silent = True
    message(e.channel.bot.request, identity=5281)
    await e.sender.tick()
    await e.sender.drain()
    calls = [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
    assert not calls or calls[-1].parameters.get("disable_notification") is False, "audible members sent silently from changed parent field"


@pytest.mark.asyncio
async def test_ack_cannot_settle_replaced_batch_manifest(work_delivery):
    from datetime import timedelta

    from app.telegram_bots.digests import DigestReservations
    from app.telegram_bots.retention import _pinned_deliveries

    e = work_delivery
    plans, identity, members = await reserved(e)
    await plans.materialize(e.link, identity)
    release = asyncio.Event()
    call = e.channel.bot.request.enqueue("sendMessage", release=release, result={"message_id": 5282, "date": 1893456000, "chat": {"id": e.link.chat_id, "type": "private"}, "text": "Saved digest"})
    await e.sender.tick()
    await asyncio.wait_for(call.started.wait(), 5)
    try:
        async with e.db.sessions.begin() as writer:
            parent = await writer.get(TelegramOutboxRow, identity)
            payload = deepcopy(parent.payload)
            for key in ("member_ids", "member_classes", "member_created_at"):
                payload[key] = payload[key][:-1]
            basis = payload["work_digest_basis"]
            basis["entries"] = basis["entries"][:-1]
            basis["members"] = basis["members"][:-1]
            part, keyboard = recipes(basis, identity)
            basis["parts"], basis["keyboard"] = part, keyboard
            payload["product_keyboard_basis"] = keyboard
            parent.payload = payload
            parent.dedupe_key = key_for(payload["category"], payload["member_ids"])
            parent.parts = [{**parent.parts[0], **part[0]}]
    finally:
        release.set()
    await e.sender.drain()
    async with e.db.sessions() as reader:
        parent = await reader.get(TelegramOutboxRow, identity)
        children = [await reader.get(TelegramOutboxRow, mid) for mid in members]
        assert (parent.state, parent.failed_reason) == ("failed", "invalid-digest-receipt")
        proof = parent.payload["digest_dispatch_conflict"]
        assert proof["payload"]["member_ids"] == members and proof["message_ids"] == [5282]
        assert all(child.state == "pending" and child.claim_id is None and child.payload["digest_id"] == identity for child in children)
        pinned = await _pinned_deliveries(reader, e.delivery_clock.now() + timedelta(days=60), 30)
        assert {identity, *members} <= pinned
    with pytest.raises(ValueError, match="reconciliation-required"):
        await DigestReservations(e.sender.outbox).abandon(e.link, identity)
    assert await e.sender.tick() == 0
