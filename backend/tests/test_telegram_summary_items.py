"""Summary request buttons open full previews without an earlier notification."""

import pytest
from _telegram_bot_test_support import ManualClock
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_bot_adapter import message
from test_telegram_summary_delivery import prepare
from test_telegram_summary_views import pending_view
from test_telegram_work_file_exports import work_exports as work_exports
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow


async def summary_anchor(e, protected, *, request_id=None):
    if protected:
        views, clock, _parent, _callback, identity = await pending_view(e)
        plans = views.plans
        e.channel.bot.request.enqueue("editMessageText", result={"message_id": 7501, "date": 1893456000, "chat": {"id": e.link.chat_id, "type": "private"}, "text": "Summary"})
        clock.advance(2)
    else:
        plans = await prepare(e)
        clock = ManualClock(plans.outbox.clock())
        plans.outbox.clock = clock.now
        e.channel._now = clock.now
        identity = await plans.plan(e.link)
        message(e.channel.bot.request, identity=7501)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "sent", e.dispatch_errors
        callback = await reader.scalar(
            select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"], TelegramCallbackRow.kind == "digest_item", TelegramCallbackRow.target_id == (request_id or e.request["id"]))
        )
        assert callback is not None
    return plans, clock, identity, callback.id


async def pending(e, protected):
    from app.telegram_bots.work_digest_items import DigestItems
    from app.telegram_bots.work_request_source import WorkRequestSources

    plans, clock, anchor, callback = await summary_anchor(e, protected)
    items = DigestItems(digests=plans, requests=e.plans, summaries=plans)
    e.channel._services.work_request_sources = WorkRequestSources(reader=e.reader, outbox=e.sender.outbox, callbacks=e.plans.callbacks, navigation=items.navigation)
    owner = await e.live.authority.prepare_live_owner(e.link.owner_user_id)
    guard = e.db.links.publication_guard(e.link, check_live_owner=owner, check_attempt_current=lambda: True)
    options = dict(callback_id=callback, query_id="summary-item-one", chat_id=e.link.chat_id, message_id=7501, message_thread_id=None, guard=guard)
    identity = await items.open(e.link, **options)
    return items, clock, anchor, callback, identity, options


@pytest.mark.asyncio
@pytest.mark.parametrize("protected", [False, True])
async def test_summary_item_opens_current_question_without_notification_dependency(work_delivery, protected):
    e = work_delivery
    items, clock, anchor, callback, identity, options = await pending(e, protected)
    assert await items.open(e.link, **options) == identity
    # Summary navigation targets the request; no discarded question outbox is its origin.
    async with e.db.sessions.begin() as writer:
        original = await writer.get(TelegramOutboxRow, e.identity)
        original.payload = {"retired-notification": True}
    message(e.channel.bot.request, identity=7901)
    clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    calls = [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
    assert e.request["question"] in calls[-1].parameters["text"], e.dispatch_errors
    assert calls[-1].parameters["protect_content"] is protected
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "sent", e.dispatch_errors
        origin = row.payload["work_request_basis"]["navigation_origin"]
        assert origin["kind"] == "summary" and origin["anchor_outbox_id"] == anchor and origin["member_id"] == e.request["id"]
        assert (await reader.get(TelegramCallbackRow, callback)).consumed_at is None
    assert await items.open(e.link, **{**options, "query_id": "summary-item-two"}) != identity


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["anchor", "callback", "source", "claim", "question", "reason", "choices", "instance_name"])
async def test_summary_preview_final_source_read_refuses_changed_origin(work_delivery, monkeypatch, change):
    from app.gateway import authz
    from app.telegram_bots import work_request_source as module
    from app.telegram_bots.work_digest_projection import DigestProjection

    e = work_delivery
    _items, clock, anchor, callback, identity, _options = await pending(e, True)
    original_read, original_provider = module.current_attention, authz.resolve_route_permissions
    active, changed = [], []

    async def read(*args, **kwargs):
        if isinstance(kwargs.get("projection"), DigestProjection):
            active.append(True)
        try:
            return await original_read(*args, **kwargs)
        finally:
            active.clear()

    async def provider(*args, **kwargs):
        held = await original_provider(*args, **kwargs)
        if active and not changed:
            async with e.db.sessions.begin() as writer:
                if change == "anchor":
                    row = await writer.get(TelegramOutboxRow, anchor)
                    row.parts = [{**row.parts[-1], "message_ids": [5802]}]
                elif change == "callback":
                    (await writer.get(TelegramCallbackRow, callback)).target_id = "f" * 32
                elif change == "source":
                    from deerflow.persistence.agent_instances.human_input import HumanInputRequestRow

                    request = await writer.get(HumanInputRequestRow, e.request["id"])
                    request.state, request.closed_reason = "closed", "withdrawn"
                elif change in {"question", "reason", "choices"}:
                    from deerflow.persistence.agent_instances.human_input import HumanInputRequestRow

                    request = await writer.get(HumanInputRequestRow, e.request["id"])
                    setattr(request, change, ["Current choice"] if change == "choices" else "Current canonical replacement")
                elif change == "instance_name":
                    from deerflow.persistence.agent_instances.model import AgentInstanceRow

                    (await writer.get(AgentInstanceRow, e.link.agent_ref)).name = "Current instance name"
                else:
                    (await writer.get(TelegramOutboxRow, identity)).claim_id = "foreign"
            changed.append(True)
        return held

    monkeypatch.setattr(module, "current_attention", read)
    monkeypatch.setattr(authz, "resolve_route_permissions", provider)
    before = len([call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"])
    message(e.channel.bot.request, identity=6101)
    clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert changed
    assert len([call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["review_event"], indirect=True)
@pytest.mark.parametrize("protected", [False, True])
async def test_summary_review_preview_delivers_exact_files_before_controls(work_exports, protected):
    from test_telegram_work_review_event_delivery import queued

    from app.telegram_bots.work_digest_items import DigestItems
    from app.telegram_bots.work_result_source import WorkResultSources

    e = work_exports
    review, _notification = await queued(e)
    plans, clock, _anchor, callback = await summary_anchor(e, protected, request_id=e.result["human_input_request_id"])
    items = DigestItems(digests=plans, requests=e.plans, reviews=review, summaries=plans)
    e.channel._services.work_result_sources = WorkResultSources(reader=e.reader, outbox=e.sender.outbox, callbacks=e.plans.callbacks, navigation=items.navigation)
    owner = await e.live.authority.prepare_live_owner(e.link.owner_user_id)
    guard = e.db.links.publication_guard(e.link, check_live_owner=owner, check_attempt_current=lambda: True)
    identity = await items.open(e.link, callback_id=callback, query_id="summary-review", chat_id=e.link.chat_id, message_id=7501, message_thread_id=None, guard=guard)
    assert not e.native_reads
    message(e.channel.bot.request, "sendDocument", identity=7911)
    message(e.channel.bot.request, identity=7912)
    for _ in range(2):
        clock.advance(2)
        assert await e.sender.tick() == 1
        await e.sender.drain()
    assert not e.dispatch_errors
    assert e.native_reads == ["outputs/result.txt"]
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "sent" and row.class_ == "reply"
        assert "result_files" in row.payload["work_request_basis"]
        assert row.parts[0]["message_ids"] == [7911] and row.parts[-1]["message_ids"] == [7912]
        controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"])))
        assert controls and all(control.message_id == 7912 and control.payload.get("presentation_pending") is False for control in controls)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["control", "claim", "question"])
async def test_summary_preview_publication_rechecks_new_controls_after_provider(work_delivery, monkeypatch, change):
    from app.gateway import authz
    from app.telegram_bots import work_digest_items as module
    from app.telegram_bots.producer import ProductSourceRetired

    e = work_delivery
    digests, _clock, _anchor, callback = await summary_anchor(e, True)
    items = module.DigestItems(digests=digests, requests=e.plans, summaries=digests)
    owner = await e.live.authority.prepare_live_owner(e.link.owner_user_id)
    guard = e.db.links.publication_guard(e.link, check_live_owner=owner, check_attempt_current=lambda: True)
    original_prepare, original_provider = module.prepare_attention, authz.resolve_route_permissions
    active, changed = [], []

    async def prepare(*args, **kwargs):
        admission = await original_prepare(*args, **kwargs)

        async def check(**options):
            if isinstance(options.get("projection"), module._PreviewProjection):
                active.append((options["session"], options["projection"].guard.preview))
            try:
                return await admission(**options)
            finally:
                active.clear()

        return check

    async def provider(*args, **kwargs):
        held = await original_provider(*args, **kwargs)
        if active and not changed:
            writer, row = active[-1]
            if change == "control":
                control = await writer.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"]))
                control.target_id = "f" * 32
            elif change == "question":
                from deerflow.persistence.agent_instances.human_input import HumanInputRequestRow

                (await writer.get(HumanInputRequestRow, e.request["id"])).question = "Changed during final publication"
            else:
                (await writer.get(TelegramOutboxRow, row.id)).claim_id = "foreign"
            await writer.flush()
            changed.append(True)
        return held

    monkeypatch.setattr(module, "prepare_attention", prepare)
    monkeypatch.setattr(authz, "resolve_route_permissions", provider)
    with pytest.raises(ProductSourceRetired):
        await items.open(e.link, callback_id=callback, query_id="item-query", chat_id=e.link.chat_id, message_id=7501, message_thread_id=None, guard=guard)
    assert changed
    async with e.db.sessions() as reader:
        assert await reader.scalar(select(TelegramOutboxRow.id).where(TelegramOutboxRow.dedupe_key.like("notice:work-navigation:%"))) is None
        assert (await reader.get(TelegramCallbackRow, callback)).consumed_at is None


@pytest.mark.asyncio
async def test_pending_summary_preview_retains_expired_origin_without_rearming(work_delivery):
    from datetime import timedelta

    from app.telegram_bots.producer import ProductSourceRetired
    from app.telegram_bots.retention import _pinned_deliveries

    e = work_delivery
    items, clock, anchor, callback, identity, _options = await pending(e, True)
    items.callbacks.clock = clock.now
    clock.advance(timedelta(days=items.callbacks.config.callbacks.max_age_days + 1).total_seconds())
    async with e.db.sessions() as reader:
        pinned = await _pinned_deliveries(reader, clock.now(), 30)
        assert {identity, anchor} <= pinned
        parent = (await reader.get(TelegramOutboxRow, anchor)).payload["summary_view_basis"]["parent_id"]
        assert parent in pinned
    await items.callbacks.sweep(now=clock.now())
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramCallbackRow, callback) is not None
    with pytest.raises(ProductSourceRetired):
        await items.navigation.read(e.link, callback_id=callback, query_id="expired")


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["locale", "revision"])
async def test_summary_preview_refresh_preserves_full_source_and_original_gesture(work_delivery, change):
    from copy import deepcopy

    from app.telegram_bots.producer import DeliveryClaimRetired
    from app.telegram_bots.work_navigation_recovery import WorkNavigationRecovery
    from deerflow.persistence.agent_instances.work import AgentWorkRow
    from deerflow.persistence.telegram_bots.model import TelegramPersonSettingsRow

    e = work_delivery
    items, clock, anchor, callback, identity, _options = await pending(e, True)
    e.channel._services.work_request_sources.recovery = WorkNavigationRecovery(e.plans, navigation=items.navigation)
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, identity)
        origin = deepcopy(row.payload["work_request_basis"]["navigation_origin"])
        old_part = deepcopy(row.parts[-1])
        if change == "locale":
            (await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)).locale = "zh-CN"
        else:
            (await writer.get(AgentWorkRow, row.work_id)).revision += 1
    clock.advance(2)
    before = len(e.channel.bot.request.calls)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert len(e.channel.bot.request.calls) == before
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "pending" and row.payload["presentation_version"] == 1
        basis = row.payload["work_request_basis"]
        assert basis["navigation_origin"] == origin and basis["concealed"] is False
        assert row.class_ == "reply" and row.parts[0]["part_id"] == old_part["part_id"]
        assert row.parts[0]["state"] == "retired" and row.parts[-1]["protect_content"] is True
        assert (await reader.get(TelegramCallbackRow, callback)).consumed_at is None
        assert (await reader.get(TelegramOutboxRow, anchor)).state == "sent"
    message(e.channel.bot.request, identity=6201)
    clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, identity)).state == "sent"
    assert len(e.dispatch_errors) == 1 and isinstance(e.dispatch_errors[0], DeliveryClaimRetired)
