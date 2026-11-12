"""Digest previews retain each family's canonical text, files and controls."""

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_bot_adapter import message
from test_telegram_work_file_exports import work_exports as work_exports
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow


async def one_member_digest(e, identity, **readers):
    from _telegram_bot_test_support import ManualClock

    from app.telegram_bots.digests import DigestReservations, category_for
    from app.telegram_bots.notifications import aware
    from app.telegram_bots.work_digest_delivery import DigestPlans, DigestSources
    from app.telegram_bots.work_digest_members import DigestMembers

    # The operator-configurable threshold isolates each family here; ordinary
    # navigation tests exercise the default three-member batch separately.
    e.sender.outbox.config.outbox.digest_threshold = 1
    if not hasattr(e, "delivery_clock"):
        e.delivery_clock = ManualClock(e.sender.outbox.clock())
        e.sender.outbox.clock = e.delivery_clock.now
    async with e.db.sessions() as reader:
        member = await reader.get(TelegramOutboxRow, identity)
        category, due = category_for(member.class_), aware(member.not_before)
    e.delivery_clock.advance(max(1, (due - e.delivery_clock.now()).total_seconds() + 1))
    batch = await DigestReservations(e.sender.outbox).reserve(e.link, category=category)
    assert batch is not None
    digests = DigestPlans(outbox=e.sender.outbox, callbacks=e.plans.callbacks, members=DigestMembers(requests=e.reader, **readers))
    e.channel._services.work_digest_sources = DigestSources(digests)
    e.channel._now = lambda: e.sender.outbox.clock()
    assert await digests.materialize(e.link, batch)
    message(e.channel.bot.request, identity=5801)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, batch)
        assert row.state == "sent", e.dispatch_errors
        callback = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"]))
        callback_id, kind = callback.id, callback.kind
    if kind == "reveal":
        from app.telegram_bots.work_digest_views import DigestViews, DigestViewSources

        views = DigestViews(digests)
        e.channel._services.work_digest_view_sources = DigestViewSources(views)
        owner = await e.live.authority.prepare_live_owner(e.link.owner_user_id)
        guard = e.db.links.publication_guard(e.link, check_live_owner=owner, check_attempt_current=lambda: True)
        view_id = await views.reveal(e.link, callback_id, chat_id=e.link.chat_id, message_id=5801, message_thread_id=None, guard=guard)
        e.channel.bot.request.enqueue("editMessageText", result={"message_id": 5801, "date": 1893456000, "chat": {"id": e.link.chat_id, "type": "private"}, "text": "Full digest"})
        e.delivery_clock.advance(2)
        assert await e.sender.tick() == 1
        await e.sender.drain()
        async with e.db.sessions() as reader:
            row = await reader.get(TelegramOutboxRow, view_id)
            assert row.state == "sent", e.dispatch_errors
            callback = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"]))
            callback_id, kind = callback.id, callback.kind
    assert kind == "digest_item"
    return digests, batch, callback_id


async def protect(e):
    from datetime import UTC, datetime

    from deerflow.persistence.telegram_bots.model import TelegramInstancePolicyRow

    async with e.db.sessions.begin() as writer:
        writer.add(TelegramInstancePolicyRow(instance_id=e.link.agent_ref, policy="protected", updated_by=e.actor.subject_id, updated_at=datetime.now(UTC)))


async def open_item(e, items, callback):
    owner = await e.live.authority.prepare_live_owner(e.link.owner_user_id)
    guard = e.db.links.publication_guard(e.link, check_live_owner=owner, check_attempt_current=lambda: True)
    return await items.open(e.link, callback_id=callback, query_id="family-item", chat_id=e.link.chat_id, message_id=5801, message_thread_id=None, guard=guard)


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["review_event"], indirect=True)
@pytest.mark.parametrize("protected", [False, True])
async def test_digest_review_preview_delivers_exact_files_before_review_controls(work_exports, protected):
    from test_telegram_work_review_event_delivery import queued

    from app.telegram_bots.work_digest_items import DigestItems
    from app.telegram_bots.work_result_source import WorkResultSources
    from app.telegram_bots.work_review_event_delivery import WorkReviewEventDelivery

    e = work_exports
    if protected:
        await protect(e)
    review, member = await queued(e)
    assert await WorkReviewEventDelivery(review).materialize(e.link, member)
    digests, _batch, callback = await one_member_digest(e, member)
    items = DigestItems(digests=digests, requests=e.plans, reviews=review)
    e.channel._services.work_result_sources = WorkResultSources(reader=e.reader, outbox=e.sender.outbox, callbacks=e.plans.callbacks, navigation=items.navigation)
    identity = await open_item(e, items, callback)
    assert not e.native_reads
    message(e.channel.bot.request, "sendDocument", identity=6301)
    message(e.channel.bot.request, identity=6302)
    for _ in range(2):
        e.delivery_clock.advance(2)
        assert await e.sender.tick() == 1
        await e.sender.drain()
    assert not e.dispatch_errors
    assert e.native_reads == ["outputs/result.txt"]
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        original = await reader.get(TelegramOutboxRow, member)
        assert row.state == "sent" and row.class_ == "reply"
        assert row.payload["work_result_identity"] == original.payload["work_result_identity"]
        assert row.payload["work_result_navigation"] == original.payload["work_result_navigation"]
        controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"])))
        assert {row.kind for row in controls} == {"accept_open", "changes_open"}
        assert all(row.message_id == 6302 for row in controls)
        assert (await reader.get(TelegramCallbackRow, callback)).consumed_at is None


@pytest.mark.asyncio
@pytest.mark.parametrize("protected", [False, True])
async def test_digest_routing_preview_retains_current_recipient_choices(work_delivery, monkeypatch, protected):
    from test_telegram_work_routing_event_delivery import routing_event

    from app.telegram_bots.work_digest_items import DigestItems
    from app.telegram_bots.work_routing_delivery import WorkRoutingSources
    from app.telegram_bots.work_routing_event_delivery import WorkRoutingEventDelivery

    e = work_delivery
    if protected:
        await protect(e)
    routing, member, _request, _peer = await routing_event(e, monkeypatch)
    assert await WorkRoutingEventDelivery(routing).materialize(e.link, member)
    digests, _batch, callback = await one_member_digest(e, member, routing=routing.reader)
    items = DigestItems(digests=digests, requests=e.plans, routing=routing)
    e.channel._services.work_routing_sources = WorkRoutingSources(reader=routing.reader, outbox=e.sender.outbox, callbacks=e.plans.callbacks, navigation=items.navigation)
    identity = await open_item(e, items, callback)
    message(e.channel.bot.request, identity=6401)
    e.delivery_clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "sent" and row.class_ == "reply", e.dispatch_errors
        assert row.payload["work_request_basis"]["version"] == 5
        controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"])))
        assert controls and all(row.kind == "route_to" and row.message_id == 6401 for row in controls)
        assert (await reader.get(TelegramCallbackRow, callback)).consumed_at is None


@pytest.mark.asyncio
@pytest.mark.parametrize("protected", [False, True])
async def test_digest_news_preview_opens_complete_current_notice(work_delivery, protected):
    from test_telegram_work_news_delivery import prepare

    from app.telegram_bots.work_digest_items import DigestItems
    from app.telegram_bots.work_news_delivery import WorkNewsSources

    e = work_delivery
    if protected:
        await protect(e)
    news, member = await prepare(e)
    digests, _batch, callback = await one_member_digest(e, member, news=news.reader)
    items = DigestItems(digests=digests, requests=e.plans, news=news)
    e.channel._services.work_news_sources = WorkNewsSources(news, navigation=items.navigation)
    identity = await open_item(e, items, callback)
    message(e.channel.bot.request, identity=6501)
    e.delivery_clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "sent" and row.class_ == "reply", e.dispatch_errors
        assert "event-peer@example.com changed an assignment" in row.parts[-1]["visible_text"]
        assert (await reader.get(TelegramCallbackRow, callback)).consumed_at is None


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["done_event"], indirect=True)
@pytest.mark.parametrize("protected", [False, True])
async def test_digest_result_preview_delivers_exact_result_files(work_exports, protected):
    from test_telegram_result_notice_delivery import prepare

    from app.telegram_bots.result_notice_delivery import ResultNoticeSources
    from app.telegram_bots.work_digest_items import DigestItems

    e = work_exports
    if protected:
        await protect(e)
    results, member = await prepare(e)
    digests, _batch, callback = await one_member_digest(e, member, results=results.reader)
    items = DigestItems(digests=digests, requests=e.plans, results=results)
    e.channel._services.result_notice_sources = ResultNoticeSources(results, navigation=items.navigation)
    identity = await open_item(e, items, callback)
    assert not e.native_reads
    message(e.channel.bot.request, "sendDocument", identity=6601)
    message(e.channel.bot.request, identity=6602)
    for _ in range(2):
        e.delivery_clock.advance(2)
        assert await e.sender.tick() == 1
        await e.sender.drain()
    assert not e.dispatch_errors
    assert e.native_reads == ["outputs/result.txt"]
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "sent" and row.class_ == "reply"
        assert "The findings are verified." in row.parts[-1]["visible_text"]
        assert (await reader.get(TelegramCallbackRow, callback)).consumed_at is None


async def pending_notice(e, family):
    from app.telegram_bots.work_digest_items import DigestItems

    if family == "news":
        from test_telegram_work_news_delivery import prepare

        from app.telegram_bots.work_news_delivery import WorkNewsSources

        plans, member = await prepare(e)
        digests, batch, callback = await one_member_digest(e, member, news=plans.reader)
        items = DigestItems(digests=digests, requests=e.plans, news=plans)
        e.channel._services.work_news_sources = WorkNewsSources(plans, navigation=items.navigation)
    else:
        from test_telegram_result_notice_delivery import prepare

        from app.telegram_bots.result_notice_delivery import ResultNoticeSources

        plans, member = await prepare(e)
        digests, batch, callback = await one_member_digest(e, member, results=plans.reader)
        items = DigestItems(digests=digests, requests=e.plans, results=plans)
        e.channel._services.result_notice_sources = ResultNoticeSources(plans, navigation=items.navigation)
    identity = await open_item(e, items, callback)
    return items, identity, batch, callback, member


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["done_event"], indirect=True)
@pytest.mark.parametrize("family", ["news", "results"])
async def test_pending_notice_preview_retains_expired_navigation_evidence(work_exports, family):
    from app.telegram_bots.callbacks import CallbackRefused
    from app.telegram_bots.retention import _pinned_deliveries

    e = work_exports
    _items, identity, batch, callback, member = await pending_notice(e, family)
    age = e.plans.callbacks.config.callbacks.max_age_days
    e.delivery_clock.advance((age + 1) * 86400)
    e.plans.callbacks.clock = e.delivery_clock.now
    async with e.db.sessions() as reader:
        assert {identity, batch, member} <= await _pinned_deliveries(reader, e.delivery_clock.now(), age)
    await e.plans.callbacks.sweep(e.delivery_clock.now())
    async with e.db.sessions() as reader:
        control = await reader.get(TelegramCallbackRow, callback)
        assert control is not None
        with pytest.raises(CallbackRefused):
            e.plans.callbacks._usable(control)


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["review_event", "done_event"], indirect=True)
@pytest.mark.parametrize("change", ["callback", "member"])
async def test_preview_file_read_rechecks_navigation_in_final_provider_snapshot(work_exports, monkeypatch, change):
    from app.gateway import authz
    from app.telegram_bots import work_file_exports as module
    from app.telegram_bots.work_digest_projection import DigestProjection

    e = work_exports
    if e.result["status"] == "submitted":
        from test_telegram_work_review_event_delivery import queued

        from app.telegram_bots.work_digest_items import DigestItems
        from app.telegram_bots.work_result_source import WorkResultSources
        from app.telegram_bots.work_review_event_delivery import WorkReviewEventDelivery

        review, member = await queued(e)
        assert await WorkReviewEventDelivery(review).materialize(e.link, member)
        digests, _batch, callback = await one_member_digest(e, member)
        items = DigestItems(digests=digests, requests=e.plans, reviews=review)
        e.channel._services.work_result_sources = WorkResultSources(reader=e.reader, outbox=e.sender.outbox, callbacks=e.plans.callbacks, navigation=items.navigation)
        await open_item(e, items, callback)
    else:
        _items, _identity, _batch, callback, member = await pending_notice(e, "results")
    original_prepare, original_provider = module.prepare_attention, authz.resolve_route_permissions
    active, changed = [], []

    async def prepare(*args, **kwargs):
        admission = await original_prepare(*args, **kwargs)

        async def check(**options):
            projection = options.get("projection")
            if isinstance(projection, module._WorkFileProjection) and isinstance(projection.delivery, DigestProjection):
                active.append(options["session"])
            try:
                return await admission(**options)
            finally:
                active.clear()

        return check

    async def provider(*args, **kwargs):
        held = await original_provider(*args, **kwargs)
        if active and not changed:
            writer = active[-1]
            if change == "callback":
                (await writer.get(TelegramCallbackRow, callback)).target_id = "f" * 32
            else:
                (await writer.get(TelegramOutboxRow, member)).skip_reason = "superseded"
            await writer.flush()
            changed.append(True)
        return held

    monkeypatch.setattr(module, "prepare_attention", prepare)
    monkeypatch.setattr(authz, "resolve_route_permissions", provider)
    message(e.channel.bot.request, "sendDocument", identity=6701)
    e.delivery_clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert changed and not e.native_reads
    assert not [call for call in e.channel.bot.request.calls if call.endpoint == "sendDocument"]


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["done_event"], indirect=True)
async def test_result_preview_retry_preserves_acknowledged_media(work_exports):
    e = work_exports
    items, identity, _batch, callback, _member = await pending_notice(e, "results")
    message(e.channel.bot.request, "sendDocument", identity=6801)
    e.channel.bot.request.enqueue("sendMessage", status=503, body={"ok": False, "error_code": 503, "description": "Service unavailable"})
    for _ in range(2):
        e.delivery_clock.advance(2)
        assert await e.sender.tick() == 1
        await e.sender.drain()
    assert await open_item(e, items, callback) == identity
    assert e.native_reads == ["outputs/result.txt"]
    message(e.channel.bot.request, identity=6802)
    e.delivery_clock.advance(60)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert len([call for call in e.channel.bot.request.calls if call.endpoint == "sendDocument"]) == 1
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, identity)).state == "sent"


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["done_event"], indirect=True)
@pytest.mark.parametrize("family", ["news", "results"])
async def test_ordinary_notice_repair_cannot_discard_navigation_origin(work_exports, family):
    from app.telegram_bots.producer import ProductSourceRetired

    e = work_exports
    items, identity, _batch, _callback, _member = await pending_notice(e, family)
    if family == "news":
        from app.telegram_bots.work_news_recovery import WorkNewsRecovery

        recovery, reason = WorkNewsRecovery(items.news), "work-news-refresh-required"
    else:
        from app.telegram_bots.result_notice_recovery import ResultNoticeRecovery

        recovery, reason = ResultNoticeRecovery(items.results), "work-result-refresh-required"
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, identity)
        row.state, row.failed_reason = "failed", reason
    owner = await e.live.authority.prepare_live_owner(e.link.owner_user_id)
    guard = e.db.links.publication_guard(e.link, check_live_owner=owner, check_attempt_current=lambda: True)
    with pytest.raises(ProductSourceRetired):
        await recovery.repair(e.link, identity, guard=guard)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "failed" and row.payload["presentation_version"] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["review_event"], indirect=True)
async def test_review_file_member_cannot_become_plain_text_preview(work_exports):
    from test_telegram_work_review_event_delivery import queued

    from app.telegram_bots.work_digest_items import DigestItems
    from app.telegram_bots.work_request_source import WorkRequestSources
    from app.telegram_bots.work_review_event_delivery import WorkReviewEventDelivery

    e = work_exports
    review, member = await queued(e)
    await WorkReviewEventDelivery(review).materialize(e.link, member)
    digests, _batch, callback = await one_member_digest(e, member)
    items = DigestItems(digests=digests, requests=e.plans, reviews=review)
    identity = await open_item(e, items, callback)
    origin = await items.navigation.read(e.link, callback_id=callback, query_id="family-item")
    settings = await digests.settings.get(e.link.owner_user_id)
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, identity)
        await e.plans._freeze(writer, e.link, row, origin.member.source, settings, pending=True, navigation_origin=origin.value)
        assert "result_files" not in row.payload["work_request_basis"]
    e.channel._services.work_request_sources = WorkRequestSources(reader=e.reader, outbox=e.sender.outbox, callbacks=e.plans.callbacks, navigation=items.navigation)
    before = len([call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"])
    message(e.channel.bot.request, identity=6901)
    e.delivery_clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    calls = [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
    assert len(calls) == before, "review controls delivered without the original result file family"


@pytest.mark.asyncio
async def test_routing_member_cannot_drop_original_response(work_delivery, monkeypatch):
    from test_telegram_work_routing_event_delivery import routing_event

    from app.telegram_bots.work_digest_items import DigestItems
    from app.telegram_bots.work_routing_delivery import WorkRoutingSources
    from app.telegram_bots.work_routing_event_delivery import WorkRoutingEventDelivery
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow

    e = work_delivery
    routing, member, req, peer = await routing_event(e, monkeypatch, "cannot_provide")
    async with e.db.sessions.begin() as writer:
        (await writer.get(AgentInstanceGrantRow, (e.instance.id, peer.subject_id))).permissions = 2
    assert await WorkRoutingEventDelivery(routing).materialize(e.link, member)
    digests, _batch, callback = await one_member_digest(e, member, routing=routing.reader)
    items = DigestItems(digests=digests, requests=e.plans, routing=routing)
    identity = await open_item(e, items, callback)
    origin = await items.navigation.read(e.link, callback_id=callback, query_id="family-item")
    settings = await digests.settings.get(e.link.owner_user_id)
    value = await routing.reader.read_routing(e.link, req["id"])
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, identity)
        assert row.payload["work_request_basis"]["routing"]["response_id"]
        await routing.freeze_routing(writer, e.link, row, value, settings, origin.member.source.public_base_url, navigation_origin=origin.value)
        assert "response_id" not in row.payload["work_request_basis"]["routing"]
    e.channel._services.work_routing_sources = WorkRoutingSources(reader=routing.reader, outbox=e.sender.outbox, callbacks=e.plans.callbacks, navigation=items.navigation)
    before = len([call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"])
    message(e.channel.bot.request, identity=6971)
    e.delivery_clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    calls = [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
    assert len(calls) == before, "routing preview dropped its original canonical refusal explanation: " + calls[-1].parameters["text"]
