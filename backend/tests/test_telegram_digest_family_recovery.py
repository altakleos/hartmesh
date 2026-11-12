"""Family preview refreshes retain their gesture and original delivery evidence."""

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_bot_adapter import message
from test_telegram_digest_item_families import one_member_digest, open_item, pending_notice
from test_telegram_work_file_exports import work_exports as work_exports
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.telegram_bots.model import TelegramOutboxRow, TelegramPersonSettingsRow


async def pending_family(e, family, monkeypatch):
    from app.telegram_bots.work_digest_items import DigestItems

    if family in {"news", "results"}:
        return await pending_notice(e, family)
    if family == "review":
        from test_telegram_work_review_event_delivery import queued

        from app.telegram_bots.work_navigation_recovery import WorkReviewNavigationRecovery
        from app.telegram_bots.work_result_source import WorkResultSources
        from app.telegram_bots.work_review_event_delivery import WorkReviewEventDelivery

        plans, member = await queued(e)
        assert await WorkReviewEventDelivery(plans).materialize(e.link, member)
        digests, batch, callback = await one_member_digest(e, member)
        items = DigestItems(digests=digests, requests=e.plans, reviews=plans)
        recovery = WorkReviewNavigationRecovery(plans, navigation=items.navigation)
        e.channel._services.work_result_sources = WorkResultSources(reader=e.reader, outbox=e.sender.outbox, callbacks=e.plans.callbacks, recovery=recovery, navigation=items.navigation)
    else:
        from test_telegram_work_routing_event_delivery import routing_event

        from app.telegram_bots.work_navigation_recovery import WorkRoutingNavigationRecovery
        from app.telegram_bots.work_routing_delivery import WorkRoutingSources
        from app.telegram_bots.work_routing_event_delivery import WorkRoutingEventDelivery

        plans, member, _request, _peer = await routing_event(e, monkeypatch)
        assert await WorkRoutingEventDelivery(plans).materialize(e.link, member)
        digests, batch, callback = await one_member_digest(e, member, routing=plans.reader)
        items = DigestItems(digests=digests, requests=e.plans, routing=plans)
        recovery = WorkRoutingNavigationRecovery(plans, navigation=items.navigation)
        e.channel._services.work_routing_sources = WorkRoutingSources(reader=plans.reader, outbox=e.sender.outbox, callbacks=e.plans.callbacks, recovery=recovery, navigation=items.navigation)
    identity = await open_item(e, items, callback)
    return items, identity, batch, callback, member


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports,family", [("review_event", "review"), ("review_event", "routing"), ("done_event", "news"), ("done_event", "results")], indirect=["work_exports"])
async def test_queued_family_preview_refresh_retains_explicit_navigation(work_exports, monkeypatch, family):
    e = work_exports
    _items, identity, _batch, _callback, _member = await pending_family(e, family, monkeypatch)
    basis_key = "work_news_basis" if family == "news" else "result_notice_basis" if family == "results" else "work_request_basis"
    async with e.db.sessions.begin() as writer:
        origin = (await writer.get(TelegramOutboxRow, identity)).payload[basis_key]["navigation_origin"]
        (await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)).locale = "zh-CN"
    e.delivery_clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        import traceback

        assert row.state == "pending", "\n".join("".join(traceback.format_exception(error)) for error in e.dispatch_errors)
        assert row.payload["presentation_version"] == 1, e.dispatch_errors
        assert row.payload[basis_key]["navigation_origin"] == origin
        assert row.payload[basis_key]["locale"] == "zh-CN" and row.payload[basis_key]["concealed"] is False
        assert row.class_ == "reply"
    if family in {"review", "results"}:
        message(e.channel.bot.request, "sendDocument", identity=6901)
    message(e.channel.bot.request, identity=6902)
    for _ in range(2 if family in {"review", "results"} else 1):
        e.delivery_clock.advance(2)
        assert await e.sender.tick() == 1
        await e.sender.drain()
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, identity)).state == "sent", e.dispatch_errors


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports,family", [("review_event", "review"), ("done_event", "results")], indirect=["work_exports"])
@pytest.mark.parametrize("upgrade", [False, True])
async def test_preview_refresh_preserves_already_acknowledged_media(work_exports, monkeypatch, family, upgrade):
    e = work_exports
    _items, identity, _batch, _callback, _member = await pending_family(e, family, monkeypatch)
    message(e.channel.bot.request, "sendDocument", identity=7001)
    e.delivery_clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert e.native_reads == ["outputs/result.txt"]
    if upgrade:
        from test_telegram_digest_item_families import protect

        await protect(e)
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)).locale = "zh-CN"
    e.delivery_clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "pending" and row.payload["presentation_version"] == 1, e.dispatch_errors
        assert row.payload["work_result_history"]
    message(e.channel.bot.request, identity=7002)
    e.delivery_clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "sent", e.dispatch_errors
        if upgrade:
            assert row.parts[0]["message_ids"] == [7001] and row.parts[0]["protect_content"] is False
            assert row.parts[-1]["message_ids"] == [7002] and row.parts[-1]["protect_content"] is True
    assert len([call for call in e.channel.bot.request.calls if call.endpoint == "sendDocument"]) == 1
    if upgrade:
        cards = [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
        assert cards[-1].parameters["protect_content"] is True
    assert e.native_reads == ["outputs/result.txt"]


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports,family", [("review_event", "review"), ("review_event", "routing"), ("done_event", "news"), ("done_event", "results")], indirect=["work_exports"])
async def test_family_refresh_rechecks_origin_after_replacement(work_exports, monkeypatch, family):
    from app.gateway import authz
    from app.telegram_bots.work_digest_projection import DigestProjection
    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow

    if family in {"news", "results"}:
        from app.telegram_bots import work_notice_navigation_recovery as module
    else:
        from app.telegram_bots import work_navigation_recovery as module
    e = work_exports
    _items, identity, _batch, callback, _member = await pending_family(e, family, monkeypatch)
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)).locale = "zh-CN"
    prepare, resolve = module.prepare_attention, authz.resolve_route_permissions
    active, changed = [], []

    async def prepared(*args, **kwargs):
        admission = await prepare(*args, **kwargs)

        async def read(**options):
            if isinstance(options.get("projection"), DigestProjection):
                active.append(options["session"])
            try:
                return await admission(**options)
            finally:
                active.clear()

        return read

    async def provider(*args, **kwargs):
        held = await resolve(*args, **kwargs)
        if active and not changed:
            writer = active[-1]
            row = await writer.get(TelegramOutboxRow, identity)
            if row.payload["presentation_version"] == 1:
                (await writer.get(TelegramCallbackRow, callback)).target_id = "f" * 32
                await writer.flush()
                changed.append(True)
        return held

    monkeypatch.setattr(module, "prepare_attention", prepared)
    monkeypatch.setattr(authz, "resolve_route_permissions", provider)
    before = len(e.channel.bot.request.calls)
    e.delivery_clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert changed and len(e.channel.bot.request.calls) == before
    assert not e.native_reads
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, identity)).payload["presentation_version"] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports,family", [("review_event", "review"), ("review_event", "routing"), ("done_event", "news"), ("done_event", "results")], indirect=["work_exports"])
async def test_notice_navigation_repairs_only_claim_free_held_delivery(work_exports, monkeypatch, family):
    e = work_exports
    _items, identity, _batch, _callback, _member = await pending_family(e, family, monkeypatch)
    service = {"news": "work_news_sources", "results": "result_notice_sources", "review": "work_result_sources", "routing": "work_routing_sources"}[family]
    owner = getattr(e.channel._services, service)
    reason = {"news": "work-news-refresh-required", "routing": "work-routing-refresh-required"}.get(family, "work-result-refresh-required")
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, identity)
        row.state, row.failed_reason, row.claim_id = "failed", reason, "foreign"
    live = await e.live.authority.prepare_live_owner(e.link.owner_user_id)
    guard = e.db.links.publication_guard(e.link, check_live_owner=live, check_attempt_current=lambda: True)
    assert await owner.recovery.repair(e.link, identity, guard=guard) is None
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramOutboxRow, identity)).claim_id = None
    assert await owner.recovery.repair(e.link, identity, guard=guard) == identity
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "pending" and row.payload["presentation_version"] == 1


@pytest.mark.asyncio
async def test_routing_no_controls_to_fresh_candidates_inherits_digest_age(work_delivery, monkeypatch):
    import traceback
    from uuid import uuid4

    from sqlalchemy import select
    from test_telegram_work_routing_event_delivery import routing_event

    from app.telegram_bots.work_digest_items import DigestItems
    from app.telegram_bots.work_navigation_recovery import WorkRoutingNavigationRecovery
    from app.telegram_bots.work_routing_delivery import WorkRoutingSources
    from app.telegram_bots.work_routing_event_delivery import WorkRoutingEventDelivery
    from deerflow.agent_instances.human_input_contract import Respond
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow

    e = work_delivery
    plans, member, req, peer = await routing_event(e, monkeypatch, "cannot_provide")
    await e.human.respond(actor=e.actor, request_id=req["id"], request=Respond(operation_id=uuid4().hex, expected_request_revision=1, expected_assignment_revision=1, disposition="wrong_recipient", text="Ask another colleague."))
    async with e.db.sessions.begin() as writer:
        (await writer.get(AgentInstanceGrantRow, (e.instance.id, peer.subject_id))).permissions = 2
    assert await WorkRoutingEventDelivery(plans).materialize(e.link, member)
    digests, _batch, callback = await one_member_digest(e, member, routing=plans.reader)
    items = DigestItems(digests=digests, requests=e.plans, routing=plans)
    recovery = WorkRoutingNavigationRecovery(plans, navigation=items.navigation)
    e.channel._services.work_routing_sources = WorkRoutingSources(reader=plans.reader, outbox=e.sender.outbox, callbacks=e.plans.callbacks, recovery=recovery, navigation=items.navigation)
    identity = await open_item(e, items, callback)
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, identity)
        assert row.payload["work_request_basis"]["routing"]["recipients"] == []
        assert not list(await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"])))
        (await writer.get(AgentInstanceGrantRow, (e.instance.id, peer.subject_id))).permissions = 7
    e.delivery_clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "pending" and row.payload["presentation_version"] == 1, "\n".join("".join(traceback.format_exception(error)) for error in e.dispatch_errors)
        controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"])))
        original = await reader.get(TelegramCallbackRow, callback)
        assert controls and all(control.created_at == original.created_at for control in controls)
    message(e.channel.bot.request, identity=7101)
    e.delivery_clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, identity)).state == "sent", e.dispatch_errors
