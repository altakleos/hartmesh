"""Reusable digest buttons supply explicit current preview origins."""

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_digest_navigation import acknowledged
from test_telegram_digest_views import concealed
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow


async def navigation(e, protected):
    if not protected:
        plans, identity, _members, callback, target = await acknowledged(e)
        return plans, identity, callback, target
    from app.telegram_bots.work_digest_views import DigestViews, DigestViewSources

    plans, _identity, _members, show, guard = await concealed(e)
    views = DigestViews(plans)
    e.channel._services.work_digest_view_sources = DigestViewSources(views)
    identity = await views.reveal(e.link, show, chat_id=e.link.chat_id, message_id=5801, message_thread_id=None, guard=guard)
    e.channel.bot.request.enqueue("editMessageText", result={"message_id": 5801, "date": 1893456000, "chat": {"id": e.link.chat_id, "type": "private"}, "text": "Full digest"})
    e.delivery_clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        parent = await reader.get(TelegramOutboxRow, identity)
        assert parent.state == "sent"
        callback = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == parent.parts[-1]["callback_group_id"]).order_by(TelegramCallbackRow.id))
        return plans, identity, callback.id, callback.target_id


@pytest.mark.asyncio
@pytest.mark.parametrize("protected", [False, True])
async def test_navigation_origin_is_current_nonconsuming_and_per_query(work_delivery, protected):
    from app.telegram_bots.work_navigation_origin import WorkNavigation

    e = work_delivery
    plans, identity, callback, target = await navigation(e, protected)
    owner = WorkNavigation(plans)
    first = await owner.read(e.link, callback_id=callback, query_id="gesture-one")
    repeat = await owner.read(e.link, callback_id=callback, query_id="gesture-one")
    later = await owner.read(e.link, callback_id=callback, query_id="gesture-two")
    assert first.value == repeat.value and first.value["query_key"] != later.value["query_key"]
    assert first.member.item.id == target and first.member.source.request["question"] == "Which source?"
    assert (await owner.restore(e.link, first.value)).value == first.value
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramCallbackRow, callback)).consumed_at is None
        assert (await reader.get(TelegramOutboxRow, identity)).state == "sent"


@pytest.mark.asyncio
@pytest.mark.parametrize("protected", [False, True])
@pytest.mark.parametrize("change", ["receipt", "callback", "member", "claim"])
async def test_navigation_origin_final_provider_refuses_changed_anchor(work_delivery, monkeypatch, protected, change):
    from app.gateway import authz
    from app.telegram_bots.producer import ProductSourceRetired
    from app.telegram_bots.work_navigation_origin import WorkNavigation

    e = work_delivery
    plans, identity, callback, target = await navigation(e, protected)
    snapshot = await WorkNavigation(plans).read(e.link, callback_id=callback, query_id="gesture")
    original = authz.resolve_route_permissions

    async def late(*args, **kwargs):
        held = await original(*args, **kwargs)
        async with e.db.sessions.begin() as writer:
            parent = await writer.get(TelegramOutboxRow, identity)
            if change == "receipt":
                parent.parts = [*parent.parts[:-1], {**parent.parts[-1], "message_ids": [5802]}]
            elif change == "callback":
                (await writer.get(TelegramCallbackRow, callback)).target_id = "f" * 32
            elif change == "member":
                child = await writer.get(TelegramOutboxRow, target)
                child.payload = {**child.payload, "digest_id": "f" * 32}
            else:
                parent.claim_id = "foreign-claim"
        return held

    monkeypatch.setattr(authz, "resolve_route_permissions", late)
    with pytest.raises(ProductSourceRetired):
        await snapshot.authorize()


@pytest.mark.asyncio
async def test_protected_navigation_final_digest_proof_is_type_strict(work_delivery, monkeypatch):
    from sqlalchemy.orm.attributes import flag_modified

    from app.gateway import authz
    from app.telegram_bots.producer import ProductSourceRetired
    from app.telegram_bots.work_navigation_origin import WorkNavigation

    e = work_delivery
    plans, _identity, callback, _target = await navigation(e, True)
    snapshot = await WorkNavigation(plans).read(e.link, callback_id=callback, query_id="gesture")
    original = authz.resolve_route_permissions

    async def late(*args, **kwargs):
        held = await original(*args, **kwargs)
        async with e.db.sessions.begin() as writer:
            row = await writer.get(TelegramOutboxRow, snapshot.value["digest_id"])
            row.payload = {**row.payload, "digest_version": True}
            flag_modified(row, "payload")
        return held

    monkeypatch.setattr(authz, "resolve_route_permissions", late)
    with pytest.raises(ProductSourceRetired):
        await snapshot.authorize()


@pytest.mark.asyncio
@pytest.mark.parametrize("protected", [False, True])
async def test_navigation_final_member_proof_is_type_strict(work_delivery, monkeypatch, protected):
    from sqlalchemy.orm.attributes import flag_modified

    from app.gateway import authz
    from app.telegram_bots.producer import ProductSourceRetired
    from app.telegram_bots.work_navigation_origin import WorkNavigation

    e = work_delivery
    plans, _identity, callback, target = await navigation(e, protected)
    snapshot = await WorkNavigation(plans).read(e.link, callback_id=callback, query_id="gesture")
    original = authz.resolve_route_permissions

    async def late(*args, **kwargs):
        held = await original(*args, **kwargs)
        async with e.db.sessions.begin() as writer:
            row = await writer.get(TelegramOutboxRow, target)
            row.payload = {**row.payload, "work_event_source": {**row.payload["work_event_source"], "version": True}}
            flag_modified(row, "payload")
        return held

    monkeypatch.setattr(authz, "resolve_route_permissions", late)
    with pytest.raises(ProductSourceRetired):
        await snapshot.authorize()
