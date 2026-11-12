"""Acknowledged digest navigation retains its receipt and current source."""

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_bot_adapter import message
from test_telegram_digest_delivery import reserved
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.producer import ProductSourceRetired
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow


async def acknowledged(e):
    plans, identity, members = await reserved(e)
    await plans.materialize(e.link, identity)
    message(e.channel.bot.request, identity=5801)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not e.dispatch_errors
    async with e.db.sessions() as reader:
        parent = await reader.get(TelegramOutboxRow, identity)
        callback = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == parent.parts[0]["callback_group_id"]).order_by(TelegramCallbackRow.id))
        return plans, identity, members, callback.id, callback.target_id


@pytest.mark.asyncio
@pytest.mark.parametrize("protected", [False, True])
async def test_digest_navigation_reads_full_current_member_without_changing_receipt(work_delivery, protected):
    from datetime import UTC, datetime

    from app.telegram_bots.work_digest_navigation import DigestNavigation
    from deerflow.persistence.telegram_bots.model import TelegramInstancePolicyRow

    e = work_delivery
    if protected:
        async with e.db.sessions.begin() as writer:
            writer.add(TelegramInstancePolicyRow(instance_id=e.link.agent_ref, policy="protected", updated_by=e.actor.subject_id, updated_at=datetime.now(UTC)))
    plans, identity, members, callback, target = await acknowledged(e)
    snapshot = await DigestNavigation(plans).read(e.link, identity, callback_id=callback, member_id=None if protected else target)
    assert len(snapshot.members) == (3 if protected else 1)
    assert snapshot.members[0].source.request["question"] == "Which source?"
    await snapshot.authorize()
    async with e.db.sessions() as reader:
        parent = await reader.get(TelegramOutboxRow, identity)
        assert parent.state == "sent" and parent.message_id == 5801
        assert (await reader.get(TelegramCallbackRow, callback)).consumed_at is None
        assert all([(await reader.get(TelegramOutboxRow, child)).state == "skipped" for child in members])


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["receipt", "parent_claim", "member_edge", "member_state", "callback", "source"])
async def test_digest_navigation_final_read_rejects_changed_parent_or_member(work_delivery, monkeypatch, change):
    from app.gateway import authz
    from app.telegram_bots.work_digest_navigation import DigestNavigation
    from deerflow.persistence.agent_instances.human_input import HumanInputRequestRow

    e = work_delivery
    plans, identity, members, callback, target = await acknowledged(e)
    snapshot = await DigestNavigation(plans).read(e.link, identity, callback_id=callback, member_id=target)
    original = authz.resolve_route_permissions

    async def late(*args, **kwargs):
        held = await original(*args, **kwargs)
        async with e.db.sessions.begin() as writer:
            parent = await writer.get(TelegramOutboxRow, identity)
            child = await writer.get(TelegramOutboxRow, target)
            if change == "receipt":
                parent.parts = [{**parent.parts[0], "message_ids": [5802]}]
            elif change == "parent_claim":
                parent.claim_id = "foreign-claim"
            elif change == "member_edge":
                child.payload = {**child.payload, "digest_id": "f" * 32}
            elif change == "member_state":
                child.skip_reason = "superseded"
            elif change == "callback":
                (await writer.get(TelegramCallbackRow, callback)).target_id = members[-1] if target != members[-1] else members[0]
            else:
                request = await writer.get(HumanInputRequestRow, child.request_id)
                request.state, request.closed_reason = "closed", "resolved"
        return held

    monkeypatch.setattr(authz, "resolve_route_permissions", late)
    with pytest.raises(ProductSourceRetired):
        await snapshot.authorize()
