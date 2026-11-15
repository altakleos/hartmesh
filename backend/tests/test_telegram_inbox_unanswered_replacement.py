"""Sealed reply replacement retains actual inputs and transport uncertainty."""

from copy import deepcopy
from uuid import uuid4

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_inbox import routed, setup, writer

from app.telegram_bots.producer import Outbox
from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow, TelegramOutboxRow


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["clean", "claim", "partial", "ambiguous", "historical", "reverse", "turn", "topic", "publication", "replacement-claimed", "dispatch-conflict", "ack-null", "dispatch-empty"])
async def test_sealed_reply_replacement_preserves_delivery_custody(db, change):
    repo, link = await setup(db)
    incoming = await routed(repo, link)
    outbox = Outbox(db.sessions, inbox=repo)
    recipe = {"kind": "text", "source_ref": {"key": "tap.failed", "values": {}}}
    previous = await outbox.plan_product(link, source_id=uuid4().hex, parts=[recipe], inbox_rows=[incoming], finish_route=True)
    replacement = await outbox.plan_product(link, source_id=uuid4().hex, parts=[recipe])
    if change == "reverse":
        other = await routed(repo, link, message=103, update_id=203)
        async with outbox._writer(link.id) as (session, _binding):
            await repo.register_delivery([other], outbox_id=previous, finish=True, session=session)
    async with db.sessions.begin() as session:
        old = await session.get(TelegramOutboxRow, previous)
        mark = await session.get(TelegramInboundMarkRow, (incoming.link_id, incoming.message_id, incoming.chat_id))
        # Negative corruption supplies adverse history only. Actual positive
        # owners and plans above come from public admission/planning APIs.
        if change == "claim":
            old.state, old.claim_owner, old.claim_id = "sending", "a" * 32, "b" * 32
        elif change in {"partial", "ambiguous", "historical"}:
            parts = deepcopy(old.parts)
            if change == "partial":
                parts[0].update(state="sent", message_ids=[902], sent_at=old.created_at.isoformat())
            elif change == "ambiguous":
                parts[0]["dispatched_at"] = old.created_at.isoformat()
            else:
                parts.append({**parts[0], "part_id": uuid4().hex, "presentation_version": 1})
                parts[0].update(state="sent", message_ids=[902])
                old.payload = {**old.payload, "presentation_version": 1}
            old.parts = parts
        elif change == "turn":
            mark.turn_id = "a" * 32
        elif change == "topic":
            (await session.get(TelegramOutboxRow, replacement)).message_thread_id = 99
        elif change == "publication":
            mark.route_basis = {**mark.route_basis, "publications": []}
        elif change == "replacement-claimed":
            new = await session.get(TelegramOutboxRow, replacement)
            new.state, new.claim_owner = "sending", "a" * 32
        elif change == "dispatch-conflict":
            old.payload = {**old.payload, "work_dispatch_conflict": {"message_ids": [902]}}
        elif change in {"ack-null", "dispatch-empty"}:
            part = {**old.parts[0]}
            part.update({"message_ids": None} if change == "ack-null" else {"dispatched_at": ""})
            old.parts = [part]
        captured = deepcopy((old.payload, old.parts, mark.operation_ids, mark.route_basis))
    session = await writer(db)
    try:
        if change == "clean":
            await repo.replace_unanswered_delivery([incoming], previous_id=previous, outbox_id=replacement, session=session)
            await session.commit()
        else:
            with pytest.raises(ValueError, match="inbox-delivery-conflict"):
                await repo.replace_unanswered_delivery([incoming], previous_id=previous, outbox_id=replacement, session=session)
            await session.rollback()
    finally:
        await session.close()
    async with db.sessions() as session:
        old = await session.get(TelegramOutboxRow, previous)
        mark = await session.get(TelegramInboundMarkRow, (incoming.link_id, incoming.message_id, incoming.chat_id))
        assert (old.payload, old.parts, mark.operation_ids, mark.route_basis) == captured
        assert mark.route_complete and mark.state == "reply_pending" and mark.closed_at is None
        assert mark.delivery_ids == ([previous, replacement] if change == "clean" else [previous])
        if change == "clean":
            assert old.state == "skipped" and old.skip_reason == "unanswered-replacement"
            # Normal replays retain the same replacement and cannot append a
            # third plan to the sealed route.
    if change == "clean":
        async with outbox._writer(link.id) as (session, _binding):
            await repo.register_delivery([incoming], outbox_id=replacement, session=session)
        third = await outbox.plan_product(link, source_id=uuid4().hex, parts=[recipe])
        async with outbox._writer(link.id) as (session, _binding):
            with pytest.raises(ValueError, match="inbox-route-complete"):
                await repo.register_delivery([incoming], outbox_id=third, session=session)
