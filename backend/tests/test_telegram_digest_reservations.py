"""Digest ownership through real SQL; the transport double proves receipts only."""

from datetime import timedelta
from uuid import uuid4

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_notification_policy import prepare
from test_telegram_outbox_workers import Clock, allowed, transport

from app.telegram_bots.producer import Outbox, OutboxSender, PartReceipt
from deerflow.persistence.telegram_bots.model import TelegramOutboxRow


async def setup(db, classes):
    from app.telegram_bots.digests import DigestReservations

    link = await prepare(db)
    clock = Clock()
    outbox = Outbox(db.sessions, clock=clock)
    ids = []
    for kind in classes:
        key = ("summary:" if kind == "summary" else "ev:") + uuid4().hex
        identity, _ = await outbox._add(link, dedupe_key=key, class_=kind, payload={"parts": [{"kind": "text", "source_ref": {"key": "chat.restarted", "values": {}}}]})
        ids.append(identity)
        clock.advance(1)
    return link, clock, outbox, DigestReservations(outbox), ids


@pytest.mark.asyncio
async def test_digest_reservation_is_bounded_ordered_and_keeps_categories_separate(db):
    link, clock, outbox, digests, ids = await setup(db, ["result", "info", "question", "summary", "interrupt", "info", *(["question"] * 9)])
    essential = await digests.reserve(link, category="essential")
    optional = await digests.reserve(link, category="discretionary")
    assert essential != optional
    async with db.sessions() as reader:
        first, second = await reader.get(TelegramOutboxRow, essential), await reader.get(TelegramOutboxRow, optional)
        assert len(first.payload["member_ids"]) == 10
        assert first.payload["member_ids"][0] == ids[2]
        assert first.payload["member_classes"] == ["question"] * 10
        assert second.payload["member_ids"] == [ids[1], ids[3], ids[5]]
        assert all([(await reader.get(TelegramOutboxRow, identity)).state == "pending" for identity in second.payload["member_ids"]])
        assert not first.parts and not second.parts  # Unrendered intents cannot dispatch.
    assert await digests.reserve(link, category="essential") is None
    assert await digests.reserve(link, category="discretionary") is None


@pytest.mark.asyncio
async def test_reserved_members_never_dispatch_before_the_digest_has_a_preview(db):
    link, clock, outbox, digests, ids = await setup(db, ["question"] * 3)
    identity = await digests.reserve(link, category="essential")
    seen = []

    async def send(link, row, part):
        seen.append(row.id)
        return PartReceipt((101,))

    sender = OutboxSender(outbox, run_id="digest", deliver_part=transport(db, send), eligibility=allowed, proactive=True)
    try:
        assert await sender.tick() == 0
        assert not seen
        async with db.sessions() as reader:
            assert all([(await reader.get(TelegramOutboxRow, member)).payload["digest_id"] == identity for member in ids])
    finally:
        await sender.stop()


@pytest.mark.asyncio
async def test_digest_members_share_claim_and_settle_only_with_its_ack(db):
    import asyncio

    link, clock, outbox, digests, ids = await setup(db, ["question"] * 3)
    identity = await digests.reserve(link, category="essential")
    # Deliberately seeded presentation: this case tests SQL/receipt ownership,
    # not source-authorized rendering, which is a separate integration contract.
    async with db.sessions.begin() as writer:
        (await writer.get(TelegramOutboxRow, identity)).parts = outbox._parts([{"kind": "text", "source_ref": {"key": "digest.presentation", "values": {}}}], 0)
    entered, release = asyncio.Event(), asyncio.Event()

    async def send(link, row, part):
        entered.set()
        await release.wait()
        return PartReceipt((101,))

    sender = OutboxSender(outbox, run_id="digest", deliver_part=transport(db, send), eligibility=allowed, proactive=True)
    try:
        assert await sender.tick() == 1
        await entered.wait()
        async with db.sessions() as reader:
            parent = await reader.get(TelegramOutboxRow, identity)
            for member in ids:
                row = await reader.get(TelegramOutboxRow, member)
                assert row.state == "sending" and (row.claim_owner, row.claim_id) == (parent.claim_owner, parent.claim_id)
                assert row.skip_reason is None
        release.set()
        await sender.drain()
        async with db.sessions() as reader:
            assert (await reader.get(TelegramOutboxRow, identity)).state == "sent"
            for member in ids:
                row = await reader.get(TelegramOutboxRow, member)
                assert row.state == "skipped" and row.skip_reason == "in-digest"
    finally:
        release.set()
        await sender.stop()


async def arm(db, outbox, link, identity):
    from app.telegram_bots.callbacks import CallbackRepository, canonical_keyboard

    callbacks = CallbackRepository(db.sessions, clock=outbox.clock)
    async with outbox._writer(link.id) as (writer, binding):
        row = await writer.get(TelegramOutboxRow, identity)
        keyboard = canonical_keyboard([[{"label": "label.show", "kind": "digest_item", "target_kind": "outbox", "target_id": member, "payload": {"outbox_id": member}}] for member in row.payload["member_ids"]])
        row.payload = {**row.payload, "product_keyboard_basis": keyboard}
        parts = outbox._parts([{"kind": "text", "source_ref": {"key": "digest.presentation", "values": {}}, "stored_keyboard": True}], 0)
        row.parts = parts
        group = await callbacks.write_group(link, keyboard, anchor_outbox_id=identity, presentation_version=0, chat_id=link.chat_id, message_id=None, message_thread_id=None, session=writer)
        parts[-1]["callback_group_id"] = group.group_id
        row.parts = parts
        return group


@pytest.mark.asyncio
async def test_ambiguous_digest_retry_and_restart_keep_members_and_controls_fixed(db):
    from app.telegram_bots.producer import TransientDeliveryError

    link, clock, outbox, digests, ids = await setup(db, ["question"] * 3)
    identity = await digests.reserve(link, category="essential")
    group = await arm(db, outbox, link, identity)
    seen = []

    async def send(link, row, part):
        seen.append((row.id, row.payload["member_ids"], part["part_id"], part["callback_group_id"]))
        if len(seen) == 1:
            raise TransientDeliveryError("Unknown send acknowledgement")
        return PartReceipt((101,))

    sender = OutboxSender(outbox, run_id="before", deliver_part=transport(db, send), eligibility=allowed, proactive=True)
    try:
        assert await sender.tick() == 1
        await sender.drain()
        async with db.sessions.begin() as writer:
            for key in [identity, *ids]:
                row = await writer.get(TelegramOutboxRow, key)
                assert row.state == "pending"
                row.state, row.claim_owner, row.claim_id, row.claimed_at = "sending", "crashed", "lost-claim", clock.now
        await sender.stop()
        sender = OutboxSender(outbox, run_id="after", deliver_part=transport(db, send), eligibility=allowed, proactive=True)
        clock.advance(3600)
        assert await sender.recover_prior_claims() == 4
        assert await digests.reserve(link, category="essential") is None
        assert await sender.tick() == 1
        await sender.drain()
        assert seen[0] == seen[1] and seen[1][-1] == group.group_id
        async with db.sessions() as reader:
            assert all([(await reader.get(TelegramOutboxRow, member)).skip_reason == "in-digest" for member in ids])
    finally:
        await sender.stop()


@pytest.mark.asyncio
async def test_terminal_abandonment_retires_digest_controls_before_releasing_members(db):
    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow

    link, clock, outbox, digests, ids = await setup(db, ["question"] * 3)
    identity = await digests.reserve(link, category="essential")
    group = await arm(db, outbox, link, identity)
    await digests.abandon(link, identity)
    async with db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, identity)).state == "skipped"
        for member in ids:
            row = await reader.get(TelegramOutboxRow, member)
            assert row.state == "pending" and "digest_id" not in row.payload
        assert all(row.consumed_at for row in await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == group.group_id)))
    # The same retired batch never silently revives its controls.
    assert await digests.reserve(link, category="essential") is None


@pytest.mark.asyncio
@pytest.mark.parametrize("corrupt", ["category", "hash", "members"])
async def test_bad_digest_basis_is_refused_before_any_claim(db, corrupt):
    link, clock, outbox, digests, ids = await setup(db, ["question"] * 3)
    identity = await digests.reserve(link, category="essential")
    await arm(db, outbox, link, identity)
    async with db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, identity)
        if corrupt == "category":
            row.payload = {**row.payload, "category": "discretionary"}
        elif corrupt == "hash":
            row.dedupe_key = "digest:essential:" + "a" * 64
        else:
            row.payload = {**row.payload, "member_ids": list(reversed(row.payload["member_ids"]))}
    seen = []

    async def send(*args):
        seen.append(True)
        return PartReceipt((101,))

    sender = OutboxSender(outbox, run_id="guard", deliver_part=transport(db, send), eligibility=allowed, proactive=True)
    try:
        assert await sender.tick() == 0
        assert not seen
        async with db.sessions() as reader:
            row = await reader.get(TelegramOutboxRow, identity)
            assert row.state == "skipped" and row.claim_id is None
            assert all(["digest_id" not in (await reader.get(TelegramOutboxRow, member)).payload for member in ids])
    finally:
        await sender.stop()


@pytest.mark.asyncio
async def test_concealed_digest_keeps_its_source_members_until_show_expires(db):
    from app.telegram_bots.callbacks import CallbackRepository, canonical_keyboard
    from app.telegram_bots.retention import _sweep_outbox
    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow

    link, clock, outbox, digests, ids = await setup(db, ["question"] * 3)
    identity = await digests.reserve(link, category="essential")
    # Retention graph fixture represents an acknowledged concealed digest.
    # It intentionally makes no claim about source/SDK materialization.
    callbacks = CallbackRepository(db.sessions, clock=clock)
    async with outbox._writer(link.id) as (writer, _binding):
        parent = await writer.get(TelegramOutboxRow, identity)
        keyboard = canonical_keyboard([[{"label": "label.show", "kind": "reveal", "target_kind": "outbox", "target_id": identity, "payload": {"outbox_id": identity}}]])
        parent.payload = {**parent.payload, "product_keyboard_basis": keyboard}
        parent.parts = outbox._parts([{"kind": "text", "source_ref": {"key": "digest.presentation", "values": {}}, "stored_keyboard": True}], 0)
        group = await callbacks.write_group(link, keyboard, anchor_outbox_id=identity, presentation_version=0, chat_id=link.chat_id, message_id=101, message_thread_id=None, session=writer)
        parent.parts = [{**parent.parts[0], "callback_group_id": group.group_id, "state": "sent", "sent_at": clock.now.isoformat(), "message_ids": [101]}]
        parent.state, parent.sent_at, parent.closed_at = "sent", clock.now, clock.now
        for member in ids:
            row = await writer.get(TelegramOutboxRow, member)
            row.state, row.skip_reason, row.closed_at = "skipped", "in-digest", clock.now
    clock.advance(31 * 86400)
    assert await _sweep_outbox(db.sessions, clock.now, 180) == 0
    async with db.sessions.begin() as writer:
        for control in await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == group.group_id)):
            control.consumed_at = clock.now
    assert await _sweep_outbox(db.sessions, clock.now, 180) == 4


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["missing", "settled", "reservation", "kind_type", "category_type", "priority_type", "capsule_type", "part_type", "multipart"])
async def test_invalid_batch_does_not_block_independent_delivery(db, damage):
    link, clock, outbox, digests, ids = await setup(db, ["question"] * 3)
    identity = await digests.reserve(link, category="essential")
    await arm(db, outbox, link, identity)
    async with db.sessions.begin() as writer:
        parent = await writer.get(TelegramOutboxRow, identity)
        member = await writer.get(TelegramOutboxRow, ids[0])
        if damage == "missing":
            await writer.delete(member)
        elif damage == "settled":
            member.state, member.skip_reason = "skipped", "superseded"
        elif damage == "reservation":
            member.payload = {**member.payload, "digest_id": "another-reservation"}
        elif damage == "kind_type":
            parent.payload = {**parent.payload, "member_classes": [[], "question", "question"]}
        elif damage == "category_type":
            parent.payload = {**parent.payload, "category": {}}
        elif damage == "priority_type":
            parent.payload = {**parent.payload, "contains_questions": {}}
        elif damage == "capsule_type":
            parent.payload = []
        elif damage == "part_type":
            parent.parts = [1]
        else:
            parent.parts = [*parent.parts, {**parent.parts[0], "part_id": uuid4().hex}]
    independent, _ = await outbox._add(link, dedupe_key="ev:" + uuid4().hex, class_="result", payload={"parts": [{"kind": "text", "source_ref": {"key": "chat.restarted", "values": {}}}]})
    seen = []

    async def send(link, row, part):
        seen.append(row.id)
        return PartReceipt((101,))

    sender = OutboxSender(outbox, run_id="invalid", deliver_part=transport(db, send), eligibility=allowed, proactive=True)
    try:
        for _ in range(5):
            await sender.tick()
            await sender.drain()
            clock.advance(2)
        assert identity not in seen and independent in seen
        async with db.sessions() as reader:
            assert (await reader.get(TelegramOutboxRow, identity)).state == "skipped"
            assert "digest_id" not in (await reader.get(TelegramOutboxRow, ids[1])).payload
    finally:
        await sender.stop()


@pytest.mark.asyncio
async def test_digest_preserves_original_member_expiry_and_strictest_silence(db):
    from app.telegram_bots.notifications import preference_skip
    from app.telegram_bots.producer import _item
    from app.telegram_bots.settings import _snapshot
    from deerflow.persistence.telegram_bots.model import TelegramPersonSettingsRow

    link, clock, outbox, digests, ids = await setup(db, ["info", "summary", "info"])
    clock.advance(23 * 3600)
    identity = await digests.reserve(link, category="discretionary")
    clock.advance(3600)
    async with db.sessions() as reader:
        parent = await reader.get(TelegramOutboxRow, identity)
        settings = _snapshot(await reader.get(TelegramPersonSettingsRow, link.owner_user_id))
        assert parent.silent
        assert preference_skip(_item(parent), settings, clock.now) == "expired"


@pytest.mark.asyncio
async def test_partial_ack_is_quarantined_without_releasing_or_blocking(db):
    link, clock, outbox, digests, ids = await setup(db, ["question"] * 3)
    identity = await digests.reserve(link, category="essential")
    await arm(db, outbox, link, identity)
    async with db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, identity)
        row.parts = [{**row.parts[0], "state": "sent", "sent_at": clock.now.isoformat(), "message_ids": [101]}, {**row.parts[0], "part_id": uuid4().hex}]

    async def send(*args):
        raise AssertionError("Invalid partial digest must never dispatch")

    sender = OutboxSender(outbox, run_id="partial", deliver_part=transport(db, send), eligibility=allowed, proactive=True)
    try:
        assert await sender.tick() == 0
        assert await sender.tick() == 0
        async with db.sessions() as reader:
            assert (await reader.get(TelegramOutboxRow, identity)).state == "failed"
            assert all([(await reader.get(TelegramOutboxRow, member)).payload["digest_id"] == identity for member in ids])
        with pytest.raises(ValueError, match="already-acknowledged"):
            await digests.abandon(link, identity)
    finally:
        await sender.stop()


@pytest.mark.asyncio
async def test_attempted_and_direct_messages_cannot_be_regrouped(db):
    link, clock, outbox, digests, ids = await setup(db, ["question"] * 5)
    async with db.sessions.begin() as writer:
        attempted = await writer.get(TelegramOutboxRow, ids[0])
        attempted.parts = [{**attempted.parts[0], "dispatched_at": clock.now.isoformat()}]
        direct = await writer.get(TelegramOutboxRow, ids[1])
        direct.dedupe_key = "reply:" + uuid4().hex
    identity = await digests.reserve(link, category="essential")
    async with db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, identity)).payload["member_ids"] == ids[2:]


@pytest.mark.asyncio
async def test_optional_digest_counts_once_and_essential_digest_bypasses_quota(db):
    from app.telegram_bots.notifications import budget_retry_at

    link, clock, outbox, digests, ids = await setup(db, ["info"] * 3 + ["question"] * 3)
    optional = await digests.reserve(link, category="discretionary")
    essential = await digests.reserve(link, category="essential")
    async with outbox._writer(link.id) as (writer, _binding):
        row = await writer.get(TelegramOutboxRow, optional)
        config = outbox.config.outbox
        assert await budget_retry_at(writer, row, now=clock.now, config=config) is None
        assert await budget_retry_at(writer, row, now=clock.now, config=config) is None
        assert await budget_retry_at(writer, await writer.get(TelegramOutboxRow, essential), now=clock.now, config=config) is None
        for identity in ids:
            assert (await writer.get(TelegramOutboxRow, identity)).payload.get("notification_reserved_at") is None
    independent, _ = await outbox._add(link, dedupe_key="ev:" + uuid4().hex, class_="info", payload={"parts": [{"kind": "text", "source_ref": {"key": "chat.restarted", "values": {}}}]})
    async with outbox._writer(link.id) as (writer, _binding):
        assert await budget_retry_at(writer, await writer.get(TelegramOutboxRow, independent), now=clock.now, config=config) == clock.now + timedelta(seconds=60)


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["unavailable", "retired"])
async def test_final_non_ack_part_never_settles_digest_members(db, state):
    link, clock, outbox, digests, ids = await setup(db, ["question"] * 3)
    identity = await digests.reserve(link, category="essential")
    await arm(db, outbox, link, identity)
    async with db.sessions.begin() as writer:
        parent = await writer.get(TelegramOutboxRow, identity)
        parent.parts = [{**parent.parts[0], "state": state}]

    async def send(*args):
        raise AssertionError("Final non-ACK part cannot send")

    sender = OutboxSender(outbox, run_id="nonack", deliver_part=transport(db, send), eligibility=allowed, proactive=True)
    try:
        await sender.tick()
        await sender.drain()
        async with db.sessions() as reader:
            assert (await reader.get(TelegramOutboxRow, identity)).state != "sent"
            assert all([(await reader.get(TelegramOutboxRow, member)).skip_reason != "in-digest" for member in ids])
    finally:
        await sender.stop()


def test_digest_threshold_cannot_exceed_supported_members():
    from pydantic import ValidationError

    from app.telegram_bots.config import TelegramBotsConfig

    assert TelegramBotsConfig(outbox={"digest_threshold": 10}).outbox.digest_threshold == 10
    with pytest.raises(ValidationError):
        TelegramBotsConfig(outbox={"digest_threshold": 11})


@pytest.mark.asyncio
async def test_empty_transport_receipt_cannot_acknowledge_members(db):
    link, clock, outbox, digests, ids = await setup(db, ["question"] * 3)
    identity = await digests.reserve(link, category="essential")
    await arm(db, outbox, link, identity)

    async def send(*args):
        return PartReceipt(())

    sender = OutboxSender(outbox, run_id="empty", deliver_part=transport(db, send), eligibility=allowed, proactive=True)
    try:
        assert await sender.tick() == 1
        await sender.drain()
        async with db.sessions() as reader:
            assert (await reader.get(TelegramOutboxRow, identity)).state == "failed"
            for member in ids:
                row = await reader.get(TelegramOutboxRow, member)
                assert row.state == "pending" and row.payload["digest_id"] == identity
                assert row.skip_reason is None
    finally:
        await sender.stop()


@pytest.mark.asyncio
async def test_digest_inherits_part_silence(db):
    link, clock, outbox, digests, ids = await setup(db, ["result"] * 3)
    async with db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, ids[0])
        assert not row.silent
        row.parts = [{**part, "silent": True} for part in row.parts]
    identity = await digests.reserve(link, category="essential")
    async with db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, identity)).silent
