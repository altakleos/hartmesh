"""Event controlled workers exercise SQL claims, retries and stale completions."""

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from _telegram_repository_test_support import add_link, bind_link, rows
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker
from telegram.error import BadRequest

from app.telegram_bots.producer import DeliveryEligibility, DeliveryRetry, Outbox, OutboxSender, PartReceipt, PermanentDeliveryError, SourceUnavailable, TransientDeliveryError
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow


class Clock:
    def __init__(self):
        self.now = datetime(2030, 1, 1, tzinfo=UTC)

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += timedelta(seconds=seconds)


async def allowed(link, row, part):
    return DeliveryEligibility.allow()


def transport(db, send):
    async def deliver(*, link_id, outbox_id, part_id, claim_owner, claim_id):
        async with db.sessions() as session:
            row = await session.get(TelegramOutboxRow, outbox_id)
            assert row.state == "sending" and (row.claim_owner, row.claim_id) == (claim_owner, claim_id)
            part = next(part for part in row.parts if part["part_id"] == part_id)
            assert part["state"] == "sending"
        return await send(await db.links.get(link_id), row, part)

    return deliver


async def setup(db, *, bot=9_000_000_001, owner="owner-a", account=7_000_000_001, parts=1):
    link = await bind_link(db, await add_link(db, bot=bot, owner=owner), account=account)
    async with db.sessions.begin() as session:
        for row in await session.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == link.id)):
            row.state = "sent"
    clock = Clock()
    outbox = Outbox(db.sessions, clock=clock)
    await outbox.add(link, dedupe_key="notice:worker", class_="reply", payload={"parts": [{"kind": "text", "source_ref": {"key": "chat.restarted", "values": {}}} for _ in range(parts)]})
    return link, clock, outbox


async def saved(db, dedupe="notice:worker"):
    return next(row for row in await rows(db, TelegramOutboxRow) if row.dedupe_key == dedupe)


@pytest.mark.asyncio
async def test_tick_returns_while_transport_is_blocked_and_caps_links(db):
    link, clock, outbox = await setup(db)
    entered, release = asyncio.Event(), asyncio.Event()

    async def send(link, row, part):
        entered.set()
        await release.wait()
        return PartReceipt((900,))

    sender = OutboxSender(outbox, run_id="process", deliver_part=transport(db, send), eligibility=allowed, concurrency=1)
    try:
        assert await sender.tick() == 1
        await entered.wait()
        assert await sender.tick() == 0
        assert (await saved(db)).state == "sending"
    finally:
        release.set()
        await sender.drain()
    assert (await saved(db)).state == "sent"


@pytest.mark.asyncio
async def test_retry_after_releases_capacity_without_counting_attempt(db):
    link, clock, outbox = await setup(db)

    async def send(link, row, part):
        raise DeliveryRetry(86_400)

    sender = OutboxSender(outbox, run_id="process", deliver_part=transport(db, send), eligibility=allowed)
    assert await sender.tick() == 1
    await sender.drain()
    row = await saved(db)
    assert row.state == "pending" and row.attempts == 0
    assert row.not_before.replace(tzinfo=UTC) >= clock() + timedelta(days=1)
    assert await sender.tick() == 0


@pytest.mark.asyncio
async def test_multipart_receipts_survive_restart_and_transport_retries(db):
    link, clock, outbox = await setup(db, parts=2)
    seen = []

    async def send(link, row, part):
        seen.append(part["part_id"])
        if len(seen) == 2:
            raise TransientDeliveryError()
        return PartReceipt((900 + len(seen),), protect_content=True)

    async def protected(link, row, part):
        return DeliveryEligibility.allow(protect_content=True)

    sender = OutboxSender(outbox, run_id="process", deliver_part=transport(db, send), eligibility=protected, jitter=lambda: 0)
    assert await sender.tick() == 1
    await sender.drain()
    first = await saved(db)
    assert first.parts[0]["state"] == "sent" and first.parts[1]["state"] == "pending"
    assert first.parts[0]["message_ids"] == [901]
    clock.advance(1)
    await sender.tick()
    await sender.drain()
    failed = await saved(db)
    assert failed.state == "pending" and failed.attempts == 1
    clock.advance(5)
    replacement = OutboxSender(outbox, run_id="next-process", deliver_part=transport(db, send), eligibility=protected)
    await replacement.tick()
    await replacement.drain()
    assert (await saved(db)).state == "sent"
    assert seen[0] != seen[1] == seen[2]


@pytest.mark.asyncio
async def test_permanent_failure_queues_one_nonrecursive_repair(db, caplog):
    link, clock, outbox = await setup(db)

    async def send(link, row, part):
        raise PermanentDeliveryError("invalid-payload")

    sender = OutboxSender(outbox, run_id="process", deliver_part=transport(db, send), eligibility=allowed)
    await sender.tick()
    await sender.drain()
    original = await saved(db)
    assert original.state == "failed" and original.failed_reason == "invalid-payload"
    repair = next(row for row in await rows(db, TelegramOutboxRow) if row.dedupe_key.startswith("delivery-failed:"))
    assert repair.class_ == "reply" and repair.payload["repair_outbox_id"] == original.id
    assert len(await rows(db, TelegramCallbackRow)) == 1
    clock.advance(1)
    await sender.tick()
    await sender.drain()
    assert len(await rows(db, TelegramOutboxRow)) == 3
    assert next(row for row in await rows(db, TelegramOutboxRow) if row.id == repair.id).state == "failed"
    assert len([record for record in caplog.records if "send_failed" in record.message]) == 2
    assert all("failed_reason=invalid-payload" in record.message and "source_ref" not in record.message for record in caplog.records if "send_failed" in record.message)


@pytest.mark.asyncio
async def test_conflict_allows_dispatched_receipt_but_stops_the_next_part(db):
    link, clock, outbox = await setup(db, parts=2)
    entered, release = asyncio.Event(), asyncio.Event()
    calls = []

    async def send(link, row, part):
        calls.append(part["part_id"])
        entered.set()
        await release.wait()
        return [800]

    sender = OutboxSender(outbox, run_id="process", deliver_part=transport(db, send), eligibility=allowed)
    try:
        await sender.tick()
        await entered.wait()
        await db.links.set_health(link.id, "conflict", holds=True)
    finally:
        release.set()
        await sender.drain()
    row = await saved(db)
    assert row.state == "pending" and row.parts[0]["state"] == "sent"
    assert row.parts[1]["state"] == "pending" and len(calls) == 1
    clock.advance(500)
    assert await sender.tick() == 0


@pytest.mark.asyncio
async def test_status_notice_bypasses_missing_agent_but_model_and_file_parts_wait(db):
    link, clock, outbox = await setup(db)
    await db.links.set_health(link.id, "agent_unavailable", holds=True)
    notice = await outbox.plan_status_notice(link, source_id="missing-target", parts=[{"kind": "text", "source_ref": {"key": "access.agent_off", "values": {}}}])
    with pytest.raises(ValueError, match="status-notice"):
        await outbox.plan_status_notice(link, source_id="bad-file", parts=[{"kind": "document", "source_ref": {"space_id": "private", "path": "answer.pdf"}}])
    sent = []

    async def send(link, row, part):
        sent.append(row.id)
        return [800]

    sender = OutboxSender(outbox, run_id="process", deliver_part=transport(db, send), eligibility=allowed)
    assert await sender.tick() == 1
    await sender.drain()
    assert sent == [notice] and (await saved(db)).state == "pending"


@pytest.mark.asyncio
async def test_degraded_delivery_keeps_retrying_and_clears_on_success(db):
    link, clock, outbox = await setup(db)
    outbox.config.outbox.max_attempts = 1
    calls = 0

    async def send(link, row, part):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise TransientDeliveryError()
        return [800]

    sender = OutboxSender(outbox, run_id="process", deliver_part=transport(db, send), eligibility=allowed, jitter=lambda: 0)
    await sender.tick()
    await sender.drain()
    row = await saved(db)
    assert row.degraded_at is not None and row.state == "pending"
    clock.advance(5)
    await sender.tick()
    await sender.drain()
    row = await saved(db)
    assert row.state == "sent" and row.degraded_at is None


@pytest.mark.asyncio
async def test_one_worker_per_link_is_atomic_across_schedulers(db):
    link, clock, outbox = await setup(db)
    await outbox.add(link, dedupe_key="notice:second", class_="reply", payload={"parts": [{"kind": "text", "source_ref": {"key": "chat.restarted", "values": {}}}]})
    entered, release = asyncio.Event(), asyncio.Event()
    calls = []

    async def send(link, row, part):
        calls.append(row.id)
        entered.set()
        await release.wait()
        return [800]

    first = OutboxSender(outbox, run_id="first", deliver_part=transport(db, send), eligibility=allowed)
    second = OutboxSender(outbox, run_id="second", deliver_part=transport(db, send), eligibility=allowed)
    try:
        assert sum(await asyncio.gather(first.tick(), second.tick())) == 1
        await entered.wait()
        assert len(calls) == 1
    finally:
        release.set()
        await asyncio.gather(first.drain(), second.drain())


@pytest.mark.asyncio
async def test_prior_process_claim_is_recovered_without_erasing_receipts(db):
    link, clock, outbox = await setup(db, parts=2)
    row = await saved(db)
    async with db.sessions.begin() as session:
        current = await session.get(TelegramOutboxRow, row.id)
        parts = current.parts.copy()
        parts[0] = {**parts[0], "state": "sent", "message_ids": [601]}
        parts[1] = {**parts[1], "state": "sending"}
        current.parts = parts
        current.state, current.claim_owner, current.claim_id = "sending", "old-process", "abandoned"
    seen = []

    async def send(link, row, part):
        seen.append(part["part_id"])
        return [602]

    sender = OutboxSender(outbox, run_id="current-process", deliver_part=transport(db, send), eligibility=allowed)
    await sender.tick()
    await sender.drain()
    current = await saved(db)
    assert current.state == "sent" and seen == [row.parts[1]["part_id"]]
    assert current.parts[0]["message_ids"] == [601]


@pytest.mark.asyncio
async def test_current_authority_is_checked_before_each_part(db):
    link, clock, outbox = await setup(db, parts=2)
    permitted = True

    async def check(link, row, part):
        return DeliveryEligibility.allow() if permitted else DeliveryEligibility.skip("no-access")

    async def send(link, row, part):
        nonlocal permitted
        permitted = False
        return [602]

    sender = OutboxSender(outbox, run_id="process", deliver_part=transport(db, send), eligibility=check)
    await sender.tick()
    await sender.drain()
    row = await saved(db)
    assert row.state == "skipped" and row.skip_reason == "no-access"
    assert [part["state"] for part in row.parts] == ["sent", "pending"]


@pytest.mark.asyncio
async def test_pacing_applies_between_distinct_reply_rows(db):
    link, clock, outbox = await setup(db)
    await outbox.add(link, dedupe_key="notice:second", class_="reply", payload={"parts": [{"kind": "text", "source_ref": {"key": "chat.restarted", "values": {}}}]})
    calls = []

    async def send(link, row, part):
        calls.append(row.id)
        return [602]

    sender = OutboxSender(outbox, run_id="process", deliver_part=transport(db, send), eligibility=allowed)
    await sender.tick()
    await sender.drain()
    await sender.tick()
    await sender.drain()
    assert len(calls) == 1
    clock.advance(1)
    await sender.tick()
    await sender.drain()
    assert len(calls) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("old_result", ["success", "transient", "permanent"])
async def test_stale_cancelled_same_process_completion_cannot_touch_successor(db, old_result):
    link, clock, outbox = await setup(db)
    old_entered, cancelled, release_old, new_entered, release_new = (asyncio.Event() for _ in range(5))

    async def old_send(link, row, part):
        old_entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            # An already-dispatched native/transport completion may outlive its
            # cancelled awaiter. Keep that exact old admission paused here.
            await release_old.wait()
        if old_result == "transient":
            raise TransientDeliveryError()
        if old_result == "permanent":
            raise PermanentDeliveryError()
        return [111]

    async def new_send(link, row, part):
        new_entered.set()
        await release_new.wait()
        return [222]

    first = OutboxSender(outbox, run_id="same-process", deliver_part=transport(db, old_send), eligibility=allowed)
    successor = OutboxSender(outbox, run_id="same-process", deliver_part=transport(db, new_send), eligibility=allowed)
    try:
        await first.tick()
        await old_entered.wait()
        old_claim = (await saved(db)).claim_id
        first._workers[link.id][2].cancel()
        await cancelled.wait()
        replacement = await db.links.store_token(link.id, "synthetic-wrapped-new", actor_id=link.owner_user_id, expected_revision=link.token_revision, token_changed=True)
        assert replacement.binding_generation == link.binding_generation
        clock.advance(1)
        await successor.tick()
        await new_entered.wait()
        current = await saved(db)
        assert current.claim_id != old_claim
        before = {column.key: getattr(current, column.key) for column in TelegramOutboxRow.__table__.columns if column.key != "class"}
        release_old.set()
        await first.drain()
        after = await saved(db)
        assert {key: getattr(after, key) for key in before} == before
        assert len(await rows(db, TelegramOutboxRow)) == 2
    finally:
        release_old.set()
        release_new.set()
        await asyncio.gather(first.drain(), successor.drain())
    assert (await saved(db)).parts[0]["message_ids"] == [222]


@pytest.mark.asyncio
async def test_missing_live_edit_anchor_replaces_same_frozen_source(db):
    link, clock, outbox = await setup(db)
    original = await saved(db)
    async with db.sessions.begin() as session:
        row = await session.get(TelegramOutboxRow, original.id)
        row.parts = [{**row.parts[0], "kind": "edit", "target_message_id": 400}]
    calls = []

    async def send(link, row, part):
        calls.append(part)
        if len(calls) == 1:
            raise BadRequest("Message to edit not found")
        return [602]

    sender = OutboxSender(outbox, run_id="process", deliver_part=transport(db, send), eligibility=allowed)
    await sender.tick()
    await sender.drain()
    replacement = await saved(db)
    assert replacement.state == "pending" and replacement.payload["presentation_version"] == 1
    assert replacement.parts[0]["state"] == "retired"
    assert replacement.parts[1]["source_ref"] == original.parts[0]["source_ref"]
    assert replacement.parts[1]["kind"] == "text" and "target_message_id" not in replacement.parts[1]
    clock.advance(1)
    await sender.tick()
    await sender.drain()
    assert (await saved(db)).state == "sent" and len(calls) == 2


@pytest.mark.asyncio
async def test_global_worker_cap_and_link_fairness_with_two_schedulers(db):
    prepared = [await setup(db, bot=9_000_000_001 + index) for index in range(9)]
    outbox, clock = prepared[0][2], prepared[0][1]
    release, full = asyncio.Event(), asyncio.Event()
    calls = []

    async def send(link, row, part):
        calls.append(link.id)
        if len(calls) == 8:
            full.set()
        await release.wait()
        return [602]

    first = OutboxSender(outbox, run_id="first", deliver_part=transport(db, send), eligibility=allowed)
    second = OutboxSender(outbox, run_id="second", deliver_part=transport(db, send), eligibility=allowed)
    try:
        assert sum(await asyncio.gather(first.tick(), second.tick())) == 8
        await asyncio.wait_for(full.wait(), timeout=10)
        assert len(calls) == len(set(calls)) == 8
        assert await first.tick() == await second.tick() == 0
    finally:
        release.set()
        await asyncio.gather(first.drain(), second.drain())
    clock.advance(1)
    assert await first.tick() == 1
    await first.drain()
    assert len(calls) == len(set(calls)) == 9


@pytest.mark.asyncio
async def test_unavailable_file_saves_visible_fallback_and_preserves_other_part_ids(db):
    link, clock, outbox = await setup(db, parts=3)
    original = await saved(db)
    async with db.sessions.begin() as session:
        row = await session.get(TelegramOutboxRow, original.id)
        row.parts = [{**row.parts[0], "state": "sent", "message_ids": [501]}, {**row.parts[1], "kind": "document", "source_ref": {"space_id": "authorized", "path": "report.pdf"}}, row.parts[2]]
    seen = []

    async def send(link, row, part):
        seen.append(part["part_id"])
        if part["kind"] == "document":
            raise SourceUnavailable(source_ref={"key": "file.out_failed", "values": {"file": "report.pdf"}}, reason="source-changed")
        return [502]

    sender = OutboxSender(outbox, run_id="process", deliver_part=transport(db, send), eligibility=allowed)
    await sender.tick()
    await sender.drain()
    changed = await saved(db)
    assert changed.state == "pending" and [part["state"] for part in changed.parts] == ["sent", "unavailable", "pending", "pending"]
    assert changed.parts[0] == original.parts[0] | {"state": "sent", "message_ids": [501]}
    assert changed.parts[1]["part_id"] == original.parts[1]["part_id"]
    assert changed.parts[2]["source_ref"] == {"key": "file.out_failed", "values": {"file": "report.pdf"}}
    assert changed.parts[3]["part_id"] == original.parts[2]["part_id"]
    for _ in range(2):
        clock.advance(1)
        await sender.tick()
        await sender.drain()
    assert (await saved(db)).state == "sent" and len(seen) == 3


@pytest.mark.asyncio
async def test_unavailable_actionable_final_file_moves_same_controls_to_visible_fallback(db):
    link, clock, outbox = await setup(db, parts=2)
    original = await saved(db)
    group, callback_id = "b" * 32, "c" * 32
    source = {"space_id": "authorized", "path": "report.pdf"}
    async with db.sessions.begin() as session:
        row = await session.get(TelegramOutboxRow, original.id)
        row.parts = [{**row.parts[0], "state": "sent", "message_ids": [501]}, {**row.parts[1], "kind": "document", "source_ref": source, "callback_group_id": group}]
        session.add(
            TelegramCallbackRow(
                id=callback_id,
                group_id=group,
                link_id=link.id,
                chat_id=link.chat_id,
                binding_generation=link.binding_generation,
                kind="file_retry",
                target_kind="outbox",
                target_id=row.id,
                payload={"operation_id": "immutable-operation"},
                created_at=clock(),
            )
        )
    observed = []

    async def send(link, row, part):
        if part["kind"] == "document":
            raise SourceUnavailable(source_ref={"key": "file.out_failed", "values": {"file": "report.pdf"}})
        observed.append(part)
        return [602]

    sender = OutboxSender(outbox, run_id="process", deliver_part=transport(db, send), eligibility=allowed)
    await sender.tick()
    await sender.drain()
    replaced = await saved(db)
    assert replaced.parts[-1]["callback_group_id"] == group
    assert replaced.parts[1]["source_ref"] == source and replaced.parts[1]["part_id"] == original.parts[1]["part_id"]
    assert replaced.parts[1]["state"] == "unavailable" and replaced.parts[1]["callback_group_id"] is None
    assert (await rows(db, TelegramCallbackRow))[0].message_id is None
    clock.advance(1)
    await sender.tick()
    await sender.drain()
    current, callbacks = await saved(db), await rows(db, TelegramCallbackRow)
    assert current.state == "sent" and observed[-1]["callback_group_id"] == group
    assert current.parts[0] == original.parts[0] | {"state": "sent", "message_ids": [501]}
    assert len(callbacks) == 1 and (callbacks[0].id, callbacks[0].group_id, callbacks[0].message_id) == (callback_id, group, 602)
    assert callbacks[0].payload["operation_id"] == "immutable-operation" and callbacks[0].consumed_at is None


@pytest.mark.asyncio
async def test_retry_button_survives_scheduling_and_failure_until_original_is_sent(db):
    link, clock, outbox = await setup(db)
    original = await saved(db)
    original_calls = 0

    async def send(link, row, part):
        nonlocal original_calls
        if row.id == original.id:
            original_calls += 1
            if original_calls == 1:
                raise PermanentDeliveryError()
            if original_calls == 2:
                raise TransientDeliveryError()
        return [600 + original_calls]

    sender = OutboxSender(outbox, run_id="process", deliver_part=transport(db, send), eligibility=allowed, jitter=lambda: 0)
    await sender.tick()
    await sender.drain()
    clock.advance(1)
    await sender.tick()
    await sender.drain()
    assert (await rows(db, TelegramCallbackRow))[0].message_id is not None
    await outbox.retry(link, original.id)
    assert (await rows(db, TelegramCallbackRow))[0].consumed_at is None
    clock.advance(1)
    await sender.tick()
    await sender.drain()
    assert (await rows(db, TelegramCallbackRow))[0].consumed_at is None
    clock.advance(5)
    await sender.tick()
    await sender.drain()
    repaired = await saved(db)
    assert repaired.state == "sent" and [part["part_id"] for part in repaired.parts] == [part["part_id"] for part in original.parts]
    assert (await rows(db, TelegramCallbackRow))[0].consumed_at is not None
    assert any(row.payload.get("kind") == "keyboard_cleanup" for row in await rows(db, TelegramOutboxRow))


@pytest.mark.asyncio
async def test_saved_protection_requirement_survives_policy_relaxation(db):
    link, clock, outbox = await setup(db)
    protected, calls = True, []

    async def policy(link, row, part):
        return DeliveryEligibility.allow(protect_content=protected)

    async def send(link, row, part):
        nonlocal protected
        calls.append(part["protect_content"])
        if len(calls) == 1:
            protected = False
            raise TransientDeliveryError()
        return [602]

    sender = OutboxSender(outbox, run_id="process", deliver_part=transport(db, send), eligibility=policy, jitter=lambda: 0)
    await sender.tick()
    await sender.drain()
    clock.advance(5)
    await sender.tick()
    await sender.drain()
    assert calls == [True, True] and (await saved(db)).parts[0]["protect_content"] is True


@pytest.mark.asyncio
async def test_another_session_factory_cannot_recover_a_live_local_transport(db):
    link, clock, outbox = await setup(db)
    entered, release = asyncio.Event(), asyncio.Event()

    async def send(link, row, part):
        entered.set()
        await release.wait()
        return [602]

    first = OutboxSender(outbox, run_id="first", deliver_part=transport(db, send), eligibility=allowed)
    other = Outbox(async_sessionmaker(db.engine, expire_on_commit=False), clock=clock)
    second = OutboxSender(other, run_id="second", deliver_part=transport(db, send), eligibility=allowed)
    try:
        await first.tick()
        await entered.wait()
        assert await second.tick() == 0
        assert (await saved(db)).claim_owner == "first"
    finally:
        release.set()
        await asyncio.gather(first.drain(), second.drain())
