"""A turned-off person's channel messages still waiting to be processed end, so none runs once they are enabled again.

A durable receipt waits in the database until a Gateway processes it: one
received and not yet claimed, one deferred for a retry, and a dead letter an
operator may requeue. Each would run the message if it were processed after
``enable``. ``disable`` ends them as the channel manager ends a message whose
owner is refused (``owner_refused``); one a Gateway is processing now reads
the refusal itself.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.channels.inbound_receipt_operations import InboundDeadLetterRequeueRequest
from app.channels.inbound_receipts import SqlInboundReceiptStore
from deerflow.persistence.base import Base
from deerflow.persistence.inbound_receipt.model import InboundReceiptRow

NOW = datetime(2026, 9, 26, 12, 0, tzinfo=UTC)


def _row(receipt_id: str, *, state: str, owner: str = "owner-1", run_id: str | None = None, kind: str = "connection") -> InboundReceiptRow:
    leased = state in ("claimed", "admitted")
    return InboundReceiptRow(
        receipt_id=receipt_id,
        provider="slack",
        binding_kind=kind,
        binding_reference=f"binding-of-{owner}" if kind == "connection" else "route:v1:sha256:" + "a" * 64,
        provider_delivery_id=f"delivery-{receipt_id}",
        thread_id=f"thread-{receipt_id}",
        payload_json={"version": 1, "owner_user_id": owner, "text": "Send me the weekly numbers"},
        payload_digest="a" * 64,
        provider_event_digest="b" * 64,
        state=state,
        lease_owner="gateway-a" if leased else None,
        lease_expires_at=NOW + timedelta(minutes=1) if leased else None,
        fencing_token=2,
        attempt_count=1,
        failure_count=8 if state == "dead_letter" else 0,
        next_attempt_at=NOW,
        run_id=run_id,
        outcome_code="attempts_exhausted" if state == "dead_letter" else None,
        received_at=NOW - timedelta(minutes=5),
        updated_at=NOW - timedelta(minutes=5),
    )


@pytest.fixture
async def store(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'receipts.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all, tables=[InboundReceiptRow.__table__])
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions.begin() as session:
        session.add_all(
            [
                _row("00000000-0000-0000-0000-000000000001", state="received"),
                _row("00000000-0000-0000-0000-000000000002", state="deferred"),
                _row("00000000-0000-0000-0000-000000000003", state="dead_letter"),
                # A webhook route's messages carry their owner too.
                _row("00000000-0000-0000-0000-000000000004", state="received", kind="webhook_route"),
                _row("00000000-0000-0000-0000-000000000005", state="claimed"),
                _row("00000000-0000-0000-0000-000000000006", state="admitted", run_id="run-being-cancelled"),
                _row("00000000-0000-0000-0000-000000000007", state="received", owner="owner-2"),
            ]
        )
    try:
        yield SqlInboundReceiptStore(sessions, clock=lambda: NOW), sessions
    finally:
        await engine.dispose()


@pytest.mark.anyio
async def test_the_owners_waiting_and_dead_lettered_messages_end_and_nothing_else(store):
    receipts, sessions = store

    assert await receipts.end_for_owners(["owner-1"], outcome_code="owner_refused") == 4

    async with sessions() as session:
        rows = {row.receipt_id[-1]: row for row in (await session.execute(select(InboundReceiptRow))).scalars()}
    for ended in ("1", "2", "3", "4"):
        assert rows[ended].state == "completed"
        assert rows[ended].outcome_code == "owner_refused"
        assert rows[ended].completed_at is not None
        assert rows[ended].lease_owner is None
        # A fence a stale processor still holding the old token cannot pass.
        assert rows[ended].fencing_token == 3
    assert rows["5"].state == "claimed", "a Gateway processing it reads the refusal itself"
    assert rows["6"].state == "admitted", "its run is what disable cancels"
    assert rows["7"].state == "received"
    assert await receipts.end_for_owners(["owner-1"], outcome_code="owner_refused") == 0
    assert await receipts.end_for_owners([], outcome_code="owner_refused") == 0
    # What is left to the Gateway processing it, until it reads the refusal.
    assert await receipts.count_claimed_for_owners(["owner-1"]) == 1
    assert await receipts.count_claimed_for_owners(["owner-2"]) == 0


@pytest.mark.anyio
async def test_an_operator_cannot_requeue_a_dead_letter_the_owners_disable_ended(store):
    receipts, _ = store
    await receipts.end_for_owners(["owner-1"], outcome_code="owner_refused")

    requeued = await receipts.requeue_dead_letter(
        InboundDeadLetterRequeueRequest(receipt_id="00000000-0000-0000-0000-000000000003", expected_fencing_token=2, expected_payload_digest="a" * 64, expected_provider_event_digest="b" * 64),
        requeued_at=NOW,
    )

    assert requeued is None
