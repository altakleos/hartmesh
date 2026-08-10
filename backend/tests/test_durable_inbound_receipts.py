"""Durable native-ingress receipt state and recovery behavior."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.channels.inbound_receipts import (
    InboundProcessingDisposition,
    InboundProcessingResult,
    InboundReceiptCandidate,
    InboundReceiptEnvelope,
    InboundReceiptProcessor,
    InboundReceiptReplayConflict,
    SqlInboundReceiptStore,
)
from app.channels.message_bus import InboundMessage
from app.channels.service import ChannelService
from app.runtime.native_binding import (
    InternalVerifiedNativeBinding,
    InternalVerifiedNativeBindingKind,
)
from deerflow.config.app_config import AppConfig
from deerflow.config.sandbox_config import SandboxConfig
from deerflow.persistence.base import Base
from deerflow.persistence.inbound_receipt.model import InboundReceiptRow


def _envelope(
    *,
    delivery_id: str,
    thread_id: str = "thread-17-reviewer",
    text: str = "Review pull request 17",
    created_at: float,
) -> InboundReceiptEnvelope:
    return InboundReceiptEnvelope.from_message(
        InboundMessage(
            channel_name="github",
            chat_id="hartmesh/runtime",
            user_id="octocat",
            text=text,
            topic_id="17:reviewer",
            owner_user_id="owner-1",
            workspace_id="hartmesh/runtime",
            verified_source_binding=InternalVerifiedNativeBinding(
                kind=InternalVerifiedNativeBindingKind.webhook_route,
                reference="route:v1:sha256:" + ("a" * 64),
            ),
            metadata={
                "message_id": f"{delivery_id}:owner-1:reviewer",
                "agent_name": "reviewer",
                "preferred_thread_id": thread_id,
                "github": {
                    "repo": "hartmesh/runtime",
                    "number": 17,
                    "event": "pull_request",
                    "delivery_id": delivery_id,
                    "installation_id": 42,
                    "recursion_limit": 100,
                    "thread_id": thread_id,
                },
            },
            created_at=created_at,
        )
    )


def _candidate(
    envelope: InboundReceiptEnvelope,
    *,
    provider_event_digest: str = "a" * 64,
) -> InboundReceiptCandidate:
    return InboundReceiptCandidate(
        envelope=envelope,
        provider_event_digest=provider_event_digest,
    )


@pytest.mark.asyncio
async def test_durable_profile_never_falls_back_to_message_bus_without_receipts() -> None:
    raw = AppConfig(sandbox=SandboxConfig(use="test")).model_dump(mode="python")
    raw["deployment"]["profile"] = "durable_production"
    raw["database"]["backend"] = "sqlite"
    config = AppConfig.model_validate(raw)
    service = ChannelService(channels_config={}, app_config=config)
    message = InboundMessage(
        channel_name="github",
        chat_id="repo:issue:17",
        user_id="octocat",
        text="review",
    )

    with pytest.raises(RuntimeError, match="durable inbound receipt processor"):
        await service.accept_verified_inbound_batch((message,))

    with pytest.raises(TimeoutError):
        await asyncio.wait_for(service.bus.get_inbound(), timeout=0.01)


@pytest.mark.asyncio
async def test_durable_profile_rejects_non_durable_receipt_processor() -> None:
    class BestEffortProcessor:
        durable = False

        async def receive_batch(self, messages) -> None:
            raise AssertionError("best-effort processor must not receive durable ingress")

    raw = AppConfig(sandbox=SandboxConfig(use="test")).model_dump(mode="python")
    raw["deployment"]["profile"] = "durable_production"
    raw["database"]["backend"] = "sqlite"
    config = AppConfig.model_validate(raw)
    service = ChannelService(channels_config={}, app_config=config)
    service.inbound_receipt_processor = BestEffortProcessor()

    with pytest.raises(RuntimeError, match="durable inbound receipt processor"):
        await service.accept_verified_inbound_batch(
            (
                InboundMessage(
                    channel_name="github",
                    chat_id="repo:issue:17",
                    user_id="octocat",
                    text="review",
                ),
            )
        )


@pytest.mark.asyncio
async def test_received_payload_survives_process_loss_before_claim(tmp_path) -> None:
    """A committed receipt, unlike the old TTL key, is enough to resume work."""

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'receipts.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(
            Base.metadata.create_all,
            tables=[InboundReceiptRow.__table__],
        )
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime(2026, 8, 8, 12, 0, tzinfo=UTC)
    envelope = _envelope(delivery_id="delivery-1", created_at=now.timestamp())

    first_process = SqlInboundReceiptStore(sessions, clock=lambda: now)
    (received,) = await first_process.receive_batch((_candidate(envelope),))
    assert received.state == "received"

    # Simulate losing the process and its MessageBus before it can claim.
    second_process = SqlInboundReceiptStore(
        sessions,
        clock=lambda: now + timedelta(seconds=1),
    )
    due = await second_process.list_due(limit=10)
    assert [item.receipt_id for item in due] == [received.receipt_id]

    claim = await second_process.claim(
        received.receipt_id,
        lease_owner="gateway-restart",
        lease_seconds=30,
    )
    assert claim is not None
    assert claim.envelope == envelope
    assert claim.state == "claimed"
    assert claim.fencing_token == 1

    await engine.dispose()


@pytest.mark.asyncio
async def test_reclaimed_receipt_rejects_stale_fencing_token(tmp_path) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'receipts.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all, tables=[InboundReceiptRow.__table__])
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime(2026, 8, 8, 12, 0, tzinfo=UTC)
    current = [now]
    store = SqlInboundReceiptStore(sessions, clock=lambda: current[0])
    (received,) = await store.receive_batch((_candidate(_envelope(delivery_id="delivery-2", created_at=now.timestamp())),))

    first = await store.claim(received.receipt_id, lease_owner="worker-1", lease_seconds=10)
    assert first is not None
    assert await store.claim(received.receipt_id, lease_owner="worker-2", lease_seconds=10) is None
    current[0] += timedelta(seconds=11)
    second = await store.claim(received.receipt_id, lease_owner="worker-2", lease_seconds=10)
    assert second is not None
    assert second.fencing_token == first.fencing_token + 1

    assert not await store.bind_admitted(
        received.receipt_id,
        lease_owner="worker-1",
        fencing_token=first.fencing_token,
        run_id="run-stale",
    )
    assert not await store.complete(
        received.receipt_id,
        lease_owner="worker-1",
        fencing_token=first.fencing_token,
        outcome_code="stale_completion",
    )
    assert await store.bind_admitted(
        received.receipt_id,
        lease_owner="worker-2",
        fencing_token=second.fencing_token,
        run_id="run-2",
    )
    assert await store.complete(
        received.receipt_id,
        lease_owner="worker-2",
        fencing_token=second.fencing_token,
        outcome_code="admitted",
    )

    await engine.dispose()


@pytest.mark.asyncio
async def test_same_thread_receipts_claim_in_fifo_order_and_defer_without_loss(tmp_path) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'receipts.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all, tables=[InboundReceiptRow.__table__])
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime(2026, 8, 8, 12, 0, tzinfo=UTC)
    current = [now]
    store = SqlInboundReceiptStore(sessions, clock=lambda: current[0])
    first, second = await store.receive_batch(
        (
            _candidate(_envelope(delivery_id="delivery-3", created_at=now.timestamp())),
            _candidate(
                _envelope(
                    delivery_id="delivery-4",
                    created_at=(now + timedelta(seconds=1)).timestamp(),
                )
            ),
        )
    )

    assert await store.claim(second.receipt_id, lease_owner="worker", lease_seconds=30) is None
    first_claim = await store.claim(first.receipt_id, lease_owner="worker", lease_seconds=30)
    assert first_claim is not None
    assert await store.defer_contention(
        first.receipt_id,
        lease_owner="worker",
        fencing_token=first_claim.fencing_token,
        delay_seconds=5,
        outcome_code="thread_busy",
    )
    # A replacement process reconstructs arbitration entirely from SQL; no
    # prior MessageBus notification or follow-up buffer is required.
    store = SqlInboundReceiptStore(sessions, clock=lambda: current[0])
    assert await store.claim(second.receipt_id, lease_owner="worker", lease_seconds=30) is None

    current[0] += timedelta(seconds=5)
    retried = await store.claim(first.receipt_id, lease_owner="worker", lease_seconds=30)
    assert retried is not None
    assert retried.failure_count == 0
    assert await store.complete(
        first.receipt_id,
        lease_owner="worker",
        fencing_token=retried.fencing_token,
        outcome_code="rejected",
    )
    assert await store.claim(second.receipt_id, lease_owner="worker", lease_seconds=30) is not None

    await engine.dispose()


@pytest.mark.asyncio
async def test_thread_contention_outlives_poison_budget_and_preserves_fifo(tmp_path) -> None:
    """A healthy busy thread never spends the malformed-receipt budget."""

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'receipts.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all, tables=[InboundReceiptRow.__table__])
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime(2026, 8, 8, 12, 0, tzinfo=UTC)
    current = [now]
    store = SqlInboundReceiptStore(sessions, clock=lambda: current[0])
    first, second = await store.receive_batch(
        (
            _candidate(_envelope(delivery_id="delivery-busy-k2", created_at=now.timestamp())),
            _candidate(
                _envelope(
                    delivery_id="delivery-busy-k3",
                    created_at=(now + timedelta(seconds=1)).timestamp(),
                )
            ),
        )
    )
    outcomes = [
        InboundProcessingResult(
            disposition=InboundProcessingDisposition.deferred,
            outcome_code="thread_busy",
        )
        for _ in range(10)
    ]
    outcomes.append(
        InboundProcessingResult(
            disposition=InboundProcessingDisposition.admitted,
            run_id="run-k2",
            outcome_code="admitted",
        )
    )

    async def process(_message):
        return outcomes.pop(0)

    async def no_wakeup(_wakeup) -> None:
        return None

    # max_attempts is the poison-failure budget. Ten legitimate contentions
    # deliberately exceed both that budget and the former exponential horizon.
    for attempt in range(11):
        processor = InboundReceiptProcessor(
            store=store,
            publish_wakeup=no_wakeup,
            process_message=process,
            lease_owner=f"gateway-{attempt}",
            retry_delay_seconds=2,
            contention_delay_seconds=30,
            max_attempts=2,
            clock=lambda: current[0],
        )
        await processor.process(first.receipt_id)
        if attempt < 10:
            async with sessions() as session:
                row = await session.get(InboundReceiptRow, first.receipt_id)
                assert row is not None
                assert row.state == "deferred"
                assert row.failure_count == 0
                assert row.attempt_count == attempt + 1
            # Simulate replacement processes over five minutes of contention.
            current[0] += timedelta(seconds=30)
            assert (
                await store.claim(
                    second.receipt_id,
                    lease_owner="out-of-order",
                    lease_seconds=30,
                )
                is None
            )

    async with sessions() as session:
        row = await session.get(InboundReceiptRow, first.receipt_id)
        assert row is not None
        assert row.state == "completed"
        assert row.run_id == "run-k2"
        assert row.failure_count == 0

    second_claim = await store.claim(
        second.receipt_id,
        lease_owner="gateway-k3",
        lease_seconds=30,
    )
    assert second_claim is not None
    assert second_claim.envelope.provider_delivery_id == "delivery-busy-k3"

    await engine.dispose()


@pytest.mark.asyncio
async def test_duplicate_receive_is_idempotent_but_changed_event_conflicts(tmp_path) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'receipts.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all, tables=[InboundReceiptRow.__table__])
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime(2026, 8, 8, 12, 0, tzinfo=UTC)
    store = SqlInboundReceiptStore(sessions, clock=lambda: now)
    envelope = _envelope(delivery_id="delivery-5", created_at=now.timestamp())

    (first,) = await store.receive_batch((_candidate(envelope),))
    (duplicate,) = await store.receive_batch((_candidate(envelope),))
    assert duplicate.receipt_id == first.receipt_id

    with pytest.raises(InboundReceiptReplayConflict, match="conflicting authenticated event"):
        await store.receive_batch(
            (
                _candidate(
                    _envelope(
                        delivery_id="delivery-5",
                        text="Different intent",
                        created_at=now.timestamp(),
                    ),
                    provider_event_digest="b" * 64,
                ),
            )
        )

    await engine.dispose()


@pytest.mark.asyncio
async def test_equal_provider_event_reuses_first_accepted_envelope_after_policy_change(tmp_path) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'receipts.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all, tables=[InboundReceiptRow.__table__])
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime(2026, 8, 8, 12, 0, tzinfo=UTC)
    store = SqlInboundReceiptStore(sessions, clock=lambda: now)
    original = _envelope(delivery_id="delivery-policy-change", created_at=now.timestamp())
    changed_policy = InboundReceiptEnvelope.from_dict(
        {
            **original.to_dict(),
            "policy_metadata": {
                **original.to_dict()["policy_metadata"],
                "github": {
                    **original.to_dict()["policy_metadata"]["github"],
                    "recursion_limit": 200,
                },
            },
        }
    )
    event_digest = "a" * 64

    (first,) = await store.receive_batch((InboundReceiptCandidate(envelope=original, provider_event_digest=event_digest),))
    (replayed,) = await store.receive_batch((InboundReceiptCandidate(envelope=changed_policy, provider_event_digest=event_digest),))

    assert replayed.receipt_id == first.receipt_id
    assert replayed.envelope == original
    assert replayed.envelope.policy_metadata["github"]["recursion_limit"] == 100

    with pytest.raises(InboundReceiptReplayConflict, match="conflicting authenticated event"):
        await store.receive_batch(
            (
                InboundReceiptCandidate(
                    envelope=changed_policy,
                    provider_event_digest="b" * 64,
                ),
            )
        )

    await engine.dispose()


@pytest.mark.asyncio
async def test_response_loss_after_admission_replays_known_run_before_binding(
    tmp_path,
    monkeypatch,
) -> None:
    """A lost bind response reruns admission, never graph/model execution."""

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'receipts.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all, tables=[InboundReceiptRow.__table__])
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime(2026, 8, 8, 12, 0, tzinfo=UTC)
    current = [now]
    store = SqlInboundReceiptStore(sessions, clock=lambda: current[0])
    (received,) = await store.receive_batch(
        (
            _candidate(
                _envelope(
                    delivery_id="delivery-response-loss",
                    created_at=now.timestamp(),
                )
            ),
        )
    )
    admitted_runs: dict[str, str] = {}
    calls = 0
    graph_starts = 0

    async def admission(message: InboundMessage) -> InboundProcessingResult:
        nonlocal calls, graph_starts
        calls += 1
        key = message.metadata["github"]["delivery_id"]
        run_id = admitted_runs.get(key)
        if run_id is None:
            graph_starts += 1
            run_id = "run-response-loss"
            admitted_runs[key] = run_id
        return InboundProcessingResult(
            disposition=InboundProcessingDisposition.admitted,
            run_id=run_id,
            outcome_code="admitted",
        )

    async def no_wakeup(_wakeup) -> None:
        return None

    first = InboundReceiptProcessor(
        store=store,
        publish_wakeup=no_wakeup,
        process_message=admission,
        lease_owner="process-1",
        clock=lambda: current[0],
    )
    original_bind = store.bind_admitted

    async def lose_bind(*_args, **_kwargs) -> bool:
        raise RuntimeError("simulated process loss")

    monkeypatch.setattr(store, "bind_admitted", lose_bind)
    with pytest.raises(RuntimeError, match="process loss"):
        await first.process(received.receipt_id)

    monkeypatch.setattr(store, "bind_admitted", original_bind)
    current[0] += timedelta(seconds=31)
    restarted = InboundReceiptProcessor(
        store=store,
        publish_wakeup=no_wakeup,
        process_message=admission,
        lease_owner="process-2",
        clock=lambda: current[0],
    )
    await restarted.process(received.receipt_id)

    assert calls == 2
    assert graph_starts == 1
    async with sessions() as session:
        row = await session.get(InboundReceiptRow, received.receipt_id)
        assert row is not None
        assert row.state == "completed"
        assert row.run_id == "run-response-loss"

    await engine.dispose()


@pytest.mark.asyncio
async def test_poison_receipt_dead_letters_without_exposing_exception_text(
    tmp_path,
    caplog,
) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'receipts.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all, tables=[InboundReceiptRow.__table__])
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime(2026, 8, 8, 12, 0, tzinfo=UTC)
    store = SqlInboundReceiptStore(sessions, clock=lambda: now)
    (received,) = await store.receive_batch((_candidate(_envelope(delivery_id="delivery-poison", created_at=now.timestamp())),))

    async def poison(_message):
        raise RuntimeError("webhook secret=must-never-be-logged")

    async def no_wakeup(_wakeup) -> None:
        return None

    processor = InboundReceiptProcessor(
        store=store,
        publish_wakeup=no_wakeup,
        process_message=poison,
        lease_owner="gateway-poison",
        max_attempts=1,
        clock=lambda: now,
    )
    with caplog.at_level("WARNING", logger="app.channels.inbound_receipts"):
        await processor.process(received.receipt_id)

    async with sessions() as session:
        row = await session.get(InboundReceiptRow, received.receipt_id)
        assert row is not None
        assert row.state == "dead_letter"
        assert row.run_id is None
        assert row.outcome_code == "attempts_exhausted"
        assert row.failure_count == 1
    assert "must-never-be-logged" not in caplog.text
    assert "RuntimeError" in caplog.text

    await engine.dispose()


@pytest.mark.asyncio
async def test_command_or_rejection_completes_receipt_without_invented_run(tmp_path) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'receipts.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all, tables=[InboundReceiptRow.__table__])
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime(2026, 8, 8, 12, 0, tzinfo=UTC)
    store = SqlInboundReceiptStore(sessions, clock=lambda: now)
    (received,) = await store.receive_batch((_candidate(_envelope(delivery_id="delivery-rejected", created_at=now.timestamp())),))

    async def rejected(_message):
        return InboundProcessingResult(
            disposition=InboundProcessingDisposition.completed,
            outcome_code="identity_rejected",
        )

    async def no_wakeup(_wakeup) -> None:
        return None

    processor = InboundReceiptProcessor(
        store=store,
        publish_wakeup=no_wakeup,
        process_message=rejected,
        lease_owner="gateway-rejected",
        clock=lambda: now,
    )
    await processor.process(received.receipt_id)

    async with sessions() as session:
        row = await session.get(InboundReceiptRow, received.receipt_id)
        assert row is not None
        assert row.state == "completed"
        assert row.run_id is None
        assert row.outcome_code == "identity_rejected"

    await engine.dispose()


@pytest.mark.asyncio
async def test_processor_shutdown_cancels_owned_claim_and_leaves_it_reclaimable(tmp_path) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'receipts.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all, tables=[InboundReceiptRow.__table__])
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime(2026, 8, 8, 12, 0, tzinfo=UTC)
    current = [now]
    store = SqlInboundReceiptStore(sessions, clock=lambda: current[0])
    (received,) = await store.receive_batch((_candidate(_envelope(delivery_id="delivery-shutdown", created_at=now.timestamp())),))
    processing_started = asyncio.Event()

    async def hung_processing(_message):
        processing_started.set()
        await asyncio.Event().wait()

    async def no_wakeup(_wakeup) -> None:
        return None

    processor = InboundReceiptProcessor(
        store=store,
        publish_wakeup=no_wakeup,
        process_message=hung_processing,
        lease_owner="process-stopping",
        clock=lambda: current[0],
    )
    await processor.start()
    assert processor.schedule(received.receipt_id)
    await processing_started.wait()

    await processor.stop()
    assert not processor.schedule(received.receipt_id)

    current[0] += timedelta(seconds=31)
    restarted = SqlInboundReceiptStore(sessions, clock=lambda: current[0])
    claim = await restarted.claim(
        received.receipt_id,
        lease_owner="process-restarted",
        lease_seconds=30,
    )
    assert claim is not None
    assert claim.fencing_token == 2

    await engine.dispose()
