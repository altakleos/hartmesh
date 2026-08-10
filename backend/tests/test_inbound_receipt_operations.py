"""Bounded operator controls for durable native-ingress receipts."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from _router_auth_helpers import make_authed_test_app
from fastapi import HTTPException
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.channels.inbound_receipt_operations import (
    InboundDeadLetterInspection,
    InboundDeadLetterRequeueRequest,
    InboundReceiptOperations,
    InboundReceiptOperationsSummary,
    InboundReceiptStateSummary,
    inbound_receipt_operator_ref,
)
from app.channels.inbound_receipts import SqlInboundReceiptStore
from app.channels.service import ChannelService
from app.gateway.auth.models import User
from app.gateway.routers import channels
from deerflow.config.app_config import AppConfig
from deerflow.config.sandbox_config import SandboxConfig
from deerflow.persistence.base import Base
from deerflow.persistence.inbound_receipt.model import InboundReceiptRow


def test_operator_identity_is_a_domain_separated_pseudonymous_reference() -> None:
    user_id = UUID("11111111-2222-3333-4444-555555555555")

    assert inbound_receipt_operator_ref(user_id) == "039386540a8f586aa2f1278925c6f60804e45e054bee52233558af0caa9e955d"
    assert str(user_id) not in inbound_receipt_operator_ref(user_id)
    assert inbound_receipt_operator_ref(UUID("21111111-2222-3333-4444-555555555555")) != inbound_receipt_operator_ref(user_id)
    local_ref = inbound_receipt_operator_ref("default")
    assert len(local_ref) == 64
    assert "default" not in local_ref


@pytest.mark.parametrize(
    "receipt_id",
    (
        "00000000000000000000000000000001",
        "{00000000-0000-0000-0000-000000000001}",
        "00000000-0000-0000-0000-00000000000A",
        "00000000-0000-0000-0000-000000000001\n",
        "not-a-receipt",
    ),
)
def test_requeue_request_rejects_noncanonical_receipt_identity(receipt_id: str) -> None:
    with pytest.raises(ValueError, match="canonical lowercase UUID"):
        InboundDeadLetterRequeueRequest(
            receipt_id=receipt_id,
            expected_fencing_token=2,
            expected_payload_digest="a" * 64,
            expected_provider_event_digest="b" * 64,
        )


@pytest.mark.asyncio
async def test_summary_caps_each_state_and_reports_only_indexed_due_age(tmp_path) -> None:
    now = datetime(2026, 8, 10, 12, 0, tzinfo=UTC)
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'receipts.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all, tables=[InboundReceiptRow.__table__])
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions.begin() as session:
        session.add_all(
            [
                _row(
                    receipt_id=f"00000000-0000-0000-0000-00000000000{index}",
                    state="received",
                    received_at=now - timedelta(hours=age),
                    next_attempt_at=now - timedelta(seconds=due_age),
                )
                for index, (age, due_age) in enumerate(((90, 30), (30, 10), (10, -60)))
            ]
            + [
                _row(
                    receipt_id="00000000-0000-0000-0000-000000000009",
                    state="deferred",
                    received_at=now - timedelta(days=2),
                    next_attempt_at=now - timedelta(seconds=15),
                    outcome_code="thread_busy",
                ),
                _row(
                    receipt_id="00000000-0000-0000-0000-000000000010",
                    state="dead_letter",
                    received_at=now - timedelta(seconds=45),
                    completed_at=now - timedelta(seconds=5),
                    failure_count=8,
                    outcome_code="attempts_exhausted",
                ),
            ]
        )

    operations = InboundReceiptOperations(
        store=SqlInboundReceiptStore(sessions, clock=lambda: now),
        publish_wakeup=lambda _receipt_id: None,
        clock=lambda: now,
    )
    summary = await operations.summary(per_state_cap=2)

    received = summary.by_state("received")
    assert received.count == 2
    assert received.capped is True
    assert received.oldest_due_age_seconds == 30
    deferred = summary.by_state("deferred")
    assert deferred.count == 1
    assert deferred.capped is False
    assert deferred.oldest_due_age_seconds == 15
    dead_letter = summary.by_state("dead_letter")
    assert dead_letter.count == 1
    assert dead_letter.capped is False
    assert dead_letter.oldest_due_age_seconds is None
    completed = summary.by_state("completed")
    assert completed.count == 0
    assert completed.oldest_due_age_seconds is None

    await engine.dispose()


@pytest.mark.asyncio
async def test_exact_dead_letter_inspection_exposes_only_bounded_safe_evidence(tmp_path) -> None:
    now = datetime(2026, 8, 10, 12, 0, tzinfo=UTC)
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'receipts.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all, tables=[InboundReceiptRow.__table__])
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    dead_letter_id = "00000000-0000-0000-0000-000000000020"
    received_id = "00000000-0000-0000-0000-000000000021"
    async with sessions.begin() as session:
        dead_letter = _row(
            receipt_id=dead_letter_id,
            state="dead_letter",
            received_at=now - timedelta(minutes=5),
            completed_at=now,
            failure_count=8,
            outcome_code="attempts_exhausted",
        )
        dead_letter.payload_json = {
            "text": "secret receipt body",
            "binding": {"reference": "private-binding"},
        }
        session.add_all(
            [
                dead_letter,
                _row(receipt_id=received_id, state="received", received_at=now),
            ]
        )

    operations = InboundReceiptOperations(
        store=SqlInboundReceiptStore(sessions, clock=lambda: now),
        publish_wakeup=lambda _receipt_id: None,
        clock=lambda: now,
    )
    inspection = await operations.inspect_dead_letter(dead_letter_id)

    assert inspection is not None
    assert inspection.to_dict() == {
        "receipt_id": dead_letter_id,
        "state": "dead_letter",
        "provider": "github",
        "binding_kind": "webhook_route",
        "thread_id": "thread-17-reviewer",
        "payload_digest": "a" * 64,
        "provider_event_digest": "b" * 64,
        "fencing_token": 2,
        "attempt_count": 3,
        "failure_count": 8,
        "outcome_code": "attempts_exhausted",
        "received_at": "2026-08-10T11:55:00Z",
        "updated_at": "2026-08-10T11:55:00Z",
        "completed_at": "2026-08-10T12:00:00Z",
    }
    assert "secret receipt body" not in repr(inspection)
    assert "private-binding" not in repr(inspection)
    assert await operations.inspect_dead_letter(received_id) is None
    assert await operations.inspect_dead_letter("00000000-0000-0000-0000-000000000099") is None

    await engine.dispose()


@pytest.mark.asyncio
async def test_fenced_exact_dead_letter_requeue_preserves_identity_and_wakes_after_commit(
    tmp_path,
    caplog,
) -> None:
    now = datetime(2026, 8, 10, 12, 0, tzinfo=UTC)
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'receipts.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all, tables=[InboundReceiptRow.__table__])
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    receipt_id = "00000000-0000-0000-0000-000000000030"
    async with sessions.begin() as session:
        row = _row(
            receipt_id=receipt_id,
            state="dead_letter",
            received_at=now - timedelta(minutes=5),
            completed_at=now,
            failure_count=8,
            outcome_code="attempts_exhausted",
        )
        row.payload_json = {"text": "secret receipt body"}
        session.add(row)

    store = SqlInboundReceiptStore(sessions, clock=lambda: now)
    wakeups: list[str] = []

    async def publish_wakeup(committed_receipt_id: str) -> None:
        # The wake-up must happen after the CAS transaction commits.
        async with sessions() as session:
            committed = await session.get(InboundReceiptRow, committed_receipt_id)
            assert committed is not None
            assert committed.state == "deferred"
        wakeups.append(committed_receipt_id)

    operations = InboundReceiptOperations(
        store=store,
        publish_wakeup=publish_wakeup,
        clock=lambda: now,
    )
    with caplog.at_level("INFO", logger="app.channels.inbound_receipt_operations"):
        result = await operations.requeue_dead_letter(
            InboundDeadLetterRequeueRequest(
                receipt_id=receipt_id,
                expected_fencing_token=2,
                expected_payload_digest="a" * 64,
                expected_provider_event_digest="b" * 64,
            ),
            actor_ref="c" * 64,
        )

    assert result.disposition == "requeued"
    assert result.fencing_token == 3
    assert result.wakeup_published is True
    assert wakeups == [receipt_id]
    async with sessions() as session:
        row = await session.get(InboundReceiptRow, receipt_id)
        assert row is not None
        assert row.state == "deferred"
        assert row.fencing_token == 3
        assert row.attempt_count == 3
        assert row.failure_count == 0
        assert row.payload_json == {"text": "secret receipt body"}
        assert row.payload_digest == "a" * 64
        assert row.provider_event_digest == "b" * 64
        assert row.run_id is None
        assert row.completed_at is None
        assert row.outcome_code == "operator_requeued"
    assert "receipt_operation_requeue" in caplog.text
    assert receipt_id in caplog.text
    assert result.correlation_id in caplog.text
    assert "actor_ref=" + ("c" * 64) in caplog.text
    assert "secret receipt body" not in caplog.text

    await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("state", "run_id", "expected_fencing_token", "expected_payload_digest"),
    [
        ("completed", None, 2, "a" * 64),
        ("dead_letter", "run-already-bound", 2, "a" * 64),
        ("dead_letter", None, 1, "a" * 64),
        ("dead_letter", None, 2, "c" * 64),
    ],
    ids=("not-dead-letter", "run-bound", "stale-fence", "payload-mismatch"),
)
async def test_requeue_fails_closed_unless_every_exact_row_fence_matches(
    tmp_path,
    state,
    run_id,
    expected_fencing_token,
    expected_payload_digest,
) -> None:
    now = datetime(2026, 8, 10, 12, 0, tzinfo=UTC)
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'receipts.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all, tables=[InboundReceiptRow.__table__])
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    receipt_id = "00000000-0000-0000-0000-000000000040"
    async with sessions.begin() as session:
        row = _row(
            receipt_id=receipt_id,
            state=state,
            received_at=now - timedelta(minutes=5),
            completed_at=now,
            failure_count=8,
            outcome_code="attempts_exhausted",
        )
        row.run_id = run_id
        session.add(row)

    wakeups: list[str] = []
    operations = InboundReceiptOperations(
        store=SqlInboundReceiptStore(sessions, clock=lambda: now),
        publish_wakeup=wakeups.append,
        clock=lambda: now,
    )
    result = await operations.requeue_dead_letter(
        InboundDeadLetterRequeueRequest(
            receipt_id=receipt_id,
            expected_fencing_token=expected_fencing_token,
            expected_payload_digest=expected_payload_digest,
            expected_provider_event_digest="b" * 64,
        ),
        actor_ref="c" * 64,
    )

    assert result.disposition == "not_requeued"
    assert result.fencing_token is None
    assert result.wakeup_published is False
    assert wakeups == []
    async with sessions() as session:
        retained = await session.get(InboundReceiptRow, receipt_id)
        assert retained is not None
        assert retained.state == state
        assert retained.fencing_token == 2
        assert retained.failure_count == 8

    await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("stored_event_digest", "expected_event_digest", "expected_disposition"),
    [
        ("b" * 64, None, "not_requeued"),
        ("b" * 64, "c" * 64, "not_requeued"),
        (None, None, "requeued"),
        (None, "b" * 64, "not_requeued"),
    ],
    ids=("nonlegacy-omitted", "nonlegacy-mismatch", "legacy-omitted", "legacy-invented"),
)
async def test_requeue_requires_exact_provider_event_evidence_without_inventing_legacy_proof(
    tmp_path,
    stored_event_digest,
    expected_event_digest,
    expected_disposition,
) -> None:
    now = datetime(2026, 8, 10, 12, 0, tzinfo=UTC)
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'receipts.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all, tables=[InboundReceiptRow.__table__])
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    receipt_id = "00000000-0000-0000-0000-000000000045"
    async with sessions.begin() as session:
        row = _row(
            receipt_id=receipt_id,
            state="dead_letter",
            received_at=now - timedelta(minutes=5),
            completed_at=now,
            failure_count=8,
            outcome_code="attempts_exhausted",
        )
        row.provider_event_digest = stored_event_digest
        session.add(row)

    operations = InboundReceiptOperations(
        store=SqlInboundReceiptStore(sessions, clock=lambda: now),
        publish_wakeup=lambda _receipt_id: None,
        clock=lambda: now,
    )
    result = await operations.requeue_dead_letter(
        InboundDeadLetterRequeueRequest(
            receipt_id=receipt_id,
            expected_fencing_token=2,
            expected_payload_digest="a" * 64,
            expected_provider_event_digest=expected_event_digest,
        ),
        actor_ref="c" * 64,
    )

    assert result.disposition == expected_disposition
    async with sessions() as session:
        retained = await session.get(InboundReceiptRow, receipt_id)
        assert retained is not None
        assert retained.state == ("deferred" if expected_disposition == "requeued" else "dead_letter")
        assert retained.provider_event_digest == stored_event_digest

    await engine.dispose()


@pytest.mark.asyncio
async def test_retention_cleanup_never_deletes_unresolved_dead_letters(tmp_path) -> None:
    now = datetime(2026, 8, 10, 12, 0, tzinfo=UTC)
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'receipts.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all, tables=[InboundReceiptRow.__table__])
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    completed_id = "00000000-0000-0000-0000-000000000050"
    dead_letter_id = "00000000-0000-0000-0000-000000000051"
    old = now - timedelta(days=30)
    async with sessions.begin() as session:
        session.add_all(
            [
                _row(
                    receipt_id=completed_id,
                    state="completed",
                    received_at=old,
                    completed_at=old,
                    outcome_code="admitted",
                ),
                _row(
                    receipt_id=dead_letter_id,
                    state="dead_letter",
                    received_at=old,
                    completed_at=old,
                    failure_count=8,
                    outcome_code="attempts_exhausted",
                ),
            ]
        )

    store = SqlInboundReceiptStore(sessions, clock=lambda: now)
    deleted = await store.cleanup_completed(
        older_than=now - timedelta(days=7),
        limit=10,
    )

    assert deleted == 1
    async with sessions() as session:
        assert await session.get(InboundReceiptRow, completed_id) is None
        dead_letter = await session.get(InboundReceiptRow, dead_letter_id)
        assert dead_letter is not None
        assert dead_letter.state == "dead_letter"

    await engine.dispose()


@pytest.mark.asyncio
async def test_concurrent_exact_requeues_have_one_fenced_winner(tmp_path) -> None:
    now = datetime(2026, 8, 10, 12, 0, tzinfo=UTC)
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'receipts.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all, tables=[InboundReceiptRow.__table__])
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    receipt_id = "00000000-0000-0000-0000-000000000052"
    async with sessions.begin() as session:
        session.add(
            _row(
                receipt_id=receipt_id,
                state="dead_letter",
                received_at=now - timedelta(days=30),
                completed_at=now,
                failure_count=8,
                outcome_code="attempts_exhausted",
            )
        )

    wakeups: list[str] = []
    request = InboundDeadLetterRequeueRequest(
        receipt_id=receipt_id,
        expected_fencing_token=2,
        expected_payload_digest="a" * 64,
        expected_provider_event_digest="b" * 64,
    )
    first = InboundReceiptOperations(
        store=SqlInboundReceiptStore(sessions, clock=lambda: now),
        publish_wakeup=wakeups.append,
        clock=lambda: now,
    )
    second = InboundReceiptOperations(
        store=SqlInboundReceiptStore(sessions, clock=lambda: now),
        publish_wakeup=wakeups.append,
        clock=lambda: now,
    )

    results = await asyncio.gather(
        first.requeue_dead_letter(request, actor_ref="c" * 64),
        second.requeue_dead_letter(request, actor_ref="d" * 64),
    )

    assert sorted(result.disposition for result in results) == [
        "not_requeued",
        "requeued",
    ]
    assert wakeups == [receipt_id]
    async with sessions() as session:
        row = await session.get(InboundReceiptRow, receipt_id)
        assert row is not None
        assert row.state == "deferred"
        assert row.fencing_token == 3

    await engine.dispose()


def _row(
    *,
    receipt_id: str,
    state: str,
    received_at: datetime,
    next_attempt_at: datetime | None = None,
    completed_at: datetime | None = None,
    failure_count: int = 0,
    outcome_code: str | None = None,
) -> InboundReceiptRow:
    return InboundReceiptRow(
        receipt_id=receipt_id,
        provider="github",
        binding_kind="webhook_route",
        binding_reference="route:v1:sha256:" + ("a" * 64),
        provider_delivery_id=f"delivery-{receipt_id}",
        thread_id="thread-17-reviewer",
        payload_json={"redacted": "test-only"},
        payload_digest="a" * 64,
        provider_event_digest="b" * 64,
        state=state,
        fencing_token=2,
        attempt_count=3,
        failure_count=failure_count,
        next_attempt_at=next_attempt_at or received_at,
        outcome_code=outcome_code,
        received_at=received_at,
        updated_at=received_at,
        completed_at=completed_at,
    )


def _user(*, role: str) -> User:
    return User(
        id=UUID("11111111-2222-3333-4444-555555555555"),
        email=f"{role}@example.com",
        password_hash="x",
        system_role=role,
    )


def test_receipt_operator_routes_require_an_administrator(monkeypatch) -> None:
    operations = SimpleNamespace(
        summary=AsyncMock(side_effect=AssertionError("must not be called")),
        inspect_dead_letter=AsyncMock(side_effect=AssertionError("must not be called")),
        requeue_dead_letter=AsyncMock(side_effect=AssertionError("must not be called")),
    )
    monkeypatch.setattr(
        "app.channels.service.get_channel_service",
        lambda: SimpleNamespace(inbound_receipt_operations=operations),
    )
    app = make_authed_test_app(user_factory=lambda: _user(role="user"))
    app.include_router(channels.router)

    with TestClient(app) as client:
        summary = client.get("/api/channels/inbound-receipts/summary")
        inspection = client.get("/api/channels/inbound-receipts/00000000-0000-0000-0000-000000000060")
        requeue = client.post(
            "/api/channels/inbound-receipts/00000000-0000-0000-0000-000000000060/requeue",
            json={
                "expected_fencing_token": 2,
                "expected_payload_digest": "a" * 64,
            },
        )

    assert summary.status_code == 403
    assert inspection.status_code == 403
    assert requeue.status_code == 403
    operations.summary.assert_not_awaited()
    operations.inspect_dead_letter.assert_not_awaited()
    operations.requeue_dead_letter.assert_not_awaited()


def test_admin_receipt_routes_are_bounded_exact_id_surfaces(monkeypatch, caplog) -> None:
    now = datetime(2026, 8, 10, 12, 0, tzinfo=UTC)

    class Store:
        async def summarize_states(self, *, per_state_cap, observed_at):
            return InboundReceiptOperationsSummary(
                generated_at=observed_at,
                per_state_cap=per_state_cap,
                states=(
                    InboundReceiptStateSummary(
                        state="dead_letter",
                        count=2,
                        capped=True,
                        oldest_due_age_seconds=None,
                    ),
                ),
            )

        async def inspect_dead_letter(self, receipt_id):
            return InboundDeadLetterInspection(
                receipt_id=receipt_id,
                provider="github",
                binding_kind="webhook_route",
                thread_id="thread-17-reviewer",
                payload_digest="a" * 64,
                provider_event_digest="b" * 64,
                fencing_token=2,
                attempt_count=3,
                failure_count=8,
                outcome_code="attempts_exhausted",
                received_at=now - timedelta(seconds=45),
                updated_at=now,
                completed_at=now,
            )

        async def requeue_dead_letter(self, request, *, requeued_at):
            assert request.expected_fencing_token == 2
            assert request.expected_payload_digest == "a" * 64
            assert request.expected_provider_event_digest == "b" * 64
            return 3

    wakeups: list[str] = []
    operations = InboundReceiptOperations(
        store=Store(),
        publish_wakeup=wakeups.append,
        clock=lambda: now,
    )
    monkeypatch.setattr(
        "app.channels.service.get_channel_service",
        lambda: SimpleNamespace(inbound_receipt_operations=operations),
    )
    app = make_authed_test_app(user_factory=lambda: _user(role="admin"))
    app.include_router(channels.router)
    receipt_id = "00000000-0000-0000-0000-000000000061"

    with caplog.at_level("INFO", logger="app.channels.inbound_receipt_operations"):
        with TestClient(app) as client:
            summary = client.get(
                "/api/channels/inbound-receipts/summary",
                params={"per_state_cap": 2},
            )
            inspection = client.get(f"/api/channels/inbound-receipts/{receipt_id}")
            requeue = client.post(
                f"/api/channels/inbound-receipts/{receipt_id}/requeue",
                json={
                    "expected_fencing_token": 2,
                    "expected_payload_digest": "a" * 64,
                    "expected_provider_event_digest": "b" * 64,
                },
            )
            unknown_field = client.post(
                f"/api/channels/inbound-receipts/{receipt_id}/requeue",
                json={
                    "expected_fencing_token": 2,
                    "expected_payload_digest": "a" * 64,
                    "expected_provider_event_digest": "b" * 64,
                    "payload": "must-not-be-accepted",
                },
            )

    assert summary.status_code == 200
    assert summary.json()["per_state_cap"] == 2
    assert summary.json()["states"] == [
        {
            "state": "dead_letter",
            "count": 2,
            "capped": True,
            "oldest_due_age_seconds": None,
        }
    ]
    assert inspection.status_code == 200
    assert set(inspection.json()) == {
        "receipt_id",
        "state",
        "provider",
        "binding_kind",
        "thread_id",
        "payload_digest",
        "provider_event_digest",
        "fencing_token",
        "attempt_count",
        "failure_count",
        "outcome_code",
        "received_at",
        "updated_at",
        "completed_at",
    }
    assert "text" not in inspection.json()
    assert "binding_reference" not in inspection.json()
    assert requeue.status_code == 200
    assert requeue.json()["disposition"] == "requeued"
    assert requeue.json()["fencing_token"] == 3
    assert "actor_ref" not in requeue.json()
    assert wakeups == [receipt_id]
    assert unknown_field.status_code == 422
    assert "actor_ref=039386540a8f586aa2f1278925c6f60804e45e054bee52233558af0caa9e955d" in caplog.text
    assert "11111111-2222-3333-4444-555555555555" not in caplog.text
    assert "admin@example.com" not in caplog.text


@pytest.mark.parametrize(
    ("operation", "method", "path", "json_body"),
    [
        ("summary", "get", "/api/channels/inbound-receipts/summary", None),
        (
            "inspect",
            "get",
            "/api/channels/inbound-receipts/00000000-0000-0000-0000-000000000071",
            None,
        ),
        (
            "requeue",
            "post",
            "/api/channels/inbound-receipts/00000000-0000-0000-0000-000000000071/requeue",
            {
                "expected_fencing_token": 2,
                "expected_payload_digest": "a" * 64,
                "expected_provider_event_digest": "b" * 64,
            },
        ),
    ],
)
def test_receipt_operator_routes_redact_unexpected_failures(
    monkeypatch,
    caplog,
    operation,
    method,
    path,
    json_body,
) -> None:
    marker = "marker-secret-provider-message"

    class Store:
        async def summarize_states(self, **_kwargs):
            raise RuntimeError(marker)

        async def inspect_dead_letter(self, _receipt_id):
            raise RuntimeError(marker)

        async def requeue_dead_letter(self, _request, **_kwargs):
            raise RuntimeError(marker)

    operations = InboundReceiptOperations(
        store=Store(),
        publish_wakeup=lambda _receipt_id: None,
    )
    monkeypatch.setattr(
        "app.channels.service.get_channel_service",
        lambda: SimpleNamespace(inbound_receipt_operations=operations),
    )
    app = make_authed_test_app(user_factory=lambda: _user(role="admin"))
    app.include_router(channels.router)

    with caplog.at_level("WARNING", logger="app.gateway.routers.channels"):
        with TestClient(app) as client:
            response = client.request(method, path, json=json_body)

    assert response.status_code == 503
    assert response.headers["content-type"] == "application/json"
    detail = response.json()["detail"]
    assert detail["code"] == "inbound_receipt_operations_unavailable"
    UUID(detail["correlation_id"])
    assert marker not in response.text
    assert marker not in caplog.text
    assert "exception_class=RuntimeError" in caplog.text
    assert f"operation={operation}" in caplog.text
    assert "actor_ref=039386540a8f586aa2f1278925c6f60804e45e054bee52233558af0caa9e955d" in caplog.text


def test_receipt_operator_unavailable_service_uses_the_same_redacted_503(
    monkeypatch,
    caplog,
) -> None:
    monkeypatch.setattr("app.channels.service.get_channel_service", lambda: None)
    app = make_authed_test_app(user_factory=lambda: _user(role="admin"))
    app.include_router(channels.router)

    with caplog.at_level("WARNING", logger="app.gateway.routers.channels"):
        with TestClient(app) as client:
            response = client.get("/api/channels/inbound-receipts/summary")

    assert response.status_code == 503
    detail = response.json()["detail"]
    assert detail["code"] == "inbound_receipt_operations_unavailable"
    UUID(detail["correlation_id"])
    assert "exception_class=RuntimeError" in caplog.text
    assert "inbound receipt operations are unavailable" not in response.text


def test_receipt_operator_serialization_failure_uses_redacted_503(
    monkeypatch,
    caplog,
) -> None:
    marker = "marker-secret-malformed-store-result"

    class MalformedSummary:
        def to_dict(self):
            raise RuntimeError(marker)

    class Store:
        async def summarize_states(self, **_kwargs):
            return MalformedSummary()

    monkeypatch.setattr(
        "app.channels.service.get_channel_service",
        lambda: SimpleNamespace(
            inbound_receipt_operations=InboundReceiptOperations(
                store=Store(),
                publish_wakeup=lambda _receipt_id: None,
            )
        ),
    )
    app = make_authed_test_app(user_factory=lambda: _user(role="admin"))
    app.include_router(channels.router)

    with caplog.at_level("WARNING", logger="app.gateway.routers.channels"):
        with TestClient(app) as client:
            response = client.get("/api/channels/inbound-receipts/summary")

    assert response.status_code == 503
    assert marker not in response.text
    assert marker not in caplog.text


def test_receipt_store_cannot_forge_a_public_http_error(monkeypatch, caplog) -> None:
    marker = "marker-secret-forged-http-error"

    class Store:
        async def inspect_dead_letter(self, _receipt_id):
            raise HTTPException(status_code=418, detail=marker)

    monkeypatch.setattr(
        "app.channels.service.get_channel_service",
        lambda: SimpleNamespace(
            inbound_receipt_operations=InboundReceiptOperations(
                store=Store(),
                publish_wakeup=lambda _receipt_id: None,
            )
        ),
    )
    app = make_authed_test_app(user_factory=lambda: _user(role="admin"))
    app.include_router(channels.router)

    with caplog.at_level("WARNING", logger="app.gateway.routers.channels"):
        with TestClient(app) as client:
            response = client.get(
                "/api/channels/inbound-receipts/00000000-0000-0000-0000-000000000073",
            )

    assert response.status_code == 503
    assert response.json()["detail"]["code"] == "inbound_receipt_operations_unavailable"
    assert marker not in response.text
    assert marker not in caplog.text


def test_malformed_receipt_path_cannot_inject_operator_logs(monkeypatch, caplog) -> None:
    marker = "marker-secret-log-injection"
    operations = InboundReceiptOperations(
        store=SimpleNamespace(),
        publish_wakeup=lambda _receipt_id: None,
    )
    monkeypatch.setattr(
        "app.channels.service.get_channel_service",
        lambda: SimpleNamespace(inbound_receipt_operations=operations),
    )
    app = make_authed_test_app(user_factory=lambda: _user(role="admin"))
    app.include_router(channels.router)

    with caplog.at_level("INFO"):
        with TestClient(app) as client:
            response = client.get(
                "/api/channels/inbound-receipts/not-a-uuid%0A" + marker,
            )

    assert response.status_code == 422
    assert response.json() == {"detail": "Invalid inbound receipt identity"}
    business_log = "\n".join(record.getMessage() for record in caplog.records if record.name.startswith(("app.gateway.routers.channels", "app.channels")))
    assert marker not in business_log


def test_postgres_channel_service_wires_operator_operations_to_receipt_store(
    monkeypatch,
) -> None:
    session_factory = object()
    monkeypatch.setattr(
        "deerflow.persistence.engine.get_session_factory",
        lambda: session_factory,
    )
    raw = AppConfig(sandbox=SandboxConfig(use="test")).model_dump(mode="python")
    raw["database"]["backend"] = "postgres"
    raw["database"]["postgres_url"] = "postgresql://user:password@db/hartmesh"
    config = AppConfig.model_validate(raw)

    service = ChannelService(channels_config={}, app_config=config)

    assert isinstance(service.inbound_receipt_operations, InboundReceiptOperations)
    assert service.inbound_receipt_processor is not None
