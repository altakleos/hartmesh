from __future__ import annotations

from datetime import UTC, datetime

import pytest

from deerflow.runtime.runs.lifecycle_query import (
    InvalidToolReceiptCursor,
    LifecyclePage,
    LifecycleQuery,
    build_tool_receipt_page,
    decode_tool_receipt_cursor,
    encode_lifecycle_cursor,
    encode_tool_receipt_cursor,
)
from deerflow.runtime.runs.manager import RunManager
from deerflow.runtime.tool_evidence import (
    DurableToolReceiptV1,
    ToolAttemptContextV1,
    receipt_event_metadata,
)


def _context(call: str, *, attempt: int = 1) -> ToolAttemptContextV1:
    return ToolAttemptContextV1(
        run_id="run-1",
        execution_task_id="run-1",
        execution_kind="lead",
        subagent_name=None,
        tool_call_id=call,
        attempt=attempt,
        owner_id="worker-1",
        lease_epoch=3,
        agent_revision_digest="a" * 64,
        assembly_fingerprint="b" * 64,
        extension_generation=2,
        subagent_catalog_digest="c" * 64,
        subagent_definition_digest=None,
    )


def _events(call: str, seq: int, *, terminal: str | None = "succeeded") -> list[dict]:
    started = DurableToolReceiptV1.started(
        context=_context(call),
        tool_name="web_search",
        request_projection_digest="d" * 64,
        occurred_at=datetime(2026, 8, 30, tzinfo=UTC),
    )
    records = [
        {
            "thread_id": "thread-1",
            "run_id": "run-1",
            "event_type": "tool_receipt.started.v1",
            "idempotency_key": started.idempotency_key,
            "category": "tool",
            "content": started.to_event_body(),
            "metadata": receipt_event_metadata(
                started,
                writer_owner_id="worker-1",
                writer_lease_epoch=3,
            ),
            "seq": seq,
            "created_at": "2026-08-30T00:00:00+00:00",
        }
    ]
    if terminal is not None:
        outcome = started.outcome(
            phase=terminal,  # type: ignore[arg-type]
            result_projection_digest="e" * 64 if terminal == "succeeded" else None,
            result_kind="tool_message" if terminal == "succeeded" else None,
            safe_error_code=None if terminal == "succeeded" else "tool_error",
            authz_decision_ref="pd_" + "f" * 64,
        )
        records.append(
            {
                "thread_id": "thread-1",
                "run_id": "run-1",
                "event_type": "tool_receipt.outcome.v1",
                "idempotency_key": outcome.idempotency_key,
                "category": "tool",
                "content": outcome.to_event_body(),
                "metadata": receipt_event_metadata(
                    outcome,
                    writer_owner_id="worker-1",
                    writer_lease_epoch=3,
                ),
                "seq": seq + 1,
                "created_at": "2026-08-30T00:00:01+00:00",
            }
        )
    return records


def test_pairs_start_and_outcome_and_keeps_crash_gap_indeterminate() -> None:
    page = build_tool_receipt_page(
        [*_events("call-1", 1), *_events("call-2", 3, terminal=None)],
        run_id="run-1",
        thread_id="thread-1",
        cursor=None,
        limit=100,
        legacy_unavailable=False,
    )

    assert [item.status for item in page.items] == ["succeeded", "indeterminate"]
    assert page.items[0].started_at == "2026-08-30T00:00:00+00:00"
    assert page.items[0].finished_at == "2026-08-30T00:00:01+00:00"
    assert page.items[1].finished_at is None
    assert page.evidence_status == "available"


def test_receipt_cursor_is_strictly_scoped_and_page_is_bounded() -> None:
    events = []
    for index in range(102):
        events.extend(_events(f"call-{index}", index * 2 + 1))

    page = build_tool_receipt_page(
        events,
        run_id="run-1",
        thread_id="thread-1",
        cursor=None,
        limit=100,
        legacy_unavailable=False,
    )
    assert len(page.items) == 100
    assert page.next_cursor is not None
    after_seq = decode_tool_receipt_cursor(page.next_cursor, run_id="run-1", thread_id="thread-1")
    assert after_seq == 199
    with pytest.raises(InvalidToolReceiptCursor):
        decode_tool_receipt_cursor(page.next_cursor, run_id="another-run", thread_id="thread-1")
    with pytest.raises(InvalidToolReceiptCursor):
        decode_tool_receipt_cursor(page.next_cursor[:-1] + "x", run_id="run-1", thread_id="thread-1")
    with pytest.raises(ValueError, match="between 1 and 100"):
        build_tool_receipt_page(events, run_id="run-1", thread_id="thread-1", cursor=None, limit=101, legacy_unavailable=False)


def test_corrupt_events_are_contained_without_raw_payload_echo() -> None:
    events = _events("call-1", 1)
    events.append(
        {
            "thread_id": "thread-1",
            "run_id": "run-1",
            "event_type": "tool_receipt.started.v1",
            "category": "tool",
            "content": {"raw_secret": "never echo me"},
            "metadata": {},
            "seq": 3,
            "created_at": "2026-08-30T00:00:02+00:00",
        }
    )

    page = build_tool_receipt_page(
        events,
        run_id="run-1",
        thread_id="thread-1",
        cursor=None,
        limit=100,
        legacy_unavailable=False,
    )
    assert page.evidence_status == "invalid"
    assert page.invalid_event_count == 1
    assert "never echo me" not in str(page)


@pytest.mark.parametrize(
    "metadata_change",
    [
        {"tool_call_id": "different-call"},
        {"dispatch_generation_digest": "0" * 64},
    ],
)
def test_mismatched_receipt_metadata_is_contained_as_invalid(
    metadata_change: dict[str, object],
) -> None:
    events = _events("call-1", 1)
    events[0]["metadata"] = {
        **events[0]["metadata"],
        **metadata_change,
    }

    page = build_tool_receipt_page(
        events,
        run_id="run-1",
        thread_id="thread-1",
        cursor=None,
        limit=100,
        legacy_unavailable=False,
    )

    assert page.items == ()
    assert page.evidence_status == "invalid"
    assert page.invalid_event_count == 2


def test_old_run_reports_legacy_unavailable() -> None:
    page = build_tool_receipt_page(
        [],
        run_id="run-1",
        thread_id="thread-1",
        cursor=None,
        limit=100,
        legacy_unavailable=True,
    )
    assert page.items == ()
    assert page.evidence_status == "legacy_unavailable"
    assert page.pruned_before is None


def test_old_run_never_projects_partial_tail_receipts() -> None:
    page = build_tool_receipt_page(
        _events("call-tail", 1),
        run_id="run-1",
        thread_id="thread-1",
        cursor=None,
        limit=100,
        legacy_unavailable=True,
    )

    assert page.items == ()
    assert page.evidence_status == "legacy_unavailable"
    assert page.invalid_event_count == 0


def test_receipt_cursor_encoder_rejects_invalid_scope() -> None:
    token = encode_tool_receipt_cursor(run_id="run-1", thread_id="thread-1", after_seq=5)
    assert decode_tool_receipt_cursor(token, run_id="run-1", thread_id="thread-1") == 5


@pytest.mark.anyio
async def test_run_manager_attaches_receipts_only_for_an_explicit_exact_run_query(
    monkeypatch,
) -> None:
    class Store:
        durable_lifecycle = True

        async def query_lifecycle(self, query):
            assert query.run_id == "run-1"
            cursor = encode_lifecycle_cursor(0)
            return LifecyclePage(
                snapshots=(
                    {
                        "run_id": "run-1",
                        "thread_id": "thread-1",
                        "status": "success",
                        "state_version": 2,
                    },
                ),
                events=(),
                next_cursor=cursor,
                minimum_available_cursor=cursor,
                read_fence_cursor=cursor,
            )

        async def get(self, run_id, *, user_id=None):
            assert run_id == "run-1" and user_id is None
            return {
                "run_id": "run-1",
                "thread_id": "thread-1",
                "status": "success",
                "state_version": 2,
                "operation_kind": "run",
                "decision_evidence_json": {"tool_receipts": {"version": 1}},
            }

    class EventStore:
        async def list_events(self, thread_id, run_id, **kwargs):
            assert (thread_id, run_id) == ("thread-1", "run-1")
            assert kwargs["user_id"] is None
            return _events("call-1", 1)

    monkeypatch.setattr(
        "deerflow.runtime.runs.lifecycle_query.build_invocation_summary",
        lambda _row: {"assembly_evidence_status": "verified"},
    )
    manager = RunManager(store=Store(), event_store=EventStore())  # type: ignore[arg-type]

    page = await manager.query_lifecycle(
        LifecycleQuery(
            run_id="run-1",
            include_tool_receipts=True,
            tool_receipt_limit=10,
        )
    )

    assert page.tool_receipts is not None
    assert [item.status for item in page.tool_receipts.items] == ["succeeded"]


@pytest.mark.anyio
async def test_manager_receipt_cursor_ignores_a_prior_page_terminal(
    monkeypatch,
) -> None:
    all_events = [*_events("call-1", 1), *_events("call-2", 3), *_events("call-3", 5)]

    class Store:
        durable_lifecycle = True

        async def query_lifecycle(self, _query):
            cursor = encode_lifecycle_cursor(0)
            return LifecyclePage(
                snapshots=(
                    {
                        "run_id": "run-1",
                        "thread_id": "thread-1",
                        "status": "success",
                        "state_version": 2,
                    },
                ),
                events=(),
                next_cursor=cursor,
                minimum_available_cursor=cursor,
                read_fence_cursor=cursor,
            )

        async def get(self, _run_id, *, user_id=None):
            assert user_id is None
            return {
                "run_id": "run-1",
                "thread_id": "thread-1",
                "status": "success",
                "state_version": 2,
                "operation_kind": "run",
                "decision_evidence_json": {"tool_receipts": {"version": 1}},
            }

    class EventStore:
        async def list_events(self, _thread_id, _run_id, **kwargs):
            after_seq = kwargs.get("after_seq")
            return [event for event in all_events if after_seq is None or event["seq"] > after_seq]

    monkeypatch.setattr(
        "deerflow.runtime.runs.lifecycle_query.build_invocation_summary",
        lambda _row: {"assembly_evidence_status": "verified"},
    )
    manager = RunManager(store=Store(), event_store=EventStore())  # type: ignore[arg-type]
    first = await manager.query_lifecycle(LifecycleQuery(run_id="run-1", include_tool_receipts=True, tool_receipt_limit=1))
    assert first.tool_receipts is not None and first.tool_receipts.next_cursor is not None

    second = await manager.query_lifecycle(
        LifecycleQuery(
            run_id="run-1",
            include_tool_receipts=True,
            tool_receipt_limit=1,
            tool_receipt_cursor=first.tool_receipts.next_cursor,
        )
    )

    assert second.tool_receipts is not None
    assert [item.status for item in second.tool_receipts.items] == ["succeeded"]
    assert second.tool_receipts.evidence_status == "available"
    assert second.tool_receipts.invalid_event_count == 0


@pytest.mark.anyio
async def test_manager_does_not_read_receipts_without_a_visible_snapshot() -> None:
    class Store:
        durable_lifecycle = True

        async def query_lifecycle(self, _query):
            cursor = encode_lifecycle_cursor(0)
            return LifecyclePage(
                snapshots=(),
                events=(),
                next_cursor=cursor,
                minimum_available_cursor=cursor,
                read_fence_cursor=cursor,
            )

    class EventStore:
        async def list_events(self, *_args, **_kwargs):
            raise AssertionError("invisible receipt evidence must not be read")

    page = await RunManager(store=Store(), event_store=EventStore()).query_lifecycle(  # type: ignore[arg-type]
        LifecycleQuery(run_id="run-1", include_tool_receipts=True)
    )

    assert page.tool_receipts is None


@pytest.mark.anyio
async def test_verified_pre_receipt_run_is_still_legacy_unavailable(
    monkeypatch,
) -> None:
    class Store:
        durable_lifecycle = True

        async def query_lifecycle(self, _query):
            cursor = encode_lifecycle_cursor(0)
            return LifecyclePage(
                snapshots=(
                    {
                        "run_id": "run-1",
                        "thread_id": "thread-1",
                        "status": "success",
                        "state_version": 2,
                    },
                ),
                events=(),
                next_cursor=cursor,
                minimum_available_cursor=cursor,
                read_fence_cursor=cursor,
            )

        async def get(self, _run_id, *, user_id=None):
            assert user_id is None
            return {
                "run_id": "run-1",
                "thread_id": "thread-1",
                "decision_evidence_json": {"version": 1, "decisions": []},
            }

    class EventStore:
        async def list_events(self, *_args, **_kwargs):
            raise AssertionError("legacy runs must not read or project tail receipts")

    monkeypatch.setattr(
        "deerflow.runtime.runs.lifecycle_query.build_invocation_summary",
        lambda _row: {"assembly_evidence_status": "verified"},
    )
    page = await RunManager(  # type: ignore[arg-type]
        store=Store(),
        event_store=EventStore(),
    ).query_lifecycle(LifecycleQuery(run_id="run-1", include_tool_receipts=True))

    assert page.tool_receipts is not None
    assert page.tool_receipts.evidence_status == "legacy_unavailable"
