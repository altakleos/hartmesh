"""Host-independent ``deerflow-runtime-api`` contract tests."""

from __future__ import annotations

import inspect
import subprocess
import sys
import tomllib
from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest


def test_runtime_api_records_are_versioned_and_frozen() -> None:
    from deerflow_runtime_api import API_VERSION, GraphInputV1, InvocationEnsureRequest, InvocationOptionsV1

    request = InvocationEnsureRequest(
        external_key="delivery-1",
        thread_id="thread-1",
        agent_hint=None,
        input=GraphInputV1(value={"messages": [{"role": "user", "content": "hello"}]}),
        options=InvocationOptionsV1(),
    )

    assert API_VERSION == "deerflow.runtime/v1"
    assert request.api_version == API_VERSION
    assert request.kind == "invocation.ensure"
    with pytest.raises(FrozenInstanceError):
        request.thread_id = "forged"  # type: ignore[misc]


def test_invocation_query_and_observation_bound_optional_mcp_task_lineage_page() -> None:
    from deerflow_runtime_api import InvocationObservation, InvocationQuery

    query = InvocationQuery(
        run_id="run-1",
        include_mcp_tasks=True,
        mcp_task_cursor="mtc1.opaque",
        mcp_task_limit=7,
    )
    assert InvocationQuery.from_dict(query.to_dict()) == query
    with pytest.raises(ValueError, match="requires include_mcp_tasks"):
        InvocationQuery(run_id="run-1", mcp_task_cursor="mtc1.opaque")

    page = {
        "items": [
            {
                "task_id": "task-1",
                "lineage_digest": "a" * 64,
                "submitting_task_id": "run-1",
                "receipt_id": "tr_" + "b" * 64,
                "server_name": "reports",
                "tool_name": "submit_report",
                "status": "completed",
                "safe_terminal_code": None,
                "notification_run_id": "run-notification",
                "created_at": "2026-01-01T00:00:00+00:00",
                "updated_at": "2026-01-01T00:01:00+00:00",
                "completed_at": "2026-01-01T00:01:00+00:00",
            }
        ],
        "next_cursor": None,
        "pruning_status": "not_pruned",
    }
    observation = InvocationObservation(
        run_id="run-1",
        thread_id="thread-1",
        status="success",
        state_version=2,
        snapshots=(),
        events=(),
        next_cursor="lc1.Mg",
        minimum_available_cursor="lc1.MA",
        read_fence_cursor="lc1.Mg",
        mcp_tasks=page,
    )
    assert observation.to_dict()["mcp_tasks"] == page
    assert InvocationObservation.from_dict(observation.to_dict()) == observation

    with pytest.raises(ValueError, match="singular invocation"):
        InvocationObservation(
            run_id=None,
            thread_id="thread-1",
            status=None,
            state_version=None,
            snapshots=(),
            events=(),
            next_cursor="lc1.Mg",
            minimum_available_cursor="lc1.MA",
            read_fence_cursor="lc1.Mg",
            mcp_tasks=page,
        )


def test_invocation_query_and_observation_bound_optional_subagent_batch_lifecycle_page() -> None:
    from deerflow_runtime_api import InvocationObservation, InvocationQuery

    query = InvocationQuery(
        run_id="run-1",
        include_subagent_batches=True,
        subagent_batch_cursor="sbc1.opaque",
        subagent_batch_limit=7,
    )
    assert InvocationQuery.from_dict(query.to_dict()) == query
    with pytest.raises(ValueError, match="requires include_subagent_batches"):
        InvocationQuery(
            run_id="run-1",
            subagent_batch_cursor="sbc1.opaque",
        )

    page = {
        "items": [
            {
                "batch_id": "sb_" + "1" * 48,
                "acceptance_digest": "a" * 64,
                "parent_tool_receipt_id": "tr_" + "b" * 64,
                "status": "completed",
                "terminal_code": "succeeded",
                "total_items": 1,
                "accepted_at": "2026-01-01T00:00:00+00:00",
                "updated_at": "2026-01-01T00:01:00+00:00",
                "completed_at": "2026-01-01T00:01:00+00:00",
                "observations": [
                    {
                        "version": 1,
                        "event": "batch.accepted",
                        "batch_id": "sb_" + "1" * 48,
                        "acceptance_digest": "a" * 64,
                        "parent_run_id": "run-1",
                        "parent_tool_receipt_id": "tr_" + "b" * 64,
                        "item_count": 1,
                        "occurred_at": "2026-01-01T00:00:00+00:00",
                    }
                ],
            }
        ],
        "next_cursor": None,
        "pruning_status": "not_pruned",
    }
    observation = InvocationObservation(
        run_id="run-1",
        thread_id="thread-1",
        status="success",
        state_version=2,
        snapshots=(),
        events=(),
        next_cursor="lc1.Mg",
        minimum_available_cursor="lc1.MA",
        read_fence_cursor="lc1.Mg",
        subagent_batches=page,
    )

    assert observation.to_dict()["subagent_batches"] == page
    assert InvocationObservation.from_dict(observation.to_dict()) == observation

    with pytest.raises(ValueError, match="singular invocation"):
        InvocationObservation(
            run_id=None,
            thread_id="thread-1",
            status=None,
            state_version=None,
            snapshots=(),
            events=(),
            next_cursor="lc1.Mg",
            minimum_available_cursor="lc1.MA",
            read_fence_cursor="lc1.Mg",
            subagent_batches=page,
        )


def test_runtime_api_nested_values_are_immutable_defensive_snapshots() -> None:
    from deerflow_runtime_api import (
        GraphInputV1,
        InvocationObservation,
        InvocationOptionsV1,
        ResumeInputV1,
        RuntimeCapabilities,
        RuntimeFailure,
        record_from_dict,
    )

    caller_input = {
        "messages": [
            {
                "role": "user",
                "content": {"parts": ["original"]},
            }
        ]
    }
    caller_snapshot = {
        "run_id": "run-1",
        "thread_id": "thread-1",
        "status": "running",
        "state_version": 2,
    }
    caller_payload = {"version": 1, "evidence": ["accepted"]}
    caller_event = {
        "event_id": "event-1",
        "cursor": "opaque-1",
        "run_id": "run-1",
        "thread_id": "thread-1",
        "lifecycle_type": "started",
        "state_version": 2,
        "status": "running",
        "created_at": "2026-01-01T00:00:00+00:00",
        "payload": caller_payload,
    }
    caller_controls = ["cancel"]
    caller_resume = {"answers": [{"parts": ["original"]}]}
    caller_interrupts = ["tools"]
    caller_failure_detail = {"version": 1}

    graph_input = GraphInputV1(value=caller_input)
    resume_input = ResumeInputV1(value=caller_resume)
    options = InvocationOptionsV1(interrupt_before=caller_interrupts)
    observation = InvocationObservation(
        run_id="run-1",
        thread_id="thread-1",
        status="running",
        state_version=2,
        snapshots=[caller_snapshot],
        events=[caller_event],
        next_cursor="opaque-1",
        minimum_available_cursor="opaque-0",
        read_fence_cursor="opaque-1",
    )
    capabilities = RuntimeCapabilities(controls=caller_controls)
    failure = RuntimeFailure(
        code="invalid_request",
        detail=caller_failure_detail,
    )
    original_input_wire = graph_input.to_dict()
    original_observation_wire = observation.to_dict()
    equivalent_input = GraphInputV1.from_dict(original_input_wire)

    caller_input["messages"][0]["content"]["parts"][0] = "mutated"
    caller_input["messages"].append({"role": "user", "content": "late"})
    caller_snapshot["status"] = "success"
    caller_payload["evidence"].append("mutated")
    caller_controls.clear()
    caller_resume["answers"][0]["parts"].append("mutated")
    caller_interrupts.append("late")
    caller_failure_detail["forged"] = True

    assert graph_input.to_dict() == original_input_wire
    assert graph_input == equivalent_input
    assert observation.to_dict() == original_observation_wire
    assert capabilities.controls == ("cancel",)
    assert resume_input.to_dict()["value"] == {"answers": [{"parts": ["original"]}]}
    assert options.interrupt_before == ("tools",)
    assert failure.to_dict()["detail"] == {"version": 1}
    with pytest.raises(TypeError):
        graph_input.value["forged"] = True  # type: ignore[index]
    with pytest.raises(TypeError):
        graph_input.value["messages"][0]["content"]["parts"][0] = "forged"  # type: ignore[index]
    with pytest.raises(TypeError):
        observation.events[0]["payload"]["forged"] = True  # type: ignore[index]

    first_wire = observation.to_dict()
    second_wire = observation.to_dict()
    first_wire["events"][0]["payload"]["evidence"].append("wire-only")
    assert second_wire == original_observation_wire
    assert observation.to_dict() == original_observation_wire
    assert record_from_dict(second_wire).to_dict() == second_wire


def test_exported_runtime_contract_classes_are_documented_and_annotated() -> None:
    import deerflow_runtime_api as runtime_api

    for name in runtime_api.__all__:
        exported = getattr(runtime_api, name)
        if not inspect.isclass(exported):
            continue
        assert inspect.getdoc(exported), f"{name} needs a public contract docstring"
        annotations = getattr(exported, "__annotations__", {})
        if hasattr(exported, "__dataclass_fields__"):
            assert annotations, f"{name} needs public field annotations"

    protocol = runtime_api.DurableInvocationPort
    for method_name in ("ensure", "observe", "control", "capabilities"):
        signature = inspect.signature(getattr(protocol, method_name))
        assert signature.return_annotation is not inspect.Signature.empty


def test_every_public_record_round_trips_strictly() -> None:
    from deerflow_runtime_api import (
        CancelInvocationRequest,
        ContextInvocationsQuery,
        GraphInputV1,
        InvocationControlReceipt,
        InvocationEnsureReceipt,
        InvocationEnsureRequest,
        InvocationObservation,
        InvocationOptionsV1,
        InvocationQuery,
        ResumeInputV1,
        RuntimeCapabilities,
        RuntimeFailure,
        record_from_dict,
    )

    records = [
        GraphInputV1(value={"messages": []}),
        ResumeInputV1(value={"answer": 42}),
        InvocationOptionsV1(
            model_name="fast",
            thinking_enabled=True,
            multitask_strategy="interrupt",
            checkpoint_id="checkpoint-1",
            interrupt_before=("tools",),
            interrupt_after="*",
        ),
        InvocationEnsureRequest(
            external_key="delivery-1",
            thread_id="thread-1",
            agent_hint="agent-1",
            input=GraphInputV1(value={"messages": []}),
            options=InvocationOptionsV1(),
        ),
        InvocationEnsureReceipt(
            disposition="created",
            run_id="run-1",
            thread_id="thread-1",
            status="pending",
            state_version=1,
        ),
        InvocationQuery(run_id="run-1", cursor=None, limit=50, include_snapshot=True),
        ContextInvocationsQuery(thread_id="thread-1", cursor="opaque", limit=50, include_snapshot=False),
        InvocationObservation(
            run_id="run-1",
            thread_id="thread-1",
            status="running",
            state_version=2,
            snapshots=({"run_id": "run-1", "thread_id": "thread-1", "status": "running", "state_version": 2},),
            events=(
                {
                    "event_id": "event-1",
                    "cursor": "opaque-1",
                    "run_id": "run-1",
                    "thread_id": "thread-1",
                    "lifecycle_type": "started",
                    "state_version": 2,
                    "status": "running",
                    "created_at": "2026-01-01T00:00:00+00:00",
                    "payload": {"version": 1},
                },
            ),
            next_cursor="opaque-1",
            minimum_available_cursor="opaque-0",
            read_fence_cursor="opaque-1",
        ),
        CancelInvocationRequest(run_id="run-1", expected_state_version=2, action="interrupt"),
        InvocationControlReceipt(
            disposition="requested",
            run_id="run-1",
            thread_id="thread-1",
            status="running",
            state_version=3,
        ),
        RuntimeCapabilities(),
        RuntimeFailure(code="cursor_gap", detail={"version": 1, "minimum_available_cursor": "opaque-3"}),
    ]

    for record in records:
        encoded = record.to_dict()
        assert record_from_dict(encoded) == record
        with pytest.raises(ValueError, match="unknown fields"):
            record_from_dict({**encoded, "forged": True})
        with pytest.raises(ValueError, match="version"):
            record_from_dict({**encoded, "api_version": "deerflow.runtime/v2"})

    with pytest.raises(ValueError, match="kind"):
        record_from_dict({"api_version": "deerflow.runtime/v1", "kind": "invocation.unknown"})


def test_tool_receipt_page_round_trips_and_rejects_unbounded_error_text() -> None:
    from deerflow_runtime_api import InvocationObservation, record_from_dict

    item = {
        "receipt_id": "tr_" + "a" * 64,
        "task_id": "run-1",
        "kind": "lead",
        "subagent_name": None,
        "tool_name": "web_search",
        "attempt": 1,
        "status": "succeeded",
        "started_at": "2026-08-30T00:00:00+00:00",
        "finished_at": "2026-08-30T00:00:01+00:00",
        "request_projection_digest": "b" * 64,
        "result_projection_digest": "c" * 64,
        "result_kind": "tool_message",
        "safe_error_code": None,
        "authz_decision_ref": "pd_" + "d" * 64,
        "guardrail_decision_refs": (),
        "agent_revision_digest": "e" * 64,
        "assembly_fingerprint": "f" * 64,
        "extension_generation": 3,
        "subagent_catalog_digest": "1" * 64,
        "subagent_definition_digest": None,
    }
    page = {
        "items": [item],
        "next_cursor": None,
        "pruned_before": None,
        "evidence_status": "available",
        "invalid_event_count": 0,
    }
    observation = InvocationObservation(
        run_id="run-1",
        thread_id="thread-1",
        status="running",
        state_version=2,
        snapshots=(),
        events=(),
        next_cursor="opaque-1",
        minimum_available_cursor="opaque-0",
        read_fence_cursor="opaque-1",
        tool_receipts=page,
    )

    assert record_from_dict(observation.to_dict()) == observation
    bad_item = {**item, "status": "failed", "safe_error_code": "provider said secret text"}
    with pytest.raises(ValueError, match="safe error code"):
        InvocationObservation(
            run_id="run-1",
            thread_id="thread-1",
            status="running",
            state_version=2,
            snapshots=(),
            events=(),
            next_cursor="opaque-1",
            minimum_available_cursor="opaque-0",
            read_fence_cursor="opaque-1",
            tool_receipts={**page, "items": [bad_item]},
        )


@pytest.mark.parametrize("row_kind", ["snapshot", "event"])
@pytest.mark.parametrize("construction", ["direct", "from_dict", "record_from_dict"])
def test_observation_rejects_cross_thread_snapshots_and_events(
    row_kind: str,
    construction: str,
) -> None:
    from deerflow_runtime_api import InvocationObservation, record_from_dict

    values = {
        "run_id": "run-1",
        "thread_id": "thread-1",
        "status": "running",
        "state_version": 2,
        "snapshots": (
            {
                "run_id": "run-2",
                "thread_id": "thread-2",
                "status": "running",
                "state_version": 2,
            },
        )
        if row_kind == "snapshot"
        else (),
        "events": (
            {
                "event_id": "event-2",
                "cursor": "opaque-2",
                "run_id": "run-2",
                "thread_id": "thread-2",
                "lifecycle_type": "started",
                "state_version": 2,
                "status": "running",
                "created_at": "2026-01-01T00:00:00+00:00",
                "payload": {"version": 1},
            },
        )
        if row_kind == "event"
        else (),
        "next_cursor": "opaque-2",
        "minimum_available_cursor": "opaque-0",
        "read_fence_cursor": "opaque-2",
    }

    with pytest.raises(ValueError, match="observed thread"):
        if construction == "direct":
            InvocationObservation(**values)
        else:
            wire = {
                **values,
                "snapshots": list(values["snapshots"]),
                "events": list(values["events"]),
                "api_version": "deerflow.runtime/v1",
                "kind": "invocation.observation",
            }
            if construction == "from_dict":
                InvocationObservation.from_dict(wire)
            else:
                record_from_dict(wire)


@pytest.mark.parametrize("row_kind", ["snapshot", "event"])
@pytest.mark.parametrize("construction", ["direct", "wire"])
def test_singular_observation_rejects_another_runs_rows(
    row_kind: str,
    construction: str,
) -> None:
    from deerflow_runtime_api import InvocationObservation, record_from_dict

    snapshot = {
        "run_id": "run-2",
        "thread_id": "thread-1",
        "status": "running",
        "state_version": 2,
    }
    event = {
        "event_id": "event-2",
        "cursor": "opaque-2",
        "run_id": "run-2",
        "thread_id": "thread-1",
        "lifecycle_type": "started",
        "state_version": 2,
        "status": "running",
        "created_at": "2026-01-01T00:00:00+00:00",
        "payload": {"version": 1},
    }
    values = {
        "run_id": "run-1",
        "thread_id": "thread-1",
        "status": "running",
        "state_version": 2,
        "snapshots": (snapshot,) if row_kind == "snapshot" else (),
        "events": (event,) if row_kind == "event" else (),
        "next_cursor": "opaque-2",
        "minimum_available_cursor": "opaque-0",
        "read_fence_cursor": "opaque-2",
    }

    with pytest.raises(ValueError, match="observed run"):
        if construction == "direct":
            InvocationObservation(**values)
        else:
            record_from_dict(
                {
                    **values,
                    "snapshots": list(values["snapshots"]),
                    "events": list(values["events"]),
                    "api_version": "deerflow.runtime/v1",
                    "kind": "invocation.observation",
                }
            )


@pytest.mark.parametrize(
    ("run_id", "summary_run_id", "summary_thread_id", "message"),
    [
        (None, "run-1", "thread-2", "observed thread"),
        ("run-1", "run-2", "thread-1", "observed run"),
    ],
)
def test_observation_rejects_a_summary_from_another_context(
    run_id: str | None,
    summary_run_id: str,
    summary_thread_id: str,
    message: str,
) -> None:
    from deerflow_runtime_api import InvocationObservation, InvocationSummaryV1

    with pytest.raises(ValueError, match=message):
        InvocationObservation(
            run_id=run_id,
            thread_id="thread-1",
            status="pending" if run_id is not None else None,
            state_version=1 if run_id is not None else None,
            snapshots=(),
            events=(),
            next_cursor="opaque-1",
            minimum_available_cursor="opaque-0",
            read_fence_cursor="opaque-1",
            summaries=(
                InvocationSummaryV1(
                    run_id=summary_run_id,
                    thread_id=summary_thread_id,
                    status="pending",
                    state_version=1,
                    source_kind="http",
                ),
            ),
        )


@pytest.mark.parametrize("collection", ["snapshots", "summaries"])
@pytest.mark.parametrize("construction", ["direct", "wire"])
def test_observation_rejects_duplicate_snapshot_or_summary_run_ids(
    collection: str,
    construction: str,
) -> None:
    from deerflow_runtime_api import InvocationObservation, InvocationSummaryV1, record_from_dict

    snapshot = {
        "run_id": "run-1",
        "thread_id": "thread-1",
        "status": "pending",
        "state_version": 1,
    }
    summary = InvocationSummaryV1(
        run_id="run-1",
        thread_id="thread-1",
        status="pending",
        state_version=1,
        source_kind="http",
    )
    values = {
        "run_id": None,
        "thread_id": "thread-1",
        "status": None,
        "state_version": None,
        "snapshots": (snapshot, snapshot) if collection == "snapshots" else (snapshot,),
        "events": (),
        "next_cursor": "opaque-1",
        "minimum_available_cursor": "opaque-0",
        "read_fence_cursor": "opaque-1",
        "summaries": (summary, summary) if collection == "summaries" else (),
    }

    with pytest.raises(ValueError, match="duplicate"):
        if construction == "direct":
            InvocationObservation(**values)
        else:
            record_from_dict(
                {
                    **values,
                    "snapshots": list(values["snapshots"]),
                    "events": [],
                    "summaries": [item.to_dict() for item in values["summaries"]],
                    "api_version": "deerflow.runtime/v1",
                    "kind": "invocation.observation",
                }
            )


@pytest.mark.parametrize("construction", ["direct", "wire"])
def test_observation_rejects_a_summary_without_a_materialized_snapshot(
    construction: str,
) -> None:
    from deerflow_runtime_api import InvocationObservation, InvocationSummaryV1, record_from_dict

    summary = InvocationSummaryV1(
        run_id="run-1",
        thread_id="thread-1",
        status="pending",
        state_version=1,
        source_kind="http",
    )
    values = {
        "run_id": None,
        "thread_id": "thread-1",
        "status": None,
        "state_version": None,
        "snapshots": (),
        "events": (),
        "next_cursor": "opaque-1",
        "minimum_available_cursor": "opaque-0",
        "read_fence_cursor": "opaque-1",
        "summaries": (summary,),
    }

    with pytest.raises(ValueError, match="materialized snapshot"):
        if construction == "direct":
            InvocationObservation(**values)
        else:
            record_from_dict(
                {
                    **values,
                    "snapshots": [],
                    "events": [],
                    "summaries": [summary.to_dict()],
                    "api_version": "deerflow.runtime/v1",
                    "kind": "invocation.observation",
                }
            )


@pytest.mark.parametrize(
    ("field", "value"),
    [("status", "running"), ("state_version", 2)],
)
@pytest.mark.parametrize("construction", ["direct", "wire"])
def test_observation_rejects_summary_snapshot_state_mismatch(
    field: str,
    value: object,
    construction: str,
) -> None:
    from deerflow_runtime_api import InvocationObservation, InvocationSummaryV1, record_from_dict

    snapshot = {
        "run_id": "run-1",
        "thread_id": "thread-1",
        "status": "pending",
        "state_version": 1,
    }
    summary_values = {
        "run_id": "run-1",
        "thread_id": "thread-1",
        "status": "pending",
        "state_version": 1,
        "source_kind": "http",
        field: value,
    }
    summary = InvocationSummaryV1(**summary_values)
    values = {
        "run_id": None,
        "thread_id": "thread-1",
        "status": None,
        "state_version": None,
        "snapshots": (snapshot,),
        "events": (),
        "next_cursor": "opaque-1",
        "minimum_available_cursor": "opaque-0",
        "read_fence_cursor": "opaque-1",
        "summaries": (summary,),
    }

    with pytest.raises(ValueError, match="summary and snapshot"):
        if construction == "direct":
            InvocationObservation(**values)
        else:
            record_from_dict(
                {
                    **values,
                    "snapshots": [snapshot],
                    "events": [],
                    "summaries": [summary.to_dict()],
                    "api_version": "deerflow.runtime/v1",
                    "kind": "invocation.observation",
                }
            )


@pytest.mark.parametrize(
    ("field", "value"),
    [("status", "success"), ("state_version", 3)],
)
@pytest.mark.parametrize("construction", ["direct", "wire"])
def test_singular_observation_rejects_current_snapshot_state_mismatch(
    field: str,
    value: object,
    construction: str,
) -> None:
    from deerflow_runtime_api import InvocationObservation, record_from_dict

    snapshot = {
        "run_id": "run-1",
        "thread_id": "thread-1",
        "status": "running",
        "state_version": 2,
        field: value,
    }
    values = {
        "run_id": "run-1",
        "thread_id": "thread-1",
        "status": "running",
        "state_version": 2,
        "snapshots": (snapshot,),
        "events": (),
        "next_cursor": "opaque-2",
        "minimum_available_cursor": "opaque-0",
        "read_fence_cursor": "opaque-2",
    }

    with pytest.raises(ValueError, match="top-level current state"):
        if construction == "direct":
            InvocationObservation(**values)
        else:
            record_from_dict(
                {
                    **values,
                    "snapshots": [snapshot],
                    "events": [],
                    "api_version": "deerflow.runtime/v1",
                    "kind": "invocation.observation",
                }
            )


def test_context_observation_accepts_historical_events_and_summary_subsets() -> None:
    from deerflow_runtime_api import InvocationObservation, InvocationSummaryV1, record_from_dict

    observation = InvocationObservation(
        run_id=None,
        thread_id="thread-1",
        status=None,
        state_version=None,
        snapshots=(
            {
                "run_id": "run-1",
                "thread_id": "thread-1",
                "status": "success",
                "state_version": 3,
            },
            {
                "run_id": "run-2",
                "thread_id": "thread-1",
                "status": "running",
                "state_version": 2,
            },
        ),
        events=(
            {
                "event_id": "event-1",
                "cursor": "opaque-1",
                "run_id": "run-1",
                "thread_id": "thread-1",
                "lifecycle_type": "accepted",
                "state_version": 1,
                "status": "pending",
                "created_at": "2026-01-01T00:00:00+00:00",
                "payload": {"version": 1},
            },
        ),
        next_cursor="opaque-2",
        minimum_available_cursor="opaque-0",
        read_fence_cursor="opaque-2",
        summaries=(
            InvocationSummaryV1(
                run_id="run-1",
                thread_id="thread-1",
                status="success",
                state_version=3,
                source_kind="http",
            ),
        ),
    )

    assert record_from_dict(observation.to_dict()) == observation


@pytest.mark.parametrize(
    ("lifecycle_type", "status"),
    [
        ("downstream_custom_state", "running"),
        ("succeeded", "running"),
    ],
)
def test_observation_rejects_unknown_or_contradictory_lifecycle_values(
    lifecycle_type: str,
    status: str,
) -> None:
    from deerflow_runtime_api import InvocationObservation, record_from_dict

    observation = {
        "api_version": "deerflow.runtime/v1",
        "kind": "invocation.observation",
        "run_id": "run-1",
        "thread_id": "thread-1",
        "status": "running",
        "state_version": 2,
        "snapshots": [
            {
                "run_id": "run-1",
                "thread_id": "thread-1",
                "status": "running",
                "state_version": 2,
            }
        ],
        "events": [
            {
                "event_id": "event-1",
                "cursor": "opaque-1",
                "run_id": "run-1",
                "thread_id": "thread-1",
                "lifecycle_type": lifecycle_type,
                "state_version": 2,
                "status": status,
                "created_at": "2026-01-01T00:00:00+00:00",
                "payload": {"version": 1},
            }
        ],
        "next_cursor": "opaque-1",
        "minimum_available_cursor": "opaque-0",
        "read_fence_cursor": "opaque-1",
    }

    with pytest.raises(ValueError, match="lifecycle"):
        record_from_dict(observation)
    with pytest.raises(ValueError, match="lifecycle"):
        InvocationObservation(
            run_id="run-1",
            thread_id="thread-1",
            status="running",
            state_version=2,
            snapshots=tuple(observation["snapshots"]),
            events=tuple(observation["events"]),
            next_cursor="opaque-1",
            minimum_available_cursor="opaque-0",
            read_fence_cursor="opaque-1",
        )


def test_public_runtime_records_reject_unknown_run_statuses() -> None:
    from deerflow_runtime_api import InvocationControlReceipt, InvocationEnsureReceipt, InvocationObservation

    with pytest.raises(ValueError, match="status"):
        InvocationEnsureReceipt(
            disposition="created",
            run_id="run-1",
            thread_id="thread-1",
            status="downstream_custom_state",
            state_version=1,
        )
    with pytest.raises(ValueError, match="status"):
        InvocationControlReceipt(
            disposition="requested",
            run_id="run-1",
            thread_id="thread-1",
            status="downstream_custom_state",
            state_version=2,
        )
    with pytest.raises(ValueError, match="status"):
        InvocationObservation(
            run_id="run-1",
            thread_id="thread-1",
            status="downstream_custom_state",
            state_version=2,
            snapshots=(),
            events=(),
            next_cursor="opaque-1",
            minimum_available_cursor="opaque-0",
            read_fence_cursor="opaque-1",
        )
    with pytest.raises(ValueError, match="status"):
        InvocationObservation(
            run_id=None,
            thread_id="thread-1",
            status=None,
            state_version=None,
            snapshots=(
                {
                    "run_id": "run-1",
                    "thread_id": "thread-1",
                    "status": "downstream_custom_state",
                    "state_version": 1,
                },
            ),
            events=(),
            next_cursor="opaque-1",
            minimum_available_cursor="opaque-0",
            read_fence_cursor="opaque-1",
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("event_id", ""),
        ("cursor", ""),
        ("run_id", ""),
        ("thread_id", ""),
        ("created_at", ""),
        ("state_version", 0),
        ("state_version", True),
    ],
)
def test_lifecycle_event_rejects_invalid_identity_or_state_version(field: str, value: object) -> None:
    from deerflow_runtime_api import InvocationObservation

    event = {
        "event_id": "event-1",
        "cursor": "opaque-1",
        "run_id": "run-1",
        "thread_id": "thread-1",
        "lifecycle_type": "started",
        "state_version": 2,
        "status": "running",
        "created_at": "2026-01-01T00:00:00+00:00",
        "payload": {"version": 1},
        field: value,
    }

    with pytest.raises(ValueError):
        InvocationObservation(
            run_id="run-1",
            thread_id="thread-1",
            status="running",
            state_version=2,
            snapshots=(),
            events=(event,),
            next_cursor="opaque-1",
            minimum_available_cursor="opaque-0",
            read_fence_cursor="opaque-1",
        )


@pytest.mark.parametrize(
    ("lifecycle_type", "status"),
    [
        ("accepted", "pending"),
        ("started", "running"),
        ("cancellation_requested", "pending"),
        ("cancellation_requested", "running"),
        ("cancelled", "error"),
        ("cancelled", "interrupted"),
        ("succeeded", "success"),
        ("failed", "error"),
        ("timed_out", "timeout"),
        ("interrupted", "error"),
        ("interrupted", "interrupted"),
    ],
)
def test_every_legal_lifecycle_status_pair_round_trips(
    lifecycle_type: str,
    status: str,
) -> None:
    from deerflow_runtime_api import InvocationObservation, record_from_dict

    observation = InvocationObservation(
        run_id="run-1",
        thread_id="thread-1",
        status=status,
        state_version=2,
        snapshots=(),
        events=(
            {
                "event_id": "event-1",
                "cursor": "opaque-1",
                "run_id": "run-1",
                "thread_id": "thread-1",
                "lifecycle_type": lifecycle_type,
                "state_version": 2,
                "status": status,
                "created_at": "2026-01-01T00:00:00+00:00",
                "payload": {"version": 1},
            },
        ),
        next_cursor="opaque-1",
        minimum_available_cursor="opaque-0",
        read_fence_cursor="opaque-1",
    )

    assert record_from_dict(observation.to_dict()) == observation


def test_public_lifecycle_vocabulary_matches_the_harness() -> None:
    from deerflow.runtime import RunStatus
    from deerflow.runtime.runs.store.base import LifecycleType

    assert {status.value for status in RunStatus} == {
        "pending",
        "running",
        "success",
        "error",
        "timeout",
        "interrupted",
    }
    assert {lifecycle_type.value for lifecycle_type in LifecycleType} == {
        "accepted",
        "started",
        "cancellation_requested",
        "cancelled",
        "succeeded",
        "failed",
        "timed_out",
        "interrupted",
    }


def test_runtime_failure_detail_is_code_specific_and_cannot_carry_policy_text() -> None:
    from deerflow_runtime_api import RuntimeFailure

    with pytest.raises(ValueError, match="detail fields"):
        RuntimeFailure(
            code="denied",
            detail={"version": 1, "policy_reason": "private"},
        )
    with pytest.raises(ValueError, match="detail fields"):
        RuntimeFailure(
            code="cursor_gap",
            detail={"version": 1},
        )
    assert (
        RuntimeFailure(
            code="indeterminate",
            detail={"version": 1, "correlation_id": "a" * 32},
        ).detail["correlation_id"]
        == "a" * 32
    )
    with pytest.raises(ValueError, match="correlation_id"):
        RuntimeFailure(
            code="indeterminate",
            detail={"version": 1, "correlation_id": "raw exception text"},
        )


def test_runtime_api_is_a_standard_library_only_workspace_dependency() -> None:
    backend = Path(__file__).parents[1]
    project = tomllib.loads((backend / "pyproject.toml").read_text())
    package = tomllib.loads((backend / "packages/runtime-api/pyproject.toml").read_text())

    assert package["project"]["version"] == "0.2.0"
    assert package["project"]["dependencies"] == []
    assert "packages/runtime-api" in project["tool"]["uv"]["workspace"]["members"]
    assert "deerflow-runtime-api==0.2.0" in project["project"]["dependencies"]

    result = subprocess.run(
        [
            sys.executable,
            "-I",
            "-c",
            (
                "import sys;"
                f"sys.path.insert(0, {str(backend / 'packages/runtime-api')!r});"
                "import deerflow_runtime_api;"
                "forbidden=('app','deerflow','fastapi','sqlalchemy',"
                "'deerflow_extension_api');"
                "assert not any(name == root or name.startswith(root + '.') "
                "for name in sys.modules for root in forbidden)"
            ),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize(
    "kwargs",
    [
        {"metadata": {}},
        {"config": {}},
        {"context": {}},
        {"command": {}},
        {"callbacks": []},
        {"credentials": {}},
    ],
)
def test_ensure_request_rejects_unpublished_property_bags(kwargs) -> None:
    from deerflow_runtime_api import GraphInputV1, InvocationEnsureRequest, InvocationOptionsV1

    with pytest.raises(TypeError):
        InvocationEnsureRequest(
            external_key="delivery-1",
            thread_id="thread-1",
            agent_hint=None,
            input=GraphInputV1(value={}),
            options=InvocationOptionsV1(),
            **kwargs,
        )
