"""Host-independent ``deerflow-runtime-api`` contract tests."""

from __future__ import annotations

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


def test_runtime_api_is_a_standard_library_only_workspace_dependency() -> None:
    backend = Path(__file__).parents[1]
    project = tomllib.loads((backend / "pyproject.toml").read_text())
    package = tomllib.loads((backend / "packages/runtime-api/pyproject.toml").read_text())

    assert package["project"]["version"] == "0.1.0"
    assert package["project"]["dependencies"] == []
    assert "packages/runtime-api" in project["tool"]["uv"]["workspace"]["members"]
    assert "deerflow-runtime-api==0.1.0" in project["project"]["dependencies"]

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
