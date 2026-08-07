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
