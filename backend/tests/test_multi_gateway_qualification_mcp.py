"""Deterministic in-cluster MCP task fixture for multi-Gateway qualification."""

from __future__ import annotations

import pytest

from deerflow.qualification_mcp_server import (
    DeterministicQualificationTaskStore,
    qualification_mcp_enabled,
)


def test_qualification_mcp_requires_exact_opt_in() -> None:
    assert qualification_mcp_enabled({"DEERFLOW_QUALIFICATION_MCP": "1"}) is True
    assert qualification_mcp_enabled({"DEERFLOW_QUALIFICATION_MCP": "true"}) is False
    assert qualification_mcp_enabled({}) is False


@pytest.mark.asyncio
async def test_qualification_mcp_submission_is_idempotent_and_completes() -> None:
    store = DeterministicQualificationTaskStore()

    first = await store.submit(
        qualification_task_id="task-1",
        result_token="result-1",
        polls_before_complete=2,
    )
    duplicate = await store.submit(
        qualification_task_id="task-1",
        result_token="result-1",
        polls_before_complete=2,
    )

    assert first == duplicate == {"task_id": "qualification-task-1", "status": "running"}
    assert await store.status("qualification-task-1") == {
        "task_id": "qualification-task-1",
        "status": "running",
        "poll_after_seconds": 1.0,
    }
    assert await store.status("qualification-task-1") == {
        "task_id": "qualification-task-1",
        "status": "completed",
        "result": {"token": "result-1"},
    }
    assert store.submission_count == 1
    assert store.terminal_transition_count == 1


@pytest.mark.asyncio
async def test_qualification_mcp_rejects_conflicting_duplicate() -> None:
    store = DeterministicQualificationTaskStore()
    await store.submit(
        qualification_task_id="task-1",
        result_token="result-1",
        polls_before_complete=2,
    )

    with pytest.raises(ValueError, match="conflicting qualification task"):
        await store.submit(
            qualification_task_id="task-1",
            result_token="different",
            polls_before_complete=2,
        )


@pytest.mark.asyncio
async def test_qualification_mcp_cancel_is_idempotent_and_terminal() -> None:
    store = DeterministicQualificationTaskStore()
    await store.submit(
        qualification_task_id="task-2",
        result_token="result-2",
        polls_before_complete=10,
    )

    assert await store.cancel("qualification-task-2") == {
        "task_id": "qualification-task-2",
        "status": "cancelled",
    }
    assert await store.cancel("qualification-task-2") == {
        "task_id": "qualification-task-2",
        "status": "cancelled",
    }
    assert store.terminal_transition_count == 1


@pytest.mark.asyncio
async def test_qualification_mcp_returns_bounded_not_found_failure() -> None:
    store = DeterministicQualificationTaskStore()

    assert await store.status("qualification-missing") == {
        "task_id": "qualification-missing",
        "status": "failed",
        "error": "qualification task not found",
        "error_code": "task_not_found",
    }
