"""Deterministic long-running MCP service used only by live qualification.

The service intentionally keeps its tiny task state in one process.  The
qualified authority under test is the Gateway's PostgreSQL MCP task row,
poller lease, terminal transition, and notification lineage; this fixture is
the stable remote system that lets the harness kill a Gateway poller without
introducing an external SaaS dependency.
"""

from __future__ import annotations

import asyncio
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Final, Literal, TypedDict

from mcp.server.fastmcp import FastMCP

_OPT_IN_ENV: Final = "DEERFLOW_QUALIFICATION_MCP"
_SAFE_TOKEN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")


class SubmitResult(TypedDict):
    task_id: str
    status: Literal["running"]


class StatusResult(TypedDict, total=False):
    task_id: str
    status: Literal["running", "completed", "failed", "cancelled"]
    result: dict[str, str]
    error: str
    error_code: str
    poll_after_seconds: float


@dataclass(slots=True)
class _Task:
    remote_task_id: str
    result_token: str
    polls_before_complete: int
    polls: int = 0
    status: Literal["running", "completed", "cancelled"] = "running"


def qualification_mcp_enabled(environment: Mapping[str, str] | None = None) -> bool:
    """Return true only for the explicit qualification-only opt-in."""

    values = os.environ if environment is None else environment
    return values.get(_OPT_IN_ENV) == "1"


def _safe(value: str, *, name: str) -> str:
    if not isinstance(value, str) or _SAFE_TOKEN.fullmatch(value) is None:
        raise ValueError(f"{name} must be a bounded safe token")
    return value


class DeterministicQualificationTaskStore:
    """Concurrency-safe idempotent state machine behind the MCP fixture."""

    def __init__(self) -> None:
        self._tasks: dict[str, _Task] = {}
        self._lock = asyncio.Lock()
        self.submission_count = 0
        self.terminal_transition_count = 0

    async def submit(
        self,
        *,
        qualification_task_id: str,
        result_token: str,
        polls_before_complete: int = 5,
    ) -> SubmitResult:
        qualification_task_id = _safe(
            qualification_task_id,
            name="qualification_task_id",
        )
        result_token = _safe(result_token, name="result_token")
        if type(polls_before_complete) is not int or not 2 <= polls_before_complete <= 120:
            raise ValueError("polls_before_complete must be in [2, 120]")
        remote_task_id = f"qualification-{qualification_task_id}"
        async with self._lock:
            current = self._tasks.get(remote_task_id)
            if current is None:
                current = _Task(
                    remote_task_id=remote_task_id,
                    result_token=result_token,
                    polls_before_complete=polls_before_complete,
                )
                self._tasks[remote_task_id] = current
                self.submission_count += 1
            elif current.result_token != result_token or current.polls_before_complete != polls_before_complete:
                raise ValueError("conflicting qualification task submission")
            return {"task_id": remote_task_id, "status": "running"}

    async def status(self, task_id: str) -> StatusResult:
        task_id = _safe(task_id, name="task_id")
        async with self._lock:
            task = self._tasks.get(task_id)
            if task is None:
                return {
                    "task_id": task_id,
                    "status": "failed",
                    "error": "qualification task not found",
                    "error_code": "task_not_found",
                }
            if task.status == "cancelled":
                return {"task_id": task_id, "status": "cancelled"}
            if task.status == "completed":
                return {
                    "task_id": task_id,
                    "status": "completed",
                    "result": {"token": task.result_token},
                }
            task.polls += 1
            if task.polls < task.polls_before_complete:
                return {
                    "task_id": task_id,
                    "status": "running",
                    "poll_after_seconds": 1.0,
                }
            task.status = "completed"
            self.terminal_transition_count += 1
            return {
                "task_id": task_id,
                "status": "completed",
                "result": {"token": task.result_token},
            }

    async def cancel(self, task_id: str) -> StatusResult:
        task_id = _safe(task_id, name="task_id")
        async with self._lock:
            task = self._tasks.get(task_id)
            if task is None:
                return {
                    "task_id": task_id,
                    "status": "failed",
                    "error": "qualification task not found",
                }
            if task.status == "running":
                task.status = "cancelled"
                self.terminal_transition_count += 1
            if task.status == "completed":
                return {
                    "task_id": task_id,
                    "status": "completed",
                    "result": {"token": task.result_token},
                }
            return {"task_id": task_id, "status": "cancelled"}


def build_qualification_mcp_app(
    store: DeterministicQualificationTaskStore | None = None,
) -> FastMCP:
    """Build the test service without starting a listener."""

    tasks = store or DeterministicQualificationTaskStore()
    server = FastMCP(
        "HartMesh multi-Gateway qualification task service",
        host="0.0.0.0",
        port=8090,
        stateless_http=True,
        json_response=True,
    )

    @server.tool(name="submit_task")
    async def submit_task(
        qualification_task_id: str,
        result_token: str,
        polls_before_complete: int = 5,
    ) -> SubmitResult:
        return await tasks.submit(
            qualification_task_id=qualification_task_id,
            result_token=result_token,
            polls_before_complete=polls_before_complete,
        )

    @server.tool(name="task_status")
    async def task_status(task_id: str) -> StatusResult:
        return await tasks.status(task_id)

    @server.tool(name="cancel_task")
    async def cancel_task(task_id: str) -> StatusResult:
        return await tasks.cancel(task_id)

    return server


def main() -> int:
    """Run the streamable-HTTP fixture only inside an opted-in live test."""

    if not qualification_mcp_enabled():
        raise SystemExit(f"refusing to start qualification MCP service without {_OPT_IN_ENV}=1")
    build_qualification_mcp_app().run(transport="streamable-http")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "DeterministicQualificationTaskStore",
    "build_qualification_mcp_app",
    "qualification_mcp_enabled",
]
