"""Process-local bridge from harness tools to the Gateway batch service."""

from __future__ import annotations

import threading
from typing import Any, Protocol

from deerflow.subagents.batch_acceptance import ParentBoundBatchRequest


class SubagentBatchSubmitter(Protocol):
    async def accept(self, request: ParentBoundBatchRequest) -> dict[str, Any]: ...

    async def get_batch(self, *, batch_id: str, user_id: str) -> dict[str, Any] | None: ...

    async def cancel_batch(self, *, batch_id: str, user_id: str) -> dict[str, Any] | None: ...


_submitter: SubagentBatchSubmitter | None = None
_lock = threading.Lock()


def set_subagent_batch_submitter(submitter: SubagentBatchSubmitter | None) -> None:
    global _submitter
    with _lock:
        _submitter = submitter


def get_subagent_batch_submitter() -> SubagentBatchSubmitter | None:
    with _lock:
        return _submitter


def is_subagent_batch_runtime_available() -> bool:
    return get_subagent_batch_submitter() is not None
