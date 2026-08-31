"""Bounded durable evidence for mutable external memory operations."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal

from deerflow_extension_api import TenantReferenceV1

MemoryOperation = Literal["get_context", "search", "get_memory", "add"]
MemoryObservationStatus = Literal[
    "succeeded",
    "empty",
    "failed_open",
    "failed_closed",
]

_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$", re.ASCII)
_WORKSPACE_REF_RE = re.compile(r"^honcho-workspace-[0-9a-f]{24}$", re.ASCII)
MEMORY_OBSERVATION_ITEM_COUNT_MAX = 100


@dataclass(frozen=True, slots=True)
class MemoryObservationV1:
    """Safe observation of one Honcho operation, never its raw content."""

    version: Literal[1]
    backend: Literal["honcho"]
    tenant: TenantReferenceV1
    workspace_ref: str
    operation: MemoryOperation
    status: MemoryObservationStatus
    safe_projection_digest: str | None
    item_count: int | None
    truncated: bool
    occurred_at: datetime

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != 1 or self.backend != "honcho":
            raise ValueError("memory observation version/backend is invalid")
        if not isinstance(self.tenant, TenantReferenceV1):
            raise TypeError("memory observation tenant must be TenantReferenceV1")
        if _WORKSPACE_REF_RE.fullmatch(self.workspace_ref) is None:
            raise ValueError("memory observation workspace_ref is invalid")
        if self.operation not in {"get_context", "search", "get_memory", "add"}:
            raise ValueError("memory observation operation is invalid")
        if self.status not in {"succeeded", "empty", "failed_open", "failed_closed"}:
            raise ValueError("memory observation status is invalid")
        if self.safe_projection_digest is not None and _DIGEST_RE.fullmatch(self.safe_projection_digest) is None:
            raise ValueError("memory observation projection digest is invalid")
        if self.item_count is not None and (type(self.item_count) is not int or not 0 <= self.item_count <= MEMORY_OBSERVATION_ITEM_COUNT_MAX):
            raise ValueError(f"memory observation item_count must be an integer from 0 through {MEMORY_OBSERVATION_ITEM_COUNT_MAX}")
        if type(self.truncated) is not bool:
            raise TypeError("memory observation truncated must be boolean")
        if not isinstance(self.occurred_at, datetime) or self.occurred_at.tzinfo is None:
            raise ValueError("memory observation occurred_at must be timezone-aware")

    def to_event_body(self) -> dict[str, object]:
        return {
            "version": self.version,
            "backend": self.backend,
            "tenant": self.tenant.to_json(),
            "workspace_ref": self.workspace_ref,
            "operation": self.operation,
            "status": self.status,
            "safe_projection_digest": self.safe_projection_digest,
            "item_count": self.item_count,
            "truncated": self.truncated,
            "occurred_at": self.occurred_at.astimezone(UTC).isoformat().replace("+00:00", "Z"),
        }


__all__ = [
    "MEMORY_OBSERVATION_ITEM_COUNT_MAX",
    "MemoryObservationStatus",
    "MemoryObservationV1",
    "MemoryOperation",
]
