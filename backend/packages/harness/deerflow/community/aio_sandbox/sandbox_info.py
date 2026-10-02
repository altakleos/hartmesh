"""Sandbox metadata for cross-process discovery and state persistence."""

from __future__ import annotations

import time
from dataclasses import dataclass, field

# ``SandboxInfo.provenance`` values. Only the backend that answered ``create``
# can tell a started resource from a found one, so it is the backend's word.
PROVENANCE_CREATED = "created"
PROVENANCE_REDISCOVERED = "rediscovered"
PROVENANCE_UNKNOWN = "unknown"
PROVENANCE_VALUES = frozenset({PROVENANCE_CREATED, PROVENANCE_REDISCOVERED, PROVENANCE_UNKNOWN})


@dataclass
class SandboxInfo:
    """Persisted sandbox metadata that enables cross-process discovery.

    This dataclass holds all the information needed to reconnect to an
    existing sandbox from a different process (e.g., gateway vs langgraph,
    multiple workers, or across K8s pods with shared storage).
    """

    sandbox_id: str
    sandbox_url: str  # e.g. http://localhost:8080 or http://k3s:30001
    container_name: str | None = None  # Only for local container backend
    container_id: str | None = None  # Only for local container backend
    created_at: float = field(default_factory=time.time)
    # Ephemeral control-plane credentials reconstructed from local Docker
    # discovery. Intentionally excluded from to_dict() and repr so they cannot
    # leak through metadata persistence or routine lifecycle logs.
    request_headers: dict[str, str] = field(default_factory=dict, repr=False, compare=False)
    # Discovery-only lifecycle signal. A backend may report a running sandbox
    # whose persisted provisioning policy is incompatible with this process,
    # but it must not destroy that sandbox while merely enumerating it. The
    # provider consumes this flag and performs replacement only after obtaining
    # its local teardown reservation and cross-instance teardown lease.
    requires_replacement: bool = field(default=False, repr=False, compare=False)
    # How the backend's ``create`` call obtained this resource: ``created``
    # when it started the container or Pod in that call, ``rediscovered`` when
    # it returned one that already existed under the deterministic name, and
    # ``unknown`` when it did not say. The provider's cancellation rollback and
    # its resource accounting key off this; an unknown value is never treated
    # as proof of fresh creation. A lifecycle signal like ``requires_replacement``:
    # never persisted, never compared.
    provenance: str = field(default=PROVENANCE_UNKNOWN, repr=False, compare=False)
    # Whose sandbox this is, ``(user_id, thread_id)``, as the backend recorded
    # it on the resource when it created it: what lets a process that adopts
    # the sandbox after a restart stop it when that owner is turned off.
    # ``None`` for a resource created before the record existed, or by a
    # backend that keeps none. Discovery-only: never persisted, never compared.
    owner: tuple[str, str] | None = field(default=None, repr=False, compare=False)
    # Process-local warm-pool continuation. Never serialize it: another
    # Gateway cannot inherit this client's execution/session ownership.
    default_shell_state: tuple[str, str | None] | None = field(default=None, repr=False, compare=False)

    def to_dict(self) -> dict:
        return {
            "sandbox_id": self.sandbox_id,
            "sandbox_url": self.sandbox_url,
            "container_name": self.container_name,
            "container_id": self.container_id,
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, data: dict) -> SandboxInfo:
        return cls(
            sandbox_id=data["sandbox_id"],
            sandbox_url=data.get("sandbox_url", data.get("base_url", "")),
            container_name=data.get("container_name"),
            container_id=data.get("container_id"),
            created_at=data.get("created_at", time.time()),
        )
