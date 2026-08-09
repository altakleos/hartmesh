"""Sandbox metadata for cross-process discovery and state persistence."""

from __future__ import annotations

import re
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType

_DIGEST = re.compile(r"[0-9a-f]{64}\Z")


@dataclass(frozen=True, slots=True)
class AcceptedSkillMaterialReceiptV1:
    """Runtime-only proof returned by the Kubernetes materialization adapter."""

    profile: str
    attempt_id: str
    snapshot_id: str
    content_digest: str
    run_id: str
    generation: int
    pod_uid: str
    lease_uid: str
    runtime_image_ids_digest: str
    verifier_receipt_digest: str
    materialization_evidence_digest: str

    def __post_init__(self) -> None:
        if self.profile != "rwx_verified_copy_v1":
            raise ValueError("accepted skill profile is invalid")
        for value, field_name, maximum in (
            (self.attempt_id, "attempt_id", 128),
            (self.run_id, "run_id", 512),
            (self.pod_uid, "pod_uid", 128),
            (self.lease_uid, "lease_uid", 128),
        ):
            if not isinstance(value, str) or not value or len(value.encode("utf-8")) > maximum or any(ord(character) < 32 for character in value):
                raise ValueError(f"accepted skill {field_name} is invalid")
        for value, field_name in (
            (self.snapshot_id, "snapshot_id"),
            (self.content_digest, "content_digest"),
            (self.runtime_image_ids_digest, "runtime_image_ids_digest"),
            (self.verifier_receipt_digest, "verifier_receipt_digest"),
            (
                self.materialization_evidence_digest,
                "materialization_evidence_digest",
            ),
        ):
            if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
                raise ValueError(f"accepted skill {field_name} is invalid")
        if self.content_digest != self.snapshot_id:
            raise ValueError("accepted skill content digest is invalid")
        if type(self.generation) is not int or self.generation < 0:
            raise ValueError("accepted skill generation is invalid")


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
    request_headers: Mapping[str, str] = field(
        default_factory=lambda: MappingProxyType({}),
        repr=False,
        compare=False,
    )
    accepted_skill_material: AcceptedSkillMaterialReceiptV1 | None = field(
        default=None,
        repr=False,
        compare=False,
    )

    def __post_init__(self) -> None:
        self.request_headers = MappingProxyType(dict(self.request_headers))

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
