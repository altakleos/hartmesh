"""Sandbox metadata for cross-process discovery and state persistence."""

from __future__ import annotations

import hashlib
import json
import re
import time
from dataclasses import dataclass, field

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

    def to_wire(self) -> dict[str, object]:
        return {
            "version": 1,
            **{name: getattr(self, name) for name in self.__dataclass_fields__},
        }


@dataclass(frozen=True, slots=True)
class AcceptedSkillMaterialReceiptV2:
    """Complete v2 proof of one isolated Kubernetes materialization."""

    profile: str
    attempt_id: str
    snapshot_id: str
    content_digest: str
    run_id: str
    generation: int
    pod_uid: str
    pod_isolation_digest: str
    lease_uid: str
    network_policy_uid: str
    network_policy_spec_digest: str
    evidence_secret_uid: str
    evidence_secret_digest: str
    capability_secret_uid: str
    capability_secret_digest: str
    sandbox_image_digest: str
    accepted_skill_runtime_image_digest: str
    runtime_image_ids_digest: str
    verifier_receipt_digest: str
    materialization_evidence_digest: str

    def __post_init__(self) -> None:
        if self.profile != "rwx_verified_copy_v2":
            raise ValueError("accepted skill profile is invalid")
        for value, field_name, maximum in (
            (self.attempt_id, "attempt_id", 128),
            (self.run_id, "run_id", 512),
            (self.pod_uid, "pod_uid", 128),
            (self.lease_uid, "lease_uid", 128),
            (self.network_policy_uid, "network_policy_uid", 128),
            (self.evidence_secret_uid, "evidence_secret_uid", 128),
            (self.capability_secret_uid, "capability_secret_uid", 128),
        ):
            if not isinstance(value, str) or not value or len(value.encode("utf-8")) > maximum or any(ord(character) < 32 for character in value):
                raise ValueError(f"accepted skill {field_name} is invalid")
        for field_name in (
            "snapshot_id",
            "content_digest",
            "pod_isolation_digest",
            "network_policy_spec_digest",
            "evidence_secret_digest",
            "capability_secret_digest",
            "sandbox_image_digest",
            "accepted_skill_runtime_image_digest",
            "runtime_image_ids_digest",
            "verifier_receipt_digest",
            "materialization_evidence_digest",
        ):
            value = getattr(self, field_name)
            if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
                raise ValueError(f"accepted skill {field_name} is invalid")
        if self.content_digest != self.snapshot_id:
            raise ValueError("accepted skill content digest is invalid")
        if type(self.generation) is not int or self.generation < 0:
            raise ValueError("accepted skill generation is invalid")
        materialization_wire = {
            "version": 2,
            **{name: getattr(self, name) for name in self.__dataclass_fields__ if name != "materialization_evidence_digest"},
        }
        expected_materialization_digest = hashlib.sha256(
            json.dumps(
                materialization_wire,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8"),
        ).hexdigest()
        if self.materialization_evidence_digest != expected_materialization_digest:
            raise ValueError(
                "accepted skill materialization evidence digest is invalid",
            )

    def to_wire(self) -> dict[str, object]:
        return {
            "version": 2,
            **{name: getattr(self, name) for name in self.__dataclass_fields__},
        }


AcceptedSkillMaterialReceipt = AcceptedSkillMaterialReceiptV1 | AcceptedSkillMaterialReceiptV2


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
    accepted_skill_material: AcceptedSkillMaterialReceipt | None = field(
        default=None,
        repr=False,
        compare=False,
    )
    # Discovery-only lifecycle signal. A backend may report a running sandbox
    # whose persisted provisioning policy is incompatible with this process,
    # but it must not destroy that sandbox while merely enumerating it. The
    # provider consumes this flag and performs replacement only after obtaining
    # its local teardown reservation and cross-instance teardown lease.
    requires_replacement: bool = field(default=False, repr=False, compare=False)

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
