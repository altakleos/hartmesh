"""Provider-neutral accepted skill materialization contracts.

The durable runtime persists only these bounded, canonical values. Provider
SDK objects, credentials, and renewal handles remain process-local.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import stat
import threading
import unicodedata
import uuid
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Any, Literal, NoReturn, Protocol, Self

from deerflow_extension_api import TenantReferenceV1

from deerflow.sandbox.egress import EgressAllowanceV1, EgressPolicyError
from deerflow.sandbox.operations import SandboxOperationKind, fenced_sandbox_facade, sandbox_operations
from deerflow.sandbox.sandbox import Sandbox

if TYPE_CHECKING:
    from deerflow.runtime.skill_projection import SkillProjectionConsumerToken
    from deerflow.sandbox.session import SandboxSessionDeclaration

_DIGEST_PATTERN = re.compile(r"^[0-9a-f]{64}$", re.ASCII)
_OPERATION_REF_PATTERN = re.compile(
    r"^accepted-operation-[0-9a-f]{32}$",
    re.ASCII,
)
_MAX_REFERENCE_BYTES = 512
_MAX_PATH_BYTES = 512
_MAX_PATH_DEPTH = 32
_MAX_FILE_BYTES = 2 * 1024 * 1024
_MAX_TOTAL_FILES = 2_048
_MAX_TOTAL_BYTES = 32 * 1024 * 1024


def _canonical_digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
            allow_nan=False,
        ).encode("utf-8"),
    ).hexdigest()


def _require_digest(value: object, field_name: str) -> str:
    if not isinstance(value, str) or _DIGEST_PATTERN.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be a lowercase SHA-256 digest")
    return value


def _require_reference(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value or len(value.encode("utf-8")) > _MAX_REFERENCE_BYTES or any(ord(character) < 0x20 or ord(character) == 0x7F for character in value):
        raise ValueError(f"{field_name} must be a bounded control-free string")
    return value


def _canonical_timestamp(value: datetime) -> str:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("lease_expires_at must be timezone-aware")
    rendered = value.astimezone(UTC).isoformat()
    return rendered.removesuffix("+00:00") + "Z"


def _parse_timestamp(value: object) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise ValueError("lease_expires_at must be canonical UTC")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise ValueError("lease_expires_at must be canonical UTC") from exc
    if _canonical_timestamp(parsed) != value:
        raise ValueError("lease_expires_at must be canonical UTC")
    return parsed


def _validate_relative_path(value: object, *, field_name: str = "path") -> str:
    if not isinstance(value, str) or not value or "\\" in value:
        raise ValueError(f"accepted material {field_name} is invalid")
    if unicodedata.normalize("NFC", value) != value:
        raise ValueError(f"accepted material {field_name} changes under normalization")
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise ValueError(f"accepted material {field_name} is invalid") from exc
    path = PurePosixPath(value)
    if (
        path.is_absolute()
        or value != path.as_posix()
        or len(encoded) > _MAX_PATH_BYTES
        or len(path.parts) > _MAX_PATH_DEPTH
        or any(part in {"", ".", ".."} for part in path.parts)
        or any(ord(character) < 0x20 or ord(character) == 0x7F for character in value)
    ):
        raise ValueError(f"accepted material {field_name} is invalid")
    return value


def _validate_manifest_closure(entries: tuple[AcceptedFileV1, ...]) -> None:
    by_path = {entry.path: entry for entry in entries}
    for entry in entries:
        for parent in PurePosixPath(entry.path).parents:
            parent_path = parent.as_posix()
            if parent_path == ".":
                continue
            parent_entry = by_path.get(parent_path)
            if parent_entry is None or parent_entry.file_type is not AcceptedFileType.DIRECTORY:
                raise ValueError(
                    "accepted material file manifest has a missing or non-directory parent",
                )

    for entry in entries:
        if entry.file_type is not AcceptedFileType.SYMLINK:
            continue
        seen = {entry.path}
        current = entry
        for _ in range(_MAX_PATH_DEPTH):
            assert current.link_target is not None
            target = PurePosixPath(current.path).parent / current.link_target
            target_path = target.as_posix()
            _validate_relative_path(target_path, field_name="resolved link target")
            if target_path in seen:
                raise ValueError("accepted material file manifest has a symlink cycle")
            seen.add(target_path)
            target_entry = by_path.get(target_path)
            if target_entry is None:
                raise ValueError("accepted material file manifest has a dangling symlink")
            if target_entry.file_type is not AcceptedFileType.SYMLINK:
                break
            current = target_entry
        else:
            raise ValueError("accepted material file manifest has an excessive symlink chain")


class AcceptedFileType(StrEnum):
    """File kinds representable by the provider-neutral manifest."""

    REGULAR = "regular"
    DIRECTORY = "directory"
    SYMLINK = "symlink"


class AcceptedMaterialCapability(StrEnum):
    """Strongest accepted-material boundary an adapter can prove."""

    EMPTY_ONLY = "empty_only"
    IMMUTABLE_READ_ONLY = "immutable_read_only"


@dataclass(frozen=True, slots=True)
class AcceptedSandboxCapabilityProfileV1:
    """Versioned declaration of execution guarantees an adapter implements.

    This is a declaration, not qualification evidence. Durable admission also
    requires a separately verified :class:`AcceptedSandboxQualificationV1`.
    """

    version: Literal[1]
    material_capability: AcceptedMaterialCapability
    atomic_provider_ownership_fencing: bool
    atomic_provider_operation_fencing: bool
    authoritative_shared_expiry: bool
    resolved_immutable_image: bool
    restricted_non_root_isolation: bool
    recoverable_resource_lookup: bool
    durable_one_replica: bool
    exact_two: bool
    digest: str

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != 1:
            raise ValueError("accepted sandbox capability profile version must be 1")
        try:
            material_capability = AcceptedMaterialCapability(
                self.material_capability,
            )
        except (TypeError, ValueError) as exc:
            raise ValueError("accepted sandbox material capability is invalid") from exc
        object.__setattr__(self, "material_capability", material_capability)
        for field_name in (
            "atomic_provider_ownership_fencing",
            "atomic_provider_operation_fencing",
            "authoritative_shared_expiry",
            "resolved_immutable_image",
            "restricted_non_root_isolation",
            "recoverable_resource_lookup",
            "durable_one_replica",
            "exact_two",
        ):
            if type(getattr(self, field_name)) is not bool:
                raise TypeError(f"{field_name} must be bool")
        if self.atomic_provider_operation_fencing and not self.atomic_provider_ownership_fencing:
            raise ValueError(
                "atomic operation fencing requires atomic provider ownership fencing",
            )
        if self.exact_two and (not self.durable_one_replica or not self.atomic_provider_operation_fencing or not self.recoverable_resource_lookup):
            raise ValueError(
                "exact-two capability requires durable one-replica, atomic operation fencing, and recoverable lookup",
            )
        if self.durable_one_replica and (
            material_capability is not AcceptedMaterialCapability.IMMUTABLE_READ_ONLY
            or not self.atomic_provider_ownership_fencing
            or not self.authoritative_shared_expiry
            or not self.resolved_immutable_image
            or not self.restricted_non_root_isolation
        ):
            raise ValueError(
                "durable one-replica capability is missing a required guarantee",
            )
        _require_digest(self.digest, "accepted sandbox capability profile digest")
        if self.digest != _canonical_digest(self._digest_payload()):
            raise ValueError("accepted sandbox capability profile digest is invalid")

    def _digest_payload(self) -> dict[str, object]:
        return {
            "version": self.version,
            "material_capability": self.material_capability.value,
            "atomic_provider_ownership_fencing": (self.atomic_provider_ownership_fencing),
            "atomic_provider_operation_fencing": (self.atomic_provider_operation_fencing),
            "authoritative_shared_expiry": self.authoritative_shared_expiry,
            "resolved_immutable_image": self.resolved_immutable_image,
            "restricted_non_root_isolation": self.restricted_non_root_isolation,
            "recoverable_resource_lookup": self.recoverable_resource_lookup,
            "durable_one_replica": self.durable_one_replica,
            "exact_two": self.exact_two,
        }

    def to_persisted(self) -> dict[str, object]:
        return {**self._digest_payload(), "digest": self.digest}

    @classmethod
    def build(
        cls,
        *,
        material_capability: AcceptedMaterialCapability,
        atomic_provider_ownership_fencing: bool,
        atomic_provider_operation_fencing: bool,
        authoritative_shared_expiry: bool,
        resolved_immutable_image: bool,
        restricted_non_root_isolation: bool,
        recoverable_resource_lookup: bool,
        durable_one_replica: bool,
        exact_two: bool,
    ) -> Self:
        payload = {
            "version": 1,
            "material_capability": AcceptedMaterialCapability(
                material_capability,
            ).value,
            "atomic_provider_ownership_fencing": (atomic_provider_ownership_fencing),
            "atomic_provider_operation_fencing": (atomic_provider_operation_fencing),
            "authoritative_shared_expiry": authoritative_shared_expiry,
            "resolved_immutable_image": resolved_immutable_image,
            "restricted_non_root_isolation": restricted_non_root_isolation,
            "recoverable_resource_lookup": recoverable_resource_lookup,
            "durable_one_replica": durable_one_replica,
            "exact_two": exact_two,
        }
        return cls(
            **payload,
            digest=_canonical_digest(payload),
        )

    @classmethod
    def from_persisted(cls, value: object) -> Self:
        fields = {
            "version",
            "material_capability",
            "atomic_provider_ownership_fencing",
            "atomic_provider_operation_fencing",
            "authoritative_shared_expiry",
            "resolved_immutable_image",
            "restricted_non_root_isolation",
            "recoverable_resource_lookup",
            "durable_one_replica",
            "exact_two",
            "digest",
        }
        if not isinstance(value, Mapping) or set(value) != fields:
            raise ValueError(
                "accepted sandbox capability profile has unknown or missing fields",
            )
        return cls(**value)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class AcceptedSandboxQualificationV1:
    """Bounded reference to independently verified, topology-bound evidence."""

    version: Literal[1]
    capability_profile_digest: str
    qualification_scope: str
    artifact_digest: str
    topology_digest: str
    verified_at: datetime
    expires_at: datetime
    status: Literal["passed", "candidate"]
    digest: str

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != 1:
            raise ValueError("accepted sandbox qualification version must be 1")
        for field_name in (
            "capability_profile_digest",
            "artifact_digest",
            "topology_digest",
        ):
            _require_digest(getattr(self, field_name), field_name)
        _require_reference(self.qualification_scope, "qualification_scope")
        _canonical_timestamp(self.verified_at)
        _canonical_timestamp(self.expires_at)
        if self.verified_at >= self.expires_at:
            raise ValueError("accepted sandbox qualification expiry is invalid")
        if self.status not in {"passed", "candidate"}:
            raise ValueError(
                "accepted sandbox qualification status must be passed or candidate",
            )
        _require_digest(self.digest, "accepted sandbox qualification digest")
        if self.digest != _canonical_digest(self._digest_payload()):
            raise ValueError("accepted sandbox qualification digest is invalid")

    def _digest_payload(self) -> dict[str, object]:
        return {
            "version": self.version,
            "capability_profile_digest": self.capability_profile_digest,
            "qualification_scope": self.qualification_scope,
            "artifact_digest": self.artifact_digest,
            "topology_digest": self.topology_digest,
            "verified_at": _canonical_timestamp(self.verified_at),
            "expires_at": _canonical_timestamp(self.expires_at),
            "status": self.status,
        }

    def to_persisted(self) -> dict[str, object]:
        return {**self._digest_payload(), "digest": self.digest}

    def is_current(self, at: datetime) -> bool:
        _canonical_timestamp(at)
        return self.status == "passed" and self.verified_at <= at < self.expires_at

    def is_candidate_current(self, at: datetime) -> bool:
        """Return whether an explicitly isolated qualification run may use it."""

        _canonical_timestamp(at)
        return self.status == "candidate" and self.verified_at <= at < self.expires_at

    @classmethod
    def build(
        cls,
        *,
        capability_profile_digest: str,
        qualification_scope: str,
        artifact_digest: str,
        topology_digest: str,
        verified_at: datetime,
        expires_at: datetime,
        status: Literal["passed", "candidate"] = "passed",
    ) -> Self:
        payload = {
            "version": 1,
            "capability_profile_digest": capability_profile_digest,
            "qualification_scope": qualification_scope,
            "artifact_digest": artifact_digest,
            "topology_digest": topology_digest,
            "verified_at": _canonical_timestamp(verified_at),
            "expires_at": _canonical_timestamp(expires_at),
            "status": status,
        }
        return cls(
            version=1,
            capability_profile_digest=capability_profile_digest,
            qualification_scope=qualification_scope,
            artifact_digest=artifact_digest,
            topology_digest=topology_digest,
            verified_at=verified_at,
            expires_at=expires_at,
            status=status,
            digest=_canonical_digest(payload),
        )

    @classmethod
    def from_persisted(cls, value: object) -> Self:
        fields = {
            "version",
            "capability_profile_digest",
            "qualification_scope",
            "artifact_digest",
            "topology_digest",
            "verified_at",
            "expires_at",
            "status",
            "digest",
        }
        if not isinstance(value, Mapping) or set(value) != fields:
            raise ValueError(
                "accepted sandbox qualification has unknown or missing fields",
            )
        return cls(
            version=value["version"],  # type: ignore[arg-type]
            capability_profile_digest=value["capability_profile_digest"],  # type: ignore[arg-type]
            qualification_scope=value["qualification_scope"],  # type: ignore[arg-type]
            artifact_digest=value["artifact_digest"],  # type: ignore[arg-type]
            topology_digest=value["topology_digest"],  # type: ignore[arg-type]
            verified_at=_parse_timestamp(value["verified_at"]),
            expires_at=_parse_timestamp(value["expires_at"]),
            status=value["status"],  # type: ignore[arg-type]
            digest=value["digest"],  # type: ignore[arg-type]
        )


@dataclass(frozen=True, slots=True)
class AcceptedSandboxIsolationFactsV1:
    """Qualified, portable isolation facts without provider resource identity."""

    version: Literal[1]
    restricted_non_root: bool
    read_only_accepted_material: bool
    privilege_escalation_disabled: bool
    runtime_class_digest: str | None
    network_policy_digest: str | None
    digest: str

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != 1:
            raise ValueError("accepted sandbox isolation facts version must be 1")
        for field_name in (
            "restricted_non_root",
            "read_only_accepted_material",
            "privilege_escalation_disabled",
        ):
            if type(getattr(self, field_name)) is not bool:
                raise TypeError(f"{field_name} must be bool")
        for field_name in ("runtime_class_digest", "network_policy_digest"):
            value = getattr(self, field_name)
            if value is not None:
                _require_digest(value, field_name)
        _require_digest(self.digest, "accepted sandbox isolation facts digest")
        if self.digest != _canonical_digest(self._digest_payload()):
            raise ValueError("accepted sandbox isolation facts digest is invalid")

    def _digest_payload(self) -> dict[str, object]:
        return {
            "version": self.version,
            "restricted_non_root": self.restricted_non_root,
            "read_only_accepted_material": self.read_only_accepted_material,
            "privilege_escalation_disabled": self.privilege_escalation_disabled,
            "runtime_class_digest": self.runtime_class_digest,
            "network_policy_digest": self.network_policy_digest,
        }

    def to_persisted(self) -> dict[str, object]:
        return {**self._digest_payload(), "digest": self.digest}

    @classmethod
    def build(
        cls,
        *,
        restricted_non_root: bool,
        read_only_accepted_material: bool,
        privilege_escalation_disabled: bool,
        runtime_class_digest: str | None,
        network_policy_digest: str | None,
    ) -> Self:
        payload = {
            "version": 1,
            "restricted_non_root": restricted_non_root,
            "read_only_accepted_material": read_only_accepted_material,
            "privilege_escalation_disabled": privilege_escalation_disabled,
            "runtime_class_digest": runtime_class_digest,
            "network_policy_digest": network_policy_digest,
        }
        return cls(**payload, digest=_canonical_digest(payload))

    @classmethod
    def from_persisted(cls, value: object) -> Self:
        fields = {
            "version",
            "restricted_non_root",
            "read_only_accepted_material",
            "privilege_escalation_disabled",
            "runtime_class_digest",
            "network_policy_digest",
            "digest",
        }
        if not isinstance(value, Mapping) or set(value) != fields:
            raise ValueError(
                "accepted sandbox isolation facts have unknown or missing fields",
            )
        return cls(**value)  # type: ignore[arg-type]


class AcceptedSkillSandboxBindingError(RuntimeError):
    """Fail-closed error for an unavailable accepted-skill projection."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


class AcceptedMaterialError(RuntimeError):
    """Stable fail-closed error raised by a materializer adapter."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


class AcceptedSandboxAuthorityLostError(AcceptedMaterialError):
    """Stable fail-closed signal that accepted execution authority was lost."""


@dataclass(frozen=True, slots=True)
class AcceptedMaterialExecutionClaimV1:
    """Mutable owner/epoch authority kept outside immutable material evidence."""

    version: Literal[1]
    tenant_digest: str
    run_id: str
    owner_worker_id: str
    state_version: int
    execution_takeover: bool
    expected_materialization_digest: str | None = None

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != 1:
            raise ValueError("accepted material execution claim version must be 1")
        _require_digest(self.tenant_digest, "tenant_digest")
        _require_reference(self.run_id, "run_id")
        _require_reference(self.owner_worker_id, "owner_worker_id")
        if type(self.state_version) is not int or self.state_version < 0:
            raise ValueError("state_version must be a non-negative integer")
        if type(self.execution_takeover) is not bool:
            raise TypeError("execution_takeover must be bool")
        if self.execution_takeover:
            _require_digest(
                self.expected_materialization_digest,
                "expected_materialization_digest",
            )
        elif self.expected_materialization_digest is not None:
            raise ValueError(
                "initial execution claim cannot carry materialization evidence",
            )

    def binds(self, request: AcceptedMaterialRequest) -> bool:
        return self.tenant_digest == request.tenant.digest and self.run_id == request.run_id

    def to_wire(self) -> dict[str, object]:
        return {
            "version": self.version,
            "tenant_digest": self.tenant_digest,
            "run_id": self.run_id,
            "owner_worker_id": self.owner_worker_id,
            "state_version": self.state_version,
            "execution_takeover": self.execution_takeover,
            "expected_materialization_digest": self.expected_materialization_digest,
        }


@dataclass(frozen=True, slots=True)
class AcceptedSkillExecutionEvidenceV1:
    """Legacy bounded proof retained for persisted v1 AIO records."""

    profile: str
    attempt_id: str
    snapshot_id: str
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
            (self.runtime_image_ids_digest, "runtime_image_ids_digest"),
            (self.verifier_receipt_digest, "verifier_receipt_digest"),
            (
                self.materialization_evidence_digest,
                "materialization_evidence_digest",
            ),
        ):
            if not isinstance(value, str) or _DIGEST_PATTERN.fullmatch(value) is None:
                raise ValueError(f"accepted skill {field_name} is invalid")
        if type(self.generation) is not int or self.generation < 0:
            raise ValueError("accepted skill generation is invalid")

    def to_persisted(self) -> dict[str, object]:
        return {
            "version": 1,
            "profile": self.profile,
            "attempt_id": self.attempt_id,
            "snapshot_id": self.snapshot_id,
            "run_id": self.run_id,
            "generation": self.generation,
            "pod_uid": self.pod_uid,
            "lease_uid": self.lease_uid,
            "runtime_image_ids_digest": self.runtime_image_ids_digest,
            "verifier_receipt_digest": self.verifier_receipt_digest,
            "materialization_evidence_digest": self.materialization_evidence_digest,
        }

    @property
    def digest(self) -> str:
        return _canonical_digest(self.to_persisted())


@dataclass(frozen=True, slots=True)
class AcceptedSkillExecutionEvidenceV2:
    """Complete non-secret proof of one v2 Kubernetes execution tuple."""

    profile: str
    attempt_id: str
    snapshot_id: str
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
            if not isinstance(value, str) or _DIGEST_PATTERN.fullmatch(value) is None:
                raise ValueError(f"accepted skill {field_name} is invalid")
        if type(self.generation) is not int or self.generation < 0:
            raise ValueError("accepted skill generation is invalid")
        materialization_wire = {
            "version": 2,
            **{name: getattr(self, name) for name in self.__dataclass_fields__ if name != "materialization_evidence_digest"},
            "content_digest": self.snapshot_id,
        }
        if self.materialization_evidence_digest != _canonical_digest(
            materialization_wire,
        ):
            raise ValueError("accepted skill materialization evidence digest is invalid")

    def to_persisted(self) -> dict[str, object]:
        return {
            "version": 2,
            **{name: getattr(self, name) for name in self.__dataclass_fields__},
        }

    @property
    def digest(self) -> str:
        return _canonical_digest(self.to_persisted())


AcceptedSkillExecutionEvidence = AcceptedSkillExecutionEvidenceV1 | AcceptedSkillExecutionEvidenceV2


@dataclass(frozen=True, slots=True)
class AcceptedSkillSandboxBindingV1:
    """Compatibility binding consumed by the existing AIO adapter."""

    snapshot_id: str | None
    run_id: str = "legacy"
    generation: int = 0
    evidence: object | None = None

    def __post_init__(self) -> None:
        if self.snapshot_id is not None and _DIGEST_PATTERN.fullmatch(self.snapshot_id) is None:
            raise ValueError(
                "accepted skill snapshot_id must be a lowercase SHA-256 digest",
            )
        if not isinstance(self.run_id, str) or not self.run_id:
            raise ValueError("accepted skill run_id must be non-empty")
        if type(self.generation) is not int or self.generation < 0:
            raise ValueError(
                "accepted skill generation must be a non-negative integer",
            )

    @classmethod
    def from_consumer_token(
        cls,
        token: SkillProjectionConsumerToken,
    ) -> Self:
        from deerflow.runtime.skill_projection import SkillProjectionConsumerToken

        if not isinstance(token, SkillProjectionConsumerToken):
            raise TypeError("accepted skill projection token is invalid")
        return cls(
            snapshot_id=token.snapshot_id,
            run_id=token.run_id,
            generation=token.generation,
            evidence=token.evidence,
        )


@dataclass(frozen=True, slots=True)
class AcceptedFileV1:
    """One bounded manifest entry in an accepted immutable snapshot."""

    version: Literal[1]
    path: str
    file_type: AcceptedFileType
    size: int
    mode: int
    digest: str | None
    link_target: str | None = None

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != 1:
            raise ValueError("accepted material file version must be 1")
        _validate_relative_path(self.path)
        try:
            file_type = AcceptedFileType(self.file_type)
        except (TypeError, ValueError) as exc:
            raise ValueError("accepted material file type is invalid") from exc
        object.__setattr__(self, "file_type", file_type)
        if type(self.size) is not int or self.size < 0 or self.size > _MAX_FILE_BYTES:
            raise ValueError("accepted material file size is invalid")
        if type(self.mode) is not int or self.mode < 0 or self.mode > 0o7777 or self.mode & 0o7222:
            raise ValueError("accepted material mode must be read-only without special bits")

        if file_type is AcceptedFileType.REGULAR:
            _require_digest(self.digest, "accepted material file digest")
            if self.link_target is not None:
                raise ValueError("regular accepted material cannot have a link target")
        elif file_type is AcceptedFileType.DIRECTORY:
            if self.size != 0 or self.digest is not None or self.link_target is not None:
                raise ValueError("accepted material directory metadata is invalid")
        else:
            target = _validate_relative_path(self.link_target, field_name="link_target")
            if self.mode != 0 or self.size != len(target.encode("utf-8")):
                raise ValueError("accepted material symlink metadata is invalid")
            if self.digest != hashlib.sha256(target.encode("utf-8")).hexdigest():
                raise ValueError("accepted material symlink digest is invalid")

    def to_persisted(self) -> dict[str, object]:
        return {
            "version": self.version,
            "path": self.path,
            "file_type": self.file_type.value,
            "size": self.size,
            "mode": self.mode,
            "digest": self.digest,
            "link_target": self.link_target,
        }

    @classmethod
    def from_persisted(cls, value: object) -> Self:
        fields = {
            "version",
            "path",
            "file_type",
            "size",
            "mode",
            "digest",
            "link_target",
        }
        if not isinstance(value, Mapping) or set(value) != fields:
            raise ValueError("accepted material file has unknown or missing fields")
        return cls(
            version=value["version"],  # type: ignore[arg-type]
            path=value["path"],  # type: ignore[arg-type]
            file_type=value["file_type"],  # type: ignore[arg-type]
            size=value["size"],  # type: ignore[arg-type]
            mode=value["mode"],  # type: ignore[arg-type]
            digest=value["digest"],  # type: ignore[arg-type]
            link_target=value["link_target"],  # type: ignore[arg-type]
        )


def capture_accepted_file_manifest(root: Path) -> tuple[AcceptedFileV1, ...]:
    """Capture exact immutable snapshot bytes into the neutral manifest shape."""

    if not isinstance(root, Path):
        raise TypeError("accepted material root must be a Path")
    try:
        root_stat = root.lstat()
    except OSError as exc:
        raise AcceptedMaterialError("accepted_material_manifest_unavailable") from exc
    if not stat.S_ISDIR(root_stat.st_mode) or stat.S_ISLNK(root_stat.st_mode):
        raise AcceptedMaterialError("accepted_material_manifest_root_invalid")

    entries: list[AcceptedFileV1] = []
    total_bytes = 0
    pending = [root]
    try:
        while pending:
            directory = pending.pop()
            with os.scandir(directory) as scanned:
                children = sorted(scanned, key=lambda child: child.name)
            for child in children:
                child_path = Path(child.path)
                relative = child_path.relative_to(root).as_posix()
                _validate_relative_path(relative)
                metadata = child.stat(follow_symlinks=False)
                mode = stat.S_IMODE(metadata.st_mode)
                if stat.S_ISDIR(metadata.st_mode):
                    entry = AcceptedFileV1(
                        version=1,
                        path=relative,
                        file_type=AcceptedFileType.DIRECTORY,
                        size=0,
                        mode=mode,
                        digest=None,
                    )
                    pending.append(child_path)
                elif stat.S_ISLNK(metadata.st_mode):
                    target = os.readlink(child_path)
                    target_bytes = target.encode("utf-8")
                    entry = AcceptedFileV1(
                        version=1,
                        path=relative,
                        file_type=AcceptedFileType.SYMLINK,
                        size=len(target_bytes),
                        mode=0,
                        digest=hashlib.sha256(target_bytes).hexdigest(),
                        link_target=target,
                    )
                elif stat.S_ISREG(metadata.st_mode):
                    if metadata.st_nlink != 1 or metadata.st_size > _MAX_FILE_BYTES:
                        raise AcceptedMaterialError("accepted_material_manifest_file_invalid")
                    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
                    descriptor = os.open(child_path, flags)
                    try:
                        opened = os.fstat(descriptor)
                        identity = (
                            metadata.st_dev,
                            metadata.st_ino,
                            metadata.st_size,
                            metadata.st_mtime_ns,
                            stat.S_IMODE(metadata.st_mode),
                        )
                        opened_identity = (
                            opened.st_dev,
                            opened.st_ino,
                            opened.st_size,
                            opened.st_mtime_ns,
                            stat.S_IMODE(opened.st_mode),
                        )
                        if identity != opened_identity or opened.st_nlink != 1:
                            raise AcceptedMaterialError("accepted_material_manifest_changed")
                        digest = hashlib.sha256()
                        remaining = opened.st_size
                        while remaining:
                            chunk = os.read(descriptor, min(64 * 1024, remaining))
                            if not chunk:
                                raise AcceptedMaterialError("accepted_material_manifest_changed")
                            digest.update(chunk)
                            remaining -= len(chunk)
                        if os.read(descriptor, 1):
                            raise AcceptedMaterialError("accepted_material_manifest_changed")
                    finally:
                        os.close(descriptor)
                    entry = AcceptedFileV1(
                        version=1,
                        path=relative,
                        file_type=AcceptedFileType.REGULAR,
                        size=metadata.st_size,
                        mode=mode,
                        digest=digest.hexdigest(),
                    )
                    total_bytes += metadata.st_size
                else:
                    raise AcceptedMaterialError("accepted_material_manifest_file_invalid")
                entries.append(entry)
                if len(entries) > _MAX_TOTAL_FILES or total_bytes > _MAX_TOTAL_BYTES:
                    raise AcceptedMaterialError("accepted_material_manifest_too_large")
    except AcceptedMaterialError:
        raise
    except (OSError, UnicodeError, ValueError) as exc:
        raise AcceptedMaterialError("accepted_material_manifest_invalid") from exc

    manifest = tuple(sorted(entries, key=lambda entry: entry.path))
    _validate_manifest_closure(manifest)
    return manifest


@dataclass(frozen=True, slots=True)
class AcceptedMaterialRequestV1:
    """Canonical provider-neutral request for one accepted snapshot."""

    version: Literal[1]
    run_id: str
    attempt_id: str
    tenant: TenantReferenceV1
    user_ref: str
    thread_ref: str
    agent_revision_digest: str
    skill_snapshot_digest: str
    skill_scope_digest: str
    file_manifest: tuple[AcceptedFileV1, ...]
    runtime_image_digest: str
    lease_expires_at: datetime
    digest: str

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != 1:
            raise ValueError("accepted material request version must be 1")
        for field_name in ("run_id", "attempt_id", "user_ref", "thread_ref"):
            _require_reference(getattr(self, field_name), field_name)
        if not isinstance(self.tenant, TenantReferenceV1):
            raise TypeError("tenant must be TenantReferenceV1")
        if type(self.tenant.version) is not int:
            raise ValueError("tenant version must be 1")
        for field_name in (
            "agent_revision_digest",
            "skill_snapshot_digest",
            "skill_scope_digest",
            "runtime_image_digest",
        ):
            _require_digest(getattr(self, field_name), field_name)
        if not isinstance(self.file_manifest, tuple) or len(self.file_manifest) > _MAX_TOTAL_FILES:
            raise ValueError("accepted material file manifest is invalid")
        if any(not isinstance(entry, AcceptedFileV1) for entry in self.file_manifest):
            raise TypeError("file_manifest must contain AcceptedFileV1 entries")
        paths = [entry.path for entry in self.file_manifest]
        if paths != sorted(paths):
            raise ValueError("accepted material file manifest must be sorted")
        if len(paths) != len(set(paths)) or len(paths) != len({path.casefold() for path in paths}):
            raise ValueError("accepted material file manifest has duplicate or case-conflicting paths")
        _validate_manifest_closure(self.file_manifest)
        total_bytes = sum(entry.size for entry in self.file_manifest)
        if total_bytes > _MAX_TOTAL_BYTES:
            raise ValueError("accepted material file manifest is too large")
        _canonical_timestamp(self.lease_expires_at)
        _require_digest(self.digest, "accepted material request digest")
        if self.digest != _canonical_digest(self._digest_payload()):
            raise ValueError("accepted material request digest is invalid")

    def _digest_payload(self) -> dict[str, object]:
        return {
            "version": self.version,
            "run_id": self.run_id,
            "attempt_id": self.attempt_id,
            "tenant": self.tenant.to_json(),
            "user_ref": self.user_ref,
            "thread_ref": self.thread_ref,
            "agent_revision_digest": self.agent_revision_digest,
            "skill_snapshot_digest": self.skill_snapshot_digest,
            "skill_scope_digest": self.skill_scope_digest,
            "file_manifest": [entry.to_persisted() for entry in self.file_manifest],
            "runtime_image_digest": self.runtime_image_digest,
            "lease_expires_at": _canonical_timestamp(self.lease_expires_at),
        }

    def to_persisted(self) -> dict[str, object]:
        return {**self._digest_payload(), "digest": self.digest}

    @classmethod
    def build(
        cls,
        *,
        run_id: str,
        attempt_id: str,
        tenant: TenantReferenceV1,
        user_ref: str,
        thread_ref: str,
        agent_revision_digest: str,
        skill_snapshot_digest: str,
        skill_scope_digest: str,
        file_manifest: Sequence[AcceptedFileV1],
        runtime_image_digest: str,
        lease_expires_at: datetime,
    ) -> Self:
        if not isinstance(file_manifest, Sequence) or isinstance(file_manifest, (str, bytes, bytearray)) or len(file_manifest) > _MAX_TOTAL_FILES:
            raise ValueError("accepted material file manifest is invalid")
        if any(not isinstance(entry, AcceptedFileV1) for entry in file_manifest):
            raise TypeError("file_manifest must contain AcceptedFileV1 entries")
        manifest = tuple(sorted(file_manifest, key=lambda entry: entry.path))
        payload = {
            "version": 1,
            "run_id": run_id,
            "attempt_id": attempt_id,
            "tenant": tenant.to_json(),
            "user_ref": user_ref,
            "thread_ref": thread_ref,
            "agent_revision_digest": agent_revision_digest,
            "skill_snapshot_digest": skill_snapshot_digest,
            "skill_scope_digest": skill_scope_digest,
            "file_manifest": [entry.to_persisted() for entry in manifest],
            "runtime_image_digest": runtime_image_digest,
            "lease_expires_at": _canonical_timestamp(lease_expires_at),
        }
        return cls(
            version=1,
            run_id=run_id,
            attempt_id=attempt_id,
            tenant=tenant,
            user_ref=user_ref,
            thread_ref=thread_ref,
            agent_revision_digest=agent_revision_digest,
            skill_snapshot_digest=skill_snapshot_digest,
            skill_scope_digest=skill_scope_digest,
            file_manifest=manifest,
            runtime_image_digest=runtime_image_digest,
            lease_expires_at=lease_expires_at,
            digest=_canonical_digest(payload),
        )

    @classmethod
    def from_persisted(cls, value: object) -> Self:
        fields = {
            "version",
            "run_id",
            "attempt_id",
            "tenant",
            "user_ref",
            "thread_ref",
            "agent_revision_digest",
            "skill_snapshot_digest",
            "skill_scope_digest",
            "file_manifest",
            "runtime_image_digest",
            "lease_expires_at",
            "digest",
        }
        if not isinstance(value, Mapping) or set(value) != fields:
            raise ValueError("accepted material request has unknown or missing fields")
        raw_manifest = value["file_manifest"]
        if (
            not isinstance(raw_manifest, Sequence)
            or isinstance(
                raw_manifest,
                (str, bytes, bytearray),
            )
            or len(raw_manifest) > _MAX_TOTAL_FILES
        ):
            raise ValueError("accepted material file manifest is invalid")
        return cls(
            version=value["version"],  # type: ignore[arg-type]
            run_id=value["run_id"],  # type: ignore[arg-type]
            attempt_id=value["attempt_id"],  # type: ignore[arg-type]
            tenant=TenantReferenceV1.from_json(value["tenant"]),
            user_ref=value["user_ref"],  # type: ignore[arg-type]
            thread_ref=value["thread_ref"],  # type: ignore[arg-type]
            agent_revision_digest=value["agent_revision_digest"],  # type: ignore[arg-type]
            skill_snapshot_digest=value["skill_snapshot_digest"],  # type: ignore[arg-type]
            skill_scope_digest=value["skill_scope_digest"],  # type: ignore[arg-type]
            file_manifest=tuple(AcceptedFileV1.from_persisted(entry) for entry in raw_manifest),
            runtime_image_digest=value["runtime_image_digest"],  # type: ignore[arg-type]
            lease_expires_at=_parse_timestamp(value["lease_expires_at"]),
            digest=value["digest"],  # type: ignore[arg-type]
        )


@dataclass(frozen=True, slots=True)
class AcceptedMaterialRequestV2:
    """V1 material facts plus immutable accepted-admission bindings."""

    version: Literal[2]
    run_id: str
    attempt_id: str
    tenant: TenantReferenceV1
    user_ref: str
    thread_ref: str
    agent_revision_digest: str
    skill_snapshot_digest: str
    skill_scope_digest: str
    file_manifest: tuple[AcceptedFileV1, ...]
    runtime_image_digest: str
    lease_expires_at: datetime
    accepted_invocation_ref: str
    accepted_invocation_digest: str
    tool_plane_base_revision_digest: str
    tool_plane_user_overlay_digest: str
    tool_plane_projection_digest: str
    tool_plane_effective_digest: str
    batch_child_attempt_ref: str | None
    capability_profile_digest: str
    digest: str
    # The run-bound egress the accepted Kind's Material renders; ``None`` only
    # for requests sealed before egress allowances existed.
    egress_allowance: EgressAllowanceV1 | None = None

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != 2:
            raise ValueError("accepted material request version must be 2")
        if self.egress_allowance is not None and not isinstance(self.egress_allowance, EgressAllowanceV1):
            raise TypeError("egress_allowance must be EgressAllowanceV1 or None")
        for field_name in (
            "run_id",
            "attempt_id",
            "user_ref",
            "thread_ref",
            "accepted_invocation_ref",
        ):
            _require_reference(getattr(self, field_name), field_name)
        if self.batch_child_attempt_ref is not None:
            _require_reference(
                self.batch_child_attempt_ref,
                "batch_child_attempt_ref",
            )
        if not isinstance(self.tenant, TenantReferenceV1):
            raise TypeError("tenant must be TenantReferenceV1")
        if type(self.tenant.version) is not int or self.tenant.version != 1:
            raise ValueError("tenant version must be 1")
        for field_name in (
            "agent_revision_digest",
            "skill_snapshot_digest",
            "skill_scope_digest",
            "runtime_image_digest",
            "accepted_invocation_digest",
            "tool_plane_base_revision_digest",
            "tool_plane_user_overlay_digest",
            "tool_plane_projection_digest",
            "tool_plane_effective_digest",
            "capability_profile_digest",
        ):
            _require_digest(getattr(self, field_name), field_name)
        if not isinstance(self.file_manifest, tuple) or len(self.file_manifest) > _MAX_TOTAL_FILES:
            raise ValueError("accepted material file manifest is invalid")
        if any(not isinstance(entry, AcceptedFileV1) for entry in self.file_manifest):
            raise TypeError("file_manifest must contain AcceptedFileV1 entries")
        paths = [entry.path for entry in self.file_manifest]
        if paths != sorted(paths):
            raise ValueError("accepted material file manifest must be sorted")
        if len(paths) != len(set(paths)) or len(paths) != len(
            {path.casefold() for path in paths},
        ):
            raise ValueError(
                "accepted material file manifest has duplicate or case-conflicting paths",
            )
        _validate_manifest_closure(self.file_manifest)
        if sum(entry.size for entry in self.file_manifest) > _MAX_TOTAL_BYTES:
            raise ValueError("accepted material file manifest is too large")
        _canonical_timestamp(self.lease_expires_at)
        _require_digest(self.digest, "accepted material request digest")
        if self.digest != _canonical_digest(self._digest_payload()):
            raise ValueError("accepted material request digest is invalid")

    def _digest_payload(self) -> dict[str, object]:
        return {
            "version": self.version,
            "run_id": self.run_id,
            "attempt_id": self.attempt_id,
            "tenant": self.tenant.to_json(),
            "user_ref": self.user_ref,
            "thread_ref": self.thread_ref,
            "agent_revision_digest": self.agent_revision_digest,
            "skill_snapshot_digest": self.skill_snapshot_digest,
            "skill_scope_digest": self.skill_scope_digest,
            "file_manifest": [entry.to_persisted() for entry in self.file_manifest],
            "runtime_image_digest": self.runtime_image_digest,
            "lease_expires_at": _canonical_timestamp(self.lease_expires_at),
            "accepted_invocation_ref": self.accepted_invocation_ref,
            "accepted_invocation_digest": self.accepted_invocation_digest,
            "tool_plane_base_revision_digest": (self.tool_plane_base_revision_digest),
            "tool_plane_user_overlay_digest": (self.tool_plane_user_overlay_digest),
            "tool_plane_projection_digest": self.tool_plane_projection_digest,
            "tool_plane_effective_digest": self.tool_plane_effective_digest,
            "batch_child_attempt_ref": self.batch_child_attempt_ref,
            "capability_profile_digest": self.capability_profile_digest,
            **({"egress_allowance": self.egress_allowance.to_json()} if self.egress_allowance is not None else {}),
        }

    def to_persisted(self) -> dict[str, object]:
        return {**self._digest_payload(), "digest": self.digest}

    @classmethod
    def build(
        cls,
        *,
        run_id: str,
        attempt_id: str,
        tenant: TenantReferenceV1,
        user_ref: str,
        thread_ref: str,
        agent_revision_digest: str,
        skill_snapshot_digest: str,
        skill_scope_digest: str,
        file_manifest: Sequence[AcceptedFileV1],
        runtime_image_digest: str,
        lease_expires_at: datetime,
        accepted_invocation_ref: str,
        accepted_invocation_digest: str,
        tool_plane_base_revision_digest: str,
        tool_plane_user_overlay_digest: str,
        tool_plane_projection_digest: str,
        tool_plane_effective_digest: str,
        batch_child_attempt_ref: str | None,
        capability_profile_digest: str,
        egress_allowance: EgressAllowanceV1 | None = None,
    ) -> Self:
        if not isinstance(file_manifest, Sequence) or isinstance(
            file_manifest,
            (str, bytes, bytearray),
        ):
            raise ValueError("accepted material file manifest is invalid")
        if egress_allowance is not None and not isinstance(egress_allowance, EgressAllowanceV1):
            raise TypeError("egress_allowance must be EgressAllowanceV1 or None")
        manifest = tuple(sorted(file_manifest, key=lambda entry: entry.path))
        payload = {
            "version": 2,
            "run_id": run_id,
            "attempt_id": attempt_id,
            "tenant": tenant.to_json(),
            "user_ref": user_ref,
            "thread_ref": thread_ref,
            "agent_revision_digest": agent_revision_digest,
            "skill_snapshot_digest": skill_snapshot_digest,
            "skill_scope_digest": skill_scope_digest,
            "file_manifest": [entry.to_persisted() for entry in manifest],
            "runtime_image_digest": runtime_image_digest,
            "lease_expires_at": _canonical_timestamp(lease_expires_at),
            "accepted_invocation_ref": accepted_invocation_ref,
            "accepted_invocation_digest": accepted_invocation_digest,
            "tool_plane_base_revision_digest": tool_plane_base_revision_digest,
            "tool_plane_user_overlay_digest": tool_plane_user_overlay_digest,
            "tool_plane_projection_digest": tool_plane_projection_digest,
            "tool_plane_effective_digest": tool_plane_effective_digest,
            "batch_child_attempt_ref": batch_child_attempt_ref,
            "capability_profile_digest": capability_profile_digest,
            **({"egress_allowance": egress_allowance.to_json()} if egress_allowance is not None else {}),
        }
        return cls(
            version=2,
            run_id=run_id,
            attempt_id=attempt_id,
            tenant=tenant,
            user_ref=user_ref,
            thread_ref=thread_ref,
            agent_revision_digest=agent_revision_digest,
            skill_snapshot_digest=skill_snapshot_digest,
            skill_scope_digest=skill_scope_digest,
            file_manifest=manifest,
            runtime_image_digest=runtime_image_digest,
            lease_expires_at=lease_expires_at,
            accepted_invocation_ref=accepted_invocation_ref,
            accepted_invocation_digest=accepted_invocation_digest,
            tool_plane_base_revision_digest=tool_plane_base_revision_digest,
            tool_plane_user_overlay_digest=tool_plane_user_overlay_digest,
            tool_plane_projection_digest=tool_plane_projection_digest,
            tool_plane_effective_digest=tool_plane_effective_digest,
            batch_child_attempt_ref=batch_child_attempt_ref,
            capability_profile_digest=capability_profile_digest,
            digest=_canonical_digest(payload),
            egress_allowance=egress_allowance,
        )

    @classmethod
    def from_persisted(cls, value: object) -> Self:
        fields = {
            "version",
            "run_id",
            "attempt_id",
            "tenant",
            "user_ref",
            "thread_ref",
            "agent_revision_digest",
            "skill_snapshot_digest",
            "skill_scope_digest",
            "file_manifest",
            "runtime_image_digest",
            "lease_expires_at",
            "accepted_invocation_ref",
            "accepted_invocation_digest",
            "tool_plane_base_revision_digest",
            "tool_plane_user_overlay_digest",
            "tool_plane_projection_digest",
            "tool_plane_effective_digest",
            "batch_child_attempt_ref",
            "capability_profile_digest",
            "digest",
        }
        if not isinstance(value, Mapping) or set(value) - {"egress_allowance"} != fields:
            raise ValueError("accepted material request has unknown or missing fields")
        egress_allowance = None
        if value.get("egress_allowance") is not None:
            try:
                egress_allowance = EgressAllowanceV1.from_json(value["egress_allowance"])
            except EgressPolicyError as exc:
                raise ValueError("accepted material request egress allowance is invalid") from exc
        raw_manifest = value["file_manifest"]
        if not isinstance(raw_manifest, Sequence) or isinstance(
            raw_manifest,
            (str, bytes, bytearray),
        ):
            raise ValueError("accepted material file manifest is invalid")
        return cls(
            version=value["version"],  # type: ignore[arg-type]
            run_id=value["run_id"],  # type: ignore[arg-type]
            attempt_id=value["attempt_id"],  # type: ignore[arg-type]
            tenant=TenantReferenceV1.from_json(value["tenant"]),
            user_ref=value["user_ref"],  # type: ignore[arg-type]
            thread_ref=value["thread_ref"],  # type: ignore[arg-type]
            agent_revision_digest=value["agent_revision_digest"],  # type: ignore[arg-type]
            skill_snapshot_digest=value["skill_snapshot_digest"],  # type: ignore[arg-type]
            skill_scope_digest=value["skill_scope_digest"],  # type: ignore[arg-type]
            file_manifest=tuple(AcceptedFileV1.from_persisted(entry) for entry in raw_manifest),
            runtime_image_digest=value["runtime_image_digest"],  # type: ignore[arg-type]
            lease_expires_at=_parse_timestamp(value["lease_expires_at"]),
            accepted_invocation_ref=value["accepted_invocation_ref"],  # type: ignore[arg-type]
            accepted_invocation_digest=value["accepted_invocation_digest"],  # type: ignore[arg-type]
            tool_plane_base_revision_digest=value["tool_plane_base_revision_digest"],  # type: ignore[arg-type]
            tool_plane_user_overlay_digest=value["tool_plane_user_overlay_digest"],  # type: ignore[arg-type]
            tool_plane_projection_digest=value["tool_plane_projection_digest"],  # type: ignore[arg-type]
            tool_plane_effective_digest=value["tool_plane_effective_digest"],  # type: ignore[arg-type]
            batch_child_attempt_ref=value["batch_child_attempt_ref"],  # type: ignore[arg-type]
            capability_profile_digest=value["capability_profile_digest"],  # type: ignore[arg-type]
            digest=value["digest"],  # type: ignore[arg-type]
            egress_allowance=egress_allowance,
        )


AcceptedMaterialRequest = AcceptedMaterialRequestV1 | AcceptedMaterialRequestV2


def rendered_egress_allowance(request: AcceptedMaterialRequest | None) -> EgressAllowanceV1:
    """The egress the accepted Kind's Material renders for ``request``.

    The accepted Kind always declares its egress: a V1 request, or a V2
    request sealed before allowances existed, renders as deny-all rather than
    inheriting a cluster or container default.
    """

    if isinstance(request, AcceptedMaterialRequestV2) and request.egress_allowance is not None:
        return request.egress_allowance
    return EgressAllowanceV1.deny_all()


def decode_accepted_material_request(value: object) -> AcceptedMaterialRequest:
    """Strictly dispatch a persisted request without implicit upgrading."""

    if not isinstance(value, Mapping):
        raise ValueError("accepted material request must be an object")
    version = value.get("version")
    if type(version) is not int:
        raise ValueError("accepted material request version is invalid")
    if version == 1:
        return AcceptedMaterialRequestV1.from_persisted(value)
    if version == 2:
        return AcceptedMaterialRequestV2.from_persisted(value)
    raise ValueError("accepted material request version is unsupported")


@dataclass(frozen=True, slots=True)
class AcceptedMaterialLeaseV1:
    """Epoch-fenced provider lease with a process-local renewal handle."""

    version: Literal[1]
    provider_kind: str
    provider_instance_ref: str
    ownership_epoch: int
    lease_expires_at: datetime
    opaque_renewal_handle: object = field(repr=False, compare=False)

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != 1:
            raise ValueError("accepted material lease version must be 1")
        _require_reference(self.provider_kind, "provider_kind")
        _require_reference(self.provider_instance_ref, "provider_instance_ref")
        if type(self.ownership_epoch) is not int or self.ownership_epoch < 0:
            raise ValueError("ownership_epoch must be a non-negative integer")
        _canonical_timestamp(self.lease_expires_at)

    def to_persisted(self) -> dict[str, object]:
        return {
            "version": self.version,
            "provider_kind": self.provider_kind,
            "provider_instance_ref": self.provider_instance_ref,
            "ownership_epoch": self.ownership_epoch,
            "lease_expires_at": _canonical_timestamp(self.lease_expires_at),
        }


@dataclass(frozen=True, slots=True)
class AcceptedExecutionEvidenceV1:
    """Canonical execution evidence joined to a durable run start."""

    version: Literal[1]
    run_id: str
    attempt_id: str
    tenant: TenantReferenceV1
    provider_kind: str
    provider_instance_ref: str
    ownership_epoch: int
    runtime_image_digest: str
    skill_snapshot_digest: str
    skill_scope_digest: str
    materialization_digest: str
    verifier_image_digest: str
    verifier_contract_version: str
    read_only_proof_digest: str
    qualification_scope: str
    digest: str

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != 1:
            raise ValueError("accepted execution evidence version must be 1")
        for field_name in (
            "run_id",
            "attempt_id",
            "provider_kind",
            "provider_instance_ref",
            "verifier_contract_version",
            "qualification_scope",
        ):
            _require_reference(getattr(self, field_name), field_name)
        if not isinstance(self.tenant, TenantReferenceV1):
            raise TypeError("tenant must be TenantReferenceV1")
        if type(self.tenant.version) is not int:
            raise ValueError("tenant version must be 1")
        if type(self.ownership_epoch) is not int or self.ownership_epoch < 0:
            raise ValueError("ownership_epoch must be a non-negative integer")
        for field_name in (
            "runtime_image_digest",
            "skill_snapshot_digest",
            "skill_scope_digest",
            "materialization_digest",
            "verifier_image_digest",
            "read_only_proof_digest",
        ):
            _require_digest(getattr(self, field_name), field_name)
        _require_digest(self.digest, "accepted execution evidence digest")
        if self.digest != _canonical_digest(self._digest_payload()):
            raise ValueError("accepted execution evidence digest is invalid")

    def _digest_payload(self) -> dict[str, object]:
        return {
            "version": self.version,
            "run_id": self.run_id,
            "attempt_id": self.attempt_id,
            "tenant": self.tenant.to_json(),
            "provider_kind": self.provider_kind,
            "provider_instance_ref": self.provider_instance_ref,
            "ownership_epoch": self.ownership_epoch,
            "runtime_image_digest": self.runtime_image_digest,
            "skill_snapshot_digest": self.skill_snapshot_digest,
            "skill_scope_digest": self.skill_scope_digest,
            "materialization_digest": self.materialization_digest,
            "verifier_image_digest": self.verifier_image_digest,
            "verifier_contract_version": self.verifier_contract_version,
            "read_only_proof_digest": self.read_only_proof_digest,
            "qualification_scope": self.qualification_scope,
        }

    def to_persisted(self) -> dict[str, object]:
        return {**self._digest_payload(), "digest": self.digest}

    def binds(
        self,
        request: AcceptedMaterialRequestV1,
        lease: AcceptedMaterialLeaseV1,
    ) -> bool:
        """Return whether all cross-boundary identity anchors agree."""

        return (
            self.run_id == request.run_id
            and self.attempt_id == request.attempt_id
            and self.tenant == request.tenant
            and self.provider_kind == lease.provider_kind
            and self.provider_instance_ref == lease.provider_instance_ref
            and self.ownership_epoch == lease.ownership_epoch
            and self.runtime_image_digest == request.runtime_image_digest
            and self.skill_snapshot_digest == request.skill_snapshot_digest
            and self.skill_scope_digest == request.skill_scope_digest
        )

    @classmethod
    def build(
        cls,
        *,
        run_id: str,
        attempt_id: str,
        tenant: TenantReferenceV1,
        provider_kind: str,
        provider_instance_ref: str,
        ownership_epoch: int,
        runtime_image_digest: str,
        skill_snapshot_digest: str,
        skill_scope_digest: str,
        materialization_digest: str,
        verifier_image_digest: str,
        verifier_contract_version: str,
        read_only_proof_digest: str,
        qualification_scope: str,
    ) -> Self:
        payload = {
            "version": 1,
            "run_id": run_id,
            "attempt_id": attempt_id,
            "tenant": tenant.to_json(),
            "provider_kind": provider_kind,
            "provider_instance_ref": provider_instance_ref,
            "ownership_epoch": ownership_epoch,
            "runtime_image_digest": runtime_image_digest,
            "skill_snapshot_digest": skill_snapshot_digest,
            "skill_scope_digest": skill_scope_digest,
            "materialization_digest": materialization_digest,
            "verifier_image_digest": verifier_image_digest,
            "verifier_contract_version": verifier_contract_version,
            "read_only_proof_digest": read_only_proof_digest,
            "qualification_scope": qualification_scope,
        }
        return cls(
            version=1,
            run_id=run_id,
            attempt_id=attempt_id,
            tenant=tenant,
            provider_kind=provider_kind,
            provider_instance_ref=provider_instance_ref,
            ownership_epoch=ownership_epoch,
            runtime_image_digest=runtime_image_digest,
            skill_snapshot_digest=skill_snapshot_digest,
            skill_scope_digest=skill_scope_digest,
            materialization_digest=materialization_digest,
            verifier_image_digest=verifier_image_digest,
            verifier_contract_version=verifier_contract_version,
            read_only_proof_digest=read_only_proof_digest,
            qualification_scope=qualification_scope,
            digest=_canonical_digest(payload),
        )

    @classmethod
    def from_persisted(cls, value: object) -> Self:
        fields = {
            "version",
            "run_id",
            "attempt_id",
            "tenant",
            "provider_kind",
            "provider_instance_ref",
            "ownership_epoch",
            "runtime_image_digest",
            "skill_snapshot_digest",
            "skill_scope_digest",
            "materialization_digest",
            "verifier_image_digest",
            "verifier_contract_version",
            "read_only_proof_digest",
            "qualification_scope",
            "digest",
        }
        if not isinstance(value, Mapping) or set(value) != fields:
            raise ValueError("accepted execution evidence has unknown or missing fields")
        return cls(
            version=value["version"],  # type: ignore[arg-type]
            run_id=value["run_id"],  # type: ignore[arg-type]
            attempt_id=value["attempt_id"],  # type: ignore[arg-type]
            tenant=TenantReferenceV1.from_json(value["tenant"]),
            provider_kind=value["provider_kind"],  # type: ignore[arg-type]
            provider_instance_ref=value["provider_instance_ref"],  # type: ignore[arg-type]
            ownership_epoch=value["ownership_epoch"],  # type: ignore[arg-type]
            runtime_image_digest=value["runtime_image_digest"],  # type: ignore[arg-type]
            skill_snapshot_digest=value["skill_snapshot_digest"],  # type: ignore[arg-type]
            skill_scope_digest=value["skill_scope_digest"],  # type: ignore[arg-type]
            materialization_digest=value["materialization_digest"],  # type: ignore[arg-type]
            verifier_image_digest=value["verifier_image_digest"],  # type: ignore[arg-type]
            verifier_contract_version=value["verifier_contract_version"],  # type: ignore[arg-type]
            read_only_proof_digest=value["read_only_proof_digest"],  # type: ignore[arg-type]
            qualification_scope=value["qualification_scope"],  # type: ignore[arg-type]
            digest=value["digest"],  # type: ignore[arg-type]
        )


def accepted_sandbox_resource_commitment(
    *,
    tenant_digest: str,
    provider_kind: str,
    provider_instance_ref: str,
) -> str:
    """Commit to a provider resource without exposing its operational handle."""

    _require_digest(tenant_digest, "tenant_digest")
    _require_reference(provider_kind, "provider_kind")
    _require_reference(provider_instance_ref, "provider_instance_ref")
    return hashlib.sha256(
        b"hartmesh.accepted-sandbox-resource.v1\0" + tenant_digest.encode("ascii") + b"\0" + provider_kind.encode("utf-8") + b"\0" + provider_instance_ref.encode("utf-8"),
    ).hexdigest()


def accepted_scope_reference(
    tenant: TenantReferenceV1,
    *,
    kind: Literal["user", "thread", "attempt", "invocation", "batch-child"],
    value: str,
) -> str:
    """Derive one bounded provider-safe reference under an accepted tenant."""

    if not isinstance(tenant, TenantReferenceV1):
        raise TypeError("tenant must be TenantReferenceV1")
    if kind not in {"user", "thread", "attempt", "invocation", "batch-child"}:
        raise ValueError("accepted scope reference kind is invalid")
    _require_reference(value, "accepted scope reference value")
    digest = hashlib.sha256(
        b"hartmesh.accepted-material.v1\0" + tenant.digest.encode("ascii") + b"\0" + kind.encode("ascii") + b"\0" + value.encode("utf-8"),
    ).hexdigest()
    return f"{kind}-{digest[:32]}"


@dataclass(frozen=True, slots=True)
class AcceptedExecutionEvidenceV2:
    """Portable accepted execution proof with no raw provider resource handle."""

    version: Literal[2]
    run_id: str
    attempt_id: str
    tenant: TenantReferenceV1
    provider_kind: str
    provider_resource_commitment: str
    ownership_epoch: int
    runtime_image_digest: str
    skill_snapshot_digest: str
    skill_scope_digest: str
    materialization_digest: str
    verifier_image_digest: str
    verifier_contract_version: str
    read_only_proof_digest: str
    qualification_scope: str
    accepted_invocation_ref: str
    accepted_invocation_digest: str
    tool_plane_base_revision_digest: str
    tool_plane_user_overlay_digest: str
    tool_plane_projection_digest: str
    tool_plane_effective_digest: str
    batch_child_attempt_ref: str | None
    capability_profile_digest: str
    qualification_evidence_digest: str
    isolation: AcceptedSandboxIsolationFactsV1
    digest: str

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != 2:
            raise ValueError("accepted execution evidence version must be 2")
        for field_name in (
            "run_id",
            "attempt_id",
            "provider_kind",
            "verifier_contract_version",
            "qualification_scope",
            "accepted_invocation_ref",
        ):
            _require_reference(getattr(self, field_name), field_name)
        if self.batch_child_attempt_ref is not None:
            _require_reference(
                self.batch_child_attempt_ref,
                "batch_child_attempt_ref",
            )
        if not isinstance(self.tenant, TenantReferenceV1):
            raise TypeError("tenant must be TenantReferenceV1")
        if type(self.tenant.version) is not int or self.tenant.version != 1:
            raise ValueError("tenant version must be 1")
        if type(self.ownership_epoch) is not int or self.ownership_epoch < 0:
            raise ValueError("ownership_epoch must be a non-negative integer")
        for field_name in (
            "provider_resource_commitment",
            "runtime_image_digest",
            "skill_snapshot_digest",
            "skill_scope_digest",
            "materialization_digest",
            "verifier_image_digest",
            "read_only_proof_digest",
            "accepted_invocation_digest",
            "tool_plane_base_revision_digest",
            "tool_plane_user_overlay_digest",
            "tool_plane_projection_digest",
            "tool_plane_effective_digest",
            "capability_profile_digest",
            "qualification_evidence_digest",
        ):
            _require_digest(getattr(self, field_name), field_name)
        if not isinstance(self.isolation, AcceptedSandboxIsolationFactsV1):
            raise TypeError("isolation must be AcceptedSandboxIsolationFactsV1")
        _require_digest(self.digest, "accepted execution evidence digest")
        if self.digest != _canonical_digest(self._digest_payload()):
            raise ValueError("accepted execution evidence digest is invalid")

    def _digest_payload(self) -> dict[str, object]:
        return {
            "version": self.version,
            "run_id": self.run_id,
            "attempt_id": self.attempt_id,
            "tenant": self.tenant.to_json(),
            "provider_kind": self.provider_kind,
            "provider_resource_commitment": self.provider_resource_commitment,
            "ownership_epoch": self.ownership_epoch,
            "runtime_image_digest": self.runtime_image_digest,
            "skill_snapshot_digest": self.skill_snapshot_digest,
            "skill_scope_digest": self.skill_scope_digest,
            "materialization_digest": self.materialization_digest,
            "verifier_image_digest": self.verifier_image_digest,
            "verifier_contract_version": self.verifier_contract_version,
            "read_only_proof_digest": self.read_only_proof_digest,
            "qualification_scope": self.qualification_scope,
            "accepted_invocation_ref": self.accepted_invocation_ref,
            "accepted_invocation_digest": self.accepted_invocation_digest,
            "tool_plane_base_revision_digest": (self.tool_plane_base_revision_digest),
            "tool_plane_user_overlay_digest": (self.tool_plane_user_overlay_digest),
            "tool_plane_projection_digest": self.tool_plane_projection_digest,
            "tool_plane_effective_digest": self.tool_plane_effective_digest,
            "batch_child_attempt_ref": self.batch_child_attempt_ref,
            "capability_profile_digest": self.capability_profile_digest,
            "qualification_evidence_digest": self.qualification_evidence_digest,
            "isolation": self.isolation.to_persisted(),
        }

    def to_persisted(self) -> dict[str, object]:
        return {**self._digest_payload(), "digest": self.digest}

    def binds(
        self,
        request: AcceptedMaterialRequestV2,
        lease: AcceptedMaterialLeaseV1,
    ) -> bool:
        return (
            self.run_id == request.run_id
            and self.attempt_id == request.attempt_id
            and self.tenant == request.tenant
            and self.provider_kind == lease.provider_kind
            and self.provider_resource_commitment
            == accepted_sandbox_resource_commitment(
                tenant_digest=request.tenant.digest,
                provider_kind=lease.provider_kind,
                provider_instance_ref=lease.provider_instance_ref,
            )
            and self.ownership_epoch == lease.ownership_epoch
            and self.runtime_image_digest == request.runtime_image_digest
            and self.skill_snapshot_digest == request.skill_snapshot_digest
            and self.skill_scope_digest == request.skill_scope_digest
            and self.accepted_invocation_ref == request.accepted_invocation_ref
            and self.accepted_invocation_digest == request.accepted_invocation_digest
            and self.tool_plane_base_revision_digest == request.tool_plane_base_revision_digest
            and self.tool_plane_user_overlay_digest == request.tool_plane_user_overlay_digest
            and self.tool_plane_projection_digest == request.tool_plane_projection_digest
            and self.tool_plane_effective_digest == request.tool_plane_effective_digest
            and self.batch_child_attempt_ref == request.batch_child_attempt_ref
            and self.capability_profile_digest == request.capability_profile_digest
        )

    @classmethod
    def build(
        cls,
        *,
        request: AcceptedMaterialRequestV2,
        lease: AcceptedMaterialLeaseV1,
        materialization_digest: str,
        verifier_image_digest: str,
        verifier_contract_version: str,
        read_only_proof_digest: str,
        qualification: AcceptedSandboxQualificationV1,
        isolation: AcceptedSandboxIsolationFactsV1,
    ) -> Self:
        if not isinstance(request, AcceptedMaterialRequestV2):
            raise TypeError("request must be AcceptedMaterialRequestV2")
        if not isinstance(lease, AcceptedMaterialLeaseV1):
            raise TypeError("lease must be AcceptedMaterialLeaseV1")
        if not isinstance(qualification, AcceptedSandboxQualificationV1):
            raise TypeError("qualification must be AcceptedSandboxQualificationV1")
        if qualification.capability_profile_digest != request.capability_profile_digest:
            raise ValueError("sandbox qualification capability profile mismatch")
        resource_commitment = accepted_sandbox_resource_commitment(
            tenant_digest=request.tenant.digest,
            provider_kind=lease.provider_kind,
            provider_instance_ref=lease.provider_instance_ref,
        )
        payload = {
            "version": 2,
            "run_id": request.run_id,
            "attempt_id": request.attempt_id,
            "tenant": request.tenant.to_json(),
            "provider_kind": lease.provider_kind,
            "provider_resource_commitment": resource_commitment,
            "ownership_epoch": lease.ownership_epoch,
            "runtime_image_digest": request.runtime_image_digest,
            "skill_snapshot_digest": request.skill_snapshot_digest,
            "skill_scope_digest": request.skill_scope_digest,
            "materialization_digest": materialization_digest,
            "verifier_image_digest": verifier_image_digest,
            "verifier_contract_version": verifier_contract_version,
            "read_only_proof_digest": read_only_proof_digest,
            "qualification_scope": qualification.qualification_scope,
            "accepted_invocation_ref": request.accepted_invocation_ref,
            "accepted_invocation_digest": request.accepted_invocation_digest,
            "tool_plane_base_revision_digest": (request.tool_plane_base_revision_digest),
            "tool_plane_user_overlay_digest": (request.tool_plane_user_overlay_digest),
            "tool_plane_projection_digest": request.tool_plane_projection_digest,
            "tool_plane_effective_digest": request.tool_plane_effective_digest,
            "batch_child_attempt_ref": request.batch_child_attempt_ref,
            "capability_profile_digest": request.capability_profile_digest,
            "qualification_evidence_digest": qualification.digest,
            "isolation": isolation.to_persisted(),
        }
        return cls(
            version=2,
            run_id=request.run_id,
            attempt_id=request.attempt_id,
            tenant=request.tenant,
            provider_kind=lease.provider_kind,
            provider_resource_commitment=resource_commitment,
            ownership_epoch=lease.ownership_epoch,
            runtime_image_digest=request.runtime_image_digest,
            skill_snapshot_digest=request.skill_snapshot_digest,
            skill_scope_digest=request.skill_scope_digest,
            materialization_digest=materialization_digest,
            verifier_image_digest=verifier_image_digest,
            verifier_contract_version=verifier_contract_version,
            read_only_proof_digest=read_only_proof_digest,
            qualification_scope=qualification.qualification_scope,
            accepted_invocation_ref=request.accepted_invocation_ref,
            accepted_invocation_digest=request.accepted_invocation_digest,
            tool_plane_base_revision_digest=(request.tool_plane_base_revision_digest),
            tool_plane_user_overlay_digest=(request.tool_plane_user_overlay_digest),
            tool_plane_projection_digest=request.tool_plane_projection_digest,
            tool_plane_effective_digest=request.tool_plane_effective_digest,
            batch_child_attempt_ref=request.batch_child_attempt_ref,
            capability_profile_digest=request.capability_profile_digest,
            qualification_evidence_digest=qualification.digest,
            isolation=isolation,
            digest=_canonical_digest(payload),
        )

    @classmethod
    def from_persisted(cls, value: object) -> Self:
        fields = {
            "version",
            "run_id",
            "attempt_id",
            "tenant",
            "provider_kind",
            "provider_resource_commitment",
            "ownership_epoch",
            "runtime_image_digest",
            "skill_snapshot_digest",
            "skill_scope_digest",
            "materialization_digest",
            "verifier_image_digest",
            "verifier_contract_version",
            "read_only_proof_digest",
            "qualification_scope",
            "accepted_invocation_ref",
            "accepted_invocation_digest",
            "tool_plane_base_revision_digest",
            "tool_plane_user_overlay_digest",
            "tool_plane_projection_digest",
            "tool_plane_effective_digest",
            "batch_child_attempt_ref",
            "capability_profile_digest",
            "qualification_evidence_digest",
            "isolation",
            "digest",
        }
        if not isinstance(value, Mapping) or set(value) != fields:
            raise ValueError("accepted execution evidence has unknown or missing fields")
        return cls(
            version=value["version"],  # type: ignore[arg-type]
            run_id=value["run_id"],  # type: ignore[arg-type]
            attempt_id=value["attempt_id"],  # type: ignore[arg-type]
            tenant=TenantReferenceV1.from_json(value["tenant"]),
            provider_kind=value["provider_kind"],  # type: ignore[arg-type]
            provider_resource_commitment=value["provider_resource_commitment"],  # type: ignore[arg-type]
            ownership_epoch=value["ownership_epoch"],  # type: ignore[arg-type]
            runtime_image_digest=value["runtime_image_digest"],  # type: ignore[arg-type]
            skill_snapshot_digest=value["skill_snapshot_digest"],  # type: ignore[arg-type]
            skill_scope_digest=value["skill_scope_digest"],  # type: ignore[arg-type]
            materialization_digest=value["materialization_digest"],  # type: ignore[arg-type]
            verifier_image_digest=value["verifier_image_digest"],  # type: ignore[arg-type]
            verifier_contract_version=value["verifier_contract_version"],  # type: ignore[arg-type]
            read_only_proof_digest=value["read_only_proof_digest"],  # type: ignore[arg-type]
            qualification_scope=value["qualification_scope"],  # type: ignore[arg-type]
            accepted_invocation_ref=value["accepted_invocation_ref"],  # type: ignore[arg-type]
            accepted_invocation_digest=value["accepted_invocation_digest"],  # type: ignore[arg-type]
            tool_plane_base_revision_digest=value["tool_plane_base_revision_digest"],  # type: ignore[arg-type]
            tool_plane_user_overlay_digest=value["tool_plane_user_overlay_digest"],  # type: ignore[arg-type]
            tool_plane_projection_digest=value["tool_plane_projection_digest"],  # type: ignore[arg-type]
            tool_plane_effective_digest=value["tool_plane_effective_digest"],  # type: ignore[arg-type]
            batch_child_attempt_ref=value["batch_child_attempt_ref"],  # type: ignore[arg-type]
            capability_profile_digest=value["capability_profile_digest"],  # type: ignore[arg-type]
            qualification_evidence_digest=value["qualification_evidence_digest"],  # type: ignore[arg-type]
            isolation=AcceptedSandboxIsolationFactsV1.from_persisted(
                value["isolation"],
            ),
            digest=value["digest"],  # type: ignore[arg-type]
        )


AcceptedExecutionEvidence = AcceptedExecutionEvidenceV1 | AcceptedExecutionEvidenceV2


def accepted_execution_evidence_reference(
    evidence: AcceptedExecutionEvidence,
) -> str:
    """Return a handle-free reference for linking later safe observations."""

    if not isinstance(
        evidence,
        (AcceptedExecutionEvidenceV1, AcceptedExecutionEvidenceV2),
    ):
        raise TypeError("evidence must be accepted execution evidence")
    return f"accepted-execution-{evidence.digest}"


def decode_accepted_execution_evidence(value: object) -> AcceptedExecutionEvidence:
    """Strictly dispatch persisted neutral evidence without implicit upgrading."""

    if not isinstance(value, Mapping):
        raise ValueError("accepted execution evidence must be an object")
    version = value.get("version")
    if type(version) is not int:
        raise ValueError("accepted execution evidence version is invalid")
    if version == 1:
        return AcceptedExecutionEvidenceV1.from_persisted(value)
    if version == 2:
        return AcceptedExecutionEvidenceV2.from_persisted(value)
    raise ValueError("accepted execution evidence version is unsupported")


class AcceptedSandboxLifecycleKind(StrEnum):
    """Non-authoritative states recorded for sandbox diagnosis."""

    ACQUIRED = "acquired"
    AUTHORITY_LOST = "authority_lost"
    RELEASED = "released"
    CLEANUP_PENDING = "cleanup_pending"
    ORPHANED = "orphaned"


@dataclass(frozen=True, slots=True)
class AcceptedSandboxLifecycleObservationV1:
    """Bounded, handle-free observation linked to accepted evidence.

    Observations explain lifecycle outcomes; they never authorize execution or
    cleanup. Provider resource references and renewal handles are deliberately
    absent.
    """

    version: Literal[1]
    kind: AcceptedSandboxLifecycleKind
    run_id: str
    attempt_ref: str
    batch_child_attempt_ref: str | None
    tool_receipt_ref: str | None
    provider_kind: str
    qualification_scope: str
    observed_at: datetime
    reason_code: str | None
    execution_evidence_digest: str
    digest: str

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != 1:
            raise ValueError("accepted sandbox lifecycle version must be 1")
        try:
            kind = AcceptedSandboxLifecycleKind(self.kind)
        except (TypeError, ValueError) as exc:
            raise ValueError("accepted sandbox lifecycle kind is invalid") from exc
        object.__setattr__(self, "kind", kind)
        for field_name in (
            "run_id",
            "attempt_ref",
            "provider_kind",
            "qualification_scope",
        ):
            _require_reference(getattr(self, field_name), field_name)
        for field_name in ("batch_child_attempt_ref", "tool_receipt_ref"):
            value = getattr(self, field_name)
            if value is not None:
                _require_reference(value, field_name)
        if self.reason_code is not None:
            _require_reference(self.reason_code, "reason_code")
        if (
            kind
            in {
                AcceptedSandboxLifecycleKind.AUTHORITY_LOST,
                AcceptedSandboxLifecycleKind.CLEANUP_PENDING,
                AcceptedSandboxLifecycleKind.ORPHANED,
            }
            and self.reason_code is None
        ):
            raise ValueError("accepted sandbox lifecycle reason code is required")
        _canonical_timestamp(self.observed_at)
        _require_digest(
            self.execution_evidence_digest,
            "execution_evidence_digest",
        )
        _require_digest(self.digest, "accepted sandbox lifecycle digest")
        payload = self._digest_payload()
        if self.digest != _canonical_digest(payload):
            raise ValueError("accepted sandbox lifecycle digest is invalid")
        if (
            len(
                json.dumps(
                    payload,
                    ensure_ascii=False,
                    separators=(",", ":"),
                    sort_keys=True,
                ).encode("utf-8"),
            )
            > 4096
        ):
            raise ValueError("accepted sandbox lifecycle observation is too large")

    def _digest_payload(self) -> dict[str, object]:
        return {
            "version": self.version,
            "kind": self.kind.value,
            "run_id": self.run_id,
            "attempt_ref": self.attempt_ref,
            "batch_child_attempt_ref": self.batch_child_attempt_ref,
            "tool_receipt_ref": self.tool_receipt_ref,
            "provider_kind": self.provider_kind,
            "qualification_scope": self.qualification_scope,
            "observed_at": _canonical_timestamp(self.observed_at),
            "reason_code": self.reason_code,
            "execution_evidence_digest": self.execution_evidence_digest,
        }

    def to_persisted(self) -> dict[str, object]:
        return {**self._digest_payload(), "digest": self.digest}

    @classmethod
    def build(
        cls,
        *,
        evidence: AcceptedExecutionEvidence,
        kind: AcceptedSandboxLifecycleKind,
        observed_at: datetime,
        reason_code: str | None = None,
        tool_receipt_ref: str | None = None,
    ) -> Self:
        if not isinstance(
            evidence,
            (AcceptedExecutionEvidenceV1, AcceptedExecutionEvidenceV2),
        ):
            raise TypeError("evidence must be accepted execution evidence")
        lifecycle_kind = AcceptedSandboxLifecycleKind(kind)
        payload = {
            "version": 1,
            "kind": lifecycle_kind.value,
            "run_id": evidence.run_id,
            "attempt_ref": evidence.attempt_id,
            "batch_child_attempt_ref": (evidence.batch_child_attempt_ref if isinstance(evidence, AcceptedExecutionEvidenceV2) else None),
            "tool_receipt_ref": tool_receipt_ref,
            "provider_kind": evidence.provider_kind,
            "qualification_scope": evidence.qualification_scope,
            "observed_at": _canonical_timestamp(observed_at),
            "reason_code": reason_code,
            "execution_evidence_digest": evidence.digest,
        }
        return cls(
            version=1,
            kind=lifecycle_kind,
            run_id=evidence.run_id,
            attempt_ref=evidence.attempt_id,
            batch_child_attempt_ref=payload["batch_child_attempt_ref"],  # type: ignore[arg-type]
            tool_receipt_ref=tool_receipt_ref,
            provider_kind=evidence.provider_kind,
            qualification_scope=evidence.qualification_scope,
            observed_at=observed_at,
            reason_code=reason_code,
            execution_evidence_digest=evidence.digest,
            digest=_canonical_digest(payload),
        )

    @classmethod
    def from_persisted(cls, value: object) -> Self:
        fields = {
            "version",
            "kind",
            "run_id",
            "attempt_ref",
            "batch_child_attempt_ref",
            "tool_receipt_ref",
            "provider_kind",
            "qualification_scope",
            "observed_at",
            "reason_code",
            "execution_evidence_digest",
            "digest",
        }
        if not isinstance(value, Mapping) or set(value) != fields:
            raise ValueError(
                "accepted sandbox lifecycle observation has unknown or missing fields",
            )
        return cls(
            version=value["version"],  # type: ignore[arg-type]
            kind=value["kind"],  # type: ignore[arg-type]
            run_id=value["run_id"],  # type: ignore[arg-type]
            attempt_ref=value["attempt_ref"],  # type: ignore[arg-type]
            batch_child_attempt_ref=value["batch_child_attempt_ref"],  # type: ignore[arg-type]
            tool_receipt_ref=value["tool_receipt_ref"],  # type: ignore[arg-type]
            provider_kind=value["provider_kind"],  # type: ignore[arg-type]
            qualification_scope=value["qualification_scope"],  # type: ignore[arg-type]
            observed_at=_parse_timestamp(value["observed_at"]),
            reason_code=value["reason_code"],  # type: ignore[arg-type]
            execution_evidence_digest=value["execution_evidence_digest"],  # type: ignore[arg-type]
            digest=value["digest"],  # type: ignore[arg-type]
        )


class AcceptedMaterializer(Protocol):
    """Provider adapter for accepted immutable material and its lease."""

    def capability(self) -> AcceptedMaterialCapability: ...

    async def acquire_and_materialize(
        self,
        request: AcceptedMaterialRequest,
        *,
        execution_claim: AcceptedMaterialExecutionClaimV1 | None = None,
    ) -> tuple[Sandbox, AcceptedMaterialLeaseV1, AcceptedExecutionEvidence]: ...

    async def validate(
        self,
        lease: AcceptedMaterialLeaseV1,
        evidence: AcceptedExecutionEvidence,
    ) -> bool: ...

    async def renew(
        self,
        lease: AcceptedMaterialLeaseV1,
    ) -> AcceptedMaterialLeaseV1: ...

    async def release(self, lease: AcceptedMaterialLeaseV1) -> None: ...


# The closed set of privileged operations an accepted session admits is
# generated from the single declaration point in ``deerflow.sandbox.operations``.
# Every public ``Sandbox`` method is a member, so a verb cannot exist on the
# provider without a fenced form on the facade.
AcceptedSandboxOperationKind = SandboxOperationKind


@dataclass(frozen=True, slots=True)
class AcceptedSandboxOperationV1:
    """Process-local operation envelope; arguments are never portable evidence."""

    version: Literal[1]
    kind: AcceptedSandboxOperationKind
    args: tuple[object, ...] = field(repr=False)
    kwargs: Mapping[str, object] = field(default_factory=dict, repr=False)
    operation_ref: str = field(
        default_factory=lambda: f"accepted-operation-{uuid.uuid4().hex}",
    )

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != 1:
            raise ValueError("accepted sandbox operation version must be 1")
        try:
            kind = AcceptedSandboxOperationKind(self.kind)
        except (TypeError, ValueError) as exc:
            raise ValueError("accepted sandbox operation kind is invalid") from exc
        object.__setattr__(self, "kind", kind)
        if not isinstance(self.args, tuple):
            raise TypeError("accepted sandbox operation args must be a tuple")
        if not isinstance(self.kwargs, Mapping) or any(not isinstance(key, str) for key in self.kwargs):
            raise TypeError("accepted sandbox operation kwargs must be a string-keyed mapping")
        object.__setattr__(self, "kwargs", dict(self.kwargs))
        if not isinstance(self.operation_ref, str) or _OPERATION_REF_PATTERN.fullmatch(self.operation_ref) is None:
            raise ValueError(
                "operation_ref must be an opaque accepted-operation UUID reference",
            )

    @classmethod
    def for_operation(cls, name: str, /, *args: object, **kwargs: object) -> Self:
        """Build the envelope for one declared operation from a live call.

        The argument split follows the declaration: parameters without a
        default travel positionally, parameters with a default by keyword.
        """
        spec = sandbox_operations().get(name)
        if spec is None:
            raise ValueError(f"unknown sandbox operation {name!r}")
        positional, keyword = spec.envelope_arguments(args, kwargs)
        return cls(
            version=1,
            kind=AcceptedSandboxOperationKind(name),
            args=positional,
            kwargs=keyword,
        )

    def delegate(self, sandbox: Sandbox) -> object:
        """Invoke the closed operation set without exposing the raw sandbox."""

        if not isinstance(sandbox, Sandbox):
            raise TypeError("accepted sandbox session requires a Sandbox")
        operation = getattr(sandbox, self.kind.value)
        return operation(*self.args, **self.kwargs)


def _install_operation_constructors() -> None:
    """Expose one ``AcceptedSandboxOperationV1.<verb>(...)`` constructor per declaration."""

    for spec in sandbox_operations().values():

        def constructor(cls, *args: object, __name: str = spec.name, **kwargs: object) -> AcceptedSandboxOperationV1:
            return cls.for_operation(__name, *args, **kwargs)

        constructor.__name__ = spec.name
        constructor.__qualname__ = f"AcceptedSandboxOperationV1.{spec.name}"
        constructor.__doc__ = spec.doc
        setattr(AcceptedSandboxOperationV1, spec.name, classmethod(constructor))


_install_operation_constructors()


AcceptedSandboxResult = object
AcceptedRunFenceValidator = Callable[
    [AcceptedMaterialExecutionClaimV1],
    Awaitable[bool],
]
AcceptedSandboxBeforeDelegate = Callable[[], Awaitable[None]]
AcceptedSandboxToolReceiptResolver = Callable[[], str | None]


class _AcceptedSandboxSessionState(StrEnum):
    OPEN = "open"
    LOST = "lost"
    CLOSING = "closing"
    CLOSED = "closed"


class AcceptedSandboxFenceAdapter(Protocol):
    """Validate an existing durable execution fence without minting authority."""

    @property
    def tenant_digest(self) -> str: ...

    @property
    def run_id(self) -> str: ...

    async def validate(self) -> bool: ...


class AcceptedSandboxSession:
    """Gate every accepted sandbox operation through both existing authorities.

    The session owns no independent lease or epoch. It composes the mutable
    durable execution claim with the provider materializer's current lease and
    evidence. The final local check deliberately does not claim distributed
    atomicity: a non-atomic provider can still accept the one operation racing
    a takeover after both validations complete.
    """

    def __init__(
        self,
        *,
        sandbox: Sandbox,
        materializer: AcceptedMaterializer,
        lease: AcceptedMaterialLeaseV1,
        evidence: AcceptedExecutionEvidence,
        execution_claim: AcceptedMaterialExecutionClaimV1 | None = None,
        run_fence_validator: AcceptedRunFenceValidator | None = None,
        fence_adapter: AcceptedSandboxFenceAdapter | None = None,
        before_delegate: AcceptedSandboxBeforeDelegate | None = None,
        tool_receipt_ref_resolver: AcceptedSandboxToolReceiptResolver | None = None,
    ) -> None:
        if not isinstance(sandbox, Sandbox):
            raise TypeError("sandbox must be a Sandbox")
        if not isinstance(lease, AcceptedMaterialLeaseV1):
            raise TypeError("lease must be AcceptedMaterialLeaseV1")
        if not isinstance(
            evidence,
            (AcceptedExecutionEvidenceV1, AcceptedExecutionEvidenceV2),
        ):
            raise TypeError("evidence must be accepted execution evidence")
        if fence_adapter is None:
            if not isinstance(execution_claim, AcceptedMaterialExecutionClaimV1):
                raise TypeError(
                    "execution_claim must be AcceptedMaterialExecutionClaimV1",
                )
            if not callable(run_fence_validator):
                raise TypeError("run_fence_validator must be callable")
            authority_tenant_digest = execution_claim.tenant_digest
            authority_run_id = execution_claim.run_id
        else:
            if execution_claim is not None or run_fence_validator is not None:
                raise TypeError(
                    "fence_adapter is mutually exclusive with a run execution claim",
                )
            authority_tenant_digest = getattr(fence_adapter, "tenant_digest", None)
            authority_run_id = getattr(fence_adapter, "run_id", None)
            if not isinstance(authority_tenant_digest, str) or not isinstance(authority_run_id, str) or not callable(getattr(fence_adapter, "validate", None)):
                raise TypeError("fence_adapter must implement AcceptedSandboxFenceAdapter")
        if before_delegate is not None and not callable(before_delegate):
            raise TypeError("before_delegate must be callable")
        if tool_receipt_ref_resolver is not None and not callable(
            tool_receipt_ref_resolver,
        ):
            raise TypeError("tool_receipt_ref_resolver must be callable")
        resource_matches = (
            lease.provider_instance_ref == evidence.provider_instance_ref
            if isinstance(evidence, AcceptedExecutionEvidenceV1)
            else evidence.provider_resource_commitment
            == accepted_sandbox_resource_commitment(
                tenant_digest=evidence.tenant.digest,
                provider_kind=lease.provider_kind,
                provider_instance_ref=lease.provider_instance_ref,
            )
        )
        if (
            sandbox.id != lease.provider_instance_ref
            or authority_run_id != evidence.run_id
            or authority_tenant_digest != evidence.tenant.digest
            or lease.provider_kind != evidence.provider_kind
            or not resource_matches
            or lease.ownership_epoch != evidence.ownership_epoch
        ):
            raise ValueError("accepted sandbox session tuple is inconsistent")
        self._sandbox = sandbox
        self._materializer = materializer
        self._lease = lease
        self._evidence = evidence
        self._execution_claim = execution_claim
        self._run_fence_validator = run_fence_validator
        self._fence_adapter = fence_adapter
        self._before_delegate = before_delegate
        self._tool_receipt_ref_resolver = tool_receipt_ref_resolver
        self._state = _AcceptedSandboxSessionState.OPEN
        self._state_lock = threading.RLock()
        self._lifecycle_lock = asyncio.Lock()
        self._closed = asyncio.Event()
        self._active_operations = 0
        self._active_operations_idle = threading.Event()
        self._active_operations_idle.set()
        self._observations = [
            AcceptedSandboxLifecycleObservationV1.build(
                evidence=evidence,
                kind=AcceptedSandboxLifecycleKind.ACQUIRED,
                observed_at=datetime.now(UTC),
            ),
        ]

    @property
    def is_open(self) -> bool:
        """Whether operations may still be admitted; false once lost or closing."""

        with self._state_lock:
            return self._state is _AcceptedSandboxSessionState.OPEN

    def _snapshot_open_lease(self) -> AcceptedMaterialLeaseV1:
        with self._state_lock:
            if self._state is not _AcceptedSandboxSessionState.OPEN:
                raise AcceptedSandboxAuthorityLostError(
                    "accepted_sandbox_session_not_open",
                )
            return self._lease

    @property
    def safe_reference(self) -> str:
        """Pseudonymous process-safe reference suitable for runtime state."""

        return f"accepted-session-{self._evidence.digest[:24]}"

    @property
    def execution_evidence_reference(self) -> str:
        """Portable link to evidence without exposing the provider resource."""

        return accepted_execution_evidence_reference(self._evidence)

    @property
    def execution_evidence_digest(self) -> str:
        """The evidence digest that run-bound diagnostics link to."""

        return self._evidence.digest

    @property
    def attempt_ref(self) -> str:
        return self._evidence.attempt_id

    @property
    def batch_child_attempt_ref(self) -> str | None:
        return self._evidence.batch_child_attempt_ref if isinstance(self._evidence, AcceptedExecutionEvidenceV2) else None

    @property
    def persistent_shell_sessions(self) -> bool | None:
        return self._sandbox.persistent_shell_sessions

    @property
    def lifecycle_observations(
        self,
    ) -> tuple[AcceptedSandboxLifecycleObservationV1, ...]:
        with self._state_lock:
            return tuple(self._observations)

    def _record_lifecycle(
        self,
        kind: AcceptedSandboxLifecycleKind,
        *,
        reason_code: str | None = None,
    ) -> None:
        tool_receipt_ref = None
        if self._tool_receipt_ref_resolver is not None:
            try:
                candidate = self._tool_receipt_ref_resolver()
                if candidate is not None:
                    tool_receipt_ref = _require_reference(
                        candidate,
                        "tool_receipt_ref",
                    )
            except Exception:
                # Diagnostics never become authority and must not obscure the
                # underlying sandbox loss when no safe receipt is available.
                tool_receipt_ref = None
        observation = AcceptedSandboxLifecycleObservationV1.build(
            evidence=self._evidence,
            kind=kind,
            observed_at=datetime.now(UTC),
            reason_code=reason_code,
            tool_receipt_ref=tool_receipt_ref,
        )
        with self._state_lock:
            self._observations.append(observation)

    def _lose(self, code: str) -> AcceptedSandboxAuthorityLostError:
        transitioned = False
        with self._state_lock:
            if self._state is _AcceptedSandboxSessionState.OPEN:
                self._state = _AcceptedSandboxSessionState.LOST
                transitioned = True
        if transitioned:
            self._record_lifecycle(
                AcceptedSandboxLifecycleKind.AUTHORITY_LOST,
                reason_code=code,
            )
        return AcceptedSandboxAuthorityLostError(code)

    async def _validate_run_fence(self) -> None:
        try:
            if self._fence_adapter is not None:
                current = await self._fence_adapter.validate()
            else:
                assert self._run_fence_validator is not None
                assert self._execution_claim is not None
                current = await self._run_fence_validator(self._execution_claim)
        except asyncio.CancelledError:
            self._lose("accepted_sandbox_session_cancelled")
            raise
        except Exception:
            raise self._lose("accepted_sandbox_run_fence_unavailable") from None
        if current is not True:
            raise self._lose("accepted_sandbox_run_fence_lost")

    def _delegate_if_current(
        self,
        lease: AcceptedMaterialLeaseV1,
        operation: AcceptedSandboxOperationV1,
    ) -> AcceptedSandboxResult:
        with self._state_lock:
            if self._state is not _AcceptedSandboxSessionState.OPEN or self._lease is not lease:
                raise AcceptedSandboxAuthorityLostError(
                    "accepted_sandbox_session_not_open",
                )
            self._active_operations += 1
            self._active_operations_idle.clear()
        try:
            return operation.delegate(self._sandbox)
        finally:
            with self._state_lock:
                self._active_operations -= 1
                if self._active_operations == 0:
                    self._active_operations_idle.set()

    async def _validate_current_authorities(self) -> AcceptedMaterialLeaseV1:
        self._snapshot_open_lease()
        async with self._lifecycle_lock:
            lease = self._snapshot_open_lease()
            await self._validate_run_fence()
            self._snapshot_open_lease()
            try:
                material_current = await self._materializer.validate(
                    lease,
                    self._evidence,
                )
            except asyncio.CancelledError:
                self._lose("accepted_sandbox_session_cancelled")
                raise
            except Exception:
                raise self._lose(
                    "accepted_sandbox_material_validation_unavailable",
                ) from None
            if material_current is not True:
                raise self._lose("accepted_sandbox_material_lease_lost")
            if self._snapshot_open_lease() is not lease:
                raise self._lose("accepted_sandbox_material_lease_changed")
            return lease

    async def validate(self) -> None:
        """Sample both authorities without delegating a provider operation."""

        await self._validate_current_authorities()

    async def execute(
        self,
        operation: AcceptedSandboxOperationV1,
    ) -> AcceptedSandboxResult:
        if not isinstance(operation, AcceptedSandboxOperationV1):
            raise TypeError("operation must be AcceptedSandboxOperationV1")
        # Check before waiting for validation/renewal serialization. ``close``
        # marks the state synchronously before provider release, so callers do
        # not queue behind slow cleanup and accidentally become eligible later.
        lease = await self._validate_current_authorities()
        if self._before_delegate is not None:
            try:
                await self._before_delegate()
            except asyncio.CancelledError:
                self._lose("accepted_sandbox_session_cancelled")
                raise
            except Exception:
                raise self._lose(
                    "accepted_sandbox_pre_delegate_check_failed",
                ) from None
        try:
            return await asyncio.to_thread(
                self._delegate_if_current,
                lease,
                operation,
            )
        except asyncio.CancelledError:
            # ``to_thread`` cannot cancel provider work which has already
            # started. Revoke the session synchronously so the raced call is
            # the last call that can reach the provider.
            self._lose("accepted_sandbox_session_cancelled")
            raise

    async def renew(self) -> None:
        async with self._lifecycle_lock:
            lease = self._snapshot_open_lease()
            await self._validate_run_fence()
            try:
                renewed = await self._materializer.renew(lease)
            except asyncio.CancelledError:
                self._lose("accepted_sandbox_session_cancelled")
                raise
            except Exception:
                raise self._lose("accepted_sandbox_material_lease_lost") from None
            if not isinstance(renewed, AcceptedMaterialLeaseV1):
                raise self._lose("accepted_sandbox_material_lease_invalid")
            with self._state_lock:
                if (
                    self._state is not _AcceptedSandboxSessionState.OPEN
                    or self._lease is not lease
                    or renewed.provider_kind != lease.provider_kind
                    or renewed.provider_instance_ref != lease.provider_instance_ref
                    or renewed.ownership_epoch != lease.ownership_epoch
                ):
                    raise self._lose("accepted_sandbox_material_lease_changed")
                self._lease = renewed

    async def close(self) -> None:
        wait_for_close = False
        with self._state_lock:
            if self._state is _AcceptedSandboxSessionState.CLOSED:
                return
            if self._state is _AcceptedSandboxSessionState.CLOSING:
                wait_for_close = True
            else:
                self._state = _AcceptedSandboxSessionState.CLOSING
        if wait_for_close:
            await self._closed.wait()
            return

        release_completed = False
        try:
            # Calls whose final local check won before close are already
            # delegated side effects. Let those bounded calls finish before
            # releasing their provider lease; renewal remains independent.
            await asyncio.to_thread(self._active_operations_idle.wait)
            async with self._lifecycle_lock:
                lease = self._lease
                try:
                    await self._materializer.release(lease)
                except Exception:
                    pass
                else:
                    release_completed = True
        finally:
            with self._state_lock:
                self._state = _AcceptedSandboxSessionState.CLOSED
            self._closed.set()
            self._record_lifecycle(
                (AcceptedSandboxLifecycleKind.RELEASED if release_completed else AcceptedSandboxLifecycleKind.CLEANUP_PENDING),
                reason_code=(None if release_completed else "accepted_sandbox_release_failed"),
            )
        if not release_completed:
            raise AcceptedMaterialError("accepted_sandbox_release_failed")


@fenced_sandbox_facade
class _AcceptedSandboxFacade(Sandbox):
    """Sandbox-compatible sync view that cannot bypass session validation.

    Every public ``Sandbox`` method is generated from the declarations in
    ``deerflow.sandbox.operations`` and routed through
    ``_execute_fenced_operation``. The decorator refuses to build the class if
    any base-class method would be inherited as an unfenced passthrough, so a
    verb added upstream fails at import rather than skipping the fence.
    """

    def __init__(self, bridge: AcceptedSandboxSessionBridge) -> None:
        super().__init__(bridge.safe_reference)
        self._bridge = bridge
        self.persistent_shell_sessions = bridge.persistent_shell_sessions

    def _execute_fenced_operation(
        self,
        name: str,
        args: tuple[object, ...],
        kwargs: Mapping[str, object],
    ) -> object:
        return self._bridge.execute_sync(
            AcceptedSandboxOperationV1.for_operation(name, *args, **kwargs),
        )


class AcceptedSandboxSessionBridge:
    """Thread-safe bridge from synchronous tools to one owner-loop session.

    The bridge is the accepted kind's declaration owner: it knows the session's
    public ref, its handle (the fenced facade), whether it is still open, and
    how to retire it, and packages those as the ``SandboxSessionDeclaration``
    the session provider resolves. ``declare_accepted_sandbox_session`` is the
    only way a bridge reaches the registry.
    """

    def __init__(
        self,
        session: AcceptedSandboxSession,
        *,
        owner_loop: asyncio.AbstractEventLoop,
        mount_scope: tuple[str, str] | None = None,
        thread_id: str | None = None,
    ) -> None:
        if not isinstance(session, AcceptedSandboxSession):
            raise TypeError("session must be AcceptedSandboxSession")
        if not isinstance(owner_loop, asyncio.AbstractEventLoop):
            raise TypeError("owner_loop must be an event loop")
        from deerflow.sandbox.session import (
            SandboxSessionDeclaration,
            SandboxSessionKind,
            SandboxSessionTerminal,
        )

        if thread_id is None and mount_scope is not None:
            thread_id = mount_scope[1]
        if thread_id is not None and (not isinstance(thread_id, str) or not thread_id):
            raise ValueError("thread_id must be a non-empty string or None")
        self._session = session
        self._owner_loop = owner_loop
        self._thread_id = thread_id
        self._sandbox = _AcceptedSandboxFacade(self)
        self._declaration = SandboxSessionDeclaration(
            public_ref=self.safe_reference,
            mount_scope=mount_scope,
            kind=SandboxSessionKind.ACCEPTED,
            terminal=SandboxSessionTerminal.RETIRE,
            handle=self._sandbox,
            is_live=lambda: self.is_open,
            retire=self.close_sync,
            # The provider's own id stays inside the session provider, which
            # translates the public ref for id-keyed provider hooks.
            provider_ref=session._sandbox.id,
            observe=self._observe,
        )

    def _observe(self, kind: str, facts: Mapping[str, str | int | bool], *, once: bool = False) -> None:
        """The accepted Kind's Observer: a run-bound diagnostic under the public ref.

        Facts observed outside the run (an ordinary acquire refused because
        this session holds the thread) land on the run's diagnostic stream with
        the same anchors the run's own facts carry, and never the container id.
        A session declared without a thread records nothing.
        """
        if self._thread_id is None:
            return
        from deerflow.sandbox.diagnostics import record_session_diagnostic
        from deerflow.sandbox.session import SandboxSessionKind

        record_session_diagnostic(
            kind,
            session_kind=SandboxSessionKind.ACCEPTED,
            run_id=self._session._evidence.run_id,
            thread_id=self._thread_id,
            sandbox_ref=self.safe_reference,
            facts=facts,
            attempt_ref=self._session.attempt_ref,
            batch_child_attempt_ref=self._session.batch_child_attempt_ref,
            execution_evidence_digest=self._session.execution_evidence_digest,
            once=once,
        )

    def record_egress_allowance(self, allowance: EgressAllowanceV1) -> None:
        """Record once which run-bound egress this session's Material rendered.

        The allowance itself is authority in the accepted invocation; this is
        the session's diagnostic of it, so the run's stream shows the profile,
        rule count, and DNS decision beside the egress facts the sandbox
        observes, and never the container id or the rule values.
        """
        if not isinstance(allowance, EgressAllowanceV1):
            raise TypeError("allowance must be EgressAllowanceV1")
        self._observe(
            "egress.bound",
            {"profile": allowance.profile, "rule_count": len(allowance.rules), "dns": allowance.dns},
            once=True,
        )

    @property
    def declaration(self) -> SandboxSessionDeclaration:
        """What the session provider resolves for this session."""
        return self._declaration

    @property
    def safe_reference(self) -> str:
        return self._session.safe_reference

    @property
    def execution_evidence_reference(self) -> str:
        return self._session.execution_evidence_reference

    @property
    def execution_evidence_digest(self) -> str:
        return self._session.execution_evidence_digest

    @property
    def attempt_ref(self) -> str:
        return self._session.attempt_ref

    @property
    def batch_child_attempt_ref(self) -> str | None:
        return self._session.batch_child_attempt_ref

    @property
    def persistent_shell_sessions(self) -> bool | None:
        return self._session.persistent_shell_sessions

    @property
    def sandbox(self) -> Sandbox:
        return self._sandbox

    @property
    def lifecycle_observations(
        self,
    ) -> tuple[AcceptedSandboxLifecycleObservationV1, ...]:
        return self._session.lifecycle_observations

    async def execute(
        self,
        operation: AcceptedSandboxOperationV1,
    ) -> AcceptedSandboxResult:
        if asyncio.get_running_loop() is self._owner_loop:
            return await self._session.execute(operation)
        future = asyncio.run_coroutine_threadsafe(
            self._session.execute(operation),
            self._owner_loop,
        )
        return await asyncio.wrap_future(future)

    def execute_sync(
        self,
        operation: AcceptedSandboxOperationV1,
    ) -> AcceptedSandboxResult:
        try:
            running_loop = asyncio.get_running_loop()
        except RuntimeError:
            running_loop = None
        if running_loop is self._owner_loop:
            raise AcceptedSandboxAuthorityLostError(
                "accepted_sandbox_sync_call_on_owner_loop",
            )
        if self._owner_loop.is_closed() or not self._owner_loop.is_running():
            raise AcceptedSandboxAuthorityLostError(
                "accepted_sandbox_owner_loop_unavailable",
            )
        future = asyncio.run_coroutine_threadsafe(
            self._session.execute(operation),
            self._owner_loop,
        )
        return future.result()

    async def renew(self) -> None:
        if asyncio.get_running_loop() is self._owner_loop:
            await self._session.renew()
            return
        future = asyncio.run_coroutine_threadsafe(
            self._session.renew(),
            self._owner_loop,
        )
        await asyncio.wrap_future(future)

    async def validate(self) -> None:
        if asyncio.get_running_loop() is self._owner_loop:
            await self._session.validate()
            return
        future = asyncio.run_coroutine_threadsafe(
            self._session.validate(),
            self._owner_loop,
        )
        await asyncio.wrap_future(future)

    @property
    def is_open(self) -> bool:
        return self._session.is_open

    def close_sync(self) -> None:
        """Retire the session from a worker thread; the provider's terminal.

        Mirrors ``execute_sync``: it must not be called on the owner loop, and
        it blocks until the materializer has released the lease.
        """
        try:
            running_loop = asyncio.get_running_loop()
        except RuntimeError:
            running_loop = None
        if running_loop is self._owner_loop:
            raise AcceptedSandboxAuthorityLostError(
                "accepted_sandbox_sync_call_on_owner_loop",
            )
        if self._owner_loop.is_closed() or not self._owner_loop.is_running():
            raise AcceptedSandboxAuthorityLostError(
                "accepted_sandbox_owner_loop_unavailable",
            )
        future = asyncio.run_coroutine_threadsafe(
            self._session.close(),
            self._owner_loop,
        )
        future.result()

    async def close(self) -> None:
        if asyncio.get_running_loop() is self._owner_loop:
            await self._session.close()
            return
        future = asyncio.run_coroutine_threadsafe(
            self._session.close(),
            self._owner_loop,
        )
        await asyncio.wrap_future(future)


def declare_accepted_sandbox_session(
    session: AcceptedSandboxSession,
    *,
    mount_scope: tuple[str, str] | None,
    owner_loop: asyncio.AbstractEventLoop | None = None,
    thread_id: str | None = None,
) -> AcceptedSandboxSessionBridge:
    """Declare one host-owned accepted session to the session provider.

    Provisioning precedes declaring, never the reverse: ``session`` already
    holds its materialized sandbox and lease. Declaring registers the
    session's public ref so the provider resolves it; binding it to an
    execution is the owner's explicit act (``set_current_sandbox_session`` for
    a whole durable run, ``bind_sandbox_session`` around one subagent
    execution). ``mount_scope`` is the ``(user_id, thread_id)`` whose ordinary
    sandbox the session displaces while it is open, or ``None`` for a session
    keyed by its own attempt, such as a batch child, which no ordinary acquire
    can collide with.

    One session, one declaration: a session that is already declared and
    still open is refused rather than re-registered, because re-registering
    would silently stop the first bridge's handle from resolving.
    """

    from deerflow.sandbox.session import get_sandbox_session_registry

    if not isinstance(session, AcceptedSandboxSession):
        raise TypeError("session must be AcceptedSandboxSession")
    registry = get_sandbox_session_registry()
    if registry.lookup(session.safe_reference) is not None:
        raise AcceptedMaterialError("accepted_sandbox_session_already_declared")
    bridge = AcceptedSandboxSessionBridge(
        session,
        owner_loop=owner_loop if owner_loop is not None else asyncio.get_running_loop(),
        mount_scope=mount_scope,
        thread_id=thread_id,
    )
    registry.declare(bridge.declaration)
    return bridge


def withdraw_accepted_sandbox_session(bridge: AcceptedSandboxSessionBridge) -> None:
    """Withdraw ``bridge``'s declaration so its public ref stops resolving.

    Revokes the registration if it is still this bridge's, and clears the
    executing context's binding if it is this bridge's. Idempotent, and safe
    to call after a later bridge has taken over the same public ref.
    """

    from deerflow.sandbox.session import (
        SandboxSessionDeclaration,
        current_sandbox_session,
        get_sandbox_session_registry,
        set_current_sandbox_session,
    )

    # Validated by what it carries, not by its type: a cleanup path hands the
    # bridge it was given, and lifecycle tests stand doubles in for the bridge.
    declaration = getattr(bridge, "declaration", None)
    if not isinstance(declaration, SandboxSessionDeclaration):
        raise TypeError("bridge must carry a SandboxSessionDeclaration")
    registry = get_sandbox_session_registry()
    if registry.lookup(declaration.public_ref) is declaration:
        registry.revoke(declaration.public_ref)
    if current_sandbox_session() is declaration:
        set_current_sandbox_session(None)


def current_accepted_sandbox_bridge() -> AcceptedSandboxSessionBridge | None:
    """The bridge behind the executing context's declared accepted session.

    Evidence consumers that need more than the handle (the execution evidence
    reference, an operation link) find the bridge here. An ordinary declared
    handle is not an accepted session and yields ``None``.
    """

    from deerflow.sandbox.session import current_sandbox_session

    declaration = current_sandbox_session()
    if declaration is None:
        return None
    handle = declaration.handle
    return handle._bridge if isinstance(handle, _AcceptedSandboxFacade) else None


def is_accepted_sandbox_facade(sandbox: object) -> bool:
    """Identify the host-owned facade without exposing its backing session."""

    return isinstance(sandbox, _AcceptedSandboxFacade)


@dataclass(frozen=True, slots=True)
class AcceptedMaterializerSelection:
    """Process-local adapter selection returned by an opted-in provider."""

    materializer: AcceptedMaterializer
    runtime_image_digest: str
    lease_duration: timedelta
    capability_profile: AcceptedSandboxCapabilityProfileV1
    qualification: AcceptedSandboxQualificationV1

    def __post_init__(self) -> None:
        if self.materializer.capability() is not AcceptedMaterialCapability.IMMUTABLE_READ_ONLY:
            raise ValueError(
                "accepted materializer selection must provide immutable read-only material",
            )
        _require_digest(
            self.runtime_image_digest,
            "accepted materializer runtime image digest",
        )
        if not isinstance(
            self.capability_profile,
            AcceptedSandboxCapabilityProfileV1,
        ):
            raise TypeError("capability_profile must be AcceptedSandboxCapabilityProfileV1")
        if not isinstance(self.qualification, AcceptedSandboxQualificationV1):
            raise TypeError("qualification must be AcceptedSandboxQualificationV1")
        if self.capability_profile.material_capability is not AcceptedMaterialCapability.IMMUTABLE_READ_ONLY or self.qualification.capability_profile_digest != self.capability_profile.digest:
            raise ValueError("accepted sandbox qualification profile mismatch")
        if not isinstance(self.lease_duration, timedelta) or self.lease_duration <= timedelta(0) or self.lease_duration > timedelta(hours=1):
            raise ValueError(
                "accepted materializer lease duration must be between zero and one hour",
            )


def validate_accepted_materialization(
    *,
    selection: AcceptedMaterializerSelection,
    request: AcceptedMaterialRequest,
    lease: AcceptedMaterialLeaseV1,
    evidence: AcceptedExecutionEvidence,
) -> None:
    """Validate a provider result before it enters durable run state."""

    if not isinstance(selection, AcceptedMaterializerSelection):
        raise TypeError("selection must be AcceptedMaterializerSelection")
    if not isinstance(request, (AcceptedMaterialRequestV1, AcceptedMaterialRequestV2)):
        raise TypeError("request must be accepted material request")
    if not isinstance(lease, AcceptedMaterialLeaseV1):
        raise TypeError("lease must be AcceptedMaterialLeaseV1")
    if not isinstance(
        evidence,
        (AcceptedExecutionEvidenceV1, AcceptedExecutionEvidenceV2),
    ):
        raise TypeError("evidence must be accepted execution evidence")

    matched_version = (isinstance(request, AcceptedMaterialRequestV1) and isinstance(evidence, AcceptedExecutionEvidenceV1)) or (isinstance(request, AcceptedMaterialRequestV2) and isinstance(evidence, AcceptedExecutionEvidenceV2))
    binds = evidence.binds(request, lease) if matched_version else False  # type: ignore[arg-type]
    if not matched_version or not binds or request.runtime_image_digest != selection.runtime_image_digest or evidence.qualification_scope != selection.qualification.qualification_scope:
        raise AcceptedMaterialError("accepted_material_evidence_mismatch")

    if isinstance(evidence, AcceptedExecutionEvidenceV2):
        profile = selection.capability_profile
        isolation = evidence.isolation
        if (
            evidence.capability_profile_digest != profile.digest
            or evidence.qualification_evidence_digest != selection.qualification.digest
            or (profile.restricted_non_root_isolation and not isolation.restricted_non_root)
            or (profile.restricted_non_root_isolation and not isolation.privilege_escalation_disabled)
            or (profile.material_capability is AcceptedMaterialCapability.IMMUTABLE_READ_ONLY and not isolation.read_only_accepted_material)
        ):
            raise AcceptedMaterialError("accepted_material_evidence_mismatch")


async def resolve_accepted_materializer(
    provider: object,
    *,
    binding: AcceptedSkillSandboxBindingV1,
    thread_id: str,
    user_id: str,
    require_durable_one_replica: bool = False,
    require_exact_two: bool = False,
    allow_qualification_candidate: bool = False,
) -> AcceptedMaterializerSelection | None:
    """Negotiate the provider's accepted materialization capability, if any.

    The worker never imports a concrete adapter: a provider that inherits
    ``AcceptedMaterialization`` answers its own qualified selection, and one
    that does not admits only the explicit empty accepted set.
    """

    from deerflow.sandbox.capabilities import AcceptedMaterialization, sandbox_capability

    materialization = sandbox_capability(provider, AcceptedMaterialization)
    if materialization is None:
        return None
    selection = await materialization.accepted_materializer_selection(
        binding=binding,
        thread_id=thread_id,
        user_id=user_id,
    )
    if selection is not None and not isinstance(
        selection,
        AcceptedMaterializerSelection,
    ):
        raise TypeError(
            "accepted_materializer_selection must return AcceptedMaterializerSelection or None",
        )
    if selection is not None:
        now = datetime.now(UTC)
        qualified = selection.qualification.is_current(now)
        candidate = allow_qualification_candidate is True and selection.qualification.is_candidate_current(now)
        if not qualified and not candidate:
            raise AcceptedMaterialError("sandbox_provider_unqualified")
    if selection is not None and ((require_durable_one_replica and not selection.capability_profile.durable_one_replica) or (require_exact_two and not selection.capability_profile.exact_two)):
        raise AcceptedMaterialError("sandbox_capability_missing")
    return selection


class _InMemoryAcceptedSandbox(Sandbox):
    """Identity-only sandbox used exclusively by contract tests."""

    @staticmethod
    def _unsupported() -> NoReturn:
        raise RuntimeError("in-memory accepted material sandbox is contract-test-only")

    def execute_command(
        self,
        command: str,
        env: dict[str, str] | None = None,
        timeout: float | None = None,
    ) -> str:
        del command, env, timeout
        self._unsupported()

    def read_file(
        self,
        path: str,
        start_line: int | None = None,
        end_line: int | None = None,
    ) -> str:
        del path, start_line, end_line
        self._unsupported()

    def download_file(self, path: str) -> bytes:
        del path
        self._unsupported()

    def list_dir(self, path: str, max_depth: int = 2) -> list[str]:
        del path, max_depth
        self._unsupported()

    def write_file(self, path: str, content: str, append: bool = False) -> None:
        del path, content, append
        self._unsupported()

    def glob(
        self,
        path: str,
        pattern: str,
        *,
        include_dirs: bool = False,
        max_results: int = 200,
    ) -> tuple[list[str], bool]:
        del path, pattern, include_dirs, max_results
        self._unsupported()

    def grep(
        self,
        path: str,
        pattern: str,
        *,
        glob: str | None = None,
        literal: bool = False,
        case_sensitive: bool = False,
        max_results: int = 100,
    ) -> tuple[list[Any], bool]:
        del path, pattern, glob, literal, case_sensitive, max_results
        self._unsupported()

    def update_file(self, path: str, content: bytes) -> None:
        del path, content
        self._unsupported()


@dataclass(slots=True)
class _InMemoryAcceptedRecord:
    request: AcceptedMaterialRequestV1
    sandbox: Sandbox
    owner: str
    lease: AcceptedMaterialLeaseV1
    evidence: AcceptedExecutionEvidenceV1
    released: bool = False


class InMemoryAcceptedMaterialState:
    """Shared state used to exercise process-recovery ownership fencing."""

    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.records: dict[
            tuple[str, str, str],
            _InMemoryAcceptedRecord,
        ] = {}
        self.records_by_provider_ref: dict[str, _InMemoryAcceptedRecord] = {}


class InMemoryAcceptedMaterializer:
    """Stateful contract-test adapter; never a production provider."""

    def __init__(
        self,
        *,
        owner: str,
        state: InMemoryAcceptedMaterialState | None = None,
        clock: Callable[[], datetime] | None = None,
        lease_duration: timedelta = timedelta(minutes=5),
    ) -> None:
        _require_reference(owner, "owner")
        if not isinstance(lease_duration, timedelta) or lease_duration <= timedelta(0) or lease_duration > timedelta(hours=1):
            raise ValueError("lease_duration must be between zero and one hour")
        self._owner = owner
        self._state = state or InMemoryAcceptedMaterialState()
        self._clock = clock or (lambda: datetime.now(UTC))
        self._lease_duration = lease_duration

    def capability(self) -> AcceptedMaterialCapability:
        return AcceptedMaterialCapability.IMMUTABLE_READ_ONLY

    @staticmethod
    def _key(
        request: AcceptedMaterialRequestV1,
    ) -> tuple[str, str, str]:
        return (request.tenant.digest, request.run_id, request.attempt_id)

    @staticmethod
    def _provider_ref(request: AcceptedMaterialRequestV1) -> str:
        suffix = _canonical_digest(
            {
                "version": 1,
                "tenant_digest": request.tenant.digest,
                "run_id": request.run_id,
                "attempt_id": request.attempt_id,
            },
        )[:32]
        return f"in-memory-{suffix}"

    def _new_tuple(
        self,
        *,
        request: AcceptedMaterialRequestV1,
        sandbox: Sandbox,
        epoch: int,
        expires_at: datetime,
    ) -> tuple[AcceptedMaterialLeaseV1, AcceptedExecutionEvidenceV1]:
        provider_ref = sandbox.id
        handle = object()
        lease = AcceptedMaterialLeaseV1(
            version=1,
            provider_kind="in_memory",
            provider_instance_ref=provider_ref,
            ownership_epoch=epoch,
            lease_expires_at=expires_at,
            opaque_renewal_handle=handle,
        )
        evidence = AcceptedExecutionEvidenceV1.build(
            run_id=request.run_id,
            attempt_id=request.attempt_id,
            tenant=request.tenant,
            provider_kind=lease.provider_kind,
            provider_instance_ref=provider_ref,
            ownership_epoch=epoch,
            runtime_image_digest=request.runtime_image_digest,
            skill_snapshot_digest=request.skill_snapshot_digest,
            skill_scope_digest=request.skill_scope_digest,
            materialization_digest=request.digest,
            verifier_image_digest=hashlib.sha256(
                b"in-memory-accepted-material-verifier-v1",
            ).hexdigest(),
            verifier_contract_version="in_memory_contract_v1",
            read_only_proof_digest=_canonical_digest(
                {
                    "version": 1,
                    "request_digest": request.digest,
                    "provider_instance_ref": provider_ref,
                    "ownership_epoch": epoch,
                    "proof": "contract-test-fence",
                },
            ),
            qualification_scope="contract_test_only",
        )
        return lease, evidence

    async def acquire_and_materialize(
        self,
        request: AcceptedMaterialRequestV1,
    ) -> tuple[Sandbox, AcceptedMaterialLeaseV1, AcceptedExecutionEvidenceV1]:
        if not isinstance(request, AcceptedMaterialRequestV1):
            raise TypeError("request must be AcceptedMaterialRequestV1")
        now = self._clock()
        _canonical_timestamp(now)
        key = self._key(request)
        with self._state.lock:
            current = self._state.records.get(key)
            if current is not None and current.request.digest != request.digest:
                raise AcceptedMaterialError("accepted_material_request_conflict")
            if current is not None and not current.released and current.lease.lease_expires_at > now:
                if current.owner != self._owner:
                    raise AcceptedMaterialError("accepted_material_claim_conflict")
                return current.sandbox, current.lease, current.evidence

            if current is None:
                if request.lease_expires_at <= now:
                    raise AcceptedMaterialError("accepted_material_lease_expired")
                sandbox = _InMemoryAcceptedSandbox(self._provider_ref(request))
                epoch = 1
                expires_at = request.lease_expires_at
            else:
                sandbox = current.sandbox
                epoch = current.lease.ownership_epoch + 1
                expires_at = now + self._lease_duration
            lease, evidence = self._new_tuple(
                request=request,
                sandbox=sandbox,
                epoch=epoch,
                expires_at=expires_at,
            )
            record = _InMemoryAcceptedRecord(
                request=request,
                sandbox=sandbox,
                owner=self._owner,
                lease=lease,
                evidence=evidence,
            )
            self._state.records[key] = record
            self._state.records_by_provider_ref[sandbox.id] = record
            return sandbox, lease, evidence

    async def validate(
        self,
        lease: AcceptedMaterialLeaseV1,
        evidence: AcceptedExecutionEvidenceV1,
    ) -> bool:
        if not isinstance(lease, AcceptedMaterialLeaseV1) or not isinstance(
            evidence,
            AcceptedExecutionEvidenceV1,
        ):
            return False
        now = self._clock()
        with self._state.lock:
            current = self._state.records_by_provider_ref.get(
                lease.provider_instance_ref,
            )
            return bool(
                current is not None and not current.released and current.owner == self._owner and current.lease is lease and current.evidence == evidence and current.lease.lease_expires_at > now and evidence.binds(current.request, lease)
            )

    async def renew(
        self,
        lease: AcceptedMaterialLeaseV1,
    ) -> AcceptedMaterialLeaseV1:
        if not isinstance(lease, AcceptedMaterialLeaseV1):
            raise TypeError("lease must be AcceptedMaterialLeaseV1")
        now = self._clock()
        with self._state.lock:
            current = self._state.records_by_provider_ref.get(
                lease.provider_instance_ref,
            )
            if current is None or current.released or current.owner != self._owner or current.lease is not lease or current.lease.lease_expires_at <= now:
                raise AcceptedMaterialError("accepted_material_lease_lost")
            renewed = AcceptedMaterialLeaseV1(
                version=1,
                provider_kind=lease.provider_kind,
                provider_instance_ref=lease.provider_instance_ref,
                ownership_epoch=lease.ownership_epoch,
                lease_expires_at=now + self._lease_duration,
                opaque_renewal_handle=lease.opaque_renewal_handle,
            )
            current.lease = renewed
            return renewed

    async def release(self, lease: AcceptedMaterialLeaseV1) -> None:
        if not isinstance(lease, AcceptedMaterialLeaseV1):
            raise TypeError("lease must be AcceptedMaterialLeaseV1")
        with self._state.lock:
            current = self._state.records_by_provider_ref.get(
                lease.provider_instance_ref,
            )
            if current is not None and current.owner == self._owner and current.lease is lease:
                current.released = True


__all__ = [
    "AcceptedExecutionEvidence",
    "AcceptedExecutionEvidenceV1",
    "AcceptedExecutionEvidenceV2",
    "AcceptedFileType",
    "AcceptedFileV1",
    "AcceptedMaterialCapability",
    "AcceptedMaterialError",
    "AcceptedMaterialExecutionClaimV1",
    "AcceptedMaterialLeaseV1",
    "AcceptedMaterialRequest",
    "AcceptedMaterialRequestV1",
    "AcceptedMaterialRequestV2",
    "AcceptedMaterializer",
    "AcceptedMaterializerSelection",
    "AcceptedRunFenceValidator",
    "AcceptedSandboxAuthorityLostError",
    "AcceptedSandboxCapabilityProfileV1",
    "AcceptedSandboxIsolationFactsV1",
    "AcceptedSandboxLifecycleKind",
    "AcceptedSandboxLifecycleObservationV1",
    "AcceptedSandboxOperationKind",
    "AcceptedSandboxOperationV1",
    "AcceptedSandboxResult",
    "AcceptedSandboxSession",
    "AcceptedSandboxSessionBridge",
    "AcceptedSandboxFenceAdapter",
    "AcceptedSandboxQualificationV1",
    "AcceptedSkillExecutionEvidence",
    "AcceptedSkillExecutionEvidenceV1",
    "AcceptedSkillExecutionEvidenceV2",
    "AcceptedSkillSandboxBindingError",
    "AcceptedSkillSandboxBindingV1",
    "capture_accepted_file_manifest",
    "accepted_execution_evidence_reference",
    "decode_accepted_execution_evidence",
    "decode_accepted_material_request",
    "accepted_sandbox_resource_commitment",
    "accepted_scope_reference",
    "current_accepted_sandbox_bridge",
    "declare_accepted_sandbox_session",
    "rendered_egress_allowance",
    "is_accepted_sandbox_facade",
    "withdraw_accepted_sandbox_session",
    "InMemoryAcceptedMaterialState",
    "InMemoryAcceptedMaterializer",
    "resolve_accepted_materializer",
    "validate_accepted_materialization",
]
