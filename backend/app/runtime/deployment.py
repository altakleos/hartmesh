"""Authenticated deployment facts kept outside the portable runtime port."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, ClassVar, Literal, Protocol, runtime_checkable

from deerflow.extensions.capabilities import (
    CapabilityHealthMonitor,
    CapabilityHealthSnapshot,
    CapabilityManifest,
    capability_health_to_dict,
    capability_manifest_to_dict,
)

DEPLOYMENT_API_VERSION = "deerflow.deployment/v1"
_SHA256_RE = re.compile(r"(?:sha256:)?[0-9a-f]{64}\Z")
_REVISION_RE = re.compile(r"[0-9a-f]{7,64}\Z")
_SAFE_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}\Z")
_IMAGE_REFERENCE_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/@-]{0,511}\Z")


class DeploymentProfile(StrEnum):
    """Operator-selected deployment promise enforced at startup/readiness."""

    local_development = "local_development"
    durable_production = "durable_production"


class PersistenceTier(StrEnum):
    """Location and loss boundary of the authoritative invocation store."""

    process_local = "process_local"
    node_durable = "node_durable"
    shared_durable = "shared_durable"


@dataclass(frozen=True)
class PersistenceReport:
    """Persistence facts; transaction atomicity is independent of durability."""

    tier: PersistenceTier
    atomic_lifecycle: bool
    restart_durable: bool
    pod_loss_durable: bool

    def to_dict(self) -> dict[str, object]:
        return {
            "version": 1,
            "tier": self.tier.value,
            "atomic_lifecycle": self.atomic_lifecycle,
            "restart_durable": self.restart_durable,
            "pod_loss_durable": self.pod_loss_durable,
        }


def describe_persistence(
    database_backend: str,
    *,
    atomic_lifecycle: bool,
) -> PersistenceReport:
    """Describe only durability supplied by the configured application store."""

    if database_backend == "memory":
        return PersistenceReport(
            tier=PersistenceTier.process_local,
            atomic_lifecycle=atomic_lifecycle,
            restart_durable=False,
            pod_loss_durable=False,
        )
    if database_backend == "sqlite":
        return PersistenceReport(
            tier=PersistenceTier.node_durable,
            atomic_lifecycle=atomic_lifecycle,
            restart_durable=True,
            pod_loss_durable=False,
        )
    if database_backend == "postgres":
        return PersistenceReport(
            tier=PersistenceTier.shared_durable,
            atomic_lifecycle=atomic_lifecycle,
            restart_durable=True,
            pod_loss_durable=True,
        )
    raise ValueError("unsupported persistence backend")


def validate_deployment_profile(config: Any) -> None:
    """Reject a production durability promise backed by process-local state."""

    deployment = getattr(config, "deployment", None)
    database = getattr(config, "database", None)
    raw_profile = getattr(deployment, "profile", "local_development")
    if not isinstance(raw_profile, (str, DeploymentProfile)):
        raw_profile = "local_development"
    raw_backend = getattr(database, "backend", "memory")
    if not isinstance(raw_backend, str):
        raw_backend = "memory"
    profile = DeploymentProfile(raw_profile)
    persistence = describe_persistence(
        raw_backend,
        atomic_lifecycle=False,
    )
    if profile is DeploymentProfile.durable_production and persistence.tier is PersistenceTier.process_local:
        raise ValueError("durable_production cannot use process-local invocation state")


def _bounded_optional(value: str | None, *, field_name: str, limit: int) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value or len(value.encode("utf-8")) > limit:
        raise ValueError(f"{field_name} must be a bounded non-empty string")
    if any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise ValueError(f"{field_name} must not contain control characters")
    return value


@dataclass(frozen=True)
class DeploymentProvenance:
    """Bounded build/image identifiers when the deployment supplies them."""

    image_reference: str | None = None
    image_digest: str | None = None
    source_revision: str | None = None

    def __post_init__(self) -> None:
        image_reference = _bounded_optional(
            self.image_reference,
            field_name="image_reference",
            limit=512,
        )
        image_digest = _bounded_optional(
            self.image_digest,
            field_name="image_digest",
            limit=71,
        )
        source_revision = _bounded_optional(
            self.source_revision,
            field_name="source_revision",
            limit=64,
        )
        if image_digest is not None and _SHA256_RE.fullmatch(image_digest) is None:
            raise ValueError("image_digest must be a SHA-256 digest")
        if image_reference is not None:
            if _IMAGE_REFERENCE_RE.fullmatch(image_reference) is None or "://" in image_reference:
                raise ValueError("image_reference must be a credential-free OCI reference")
            if "@" in image_reference and (image_reference.count("@") != 1 or re.search(r"@sha256:[0-9a-f]{64}\Z", image_reference) is None):
                raise ValueError("image_reference must not contain credentials")
        if source_revision is not None and _REVISION_RE.fullmatch(source_revision) is None:
            raise ValueError("source_revision must be a lowercase hexadecimal revision")
        object.__setattr__(self, "image_reference", image_reference)
        object.__setattr__(self, "image_digest", image_digest)
        object.__setattr__(self, "source_revision", source_revision)

    @classmethod
    def from_environment(cls) -> DeploymentProvenance:
        """Read optional deployer-stamped non-secret artifact identifiers."""

        return cls(
            image_reference=os.getenv("DEER_FLOW_IMAGE_REFERENCE"),
            image_digest=os.getenv("DEER_FLOW_IMAGE_DIGEST"),
            source_revision=os.getenv("DEER_FLOW_SOURCE_REVISION"),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "version": 1,
            "image_reference": self.image_reference,
            "image_digest": self.image_digest,
            "source_revision": self.source_revision,
        }


@dataclass(frozen=True)
class QualificationEvidence:
    """One completed, externally produced qualification artifact reference."""

    qualification_id: str
    artifact_digest: str
    completed_at: datetime

    def __post_init__(self) -> None:
        if _SAFE_ID_RE.fullmatch(self.qualification_id) is None:
            raise ValueError("qualification_id must be a bounded safe identifier")
        if _SHA256_RE.fullmatch(self.artifact_digest) is None:
            raise ValueError("artifact_digest must be a SHA-256 digest")
        if self.completed_at.tzinfo is None or self.completed_at.utcoffset() is None:
            raise ValueError("completed_at must be timezone-aware")

    def to_dict(self) -> dict[str, object]:
        return {
            "qualification_id": self.qualification_id,
            "artifact_digest": self.artifact_digest,
            "completed_at": self.completed_at.astimezone(UTC).isoformat().replace("+00:00", "Z"),
        }


@dataclass(frozen=True)
class DeploymentQualification:
    """Completed evidence, or an explicit statement that none was supplied."""

    evidence: tuple[QualificationEvidence, ...] = ()

    def __post_init__(self) -> None:
        evidence = tuple(self.evidence)
        if not all(isinstance(item, QualificationEvidence) for item in evidence):
            raise TypeError("qualification evidence must use QualificationEvidence")
        object.__setattr__(
            self,
            "evidence",
            tuple(
                sorted(
                    evidence,
                    key=lambda item: (
                        item.qualification_id,
                        item.completed_at,
                        item.artifact_digest,
                    ),
                )
            ),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "version": 1,
            "status": "qualified" if self.evidence else "unqualified",
            "evidence": [item.to_dict() for item in self.evidence],
        }


@dataclass(frozen=True)
class DeploymentReport:
    """Safe administrator report for one running Gateway deployment."""

    KIND: ClassVar[str] = "runtime.deployment.report"
    profile: DeploymentProfile
    persistence: PersistenceReport
    extension_manifest: CapabilityManifest
    capability_health: tuple[CapabilityHealthSnapshot, ...]
    provenance: DeploymentProvenance = field(default_factory=DeploymentProvenance)
    qualification: DeploymentQualification = field(default_factory=DeploymentQualification)
    api_version: Literal["deerflow.deployment/v1"] = field(
        default=DEPLOYMENT_API_VERSION,
        init=False,
    )
    kind: Literal["runtime.deployment.report"] = field(default=KIND, init=False)

    def __post_init__(self) -> None:
        health = tuple(self.capability_health)
        if not all(isinstance(item, CapabilityHealthSnapshot) for item in health):
            raise TypeError("capability_health must contain health snapshots")
        object.__setattr__(self, "capability_health", health)

    def to_dict(self) -> dict[str, object]:
        """Return a fresh safe wire projection; no plugin configuration is included."""

        return {
            "api_version": self.api_version,
            "kind": self.kind,
            "profile": self.profile.value,
            "extension_manifest": capability_manifest_to_dict(self.extension_manifest),
            "capability_health": capability_health_to_dict(self.capability_health),
            "provenance": self.provenance.to_dict(),
            "persistence": self.persistence.to_dict(),
            "qualification": self.qualification.to_dict(),
        }


@runtime_checkable
class DeploymentReportPort(Protocol):
    """Administrative Seam kept separate from ``DurableInvocationPort``."""

    async def deployment_report(self) -> DeploymentReport:
        """Return safe immutable and live facts for the current deployment."""

        ...


class GatewayDeploymentReporter(DeploymentReportPort):
    """Compose deployment facts from startup-bound Gateway collaborators."""

    def __init__(
        self,
        *,
        profile: DeploymentProfile | str,
        database_backend: str,
        atomic_lifecycle: bool,
        manifest: CapabilityManifest,
        health_monitor: CapabilityHealthMonitor,
        provenance: DeploymentProvenance | None = None,
        qualification: DeploymentQualification | None = None,
    ) -> None:
        self._profile = DeploymentProfile(profile)
        self._persistence = describe_persistence(
            database_backend,
            atomic_lifecycle=atomic_lifecycle,
        )
        self._manifest = manifest
        self._health_monitor = health_monitor
        self._provenance = provenance or DeploymentProvenance()
        self._qualification = qualification or DeploymentQualification()

    @property
    def persistence_ready(self) -> bool:
        """Whether this store satisfies the configured deployment promise."""

        if self._profile is DeploymentProfile.local_development:
            return True
        return self._persistence.restart_durable and self._persistence.atomic_lifecycle

    def with_runtime_store(
        self,
        *,
        profile: DeploymentProfile | str,
        database_backend: str,
        atomic_lifecycle: bool,
    ) -> GatewayDeploymentReporter:
        """Return a reporter bound to the store/config captured at startup."""

        return GatewayDeploymentReporter(
            profile=profile,
            database_backend=database_backend,
            atomic_lifecycle=atomic_lifecycle,
            manifest=self._manifest,
            health_monitor=self._health_monitor,
            provenance=self._provenance,
            qualification=self._qualification,
        )

    async def deployment_report(self) -> DeploymentReport:
        return DeploymentReport(
            profile=self._profile,
            persistence=self._persistence,
            extension_manifest=self._manifest,
            capability_health=tuple(await self._health_monitor.health()),
            provenance=self._provenance,
            qualification=self._qualification,
        )


__all__ = [
    "DEPLOYMENT_API_VERSION",
    "DeploymentProfile",
    "DeploymentProvenance",
    "DeploymentQualification",
    "DeploymentReport",
    "DeploymentReportPort",
    "GatewayDeploymentReporter",
    "PersistenceReport",
    "PersistenceTier",
    "QualificationEvidence",
    "describe_persistence",
    "validate_deployment_profile",
]
