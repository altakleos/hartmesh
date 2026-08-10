"""Authenticated deployment facts kept outside the portable runtime port."""

from __future__ import annotations

import json
import math
import os
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, ClassVar, Literal, Protocol, runtime_checkable

from app.runtime.readiness import RuntimeReadinessSnapshot
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
_RFC3339_RE = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
    r"(?:\.[0-9]+)?(?:Z|[+-][0-9]{2}:[0-9]{2})\Z"
)


class DeploymentProfile(StrEnum):
    """Operator-selected deployment promise enforced at startup/readiness."""

    local_development = "local_development"
    durable_production = "durable_production"


class PersistenceTier(StrEnum):
    """Location and loss boundary of the authoritative invocation store."""

    process_local = "process_local"
    node_durable = "node_durable"
    shared_durable = "shared_durable"


class IngressDeliveryGuarantee(StrEnum):
    """Acknowledgment boundary for one native ingress source."""

    best_effort = "best_effort"
    durable = "durable"


@dataclass(frozen=True)
class NativeIngressReport:
    """Finite per-source receipt durability kept out of portable capabilities."""

    sources: tuple[tuple[str, IngressDeliveryGuarantee], ...] = ()

    def __post_init__(self) -> None:
        normalized = tuple(sorted((str(source), IngressDeliveryGuarantee(guarantee)) for source, guarantee in self.sources))
        if len(normalized) != len({source for source, _ in normalized}):
            raise ValueError("native ingress report contains duplicate sources")
        if any(_SAFE_ID_RE.fullmatch(source) is None for source, _ in normalized):
            raise ValueError("native ingress source must be a bounded safe identifier")
        object.__setattr__(self, "sources", normalized)

    def to_dict(self) -> dict[str, object]:
        return {
            "version": 1,
            "sources": {source: guarantee.value for source, guarantee in self.sources},
        }


def describe_native_ingress(
    config: Any,
    *,
    verified_sources: frozenset[str] = frozenset(),
) -> NativeIngressReport:
    """Describe configured source support without claiming bus durability."""

    database = getattr(config, "database", None)
    database_backend = getattr(database, "backend", "memory")
    dedupe = getattr(config, "dedupe_storage", None)
    receipt_backend = getattr(dedupe, "backend", "auto")
    if hasattr(receipt_backend, "value"):
        receipt_backend = receipt_backend.value
    extra = getattr(config, "model_extra", None) or {}
    channels = extra.get("channels", {}) if isinstance(extra, dict) else {}
    github = channels.get("github", {}) if isinstance(channels, dict) else {}
    if not isinstance(github, dict) or not github.get("enabled", False):
        return NativeIngressReport()
    guarantee = IngressDeliveryGuarantee.durable if ("github" in verified_sources and database_backend == "postgres" and receipt_backend in {"auto", "postgres"}) else IngressDeliveryGuarantee.best_effort
    return NativeIngressReport(sources=(("github", guarantee),))


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


def validate_deployment_profile(
    config: Any,
    *,
    verified_sources: frozenset[str] = frozenset(),
) -> None:
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
    command_timeout = getattr(database, "command_timeout", 30)
    finite_command_timeout = isinstance(command_timeout, (int, float)) and not isinstance(command_timeout, bool) and math.isfinite(command_timeout) and command_timeout > 0
    if profile is DeploymentProfile.durable_production and raw_backend == "postgres" and not finite_command_timeout:
        raise ValueError("durable_production requires a finite PostgreSQL command_timeout")
    ingress = describe_native_ingress(
        config,
        verified_sources=verified_sources,
    )
    if profile is DeploymentProfile.durable_production and any(guarantee is not IngressDeliveryGuarantee.durable for _source, guarantee in ingress.sources):
        raise ValueError("durable_production requires verified durable native ingress with PostgreSQL inbound receipt storage for every enabled native source")


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
    """One operator-declared reference to a purportedly passing artifact."""

    qualification_id: str
    artifact_digest: str
    completed_at: datetime
    scope: str = "legacy_unspecified"
    status: Literal["passed"] = "passed"

    def __post_init__(self) -> None:
        if _SAFE_ID_RE.fullmatch(self.qualification_id) is None:
            raise ValueError("qualification_id must be a bounded safe identifier")
        if _SHA256_RE.fullmatch(self.artifact_digest) is None:
            raise ValueError("artifact_digest must be a SHA-256 digest")
        if self.completed_at.tzinfo is None or self.completed_at.utcoffset() is None:
            raise ValueError("completed_at must be timezone-aware")
        if _SAFE_ID_RE.fullmatch(self.scope) is None:
            raise ValueError("qualification scope must be a bounded safe identifier")
        if self.status != "passed":
            raise ValueError("only completed passing qualification evidence is accepted")

    def to_dict(self) -> dict[str, object]:
        return {
            "qualification_id": self.qualification_id,
            "scope": self.scope,
            "status": self.status,
            "artifact_digest": self.artifact_digest,
            "completed_at": self.completed_at.astimezone(UTC).isoformat().replace("+00:00", "Z"),
        }


@dataclass(frozen=True)
class DeploymentQualification:
    """Operator-declared evidence references, never in-process attestation."""

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
                        item.scope,
                        item.completed_at,
                        item.artifact_digest,
                    ),
                )
            ),
        )

    @classmethod
    def from_environment(cls) -> DeploymentQualification:
        """Read bounded operator assertions without fetching their artifacts."""

        raw = os.getenv("DEER_FLOW_QUALIFICATION_EVIDENCE")
        if raw is None:
            return cls()
        if not raw or len(raw.encode("utf-8")) > 16 * 1024:
            raise ValueError("qualification evidence must be bounded JSON")
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError("qualification evidence must be valid JSON") from exc
        if not isinstance(payload, list) or len(payload) > 16:
            raise ValueError("qualification evidence must be a list of at most 16 items")
        evidence: list[QualificationEvidence] = []
        legacy_fields = {
            "qualificationId",
            "artifactDigest",
            "completedAt",
        }
        current_fields = legacy_fields | {"scope", "status"}
        for item in payload:
            if not isinstance(item, dict) or frozenset(item) not in {
                frozenset(legacy_fields),
                frozenset(current_fields),
            }:
                raise ValueError("qualification evidence fields are invalid")
            try:
                completed_at_raw = item["completedAt"]
                if not isinstance(completed_at_raw, str) or _RFC3339_RE.fullmatch(completed_at_raw) is None:
                    raise ValueError("completedAt must be RFC3339")
                completed_at = datetime.fromisoformat(completed_at_raw.replace("Z", "+00:00"))
                evidence.append(
                    QualificationEvidence(
                        qualification_id=item["qualificationId"],
                        artifact_digest=item["artifactDigest"],
                        completed_at=completed_at,
                        scope=item.get("scope", "legacy_unspecified"),
                        status=item.get("status", "passed"),
                    )
                )
            except (AttributeError, TypeError, ValueError) as exc:
                raise ValueError("qualification evidence values are invalid") from exc
        return cls(evidence=tuple(evidence))

    def to_dict(self) -> dict[str, object]:
        return {
            "version": 1,
            "status": "qualified" if self.evidence else "unqualified",
            "trust": "operator_asserted" if self.evidence else "none_declared",
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
    admission_readiness: RuntimeReadinessSnapshot | None = None
    provenance: DeploymentProvenance = field(default_factory=DeploymentProvenance)
    qualification: DeploymentQualification = field(default_factory=DeploymentQualification)
    native_ingress: NativeIngressReport = field(default_factory=NativeIngressReport)
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
            "admission_readiness": (
                self.admission_readiness.to_dict()
                if self.admission_readiness is not None
                else {
                    "version": 1,
                    "status": "unknown",
                    "reason_codes": ["not_evaluated"],
                    "checked_at": None,
                    "correlation_id": None,
                }
            ),
            "provenance": self.provenance.to_dict(),
            "persistence": self.persistence.to_dict(),
            "native_ingress": self.native_ingress.to_dict(),
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
        readiness_supplier: Callable[[], RuntimeReadinessSnapshot | None] | None = None,
        provenance: DeploymentProvenance | None = None,
        qualification: DeploymentQualification | None = None,
        native_ingress: NativeIngressReport | None = None,
        native_ingress_supplier: Callable[[], NativeIngressReport] | None = None,
    ) -> None:
        self._profile = DeploymentProfile(profile)
        self._persistence = describe_persistence(
            database_backend,
            atomic_lifecycle=atomic_lifecycle,
        )
        self._manifest = manifest
        self._health_monitor = health_monitor
        self._readiness_supplier = readiness_supplier or (lambda: None)
        self._provenance = provenance or DeploymentProvenance()
        self._qualification = qualification or DeploymentQualification()
        if native_ingress is not None and native_ingress_supplier is not None:
            raise ValueError("native_ingress and native_ingress_supplier are mutually exclusive")
        static_native_ingress = native_ingress or NativeIngressReport()
        self._native_ingress_supplier = native_ingress_supplier or (lambda: static_native_ingress)

    @property
    def persistence_ready(self) -> bool:
        """Whether this store satisfies the configured deployment promise."""

        if self._profile is DeploymentProfile.local_development:
            return True
        return self._persistence.restart_durable and self._persistence.atomic_lifecycle

    @property
    def admission_profile_ready(self) -> bool:
        """Whether persistence and enabled ingress satisfy the profile."""

        if not self.persistence_ready:
            return False
        if self._profile is DeploymentProfile.local_development:
            return True
        return all(guarantee is IngressDeliveryGuarantee.durable for _source, guarantee in self._native_ingress_supplier().sources)

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
            readiness_supplier=self._readiness_supplier,
            provenance=self._provenance,
            qualification=self._qualification,
            native_ingress_supplier=self._native_ingress_supplier,
        )

    async def deployment_report(self) -> DeploymentReport:
        return DeploymentReport(
            profile=self._profile,
            persistence=self._persistence,
            extension_manifest=self._manifest,
            capability_health=tuple(await self._health_monitor.health()),
            admission_readiness=self._readiness_supplier(),
            provenance=self._provenance,
            qualification=self._qualification,
            native_ingress=self._native_ingress_supplier(),
        )


__all__ = [
    "DEPLOYMENT_API_VERSION",
    "DeploymentProfile",
    "DeploymentProvenance",
    "DeploymentQualification",
    "DeploymentReport",
    "DeploymentReportPort",
    "GatewayDeploymentReporter",
    "IngressDeliveryGuarantee",
    "NativeIngressReport",
    "PersistenceReport",
    "PersistenceTier",
    "QualificationEvidence",
    "describe_persistence",
    "describe_native_ingress",
    "validate_deployment_profile",
]
