"""Bounded live evidence for the exact two-Gateway Kubernetes profile."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from types import MappingProxyType
from typing import ClassVar, Literal, Protocol

from deerflow.deployment.topology import (
    MULTI_GATEWAY_IMAGE_NAMES,
    MULTI_GATEWAY_PROFILE,
    MULTI_GATEWAY_QUALIFICATION_SCOPE,
    MULTI_GATEWAY_REPLICA_COUNT,
    ReplicaRegistrationV1,
)
from deerflow.qualification_evidence import (
    MAX_QUALIFICATION_EVIDENCE_BYTES,
    QualificationVerificationError,
    QualificationVerificationResult,
    _bounded_safe,
    _canonical_json_bytes,
    _parse_canonical_qualification_json,
    qualification_evidence_digest,
)

MULTI_GATEWAY_QUALIFICATION_API_VERSION = "deerflow.kubernetes-multi-gateway-qualification/v1"
MULTI_GATEWAY_QUALIFICATION_KIND = "kubernetes.qualification.evidence"
MULTI_GATEWAY_QUALIFICATION_SCENARIOS = (
    "topology_identity",
    "concurrent_admission",
    "execution_ownership",
    "owner_sigkill",
    "sse_reconnect",
    "scheduler_occurrence",
    "scheduler_owner_loss",
    "sandbox_recovery",
    "mcp_task_notification",
    "cancellation_finalization",
    "redis_outage_recovery",
    "postgresql_interruption",
    "config_artifact_skew",
    "tenant_separation",
    "unsupported_surfaces",
    "upgrade_truthfulness",
)
MULTI_GATEWAY_SCENARIO_RESULTS: Mapping[str, str] = MappingProxyType(
    {
        "topology_identity": "two_distinct_identical_replicas",
        "concurrent_admission": "one_run_stable_conflict",
        "execution_ownership": "one_authoritative_attempt",
        "owner_sigkill": "bounded_takeover_stale_rejected",
        "sse_reconnect": "complete_ordered_cursor_replay",
        "scheduler_occurrence": "one_launch_global_cap",
        "scheduler_owner_loss": "eventual_single_admission",
        "sandbox_recovery": "exact_material_recovered",
        "mcp_task_notification": "one_result_one_notification",
        "cancellation_finalization": "one_terminal_complete_cleanup",
        "redis_outage_recovery": "retryable_transport_history_rebuilt",
        "postgresql_interruption": "no_split_brain_stale_rejected",
        "config_artifact_skew": "mismatched_pod_unready",
        "tenant_separation": "cross_tenant_access_denied",
        "unsupported_surfaces": "all_unsupported_rejected",
        "upgrade_truthfulness": "mixed_rejected_maintenance_passed",
    }
)

_SHA256 = re.compile(r"sha256:[0-9a-f]{64}\Z")
_RAW_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_SCHEMA_REF = re.compile(r"schema:sha256:[0-9a-f]{64}\Z")
_TENANT_REF = re.compile(r"tenant-[0-9a-f]{16}\Z")
_SAFE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}\Z")
_SAFE_NAMESPACE = re.compile(r"[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?\Z")
_MIGRATION = re.compile(r"[0-9]{4}_[a-z0-9_]{1,123}\Z")
_GIT_REVISION = re.compile(r"[0-9a-f]{7,64}\Z")
_KUBERNETES_REF_FIELDS = frozenset(
    {
        "gateway_service_uid",
        "gateway_pod_0_uid",
        "gateway_pod_1_uid",
        "provisioner_pod_uid",
        "sandbox_pvc_uid",
    }
)
_TAKEOVER_SCENARIOS = frozenset(
    {
        "owner_sigkill",
        "scheduler_owner_loss",
        "sandbox_recovery",
        "mcp_task_notification",
        "postgresql_interruption",
    }
)
_STALE_REJECTION_SCENARIOS = frozenset({"owner_sigkill", "postgresql_interruption"})
_POD_LOSS_SCENARIOS = frozenset(
    {
        "owner_sigkill",
        "scheduler_owner_loss",
        "sandbox_recovery",
        "mcp_task_notification",
    }
)
_DEPENDENCY_INTERRUPTION_SCENARIOS = frozenset({"redis_outage_recovery", "postgresql_interruption"})
_EXPECTED_AUTHORITATIVE_COUNTS = MappingProxyType(
    {
        "topology_identity": 2,
        "owner_sigkill": 6,
        "scheduler_owner_loss": 2,
        "cancellation_finalization": 3,
        "postgresql_interruption": 3,
    }
)
_EXPECTED_VERIFIED_CASE_COUNTS = MappingProxyType(
    {
        "topology_identity": 2,
        "owner_sigkill": 6,
        "scheduler_owner_loss": 2,
        "cancellation_finalization": 3,
        "unsupported_surfaces": 8,
        "upgrade_truthfulness": 2,
        "postgresql_interruption": 3,
    }
)


def _require_digest(value: object, *, name: str, raw: bool = False) -> str:
    pattern = _RAW_SHA256 if raw else _SHA256
    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        raise ValueError(f"{name} is invalid")
    return value


def _aware(value: datetime, *, name: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


def _timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _parse_timestamp(value: object, *, name: str) -> datetime:
    if not isinstance(value, str) or not value or len(value) > 64:
        raise ValueError(f"{name} is invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{name} is invalid") from exc
    return _aware(parsed, name=name)


@dataclass(frozen=True, slots=True)
class MultiGatewayScenarioEvidenceV1:
    """One independently named scenario with bounded invariant counters."""

    scenario_id: str
    result_code: str
    input_digest: str
    evidence_digest: str
    authoritative_count: int
    duplicate_count: int
    stale_write_rejections: int
    takeover_count: int
    pod_deletion_count: int
    pod_restart_count: int
    lease_epoch_before: int
    lease_epoch_after: int
    dependency_interruption_count: int
    duration_millis: int
    verified_case_count: int = 1
    cleanup_count: int = 0
    retryable_failure_count: int = 0
    status: Literal["passed"] = "passed"

    def __post_init__(self) -> None:
        expected_result = MULTI_GATEWAY_SCENARIO_RESULTS.get(self.scenario_id)
        if expected_result is None or self.result_code != expected_result:
            raise ValueError("multi-Gateway scenario result is invalid")
        _require_digest(self.input_digest, name="scenario input digest")
        _require_digest(self.evidence_digest, name="scenario evidence digest")
        counts = (
            self.authoritative_count,
            self.duplicate_count,
            self.stale_write_rejections,
            self.takeover_count,
            self.pod_deletion_count,
            self.pod_restart_count,
            self.lease_epoch_before,
            self.lease_epoch_after,
            self.dependency_interruption_count,
            self.duration_millis,
            self.verified_case_count,
            self.cleanup_count,
            self.retryable_failure_count,
        )
        if any(type(value) is not int or value < 0 for value in counts):
            raise ValueError("scenario counters must be non-negative integers")
        expected_authoritative = _EXPECTED_AUTHORITATIVE_COUNTS.get(
            self.scenario_id,
            1,
        )
        expected_verified_cases = _EXPECTED_VERIFIED_CASE_COUNTS.get(
            self.scenario_id,
            1,
        )
        expected_cleanup = 3 if self.scenario_id == "cancellation_finalization" else 0
        expected_retryable_failures = 1 if self.scenario_id == "redis_outage_recovery" else 0
        if (
            self.authoritative_count != expected_authoritative
            or self.duplicate_count != 0
            or self.verified_case_count != expected_verified_cases
            or self.cleanup_count != expected_cleanup
            or self.retryable_failure_count != expected_retryable_failures
            or not 1 <= self.duration_millis <= 3_600_000
            or self.status != "passed"
        ):
            raise ValueError("scenario invariant counters are invalid")
        if self.scenario_id == "owner_sigkill" and (self.stale_write_rejections != 6 or self.takeover_count != 6 or self.pod_deletion_count != 6 or self.pod_restart_count != 6):
            raise ValueError("scenario invariant counters are invalid")
        if self.scenario_id == "postgresql_interruption" and (self.stale_write_rejections != 7 or self.takeover_count != 3 or self.pod_deletion_count != 0 or self.pod_restart_count != 0):
            raise ValueError("scenario invariant counters are invalid")
        if self.scenario_id == "scheduler_owner_loss" and (self.takeover_count != 2 or self.pod_deletion_count != 2 or self.pod_restart_count != 2):
            raise ValueError("scenario invariant counters are invalid")
        if self.scenario_id in _TAKEOVER_SCENARIOS and (self.takeover_count < 1 or self.lease_epoch_after <= self.lease_epoch_before):
            raise ValueError("takeover scenario lacks takeover evidence")
        if self.scenario_id in _STALE_REJECTION_SCENARIOS and self.stale_write_rejections < 1:
            raise ValueError("fencing scenario lacks stale-write rejection")
        if self.scenario_id in _POD_LOSS_SCENARIOS and (self.pod_deletion_count < 1 or self.pod_restart_count < 1):
            raise ValueError("pod-loss scenario lacks delete/restart evidence")
        if self.scenario_id in _DEPENDENCY_INTERRUPTION_SCENARIOS and (self.dependency_interruption_count != 1):
            raise ValueError("dependency scenario lacks one bounded interruption")
        if self.scenario_id == "upgrade_truthfulness" and (self.pod_deletion_count < 2 or self.pod_restart_count < 2):
            raise ValueError("upgrade scenario lacks maintenance restart evidence")

    def to_dict(self) -> dict[str, object]:
        return {
            "scenario_id": self.scenario_id,
            "status": self.status,
            "result_code": self.result_code,
            "input_digest": self.input_digest,
            "evidence_digest": self.evidence_digest,
            "authoritative_count": self.authoritative_count,
            "duplicate_count": self.duplicate_count,
            "stale_write_rejections": self.stale_write_rejections,
            "takeover_count": self.takeover_count,
            "pod_deletion_count": self.pod_deletion_count,
            "pod_restart_count": self.pod_restart_count,
            "lease_epoch_before": self.lease_epoch_before,
            "lease_epoch_after": self.lease_epoch_after,
            "dependency_interruption_count": self.dependency_interruption_count,
            "duration_millis": self.duration_millis,
            "verified_case_count": self.verified_case_count,
            "cleanup_count": self.cleanup_count,
            "retryable_failure_count": self.retryable_failure_count,
        }

    @classmethod
    def from_dict(cls, value: object) -> MultiGatewayScenarioEvidenceV1:
        fields = {
            "scenario_id",
            "status",
            "result_code",
            "input_digest",
            "evidence_digest",
            "authoritative_count",
            "duplicate_count",
            "stale_write_rejections",
            "takeover_count",
            "pod_deletion_count",
            "pod_restart_count",
            "lease_epoch_before",
            "lease_epoch_after",
            "dependency_interruption_count",
            "duration_millis",
            "verified_case_count",
            "cleanup_count",
            "retryable_failure_count",
        }
        if not isinstance(value, dict) or set(value) != fields:
            raise ValueError("multi-Gateway scenario fields are invalid")
        try:
            return cls(**value)
        except (TypeError, ValueError) as exc:
            raise ValueError("multi-Gateway scenario values are invalid") from exc


SafeObservationValue = str | int | bool


def _safe_observation_facts(
    value: Mapping[str, SafeObservationValue],
    *,
    name: str,
) -> Mapping[str, SafeObservationValue]:
    if not isinstance(value, Mapping) or not 1 <= len(value) <= 32:
        raise ValueError(f"{name} must contain 1-32 bounded facts")
    facts: dict[str, SafeObservationValue] = {}
    for key, item in sorted(value.items()):
        if not isinstance(key, str) or _SAFE_ID.fullmatch(key) is None:
            raise ValueError(f"{name} contains an invalid key")
        if isinstance(item, bool):
            facts[key] = item
        elif type(item) is int and 0 <= item <= 2**63 - 1:
            facts[key] = item
        elif isinstance(item, str):
            facts[key] = _bounded_safe(
                item,
                name=f"{name}.{key}",
                limit=256,
            )
        else:
            raise ValueError(f"{name} contains an invalid value")
    if len(_canonical_json_bytes(facts)) > 4096:
        raise ValueError(f"{name} exceeds 4 KiB")
    return MappingProxyType(facts)


@dataclass(frozen=True, slots=True)
class MultiGatewayScenarioObservationV1:
    """Raw bounded facts returned by one independently implemented live case."""

    scenario_id: str
    input_facts: Mapping[str, SafeObservationValue]
    evidence_facts: Mapping[str, SafeObservationValue]
    authoritative_count: int
    duplicate_count: int
    stale_write_rejections: int
    takeover_count: int
    pod_deletion_count: int
    pod_restart_count: int
    lease_epoch_before: int
    lease_epoch_after: int
    dependency_interruption_count: int
    duration_millis: int
    verified_case_count: int = 1
    cleanup_count: int = 0
    retryable_failure_count: int = 0

    def __post_init__(self) -> None:
        if self.scenario_id not in MULTI_GATEWAY_SCENARIO_RESULTS:
            raise ValueError("observation scenario is invalid")
        object.__setattr__(
            self,
            "input_facts",
            _safe_observation_facts(self.input_facts, name="input_facts"),
        )
        object.__setattr__(
            self,
            "evidence_facts",
            _safe_observation_facts(
                self.evidence_facts,
                name="evidence_facts",
            ),
        )

    def to_evidence(self) -> MultiGatewayScenarioEvidenceV1:
        """Digest the independently observed facts and enforce invariants."""

        return MultiGatewayScenarioEvidenceV1(
            scenario_id=self.scenario_id,
            result_code=MULTI_GATEWAY_SCENARIO_RESULTS[self.scenario_id],
            input_digest="sha256:" + hashlib.sha256(_canonical_json_bytes(dict(self.input_facts))).hexdigest(),
            evidence_digest="sha256:" + hashlib.sha256(_canonical_json_bytes(dict(self.evidence_facts))).hexdigest(),
            authoritative_count=self.authoritative_count,
            duplicate_count=self.duplicate_count,
            stale_write_rejections=self.stale_write_rejections,
            takeover_count=self.takeover_count,
            pod_deletion_count=self.pod_deletion_count,
            pod_restart_count=self.pod_restart_count,
            lease_epoch_before=self.lease_epoch_before,
            lease_epoch_after=self.lease_epoch_after,
            dependency_interruption_count=(self.dependency_interruption_count),
            duration_millis=self.duration_millis,
            verified_case_count=self.verified_case_count,
            cleanup_count=self.cleanup_count,
            retryable_failure_count=self.retryable_failure_count,
        )


@dataclass(frozen=True, slots=True)
class MultiGatewayQualificationSubjectsV1:
    """Exact independently collected subjects used to build one artifact."""

    git_revision: str
    chart_version: str
    chart_digest: str
    image_digests: Mapping[str, str]
    configuration_digest: str
    migration_head: str
    tenant_public_ref: str
    tenant_digest: str
    namespace: str
    kubernetes_refs: Mapping[str, str]
    database_schema_ref: str
    redis_namespace_digest: str
    redis_acl_proof_digest: str
    extension_artifact_digest: str
    extension_configuration_digest: str
    capability_manifest_digest: str
    topology_registrations: tuple[ReplicaRegistrationV1, ...]

    @classmethod
    def from_evidence(
        cls,
        evidence: KubernetesMultiGatewayQualificationEvidenceV1,
    ) -> MultiGatewayQualificationSubjectsV1:
        """Copy subjects from a validated artifact for contract-test drivers."""

        return cls(
            git_revision=evidence.git_revision,
            chart_version=evidence.chart_version,
            chart_digest=evidence.chart_digest,
            image_digests=evidence.image_digests,
            configuration_digest=evidence.configuration_digest,
            migration_head=evidence.migration_head,
            tenant_public_ref=evidence.tenant_public_ref,
            tenant_digest=evidence.tenant_digest,
            namespace=evidence.namespace,
            kubernetes_refs=evidence.kubernetes_refs,
            database_schema_ref=evidence.database_schema_ref,
            redis_namespace_digest=evidence.redis_namespace_digest,
            redis_acl_proof_digest=evidence.redis_acl_proof_digest,
            extension_artifact_digest=evidence.extension_artifact_digest,
            extension_configuration_digest=(evidence.extension_configuration_digest),
            capability_manifest_digest=evidence.capability_manifest_digest,
            topology_registrations=evidence.topology_registrations,
        )


class MultiGatewayQualificationDriver(Protocol):
    """Live adapter boundary; a driver cannot omit or reorder scenarios."""

    async def prepare(self) -> MultiGatewayQualificationSubjectsV1: ...

    async def run_scenario(
        self,
        scenario_id: str,
    ) -> MultiGatewayScenarioObservationV1: ...

    async def close(self) -> None: ...


@dataclass(frozen=True, slots=True)
class KubernetesMultiGatewayQualificationEvidenceV1:
    """Canonical passed evidence for the exact two-replica topology."""

    API_VERSION: ClassVar[str] = MULTI_GATEWAY_QUALIFICATION_API_VERSION
    SCOPE: ClassVar[str] = MULTI_GATEWAY_QUALIFICATION_SCOPE
    REQUIRED_SCENARIOS: ClassVar[tuple[str, ...]] = MULTI_GATEWAY_QUALIFICATION_SCENARIOS

    qualification_id: str
    git_revision: str
    chart_version: str
    chart_digest: str
    image_digests: Mapping[str, str]
    configuration_digest: str
    migration_head: str
    tenant_public_ref: str
    tenant_digest: str
    namespace: str
    kubernetes_refs: Mapping[str, str]
    database_schema_ref: str
    redis_namespace_digest: str
    redis_acl_proof_digest: str
    extension_artifact_digest: str
    extension_configuration_digest: str
    capability_manifest_digest: str
    topology_registrations: tuple[ReplicaRegistrationV1, ...]
    scenarios: tuple[MultiGatewayScenarioEvidenceV1, ...]
    started_at: datetime
    completed_at: datetime
    artifact_digest: str = ""

    def __post_init__(self) -> None:
        if _SAFE_ID.fullmatch(self.qualification_id) is None:
            raise ValueError("qualification_id is invalid")
        if _GIT_REVISION.fullmatch(self.git_revision) is None:
            raise ValueError("git_revision is invalid")
        _bounded_safe(self.chart_version, name="chart_version", limit=128)
        for name in (
            "chart_digest",
            "configuration_digest",
            "redis_namespace_digest",
            "redis_acl_proof_digest",
            "extension_artifact_digest",
            "extension_configuration_digest",
            "capability_manifest_digest",
        ):
            _require_digest(getattr(self, name), name=name)
        if _MIGRATION.fullmatch(self.migration_head) is None:
            raise ValueError("migration_head is invalid")
        if _TENANT_REF.fullmatch(self.tenant_public_ref) is None:
            raise ValueError("tenant_public_ref is invalid")
        _require_digest(self.tenant_digest, name="tenant_digest", raw=True)
        if _SAFE_NAMESPACE.fullmatch(self.namespace) is None or not self.namespace.startswith("hartmesh-qualification-"):
            raise ValueError("namespace is invalid")
        if _SCHEMA_REF.fullmatch(self.database_schema_ref) is None:
            raise ValueError("database_schema_ref is invalid")

        images = dict(sorted(self.image_digests.items()))
        if set(images) != MULTI_GATEWAY_IMAGE_NAMES:
            raise ValueError("qualified image set is invalid")
        for digest in images.values():
            _require_digest(digest, name="image digest")
        object.__setattr__(self, "image_digests", MappingProxyType(images))

        refs = dict(sorted(self.kubernetes_refs.items()))
        if set(refs) != _KUBERNETES_REF_FIELDS:
            raise ValueError("Kubernetes UID reference set is invalid")
        if any(not isinstance(value, str) or _SAFE_ID.fullmatch(value) is None for value in refs.values()):
            raise ValueError("Kubernetes UID reference is invalid")
        if refs["gateway_pod_0_uid"] == refs["gateway_pod_1_uid"]:
            raise ValueError("Gateway pod UIDs must be distinct")
        object.__setattr__(self, "kubernetes_refs", MappingProxyType(refs))

        registrations = tuple(self.topology_registrations)
        if len(registrations) != MULTI_GATEWAY_REPLICA_COUNT or len({item.replica_id for item in registrations}) != 2 or tuple(sorted(item.replica_id for item in registrations)) != tuple(item.replica_id for item in registrations):
            raise ValueError("topology registrations must be two distinct ordered replicas")
        topology_digests = {item.topology_fingerprint.digest for item in registrations}
        if len(topology_digests) != 1:
            raise ValueError("topology registration fingerprints differ")
        for registration in registrations:
            fingerprint = registration.topology_fingerprint
            expected = {
                "profile": MULTI_GATEWAY_PROFILE,
                "tenant_digest": self.tenant_digest,
                "image_digests": dict(self.image_digests),
                "config_digest": self.configuration_digest,
                "database_schema_ref": self.database_schema_ref,
                "redis_namespace_digest": self.redis_namespace_digest,
                "extension_artifact_digest": self.extension_artifact_digest,
                "extension_configuration_digest": (self.extension_configuration_digest),
                "capability_manifest_digest": self.capability_manifest_digest.removeprefix("sha256:"),
                "migration_head": self.migration_head,
                "accepted_materialization_profile": "rwx_verified_copy_v2",
            }
            actual = fingerprint.to_dict()
            if any(actual[name] != value for name, value in expected.items()):
                raise ValueError("topology fingerprint does not match artifact subjects")
        object.__setattr__(self, "topology_registrations", registrations)

        scenarios = tuple(self.scenarios)
        if tuple(item.scenario_id for item in scenarios) != self.REQUIRED_SCENARIOS:
            raise ValueError("multi-Gateway scenario coverage is incomplete or out of order")
        object.__setattr__(self, "scenarios", scenarios)

        started_at = _aware(self.started_at, name="started_at")
        completed_at = _aware(self.completed_at, name="completed_at")
        if not started_at <= completed_at <= started_at + timedelta(hours=24):
            raise ValueError("qualification timestamps are invalid")
        object.__setattr__(self, "started_at", started_at)
        object.__setattr__(self, "completed_at", completed_at)
        computed = "sha256:" + hashlib.sha256(_canonical_json_bytes(self._core_dict())).hexdigest()
        if self.artifact_digest:
            _require_digest(self.artifact_digest, name="artifact_digest")
            if self.artifact_digest != computed:
                raise ValueError("qualification artifact digest mismatch")
        else:
            object.__setattr__(self, "artifact_digest", computed)

    def _core_dict(self) -> dict[str, object]:
        return {
            "api_version": self.API_VERSION,
            "kind": MULTI_GATEWAY_QUALIFICATION_KIND,
            "status": "passed",
            "scope": self.SCOPE,
            "qualification_id": self.qualification_id,
            "artifacts": {
                "git_revision": self.git_revision,
                "chart_version": self.chart_version,
                "chart_digest": self.chart_digest,
                "image_digests": dict(self.image_digests),
                "configuration_digest": self.configuration_digest,
                "migration_head": self.migration_head,
                "extension_artifact_digest": self.extension_artifact_digest,
                "extension_configuration_digest": (self.extension_configuration_digest),
                "capability_manifest_digest": self.capability_manifest_digest,
            },
            "environment": {
                "tenant_public_ref": self.tenant_public_ref,
                "tenant_digest": self.tenant_digest,
                "namespace": self.namespace,
                "kubernetes_refs": dict(self.kubernetes_refs),
                "database_schema_ref": self.database_schema_ref,
                "redis_namespace_digest": self.redis_namespace_digest,
                "redis_acl_proof_digest": self.redis_acl_proof_digest,
            },
            "topology_registrations": [item.to_dict() for item in self.topology_registrations],
            "scenarios": [item.to_dict() for item in self.scenarios],
            "started_at": _timestamp(self.started_at),
            "completed_at": _timestamp(self.completed_at),
        }

    def to_dict(self) -> dict[str, object]:
        return {**self._core_dict(), "artifact_digest": self.artifact_digest}

    def canonical_bytes(self) -> bytes:
        payload = _canonical_json_bytes(self.to_dict())
        if len(payload) > MAX_QUALIFICATION_EVIDENCE_BYTES:
            raise ValueError("qualification evidence exceeds 64 KiB")
        return payload

    @classmethod
    def from_dict(
        cls,
        value: object,
    ) -> KubernetesMultiGatewayQualificationEvidenceV1:
        fields = {
            "api_version",
            "kind",
            "status",
            "scope",
            "qualification_id",
            "artifacts",
            "environment",
            "topology_registrations",
            "scenarios",
            "started_at",
            "completed_at",
            "artifact_digest",
        }
        if not isinstance(value, dict) or set(value) != fields:
            raise ValueError("multi-Gateway evidence fields are invalid")
        if value["api_version"] != cls.API_VERSION or value["kind"] != MULTI_GATEWAY_QUALIFICATION_KIND or value["status"] != "passed" or value["scope"] != cls.SCOPE:
            raise ValueError("multi-Gateway evidence discriminator is invalid")
        artifact_fields = {
            "git_revision",
            "chart_version",
            "chart_digest",
            "image_digests",
            "configuration_digest",
            "migration_head",
            "extension_artifact_digest",
            "extension_configuration_digest",
            "capability_manifest_digest",
        }
        environment_fields = {
            "tenant_public_ref",
            "tenant_digest",
            "namespace",
            "kubernetes_refs",
            "database_schema_ref",
            "redis_namespace_digest",
            "redis_acl_proof_digest",
        }
        artifacts = value["artifacts"]
        environment = value["environment"]
        registrations = value["topology_registrations"]
        scenarios = value["scenarios"]
        if not isinstance(artifacts, dict) or set(artifacts) != artifact_fields:
            raise ValueError("multi-Gateway artifact fields are invalid")
        if not isinstance(environment, dict) or set(environment) != environment_fields:
            raise ValueError("multi-Gateway environment fields are invalid")
        if not isinstance(registrations, list) or len(registrations) != 2:
            raise ValueError("multi-Gateway topology registrations are invalid")
        if not isinstance(scenarios, list) or len(scenarios) != 16:
            raise ValueError("multi-Gateway scenarios are invalid")
        try:
            return cls(
                qualification_id=value["qualification_id"],
                **artifacts,
                **environment,
                topology_registrations=tuple(ReplicaRegistrationV1.from_dict(item) for item in registrations),
                scenarios=tuple(MultiGatewayScenarioEvidenceV1.from_dict(item) for item in scenarios),
                started_at=_parse_timestamp(value["started_at"], name="started_at"),
                completed_at=_parse_timestamp(
                    value["completed_at"],
                    name="completed_at",
                ),
                artifact_digest=value["artifact_digest"],
            )
        except (TypeError, ValueError) as exc:
            raise ValueError("multi-Gateway evidence values are invalid") from exc

    @classmethod
    def from_bytes(
        cls,
        payload: bytes,
    ) -> KubernetesMultiGatewayQualificationEvidenceV1:
        evidence = cls.from_dict(_parse_canonical_qualification_json(payload))
        if payload != evidence.canonical_bytes():
            raise ValueError("qualification evidence is not canonical JSON")
        return evidence


async def run_multi_gateway_qualification(
    driver: MultiGatewayQualificationDriver,
    *,
    qualification_id: str,
    clock: Callable[[], datetime] | None = None,
) -> KubernetesMultiGatewayQualificationEvidenceV1:
    """Run all 16 cases and emit evidence only after every invariant passes."""

    now = clock or (lambda: datetime.now(UTC))
    started_at = _aware(now(), name="qualification start")
    try:
        subjects = await driver.prepare()
        if not isinstance(subjects, MultiGatewayQualificationSubjectsV1):
            raise TypeError("qualification driver returned invalid subjects")
        scenarios: list[MultiGatewayScenarioEvidenceV1] = []
        for scenario_id in MULTI_GATEWAY_QUALIFICATION_SCENARIOS:
            observation = await driver.run_scenario(scenario_id)
            if not isinstance(observation, MultiGatewayScenarioObservationV1):
                raise TypeError("qualification driver returned invalid observation")
            if observation.scenario_id != scenario_id:
                raise ValueError("qualification driver returned an unexpected scenario")
            scenarios.append(observation.to_evidence())
        completed_at = _aware(now(), name="qualification completion")
        return KubernetesMultiGatewayQualificationEvidenceV1(
            qualification_id=qualification_id,
            git_revision=subjects.git_revision,
            chart_version=subjects.chart_version,
            chart_digest=subjects.chart_digest,
            image_digests=subjects.image_digests,
            configuration_digest=subjects.configuration_digest,
            migration_head=subjects.migration_head,
            tenant_public_ref=subjects.tenant_public_ref,
            tenant_digest=subjects.tenant_digest,
            namespace=subjects.namespace,
            kubernetes_refs=subjects.kubernetes_refs,
            database_schema_ref=subjects.database_schema_ref,
            redis_namespace_digest=subjects.redis_namespace_digest,
            redis_acl_proof_digest=subjects.redis_acl_proof_digest,
            extension_artifact_digest=subjects.extension_artifact_digest,
            extension_configuration_digest=(subjects.extension_configuration_digest),
            capability_manifest_digest=subjects.capability_manifest_digest,
            topology_registrations=subjects.topology_registrations,
            scenarios=tuple(scenarios),
            started_at=started_at,
            completed_at=completed_at,
        )
    finally:
        await driver.close()


@dataclass(frozen=True, slots=True)
class MultiGatewayQualificationExpectationV1:
    """Independent exact subjects and freshness budget for offline verification."""

    qualification_id: str
    git_revision: str
    chart_version: str
    chart_digest: str
    image_digests: Mapping[str, str]
    configuration_digest: str
    migration_head: str
    tenant_public_ref: str
    tenant_digest: str
    namespace: str
    kubernetes_refs: Mapping[str, str]
    database_schema_ref: str
    redis_namespace_digest: str
    redis_acl_proof_digest: str
    extension_artifact_digest: str
    extension_configuration_digest: str
    capability_manifest_digest: str
    topology_digest: str
    scope: str
    required_scenarios: tuple[str, ...]
    max_age_seconds: int

    def __post_init__(self) -> None:
        if self.scope != MULTI_GATEWAY_QUALIFICATION_SCOPE:
            raise ValueError("expected multi-Gateway scope is invalid")
        if tuple(self.required_scenarios) != MULTI_GATEWAY_QUALIFICATION_SCENARIOS:
            raise ValueError("expected multi-Gateway scenarios are invalid")
        if type(self.max_age_seconds) is not int or not 1 <= self.max_age_seconds <= 2_592_000:
            raise ValueError("max_age_seconds must be in [1, 2592000]")
        images = dict(sorted(self.image_digests.items()))
        if set(images) != MULTI_GATEWAY_IMAGE_NAMES:
            raise ValueError("expected image set is invalid")
        for digest in images.values():
            _require_digest(digest, name="expected image digest")
        object.__setattr__(self, "image_digests", MappingProxyType(images))
        refs = dict(sorted(self.kubernetes_refs.items()))
        if set(refs) != _KUBERNETES_REF_FIELDS:
            raise ValueError("expected Kubernetes UID reference set is invalid")
        if any(not isinstance(value, str) or _SAFE_ID.fullmatch(value) is None for value in refs.values()):
            raise ValueError("expected Kubernetes UID reference is invalid")
        if refs["gateway_pod_0_uid"] == refs["gateway_pod_1_uid"]:
            raise ValueError("expected Gateway pod UIDs must be distinct")
        object.__setattr__(self, "kubernetes_refs", MappingProxyType(refs))
        _require_digest(
            self.redis_acl_proof_digest,
            name="expected redis_acl_proof_digest",
        )
        _require_digest(self.topology_digest, name="topology_digest", raw=True)


def verify_multi_gateway_qualification_evidence(
    artifact: bytes,
    *,
    declared_digest: str,
    expected: MultiGatewayQualificationExpectationV1,
    now: datetime | None = None,
) -> QualificationVerificationResult:
    """Verify bytes, exact subjects, scenarios, topology identity, and freshness."""

    try:
        actual_digest = qualification_evidence_digest(artifact)
    except (TypeError, ValueError) as exc:
        raise QualificationVerificationError("artifact_unreadable") from exc
    if not isinstance(declared_digest, str) or _SHA256.fullmatch(declared_digest) is None:
        raise QualificationVerificationError("declared_digest_invalid")
    if actual_digest != declared_digest:
        raise QualificationVerificationError("artifact_digest_mismatch")
    if not isinstance(expected, MultiGatewayQualificationExpectationV1):
        raise QualificationVerificationError("expectation_invalid")
    try:
        evidence = KubernetesMultiGatewayQualificationEvidenceV1.from_bytes(artifact)
    except (TypeError, ValueError) as exc:
        raise QualificationVerificationError("artifact_invalid") from exc
    subjects = {
        "qualification_id": evidence.qualification_id,
        "git_revision": evidence.git_revision,
        "chart_version": evidence.chart_version,
        "chart_digest": evidence.chart_digest,
        "image_digests": dict(evidence.image_digests),
        "configuration_digest": evidence.configuration_digest,
        "migration_head": evidence.migration_head,
        "tenant_public_ref": evidence.tenant_public_ref,
        "tenant_digest": evidence.tenant_digest,
        "namespace": evidence.namespace,
        "kubernetes_refs": dict(evidence.kubernetes_refs),
        "database_schema_ref": evidence.database_schema_ref,
        "redis_namespace_digest": evidence.redis_namespace_digest,
        "redis_acl_proof_digest": evidence.redis_acl_proof_digest,
        "extension_artifact_digest": evidence.extension_artifact_digest,
        "extension_configuration_digest": evidence.extension_configuration_digest,
        "capability_manifest_digest": evidence.capability_manifest_digest,
        "topology_digest": evidence.topology_registrations[0].topology_fingerprint.digest,
        "scope": evidence.SCOPE,
    }
    if any(subjects[name] != getattr(expected, name) for name in subjects):
        raise QualificationVerificationError("subject_mismatch")
    if tuple(item.scenario_id for item in evidence.scenarios) != tuple(expected.required_scenarios):
        raise QualificationVerificationError("scenario_mismatch")
    checked_at = _aware(now or datetime.now(UTC), name="verification time")
    age = checked_at - evidence.completed_at
    if age < timedelta(minutes=-5) or age > timedelta(seconds=expected.max_age_seconds):
        raise QualificationVerificationError("artifact_stale")
    return QualificationVerificationResult(
        qualification_id=evidence.qualification_id,
        scope=evidence.SCOPE,
        artifact_digest=actual_digest,
    )


__all__ = [
    "KubernetesMultiGatewayQualificationEvidenceV1",
    "MULTI_GATEWAY_QUALIFICATION_API_VERSION",
    "MULTI_GATEWAY_QUALIFICATION_SCENARIOS",
    "MULTI_GATEWAY_SCENARIO_RESULTS",
    "MultiGatewayQualificationDriver",
    "MultiGatewayQualificationExpectationV1",
    "MultiGatewayQualificationSubjectsV1",
    "MultiGatewayScenarioObservationV1",
    "MultiGatewayScenarioEvidenceV1",
    "run_multi_gateway_qualification",
    "verify_multi_gateway_qualification_evidence",
]
