"""Strict offline qualification evidence parsing and subject verification.

This module deliberately uses only the Python standard library.  The live
Kubernetes qualification harness produces this schema, while release tooling
uses :func:`verify_qualification_evidence` without importing cluster clients.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import ClassVar, Literal

QUALIFICATION_SCENARIOS = (
    "accepted_before_client_response",
    "accepted_before_worker_start",
    "active_execution",
    "terminal_before_lifecycle_commit",
    "graceful_rollout_termination",
    "forced_kill_after_graceful_deadline",
)
QUALIFICATION_EVIDENCE_API_VERSION = "deerflow.kubernetes-qualification/v1"
QUALIFICATION_EVIDENCE_KIND = "kubernetes.qualification.evidence"
QUALIFICATION_SCOPE = "durable_one_replica_pod_recovery"
QUALIFICATION_VERIFICATION_API_VERSION = "deerflow.qualification-verification/v1"
MAX_QUALIFICATION_EVIDENCE_BYTES = 64 * 1024

_SAFE_NAMESPACE = re.compile(r"[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?\Z")
_IMAGE_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
_SAFE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}\Z")
_RFC3339 = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
    r"(?:\.[0-9]{1,9})?(?:Z|[+-][0-9]{2}:[0-9]{2})\Z"
)


def _expected_worker_attachments(scenario: str) -> int:
    return 0 if scenario == "accepted_before_worker_start" else 1


def _execution_counts_are_valid(
    scenario: str,
    graph_starts: int,
    model_starts: int,
) -> bool:
    if scenario == "accepted_before_worker_start":
        return graph_starts == 0 and model_starts == 0
    if scenario == "accepted_before_client_response":
        return graph_starts <= 1 and model_starts <= 1
    return graph_starts == 1 and model_starts == 1


def _expected_termination_mode(
    scenario: str,
) -> Literal["abrupt", "graceful", "forced_deadline"]:
    if scenario == "graceful_rollout_termination":
        return "graceful"
    if scenario == "forced_kill_after_graceful_deadline":
        return "forced_deadline"
    return "abrupt"


def _bounded_safe(value: str, *, name: str, limit: int = 256) -> str:
    if not isinstance(value, str) or not value or len(value.encode("utf-8")) > limit:
        raise ValueError(f"{name} must be a bounded non-empty string")
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        raise ValueError(f"{name} must not contain control characters")
    return value


def _canonical_json_bytes(value: object) -> bytes:
    return (
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
        + b"\n"
    )


def _reject_duplicate_object_fields(
    pairs: list[tuple[str, object]],
) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("qualification evidence contains duplicate fields")
        result[key] = value
    return result


def _reject_non_finite_number(_value: str) -> object:
    raise ValueError("qualification evidence contains non-finite numbers")


@dataclass(frozen=True)
class ScenarioEvidence:
    """Bounded passing outcome for one required real-pod fault point."""

    name: str
    run_id: str
    worker_attachments: int
    graph_starts: int
    model_starts: int
    terminal_status: str
    termination_mode: Literal["abrupt", "graceful", "forced_deadline"]
    old_pod_termination_millis: int
    barrier_reached: bool = True
    status: Literal["passed"] = "passed"

    def __post_init__(self) -> None:
        if _SAFE_ID.fullmatch(self.name) is None:
            raise ValueError("scenario name is invalid")
        _bounded_safe(self.run_id, name="scenario run_id", limit=128)
        counters = (
            self.worker_attachments,
            self.graph_starts,
            self.model_starts,
            self.old_pod_termination_millis,
        )
        if any(type(value) is not int for value in counters):
            raise ValueError("scenario counters must be integers")
        if self.worker_attachments < 0 or self.graph_starts < 0 or self.model_starts < 0:
            raise ValueError("scenario start counters must be non-negative")
        if self.termination_mode not in {"abrupt", "graceful", "forced_deadline"}:
            raise ValueError("scenario termination mode is invalid")
        if not 0 <= self.old_pod_termination_millis <= 300_000:
            raise ValueError("scenario pod termination duration is invalid")
        if type(self.barrier_reached) is not bool or not self.barrier_reached:
            raise ValueError("completed scenario evidence must reach its barrier")
        if self.status != "passed":
            raise ValueError("completed scenario evidence must be passed")
        if self.worker_attachments != _expected_worker_attachments(self.name):
            raise ValueError("scenario worker attachment evidence is inconsistent")
        if not _execution_counts_are_valid(
            self.name,
            self.graph_starts,
            self.model_starts,
        ):
            raise ValueError("scenario execution evidence is inconsistent")
        if self.termination_mode != _expected_termination_mode(self.name):
            raise ValueError("scenario termination evidence is inconsistent")
        _bounded_safe(self.terminal_status, name="terminal status", limit=32)

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "status": self.status,
            "barrier_reached": self.barrier_reached,
            "run_id": self.run_id,
            "terminal_status": self.terminal_status,
            "termination_mode": self.termination_mode,
            "old_pod_termination_millis": self.old_pod_termination_millis,
            "worker_attachments": self.worker_attachments,
            "graph_starts": self.graph_starts,
            "model_starts": self.model_starts,
        }

    @classmethod
    def from_dict(cls, value: object) -> ScenarioEvidence:
        fields = {
            "name",
            "status",
            "barrier_reached",
            "run_id",
            "terminal_status",
            "termination_mode",
            "old_pod_termination_millis",
            "worker_attachments",
            "graph_starts",
            "model_starts",
        }
        if not isinstance(value, dict) or set(value) != fields:
            raise ValueError("scenario evidence fields are invalid")
        try:
            return cls(
                name=value["name"],
                status=value["status"],
                barrier_reached=value["barrier_reached"],
                run_id=value["run_id"],
                terminal_status=value["terminal_status"],
                termination_mode=value["termination_mode"],
                old_pod_termination_millis=value["old_pod_termination_millis"],
                worker_attachments=value["worker_attachments"],
                graph_starts=value["graph_starts"],
                model_starts=value["model_starts"],
            )
        except (TypeError, ValueError) as exc:
            raise ValueError("scenario evidence values are invalid") from exc


@dataclass(frozen=True)
class StoreContinuityEvidence:
    """Exact shared-store identity retained across Gateway replacements."""

    component: Literal["postgres", "redis"]
    pod_uid: str
    volume_uid: str
    image_id: str
    version: str

    def __post_init__(self) -> None:
        if self.component not in {"postgres", "redis"}:
            raise ValueError("store component is invalid")
        for field_name, limit in (
            ("pod_uid", 128),
            ("volume_uid", 128),
            ("image_id", 576),
            ("version", 256),
        ):
            _bounded_safe(getattr(self, field_name), name=field_name, limit=limit)

    def to_dict(self) -> dict[str, str]:
        return {
            "component": self.component,
            "pod_uid": self.pod_uid,
            "volume_uid": self.volume_uid,
            "image_id": self.image_id,
            "version": self.version,
        }

    @classmethod
    def from_dict(cls, value: object) -> StoreContinuityEvidence:
        fields = {"component", "pod_uid", "volume_uid", "image_id", "version"}
        if not isinstance(value, dict) or set(value) != fields:
            raise ValueError("store continuity evidence fields are invalid")
        try:
            return cls(**value)
        except (TypeError, ValueError) as exc:
            raise ValueError("store continuity evidence values are invalid") from exc


@dataclass(frozen=True)
class KubernetesQualificationEvidence:
    """Canonical v1 proof for one exact image/chart/configuration run."""

    REQUIRED_SCENARIOS: ClassVar[tuple[str, ...]] = QUALIFICATION_SCENARIOS
    API_VERSION: ClassVar[str] = QUALIFICATION_EVIDENCE_API_VERSION
    SCOPE: ClassVar[str] = QUALIFICATION_SCOPE

    qualification_id: str
    image_reference: str
    image_digest: str
    chart_version: str
    chart_digest: str
    configuration_digest: str
    migration_head: str
    stores: tuple[StoreContinuityEvidence, ...]
    kubernetes_server_version: str
    cluster_context: str
    cluster_driver: str | None
    namespace: str
    completed_at: datetime
    scenarios: tuple[ScenarioEvidence, ...]

    def __post_init__(self) -> None:
        if _SAFE_ID.fullmatch(self.qualification_id) is None:
            raise ValueError("qualification_id is invalid")
        _bounded_safe(self.image_reference, name="image_reference", limit=576)
        if _IMAGE_DIGEST.fullmatch(self.image_digest) is None:
            raise ValueError("image_digest is invalid")
        if not self.image_reference.endswith("@" + self.image_digest):
            raise ValueError("image_reference must be pinned to image_digest")
        for field_name in (
            "chart_version",
            "migration_head",
            "kubernetes_server_version",
            "cluster_context",
        ):
            _bounded_safe(getattr(self, field_name), name=field_name, limit=256)
        for digest_name in ("chart_digest", "configuration_digest"):
            if _IMAGE_DIGEST.fullmatch(getattr(self, digest_name)) is None:
                raise ValueError(f"{digest_name} is invalid")
        if self.cluster_driver is not None and _SAFE_ID.fullmatch(self.cluster_driver) is None:
            raise ValueError("cluster_driver is invalid")
        stores = tuple(self.stores)
        if tuple(item.component for item in stores) != ("postgres", "redis"):
            raise ValueError("store continuity evidence is incomplete or out of order")
        if _SAFE_NAMESPACE.fullmatch(self.namespace) is None:
            raise ValueError("namespace is invalid")
        if not self.namespace.startswith("hartmesh-qualification-"):
            raise ValueError("evidence namespace is not a qualification namespace")
        if self.completed_at.tzinfo is None or self.completed_at.utcoffset() is None:
            raise ValueError("completed_at must be timezone-aware")
        scenarios = tuple(self.scenarios)
        if tuple(item.name for item in scenarios) != self.REQUIRED_SCENARIOS:
            raise ValueError("scenario coverage is incomplete or out of order")
        object.__setattr__(self, "scenarios", scenarios)
        object.__setattr__(self, "stores", stores)

    def to_dict(self) -> dict[str, object]:
        return {
            "api_version": self.API_VERSION,
            "kind": QUALIFICATION_EVIDENCE_KIND,
            "status": "passed",
            "scope": self.SCOPE,
            "qualification_id": self.qualification_id,
            "artifact": {
                "image_reference": self.image_reference,
                "image_digest": self.image_digest,
                "chart_version": self.chart_version,
                "chart_digest": self.chart_digest,
                "configuration_digest": self.configuration_digest,
                "migration_head": self.migration_head,
            },
            "environment": {
                "stores": [store.to_dict() for store in self.stores],
                "kubernetes_server_version": self.kubernetes_server_version,
                "cluster_context": self.cluster_context,
                "cluster_driver": self.cluster_driver,
                "namespace": self.namespace,
            },
            "completed_at": self.completed_at.astimezone(UTC).isoformat().replace("+00:00", "Z"),
            "scenarios": [scenario.to_dict() for scenario in self.scenarios],
        }

    def canonical_bytes(self) -> bytes:
        """Return the sole byte representation accepted for v1 digests."""

        payload = _canonical_json_bytes(self.to_dict())
        if len(payload) > MAX_QUALIFICATION_EVIDENCE_BYTES:
            raise ValueError("qualification evidence exceeds 64 KiB")
        return payload

    @classmethod
    def from_dict(cls, value: object) -> KubernetesQualificationEvidence:
        fields = {
            "api_version",
            "kind",
            "status",
            "scope",
            "qualification_id",
            "artifact",
            "environment",
            "completed_at",
            "scenarios",
        }
        if not isinstance(value, dict):
            raise ValueError("evidence must be an object")
        if set(value) - fields:
            raise ValueError("unknown evidence fields")
        if set(value) != fields:
            raise ValueError("evidence fields are incomplete")
        if value["api_version"] != cls.API_VERSION or value["kind"] != QUALIFICATION_EVIDENCE_KIND or value["status"] != "passed" or value["scope"] != cls.SCOPE:
            raise ValueError("evidence discriminator is invalid")
        artifact = value["artifact"]
        environment = value["environment"]
        artifact_fields = {
            "image_reference",
            "image_digest",
            "chart_version",
            "chart_digest",
            "configuration_digest",
            "migration_head",
        }
        environment_fields = {
            "stores",
            "kubernetes_server_version",
            "cluster_context",
            "cluster_driver",
            "namespace",
        }
        if not isinstance(artifact, dict) or set(artifact) != artifact_fields:
            raise ValueError("artifact evidence fields are invalid")
        if not isinstance(environment, dict) or set(environment) != environment_fields:
            raise ValueError("environment evidence fields are invalid")
        stores = environment["stores"]
        scenarios = value["scenarios"]
        if not isinstance(stores, list) or len(stores) != 2:
            raise ValueError("store continuity evidence must contain two items")
        if not isinstance(scenarios, list) or len(scenarios) > 32:
            raise ValueError("scenario evidence must be a bounded list")
        completed_at_raw = value["completed_at"]
        if not isinstance(completed_at_raw, str) or len(completed_at_raw) > 64 or _RFC3339.fullmatch(completed_at_raw) is None:
            raise ValueError("completed_at must be a bounded RFC3339 timestamp")
        try:
            completed_at = datetime.fromisoformat(completed_at_raw.replace("Z", "+00:00"))
            return cls(
                qualification_id=value["qualification_id"],
                image_reference=artifact["image_reference"],
                image_digest=artifact["image_digest"],
                chart_version=artifact["chart_version"],
                chart_digest=artifact["chart_digest"],
                configuration_digest=artifact["configuration_digest"],
                migration_head=artifact["migration_head"],
                stores=tuple(StoreContinuityEvidence.from_dict(item) for item in stores),
                kubernetes_server_version=environment["kubernetes_server_version"],
                cluster_context=environment["cluster_context"],
                cluster_driver=environment["cluster_driver"],
                namespace=environment["namespace"],
                completed_at=completed_at,
                scenarios=tuple(ScenarioEvidence.from_dict(item) for item in scenarios),
            )
        except (AttributeError, TypeError, ValueError) as exc:
            if isinstance(exc, ValueError) and "scenario coverage" in str(exc):
                raise
            raise ValueError("evidence values are invalid") from exc

    @classmethod
    def from_bytes(cls, payload: bytes) -> KubernetesQualificationEvidence:
        """Parse one bounded canonical artifact and reject alternate encodings."""

        if not isinstance(payload, bytes) or not payload:
            raise ValueError("qualification evidence must be non-empty bytes")
        if len(payload) > MAX_QUALIFICATION_EVIDENCE_BYTES:
            raise ValueError("qualification evidence exceeds 64 KiB")
        try:
            value = json.loads(
                payload.decode("utf-8"),
                object_pairs_hook=_reject_duplicate_object_fields,
                parse_constant=_reject_non_finite_number,
            )
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
            raise ValueError("qualification evidence must be valid UTF-8 JSON") from exc
        evidence = cls.from_dict(value)
        if payload != evidence.canonical_bytes():
            raise ValueError("qualification evidence is not canonical v1 JSON")
        return evidence

    def write(self, path: Path) -> None:
        """Atomically write canonical bounded evidence."""

        write_qualification_evidence(path, self.canonical_bytes())


@dataclass(frozen=True)
class KubernetesQualificationFailureEvidence:
    """Safe non-passing artifact retained when a live scenario fails."""

    qualification_id: str
    image_digest: str
    chart_version: str
    chart_digest: str
    configuration_digest: str
    cluster_context: str
    namespace: str
    completed_at: datetime
    completed_scenarios: tuple[str, ...]
    failure_code: str

    def __post_init__(self) -> None:
        if _SAFE_ID.fullmatch(self.qualification_id) is None:
            raise ValueError("qualification_id is invalid")
        for digest in (
            self.image_digest,
            self.chart_digest,
            self.configuration_digest,
        ):
            if _IMAGE_DIGEST.fullmatch(digest) is None:
                raise ValueError("failure evidence digest is invalid")
        _bounded_safe(self.chart_version, name="chart_version", limit=256)
        _bounded_safe(self.cluster_context, name="cluster_context", limit=256)
        if _SAFE_NAMESPACE.fullmatch(self.namespace) is None:
            raise ValueError("namespace is invalid")
        if self.completed_at.tzinfo is None or self.completed_at.utcoffset() is None:
            raise ValueError("completed_at must be timezone-aware")
        completed = tuple(self.completed_scenarios)
        if completed != QUALIFICATION_SCENARIOS[: len(completed)]:
            raise ValueError("completed scenarios must be an ordered required prefix")
        if _SAFE_ID.fullmatch(self.failure_code) is None:
            raise ValueError("failure_code is invalid")
        object.__setattr__(self, "completed_scenarios", completed)

    def to_dict(self) -> dict[str, object]:
        return {
            "api_version": QUALIFICATION_EVIDENCE_API_VERSION,
            "kind": QUALIFICATION_EVIDENCE_KIND,
            "status": "failed",
            "scope": QUALIFICATION_SCOPE,
            "qualification_id": self.qualification_id,
            "image_digest": self.image_digest,
            "chart_version": self.chart_version,
            "chart_digest": self.chart_digest,
            "configuration_digest": self.configuration_digest,
            "cluster_context": self.cluster_context,
            "namespace": self.namespace,
            "completed_at": self.completed_at.astimezone(UTC).isoformat().replace("+00:00", "Z"),
            "completed_scenarios": list(self.completed_scenarios),
            "failure_code": self.failure_code,
        }

    def write(self, path: Path) -> None:
        """Atomically write bounded failure evidence that cannot verify as passed."""

        write_qualification_evidence(path, _canonical_json_bytes(self.to_dict()))


def write_qualification_evidence(path: Path, payload: bytes) -> None:
    """Atomically write already-canonical bounded evidence bytes."""

    if not isinstance(payload, bytes) or not payload:
        raise ValueError("qualification evidence must be non-empty bytes")
    if len(payload) > MAX_QUALIFICATION_EVIDENCE_BYTES:
        raise ValueError("qualification evidence exceeds 64 KiB")
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(
        dir=path.parent,
        prefix=f".{path.name}.",
        delete=False,
    ) as temporary:
        temporary.write(payload)
        temporary.flush()
        os.fsync(temporary.fileno())
        temporary_path = Path(temporary.name)
    os.replace(temporary_path, path)


def qualification_evidence_digest(payload: bytes) -> str:
    """Return the SHA-256 reference for canonical evidence bytes."""

    if not isinstance(payload, bytes) or not payload:
        raise ValueError("qualification evidence must be non-empty bytes")
    if len(payload) > MAX_QUALIFICATION_EVIDENCE_BYTES:
        raise ValueError("qualification evidence exceeds 64 KiB")
    return "sha256:" + hashlib.sha256(payload).hexdigest()


@dataclass(frozen=True)
class QualificationEvidenceExpectation:
    """Independent exact subjects required by an offline verifier."""

    qualification_id: str
    image_digest: str
    chart_version: str
    chart_digest: str
    configuration_digest: str
    migration_head: str
    scope: str
    namespace: str
    required_scenarios: tuple[str, ...]

    def __post_init__(self) -> None:
        if _SAFE_ID.fullmatch(self.qualification_id) is None:
            raise ValueError("expected qualification_id is invalid")
        for digest_name in (
            "image_digest",
            "chart_digest",
            "configuration_digest",
        ):
            if _IMAGE_DIGEST.fullmatch(getattr(self, digest_name)) is None:
                raise ValueError(f"expected {digest_name} is invalid")
        for field_name in ("chart_version", "migration_head"):
            _bounded_safe(getattr(self, field_name), name=f"expected {field_name}", limit=256)
        if _SAFE_ID.fullmatch(self.scope) is None:
            raise ValueError("expected scope is invalid")
        if _SAFE_NAMESPACE.fullmatch(self.namespace) is None:
            raise ValueError("expected namespace is invalid")
        scenarios = tuple(self.required_scenarios)
        if not scenarios or len(scenarios) > 32:
            raise ValueError("expected scenarios must be a bounded non-empty tuple")
        if len(scenarios) != len(set(scenarios)):
            raise ValueError("expected scenarios must not contain duplicates")
        if any(_SAFE_ID.fullmatch(item) is None for item in scenarios):
            raise ValueError("expected scenario name is invalid")
        object.__setattr__(self, "required_scenarios", scenarios)


class QualificationVerificationError(ValueError):
    """Safe verifier failure carrying one stable machine-readable code."""

    def __init__(self, code: str) -> None:
        if _SAFE_ID.fullmatch(code) is None:
            raise ValueError("verification error code is invalid")
        self.code = code
        super().__init__(code)


@dataclass(frozen=True)
class QualificationVerificationResult:
    """Successful exact-subject verification result for automation."""

    qualification_id: str
    scope: str
    artifact_digest: str

    def to_dict(self) -> dict[str, str]:
        return {
            "api_version": QUALIFICATION_VERIFICATION_API_VERSION,
            "kind": "qualification.verification",
            "status": "verified",
            "trust": "external_evidence_verified",
            "qualification_id": self.qualification_id,
            "scope": self.scope,
            "artifact_digest": self.artifact_digest,
        }


def verify_qualification_evidence(
    artifact: bytes,
    *,
    declared_digest: str,
    expected: QualificationEvidenceExpectation,
) -> QualificationVerificationResult:
    """Verify digest before parsing, then require exact independent subjects."""

    try:
        actual_digest = qualification_evidence_digest(artifact)
    except (TypeError, ValueError) as exc:
        raise QualificationVerificationError("artifact_unreadable") from exc
    if not isinstance(declared_digest, str) or _IMAGE_DIGEST.fullmatch(declared_digest) is None:
        raise QualificationVerificationError("declared_digest_invalid")
    if actual_digest != declared_digest:
        raise QualificationVerificationError("artifact_digest_mismatch")
    try:
        evidence = KubernetesQualificationEvidence.from_bytes(artifact)
    except (TypeError, ValueError) as exc:
        raise QualificationVerificationError("artifact_invalid") from exc
    subjects = {
        "qualification_id": evidence.qualification_id,
        "image_digest": evidence.image_digest,
        "chart_version": evidence.chart_version,
        "chart_digest": evidence.chart_digest,
        "configuration_digest": evidence.configuration_digest,
        "migration_head": evidence.migration_head,
        "scope": evidence.SCOPE,
        "namespace": evidence.namespace,
    }
    if any(subjects[name] != getattr(expected, name) for name in subjects):
        raise QualificationVerificationError("subject_mismatch")
    if tuple(item.name for item in evidence.scenarios) != expected.required_scenarios:
        raise QualificationVerificationError("scenario_mismatch")
    return QualificationVerificationResult(
        qualification_id=evidence.qualification_id,
        scope=evidence.SCOPE,
        artifact_digest=actual_digest,
    )


__all__ = [
    "KubernetesQualificationEvidence",
    "KubernetesQualificationFailureEvidence",
    "MAX_QUALIFICATION_EVIDENCE_BYTES",
    "QUALIFICATION_EVIDENCE_API_VERSION",
    "QUALIFICATION_SCENARIOS",
    "QUALIFICATION_SCOPE",
    "QUALIFICATION_VERIFICATION_API_VERSION",
    "QualificationEvidenceExpectation",
    "QualificationVerificationError",
    "QualificationVerificationResult",
    "ScenarioEvidence",
    "StoreContinuityEvidence",
    "qualification_evidence_digest",
    "verify_qualification_evidence",
    "write_qualification_evidence",
]
