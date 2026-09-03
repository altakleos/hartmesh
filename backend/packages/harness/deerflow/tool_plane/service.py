"""State machine and ports for governed tool-plane revisions.

The service is intentionally the only component allowed to coordinate revision
state with projection writes.  SQL and the filesystem cannot share a
transaction; ``prepared`` is therefore durable before projection and every
failure becomes an explicit recovery state.
"""

from __future__ import annotations

import asyncio
import copy
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from itertools import combinations
from typing import Any, Literal, Protocol

from deerflow_extension_api import (
    VerifiedActorContextV1,
    authority_categories_v1,
    canonicalize_authority_v1,
    effective_authority_digest_v1,
)

from deerflow.tool_plane.contracts import (
    EMPTY_OVERLAY_MARKER_V1,
    DeploymentToolPlaneRevisionV1,
    EffectiveToolPlaneRevisionV1,
    ToolPlaneRevisionError,
    ToolPlaneRevisionScopeV1,
    UserToolPlaneOverlayV1,
    canonical_tool_plane_digest,
    canonicalize_deployment_candidate,
    canonicalize_user_overlay_candidate,
)

RevisionState = Literal[
    "bootstrap_required",
    "staged",
    "validating",
    "validated",
    "rejected",
    "prepared",
    "promoted",
    "superseded",
    "recovery_required",
]

_DEFAULT_AUTHORITY_UNIVERSE = (
    "tool_plane:admin",
    "tool_plane:mutate",
    "tool_plane:read",
    "tool_plane:reconcile",
)


def _now() -> datetime:
    return datetime.now(UTC)


def user_scope_reference_for_subject(
    *,
    tenant_digest: str,
    subject_kind: str,
    subject_id: str,
) -> str:
    """Derive an opaque overlay reference from server-owned subject facts."""

    digest = canonical_tool_plane_digest(
        {
            "version": 1,
            "tenant_digest": tenant_digest,
            "subject_kind": subject_kind,
            "subject_id": subject_id,
        }
    )
    return f"user-{digest[:32]}"


def user_scope_reference(actor: VerifiedActorContextV1) -> str:
    """Derive an opaque stable overlay reference from trusted actor facts."""

    if not isinstance(actor, VerifiedActorContextV1):
        raise TypeError("actor must be VerifiedActorContextV1")
    subject = actor.identity.effective_subject
    return user_scope_reference_for_subject(
        tenant_digest=actor.tenant.digest,
        subject_kind=subject.kind,
        subject_id=subject.subject_id,
    )


@dataclass(frozen=True, slots=True)
class ToolPlaneUserInventorySnapshot:
    """Bounded server-owned subject inventory; raw IDs never enter safe evidence."""

    subject_ids: tuple[str, ...]
    generation_digest: str = field(init=False)

    def __post_init__(self) -> None:
        subject_ids = tuple(sorted(set(self.subject_ids)))
        if len(subject_ids) > 10_000 or any(not isinstance(subject_id, str) or not subject_id or len(subject_id.encode("utf-8")) > 512 for subject_id in subject_ids):
            raise ToolPlaneRevisionError("bootstrap_inventory_changed")
        object.__setattr__(self, "subject_ids", subject_ids)
        object.__setattr__(
            self,
            "generation_digest",
            canonical_tool_plane_digest({"version": 1, "subject_ids": list(subject_ids)}),
        )


class ToolPlaneUserInventory(Protocol):
    async def snapshot(self) -> ToolPlaneUserInventorySnapshot: ...


class StaticToolPlaneUserInventory:
    """Small deterministic inventory adapter for local mode and tests."""

    def __init__(self, subject_ids: tuple[str, ...] = ()) -> None:
        self._subject_ids = subject_ids

    async def snapshot(self) -> ToolPlaneUserInventorySnapshot:
        return ToolPlaneUserInventorySnapshot(self._subject_ids)


class RegisteredToolPlaneUserInventory:
    """Read the protected user-skill storage index."""

    async def snapshot(self) -> ToolPlaneUserInventorySnapshot:
        from deerflow.skills.storage.user_inventory import (
            list_registered_user_skill_subjects,
        )

        try:
            subject_ids = await asyncio.to_thread(list_registered_user_skill_subjects)
        except (OSError, ValueError) as exc:
            raise ToolPlaneRevisionError("bootstrap_inventory_changed") from exc
        return ToolPlaneUserInventorySnapshot(subject_ids)


class CompositeToolPlaneUserInventory:
    """Union authoritative inventory adapters without exposing source details."""

    def __init__(self, *inventories: ToolPlaneUserInventory) -> None:
        self._inventories = tuple(inventories)

    async def snapshot(self) -> ToolPlaneUserInventorySnapshot:
        subject_ids: set[str] = set()
        for inventory in self._inventories:
            subject_ids.update((await inventory.snapshot()).subject_ids)
        return ToolPlaneUserInventorySnapshot(tuple(subject_ids))


@dataclass(frozen=True, slots=True)
class ScopedStageRevisionRequest:
    scope: ToolPlaneRevisionScopeV1
    candidate: Mapping[str, object]

    def __post_init__(self) -> None:
        if not isinstance(self.scope, ToolPlaneRevisionScopeV1):
            raise TypeError("scope must be ToolPlaneRevisionScopeV1")
        if not isinstance(self.candidate, Mapping):
            raise TypeError("candidate must be a mapping")
        object.__setattr__(self, "candidate", copy.deepcopy(dict(self.candidate)))


@dataclass(frozen=True, slots=True)
class RevisionEventV1:
    event_id: str
    revision_id: str
    state: RevisionState
    actor_digest: str
    occurred_at: datetime
    safe_details: Mapping[str, object] = field(default_factory=dict)

    def to_json(self) -> dict[str, object]:
        return {
            "version": 1,
            "event_id": self.event_id,
            "revision_id": self.revision_id,
            "state": self.state,
            "actor_digest": self.actor_digest,
            "occurred_at": self.occurred_at.isoformat(),
            "safe_details": dict(self.safe_details),
        }


@dataclass(frozen=True, slots=True)
class ToolPlaneValidationFindingV1:
    code: str
    severity: Literal["warning", "error"]
    location: str | None = None

    def to_json(self) -> dict[str, object]:
        return {
            "code": self.code,
            "severity": self.severity,
            "location": self.location,
        }


@dataclass(frozen=True, slots=True)
class ToolPlaneValidationReportV1:
    revision_digest: str
    content_digest: str
    validator_policy_digest: str
    validator_versions: Mapping[str, str]
    result: Literal["passed", "failed", "unqualified"]
    findings: tuple[ToolPlaneValidationFindingV1, ...] = ()
    validated_at: datetime = field(default_factory=_now)
    report_digest: str = field(init=False)

    def __post_init__(self) -> None:
        if len(self.findings) > 256:
            raise ValueError("validation reports accept at most 256 findings")
        projection = self._projection()
        if len(str(projection).encode("utf-8")) > 64 * 1024:
            raise ValueError("validation reports are limited to 64 KiB")
        object.__setattr__(self, "report_digest", canonical_tool_plane_digest(projection))

    def _projection(self) -> dict[str, object]:
        return {
            "version": 1,
            "revision_digest": self.revision_digest,
            "content_digest": self.content_digest,
            "validator_policy_digest": self.validator_policy_digest,
            "validator_versions": dict(sorted(self.validator_versions.items())),
            "result": self.result,
            "findings": [finding.to_json() for finding in self.findings],
            "validated_at": self.validated_at.isoformat(),
        }

    def to_json(self) -> dict[str, object]:
        return {**self._projection(), "report_digest": self.report_digest}


@dataclass(frozen=True, slots=True)
class OverlayCompatibilityV1:
    """Immutable proof that one overlay was checked against one base."""

    base_revision_digest: str
    overlay_revision_digest: str
    validator_policy_digest: str
    report: ToolPlaneValidationReportV1
    compatible: bool
    created_at: datetime = field(default_factory=_now)
    attestation_digest: str = field(init=False)

    def __post_init__(self) -> None:
        for value in (
            self.base_revision_digest,
            self.overlay_revision_digest,
            self.validator_policy_digest,
        ):
            if len(value) != 64:
                raise ValueError("compatibility digests must be SHA-256 digests")
        if self.report.revision_digest != self.overlay_revision_digest:
            raise ValueError("compatibility report must bind the overlay revision")
        if self.report.validator_policy_digest != self.validator_policy_digest:
            raise ValueError("compatibility report must bind the validator policy")
        if self.compatible != (self.report.result == "passed"):
            raise ValueError("compatibility result must match its validation report")
        object.__setattr__(
            self,
            "attestation_digest",
            canonical_tool_plane_digest(self._projection()),
        )

    @property
    def key(self) -> tuple[str, str, str]:
        return (
            self.base_revision_digest,
            self.overlay_revision_digest,
            self.validator_policy_digest,
        )

    def _projection(self) -> dict[str, object]:
        return {
            "version": 1,
            "base_revision_digest": self.base_revision_digest,
            "overlay_revision_digest": self.overlay_revision_digest,
            "validator_policy_digest": self.validator_policy_digest,
            "report_digest": self.report.report_digest,
            "compatible": self.compatible,
            "created_at": self.created_at.isoformat(),
        }

    def to_json(self) -> dict[str, object]:
        return {
            **self._projection(),
            "attestation_digest": self.attestation_digest,
            "report": self.report.to_json(),
        }


@dataclass(frozen=True, slots=True)
class ToolPlaneRevisionRecord:
    revision_id: str
    revision_digest: str
    tenant_ref: str
    tenant_digest: str
    scope: ToolPlaneRevisionScopeV1
    content_digest: str
    manifest: Mapping[str, object]
    parent_revision_digest: str | None
    base_revision_digest: str | None
    state: RevisionState
    staging_actor_digest: str
    staged_at: datetime
    validation_report: ToolPlaneValidationReportV1 | None = None
    promotion_actor_digest: str | None = None
    previous_revision_id: str | None = None
    desired_projection_digest: str | None = None
    observed_projection_digest: str | None = None
    promoted_at: datetime | None = None
    rollback_source_revision_id: str | None = None
    bootstrap_inventory_digest: str | None = None
    storage_subject_id: str | None = field(default=None, repr=False)
    bootstrap_overlay_revision_ids: tuple[str, ...] = field(default=(), repr=False)
    bootstrap_inventory_subject_ids: tuple[str, ...] = field(default=(), repr=False)

    def to_safe_json(self, *, include_manifest: bool = True) -> dict[str, object]:
        result: dict[str, object] = {
            "version": 1,
            "revision_id": self.revision_id,
            "revision_digest": self.revision_digest,
            "tenant_ref": self.tenant_ref,
            "scope": self.scope.to_json(),
            "content_digest": self.content_digest,
            "parent_revision_digest": self.parent_revision_digest,
            "base_revision_digest": self.base_revision_digest,
            "state": self.state,
            "staging_actor_digest": self.staging_actor_digest,
            "staged_at": self.staged_at.isoformat(),
            "validation_report": (None if self.validation_report is None else self.validation_report.to_json()),
            "promotion_actor_digest": self.promotion_actor_digest,
            "previous_revision_id": self.previous_revision_id,
            "desired_projection_digest": self.desired_projection_digest,
            "observed_projection_digest": self.observed_projection_digest,
            "promoted_at": None if self.promoted_at is None else self.promoted_at.isoformat(),
            "rollback_source_revision_id": self.rollback_source_revision_id,
            "bootstrap_inventory_digest": self.bootstrap_inventory_digest,
        }
        if include_manifest:
            result["manifest"] = copy.deepcopy(dict(self.manifest))
        return result


@dataclass(frozen=True, slots=True)
class StagedRevision:
    revision_id: str
    revision_digest: str
    content_digest: str
    scope: ToolPlaneRevisionScopeV1
    state: RevisionState
    staged_at: datetime


@dataclass(frozen=True, slots=True)
class BootstrapStagingResult:
    """Base plus every nonempty overlay staged at one bootstrap high-water."""

    base_revision: StagedRevision
    overlay_revisions: tuple[StagedRevision, ...]
    inventory_digest: str

    @property
    def revision_id(self) -> str:
        return self.base_revision.revision_id

    @property
    def revision_digest(self) -> str:
        return self.base_revision.revision_digest

    @property
    def content_digest(self) -> str:
        return self.base_revision.content_digest

    @property
    def scope(self) -> ToolPlaneRevisionScopeV1:
        return self.base_revision.scope

    @property
    def state(self) -> RevisionState:
        return self.base_revision.state

    @property
    def staged_at(self) -> datetime:
        return self.base_revision.staged_at


@dataclass(frozen=True, slots=True)
class PromotionResult:
    revision_id: str
    revision_digest: str
    state: RevisionState
    actor_digest: str
    previous_revision_id: str | None
    desired_projection_digest: str
    observed_projection_digest: str
    promoted_at: datetime


@dataclass(frozen=True, slots=True)
class ToolPlaneStatus:
    scope: ToolPlaneRevisionScopeV1
    governance_state: Literal[
        "bootstrap_required",
        "governed",
        "unmanaged",
        "recovery_required",
        "immutable",
    ]
    active_revision_id: str | None
    active_revision_digest: str | None
    generation: int
    projection_digest: str | None
    drift: bool

    def to_json(self) -> dict[str, object]:
        return {
            "version": 1,
            "scope": self.scope.to_json(),
            "governance_state": self.governance_state,
            "active_revision_id": self.active_revision_id,
            "active_revision_digest": self.active_revision_digest,
            "generation": self.generation,
            "projection_digest": self.projection_digest,
            "drift": self.drift,
        }


class ToolPlaneValidator(Protocol):
    @property
    def policy_digest(self) -> str: ...

    async def validate(
        self,
        revision: ToolPlaneRevisionRecord,
    ) -> ToolPlaneValidationReportV1: ...

    async def validate_compatibility(
        self,
        *,
        base: ToolPlaneRevisionRecord,
        overlay: ToolPlaneRevisionRecord,
    ) -> ToolPlaneValidationReportV1: ...


class ToolPlaneProjection(Protocol):
    async def project(
        self,
        scope: ToolPlaneRevisionScopeV1,
        manifest: Mapping[str, object],
        *,
        desired_digest: str,
    ) -> str: ...

    async def observed_digest(
        self,
        scope: ToolPlaneRevisionScopeV1,
    ) -> str | None: ...


class DeterministicToolPlaneValidator:
    """Built-in deterministic structural validator.

    Archive scanners and review adapters can wrap this port and add findings;
    this implementation establishes exact digest/policy binding and is also
    useful for local development and unit tests.
    """

    def __init__(self, *, policy_digest: str) -> None:
        if len(policy_digest) != 64:
            raise ValueError("policy_digest must be a SHA-256 digest")
        self._policy_digest = policy_digest

    @property
    def policy_digest(self) -> str:
        return self._policy_digest

    async def validate(
        self,
        revision: ToolPlaneRevisionRecord,
    ) -> ToolPlaneValidationReportV1:
        manifest_policy = revision.manifest.get("validation_policy_digest")
        findings: tuple[ToolPlaneValidationFindingV1, ...] = ()
        result: Literal["passed", "failed", "unqualified"] = "passed"
        if revision.scope.kind == "deployment_base" and manifest_policy != self._policy_digest:
            findings = (
                ToolPlaneValidationFindingV1(
                    code="validation_policy_changed",
                    severity="error",
                    location="validation_policy_digest",
                ),
            )
            result = "failed"
        return ToolPlaneValidationReportV1(
            revision_digest=revision.revision_digest,
            content_digest=revision.content_digest,
            validator_policy_digest=self._policy_digest,
            validator_versions={
                "canonicalizer": "deerflow-tool-plane/v1",
                "mcp_schema": "extensions-config/v1",
                "skill_review": "structural-only/v1",
            },
            result=result,
            findings=findings,
        )

    async def validate_compatibility(
        self,
        *,
        base: ToolPlaneRevisionRecord,
        overlay: ToolPlaneRevisionRecord,
    ) -> ToolPlaneValidationReportV1:
        del base
        return await self.validate(overlay)


class InMemoryToolPlaneProjection:
    """Process-local projection adapter with deterministic fail injection."""

    def __init__(self) -> None:
        self._digests: dict[str, str] = {}
        self._manifests: dict[str, dict[str, object]] = {}
        self.project_count = 0
        self.fail_next: str | None = None

    async def project(
        self,
        scope: ToolPlaneRevisionScopeV1,
        manifest: Mapping[str, object],
        *,
        desired_digest: str,
    ) -> str:
        self.project_count += 1
        failure = self.fail_next
        self.fail_next = None
        if failure is not None:
            raise ToolPlaneRevisionError(failure)
        self._manifests[scope.key] = copy.deepcopy(dict(manifest))
        self._digests[scope.key] = desired_digest
        return desired_digest

    async def observed_digest(
        self,
        scope: ToolPlaneRevisionScopeV1,
    ) -> str | None:
        return self._digests.get(scope.key)

    def inject_drift(self, scope: ToolPlaneRevisionScopeV1, digest: str) -> None:
        self._digests[scope.key] = digest


class InMemoryToolPlaneRevisionRepository:
    """Bounded process-local repository implementing the SQL repository port."""

    def __init__(self, *, tenant: Any) -> None:
        self.tenant = tenant
        self._records: dict[str, ToolPlaneRevisionRecord] = {}
        self._events: list[RevisionEventV1] = []
        self._active: dict[str, str] = {}
        self._generations: dict[str, int] = {}
        self._overlay_set_generation = 0
        self._bootstrap_required = False
        self._compatibilities: dict[tuple[str, str, str], OverlayCompatibilityV1] = {}
        self._lock = asyncio.Lock()

    @staticmethod
    def _copy_record(record: ToolPlaneRevisionRecord) -> ToolPlaneRevisionRecord:
        # Frozen dataclasses do not recursively freeze their Mapping fields.
        # Repository boundaries therefore copy the complete record so an
        # inspector cannot mutate append-only history through a returned dict.
        return copy.deepcopy(record)

    async def initialize(self, *, existing_projection: bool) -> None:
        async with self._lock:
            if not self._records and existing_projection:
                self._bootstrap_required = True

    async def bootstrap_required(self) -> bool:
        async with self._lock:
            return self._bootstrap_required

    async def clear_bootstrap(self) -> None:
        async with self._lock:
            self._bootstrap_required = False

    def _append_event(
        self,
        record: ToolPlaneRevisionRecord,
        *,
        state: RevisionState,
        actor_digest: str,
        safe_details: Mapping[str, object] | None = None,
    ) -> None:
        self._events.append(
            RevisionEventV1(
                event_id=str(uuid.uuid4()),
                revision_id=record.revision_id,
                state=state,
                actor_digest=actor_digest,
                occurred_at=_now(),
                safe_details=dict(safe_details or {}),
            )
        )

    async def add(self, record: ToolPlaneRevisionRecord) -> None:
        async with self._lock:
            self._add_unlocked(record)

    def _add_unlocked(self, record: ToolPlaneRevisionRecord) -> None:
        if record.revision_id in self._records:
            raise ToolPlaneRevisionError("revision_conflict")
        stored = self._copy_record(record)
        self._records[record.revision_id] = stored
        self._append_event(
            stored,
            state="staged",
            actor_digest=stored.staging_actor_digest,
            safe_details=(
                {
                    "operation": "rollback",
                    "source_revision_id": stored.rollback_source_revision_id,
                }
                if stored.rollback_source_revision_id is not None
                else None
            ),
        )

    async def add_bootstrap(
        self,
        base: ToolPlaneRevisionRecord,
        overlays: tuple[ToolPlaneRevisionRecord, ...],
    ) -> None:
        """Append one complete bootstrap staging batch atomically."""

        async with self._lock:
            records = (base, *overlays)
            if len({record.revision_id for record in records}) != len(records):
                raise ToolPlaneRevisionError("revision_conflict")
            if any(record.revision_id in self._records for record in records):
                raise ToolPlaneRevisionError("revision_conflict")
            for record in records:
                self._add_unlocked(record)

    async def save_compatibility(
        self,
        attestation: OverlayCompatibilityV1,
    ) -> OverlayCompatibilityV1:
        async with self._lock:
            current = self._compatibilities.get(attestation.key)
            if current is not None:
                return copy.deepcopy(current)
            self._compatibilities[attestation.key] = copy.deepcopy(attestation)
            return copy.deepcopy(attestation)

    async def compatibility_attestation(
        self,
        *,
        base_revision_digest: str,
        overlay_revision_digest: str,
        validator_policy_digest: str,
    ) -> OverlayCompatibilityV1 | None:
        async with self._lock:
            result = self._compatibilities.get(
                (
                    base_revision_digest,
                    overlay_revision_digest,
                    validator_policy_digest,
                )
            )
            return None if result is None else copy.deepcopy(result)

    async def get(self, revision_id: str) -> ToolPlaneRevisionRecord | None:
        async with self._lock:
            record = self._records.get(revision_id)
            return None if record is None else self._copy_record(record)

    async def list_scope(
        self,
        scope: ToolPlaneRevisionScopeV1,
        *,
        limit: int = 100,
    ) -> list[ToolPlaneRevisionRecord]:
        if limit < 1 or limit > 100:
            raise ValueError("limit must be between 1 and 100")
        async with self._lock:
            return [self._copy_record(record) for record in reversed(tuple(self._records.values())) if record.scope == scope][:limit]

    async def events(self, revision_id: str) -> list[RevisionEventV1]:
        async with self._lock:
            return [copy.deepcopy(event) for event in self._events if event.revision_id == revision_id]

    async def active(
        self,
        scope: ToolPlaneRevisionScopeV1,
    ) -> ToolPlaneRevisionRecord | None:
        async with self._lock:
            revision_id = self._active.get(scope.key)
            return None if revision_id is None else self._copy_record(self._records[revision_id])

    async def generation(self, scope: ToolPlaneRevisionScopeV1) -> int:
        async with self._lock:
            return self._generations.get(scope.key, 0)

    async def overlay_set_generation(self) -> int:
        async with self._lock:
            return self._overlay_set_generation

    async def active_overlays(self) -> tuple[int, tuple[ToolPlaneRevisionRecord, ...]]:
        async with self._lock:
            overlays = tuple(self._records[revision_id] for key, revision_id in sorted(self._active.items()) if key.startswith("user_overlay:"))
            return self._overlay_set_generation, tuple(self._copy_record(record) for record in overlays)

    async def active_overlays_page(
        self,
        *,
        after_ref: str | None,
        limit: int,
    ) -> tuple[int, tuple[ToolPlaneRevisionRecord, ...], str | None]:
        if limit < 1 or limit > 1_000:
            raise ValueError("limit must be between 1 and 1000")
        async with self._lock:
            pairs = [
                (key.removeprefix("user_overlay:"), self._records[revision_id]) for key, revision_id in sorted(self._active.items()) if key.startswith("user_overlay:") and (after_ref is None or key.removeprefix("user_overlay:") > after_ref)
            ]
            selected = pairs[:limit]
            next_cursor = selected[-1][0] if len(pairs) > len(selected) and selected else None
            return (
                self._overlay_set_generation,
                tuple(self._copy_record(record) for _, record in selected),
                next_cursor,
            )

    async def begin_validation(
        self,
        revision_id: str,
        *,
        actor_digest: str,
    ) -> ToolPlaneRevisionRecord:
        async with self._lock:
            record = self._records.get(revision_id)
            if record is None:
                raise ToolPlaneRevisionError("revision_not_found")
            if record.state != "staged":
                raise ToolPlaneRevisionError("validation_stale")
            updated = replace(record, state="validating", validation_report=None)
            self._records[revision_id] = updated
            self._append_event(updated, state="validating", actor_digest=actor_digest)
            return self._copy_record(updated)

    async def complete_validation(
        self,
        revision_id: str,
        *,
        actor_digest: str,
        report: ToolPlaneValidationReportV1,
    ) -> ToolPlaneRevisionRecord:
        async with self._lock:
            record = self._records.get(revision_id)
            if record is None or record.state != "validating":
                raise ToolPlaneRevisionError("revision_conflict")
            if report.revision_digest != record.revision_digest or report.content_digest != record.content_digest:
                raise ToolPlaneRevisionError("validation_stale")
            state: RevisionState = "validated" if report.result == "passed" else "rejected"
            updated = replace(record, state=state, validation_report=report)
            self._records[revision_id] = updated
            self._append_event(
                updated,
                state=state,
                actor_digest=actor_digest,
                safe_details={"report_digest": report.report_digest, "result": report.result},
            )
            return self._copy_record(updated)

    async def prepare_activation(
        self,
        revision_id: str,
        *,
        actor_digest: str,
        expected_active_digest: str | None,
        expected_base_generation: int | None,
        expected_overlay_set_generation: int | None,
        required_compatibility: tuple[tuple[str, str, str], ...] = (),
    ) -> ToolPlaneRevisionRecord:
        async with self._lock:
            record = self._records.get(revision_id)
            if record is None:
                raise ToolPlaneRevisionError("revision_not_found")
            if any(other.revision_id != revision_id and other.state in {"prepared", "recovery_required"} for other in self._records.values()):
                raise ToolPlaneRevisionError("recovery_required")
            if record.state not in {"validated", "superseded"}:
                raise ToolPlaneRevisionError("validation_stale")
            if record.validation_report is None or record.validation_report.result != "passed":
                raise ToolPlaneRevisionError("validation_failed")
            active_id = self._active.get(record.scope.key)
            active = None if active_id is None else self._records[active_id]
            active_digest = None if active is None else active.revision_digest
            if active_digest != expected_active_digest:
                raise ToolPlaneRevisionError("revision_conflict")
            if record.parent_revision_digest != active_digest:
                raise ToolPlaneRevisionError("revision_conflict")
            if expected_base_generation is not None:
                base_generation = self._generations.get("deployment_base", 0)
                if base_generation != expected_base_generation:
                    raise ToolPlaneRevisionError("base_revision_changed")
            if expected_overlay_set_generation is not None and self._overlay_set_generation != expected_overlay_set_generation:
                raise ToolPlaneRevisionError("active_overlay_set_changed")
            if any((attestation := self._compatibilities.get(key)) is None or not attestation.compatible for key in required_compatibility):
                raise ToolPlaneRevisionError("overlay_preflight_incomplete")
            updated = replace(
                record,
                state="prepared",
                promotion_actor_digest=actor_digest,
                previous_revision_id=active_id,
                desired_projection_digest=record.content_digest,
            )
            self._records[revision_id] = updated
            self._append_event(updated, state="prepared", actor_digest=actor_digest)
            return self._copy_record(updated)

    async def finalize_activation(
        self,
        revision_id: str,
        *,
        actor_digest: str,
        observed_projection_digest: str,
    ) -> ToolPlaneRevisionRecord:
        async with self._lock:
            record = self._records.get(revision_id)
            if record is None or record.state not in {
                "prepared",
                "recovery_required",
            }:
                raise ToolPlaneRevisionError("revision_conflict")
            if observed_projection_digest != record.desired_projection_digest:
                updated = replace(
                    record,
                    state="recovery_required",
                    observed_projection_digest=observed_projection_digest,
                )
                self._records[revision_id] = updated
                self._append_event(
                    updated,
                    state="recovery_required",
                    actor_digest=actor_digest,
                    safe_details={"reason": "projection_digest_mismatch"},
                )
                raise ToolPlaneRevisionError("projection_digest_mismatch")
            previous_id = record.previous_revision_id
            if previous_id is not None and previous_id != revision_id:
                previous = self._records[previous_id]
                self._records[previous_id] = replace(previous, state="superseded")
            promoted_at = _now()
            updated = replace(
                record,
                state="promoted",
                observed_projection_digest=observed_projection_digest,
                promoted_at=promoted_at,
            )
            self._records[revision_id] = updated
            self._active[record.scope.key] = revision_id
            self._generations[record.scope.key] = self._generations.get(record.scope.key, 0) + 1
            if record.scope.kind == "user_overlay":
                self._overlay_set_generation += 1
            self._append_event(updated, state="promoted", actor_digest=actor_digest)
            return self._copy_record(updated)

    async def mark_recovery_required(
        self,
        revision_id: str,
        *,
        actor_digest: str,
        reason: str,
    ) -> None:
        async with self._lock:
            record = self._records.get(revision_id)
            if record is None or record.state not in {
                "prepared",
                "recovery_required",
            }:
                return
            updated = replace(record, state="recovery_required")
            self._records[revision_id] = updated
            self._append_event(
                updated,
                state="recovery_required",
                actor_digest=actor_digest,
                safe_details={"reason": reason},
            )

    async def prepared_or_recovery(self) -> tuple[ToolPlaneRevisionRecord, ...]:
        async with self._lock:
            return tuple(self._copy_record(record) for record in self._records.values() if record.state in {"prepared", "recovery_required"})


class ToolPlaneRevisionService:
    """The single governance interface used by APIs and runtime admission."""

    def __init__(
        self,
        *,
        repository: Any,
        projection: ToolPlaneProjection,
        validator: ToolPlaneValidator,
        durable: bool,
        immutable: bool = False,
        preflight_timeout_seconds: float = 30.0,
        artifact_store: Any | None = None,
        user_inventory: ToolPlaneUserInventory | None = None,
        active_overlay_page_size: int = 250,
        maximum_active_overlays: int = 10_000,
        authority_universe: tuple[str, ...] = _DEFAULT_AUTHORITY_UNIVERSE,
    ) -> None:
        self._repository = repository
        self._projection = projection
        self._validator = validator
        self._durable = durable
        self._immutable = immutable
        self._preflight_timeout_seconds = preflight_timeout_seconds
        self._artifact_store = artifact_store
        self._user_inventory = user_inventory or StaticToolPlaneUserInventory()
        if active_overlay_page_size < 1 or active_overlay_page_size > 1_000:
            raise ValueError("active_overlay_page_size must be between 1 and 1000")
        if maximum_active_overlays < 1 or maximum_active_overlays > 10_000:
            raise ValueError("maximum_active_overlays must be between 1 and 10000")
        self._active_overlay_page_size = active_overlay_page_size
        self._maximum_active_overlays = maximum_active_overlays
        canonical_authorities = canonicalize_authority_v1(authority_universe)
        if len(canonical_authorities) > 12:
            raise ValueError("authority_universe accepts at most 12 permissions")
        self._authority_by_digest: dict[str, frozenset[str]] = {}
        for size in range(len(canonical_authorities) + 1):
            for authority_set in combinations(canonical_authorities, size):
                self._authority_by_digest[effective_authority_digest_v1(authority_set)] = frozenset(authority_set)

    @property
    def immutable(self) -> bool:
        return self._immutable

    @property
    def durable(self) -> bool:
        return self._durable

    @property
    def validation_policy_digest(self) -> str:
        return self._validator.policy_digest

    def _verify_actor(self, actor: VerifiedActorContextV1) -> frozenset[str]:
        if not isinstance(actor, VerifiedActorContextV1):
            raise ToolPlaneRevisionError("promotion_not_authorized")
        if actor.tenant.public_ref != self._repository.tenant.public_ref or actor.tenant.digest != self._repository.tenant.digest:
            raise ToolPlaneRevisionError("promotion_not_authorized")
        authorities = self._authority_by_digest.get(
            actor.credential.effective_authority_digest,
        )
        if authorities is None or authority_categories_v1(authorities) != actor.credential.authority_categories:
            raise ToolPlaneRevisionError("promotion_not_authorized")
        return authorities

    def _authorize_scope(
        self,
        scope: ToolPlaneRevisionScopeV1,
        actor: VerifiedActorContextV1,
        *,
        mutation: bool,
        administrative: bool = False,
    ) -> None:
        authorities = self._verify_actor(actor)
        if scope.kind == "deployment_base" or administrative:
            required_authority = "tool_plane:admin"
        elif mutation:
            required_authority = "tool_plane:mutate"
        else:
            required_authority = "tool_plane:read"
        has_required_authority = required_authority in authorities or (required_authority == "tool_plane:read" and "tool_plane:mutate" in authorities)
        if not has_required_authority:
            raise ToolPlaneRevisionError("promotion_not_authorized")
        if mutation and self._immutable:
            raise ToolPlaneRevisionError("immutable_deployment")
        subject = actor.identity.effective_subject
        if scope.kind == "deployment_base":
            if subject.kind != "human" or subject.role != "admin":
                raise ToolPlaneRevisionError("promotion_not_authorized")
            return
        if administrative:
            if subject.kind != "human" or subject.role != "admin":
                raise ToolPlaneRevisionError("promotion_not_authorized")
            return
        if scope.user_ref != user_scope_reference(actor):
            raise ToolPlaneRevisionError("promotion_not_authorized")

    async def initialize(self, *, existing_projection: bool) -> None:
        if not existing_projection and hasattr(self._projection, "has_existing_user_projection"):
            inventory = await self._user_inventory.snapshot()
            existing_projection = await self._projection.has_existing_user_projection(inventory.subject_ids)
        await self._repository.initialize(existing_projection=existing_projection)

    async def stage_skill_archive(
        self,
        scope: ToolPlaneRevisionScopeV1,
        source: Any,
        actor: VerifiedActorContextV1,
    ) -> Any:
        """Safely stage skill bytes without creating or activating a revision."""

        self._authorize_scope(scope, actor, mutation=True)
        if self._artifact_store is None:
            raise ToolPlaneRevisionError("validator_unavailable")
        return await asyncio.to_thread(self._artifact_store.stage_archive, source)

    async def stage_current_projection(
        self,
        actor: VerifiedActorContextV1,
    ) -> BootstrapStagingResult:
        """Stage the exact base and every indexed nonempty user overlay."""

        scope = ToolPlaneRevisionScopeV1(kind="deployment_base")
        self._authorize_scope(scope, actor, mutation=True)
        if (
            self._artifact_store is None
            or not hasattr(
                self._projection,
                "capture_current_deployment",
            )
            or not hasattr(self._projection, "capture_current_user")
        ):
            raise ToolPlaneRevisionError("validator_unavailable")
        first_inventory = await self._user_inventory.snapshot()
        await self._assert_inventory_is_complete(first_inventory)
        candidate, _ = await self._projection.capture_current_deployment(
            validation_policy_digest=self._validator.policy_digest,
            artifact_store=self._artifact_store,
        )
        active = await self._repository.active(scope)
        candidate["parent_revision_digest"] = None if active is None else active.revision_digest
        material = canonicalize_deployment_candidate(candidate)
        base_revision_digest = canonical_tool_plane_digest(
            {
                "version": 1,
                "tenant_digest": actor.tenant.digest,
                "scope": scope.to_json(),
                "content_digest": material.digest,
            }
        )
        overlay_records: list[ToolPlaneRevisionRecord] = []
        for subject_id in first_inventory.subject_ids:
            overlay_candidate, _ = await self._projection.capture_current_user(
                storage_subject_id=subject_id,
                base_revision_digest=base_revision_digest,
                artifact_store=self._artifact_store,
            )
            overlay_material = canonicalize_user_overlay_candidate(overlay_candidate)
            if overlay_material.is_empty:
                continue
            overlay_scope = ToolPlaneRevisionScopeV1(
                kind="user_overlay",
                user_ref=user_scope_reference_for_subject(
                    tenant_digest=actor.tenant.digest,
                    subject_kind="human",
                    subject_id=subject_id,
                ),
            )
            overlay_revision_digest = canonical_tool_plane_digest(
                {
                    "version": 1,
                    "tenant_digest": actor.tenant.digest,
                    "scope": overlay_scope.to_json(),
                    "content_digest": overlay_material.digest,
                }
            )
            overlay_records.append(
                ToolPlaneRevisionRecord(
                    revision_id=str(uuid.uuid4()),
                    revision_digest=overlay_revision_digest,
                    tenant_ref=actor.tenant.public_ref,
                    tenant_digest=actor.tenant.digest,
                    scope=overlay_scope,
                    content_digest=overlay_material.digest,
                    manifest=overlay_material.to_json(),
                    parent_revision_digest=None,
                    base_revision_digest=base_revision_digest,
                    state="staged",
                    staging_actor_digest=actor.digest,
                    staged_at=_now(),
                    storage_subject_id=subject_id,
                )
            )
        second_inventory = await self._user_inventory.snapshot()
        await self._assert_inventory_is_complete(second_inventory)
        if second_inventory.generation_digest != first_inventory.generation_digest:
            raise ToolPlaneRevisionError("bootstrap_inventory_changed")
        inventory_digest = canonical_tool_plane_digest(
            {
                "version": 1,
                "subject_inventory_digest": first_inventory.generation_digest,
                "base_content_digest": material.digest,
                "overlays": [
                    {
                        "user_ref": overlay.scope.user_ref,
                        "content_digest": overlay.content_digest,
                    }
                    for overlay in overlay_records
                ],
            }
        )
        record = ToolPlaneRevisionRecord(
            revision_id=str(uuid.uuid4()),
            revision_digest=base_revision_digest,
            tenant_ref=actor.tenant.public_ref,
            tenant_digest=actor.tenant.digest,
            scope=scope,
            content_digest=material.digest,
            manifest=material.to_json(),
            parent_revision_digest=material.parent_revision_digest,
            base_revision_digest=None,
            state="staged",
            staging_actor_digest=actor.digest,
            staged_at=_now(),
            bootstrap_inventory_digest=inventory_digest,
            bootstrap_overlay_revision_ids=tuple(overlay.revision_id for overlay in overlay_records),
            bootstrap_inventory_subject_ids=first_inventory.subject_ids,
        )
        await self._repository.add_bootstrap(record, tuple(overlay_records))
        base_staged = StagedRevision(
            revision_id=record.revision_id,
            revision_digest=record.revision_digest,
            content_digest=record.content_digest,
            scope=record.scope,
            state=record.state,
            staged_at=record.staged_at,
        )
        return BootstrapStagingResult(
            base_revision=base_staged,
            overlay_revisions=tuple(
                StagedRevision(
                    revision_id=overlay.revision_id,
                    revision_digest=overlay.revision_digest,
                    content_digest=overlay.content_digest,
                    scope=overlay.scope,
                    state=overlay.state,
                    staged_at=overlay.staged_at,
                )
                for overlay in overlay_records
            ),
            inventory_digest=inventory_digest,
        )

    async def stage(
        self,
        request: ScopedStageRevisionRequest,
        actor: VerifiedActorContextV1,
    ) -> StagedRevision:
        self._authorize_scope(request.scope, actor, mutation=True)
        if request.scope.kind == "deployment_base":
            material: DeploymentToolPlaneRevisionV1 | UserToolPlaneOverlayV1 = canonicalize_deployment_candidate(request.candidate)
            base_digest = None
        else:
            material = canonicalize_user_overlay_candidate(request.candidate)
            base_digest = material.base_revision_digest
        manifest = material.to_json()
        content_digest = material.digest
        revision_digest = canonical_tool_plane_digest(
            {
                "version": 1,
                "tenant_digest": actor.tenant.digest,
                "scope": request.scope.to_json(),
                "content_digest": content_digest,
            }
        )
        revision_id = str(uuid.uuid4())
        record = ToolPlaneRevisionRecord(
            revision_id=revision_id,
            revision_digest=revision_digest,
            tenant_ref=actor.tenant.public_ref,
            tenant_digest=actor.tenant.digest,
            scope=request.scope,
            content_digest=content_digest,
            manifest=manifest,
            parent_revision_digest=material.parent_revision_digest,
            base_revision_digest=base_digest,
            state="staged",
            staging_actor_digest=actor.digest,
            staged_at=_now(),
            storage_subject_id=(actor.identity.effective_subject.subject_id if request.scope.kind == "user_overlay" else None),
        )
        await self._repository.add(record)
        return StagedRevision(
            revision_id=record.revision_id,
            revision_digest=record.revision_digest,
            content_digest=record.content_digest,
            scope=record.scope,
            state=record.state,
            staged_at=record.staged_at,
        )

    async def _record_for_actor(
        self,
        revision_id: str,
        actor: VerifiedActorContextV1,
        *,
        mutation: bool,
        administrative: bool = False,
    ) -> ToolPlaneRevisionRecord:
        self._verify_actor(actor)
        record = await self._repository.get(revision_id)
        if record is None:
            raise ToolPlaneRevisionError("revision_not_found")
        self._authorize_scope(
            record.scope,
            actor,
            mutation=mutation,
            administrative=administrative,
        )
        return record

    async def validate(
        self,
        revision_ref: str,
        actor: VerifiedActorContextV1,
    ) -> ToolPlaneValidationReportV1:
        record = await self._record_for_actor(revision_ref, actor, mutation=True)
        return await self._validate_record(record, actor)

    async def admin_validate(
        self,
        revision_ref: str,
        actor: VerifiedActorContextV1,
    ) -> ToolPlaneValidationReportV1:
        """Validate a named user revision through the explicit admin path."""

        record = await self._record_for_actor(
            revision_ref,
            actor,
            mutation=True,
            administrative=True,
        )
        return await self._validate_record(record, actor)

    async def _validate_record(
        self,
        record: ToolPlaneRevisionRecord,
        actor: VerifiedActorContextV1,
    ) -> ToolPlaneValidationReportV1:
        validating = await self._repository.begin_validation(
            record.revision_id,
            actor_digest=actor.digest,
        )
        try:
            report = await self._validator.validate(validating)
        except ToolPlaneRevisionError as exc:
            report = ToolPlaneValidationReportV1(
                revision_digest=validating.revision_digest,
                content_digest=validating.content_digest,
                validator_policy_digest=self._validator.policy_digest,
                validator_versions={"pipeline": "unavailable"},
                result=("unqualified" if exc.code == "validator_unavailable" else "failed"),
                findings=(
                    ToolPlaneValidationFindingV1(
                        code=exc.code,
                        severity="error",
                    ),
                ),
            )
        except Exception:
            report = ToolPlaneValidationReportV1(
                revision_digest=validating.revision_digest,
                content_digest=validating.content_digest,
                validator_policy_digest=self._validator.policy_digest,
                validator_versions={"pipeline": "unavailable"},
                result="unqualified",
                findings=(
                    ToolPlaneValidationFindingV1(
                        code="validator_unavailable",
                        severity="error",
                    ),
                ),
            )
        await self._repository.complete_validation(
            record.revision_id,
            actor_digest=actor.digest,
            report=report,
        )
        return report

    @staticmethod
    def _overlay_is_empty(record: ToolPlaneRevisionRecord) -> bool:
        return not any(
            record.manifest.get(field_name)
            for field_name in (
                "custom_skills",
                "mcp_enablement",
                "managed_integration_enablement",
                "credential_selectors",
                "skill_states",
            )
        )

    async def _attest_compatibility(
        self,
        *,
        base: ToolPlaneRevisionRecord,
        overlay: ToolPlaneRevisionRecord,
    ) -> OverlayCompatibilityV1:
        existing = await self._repository.compatibility_attestation(
            base_revision_digest=base.revision_digest,
            overlay_revision_digest=overlay.revision_digest,
            validator_policy_digest=self._validator.policy_digest,
        )
        if existing is not None:
            if not existing.compatible:
                raise ToolPlaneRevisionError("overlay_preflight_failed")
            return existing
        try:
            async with asyncio.timeout(self._preflight_timeout_seconds):
                report = await self._validator.validate_compatibility(
                    base=base,
                    overlay=overlay,
                )
        except TimeoutError as exc:
            raise ToolPlaneRevisionError("overlay_preflight_incomplete") from exc
        except ToolPlaneRevisionError:
            raise
        except Exception as exc:
            raise ToolPlaneRevisionError("overlay_preflight_incomplete") from exc
        try:
            attestation = OverlayCompatibilityV1(
                base_revision_digest=base.revision_digest,
                overlay_revision_digest=overlay.revision_digest,
                validator_policy_digest=self._validator.policy_digest,
                report=report,
                compatible=report.result == "passed",
            )
        except ValueError as exc:
            raise ToolPlaneRevisionError("overlay_preflight_failed") from exc
        persisted = await self._repository.save_compatibility(attestation)
        if not persisted.compatible:
            raise ToolPlaneRevisionError("overlay_preflight_failed")
        return persisted

    async def _preflight_base(
        self,
        record: ToolPlaneRevisionRecord,
    ) -> tuple[int, tuple[tuple[str, str, str], ...]]:
        try:
            async with asyncio.timeout(self._preflight_timeout_seconds):
                generation, overlays = await self._active_overlay_snapshot()
                required: list[tuple[str, str, str]] = []
                for overlay in overlays:
                    if self._overlay_is_empty(overlay):
                        continue
                    attestation = await self._attest_compatibility(
                        base=record,
                        overlay=overlay,
                    )
                    required.append(attestation.key)
                return generation, tuple(required)
        except TimeoutError as exc:
            raise ToolPlaneRevisionError("overlay_preflight_incomplete") from exc

    async def _active_overlay_snapshot(
        self,
    ) -> tuple[int, tuple[ToolPlaneRevisionRecord, ...]]:
        if not hasattr(self._repository, "active_overlays_page"):
            generation, overlays = await self._repository.active_overlays()
            if len(overlays) > self._maximum_active_overlays:
                raise ToolPlaneRevisionError("overlay_preflight_incomplete")
            return generation, overlays
        cursor: str | None = None
        expected_generation: int | None = None
        overlays: list[ToolPlaneRevisionRecord] = []
        while True:
            generation, page, next_cursor = await self._repository.active_overlays_page(
                after_ref=cursor,
                limit=self._active_overlay_page_size,
            )
            if expected_generation is None:
                expected_generation = generation
            elif generation != expected_generation:
                raise ToolPlaneRevisionError("active_overlay_set_changed")
            overlays.extend(page)
            if len(overlays) > self._maximum_active_overlays:
                raise ToolPlaneRevisionError("overlay_preflight_incomplete")
            if next_cursor is None:
                break
            if next_cursor == cursor:
                raise ToolPlaneRevisionError("overlay_preflight_incomplete")
            cursor = next_cursor
        return expected_generation or 0, tuple(overlays)

    async def _assert_bootstrap_inventory(
        self,
        base: ToolPlaneRevisionRecord,
    ) -> None:
        if base.bootstrap_inventory_digest is None or self._artifact_store is None or not hasattr(self._projection, "capture_current_deployment") or not hasattr(self._projection, "capture_current_user"):
            raise ToolPlaneRevisionError("tool_plane_bootstrap_required")
        inventory = await self._user_inventory.snapshot()
        await self._assert_inventory_is_complete(inventory)
        if inventory.subject_ids != base.bootstrap_inventory_subject_ids:
            raise ToolPlaneRevisionError("bootstrap_inventory_changed")
        candidate, _ = await self._projection.capture_current_deployment(
            validation_policy_digest=self._validator.policy_digest,
            artifact_store=self._artifact_store,
        )
        candidate["parent_revision_digest"] = base.parent_revision_digest
        current_base = canonicalize_deployment_candidate(candidate)
        overlays: list[dict[str, object]] = []
        for subject_id in inventory.subject_ids:
            overlay_candidate, _ = await self._projection.capture_current_user(
                storage_subject_id=subject_id,
                base_revision_digest=base.revision_digest,
                artifact_store=self._artifact_store,
            )
            overlay = canonicalize_user_overlay_candidate(overlay_candidate)
            if overlay.is_empty:
                continue
            overlays.append(
                {
                    "user_ref": user_scope_reference_for_subject(
                        tenant_digest=base.tenant_digest,
                        subject_kind="human",
                        subject_id=subject_id,
                    ),
                    "content_digest": overlay.digest,
                }
            )
        observed = canonical_tool_plane_digest(
            {
                "version": 1,
                "subject_inventory_digest": inventory.generation_digest,
                "base_content_digest": current_base.digest,
                "overlays": overlays,
            }
        )
        if observed != base.bootstrap_inventory_digest:
            raise ToolPlaneRevisionError("bootstrap_inventory_changed")

    async def _assert_inventory_is_complete(
        self,
        inventory: ToolPlaneUserInventorySnapshot,
    ) -> None:
        if hasattr(self._projection, "has_unindexed_user_projection") and (await self._projection.has_unindexed_user_projection(inventory.subject_ids)):
            raise ToolPlaneRevisionError("bootstrap_inventory_changed")

    async def _bootstrap_base_for(
        self,
        record: ToolPlaneRevisionRecord,
    ) -> ToolPlaneRevisionRecord:
        if record.scope.kind == "deployment_base":
            base = record
        else:
            base = await self._repository.active(ToolPlaneRevisionScopeV1(kind="deployment_base"))
            if base is None or record.revision_id not in base.bootstrap_overlay_revision_ids:
                raise ToolPlaneRevisionError("tool_plane_bootstrap_required")
        if base.bootstrap_inventory_digest is None:
            raise ToolPlaneRevisionError("tool_plane_bootstrap_required")
        return base

    async def _complete_bootstrap_if_ready(
        self,
        actor: VerifiedActorContextV1,
    ) -> None:
        if not await self._repository.bootstrap_required():
            return
        base = await self._repository.active(ToolPlaneRevisionScopeV1(kind="deployment_base"))
        if base is None or base.bootstrap_inventory_digest is None:
            return
        for revision_id in base.bootstrap_overlay_revision_ids:
            overlay = await self._repository.get(revision_id)
            if overlay is None:
                raise ToolPlaneRevisionError("recovery_required")
            active = await self._repository.active(overlay.scope)
            if active is None or active.revision_id != revision_id:
                return
        await self._assert_bootstrap_inventory(base)
        if await self._observed_record(base) != base.content_digest:
            raise ToolPlaneRevisionError("projection_digest_mismatch")
        for revision_id in base.bootstrap_overlay_revision_ids:
            overlay = await self._repository.get(revision_id)
            assert overlay is not None
            if await self._observed_record(overlay) != overlay.content_digest:
                raise ToolPlaneRevisionError("projection_digest_mismatch")
        await self._repository.clear_bootstrap()

    async def promote(
        self,
        revision_ref: str,
        actor: VerifiedActorContextV1,
    ) -> PromotionResult:
        record = await self._record_for_actor(revision_ref, actor, mutation=True)
        return await self._promote_record(record, actor)

    async def admin_promote(
        self,
        revision_ref: str,
        actor: VerifiedActorContextV1,
    ) -> PromotionResult:
        """Promote a named user revision through the explicit admin path."""

        record = await self._record_for_actor(
            revision_ref,
            actor,
            mutation=True,
            administrative=True,
        )
        return await self._promote_record(record, actor)

    async def _promote_record(
        self,
        record: ToolPlaneRevisionRecord,
        actor: VerifiedActorContextV1,
    ) -> PromotionResult:
        if record.validation_report is None or record.validation_report.result != "passed":
            raise ToolPlaneRevisionError("validation_failed")
        if record.validation_report.content_digest != record.content_digest:
            raise ToolPlaneRevisionError("validation_stale")
        if await self._repository.bootstrap_required():
            bootstrap_base = await self._bootstrap_base_for(record)
            await self._assert_bootstrap_inventory(bootstrap_base)
        active = await self._repository.active(record.scope)
        expected_active_digest = None if active is None else active.revision_digest
        expected_base_generation: int | None = None
        expected_overlay_generation: int | None = None
        required_compatibility: tuple[tuple[str, str, str], ...] = ()
        if record.scope.kind == "deployment_base":
            (
                expected_overlay_generation,
                required_compatibility,
            ) = await self._preflight_base(record)
        else:
            base_scope = ToolPlaneRevisionScopeV1(kind="deployment_base")
            base = await self._repository.active(base_scope)
            if base is None or (base.revision_digest != record.base_revision_digest and record.rollback_source_revision_id is None):
                raise ToolPlaneRevisionError("base_revision_changed")
            expected_base_generation = await self._repository.generation(base_scope)
            compatibility = await self._attest_compatibility(
                base=base,
                overlay=record,
            )
            required_compatibility = (compatibility.key,)
        prepared = await self._repository.prepare_activation(
            record.revision_id,
            actor_digest=actor.digest,
            expected_active_digest=expected_active_digest,
            expected_base_generation=expected_base_generation,
            expected_overlay_set_generation=expected_overlay_generation,
            required_compatibility=required_compatibility,
        )
        try:
            observed = await self._project_record(prepared)
        except Exception as exc:
            reason = exc.code if isinstance(exc, ToolPlaneRevisionError) else "projection_failed"
            await self._repository.mark_recovery_required(
                prepared.revision_id,
                actor_digest=actor.digest,
                reason=reason,
            )
            if isinstance(exc, ToolPlaneRevisionError):
                raise
            raise ToolPlaneRevisionError("projection_failed") from exc
        promoted = await self._repository.finalize_activation(
            prepared.revision_id,
            actor_digest=actor.digest,
            observed_projection_digest=observed,
        )
        await self._complete_bootstrap_if_ready(actor)
        assert promoted.promoted_at is not None
        assert promoted.desired_projection_digest is not None
        assert promoted.observed_projection_digest is not None
        return PromotionResult(
            revision_id=promoted.revision_id,
            revision_digest=promoted.revision_digest,
            state=promoted.state,
            actor_digest=actor.digest,
            previous_revision_id=promoted.previous_revision_id,
            desired_projection_digest=promoted.desired_projection_digest,
            observed_projection_digest=promoted.observed_projection_digest,
            promoted_at=promoted.promoted_at,
        )

    async def _project_record(self, record: ToolPlaneRevisionRecord) -> str:
        if record.scope.kind == "user_overlay" and hasattr(
            self._projection,
            "project_for_actor",
        ):
            if not record.storage_subject_id:
                raise ToolPlaneRevisionError("recovery_required")
            return await self._projection.project_for_actor(
                record.scope,
                record.manifest,
                desired_digest=record.content_digest,
                storage_subject_id=record.storage_subject_id,
            )
        return await self._projection.project(
            record.scope,
            record.manifest,
            desired_digest=record.content_digest,
        )

    async def _observed_record(
        self,
        record: ToolPlaneRevisionRecord,
    ) -> str | None:
        if record.scope.kind == "user_overlay" and hasattr(
            self._projection,
            "observed_digest_for_actor",
        ):
            if not record.storage_subject_id:
                return None
            return await self._projection.observed_digest_for_actor(
                record.scope,
                storage_subject_id=record.storage_subject_id,
            )
        return await self._projection.observed_digest(record.scope)

    async def rollback(
        self,
        revision_ref: str,
        actor: VerifiedActorContextV1,
    ) -> PromotionResult:
        target = await self._record_for_actor(revision_ref, actor, mutation=True)
        return await self._rollback_record(target, actor)

    async def admin_rollback(
        self,
        revision_ref: str,
        actor: VerifiedActorContextV1,
    ) -> PromotionResult:
        """Roll back a named user scope through the explicit admin path."""

        target = await self._record_for_actor(
            revision_ref,
            actor,
            mutation=True,
            administrative=True,
        )
        return await self._rollback_record(target, actor)

    async def _rollback_record(
        self,
        target: ToolPlaneRevisionRecord,
        actor: VerifiedActorContextV1,
    ) -> PromotionResult:
        if target.state not in {"promoted", "superseded"}:
            raise ToolPlaneRevisionError("validation_stale")
        if target.validation_report is None or target.validation_report.result != "passed":
            raise ToolPlaneRevisionError("validation_failed")
        active = await self._repository.active(target.scope)
        if active is None:
            raise ToolPlaneRevisionError("revision_conflict")
        if active.revision_id == target.revision_id:
            raise ToolPlaneRevisionError("revision_conflict")
        revision_id = str(uuid.uuid4())
        revision_digest = canonical_tool_plane_digest(
            {
                "version": 1,
                "tenant_digest": actor.tenant.digest,
                "scope": target.scope.to_json(),
                "content_digest": target.content_digest,
                "parent_revision_digest": active.revision_digest,
                "rollback_source_revision_digest": target.revision_digest,
            }
        )
        record = ToolPlaneRevisionRecord(
            revision_id=revision_id,
            revision_digest=revision_digest,
            tenant_ref=target.tenant_ref,
            tenant_digest=target.tenant_digest,
            scope=target.scope,
            content_digest=target.content_digest,
            manifest=copy.deepcopy(dict(target.manifest)),
            parent_revision_digest=active.revision_digest,
            base_revision_digest=target.base_revision_digest,
            state="staged",
            staging_actor_digest=actor.digest,
            staged_at=_now(),
            rollback_source_revision_id=target.revision_id,
            storage_subject_id=target.storage_subject_id,
        )
        await self._repository.add(record)
        report = await self._validate_record(record, actor)
        if report.result != "passed":
            raise ToolPlaneRevisionError("validation_failed")
        validated = await self._repository.get(record.revision_id)
        if validated is None:
            raise ToolPlaneRevisionError("revision_not_found")
        return await self._promote_record(validated, actor)

    async def list_for_actor(
        self,
        actor: VerifiedActorContextV1,
        *,
        limit: int = 50,
    ) -> list[ToolPlaneRevisionRecord]:
        scope = ToolPlaneRevisionScopeV1(
            kind="user_overlay",
            user_ref=user_scope_reference(actor),
        )
        self._authorize_scope(scope, actor, mutation=False)
        return await self._repository.list_scope(scope, limit=limit)

    async def inspect_for_actor(
        self,
        revision_ref: str,
        actor: VerifiedActorContextV1,
    ) -> ToolPlaneRevisionRecord:
        return await self._record_for_actor(
            revision_ref,
            actor,
            mutation=False,
        )

    async def admin_inspect(
        self,
        revision_ref: str,
        actor: VerifiedActorContextV1,
    ) -> ToolPlaneRevisionRecord:
        return await self._record_for_actor(
            revision_ref,
            actor,
            mutation=False,
            administrative=True,
        )

    async def admin_list(
        self,
        scope: ToolPlaneRevisionScopeV1,
        actor: VerifiedActorContextV1,
        *,
        limit: int = 50,
    ) -> list[ToolPlaneRevisionRecord]:
        self._authorize_scope(scope, actor, mutation=False, administrative=True)
        return await self._repository.list_scope(scope, limit=limit)

    async def status_for_actor(
        self,
        actor: VerifiedActorContextV1,
    ) -> ToolPlaneStatus:
        scope = ToolPlaneRevisionScopeV1(
            kind="user_overlay",
            user_ref=user_scope_reference(actor),
        )
        self._authorize_scope(scope, actor, mutation=False)
        return await self._status(scope)

    async def admin_status(
        self,
        scope: ToolPlaneRevisionScopeV1,
        actor: VerifiedActorContextV1,
    ) -> ToolPlaneStatus:
        self._authorize_scope(scope, actor, mutation=False, administrative=True)
        return await self._status(scope)

    async def _status(self, scope: ToolPlaneRevisionScopeV1) -> ToolPlaneStatus:
        active = await self._repository.active(scope)
        generation = await self._repository.generation(scope)
        observed = await self._projection.observed_digest(scope) if active is None else await self._observed_record(active)
        if self._immutable:
            state = "immutable"
        elif await self._repository.bootstrap_required():
            state = "bootstrap_required"
        elif active is None:
            state = "unmanaged"
        elif active.state == "recovery_required":
            state = "recovery_required"
        else:
            state = "governed"
        expected = None if active is None else active.content_digest
        return ToolPlaneStatus(
            scope=scope,
            governance_state=state,
            active_revision_id=None if active is None else active.revision_id,
            active_revision_digest=None if active is None else active.revision_digest,
            generation=generation,
            projection_digest=observed,
            drift=active is not None and observed != expected,
        )

    async def effective_for_actor(
        self,
        actor: VerifiedActorContextV1,
    ) -> EffectiveToolPlaneRevisionV1:
        self._verify_actor(actor)
        if await self._repository.bootstrap_required():
            raise ToolPlaneRevisionError("tool_plane_bootstrap_required")
        base_scope = ToolPlaneRevisionScopeV1(kind="deployment_base")
        overlay_scope = ToolPlaneRevisionScopeV1(
            kind="user_overlay",
            user_ref=user_scope_reference(actor),
        )
        for _ in range(3):
            base_generation = await self._repository.generation(base_scope)
            overlay_generation = await self._repository.generation(overlay_scope)
            overlay_set_generation = await self._repository.overlay_set_generation()
            base = await self._repository.active(base_scope)
            if base is None:
                raise ToolPlaneRevisionError("tool_plane_bootstrap_required")
            overlay = await self._repository.active(overlay_scope)
            base_observed = await self._observed_record(base)
            overlay_observed = await self._projection.observed_digest(overlay_scope) if overlay is None else await self._observed_record(overlay)
            if base_generation == await self._repository.generation(base_scope) and overlay_generation == await self._repository.generation(overlay_scope) and overlay_set_generation == await self._repository.overlay_set_generation():
                break
        else:
            raise ToolPlaneRevisionError("revision_conflict")
        if overlay is not None and not self._overlay_is_empty(overlay):
            compatibility = await self._repository.compatibility_attestation(
                base_revision_digest=base.revision_digest,
                overlay_revision_digest=overlay.revision_digest,
                validator_policy_digest=self._validator.policy_digest,
            )
            if compatibility is None or not compatibility.compatible:
                raise ToolPlaneRevisionError("base_revision_changed")
        drift = base_observed != base.content_digest or (overlay is not None and overlay_observed != overlay.content_digest)
        if drift and self._durable:
            raise ToolPlaneRevisionError("unmanaged_drift")
        overlay_digest = EMPTY_OVERLAY_MARKER_V1 if overlay is None else overlay.revision_digest
        projection_digest = canonical_tool_plane_digest(
            {
                "version": 1,
                "base_projection_digest": base_observed,
                "overlay_projection_digest": overlay_observed,
            }
        )
        base_mcp = {str(item["server_id"]): bool(item.get("enabled", True)) for item in base.manifest.get("mcp_servers", []) if isinstance(item, Mapping) and isinstance(item.get("server_id"), str)}
        base_integrations = {str(item["name"]): bool(item.get("enabled", True)) for item in base.manifest.get("managed_integrations", []) if isinstance(item, Mapping) and isinstance(item.get("name"), str)}
        overlay_mcp = {str(item["id"]): bool(item.get("enabled", True)) for item in (() if overlay is None else overlay.manifest.get("mcp_enablement", [])) if isinstance(item, Mapping) and isinstance(item.get("id"), str)}
        overlay_credential_selectors = {
            str(item["server_id"]): {
                "binding_ref": item["binding_ref"],
                "version": item["version"],
            }
            for item in (() if overlay is None else overlay.manifest.get("credential_selectors", []))
            if isinstance(item, Mapping) and isinstance(item.get("server_id"), str)
        }
        overlay_integrations = {
            str(item["id"]): bool(item.get("enabled", True)) for item in (() if overlay is None else overlay.manifest.get("managed_integration_enablement", [])) if isinstance(item, Mapping) and isinstance(item.get("id"), str)
        }
        effective_mcp_servers: list[dict[str, object]] = []
        for item in base.manifest.get("mcp_servers", []):
            if not isinstance(item, Mapping) or not isinstance(item.get("server_id"), str):
                continue
            server_id = str(item["server_id"])
            if not overlay_mcp.get(server_id, bool(item.get("enabled", True))):
                continue
            effective_server = copy.deepcopy(dict(item))
            effective_server["enabled"] = True
            selector = overlay_credential_selectors.get(server_id)
            if selector is not None:
                effective_server["credential_binding"] = selector
            effective_mcp_servers.append(effective_server)
        effective_global_skill_states = tuple(
            {
                "name": str(item["name"]),
                "enabled": bool(item.get("enabled", True)),
            }
            for item in sorted(
                (
                    item
                    for item in (
                        *base.manifest.get("public_skills", []),
                        *base.manifest.get("managed_integrations", []),
                    )
                    if isinstance(item, Mapping) and isinstance(item.get("name"), str)
                ),
                key=lambda item: str(item["name"]),
            )
        )
        return EffectiveToolPlaneRevisionV1(
            base_revision_digest=base.revision_digest,
            user_overlay_digest=overlay_digest,
            base_generation=base_generation,
            overlay_generation=overlay_generation,
            projection_digest=projection_digest,
            effective_mcp_server_ids=tuple(name for name, enabled in base_mcp.items() if overlay_mcp.get(name, enabled)),
            effective_mcp_servers=tuple(effective_mcp_servers),
            effective_global_skill_states=effective_global_skill_states,
            effective_managed_integration_ids=tuple(name for name, enabled in base_integrations.items() if overlay_integrations.get(name, enabled)),
            governance_state="unmanaged" if drift else "governed",
        )

    async def readiness_reason(self) -> str | None:
        """Return one safe fail-closed readiness reason, if any."""

        if await self._repository.bootstrap_required():
            return "tool_plane_bootstrap_required" if self._durable else None
        if await self._repository.prepared_or_recovery():
            return "recovery_required"
        base_scope = ToolPlaneRevisionScopeV1(kind="deployment_base")
        base = await self._repository.active(base_scope)
        if base is None:
            return "tool_plane_bootstrap_required" if self._durable else None
        if await self._observed_record(base) != base.content_digest:
            return "unmanaged_drift" if self._durable else None
        try:
            _, overlays = await self._active_overlay_snapshot()
        except ToolPlaneRevisionError:
            return "recovery_required"
        for overlay in overlays:
            if await self._observed_record(overlay) != overlay.content_digest:
                return "unmanaged_drift" if self._durable else None
            if not self._overlay_is_empty(overlay):
                attestation = await self._repository.compatibility_attestation(
                    base_revision_digest=base.revision_digest,
                    overlay_revision_digest=overlay.revision_digest,
                    validator_policy_digest=self._validator.policy_digest,
                )
                if attestation is None or not attestation.compatible:
                    return "recovery_required"
        return None

    async def reconcile(self, actor: VerifiedActorContextV1) -> None:
        """Replay an immutable prepared projection or leave readiness blocked."""

        self._verify_actor(actor)
        for record in await self._repository.prepared_or_recovery():
            try:
                if record.desired_projection_digest != record.content_digest:
                    raise ToolPlaneRevisionError("projection_digest_mismatch")
                active = await self._repository.active(record.scope)
                active_id = None if active is None else active.revision_id
                if active_id != record.previous_revision_id:
                    raise ToolPlaneRevisionError("revision_conflict")
                if await self._repository.bootstrap_required():
                    bootstrap_base = await self._bootstrap_base_for(record)
                    await self._assert_bootstrap_inventory(bootstrap_base)
                if record.scope.kind == "deployment_base":
                    await self._preflight_base(record)
                else:
                    base = await self._repository.active(ToolPlaneRevisionScopeV1(kind="deployment_base"))
                    if base is None:
                        raise ToolPlaneRevisionError("base_revision_changed")
                    if base.revision_digest != record.base_revision_digest and record.rollback_source_revision_id is None:
                        raise ToolPlaneRevisionError("base_revision_changed")
                    await self._attest_compatibility(base=base, overlay=record)
                observed = await self._observed_record(record)
                if observed != record.desired_projection_digest:
                    observed = await self._project_record(record)
                if observed != record.desired_projection_digest:
                    raise ToolPlaneRevisionError("projection_digest_mismatch")
                await self._repository.finalize_activation(
                    record.revision_id,
                    actor_digest=actor.digest,
                    observed_projection_digest=observed,
                )
                await self._complete_bootstrap_if_ready(actor)
            except Exception as exc:
                reason = exc.code if isinstance(exc, ToolPlaneRevisionError) else "projection_failed"
                await self._repository.mark_recovery_required(
                    record.revision_id,
                    actor_digest=actor.digest,
                    reason=reason,
                )


__all__ = [
    "BootstrapStagingResult",
    "CompositeToolPlaneUserInventory",
    "DeterministicToolPlaneValidator",
    "InMemoryToolPlaneProjection",
    "InMemoryToolPlaneRevisionRepository",
    "OverlayCompatibilityV1",
    "PromotionResult",
    "RegisteredToolPlaneUserInventory",
    "RevisionEventV1",
    "ScopedStageRevisionRequest",
    "StaticToolPlaneUserInventory",
    "StagedRevision",
    "ToolPlaneRevisionRecord",
    "ToolPlaneRevisionService",
    "ToolPlaneStatus",
    "ToolPlaneUserInventorySnapshot",
    "ToolPlaneValidationFindingV1",
    "ToolPlaneValidationReportV1",
    "user_scope_reference",
    "user_scope_reference_for_subject",
]
