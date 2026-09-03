"""Immutable, parent-bound acceptance contracts for durable subagent batches.

This module is the only place that turns the trusted context of an already
accepted lead-tool attempt into batch-owned immutable evidence.  Operational
prompts and runtime objects deliberately stay on :class:`ParentBoundBatchRequest`
and never enter :class:`AcceptedBatchV1`'s safe persisted projection.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, Literal, Protocol, runtime_checkable

from deerflow_extension_api import (
    ConstraintProjectionV1,
    ConstraintProjectionV2,
    TenantReferenceV1,
)

from deerflow.runtime.accepted_invocation import (
    AcceptedInvocation,
    ResolvedAgentMaterialV1,
    ResolvedAgentRevision,
    canonical_digest,
)
from deerflow.runtime.constraints import (
    ConstraintFenceError,
    validate_constraint_fence,
)
from deerflow.runtime.skill_projection import SkillProjectionConsumerToken
from deerflow.runtime.subagent_snapshot import (
    ResolvedSkillScopesV1,
    ResolvedSubagentCatalogV1,
    ResolvedSubagentDefinitionV1,
)
from deerflow.runtime.tool_evidence import (
    DurableToolReceiptSink,
    DurableToolReceiptV1,
    ToolEvidenceRuntimeBinding,
)

BATCH_ACCEPTANCE_VERSION = 1
BATCH_CANONICALIZATION_VERSION = "sha256-canonical-json-v1"
MAX_BATCH_TITLE_BYTES = 256
MAX_BATCH_ITEM_KEY_BYTES = 128
MAX_BATCH_ITEM_PROMPT_BYTES = 100_000
MAX_BATCH_ITEMS = 100_000
MAX_BATCH_ACCEPTANCE_BYTES = 64 * 1024
MAX_BATCH_EXECUTION_BYTES = 768 * 1024
MAX_BATCH_ATTEMPT_EVIDENCE_BYTES = 16 * 1024
MAX_BATCH_ATTEMPT_RECORDS_PER_ITEM = 128
PARENT_BATCH_ACCEPTANCE_CONTEXT_KEY = "__deerflow_accepted_parent_batch_context_v1"
_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_SAFE_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,255}\Z")
_RECEIPT_ID_RE = re.compile(r"tr_[0-9a-f]{64}\Z")
_CONSTRAINT_V1_FIELDS = frozenset(
    {
        "version",
        "request_digest",
        "agent_revision_digest",
        "projection_revision",
        "issued_at",
        "valid_until",
        "evidence_id",
        "evidence_digest",
        "max_total_subagents",
        "projection_digest",
    }
)
_CONSTRAINT_V2_FIELDS = frozenset(
    {
        "version",
        "request_digest",
        "trusted_context_digest",
        "thread_id",
        "agent_revision_digest",
        "profile_revision_digest",
        "extension_manifest_digest",
        "extension_generation",
        "projection_revision",
        "issued_at",
        "valid_until",
        "evidence_id",
        "evidence_digest",
        "mandatory_obligations",
        "max_total_subagents",
        "projection_digest",
    }
)


class BatchAdmissionError(RuntimeError):
    """Fail-closed batch admission error with a safe stable reason code."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


class BatchAdmissionConflict(BatchAdmissionError):
    def __init__(self) -> None:
        super().__init__("batch_admission_conflict")


class BatchLeaseLost(BatchAdmissionError):
    def __init__(self) -> None:
        super().__init__("lease_lost")


def strip_parent_batch_acceptance_context(context: object) -> None:
    """Remove the non-forgeable accepted-parent object from caller context."""

    if isinstance(context, dict):
        context.pop(PARENT_BATCH_ACCEPTANCE_CONTEXT_KEY, None)


def _invalid(code: str = "batch_acceptance_invalid") -> None:
    raise BatchAdmissionError(code)


def _bounded_text(
    value: object,
    *,
    max_bytes: int,
    code: str,
    allow_empty: bool = False,
) -> str:
    if not isinstance(value, str) or (not allow_empty and not value):
        _invalid(code)
    try:
        encoded = value.encode("utf-8")
    except UnicodeError:
        _invalid(code)
    if len(encoded) > max_bytes or any(ord(character) < 32 and character not in "\n\r\t" for character in value) or any(ord(character) == 127 for character in value):
        _invalid(code)
    return value


def _digest(value: object, *, code: str = "batch_digest_invalid") -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        _invalid(code)
    return value


def _safe_id(value: object, *, code: str) -> str:
    if not isinstance(value, str) or _SAFE_ID_RE.fullmatch(value) is None:
        _invalid(code)
    return value


def _canonical_bytes(value: object) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError) as exc:
        raise BatchAdmissionError("batch_acceptance_not_canonical_json") from exc


def _constraint_from_evidence(
    value: Mapping[str, object] | None,
) -> ConstraintProjectionV1 | ConstraintProjectionV2 | None:
    if value is None:
        return None
    if not isinstance(value, Mapping):
        _invalid("batch_constraint_mismatch")
    version = value.get("version")
    expected = _CONSTRAINT_V1_FIELDS if version == 1 else _CONSTRAINT_V2_FIELDS if version == 2 else frozenset()
    if not expected or set(value) != expected:
        _invalid("batch_constraint_mismatch")
    projection_digest = value.get("projection_digest")
    _digest(projection_digest, code="batch_constraint_mismatch")
    payload = {key: _thaw_json(child) for key, child in value.items() if key != "projection_digest"}
    if projection_digest != canonical_digest(payload):
        _invalid("batch_constraint_mismatch")
    try:
        if version == 1:
            return ConstraintProjectionV1(
                request_digest=payload["request_digest"],
                agent_revision_digest=payload["agent_revision_digest"],
                projection_revision=payload["projection_revision"],
                issued_at=datetime.fromisoformat(payload["issued_at"]),
                valid_until=datetime.fromisoformat(payload["valid_until"]),
                evidence_id=payload["evidence_id"],
                evidence_digest=payload["evidence_digest"],
                max_total_subagents=payload["max_total_subagents"],
            )
        return ConstraintProjectionV2(
            request_digest=payload["request_digest"],
            trusted_context_digest=payload["trusted_context_digest"],
            thread_id=payload["thread_id"],
            agent_revision_digest=payload["agent_revision_digest"],
            profile_revision_digest=payload["profile_revision_digest"],
            extension_manifest_digest=payload["extension_manifest_digest"],
            extension_generation=payload["extension_generation"],
            projection_revision=payload["projection_revision"],
            issued_at=datetime.fromisoformat(payload["issued_at"]),
            valid_until=datetime.fromisoformat(payload["valid_until"]),
            evidence_id=payload["evidence_id"],
            evidence_digest=payload["evidence_digest"],
            mandatory_obligations=tuple(payload["mandatory_obligations"]),
            max_total_subagents=payload["max_total_subagents"],
        )
    except BatchAdmissionError:
        raise
    except Exception as exc:
        raise BatchAdmissionError("batch_constraint_mismatch") from exc


def _model_constraints_digest(
    definition: ResolvedSubagentDefinitionV1,
    model_profile: Mapping[str, object],
) -> str:
    parent_model = model_profile.get("name")
    model_selector = definition.model or (parent_model if isinstance(parent_model, str) else None)
    return canonical_digest(
        {
            "version": 1,
            "model_selector": model_selector,
            "model_settings": dict(definition.model_settings),
            "policy_settings": dict(definition.policy_settings),
            "parent_model_profile_digest": canonical_digest(dict(model_profile)),
        }
    )


@dataclass(frozen=True, slots=True)
class BatchItemRequestV1:
    """Protected operational input for one immutable accepted item."""

    key: str
    prompt: str = field(repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "key",
            _bounded_text(
                self.key,
                max_bytes=MAX_BATCH_ITEM_KEY_BYTES,
                code="batch_item_key_invalid",
            ),
        )
        object.__setattr__(
            self,
            "prompt",
            _bounded_text(
                self.prompt,
                max_bytes=MAX_BATCH_ITEM_PROMPT_BYTES,
                code="batch_item_prompt_invalid",
            ),
        )


@dataclass(frozen=True, slots=True)
class BatchLimitsV1:
    max_live_items: int
    max_running_items: int
    max_attempts: int
    max_attempt_records_per_item: int
    max_result_chars: int
    max_total_runtime_seconds: int

    def __post_init__(self) -> None:
        for name, value in (
            ("max_live_items", self.max_live_items),
            ("max_running_items", self.max_running_items),
            ("max_attempts", self.max_attempts),
            (
                "max_attempt_records_per_item",
                self.max_attempt_records_per_item,
            ),
            ("max_result_chars", self.max_result_chars),
            ("max_total_runtime_seconds", self.max_total_runtime_seconds),
        ):
            if type(value) is not int or value < 1:
                _invalid(f"batch_{name}_invalid")
        if self.max_running_items > self.max_live_items:
            _invalid("batch_running_limit_exceeds_live_limit")
        if self.max_attempt_records_per_item < self.max_attempts:
            _invalid("batch_evidence_limit_below_attempt_limit")
        if self.max_attempt_records_per_item > MAX_BATCH_ATTEMPT_RECORDS_PER_ITEM:
            _invalid("batch_evidence_count_too_large")

    def to_json(self) -> dict[str, int]:
        return {
            "max_live_items": self.max_live_items,
            "max_running_items": self.max_running_items,
            "max_attempts": self.max_attempts,
            "max_attempt_records_per_item": (self.max_attempt_records_per_item),
            "max_result_chars": self.max_result_chars,
            "max_total_runtime_seconds": self.max_total_runtime_seconds,
        }

    @classmethod
    def from_json(cls, value: Mapping[str, object]) -> BatchLimitsV1:
        expected = {
            "max_live_items",
            "max_running_items",
            "max_attempts",
            "max_attempt_records_per_item",
            "max_result_chars",
            "max_total_runtime_seconds",
        }
        if not isinstance(value, Mapping) or set(value) != expected:
            _invalid("batch_limits_invalid")
        try:
            return cls(**dict(value))  # type: ignore[arg-type]
        except BatchAdmissionError:
            raise
        except (TypeError, ValueError) as exc:
            raise BatchAdmissionError("batch_limits_invalid") from exc


@dataclass(frozen=True, slots=True)
class ParentBoundBatchRequest:
    """Trusted typed inputs supplied by the accepted ``batch_task`` boundary."""

    tenant: TenantReferenceV1
    accepted_parent: AcceptedInvocation
    resolved_parent_material: ResolvedAgentMaterialV1 = field(repr=False)
    parent_tool_binding: ToolEvidenceRuntimeBinding = field(repr=False)
    parent_tool_receipt: DurableToolReceiptV1
    parent_tool_receipt_sink: DurableToolReceiptSink = field(
        repr=False,
        compare=False,
    )
    user_id: str
    thread_id: str
    run_id: str
    submission_key: str
    title: str
    subagent_name: str
    items: tuple[BatchItemRequestV1, ...]
    limits: BatchLimitsV1
    parent_cancellable: bool = False
    # Process-local accepted adapters.  They are never serialized by the
    # acceptance model; recovery either reconstructs and revalidates them or
    # fails with execution_material_unavailable.
    app_config: Any | None = field(default=None, repr=False, compare=False)
    extensions: Any | None = field(default=None, repr=False, compare=False)
    authorization_provider: Any | None = field(
        default=None,
        repr=False,
        compare=False,
    )
    invocation_constraints: Any | None = field(
        default=None,
        repr=False,
        compare=False,
    )
    skill_projection_token: Any | None = field(
        default=None,
        repr=False,
        compare=False,
    )

    def __post_init__(self) -> None:
        if not isinstance(self.tenant, TenantReferenceV1):
            _invalid("batch_tenant_invalid")
        if not isinstance(self.accepted_parent, AcceptedInvocation):
            _invalid("parent_not_accepted")
        if self.accepted_parent.tenant != self.tenant:
            _invalid("batch_tenant_mismatch")
        if not isinstance(self.resolved_parent_material, ResolvedAgentMaterialV1):
            _invalid("execution_material_unavailable")
        try:
            resolved_revision = ResolvedAgentRevision.from_material(self.resolved_parent_material)
        except Exception as exc:
            raise BatchAdmissionError("execution_material_unavailable") from exc
        if resolved_revision.digest != self.accepted_parent.agent_revision.digest:
            _invalid("parent_execution_material_mismatch")
        if self.thread_id != self.accepted_parent.thread_id:
            _invalid("batch_parent_thread_mismatch")
        if self.user_id != self.accepted_parent.principal.user_id:
            _invalid("batch_parent_principal_mismatch")
        projection_token = self.skill_projection_token
        if projection_token is not None:
            snapshot = self.resolved_parent_material.skill_snapshot
            snapshot_id = None if snapshot is None else snapshot.snapshot_id
            if (
                not isinstance(projection_token, SkillProjectionConsumerToken)
                or projection_token.user_id != self.user_id
                or projection_token.thread_id != self.thread_id
                or projection_token.run_id != self.run_id
                or projection_token.snapshot_id != snapshot_id
                or type(projection_token.generation) is not int
                or projection_token.generation < 1
            ):
                _invalid("execution_material_unavailable")
        _safe_id(self.thread_id, code="batch_thread_id_invalid")
        _safe_id(self.run_id, code="batch_run_id_invalid")
        _bounded_text(
            self.user_id,
            max_bytes=64,
            code="batch_user_id_invalid",
        )
        _bounded_text(
            self.submission_key,
            max_bytes=256,
            code="batch_submission_key_invalid",
        )
        object.__setattr__(
            self,
            "title",
            _bounded_text(
                self.title,
                max_bytes=MAX_BATCH_TITLE_BYTES,
                code="batch_title_invalid",
            ),
        )
        object.__setattr__(
            self,
            "subagent_name",
            _bounded_text(
                self.subagent_name,
                max_bytes=128,
                code="batch_subagent_name_invalid",
            ),
        )
        if type(self.parent_cancellable) is not bool:
            _invalid("batch_parent_cancellable_invalid")
        if self.parent_cancellable:
            # Parent-run cancellation has no durable batch-control event in
            # this release. Accepting true would promise a cascade that the
            # runtime cannot enforce, so the initial policy is explicitly
            # non-cascading and fail-closed.
            _invalid("batch_parent_cascade_unsupported")
        if not isinstance(self.limits, BatchLimitsV1):
            _invalid("batch_limits_invalid")

        items = tuple(self.items)
        if not items or len(items) > MAX_BATCH_ITEMS or any(not isinstance(item, BatchItemRequestV1) for item in items):
            _invalid("batch_item_count_invalid")
        if len({item.key for item in items}) != len(items):
            _invalid("batch_item_key_conflict")
        object.__setattr__(self, "items", items)

        try:
            accepted_constraints = validate_constraint_fence(
                self.accepted_parent,
                request_digest=None,
                clock=None,
            )
        except ConstraintFenceError as exc:
            raise BatchAdmissionError("batch_constraint_mismatch") from exc
        if accepted_constraints != self.invocation_constraints:
            _invalid("batch_constraint_mismatch")
        constraint_limit = None if accepted_constraints is None else accepted_constraints.max_total_subagents
        if constraint_limit is not None and len(items) > constraint_limit:
            _invalid("batch_constraint_limit_exceeded")

        catalog = self.resolved_parent_material.subagent_catalog
        definition = catalog.get(self.subagent_name)
        if definition is None:
            _invalid("subagent_not_accepted")
        scope = f"subagent:{definition.name}"
        if scope not in self.resolved_parent_material.skill_scopes.scopes:
            _invalid("execution_material_unavailable")

        binding = self.parent_tool_binding
        receipt = self.parent_tool_receipt
        if not isinstance(binding, ToolEvidenceRuntimeBinding):
            _invalid("tool_attempt_not_active")
        if not isinstance(
            self.parent_tool_receipt_sink,
            DurableToolReceiptSink,
        ):
            _invalid("tool_attempt_not_active")
        if not isinstance(receipt, DurableToolReceiptV1) or receipt.phase != "started" or receipt.tool_name != "batch_task":
            _invalid("tool_attempt_not_active")
        context = receipt.context
        if (
            binding.execution_kind != "lead"
            or context.execution_kind != "lead"
            or binding.run_id != self.run_id
            or context.run_id != self.run_id
            or context.execution_task_id != binding.execution_task_id
            or context.owner_id != binding.owner_id
            or context.lease_epoch != binding.lease_epoch
            or context.agent_revision_digest != self.accepted_parent.agent_revision.digest
            or context.agent_revision_digest != binding.agent_revision_digest
            or context.assembly_fingerprint != binding.assembly_fingerprint
            or context.subagent_catalog_digest != catalog.digest
            or context.subagent_catalog_digest != binding.subagent_catalog_digest
            or context.extension_generation != self.accepted_parent.extension_generation
            or context.extension_generation != binding.extension_generation
            or context.capability_manifest_digest != self.accepted_parent.extension_manifest_digest
            or context.artifact_manifest_digest != self.accepted_parent.extension_artifact_manifest_digest
            or context.extension_configuration_digest != self.accepted_parent.extension_configuration_digest
            or binding.capability_manifest_digest != self.accepted_parent.extension_manifest_digest
            or binding.artifact_manifest_digest != self.accepted_parent.extension_artifact_manifest_digest
            or binding.extension_configuration_digest != self.accepted_parent.extension_configuration_digest
            or context.tenant != self.tenant
            or binding.tenant != self.tenant
        ):
            _invalid("tool_attempt_not_active")

    @property
    def selected_definition(self) -> ResolvedSubagentDefinitionV1:
        definition = self.resolved_parent_material.subagent_catalog.get(self.subagent_name)
        if definition is None:  # defended by __post_init__
            raise BatchAdmissionError("subagent_not_accepted")
        return definition

    @property
    def accepted_skill_digests(self) -> tuple[str, ...]:
        return self.resolved_parent_material.skill_scopes.for_scope(f"subagent:{self.subagent_name}")


@dataclass(frozen=True, slots=True)
class AcceptedBatchItemV1:
    version: Literal[1]
    item_id: str
    ordinal: int
    request_digest: str

    def __post_init__(self) -> None:
        if self.version != 1:
            _invalid("batch_item_version_unsupported")
        _safe_id(self.item_id, code="batch_item_id_invalid")
        if type(self.ordinal) is not int or self.ordinal < 0:
            _invalid("batch_item_ordinal_invalid")
        _digest(self.request_digest, code="batch_item_request_digest_invalid")

    @classmethod
    def from_request(
        cls,
        request: BatchItemRequestV1,
        *,
        batch_id: str,
        ordinal: int,
    ) -> AcceptedBatchItemV1:
        identity = canonical_digest(
            {
                "version": 1,
                "domain": "accepted_batch_item_identity",
                "batch_id": batch_id,
                "ordinal": ordinal,
                "key": request.key,
            }
        )
        item_id = f"bi_{identity[:48]}"
        request_digest = canonical_digest(
            {
                "version": 1,
                "domain": "accepted_batch_item_request",
                "item_id": item_id,
                "ordinal": ordinal,
                "key": request.key,
                "prompt": request.prompt,
            }
        )
        return cls(
            version=1,
            item_id=item_id,
            ordinal=ordinal,
            request_digest=request_digest,
        )

    @classmethod
    def from_requests(
        cls,
        requests: Sequence[BatchItemRequestV1],
        *,
        batch_id: str,
    ) -> tuple[AcceptedBatchItemV1, ...]:
        """Build the ordered item commitments stored beside one batch."""

        return tuple(cls.from_request(request, batch_id=batch_id, ordinal=ordinal) for ordinal, request in enumerate(requests))

    @staticmethod
    def root_digest(items: Sequence[AcceptedBatchItemV1]) -> str:
        """Commit an ordered item set without embedding it in batch evidence."""

        return canonical_digest(
            {
                "version": 1,
                "domain": "accepted_batch_item_root",
                "items": [item.to_json() for item in items],
            }
        )

    def to_json(self) -> dict[str, object]:
        return {
            "version": self.version,
            "item_id": self.item_id,
            "ordinal": self.ordinal,
            "request_digest": self.request_digest,
        }

    @classmethod
    def from_json(cls, value: Mapping[str, object]) -> AcceptedBatchItemV1:
        if not isinstance(value, Mapping) or set(value) != {
            "version",
            "item_id",
            "ordinal",
            "request_digest",
        }:
            _invalid("batch_item_acceptance_invalid")
        try:
            return cls(**dict(value))  # type: ignore[arg-type]
        except BatchAdmissionError:
            raise
        except (TypeError, ValueError) as exc:
            raise BatchAdmissionError("batch_item_acceptance_invalid") from exc


_ATTEMPT_TERMINAL_CODES = frozenset(
    {
        "succeeded",
        "execution_failed",
        "lease_expired",
        "cancelled",
        "queue_rejected",
        "policy_stopped",
        "provider_not_qualified",
        "result_too_large",
        "attempt_limit_exhausted",
        "evidence_limit_exhausted",
        "execution_material_unavailable",
    }
)


@dataclass(frozen=True, slots=True)
class BatchAttemptEvidenceV1:
    """Bounded, payload-free evidence for one terminalized item attempt."""

    version: Literal[1]
    batch_id: str
    item_id: str
    attempt_id: str
    acceptance_digest: str
    request_digest: str
    attempt_number: int
    lease_epoch: int
    terminal_code: str
    consumed: bool
    result_digest: str | None
    evidence_digest: str

    def __post_init__(self) -> None:
        if self.version != 1:
            _invalid("batch_attempt_version_unsupported")
        for name in ("batch_id", "item_id", "attempt_id"):
            _safe_id(getattr(self, name), code="batch_attempt_evidence_invalid")
        for name in ("acceptance_digest", "request_digest", "evidence_digest"):
            _digest(getattr(self, name), code="batch_attempt_evidence_invalid")
        if self.result_digest is not None:
            _digest(self.result_digest, code="batch_attempt_evidence_invalid")
        if type(self.attempt_number) is not int or self.attempt_number < 1:
            _invalid("batch_attempt_evidence_invalid")
        if type(self.lease_epoch) is not int or self.lease_epoch < 1:
            _invalid("batch_attempt_evidence_invalid")
        if self.terminal_code not in _ATTEMPT_TERMINAL_CODES:
            _invalid("batch_attempt_terminal_code_invalid")
        if type(self.consumed) is not bool:
            _invalid("batch_attempt_evidence_invalid")
        if self.evidence_digest != canonical_digest(self._digest_projection()):
            _invalid("batch_attempt_evidence_digest_mismatch")
        if len(_canonical_bytes(self.to_persisted_json())) > MAX_BATCH_ATTEMPT_EVIDENCE_BYTES:
            _invalid("batch_attempt_evidence_too_large")

    @classmethod
    def terminal(
        cls,
        *,
        batch_id: str,
        item_id: str,
        attempt_id: str,
        acceptance_digest: str,
        request_digest: str,
        attempt_number: int,
        lease_epoch: int,
        terminal_code: str,
        consumed: bool,
        result_digest: str | None = None,
    ) -> BatchAttemptEvidenceV1:
        values: dict[str, object] = {
            "version": 1,
            "batch_id": batch_id,
            "item_id": item_id,
            "attempt_id": attempt_id,
            "acceptance_digest": acceptance_digest,
            "request_digest": request_digest,
            "attempt_number": attempt_number,
            "lease_epoch": lease_epoch,
            "terminal_code": terminal_code,
            "consumed": consumed,
            "result_digest": result_digest,
        }
        evidence_digest = canonical_digest(
            {
                "version": 1,
                "domain": "subagent_batch_attempt_terminal_evidence",
                "evidence": values,
            }
        )
        return cls(**values, evidence_digest=evidence_digest)  # type: ignore[arg-type]

    def _digest_projection(self) -> dict[str, object]:
        return {
            "version": 1,
            "domain": "subagent_batch_attempt_terminal_evidence",
            "evidence": self.to_persisted_json(include_digest=False),
        }

    def to_persisted_json(self, *, include_digest: bool = True) -> dict[str, object]:
        value: dict[str, object] = {
            "version": self.version,
            "batch_id": self.batch_id,
            "item_id": self.item_id,
            "attempt_id": self.attempt_id,
            "acceptance_digest": self.acceptance_digest,
            "request_digest": self.request_digest,
            "attempt_number": self.attempt_number,
            "lease_epoch": self.lease_epoch,
            "terminal_code": self.terminal_code,
            "consumed": self.consumed,
            "result_digest": self.result_digest,
        }
        if include_digest:
            value["evidence_digest"] = self.evidence_digest
        return value

    @classmethod
    def from_persisted_json(
        cls,
        value: Mapping[str, object],
    ) -> BatchAttemptEvidenceV1:
        expected = {
            "version",
            "batch_id",
            "item_id",
            "attempt_id",
            "acceptance_digest",
            "request_digest",
            "attempt_number",
            "lease_epoch",
            "terminal_code",
            "consumed",
            "result_digest",
            "evidence_digest",
        }
        if not isinstance(value, Mapping) or set(value) != expected:
            _invalid("batch_attempt_evidence_invalid")
        try:
            return cls(**dict(value))  # type: ignore[arg-type]
        except BatchAdmissionError:
            raise
        except (TypeError, ValueError) as exc:
            raise BatchAdmissionError("batch_attempt_evidence_invalid") from exc


@dataclass(frozen=True, slots=True)
class AcceptedBatchV1:
    """Safe immutable evidence for one accepted parent-bound batch."""

    version: Literal[1]
    canonicalization_version: str
    batch_id: str
    tenant: TenantReferenceV1
    parent_run_id: str
    parent_thread_id: str
    parent_invocation_digest: str
    parent_agent_revision_digest: str
    parent_assembly_fingerprint: str
    parent_tool_receipt_id: str
    parent_tool_call_id: str
    parent_tool_attempt: int
    subagent_catalog_digest: str
    subagent_definition_digest: str
    skill_scope_digest: str
    skill_material_digests: tuple[str, ...]
    extension_generation: int
    capability_manifest_digest: str | None
    extension_artifact_manifest_digest: str | None
    extension_configuration_digest: str | None
    model_selector: str | None
    model_constraints_digest: str
    invocation_constraints_digest: str | None
    allowed_tool_names: tuple[str, ...]
    allowed_tool_contract_digests: tuple[str, ...]
    allowed_tool_contract_digest: str
    item_count: int
    item_root_digest: str
    limits: BatchLimitsV1
    parent_cancellable: bool
    acceptance_digest: str

    def __post_init__(self) -> None:
        if self.version != BATCH_ACCEPTANCE_VERSION:
            _invalid("batch_acceptance_version_unsupported")
        if self.canonicalization_version != BATCH_CANONICALIZATION_VERSION:
            _invalid("batch_canonicalization_version_unsupported")
        _safe_id(self.batch_id, code="batch_id_invalid")
        if not isinstance(self.tenant, TenantReferenceV1):
            _invalid("batch_tenant_invalid")
        _safe_id(self.parent_run_id, code="batch_parent_run_invalid")
        _safe_id(self.parent_thread_id, code="batch_parent_thread_invalid")
        if not isinstance(self.parent_tool_receipt_id, str) or _RECEIPT_ID_RE.fullmatch(self.parent_tool_receipt_id) is None:
            _invalid("batch_parent_receipt_invalid")
        _safe_id(
            self.parent_tool_call_id,
            code="batch_parent_tool_call_invalid",
        )
        if type(self.parent_tool_attempt) is not int or self.parent_tool_attempt < 1:
            _invalid("batch_parent_tool_attempt_invalid")
        for name in (
            "parent_invocation_digest",
            "parent_agent_revision_digest",
            "parent_assembly_fingerprint",
            "subagent_catalog_digest",
            "subagent_definition_digest",
            "skill_scope_digest",
            "model_constraints_digest",
            "allowed_tool_contract_digest",
            "item_root_digest",
        ):
            _digest(getattr(self, name), code=f"batch_{name}_invalid")
        if self.invocation_constraints_digest is not None:
            _digest(
                self.invocation_constraints_digest,
                code="batch_invocation_constraints_digest_invalid",
            )
        if type(self.extension_generation) is not int or self.extension_generation < 0:
            _invalid("batch_extension_generation_invalid")
        for digest_name in (
            "capability_manifest_digest",
            "extension_artifact_manifest_digest",
            "extension_configuration_digest",
        ):
            value = getattr(self, digest_name)
            if value is None:
                continue
            raw = value.removeprefix("sha256:")
            _digest(raw, code=f"batch_{digest_name}_invalid")
        if (self.extension_artifact_manifest_digest is None) != (self.extension_configuration_digest is None):
            _invalid("batch_extension_artifact_tuple_invalid")
        if self.model_selector is not None:
            _bounded_text(
                self.model_selector,
                max_bytes=128,
                code="batch_model_selector_invalid",
            )
        skill_digests = tuple(self.skill_material_digests)
        if len(skill_digests) > 64 or len(set(skill_digests)) != len(skill_digests):
            _invalid("batch_skill_material_invalid")
        for digest_value in skill_digests:
            _digest(digest_value, code="batch_skill_material_invalid")
        object.__setattr__(self, "skill_material_digests", skill_digests)
        tools = tuple(self.allowed_tool_names)
        if len(tools) > 512 or len(set(tools)) != len(tools):
            _invalid("batch_tool_contract_invalid")
        for name in tools:
            _bounded_text(name, max_bytes=128, code="batch_tool_contract_invalid")
        object.__setattr__(self, "allowed_tool_names", tools)
        tool_contracts = tuple(self.allowed_tool_contract_digests)
        if len(tool_contracts) != len(tools):
            _invalid("batch_tool_contract_invalid")
        for digest_value in tool_contracts:
            _digest(digest_value, code="batch_tool_contract_invalid")
        object.__setattr__(
            self,
            "allowed_tool_contract_digests",
            tool_contracts,
        )
        if type(self.item_count) is not int or self.item_count < 1 or self.item_count > MAX_BATCH_ITEMS:
            _invalid("batch_item_count_invalid")
        if not isinstance(self.limits, BatchLimitsV1):
            _invalid("batch_limits_invalid")
        if type(self.parent_cancellable) is not bool:
            _invalid("batch_parent_cancellable_invalid")
        expected_tools = canonical_digest(
            {
                "version": 1,
                "domain": "accepted_batch_tool_contract",
                "tools": [
                    {"name": name, "contract_digest": contract_digest}
                    for name, contract_digest in zip(
                        tools,
                        tool_contracts,
                        strict=True,
                    )
                ],
            }
        )
        if self.allowed_tool_contract_digest != expected_tools:
            _invalid("batch_tool_contract_mismatch")
        expected_acceptance = canonical_digest(self._digest_projection())
        if self.acceptance_digest != expected_acceptance:
            _invalid("batch_acceptance_digest_mismatch")
        if len(_canonical_bytes(self.to_persisted_json())) > MAX_BATCH_ACCEPTANCE_BYTES:
            _invalid("batch_acceptance_too_large")

    @classmethod
    def from_parent_request(
        cls,
        request: ParentBoundBatchRequest,
        *,
        batch_id: str,
    ) -> AcceptedBatchV1:
        if not isinstance(request, ParentBoundBatchRequest):
            _invalid("parent_not_accepted")
        _safe_id(batch_id, code="batch_id_invalid")
        definition = request.selected_definition
        accepted_items = AcceptedBatchItemV1.from_requests(
            request.items,
            batch_id=batch_id,
        )
        item_root = AcceptedBatchItemV1.root_digest(accepted_items)
        tools = tuple(definition.tool_names)
        tool_contracts = definition.tool_contract_digests
        if tool_contracts is None:
            if tools:
                _invalid("execution_material_unavailable")
            tool_contracts = ()
        tool_digest = canonical_digest(
            {
                "version": 1,
                "domain": "accepted_batch_tool_contract",
                "tools": [
                    {"name": name, "contract_digest": contract_digest}
                    for name, contract_digest in zip(
                        tools,
                        tool_contracts,
                        strict=True,
                    )
                ],
            }
        )
        skill_digests = tuple(request.accepted_skill_digests)
        skill_scope_digest = canonical_digest(
            {
                "version": 1,
                "scope": f"subagent:{definition.name}",
                "material_digests": list(skill_digests),
            }
        )
        parent_model = request.resolved_parent_material.model_profile.get("name")
        model_selector = definition.model or (parent_model if isinstance(parent_model, str) else None)
        model_constraints_digest = _model_constraints_digest(
            definition,
            request.resolved_parent_material.model_profile,
        )
        raw_constraints = request.accepted_parent.decision_evidence.get("constraints")
        invocation_constraints_digest = None if raw_constraints is None else raw_constraints.get("projection_digest") if isinstance(raw_constraints, Mapping) else None
        receipt_context = request.parent_tool_receipt.context
        values: dict[str, Any] = {
            "version": BATCH_ACCEPTANCE_VERSION,
            "canonicalization_version": BATCH_CANONICALIZATION_VERSION,
            "batch_id": batch_id,
            "tenant": request.tenant,
            "parent_run_id": request.run_id,
            "parent_thread_id": request.thread_id,
            "parent_invocation_digest": request.accepted_parent.runtime_identity_digest,
            "parent_agent_revision_digest": request.accepted_parent.agent_revision.digest,
            "parent_assembly_fingerprint": receipt_context.assembly_fingerprint,
            "parent_tool_receipt_id": request.parent_tool_receipt.receipt_id,
            "parent_tool_call_id": receipt_context.tool_call_id,
            "parent_tool_attempt": receipt_context.attempt,
            "subagent_catalog_digest": request.resolved_parent_material.subagent_catalog.digest,
            "subagent_definition_digest": definition.definition_digest,
            "skill_scope_digest": skill_scope_digest,
            "skill_material_digests": skill_digests,
            "extension_generation": receipt_context.extension_generation,
            "capability_manifest_digest": receipt_context.capability_manifest_digest,
            "extension_artifact_manifest_digest": receipt_context.artifact_manifest_digest,
            "extension_configuration_digest": receipt_context.extension_configuration_digest,
            "model_selector": model_selector,
            "model_constraints_digest": model_constraints_digest,
            "invocation_constraints_digest": invocation_constraints_digest,
            "allowed_tool_names": tools,
            "allowed_tool_contract_digests": tool_contracts,
            "allowed_tool_contract_digest": tool_digest,
            "item_count": len(accepted_items),
            "item_root_digest": item_root,
            "limits": request.limits,
            "parent_cancellable": request.parent_cancellable,
        }
        provisional = cls.__new__(cls)
        for name, value in values.items():
            object.__setattr__(provisional, name, value)
        acceptance_digest = canonical_digest(provisional._digest_projection())
        return cls(**values, acceptance_digest=acceptance_digest)

    def _digest_projection(self) -> dict[str, object]:
        value = self.to_persisted_json(include_digest=False)
        return {
            "version": 1,
            "domain": "accepted_parent_bound_subagent_batch",
            "acceptance": value,
        }

    @property
    def evidence_size_bytes(self) -> int:
        """Serialized size of the safe acceptance evidence."""

        return len(_canonical_bytes(self.to_persisted_json()))

    def to_persisted_json(
        self,
        *,
        include_digest: bool = True,
    ) -> dict[str, object]:
        value: dict[str, object] = {
            "version": self.version,
            "canonicalization_version": self.canonicalization_version,
            "batch_id": self.batch_id,
            "tenant": {
                "version": self.tenant.version,
                "public_ref": self.tenant.public_ref,
                "digest": self.tenant.digest,
            },
            "parent_run_id": self.parent_run_id,
            "parent_thread_id": self.parent_thread_id,
            "parent_invocation_digest": self.parent_invocation_digest,
            "parent_agent_revision_digest": self.parent_agent_revision_digest,
            "parent_assembly_fingerprint": self.parent_assembly_fingerprint,
            "parent_tool_receipt_id": self.parent_tool_receipt_id,
            "parent_tool_call_id": self.parent_tool_call_id,
            "parent_tool_attempt": self.parent_tool_attempt,
            "subagent_catalog_digest": self.subagent_catalog_digest,
            "subagent_definition_digest": self.subagent_definition_digest,
            "skill_scope_digest": self.skill_scope_digest,
            "skill_material_digests": list(self.skill_material_digests),
            "extension_generation": self.extension_generation,
            "capability_manifest_digest": self.capability_manifest_digest,
            "extension_artifact_manifest_digest": self.extension_artifact_manifest_digest,
            "extension_configuration_digest": self.extension_configuration_digest,
            "model_selector": self.model_selector,
            "model_constraints_digest": self.model_constraints_digest,
            "invocation_constraints_digest": self.invocation_constraints_digest,
            "allowed_tool_names": list(self.allowed_tool_names),
            "allowed_tool_contract_digests": list(self.allowed_tool_contract_digests),
            "allowed_tool_contract_digest": self.allowed_tool_contract_digest,
            "item_count": self.item_count,
            "item_root_digest": self.item_root_digest,
            "limits": self.limits.to_json(),
            "parent_cancellable": self.parent_cancellable,
        }
        if include_digest:
            value["acceptance_digest"] = self.acceptance_digest
        return value

    @classmethod
    def from_persisted_json(cls, value: Mapping[str, object]) -> AcceptedBatchV1:
        expected = {
            "version",
            "canonicalization_version",
            "batch_id",
            "tenant",
            "parent_run_id",
            "parent_thread_id",
            "parent_invocation_digest",
            "parent_agent_revision_digest",
            "parent_assembly_fingerprint",
            "parent_tool_receipt_id",
            "parent_tool_call_id",
            "parent_tool_attempt",
            "subagent_catalog_digest",
            "subagent_definition_digest",
            "skill_scope_digest",
            "skill_material_digests",
            "extension_generation",
            "capability_manifest_digest",
            "extension_artifact_manifest_digest",
            "extension_configuration_digest",
            "model_selector",
            "model_constraints_digest",
            "invocation_constraints_digest",
            "allowed_tool_names",
            "allowed_tool_contract_digests",
            "allowed_tool_contract_digest",
            "item_count",
            "item_root_digest",
            "limits",
            "parent_cancellable",
            "acceptance_digest",
        }
        if not isinstance(value, Mapping) or set(value) != expected:
            _invalid("batch_acceptance_fields_invalid")
        raw_tenant = value.get("tenant")
        raw_skills = value.get("skill_material_digests")
        raw_tools = value.get("allowed_tool_names")
        raw_tool_contracts = value.get("allowed_tool_contract_digests")
        raw_limits = value.get("limits")
        if (
            not isinstance(raw_tenant, Mapping)
            or set(raw_tenant) != {"version", "public_ref", "digest"}
            or not isinstance(raw_skills, Sequence)
            or isinstance(raw_skills, str | bytes | bytearray)
            or not isinstance(raw_tools, Sequence)
            or isinstance(raw_tools, str | bytes | bytearray)
            or not isinstance(raw_tool_contracts, Sequence)
            or isinstance(raw_tool_contracts, str | bytes | bytearray)
            or not isinstance(raw_limits, Mapping)
        ):
            _invalid("batch_acceptance_fields_invalid")
        try:
            fields = dict(value)
            fields["tenant"] = TenantReferenceV1(
                version=raw_tenant["version"],  # type: ignore[arg-type]
                public_ref=raw_tenant["public_ref"],  # type: ignore[arg-type]
                digest=raw_tenant["digest"],  # type: ignore[arg-type]
            )
            fields["skill_material_digests"] = tuple(raw_skills)
            fields["allowed_tool_names"] = tuple(raw_tools)
            fields["allowed_tool_contract_digests"] = tuple(raw_tool_contracts)
            fields["limits"] = BatchLimitsV1.from_json(raw_limits)
            return cls(**fields)  # type: ignore[arg-type]
        except BatchAdmissionError:
            raise
        except (KeyError, TypeError, ValueError) as exc:
            raise BatchAdmissionError("batch_acceptance_fields_invalid") from exc


@dataclass(frozen=True, slots=True)
class ParentBoundBatchExecutionV1:
    """Protected immutable material used to reconstruct accepted child work.

    This record is intentionally not an evidence/API projection: it contains
    the selected system prompt and the parent principal needed by the child.
    It still excludes item prompts, results, credentials, provider handles,
    and live Python objects.
    """

    version: Literal[1]
    batch_id: str
    acceptance_digest: str
    user_id: str
    selected_subagent_name: str
    parent_principal: Mapping[str, object]
    parent_origin: Mapping[str, object]
    trusted_context: Mapping[str, object] | None
    agent_id: str
    storage_source: str
    storage_version: str
    model_profile: Mapping[str, object]
    tool_groups: tuple[str, ...]
    parent_tool_names: tuple[str, ...]
    runtime_defaults: Mapping[str, object]
    catalog: ResolvedSubagentCatalogV1
    skill_scopes: ResolvedSkillScopesV1
    skill_projections: tuple[Mapping[str, object], ...]
    skill_snapshot_id: str | None
    skill_snapshot_digest: str | None
    skill_snapshot_file_count: int
    skill_snapshot_total_bytes: int
    accepted_constraints: Mapping[str, object] | None
    execution_digest: str

    def __post_init__(self) -> None:
        if self.version != 1:
            _invalid("batch_execution_version_unsupported")
        _safe_id(self.batch_id, code="batch_id_invalid")
        _digest(self.acceptance_digest, code="batch_acceptance_digest_invalid")
        _bounded_text(self.user_id, max_bytes=64, code="batch_user_id_invalid")
        _bounded_text(
            self.selected_subagent_name,
            max_bytes=128,
            code="batch_subagent_name_invalid",
        )
        for field_name, value in (
            ("agent_id", self.agent_id),
            ("storage_source", self.storage_source),
            ("storage_version", self.storage_version),
        ):
            _bounded_text(
                value,
                max_bytes=256,
                code=f"batch_{field_name}_invalid",
            )
        if not isinstance(self.catalog, ResolvedSubagentCatalogV1):
            _invalid("batch_catalog_invalid")
        if not isinstance(self.skill_scopes, ResolvedSkillScopesV1):
            _invalid("batch_skill_scopes_invalid")
        definition = self.catalog.get(self.selected_subagent_name)
        if definition is None:
            _invalid("subagent_not_accepted")
        expected_scopes = {
            "lead",
            *(f"subagent:{name}" for name in self.catalog.allowed_names),
        }
        if set(self.skill_scopes.scopes) != expected_scopes:
            _invalid("batch_skill_scopes_invalid")
        for field_name in (
            "parent_principal",
            "parent_origin",
            "model_profile",
            "runtime_defaults",
        ):
            value = getattr(self, field_name)
            if not isinstance(value, Mapping):
                _invalid("batch_execution_fields_invalid")
            object.__setattr__(self, field_name, _frozen_mapping(value))
        if self.trusted_context is not None:
            if not isinstance(self.trusted_context, Mapping):
                _invalid("batch_trusted_context_invalid")
            object.__setattr__(
                self,
                "trusted_context",
                _frozen_mapping(self.trusted_context),
            )
        if self.accepted_constraints is not None:
            if not isinstance(self.accepted_constraints, Mapping):
                _invalid("batch_constraints_invalid")
            _constraint_from_evidence(self.accepted_constraints)
            object.__setattr__(
                self,
                "accepted_constraints",
                _frozen_mapping(self.accepted_constraints),
            )
        tool_groups = _bounded_names(self.tool_groups, max_count=128)
        parent_tool_names = _bounded_names(self.parent_tool_names, max_count=512)
        object.__setattr__(self, "tool_groups", tool_groups)
        object.__setattr__(self, "parent_tool_names", parent_tool_names)
        projections: list[Mapping[str, object]] = []
        if len(self.skill_projections) > 64:
            _invalid("batch_skill_material_invalid")
        for projection in self.skill_projections:
            if not isinstance(projection, Mapping):
                _invalid("batch_skill_material_invalid")
            projections.append(_frozen_mapping(projection))
        object.__setattr__(self, "skill_projections", tuple(projections))
        if (self.skill_snapshot_id is None) != (self.skill_snapshot_digest is None):
            _invalid("batch_skill_snapshot_invalid")
        if self.skill_snapshot_id is not None:
            _digest(self.skill_snapshot_id, code="batch_skill_snapshot_invalid")
            _digest(self.skill_snapshot_digest, code="batch_skill_snapshot_invalid")
        for value in (
            self.skill_snapshot_file_count,
            self.skill_snapshot_total_bytes,
        ):
            if type(value) is not int or value < 0:
                _invalid("batch_skill_snapshot_invalid")
        if self.skill_snapshot_id is None and (self.skill_snapshot_file_count != 0 or self.skill_snapshot_total_bytes != 0 or self.skill_projections):
            _invalid("batch_skill_snapshot_invalid")
        _digest(self.execution_digest, code="batch_execution_digest_invalid")
        if self.execution_digest != canonical_digest(self._digest_projection()):
            _invalid("batch_execution_digest_mismatch")
        if len(_canonical_bytes(self.to_persisted_json())) > MAX_BATCH_EXECUTION_BYTES:
            _invalid("batch_execution_too_large")

    @property
    def selected_definition(self) -> ResolvedSubagentDefinitionV1:
        definition = self.catalog.get(self.selected_subagent_name)
        if definition is None:  # defended by __post_init__
            raise BatchAdmissionError("subagent_not_accepted")
        return definition

    @property
    def constraint_projection(
        self,
    ) -> ConstraintProjectionV1 | ConstraintProjectionV2 | None:
        return _constraint_from_evidence(self.accepted_constraints)

    def validate_constraint_freshness(
        self,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        """Recheck accepted policy time bounds immediately before child work."""

        projection = self.constraint_projection
        if projection is None:
            return
        now = datetime.now(UTC) if clock is None else clock()
        if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() is None or projection.issued_at > now + timedelta(seconds=30):
            _invalid("execution_material_unavailable")
        if projection.valid_until <= now:
            _invalid("policy_stopped")

    def verify_against_acceptance(self, accepted: AcceptedBatchV1) -> None:
        """Verify protected execution material against its safe commitment."""

        definition = self.selected_definition
        skill_digests = self.skill_scopes.for_scope(f"subagent:{self.selected_subagent_name}")
        skill_scope_digest = canonical_digest(
            {
                "version": 1,
                "scope": f"subagent:{self.selected_subagent_name}",
                "material_digests": list(skill_digests),
            }
        )
        raw_constraints_digest = None if self.accepted_constraints is None else self.accepted_constraints.get("projection_digest")
        parent_model = self.model_profile.get("name")
        model_selector = definition.model or (parent_model if isinstance(parent_model, str) else None)
        if (
            not isinstance(accepted, AcceptedBatchV1)
            or self.batch_id != accepted.batch_id
            or self.acceptance_digest != accepted.acceptance_digest
            or self.catalog.digest != accepted.subagent_catalog_digest
            or definition.definition_digest != accepted.subagent_definition_digest
            or tuple(skill_digests) != accepted.skill_material_digests
            or skill_scope_digest != accepted.skill_scope_digest
            or model_selector != accepted.model_selector
            or _model_constraints_digest(definition, self.model_profile) != accepted.model_constraints_digest
            or raw_constraints_digest != accepted.invocation_constraints_digest
            or tuple(definition.tool_names) != accepted.allowed_tool_names
            or tuple(definition.tool_contract_digests or ()) != accepted.allowed_tool_contract_digests
        ):
            _invalid("batch_acceptance_mismatch")

    @classmethod
    def from_parent_request(
        cls,
        request: ParentBoundBatchRequest,
        *,
        accepted: AcceptedBatchV1,
    ) -> ParentBoundBatchExecutionV1:
        if not isinstance(accepted, AcceptedBatchV1):
            _invalid("batch_acceptance_mismatch")
        # Recompute the safe acceptance from the same trusted request. This is
        # deliberately the only cross-object comparison: callers cannot pair a
        # protected execution record with acceptance for different items,
        # limits, parent evidence, or selected material.
        expected_acceptance = AcceptedBatchV1.from_parent_request(
            request,
            batch_id=accepted.batch_id,
        )
        if expected_acceptance != accepted:
            _invalid("batch_acceptance_mismatch")
        material = request.resolved_parent_material
        snapshot = material.skill_snapshot
        trusted_context = request.accepted_parent.trusted_context
        if trusted_context is not None and (trusted_context.runtime_reference_count and not trusted_context.runtime_state_complete):
            _invalid("execution_material_unavailable")
        values: dict[str, Any] = {
            "version": 1,
            "batch_id": accepted.batch_id,
            "acceptance_digest": accepted.acceptance_digest,
            "user_id": request.user_id,
            "selected_subagent_name": request.subagent_name,
            "parent_principal": request.accepted_parent.principal.to_json(),
            "parent_origin": request.accepted_parent.origin.to_json(),
            "trusted_context": (None if trusted_context is None else trusted_context.to_persisted_json()),
            "agent_id": material.agent_id,
            "storage_source": material.storage_source,
            "storage_version": material.storage_version,
            "model_profile": dict(material.model_profile),
            "tool_groups": tuple(material.tool_groups),
            "parent_tool_names": tuple(material.tools),
            "runtime_defaults": dict(material.runtime_defaults),
            "catalog": material.subagent_catalog,
            "skill_scopes": material.skill_scopes,
            "skill_projections": tuple(projection.to_json() for projection in (() if snapshot is None else snapshot.projections)),
            "skill_snapshot_id": (None if snapshot is None else snapshot.snapshot_id),
            "skill_snapshot_digest": (None if snapshot is None else snapshot.content_digest),
            "skill_snapshot_file_count": (0 if snapshot is None else snapshot.file_count),
            "skill_snapshot_total_bytes": (0 if snapshot is None else snapshot.total_bytes),
            "accepted_constraints": (
                dict(request.accepted_parent.decision_evidence["constraints"])
                if isinstance(
                    request.accepted_parent.decision_evidence.get("constraints"),
                    Mapping,
                )
                else None
            ),
        }
        execution_digest = canonical_digest(
            {
                "version": 1,
                "domain": "parent_bound_batch_execution",
                "execution": _execution_json(values),
            }
        )
        return cls(**values, execution_digest=execution_digest)

    def _digest_projection(self) -> dict[str, object]:
        return {
            "version": 1,
            "domain": "parent_bound_batch_execution",
            "execution": self.to_persisted_json(include_digest=False),
        }

    def to_persisted_json(
        self,
        *,
        include_digest: bool = True,
    ) -> dict[str, object]:
        value: dict[str, object] = {
            "version": self.version,
            "batch_id": self.batch_id,
            "acceptance_digest": self.acceptance_digest,
            "user_id": self.user_id,
            "selected_subagent_name": self.selected_subagent_name,
            "parent_principal": _thaw_json(self.parent_principal),
            "parent_origin": _thaw_json(self.parent_origin),
            "trusted_context": _thaw_json(self.trusted_context),
            "agent_id": self.agent_id,
            "storage_source": self.storage_source,
            "storage_version": self.storage_version,
            "model_profile": _thaw_json(self.model_profile),
            "tool_groups": list(self.tool_groups),
            "parent_tool_names": list(self.parent_tool_names),
            "runtime_defaults": _thaw_json(self.runtime_defaults),
            "catalog": self.catalog.to_persisted_json(),
            "skill_scopes": self.skill_scopes.to_persisted_json(),
            "skill_projections": [_thaw_json(projection) for projection in self.skill_projections],
            "skill_snapshot_id": self.skill_snapshot_id,
            "skill_snapshot_digest": self.skill_snapshot_digest,
            "skill_snapshot_file_count": self.skill_snapshot_file_count,
            "skill_snapshot_total_bytes": self.skill_snapshot_total_bytes,
            "accepted_constraints": _thaw_json(self.accepted_constraints),
        }
        if include_digest:
            value["execution_digest"] = self.execution_digest
        return value

    @classmethod
    def from_persisted_json(
        cls,
        value: Mapping[str, object],
    ) -> ParentBoundBatchExecutionV1:
        expected = {
            "version",
            "batch_id",
            "acceptance_digest",
            "user_id",
            "selected_subagent_name",
            "parent_principal",
            "parent_origin",
            "trusted_context",
            "agent_id",
            "storage_source",
            "storage_version",
            "model_profile",
            "tool_groups",
            "parent_tool_names",
            "runtime_defaults",
            "catalog",
            "skill_scopes",
            "skill_projections",
            "skill_snapshot_id",
            "skill_snapshot_digest",
            "skill_snapshot_file_count",
            "skill_snapshot_total_bytes",
            "accepted_constraints",
            "execution_digest",
        }
        if not isinstance(value, Mapping) or set(value) != expected:
            _invalid("batch_execution_fields_invalid")
        sequence_fields = (
            "tool_groups",
            "parent_tool_names",
            "skill_projections",
        )
        if any(not isinstance(value.get(name), Sequence) or isinstance(value.get(name), str | bytes | bytearray) for name in sequence_fields):
            _invalid("batch_execution_fields_invalid")
        for name in (
            "parent_principal",
            "parent_origin",
            "model_profile",
            "runtime_defaults",
            "catalog",
            "skill_scopes",
        ):
            if not isinstance(value.get(name), Mapping):
                _invalid("batch_execution_fields_invalid")
        for name in ("trusted_context", "accepted_constraints"):
            if value.get(name) is not None and not isinstance(value.get(name), Mapping):
                _invalid("batch_execution_fields_invalid")
        try:
            fields = dict(value)
            fields["tool_groups"] = tuple(fields["tool_groups"])
            fields["parent_tool_names"] = tuple(fields["parent_tool_names"])
            fields["skill_projections"] = tuple(fields["skill_projections"])
            fields["catalog"] = ResolvedSubagentCatalogV1.from_persisted_json(fields["catalog"])
            fields["skill_scopes"] = ResolvedSkillScopesV1.from_persisted_json(fields["skill_scopes"])
            return cls(**fields)  # type: ignore[arg-type]
        except BatchAdmissionError:
            raise
        except Exception as exc:
            raise BatchAdmissionError("batch_execution_fields_invalid") from exc


def _execution_json(values: Mapping[str, object]) -> dict[str, object]:
    result = dict(values)
    catalog = result.get("catalog")
    scopes = result.get("skill_scopes")
    if isinstance(catalog, ResolvedSubagentCatalogV1):
        result["catalog"] = catalog.to_persisted_json()
    if isinstance(scopes, ResolvedSkillScopesV1):
        result["skill_scopes"] = scopes.to_persisted_json()
    for name in ("tool_groups", "parent_tool_names", "skill_projections"):
        if isinstance(result.get(name), tuple):
            result[name] = [
                _thaw_json(item)
                for item in result[name]  # type: ignore[union-attr]
            ]
    return _thaw_json(result)


def _frozen_mapping(value: Mapping[str, object]) -> Mapping[str, object]:
    # Accepted parent material is recursively frozen with MappingProxyType and
    # tuples. Thaw it before the strict JSON round-trip so nested model/runtime
    # settings remain serializable without weakening the canonical validator.
    plain = json.loads(_canonical_bytes(_thaw_json(value)))
    return _freeze_json(plain)


def _freeze_json(value: object) -> object:
    from types import MappingProxyType

    if isinstance(value, Mapping):
        return MappingProxyType({str(key): _freeze_json(child) for key, child in value.items()})
    if isinstance(value, list | tuple):
        return tuple(_freeze_json(child) for child in value)
    return value


def _thaw_json(value: object) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _thaw_json(child) for key, child in value.items()}
    if isinstance(value, tuple | list):
        return [_thaw_json(child) for child in value]
    return value


def _bounded_names(
    values: Sequence[str],
    *,
    max_count: int,
) -> tuple[str, ...]:
    if not isinstance(values, Sequence) or isinstance(values, str | bytes | bytearray) or len(values) > max_count:
        _invalid("batch_execution_fields_invalid")
    names = tuple(values)
    if len(names) != len(set(names)):
        _invalid("batch_execution_fields_invalid")
    for name in names:
        _bounded_text(
            name,
            max_bytes=128,
            code="batch_execution_fields_invalid",
        )
    return names


@runtime_checkable
class AcceptedParentBatchService(Protocol):
    async def accept(self, request: ParentBoundBatchRequest) -> dict[str, Any]: ...

    async def load_execution(self, batch_id: str) -> object: ...


__all__ = [
    "AcceptedBatchItemV1",
    "AcceptedBatchV1",
    "AcceptedParentBatchService",
    "BATCH_ACCEPTANCE_VERSION",
    "BATCH_CANONICALIZATION_VERSION",
    "BatchAdmissionConflict",
    "BatchAdmissionError",
    "BatchAttemptEvidenceV1",
    "BatchItemRequestV1",
    "BatchLeaseLost",
    "BatchLimitsV1",
    "PARENT_BATCH_ACCEPTANCE_CONTEXT_KEY",
    "ParentBoundBatchRequest",
    "ParentBoundBatchExecutionV1",
    "strip_parent_batch_acceptance_context",
]
