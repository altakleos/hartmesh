"""Bounded durable evidence for tool attempts.

This module is the transport-neutral contract between agent middleware and the
run event ledger.  Durable receipts deliberately contain commitments and
small, classified facts only; raw tool arguments, results, and exception text
never enter an event body.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import re
import threading
from collections.abc import AsyncIterator, Iterable, Iterator, Mapping
from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import Any, Literal, Protocol, runtime_checkable

from deerflow_extension_api import TenantReferenceV1

from deerflow.runtime.events.catalog import (
    TOOL_RECEIPT_OUTCOME_EVENT as _TOOL_RECEIPT_OUTCOME_DEFINITION,
)
from deerflow.runtime.events.catalog import (
    TOOL_RECEIPT_STARTED_EVENT as _TOOL_RECEIPT_STARTED_DEFINITION,
)

TOOL_RECEIPT_STARTED_EVENT = _TOOL_RECEIPT_STARTED_DEFINITION.event_type
TOOL_RECEIPT_OUTCOME_EVENT = _TOOL_RECEIPT_OUTCOME_DEFINITION.event_type
TOOL_RECEIPT_CATEGORY = _TOOL_RECEIPT_STARTED_DEFINITION.category

TOOL_EVIDENCE_CONTEXT_KEY = "__deerflow_tool_evidence_context_v1"
TOOL_EVIDENCE_SINK_KEY = "__deerflow_tool_evidence_sink_v1"

MAX_TOOL_NAME_BYTES = 128
MAX_DECISION_REF_BYTES = 128
MAX_DECISION_REFS = 16
MAX_EVENT_BODY_BYTES = 8 * 1024
MAX_PROJECTION_DEPTH = 4
MAX_PROJECTION_FIELDS = 64
MAX_PROJECTION_ITEMS = 64
MAX_EVIDENCE_SAFE_STRING_BYTES = 256

_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
_RECEIPT_ID_RE = re.compile(r"^tr_[0-9a-f]{64}$")
_DECISION_REF_RE = re.compile(r"^pd_[0-9a-f]{64}$")
_SAFE_TOOL_NAME_RE = re.compile(r"^[A-Za-z0-9_.:/-]+$")
_CAMEL_ACRONYM_BOUNDARY_RE = re.compile(r"([A-Z]+)([A-Z][a-z])")
_CAMEL_WORD_BOUNDARY_RE = re.compile(r"([a-z0-9])([A-Z])")
_NON_ALNUM_RE = re.compile(r"[^A-Za-z0-9]+")
_SECRET_FIELD_TOKENS = frozenset(
    {
        "auth",
        "authorization",
        "cookie",
        "credential",
        "credentials",
        "header",
        "headers",
        "passwd",
        "password",
        "pwd",
        "secret",
        "session",
        "token",
    }
)
_SECRET_FIELD_TOKEN_PAIRS = frozenset(
    {
        ("access", "key"),
        ("api", "key"),
        ("private", "key"),
        ("secret", "key"),
        ("session", "key"),
    }
)
_SECRET_FIELD_COMPOUNDS = frozenset(
    {
        "accesskey",
        "apikey",
        "privatekey",
        "secretkey",
        "sessionkey",
    }
)
_SECRET_FIELD_COMPACT_TERMS = frozenset(
    {
        "auth",
        "authorization",
        "cookie",
        "credential",
        "credentials",
        "header",
        "headers",
        "passwd",
        "password",
        "pwd",
        "secret",
        "session",
        "token",
    }
)

SAFE_ERROR_CODES = frozenset(
    {
        "authorization_denied",
        "guardrail_denied",
        "cancelled",
        "timeout",
        "tool_error",
        "invalid_input",
        "permission_denied",
        "rate_limited",
        "transient_error",
        "configuration_error",
        "not_found",
        "no_results",
        "internal_error",
        "unknown_error",
    }
)

ToolExecutionKind = Literal["lead", "subagent"]
ToolReceiptPhase = Literal["started", "succeeded", "failed", "denied", "cancelled"]


class ToolEvidenceError(ValueError):
    """A bounded machine-code failure in durable tool evidence handling."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


class ToolReceiptIntegrityError(ToolEvidenceError):
    """A duplicate durable identity disagreed with the stored fact."""


class ToolReceiptOwnershipLost(ToolEvidenceError):
    """The writer no longer owns the run execution fence."""


def _canonical_json_bytes(value: object) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ToolEvidenceError("evidence_not_canonical_json") from exc


def canonical_digest(value: object) -> str:
    return hashlib.sha256(_canonical_json_bytes(value)).hexdigest()


@dataclass(frozen=True, slots=True)
class ToolDispatchObservationV1:
    """One process-local observation of a LangGraph tool dispatch."""

    lineage_digest: str
    node_attempt: int

    def __post_init__(self) -> None:
        _require_digest(self.lineage_digest, "dispatch_lineage_digest_invalid")
        if type(self.node_attempt) is not int or self.node_attempt < 1:
            raise ToolEvidenceError("node_attempt_invalid")


def observe_tool_dispatch(
    *,
    checkpoint_id: str,
    checkpoint_ns: str,
    task_id: str,
    node_attempt: int,
) -> ToolDispatchObservationV1:
    """Validate public LangGraph retry information as a local observation.

    ``node_attempt`` may restart when execution is reconstructed. Durable
    attempt numbering therefore remains store-owned; the lineage and counter
    only let one live binding translate subsequent local retries.
    """

    if not isinstance(checkpoint_id, str) or len(checkpoint_id.encode("utf-8")) > 256:
        raise ToolEvidenceError("checkpoint_id_invalid")
    if not isinstance(checkpoint_ns, str) or len(checkpoint_ns.encode("utf-8")) > 512:
        raise ToolEvidenceError("checkpoint_ns_invalid")
    _require_nonempty(task_id, "dispatch_task_id_invalid", max_bytes=256)
    if type(node_attempt) is not int or node_attempt < 1:
        raise ToolEvidenceError("node_attempt_invalid")
    return ToolDispatchObservationV1(
        lineage_digest=canonical_digest(
            {
                "version": 1,
                "checkpoint_id": checkpoint_id,
                "checkpoint_ns": checkpoint_ns,
                "task_id": task_id,
            }
        ),
        node_attempt=node_attempt,
    )


def _require_nonempty(value: object, code: str, *, max_bytes: int) -> str:
    if not isinstance(value, str) or not value or len(value.encode("utf-8")) > max_bytes:
        raise ToolEvidenceError(code)
    return value


def _require_digest(value: object, code: str) -> str:
    if not isinstance(value, str) or _DIGEST_RE.fullmatch(value) is None:
        raise ToolEvidenceError(code)
    return value


def _validate_tool_name(value: object) -> str:
    name = _require_nonempty(value, "tool_name_invalid", max_bytes=MAX_TOOL_NAME_BYTES)
    if _SAFE_TOOL_NAME_RE.fullmatch(name) is None:
        raise ToolEvidenceError("tool_name_invalid")
    return name


def _is_secret_field_name(field_name: str) -> bool:
    """Recognize delimited, camelCase, and collapsed secret field names."""

    separated = _CAMEL_ACRONYM_BOUNDARY_RE.sub(r"\1_\2", field_name)
    separated = _CAMEL_WORD_BOUNDARY_RE.sub(r"\1_\2", separated)
    tokens = tuple(token for token in _NON_ALNUM_RE.split(separated.lower()) if token)
    if any(token in _SECRET_FIELD_TOKENS for token in tokens):
        return True
    adjacent = set(zip(tokens, tokens[1:], strict=False))
    if not adjacent.isdisjoint(_SECRET_FIELD_TOKEN_PAIRS):
        return True
    compact = "".join(tokens)
    return any(compound in compact for compound in _SECRET_FIELD_COMPOUNDS) or any(term in compact for term in _SECRET_FIELD_COMPACT_TERMS)


@dataclass(frozen=True, slots=True)
class ToolAttemptContextV1:
    run_id: str
    execution_task_id: str
    execution_kind: ToolExecutionKind
    subagent_name: str | None
    tool_call_id: str
    attempt: int
    owner_id: str
    lease_epoch: int
    agent_revision_digest: str
    assembly_fingerprint: str
    extension_generation: int
    subagent_catalog_digest: str
    subagent_definition_digest: str | None
    capability_manifest_digest: str | None = None
    artifact_manifest_digest: str | None = None
    extension_configuration_digest: str | None = None
    tenant: TenantReferenceV1 | None = None

    def __post_init__(self) -> None:
        _require_nonempty(self.run_id, "run_id_invalid", max_bytes=64)
        _require_nonempty(self.execution_task_id, "execution_task_id_invalid", max_bytes=128)
        _require_nonempty(self.tool_call_id, "tool_call_id_invalid", max_bytes=256)
        _require_nonempty(self.owner_id, "owner_id_invalid", max_bytes=128)
        if self.execution_kind not in ("lead", "subagent"):
            raise ToolEvidenceError("execution_kind_invalid")
        if self.execution_kind == "lead":
            if self.subagent_name is not None or self.subagent_definition_digest is not None:
                raise ToolEvidenceError("lead_subagent_anchor_invalid")
        else:
            _require_nonempty(self.subagent_name, "subagent_name_invalid", max_bytes=128)
            _require_digest(self.subagent_definition_digest, "subagent_definition_digest_invalid")
        if type(self.attempt) is not int or self.attempt < 1:
            raise ToolEvidenceError("attempt_invalid")
        if type(self.lease_epoch) is not int or self.lease_epoch < 0:
            raise ToolEvidenceError("lease_epoch_invalid")
        if type(self.extension_generation) is not int or self.extension_generation < 0:
            raise ToolEvidenceError("extension_generation_invalid")
        _require_digest(self.agent_revision_digest, "agent_revision_digest_invalid")
        _require_digest(self.assembly_fingerprint, "assembly_fingerprint_invalid")
        _require_digest(self.subagent_catalog_digest, "subagent_catalog_digest_invalid")
        if self.tenant is not None and not isinstance(
            self.tenant,
            TenantReferenceV1,
        ):
            raise ToolEvidenceError("tenant_anchor_invalid")
        if self.capability_manifest_digest is not None:
            _require_digest(
                self.capability_manifest_digest,
                "capability_manifest_digest_invalid",
            )
        for digest, code in (
            (self.artifact_manifest_digest, "artifact_manifest_digest_invalid"),
            (
                self.extension_configuration_digest,
                "extension_configuration_digest_invalid",
            ),
        ):
            if digest is not None:
                if not isinstance(digest, str) or not digest.startswith("sha256:"):
                    raise ToolEvidenceError(code)
                _require_digest(digest.removeprefix("sha256:"), code)
        if (self.artifact_manifest_digest is None) != (self.extension_configuration_digest is None):
            raise ToolEvidenceError("extension_artifact_tuple_invalid")
        if self.artifact_manifest_digest is not None and self.capability_manifest_digest is None:
            raise ToolEvidenceError("extension_artifact_tuple_invalid")

    def to_dict(self) -> dict[str, object]:
        result: dict[str, object] = {
            "run_id": self.run_id,
            "execution_task_id": self.execution_task_id,
            "execution_kind": self.execution_kind,
            "subagent_name": self.subagent_name,
            "tool_call_id": self.tool_call_id,
            "attempt": self.attempt,
            "owner_id": self.owner_id,
            "lease_epoch": self.lease_epoch,
            "agent_revision_digest": self.agent_revision_digest,
            "assembly_fingerprint": self.assembly_fingerprint,
            "extension_generation": self.extension_generation,
            "subagent_catalog_digest": self.subagent_catalog_digest,
            "subagent_definition_digest": self.subagent_definition_digest,
        }
        if self.tenant is not None:
            result["tenant_ref"] = self.tenant.public_ref
            result["tenant_digest"] = self.tenant.digest
        if self.artifact_manifest_digest is not None:
            result["capability_manifest_digest"] = self.capability_manifest_digest
            result["artifact_manifest_digest"] = self.artifact_manifest_digest
            result["extension_configuration_digest"] = self.extension_configuration_digest
        return result

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> ToolAttemptContextV1:
        expected = {
            "run_id",
            "execution_task_id",
            "execution_kind",
            "subagent_name",
            "tool_call_id",
            "attempt",
            "owner_id",
            "lease_epoch",
            "agent_revision_digest",
            "assembly_fingerprint",
            "extension_generation",
            "subagent_catalog_digest",
            "subagent_definition_digest",
        }
        tenant_fields = {"tenant_ref", "tenant_digest"}
        artifact_fields = {
            "capability_manifest_digest",
            "artifact_manifest_digest",
            "extension_configuration_digest",
        }
        if set(value) not in (
            expected,
            expected | tenant_fields,
            expected | tenant_fields | artifact_fields,
        ):
            raise ToolEvidenceError("attempt_context_fields_invalid")
        fields = dict(value)
        tenant = None
        if tenant_fields.issubset(value):
            try:
                tenant = TenantReferenceV1(
                    version=1,
                    public_ref=fields.pop("tenant_ref"),  # type: ignore[arg-type]
                    digest=fields.pop("tenant_digest"),  # type: ignore[arg-type]
                )
            except (TypeError, ValueError) as exc:
                raise ToolEvidenceError("tenant_anchor_invalid") from exc
        return cls(**fields, tenant=tenant)  # type: ignore[arg-type]


def stable_receipt_id(context: ToolAttemptContextV1) -> str:
    identity = {
        "version": 2 if context.tenant is not None else 1,
        "run_id": context.run_id,
        "execution_task_id": context.execution_task_id,
        "tool_call_id": context.tool_call_id,
        "attempt": context.attempt,
    }
    if context.tenant is not None:
        identity["tenant_digest"] = context.tenant.digest
    return f"tr_{canonical_digest(identity)}"


def tool_dispatch_generation_digest(context: ToolAttemptContextV1) -> str:
    """Derive the store-owned generation token for one durable attempt."""

    if not isinstance(context, ToolAttemptContextV1):
        raise ToolEvidenceError("attempt_context_invalid")
    projection = {
        "version": 2 if context.tenant is not None else 1,
        "domain": "durable_tool_dispatch_generation",
        "run_id": context.run_id,
        "execution_task_id": context.execution_task_id,
        "tool_call_id": context.tool_call_id,
        "attempt": context.attempt,
    }
    if context.tenant is not None:
        projection["tenant_digest"] = context.tenant.digest
    return canonical_digest(projection)


def stable_subagent_task_id(
    parent: ToolEvidenceRuntimeBinding,
    *,
    parent_tool_call_id: str,
    subagent_name: str,
) -> str:
    """Derive a process-independent child task identity from accepted facts."""

    if not isinstance(parent, ToolEvidenceRuntimeBinding):
        raise ToolEvidenceError("tool_evidence_parent_binding_invalid")
    _require_nonempty(parent_tool_call_id, "tool_call_id_invalid", max_bytes=256)
    _require_nonempty(subagent_name, "subagent_name_invalid", max_bytes=128)
    projection = {
        "version": 2 if parent.tenant is not None else 1,
        "run_id": parent.run_id,
        "parent_execution_task_id": parent.execution_task_id,
        "parent_tool_call_id": parent_tool_call_id,
        "subagent_name": subagent_name,
    }
    if parent.tenant is not None:
        projection["tenant_digest"] = parent.tenant.digest
    return "st_" + canonical_digest(projection)


def _json_type(value: object) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, Mapping):
        return "object"
    if isinstance(value, (list, tuple)):
        return "array"
    raise ToolEvidenceError("argument_type_unsupported")


def _safe_scalar(value: object) -> object:
    if isinstance(value, str):
        if len(value.encode("utf-8")) > MAX_EVIDENCE_SAFE_STRING_BYTES:
            raise ToolEvidenceError("evidence_safe_value_too_long")
        return value
    if value is None or isinstance(value, (bool, int)):
        return value
    if isinstance(value, float) and math.isfinite(value):
        return value
    raise ToolEvidenceError("evidence_safe_value_not_scalar")


def _project_value(
    value: object,
    *,
    path: str,
    field_name: str,
    evidence_safe_fields: frozenset[str],
    depth: int,
) -> dict[str, object]:
    if depth > MAX_PROJECTION_DEPTH:
        raise ToolEvidenceError("projection_too_deep")
    value_type = _json_type(value)
    if _is_secret_field_name(field_name):
        return {"classification": "secret_handle", "type": value_type}
    if path in evidence_safe_fields:
        return {
            "classification": "evidence_safe",
            "type": value_type,
            "value": _safe_scalar(value),
        }
    if isinstance(value, Mapping):
        if len(value) > MAX_PROJECTION_FIELDS:
            raise ToolEvidenceError("projection_too_many_fields")
        fields: dict[str, object] = {}
        for raw_key, child in value.items():
            if not isinstance(raw_key, str) or not raw_key or len(raw_key.encode("utf-8")) > 128:
                raise ToolEvidenceError("argument_field_name_invalid")
            child_path = f"{path}.{raw_key}" if path else raw_key
            fields[raw_key] = _project_value(
                child,
                path=child_path,
                field_name=raw_key,
                evidence_safe_fields=evidence_safe_fields,
                depth=depth + 1,
            )
        return {"classification": "shape", "type": "object", "fields": fields}
    if isinstance(value, (list, tuple)):
        if len(value) > MAX_PROJECTION_ITEMS:
            raise ToolEvidenceError("projection_too_many_items")
        return {
            "classification": "shape",
            "type": "array",
            "length": len(value),
            "items": [
                _project_value(
                    child,
                    path=f"{path}[]",
                    field_name=field_name,
                    evidence_safe_fields=evidence_safe_fields,
                    depth=depth + 1,
                )
                for child in value
            ],
        }
    marker: dict[str, object] = {"classification": "shape", "type": value_type}
    if isinstance(value, str):
        marker["utf8_bytes"] = len(value.encode("utf-8"))
    return marker


def build_request_projection(
    tool_name: str,
    arguments: Mapping[str, object],
    *,
    evidence_safe_fields: frozenset[str] = frozenset(),
) -> dict[str, object]:
    """Return a canonical-safe request shape without raw unclassified values."""

    try:
        name = _validate_tool_name(tool_name)
    except ToolEvidenceError as exc:
        if isinstance(tool_name, str) and len(tool_name.encode("utf-8")) > MAX_TOOL_NAME_BYTES:
            raise ToolEvidenceError("tool_name_too_long") from exc
        raise
    if not isinstance(arguments, Mapping):
        raise ToolEvidenceError("arguments_not_object")
    if not isinstance(evidence_safe_fields, frozenset) or any(not isinstance(item, str) for item in evidence_safe_fields):
        raise ToolEvidenceError("evidence_safe_policy_invalid")
    projection = _project_value(
        arguments,
        path="",
        field_name="arguments",
        evidence_safe_fields=evidence_safe_fields,
        depth=0,
    )
    seen_paths: set[str] = set()

    def _walk(value: object, prefix: str = "") -> None:
        if not isinstance(value, Mapping):
            return
        for key, child in value.items():
            path = f"{prefix}.{key}" if prefix else str(key)
            seen_paths.add(path)
            if isinstance(child, Mapping):
                _walk(child, path)

    _walk(arguments)
    if not evidence_safe_fields.issubset(seen_paths):
        raise ToolEvidenceError("evidence_safe_field_unknown")
    result = {"version": 1, "tool_name": name, "arguments": projection["fields"]}
    if len(_canonical_json_bytes(result)) > MAX_EVENT_BODY_BYTES:
        raise ToolEvidenceError("request_projection_too_large")
    return result


def evidence_safe_fields_from_tool(tool: object) -> frozenset[str]:
    """Read only host-registered schema markers; arguments cannot opt in."""

    schema = getattr(tool, "args_schema", None)
    json_schema = getattr(schema, "model_json_schema", None)
    if not callable(json_schema):
        return frozenset()
    try:
        declared = json_schema()
    except Exception:
        # Some valid tools contain injected callable fields that Pydantic
        # deliberately cannot render. An unreadable schema opts no arguments
        # into plaintext evidence without blocking the call.
        return frozenset()
    properties = declared.get("properties") if isinstance(declared, dict) else None
    if not isinstance(properties, dict):
        return frozenset()
    safe: set[str] = set()
    for name, field_schema in properties.items():
        if not isinstance(name, str) or not isinstance(field_schema, dict):
            continue
        marker = field_schema.get(
            "x-deerflow-evidence-safe",
            field_schema.get("evidence_safe"),
        )
        if marker is True:
            safe.add(name)
        elif marker not in (None, False):
            raise ToolEvidenceError("evidence_safe_policy_invalid")
    return frozenset(safe)


def digest_request_projection(projection: Mapping[str, object]) -> str:
    return canonical_digest(projection)


def _canonical_result_value(value: object, *, depth: int = 0) -> object:
    if depth > 16:
        raise ToolEvidenceError("result_projection_too_deep")
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ToolEvidenceError("result_projection_not_json")
        return value
    if isinstance(value, Mapping):
        return {str(key): _canonical_result_value(child, depth=depth + 1) for key, child in value.items()}
    if isinstance(value, (list, tuple)):
        return [_canonical_result_value(child, depth=depth + 1) for child in value]
    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        return _canonical_result_value(model_dump(mode="json"), depth=depth + 1)
    raise ToolEvidenceError("result_projection_not_json")


def digest_result_projection(result: object, *, result_kind: str, status: str) -> str:
    """Commit to the exact already-sanitized model-visible result."""

    _require_nonempty(result_kind, "result_kind_invalid", max_bytes=64)
    _require_nonempty(status, "result_status_invalid", max_bytes=32)
    projection = {
        "version": 1,
        "result_kind": result_kind,
        "status": status,
        "result": _canonical_result_value(result),
    }
    return canonical_digest(projection)


@dataclass(frozen=True, slots=True)
class DurableToolReceiptV1:
    version: Literal[1, 2, 3]
    receipt_id: str
    idempotency_key: str
    phase: ToolReceiptPhase
    tool_name: str
    request_projection_digest: str
    result_projection_digest: str | None
    result_kind: str | None
    safe_error_code: str | None
    authz_decision_ref: str | None
    guardrail_decision_refs: tuple[str, ...]
    occurred_at: datetime
    context: ToolAttemptContextV1

    def __post_init__(self) -> None:
        expected_version = 3 if self.context.artifact_manifest_digest is not None else (2 if self.context.tenant is not None else 1)
        if self.version != expected_version:
            raise ToolEvidenceError("receipt_version_invalid")
        if _RECEIPT_ID_RE.fullmatch(self.receipt_id) is None or self.receipt_id != stable_receipt_id(self.context):
            raise ToolEvidenceError("receipt_id_invalid")
        expected_key = f"{self.receipt_id}:{'start' if self.phase == 'started' else 'terminal'}"
        if self.idempotency_key != expected_key:
            raise ToolEvidenceError("receipt_idempotency_key_invalid")
        if self.phase not in ("started", "succeeded", "failed", "denied", "cancelled"):
            raise ToolEvidenceError("receipt_phase_invalid")
        _validate_tool_name(self.tool_name)
        _require_digest(self.request_projection_digest, "request_projection_digest_invalid")
        if self.result_projection_digest is not None:
            _require_digest(self.result_projection_digest, "result_projection_digest_invalid")
        if self.result_kind is not None:
            _require_nonempty(self.result_kind, "result_kind_invalid", max_bytes=64)
        if self.safe_error_code is not None and self.safe_error_code not in SAFE_ERROR_CODES:
            raise ToolEvidenceError("safe_error_code_invalid")
        if self.authz_decision_ref is not None:
            if not isinstance(self.authz_decision_ref, str) or _DECISION_REF_RE.fullmatch(self.authz_decision_ref) is None:
                raise ToolEvidenceError("authz_decision_ref_invalid")
        refs = tuple(self.guardrail_decision_refs)
        if len(refs) > MAX_DECISION_REFS or len(set(refs)) != len(refs):
            raise ToolEvidenceError("guardrail_decision_refs_invalid")
        for ref in refs:
            if not isinstance(ref, str) or _DECISION_REF_RE.fullmatch(ref) is None:
                raise ToolEvidenceError("guardrail_decision_ref_invalid")
        object.__setattr__(self, "guardrail_decision_refs", refs)
        if not isinstance(self.occurred_at, datetime) or self.occurred_at.tzinfo is None:
            raise ToolEvidenceError("occurred_at_invalid")
        if self.phase == "started":
            if any(
                value is not None
                for value in (
                    self.result_projection_digest,
                    self.result_kind,
                    self.safe_error_code,
                )
            ):
                raise ToolEvidenceError("started_outcome_fields_invalid")
            if self.authz_decision_ref is not None or refs:
                raise ToolEvidenceError("started_policy_refs_invalid")
        elif self.phase == "succeeded":
            if self.result_projection_digest is None or self.result_kind is None or self.safe_error_code is not None:
                raise ToolEvidenceError("success_outcome_fields_invalid")
        elif self.phase in ("failed", "denied", "cancelled") and self.safe_error_code is None:
            raise ToolEvidenceError("error_outcome_code_missing")
        if len(_canonical_json_bytes(self.to_event_body())) > MAX_EVENT_BODY_BYTES:
            raise ToolEvidenceError("receipt_body_too_large")

    def to_event_body(self) -> dict[str, object]:
        """Serialize logical evidence; the store owns and adds occurred_at."""

        return {
            "version": self.version,
            "receipt_id": self.receipt_id,
            "idempotency_key": self.idempotency_key,
            "phase": self.phase,
            "tool_name": self.tool_name,
            "request_projection_digest": self.request_projection_digest,
            "result_projection_digest": self.result_projection_digest,
            "result_kind": self.result_kind,
            "safe_error_code": self.safe_error_code,
            "authz_decision_ref": self.authz_decision_ref,
            "guardrail_decision_refs": list(self.guardrail_decision_refs),
            "context": self.context.to_dict(),
        }

    @classmethod
    def from_event_body(
        cls,
        value: Mapping[str, object],
        *,
        occurred_at: datetime,
    ) -> DurableToolReceiptV1:
        expected = {
            "version",
            "receipt_id",
            "idempotency_key",
            "phase",
            "tool_name",
            "request_projection_digest",
            "result_projection_digest",
            "result_kind",
            "safe_error_code",
            "authz_decision_ref",
            "guardrail_decision_refs",
            "context",
        }
        if not isinstance(value, Mapping) or set(value) != expected:
            raise ToolEvidenceError("receipt_body_fields_invalid")
        raw_context = value.get("context")
        raw_refs = value.get("guardrail_decision_refs")
        if not isinstance(raw_context, Mapping) or not isinstance(raw_refs, (list, tuple)):
            raise ToolEvidenceError("receipt_body_fields_invalid")
        try:
            return cls(
                version=value["version"],  # type: ignore[arg-type]
                receipt_id=value["receipt_id"],  # type: ignore[arg-type]
                idempotency_key=value["idempotency_key"],  # type: ignore[arg-type]
                phase=value["phase"],  # type: ignore[arg-type]
                tool_name=value["tool_name"],  # type: ignore[arg-type]
                request_projection_digest=value["request_projection_digest"],  # type: ignore[arg-type]
                result_projection_digest=value["result_projection_digest"],  # type: ignore[arg-type]
                result_kind=value["result_kind"],  # type: ignore[arg-type]
                safe_error_code=value["safe_error_code"],  # type: ignore[arg-type]
                authz_decision_ref=value["authz_decision_ref"],  # type: ignore[arg-type]
                guardrail_decision_refs=tuple(raw_refs),  # type: ignore[arg-type]
                occurred_at=occurred_at,
                context=ToolAttemptContextV1.from_dict(raw_context),
            )
        except ToolEvidenceError:
            raise
        except (KeyError, TypeError, ValueError) as exc:
            raise ToolEvidenceError("receipt_body_fields_invalid") from exc

    @classmethod
    def started(
        cls,
        *,
        context: ToolAttemptContextV1,
        tool_name: str,
        request_projection_digest: str,
        occurred_at: datetime | None = None,
    ) -> DurableToolReceiptV1:
        receipt_id = stable_receipt_id(context)
        return cls(
            version=(3 if context.artifact_manifest_digest is not None else (2 if context.tenant is not None else 1)),
            receipt_id=receipt_id,
            idempotency_key=f"{receipt_id}:start",
            phase="started",
            tool_name=tool_name,
            request_projection_digest=request_projection_digest,
            result_projection_digest=None,
            result_kind=None,
            safe_error_code=None,
            authz_decision_ref=None,
            guardrail_decision_refs=(),
            occurred_at=occurred_at or datetime.now(UTC),
            context=context,
        )

    def outcome(
        self,
        *,
        phase: Literal["succeeded", "failed", "denied", "cancelled"],
        result_projection_digest: str | None,
        result_kind: str | None,
        safe_error_code: str | None,
        authz_decision_ref: str | None = None,
        guardrail_decision_refs: tuple[str, ...] = (),
        occurred_at: datetime | None = None,
    ) -> DurableToolReceiptV1:
        return replace(
            self,
            idempotency_key=f"{self.receipt_id}:terminal",
            phase=phase,
            result_projection_digest=result_projection_digest,
            result_kind=result_kind,
            safe_error_code=safe_error_code,
            authz_decision_ref=authz_decision_ref,
            guardrail_decision_refs=guardrail_decision_refs,
            occurred_at=occurred_at or datetime.now(UTC),
        )


@dataclass(slots=True)
class _ActiveToolReceiptBinding:
    receipt: DurableToolReceiptV1
    active: bool = True


_ACTIVE_TOOL_RECEIPT: ContextVar[_ActiveToolReceiptBinding | None] = ContextVar(
    "deerflow_active_tool_receipt",
    default=None,
)


def get_active_tool_receipt() -> DurableToolReceiptV1 | None:
    """Return the durable ``started`` receipt for the current tool task.

    The receipt is bound only after its durable reservation has been
    acknowledged and is reset before control leaves the inner tool call.
    ``ContextVar`` scoping keeps concurrent lead/subagent calls isolated.
    """

    binding = _ACTIVE_TOOL_RECEIPT.get()
    if not isinstance(binding, _ActiveToolReceiptBinding) or not binding.active:
        return None
    return binding.receipt


@contextmanager
def active_tool_receipt_context(
    receipt: DurableToolReceiptV1,
) -> Iterator[None]:
    """Bind one server-reserved receipt around its inner tool execution."""

    if not isinstance(receipt, DurableToolReceiptV1) or receipt.phase != "started":
        raise ToolEvidenceError("active_tool_receipt_invalid")
    binding = _ActiveToolReceiptBinding(receipt=receipt)
    token = _ACTIVE_TOOL_RECEIPT.set(binding)
    try:
        yield
    finally:
        # Child asyncio tasks inherit a copy of the ContextVar mapping. They
        # still reference this same binding object, so invalidating it closes
        # the receipt after the synchronous tool scope ends there as well.
        binding.active = False
        _ACTIVE_TOOL_RECEIPT.reset(token)


@dataclass(frozen=True, slots=True)
class ToolAttemptReservation:
    """A durable start plus any completed-attempt recovery replay."""

    started: DurableToolReceiptV1
    replayed_outcome: DurableToolReceiptV1 | None = None

    def __post_init__(self) -> None:
        if self.started.phase != "started":
            raise ToolEvidenceError("reservation_start_invalid")
        outcome = self.replayed_outcome
        if outcome is not None and (
            outcome.phase == "started"
            or outcome.receipt_id != self.started.receipt_id
            or outcome.context != self.started.context
            or outcome.tool_name != self.started.tool_name
            or outcome.request_projection_digest != self.started.request_projection_digest
        ):
            raise ToolEvidenceError("reservation_outcome_invalid")


@dataclass(frozen=True, slots=True)
class ParsedToolReceiptEventV1:
    """Validated receipt payload plus its store-owned join metadata."""

    receipt: DurableToolReceiptV1
    writer_fence_digest: str
    dispatch_generation_digest: str


def _event_timestamp(event: Mapping[str, object]) -> datetime:
    value = event.get("created_at")
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ToolReceiptIntegrityError("receipt_event_timestamp_invalid") from exc
    else:
        raise ToolReceiptIntegrityError("receipt_event_timestamp_invalid")
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed


def parse_tool_receipt_event(
    event: Mapping[str, object],
) -> ParsedToolReceiptEventV1:
    """Validate one persisted receipt event through the receipt boundary."""

    body = event.get("content")
    if not isinstance(body, Mapping):
        raise ToolReceiptIntegrityError("receipt_event_body_invalid")
    try:
        receipt = DurableToolReceiptV1.from_event_body(body, occurred_at=_event_timestamp(event))
    except ToolEvidenceError as exc:
        raise ToolReceiptIntegrityError("receipt_event_body_invalid") from exc
    expected_type = TOOL_RECEIPT_STARTED_EVENT if receipt.phase == "started" else TOOL_RECEIPT_OUTCOME_EVENT
    if event.get("event_type") != expected_type or event.get("category") != TOOL_RECEIPT_CATEGORY or event.get("run_id") != receipt.context.run_id or event.get("idempotency_key") != receipt.idempotency_key:
        raise ToolReceiptIntegrityError("receipt_event_envelope_invalid")
    writer_fence_digest, dispatch_generation_digest = _validated_receipt_event_metadata(
        event,
        receipt,
    )
    return ParsedToolReceiptEventV1(
        receipt=receipt,
        writer_fence_digest=writer_fence_digest,
        dispatch_generation_digest=dispatch_generation_digest,
    )


def receipt_from_event(event: Mapping[str, object]) -> DurableToolReceiptV1:
    """Compatibility projection of a fully validated receipt event."""

    return parse_tool_receipt_event(event).receipt


def _validated_receipt_event_metadata(
    event: Mapping[str, object],
    receipt: DurableToolReceiptV1,
) -> tuple[str, str]:
    metadata = event.get("metadata")
    context = receipt.context
    if not isinstance(metadata, Mapping):
        raise ToolReceiptIntegrityError("receipt_event_metadata_invalid")
    attempt = metadata.get("attempt")
    if metadata.get("receipt_id") != receipt.receipt_id or metadata.get("task_id") != context.execution_task_id or metadata.get("tool_call_id") != context.tool_call_id or type(attempt) is not int or attempt != context.attempt:
        raise ToolReceiptIntegrityError("receipt_event_metadata_invalid")
    writer_fence_digest = metadata.get("writer_fence_digest")
    dispatch_generation_digest = metadata.get("dispatch_generation_digest")
    if not isinstance(writer_fence_digest, str) or _DIGEST_RE.fullmatch(writer_fence_digest) is None or not isinstance(dispatch_generation_digest, str) or _DIGEST_RE.fullmatch(dispatch_generation_digest) is None:
        raise ToolReceiptIntegrityError("receipt_event_metadata_invalid")
    if dispatch_generation_digest != tool_dispatch_generation_digest(context):
        raise ToolReceiptIntegrityError("receipt_event_metadata_invalid")
    return writer_fence_digest, dispatch_generation_digest


def receipt_dispatch_generation_from_event(event: Mapping[str, object]) -> str:
    """Return one fully validated receipt event's dispatch generation."""

    return parse_tool_receipt_event(event).dispatch_generation_digest


def reserve_attempt_from_events(
    events: Iterable[Mapping[str, object]],
    *,
    binding: ToolEvidenceRuntimeBinding,
    tool_call_id: str,
    tool_name: str,
    request_projection_digest: str,
    observed_node_attempt: int,
    expected_attempt: int | None,
) -> tuple[
    DurableToolReceiptV1,
    Mapping[str, object] | None,
    Mapping[str, object] | None,
]:
    """Replay the selected durable attempt or reserve the next identity.

    Callers must invoke this while holding their durable write/fence lock. The
    second item is a reused start. The third is a terminal written under an
    already-completed attempt, which tells middleware not to redispatch.

    The durable history owns numbering. A fresh process has no translated
    ``expected_attempt`` and conservatively binds a reset local counter to the
    latest persisted attempt. Once bound, the local offset permits only the
    latest attempt or its immediate successor.
    """

    _require_nonempty(tool_call_id, "tool_call_id_invalid", max_bytes=256)
    _validate_tool_name(tool_name)
    _require_digest(request_projection_digest, "request_projection_digest_invalid")
    if type(observed_node_attempt) is not int or observed_node_attempt < 1:
        raise ToolEvidenceError("node_attempt_invalid")
    if expected_attempt is not None and (type(expected_attempt) is not int or expected_attempt < 1):
        raise ToolEvidenceError("expected_attempt_invalid")
    starts: list[tuple[DurableToolReceiptV1, Mapping[str, object]]] = []
    outcomes: dict[str, tuple[DurableToolReceiptV1, Mapping[str, object]]] = {}
    for event in events:
        if event.get("event_type") not in (
            TOOL_RECEIPT_STARTED_EVENT,
            TOOL_RECEIPT_OUTCOME_EVENT,
        ):
            continue
        receipt = parse_tool_receipt_event(event).receipt
        if receipt.context.run_id != binding.run_id:
            raise ToolReceiptIntegrityError("receipt_attempt_history_invalid")
        if receipt.context.execution_task_id != binding.execution_task_id or receipt.context.tool_call_id != tool_call_id:
            continue
        if receipt.phase == "started":
            starts.append((receipt, event))
        elif receipt.receipt_id in outcomes:
            raise ToolReceiptIntegrityError("receipt_attempt_history_invalid")
        else:
            outcomes[receipt.receipt_id] = (receipt, event)

    if not starts:
        if outcomes or expected_attempt not in (None, 1):
            raise ToolReceiptIntegrityError("receipt_attempt_history_invalid")
        context = binding.make_attempt(tool_call_id, 1)
        return (
            DurableToolReceiptV1.started(
                context=context,
                tool_name=tool_name,
                request_projection_digest=request_projection_digest,
            ),
            None,
            None,
        )

    starts.sort(key=lambda item: item[0].context.attempt)
    attempts = [receipt.context.attempt for receipt, _event in starts]
    if attempts != list(range(1, attempts[-1] + 1)) or len(set(attempts)) != len(attempts):
        raise ToolReceiptIntegrityError("receipt_attempt_history_invalid")
    latest, latest_event = starts[-1]
    unknown_outcomes = set(outcomes).difference(receipt.receipt_id for receipt, _event in starts)
    if unknown_outcomes:
        raise ToolReceiptIntegrityError("receipt_attempt_history_invalid")
    for started, _event in starts:
        outcome_entry = outcomes.get(started.receipt_id)
        outcome = outcome_entry[0] if outcome_entry is not None else None
        if outcome is not None and (outcome.context != started.context or outcome.tool_name != started.tool_name or outcome.request_projection_digest != started.request_projection_digest):
            raise ToolReceiptIntegrityError("receipt_attempt_history_invalid")
        expected = binding.make_attempt(tool_call_id, started.context.attempt)
        if (
            started.context.execution_kind != expected.execution_kind
            or started.context.subagent_name != expected.subagent_name
            or started.context.agent_revision_digest != expected.agent_revision_digest
            or started.context.assembly_fingerprint != expected.assembly_fingerprint
            or started.context.extension_generation != expected.extension_generation
            or started.context.subagent_catalog_digest != expected.subagent_catalog_digest
            or started.context.subagent_definition_digest != expected.subagent_definition_digest
            or started.tool_name != tool_name
            or started.request_projection_digest != request_projection_digest
        ):
            raise ToolReceiptIntegrityError("receipt_attempt_replay_conflict")

    latest_attempt = latest.context.attempt
    if expected_attempt is None:
        if observed_node_attempt > latest_attempt + 1:
            raise ToolReceiptIntegrityError("receipt_attempt_history_invalid")
        selected_attempt = latest_attempt + 1 if observed_node_attempt == latest_attempt + 1 else latest_attempt
    elif expected_attempt in (latest_attempt, latest_attempt + 1):
        selected_attempt = expected_attempt
    else:
        raise ToolReceiptIntegrityError("receipt_attempt_history_invalid")

    if selected_attempt == latest_attempt:
        outcome_entry = outcomes.get(latest.receipt_id)
        return (
            latest,
            latest_event,
            outcome_entry[1] if outcome_entry is not None else None,
        )

    context = binding.make_attempt(tool_call_id, selected_attempt)
    return (
        DurableToolReceiptV1.started(
            context=context,
            tool_name=tool_name,
            request_projection_digest=request_projection_digest,
        ),
        None,
        None,
    )


def tool_writer_fence_digest(owner_id: str, lease_epoch: int) -> str:
    """Commit to the fence that physically appended an event."""

    _require_nonempty(owner_id, "owner_id_invalid", max_bytes=128)
    if type(lease_epoch) is not int or lease_epoch < 0:
        raise ToolEvidenceError("lease_epoch_invalid")
    return canonical_digest(
        {
            "version": 1,
            "owner_id": owner_id,
            "lease_epoch": lease_epoch,
        }
    )


def receipt_event_metadata(
    receipt: DurableToolReceiptV1,
    *,
    writer_owner_id: str,
    writer_lease_epoch: int,
) -> dict[str, object]:
    """Return bounded join metadata for receipt-store indexes."""

    return {
        "receipt_id": receipt.receipt_id,
        "task_id": receipt.context.execution_task_id,
        "tool_call_id": receipt.context.tool_call_id,
        "attempt": receipt.context.attempt,
        "writer_fence_digest": tool_writer_fence_digest(
            writer_owner_id,
            writer_lease_epoch,
        ),
        "dispatch_generation_digest": tool_dispatch_generation_digest(receipt.context),
    }


def require_started_transition(
    events: Iterable[Mapping[str, object]],
    outcome: DurableToolReceiptV1,
) -> None:
    """Require one matching immutable start before a new terminal append."""

    if outcome.phase == "started":
        raise ToolReceiptIntegrityError("receipt_outcome_expected")
    matching: list[ParsedToolReceiptEventV1] = []
    for event in events:
        if event.get("event_type") != TOOL_RECEIPT_STARTED_EVENT:
            continue
        body = event.get("content")
        if not isinstance(body, Mapping) or body.get("receipt_id") != outcome.receipt_id:
            continue
        matching.append(parse_tool_receipt_event(event))
    if len(matching) != 1:
        raise ToolReceiptIntegrityError("receipt_start_missing" if not matching else "receipt_attempt_history_invalid")
    started_event = matching[0]
    started = started_event.receipt
    if started.context != outcome.context or started.tool_name != outcome.tool_name or started.request_projection_digest != outcome.request_projection_digest:
        raise ToolReceiptIntegrityError("receipt_outcome_start_mismatch")
    if started_event.dispatch_generation_digest != tool_dispatch_generation_digest(outcome.context):
        raise ToolReceiptIntegrityError("receipt_outcome_start_mismatch")


@runtime_checkable
class DurableToolReceiptSink(Protocol):
    async def reserve_started(
        self,
        *,
        binding: ToolEvidenceRuntimeBinding,
        tool_call_id: str,
        tool_name: str,
        request_projection_digest: str,
        dispatch: ToolDispatchObservationV1,
    ) -> ToolAttemptReservation: ...

    async def record_started(self, receipt: DurableToolReceiptV1) -> None: ...

    async def record_outcome(self, receipt: DurableToolReceiptV1) -> None: ...


class NullDurableToolReceiptSink:
    """No-op adapter for ordinary/direct agent executions."""

    async def reserve_started(
        self,
        *,
        binding: ToolEvidenceRuntimeBinding,
        tool_call_id: str,
        tool_name: str,
        request_projection_digest: str,
        dispatch: ToolDispatchObservationV1,
    ) -> ToolAttemptReservation:
        expected_attempt = binding.expected_dispatch_attempt(tool_call_id, dispatch)
        attempt = expected_attempt if expected_attempt is not None else 1
        binding.bind_dispatch_attempt(tool_call_id, dispatch, attempt)
        return ToolAttemptReservation(
            started=DurableToolReceiptV1.started(
                context=binding.make_attempt(tool_call_id, attempt),
                tool_name=tool_name,
                request_projection_digest=request_projection_digest,
            )
        )

    async def record_started(self, receipt: DurableToolReceiptV1) -> None:
        del receipt

    async def record_outcome(self, receipt: DurableToolReceiptV1) -> None:
        del receipt


class RunEventToolReceiptSink:
    """Adapt validated receipts onto fenced, idempotent run events."""

    def __init__(self, event_store: Any) -> None:
        self._event_store = event_store
        self._active_fences: dict[str, tuple[str, int]] = {}

    async def reserve_started(
        self,
        *,
        binding: ToolEvidenceRuntimeBinding,
        tool_call_id: str,
        tool_name: str,
        request_projection_digest: str,
        dispatch: ToolDispatchObservationV1,
    ) -> ToolAttemptReservation:
        expected_attempt = binding.expected_dispatch_attempt(tool_call_id, dispatch)
        outcome = await self._event_store.reserve_tool_attempt(
            binding.run_id,
            binding=binding,
            tool_call_id=tool_call_id,
            tool_name=tool_name,
            request_projection_digest=request_projection_digest,
            observed_node_attempt=dispatch.node_attempt,
            expected_attempt=expected_attempt,
            owner_id=binding.owner_id,
            lease_epoch=binding.lease_epoch,
        )
        receipt = parse_tool_receipt_event(outcome.event).receipt
        replayed_outcome = parse_tool_receipt_event(outcome.terminal_event).receipt if outcome.terminal_event is not None else None
        binding.bind_dispatch_attempt(
            tool_call_id,
            dispatch,
            receipt.context.attempt,
        )
        if replayed_outcome is None:
            self._active_fences[receipt.receipt_id] = (
                binding.owner_id,
                binding.lease_epoch,
            )
        return ToolAttemptReservation(
            started=receipt,
            replayed_outcome=replayed_outcome,
        )

    async def _record(
        self,
        receipt: DurableToolReceiptV1,
        *,
        event_type: str,
    ) -> None:
        active = self._active_fences.get(receipt.receipt_id)
        if active is None:
            active = (
                receipt.context.owner_id,
                receipt.context.lease_epoch,
            )
        owner_id, lease_epoch = active
        await self._event_store.append_idempotent(
            receipt.context.run_id,
            event_type=event_type,
            idempotency_key=receipt.idempotency_key,
            body=receipt.to_event_body(),
            owner_id=owner_id,
            lease_epoch=lease_epoch,
        )

    async def record_started(self, receipt: DurableToolReceiptV1) -> None:
        if receipt.phase != "started":
            raise ToolEvidenceError("started_receipt_phase_invalid")
        self._active_fences[receipt.receipt_id] = (
            receipt.context.owner_id,
            receipt.context.lease_epoch,
        )
        await self._record(
            receipt,
            event_type=TOOL_RECEIPT_STARTED_EVENT,
        )

    async def record_outcome(self, receipt: DurableToolReceiptV1) -> None:
        if receipt.phase == "started":
            raise ToolEvidenceError("outcome_receipt_phase_invalid")
        await self._record(receipt, event_type=TOOL_RECEIPT_OUTCOME_EVENT)
        self._active_fences.pop(receipt.receipt_id, None)


class ToolEvidenceRuntimeBinding:
    """Host-owned immutable anchors used by durable attempt reservation."""

    __slots__ = (
        "run_id",
        "execution_task_id",
        "execution_kind",
        "subagent_name",
        "owner_id",
        "lease_epoch",
        "agent_revision_digest",
        "assembly_fingerprint",
        "extension_generation",
        "capability_manifest_digest",
        "artifact_manifest_digest",
        "extension_configuration_digest",
        "subagent_catalog_digest",
        "subagent_definition_digest",
        "tenant",
        "_dispatch_locks",
        "_dispatch_offsets",
        "_dispatch_locks_guard",
    )

    def __init__(
        self,
        *,
        run_id: str,
        execution_task_id: str,
        execution_kind: ToolExecutionKind,
        subagent_name: str | None,
        owner_id: str,
        lease_epoch: int,
        agent_revision_digest: str,
        assembly_fingerprint: str,
        extension_generation: int,
        subagent_catalog_digest: str,
        subagent_definition_digest: str | None,
        capability_manifest_digest: str | None = None,
        artifact_manifest_digest: str | None = None,
        extension_configuration_digest: str | None = None,
        tenant: TenantReferenceV1 | None = None,
    ) -> None:
        # Validate all static anchors through the public context type.
        ToolAttemptContextV1(
            run_id=run_id,
            execution_task_id=execution_task_id,
            execution_kind=execution_kind,
            subagent_name=subagent_name,
            tool_call_id="validation",
            attempt=1,
            owner_id=owner_id,
            lease_epoch=lease_epoch,
            agent_revision_digest=agent_revision_digest,
            assembly_fingerprint=assembly_fingerprint,
            extension_generation=extension_generation,
            subagent_catalog_digest=subagent_catalog_digest,
            subagent_definition_digest=subagent_definition_digest,
            capability_manifest_digest=capability_manifest_digest,
            artifact_manifest_digest=artifact_manifest_digest,
            extension_configuration_digest=extension_configuration_digest,
            tenant=tenant,
        )
        self.run_id = run_id
        self.execution_task_id = execution_task_id
        self.execution_kind = execution_kind
        self.subagent_name = subagent_name
        self.owner_id = owner_id
        self.lease_epoch = lease_epoch
        self.agent_revision_digest = agent_revision_digest
        self.assembly_fingerprint = assembly_fingerprint
        self.extension_generation = extension_generation
        self.subagent_catalog_digest = subagent_catalog_digest
        self.subagent_definition_digest = subagent_definition_digest
        self.capability_manifest_digest = capability_manifest_digest
        self.artifact_manifest_digest = artifact_manifest_digest
        self.extension_configuration_digest = extension_configuration_digest
        self.tenant = tenant
        self._dispatch_locks: dict[str, asyncio.Lock] = {}
        self._dispatch_offsets: dict[tuple[str, str], int] = {}
        self._dispatch_locks_guard = threading.Lock()

    def make_attempt(self, tool_call_id: str, attempt: int) -> ToolAttemptContextV1:
        _require_nonempty(tool_call_id, "tool_call_id_invalid", max_bytes=256)
        return ToolAttemptContextV1(
            run_id=self.run_id,
            execution_task_id=self.execution_task_id,
            execution_kind=self.execution_kind,
            subagent_name=self.subagent_name,
            tool_call_id=tool_call_id,
            attempt=attempt,
            owner_id=self.owner_id,
            lease_epoch=self.lease_epoch,
            agent_revision_digest=self.agent_revision_digest,
            assembly_fingerprint=self.assembly_fingerprint,
            extension_generation=self.extension_generation,
            subagent_catalog_digest=self.subagent_catalog_digest,
            subagent_definition_digest=self.subagent_definition_digest,
            capability_manifest_digest=self.capability_manifest_digest,
            artifact_manifest_digest=self.artifact_manifest_digest,
            extension_configuration_digest=self.extension_configuration_digest,
            tenant=self.tenant,
        )

    def expected_dispatch_attempt(
        self,
        tool_call_id: str,
        dispatch: ToolDispatchObservationV1,
    ) -> int | None:
        """Translate a live retry counter after its durable offset is known."""

        _require_nonempty(tool_call_id, "tool_call_id_invalid", max_bytes=256)
        if not isinstance(dispatch, ToolDispatchObservationV1):
            raise ToolEvidenceError("tool_dispatch_observation_invalid")
        key = (tool_call_id, dispatch.lineage_digest)
        with self._dispatch_locks_guard:
            offset = self._dispatch_offsets.get(key)
        if offset is None:
            return None
        expected = dispatch.node_attempt + offset
        if expected < 1:
            raise ToolReceiptIntegrityError("receipt_attempt_history_invalid")
        return expected

    def bind_dispatch_attempt(
        self,
        tool_call_id: str,
        dispatch: ToolDispatchObservationV1,
        durable_attempt: int,
    ) -> None:
        """Remember the local-to-durable retry offset for this live binding."""

        _require_nonempty(tool_call_id, "tool_call_id_invalid", max_bytes=256)
        if not isinstance(dispatch, ToolDispatchObservationV1):
            raise ToolEvidenceError("tool_dispatch_observation_invalid")
        if type(durable_attempt) is not int or durable_attempt < 1:
            raise ToolEvidenceError("attempt_invalid")
        key = (tool_call_id, dispatch.lineage_digest)
        offset = durable_attempt - dispatch.node_attempt
        with self._dispatch_locks_guard:
            existing = self._dispatch_offsets.get(key)
            if existing is not None and existing != offset:
                raise ToolReceiptIntegrityError("receipt_attempt_history_invalid")
            self._dispatch_offsets[key] = offset

    @asynccontextmanager
    async def serialize_dispatch(self, tool_call_id: str) -> AsyncIterator[None]:
        """Serialize duplicate live dispatches; durable storage owns numbering."""

        _require_nonempty(tool_call_id, "tool_call_id_invalid", max_bytes=256)
        with self._dispatch_locks_guard:
            lock = self._dispatch_locks.setdefault(tool_call_id, asyncio.Lock())
        async with lock:
            yield

    def for_subagent(
        self,
        *,
        execution_task_id: str,
        subagent_name: str,
        subagent_definition_digest: str,
    ) -> ToolEvidenceRuntimeBinding:
        return ToolEvidenceRuntimeBinding(
            run_id=self.run_id,
            execution_task_id=execution_task_id,
            execution_kind="subagent",
            subagent_name=subagent_name,
            owner_id=self.owner_id,
            lease_epoch=self.lease_epoch,
            agent_revision_digest=self.agent_revision_digest,
            assembly_fingerprint=self.assembly_fingerprint,
            extension_generation=self.extension_generation,
            subagent_catalog_digest=self.subagent_catalog_digest,
            subagent_definition_digest=subagent_definition_digest,
            capability_manifest_digest=self.capability_manifest_digest,
            artifact_manifest_digest=self.artifact_manifest_digest,
            extension_configuration_digest=self.extension_configuration_digest,
            tenant=self.tenant,
        )


def require_tool_attempt_binding_fence(
    binding: object,
    *,
    run_id: str,
    owner_id: str,
    lease_epoch: int,
) -> ToolEvidenceRuntimeBinding:
    """Require one reservation request to use a single immutable fence."""

    if not isinstance(binding, ToolEvidenceRuntimeBinding) or binding.run_id != run_id or binding.owner_id != owner_id or binding.lease_epoch != lease_epoch:
        raise ToolReceiptIntegrityError("receipt_attempt_binding_invalid")
    return binding


class CrossLoopDurableToolReceiptSink:
    """Schedule child-loop receipt writes onto the parent run's owner loop."""

    def __init__(self, sink: DurableToolReceiptSink, owner_loop: asyncio.AbstractEventLoop) -> None:
        self._sink = sink
        self._owner_loop = owner_loop

    async def _run(
        self,
        receipt: DurableToolReceiptV1,
        *,
        outcome: bool,
    ) -> None:
        current = asyncio.get_running_loop()
        operation = self._sink.record_outcome(receipt) if outcome else self._sink.record_started(receipt)
        if current is self._owner_loop:
            await operation
            return
        future = asyncio.run_coroutine_threadsafe(operation, self._owner_loop)
        await asyncio.wrap_future(future)

    async def reserve_started(
        self,
        *,
        binding: ToolEvidenceRuntimeBinding,
        tool_call_id: str,
        tool_name: str,
        request_projection_digest: str,
        dispatch: ToolDispatchObservationV1,
    ) -> ToolAttemptReservation:
        operation = self._sink.reserve_started(
            binding=binding,
            tool_call_id=tool_call_id,
            tool_name=tool_name,
            request_projection_digest=request_projection_digest,
            dispatch=dispatch,
        )
        if asyncio.get_running_loop() is self._owner_loop:
            return await operation
        future = asyncio.run_coroutine_threadsafe(operation, self._owner_loop)
        return await asyncio.wrap_future(future)

    async def record_started(self, receipt: DurableToolReceiptV1) -> None:
        await self._run(receipt, outcome=False)

    async def record_outcome(self, receipt: DurableToolReceiptV1) -> None:
        await self._run(receipt, outcome=True)


def cross_loop_receipt_sink(sink: DurableToolReceiptSink) -> DurableToolReceiptSink:
    """Capture the current owner loop for an imminent isolated child task."""

    if isinstance(sink, (NullDurableToolReceiptSink, CrossLoopDurableToolReceiptSink)):
        return sink
    return CrossLoopDurableToolReceiptSink(sink, asyncio.get_running_loop())


def strip_tool_evidence_context(context: object) -> None:
    if isinstance(context, dict):
        context.pop(TOOL_EVIDENCE_CONTEXT_KEY, None)
        context.pop(TOOL_EVIDENCE_SINK_KEY, None)


def install_tool_evidence_context(
    context: dict[str, object],
    *,
    binding: ToolEvidenceRuntimeBinding,
    sink: DurableToolReceiptSink,
) -> None:
    if not isinstance(binding, ToolEvidenceRuntimeBinding) or not isinstance(sink, DurableToolReceiptSink):
        raise ToolEvidenceError("tool_evidence_runtime_binding_invalid")
    context[TOOL_EVIDENCE_CONTEXT_KEY] = binding
    context[TOOL_EVIDENCE_SINK_KEY] = sink


def resolve_tool_evidence_context(
    context: object,
) -> tuple[ToolEvidenceRuntimeBinding | None, DurableToolReceiptSink]:
    if not isinstance(context, dict):
        return None, NullDurableToolReceiptSink()
    binding = context.get(TOOL_EVIDENCE_CONTEXT_KEY)
    sink = context.get(TOOL_EVIDENCE_SINK_KEY)
    if not isinstance(binding, ToolEvidenceRuntimeBinding) or not isinstance(sink, DurableToolReceiptSink):
        return None, NullDurableToolReceiptSink()
    return binding, sink
