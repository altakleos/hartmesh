"""Typed paging contract for the authoritative invocation lifecycle journal."""

from __future__ import annotations

import base64
import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal

from deerflow_extension_api import validate_thread_identifier

from deerflow.runtime.assembly_evidence import (
    AssemblyEvidenceError,
    AssemblyEvidenceV1,
    assembly_evidence_digest,
)

_CURSOR_VERSION = "deerflow.lifecycle.cursor/v1"
_TOOL_RECEIPT_CURSOR_VERSION = "deerflow.tool-receipt.cursor/v1"
INVOCATION_SOURCE_KINDS = frozenset({"http", "scheduled_task", "native_channel", "service"})
MAX_LIFECYCLE_PAGE_SIZE = 500
MAX_TOOL_RECEIPT_PAGE_SIZE = 100
MAX_INVOCATION_SUMMARY_BYTES = 16 * 1024
_MAX_CORRELATION_REFERENCES = 64
_MAX_CORRELATION_VALUE_BYTES = 1024
_MAX_VISIBILITY_SELECTORS = 128
_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_PUBLIC_IDENTIFIER_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_.:-]{0,191}\Z", re.ASCII)
_NONTERMINAL_RUN_STATUSES = frozenset({"pending", "running"})


class InvalidLifecycleCursor(ValueError):
    """The opaque lifecycle cursor is malformed or uses another version."""


class InvalidToolReceiptCursor(ValueError):
    """A receipt cursor is malformed or scoped to another authorized run."""


class CursorGap(ValueError):
    """The requested cursor is older than retained lifecycle evidence."""

    def __init__(self, minimum_available_cursor: str) -> None:
        self.minimum_available_cursor = minimum_available_cursor
        super().__init__("lifecycle cursor is older than retained evidence")


class CursorAhead(ValueError):
    """The requested cursor is newer than the current committed fence."""

    def __init__(self, read_fence_cursor: str) -> None:
        self.read_fence_cursor = read_fence_cursor
        super().__init__("lifecycle cursor is ahead of committed evidence")


class LifecycleOrderingCorruption(RuntimeError):
    """Lifecycle events and their global cursor metadata cannot be reconciled."""


@dataclass(frozen=True)
class LifecycleVisibilityScope:
    """Finite host-resolved visibility predicate for one exact context query."""

    thread_id: str
    allow_context: bool = False
    run_ids: tuple[str, ...] = ()
    owner_ids: tuple[str, ...] = ()
    source_kinds: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "thread_id",
            validate_thread_identifier(
                self.thread_id,
                field_name="lifecycle visibility thread_id",
            ),
        )
        for name in ("run_ids", "owner_ids", "source_kinds"):
            values = getattr(self, name)
            if not isinstance(values, tuple) or any(not isinstance(value, str) or not value for value in values):
                raise ValueError(f"lifecycle visibility {name} must contain non-empty strings")
            if len(values) != len(set(values)):
                raise ValueError(f"lifecycle visibility {name} contains duplicates")
        if len(self.run_ids) + len(self.owner_ids) + len(self.source_kinds) > _MAX_VISIBILITY_SELECTORS:
            raise ValueError("lifecycle visibility scope exceeds its selector bound")
        if any(value not in INVOCATION_SOURCE_KINDS for value in self.source_kinds):
            raise ValueError("lifecycle visibility scope has an unsupported source kind")
        if not (self.allow_context or self.run_ids or self.owner_ids or self.source_kinds):
            raise ValueError("lifecycle visibility scope must grant a finite selector")

    def permits(self, *, run_id: str, owner_id: str | None, source_kind: str | None) -> bool:
        return bool(self.allow_context or run_id in self.run_ids or (owner_id is not None and owner_id in self.owner_ids) or source_kind in self.source_kinds)


def encode_lifecycle_cursor(cursor: int) -> str:
    """Encode a non-negative lifecycle position as an opaque v1 cursor."""

    if type(cursor) is not int or cursor < 0:
        raise ValueError("lifecycle cursor must be a non-negative integer")
    payload = json.dumps(
        {"cursor": cursor, "version": _CURSOR_VERSION},
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")
    encoded = base64.urlsafe_b64encode(payload).rstrip(b"=").decode("ascii")
    return f"lc1.{encoded}"


def decode_lifecycle_cursor(token: str) -> int:
    """Decode and strictly validate an opaque v1 lifecycle cursor."""

    if not isinstance(token, str) or not token.startswith("lc1."):
        raise InvalidLifecycleCursor("invalid lifecycle cursor")
    encoded = token[4:]
    try:
        padding = "=" * (-len(encoded) % 4)
        raw = base64.b64decode(encoded + padding, altchars=b"-_", validate=True)
        payload = json.loads(raw)
    except (ValueError, UnicodeError, json.JSONDecodeError) as exc:
        raise InvalidLifecycleCursor("invalid lifecycle cursor") from exc
    if not isinstance(payload, dict) or set(payload) != {"cursor", "version"}:
        raise InvalidLifecycleCursor("invalid lifecycle cursor fields")
    if payload["version"] != _CURSOR_VERSION:
        raise InvalidLifecycleCursor("unsupported lifecycle cursor version")
    cursor = payload["cursor"]
    if type(cursor) is not int or cursor < 0:
        raise InvalidLifecycleCursor("invalid lifecycle cursor value")
    return cursor


def _receipt_cursor_checksum(payload: Mapping[str, object]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(b"hartmesh/tool-receipt-cursor/v1\0" + encoded).hexdigest()


def encode_tool_receipt_cursor(*, run_id: str, thread_id: str, after_seq: int) -> str:
    if not isinstance(run_id, str) or not run_id or len(run_id.encode("utf-8")) > 64:
        raise ValueError("invalid tool receipt cursor run_id")
    validate_thread_identifier(thread_id, field_name="tool receipt cursor thread_id")
    if type(after_seq) is not int or after_seq < 0:
        raise ValueError("invalid tool receipt cursor position")
    core = {
        "version": _TOOL_RECEIPT_CURSOR_VERSION,
        "run_id": run_id,
        "thread_id": thread_id,
        "after_seq": after_seq,
    }
    payload = {**core, "checksum": _receipt_cursor_checksum(core)}
    encoded = base64.urlsafe_b64encode(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")).rstrip(b"=").decode("ascii")
    return f"trc1.{encoded}"


def decode_tool_receipt_cursor(token: str, *, run_id: str, thread_id: str) -> int:
    if not isinstance(token, str) or len(token.encode("utf-8")) > 4096 or not token.startswith("trc1."):
        raise InvalidToolReceiptCursor("invalid tool receipt cursor")
    encoded = token[5:]
    try:
        raw = base64.b64decode(encoded + "=" * (-len(encoded) % 4), altchars=b"-_", validate=True)
        payload = json.loads(raw)
    except (ValueError, UnicodeError, json.JSONDecodeError) as exc:
        raise InvalidToolReceiptCursor("invalid tool receipt cursor") from exc
    expected = {"version", "run_id", "thread_id", "after_seq", "checksum"}
    if not isinstance(payload, dict) or set(payload) != expected:
        raise InvalidToolReceiptCursor("invalid tool receipt cursor fields")
    core = {key: payload[key] for key in ("version", "run_id", "thread_id", "after_seq")}
    if (
        payload["version"] != _TOOL_RECEIPT_CURSOR_VERSION
        or payload["run_id"] != run_id
        or payload["thread_id"] != thread_id
        or payload["checksum"] != _receipt_cursor_checksum(core)
        or type(payload["after_seq"]) is not int
        or payload["after_seq"] < 0
    ):
        raise InvalidToolReceiptCursor("tool receipt cursor scope or checksum mismatch")
    return payload["after_seq"]


@dataclass(frozen=True, slots=True)
class ToolReceiptLifecycleItem:
    receipt_id: str
    task_id: str
    kind: Literal["lead", "subagent"]
    subagent_name: str | None
    tool_name: str
    attempt: int
    status: Literal["succeeded", "failed", "denied", "cancelled", "indeterminate"]
    started_at: str
    finished_at: str | None
    request_projection_digest: str
    result_projection_digest: str | None
    result_kind: str | None
    safe_error_code: str | None
    authz_decision_ref: str | None
    guardrail_decision_refs: tuple[str, ...]
    agent_revision_digest: str
    assembly_fingerprint: str
    extension_generation: int
    subagent_catalog_digest: str
    subagent_definition_digest: str | None

    def to_dict(self) -> dict[str, object]:
        return {
            "receipt_id": self.receipt_id,
            "task_id": self.task_id,
            "kind": self.kind,
            "subagent_name": self.subagent_name,
            "tool_name": self.tool_name,
            "attempt": self.attempt,
            "status": self.status,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "request_projection_digest": self.request_projection_digest,
            "result_projection_digest": self.result_projection_digest,
            "result_kind": self.result_kind,
            "safe_error_code": self.safe_error_code,
            "authz_decision_ref": self.authz_decision_ref,
            "guardrail_decision_refs": self.guardrail_decision_refs,
            "agent_revision_digest": self.agent_revision_digest,
            "assembly_fingerprint": self.assembly_fingerprint,
            "extension_generation": self.extension_generation,
            "subagent_catalog_digest": self.subagent_catalog_digest,
            "subagent_definition_digest": self.subagent_definition_digest,
        }


@dataclass(frozen=True, slots=True)
class ToolReceiptPage:
    items: tuple[ToolReceiptLifecycleItem, ...]
    next_cursor: str | None
    pruned_before: str | None
    evidence_status: Literal["available", "legacy_unavailable", "invalid"]
    invalid_event_count: int = 0

    def to_dict(self) -> dict[str, object]:
        return {
            "items": tuple(item.to_dict() for item in self.items),
            "next_cursor": self.next_cursor,
            "pruned_before": self.pruned_before,
            "evidence_status": self.evidence_status,
            "invalid_event_count": self.invalid_event_count,
        }


def build_tool_receipt_page(
    events: list[dict] | tuple[dict, ...],
    *,
    run_id: str,
    thread_id: str,
    cursor: str | None,
    limit: int,
    legacy_unavailable: bool,
    events_include_prefix: bool = True,
) -> ToolReceiptPage:
    """Pair bounded start/outcome evidence without exposing corrupt payloads."""

    from deerflow.runtime.tool_evidence import (
        TOOL_RECEIPT_OUTCOME_EVENT,
        TOOL_RECEIPT_STARTED_EVENT,
        ToolEvidenceError,
        receipt_from_event,
    )

    if type(limit) is not int or not 1 <= limit <= MAX_TOOL_RECEIPT_PAGE_SIZE:
        raise ValueError("tool receipt limit must be between 1 and 100")
    after_seq = 0 if cursor is None else decode_tool_receipt_cursor(cursor, run_id=run_id, thread_id=thread_id)
    starts: list[tuple[int, Any, str]] = []
    outcomes: dict[str, tuple[Any, str]] = {}
    invalid = 0
    for event in sorted(events, key=lambda item: item.get("seq", -1)):
        try:
            if event.get("run_id") != run_id or event.get("thread_id") != thread_id:
                raise ToolEvidenceError("receipt_event_scope_invalid")
            seq = event.get("seq")
            event_type = event.get("event_type")
            if (
                type(seq) is not int
                or seq < 1
                or event.get("category") != "tool"
                or event_type
                not in {
                    TOOL_RECEIPT_STARTED_EVENT,
                    TOOL_RECEIPT_OUTCOME_EVENT,
                }
            ):
                raise ToolEvidenceError("receipt_event_invalid")
            receipt = receipt_from_event(event)
            if receipt.context.run_id != run_id:
                raise ToolEvidenceError("receipt_event_scope_invalid")
            timestamp = str(event["created_at"])
            if event_type == TOOL_RECEIPT_STARTED_EVENT:
                if receipt.phase != "started":
                    raise ToolEvidenceError("receipt_event_phase_invalid")
                starts.append((seq, receipt, timestamp))
            else:
                if receipt.phase == "started" or receipt.receipt_id in outcomes:
                    raise ToolEvidenceError("receipt_event_phase_invalid")
                outcomes[receipt.receipt_id] = (receipt, timestamp)
        except (ToolEvidenceError, KeyError, TypeError, ValueError):
            invalid += 1

    selected_starts = [entry for entry in starts if entry[0] > after_seq]
    has_more = len(selected_starts) > limit
    selected_starts = selected_starts[:limit]
    items: list[ToolReceiptLifecycleItem] = []
    selected_ids: set[str] = set()
    for _seq, started, started_at in selected_starts:
        if started.receipt_id in selected_ids:
            invalid += 1
            continue
        selected_ids.add(started.receipt_id)
        outcome_entry = outcomes.get(started.receipt_id)
        outcome = outcome_entry[0] if outcome_entry is not None else None
        finished_at = outcome_entry[1] if outcome_entry is not None else None
        if outcome is not None and (outcome.context != started.context or outcome.tool_name != started.tool_name or outcome.request_projection_digest != started.request_projection_digest):
            invalid += 1
            outcome = None
            finished_at = None
        context = started.context
        items.append(
            ToolReceiptLifecycleItem(
                receipt_id=started.receipt_id,
                task_id=context.execution_task_id,
                kind=context.execution_kind,
                subagent_name=context.subagent_name,
                tool_name=started.tool_name,
                attempt=context.attempt,
                status=(outcome.phase if outcome is not None else "indeterminate"),
                started_at=started_at,
                finished_at=finished_at,
                request_projection_digest=started.request_projection_digest,
                result_projection_digest=(outcome.result_projection_digest if outcome is not None else None),
                result_kind=(outcome.result_kind if outcome is not None else None),
                safe_error_code=(outcome.safe_error_code if outcome is not None else None),
                authz_decision_ref=(outcome.authz_decision_ref if outcome is not None else None),
                guardrail_decision_refs=(outcome.guardrail_decision_refs if outcome is not None else ()),
                agent_revision_digest=context.agent_revision_digest,
                assembly_fingerprint=context.assembly_fingerprint,
                extension_generation=context.extension_generation,
                subagent_catalog_digest=context.subagent_catalog_digest,
                subagent_definition_digest=context.subagent_definition_digest,
            )
        )
    # A store-side cursor window can begin with the outcome for the final
    # receipt on the preceding page. Its start is intentionally outside this
    # read, so only a complete-prefix projection may classify an unmatched
    # outcome as corrupt.
    if events_include_prefix:
        orphan_outcomes = set(outcomes) - {receipt.receipt_id for _, receipt, _ in starts}
        invalid += len(orphan_outcomes)
    next_cursor = None
    if has_more and selected_starts:
        next_cursor = encode_tool_receipt_cursor(
            run_id=run_id,
            thread_id=thread_id,
            after_seq=selected_starts[-1][0],
        )
    status: Literal["available", "legacy_unavailable", "invalid"]
    if invalid:
        status = "invalid"
    elif legacy_unavailable and not starts:
        status = "legacy_unavailable"
    else:
        status = "available"
    return ToolReceiptPage(
        items=tuple(items),
        next_cursor=next_cursor,
        pruned_before=None,
        evidence_status=status,
        invalid_event_count=invalid,
    )


@dataclass(frozen=True)
class LifecycleQuery:
    """One invocation or context query after authorization and visibility."""

    run_id: str | None = None
    thread_id: str | None = None
    owner_scope: str | None = None
    cursor: str | None = None
    limit: int = 100
    include_snapshot: bool = True
    source_kind: str | None = None
    visibility_scope: LifecycleVisibilityScope | None = None
    include_tool_receipts: bool = False
    tool_receipt_cursor: str | None = None
    tool_receipt_limit: int = 100

    def __post_init__(self) -> None:
        if (self.run_id is None) == (self.thread_id is None):
            raise ValueError("exactly one lifecycle query target is required")
        if type(self.limit) is not int or not 1 <= self.limit <= MAX_LIFECYCLE_PAGE_SIZE:
            raise ValueError(f"lifecycle query limit must be between 1 and {MAX_LIFECYCLE_PAGE_SIZE}")
        if self.cursor is not None:
            decode_lifecycle_cursor(self.cursor)
        if self.source_kind is not None and self.source_kind not in INVOCATION_SOURCE_KINDS:
            raise ValueError("unsupported invocation source kind")
        if self.visibility_scope is not None and not isinstance(
            self.visibility_scope,
            LifecycleVisibilityScope,
        ):
            raise TypeError("visibility_scope must be a LifecycleVisibilityScope or None")
        if self.visibility_scope is not None and self.visibility_scope.thread_id != self.thread_id:
            raise ValueError("lifecycle visibility scope is bound to another exact context")
        if type(self.include_tool_receipts) is not bool:
            raise TypeError("include_tool_receipts must be a boolean")
        if self.include_tool_receipts and self.run_id is None:
            raise ValueError("tool receipts require one exact invocation target")
        if type(self.tool_receipt_limit) is not int or not 1 <= self.tool_receipt_limit <= MAX_TOOL_RECEIPT_PAGE_SIZE:
            raise ValueError("tool receipt limit must be between 1 and 100")
        if self.tool_receipt_cursor is not None and not self.include_tool_receipts:
            raise ValueError("tool receipt cursor requires receipt inclusion")


@dataclass(frozen=True)
class LifecyclePage:
    """One bounded, consistently fenced lifecycle observation page."""

    snapshots: tuple[dict[str, Any], ...]
    events: tuple[dict[str, Any], ...]
    next_cursor: str
    minimum_available_cursor: str
    read_fence_cursor: str
    summaries: tuple[dict[str, Any], ...] = ()
    tool_receipts: ToolReceiptPage | None = None


def invocation_source_kind(row: Mapping[str, Any]) -> str | None:
    """Return a supported source kind from a persisted invocation row."""

    origin = row.get("origin_json")
    source_kind = origin.get("source_kind") if isinstance(origin, Mapping) else None
    return source_kind if isinstance(source_kind, str) and source_kind in INVOCATION_SOURCE_KINDS else None


def _safe_digest(value: Any) -> str | None:
    return value if isinstance(value, str) and _SHA256_RE.fullmatch(value) is not None else None


def _assembly_evidence_projection(row: Mapping[str, Any]) -> tuple[dict[str, Any] | None, str]:
    """Return only revalidated public evidence, never raw stored material."""

    raw_evidence = row.get("assembly_evidence_json")
    raw_digest = row.get("assembly_evidence_digest")
    if raw_evidence is None and raw_digest is None:
        status = str(row.get("status"))
        return None, "pending" if status in _NONTERMINAL_RUN_STATUSES else "legacy_unavailable"
    if not isinstance(raw_evidence, Mapping) or _safe_digest(raw_digest) is None:
        return None, "legacy_unavailable"
    try:
        evidence = AssemblyEvidenceV1.from_persisted_json(raw_evidence)
        if assembly_evidence_digest(evidence) != raw_digest:
            return None, "legacy_unavailable"
        accepted_revision_digest = _safe_digest(row.get("agent_revision_digest"))
        extension_generation = row.get("extension_generation")
        if (
            accepted_revision_digest is None
            or type(extension_generation) is not int
            or extension_generation < 0
            or evidence.accepted_agent_revision_digest != accepted_revision_digest
            or evidence.extension_generation != extension_generation
        ):
            return None, "legacy_unavailable"
    except (AssemblyEvidenceError, TypeError, ValueError):
        return None, "legacy_unavailable"
    return (
        {
            "version": evidence.version,
            "fingerprint": evidence.fingerprint,
            "effective_model": evidence.effective_model,
            "prompt_digest": evidence.prompt_digest,
            "toolset_digest": evidence.toolset_digest,
            "middleware_digest": evidence.middleware_digest,
            "skillset_digest": evidence.skillset_digest,
            "policy_digest": evidence.policy_digest,
        },
        "verified",
    )


def _subagent_catalog_projection(
    row: Mapping[str, Any],
) -> tuple[dict[str, Any] | None, str]:
    """Return the validated catalog summary without prompts or definitions."""

    revision = row.get("agent_revision_json")
    if not isinstance(revision, Mapping):
        return None, "legacy_unavailable"
    accepted_digest = _safe_digest(row.get("agent_revision_digest"))
    if accepted_digest is None or _safe_digest(revision.get("digest")) != accepted_digest:
        return None, "legacy_unavailable"
    raw_catalog = revision.get("subagent_catalog")
    if not isinstance(raw_catalog, Mapping):
        return None, "legacy_unavailable"
    try:
        from deerflow.runtime.subagent_snapshot import (
            ResolvedSubagentCatalogV1,
            SubagentCatalogError,
        )

        catalog = ResolvedSubagentCatalogV1.from_persisted_json(raw_catalog)
    except (SubagentCatalogError, TypeError, ValueError):
        return None, "legacy_unavailable"
    return (
        {
            "version": catalog.version,
            "digest": catalog.digest,
            "count": len(catalog.entries),
            "allowed_names": catalog.allowed_names,
        },
        "verified",
    )


def _safe_correlation_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int)):
        return value
    if isinstance(value, str):
        if len(value.encode("utf-8")) > _MAX_CORRELATION_VALUE_BYTES:
            raise ValueError("correlation reference value is too large")
        return value
    if isinstance(value, (list, tuple)):
        result = tuple(_safe_correlation_value(item) for item in value)
        if any(isinstance(item, tuple) for item in result):
            raise ValueError("nested correlation reference lists are unsupported")
        return result
    raise ValueError("unsupported correlation reference value")


def build_invocation_summary(row: Mapping[str, Any]) -> dict[str, Any] | None:
    """Project one normal accepted row into bounded public-safe summary facts."""

    if row.get("operation_kind", "run") != "run":
        return None
    source_kind = invocation_source_kind(row)
    origin = row.get("origin_json")
    if source_kind is None or not isinstance(origin, Mapping):
        # Historical rows without accepted Origin remain readable through the
        # legacy lifecycle snapshot/event fields, but cannot prove a source.
        return None
    try:
        references: list[dict[str, Any]] = []
        base = origin.get("references") or {}
        if not isinstance(base, Mapping):
            return None
        for key, value in sorted(base.items(), key=lambda item: str(item[0])):
            if not isinstance(key, str) or _PUBLIC_IDENTIFIER_RE.fullmatch(key) is None:
                return None
            references.append({"namespace": "origin", "key": key, "value": _safe_correlation_value(value)})
        contributor_references = origin.get("contributor_references") or ()
        if not isinstance(contributor_references, (list, tuple)):
            return None
        for reference in contributor_references:
            if not isinstance(reference, Mapping) or reference.get("storage_class") != "persistable" or reference.get("purpose") != "correlation":
                continue
            contribution_id = reference.get("contribution_id")
            namespace = reference.get("namespace")
            key = reference.get("key")
            if not all(isinstance(item, str) and item for item in (contribution_id, namespace, key)):
                return None
            public_namespace = f"{contribution_id}:{namespace}"
            if _PUBLIC_IDENTIFIER_RE.fullmatch(public_namespace) is None or _PUBLIC_IDENTIFIER_RE.fullmatch(key) is None:
                return None
            references.append(
                {
                    "namespace": public_namespace,
                    "key": key,
                    "value": _safe_correlation_value(reference.get("value")),
                }
            )
        if len(references) > _MAX_CORRELATION_REFERENCES:
            return None
        names = [(reference["namespace"], reference["key"]) for reference in references]
        if len(set(names)) != len(names):
            return None

        decisions = row.get("decision_evidence_json")
        decisions = decisions if isinstance(decisions, Mapping) else {}
        authorization_digests: list[str] = []
        raw_authorization = decisions.get("decisions") or ()
        if isinstance(raw_authorization, (list, tuple)):
            for decision in raw_authorization:
                digest = _safe_digest(decision.get("evidence_digest")) if isinstance(decision, Mapping) else None
                if digest is not None and digest not in authorization_digests:
                    authorization_digests.append(digest)
        constraints = decisions.get("constraints")
        constraint_evidence_digest = _safe_digest(constraints.get("evidence_digest")) if isinstance(constraints, Mapping) else None
        manifest = decisions.get("capability_manifest")
        extension_manifest_digest = _safe_digest(manifest.get("digest")) if isinstance(manifest, Mapping) else None
        assembly_evidence, assembly_evidence_status = _assembly_evidence_projection(row)
        subagent_catalog, subagent_catalog_status = _subagent_catalog_projection(row)
        summary = {
            "run_id": str(row["run_id"]),
            "thread_id": str(row["thread_id"]),
            "status": str(row["status"]),
            "state_version": int(row["state_version"]),
            "source_kind": source_kind,
            "correlation_references": tuple(references),
            "agent_revision_digest": _safe_digest(row.get("agent_revision_digest")),
            "extension_generation": (row.get("extension_generation") if type(row.get("extension_generation")) is int and row.get("extension_generation") >= 0 else None),
            "extension_manifest_digest": extension_manifest_digest,
            "caller_intent_digest": _safe_digest(row.get("caller_intent_digest")),
            "accepted_context_digest": _safe_digest(row.get("accepted_context_digest")),
            "authorization_evidence_digests": tuple(authorization_digests),
            "constraint_evidence_digest": constraint_evidence_digest,
            "assembly_evidence": assembly_evidence,
            "assembly_evidence_status": assembly_evidence_status,
            "subagent_catalog": subagent_catalog,
            "subagent_catalog_status": subagent_catalog_status,
        }
        encoded = json.dumps(summary, ensure_ascii=False, separators=(",", ":"), sort_keys=True, allow_nan=False).encode("utf-8")
        return summary if len(encoded) <= MAX_INVOCATION_SUMMARY_BYTES else None
    except (KeyError, TypeError, ValueError):
        return None


def validate_cursor_window(cursor: str | None, *, pruned_through: int, last_cursor: int) -> int:
    """Validate a requested cursor against the retained lifecycle interval."""

    requested = pruned_through if cursor is None else decode_lifecycle_cursor(cursor)
    if requested < pruned_through:
        raise CursorGap(encode_lifecycle_cursor(pruned_through))
    if requested > last_cursor:
        raise CursorAhead(encode_lifecycle_cursor(last_cursor))
    return requested


__all__ = [
    "CursorAhead",
    "CursorGap",
    "InvalidLifecycleCursor",
    "InvalidToolReceiptCursor",
    "LifecycleOrderingCorruption",
    "LifecyclePage",
    "LifecycleQuery",
    "MAX_LIFECYCLE_PAGE_SIZE",
    "MAX_TOOL_RECEIPT_PAGE_SIZE",
    "ToolReceiptLifecycleItem",
    "ToolReceiptPage",
    "build_tool_receipt_page",
    "decode_tool_receipt_cursor",
    "encode_tool_receipt_cursor",
    "build_invocation_summary",
    "decode_lifecycle_cursor",
    "encode_lifecycle_cursor",
    "invocation_source_kind",
    "validate_cursor_window",
]
