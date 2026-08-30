"""Immutable, standard-library-only contracts for durable invocations.

The records in this module are the wire-neutral Interface shared by embedded
and HTTP adapters. Construction snapshots every caller-owned JSON container;
``to_dict()`` returns a fresh mutable JSON-compatible copy.
"""

from __future__ import annotations

import json
import math
import re
from collections.abc import Mapping
from dataclasses import dataclass, field, fields
from datetime import datetime
from enum import StrEnum
from types import MappingProxyType
from typing import Any, ClassVar, Literal, Protocol, Self, runtime_checkable

API_VERSION = "deerflow.runtime/v1"
type JsonScalar = str | int | float | bool | None
type JsonValue = JsonScalar | list[JsonValue] | dict[str, JsonValue]
type ImmutableJsonValue = JsonScalar | tuple[ImmutableJsonValue, ...] | Mapping[str, ImmutableJsonValue]

_RUN_STATUSES = frozenset({"pending", "running", "success", "error", "timeout", "interrupted"})
_INVOCATION_SOURCE_KINDS = frozenset({"http", "scheduled_task", "native_channel", "service"})
_CORRELATION_ID_RE = re.compile(r"[0-9a-f]{32}\Z")
_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_PUBLIC_IDENTIFIER_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_.:-]{0,191}\Z", re.ASCII)
_AGENT_IDENTIFIER_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9-]{0,127}\Z", re.ASCII)
_THREAD_IDENTIFIER_RE = re.compile(r"[A-Za-z0-9_-]{1,64}\Z", re.ASCII)
_TOOL_NAME_RE = re.compile(r"[A-Za-z0-9_.:/-]+\Z", re.ASCII)
_RECEIPT_ID_RE = re.compile(r"tr_[0-9a-f]{64}\Z")
_DECISION_REF_RE = re.compile(r"pd_[0-9a-f]{64}\Z")
_MAX_CORRELATION_VALUE_BYTES = 1024
_MAX_CORRELATION_REFERENCES = 64
_MAX_AUTHORIZATION_EVIDENCE_DIGESTS = 64
_MAX_INVOCATION_SUMMARY_BYTES = 16 * 1024
_ASSEMBLY_EVIDENCE_FIELDS = (
    "version",
    "fingerprint",
    "effective_model",
    "prompt_digest",
    "toolset_digest",
    "middleware_digest",
    "skillset_digest",
    "policy_digest",
)
_SUBAGENT_CATALOG_FIELDS = (
    "version",
    "digest",
    "count",
    "allowed_names",
)
MAX_OBSERVATION_PAGE_SIZE = 500
MAX_TOOL_RECEIPT_PAGE_SIZE = 100
MAX_LIFECYCLE_EVENT_PAYLOAD_BYTES = 4 * 1024
MAX_OBSERVATION_PAYLOAD_BYTES = 12 * 1024 * 1024
_SAFE_TOOL_RECEIPT_ERROR_CODES = frozenset(
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


class EnsureDisposition(StrEnum):
    """Finite outcomes for idempotent durable admission."""

    created = "created"
    known = "known"
    conflict = "conflict"
    denied = "denied"
    indeterminate = "indeterminate"
    thread_busy = "thread_busy"


class ControlDisposition(StrEnum):
    """Finite outcomes for version-fenced cancellation."""

    requested = "requested"
    already_requested = "already_requested"
    already_terminal = "already_terminal"
    stale = "stale"
    not_found_or_invisible = "not_found_or_invisible"
    denied = "denied"
    indeterminate = "indeterminate"


class FailureCode(StrEnum):
    """Stable, safe failure codes exposed by every portable adapter."""

    invalid_request = "invalid_request"
    denied = "denied"
    indeterminate = "indeterminate"
    not_found_or_invisible = "not_found_or_invisible"
    conflict = "conflict"
    thread_busy = "thread_busy"
    stale = "stale"
    cursor_gap = "cursor_gap"
    cursor_ahead = "cursor_ahead"


def _json_value(value: Any, *, object_only: bool = False) -> ImmutableJsonValue:
    if object_only and not isinstance(value, Mapping):
        raise TypeError("value must be a JSON object")
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("JSON numbers must be finite")
        return value
    if isinstance(value, (list, tuple)):
        return tuple(_json_value(item) for item in value)
    if isinstance(value, Mapping):
        if not all(isinstance(key, str) for key in value):
            raise TypeError("JSON object keys must be strings")
        return MappingProxyType({key: _json_value(item) for key, item in value.items()})
    raise TypeError(f"unsupported JSON value: {type(value).__name__}")


def _nonempty(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} must be a non-empty string")
    return value


def _optional_nonempty(value: Any, name: str) -> str | None:
    if value is None:
        return None
    return _nonempty(value, name)


def _optional_agent_identifier(value: Any, name: str) -> str | None:
    if value is None:
        return None
    if value == "lead_agent":
        return value
    if not isinstance(value, str) or _AGENT_IDENTIFIER_RE.fullmatch(value) is None:
        raise ValueError(f"{name} must be a 1-128 character ASCII agent identifier with an alphanumeric first character and only letters, digits, or hyphens, or the reserved built-in 'lead_agent'")
    return value.lower()


def _optional_model_profile_identifier(value: Any, name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value or len(value.encode("utf-8")) > 128:
        raise ValueError(f"{name} model profile identifier must be a non-empty string limited to 128 UTF-8 bytes")
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        raise ValueError(f"{name} model profile identifier must not contain ASCII control characters")
    return value


def _thread_identifier(value: Any, name: str = "thread_id") -> str:
    if not isinstance(value, str) or _THREAD_IDENTIFIER_RE.fullmatch(value) is None:
        raise ValueError(f"{name} must be a 1-64 character ASCII thread identifier containing only letters, digits, underscores, or hyphens")
    return value


def _run_status(value: Any) -> str:
    if not isinstance(value, str) or value not in _RUN_STATUSES:
        raise ValueError("unsupported run status")
    return value


def _source_kind(value: Any) -> str:
    if not isinstance(value, str) or value not in _INVOCATION_SOURCE_KINDS:
        raise ValueError("unsupported invocation source kind")
    return value


def _optional_digest(value: Any, name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"{name} must be a lowercase SHA-256 digest or null")
    return value


def _assembly_evidence(value: Any) -> Mapping[str, ImmutableJsonValue]:
    if not isinstance(value, Mapping) or set(value) != set(_ASSEMBLY_EVIDENCE_FIELDS):
        raise ValueError("assembly_evidence must contain exactly the bounded v1 projection")
    if type(value.get("version")) is not int or value["version"] != 1:
        raise ValueError("assembly_evidence version must be 1")
    effective_model = _optional_model_profile_identifier(value.get("effective_model"), "assembly_evidence")
    if effective_model is None:
        raise ValueError("assembly_evidence effective_model must be non-null")
    projected: dict[str, ImmutableJsonValue] = {
        "version": 1,
        "fingerprint": _optional_digest(value.get("fingerprint"), "assembly_evidence fingerprint"),
        "effective_model": effective_model,
    }
    for name in _ASSEMBLY_EVIDENCE_FIELDS[3:]:
        projected[name] = _optional_digest(value.get(name), f"assembly_evidence {name}")
    if any(projected[name] is None for name in _ASSEMBLY_EVIDENCE_FIELDS if name != "version"):
        raise ValueError("assembly_evidence digests and effective_model must be non-null")
    return MappingProxyType(projected)


def _subagent_catalog(value: Any) -> Mapping[str, ImmutableJsonValue]:
    if not isinstance(value, Mapping) or set(value) != set(_SUBAGENT_CATALOG_FIELDS):
        raise ValueError("subagent catalog must contain exactly the bounded v1 projection")
    if type(value.get("version")) is not int or value["version"] != 1:
        raise ValueError("subagent catalog version must be 1")
    digest = _optional_digest(value.get("digest"), "subagent catalog digest")
    count = value.get("count")
    if type(count) is not int or not 0 <= count <= 64:
        raise ValueError("subagent catalog count must be an integer from 0 through 64")
    raw_names = value.get("allowed_names")
    if not isinstance(raw_names, (list, tuple)):
        raise ValueError("subagent catalog allowed_names must be a list")
    names = tuple(raw_names)
    if len(names) != count:
        raise ValueError("subagent catalog count must equal its allowed name count")
    if any(not isinstance(name, str) or _AGENT_IDENTIFIER_RE.fullmatch(name) is None or name != name.lower() for name in names):
        raise ValueError("subagent catalog allowed names must be canonical agent identifiers")
    if names != tuple(sorted(set(names))):
        raise ValueError("subagent catalog allowed names must be sorted and unique")
    if digest is None:
        raise ValueError("subagent catalog digest must be non-null")
    return MappingProxyType(
        {
            "version": 1,
            "digest": digest,
            "count": count,
            "allowed_names": names,
        }
    )


def _correlation_value(value: Any) -> ImmutableJsonValue:
    if value is None or isinstance(value, (bool, int)):
        return value
    if isinstance(value, str):
        if len(value.encode("utf-8")) > _MAX_CORRELATION_VALUE_BYTES:
            raise ValueError("correlation reference strings are limited to 1 KiB UTF-8")
        return value
    if isinstance(value, (list, tuple)):
        frozen = tuple(_correlation_value(item) for item in value)
        if any(isinstance(item, tuple) for item in frozen):
            raise TypeError("correlation reference values cannot contain nested lists")
        return frozen
    raise TypeError("correlation references accept only strings, integers, booleans, null, or lists of those values")


def _state_version(value: Any, *, nullable: bool = False) -> int | None:
    if nullable and value is None:
        return None
    if type(value) is not int or value < 0:
        raise ValueError("state_version must be a non-negative integer")
    return value


def _wire(value: Any) -> JsonValue:
    if isinstance(value, _Record):
        return value.to_dict()
    if isinstance(value, StrEnum):
        return value.value
    if isinstance(value, (list, tuple)):
        return [_wire(item) for item in value]
    if isinstance(value, Mapping):
        return {str(key): _wire(item) for key, item in value.items()}
    return value


class _Record:
    KIND: ClassVar[str]

    def to_dict(self) -> dict[str, JsonValue]:
        return {item.name: _wire(getattr(self, item.name)) for item in fields(self)}

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> Self:
        if not isinstance(payload, Mapping):
            raise TypeError("runtime API record must be an object")
        expected = {item.name for item in fields(cls)}
        unknown = set(payload) - expected
        missing = expected - set(payload)
        if unknown:
            raise ValueError(f"unknown fields for {cls.KIND}: {', '.join(sorted(unknown))}")
        if missing:
            raise ValueError(f"missing fields for {cls.KIND}: {', '.join(sorted(missing))}")
        if payload["api_version"] != API_VERSION:
            raise ValueError("unsupported runtime API version")
        if payload["kind"] != cls.KIND:
            raise ValueError("unexpected runtime API record kind")
        values = dict(payload)
        values.pop("api_version")
        values.pop("kind")
        return cls._from_wire(values)

    @classmethod
    def _from_wire(cls, values: dict[str, Any]) -> Self:
        return cls(**values)


@dataclass(frozen=True)
class GraphInputV1(_Record):
    """A validated immutable snapshot of graph input JSON."""

    KIND: ClassVar[str] = "invocation.input.graph"
    value: Mapping[str, ImmutableJsonValue]
    api_version: str = field(default=API_VERSION, init=False)
    kind: Literal["invocation.input.graph"] = field(default=KIND, init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "value", _json_value(self.value, object_only=True))


@dataclass(frozen=True)
class ResumeInputV1(_Record):
    """A validated immutable snapshot of a graph resume value."""

    KIND: ClassVar[str] = "invocation.input.resume"
    value: ImmutableJsonValue
    api_version: str = field(default=API_VERSION, init=False)
    kind: Literal["invocation.input.resume"] = field(default=KIND, init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "value", _json_value(self.value))


@dataclass(frozen=True)
class InvocationOptionsV1(_Record):
    """Finite execution options whose values participate in replay identity."""

    KIND: ClassVar[str] = "invocation.options"
    model_name: str | None = None
    thinking_enabled: bool | None = None
    multitask_strategy: Literal["reject", "rollback", "interrupt"] = "reject"
    checkpoint_id: str | None = None
    interrupt_before: tuple[str, ...] | Literal["*"] | None = None
    interrupt_after: tuple[str, ...] | Literal["*"] | None = None
    api_version: str = field(default=API_VERSION, init=False)
    kind: Literal["invocation.options"] = field(default=KIND, init=False)

    def __post_init__(self) -> None:
        _optional_model_profile_identifier(self.model_name, "model_name")
        if self.thinking_enabled is not None and type(self.thinking_enabled) is not bool:
            raise TypeError("thinking_enabled must be a boolean or null")
        if self.multitask_strategy not in {"reject", "rollback", "interrupt"}:
            raise ValueError("unsupported multitask strategy")
        _optional_nonempty(self.checkpoint_id, "checkpoint_id")
        for name in ("interrupt_before", "interrupt_after"):
            value = getattr(self, name)
            if isinstance(value, list):
                object.__setattr__(self, name, tuple(value))
                value = getattr(self, name)
            if value not in (None, "*") and (not isinstance(value, tuple) or not all(isinstance(item, str) and item for item in value)):
                raise TypeError(f"{name} must be a string list, '*', or null")


@dataclass(frozen=True)
class InvocationEnsureRequest(_Record):
    """One source-keyed durable invocation request.

    These fields form canonical caller intent. Equal intent reuses the retained
    invocation and its separately accepted effective execution; changed intent
    conflicts. Replay does not repeat admission resolution or execution.
    Principal, scope, Origin, and accepted facts are host supplied.
    """

    KIND: ClassVar[str] = "invocation.ensure"
    external_key: str
    thread_id: str
    agent_hint: str | None
    input: GraphInputV1 | ResumeInputV1
    options: InvocationOptionsV1
    api_version: str = field(default=API_VERSION, init=False)
    kind: Literal["invocation.ensure"] = field(default=KIND, init=False)

    def __post_init__(self) -> None:
        _nonempty(self.external_key, "external_key")
        _thread_identifier(self.thread_id)
        object.__setattr__(
            self,
            "agent_hint",
            _optional_agent_identifier(self.agent_hint, "agent_hint"),
        )
        if not isinstance(self.input, (GraphInputV1, ResumeInputV1)):
            raise TypeError("input must be GraphInputV1 or ResumeInputV1")
        if not isinstance(self.options, InvocationOptionsV1):
            raise TypeError("options must be InvocationOptionsV1")

    @classmethod
    def _from_wire(cls, values: dict[str, Any]) -> Self:
        values["input"] = record_from_dict(values["input"])
        values["options"] = InvocationOptionsV1.from_dict(values["options"])
        return cls(**values)


@dataclass(frozen=True)
class InvocationEnsureReceipt(_Record):
    """Admission result, with invocation identity only for created/known rows."""

    KIND: ClassVar[str] = "invocation.ensure.receipt"
    disposition: EnsureDisposition | str
    run_id: str | None = None
    thread_id: str | None = None
    status: str | None = None
    state_version: int | None = None
    api_version: str = field(default=API_VERSION, init=False)
    kind: Literal["invocation.ensure.receipt"] = field(default=KIND, init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "disposition", EnsureDisposition(self.disposition))
        visible = self.disposition in {EnsureDisposition.created, EnsureDisposition.known}
        if visible:
            _nonempty(self.run_id, "run_id")
            _thread_identifier(self.thread_id)
            _run_status(self.status)
            _state_version(self.state_version)
        elif any(value is not None for value in (self.run_id, self.thread_id, self.status, self.state_version)):
            raise ValueError("non-visible ensure receipts cannot carry invocation fields")


@dataclass(frozen=True)
class InvocationQuery(_Record):
    """Access-filtered lifecycle page query for one invocation.

    ``cursor`` is opaque and ``include_snapshot`` controls snapshot inclusion;
    events remain ordered after the cursor through the adapter's read fence.
    """

    KIND: ClassVar[str] = "invocation.query"
    run_id: str
    cursor: str | None = None
    limit: int = 100
    include_snapshot: bool = True
    include_tool_receipts: bool = False
    tool_receipt_cursor: str | None = None
    tool_receipt_limit: int = 100
    api_version: str = field(default=API_VERSION, init=False)
    kind: Literal["invocation.query"] = field(default=KIND, init=False)

    def __post_init__(self) -> None:
        _nonempty(self.run_id, "run_id")
        _optional_nonempty(self.cursor, "cursor")
        if type(self.limit) is not int or not 1 <= self.limit <= MAX_OBSERVATION_PAGE_SIZE:
            raise ValueError(f"limit must be between 1 and {MAX_OBSERVATION_PAGE_SIZE}")
        if type(self.include_snapshot) is not bool:
            raise TypeError("include_snapshot must be a boolean")
        if type(self.include_tool_receipts) is not bool:
            raise TypeError("include_tool_receipts must be a boolean")
        _optional_nonempty(self.tool_receipt_cursor, "tool_receipt_cursor")
        if self.tool_receipt_cursor is not None and not self.include_tool_receipts:
            raise ValueError("tool_receipt_cursor requires include_tool_receipts")
        if type(self.tool_receipt_limit) is not int or not 1 <= self.tool_receipt_limit <= MAX_TOOL_RECEIPT_PAGE_SIZE:
            raise ValueError("tool_receipt_limit must be between 1 and 100")

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> Self:
        """Parse current queries and legacy v1 payloads without receipt fields."""

        if not isinstance(payload, Mapping):
            raise TypeError("runtime API record must be an object")
        expected = {item.name for item in fields(cls)}
        unknown = set(payload) - expected
        missing = expected - set(payload)
        if unknown:
            raise ValueError(f"unknown fields for {cls.KIND}: {', '.join(sorted(unknown))}")
        optional = {"include_tool_receipts", "tool_receipt_cursor", "tool_receipt_limit"}
        if missing - optional:
            raise ValueError(f"missing fields for {cls.KIND}: {', '.join(sorted(missing - optional))}")
        if payload["api_version"] != API_VERSION or payload["kind"] != cls.KIND:
            raise ValueError("unsupported runtime API query envelope")
        values = dict(payload)
        values.pop("api_version")
        values.pop("kind")
        values.setdefault("include_tool_receipts", False)
        values.setdefault("tool_receipt_cursor", None)
        values.setdefault("tool_receipt_limit", 100)
        return cls(**values)


@dataclass(frozen=True)
class ContextInvocationsQuery(_Record):
    """Access-filtered lifecycle page query for one visible thread/context.

    ``source_kind`` is a server-side filter over sealed accepted Origin facts;
    it never accepts caller-defined source metadata.
    """

    KIND: ClassVar[str] = "context.invocations.query"
    thread_id: str
    cursor: str | None = None
    limit: int = 100
    include_snapshot: bool = True
    source_kind: Literal["http", "scheduled_task", "native_channel", "service"] | None = None
    api_version: str = field(default=API_VERSION, init=False)
    kind: Literal["context.invocations.query"] = field(default=KIND, init=False)

    def __post_init__(self) -> None:
        _thread_identifier(self.thread_id)
        _optional_nonempty(self.cursor, "cursor")
        if type(self.limit) is not int or not 1 <= self.limit <= MAX_OBSERVATION_PAGE_SIZE:
            raise ValueError(f"limit must be between 1 and {MAX_OBSERVATION_PAGE_SIZE}")
        if type(self.include_snapshot) is not bool:
            raise TypeError("include_snapshot must be a boolean")
        if self.source_kind is not None:
            _source_kind(self.source_kind)

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> Self:
        """Parse current queries and legacy v1 payloads without a source filter."""

        if not isinstance(payload, Mapping):
            raise TypeError("runtime API record must be an object")
        expected = {item.name for item in fields(cls)}
        unknown = set(payload) - expected
        missing = expected - set(payload)
        if unknown:
            raise ValueError(f"unknown fields for {cls.KIND}: {', '.join(sorted(unknown))}")
        required_missing = missing - {"source_kind"}
        if required_missing:
            raise ValueError(f"missing fields for {cls.KIND}: {', '.join(sorted(required_missing))}")
        if payload["api_version"] != API_VERSION:
            raise ValueError("unsupported runtime API version")
        if payload["kind"] != cls.KIND:
            raise ValueError("unexpected runtime API record kind")
        values = dict(payload)
        values.pop("api_version")
        values.pop("kind")
        values.setdefault("source_kind", None)
        return cls(**values)


@dataclass(frozen=True)
class InvocationCorrelationReferenceV1(_Record):
    """One bounded, safe Origin correlation value in a stable namespace."""

    KIND: ClassVar[str] = "invocation.correlation-reference.v1"
    namespace: str
    key: str
    value: ImmutableJsonValue
    api_version: str = field(default=API_VERSION, init=False)
    kind: Literal["invocation.correlation-reference.v1"] = field(default=KIND, init=False)

    def __post_init__(self) -> None:
        for name in ("namespace", "key"):
            value = getattr(self, name)
            if not isinstance(value, str) or _PUBLIC_IDENTIFIER_RE.fullmatch(value) is None:
                raise ValueError(f"{name} must be a bounded ASCII identifier")
        object.__setattr__(self, "value", _correlation_value(self.value))


@dataclass(frozen=True)
class InvocationSummaryV1(_Record):
    """Safe accepted evidence and current state for one visible invocation.

    The summary is joined from its authoritative normal ``RunRow`` under the
    lifecycle page's read snapshot. It contains no model input, credentials,
    private policy reasons, or unbounded Origin data.
    """

    KIND: ClassVar[str] = "invocation.summary.v1"
    run_id: str
    thread_id: str
    status: str
    state_version: int
    source_kind: Literal["http", "scheduled_task", "native_channel", "service"]
    correlation_references: tuple[InvocationCorrelationReferenceV1, ...] = ()
    agent_revision_digest: str | None = None
    extension_generation: int | None = None
    extension_manifest_digest: str | None = None
    caller_intent_digest: str | None = None
    accepted_context_digest: str | None = None
    authorization_evidence_digests: tuple[str, ...] = ()
    constraint_evidence_digest: str | None = None
    assembly_evidence: Mapping[str, ImmutableJsonValue] | None = None
    assembly_evidence_status: Literal["legacy_unavailable", "pending", "verified"] | None = None
    subagent_catalog: Mapping[str, ImmutableJsonValue] | None = None
    subagent_catalog_status: Literal["legacy_unavailable", "verified"] | None = None
    api_version: str = field(default=API_VERSION, init=False)
    kind: Literal["invocation.summary.v1"] = field(default=KIND, init=False)

    def __post_init__(self) -> None:
        _nonempty(self.run_id, "run_id")
        _thread_identifier(self.thread_id)
        _run_status(self.status)
        _state_version(self.state_version)
        _source_kind(self.source_kind)
        references = tuple(self.correlation_references)
        if len(references) > _MAX_CORRELATION_REFERENCES:
            raise ValueError("an invocation summary may contain at most 64 correlation references")
        if not all(isinstance(reference, InvocationCorrelationReferenceV1) for reference in references):
            raise TypeError("correlation_references must contain InvocationCorrelationReferenceV1 values")
        if len({(reference.namespace, reference.key) for reference in references}) != len(references):
            raise ValueError("invocation summary correlation keys must be unique after namespacing")
        object.__setattr__(self, "correlation_references", references)
        _optional_digest(self.agent_revision_digest, "agent_revision_digest")
        if self.extension_generation is not None and (type(self.extension_generation) is not int or self.extension_generation < 0):
            raise ValueError("extension_generation must be a non-negative integer or null")
        for name in (
            "extension_manifest_digest",
            "caller_intent_digest",
            "accepted_context_digest",
            "constraint_evidence_digest",
        ):
            _optional_digest(getattr(self, name), name)
        authorization_digests = tuple(self.authorization_evidence_digests)
        if len(authorization_digests) > _MAX_AUTHORIZATION_EVIDENCE_DIGESTS:
            raise ValueError("an invocation summary may contain at most 64 authorization evidence digests")
        for digest in authorization_digests:
            if not isinstance(digest, str) or _SHA256_RE.fullmatch(digest) is None:
                raise ValueError("authorization_evidence_digest must be a lowercase SHA-256 digest")
        if len(set(authorization_digests)) != len(authorization_digests):
            raise ValueError("invocation summary authorization evidence digests must be unique")
        object.__setattr__(self, "authorization_evidence_digests", authorization_digests)
        evidence_status = self.assembly_evidence_status
        if evidence_status is None:
            evidence_status = "pending" if self.status in {"pending", "running"} else "legacy_unavailable"
            object.__setattr__(self, "assembly_evidence_status", evidence_status)
        if evidence_status not in {"legacy_unavailable", "pending", "verified"}:
            raise ValueError("unsupported assembly_evidence_status")
        if self.assembly_evidence is None:
            if evidence_status == "verified":
                raise ValueError("verified assembly evidence must be present")
        else:
            if evidence_status != "verified":
                raise ValueError("assembly evidence may be present only when verified")
            object.__setattr__(self, "assembly_evidence", _assembly_evidence(self.assembly_evidence))
        catalog_status = self.subagent_catalog_status
        if catalog_status is None:
            catalog_status = "legacy_unavailable"
            object.__setattr__(self, "subagent_catalog_status", catalog_status)
        if catalog_status not in {"legacy_unavailable", "verified"}:
            raise ValueError("unsupported subagent catalog status")
        if self.subagent_catalog is None:
            if catalog_status == "verified":
                raise ValueError("verified subagent catalog must be present")
        else:
            if catalog_status != "verified":
                raise ValueError("subagent catalog may be present only when verified")
            object.__setattr__(self, "subagent_catalog", _subagent_catalog(self.subagent_catalog))
        encoded = json.dumps(self.to_dict(), ensure_ascii=False, separators=(",", ":"), sort_keys=True, allow_nan=False).encode("utf-8")
        if len(encoded) > _MAX_INVOCATION_SUMMARY_BYTES:
            raise ValueError("an invocation summary is limited to 16 KiB canonical JSON")

    @classmethod
    def _from_wire(cls, values: dict[str, Any]) -> Self:
        references = values.get("correlation_references")
        if not isinstance(references, (list, tuple)):
            raise TypeError("correlation_references must be a list")
        values["correlation_references"] = tuple(InvocationCorrelationReferenceV1.from_dict(reference) for reference in references)
        return cls(**values)

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> Self:
        """Parse current summaries and legacy v1 payloads without assembly fields."""

        if not isinstance(payload, Mapping):
            raise TypeError("runtime API record must be an object")
        expected = {item.name for item in fields(cls)}
        unknown = set(payload) - expected
        missing = expected - set(payload)
        if unknown:
            raise ValueError(f"unknown fields for {cls.KIND}: {', '.join(sorted(unknown))}")
        additive_field_pairs = (
            {"assembly_evidence", "assembly_evidence_status"},
            {"subagent_catalog", "subagent_catalog_status"},
        )
        for field_pair in additive_field_pairs:
            if missing & field_pair and missing & field_pair != field_pair:
                raise ValueError(f"missing fields for {cls.KIND}: {', '.join(sorted(missing & field_pair))}")
        additive_fields = set().union(*additive_field_pairs)
        required_missing = missing - additive_fields
        if required_missing:
            raise ValueError(f"missing fields for {cls.KIND}: {', '.join(sorted(required_missing))}")
        if payload["api_version"] != API_VERSION:
            raise ValueError("unsupported runtime API version")
        if payload["kind"] != cls.KIND:
            raise ValueError("unexpected runtime API record kind")
        values = dict(payload)
        values.pop("api_version")
        values.pop("kind")
        values.setdefault("assembly_evidence", None)
        values.setdefault("assembly_evidence_status", None)
        values.setdefault("subagent_catalog", None)
        values.setdefault("subagent_catalog_status", None)
        return cls._from_wire(values)


_SNAPSHOT_FIELDS = {"run_id", "thread_id", "status", "state_version"}
_EVENT_FIELDS = {
    "event_id",
    "cursor",
    "run_id",
    "thread_id",
    "lifecycle_type",
    "state_version",
    "status",
    "created_at",
    "payload",
}
_LIFECYCLE_STATUSES_BY_TYPE = {
    "accepted": frozenset({"pending"}),
    "started": frozenset({"running"}),
    "cancellation_requested": frozenset({"pending", "running"}),
    "cancelled": frozenset({"error", "interrupted"}),
    "succeeded": frozenset({"success"}),
    "failed": frozenset({"error"}),
    "timed_out": frozenset({"timeout"}),
    "interrupted": frozenset({"error", "interrupted"}),
}

_TOOL_RECEIPT_PAGE_FIELDS = {
    "items",
    "next_cursor",
    "pruned_before",
    "evidence_status",
    "invalid_event_count",
}
_TOOL_RECEIPT_ITEM_FIELDS = {
    "receipt_id",
    "task_id",
    "kind",
    "subagent_name",
    "tool_name",
    "attempt",
    "status",
    "started_at",
    "finished_at",
    "request_projection_digest",
    "result_projection_digest",
    "result_kind",
    "safe_error_code",
    "authz_decision_ref",
    "guardrail_decision_refs",
    "agent_revision_digest",
    "assembly_fingerprint",
    "extension_generation",
    "subagent_catalog_digest",
    "subagent_definition_digest",
}


def _tool_receipt_page(value: Any) -> Mapping[str, ImmutableJsonValue]:
    if not isinstance(value, Mapping) or set(value) != _TOOL_RECEIPT_PAGE_FIELDS:
        raise ValueError("tool receipt page fields are invalid")
    items = value.get("items")
    if not isinstance(items, (list, tuple)) or len(items) > MAX_TOOL_RECEIPT_PAGE_SIZE:
        raise ValueError("tool receipt items must be a list of at most 100")
    for item in items:
        if not isinstance(item, Mapping) or set(item) != _TOOL_RECEIPT_ITEM_FIELDS:
            raise ValueError("tool receipt item fields are invalid")
        if not isinstance(item.get("receipt_id"), str) or _RECEIPT_ID_RE.fullmatch(item["receipt_id"]) is None:
            raise ValueError("tool receipt id is invalid")
        kind = item.get("kind")
        status = item.get("status")
        if kind not in {"lead", "subagent"} or status not in {
            "succeeded",
            "failed",
            "denied",
            "cancelled",
            "indeterminate",
        }:
            raise ValueError("tool receipt execution kind or status is invalid")
        if type(item.get("attempt")) is not int or item["attempt"] < 1:
            raise ValueError("tool receipt attempt is invalid")
        task_id = item.get("task_id")
        if not isinstance(task_id, str) or not task_id or len(task_id.encode("utf-8")) > 128:
            raise ValueError("tool receipt task id is invalid")
        subagent_name = item.get("subagent_name")
        if (kind == "lead" and subagent_name is not None) or (kind == "subagent" and (not isinstance(subagent_name, str) or not subagent_name or len(subagent_name.encode("utf-8")) > 128)):
            raise ValueError("tool receipt subagent name is invalid")
        tool_name = item.get("tool_name")
        if not isinstance(tool_name, str) or len(tool_name.encode("utf-8")) > 128 or _TOOL_NAME_RE.fullmatch(tool_name) is None:
            raise ValueError("tool receipt tool name is invalid")
        for timestamp_name in ("started_at", "finished_at"):
            timestamp = item.get(timestamp_name)
            if timestamp is None and timestamp_name == "finished_at":
                continue
            if not isinstance(timestamp, str) or len(timestamp.encode("utf-8")) > 64:
                raise ValueError(f"tool receipt {timestamp_name} is invalid")
            try:
                parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            except ValueError as exc:
                raise ValueError(f"tool receipt {timestamp_name} is invalid") from exc
            if parsed.tzinfo is None:
                raise ValueError(f"tool receipt {timestamp_name} is invalid")
        _optional_digest(item.get("request_projection_digest"), "tool receipt request projection")
        if item.get("request_projection_digest") is None:
            raise ValueError("tool receipt request projection digest is required")
        _optional_digest(item.get("result_projection_digest"), "tool receipt result projection")
        _optional_digest(item.get("agent_revision_digest"), "tool receipt agent revision")
        _optional_digest(item.get("assembly_fingerprint"), "tool receipt assembly fingerprint")
        _optional_digest(item.get("subagent_catalog_digest"), "tool receipt subagent catalog")
        if any(item.get(name) is None for name in ("agent_revision_digest", "assembly_fingerprint", "subagent_catalog_digest")):
            raise ValueError("tool receipt accepted anchor digest is required")
        definition_digest = _optional_digest(
            item.get("subagent_definition_digest"),
            "tool receipt subagent definition",
        )
        if (kind == "lead" and definition_digest is not None) or (kind == "subagent" and definition_digest is None):
            raise ValueError("tool receipt subagent definition anchor is invalid")
        if type(item.get("extension_generation")) is not int or item["extension_generation"] < 0:
            raise ValueError("tool receipt extension generation is invalid")
        result_kind = item.get("result_kind")
        if result_kind is not None and (not isinstance(result_kind, str) or not result_kind or len(result_kind.encode("utf-8")) > 64):
            raise ValueError("tool receipt result kind is invalid")
        safe_error_code = item.get("safe_error_code")
        if safe_error_code is not None and safe_error_code not in _SAFE_TOOL_RECEIPT_ERROR_CODES:
            raise ValueError("tool receipt safe error code is invalid")
        if status == "succeeded" and (item.get("result_projection_digest") is None or result_kind is None or safe_error_code is not None or item.get("finished_at") is None):
            raise ValueError("successful tool receipt outcome fields are invalid")
        if status in {"failed", "denied", "cancelled"} and (safe_error_code is None or item.get("finished_at") is None):
            raise ValueError("terminal tool receipt outcome fields are invalid")
        if status == "indeterminate" and any(
            item.get(name) is not None
            for name in (
                "finished_at",
                "result_projection_digest",
                "result_kind",
                "safe_error_code",
                "authz_decision_ref",
            )
        ):
            raise ValueError("indeterminate tool receipt outcome fields are invalid")
        authz_ref = item.get("authz_decision_ref")
        if authz_ref is not None and (not isinstance(authz_ref, str) or _DECISION_REF_RE.fullmatch(authz_ref) is None):
            raise ValueError("tool receipt authorization reference is invalid")
        guardrail_refs = item.get("guardrail_decision_refs")
        if not isinstance(guardrail_refs, (list, tuple)) or len(guardrail_refs) > 16 or any(not isinstance(ref, str) or _DECISION_REF_RE.fullmatch(ref) is None for ref in guardrail_refs):
            raise ValueError("tool receipt guardrail references are invalid")
        if len(set(guardrail_refs)) != len(guardrail_refs):
            raise ValueError("tool receipt guardrail references are invalid")
        if status == "indeterminate" and guardrail_refs:
            raise ValueError("indeterminate tool receipt outcome fields are invalid")
    if value.get("evidence_status") not in {"available", "legacy_unavailable", "invalid"}:
        raise ValueError("tool receipt evidence status is invalid")
    if type(value.get("invalid_event_count")) is not int or value["invalid_event_count"] < 0:
        raise ValueError("tool receipt invalid event count is invalid")
    for name in ("next_cursor", "pruned_before"):
        if value.get(name) is not None:
            cursor = _nonempty(value[name], name)
            if len(cursor.encode("utf-8")) > 4096:
                raise ValueError(f"{name} is too long")
    frozen = _json_value(value, object_only=True)
    assert isinstance(frozen, Mapping)
    return frozen


def _fixed_public_rows(
    rows: Any,
    *,
    expected: set[str],
    label: str,
) -> tuple[Mapping[str, ImmutableJsonValue], ...]:
    if not isinstance(rows, (list, tuple)):
        raise TypeError(f"{label} must be a list")
    result: list[Mapping[str, ImmutableJsonValue]] = []
    for row in rows:
        if not isinstance(row, Mapping) or set(row) != expected:
            raise ValueError(f"{label} contain invalid fields")
        frozen = _json_value(row, object_only=True)
        if not isinstance(frozen, Mapping):  # pragma: no cover - object_only contract
            raise TypeError(f"{label} must contain JSON objects")
        result.append(frozen)
    return tuple(result)


def _validate_lifecycle_event(row: Mapping[str, ImmutableJsonValue]) -> None:
    for name in ("event_id", "cursor", "run_id", "created_at"):
        _nonempty(row[name], name)
    _thread_identifier(row["thread_id"])
    lifecycle_type = row["lifecycle_type"]
    status = row["status"]
    allowed_statuses = _LIFECYCLE_STATUSES_BY_TYPE.get(lifecycle_type) if isinstance(lifecycle_type, str) else None
    if allowed_statuses is None:
        raise ValueError("unsupported lifecycle type")
    _run_status(status)
    if status not in allowed_statuses:
        raise ValueError("lifecycle type and status are incompatible")
    state_version = _state_version(row["state_version"])
    if state_version == 0:
        raise ValueError("lifecycle event state_version must be positive")
    payload = row["payload"]
    if not isinstance(payload, Mapping):
        raise TypeError("lifecycle event payload must be an object")
    encoded = json.dumps(_wire(payload), ensure_ascii=False, separators=(",", ":"), sort_keys=True, allow_nan=False).encode("utf-8")
    if len(encoded) > MAX_LIFECYCLE_EVENT_PAYLOAD_BYTES:
        raise ValueError("lifecycle event payload is limited to 4 KiB canonical JSON")


def _validate_snapshot(row: Mapping[str, ImmutableJsonValue]) -> None:
    _nonempty(row["run_id"], "run_id")
    _thread_identifier(row["thread_id"])
    _run_status(row["status"])
    _state_version(row["state_version"])


@dataclass(frozen=True)
class InvocationObservation(_Record):
    """One access-filtered, bounded, at-least-once lifecycle page.

    Cursor polling of these transactional lifecycle rows is the authoritative
    v1 evidence path and does not require an event sink. An asynchronous push
    path can accelerate delivery but cannot replace this correctness surface.
    Event IDs/cursors make repeated pages harmless. ``next_cursor`` advances
    within ``read_fence_cursor`` and ``minimum_available_cursor`` identifies
    the retention boundary after pruning. ``summaries`` joins safe accepted
    evidence for only the normal runs materialized by this page. Every row
    belongs to ``thread_id``; singular pages additionally bind every row to
    ``run_id``. Snapshot and summary run IDs are unique, each summary matches
    one snapshot's immutable identity and current state, and a singular
    snapshot matches the top-level current state. Historical events retain
    their own transition state and need not equal that current state.
    """

    KIND: ClassVar[str] = "invocation.observation"
    run_id: str | None
    thread_id: str
    status: str | None
    state_version: int | None
    snapshots: tuple[Mapping[str, ImmutableJsonValue], ...]
    events: tuple[Mapping[str, ImmutableJsonValue], ...]
    next_cursor: str
    minimum_available_cursor: str
    read_fence_cursor: str
    summaries: tuple[InvocationSummaryV1, ...] = ()
    tool_receipts: Mapping[str, ImmutableJsonValue] | None = None
    api_version: str = field(default=API_VERSION, init=False)
    kind: Literal["invocation.observation"] = field(default=KIND, init=False)

    def __post_init__(self) -> None:
        _thread_identifier(self.thread_id)
        if self.run_id is not None:
            _nonempty(self.run_id, "run_id")
            _run_status(self.status)
            _state_version(self.state_version)
        elif self.status is not None or self.state_version is not None:
            raise ValueError("context observations cannot carry singular run state")
        snapshots = _fixed_public_rows(self.snapshots, expected=_SNAPSHOT_FIELDS, label="snapshots")
        for snapshot in snapshots:
            _validate_snapshot(snapshot)
            if snapshot["thread_id"] != self.thread_id:
                raise ValueError("observation snapshots must belong to the observed thread")
            if self.run_id is not None and snapshot["run_id"] != self.run_id:
                raise ValueError("singular observation snapshots must belong to the observed run")
        if len({snapshot["run_id"] for snapshot in snapshots}) != len(snapshots):
            raise ValueError("observation snapshots contain duplicate run IDs")
        if self.run_id is not None and snapshots:
            current = snapshots[0]
            if current["status"] != self.status or current["state_version"] != self.state_version:
                raise ValueError("singular observation snapshot must agree with the top-level current state")
        object.__setattr__(self, "snapshots", snapshots)
        events = _fixed_public_rows(self.events, expected=_EVENT_FIELDS, label="events")
        for event in events:
            _validate_lifecycle_event(event)
            if event["thread_id"] != self.thread_id:
                raise ValueError("observation events must belong to the observed thread")
            if self.run_id is not None and event["run_id"] != self.run_id:
                raise ValueError("singular observation events must belong to the observed run")
        object.__setattr__(self, "events", events)
        _nonempty(self.next_cursor, "next_cursor")
        _nonempty(self.minimum_available_cursor, "minimum_available_cursor")
        _nonempty(self.read_fence_cursor, "read_fence_cursor")
        summaries = tuple(self.summaries)
        if not all(isinstance(summary, InvocationSummaryV1) for summary in summaries):
            raise TypeError("summaries must contain InvocationSummaryV1 values")
        if any(summary.thread_id != self.thread_id for summary in summaries):
            raise ValueError("observation summaries must belong to the observed thread")
        if self.run_id is not None and any(summary.run_id != self.run_id for summary in summaries):
            raise ValueError("singular observation summaries must belong to the observed run")
        if len({summary.run_id for summary in summaries}) != len(summaries):
            raise ValueError("observation summaries contain duplicate run IDs")
        snapshot_by_run_id = {snapshot["run_id"]: snapshot for snapshot in snapshots}
        if any(summary.run_id not in snapshot_by_run_id for summary in summaries):
            raise ValueError("every observation summary must have one materialized snapshot")
        for summary in summaries:
            snapshot = snapshot_by_run_id[summary.run_id]
            if summary.status != snapshot["status"] or summary.state_version != snapshot["state_version"]:
                raise ValueError("observation summary and snapshot current state must agree")
        object.__setattr__(self, "summaries", summaries)
        if self.tool_receipts is not None:
            if self.run_id is None:
                raise ValueError("tool receipts require a singular invocation observation")
            object.__setattr__(self, "tool_receipts", _tool_receipt_page(self.tool_receipts))
        encoded = json.dumps(self.to_dict(), ensure_ascii=False, separators=(",", ":"), sort_keys=True, allow_nan=False).encode("utf-8")
        if len(encoded) > MAX_OBSERVATION_PAYLOAD_BYTES:
            raise ValueError("an invocation observation is limited to 12 MiB canonical JSON")

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> Self:
        """Parse current observations and legacy v1 payloads without summaries."""

        if not isinstance(payload, Mapping):
            raise TypeError("runtime API record must be an object")
        expected = {item.name for item in fields(cls)}
        unknown = set(payload) - expected
        missing = expected - set(payload)
        if unknown:
            raise ValueError(f"unknown fields for {cls.KIND}: {', '.join(sorted(unknown))}")
        optional = {"summaries", "tool_receipts"}
        if missing - optional:
            raise ValueError(f"missing fields for {cls.KIND}: {', '.join(sorted(missing - optional))}")
        if payload["api_version"] != API_VERSION:
            raise ValueError("unsupported runtime API version")
        if payload["kind"] != cls.KIND:
            raise ValueError("unexpected runtime API record kind")
        values = dict(payload)
        values.pop("api_version")
        values.pop("kind")
        values.setdefault("summaries", ())
        values.setdefault("tool_receipts", None)
        return cls._from_wire(values)

    @classmethod
    def _from_wire(cls, values: dict[str, Any]) -> Self:
        summaries = values.get("summaries")
        if not isinstance(summaries, (list, tuple)):
            raise TypeError("summaries must be a list")
        values["summaries"] = tuple(InvocationSummaryV1.from_dict(summary) for summary in summaries)
        return cls(**values)


@dataclass(frozen=True)
class CancelInvocationRequest(_Record):
    """Request cancellation fenced by the caller's observed state version."""

    KIND: ClassVar[str] = "invocation.cancel"
    run_id: str
    expected_state_version: int
    action: Literal["interrupt", "rollback"] = "interrupt"
    api_version: str = field(default=API_VERSION, init=False)
    kind: Literal["invocation.cancel"] = field(default=KIND, init=False)

    def __post_init__(self) -> None:
        _nonempty(self.run_id, "run_id")
        _state_version(self.expected_state_version)
        if self.action not in {"interrupt", "rollback"}:
            raise ValueError("unsupported cancellation action")


@dataclass(frozen=True)
class InvocationControlReceipt(_Record):
    """Cancellation result with current state only when visibility permits."""

    KIND: ClassVar[str] = "invocation.control.receipt"
    disposition: ControlDisposition | str
    run_id: str | None = None
    thread_id: str | None = None
    status: str | None = None
    state_version: int | None = None
    api_version: str = field(default=API_VERSION, init=False)
    kind: Literal["invocation.control.receipt"] = field(default=KIND, init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "disposition", ControlDisposition(self.disposition))
        hidden = self.disposition in {
            ControlDisposition.not_found_or_invisible,
            ControlDisposition.denied,
            ControlDisposition.indeterminate,
        }
        values = (self.run_id, self.thread_id, self.status, self.state_version)
        if hidden:
            if any(value is not None for value in values):
                raise ValueError("hidden control receipts cannot carry invocation fields")
        else:
            _nonempty(self.run_id, "run_id")
            _thread_identifier(self.thread_id)
            _run_status(self.status)
            _state_version(self.state_version)


@dataclass(frozen=True)
class RuntimeCapabilities(_Record):
    """Truthful feature support for one durable invocation adapter."""

    KIND: ClassVar[str] = "runtime.capabilities"
    ensure: bool = True
    observe_invocation: bool = True
    observe_context: bool = True
    controls: tuple[str, ...] = ("cancel",)
    context_export: bool = False
    context_retirement: bool = False
    api_version: str = field(default=API_VERSION, init=False)
    kind: Literal["runtime.capabilities"] = field(default=KIND, init=False)

    def __post_init__(self) -> None:
        if isinstance(self.controls, list):
            object.__setattr__(self, "controls", tuple(self.controls))
        if self.controls != ("cancel",):
            raise ValueError("runtime v1 supports only the cancel control")
        for name in (
            "ensure",
            "observe_invocation",
            "observe_context",
            "context_export",
            "context_retirement",
        ):
            if type(getattr(self, name)) is not bool:
                raise TypeError(f"{name} must be a boolean")


@dataclass(frozen=True)
class RuntimeFailure(_Record):
    """Safe portable failure without exception text or private policy reasons."""

    KIND: ClassVar[str] = "runtime.failure"
    code: FailureCode | str
    detail: Mapping[str, ImmutableJsonValue]
    api_version: str = field(default=API_VERSION, init=False)
    kind: Literal["runtime.failure"] = field(default=KIND, init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "code", FailureCode(self.code))
        detail = _json_value(self.detail, object_only=True)
        if detail.get("version") != 1:
            raise ValueError("runtime failure detail must have version 1")
        expected_fields = {"version"}
        cursor_field: str | None = None
        if self.code is FailureCode.cursor_gap:
            cursor_field = "minimum_available_cursor"
        elif self.code is FailureCode.cursor_ahead:
            cursor_field = "read_fence_cursor"
        if cursor_field is not None:
            expected_fields.add(cursor_field)
        actual_fields = set(detail)
        if self.code is FailureCode.indeterminate and "correlation_id" in actual_fields:
            expected_fields.add("correlation_id")
        if actual_fields != expected_fields:
            raise ValueError("runtime failure has invalid detail fields")
        if cursor_field is not None:
            _nonempty(detail[cursor_field], cursor_field)
        correlation_id = detail.get("correlation_id")
        if correlation_id is not None and (not isinstance(correlation_id, str) or _CORRELATION_ID_RE.fullmatch(correlation_id) is None):
            raise ValueError("correlation_id must be 32 lowercase hexadecimal characters")
        object.__setattr__(self, "detail", detail)


@runtime_checkable
class DurableInvocationPort(Protocol):
    """Portable durable-invocation Seam implemented by every adapter.

    Implementations authenticate and bind the caller outside this Interface.
    They preserve idempotent ensure semantics, access-filter observations,
    version-fenced control, and finite safe failures without exposing stores,
    workers, graph objects, framework responses, or deployment state.
    """

    async def ensure(
        self,
        request: InvocationEnsureRequest,
    ) -> InvocationEnsureReceipt | RuntimeFailure:
        """Create or reuse by canonical intent and retain accepted execution."""

        ...

    async def observe(
        self,
        request: InvocationQuery | ContextInvocationsQuery,
    ) -> InvocationObservation | RuntimeFailure:
        """Read one access-filtered invocation or context lifecycle page."""

        ...

    async def control(
        self,
        request: CancelInvocationRequest,
    ) -> InvocationControlReceipt | RuntimeFailure:
        """Apply the supported version-fenced invocation control."""

        ...

    def capabilities(self) -> RuntimeCapabilities:
        """Return the adapter's finite supported operation set."""

        ...


_RECORDS: dict[str, type[_Record]] = {
    record.KIND: record
    for record in (
        GraphInputV1,
        ResumeInputV1,
        InvocationOptionsV1,
        InvocationEnsureRequest,
        InvocationEnsureReceipt,
        InvocationQuery,
        ContextInvocationsQuery,
        InvocationCorrelationReferenceV1,
        InvocationSummaryV1,
        InvocationObservation,
        CancelInvocationRequest,
        InvocationControlReceipt,
        RuntimeCapabilities,
        RuntimeFailure,
    )
}


def record_from_dict(
    payload: Mapping[str, Any],
) -> (
    GraphInputV1
    | ResumeInputV1
    | InvocationOptionsV1
    | InvocationEnsureRequest
    | InvocationEnsureReceipt
    | InvocationQuery
    | ContextInvocationsQuery
    | InvocationCorrelationReferenceV1
    | InvocationSummaryV1
    | InvocationObservation
    | CancelInvocationRequest
    | InvocationControlReceipt
    | RuntimeCapabilities
    | RuntimeFailure
):
    """Parse one strict v1 wire object into its immutable public record."""

    if not isinstance(payload, Mapping):
        raise TypeError("runtime API record must be an object")
    if payload.get("api_version") != API_VERSION:
        raise ValueError("unsupported runtime API version")
    kind = payload.get("kind")
    if not isinstance(kind, str) or kind not in _RECORDS:
        raise ValueError("unknown runtime API record kind")
    return _RECORDS[kind].from_dict(payload)


__all__ = [
    "API_VERSION",
    "CancelInvocationRequest",
    "ContextInvocationsQuery",
    "ControlDisposition",
    "DurableInvocationPort",
    "EnsureDisposition",
    "FailureCode",
    "GraphInputV1",
    "InvocationControlReceipt",
    "InvocationCorrelationReferenceV1",
    "InvocationEnsureReceipt",
    "InvocationEnsureRequest",
    "InvocationObservation",
    "InvocationOptionsV1",
    "InvocationQuery",
    "InvocationSummaryV1",
    "ImmutableJsonValue",
    "JsonValue",
    "MAX_OBSERVATION_PAGE_SIZE",
    "MAX_TOOL_RECEIPT_PAGE_SIZE",
    "MAX_LIFECYCLE_EVENT_PAYLOAD_BYTES",
    "MAX_OBSERVATION_PAYLOAD_BYTES",
    "ResumeInputV1",
    "RuntimeCapabilities",
    "RuntimeFailure",
    "record_from_dict",
]
