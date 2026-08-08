"""Immutable, standard-library-only contracts for durable invocations.

The records in this module are the wire-neutral Interface shared by embedded
and HTTP adapters. Construction snapshots every caller-owned JSON container;
``to_dict()`` returns a fresh mutable JSON-compatible copy.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass, field, fields
from enum import StrEnum
from types import MappingProxyType
from typing import Any, ClassVar, Literal, Protocol, Self, runtime_checkable

API_VERSION = "deerflow.runtime/v1"
type JsonScalar = str | int | float | bool | None
type JsonValue = JsonScalar | list[JsonValue] | dict[str, JsonValue]
type ImmutableJsonValue = JsonScalar | tuple[ImmutableJsonValue, ...] | Mapping[str, ImmutableJsonValue]

_RUN_STATUSES = frozenset({"pending", "running", "success", "error", "timeout", "interrupted"})


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


def _run_status(value: Any) -> str:
    if not isinstance(value, str) or value not in _RUN_STATUSES:
        raise ValueError("unsupported run status")
    return value


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
        _optional_nonempty(self.model_name, "model_name")
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

    Equal retries reuse the retained invocation; changed caller intent returns
    a conflict. Principal, scope, Origin, and accepted facts are host supplied.
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
        _nonempty(self.thread_id, "thread_id")
        _optional_nonempty(self.agent_hint, "agent_hint")
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
            _nonempty(self.thread_id, "thread_id")
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
    api_version: str = field(default=API_VERSION, init=False)
    kind: Literal["invocation.query"] = field(default=KIND, init=False)

    def __post_init__(self) -> None:
        _nonempty(self.run_id, "run_id")
        _optional_nonempty(self.cursor, "cursor")
        if type(self.limit) is not int or not 1 <= self.limit <= 1000:
            raise ValueError("limit must be between 1 and 1000")
        if type(self.include_snapshot) is not bool:
            raise TypeError("include_snapshot must be a boolean")


@dataclass(frozen=True)
class ContextInvocationsQuery(_Record):
    """Access-filtered lifecycle page query for one visible thread/context."""

    KIND: ClassVar[str] = "context.invocations.query"
    thread_id: str
    cursor: str | None = None
    limit: int = 100
    include_snapshot: bool = True
    api_version: str = field(default=API_VERSION, init=False)
    kind: Literal["context.invocations.query"] = field(default=KIND, init=False)

    def __post_init__(self) -> None:
        _nonempty(self.thread_id, "thread_id")
        _optional_nonempty(self.cursor, "cursor")
        if type(self.limit) is not int or not 1 <= self.limit <= 1000:
            raise ValueError("limit must be between 1 and 1000")
        if type(self.include_snapshot) is not bool:
            raise TypeError("include_snapshot must be a boolean")


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
    for name in ("event_id", "cursor", "run_id", "thread_id", "created_at"):
        _nonempty(row[name], name)
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


def _validate_snapshot(row: Mapping[str, ImmutableJsonValue]) -> None:
    _nonempty(row["run_id"], "run_id")
    _nonempty(row["thread_id"], "thread_id")
    _run_status(row["status"])
    _state_version(row["state_version"])


@dataclass(frozen=True)
class InvocationObservation(_Record):
    """An authoritative snapshot plus one at-least-once lifecycle page.

    Event IDs/cursors make repeated pages harmless. ``next_cursor`` advances
    within ``read_fence_cursor`` and ``minimum_available_cursor`` identifies
    the retention boundary after pruning.
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
    api_version: str = field(default=API_VERSION, init=False)
    kind: Literal["invocation.observation"] = field(default=KIND, init=False)

    def __post_init__(self) -> None:
        _nonempty(self.thread_id, "thread_id")
        if self.run_id is not None:
            _nonempty(self.run_id, "run_id")
            _run_status(self.status)
            _state_version(self.state_version)
        elif self.status is not None or self.state_version is not None:
            raise ValueError("context observations cannot carry singular run state")
        snapshots = _fixed_public_rows(self.snapshots, expected=_SNAPSHOT_FIELDS, label="snapshots")
        for snapshot in snapshots:
            _validate_snapshot(snapshot)
        object.__setattr__(self, "snapshots", snapshots)
        events = _fixed_public_rows(self.events, expected=_EVENT_FIELDS, label="events")
        for event in events:
            _validate_lifecycle_event(event)
        object.__setattr__(self, "events", events)
        _nonempty(self.next_cursor, "next_cursor")
        _nonempty(self.minimum_available_cursor, "minimum_available_cursor")
        _nonempty(self.read_fence_cursor, "read_fence_cursor")


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
            _nonempty(self.thread_id, "thread_id")
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
        if set(detail) != expected_fields:
            raise ValueError("runtime failure has invalid detail fields")
        if cursor_field is not None:
            _nonempty(detail[cursor_field], cursor_field)
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
        """Create or reuse one invocation for the request's external key."""

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
    "InvocationEnsureReceipt",
    "InvocationEnsureRequest",
    "InvocationObservation",
    "InvocationOptionsV1",
    "InvocationQuery",
    "ImmutableJsonValue",
    "JsonValue",
    "ResumeInputV1",
    "RuntimeCapabilities",
    "RuntimeFailure",
    "record_from_dict",
]
