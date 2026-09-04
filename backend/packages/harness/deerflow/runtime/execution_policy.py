"""Accepted execution budgets and deterministic circuit-breaker decisions.

This module is deliberately transport and persistence neutral.  It owns the
canonical policy contracts, the pure evaluator, and the private keyed equality
commitment used for repeated-tool detection.  Runtime adapters own clocks,
fences, durable writes, and cancellation.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import math
import os
import re
import secrets
import unicodedata
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from enum import StrEnum
from functools import lru_cache
from types import MappingProxyType
from typing import Literal

EXECUTION_POLICY_HMAC_KEYS_ENV = "EXECUTION_POLICY_HMAC_KEYS"
EXECUTION_POLICY_HMAC_ACTIVE_KEY_ID_ENV = "EXECUTION_POLICY_HMAC_ACTIVE_KEY_ID"
EXECUTION_BUDGET_VERSION = 1
EXECUTION_POLICY_STATE_VERSION = 1
EXECUTION_POLICY_OBSERVATION_VERSION = 1
TOOL_EQUIVALENCE_COMMITMENT_VERSION = 1
TOOL_EQUIVALENCE_NORMALIZER_VERSION = 1
EXECUTION_POLICY_OBSERVER_CONTEXT_KEY = "__execution_policy_observer_v1"

_BUDGET_DOMAIN = b"hartmesh.execution-budget/v1\0"
_STATE_DOMAIN = b"hartmesh.execution-policy-state/v1\0"
_DECISION_OUTBOX_DOMAIN = b"hartmesh.execution-policy-decision-outbox/v1\0"
_COMMITMENT_DOMAIN = b"hartmesh.tool-equivalence/v1\0"
_NORMALIZER_MANIFEST_DOMAIN = b"hartmesh.tool-equivalence-normalizers/v1\0"
_KEY_CONFIRMATION_DOMAIN = b"hartmesh.execution-policy-key-confirmation/v1\0"
_KEYRING_CONFIRMATION_DOMAIN = b"hartmesh.execution-policy-keyring/v1\0"
_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
_KEY_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,31}$")
_SAFE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_SECRET_FIELD_TOKENS = frozenset(
    {
        "apikey",
        "authorization",
        "bearer",
        "cookie",
        "credential",
        "oauth",
        "password",
        "secret",
        "session",
        "token",
    }
)
_NORMALIZER_ALLOWED_FIELDS: dict[str, frozenset[str]] = {
    "write_file": frozenset({"path", "content"}),
    "str_replace": frozenset({"path", "old_str", "new_str", "replace_all"}),
    "web_search": frozenset({"query", "max_results", "time_range"}),
    "image_search": frozenset({"query", "max_results", "size", "type_image", "layout"}),
    "memory_search": frozenset({"query", "category", "limit"}),
    "web_fetch": frozenset({"url"}),
}
_MIN_KEY_BYTES = 32
_MAX_KEY_BYTES = 128
_MAX_KEYS = 8
_MAX_ENV_BYTES = 16_384
_MAX_CANONICAL_BYTES = 64 * 1024
_MAX_WINDOW = 256


class ExecutionPolicyError(ValueError):
    """A bounded machine-code failure at the execution-policy boundary."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def _canonical(value: object, *, depth: int = 0) -> object:
    if depth > 16:
        raise ExecutionPolicyError("execution_policy_value_invalid")
    if value is None or isinstance(value, bool | int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ExecutionPolicyError("execution_policy_value_invalid")
        return value
    if isinstance(value, str):
        if len(value.encode("utf-8")) > 4096:
            raise ExecutionPolicyError("execution_policy_value_invalid")
        return unicodedata.normalize("NFC", value)
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise ExecutionPolicyError("execution_policy_value_invalid")
        return {unicodedata.normalize("NFC", key): _canonical(child, depth=depth + 1) for key, child in sorted(value.items())}
    if isinstance(value, Sequence) and not isinstance(value, bytes | bytearray | str):
        if len(value) > 512:
            raise ExecutionPolicyError("execution_policy_value_invalid")
        return [_canonical(child, depth=depth + 1) for child in value]
    raise ExecutionPolicyError("execution_policy_value_invalid")


def _canonical_bytes(value: object) -> bytes:
    rendered = json.dumps(
        _canonical(value),
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    if len(rendered) > _MAX_CANONICAL_BYTES:
        raise ExecutionPolicyError("execution_policy_value_invalid")
    return rendered


def _digest(domain: bytes, value: object) -> str:
    return hashlib.sha256(domain + _canonical_bytes(value)).hexdigest()


def _require_digest(value: object, code: str = "execution_policy_digest_invalid") -> str:
    if not isinstance(value, str) or _DIGEST_RE.fullmatch(value) is None:
        raise ExecutionPolicyError(code)
    return value


def _require_positive(value: object, name: str, *, maximum: int) -> int:
    if type(value) is not int or value < 1 or value > maximum:
        raise ExecutionPolicyError(f"execution_budget_{name}_invalid")
    return value


_BUDGET_LIMIT_FIELDS = (
    "max_agent_turns",
    "max_total_tool_attempts",
    "repeated_tool_warn",
    "repeated_tool_stop",
    "repeated_tool_window",
    "max_no_progress_observations",
    "max_batches",
    "max_batch_items",
    "max_batch_concurrency",
    "max_batch_attempts",
    "max_batch_runtime_seconds",
    "max_delegation_depth",
    "max_retrieval_calls",
    "max_retrieval_results",
    "max_retrieval_sources",
    "max_retrieval_bytes",
    "max_sandbox_operations",
    "max_sandbox_runtime_seconds",
    "terminal_grace_seconds",
)


@dataclass(frozen=True, slots=True)
class ExecutionBudgetV1:
    """Canonical immutable limits accepted before durable model work."""

    profile: str
    equivalence_key_id: str
    equivalence_normalizer_manifest_digest: str
    max_agent_turns: int = 1000
    max_total_tool_attempts: int = 1000
    per_tool_category_attempts: tuple[tuple[str, int], ...] = ()
    repeated_tool_warn: int = 3
    repeated_tool_stop: int = 5
    repeated_tool_window: int = 20
    max_no_progress_observations: int = 3
    max_batches: int = 16
    max_batch_items: int = 1000
    max_batch_concurrency: int = 32
    max_batch_attempts: int = 3000
    max_batch_runtime_seconds: int = 86_400
    max_delegation_depth: int = 4
    max_retrieval_calls: int = 100
    max_retrieval_results: int = 1000
    max_retrieval_sources: int = 1000
    max_retrieval_bytes: int = 10 * 1024 * 1024
    max_sandbox_operations: int = 1000
    max_sandbox_runtime_seconds: int = 86_400
    terminal_grace_seconds: int = 30

    def __post_init__(self) -> None:
        if not isinstance(self.profile, str) or _SAFE_ID_RE.fullmatch(self.profile) is None:
            raise ExecutionPolicyError("execution_budget_profile_invalid")
        if not isinstance(self.equivalence_key_id, str) or _KEY_ID_RE.fullmatch(self.equivalence_key_id) is None:
            raise ExecutionPolicyError("execution_budget_key_id_invalid")
        _require_digest(self.equivalence_normalizer_manifest_digest)
        maxima = {
            "max_agent_turns": 1_000_000,
            "max_total_tool_attempts": 1_000_000,
            "repeated_tool_warn": _MAX_WINDOW,
            "repeated_tool_stop": _MAX_WINDOW,
            "repeated_tool_window": _MAX_WINDOW,
            "max_no_progress_observations": 10_000,
            "max_batches": 100_000,
            "max_batch_items": 1_000_000,
            "max_batch_concurrency": 100_000,
            "max_batch_attempts": 3_000_000,
            "max_batch_runtime_seconds": 31_536_000,
            "max_delegation_depth": 1_000,
            "max_retrieval_calls": _MAX_WINDOW,
            "max_retrieval_results": 10_000_000,
            "max_retrieval_sources": 10_000_000,
            "max_retrieval_bytes": 10 * 1024 * 1024 * 1024,
            "max_sandbox_operations": 1_000_000,
            "max_sandbox_runtime_seconds": 31_536_000,
            "terminal_grace_seconds": 3600,
        }
        for name, maximum in maxima.items():
            _require_positive(getattr(self, name), name, maximum=maximum)
        if self.repeated_tool_warn > self.repeated_tool_stop or self.repeated_tool_stop > self.repeated_tool_window:
            raise ExecutionPolicyError("execution_budget_repeated_tool_threshold_invalid")
        normalized_categories: list[tuple[str, int]] = []
        for category, limit in self.per_tool_category_attempts:
            if not isinstance(category, str) or _SAFE_ID_RE.fullmatch(category) is None:
                raise ExecutionPolicyError("execution_budget_tool_category_invalid")
            normalized_categories.append((category, _require_positive(limit, "tool_category", maximum=1_000_000)))
        if tuple(sorted(normalized_categories)) != self.per_tool_category_attempts or len({item[0] for item in normalized_categories}) != len(normalized_categories):
            raise ExecutionPolicyError("execution_budget_tool_category_invalid")

    @classmethod
    def build(
        cls,
        *,
        profile: str = "default",
        equivalence_key_id: str = "local-ephemeral-v1",
        equivalence_normalizer_manifest_digest: str | None = None,
        per_tool_category_attempts: Mapping[str, int] | None = None,
        **limits: int,
    ) -> ExecutionBudgetV1:
        unknown = set(limits) - set(_BUDGET_LIMIT_FIELDS)
        if unknown:
            raise ExecutionPolicyError("execution_budget_field_forbidden")
        return cls(
            profile=profile,
            equivalence_key_id=equivalence_key_id,
            equivalence_normalizer_manifest_digest=(equivalence_normalizer_manifest_digest or normalizer_manifest_digest()),
            per_tool_category_attempts=tuple(sorted((per_tool_category_attempts or {}).items())),
            **limits,
        )

    @property
    def digest(self) -> str:
        return _digest(_BUDGET_DOMAIN, self._projection())

    def _projection(self) -> dict[str, object]:
        return {
            "version": EXECUTION_BUDGET_VERSION,
            "profile": self.profile,
            "equivalence_key_id": self.equivalence_key_id,
            "equivalence_normalizer_manifest_digest": self.equivalence_normalizer_manifest_digest,
            **{name: getattr(self, name) for name in _BUDGET_LIMIT_FIELDS},
            "per_tool_category_attempts": dict(self.per_tool_category_attempts),
        }

    def to_json(self) -> dict[str, object]:
        return {**self._projection(), "digest": self.digest}

    @classmethod
    def from_json(cls, value: Mapping[str, object]) -> ExecutionBudgetV1:
        expected = {
            "version",
            "profile",
            "equivalence_key_id",
            "equivalence_normalizer_manifest_digest",
            "per_tool_category_attempts",
            *_BUDGET_LIMIT_FIELDS,
            "digest",
        }
        if set(value) != expected or value.get("version") != EXECUTION_BUDGET_VERSION:
            raise ExecutionPolicyError("execution_budget_version_unsupported")
        categories = value.get("per_tool_category_attempts")
        if not isinstance(categories, Mapping) or any(not isinstance(key, str) for key in categories):
            raise ExecutionPolicyError("execution_budget_tool_category_invalid")
        budget = cls.build(
            profile=value["profile"],  # type: ignore[arg-type]
            equivalence_key_id=value["equivalence_key_id"],  # type: ignore[arg-type]
            equivalence_normalizer_manifest_digest=value["equivalence_normalizer_manifest_digest"],  # type: ignore[arg-type]
            per_tool_category_attempts=dict(categories),  # type: ignore[arg-type]
            **{name: value[name] for name in _BUDGET_LIMIT_FIELDS},  # type: ignore[arg-type]
        )
        if _require_digest(value.get("digest")) != budget.digest:
            raise ExecutionPolicyError("execution_policy_digest_invalid")
        return budget

    def narrow(self, requested: Mapping[str, object]) -> ExecutionBudgetV1:
        forbidden = set(requested) - set(_BUDGET_LIMIT_FIELDS) - {"per_tool_category_attempts"}
        if forbidden:
            raise ExecutionPolicyError("execution_budget_field_forbidden")
        changes: dict[str, object] = {}
        for name in _BUDGET_LIMIT_FIELDS:
            if name not in requested:
                continue
            candidate = requested[name]
            if type(candidate) is not int or candidate > getattr(self, name):
                raise ExecutionPolicyError("execution_budget_broadening_forbidden")
            changes[name] = candidate
        if "per_tool_category_attempts" in requested:
            categories = requested["per_tool_category_attempts"]
            if not isinstance(categories, Mapping):
                raise ExecutionPolicyError("execution_budget_field_invalid")
            current = dict(self.per_tool_category_attempts)
            # Omitted categories retain their server ceiling. Replacing the
            # map with only caller-supplied entries would silently broaden
            # every omitted category.
            narrowed = dict(current)
            for category, candidate in categories.items():
                if category not in current or type(candidate) is not int or candidate > current[category]:
                    raise ExecutionPolicyError("execution_budget_broadening_forbidden")
                narrowed[str(category)] = candidate
            changes["per_tool_category_attempts"] = tuple(sorted(narrowed.items()))
        return replace(self, **changes)


class PolicyDecision(StrEnum):
    allow = "allow"
    warn = "warn"
    stop = "stop"


@dataclass(frozen=True, slots=True)
class PolicyDecisionOutboxV1:
    """Safe decision payload retained until its fenced event is confirmed."""

    decision: PolicyDecision
    reason_code: str
    current: int
    limit: int
    state_digest: str
    summary_key: str

    def __post_init__(self) -> None:
        if not isinstance(self.decision, PolicyDecision):
            raise ExecutionPolicyError("execution_policy_state_invalid")
        if _SAFE_ID_RE.fullmatch(self.reason_code) is None:
            raise ExecutionPolicyError("execution_policy_state_invalid")
        if type(self.current) is not int or self.current < 0:
            raise ExecutionPolicyError("execution_policy_state_invalid")
        if type(self.limit) is not int or self.limit < 0:
            raise ExecutionPolicyError("execution_policy_state_invalid")
        _require_digest(self.state_digest)
        if _SAFE_ID_RE.fullmatch(self.summary_key) is None:
            raise ExecutionPolicyError("execution_policy_state_invalid")

    def to_json(self) -> dict[str, object]:
        return {
            "decision": self.decision.value,
            "reason_code": self.reason_code,
            "current": self.current,
            "limit": self.limit,
            "state_digest": self.state_digest,
            "summary_key": self.summary_key,
        }

    @classmethod
    def from_json(cls, value: Mapping[str, object]) -> PolicyDecisionOutboxV1:
        if set(value) != {
            "decision",
            "reason_code",
            "current",
            "limit",
            "state_digest",
            "summary_key",
        }:
            raise ExecutionPolicyError("execution_policy_state_invalid")
        try:
            return cls(
                decision=PolicyDecision(value["decision"]),
                reason_code=value["reason_code"],  # type: ignore[arg-type]
                current=value["current"],  # type: ignore[arg-type]
                limit=value["limit"],  # type: ignore[arg-type]
                state_digest=value["state_digest"],  # type: ignore[arg-type]
                summary_key=value["summary_key"],  # type: ignore[arg-type]
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ExecutionPolicyError("execution_policy_state_invalid") from exc


ObservationKind = Literal[
    "turn",
    "tool_attempt",
    "no_progress",
    "batch",
    "retrieval",
    "sandbox",
]


@dataclass(frozen=True, slots=True)
class ExecutionPolicyObservationV1:
    kind: ObservationKind
    count: int = 1
    attempt_count: int = 0
    bytes_used: int = 0
    runtime_seconds: int = 0
    result_count: int = 0
    source_count: int = 0
    observation_id: str | None = None
    tool_name: str | None = None
    tool_category: str | None = None
    equivalence_commitment: str | None = None

    def __post_init__(self) -> None:
        if self.kind not in {"turn", "tool_attempt", "no_progress", "batch", "retrieval", "sandbox"}:
            raise ExecutionPolicyError("execution_policy_observation_invalid")
        for name in (
            "count",
            "attempt_count",
            "bytes_used",
            "runtime_seconds",
            "result_count",
            "source_count",
        ):
            value = getattr(self, name)
            if type(value) is not int or value < 0:
                raise ExecutionPolicyError("execution_policy_observation_invalid")
        for value in (self.tool_name, self.tool_category):
            if value is not None and (not isinstance(value, str) or _SAFE_ID_RE.fullmatch(value) is None):
                raise ExecutionPolicyError("execution_policy_observation_invalid")
        if self.observation_id is not None and (not isinstance(self.observation_id, str) or _SAFE_ID_RE.fullmatch(self.observation_id) is None):
            raise ExecutionPolicyError("execution_policy_observation_invalid")
        if self.equivalence_commitment is not None:
            _require_digest(self.equivalence_commitment)
        if self.kind == "tool_attempt" and (self.tool_name is None or self.tool_category is None):
            raise ExecutionPolicyError("execution_policy_observation_invalid")

    @classmethod
    def tool_attempt(
        cls,
        *,
        tool_name: str,
        tool_category: str,
        equivalence_commitment: str | None,
        observation_id: str | None = None,
    ) -> ExecutionPolicyObservationV1:
        return cls(
            kind="tool_attempt",
            observation_id=observation_id,
            tool_name=tool_name,
            tool_category=tool_category,
            equivalence_commitment=equivalence_commitment,
        )


@dataclass(frozen=True, slots=True)
class ExecutionPolicyStateV1:
    budget_digest: str
    turns: int = 0
    total_tool_attempts: int = 0
    tool_category_attempts: tuple[tuple[str, int], ...] = ()
    no_progress_observations: int = 0
    batches: int = 0
    batch_items: int = 0
    batch_attempts: int = 0
    batch_runtime_seconds: int = 0
    retrieval_calls: int = 0
    retrieval_results: int = 0
    retrieval_sources: int = 0
    retrieval_bytes: int = 0
    sandbox_operations: int = 0
    sandbox_runtime_seconds: int = 0
    recent_tool_commitments: tuple[str, ...] = ()
    emitted_decisions: tuple[str, ...] = ()
    decision_outbox: tuple[PolicyDecisionOutboxV1, ...] = ()
    processed_observation_ids: tuple[str, ...] = ()
    terminal_reason: str | None = None

    def __post_init__(self) -> None:
        _require_digest(self.budget_digest)
        for name in (
            "turns",
            "total_tool_attempts",
            "no_progress_observations",
            "batches",
            "batch_items",
            "batch_attempts",
            "batch_runtime_seconds",
            "retrieval_calls",
            "retrieval_results",
            "retrieval_sources",
            "retrieval_bytes",
            "sandbox_operations",
            "sandbox_runtime_seconds",
        ):
            value = getattr(self, name)
            if type(value) is not int or value < 0:
                raise ExecutionPolicyError("execution_policy_state_invalid")
        if tuple(sorted(self.tool_category_attempts)) != self.tool_category_attempts or len({item[0] for item in self.tool_category_attempts}) != len(self.tool_category_attempts):
            raise ExecutionPolicyError("execution_policy_state_invalid")
        for category, count in self.tool_category_attempts:
            if _SAFE_ID_RE.fullmatch(category) is None or type(count) is not int or count < 0:
                raise ExecutionPolicyError("execution_policy_state_invalid")
        if len(self.recent_tool_commitments) > _MAX_WINDOW or any(_DIGEST_RE.fullmatch(value) is None for value in self.recent_tool_commitments):
            raise ExecutionPolicyError("execution_policy_state_invalid")
        if len(self.emitted_decisions) > 64 or tuple(sorted(set(self.emitted_decisions))) != self.emitted_decisions:
            raise ExecutionPolicyError("execution_policy_state_invalid")
        if len(self.decision_outbox) > 64 or any(not isinstance(item, PolicyDecisionOutboxV1) for item in self.decision_outbox) or len({item.state_digest for item in self.decision_outbox}) != len(self.decision_outbox):
            raise ExecutionPolicyError("execution_policy_state_invalid")
        if len(self.processed_observation_ids) > _MAX_WINDOW or len(set(self.processed_observation_ids)) != len(self.processed_observation_ids) or any(_SAFE_ID_RE.fullmatch(value) is None for value in self.processed_observation_ids):
            raise ExecutionPolicyError("execution_policy_state_invalid")
        if self.terminal_reason is not None and _SAFE_ID_RE.fullmatch(self.terminal_reason) is None:
            raise ExecutionPolicyError("execution_policy_state_invalid")

    @classmethod
    def initial(cls, budget: ExecutionBudgetV1) -> ExecutionPolicyStateV1:
        return cls(budget_digest=budget.digest)

    def _projection(self) -> dict[str, object]:
        # The decision outbox is deliberately excluded from the state digest:
        # flushing queued decisions must not change the CAS identity of the
        # counters, so the worker can clear the outbox against the same
        # expected digest it read. Outbox integrity is protected separately by
        # ``decision_outbox_digest`` in ``to_json``.
        return {
            "version": EXECUTION_POLICY_STATE_VERSION,
            "budget_digest": self.budget_digest,
            "turns": self.turns,
            "total_tool_attempts": self.total_tool_attempts,
            "tool_category_attempts": dict(self.tool_category_attempts),
            "no_progress_observations": self.no_progress_observations,
            "batches": self.batches,
            "batch_items": self.batch_items,
            "batch_attempts": self.batch_attempts,
            "batch_runtime_seconds": self.batch_runtime_seconds,
            "retrieval_calls": self.retrieval_calls,
            "retrieval_results": self.retrieval_results,
            "retrieval_sources": self.retrieval_sources,
            "retrieval_bytes": self.retrieval_bytes,
            "sandbox_operations": self.sandbox_operations,
            "sandbox_runtime_seconds": self.sandbox_runtime_seconds,
            "recent_tool_commitments": list(self.recent_tool_commitments),
            "emitted_decisions": list(self.emitted_decisions),
            "processed_observation_ids": list(self.processed_observation_ids),
            "terminal_reason": self.terminal_reason,
        }

    def to_json(self) -> dict[str, object]:
        outbox = [item.to_json() for item in self.decision_outbox]
        return {
            **self._projection(),
            "decision_outbox": outbox,
            "decision_outbox_digest": _digest(_DECISION_OUTBOX_DOMAIN, outbox),
            "digest": self.digest,
        }

    @classmethod
    def from_json(cls, value: Mapping[str, object]) -> ExecutionPolicyStateV1:
        expected = {
            "version",
            "budget_digest",
            "turns",
            "total_tool_attempts",
            "tool_category_attempts",
            "no_progress_observations",
            "batches",
            "batch_items",
            "batch_attempts",
            "batch_runtime_seconds",
            "retrieval_calls",
            "retrieval_results",
            "retrieval_sources",
            "retrieval_bytes",
            "sandbox_operations",
            "sandbox_runtime_seconds",
            "recent_tool_commitments",
            "emitted_decisions",
            "decision_outbox",
            "decision_outbox_digest",
            "processed_observation_ids",
            "terminal_reason",
            "digest",
        }
        if set(value) != expected or value.get("version") != EXECUTION_POLICY_STATE_VERSION:
            raise ExecutionPolicyError("execution_policy_state_version_unsupported")
        categories = value.get("tool_category_attempts")
        commitments = value.get("recent_tool_commitments")
        decisions = value.get("emitted_decisions")
        processed = value.get("processed_observation_ids")
        outbox = value.get("decision_outbox")
        if (
            not isinstance(categories, Mapping)
            or not isinstance(commitments, list)
            or not isinstance(decisions, list)
            or not isinstance(processed, list)
            or not isinstance(outbox, list)
            or any(not isinstance(item, Mapping) for item in outbox)
        ):
            raise ExecutionPolicyError("execution_policy_state_invalid")
        if _require_digest(value.get("decision_outbox_digest")) != _digest(
            _DECISION_OUTBOX_DOMAIN,
            outbox,
        ):
            raise ExecutionPolicyError("execution_policy_digest_invalid")
        try:
            state = cls(
                budget_digest=value["budget_digest"],  # type: ignore[arg-type]
                turns=value["turns"],  # type: ignore[arg-type]
                total_tool_attempts=value["total_tool_attempts"],  # type: ignore[arg-type]
                tool_category_attempts=tuple(sorted(categories.items())),  # type: ignore[arg-type]
                no_progress_observations=value["no_progress_observations"],  # type: ignore[arg-type]
                batches=value["batches"],  # type: ignore[arg-type]
                batch_items=value["batch_items"],  # type: ignore[arg-type]
                batch_attempts=value["batch_attempts"],  # type: ignore[arg-type]
                batch_runtime_seconds=value["batch_runtime_seconds"],  # type: ignore[arg-type]
                retrieval_calls=value["retrieval_calls"],  # type: ignore[arg-type]
                retrieval_results=value["retrieval_results"],  # type: ignore[arg-type]
                retrieval_sources=value["retrieval_sources"],  # type: ignore[arg-type]
                retrieval_bytes=value["retrieval_bytes"],  # type: ignore[arg-type]
                sandbox_operations=value["sandbox_operations"],  # type: ignore[arg-type]
                sandbox_runtime_seconds=value["sandbox_runtime_seconds"],  # type: ignore[arg-type]
                recent_tool_commitments=tuple(commitments),  # type: ignore[arg-type]
                emitted_decisions=tuple(decisions),  # type: ignore[arg-type]
                decision_outbox=tuple(PolicyDecisionOutboxV1.from_json(item) for item in outbox),
                processed_observation_ids=tuple(processed),  # type: ignore[arg-type]
                terminal_reason=value["terminal_reason"],  # type: ignore[arg-type]
            )
        except (KeyError, TypeError) as exc:
            raise ExecutionPolicyError("execution_policy_state_invalid") from exc
        if _require_digest(value.get("digest")) != state.digest:
            raise ExecutionPolicyError("execution_policy_digest_invalid")
        return state

    @property
    def digest(self) -> str:
        return _digest(_STATE_DOMAIN, self._projection())


@dataclass(frozen=True, slots=True)
class PolicyEvaluationV1:
    decision: PolicyDecision
    reason_code: str | None
    current: int | None
    limit: int | None
    next_state: ExecutionPolicyStateV1
    next_state_digest: str
    durable_event_required: bool
    summary_key: str | None


class ExecutionPolicyEvaluator:
    """Pure deterministic policy evaluator over compact normalized state."""

    @staticmethod
    def _warning_limit(hard_limit: int) -> int:
        return max(1, hard_limit - 1)

    def evaluate(
        self,
        budget: ExecutionBudgetV1,
        state: ExecutionPolicyStateV1,
        observation: ExecutionPolicyObservationV1,
    ) -> PolicyEvaluationV1:
        if state.budget_digest != budget.digest:
            raise ExecutionPolicyError("policy_state_inconsistent")
        if state.terminal_reason is not None:
            return self._result(
                PolicyDecision.stop,
                state.terminal_reason,
                None,
                None,
                state,
                event=False,
            )
        if observation.observation_id is not None and observation.observation_id in state.processed_observation_ids:
            return self._result(
                PolicyDecision.allow,
                None,
                None,
                None,
                state,
                event=False,
            )

        changes: dict[str, object] = {}
        if observation.observation_id is not None:
            changes["processed_observation_ids"] = (
                *state.processed_observation_ids,
                observation.observation_id,
            )
        checks: list[tuple[str, int, int, int | None]] = []
        if observation.kind == "turn":
            current = state.turns + observation.count
            changes["turns"] = current
            checks.append(
                (
                    "turn_budget_exhausted",
                    current,
                    budget.max_agent_turns,
                    self._warning_limit(budget.max_agent_turns),
                )
            )
        elif observation.kind == "tool_attempt":
            current = state.total_tool_attempts + observation.count
            changes["total_tool_attempts"] = current
            checks.append(
                (
                    "tool_attempt_budget_exhausted",
                    current,
                    budget.max_total_tool_attempts,
                    self._warning_limit(budget.max_total_tool_attempts),
                )
            )
            categories = dict(state.tool_category_attempts)
            assert observation.tool_category is not None
            categories[observation.tool_category] = categories.get(observation.tool_category, 0) + observation.count
            changes["tool_category_attempts"] = tuple(sorted(categories.items()))
            category_limit = dict(budget.per_tool_category_attempts).get(observation.tool_category)
            if category_limit is not None:
                checks.append(
                    (
                        "tool_attempt_budget_exhausted",
                        categories[observation.tool_category],
                        category_limit,
                        self._warning_limit(category_limit),
                    )
                )
            if observation.tool_category == "sandbox":
                # Sandbox shell/exec tools are the enforceable sandbox
                # operation seam in this checkout, so each such attempt also
                # consumes the dedicated sandbox-operation budget.
                sandbox_current = state.sandbox_operations + observation.count
                changes["sandbox_operations"] = sandbox_current
                checks.append(
                    (
                        "sandbox_operation_budget_exhausted",
                        sandbox_current,
                        budget.max_sandbox_operations,
                        self._warning_limit(budget.max_sandbox_operations),
                    )
                )
            if observation.equivalence_commitment is not None:
                window = (*state.recent_tool_commitments, observation.equivalence_commitment)[-budget.repeated_tool_window :]
                changes["recent_tool_commitments"] = window
                repeated = window.count(observation.equivalence_commitment)
                checks.append(("repeated_tool_loop", repeated, budget.repeated_tool_stop, budget.repeated_tool_warn))
        elif observation.kind == "no_progress":
            current = state.no_progress_observations + observation.count
            changes["no_progress_observations"] = current
            checks.append(("no_progress_loop", current, budget.max_no_progress_observations, max(1, budget.max_no_progress_observations - 1)))
        elif observation.kind == "batch":
            changes.update(
                batches=state.batches + 1,
                batch_items=state.batch_items + observation.count,
                batch_attempts=state.batch_attempts + observation.attempt_count,
                batch_runtime_seconds=state.batch_runtime_seconds + observation.runtime_seconds,
            )
            checks.extend(
                (
                    (
                        "batch_count_budget_exhausted",
                        int(changes["batches"]),
                        budget.max_batches,
                        self._warning_limit(budget.max_batches),
                    ),
                    (
                        "batch_item_budget_exhausted",
                        int(changes["batch_items"]),
                        budget.max_batch_items,
                        self._warning_limit(budget.max_batch_items),
                    ),
                    (
                        "batch_attempt_budget_exhausted",
                        int(changes["batch_attempts"]),
                        budget.max_batch_attempts,
                        self._warning_limit(budget.max_batch_attempts),
                    ),
                    (
                        "batch_runtime_budget_exhausted",
                        int(changes["batch_runtime_seconds"]),
                        budget.max_batch_runtime_seconds,
                        self._warning_limit(budget.max_batch_runtime_seconds),
                    ),
                )
            )
        elif observation.kind == "retrieval":
            changes.update(
                retrieval_calls=state.retrieval_calls + observation.count,
                retrieval_results=state.retrieval_results + observation.result_count,
                retrieval_sources=state.retrieval_sources + observation.source_count,
                retrieval_bytes=state.retrieval_bytes + observation.bytes_used,
            )
            checks.extend(
                (
                    ("retrieval_budget_exhausted", int(changes["retrieval_calls"]), budget.max_retrieval_calls, self._warning_limit(budget.max_retrieval_calls)),
                    ("retrieval_budget_exhausted", int(changes["retrieval_results"]), budget.max_retrieval_results, self._warning_limit(budget.max_retrieval_results)),
                    ("retrieval_budget_exhausted", int(changes["retrieval_sources"]), budget.max_retrieval_sources, self._warning_limit(budget.max_retrieval_sources)),
                    ("retrieval_budget_exhausted", int(changes["retrieval_bytes"]), budget.max_retrieval_bytes, self._warning_limit(budget.max_retrieval_bytes)),
                )
            )
        elif observation.kind == "sandbox":
            changes.update(
                sandbox_operations=state.sandbox_operations + observation.count,
                sandbox_runtime_seconds=state.sandbox_runtime_seconds + observation.runtime_seconds,
            )
            checks.extend(
                (
                    ("sandbox_operation_budget_exhausted", int(changes["sandbox_operations"]), budget.max_sandbox_operations, self._warning_limit(budget.max_sandbox_operations)),
                    ("sandbox_runtime_budget_exhausted", int(changes["sandbox_runtime_seconds"]), budget.max_sandbox_runtime_seconds, self._warning_limit(budget.max_sandbox_runtime_seconds)),
                )
            )

        next_state = replace(state, **changes)
        for reason, current, hard_limit, warning_limit in checks:
            if current >= hard_limit:
                event_key = f"stop:{reason}"
                event = event_key not in next_state.emitted_decisions
                emitted = tuple(sorted({*next_state.emitted_decisions, event_key}))
                terminal = replace(next_state, emitted_decisions=emitted, terminal_reason=reason)
                if event:
                    terminal = replace(
                        terminal,
                        decision_outbox=(
                            *terminal.decision_outbox,
                            PolicyDecisionOutboxV1(
                                decision=PolicyDecision.stop,
                                reason_code=reason,
                                current=current,
                                limit=hard_limit,
                                state_digest=terminal.digest,
                                summary_key=f"execution_policy.{reason}",
                            ),
                        ),
                    )
                return self._result(PolicyDecision.stop, reason, current, hard_limit, terminal, event=event)
        for reason, current, _hard_limit, warning_limit in checks:
            if warning_limit is not None and current >= warning_limit:
                event_key = f"warn:{reason}:{warning_limit}"
                event = event_key not in next_state.emitted_decisions
                if event:
                    next_state = replace(next_state, emitted_decisions=tuple(sorted({*next_state.emitted_decisions, event_key})))
                    next_state = replace(
                        next_state,
                        decision_outbox=(
                            *next_state.decision_outbox,
                            PolicyDecisionOutboxV1(
                                decision=PolicyDecision.warn,
                                reason_code=reason,
                                current=current,
                                limit=warning_limit,
                                state_digest=next_state.digest,
                                summary_key=f"execution_policy.{reason}",
                            ),
                        ),
                    )
                return self._result(PolicyDecision.warn, reason, current, warning_limit, next_state, event=event)
        return self._result(PolicyDecision.allow, None, None, None, next_state, event=False)

    @staticmethod
    def _result(
        decision: PolicyDecision,
        reason: str | None,
        current: int | None,
        limit: int | None,
        state: ExecutionPolicyStateV1,
        *,
        event: bool,
    ) -> PolicyEvaluationV1:
        return PolicyEvaluationV1(
            decision=decision,
            reason_code=reason,
            current=current,
            limit=limit,
            next_state=state,
            next_state_digest=state.digest,
            durable_event_required=event,
            summary_key=(None if reason is None else f"execution_policy.{reason}"),
        )


@dataclass(frozen=True, slots=True)
class ToolEquivalenceCommitmentV1:
    version: int
    tool_name: str
    normalizer_version: int
    normalizer_manifest_digest: str
    key_id: str
    digest: str

    def __post_init__(self) -> None:
        if self.version != TOOL_EQUIVALENCE_COMMITMENT_VERSION or self.normalizer_version != TOOL_EQUIVALENCE_NORMALIZER_VERSION:
            raise ExecutionPolicyError("policy_equivalence_normalizer_unavailable")
        if _SAFE_ID_RE.fullmatch(self.tool_name) is None or _KEY_ID_RE.fullmatch(self.key_id) is None:
            raise ExecutionPolicyError("execution_policy_commitment_invalid")
        _require_digest(self.normalizer_manifest_digest)
        _require_digest(self.digest)


@dataclass(frozen=True, slots=True)
class ToolEquivalenceKeyringConfirmationV1:
    version: int
    digest: str


def _decode_key(value: object) -> bytes:
    if not isinstance(value, str) or not value:
        raise ExecutionPolicyError("execution_policy_keyring_invalid")
    try:
        encoded = value.encode("ascii")
        decoded = base64.b64decode(encoded + b"=" * (-len(encoded) % 4), altchars=b"-_", validate=True)
    except (UnicodeEncodeError, ValueError) as exc:
        raise ExecutionPolicyError("execution_policy_keyring_invalid") from exc
    if not _MIN_KEY_BYTES <= len(decoded) <= _MAX_KEY_BYTES or base64.urlsafe_b64encode(decoded).decode("ascii").rstrip("=") != value:
        raise ExecutionPolicyError("execution_policy_keyring_invalid")
    return decoded


class ToolEquivalenceKeyring:
    """Startup-frozen bounded keyring; key bytes never enter public evidence."""

    __slots__ = ("_active_key_id", "_keys")

    def __init__(self, *, active_key_id: str, keys: Mapping[str, bytes]) -> None:
        if _KEY_ID_RE.fullmatch(active_key_id) is None or not 1 <= len(keys) <= _MAX_KEYS:
            raise ExecutionPolicyError("execution_policy_keyring_invalid")
        copied: dict[str, bytes] = {}
        for key_id, secret in keys.items():
            if _KEY_ID_RE.fullmatch(key_id) is None or not isinstance(secret, bytes) or not _MIN_KEY_BYTES <= len(secret) <= _MAX_KEY_BYTES:
                raise ExecutionPolicyError("execution_policy_keyring_invalid")
            copied[key_id] = bytes(secret)
        if active_key_id not in copied:
            raise ExecutionPolicyError("execution_policy_keyring_invalid")
        self._active_key_id = active_key_id
        self._keys = MappingProxyType(copied)

    @classmethod
    def from_environment(
        cls,
        *,
        required: bool,
        environ: Mapping[str, str] | None = None,
    ) -> ToolEquivalenceKeyring | None:
        values = os.environ if environ is None else environ
        raw = values.get(EXECUTION_POLICY_HMAC_KEYS_ENV)
        active = values.get(EXECUTION_POLICY_HMAC_ACTIVE_KEY_ID_ENV)
        if raw is None and active is None:
            if required:
                raise ExecutionPolicyError("policy_equivalence_key_unavailable")
            return None
        if raw is None or active is None or len(raw.encode("utf-8")) > _MAX_ENV_BYTES:
            raise ExecutionPolicyError("execution_policy_keyring_invalid")

        def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
            result: dict[str, object] = {}
            for key, value in pairs:
                if key in result:
                    raise ExecutionPolicyError("execution_policy_keyring_invalid")
                result[key] = value
            return result

        try:
            parsed = json.loads(raw, object_pairs_hook=_unique_object)
        except (ExecutionPolicyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ExecutionPolicyError("execution_policy_keyring_invalid") from exc
        if not isinstance(parsed, dict) or any(not isinstance(key, str) for key in parsed):
            raise ExecutionPolicyError("execution_policy_keyring_invalid")
        return cls(active_key_id=active, keys={key: _decode_key(value) for key, value in parsed.items()})

    @classmethod
    def ephemeral(cls) -> ToolEquivalenceKeyring:
        """Return a process-only local keyring that cannot qualify durability."""

        return cls(
            active_key_id="local-ephemeral-v1",
            keys={"local-ephemeral-v1": secrets.token_bytes(32)},
        )

    @property
    def active_key_id(self) -> str:
        return self._active_key_id

    def sign(self, payload: object, *, key_id: str) -> str:
        secret = self._keys.get(key_id)
        if secret is None:
            raise ExecutionPolicyError("policy_equivalence_key_unavailable")
        return hmac.new(secret, _COMMITMENT_DOMAIN + _canonical_bytes(payload), hashlib.sha256).hexdigest()

    def require_key(self, key_id: str) -> None:
        if key_id not in self._keys:
            raise ExecutionPolicyError("policy_equivalence_key_unavailable")

    def confirmation(self) -> ToolEquivalenceKeyringConfirmationV1:
        confirmations = [
            {
                "key_id": key_id,
                "confirmation": hmac.new(secret, _KEY_CONFIRMATION_DOMAIN + key_id.encode("ascii"), hashlib.sha256).hexdigest(),
            }
            for key_id, secret in sorted(self._keys.items())
        ]
        return ToolEquivalenceKeyringConfirmationV1(
            version=1,
            digest="sha256:" + hashlib.sha256(_KEYRING_CONFIRMATION_DOMAIN + _canonical_bytes({"active_key_id": self._active_key_id, "keys": confirmations})).hexdigest(),
        )


@lru_cache(maxsize=1)
def local_ephemeral_keyring() -> ToolEquivalenceKeyring:
    """Process-stable local keyring; explicitly not restart qualified."""

    return ToolEquivalenceKeyring.ephemeral()


_NORMALIZER_MANIFEST = {
    "version": TOOL_EQUIVALENCE_NORMALIZER_VERSION,
    "read_file": "path-and-200-line-buckets-v1",
    "write_file": "safe-full-arguments-v1",
    "str_replace": "safe-full-arguments-v1",
    "web_search": "typed-search-arguments-v1",
    "image_search": "typed-search-arguments-v1",
    "memory_search": "typed-search-arguments-v1",
    "web_fetch": "typed-url-arguments-v1",
}


def normalizer_manifest_digest() -> str:
    return _digest(_NORMALIZER_MANIFEST_DOMAIN, _NORMALIZER_MANIFEST)


def _secret_field_name(value: str) -> bool:
    tokens = tuple(token for token in re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", value).lower().replace("api_key", "apikey").split("_") if token)
    compact = "".join(re.findall(r"[a-z0-9]+", "_".join(tokens)))
    return any(token in _SECRET_FIELD_TOKENS for token in tokens) or any(marker in compact for marker in _SECRET_FIELD_TOKENS)


def _contains_secret_field(value: object, *, depth: int = 0) -> bool:
    if depth > 16:
        return True
    if isinstance(value, Mapping):
        return any(not isinstance(key, str) or _secret_field_name(key) or _contains_secret_field(child, depth=depth + 1) for key, child in value.items())
    if isinstance(value, Sequence) and not isinstance(value, bytes | bytearray | str):
        return any(_contains_secret_field(child, depth=depth + 1) for child in value)
    return False


def _read_file_projection(arguments: Mapping[str, object]) -> dict[str, object]:
    path = arguments.get("path", "")
    if not isinstance(path, str) or not path or len(path.encode("utf-8")) > 4096:
        raise ExecutionPolicyError("execution_policy_value_invalid")
    try:
        start = max(1, int(arguments.get("start_line", 1)))
        end = max(1, int(arguments.get("end_line", start)))
    except (TypeError, ValueError) as exc:
        raise ExecutionPolicyError("execution_policy_value_invalid") from exc
    start, end = sorted((start, end))
    return {
        "path": unicodedata.normalize("NFC", path),
        "start_bucket": (start - 1) // 200,
        "end_bucket": (end - 1) // 200,
    }


def build_tool_equivalence_commitment(
    *,
    tenant_digest: str,
    run_ref: str,
    tool_name: str,
    arguments: Mapping[str, object],
    keyring: ToolEquivalenceKeyring,
    key_id: str,
) -> ToolEquivalenceCommitmentV1 | None:
    """Build protected equality state, excluding secret or unsafe arguments."""

    _require_digest(tenant_digest)
    if not isinstance(run_ref, str) or not run_ref or len(run_ref.encode("utf-8")) > 256:
        raise ExecutionPolicyError("execution_policy_commitment_invalid")
    if not isinstance(tool_name, str) or _SAFE_ID_RE.fullmatch(tool_name) is None:
        raise ExecutionPolicyError("execution_policy_commitment_invalid")
    if not isinstance(arguments, Mapping) or _contains_secret_field(arguments):
        return None
    try:
        projection: object
        if tool_name == "read_file":
            projection = _read_file_projection(arguments)
            normalizer = _NORMALIZER_MANIFEST["read_file"]
        else:
            allowed_fields = _NORMALIZER_ALLOWED_FIELDS.get(tool_name)
            if allowed_fields is None or not set(arguments).issubset(allowed_fields):
                # Unknown tools have no typed classifier at this boundary.
                # Excluding them is safer than treating arbitrary arguments as
                # approved merely because their field names look harmless.
                return None
            projection = _canonical(arguments)
            normalizer = _NORMALIZER_MANIFEST[tool_name]
        payload = {
            "version": TOOL_EQUIVALENCE_COMMITMENT_VERSION,
            "tenant_digest": tenant_digest,
            "run_ref": run_ref,
            "tool_name": tool_name,
            "normalizer_version": TOOL_EQUIVALENCE_NORMALIZER_VERSION,
            "normalizer": normalizer,
            "values": projection,
        }
        digest = keyring.sign(payload, key_id=key_id)
    except ExecutionPolicyError as exc:
        if exc.code == "policy_equivalence_key_unavailable":
            raise
        return None
    return ToolEquivalenceCommitmentV1(
        version=TOOL_EQUIVALENCE_COMMITMENT_VERSION,
        tool_name=tool_name,
        normalizer_version=TOOL_EQUIVALENCE_NORMALIZER_VERSION,
        normalizer_manifest_digest=normalizer_manifest_digest(),
        key_id=key_id,
        digest=digest,
    )


def resolve_execution_budget(
    config: object,
    *,
    keyring: ToolEquivalenceKeyring,
    max_recursion_limit: int,
    non_interactive: bool,
    requested_limits: Mapping[str, object] | None = None,
) -> ExecutionBudgetV1:
    """Resolve server policy, scheduler narrowing, and optional caller narrowing."""

    values = {name: getattr(config, name) for name in _BUDGET_LIMIT_FIELDS}
    values["max_agent_turns"] = min(
        int(values["max_agent_turns"]),
        max_recursion_limit,
    )
    if non_interactive:
        values["max_agent_turns"] = min(
            int(values["max_agent_turns"]),
            int(getattr(config, "scheduler_max_agent_turns")),
        )
        values["max_total_tool_attempts"] = min(
            int(values["max_total_tool_attempts"]),
            int(getattr(config, "scheduler_max_total_tool_attempts")),
        )
    budget = ExecutionBudgetV1.build(
        profile=(str(getattr(config, "scheduler_profile")) if non_interactive else str(getattr(config, "profile"))),
        equivalence_key_id=keyring.active_key_id,
        per_tool_category_attempts=getattr(
            config,
            "per_tool_category_attempts",
            {},
        ),
        **values,
    )
    return budget if requested_limits is None else budget.narrow(requested_limits)


__all__ = [
    "EXECUTION_BUDGET_VERSION",
    "EXECUTION_POLICY_HMAC_ACTIVE_KEY_ID_ENV",
    "EXECUTION_POLICY_HMAC_KEYS_ENV",
    "ExecutionBudgetV1",
    "ExecutionPolicyError",
    "ExecutionPolicyEvaluator",
    "ExecutionPolicyObservationV1",
    "ExecutionPolicyStateV1",
    "PolicyDecision",
    "PolicyDecisionOutboxV1",
    "PolicyEvaluationV1",
    "ToolEquivalenceCommitmentV1",
    "ToolEquivalenceKeyring",
    "ToolEquivalenceKeyringConfirmationV1",
    "build_tool_equivalence_commitment",
    "normalizer_manifest_digest",
    "local_ephemeral_keyring",
    "resolve_execution_budget",
]
