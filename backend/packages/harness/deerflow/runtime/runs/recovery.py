"""Typed recovery decisions for server-owned execution takeover.

This module deliberately separates immutable admission policy from the
operator's reversible claim switch and from Gateway-specific reconstruction.
The harness owns the finite outcomes; the application owns checkpoint and
tool reconciliation because those require app-scoped dependencies.
"""

from __future__ import annotations

import json
import re
import urllib.parse
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Literal

_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
_RECEIPT_ID_RE = re.compile(r"^tr_[0-9a-f]{64}$")
_TOOL_NAME_RE = re.compile(r"^[A-Za-z0-9_.:/-]+$")
_MAX_RECOVERY_PAYLOAD_BYTES = 256 * 1024
_SUPPORTED_STREAM_MODES = frozenset(
    {
        "values",
        "messages-tuple",
        "updates",
        "debug",
        "tasks",
        "checkpoints",
        "custom",
    }
)
_SECRET_CONFIG_KEYS = frozenset(
    {
        "secret",
        "secrets",
        "token",
        "access_token",
        "refresh_token",
        "auth_token",
        "api_key",
        "password",
        "passwd",
        "cookie",
        "cookies",
        "credential",
        "credentials",
        "authorization_header",
        "authorization",
        "bearer",
        "headers",
        "client_secret",
        "private_key",
    }
)
_SECRET_CONFIG_TOKENS = frozenset(
    {
        "secret",
        "secrets",
        "token",
        "password",
        "passwd",
        "credential",
        "credentials",
        "bearer",
        "authorization",
        "cookie",
        "cookies",
    }
)
_SECRET_CONFIG_PAIRS = frozenset(
    {
        ("api", "key"),
        ("client", "secret"),
        ("private", "key"),
        ("access", "key"),
        ("auth", "header"),
    }
)
_URL_SCHEME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*://")
_SECRET_URL_QUERY_KEYS = frozenset(
    {
        "code",
        "key",
        "pw",
        "sas",
        "sig",
        "signature",
    }
)

# Exact-two recovery stores only the semantic inputs needed to reconstruct a
# worker. New RunnableConfig keys are unsupported until they are deliberately
# classified here; otherwise a future integration could silently turn a
# runtime-only credential into durable replay material.
_RECOVERY_TOP_LEVEL_PERSISTED_KEYS = frozenset({"recursion_limit", "configurable", "context"})
_RECOVERY_TOP_LEVEL_REDERIVED_KEYS = frozenset({"metadata", "run_name", "tags"})
_RECOVERY_CONFIGURABLE_PERSISTED_KEYS = frozenset({"thread_id", "checkpoint_ns", "checkpoint_id", "checkpoint_map"})
_RECOVERY_CONTEXT_PERSISTED_KEYS = frozenset({"mode", "disable_clarification"})
_RECOVERY_PINNED_AGENT_KEYS = frozenset(
    {
        "model_name",
        "thinking_enabled",
        "reasoning_effort",
        "is_plan_mode",
        "subagent_enabled",
        "max_concurrent_subagents",
        "max_total_subagents",
        "agent_name",
        "is_bootstrap",
        "non_interactive",
        "channel_name",
    }
)
_RECOVERY_REBUILT_IDENTITY_KEYS = frozenset(
    {
        "thread_id",
        "user_id",
        "user_role",
        "oauth_provider",
        "oauth_id",
        "is_internal",
        "authz_attributes",
        "channel_user_id",
        "langgraph_auth_user",
        "langgraph_auth_user_id",
        "__deerflow_invocation_identity",
        "__deerflow_invocation_origin",
        "__deerflow_trusted_run_context",
        "__deerflow_tenant_reference",
        "tenant",
        "tenant_id",
        "tenantId",
        "tenant_ref",
        "tenant_digest",
        "x_tenant_id",
        "x-tenant-id",
        "X-Tenant-ID",
    }
)
_RECOVERY_CONFIGURABLE_CLASSIFIED_KEYS = _RECOVERY_CONFIGURABLE_PERSISTED_KEYS | _RECOVERY_CONTEXT_PERSISTED_KEYS | _RECOVERY_PINNED_AGENT_KEYS
_RECOVERY_CONTEXT_CLASSIFIED_KEYS = _RECOVERY_CONTEXT_PERSISTED_KEYS | _RECOVERY_PINNED_AGENT_KEYS | _RECOVERY_REBUILT_IDENTITY_KEYS


def _detached_json(value: object, *, error_code: str) -> object:
    try:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return json.loads(encoded)
    except (TypeError, ValueError) as exc:
        raise ValueError(error_code) from exc


def _secret_config_key(key: str) -> bool:
    camel_split = re.sub(
        r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])",
        "_",
        key.strip(),
    )
    normalized = re.sub(r"[^A-Za-z0-9]+", "_", camel_split).lower()
    tokens = tuple(part for part in normalized.split("_") if part)
    return normalized.startswith("__") or normalized in _SECRET_CONFIG_KEYS or any(token in _SECRET_CONFIG_TOKENS for token in tokens) or any(pair in _SECRET_CONFIG_PAIRS for pair in zip(tokens, tokens[1:], strict=False))


def _url_contains_secret(value: str) -> bool:
    """Reject credentials embedded in otherwise innocuous URL fields."""

    if _URL_SCHEME_RE.match(value.strip()) is None:
        return False
    try:
        parsed = urllib.parse.urlsplit(value.strip())
    except ValueError:
        # A URL-shaped value that cannot be parsed cannot be proven safe to
        # persist for exact replay.
        return True
    if parsed.username is not None or parsed.password is not None:
        return True
    for encoded_parameters in (parsed.query, parsed.fragment):
        try:
            parameters = urllib.parse.parse_qsl(
                encoded_parameters,
                keep_blank_values=True,
                max_num_fields=256,
            )
        except ValueError:
            return True
        for key, _value in parameters:
            camel_split = re.sub(
                r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])",
                "_",
                key.strip(),
            )
            normalized = (
                re.sub(
                    r"[^A-Za-z0-9]+",
                    "_",
                    camel_split,
                )
                .strip("_")
                .lower()
            )
            if _secret_config_key(key) or normalized in _SECRET_URL_QUERY_KEYS or normalized.endswith("_signature") or normalized.endswith("_credential"):
                return True
    return False


def _config_contains_secret(value: object) -> bool:
    if isinstance(value, Mapping):
        for key, child in value.items():
            if not isinstance(key, str):
                return True
            if _secret_config_key(key):
                return True
            if _config_contains_secret(child):
                return True
        return False
    if isinstance(value, list):
        return any(_config_contains_secret(child) for child in value)
    if isinstance(value, str):
        return _url_contains_secret(value)
    return False


def _validate_projected_recovery_config(config: Mapping[str, Any]) -> None:
    if set(config) - _RECOVERY_TOP_LEVEL_PERSISTED_KEYS:
        raise ValueError("recovery_payload_config_keys_invalid")
    if set(config) != {"recursion_limit", "configurable"} and set(config) != {"recursion_limit", "configurable", "context"}:
        raise ValueError("recovery_payload_config_keys_invalid")
    recursion_limit = config.get("recursion_limit")
    if type(recursion_limit) is not int or recursion_limit <= 0 or recursion_limit > 1_000_000:
        raise ValueError("recovery_payload_recursion_limit_invalid")
    configurable = config.get("configurable")
    if not isinstance(configurable, Mapping):
        raise ValueError("recovery_payload_configurable_invalid")
    if set(configurable) - _RECOVERY_CONFIGURABLE_PERSISTED_KEYS:
        raise ValueError("recovery_payload_config_configurable_keys_invalid")
    thread_id = configurable.get("thread_id")
    if not isinstance(thread_id, str) or not thread_id:
        raise ValueError("recovery_payload_thread_id_invalid")
    checkpoint_ns = configurable.get("checkpoint_ns", "")
    if not isinstance(checkpoint_ns, str):
        raise ValueError("recovery_payload_checkpoint_ns_invalid")
    checkpoint_id = configurable.get("checkpoint_id")
    if checkpoint_id is not None and (not isinstance(checkpoint_id, str) or not checkpoint_id):
        raise ValueError("recovery_payload_checkpoint_id_invalid")
    checkpoint_map = configurable.get("checkpoint_map")
    if checkpoint_map is not None and not isinstance(
        checkpoint_map,
        Mapping,
    ):
        raise ValueError("recovery_payload_checkpoint_map_invalid")
    context = config.get("context")
    if context is None:
        return
    if not isinstance(context, Mapping):
        raise ValueError("recovery_payload_context_invalid")
    if set(context) - _RECOVERY_CONTEXT_PERSISTED_KEYS:
        raise ValueError("recovery_payload_config_context_keys_invalid")
    mode = context.get("mode")
    if mode is not None and (not isinstance(mode, str) or not mode):
        raise ValueError("recovery_payload_mode_invalid")
    disable_clarification = context.get("disable_clarification")
    if disable_clarification is not None and type(disable_clarification) is not bool:
        raise ValueError("recovery_payload_disable_clarification_invalid")


def project_execution_recovery_config(
    value: Mapping[str, Any],
) -> dict[str, Any]:
    """Return the classified, secret-free RunnableConfig replay projection."""

    detached = _detached_json(
        value,
        error_code="recovery_payload_config_invalid",
    )
    if not isinstance(detached, dict):
        raise ValueError("recovery_payload_config_invalid")
    if _config_contains_secret(detached):
        raise ValueError("recovery_payload_secret_config")
    unknown_top_level = set(detached) - (_RECOVERY_TOP_LEVEL_PERSISTED_KEYS | _RECOVERY_TOP_LEVEL_REDERIVED_KEYS)
    if unknown_top_level:
        raise ValueError("recovery_payload_config_keys_invalid")
    configurable = detached.get("configurable")
    if not isinstance(configurable, Mapping):
        raise ValueError("recovery_payload_configurable_invalid")
    unknown_configurable = set(configurable) - (_RECOVERY_CONFIGURABLE_CLASSIFIED_KEYS)
    if unknown_configurable:
        raise ValueError("recovery_payload_config_configurable_keys_invalid")
    context = detached.get("context")
    if context is not None and not isinstance(context, Mapping):
        raise ValueError("recovery_payload_context_invalid")
    runtime_context = context if isinstance(context, Mapping) else {}
    unknown_context = set(runtime_context) - _RECOVERY_CONTEXT_CLASSIFIED_KEYS
    if unknown_context:
        raise ValueError("recovery_payload_config_context_keys_invalid")

    projected_configurable = {
        key: configurable[key]
        for key in (
            "thread_id",
            "checkpoint_ns",
            "checkpoint_id",
            "checkpoint_map",
        )
        if key in configurable and configurable[key] is not None
    }
    projected_configurable.setdefault("checkpoint_ns", "")
    projected_context: dict[str, Any] = {}
    for key in ("mode", "disable_clarification"):
        if key in runtime_context and runtime_context[key] is not None:
            projected_context[key] = runtime_context[key]
        elif key in configurable and configurable[key] is not None:
            projected_context[key] = configurable[key]
    projected: dict[str, Any] = {
        "recursion_limit": detached.get("recursion_limit"),
        "configurable": projected_configurable,
    }
    if projected_context:
        projected["context"] = projected_context
    _validate_projected_recovery_config(projected)
    return projected


def _normalize_interrupt(
    value: object,
    *,
    field_name: str,
) -> tuple[str, ...] | Literal["*"] | None:
    if value is None or value == "*":
        return value
    if not isinstance(value, (list, tuple)) or len(value) > 256:
        raise ValueError(f"recovery_payload_{field_name}_invalid")
    normalized = tuple(value)
    if any(not isinstance(item, str) or not item or len(item.encode("utf-8")) > 256 for item in normalized):
        raise ValueError(f"recovery_payload_{field_name}_invalid")
    return normalized


@dataclass(frozen=True, slots=True)
class ExecutionRecoveryPayloadV1:
    """Bounded, secret-free execution inputs sealed by Gateway admission."""

    input_kind: Literal["graph", "command_resume"]
    input_value: Any
    config: Mapping[str, Any]
    stream_modes: tuple[str, ...]
    stream_subgraphs: bool
    interrupt_before: tuple[str, ...] | Literal["*"] | None = None
    interrupt_after: tuple[str, ...] | Literal["*"] | None = None

    def __post_init__(self) -> None:
        if self.input_kind not in {"graph", "command_resume"}:
            raise ValueError("recovery_payload_input_kind_invalid")
        detached_input = _detached_json(
            self.input_value,
            error_code="recovery_payload_input_invalid",
        )
        if self.input_kind == "graph" and not isinstance(
            detached_input,
            dict,
        ):
            raise ValueError("recovery_payload_input_invalid")
        detached_config = _detached_json(
            self.config,
            error_code="recovery_payload_config_invalid",
        )
        if not isinstance(detached_config, dict):
            raise ValueError("recovery_payload_config_invalid")
        if _config_contains_secret(detached_config):
            raise ValueError("recovery_payload_secret_config")
        _validate_projected_recovery_config(detached_config)
        modes = tuple(self.stream_modes)
        if not modes or len(modes) > len(_SUPPORTED_STREAM_MODES) or len(set(modes)) != len(modes) or any(mode not in _SUPPORTED_STREAM_MODES for mode in modes):
            raise ValueError("recovery_payload_stream_modes_invalid")
        if type(self.stream_subgraphs) is not bool:
            raise ValueError("recovery_payload_stream_subgraphs_invalid")
        before = _normalize_interrupt(
            self.interrupt_before,
            field_name="interrupt_before",
        )
        after = _normalize_interrupt(
            self.interrupt_after,
            field_name="interrupt_after",
        )
        object.__setattr__(self, "input_value", detached_input)
        object.__setattr__(self, "config", detached_config)
        object.__setattr__(self, "stream_modes", modes)
        object.__setattr__(self, "interrupt_before", before)
        object.__setattr__(self, "interrupt_after", after)
        persisted = self._unbounded_persisted()
        if (
            len(
                json.dumps(
                    persisted,
                    ensure_ascii=False,
                    allow_nan=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
            )
            > _MAX_RECOVERY_PAYLOAD_BYTES
        ):
            raise ValueError("recovery_payload_too_large")

    def _unbounded_persisted(self) -> dict[str, object]:
        return {
            "version": 1,
            "input_kind": self.input_kind,
            "input_value": self.input_value,
            "config": self.config,
            "stream_modes": list(self.stream_modes),
            "stream_subgraphs": self.stream_subgraphs,
            "interrupt_before": (list(self.interrupt_before) if isinstance(self.interrupt_before, tuple) else self.interrupt_before),
            "interrupt_after": (list(self.interrupt_after) if isinstance(self.interrupt_after, tuple) else self.interrupt_after),
        }

    def to_persisted(self) -> dict[str, object]:
        return json.loads(
            json.dumps(
                self._unbounded_persisted(),
                ensure_ascii=False,
                allow_nan=False,
                sort_keys=True,
                separators=(",", ":"),
            )
        )

    @classmethod
    def from_persisted(
        cls,
        value: Mapping[str, object],
    ) -> ExecutionRecoveryPayloadV1:
        expected = {
            "version",
            "input_kind",
            "input_value",
            "config",
            "stream_modes",
            "stream_subgraphs",
            "interrupt_before",
            "interrupt_after",
        }
        if not isinstance(value, Mapping) or set(value) != expected:
            raise ValueError("recovery_payload_fields_invalid")
        if value.get("version") != 1:
            raise ValueError("recovery_payload_version_invalid")
        modes = value.get("stream_modes")
        if not isinstance(modes, list):
            raise ValueError("recovery_payload_stream_modes_invalid")
        config = value.get("config")
        if not isinstance(config, Mapping):
            raise ValueError("recovery_payload_config_invalid")
        return cls(
            input_kind=value.get("input_kind"),  # type: ignore[arg-type]
            input_value=value.get("input_value"),
            config=config,
            stream_modes=tuple(modes),
            stream_subgraphs=value.get("stream_subgraphs"),  # type: ignore[arg-type]
            interrupt_before=value.get("interrupt_before"),  # type: ignore[arg-type]
            interrupt_after=value.get("interrupt_after"),  # type: ignore[arg-type]
        )


class ExecutionRecoveryDisposition(StrEnum):
    """Finite result returned by the application recovery coordinator."""

    restart_pre_graph = "restart_pre_graph"
    resume_checkpoint = "resume_checkpoint"
    # Compatibility spelling for early coordinator tests. Production
    # coordinators return one of the two explicit safe-point dispositions.
    resumed = "resumed"
    resume_reconciled_tool = "resume_reconciled_tool"
    terminalize_tool_attempt_indeterminate = "terminalize_tool_attempt_indeterminate"
    terminalize_checkpoint_unavailable = "terminalize_checkpoint_unavailable"


@dataclass(frozen=True, slots=True)
class ReconciledToolRecoveryProofV1:
    """Accepted anchors proving that one open receipt may be reattached.

    The application coordinator issues this proof only after matching the
    durable started receipt to an immutable accepted tool descriptor. It is
    intentionally descriptive rather than a mutable registration token: the
    RunManager still owns the lease CAS and the worker/event stores retain
    their independent epoch fences.
    """

    receipt_id: str
    tool_name: str
    recovery_kind: Literal["receipt_idempotent_reconcile_v1"]
    assembly_evidence_digest: str
    dispatch_generation_digest: str
    takeover_owner_worker_id: str
    takeover_state_version: int

    def __post_init__(self) -> None:
        if _RECEIPT_ID_RE.fullmatch(self.receipt_id) is None:
            raise ValueError("recovery_receipt_id_invalid")
        if _TOOL_NAME_RE.fullmatch(self.tool_name) is None:
            raise ValueError("recovery_tool_name_invalid")
        if self.recovery_kind != "receipt_idempotent_reconcile_v1":
            raise ValueError("recovery_kind_invalid")
        if _DIGEST_RE.fullmatch(self.assembly_evidence_digest) is None:
            raise ValueError("recovery_assembly_digest_invalid")
        if _DIGEST_RE.fullmatch(self.dispatch_generation_digest) is None:
            raise ValueError("recovery_dispatch_digest_invalid")
        if (
            not isinstance(self.takeover_owner_worker_id, str)
            or not self.takeover_owner_worker_id
            or len(self.takeover_owner_worker_id.encode("utf-8")) > 128
            or any(ord(character) < 32 or ord(character) == 127 for character in self.takeover_owner_worker_id)
        ):
            raise ValueError("recovery_takeover_owner_invalid")
        if type(self.takeover_state_version) is not int or self.takeover_state_version <= 0:
            raise ValueError("recovery_takeover_state_version_invalid")


@dataclass(frozen=True, slots=True)
class ExecutionRecoveryDecision:
    """One finite coordinator decision, with proof for tool reattachment."""

    disposition: ExecutionRecoveryDisposition
    reconciled_tool: ReconciledToolRecoveryProofV1 | None = None

    def __post_init__(self) -> None:
        disposition = ExecutionRecoveryDisposition(self.disposition)
        object.__setattr__(self, "disposition", disposition)
        needs_proof = disposition is ExecutionRecoveryDisposition.resume_reconciled_tool
        if needs_proof != (self.reconciled_tool is not None):
            raise ValueError("recovery_reconciliation_proof_mismatch")


RECOVERY_TOOL_ATTEMPT_INDETERMINATE_STOP_REASON = "recovery_tool_attempt_indeterminate"
RECOVERY_CHECKPOINT_UNAVAILABLE_STOP_REASON = "recovery_checkpoint_unavailable"


__all__ = [
    "ExecutionRecoveryDecision",
    "ExecutionRecoveryDisposition",
    "ExecutionRecoveryPayloadV1",
    "ReconciledToolRecoveryProofV1",
    "RECOVERY_CHECKPOINT_UNAVAILABLE_STOP_REASON",
    "RECOVERY_TOOL_ATTEMPT_INDETERMINATE_STOP_REASON",
    "project_execution_recovery_config",
]
