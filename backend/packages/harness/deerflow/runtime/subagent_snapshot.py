"""Resolve live subagent definitions into immutable accepted-run material.

This module owns the canonical representation used at durable admission.  Live
registry precedence remains in :mod:`deerflow.subagents.registry`; every other
accepted-run caller consumes the catalog defined here and never re-resolves a
managed definition.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, fields
from types import MappingProxyType
from typing import Literal, Self

from deerflow.runtime.accepted_invocation import canonical_digest

type JsonScalar = str | int | float | bool | None
type SubagentSourceKind = Literal["builtin", "config", "managed"]

SUBAGENT_CATALOG_VERSION = 1
MAX_SUBAGENT_CATALOG_ENTRIES = 64
MAX_SUBAGENT_IDENTIFIER_BYTES = 128
MAX_SUBAGENT_DESCRIPTION_BYTES = 2 * 1024
MAX_SUBAGENT_PROMPT_BYTES = 256 * 1024
MAX_SUBAGENT_CATALOG_BYTES = 256 * 1024
MAX_SUBAGENT_SKILL_SCOPES = MAX_SUBAGENT_CATALOG_ENTRIES + 1
MAX_SKILLS_PER_AGENT_SCOPE = 64
_MAX_JSON_DEPTH = 12
_MAX_JSON_ENTRIES = 512
_HEX = frozenset("0123456789abcdef")
_SOURCE_KINDS = frozenset({"builtin", "config", "managed"})
_OPTIONAL_DEFINITION_FIELDS = frozenset(
    {
        "inherits_tools",
        "disallowed_tool_names",
        "policy_settings",
        "tool_contract_digests",
    }
)


class SubagentCatalogError(RuntimeError):
    """Fail-closed catalog error whose code is safe to expose."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def _invalid() -> None:
    raise SubagentCatalogError("subagent_catalog_invalid")


def _is_digest(value: object) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(character in _HEX for character in value)


def _bounded_text(
    value: object,
    *,
    max_bytes: int,
    allow_empty: bool = False,
) -> str:
    if not isinstance(value, str) or (not allow_empty and not value):
        _invalid()
    try:
        encoded = value.encode("utf-8")
    except UnicodeError:
        _invalid()
    if len(encoded) > max_bytes or any(ord(character) < 32 and character not in "\n\r\t" for character in value) or any(ord(character) == 127 for character in value):
        _invalid()
    return value


def _canonical_name(value: object) -> str:
    from deerflow_extension_api import canonicalize_agent_identifier

    try:
        return canonicalize_agent_identifier(value, field_name="subagent name")
    except ValueError:
        _invalid()


def _plain_json(value: object, *, depth: int = 0) -> object:
    if depth > _MAX_JSON_DEPTH:
        _invalid()
    if value is None or isinstance(value, bool | int | str):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            _invalid()
        return value
    if isinstance(value, Mapping):
        if len(value) > _MAX_JSON_ENTRIES:
            _invalid()
        projected: dict[str, object] = {}
        for raw_key, child in sorted(value.items(), key=lambda pair: str(pair[0])):
            if not isinstance(raw_key, str) or not raw_key:
                _invalid()
            _bounded_text(raw_key, max_bytes=MAX_SUBAGENT_IDENTIFIER_BYTES)
            projected[raw_key] = _plain_json(child, depth=depth + 1)
        return projected
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        if len(value) > _MAX_JSON_ENTRIES:
            _invalid()
        return [_plain_json(child, depth=depth + 1) for child in value]
    _invalid()


def _freeze_json(value: object) -> object:
    if isinstance(value, Mapping):
        return MappingProxyType({str(key): _freeze_json(child) for key, child in value.items()})
    if isinstance(value, list | tuple):
        return tuple(_freeze_json(child) for child in value)
    return value


def _thaw_json(value: object) -> object:
    if isinstance(value, Mapping):
        return {str(key): _thaw_json(child) for key, child in value.items()}
    if isinstance(value, tuple | list):
        return [_thaw_json(child) for child in value]
    return value


def _canonical_names(values: Sequence[object], *, canonical_agent_names: bool = False) -> tuple[str, ...]:
    if not isinstance(values, Sequence) or isinstance(values, str | bytes | bytearray) or len(values) > _MAX_JSON_ENTRIES:
        _invalid()
    names: list[str] = []
    for value in values:
        name = _canonical_name(value) if canonical_agent_names else _bounded_text(value, max_bytes=MAX_SUBAGENT_IDENTIFIER_BYTES)
        if name in names:
            _invalid()
        names.append(name)
    return tuple(sorted(names))


def resolved_tool_contract_digest(tool: object) -> str:
    """Hash one tool's stable name/schema/source contract without its payload."""

    from deerflow.agents.assembly_descriptor import describe_tool

    descriptor = describe_tool(tool)
    return canonical_digest(
        {
            "version": 1,
            "domain": "resolved_subagent_tool_contract",
            "tool": {
                "name": descriptor.name,
                "description_hash": descriptor.description_hash,
                "schema_hash": descriptor.schema_hash,
                "source": descriptor.source,
                "mcp_server": descriptor.mcp_server,
                "mcp_transport": descriptor.mcp_transport,
            },
        }
    )


@dataclass(frozen=True)
class ResolvedSubagentDefinitionV1:
    """One fully resolved, execution-relevant subagent definition."""

    version: Literal[1]
    name: str
    source_kind: SubagentSourceKind
    source_version: str
    description: str
    system_prompt: str
    model: str | None
    model_settings: Mapping[str, object]
    tool_names: tuple[str, ...]
    skill_names: tuple[str, ...]
    max_turns: int
    timeout_seconds: float
    definition_digest: str
    # ``tools=None`` means inherit the parent tool surface.  Keep that distinct
    # from an explicit empty allowlist without weakening the stable V1 fields.
    inherits_tools: bool = False
    disallowed_tool_names: tuple[str, ...] = ()
    # Middleware/policy settings that affect construction (currently the
    # effective token-budget policy) live here rather than as mutable config.
    policy_settings: Mapping[str, object] = field(default_factory=dict)
    # Older accepted catalogs did not bind schemas. ``None`` preserves that
    # observable legacy state; durable batches require a complete tuple.
    tool_contract_digests: tuple[str, ...] | None = None

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != SUBAGENT_CATALOG_VERSION:
            _invalid()
        object.__setattr__(self, "name", _canonical_name(self.name))
        if self.source_kind not in _SOURCE_KINDS:
            _invalid()
        object.__setattr__(self, "source_version", _bounded_text(self.source_version, max_bytes=MAX_SUBAGENT_IDENTIFIER_BYTES))
        object.__setattr__(self, "description", _bounded_text(self.description, max_bytes=MAX_SUBAGENT_DESCRIPTION_BYTES))
        object.__setattr__(self, "system_prompt", _bounded_text(self.system_prompt, max_bytes=MAX_SUBAGENT_PROMPT_BYTES, allow_empty=True))
        if self.model is not None:
            object.__setattr__(self, "model", _bounded_text(self.model, max_bytes=MAX_SUBAGENT_IDENTIFIER_BYTES))
        if type(self.max_turns) is not int or self.max_turns < 1:
            _invalid()
        if isinstance(self.timeout_seconds, bool) or not isinstance(self.timeout_seconds, int | float) or not math.isfinite(float(self.timeout_seconds)) or float(self.timeout_seconds) <= 0:
            _invalid()
        object.__setattr__(self, "timeout_seconds", float(self.timeout_seconds))
        if type(self.inherits_tools) is not bool:
            _invalid()
        object.__setattr__(self, "tool_names", _canonical_names(self.tool_names))
        contracts = self.tool_contract_digests
        if contracts is not None:
            if not isinstance(contracts, Sequence) or isinstance(contracts, str | bytes | bytearray) or len(contracts) != len(self.tool_names) or any(not _is_digest(value) for value in contracts):
                _invalid()
            object.__setattr__(self, "tool_contract_digests", tuple(contracts))
        object.__setattr__(self, "skill_names", _canonical_names(self.skill_names))
        object.__setattr__(self, "disallowed_tool_names", _canonical_names(self.disallowed_tool_names))
        model_settings = _plain_json(self.model_settings)
        policy_settings = _plain_json(self.policy_settings)
        if not isinstance(model_settings, Mapping) or not isinstance(policy_settings, Mapping):
            _invalid()
        object.__setattr__(self, "model_settings", _freeze_json(model_settings))
        object.__setattr__(self, "policy_settings", _freeze_json(policy_settings))
        if not _is_digest(self.definition_digest) or self.definition_digest != canonical_digest(self._digest_projection()):
            _invalid()

    def _digest_projection(self) -> dict[str, object]:
        projection: dict[str, object] = {
            "version": self.version,
            "name": self.name,
            "source_kind": self.source_kind,
            "source_version": self.source_version,
            "description": self.description,
            "system_prompt": self.system_prompt,
            "model": self.model,
            "model_settings": _thaw_json(self.model_settings),
            "tool_names": list(self.tool_names),
            "skill_names": list(self.skill_names),
            "max_turns": self.max_turns,
            "timeout_seconds": self.timeout_seconds,
            "inherits_tools": self.inherits_tools,
            "disallowed_tool_names": list(self.disallowed_tool_names),
            "policy_settings": _thaw_json(self.policy_settings),
        }
        if self.tool_contract_digests is not None:
            projection["tool_contract_digests"] = list(self.tool_contract_digests)
        return projection

    def to_persisted_json(self) -> dict[str, object]:
        return {**self._digest_projection(), "definition_digest": self.definition_digest}

    @classmethod
    def from_persisted_json(cls, value: Mapping[str, object]) -> Self:
        if not isinstance(value, Mapping):
            _invalid()
        required = {item.name for item in fields(cls)} - _OPTIONAL_DEFINITION_FIELDS
        allowed = {item.name for item in fields(cls)}
        if not required <= set(value) or set(value) - allowed:
            _invalid()
        for list_field in (
            "tool_names",
            "skill_names",
            "disallowed_tool_names",
        ):
            raw_list = value.get(list_field, ())
            if not isinstance(raw_list, Sequence) or isinstance(
                raw_list,
                str | bytes | bytearray,
            ):
                _invalid()
        raw_contracts = value.get("tool_contract_digests")
        if raw_contracts is not None and (not isinstance(raw_contracts, Sequence) or isinstance(raw_contracts, str | bytes | bytearray)):
            _invalid()
        if not isinstance(value.get("model_settings"), Mapping) or not isinstance(
            value.get("policy_settings", {}),
            Mapping,
        ):
            _invalid()
        try:
            return cls(
                **{
                    **dict(value),
                    "inherits_tools": value.get("inherits_tools", False),
                    "disallowed_tool_names": tuple(value.get("disallowed_tool_names", ())),
                    "policy_settings": value.get("policy_settings", {}),
                    "tool_contract_digests": (None if raw_contracts is None else tuple(raw_contracts)),
                    "tool_names": tuple(value.get("tool_names", ())),
                    "skill_names": tuple(value.get("skill_names", ())),
                }
            )
        except SubagentCatalogError:
            raise
        except (TypeError, ValueError):
            _invalid()

    def to_subagent_config(self):
        """Rebuild the executor's lightweight config without a registry read."""

        from deerflow.subagents.config import SubagentConfig

        return SubagentConfig(
            name=self.name,
            description=self.description,
            system_prompt=self.system_prompt or None,
            # Admission expands inheritance into the exact then-available name
            # set. Keeping ``inherits_tools`` in the digest records the source
            # semantics, while execution receives an explicit allowlist so a
            # later MCP/config edit cannot widen the accepted worker.
            tools=list(self.tool_names),
            disallowed_tools=list(self.disallowed_tool_names),
            skills=list(self.skill_names),
            model=self.model or "inherit",
            max_turns=self.max_turns,
            timeout_seconds=self.timeout_seconds,
        )

    def verify_execution_settings(
        self,
        app_config: object,
        *,
        parent_model_name: str | None,
    ) -> None:
        """Check that construction inputs agree with the accepted projection."""

        effective_model = self.model or parent_model_name
        try:
            expected_model_settings = _model_settings_for(
                app_config,
                effective_model,
            )
            expected_policy_settings = _policy_settings_for(
                app_config,
                self.name,
            )
        except Exception as exc:
            raise SubagentCatalogError("subagent_definition_drift") from exc
        if _thaw_json(self.model_settings) != expected_model_settings or _thaw_json(self.policy_settings) != expected_policy_settings:
            raise SubagentCatalogError("subagent_definition_drift")


def resolved_subagent_definition(
    *,
    name: str,
    source_kind: SubagentSourceKind,
    source_version: str,
    description: str,
    system_prompt: str,
    model: str | None,
    model_settings: Mapping[str, object],
    tool_names: Sequence[str],
    skill_names: Sequence[str],
    max_turns: int,
    timeout_seconds: float,
    inherits_tools: bool = False,
    disallowed_tool_names: Sequence[str] = (),
    policy_settings: Mapping[str, object] | None = None,
    tool_contract_digests: Sequence[str] | None = None,
) -> ResolvedSubagentDefinitionV1:
    """Build one definition and derive its digest from canonical fields."""

    raw_tool_names = tuple(tool_names)
    canonical_tool_names = _canonical_names(raw_tool_names)
    normalized_tool_contracts: tuple[str, ...] | None = None
    if tool_contract_digests is not None:
        raw_contracts = tuple(tool_contract_digests)
        if len(raw_contracts) != len(raw_tool_names) or any(not _is_digest(value) for value in raw_contracts):
            _invalid()
        contracts_by_name = dict(zip(raw_tool_names, raw_contracts, strict=True))
        normalized_tool_contracts = tuple(contracts_by_name[name] for name in canonical_tool_names)
    values: dict[str, object] = {
        "version": SUBAGENT_CATALOG_VERSION,
        "name": _canonical_name(name),
        "source_kind": source_kind,
        "source_version": source_version,
        "description": description,
        "system_prompt": system_prompt,
        "model": model,
        "model_settings": model_settings,
        "tool_names": canonical_tool_names,
        "skill_names": tuple(skill_names),
        "max_turns": max_turns,
        "timeout_seconds": float(timeout_seconds),
        "inherits_tools": inherits_tools,
        "disallowed_tool_names": tuple(disallowed_tool_names),
        "policy_settings": policy_settings or {},
        "tool_contract_digests": normalized_tool_contracts,
    }
    # Normalize once through a temporary projection without trusting callers to
    # supply their own digest.  The final dataclass verifies it again.
    normalized = {
        **values,
        "source_version": _bounded_text(source_version, max_bytes=MAX_SUBAGENT_IDENTIFIER_BYTES),
        "description": _bounded_text(description, max_bytes=MAX_SUBAGENT_DESCRIPTION_BYTES),
        "system_prompt": _bounded_text(system_prompt, max_bytes=MAX_SUBAGENT_PROMPT_BYTES, allow_empty=True),
        "model_settings": _plain_json(model_settings),
        "tool_names": list(canonical_tool_names),
        "skill_names": list(_canonical_names(tuple(skill_names))),
        "timeout_seconds": float(timeout_seconds),
        "disallowed_tool_names": list(_canonical_names(tuple(disallowed_tool_names))),
        "policy_settings": _plain_json(policy_settings or {}),
    }
    if normalized_tool_contracts is not None:
        normalized["tool_contract_digests"] = list(normalized_tool_contracts)
    else:
        normalized.pop("tool_contract_digests", None)
    return ResolvedSubagentDefinitionV1(
        **values,
        definition_digest=canonical_digest(normalized),
    )


@dataclass(frozen=True)
class ResolvedSubagentCatalogV1:
    """Canonical accepted catalog shared by lead discovery and dispatch."""

    version: Literal[1]
    entries: tuple[ResolvedSubagentDefinitionV1, ...]
    allowed_names: tuple[str, ...]
    digest: str

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != SUBAGENT_CATALOG_VERSION:
            _invalid()
        if not isinstance(self.entries, Sequence) or isinstance(self.entries, str | bytes | bytearray) or len(self.entries) > MAX_SUBAGENT_CATALOG_ENTRIES:
            _invalid()
        entries = tuple(self.entries)
        if any(not isinstance(entry, ResolvedSubagentDefinitionV1) for entry in entries):
            _invalid()
        entries = tuple(sorted(entries, key=lambda item: item.name))
        names = tuple(entry.name for entry in entries)
        if len(names) != len(set(names)):
            _invalid()
        allowed_names = _canonical_names(self.allowed_names, canonical_agent_names=True)
        if allowed_names != names:
            _invalid()
        object.__setattr__(self, "entries", entries)
        object.__setattr__(self, "allowed_names", allowed_names)
        if not _is_digest(self.digest) or self.digest != canonical_digest(self._digest_projection()):
            _invalid()
        if len(_canonical_json_bytes(self.to_persisted_json())) > MAX_SUBAGENT_CATALOG_BYTES:
            _invalid()

    def _digest_projection(self) -> dict[str, object]:
        return {
            "version": self.version,
            "entries": [entry.to_persisted_json() for entry in self.entries],
            "allowed_names": list(self.allowed_names),
        }

    @classmethod
    def from_entries(
        cls,
        entries: Sequence[ResolvedSubagentDefinitionV1],
        *,
        allowed_names: Sequence[str],
    ) -> Self:
        if not isinstance(entries, Sequence) or isinstance(entries, str | bytes | bytearray) or len(entries) > MAX_SUBAGENT_CATALOG_ENTRIES or any(not isinstance(entry, ResolvedSubagentDefinitionV1) for entry in entries):
            _invalid()
        ordered_entries = tuple(sorted(entries, key=lambda item: item.name))
        ordered_names = _canonical_names(tuple(allowed_names), canonical_agent_names=True)
        projection = {
            "version": SUBAGENT_CATALOG_VERSION,
            "entries": [entry.to_persisted_json() for entry in ordered_entries],
            "allowed_names": list(ordered_names),
        }
        return cls(
            version=SUBAGENT_CATALOG_VERSION,
            entries=ordered_entries,
            allowed_names=ordered_names,
            digest=canonical_digest(projection),
        )

    @classmethod
    def empty(cls) -> Self:
        return cls.from_entries((), allowed_names=())

    def get(self, name: str) -> ResolvedSubagentDefinitionV1 | None:
        try:
            canonical = _canonical_name(name)
        except SubagentCatalogError:
            return None
        return next((entry for entry in self.entries if entry.name == canonical), None)

    def to_persisted_json(self) -> dict[str, object]:
        return {**self._digest_projection(), "digest": self.digest}

    @classmethod
    def from_persisted_json(cls, value: Mapping[str, object]) -> Self:
        if not isinstance(value, Mapping) or set(value) != {"version", "entries", "allowed_names", "digest"}:
            _invalid()
        raw_entries = value.get("entries")
        raw_allowed = value.get("allowed_names")
        if not isinstance(raw_entries, Sequence) or isinstance(raw_entries, str | bytes | bytearray):
            _invalid()
        if not isinstance(raw_allowed, Sequence) or isinstance(raw_allowed, str | bytes | bytearray):
            _invalid()
        try:
            return cls(
                version=value.get("version"),
                entries=tuple(ResolvedSubagentDefinitionV1.from_persisted_json(item) for item in raw_entries),
                allowed_names=tuple(raw_allowed),
                digest=value.get("digest"),
            )
        except SubagentCatalogError:
            raise
        except (TypeError, ValueError):
            _invalid()


def _canonical_json_bytes(value: object) -> bytes:
    import json

    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError):
        _invalid()


@dataclass(frozen=True)
class ResolvedSkillScopesV1:
    """Per-agent permission map over immutable accepted package digests."""

    version: Literal[1]
    scopes: Mapping[str, tuple[str, ...]]
    digest: str

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != SUBAGENT_CATALOG_VERSION or not isinstance(self.scopes, Mapping) or len(self.scopes) > MAX_SUBAGENT_SKILL_SCOPES:
            _invalid()
        normalized: dict[str, tuple[str, ...]] = {}
        for scope, raw_digests in self.scopes.items():
            if scope == "lead":
                canonical_scope = scope
            elif isinstance(scope, str) and scope.startswith("subagent:"):
                canonical_scope = f"subagent:{_canonical_name(scope.removeprefix('subagent:'))}"
            else:
                _invalid()
            if canonical_scope in normalized or not isinstance(raw_digests, Sequence) or isinstance(raw_digests, str | bytes | bytearray) or len(raw_digests) > MAX_SKILLS_PER_AGENT_SCOPE:
                _invalid()
            digests = tuple(sorted(raw_digests))
            if len(digests) != len(set(digests)) or any(not _is_digest(value) for value in digests):
                _invalid()
            normalized[canonical_scope] = digests
        if "lead" not in normalized:
            _invalid()
        object.__setattr__(self, "scopes", MappingProxyType(dict(sorted(normalized.items()))))
        if not _is_digest(self.digest) or self.digest != canonical_digest(self._digest_projection()):
            _invalid()

    def _digest_projection(self) -> dict[str, object]:
        return {
            "version": self.version,
            "scopes": {scope: list(digests) for scope, digests in sorted(self.scopes.items())},
        }

    @classmethod
    def from_scopes(cls, scopes: Mapping[str, Sequence[str]]) -> Self:
        if not isinstance(scopes, Mapping) or len(scopes) > MAX_SUBAGENT_SKILL_SCOPES:
            _invalid()
        for scope, digests in scopes.items():
            if not isinstance(scope, str) or not isinstance(digests, Sequence) or isinstance(digests, str | bytes | bytearray) or len(digests) > MAX_SKILLS_PER_AGENT_SCOPE or any(not _is_digest(digest) for digest in digests):
                _invalid()
        normalized = {str(scope): tuple(sorted(digests)) for scope, digests in sorted(scopes.items())}
        projection = {
            "version": SUBAGENT_CATALOG_VERSION,
            "scopes": {scope: list(digests) for scope, digests in normalized.items()},
        }
        return cls(
            version=SUBAGENT_CATALOG_VERSION,
            scopes=normalized,
            digest=canonical_digest(projection),
        )

    @classmethod
    def empty(cls) -> Self:
        return cls.from_scopes({"lead": ()})

    def for_scope(self, scope: str) -> tuple[str, ...]:
        return self.scopes.get(scope, ())

    def to_persisted_json(self) -> dict[str, object]:
        return {**self._digest_projection(), "digest": self.digest}

    @classmethod
    def from_persisted_json(cls, value: Mapping[str, object]) -> Self:
        if not isinstance(value, Mapping) or set(value) != {"version", "scopes", "digest"}:
            _invalid()
        raw_scopes = value.get("scopes")
        if not isinstance(raw_scopes, Mapping) or len(raw_scopes) > MAX_SUBAGENT_SKILL_SCOPES:
            _invalid()
        normalized: dict[str, tuple[object, ...]] = {}
        for scope, digests in raw_scopes.items():
            if not isinstance(digests, Sequence) or isinstance(
                digests,
                str | bytes | bytearray,
            ):
                _invalid()
            normalized[str(scope)] = tuple(digests)
        try:
            return cls(
                version=value.get("version"),
                scopes=normalized,
                digest=value.get("digest"),
            )
        except SubagentCatalogError:
            raise
        except (TypeError, ValueError):
            _invalid()


type SubagentFieldDisposition = Literal["included", "descriptive", "excluded"]


@dataclass(frozen=True)
class SubagentFieldClassification:
    disposition: SubagentFieldDisposition
    rationale: str


def _field(
    disposition: SubagentFieldDisposition,
    rationale: str,
) -> SubagentFieldClassification:
    return SubagentFieldClassification(disposition, rationale)


_EXECUTION_FIELD = "Included because the value changes reachability, discovery, construction, or an enforced runtime limit."
_SOURCE_PAYLOAD_FIELD = "Included through the validated ManagedSubagentDefinition payload that supplies execution fields."

SUBAGENT_CONFIG_FIELD_CLASSIFICATIONS = {
    name: _field("included", _EXECUTION_FIELD)
    for name in {
        "name",
        "description",
        "system_prompt",
        "tools",
        "disallowed_tools",
        "skills",
        "model",
        "max_turns",
        "timeout_seconds",
    }
}
MANAGED_SUBAGENT_FIELD_CLASSIFICATIONS = {
    **{
        name: _field("included", _EXECUTION_FIELD)
        for name in {
            "name",
            "description",
            "system_prompt",
            "tools",
            "disallowed_tools",
            "skills",
            "model",
            "max_turns",
            "timeout_seconds",
            "enabled",
        }
    },
    "display_name": _field(
        "descriptive",
        "Administrator UI label only; lead discovery uses the execution description and canonical name.",
    ),
}
CUSTOM_SUBAGENT_FIELD_CLASSIFICATIONS = {
    name: _field("included", _EXECUTION_FIELD)
    for name in {
        "description",
        "system_prompt",
        "tools",
        "disallowed_tools",
        "skills",
        "model",
        "max_turns",
        "timeout_seconds",
    }
}
SUBAGENT_OVERRIDE_FIELD_CLASSIFICATIONS = {
    name: _field("included", _EXECUTION_FIELD)
    for name in {
        "timeout_seconds",
        "max_turns",
        "model",
        "skills",
        "token_budget",
    }
}
SUBAGENTS_APP_FIELD_CLASSIFICATIONS = {
    name: _field("included", _EXECUTION_FIELD)
    for name in {
        "timeout_seconds",
        "max_turns",
        "max_total_per_run",
        "max_catalog_entries",
        "token_budget",
        "agents",
        "custom_agents",
    }
}
MANAGED_SUBAGENT_ROW_FIELD_CLASSIFICATIONS = {
    "id": _field(
        "excluded",
        "Database surrogate identity; it never participates in registry resolution or execution.",
    ),
    "name": _field(
        "excluded",
        "Database lookup index; validated definition.name is the canonical execution identity.",
    ),
    "definition": _field("included", _SOURCE_PAYLOAD_FIELD),
    "created_at": _field(
        "excluded",
        "Database audit timestamp with no execution semantics.",
    ),
    "updated_at": _field(
        "excluded",
        "Cache invalidation metadata only; source and definition digests bind accepted execution.",
    ),
}

SUBAGENT_CONFIG_INCLUDED_FIELDS = frozenset(SUBAGENT_CONFIG_FIELD_CLASSIFICATIONS)
MANAGED_SUBAGENT_INCLUDED_FIELDS = frozenset(name for name, classification in MANAGED_SUBAGENT_FIELD_CLASSIFICATIONS.items() if classification.disposition == "included")
MANAGED_SUBAGENT_DESCRIPTIVE_FIELDS = frozenset(name for name, classification in MANAGED_SUBAGENT_FIELD_CLASSIFICATIONS.items() if classification.disposition == "descriptive")
CUSTOM_SUBAGENT_INCLUDED_FIELDS = frozenset(CUSTOM_SUBAGENT_FIELD_CLASSIFICATIONS)
SUBAGENT_OVERRIDE_INCLUDED_FIELDS = frozenset(SUBAGENT_OVERRIDE_FIELD_CLASSIFICATIONS)
SUBAGENTS_APP_INCLUDED_FIELDS = frozenset(SUBAGENTS_APP_FIELD_CLASSIFICATIONS)


def assert_subagent_projection_complete() -> None:
    """Require an explicit treatment for every live definition field."""

    from deerflow.config.subagents_config import (
        CustomSubagentConfig,
        SubagentOverrideConfig,
        SubagentsAppConfig,
    )
    from deerflow.persistence.managed_subagents import ManagedSubagentDefinition
    from deerflow.persistence.managed_subagents.model import ManagedSubagentRow
    from deerflow.subagents.config import SubagentConfig

    classifications = (
        (
            set(SubagentConfig.__dataclass_fields__),
            SUBAGENT_CONFIG_FIELD_CLASSIFICATIONS,
        ),
        (
            set(ManagedSubagentDefinition.model_fields),
            MANAGED_SUBAGENT_FIELD_CLASSIFICATIONS,
        ),
        (
            set(CustomSubagentConfig.model_fields),
            CUSTOM_SUBAGENT_FIELD_CLASSIFICATIONS,
        ),
        (
            set(SubagentOverrideConfig.model_fields),
            SUBAGENT_OVERRIDE_FIELD_CLASSIFICATIONS,
        ),
        (
            set(SubagentsAppConfig.model_fields),
            SUBAGENTS_APP_FIELD_CLASSIFICATIONS,
        ),
        (
            set(ManagedSubagentRow.__table__.columns.keys()),
            MANAGED_SUBAGENT_ROW_FIELD_CLASSIFICATIONS,
        ),
    )
    for actual, classified in classifications:
        classified_names = set(classified)
        if actual != classified_names:
            missing = sorted(actual - classified_names)
            stale = sorted(classified_names - actual)
            raise AssertionError(f"Subagent snapshot field classification drifted; missing={missing}, stale={stale}")
        if any(classification.disposition not in {"included", "descriptive", "excluded"} or not classification.rationale.strip() for classification in classified.values()):
            raise AssertionError("Subagent snapshot fields require a disposition and rationale")


def _safe_model_settings(value: object) -> Mapping[str, object]:
    """Project non-secret model behavior settings into canonical JSON."""

    from deerflow.sandbox.env_policy import is_blocked_env_name

    def _secret_key(key: str) -> bool:
        normalized = "".join(character for character in key.lower() if character.isalnum())
        return is_blocked_env_name(key) or normalized in {
            "auth",
            "authorization",
            "cookie",
            "proxyauthorization",
            "setcookie",
        }

    def _project(value: object) -> object:
        model_dump = getattr(value, "model_dump", None)
        if callable(model_dump):
            value = model_dump(mode="python")
        if isinstance(value, Mapping):
            result: dict[str, object] = {}
            for raw_key, child in sorted(
                value.items(),
                key=lambda pair: str(pair[0]),
            ):
                if not isinstance(raw_key, str) or _secret_key(raw_key):
                    continue
                try:
                    result[raw_key] = _project(child)
                except SubagentCatalogError:
                    continue
            return result
        if isinstance(value, Sequence) and not isinstance(
            value,
            str | bytes | bytearray,
        ):
            return [_project(child) for child in value]
        return _plain_json(value)

    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        value = model_dump(mode="python")
    if not isinstance(value, Mapping):
        return {}
    excluded_metadata = frozenset({"name", "display_name", "description", "pricing"})
    projected: dict[str, object] = {}
    for raw_key, child in sorted(value.items(), key=lambda pair: str(pair[0])):
        if not isinstance(raw_key, str) or raw_key in excluded_metadata or _secret_key(raw_key):
            continue
        try:
            projected[raw_key] = _project(child)
        except SubagentCatalogError:
            # Arbitrary provider clients/objects and credential handles do not
            # belong in accepted JSON. Their stable profile is already pinned
            # by the parent AppConfig revision.
            continue
    return projected


def _model_settings_for(
    app_config: object,
    model_name: str | None,
) -> dict[str, object]:
    if model_name is None:
        return {}
    getter = getattr(app_config, "get_model_config", None)
    model_config = getter(model_name) if callable(getter) else None
    if model_config is None:
        _invalid()
    return dict(_safe_model_settings(model_config))


def _policy_settings_for(
    app_config: object,
    subagent_name: str,
) -> dict[str, object]:
    subagents_config = getattr(app_config, "subagents", app_config)
    token_budget_getter = getattr(subagents_config, "get_token_budget_for", None)
    if not callable(token_budget_getter):
        return {}
    token_budget = token_budget_getter(
        subagent_name,
        summarization_enabled=bool(
            getattr(
                getattr(app_config, "summarization", None),
                "enabled",
                False,
            )
        ),
    )
    if token_budget is None or not hasattr(token_budget, "model_dump"):
        return {}
    return {"token_budget": token_budget.model_dump(mode="json")}


def _available_tool_contracts(
    app_config: object,
    *,
    agent_config: object | None,
    model_name: str | None,
) -> Mapping[str, str]:
    """Resolve the pre-authorization tool ceiling once at admission."""

    from deerflow.tools import get_available_tools

    groups = getattr(agent_config, "tool_groups", None)
    try:
        tools = get_available_tools(
            groups=(None if groups is None else list(groups)),
            include_upload_tool=False,
            model_name=model_name,
            subagent_enabled=False,
            app_config=app_config,
        )
        names = _canonical_names(
            tuple(str(getattr(tool, "name", "")) for tool in tools),
        )
        tools_by_name = {str(getattr(tool, "name", "")): tool for tool in tools}
        if len(tools_by_name) != len(tools):
            _invalid()
        return MappingProxyType({name: resolved_tool_contract_digest(tools_by_name[name]) for name in names})
    except SubagentCatalogError:
        raise
    except Exception as exc:
        raise SubagentCatalogError("subagent_catalog_invalid") from exc


def _enabled_skill_names(app_config: object, *, user_id: str | None) -> tuple[str, ...]:
    from deerflow.skills.storage import get_or_new_skill_storage, get_or_new_user_skill_storage

    storage = get_or_new_user_skill_storage(user_id, app_config=app_config) if user_id else get_or_new_skill_storage(app_config=app_config)
    skills = storage.load_skills(enabled_only=True)
    names = [str(skill.name) for skill in skills]
    if len(names) != len(set(names)):
        _invalid()
    return tuple(sorted(names))


def snapshot_effective_subagents(
    *,
    app_config: object,
    agent_config: object | None,
    user_id: str | None,
    is_bootstrap: bool,
    enabled: bool = True,
    available_skill_names: Sequence[str] | None = None,
    parent_model_name: str | None = None,
) -> ResolvedSubagentCatalogV1:
    """Resolve every subagent reachable by this accepted lead exactly once."""

    assert_subagent_projection_complete()
    if not enabled:
        return ResolvedSubagentCatalogV1.empty()

    from deerflow.subagents import registry

    policy = getattr(agent_config, "allowed_subagents", None) if not is_bootstrap else None
    if policy == []:
        return ResolvedSubagentCatalogV1.empty()

    requested: tuple[str, ...] | None = None
    if policy is not None:
        try:
            requested = _canonical_names(tuple(policy), canonical_agent_names=True)
        except SubagentCatalogError:
            raise
    try:
        registered_names, resolved_definitions = registry._snapshot_resolved_subagent_configs(
            app_config=app_config,
            allowed_subagents=(None if requested is None else list(requested)),
        )
    except SubagentCatalogError:
        raise
    except Exception as exc:
        raise SubagentCatalogError("subagent_catalog_invalid") from exc
    if requested is not None:
        resolved_registered_names = _canonical_names(
            tuple(registered_names),
            canonical_agent_names=True,
        )
        if resolved_registered_names != requested:
            raise SubagentCatalogError("subagent_definition_missing")

    canonical_names = _canonical_names(
        tuple(definition.name for definition in resolved_definitions),
        canonical_agent_names=True,
    )
    configured_cap = getattr(getattr(app_config, "subagents", None), "max_catalog_entries", MAX_SUBAGENT_CATALOG_ENTRIES)
    if type(configured_cap) is not int or configured_cap < 1 or configured_cap > MAX_SUBAGENT_CATALOG_ENTRIES or len(canonical_names) > configured_cap:
        _invalid()

    loaded_skill_names: tuple[str, ...] | None = None
    if available_skill_names is not None:
        loaded_skill_names = _canonical_names(tuple(available_skill_names))
    available_tools_by_model: dict[str | None, Mapping[str, str]] = {}
    entries: list[ResolvedSubagentDefinitionV1] = []
    for resolved in resolved_definitions:
        name = resolved.name
        config = resolved.config
        source_kind = resolved.source_kind
        source_material = resolved.source_material

        configured_skills = config.skills
        if configured_skills is None:
            if loaded_skill_names is None:
                loaded_skill_names = _enabled_skill_names(app_config, user_id=user_id)
            skill_names = loaded_skill_names
        else:
            requested_skills = _canonical_names(tuple(configured_skills))
            if requested_skills:
                if loaded_skill_names is None:
                    loaded_skill_names = _enabled_skill_names(app_config, user_id=user_id)
                if not set(requested_skills) <= set(loaded_skill_names):
                    raise SubagentCatalogError("subagent_skill_material_missing")
            skill_names = requested_skills

        configured_model = None if config.model == "inherit" else config.model
        effective_model = configured_model or parent_model_name
        model_settings = _model_settings_for(app_config, effective_model)
        if effective_model not in available_tools_by_model:
            available_tools_by_model[effective_model] = _available_tool_contracts(
                app_config,
                agent_config=agent_config,
                model_name=effective_model,
            )
        all_tool_contracts = available_tools_by_model[effective_model]
        all_tool_names = tuple(all_tool_contracts)
        denied_tool_names = set(_canonical_names(tuple(config.disallowed_tools or ())))
        if config.tools is None:
            effective_tool_names = tuple(tool_name for tool_name in all_tool_names if tool_name not in denied_tool_names)
        else:
            # Explicit names are already the registry's authorization ceiling.
            # Preserve them even when a provider-backed tool is temporarily
            # unavailable; remote/provider health is deliberately not frozen.
            effective_tool_names = tuple(tool_name for tool_name in _canonical_names(tuple(config.tools)) if tool_name not in denied_tool_names)
        tool_contract_digests = tuple(all_tool_contracts[name] for name in effective_tool_names) if set(effective_tool_names) <= set(all_tool_contracts) else None
        source_version = canonical_digest(
            {
                "version": 1,
                "source_kind": source_kind,
                "definition": source_material,
            }
        )
        entries.append(
            resolved_subagent_definition(
                name=config.name,
                source_kind=source_kind,
                source_version=source_version,
                description=config.description,
                system_prompt=config.system_prompt or "",
                model=configured_model,
                model_settings=model_settings,
                tool_names=effective_tool_names,
                tool_contract_digests=tool_contract_digests,
                skill_names=skill_names,
                max_turns=config.max_turns,
                timeout_seconds=config.timeout_seconds,
                inherits_tools=config.tools is None,
                disallowed_tool_names=tuple(config.disallowed_tools or ()),
                policy_settings=_policy_settings_for(app_config, name),
            )
        )
    return ResolvedSubagentCatalogV1.from_entries(entries, allowed_names=canonical_names)


__all__ = [
    "CUSTOM_SUBAGENT_INCLUDED_FIELDS",
    "JsonScalar",
    "MANAGED_SUBAGENT_DESCRIPTIVE_FIELDS",
    "MANAGED_SUBAGENT_INCLUDED_FIELDS",
    "MAX_SUBAGENT_CATALOG_BYTES",
    "MAX_SUBAGENT_CATALOG_ENTRIES",
    "ResolvedSubagentCatalogV1",
    "ResolvedSubagentDefinitionV1",
    "ResolvedSkillScopesV1",
    "SUBAGENT_CONFIG_INCLUDED_FIELDS",
    "SUBAGENT_CATALOG_VERSION",
    "SUBAGENT_OVERRIDE_INCLUDED_FIELDS",
    "SUBAGENTS_APP_INCLUDED_FIELDS",
    "SubagentCatalogError",
    "assert_subagent_projection_complete",
    "resolved_subagent_definition",
    "resolved_tool_contract_digest",
    "snapshot_effective_subagents",
]
