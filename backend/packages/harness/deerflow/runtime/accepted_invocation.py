"""Immutable accepted-invocation facts and versioned digest projectors."""

from __future__ import annotations

import copy
import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any

_DIGEST_VERSION = 1
_AGENT_REVISION_VERSION = 1
_DECISION_EVIDENCE_V1 = {"version": 1, "decisions": []}


def _canonical_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _canonical_value(item) for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_canonical_value(item) for item in value]
    if isinstance(value, bytes):
        return {"bytes_sha256": hashlib.sha256(value).hexdigest(), "length": len(value)}
    if hasattr(value, "model_dump"):
        return _canonical_value(value.model_dump(mode="json"))
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def canonical_digest(value: Any) -> str:
    canonical = json.dumps(
        _canonical_value(value),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def _frozen_json_mapping(value: Mapping[str, Any] | None) -> Mapping[str, Any]:
    return _deep_freeze(_deep_thaw(value or {}))


def _deep_freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({str(key): _deep_freeze(item) for key, item in value.items()})
    if isinstance(value, (list, tuple, set, frozenset)):
        return tuple(_deep_freeze(item) for item in value)
    return value


def _deep_thaw(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _deep_thaw(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_deep_thaw(item) for item in value]
    return value


@dataclass(frozen=True)
class PrincipalProjection:
    user_id: str | None = None
    role: str | None = None
    oauth_provider: str | None = None
    oauth_id: str | None = None
    channel_user_id: str | None = None
    is_internal: bool = False

    def to_json(self) -> dict[str, Any]:
        return {
            "version": 1,
            "user_id": self.user_id,
            "role": self.role,
            "oauth_provider": self.oauth_provider,
            "oauth_id": self.oauth_id,
            "channel_user_id": self.channel_user_id,
            "is_internal": self.is_internal,
        }


@dataclass(frozen=True)
class InvocationOrigin:
    source_kind: str
    references: Mapping[str, Any] = field(default_factory=dict)
    contributor_references: tuple[dict[str, Any], ...] = ()

    def __post_init__(self) -> None:
        if self.source_kind not in {
            "http",
            "scheduled_task",
            "native_channel",
            "service",
        }:
            raise ValueError(f"unsupported invocation source kind {self.source_kind!r}")
        object.__setattr__(self, "references", _frozen_json_mapping(self.references))
        object.__setattr__(
            self,
            "contributor_references",
            tuple(_deep_freeze(_deep_thaw(item)) for item in self.contributor_references),
        )

    def base_json(self) -> dict[str, Any]:
        return {
            "version": 1,
            "source_kind": self.source_kind,
            "references": _deep_thaw(self.references),
        }

    def to_json(self) -> dict[str, Any]:
        return {
            **self.base_json(),
            "contributor_references": [_deep_thaw(item) for item in self.contributor_references],
        }


@dataclass(frozen=True)
class ResolvedAgentMaterialV1:
    """Captured graph-factory inputs.

    Projection fields are JSON-safe and form the revision identity. Runtime
    objects are deep-copied/captured by the resolver and intentionally remain
    process-local; they are never persisted by this type.
    """

    agent_id: str
    storage_source: str
    storage_version: str
    agent_config: Mapping[str, Any] | None
    soul: str | bytes
    model_profile: Mapping[str, Any]
    tool_groups: tuple[str, ...] = ()
    tools: tuple[str, ...] = ()
    skills: tuple[Mapping[str, Any], ...] = ()
    runtime_defaults: Mapping[str, Any] = field(default_factory=dict)
    app_config: Any | None = field(default=None, repr=False, compare=False)
    agent_config_object: Any | None = field(default=None, repr=False, compare=False)
    enabled_skill_objects: tuple[Any, ...] = field(default=(), repr=False, compare=False)
    all_skill_objects: tuple[Any, ...] = field(default=(), repr=False, compare=False)
    user_id: str | None = field(default=None, repr=False, compare=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "agent_config", None if self.agent_config is None else _frozen_json_mapping(self.agent_config))
        object.__setattr__(self, "model_profile", _frozen_json_mapping(self.model_profile))
        object.__setattr__(self, "runtime_defaults", _frozen_json_mapping(self.runtime_defaults))
        object.__setattr__(self, "tool_groups", tuple(self.tool_groups))
        object.__setattr__(self, "tools", tuple(self.tools))
        object.__setattr__(self, "skills", tuple(_frozen_json_mapping(item) for item in self.skills))
        if isinstance(self.soul, bytes):
            soul = bytes(self.soul)
        else:
            soul = self.soul.encode("utf-8")
        object.__setattr__(self, "soul", soul)

    def projector(self) -> dict[str, Any]:
        return {
            "version": _AGENT_REVISION_VERSION,
            "agent_id": self.agent_id,
            "storage_source": self.storage_source,
            "storage_version": self.storage_version,
            "agent_config": None if self.agent_config is None else _deep_thaw(self.agent_config),
            "soul": self.soul,
            "model_profile": _deep_thaw(self.model_profile),
            "tool_groups": list(self.tool_groups),
            "tools": list(self.tools),
            "skills": [_deep_thaw(skill) for skill in self.skills],
            "runtime_defaults": _deep_thaw(self.runtime_defaults),
        }


@dataclass(frozen=True)
class ResolvedAgentRevision:
    agent_id: str
    digest: str
    storage_source: str
    storage_version: str
    material: ResolvedAgentMaterialV1 | None = field(default=None, repr=False, compare=False)

    @classmethod
    def from_material(cls, material: ResolvedAgentMaterialV1) -> ResolvedAgentRevision:
        return cls(
            agent_id=material.agent_id,
            digest=canonical_digest(material.projector()),
            storage_source=material.storage_source,
            storage_version=material.storage_version,
            material=material,
        )

    def to_json(self) -> dict[str, Any]:
        return {
            "version": _AGENT_REVISION_VERSION,
            "agent_id": self.agent_id,
            "storage_source": self.storage_source,
            "storage_version": self.storage_version,
            "digest": self.digest,
        }


@dataclass(frozen=True)
class AcceptedInvocation:
    principal: PrincipalProjection
    origin: InvocationOrigin
    thread_id: str
    context_references: Mapping[str, Any]
    agent_revision: ResolvedAgentRevision
    normalized_input: Any
    execution_options: Mapping[str, Any]
    extension_generation: int
    principal_digest: str
    base_origin_digest: str
    accepted_context_digest: str
    runtime_identity_digest: str
    contributor_execution_digest: str
    decision_evidence: Mapping[str, Any] = field(default_factory=lambda: copy.deepcopy(_DECISION_EVIDENCE_V1))

    def __post_init__(self) -> None:
        object.__setattr__(self, "context_references", _frozen_json_mapping(self.context_references))
        object.__setattr__(self, "execution_options", _frozen_json_mapping(self.execution_options))
        object.__setattr__(self, "normalized_input", _deep_freeze(_canonical_value(self.normalized_input)))
        object.__setattr__(self, "decision_evidence", _frozen_json_mapping(self.decision_evidence))

    @property
    def extension_manifest_digest(self) -> str | None:
        """Digest of the immutable Capability Host manifest accepted for this run."""

        evidence = self.decision_evidence.get("capability_manifest")
        if not isinstance(evidence, Mapping):
            return None
        digest = evidence.get("digest")
        if not isinstance(digest, str) or len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
            return None
        return digest

    @classmethod
    def seal(
        cls,
        *,
        principal: PrincipalProjection,
        origin: InvocationOrigin,
        thread_id: str,
        context_references: Mapping[str, Any],
        agent_revision: ResolvedAgentRevision,
        normalized_input: Any,
        execution_options: Mapping[str, Any],
        extension_generation: int,
        extension_manifest_digest: str | None = None,
        contributor_execution_digest: str,
    ) -> AcceptedInvocation:
        if extension_manifest_digest is not None and (len(extension_manifest_digest) != 64 or any(character not in "0123456789abcdef" for character in extension_manifest_digest)):
            raise ValueError("extension_manifest_digest must be a lowercase SHA-256 digest")
        principal_digest = canonical_digest({"version": _DIGEST_VERSION, "principal": principal.to_json()})
        base_origin_digest = canonical_digest({"version": _DIGEST_VERSION, "origin": origin.base_json()})
        accepted_context_digest = canonical_digest(
            {
                "version": _DIGEST_VERSION,
                "context": context_references,
                "contributor_execution_digest": contributor_execution_digest,
            }
        )
        runtime_identity_digest = canonical_digest(
            {
                "version": _DIGEST_VERSION,
                "principal_digest": principal_digest,
                "base_origin_digest": base_origin_digest,
                "thread_id": thread_id,
                "agent_revision_digest": agent_revision.digest,
                "input": normalized_input,
                "execution_options": execution_options,
                "extension_generation": extension_generation,
                "extension_manifest_digest": extension_manifest_digest,
                "accepted_context_digest": accepted_context_digest,
            }
        )
        decision_evidence = copy.deepcopy(_DECISION_EVIDENCE_V1)
        if extension_manifest_digest is not None:
            decision_evidence["capability_manifest"] = {
                "version": 1,
                "generation": extension_generation,
                "digest": extension_manifest_digest,
            }
        return cls(
            principal=principal,
            origin=origin,
            thread_id=thread_id,
            context_references=context_references,
            agent_revision=agent_revision,
            normalized_input=normalized_input,
            execution_options=execution_options,
            extension_generation=extension_generation,
            principal_digest=principal_digest,
            base_origin_digest=base_origin_digest,
            accepted_context_digest=accepted_context_digest,
            runtime_identity_digest=runtime_identity_digest,
            contributor_execution_digest=contributor_execution_digest,
            decision_evidence=decision_evidence,
        )

    def to_persisted(self) -> dict[str, Any]:
        return {
            "origin_json": self.origin.to_json(),
            "principal_projection_json": self.principal.to_json(),
            "principal_projection_digest": self.principal_digest,
            "base_origin_digest": self.base_origin_digest,
            "accepted_context_digest": self.accepted_context_digest,
            "agent_revision_json": self.agent_revision.to_json(),
            "agent_revision_digest": self.agent_revision.digest,
            "extension_generation": self.extension_generation,
            "decision_evidence_json": _deep_thaw(self.decision_evidence),
        }

    @classmethod
    def from_persisted(cls, row: Mapping[str, Any]) -> AcceptedInvocation | None:
        revision_digest = row.get("agent_revision_digest")
        revision_json = row.get("agent_revision_json")
        origin_json = row.get("origin_json")
        principal_json = row.get("principal_projection_json")
        if not revision_digest or not isinstance(revision_json, Mapping) or not isinstance(origin_json, Mapping) or not isinstance(principal_json, Mapping):
            return None
        principal = PrincipalProjection(
            user_id=principal_json.get("user_id"),
            role=principal_json.get("role"),
            oauth_provider=principal_json.get("oauth_provider"),
            oauth_id=principal_json.get("oauth_id"),
            channel_user_id=principal_json.get("channel_user_id"),
            is_internal=bool(principal_json.get("is_internal", False)),
        )
        origin = InvocationOrigin(
            source_kind=str(origin_json.get("source_kind")),
            references=origin_json.get("references") or {},
            contributor_references=tuple(origin_json.get("contributor_references") or ()),
        )
        revision = ResolvedAgentRevision(
            agent_id=str(revision_json.get("agent_id") or "default"),
            digest=str(revision_digest),
            storage_source=str(revision_json.get("storage_source") or "unknown"),
            storage_version=str(revision_json.get("storage_version") or "unknown"),
        )
        return cls(
            principal=principal,
            origin=origin,
            thread_id=str(row.get("thread_id") or ""),
            context_references={},
            agent_revision=revision,
            normalized_input={},
            execution_options={},
            extension_generation=int(row.get("extension_generation") or 0),
            principal_digest=str(row.get("principal_projection_digest") or ""),
            base_origin_digest=str(row.get("base_origin_digest") or ""),
            accepted_context_digest=str(row.get("accepted_context_digest") or ""),
            runtime_identity_digest="",
            contributor_execution_digest="",
            decision_evidence=row.get("decision_evidence_json") or copy.deepcopy(_DECISION_EVIDENCE_V1),
        )


__all__ = [
    "AcceptedInvocation",
    "InvocationOrigin",
    "PrincipalProjection",
    "ResolvedAgentMaterialV1",
    "ResolvedAgentRevision",
    "canonical_digest",
]
