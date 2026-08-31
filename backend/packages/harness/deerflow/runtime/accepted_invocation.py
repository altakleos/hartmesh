"""Immutable accepted-invocation facts and versioned digest projectors."""

from __future__ import annotations

import copy
import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from types import MappingProxyType
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from deerflow.runtime.skill_snapshot import AcceptedSkillSnapshot
    from deerflow.runtime.subagent_snapshot import (
        ResolvedSkillScopesV1,
        ResolvedSubagentCatalogV1,
    )

from deerflow_extension_api import (
    InvocationIdentityV1,
    TrustedRunContextV1,
    canonicalize_agent_identifier,
    validate_model_profile_identifier,
    validate_thread_identifier,
)

_DIGEST_VERSION = 1
_AGENT_REVISION_VERSION = 1
_DECISION_EVIDENCE_V1 = {"version": 1, "decisions": []}
_TOOL_RECEIPT_EVIDENCE_V1 = {"version": 1}
_SHA256_LENGTH = 64
_EFFECTIVE_EXECUTION_PROJECTION_KEY = "__accepted_request_projection_v1"
_REQUEST_DIGEST_VERSION = "sha256-canonical-json-v1"
_CANONICAL_EXECUTION_SEMANTICS = "canonical_execution_v2"
_EFFECTIVE_EXECUTION_FIELDS_V1 = frozenset(
    {
        "thread_id",
        "agent_selector",
        "agent_revision_digest",
        "principal_digest",
        "base_origin_digest",
        "accepted_context_digest",
        "runtime_identity_digest",
        "contributor_execution_digest",
        "extension_generation",
        "input",
        "command",
        "multitask_strategy",
        "checkpoint",
        "interrupt_before",
        "interrupt_after",
        "execution_context",
        "recursion_limit",
    }
)
_ACCEPTED_CONTEXT_REFERENCE_KEYS = frozenset(
    {
        "non_interactive",
        "is_plan_mode",
        "subagent_enabled",
        "max_concurrent_subagents",
        "max_total_subagents",
    }
)

# Host-internal runtime context keys. Callers may not supply either value;
# the worker installs them only from the accepted record after its fences pass.
INVOCATION_IDENTITY_CONTEXT_KEY = "__deerflow_invocation_identity"
INVOCATION_ORIGIN_CONTEXT_KEY = "__deerflow_invocation_origin"
TRUSTED_RUN_CONTEXT_KEY = "__deerflow_trusted_run_context"


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
    """Return the lowercase SHA-256 digest of one canonical JSON projection."""

    canonical = json.dumps(
        _canonical_value(value),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def canonical_effective_execution_digest(value: Any) -> str:
    """Hash the host-internal effective execution projection contract."""

    canonical = json.dumps(
        {
            "version": _REQUEST_DIGEST_VERSION,
            "request": _canonical_value(value),
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def _require_digest(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or len(value) != _SHA256_LENGTH or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{field_name} must be a lowercase SHA-256 digest")
    return value


def _require_matching_digest(
    value: object,
    expected: str,
    *,
    field_name: str,
) -> str:
    digest = _require_digest(value, field_name=field_name)
    if digest != expected:
        raise ValueError(f"{field_name} does not match its persisted facts")
    return digest


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
    """Immutable effective-principal facts accepted for one invocation."""

    user_id: str | None = None
    role: str | None = None
    oauth_provider: str | None = None
    oauth_id: str | None = None
    channel_user_id: str | None = None
    is_internal: bool = False
    identity: InvocationIdentityV1 | None = None

    def __post_init__(self) -> None:
        identity = self.identity
        if identity is None:
            if self.is_internal and (self.channel_user_id is not None or self.role not in {"internal", "service"}):
                object.__setattr__(self, "is_internal", False)
            return
        if not isinstance(identity, InvocationIdentityV1):
            raise TypeError("identity must be InvocationIdentityV1 or None")
        subject = identity.effective_subject
        object.__setattr__(self, "user_id", subject.subject_id)
        object.__setattr__(self, "role", subject.role)
        object.__setattr__(self, "oauth_provider", subject.oauth_provider)
        object.__setattr__(self, "oauth_id", subject.oauth_id)
        object.__setattr__(self, "is_internal", subject.kind == "service")

    def to_json(self) -> dict[str, Any]:
        result = {
            "version": 2 if self.identity is not None else 1,
            "user_id": self.user_id,
            "role": self.role,
            "oauth_provider": self.oauth_provider,
            "oauth_id": self.oauth_id,
            "channel_user_id": self.channel_user_id,
            "is_internal": self.is_internal,
        }
        if self.identity is not None:
            result["identity"] = self.identity.to_json()
        return result


@dataclass(frozen=True)
class InvocationOrigin:
    """Immutable source kind and bounded correlation evidence for an invocation."""

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
    subagent_catalog: ResolvedSubagentCatalogV1 | None = None
    skill_scopes: ResolvedSkillScopesV1 | None = None
    app_config: Any | None = field(default=None, repr=False, compare=False)
    agent_config_object: Any | None = field(default=None, repr=False, compare=False)
    enabled_skill_objects: tuple[Any, ...] = field(default=(), repr=False, compare=False)
    all_skill_objects: tuple[Any, ...] = field(default=(), repr=False, compare=False)
    user_id: str | None = field(default=None, repr=False, compare=False)
    skill_snapshot: AcceptedSkillSnapshot | None = field(
        default=None,
        repr=False,
        compare=False,
    )

    def __post_init__(self) -> None:
        from deerflow.runtime.subagent_snapshot import (
            ResolvedSkillScopesV1,
            ResolvedSubagentCatalogV1,
        )

        catalog = self.subagent_catalog
        if catalog is None:
            catalog = ResolvedSubagentCatalogV1.empty()
        if not isinstance(catalog, ResolvedSubagentCatalogV1):
            raise TypeError("subagent_catalog must be ResolvedSubagentCatalogV1 or None")
        scopes = self.skill_scopes
        if scopes is None:
            scopes = ResolvedSkillScopesV1.empty()
        if not isinstance(scopes, ResolvedSkillScopesV1):
            raise TypeError("skill_scopes must be ResolvedSkillScopesV1 or None")
        expected_scopes = {"lead", *(f"subagent:{name}" for name in catalog.allowed_names)}
        if set(scopes.scopes) != expected_scopes:
            raise ValueError("accepted skill scopes must exactly match the subagent catalog")
        object.__setattr__(self, "subagent_catalog", catalog)
        object.__setattr__(self, "skill_scopes", scopes)
        canonical_agent_id = canonicalize_agent_identifier(
            self.agent_id,
            field_name="resolved agent material id",
        )
        object.__setattr__(self, "agent_id", canonical_agent_id)
        if self.agent_config is not None and "name" in self.agent_config:
            config_agent_id = canonicalize_agent_identifier(
                self.agent_config["name"],
                field_name="resolved agent material config name",
            )
            if config_agent_id != canonical_agent_id:
                raise ValueError("resolved agent material config name must match agent_id")
        profile_name = self.model_profile.get("name")
        if profile_name is not None:
            validate_model_profile_identifier(
                profile_name,
                field_name="resolved model profile identifier",
            )
        object.__setattr__(self, "agent_config", None if self.agent_config is None else _frozen_json_mapping(self.agent_config))
        object.__setattr__(self, "model_profile", _frozen_json_mapping(self.model_profile))
        object.__setattr__(self, "runtime_defaults", _frozen_json_mapping(self.runtime_defaults))
        object.__setattr__(self, "tool_groups", tuple(self.tool_groups))
        object.__setattr__(self, "tools", tuple(self.tools))
        object.__setattr__(self, "skills", tuple(_frozen_json_mapping(item) for item in self.skills))
        object.__setattr__(self, "enabled_skill_objects", tuple(self.enabled_skill_objects))
        object.__setattr__(self, "all_skill_objects", tuple(self.all_skill_objects))
        if isinstance(self.soul, bytes):
            soul = bytes(self.soul)
        else:
            soul = self.soul.encode("utf-8")
        object.__setattr__(self, "soul", soul)

    def verify_process_material(self) -> None:
        """Verify process-local immutable material immediately before use."""
        if self.skill_snapshot is not None:
            self.skill_snapshot.verify()
        permitted = {projection.content_digest for projection in (() if self.skill_snapshot is None else self.skill_snapshot.projections)}
        required = {digest for digests in self.skill_scopes.scopes.values() for digest in digests}
        if not required <= permitted:
            from deerflow.runtime.subagent_snapshot import SubagentCatalogError

            raise SubagentCatalogError("subagent_skill_material_missing")

    def skill_objects_for_scope(self, scope: str) -> tuple[Any, ...]:
        """Return only the accepted packages authorized for one agent scope."""

        digests = set(self.skill_scopes.for_scope(scope))
        if not digests or self.skill_snapshot is None:
            return ()
        projection_by_name = {projection.name: projection for projection in self.skill_snapshot.projections}
        return tuple(skill for skill in self.skill_snapshot.skills if ((projection := projection_by_name.get(str(getattr(skill, "name", "")))) is not None and projection.content_digest in digests))

    def retain_process_material(self) -> ResolvedAgentMaterialV1:
        """Return an equivalent material record with an independent lease."""
        if self.skill_snapshot is None:
            return self
        return replace(
            self,
            skill_snapshot=self.skill_snapshot.retain(),
        )

    def release_process_material(self) -> None:
        """Idempotently release process-local material after launch or terminalization."""
        if self.skill_snapshot is not None:
            self.skill_snapshot.release()

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
            "subagent_catalog": self.subagent_catalog.to_persisted_json(),
            "skill_scopes": self.skill_scopes.to_persisted_json(),
        }


@dataclass(frozen=True)
class ResolvedAgentRevision:
    """Stable revision identity with optional process-local captured material."""

    agent_id: str
    digest: str
    storage_source: str
    storage_version: str
    subagent_catalog: ResolvedSubagentCatalogV1 | None = field(default=None, repr=False)
    skill_scopes: ResolvedSkillScopesV1 | None = field(default=None, repr=False)
    legacy_live_catalog: bool = field(default=False, repr=False)
    material: ResolvedAgentMaterialV1 | None = field(default=None, repr=False, compare=False)

    @classmethod
    def from_material(cls, material: ResolvedAgentMaterialV1) -> ResolvedAgentRevision:
        return cls(
            agent_id=material.agent_id,
            digest=canonical_digest(material.projector()),
            storage_source=material.storage_source,
            storage_version=material.storage_version,
            subagent_catalog=material.subagent_catalog,
            skill_scopes=material.skill_scopes,
            material=material,
        )

    def to_json(self) -> dict[str, Any]:
        result = {
            "version": _AGENT_REVISION_VERSION,
            "agent_id": self.agent_id,
            "storage_source": self.storage_source,
            "storage_version": self.storage_version,
            "digest": self.digest,
        }
        if self.subagent_catalog is not None and self.skill_scopes is not None:
            result["subagent_catalog"] = self.subagent_catalog.to_persisted_json()
            result["skill_scopes"] = self.skill_scopes.to_persisted_json()
        return result


@dataclass(frozen=True)
class AcceptedInvocation:
    """Sealed immutable execution facts retained from durable admission."""

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
    trusted_context: TrustedRunContextV1 | None = field(default=None, repr=False)
    decision_evidence: Mapping[str, Any] = field(default_factory=lambda: copy.deepcopy(_DECISION_EVIDENCE_V1))

    def __post_init__(self) -> None:
        object.__setattr__(self, "context_references", _frozen_json_mapping(self.context_references))
        object.__setattr__(self, "execution_options", _frozen_json_mapping(self.execution_options))
        object.__setattr__(self, "normalized_input", _deep_freeze(_canonical_value(self.normalized_input)))
        object.__setattr__(self, "decision_evidence", _frozen_json_mapping(self.decision_evidence))
        if self.trusted_context is not None and not isinstance(self.trusted_context, TrustedRunContextV1):
            raise TypeError("trusted_context must be TrustedRunContextV1 or None")

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

    @property
    def tool_receipt_evidence_version(self) -> int | None:
        """Receipt capability captured when this invocation was admitted."""

        return self.tool_receipt_evidence_version_from_persisted({"decision_evidence_json": self.decision_evidence})

    @staticmethod
    def tool_receipt_evidence_version_from_persisted(
        row: Mapping[str, Any],
    ) -> int | None:
        """Read only the additive capability marker from a persisted row."""

        decision_evidence = row.get("decision_evidence_json")
        if not isinstance(decision_evidence, Mapping):
            return None
        evidence = decision_evidence.get("tool_receipts")
        if not isinstance(evidence, Mapping) or set(evidence) != {"version"}:
            return None
        return 1 if evidence.get("version") == 1 else None

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
        trusted_context: TrustedRunContextV1 | None = None,
    ) -> AcceptedInvocation:
        thread_id = validate_thread_identifier(thread_id)
        if extension_manifest_digest is not None and (len(extension_manifest_digest) != 64 or any(character not in "0123456789abcdef" for character in extension_manifest_digest)):
            raise ValueError("extension_manifest_digest must be a lowercase SHA-256 digest")
        if trusted_context is not None:
            if principal.identity != trusted_context.identity:
                raise ValueError("trusted context identity must match the accepted principal")
            if origin.source_kind != trusted_context.origin.source_kind:
                raise ValueError("trusted context Origin must match the accepted source")
            if thread_id != trusted_context.thread_id:
                raise ValueError("trusted context thread must match the accepted thread")
            if agent_revision.agent_id != trusted_context.agent_revision.agent_id or agent_revision.digest != trusted_context.agent_revision.digest:
                raise ValueError("trusted context agent revision must match the accepted revision")
            if extension_generation != trusted_context.extension_generation or extension_manifest_digest != trusted_context.extension_manifest_digest:
                raise ValueError("trusted context extension generation must match accepted evidence")
        principal_digest = canonical_digest({"version": _DIGEST_VERSION, "principal": principal.to_json()})
        base_origin_digest = canonical_digest({"version": _DIGEST_VERSION, "origin": origin.base_json()})
        accepted_context_digest = canonical_digest(
            {
                "version": _DIGEST_VERSION,
                "context": context_references,
                "contributor_execution_digest": contributor_execution_digest,
                "trusted_context_execution_digest": None if trusted_context is None else trusted_context.execution_digest,
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
        decision_evidence["tool_receipts"] = copy.deepcopy(_TOOL_RECEIPT_EVIDENCE_V1)
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
            trusted_context=trusted_context,
            decision_evidence=decision_evidence,
        )

    def to_persisted(self) -> dict[str, Any]:
        decision_evidence = _deep_thaw(self.decision_evidence)
        if self.trusted_context is not None:
            decision_evidence["trusted_run_context"] = self.trusted_context.to_persisted_json()
        return {
            "origin_json": self.origin.to_json(),
            "principal_projection_json": self.principal.to_json(),
            "principal_projection_digest": self.principal_digest,
            "base_origin_digest": self.base_origin_digest,
            "accepted_context_digest": self.accepted_context_digest,
            "agent_revision_json": self.agent_revision.to_json(),
            "agent_revision_digest": self.agent_revision.digest,
            "extension_generation": self.extension_generation,
            "decision_evidence_json": decision_evidence,
        }

    @classmethod
    def from_persisted(cls, row: Mapping[str, Any]) -> AcceptedInvocation | None:
        revision_digest = row.get("agent_revision_digest")
        revision_json = row.get("agent_revision_json")
        origin_json = row.get("origin_json")
        principal_json = row.get("principal_projection_json")
        accepted_fields = (
            revision_digest,
            revision_json,
            origin_json,
            principal_json,
            row.get("principal_projection_digest"),
            row.get("base_origin_digest"),
            row.get("accepted_context_digest"),
        )
        if all(value is None for value in accepted_fields):
            return None
        if not isinstance(revision_json, Mapping) or not isinstance(origin_json, Mapping) or not isinstance(principal_json, Mapping):
            raise ValueError("accepted invocation evidence is incomplete")

        principal_version = principal_json.get("version")
        if principal_version not in {1, 2}:
            raise ValueError("principal projection has an unsupported version")
        known_principal_fields = {
            "version",
            "user_id",
            "role",
            "oauth_provider",
            "oauth_id",
            "channel_user_id",
            "is_internal",
            "identity",
        }
        if set(principal_json) - known_principal_fields:
            raise ValueError("principal projection has unknown fields")
        if principal_version == 2 and set(principal_json) != known_principal_fields:
            raise ValueError("principal projection version 2 is missing fields")
        identity_json = principal_json.get("identity")
        principal = PrincipalProjection(
            user_id=principal_json.get("user_id"),
            role=principal_json.get("role"),
            oauth_provider=principal_json.get("oauth_provider"),
            oauth_id=principal_json.get("oauth_id"),
            channel_user_id=principal_json.get("channel_user_id"),
            is_internal=bool(principal_json.get("is_internal", False)),
            identity=(InvocationIdentityV1.from_json(identity_json) if isinstance(identity_json, Mapping) else None),
        )
        if principal_version == 2:
            if principal.identity is None:
                raise ValueError("principal projection version 2 requires split identity")
            if principal.to_json() != _deep_thaw(principal_json):
                raise ValueError("principal projection contradicts its split identity")
        persisted_principal_digest = _require_matching_digest(
            row.get("principal_projection_digest"),
            canonical_digest({"version": _DIGEST_VERSION, "principal": principal.to_json()}),
            field_name="principal projection digest",
        )

        known_origin_fields = {
            "version",
            "source_kind",
            "references",
            "contributor_references",
        }
        if origin_json.get("version") != 1 or set(origin_json) - known_origin_fields:
            raise ValueError("accepted Origin has unknown fields or an unsupported version")
        references = origin_json.get("references")
        contributor_references = origin_json.get("contributor_references", ())
        if not isinstance(references, Mapping) or not isinstance(contributor_references, (list, tuple)):
            raise ValueError("accepted Origin references are malformed")
        origin = InvocationOrigin(
            source_kind=str(origin_json.get("source_kind")),
            references=references,
            contributor_references=tuple(contributor_references),
        )
        persisted_base_origin_digest = _require_matching_digest(
            row.get("base_origin_digest"),
            canonical_digest({"version": _DIGEST_VERSION, "origin": origin.base_json()}),
            field_name="base Origin digest",
        )
        if principal.identity is None and principal.user_id is not None and origin.source_kind in {"native_channel", "scheduled_task"}:
            # Legacy rows did not distinguish a represented human from the
            # internal worker that delivered it. Source evidence can safely
            # remove that historical privilege, but never prove service
            # authority or reconstruct an acting service.
            principal = replace(principal, is_internal=False)

        if revision_json.get("version") != _AGENT_REVISION_VERSION:
            raise ValueError("agent revision has an unsupported version")
        base_revision_fields = {
            "version",
            "agent_id",
            "storage_source",
            "storage_version",
            "digest",
        }
        snapshotted_revision_fields = base_revision_fields | {
            "subagent_catalog",
            "skill_scopes",
        }
        revision_fields = set(revision_json)
        if revision_fields == base_revision_fields:
            subagent_catalog = None
            skill_scopes = None
            legacy_live_catalog = True
        elif revision_fields == snapshotted_revision_fields:
            from deerflow.runtime.subagent_snapshot import (
                ResolvedSkillScopesV1,
                ResolvedSubagentCatalogV1,
                SubagentCatalogError,
            )

            raw_catalog = revision_json.get("subagent_catalog")
            raw_scopes = revision_json.get("skill_scopes")
            if not isinstance(raw_catalog, Mapping) or not isinstance(raw_scopes, Mapping):
                raise SubagentCatalogError("subagent_catalog_invalid")
            subagent_catalog = ResolvedSubagentCatalogV1.from_persisted_json(raw_catalog)
            skill_scopes = ResolvedSkillScopesV1.from_persisted_json(raw_scopes)
            expected_scopes = {
                "lead",
                *(f"subagent:{name}" for name in subagent_catalog.allowed_names),
            }
            if set(skill_scopes.scopes) != expected_scopes:
                raise SubagentCatalogError("subagent_catalog_invalid")
            legacy_live_catalog = False
        else:
            raise ValueError("agent revision has unknown or missing fields")
        persisted_revision_digest = _require_digest(
            revision_digest,
            field_name="agent revision digest",
        )
        _require_matching_digest(
            revision_json.get("digest"),
            persisted_revision_digest,
            field_name="agent revision digest",
        )
        revision = ResolvedAgentRevision(
            agent_id=canonicalize_agent_identifier(
                revision_json.get("agent_id"),
                field_name="persisted agent revision id",
            ),
            digest=persisted_revision_digest,
            storage_source=str(revision_json.get("storage_source")),
            storage_version=str(revision_json.get("storage_version")),
            subagent_catalog=subagent_catalog,
            skill_scopes=skill_scopes,
            legacy_live_catalog=legacy_live_catalog,
        )
        thread_id = validate_thread_identifier(row.get("thread_id"), field_name="persisted thread_id")
        extension_generation = row.get("extension_generation")
        if type(extension_generation) is not int or extension_generation < 0:
            raise ValueError("extension generation must be a non-negative integer")
        persisted_context_digest = _require_digest(
            row.get("accepted_context_digest"),
            field_name="accepted context digest",
        )
        decision_evidence = row.get("decision_evidence_json")
        if decision_evidence is None:
            decision_evidence = copy.deepcopy(_DECISION_EVIDENCE_V1)
        if not isinstance(decision_evidence, Mapping):
            raise ValueError("accepted decision evidence must be a mapping")
        if decision_evidence.get("version") != 1 or not isinstance(decision_evidence.get("decisions", []), (list, tuple)):
            raise ValueError("accepted decision evidence has an unsupported version or malformed decisions")
        tool_receipt_evidence = decision_evidence.get("tool_receipts")
        if tool_receipt_evidence is not None and (not isinstance(tool_receipt_evidence, Mapping) or set(tool_receipt_evidence) != {"version"} or tool_receipt_evidence.get("version") != 1):
            raise ValueError("accepted tool receipt evidence is malformed")
        trusted_json = decision_evidence.get("trusted_run_context")
        if "trusted_run_context" in decision_evidence and not isinstance(trusted_json, Mapping):
            raise ValueError("trusted run-context evidence is malformed")
        trusted_context = TrustedRunContextV1.from_persisted_json(trusted_json) if isinstance(trusted_json, Mapping) else None
        manifest_evidence = decision_evidence.get("capability_manifest")
        manifest_digest: str | None = None
        if manifest_evidence is not None:
            if not isinstance(manifest_evidence, Mapping) or set(manifest_evidence) != {"version", "generation", "digest"} or manifest_evidence.get("version") != 1:
                raise ValueError("extension manifest evidence is malformed")
            if manifest_evidence.get("generation") != extension_generation:
                raise ValueError("extension manifest generation contradicts accepted evidence")
            manifest_digest = _require_digest(
                manifest_evidence.get("digest"),
                field_name="extension manifest digest",
            )
        if trusted_context is not None:
            if principal.identity != trusted_context.identity:
                raise ValueError("trusted context identity contradicts the accepted principal")
            if origin.source_kind != trusted_context.origin.source_kind:
                raise ValueError("trusted context Origin contradicts the accepted source")
            if thread_id != trusted_context.thread_id:
                raise ValueError("trusted context thread contradicts the accepted thread")
            if revision.agent_id != trusted_context.agent_revision.agent_id or revision.digest != trusted_context.agent_revision.digest:
                raise ValueError("trusted context agent revision contradicts accepted evidence")
            if extension_generation != trusted_context.extension_generation:
                raise ValueError("trusted context extension generation contradicts accepted evidence")
            if manifest_digest != trusted_context.extension_manifest_digest:
                raise ValueError("trusted context extension manifest contradicts accepted evidence")

            trusted_base_references: dict[str, Any] = {}
            for reference in trusted_context.origin.references:
                if reference.storage_class != "persistable" or reference.purpose != "correlation" or reference.key in trusted_base_references:
                    raise ValueError("trusted Origin references contradict accepted evidence")
                trusted_base_references[reference.key] = reference.value
            if _canonical_value(trusted_base_references) != _canonical_value(origin.references):
                raise ValueError("trusted Origin references contradict accepted evidence")

            expected_contributor_references: list[dict[str, Any]] = []
            for reference in trusted_context.origin.contributor_references:
                prefix = "origin_contributor:"
                if not reference.capability_id.startswith(prefix):
                    raise ValueError("trusted Origin contributor identity contradicts accepted evidence")
                item = reference.reference
                if item.storage_class != "persistable":
                    continue
                expected_contributor_references.append(
                    {
                        "contribution_id": reference.capability_id.removeprefix(prefix),
                        "namespace": reference.namespace,
                        "key": item.key,
                        "value": item.value,
                        "storage_class": item.storage_class,
                        "purpose": item.purpose,
                    }
                )
            if _canonical_value(expected_contributor_references) != _canonical_value(origin.contributor_references):
                raise ValueError("trusted Origin contributors contradict accepted evidence")

            expected_trusted_origin_digest = canonical_digest(
                {
                    "version": 1,
                    "source_kind": trusted_context.origin.source_kind,
                    "references": [
                        {
                            "key": reference.key,
                            "value": reference.value,
                            "storage_class": reference.storage_class,
                            "purpose": reference.purpose,
                        }
                        for reference in trusted_context.origin.references
                    ],
                    "contributor_references": [reference.to_json() for reference in trusted_context.origin.contributor_references],
                }
            )
            _require_matching_digest(
                trusted_context.origin.digest,
                expected_trusted_origin_digest,
                field_name="trusted Origin digest",
            )
            if "external_key" in row and trusted_context.external_key_reference != row.get("external_key"):
                raise ValueError("trusted context external key contradicts accepted evidence")

        runtime_identity_digest = ""
        contributor_execution_digest = ""
        normalized_input: Any = {}
        execution_options: dict[str, Any] = {}
        kwargs = row.get("kwargs")
        effective_projection = kwargs.get(_EFFECTIVE_EXECUTION_PROJECTION_KEY) if isinstance(kwargs, Mapping) else None
        if effective_projection is not None:
            if not isinstance(effective_projection, Mapping):
                raise ValueError("accepted effective execution projection is malformed")
            projection_fields = set(effective_projection)
            accepted_semantics = effective_projection.get("accepted_digest_semantics")
            expected_projection_fields = set(_EFFECTIVE_EXECUTION_FIELDS_V1)
            if accepted_semantics == _CANONICAL_EXECUTION_SEMANTICS:
                expected_projection_fields.add("accepted_digest_semantics")
            elif "accepted_digest_semantics" in projection_fields:
                raise ValueError("accepted effective execution projection version is unsupported")
            if projection_fields != expected_projection_fields:
                raise ValueError("accepted effective execution projection has unknown or missing fields")
            if row.get("request_digest_version") != _REQUEST_DIGEST_VERSION:
                raise ValueError("accepted effective execution digest version is unsupported")
            _require_matching_digest(
                row.get("request_digest"),
                canonical_effective_execution_digest(effective_projection),
                field_name="effective execution digest",
            )
            expected_identities = {
                "thread_id": thread_id,
                "agent_revision_digest": revision.digest,
                "principal_digest": persisted_principal_digest,
                "base_origin_digest": persisted_base_origin_digest,
                "accepted_context_digest": persisted_context_digest,
                "extension_generation": extension_generation,
            }
            for field_name, expected in expected_identities.items():
                if effective_projection.get(field_name) != expected:
                    raise ValueError(f"accepted effective execution {field_name} contradicts accepted evidence")
            runtime_identity_digest = _require_digest(
                effective_projection.get("runtime_identity_digest"),
                field_name="runtime identity digest",
            )
            contributor_execution_digest = _require_digest(
                effective_projection.get("contributor_execution_digest"),
                field_name="contributor execution digest",
            )
            execution_context = effective_projection.get("execution_context")
            if not isinstance(execution_context, Mapping):
                raise ValueError("accepted effective execution context is malformed")
            context_references = {key: execution_context[key] for key in _ACCEPTED_CONTEXT_REFERENCE_KEYS if key in execution_context}
            expected_context_digest = canonical_digest(
                {
                    "version": _DIGEST_VERSION,
                    "context": context_references,
                    "contributor_execution_digest": contributor_execution_digest,
                    "trusted_context_execution_digest": (None if trusted_context is None else trusted_context.execution_digest),
                }
            )
            _require_matching_digest(
                persisted_context_digest,
                expected_context_digest,
                field_name="accepted context digest",
            )
            if accepted_semantics == _CANONICAL_EXECUTION_SEMANTICS:
                checkpoint = effective_projection.get("checkpoint")
                if not isinstance(checkpoint, Mapping):
                    raise ValueError("accepted effective checkpoint is malformed")
                normalized_input = effective_projection.get("input")
                execution_options = {
                    "multitask_strategy": effective_projection.get(
                        "multitask_strategy",
                    ),
                    "interrupt_before": effective_projection.get(
                        "interrupt_before",
                    ),
                    "interrupt_after": effective_projection.get(
                        "interrupt_after",
                    ),
                    "checkpoint_id": checkpoint.get("checkpoint_id"),
                    "recursion_limit": effective_projection.get(
                        "recursion_limit",
                    ),
                }
                expected_runtime_identity_digest = canonical_digest(
                    {
                        "version": _DIGEST_VERSION,
                        "principal_digest": persisted_principal_digest,
                        "base_origin_digest": persisted_base_origin_digest,
                        "thread_id": thread_id,
                        "agent_revision_digest": revision.digest,
                        "input": effective_projection.get("input"),
                        "execution_options": {
                            "multitask_strategy": effective_projection.get("multitask_strategy"),
                            "interrupt_before": effective_projection.get("interrupt_before"),
                            "interrupt_after": effective_projection.get("interrupt_after"),
                            "checkpoint_id": checkpoint.get("checkpoint_id"),
                            "recursion_limit": effective_projection.get("recursion_limit"),
                        },
                        "extension_generation": extension_generation,
                        "extension_manifest_digest": manifest_digest,
                        "accepted_context_digest": persisted_context_digest,
                    }
                )
                _require_matching_digest(
                    runtime_identity_digest,
                    expected_runtime_identity_digest,
                    field_name="runtime identity digest",
                )
        return cls(
            principal=principal,
            origin=origin,
            thread_id=thread_id,
            context_references={},
            agent_revision=revision,
            normalized_input=normalized_input,
            execution_options=execution_options,
            extension_generation=extension_generation,
            principal_digest=persisted_principal_digest,
            base_origin_digest=persisted_base_origin_digest,
            accepted_context_digest=persisted_context_digest,
            runtime_identity_digest=runtime_identity_digest,
            contributor_execution_digest=contributor_execution_digest,
            trusted_context=trusted_context,
            decision_evidence=decision_evidence,
        )


__all__ = [
    "AcceptedInvocation",
    "InvocationOrigin",
    "INVOCATION_IDENTITY_CONTEXT_KEY",
    "INVOCATION_ORIGIN_CONTEXT_KEY",
    "PrincipalProjection",
    "ResolvedAgentMaterialV1",
    "ResolvedAgentRevision",
    "TRUSTED_RUN_CONTEXT_KEY",
    "canonical_digest",
]
