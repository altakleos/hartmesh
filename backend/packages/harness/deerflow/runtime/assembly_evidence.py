"""Bind an accepted invocation to the lead-agent graph actually assembled.

This module is the only place that knows how an observer-facing
``AgentAssemblyDescriptor`` becomes authoritative durable evidence.  It
validates the descriptor, reconciles the accepted-run anchors, and emits a
small V1 record containing digests rather than prompts, schemas, policy
payloads, or extension data.

The evidence is an execution record.  It is not a signature or a
cryptographic attestation of the Python source that produced the graph.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, MutableMapping, Sequence
from dataclasses import dataclass, fields
from typing import Literal, Self

from deerflow_extension_api import (
    AgentAssemblyDescriptor,
    ConstraintProjectionV1,
    ConstraintProjectionV2,
    MiddlewareDescriptor,
    TenantReferenceV1,
    ToolDescriptor,
)

from deerflow.runtime.accepted_invocation import (
    AcceptedInvocation,
    ResolvedAgentMaterialV1,
    canonical_digest,
)

REQUIRE_ASSEMBLY_EVIDENCE_CONTEXT_KEY = "__deerflow_require_assembly_evidence"

ASSEMBLY_EVIDENCE_VERSION = 3
ASSEMBLY_DESCRIPTOR_VERSION = 1
MAX_ASSEMBLY_IDENTIFIER_BYTES = 128
MAX_ASSEMBLY_EVIDENCE_BYTES = 8 * 1024

_MAX_DESCRIPTOR_BYTES = 128 * 1024
_MAX_DESCRIPTOR_ENTRIES = 512
_MAX_JSON_DEPTH = 16
_HEX_DIGITS = frozenset("0123456789abcdef")

# ``requested_model`` and ``build`` remain useful observer metadata but do not
# identify the graph that reaches the provider.  Everything else participates
# in the canonical descriptor fingerprint.  The summarized set documents
# which fingerprinted fields also feed individual bounded evidence digests.
_FINGERPRINTED_DESCRIPTOR_FIELDS = frozenset(
    {
        "namespace",
        "agent_name",
        "effective_model",
        "model_parameters",
        "thinking_enabled",
        "reasoning_effort",
        "base_prompt_hash",
        "tools",
        "middlewares",
        "deferred_tool_names",
        "enabled_skills",
        "effective_policies",
    }
)
_SUMMARIZED_DESCRIPTOR_FIELDS = frozenset(
    {
        "namespace",
        "agent_name",
        "effective_model",
        "base_prompt_hash",
        "tools",
        "middlewares",
        "enabled_skills",
        "effective_policies",
    }
)
_EXCLUDED_DESCRIPTOR_FIELDS = frozenset({"requested_model", "build"})
_FINGERPRINTED_TOOL_FIELDS = frozenset(
    {
        "name",
        "description_hash",
        "schema_hash",
        "source",
        "mcp_server",
        "mcp_transport",
    }
)
_EXCLUDED_TOOL_FIELDS: frozenset[str] = frozenset()
_FINGERPRINTED_MIDDLEWARE_FIELDS = frozenset(
    {
        "name",
        "module",
        "policy_parameters",
        "extension",
    }
)
_EXCLUDED_MIDDLEWARE_FIELDS: frozenset[str] = frozenset()

_DURABLE_POLICY_FIELDS = (
    "bootstrap",
    "non_interactive",
    "plan_mode",
    "recursion_limit",
    "subagents",
)
_SECRET_KEY_PARTS = frozenset(
    {
        "secret",
        "password",
        "passwd",
        "credential",
        "credentials",
    }
)
_SECRET_KEY_NAMES = frozenset(
    {
        "apikey",
        "api_key",
        "accesstoken",
        "access_token",
        "authtoken",
        "auth_token",
        "clientsecret",
        "client_secret",
        "privatekey",
        "private_key",
    }
)


class AssemblyEvidenceError(RuntimeError):
    """A fail-closed assembly failure safe to report outside the process."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


class _AssemblyEvidenceRequirement:
    __slots__ = ()


_SERVER_OWNED_REQUIREMENT = _AssemblyEvidenceRequirement()


def install_assembly_evidence_requirement(context: MutableMapping[str, object]) -> None:
    """Install the opaque server-owned marker into rebuilt trusted context."""

    context[REQUIRE_ASSEMBLY_EVIDENCE_CONTEXT_KEY] = _SERVER_OWNED_REQUIREMENT


def assembly_evidence_is_required(context: Mapping[str, object]) -> bool:
    """Return true only for the marker created inside this process."""

    return context.get(REQUIRE_ASSEMBLY_EVIDENCE_CONTEXT_KEY) is _SERVER_OWNED_REQUIREMENT


def strip_assembly_evidence_requirement(context: MutableMapping[str, object]) -> None:
    """Remove any client-supplied spelling before trusted context is rebuilt."""

    context.pop(REQUIRE_ASSEMBLY_EVIDENCE_CONTEXT_KEY, None)


def _invalid() -> None:
    raise AssemblyEvidenceError("assembly_descriptor_invalid")


def _is_digest(value: object) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(character in _HEX_DIGITS for character in value)


def _validate_digest(value: object) -> str:
    if not _is_digest(value):
        _invalid()
    return value


def _validate_identifier(value: object, *, allow_empty: bool = False) -> str:
    if not isinstance(value, str) or (not value and not allow_empty):
        _invalid()
    try:
        encoded = value.encode("utf-8")
    except UnicodeError:
        _invalid()
    if len(encoded) > MAX_ASSEMBLY_IDENTIFIER_BYTES or any(ord(character) < 32 or ord(character) == 127 for character in value):
        _invalid()
    return value


def _secret_shaped_key(value: str) -> bool:
    lowered = value.lower()
    if lowered in _SECRET_KEY_NAMES:
        return True
    normalized_parts = tuple(part for part in lowered.replace("-", "_").split("_") if part)
    return any(part in _SECRET_KEY_PARTS for part in normalized_parts)


def _validate_plain_json(value: object, *, depth: int = 0, reject_secret_keys: bool = True) -> object:
    if depth > _MAX_JSON_DEPTH:
        _invalid()
    if value is None or isinstance(value, bool | int | str):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            _invalid()
        return value
    if isinstance(value, Mapping):
        if len(value) > _MAX_DESCRIPTOR_ENTRIES:
            _invalid()
        projected: dict[str, object] = {}
        for key, child in value.items():
            if not isinstance(key, str) or (reject_secret_keys and _secret_shaped_key(key)):
                _invalid()
            projected[key] = _validate_plain_json(child, depth=depth + 1, reject_secret_keys=reject_secret_keys)
        return projected
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        if len(value) > _MAX_DESCRIPTOR_ENTRIES:
            _invalid()
        return [_validate_plain_json(child, depth=depth + 1, reject_secret_keys=reject_secret_keys) for child in value]
    _invalid()


def _canonical_size(value: object) -> int:
    from deerflow_extension_api import canonical_json

    try:
        return len(canonical_json(value).encode("utf-8"))
    except (TypeError, ValueError, UnicodeError):
        _invalid()


def assert_descriptor_projection_complete() -> None:
    """Fail when a descriptor field has no deliberate evidence treatment."""

    classifications = (
        (
            AgentAssemblyDescriptor,
            _FINGERPRINTED_DESCRIPTOR_FIELDS,
            _EXCLUDED_DESCRIPTOR_FIELDS,
        ),
        (
            ToolDescriptor,
            _FINGERPRINTED_TOOL_FIELDS,
            _EXCLUDED_TOOL_FIELDS,
        ),
        (
            MiddlewareDescriptor,
            _FINGERPRINTED_MIDDLEWARE_FIELDS,
            _EXCLUDED_MIDDLEWARE_FIELDS,
        ),
    )
    for descriptor_type, fingerprinted, excluded in classifications:
        actual = {field.name for field in fields(descriptor_type)}
        classified = fingerprinted | excluded
        if actual != classified:
            missing = sorted(actual - classified)
            stale = sorted(classified - actual)
            raise AssertionError(f"{descriptor_type.__name__} evidence classification drifted; missing={missing}, stale={stale}")
    if not _SUMMARIZED_DESCRIPTOR_FIELDS <= _FINGERPRINTED_DESCRIPTOR_FIELDS:
        raise AssertionError("summarized descriptor fields must also be fingerprinted")


def _tool_projection(tool: ToolDescriptor) -> dict[str, object]:
    if type(tool) is not ToolDescriptor:
        _invalid()
    return {
        "name": _validate_identifier(tool.name),
        "description_hash": _validate_digest(tool.description_hash),
        "schema_hash": _validate_digest(tool.schema_hash),
        "source": _validate_identifier(tool.source),
        "mcp_server": None if tool.mcp_server is None else _validate_identifier(tool.mcp_server),
        "mcp_transport": None if tool.mcp_transport is None else _validate_identifier(tool.mcp_transport),
    }


def _middleware_projection(middleware: MiddlewareDescriptor) -> dict[str, object]:
    if type(middleware) is not MiddlewareDescriptor:
        _invalid()
    return {
        "name": _validate_identifier(middleware.name),
        "module": _validate_identifier(middleware.module),
        "extension": None if middleware.extension is None else _validate_identifier(middleware.extension),
        "policy_parameters": _validate_plain_json(middleware.policy_parameters),
    }


def _validated_descriptor_projection(
    descriptor: AgentAssemblyDescriptor,
) -> dict[str, object]:
    if type(descriptor) is not AgentAssemblyDescriptor:
        _invalid()
    assert_descriptor_projection_complete()

    if len(descriptor.tools) > _MAX_DESCRIPTOR_ENTRIES or len(descriptor.middlewares) > _MAX_DESCRIPTOR_ENTRIES:
        _invalid()
    if len(descriptor.deferred_tool_names) > _MAX_DESCRIPTOR_ENTRIES or len(descriptor.enabled_skills) > _MAX_DESCRIPTOR_ENTRIES:
        _invalid()

    tools = [_tool_projection(tool) for tool in descriptor.tools]
    tool_names = [tool["name"] for tool in tools]
    if len(tool_names) != len(set(tool_names)):
        _invalid()

    middlewares = [_middleware_projection(middleware) for middleware in descriptor.middlewares]
    middleware_identities = [canonical_digest(middleware) for middleware in middlewares]
    if len(middleware_identities) != len(set(middleware_identities)):
        _invalid()

    deferred_names = [_validate_identifier(name) for name in descriptor.deferred_tool_names]
    skill_names = [_validate_identifier(name) for name in descriptor.enabled_skills]
    if len(deferred_names) != len(set(deferred_names)) or len(skill_names) != len(set(skill_names)):
        _invalid()

    projection = {
        "namespace": _validate_identifier(descriptor.namespace),
        "agent_name": _validate_identifier(descriptor.agent_name),
        "effective_model": _validate_identifier(descriptor.effective_model),
        "model_parameters": _validate_plain_json(descriptor.model_parameters),
        "thinking_enabled": descriptor.thinking_enabled,
        "reasoning_effort": _validate_plain_json(descriptor.reasoning_effort),
        "base_prompt_hash": _validate_digest(descriptor.base_prompt_hash),
        "tools": sorted(tools, key=lambda entry: str(entry["name"])),
        "middlewares": middlewares,
        "deferred_tool_names": sorted(deferred_names),
        "enabled_skills": sorted(skill_names),
        "effective_policies": _validate_plain_json(descriptor.effective_policies),
    }
    if type(descriptor.thinking_enabled) is not bool or _canonical_size(projection) > _MAX_DESCRIPTOR_BYTES:
        _invalid()
    return projection


def canonical_skillset_digest(skill_names: Sequence[str], *, catalog_digest: str) -> str:
    """Canonicalize accepted skill names plus their immutable catalog digest."""

    names = [_validate_identifier(name) for name in skill_names]
    if len(names) > _MAX_DESCRIPTOR_ENTRIES or len(names) != len(set(names)):
        _invalid()
    return canonical_digest(
        {
            "enabled_skills": sorted(names),
            "skill_catalog_hash": _validate_digest(catalog_digest),
        }
    )


def canonical_durable_policy_digest(effective_policies: Mapping[str, object]) -> str:
    """Digest the accepted-run policy anchors independently knowable at admission."""

    if not isinstance(effective_policies, Mapping) or any(field not in effective_policies for field in _DURABLE_POLICY_FIELDS):
        _invalid()
    projection = {field: _validate_plain_json(effective_policies[field]) for field in _DURABLE_POLICY_FIELDS}
    return canonical_digest(projection)


@dataclass(frozen=True)
class AcceptedAssemblyAnchors:
    """Immutable accepted-run facts an actual descriptor must satisfy."""

    run_id: str
    expected_namespace: str
    expected_agent_name: str
    expected_effective_model: str
    expected_skillset_digest: str
    expected_policy_digest: str
    agent_revision_digest: str
    extension_generation: int
    capability_manifest_digest: str | None = None
    artifact_manifest_digest: str | None = None
    extension_configuration_digest: str | None = None
    tenant: TenantReferenceV1 | None = None

    def __post_init__(self) -> None:
        _validate_identifier(self.run_id)
        _validate_identifier(self.expected_namespace)
        _validate_identifier(self.expected_agent_name)
        _validate_identifier(self.expected_effective_model)
        _validate_digest(self.expected_skillset_digest)
        _validate_digest(self.expected_policy_digest)
        _validate_digest(self.agent_revision_digest)
        if type(self.extension_generation) is not int or self.extension_generation < 0:
            _invalid()
        if self.tenant is not None and not isinstance(self.tenant, TenantReferenceV1):
            _invalid()
        if self.capability_manifest_digest is not None:
            _validate_digest(self.capability_manifest_digest)
        for digest in (
            self.artifact_manifest_digest,
            self.extension_configuration_digest,
        ):
            if digest is not None and (not digest.startswith("sha256:") or not _is_digest(digest.removeprefix("sha256:"))):
                _invalid()
        if (self.artifact_manifest_digest is None) != (self.extension_configuration_digest is None):
            _invalid()
        if self.artifact_manifest_digest is not None and self.capability_manifest_digest is None:
            _invalid()


def build_accepted_assembly_anchors(
    *,
    run_id: str,
    accepted: AcceptedInvocation,
    material: ResolvedAgentMaterialV1,
    app_config: object | None,
    accepted_constraints: ConstraintProjectionV1 | ConstraintProjectionV2 | None = None,
) -> AcceptedAssemblyAnchors:
    """Derive authoritative descriptor expectations from accepted material."""

    from deerflow.agents.assembly_descriptor import (
        skill_catalog_digest_from_snapshot,
        subagent_release_policy,
    )

    defaults = material.runtime_defaults
    is_bootstrap = bool(defaults.get("is_bootstrap", False))
    agent_name = "bootstrap" if is_bootstrap else str(defaults.get("agent_name") or "lead-agent")
    effective_model = material.model_profile.get("name")
    if not isinstance(effective_model, str):
        effective_model = ""

    enabled_skills = list(material.enabled_skill_objects)
    if is_bootstrap:
        enabled_skills = [skill for skill in enabled_skills if getattr(skill, "name", None) == "bootstrap"]
    skill_names = tuple(str(getattr(skill, "name", "")) for skill in enabled_skills)
    expected_skillset_digest = canonical_skillset_digest(
        skill_names,
        catalog_digest=skill_catalog_digest_from_snapshot(
            enabled_skills,
            material.skills,
        ),
    )

    requested_subagents = bool(defaults.get("subagent_enabled", False))
    allowed_subagents = getattr(
        material.agent_config_object,
        "allowed_subagents",
        None,
    )
    max_total = int(defaults.get("max_total_subagents", 6))
    if accepted_constraints is not None and accepted_constraints.max_total_subagents is not None:
        max_total = accepted_constraints.max_total_subagents
    durable_policies = {
        "bootstrap": is_bootstrap,
        "non_interactive": bool(defaults.get("non_interactive", False)),
        "plan_mode": bool(defaults.get("is_plan_mode", False)),
        "recursion_limit": accepted.execution_options.get(
            "recursion_limit",
            "framework-default",
        ),
        "subagents": subagent_release_policy(
            material.app_config or app_config,
            enabled=requested_subagents and allowed_subagents != [],
            max_concurrent=int(
                defaults.get("max_concurrent_subagents", 3),
            ),
            max_total=max_total,
            resolved_subagent_catalog=material.subagent_catalog,
        ),
    }
    return AcceptedAssemblyAnchors(
        run_id=run_id,
        expected_namespace="deerflow",
        expected_agent_name=agent_name,
        expected_effective_model=effective_model,
        expected_skillset_digest=expected_skillset_digest,
        expected_policy_digest=canonical_durable_policy_digest(
            durable_policies,
        ),
        agent_revision_digest=accepted.agent_revision.digest,
        extension_generation=accepted.extension_generation,
        capability_manifest_digest=accepted.extension_manifest_digest,
        artifact_manifest_digest=accepted.extension_artifact_manifest_digest,
        extension_configuration_digest=accepted.extension_configuration_digest,
        tenant=accepted.tenant,
    )


@dataclass(frozen=True)
class AssemblyEvidenceV1:
    """Bounded persisted proof of one validated lead-agent assembly."""

    version: Literal[1, 2, 3]
    fingerprint: str
    descriptor_version: int
    namespace: str
    agent_name: str
    effective_model: str
    prompt_digest: str
    toolset_digest: str
    middleware_digest: str
    skillset_digest: str
    policy_digest: str
    accepted_agent_revision_digest: str
    extension_generation: int
    accepted_capability_manifest_digest: str | None = None
    accepted_artifact_manifest_digest: str | None = None
    accepted_extension_configuration_digest: str | None = None
    tenant: TenantReferenceV1 | None = None

    def __post_init__(self) -> None:
        if self.version not in (1, 2, ASSEMBLY_EVIDENCE_VERSION) or type(self.version) is not int:
            _invalid()
        if self.descriptor_version != ASSEMBLY_DESCRIPTOR_VERSION or type(self.descriptor_version) is not int:
            _invalid()
        for digest in (
            self.fingerprint,
            self.prompt_digest,
            self.toolset_digest,
            self.middleware_digest,
            self.skillset_digest,
            self.policy_digest,
            self.accepted_agent_revision_digest,
        ):
            _validate_digest(digest)
        _validate_identifier(self.namespace)
        _validate_identifier(self.agent_name)
        _validate_identifier(self.effective_model)
        if type(self.extension_generation) is not int or self.extension_generation < 0:
            _invalid()
        if self.version == 1:
            if self.tenant is not None:
                _invalid()
        elif not isinstance(self.tenant, TenantReferenceV1):
            _invalid()
        if self.version < 3:
            if any(
                digest is not None
                for digest in (
                    self.accepted_capability_manifest_digest,
                    self.accepted_artifact_manifest_digest,
                    self.accepted_extension_configuration_digest,
                )
            ):
                _invalid()
        else:
            _validate_digest(self.accepted_capability_manifest_digest)
            for digest in (
                self.accepted_artifact_manifest_digest,
                self.accepted_extension_configuration_digest,
            ):
                if not isinstance(digest, str) or not digest.startswith("sha256:"):
                    _invalid()
                _validate_digest(digest.removeprefix("sha256:"))
        if _canonical_size(self._projection()) > MAX_ASSEMBLY_EVIDENCE_BYTES:
            _invalid()

    def _projection(self) -> dict[str, object]:
        projection: dict[str, object] = {
            "version": self.version,
            "fingerprint": self.fingerprint,
            "descriptor_version": self.descriptor_version,
            "namespace": self.namespace,
            "agent_name": self.agent_name,
            "effective_model": self.effective_model,
            "prompt_digest": self.prompt_digest,
            "toolset_digest": self.toolset_digest,
            "middleware_digest": self.middleware_digest,
            "skillset_digest": self.skillset_digest,
            "policy_digest": self.policy_digest,
            "accepted_agent_revision_digest": self.accepted_agent_revision_digest,
            "extension_generation": self.extension_generation,
        }
        if self.version >= 2:
            assert self.tenant is not None
            projection["tenant_ref"] = self.tenant.public_ref
            projection["tenant_digest"] = self.tenant.digest
        if self.version >= 3:
            projection["accepted_capability_manifest_digest"] = self.accepted_capability_manifest_digest
            projection["accepted_artifact_manifest_digest"] = self.accepted_artifact_manifest_digest
            projection["accepted_extension_configuration_digest"] = self.accepted_extension_configuration_digest
        return projection

    def to_persisted_json(self) -> dict[str, object]:
        return self._projection()

    @classmethod
    def from_persisted_json(cls, value: Mapping[str, object]) -> Self:
        if not isinstance(value, Mapping):
            _invalid()
        version = value.get("version")
        expected = {
            "version",
            "fingerprint",
            "descriptor_version",
            "namespace",
            "agent_name",
            "effective_model",
            "prompt_digest",
            "toolset_digest",
            "middleware_digest",
            "skillset_digest",
            "policy_digest",
            "accepted_agent_revision_digest",
            "extension_generation",
        }
        if version in {2, ASSEMBLY_EVIDENCE_VERSION}:
            expected |= {"tenant_ref", "tenant_digest"}
        if version == ASSEMBLY_EVIDENCE_VERSION:
            expected |= {
                "accepted_capability_manifest_digest",
                "accepted_artifact_manifest_digest",
                "accepted_extension_configuration_digest",
            }
        if set(value) != expected:
            _invalid()
        try:
            fields = dict(value)
            tenant = None
            if version in {2, ASSEMBLY_EVIDENCE_VERSION}:
                tenant = TenantReferenceV1(
                    version=1,
                    public_ref=fields.pop("tenant_ref"),  # type: ignore[arg-type]
                    digest=fields.pop("tenant_digest"),  # type: ignore[arg-type]
                )
            return cls(**fields, tenant=tenant)  # type: ignore[arg-type]
        except AssemblyEvidenceError:
            raise
        except (TypeError, ValueError):
            _invalid()


def assembly_evidence_digest(value: Mapping[str, object] | AssemblyEvidenceV1) -> str:
    """Digest a validated V1 persisted record for store-level comparison."""

    evidence = value if isinstance(value, AssemblyEvidenceV1) else AssemblyEvidenceV1.from_persisted_json(value)
    return canonical_digest(evidence.to_persisted_json())


def build_assembly_evidence(
    descriptor: AgentAssemblyDescriptor,
    *,
    anchors: AcceptedAssemblyAnchors,
) -> AssemblyEvidenceV1:
    """Validate an actual assembly against accepted facts and summarize it."""

    if not isinstance(anchors, AcceptedAssemblyAnchors):
        _invalid()
    projection = _validated_descriptor_projection(descriptor)

    if projection["namespace"] != anchors.expected_namespace or projection["agent_name"] != anchors.expected_agent_name:
        raise AssemblyEvidenceError("assembly_agent_mismatch")
    if projection["effective_model"] != anchors.expected_effective_model:
        raise AssemblyEvidenceError("assembly_model_mismatch")

    policies = projection["effective_policies"]
    if not isinstance(policies, Mapping):
        _invalid()
    catalog_digest = policies.get("skill_catalog_hash")
    skillset_digest = canonical_skillset_digest(descriptor.enabled_skills, catalog_digest=catalog_digest)
    if skillset_digest != anchors.expected_skillset_digest:
        raise AssemblyEvidenceError("assembly_skill_mismatch")

    policy_anchor_digest = canonical_durable_policy_digest(policies)
    if policy_anchor_digest != anchors.expected_policy_digest:
        raise AssemblyEvidenceError("assembly_policy_mismatch")

    toolset_projection = {
        "tools": projection["tools"],
        "deferred_tool_names": projection["deferred_tool_names"],
    }
    return AssemblyEvidenceV1(
        version=(ASSEMBLY_EVIDENCE_VERSION if anchors.artifact_manifest_digest is not None else (2 if anchors.tenant is not None else 1)),
        fingerprint=canonical_digest(projection),
        descriptor_version=ASSEMBLY_DESCRIPTOR_VERSION,
        namespace=descriptor.namespace,
        agent_name=descriptor.agent_name,
        effective_model=descriptor.effective_model,
        prompt_digest=descriptor.base_prompt_hash,
        toolset_digest=canonical_digest(toolset_projection),
        middleware_digest=canonical_digest(projection["middlewares"]),
        skillset_digest=skillset_digest,
        policy_digest=canonical_digest(policies),
        accepted_agent_revision_digest=anchors.agent_revision_digest,
        extension_generation=anchors.extension_generation,
        accepted_capability_manifest_digest=anchors.capability_manifest_digest,
        accepted_artifact_manifest_digest=anchors.artifact_manifest_digest,
        accepted_extension_configuration_digest=anchors.extension_configuration_digest,
        tenant=anchors.tenant,
    )


def verify_bound_assembly(
    actual: AssemblyEvidenceV1,
    *,
    persisted: AssemblyEvidenceV1,
) -> None:
    """Require exact V1 evidence equality for a continuation or recovery."""

    if not isinstance(actual, AssemblyEvidenceV1) or not isinstance(persisted, AssemblyEvidenceV1) or actual.to_persisted_json() != persisted.to_persisted_json():
        raise AssemblyEvidenceError("assembly_evidence_mismatch")


def assembly_evidence_binding_matches(
    actual: AssemblyEvidenceV1,
    *,
    actual_digest: str,
    persisted_json: Mapping[str, object],
    persisted_digest: str,
) -> bool:
    """Return whether persisted bytes are the same valid evidence binding."""

    if assembly_evidence_digest(actual) != actual_digest:
        return False
    try:
        persisted = AssemblyEvidenceV1.from_persisted_json(persisted_json)
        verify_bound_assembly(actual, persisted=persisted)
    except AssemblyEvidenceError:
        return False
    return persisted_digest == actual_digest and assembly_evidence_digest(persisted) == persisted_digest


__all__ = [
    "ASSEMBLY_DESCRIPTOR_VERSION",
    "ASSEMBLY_EVIDENCE_VERSION",
    "MAX_ASSEMBLY_EVIDENCE_BYTES",
    "REQUIRE_ASSEMBLY_EVIDENCE_CONTEXT_KEY",
    "AcceptedAssemblyAnchors",
    "AssemblyEvidenceError",
    "AssemblyEvidenceV1",
    "assembly_evidence_binding_matches",
    "assembly_evidence_digest",
    "assembly_evidence_is_required",
    "assert_descriptor_projection_complete",
    "build_accepted_assembly_anchors",
    "build_assembly_evidence",
    "canonical_durable_policy_digest",
    "canonical_skillset_digest",
    "install_assembly_evidence_requirement",
    "strip_assembly_evidence_requirement",
    "verify_bound_assembly",
]
