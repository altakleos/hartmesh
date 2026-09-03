"""Host-independent invocation contributor contracts.

Contributors receive narrow, immutable host projections and may return only
bounded scalar references in their own namespace.  Credentials and arbitrary
host objects never cross this contract boundary.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from types import MappingProxyType
from typing import Literal, Protocol, runtime_checkable

from deerflow_extension_api.credentials import (
    CredentialEvidenceV1,
    VerifiedActorContextV1,
)
from deerflow_extension_api.health import CapabilityHealthProbe
from deerflow_extension_api.identifiers import (
    canonicalize_agent_identifier,
    validate_model_profile_identifier,
    validate_thread_identifier,
)
from deerflow_extension_api.identity import InvocationIdentityV1
from deerflow_extension_api.tenant import TenantReferenceV1

ORIGIN_CONTRIBUTOR_CAPABILITY_API_VERSION = "1.0"
RUN_CONTEXT_CONTRIBUTOR_CAPABILITY_API_VERSION = "1.0"
ORIGIN_CONTRIBUTOR_KIND = "origin_contributor"
RUN_CONTEXT_CONTRIBUTOR_KIND = "run_context_contributor"

type StorageClass = Literal["persistable", "runtime_only"]
type ReferencePurpose = Literal["execution", "correlation", "secret_handle"]
type SafeScalarV1 = str | int | bool | None
type SafeValueV1 = SafeScalarV1 | tuple[SafeScalarV1, ...] | list[SafeScalarV1]

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_.-]{0,63}$", re.ASCII)
_MAX_REFERENCES = 32
_MAX_STRING_BYTES = 1024
_MAX_CANONICAL_BYTES = 8192
_MAX_TRUSTED_CONTEXT_BYTES = 32768
_DIGEST = re.compile(r"^[0-9a-f]{64}$", re.ASCII)


def _validate_contributor_identifier(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or _IDENTIFIER.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be a 1-64 character ASCII identifier")
    return value


def _validate_scalar(value: object) -> None:
    # bool must be checked before int because it is an int subclass.
    if value is None or isinstance(value, bool) or isinstance(value, int):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("safe context references reject non-finite numbers")
        raise TypeError("safe context references accept integers, not floating-point values")
    if isinstance(value, str):
        if len(value.encode("utf-8")) > _MAX_STRING_BYTES:
            raise ValueError("safe context reference strings are limited to 1 KiB UTF-8")
        return
    raise TypeError("safe context references accept only strings, integers, booleans, null, or lists of those values")


@dataclass(frozen=True)
class SafeContextReferenceV1:
    key: str
    value: SafeValueV1
    storage_class: StorageClass
    purpose: ReferencePurpose

    def __post_init__(self) -> None:
        _validate_contributor_identifier(self.key, field_name="reference key")
        if self.storage_class not in ("persistable", "runtime_only"):
            raise ValueError("storage_class must be 'persistable' or 'runtime_only'")
        if self.purpose not in ("execution", "correlation", "secret_handle"):
            raise ValueError("purpose must be 'execution', 'correlation', or 'secret_handle'")
        value = self.value
        if isinstance(value, (list, tuple)):
            for item in value:
                _validate_scalar(item)
            # Freeze caller-owned lists so post-validation mutation cannot alter
            # an accepted contribution or its digest.
            object.__setattr__(self, "value", tuple(value))
        else:
            _validate_scalar(value)
        if self.purpose == "secret_handle" and not isinstance(self.value, str):
            raise TypeError("a secret_handle reference must contain one stable string identifier")
        if self.purpose == "secret_handle" and any(ord(character) < 32 or ord(character) == 127 for character in self.value):
            raise ValueError("a secret_handle identifier must not contain control characters")


def _reference_json(reference: SafeContextReferenceV1) -> dict[str, object]:
    return {
        "key": reference.key,
        "value": list(reference.value) if isinstance(reference.value, tuple) else reference.value,
        "storage_class": reference.storage_class,
        "purpose": reference.purpose,
    }


def _reference_from_json(value: object) -> SafeContextReferenceV1:
    if not isinstance(value, dict) or set(value) != {
        "key",
        "value",
        "storage_class",
        "purpose",
    }:
        raise ValueError("safe context reference has unknown or missing fields")
    return SafeContextReferenceV1(
        key=value["key"],  # type: ignore[arg-type]
        value=value["value"],  # type: ignore[arg-type]
        storage_class=value["storage_class"],  # type: ignore[arg-type]
        purpose=value["purpose"],  # type: ignore[arg-type]
    )


def _validate_contribution(namespace: str, references: tuple[SafeContextReferenceV1, ...]) -> None:
    _validate_contributor_identifier(namespace, field_name="contribution namespace")
    if len(references) > _MAX_REFERENCES:
        raise ValueError("a contributor may return at most 32 references")
    seen: set[str] = set()
    for reference in references:
        if not isinstance(reference, SafeContextReferenceV1):
            raise TypeError("contribution references must be SafeContextReferenceV1 values")
        if reference.key in seen:
            raise ValueError(f"duplicate reference key {reference.key!r}")
        seen.add(reference.key)
    canonical = json.dumps(
        {
            "namespace": namespace,
            "references": [
                {
                    "key": reference.key,
                    "purpose": reference.purpose,
                    "storage_class": reference.storage_class,
                    "value": reference.value,
                }
                for reference in references
            ],
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    if len(canonical) > _MAX_CANONICAL_BYTES:
        raise ValueError("a canonical contributor result is limited to 8 KiB")


@dataclass(frozen=True)
class OriginContributionRequestV1:
    source_kind: str
    authenticated_subject_reference: str | None = None
    source_references: tuple[SafeContextReferenceV1, ...] = ()
    identity: InvocationIdentityV1 | None = None
    tenant: TenantReferenceV1 | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "source_references", tuple(self.source_references))
        if self.identity is not None and not isinstance(self.identity, InvocationIdentityV1):
            raise TypeError("identity must be InvocationIdentityV1 or None")
        if self.tenant is not None and not isinstance(self.tenant, TenantReferenceV1):
            raise TypeError("tenant must be TenantReferenceV1 or None")


@dataclass(frozen=True)
class OriginContributionV1:
    namespace: str
    references: tuple[SafeContextReferenceV1, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "references", tuple(self.references))
        _validate_contribution(self.namespace, self.references)


@dataclass(frozen=True)
class PrincipalProjectionV1:
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


@dataclass(frozen=True)
class SealedOriginV1:
    source_kind: str
    references: tuple[SafeContextReferenceV1, ...] = ()
    digest: str = ""
    contributor_references: tuple[NamespacedContextReferenceV1, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "references", tuple(self.references))
        object.__setattr__(self, "contributor_references", tuple(self.contributor_references))
        if not isinstance(self.source_kind, str) or not self.source_kind or len(self.source_kind.encode("utf-8")) > 64:
            raise ValueError("Origin source_kind must be a non-empty string of at most 64 UTF-8 bytes")
        if self.digest and _DIGEST.fullmatch(self.digest) is None:
            raise ValueError("Origin digest must be a lowercase SHA-256 digest")
        for reference in self.references:
            if not isinstance(reference, SafeContextReferenceV1):
                raise TypeError("Origin references must contain SafeContextReferenceV1 values")
        for reference in self.contributor_references:
            if not isinstance(reference, NamespacedContextReferenceV1):
                raise TypeError("Origin contributor_references must contain NamespacedContextReferenceV1 values")


@dataclass(frozen=True)
class NamespacedContextReferenceV1:
    """One host-attributed contributor reference with a stable full name."""

    capability_id: str
    namespace: str
    reference: SafeContextReferenceV1

    def __post_init__(self) -> None:
        if not isinstance(self.capability_id, str) or not self.capability_id or len(self.capability_id.encode("utf-8")) > 160:
            raise ValueError("capability_id must be a non-empty string of at most 160 UTF-8 bytes")
        _validate_contributor_identifier(self.namespace, field_name="reference namespace")
        if not isinstance(self.reference, SafeContextReferenceV1):
            raise TypeError("reference must be SafeContextReferenceV1")

    @property
    def fully_qualified_key(self) -> str:
        return f"{self.namespace}.{self.reference.key}"

    def to_json(self) -> dict[str, object]:
        return {
            "capability_id": self.capability_id,
            "namespace": self.namespace,
            "reference": _reference_json(self.reference),
        }

    @classmethod
    def from_json(cls, value: object) -> NamespacedContextReferenceV1:
        if not isinstance(value, dict) or set(value) != {
            "capability_id",
            "namespace",
            "reference",
        }:
            raise ValueError("namespaced context reference has unknown or missing fields")
        return cls(
            capability_id=value["capability_id"],  # type: ignore[arg-type]
            namespace=value["namespace"],  # type: ignore[arg-type]
            reference=_reference_from_json(value["reference"]),
        )


@dataclass(frozen=True)
class ResolvedAgentRevisionReferenceV1:
    agent_id: str
    digest: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "agent_id",
            canonicalize_agent_identifier(self.agent_id, field_name="resolved agent id"),
        )
        if _DIGEST.fullmatch(self.digest) is None:
            raise ValueError("resolved agent digest must be a lowercase SHA-256 digest")


@dataclass(frozen=True)
class ResolvedProfileRevisionReferenceV1:
    profile_id: str
    digest: str

    def __post_init__(self) -> None:
        validate_model_profile_identifier(self.profile_id, field_name="resolved model profile identifier")
        if _DIGEST.fullmatch(self.digest) is None:
            raise ValueError("resolved profile digest must be a lowercase SHA-256 digest")


def _trusted_reference_projection(
    items: tuple[NamespacedContextReferenceV1, ...],
) -> list[dict[str, object]]:
    return [item.to_json() for item in items]


@dataclass(frozen=True)
class TrustedRunContextV1:
    """Finite host-sealed facts shared by policy and execution boundaries.

    Runtime-only values are available only on the accepting process. The
    persisted projection retains their digest/count but never their values.
    """

    identity: InvocationIdentityV1
    origin: SealedOriginV1
    thread_id: str
    external_key_reference: str | None
    agent_revision: ResolvedAgentRevisionReferenceV1
    profile_revision: ResolvedProfileRevisionReferenceV1
    extension_generation: int
    extension_manifest_digest: str | None
    extension_artifact_manifest_digest: str | None = None
    extension_configuration_digest: str | None = None
    tenant: TenantReferenceV1 | None = None
    persistable_references: tuple[NamespacedContextReferenceV1, ...] = ()
    runtime_only_references: tuple[NamespacedContextReferenceV1, ...] = ()
    secret_handles: tuple[NamespacedContextReferenceV1, ...] = ()
    run_id: str | None = None
    runtime_reference_digest: str = ""
    runtime_reference_count: int = 0
    runtime_state_complete: bool = True
    # Appended after every v1-v3 constructor slot so adding v4 evidence is
    # backward compatible for callers using optional positional arguments.
    credential: CredentialEvidenceV1 | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.identity, InvocationIdentityV1):
            raise TypeError("trusted run context identity must be InvocationIdentityV1")
        if self.credential is not None and not isinstance(self.credential, CredentialEvidenceV1):
            raise TypeError("trusted run context credential must be CredentialEvidenceV1 or None")
        if self.tenant is not None and not isinstance(self.tenant, TenantReferenceV1):
            raise TypeError("trusted run context tenant must be TenantReferenceV1 or None")
        if self.credential is not None and self.tenant is None:
            raise ValueError("credential-bound trusted run context requires a tenant reference")
        if self.credential is not None and self.tenant is not None:
            VerifiedActorContextV1(
                identity=self.identity,
                credential=self.credential,
                tenant=self.tenant,
            )
        if not isinstance(self.origin, SealedOriginV1):
            raise TypeError("trusted run context origin must be SealedOriginV1")
        validate_thread_identifier(self.thread_id, field_name="thread_id")
        for field_name, value, limit in (
            ("external_key_reference", self.external_key_reference, 384),
            ("run_id", self.run_id, 128),
        ):
            if value is None:
                continue
            if not isinstance(value, str) or not value or len(value.encode("utf-8")) > limit or any(ord(character) < 32 or ord(character) == 127 for character in value):
                raise ValueError(f"{field_name} must be a bounded non-empty string")
        if not isinstance(self.agent_revision, ResolvedAgentRevisionReferenceV1):
            raise TypeError("agent_revision must be ResolvedAgentRevisionReferenceV1")
        if not isinstance(self.profile_revision, ResolvedProfileRevisionReferenceV1):
            raise TypeError("profile_revision must be ResolvedProfileRevisionReferenceV1")
        if type(self.extension_generation) is not int or self.extension_generation < 0:
            raise ValueError("extension_generation must be a non-negative integer")
        if self.extension_manifest_digest is not None and _DIGEST.fullmatch(self.extension_manifest_digest) is None:
            raise ValueError("extension_manifest_digest must be a lowercase SHA-256 digest")
        for field_name in (
            "extension_artifact_manifest_digest",
            "extension_configuration_digest",
        ):
            digest = getattr(self, field_name)
            if digest is not None and re.fullmatch(r"sha256:[0-9a-f]{64}", digest) is None:
                raise ValueError(f"{field_name} must use sha256:<64 lowercase hex>")
        if (self.extension_artifact_manifest_digest is None) != (self.extension_configuration_digest is None):
            raise ValueError("extension artifact and configuration digests must be supplied together")
        if self.extension_artifact_manifest_digest is not None and self.tenant is None:
            raise ValueError("artifact-bound trusted run context requires a tenant reference")
        if self.extension_artifact_manifest_digest is not None and self.extension_manifest_digest is None:
            raise ValueError("artifact-bound trusted run context requires a capability manifest digest")

        for name in (
            "persistable_references",
            "runtime_only_references",
            "secret_handles",
        ):
            items = tuple(getattr(self, name))
            object.__setattr__(self, name, items)
            if any(not isinstance(item, NamespacedContextReferenceV1) for item in items):
                raise TypeError(f"{name} must contain NamespacedContextReferenceV1 values")
        if any(item.reference.storage_class != "persistable" or item.reference.purpose == "secret_handle" for item in self.persistable_references):
            raise ValueError("persistable_references must be non-handle persistable references")
        if any(item.reference.storage_class != "runtime_only" or item.reference.purpose != "execution" for item in self.runtime_only_references):
            raise ValueError("runtime_only_references must be runtime-only execution references")
        if any(item.reference.purpose != "secret_handle" for item in self.secret_handles):
            raise ValueError("secret_handles must contain only stable secret-handle references")

        all_items = (
            *self.persistable_references,
            *self.runtime_only_references,
            *self.secret_handles,
        )
        if len(all_items) > _MAX_REFERENCES:
            raise ValueError("trusted run context accepts at most 32 aggregate references")
        keys = [item.fully_qualified_key for item in all_items]
        if len(keys) != len(set(keys)):
            raise ValueError("trusted run context rejects duplicate fully qualified keys")
        if any(item not in all_items for item in self.origin.contributor_references):
            raise ValueError("Origin contributor references must belong to the trusted run-context products")
        aggregate_references = json.dumps(
            {"version": 1, "references": _trusted_reference_projection(all_items)},
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        if len(aggregate_references) > _MAX_CANONICAL_BYTES:
            raise ValueError("trusted run context aggregate references are limited to 8 KiB")
        runtime_items = tuple(item for item in (*self.runtime_only_references, *self.secret_handles) if item.reference.storage_class == "runtime_only")
        computed_runtime_digest = hashlib.sha256(
            json.dumps(
                {
                    "version": 1,
                    "references": _trusted_reference_projection(runtime_items),
                },
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
        ).hexdigest()
        if self.runtime_reference_digest:
            if _DIGEST.fullmatch(self.runtime_reference_digest) is None:
                raise ValueError("runtime_reference_digest must be a lowercase SHA-256 digest")
            if self.runtime_state_complete and self.runtime_reference_digest != computed_runtime_digest:
                raise ValueError("runtime_reference_digest must match the accepted runtime-only references")
        else:
            object.__setattr__(self, "runtime_reference_digest", computed_runtime_digest)
        if self.runtime_reference_count == 0 and runtime_items:
            object.__setattr__(self, "runtime_reference_count", len(runtime_items))
        if type(self.runtime_reference_count) is not int or self.runtime_reference_count < 0:
            raise ValueError("runtime_reference_count must be a non-negative integer")
        if type(self.runtime_state_complete) is not bool:
            raise TypeError("runtime_state_complete must be a boolean")
        if self.runtime_state_complete and self.runtime_reference_count != len(runtime_items):
            raise ValueError("runtime_reference_count must match the accepted runtime-only references")
        if not self.runtime_state_complete and (runtime_items or self.runtime_reference_count == 0):
            raise ValueError("an incomplete runtime state must contain only a retained positive digest/count")

        canonical = json.dumps(
            self._full_projection(),
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        if len(canonical) > _MAX_TRUSTED_CONTEXT_BYTES:
            raise ValueError("canonical trusted run context is limited to 32 KiB")

    def _full_projection(self) -> dict[str, object]:
        version = 4 if self.credential is not None else (3 if self.extension_artifact_manifest_digest is not None else (2 if self.tenant is not None else 1))
        projection = {
            "version": version,
            "identity": self.identity.to_json(),
            "origin": {
                "source_kind": self.origin.source_kind,
                "references": [_reference_json(item) for item in self.origin.references],
                "digest": self.origin.digest,
                "contributor_references": _trusted_reference_projection(self.origin.contributor_references),
            },
            "thread_id": self.thread_id,
            "external_key_reference": self.external_key_reference,
            "agent_revision": {
                "agent_id": self.agent_revision.agent_id,
                "digest": self.agent_revision.digest,
            },
            "profile_revision": {
                "profile_id": self.profile_revision.profile_id,
                "digest": self.profile_revision.digest,
            },
            "extension_generation": self.extension_generation,
            "extension_manifest_digest": self.extension_manifest_digest,
            "persistable_references": _trusted_reference_projection(self.persistable_references),
            "runtime_only_references": _trusted_reference_projection(self.runtime_only_references),
            "secret_handles": _trusted_reference_projection(self.secret_handles),
            "run_id": self.run_id,
            "runtime_reference_digest": self.runtime_reference_digest,
            "runtime_reference_count": self.runtime_reference_count,
        }
        if self.tenant is not None:
            projection["tenant"] = self.tenant.to_json()
        if version == 4:
            projection["credential"] = self.credential.to_json()  # type: ignore[union-attr]
            projection["extension_artifact_manifest_digest"] = self.extension_artifact_manifest_digest
            projection["extension_configuration_digest"] = self.extension_configuration_digest
        elif self.extension_artifact_manifest_digest is not None:
            projection["extension_artifact_manifest_digest"] = self.extension_artifact_manifest_digest
            projection["extension_configuration_digest"] = self.extension_configuration_digest
        return projection

    def bind_run(self, run_id: str) -> TrustedRunContextV1:
        """Return the same accepted facts bound to the admitted run ID."""

        return replace(self, run_id=run_id)

    @property
    def authorization_attributes(self) -> Mapping[str, SafeValueV1]:
        """Read-only, namespaced execution values for compatibility policy."""

        return MappingProxyType(
            {
                item.fully_qualified_key: item.reference.value
                for item in (
                    *self.persistable_references,
                    *self.runtime_only_references,
                )
                if item.reference.purpose == "execution"
            }
        )

    @property
    def verified_actor(self) -> VerifiedActorContextV1 | None:
        """Return the composed actor evidence for credential-bound contexts."""

        if self.credential is None or self.tenant is None:
            return None
        return VerifiedActorContextV1(
            identity=self.identity,
            credential=self.credential,
            tenant=self.tenant,
        )

    @property
    def digest(self) -> str:
        """Stable audit-evidence digest, including persistable correlation."""

        projection = self._persisted_projection()
        return hashlib.sha256(
            json.dumps(
                projection,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
        ).hexdigest()

    @property
    def execution_digest(self) -> str:
        """Stable execution identity, excluding audit-only correlation."""

        persistable_execution = tuple(item for item in self.persistable_references if item.reference.purpose == "execution")
        persistable_handles = tuple(item for item in self.secret_handles if item.reference.storage_class == "persistable")
        version = 4 if self.credential is not None else (3 if self.extension_artifact_manifest_digest is not None else (2 if self.tenant is not None else 1))
        projection = {
            "version": version,
            "identity": self.identity.to_json(),
            "base_origin": {
                "source_kind": self.origin.source_kind,
                "references": [_reference_json(item) for item in self.origin.references],
            },
            "thread_id": self.thread_id,
            "external_key_reference": self.external_key_reference,
            "agent_revision": {
                "agent_id": self.agent_revision.agent_id,
                "digest": self.agent_revision.digest,
            },
            "profile_revision": {
                "profile_id": self.profile_revision.profile_id,
                "digest": self.profile_revision.digest,
            },
            "extension_generation": self.extension_generation,
            "extension_manifest_digest": self.extension_manifest_digest,
            "persistable_execution_references": _trusted_reference_projection(persistable_execution),
            "persistable_secret_handles": _trusted_reference_projection(persistable_handles),
            "runtime_reference_digest": self.runtime_reference_digest,
            "runtime_reference_count": self.runtime_reference_count,
        }
        if self.tenant is not None:
            projection["tenant"] = self.tenant.to_json()
        if version == 4:
            projection["credential"] = self.credential.to_json()  # type: ignore[union-attr]
            projection["extension_artifact_manifest_digest"] = self.extension_artifact_manifest_digest
            projection["extension_configuration_digest"] = self.extension_configuration_digest
        elif self.extension_artifact_manifest_digest is not None:
            projection["extension_artifact_manifest_digest"] = self.extension_artifact_manifest_digest
            projection["extension_configuration_digest"] = self.extension_configuration_digest
        return hashlib.sha256(
            json.dumps(
                projection,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
        ).hexdigest()

    def _persisted_projection(self) -> dict[str, object]:
        projection = self._full_projection()
        projection["origin"] = {
            **projection["origin"],  # type: ignore[dict-item]
            "contributor_references": _trusted_reference_projection(tuple(item for item in self.origin.contributor_references if item.reference.storage_class == "persistable")),
        }
        projection["runtime_only_references"] = []
        projection["secret_handles"] = _trusted_reference_projection(tuple(item for item in self.secret_handles if item.reference.storage_class == "persistable"))
        projection["run_id"] = None
        return projection

    def to_persisted_json(self) -> dict[str, object]:
        """Return accepted evidence without runtime-only values."""

        projection = self._persisted_projection()
        projection["evidence_digest"] = self.digest
        return projection

    @classmethod
    def from_persisted_json(cls, value: object) -> TrustedRunContextV1:
        expected = {
            "version",
            "identity",
            "origin",
            "thread_id",
            "external_key_reference",
            "agent_revision",
            "profile_revision",
            "extension_generation",
            "extension_manifest_digest",
            "persistable_references",
            "runtime_only_references",
            "secret_handles",
            "run_id",
            "runtime_reference_digest",
            "runtime_reference_count",
            "evidence_digest",
        }
        if not isinstance(value, dict) or value.get("version") not in {1, 2, 3, 4}:
            raise ValueError("trusted run context has unknown fields or an unsupported version")
        if value.get("version") in {2, 3, 4}:
            expected.add("tenant")
        if value.get("version") in {3, 4}:
            expected.update(
                {
                    "extension_artifact_manifest_digest",
                    "extension_configuration_digest",
                }
            )
        if value.get("version") == 4:
            expected.add("credential")
        if set(value) != expected:
            raise ValueError("trusted run context has unknown fields or an unsupported version")
        origin = value["origin"]
        agent = value["agent_revision"]
        profile = value["profile_revision"]
        if not isinstance(origin, dict) or set(origin) != {
            "source_kind",
            "references",
            "digest",
            "contributor_references",
        }:
            raise ValueError("trusted Origin has unknown or missing fields")
        if not isinstance(agent, dict) or set(agent) != {"agent_id", "digest"}:
            raise ValueError("trusted agent revision has unknown or missing fields")
        if not isinstance(profile, dict) or set(profile) != {"profile_id", "digest"}:
            raise ValueError("trusted profile revision has unknown or missing fields")
        runtime_count = value["runtime_reference_count"]
        trusted = cls(
            identity=InvocationIdentityV1.from_json(value["identity"]),  # type: ignore[arg-type]
            tenant=(TenantReferenceV1.from_json(value["tenant"]) if value.get("version") in {2, 3, 4} else None),
            credential=(CredentialEvidenceV1.from_json(value["credential"]) if value.get("version") == 4 else None),
            origin=SealedOriginV1(
                source_kind=origin["source_kind"],  # type: ignore[arg-type]
                references=tuple(_reference_from_json(item) for item in origin["references"]),  # type: ignore[union-attr]
                digest=origin["digest"],  # type: ignore[arg-type]
                contributor_references=tuple(NamespacedContextReferenceV1.from_json(item) for item in origin["contributor_references"]),  # type: ignore[union-attr]
            ),
            thread_id=value["thread_id"],  # type: ignore[arg-type]
            external_key_reference=value["external_key_reference"],  # type: ignore[arg-type]
            agent_revision=ResolvedAgentRevisionReferenceV1(agent_id=agent["agent_id"], digest=agent["digest"]),  # type: ignore[arg-type]
            profile_revision=ResolvedProfileRevisionReferenceV1(profile_id=profile["profile_id"], digest=profile["digest"]),  # type: ignore[arg-type]
            extension_generation=value["extension_generation"],  # type: ignore[arg-type]
            extension_manifest_digest=value["extension_manifest_digest"],  # type: ignore[arg-type]
            extension_artifact_manifest_digest=value.get("extension_artifact_manifest_digest"),  # type: ignore[arg-type]
            extension_configuration_digest=value.get("extension_configuration_digest"),  # type: ignore[arg-type]
            persistable_references=tuple(NamespacedContextReferenceV1.from_json(item) for item in value["persistable_references"]),  # type: ignore[union-attr]
            runtime_only_references=(),
            secret_handles=tuple(NamespacedContextReferenceV1.from_json(item) for item in value["secret_handles"]),  # type: ignore[union-attr]
            runtime_reference_digest=value["runtime_reference_digest"],  # type: ignore[arg-type]
            runtime_reference_count=runtime_count,  # type: ignore[arg-type]
            runtime_state_complete=runtime_count == 0,
        )
        evidence_digest = value["evidence_digest"]
        if not isinstance(evidence_digest, str) or _DIGEST.fullmatch(evidence_digest) is None:
            raise ValueError("trusted run-context evidence_digest must be a lowercase SHA-256 digest")
        if evidence_digest != trusted.digest:
            raise ValueError("trusted run-context evidence digest does not match its persisted facts")
        return trusted


@dataclass(frozen=True)
class RunContextContributionRequestV1:
    principal: PrincipalProjectionV1
    origin: SealedOriginV1
    thread_id: str
    agent_revision: ResolvedAgentRevisionReferenceV1
    external_key_reference: str | None = None
    tenant: TenantReferenceV1 | None = None

    def __post_init__(self) -> None:
        validate_thread_identifier(self.thread_id, field_name="thread_id")
        if self.tenant is not None and not isinstance(self.tenant, TenantReferenceV1):
            raise TypeError("tenant must be TenantReferenceV1 or None")


@dataclass(frozen=True)
class RunContextContributionV1:
    namespace: str
    references: tuple[SafeContextReferenceV1, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "references", tuple(self.references))
        _validate_contribution(self.namespace, self.references)


@runtime_checkable
class OriginContributor(Protocol):
    async def contribute(self, request: OriginContributionRequestV1) -> OriginContributionV1 | None:
        return None


@runtime_checkable
class RunContextContributor(Protocol):
    async def contribute(self, request: RunContextContributionRequestV1) -> RunContextContributionV1 | None:
        return None


@dataclass(frozen=True)
class OriginContributorFactory:
    contribution_id: str
    capability_api_version: str
    factory: Callable[[], OriginContributor]
    kind: Literal["origin_contributor"]
    health_probe: CapabilityHealthProbe | None = None

    def __post_init__(self) -> None:
        _validate_contributor_identifier(self.contribution_id, field_name="origin contributor contribution_id")
        if self.capability_api_version != ORIGIN_CONTRIBUTOR_CAPABILITY_API_VERSION:
            raise ValueError(f"unsupported origin contributor capability API version {self.capability_api_version!r}; expected {ORIGIN_CONTRIBUTOR_CAPABILITY_API_VERSION!r}")
        if self.kind != ORIGIN_CONTRIBUTOR_KIND:
            raise ValueError(f"origin contributor kind must be {ORIGIN_CONTRIBUTOR_KIND!r}")
        if not callable(self.factory):
            raise TypeError("origin contributor factory must be callable")
        if self.health_probe is not None and not callable(self.health_probe):
            raise TypeError("origin contributor health_probe must be callable")


@dataclass(frozen=True)
class RunContextContributorFactory:
    contribution_id: str
    capability_api_version: str
    factory: Callable[[], RunContextContributor]
    kind: Literal["run_context_contributor"]
    health_probe: CapabilityHealthProbe | None = None

    def __post_init__(self) -> None:
        _validate_contributor_identifier(self.contribution_id, field_name="run-context contributor contribution_id")
        if self.capability_api_version != RUN_CONTEXT_CONTRIBUTOR_CAPABILITY_API_VERSION:
            raise ValueError(f"unsupported run-context contributor capability API version {self.capability_api_version!r}; expected {RUN_CONTEXT_CONTRIBUTOR_CAPABILITY_API_VERSION!r}")
        if self.kind != RUN_CONTEXT_CONTRIBUTOR_KIND:
            raise ValueError(f"run-context contributor kind must be {RUN_CONTEXT_CONTRIBUTOR_KIND!r}")
        if not callable(self.factory):
            raise TypeError("run-context contributor factory must be callable")
        if self.health_probe is not None and not callable(self.health_probe):
            raise TypeError("run-context contributor health_probe must be callable")
