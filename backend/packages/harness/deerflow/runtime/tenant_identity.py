"""Server-owned deployment tenant identity and namespace projections.

The canonical identifier is operator configuration. Runtime and persistence
consumers receive only the bounded pseudonymous reference produced here.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Literal, Self

from deerflow_extension_api.tenant import TenantReferenceV1

from deerflow.deployment.topology import DeploymentProfile, coerce_deployment_profile

if TYPE_CHECKING:
    from deerflow.config.deployment_config import DeploymentConfig

TENANT_ID_ENV = "DEER_FLOW_TENANT_ID"
TENANT_REFERENCE_CONTEXT_KEY = "__deerflow_tenant_reference"
TENANT_PREFIX_SCHEMA_VERSION = 1
TENANT_ID_PATTERN = re.compile(
    r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$",
    re.ASCII,
)
_PUBLIC_REF = re.compile(r"^tenant-[0-9a-f]{16}$", re.ASCII)
_MAX_LEGACY_REDIS_PREFIX_BYTES = 512


def _canonical_digest(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


class TenantIdentityError(ValueError):
    """Stable, actionable tenant startup failure."""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(f"{code}: {message}")


class TenantSubsystem(StrEnum):
    """Versioned downstream namespace families derived from one tenant."""

    REDIS = "redis"
    OPENSANDBOX = "opensandbox"
    HONCHO = "honcho"
    MCP_TASKS = "mcp_tasks"
    EXTENSIONS = "extensions"
    QUALIFICATION = "qualification"


class RedisTenantComponent(StrEnum):
    """Covered Redis key families within one tenant namespace."""

    STREAM_BRIDGE = "stream_bridge"
    CHECKPOINT_CACHE = "checkpoint_cache"
    SANDBOX_OWNERSHIP = "sandbox_ownership"
    QUALIFICATION = "qualification"


@dataclass(frozen=True)
class LegacyRedisPrefixRecordV1:
    """Exact operator-recorded Redis projections for one migration window."""

    version: Literal[1] = 1
    stream_bridge: str | None = None
    checkpoint_cache: str | None = None
    sandbox_ownership: str | None = None

    def __post_init__(self) -> None:
        if self.version != 1:
            raise ValueError("legacy Redis prefix record version must be 1")
        for field_name, value in (
            ("stream_bridge", self.stream_bridge),
            ("checkpoint_cache", self.checkpoint_cache),
            ("sandbox_ownership", self.sandbox_ownership),
        ):
            if value is None:
                continue
            if not isinstance(value, str) or not value or len(value.encode("utf-8")) > _MAX_LEGACY_REDIS_PREFIX_BYTES or any(ord(character) < 0x20 or ord(character) == 0x7F for character in value):
                raise TenantIdentityError(
                    "tenant_namespace_conflict",
                    f"legacy Redis {field_name} prefix must be a nonempty control-free string of at most {_MAX_LEGACY_REDIS_PREFIX_BYTES} bytes",
                )

    def prefix_for(self, component: RedisTenantComponent) -> str | None:
        """Return the recorded compatibility prefix for one component."""

        if component is RedisTenantComponent.STREAM_BRIDGE:
            return self.stream_bridge
        if component is RedisTenantComponent.CHECKPOINT_CACHE:
            return self.checkpoint_cache
        if component is RedisTenantComponent.SANDBOX_OWNERSHIP:
            return self.sandbox_ownership
        return None

    def to_json(self) -> dict[str, object]:
        """Serialize the fixed, bounded migration record."""

        return {
            "version": self.version,
            "stream_bridge": self.stream_bridge,
            "checkpoint_cache": self.checkpoint_cache,
            "sandbox_ownership": self.sandbox_ownership,
        }

    @classmethod
    def from_json(cls, value: object) -> Self:
        """Parse the fixed migration record without accepting extra fields."""

        expected = {
            "version",
            "stream_bridge",
            "checkpoint_cache",
            "sandbox_ownership",
        }
        if not isinstance(value, Mapping) or set(value) != expected:
            raise ValueError("legacy Redis prefix record has unknown or missing fields")
        return cls(
            version=value["version"],  # type: ignore[arg-type]
            stream_bridge=value["stream_bridge"],  # type: ignore[arg-type]
            checkpoint_cache=value["checkpoint_cache"],  # type: ignore[arg-type]
            sandbox_ownership=value["sandbox_ownership"],  # type: ignore[arg-type]
        )

    @property
    def is_empty(self) -> bool:
        """Return whether no legacy component projection was recorded."""

        return all(
            value is None
            for value in (
                self.stream_bridge,
                self.checkpoint_cache,
                self.sandbox_ownership,
            )
        )


_REDIS_COMPONENT_SUFFIX: Mapping[RedisTenantComponent, str] = {
    RedisTenantComponent.STREAM_BRIDGE: "",
    RedisTenantComponent.CHECKPOINT_CACHE: "ckpt-hist:v1",
    RedisTenantComponent.SANDBOX_OWNERSHIP: "deerflow:sandbox:owner",
    RedisTenantComponent.QUALIFICATION: "qualification",
}


@dataclass(frozen=True)
class TenantNamespaceV1:
    """Bounded deterministic projection for one downstream subsystem."""

    subsystem: TenantSubsystem
    key_prefix: str
    metadata_ref: str
    digest: str

    def __post_init__(self) -> None:
        if not isinstance(self.subsystem, TenantSubsystem):
            raise TypeError("subsystem must be TenantSubsystem")
        if not isinstance(self.key_prefix, str) or not self.key_prefix:
            raise ValueError("key_prefix must be a non-empty string")
        if _PUBLIC_REF.fullmatch(self.metadata_ref) is None:
            raise ValueError("metadata_ref must be a tenant public reference")
        if re.fullmatch(r"[0-9a-f]{64}", self.digest, re.ASCII) is None:
            raise ValueError("namespace digest must be a lowercase SHA-256 digest")


@dataclass(frozen=True)
class TenantIdentityV1:
    """Immutable operator-selected identity resolved once per Gateway."""

    version: Literal[1]
    canonical_id: str
    digest: str
    public_ref: str

    def __post_init__(self) -> None:
        if self.version != 1:
            raise ValueError("tenant identity version must be 1")
        self._validate_canonical_id(self.canonical_id)
        expected_digest = _canonical_digest({"version": 1, "tenant_id": self.canonical_id})
        if self.digest != expected_digest:
            raise ValueError("tenant digest does not match the canonical identity")
        if self.public_ref != f"tenant-{self.digest[:16]}":
            raise ValueError("tenant public_ref does not match the digest prefix")

    @staticmethod
    def _validate_canonical_id(value: object) -> str:
        if not isinstance(value, str) or TENANT_ID_PATTERN.fullmatch(value) is None:
            raise TenantIdentityError(
                "tenant_identity_invalid",
                "deployment.tenant_id/DEER_FLOW_TENANT_ID must be a lowercase DNS label of 1-63 characters; values are not lowercased or sanitized",
            )
        return value

    @classmethod
    def from_canonical_id(cls, canonical_id: str) -> Self:
        canonical_id = cls._validate_canonical_id(canonical_id)
        digest = _canonical_digest({"version": 1, "tenant_id": canonical_id})
        return cls(
            version=1,
            canonical_id=canonical_id,
            digest=digest,
            public_ref=f"tenant-{digest[:16]}",
        )

    @classmethod
    def resolve(
        cls,
        *,
        deployment_config: DeploymentConfig,
        environ: Mapping[str, str],
    ) -> Self:
        if TENANT_ID_ENV in environ:
            configured = environ[TENANT_ID_ENV]
            explicit = True
        else:
            configured = deployment_config.tenant_id
            explicit = configured is not None

        profile = coerce_deployment_profile(deployment_config.profile)
        if configured is None:
            if profile.is_durable:
                raise TenantIdentityError(
                    "tenant_identity_required",
                    "durable_production requires an explicit non-'local' deployment.tenant_id or DEER_FLOW_TENANT_ID",
                )
            configured = "local"

        if profile is not DeploymentProfile.local_development and (not explicit or configured == "local"):
            raise TenantIdentityError(
                "tenant_identity_required",
                "durable_production requires an explicit non-'local' deployment.tenant_id or DEER_FLOW_TENANT_ID",
            )
        return cls.from_canonical_id(configured)

    def namespace(self, subsystem: TenantSubsystem) -> TenantNamespaceV1:
        return tenant_namespace_from_reference(
            self.to_persisted_reference(),
            subsystem,
        )

    def to_persisted_reference(self) -> TenantReferenceV1:
        return TenantReferenceV1(
            version=1,
            public_ref=self.public_ref,
            digest=self.digest,
        )


def tenant_namespace_from_reference(
    reference: TenantReferenceV1,
    subsystem: TenantSubsystem,
) -> TenantNamespaceV1:
    """Project a safe tenant reference into one subsystem namespace."""

    if not isinstance(reference, TenantReferenceV1):
        raise TypeError("reference must be TenantReferenceV1")
    if not isinstance(subsystem, TenantSubsystem):
        raise TypeError("subsystem must be TenantSubsystem")
    digest_prefix = reference.digest[:16]
    if subsystem is TenantSubsystem.REDIS:
        key_prefix = f"hm:v1:{reference.public_ref}:{subsystem.value}:"
    else:
        dns_subsystem = subsystem.value.replace("_", "-")
        key_prefix = f"hm-v1-{digest_prefix}-{dns_subsystem}"
    namespace_digest = _canonical_digest(
        {
            "version": 1,
            "tenant_digest": reference.digest,
            "subsystem": subsystem.value,
            "key_prefix": key_prefix,
            "metadata_ref": reference.public_ref,
        }
    )
    return TenantNamespaceV1(
        subsystem=subsystem,
        key_prefix=key_prefix,
        metadata_ref=reference.public_ref,
        digest=namespace_digest,
    )


def redis_component_key_prefix(
    namespace: TenantNamespaceV1,
    component: RedisTenantComponent,
    *,
    configured_prefix: str | None = None,
    configured_field: str | None = None,
    legacy_record: LegacyRedisPrefixRecordV1 | None = None,
) -> str:
    """Return one centrally-derived Redis key/channel prefix.

    The first tenant-identity feature release keeps the legacy prefix knobs as
    validation-only compatibility inputs. A configured value may equal the
    canonical projection, but it can no longer select an unrelated namespace.
    """

    if namespace.subsystem is not TenantSubsystem.REDIS:
        raise TypeError("namespace must be the Redis tenant projection")
    if not isinstance(component, RedisTenantComponent):
        raise TypeError("component must be RedisTenantComponent")

    base = namespace.key_prefix.rstrip(":")
    suffix = _REDIS_COMPONENT_SUFFIX[component]
    expected = f"{base}:{suffix}" if suffix else base
    if legacy_record is not None and not isinstance(
        legacy_record,
        LegacyRedisPrefixRecordV1,
    ):
        raise TypeError("legacy_record must be LegacyRedisPrefixRecordV1 or None")
    if configured_prefix is not None:
        selected = configured_prefix.rstrip(":")
        legacy = None if legacy_record is None else legacy_record.prefix_for(component)
        if selected == expected:
            return expected
        if legacy is not None and selected == legacy.rstrip(":"):
            return selected
        field = configured_field or "Redis key prefix"
        raise TenantIdentityError(
            "tenant_namespace_conflict",
            f"{field} conflicts with the canonical or operator-recorded legacy {component.value} tenant prefix",
        )
    return expected


def redis_component_match_pattern(
    namespace: TenantNamespaceV1,
    component: RedisTenantComponent,
) -> str:
    """Return the bounded inventory/ACL match for one current key family."""

    prefix = redis_component_key_prefix(namespace, component)
    if component is RedisTenantComponent.STREAM_BRIDGE:
        return f"{prefix}:deerflow:stream_bridge:*"
    return f"{prefix}:*"


def tenant_observability_projection(
    reference: TenantReferenceV1,
) -> dict[str, object]:
    """Return the only tenant shape allowed in health/deployment output."""

    if not isinstance(reference, TenantReferenceV1):
        raise TypeError("reference must be TenantReferenceV1")
    return {
        "version": reference.version,
        "public_ref": reference.public_ref,
        "digest": reference.digest,
        "prefix_schema_version": TENANT_PREFIX_SCHEMA_VERSION,
    }


def tenant_admission_scope(
    reference: TenantReferenceV1,
    base_scope: str,
) -> str:
    """Bind a host-authenticated idempotency scope to one process tenant."""

    if not isinstance(reference, TenantReferenceV1):
        raise TypeError("reference must be TenantReferenceV1")
    if not isinstance(base_scope, str) or not base_scope:
        raise ValueError("base_scope must be a non-empty string")
    digest = _canonical_digest(
        {
            "version": 1,
            "domain": "deerflow-tenant-admission-scope",
            "tenant_digest": reference.digest,
            "base_scope": base_scope,
        }
    )
    return f"tenant:v1:sha256:{digest}"


__all__ = [
    "RedisTenantComponent",
    "LegacyRedisPrefixRecordV1",
    "TENANT_ID_ENV",
    "TENANT_ID_PATTERN",
    "TENANT_PREFIX_SCHEMA_VERSION",
    "TENANT_REFERENCE_CONTEXT_KEY",
    "TenantIdentityError",
    "TenantIdentityV1",
    "TenantNamespaceV1",
    "TenantReferenceV1",
    "TenantSubsystem",
    "redis_component_key_prefix",
    "redis_component_match_pattern",
    "tenant_admission_scope",
    "tenant_namespace_from_reference",
    "tenant_observability_projection",
]
