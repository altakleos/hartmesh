"""Exact topology contracts for the conservative two-Gateway profile.

The inventory is deliberately checked in as cross-component data under
``contracts/deployment``.  This module owns its strict bounded parser and the
canonical sets that make omissions visible when startup infrastructure grows.
It contains no credentials, raw tenant identifiers, or provider SDK objects.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from types import MappingProxyType
from typing import Final, Literal, Protocol, runtime_checkable

MULTI_GATEWAY_PROFILE: Final = "durable_two_gateway_v1"
MULTI_GATEWAY_QUALIFICATION_SCOPE: Final = "durable_two_gateway_v1_postgres_redis_aio_rwx"
MULTI_GATEWAY_REPLICA_COUNT: Final = 2
MULTI_GATEWAY_IMAGE_NAMES: Final[frozenset[str]] = frozenset(
    {
        "gateway",
        "frontend",
        "nginx",
        "provisioner",
        "sandbox",
        "postgres",
        "redis",
    }
)
TOPOLOGY_HEARTBEAT_INTERVAL_SECONDS: Final = 10.0
TOPOLOGY_LIVE_TTL_SECONDS: Final = 35.0

TOPOLOGY_INVENTORY_CLASSIFICATIONS: Final[frozenset[str]] = frozenset(
    {
        "process_local_cache_only",
        "local_substitutable_store",
        "remote_owned_service",
        "true_external_dependency",
        "singleton_activity",
        "replica_safe_stateless_work",
    }
)

# STARTUP_ONLY_FIELDS is the canonical registry for process-frozen AppConfig
# sections.  These extra hot/reloadable sections can also change how a replica
# behaves and therefore participate in the topology audit even though they do
# not construct startup singletons themselves.
MULTI_GATEWAY_ADDITIONAL_CONFIG_FIELDS: Final[frozenset[str]] = frozenset(
    {
        "auth",
        "circuit_breaker",
        "execution_policy",
        "llm_call",
        "memory",
        "token_budget",
    }
)

_MAX_INVENTORY_BYTES = 64 * 1024
_MAX_DEPENDENCIES = 128
_SAFE_ID = re.compile(r"[a-z][a-z0-9_]{0,127}\Z")
_SAFE_FIELD = re.compile(r"[a-z][a-z0-9_]{0,63}\Z")
_SAFE_CONFIG_FIELD = re.compile(
    r"(?=.{1,128}\Z)[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)*\Z",
)
_SHA256 = re.compile(r"sha256:[0-9a-f]{64}\Z")
_RAW_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_SCHEMA_REFERENCE = re.compile(r"schema:sha256:[0-9a-f]{64}\Z")
_MIGRATION_HEAD = re.compile(r"[0-9]{4}_[a-z0-9_]{1,123}\Z")
_REPLICA_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
_TEXT_LIMITS = {
    "authority": 512,
    "required_adapter": 512,
    "requirement": 512,
}

TopologyInventoryClassification = Literal[
    "process_local_cache_only",
    "local_substitutable_store",
    "remote_owned_service",
    "true_external_dependency",
    "singleton_activity",
    "replica_safe_stateless_work",
]


class DeploymentProfile(StrEnum):
    """Central startup promise shared by app validation and topology code."""

    local_development = "local_development"
    durable_production = "durable_production"
    durable_two_gateway_v1 = MULTI_GATEWAY_PROFILE

    @property
    def is_durable(self) -> bool:
        return self is not DeploymentProfile.local_development


def coerce_deployment_profile(value: object | None = None) -> DeploymentProfile:
    """Resolve one profile through the canonical enum, defaulting only omission."""

    if value is None:
        return DeploymentProfile.local_development
    raw = getattr(value, "value", value)
    try:
        return DeploymentProfile(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError("unknown deployment profile") from exc


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _require_digest(value: object, *, field_name: str, prefixed: bool) -> str:
    pattern = _SHA256 if prefixed else _RAW_SHA256
    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        form = "sha256:<64 lowercase hex>" if prefixed else "64 lowercase hexadecimal characters"
        raise ValueError(f"{field_name} must use {form}")
    return value


@dataclass(frozen=True, slots=True)
class TopologyFingerprintV1:
    """Canonical redacted identity that every qualified replica must share."""

    profile: str
    tenant_digest: str
    image_digests: Mapping[str, str]
    config_digest: str
    database_schema_ref: str
    redis_namespace_digest: str
    extension_artifact_digest: str
    extension_configuration_digest: str
    capability_manifest_digest: str
    migration_head: str
    accepted_materialization_profile: str
    mcp_task_replay_keyring_confirmation_version: int | None = None
    mcp_task_replay_keyring_confirmation_digest: str | None = None
    execution_policy_keyring_confirmation_version: int | None = None
    execution_policy_keyring_confirmation_digest: str | None = None
    digest: str = ""

    def __post_init__(self) -> None:
        if self.profile != MULTI_GATEWAY_PROFILE:
            raise ValueError("profile must be durable_two_gateway_v1")
        _require_digest(
            self.tenant_digest,
            field_name="tenant_digest",
            prefixed=False,
        )
        if not isinstance(self.image_digests, Mapping):
            raise TypeError("image_digests must be a mapping")
        images = dict(sorted(self.image_digests.items()))
        if set(images) != MULTI_GATEWAY_IMAGE_NAMES:
            raise ValueError("image_digests must contain the exact qualified image set")
        for name, digest in images.items():
            if not isinstance(name, str) or _SAFE_FIELD.fullmatch(name) is None:
                raise ValueError("image_digests contains an invalid image name")
            _require_digest(
                digest,
                field_name=f"image_digests.{name}",
                prefixed=True,
            )
        object.__setattr__(self, "image_digests", MappingProxyType(images))
        for field_name in (
            "config_digest",
            "redis_namespace_digest",
            "extension_artifact_digest",
            "extension_configuration_digest",
        ):
            _require_digest(
                getattr(self, field_name),
                field_name=field_name,
                prefixed=True,
            )
        _require_digest(
            self.capability_manifest_digest,
            field_name="capability_manifest_digest",
            prefixed=False,
        )
        confirmation_version = self.mcp_task_replay_keyring_confirmation_version
        confirmation_digest = self.mcp_task_replay_keyring_confirmation_digest
        if (confirmation_version is None) != (confirmation_digest is None):
            raise ValueError(
                "MCP replay keyring confirmation fields must be paired",
            )
        if confirmation_version is not None:
            if type(confirmation_version) is not int or confirmation_version != 1:
                raise ValueError(
                    "mcp_task_replay_keyring_confirmation_version must be 1",
                )
            _require_digest(
                confirmation_digest,
                field_name="mcp_task_replay_keyring_confirmation_digest",
                prefixed=True,
            )
        policy_confirmation_version = self.execution_policy_keyring_confirmation_version
        policy_confirmation_digest = self.execution_policy_keyring_confirmation_digest
        if (policy_confirmation_version is None) != (policy_confirmation_digest is None):
            raise ValueError("execution policy keyring confirmation fields must be paired")
        if policy_confirmation_version is not None:
            if type(policy_confirmation_version) is not int or policy_confirmation_version != 1:
                raise ValueError("execution_policy_keyring_confirmation_version must be 1")
            _require_digest(
                policy_confirmation_digest,
                field_name="execution_policy_keyring_confirmation_digest",
                prefixed=True,
            )
        if not isinstance(self.database_schema_ref, str) or _SCHEMA_REFERENCE.fullmatch(self.database_schema_ref) is None:
            raise ValueError("database_schema_ref must be a redacted schema:sha256 reference")
        if not isinstance(self.migration_head, str) or _MIGRATION_HEAD.fullmatch(self.migration_head) is None:
            raise ValueError("migration_head must be a bounded Alembic revision")
        if self.accepted_materialization_profile != "rwx_verified_copy_v2":
            raise ValueError("accepted_materialization_profile must be rwx_verified_copy_v2")
        computed = hashlib.sha256(_canonical_json(self._core_dict())).hexdigest()
        if self.digest:
            _require_digest(self.digest, field_name="digest", prefixed=False)
            if self.digest != computed:
                raise ValueError("topology fingerprint digest mismatch")
        else:
            object.__setattr__(self, "digest", computed)

    @classmethod
    def create(
        cls,
        *,
        profile: str,
        tenant_digest: str,
        image_digests: Mapping[str, str],
        config_digest: str,
        database_schema_ref: str,
        redis_namespace_digest: str,
        extension_artifact_digest: str,
        extension_configuration_digest: str,
        capability_manifest_digest: str,
        migration_head: str,
        accepted_materialization_profile: str,
        mcp_task_replay_keyring_confirmation_version: int | None = None,
        mcp_task_replay_keyring_confirmation_digest: str | None = None,
        execution_policy_keyring_confirmation_version: int | None = None,
        execution_policy_keyring_confirmation_digest: str | None = None,
    ) -> TopologyFingerprintV1:
        return cls(
            profile=profile,
            tenant_digest=tenant_digest,
            image_digests=image_digests,
            config_digest=config_digest,
            database_schema_ref=database_schema_ref,
            redis_namespace_digest=redis_namespace_digest,
            extension_artifact_digest=extension_artifact_digest,
            extension_configuration_digest=extension_configuration_digest,
            capability_manifest_digest=capability_manifest_digest,
            mcp_task_replay_keyring_confirmation_version=(mcp_task_replay_keyring_confirmation_version),
            mcp_task_replay_keyring_confirmation_digest=(mcp_task_replay_keyring_confirmation_digest),
            execution_policy_keyring_confirmation_version=(execution_policy_keyring_confirmation_version),
            execution_policy_keyring_confirmation_digest=(execution_policy_keyring_confirmation_digest),
            migration_head=migration_head,
            accepted_materialization_profile=accepted_materialization_profile,
        )

    def _core_dict(self) -> dict[str, object]:
        core: dict[str, object] = {
            "version": 1,
            "profile": self.profile,
            "tenant_digest": self.tenant_digest,
            "image_digests": dict(self.image_digests),
            "config_digest": self.config_digest,
            "database_schema_ref": self.database_schema_ref,
            "redis_namespace_digest": self.redis_namespace_digest,
            "extension_artifact_digest": self.extension_artifact_digest,
            "extension_configuration_digest": self.extension_configuration_digest,
            "capability_manifest_digest": self.capability_manifest_digest,
            "migration_head": self.migration_head,
            "accepted_materialization_profile": self.accepted_materialization_profile,
        }
        if self.mcp_task_replay_keyring_confirmation_version is not None:
            core["mcp_task_replay_keyring_confirmation_version"] = self.mcp_task_replay_keyring_confirmation_version
            core["mcp_task_replay_keyring_confirmation_digest"] = self.mcp_task_replay_keyring_confirmation_digest
        if self.execution_policy_keyring_confirmation_version is not None:
            core["execution_policy_keyring_confirmation_version"] = self.execution_policy_keyring_confirmation_version
            core["execution_policy_keyring_confirmation_digest"] = self.execution_policy_keyring_confirmation_digest
        return core

    def to_dict(self) -> dict[str, object]:
        return {**self._core_dict(), "digest": self.digest}

    def canonical_bytes(self) -> bytes:
        """Return canonical wire bytes including the verified digest."""

        return _canonical_json(self.to_dict()) + b"\n"

    @classmethod
    def from_dict(cls, value: object) -> TopologyFingerprintV1:
        legacy_fields = {
            "version",
            "profile",
            "tenant_digest",
            "image_digests",
            "config_digest",
            "database_schema_ref",
            "redis_namespace_digest",
            "extension_artifact_digest",
            "extension_configuration_digest",
            "capability_manifest_digest",
            "migration_head",
            "accepted_materialization_profile",
            "digest",
        }
        confirmation_fields = {
            "mcp_task_replay_keyring_confirmation_version",
            "mcp_task_replay_keyring_confirmation_digest",
        }
        policy_confirmation_fields = {
            "execution_policy_keyring_confirmation_version",
            "execution_policy_keyring_confirmation_digest",
        }
        if not isinstance(value, dict) or value.get("version") != 1:
            raise ValueError("topology fingerprint fields are invalid")
        actual_fields = set(value)
        if actual_fields not in {
            frozenset(legacy_fields),
            frozenset(legacy_fields | confirmation_fields),
            frozenset(legacy_fields | policy_confirmation_fields),
            frozenset(legacy_fields | confirmation_fields | policy_confirmation_fields),
        }:
            raise ValueError("topology fingerprint fields are invalid")
        images = value.get("image_digests")
        if not isinstance(images, dict):
            raise ValueError("topology fingerprint image_digests is invalid")
        try:
            return cls(
                profile=value["profile"],
                tenant_digest=value["tenant_digest"],
                image_digests=images,
                config_digest=value["config_digest"],
                database_schema_ref=value["database_schema_ref"],
                redis_namespace_digest=value["redis_namespace_digest"],
                extension_artifact_digest=value["extension_artifact_digest"],
                extension_configuration_digest=value["extension_configuration_digest"],
                capability_manifest_digest=value["capability_manifest_digest"],
                mcp_task_replay_keyring_confirmation_version=value.get(
                    "mcp_task_replay_keyring_confirmation_version",
                ),
                mcp_task_replay_keyring_confirmation_digest=value.get(
                    "mcp_task_replay_keyring_confirmation_digest",
                ),
                execution_policy_keyring_confirmation_version=value.get(
                    "execution_policy_keyring_confirmation_version",
                ),
                execution_policy_keyring_confirmation_digest=value.get(
                    "execution_policy_keyring_confirmation_digest",
                ),
                migration_head=value["migration_head"],
                accepted_materialization_profile=value["accepted_materialization_profile"],
                digest=value["digest"],
            )
        except (TypeError, ValueError) as exc:
            raise ValueError("topology fingerprint values are invalid") from exc


@dataclass(frozen=True, slots=True)
class TopologyStartupFactsV1:
    """Deployer-stamped, credential-free facts unique to one release/pod."""

    replica_id: str
    image_digests: Mapping[str, str]
    config_digest: str
    database_schema_ref: str

    def __post_init__(self) -> None:
        if _REPLICA_ID.fullmatch(self.replica_id) is None:
            raise ValueError("replica_id must be a bounded safe identifier")
        if not isinstance(self.image_digests, Mapping):
            raise TypeError("image_digests must be a mapping")
        images = dict(sorted(self.image_digests.items()))
        if set(images) != MULTI_GATEWAY_IMAGE_NAMES:
            raise ValueError("image_digests must contain the exact qualified image set")
        for name, digest in images.items():
            if _SAFE_FIELD.fullmatch(name) is None:
                raise ValueError("image_digests contains an invalid image name")
            _require_digest(
                digest,
                field_name=f"image_digests.{name}",
                prefixed=True,
            )
        _require_digest(
            self.config_digest,
            field_name="config_digest",
            prefixed=True,
        )
        if _SCHEMA_REFERENCE.fullmatch(self.database_schema_ref) is None:
            raise ValueError("database_schema_ref must be a redacted schema reference")
        object.__setattr__(self, "image_digests", MappingProxyType(images))

    @classmethod
    def from_environment(
        cls,
        environ: Mapping[str, str] | None = None,
    ) -> TopologyStartupFactsV1:
        values = os.environ if environ is None else environ
        raw_images = values.get("DEER_FLOW_TOPOLOGY_IMAGE_DIGESTS")
        if not isinstance(raw_images, str) or not raw_images or len(raw_images.encode("utf-8")) > 4096:
            raise ValueError("topology image digests are missing or invalid")
        try:
            images = json.loads(raw_images)
        except json.JSONDecodeError as exc:
            raise ValueError("topology image digests are invalid") from exc
        if not isinstance(images, dict):
            raise ValueError("topology image digests must be a mapping")
        return cls(
            replica_id=values.get("DEER_FLOW_REPLICA_ID", ""),
            image_digests=images,
            config_digest=values.get("DEER_FLOW_TOPOLOGY_CONFIG_DIGEST", ""),
            database_schema_ref=values.get(
                "DEER_FLOW_TOPOLOGY_DATABASE_SCHEMA_REF",
                "",
            ),
        )


def _prefixed_digest(value: object, *, field_name: str) -> str:
    if isinstance(value, str) and _RAW_SHA256.fullmatch(value):
        return f"sha256:{value}"
    if isinstance(value, str) and _SHA256.fullmatch(value):
        return value
    raise ValueError(f"{field_name} must be a SHA-256 digest")


def build_topology_fingerprint(
    *,
    facts: TopologyStartupFactsV1,
    tenant_digest: str,
    redis_namespace_digest: str,
    capability_manifest: object,
    config: object,
    mcp_task_replay_keyring_confirmation_version: int,
    mcp_task_replay_keyring_confirmation_digest: str,
    execution_policy_keyring_confirmation_version: int | None = None,
    execution_policy_keyring_confirmation_digest: str | None = None,
) -> TopologyFingerprintV1:
    """Bind verified startup facts without serializing config or credentials."""

    if not isinstance(facts, TopologyStartupFactsV1):
        raise TypeError("facts must be TopologyStartupFactsV1")
    profile = _string_value(_attr(_attr(config, "deployment"), "profile"))
    accepted_profile = _string_value(_attr(_attr(config, "sandbox"), "accepted_skill_projection_profile"))
    artifact_digest = _prefixed_digest(
        _attr(capability_manifest, "artifact_manifest_digest"),
        field_name="extension artifact manifest digest",
    )
    configuration_digest = _prefixed_digest(
        _attr(capability_manifest, "extension_configuration_digest"),
        field_name="extension configuration digest",
    )
    capability_digest = _string_value(_attr(capability_manifest, "digest"))
    from deerflow.persistence.bootstrap import get_expected_migration_head

    return TopologyFingerprintV1.create(
        profile=profile,
        tenant_digest=tenant_digest,
        image_digests=facts.image_digests,
        config_digest=facts.config_digest,
        database_schema_ref=facts.database_schema_ref,
        redis_namespace_digest=_prefixed_digest(
            redis_namespace_digest,
            field_name="Redis namespace digest",
        ),
        extension_artifact_digest=artifact_digest,
        extension_configuration_digest=configuration_digest,
        capability_manifest_digest=capability_digest,
        mcp_task_replay_keyring_confirmation_version=(mcp_task_replay_keyring_confirmation_version),
        mcp_task_replay_keyring_confirmation_digest=(mcp_task_replay_keyring_confirmation_digest),
        execution_policy_keyring_confirmation_version=(execution_policy_keyring_confirmation_version),
        execution_policy_keyring_confirmation_digest=(execution_policy_keyring_confirmation_digest),
        migration_head=get_expected_migration_head(),
        accepted_materialization_profile=accepted_profile,
    )


def _aware_timestamp(value: object, *, field_name: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(UTC)


def _timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _parse_timestamp(value: object, *, field_name: str) -> datetime:
    if not isinstance(value, str) or not value or len(value) > 64:
        raise ValueError(f"{field_name} is invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field_name} is invalid") from exc
    return _aware_timestamp(parsed, field_name=field_name)


@dataclass(frozen=True, slots=True)
class ReplicaRegistrationV1:
    """One safe live-replica record stored in the shared topology registry."""

    replica_id: str
    topology_fingerprint: TopologyFingerprintV1
    started_at: datetime
    heartbeat_at: datetime

    def __post_init__(self) -> None:
        if not isinstance(self.replica_id, str) or _REPLICA_ID.fullmatch(self.replica_id) is None:
            raise ValueError("replica_id must be a bounded safe identifier")
        if not isinstance(self.topology_fingerprint, TopologyFingerprintV1):
            raise TypeError("topology_fingerprint must be TopologyFingerprintV1")
        started_at = _aware_timestamp(self.started_at, field_name="started_at")
        heartbeat_at = _aware_timestamp(
            self.heartbeat_at,
            field_name="heartbeat_at",
        )
        if heartbeat_at < started_at:
            raise ValueError("heartbeat_at cannot be before started_at")
        object.__setattr__(self, "started_at", started_at)
        object.__setattr__(self, "heartbeat_at", heartbeat_at)

    def to_dict(self) -> dict[str, object]:
        return {
            "version": 1,
            "replica_id": self.replica_id,
            "topology_fingerprint": self.topology_fingerprint.to_dict(),
            "started_at": _timestamp(self.started_at),
            "heartbeat_at": _timestamp(self.heartbeat_at),
        }

    @classmethod
    def from_dict(cls, value: object) -> ReplicaRegistrationV1:
        fields = {
            "version",
            "replica_id",
            "topology_fingerprint",
            "started_at",
            "heartbeat_at",
        }
        if not isinstance(value, dict) or set(value) != fields or value.get("version") != 1:
            raise ValueError("replica registration fields are invalid")
        try:
            return cls(
                replica_id=value["replica_id"],
                topology_fingerprint=TopologyFingerprintV1.from_dict(value["topology_fingerprint"]),
                started_at=_parse_timestamp(
                    value["started_at"],
                    field_name="started_at",
                ),
                heartbeat_at=_parse_timestamp(
                    value["heartbeat_at"],
                    field_name="heartbeat_at",
                ),
            )
        except (TypeError, ValueError) as exc:
            raise ValueError("replica registration values are invalid") from exc


@runtime_checkable
class TopologyRegistry(Protocol):
    """Shared registration port bound to one process's fingerprint."""

    async def register(self, registration: ReplicaRegistrationV1) -> None: ...

    async def heartbeat(self) -> ReplicaRegistrationV1: ...

    async def compatible_live_replicas(
        self,
    ) -> tuple[ReplicaRegistrationV1, ...]: ...

    async def status(self) -> TopologyStatusV1: ...


TOPOLOGY_ERROR_CODES: Final[frozenset[str]] = frozenset(
    {
        "topology_profile_unsupported",
        "topology_fingerprint_mismatch",
        "topology_replica_count_invalid",
        "topology_dependency_not_shared",
        "topology_extension_not_replica_safe",
        "topology_channel_not_replica_safe",
        "topology_qualification_missing",
        "topology_registration_missing",
        "topology_registration_expired",
    }
)


class TopologyError(RuntimeError):
    """Bounded public topology failure carrying one stable safe code."""

    def __init__(self, code: str) -> None:
        if code not in TOPOLOGY_ERROR_CODES:
            raise ValueError("unknown topology error code")
        self.code = code
        super().__init__(code)


_AIO_SANDBOX_PROVIDERS: Final[frozenset[str]] = frozenset(
    {
        "deerflow.community.aio_sandbox:AioSandboxProvider",
        "deerflow.community.aio_sandbox.provider:AioSandboxProvider",
    }
)
_GOVERNANCE_EXTENSION = (
    "governance",
    "hartmesh-governance-extension",
    "hartmesh_governance_extension:install",
)


def _attr(value: object, name: str, default: object = None) -> object:
    return getattr(value, name, default) if value is not None else default


def _string_value(value: object) -> str:
    raw = getattr(value, "value", value)
    return raw if isinstance(raw, str) else ""


def _channels_enabled(config: object) -> bool:
    connections = _attr(config, "channel_connections")
    if bool(_attr(connections, "enabled", False)):
        return True
    for provider in (
        "slack",
        "telegram",
        "discord",
        "feishu",
        "dingtalk",
        "wechat",
        "wecom",
        "buzz",
    ):
        if bool(_attr(_attr(connections, provider), "enabled", False)):
            return True

    extra = _attr(config, "model_extra", {})
    if not isinstance(extra, Mapping):
        extra = _attr(config, "__pydantic_extra__", {})
    channels = extra.get("channels", {}) if isinstance(extra, Mapping) else {}
    if not isinstance(channels, Mapping):
        return bool(channels)
    return any(isinstance(provider_config, Mapping) and bool(provider_config.get("enabled", False)) for provider_config in channels.values())


def _enabled_plugin_identity(plugin: object) -> tuple[str, str, str] | None:
    if not bool(_attr(plugin, "enabled", True)):
        return None
    return (
        _string_value(_attr(plugin, "name")),
        _string_value(_attr(plugin, "package")),
        _string_value(_attr(plugin, "use")),
    )


def validate_multi_gateway_config(
    config: object,
    *,
    qualification_scopes: frozenset[str],
    webhook_route_enabled: bool,
    tenant_identity: object | None = None,
    qualification_candidate: bool = False,
) -> None:
    """Fail closed unless *config* is the exact qualified V1 support matrix."""

    deployment = _attr(config, "deployment")
    if _string_value(_attr(deployment, "profile")) != MULTI_GATEWAY_PROFILE:
        raise TopologyError("topology_profile_unsupported")
    if tenant_identity is None:
        tenant_id = _attr(deployment, "tenant_id")
        if not isinstance(tenant_id, str) or not tenant_id or tenant_id == "local":
            raise TopologyError("topology_dependency_not_shared")
    elif _RAW_SHA256.fullmatch(_string_value(_attr(tenant_identity, "digest"))) is None:
        raise TopologyError("topology_dependency_not_shared")
    # This checkout contains the qualification machinery, but it does not bundle
    # a passing artifact whose subjects can be authenticated at startup.  An
    # operator-declared scope is report metadata, not release authority.
    if qualification_candidate is not True:
        raise TopologyError("topology_qualification_missing")

    database = _attr(config, "database")
    command_timeout = _attr(database, "command_timeout")
    if (
        _string_value(_attr(database, "backend")) != "postgres"
        or not isinstance(command_timeout, (int, float))
        or isinstance(command_timeout, bool)
        or not 0 < float(command_timeout) < float("inf")
        or _string_value(_attr(_attr(database, "checkpoint_cache"), "type")) != "redis"
    ):
        raise TopologyError("topology_dependency_not_shared")
    checkpointer = _attr(config, "checkpointer")
    if checkpointer is not None:
        if _string_value(_attr(checkpointer, "type")) != "postgres":
            raise TopologyError("topology_dependency_not_shared")
        checkpointer_schema = _string_value(_attr(checkpointer, "postgres_schema", ""))
        database_schema = _string_value(_attr(database, "postgres_schema", ""))
        if checkpointer_schema != database_schema:
            raise TopologyError("topology_dependency_not_shared")

    exact_values = (
        (_attr(_attr(config, "run_events"), "backend"), "db"),
        (_attr(_attr(config, "agent_storage"), "backend"), "db"),
        (_attr(_attr(config, "stream_bridge"), "type"), "redis"),
    )
    if any(_string_value(actual) != expected for actual, expected in exact_values):
        raise TopologyError("topology_dependency_not_shared")
    if not (
        bool(_attr(_attr(config, "scheduler"), "enabled", False))
        and bool(_attr(_attr(config, "scheduler"), "multi_instance", False))
        and bool(_attr(_attr(config, "mcp_tasks"), "enabled", False))
        and bool(
            _attr(
                _attr(config, "run_ownership"),
                "heartbeat_enabled",
                False,
            )
        )
    ):
        raise TopologyError("topology_dependency_not_shared")
    if _string_value(_attr(_attr(config, "dedupe_storage"), "backend")) not in {"auto", "postgres"}:
        raise TopologyError("topology_dependency_not_shared")

    sandbox = _attr(config, "sandbox")
    sandbox_image = _attr(sandbox, "image")
    ownership = _attr(sandbox, "ownership")
    if (
        _string_value(_attr(sandbox, "use")) not in _AIO_SANDBOX_PROVIDERS
        or not isinstance(sandbox_image, str)
        or re.search(r"@sha256:[0-9a-f]{64}\Z", sandbox_image) is None
        or _string_value(_attr(ownership, "type")) != "redis"
        or not _string_value(_attr(sandbox, "provisioner_url"))
        or bool(_attr(sandbox, "provisioner_api_key"))
        or not _string_value(_attr(sandbox, "provisioner_service_account_token_file"))
        or _string_value(_attr(sandbox, "accepted_skill_projection_profile")) != "rwx_verified_copy_v2"
        or _string_value(_attr(sandbox, "accepted_materialization_profile", "disabled")) != "disabled"
    ):
        raise TopologyError("topology_dependency_not_shared")

    if webhook_route_enabled or _channels_enabled(config):
        raise TopologyError("topology_channel_not_replica_safe")

    enabled_plugins = tuple(identity for identity in (_enabled_plugin_identity(plugin) for plugin in (_attr(config, "plugins", ()) or ())) if identity is not None)
    if enabled_plugins not in {(), (_GOVERNANCE_EXTENSION,)}:
        raise TopologyError("topology_extension_not_replica_safe")


def validate_extension_replica_safety(loaded_extensions: object) -> None:
    """Validate Gateway-lifetime extension services for the exact profile."""

    from deerflow_extension_api import ReplicaSafety

    services = _attr(loaded_extensions, "services", ()) or ()
    for entry in services:
        if not isinstance(entry, tuple) or len(entry) != 2:
            raise TopologyError("topology_extension_not_replica_safe")
        service = entry[1]
        try:
            classification = ReplicaSafety(_attr(service, "replica_safety", ReplicaSafety.UNCLASSIFIED))
        except (TypeError, ValueError) as exc:
            raise TopologyError("topology_extension_not_replica_safe") from exc
        if classification in {
            ReplicaSafety.UNCLASSIFIED,
            ReplicaSafety.SINGLE_REPLICA_ONLY,
        }:
            raise TopologyError("topology_extension_not_replica_safe")
        if classification in {
            ReplicaSafety.SHARED_STORE_FENCED,
            ReplicaSafety.SINGLETON_LEASED,
        } and not (_string_value(_attr(service, "replica_safety_health_capability_id")) and _string_value(_attr(service, "replica_safety_fence_evidence_kind"))):
            raise TopologyError("topology_extension_not_replica_safe")


@dataclass(frozen=True, slots=True)
class TopologyStatusV1:
    """Safe readiness/capability projection for one registered replica."""

    replica_id: str | None
    topology_digest: str | None
    ready: bool
    live_compatible_replicas: int
    degraded_replicas: int
    qualification_ready: bool
    reason_code: str | None = None

    def __post_init__(self) -> None:
        if self.replica_id is not None and _REPLICA_ID.fullmatch(self.replica_id) is None:
            raise ValueError("topology status replica_id is invalid")
        if self.topology_digest is not None:
            _require_digest(
                self.topology_digest,
                field_name="topology_digest",
                prefixed=False,
            )
        if type(self.ready) is not bool or type(self.qualification_ready) is not bool:
            raise TypeError("topology status booleans are invalid")
        if type(self.live_compatible_replicas) is not int or not 0 <= self.live_compatible_replicas <= MULTI_GATEWAY_REPLICA_COUNT or type(self.degraded_replicas) is not int or not 0 <= self.degraded_replicas <= MULTI_GATEWAY_REPLICA_COUNT:
            raise ValueError("topology status replica counts are invalid")
        if self.reason_code is not None and self.reason_code not in TOPOLOGY_ERROR_CODES:
            raise ValueError("topology status reason code is invalid")

    def to_dict(self) -> dict[str, object]:
        return {
            "version": 1,
            "profile": MULTI_GATEWAY_PROFILE,
            "replica_id": self.replica_id,
            "topology_digest": self.topology_digest,
            "ready": self.ready,
            "live_compatible_replicas": self.live_compatible_replicas,
            "degraded_replicas": self.degraded_replicas,
            "qualification_ready": self.qualification_ready,
            "reason_code": self.reason_code,
            "execution_recovery": {
                "version": 1,
                "post_dispatch_takeover_available": False,
                "reason_code": ("linearizable_execution_authority_unavailable"),
            },
        }


class TopologyHeartbeatSupervisor:
    """Own one registration heartbeat task with bounded re-registration."""

    def __init__(
        self,
        *,
        registry: TopologyRegistry,
        registration: ReplicaRegistrationV1,
        heartbeat_interval_seconds: float,
    ) -> None:
        if not isinstance(registry, TopologyRegistry):
            raise TypeError("registry must implement TopologyRegistry")
        if not isinstance(registration, ReplicaRegistrationV1):
            raise TypeError("registration must be ReplicaRegistrationV1")
        if not isinstance(heartbeat_interval_seconds, (int, float)) or isinstance(heartbeat_interval_seconds, bool) or not 0 < heartbeat_interval_seconds <= 300:
            raise ValueError("heartbeat_interval_seconds must be in (0, 300]")
        self._registry = registry
        self._registration = registration
        self._interval = float(heartbeat_interval_seconds)
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if self._task is not None:
            return
        await self._registry.register(self._registration)
        self._task = asyncio.create_task(
            self._run(),
            name="topology-registration-heartbeat",
        )

    async def _run(self) -> None:
        while True:
            await asyncio.sleep(self._interval)
            try:
                await self._registry.heartbeat()
            except TopologyError as exc:
                if exc.code in {
                    "topology_registration_expired",
                    "topology_registration_missing",
                }:
                    try:
                        await self._registry.register(self._registration)
                    except TopologyError:
                        pass
            except Exception:  # noqa: BLE001 - readiness reports the authority outage
                pass

    async def status(self) -> TopologyStatusV1:
        return await self._registry.status()

    async def close(self) -> None:
        task = self._task
        self._task = None
        if task is None:
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


@dataclass(slots=True)
class _InMemoryTopologyState:
    registrations: dict[tuple[str, str, str], ReplicaRegistrationV1] = field(default_factory=dict)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


class InMemoryTopologyRegistry:
    """Contract-test adapter only; it is never production qualification proof."""

    def __init__(
        self,
        *,
        live_ttl_seconds: float,
        clock: Callable[[], datetime] | None = None,
        shared_state: _InMemoryTopologyState | None = None,
    ) -> None:
        if not isinstance(live_ttl_seconds, (int, float)) or isinstance(live_ttl_seconds, bool) or not 1 <= live_ttl_seconds <= 3600:
            raise ValueError("live_ttl_seconds must be in [1, 3600]")
        self._live_ttl = timedelta(seconds=float(live_ttl_seconds))
        self._clock = clock or (lambda: datetime.now(UTC))
        self._state = shared_state or _InMemoryTopologyState()
        self._registration: ReplicaRegistrationV1 | None = None

    @staticmethod
    def shared_state() -> _InMemoryTopologyState:
        """Create explicit shared state for contract tests spanning adapters."""

        return _InMemoryTopologyState()

    def _now(self) -> datetime:
        return _aware_timestamp(self._clock(), field_name="topology clock")

    def _is_live(
        self,
        registration: ReplicaRegistrationV1,
        *,
        now: datetime,
    ) -> bool:
        return registration.heartbeat_at >= now - self._live_ttl

    @staticmethod
    def _key(
        registration: ReplicaRegistrationV1,
    ) -> tuple[str, str, str]:
        fingerprint = registration.topology_fingerprint
        return (
            fingerprint.tenant_digest,
            fingerprint.profile,
            registration.replica_id,
        )

    def _same_subject(
        self,
        registration: ReplicaRegistrationV1,
        candidate: ReplicaRegistrationV1,
    ) -> bool:
        left = registration.topology_fingerprint
        right = candidate.topology_fingerprint
        return left.tenant_digest == right.tenant_digest and left.profile == right.profile

    async def register(self, registration: ReplicaRegistrationV1) -> None:
        if not isinstance(registration, ReplicaRegistrationV1):
            raise TypeError("registration must be ReplicaRegistrationV1")
        now = self._now()
        async with self._state.lock:
            live = tuple(item for item in self._state.registrations.values() if self._same_subject(registration, item) and self._is_live(item, now=now))
            if any(item.topology_fingerprint.digest != registration.topology_fingerprint.digest for item in live):
                raise TopologyError("topology_fingerprint_mismatch")
            key = self._key(registration)
            existing = self._state.registrations.get(key)
            if existing is not None and self._is_live(existing, now=now):
                if existing.topology_fingerprint.digest != registration.topology_fingerprint.digest:
                    raise TopologyError("topology_fingerprint_mismatch")
                if existing.started_at != registration.started_at:
                    raise TopologyError("topology_fingerprint_mismatch")
                self._registration = existing
                return
            other_live = tuple(item for item in live if item.replica_id != registration.replica_id)
            if len(other_live) >= MULTI_GATEWAY_REPLICA_COUNT:
                raise TopologyError("topology_replica_count_invalid")
            self._state.registrations[key] = registration
            self._registration = registration

    async def heartbeat(self) -> ReplicaRegistrationV1:
        registration = self._registration
        if registration is None:
            raise TopologyError("topology_registration_missing")
        now = self._now()
        async with self._state.lock:
            key = self._key(registration)
            current = self._state.registrations.get(key)
            if current is None:
                raise TopologyError("topology_registration_missing")
            if current.topology_fingerprint.digest != registration.topology_fingerprint.digest:
                raise TopologyError("topology_fingerprint_mismatch")
            if not self._is_live(current, now=now):
                raise TopologyError("topology_registration_expired")
            updated = ReplicaRegistrationV1(
                replica_id=current.replica_id,
                topology_fingerprint=current.topology_fingerprint,
                started_at=current.started_at,
                heartbeat_at=now,
            )
            self._state.registrations[key] = updated
            self._registration = updated
            return updated

    async def compatible_live_replicas(
        self,
    ) -> tuple[ReplicaRegistrationV1, ...]:
        registration = self._registration
        if registration is None:
            raise TopologyError("topology_registration_missing")
        now = self._now()
        async with self._state.lock:
            live = tuple(item for item in self._state.registrations.values() if self._same_subject(registration, item) and self._is_live(item, now=now))
            if any(item.topology_fingerprint.digest != registration.topology_fingerprint.digest for item in live):
                raise TopologyError("topology_fingerprint_mismatch")
            return tuple(sorted(live, key=lambda item: item.replica_id))

    async def status(self) -> TopologyStatusV1:
        registration = self._registration
        if registration is None:
            return TopologyStatusV1(
                replica_id=None,
                topology_digest=None,
                ready=False,
                live_compatible_replicas=0,
                degraded_replicas=MULTI_GATEWAY_REPLICA_COUNT,
                qualification_ready=False,
                reason_code="topology_registration_missing",
            )
        now = self._now()
        try:
            live = await self.compatible_live_replicas()
        except TopologyError as exc:
            return TopologyStatusV1(
                replica_id=registration.replica_id,
                topology_digest=registration.topology_fingerprint.digest,
                ready=False,
                live_compatible_replicas=0,
                degraded_replicas=MULTI_GATEWAY_REPLICA_COUNT,
                qualification_ready=False,
                reason_code=exc.code,
            )
        own = next(
            (item for item in live if item.replica_id == registration.replica_id),
            None,
        )
        own_ready = own is not None and self._is_live(own, now=now)
        count = min(len(live), MULTI_GATEWAY_REPLICA_COUNT)
        return TopologyStatusV1(
            replica_id=registration.replica_id,
            topology_digest=registration.topology_fingerprint.digest,
            ready=own_ready,
            live_compatible_replicas=count,
            degraded_replicas=MULTI_GATEWAY_REPLICA_COUNT - count,
            qualification_ready=(own_ready and count == MULTI_GATEWAY_REPLICA_COUNT),
            reason_code=(None if own_ready else "topology_registration_expired"),
        )


def _bounded_text(value: object, *, field_name: str, limit: int) -> str:
    if not isinstance(value, str) or not value or len(value.encode("utf-8")) > limit or any(ord(character) < 32 or ord(character) == 127 for character in value):
        raise ValueError(f"topology inventory {field_name} is invalid")
    return value


def _safe_identifiers(
    value: object,
    *,
    field_name: str,
    allow_empty: bool = False,
    pattern: re.Pattern[str] = _SAFE_FIELD,
) -> tuple[str, ...]:
    if not isinstance(value, list) or (not value and not allow_empty):
        qualifier = "a list" if allow_empty else "a nonempty list"
        raise ValueError(f"topology inventory {field_name} must be {qualifier}")
    if len(value) > 64 or any(not isinstance(item, str) or pattern.fullmatch(item) is None for item in value):
        raise ValueError(f"topology inventory {field_name} is invalid")
    normalized = tuple(value)
    if len(normalized) != len(set(normalized)):
        raise ValueError(f"topology inventory {field_name} contains duplicates")
    return normalized


@dataclass(frozen=True, slots=True)
class TopologyInventoryDependencyV1:
    """One classified mutable dependency used by the supported surface."""

    id: str
    classification: TopologyInventoryClassification
    authority: str
    required_adapter: str
    config_fields: tuple[str, ...]
    service_registrations: tuple[str, ...]
    runtime_state_fields: tuple[str, ...]
    requirement: str

    @classmethod
    def from_dict(cls, value: object) -> TopologyInventoryDependencyV1:
        fields = {
            "id",
            "classification",
            "authority",
            "required_adapter",
            "config_fields",
            "service_registrations",
            "runtime_state_fields",
            "requirement",
        }
        if not isinstance(value, dict) or set(value) != fields:
            raise ValueError("topology inventory dependency fields are invalid")
        identifier = value["id"]
        classification = value["classification"]
        if not isinstance(identifier, str) or _SAFE_ID.fullmatch(identifier) is None:
            raise ValueError("topology inventory dependency id is invalid")
        if classification not in TOPOLOGY_INVENTORY_CLASSIFICATIONS:
            raise ValueError("topology inventory classification is invalid")
        return cls(
            id=identifier,
            classification=classification,
            authority=_bounded_text(
                value["authority"],
                field_name="authority",
                limit=_TEXT_LIMITS["authority"],
            ),
            required_adapter=_bounded_text(
                value["required_adapter"],
                field_name="required_adapter",
                limit=_TEXT_LIMITS["required_adapter"],
            ),
            config_fields=_safe_identifiers(
                value["config_fields"],
                field_name="config_fields",
                pattern=_SAFE_CONFIG_FIELD,
            ),
            service_registrations=_safe_identifiers(
                value["service_registrations"],
                field_name="service_registrations",
            ),
            runtime_state_fields=_safe_identifiers(
                value["runtime_state_fields"],
                field_name="runtime_state_fields",
                allow_empty=True,
            ),
            requirement=_bounded_text(
                value["requirement"],
                field_name="requirement",
                limit=_TEXT_LIMITS["requirement"],
            ),
        )


@dataclass(frozen=True, slots=True)
class TopologyInventoryV1:
    """Strict checked-in audit for one exact deployment profile."""

    profile: Literal["durable_two_gateway_v1"]
    scope: Literal["durable_two_gateway_v1_postgres_redis_aio_rwx"]
    replica_count: Literal[2]
    dependencies: tuple[TopologyInventoryDependencyV1, ...]
    schema_version: Literal[1] = 1

    @classmethod
    def from_dict(cls, value: object) -> TopologyInventoryV1:
        fields = {
            "schema_version",
            "profile",
            "scope",
            "replica_count",
            "dependencies",
        }
        if not isinstance(value, dict) or set(value) != fields:
            raise ValueError("topology inventory fields are invalid")
        if value["schema_version"] != 1:
            raise ValueError("topology inventory schema version is invalid")
        if value["profile"] != MULTI_GATEWAY_PROFILE:
            raise ValueError("topology inventory profile is invalid")
        if value["scope"] != MULTI_GATEWAY_QUALIFICATION_SCOPE:
            raise ValueError("topology inventory scope is invalid")
        if value["replica_count"] != MULTI_GATEWAY_REPLICA_COUNT:
            raise ValueError("topology inventory replica count is invalid")
        raw_dependencies = value["dependencies"]
        if not isinstance(raw_dependencies, list) or not raw_dependencies or len(raw_dependencies) > _MAX_DEPENDENCIES:
            raise ValueError("topology inventory dependencies are invalid")
        dependencies = tuple(TopologyInventoryDependencyV1.from_dict(item) for item in raw_dependencies)
        identifiers = tuple(item.id for item in dependencies)
        if len(identifiers) != len(set(identifiers)):
            raise ValueError("topology inventory dependency ids are duplicated")
        return cls(
            profile=MULTI_GATEWAY_PROFILE,
            scope=MULTI_GATEWAY_QUALIFICATION_SCOPE,
            replica_count=MULTI_GATEWAY_REPLICA_COUNT,
            dependencies=dependencies,
        )


class TopologyServiceRegistry:
    """Canonical Gateway construction registrations for topology auditing."""

    def __init__(self) -> None:
        self._registrations: dict[str, str] = {}

    def register(self, name: str, *, construction_ref: str) -> None:
        if _SAFE_FIELD.fullmatch(name) is None:
            raise ValueError("topology service registration name is invalid")
        if not isinstance(construction_ref, str) or not construction_ref or len(construction_ref.encode("utf-8")) > 256:
            raise ValueError(
                "topology service construction reference is invalid",
            )
        if name in self._registrations:
            raise ValueError("topology service registration is duplicated")
        self._registrations[name] = construction_ref

    @property
    def names(self) -> frozenset[str]:
        return frozenset(self._registrations)

    def validate_inventory(self, inventory: TopologyInventoryV1) -> None:
        expected = {service for dependency in inventory.dependencies for service in dependency.service_registrations}
        if self.names != expected:
            raise TopologyError("topology_dependency_not_shared")


def _reject_duplicate_fields(
    pairs: list[tuple[str, object]],
) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("topology inventory contains duplicate fields")
        value[key] = item
    return value


def load_topology_inventory(path: str | Path) -> TopologyInventoryV1:
    """Load one bounded canonical inventory document from a trusted checkout."""

    inventory_path = Path(path)
    payload = inventory_path.read_bytes()
    if not payload or len(payload) > _MAX_INVENTORY_BYTES:
        raise ValueError("topology inventory must be bounded nonempty JSON")
    try:
        value = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_reject_duplicate_fields,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("topology inventory must be valid UTF-8 JSON") from exc
    return TopologyInventoryV1.from_dict(value)


def default_topology_inventory_path() -> Path:
    """Resolve the checked-in inventory in source and packaged containers."""

    return Path(__file__).resolve().parents[5] / "contracts" / "deployment" / "durable_two_gateway_v1.topology.json"


def multi_gateway_run_store_ready(run_store: object) -> bool:
    """Return whether the live run adapter uses the qualified lease clock."""

    # Importing the enum here would make this deployment/config module recurse
    # through ``deerflow.runtime`` while application config is still loading.
    # Normalize the typed StrEnum's public value at this boundary instead.
    return (
        _string_value(
            getattr(run_store, "lease_clock_authority", None),
        )
        == "database_v1"
    )


def validate_multi_gateway_run_store(run_store: object) -> None:
    """Reject an exact-two live adapter without database-owned lease time."""

    if not multi_gateway_run_store_ready(run_store):
        raise TopologyError("topology_dependency_not_shared")


def validate_topology_inventory_runtime_state(
    state: object,
    *,
    path: str | Path | None = None,
) -> TopologyInventoryV1:
    """Fail startup unless every inventoried runtime dependency was built."""

    inventory = load_topology_inventory(
        default_topology_inventory_path() if path is None else path,
    )
    required_fields = {field_name for dependency in inventory.dependencies for field_name in dependency.runtime_state_fields}
    if any(not hasattr(state, field_name) for field_name in required_fields):
        raise TopologyError("topology_dependency_not_shared")
    service_registry = getattr(state, "topology_service_registry", None)
    if not isinstance(service_registry, TopologyServiceRegistry):
        raise TopologyError("topology_dependency_not_shared")
    service_registry.validate_inventory(inventory)
    validate_multi_gateway_run_store(getattr(state, "run_store", None))
    return inventory


__all__ = [
    "DeploymentProfile",
    "InMemoryTopologyRegistry",
    "MULTI_GATEWAY_ADDITIONAL_CONFIG_FIELDS",
    "MULTI_GATEWAY_PROFILE",
    "MULTI_GATEWAY_QUALIFICATION_SCOPE",
    "MULTI_GATEWAY_REPLICA_COUNT",
    "ReplicaRegistrationV1",
    "TOPOLOGY_ERROR_CODES",
    "TOPOLOGY_INVENTORY_CLASSIFICATIONS",
    "TopologyError",
    "TopologyFingerprintV1",
    "TopologyInventoryClassification",
    "TopologyInventoryDependencyV1",
    "TopologyInventoryV1",
    "TopologyRegistry",
    "TopologyStatusV1",
    "TopologyServiceRegistry",
    "load_topology_inventory",
    "default_topology_inventory_path",
    "multi_gateway_run_store_ready",
    "validate_multi_gateway_run_store",
    "validate_topology_inventory_runtime_state",
]
