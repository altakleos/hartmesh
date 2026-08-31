"""Portable Honcho configuration and tenant-aware identity resolution.

The backend receives HartMesh tenancy only as a strict JSON-safe dictionary
under :data:`HARTMESH_TENANT_CONFIG_KEY`. This module deliberately imports no
DeerFlow host types; the host owns projection construction and deployment-
profile policy.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from math import isfinite
from typing import Any, Literal
from urllib.parse import urlsplit

HARTMESH_TENANT_CONFIG_KEY = "_hartmesh_tenant"
HONCHO_ID_MAX_LENGTH = 100
HONCHO_TEXT_CHAR_MAX = 100_000
STABLE_ID_DIGEST_HEX_LENGTH = 16

_ID_RE = re.compile(r"[^a-zA-Z0-9_-]+")
_VALID_ID_RE = re.compile(r"^[a-zA-Z0-9_-]+$", re.ASCII)
_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$", re.ASCII)
_PUBLIC_REF_RE = re.compile(r"^tenant-[0-9a-f]{16}$", re.ASCII)
_TENANT_FIELDS = frozenset(
    {
        "version",
        "tenant_public_ref",
        "tenant_digest",
        "workspace_namespace",
        "isolation_mode",
    }
)

HonchoIsolationMode = Literal["tenant_user", "local_explicit_shared"]


class HonchoConfigError(ValueError):
    """Stable configuration failure safe to surface during startup."""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(f"{code}: {message}")


def sanitize_id(raw: str) -> str:
    """Map arbitrary text onto Honcho's ID grammar with a readable cap."""

    return _ID_RE.sub("-", str(raw)).strip("-")[:64]


def stable_id(raw: str, *, max_length: int = 64) -> str:
    """Return a readable ID with a collision-resistant SHA-256 suffix.

    The digest is reserved before the readable prefix is truncated, so even a
    nearly exhausted provider budget cannot trim away collision resistance.
    """

    if type(max_length) is not int or max_length < STABLE_ID_DIGEST_HEX_LENGTH:
        raise ValueError("Honcho stable ID budget is too small for its digest")
    digest = hashlib.sha256(str(raw).encode("utf-8")).hexdigest()[:STABLE_ID_DIGEST_HEX_LENGTH]
    readable_budget = max_length - STABLE_ID_DIGEST_HEX_LENGTH - 1
    readable = sanitize_id(str(raw))[: max(0, readable_budget)].rstrip("-")
    return f"{readable}-{digest}" if readable else digest


def _validate_id(value: object, *, field_name: str, code: str) -> str:
    if not isinstance(value, str) or not value or len(value) > HONCHO_ID_MAX_LENGTH or _VALID_ID_RE.fullmatch(value) is None:
        raise HonchoConfigError(
            code,
            f"{field_name} must match Honcho's ID grammar and be at most {HONCHO_ID_MAX_LENGTH} characters",
        )
    return value


def _parse_override_map(cfg: Mapping[str, Any], key: str) -> dict[str, str]:
    raw = cfg.get(key, {})
    if not isinstance(raw, Mapping):
        raise HonchoConfigError(
            "honcho_tenant_projection_invalid",
            f"{key} must be a mapping",
        )
    out: dict[str, str] = {}
    for k, v in raw.items():
        if v is None or not str(v).strip():
            raise ValueError(f"Honcho backend: {key} contains an empty value; remove the entry or set a non-empty id.")
        if not isinstance(k, str) or not k:
            raise ValueError(f"Honcho backend: {key} keys must be non-empty user identifiers")
        value = str(v)
        _validate_id(
            value,
            field_name=f"{key} value",
            code="honcho_identity_collision" if key == "user_peer_overrides" else "honcho_workspace_namespace_conflict",
        )
        out[str(k)] = value
    return out


def _parse_bool(cfg: Mapping[str, Any], key: str, *, default: bool = False) -> bool:
    value = cfg.get(key, default)
    if type(value) is not bool:
        raise ValueError(f"Honcho backend: {key} must be a boolean")
    return value


@dataclass(frozen=True)
class HonchoTenantConfig:
    """Validated plain projection supplied by the DeerFlow host."""

    version: int
    tenant_public_ref: str
    tenant_digest: str
    workspace_namespace: str
    isolation_mode: HonchoIsolationMode

    def __post_init__(self) -> None:
        invalid = type(self.version) is not int or self.version != 1
        invalid = invalid or _PUBLIC_REF_RE.fullmatch(self.tenant_public_ref) is None
        invalid = invalid or _DIGEST_RE.fullmatch(self.tenant_digest) is None
        invalid = invalid or self.tenant_public_ref != f"tenant-{self.tenant_digest[:16]}"
        invalid = invalid or self.isolation_mode not in ("tenant_user", "local_explicit_shared")
        invalid = invalid or self.workspace_namespace != f"hm-v1-{self.tenant_digest[:16]}-honcho-"
        try:
            _validate_id(
                self.workspace_namespace,
                field_name="workspace_namespace",
                code="honcho_tenant_projection_invalid",
            )
        except HonchoConfigError:
            invalid = True
        invalid = invalid or not self.workspace_namespace.endswith("-")
        invalid = invalid or len(self.workspace_namespace) > HONCHO_ID_MAX_LENGTH - STABLE_ID_DIGEST_HEX_LENGTH
        if invalid:
            raise HonchoConfigError(
                "honcho_tenant_projection_invalid",
                "reserved tenant projection fails its version, digest, namespace, or isolation contract",
            )

    @classmethod
    def from_mapping(cls, value: object) -> HonchoTenantConfig:
        if not isinstance(value, Mapping) or frozenset(value) != _TENANT_FIELDS:
            raise HonchoConfigError(
                "honcho_tenant_projection_invalid",
                "reserved tenant projection has unknown or missing fields",
            )
        try:
            return cls(
                version=value["version"],  # type: ignore[arg-type]
                tenant_public_ref=value["tenant_public_ref"],  # type: ignore[arg-type]
                tenant_digest=value["tenant_digest"],  # type: ignore[arg-type]
                workspace_namespace=value["workspace_namespace"],  # type: ignore[arg-type]
                isolation_mode=value["isolation_mode"],  # type: ignore[arg-type]
            )
        except (KeyError, TypeError, ValueError) as exc:
            if isinstance(exc, HonchoConfigError):
                raise
            raise HonchoConfigError(
                "honcho_tenant_projection_invalid",
                "reserved tenant projection values are invalid",
            ) from None


@dataclass
class HonchoConfig:
    base_url: str = "http://localhost:8000"
    api_key: str | None = None
    workspace_prefix: str = "deerflow-u-"
    workspace_overrides: dict[str, str] = field(default_factory=dict)
    user_peer_overrides: dict[str, str] = field(default_factory=dict)
    assistant_peer: str = "deerflow"
    timeout_seconds: float = 10.0
    connect_timeout_seconds: float = 3.0
    message_char_limit: int = 8000
    max_injection_chars: int = 6000
    allow_insecure_http: bool = False
    allow_local_shared_workspaces: bool = False
    read_fail_closed: bool = False
    storage_path: str = ""
    tenant: HonchoTenantConfig | None = None

    def __post_init__(self) -> None:
        if not isfinite(self.timeout_seconds) or self.timeout_seconds <= 0:
            raise ValueError("Honcho backend: timeout_seconds must be a finite value > 0")
        if not isfinite(self.connect_timeout_seconds) or self.connect_timeout_seconds <= 0:
            raise ValueError("Honcho backend: connect_timeout_seconds must be a finite value > 0")
        if type(self.message_char_limit) is not int or not 1 <= self.message_char_limit <= HONCHO_TEXT_CHAR_MAX:
            raise ValueError(f"Honcho backend: message_char_limit must be an integer from 1 through {HONCHO_TEXT_CHAR_MAX}")
        if type(self.max_injection_chars) is not int or not 1 <= self.max_injection_chars <= HONCHO_TEXT_CHAR_MAX:
            raise ValueError(f"Honcho backend: max_injection_chars must be an integer from 1 through {HONCHO_TEXT_CHAR_MAX}")

        parsed_url = urlsplit(self.base_url)
        if parsed_url.scheme not in {"http", "https"} or not parsed_url.hostname or parsed_url.username is not None or parsed_url.password is not None or parsed_url.query or parsed_url.fragment:
            raise ValueError("Honcho backend: base_url must be an http(s) URL without credentials, query, or fragment")
        if self.api_key and parsed_url.scheme == "http" and not self.allow_insecure_http:
            raise ValueError("Honcho backend: api_key over plain http requires backend_config.allow_insecure_http: true (the key would be sent unencrypted). Use https, or set the opt-in for local development.")

        _validate_id(
            self.workspace_prefix,
            field_name="workspace_prefix",
            code="honcho_workspace_namespace_conflict",
        )
        if len(self.workspace_prefix) > HONCHO_ID_MAX_LENGTH - STABLE_ID_DIGEST_HEX_LENGTH:
            raise HonchoConfigError(
                "honcho_workspace_namespace_conflict",
                "workspace_prefix leaves no room for a collision-resistant user ID",
            )
        _validate_id(
            self.assistant_peer,
            field_name="assistant_peer",
            code="honcho_identity_collision",
        )
        for value in self.workspace_overrides.values():
            _validate_id(
                value,
                field_name="workspace override",
                code="honcho_workspace_namespace_conflict",
            )
        for value in self.user_peer_overrides.values():
            _validate_id(
                value,
                field_name="user peer override",
                code="honcho_identity_collision",
            )

        self._validate_identity_policy()

    def _validate_identity_policy(self) -> None:
        if len(set(self.user_peer_overrides.values())) != len(self.user_peer_overrides):
            raise HonchoConfigError(
                "honcho_identity_collision",
                "configured user peer overrides must be unique",
            )

        resolver = HonchoIdentityResolver(self)
        assistant = resolver.assistant_peer()
        if assistant in self.user_peer_overrides.values():
            raise HonchoConfigError(
                "honcho_identity_collision",
                "a user peer override collides with the reserved assistant peer",
            )

        if self.tenant is None:
            return

        expected_user_prefix = f"hm-u-{self.tenant.tenant_digest[:12]}-"
        if any(not value.startswith(expected_user_prefix) for value in self.user_peer_overrides.values()):
            raise HonchoConfigError(
                "honcho_identity_collision",
                "tenant-scoped user peer overrides must retain the reserved tenant/user prefix",
            )

        if self.tenant.isolation_mode == "local_explicit_shared":
            if not self.allow_local_shared_workspaces or not self.workspace_overrides:
                raise HonchoConfigError(
                    "honcho_shared_workspace_forbidden",
                    "local shared isolation mode requires allow_local_shared_workspaces and a workspace override",
                )
            return

        for user_id, workspace in self.workspace_overrides.items():
            if workspace != resolver.derived_workspace(user_id):
                raise HonchoConfigError(
                    "honcho_shared_workspace_forbidden",
                    "tenant-user isolation forbids shared or namespace-escaping workspace overrides",
                )
        if len(set(self.workspace_overrides.values())) != len(self.workspace_overrides):
            raise HonchoConfigError(
                "honcho_identity_collision",
                "tenant-user workspace overrides must remain unique",
            )

    @classmethod
    def from_backend_config(cls, backend_config: Mapping[str, Any] | None) -> HonchoConfig:
        cfg = dict(backend_config or {})
        failure_policy = cfg.get("failure_policy") or {}
        if not isinstance(failure_policy, Mapping):
            raise ValueError("Honcho backend: failure_policy must be a mapping")
        base_url = str(cfg.get("base_url", "http://localhost:8000")).rstrip("/")
        tenant_raw = cfg.get(HARTMESH_TENANT_CONFIG_KEY)
        tenant = HonchoTenantConfig.from_mapping(tenant_raw) if tenant_raw is not None else None
        configured_prefix = cfg.get("workspace_prefix")
        if tenant is not None:
            if configured_prefix is not None and str(configured_prefix) != tenant.workspace_namespace:
                raise HonchoConfigError(
                    "honcho_workspace_namespace_conflict",
                    "deprecated workspace_prefix conflicts with the host-derived tenant namespace",
                )
            workspace_prefix = tenant.workspace_namespace
        else:
            workspace_prefix = str(configured_prefix if configured_prefix is not None else "deerflow-u-")

        return cls(
            base_url=base_url,
            api_key=cfg.get("api_key") or None,
            workspace_prefix=workspace_prefix,
            workspace_overrides=_parse_override_map(cfg, "workspace_overrides"),
            user_peer_overrides=_parse_override_map(cfg, "user_peer_overrides"),
            assistant_peer=str(cfg.get("assistant_peer", "deerflow")),
            timeout_seconds=float(cfg.get("timeout_seconds", 10.0)),
            connect_timeout_seconds=float(cfg.get("connect_timeout_seconds", 3.0)),
            message_char_limit=int(cfg.get("message_char_limit", 8000)),
            max_injection_chars=int(cfg.get("max_injection_chars", 6000)),
            allow_insecure_http=_parse_bool(cfg, "allow_insecure_http"),
            allow_local_shared_workspaces=_parse_bool(
                cfg,
                "allow_local_shared_workspaces",
            ),
            read_fail_closed=str(failure_policy.get("read", "")).lower() == "fail_closed",
            storage_path=str(cfg.get("storage_path") or ""),
            tenant=tenant,
        )


class HonchoIdentityResolver:
    """Single ownership point for every workspace, peer, and session ID."""

    def __init__(self, config: HonchoConfig) -> None:
        self._config = config

    def derived_workspace(self, user_id: str) -> str:
        if not user_id:
            raise ValueError("user_id must be non-empty")
        budget = HONCHO_ID_MAX_LENGTH - len(self._config.workspace_prefix)
        return f"{self._config.workspace_prefix}{stable_id(user_id, max_length=budget)}"

    def workspace(self, user_id: str | None) -> str | None:
        if not user_id:
            return None
        return self._config.workspace_overrides.get(user_id) or self.derived_workspace(user_id)

    def user_peer(self, user_id: str) -> str:
        if not user_id:
            raise ValueError("user_id must be non-empty")
        override = self._config.user_peer_overrides.get(user_id)
        if override is not None:
            return override
        if self._config.tenant is None:
            return stable_id(user_id)
        prefix = f"hm-u-{self._config.tenant.tenant_digest[:12]}-"
        return f"{prefix}{stable_id(user_id, max_length=HONCHO_ID_MAX_LENGTH - len(prefix))}"

    def assistant_peer(self) -> str:
        if self._config.tenant is None:
            return self._config.assistant_peer
        prefix = f"hm-a-{self._config.tenant.tenant_digest[:12]}-"
        return f"{prefix}{stable_id(self._config.assistant_peer, max_length=HONCHO_ID_MAX_LENGTH - len(prefix))}"

    def session(self, thread_id: str) -> str:
        if not thread_id:
            raise ValueError("thread_id must be non-empty")
        if self._config.tenant is None:
            prefix = "df-"
        else:
            prefix = f"hm-s-{self._config.tenant.tenant_digest[:12]}-"
        return f"{prefix}{stable_id(thread_id, max_length=HONCHO_ID_MAX_LENGTH - len(prefix))}"

    def safe_diagnostics(self) -> Mapping[str, object]:
        tenant = self._config.tenant
        return {
            "version": 1,
            "tenant_public_ref": tenant.tenant_public_ref if tenant is not None else None,
            "tenant_digest_prefix": tenant.tenant_digest[:12] if tenant is not None else None,
            "workspace_namespace": tenant.workspace_namespace if tenant is not None else None,
            "isolation_mode": tenant.isolation_mode if tenant is not None else "legacy_local_unscoped",
        }


__all__ = [
    "HARTMESH_TENANT_CONFIG_KEY",
    "HONCHO_ID_MAX_LENGTH",
    "HONCHO_TEXT_CHAR_MAX",
    "HonchoConfig",
    "HonchoConfigError",
    "HonchoIdentityResolver",
    "HonchoIsolationMode",
    "HonchoTenantConfig",
    "STABLE_ID_DIGEST_HEX_LENGTH",
    "sanitize_id",
    "stable_id",
]
