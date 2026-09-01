"""Host-owned tenant projection and deployment policy for Honcho memory."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from typing import Any, Literal
from urllib.parse import urlsplit

from deerflow.agents.memory.backends.honcho.config import (
    HARTMESH_TENANT_CONFIG_KEY,
    HonchoConfig,
    HonchoConfigError,
    HonchoIdentityResolver,
)
from deerflow.runtime.tenant_identity import TenantIdentityV1, TenantSubsystem

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class HonchoTenantProjectionV1:
    """JSON-safe host projection consumed by the portable backend."""

    version: Literal[1]
    tenant_public_ref: str
    tenant_digest: str
    workspace_namespace: str
    isolation_mode: Literal["tenant_user", "local_explicit_shared"]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def _profile_value(profile: object) -> str:
    value = getattr(profile, "value", profile)
    if not isinstance(value, str):
        raise TypeError("deployment_profile must be a string or string enum")
    return value


def _projection(
    tenant_identity: TenantIdentityV1,
    *,
    isolation_mode: Literal["tenant_user", "local_explicit_shared"],
) -> HonchoTenantProjectionV1:
    if not isinstance(tenant_identity, TenantIdentityV1):
        raise TypeError("tenant_identity must be TenantIdentityV1")
    namespace = tenant_identity.namespace(TenantSubsystem.HONCHO)
    # Project 04's provider identifier is already grammar-safe. A trailing
    # delimiter turns it into a composition namespace while retaining enough
    # of Honcho's 100-character budget for the collision-resistant user ID.
    return HonchoTenantProjectionV1(
        version=1,
        tenant_public_ref=namespace.metadata_ref,
        tenant_digest=tenant_identity.digest,
        workspace_namespace=f"{namespace.key_prefix}-",
        isolation_mode=isolation_mode,
    )


def project_honcho_backend_config(
    backend_config: Mapping[str, Any] | None,
    *,
    tenant_identity: TenantIdentityV1,
    deployment_profile: object,
) -> dict[str, Any]:
    """Inject the server tenant and enforce profile-specific override policy.

    Caller/file input can never supply ``_hartmesh_tenant``. The raw value is
    discarded without rendering it into logs, and the immutable process
    identity supplies the replacement.
    """

    config = dict(backend_config or {})
    if HARTMESH_TENANT_CONFIG_KEY in config:
        config.pop(HARTMESH_TENANT_CONFIG_KEY, None)
        logger.warning("Ignoring caller-configured reserved Honcho tenant projection; the Gateway injects its server-owned value")

    profile = _profile_value(deployment_profile)
    if profile not in {"local_development", "durable_production"}:
        raise ValueError("unknown deployment profile")
    allow_local_shared_value = config.get("allow_local_shared_workspaces", False)
    if type(allow_local_shared_value) is not bool:
        raise HonchoConfigError(
            "honcho_shared_workspace_forbidden",
            "allow_local_shared_workspaces must be an explicit boolean",
        )
    if "workspace_prefix" in config:
        logger.warning("Honcho workspace_prefix is deprecated; the Gateway validates it against the server-owned tenant namespace")

    tenant_user = _projection(tenant_identity, isolation_mode="tenant_user")
    baseline = {
        **config,
        "workspace_overrides": {},
        HARTMESH_TENANT_CONFIG_KEY: tenant_user.to_dict(),
    }
    baseline_config = HonchoConfig.from_backend_config(baseline)
    baseline_resolver = HonchoIdentityResolver(baseline_config)

    raw_overrides = config.get("workspace_overrides", {})
    if not isinstance(raw_overrides, Mapping):
        raise HonchoConfigError(
            "honcho_workspace_namespace_conflict",
            "workspace_overrides must be a mapping",
        )
    custom_override = any(str(workspace) != baseline_resolver.derived_workspace(str(user_id)) for user_id, workspace in raw_overrides.items())
    allow_local_shared = allow_local_shared_value

    isolation_mode: Literal["tenant_user", "local_explicit_shared"] = "tenant_user"
    if custom_override:
        if profile != "local_development" or not allow_local_shared:
            raise HonchoConfigError(
                "honcho_shared_workspace_forbidden",
                "shared or namespace-escaping workspaces are forbidden outside the explicit local-development opt-in",
            )
        isolation_mode = "local_explicit_shared"
        logger.warning("Honcho local explicit shared-workspace mode is enabled; workspace-scoped search results may be visible to every configured sharing user")

    configured_scheme = urlsplit(str(config.get("base_url", "http://localhost:8000"))).scheme.lower()
    if profile == "durable_production" and config.get("api_key") and configured_scheme == "http":
        raise HonchoConfigError(
            "honcho_tenant_projection_invalid",
            "durable production requires HTTPS when a Honcho API key is configured",
        )

    projected = {
        **config,
        HARTMESH_TENANT_CONFIG_KEY: _projection(
            tenant_identity,
            isolation_mode=isolation_mode,
        ).to_dict(),
    }
    # Defense in depth: parse the exact dictionary that will cross the portable
    # backend boundary before returning it to the manager factory.
    HonchoConfig.from_backend_config(projected)
    return projected


__all__ = [
    "HARTMESH_TENANT_CONFIG_KEY",
    "HonchoTenantProjectionV1",
    "project_honcho_backend_config",
]
