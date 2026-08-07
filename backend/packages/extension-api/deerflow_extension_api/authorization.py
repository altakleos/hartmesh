"""Host-independent authorization contracts for DeerFlow capabilities."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol, runtime_checkable

AUTHORIZATION_PROVIDER_CAPABILITY_API_VERSION = "1.0"
AUTHORIZATION_PROVIDER_KIND = "authorization_provider"


@dataclass
class Principal:
    """Actor resolved from trusted host identity context."""

    user_id: str | None = None
    role: str | None = None
    oauth_provider: str | None = None
    oauth_id: str | None = None
    channel_user_id: str | None = None
    is_internal: bool = False
    attributes: dict[str, Any] = field(default_factory=dict)


@dataclass
class AuthzRequest:
    """Context supplied for one authorization decision."""

    principal: Principal
    resource: str
    action: str
    target: str
    context: dict[str, Any] = field(default_factory=dict)


@dataclass
class AuthzReason:
    """Structured reason for an allow or deny decision."""

    code: str
    message: str = ""


@dataclass
class AuthzDecision:
    """Provider allow or deny verdict."""

    allow: bool
    reasons: list[AuthzReason] = field(default_factory=list)
    policy_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@runtime_checkable
class AuthorizationProvider(Protocol):
    """Host-independent fine-grained authorization provider contract."""

    name: str

    def authorize(self, request: AuthzRequest) -> AuthzDecision: ...

    async def aauthorize(self, request: AuthzRequest) -> AuthzDecision: ...

    def filter_resources(
        self,
        principal: Principal,
        resource_type: str,
        candidates: list[str],
    ) -> list[str]: ...


@dataclass(frozen=True)
class AuthorizationProviderFactory:
    """One authoritative authorization-provider factory contribution."""

    contribution_id: str
    capability_api_version: str
    factory: Callable[[], AuthorizationProvider]
    kind: Literal["authorization_provider"]

    def __post_init__(self) -> None:
        if not isinstance(self.contribution_id, str) or not self.contribution_id.strip():
            raise ValueError("authorization provider contribution_id must be a non-empty string")
        if self.capability_api_version != AUTHORIZATION_PROVIDER_CAPABILITY_API_VERSION:
            raise ValueError(f"unsupported authorization provider capability API version {self.capability_api_version!r}; expected {AUTHORIZATION_PROVIDER_CAPABILITY_API_VERSION!r}")
        if self.kind != AUTHORIZATION_PROVIDER_KIND:
            raise ValueError(f"authorization provider factory kind must be {AUTHORIZATION_PROVIDER_KIND!r}")
        if not callable(self.factory):
            raise TypeError("authorization provider factory must be callable")
