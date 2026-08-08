"""Host-independent authorization contracts for DeerFlow capabilities."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal, Protocol, runtime_checkable

from deerflow_extension_api.health import CapabilityHealthProbe
from deerflow_extension_api.identity import InvocationIdentityV1

if TYPE_CHECKING:
    from deerflow_extension_api.contributors import TrustedRunContextV1

AUTHORIZATION_PROVIDER_CAPABILITY_API_VERSION = "1.0"
AUTHORIZATION_PROVIDER_KIND = "authorization_provider"


@dataclass(frozen=True)
class Principal:
    """Compatibility authorization principal backed by split identity.

    New hosts construct this record with :meth:`from_identity`. ``is_internal``
    is retained only for older providers and means that the *effective subject*
    is a service; an acting service never promotes a represented human.
    """

    user_id: str | None = None
    role: str | None = None
    oauth_provider: str | None = None
    oauth_id: str | None = None
    channel_user_id: str | None = None
    is_internal: bool = False
    attributes: Mapping[str, Any] = field(default_factory=dict)
    identity: InvocationIdentityV1 | None = None

    def __post_init__(self) -> None:
        from deerflow_extension_api.identity import _freeze_attributes

        identity = self.identity
        if identity is not None:
            if not isinstance(identity, InvocationIdentityV1):
                raise TypeError("identity must be InvocationIdentityV1 or None")
            subject = identity.effective_subject
            object.__setattr__(self, "user_id", subject.subject_id)
            object.__setattr__(self, "role", subject.role)
            object.__setattr__(self, "oauth_provider", subject.oauth_provider)
            object.__setattr__(self, "oauth_id", subject.oauth_id)
            object.__setattr__(self, "is_internal", subject.kind == "service")
            object.__setattr__(self, "attributes", subject.attributes)
            return
        object.__setattr__(self, "attributes", _freeze_attributes(self.attributes))
        if self.is_internal and (self.channel_user_id is not None or self.role not in {"internal", "service"}):
            object.__setattr__(self, "is_internal", False)

    @classmethod
    def from_identity(
        cls,
        identity: InvocationIdentityV1,
        *,
        channel_user_id: str | None = None,
    ) -> Principal:
        return cls(channel_user_id=channel_user_id, identity=identity)

    def identity_json(self) -> dict[str, Any] | None:
        return None if self.identity is None else self.identity.to_json()


@dataclass
class AuthzRequest:
    """Context supplied for one authorization decision."""

    principal: Principal
    resource: str
    action: str
    target: str
    context: dict[str, Any] = field(default_factory=dict)
    trusted_context: TrustedRunContextV1 | None = None


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
    health_probe: CapabilityHealthProbe | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.contribution_id, str) or not self.contribution_id.strip():
            raise ValueError("authorization provider contribution_id must be a non-empty string")
        if self.capability_api_version != AUTHORIZATION_PROVIDER_CAPABILITY_API_VERSION:
            raise ValueError(f"unsupported authorization provider capability API version {self.capability_api_version!r}; expected {AUTHORIZATION_PROVIDER_CAPABILITY_API_VERSION!r}")
        if self.kind != AUTHORIZATION_PROVIDER_KIND:
            raise ValueError(f"authorization provider factory kind must be {AUTHORIZATION_PROVIDER_KIND!r}")
        if not callable(self.factory):
            raise TypeError("authorization provider factory must be callable")
        if self.health_probe is not None and not callable(self.health_probe):
            raise TypeError("authorization provider health_probe must be callable")
