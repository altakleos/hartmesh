"""Host-independent authorization contracts for DeerFlow capabilities."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Literal, Protocol, runtime_checkable

from deerflow_extension_api.health import CapabilityHealthProbe
from deerflow_extension_api.identity import InvocationIdentityV1

if TYPE_CHECKING:
    from deerflow_extension_api.contributors import TrustedRunContextV1

AUTHORIZATION_PROVIDER_CAPABILITY_API_VERSION = "1.0"
AUTHORIZATION_PROVIDER_KIND = "authorization_provider"


def _freeze_authorization_value(value: Any) -> Any:
    """Snapshot supported JSON values and already-immutable public records."""

    if value is None or isinstance(value, (str, bool, int, float)):
        return value
    if isinstance(value, Mapping):
        if not all(isinstance(key, str) for key in value):
            raise TypeError("authorization mappings require string keys")
        return MappingProxyType({key: _freeze_authorization_value(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_authorization_value(item) for item in value)
    from deerflow_extension_api.contributors import SealedOriginV1, TrustedRunContextV1

    if isinstance(value, (SealedOriginV1, TrustedRunContextV1)):
        return value
    raise TypeError(f"authorization values cannot retain {type(value).__name__}")


def _thaw_authorization_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _thaw_authorization_value(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw_authorization_value(item) for item in value]
    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        return _thaw_authorization_value(to_dict())
    to_json = getattr(value, "to_json", None)
    if callable(to_json):
        return _thaw_authorization_value(to_json())
    return value


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

    def to_dict(self) -> dict[str, Any]:
        """Return a fresh mutable compatibility projection."""

        return {
            "user_id": self.user_id,
            "role": self.role,
            "oauth_provider": self.oauth_provider,
            "oauth_id": self.oauth_id,
            "channel_user_id": self.channel_user_id,
            "is_internal": self.is_internal,
            "attributes": _thaw_authorization_value(self.attributes),
            "identity": self.identity_json(),
        }


@dataclass(frozen=True)
class AuthzRequest:
    """Immutable context supplied for one authorization decision."""

    principal: Principal
    resource: str
    action: str
    target: str
    context: Mapping[str, Any] = field(default_factory=dict)
    trusted_context: TrustedRunContextV1 | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.principal, Principal):
            raise TypeError("authorization principal must be Principal")
        object.__setattr__(self, "context", _freeze_authorization_value(self.context))

    def to_dict(self) -> dict[str, Any]:
        """Return a fresh mutable provider-neutral copy."""

        return {
            "principal": self.principal.to_dict(),
            "resource": self.resource,
            "action": self.action,
            "target": self.target,
            "context": _thaw_authorization_value(self.context),
            "trusted_context": _thaw_authorization_value(self.trusted_context),
        }


@dataclass(frozen=True)
class AuthzReason:
    """Structured reason for an allow or deny decision."""

    code: str
    message: str = ""

    def to_dict(self) -> dict[str, str]:
        return {"code": self.code, "message": self.message}


@dataclass(frozen=True)
class AuthzDecision:
    """Immutable provider allow or deny verdict."""

    allow: bool
    reasons: Sequence[AuthzReason] = field(default_factory=tuple)
    policy_id: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "reasons", tuple(self.reasons))
        object.__setattr__(self, "metadata", _freeze_authorization_value(self.metadata))

    def to_dict(self) -> dict[str, Any]:
        """Return a fresh mutable wire-safe decision copy."""

        return {
            "allow": self.allow,
            "reasons": [reason.to_dict() for reason in self.reasons],
            "policy_id": self.policy_id,
            "metadata": _thaw_authorization_value(self.metadata),
        }


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
