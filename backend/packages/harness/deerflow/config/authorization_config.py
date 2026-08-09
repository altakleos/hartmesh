"""Configuration for fine-grained resource authorization.

When enabled, a pluggable :class:`~deerflow.authz.provider.AuthorizationProvider`
becomes the policy brain for resource-level authorization, enforced at two
layers: assembly-time capability filtering (tools the agent can never see) and
run-time execution deny (reuses :class:`~deerflow.guardrails.middleware.GuardrailMiddleware`
via an adapter). Default ``enabled: false`` preserves today's behavior where
every authenticated user has access to all tools, models, skills, and sandbox.
"""

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

_MAX_OBSERVATION_GRANT_SELECTORS = 128
_MAX_OBSERVATION_IDENTIFIER_BYTES = 256
_INVOCATION_SOURCE_KINDS = frozenset({"http", "scheduled_task", "native_channel", "service"})


def _validate_observation_identifiers(
    values: tuple[str, ...],
    *,
    field_name: str,
) -> tuple[str, ...]:
    for value in values:
        if not isinstance(value, str) or not value:
            raise ValueError(f"{field_name} must contain non-empty strings")
        if len(value.encode("utf-8")) > _MAX_OBSERVATION_IDENTIFIER_BYTES:
            raise ValueError(f"{field_name} contains an identifier over 256 UTF-8 bytes")
        if any(ord(character) < 32 or ord(character) == 127 for character in value):
            raise ValueError(f"{field_name} contains a control character")
    if len(values) != len(set(values)):
        raise ValueError(f"{field_name} contains duplicate identifiers")
    return values


class ServiceObservationGrantConfig(BaseModel):
    """Operator-established finite cross-owner observation scope."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    service_id: str
    run_ids: tuple[str, ...] = ()
    thread_ids: tuple[str, ...] = ()
    owner_ids: tuple[str, ...] = ()
    source_kinds: tuple[str, ...] = ()

    @field_validator("service_id")
    @classmethod
    def _validate_service_id(cls, value: str) -> str:
        return _validate_observation_identifiers((value,), field_name="service_id")[0]

    @field_validator("run_ids", "thread_ids", "owner_ids", "source_kinds")
    @classmethod
    def _validate_selectors(cls, values: tuple[str, ...], info) -> tuple[str, ...]:
        return _validate_observation_identifiers(values, field_name=info.field_name)

    @model_validator(mode="after")
    def _validate_scope(self):
        if any(value not in _INVOCATION_SOURCE_KINDS for value in self.source_kinds):
            raise ValueError("source_kinds contains an unsupported invocation source kind")
        total = len(self.run_ids) + len(self.thread_ids) + len(self.owner_ids) + len(self.source_kinds)
        if total == 0:
            raise ValueError("service observation grant requires at least one finite selector")
        if total > _MAX_OBSERVATION_GRANT_SELECTORS:
            raise ValueError("service observation grant exceeds 128 aggregate selectors")
        return self


class AuthorizationProviderConfig(BaseModel):
    """Configuration for an authorization provider."""

    use: str = Field(description="Class path (e.g. 'deerflow.authz.rbac:RbacAuthorizationProvider')")
    config: dict = Field(default_factory=dict, description="Provider-specific settings passed as kwargs")


class InvocationOperationsAuthorizationConfig(BaseModel):
    """Startup-only authorization controls for durable invocation operations."""

    start_enabled: bool = Field(default=False, description="Authorize new durable invocation admission")
    observe_enabled: bool = Field(default=False, description="Authorize visible run and context observation")
    cancel_enabled: bool = Field(default=False, description="Authorize visible run cancellation")
    timeout_seconds: float = Field(default=2.0, gt=0, description="Host timeout for each invocation authorization decision")


class AuthorizationConfig(BaseModel):
    """Configuration for fine-grained resource authorization.

    Mirrors :class:`~deerflow.config.guardrails_config.GuardrailsConfig` in
    shape: a provider loaded by class path, a fail-closed default, and a
    live-reloadable singleton.
    """

    enabled: bool = Field(default=False, description="Enable fine-grained authorization")
    fail_closed: bool = Field(default=True, description="Block access if the provider errors or identity is unresolved")
    default_role: str = Field(default="user", description="Role applied when user_role is None (e.g. unbound IM channels)")
    provider: AuthorizationProviderConfig | None = Field(default=None, description="Authorization provider configuration")
    invocation_operations: InvocationOperationsAuthorizationConfig = Field(
        default_factory=InvocationOperationsAuthorizationConfig,
        description="Startup-only durable invocation operation controls",
    )
    service_observation_grants: tuple[ServiceObservationGrantConfig, ...] = Field(
        default=(),
        description=("Operator-established, hot-reloaded finite visibility scopes for authenticated services; the invocation authorization provider still decides observe access"),
    )

    @model_validator(mode="after")
    def _validate_unique_observer_services(self):
        service_ids = tuple(item.service_id for item in self.service_observation_grants)
        if len(service_ids) != len(set(service_ids)):
            raise ValueError("service_observation_grants contains a duplicate service_id")
        if self.service_observation_grants and (not self.enabled or not self.invocation_operations.observe_enabled):
            raise ValueError("service_observation_grants require enabled invocation observe authorization")
        return self


_authorization_config: AuthorizationConfig | None = None


def get_authorization_config() -> AuthorizationConfig:
    """Get the authorization config, returning defaults if not loaded."""
    global _authorization_config
    if _authorization_config is None:
        _authorization_config = AuthorizationConfig()
    return _authorization_config


def load_authorization_config_from_dict(data: dict) -> AuthorizationConfig:
    """Load authorization config from a dict (called during AppConfig loading)."""
    global _authorization_config
    _authorization_config = AuthorizationConfig.model_validate(data)
    return _authorization_config


def reset_authorization_config() -> None:
    """Reset the cached config instance. Used in tests to prevent singleton leaks."""
    global _authorization_config
    _authorization_config = None
