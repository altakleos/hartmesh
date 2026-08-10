"""Gateway-owned coherent authorization-provider resolution."""

from __future__ import annotations

import json
import logging
import threading
from dataclasses import dataclass
from typing import Literal

from deerflow_extension_api import AuthorizationProvider

from deerflow.authz.runtime import resolve_authorization_provider
from deerflow.config.authorization_config import AuthorizationConfig
from deerflow.diagnostics import (
    bounded_diagnostic,
    log_bounded_failure,
    require_async_authoritative_operation,
)
from deerflow.extensions.registry import (
    LoadedExtensions,
    RegisteredAuthorizationProviderFactory,
)

logger = logging.getLogger(__name__)

AuthorizationSourceKind = Literal["disabled", "legacy", "extension"]


@dataclass(frozen=True)
class AuthorizationResolutionSnapshot:
    """One complete provider resolution published atomically to readers."""

    generation: int
    provider: AuthorizationProvider | None
    source_kind: AuthorizationSourceKind
    config_signature: str
    extension_generation: int
    registration: RegisteredAuthorizationProviderFactory | None = None


def _config_signature(config: AuthorizationConfig) -> str:
    # Invocation-operation flags are startup-only and are snapshotted by the
    # Gateway application. They must not participate in the legacy provider's
    # hot-reload signature or manufacture a second provider generation.
    return json.dumps(
        config.model_dump(
            mode="json",
            exclude={"invocation_operations", "service_observation_grants"},
        ),
        sort_keys=True,
        separators=(",", ":"),
    )


class AuthorizationProviderResolver:
    """Own one coherent provider instance for the Gateway process."""

    def __init__(
        self,
        extensions: LoadedExtensions,
        startup_config: AuthorizationConfig,
    ) -> None:
        self._lock = threading.RLock()
        self._extensions = extensions
        self._registration = extensions.authorization_provider_factory
        self._plugin_provider: AuthorizationProvider | None = None
        self._snapshot: AuthorizationResolutionSnapshot | None = None

        self._reject_ambiguity(startup_config)
        if self._registration is not None:
            self._plugin_provider = self._construct_plugin_provider(self._registration)
        self.resolve(startup_config)

    def _reject_ambiguity(self, config: AuthorizationConfig) -> None:
        if self._registration is not None and config.provider is not None:
            raise ValueError("extension authorization provider factory and legacy authorization.provider.use are mutually exclusive")

    @staticmethod
    def _construct_plugin_provider(
        registration: RegisteredAuthorizationProviderFactory,
    ) -> AuthorizationProvider:
        try:
            provider = registration.factory()
        except Exception as exc:
            diagnostic = bounded_diagnostic(
                code="authorization_initialization_failed",
                operation="aauthorize",
                error=exc,
                capability_id=f"authorization_provider:{registration.contribution_id}",
                contribution_id=registration.contribution_id,
            )
            log_bounded_failure(logger, diagnostic, level=logging.ERROR)
            raise ValueError(f"authorization provider {registration.contribution_id!r} failed to initialize: {diagnostic.code} error_class={diagnostic.error_class} correlation_id={diagnostic.correlation_id}") from None
        if not isinstance(provider, AuthorizationProvider):
            raise ValueError(f"Extension authorization provider factory {registration.contribution_id!r} did not return an AuthorizationProvider")
        try:
            require_async_authoritative_operation(provider, "aauthorize")
        except Exception as exc:
            diagnostic = bounded_diagnostic(
                code="authoritative_operation_not_async",
                operation="aauthorize",
                error=exc,
                capability_id=f"authorization_provider:{registration.contribution_id}",
                contribution_id=registration.contribution_id,
            )
            log_bounded_failure(logger, diagnostic, level=logging.ERROR)
            raise ValueError(f"authorization provider {registration.contribution_id!r} failed to initialize: {diagnostic.code} error_class={diagnostic.error_class} correlation_id={diagnostic.correlation_id}") from None
        return provider

    def snapshot(self) -> AuthorizationResolutionSnapshot:
        with self._lock:
            assert self._snapshot is not None
            return self._snapshot

    def resolve(
        self,
        config: AuthorizationConfig,
    ) -> AuthorizationResolutionSnapshot:
        """Return the complete snapshot for *config*, replacing it if needed."""
        signature = _config_signature(config)
        with self._lock:
            self._reject_ambiguity(config)
            current = self._snapshot
            if current is not None and current.config_signature == signature:
                return current

            if config.enabled is not True:
                provider = None
                source_kind: AuthorizationSourceKind = "disabled"
                registration = None
            elif self._registration is not None:
                provider = self._plugin_provider
                source_kind = "extension"
                registration = self._registration
            else:
                provider = resolve_authorization_provider(config)
                source_kind = "legacy"
                registration = None

            generation = 1 if current is None else current.generation + 1
            replacement = AuthorizationResolutionSnapshot(
                generation=generation,
                provider=provider,
                source_kind=source_kind,
                config_signature=signature,
                extension_generation=self._extensions.generation,
                registration=registration,
            )
            self._snapshot = replacement
            return replacement
