"""Provider factory — resolves and constructs the configured AuthorizationProvider.

This is the single entry point for creating an authorization provider from
``AuthorizationConfig``. It does not cache instances (Phase 1B resolves once
per agent build and passes the same instance to Layer 1 and Layer 2).
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from deerflow.authz.provider import AuthorizationProvider, AuthzRequest
from deerflow.config.authorization_config import AuthorizationConfig
from deerflow.diagnostics import (
    bounded_diagnostic,
    log_bounded_failure,
    require_async_authoritative_operation,
)
from deerflow.reflection import resolve_variable

AUTHORIZATION_PROVIDER_CONTEXT_KEY = "__deerflow_authorization_provider"
logger = logging.getLogger(__name__)


def _legacy_provider_failure(
    *,
    code: str,
    operation: str,
    error: BaseException,
) -> ValueError:
    """Return one safely attributable startup failure for legacy providers."""

    diagnostic = bounded_diagnostic(
        code=code,
        operation=operation,
        error=error,
        capability_id="authorization_provider:legacy",
    )
    log_bounded_failure(logger, diagnostic, level=logging.ERROR)
    return ValueError(f"authorization provider initialization failed: code={diagnostic.code} error_class={diagnostic.error_class} correlation_id={diagnostic.correlation_id}")


@dataclass(frozen=True)
class AuthorizedToolCallReceipt:
    """Exact provider/request pair allowed by the operation-time tool check."""

    provider: AuthorizationProvider
    request: AuthzRequest


def authorization_provider_from_context(
    context: Mapping[str, Any] | None,
) -> AuthorizationProvider | None:
    """Return the host-supplied provider carried by a runtime context."""
    if context is None:
        return None
    provider = context.get(AUTHORIZATION_PROVIDER_CONTEXT_KEY)
    return provider if isinstance(provider, AuthorizationProvider) else None


def resolve_authorization_provider(
    config: AuthorizationConfig,
) -> AuthorizationProvider | None:
    """Resolve the authorization provider from config.

    Returns:
        A constructed ``AuthorizationProvider`` instance, or ``None`` if
        authorization is disabled.

    Raises:
        ValueError: If ``enabled`` is True but no provider is configured,
            or if the class path is invalid / construction fails / the
            instance does not satisfy the ``AuthorizationProvider`` Protocol.
    """
    if not config.enabled:
        return None

    if config.provider is None:
        raise ValueError("authorization.enabled is true but no provider is configured; set authorization.provider.use to a class path")

    class_path = config.provider.use
    try:
        provider_cls = resolve_variable(class_path, expected_type=type)
    except (ImportError, ValueError) as err:
        raise _legacy_provider_failure(
            code="authorization_provider_resolution_failed",
            operation="resolve_authorization_provider",
            error=err,
        ) from None

    kwargs = dict(config.provider.config) if config.provider.config else {}
    try:
        instance = provider_cls(**kwargs)
    except Exception as err:
        raise _legacy_provider_failure(
            code="authorization_provider_initialization_failed",
            operation="construct_authorization_provider",
            error=err,
        ) from None

    if not isinstance(instance, AuthorizationProvider):
        raise _legacy_provider_failure(
            code="authorization_provider_contract_invalid",
            operation="validate_authorization_provider",
            error=TypeError("authorization_provider_protocol_invalid"),
        ) from None
    try:
        require_async_authoritative_operation(instance, "aauthorize")
    except TypeError as err:
        raise _legacy_provider_failure(
            code="authoritative_operation_not_async",
            operation="aauthorize",
            error=err,
        ) from None

    from deerflow.authz.rbac import RbacAuthorizationProvider

    if isinstance(instance, RbacAuthorizationProvider):
        try:
            instance.validate_role(config.default_role, field="authorization.default_role")
        except ValueError as err:
            raise _legacy_provider_failure(
                code="authorization_default_role_invalid",
                operation="validate_authorization_default_role",
                error=err,
            ) from None

    return instance
