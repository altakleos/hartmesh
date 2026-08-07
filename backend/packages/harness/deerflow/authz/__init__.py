"""Pluggable fine-grained authorization (resource-level RBAC and beyond)."""

from deerflow.authz.adapter import GuardrailAuthorizationAdapter
from deerflow.authz.enforcement import filter_tools_by_authorization
from deerflow.authz.principal import build_principal_from_context, normalize_authz_attributes
from deerflow.authz.provider import AuthorizationProvider, AuthzDecision, AuthzReason, AuthzRequest, Principal
from deerflow.authz.rbac import RbacAuthorizationProvider
from deerflow.authz.runtime import (
    AUTHORIZATION_PROVIDER_CONTEXT_KEY,
    authorization_provider_from_context,
    resolve_authorization_provider,
)
from deerflow.authz.tool_filter import apply_tool_authorization

__all__ = [
    "AuthzDecision",
    "AuthzReason",
    "AuthzRequest",
    "AuthorizationProvider",
    "AUTHORIZATION_PROVIDER_CONTEXT_KEY",
    "GuardrailAuthorizationAdapter",
    "Principal",
    "RbacAuthorizationProvider",
    "apply_tool_authorization",
    "authorization_provider_from_context",
    "build_principal_from_context",
    "filter_tools_by_authorization",
    "normalize_authz_attributes",
    "resolve_authorization_provider",
]
