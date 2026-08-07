"""Public contracts for DeerFlow extensions.

This package MUST NOT import `deerflow`. Everything an extension needs to
integrate lives here, so an extension depends on this package alone and can
be released independently of the host.
"""

from __future__ import annotations

from deerflow_extension_api.authorization import (
    AUTHORIZATION_PROVIDER_CAPABILITY_API_VERSION,
    AUTHORIZATION_PROVIDER_KIND,
    AuthorizationProvider,
    AuthorizationProviderFactory,
    AuthzDecision,
    AuthzReason,
    AuthzRequest,
    Principal,
)
from deerflow_extension_api.contracts import (
    ExtensionInstall,
    ExtensionRegistry,
    HostPolicySnapshot,
    MiddlewareContributor,
    extension,
)
from deerflow_extension_api.contributors import (
    ORIGIN_CONTRIBUTOR_CAPABILITY_API_VERSION,
    ORIGIN_CONTRIBUTOR_KIND,
    RUN_CONTEXT_CONTRIBUTOR_CAPABILITY_API_VERSION,
    RUN_CONTEXT_CONTRIBUTOR_KIND,
    OriginContributionRequestV1,
    OriginContributionV1,
    OriginContributor,
    OriginContributorFactory,
    PrincipalProjectionV1,
    ResolvedAgentRevisionReferenceV1,
    RunContextContributionRequestV1,
    RunContextContributionV1,
    RunContextContributor,
    RunContextContributorFactory,
    SafeContextReferenceV1,
    SealedOriginV1,
)
from deerflow_extension_api.placement import (
    AgentBuildContext,
    AgentScope,
    MiddlewarePlacement,
    Placement,
)
from deerflow_extension_api.runtime_bridge import (
    EXTENSION_TASK_STORE_KEY,
    task_store_from_runtime,
)
from deerflow_extension_api.state import ExtensionData

#: Contract version. Pre-1.0 minors may break and only patches promise to be
#: additive. From 1.0 on, bump the major on any breaking change; see the spec's
#: evolution rules for what counts as additive.
API_VERSION = "0.3.0"

__all__ = [
    "API_VERSION",
    "AUTHORIZATION_PROVIDER_CAPABILITY_API_VERSION",
    "AUTHORIZATION_PROVIDER_KIND",
    "EXTENSION_TASK_STORE_KEY",
    "AgentBuildContext",
    "AgentScope",
    "AuthorizationProvider",
    "AuthorizationProviderFactory",
    "AuthzDecision",
    "AuthzReason",
    "AuthzRequest",
    "ExtensionData",
    "ExtensionInstall",
    "ExtensionRegistry",
    "HostPolicySnapshot",
    "MiddlewareContributor",
    "MiddlewarePlacement",
    "Placement",
    "Principal",
    "ORIGIN_CONTRIBUTOR_CAPABILITY_API_VERSION",
    "ORIGIN_CONTRIBUTOR_KIND",
    "RUN_CONTEXT_CONTRIBUTOR_CAPABILITY_API_VERSION",
    "RUN_CONTEXT_CONTRIBUTOR_KIND",
    "OriginContributionRequestV1",
    "OriginContributionV1",
    "OriginContributor",
    "OriginContributorFactory",
    "PrincipalProjectionV1",
    "ResolvedAgentRevisionReferenceV1",
    "RunContextContributionRequestV1",
    "RunContextContributionV1",
    "RunContextContributor",
    "RunContextContributorFactory",
    "SafeContextReferenceV1",
    "SealedOriginV1",
    "extension",
    "task_store_from_runtime",
]
