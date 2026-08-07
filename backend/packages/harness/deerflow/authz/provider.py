"""AuthorizationProvider protocol and data structures for fine-grained resource authorization.

This is the policy brain for resource-level authorization (RBAC and beyond),
deliberately kept as a sibling to :mod:`deerflow.guardrails` rather than folded
into it. PR #3665 (which added ``user_role``/``user_id`` to
``GuardrailRequest``) explicitly scoped guardrails to *execution-time* checks
only — *"保持 Guardrail 的职责边界不变：不新增 policy engine、RBAC 系统、
governance 子系统"*. This module is the RBAC brain that #3665 deferred.

The provider is enforced at **two layers** from one policy:

1. **Assembly-time capability filter** — removes tools a role can never use
   *before* they are bound to the agent, so the model never sees them and
   ``tool_search`` can never promote them back (fail-closed).
2. **Run-time execution deny** — reuses :class:`~deerflow.guardrails.middleware.GuardrailMiddleware`
   via a thin adapter (see :mod:`deerflow.authz.adapter`), catching dynamic
   resources and argument-based restrictions.

See ``docs/plans/2026-07-10-pluggable-authorization-rfc.md`` (issue #4063) for
the full design rationale.
"""

from deerflow_extension_api.authorization import (
    AuthorizationProvider,
    AuthzDecision,
    AuthzReason,
    AuthzRequest,
    Principal,
)

__all__ = [
    "AuthorizationProvider",
    "AuthzDecision",
    "AuthzReason",
    "AuthzRequest",
    "Principal",
]
