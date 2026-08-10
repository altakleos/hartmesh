"""Shared Phase 1B authorization enforcement helpers."""

from __future__ import annotations

import logging
from collections.abc import Sequence

from langchain_core.tools import BaseTool

from deerflow.authz.provider import AuthorizationProvider, Principal
from deerflow.diagnostics import bounded_diagnostic, log_bounded_failure

logger = logging.getLogger(__name__)


def filter_tools_by_authorization(
    tools: Sequence[BaseTool],
    *,
    provider: AuthorizationProvider | None,
    principal: Principal,
    fail_closed: bool,
) -> list[BaseTool]:
    """Return the policy-visible subset of *tools* without changing its order.

    The caller must invoke this before deferred-tool assembly. Provider errors
    and malformed filter results deny every tool when ``fail_closed`` is true;
    an explicitly configured fail-open policy preserves the original set.
    """
    original_tools = list(tools)
    if provider is None:
        return original_tools

    candidates = [tool.name for tool in original_tools]
    try:
        allowed = provider.filter_resources(principal, "tool", candidates)
        if not isinstance(allowed, list) or any(not isinstance(name, str) for name in allowed):
            raise TypeError("AuthorizationProvider.filter_resources must return list[str]")
    except Exception as exc:
        diagnostic = bounded_diagnostic(
            code="authorization_resource_filter_failed",
            operation="filter_authorized_tools",
            error=exc,
            capability_id="authorization_provider",
        )
        log_bounded_failure(logger, diagnostic)
        return [] if fail_closed else original_tools

    allowed_names = set(allowed)
    return [tool for tool in original_tools if tool.name in allowed_names]
