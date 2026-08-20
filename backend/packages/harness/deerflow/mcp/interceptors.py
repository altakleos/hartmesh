"""Shared construction of MCP tool-call interceptors."""

from __future__ import annotations

import logging
from typing import Any

from deerflow.config.extensions_config import ExtensionsConfig
from deerflow.diagnostics import bounded_diagnostic, log_bounded_failure
from deerflow.mcp.oauth import build_oauth_tool_interceptor
from deerflow.reflection import resolve_variable

logger = logging.getLogger(__name__)


def build_mcp_tool_interceptors(
    extensions_config: ExtensionsConfig,
    *,
    oauth_builder: Any = build_oauth_tool_interceptor,
    resolver: Any = resolve_variable,
    target_logger: logging.Logger = logger,
) -> list[Any]:
    """Build OAuth followed by configured custom MCP interceptors."""
    interceptors: list[Any] = []
    oauth_interceptor = oauth_builder(extensions_config)
    if oauth_interceptor is not None:
        interceptors.append(oauth_interceptor)

    raw_paths = (extensions_config.model_extra or {}).get("mcpInterceptors")
    if isinstance(raw_paths, str):
        raw_paths = [raw_paths]
    elif not isinstance(raw_paths, list):
        if raw_paths is not None:
            target_logger.warning(
                "mcpInterceptors must be a list of strings, got %s; skipping",
                type(raw_paths).__name__,
            )
        raw_paths = []

    for interceptor_path in raw_paths:
        try:
            builder = resolver(interceptor_path)
            interceptor = builder()
            if callable(interceptor):
                interceptors.append(interceptor)
                target_logger.info("Loaded MCP interceptor: %s", interceptor_path)
            elif interceptor is not None:
                target_logger.warning(
                    "Builder %s returned non-callable %s; skipping",
                    interceptor_path,
                    type(interceptor).__name__,
                )
        except Exception as exc:
            diagnostic = bounded_diagnostic(
                code="legacy_mcp_interceptor_load_failed",
                operation="load_mcp_interceptor",
                error=exc,
                contribution_id=(interceptor_path if isinstance(interceptor_path, str) else None),
            )
            log_bounded_failure(target_logger, diagnostic)
    return interceptors
