"""Credential-safe categories for reporting a single MCP discovery attempt."""

import json
from contextlib import contextmanager
from typing import Literal

import httpx

DiscoveryReason = Literal["changed", "refused", "unreachable", "not-mcp", "timeout", "bad-token-format", "address-not-public"]


class InvalidCredentialHeader(ValueError):
    """A configured or fetched credential cannot form an HTTP header."""


class EndpointAddressRefused(ValueError):
    """The HTTP transport rejected the address under its endpoint policy."""


def discovery_failure(error: Exception) -> DiscoveryReason:
    from deerflow.mcp.oauth import McpOAuthReauthorizationRequired
    from deerflow.mcp.tasks.runtime import McpTaskConfigurationError

    # AnyIO's session transport reports the initial connection exception inside
    # a task group. Select a specific category without inspecting remote text.
    if isinstance(error, ExceptionGroup):
        reasons = {discovery_failure(child) for child in error.exceptions}
        for reason in ("address-not-public", "bad-token-format", "refused", "timeout", "unreachable", "changed"):
            if reason in reasons:
                return reason
        return "not-mcp"
    if isinstance(error, EndpointAddressRefused):
        return "address-not-public"
    if isinstance(error, InvalidCredentialHeader):
        return "bad-token-format"
    if isinstance(error, McpOAuthReauthorizationRequired) or isinstance(error, httpx.HTTPStatusError) and error.response.status_code in (401, 403):
        return "refused"
    if isinstance(error, (TimeoutError, httpx.TimeoutException)):
        return "timeout"
    if isinstance(error, (OSError, httpx.NetworkError)):
        return "unreachable"
    if isinstance(error, McpTaskConfigurationError):
        return "changed"
    return "not-mcp"


async def _check_mcp_response(response: httpx.Response) -> None:
    if response.request.method != "POST" or response.status_code != 200:
        return
    media_type = response.headers.get("Content-Type", "").partition(";")[0].strip().lower()
    if media_type in ("application/json", "text/event-stream"):
        return
    # The SDK reports unexpected media on its read stream without completing
    # the waiting JSON-RPC request. Reject that response at the existing HTTP
    # request instead; notifications don't require a JSON-RPC response.
    try:
        message = json.loads(response.request.content)
    except (ValueError, httpx.RequestNotRead):
        return
    if isinstance(message, dict) and "id" in message and message.get("method") in ("initialize", "tools/list"):
        raise ValueError("The response is not an MCP message")


@contextmanager
def check_discovery_media(connections):
    from mcp.shared._httpx_utils import create_mcp_http_client

    armed = True
    originals = []
    for connection in connections.values():
        if connection.get("transport") != "http":
            continue
        original = connection.get("httpx_client_factory")
        originals.append((connection, original))

        def factory(*args, base=original or create_mcp_http_client, **kwargs):
            client = base(*args, **kwargs)
            if armed:
                client.event_hooks["response"].append(_check_mcp_response)
            return client

        connection["httpx_client_factory"] = factory
    try:
        yield
    finally:
        # The adapter may retain a shallow copy in a callable tool. From this
        # point even a retained factory delegates directly to the ordinary one.
        armed = False
        for connection, original in originals:
            if original is None:
                connection.pop("httpx_client_factory", None)
            else:
                connection["httpx_client_factory"] = original
