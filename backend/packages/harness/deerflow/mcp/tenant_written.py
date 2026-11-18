"""Write-time rules for MCP definitions where connections are written by tenants.

Every writer of an MCP definition calls :func:`require_tenant_written_definition`
inside its mutation lock: the Gateway routes through
``routers/mcp.py::_admit_mcp_transition`` and the embedded client in
``DeerFlowClient.update_mcp_config``. The rule itself, and how a deployment
declares itself, live in ``deerflow.community.endpoint_policy``.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import ValidationError

from deerflow.community.endpoint_policy import address_problem, tenant_written
from deerflow.config.extensions_config import McpServerConfig, normalize_mcp_transport_alias

RefusalReason = Literal["address-not-public", "environment-reference"]

_SENTENCES: dict[RefusalReason, str] = {
    "address-not-public": "MCP connections on this deployment must use a public HTTP(S) address.",
    "environment-reference": "Values that begin with `$` are not accepted for MCP connections on this deployment.",
}


class TenantWrittenDefinitionRefused(ValueError):
    """A definition a write would store breaks a tenant-written rule; the message says what is accepted."""

    def __init__(self, reason: RefusalReason) -> None:
        super().__init__(_SENTENCES[reason])
        self.reason = reason


def _stored_definition_is_kept(previous: Any, candidate: Any) -> bool:
    """Whether *candidate* is the stored definition, as it is or only switched off.

    Both are read through the connection's model first: a stored definition and
    its merged form differ in defaults that mean nothing.
    """
    if previous is None:
        return False
    try:
        stored = McpServerConfig.model_validate(previous).model_dump()
        new = McpServerConfig.model_validate(candidate).model_dump()
    except (ValueError, TypeError, ValidationError):
        return False
    return new == stored or (new["enabled"] is False and new == {**stored, "enabled": False})


def _has_environment_reference(value: Any) -> bool:
    if isinstance(value, str):
        return value.startswith("$")
    if isinstance(value, dict):
        return any(_has_environment_reference(item) for item in value.values())
    if isinstance(value, list | tuple):
        return any(_has_environment_reference(item) for item in value)
    return False


def require_tenant_written_definition(previous: Any, candidate: Any, *, deployment_connection: bool) -> None:
    """Hold a definition a write creates, changes or switches on to the tenant-written rules.

    Checked on the definition as it would be stored, after masked values were
    put back. A definition the write leaves as stored, or only switches off, is
    not checked, so an earlier entry never blocks an unrelated write; removal
    is always allowed. Can look a host name up, so callers run it off the event
    loop. Raises :class:`TenantWrittenDefinitionRefused`.
    """
    # A candidate that is not an object is refused by validation before it is stored.
    if not isinstance(candidate, dict) or not tenant_written() or _stored_definition_is_kept(previous, candidate):
        return
    definition = normalize_mcp_transport_alias(candidate)
    remote = str(definition.get("type") or "stdio").lower() in {"http", "sse"}
    oauth = definition.get("oauth")
    # The token address is used only while the OAuth setup is switched on.
    token_address_used = isinstance(oauth, dict) and oauth.get("enabled", True) is not False
    if (remote and address_problem(definition.get("url"))) or (token_address_used and address_problem(oauth.get("token_url"))):
        raise TenantWrittenDefinitionRefused("address-not-public")
    # A personal connection's values were always used as typed, so there a
    # leading `$` is ordinary text.
    if deployment_connection and _has_environment_reference(definition):
        raise TenantWrittenDefinitionRefused("environment-reference")
