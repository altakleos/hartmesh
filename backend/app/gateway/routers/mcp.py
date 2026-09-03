import asyncio
import json
import logging
import re
from pathlib import Path
from typing import Any, Literal, NoReturn

from deerflow_extension_api import (
    validate_mcp_server_identifier,
    validate_mcp_tool_identifier,
)
from fastapi import APIRouter, HTTPException, Request, status
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from app.gateway.deps import require_admin_user
from app.gateway.tool_plane_guard import reject_direct_tool_plane_mutation
from deerflow.config.extensions_config import (
    ExtensionsConfig,
    McpRoutingConfig,
    McpTaskToolsetConfig,
    McpToolOverride,
    atomic_write_extensions_config,
    extensions_config_file_lock,
    extensions_config_write_lock,
    get_extensions_config,
    normalize_mcp_transport_alias,
    reload_extensions_config,
)
from deerflow.config.runtime_paths import project_root
from deerflow.constants import DEFAULT_MCP_SESSION_INIT_TIMEOUT
from deerflow.mcp.cache import reset_mcp_tools_cache
from deerflow.mcp.launch_policy import (
    MCP_STDIO_COMMAND_ALLOWLIST_ENV,
    McpStdioLaunchPolicyViolation,
    allowed_stdio_commands,
    validate_mcp_stdio_launch,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api", tags=["mcp"])

_ADMIN_REQUIRED_DETAIL = "Admin privileges required to manage MCP configuration."
_MCP_STDIO_COMMAND_ALLOWLIST_ENV = MCP_STDIO_COMMAND_ALLOWLIST_ENV


class McpUserScopedAuthConfigResponse(BaseModel):
    """Per-user credential injection configuration for an MCP server."""

    enabled: bool = Field(default=True, description="Whether user-scoped credential injection is enabled")
    header: str = Field(default="Authorization", description="HTTP header to set with the resolved user credential")
    users: dict[str, str] = Field(default_factory=dict, description="Map of DeerFlow user id to credential header value")
    on_missing: Literal["deny", "passthrough"] = Field(default="deny", description="Behavior when the calling user has no mapped credential")
    # Mirror the harness-side McpUserScopedAuthConfig (extra="allow"): without
    # this, an operator's unknown key inside user_auth would be silently
    # stripped by the next admin PUT, while server-level extras are preserved.
    model_config = ConfigDict(extra="allow")

    # Mirror the harness-side non-blank check: a blank header accepted here
    # would be persisted, then fail ExtensionsConfig validation on reload —
    # wedging every subsequent config load until the file is hand-edited.
    @field_validator("header")
    @classmethod
    def _validate_header_not_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("user_auth.header must not be empty")
        return value


class McpContextHeadersConfigResponse(BaseModel):
    """Per-request credential injection configuration for an MCP server.

    Holds header names and run-context key names only — never a credential —
    so unlike ``user_auth`` its declared fields are returned unmasked by GET.
    """

    enabled: bool = Field(default=True, description="Whether request-scoped header injection is enabled")
    headers: dict[str, str] = Field(
        default_factory=dict,
        description="Map of HTTP header name to the key read from the run request's config.context.secrets",
    )
    on_missing: Literal["deny", "passthrough"] = Field(default="deny", description="Behavior when a mapped key is absent from the request secrets")
    # Mirror the harness-side McpContextHeadersConfig (extra="allow"): without
    # this, an operator's unknown key inside headers_from_context would be
    # silently stripped by the next admin PUT.
    model_config = ConfigDict(extra="allow")

    # Mirror the harness-side entry check: a blank name accepted here would be
    # persisted, then fail ExtensionsConfig validation on reload — wedging every
    # subsequent config load until the file is hand-edited.
    @field_validator("headers")
    @classmethod
    def _validate_mapping_entries(cls, value: dict[str, str]) -> dict[str, str]:
        seen: dict[str, str] = {}
        for header_name, secret_key in value.items():
            if not header_name.strip():
                raise ValueError("headers_from_context.headers must not contain a blank header name")
            if not isinstance(secret_key, str) or not secret_key.strip():
                raise ValueError(f"headers_from_context.headers[{header_name!r}] must name a non-blank secret key from config.context.secrets")
            lowered = header_name.lower()
            if lowered in seen:
                raise ValueError(f"headers_from_context.headers maps the same HTTP header under two spellings ({seen[lowered]!r} and {header_name!r}); header names are case-insensitive, so keep only one")
            seen[lowered] = header_name
        return value


class McpOAuthConfigResponse(BaseModel):
    """OAuth configuration for an MCP server."""

    enabled: bool = Field(default=True, description="Whether OAuth token injection is enabled")
    token_url: str = Field(default="", description="OAuth token endpoint URL")
    grant_type: Literal["client_credentials", "refresh_token"] = Field(default="client_credentials", description="OAuth grant type")
    client_id: str | None = Field(default=None, description="OAuth client ID")
    client_secret: str | None = Field(default=None, description="OAuth client secret")
    refresh_token: str | None = Field(default=None, description="OAuth refresh token")
    scope: str | None = Field(default=None, description="OAuth scope")
    audience: str | None = Field(default=None, description="OAuth audience")
    token_field: str = Field(default="access_token", description="Token response field containing access token")
    token_type_field: str = Field(default="token_type", description="Token response field containing token type")
    expires_in_field: str = Field(default="expires_in", description="Token response field containing expires-in seconds")
    default_token_type: str = Field(default="Bearer", description="Default token type when response omits token_type")
    refresh_skew_seconds: int = Field(default=60, description="Refresh this many seconds before expiry")
    extra_token_params: dict[str, str] = Field(default_factory=dict, description="Additional form params sent to token endpoint")
    # Mirror the harness-side McpOAuthConfig (extra="allow"): provider-specific
    # OAuth fields must survive the Gateway's GET -> edit -> PUT round-trip.
    model_config = ConfigDict(extra="allow")


class McpServerConfigResponse(BaseModel):
    """Response model for MCP server configuration."""

    enabled: bool = Field(default=True, description="Whether this MCP server is enabled")
    type: str = Field(default="stdio", description="Transport type: 'stdio', 'sse', or 'http'")
    command: str | None = Field(default=None, description="Command to execute to start the MCP server (for stdio type)")
    args: list[str] = Field(default_factory=list, description="Arguments to pass to the command (for stdio type)")
    env: dict[str, str] = Field(default_factory=dict, description="Environment variables for the MCP server")
    url: str | None = Field(default=None, description="URL of the MCP server (for sse or http type)")
    headers: dict[str, str] = Field(default_factory=dict, description="HTTP headers to send (for sse or http type)")
    oauth: McpOAuthConfigResponse | None = Field(default=None, description="OAuth configuration for MCP HTTP/SSE servers")
    user_auth: McpUserScopedAuthConfigResponse | None = Field(default=None, description="Per-user credential injection for MCP HTTP/SSE servers")
    headers_from_context: McpContextHeadersConfigResponse | None = Field(default=None, description="Per-request credential injection for MCP HTTP/SSE servers: map header names to config.context.secrets keys")
    description: str = Field(default="", description="Human-readable description of what this MCP server provides")
    routing: McpRoutingConfig = Field(default_factory=McpRoutingConfig, description="Soft routing hints for tools from this MCP server")
    tools: dict[str, McpToolOverride] = Field(default_factory=dict, description="Per-original-tool MCP configuration overrides")
    tool_name_prefix: bool = Field(default=True, description="Whether to prefix discovered tool names with the MCP server name")
    tool_call_timeout: float | None = Field(
        default=None,
        description="Timeout in seconds for individual stdio MCP calls and durable-task calls on every transport",
    )
    # Default matches McpServerConfig: this model's defaults feed model_dump()
    # into the persisted extensions config on PUT, so an API-created server that
    # omits the field must get the same bring-up timeout as a file-created one.
    session_init_timeout: float | None = Field(
        default=DEFAULT_MCP_SESSION_INIT_TIMEOUT,
        description="Timeout in seconds for MCP server bring-up and durable HTTP/SSE task-session initialization; null means no timeout",
    )
    task_toolsets: list[McpTaskToolsetConfig] = Field(
        default_factory=list,
        description="Raw submit/status/cancel tool groups managed as durable background tasks",
    )
    model_config = ConfigDict(extra="allow")

    @field_validator("headers")
    @classmethod
    def _validate_header_names(cls, value: dict[str, str]) -> dict[str, str]:
        # Mirror the harness-side McpServerConfig check: HTTP field names are
        # case-insensitive, so a config carrying one header under two spellings
        # would persist, reload into a connection with both fields, and let a
        # later override replace only one of them. Reject at the API boundary
        # instead of wedging the next ExtensionsConfig reload.
        seen: dict[str, str] = {}
        for header_name in value:
            lowered = header_name.lower()
            if lowered in seen:
                raise ValueError(f"headers maps the same HTTP header under two spellings ({seen[lowered]!r} and {header_name!r}); header names are case-insensitive, so keep only one")
            seen[lowered] = header_name
        return value

    @model_validator(mode="before")
    @classmethod
    def _accept_transport_alias(cls, data: Any) -> Any:
        """Keep API parsing aligned with the runtime MCP config model."""
        return normalize_mcp_transport_alias(data)

    @field_validator("tools")
    @classmethod
    def _validate_tool_override_names(
        cls,
        value: dict[str, McpToolOverride],
    ) -> dict[str, McpToolOverride]:
        for tool_name in value:
            validate_mcp_tool_identifier(tool_name, field_name="MCP tool override identifier")
        return value


class McpConfigResponse(BaseModel):
    """Response model for MCP configuration."""

    mcp_servers: dict[str, McpServerConfigResponse] = Field(
        default_factory=dict,
        description="Map of MCP server name to configuration",
    )


class McpConfigUpdateRequest(BaseModel):
    """Request model for updating MCP configuration."""

    mcp_servers: dict[str, McpServerConfigResponse] = Field(
        ...,
        description="Map of MCP server name to configuration",
    )

    @field_validator("mcp_servers")
    @classmethod
    def _validate_server_names(
        cls,
        value: dict[str, McpServerConfigResponse],
    ) -> dict[str, McpServerConfigResponse]:
        for server_name, server in value.items():
            validate_mcp_server_identifier(server_name)
            if server.tool_name_prefix:
                try:
                    validate_mcp_tool_identifier(
                        f"{server_name}_x",
                        field_name="prefixed MCP callable name",
                    )
                except ValueError as exc:
                    raise ValueError(f"MCP server {server_name!r} cannot prefix callable tool names. Rename the server to an ASCII tool identifier or set tool_name_prefix=false") from exc
        return value


class McpServerStateUpdateRequest(BaseModel):
    """Request model for enabling or disabling one MCP server."""

    server_name: str = Field(
        ...,
        description="Name of the MCP server to update",
    )
    enabled: bool = Field(..., description="Whether the MCP server is enabled")

    @field_validator("server_name")
    @classmethod
    def _validate_server_name(cls, value: str) -> str:
        return validate_mcp_server_identifier(value)


class McpServerConfigUpdateRequest(BaseModel):
    """Request model for replacing one MCP server configuration."""

    server_name: str = Field(
        ...,
        description="Name of the existing MCP server to update",
    )
    server: McpServerConfigResponse = Field(
        ...,
        description="Complete replacement configuration for the selected MCP server",
    )


class McpCacheResetResponse(BaseModel):
    """Response model for resetting the MCP tools cache."""

    success: bool = Field(description="Whether the MCP tools cache was reset")
    message: str = Field(description="Human-readable reset status")


_MASKED_VALUE = "***"
_SENSITIVE_EXTRA_KEY_RE = re.compile(
    r"(^|_)(api_key|apikey|access_key|private_key|client_secret|secret|token|password|passwd|credential|credentials|authorization|bearer)(_|$)",
    re.IGNORECASE,
)


def _normalize_config_key(key: str) -> str:
    with_boundaries = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", key)
    with_boundaries = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", with_boundaries)
    return re.sub(r"[^a-z0-9]+", "_", with_boundaries.lower()).strip("_")


def _is_sensitive_extra_key(key: str) -> bool:
    return bool(_SENSITIVE_EXTRA_KEY_RE.search(_normalize_config_key(key)))


def _mask_sensitive_extra_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _MASKED_VALUE if _is_sensitive_extra_key(str(key)) else _mask_sensitive_extra_value(nested) for key, nested in value.items()}
    if isinstance(value, list):
        return [_mask_sensitive_extra_value(item) for item in value]
    return value


def _contains_masked_sensitive_extra_value(key: str, value: Any) -> bool:
    if value == _MASKED_VALUE and _is_sensitive_extra_key(key):
        return True
    if isinstance(value, dict):
        return any(_contains_masked_sensitive_extra_value(str(nested_key), nested_value) for nested_key, nested_value in value.items())
    if isinstance(value, list):
        return any(_contains_masked_sensitive_extra_value(key, item) for item in value)
    return False


def _ensure_no_masked_secrets(server: McpServerConfigResponse) -> None:
    """Reject request-only masked placeholders before config persistence."""

    def reject(location: str) -> None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Cannot persist masked secret placeholder for {location}; provide a real value.",
        )

    for key, value in server.env.items():
        if value == _MASKED_VALUE:
            reject(f"env key '{key}'")
    for key, value in server.headers.items():
        if value == _MASKED_VALUE:
            reject(f"header '{key}'")
    for key, value in (server.model_extra or {}).items():
        if _contains_masked_sensitive_extra_value(str(key), value):
            reject(f"extra config key '{key}'")

    if server.oauth is not None:
        if server.oauth.client_secret == _MASKED_VALUE:
            reject("oauth client_secret")
        if server.oauth.refresh_token == _MASKED_VALUE:
            reject("oauth refresh_token")
        for key, value in server.oauth.extra_token_params.items():
            if value == _MASKED_VALUE:
                reject(f"oauth extra_token_params key '{key}'")
        for key, value in (server.oauth.model_extra or {}).items():
            if _contains_masked_sensitive_extra_value(str(key), value):
                reject(f"oauth extra config key '{key}'")

    if server.user_auth is not None:
        for key, value in server.user_auth.users.items():
            if value == _MASKED_VALUE:
                reject(f"user_auth credential '{key}'")
        for key, value in (server.user_auth.model_extra or {}).items():
            if _contains_masked_sensitive_extra_value(str(key), value):
                reject(f"user_auth extra config key '{key}'")

    if server.headers_from_context is not None:
        for key, value in (server.headers_from_context.model_extra or {}).items():
            if _contains_masked_sensitive_extra_value(str(key), value):
                reject(f"headers_from_context extra config key '{key}'")

    for tool_name, tool_override in server.tools.items():
        for key, value in (tool_override.model_extra or {}).items():
            if _contains_masked_sensitive_extra_value(str(key), value):
                reject(f"tools override '{tool_name}' extra config key '{key}'")


def _merge_extra_value_preserving_masked(key: str, incoming_value: Any, existing_value: Any, *, existing_present: bool) -> Any:
    if incoming_value == _MASKED_VALUE and _is_sensitive_extra_key(key):
        if existing_present:
            return existing_value
        raise HTTPException(
            status_code=400,
            detail=f"Cannot set extra config key '{key}' to masked value '***'; provide a real value.",
        )

    if isinstance(incoming_value, dict) and isinstance(existing_value, dict):
        merged: dict[str, Any] = {}
        for nested_key, nested_value in incoming_value.items():
            nested_present = nested_key in existing_value
            merged[nested_key] = _merge_extra_value_preserving_masked(
                str(nested_key),
                nested_value,
                existing_value.get(nested_key),
                existing_present=nested_present,
            )
        return merged

    if isinstance(incoming_value, list) and isinstance(existing_value, list):
        if _contains_masked_sensitive_extra_value(key, incoming_value):
            if incoming_value != _mask_sensitive_extra_value(existing_value):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Cannot edit extra config array '{key}' while masked secrets remain; provide real values for every masked secret.",
                )
            return existing_value
        return incoming_value

    return incoming_value


def _validate_mcp_update_request(
    request: McpConfigUpdateRequest,
    *,
    enforce_execution_policy: bool = True,
) -> None:
    """Validate API-submitted MCP config before it is persisted.

    Local config files can still express arbitrary advanced setups, but the
    HTTP API is an untrusted boundary. Restricting stdio commands here reduces
    the blast radius of a compromised authenticated browser session.

    Command shape and code-injecting environment variables are invalid at the
    API boundary even while a server remains disabled. The allowlist and its
    companion argument screen are execution policy, so targeted offline edits
    may defer only those checks until the server is enabled.
    """
    allowed_commands = allowed_stdio_commands() if enforce_execution_policy else frozenset()
    for name, server in request.mcp_servers.items():
        transport_type = (server.type or "stdio").lower()
        if transport_type != "stdio":
            continue
        try:
            validate_mcp_stdio_launch(
                command=server.command,
                args=server.args,
                env_names=server.env,
                allowed_commands=allowed_commands,
                enforce_execution_policy=enforce_execution_policy,
            )
        except McpStdioLaunchPolicyViolation as exc:
            if exc.code == "command_required":
                detail = f"MCP server '{name}' with stdio transport requires a command."
            elif exc.code == "command_not_bare":
                detail = f"MCP server '{name}' command must be a single executable name; put parameters in args instead."
            elif exc.code == "command_not_allowed":
                allowed = ", ".join(exc.allowed_commands) or "<none>"
                detail = f"MCP server '{name}' uses disallowed stdio command '{exc.value}'. Allowed commands: {allowed}. Configure {MCP_STDIO_COMMAND_ALLOWLIST_ENV} to extend this list."
            elif exc.code == "argument_not_allowed":
                detail = f"MCP server '{name}' passes '{exc.value}' to '{server.command}', which would run arbitrary code. Point the server at a package or module instead."
            else:
                detail = f"MCP server '{name}' sets environment variable '{exc.value}', which would run arbitrary code at process startup."
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=detail,
            ) from exc


def _mask_server_config(server: McpServerConfigResponse) -> McpServerConfigResponse:
    """Return a copy of server config with sensitive fields masked.

    Masks env values, header values, and removes OAuth secrets so they
    are not exposed through the GET API endpoint.
    """
    masked_env = {k: _MASKED_VALUE for k in server.env}
    masked_headers = {k: _MASKED_VALUE for k in server.headers}
    masked_oauth = None
    if server.oauth is not None:
        # These values are arbitrary form fields sent directly to the token
        # endpoint. Treat the whole map as credential-bearing instead of
        # trying to recognize secrets from an open-ended key vocabulary.
        masked_extra_token_params = {key: _MASKED_VALUE for key in server.oauth.extra_token_params}
        masked_oauth_extra = {key: _MASKED_VALUE if _is_sensitive_extra_key(key) else _mask_sensitive_extra_value(value) for key, value in (server.oauth.model_extra or {}).items()}
        masked_oauth = server.oauth.model_copy(
            update={
                "client_secret": None,
                "refresh_token": None,
                "extra_token_params": masked_extra_token_params,
                **masked_oauth_extra,
            }
        )
    masked_user_auth = None
    if server.user_auth is not None:
        # Extras inside user_auth get the same sensitive-key masking as
        # server-level extras: they round-trip through PUT (extra="allow"), so
        # an operator-stored secret-bearing key must not come back in
        # cleartext from GET while the identical key at server level is masked.
        masked_ua_extra = {key: _MASKED_VALUE if _is_sensitive_extra_key(key) else _mask_sensitive_extra_value(value) for key, value in (server.user_auth.model_extra or {}).items()}
        masked_user_auth = server.user_auth.model_copy(update={"users": {k: _MASKED_VALUE for k in server.user_auth.users}, **masked_ua_extra})
    masked_headers_from_context = None
    if server.headers_from_context is not None:
        # The declared fields hold names only and stay in cleartext — masking
        # them would show operators `***` where a header name belongs. Extras
        # get the same treatment as everywhere else, since `extra="allow"` lets
        # an operator store a secret-bearing key that round-trips through PUT.
        masked_ch_extra = {key: _MASKED_VALUE if _is_sensitive_extra_key(key) else _mask_sensitive_extra_value(value) for key, value in (server.headers_from_context.model_extra or {}).items()}
        masked_headers_from_context = server.headers_from_context.model_copy(update=masked_ch_extra)
    masked_tools = {}
    for tool_name, tool_override in server.tools.items():
        masked_tool_extra = {key: _MASKED_VALUE if _is_sensitive_extra_key(key) else _mask_sensitive_extra_value(value) for key, value in (tool_override.model_extra or {}).items()}
        masked_tools[tool_name] = tool_override.model_copy(update=masked_tool_extra)
    masked_extra = {key: _MASKED_VALUE if _is_sensitive_extra_key(key) else _mask_sensitive_extra_value(value) for key, value in (server.model_extra or {}).items()}
    return server.model_copy(
        update={
            "env": masked_env,
            "headers": masked_headers,
            "oauth": masked_oauth,
            "user_auth": masked_user_auth,
            "headers_from_context": masked_headers_from_context,
            "tools": masked_tools,
            **masked_extra,
        }
    )


def _merge_preserving_secrets(
    incoming: McpServerConfigResponse,
    existing: McpServerConfigResponse,
    *,
    preserve_omitted_fields: bool = True,
) -> McpServerConfigResponse:
    """Merge incoming config with existing, preserving secrets masked by GET.

    When the frontend toggles ``enabled`` it round-trips the full config:
    GET (masked) → modify enabled → PUT (masked values sent back).
    This function ensures masked values (``***``) are replaced with the
    real secrets from the current on-disk config.

    ``***`` is only accepted for keys that already exist in *existing*.
    New keys must provide a real value.

    For OAuth secrets, ``None`` means "preserve the existing stored value"
    so masked GET responses can be safely round-tripped. To explicitly clear
    a stored secret, clients may send an empty string, which is converted
    to ``None`` before persisting.

    ``preserve_omitted_fields`` keeps the legacy bulk PUT's partial-update
    behavior. Targeted PUT disables it because that endpoint is a complete
    replacement: omissions must delete/reset ordinary fields, while explicit
    masked placeholders still restore only their matching stored secrets.
    """
    merged_env = {}
    for k, v in incoming.env.items():
        if v == _MASKED_VALUE:
            if k in existing.env:
                merged_env[k] = existing.env[k]
            else:
                raise HTTPException(
                    status_code=400,
                    detail=f"Cannot set env key '{k}' to masked value '***'; provide a real value.",
                )
        else:
            merged_env[k] = v

    merged_headers = {}
    for k, v in incoming.headers.items():
        if v == _MASKED_VALUE:
            if k in existing.headers:
                merged_headers[k] = existing.headers[k]
            else:
                raise HTTPException(
                    status_code=400,
                    detail=f"Cannot set header '{k}' to masked value '***'; provide a real value.",
                )
        else:
            merged_headers[k] = v

    merged_oauth = incoming.oauth
    if incoming.oauth is not None:
        incoming_oauth = incoming.oauth
        base_oauth = existing.oauth
        base_extra_token_params = base_oauth.extra_token_params if base_oauth is not None else {}
        merged_extra_token_params: dict[str, str] = {}
        for key, value in incoming_oauth.extra_token_params.items():
            if value == _MASKED_VALUE:
                if key not in base_extra_token_params:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Cannot set oauth extra_token_params key '{key}' to masked value '***'; provide a real value.",
                    )
                merged_extra_token_params[key] = base_extra_token_params[key]
            else:
                merged_extra_token_params[key] = value
        if preserve_omitted_fields and "extra_token_params" not in incoming_oauth.model_fields_set:
            merged_extra_token_params = dict(base_extra_token_params)

        base_oauth_extra = (base_oauth.model_extra or {}) if base_oauth is not None else {}
        merged_oauth_extra: dict[str, Any] = {}
        for key, value in (incoming_oauth.model_extra or {}).items():
            merged_oauth_extra[key] = _merge_extra_value_preserving_masked(
                key,
                value,
                base_oauth_extra.get(key),
                existing_present=key in base_oauth_extra,
            )
        if preserve_omitted_fields:
            for key, value in base_oauth_extra.items():
                if key not in (incoming_oauth.model_extra or {}):
                    merged_oauth_extra[key] = value

        if base_oauth is not None:
            # None = preserve (masked round-trip), "" = explicitly clear,
            # else = new value.
            merged_client_secret = base_oauth.client_secret if incoming_oauth.client_secret is None else (None if incoming_oauth.client_secret == "" else incoming_oauth.client_secret)
            merged_refresh_token = base_oauth.refresh_token if incoming_oauth.refresh_token is None else (None if incoming_oauth.refresh_token == "" else incoming_oauth.refresh_token)
        else:
            merged_client_secret = incoming_oauth.client_secret
            merged_refresh_token = incoming_oauth.refresh_token
        merged_oauth = incoming_oauth.model_copy(
            update={
                "client_secret": merged_client_secret,
                "refresh_token": merged_refresh_token,
                "extra_token_params": merged_extra_token_params,
                **merged_oauth_extra,
            }
        )
    merged_user_auth = incoming.user_auth
    if incoming.user_auth is not None:
        incoming_ua = incoming.user_auth
        base = existing.user_auth
        set_fields = incoming_ua.model_fields_set
        base_extra = (base.model_extra or {}) if base is not None else {}
        merged_extra: dict[str, Any] = {}
        for key, value in (incoming_ua.model_extra or {}).items():
            merged_extra[key] = _merge_extra_value_preserving_masked(
                key,
                value,
                base_extra.get(key),
                existing_present=key in base_extra,
            )

        existing_users = base.users if base is not None else {}
        merged_users = {}
        for k, v in incoming_ua.users.items():
            if v == _MASKED_VALUE:
                if k in existing_users:
                    merged_users[k] = existing_users[k]
                else:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Cannot set user_auth credential for '{k}' to masked value '***'; provide a real value.",
                    )
            else:
                merged_users[k] = v

        if preserve_omitted_fields:
            # A partial user_auth payload (for example only enabled=false)
            # inherits omitted sub-fields under the legacy bulk PUT contract.
            effective: dict[str, Any] = {}
            if base is not None:
                effective.update({name: getattr(base, name) for name in ("enabled", "header", "users", "on_missing")})
                effective.update(base_extra)
            for name in ("enabled", "header", "on_missing"):
                if name in set_fields:
                    effective[name] = getattr(incoming_ua, name)
            effective.update(merged_extra)
            if "users" in set_fields:
                effective["users"] = merged_users
            merged_user_auth = McpUserScopedAuthConfigResponse(**effective)
        else:
            # Targeted PUT is a complete replacement. Start from the incoming
            # block so omitted ordinary sub-fields reset and omitted extras or
            # users disappear; only explicit masked values above are restored.
            merged_user_auth = incoming_ua.model_copy(
                update={"users": merged_users, **merged_extra},
            )

    merged_context_headers = incoming.headers_from_context
    if incoming.headers_from_context is not None:
        incoming_ch = incoming.headers_from_context
        base_ch = existing.headers_from_context
        set_fields = incoming_ch.model_fields_set
        # Extras are masked by GET (see _mask_server_config), so a round-trip
        # PUT must swap masked sentinel values back for the stored ones — the
        # same contract user_auth extras and server-level extras get.
        base_ch_extra = (base_ch.model_extra or {}) if base_ch is not None else {}
        merged_ch_extra: dict[str, Any] = {}
        for key, value in (incoming_ch.model_extra or {}).items():
            merged_ch_extra[key] = _merge_extra_value_preserving_masked(
                key,
                value,
                base_ch_extra.get(key),
                existing_present=key in base_ch_extra,
            )

        if preserve_omitted_fields:
            # The legacy bulk PUT accepts partial nested blocks. Only fields
            # the request set are replaced, while omitted fields carry over.
            effective: dict[str, Any] = {}
            if base_ch is not None:
                effective.update({name: getattr(base_ch, name) for name in ("enabled", "headers", "on_missing")})
                effective.update(base_ch_extra)
            for name in ("enabled", "headers", "on_missing"):
                if name in set_fields:
                    effective[name] = getattr(incoming_ch, name)
            effective.update(merged_ch_extra)
            merged_context_headers = McpContextHeadersConfigResponse(**effective)
        else:
            # The targeted PUT is a complete replacement. Omitted ordinary
            # fields and extras reset/disappear; explicit masked extras alone
            # are restored from the stored block.
            merged_context_headers = incoming_ch.model_copy(update=merged_ch_extra)

    merged_tools = {}
    for tool_name, incoming_tool in incoming.tools.items():
        base_tool = existing.tools.get(tool_name)
        base_tool_extra = (base_tool.model_extra or {}) if base_tool is not None else {}
        merged_tool_extra: dict[str, Any] = {}
        for key, value in (incoming_tool.model_extra or {}).items():
            merged_tool_extra[key] = _merge_extra_value_preserving_masked(
                key,
                value,
                base_tool_extra.get(key),
                existing_present=key in base_tool_extra,
            )
        if preserve_omitted_fields:
            for key, value in base_tool_extra.items():
                if key not in (incoming_tool.model_extra or {}):
                    merged_tool_extra[key] = value
        merged_routing = incoming_tool.routing
        if preserve_omitted_fields and base_tool is not None and "routing" not in incoming_tool.model_fields_set:
            merged_routing = base_tool.routing
        merged_tools[tool_name] = incoming_tool.model_copy(
            update={"routing": merged_routing, **merged_tool_extra},
        )

    update = {
        "env": merged_env,
        "headers": merged_headers,
        "oauth": merged_oauth,
        "user_auth": merged_user_auth,
        "headers_from_context": merged_context_headers,
        "tools": merged_tools,
    }
    if preserve_omitted_fields and "user_auth" not in incoming.model_fields_set:
        update["user_auth"] = existing.user_auth
    if preserve_omitted_fields and "headers_from_context" not in incoming.model_fields_set:
        update["headers_from_context"] = existing.headers_from_context
    if preserve_omitted_fields and "routing" not in incoming.model_fields_set:
        update["routing"] = existing.routing
    if preserve_omitted_fields and "tools" not in incoming.model_fields_set:
        update["tools"] = existing.tools
    incoming_extra = incoming.model_extra or {}
    existing_extra = existing.model_extra or {}
    for key, value in incoming_extra.items():
        update[key] = _merge_extra_value_preserving_masked(
            key,
            value,
            existing_extra.get(key),
            existing_present=key in existing_extra,
        )
    if preserve_omitted_fields:
        for key, value in (existing.model_extra or {}).items():
            if key not in (incoming.model_extra or {}):
                update[key] = value
    merged = incoming.model_copy(update=update)
    _ensure_no_masked_secrets(merged)
    return merged


@router.get(
    "/mcp/config",
    response_model=McpConfigResponse,
    summary="Get MCP Configuration",
    description="Retrieve the current Model Context Protocol (MCP) server configurations.",
)
async def get_mcp_configuration(request: Request) -> McpConfigResponse:
    """Get the current MCP configuration.

    Returns:
        The current MCP configuration with all servers.

    Example:
        ```json
        {
            "mcp_servers": {
                "github": {
                    "enabled": true,
                    "command": "npx",
                    "args": ["-y", "@modelcontextprotocol/server-github"],
                    "env": {"GITHUB_TOKEN": "***"},
                    "description": "GitHub MCP server for repository operations"
                }
            }
        }
        ```
    """
    await require_admin_user(request, detail=_ADMIN_REQUIRED_DETAIL)

    raw_servers = await asyncio.to_thread(_load_raw_mcp_server_responses)
    servers = {name: _mask_server_config(server) for name, server in raw_servers.items()}
    return McpConfigResponse(mcp_servers=servers)


def _raise_invalid_mcp_configuration(detail: str, *, cause: Exception | None = None) -> NoReturn:
    error = HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=f"Invalid MCP configuration: {detail}",
    )
    if cause is not None:
        raise error from cause
    raise error


def _validation_error_summary(exc: ValidationError) -> str:
    errors = exc.errors(include_url=False, include_input=False)
    return "; ".join(f"{'.'.join(str(part) for part in error['loc']) or 'config'}: {error['msg']}" for error in errors)


def _mcp_server_response_from_raw(server_name: str, raw_server: Any) -> McpServerConfigResponse:
    try:
        return McpServerConfigResponse.model_validate(raw_server)
    except ValidationError as exc:
        _raise_invalid_mcp_configuration(f"mcpServers.{server_name}: {_validation_error_summary(exc)}", cause=exc)


def _validate_extensions_config_candidate(raw_data: dict) -> None:
    """Reject a runtime-invalid candidate without changing its placeholders."""
    try:
        resolved_data = ExtensionsConfig.resolve_env_variables(raw_data)
        ExtensionsConfig.model_validate(resolved_data)
    except ValidationError as exc:
        _raise_invalid_mcp_configuration(_validation_error_summary(exc), cause=exc)


def _apply_mcp_config_update(body: McpConfigUpdateRequest) -> dict:
    """Worker-thread body for :func:`update_mcp_configuration`.

    Resolving the config path, the existence probe, reading the raw JSON,
    writing the merged config, and reloading it are all blocking filesystem IO
    that must stay off the event loop. The merge is pure in-memory work but
    lives here too so the whole read-modify-write is a single worker hop.
    Returns the reloaded MCP server configs for the response.
    """
    # Resolve before entering the critical section so every writer locks the
    # same sidecar path for the complete read-modify-write cycle.
    config_path = ExtensionsConfig.resolve_config_path()
    if config_path is None:
        config_path = project_root() / "extensions_config.json"
        logger.info(f"No existing extensions config found. Creating new config at: {config_path}")

    with extensions_config_write_lock, extensions_config_file_lock(config_path):
        # Load raw (un-resolved) JSON from disk to use as the merge source.
        # This preserves $VAR placeholders in env values and top-level keys
        # like mcpInterceptors that would otherwise be lost.
        raw_data = _load_raw_extensions_config(config_path, create=True)
        raw_servers = _raw_mcp_servers(raw_data)
        raw_other_keys: dict = {}
        raw_skills: dict[str, dict] | None = None
        if isinstance(raw_data.get("skills"), dict):
            raw_skills = raw_data["skills"]
        # Preserve any top-level keys beyond mcpServers/skills
        for key, value in raw_data.items():
            if key not in ("mcpServers", "skills"):
                raw_other_keys[key] = value

        # Merge incoming server configs with raw on-disk secrets
        merged_servers: dict[str, McpServerConfigResponse] = {}
        for name, incoming in body.mcp_servers.items():
            raw_server = raw_servers.get(name)
            if raw_server is not None:
                merged = _merge_preserving_secrets(
                    incoming,
                    _mcp_server_response_from_raw(name, raw_server),
                )
            else:
                merged = incoming
            _ensure_no_masked_secrets(merged)
            merged_servers[name] = merged

        # Build config data preserving all top-level keys from the original file
        config_data = dict(raw_other_keys)
        config_data["mcpServers"] = {name: server.model_dump() for name, server in merged_servers.items()}
        if raw_skills is None:
            current_config = get_extensions_config()
            raw_skills = {name: {"enabled": skill.enabled} for name, skill in current_config.skills.items()}
        config_data["skills"] = raw_skills

        _validate_extensions_config_candidate(config_data)
        atomic_write_extensions_config(config_path, config_data)

        logger.info(f"MCP configuration updated and saved to: {config_path}")

        # Reload the Gateway configuration and update the global cache. The
        # agent runtime lives in Gateway, so this keeps API reads and tool
        # execution aligned after extensions_config.json changes.
        reload_extensions_config()
        return _mcp_server_responses_from_raw(config_data)


def _apply_mcp_server_state_update(body: McpServerStateUpdateRequest) -> dict:
    """Update one server state while preserving the raw extensions config."""
    config_path = ExtensionsConfig.resolve_config_path()
    if config_path is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"MCP server '{body.server_name}' not found",
        )

    with extensions_config_write_lock, extensions_config_file_lock(config_path):
        if not config_path.exists():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"MCP server '{body.server_name}' not found",
            )

        raw_data = _load_raw_extensions_config(config_path, create=False)
        raw_servers = _raw_mcp_servers(raw_data)
        if body.server_name not in raw_servers:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"MCP server '{body.server_name}' not found",
            )
        raw_server = raw_servers[body.server_name]
        target_server = _mcp_server_response_from_raw(body.server_name, raw_server)

        if body.enabled:
            _validate_mcp_update_request(
                McpConfigUpdateRequest(
                    mcp_servers={body.server_name: target_server},
                )
            )

        raw_server["enabled"] = body.enabled
        _validate_extensions_config_candidate(raw_data)
        atomic_write_extensions_config(config_path, raw_data)

        logger.info("MCP server %s enabled state updated to %s", body.server_name, body.enabled)
        reload_extensions_config()
        return _mcp_server_responses_from_raw(raw_data)


def _mcp_config_path(*, create: bool) -> Path:
    """Resolve the shared extensions config path for a targeted mutation."""
    config_path = ExtensionsConfig.resolve_config_path()
    if config_path is None:
        if create:
            config_path = project_root() / "extensions_config.json"
            logger.info("No existing extensions config found. Creating new config at: %s", config_path)
        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="MCP configuration not found",
            )
    return config_path


def _load_raw_extensions_config(config_path: Path, *, create: bool) -> dict:
    if config_path.exists():
        try:
            with open(config_path, encoding="utf-8") as f:
                raw_data = json.load(f)
        except json.JSONDecodeError as exc:
            _raise_invalid_mcp_configuration(
                f"Extensions configuration is not valid JSON: {exc.msg} at line {exc.lineno} column {exc.colno}",
                cause=exc,
            )
        if not isinstance(raw_data, dict):
            _raise_invalid_mcp_configuration("Extensions configuration must be a JSON object")
        return raw_data
    if not create:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="MCP configuration not found",
        )
    return {}


def _raw_mcp_servers(raw_data: dict) -> dict[str, dict]:
    raw_servers = raw_data.get("mcpServers", {})
    if not isinstance(raw_servers, dict):
        _raise_invalid_mcp_configuration("`mcpServers` must be a JSON object")
    return raw_servers


def _mcp_server_responses_from_raw(raw_data: dict) -> dict[str, McpServerConfigResponse]:
    """Build editable API models without expanding environment placeholders."""
    return {name: _mcp_server_response_from_raw(name, server) for name, server in _raw_mcp_servers(raw_data).items()}


def _load_raw_mcp_server_responses() -> dict[str, McpServerConfigResponse]:
    """Read editable MCP server definitions under the shared config lock."""
    config_path = ExtensionsConfig.resolve_config_path()
    if config_path is None:
        return {}

    with extensions_config_write_lock, extensions_config_file_lock(config_path):
        raw_data = _load_raw_extensions_config(config_path, create=False)
        return _mcp_server_responses_from_raw(raw_data)


def _ensure_skills_key(raw_data: dict) -> None:
    if isinstance(raw_data.get("skills"), dict):
        return
    current_config = get_extensions_config()
    raw_data["skills"] = {name: {"enabled": skill.enabled} for name, skill in current_config.skills.items()}


def _apply_mcp_servers_create(body: McpConfigUpdateRequest) -> dict:
    """Atomically add servers without replacing entries already on disk."""
    config_path = _mcp_config_path(create=True)
    with extensions_config_write_lock, extensions_config_file_lock(config_path):
        raw_data = _load_raw_extensions_config(config_path, create=True)
        raw_servers = _raw_mcp_servers(raw_data)
        duplicate = next((name for name in body.mcp_servers if name in raw_servers), None)
        if duplicate is not None:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"MCP server '{duplicate}' already exists",
            )

        for name, incoming in body.mcp_servers.items():
            _ensure_no_masked_secrets(incoming)
            raw_servers[name] = incoming.model_dump()
        raw_data["mcpServers"] = raw_servers
        _ensure_skills_key(raw_data)
        _validate_extensions_config_candidate(raw_data)
        atomic_write_extensions_config(config_path, raw_data)

        logger.info("Added MCP servers: %s", ", ".join(body.mcp_servers))
        reload_extensions_config()
        return _mcp_server_responses_from_raw(raw_data)


def _apply_mcp_server_config_update(body: McpServerConfigUpdateRequest) -> dict:
    """Atomically replace one server while preserving concurrent sibling edits."""
    config_path = _mcp_config_path(create=False)
    with extensions_config_write_lock, extensions_config_file_lock(config_path):
        raw_data = _load_raw_extensions_config(config_path, create=False)
        raw_servers = _raw_mcp_servers(raw_data)
        if body.server_name not in raw_servers:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"MCP server '{body.server_name}' not found",
            )
        existing_server = _mcp_server_response_from_raw(body.server_name, raw_servers[body.server_name])

        merged = _merge_preserving_secrets(
            body.server,
            existing_server,
            preserve_omitted_fields=False,
        )
        _ensure_no_masked_secrets(merged)
        raw_servers[body.server_name] = merged.model_dump()
        raw_data["mcpServers"] = raw_servers
        _validate_extensions_config_candidate(raw_data)
        atomic_write_extensions_config(config_path, raw_data)

        logger.info("Updated MCP server: %s", body.server_name)
        reload_extensions_config()
        return _mcp_server_responses_from_raw(raw_data)


def _apply_mcp_server_delete(server_name: str) -> dict:
    """Atomically remove one server while preserving every sibling entry."""
    config_path = _mcp_config_path(create=False)
    with extensions_config_write_lock, extensions_config_file_lock(config_path):
        raw_data = _load_raw_extensions_config(config_path, create=False)
        raw_servers = _raw_mcp_servers(raw_data)
        if server_name not in raw_servers:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"MCP server '{server_name}' not found",
            )

        del raw_servers[server_name]
        raw_data["mcpServers"] = raw_servers
        _validate_extensions_config_candidate(raw_data)
        atomic_write_extensions_config(config_path, raw_data)

        logger.info("Deleted MCP server: %s", server_name)
        reload_extensions_config()
        return _mcp_server_responses_from_raw(raw_data)


@router.post(
    "/mcp/cache/reset",
    response_model=McpCacheResetResponse,
    summary="Reset MCP Tools Cache",
    description=("Reset cached MCP tools and pooled sessions process-wide so tools are reloaded on next use. This affects all threads and users in the current Gateway process."),
)
async def reset_mcp_tools_cache_endpoint(request: Request) -> McpCacheResetResponse:
    """Reset cached MCP tools and persistent sessions process-wide.

    The next agent run or tool lookup will reload tools from the configured MCP
    servers. This affects all threads and users in the current Gateway process,
    and avoids relying on extensions_config.json mtime changes.
    """
    await require_admin_user(request, detail=_ADMIN_REQUIRED_DETAIL)
    reset_mcp_tools_cache()
    return McpCacheResetResponse(
        success=True,
        message="MCP tools cache reset. Tools will reload on next use.",
    )


@router.put(
    "/mcp/config",
    response_model=McpConfigResponse,
    summary="Update MCP Configuration",
    description="Update Model Context Protocol (MCP) server configurations and save to file.",
)
async def update_mcp_configuration(request: Request, body: McpConfigUpdateRequest) -> McpConfigResponse:
    """Update the MCP configuration.

    This will:
    1. Save the new configuration to the mcp_config.json file
    2. Reload the configuration cache
    3. Reset MCP tools cache to trigger reinitialization

    Args:
        request: The new MCP configuration to save.

    Returns:
        The updated MCP configuration.

    Raises:
        HTTPException: 500 if the configuration file cannot be written.

    Example Request:
        ```json
        {
            "mcp_servers": {
                "github": {
                    "enabled": true,
                    "command": "npx",
                    "args": ["-y", "@modelcontextprotocol/server-github"],
                    "env": {"GITHUB_TOKEN": "$GITHUB_TOKEN"},
                    "description": "GitHub MCP server for repository operations"
                }
            }
        }
        ```
    """
    try:
        await require_admin_user(request, detail=_ADMIN_REQUIRED_DETAIL)
        reject_direct_tool_plane_mutation(request, surface="mcp_configuration")
        _validate_mcp_update_request(body)

        # Offload the blocking read-modify-write of extensions_config.json
        # (path resolve, existence probe, raw read, merged write, reload). The
        # worker takes extensions_config_write_lock for the whole RMW, so it stays
        # atomic and serialized against the skills router (the other writer of
        # this file) even if this request is cancelled mid-write.
        reloaded_servers = await asyncio.to_thread(_apply_mcp_config_update, body)

        servers = {name: _mask_server_config(McpServerConfigResponse(**server.model_dump())) for name, server in reloaded_servers.items()}
        reset_mcp_tools_cache()
        return McpConfigResponse(mcp_servers=servers)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to update MCP configuration: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to update MCP configuration: {str(e)}")


@router.post(
    "/mcp/config/servers",
    response_model=McpConfigResponse,
    summary="Add MCP Servers",
    description="Add one or more MCP servers without replacing existing configurations.",
)
async def create_mcp_servers(request: Request, body: McpConfigUpdateRequest) -> McpConfigResponse:
    """Add servers atomically and reject names that already exist."""
    try:
        await require_admin_user(request, detail=_ADMIN_REQUIRED_DETAIL)
        reject_direct_tool_plane_mutation(request, surface="mcp_server_create")
        _validate_mcp_update_request(body)
        reloaded_servers = await asyncio.to_thread(_apply_mcp_servers_create, body)

        servers = {name: _mask_server_config(McpServerConfigResponse(**server.model_dump())) for name, server in reloaded_servers.items()}
        reset_mcp_tools_cache()
        return McpConfigResponse(mcp_servers=servers)
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to add MCP servers: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to add MCP servers: {str(e)}")


@router.put(
    "/mcp/config/server",
    response_model=McpConfigResponse,
    summary="Update MCP Server",
    description="Replace one MCP server without replacing sibling configurations.",
)
async def update_mcp_server(request: Request, body: McpServerConfigUpdateRequest) -> McpConfigResponse:
    """Update one existing server and reload the MCP tool cache."""
    try:
        await require_admin_user(request, detail=_ADMIN_REQUIRED_DETAIL)
        reject_direct_tool_plane_mutation(request, surface="mcp_server_update")
        _validate_mcp_update_request(
            McpConfigUpdateRequest(mcp_servers={body.server_name: body.server}),
            enforce_execution_policy=body.server.enabled,
        )
        reloaded_servers = await asyncio.to_thread(_apply_mcp_server_config_update, body)

        servers = {name: _mask_server_config(McpServerConfigResponse(**server.model_dump())) for name, server in reloaded_servers.items()}
        reset_mcp_tools_cache()
        return McpConfigResponse(mcp_servers=servers)
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to update MCP server %s: %s", body.server_name, e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to update MCP server: {str(e)}")


@router.delete(
    "/mcp/config/servers/{server_name:path}",
    response_model=McpConfigResponse,
    summary="Delete MCP Server",
    description="Delete one MCP server without replacing sibling configurations.",
)
async def delete_mcp_server(request: Request, server_name: str) -> McpConfigResponse:
    """Delete one existing server and reload the MCP tool cache."""
    try:
        await require_admin_user(request, detail=_ADMIN_REQUIRED_DETAIL)
        reject_direct_tool_plane_mutation(request, surface="mcp_server_delete")
        reloaded_servers = await asyncio.to_thread(_apply_mcp_server_delete, server_name)

        servers = {name: _mask_server_config(McpServerConfigResponse(**server.model_dump())) for name, server in reloaded_servers.items()}
        reset_mcp_tools_cache()
        return McpConfigResponse(mcp_servers=servers)
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to delete MCP server %s: %s", server_name, e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to delete MCP server: {str(e)}")


@router.patch(
    "/mcp/config",
    response_model=McpConfigResponse,
    summary="Update MCP Server State",
    description="Enable or disable one MCP server without replacing the full extensions configuration.",
)
async def update_mcp_server_state(request: Request, body: McpServerStateUpdateRequest) -> McpConfigResponse:
    """Enable or disable one MCP server and reload the MCP tool cache."""
    try:
        await require_admin_user(request, detail=_ADMIN_REQUIRED_DETAIL)
        reject_direct_tool_plane_mutation(request, surface="mcp_server_state")
        reloaded_servers = await asyncio.to_thread(_apply_mcp_server_state_update, body)

        servers = {name: _mask_server_config(McpServerConfigResponse(**server.model_dump())) for name, server in reloaded_servers.items()}
        reset_mcp_tools_cache()
        return McpConfigResponse(mcp_servers=servers)
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to update MCP server %s state: %s", body.server_name, e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to update MCP server state: {str(e)}")
