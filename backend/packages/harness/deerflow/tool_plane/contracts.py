"""Versioned, secret-free contracts for governed skill and MCP material.

These contracts intentionally describe approved material rather than the files
that happen to hold it.  They are the only values persisted in revision and
audit rows, so canonicalization must fail before a credential value can cross
that boundary.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Literal
from urllib.parse import parse_qsl, urlsplit

ToolPlaneScopeKind = Literal["deployment_base", "user_overlay"]

_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
_SELECTOR = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,127}\Z")
_BINDING_REFERENCE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,255}\Z")
_SEMANTIC_VERSION = re.compile(
    r"(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)"
    r"(?:-(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)"
    r"(?:\.(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*))*)?"
    r"(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?\Z"
)
_TRANSPORTS = frozenset({"stdio", "sse", "http", "streamable_http"})
_MAX_SERVERS = 128
_MAX_SKILLS = 512
_MAX_TOOLS = 512
_MAX_SUMMARY_BYTES = 2048
_SECRET_FIELD = re.compile(
    r"(?:authorization|api[-_]?key|credential|password|secret|token|assertion)",
    re.IGNORECASE,
)


def _canonical_json_value(value: object) -> object:
    if isinstance(value, Mapping):
        return {str(key): _canonical_json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_canonical_json_value(item) for item in value]
    return value


def canonical_json_bytes(value: object) -> bytes:
    """Encode a JSON projection using the tool-plane canonical form."""

    return json.dumps(
        _canonical_json_value(value),
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def canonical_tool_plane_digest(value: object) -> str:
    """Return the lowercase SHA-256 digest of canonical tool-plane JSON."""

    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def is_semantic_version(value: object) -> bool:
    """Return whether ``value`` is one bounded SemVer 2.0 identifier."""

    return isinstance(value, str) and len(value.encode("utf-8")) <= 128 and _SEMANTIC_VERSION.fullmatch(value) is not None


class ToolPlaneRevisionError(RuntimeError):
    """Stable error boundary whose message and details are safe to expose."""

    def __init__(
        self,
        code: str,
        *,
        safe_details: Mapping[str, object] | None = None,
    ) -> None:
        self.code = code
        # Details are already a deliberately tiny safe projection.  Keep them
        # directly JSON serializable because routers and audit sinks consume
        # this boundary without custom encoders.
        self.safe_details = dict(safe_details or {})
        super().__init__(code)


def _require_mapping(value: object, *, field_name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ToolPlaneRevisionError(
            "validation_failed",
            safe_details={"field": field_name, "reason": "mapping_required"},
        )
    return value


def _identifier(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or _IDENTIFIER.fullmatch(value) is None:
        raise ToolPlaneRevisionError(
            "validation_failed",
            safe_details={"field": field_name, "reason": "invalid_identifier"},
        )
    return value


def _digest(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
        raise ToolPlaneRevisionError(
            "validation_failed",
            safe_details={"field": field_name, "reason": "invalid_digest"},
        )
    return value


def _optional_digest(value: object, *, field_name: str) -> str | None:
    if value is None:
        return None
    return _digest(value, field_name=field_name)


def _bounded_summary(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or len(value.encode("utf-8")) > _MAX_SUMMARY_BYTES:
        raise ToolPlaneRevisionError(
            "validation_failed",
            safe_details={"field": "change_summary", "reason": "value_too_large"},
        )
    return value


def _bool(value: object, *, field_name: str, default: bool = True) -> bool:
    if value is None:
        return default
    if type(value) is not bool:
        raise ToolPlaneRevisionError(
            "validation_failed",
            safe_details={"field": field_name, "reason": "boolean_required"},
        )
    return value


def _selector(value: object, *, field_name: str) -> str:
    """Return an environment selector without ever echoing a rejected value."""

    name: str | None = None
    if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
        name = value[2:-1]
    elif isinstance(value, str) and value.startswith("$"):
        name = value[1:]
    if name is None or _SELECTOR.fullmatch(name) is None:
        raise ToolPlaneRevisionError(
            "secret_value_present",
            safe_details={"field": field_name, "reason": "selector_required"},
        )
    return f"env:{name}"


def _secret_value_error(field_name: str) -> ToolPlaneRevisionError:
    return ToolPlaneRevisionError(
        "secret_value_present",
        safe_details={"field": field_name, "reason": "selector_required"},
    )


def _safe_url(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.startswith(("https://", "http://")) or len(value) > 2048:
        raise ToolPlaneRevisionError(
            "validation_failed",
            safe_details={"field": field_name, "reason": "invalid_url"},
        )
    parsed = urlsplit(value)
    if parsed.username is not None or parsed.password is not None or any(_SECRET_FIELD.search(key) for key, _ in parse_qsl(parsed.query, keep_blank_values=True)):
        raise _secret_value_error(field_name)
    return value


def _timeout(value: object, *, field_name: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ToolPlaneRevisionError(
            "validation_failed",
            safe_details={"field": field_name, "reason": "invalid_timeout"},
        )
    result = float(value)
    if not math.isfinite(result) or result <= 0 or result > 3600:
        raise ToolPlaneRevisionError(
            "validation_failed",
            safe_details={"field": field_name, "reason": "invalid_timeout"},
        )
    return result


def _canonical_routing(value: object, *, field_name: str) -> dict[str, object]:
    mapping = _require_mapping(value or {}, field_name=field_name)
    if set(mapping) - {"mode", "priority", "keywords"}:
        raise ToolPlaneRevisionError(
            "validation_failed",
            safe_details={"field": field_name, "reason": "unknown_fields"},
        )
    mode = mapping.get("mode", "off")
    priority = mapping.get("priority", 0)
    if mode not in {"off", "prefer"} or type(priority) is not int or not 0 <= priority <= 100:
        raise ToolPlaneRevisionError(
            "validation_failed",
            safe_details={"field": field_name, "reason": "invalid_routing"},
        )
    return {
        "mode": mode,
        "priority": priority,
        "keywords": _canonical_string_list(
            mapping.get("keywords"),
            field_name=f"{field_name}.keywords",
            maximum=128,
        ),
    }


def _canonical_task_toolsets(value: object, *, field_name: str) -> list[dict[str, str]]:
    if value is None:
        return []
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence) or len(value) > 64:
        raise ToolPlaneRevisionError(
            "validation_failed",
            safe_details={"field": field_name, "reason": "list_required"},
        )
    result: list[dict[str, str]] = []
    for index, raw in enumerate(value):
        item = _require_mapping(raw, field_name=f"{field_name}.{index}")
        expected = {"name", "submit_tool", "status_tool", "cancel_tool"}
        if set(item) != expected:
            raise ToolPlaneRevisionError(
                "validation_failed",
                safe_details={"field": f"{field_name}.{index}", "reason": "invalid_fields"},
            )
        result.append({key: _identifier(item[key], field_name=f"{field_name}.{index}.{key}") for key in sorted(expected)})
    return sorted(result, key=lambda item: item["name"])


def _canonical_string_list(
    value: object,
    *,
    field_name: str,
    maximum: int,
    identifiers: bool = False,
) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise ToolPlaneRevisionError(
            "validation_failed",
            safe_details={"field": field_name, "reason": "list_required"},
        )
    if len(value) > maximum:
        raise ToolPlaneRevisionError(
            "validation_failed",
            safe_details={"field": field_name, "reason": "too_many_items"},
        )
    result: set[str] = set()
    for item in value:
        if not isinstance(item, str) or not item or len(item.encode("utf-8")) > 1024:
            raise ToolPlaneRevisionError(
                "validation_failed",
                safe_details={"field": field_name, "reason": "invalid_item"},
            )
        if identifiers:
            _identifier(item, field_name=field_name)
        result.add(item)
    return sorted(result)


def _canonical_ordered_string_list(
    value: object,
    *,
    field_name: str,
    maximum: int,
) -> list[str]:
    """Validate a sequence whose ordering and repetition are semantic."""

    if value is None:
        return []
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise ToolPlaneRevisionError(
            "validation_failed",
            safe_details={"field": field_name, "reason": "list_required"},
        )
    if len(value) > maximum:
        raise ToolPlaneRevisionError(
            "validation_failed",
            safe_details={"field": field_name, "reason": "too_many_items"},
        )
    result: list[str] = []
    for item in value:
        if not isinstance(item, str) or not item or len(item.encode("utf-8")) > 1024:
            raise ToolPlaneRevisionError(
                "validation_failed",
                safe_details={"field": field_name, "reason": "invalid_item"},
            )
        result.append(item)
    return result


def _canonical_skill_entries(
    value: object,
    *,
    field_name: str,
    require_provider: bool = False,
) -> list[dict[str, object]]:
    if value is None:
        return []
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        if len(value) == 0:
            return []
        raise ToolPlaneRevisionError(
            "validation_failed",
            safe_details={"field": field_name, "reason": "mapping_required"},
        )
    mapping = _require_mapping(value, field_name=field_name)
    if len(mapping) > _MAX_SKILLS:
        raise ToolPlaneRevisionError(
            "validation_failed",
            safe_details={"field": field_name, "reason": "too_many_items"},
        )
    result: list[dict[str, object]] = []
    for name in sorted(mapping):
        skill_name = _identifier(name, field_name=f"{field_name}.name")
        entry = _require_mapping(mapping[name], field_name=f"{field_name}.{skill_name}")
        allowed = {
            "enabled",
            "version",
            "archive_digest",
            "tree_digest",
            "manifest_digest",
            "entry_points",
        }
        if require_provider:
            allowed.add("provider")
        if unknown := set(entry) - allowed:
            raise ToolPlaneRevisionError(
                "validation_failed",
                safe_details={
                    "field": f"{field_name}.{skill_name}",
                    "reason": "unknown_fields",
                    "fields": sorted(str(item) for item in unknown)[:16],
                },
            )
        item: dict[str, object] = {
            "name": skill_name,
            "enabled": _bool(
                entry.get("enabled"),
                field_name=f"{field_name}.{skill_name}.enabled",
            ),
            "tree_digest": _digest(
                entry.get("tree_digest"),
                field_name=f"{field_name}.{skill_name}.tree_digest",
            ),
            "manifest_digest": _digest(
                entry.get("manifest_digest"),
                field_name=f"{field_name}.{skill_name}.manifest_digest",
            ),
            "entry_points": _canonical_string_list(
                entry.get("entry_points"),
                field_name=f"{field_name}.{skill_name}.entry_points",
                maximum=32,
            ),
        }
        if require_provider:
            item["provider"] = _identifier(
                entry.get("provider"),
                field_name=f"{field_name}.{skill_name}.provider",
            )
        version = entry.get("version")
        if version is not None:
            if not is_semantic_version(version):
                raise ToolPlaneRevisionError(
                    "validation_failed",
                    safe_details={
                        "field": f"{field_name}.{skill_name}.version",
                        "reason": "invalid_semantic_version",
                    },
                )
            item["version"] = version
        archive_digest = entry.get("archive_digest")
        if archive_digest is not None:
            item["archive_digest"] = _digest(
                archive_digest,
                field_name=f"{field_name}.{skill_name}.archive_digest",
            )
        result.append(item)
    return result


def _canonical_mcp_servers(value: object) -> list[dict[str, object]]:
    if value is None:
        return []
    mapping = _require_mapping(value, field_name="mcp_servers")
    if len(mapping) > _MAX_SERVERS:
        raise ToolPlaneRevisionError(
            "validation_failed",
            safe_details={"field": "mcp_servers", "reason": "too_many_items"},
        )
    result: list[dict[str, object]] = []
    for raw_name in sorted(mapping):
        name = _identifier(raw_name, field_name="mcp_servers.name")
        server = _require_mapping(mapping[raw_name], field_name=f"mcp_servers.{name}")
        allowed_server_fields = {
            "enabled",
            "type",
            "transport",
            "command",
            "args",
            "env",
            "url",
            "headers",
            "oauth",
            "user_auth",
            "credential_binding_id",
            "credential_version",
            "headers_from_context",
            "description",
            "routing",
            "tools",
            "tool_name_prefix",
            "tool_call_timeout",
            "session_init_timeout",
            "task_toolsets",
        }
        if unknown := set(server) - allowed_server_fields:
            field = str(sorted(unknown)[0])
            if _SECRET_FIELD.search(field):
                raise _secret_value_error(f"mcp_servers.{name}.{field}")
            raise ToolPlaneRevisionError(
                "validation_failed",
                safe_details={
                    "field": f"mcp_servers.{name}",
                    "reason": "unknown_fields",
                    "fields": sorted(str(item) for item in unknown)[:16],
                },
            )
        if "type" in server and "transport" in server:
            left = str(server["type"]).replace("streamable-http", "streamable_http")
            right = str(server["transport"]).replace("streamable-http", "streamable_http")
            if left != right:
                raise ToolPlaneRevisionError(
                    "validation_failed",
                    safe_details={
                        "field": f"mcp_servers.{name}.transport",
                        "reason": "conflicting_aliases",
                    },
                )
        transport = server.get("type", server.get("transport", "stdio"))
        if transport == "streamable-http":
            transport = "streamable_http"
        if not isinstance(transport, str) or transport not in _TRANSPORTS:
            raise ToolPlaneRevisionError(
                "validation_failed",
                safe_details={
                    "field": f"mcp_servers.{name}.transport",
                    "reason": "transport_not_allowed",
                },
            )
        canonical: dict[str, object] = {
            "server_id": name,
            "enabled": _bool(
                server.get("enabled"),
                field_name=f"mcp_servers.{name}.enabled",
            ),
            "transport": transport,
        }
        command = server.get("command")
        if command is not None:
            if not isinstance(command, str) or not command or len(command) > 512:
                raise ToolPlaneRevisionError(
                    "validation_failed",
                    safe_details={
                        "field": f"mcp_servers.{name}.command",
                        "reason": "invalid_value",
                    },
                )
            canonical["command"] = command
        arguments = _canonical_ordered_string_list(
            server.get("args"),
            field_name=f"mcp_servers.{name}.args",
            maximum=128,
        )
        if any(_SECRET_FIELD.search(argument) for argument in arguments):
            raise _secret_value_error(f"mcp_servers.{name}.args")
        canonical["args"] = arguments
        url = server.get("url")
        if url is not None:
            canonical["url"] = _safe_url(
                url,
                field_name=f"mcp_servers.{name}.url",
            )

        tools_value = server.get("tools", {})
        tools = _require_mapping(tools_value, field_name=f"mcp_servers.{name}.tools")
        if len(tools) > _MAX_TOOLS:
            raise ToolPlaneRevisionError(
                "validation_failed",
                safe_details={
                    "field": f"mcp_servers.{name}.tools",
                    "reason": "too_many_items",
                },
            )
        tool_allowlist = sorted(_identifier(item, field_name=f"mcp_servers.{name}.tools") for item in tools)
        canonical["tool_allowlist"] = tool_allowlist
        tool_overrides: dict[str, dict[str, object]] = {}
        for tool_name in tool_allowlist:
            override = _require_mapping(
                tools[tool_name],
                field_name=f"mcp_servers.{name}.tools.{tool_name}",
            )
            if set(override) - {"routing"}:
                raise ToolPlaneRevisionError(
                    "validation_failed",
                    safe_details={
                        "field": f"mcp_servers.{name}.tools.{tool_name}",
                        "reason": "unknown_fields",
                    },
                )
            tool_overrides[tool_name] = {
                "routing": _canonical_routing(
                    override.get("routing"),
                    field_name=f"mcp_servers.{name}.tools.{tool_name}.routing",
                )
            }
        canonical["tool_overrides"] = tool_overrides

        selectors: list[dict[str, str]] = []
        for container_name in ("headers", "env"):
            container = _require_mapping(
                server.get(container_name, {}),
                field_name=f"mcp_servers.{name}.{container_name}",
            )
            seen_fields: set[str] = set()
            for key in sorted(container, key=lambda item: str(item).casefold()):
                safe_key = str(key).casefold() if container_name == "headers" else str(key)
                canonical_field = f"{container_name}.{safe_key}"
                if canonical_field in seen_fields:
                    raise ToolPlaneRevisionError(
                        "validation_failed",
                        safe_details={
                            "field": f"mcp_servers.{name}.{container_name}",
                            "reason": "duplicate_identifier",
                        },
                    )
                seen_fields.add(canonical_field)
                selectors.append(
                    {
                        "field": canonical_field,
                        "selector": _selector(
                            container[key],
                            field_name=f"mcp_servers.{name}.{container_name}.{safe_key}",
                        ),
                    }
                )

        oauth = server.get("oauth")
        if oauth is not None:
            oauth_mapping = _require_mapping(oauth, field_name=f"mcp_servers.{name}.oauth")
            allowed_oauth = {
                "enabled",
                "token_url",
                "grant_type",
                "client_id",
                "client_secret",
                "refresh_token",
                "access_token",
                "scope",
                "audience",
                "token_field",
                "token_type_field",
                "expires_in_field",
                "default_token_type",
                "refresh_skew_seconds",
            }
            if unknown := set(oauth_mapping) - allowed_oauth:
                first = str(sorted(unknown)[0])
                if _SECRET_FIELD.search(first) or first == "extra_token_params":
                    raise _secret_value_error(f"mcp_servers.{name}.oauth.{first}")
                raise ToolPlaneRevisionError(
                    "validation_failed",
                    safe_details={
                        "field": f"mcp_servers.{name}.oauth",
                        "reason": "unknown_fields",
                    },
                )
            safe_oauth: dict[str, object] = {}
            for key in (
                "enabled",
                "token_url",
                "grant_type",
                "client_id",
                "scope",
                "audience",
                "token_field",
                "token_type_field",
                "expires_in_field",
                "default_token_type",
                "refresh_skew_seconds",
            ):
                if key in oauth_mapping:
                    safe_oauth[key] = oauth_mapping[key]
            if "token_url" in safe_oauth:
                safe_oauth["token_url"] = _safe_url(
                    safe_oauth["token_url"],
                    field_name=f"mcp_servers.{name}.oauth.token_url",
                )
            canonical["oauth_structure"] = safe_oauth
            for key in ("client_secret", "refresh_token", "access_token"):
                if key in oauth_mapping and oauth_mapping[key] is not None:
                    selectors.append(
                        {
                            "field": f"oauth.{key}",
                            "selector": _selector(
                                oauth_mapping[key],
                                field_name=f"mcp_servers.{name}.oauth.{key}",
                            ),
                        }
                    )

        user_auth = server.get("user_auth")
        if user_auth is not None:
            # User identities and bindings belong in the verified user's
            # overlay, never in a deployment-wide revision.
            raise _secret_value_error(f"mcp_servers.{name}.user_auth")

        context_headers = server.get("headers_from_context")
        if context_headers is not None:
            context_mapping = _require_mapping(
                context_headers,
                field_name=f"mcp_servers.{name}.headers_from_context",
            )
            header_mapping = _require_mapping(
                context_mapping.get("headers", {}),
                field_name=f"mcp_servers.{name}.headers_from_context.headers",
            )
            context_selectors: list[dict[str, str]] = []
            seen_headers: set[str] = set()
            for header, selector in sorted(
                header_mapping.items(),
                key=lambda item: str(item[0]).casefold(),
            ):
                canonical_header = str(header).casefold()
                if canonical_header in seen_headers:
                    raise ToolPlaneRevisionError(
                        "validation_failed",
                        safe_details={
                            "field": f"mcp_servers.{name}.headers_from_context.headers",
                            "reason": "duplicate_identifier",
                        },
                    )
                seen_headers.add(canonical_header)
                context_selectors.append(
                    {
                        "header": canonical_header,
                        "selector": f"context:{_identifier(selector, field_name='context_selector')}",
                    }
                )
            canonical["context_secret_selectors"] = context_selectors
            context_unknown = set(context_mapping) - {"enabled", "headers", "on_missing"}
            if context_unknown:
                raise ToolPlaneRevisionError(
                    "validation_failed",
                    safe_details={
                        "field": f"mcp_servers.{name}.headers_from_context",
                        "reason": "unknown_fields",
                    },
                )
            canonical["context_headers_structure"] = {
                "enabled": _bool(
                    context_mapping.get("enabled"),
                    field_name=f"mcp_servers.{name}.headers_from_context.enabled",
                ),
                "on_missing": context_mapping.get("on_missing", "deny"),
            }
            if canonical["context_headers_structure"]["on_missing"] not in {
                "deny",
                "passthrough",
            }:
                raise ToolPlaneRevisionError(
                    "validation_failed",
                    safe_details={
                        "field": f"mcp_servers.{name}.headers_from_context.on_missing",
                        "reason": "invalid_value",
                    },
                )
        description = server.get("description", "")
        if not isinstance(description, str) or len(description.encode("utf-8")) > 4096:
            raise ToolPlaneRevisionError(
                "validation_failed",
                safe_details={
                    "field": f"mcp_servers.{name}.description",
                    "reason": "invalid_value",
                },
            )
        canonical["description"] = description
        canonical["routing"] = _canonical_routing(
            server.get("routing"),
            field_name=f"mcp_servers.{name}.routing",
        )
        canonical["tool_name_prefix"] = _bool(
            server.get("tool_name_prefix"),
            field_name=f"mcp_servers.{name}.tool_name_prefix",
        )
        canonical["tool_call_timeout"] = _timeout(
            server.get("tool_call_timeout"),
            field_name=f"mcp_servers.{name}.tool_call_timeout",
        )
        canonical["session_init_timeout"] = _timeout(
            server.get("session_init_timeout"),
            field_name=f"mcp_servers.{name}.session_init_timeout",
        )
        canonical["task_toolsets"] = _canonical_task_toolsets(
            server.get("task_toolsets"),
            field_name=f"mcp_servers.{name}.task_toolsets",
        )
        binding_id = server.get("credential_binding_id")
        binding_version = server.get("credential_version", 1)
        if binding_id is not None:
            if not isinstance(binding_id, str) or _BINDING_REFERENCE.fullmatch(binding_id) is None or type(binding_version) is not int or binding_version < 1:
                raise ToolPlaneRevisionError(
                    "validation_failed",
                    safe_details={
                        "field": f"mcp_servers.{name}.credential_binding_id",
                        "reason": "invalid_reference",
                    },
                )
            canonical["credential_binding"] = {
                "binding_ref": binding_id,
                "version": binding_version,
            }
        elif "credential_version" in server:
            raise ToolPlaneRevisionError(
                "validation_failed",
                safe_details={
                    "field": f"mcp_servers.{name}.credential_version",
                    "reason": "binding_required",
                },
            )
        canonical["secret_selectors"] = sorted(
            selectors,
            key=lambda item: (item["field"], item["selector"]),
        )
        result.append(canonical)
    return result


def runtime_mcp_servers_from_canonical(
    value: object,
) -> dict[str, dict[str, object]]:
    """Rebuild unresolved runtime MCP config from secret-safe canonical entries.

    The returned values contain selector placeholders (for example
    ``$SEARCH_TOKEN``), never resolved environment values.  Re-canonicalizing
    this projection is therefore also the strict validator for effective MCP
    evidence used by accepted invocations.
    """

    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise ToolPlaneRevisionError(
            "validation_failed",
            safe_details={"field": "effective_mcp_servers", "reason": "list_required"},
        )
    if len(value) > _MAX_SERVERS:
        raise ToolPlaneRevisionError(
            "validation_failed",
            safe_details={"field": "effective_mcp_servers", "reason": "too_many_items"},
        )
    result: dict[str, dict[str, object]] = {}
    for index, raw in enumerate(value):
        server = _require_mapping(
            raw,
            field_name=f"effective_mcp_servers.{index}",
        )
        server_id = _identifier(
            server.get("server_id"),
            field_name=f"effective_mcp_servers.{index}.server_id",
        )
        if server_id in result:
            raise ToolPlaneRevisionError(
                "validation_failed",
                safe_details={
                    "field": "effective_mcp_servers",
                    "reason": "duplicate_identifier",
                },
            )
        transport = server.get("transport")
        config: dict[str, object] = {
            "enabled": server.get("enabled", True),
            "type": ("streamable-http" if transport == "streamable_http" else transport),
            "args": list(server.get("args", [])),
        }
        for field_name in ("command", "url"):
            if server.get(field_name) is not None:
                config[field_name] = server[field_name]
        tools = server.get("tool_overrides", {})
        if isinstance(tools, Mapping) and tools:
            config["tools"] = json.loads(canonical_json_bytes(tools))
        for field_name in (
            "description",
            "routing",
            "tool_name_prefix",
            "tool_call_timeout",
            "session_init_timeout",
            "task_toolsets",
        ):
            if field_name in server:
                config[field_name] = json.loads(canonical_json_bytes(server[field_name]))
        credential_binding = server.get("credential_binding")
        if isinstance(credential_binding, Mapping):
            config["credential_binding_id"] = credential_binding.get("binding_ref")
            config["credential_version"] = credential_binding.get("version")
        headers: dict[str, str] = {}
        environment: dict[str, str] = {}
        oauth_raw = server.get("oauth_structure", {})
        oauth: dict[str, object] = json.loads(canonical_json_bytes(oauth_raw)) if isinstance(oauth_raw, Mapping) else {}
        selectors = server.get("secret_selectors", [])
        if not isinstance(selectors, list):
            raise ToolPlaneRevisionError(
                "validation_failed",
                safe_details={
                    "field": f"effective_mcp_servers.{index}.secret_selectors",
                    "reason": "list_required",
                },
            )
        for selector in selectors:
            if not isinstance(selector, Mapping):
                raise ToolPlaneRevisionError(
                    "validation_failed",
                    safe_details={
                        "field": f"effective_mcp_servers.{index}.secret_selectors",
                        "reason": "mapping_required",
                    },
                )
            field_name = selector.get("field")
            selector_name = selector.get("selector")
            if not isinstance(selector_name, str) or not selector_name.startswith("env:"):
                raise _secret_value_error(f"effective_mcp_servers.{index}.secret_selectors")
            unresolved = f"${selector_name.removeprefix('env:')}"
            if isinstance(field_name, str) and field_name.startswith("headers."):
                headers[field_name.removeprefix("headers.")] = unresolved
            elif isinstance(field_name, str) and field_name.startswith("env."):
                environment[field_name.removeprefix("env.")] = unresolved
            elif isinstance(field_name, str) and field_name.startswith("oauth."):
                oauth[field_name.removeprefix("oauth.")] = unresolved
            else:
                raise ToolPlaneRevisionError(
                    "validation_failed",
                    safe_details={
                        "field": f"effective_mcp_servers.{index}.secret_selectors",
                        "reason": "invalid_field",
                    },
                )
        if headers:
            config["headers"] = headers
        if environment:
            config["env"] = environment
        if oauth:
            config["oauth"] = oauth
        context_selectors = server.get("context_secret_selectors", [])
        if context_selectors:
            if not isinstance(context_selectors, list):
                raise ToolPlaneRevisionError(
                    "validation_failed",
                    safe_details={
                        "field": (f"effective_mcp_servers.{index}.context_secret_selectors"),
                        "reason": "list_required",
                    },
                )
            context_structure = server.get("context_headers_structure", {})
            if not isinstance(context_structure, Mapping):
                raise ToolPlaneRevisionError(
                    "validation_failed",
                    safe_details={
                        "field": (f"effective_mcp_servers.{index}.context_headers_structure"),
                        "reason": "mapping_required",
                    },
                )
            context_headers: dict[str, str] = {}
            for item in context_selectors:
                if not isinstance(item, Mapping) or not isinstance(item.get("header"), str) or not isinstance(item.get("selector"), str) or not str(item["selector"]).startswith("context:"):
                    raise ToolPlaneRevisionError(
                        "validation_failed",
                        safe_details={
                            "field": (f"effective_mcp_servers.{index}.context_secret_selectors"),
                            "reason": "invalid_selector",
                        },
                    )
                context_headers[str(item["header"])] = str(item["selector"]).removeprefix("context:")
            config["headers_from_context"] = {
                "enabled": context_structure.get("enabled", True),
                "headers": context_headers,
                "on_missing": context_structure.get("on_missing", "deny"),
            }
        result[server_id] = config
    return result


def canonicalize_effective_mcp_servers(
    value: object,
) -> tuple[Mapping[str, object], ...]:
    """Validate that effective MCP entries are exact canonical safe material."""

    runtime = runtime_mcp_servers_from_canonical(value)
    rebuilt = _canonical_mcp_servers(runtime)
    supplied = json.loads(canonical_json_bytes(value))
    if supplied != rebuilt:
        raise ToolPlaneRevisionError(
            "validation_failed",
            safe_details={
                "field": "effective_mcp_servers",
                "reason": "noncanonical_material",
            },
        )
    return tuple(MappingProxyType(item) for item in rebuilt)


def _canonical_effective_global_skill_states(
    value: object,
) -> tuple[Mapping[str, object], ...]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise ToolPlaneRevisionError(
            "validation_failed",
            safe_details={
                "field": "effective_global_skill_states",
                "reason": "list_required",
            },
        )
    if len(value) > _MAX_SKILLS:
        raise ToolPlaneRevisionError(
            "validation_failed",
            safe_details={
                "field": "effective_global_skill_states",
                "reason": "too_many_items",
            },
        )
    states: list[dict[str, object]] = []
    seen: set[str] = set()
    for index, raw in enumerate(value):
        item = _require_mapping(
            raw,
            field_name=f"effective_global_skill_states.{index}",
        )
        if set(item) != {"name", "enabled"}:
            raise ToolPlaneRevisionError(
                "validation_failed",
                safe_details={
                    "field": f"effective_global_skill_states.{index}",
                    "reason": "invalid_fields",
                },
            )
        name = _identifier(
            item.get("name"),
            field_name=f"effective_global_skill_states.{index}.name",
        )
        if name in seen:
            raise ToolPlaneRevisionError(
                "validation_failed",
                safe_details={
                    "field": "effective_global_skill_states",
                    "reason": "duplicate_identifier",
                },
            )
        seen.add(name)
        states.append(
            {
                "name": name,
                "enabled": _bool(
                    item.get("enabled"),
                    field_name=f"effective_global_skill_states.{index}.enabled",
                ),
            }
        )
    if states != sorted(states, key=lambda item: str(item["name"])):
        raise ToolPlaneRevisionError(
            "validation_failed",
            safe_details={
                "field": "effective_global_skill_states",
                "reason": "noncanonical_material",
            },
        )
    return tuple(MappingProxyType(item) for item in states)


def _canonical_bool_map(value: object, *, field_name: str) -> list[dict[str, object]]:
    if value is None:
        return []
    mapping = _require_mapping(value, field_name=field_name)
    if len(mapping) > _MAX_SKILLS:
        raise ToolPlaneRevisionError(
            "validation_failed",
            safe_details={"field": field_name, "reason": "too_many_items"},
        )
    return [
        {
            "id": _identifier(key, field_name=field_name),
            "enabled": _bool(mapping[key], field_name=f"{field_name}.{key}"),
        }
        for key in sorted(mapping)
    ]


@dataclass(frozen=True, slots=True)
class ToolPlaneRevisionScopeV1:
    """Tenant-local deployment-base or opaque user-overlay scope."""

    kind: ToolPlaneScopeKind
    user_ref: str | None = None

    def __post_init__(self) -> None:
        if self.kind == "deployment_base":
            if self.user_ref is not None:
                raise ValueError("deployment_base scope must not contain user_ref")
        elif self.kind == "user_overlay":
            if not isinstance(self.user_ref, str) or _BINDING_REFERENCE.fullmatch(self.user_ref) is None:
                raise ValueError("user_overlay scope requires a bounded user_ref")
        else:
            raise ValueError("unsupported tool-plane scope kind")

    def to_json(self) -> dict[str, object]:
        """Return the canonical versioned scope projection."""

        return {"version": 1, "kind": self.kind, "user_ref": self.user_ref}

    @property
    def key(self) -> str:
        """Return the repository key for this scope."""

        return self.kind if self.user_ref is None else f"{self.kind}:{self.user_ref}"


@dataclass(frozen=True, slots=True)
class DeploymentToolPlaneRevisionV1:
    """Canonical immutable deployment-wide MCP and skill material."""

    mcp_servers: tuple[Mapping[str, object], ...] = ()
    public_skills: tuple[Mapping[str, object], ...] = ()
    managed_integrations: tuple[Mapping[str, object], ...] = ()
    validation_policy_digest: str = ""
    parent_revision_digest: str | None = None
    change_summary: str | None = None

    def to_json(self) -> dict[str, object]:
        """Return the canonical versioned deployment manifest."""

        return {
            "version": 1,
            "kind": "deployment_base",
            "canonicalization_version": 1,
            "mcp_servers": [dict(item) for item in self.mcp_servers],
            "public_skills": [dict(item) for item in self.public_skills],
            "managed_integrations": [dict(item) for item in self.managed_integrations],
            "validation_policy_digest": self.validation_policy_digest,
            "parent_revision_digest": self.parent_revision_digest,
            "change_summary": self.change_summary,
        }

    @property
    def digest(self) -> str:
        """Return this canonical deployment manifest's identity."""

        return canonical_tool_plane_digest(self.to_json())


@dataclass(frozen=True, slots=True)
class UserToolPlaneOverlayV1:
    """Canonical immutable per-user enablement and custom-skill material."""

    base_revision_digest: str
    custom_skills: tuple[Mapping[str, object], ...] = ()
    mcp_enablement: tuple[Mapping[str, object], ...] = ()
    managed_integration_enablement: tuple[Mapping[str, object], ...] = ()
    credential_selectors: tuple[Mapping[str, object], ...] = ()
    skill_states: tuple[Mapping[str, object], ...] = ()
    parent_revision_digest: str | None = None
    change_summary: str | None = None

    def to_json(self) -> dict[str, object]:
        """Return the canonical versioned user-overlay manifest."""

        return {
            "version": 1,
            "kind": "user_overlay",
            "canonicalization_version": 1,
            "base_revision_digest": self.base_revision_digest,
            "custom_skills": [dict(item) for item in self.custom_skills],
            "mcp_enablement": [dict(item) for item in self.mcp_enablement],
            "managed_integration_enablement": [dict(item) for item in self.managed_integration_enablement],
            "credential_selectors": [dict(item) for item in self.credential_selectors],
            "skill_states": [dict(item) for item in self.skill_states],
            "parent_revision_digest": self.parent_revision_digest,
            "change_summary": self.change_summary,
        }

    @property
    def digest(self) -> str:
        """Return this canonical overlay manifest's identity."""

        return canonical_tool_plane_digest(self.to_json())

    @property
    def is_empty(self) -> bool:
        """Return whether the overlay contributes no user-specific material."""

        return not any(
            (
                self.custom_skills,
                self.mcp_enablement,
                self.managed_integration_enablement,
                self.credential_selectors,
                self.skill_states,
            )
        )


def canonicalize_deployment_candidate(
    value: Mapping[str, object],
) -> DeploymentToolPlaneRevisionV1:
    """Validate and canonicalize a deployment-base candidate."""

    mapping = _require_mapping(value, field_name="candidate")
    allowed = {
        "version",
        "mcp_servers",
        "public_skills",
        "managed_integrations",
        "validation_policy_digest",
        "parent_revision_digest",
        "change_summary",
    }
    if unknown := set(mapping) - allowed:
        raise ToolPlaneRevisionError(
            "validation_failed",
            safe_details={"field": "candidate", "reason": "unknown_fields", "fields": sorted(unknown)},
        )
    if mapping.get("version", 1) != 1:
        raise ToolPlaneRevisionError(
            "validation_failed",
            safe_details={"field": "version", "reason": "unsupported_version"},
        )
    integrations = _canonical_skill_entries(
        mapping.get("managed_integrations"),
        field_name="managed_integrations",
        require_provider=True,
    )
    return DeploymentToolPlaneRevisionV1(
        mcp_servers=tuple(MappingProxyType(item) for item in _canonical_mcp_servers(mapping.get("mcp_servers"))),
        public_skills=tuple(
            MappingProxyType(item)
            for item in _canonical_skill_entries(
                mapping.get("public_skills"),
                field_name="public_skills",
            )
        ),
        managed_integrations=tuple(MappingProxyType(item) for item in integrations),
        validation_policy_digest=_digest(
            mapping.get("validation_policy_digest"),
            field_name="validation_policy_digest",
        ),
        parent_revision_digest=_optional_digest(
            mapping.get("parent_revision_digest"),
            field_name="parent_revision_digest",
        ),
        change_summary=_bounded_summary(mapping.get("change_summary")),
    )


def canonicalize_user_overlay_candidate(
    value: Mapping[str, object],
) -> UserToolPlaneOverlayV1:
    """Validate and canonicalize a user-overlay candidate."""

    mapping = _require_mapping(value, field_name="candidate")
    allowed = {
        "version",
        "base_revision_digest",
        "custom_skills",
        "mcp_enablement",
        "managed_integration_enablement",
        "credential_selectors",
        "skill_states",
        "parent_revision_digest",
        "change_summary",
    }
    if unknown := set(mapping) - allowed:
        raise ToolPlaneRevisionError(
            "validation_failed",
            safe_details={"field": "candidate", "reason": "unknown_fields", "fields": sorted(unknown)},
        )
    if mapping.get("version", 1) != 1:
        raise ToolPlaneRevisionError(
            "validation_failed",
            safe_details={"field": "version", "reason": "unsupported_version"},
        )
    selector_mapping = _require_mapping(
        mapping.get("credential_selectors", {}),
        field_name="credential_selectors",
    )
    selectors: list[dict[str, object]] = []
    for server_id in sorted(selector_mapping):
        item = _require_mapping(
            selector_mapping[server_id],
            field_name=f"credential_selectors.{server_id}",
        )
        if set(item) != {"binding_ref", "version"}:
            raise ToolPlaneRevisionError(
                "validation_failed",
                safe_details={
                    "field": f"credential_selectors.{server_id}",
                    "reason": "invalid_fields",
                },
            )
        binding_ref = item["binding_ref"]
        version = item["version"]
        if not isinstance(binding_ref, str) or _BINDING_REFERENCE.fullmatch(binding_ref) is None:
            raise ToolPlaneRevisionError(
                "validation_failed",
                safe_details={
                    "field": f"credential_selectors.{server_id}.binding_ref",
                    "reason": "invalid_reference",
                },
            )
        if type(version) is not int or version < 1 or version > 2**31 - 1:
            raise ToolPlaneRevisionError(
                "validation_failed",
                safe_details={
                    "field": f"credential_selectors.{server_id}.version",
                    "reason": "invalid_version",
                },
            )
        selectors.append(
            {
                "server_id": _identifier(server_id, field_name="credential_selectors.server_id"),
                "binding_ref": binding_ref,
                "version": version,
            }
        )

    raw_states = _require_mapping(mapping.get("skill_states", {}), field_name="skill_states")
    states: list[dict[str, object]] = []
    for skill_name in sorted(raw_states):
        state = _require_mapping(raw_states[skill_name], field_name=f"skill_states.{skill_name}")
        if set(state) != {"enabled"}:
            raise ToolPlaneRevisionError(
                "validation_failed",
                safe_details={"field": f"skill_states.{skill_name}", "reason": "invalid_fields"},
            )
        states.append(
            {
                "name": _identifier(skill_name, field_name="skill_states.name"),
                "enabled": _bool(state["enabled"], field_name=f"skill_states.{skill_name}.enabled"),
            }
        )

    return UserToolPlaneOverlayV1(
        base_revision_digest=_digest(
            mapping.get("base_revision_digest"),
            field_name="base_revision_digest",
        ),
        custom_skills=tuple(
            MappingProxyType(item)
            for item in _canonical_skill_entries(
                mapping.get("custom_skills"),
                field_name="custom_skills",
            )
        ),
        mcp_enablement=tuple(
            MappingProxyType(item)
            for item in _canonical_bool_map(
                mapping.get("mcp_enablement"),
                field_name="mcp_enablement",
            )
        ),
        managed_integration_enablement=tuple(
            MappingProxyType(item)
            for item in _canonical_bool_map(
                mapping.get("managed_integration_enablement"),
                field_name="managed_integration_enablement",
            )
        ),
        credential_selectors=tuple(MappingProxyType(item) for item in selectors),
        skill_states=tuple(MappingProxyType(item) for item in states),
        parent_revision_digest=_optional_digest(
            mapping.get("parent_revision_digest"),
            field_name="parent_revision_digest",
        ),
        change_summary=_bounded_summary(mapping.get("change_summary")),
    )


EMPTY_OVERLAY_MARKER_V1 = canonical_tool_plane_digest({"version": 1, "kind": "empty_user_overlay"})


@dataclass(frozen=True, slots=True)
class EffectiveToolPlaneRevisionV1:
    """Canonical admitted composition of one base and one user overlay."""

    base_revision_digest: str
    user_overlay_digest: str
    base_generation: int
    overlay_generation: int
    projection_digest: str
    effective_mcp_server_ids: tuple[str, ...] = ()
    effective_mcp_servers: tuple[Mapping[str, object], ...] = ()
    effective_global_skill_states: tuple[Mapping[str, object], ...] = ()
    effective_managed_integration_ids: tuple[str, ...] = ()
    governance_state: Literal["governed", "unmanaged"] = "governed"
    effective_digest: str = field(init=False)

    def __post_init__(self) -> None:
        for name in ("base_revision_digest", "user_overlay_digest", "projection_digest"):
            _digest(getattr(self, name), field_name=name)
        if type(self.base_generation) is not int or self.base_generation < 0:
            raise ValueError("base_generation must be a non-negative integer")
        if type(self.overlay_generation) is not int or self.overlay_generation < 0:
            raise ValueError("overlay_generation must be a non-negative integer")
        if self.governance_state not in {"governed", "unmanaged"}:
            raise ValueError("governance_state is invalid")
        for field_name in (
            "effective_mcp_server_ids",
            "effective_managed_integration_ids",
        ):
            values = tuple(getattr(self, field_name))
            normalized = tuple(sorted({_identifier(value, field_name=field_name) for value in values}))
            object.__setattr__(self, field_name, normalized)
        effective_servers = canonicalize_effective_mcp_servers(self.effective_mcp_servers)
        object.__setattr__(self, "effective_mcp_servers", effective_servers)
        effective_server_ids = tuple(str(server["server_id"]) for server in effective_servers if server.get("enabled") is True)
        if effective_server_ids != self.effective_mcp_server_ids:
            raise ValueError("effective_mcp_server_ids must match effective_mcp_servers")
        global_skill_states = _canonical_effective_global_skill_states(self.effective_global_skill_states)
        object.__setattr__(
            self,
            "effective_global_skill_states",
            global_skill_states,
        )
        projection = {
            "version": 1,
            "base_revision_digest": self.base_revision_digest,
            "user_overlay_digest": self.user_overlay_digest,
            "base_generation": self.base_generation,
            "overlay_generation": self.overlay_generation,
            "projection_digest": self.projection_digest,
            "effective_mcp_server_ids": list(self.effective_mcp_server_ids),
            "effective_mcp_servers": [dict(server) for server in self.effective_mcp_servers],
            "effective_global_skill_states": [dict(state) for state in self.effective_global_skill_states],
            "effective_managed_integration_ids": list(self.effective_managed_integration_ids),
            "governance_state": self.governance_state,
        }
        object.__setattr__(self, "effective_digest", canonical_tool_plane_digest(projection))

    def to_json(self) -> dict[str, object]:
        """Return the complete secret-safe accepted effective projection."""

        return {
            "version": 1,
            "base_revision_digest": self.base_revision_digest,
            "user_overlay_digest": self.user_overlay_digest,
            "base_generation": self.base_generation,
            "overlay_generation": self.overlay_generation,
            "projection_digest": self.projection_digest,
            "effective_mcp_server_ids": list(self.effective_mcp_server_ids),
            "effective_mcp_servers": [json.loads(canonical_json_bytes(server)) for server in self.effective_mcp_servers],
            "effective_global_skill_states": [dict(state) for state in self.effective_global_skill_states],
            "effective_managed_integration_ids": list(self.effective_managed_integration_ids),
            "governance_state": self.governance_state,
            "effective_digest": self.effective_digest,
        }


__all__ = [
    "DeploymentToolPlaneRevisionV1",
    "EMPTY_OVERLAY_MARKER_V1",
    "EffectiveToolPlaneRevisionV1",
    "ToolPlaneRevisionError",
    "ToolPlaneRevisionScopeV1",
    "UserToolPlaneOverlayV1",
    "canonical_json_bytes",
    "canonical_tool_plane_digest",
    "canonicalize_deployment_candidate",
    "canonicalize_effective_mcp_servers",
    "canonicalize_user_overlay_candidate",
    "is_semantic_version",
    "runtime_mcp_servers_from_canonical",
]
