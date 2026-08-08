"""Host-independent Hartmesh identifier domains.

These validators define execution and policy identities, not display labels.
They never truncate or hash caller values. Agent identifiers have one explicit
lowercase canonical form because the existing agent store is case-insensitive;
all other domains preserve exact case and bytes.
"""

from __future__ import annotations

import re

AGENT_IDENTIFIER_PATTERN = r"[A-Za-z0-9][A-Za-z0-9-]{0,127}"
_AGENT_IDENTIFIER_RE = re.compile(rf"{AGENT_IDENTIFIER_PATTERN}\Z", re.ASCII)
RESERVED_AGENT_IDENTIFIERS = frozenset({"lead_agent"})
THREAD_IDENTIFIER_PATTERN = r"[A-Za-z0-9_-]{1,64}"
_THREAD_IDENTIFIER_RE = re.compile(rf"{THREAD_IDENTIFIER_PATTERN}\Z", re.ASCII)
MODEL_PROFILE_IDENTIFIER_MAX_BYTES = 128
MCP_SERVER_IDENTIFIER_MAX_BYTES = 128
MCP_TOOL_IDENTIFIER_PATTERN = r"[A-Za-z0-9_-]{1,128}"
_MCP_TOOL_IDENTIFIER_RE = re.compile(rf"{MCP_TOOL_IDENTIFIER_PATTERN}\Z", re.ASCII)


def validate_agent_identifier(
    value: object,
    *,
    field_name: str = "agent identifier",
) -> str:
    """Return an exact ASCII agent identifier or raise ``ValueError``.

    Agent identifiers are 1-128 characters, begin with an ASCII letter or
    digit, and thereafter contain only ASCII letters, digits, or ``-``.
    """

    if not isinstance(value, str) or _AGENT_IDENTIFIER_RE.fullmatch(value) is None:
        raise ValueError(f"{field_name} must match {AGENT_IDENTIFIER_PATTERN!r} (1-128 case-sensitive ASCII letters, digits, or hyphens; the first character must be alphanumeric)")
    return value


def canonicalize_agent_identifier(
    value: object,
    *,
    field_name: str = "agent identifier",
) -> str:
    """Validate and return the documented lowercase agent identity.

    Agent identifiers are case-insensitive for backward compatibility with
    agent storage. The lowercase value is the identity returned by every host
    and portable boundary; display casing belongs in a separate label.
    """

    if isinstance(value, str) and value in RESERVED_AGENT_IDENTIFIERS:
        return value
    return validate_agent_identifier(value, field_name=field_name).lower()


def validate_thread_identifier(
    value: object,
    *,
    field_name: str = "thread identifier",
) -> str:
    """Return an exact canonical thread identifier or raise ``ValueError``.

    Thread identifiers are 1-64 case-sensitive ASCII letters, digits,
    underscores, or hyphens. Any of those characters may appear first.
    """

    if not isinstance(value, str) or _THREAD_IDENTIFIER_RE.fullmatch(value) is None:
        raise ValueError(f"{field_name} must match {THREAD_IDENTIFIER_PATTERN!r} (1-64 case-sensitive ASCII letters, digits, underscores, or hyphens)")
    return value


def validate_model_profile_identifier(
    value: object,
    *,
    field_name: str = "model profile identifier",
) -> str:
    """Return an exact bounded model/profile identifier or raise ``ValueError``.

    Profile identifiers are case-sensitive, non-empty UTF-8 strings of at most
    128 bytes. Unicode and ordinary spaces are permitted; ASCII control
    characters and DEL are not. The value is never normalized or truncated.
    """

    if not isinstance(value, str) or not value:
        raise ValueError(f"{field_name} must be a non-empty string limited to 128 UTF-8 bytes")
    if len(value.encode("utf-8")) > MODEL_PROFILE_IDENTIFIER_MAX_BYTES:
        raise ValueError(f"{field_name} must be limited to 128 UTF-8 bytes")
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        raise ValueError(f"{field_name} must not contain ASCII control characters")
    return value


def validate_mcp_server_identifier(
    value: object,
    *,
    field_name: str = "MCP server identifier",
) -> str:
    """Return an exact configured MCP server identity or raise ``ValueError``.

    Server identities are case-sensitive, non-empty UTF-8 strings of at most
    128 bytes without ASCII control characters. They are configuration keys,
    not function identifiers, so Unicode and spaces remain supported.
    """

    if not isinstance(value, str) or not value:
        raise ValueError(f"{field_name} must be a non-empty string limited to 128 UTF-8 bytes")
    if len(value.encode("utf-8")) > MCP_SERVER_IDENTIFIER_MAX_BYTES:
        raise ValueError(f"{field_name} must be limited to 128 UTF-8 bytes")
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        raise ValueError(f"{field_name} must not contain ASCII control characters")
    return value


def validate_mcp_tool_identifier(
    value: object,
    *,
    field_name: str = "MCP tool identifier",
) -> str:
    """Return an exact host-bindable MCP function identity or raise ``ValueError``."""

    if not isinstance(value, str) or _MCP_TOOL_IDENTIFIER_RE.fullmatch(value) is None:
        raise ValueError(f"{field_name} must match {MCP_TOOL_IDENTIFIER_PATTERN!r} (1-128 case-sensitive ASCII letters, digits, underscores, or hyphens)")
    return value


__all__ = [
    "AGENT_IDENTIFIER_PATTERN",
    "MODEL_PROFILE_IDENTIFIER_MAX_BYTES",
    "MCP_SERVER_IDENTIFIER_MAX_BYTES",
    "MCP_TOOL_IDENTIFIER_PATTERN",
    "RESERVED_AGENT_IDENTIFIERS",
    "THREAD_IDENTIFIER_PATTERN",
    "canonicalize_agent_identifier",
    "validate_agent_identifier",
    "validate_model_profile_identifier",
    "validate_mcp_server_identifier",
    "validate_mcp_tool_identifier",
    "validate_thread_identifier",
]
