"""Host-independent contracts for authoritative MCP call preparation."""

from __future__ import annotations

import json
import re
from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal, Protocol, runtime_checkable

from deerflow_extension_api.contributors import (
    PrincipalProjectionV1,
    ResolvedAgentRevisionReferenceV1,
    SafeContextReferenceV1,
    SealedOriginV1,
    TrustedRunContextV1,
)
from deerflow_extension_api.health import CapabilityHealthProbe

MCP_INTERCEPTOR_CAPABILITY_API_VERSION = "1.0"
MCP_INTERCEPTOR_KIND = "mcp_interceptor"

_DIGEST = re.compile(r"^[0-9a-f]{64}$", re.ASCII)
_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/@-]{0,127}$", re.ASCII)
_HEADER_NAME = re.compile(r"^[!#$%&'*+.^_`|~0-9A-Za-z-]{1,128}$", re.ASCII)
_MAX_HEADERS = 16
_MAX_HEADER_VALUE_BYTES = 1024
_MAX_EVIDENCE_REFERENCES = 32
_MAX_CANONICAL_RESULT_BYTES = 8192


def _validate_identifier(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or _IDENTIFIER.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be a 1-128 character ASCII identifier")
    return value


def _validate_digest(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be a 64-character lowercase SHA-256 digest")
    return value


@dataclass(frozen=True)
class McpCallProjectionV1:
    """One sealed, credential-free MCP operation projection."""

    principal: PrincipalProjectionV1
    origin: SealedOriginV1
    thread_id: str
    run_id: str
    agent_revision: ResolvedAgentRevisionReferenceV1
    extension_generation: int
    server_name: str
    tool_name: str
    arguments_digest: str
    trusted_context: TrustedRunContextV1 | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.principal, PrincipalProjectionV1):
            raise TypeError("principal must be PrincipalProjectionV1")
        if not isinstance(self.origin, SealedOriginV1):
            raise TypeError("origin must be SealedOriginV1")
        _validate_identifier(self.thread_id, field_name="thread_id")
        _validate_identifier(self.run_id, field_name="run_id")
        if not isinstance(self.agent_revision, ResolvedAgentRevisionReferenceV1):
            raise TypeError("agent_revision must be ResolvedAgentRevisionReferenceV1")
        if type(self.extension_generation) is not int or self.extension_generation < 0:
            raise ValueError("extension_generation must be a non-negative integer")
        _validate_identifier(self.server_name, field_name="server_name")
        _validate_identifier(self.tool_name, field_name="tool_name")
        _validate_digest(self.arguments_digest, field_name="arguments_digest")
        if self.trusted_context is not None:
            if not isinstance(self.trusted_context, TrustedRunContextV1):
                raise TypeError("trusted_context must be TrustedRunContextV1 or None")
            if (
                self.trusted_context.identity != self.principal.identity
                or self.trusted_context.origin != self.origin
                or self.trusted_context.thread_id != self.thread_id
                or self.trusted_context.run_id != self.run_id
                or self.trusted_context.agent_revision != self.agent_revision
                or self.trusted_context.extension_generation != self.extension_generation
            ):
                raise ValueError("MCP projection facts must match the trusted run context")


@dataclass(frozen=True)
class McpHeaderV1:
    """One transient header addition for the current MCP operation only."""

    name: str
    value: str

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or _HEADER_NAME.fullmatch(self.name) is None:
            raise ValueError("MCP header name must be a valid 1-128 character HTTP field name")
        if not isinstance(self.value, str):
            raise TypeError("MCP header value must be a string")
        if any(ord(character) < 32 or ord(character) == 127 for character in self.value):
            raise ValueError("MCP header value must not contain control characters")
        if len(self.value.encode("utf-8")) > _MAX_HEADER_VALUE_BYTES:
            raise ValueError("MCP header value is limited to 1 KiB UTF-8")


@dataclass(frozen=True)
class PreparedMcpCallV1:
    """Bounded transient additions and safe audit evidence for one call."""

    headers: tuple[McpHeaderV1, ...] = ()
    evidence_references: tuple[SafeContextReferenceV1, ...] = ()

    def __post_init__(self) -> None:
        headers = tuple(self.headers)
        evidence = tuple(self.evidence_references)
        object.__setattr__(self, "headers", headers)
        object.__setattr__(self, "evidence_references", evidence)
        if len(headers) > _MAX_HEADERS:
            raise ValueError("an MCP preparation may add at most 16 headers")
        if len(evidence) > _MAX_EVIDENCE_REFERENCES:
            raise ValueError("an MCP preparation may return at most 32 evidence references")
        seen_headers: set[str] = set()
        for header in headers:
            if not isinstance(header, McpHeaderV1):
                raise TypeError("headers must contain McpHeaderV1 values")
            folded = header.name.casefold()
            if folded in seen_headers:
                raise ValueError(f"duplicate MCP header {header.name!r}")
            seen_headers.add(folded)
        seen_evidence: set[str] = set()
        for reference in evidence:
            if not isinstance(reference, SafeContextReferenceV1):
                raise TypeError("evidence_references must contain SafeContextReferenceV1 values")
            if reference.key in seen_evidence:
                raise ValueError(f"duplicate MCP evidence reference {reference.key!r}")
            seen_evidence.add(reference.key)
        canonical = json.dumps(
            {
                "evidence_references": [
                    {
                        "key": reference.key,
                        "purpose": reference.purpose,
                        "storage_class": reference.storage_class,
                        "value": reference.value,
                    }
                    for reference in evidence
                ],
                "headers": [{"name": header.name, "value": header.value} for header in headers],
            },
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        if len(canonical) > _MAX_CANONICAL_RESULT_BYTES:
            raise ValueError("a canonical MCP preparation result is limited to 8 KiB")


@dataclass(frozen=True)
class McpCallRejectedV1:
    """Required credentials or evidence could not be prepared safely."""


@dataclass(frozen=True)
class McpCallIndeterminateV1:
    """The interceptor could not determine a safe prepared call."""


@runtime_checkable
class McpInterceptor(Protocol):
    async def prepare_call(
        self,
        request: McpCallProjectionV1,
    ) -> PreparedMcpCallV1 | McpCallRejectedV1 | McpCallIndeterminateV1:
        return McpCallIndeterminateV1()


@dataclass(frozen=True)
class McpInterceptorDescriptor:
    """One trusted operator-plugin MCP preparation contribution."""

    contribution_id: str
    capability_api_version: str
    factory: Callable[[], McpInterceptor]
    kind: Literal["mcp_interceptor"]
    health_probe: CapabilityHealthProbe | None = None

    def __post_init__(self) -> None:
        _validate_identifier(self.contribution_id, field_name="MCP interceptor contribution_id")
        if self.capability_api_version != MCP_INTERCEPTOR_CAPABILITY_API_VERSION:
            raise ValueError(f"unsupported MCP interceptor capability API version {self.capability_api_version!r}; expected {MCP_INTERCEPTOR_CAPABILITY_API_VERSION!r}")
        if self.kind != MCP_INTERCEPTOR_KIND:
            raise ValueError(f"MCP interceptor kind must be {MCP_INTERCEPTOR_KIND!r}")
        if not callable(self.factory):
            raise TypeError("MCP interceptor factory must be callable")
        if self.health_probe is not None and not callable(self.health_probe):
            raise TypeError("MCP interceptor health_probe must be callable")


__all__ = [
    "MCP_INTERCEPTOR_CAPABILITY_API_VERSION",
    "MCP_INTERCEPTOR_KIND",
    "McpCallIndeterminateV1",
    "McpCallProjectionV1",
    "McpCallRejectedV1",
    "McpHeaderV1",
    "McpInterceptor",
    "McpInterceptorDescriptor",
    "PreparedMcpCallV1",
]
