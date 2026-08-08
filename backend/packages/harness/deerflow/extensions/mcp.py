"""Capability Host enforcement for required MCP call preparation."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from collections.abc import Collection, Mapping
from dataclasses import dataclass
from functools import partial
from typing import Any

from deerflow_extension_api import (
    McpCallIndeterminateV1,
    McpCallProjectionV1,
    McpCallRejectedV1,
    McpInterceptor,
    PreparedMcpCallV1,
    PrincipalProjectionV1,
    ResolvedAgentRevisionReferenceV1,
    SafeContextReferenceV1,
    SealedOriginV1,
    TrustedRunContextV1,
)

from deerflow.authz.runtime import (
    AuthorizedToolCallReceipt,
    authorization_provider_from_context,
)
from deerflow.extensions.capabilities import CapabilityHealthMonitor
from deerflow.extensions.registry import LoadedExtensions
from deerflow.guardrails.provider import current_guardrail_provider_receipt

logger = logging.getLogger(__name__)

MCP_INVOCATION_FACTS_CONTEXT_KEY = "__deerflow_mcp_invocation_facts"
MCP_PREPARATION_AUDIT_SINK_CONTEXT_KEY = "__deerflow_mcp_preparation_audit_sink"
_CAPABILITY_PREFIX = "mcp_interceptor:"
_DEFAULT_TIMEOUT_SECONDS = 2.0


@dataclass(frozen=True)
class McpInvocationFacts:
    """Host-sealed invocation facts carried to the MCP operation boundary."""

    principal: PrincipalProjectionV1
    origin: SealedOriginV1
    thread_id: str
    run_id: str
    agent_revision: ResolvedAgentRevisionReferenceV1
    extension_generation: int
    trusted_context: TrustedRunContextV1 | None = None

    @classmethod
    def from_accepted(cls, accepted: Any, *, run_id: str) -> McpInvocationFacts:
        principal = accepted.principal
        origin = accepted.origin
        revision = accepted.agent_revision
        accepted_trusted_context = getattr(accepted, "trusted_context", None)
        trusted_context = accepted_trusted_context.bind_run(run_id) if isinstance(accepted_trusted_context, TrustedRunContextV1) else None
        return cls(
            principal=PrincipalProjectionV1(
                user_id=principal.user_id,
                role=principal.role,
                oauth_provider=principal.oauth_provider,
                oauth_id=principal.oauth_id,
                channel_user_id=principal.channel_user_id,
                is_internal=principal.is_internal,
                identity=principal.identity,
            ),
            origin=(
                trusted_context.origin
                if trusted_context is not None
                else SealedOriginV1(
                    source_kind=origin.source_kind,
                    references=tuple(
                        SafeContextReferenceV1(
                            key=key,
                            value=value,
                            storage_class="persistable",
                            purpose="correlation",
                        )
                        for key, value in sorted(origin.references.items())
                    ),
                    digest=accepted.base_origin_digest,
                )
            ),
            thread_id=accepted.thread_id,
            run_id=run_id,
            agent_revision=ResolvedAgentRevisionReferenceV1(
                agent_id=revision.agent_id,
                digest=revision.digest,
            ),
            extension_generation=accepted.extension_generation,
            trusted_context=trusted_context,
        )


@dataclass(frozen=True)
class McpInterceptorDiagnostic:
    capability_id: str
    diagnostic_code: str


@dataclass(frozen=True)
class _InitializedInterceptor:
    contribution_id: str
    capability_id: str
    interceptor: McpInterceptor


class McpCallPreparationError(RuntimeError):
    """Safe fail-closed MCP operation error."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(f"MCP call preparation failed ({code})")


@dataclass(frozen=True)
class McpPreparationAuditSink:
    """Thread-safe bridge back to the parent run's loop-bound journal."""

    journal: Any
    event_loop: asyncio.AbstractEventLoop

    def record_middleware(
        self,
        tag: str,
        *,
        name: str,
        hook: str,
        action: str,
        changes: dict[str, Any],
    ) -> None:
        callback = partial(
            self.journal.record_middleware,
            tag,
            name=name,
            hook=hook,
            action=action,
            changes=changes,
        )
        try:
            self.event_loop.call_soon_threadsafe(callback)
        except RuntimeError:
            logger.warning("Failed to schedule MCP preparation evidence on the parent run loop")


def build_mcp_preparation_audit_sink(
    context: Mapping[str, Any] | None,
) -> McpPreparationAuditSink | None:
    """Capture a safe bridge without moving the loop-bound journal itself."""

    if context is None:
        return None
    journal = context.get("__run_journal")
    if journal is None or not callable(getattr(journal, "record_middleware", None)):
        return None
    try:
        event_loop = asyncio.get_running_loop()
    except RuntimeError:
        return None
    return McpPreparationAuditSink(journal=journal, event_loop=event_loop)


def _canonical_arguments_digest(arguments: object) -> str:
    canonical = json.dumps(
        {"version": 1, "arguments": arguments},
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def mcp_invocation_facts_from_context(context: object) -> McpInvocationFacts | None:
    if not isinstance(context, Mapping):
        return None
    facts = context.get(MCP_INVOCATION_FACTS_CONTEXT_KEY)
    return facts if isinstance(facts, McpInvocationFacts) else None


def _runtime_context(request: object) -> Mapping[str, Any]:
    runtime = getattr(request, "runtime", None)
    context = getattr(runtime, "context", None) if runtime is not None else None
    return context if isinstance(context, Mapping) else {}


class McpInterceptorHost:
    """Startup-initialized required MCP preparation runtime for one generation."""

    def __init__(
        self,
        extensions: LoadedExtensions,
        *,
        required_capabilities: Collection[str] = (),
        timeout_seconds: float = _DEFAULT_TIMEOUT_SECONDS,
    ) -> None:
        required = tuple(required_capabilities)
        if len(required) != len(set(required)):
            raise ValueError("required_capabilities contains a duplicate capability ID")
        self._required = frozenset(capability_id for capability_id in required if capability_id.startswith(_CAPABILITY_PREFIX))
        self._generation = extensions.generation
        self._timeout_seconds = float(timeout_seconds)
        if self._timeout_seconds <= 0:
            raise ValueError("MCP interceptor timeout_seconds must be positive")

        diagnostics: list[McpInterceptorDiagnostic] = []
        initialized: list[_InitializedInterceptor] = []
        registered_ids: set[str] = set()
        for registration in sorted(
            extensions.mcp_interceptor_descriptors,
            key=lambda item: item.contribution_id,
        ):
            capability_id = f"{_CAPABILITY_PREFIX}{registration.contribution_id}"
            registered_ids.add(capability_id)
            if registration.contribution_id in extensions.mcp_interceptor_conflicts:
                diagnostics.append(
                    McpInterceptorDiagnostic(
                        capability_id=capability_id,
                        diagnostic_code="duplicate_registration",
                    )
                )
                continue
            try:
                interceptor = registration.factory()
                if not isinstance(interceptor, McpInterceptor):
                    raise TypeError("factory returned an object that does not implement McpInterceptor")
            except Exception:
                diagnostics.append(
                    McpInterceptorDiagnostic(
                        capability_id=capability_id,
                        diagnostic_code="initialization_failed",
                    )
                )
                continue
            initialized.append(
                _InitializedInterceptor(
                    contribution_id=registration.contribution_id,
                    capability_id=capability_id,
                    interceptor=interceptor,
                )
            )
        for capability_id in sorted(self._required - registered_ids):
            diagnostics.append(
                McpInterceptorDiagnostic(
                    capability_id=capability_id,
                    diagnostic_code="not_registered",
                )
            )
        self._initialized = tuple(initialized)
        self.startup_diagnostics = tuple(diagnostics)

    @property
    def required_capability_ids(self) -> frozenset[str]:
        return self._required

    @property
    def initialized_capability_ids(self) -> frozenset[str]:
        return frozenset(item.capability_id for item in self._initialized)

    def build_tool_interceptor(
        self,
        *,
        health_monitor: CapabilityHealthMonitor,
        compatibility_interceptors: Collection[Any] = (),
    ) -> Any:
        """Build one authorization-to-network boundary pinned to this set."""

        required_by_id = {item.capability_id: item for item in self._initialized if item.capability_id in self._required}
        ordered = tuple(sorted(required_by_id.values(), key=lambda item: item.contribution_id))
        required = self._required
        generation = self._generation
        timeout_seconds = self._timeout_seconds
        compatibility = tuple(compatibility_interceptors)

        async def prepare_required_call(request: Any, handler: Any) -> Any:
            context = _runtime_context(request)
            provider = authorization_provider_from_context(context)
            facts = mcp_invocation_facts_from_context(context)
            if facts is None:
                raise McpCallPreparationError("accepted_invocation_missing")
            receipt = current_guardrail_provider_receipt()
            if provider is None or not isinstance(receipt, AuthorizedToolCallReceipt) or receipt.provider is not provider:
                raise McpCallPreparationError("authorization_indeterminate")
            authz_request = receipt.request
            principal = authz_request.principal
            if (
                authz_request.resource != "tool"
                or authz_request.action != "call"
                or not authz_request.target
                or principal.user_id != facts.principal.user_id
                or (facts.principal.role is not None and principal.role != facts.principal.role)
                or principal.oauth_provider != facts.principal.oauth_provider
                or principal.oauth_id != facts.principal.oauth_id
                or principal.channel_user_id != facts.principal.channel_user_id
                or principal.is_internal is not facts.principal.is_internal
                or principal.identity != facts.principal.identity
                or authz_request.trusted_context != facts.trusted_context
                or authz_request.context.get("origin") != facts.origin
                or authz_request.context.get("thread_id") != facts.thread_id
                or authz_request.context.get("run_id") != facts.run_id
            ):
                raise McpCallPreparationError("authorization_indeterminate")
            try:
                authorized_arguments_digest = _canonical_arguments_digest(authz_request.context.get("tool_input"))
                initial_arguments_digest = _canonical_arguments_digest(request.args)
            except Exception:
                raise McpCallPreparationError("authorization_indeterminate") from None
            if authorized_arguments_digest != initial_arguments_digest:
                raise McpCallPreparationError("authorization_indeterminate")
            if facts.extension_generation != generation:
                raise McpCallPreparationError("capability_generation_mismatch")
            if frozenset(required_by_id) != required:
                raise McpCallPreparationError("required_interceptor_unavailable")
            initial_name = str(request.name)
            initial_server_name = str(request.server_name)
            dispatched = False

            async def prepare_then_dispatch(prepared_request: Any) -> Any:
                nonlocal dispatched
                if dispatched:
                    raise McpCallPreparationError("handler_reentry")
                dispatched = True
                try:
                    arguments_digest = _canonical_arguments_digest(prepared_request.args)
                except Exception:
                    raise McpCallPreparationError("preparation_indeterminate") from None
                if arguments_digest != authorized_arguments_digest or str(prepared_request.name) != initial_name or str(prepared_request.server_name) != initial_server_name:
                    raise McpCallPreparationError("compatibility_call_changed")
                if facts.extension_generation != generation:
                    raise McpCallPreparationError("capability_generation_mismatch")
                if frozenset(required_by_id) != required:
                    raise McpCallPreparationError("required_interceptor_unavailable")
                health = await health_monitor.health_for(required, refresh=True)
                if {item.capability_id for item in health} != required or any(item.status != "healthy" for item in health):
                    raise McpCallPreparationError("required_interceptor_unhealthy")
                try:
                    projection = McpCallProjectionV1(
                        principal=facts.principal,
                        origin=facts.origin,
                        thread_id=facts.thread_id,
                        run_id=facts.run_id,
                        agent_revision=facts.agent_revision,
                        extension_generation=facts.extension_generation,
                        server_name=initial_server_name,
                        tool_name=initial_name,
                        arguments_digest=arguments_digest,
                        trusted_context=facts.trusted_context,
                    )
                except Exception:
                    raise McpCallPreparationError("preparation_indeterminate")

                headers = dict(prepared_request.headers or {})
                header_names = {str(name).casefold(): str(name) for name in headers}
                evidence: list[dict[str, Any]] = []
                for item in ordered:
                    try:
                        async with asyncio.timeout(timeout_seconds):
                            result = await item.interceptor.prepare_call(projection)
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        raise McpCallPreparationError("preparation_indeterminate") from None
                    if type(result) is McpCallRejectedV1:
                        raise McpCallPreparationError("preparation_rejected")
                    if type(result) is McpCallIndeterminateV1:
                        raise McpCallPreparationError("preparation_indeterminate")
                    if type(result) is not PreparedMcpCallV1:
                        raise McpCallPreparationError("preparation_indeterminate")
                    contribution_evidence: list[dict[str, Any]] = []
                    for header in result.headers:
                        folded = header.name.casefold()
                        existing_name = header_names.get(folded)
                        if existing_name is not None:
                            if headers[existing_name] != header.value:
                                raise McpCallPreparationError("header_conflict")
                            continue
                        headers[header.name] = header.value
                        header_names[folded] = header.name
                    for reference in result.evidence_references:
                        if reference.storage_class != "persistable":
                            continue
                        contribution_evidence.append(
                            {
                                "key": reference.key,
                                "purpose": reference.purpose,
                                "value": reference.value,
                            }
                        )
                    evidence.append(
                        {
                            "contribution_id": item.contribution_id,
                            "references": contribution_evidence,
                        }
                    )
                _record_audit(context, generation=generation, evidence=evidence)
                return await handler(prepared_request.override(headers=headers))

            composed = prepare_then_dispatch
            for interceptor in reversed(compatibility):
                inner = composed

                async def wrapped(
                    compatibility_request: Any,
                    _interceptor: Any = interceptor,
                    _handler: Any = inner,
                ) -> Any:
                    return await _interceptor(compatibility_request, _handler)

                composed = wrapped
            return await composed(request)

        return prepare_required_call


@dataclass(frozen=True)
class McpInterceptorRuntime:
    host: McpInterceptorHost
    health_monitor: CapabilityHealthMonitor


_runtime: McpInterceptorRuntime | None = None


def configure_mcp_interceptor_runtime(
    host: McpInterceptorHost,
    health_monitor: CapabilityHealthMonitor,
) -> None:
    """Install the Gateway-owned immutable MCP runtime for tool construction."""

    global _runtime
    _runtime = McpInterceptorRuntime(
        host=host,
        health_monitor=health_monitor,
    )


def reset_mcp_interceptor_runtime() -> None:
    global _runtime
    _runtime = None


def get_required_mcp_tool_interceptor(
    *,
    compatibility_interceptors: Collection[Any] = (),
) -> Any | None:
    """Return the configured trusted wrapper, or none when nothing is required."""

    runtime = _runtime
    if runtime is None or not runtime.host.required_capability_ids:
        return None
    return runtime.host.build_tool_interceptor(
        health_monitor=runtime.health_monitor,
        compatibility_interceptors=compatibility_interceptors,
    )


def _record_audit(
    context: Mapping[str, Any],
    *,
    generation: int,
    evidence: list[dict[str, Any]],
) -> None:
    journal = context.get(MCP_PREPARATION_AUDIT_SINK_CONTEXT_KEY)
    if journal is None:
        journal = context.get("__run_journal")
    if journal is None:
        return
    try:
        from deerflow.runtime.events.catalog import MIDDLEWARE_MCP_PREPARATION_TAG

        journal.record_middleware(
            tag=MIDDLEWARE_MCP_PREPARATION_TAG,
            name="McpInterceptorHost",
            hook="prepare_call",
            action="prepared_mcp_call",
            changes={
                "version": 1,
                "extension_generation": generation,
                "contributions": evidence,
            },
        )
    except Exception:
        logger.warning("Failed to record MCP preparation evidence", exc_info=True)


__all__ = [
    "MCP_INVOCATION_FACTS_CONTEXT_KEY",
    "MCP_PREPARATION_AUDIT_SINK_CONTEXT_KEY",
    "McpCallPreparationError",
    "McpInterceptorDiagnostic",
    "McpInterceptorHost",
    "McpInterceptorRuntime",
    "McpInvocationFacts",
    "McpPreparationAuditSink",
    "build_mcp_preparation_audit_sink",
    "configure_mcp_interceptor_runtime",
    "get_required_mcp_tool_interceptor",
    "mcp_invocation_facts_from_context",
    "reset_mcp_interceptor_runtime",
]
