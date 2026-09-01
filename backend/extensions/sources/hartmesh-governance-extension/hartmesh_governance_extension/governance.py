"""Tenant-aware policy and bounded audit contributions using only public API."""

from __future__ import annotations

import asyncio
import json
import os
import queue
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

from deerflow_extension_api import (
    INVOCATION_CONSTRAINTS_CAPABILITY_API_VERSION_V2,
    INVOCATION_CONSTRAINTS_KIND,
    MCP_INTERCEPTOR_CAPABILITY_API_VERSION,
    MCP_INTERCEPTOR_KIND,
    AgentAssemblyDescriptor,
    CapabilityHealthResult,
    ConstraintIndeterminate,
    ConstraintProjectionRequestV2,
    ConstraintProjectionV2,
    ConstraintRejected,
    ExtensionData,
    ExtensionRegistry,
    ExtensionRuntimeDeps,
    InvocationConstraintsProviderFactory,
    McpCallIndeterminateV1,
    McpCallProjectionV1,
    McpInterceptorDescriptor,
    PreparedMcpCallV1,
    ReplicaSafety,
    SafeContextReferenceV1,
    TaskInfo,
    TaskOutcome,
    canonical_hash,
)

_MAX_EVENT_FIELDS = 20
_MAX_EVENT_BYTES = 4096


class GovernanceAuditPort(Protocol):
    """Non-blocking publication and finite health boundary for audit export."""

    def publish(self, event: Mapping[str, str | int | bool | None]) -> None: ...

    async def health(self) -> bool: ...


def _bounded_text(value: object, *, maximum: int = 160) -> str | None:
    if not isinstance(value, str) or not value or len(value.encode("utf-8")) > maximum:
        return None
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        return None
    return value


def _safe_event(**facts: object) -> dict[str, str | int | bool | None]:
    if len(facts) > _MAX_EVENT_FIELDS:
        raise ValueError("governance audit event exceeds the field limit")
    event: dict[str, str | int | bool | None] = {}
    for key, value in facts.items():
        if not key.isidentifier() or len(key) > 64:
            raise ValueError("governance audit event key is invalid")
        if value is None or type(value) in {bool, int}:
            event[key] = value  # type: ignore[assignment]
            continue
        bounded = _bounded_text(value)
        if bounded is None:
            raise ValueError("governance audit event value is invalid")
        event[key] = bounded
    encoded = json.dumps(event, separators=(",", ":"), sort_keys=True).encode("utf-8")
    if len(encoded) > _MAX_EVENT_BYTES:
        raise ValueError("governance audit event exceeds the byte limit")
    return event


class HttpAuditPort:
    """Bounded stdlib HTTP adapter; publication only queues safe references."""

    def __init__(
        self,
        endpoint: str,
        *,
        request_timeout_seconds: float,
        queue_size: int = 256,
    ) -> None:
        if not endpoint.startswith("https://"):
            raise ValueError("audit endpoint must use HTTPS")
        self._endpoint = endpoint.rstrip("/")
        self._request_timeout_seconds = request_timeout_seconds
        self._queue: queue.Queue[dict[str, str | int | bool | None]] = queue.Queue(maxsize=queue_size)
        self._stop = asyncio.Event()
        self._worker: asyncio.Task[None] | None = None

    def publish(self, event: Mapping[str, str | int | bool | None]) -> None:
        try:
            self._queue.put_nowait(dict(event))
        except queue.Full:
            # Bounded fail-closed telemetry posture: never block the agent or
            # evict older evidence to make a new event appear successful.
            return

    def _request(self, path: str, payload: bytes | None = None) -> bool:
        request = urllib.request.Request(
            f"{self._endpoint}/{path}",
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST" if payload is not None else "GET",
        )
        try:
            with urllib.request.urlopen(
                request,
                timeout=self._request_timeout_seconds,
            ) as response:
                return 200 <= response.status < 300
        except (OSError, urllib.error.URLError):
            return False

    async def health(self) -> bool:
        return await asyncio.to_thread(self._request, "health")

    async def start(self) -> None:
        if self._worker is None:
            self._worker = asyncio.create_task(self._drain())

    async def stop(self) -> None:
        self._stop.set()
        worker = self._worker
        if worker is None:
            return
        try:
            await asyncio.wait_for(worker, timeout=self._request_timeout_seconds)
        except TimeoutError:
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)
        self._worker = None

    async def _drain(self) -> None:
        while not self._stop.is_set() or not self._queue.empty():
            try:
                event = self._queue.get_nowait()
            except queue.Empty:
                await asyncio.sleep(0.05)
                continue
            payload = json.dumps(event, separators=(",", ":"), sort_keys=True).encode("utf-8")
            await asyncio.to_thread(self._request, "events", payload)


@dataclass(frozen=True)
class GovernanceConfig:
    policy_revision: str = "governance-template.v1"
    max_total_subagents: int = 2
    allowed_tenant_refs: tuple[str, ...] = ()
    projection_ttl_seconds: int = 300
    health_timeout_seconds: float = 2.0
    audit_endpoint: str | None = None
    audit_adapter: str = "unavailable"
    fail_closed_startup: bool = True

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> GovernanceConfig:
        allowed = {
            "policy_revision",
            "max_total_subagents",
            "allowed_tenant_refs",
            "projection_ttl_seconds",
            "health_timeout_seconds",
            "audit_endpoint",
            "audit_adapter",
            "fail_closed_startup",
        }
        if set(value) - allowed:
            raise ValueError("governance config contains unsupported fields")
        tenants = value.get("allowed_tenant_refs", ())
        if not isinstance(tenants, (list, tuple)):
            raise ValueError("allowed_tenant_refs must be a list")
        config = cls(
            policy_revision=value.get("policy_revision", cls.policy_revision),
            max_total_subagents=value.get("max_total_subagents", cls.max_total_subagents),
            allowed_tenant_refs=tuple(tenants),
            projection_ttl_seconds=value.get("projection_ttl_seconds", cls.projection_ttl_seconds),
            health_timeout_seconds=value.get("health_timeout_seconds", cls.health_timeout_seconds),
            audit_endpoint=value.get("audit_endpoint"),
            audit_adapter=value.get("audit_adapter", cls.audit_adapter),
            fail_closed_startup=value.get("fail_closed_startup", cls.fail_closed_startup),
        )
        if _bounded_text(config.policy_revision, maximum=128) is None:
            raise ValueError("policy_revision is invalid")
        if type(config.max_total_subagents) is not int or not 0 <= config.max_total_subagents <= 10_000:
            raise ValueError("max_total_subagents is invalid")
        if type(config.projection_ttl_seconds) is not int or not 1 <= config.projection_ttl_seconds <= 900:
            raise ValueError("projection_ttl_seconds is invalid")
        if type(config.health_timeout_seconds) not in {int, float} or not 0 < config.health_timeout_seconds <= 30:
            raise ValueError("health_timeout_seconds is invalid")
        if type(config.fail_closed_startup) is not bool:
            raise ValueError("fail_closed_startup must be a boolean")
        if config.audit_adapter not in {"unavailable", "stateless_qualification"}:
            raise ValueError("audit_adapter is invalid")
        if config.audit_endpoint is not None and config.audit_adapter != "unavailable":
            raise ValueError("audit_endpoint and audit_adapter are mutually exclusive")
        if len(config.allowed_tenant_refs) > 128 or any(
            _bounded_text(item, maximum=128) is None for item in config.allowed_tenant_refs
        ):
            raise ValueError("allowed_tenant_refs contains an invalid reference")
        if len(config.allowed_tenant_refs) != len(set(config.allowed_tenant_refs)):
            raise ValueError("allowed_tenant_refs contains duplicates")
        return config


class GovernanceConstraints:
    def __init__(
        self,
        config: GovernanceConfig,
        audit: GovernanceAuditPort,
        *,
        clock: Callable[[], datetime],
    ) -> None:
        self._config = config
        self._audit = audit
        self._clock = clock

    async def project(
        self,
        request: ConstraintProjectionRequestV2,
    ) -> ConstraintProjectionV2 | ConstraintRejected | ConstraintIndeterminate:
        tenant = request.tenant
        if tenant is None or request.extension_artifact_manifest_digest is None:
            return ConstraintIndeterminate()
        if self._config.allowed_tenant_refs and tenant.public_ref not in self._config.allowed_tenant_refs:
            return ConstraintRejected()
        limit = min(
            self._config.max_total_subagents,
            request.host_max_total_subagents,
        )
        issued_at = self._clock()
        if issued_at.tzinfo is None or issued_at.utcoffset() is None:
            return ConstraintIndeterminate()
        evidence_digest = canonical_hash(
            {
                "version": 1,
                "policy_revision": self._config.policy_revision,
                "tenant_ref": tenant.public_ref,
                "request_digest": request.request_digest,
                "trusted_context_digest": request.trusted_context_digest,
                "agent_revision_digest": request.agent_revision.digest,
                "extension_artifact_manifest_digest": (request.extension_artifact_manifest_digest),
                "extension_configuration_digest": (request.extension_configuration_digest),
                "max_total_subagents": limit,
            }
        )
        self._audit.publish(
            _safe_event(
                event="constraint_projected",
                tenant_ref=tenant.public_ref,
                thread_id=request.thread_id,
                agent_revision_digest=request.agent_revision.digest,
                evidence_digest=evidence_digest,
                max_total_subagents=limit,
            )
        )
        return ConstraintProjectionV2(
            request_digest=request.request_digest,
            trusted_context_digest=request.trusted_context_digest,
            thread_id=request.thread_id,
            agent_revision_digest=request.agent_revision.digest,
            profile_revision_digest=request.profile_revision.digest,
            extension_manifest_digest=request.extension_manifest_digest,
            extension_generation=request.extension_generation,
            projection_revision=self._config.policy_revision,
            issued_at=issued_at,
            valid_until=issued_at + timedelta(seconds=self._config.projection_ttl_seconds),
            evidence_id=f"governance:{evidence_digest[:24]}",
            evidence_digest=evidence_digest,
            mandatory_obligations=("max_total_subagents",),
            max_total_subagents=limit,
        )


class GovernanceTaskLifecycle:
    def __init__(self, audit: GovernanceAuditPort) -> None:
        self._audit = audit

    async def on_task_start(
        self,
        app_store: ExtensionData,
        task_store: ExtensionData,
        info: TaskInfo,
    ) -> None:
        self._audit.publish(
            _safe_event(
                event="task_started",
                task_id=info.task_id,
                run_id=info.run_id,
                thread_id=info.thread_id,
                task_kind=info.kind,
                parent_task_id=info.parent_task_id,
                resumed=info.resumed,
            )
        )

    async def on_task_stop(
        self,
        app_store: ExtensionData,
        task_store: ExtensionData,
        info: TaskInfo,
        outcome: TaskOutcome,
    ) -> None:
        self._audit.publish(
            _safe_event(
                event="task_stopped",
                task_id=info.task_id,
                run_id=info.run_id,
                thread_id=info.thread_id,
                task_kind=info.kind,
                outcome=outcome.value,
            )
        )


class GovernanceAssemblyObserver:
    def __init__(self, audit: GovernanceAuditPort) -> None:
        self._audit = audit

    def on_agent_assembled(
        self,
        app_store: ExtensionData,
        descriptor: AgentAssemblyDescriptor,
    ) -> None:
        self._audit.publish(
            _safe_event(
                event="agent_assembled",
                assembly_fingerprint=descriptor.fingerprint,
                agent_namespace=descriptor.namespace,
                agent_name=descriptor.agent_name,
                tool_count=len(descriptor.tools),
                middleware_count=len(descriptor.middlewares),
            )
        )


class GovernanceMcpInterceptor:
    def __init__(self, audit: GovernanceAuditPort) -> None:
        self._audit = audit

    async def prepare_call(
        self,
        request: McpCallProjectionV1,
    ) -> PreparedMcpCallV1 | McpCallIndeterminateV1:
        trusted = request.trusted_context
        tenant = trusted.tenant if trusted is not None else None
        if trusted is None or tenant is None or trusted.extension_artifact_manifest_digest is None:
            return McpCallIndeterminateV1()
        decision_ref = canonical_hash(
            {
                "version": 1,
                "tenant_ref": tenant.public_ref,
                "trusted_context_digest": trusted.digest,
                "server_name": request.server_name,
                "tool_name": request.tool_name,
                "arguments_digest": request.arguments_digest,
            }
        )
        self._audit.publish(
            _safe_event(
                event="mcp_prepared",
                tenant_ref=tenant.public_ref,
                run_id=request.run_id,
                server_name=request.server_name,
                tool_name=request.tool_name,
                decision_ref=decision_ref,
            )
        )
        return PreparedMcpCallV1(
            evidence_references=(
                SafeContextReferenceV1(
                    key="governance_decision_ref",
                    value=decision_ref,
                    storage_class="persistable",
                    purpose="correlation",
                ),
            )
        )


class GovernanceAuditService:
    # The qualified test adapter has no local ownership or authoritative
    # mutable state; every event is derived from the one fenced run attempt.
    replica_safety = ReplicaSafety.STATELESS_REPLICA_SAFE

    def __init__(
        self,
        audit: GovernanceAuditPort,
        *,
        health_timeout_seconds: float,
        fail_closed_startup: bool,
    ) -> None:
        self._audit = audit
        self._health_timeout_seconds = health_timeout_seconds
        self._fail_closed_startup = fail_closed_startup

    async def _healthy(self) -> bool:
        try:
            async with asyncio.timeout(self._health_timeout_seconds):
                return await self._audit.health()
        except (TimeoutError, OSError):
            return False

    async def health(self) -> CapabilityHealthResult:
        healthy = await self._healthy()
        return CapabilityHealthResult(
            status="healthy" if healthy else "unhealthy",
            diagnostic_code=None if healthy else "governance_audit_unavailable",
        )

    async def start(self, deps: ExtensionRuntimeDeps) -> None:
        starter = getattr(self._audit, "start", None)
        if callable(starter):
            await starter()
        if self._fail_closed_startup and not await self._healthy():
            raise RuntimeError("governance_audit_unavailable")

    async def stop(self) -> None:
        stopper = getattr(self._audit, "stop", None)
        if callable(stopper):
            await stopper()


class _UnavailableAuditPort:
    def publish(self, event: Mapping[str, str | int | bool | None]) -> None:
        return None

    async def health(self) -> bool:
        return False


class _StatelessQualificationAuditPort:
    """Side-effect-free health adapter for the exact live qualification lane."""

    def publish(self, event: Mapping[str, str | int | bool | None]) -> None:
        del event

    async def health(self) -> bool:
        return True


def register_governance(
    registry: ExtensionRegistry,
    raw_config: Mapping[str, Any],
    *,
    audit_port: GovernanceAuditPort | None = None,
    clock: Callable[[], datetime] | None = None,
) -> None:
    """Register contributions; tests may inject a fake bounded audit port."""

    config = GovernanceConfig.from_mapping(raw_config)
    audit = audit_port
    if audit is None:
        if config.audit_adapter == "stateless_qualification":
            if os.getenv("DEERFLOW_TEST_KUBERNETES_RUNTIME") != "1":
                raise ValueError("qualification audit adapter is disabled")
            audit = _StatelessQualificationAuditPort()
        elif config.audit_endpoint is not None:
            audit = HttpAuditPort(
                config.audit_endpoint,
                request_timeout_seconds=config.health_timeout_seconds,
            )
    audit = audit or _UnavailableAuditPort()
    service = GovernanceAuditService(
        audit,
        health_timeout_seconds=config.health_timeout_seconds,
        fail_closed_startup=config.fail_closed_startup,
    )
    now = clock or (lambda: datetime.now(UTC))
    registry.invocation_constraints(
        InvocationConstraintsProviderFactory(
            contribution_id="hartmesh.governance.constraints",
            capability_api_version=(INVOCATION_CONSTRAINTS_CAPABILITY_API_VERSION_V2),
            factory=lambda: GovernanceConstraints(config, audit, clock=now),
            kind=INVOCATION_CONSTRAINTS_KIND,
            health_probe=service.health,
        )
    )
    registry.mcp_interceptor(
        McpInterceptorDescriptor(
            contribution_id="hartmesh.governance.mcp",
            capability_api_version=MCP_INTERCEPTOR_CAPABILITY_API_VERSION,
            factory=lambda: GovernanceMcpInterceptor(audit),
            kind=MCP_INTERCEPTOR_KIND,
            health_probe=service.health,
        )
    )
    registry.task_lifecycle(GovernanceTaskLifecycle(audit))
    registry.agent_assembly_observer(GovernanceAssemblyObserver(audit))
    registry.service(service)
