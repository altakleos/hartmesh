from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

import pytest
from deerflow_extension_api import (
    ActingServiceV1,
    AgentAssemblyDescriptor,
    ConstraintProjectionRequestV2,
    ConstraintProjectionV2,
    EffectiveSubjectV1,
    ExtensionData,
    ExtensionRuntimeDeps,
    InvocationIdentityV1,
    McpCallProjectionV1,
    PrincipalProjectionV1,
    ReplicaSafety,
    ResolvedAgentRevisionReferenceV1,
    ResolvedProfileRevisionReferenceV1,
    SealedOriginV1,
    TaskInfo,
    TaskOutcome,
    TenantReferenceV1,
    TrustedRunContextV1,
)

from hartmesh_governance_extension import install, register_governance

_DIGESTS = tuple(character * 64 for character in "abcdef123456")


class FakeRegistry:
    def __init__(self) -> None:
        self.constraints: list[Any] = []
        self.mcp: list[Any] = []
        self.lifecycle: list[Any] = []
        self.assembly: list[Any] = []
        self.services: list[Any] = []

    def invocation_constraints(self, descriptor: Any) -> None:
        self.constraints.append(descriptor)

    def mcp_interceptor(self, descriptor: Any) -> None:
        self.mcp.append(descriptor)

    def task_lifecycle(self, contributor: Any) -> None:
        self.lifecycle.append(contributor)

    def agent_assembly_observer(self, observer: Any) -> None:
        self.assembly.append(observer)

    def service(self, service: Any) -> None:
        self.services.append(service)


class FakeAuditPort:
    def __init__(self, *, healthy: bool = True) -> None:
        self.healthy = healthy
        self.events: list[dict[str, object]] = []

    def publish(self, event) -> None:
        self.events.append(dict(event))

    async def health(self) -> bool:
        return self.healthy


def _trusted() -> TrustedRunContextV1:
    identity = InvocationIdentityV1(
        effective_subject=EffectiveSubjectV1(
            kind="human",
            subject_id="subject-1",
            role="member",
        ),
        acting_service=ActingServiceV1(service_id="gateway"),
    )
    tenant = TenantReferenceV1(
        version=1,
        public_ref=f"tenant-{_DIGESTS[0][:16]}",
        digest=_DIGESTS[0],
    )
    return TrustedRunContextV1(
        identity=identity,
        tenant=tenant,
        origin=SealedOriginV1(
            source_kind="http",
            digest=_DIGESTS[1],
        ),
        thread_id="thread-1",
        external_key_reference=None,
        agent_revision=ResolvedAgentRevisionReferenceV1(
            agent_id="default",
            digest=_DIGESTS[2],
        ),
        profile_revision=ResolvedProfileRevisionReferenceV1(
            profile_id="default",
            digest=_DIGESTS[3],
        ),
        extension_generation=7,
        extension_manifest_digest=_DIGESTS[4],
        extension_artifact_manifest_digest="sha256:" + _DIGESTS[5],
        extension_configuration_digest="sha256:" + _DIGESTS[6],
        run_id="run-1",
    )


def _constraint_request(trusted: TrustedRunContextV1) -> ConstraintProjectionRequestV2:
    return ConstraintProjectionRequestV2(
        identity=trusted.identity,
        origin=trusted.origin,
        policy_lookup_references=(),
        thread_id=trusted.thread_id,
        external_key_reference=None,
        agent_revision=trusted.agent_revision,
        profile_revision=trusted.profile_revision,
        request_digest=_DIGESTS[7],
        trusted_context_digest=trusted.digest,
        extension_manifest_digest=trusted.extension_manifest_digest,
        extension_generation=trusted.extension_generation,
        host_max_total_subagents=3,
        tenant=trusted.tenant,
        extension_artifact_manifest_digest=(trusted.extension_artifact_manifest_digest),
        extension_configuration_digest=trusted.extension_configuration_digest,
    )


@pytest.mark.asyncio
async def test_policy_never_widens_host_limit_and_audits_only_safe_references() -> None:
    registry = FakeRegistry()
    audit = FakeAuditPort()
    now = datetime(2026, 9, 1, tzinfo=UTC)
    register_governance(
        registry,
        {"max_total_subagents": 8},
        audit_port=audit,
        clock=lambda: now,
    )
    trusted = _trusted()

    projected = await registry.constraints[0].factory().project(_constraint_request(trusted))

    assert isinstance(projected, ConstraintProjectionV2)
    assert projected.max_total_subagents == 3
    assert projected.agent_revision_digest == trusted.agent_revision.digest
    assert audit.events == [
        {
            "event": "constraint_projected",
            "tenant_ref": trusted.tenant.public_ref,
            "thread_id": "thread-1",
            "agent_revision_digest": trusted.agent_revision.digest,
            "evidence_digest": projected.evidence_digest,
            "max_total_subagents": 3,
        }
    ]
    forbidden = ("prompt", "payload", "credential", "memory", "tenant_config")
    assert all(token not in str(audit.events).lower() for token in forbidden)


@pytest.mark.asyncio
async def test_mcp_lifecycle_and_assembly_export_bounded_host_facts_only() -> None:
    registry = FakeRegistry()
    audit = FakeAuditPort()
    register_governance(registry, {}, audit_port=audit)
    trusted = _trusted()
    mcp_request = McpCallProjectionV1(
        principal=PrincipalProjectionV1(identity=trusted.identity),
        origin=trusted.origin,
        thread_id=trusted.thread_id,
        run_id="run-1",
        agent_revision=trusted.agent_revision,
        extension_generation=trusted.extension_generation,
        server_name="records",
        tool_name="lookup",
        arguments_digest=_DIGESTS[8],
        trusted_context=trusted,
    )

    prepared = await registry.mcp[0].factory().prepare_call(mcp_request)
    assert prepared.headers == ()
    assert [item.key for item in prepared.evidence_references] == ["governance_decision_ref"]

    task = TaskInfo(
        task_id="task-1",
        run_id="run-1",
        thread_id="thread-1",
        kind="lead",
    )
    await registry.lifecycle[0].on_task_start(ExtensionData("app"), ExtensionData("task"), task)
    await registry.lifecycle[0].on_task_stop(
        ExtensionData("app"),
        ExtensionData("task"),
        task,
        TaskOutcome.COMPLETED,
    )
    descriptor = AgentAssemblyDescriptor(
        namespace="lead",
        agent_name="default",
        requested_model=None,
        effective_model="model",
        model_parameters={},
        thinking_enabled=False,
        reasoning_effort=None,
        base_prompt_hash=_DIGESTS[9],
        tools=(),
        middlewares=(),
        deferred_tool_names=(),
        enabled_skills=(),
        effective_policies={},
    )
    registry.assembly[0].on_agent_assembled(ExtensionData("app"), descriptor)

    assert [event["event"] for event in audit.events] == [
        "mcp_prepared",
        "task_started",
        "task_stopped",
        "agent_assembled",
    ]
    assert "arguments" not in str(audit.events)
    assert descriptor.fingerprint in str(audit.events)


@pytest.mark.asyncio
async def test_required_health_and_startup_are_finite_and_fail_closed() -> None:
    registry = FakeRegistry()
    audit = FakeAuditPort(healthy=False)
    register_governance(
        registry,
        {"health_timeout_seconds": 0.05, "fail_closed_startup": True},
        audit_port=audit,
    )

    health = await asyncio.wait_for(
        registry.constraints[0].health_probe(),
        timeout=0.2,
    )
    assert health.status == "unhealthy"
    assert health.diagnostic_code == "governance_audit_unavailable"
    with pytest.raises(RuntimeError, match="governance_audit_unavailable"):
        await asyncio.wait_for(
            registry.services[0].start(ExtensionRuntimeDeps()),
            timeout=0.2,
        )


def test_entry_point_registers_only_typed_public_contributions() -> None:
    registry = FakeRegistry()

    install(registry, {"fail_closed_startup": False})

    assert install.__deerflow_api__ == "0.13.0"
    assert len(registry.constraints) == 1
    assert len(registry.mcp) == 1
    assert len(registry.lifecycle) == 1
    assert len(registry.assembly) == 1
    assert len(registry.services) == 1


@pytest.mark.asyncio
async def test_stateless_qualification_audit_adapter_is_explicit_and_test_gated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("DEERFLOW_TEST_KUBERNETES_RUNTIME", raising=False)
    with pytest.raises(ValueError, match="qualification audit adapter is disabled"):
        register_governance(
            FakeRegistry(),
            {"audit_adapter": "stateless_qualification"},
        )

    monkeypatch.setenv("DEERFLOW_TEST_KUBERNETES_RUNTIME", "1")
    registry = FakeRegistry()
    register_governance(
        registry,
        {
            "audit_adapter": "stateless_qualification",
            "fail_closed_startup": True,
        },
    )

    assert registry.services[0].replica_safety is ReplicaSafety.STATELESS_REPLICA_SAFE
    assert (await registry.services[0].health()).status == "healthy"
    await registry.services[0].start(ExtensionRuntimeDeps())
