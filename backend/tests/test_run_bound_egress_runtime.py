"""Admission seals the egress allowance; the session records it; evidence shows it."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from deerflow_extension_api import TenantReferenceV1

from app.gateway.auth_disabled import AUTH_SOURCE_SESSION
from app.runtime.invocation import InternalLaunchIntent
from deerflow.config.execution_policy_config import ExecutionPolicyConfig
from deerflow.runtime.accepted_invocation import ResolvedAgentMaterialV1, ResolvedAgentRevision
from deerflow.runtime.evidence_summary import build_evidence_summary_v1
from deerflow.runtime.execution_policy import ExecutionBudgetV1
from deerflow.runtime.tenant_identity import TenantIdentityV1
from deerflow.sandbox.accepted_material import (
    AcceptedExecutionEvidenceV1,
    AcceptedMaterialExecutionClaimV1,
    AcceptedMaterialLeaseV1,
    AcceptedMaterialRequestV1,
    AcceptedMaterialRequestV2,
    AcceptedSandboxSession,
    AcceptedSandboxSessionBridge,
    rendered_egress_allowance,
)
from deerflow.sandbox.diagnostics import discard_sandbox_diagnostics, sandbox_diagnostics
from deerflow.sandbox.egress import EgressAllowanceV1, EgressPolicyError, EgressRuleV1
from deerflow.sandbox.sandbox import Sandbox

_TEST_TENANT_IDENTITY = TenantIdentityV1.from_canonical_id("local")


def _material() -> ResolvedAgentMaterialV1:
    return ResolvedAgentMaterialV1(
        agent_id="reviewer",
        storage_source="file",
        storage_version="v1",
        agent_config={"name": "reviewer", "skills": []},
        soul="steady",
        model_profile={"name": "default", "model": "gpt-test"},
        tool_groups=("coding",),
        tools=("bash",),
        skills=(),
        runtime_defaults={},
    )


def _request() -> SimpleNamespace:
    return SimpleNamespace(
        state=SimpleNamespace(user=SimpleNamespace(id="u1", system_role="member"), auth_source=AUTH_SOURCE_SESSION),
        app=SimpleNamespace(
            state=SimpleNamespace(
                extensions=SimpleNamespace(generation=1),
                capability_manifest=SimpleNamespace(digest="f" * 64),
                contributor_host=None,
                tenant_identity=_TEST_TENANT_IDENTITY,
                credential_audit_repo=SimpleNamespace(record=AsyncMock()),
            )
        ),
    )


def _ceiling_config() -> SimpleNamespace:
    return SimpleNamespace(
        execution_policy=ExecutionPolicyConfig.model_validate(
            {"accepted_egress": {"profile": "team-egress-v1", "dns": True, "allow": [{"cidr": "140.82.112.0/20", "port": 443}]}},
        ),
        max_recursion_limit=100,
    )


async def _seal(monkeypatch, *, context: dict[str, object], app_config: object):
    from app.gateway import services

    monkeypatch.setattr(services, "resolve_agent_revision", lambda *_args, **_kwargs: ResolvedAgentRevision.from_material(_material()))
    config: dict[str, object] = {"context": context}
    intent = InternalLaunchIntent(thread_id="thread-egress", input={"messages": []})
    accepted = await services._seal_accepted_invocation(
        request=_request(),
        intent=intent,
        config=config,
        graph_input={"messages": []},
        owner_user_id="u1",
        run_ctx=SimpleNamespace(app_config=app_config),
    )
    return services, accepted, config, intent


@pytest.mark.asyncio
async def test_admission_binds_the_operator_ceiling_when_the_caller_asks_for_nothing(monkeypatch) -> None:
    services, accepted, config, intent = await _seal(monkeypatch, context={}, app_config=object())
    allowance = accepted.egress_allowance
    assert allowance == EgressAllowanceV1.build(profile="accepted-egress-v1", dns=False, rules=())
    assert config["context"]["accepted_egress_allowance"] == allowance
    projection = services._effective_execution_projection(intent, accepted=accepted, graph_input={"messages": []}, config=config)
    assert projection.to_persisted()["egress_allowance"] == allowance.to_json()

    services, accepted, _config, _intent = await _seal(monkeypatch, context={}, app_config=_ceiling_config())
    assert accepted.egress_allowance is not None
    assert accepted.egress_allowance.profile == "team-egress-v1"
    assert accepted.egress_allowance.dns is True
    assert [rule.cidr for rule in accepted.egress_allowance.rules] == ["140.82.112.0/20"]


@pytest.mark.asyncio
async def test_admission_lets_the_caller_narrow_but_never_broaden(monkeypatch) -> None:
    _services, accepted, _config, _intent = await _seal(
        monkeypatch,
        context={"egress_allowance": {"dns": False, "rules": [{"cidr": "140.82.113.0/24", "port": 443}]}},
        app_config=_ceiling_config(),
    )
    assert accepted.egress_allowance is not None
    assert accepted.egress_allowance.dns is False
    assert [(rule.cidr, rule.port) for rule in accepted.egress_allowance.rules] == [("140.82.113.0/24", 443)]

    with pytest.raises(EgressPolicyError, match="egress_allowance_broadening_forbidden"):
        await _seal(monkeypatch, context={"egress_allowance": {"rules": [{"cidr": "1.1.1.0/24", "port": 443}]}}, app_config=_ceiling_config())
    with pytest.raises(EgressPolicyError, match="egress_allowance_broadening_forbidden"):
        await _seal(monkeypatch, context={"egress_allowance": {"rules": [{"cidr": "140.82.113.0/24", "port": 443}]}}, app_config=object())
    with pytest.raises(ValueError, match="egress allowance request must be an object"):
        await _seal(monkeypatch, context={"egress_allowance": "all"}, app_config=_ceiling_config())


class _RecordingSandbox(Sandbox):
    """A handle that is never operated on; the test records diagnostics only."""

    def __init__(self) -> None:
        super().__init__("raw-provider-resource")

    def _never(self, *args, **kwargs):  # pragma: no cover - never invoked
        raise AssertionError("the sandbox must not be operated on")

    execute_command = download_file = glob = grep = list_dir = read_file = update_file = write_file = _never


class _Materializer:
    async def validate(self, lease, evidence):
        return True

    async def renew(self, lease):
        return lease

    async def release(self, lease):
        return None


def _session() -> AcceptedSandboxSession:
    tenant = TenantReferenceV1(version=1, public_ref="tenant-1111111111111111", digest="1" * 64)
    expires_at = datetime(2030, 1, 1, tzinfo=UTC)
    request = AcceptedMaterialRequestV1.build(
        run_id="run-egress",
        attempt_id="attempt-1",
        tenant=tenant,
        user_ref="user-ref",
        thread_ref="thread-ref",
        agent_revision_digest="2" * 64,
        skill_snapshot_digest="3" * 64,
        skill_scope_digest="4" * 64,
        file_manifest=(),
        runtime_image_digest="5" * 64,
        lease_expires_at=expires_at,
    )
    lease = AcceptedMaterialLeaseV1(version=1, provider_kind="test", provider_instance_ref="raw-provider-resource", ownership_epoch=7, lease_expires_at=expires_at, opaque_renewal_handle=object())
    evidence = AcceptedExecutionEvidenceV1.build(
        run_id=request.run_id,
        attempt_id=request.attempt_id,
        tenant=tenant,
        provider_kind=lease.provider_kind,
        provider_instance_ref=lease.provider_instance_ref,
        ownership_epoch=lease.ownership_epoch,
        runtime_image_digest=request.runtime_image_digest,
        skill_snapshot_digest=request.skill_snapshot_digest,
        skill_scope_digest=request.skill_scope_digest,
        materialization_digest=request.digest,
        verifier_image_digest="6" * 64,
        verifier_contract_version="test_v1",
        read_only_proof_digest="7" * 64,
        qualification_scope="contract_test_only",
    )
    claim = AcceptedMaterialExecutionClaimV1(version=1, tenant_digest=tenant.digest, run_id=request.run_id, owner_worker_id="worker-1", state_version=8, execution_takeover=False)

    async def validate_run(_claim):
        return True

    return AcceptedSandboxSession(sandbox=_RecordingSandbox(), materializer=_Materializer(), lease=lease, evidence=evidence, execution_claim=claim, run_fence_validator=validate_run)


@pytest.mark.asyncio
async def test_session_records_the_rendered_allowance_once_as_a_run_bound_diagnostic() -> None:
    discard_sandbox_diagnostics("run-egress")
    allowance = EgressAllowanceV1.build(profile="team-egress-v1", dns=True, rules=(EgressRuleV1.build(cidr="140.82.112.0/20", port=443),))
    bridge = AcceptedSandboxSessionBridge(_session(), owner_loop=asyncio.get_running_loop(), mount_scope=("user-1", "thread-1"))
    bridge.record_egress_allowance(allowance)
    bridge.record_egress_allowance(allowance)
    observations = [observation for _sequence, observation in sandbox_diagnostics("run-egress").since(0)]
    assert [observation.kind for observation in observations] == ["egress.bound"]
    bound = observations[0]
    assert bound.session_kind.value == "accepted"
    assert bound.thread_id == "thread-1"
    assert bound.sandbox_ref == bridge.safe_reference
    assert bound.execution_evidence_digest == bridge.execution_evidence_digest
    assert dict(bound.facts) == {"profile": "team-egress-v1", "rule_count": 1, "dns": True}

    silent = AcceptedSandboxSessionBridge(_session(), owner_loop=asyncio.get_running_loop())
    silent.record_egress_allowance(EgressAllowanceV1.deny_all())
    assert len(sandbox_diagnostics("run-egress").since(0)) == 1
    discard_sandbox_diagnostics("run-egress")


def test_rendered_allowance_is_the_request_allowance_or_deny_all() -> None:
    now = datetime(2026, 9, 5, tzinfo=UTC)
    tenant = TenantReferenceV1(version=1, public_ref="tenant-1111111111111111", digest="1" * 64)
    v1 = AcceptedMaterialRequestV1.build(
        run_id="run-1",
        attempt_id="attempt-1",
        tenant=tenant,
        user_ref="user-ref",
        thread_ref="thread-ref",
        agent_revision_digest="2" * 64,
        skill_snapshot_digest="3" * 64,
        skill_scope_digest="4" * 64,
        file_manifest=(),
        runtime_image_digest="5" * 64,
        lease_expires_at=now,
    )
    assert rendered_egress_allowance(v1) == EgressAllowanceV1.deny_all()
    allowance = EgressAllowanceV1.build(profile="p", dns=False, rules=())
    v2 = AcceptedMaterialRequestV2.build(
        run_id="run-1",
        attempt_id="attempt-1",
        tenant=tenant,
        user_ref="user-ref",
        thread_ref="thread-ref",
        agent_revision_digest="2" * 64,
        skill_snapshot_digest="3" * 64,
        skill_scope_digest="4" * 64,
        file_manifest=(),
        runtime_image_digest="5" * 64,
        lease_expires_at=now,
        accepted_invocation_ref="invocation-ref",
        accepted_invocation_digest="6" * 64,
        tool_plane_base_revision_digest="7" * 64,
        tool_plane_user_overlay_digest="8" * 64,
        tool_plane_projection_digest="9" * 64,
        tool_plane_effective_digest="a" * 64,
        batch_child_attempt_ref=None,
        capability_profile_digest="b" * 64,
        egress_allowance=allowance,
    )
    assert rendered_egress_allowance(v2) is allowance
    assert rendered_egress_allowance(None) == EgressAllowanceV1.deny_all()


def test_evidence_summary_projects_the_allowance_beside_the_budget() -> None:
    budget = ExecutionBudgetV1.build()
    allowance = EgressAllowanceV1.build(profile="team-egress-v1", dns=True, rules=(EgressRuleV1.build(cidr="140.82.112.0/20", port=443), EgressRuleV1.build(cidr="::/0", port=443)))
    arguments = {
        "run_ref": "run-public",
        "thread_ref": "thread-public",
        "status": "success",
        "accepted_at": "2026-09-05T12:00:00Z",
        "updated_at": "2026-09-05T12:01:00Z",
        "terminal_reason": None,
        "budget": budget,
        "policy_state": None,
        "admission": None,
        "assembly": None,
        "decision_events": [],
        "event_counts": {},
        "batches": [],
        "artifacts": {"file_count": 0, "bundle_state": "not_applicable"},
        "qualification": "unqualified",
    }
    policy = build_evidence_summary_v1(**arguments, egress_allowance=allowance)["sections"]["policy"]["data"]
    assert policy["egress_profile"] == "team-egress-v1"
    assert policy["egress_digest"] == allowance.digest
    assert policy["egress_rule_count"] == 2
    assert policy["egress_dns"] is True
    legacy = build_evidence_summary_v1(**arguments)["sections"]["policy"]["data"]
    assert legacy["egress_profile"] is None and legacy["egress_digest"] is None and legacy["egress_rule_count"] is None and legacy["egress_dns"] is None
    assert "egress_profile" not in build_evidence_summary_v1(**{**arguments, "budget": None})["sections"]["policy"]["data"]
