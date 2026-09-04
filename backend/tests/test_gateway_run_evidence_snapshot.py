from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime

import pytest
from deerflow_extension_api import (
    CredentialEvidenceV1,
    EffectiveSubjectV1,
    InvocationIdentityV1,
    ResolvedAgentRevisionReferenceV1,
    ResolvedProfileRevisionReferenceV1,
    SealedOriginV1,
    TrustedRunContextV1,
    effective_authority_digest_v1,
)

from app.gateway.run_evidence import GatewayRunEvidenceSnapshotReader
from app.runtime.idempotency import REQUEST_DIGEST_VERSION, canonical_request_digest
from deerflow.retrieval import (
    RetrievalObservationDraftV1,
    RetrievalObservationV1,
)
from deerflow.runtime.accepted_invocation import (
    AcceptedInvocation,
    InvocationOrigin,
    PrincipalProjection,
    ResolvedAgentMaterialV1,
    ResolvedAgentRevision,
    canonical_digest,
)
from deerflow.runtime.assembly_evidence import (
    ASSEMBLY_DESCRIPTOR_VERSION,
    ASSEMBLY_EVIDENCE_VERSION,
    AssemblyEvidenceV1,
    assembly_evidence_digest,
)
from deerflow.runtime.run_evidence import (
    EvidenceSnapshotRequest,
    RunEvidenceBundleError,
    RunEvidenceSnapshotService,
)
from deerflow.runtime.tenant_identity import TenantIdentityV1
from deerflow.runtime.tool_evidence import (
    DurableToolReceiptV1,
    ToolAttemptContextV1,
    receipt_event_metadata,
)
from deerflow.sandbox.accepted_material import (
    AcceptedExecutionEvidenceV2,
    AcceptedMaterialCapability,
    AcceptedMaterialLeaseV1,
    AcceptedMaterialRequestV2,
    AcceptedSandboxCapabilityProfileV1,
    AcceptedSandboxIsolationFactsV1,
    AcceptedSandboxQualificationV1,
    accepted_execution_evidence_reference,
    accepted_scope_reference,
)
from deerflow.tool_plane import EffectiveToolPlaneRevisionV1

_TENANT = TenantIdentityV1.from_canonical_id("local").to_persisted_reference()
_OWNER = "owner-1"
_THREAD = "thread-evidence"
_RUN = "run-evidence"


def _accepted(
    *,
    mcp_submit_tool: str | None = None,
    mcp_tool_name_prefix: bool = False,
) -> AcceptedInvocation:
    material = ResolvedAgentMaterialV1(
        agent_id="lead_agent",
        storage_source="builtin",
        storage_version="1",
        agent_config={"name": "lead_agent"},
        soul="private-material-soul",
        model_profile={"name": "default", "model": "test"},
    )
    revision = ResolvedAgentRevision.from_material(material)
    identity = InvocationIdentityV1(
        effective_subject=EffectiveSubjectV1(
            kind="human",
            subject_id=_OWNER,
            role="member",
        )
    )
    origin_digest = canonical_digest(
        {
            "version": 1,
            "source_kind": "http",
            "references": [],
            "contributor_references": [],
        }
    )
    extension_manifest = "a" * 64
    extension_artifact = "sha256:" + "b" * 64
    extension_config = "sha256:" + "c" * 64
    trusted = TrustedRunContextV1(
        identity=identity,
        origin=SealedOriginV1(source_kind="http", digest=origin_digest),
        thread_id=_THREAD,
        external_key_reference=None,
        agent_revision=ResolvedAgentRevisionReferenceV1(
            agent_id=revision.agent_id,
            digest=revision.digest,
        ),
        profile_revision=ResolvedProfileRevisionReferenceV1(
            profile_id="default",
            digest="d" * 64,
        ),
        extension_generation=1,
        extension_manifest_digest=extension_manifest,
        extension_artifact_manifest_digest=extension_artifact,
        extension_configuration_digest=extension_config,
        tenant=_TENANT,
        credential=CredentialEvidenceV1(
            method="session",
            credential_ref=None,
            effective_authority_digest=effective_authority_digest_v1(("runs:read",)),
            authority_categories=("runs",),
        ),
    )
    effective_servers = ()
    effective_server_ids = ()
    if mcp_submit_tool is not None:
        effective_server_ids = ("reports",)
        effective_servers = (
            {
                "server_id": "reports",
                "enabled": True,
                "transport": "http",
                "args": [],
                "url": "https://mcp.example.test",
                "tool_allowlist": [],
                "tool_overrides": {},
                "description": "",
                "routing": {"mode": "off", "priority": 0, "keywords": []},
                "tool_name_prefix": mcp_tool_name_prefix,
                "tool_call_timeout": None,
                "session_init_timeout": None,
                "task_toolsets": [
                    {
                        "name": "reports",
                        "submit_tool": mcp_submit_tool,
                        "status_tool": "report_status",
                        "cancel_tool": "cancel_report",
                    }
                ],
                "secret_selectors": [],
            },
        )
    tool_plane = EffectiveToolPlaneRevisionV1(
        base_revision_digest="e" * 64,
        user_overlay_digest="f" * 64,
        base_generation=1,
        overlay_generation=1,
        projection_digest="1" * 64,
        effective_mcp_server_ids=effective_server_ids,
        effective_mcp_servers=effective_servers,
    )
    return AcceptedInvocation.seal(
        principal=PrincipalProjection(identity=identity),
        origin=InvocationOrigin(source_kind="http"),
        thread_id=_THREAD,
        context_references={},
        agent_revision=revision,
        normalized_input={},
        execution_options={
            "multitask_strategy": "reject",
            "interrupt_before": None,
            "interrupt_after": None,
            "checkpoint_id": None,
            "recursion_limit": 100,
        },
        extension_generation=1,
        extension_manifest_digest=extension_manifest,
        extension_artifact_manifest_digest=extension_artifact,
        extension_configuration_digest=extension_config,
        tool_plane_revision=tool_plane.to_json(),
        contributor_execution_digest="2" * 64,
        tenant=_TENANT,
        trusted_context=trusted,
    )


def _row(
    *,
    mcp_submit_tool: str | None = None,
    mcp_tool_name_prefix: bool = False,
) -> dict:
    accepted = _accepted(
        mcp_submit_tool=mcp_submit_tool,
        mcp_tool_name_prefix=mcp_tool_name_prefix,
    )
    persisted = accepted.to_persisted()
    persisted_tool_plane = persisted["decision_evidence_json"]["tool_plane_revision"]
    effective = {
        "accepted_digest_semantics": "canonical_execution_v2",
        "thread_id": accepted.thread_id,
        "agent_selector": "default",
        "agent_revision_digest": accepted.agent_revision.digest,
        "principal_digest": accepted.principal_digest,
        "base_origin_digest": accepted.base_origin_digest,
        "tenant_digest": accepted.tenant.digest,
        "accepted_context_digest": accepted.accepted_context_digest,
        "runtime_identity_digest": accepted.runtime_identity_digest,
        "contributor_execution_digest": accepted.contributor_execution_digest,
        "extension_generation": accepted.extension_generation,
        "extension_artifact_manifest_digest": (accepted.extension_artifact_manifest_digest),
        "extension_configuration_digest": (accepted.extension_configuration_digest),
        "tool_plane_revision": persisted_tool_plane,
        "input": {},
        "command": None,
        "multitask_strategy": "reject",
        "checkpoint": {},
        "interrupt_before": None,
        "interrupt_after": None,
        "execution_context": {},
        "recursion_limit": 100,
    }
    assembly = AssemblyEvidenceV1(
        version=ASSEMBLY_EVIDENCE_VERSION,
        fingerprint="3" * 64,
        descriptor_version=ASSEMBLY_DESCRIPTOR_VERSION,
        namespace="deerflow",
        agent_name="lead_agent",
        effective_model="default",
        prompt_digest="4" * 64,
        toolset_digest="5" * 64,
        middleware_digest="6" * 64,
        skillset_digest="7" * 64,
        policy_digest="8" * 64,
        accepted_agent_revision_digest=accepted.agent_revision.digest,
        extension_generation=accepted.extension_generation,
        accepted_capability_manifest_digest=accepted.extension_manifest_digest,
        accepted_artifact_manifest_digest=(accepted.extension_artifact_manifest_digest),
        accepted_extension_configuration_digest=(accepted.extension_configuration_digest),
        tenant=_TENANT,
    )
    return {
        "run_id": _RUN,
        "thread_id": _THREAD,
        "user_id": _OWNER,
        "operation_kind": "run",
        "status": "success",
        "stop_reason": "completed",
        "state_version": 3,
        "created_at": "2026-09-04T12:00:00+00:00",
        "updated_at": "2026-09-04T12:01:00+00:00",
        "kwargs": {"__accepted_request_projection_v1": effective},
        "request_digest": canonical_request_digest(effective),
        "request_digest_version": REQUEST_DIGEST_VERSION,
        "assembly_evidence_json": assembly.to_persisted_json(),
        "assembly_evidence_digest": assembly_evidence_digest(assembly),
        "execution_evidence_json": None,
        "execution_evidence_digest": None,
        **persisted,
    }


def _events() -> list[dict]:
    return [
        {
            "thread_id": _THREAD,
            "run_id": _RUN,
            "seq": 10,
            "event_type": "run.terminal.v1",
            "category": "trace",
            "content": {
                "version": 1,
                "status": "success",
                "stop_reason": "completed",
                "failure": None,
            },
            "metadata": {},
            "created_at": "2026-09-04T12:01:00+00:00",
        },
        {
            "thread_id": _THREAD,
            "run_id": _RUN,
            "seq": 11,
            "event_type": "run.delivery",
            "category": "outputs",
            "content": {
                "presented": 1,
                "paths": ["/mnt/user-data/outputs/report.txt"],
                "by_tool": {"present_files": ["/mnt/user-data/outputs/report.txt"]},
            },
            "metadata": {},
            "created_at": "2026-09-04T12:01:00+00:00",
        },
    ]


def _sandbox_evidence(
    row: dict,
    *,
    drift_field: str | None = None,
) -> AcceptedExecutionEvidenceV2:
    accepted = AcceptedInvocation.from_persisted(row)
    assert accepted is not None
    scopes = accepted.agent_revision.skill_scopes
    tool_plane = accepted.tool_plane_revision
    assert scopes is not None
    assert tool_plane is not None
    profile = AcceptedSandboxCapabilityProfileV1.build(
        material_capability=AcceptedMaterialCapability.IMMUTABLE_READ_ONLY,
        atomic_provider_ownership_fencing=True,
        atomic_provider_operation_fencing=False,
        authoritative_shared_expiry=True,
        resolved_immutable_image=True,
        restricted_non_root_isolation=True,
        recoverable_resource_lookup=True,
        durable_one_replica=True,
        exact_two=False,
    )
    values = {
        "accepted_invocation_ref": accepted_scope_reference(
            _TENANT,
            kind="invocation",
            value=f"{_RUN}:{accepted.runtime_identity_digest}",
        ),
        "skill_scope_digest": scopes.digest,
        "tool_plane_base_revision_digest": tool_plane["base_revision_digest"],
        "tool_plane_user_overlay_digest": tool_plane["user_overlay_digest"],
        "tool_plane_projection_digest": tool_plane["projection_digest"],
        "tool_plane_effective_digest": tool_plane["effective_digest"],
    }
    if drift_field is not None:
        values[drift_field] = "invocation-wrong" if drift_field == "accepted_invocation_ref" else "0" * 64
    request = AcceptedMaterialRequestV2.build(
        run_id=_RUN,
        attempt_id=accepted_scope_reference(
            _TENANT,
            kind="attempt",
            value=f"{_RUN}:3",
        ),
        tenant=_TENANT,
        user_ref=accepted_scope_reference(_TENANT, kind="user", value=_OWNER),
        thread_ref=accepted_scope_reference(
            _TENANT,
            kind="thread",
            value=_THREAD,
        ),
        agent_revision_digest=accepted.agent_revision.digest,
        skill_snapshot_digest="9" * 64,
        skill_scope_digest=values["skill_scope_digest"],
        file_manifest=(),
        runtime_image_digest="a" * 64,
        lease_expires_at=datetime(2030, 1, 1, tzinfo=UTC),
        accepted_invocation_ref=values["accepted_invocation_ref"],
        accepted_invocation_digest=accepted.runtime_identity_digest,
        tool_plane_base_revision_digest=values["tool_plane_base_revision_digest"],
        tool_plane_user_overlay_digest=values["tool_plane_user_overlay_digest"],
        tool_plane_projection_digest=values["tool_plane_projection_digest"],
        tool_plane_effective_digest=values["tool_plane_effective_digest"],
        batch_child_attempt_ref=None,
        capability_profile_digest=profile.digest,
    )
    lease = AcceptedMaterialLeaseV1(
        version=1,
        provider_kind="aio_kubernetes",
        provider_instance_ref="private-sandbox-resource",
        ownership_epoch=3,
        lease_expires_at=request.lease_expires_at,
        opaque_renewal_handle=object(),
    )
    qualification = AcceptedSandboxQualificationV1.build(
        capability_profile_digest=profile.digest,
        qualification_scope="test_only",
        artifact_digest="b" * 64,
        topology_digest="c" * 64,
        verified_at=datetime(2026, 9, 4, tzinfo=UTC),
        expires_at=datetime(2030, 1, 1, tzinfo=UTC),
    )
    return AcceptedExecutionEvidenceV2.build(
        request=request,
        lease=lease,
        materialization_digest="d" * 64,
        verifier_image_digest="e" * 64,
        verifier_contract_version="rwx_verified_copy_v2",
        read_only_proof_digest="f" * 64,
        qualification=qualification,
        isolation=AcceptedSandboxIsolationFactsV1.build(
            restricted_non_root=True,
            read_only_accepted_material=True,
            privilege_escalation_disabled=True,
            runtime_class_digest=None,
            network_policy_digest="1" * 64,
        ),
    )


def _with_sandbox(row: dict, evidence: AcceptedExecutionEvidenceV2) -> dict:
    row["execution_evidence_json"] = evidence.to_persisted()
    row["execution_evidence_digest"] = evidence.digest
    return row


def _receipt_events(
    tool_name: str,
    *,
    phase: str,
    row: dict,
    seq: int = 1,
    capability_kind: str | None = None,
) -> tuple[list[dict], str]:
    accepted = AcceptedInvocation.from_persisted(row)
    assert accepted is not None
    assembly = AssemblyEvidenceV1.from_persisted_json(row["assembly_evidence_json"])
    catalog = accepted.agent_revision.subagent_catalog
    assert catalog is not None
    context = ToolAttemptContextV1(
        run_id=_RUN,
        execution_task_id=_RUN,
        execution_kind="lead",
        subagent_name=None,
        tool_call_id=f"call-{tool_name}",
        attempt=1,
        owner_id="worker-evidence",
        lease_epoch=3,
        agent_revision_digest=accepted.agent_revision.digest,
        assembly_fingerprint=assembly.fingerprint,
        extension_generation=accepted.extension_generation,
        subagent_catalog_digest=catalog.digest,
        subagent_definition_digest=None,
        capability_manifest_digest=accepted.extension_manifest_digest,
        artifact_manifest_digest=accepted.extension_artifact_manifest_digest,
        extension_configuration_digest=accepted.extension_configuration_digest,
        tenant=_TENANT,
    )
    started = DurableToolReceiptV1.started(
        context=context,
        tool_name=tool_name,
        request_projection_digest="9" * 64,
        occurred_at=datetime(2026, 9, 4, 12, 0, 10, tzinfo=UTC),
    )
    succeeded = phase == "succeeded"
    outcome = started.outcome(
        phase=phase,  # type: ignore[arg-type]
        result_projection_digest="a" * 64 if succeeded else None,
        result_kind="tool_message" if succeeded else None,
        safe_error_code=None if succeeded else "internal_error",
        occurred_at=datetime(2026, 9, 4, 12, 0, 11, tzinfo=UTC),
    )

    def event(receipt: DurableToolReceiptV1, event_seq: int) -> dict:
        return {
            "thread_id": _THREAD,
            "run_id": _RUN,
            "seq": event_seq,
            "event_type": ("tool_receipt.started.v1" if receipt.phase == "started" else "tool_receipt.outcome.v1"),
            "idempotency_key": receipt.idempotency_key,
            "category": "tool",
            "content": receipt.to_event_body(),
            "metadata": receipt_event_metadata(
                receipt,
                writer_owner_id="worker-evidence",
                writer_lease_epoch=3,
                include_capability_marker=receipt.phase == "started",
                capability_kind=(capability_kind if receipt.phase == "started" else None),
            ),
            "created_at": receipt.occurred_at.isoformat(),
        }

    return [event(started, seq), event(outcome, seq + 1)], started.receipt_id


def _retrieval_event(
    receipt_events: list[dict],
    *,
    row: dict,
    accepted_execution_evidence_ref: str | None = None,
    accepted_sandbox_operation_ref: str | None = None,
    mcp_evidence_ref: str | None = None,
    seq: int = 3,
) -> dict:
    outcome_event = receipt_events[1]
    outcome = DurableToolReceiptV1.from_event_body(
        outcome_event["content"],
        occurred_at=datetime.fromisoformat(outcome_event["created_at"]),
    )
    accepted = AcceptedInvocation.from_persisted(row)
    assert accepted is not None
    tool_plane = accepted.tool_plane_revision
    assert tool_plane is not None
    draft = RetrievalObservationDraftV1(
        tenant_ref=_TENANT.public_ref,
        tenant_digest=_TENANT.digest,
        run_id=_RUN,
        receipt_id=outcome.receipt_id,
        attempt=outcome.context.attempt,
        provider_id="test_provider",
        tool_kind="external_lookup",
        adapter_capability_version="test-v1",
        policy_digest="2" * 64,
        safe_constraints={
            "version": 1,
            "provider_id": "test_provider",
            "collection_public_refs": [],
            "domain_scope": "provider_default",
            "recency_days": None,
            "max_results": 2,
            "max_item_bytes": 1024,
            "max_aggregate_bytes": 4096,
            "timeout_ms": 2000,
            "allow_redirects": False,
            "accept_partial": False,
            "source_schemes": ["https"],
            "policy_digest": "2" * 64,
        },
        started_at=datetime(2026, 9, 4, 12, 0, 10, tzinfo=UTC),
        provider_finished_at=outcome.occurred_at,
        provider_status="success",
        safe_reason=None,
        result_count=1,
        source_count=1,
        source_references=("https://example.com",),
        truncated=False,
        partial=False,
        safe_provider_request_ref="request-safe-ref",
        tool_plane_base_revision_digest=tool_plane["base_revision_digest"],
        tool_plane_user_overlay_digest=tool_plane["user_overlay_digest"],
        tool_plane_projection_digest=tool_plane["projection_digest"],
        tool_plane_effective_digest=tool_plane["effective_digest"],
        accepted_execution_evidence_ref=accepted_execution_evidence_ref,
        accepted_sandbox_operation_ref=accepted_sandbox_operation_ref,
        mcp_evidence_ref=mcp_evidence_ref,
    )
    observation = RetrievalObservationV1.finalize(outcome, draft)
    return {
        "thread_id": _THREAD,
        "run_id": _RUN,
        "seq": seq,
        "event_type": "retrieval.observation.v1",
        "idempotency_key": observation.idempotency_key,
        "category": "tool",
        "content": observation.to_event_body(),
        "metadata": {},
        "created_at": observation.terminal_at.isoformat(),
    }


class _RunStore:
    def __init__(self, row: dict) -> None:
        self.row = row

    async def get(self, run_id: str, *, user_id: str | None = None):
        if run_id != _RUN or user_id != _OWNER:
            return None
        return deepcopy(self.row)


class _EventStore:
    def __init__(self, events: list[dict]) -> None:
        self.events = events

    async def list_events(
        self,
        thread_id,
        run_id,
        *,
        event_types=None,
        limit=500,
        after_seq=None,
        user_id=None,
        **_kwargs,
    ):
        rows = self.events
        if event_types is not None:
            rows = [row for row in rows if row["event_type"] in event_types]
        if after_seq is not None:
            rows = [row for row in rows if row["seq"] > after_seq]
        return deepcopy(rows[:limit])


class _McpTaskRepo:
    def __init__(
        self,
        receipt_id: str | None,
        *,
        row: dict | None = None,
        tool_name: str = "submit_report",
        commitment_version: object = 1,
        commitment_state: str = "present",
        anchor_overrides: dict[str, object] | None = None,
    ) -> None:
        self.receipt_id = receipt_id
        self.row = row
        self.tool_name = tool_name
        self.commitment_version = commitment_version
        self.commitment_state = commitment_state
        self.anchor_overrides = anchor_overrides or {}

    async def list_by_parent_run(self, *_args, **_kwargs):
        assert _kwargs.get("include_evidence_anchors") is True
        items = []
        if self.receipt_id is not None:
            assert self.row is not None
            accepted = AcceptedInvocation.from_persisted(self.row)
            assert accepted is not None
            assembly = AssemblyEvidenceV1.from_persisted_json(self.row["assembly_evidence_json"])
            catalog = accepted.agent_revision.subagent_catalog
            assert catalog is not None
            anchors = {
                "lineage_version": 2,
                "lineage_kind": "agent_tool",
                "tenant_ref": _TENANT.public_ref,
                "tenant_digest": _TENANT.digest,
                "parent_run_id": _RUN,
                "parent_execution_task_id": _RUN,
                "parent_execution_kind": "lead",
                "parent_subagent_name": None,
                "agent_revision_digest": accepted.agent_revision.digest,
                "assembly_fingerprint": assembly.fingerprint,
                "subagent_catalog_digest": catalog.digest,
                "subagent_definition_digest": None,
                "extension_generation": accepted.extension_generation,
                "extension_manifest_digest": accepted.extension_manifest_digest,
                "accepted_origin_digest": accepted.base_origin_digest,
                "artifact_manifest_digest": (accepted.extension_artifact_manifest_digest),
                "extension_configuration_digest": (accepted.extension_configuration_digest),
                **self.anchor_overrides,
            }
            items.append(
                {
                    "lineage_digest": "b" * 64,
                    "submitting_task_id": _RUN,
                    "receipt_id": self.receipt_id,
                    "server_name": "reports",
                    "tool_name": self.tool_name,
                    "status": "completed",
                    "safe_terminal_code": None,
                    "completed_at": "2026-09-04T12:00:30+00:00",
                    "request_commitment_version": self.commitment_version,
                    "request_commitment_state": self.commitment_state,
                    "evidence_anchors": anchors,
                }
            )
        return {
            "items": items,
            "next_cursor": None,
            "pruning_status": "not_pruned",
        }


def _request() -> EvidenceSnapshotRequest:
    return EvidenceSnapshotRequest(
        tenant=_TENANT,
        thread_id=_THREAD,
        run_id=_RUN,
        owner_id=_OWNER,
    )


@pytest.mark.asyncio
async def test_gateway_snapshot_missing_or_mismatched_run_is_generic_not_found() -> None:
    reader = GatewayRunEvidenceSnapshotReader(
        run_store=_RunStore(_row()),
        event_store=_EventStore(_events()),
    )
    request = EvidenceSnapshotRequest(
        tenant=_TENANT,
        thread_id=_THREAD,
        run_id="guessed-run",
        owner_id=_OWNER,
    )

    with pytest.raises(RunEvidenceBundleError, match="run_not_found") as raised:
        await reader.read(request)

    assert raised.value.code == "run_not_found"


@pytest.mark.asyncio
async def test_gateway_snapshot_aggregates_current_safe_evidence_only() -> None:
    row = _row()
    event_store = _EventStore(_events())
    reader = GatewayRunEvidenceSnapshotReader(
        run_store=_RunStore(row),
        event_store=event_store,
    )

    snapshot = await RunEvidenceSnapshotService(reader).build(_request())

    manifest = snapshot.to_manifest(())
    rendered = manifest.canonical_bytes()
    assert snapshot.artifact_paths == ("/mnt/user-data/outputs/report.txt",)
    assert manifest.admission["accepted_invocation_digest"]
    assert manifest.admission["credential_evidence_digest"]
    assert b"owner-1" not in rendered
    assert b"lead_agent" not in rendered
    assert b"report.txt" not in rendered
    assert b"private-material-soul" not in rendered


@pytest.mark.asyncio
async def test_gateway_snapshot_revalidation_detects_new_event() -> None:
    event_store = _EventStore(_events())
    reader = GatewayRunEvidenceSnapshotReader(
        run_store=_RunStore(_row()),
        event_store=event_store,
    )
    source = await reader.read(_request())
    event_store.events.append(
        {
            **_events()[-1],
            "seq": 12,
            "event_type": "late.event",
        }
    )

    assert await reader.revalidate(_request(), source) is False


@pytest.mark.asyncio
async def test_gateway_snapshot_revalidation_detects_rehashed_evidence_rewrite() -> None:
    run_store = _RunStore(_row())
    reader = GatewayRunEvidenceSnapshotReader(
        run_store=run_store,
        event_store=_EventStore(_events()),
    )
    source = await reader.read(_request())
    assembly = dict(run_store.row["assembly_evidence_json"])
    assembly["accepted_capability_manifest_digest"] = "0" * 64
    changed = AssemblyEvidenceV1.from_persisted_json(assembly)
    run_store.row["assembly_evidence_json"] = changed.to_persisted_json()
    run_store.row["assembly_evidence_digest"] = assembly_evidence_digest(changed)

    assert await reader.revalidate(_request(), source) is False


@pytest.mark.asyncio
async def test_gateway_snapshot_rejects_cross_linked_assembly() -> None:
    row = _row()
    row["assembly_evidence_digest"] = "0" * 64
    reader = GatewayRunEvidenceSnapshotReader(
        run_store=_RunStore(row),
        event_store=_EventStore(_events()),
    )

    with pytest.raises(RunEvidenceBundleError, match="evidence_cross_link_invalid"):
        await reader.read(_request())


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field",
    [
        "accepted_capability_manifest_digest",
        "accepted_artifact_manifest_digest",
        "accepted_extension_configuration_digest",
    ],
)
async def test_gateway_snapshot_rejects_rehashed_assembly_anchor_drift(
    field: str,
) -> None:
    row = _row()
    assembly = dict(row["assembly_evidence_json"])
    assembly[field] = "sha256:" + "0" * 64 if field != "accepted_capability_manifest_digest" else "0" * 64
    changed = AssemblyEvidenceV1.from_persisted_json(assembly)
    row["assembly_evidence_json"] = changed.to_persisted_json()
    row["assembly_evidence_digest"] = assembly_evidence_digest(changed)
    reader = GatewayRunEvidenceSnapshotReader(
        run_store=_RunStore(row),
        event_store=_EventStore(_events()),
    )

    with pytest.raises(RunEvidenceBundleError, match="evidence_cross_link_invalid"):
        await reader.read(_request())


@pytest.mark.asyncio
async def test_gateway_snapshot_rejects_event_store_scope_drift() -> None:
    events = _events()
    events[0]["thread_id"] = "other-thread"
    reader = GatewayRunEvidenceSnapshotReader(
        run_store=_RunStore(_row()),
        event_store=_EventStore(events),
    )

    with pytest.raises(RunEvidenceBundleError, match="evidence_cross_link_invalid"):
        await reader.read(_request())


@pytest.mark.asyncio
@pytest.mark.parametrize("tool_name", ["batch_task", "submit_report"])
async def test_gateway_snapshot_records_explicit_terminal_submission_failures(
    tool_name: str,
) -> None:
    row = _row(mcp_submit_tool="submit_report" if tool_name == "submit_report" else None)
    receipt_events, _receipt_id = _receipt_events(
        tool_name,
        phase="failed",
        row=row,
    )
    reader = GatewayRunEvidenceSnapshotReader(
        run_store=_RunStore(row),
        event_store=_EventStore([*receipt_events, *_events()]),
        mcp_task_repo=(_McpTaskRepo(None) if tool_name == "submit_report" else None),
    )

    snapshot = await RunEvidenceSnapshotService(reader).build(_request())

    section_name = "mcp_tasks" if tool_name == "submit_report" else "subagent_batches"
    section = next(item for item in snapshot.sections if item.name == section_name)
    assert section.state == "complete"
    assert section.item_count == 1
    assert any(link.subject_section == section_name for link in snapshot.links)


@pytest.mark.asyncio
@pytest.mark.parametrize("tool_name", ["batch_task", "submit_report", "web_search"])
async def test_gateway_snapshot_fails_closed_when_required_attempt_evidence_is_missing(
    tool_name: str,
) -> None:
    row = _row(mcp_submit_tool="submit_report" if tool_name == "submit_report" else None)
    receipt_events, _receipt_id = _receipt_events(
        tool_name,
        phase="succeeded",
        row=row,
        capability_kind="retrieval" if tool_name == "web_search" else None,
    )
    reader = GatewayRunEvidenceSnapshotReader(
        run_store=_RunStore(row),
        event_store=_EventStore([*receipt_events, *_events()]),
    )

    with pytest.raises(RunEvidenceBundleError, match="evidence_incomplete"):
        await reader.read(_request())


@pytest.mark.asyncio
async def test_gateway_snapshot_derives_retrieval_from_persisted_capability_not_name() -> None:
    row = _row()
    ordinary_events, _ordinary_receipt = _receipt_events(
        "web_search",
        phase="succeeded",
        row=row,
    )
    ordinary = GatewayRunEvidenceSnapshotReader(
        run_store=_RunStore(row),
        event_store=_EventStore([*ordinary_events, *_events()]),
    )

    snapshot = await ordinary.read(_request())
    section = next(item for item in snapshot.sections if item.name == "retrieval_observations")
    assert section.state == "absent_by_design"

    declared_events, _declared_receipt = _receipt_events(
        "lookup_external_sources",
        phase="succeeded",
        row=row,
        capability_kind="retrieval",
    )
    declared = GatewayRunEvidenceSnapshotReader(
        run_store=_RunStore(row),
        event_store=_EventStore([*declared_events, *_events()]),
    )
    with pytest.raises(RunEvidenceBundleError, match="evidence_incomplete"):
        await declared.read(_request())


@pytest.mark.asyncio
async def test_gateway_snapshot_accepts_complete_declared_retrieval() -> None:
    row = _row()
    receipt_events, _receipt_id = _receipt_events(
        "lookup_external_sources",
        phase="succeeded",
        row=row,
        capability_kind="retrieval",
    )
    observation = _retrieval_event(receipt_events, row=row)
    reader = GatewayRunEvidenceSnapshotReader(
        run_store=_RunStore(row),
        event_store=_EventStore(
            [*receipt_events, observation, *_events()],
        ),
    )

    snapshot = await reader.read(_request())

    section = next(item for item in snapshot.sections if item.name == "retrieval_observations")
    assert section.state == "complete"
    assert section.item_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "drift_field",
    [
        "accepted_invocation_ref",
        "skill_scope_digest",
        "tool_plane_base_revision_digest",
        "tool_plane_user_overlay_digest",
        "tool_plane_projection_digest",
    ],
)
async def test_gateway_snapshot_rejects_rehashed_sandbox_anchor_drift(
    drift_field: str,
) -> None:
    row = _row()
    _with_sandbox(row, _sandbox_evidence(row, drift_field=drift_field))
    reader = GatewayRunEvidenceSnapshotReader(
        run_store=_RunStore(row),
        event_store=_EventStore(_events()),
    )

    with pytest.raises(RunEvidenceBundleError, match="evidence_cross_link_invalid"):
        await reader.read(_request())


@pytest.mark.asyncio
async def test_gateway_snapshot_validates_retrieval_sandbox_references() -> None:
    row = _row()
    sandbox = _sandbox_evidence(row)
    _with_sandbox(row, sandbox)
    receipt_events, _receipt_id = _receipt_events(
        "lookup_external_sources",
        phase="succeeded",
        row=row,
        capability_kind="retrieval",
    )
    evidence_ref = accepted_execution_evidence_reference(sandbox)
    valid = _retrieval_event(
        receipt_events,
        row=row,
        accepted_execution_evidence_ref=evidence_ref,
        accepted_sandbox_operation_ref="accepted-operation-" + "a" * 32,
    )
    reader = GatewayRunEvidenceSnapshotReader(
        run_store=_RunStore(row),
        event_store=_EventStore([*receipt_events, valid, *_events()]),
    )
    assert (await reader.read(_request())).terminal_status == "success"

    mismatched = _retrieval_event(
        receipt_events,
        row=row,
        accepted_execution_evidence_ref="accepted-execution-" + "0" * 64,
    )
    invalid_reader = GatewayRunEvidenceSnapshotReader(
        run_store=_RunStore(row),
        event_store=_EventStore([*receipt_events, mismatched, *_events()]),
    )
    with pytest.raises(RunEvidenceBundleError, match="evidence_cross_link_invalid"):
        await invalid_reader.read(_request())


@pytest.mark.asyncio
async def test_gateway_snapshot_validates_retrieval_mcp_reference() -> None:
    row = _row(mcp_submit_tool="lookup_external_sources")
    receipt_events, receipt_id = _receipt_events(
        "lookup_external_sources",
        phase="succeeded",
        row=row,
        capability_kind="retrieval",
    )
    valid = _retrieval_event(
        receipt_events,
        row=row,
        mcp_evidence_ref="b" * 64,
    )
    reader = GatewayRunEvidenceSnapshotReader(
        run_store=_RunStore(row),
        event_store=_EventStore([*receipt_events, valid, *_events()]),
        mcp_task_repo=_McpTaskRepo(
            receipt_id,
            row=row,
            tool_name="lookup_external_sources",
        ),
    )
    assert (await reader.read(_request())).terminal_status == "success"

    mismatched = _retrieval_event(
        receipt_events,
        row=row,
        mcp_evidence_ref="c" * 64,
    )
    invalid_reader = GatewayRunEvidenceSnapshotReader(
        run_store=_RunStore(row),
        event_store=_EventStore([*receipt_events, mismatched, *_events()]),
        mcp_task_repo=_McpTaskRepo(
            receipt_id,
            row=row,
            tool_name="lookup_external_sources",
        ),
    )
    with pytest.raises(RunEvidenceBundleError, match="evidence_cross_link_invalid"):
        await invalid_reader.read(_request())


@pytest.mark.asyncio
async def test_gateway_snapshot_commits_mcp_lineage_and_private_commitment_presence() -> None:
    row = _row(mcp_submit_tool="submit_report")
    receipt_events, receipt_id = _receipt_events(
        "submit_report",
        phase="succeeded",
        row=row,
    )
    reader = GatewayRunEvidenceSnapshotReader(
        run_store=_RunStore(row),
        event_store=_EventStore([*receipt_events, *_events()]),
        mcp_task_repo=_McpTaskRepo(receipt_id, row=row),
    )

    snapshot = await RunEvidenceSnapshotService(reader).build(_request())

    section = next(item for item in snapshot.sections if item.name == "mcp_tasks")
    assert section.state == "complete"
    assert section.item_count == 1
    assert any(link.kind == "mcp_task_to_tool_receipt" for link in snapshot.links)

    unavailable = GatewayRunEvidenceSnapshotReader(
        run_store=_RunStore(row),
        event_store=_EventStore([*receipt_events, *_events()]),
        mcp_task_repo=_McpTaskRepo(
            receipt_id,
            row=row,
            commitment_state="legacy_unavailable",
        ),
    )
    with pytest.raises(RunEvidenceBundleError, match="evidence_legacy_unbound"):
        await unavailable.read(_request())

    boolean_version = GatewayRunEvidenceSnapshotReader(
        run_store=_RunStore(row),
        event_store=_EventStore([*receipt_events, *_events()]),
        mcp_task_repo=_McpTaskRepo(
            receipt_id,
            row=row,
            commitment_version=True,
        ),
    )
    with pytest.raises(RunEvidenceBundleError, match="evidence_cross_link_invalid"):
        await boolean_version.read(_request())


@pytest.mark.asyncio
async def test_gateway_snapshot_accepts_default_prefixed_mcp_submission() -> None:
    row = _row(
        mcp_submit_tool="submit_report",
        mcp_tool_name_prefix=True,
    )
    receipt_events, receipt_id = _receipt_events(
        "reports_submit_report",
        phase="succeeded",
        row=row,
    )
    reader = GatewayRunEvidenceSnapshotReader(
        run_store=_RunStore(row),
        event_store=_EventStore([*receipt_events, *_events()]),
        mcp_task_repo=_McpTaskRepo(receipt_id, row=row),
    )

    snapshot = await reader.read(_request())

    section = next(item for item in snapshot.sections if item.name == "mcp_tasks")
    assert section.state == "complete"
    assert section.item_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("lineage_version", 1),
        ("lineage_kind", "standalone_api"),
        ("tenant_ref", "tenant-" + "0" * 16),
        ("tenant_digest", "0" * 64),
        ("parent_run_id", "other-run"),
        ("parent_execution_task_id", "other-task"),
        ("parent_execution_kind", "subagent"),
        ("parent_subagent_name", "other-agent"),
        ("agent_revision_digest", "0" * 64),
        ("assembly_fingerprint", "0" * 64),
        ("subagent_catalog_digest", "0" * 64),
        ("subagent_definition_digest", "0" * 64),
        ("extension_generation", 99),
        ("extension_manifest_digest", "0" * 64),
        ("accepted_origin_digest", "0" * 64),
        ("artifact_manifest_digest", "sha256:" + "0" * 64),
        ("extension_configuration_digest", "sha256:" + "0" * 64),
    ],
)
async def test_gateway_snapshot_rejects_mcp_lineage_anchor_drift(
    field: str,
    value: object,
) -> None:
    row = _row(mcp_submit_tool="submit_report")
    receipt_events, receipt_id = _receipt_events(
        "submit_report",
        phase="succeeded",
        row=row,
    )
    reader = GatewayRunEvidenceSnapshotReader(
        run_store=_RunStore(row),
        event_store=_EventStore([*receipt_events, *_events()]),
        mcp_task_repo=_McpTaskRepo(
            receipt_id,
            row=row,
            anchor_overrides={field: value},
        ),
    )

    with pytest.raises(RunEvidenceBundleError, match="evidence_cross_link_invalid"):
        await reader.read(_request())


@pytest.mark.asyncio
async def test_gateway_snapshot_derives_empty_mcp_section_from_accepted_capability() -> None:
    row = _row(mcp_submit_tool="submit_report")
    reader = GatewayRunEvidenceSnapshotReader(
        run_store=_RunStore(row),
        event_store=_EventStore(_events()),
        mcp_task_repo=_McpTaskRepo(None),
    )

    snapshot = await RunEvidenceSnapshotService(reader).build(_request())

    section = next(item for item in snapshot.sections if item.name == "mcp_tasks")
    assert section.state == "complete"
    assert section.required is True
    assert section.item_count == 0
