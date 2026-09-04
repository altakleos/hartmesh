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
from deerflow.tool_plane import EffectiveToolPlaneRevisionV1

_TENANT = TenantIdentityV1.from_canonical_id("local").to_persisted_reference()
_OWNER = "owner-1"
_THREAD = "thread-evidence"
_RUN = "run-evidence"


def _accepted(*, mcp_submit_tool: str | None = None) -> AcceptedInvocation:
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
                "tool_name_prefix": True,
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


def _row(*, mcp_submit_tool: str | None = None) -> dict:
    accepted = _accepted(mcp_submit_tool=mcp_submit_tool)
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


def _receipt_events(
    tool_name: str,
    *,
    phase: str,
    row: dict,
    seq: int = 1,
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
            ),
            "created_at": receipt.occurred_at.isoformat(),
        }

    return [event(started, seq), event(outcome, seq + 1)], started.receipt_id


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
        commitment_version: object = 1,
        commitment_state: str = "present",
    ) -> None:
        self.receipt_id = receipt_id
        self.commitment_version = commitment_version
        self.commitment_state = commitment_state

    async def list_by_parent_run(self, *_args, **_kwargs):
        items = []
        if self.receipt_id is not None:
            items.append(
                {
                    "lineage_digest": "b" * 64,
                    "receipt_id": self.receipt_id,
                    "status": "completed",
                    "safe_terminal_code": None,
                    "completed_at": "2026-09-04T12:00:30+00:00",
                    "request_commitment_version": self.commitment_version,
                    "request_commitment_state": self.commitment_state,
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
    )
    reader = GatewayRunEvidenceSnapshotReader(
        run_store=_RunStore(row),
        event_store=_EventStore([*receipt_events, *_events()]),
    )

    with pytest.raises(RunEvidenceBundleError, match="evidence_incomplete"):
        await reader.read(_request())


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
        mcp_task_repo=_McpTaskRepo(receipt_id),
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
            commitment_state="legacy_unavailable",
        ),
    )
    with pytest.raises(RunEvidenceBundleError, match="evidence_cross_link_invalid"):
        await unavailable.read(_request())

    boolean_version = GatewayRunEvidenceSnapshotReader(
        run_store=_RunStore(row),
        event_store=_EventStore([*receipt_events, *_events()]),
        mcp_task_repo=_McpTaskRepo(
            receipt_id,
            commitment_version=True,
        ),
    )
    with pytest.raises(RunEvidenceBundleError, match="evidence_cross_link_invalid"):
        await boolean_version.read(_request())


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
