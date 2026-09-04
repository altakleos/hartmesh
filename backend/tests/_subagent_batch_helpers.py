from __future__ import annotations

from deerflow.runtime.accepted_invocation import (
    AcceptedInvocation,
    InvocationOrigin,
    PrincipalProjection,
    ResolvedAgentMaterialV1,
    ResolvedAgentRevision,
    canonical_digest,
)
from deerflow.runtime.subagent_snapshot import (
    ResolvedSkillScopesV1,
    ResolvedSubagentCatalogV1,
    resolved_subagent_definition,
)
from deerflow.runtime.tenant_identity import TenantIdentityV1
from deerflow.runtime.tool_evidence import (
    DurableToolReceiptV1,
    NullDurableToolReceiptSink,
    ToolEvidenceRuntimeBinding,
)
from deerflow.subagents.batch_acceptance import (
    AcceptedBatchItemV1,
    AcceptedBatchV1,
    BatchItemRequestV1,
    BatchLimitsV1,
    ParentBoundBatchExecutionV1,
    ParentBoundBatchRequest,
)


class ActiveDurableToolReceiptSink:
    """Small protocol-complete sink for service-bound unit tests."""

    async def reserve_started(self, **kwargs):
        return await NullDurableToolReceiptSink().reserve_started(**kwargs)

    async def record_started(self, _receipt) -> None:
        return None

    async def record_outcome(self, _receipt) -> None:
        return None


def make_parent_batch_request(
    *,
    app_config=None,
    items: tuple[BatchItemRequestV1, ...] | None = None,
    tool_call_id: str = "call-1",
    definition=None,
    skill_snapshot=None,
    skill_scope_digests: tuple[str, ...] = (),
    extension_generation: int = 0,
    capability_manifest_digest: str | None = None,
    artifact_manifest_digest: str | None = None,
    extension_configuration_digest: str | None = None,
    extensions=None,
    tool_plane_revision=None,
) -> ParentBoundBatchRequest:
    tenant = TenantIdentityV1.from_canonical_id("tenant-a").to_persisted_reference()
    if definition is None:
        definition = resolved_subagent_definition(
            name="general-purpose",
            source_kind="builtin",
            source_version="v1",
            description="General purpose",
            system_prompt="Work carefully.",
            model=None,
            model_settings={},
            tool_names=(),
            skill_names=(),
            max_turns=20,
            timeout_seconds=300,
        )
    catalog = ResolvedSubagentCatalogV1.from_entries(
        (definition,),
        allowed_names=(definition.name,),
    )
    scopes = ResolvedSkillScopesV1.from_scopes(
        {
            "lead": (),
            f"subagent:{definition.name}": skill_scope_digests,
        }
    )
    material = ResolvedAgentMaterialV1(
        agent_id="default",
        storage_source="builtin",
        storage_version="v1",
        agent_config=None,
        soul="lead",
        model_profile={"name": "model-a"},
        tools=(),
        runtime_defaults={"subagent_enabled": True},
        subagent_catalog=catalog,
        skill_scopes=scopes,
        user_id="user-1",
        app_config=app_config,
        skill_snapshot=skill_snapshot,
    )
    accepted = AcceptedInvocation.seal(
        principal=PrincipalProjection(user_id="user-1", role="member"),
        origin=InvocationOrigin(source_kind="http"),
        thread_id="thread-1",
        context_references={"subagent_enabled": True},
        agent_revision=ResolvedAgentRevision.from_material(material),
        normalized_input={"messages": []},
        execution_options={"multitask_strategy": "reject"},
        extension_generation=extension_generation,
        extension_manifest_digest=capability_manifest_digest,
        extension_artifact_manifest_digest=artifact_manifest_digest,
        extension_configuration_digest=extension_configuration_digest,
        tool_plane_revision=tool_plane_revision,
        contributor_execution_digest=canonical_digest({"version": 1, "execution": []}),
        tenant=tenant,
    )
    binding = ToolEvidenceRuntimeBinding(
        run_id="run-1",
        execution_task_id="run-1",
        execution_kind="lead",
        subagent_name=None,
        owner_id="worker-1",
        lease_epoch=4,
        agent_revision_digest=accepted.agent_revision.digest,
        assembly_fingerprint="a" * 64,
        extension_generation=extension_generation,
        capability_manifest_digest=capability_manifest_digest,
        artifact_manifest_digest=artifact_manifest_digest,
        extension_configuration_digest=extension_configuration_digest,
        subagent_catalog_digest=catalog.digest,
        subagent_definition_digest=None,
        tenant=tenant,
    )
    receipt = DurableToolReceiptV1.started(
        context=binding.make_attempt(tool_call_id, 1),
        tool_name="batch_task",
        request_projection_digest="b" * 64,
    )
    return ParentBoundBatchRequest(
        tenant=tenant,
        accepted_parent=accepted,
        resolved_parent_material=material,
        parent_tool_binding=binding,
        parent_tool_receipt=receipt,
        parent_tool_receipt_sink=ActiveDurableToolReceiptSink(),
        user_id="user-1",
        thread_id="thread-1",
        run_id="run-1",
        submission_key=f"{receipt.receipt_id}:{tool_call_id}",
        title="Records",
        subagent_name=definition.name,
        items=items or (BatchItemRequestV1(key="record-1", prompt="Process record 1"),),
        limits=BatchLimitsV1(
            max_live_items=20,
            max_running_items=10,
            max_attempts=3,
            max_attempt_records_per_item=64,
            max_result_chars=100_000,
            max_total_runtime_seconds=86_400,
        ),
        app_config=app_config,
        extensions=extensions,
    )


def make_claimed_item(request: ParentBoundBatchRequest) -> dict:
    accepted = AcceptedBatchV1.from_parent_request(request, batch_id="batch-1")
    immutable = AcceptedBatchItemV1.from_request(
        request.items[0],
        batch_id=accepted.batch_id,
        ordinal=0,
    )
    execution = ParentBoundBatchExecutionV1.from_parent_request(
        request,
        accepted=accepted,
    )
    return {
        "id": immutable.item_id,
        "item_key": request.items[0].key,
        "position": 0,
        "prompt": request.items[0].prompt,
        "request_digest": immutable.request_digest,
        "attempt_id": "ba_attempt-one",
        "lease_epoch": 1,
        "batch": {
            "id": accepted.batch_id,
            "thread_id": accepted.parent_thread_id,
            "user_id": request.user_id,
            "run_id": accepted.parent_run_id,
            "acceptance": accepted,
            "execution": execution,
        },
    }
