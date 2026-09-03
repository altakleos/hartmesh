from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
import pytest_asyncio
from deerflow_extension_api import ConstraintProjectionV1
from sqlalchemy import update

from deerflow.config.database_config import DatabaseConfig
from deerflow.persistence.engine import (
    close_engine,
    get_session_factory,
    init_engine_from_config,
)
from deerflow.persistence.subagent_batches import (
    SubagentBatchAttemptRow,
    SubagentBatchItemRow,
    SubagentBatchRepository,
    SubagentBatchRow,
)
from deerflow.runtime.accepted_invocation import (
    AcceptedInvocation,
    InvocationOrigin,
    PrincipalProjection,
    ResolvedAgentMaterialV1,
    ResolvedAgentRevision,
    canonical_digest,
)
from deerflow.runtime.skill_projection import SkillProjectionConsumerToken
from deerflow.runtime.subagent_snapshot import (
    ResolvedSkillScopesV1,
    ResolvedSubagentCatalogV1,
    resolved_subagent_definition,
    resolved_tool_contract_digest,
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
    BatchAdmissionConflict,
    BatchAdmissionError,
    BatchAttemptEvidenceV1,
    BatchItemRequestV1,
    BatchLimitsV1,
    ParentBoundBatchExecutionV1,
    ParentBoundBatchRequest,
)


@pytest_asyncio.fixture(autouse=True)
async def _close_test_engine() -> None:
    yield
    await close_engine()


def _parent_request(
    *,
    prompt: str = "Process record one",
    tenant_id: str = "tenant-a",
    model_profile: dict[str, object] | None = None,
) -> ParentBoundBatchRequest:
    tenant = TenantIdentityV1.from_canonical_id(tenant_id).to_persisted_reference()
    accepted_tools = (
        SimpleNamespace(name="bash"),
        SimpleNamespace(name="read_file"),
    )
    definition = resolved_subagent_definition(
        name="general-purpose",
        source_kind="builtin",
        source_version="v1",
        description="General purpose",
        system_prompt="Work carefully.",
        model=None,
        model_settings={},
        tool_names=("bash", "read_file"),
        tool_contract_digests=tuple(resolved_tool_contract_digest(tool) for tool in accepted_tools),
        skill_names=(),
        max_turns=20,
        timeout_seconds=300,
    )
    catalog = ResolvedSubagentCatalogV1.from_entries(
        (definition,),
        allowed_names=(definition.name,),
    )
    scopes = ResolvedSkillScopesV1.from_scopes({"lead": (), "subagent:general-purpose": ()})
    material = ResolvedAgentMaterialV1(
        agent_id="default",
        storage_source="builtin",
        storage_version="v1",
        agent_config=None,
        soul="lead",
        model_profile=(model_profile if model_profile is not None else {"name": "model-a", "app_execution_digest": "e" * 64}),
        tool_groups=(),
        tools=("bash", "read_file"),
        skills=(),
        runtime_defaults={"subagent_enabled": True},
        subagent_catalog=catalog,
        skill_scopes=scopes,
        user_id="user-1",
    )
    accepted = AcceptedInvocation.seal(
        principal=PrincipalProjection(user_id="user-1", role="member"),
        origin=InvocationOrigin(source_kind="http"),
        thread_id="thread-1",
        context_references={"subagent_enabled": True},
        agent_revision=ResolvedAgentRevision.from_material(material),
        normalized_input={"messages": []},
        execution_options={"multitask_strategy": "reject"},
        extension_generation=1,
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
        extension_generation=accepted.extension_generation,
        subagent_catalog_digest=catalog.digest,
        subagent_definition_digest=None,
        tenant=tenant,
    )
    receipt = DurableToolReceiptV1.started(
        context=binding.make_attempt("call-1", 1),
        tool_name="batch_task",
        request_projection_digest="b" * 64,
    )
    return ParentBoundBatchRequest(
        tenant=tenant,
        accepted_parent=accepted,
        resolved_parent_material=material,
        parent_tool_binding=binding,
        parent_tool_receipt=receipt,
        parent_tool_receipt_sink=NullDurableToolReceiptSink(),
        user_id="user-1",
        thread_id="thread-1",
        run_id="run-1",
        submission_key="run-1:call-1",
        title="Records",
        subagent_name="general-purpose",
        items=(BatchItemRequestV1(key="record-1", prompt=prompt),),
        limits=BatchLimitsV1(
            max_live_items=10,
            max_running_items=2,
            max_attempts=3,
            max_attempt_records_per_item=64,
            max_result_chars=100_000,
            max_total_runtime_seconds=3600,
        ),
        parent_cancellable=False,
    )


def _with_constraints(
    request: ParentBoundBatchRequest,
    *,
    max_total_subagents: int | None = 4,
) -> ParentBoundBatchRequest:
    now = datetime.now(UTC)
    projection = ConstraintProjectionV1(
        request_digest="c" * 64,
        agent_revision_digest=request.accepted_parent.agent_revision.digest,
        projection_revision="policy-v1",
        issued_at=now - timedelta(seconds=1),
        valid_until=now + timedelta(minutes=10),
        evidence_id="constraint-evidence-v1",
        evidence_digest="d" * 64,
        max_total_subagents=max_total_subagents,
    )
    normalized = {
        "version": 1,
        "request_digest": projection.request_digest,
        "agent_revision_digest": projection.agent_revision_digest,
        "projection_revision": projection.projection_revision,
        "issued_at": projection.issued_at.isoformat(),
        "valid_until": projection.valid_until.isoformat(),
        "evidence_id": projection.evidence_id,
        "evidence_digest": projection.evidence_digest,
        "max_total_subagents": projection.max_total_subagents,
    }
    normalized["projection_digest"] = canonical_digest(normalized)
    evidence = dict(request.accepted_parent.decision_evidence)
    evidence["constraints"] = normalized
    accepted_parent = replace(
        request.accepted_parent,
        decision_evidence=evidence,
    )
    return replace(
        request,
        accepted_parent=accepted_parent,
        invocation_constraints=projection,
    )


def test_accepted_batch_is_canonical_parent_bound_and_prompt_safe() -> None:
    request = _parent_request()

    first = AcceptedBatchV1.from_parent_request(
        request,
        batch_id="subagent-batch-1",
    )
    restored = AcceptedBatchV1.from_persisted_json(first.to_persisted_json())

    assert restored == first
    assert restored.acceptance_digest == first.acceptance_digest
    assert restored.tenant == request.tenant
    assert restored.parent_invocation_digest == request.accepted_parent.runtime_identity_digest
    assert restored.parent_tool_receipt_id == request.parent_tool_receipt.receipt_id
    assert restored.subagent_definition_digest == request.resolved_parent_material.subagent_catalog.get("general-purpose").definition_digest
    assert restored.allowed_tool_contract_digests == request.selected_definition.tool_contract_digests
    serialized = json.dumps(first.to_persisted_json(), sort_keys=True)
    assert "Process record one" not in serialized
    assert "Work carefully" not in serialized


def test_batch_acceptance_digest_changes_with_immutable_item_request() -> None:
    first = AcceptedBatchV1.from_parent_request(
        _parent_request(prompt="Process record one"),
        batch_id="subagent-batch-1",
    )
    changed = AcceptedBatchV1.from_parent_request(
        _parent_request(prompt="Process record two"),
        batch_id="subagent-batch-1",
    )

    assert first.item_root_digest != changed.item_root_digest
    assert first.acceptance_digest != changed.acceptance_digest


def test_execution_rejects_same_named_tool_with_changed_contract() -> None:
    from deerflow.config.subagent_batches_config import SubagentBatchesConfig
    from deerflow.config.subagent_runtime_config import SubagentRuntimeConfig
    from deerflow.subagents.batch_service import SubagentBatchService

    request = _parent_request()
    accepted = AcceptedBatchV1.from_parent_request(
        request,
        batch_id="subagent-batch-tool-contract",
    )
    service = SubagentBatchService(
        repository=SimpleNamespace(),
        config=SubagentBatchesConfig(),
        runtime_config=SubagentRuntimeConfig(max_running=1),
    )
    unchanged = [SimpleNamespace(name="bash"), SimpleNamespace(name="read_file")]

    assert [tool.name for tool in service._validated_tools(accepted, unchanged)] == [
        "bash",
        "read_file",
    ]
    with pytest.raises(BatchAdmissionError, match="provider_not_qualified"):
        service._validated_tools(
            accepted,
            [
                SimpleNamespace(name="bash", description="changed contract"),
                SimpleNamespace(name="read_file"),
            ],
        )


def test_maximum_configured_item_count_keeps_batch_evidence_bounded() -> None:
    request = _parent_request()
    request = replace(
        request,
        items=tuple(BatchItemRequestV1(key=f"record-{index}", prompt="process") for index in range(5_000)),
    )

    accepted = AcceptedBatchV1.from_parent_request(
        request,
        batch_id="subagent-batch-large",
    )

    assert accepted.item_count == 5_000
    assert accepted.evidence_size_bytes <= 16_384
    assert "process" not in json.dumps(accepted.to_persisted_json())


def test_batch_acceptance_rejects_unknown_schema_version() -> None:
    accepted = AcceptedBatchV1.from_parent_request(
        _parent_request(),
        batch_id="subagent-batch-1",
    )
    persisted = accepted.to_persisted_json()
    persisted["version"] = 2

    with pytest.raises(
        BatchAdmissionError,
        match="batch_acceptance_version_unsupported",
    ):
        AcceptedBatchV1.from_persisted_json(persisted)


def test_batch_acceptance_rejects_cross_tenant_parent_context() -> None:
    request = _parent_request()
    other_tenant = TenantIdentityV1.from_canonical_id("tenant-b").to_persisted_reference()

    with pytest.raises(BatchAdmissionError, match="batch_tenant_mismatch"):
        replace(request, tenant=other_tenant)


@pytest.mark.parametrize(
    ("field_name", "changed_value"),
    (
        ("tenant", TenantIdentityV1.from_canonical_id("tenant-b").to_persisted_reference()),
        ("parent_invocation_digest", "f" * 64),
        ("parent_assembly_fingerprint", "f" * 64),
        ("parent_tool_receipt_id", f"tr_{'f' * 64}"),
        ("subagent_catalog_digest", "f" * 64),
        ("skill_scope_digest", "f" * 64),
        ("extension_generation", 2),
        ("model_constraints_digest", "f" * 64),
        ("invocation_constraints_digest", "f" * 64),
    ),
)
def test_each_safe_acceptance_binding_is_digest_protected(
    field_name: str,
    changed_value: object,
) -> None:
    accepted = AcceptedBatchV1.from_parent_request(
        _parent_request(),
        batch_id="subagent-batch-bindings",
    )

    with pytest.raises(BatchAdmissionError):
        replace(accepted, **{field_name: changed_value})


def test_parent_tool_binding_must_match_the_active_receipt_context() -> None:
    request = _parent_request()
    original = request.parent_tool_binding
    changed_binding = ToolEvidenceRuntimeBinding(
        run_id=original.run_id,
        execution_task_id=original.execution_task_id,
        execution_kind=original.execution_kind,
        subagent_name=original.subagent_name,
        owner_id=original.owner_id,
        lease_epoch=original.lease_epoch,
        agent_revision_digest=original.agent_revision_digest,
        assembly_fingerprint="f" * 64,
        extension_generation=original.extension_generation,
        capability_manifest_digest=original.capability_manifest_digest,
        artifact_manifest_digest=original.artifact_manifest_digest,
        extension_configuration_digest=original.extension_configuration_digest,
        subagent_catalog_digest=original.subagent_catalog_digest,
        subagent_definition_digest=original.subagent_definition_digest,
        tenant=original.tenant,
    )

    with pytest.raises(BatchAdmissionError, match="tool_attempt_not_active"):
        replace(request, parent_tool_binding=changed_binding)


def test_skill_projection_token_must_match_the_accepted_parent() -> None:
    request = _parent_request()
    mismatched = SkillProjectionConsumerToken(
        user_id="another-user",
        thread_id=request.thread_id,
        sandbox_id="sandbox-1",
        run_id=request.run_id,
        generation=1,
        consumer_id="lead",
        snapshot_id=None,
    )

    with pytest.raises(
        BatchAdmissionError,
        match="execution_material_unavailable",
    ):
        replace(request, skill_projection_token=mismatched)


def test_parent_cancellation_cascade_is_explicitly_unsupported() -> None:
    with pytest.raises(
        BatchAdmissionError,
        match="batch_parent_cascade_unsupported",
    ):
        replace(_parent_request(), parent_cancellable=True)


def test_attempt_evidence_serializes_only_a_result_digest() -> None:
    evidence = BatchAttemptEvidenceV1.terminal(
        batch_id="batch-1",
        item_id="item-1",
        attempt_id="attempt-1",
        acceptance_digest="a" * 64,
        request_digest="b" * 64,
        attempt_number=1,
        lease_epoch=1,
        terminal_code="succeeded",
        consumed=True,
        result_digest=canonical_digest("private result payload"),
    )

    serialized = json.dumps(evidence.to_persisted_json(), sort_keys=True)
    assert len(serialized.encode()) < 16 * 1024
    assert "private result payload" not in serialized
    assert evidence.result_digest in serialized


def test_batch_acceptance_binds_exact_typed_invocation_constraints() -> None:
    request = _with_constraints(_parent_request())

    accepted = AcceptedBatchV1.from_parent_request(
        request,
        batch_id="subagent-batch-constraints",
    )
    execution = ParentBoundBatchExecutionV1.from_parent_request(
        request,
        accepted=accepted,
    )

    raw_constraints = request.accepted_parent.decision_evidence["constraints"]
    assert accepted.invocation_constraints_digest == raw_constraints["projection_digest"]
    assert execution.constraint_projection == request.invocation_constraints


def test_parent_bound_execution_rechecks_constraint_freshness_before_work() -> None:
    request = _with_constraints(_parent_request())
    accepted = AcceptedBatchV1.from_parent_request(
        request,
        batch_id="subagent-batch-expired-constraint",
    )
    execution = ParentBoundBatchExecutionV1.from_parent_request(
        request,
        accepted=accepted,
    )
    projection = request.invocation_constraints
    assert projection is not None

    with pytest.raises(BatchAdmissionError, match="policy_stopped"):
        execution.validate_constraint_freshness(clock=lambda: projection.valid_until + timedelta(seconds=1))


def test_batch_acceptance_rejects_missing_or_changed_constraint_object() -> None:
    constrained = _with_constraints(_parent_request())

    with pytest.raises(BatchAdmissionError, match="batch_constraint_mismatch"):
        replace(constrained, invocation_constraints=None)

    changed = replace(
        constrained.invocation_constraints,
        evidence_digest="e" * 64,
    )
    with pytest.raises(BatchAdmissionError, match="batch_constraint_mismatch"):
        replace(constrained, invocation_constraints=changed)


def test_batch_acceptance_applies_parent_subagent_limit() -> None:
    request = _parent_request()
    request = replace(
        request,
        items=(
            BatchItemRequestV1(key="one", prompt="first"),
            BatchItemRequestV1(key="two", prompt="second"),
        ),
    )

    with pytest.raises(
        BatchAdmissionError,
        match="batch_constraint_limit_exceeded",
    ):
        _with_constraints(request, max_total_subagents=1)


def test_parent_bound_execution_round_trips_protected_accepted_material() -> None:
    request = _parent_request()
    accepted = AcceptedBatchV1.from_parent_request(
        request,
        batch_id="subagent-batch-1",
    )

    execution = ParentBoundBatchExecutionV1.from_parent_request(
        request,
        accepted=accepted,
    )
    restored = ParentBoundBatchExecutionV1.from_persisted_json(execution.to_persisted_json())

    assert restored == execution
    assert restored.acceptance_digest == accepted.acceptance_digest
    assert restored.selected_definition.definition_digest == accepted.subagent_definition_digest
    assert restored.catalog.digest == accepted.subagent_catalog_digest
    assert "Process record one" not in json.dumps(execution.to_persisted_json())


def test_parent_bound_execution_accepts_nested_frozen_model_material() -> None:
    request = _parent_request(
        model_profile={
            "name": "model-a",
            "app_execution_digest": "e" * 64,
            "parameters": {
                "stop": ["END"],
                "routing": {"tier": "standard"},
            },
        },
    )
    accepted = AcceptedBatchV1.from_parent_request(
        request,
        batch_id="batch-nested-model-material",
    )

    execution = ParentBoundBatchExecutionV1.from_parent_request(
        request,
        accepted=accepted,
    )

    assert ParentBoundBatchExecutionV1.from_persisted_json(execution.to_persisted_json()) == execution


def test_parent_bound_execution_rejects_a_different_item_acceptance() -> None:
    request = _parent_request(prompt="first protected prompt")
    accepted = AcceptedBatchV1.from_parent_request(
        request,
        batch_id="subagent-batch-1",
    )

    with pytest.raises(BatchAdmissionError, match="batch_acceptance_mismatch"):
        ParentBoundBatchExecutionV1.from_parent_request(
            _parent_request(prompt="different protected prompt"),
            accepted=accepted,
        )


@pytest.mark.asyncio
async def test_repository_accepts_atomically_and_conflicting_retry_fails_closed(
    tmp_path,
) -> None:
    request = _parent_request()
    await init_engine_from_config(DatabaseConfig(backend="sqlite", sqlite_dir=str(tmp_path)))
    session_factory = get_session_factory()
    assert session_factory is not None
    repository = SubagentBatchRepository(session_factory, tenant=request.tenant)
    accepted = AcceptedBatchV1.from_parent_request(
        request,
        batch_id="subagent-batch-1",
    )
    execution = ParentBoundBatchExecutionV1.from_parent_request(
        request,
        accepted=accepted,
    )

    created = await repository.accept_batch(
        accepted=accepted,
        execution=execution,
        item_requests=request.items,
        user_id=request.user_id,
        submission_key=request.submission_key,
        title=request.title,
        subagent_type=request.subagent_name,
    )
    duplicate = await repository.accept_batch(
        accepted=accepted,
        execution=execution,
        item_requests=request.items,
        user_id=request.user_id,
        submission_key=request.submission_key,
        title=request.title,
        subagent_type=request.subagent_name,
    )

    assert duplicate["id"] == created["id"] == "subagent-batch-1"
    assert created["acceptance_digest"] == accepted.acceptance_digest
    assert created["compatibility_state"] == "accepted_v1"
    assert "acceptance" not in created
    assert "execution" not in created

    changed_request = _parent_request(prompt="Changed immutable request")
    changed_accepted = AcceptedBatchV1.from_parent_request(
        changed_request,
        batch_id="subagent-batch-2",
    )
    changed_execution = ParentBoundBatchExecutionV1.from_parent_request(
        changed_request,
        accepted=changed_accepted,
    )
    with pytest.raises(BatchAdmissionConflict):
        await repository.accept_batch(
            accepted=changed_accepted,
            execution=changed_execution,
            item_requests=changed_request.items,
            user_id=changed_request.user_id,
            submission_key=changed_request.submission_key,
            title=changed_request.title,
            subagent_type=changed_request.subagent_name,
        )


@pytest.mark.asyncio
async def test_repository_tenant_scope_is_not_authorized_by_batch_id(tmp_path) -> None:
    request = _parent_request()
    await init_engine_from_config(DatabaseConfig(backend="sqlite", sqlite_dir=str(tmp_path)))
    session_factory = get_session_factory()
    assert session_factory is not None
    accepted = AcceptedBatchV1.from_parent_request(
        request,
        batch_id="subagent-batch-tenant",
    )
    execution = ParentBoundBatchExecutionV1.from_parent_request(
        request,
        accepted=accepted,
    )
    repository = SubagentBatchRepository(session_factory, tenant=request.tenant)
    await repository.accept_batch(
        accepted=accepted,
        execution=execution,
        item_requests=request.items,
        user_id=request.user_id,
        submission_key=request.submission_key,
        title=request.title,
        subagent_type=request.subagent_name,
    )

    other_tenant = TenantIdentityV1.from_canonical_id("tenant-b").to_persisted_reference()
    other_repository = SubagentBatchRepository(
        session_factory,
        tenant=other_tenant,
    )

    assert (
        await other_repository.get_batch(
            "subagent-batch-tenant",
            user_id=request.user_id,
        )
        is None
    )


@pytest.mark.asyncio
async def test_same_submission_identity_is_independent_between_tenants(
    tmp_path,
) -> None:
    first_request = _parent_request(tenant_id="tenant-a")
    second_request = _parent_request(tenant_id="tenant-b")
    await init_engine_from_config(DatabaseConfig(backend="sqlite", sqlite_dir=str(tmp_path)))
    session_factory = get_session_factory()
    assert session_factory is not None

    created = []
    for ordinal, request in enumerate((first_request, second_request), start=1):
        accepted = AcceptedBatchV1.from_parent_request(
            request,
            batch_id=f"subagent-batch-tenant-{ordinal}",
        )
        execution = ParentBoundBatchExecutionV1.from_parent_request(
            request,
            accepted=accepted,
        )
        repository = SubagentBatchRepository(
            session_factory,
            tenant=request.tenant,
        )
        created.append(
            await repository.accept_batch(
                accepted=accepted,
                execution=execution,
                item_requests=request.items,
                user_id=request.user_id,
                submission_key=request.submission_key,
                title=request.title,
                subagent_type=request.subagent_name,
            )
        )

    assert [batch["id"] for batch in created] == [
        "subagent-batch-tenant-1",
        "subagent-batch-tenant-2",
    ]


@pytest.mark.asyncio
async def test_legacy_unbound_rows_are_invisible_to_tenant_repository(
    tmp_path,
) -> None:
    request = _parent_request()
    await init_engine_from_config(DatabaseConfig(backend="sqlite", sqlite_dir=str(tmp_path)))
    session_factory = get_session_factory()
    assert session_factory is not None
    legacy_repository = SubagentBatchRepository(session_factory)
    tenant_repository = SubagentBatchRepository(
        session_factory,
        tenant=request.tenant,
    )
    with pytest.raises(BatchAdmissionError, match="legacy_batch_unbound"):
        await tenant_repository.create_batch(
            batch_id="forbidden-new-unbound-batch",
            user_id=request.user_id,
            thread_id=request.thread_id,
            run_id=request.run_id,
            tool_call_id="legacy-call",
            submission_key="forbidden-legacy-submission",
            title="Legacy",
            subagent_type=request.subagent_name,
            items=[{"key": "legacy-item", "prompt": "private legacy prompt"}],
            max_live_items=1,
            max_running_items=1,
            max_attempts=1,
            execution_spec={"legacy": True},
        )
    await legacy_repository.create_batch(
        batch_id="legacy-unbound-batch",
        user_id=request.user_id,
        thread_id=request.thread_id,
        run_id=request.run_id,
        tool_call_id="legacy-call",
        submission_key="legacy-submission",
        title="Legacy",
        subagent_type=request.subagent_name,
        items=[{"key": "legacy-item", "prompt": "private legacy prompt"}],
        max_live_items=1,
        max_running_items=1,
        max_attempts=1,
        execution_spec={"legacy": True},
    )

    assert (
        await tenant_repository.get_batch(
            "legacy-unbound-batch",
            user_id=request.user_id,
        )
        is None
    )
    assert (
        await tenant_repository.claim_items(
            now=datetime.now(UTC),
            lease_owner="tenant-worker",
            lease_seconds=60,
            limit=1,
        )
        == []
    )


async def _accepted_repository(tmp_path, *, request=None):
    request = request or _parent_request()
    await init_engine_from_config(DatabaseConfig(backend="sqlite", sqlite_dir=str(tmp_path)))
    session_factory = get_session_factory()
    assert session_factory is not None
    repository = SubagentBatchRepository(session_factory, tenant=request.tenant)
    accepted = AcceptedBatchV1.from_parent_request(
        request,
        batch_id="subagent-batch-fenced",
    )
    execution = ParentBoundBatchExecutionV1.from_parent_request(
        request,
        accepted=accepted,
    )
    await repository.accept_batch(
        accepted=accepted,
        execution=execution,
        item_requests=request.items,
        user_id=request.user_id,
        submission_key=request.submission_key,
        title=request.title,
        subagent_type=request.subagent_name,
    )
    return repository, request, accepted


@pytest.mark.asyncio
async def test_postgres_database_clock_uses_wall_time_not_transaction_start() -> None:
    statements = []

    class Result:
        def scalar_one(self):
            return datetime.now(UTC)

    class Session:
        def get_bind(self):
            return SimpleNamespace(
                dialect=SimpleNamespace(name="postgresql"),
            )

        async def execute(self, statement):
            statements.append(str(statement))
            return Result()

    repository = SubagentBatchRepository(lambda: None, tenant=_parent_request().tenant)

    await repository._now(Session(), datetime(2000, 1, 1, tzinfo=UTC))

    assert len(statements) == 1
    assert "clock_timestamp" in statements[0]


@pytest.mark.asyncio
async def test_attempt_epoch_fences_stale_terminal_publication(tmp_path) -> None:
    repository, request, _accepted = await _accepted_repository(tmp_path)
    first = (
        await repository.claim_items(
            now=None,
            lease_owner="private-worker-one",
            lease_seconds=60,
            limit=1,
        )
    )[0]
    assert first["lease_epoch"] == 1
    assert first["attempt_id"].startswith("ba_")
    assert await repository.mark_item_running(
        first["id"],
        attempt_id=first["attempt_id"],
        lease_epoch=first["lease_epoch"],
        lease_owner="private-worker-one",
        now=None,
    )
    session_factory = get_session_factory()
    assert session_factory is not None
    async with session_factory() as session:
        await session.execute(update(SubagentBatchItemRow).where(SubagentBatchItemRow.id == first["id"]).values(lease_expires_at=datetime.now(UTC) - timedelta(seconds=1)))
        await session.commit()

    second = (
        await repository.claim_items(
            now=None,
            lease_owner="private-worker-two",
            lease_seconds=60,
            limit=1,
        )
    )[0]
    assert second["id"] == first["id"]
    assert second["lease_epoch"] == 2
    assert not await repository.finalize_item(
        first["id"],
        attempt_id=first["attempt_id"],
        lease_epoch=first["lease_epoch"],
        lease_owner="private-worker-one",
        succeeded=True,
        result="stale secret result",
        result_preview="stale secret result",
        result_truncated=False,
        error=None,
        stop_reason=None,
        token_usage=None,
        model_name="model-a",
        completed_at=None,
    )
    assert await repository.finalize_item(
        second["id"],
        attempt_id=second["attempt_id"],
        lease_epoch=second["lease_epoch"],
        lease_owner="private-worker-two",
        succeeded=True,
        result="accepted private result",
        result_preview="accepted preview",
        result_truncated=False,
        error=None,
        stop_reason="completed",
        token_usage={"total_tokens": 4},
        model_name="model-a",
        completed_at=None,
    )

    attempts = await repository.list_attempts(
        "subagent-batch-fenced",
        user_id=request.user_id,
    )
    assert attempts is not None
    assert [attempt["terminal_code"] for attempt in attempts] == [
        "lease_expired",
        "succeeded",
    ]
    serialized = json.dumps(attempts)
    assert "private-worker" not in serialized
    assert "secret result" not in serialized
    assert "Process record one" not in serialized


@pytest.mark.asyncio
async def test_queue_rejection_is_evidenced_without_consuming_attempt(tmp_path) -> None:
    repository, request, _accepted = await _accepted_repository(tmp_path)
    first = (
        await repository.claim_items(
            now=None,
            lease_owner="worker-one",
            lease_seconds=60,
            limit=1,
        )
    )[0]

    assert await repository.requeue_item_after_admission_failure(
        first["id"],
        attempt_id=first["attempt_id"],
        lease_epoch=first["lease_epoch"],
        lease_owner="worker-one",
        error="private capacity detail",
        now=None,
    )
    retried = (
        await repository.claim_items(
            now=None,
            lease_owner="worker-two",
            lease_seconds=60,
            limit=1,
        )
    )[0]
    assert retried["attempt"] == 1
    assert retried["lease_epoch"] == 2
    attempts = await repository.list_attempts(
        "subagent-batch-fenced",
        user_id=request.user_id,
    )
    assert attempts is not None
    assert [attempt["attempt_number"] for attempt in attempts] == [1, 1]
    assert attempts[0]["terminal_code"] == "queue_rejected"
    assert attempts[0]["consumed"] is False
    assert "error" not in attempts[0]


@pytest.mark.asyncio
async def test_queue_rejection_attempt_evidence_is_hard_bounded(tmp_path) -> None:
    request = _parent_request()
    request = replace(
        request,
        limits=replace(
            request.limits,
            max_attempts=1,
            max_attempt_records_per_item=2,
        ),
    )
    repository, request, _accepted = await _accepted_repository(
        tmp_path,
        request=request,
    )

    for owner in ("worker-one", "worker-two"):
        claimed = (
            await repository.claim_items(
                now=None,
                lease_owner=owner,
                lease_seconds=60,
                limit=1,
            )
        )[0]
        assert await repository.requeue_item_after_admission_failure(
            claimed["id"],
            attempt_id=claimed["attempt_id"],
            lease_epoch=claimed["lease_epoch"],
            lease_owner=owner,
            error="private capacity detail",
            now=None,
        )

    assert (
        await repository.claim_items(
            now=None,
            lease_owner="worker-three",
            lease_seconds=60,
            limit=1,
        )
        == []
    )
    attempts = await repository.list_attempts(
        "subagent-batch-fenced",
        user_id=request.user_id,
    )
    items = await repository.list_items(
        "subagent-batch-fenced",
        user_id=request.user_id,
    )
    batch = await repository.get_batch(
        "subagent-batch-fenced",
        user_id=request.user_id,
    )

    assert attempts is not None
    assert len(attempts) == 2
    assert all(row["terminal_code"] == "queue_rejected" for row in attempts)
    assert all(row["consumed"] is False for row in attempts)
    assert items is not None
    assert items[0]["terminal_code"] == "evidence_limit_exhausted"
    assert items[0]["terminal_evidence_digest"] is None
    assert batch is not None
    assert batch["status"] == "failed"
    assert batch["terminal_code"] == "evidence_limit_exhausted"


@pytest.mark.asyncio
async def test_zero_length_lease_cannot_start_or_renew(tmp_path) -> None:
    repository, request, _accepted = await _accepted_repository(tmp_path)
    claimed = (
        await repository.claim_items(
            now=None,
            lease_owner="worker",
            lease_seconds=0,
            limit=1,
        )
    )[0]

    attempts = await repository.list_attempts(
        "subagent-batch-fenced",
        user_id=request.user_id,
    )
    assert attempts is not None
    assert attempts[0]["status"] == "claimed"
    assert attempts[0]["started_at"] is None
    assert not await repository.mark_item_running(
        claimed["id"],
        attempt_id=claimed["attempt_id"],
        lease_epoch=claimed["lease_epoch"],
        lease_owner="worker",
        now=None,
    )
    assert await repository.renew_item_lease(
        claimed["id"],
        attempt_id=claimed["attempt_id"],
        lease_epoch=claimed["lease_epoch"],
        lease_owner="worker",
        lease_seconds=60,
        now=None,
    ) == {"valid": False, "cancel_requested": True}
    assert not await repository.requeue_item_after_admission_failure(
        claimed["id"],
        attempt_id=claimed["attempt_id"],
        lease_epoch=claimed["lease_epoch"],
        lease_owner="worker",
        error="queue_rejected",
        now=None,
    )


@pytest.mark.asyncio
async def test_started_attempt_cannot_be_reclassified_as_queue_rejected(
    tmp_path,
) -> None:
    repository, request, _accepted = await _accepted_repository(tmp_path)
    claimed = (
        await repository.claim_items(
            now=None,
            lease_owner="worker",
            lease_seconds=60,
            limit=1,
        )
    )[0]
    assert await repository.mark_item_running(
        claimed["id"],
        attempt_id=claimed["attempt_id"],
        lease_epoch=claimed["lease_epoch"],
        lease_owner="worker",
        now=None,
    )

    assert not await repository.requeue_item_after_admission_failure(
        claimed["id"],
        attempt_id=claimed["attempt_id"],
        lease_epoch=claimed["lease_epoch"],
        lease_owner="worker",
        error="queue_rejected",
        now=None,
    )
    attempts = await repository.list_attempts(
        "subagent-batch-fenced",
        user_id=request.user_id,
    )
    assert attempts is not None
    assert attempts[0]["status"] == "started"
    assert attempts[0]["consumed"] is True


@pytest.mark.asyncio
async def test_started_transition_rejects_mismatched_attempt_row_epoch(
    tmp_path,
) -> None:
    repository, request, _accepted = await _accepted_repository(tmp_path)
    claimed = (
        await repository.claim_items(
            now=None,
            lease_owner="worker",
            lease_seconds=60,
            limit=1,
        )
    )[0]
    session_factory = get_session_factory()
    assert session_factory is not None
    async with session_factory() as session:
        await session.execute(update(SubagentBatchAttemptRow).where(SubagentBatchAttemptRow.id == claimed["attempt_id"]).values(lease_epoch=claimed["lease_epoch"] + 1))
        await session.commit()

    assert not await repository.mark_item_running(
        claimed["id"],
        attempt_id=claimed["attempt_id"],
        lease_epoch=claimed["lease_epoch"],
        lease_owner="worker",
        now=None,
    )
    attempts = await repository.list_attempts(
        "subagent-batch-fenced",
        user_id=request.user_id,
    )
    assert attempts is not None
    assert attempts[0]["status"] == "claimed"
    assert attempts[0]["started_at"] is None


@pytest.mark.asyncio
async def test_nonretryable_reason_is_preserved_on_batch_terminal_projection(
    tmp_path,
) -> None:
    repository, request, _accepted = await _accepted_repository(tmp_path)
    claimed = (
        await repository.claim_items(
            now=None,
            lease_owner="worker",
            lease_seconds=60,
            limit=1,
        )
    )[0]
    assert await repository.finalize_item(
        claimed["id"],
        attempt_id=claimed["attempt_id"],
        lease_epoch=claimed["lease_epoch"],
        lease_owner="worker",
        succeeded=False,
        result=None,
        result_preview=None,
        result_truncated=False,
        error="provider_not_qualified",
        stop_reason=None,
        token_usage=None,
        model_name=None,
        completed_at=None,
        terminal_code="provider_not_qualified",
    )

    batch = await repository.get_batch(
        "subagent-batch-fenced",
        user_id=request.user_id,
    )
    assert batch is not None
    assert batch["status"] == "failed"
    assert batch["terminal_code"] == "provider_not_qualified"


@pytest.mark.asyncio
async def test_cancellation_fences_active_attempt_and_records_safe_evidence(
    tmp_path,
) -> None:
    repository, request, _accepted = await _accepted_repository(tmp_path)
    claimed = (
        await repository.claim_items(
            now=None,
            lease_owner="private-worker",
            lease_seconds=60,
            limit=1,
        )
    )[0]
    assert await repository.mark_item_running(
        claimed["id"],
        attempt_id=claimed["attempt_id"],
        lease_epoch=claimed["lease_epoch"],
        lease_owner="private-worker",
        now=None,
    )

    cancelled = await repository.cancel_batch(
        "subagent-batch-fenced",
        user_id=request.user_id,
    )

    assert cancelled is not None
    assert cancelled["status"] == "cancelled"
    assert cancelled["terminal_code"] == "cancelled"
    assert cancelled["cancel_epoch"] == 1
    assert cancelled["parent_cancellable"] is False
    assert not await repository.finalize_item(
        claimed["id"],
        attempt_id=claimed["attempt_id"],
        lease_epoch=claimed["lease_epoch"],
        lease_owner="private-worker",
        succeeded=True,
        result="late private result",
        result_preview="late private result",
        result_truncated=False,
        error=None,
        stop_reason=None,
        token_usage=None,
        model_name="model-a",
        completed_at=None,
    )
    attempts = await repository.list_attempts(
        "subagent-batch-fenced",
        user_id=request.user_id,
    )
    assert attempts is not None
    assert attempts[0]["terminal_code"] == "cancelled"
    assert attempts[0]["consumed"] is True
    assert "private-worker" not in json.dumps(attempts)


@pytest.mark.asyncio
async def test_total_runtime_limit_stops_batch_using_persisted_acceptance(
    tmp_path,
) -> None:
    repository, request, _accepted = await _accepted_repository(tmp_path)
    session_factory = get_session_factory()
    assert session_factory is not None
    async with session_factory() as session:
        await session.execute(update(SubagentBatchRow).where(SubagentBatchRow.id == "subagent-batch-fenced").values(accepted_at=datetime.now(UTC) - timedelta(days=2)))
        await session.commit()

    assert (
        await repository.claim_items(
            now=datetime.now(UTC) + timedelta(days=100),
            lease_owner="worker",
            lease_seconds=60,
            limit=1,
        )
        == []
    )
    batch = await repository.get_batch(
        "subagent-batch-fenced",
        user_id=request.user_id,
    )
    assert batch is not None
    assert batch["status"] == "failed"
    assert batch["terminal_code"] == "policy_stopped"


@pytest.mark.asyncio
async def test_total_runtime_limit_fences_an_active_attempt_on_renewal(
    tmp_path,
) -> None:
    repository, request, _accepted = await _accepted_repository(tmp_path)
    claimed = (
        await repository.claim_items(
            now=None,
            lease_owner="worker",
            lease_seconds=60,
            limit=1,
        )
    )[0]
    assert await repository.mark_item_running(
        claimed["id"],
        attempt_id=claimed["attempt_id"],
        lease_epoch=claimed["lease_epoch"],
        lease_owner="worker",
        now=None,
    )
    session_factory = get_session_factory()
    assert session_factory is not None
    async with session_factory() as session:
        await session.execute(update(SubagentBatchRow).where(SubagentBatchRow.id == "subagent-batch-fenced").values(accepted_at=datetime.now(UTC) - timedelta(days=2)))
        await session.commit()

    renewed = await repository.renew_item_lease(
        claimed["id"],
        attempt_id=claimed["attempt_id"],
        lease_epoch=claimed["lease_epoch"],
        lease_owner="worker",
        lease_seconds=60,
        now=None,
    )

    assert renewed == {"valid": False, "cancel_requested": True}
    batch = await repository.get_batch(
        "subagent-batch-fenced",
        user_id=request.user_id,
    )
    attempts = await repository.list_attempts(
        "subagent-batch-fenced",
        user_id=request.user_id,
    )
    assert batch is not None
    assert batch["status"] == "failed"
    assert batch["terminal_code"] == "policy_stopped"
    assert attempts is not None
    assert attempts[0]["terminal_code"] == "policy_stopped"


@pytest.mark.asyncio
async def test_tenant_repository_ignores_process_clock_for_lease_authority(
    tmp_path,
) -> None:
    repository, _request, _accepted = await _accepted_repository(tmp_path)

    claimed = await repository.claim_items(
        now=datetime.now(UTC) + timedelta(days=365),
        lease_owner="worker",
        lease_seconds=60,
        limit=1,
    )

    assert len(claimed) == 1


@pytest.mark.asyncio
async def test_changed_operational_prompt_fails_closed_before_claim(tmp_path) -> None:
    repository, request, _accepted = await _accepted_repository(tmp_path)
    session_factory = get_session_factory()
    assert session_factory is not None
    async with session_factory() as session:
        await session.execute(update(SubagentBatchItemRow).where(SubagentBatchItemRow.batch_id == "subagent-batch-fenced").values(prompt="tampered private prompt"))
        await session.commit()

    assert (
        await repository.claim_items(
            now=None,
            lease_owner="worker",
            lease_seconds=60,
            limit=1,
        )
        == []
    )
    batch = await repository.get_batch(
        "subagent-batch-fenced",
        user_id=request.user_id,
    )
    assert batch is not None
    assert batch["status"] == "failed"
    assert batch["terminal_code"] == "execution_material_unavailable"


@pytest.mark.asyncio
async def test_changed_item_and_row_digest_still_fail_the_accepted_root(
    tmp_path,
) -> None:
    repository, request, _accepted = await _accepted_repository(tmp_path)
    changed = BatchItemRequestV1(
        key=request.items[0].key,
        prompt="tampered prompt with a matching row digest",
    )
    changed_commitment = AcceptedBatchItemV1.from_request(
        changed,
        batch_id="subagent-batch-fenced",
        ordinal=0,
    )
    session_factory = get_session_factory()
    assert session_factory is not None
    async with session_factory() as session:
        await session.execute(
            update(SubagentBatchItemRow)
            .where(SubagentBatchItemRow.batch_id == "subagent-batch-fenced")
            .values(
                prompt=changed.prompt,
                request_digest=changed_commitment.request_digest,
            )
        )
        await session.commit()

    assert (
        await repository.claim_items(
            now=None,
            lease_owner="worker",
            lease_seconds=60,
            limit=1,
        )
        == []
    )
    batch = await repository.get_batch(
        "subagent-batch-fenced",
        user_id=request.user_id,
    )
    assert batch is not None
    assert batch["terminal_code"] == "execution_material_unavailable"


@pytest.mark.asyncio
async def test_corrupt_acceptance_terminalizes_instead_of_crashing_scheduler(
    tmp_path,
) -> None:
    repository, request, _accepted = await _accepted_repository(tmp_path)
    session_factory = get_session_factory()
    assert session_factory is not None
    async with session_factory() as session:
        await session.execute(update(SubagentBatchRow).where(SubagentBatchRow.id == "subagent-batch-fenced").values(acceptance_json={"version": 999}))
        await session.commit()

    assert (
        await repository.claim_items(
            now=None,
            lease_owner="worker",
            lease_seconds=60,
            limit=1,
        )
        == []
    )
    batch = await repository.get_batch(
        "subagent-batch-fenced",
        user_id=request.user_id,
    )
    assert batch is not None
    assert batch["status"] == "failed"
    assert batch["terminal_code"] == "execution_material_unavailable"


@pytest.mark.asyncio
async def test_corrupt_active_attempt_cannot_crash_fail_closed_terminalization(
    tmp_path,
) -> None:
    repository, request, _accepted = await _accepted_repository(tmp_path)
    claimed = (
        await repository.claim_items(
            now=None,
            lease_owner="worker",
            lease_seconds=60,
            limit=1,
        )
    )[0]
    session_factory = get_session_factory()
    assert session_factory is not None
    async with session_factory() as session:
        await session.execute(update(SubagentBatchRow).where(SubagentBatchRow.id == "subagent-batch-fenced").values(acceptance_json={"version": 999}))
        await session.execute(update(SubagentBatchItemRow).where(SubagentBatchItemRow.id == claimed["id"]).values(request_digest="corrupt"))
        await session.commit()

    assert (
        await repository.claim_items(
            now=None,
            lease_owner="replacement-worker",
            lease_seconds=60,
            limit=1,
        )
        == []
    )
    batch = await repository.get_batch(
        "subagent-batch-fenced",
        user_id=request.user_id,
    )
    attempts = await repository.list_attempts(
        "subagent-batch-fenced",
        user_id=request.user_id,
    )
    assert batch is not None
    assert batch["status"] == "failed"
    assert batch["terminal_code"] == "execution_material_unavailable"
    assert attempts is not None
    assert attempts[0]["status"] == "terminal"
    assert attempts[0]["terminal_code"] == "execution_material_unavailable"
    assert attempts[0]["evidence_digest"] is None


@pytest.mark.asyncio
async def test_lifecycle_observations_are_bounded_and_payload_free(tmp_path) -> None:
    repository, request, _accepted = await _accepted_repository(tmp_path)
    claimed = (
        await repository.claim_items(
            now=None,
            lease_owner="private-worker",
            lease_seconds=60,
            limit=1,
        )
    )[0]
    assert await repository.mark_item_running(
        claimed["id"],
        attempt_id=claimed["attempt_id"],
        lease_epoch=claimed["lease_epoch"],
        lease_owner="private-worker",
        now=None,
    )
    assert await repository.finalize_item(
        claimed["id"],
        attempt_id=claimed["attempt_id"],
        lease_epoch=claimed["lease_epoch"],
        lease_owner="private-worker",
        succeeded=True,
        result="private terminal result",
        result_preview="private terminal result",
        result_truncated=False,
        error=None,
        stop_reason="completed",
        token_usage=None,
        model_name="model-a",
        completed_at=None,
    )

    observations = await repository.list_observations(
        "subagent-batch-fenced",
        user_id=request.user_id,
        limit=100,
    )

    assert observations is not None
    assert [row["event"] for row in observations] == [
        "batch.accepted",
        "batch.item_attempt",
        "batch.item_attempt",
        "batch.item_attempt",
        "batch.terminal",
    ]
    assert [row.get("transition") for row in observations if row["event"] == "batch.item_attempt"] == ["claimed", "started", "terminal"]
    serialized = json.dumps(observations)
    assert len(observations) <= 100
    assert "private-worker" not in serialized
    assert "private terminal result" not in serialized
    assert "Process record one" not in serialized


@pytest.mark.asyncio
async def test_bounded_observations_keep_acceptance_and_terminal_edges(
    tmp_path,
) -> None:
    repository, request, _accepted = await _accepted_repository(tmp_path)
    for owner in ("worker-one", "worker-two"):
        claimed = (
            await repository.claim_items(
                now=None,
                lease_owner=owner,
                lease_seconds=60,
                limit=1,
            )
        )[0]
        assert await repository.requeue_item_after_admission_failure(
            claimed["id"],
            attempt_id=claimed["attempt_id"],
            lease_epoch=claimed["lease_epoch"],
            lease_owner=owner,
            error="queue_rejected",
            now=None,
        )
    winner = (
        await repository.claim_items(
            now=None,
            lease_owner="worker-winner",
            lease_seconds=60,
            limit=1,
        )
    )[0]
    assert await repository.finalize_item(
        winner["id"],
        attempt_id=winner["attempt_id"],
        lease_epoch=winner["lease_epoch"],
        lease_owner="worker-winner",
        succeeded=True,
        result="private result",
        result_preview="private result",
        result_truncated=False,
        error=None,
        stop_reason=None,
        token_usage=None,
        model_name="model-a",
        completed_at=None,
    )

    observations = await repository.list_observations(
        "subagent-batch-fenced",
        user_id=request.user_id,
        limit=4,
    )

    assert observations is not None
    assert len(observations) == 4
    assert observations[0]["event"] == "batch.accepted"
    assert observations[-1]["event"] == "batch.terminal"
