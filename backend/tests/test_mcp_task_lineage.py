from dataclasses import replace
from types import SimpleNamespace

import pytest
from deerflow_extension_api import (
    EffectiveSubjectV1,
    InvocationIdentityV1,
    PrincipalProjectionV1,
    ResolvedAgentRevisionReferenceV1,
    ResolvedProfileRevisionReferenceV1,
    SealedOriginV1,
    TenantReferenceV1,
    TrustedRunContextV1,
)

from deerflow.extensions.mcp import (
    MCP_INVOCATION_FACTS_CONTEXT_KEY,
    McpInvocationFacts,
)
from deerflow.mcp.tasks.lineage import (
    CredentialSelector,
    McpTaskLineageBinder,
    McpTaskLineageError,
    McpTaskLineageV1,
    TrustedMcpSubmissionContext,
    configured_credential_selector,
    require_current_credential_selector,
)
from deerflow.runtime.tool_evidence import (
    DurableToolReceiptV1,
    ToolAttemptContextV1,
    active_tool_receipt_context,
    build_request_projection,
)

_DIGEST_A = "a" * 64
_DIGEST_B = "b" * 64
_DIGEST_C = "c" * 64
_DIGEST_D = "d" * 64
_DIGEST_E = "e" * 64


def _tenant() -> TenantReferenceV1:
    return TenantReferenceV1(
        version=1,
        public_ref=f"tenant-{_DIGEST_A[:16]}",
        digest=_DIGEST_A,
    )


def _trusted_lead_context() -> TrustedMcpSubmissionContext:
    return TrustedMcpSubmissionContext(
        tenant=_tenant(),
        principal_identity=InvocationIdentityV1(
            effective_subject=EffectiveSubjectV1(
                kind="human",
                subject_id="raw-user-123",
                role="member",
            )
        ),
        parent_run_id="run-1",
        parent_execution_task_id="run-1",
        parent_execution_kind="lead",
        parent_subagent_name=None,
        parent_tool_receipt_id=f"tr_{_DIGEST_B}",
        agent_revision_digest=_DIGEST_C,
        assembly_fingerprint=_DIGEST_D,
        subagent_catalog_digest=_DIGEST_E,
        subagent_definition_digest=None,
        extension_generation=7,
        extension_manifest_digest=_DIGEST_B,
        accepted_origin_digest=_DIGEST_C,
    )


def _host_runtime_context() -> tuple[dict[str, object], DurableToolReceiptV1]:
    identity = _trusted_lead_context().principal_identity
    origin = SealedOriginV1(source_kind="web", digest=_DIGEST_C)
    trusted = TrustedRunContextV1(
        identity=identity,
        origin=origin,
        thread_id="thread-1",
        external_key_reference=None,
        agent_revision=ResolvedAgentRevisionReferenceV1(
            agent_id="lead_agent",
            digest=_DIGEST_C,
        ),
        profile_revision=ResolvedProfileRevisionReferenceV1(
            profile_id="default",
            digest=_DIGEST_D,
        ),
        extension_generation=7,
        extension_manifest_digest=_DIGEST_B,
        tenant=_tenant(),
        run_id="run-1",
    )
    facts = McpInvocationFacts(
        principal=PrincipalProjectionV1(identity=identity),
        origin=origin,
        thread_id="thread-1",
        run_id="run-1",
        agent_revision=trusted.agent_revision,
        extension_generation=7,
        extension_manifest_digest=_DIGEST_B,
        trusted_context=trusted,
    )
    receipt = DurableToolReceiptV1.started(
        context=ToolAttemptContextV1(
            run_id="run-1",
            execution_task_id="run-1",
            execution_kind="lead",
            subagent_name=None,
            tool_call_id="call-1",
            attempt=1,
            owner_id="worker-1",
            lease_epoch=3,
            agent_revision_digest=_DIGEST_C,
            assembly_fingerprint=_DIGEST_D,
            extension_generation=7,
            subagent_catalog_digest=_DIGEST_E,
            subagent_definition_digest=None,
            tenant=_tenant(),
        ),
        tool_name="reports_submit_report",
        request_projection_digest=_DIGEST_A,
    )
    return {MCP_INVOCATION_FACTS_CONTEXT_KEY: facts}, receipt


def test_agent_lineage_is_deterministic_and_persists_only_safe_commitments():
    binder = McpTaskLineageBinder()
    request_projection = build_request_projection(
        "submit_report",
        {"api_key": "api-key-material", "topic": "quarterly results"},
    )
    first = binder.for_agent_tool(
        trusted_runtime=_trusted_lead_context(),
        server_name="reports",
        tool_name="submit_report",
        safe_request_projection=request_projection,
        credential_selector=None,
    )
    second = binder.for_agent_tool(
        trusted_runtime=_trusted_lead_context(),
        server_name="reports",
        tool_name="submit_report",
        safe_request_projection={
            "arguments": {
                "topic": {"utf8_bytes": 17, "type": "string", "classification": "shape"},
                "api_key": {"type": "string", "classification": "secret_handle"},
            },
            "tool_name": "submit_report",
            "version": 1,
        },
        credential_selector=None,
    )

    assert first == second
    persisted = first.to_persisted_json()
    assert persisted["digest"] == first.digest
    assert persisted["principal_ref"].startswith("principal-")
    assert persisted["request_projection_digest"] == first.request_projection_digest
    serialized = repr(persisted)
    assert "raw-user-123" not in serialized
    assert "api-key-material" not in serialized
    assert first.from_persisted_json(persisted) == first


def test_binder_rejects_an_unclassified_raw_request_projection():
    with pytest.raises(McpTaskLineageError) as exc_info:
        McpTaskLineageBinder().for_agent_tool(
            trusted_runtime=_trusted_lead_context(),
            server_name="reports",
            tool_name="submit_report",
            safe_request_projection={
                "version": 1,
                "tool_name": "submit_report",
                "arguments": {"api_key": "api-key-material"},
            },
            credential_selector=None,
        )

    assert exc_info.value.code == "mcp_task_lineage_invalid"


def test_request_projection_digest_commits_to_the_configured_server_name():
    binder = McpTaskLineageBinder()
    projection = build_request_projection("submit_report", {"topic": "same shape"})

    reports = binder.for_agent_tool(
        trusted_runtime=_trusted_lead_context(),
        server_name="reports",
        tool_name="submit_report",
        safe_request_projection=projection,
        credential_selector=None,
    )
    archive = binder.for_agent_tool(
        trusted_runtime=_trusted_lead_context(),
        server_name="archive",
        tool_name="submit_report",
        safe_request_projection=projection,
        credential_selector=None,
    )

    assert reports.request_projection_digest != archive.request_projection_digest


def test_kind_invariants_reject_missing_agent_parent_and_forged_standalone_parent():
    binder = McpTaskLineageBinder()
    projection = build_request_projection("submit_report", {"topic": "safe shape"})
    agent = binder.for_agent_tool(
        trusted_runtime=_trusted_lead_context(),
        server_name="reports",
        tool_name="submit_report",
        safe_request_projection=projection,
        credential_selector=None,
    )
    tampered_agent = {**agent.to_persisted_json(), "parent_run_id": None}
    with pytest.raises(McpTaskLineageError):
        McpTaskLineageV1.from_persisted_json(tampered_agent)

    standalone = binder.for_standalone_api(
        tenant=_tenant(),
        principal_identity=_trusted_lead_context().principal_identity,
        extension_generation=7,
        extension_manifest_digest=_DIGEST_B,
        accepted_origin_digest=_DIGEST_C,
        server_name="reports",
        tool_name="submit_report",
        safe_request_projection=projection,
        credential_selector=None,
    )
    assert standalone.kind == "standalone_api"
    assert standalone.parent_run_id is None
    forged_standalone = {
        **standalone.to_persisted_json(),
        "parent_run_id": "forged-parent",
    }
    with pytest.raises(McpTaskLineageError):
        McpTaskLineageV1.from_persisted_json(forged_standalone)


def test_credential_selector_reference_changes_for_every_security_anchor():
    trusted = _trusted_lead_context()
    selector = CredentialSelector(binding_id="reports-user-auth", version=3)
    lineage = McpTaskLineageBinder().for_agent_tool(
        trusted_runtime=trusted,
        server_name="reports",
        tool_name="submit_report",
        safe_request_projection=build_request_projection("submit_report", {}),
        credential_selector=selector,
    )
    principal_ref = lineage.principal_ref

    persisted = lineage.to_persisted_json()
    assert persisted["credential_selector_version"] == 3
    assert "reports-user-auth" not in repr(persisted)

    baseline = selector.safe_reference(
        tenant_digest=trusted.tenant.digest,
        principal_ref=principal_ref,
        server_name="reports",
    )
    other_tenant = selector.safe_reference(
        tenant_digest=_DIGEST_B,
        principal_ref=principal_ref,
        server_name="reports",
    )
    other_principal = selector.safe_reference(
        tenant_digest=trusted.tenant.digest,
        principal_ref=f"principal-{'f' * 24}",
        server_name="reports",
    )
    other_binding = CredentialSelector(
        binding_id="reports-oauth",
        version=3,
    ).safe_reference(
        tenant_digest=trusted.tenant.digest,
        principal_ref=principal_ref,
        server_name="reports",
    )
    other_version = replace(selector, version=4).safe_reference(
        tenant_digest=trusted.tenant.digest,
        principal_ref=principal_ref,
        server_name="reports",
    )

    assert len({baseline, other_tenant, other_principal, other_binding, other_version}) == 5

    invalid_pair = {
        **persisted,
        "credential_selector_ref": None,
    }
    with pytest.raises(McpTaskLineageError):
        McpTaskLineageV1.from_persisted_json(invalid_pair)


def test_recovery_rejects_an_unavailable_credential_binding_version() -> None:
    config = SimpleNamespace(
        credential_binding_id="reports-user-auth",
        credential_version=3,
        user_auth=None,
        oauth=None,
        headers={},
        type="http",
    )
    selector = configured_credential_selector("reports", config)
    assert selector is not None
    lineage = McpTaskLineageBinder().for_agent_tool(
        trusted_runtime=_trusted_lead_context(),
        server_name="reports",
        tool_name="submit_report",
        safe_request_projection=build_request_projection("submit_report", {}),
        credential_selector=selector,
    )

    require_current_credential_selector(lineage, config)
    config.credential_version = 4
    with pytest.raises(McpTaskLineageError) as exc_info:
        require_current_credential_selector(lineage, config)

    assert exc_info.value.code == "mcp_task_credential_binding_unavailable"


def test_implicit_credential_selector_commits_to_every_active_mechanism() -> None:
    config = SimpleNamespace(
        credential_binding_id=None,
        credential_version=1,
        user_auth=SimpleNamespace(enabled=True),
        oauth=SimpleNamespace(enabled=True),
        headers={"X-Api-Key": "must-not-enter-the-selector"},
        type="http",
    )

    selector = configured_credential_selector("reports", config)

    assert selector is not None
    assert selector.binding_id == ("auto:static-http+oauth+user-auth:reports")
    assert "Api-Key" not in selector.binding_id
    assert "must-not-enter" not in selector.binding_id


def test_oversized_or_digest_mismatched_lineage_is_rejected():
    binder = McpTaskLineageBinder()
    oversized_projection = {
        "version": 1,
        "tool_name": "submit_report",
        "arguments": {
            "topic": {
                "classification": "evidence_safe",
                "type": "string",
                "value": "x" * 9_000,
            }
        },
    }
    with pytest.raises(McpTaskLineageError):
        binder.for_agent_tool(
            trusted_runtime=_trusted_lead_context(),
            server_name="reports",
            tool_name="submit_report",
            safe_request_projection=oversized_projection,
            credential_selector=None,
        )

    lineage = binder.for_agent_tool(
        trusted_runtime=_trusted_lead_context(),
        server_name="reports",
        tool_name="submit_report",
        safe_request_projection=build_request_projection("submit_report", {}),
        credential_selector=None,
    )
    mismatched = {**lineage.to_persisted_json(), "digest": _DIGEST_A}
    with pytest.raises(McpTaskLineageError):
        McpTaskLineageV1.from_persisted_json(mismatched)


def test_agent_submission_context_requires_the_active_trusted_started_receipt():
    runtime_context, receipt = _host_runtime_context()

    with pytest.raises(McpTaskLineageError) as exc_info:
        TrustedMcpSubmissionContext.from_runtime_context(
            runtime_context,
            expected_tool_name="reports_submit_report",
        )
    assert exc_info.value.code == "mcp_task_lineage_unavailable"

    with active_tool_receipt_context(receipt):
        trusted = TrustedMcpSubmissionContext.from_runtime_context(
            runtime_context,
            expected_tool_name="reports_submit_report",
        )

    assert trusted.parent_run_id == "run-1"
    assert trusted.parent_execution_task_id == "run-1"
    assert trusted.parent_tool_receipt_id == receipt.receipt_id
    assert trusted.tenant == _tenant()


def test_agent_submission_context_rejects_cross_tenant_receipt() -> None:
    runtime_context, receipt = _host_runtime_context()
    other_tenant = TenantReferenceV1(
        version=1,
        public_ref=f"tenant-{_DIGEST_B[:16]}",
        digest=_DIGEST_B,
    )
    forged = DurableToolReceiptV1.started(
        context=replace(receipt.context, tenant=other_tenant),
        tool_name=receipt.tool_name,
        request_projection_digest=receipt.request_projection_digest,
    )

    with active_tool_receipt_context(forged):
        with pytest.raises(McpTaskLineageError) as exc_info:
            TrustedMcpSubmissionContext.from_runtime_context(
                runtime_context,
                expected_tool_name="reports_submit_report",
            )

    assert exc_info.value.code == "mcp_task_lineage_unavailable"


def test_agent_submission_context_rejects_a_receipt_for_another_tool() -> None:
    runtime_context, receipt = _host_runtime_context()
    other_tool = DurableToolReceiptV1.started(
        context=receipt.context,
        tool_name="reports_other_tool",
        request_projection_digest=receipt.request_projection_digest,
    )

    with active_tool_receipt_context(other_tool):
        with pytest.raises(McpTaskLineageError) as exc_info:
            TrustedMcpSubmissionContext.from_runtime_context(
                runtime_context,
                expected_tool_name="reports_submit_report",
            )

    assert exc_info.value.code == "mcp_task_lineage_unavailable"
