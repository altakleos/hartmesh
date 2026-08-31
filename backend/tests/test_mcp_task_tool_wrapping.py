from types import SimpleNamespace
from unittest.mock import patch

import pytest
from deerflow_extension_api import (
    EffectiveSubjectV1,
    InvocationIdentityV1,
    PrincipalProjectionV1,
    ResolvedAgentRevisionReferenceV1,
    ResolvedProfileRevisionReferenceV1,
    SealedOriginV1,
    TrustedRunContextV1,
)
from langchain_core.tools import StructuredTool
from langgraph.runtime import ExecutionInfo
from pydantic import BaseModel

from deerflow.agents.middlewares.tool_receipt_middleware import ToolReceiptMiddleware
from deerflow.config.extensions_config import ExtensionsConfig, McpServerConfig
from deerflow.extensions.mcp import (
    MCP_INVOCATION_FACTS_CONTEXT_KEY,
    McpInvocationFacts,
)
from deerflow.mcp.tasks import McpTaskLineageError
from deerflow.mcp.tasks.runtime import (
    McpTaskConfigurationError,
    set_mcp_task_config_snapshot,
    set_mcp_task_submitter,
)
from deerflow.mcp.tools import _configure_task_tools_for_server, get_mcp_tools
from deerflow.runtime.tenant_identity import TenantIdentityV1
from deerflow.runtime.tool_evidence import (
    TOOL_EVIDENCE_CONTEXT_KEY,
    TOOL_EVIDENCE_SINK_KEY,
    DurableToolReceiptV1,
    ToolAttemptContextV1,
    ToolAttemptReservation,
    ToolEvidenceRuntimeBinding,
    active_tool_receipt_context,
)

_TENANT = TenantIdentityV1.from_canonical_id("test").to_persisted_reference()


def _runtime_and_receipt():
    identity = InvocationIdentityV1(
        effective_subject=EffectiveSubjectV1(
            kind="human",
            subject_id="test-user-autouse",
            role="member",
        )
    )
    origin = SealedOriginV1(source_kind="web", digest="e" * 64)
    trusted = TrustedRunContextV1(
        identity=identity,
        origin=origin,
        thread_id="thread-1",
        external_key_reference=None,
        agent_revision=ResolvedAgentRevisionReferenceV1(
            agent_id="lead_agent",
            digest="a" * 64,
        ),
        profile_revision=ResolvedProfileRevisionReferenceV1(
            profile_id="default",
            digest="b" * 64,
        ),
        extension_generation=3,
        extension_manifest_digest="c" * 64,
        tenant=_TENANT,
        run_id="run-1",
    )
    facts = McpInvocationFacts(
        principal=PrincipalProjectionV1(
            user_id="test-user-autouse",
            identity=identity,
        ),
        origin=origin,
        thread_id="thread-1",
        run_id="run-1",
        agent_revision=trusted.agent_revision,
        extension_generation=3,
        extension_manifest_digest="c" * 64,
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
            lease_epoch=1,
            agent_revision_digest="a" * 64,
            assembly_fingerprint="d" * 64,
            extension_generation=3,
            subagent_catalog_digest="f" * 64,
            subagent_definition_digest=None,
            tenant=_TENANT,
        ),
        tool_name="submit_report",
        request_projection_digest="9" * 64,
    )
    return (
        SimpleNamespace(
            context={MCP_INVOCATION_FACTS_CONTEXT_KEY: facts},
            config={},
            tool_call_id="call-1",
        ),
        receipt,
    )


class _SubmitArgs(BaseModel):
    topic: str


def _tool(name: str, *, description: str | None = None) -> StructuredTool:
    async def call(topic: str):
        return topic

    return StructuredTool(
        name=name,
        description=description if description is not None else name,
        args_schema=_SubmitArgs,
        coroutine=call,
    )


def _server_config() -> McpServerConfig:
    return McpServerConfig.model_validate(
        {
            "task_toolsets": [
                {
                    "name": "report-generation",
                    "submit_tool": "submit_report",
                    "status_tool": "get_report_status",
                    "cancel_tool": "cancel_report",
                }
            ]
        }
    )


class FakeSubmitter:
    def __init__(self):
        self.calls = []

    async def submit(self, **kwargs):
        self.calls.append(kwargs)
        return {
            "id": "mcp-task-local-1",
            "status": "submitted",
            "remote_task_id": "must-not-leak",
            "driver_data": {"must": "not leak"},
        }


def test_unconfigured_server_tools_are_returned_unchanged() -> None:
    tools = [_tool("reports_search")]

    configured = _configure_task_tools_for_server(
        tools,
        server_name="reports",
        server_config=McpServerConfig(),
        tool_name_prefix=True,
    )

    assert configured == tools
    assert configured[0] is tools[0]


def test_configured_status_and_cancel_tools_are_hidden_from_the_agent() -> None:
    tools = [
        _tool("reports_submit_report"),
        _tool("reports_get_report_status"),
        _tool("reports_cancel_report"),
        _tool("reports_search"),
    ]

    configured = _configure_task_tools_for_server(
        tools,
        server_name="reports",
        server_config=_server_config(),
        tool_name_prefix=True,
    )

    assert [tool.name for tool in configured] == ["reports_submit_report", "reports_search"]


def test_submit_wrapper_preserves_server_description_and_appends_background_contract() -> None:
    tools = [
        _tool(
            "submit_report",
            description="Generate a quarterly financial report for the requested topic.",
        ),
        _tool("get_report_status"),
        _tool("cancel_report"),
    ]

    configured = _configure_task_tools_for_server(
        tools,
        server_name="reports",
        server_config=_server_config(),
        tool_name_prefix=False,
    )

    assert configured[0].description == (
        "Generate a quarterly financial report for the requested topic.\n\nSubmitted as durable background task 'report-generation'; returns a DeerFlow task ID immediately and status polling is handled automatically."
    )


def test_configured_task_toolsets_fail_when_a_raw_tool_is_missing() -> None:
    with pytest.raises(McpTaskConfigurationError, match="cancel_report"):
        _configure_task_tools_for_server(
            [_tool("reports_submit_report"), _tool("reports_get_report_status")],
            server_name="reports",
            server_config=_server_config(),
            tool_name_prefix=True,
        )


@pytest.mark.asyncio
async def test_submit_wrapper_persists_before_returning_only_the_local_handle() -> None:
    submitter = FakeSubmitter()
    set_mcp_task_submitter(submitter)
    try:
        configured = _configure_task_tools_for_server(
            [
                _tool("submit_report"),
                _tool("get_report_status"),
                _tool("cancel_report"),
            ],
            server_name="reports",
            server_config=_server_config(),
            tool_name_prefix=False,
        )
        submit_tool = configured[0]
        runtime, receipt = _runtime_and_receipt()

        with active_tool_receipt_context(receipt):
            result = await submit_tool.coroutine(runtime=runtime, topic="MCP")

        assert result == {
            "task_id": "mcp-task-local-1",
            "task_name": "report-generation",
            "status": "submitted",
            "message": "Task is running in the background.",
        }
        call = submitter.calls[0]
        request = call["request"]
        assert call["driver_name"] == "ordinary-tools"
        assert request.user_id == "test-user-autouse"
        assert request.thread_id == "thread-1"
        assert request.lineage.kind == "agent_tool"
        assert request.lineage.parent_run_id == "run-1"
        assert request.lineage.parent_tool_receipt_id == receipt.receipt_id
        assert request.lineage.tenant == _TENANT
        assert request.lineage.agent_revision_digest == "a" * 64
        assert request.lineage.assembly_fingerprint == "d" * 64
        assert request.lineage.subagent_catalog_digest == "f" * 64
        assert request.lineage.extension_generation == 3
        assert request.lineage.extension_manifest_digest == "c" * 64
        assert request.arguments == {"topic": "MCP"}
        assert request.driver_data == {
            "submit_tool": "submit_report",
            "status_tool": "get_report_status",
            "cancel_tool": "cancel_report",
        }
    finally:
        set_mcp_task_submitter(None)


@pytest.mark.asyncio
async def test_failure_after_task_creation_keeps_one_attributed_task_and_failed_receipt() -> None:
    class RecordingSink:
        def __init__(self):
            self.started = []
            self.outcomes = []

        async def reserve_started(
            self,
            *,
            binding,
            tool_call_id,
            tool_name,
            request_projection_digest,
            dispatch,
        ):
            assert dispatch.node_attempt == 1
            started = DurableToolReceiptV1.started(
                context=binding.make_attempt(tool_call_id, 1),
                tool_name=tool_name,
                request_projection_digest=request_projection_digest,
            )
            self.started.append(started)
            return ToolAttemptReservation(started=started)

        async def record_started(self, receipt):
            self.started.append(receipt)

        async def record_outcome(self, receipt):
            self.outcomes.append(receipt)

    submitter = FakeSubmitter()
    set_mcp_task_submitter(submitter)
    try:
        configured = _configure_task_tools_for_server(
            [_tool("submit_report"), _tool("get_report_status"), _tool("cancel_report")],
            server_name="reports",
            server_config=_server_config(),
            tool_name_prefix=False,
        )
        submit_tool = configured[0]
        accepted_runtime, _manual_receipt = _runtime_and_receipt()
        sink = RecordingSink()
        context = dict(accepted_runtime.context)
        context[TOOL_EVIDENCE_CONTEXT_KEY] = ToolEvidenceRuntimeBinding(
            run_id="run-1",
            execution_task_id="run-1",
            execution_kind="lead",
            subagent_name=None,
            owner_id="worker-1",
            lease_epoch=1,
            agent_revision_digest="a" * 64,
            assembly_fingerprint="d" * 64,
            extension_generation=3,
            subagent_catalog_digest="f" * 64,
            subagent_definition_digest=None,
            tenant=_TENANT,
        )
        context[TOOL_EVIDENCE_SINK_KEY] = sink
        request = SimpleNamespace(
            tool_call={
                "name": "submit_report",
                "id": "call-1",
                "args": {"topic": "MCP"},
            },
            tool=submit_tool,
            runtime=SimpleNamespace(
                context=context,
                execution_info=ExecutionInfo(
                    checkpoint_id="checkpoint-1",
                    checkpoint_ns="",
                    task_id="node-task-1",
                    thread_id="thread-1",
                    run_id="run-1",
                    node_attempt=1,
                ),
            ),
        )

        async def persist_then_fail(req):
            await submit_tool.coroutine(runtime=req.runtime, topic="MCP")
            raise RuntimeError("post-submit middleware failure")

        with pytest.raises(RuntimeError, match="post-submit middleware failure"):
            await ToolReceiptMiddleware().awrap_tool_call(
                request,
                persist_then_fail,
            )

        assert len(submitter.calls) == 1
        lineage = submitter.calls[0]["request"].lineage
        assert lineage.parent_tool_receipt_id == sink.started[0].receipt_id
        assert lineage.parent_run_id == "run-1"
        assert len(sink.outcomes) == 1
        assert sink.outcomes[0].receipt_id == lineage.parent_tool_receipt_id
        assert sink.outcomes[0].phase == "failed"
        assert sink.outcomes[0].safe_error_code == "internal_error"
    finally:
        set_mcp_task_submitter(None)


@pytest.mark.asyncio
async def test_frozen_subagent_submission_binds_its_task_and_definition() -> None:
    submitter = FakeSubmitter()
    set_mcp_task_submitter(submitter)
    try:
        configured = _configure_task_tools_for_server(
            [_tool("submit_report"), _tool("get_report_status"), _tool("cancel_report")],
            server_name="reports",
            server_config=_server_config(),
            tool_name_prefix=False,
        )
        runtime, _lead_receipt = _runtime_and_receipt()
        receipt = DurableToolReceiptV1.started(
            context=ToolAttemptContextV1(
                run_id="run-1",
                execution_task_id="subagent-task-7",
                execution_kind="subagent",
                subagent_name="researcher",
                tool_call_id="call-subagent-1",
                attempt=1,
                owner_id="worker-1",
                lease_epoch=1,
                agent_revision_digest="a" * 64,
                assembly_fingerprint="d" * 64,
                extension_generation=3,
                subagent_catalog_digest="f" * 64,
                subagent_definition_digest="8" * 64,
                tenant=_TENANT,
            ),
            tool_name="submit_report",
            request_projection_digest="9" * 64,
        )

        with active_tool_receipt_context(receipt):
            await configured[0].coroutine(runtime=runtime, topic="MCP")

        lineage = submitter.calls[0]["request"].lineage
        assert lineage.parent_run_id == "run-1"
        assert lineage.parent_execution_task_id == "subagent-task-7"
        assert lineage.parent_execution_kind == "subagent"
        assert lineage.parent_subagent_name == "researcher"
        assert lineage.subagent_definition_digest == "8" * 64
        assert lineage.parent_tool_receipt_id == receipt.receipt_id
    finally:
        set_mcp_task_submitter(None)


@pytest.mark.asyncio
async def test_submit_wrapper_fails_before_remote_without_active_durable_receipt() -> None:
    submitter = FakeSubmitter()
    set_mcp_task_submitter(submitter)
    try:
        configured = _configure_task_tools_for_server(
            [_tool("submit_report"), _tool("get_report_status"), _tool("cancel_report")],
            server_name="reports",
            server_config=_server_config(),
            tool_name_prefix=False,
        )
        runtime, _receipt = _runtime_and_receipt()

        with pytest.raises(McpTaskLineageError) as exc_info:
            await configured[0].coroutine(runtime=runtime, topic="MCP")

        assert exc_info.value.code == "mcp_task_lineage_unavailable"
        assert submitter.calls == []
    finally:
        set_mcp_task_submitter(None)


@pytest.mark.asyncio
async def test_submit_wrapper_fails_clearly_without_gateway_task_runtime() -> None:
    set_mcp_task_submitter(None)
    configured = _configure_task_tools_for_server(
        [_tool("submit_report"), _tool("get_report_status"), _tool("cancel_report")],
        server_name="reports",
        server_config=_server_config(),
        tool_name_prefix=False,
    )

    with pytest.raises(McpTaskConfigurationError, match="not initialized"):
        await configured[0].coroutine(topic="MCP")


@pytest.mark.asyncio
async def test_tool_reload_rejects_task_server_runtime_config_drift() -> None:
    startup = ExtensionsConfig(mcpServers={"reports": _server_config()})
    current = ExtensionsConfig(mcpServers={"reports": _server_config()})
    current.mcp_servers["reports"].env["TOKEN"] = "rotated"
    set_mcp_task_config_snapshot(startup)
    try:
        with (
            patch(
                "deerflow.mcp.tools.ExtensionsConfig.from_file",
                return_value=current,
            ),
            pytest.raises(McpTaskConfigurationError, match="reports.*restart"),
        ):
            await get_mcp_tools()
    finally:
        set_mcp_task_config_snapshot(None)
