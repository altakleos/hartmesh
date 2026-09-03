from __future__ import annotations

from deerflow.runtime.runs.worker import _build_runtime_context, _install_runtime_context
from deerflow.runtime.tool_evidence import (
    TOOL_EVIDENCE_CONTEXT_KEY,
    TOOL_EVIDENCE_SINK_KEY,
    NullDurableToolReceiptSink,
    ToolEvidenceRuntimeBinding,
    install_tool_evidence_context,
)
from deerflow.subagents.batch_acceptance import (
    PARENT_BATCH_ACCEPTANCE_CONTEXT_KEY,
)


def _binding() -> ToolEvidenceRuntimeBinding:
    return ToolEvidenceRuntimeBinding(
        run_id="run-1",
        execution_task_id="run-1",
        execution_kind="lead",
        subagent_name=None,
        owner_id="worker-1",
        lease_epoch=4,
        agent_revision_digest="a" * 64,
        assembly_fingerprint="b" * 64,
        extension_generation=3,
        subagent_catalog_digest="c" * 64,
        subagent_definition_digest=None,
    )


def test_caller_cannot_inject_tool_evidence_capabilities() -> None:
    forged = {
        TOOL_EVIDENCE_CONTEXT_KEY: {"owner_id": "attacker"},
        TOOL_EVIDENCE_SINK_KEY: "attacker-sink",
    }

    runtime_context = _build_runtime_context("thread-1", "run-1", forged)

    assert TOOL_EVIDENCE_CONTEXT_KEY not in runtime_context
    assert TOOL_EVIDENCE_SINK_KEY not in runtime_context


def test_caller_cannot_inject_accepted_parent_batch_capability() -> None:
    runtime_context = _build_runtime_context(
        "thread-1",
        "run-1",
        {PARENT_BATCH_ACCEPTANCE_CONTEXT_KEY: "forged"},
    )

    assert PARENT_BATCH_ACCEPTANCE_CONTEXT_KEY not in runtime_context


def test_install_replaces_forged_values_only_with_typed_host_objects() -> None:
    config = {
        "context": {
            TOOL_EVIDENCE_CONTEXT_KEY: "forged",
            TOOL_EVIDENCE_SINK_KEY: "forged",
            PARENT_BATCH_ACCEPTANCE_CONTEXT_KEY: "forged",
        },
        "configurable": {
            TOOL_EVIDENCE_CONTEXT_KEY: "forged",
            TOOL_EVIDENCE_SINK_KEY: "forged",
            PARENT_BATCH_ACCEPTANCE_CONTEXT_KEY: "forged",
        },
    }
    runtime_context: dict[str, object] = {
        "thread_id": "thread-1",
        "run_id": "run-1",
    }
    binding = _binding()
    sink = NullDurableToolReceiptSink()
    install_tool_evidence_context(runtime_context, binding=binding, sink=sink)
    accepted_parent = object()
    runtime_context[PARENT_BATCH_ACCEPTANCE_CONTEXT_KEY] = accepted_parent

    _install_runtime_context(config, runtime_context)

    assert config["context"][TOOL_EVIDENCE_CONTEXT_KEY] is binding
    assert config["context"][TOOL_EVIDENCE_SINK_KEY] is sink
    assert TOOL_EVIDENCE_CONTEXT_KEY not in config["configurable"]
    assert TOOL_EVIDENCE_SINK_KEY not in config["configurable"]
    assert config["context"][PARENT_BATCH_ACCEPTANCE_CONTEXT_KEY] is accepted_parent
    assert PARENT_BATCH_ACCEPTANCE_CONTEXT_KEY not in config["configurable"]
