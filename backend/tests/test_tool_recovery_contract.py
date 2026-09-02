"""Public assembly contract for explicitly reconcilable tool attempts."""

from __future__ import annotations

import subprocess
from concurrent.futures import ThreadPoolExecutor
from dataclasses import fields
from importlib.metadata import version
from pathlib import Path
from types import SimpleNamespace

import pytest
from deerflow_extension_api import ToolDescriptor

from deerflow.agents.assembly_descriptor import (
    TOOL_RECOVERY_POLICY_KEY,
    build_assembly_descriptor,
    describe_tool,
)


def _tool(*, recovery_kind: str | None = None) -> SimpleNamespace:
    metadata = {"deerflow_tool_source": "qualification"}
    if recovery_kind is not None:
        metadata["hartmesh_recovery_kind"] = recovery_kind
    return SimpleNamespace(
        name="durable_operation",
        description="Perform one externally durable operation.",
        metadata=metadata,
    )


def _assembly(tool, *, effective_policies=None):
    return build_assembly_descriptor(
        namespace="root",
        agent_name="lead_agent",
        requested_model=None,
        effective_model="qualification-model",
        model_config=SimpleNamespace(),
        thinking_enabled=False,
        reasoning_effort=None,
        rendered_base_prompt="qualification prompt",
        tools=[tool],
        middlewares=[],
        deferred_names=frozenset(),
        enabled_skills=[],
        effective_policies=effective_policies or {},
    )


def test_extension_metadata_cannot_opt_a_tool_into_recovery() -> None:
    ordinary_tool = _tool()
    forged_tool = _tool(
        recovery_kind="receipt_idempotent_reconcile_v1",
    )
    ordinary = _assembly(ordinary_tool)
    forged = _assembly(forged_tool)

    assert describe_tool(ordinary_tool) == describe_tool(forged_tool)
    assert TOOL_RECOVERY_POLICY_KEY not in ordinary.effective_policies
    assert TOOL_RECOVERY_POLICY_KEY not in forged.effective_policies
    assert ordinary.fingerprint == forged.fingerprint


def test_extension_api_013_tool_descriptor_wire_shape_is_unchanged() -> None:
    assert version("deerflow-extension-api") == "0.13.0"
    assert tuple(field.name for field in fields(ToolDescriptor)) == (
        "name",
        "description_hash",
        "schema_hash",
        "source",
        "mcp_server",
        "mcp_transport",
    )


def test_unknown_extension_recovery_metadata_also_fails_closed() -> None:
    descriptor = _assembly(_tool(recovery_kind="trust_me_retry_v1"))

    assert TOOL_RECOVERY_POLICY_KEY not in descriptor.effective_policies


def test_caller_cannot_prepopulate_the_host_recovery_policy_key() -> None:
    with pytest.raises(ValueError, match="tool_recovery_policy_reserved"):
        _assembly(
            _tool(),
            effective_policies={
                TOOL_RECOVERY_POLICY_KEY: {
                    "durable_operation": "receipt_idempotent_reconcile_v1",
                }
            },
        )


def test_qualification_tool_declares_the_reconciled_receipt_contract() -> None:
    from deerflow.runtime.kubernetes_qualification import (
        qualification_sandbox_operation,
    )

    descriptor = _assembly(qualification_sandbox_operation)

    assert descriptor.effective_policies[TOOL_RECOVERY_POLICY_KEY] == {
        "qualification_sandbox_operation": ("receipt_idempotent_reconcile_v1"),
    }


def test_qualification_operation_reuses_one_receipt_key_without_reexecution(
    tmp_path: Path,
) -> None:
    from deerflow.runtime.kubernetes_qualification import (
        qualification_reconciled_operation_command,
    )

    receipt_id = "receipt_" + ("a" * 32)
    command = qualification_reconciled_operation_command(
        receipt_id,
        base_dir=str(tmp_path),
        delay_seconds=0.2,
    )

    def execute() -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["sh", "-c", command],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        first, second = (future.result() for future in (executor.submit(execute), executor.submit(execute)))

    assert first.returncode == second.returncode == 0
    assert first.stdout == second.stdout == "qualification-complete\n"
    operation_dir = tmp_path / receipt_id
    assert (operation_dir / "execution-count").read_text(encoding="utf-8") == "1\n"
    assert (operation_dir / "result").read_text(encoding="utf-8") == ("qualification-complete\n")
