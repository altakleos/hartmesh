"""Closure assertions for the documented durable invocation contract."""

from __future__ import annotations

import ast
import re
from pathlib import Path

_BACKEND_ROOT = Path(__file__).resolve().parents[1]
_REPO_ROOT = _BACKEND_ROOT.parent


def _runtime_guide() -> str:
    return (_BACKEND_ROOT / "docs" / "INVOCATION_RUNTIME.md").read_text(encoding="utf-8")


def _collected_test_names() -> set[str]:
    names: set[str] = set()
    for path in (_BACKEND_ROOT / "tests").rglob("test_*.py"):
        module = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        names.update(node.name for node in ast.walk(module) if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name.startswith("test_"))
    return names


def _closure_matrix_rows(guide: str) -> dict[str, dict[str, str]]:
    section = guide.split("## Concern-to-evidence closure matrix", maxsplit=1)[1]
    section = section.split("\n## ", maxsplit=1)[0]
    lines = [line for line in section.splitlines() if line.startswith("| ")]
    headings = [cell.strip() for cell in lines[0].strip("|").split("|")]
    rows: dict[str, dict[str, str]] = {}
    for line in lines[1:]:
        values = [cell.strip() for cell in line.strip("|").split("|")]
        row = dict(zip(headings, values, strict=True))
        rows[row["Concern"]] = row
    return rows


def test_closure_matrix_names_every_implemented_invariant_and_evidence_boundary() -> None:
    guide = _runtime_guide()

    assert "## Concern-to-evidence closure matrix" in guide
    assert "| Concern | Implemented invariant | Implementation | Named evidence | Deployment evidence | Status |" in guide
    expected_concerns = {
        "Application Module and portable Adapters",
        "All durable launch sources",
        "Keyed native ingress receipts",
        "Canonical keyed replay",
        "Split identity and sealed Origin",
        "Trusted contributor context",
        "Restrictive authorization and constraints",
        "Pinned agent and extension material",
        "Transactional lifecycle evidence",
        "Polling observation and bounded summaries",
        "Scoped service observation",
        "Clarification continuation",
        "Graceful shutdown and process recovery",
        "PostgreSQL schema and arbitration",
        "One-replica deployment truth",
        "Live Kubernetes pod recovery",
        "Legacy compatibility and native execution",
    }
    rows = _closure_matrix_rows(guide)
    assert set(rows) == expected_concerns

    collected_test_names = _collected_test_names()
    for concern, row in rows.items():
        implementation_paths = re.findall(r"`([^`]+)`", row["Implementation"])
        assert implementation_paths, concern
        for relative_path in implementation_paths:
            assert (_REPO_ROOT / relative_path).exists(), relative_path

        evidence_names = re.findall(r"`(test_[^`]+)`", row["Named evidence"])
        assert evidence_names, concern
        for test_name in evidence_names:
            assert test_name in collected_test_names, test_name

    required_row_evidence = {
        "Keyed native ingress receipts": {
            "test_received_payload_survives_process_loss_before_claim",
            "test_response_loss_after_admission_replays_known_run_before_binding",
            "test_signed_route_reaches_real_runtime_and_redelivery_replays",
        },
        "All durable launch sources": {
            "test_gateway_create_stream_wait_routes_share_durable_admission",
            "test_scheduled_occurrence_enters_runtime_with_typed_execution_facts",
            "test_authenticated_channel_launch_enters_runtime_with_typed_source_facts",
            "test_ensure_builds_a_host_trusted_service_launch_intent",
        },
        "Polling observation and bounded summaries": {
            "test_context_query_excludes_other_owners_and_auxiliary_rows",
            "test_malformed_ahead_and_pruned_cursors_are_typed",
            "test_sql_context_page_loads_summary_rows_only_for_bounded_page_ids",
            "test_postgres_query_uses_one_repeatable_read_snapshot",
        },
        "Clarification continuation": {
            "test_clarification_completes_then_answer_starts_new_same_thread_invocation",
            "test_native_channel_revalidates_owner_dedupes_and_continues_clarification",
        },
        "Graceful shutdown and process recovery": {
            "test_shutdown_orders_producers_runs_memory_and_dependencies",
            "test_orphan_recovery_records_failed_with_stable_reason",
        },
        "Legacy compatibility and native execution": {
            "test_gateway_mounts_runtime_routes_without_replacing_legacy_runs",
            "test_full_chain_order",
            "test_make_lead_agent_custom_skill_allowlist_does_not_activate_tool_policy",
            "test_after_agent_queues_memory_under_runtime_user",
            "test_aexecute_propagates_one_trusted_run_context_without_free_form_attributes",
            "test_sandbox_middleware_state_matches_thread_state_sandbox_field",
        },
    }
    for concern, evidence_names in required_row_evidence.items():
        row_evidence = rows[concern]["Named evidence"]
        assert all(f"`{test_name}`" in row_evidence for test_name in evidence_names)


def test_clarification_observation_and_recovery_decisions_are_explicit() -> None:
    guide = _runtime_guide()

    assert "A clarification request completes its current invocation successfully" in guide
    assert "The user's answer starts a new invocation on the same thread" in guide
    assert "`input_required` is not a v1 lifecycle state" in guide
    assert "Polling `DurableInvocationPort.observe` is the supported durable evidence path" in guide
    assert "No event sink or broker is required for correctness" in guide
    assert "Repository process-loss simulation is not Kubernetes pod-recovery qualification" in guide


def test_declared_non_goals_are_complete_and_separate_from_defects() -> None:
    guide = _runtime_guide()

    for deferred in (
        "multi-replica Gateway coordination",
        "scheduler high availability",
        "event broker or push sink",
        "general channel extension contract",
        "context export and retirement",
        "synchronous-client durability",
        "speculative budget, deadline, resource, or effect ceilings",
        "PodDisruptionBudget and topology spread",
    ):
        assert deferred in guide


def test_storage_and_qualification_claims_name_their_evidence_boundary() -> None:
    guide = _runtime_guide()
    helm_guide = (_REPO_ROOT / "deploy" / "helm" / "deer-flow" / "README.md").read_text(encoding="utf-8")

    assert "`process_local` survives neither process restart nor pod loss" in guide
    assert "one Gateway replica" in guide
    assert "shared PostgreSQL" in guide
    assert "shared Redis" in guide
    assert "default skip and an unverified declaration are both unpassed release gates" in guide
    assert "process-loss simulation, image construction, and Helm rendering are separate evidence" in guide
    assert '`trust="operator_asserted"`' in guide
    assert "external_evidence_verified" in guide
    assert "operator copies only an artifact-bound passing" in helm_guide
    assert "external_evidence_verified" in helm_guide


def test_durable_examples_and_fixtures_use_generic_provider_names() -> None:
    expected_generic_fragments = {
        _REPO_ROOT / "config.example.yaml": ("mcp_interceptor:example.credential_broker", "example_request_observer"),
        _BACKEND_ROOT / "docs" / "MCP_SERVER.md": ("example_mcp_credentials", "example.credential_broker"),
        _BACKEND_ROOT / "tests" / "test_extension_app_loading.py": ("example_policy",),
        _BACKEND_ROOT / "tests" / "test_capability_manifest.py": ("example-policy", "example-observer"),
        _BACKEND_ROOT / "tests" / "test_capability_readiness.py": ("example-policy", "example-authority"),
        _BACKEND_ROOT / "tests" / "test_capability_host_authorization.py": ("example.authorization", "example-policy"),
        _BACKEND_ROOT / "tests" / "test_required_mcp_interceptors.py": ("example.plugin", "example-mcp"),
    }

    for path, fragments in expected_generic_fragments.items():
        rendered = path.read_text(encoding="utf-8")
        assert all(fragment in rendered for fragment in fragments), path
