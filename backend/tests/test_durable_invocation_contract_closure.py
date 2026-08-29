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
        "Trusted contributor and hydrated evidence",
        "Restrictive authorization and constraints",
        "Pinned agent and extension material",
        "Bound actual agent assembly",
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
        "Canonical keyed replay": {
            "test_cancelled_atomic_admission_reconciles_commit_before_return",
            "test_cancelled_atomic_unique_failure_does_not_retry_admission",
            "test_cancelled_known_replay_does_not_close_retained_run",
            "test_cancelled_reservation_releases_commit_before_return",
        },
        "Keyed native ingress receipts": {
            "test_received_payload_survives_process_loss_before_claim",
            "test_thread_contention_outlives_poison_budget_and_preserves_fifo",
            "test_poison_receipt_dead_letters_without_exposing_exception_text",
            "test_response_loss_after_admission_replays_known_run_before_binding",
            "test_signed_route_reaches_real_runtime_and_redelivery_replays",
        },
        "Trusted contributor and hydrated evidence": {
            "test_hydration_recomputes_bound_effective_and_trusted_evidence",
            "test_store_reconstruction_rejects_corrupt_accepted_evidence_before_recovery",
            "test_external_replay_lookup_rejects_corrupt_accepted_evidence_with_stable_error",
        },
        "Restrictive authorization and constraints": {
            "test_task_dispatch_inflight_equal_replay_waits_for_one_physical_start",
            "test_create_app_fails_closed_for_malformed_required_v2_constraints_provider",
        },
        "Pinned agent and extension material": {
            "test_remote_v1_material_receipt_is_compatibility_only",
            "test_accepted_pod_isolation_digest_binds_every_pod_security_field",
            "test_v2_execution_fence_rereads_every_supporting_resource",
        },
        "Bound actual agent assembly": {
            "test_accepted_durable_evidence_is_bound_before_checkpoint_access_and_astream",
            "test_recovered_assembly_must_match_original_before_astream",
            "test_ownership_loss_during_evidence_bind_does_not_terminalize_new_owner",
            "test_lifecycle_summary_does_not_verify_evidence_from_another_accepted_run",
        },
        "Transactional lifecycle evidence": {
            "test_lifecycle_readiness_rejects_deleted_interior_event_without_scanning",
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
    assert "The user's answer starts a new invocation on the same DeerFlow thread" in guide
    assert "`input_required` is not a v1 lifecycle state" in guide
    assert "Polling `DurableInvocationPort.observe` is the supported durable evidence path" in guide
    assert "No event sink or broker is required for correctness" in guide
    assert "Repository process-loss simulation is not Kubernetes pod-recovery qualification" in guide


def test_runtime_semantics_are_consistent_across_public_and_model_facing_docs() -> None:
    guide = _runtime_guide()
    root_readme = (_REPO_ROOT / "README.md").read_text(encoding="utf-8")
    api_guide = (_BACKEND_ROOT / "docs" / "API.md").read_text(encoding="utf-8")
    runtime_api_guide = (_BACKEND_ROOT / "packages" / "runtime-api" / "README.md").read_text(encoding="utf-8")
    runtime_api_contract = (_BACKEND_ROOT / "packages" / "runtime-api" / "deerflow_runtime_api" / "__init__.py").read_text(encoding="utf-8")
    extension_api_guide = (_BACKEND_ROOT / "packages" / "extension-api" / "README.md").read_text(encoding="utf-8")
    backend_guide = (_BACKEND_ROOT / "AGENTS.md").read_text(encoding="utf-8")
    clarification_middleware = (_BACKEND_ROOT / "packages" / "harness" / "deerflow" / "agents" / "middlewares" / "clarification_middleware.py").read_text(encoding="utf-8")
    clarification_tool = (_BACKEND_ROOT / "packages" / "harness" / "deerflow" / "tools" / "builtins" / "clarification_tool.py").read_text(encoding="utf-8")
    lead_prompt = (_BACKEND_ROOT / "packages" / "harness" / "deerflow" / "agents" / "lead_agent" / "prompt.py").read_text(encoding="utf-8")
    normalized_guide = " ".join(guide.split())
    normalized_runtime_api_contract = " ".join(runtime_api_contract.split())

    for required in (
        "canonical caller intent",
        "accepted effective execution",
        "does not rerun contributors, authorization, constraints, default resolution, agent/profile routing, or model execution",
        "Cursor polling of transactional lifecycle rows is the authoritative v1 evidence path",
        "optional at-least-once acceleration",
        "same DeerFlow thread, checkpoints, memory, workspace, and conversation context",
        "one startup-frozen process generation",
        "does not coordinate simultaneous generations or rolling replicas",
        "terminalized as failed with `stop_reason=orphan_recovered`",
        "does not resume model execution",
        "A product retry is a new invocation under the new process generation",
        "Ingress receipt boundary",
        "Admission boundary",
        "Execution boundary",
        "Observation boundary",
        "Outbound delivery boundary",
        "does not promise exactly-once model execution",
        "application-hosted in-process `InvocationRuntime` Adapter",
        "local, non-durable embedded `DeerFlowClient`",
        "opt-in deterministic fault injection",
        "disabled in ordinary processes",
    ):
        assert required in normalized_guide

    assert "canonical caller intent" in runtime_api_guide
    assert "accepted effective execution" in runtime_api_guide
    assert "canonical caller intent" in normalized_runtime_api_contract
    assert "accepted effective execution" in normalized_runtime_api_contract
    assert "authoritative v1 evidence path" in normalized_runtime_api_contract
    assert "new invocation on the same DeerFlow thread" in root_readme
    assert "new invocation on the same DeerFlow thread" in api_guide
    assert "one startup-frozen process generation" in extension_api_guide
    assert "one startup-frozen process generation" in backend_guide
    assert "does not enter `InvocationRuntime`" in backend_guide

    for model_facing_text in (clarification_middleware, clarification_tool, lead_prompt):
        normalized_model_text = " ".join(model_facing_text.split())
        assert "ends the current invocation successfully" in normalized_model_text
        assert "new invocation on the same DeerFlow thread" in normalized_model_text

    for stale_clarification_claim in (
        "Waits for user response before continuing",
        "Wait for the user's response before continuing",
        "[Execution stops - wait for user response]",
    ):
        assert stale_clarification_claim not in clarification_middleware
        assert stale_clarification_claim not in clarification_tool
        assert stale_clarification_claim not in lead_prompt


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


def test_post_commit_obligation_telemetry_names_its_process_boundary() -> None:
    guide = " ".join(_runtime_guide().split())

    assert "optional `post_commit_obligations` field" in guide
    assert "Quarantine is not a third workload bucket" in guide
    assert "These counters reset on restart" in guide
    assert "neither durable lifecycle evidence nor a cross-replica total" in guide
    assert "serialized v1 readiness reason `admission_compensation_pending` remains" in guide


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
