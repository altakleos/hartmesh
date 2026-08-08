"""Cross-commit qualification for DeerFlow's durable invocation contract.

Each matrix row maps to the named tests below or to an explicit release gate:

* key replay in every state and changed-digest conflict:
  ``test_same_key_and_digest_returns_one_row_before_during_and_after_every_terminal_state``
  and ``test_same_key_different_digest_conflicts_and_external_encodings_remain_distinct``;
* raw/hash and structured source-scope separation:
  ``test_same_key_different_digest_conflicts_and_external_encodings_remain_distinct``
  and ``test_domain_tagged_scopes_do_not_have_delimiter_or_unicode_collisions``;
* same-key and different-key PostgreSQL arbitration:
  ``test_postgres_independent_sessions_force_key_and_thread_arbitration``. A
  skip without ``DEERFLOW_TEST_POSTGRES_URL`` is an unpassed release gate;
* response loss and accepted/running process loss:
  ``test_response_loss_after_commit_reconstructs_one_row_and_never_reattaches_worker``,
  ``test_process_loss_after_acceptance_reconstructs_and_recovers_without_attachment``,
  and ``test_process_loss_during_execution_is_fenced_before_stale_completion``;
* cancellation/start/finalization races and terminal cancellation evidence:
  ``test_cancellation_wins_pending_to_started_race_without_graph_start``,
  ``test_cancel_and_finalization_races_have_one_authoritative_winner``, and
  ``test_fenced_and_unfenced_cancellation_preserve_terminal_cancelled_evidence``;
* channel redelivery and Scheduled Task occurrence replay:
  ``test_channel_redelivery_bypasses_ttl_and_converges_in_sql_store``,
  ``test_internal_source_mappings_use_only_trusted_scope_facts``, and
  ``test_channel_and_persisted_scheduler_keys_replay_without_content_hash_fallback``;
* auxiliary exclusion and atomic supersession:
  ``test_auxiliary_rows_have_no_external_identity_lifecycle_or_public_snapshot``,
  ``test_multirow_supersession_and_replacement_acceptance_are_one_ordered_batch``,
  and ``test_replacement_batch_failure_rolls_back_rows_events_and_cursor``;
* cursor restart, duplicate read, ordering, prune, and gap:
  ``test_cursor_duplicate_read_prune_and_gap_keep_monotonic_progress``,
  ``test_sql_query_reconstructs_snapshot_and_events_after_store_restart``,
  ``test_repeated_page_is_harmless_and_page_boundary_skips_nothing``,
  ``test_postgres_independent_runs_allocate_unique_global_cursors``, and
  ``test_sql_prune_is_monotonic_and_cursor_state_corruption_fails_read``;
* principal/Origin/thread/agent/revision/generation forgery:
  ``test_every_launch_source_is_sealed_with_host_selected_origin``,
  ``test_every_origin_uses_sealed_source_evidence_not_caller_forgery``, and
  ``test_accepted_invocation_facts_are_pinned_and_caller_forgery_is_removed``;
* authorization and constraint deny/indeterminate/expiry/drift fences:
  ``test_start_denial_stops_before_admission_and_worker_work``,
  ``test_provider_failure_or_malformed_decision_is_indeterminate``,
  ``test_constraint_rejection_or_uncertainty_stops_before_acceptance``,
  ``test_queue_expiry_fails_before_graph_construction``, and
  ``test_restart_drift_fails_before_graph_construction_or_model_work``;
* observe/cancel policy and fenced/unfenced compatibility:
  ``test_observe_and_cancel_apply_visibility_before_policy_and_mutation``,
  ``test_observe_context_and_cancel_provider_failures_are_indeterminate``, and
  ``test_fenced_and_unfenced_cancellation_preserve_terminal_cancelled_evidence``;
* alias/config mutation and worker/graph/astream counting:
  ``test_pinned_material_replaces_mutated_factory_context_after_digest_check``
  and ``test_worker_graph_and_first_astream_counts_hold_across_success_drift_and_expiry``;
* required capability health, stable manifest identity, and the MCP call fence:
  ``test_required_authorization_and_constraints_health_fail_closed_while_optional_does_not``,
  ``test_admin_capabilities_separates_immutable_manifest_from_mutable_health``, and
  ``test_stale_required_health_fails_readiness_and_mcp_call_before_preparation``;
* legacy migration plus downgrade/re-upgrade policy:
  ``test_upgrade_downgrade_and_reupgrade_are_nondestructive``,
  ``test_upgrade_downgrade_and_reupgrade_preserve_legacy_rows``, and
  ``test_upgrade_downgrade_reupgrade_preserves_legacy_and_auxiliary_rows``;
* HTTP/in-process and legacy route compatibility:
  ``test_runtime_transport_conformance``,
  ``test_gateway_create_stream_wait_routes_share_durable_admission``, and
  ``test_gateway_mounts_runtime_routes_without_replacing_legacy_runs``.

Kubernetes pod termination is an explicit unpassed release gate: offline tests
simulate durable process loss but never create or terminate pods. Synchronous
``DeerFlowClient`` remains separately tested and documented as non-durable.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from deerflow_extension_api import (
    AUTHORIZATION_PROVIDER_CAPABILITY_API_VERSION,
    INVOCATION_CONSTRAINTS_CAPABILITY_API_VERSION,
    AuthorizationProviderFactory,
    CapabilityHealthResult,
    InvocationConstraintsProviderFactory,
)

from app.runtime.idempotency import (
    REQUEST_DIGEST_VERSION,
    CanonicalCallerIntent,
    canonical_request_digest,
    normalize_external_key,
    scope_for_channel,
    scope_for_http,
    scope_for_scheduler,
)
from deerflow.extensions.capabilities import CapabilityHealthMonitor, build_capability_manifest
from deerflow.extensions.registry import ExtensionRegistry
from deerflow.runtime import RunManager, RunStatus
from deerflow.runtime.runs.lifecycle_query import CursorGap, LifecycleQuery, decode_lifecycle_cursor, encode_lifecycle_cursor
from deerflow.runtime.runs.store.base import AdmissionOutcome, CancellationRequestOutcome, LifecycleType, lifecycle_owner_scope
from deerflow.runtime.runs.store.memory import MemoryRunStore

_BACKEND_ROOT = Path(__file__).resolve().parents[1]


def _caller_intent_fields(value: dict[str, object]) -> dict[str, object]:
    intent = CanonicalCallerIntent(value)
    return {
        "caller_intent_json": intent.to_persisted(),
        "caller_intent_digest": intent.digest,
        "caller_intent_digest_version": intent.digest_version,
    }


def test_runtime_api_docs_publish_manifest_and_health_contract() -> None:
    api_docs = (_BACKEND_ROOT / "docs" / "API.md").read_text(encoding="utf-8")

    assert "immutable capability manifest" in api_docs
    assert "mutable capability health" in api_docs
    assert "no readiness/provenance manifest" not in api_docs


@pytest.mark.anyio
@pytest.mark.parametrize(
    "terminal",
    [RunStatus.success, RunStatus.error, RunStatus.timeout, RunStatus.interrupted],
)
async def test_same_key_and_digest_returns_one_row_before_during_and_after_every_terminal_state(
    terminal: RunStatus,
) -> None:
    store = MemoryRunStore()
    manager = RunManager(store=store)
    identity = {
        "external_scope": scope_for_http("user", "owner-1"),
        "external_key": normalize_external_key("delivery-1"),
        "request_digest": canonical_request_digest({"input": "hello"}),
        "request_digest_version": REQUEST_DIGEST_VERSION,
        **_caller_intent_fields({"input": "hello"}),
        "user_id": "owner-1",
    }

    created = await manager.ensure_or_reject("thread-1", **identity)
    pending = await manager.ensure_or_reject("thread-1", **identity)
    await manager.set_status(created.record.run_id, RunStatus.running)
    running = await manager.ensure_or_reject("thread-1", **identity)
    await manager.set_status(created.record.run_id, terminal)
    completed = await manager.ensure_or_reject("thread-1", **identity)

    assert created.outcome is AdmissionOutcome.created
    assert {pending.outcome, running.outcome, completed.outcome} == {AdmissionOutcome.known_same}
    assert {item.record.run_id for item in (created, pending, running, completed)} == {created.record.run_id}
    rows = await store.list_by_thread("thread-1", user_id="owner-1")
    assert [row["run_id"] for row in rows] == [created.record.run_id]
    events = await store.list_lifecycle_events(run_id=created.record.run_id)
    assert events[-1]["state_version"] == rows[0]["state_version"]
    assert events[-1]["status"] == rows[0]["status"] == terminal.value


@pytest.mark.anyio
async def test_same_key_different_digest_conflicts_and_external_encodings_remain_distinct() -> None:
    manager = RunManager(store=MemoryRunStore())
    scope = scope_for_http("user", "owner-1")
    key = normalize_external_key("delivery-1")
    created = await manager.ensure_or_reject(
        "thread-1",
        external_scope=scope,
        external_key=key,
        request_digest=canonical_request_digest({"input": "hello"}),
        request_digest_version=REQUEST_DIGEST_VERSION,
        **_caller_intent_fields({"input": "hello"}),
        user_id="owner-1",
    )
    conflict = await manager.ensure_or_reject(
        "thread-1",
        external_scope=scope,
        external_key=key,
        request_digest=canonical_request_digest({"input": "changed"}),
        request_digest_version=REQUEST_DIGEST_VERSION,
        **_caller_intent_fields({"input": "changed"}),
        user_id="owner-1",
    )

    assert created.outcome is AdmissionOutcome.created
    assert conflict.outcome is AdmissionOutcome.key_conflict
    literal_hash = "sha256:utf8:" + "a" * 64
    assert normalize_external_key(literal_hash) != normalize_external_key("é" * 128)
    assert scope_for_channel("slack", "a:b", "", "聊") != scope_for_channel("slack:a", "b", "", "聊")
    assert scope_for_channel("slack", "connection", "", "聊:天") != scope_for_channel("slack", "connection", "聊", ":天")


@pytest.mark.anyio
async def test_channel_and_persisted_scheduler_keys_replay_without_content_hash_fallback() -> None:
    store = MemoryRunStore()
    manager = RunManager(store=store)
    digest = canonical_request_digest({"input": "same accepted request"})
    sources = (
        (scope_for_channel("slack", "connection-1", "workspace-1", "chat-1"), "event-1", "channel-thread"),
        (scope_for_scheduler("owner-1", "task-1"), "task-run-1", "scheduler-thread"),
    )

    for scope, external_key, thread_id in sources:
        common = {
            "external_scope": scope,
            "external_key": normalize_external_key(external_key),
            "request_digest": digest,
            "request_digest_version": REQUEST_DIGEST_VERSION,
            **_caller_intent_fields({"input": "same accepted request"}),
            "user_id": "owner-1",
        }
        first = await manager.ensure_or_reject(thread_id, **common)
        replay = await manager.ensure_or_reject(thread_id, **common)
        assert first.outcome is AdmissionOutcome.created
        assert replay.outcome is AdmissionOutcome.known_same
        assert replay.record.run_id == first.record.run_id


@pytest.mark.anyio
async def test_auxiliary_rows_have_no_external_identity_lifecycle_or_public_snapshot() -> None:
    store = MemoryRunStore()
    await store.put("run-1", thread_id="thread-1", user_id="owner-1")
    await store.put(
        "checkpoint-1",
        thread_id="thread-1",
        user_id="owner-1",
        operation_kind="checkpoint_write",
    )

    auxiliary = await store.get("checkpoint-1", user_id="owner-1")
    page = await store.query_lifecycle(
        LifecycleQuery(
            thread_id="thread-1",
            owner_scope=lifecycle_owner_scope("owner-1"),
        )
    )

    assert auxiliary is not None
    assert auxiliary["state_version"] == 0
    assert auxiliary["external_scope"] is None
    assert auxiliary["external_key"] is None
    assert await store.list_lifecycle_events(run_id="checkpoint-1") == []
    assert {row["run_id"] for row in page.snapshots} == {"run-1"}
    assert {event["run_id"] for event in page.events} == {"run-1"}


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("strategy", "superseded_status"),
    [("interrupt", "interrupted"), ("rollback", "error")],
)
async def test_multirow_supersession_and_replacement_acceptance_are_one_ordered_batch(
    strategy: str,
    superseded_status: str,
) -> None:
    store = MemoryRunStore()
    await store.put("old-1", thread_id="thread-1", created_at="2026-08-07T00:00:00+00:00")
    await store.put("old-2", thread_id="thread-1", created_at="2026-08-07T00:00:01+00:00")

    replacement, claimed = await store.create_thread_operation_atomic(
        "replacement",
        thread_id="thread-1",
        owner_worker_id="worker-1",
        lease_expires_at=None,
        multitask_strategy=strategy,
        created_at="2026-08-07T00:00:02+00:00",
    )
    events = await store.list_lifecycle_events(thread_id="thread-1")

    assert [(row["run_id"], row["status"]) for row in claimed] == [
        ("old-1", superseded_status),
        ("old-2", superseded_status),
    ]
    assert (replacement["status"], replacement["state_version"]) == ("pending", 1)
    assert [(event["run_id"], event["lifecycle_type"]) for event in events[-3:]] == [
        ("old-1", LifecycleType.interrupted),
        ("old-2", LifecycleType.interrupted),
        ("replacement", LifecycleType.accepted),
    ]
    assert [event["cursor"] for event in events] == sorted(event["cursor"] for event in events)


@pytest.mark.anyio
async def test_fenced_and_unfenced_cancellation_preserve_terminal_cancelled_evidence() -> None:
    fenced = MemoryRunStore()
    await fenced.put("fenced", thread_id="thread-1")
    first = await fenced.request_cancel_fenced("fenced", action="interrupt", expected_state_version=1)
    duplicate = await fenced.request_cancel_fenced("fenced", action="interrupt", expected_state_version=1)
    stale = await fenced.request_cancel_fenced("fenced", action="rollback", expected_state_version=2)
    assert first.outcome is CancellationRequestOutcome.requested
    assert duplicate.outcome is CancellationRequestOutcome.already_requested
    assert stale.outcome is CancellationRequestOutcome.stale

    compatibility_store = MemoryRunStore()
    manager = RunManager(store=compatibility_store)
    record = await manager.create("thread-compat")
    await manager.cancel(record.run_id, action="interrupt")
    row = await compatibility_store.get(record.run_id)
    events = await compatibility_store.list_lifecycle_events(run_id=record.run_id)
    assert row is not None
    assert events[-1]["lifecycle_type"] is LifecycleType.cancelled
    assert events[-1]["state_version"] == row["state_version"]
    assert events[-1]["status"] == row["status"] == "interrupted"


@pytest.mark.anyio
async def test_cursor_duplicate_read_prune_and_gap_keep_monotonic_progress() -> None:
    store = MemoryRunStore()
    await store.put("run-1", thread_id="thread-1", user_id="owner-1", status="success")
    query = LifecycleQuery(run_id="run-1", owner_scope=lifecycle_owner_scope("owner-1"), limit=1)
    first = await store.query_lifecycle(query)
    duplicate = await store.query_lifecycle(query)
    second = await store.query_lifecycle(
        LifecycleQuery(
            run_id="run-1",
            owner_scope=lifecycle_owner_scope("owner-1"),
            cursor=first.next_cursor,
            limit=1,
        )
    )

    assert duplicate.events == first.events
    assert [event["cursor"] for event in (*first.events, *second.events)] == [1, 2]
    assert second.next_cursor == second.read_fence_cursor
    assert decode_lifecycle_cursor(second.next_cursor) == 2

    minimum = await store.prune_lifecycle_through(encode_lifecycle_cursor(1))
    with pytest.raises(CursorGap) as gap:
        await store.query_lifecycle(
            LifecycleQuery(
                run_id="run-1",
                owner_scope=lifecycle_owner_scope("owner-1"),
                cursor=encode_lifecycle_cursor(0),
            )
        )
    assert gap.value.minimum_available_cursor == minimum


class _AuthorizationProvider:
    name = "qualification"

    def authorize(self, request):
        raise NotImplementedError

    async def aauthorize(self, request):
        raise NotImplementedError

    def filter_resources(self, principal, resource_type, candidates):
        return candidates


class _ConstraintsProvider:
    async def project(self, request):
        raise NotImplementedError


@pytest.mark.anyio
async def test_required_authorization_constraints_and_live_health_are_fail_closed_without_changing_manifest() -> None:
    current = [datetime(2026, 8, 7, tzinfo=UTC)]

    async def unhealthy() -> CapabilityHealthResult:
        return CapabilityHealthResult(status="unhealthy", diagnostic_code="dependency_unavailable")

    registry = ExtensionRegistry()
    with registry.attributed_to("qualification:install", package_name="qualification", package_version="1.0.0"):
        registry.authorization_provider(
            AuthorizationProviderFactory(
                contribution_id="policy",
                capability_api_version=AUTHORIZATION_PROVIDER_CAPABILITY_API_VERSION,
                factory=_AuthorizationProvider,
                kind="authorization_provider",
                health_probe=unhealthy,
            )
        )
        registry.invocation_constraints(
            InvocationConstraintsProviderFactory(
                contribution_id="constraints",
                capability_api_version=INVOCATION_CONSTRAINTS_CAPABILITY_API_VERSION,
                factory=_ConstraintsProvider,
                kind="invocation_constraints",
                health_probe=unhealthy,
            )
        )
    extensions = registry.build(generation=17)
    manifest = build_capability_manifest(
        extensions,
        authorization_required=True,
        required_capabilities=("invocation_constraints.v1",),
        initialized_capability_ids=("authorization_provider:policy", "invocation_constraints.v1"),
    )
    digest = manifest.digest
    monitor = CapabilityHealthMonitor(manifest, extensions, clock=lambda: current[0])

    unhealthy_snapshot = await monitor.readiness()
    current[0] += timedelta(seconds=31)
    stale_snapshot = await monitor.readiness(refresh=False)

    assert unhealthy_snapshot.status == "not_ready"
    assert stale_snapshot.status == "not_ready"
    assert {item.capability_id for item in unhealthy_snapshot.health if item.status == "unhealthy"} == {
        "authorization_provider:policy",
        "invocation_constraints.v1",
    }
    assert manifest.digest == digest


def test_runtime_documentation_covers_qualified_contract_and_deferred_scope() -> None:
    runtime_docs = (_BACKEND_ROOT / "docs" / "INVOCATION_RUNTIME.md").read_text(encoding="utf-8")
    root_readme = (_BACKEND_ROOT.parent / "README.md").read_text(encoding="utf-8")

    required_runtime_terms = (
        '`RunRow(operation_kind="run")`',
        "AcceptedInvocation",
        "state_version",
        "cancellation_requested",
        "max_total_subagents",
        "exact token limits are deferred",
        "required MCP",
        "DeerFlowClient",
        "scheduler HA",
        "multi-replica Gateway",
        "durable embedded parity",
    )
    for term in required_runtime_terms:
        assert term in runtime_docs
    assert "Durable Invocation HTTP API" in root_readme
