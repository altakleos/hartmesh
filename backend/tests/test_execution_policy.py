"""Public contracts for accepted execution policy and private equivalence state."""

from __future__ import annotations

import base64
import json

import pytest
from deerflow_extension_api import TenantReferenceV1
from pydantic import ValidationError

from deerflow.config.execution_policy_config import ExecutionPolicyConfig
from deerflow.runtime.accepted_invocation import (
    AcceptedInvocation,
    InvocationOrigin,
    PrincipalProjection,
    ResolvedAgentMaterialV1,
    ResolvedAgentRevision,
    canonical_digest,
)
from deerflow.runtime.execution_policy import (
    ExecutionBudgetV1,
    ExecutionPolicyEvaluator,
    ExecutionPolicyObservationV1,
    ExecutionPolicyStateV1,
    PolicyDecision,
    ToolEquivalenceKeyring,
    build_tool_equivalence_commitment,
    normalizer_manifest_digest,
    resolve_execution_budget,
)


def _key(character: str) -> str:
    return base64.urlsafe_b64encode(character.encode("ascii") * 32).decode("ascii").rstrip("=")


def _keyring(*, active: str = "current-v1") -> ToolEquivalenceKeyring:
    result = ToolEquivalenceKeyring.from_environment(
        required=True,
        environ={
            "EXECUTION_POLICY_HMAC_KEYS": json.dumps(
                {
                    "old-v1": _key("o"),
                    "current-v1": _key("n"),
                }
            ),
            "EXECUTION_POLICY_HMAC_ACTIVE_KEY_ID": active,
        },
    )
    assert result is not None
    return result


def test_execution_budget_is_canonical_and_caller_can_only_narrow() -> None:
    left = ExecutionBudgetV1.build(
        profile="interactive",
        equivalence_key_id="current-v1",
        per_tool_category_attempts={"retrieval": 12, "sandbox": 20},
    )
    right = ExecutionBudgetV1.build(
        profile="interactive",
        equivalence_key_id="current-v1",
        per_tool_category_attempts={"sandbox": 20, "retrieval": 12},
    )

    assert left == right
    assert left.digest == right.digest
    assert ExecutionBudgetV1.from_json(left.to_json()) == left

    narrowed = left.narrow(
        {
            "max_agent_turns": 100,
            "max_total_tool_attempts": 200,
            "max_retrieval_bytes": 10_000,
        }
    )
    assert narrowed.max_agent_turns == 100
    assert narrowed.max_total_tool_attempts == 200
    assert narrowed.max_retrieval_bytes == 10_000

    with pytest.raises(ValueError, match="execution_budget_broadening_forbidden"):
        left.narrow({"max_agent_turns": left.max_agent_turns + 1})
    with pytest.raises(ValueError, match="execution_budget_field_forbidden"):
        left.narrow({"equivalence_key_id": "attacker"})


def test_category_narrowing_preserves_omitted_server_ceilings() -> None:
    budget = ExecutionBudgetV1.build(
        per_tool_category_attempts={"retrieval": 20, "sandbox": 10},
    )

    narrowed = budget.narrow(
        {"per_tool_category_attempts": {"retrieval": 5}},
    )

    assert dict(narrowed.per_tool_category_attempts) == {
        "retrieval": 5,
        "sandbox": 10,
    }


def test_policy_evaluator_is_pure_and_emits_one_threshold_decision() -> None:
    budget = ExecutionBudgetV1.build(
        equivalence_key_id="current-v1",
        repeated_tool_warn=2,
        repeated_tool_stop=3,
        max_total_tool_attempts=5,
    )
    evaluator = ExecutionPolicyEvaluator()
    state = ExecutionPolicyStateV1.initial(budget)
    observation = ExecutionPolicyObservationV1.tool_attempt(
        tool_name="read_file",
        tool_category="filesystem",
        equivalence_commitment="a" * 64,
    )

    first = evaluator.evaluate(budget, state, observation)
    second = evaluator.evaluate(budget, first.next_state, observation)
    stopped = evaluator.evaluate(budget, second.next_state, observation)
    duplicate_stop = evaluator.evaluate(budget, stopped.next_state, observation)

    assert first.decision is PolicyDecision.allow
    assert second.decision is PolicyDecision.warn
    assert second.reason_code == "repeated_tool_loop"
    assert stopped.decision is PolicyDecision.stop
    assert stopped.reason_code == "repeated_tool_loop"
    assert duplicate_stop.decision is PolicyDecision.stop
    assert duplicate_stop.durable_event_required is False
    assert evaluator.evaluate(budget, state, observation) == first


def test_policy_state_round_trips_without_weakening_its_digest() -> None:
    budget = ExecutionBudgetV1.build(equivalence_key_id="current-v1")
    evaluated = ExecutionPolicyEvaluator().evaluate(
        budget,
        ExecutionPolicyStateV1.initial(budget),
        ExecutionPolicyObservationV1.tool_attempt(
            tool_name="read_file",
            tool_category="filesystem",
            equivalence_commitment="a" * 64,
        ),
    )

    persisted = evaluated.next_state.to_json()
    assert ExecutionPolicyStateV1.from_json(persisted) == evaluated.next_state
    persisted["turns"] = 99
    with pytest.raises(ValueError, match="execution_policy_digest_invalid"):
        ExecutionPolicyStateV1.from_json(persisted)


def test_policy_decision_outbox_survives_crash_until_fenced_publication() -> None:
    budget = ExecutionBudgetV1.build(max_agent_turns=2)
    warning = ExecutionPolicyEvaluator().evaluate(
        budget,
        ExecutionPolicyStateV1.initial(budget),
        ExecutionPolicyObservationV1(kind="turn"),
    )

    assert warning.durable_event_required is True
    assert len(warning.next_state.decision_outbox) == 1
    pending = warning.next_state.decision_outbox[0]
    assert pending.decision is PolicyDecision.warn
    assert pending.state_digest == warning.next_state.digest
    assert ExecutionPolicyStateV1.from_json(warning.next_state.to_json()) == warning.next_state

    corrupted = warning.next_state.to_json()
    corrupted["decision_outbox"][0]["reason_code"] = "different"  # type: ignore[index]
    with pytest.raises(ValueError, match="execution_policy_digest_invalid"):
        ExecutionPolicyStateV1.from_json(corrupted)


def test_retrieval_result_observation_is_idempotent_and_enforces_aggregate_limit() -> None:
    budget = ExecutionBudgetV1.build(
        max_retrieval_calls=2,
        max_retrieval_results=3,
        max_retrieval_sources=3,
    )
    evaluator = ExecutionPolicyEvaluator()
    state = evaluator.evaluate(
        budget,
        ExecutionPolicyStateV1.initial(budget),
        ExecutionPolicyObservationV1(kind="retrieval", count=1),
    ).next_state
    results = ExecutionPolicyObservationV1(
        kind="retrieval",
        count=0,
        result_count=3,
        source_count=2,
        observation_id="ro_" + ("a" * 64),
    )

    stopped = evaluator.evaluate(budget, state, results)
    replayed = evaluator.evaluate(budget, stopped.next_state, results)

    assert stopped.decision is PolicyDecision.stop
    assert stopped.reason_code == "retrieval_budget_exhausted"
    assert stopped.next_state.retrieval_calls == 1
    assert stopped.next_state.retrieval_results == 3
    assert stopped.next_state.retrieval_sources == 2
    assert replayed.next_state == stopped.next_state
    assert replayed.durable_event_required is False


@pytest.mark.parametrize(
    ("budget_limits", "observation", "reason"),
    [
        ({"max_agent_turns": 1}, ExecutionPolicyObservationV1(kind="turn"), "turn_budget_exhausted"),
        (
            {"max_total_tool_attempts": 1},
            ExecutionPolicyObservationV1.tool_attempt(
                tool_name="read_file",
                tool_category="filesystem",
                equivalence_commitment=None,
            ),
            "tool_attempt_budget_exhausted",
        ),
        ({"max_no_progress_observations": 1}, ExecutionPolicyObservationV1(kind="no_progress"), "no_progress_loop"),
        ({"max_batches": 1}, ExecutionPolicyObservationV1(kind="batch"), "batch_count_budget_exhausted"),
        ({"max_retrieval_calls": 1}, ExecutionPolicyObservationV1(kind="retrieval"), "retrieval_budget_exhausted"),
        ({"max_sandbox_operations": 1}, ExecutionPolicyObservationV1(kind="sandbox"), "sandbox_operation_budget_exhausted"),
    ],
)
def test_each_supported_observation_has_a_stable_stop_reason(
    budget_limits: dict[str, int],
    observation: ExecutionPolicyObservationV1,
    reason: str,
) -> None:
    budget = ExecutionBudgetV1.build(**budget_limits)
    decision = ExecutionPolicyEvaluator().evaluate(
        budget,
        ExecutionPolicyStateV1.initial(budget),
        observation,
    )
    assert decision.decision is PolicyDecision.stop
    assert decision.reason_code == reason


@pytest.mark.parametrize(
    ("budget_limits", "observation", "reason"),
    [
        ({"max_agent_turns": 2}, ExecutionPolicyObservationV1(kind="turn"), "turn_budget_exhausted"),
        (
            {"max_total_tool_attempts": 2},
            ExecutionPolicyObservationV1.tool_attempt(
                tool_name="read_file",
                tool_category="filesystem",
                equivalence_commitment=None,
            ),
            "tool_attempt_budget_exhausted",
        ),
        ({"max_batches": 2}, ExecutionPolicyObservationV1(kind="batch"), "batch_count_budget_exhausted"),
        ({"max_retrieval_calls": 2}, ExecutionPolicyObservationV1(kind="retrieval"), "retrieval_budget_exhausted"),
        ({"max_sandbox_operations": 2}, ExecutionPolicyObservationV1(kind="sandbox"), "sandbox_operation_budget_exhausted"),
    ],
)
def test_supported_budgets_emit_a_bounded_warning_before_stop(
    budget_limits: dict[str, int],
    observation: ExecutionPolicyObservationV1,
    reason: str,
) -> None:
    budget = ExecutionBudgetV1.build(**budget_limits)
    warning = ExecutionPolicyEvaluator().evaluate(
        budget,
        ExecutionPolicyStateV1.initial(budget),
        observation,
    )

    assert warning.decision is PolicyDecision.warn
    assert warning.reason_code == reason
    assert warning.durable_event_required is True


def test_private_tool_commitments_preserve_values_and_normalize_read_ranges() -> None:
    keyring = _keyring()
    common = {
        "tenant_digest": "a" * 64,
        "run_ref": "run-public-reference",
        "keyring": keyring,
        "key_id": "current-v1",
    }

    left = build_tool_equivalence_commitment(
        tool_name="read_file",
        arguments={"path": "/tmp/alpha", "start_line": 1, "end_line": 50},
        **common,
    )
    equivalent = build_tool_equivalence_commitment(
        tool_name="read_file",
        arguments={"path": "/tmp/alpha", "start_line": 2, "end_line": 199},
        **common,
    )
    different_path = build_tool_equivalence_commitment(
        tool_name="read_file",
        arguments={"path": "/tmp/bravo", "start_line": 1, "end_line": 50},
        **common,
    )
    different_key = build_tool_equivalence_commitment(
        tool_name="read_file",
        arguments={"path": "/tmp/alpha", "start_line": 1, "end_line": 50},
        **{**common, "key_id": "old-v1"},
    )

    assert left is not None
    assert equivalent is not None
    assert left.digest == equivalent.digest
    assert left.digest != different_path.digest
    assert left.digest != different_key.digest
    assert left.normalizer_manifest_digest == normalizer_manifest_digest()
    assert "alpha" not in repr(left)


def test_secret_shaped_or_unclassifiable_arguments_are_excluded() -> None:
    keyring = _keyring()
    common = {
        "tenant_digest": "a" * 64,
        "run_ref": "run-public-reference",
        "tool_name": "community_tool",
        "keyring": keyring,
        "key_id": "current-v1",
    }

    assert (
        build_tool_equivalence_commitment(
            arguments={"query": "safe", "api_key": "secret"},
            **common,
        )
        is None
    )
    assert (
        build_tool_equivalence_commitment(
            arguments={"query": object()},
            **common,
        )
        is None
    )
    assert (
        build_tool_equivalence_commitment(
            arguments={"headers": {"X-Api-Key": "secret"}},
            **common,
        )
        is None
    )
    assert (
        build_tool_equivalence_commitment(
            arguments={"query": "safe"},
            **common,
        )
        is None
    )


def test_typed_search_normalizer_preserves_same_length_query_values() -> None:
    common = {
        "tenant_digest": "a" * 64,
        "run_ref": "run-public-reference",
        "tool_name": "web_search",
        "keyring": _keyring(),
        "key_id": "current-v1",
    }

    left = build_tool_equivalence_commitment(
        arguments={"query": "alpha", "max_results": 5},
        **common,
    )
    right = build_tool_equivalence_commitment(
        arguments={"query": "bravo", "max_results": 5},
        **common,
    )

    assert left is not None
    assert right is not None
    assert left.digest != right.digest


def test_missing_historical_key_fails_closed() -> None:
    keyring = _keyring()
    with pytest.raises(ValueError, match="policy_equivalence_key_unavailable"):
        build_tool_equivalence_commitment(
            tenant_digest="a" * 64,
            run_ref="run-public-reference",
            tool_name="read_file",
            arguments={"path": "/tmp/a"},
            keyring=keyring,
            key_id="missing-v1",
        )


def test_keyring_rejects_duplicate_json_key_ids() -> None:
    with pytest.raises(ValueError, match="execution_policy_keyring_invalid"):
        ToolEquivalenceKeyring.from_environment(
            required=True,
            environ={
                "EXECUTION_POLICY_HMAC_KEYS": ('{"same-v1":"' + _key("a") + '","same-v1":"' + _key("b") + '"}'),
                "EXECUTION_POLICY_HMAC_ACTIVE_KEY_ID": "same-v1",
            },
        )


def test_accepted_invocation_persists_and_validates_execution_budget() -> None:
    budget = ExecutionBudgetV1.build(equivalence_key_id="current-v1")
    material = ResolvedAgentMaterialV1(
        agent_id="default",
        storage_source="file",
        storage_version="v1",
        agent_config=None,
        soul="test",
        model_profile={"name": "default", "model": "test"},
        tool_groups=(),
        tools=(),
        skills=(),
        runtime_defaults={},
    )
    accepted = AcceptedInvocation.seal(
        principal=PrincipalProjection(user_id="user-1"),
        origin=InvocationOrigin(source_kind="http"),
        tenant=TenantReferenceV1(
            version=1,
            public_ref="tenant-aaaaaaaaaaaaaaaa",
            digest="a" * 64,
        ),
        thread_id="thread-policy",
        context_references={},
        agent_revision=ResolvedAgentRevision.from_material(material),
        normalized_input={},
        execution_options={},
        extension_generation=0,
        contributor_execution_digest=canonical_digest({"version": 1, "execution": []}),
        execution_budget=budget,
    )

    assert accepted.execution_budget == budget
    assert (
        AcceptedInvocation.from_persisted(
            {
                **accepted.to_persisted(),
                "thread_id": accepted.thread_id,
                "kwargs": {},
            }
        ).execution_budget
        == budget
    )

    forged = accepted.to_persisted()
    forged["decision_evidence_json"]["execution_budget"]["max_agent_turns"] += 1
    with pytest.raises(ValueError, match="execution policy"):
        AcceptedInvocation.from_persisted({**forged, "thread_id": accepted.thread_id, "kwargs": {}})


def test_policy_config_resolves_server_caps_and_scheduler_profile() -> None:
    config = ExecutionPolicyConfig(
        max_agent_turns=900,
        max_total_tool_attempts=800,
        scheduler_max_agent_turns=100,
        scheduler_max_total_tool_attempts=200,
    )
    keyring = _keyring()

    interactive = resolve_execution_budget(
        config,
        keyring=keyring,
        max_recursion_limit=500,
        non_interactive=False,
    )
    scheduled = resolve_execution_budget(
        config,
        keyring=keyring,
        max_recursion_limit=500,
        non_interactive=True,
    )

    assert interactive.max_agent_turns == 500
    assert interactive.max_total_tool_attempts == 800
    assert scheduled.profile == "scheduled-v1"
    assert scheduled.max_agent_turns == 100
    assert scheduled.max_total_tool_attempts == 200
    assert scheduled.equivalence_key_id == keyring.active_key_id

    with pytest.raises(ValidationError):
        ExecutionPolicyConfig(repeated_tool_warn=5, repeated_tool_stop=4)


def test_sandbox_category_tool_attempts_enforce_the_sandbox_operation_budget() -> None:
    budget = ExecutionBudgetV1.build(max_sandbox_operations=2)
    evaluator = ExecutionPolicyEvaluator()
    observation = ExecutionPolicyObservationV1.tool_attempt(
        tool_name="bash",
        tool_category="sandbox",
        equivalence_commitment=None,
    )

    warning = evaluator.evaluate(budget, ExecutionPolicyStateV1.initial(budget), observation)
    stopped = evaluator.evaluate(budget, warning.next_state, observation)

    assert warning.decision is PolicyDecision.warn
    assert warning.reason_code == "sandbox_operation_budget_exhausted"
    assert warning.next_state.sandbox_operations == 1
    assert stopped.decision is PolicyDecision.stop
    assert stopped.reason_code == "sandbox_operation_budget_exhausted"
    assert stopped.next_state.sandbox_operations == 2

    filesystem = evaluator.evaluate(
        budget,
        ExecutionPolicyStateV1.initial(budget),
        ExecutionPolicyObservationV1.tool_attempt(
            tool_name="read_file",
            tool_category="filesystem_read",
            equivalence_commitment=None,
        ),
    )
    assert filesystem.next_state.sandbox_operations == 0


def test_key_rotation_preserves_recovery_for_old_accepted_runs() -> None:
    before_rotation = ToolEquivalenceKeyring.from_environment(
        required=True,
        environ={
            "EXECUTION_POLICY_HMAC_KEYS": json.dumps({"old-v1": _key("o")}),
            "EXECUTION_POLICY_HMAC_ACTIVE_KEY_ID": "old-v1",
        },
    )
    after_rotation = _keyring(active="current-v1")
    assert before_rotation is not None

    accepted_budget = ExecutionBudgetV1.build(equivalence_key_id="old-v1")
    common = {
        "tenant_digest": "a" * 64,
        "run_ref": "run-public-reference",
        "tool_name": "read_file",
        "arguments": {"path": "/tmp/alpha", "start_line": 1, "end_line": 50},
    }

    original = build_tool_equivalence_commitment(
        keyring=before_rotation,
        key_id=accepted_budget.equivalence_key_id,
        **common,
    )
    # Rotation is additive: the accepted run recovers under its pinned key ID
    # and reproduces identical commitments, while a new admission pins the
    # rotated active key and produces different private state.
    after_rotation.require_key(accepted_budget.equivalence_key_id)
    recovered = build_tool_equivalence_commitment(
        keyring=after_rotation,
        key_id=accepted_budget.equivalence_key_id,
        **common,
    )
    fresh_admission = build_tool_equivalence_commitment(
        keyring=after_rotation,
        key_id=after_rotation.active_key_id,
        **common,
    )

    assert original is not None and recovered is not None and fresh_admission is not None
    assert recovered.digest == original.digest
    assert recovered.key_id == "old-v1"
    assert fresh_admission.key_id == "current-v1"
    assert fresh_admission.digest != original.digest

    dropped_history = ToolEquivalenceKeyring.from_environment(
        required=True,
        environ={
            "EXECUTION_POLICY_HMAC_KEYS": json.dumps({"current-v1": _key("n")}),
            "EXECUTION_POLICY_HMAC_ACTIVE_KEY_ID": "current-v1",
        },
    )
    assert dropped_history is not None
    with pytest.raises(ValueError, match="policy_equivalence_key_unavailable"):
        dropped_history.require_key(accepted_budget.equivalence_key_id)


def test_low_entropy_query_dictionary_cannot_be_correlated_without_the_key() -> None:
    import hashlib

    dictionary = ["weather", "news", "password reset", "hello", "stock price", "ok"]
    common = {
        "tenant_digest": "a" * 64,
        "run_ref": "run-public-reference",
        "tool_name": "web_search",
        "key_id": "current-v1",
    }
    left_keyring = _keyring()
    right_keyring = ToolEquivalenceKeyring.from_environment(
        required=True,
        environ={
            "EXECUTION_POLICY_HMAC_KEYS": json.dumps({"current-v1": _key("x")}),
            "EXECUTION_POLICY_HMAC_ACTIVE_KEY_ID": "current-v1",
        },
    )
    assert right_keyring is not None

    for query in dictionary:
        arguments = {"query": query}
        left = build_tool_equivalence_commitment(arguments=arguments, keyring=left_keyring, **common)
        right = build_tool_equivalence_commitment(arguments=arguments, keyring=right_keyring, **common)
        assert left is not None and right is not None
        # An attacker holding a candidate dictionary but no deployment key
        # cannot reproduce the commitment from the query alone.
        for candidate in (
            hashlib.sha256(query.encode()).hexdigest(),
            hashlib.sha256(json.dumps(arguments, sort_keys=True).encode()).hexdigest(),
        ):
            assert left.digest != candidate
        # Changing the secret key changes every commitment.
        assert left.digest != right.digest
        assert query not in repr(left)
