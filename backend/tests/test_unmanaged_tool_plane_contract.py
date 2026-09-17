"""The unmanaged tool-plane state is a decision, and only a supported one.

The composed regression (``test_unmanaged_retrieval_gateway_stream_e2e``)
proves the default profile can run its configured search tool. These are the
neighbouring states: the governed path is unchanged, the ungoverned one cannot
be reached by a deployment that promises governance, and neither can be turned
into the other by recovery, delegation or a hand-built context.
"""

from __future__ import annotations

import pytest

from deerflow.retrieval import RetrievalEvidenceError, resolve_tool_plane_provenance
from deerflow.retrieval.contracts import RetrievalObservationDraftV1
from deerflow.runtime.accepted_invocation import (
    AcceptedInvocation,
    InvocationOrigin,
    PrincipalProjection,
    canonical_digest,
)
from deerflow.runtime.agent_revision import ResolvedAgentMaterialV1, ResolvedAgentRevision
from deerflow.runtime.tenant_identity import TenantIdentityV1
from deerflow.tool_plane.contracts import EffectiveToolPlaneRevisionV1

_TENANT = TenantIdentityV1.from_canonical_id("local").to_persisted_reference()
_GOVERNED_DIGESTS = {
    "base_revision_digest": "a" * 64,
    "user_overlay_digest": "b" * 64,
    "projection_digest": "c" * 64,
    "effective_digest": "d" * 64,
}
_UNMANAGED = {
    "version": 1,
    "deployment_profile": "local_development",
    "governance_state": "tool_plane_bootstrap_required",
}


def _material() -> ResolvedAgentMaterialV1:
    return ResolvedAgentMaterialV1(
        agent_id="lead-agent",
        storage_source="test",
        storage_version="1",
        agent_config=None,
        soul="",
        model_profile={},
    )


def _common() -> dict[str, object]:
    return {
        "principal": PrincipalProjection(user_id="u1", role="member"),
        "origin": InvocationOrigin(source_kind="http"),
        "thread_id": "thread-unmanaged",
        "context_references": {},
        "agent_revision": ResolvedAgentRevision.from_material(_material()),
        "normalized_input": {"messages": []},
        "execution_options": {"multitask_strategy": "reject"},
        "extension_generation": 7,
        "contributor_execution_digest": canonical_digest({"version": 1, "execution": []}),
        "tenant": _TENANT,
    }


def _governed_revision() -> dict[str, object]:
    return EffectiveToolPlaneRevisionV1(
        base_revision_digest="c" * 64,
        user_overlay_digest="d" * 64,
        base_generation=4,
        overlay_generation=2,
        projection_digest="e" * 64,
    ).to_json()


def test_the_unmanaged_state_is_sealed_into_the_accepted_record() -> None:
    accepted = AcceptedInvocation.seal(**_common(), tool_plane_unmanaged=_UNMANAGED)
    ungoverned = AcceptedInvocation.seal(**_common())

    assert accepted.tool_plane_unmanaged == _UNMANAGED
    assert accepted.tool_plane_revision is None
    assert accepted.to_persisted()["decision_evidence_json"]["tool_plane_unmanaged"] == _UNMANAGED
    # Bound into the run's identity, not decorative beside it: a run admitted
    # ungoverned is not the same run as one admitted with no statement.
    assert accepted.runtime_identity_digest != ungoverned.runtime_identity_digest


def test_a_durable_profile_cannot_be_admitted_unmanaged() -> None:
    for profile in ("durable_production", "durable_two_gateway_v1"):
        with pytest.raises(ValueError, match="durable deployment profile"):
            AcceptedInvocation.seal(
                **_common(),
                tool_plane_unmanaged={**_UNMANAGED, "deployment_profile": profile},
            )


def test_a_run_cannot_be_both_governed_and_unmanaged() -> None:
    with pytest.raises(ValueError, match="both governed and unmanaged"):
        AcceptedInvocation.seal(
            **_common(),
            tool_plane_revision=_governed_revision(),
            tool_plane_unmanaged=_UNMANAGED,
        )


def test_malformed_unmanaged_evidence_is_refused() -> None:
    for broken in (
        {**_UNMANAGED, "governance_state": "governed"},
        {**_UNMANAGED, "governance_state": "anything"},
        {**_UNMANAGED, "deployment_profile": "not-a-profile"},
        {**_UNMANAGED, "version": 2},
        {"version": 1, "governance_state": "unmanaged_drift"},
        {**_UNMANAGED, "extra": True},
    ):
        with pytest.raises(ValueError):
            AcceptedInvocation.seal(**_common(), tool_plane_unmanaged=broken)


def test_drift_on_a_non_durable_deployment_is_a_supported_unmanaged_state() -> None:
    accepted = AcceptedInvocation.seal(
        **_common(),
        tool_plane_unmanaged={**_UNMANAGED, "governance_state": "unmanaged_drift"},
    )
    assert accepted.tool_plane_unmanaged is not None
    assert accepted.tool_plane_unmanaged["governance_state"] == "unmanaged_drift"


def test_the_provenance_of_a_run_is_whatever_its_admission_sealed() -> None:
    assert resolve_tool_plane_provenance({"accepted_tool_plane_revision": _GOVERNED_DIGESTS}) == (
        "governed",
        _GOVERNED_DIGESTS,
    )
    assert resolve_tool_plane_provenance({"accepted_tool_plane_unmanaged": _UNMANAGED}) == ("unmanaged", None)


def test_an_admission_that_said_nothing_still_refuses_retrieval() -> None:
    """The original failure mode stays a failure: absence is not a mode."""

    with pytest.raises(RetrievalEvidenceError, match="retrieval_tool_plane_context_unavailable"):
        resolve_tool_plane_provenance({})


def test_a_context_carrying_both_is_refused() -> None:
    with pytest.raises(RetrievalEvidenceError, match="retrieval_tool_plane_context_ambiguous"):
        resolve_tool_plane_provenance(
            {
                "accepted_tool_plane_revision": _GOVERNED_DIGESTS,
                "accepted_tool_plane_unmanaged": _UNMANAGED,
            }
        )


def test_governed_context_with_missing_or_malformed_digests_still_fails() -> None:
    for broken in (
        {name: value for name, value in _GOVERNED_DIGESTS.items() if name != "effective_digest"},
        {**_GOVERNED_DIGESTS, "projection_digest": "not-a-digest"},
        {**_GOVERNED_DIGESTS, "user_overlay_digest": "A" * 64},
        {**_GOVERNED_DIGESTS, "base_revision_digest": None},
    ):
        with pytest.raises(RetrievalEvidenceError, match="retrieval_tool_plane_context_unavailable"):
            resolve_tool_plane_provenance({"accepted_tool_plane_revision": broken})


def test_an_unmanaged_shape_that_is_not_the_sealed_one_is_refused() -> None:
    """A dictionary someone put in the context is not an admission decision."""

    for broken in (
        {"version": 1, "governance_state": "governed"},
        {"version": 2, "governance_state": "unmanaged_drift"},
        {"governance_state": "unmanaged_drift"},
    ):
        with pytest.raises(RetrievalEvidenceError, match="retrieval_tool_plane_context_unavailable"):
            resolve_tool_plane_provenance({"accepted_tool_plane_unmanaged": broken})


def _draft(**overrides: object) -> RetrievalObservationDraftV1:
    from datetime import UTC, datetime

    started = datetime.now(UTC)
    fields: dict[str, object] = {
        "tenant_ref": _TENANT.public_ref,
        "tenant_digest": _TENANT.digest,
        "run_id": "run-1",
        "receipt_id": "tr_" + "a" * 64,
        "attempt": 1,
        "provider_id": "duckduckgo",
        "tool_kind": "web_search",
        "adapter_capability_version": "ddgs-controlled-http-v1",
        "policy_digest": "e" * 64,
        "safe_constraints": {
            "version": 1,
            "provider_id": "duckduckgo",
            "policy_status": "not_evaluated",
        },
        "started_at": started,
        "provider_finished_at": started,
        "provider_status": "empty",
        "safe_reason": "no_results",
        "result_count": 0,
        "source_count": 0,
        "source_references": (),
        "truncated": False,
        "partial": False,
        "safe_provider_request_ref": None,
    }
    fields.update(overrides)
    return RetrievalObservationDraftV1(**fields)  # type: ignore[arg-type]


def test_an_unmanaged_observation_carries_no_governed_digest() -> None:
    draft = _draft(tool_plane_mode="unmanaged")
    projection = draft.to_event_projection()

    assert projection["tool_plane"] == {
        "mode": "unmanaged",
        "base_revision_digest": None,
        "user_overlay_digest": None,
        "projection_digest": None,
        "effective_digest": None,
    }
    # Round-trips through the persisted shape as the same thing.
    assert RetrievalObservationDraftV1.from_event_projection(projection).tool_plane_mode == "unmanaged"


def test_an_unmanaged_observation_cannot_carry_a_digest() -> None:
    with pytest.raises(RetrievalEvidenceError, match="retrieval_tool_plane_mode_invalid"):
        _draft(tool_plane_mode="unmanaged", tool_plane_effective_digest="d" * 64)


def test_a_governed_observation_still_requires_all_four_digests() -> None:
    governed = {
        "tool_plane_base_revision_digest": "a" * 64,
        "tool_plane_user_overlay_digest": "b" * 64,
        "tool_plane_projection_digest": "c" * 64,
        "tool_plane_effective_digest": "d" * 64,
    }
    assert _draft(**governed).tool_plane_mode == "governed"
    for name in governed:
        with pytest.raises(RetrievalEvidenceError):
            _draft(**{**governed, name: None})


def test_a_persisted_observation_without_a_mode_is_governed() -> None:
    """Rows written before the mode existed could only have been governed."""

    projection = _draft(
        tool_plane_base_revision_digest="a" * 64,
        tool_plane_user_overlay_digest="b" * 64,
        tool_plane_projection_digest="c" * 64,
        tool_plane_effective_digest="d" * 64,
    ).to_event_projection()
    legacy = dict(projection)
    legacy["tool_plane"] = {name: value for name, value in projection["tool_plane"].items() if name != "mode"}  # type: ignore[union-attr]

    assert RetrievalObservationDraftV1.from_event_projection(legacy).tool_plane_mode == "governed"


def test_a_promoted_deployment_refuses_to_execute_an_older_ungoverned_run() -> None:
    """Recovery cannot carry an ungoverned decision into a durable promise."""

    from deerflow.runtime.accepted_invocation import unmanaged_tool_plane_is_honourable

    assert unmanaged_tool_plane_is_honourable(_UNMANAGED, durable_deployment=False) is True
    assert unmanaged_tool_plane_is_honourable(_UNMANAGED, durable_deployment=True) is False
    assert unmanaged_tool_plane_is_honourable(None, durable_deployment=False) is False


def test_the_mode_is_not_specific_to_one_provider() -> None:
    """The unmanaged path is the common retrieval path, not a DuckDuckGo one."""

    draft = _draft(
        provider_id="serply",
        tool_kind="web_search",
        adapter_capability_version="serply-http-v1",
        safe_constraints={"version": 1, "provider_id": "serply", "policy_status": "not_evaluated"},
        tool_plane_mode="unmanaged",
    )
    projection = draft.to_event_projection()

    assert projection["tool_plane"]["mode"] == "unmanaged"
    assert draft.tool_plane_effective_digest is None
    assert RetrievalObservationDraftV1.from_event_projection(projection).provider_id == "serply"


def test_a_forged_seal_is_refused_by_the_resolver() -> None:
    """A seal the record would refuse cannot be believed because it is in the context.

    The context key is server-owned and stripped from anything a caller
    supplies, so this is the second lock: a value naming a durable profile,
    or shaped like a seal without being one, is not a decision.
    """

    for forged in (
        {**_UNMANAGED, "deployment_profile": "durable_two_gateway_v1"},
        {**_UNMANAGED, "deployment_profile": "durable_production"},
        {**_UNMANAGED, "deployment_profile": "whatever"},
        {**_UNMANAGED, "extra": "field"},
    ):
        with pytest.raises(RetrievalEvidenceError, match="retrieval_tool_plane_context_unavailable"):
            resolve_tool_plane_provenance({"accepted_tool_plane_unmanaged": forged})


def test_no_caller_can_supply_an_accepted_fact() -> None:
    """Admission's own stamps are server-owned, by prefix rather than by list.

    Every `accepted_*` key is something admission decided. A caller that could
    put one in `config.context` would be handing the runtime a decision nobody
    made -- the mode included.
    """

    from deerflow.runtime.runs.worker import _build_runtime_context

    forged = {
        "accepted_tool_plane_unmanaged": _UNMANAGED,
        "accepted_tool_plane_revision": _GOVERNED_DIGESTS,
        "accepted_agent_revision_digest": "f" * 64,
        "accepted_execution_budget": {"version": 1},
        "agent_name": "kept",
    }
    built = _build_runtime_context("thread-1", "run-1", forged)

    assert [key for key in built if key.startswith("accepted_")] == []
    assert built["agent_name"] == "kept", "only the server-owned facts are stripped"
