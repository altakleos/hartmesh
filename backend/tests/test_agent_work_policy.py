import pytest

from deerflow.agent_instances.contract import DefinitionSnapshot
from deerflow.config.agents_config import AgentConfig, preserve_non_managed_fields


def test_work_policy_is_opt_in_and_historical_snapshot_bytes_stay_exact():
    original = {"name": "analyst", "skills": []}
    snapshot = DefinitionSnapshot.capture(owner_id="alice", config=original, soul="A mandate")
    assert AgentConfig(**original).work_policy is None
    assert snapshot.config == original and "work_policy" not in snapshot.config
    assert snapshot.validated() == snapshot
    enabled = AgentConfig(name="analyst", work_policy={"enabled": True})
    assert enabled.work_policy.review_required is True
    assert preserve_non_managed_fields(enabled)["work_policy"]["enabled"] is True


@pytest.mark.parametrize("policy", [{"enabled": "true"}, {"enabled": True, "unknown": 1}, {"max_derived_per_activation": 100000}, {"responsibilities": [{"key": "a", "label": "A"}, {"key": "a", "label": "B"}]}])
def test_work_policy_rejects_coercions_unknown_fields_and_unbounded_or_duplicate_responsibilities(policy):
    with pytest.raises(ValueError):
        AgentConfig(name="analyst", work_policy=policy)
