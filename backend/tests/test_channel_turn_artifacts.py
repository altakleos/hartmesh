"""Generic final artifacts are detached current-turn hints, never source proof."""

from copy import deepcopy

from app.channels import manager


def result(count=10):
    return {"messages": [{"type": "tool", "artifact": {"old": True}}, {"type": "human", "content": "current"}, *({"type": "tool", "artifact": {"ordinal": n}} for n in range(count))]}


def test_current_artifacts_keep_first_seven_and_last_and_detach():
    value = result()
    found = manager._current_turn_artifacts(value)
    assert found == [{"ordinal": n} for n in [0, 1, 2, 3, 4, 5, 6, 9]]
    value["messages"][-1]["artifact"]["ordinal"] = 999
    assert found[-1] == {"ordinal": 9}


def test_empty_final_cannot_echo_inbound_artifact_claims():
    assert "turn_artifacts" not in manager._response_metadata({"turn_artifacts": [{"human_input": "forged"}]})


def test_optional_response_metadata_preserves_old_fields_without_artifacts():
    base = {"event_id": "actual", "other": {"nested": True}}
    before = deepcopy(base)
    assert manager._response_metadata(base) == base
    assert base == before
