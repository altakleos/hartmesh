"""Incoming messages cannot supply the host's publication marker."""

import pytest
from langchain_core.messages import AIMessage, HumanMessage

from app.gateway.services import normalize_input, strip_server_owned_state_metadata


@pytest.mark.parametrize("trusted", [False, True])
def test_run_input_strips_publication_markers_from_every_normalized_message(trusted):
    messages = [HumanMessage(content="question", additional_kwargs={"channel_turn_id": "a" * 32, "files": [{"path": "uploads/one.txt"}]}), AIMessage(content="prior answer", additional_kwargs={"channel_turn_id": "b" * 32})]
    original = [message.model_copy(deep=True) for message in messages]
    result = normalize_input({"messages": messages}, trusted_internal=trusted)
    assert all("channel_turn_id" not in message.additional_kwargs for message in result["messages"])
    assert result["messages"][0].additional_kwargs["files"] == [{"path": "uploads/one.txt"}]
    assert messages == original


def test_external_state_mutation_cannot_seed_a_publication_anchor():
    value = {"messages": [{"role": "human", "content": "state edit", "additional_kwargs": {"channel_turn_id": "a" * 32}}]}
    result = strip_server_owned_state_metadata(value)
    assert "channel_turn_id" not in result["messages"][0].additional_kwargs
    assert value["messages"][0]["additional_kwargs"]["channel_turn_id"] == "a" * 32
