"""Response controls are generic input data with exact immutable capture."""

from copy import deepcopy
from uuid import uuid4

import pytest
from langchain_core.messages import HumanMessage

from deerflow.runtime.runs.output_capture import OutputCaptureObserver
from deerflow.runtime.runs.output_source import inspect_output_message


def response(**changes):
    return {"version": 1, "kind": "human_input_response", "source": "ask_clarification", "request_id": "request", "response_kind": "option", "option_id": "option-1", "value": "First choice", **changes}


@pytest.mark.parametrize("value", [response(), {key: value for key, value in response(response_kind="text").items() if key != "option_id"}])
def test_valid_response_is_eligible_human_input_without_becoming_output_authority(value):
    human = HumanMessage(value["value"], id="actual-human", additional_kwargs={"human_input_response": value})
    facts = inspect_output_message(human.model_dump())
    assert facts.unavailable_reason is None and facts.role == "human" and facts.text == value["value"]


@pytest.mark.parametrize("value", [response(version=True), response(extra="unknown"), response(option_id=""), response(value=1), response(response_kind="other")])
def test_invalid_response_stays_unsupported_input(value):
    facts = inspect_output_message(HumanMessage("choice", id="human", additional_kwargs={"human_input_response": value}).model_dump())
    assert facts.unavailable_reason == "unsupported_input"


@pytest.mark.parametrize("mutation", ["value", "request_id", "remove"])
def test_response_is_frozen_before_model_input_and_cannot_mutate_under_same_text(mutation):
    root, node, model = uuid4(), uuid4(), uuid4()
    initial = response()
    human = HumanMessage("First choice", id="actual-human", additional_kwargs={"human_input_response": deepcopy(initial)})
    capture = OutputCaptureObserver(owner_user_id="owner", thread_id="thread", run_id="run")
    capture.begin_epoch(root, {"messages": [human]}, pre_existing_ids=set())
    capture.chain_start(run_id=root, parent_run_id=None, tags=[], metadata={}, name="LangGraph", inputs={})
    capture.chain_start(run_id=node, parent_run_id=root, tags=[], metadata={"langgraph_node": "model"}, name="model", inputs={"messages": [human]})
    if mutation == "remove":
        human.additional_kwargs.pop("human_input_response")
    else:
        human.additional_kwargs["human_input_response"][mutation] = "changed-control"
    capture.model_start(run_id=model, parent_run_id=node, tags=[], messages=[[human]], metadata={"langgraph_node": "model"})
    assert capture.uncertain, "Unchanged text and channel id must not hide changed response controls"
