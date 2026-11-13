"""Assignment proposals return bounded artifacts; human consent owns effects."""

from datetime import datetime

import pytest
from langchain.tools import ToolRuntime
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langgraph.graph import END, START, MessagesState, StateGraph
from langgraph.prebuilt import ToolNode
from langgraph.types import Command
from pydantic import ValidationError
from test_agent_chat_tools import context

DRAFT = {"objective": "Check <the> guide", "success_criteria": "Report verified gaps", "due_text": None, "due_at": None}


def invoke(messages, **changes):
    from deerflow.agent_instances.chat_tools import propose_assignment

    runtime = ToolRuntime(state={"messages": messages}, context=context("company"), config={}, stream_writer=lambda _: None, tool_call_id="proposal-call", store=None)
    return propose_assignment.invoke({"proposal": {**DRAFT, **changes}, "runtime": runtime})


def test_proposal_is_a_terminal_artifact_without_accessing_a_service():
    result = invoke([HumanMessage(content="Review this guide", additional_kwargs={"shows_cards": ["assignment_card"]})])
    assert isinstance(result, Command) and result.goto == END
    assert set(result.update) == {"messages"}
    (message,) = result.update["messages"]
    assert message.id == "assignment-card:proposal-call"
    assert message.tool_call_id == "proposal-call" and message.name == "propose_assignment"
    assert message.artifact == {"assignment_card": {"version": 1, "kind": "assignment_card", "tool_call_id": "proposal-call", "draft": DRAFT}}
    assert DRAFT["objective"] in message.content and DRAFT["success_criteria"] in message.content
    assert message.content.endswith("Nothing is saved or started until the person confirms it.")


@pytest.mark.parametrize("messages", [[], [HumanMessage(content="No card here")], [HumanMessage(content="Old", additional_kwargs={"shows_cards": ["assignment_card"]}), HumanMessage(content="Current")]])
def test_newest_visible_human_message_must_support_cards(messages):
    result = invoke(messages)
    assert isinstance(result, ToolMessage) and result.artifact is None
    assert result.content == "This conversation cannot show an assignment card. Do the request here."


def test_hidden_human_messages_do_not_erase_original_mark():
    result = invoke([HumanMessage(content="Original", additional_kwargs={"shows_cards": ["assignment_card"]}), AIMessage(content="Thinking"), HumanMessage(content="Reminder", additional_kwargs={"hide_from_ui": True})])
    assert isinstance(result, Command)


def test_just_answer_response_takes_precedence_over_card_mark():
    result = invoke(
        [HumanMessage(content="Do it here", additional_kwargs={"shows_cards": ["assignment_card"], "assignment_card_response": {"version": 1, "kind": "assignment_card_response", "tool_call_id": "old-call", "choice": "answer_here"}})]
    )
    assert isinstance(result, ToolMessage) and result.artifact is None
    assert result.content == "The person chose to have this done here. Do it now. Do not propose an assignment for it."


@pytest.mark.parametrize(
    "changes",
    [
        {"objective": ""},
        {"objective": " "},
        {"objective": "x" * 4097},
        {"success_criteria": ""},
        {"success_criteria": "x" * 4097},
        {"due_text": "x" * 121},
        {"due_at": "2026-12-01T18:00:00"},
        {"due_at": datetime(2026, 12, 1)},
        {"unknown": "value"},
    ],
)
def test_proposal_schema_refuses_invalid_or_unknown_fields(changes):
    from deerflow.agent_instances.chat_tools import ProposeAssignment

    with pytest.raises(ValidationError):
        ProposeAssignment.model_validate({**DRAFT, **changes})


def test_proposal_schema_preserves_an_explicit_offset():
    from deerflow.agent_instances.chat_tools import ProposeAssignment

    draft = ProposeAssignment.model_validate({**DRAFT, "objective": "  A guide  ", "due_text": " Friday ", "due_at": "2026-12-04T18:00:00-05:00"})
    assert draft.objective == "A guide" and draft.due_text == "Friday"
    assert draft.model_dump(mode="json")["due_at"] == "2026-12-04T18:00:00-05:00"


def test_actual_tool_node_injects_runtime_and_ends_with_the_card():
    from deerflow.agent_instances.chat_tools import propose_assignment

    graph = StateGraph(MessagesState, context_schema=dict)
    graph.add_node("tools", ToolNode([propose_assignment]))
    graph.add_edge(START, "tools")
    graph.add_edge("tools", END)
    messages = [
        HumanMessage(content="Review the guide", additional_kwargs={"shows_cards": ["assignment_card"]}),
        AIMessage(content="", tool_calls=[{"name": "propose_assignment", "args": {"proposal": DRAFT}, "id": "graph-proposal", "type": "tool_call"}]),
    ]
    result = graph.compile().invoke({"messages": messages}, context=context("personal"))
    message = result["messages"][-1]
    assert message.id.startswith("assignment-card:graph-proposal:")
    assert message.artifact["assignment_card"]["tool_call_id"] == "graph-proposal"
