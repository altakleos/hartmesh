"""Exercise proposal termination through the actual agent tool/model graph."""

import pytest
from langchain.agents import create_agent
from langchain.tools import tool
from langchain_core.language_models.fake_chat_models import FakeMessagesListChatModel
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from test_agent_chat_tools import context

from deerflow.agent_instances.chat_tools import propose_assignment


class RecordedModel(FakeMessagesListChatModel):
    calls: int = 0

    def bind_tools(self, tools, **kwargs):
        return self

    def _generate(self, *args, **kwargs):
        self.calls += 1
        return super()._generate(*args, **kwargs)


def proposal_call(number=1, **updates):
    return {"name": "propose_assignment", "args": {"proposal": {"objective": f"Review guide {number}", "success_criteria": "Report verified gaps"}}, "id": "reused-provider-call", "type": "tool_call", **updates}


def build(marks, *, sibling=False):
    from deerflow.agents.middlewares.assignment_proposal_middleware import AssignmentProposalMiddleware

    effects = []

    @tool
    def effect() -> str:
        """Record the synthetic sibling action."""
        effects.append(True)
        return "Recorded"

    calls = ([{"name": "effect", "args": {}, "id": "effect-call", "type": "tool_call"}] if sibling else []) + [proposal_call()]
    model = RecordedModel(responses=[AIMessage(id="issuer-1", content="", tool_calls=calls), AIMessage(id="followup", content="A normal answer")])
    graph = create_agent(model, tools=[propose_assignment, effect], middleware=[AssignmentProposalMiddleware()], context_schema=dict)
    return graph, model, effects, {"messages": [HumanMessage(id="human-1", content="Review the guide", additional_kwargs=marks)]}


@pytest.mark.parametrize("sibling", [False, True])
def test_actual_agent_ends_after_supported_proposal_and_drops_sibling_effects(sibling):
    graph, model, effects, incoming = build({"shows_cards": ["assignment_card"]}, sibling=sibling)
    result = graph.invoke(incoming, context=context("company"))
    assert model.calls == 1 and effects == []
    assert isinstance(result["messages"][-1], ToolMessage)
    assert result["messages"][-1].artifact["assignment_card"]["draft"]["objective"] == "Review guide 1"


@pytest.mark.asyncio
async def test_async_actual_agent_also_ends_after_supported_proposal():
    graph, model, effects, incoming = build({"shows_cards": ["assignment_card"]}, sibling=True)
    result = await graph.ainvoke(incoming, context=context("company"))
    assert isinstance(result["messages"][-1], ToolMessage) and effects == []
    assert not any(message.content == "A normal answer" for message in result["messages"])


@pytest.mark.parametrize("marks", [{}, {"shows_cards": ["assignment_card"], "assignment_card_response": {"choice": "answer_here"}}])
def test_fallback_and_just_answer_continue_the_actual_agent_and_keep_siblings(marks):
    graph, model, effects, incoming = build(marks, sibling=True)
    result = graph.invoke(incoming, context=context("company"))
    assert model.calls == 2 and effects == [True]
    assert result["messages"][-1].content == "A normal answer"
    assert not any(isinstance(message, ToolMessage) and message.artifact for message in result["messages"])


def test_reused_provider_call_id_keeps_distinct_cards_and_same_call_replay_stable():
    from langgraph.graph import END, START, MessagesState, StateGraph
    from langgraph.prebuilt import ToolNode

    builder = StateGraph(MessagesState, context_schema=dict)
    builder.add_node("tools", ToolNode([propose_assignment]))
    builder.add_edge(START, "tools")
    builder.add_edge("tools", END)
    graph = builder.compile()

    def incoming(number):
        return [HumanMessage(id=f"human-{number}", content=f"Review {number}", additional_kwargs={"shows_cards": ["assignment_card"]}), AIMessage(id=f"issuer-{number}", content="", tool_calls=[proposal_call(number)])]

    first = graph.invoke({"messages": incoming(1)}, context=context("company"))
    replay = graph.invoke({"messages": first["messages"]}, context=context("company"))
    assert [message.id for message in replay["messages"]] == [message.id for message in first["messages"]]
    second = graph.invoke({"messages": first["messages"] + incoming(2)}, context=context("company"))
    cards = [message for message in second["messages"] if isinstance(message, ToolMessage) and message.artifact]
    assert len(cards) == len({message.id for message in cards}) == 2
    assert [message.artifact["assignment_card"]["draft"]["objective"] for message in cards] == ["Review guide 1", "Review guide 2"]


def test_old_successful_card_cannot_end_a_new_visible_human_turn():
    graph, _model, _effects, incoming = build({"shows_cards": ["assignment_card"]})
    first = graph.invoke(incoming, context=context("company"))
    model = RecordedModel(responses=[AIMessage(content="A new answer")])
    from deerflow.agents.middlewares.assignment_proposal_middleware import AssignmentProposalMiddleware

    next_graph = create_agent(model, tools=[propose_assignment], middleware=[AssignmentProposalMiddleware()], context_schema=dict)
    result = next_graph.invoke({"messages": first["messages"] + [HumanMessage(id="new-human", content="What changed?", additional_kwargs={"shows_cards": ["assignment_card"]})]}, context=context("company"))
    assert model.calls == 1 and result["messages"][-1].content == "A new answer"


def test_malformed_proposal_arguments_are_not_a_successful_terminal_card():
    from deerflow.agents.middlewares.assignment_proposal_middleware import AssignmentProposalMiddleware

    model = RecordedModel(responses=[AIMessage(content="", tool_calls=[proposal_call(args={"proposal": {"objective": "Missing criteria"}})]), AIMessage(content="Please specify done when")])
    graph = create_agent(model, tools=[propose_assignment], middleware=[AssignmentProposalMiddleware()], context_schema=dict)
    result = graph.invoke({"messages": [HumanMessage(content="Review", additional_kwargs={"shows_cards": ["assignment_card"]})]}, context=context("company"))
    assert model.calls == 2 and result["messages"][-1].content == "Please specify done when"
    assert not any(isinstance(message, ToolMessage) and message.artifact for message in result["messages"])


def test_actual_assembly_installs_companion_only_for_authorized_proposal_tool(monkeypatch):
    from test_agent_assembly_descriptor import TestLeadAgentAssembly
    from test_agent_chat_tools import execution

    from deerflow.agent_instances import chat_tools
    from deerflow.agent_instances.conversations import AGENT_EXECUTION_CONTEXT_KEY
    from deerflow.agents.lead_agent import agent

    config = TestLeadAgentAssembly._isolate_from_the_ambient_config(monkeypatch)

    def assembled():
        return agent.assemble_lead_agent({"context": {AGENT_EXECUTION_CONTEXT_KEY: execution("company")}, "configurable": {"thread_id": "chat"}}, app_config=config)

    assert "AssignmentProposalMiddleware" not in [type(item).__name__ for item in assembled().graph["middleware"]]
    monkeypatch.setattr(chat_tools, "chat_tools", lambda execution: [chat_tools.show_web_page, chat_tools.propose_assignment])
    result = assembled()
    types = [type(item).__name__ for item in result.graph["middleware"]]
    assert types.count("AssignmentProposalMiddleware") == 1
    assert types.index("AssignmentProposalMiddleware") < types.index("ClarificationMiddleware")


def test_clarification_keeps_priority_over_a_supported_proposal():
    from deerflow.agents.middlewares.assignment_proposal_middleware import AssignmentProposalMiddleware
    from deerflow.agents.middlewares.clarification_middleware import ClarificationMiddleware
    from deerflow.tools.builtins.clarification_tool import ask_clarification_tool

    calls = [proposal_call(), {"name": "ask_clarification", "args": {"question": "Which guide?"}, "id": "clarify", "type": "tool_call"}]
    model = RecordedModel(responses=[AIMessage(id="issuer", content="", tool_calls=calls), AIMessage(content="Unexpected followup")])
    graph = create_agent(model, tools=[propose_assignment, ask_clarification_tool], middleware=[AssignmentProposalMiddleware(), ClarificationMiddleware()], context_schema=dict)
    result = graph.invoke({"messages": [HumanMessage(content="Review", additional_kwargs={"shows_cards": ["assignment_card"]})]}, context=context("company"))
    assert model.calls == 1 and result["messages"][-1].name == "ask_clarification"
    assert not any(isinstance(message, ToolMessage) and (message.artifact or {}).get("assignment_card") for message in result["messages"])


def test_supported_parallel_proposals_keep_one_card_and_sync_provider_blocks():
    from types import SimpleNamespace

    from deerflow.agents.middlewares.assignment_proposal_middleware import AssignmentProposalMiddleware

    first, second = proposal_call(), proposal_call(2, id="second-call")
    message = AIMessage(
        id="issuer",
        content=[{"type": "tool_use", "id": first["id"], "name": "propose_assignment", "input": first["args"]}, {"type": "tool_use", "id": "second-call", "name": "propose_assignment", "input": second["args"]}],
        tool_calls=[first, second],
    )
    state = {"messages": [HumanMessage(content="Review", additional_kwargs={"shows_cards": ["assignment_card"]}), message]}
    patched = AssignmentProposalMiddleware().after_model(state, SimpleNamespace())["messages"][0]
    assert patched.tool_calls == [first]
    assert [block["id"] for block in patched.content] == [first["id"]]


def test_summary_named_human_does_not_hide_real_card_or_just_answer_marks():
    from langgraph.types import Command
    from test_assignment_proposal import invoke

    user = HumanMessage(content="Review", additional_kwargs={"shows_cards": ["assignment_card"]})
    summary = HumanMessage(name="summary", content="A hidden summary")
    assert isinstance(invoke([user, summary]), Command)
    user = HumanMessage(content="Here", additional_kwargs={"shows_cards": ["assignment_card"], "assignment_card_response": {"choice": "answer_here"}})
    assert "chose to have this done here" in invoke([user, summary]).content
