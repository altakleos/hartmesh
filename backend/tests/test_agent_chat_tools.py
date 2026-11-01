"""Page offers carry bounded keys, never addresses or reader authority."""

from typing import get_args

import pytest
from langchain.tools import ToolRuntime
from langchain_core.messages import AIMessage, ToolMessage
from langgraph.graph import END, START, MessagesState, StateGraph
from langgraph.prebuilt import ToolNode
from langgraph.types import Command
from pydantic import ValidationError

from deerflow.agent_instances.chat_tools import WebPageKey, show_web_page
from deerflow.agent_instances.contract import DefinitionSnapshot, InstanceIdentity
from deerflow.agent_instances.conversations import AGENT_EXECUTION_CONTEXT_KEY, AgentConversations, AgentExecution
from deerflow.spaces.contract import PrincipalRef

PAGES = ("access", "files", "memory", "conversations", "work", "agentSettings", "addAssistant", "addEmployee", "telegram", "scheduled", "account", "timezone")
AGENT_PAGES = ("access", "files", "work", "agentSettings")
REFUSAL = "That page does not exist in this chat."
OFFER = "A link to this page is shown with your reply where this chat can show one. Say in one sentence what the page is for. Do not write an address."
DESCRIPTION = (
    "Offer the person a link that opens one page of the web app. Use it when the person asks for something that is done on a page: "
    "who has access, the files, what is remembered, earlier conversations, assignment history, settings, adding an AI assistant or an AI employee, "
    "Telegram bots, scheduled tasks, the account. Answer what you can answer yourself. Name the page; never write an address."
)


def execution(custody):
    definition = DefinitionSnapshot.capture(owner_id="creator", config={"name": "helper"}, soul="Help with the request.")
    instance = InstanceIdentity(
        id="a" * 32,
        principal=PrincipalRef("nonhuman", "agent:" + "a" * 32),
        name="Helper",
        custody=custody,
        owner_id="creator" if custody == "personal" else None,
        creator_id="creator",
        supervisor=PrincipalRef("human", "creator"),
        definition_revision=definition.revision,
        home_id="b" * 32,
        status="active",
        generation=1,
    )
    return AgentExecution(instance, PrincipalRef("human", "requester"), definition, 1, AgentConversations(None), "chat", "c" * 32)


def context(kind):
    if kind == "ordinary":
        return {}
    if kind == "custom":
        return {"agent_name": "custom-helper"}
    return {"agent_name": "adopted-helper", AGENT_EXECUTION_CONTEXT_KEY: execution(kind)}


def runtime(context=None):
    return ToolRuntime(state={"messages": []}, context=context, config={}, stream_writer=lambda _: None, tool_call_id="page-offer", store=None)


def offer(page, ctx=None):
    return show_web_page.invoke({"page": page, "runtime": runtime(ctx)})


def test_show_web_page_accepts_only_listed_keys():
    assert get_args(WebPageKey) == PAGES
    schema = show_web_page.tool_call_schema.model_json_schema()
    assert set(schema["properties"]) == {"page"}
    assert schema["properties"]["page"]["enum"] == list(PAGES)
    assert schema["required"] == ["page"]
    for page in PAGES:
        assert show_web_page.tool_call_schema.model_validate({"page": page}).page == page
    for invalid in ("home", "https://example.test", "../files", "", None, 1):
        with pytest.raises(ValidationError):
            offer(invalid)


def test_show_web_page_model_description():
    assert " ".join(show_web_page.description.split()) == DESCRIPTION


@pytest.mark.parametrize("page", PAGES)
def test_show_web_page_returns_no_address(page):
    result = offer(page, context("personal"))
    assert result.name == "show_web_page"
    assert result.tool_call_id == "page-offer"
    assert result.content == OFFER
    assert result.artifact == {"web_page": {"version": 1, "kind": "web_page", "key": page}}
    assert result.additional_kwargs == {}


@pytest.mark.parametrize("page", AGENT_PAGES)
@pytest.mark.parametrize("kind", ("ordinary", "custom"))
def test_show_web_page_refuses_agent_pages_outside_an_agent(page, kind):
    result = offer(page, context(kind))
    assert result.artifact is None
    assert result.content == REFUSAL
    assert result.tool_call_id == "page-offer"


@pytest.mark.parametrize("page", AGENT_PAGES)
@pytest.mark.parametrize("custody", ("personal", "company"))
def test_show_web_page_accepts_agent_pages_for_both_instance_kinds(page, custody):
    assert offer(page, context(custody)).artifact["web_page"]["key"] == page


def test_show_web_page_refuses_memory_for_a_custom_agent():
    result = offer("memory", context("custom"))
    assert result.artifact is None
    assert result.content == REFUSAL


@pytest.mark.parametrize("kind", ("ordinary", "personal", "company"))
def test_show_web_page_accepts_memory_for_default_chat_and_instances(kind):
    assert offer("memory", context(kind)).artifact["web_page"]["key"] == "memory"


@pytest.mark.parametrize("page", ("conversations", "addAssistant", "addEmployee", "telegram", "scheduled", "account", "timezone"))
@pytest.mark.parametrize("kind", ("ordinary", "custom", "personal", "company"))
def test_show_web_page_accepts_conversations_and_global_pages_for_every_agent(page, kind):
    assert offer(page, context(kind)).artifact["web_page"]["key"] == page


@pytest.mark.parametrize("forged", ({"instance": "a" * 32}, "a" * 32, object()))
def test_show_web_page_serialized_or_untyped_execution_is_not_an_agent(forged):
    ctx = {"agent_name": "custom-helper", AGENT_EXECUTION_CONTEXT_KEY: forged}
    for page in (*AGENT_PAGES, "memory"):
        result = offer(page, ctx)
        assert result.artifact is None
        assert result.content == REFUSAL


def test_show_web_page_absent_context_keeps_default_chat_pages():
    assert offer("conversations").artifact["web_page"]["key"] == "conversations"
    assert offer("memory").artifact["web_page"]["key"] == "memory"
    assert offer("files").artifact is None


def test_show_web_page_does_not_end_the_turn():
    for page in PAGES:
        result = offer(page)
        assert isinstance(result, ToolMessage)
        assert not isinstance(result, Command)
        assert not show_web_page.return_direct


def test_show_web_page_tool_node_injects_runtime_and_preserves_artifact():
    graph = StateGraph(MessagesState, context_schema=dict)
    graph.add_node("tools", ToolNode([show_web_page]))
    graph.add_edge(START, "tools")
    graph.add_edge("tools", END)
    answer = graph.compile().invoke(
        {"messages": [AIMessage(content="", tool_calls=[{"name": "show_web_page", "args": {"page": "files"}, "id": "graph-page", "type": "tool_call"}])]},
        context=context("company"),
    )
    result = answer["messages"][-1]
    assert isinstance(result, ToolMessage)
    assert result.tool_call_id == "graph-page"
    assert result.content == OFFER
    assert result.artifact == {"web_page": {"version": 1, "kind": "web_page", "key": "files"}}
