"""Graph-level test for clarifications whose provider reuses a call ID.

Some providers number tool calls per response (``call_0``), so two turns can
issue ``ask_clarification`` under the same call ID. Each question's ToolMessage
must keep its own ID in a real ``create_agent`` graph with a checkpointer:
``add_messages`` replaces a message that shares an ID in place, which would
overwrite the earlier question and leave the later call without a result.
"""

from __future__ import annotations

from langchain.agents import create_agent
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langgraph.checkpoint.memory import InMemorySaver

from deerflow.agents.middlewares.clarification_middleware import ClarificationMiddleware
from deerflow.tools.builtins.clarification_tool import ask_clarification_tool

_REUSED_CALL_ID = "call_0"


def _ask(question: str) -> AIMessage:
    return AIMessage(
        content="",
        tool_calls=[
            {
                "id": _REUSED_CALL_ID,
                "name": "ask_clarification",
                "args": {"question": question, "clarification_type": "missing_info"},
            }
        ],
    )


class _ScriptedModel(BaseChatModel):
    """Replays ``replies`` in order and records the messages of each call."""

    replies: list[AIMessage]
    seen: list[list] = []

    @property
    def _llm_type(self) -> str:
        return "fake-reused-call-id"

    def bind_tools(self, tools, **kwargs):
        return self

    def _generate(self, messages, stop=None, run_manager=None, **kwargs):
        self.seen.append(list(messages))
        return ChatResult(generations=[ChatGeneration(message=self.replies[len(self.seen) - 1])])

    async def _agenerate(self, messages, stop=None, run_manager=None, **kwargs):
        return self._generate(messages, stop=stop, run_manager=run_manager, **kwargs)


def _assert_every_call_answered_in_place(messages: list) -> None:
    """Each tool call is followed by its own result, as providers require."""
    for index, message in enumerate(messages):
        if isinstance(message, AIMessage) and message.tool_calls:
            result = messages[index + 1]
            assert isinstance(result, ToolMessage), f"call at {index} has no result: {messages}"
            assert result.tool_call_id == _REUSED_CALL_ID


def test_reused_call_id_keeps_each_unattended_clarification_result():
    model = _ScriptedModel(replies=[_ask("Which city?"), _ask("How many days?"), AIMessage(content="done")], seen=[])
    agent = create_agent(model=model, tools=[ask_clarification_tool], middleware=[ClarificationMiddleware()], checkpointer=InMemorySaver())
    config = {"configurable": {"thread_id": "reused-call-id-unattended"}}
    unattended = {"disable_clarification": True}

    # ``ask_clarification`` returns directly, so each unattended run ends at
    # its "proceed" result and the next run continues the thread.
    for text in ["Plan a trip", "Continue", "Continue"]:
        agent.invoke({"messages": [HumanMessage(content=text)]}, config, context=unattended)

    messages = agent.get_state(config).values["messages"]
    _assert_every_call_answered_in_place(messages)
    assert len([message for message in messages if isinstance(message, ToolMessage)]) == 2
    _assert_every_call_answered_in_place(model.seen[-1])
