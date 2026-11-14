"""Supported proposals wait for human consent at the real agent-loop boundary."""

from langchain.agents.middleware import AgentMiddleware, hook_config
from langchain_core.messages import AIMessage

from deerflow.agent_instances.chat_tools import successful_assignment_proposal, supports_assignment_card
from deerflow.agents.middlewares.tool_call_metadata import clone_ai_message_with_tool_calls


class AssignmentProposalMiddleware(AgentMiddleware):
    """Only a current successful card ends a turn; model guidance continues it."""

    def _isolate(self, state):
        messages = state.get("messages", [])
        if not messages or not isinstance(messages[-1], AIMessage) or not supports_assignment_card(messages):
            return None
        message = messages[-1]
        selected = next((call for call in message.tool_calls if call.get("name") == "propose_assignment"), None)
        if selected is None or message.tool_calls == [selected]:
            return None
        return {"messages": [clone_ai_message_with_tool_calls(message, [selected])]}

    def after_model(self, state, runtime):
        return self._isolate(state)

    async def aafter_model(self, state, runtime):
        return self._isolate(state)

    def _completed(self, state):
        return {"jump_to": "end"} if successful_assignment_proposal(state.get("messages", [])) is not None else None

    @hook_config(can_jump_to=["end"])
    def before_model(self, state, runtime):
        return self._completed(state)

    @hook_config(can_jump_to=["end"])
    async def abefore_model(self, state, runtime):
        return self._completed(state)
