"""Supported proposals wait for human consent at the real agent-loop boundary."""

from langchain.agents.middleware import AgentMiddleware, hook_config
from langchain_core.messages import AIMessage, ToolMessage

from deerflow.agent_instances.chat_tools import ProposeAssignment, proposal_issuer, proposal_message_id, supports_assignment_card
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
        messages = state.get("messages", [])
        if not supports_assignment_card(messages):
            return None
        # Hidden middleware human reminders may follow the ToolMessage. The
        # exact issuing AI message must still belong to this visible user turn.
        for message in reversed(messages):
            if isinstance(message, AIMessage):
                break
            if not isinstance(message, ToolMessage) or message.name != "propose_assignment":
                continue
            issuer = proposal_issuer(messages, message.tool_call_id)
            if issuer is None or message.id != proposal_message_id(message.tool_call_id, issuer):
                continue
            issuing = next((item for item in reversed(messages) if isinstance(item, AIMessage) and item.id == issuer), None)
            call = next((item for item in issuing.tool_calls if item.get("id") == message.tool_call_id and item.get("name") == "propose_assignment"), None) if issuing else None
            card = message.artifact.get("assignment_card") if type(message.artifact) is dict else None
            if type(card) is not dict or type(card.get("version")) is not int or card["version"] != 1 or card.get("kind") != "assignment_card" or card.get("tool_call_id") != message.tool_call_id:
                continue
            try:
                original = ProposeAssignment.model_validate(call["args"]["proposal"]) if call else None
                parsed = ProposeAssignment.model_validate(card.get("draft"))
                if original is not None and original == parsed:
                    return {"jump_to": "end"}
            except (ValueError, TypeError, KeyError):
                continue
        return None

    @hook_config(can_jump_to=["end"])
    def before_model(self, state, runtime):
        return self._completed(state)

    @hook_config(can_jump_to=["end"])
    async def abefore_model(self, state, runtime):
        return self._completed(state)
