"""Recheck host instance authority before model and tool dispatch, also in children."""

from langchain.agents.middleware import AgentMiddleware

from deerflow.agent_instances.conversations import AGENT_EXECUTION_CONTEXT_KEY, AgentExecution


class InstanceAuthorityMiddleware(AgentMiddleware):
    @staticmethod
    def _execution(runtime):
        value = (runtime.context or {}).get(AGENT_EXECUTION_CONTEXT_KEY)
        if not isinstance(value, AgentExecution):
            from deerflow.agent_instances.contract import AgentDenied

            raise AgentDenied("The host instance binding is unavailable")
        return value

    def before_model(self, state, runtime):
        self._execution(runtime).validate_sync()

    async def abefore_model(self, state, runtime):
        await self._execution(runtime).validate()

    def wrap_tool_call(self, request, handler):
        self._execution(request.runtime).validate_sync()
        return handler(request)

    async def awrap_tool_call(self, request, handler):
        await self._execution(request.runtime).validate()
        return await handler(request)
