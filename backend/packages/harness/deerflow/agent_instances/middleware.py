"""Recheck host instance authority before model and tool dispatch, also in children."""

from langchain.agents.middleware import AgentMiddleware

from deerflow.agent_instances.conversations import AGENT_EXECUTION_CONTEXT_KEY, AgentExecution


class InstanceAuthorityMiddleware(AgentMiddleware):
    def __init__(self, *, require_memory_audience=False):
        super().__init__()
        self.require_memory_audience = require_memory_audience

    def _validate_sync(self, runtime):
        execution = self._execution(runtime)
        execution.validate_sync()
        if self.require_memory_audience:
            from deerflow.agent_instances.memory import _bridge, validate_memory_audience

            _bridge(execution.owner_loop, validate_memory_audience(execution))

    async def _validate_async(self, runtime):
        execution = self._execution(runtime)
        await execution.validate()
        if self.require_memory_audience:
            from deerflow.agent_instances.memory import validate_memory_audience

            await validate_memory_audience(execution)

    @staticmethod
    def _execution(runtime):
        value = (runtime.context or {}).get(AGENT_EXECUTION_CONTEXT_KEY)
        if not isinstance(value, AgentExecution):
            from deerflow.agent_instances.contract import AgentDenied

            raise AgentDenied("The host instance binding is unavailable")
        return value

    def before_model(self, state, runtime):
        self._validate_sync(runtime)

    async def abefore_model(self, state, runtime):
        await self._validate_async(runtime)

    def wrap_tool_call(self, request, handler):
        self._validate_sync(request.runtime)
        return handler(request)

    async def awrap_tool_call(self, request, handler):
        await self._validate_async(request.runtime)
        return await handler(request)
