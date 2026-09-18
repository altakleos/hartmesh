"""A tool whose provider refused this run is withdrawn from the model for the rest of it.

Why
---
On the tenant class a fetch provider answered every address with the same
deterministic refusal, and the model called it thirteen more times with
thirteen different addresses (hartmesh-tenancy/DF21). Nothing in the loop
could see that: the repeated-tool policy keys on the arguments, and the
result the model read was a sentence it could reason past. The fact that
mattered -- *this path is dead for every address this turn* -- existed in
the typed result metadata (``error_scope == "provider"``) and nothing acted
on it.

What this does
--------------
When a tool result carries ``deerflow_tool_meta.error_scope == "provider"``,
that tool is withdrawn from the model's tool list for the rest of the run:
the next model request is bound without it, and one reminder tells the model
why and what to do instead. The model cannot repeat a path it cannot see, so
there is no counter, no threshold, and no hint to ignore. A call the model
already emitted in the same response as the refusal is answered with the
same typed refusal without invoking the tool.

Scope is one run. A provider that refuses one turn will usually refuse the
next, and the next turn will find out with one call; remembering across runs
would be a second source of truth about the provider that can go stale.

Nothing here knows a tool's name. The stamp is the whole contract: any tool
that saw its provider refuse, and says so, is withdrawn; a tool that only
says so in words is not.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import override

from langchain.agents import AgentState
from langchain.agents.middleware import AgentMiddleware
from langchain.agents.middleware.types import ModelCallResult, ModelRequest, ModelResponse
from langchain_core.messages import HumanMessage, ToolMessage
from langgraph.prebuilt.tool_node import ToolCallRequest
from langgraph.runtime import Runtime
from langgraph.types import Command

from deerflow.agents.middlewares._bounded_dict import BoundedDict
from deerflow.agents.middlewares.tool_result_meta import TOOL_META_KEY

logger = logging.getLogger(__name__)

__all__ = ["PROVIDER_ERROR_SCOPE", "ProviderRefusalMiddleware", "withdrawal_reminder"]

PROVIDER_ERROR_SCOPE = "provider"


def withdrawal_reminder(tool_names: list[str]) -> str:
    names = ", ".join(f"`{name}`" for name in tool_names)
    plural = len(tool_names) != 1
    return (
        "<system_reminder>\n"
        f"{names} {'are' if plural else 'is'} unavailable for the rest of this turn: the provider refused the request, and it will refuse every address the same way. "
        f"{'They have' if plural else 'It has'} been removed from your tools. Do not try to fetch again; answer from the search results you already have, "
        "cite them as search results, and tell the person plainly that the source pages could not be fetched.\n"
        "</system_reminder>"
    )


@dataclass
class _RunState:
    withdrawn: list[str] = field(default_factory=list)
    reminded: bool = False


def _run_key(runtime: Runtime | None) -> tuple[str, str]:
    context = getattr(runtime, "context", None) or {}
    return str(context.get("thread_id") or "default"), str(context.get("run_id") or "default")


def _result_messages(result: ToolMessage | Command) -> list[ToolMessage]:
    if isinstance(result, ToolMessage):
        return [result]
    update = getattr(result, "update", None)
    messages = update.get("messages") if isinstance(update, dict) else None
    if isinstance(messages, ToolMessage):
        return [messages]
    if isinstance(messages, list | tuple):
        return [message for message in messages if isinstance(message, ToolMessage)]
    return []


def _provider_refused(message: ToolMessage) -> bool:
    meta = (message.additional_kwargs or {}).get(TOOL_META_KEY)
    return isinstance(meta, dict) and meta.get("error_scope") == PROVIDER_ERROR_SCOPE


class ProviderRefusalMiddleware(AgentMiddleware[AgentState]):
    """Withdraw a tool from the model once its provider has refused this run."""

    def __init__(self, *, max_tracked_runs: int = 1000) -> None:
        super().__init__()
        self._lock = threading.Lock()
        self._runs: BoundedDict[tuple[str, str], _RunState] = BoundedDict(max_tracked_runs)

    def release_policy_parameters(self) -> dict[str, object]:
        return {"trigger": f"deerflow_tool_meta.error_scope == {PROVIDER_ERROR_SCOPE!r}", "scope": "run"}

    # -- state ---------------------------------------------------------------

    def _withdrawn(self, runtime: Runtime | None) -> list[str]:
        with self._lock:
            state = self._runs.get(_run_key(runtime))
            return list(state.withdrawn) if state is not None else []

    def _withdraw(self, runtime: Runtime | None, tool_name: str) -> None:
        with self._lock:
            state = self._runs.setdefault(_run_key(runtime), _RunState())
            if tool_name not in state.withdrawn:
                state.withdrawn.append(tool_name)
                state.reminded = False
                logger.info("Tool %s withdrawn for the rest of the run: its provider refused", tool_name)

    def _take_reminder(self, runtime: Runtime | None) -> list[str]:
        with self._lock:
            state = self._runs.get(_run_key(runtime))
            if state is None or state.reminded or not state.withdrawn:
                return []
            state.reminded = True
            return list(state.withdrawn)

    # -- tool calls ----------------------------------------------------------

    def _observe(self, request: ToolCallRequest, result: ToolMessage | Command) -> ToolMessage | Command:
        if any(_provider_refused(message) for message in _result_messages(result)):
            name = str(request.tool_call.get("name") or "")
            if name:
                self._withdraw(getattr(request, "runtime", None), name)
        return result

    def _already_withdrawn(self, request: ToolCallRequest) -> ToolMessage | None:
        name = str(request.tool_call.get("name") or "")
        if not name or name not in self._withdrawn(getattr(request, "runtime", None)):
            return None
        tool_call_id = str(request.tool_call.get("id") or "missing_tool_call_id")
        return ToolMessage(
            content=f"Error: {name} is unavailable for the rest of this turn: its provider refused this deployment's requests. Do not call it again; answer from what you already have and say that sources could not be fetched.",
            tool_call_id=tool_call_id,
            name=name,
            status="error",
            additional_kwargs={
                TOOL_META_KEY: {
                    "status": "error",
                    "error_type": "config",
                    "recoverable_by_model": False,
                    "recommended_next_action": "stop",
                    "source": "tool_return",
                    "error_scope": PROVIDER_ERROR_SCOPE,
                }
            },
        )

    @override
    def wrap_tool_call(self, request: ToolCallRequest, handler: Callable[[ToolCallRequest], ToolMessage | Command]) -> ToolMessage | Command:
        withdrawn = self._already_withdrawn(request)
        if withdrawn is not None:
            return withdrawn
        return self._observe(request, handler(request))

    @override
    async def awrap_tool_call(self, request: ToolCallRequest, handler: Callable[[ToolCallRequest], Awaitable[ToolMessage | Command]]) -> ToolMessage | Command:
        withdrawn = self._already_withdrawn(request)
        if withdrawn is not None:
            return withdrawn
        return self._observe(request, await handler(request))

    # -- model calls ---------------------------------------------------------

    def _without_withdrawn(self, request: ModelRequest) -> ModelRequest:
        runtime = getattr(request, "runtime", None)
        withdrawn = self._withdrawn(runtime)
        if not withdrawn:
            return request
        tools = [tool for tool in request.tools if getattr(tool, "name", None) not in withdrawn]
        messages = list(request.messages)
        reminder = self._take_reminder(runtime)
        if reminder:
            # Appended after every ToolMessage of the previous step, so the
            # provider's tool_call/tool_result pairing stays intact.
            messages.append(HumanMessage(content=withdrawal_reminder(reminder), additional_kwargs={"hide_from_ui": True}))
        return request.override(tools=tools, messages=messages)

    @override
    def wrap_model_call(self, request: ModelRequest, handler: Callable[[ModelRequest], ModelResponse]) -> ModelCallResult:
        return handler(self._without_withdrawn(request))

    @override
    async def awrap_model_call(self, request: ModelRequest, handler: Callable[[ModelRequest], Awaitable[ModelResponse]]) -> ModelCallResult:
        return await handler(self._without_withdrawn(request))
