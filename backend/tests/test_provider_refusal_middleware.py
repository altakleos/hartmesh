"""A tool whose provider refused this run disappears from the model's tools.

The tenant-class evidence: thirteen ``web_fetch`` calls to thirteen distinct
addresses, each answered by the provider's same deterministic 401. The
argument-keyed repeated-tool policy could not see it. These tests drive the
middleware the way the agent loop does -- a tool result comes back, the next
model request is built -- and check that a provider-scope stamp withdraws
the tool while an origin-scope stamp (one page's 403) changes nothing.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langgraph.types import Command

from deerflow.agents.middlewares.provider_refusal_middleware import ProviderRefusalMiddleware, withdrawal_reminder
from deerflow.agents.middlewares.tool_result_meta import TOOL_META_KEY


def _runtime(thread_id: str = "t1", run_id: str = "r1") -> SimpleNamespace:
    return SimpleNamespace(context={"thread_id": thread_id, "run_id": run_id})


def _tool_request(name: str, runtime: SimpleNamespace, call_id: str = "c1") -> SimpleNamespace:
    return SimpleNamespace(tool_call={"name": name, "args": {"url": "https://example.org/"}, "id": call_id}, runtime=runtime, state={})


def _stamped(name: str, scope: str, call_id: str = "c1") -> ToolMessage:
    return ToolMessage(
        "Error: refused",
        tool_call_id=call_id,
        name=name,
        status="error",
        additional_kwargs={TOOL_META_KEY: {"status": "error", "error_type": "auth", "recoverable_by_model": scope != "provider", "recommended_next_action": "stop", "source": "tool_return", "error_scope": scope}},
    )


class _ModelRequest:
    def __init__(self, tools: list[Any], messages: list[Any], runtime: SimpleNamespace) -> None:
        self.tools = tools
        self.messages = messages
        self.runtime = runtime

    def override(self, **changes: Any) -> _ModelRequest:
        updated = _ModelRequest(self.tools, self.messages, self.runtime)
        for key, value in changes.items():
            setattr(updated, key, value)
        return updated


def _tools(*names: str) -> list[SimpleNamespace]:
    return [SimpleNamespace(name=name) for name in names]


def _model_pass(middleware: ProviderRefusalMiddleware, runtime: SimpleNamespace, messages: list[Any] | None = None) -> _ModelRequest:
    seen: list[_ModelRequest] = []

    def handler(request: _ModelRequest) -> Any:
        seen.append(request)
        return AIMessage("ok")

    middleware.wrap_model_call(_ModelRequest(_tools("web_search", "web_fetch", "bash"), messages or [HumanMessage("hi")], runtime), handler)
    return seen[0]


def test_a_provider_scope_refusal_withdraws_the_tool_and_reminds_the_model_once() -> None:
    middleware = ProviderRefusalMiddleware()
    runtime = _runtime()
    result = middleware.wrap_tool_call(_tool_request("web_fetch", runtime), lambda _request: _stamped("web_fetch", "provider"))
    assert isinstance(result, ToolMessage), "the refusal itself reaches the model unchanged"

    first = _model_pass(middleware, runtime)
    assert [tool.name for tool in first.tools] == ["web_search", "bash"], "the model cannot call what it cannot see"
    assert isinstance(first.messages[-1], HumanMessage) and first.messages[-1].content == withdrawal_reminder(["web_fetch"])
    assert first.messages[-1].additional_kwargs.get("hide_from_ui") is True

    second = _model_pass(middleware, runtime)
    assert [tool.name for tool in second.tools] == ["web_search", "bash"], "withdrawn for the rest of the run"
    assert not any(isinstance(message, HumanMessage) and "unavailable" in str(message.content) for message in second.messages), "one reminder, not one per request"


def test_an_origin_scope_refusal_withdraws_nothing() -> None:
    # A paywalled page's 403 says nothing about the next address.
    middleware = ProviderRefusalMiddleware()
    runtime = _runtime()
    middleware.wrap_tool_call(_tool_request("web_fetch", runtime), lambda _request: _stamped("web_fetch", "origin"))
    request = _model_pass(middleware, runtime)
    assert [tool.name for tool in request.tools] == ["web_search", "web_fetch", "bash"]
    assert len(request.messages) == 1


def test_a_call_already_emitted_after_the_refusal_is_answered_without_invoking_the_tool() -> None:
    middleware = ProviderRefusalMiddleware()
    runtime = _runtime()
    middleware.wrap_tool_call(_tool_request("web_fetch", runtime, "c1"), lambda _request: _stamped("web_fetch", "provider", "c1"))
    invoked: list[str] = []

    def handler(_request: Any) -> ToolMessage:
        invoked.append("yes")
        return ToolMessage("page", tool_call_id="c2", name="web_fetch")

    answer = middleware.wrap_tool_call(_tool_request("web_fetch", runtime, "c2"), handler)
    assert invoked == []
    assert isinstance(answer, ToolMessage) and answer.tool_call_id == "c2" and answer.status == "error"
    assert answer.additional_kwargs[TOOL_META_KEY]["error_scope"] == "provider"
    assert "unavailable for the rest of this turn" in answer.content


def test_a_stamp_inside_a_command_result_counts_too() -> None:
    middleware = ProviderRefusalMiddleware()
    runtime = _runtime()
    command = Command(update={"messages": [_stamped("web_fetch", "provider")]})
    middleware.wrap_tool_call(_tool_request("web_fetch", runtime), lambda _request: command)
    assert [tool.name for tool in _model_pass(middleware, runtime).tools] == ["web_search", "bash"]


def test_the_withdrawal_is_scoped_to_the_run_that_saw_the_refusal() -> None:
    middleware = ProviderRefusalMiddleware()
    middleware.wrap_tool_call(_tool_request("web_fetch", _runtime("t1", "r1")), lambda _request: _stamped("web_fetch", "provider"))
    same_thread_next_run = _model_pass(middleware, _runtime("t1", "r2"))
    assert [tool.name for tool in same_thread_next_run.tools] == ["web_search", "web_fetch", "bash"], "the next turn finds out for itself, with one call"
    other_thread = _model_pass(middleware, _runtime("t2", "r1"))
    assert [tool.name for tool in other_thread.tools] == ["web_search", "web_fetch", "bash"]


def test_words_alone_withdraw_nothing() -> None:
    # The same sentence a provider refusal carries, with an origin stamp or no
    # stamp at all: the contract is the stamp, never the text.
    middleware = ProviderRefusalMiddleware()
    runtime = _runtime()
    unstamped = ToolMessage("Error: web_fetch is unavailable for the rest of this turn: 401 authentication required", tool_call_id="c1", name="web_fetch", status="error")
    middleware.wrap_tool_call(_tool_request("web_fetch", runtime), lambda _request: unstamped)
    assert [tool.name for tool in _model_pass(middleware, runtime).tools] == ["web_search", "web_fetch", "bash"]


def test_the_async_hooks_behave_the_same() -> None:
    middleware = ProviderRefusalMiddleware()
    runtime = _runtime()

    async def refuse(_request: Any) -> ToolMessage:
        return _stamped("web_fetch", "provider")

    async def scenario() -> list[str]:
        await middleware.awrap_tool_call(_tool_request("web_fetch", runtime), refuse)
        seen: list[_ModelRequest] = []

        async def model(request: _ModelRequest) -> Any:
            seen.append(request)
            return AIMessage("ok")

        await middleware.awrap_model_call(_ModelRequest(_tools("web_search", "web_fetch"), [HumanMessage("hi")], runtime), model)
        return [tool.name for tool in seen[0].tools]

    assert asyncio.run(scenario()) == ["web_search"]


def test_the_middleware_is_in_the_lead_runtime_stack_outside_the_stamping_step() -> None:
    from deerflow.agents.middlewares.tool_error_handling_middleware import ToolErrorHandlingMiddleware, build_lead_runtime_middlewares
    from deerflow.config.app_config import AppConfig
    from deerflow.config.sandbox_config import SandboxConfig

    middlewares = build_lead_runtime_middlewares(app_config=AppConfig(models=[], sandbox=SandboxConfig(use="test")), lazy_init=True)
    names = [type(middleware).__name__ for middleware in middlewares]
    assert "ProviderRefusalMiddleware" in names
    assert names.index("ProviderRefusalMiddleware") < names.index(ToolErrorHandlingMiddleware.__name__)
