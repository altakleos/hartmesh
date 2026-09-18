"""A tool name the runtime cannot honour is answered, never dispatched.

The shape a released-profile qualification captured, replayed byte for byte:
the model put a 129-byte shell command where a tool name
belongs, with only a ``description`` in its arguments. The receipt layer refused
the name -- correctly -- and that refusal became
``Runtime operation failed (reference: ...)`` for a person who had asked for a
PDF.

These tests drive the guard the way the agent loop does, and pin the three
properties the repair is for: nothing executes, the model gets one result it
can act on, and the hostile string never becomes an identity.
"""

from __future__ import annotations

import asyncio
import logging
from types import SimpleNamespace
from typing import Any

import pytest
from langchain_core.messages import AIMessage, ToolMessage

from deerflow.agents.middlewares.tool_result_meta import TOOL_META_KEY
from deerflow.agents.middlewares.unbound_tool_call_middleware import (
    REFUSED_TOOL_NAME,
    UnboundToolCallMiddleware,
)
from deerflow.runtime.tool_evidence import MAX_TOOL_NAME_BYTES, ToolEvidenceError, is_safe_tool_name

#: Verbatim from the capture: the whole 129-byte string Ling sent as the name.
CAPTURED_NAME = 'weasyprint --version 2>/dev/null; python3 -c "import weasyprint; print(weasyprint.__version__)" 2>/dev/null || echo "checking..."'
CAPTURED_ID = "call_95d8f7f3448a413284e177bb"
CAPTURED_ARGS = {"description": "Check weasyprint and create output dir"}

BOUND_TOOLS = ["bash", "write_file", "read_file", "web_search", "present_files"]


def _runtime(run_id: str = "run-1") -> SimpleNamespace:
    return SimpleNamespace(context={"thread_id": "t1", "run_id": run_id})


class _ModelRequest:
    def __init__(self, tools: list[Any], runtime: SimpleNamespace) -> None:
        self.tools = tools
        self.messages: list[Any] = []
        self.runtime = runtime


def _tool_request(name: object, runtime: SimpleNamespace, *, registered: bool, call_id: object = CAPTURED_ID, args: dict | None = None) -> SimpleNamespace:
    """A ToolCallRequest as the graph builds it.

    ``tool`` is the ``ToolNode``'s own answer about registration: ``None`` when
    nothing is bound under that name, which is the fact the guard reads instead
    of keeping a name list of its own.
    """
    return SimpleNamespace(
        tool_call={"name": name, "args": args if args is not None else CAPTURED_ARGS, "id": call_id},
        tool=SimpleNamespace(name=name) if registered else None,
        state={},
        runtime=runtime,
    )


def _offer_tools(middleware: UnboundToolCallMiddleware, runtime: SimpleNamespace, tools: list[str] = BOUND_TOOLS) -> None:
    """One model pass, so the middleware has seen what this run was offered."""
    middleware.wrap_model_call(_ModelRequest([SimpleNamespace(name=name) for name in tools], runtime), lambda _request: AIMessage("ok"))


def _dispatch(middleware: UnboundToolCallMiddleware, request: SimpleNamespace) -> tuple[Any, list[str]]:
    """Run one tool call through the guard, recording whether the tool ran."""
    executed: list[str] = []

    def handler(inner: SimpleNamespace) -> ToolMessage:
        executed.append(str(inner.tool_call.get("name")))
        return ToolMessage("ran", tool_call_id=str(inner.tool_call.get("id")), name=str(inner.tool_call.get("name")))

    return middleware.wrap_tool_call(request, handler), executed


def test_the_captured_shell_command_as_a_tool_name_is_refused_before_anything_runs() -> None:
    middleware, runtime = UnboundToolCallMiddleware(), _runtime()
    _offer_tools(middleware, runtime)

    result, executed = _dispatch(middleware, _tool_request(CAPTURED_NAME, runtime, registered=False))

    assert executed == [], "the whole point: no shell, no dispatch, nothing downstream of here"
    assert isinstance(result, ToolMessage)
    assert result.tool_call_id == CAPTURED_ID, "a result is addressed by the call it answers"
    assert result.status == "error"
    meta = result.additional_kwargs[TOOL_META_KEY]
    assert meta["recoverable_by_model"] is True, "the model's next call is the fix; ending the run is not"
    assert meta["error_type"] == "invalid_tool_name"


def test_the_refusal_names_the_tools_this_run_actually_has() -> None:
    # Actionable means naming the interface, and the names come from what the
    # model was offered on its own request -- not from a list kept here that
    # could drift from the tools actually bound.
    middleware, runtime = UnboundToolCallMiddleware(), _runtime()
    _offer_tools(middleware, runtime)

    result, _ = _dispatch(middleware, _tool_request(CAPTURED_NAME, runtime, registered=False))

    assert "bash" in result.content and "write_file" in result.content
    assert "belongs in the arguments" in result.content, "it must say where the command should have gone"


def test_the_model_supplied_name_never_becomes_an_identity(caplog: pytest.LogCaptureFixture) -> None:
    """The string is hostile input and is treated as such everywhere.

    It is not the result's ``name`` (which is what a receipt and the UI read),
    it is not echoed into the content, and it is not logged -- the log carries
    a length and a digest, which correlates with the provider trace and
    reproduces none of the payload.
    """
    middleware, runtime = UnboundToolCallMiddleware(), _runtime()
    _offer_tools(middleware, runtime)

    with caplog.at_level(logging.WARNING):
        result, _ = _dispatch(middleware, _tool_request(CAPTURED_NAME, runtime, registered=False))

    assert result.name == REFUSED_TOOL_NAME
    assert is_safe_tool_name(result.name), "whatever answers the call must itself be a usable receipt identity"
    assert "weasyprint" not in result.content
    assert "2>/dev/null" not in caplog.text and "weasyprint" not in caplog.text
    assert "len=129" in caplog.text, "enough to correlate with the trace"


def test_a_registered_call_is_untouched() -> None:
    middleware, runtime = UnboundToolCallMiddleware(), _runtime()
    _offer_tools(middleware, runtime)

    result, executed = _dispatch(middleware, _tool_request("bash", runtime, registered=True, call_id="c2", args={"command": "ls"}))

    assert executed == ["bash"], "the guard is invisible to every call that can be honoured"
    assert isinstance(result, ToolMessage) and result.content == "ran"


@pytest.mark.parametrize(
    ("name", "registered", "why"),
    [
        ("  ", False, "whitespace only"),
        ("rm -rf /", False, "shell punctuation"),
        ("tool;name", False, "a separator"),
        ("tool name", False, "an inner space"),
        ("x" * (MAX_TOOL_NAME_BYTES + 1), False, "overlength"),
        ("ünïcödé_tool", False, "outside the safe set"),
        (None, False, "absent"),
        (12, False, "not a string"),
        ("not_a_real_tool", False, "syntactically fine, but nothing is bound under it"),
    ],
)
def test_every_name_the_runtime_cannot_honour_is_refused(name: object, registered: bool, why: str) -> None:
    middleware, runtime = UnboundToolCallMiddleware(), _runtime()
    _offer_tools(middleware, runtime)

    result, executed = _dispatch(middleware, _tool_request(name, runtime, registered=registered, call_id="c3"))

    assert executed == [], f"{why}: must not dispatch"
    assert isinstance(result, ToolMessage) and result.status == "error"


def test_a_partially_streamed_call_is_refused_without_needing_its_arguments() -> None:
    """The capture's first frame carried the name with ``args: null``.

    Providers stream a tool call in pieces, and the name arrives before the
    arguments do. The guard reads only the name and the binding, so a call
    whose arguments have not assembled yet is refused on the same grounds and
    never raises on the missing half.
    """
    middleware, runtime = UnboundToolCallMiddleware(), _runtime()
    _offer_tools(middleware, runtime)

    partial = _tool_request(CAPTURED_NAME, runtime, registered=False, call_id=CAPTURED_ID)
    partial.tool_call["args"] = None

    result, executed = _dispatch(middleware, partial)

    assert executed == []
    assert isinstance(result, ToolMessage) and result.name == REFUSED_TOOL_NAME


def test_a_safe_name_bound_under_a_different_spelling_still_dispatches() -> None:
    # Registration is the ToolNode's answer, not a guess from the string: a
    # name this guard has never seen is fine as long as something is bound.
    middleware, runtime = UnboundToolCallMiddleware(), _runtime()
    _offer_tools(middleware, runtime, ["bash"])

    _, executed = _dispatch(middleware, _tool_request("mcp:acme/do-thing", runtime, registered=True, call_id="c4"))

    assert executed == ["mcp:acme/do-thing"]


def test_a_tool_actually_bound_under_an_unusable_name_is_still_refused() -> None:
    """Registration is not enough; the name must also be able to be evidence.

    An MCP server names its own tools, so a tool really can be bound under a
    string the receipt layer would refuse as an identity. Dispatching it would
    reach the same terminal ``ToolEvidenceError`` by a different road, so the
    guard applies the receipt's rule even when the ``ToolNode`` says the tool
    exists. This is the half of the check that registration cannot cover.
    """
    middleware, runtime = UnboundToolCallMiddleware(), _runtime()
    _offer_tools(middleware, runtime)

    hostile = "do thing; echo hi"
    result, executed = _dispatch(middleware, _tool_request(hostile, runtime, registered=True, call_id="c5"))

    assert executed == [], "bound, and still not dispatchable: the receipt could never name it"
    assert isinstance(result, ToolMessage) and result.name == REFUSED_TOOL_NAME


def test_a_call_with_no_usable_id_is_still_refused_rather_than_dispatched() -> None:
    """A missing address is not a reason to run it.

    An earlier version of this guard let such a call through, reasoning that a
    ``ToolMessage`` is addressed by the call it answers and the malformed-id
    recovery would deal with it. That recovery runs on the *next* model
    request, and the receipt reserves this call's evidence before then -- so
    the fall-through dispatched exactly the shape this guard exists to stop,
    and reached the same terminal error by the same route. A degenerate model
    output is also the case most likely to corrupt the name and the id
    together.

    The answer goes out with a blank address. The orphan pass drops it and
    ``DanglingToolCallMiddleware`` patches the unanswered call on the next
    request, so the model is still told; nothing executes either way.
    """
    middleware, runtime = UnboundToolCallMiddleware(), _runtime()
    _offer_tools(middleware, runtime)

    for missing in ("", "   ", None, 7):
        result, executed = _dispatch(middleware, _tool_request(CAPTURED_NAME, runtime, registered=False, call_id=missing))
        assert executed == [], f"id={missing!r}: dispatching is what produced the terminal error"
        assert isinstance(result, ToolMessage) and result.name == REFUSED_TOOL_NAME


def test_the_async_path_behaves_the_same() -> None:
    middleware, runtime = UnboundToolCallMiddleware(), _runtime()
    executed: list[str] = []

    async def handler(inner: SimpleNamespace) -> ToolMessage:
        executed.append(str(inner.tool_call.get("name")))
        return ToolMessage("ran", tool_call_id="x", name="bash")

    async def scenario() -> Any:
        await middleware.awrap_model_call(_ModelRequest([SimpleNamespace(name="bash")], runtime), _ok_model)
        return await middleware.awrap_tool_call(_tool_request(CAPTURED_NAME, runtime, registered=False), handler)

    result = asyncio.run(scenario())
    assert executed == []
    assert isinstance(result, ToolMessage) and result.name == REFUSED_TOOL_NAME


async def _ok_model(_request: Any) -> AIMessage:
    return AIMessage("ok")


def test_the_guard_and_the_receipt_layer_answer_the_same_question() -> None:
    """One rule, asked twice, never two rules that can disagree.

    This is the defect itself: the receipt's safe-name constraint and the
    upstream check were different questions, so a name could pass one and fail
    the other with the failure arriving too late to recover. Every name the
    guard admits must be one the receipt layer would accept as an identity.
    """
    from deerflow.runtime.tool_evidence import _validate_tool_name

    for name in ["bash", "mcp:acme/do-thing", "a.b-c_d", "x" * MAX_TOOL_NAME_BYTES]:
        assert is_safe_tool_name(name)
        assert _validate_tool_name(name) == name

    for name in [CAPTURED_NAME, "", "  ", "tool name", "x" * (MAX_TOOL_NAME_BYTES + 1), None, 12]:
        assert not is_safe_tool_name(name)
        with pytest.raises(ToolEvidenceError):
            _validate_tool_name(name)


def test_the_guard_is_outermost_so_nothing_is_reserved_for_a_refused_call() -> None:
    """Placement is the repair.

    ``ToolReceiptMiddleware`` writes durable start evidence before any inner
    code runs, so a guard *inside* it would refuse a name the receipt had
    already tried to make an identity -- which is exactly the failure. The
    guard has to be outside it.
    """
    from deerflow.agents.middlewares.tool_error_handling_middleware import build_lead_runtime_middlewares
    from deerflow.agents.middlewares.tool_receipt_middleware import ToolReceiptMiddleware
    from deerflow.config.app_config import AppConfig
    from deerflow.config.sandbox_config import SandboxConfig

    names = [type(middleware).__name__ for middleware in build_lead_runtime_middlewares(app_config=AppConfig(models=[], sandbox=SandboxConfig(use="test")), lazy_init=True)]
    assert names.index("UnboundToolCallMiddleware") < names.index(ToolReceiptMiddleware.__name__)
