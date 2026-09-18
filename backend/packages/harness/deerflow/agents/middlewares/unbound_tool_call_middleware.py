"""A tool call the runtime cannot honour is answered, not dispatched.

Why
---
In a released-profile qualification run, `the model` emitted a shell command
where a tool name belongs::

    weasyprint --version 2>/dev/null; python3 -c "import weasyprint; ..." || echo "checking..."

with only a ``description`` in its arguments. Nothing executed it -- but the
run still died. ``ToolReceiptMiddleware`` is the outermost tool wrapper, and it
writes durable start evidence *before* any inner code runs, so the first
boundary to look at that 129-byte name was the receipt's own safe-name rule.
It refused, correctly, and the refusal surfaced as ``ToolEvidenceError`` ->
``Runtime operation failed (reference: ...)``. The person asked for a PDF and
got an opaque failure; nine model calls and 168,865 tokens were already spent.

Two boundaries disagreed about what a tool name is. The receipt layer required
a syntactically safe one. Everything upstream accepted any nonblank string. A
disagreement like that cannot be fixed by making the strict side lenient --
that name must never become a canonical identity -- so it is fixed by asking
the strict side's own question earlier, while the answer is still recoverable.

What this does
--------------
It runs outermost of the tool wrappers, ahead of the receipt, and refuses a
call the runtime cannot honour:

* a name that could never be a receipt identity -- ``is_safe_tool_name`` from
  ``runtime/tool_evidence.py``, the module that owns the rule, asked here
  rather than restated;
* a name that is not bound for this run -- ``request.tool is None`` is the
  ``ToolNode``'s own answer about registration, so there is no name list here
  to drift from the tools actually bound.

The call is answered with one bounded ``ToolMessage`` the model can act on: it
says the call was not executed and names the tools this run actually has, taken
from the model request that offered them. Nothing is dispatched and no
reservation is made, so the ledger records the attempts that happened and this
non-attempt leaves no half-written trace; a corrected call gets an ordinary
receipt.

The name itself is treated as hostile input throughout. It is never the
``ToolMessage``'s ``name``, never an evidence key, and never logged -- the log
line carries its length and digest, which is enough to correlate with the
provider trace and carries none of the payload.
"""

from __future__ import annotations

import hashlib
import logging
import threading
from collections.abc import Awaitable, Callable
from typing import Any, override

from langchain.agents import AgentState
from langchain.agents.middleware import AgentMiddleware
from langchain.agents.middleware.types import ModelCallResult, ModelRequest, ToolCallRequest
from langchain_core.messages import ToolMessage
from langgraph.runtime import Runtime

from deerflow.agents.middlewares._bounded_dict import BoundedDict
from deerflow.agents.middlewares.tool_result_meta import TOOL_META_KEY
from deerflow.runtime.tool_evidence import is_safe_tool_name

logger = logging.getLogger(__name__)

__all__ = ["REFUSED_TOOL_NAME", "UnboundToolCallMiddleware", "unbound_tool_message"]

#: The safe stand-in a refused call is answered under. The model's own string
#: never reaches a message field, a receipt or a log.
REFUSED_TOOL_NAME = "unknown_tool"

#: Enough registered names to make the answer actionable without turning one
#: tool result into a catalogue.
MAX_NAMED_TOOLS = 24


def _run_key(runtime: Runtime | None) -> tuple[str, str]:
    context = getattr(runtime, "context", None) or {}
    return str(context.get("thread_id") or "default"), str(context.get("run_id") or "default")


def _name_fingerprint(name: object) -> str:
    """A correlatable reference to a name that must not be reproduced."""
    if not isinstance(name, str):
        return f"type={type(name).__name__}"
    return f"len={len(name)} sha256={hashlib.sha256(name.encode('utf-8', 'replace')).hexdigest()[:16]}"


def unbound_tool_message(tool_call_id: str, available: list[str]) -> ToolMessage:
    """The one bounded, recoverable result a refused call is answered with."""
    if available:
        offered = ", ".join(available[:MAX_NAMED_TOOLS])
        guidance = f"Reissue it as one of the tools available to you: {offered}. The tool name must be exactly one of those names; everything else -- a command line, a file path, a description -- belongs in the arguments."
    else:
        guidance = "Reissue it naming one of the tools available to you. The tool name must be exactly a tool's name; everything else -- a command line, a file path, a description -- belongs in the arguments."
    return ToolMessage(
        content=f"Error: this call was not executed, because the name it used is not one of the tools available to you. {guidance}",
        tool_call_id=tool_call_id,
        name=REFUSED_TOOL_NAME,
        status="error",
        additional_kwargs={
            TOOL_META_KEY: {
                "status": "error",
                "error_type": "invalid_tool_name",
                # The model can fix this itself, and the next call is the fix.
                "recoverable_by_model": True,
                "recommended_next_action": "try_alternative",
                "source": "tool_return",
                "error_scope": "origin",
            }
        },
    )


class UnboundToolCallMiddleware(AgentMiddleware[AgentState]):
    """Refuse a call the runtime cannot honour, while the model can still fix it."""

    def __init__(self, *, max_tracked_runs: int = 1000) -> None:
        super().__init__()
        self._lock = threading.Lock()
        self._offered: BoundedDict[tuple[str, str], list[str]] = BoundedDict(max_tracked_runs)

    def release_policy_parameters(self) -> dict[str, object]:
        return {"scope": "one tool call", "source": "bound tools + receipt name rule", "execution": "never"}

    # -- what this run was offered -------------------------------------------------

    def _remember(self, request: ModelRequest) -> None:
        names = [name for tool in (getattr(request, "tools", None) or []) if is_safe_tool_name(name := str(getattr(tool, "name", "") or ""))]
        if names:
            with self._lock:
                self._offered[_run_key(getattr(request, "runtime", None))] = names

    def _available(self, runtime: Runtime | None) -> list[str]:
        with self._lock:
            return list(self._offered.get(_run_key(runtime), []))

    @override
    def wrap_model_call(self, request: ModelRequest, handler: Callable[[ModelRequest], ModelCallResult]) -> ModelCallResult:
        self._remember(request)
        return handler(request)

    @override
    async def awrap_model_call(self, request: ModelRequest, handler: Callable[[ModelRequest], Awaitable[ModelCallResult]]) -> ModelCallResult:
        self._remember(request)
        return await handler(request)

    # -- the pre-dispatch boundary -------------------------------------------------

    def _refusal(self, request: ToolCallRequest) -> ToolMessage | None:
        """The answer for a call that must not be dispatched, or None to proceed."""
        tool_call = getattr(request, "tool_call", None) or {}
        name = tool_call.get("name")
        registered = getattr(request, "tool", None) is not None
        if is_safe_tool_name(name) and registered:
            return None
        tool_call_id = tool_call.get("id")
        if not isinstance(tool_call_id, str) or not tool_call_id.strip():
            # Without an id the model cannot be answered at all: a ToolMessage
            # is addressed by the call it answers. Let it through to the
            # existing malformed-id recovery rather than invent an address.
            return None
        logger.warning(
            "Refused a tool call before dispatch: name is %s (%s)",
            "not a registered tool" if is_safe_tool_name(name) else "not a usable tool name",
            _name_fingerprint(name),
        )
        return unbound_tool_message(tool_call_id, self._available(getattr(request, "runtime", None)))

    @override
    def wrap_tool_call(self, request: ToolCallRequest, handler: Callable[[ToolCallRequest], Any]) -> Any:
        return self._refusal(request) or handler(request)

    @override
    async def awrap_tool_call(self, request: ToolCallRequest, handler: Callable[[ToolCallRequest], Awaitable[Any]]) -> Any:
        refusal = self._refusal(request)
        if refusal is not None:
            return refusal
        return await handler(request)
