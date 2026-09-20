"""A deterministic, cost-free streaming chat model for turn-phase timing tests.

Loaded by the Gateway through the ordinary ``models[].use`` config path, so the
run that exercises it goes through the real model factory, the real lead-agent
graph, the real worker and the real SSE route. Nothing here is a stand-in for
those layers; only the inference is synthetic.

The script is chosen by the last user message so one server can serve every
case, and every delay is an ``asyncio.sleep`` so cancellation interrupts it
exactly as it would interrupt a provider call:

* ``probe:text``   -- a hidden reasoning block, a controlled pause, then three
                      text chunks, then a controlled tail before completion.
* ``probe:silent`` -- a hidden reasoning block, the pause, and no answer text.
* ``probe:hang``   -- a hidden reasoning block, then a long pause meant to be
                      cancelled from outside.
* ``probe:search`` -- one declared retrieval tool call, then the ordinary text
* ``probe:malformed <url>`` -- the exact tool call a released-profile
  qualification captured:
  a shell command where the tool name belongs. Then, once the runtime has
  answered it, an ordinary ``web_fetch`` for that address and the text script,
  so one turn shows the refusal and the recovery.
* ``probe:bash <command>`` -- one ``bash`` call running that command in the
  turn's sandbox, then the ordinary text answer. How a suite proves a role
  can still use a tool and the sandbox without a real model.
* ``probe:fetch <url>`` -- one ``web_fetch`` call for that address, then the
  ordinary text; every ``bind_tools`` call records the tool names it was
  given in :data:`BOUND_TOOL_NAMES`, so a test can see what the model could
  call on each request
                      script once the tool result comes back.

Delays are read from the environment when the model is built, so a test sets
them before the server starts and they apply to every run it serves.
"""

from __future__ import annotations

import asyncio
import json
import os
from collections.abc import AsyncIterator, Callable
from typing import Any

from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import AIMessage, AIMessageChunk, BaseMessage, HumanMessage
from langchain_core.outputs import ChatGeneration, ChatGenerationChunk, ChatResult
from langchain_core.runnables import Runnable

FIRST_TEXT_DELAY_ENV = "HARTMESH_PROBE_FIRST_TEXT_DELAY_S"
TAIL_DELAY_ENV = "HARTMESH_PROBE_TAIL_DELAY_S"
HANG_DELAY_ENV = "HARTMESH_PROBE_HANG_DELAY_S"

TEXT_CHUNKS = ("Hello", " from", " the probe.")
SEARCH_TOOL_NAME = "web_search"
SEARCH_QUERY = "what is the capital of france"
FETCH_TOOL_NAME = "web_fetch"
BASH_TOOL_NAME = "bash"
#: Verbatim from a released-profile qualification capture
#: ``the model`` sent this 129-byte shell command as the
#: tool *name*, carrying only a description in its arguments.
MALFORMED_TOOL_NAME = 'weasyprint --version 2>/dev/null; python3 -c "import weasyprint; print(weasyprint.__version__)" 2>/dev/null || echo "checking..."'
MALFORMED_TOOL_CALL_ID = "call_95d8f7f3448a413284e177bb"
MALFORMED_TOOL_ARGS = {"description": "Check weasyprint and create output dir"}
#: The tool names bound on every model request, in order. Process-global,
#: like the model instance the Gateway builds; a test clears it.
BOUND_TOOL_NAMES: list[list[str]] = []

#: Callbacks run once at the start of every streamed model call -- that is,
#: inside the agent loop.
#:
#: A test that has to write a file "as the turn produces it" hooks here rather
#: than from the SSE consumer. This is inside ``RuntimeDeliveryMiddleware``'s
#: ``before_agent``/``after_agent`` window by construction; the consumer races
#: both edges of it. Write too early and the file is already in the
#: middleware's snapshot, so nothing was produced; too late and the diff has
#: been taken -- and either way the worker's fence, whose own snapshot predates
#: the first published frame, still sees a file this turn made that nothing
#: presented, and the run ends ``artifact_delivery_incomplete``. The late edge
#: is the reachable one, because the client decides when it writes while the
#: run is free to finish. A test that sets this clears it.
ON_TURN_UNDER_WAY: list[Callable[[], None]] = []
_REASONING_BLOCK = [{"type": "reasoning", "reasoning": "hidden deliberation"}]


def _env_seconds(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    return max(0.0, float(raw))


def _script_for(messages: list[BaseMessage]) -> str:
    for message in reversed(messages):
        if isinstance(message, HumanMessage):
            content = message.content
            text = content if isinstance(content, str) else " ".join(str(block.get("text", "")) for block in content if isinstance(block, dict))
            if "probe:silent" in text:
                return "silent"
            if "probe:hang" in text:
                return "hang"
            if "probe:search" in text:
                return "search"
            if "probe:malformed" in text:
                return "malformed"
            if "probe:fetch" in text:
                return "fetch"
            if "probe:bash" in text:
                return "bash"
            return "text"
    return "text"


def _bash_command(messages: list[BaseMessage]) -> str:
    for message in reversed(messages):
        if isinstance(message, HumanMessage):
            content = message.content
            text = content if isinstance(content, str) else " ".join(str(block.get("text", "")) for block in content if isinstance(block, dict))
            if "probe:bash" in text:
                # The first line only: a middleware appends its own reminder
                # to the user's message, and that is not part of the command.
                return text.split("probe:bash", 1)[1].strip().splitlines()[0].strip()
    return "true"


def _fetch_url(messages: list[BaseMessage]) -> str:
    for message in reversed(messages):
        if isinstance(message, HumanMessage):
            content = message.content
            text = content if isinstance(content, str) else " ".join(str(block.get("text", "")) for block in content if isinstance(block, dict))
            for marker in ("probe:malformed", "probe:fetch"):
                if marker in text:
                    return text.split(marker, 1)[1].strip().split()[0]
    return "https://example.org/"


class ProbeStreamingChatModel(BaseChatModel):
    """Streams a scripted answer with controlled delays; no network, no key."""

    model: str = "probe"
    first_text_delay_s: float = 0.0
    tail_delay_s: float = 0.0
    hang_delay_s: float = 30.0

    def __init__(self, **kwargs: Any) -> None:
        kwargs.setdefault("first_text_delay_s", _env_seconds(FIRST_TEXT_DELAY_ENV, 0.0))
        kwargs.setdefault("tail_delay_s", _env_seconds(TAIL_DELAY_ENV, 0.0))
        kwargs.setdefault("hang_delay_s", _env_seconds(HANG_DELAY_ENV, 30.0))
        # The factory forwards profile settings a real provider would consume;
        # the probe has no provider and ignores anything it does not declare.
        known = set(type(self).model_fields)
        super().__init__(**{key: value for key, value in kwargs.items() if key in known})

    @property
    def _llm_type(self) -> str:
        return "hartmesh-turn-phase-probe"

    def bind_tools(self, tools: Any, *, tool_choice: Any = None, **kwargs: Any) -> Runnable:  # type: ignore[override]
        BOUND_TOOL_NAMES.append([str(getattr(tool, "name", None) or (tool.get("name") if isinstance(tool, dict) else tool)) for tool in (tools or [])])
        return self

    @staticmethod
    def _tool_results_this_turn(messages: list[BaseMessage]) -> int:
        """How many tool results this turn has already collected."""
        latest_user = -1
        for index, message in enumerate(messages):
            if isinstance(message, HumanMessage) and not (getattr(message, "additional_kwargs", None) or {}).get("hide_from_ui"):
                latest_user = index
        return sum(1 for message in messages[latest_user + 1 :] if getattr(message, "type", "") == "tool")

    @staticmethod
    def _already_called_tool(messages: list[BaseMessage]) -> bool:
        """Whether this turn has already had its one scripted tool call.

        Scoped to the messages after the latest real user message, not to the
        whole thread: a second turn in the same chat carries the first turn's
        tool result in history, and a thread-wide scan would make the probe
        answer it from nothing -- which is not what the model it stands in for
        would do, and would leave a same-chat follow-up untestable.
        """
        latest_user = -1
        for index, message in enumerate(messages):
            if isinstance(message, HumanMessage) and not (getattr(message, "additional_kwargs", None) or {}).get("hide_from_ui"):
                latest_user = index
        return any(getattr(message, "type", "") == "tool" for message in messages[latest_user + 1 :])

    def _generate(self, messages: list[BaseMessage], stop: list[str] | None = None, run_manager: Any = None, **kwargs: Any) -> ChatResult:
        script = _script_for(messages)
        content: list[dict[str, Any]] = list(_REASONING_BLOCK)
        # The agent loop streams; this path is for the side-channel calls a
        # middleware makes with its own trimmed message list. Answering those
        # with a tool call would dispatch a tool nobody asked for, so the
        # retrieval script is scripted only in ``_astream``.
        if script in ("text", "search", "fetch", "malformed", "bash"):
            content.append({"type": "text", "text": "".join(TEXT_CHUNKS)})
        return ChatResult(generations=[ChatGeneration(message=AIMessage(content=content))])

    async def _astream(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: Any = None,
        **kwargs: Any,
    ) -> AsyncIterator[ChatGenerationChunk]:
        script = _script_for(messages)
        for under_way in list(ON_TURN_UNDER_WAY):
            under_way()
        # Hidden reasoning first: bytes on the wire, but not the answer.
        yield ChatGenerationChunk(message=AIMessageChunk(content=list(_REASONING_BLOCK)))
        if script == "search" and not self._already_called_tool(messages):
            # Exactly one declared retrieval call. The graph comes back here
            # with the tool result, and the second pass answers as usual.
            yield ChatGenerationChunk(
                message=AIMessageChunk(
                    content=[],
                    tool_call_chunks=[
                        {
                            "name": SEARCH_TOOL_NAME,
                            "args": f'{{"query": "{SEARCH_QUERY}", "max_results": 3}}',
                            "id": "probe-search-1",
                            "index": 0,
                            "type": "tool_call_chunk",
                        }
                    ],
                )
            )
            return
        if script == "malformed":
            # Stage one is the captured shape; stage two is the same model
            # recovering with a call the runtime can honour. Anything after
            # that falls through to the ordinary text script.
            stage = self._tool_results_this_turn(messages)
            if stage < 2:
                call = (
                    {"name": MALFORMED_TOOL_NAME, "args": json.dumps(MALFORMED_TOOL_ARGS), "id": MALFORMED_TOOL_CALL_ID, "index": 0, "type": "tool_call_chunk"}
                    if stage == 0
                    else {"name": FETCH_TOOL_NAME, "args": json.dumps({"url": _fetch_url(messages)}), "id": "probe-fetch-after-refusal", "index": 0, "type": "tool_call_chunk"}
                )
                yield ChatGenerationChunk(message=AIMessageChunk(content=[], tool_call_chunks=[call]))
                return
        if script == "bash" and not self._already_called_tool(messages):
            yield ChatGenerationChunk(
                message=AIMessageChunk(
                    content=[],
                    tool_call_chunks=[
                        {
                            "name": BASH_TOOL_NAME,
                            "args": json.dumps({"command": _bash_command(messages), "description": "probe"}),
                            "id": "probe-bash-1",
                            "index": 0,
                            "type": "tool_call_chunk",
                        }
                    ],
                )
            )
            return
        if script == "fetch" and not self._already_called_tool(messages):
            yield ChatGenerationChunk(
                message=AIMessageChunk(
                    content=[],
                    tool_call_chunks=[
                        {
                            "name": FETCH_TOOL_NAME,
                            "args": json.dumps({"url": _fetch_url(messages)}),
                            "id": "probe-fetch-1",
                            "index": 0,
                            "type": "tool_call_chunk",
                        }
                    ],
                )
            )
            return
        if script == "hang":
            await asyncio.sleep(self.hang_delay_s)
            return
        await asyncio.sleep(self.first_text_delay_s)
        if script == "silent":
            return
        for piece in TEXT_CHUNKS:
            yield ChatGenerationChunk(message=AIMessageChunk(content=[{"type": "text", "text": piece}]))
            await asyncio.sleep(0.01)
        await asyncio.sleep(self.tail_delay_s)
