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
from collections.abc import AsyncIterator
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
#: The tool names bound on every model request, in order. Process-global,
#: like the model instance the Gateway builds; a test clears it.
BOUND_TOOL_NAMES: list[list[str]] = []
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
            if "probe:fetch" in text:
                return "fetch"
            return "text"
    return "text"


def _fetch_url(messages: list[BaseMessage]) -> str:
    for message in reversed(messages):
        if isinstance(message, HumanMessage):
            content = message.content
            text = content if isinstance(content, str) else " ".join(str(block.get("text", "")) for block in content if isinstance(block, dict))
            marker = "probe:fetch"
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
    def _already_called_tool(messages: list[BaseMessage]) -> bool:
        return any(getattr(message, "type", "") == "tool" for message in messages)

    def _generate(self, messages: list[BaseMessage], stop: list[str] | None = None, run_manager: Any = None, **kwargs: Any) -> ChatResult:
        script = _script_for(messages)
        content: list[dict[str, Any]] = list(_REASONING_BLOCK)
        # The agent loop streams; this path is for the side-channel calls a
        # middleware makes with its own trimmed message list. Answering those
        # with a tool call would dispatch a tool nobody asked for, so the
        # retrieval script is scripted only in ``_astream``.
        if script in ("text", "search", "fetch"):
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
