"""A turn that made files for the user hands them over, whether or not the model says so.

Why
---
Files reach a person only through a presentation, and until now every
presentation was a model judgement: ``present_files`` as a separate call, or
the ``present`` argument on the call that makes the files. That judgement has
now failed on the tenant class twice. DF17 was the business-report skill
rewriting four files and answering "Done"; DF22 was an ordinary request --
"Create pdf about muse agent" -- where ``bash`` wrote a valid 14,710-byte PDF
without naming it under ``present`` and the agent never called
``present_files``. Both runs were correctly failed by the delivery fence, and
both users had asked for a file that existed.

Three releases went into that failure before this one: DF13 gave the browser a
live notice, DF14 made the notice durable as a receipt, DF17 added the
``present`` argument. The first two report the failure better. The third makes
it less likely. None removes the dependency, which is why it keeps happening.

What this does
--------------
The runtime already knows the answer at the moment it decides to fail: the
fence computes the exact set of files the turn created or changed under this
conversation's outputs directory, and subtracts the ones that were presented.
That set is typed, ownership-scoped and computed from the filesystem, not from
anything the model said. This middleware presents it.

At the start of the agent it snapshots the same roots the fence scans; at the
end it diffs them, subtracts what this turn already presented, and hands the
remainder over: an ``artifacts`` update for the workspace panel, and the
``presented_files`` tag on the turn's final assistant message so the files also
appear as chips under the answer, which is where a person actually looks (the
browser draws chips from a ``present_files`` call or from a tagged message --
``frontend-hm/src/core/messages/utils.ts``). The same list goes into
``runtime.context`` for the worker, which merges it into the delivery receipt;
without that the fence would still see nothing presented and fail a run whose
files had just been handed over. Writing a fact into ``runtime.context`` for
the worker to read after the graph is the channel the guard middlewares
already use for ``stop_reason``.

What this is not
----------------
It does not parse output, take the model's word, widen path authorization, or
publish arbitrary changed files. The set is exactly what the fence already
asserts *must* be delivered: regular files, under this thread and user's own
outputs root, created or changed by this turn, with the tool-output storage
subdirectory excluded by the same scanner the worker uses. If this set is the
wrong thing to deliver, the fence is wrong to demand it.

Model curation still wins. When the model names files under ``present`` or
calls ``present_files``, its selection stands and only files it left out are
added. A delegated task presents nothing, the same rule the typed tool
enforces: a subagent reports its paths to the agent that delegated, and that
agent presents. The cost, stated plainly: on a turn where the model curates
nothing, an intermediate left in ``outputs`` (a ``report.md`` beside the
``report.pdf``) is handed over too. That is worse than curation and much
better than an error for a file the person asked for. Nothing under
``workspace`` is ever touched.

Clarifying questions are covered
-------------------------------
A turn that asks the person something is a *completed* run, not a suspended
one. ``ClarificationMiddleware`` ends the turn with ``Command(goto=END)``, and
``create_agent`` compiles that jump with ``end_destination=exit_node`` -- the
last ``after_agent`` node, not raw ``END``. So ``after_agent`` runs, and a file
the model drafted before asking is handed over rather than left to fail the
fence. The
tenant class produces exactly this shape -- the `.21` capture of "Create pdf
about muse agent" is a 21.6 s turn offering three options followed by a
152.8 s turn that writes the PDF.

Known gap
---------
``after_agent`` does not run when a graph *interrupts*, so a turn suspended
mid-flight would hand nothing over, and the resumed run's snapshot is taken
after the file already exists, so its own scan finds nothing produced. No path
in this product reaches that shape: nothing in the harness or the app calls
``interrupt()`` or configures ``interrupt_before``/``interrupt_after``. The
consequence is pinned in ``tests/test_runtime_delivery_middleware.py`` so that
introducing an interrupt path is a deliberate act with a known cost.
"""

from __future__ import annotations

import logging
import threading
from typing import Any, override

from langchain.agents import AgentState
from langchain.agents.middleware import AgentMiddleware
from langchain_core.messages import AIMessage, HumanMessage
from langgraph.runtime import Runtime

from deerflow.agents.middlewares._bounded_dict import BoundedDict
from deerflow.runtime.presented_files import (
    PRESENTED_BY_KEY,
    PRESENTED_FILES_KEY,
    RUNTIME_PRESENTED_FILES_CONTEXT_KEY,
    presented_files_of,
)
from deerflow.runtime.user_context import get_effective_user_id
from deerflow.sandbox.lease import sandbox_command_scope
from deerflow.workspace_changes.diff import get_changed_output_paths
from deerflow.workspace_changes.recorder import capture_workspace_snapshot

logger = logging.getLogger(__name__)

__all__ = ["RUNTIME_PRESENTED_FILES_CONTEXT_KEY", "PRESENTED_BY_KEY", "RuntimeDeliveryMiddleware"]

#: One run's worth of files; the same bound ``present`` puts on one call.
MAX_RUNTIME_PRESENTED = 64


def _run_key(runtime: Runtime | None) -> tuple[str, str]:
    context = getattr(runtime, "context", None) or {}
    return str(context.get("thread_id") or "default"), str(context.get("run_id") or "default")


def _thread_id(runtime: Runtime | None) -> str | None:
    context = getattr(runtime, "context", None) or {}
    thread_id = context.get("thread_id")
    return str(thread_id) if thread_id else None


def _messages_of_current_turn(state: Any) -> list[Any]:
    """Messages after the latest real user message.

    Scoped to the turn rather than the thread on purpose: a file this turn
    rewrote may carry the same path the model presented last turn, and the
    person is owed the new version.
    """
    messages = (state or {}).get("messages") if isinstance(state, dict) else None
    if not isinstance(messages, list):
        return []
    latest_user = -1
    for index, message in enumerate(messages):
        if isinstance(message, HumanMessage) and not (message.additional_kwargs or {}).get("hide_from_ui"):
            latest_user = index
    return list(messages[latest_user + 1 :])


def _already_presented(turn_messages: list[Any]) -> set[str]:
    """Every path this turn has handed over, whichever message carries the tag."""
    presented: set[str] = set()
    for message in turn_messages:
        presented.update(presented_files_of(message))
    return presented


def _covered(presented: set[str], produced: str) -> bool:
    """Whether a presented path already covers this produced file.

    The same rule the fence applies, so a directory the model presented
    accounts for the files under it and the runtime adds nothing.
    """
    for path in presented:
        trimmed = path.rstrip("/")
        if trimmed and (produced == trimmed or produced.startswith(f"{trimmed}/")):
            return True
    return False


def _final_assistant_message(turn_messages: list[Any]) -> AIMessage | None:
    for message in reversed(turn_messages):
        if isinstance(message, AIMessage) and getattr(message, "id", None):
            return message
    return None


class RuntimeDeliveryMiddleware(AgentMiddleware[AgentState]):
    """Hand over the files a turn produced and the model did not present."""

    def __init__(self, *, max_tracked_runs: int = 1000) -> None:
        super().__init__()
        self._lock = threading.Lock()
        self._snapshots: BoundedDict[tuple[str, str], Any] = BoundedDict(max_tracked_runs)

    def release_policy_parameters(self) -> dict[str, object]:
        return {"scope": "thread outputs root", "source": "filesystem diff", "curation": "model selection wins"}

    async def _snapshot(self, runtime: Runtime | None) -> Any:
        thread_id = _thread_id(runtime)
        if not thread_id:
            return None
        if sandbox_command_scope(getattr(runtime, "context", None)) is not None:
            # A delegated task reports its paths to the agent that delegated;
            # it does not present. The typed tool refuses on exactly this
            # condition (``tools/presentation.py``), and a runtime handover
            # that ignored it would be a second, quieter way around the same
            # rule.
            return None
        return await capture_workspace_snapshot(thread_id, user_id=get_effective_user_id(), include_text=False)

    @override
    async def abefore_agent(self, state: AgentState, runtime: Runtime) -> dict | None:
        try:
            snapshot = await self._snapshot(runtime)
        except Exception:
            # Best effort by contract: without a snapshot this middleware
            # presents nothing and the fence behaves exactly as before.
            logger.warning("Could not snapshot outputs before the agent; runtime delivery is off for this run", exc_info=True)
            snapshot = None
        if snapshot is not None:
            with self._lock:
                self._snapshots[_run_key(runtime)] = snapshot
        return None

    @override
    async def aafter_agent(self, state: AgentState, runtime: Runtime) -> dict | None:
        with self._lock:
            before = self._snapshots.pop(_run_key(runtime), None)
        if before is None:
            return None
        try:
            after = await self._snapshot(runtime)
            if after is None:
                return None
            produced = get_changed_output_paths(before, after)
        except Exception:
            logger.warning("Could not scan outputs after the agent; runtime delivery is off for this run", exc_info=True)
            return None
        if not produced:
            return None

        turn_messages = _messages_of_current_turn(state)
        presented = _already_presented(turn_messages)
        undelivered = [path for path in produced if not _covered(presented, path)][:MAX_RUNTIME_PRESENTED]
        if not undelivered:
            return None

        # The worker merges this into the delivery receipt; without it the
        # fence sees nothing presented and fails a run whose files were just
        # handed over.
        context = getattr(runtime, "context", None)
        if isinstance(context, dict):
            # Accumulated, not assigned: a goal continuation runs the agent
            # again inside the same run, and the receipt covers the run.
            existing = context.get(RUNTIME_PRESENTED_FILES_CONTEXT_KEY)
            carried = list(existing) if isinstance(existing, list) else []
            context[RUNTIME_PRESENTED_FILES_CONTEXT_KEY] = list(dict.fromkeys([*carried, *undelivered]))
        logger.info("Runtime presented %d file(s) the turn produced and did not hand over", len(undelivered))

        update: dict[str, Any] = {"artifacts": list(undelivered)}
        final = _final_assistant_message(turn_messages)
        if final is not None:
            # Re-emitted by id, so ``add_messages`` replaces it in place: the
            # chips the browser draws come only from a tagged message or a
            # ``present_files`` call, so an ``artifacts`` update alone would
            # put the file in the side panel and nowhere near the answer.
            tagged = final.model_copy(
                update={
                    "additional_kwargs": {
                        **(final.additional_kwargs or {}),
                        PRESENTED_FILES_KEY: list(undelivered),
                        PRESENTED_BY_KEY: "runtime",
                    }
                }
            )
            update["messages"] = [tagged]
        return update
