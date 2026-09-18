"""A turn that made files for the user hands them over.

The tenant-class shape: "Create pdf about muse agent" produced a valid
14,710-byte PDF through ``bash`` without the typed ``present`` argument, the
agent never called ``present_files``, and the delivery fence correctly ended
the run ``error``. These tests drive the middleware over a real outputs
directory -- files written between its two hooks, exactly as a turn writes
them -- and check what it hands over, what it leaves to the model, and the
one shape it does not cover.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from deerflow.agents.middlewares.runtime_delivery_middleware import (
    PRESENTED_BY_KEY,
    RUNTIME_PRESENTED_FILES_CONTEXT_KEY,
    RuntimeDeliveryMiddleware,
)
from deerflow.runtime.presented_files import PRESENTED_FILES_KEY
from deerflow.runtime.user_context import get_effective_user_id

THREAD_ID = "thread-df22"
OUTPUTS = "/mnt/user-data/outputs"


@pytest.fixture
def thread_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A real per-thread outputs directory the middleware's scanner will read."""
    monkeypatch.setenv("DEER_FLOW_HOME", str(tmp_path))
    from deerflow.config import paths as paths_module

    # The Paths singleton caches the home it read at first use; rebuild it so
    # this test's writes land under tmp_path and not the developer's tree.
    monkeypatch.setattr(paths_module, "_paths", paths_module.Paths())
    # The middleware resolves the user the way the worker does, so the
    # directory it scans is the current user's, not a fixed one.
    outputs = paths_module.get_paths().sandbox_outputs_dir(THREAD_ID, user_id=get_effective_user_id())
    outputs.mkdir(parents=True, exist_ok=True)
    return outputs


def _runtime(run_id: str = "run-1") -> SimpleNamespace:
    return SimpleNamespace(context={"thread_id": THREAD_ID, "run_id": run_id})


def _state(*messages: Any) -> dict[str, Any]:
    return {"messages": list(messages)}


def _answer(text: str = "Here is the report.", message_id: str = "ai-final") -> AIMessage:
    return AIMessage(text, id=message_id)


def _presenting_tool_message(*paths: str) -> ToolMessage:
    return ToolMessage("Presented to the user: 1 file", tool_call_id="c1", name="bash", additional_kwargs={PRESENTED_FILES_KEY: list(paths)})


def _turn(middleware: RuntimeDeliveryMiddleware, runtime: SimpleNamespace, write: Any, state: dict[str, Any]) -> dict[str, Any] | None:
    """One agent pass: the before hook, the turn's writes, then the after hook."""

    async def run() -> dict[str, Any] | None:
        await middleware.abefore_agent(_state(), runtime)
        write()
        return await middleware.aafter_agent(state, runtime)

    return asyncio.run(run())


def test_a_file_the_turn_made_and_nobody_presented_is_handed_over(thread_home: Path) -> None:
    middleware, runtime = RuntimeDeliveryMiddleware(), _runtime()
    state = _state(HumanMessage("Create pdf about muse agent"), _answer())

    update = _turn(middleware, runtime, lambda: (thread_home / "Muse_Agent_Report.pdf").write_bytes(b"%PDF-1.7 report"), state)

    assert update is not None, "the turn produced a file and presented nothing"
    assert update["artifacts"] == [f"{OUTPUTS}/Muse_Agent_Report.pdf"]
    # The chips under the answer come only from a tagged message; an
    # artifacts update alone would put the file in the side panel and nowhere
    # near what the person is reading.
    [tagged] = update["messages"]
    assert tagged.id == "ai-final", "re-emitted by id so the reducer replaces it in place"
    assert tagged.content == "Here is the report.", "the answer itself is untouched"
    assert tagged.additional_kwargs[PRESENTED_FILES_KEY] == [f"{OUTPUTS}/Muse_Agent_Report.pdf"]
    assert tagged.additional_kwargs[PRESENTED_BY_KEY] == "runtime"
    # And the worker, which owns the receipt and the fence, is told.
    assert runtime.context[RUNTIME_PRESENTED_FILES_CONTEXT_KEY] == [f"{OUTPUTS}/Muse_Agent_Report.pdf"]


def test_a_turn_that_made_nothing_hands_over_nothing(thread_home: Path) -> None:
    middleware, runtime = RuntimeDeliveryMiddleware(), _runtime()
    update = _turn(middleware, runtime, lambda: None, _state(HumanMessage("what is 2+2"), _answer("Four.")))
    assert update is None
    assert RUNTIME_PRESENTED_FILES_CONTEXT_KEY not in runtime.context


def test_what_the_model_presented_is_left_alone(thread_home: Path) -> None:
    middleware, runtime = RuntimeDeliveryMiddleware(), _runtime()
    path = f"{OUTPUTS}/report.pdf"
    state = _state(HumanMessage("make a report"), _presenting_tool_message(path), _answer())

    update = _turn(middleware, runtime, lambda: (thread_home / "report.pdf").write_bytes(b"%PDF"), state)

    assert update is None, "the model curated; the runtime adds nothing"
    assert RUNTIME_PRESENTED_FILES_CONTEXT_KEY not in runtime.context


def test_only_the_files_the_model_left_out_are_added(thread_home: Path) -> None:
    middleware, runtime = RuntimeDeliveryMiddleware(), _runtime()
    state = _state(HumanMessage("make a report"), _presenting_tool_message(f"{OUTPUTS}/report.pdf"), _answer())

    def write() -> None:
        (thread_home / "report.pdf").write_bytes(b"%PDF")
        (thread_home / "chart.png").write_bytes(b"\x89PNG")

    update = _turn(middleware, runtime, write, state)

    assert update is not None
    assert update["artifacts"] == [f"{OUTPUTS}/chart.png"], "model selection stands; only the omission is added"


def test_a_presented_directory_covers_the_files_under_it(thread_home: Path) -> None:
    middleware, runtime = RuntimeDeliveryMiddleware(), _runtime()
    state = _state(HumanMessage("make a site"), _presenting_tool_message(f"{OUTPUTS}/site"), _answer())

    def write() -> None:
        (thread_home / "site").mkdir()
        (thread_home / "site" / "index.html").write_text("<h1>hi</h1>", encoding="utf-8")

    assert _turn(middleware, runtime, write, state) is None, "the same rule the fence applies"


def test_a_file_presented_last_turn_and_rewritten_this_turn_is_handed_over_again(thread_home: Path) -> None:
    # The DF17 shape: a revision rewrites the same paths and the model says
    # "Done". Scoping the presented scan to this turn is what catches it.
    middleware, runtime = RuntimeDeliveryMiddleware(), _runtime()
    (thread_home / "report.pdf").write_bytes(b"%PDF first")
    state = _state(
        HumanMessage("make a report"),
        _presenting_tool_message(f"{OUTPUTS}/report.pdf"),
        _answer("Done.", "ai-1"),
        HumanMessage("now revise it"),
        _answer("Done.", "ai-2"),
    )

    update = _turn(middleware, runtime, lambda: (thread_home / "report.pdf").write_bytes(b"%PDF second and longer"), state)

    assert update is not None, "last turn's presentation does not deliver this turn's file"
    assert update["artifacts"] == [f"{OUTPUTS}/report.pdf"]
    assert update["messages"][0].id == "ai-2", "the tag lands on this turn's answer"


def test_nothing_under_workspace_is_ever_handed_over(thread_home: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from deerflow.config import paths as paths_module

    workspace = paths_module.get_paths().sandbox_work_dir(THREAD_ID, user_id=get_effective_user_id())
    workspace.mkdir(parents=True, exist_ok=True)
    middleware, runtime = RuntimeDeliveryMiddleware(), _runtime()

    update = _turn(middleware, runtime, lambda: (workspace / "scratch.py").write_text("x = 1", encoding="utf-8"), _state(HumanMessage("run something"), _answer()))

    assert update is None, "the working directory is not a deliverable"


def test_an_answerless_turn_still_reaches_the_panel(thread_home: Path) -> None:
    middleware, runtime = RuntimeDeliveryMiddleware(), _runtime()
    update = _turn(middleware, runtime, lambda: (thread_home / "out.txt").write_text("hi", encoding="utf-8"), _state(HumanMessage("make a file")))
    assert update is not None
    assert update["artifacts"] == [f"{OUTPUTS}/out.txt"]
    assert "messages" not in update, "no assistant message to tag; the panel still gets the file"


def test_continuations_within_one_run_accumulate_rather_than_replace(thread_home: Path) -> None:
    middleware, runtime = RuntimeDeliveryMiddleware(), _runtime()
    _turn(middleware, runtime, lambda: (thread_home / "first.txt").write_text("1", encoding="utf-8"), _state(HumanMessage("go"), _answer("a", "ai-a")))
    _turn(middleware, runtime, lambda: (thread_home / "second.txt").write_text("2", encoding="utf-8"), _state(HumanMessage("go"), _answer("b", "ai-b")))

    assert runtime.context[RUNTIME_PRESENTED_FILES_CONTEXT_KEY] == [f"{OUTPUTS}/first.txt", f"{OUTPUTS}/second.txt"], "the receipt covers the run, not the last continuation"


def test_a_run_whose_snapshot_failed_presents_nothing(thread_home: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Best effort by contract: without a snapshot the fence behaves exactly as
    # it did before this middleware existed.
    from deerflow.agents.middlewares import runtime_delivery_middleware as module

    async def boom(*_args: Any, **_kwargs: Any) -> Any:
        raise OSError("no")

    monkeypatch.setattr(module, "capture_workspace_snapshot", boom)
    middleware, runtime = RuntimeDeliveryMiddleware(), _runtime()
    update = _turn(middleware, runtime, lambda: (thread_home / "out.txt").write_text("hi", encoding="utf-8"), _state(HumanMessage("go"), _answer()))
    assert update is None


def _clarification(message_id: str = "ai-clarify") -> AIMessage:
    """What the model actually sends when it needs one more thing from the person.

    Not an interrupt: ``ask_clarification`` is a client-side ``return_direct``
    call, and with the siblings dropped ``create_agent`` routes to END
    (``clarification_middleware``). The run *completes*, which is why the
    ``after_agent`` hooks below run at all.
    """
    return AIMessage(
        "Which Muse agent do you mean?",
        id=message_id,
        tool_calls=[{"name": "ask_clarification", "args": {"question": "Which Muse agent do you mean?"}, "id": "clarify-1"}],
    )


def test_a_clarifying_question_still_hands_over_a_file_the_turn_already_made(thread_home: Path) -> None:
    """The shape the tenant class actually produces, and it is covered.

    The `.21` capture of "Create pdf about muse agent" is two runs: a 21.6 s
    turn offering three options, then a 152.8 s turn that writes the PDF. That
    first turn ends with ``TerminalResponseMiddleware.after_agent`` and an
    ``end`` event on the wire -- a completed run, not an interrupt -- so this
    middleware's own ``after_agent`` runs on it like any other.

    It matters when the model drafts something *before* asking. The fence
    demands delivery of whatever the turn produced, so without this the
    clarification turn would end ``artifact_delivery_incomplete`` for a draft
    the person can see no other way. The draft is handed over, and the
    clarification's tool call survives the re-tag so the options still render.
    """
    middleware, runtime = RuntimeDeliveryMiddleware(), _runtime()
    clarification = _clarification()
    update = _turn(
        middleware,
        runtime,
        lambda: (thread_home / "Muse_Agent_Report.pdf").write_bytes(b"%PDF-1.4 draft"),
        _state(HumanMessage("Create pdf about muse agent"), clarification),
    )

    assert update is not None, "a completed clarification turn is not exempt from the fence it has to satisfy"
    assert update["artifacts"] == [f"{OUTPUTS}/Muse_Agent_Report.pdf"]
    [tagged] = update["messages"]
    assert tagged.id == clarification.id, "re-emitted by id, so the question is replaced rather than duplicated"
    assert tagged.additional_kwargs[PRESENTED_FILES_KEY] == [f"{OUTPUTS}/Muse_Agent_Report.pdf"]
    assert tagged.additional_kwargs[PRESENTED_BY_KEY] == "runtime"
    assert [call["name"] for call in tagged.tool_calls] == ["ask_clarification"], "tagging must not cost the person the options"
    assert runtime.context[RUNTIME_PRESENTED_FILES_CONTEXT_KEY] == [f"{OUTPUTS}/Muse_Agent_Report.pdf"]


def test_a_clarifying_question_that_drafted_nothing_hands_over_nothing(thread_home: Path) -> None:
    # The ordinary case: the model asks before it writes, which is what the
    # `.21` capture shows. Nothing was produced, so nothing is delivered and
    # the question reaches the person untouched.
    middleware, runtime = RuntimeDeliveryMiddleware(), _runtime()
    update = _turn(middleware, runtime, lambda: None, _state(HumanMessage("Create pdf about muse agent"), _clarification()))
    assert update is None
    assert RUNTIME_PRESENTED_FILES_CONTEXT_KEY not in runtime.context


def test_a_graph_interrupt_would_not_be_covered_if_one_were_ever_introduced(thread_home: Path) -> None:
    """The gap this middleware has, stated at its real size.

    ``after_agent`` does not run when a graph interrupts, so a turn suspended
    mid-flight hands nothing over, and the resumed run is a new run whose
    snapshot is taken after the file already exists -- its own scan finds
    nothing produced and the fence passes.

    **No path in this product reaches that shape.** Clarification is a
    completed run (see above), and neither the harness nor the app calls
    ``interrupt()`` or configures ``interrupt_before``/``interrupt_after``.
    This test pins the consequence so that introducing an interrupt path
    anywhere in the agent graph is a deliberate act with a known cost, rather
    than a silent hole. If one is introduced, this test is the one to change.
    """
    middleware, runtime = RuntimeDeliveryMiddleware(), _runtime()

    async def interrupted() -> None:
        await middleware.abefore_agent(_state(), runtime)
        (thread_home / "draft.pdf").write_bytes(b"%PDF")
        # ... the graph interrupts here; aafter_agent is never awaited.

    asyncio.run(interrupted())
    assert RUNTIME_PRESENTED_FILES_CONTEXT_KEY not in runtime.context

    # The resumed run: a fresh snapshot, taken with the file already in place.
    resumed, resumed_runtime = RuntimeDeliveryMiddleware(), _runtime("run-2")
    update = _turn(resumed, resumed_runtime, lambda: None, _state(HumanMessage("the second one"), _answer()))
    assert update is None, "the resumed run produced nothing; its own fence passes for the same reason"


def test_a_delegated_task_presents_nothing(thread_home: Path) -> None:
    """The rule the typed tool already enforces, enforced here too.

    A subagent reports the paths it made to the agent that delegated, and that
    agent presents. This middleware is attached to the subagent stack as well
    as the lead's, so without the check it would be a second, quieter way
    around ``validate_presentation``'s refusal.
    """
    from deerflow.sandbox.lease import SANDBOX_COMMAND_SCOPE_CONTEXT_KEY

    middleware = RuntimeDeliveryMiddleware()
    runtime = SimpleNamespace(context={"thread_id": THREAD_ID, "run_id": "run-sub", SANDBOX_COMMAND_SCOPE_CONTEXT_KEY: "scope-1"})

    update = _turn(middleware, runtime, lambda: (thread_home / "draft.pdf").write_bytes(b"%PDF"), _state(HumanMessage("go"), _answer()))

    assert update is None
    assert RUNTIME_PRESENTED_FILES_CONTEXT_KEY not in runtime.context


def test_the_middleware_is_in_the_lead_runtime_stack() -> None:
    from deerflow.agents.middlewares.tool_error_handling_middleware import build_lead_runtime_middlewares
    from deerflow.config.app_config import AppConfig
    from deerflow.config.sandbox_config import SandboxConfig

    middlewares = build_lead_runtime_middlewares(app_config=AppConfig(models=[], sandbox=SandboxConfig(use="test")), lazy_init=True)
    assert any(isinstance(middleware, RuntimeDeliveryMiddleware) for middleware in middlewares)
