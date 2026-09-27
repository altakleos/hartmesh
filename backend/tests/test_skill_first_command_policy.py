"""SkillToolPolicyMiddleware: the order a report's first sandbox work takes.

Released-profile report chats (v2.1.0+hartmesh.34, four fresh chats) read
business-report's ``SKILL.md`` and, in three of them, chose an upload
inspection in the same message; two then ran two more inspections after the
read returned, before building. A skill that declares a ``first-command`` gets
that command as the first thing the sandbox runs in the turn that loads it,
and every refusal on the way says what the command is.
"""

import asyncio
import dataclasses
from types import SimpleNamespace

import pytest
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langgraph.prebuilt.tool_node import ToolCallRequest
from test_skill_tool_policy_middleware import (
    _SLASH_SOURCE_OWNER_TOKEN,
    _bash_call,
    _completed_read,
    _middleware,
    _read_call,
    _skill,
)

from deerflow.agents.middlewares.tool_result_meta import TOOL_META_KEY
from deerflow.runtime.secret_context import write_slash_skill_source_path
from deerflow.sandbox.tool_metadata import is_sandbox_tool, tag_sandbox_tool
from deerflow.skills.first_command import FirstCommand

_PROMPT = "Use the business-report skill to create the August 2026 business review from the uploaded workbook as PDF, Word and Excel."
_PROBE = "cd /mnt/user-data && python3 -c \"\nimport openpyxl\nwb = openpyxl.load_workbook('/mnt/user-data/uploads/input.xlsx', data_only=True)\nprint(wb.sheetnames)\n\"\n"


# ── The first command: nothing else in the sandbox before it ─────────────────

_DIRECTORY = "/mnt/skills/public/business-report"
_SKILL_MD = f"{_DIRECTORY}/SKILL.md"
_BUILD = (
    f'SKILL_DIR="{_DIRECTORY}"; python "${{SKILL_DIR:?assign SKILL_DIR first, as its own statement}}/scripts/report.py" build /mnt/user-data/uploads/input.xlsx'
    " --period 2026-08 --out /mnt/user-data/outputs/reports/2026-08-business-review --render pdf,docx,xlsx"
)


def _report_skill(first_command="scripts/report.py build", **changes):
    declared = None if first_command is None else FirstCommand(*first_command.split()[:1], tuple(first_command.split()[1:]))
    return dataclasses.replace(_skill("business-report", None), first_command=declared, **changes)


class _Tool:
    def __init__(self, name: str, *, sandbox: bool):
        self.name = name
        self.metadata = None
        if sandbox:
            tag_sandbox_tool(self)


def _ran(call: dict, content: str = "done", **kwargs) -> list:
    return [AIMessage(content="", tool_calls=[call]), ToolMessage(content=content, tool_call_id=call["id"], name=call["name"], **kwargs)]


def _loaded() -> list:
    return _completed_read(_SKILL_MD, "read-1")


def _request(call: dict, *, earlier=(), beside=(), sandbox=True, state_extra=None, context=None) -> ToolCallRequest:
    messages = [HumanMessage(content=_PROMPT), *earlier, AIMessage(content="", tool_calls=[*beside, call])]
    state = {"messages": messages, **(state_extra or {})}
    return ToolCallRequest(tool_call=call, tool=_Tool(call["name"], sandbox=sandbox), state=state, runtime=SimpleNamespace(context={} if context is None else context))


def _runs(middleware, request) -> bool:
    return middleware.wrap_tool_call(request, lambda _request: "executed") == "executed"


@pytest.mark.parametrize(
    "call",
    [
        pytest.param(_bash_call(_PROBE, "inspect-2"), id="workbook-structure"),
        pytest.param(_bash_call("ls -la /mnt/user-data/uploads/input.xlsx", "inspect-2"), id="upload"),
        pytest.param(_bash_call(f"ls {_DIRECTORY}/scripts", "inspect-2"), id="script-directory"),
        pytest.param(_bash_call("ls -R /mnt/user-data/outputs", "inspect-2"), id="outputs"),
        pytest.param(_read_call("/mnt/user-data/uploads/input.xlsx", "inspect-2"), id="read-the-upload"),
        pytest.param(_read_call(f"{_DIRECTORY}/scripts/report.py", "inspect-2"), id="read-the-script"),
        pytest.param(_bash_call(f"python {_DIRECTORY}/scripts/report.py inspect /mnt/user-data/uploads/input.xlsx", "inspect-2"), id="the-skill-inspect"),
    ],
)
def test_after_the_load_the_sandbox_runs_nothing_before_the_first_command(call):
    middleware = _middleware([_report_skill()])
    executed: list[str] = []

    result = middleware.wrap_tool_call(_request(call, earlier=_loaded()), lambda request: executed.append(request.tool_call["id"]) or "executed")

    assert executed == []
    assert result.status == "error"
    assert result.tool_call_id == "inspect-2"
    assert result.content.startswith("Not run:")
    assert "`scripts/report.py build`" in result.content
    # The form the model can copy, with the skill's own directory filled in.
    assert f'`SKILL_DIR="{_DIRECTORY}"; python "${{SKILL_DIR:?}}/scripts/report.py" build`' in result.content
    assert "ask the person or answer instead" in result.content
    assert result.additional_kwargs[TOOL_META_KEY]["error_type"] == "not_run"


def test_async_after_the_load_the_sandbox_runs_nothing_before_the_first_command():
    middleware = _middleware([_report_skill()])

    async def handler(_request):
        return "executed"

    result = asyncio.run(middleware.awrap_tool_call(_request(_bash_call(_PROBE, "inspect-2"), earlier=_loaded()), handler))

    assert result.content.startswith("Not run:")


def test_the_first_command_runs():
    assert _runs(_middleware([_report_skill()]), _request(_bash_call(_BUILD, "build-1"), earlier=_loaded()))


def test_a_model_that_inspects_again_after_a_refusal_is_refused_again():
    middleware = _middleware([_report_skill()])
    refused_once = [*_loaded(), *_ran(_bash_call(_PROBE, "inspect-2"), "Not run: …", status="error", additional_kwargs={TOOL_META_KEY: {"error_type": "not_run"}})]

    assert not _runs(middleware, _request(_bash_call(_PROBE, "inspect-3"), earlier=refused_once))
    assert _runs(middleware, _request(_bash_call(_BUILD, "build-1"), earlier=refused_once))


@pytest.mark.parametrize(
    ("content", "status"),
    [
        pytest.param("Built draft 1: August 2026 review", "success", id="built"),
        pytest.param("No rows in 2026-09. The files cover 2025-08-01 to 2026-08-31.", "error", id="no-rows-in-period"),
        pytest.param("Error: Unsafe absolute paths in command", "error", id="refused-by-the-sandbox"),
    ],
)
def test_once_the_first_command_has_run_everything_runs_again(content, status):
    """Recovery after a build that failed is the model's own to choose."""
    after = [*_loaded(), *_ran(_bash_call(_BUILD, "build-1"), content, status=status)]

    assert _runs(_middleware([_report_skill()]), _request(_bash_call(_PROBE, "inspect-2"), earlier=after))


def test_a_first_command_that_was_not_run_has_not_run():
    after = [*_loaded(), *_ran(_bash_call(_BUILD, "build-1"), "Not run: …", status="error", additional_kwargs={TOOL_META_KEY: {"error_type": "not_run"}})]

    assert not _runs(_middleware([_report_skill()]), _request(_bash_call(_PROBE, "inspect-2"), earlier=after))


def test_a_call_chosen_beside_the_first_command_waits_for_its_output():
    middleware = _middleware([_report_skill()])
    build = _bash_call(_BUILD, "build-1")

    assert not _runs(middleware, _request(_bash_call(_PROBE, "inspect-2"), earlier=_loaded(), beside=[build]))
    assert _runs(middleware, _request(build, earlier=_loaded(), beside=[_bash_call(_PROBE, "inspect-2")]))


@pytest.mark.parametrize(
    "call",
    [
        pytest.param({"name": "ask_clarification", "args": {"question": "Which month?"}, "id": "ask-1", "type": "tool_call"}, id="ask-the-user"),
        pytest.param({"name": "web_search", "args": {"query": "x"}, "id": "search-1", "type": "tool_call"}, id="web"),
        pytest.param({"name": "write_todos", "args": {"todos": []}, "id": "todo-1", "type": "tool_call"}, id="plan"),
    ],
)
def test_work_outside_the_sandbox_is_not_ordered(call):
    assert _runs(_middleware([_report_skill()]), _request(call, earlier=_loaded(), sandbox=False))


def test_a_call_with_no_known_tool_is_not_ordered():
    request = _request(_bash_call(_PROBE, "inspect-2"), earlier=_loaded())
    request = ToolCallRequest(tool_call=request.tool_call, tool=None, state=request.state, runtime=request.runtime)

    assert _runs(_middleware([_report_skill()]), request)


def test_a_skill_without_a_first_command_orders_nothing_after_its_load():
    assert _runs(_middleware([_report_skill(None)]), _request(_bash_call(_PROBE, "inspect-2"), earlier=_loaded()))


@pytest.mark.parametrize(
    "between",
    [
        pytest.param([AIMessage(content="Which export should I use? Please upload it.")], id="the-model-answered"),
        pytest.param([HumanMessage(content="Actually, first tidy the notes file.")], id="the-user-spoke-again"),
    ],
)
def test_the_order_holds_for_the_turn_that_loads_the_skill(between):
    assert _runs(_middleware([_report_skill()]), _request(_bash_call(_PROBE, "inspect-2"), earlier=[*_loaded(), *between]))


def test_a_skill_loaded_and_built_in_an_earlier_turn_orders_nothing():
    earlier = [*_loaded(), *_ran(_bash_call(_BUILD, "build-1")), AIMessage(content="Done."), HumanMessage(content="Drop the warranty jobs."), *_completed_read(_SKILL_MD, "read-2")]

    assert _runs(_middleware([_report_skill()]), _request(_bash_call(_PROBE, "inspect-2"), earlier=earlier))


def test_a_summarized_conversation_is_not_ordered():
    """Compaction hides earlier loads and runs, so a re-read is not known to be a first load."""
    request = _request(_bash_call(_PROBE, "inspect-2"), earlier=_loaded(), state_extra={"summary_text": "Earlier: an August report was built."})

    assert _runs(_middleware([_report_skill()]), request)


def test_a_slash_activated_skill_is_not_ordered():
    context: dict = {}
    write_slash_skill_source_path(context, _SKILL_MD, owner_token=_SLASH_SOURCE_OWNER_TOKEN)

    assert _runs(_middleware([_report_skill()]), _request(_bash_call(_PROBE, "inspect-2"), earlier=_loaded(), context=context))


@pytest.mark.parametrize(
    "skills",
    [
        pytest.param([_report_skill(enabled=False)], id="disabled"),
        pytest.param([], id="not-in-the-registry"),
    ],
)
def test_a_skill_that_does_not_resolve_orders_nothing(skills):
    assert _runs(_middleware(skills), _request(_bash_call(_PROBE, "inspect-2"), earlier=_loaded()))


def test_a_registry_that_cannot_load_orders_nothing():
    middleware = _middleware([_report_skill()])

    def fail():
        raise OSError("skills unreadable")

    middleware._storage = fail

    assert _runs(middleware, _request(_bash_call(_PROBE, "inspect-2"), earlier=_loaded()))


def test_a_call_beside_the_first_load_is_told_the_first_command():
    """The first-load refusal is where the model first hears of the order, so it names the command."""
    middleware = _middleware([_report_skill()])
    request = _request(_bash_call(_PROBE, "inspect-1"), beside=[_read_call(_SKILL_MD, "read-1")])

    result = middleware.wrap_tool_call(request, lambda _request: "executed")

    assert result.content.startswith("Not run: this call was chosen in the same message that loads the business-report skill's instructions")
    assert "`scripts/report.py build`" in result.content
    assert result.additional_kwargs[TOOL_META_KEY]["error_type"] == "not_run"


def test_async_a_call_beside_the_first_load_is_told_the_first_command():
    middleware = _middleware([_report_skill()])

    async def handler(_request):
        return "executed"

    result = asyncio.run(middleware.awrap_tool_call(_request(_bash_call(_PROBE, "inspect-1"), beside=[_read_call(_SKILL_MD, "read-1")]), handler))

    assert "`scripts/report.py build`" in result.content


def test_a_call_beside_the_first_load_of_a_skill_already_run_is_not_told_to_run_it_again():
    earlier = [*_ran(_bash_call(_BUILD, "build-0"), "Built draft 1"), AIMessage(content="Done."), HumanMessage(content="Add a note to the review.")]
    request = _request(_bash_call(_PROBE, "inspect-1"), earlier=earlier, beside=[_read_call(_SKILL_MD, "read-1")])

    result = _middleware([_report_skill()]).wrap_tool_call(request, lambda _request: "executed")

    assert result.content.startswith("Not run: this call was chosen in the same message")
    assert "SKILL_DIR" not in result.content


def test_a_call_beside_the_load_of_a_skill_without_a_first_command_is_refused_as_before():
    result = _middleware([_report_skill(None)]).wrap_tool_call(_request(_bash_call(_PROBE, "inspect-1"), beside=[_read_call(_SKILL_MD, "read-1")]), lambda _request: "executed")

    assert result.content.startswith("Not run: this call was chosen in the same message")
    assert "SKILL_DIR" not in result.content


@pytest.mark.parametrize(
    "between",
    [
        pytest.param({"name": "write_todos", "args": {"todos": []}, "id": "todo-1", "type": "tool_call"}, id="plan"),
        pytest.param({"name": "web_search", "args": {"query": "x"}, "id": "search-1", "type": "tool_call"}, id="web"),
        pytest.param({"name": "describe_skill", "args": {"name": "business-report"}, "id": "discover-1", "type": "tool_call"}, id="discovery"),
        pytest.param({"name": "list_uploaded_files", "args": {}, "id": "uploads-1", "type": "tool_call"}, id="uploads"),
    ],
)
def test_work_outside_the_sandbox_does_not_end_the_order(between):
    """Only a run of the first command, or an answer, ends it."""
    middleware = _middleware([_report_skill()])
    earlier = [*_loaded(), *_ran(between, "ok")]

    assert not _runs(middleware, _request(_bash_call(_PROBE, "inspect-2"), earlier=earlier))
    assert _runs(middleware, _request(_bash_call(_BUILD, "build-1"), earlier=earlier))


def test_a_message_the_runtime_adds_is_not_the_person_speaking_again():
    reminder = HumanMessage(content="Your todo list is empty.", name="todo_reminder", additional_kwargs={"hide_from_ui": True})

    assert not _runs(_middleware([_report_skill()]), _request(_bash_call(_PROBE, "inspect-2"), earlier=[*_loaded(), reminder]))


def test_the_order_holds_while_the_skill_is_in_skill_context():
    """The durable capture records the load in the same turn; that is not a load from before."""
    captured = {"skill_context": [{"name": "business-report", "path": _SKILL_MD, "description": "", "loaded_at": 2}, {"name": "data-analysis", "path": "/mnt/skills/public/data-analysis/SKILL.md", "description": "", "loaded_at": 0}]}
    middleware = _middleware([_report_skill(), _skill("data-analysis", None)])

    async def handler(_request):
        return "executed"

    result = asyncio.run(middleware.awrap_tool_call(_request(_bash_call(_PROBE, "inspect-2"), earlier=_loaded(), state_extra=captured), handler))

    assert result != "executed"
    assert result.content.startswith("Not run:")


def test_another_skills_instructions_can_be_read_before_the_first_command():
    middleware = _middleware([_report_skill(), _skill("data-analysis", None)])

    assert _runs(middleware, _request(_read_call("/mnt/skills/public/data-analysis/SKILL.md", "read-2"), earlier=_loaded()))
    assert not _runs(middleware, _request(_read_call("/mnt/skills/public/data-analysis/references/charts.md", "read-2"), earlier=_loaded()))


def test_two_skills_with_first_commands_do_not_hold_each_other():
    other_directory = "/mnt/skills/public/ledger-close"
    other = dataclasses.replace(_skill("ledger-close", None), first_command=FirstCommand("scripts/close.py", ("run",)))
    middleware = _middleware([_report_skill(), other])
    earlier = [*_loaded(), *_completed_read(f"{other_directory}/SKILL.md", "read-2")]

    assert _runs(middleware, _request(_bash_call(_BUILD, "build-1"), earlier=earlier))
    assert _runs(middleware, _request(_bash_call(f"python {other_directory}/scripts/close.py run", "close-1"), earlier=earlier))
    refused = middleware.wrap_tool_call(_request(_bash_call(_PROBE, "inspect-2"), earlier=earlier), lambda _request: "executed")
    assert "`scripts/report.py build`" in refused.content
    assert "`scripts/close.py run`" in refused.content
    # Once one has run, only the other still orders the sandbox.
    built = [*earlier, *_ran(_bash_call(_BUILD, "build-1"), "Built draft 1")]
    refused = middleware.wrap_tool_call(_request(_bash_call(_PROBE, "inspect-2"), earlier=built), lambda _request: "executed")
    assert "`scripts/report.py build`" not in refused.content
    assert "`scripts/close.py run`" in refused.content


def test_a_skill_run_from_a_slash_command_earlier_is_not_run_again_after_its_first_read():
    """A later turn's first read of the skill does not force another build of a report already built."""
    earlier = [*_ran(_bash_call(_BUILD, "build-0"), "Built draft 1"), AIMessage(content="Done."), HumanMessage(content="Add a note to the review."), *_loaded()]

    assert _runs(_middleware([_report_skill()]), _request(_bash_call(_PROBE, "inspect-2"), earlier=earlier))


def test_a_skill_this_agent_may_not_use_orders_nothing():
    assert _runs(_middleware([_report_skill()], available_skills={"data-analysis"}), _request(_bash_call(_PROBE, "inspect-2"), earlier=_loaded()))


def test_every_sandbox_tool_says_it_works_in_the_sandbox():
    """The order reads the tag, so an untagged sandbox tool would inspect the upload before the build."""
    from langchain_core.tools import BaseTool

    from deerflow.sandbox import tools

    defined = {tool.name: tool for tool in vars(tools).values() if isinstance(tool, BaseTool)}

    assert set(defined) == {"bash", "ls", "glob", "grep", "read_file", "write_file", "str_replace"}
    assert all(is_sandbox_tool(tool) for tool in defined.values())


def test_a_subagent_is_not_held_to_a_first_command():
    """It sees only its own messages, so a build the lead already delivered would be run again."""
    from test_skill_tool_policy_middleware import _SLASH_SOURCE_OWNER_TOKEN as token

    from deerflow.agents.middlewares.skill_tool_policy_middleware import SkillToolPolicyMiddleware

    middleware = SkillToolPolicyMiddleware(slash_source_owner_token=token, first_command_order=False)
    middleware._storage = _middleware([_report_skill()])._storage

    assert _runs(middleware, _request(_bash_call(_PROBE, "inspect-2"), earlier=_loaded()))
    beside = middleware.wrap_tool_call(_request(_bash_call(_PROBE, "inspect-1"), beside=[_read_call(_SKILL_MD, "read-1")]), lambda _request: "executed")
    assert beside.content.startswith("Not run: this call was chosen in the same message")
    assert "SKILL_DIR" not in beside.content


def test_a_summarized_conversation_is_not_told_of_an_order_it_does_not_hold():
    request = _request(_bash_call(_PROBE, "inspect-1"), beside=[_read_call(_SKILL_MD, "read-1")], state_extra={"summary_text": "Earlier: an August report was built."})

    result = _middleware([_report_skill()]).wrap_tool_call(request, lambda _request: "executed")

    assert result.content.startswith("Not run: this call was chosen in the same message")
    assert "SKILL_DIR" not in result.content


def test_a_result_with_malformed_metadata_is_read_as_a_run():
    after = [*_loaded(), *_ran(_bash_call(_BUILD, "build-1"), "Built draft 1", additional_kwargs={TOOL_META_KEY: "not-a-mapping"})]

    assert _runs(_middleware([_report_skill()]), _request(_bash_call(_PROBE, "inspect-2"), earlier=after))
