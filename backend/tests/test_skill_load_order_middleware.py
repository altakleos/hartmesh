"""SkillLoadOrderMiddleware: a skill's work runs only once its instructions can govern it.

HartMesh-owned successor of the ordering half of ``SkillToolPolicyMiddleware``.
Upstream (#6935) made ``allowed-tools`` compatibility metadata and deleted that
middleware, so the tool-set filtering tests went with it; tool authority now
belongs to agent assembly and execution authorization. What remains here is the
order a skill's own work takes: a call chosen beside a skill's first
``SKILL.md`` read is held back, and the lead (not a subagent) runs a skill's
declared first command before anything else in the sandbox.

``tests/test_skill_first_command_policy.py`` covers the first-command order in
depth and imports the helpers below.
"""

import asyncio
import dataclasses
from pathlib import Path
from types import SimpleNamespace

import pytest
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langgraph.prebuilt.tool_node import ToolCallRequest

from deerflow.runtime.secret_context import write_slash_skill_source_path
from deerflow.sandbox.tool_metadata import tag_sandbox_tool
from deerflow.skills.first_command import FirstCommand
from deerflow.skills.types import Skill, SkillCategory

_SLASH_SOURCE_OWNER_TOKEN = "test-slash-source-owner"


class StorageStub:
    def __init__(self, skills):
        self._skills = skills
        self.load_calls = 0

    def load_skills(self, *, enabled_only=False):
        self.load_calls += 1
        return [skill for skill in self._skills if skill.enabled or not enabled_only]

    def get_container_root(self):
        return "/mnt/skills"


def _skill(name: str, allowed_tools, *, enabled=True):
    skill_dir = Path(f"/tmp/skills/public/{name}")
    return Skill(
        name=name,
        description=f"Description for {name}",
        license="MIT",
        skill_dir=skill_dir,
        skill_file=skill_dir / "SKILL.md",
        relative_path=Path(name),
        category=SkillCategory.PUBLIC,
        allowed_tools=None if allowed_tools is None else tuple(allowed_tools),
        enabled=enabled,
    )


def _middleware(skills, *, available_skills=None, first_command_order=True):
    from deerflow.agents.middlewares.skill_load_order_middleware import SkillLoadOrderMiddleware

    middleware = SkillLoadOrderMiddleware(
        available_skills=available_skills,
        slash_source_owner_token=_SLASH_SOURCE_OWNER_TOKEN,
        first_command_order=first_command_order,
    )
    middleware._storage = lambda: StorageStub(skills)
    return middleware


# ── Construction: the slash-source token is shared with skill activation ────


@pytest.mark.parametrize(
    "middleware_class_path",
    [
        "deerflow.agents.middlewares.skill_activation_middleware.SkillActivationMiddleware",
        "deerflow.agents.middlewares.skill_load_order_middleware.SkillLoadOrderMiddleware",
    ],
)
def test_skill_middlewares_require_shared_slash_source_token(middleware_class_path):
    module_name, class_name = middleware_class_path.rsplit(".", 1)
    module = __import__(module_name, fromlist=[class_name])
    middleware_class = getattr(module, class_name)

    with pytest.raises(TypeError, match="slash_source_owner_token"):
        middleware_class()


@pytest.mark.parametrize("invalid_token", [None, "", 7])
def test_skill_middlewares_reject_invalid_slash_source_tokens(invalid_token):
    from deerflow.agents.middlewares.skill_activation_middleware import SkillActivationMiddleware
    from deerflow.agents.middlewares.skill_load_order_middleware import SkillLoadOrderMiddleware

    for middleware_class in (SkillActivationMiddleware, SkillLoadOrderMiddleware):
        with pytest.raises(ValueError, match="non-empty string"):
            middleware_class(slash_source_owner_token=invalid_token)


# ── Calls that load no skill pay nothing ─────────────────────────────────────


class ToolRequestStub:
    def __init__(self, name: str, *, state=None, context=None):
        self.tool_call = {"name": name, "id": "call-1", "args": {}}
        self.state = state or {}
        self.runtime = SimpleNamespace(context={} if context is None else context)


def test_sync_passive_tool_call_skips_storage():
    middleware = _middleware([])

    def fail_storage():
        raise AssertionError("a call beside no skill load must not load skill storage")

    middleware._storage = fail_storage

    assert middleware.wrap_tool_call(ToolRequestStub("task"), lambda _: "executed") == "executed"


def test_async_passive_tool_call_skips_storage_and_thread_offload(monkeypatch):
    middleware = _middleware([])

    def fail_storage():
        raise AssertionError("a call beside no skill load must not load skill storage")

    async def fail_offload(*_args, **_kwargs):
        raise AssertionError("a call beside no skill load must not leave the event loop")

    middleware._storage = fail_storage
    monkeypatch.setattr("deerflow.agents.middlewares.skill_load_order_middleware.asyncio.to_thread", fail_offload)

    async def handler(_):
        return "executed"

    assert asyncio.run(middleware.awrap_tool_call(ToolRequestStub("task"), handler)) == "executed"


# ── Calls chosen alongside a skill's first load ──────────────────────────────
#
# A released-profile report chat read business-report's SKILL.md and, in the
# same assistant message, inspected the upload with openpyxl (and in another
# sample ran ``ls -la`` on it). Those calls were selected before the skill's
# instructions could reach the model, so no text inside SKILL.md could govern
# them. The order therefore starts at the message that loads the skill.

_REPORT_SKILL_MD = "/mnt/skills/.accepted/snap-1/public/business-report/SKILL.md"


def _read_call(path: str, call_id: str, *, name: str = "read_file") -> dict:
    return {"name": name, "args": {"description": "read", "path": path}, "id": call_id, "type": "tool_call"}


def _bash_call(command: str, call_id: str) -> dict:
    return {"name": "bash", "args": {"description": "inspect", "command": command}, "id": call_id, "type": "tool_call"}


def _completed_read(path: str, call_id: str, *, status: str = "success"):
    from deerflow.agents.middlewares.skill_context import SKILL_CONTEXT_ENTRY_KEY

    return [
        AIMessage(content="", tool_calls=[_read_call(path, call_id)]),
        ToolMessage(
            content="---\nname: business-report\ndescription: Report.\n---\n# Business Report Skill" if status == "success" else "Error: no such file",
            tool_call_id=call_id,
            name="read_file",
            status=status,
            additional_kwargs={SKILL_CONTEXT_ENTRY_KEY: {"path": path, "description": "Report."}} if status == "success" else {},
        ),
    ]


def _batch_request(calls: list[dict], call_id: str, *, earlier=(), context=None, tool=None) -> ToolCallRequest:
    messages = [HumanMessage(content="Make the August 2026 business review as PDF, Word and Excel."), *earlier, AIMessage(content="", tool_calls=calls)]
    state = {"messages": messages}
    call = next(call for call in calls if call["id"] == call_id)
    return ToolCallRequest(tool_call=call, tool=tool, state=state, runtime=SimpleNamespace(context={} if context is None else context))


_OBSERVED_FIRST_BATCHES = {
    "workbook": [_read_call(_REPORT_SKILL_MD, "read-1"), _bash_call("python -c \"import openpyxl; wb = openpyxl.load_workbook('/mnt/user-data/uploads/input.xlsx', read_only=True); print(wb.sheetnames)\"", "inspect-1")],
    "file-existence": [_read_call(_REPORT_SKILL_MD, "read-1"), _bash_call("ls -la /mnt/user-data/uploads/input.xlsx", "inspect-1")],
    "script-directory": [_read_call(_REPORT_SKILL_MD, "read-1"), _bash_call("ls /mnt/skills/.accepted/snap-1/public/business-report/scripts", "inspect-1")],
    # The same inspections through the file tools: a read of the upload is not
    # skill material, and only reads count as loading it.
    "workbook-read": [_read_call(_REPORT_SKILL_MD, "read-1"), {**_read_call("/mnt/user-data/uploads/input.xlsx", "inspect-1"), "name": "read_file"}],
    "script-directory-glob": [
        _read_call(_REPORT_SKILL_MD, "read-1"),
        {"name": "glob", "args": {"description": "list", "pattern": "*", "path": "/mnt/skills/.accepted/snap-1/public/business-report/scripts"}, "id": "inspect-1", "type": "tool_call"},
    ],
}


@pytest.mark.parametrize("shape", sorted(_OBSERVED_FIRST_BATCHES))
def test_a_call_chosen_alongside_a_skill_load_is_not_run(shape):
    middleware = _middleware([])
    executed: list[str] = []

    result = middleware.wrap_tool_call(_batch_request(_OBSERVED_FIRST_BATCHES[shape], "inspect-1"), lambda request: executed.append(request.tool_call["id"]) or "executed")

    assert executed == []
    assert result.status == "error"
    assert result.name == next(call["name"] for call in _OBSERVED_FIRST_BATCHES[shape] if call["id"] == "inspect-1")
    assert result.tool_call_id == "inspect-1"
    assert result.content.startswith("Not run:")
    assert "business-report" in result.content
    assert _REPORT_SKILL_MD in result.content


def test_the_skill_load_itself_runs():
    middleware = _middleware([])

    assert middleware.wrap_tool_call(_batch_request(_OBSERVED_FIRST_BATCHES["workbook"], "read-1"), lambda _: "executed") == "executed"


def test_async_a_call_chosen_alongside_a_skill_load_is_not_run():
    middleware = _middleware([])
    executed: list[str] = []

    async def handler(request):
        executed.append(request.tool_call["id"])
        return "executed"

    result = asyncio.run(middleware.awrap_tool_call(_batch_request(_OBSERVED_FIRST_BATCHES["workbook"], "inspect-1"), handler))

    assert executed == []
    assert result.status == "error"
    assert result.content.startswith("Not run:")


def test_skill_material_loaded_together_all_runs():
    middleware = _middleware([])
    calls = [
        _read_call(_REPORT_SKILL_MD, "read-1"),
        _read_call("/mnt/skills/public/frontend-design/SKILL.md", "read-2"),
        _read_call("/mnt/skills/.accepted/snap-1/public/business-report/profiles/services-generic.json", "read-3"),
    ]

    for call in calls:
        assert middleware.wrap_tool_call(_batch_request(calls, call["id"]), lambda _: "executed") == "executed"


def test_a_skill_already_read_earlier_does_not_hold_back_its_siblings():
    middleware = _middleware([])
    request = _batch_request(
        [_read_call(_REPORT_SKILL_MD, "read-2"), _bash_call("ls -la /mnt/user-data/uploads/input.xlsx", "inspect-1")],
        "inspect-1",
        earlier=_completed_read(_REPORT_SKILL_MD, "read-1"),
    )

    assert middleware.wrap_tool_call(request, lambda _: "executed") == "executed"


def test_a_failed_earlier_read_leaves_the_instructions_unread():
    middleware = _middleware([])
    request = _batch_request(
        [_read_call(_REPORT_SKILL_MD, "read-2"), _bash_call("ls -la /mnt/user-data/uploads/input.xlsx", "inspect-1")],
        "inspect-1",
        earlier=_completed_read(_REPORT_SKILL_MD, "read-1", status="error"),
    )

    assert middleware.wrap_tool_call(request, lambda _: "executed").content.startswith("Not run:")


def test_a_slash_activated_skill_is_already_in_hand():
    report = _skill("business-report", None)
    middleware = _middleware([report])
    context: dict = {}
    write_slash_skill_source_path(context, report.get_container_file_path(), owner_token=_SLASH_SOURCE_OWNER_TOKEN)
    calls = [_read_call(report.get_container_file_path(), "read-1"), _bash_call("ls -la /mnt/user-data/uploads/input.xlsx", "inspect-1")]
    request = _batch_request(calls, "inspect-1", context=context)

    assert middleware.wrap_tool_call(request, lambda _: "executed") == "executed"


@pytest.mark.parametrize(
    "calls",
    [
        pytest.param([_bash_call("ls -la /mnt/user-data/uploads/input.xlsx", "inspect-1")], id="no-read"),
        pytest.param([_read_call("/mnt/user-data/uploads/SKILL.md", "read-1"), _bash_call("ls", "inspect-1")], id="skill-named-upload"),
        pytest.param([_read_call("/mnt/user-data/uploads/notes.md", "read-1"), _bash_call("ls", "inspect-1")], id="ordinary-read"),
        pytest.param([_read_call(_REPORT_SKILL_MD, "read-1", name="write_file"), _bash_call("ls", "inspect-1")], id="not-a-read-tool"),
        pytest.param([_read_call("/mnt/skills/.accepted/snap-1/public/business-report/profiles/services-generic.json", "read-1"), _bash_call("ls", "inspect-1")], id="skill-resource-only"),
    ],
)
def test_a_batch_that_loads_no_skill_runs_untouched(calls):
    middleware = _middleware([])

    assert middleware.wrap_tool_call(_batch_request(calls, "inspect-1"), lambda _: "executed") == "executed"


def test_the_configured_skills_root_and_read_tools_decide_what_a_load_is():
    from deerflow.agents.middlewares.skill_load_order_middleware import SkillLoadOrderMiddleware
    from deerflow.config.app_config import AppConfig
    from deerflow.config.sandbox_config import SandboxConfig
    from deerflow.config.skills_config import SkillsConfig
    from deerflow.config.summarization_config import SummarizationConfig

    app_config = AppConfig(
        sandbox=SandboxConfig(use="test"),
        skills=SkillsConfig(container_path="/opt/skills"),
        summarization=SummarizationConfig(skill_file_read_tool_names=["view"]),
    )
    middleware = SkillLoadOrderMiddleware(app_config=app_config, slash_source_owner_token=_SLASH_SOURCE_OWNER_TOKEN)
    middleware._storage = lambda: StorageStub([])

    configured = [_read_call("/opt/skills/public/business-report/SKILL.md", "read-1", name="view"), _bash_call("ls", "inspect-1")]
    default_names = [_read_call("/opt/skills/public/business-report/SKILL.md", "read-1"), _bash_call("ls", "inspect-1")]
    default_root = [_read_call(_REPORT_SKILL_MD, "read-1", name="view"), _bash_call("ls", "inspect-1")]

    assert middleware.wrap_tool_call(_batch_request(configured, "inspect-1"), lambda _: "executed").content.startswith("Not run:")
    assert middleware.wrap_tool_call(_batch_request(default_names, "inspect-1"), lambda _: "executed") == "executed"
    assert middleware.wrap_tool_call(_batch_request(default_root, "inspect-1"), lambda _: "executed") == "executed"


def test_a_held_back_task_carries_a_final_subagent_status():
    """The web UI settles a subtask card from the structured status; without one it spins."""
    from deerflow.subagents.status_contract import SUBAGENT_STATUS_KEY

    middleware = _middleware([])
    calls = [_read_call(_REPORT_SKILL_MD, "read-1"), {"name": "task", "args": {"description": "analyse", "prompt": "Analyse the workbook"}, "id": "task-1", "type": "tool_call"}]

    result = middleware.wrap_tool_call(_batch_request(calls, "task-1"), lambda _: "executed")

    assert result.content.startswith("Not run:")
    assert result.additional_kwargs[SUBAGENT_STATUS_KEY] == "failed"


def test_the_refusal_category_is_declared_not_read_from_the_skill_name():
    """A skill named like an auth failure must not make the refusal read as one."""
    from deerflow.agents.middlewares.tool_result_meta import TOOL_META_KEY, normalize_tool_result

    middleware = _middleware([])
    calls = [_read_call("/mnt/skills/custom/authentication-review/SKILL.md", "read-1"), _bash_call("ls", "inspect-1")]

    result = normalize_tool_result(middleware.wrap_tool_call(_batch_request(calls, "inspect-1"), lambda _: "executed"))

    meta = result.additional_kwargs[TOOL_META_KEY]
    assert meta["error_type"] == "not_run"
    assert meta["recoverable_by_model"] is True
    assert meta["recommended_next_action"] == "continue"


def test_a_skill_read_before_summarization_is_still_in_hand():
    """Compaction drops the earlier read but keeps its skill_context reference, which asks for a re-read."""
    report = _skill("business-report", None)
    middleware = _middleware([report])
    request = _batch_request(
        [_read_call(report.get_container_file_path(), "read-2"), _bash_call("SKILL_DIR=x; python build", "build-1")],
        "build-1",
    )
    request.state["skill_context"] = [{"name": "business-report", "path": report.get_container_file_path(), "description": "Report.", "loaded_at": 3}]

    assert middleware.wrap_tool_call(request, lambda _: "executed") == "executed"


@pytest.mark.parametrize("tool_name", ["describe_skill", "tool_search"])
def test_discovery_beside_a_skill_load_runs(tool_name):
    """Discovery returns metadata and runs nothing, so it cannot act on the skill's subject."""
    middleware = _middleware([])
    calls = [_read_call(_REPORT_SKILL_MD, "read-1"), {"name": tool_name, "args": {"name": "select:business-report"}, "id": "discover-1", "type": "tool_call"}]

    assert middleware.wrap_tool_call(_batch_request(calls, "discover-1"), lambda _: "executed") == "executed"


@pytest.mark.parametrize("allowed_tools", [None, (), ("read_file",)])
def test_allowed_tools_never_filter_or_refuse_a_call(allowed_tools):
    """``allowed-tools`` is compatibility metadata (#6935): it decides nothing here."""
    report = _skill("business-report", allowed_tools)
    middleware = _middleware([report])
    request = _batch_request(
        [_bash_call("ls -la /mnt/user-data/uploads/input.xlsx", "inspect-1")],
        "inspect-1",
        earlier=_completed_read(report.get_container_file_path(), "read-1"),
    )

    assert middleware.wrap_tool_call(request, lambda _: "executed") == "executed"


# ── The first command: the lead's order, not a subagent's ────────────────────

_FIRST_COMMAND_SKILL_MD = "/mnt/skills/public/business-report/SKILL.md"


class _SandboxTool:
    def __init__(self, name: str):
        self.name = name
        self.metadata = None
        tag_sandbox_tool(self)


def _first_command_skill():
    return dataclasses.replace(_skill("business-report", None), first_command=FirstCommand("scripts/report.py", ("build",)))


@pytest.mark.parametrize("first_command_order", [True, False], ids=["lead", "subagent"])
def test_only_the_lead_holds_the_sandbox_to_a_skills_first_command(first_command_order):
    """A subagent sees only its own messages, so a build the lead already delivered would be run again."""
    middleware = _middleware([_first_command_skill()], first_command_order=first_command_order)
    probe = _bash_call("ls -la /mnt/user-data/uploads/input.xlsx", "inspect-2")
    request = _batch_request([probe], "inspect-2", earlier=_completed_read(_FIRST_COMMAND_SKILL_MD, "read-1"), tool=_SandboxTool("bash"))

    result = middleware.wrap_tool_call(request, lambda _request: "executed")

    if first_command_order:
        assert result.content.startswith("Not run:")
        assert "`scripts/report.py build`" in result.content
    else:
        assert result == "executed"


def test_a_subagent_still_holds_back_a_call_chosen_beside_the_skill_load():
    """``first_command_order=False`` drops only the first-command order, not the load order."""
    middleware = _middleware([_first_command_skill()], first_command_order=False)
    calls = [_read_call(_FIRST_COMMAND_SKILL_MD, "read-1"), _bash_call("ls -la /mnt/user-data/uploads/input.xlsx", "inspect-1")]

    result = middleware.wrap_tool_call(_batch_request(calls, "inspect-1", tool=_SandboxTool("bash")), lambda _request: "executed")

    assert result.content.startswith("Not run: this call was chosen in the same message")
    assert "SKILL_DIR" not in result.content
