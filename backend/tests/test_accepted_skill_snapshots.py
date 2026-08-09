"""Accepted skill material remains immutable for the lifetime of a run."""

from __future__ import annotations

import importlib
import json
import os
import shutil
import stat
import sys
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from langchain.agents.middleware.types import ModelRequest
from langchain_core.messages import AIMessage, HumanMessage

from deerflow.agents.middlewares.skill_activation_middleware import (
    SkillActivationMiddleware,
    is_slash_skill_activation_reminder,
)
from deerflow.config.app_config import AppConfig
from deerflow.config.paths import Paths
from deerflow.config.sandbox_config import SandboxConfig
from deerflow.runtime.accepted_invocation import (
    AcceptedInvocation,
    InvocationOrigin,
    PrincipalProjection,
    canonical_digest,
)
from deerflow.runtime.agent_revision import (
    RESOLVED_AGENT_MATERIAL_CONTEXT_KEY,
    resolve_agent_revision,
)
from deerflow.runtime.runs.manager import RunManager
from deerflow.runtime.runs.schemas import RunStatus
from deerflow.runtime.runs.worker import RunContext, run_agent
from deerflow.runtime.skill_snapshot import (
    SkillSnapshotError,
    SkillSnapshotLimits,
    cleanup_abandoned_skill_snapshots,
    snapshot_effective_skills,
)
from deerflow.sandbox.local.local_sandbox import LocalSandbox, PathMapping
from deerflow.skills.parser import parse_skill_file
from deerflow.skills.types import Skill, SkillCategory
from deerflow.subagents.config import SubagentConfig


@pytest.fixture
def snapshot_paths(monkeypatch, tmp_path: Path) -> Paths:
    """Keep process-local snapshot resources inside each test directory."""
    from deerflow.runtime import skill_snapshot as snapshot_module

    paths = Paths(tmp_path / "state")
    monkeypatch.setattr(snapshot_module, "get_paths", lambda: paths)
    yield paths
    cleanup_abandoned_skill_snapshots()


def _write_skill(
    root: Path,
    *,
    body: str,
    allowed_tools: str = "read_file",
    name: str = "immutable-skill",
) -> Path:
    skill_dir = root / name
    skill_dir.mkdir(parents=True, exist_ok=True)
    skill_file = skill_dir / "SKILL.md"
    skill_file.write_text(
        f"---\nname: {name}\ndescription: Prove accepted skill bytes stay pinned\nallowed-tools: [{allowed_tools}]\n---\n\n{body}\n",
        encoding="utf-8",
    )
    return skill_file


def _parsed_skill(skill_file: Path) -> Skill:
    skill = parse_skill_file(
        skill_file,
        SkillCategory.CUSTOM,
        relative_path=Path(skill_file.parent.name),
    )
    assert skill is not None
    return replace(skill, enabled=True)


def _resolve_revision(
    monkeypatch,
    skill: Skill,
):
    from deerflow.runtime import agent_revision as revision_module

    monkeypatch.setattr(
        revision_module,
        "_skills",
        lambda _app_config, *, user_id: ((skill,), (skill,)),
    )
    monkeypatch.setattr(
        revision_module,
        "load_agent_soul",
        lambda *_a, **_kw: "",
    )
    return resolve_agent_revision(
        {"configurable": {}},
        app_config=AppConfig(sandbox=SandboxConfig(use="test")),
        user_id="user-1",
    )


class _NamedTool:
    def __init__(self, name: str) -> None:
        self.name = name


class _ModelRequest:
    def __init__(self, tools, *, state, context) -> None:
        self.tools = tools
        self.state = state
        self.runtime = SimpleNamespace(context=context)
        self.messages = []

    def override(self, **updates):
        return _ModelRequest(
            updates.get("tools", self.tools),
            state=updates.get("state", self.state),
            context=self.runtime.context,
        )


def _accepted(revision) -> AcceptedInvocation:
    return AcceptedInvocation.seal(
        principal=PrincipalProjection(user_id="user-1"),
        origin=InvocationOrigin(source_kind="http"),
        thread_id="thread-1",
        context_references={},
        agent_revision=revision,
        normalized_input={},
        execution_options={},
        extension_generation=1,
        contributor_execution_digest=canonical_digest({"version": 1, "execution": []}),
    )


def test_same_process_live_edit_cannot_replace_accepted_slash_skill(
    monkeypatch,
    tmp_path: Path,
    snapshot_paths: Paths,
) -> None:
    """Regression: execution used to reread mutable SKILL.md after acceptance."""
    from deerflow.agents.middlewares import skill_activation_middleware as activation_module

    skill_file = _write_skill(tmp_path, body="ACCEPTED INSTRUCTIONS")
    skill = _parsed_skill(skill_file)
    revision = _resolve_revision(monkeypatch, skill)
    accepted = _accepted(revision)
    assert accepted.agent_revision.material is not None

    _write_skill(tmp_path, body="MUTATED INSTRUCTIONS")
    storage = SimpleNamespace(
        load_skills=lambda *, enabled_only: [skill],
        get_container_root=lambda: "/mnt/skills",
        get_skills_root_path=lambda: tmp_path,
        validate_skill_file_path=lambda path: path.resolve(),
    )
    monkeypatch.setattr(
        activation_module,
        "get_or_new_user_skill_storage",
        lambda *_a, **_kw: storage,
    )

    runtime = SimpleNamespace(
        context={
            RESOLVED_AGENT_MATERIAL_CONTEXT_KEY: accepted.agent_revision.material,
        }
    )
    original = HumanMessage(
        content="/immutable-skill do the work",
        id="message-1",
    )
    request = ModelRequest(
        model=object(),
        messages=[original],
        state={"messages": [original]},
        runtime=runtime,
    )
    captured: dict[str, object] = {}

    def handler(model_request: ModelRequest) -> AIMessage:
        captured["messages"] = model_request.messages
        return AIMessage(content="ok")

    result = SkillActivationMiddleware(
        app_config=AppConfig(sandbox=SandboxConfig(use="test")),
        user_id="user-1",
        slash_source_owner_token="test-owner",
    ).wrap_model_call(request, handler)

    assert result.content == "ok"
    messages = captured["messages"]
    assert isinstance(messages, list)
    reminder = next(message for message in messages if is_slash_skill_activation_reminder(message))
    assert "ACCEPTED INSTRUCTIONS" in reminder.content
    assert "MUTATED INSTRUCTIONS" not in reminder.content
    accepted.agent_revision.material.release_process_material()


def test_supporting_files_and_sandbox_reads_use_the_accepted_snapshot(
    monkeypatch,
    tmp_path: Path,
    snapshot_paths: Paths,
) -> None:
    skill_file = _write_skill(tmp_path, body="Read references/guide.txt")
    support = skill_file.parent / "references" / "guide.txt"
    support.parent.mkdir()
    support.write_text("ACCEPTED RESOURCE", encoding="utf-8")
    revision = _resolve_revision(monkeypatch, _parsed_skill(skill_file))
    material = revision.material
    assert material is not None and material.skill_snapshot is not None
    snapshot_skill = material.enabled_skill_objects[0]
    support.write_text("MUTATED RESOURCE", encoding="utf-8")

    sandbox = LocalSandbox(
        "snapshot-test",
        path_mappings=[
            PathMapping(
                container_path="/mnt/skills/.accepted",
                local_path=str(snapshot_paths.skill_snapshot_scope_dir("user-1")),
                read_only=True,
            )
        ],
    )
    container_path = f"{snapshot_skill.get_container_path()}/references/guide.txt"

    assert sandbox.read_file(container_path) == "ACCEPTED RESOURCE"
    assert ".accepted" in container_path
    material.release_process_material()


def test_deleting_live_tree_cannot_remove_accepted_supporting_material(
    monkeypatch,
    tmp_path: Path,
    snapshot_paths: Paths,
) -> None:
    skill_file = _write_skill(tmp_path, body="Read scripts/run.sh")
    script = skill_file.parent / "scripts" / "run.sh"
    script.parent.mkdir()
    script.write_text("#!/bin/sh\necho accepted\n", encoding="utf-8")
    script.chmod(0o755)
    revision = _resolve_revision(monkeypatch, _parsed_skill(skill_file))
    material = revision.material
    assert material is not None
    snapshot_script = material.enabled_skill_objects[0].skill_dir / "scripts/run.sh"

    shutil.rmtree(skill_file.parent)

    assert snapshot_script.read_text(encoding="utf-8").endswith("echo accepted\n")
    assert snapshot_script.stat().st_mode & stat.S_IXUSR
    assert not snapshot_script.stat().st_mode & stat.S_IWUSR
    material.release_process_material()


def test_live_allowed_tools_edit_cannot_widen_accepted_policy(
    monkeypatch,
    tmp_path: Path,
    snapshot_paths: Paths,
) -> None:
    from deerflow.agents.middlewares.skill_tool_policy_middleware import (
        SkillToolPolicyMiddleware,
    )

    skill_file = _write_skill(
        tmp_path,
        body="Accepted policy",
        allowed_tools="read_file",
    )
    revision = _resolve_revision(monkeypatch, _parsed_skill(skill_file))
    material = revision.material
    assert material is not None
    snapshot_skill = material.enabled_skill_objects[0]
    _write_skill(
        tmp_path,
        body="Mutated policy",
        allowed_tools="read_file, web_search",
    )
    request = _ModelRequest(
        [_NamedTool("read_file"), _NamedTool("web_search")],
        state={"skill_context": [{"path": snapshot_skill.get_container_file_path()}]},
        context={RESOLVED_AGENT_MATERIAL_CONTEXT_KEY: material},
    )
    middleware = SkillToolPolicyMiddleware(
        slash_source_owner_token="test-owner",
    )

    filtered = middleware.wrap_model_call(request, lambda value: value)

    assert [tool.name for tool in filtered.tools] == ["read_file"]
    material.release_process_material()


@pytest.mark.asyncio
async def test_lead_and_subagent_share_the_same_snapshot_and_revision(
    monkeypatch,
    tmp_path: Path,
    snapshot_paths: Paths,
) -> None:
    monkeypatch.delitem(sys.modules, "deerflow.subagents.executor", raising=False)
    executor_module = importlib.import_module("deerflow.subagents.executor")
    skill_file = _write_skill(tmp_path, body="Shared instructions")
    revision = _resolve_revision(monkeypatch, _parsed_skill(skill_file))
    material = revision.material
    assert material is not None
    executor = executor_module.SubagentExecutor(
        config=SubagentConfig(
            name="general-purpose",
            description="test",
            skills=["immutable-skill"],
        ),
        tools=[],
        app_config=material.app_config,
        parent_model="test-model",
        resolved_agent_material=material,
    )

    subagent_skills = await executor._load_skills()

    assert subagent_skills == list(material.enabled_skill_objects)
    assert revision.digest == type(revision).from_material(material).digest
    material.release_process_material()


@pytest.mark.parametrize(
    ("tree_mutation", "expected_code"),
    [
        ("symlink", "skill_snapshot_symlink"),
        ("traversal", "skill_snapshot_path_invalid"),
        ("special", "skill_snapshot_special_file"),
    ],
)
def test_unsafe_skill_trees_fail_closed(
    tmp_path: Path,
    snapshot_paths: Paths,
    tree_mutation: str,
    expected_code: str,
) -> None:
    skill_file = _write_skill(tmp_path, body="Safe before mutation")
    skill = _parsed_skill(skill_file)
    if tree_mutation == "symlink":
        (skill_file.parent / "link").symlink_to(skill_file)
    elif tree_mutation == "traversal":
        skill = replace(skill, relative_path=Path("../escape"))
    else:
        if not hasattr(os, "mkfifo"):
            pytest.skip("special-file fixture requires mkfifo")
        os.mkfifo(skill_file.parent / "pipe")

    with pytest.raises(SkillSnapshotError, match=expected_code):
        snapshot_effective_skills((skill,), user_id="user-1")


@pytest.mark.parametrize(
    ("limits", "expected_code"),
    [
        (SkillSnapshotLimits(max_file_bytes=8), "skill_snapshot_file_too_large"),
        (
            SkillSnapshotLimits(max_files_per_skill=1),
            "skill_snapshot_too_many_files",
        ),
        (
            SkillSnapshotLimits(max_relative_path_bytes=3),
            "skill_snapshot_path_too_long",
        ),
    ],
)
def test_snapshot_bounds_fail_closed(
    tmp_path: Path,
    snapshot_paths: Paths,
    limits: SkillSnapshotLimits,
    expected_code: str,
) -> None:
    skill_file = _write_skill(tmp_path, body="Bounded material")
    (skill_file.parent / "support.txt").write_text("support", encoding="utf-8")

    with pytest.raises(SkillSnapshotError, match=expected_code):
        snapshot_effective_skills(
            (_parsed_skill(skill_file),),
            user_id="user-1",
            limits=limits,
        )


@pytest.mark.parametrize(
    ("limits", "expected_code"),
    [
        (
            SkillSnapshotLimits(max_total_files=3),
            "skill_snapshot_too_many_files",
        ),
        (
            SkillSnapshotLimits(max_total_bytes=350),
            "skill_snapshot_total_too_large",
        ),
    ],
)
def test_aggregate_snapshot_bounds_apply_across_skills(
    tmp_path: Path,
    snapshot_paths: Paths,
    limits: SkillSnapshotLimits,
    expected_code: str,
) -> None:
    first_file = _write_skill(
        tmp_path / "first",
        body="a" * 100,
        name="first-skill",
    )
    second_file = _write_skill(
        tmp_path / "second",
        body="b" * 100,
        name="second-skill",
    )
    (first_file.parent / "support.txt").write_text("first", encoding="utf-8")
    (second_file.parent / "support.txt").write_text("second", encoding="utf-8")

    with pytest.raises(SkillSnapshotError, match=expected_code):
        snapshot_effective_skills(
            (_parsed_skill(first_file), _parsed_skill(second_file)),
            user_id="user-1",
            limits=limits,
        )


def test_unreadable_and_mid_snapshot_mutation_fail_closed(
    monkeypatch,
    tmp_path: Path,
    snapshot_paths: Paths,
) -> None:
    from deerflow.runtime import skill_snapshot as snapshot_module

    skill_file = _write_skill(tmp_path, body="Initial")
    support = skill_file.parent / "support.txt"
    support.write_text("initial", encoding="utf-8")
    skill = _parsed_skill(skill_file)
    original_read = snapshot_module._read_stable_regular_file

    def unreadable(path: Path, *, limits):
        if path.name == "support.txt":
            raise SkillSnapshotError("skill_snapshot_file_unreadable")
        return original_read(path, limits=limits)

    monkeypatch.setattr(snapshot_module, "_read_stable_regular_file", unreadable)
    with pytest.raises(
        SkillSnapshotError,
        match="skill_snapshot_file_unreadable",
    ):
        snapshot_effective_skills((skill,), user_id="user-1")

    monkeypatch.setattr(
        snapshot_module,
        "_read_stable_regular_file",
        original_read,
    )
    original_walk = snapshot_module._walk_skill_files
    calls = 0

    def mutate_after_copy(root: Path, *, limits):
        nonlocal calls
        result = original_walk(root, limits=limits)
        calls += 1
        if calls == 1:
            support.write_text("changed", encoding="utf-8")
        return result

    monkeypatch.setattr(snapshot_module, "_walk_skill_files", mutate_after_copy)
    with pytest.raises(SkillSnapshotError, match="skill_snapshot_changed"):
        snapshot_effective_skills((skill,), user_id="user-1")


def test_snapshot_publication_and_shared_cleanup_are_atomic(
    tmp_path: Path,
    snapshot_paths: Paths,
) -> None:
    skill_file = _write_skill(tmp_path, body="Shared")
    skill = _parsed_skill(skill_file)
    first = snapshot_effective_skills((skill,), user_id="user-1")
    second = snapshot_effective_skills((skill,), user_id="user-1")
    assert first is not None and second is not None
    assert first.root == second.root
    assert first.root.is_dir()
    assert not any(path.name.startswith(".building-") for path in first.root.parent.iterdir())

    first.release()
    assert second.root.is_dir()
    second.release()
    assert not second.root.exists()


def test_subagent_material_lease_outlives_parent_terminal_cleanup(
    monkeypatch,
    tmp_path: Path,
    snapshot_paths: Paths,
) -> None:
    skill_file = _write_skill(tmp_path, body="Child still running")
    material = _resolve_revision(
        monkeypatch,
        _parsed_skill(skill_file),
    ).material
    assert material is not None and material.skill_snapshot is not None
    snapshot_root = material.skill_snapshot.root

    child_material = material.retain_process_material()
    material.release_process_material()

    assert snapshot_root.is_dir()
    child_material.verify_process_material()
    child_material.release_process_material()
    assert not snapshot_root.exists()


def test_startup_cleanup_removes_only_abandoned_snapshots(
    tmp_path: Path,
    snapshot_paths: Paths,
) -> None:
    skill_file = _write_skill(tmp_path, body="Active")
    active = snapshot_effective_skills(
        (_parsed_skill(skill_file),),
        user_id="user-1",
    )
    assert active is not None
    abandoned = snapshot_paths.skill_snapshot_scope_dir("user-2") / ("f" * 64)
    abandoned.mkdir(parents=True)
    (abandoned / "partial").write_text("stale", encoding="utf-8")

    assert cleanup_abandoned_skill_snapshots() == 1
    assert active.root.is_dir()
    assert not abandoned.exists()
    active.release()


@pytest.mark.asyncio
async def test_terminal_worker_releases_snapshot_after_execution(
    monkeypatch,
    tmp_path: Path,
    snapshot_paths: Paths,
) -> None:
    skill_file = _write_skill(tmp_path, body="Terminal cleanup")
    revision = _resolve_revision(monkeypatch, _parsed_skill(skill_file))
    material = revision.material
    assert material is not None and material.skill_snapshot is not None
    snapshot_root = material.skill_snapshot.root
    manager = RunManager()
    record = await manager.create_or_reject(
        "thread-worker",
        accepted_invocation=_accepted(revision),
    )

    class _Agent:
        async def astream(self, *_args, **_kwargs):
            yield {"messages": []}

    def factory(*, config):
        assert snapshot_root.is_dir()
        assert config["context"][RESOLVED_AGENT_MATERIAL_CONTEXT_KEY] is material
        return _Agent()

    bridge = SimpleNamespace(
        publish=AsyncMock(),
        publish_end=AsyncMock(),
        cleanup=AsyncMock(),
    )
    await run_agent(
        bridge,
        manager,
        record,
        ctx=RunContext(checkpointer=None),
        agent_factory=factory,
        graph_input={},
        config={},
    )

    assert record.status is RunStatus.success
    assert not snapshot_root.exists()


@pytest.mark.asyncio
async def test_cancelled_before_start_releases_snapshot(
    monkeypatch,
    tmp_path: Path,
    snapshot_paths: Paths,
) -> None:
    skill_file = _write_skill(tmp_path, body="Cancelled cleanup")
    revision = _resolve_revision(monkeypatch, _parsed_skill(skill_file))
    material = revision.material
    assert material is not None and material.skill_snapshot is not None
    snapshot_root = material.skill_snapshot.root
    manager = RunManager()
    record = await manager.create_or_reject(
        "thread-worker",
        accepted_invocation=_accepted(revision),
    )
    record.abort_event.set()
    bridge = SimpleNamespace(
        publish=AsyncMock(),
        publish_end=AsyncMock(),
        cleanup=AsyncMock(),
    )

    await run_agent(
        bridge,
        manager,
        record,
        ctx=RunContext(checkpointer=None),
        agent_factory=lambda **_kwargs: (_ for _ in ()).throw(AssertionError("cancelled run must not construct a graph")),
        graph_input={},
        config={},
    )

    assert not snapshot_root.exists()


@pytest.mark.asyncio
async def test_snapshot_drift_fails_before_graph_construction(
    monkeypatch,
    tmp_path: Path,
    snapshot_paths: Paths,
) -> None:
    skill_file = _write_skill(tmp_path, body="Pinned")
    revision = _resolve_revision(monkeypatch, _parsed_skill(skill_file))
    material = revision.material
    assert material is not None and material.skill_snapshot is not None
    snapshot_file = material.enabled_skill_objects[0].skill_file
    snapshot_file.chmod(0o600)
    snapshot_file.write_text("tampered", encoding="utf-8")
    manager = RunManager()
    record = await manager.create_or_reject(
        "thread-worker",
        accepted_invocation=_accepted(revision),
    )
    factory_calls = 0

    def factory(*, config):
        nonlocal factory_calls
        factory_calls += 1
        raise AssertionError("graph construction must not run")

    bridge = SimpleNamespace(
        publish=AsyncMock(),
        publish_end=AsyncMock(),
        cleanup=AsyncMock(),
    )
    await run_agent(
        bridge,
        manager,
        record,
        ctx=RunContext(checkpointer=None),
        agent_factory=factory,
        graph_input={},
        config={},
    )

    assert factory_calls == 0
    assert record.status is RunStatus.error
    assert record.stop_reason == "agent_revision_drift"
    assert not material.skill_snapshot.root.exists()


def test_persisted_acceptance_contains_only_skill_evidence(
    monkeypatch,
    tmp_path: Path,
    snapshot_paths: Paths,
) -> None:
    skill_file = _write_skill(tmp_path, body="PRIVATE INSTRUCTION BODY")
    revision = _resolve_revision(monkeypatch, _parsed_skill(skill_file))
    accepted = _accepted(revision)

    persisted = json.dumps(accepted.to_persisted(), sort_keys=True)

    assert revision.digest in persisted
    assert "PRIVATE INSTRUCTION BODY" not in persisted
    assert ".accepted" not in persisted
    assert str(tmp_path) not in persisted
    assert accepted.agent_revision.material is not None
    accepted.agent_revision.material.release_process_material()


def test_accepted_empty_skill_set_never_falls_back_to_live_registry(
    monkeypatch,
    snapshot_paths: Paths,
) -> None:
    from deerflow.agents.middlewares import skill_activation_middleware as activation_module
    from deerflow.runtime import agent_revision as revision_module

    monkeypatch.setattr(
        revision_module,
        "_skills",
        lambda _app_config, *, user_id: ((), ()),
    )
    monkeypatch.setattr(revision_module, "load_agent_soul", lambda *_a, **_kw: "")
    revision = resolve_agent_revision(
        {"configurable": {}},
        app_config=AppConfig(sandbox=SandboxConfig(use="test")),
        user_id="user-1",
    )
    material = revision.material
    assert material is not None and material.skill_snapshot is None
    monkeypatch.setattr(
        activation_module,
        "get_or_new_user_skill_storage",
        lambda *_a, **_kw: (_ for _ in ()).throw(AssertionError("accepted execution must not consult live skills")),
    )
    middleware = SkillActivationMiddleware(
        app_config=material.app_config,
        user_id="user-1",
        slash_source_owner_token="test-owner",
    )

    resolution = middleware._resolve_activation(
        "/new-live-skill run",
        run_context={RESOLVED_AGENT_MATERIAL_CONTEXT_KEY: material},
    )

    assert resolution is not None
    assert resolution.failure_message == "Skill `/new-live-skill` is not installed."


def test_live_registry_change_creates_a_later_revision_only(
    monkeypatch,
    tmp_path: Path,
    snapshot_paths: Paths,
) -> None:
    skill_file = _write_skill(tmp_path, body="Revision one")
    first = _resolve_revision(monkeypatch, _parsed_skill(skill_file))
    first_material = first.material
    assert first_material is not None
    _write_skill(tmp_path, body="Revision two")
    second = _resolve_revision(monkeypatch, _parsed_skill(skill_file))
    second_material = second.material
    assert second_material is not None

    assert first.digest != second.digest
    assert first_material.enabled_skill_objects[0].skill_file.read_text(encoding="utf-8").strip().endswith("Revision one")
    assert second_material.enabled_skill_objects[0].skill_file.read_text(encoding="utf-8").strip().endswith("Revision two")
    first_material.release_process_material()
    second_material.release_process_material()
