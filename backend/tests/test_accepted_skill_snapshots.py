"""Accepted skill material remains immutable for the lifetime of a run."""

from __future__ import annotations

import asyncio
import hashlib
import importlib
import json
import os
import shutil
import stat
import sys
from dataclasses import replace
from datetime import UTC, datetime, timedelta
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
from deerflow.config.run_ownership_config import RunOwnershipConfig
from deerflow.config.sandbox_config import SandboxConfig
from deerflow.runtime.accepted_invocation import (
    AcceptedInvocation,
    InvocationOrigin,
    PrincipalProjection,
    ResolvedAgentMaterialV1,
    ResolvedAgentRevision,
    canonical_digest,
)
from deerflow.runtime.agent_revision import (
    RESOLVED_AGENT_MATERIAL_CONTEXT_KEY,
    resolve_agent_revision,
)
from deerflow.runtime.events.store.memory import MemoryRunEventStore
from deerflow.runtime.runs.manager import RunManager
from deerflow.runtime.runs.schemas import RunStatus
from deerflow.runtime.runs.store.memory import MemoryRunStore
from deerflow.runtime.runs.worker import (
    RunContext,
    _AcceptedMaterializationResult,
    run_agent,
)
from deerflow.runtime.skill_snapshot import (
    SkillSnapshotError,
    SkillSnapshotLimits,
    cleanup_abandoned_skill_snapshots,
    snapshot_effective_skills,
)
from deerflow.runtime.tenant_identity import (
    TENANT_REFERENCE_CONTEXT_KEY,
    TenantIdentityV1,
)
from deerflow.sandbox import tools as sandbox_tools
from deerflow.sandbox.accepted_material import (
    AcceptedExecutionEvidenceV1,
    AcceptedExecutionEvidenceV2,
    AcceptedMaterialCapability,
    AcceptedMaterialLeaseV1,
    AcceptedMaterialRequestV1,
    AcceptedMaterialRequestV2,
    AcceptedSandboxCapabilityProfileV1,
    AcceptedSandboxQualificationV1,
    AcceptedSkillExecutionEvidenceV2,
)
from deerflow.sandbox.local.local_sandbox import LocalSandbox, PathMapping
from deerflow.sandbox.sandbox import Sandbox
from deerflow.sandbox.sandbox_provider import AcceptedSkillExecutionEvidenceV1
from deerflow.skills.parser import parse_skill_file
from deerflow.skills.types import Skill, SkillCategory

_TEST_TENANT = TenantIdentityV1.from_canonical_id("local").to_persisted_reference()


@pytest.fixture
def snapshot_paths(monkeypatch, tmp_path: Path) -> Paths:
    """Keep process-local snapshot resources inside each test directory."""
    from deerflow.runtime import skill_snapshot as snapshot_module

    monkeypatch.delattr(sandbox_tools._get_skills_container_path, "_cached", raising=False)
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


def _accepted(
    revision,
    *,
    tool_plane_revision=None,
) -> AcceptedInvocation:
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
        tenant=_TEST_TENANT,
        tool_plane_revision=tool_plane_revision,
    )


def _assembled_agent_for_revision(revision: ResolvedAgentRevision, graph: object):
    """Build the minimal authoritative descriptor needed by durable worker tests."""

    from deerflow.agents.assembly_descriptor import (
        build_assembly_descriptor,
        subagent_release_policy,
    )
    from deerflow.agents.lead_agent.agent import LeadAgentAssembly

    material = revision.material
    assert material is not None
    defaults = material.runtime_defaults
    enabled_skills = list(material.enabled_skill_objects)
    if bool(defaults.get("is_bootstrap", False)):
        enabled_skills = [skill for skill in enabled_skills if skill.name == "bootstrap"]
    requested_subagents = bool(defaults.get("subagent_enabled", False))
    allowed_subagents = getattr(material.agent_config_object, "allowed_subagents", None)
    max_concurrent = int(defaults.get("max_concurrent_subagents", 3))
    max_total = int(defaults.get("max_total_subagents", 6))
    descriptor = build_assembly_descriptor(
        namespace="deerflow",
        agent_name=("bootstrap" if bool(defaults.get("is_bootstrap", False)) else str(defaults.get("agent_name") or "lead-agent")),
        requested_model=None,
        effective_model=str(material.model_profile["name"]),
        model_config=SimpleNamespace(),
        thinking_enabled=bool(defaults.get("thinking_enabled", True)),
        reasoning_effort=defaults.get("reasoning_effort"),
        rendered_base_prompt="accepted prompt",
        tools=[],
        middlewares=[],
        deferred_names=frozenset(),
        enabled_skills=enabled_skills,
        effective_policies={
            "bootstrap": bool(defaults.get("is_bootstrap", False)),
            "non_interactive": bool(defaults.get("non_interactive", False)),
            "plan_mode": bool(defaults.get("is_plan_mode", False)),
            "recursion_limit": "framework-default",
            "subagents": subagent_release_policy(
                material.app_config,
                enabled=requested_subagents and allowed_subagents != [],
                max_concurrent=max_concurrent,
                max_total=max_total,
                resolved_subagent_catalog=material.subagent_catalog,
            ),
        },
    )
    return LeadAgentAssembly(graph=graph, descriptor=descriptor)


def _runtime_for_revision(revision) -> SimpleNamespace:
    material = revision.material
    assert material is not None
    return SimpleNamespace(
        state={},
        context={
            "thread_id": "thread-1",
            "run_id": "run-1",
            "user_id": "user-1",
            RESOLVED_AGENT_MATERIAL_CONTEXT_KEY: material,
        },
        config={"configurable": {"thread_id": "thread-1"}},
    )


def test_accepted_execution_rejects_live_skill_reads_before_sandbox_io(
    monkeypatch,
    tmp_path: Path,
    snapshot_paths: Paths,
) -> None:
    skill_file = _write_skill(tmp_path, body="ACCEPTED")
    revision = _resolve_revision(monkeypatch, _parsed_skill(skill_file))
    runtime = _runtime_for_revision(revision)
    sandbox_calls = 0

    def unexpected_sandbox(_runtime):
        nonlocal sandbox_calls
        sandbox_calls += 1
        raise AssertionError("live skill denial must precede sandbox IO")

    monkeypatch.setattr(sandbox_tools, "ensure_sandbox_initialized", unexpected_sandbox)

    result = sandbox_tools.read_file_tool.func(
        runtime,
        "/mnt/skills/custom/immutable-skill/SKILL.md",
        "read mutable instructions",
    )

    assert result == "Error: Permission denied reading file: /mnt/skills/custom/immutable-skill/SKILL.md"
    assert sandbox_calls == 0
    revision.material.release_process_material()


@pytest.mark.parametrize(
    ("invoke", "expected"),
    [
        (
            lambda runtime: sandbox_tools.ls_tool.func(runtime, "/mnt/skills", "list mutable skills"),
            "Error: Permission denied: /mnt/skills",
        ),
        (
            lambda runtime: sandbox_tools.glob_tool.func(runtime, "**/*", "/mnt/skills/custom", "find mutable skills"),
            "Error: Permission denied: /mnt/skills/custom",
        ),
        (
            lambda runtime: sandbox_tools.grep_tool.func(runtime, "secret", "/mnt/skills/public", "search mutable skills"),
            "Error: Permission denied: /mnt/skills/public",
        ),
        (
            lambda runtime: sandbox_tools.bash_tool.func(runtime, "bash /mnt/skills/custom/tool/run.sh", "execute mutable script"),
            "Error: Durable invocation may access only its accepted skill snapshot",
        ),
        (
            lambda runtime: sandbox_tools.bash_tool.func(
                runtime,
                "cd /mnt/skills/.accepted/" + "a" * 64 + "; cat ../../custom/tool/SKILL.md",
                "escape accepted tree",
            ),
            "Error: Durable invocation may access only its accepted skill snapshot",
        ),
    ],
)
def test_accepted_execution_rejects_all_live_skill_tool_bypasses_before_io(
    monkeypatch,
    tmp_path: Path,
    snapshot_paths: Paths,
    invoke,
    expected: str,
) -> None:
    skill_file = _write_skill(tmp_path, body="ACCEPTED")
    revision = _resolve_revision(monkeypatch, _parsed_skill(skill_file))
    runtime = _runtime_for_revision(revision)
    sandbox_calls = 0

    def unexpected_sandbox(_runtime):
        nonlocal sandbox_calls
        sandbox_calls += 1
        raise AssertionError("accepted skill denial must precede sandbox IO")

    monkeypatch.setattr(sandbox_tools, "ensure_sandbox_initialized", unexpected_sandbox)

    assert invoke(runtime) == expected
    assert sandbox_calls == 0
    revision.material.release_process_material()


def test_accepted_execution_allows_only_its_exact_snapshot_path(
    monkeypatch,
    tmp_path: Path,
    snapshot_paths: Paths,
) -> None:
    skill_file = _write_skill(tmp_path, body="ACCEPTED")
    revision = _resolve_revision(monkeypatch, _parsed_skill(skill_file))
    runtime = _runtime_for_revision(revision)
    material = revision.material
    assert material is not None and material.skill_snapshot is not None
    snapshot_root = f"/mnt/skills/.accepted/{material.skill_snapshot.snapshot_id}"

    sandbox_tools._validate_runtime_skill_path(runtime, "/mnt/skills/.accepted")
    sandbox_tools._validate_runtime_skill_path(runtime, f"{snapshot_root}/custom/immutable-skill/SKILL.md")
    with pytest.raises(PermissionError, match="only its accepted skill snapshot"):
        sandbox_tools._validate_runtime_skill_path(runtime, f"/mnt/skills/.accepted/{'f' * 64}/custom/other/SKILL.md")
    material.release_process_material()


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


def test_local_sandbox_exposes_only_the_bound_accepted_snapshot(
    monkeypatch,
    tmp_path: Path,
    snapshot_paths: Paths,
) -> None:
    """A same-subject sibling snapshot must not be visible inside the run."""
    import deerflow.config as config_module
    from deerflow.config import paths as paths_module
    from deerflow.runtime.skill_projection import SkillProjectionClear, SkillProjectionEvidence
    from deerflow.sandbox.local.local_sandbox_provider import LocalSandboxProvider
    from deerflow.sandbox.sandbox_provider import AcceptedSkillSandboxBindingV1

    first_file = _write_skill(
        tmp_path / "first",
        body="FIRST ACCEPTED RESOURCE",
        name="first-skill",
    )
    second_file = _write_skill(
        tmp_path / "second",
        body="SIBLING SECRET RESOURCE",
        name="second-skill",
    )
    first = snapshot_effective_skills(
        (_parsed_skill(first_file),),
        user_id="user-1",
    )
    second = snapshot_effective_skills(
        (_parsed_skill(second_file),),
        user_id="user-1",
    )
    assert first is not None and second is not None

    projection = SimpleNamespace(
        public=tmp_path / "view" / "public",
        custom=tmp_path / "view" / "custom",
        legacy=tmp_path / "view" / "legacy",
        integrations=tmp_path / "view" / "integrations",
    )
    for path in vars(projection).values():
        path.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(paths_module, "get_paths", lambda: snapshot_paths)
    monkeypatch.setattr(
        config_module,
        "get_app_config",
        lambda: SimpleNamespace(
            skills=SimpleNamespace(container_path="/mnt/skills"),
            sandbox=SimpleNamespace(mounts=[]),
        ),
    )
    monkeypatch.setattr(
        LocalSandboxProvider,
        "_ensure_skills_projection",
        staticmethod(lambda *_args, **_kwargs: projection),
    )

    provider = LocalSandboxProvider()
    sandbox_id = provider.acquire_accepted_skills("thread-1", user_id="user-1")
    provider.bind_accepted_skill_snapshot(
        sandbox_id,
        thread_id="thread-1",
        user_id="user-1",
        binding=AcceptedSkillSandboxBindingV1(
            snapshot_id=first.snapshot_id,
            run_id="run-first",
            generation=1,
            evidence=SkillProjectionEvidence.from_snapshot(first),
        ),
    )
    sandbox = provider.get(sandbox_id)
    assert sandbox is not None

    selected_path = f"/mnt/skills/.accepted/{first.snapshot_id}/custom/first-skill/SKILL.md"
    sibling_path = f"/mnt/skills/.accepted/{second.snapshot_id}/custom/second-skill/SKILL.md"
    assert "FIRST ACCEPTED RESOURCE" in sandbox.read_file(selected_path)
    with pytest.raises(FileNotFoundError):
        sandbox.read_file(sibling_path)

    assert provider.clear_accepted_skill_snapshot(
        SkillProjectionClear(
            user_id="user-1",
            thread_id="thread-1",
            sandbox_id=sandbox_id,
            run_id="run-first",
            generation=1,
            snapshot_id=first.snapshot_id,
        )
    )
    provider.release(sandbox_id)
    cached_id = provider.acquire_accepted_skills("thread-1", user_id="user-1")
    assert cached_id == sandbox_id
    provider.bind_accepted_skill_snapshot(
        cached_id,
        thread_id="thread-1",
        user_id="user-1",
        binding=AcceptedSkillSandboxBindingV1(
            snapshot_id=None,
            run_id="run-empty",
            generation=2,
            evidence=SkillProjectionEvidence.from_snapshot(None),
        ),
    )
    with pytest.raises(FileNotFoundError):
        sandbox.read_file(selected_path)

    assert provider.clear_accepted_skill_snapshot(
        SkillProjectionClear(
            user_id="user-1",
            thread_id="thread-1",
            sandbox_id=sandbox_id,
            run_id="run-empty",
            generation=2,
            snapshot_id=None,
        )
    )
    provider.release(cached_id)
    provider.bind_accepted_skill_snapshot(
        provider.acquire("thread-1", user_id="user-1"),
        thread_id="thread-1",
        user_id="user-1",
        binding=AcceptedSkillSandboxBindingV1(
            snapshot_id=second.snapshot_id,
            evidence=SkillProjectionEvidence.from_snapshot(second),
        ),
    )
    assert "SIBLING SECRET RESOURCE" in sandbox.read_file(sibling_path)
    with pytest.raises(FileNotFoundError):
        sandbox.read_file(selected_path)

    # Provider release alone is not an invocation-clear authority.
    provider.release(sandbox_id)
    first.release()
    second.release()


def test_failed_active_view_publication_leaves_no_partial_snapshot(
    monkeypatch,
    tmp_path: Path,
    snapshot_paths: Paths,
) -> None:
    from deerflow.runtime import skill_snapshot as snapshot_module
    from deerflow.runtime.skill_projection import SkillProjectionEvidence

    skill_file = _write_skill(tmp_path, body="never partially visible")
    snapshot = snapshot_effective_skills(
        (_parsed_skill(skill_file),),
        user_id="user-1",
    )
    assert snapshot is not None

    def fail_publish(*_args: object) -> None:
        raise OSError("publish failed")

    monkeypatch.setattr(
        snapshot_module.os,
        "replace",
        fail_publish,
    )

    with pytest.raises(OSError, match="publish failed"):
        snapshot_module.bind_skill_snapshot_active_view(
            user_id="user-1",
            thread_id="thread-1",
            snapshot_id=snapshot.snapshot_id,
            evidence=SkillProjectionEvidence.from_snapshot(snapshot),
        )

    view = snapshot_paths.skill_snapshot_active_view_dir(
        "user-1",
        "thread-1",
    )
    assert list(view.iterdir()) == []
    assert not any(child.name.startswith(".binding-") for child in view.parent.iterdir())
    snapshot.release()


def test_replacement_waits_until_prior_projection_cleanup_finishes(
    monkeypatch,
    tmp_path: Path,
    snapshot_paths: Paths,
) -> None:
    """Ownership remains busy until physical cleanup of the prior run ends."""
    import threading

    import deerflow.config as config_module
    from deerflow.config import paths as paths_module
    from deerflow.runtime.skill_projection import (
        SkillProjectionBusyError,
        SkillProjectionEvidence,
        get_skill_projection_coordinator,
    )
    from deerflow.sandbox.local.local_sandbox_provider import LocalSandboxProvider
    from deerflow.sandbox.sandbox_provider import (
        AcceptedSkillSandboxBindingV1,
        release_accepted_skill_consumer,
        reset_sandbox_provider,
        set_sandbox_provider,
    )

    first_file = _write_skill(tmp_path / "stale-a", body="STALE A", name="stale-a")
    second_file = _write_skill(tmp_path / "current-b", body="CURRENT B", name="current-b")
    first = snapshot_effective_skills((_parsed_skill(first_file),), user_id="fenced-user")
    second = snapshot_effective_skills((_parsed_skill(second_file),), user_id="fenced-user")
    assert first is not None and second is not None
    projection = SimpleNamespace(
        public=tmp_path / "view" / "public",
        custom=tmp_path / "view" / "custom",
        legacy=tmp_path / "view" / "legacy",
        integrations=tmp_path / "view" / "integrations",
    )
    for path in vars(projection).values():
        path.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(paths_module, "get_paths", lambda: snapshot_paths)
    monkeypatch.setattr(
        config_module,
        "get_app_config",
        lambda: SimpleNamespace(
            skills=SimpleNamespace(container_path="/mnt/skills"),
            sandbox=SimpleNamespace(mounts=[]),
        ),
    )
    monkeypatch.setattr(
        LocalSandboxProvider,
        "_ensure_skills_projection",
        staticmethod(lambda *_args, **_kwargs: projection),
    )

    provider = LocalSandboxProvider()
    set_sandbox_provider(provider)
    coordinator = get_skill_projection_coordinator()
    sandbox_id = provider.acquire_accepted_skills(
        "thread-fenced",
        user_id="fenced-user",
    )

    def activate(run_id: str, snapshot) -> object:

        coordinator.claim_committed_run(
            user_id="fenced-user",
            thread_id="thread-fenced",
            run_id=run_id,
            snapshot_id=snapshot.snapshot_id,
            evidence=SkillProjectionEvidence.from_snapshot(snapshot),
        )
        token = coordinator.activate(
            user_id="fenced-user",
            thread_id="thread-fenced",
            sandbox_id=sandbox_id,
            run_id=run_id,
            snapshot_id=snapshot.snapshot_id,
            consumer_id=f"run:{run_id}:lead",
        )
        provider.bind_accepted_skill_snapshot(
            sandbox_id,
            thread_id="thread-fenced",
            user_id="fenced-user",
            binding=AcceptedSkillSandboxBindingV1.from_consumer_token(token),
        )
        return token

    token_a = activate("run-stale-a", first)
    clear_started = threading.Event()
    allow_clear = threading.Event()
    original_clear = provider.clear_accepted_skill_snapshot
    original_release = provider.release
    provider_releases: list[str] = []

    def delayed_clear(clear):
        clear_started.set()
        assert allow_clear.wait(timeout=5)
        return original_clear(clear)

    monkeypatch.setattr(provider, "clear_accepted_skill_snapshot", delayed_clear)
    monkeypatch.setattr(provider, "release", provider_releases.append)
    result: list[bool] = []
    cleanup = threading.Thread(
        target=lambda: result.append(release_accepted_skill_consumer(token_a)),
    )
    cleanup.start()
    assert clear_started.wait(timeout=5)

    with pytest.raises(
        SkillProjectionBusyError,
        match="skill_projection_thread_busy",
    ):
        activate("run-current-b", second)

    allow_clear.set()
    cleanup.join(timeout=5)
    assert not cleanup.is_alive()
    assert result == [True]
    assert provider_releases == [sandbox_id]

    token_b = activate("run-current-b", second)

    sandbox = provider.get(sandbox_id)
    assert sandbox is not None
    current_path = f"/mnt/skills/.accepted/{second.snapshot_id}/custom/current-b/SKILL.md"
    stale_path = f"/mnt/skills/.accepted/{first.snapshot_id}/custom/stale-a/SKILL.md"
    assert "CURRENT B" in sandbox.read_file(current_path)
    with pytest.raises(FileNotFoundError):
        sandbox.read_file(stale_path)

    monkeypatch.setattr(provider, "clear_accepted_skill_snapshot", original_clear)
    monkeypatch.setattr(provider, "release", original_release)
    assert release_accepted_skill_consumer(token_b)
    reset_sandbox_provider()
    first.release()
    second.release()


def test_active_view_rejects_symlink_inserted_into_accepted_source(
    tmp_path: Path,
    snapshot_paths: Paths,
) -> None:
    from deerflow.runtime import skill_snapshot as snapshot_module
    from deerflow.runtime.skill_projection import SkillProjectionEvidence

    skill_file = _write_skill(tmp_path / "source-symlink", body="accepted")
    support = skill_file.parent / "references" / "guide.txt"
    support.parent.mkdir()
    support.write_text("accepted support", encoding="utf-8")
    snapshot = snapshot_effective_skills((_parsed_skill(skill_file),), user_id="symlink-user")
    assert snapshot is not None

    projection = snapshot.projections[0]
    copied_support = snapshot.root / projection.category / projection.relative_path / "references" / "guide.txt"
    copied_support.parent.chmod(0o700)
    copied_support.chmod(0o600)
    copied_support.unlink()
    outside = tmp_path / "outside-secret"
    outside.write_text("must not be copied", encoding="utf-8")
    copied_support.symlink_to(outside)

    with pytest.raises(SkillSnapshotError, match="skill_snapshot_symlink"):
        snapshot_module.bind_skill_snapshot_active_view(
            user_id="symlink-user",
            thread_id="thread-symlink",
            snapshot_id=snapshot.snapshot_id,
            run_id="run-symlink",
            generation=1,
            evidence=SkillProjectionEvidence.from_snapshot(snapshot),
        )
    snapshot.release()


def test_active_view_rejects_source_mutation_during_verified_copy(
    monkeypatch,
    tmp_path: Path,
    snapshot_paths: Paths,
) -> None:
    from deerflow.runtime import skill_snapshot as snapshot_module
    from deerflow.runtime.skill_projection import SkillProjectionEvidence

    skill_file = _write_skill(tmp_path / "source-race", body="accepted")
    support = skill_file.parent / "references" / "guide.txt"
    support.parent.mkdir()
    support.write_text("accepted support", encoding="utf-8")
    snapshot = snapshot_effective_skills((_parsed_skill(skill_file),), user_id="race-user")
    assert snapshot is not None
    projection = snapshot.projections[0]
    copied_support = snapshot.root / projection.category / projection.relative_path / "references" / "guide.txt"
    original_write = snapshot_module._write_private_file
    mutated = False

    def mutate_after_first_write(*args, **kwargs):
        nonlocal mutated
        original_write(*args, **kwargs)
        if not mutated:
            mutated = True
            copied_support.chmod(0o600)
            copied_support.write_text("changed during copy", encoding="utf-8")

    monkeypatch.setattr(snapshot_module, "_write_private_file", mutate_after_first_write)
    with pytest.raises(SkillSnapshotError, match="skill_snapshot_(changed|drift)"):
        snapshot_module.bind_skill_snapshot_active_view(
            user_id="race-user",
            thread_id="thread-race",
            snapshot_id=snapshot.snapshot_id,
            run_id="run-race",
            generation=1,
            evidence=SkillProjectionEvidence.from_snapshot(snapshot),
        )
    view = snapshot_paths.skill_snapshot_active_view_dir("race-user", "thread-race")
    assert list(view.iterdir()) == []
    snapshot.release()


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
    from deerflow.runtime.subagent_snapshot import (
        ResolvedSkillScopesV1,
        ResolvedSubagentCatalogV1,
        resolved_subagent_definition,
    )

    projection = material.skill_snapshot.projections[0]
    definition = resolved_subagent_definition(
        name="general-purpose",
        source_kind="builtin",
        source_version="test-source",
        description="test",
        system_prompt="",
        model=None,
        model_settings={},
        tool_names=(),
        skill_names=("immutable-skill",),
        max_turns=50,
        timeout_seconds=900,
        inherits_tools=True,
        policy_settings={
            "token_budget": material.app_config.subagents.get_token_budget_for(
                "general-purpose",
                summarization_enabled=False,
            ).model_dump(mode="json"),
        },
    )
    catalog = ResolvedSubagentCatalogV1.from_entries(
        (definition,),
        allowed_names=("general-purpose",),
    )
    material = replace(
        material,
        # This test exercises shared skill material only.  Leave model
        # construction deferred instead of inventing a live model profile for
        # the synthetic catalog assembled below.
        app_config=None,
        subagent_catalog=catalog,
        skill_scopes=ResolvedSkillScopesV1.from_scopes(
            {
                "lead": (projection.content_digest,),
                "subagent:general-purpose": (projection.content_digest,),
            }
        ),
    )
    revision = ResolvedAgentRevision.from_material(material)
    executor = executor_module.SubagentExecutor(
        config=definition.to_subagent_config(),
        tools=[],
        app_config=material.app_config,
        parent_model=None,
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
    abandoned_view = snapshot_paths.skill_snapshot_active_view_dir(
        "user-3",
        "thread-stale",
    )
    abandoned_view.mkdir(parents=True)
    (abandoned_view / "partial").write_text("stale", encoding="utf-8")

    assert cleanup_abandoned_skill_snapshots() == 2
    assert active.root.is_dir()
    assert not abandoned.exists()
    assert not abandoned_view.exists()
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
    manager = RunManager(tenant=_TEST_TENANT)
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
    monkeypatch.setattr(
        "deerflow.runtime.runs.worker._materialize_accepted_skill_projection",
        AsyncMock(return_value=("sandbox:worker", None)),
    )
    await run_agent(
        bridge,
        manager,
        record,
        ctx=RunContext(checkpointer=None, tenant=_TEST_TENANT),
        agent_factory=factory,
        graph_input={},
        config={},
    )

    assert record.status is RunStatus.success
    assert not snapshot_root.exists()


@pytest.mark.asyncio
async def test_replacement_worker_waits_for_old_projection_before_start(
    monkeypatch,
) -> None:
    material = ResolvedAgentMaterialV1(
        agent_id="lead-agent",
        storage_source="test",
        storage_version="1",
        agent_config=None,
        soul="",
        model_profile={},
    )
    revision = ResolvedAgentRevision.from_material(material)
    accepted = _accepted(revision)
    manager = RunManager(tenant=_TEST_TENANT)
    old = await manager.create_or_reject(
        "thread-1",
        user_id="user-1",
        accepted_invocation=accepted,
    )
    await manager.set_status(old.run_id, RunStatus.running)
    replacement = await manager.create_or_reject(
        "thread-1",
        user_id="user-1",
        accepted_invocation=accepted,
        multitask_strategy="interrupt",
    )
    from deerflow.runtime.skill_projection import (
        SkillProjectionEvidence,
        get_skill_projection_coordinator,
    )

    coordinator = get_skill_projection_coordinator()
    reservation = coordinator.reserve_admission(
        user_id="user-1",
        thread_id="thread-1",
        reservation_id="old:worker-wait",
        snapshot_id=None,
        evidence=SkillProjectionEvidence.from_snapshot(None),
    )
    coordinator.promote_admission(reservation, run_id=old.run_id)
    old_token = coordinator.activate(
        user_id="user-1",
        thread_id="thread-1",
        sandbox_id="sandbox:worker-wait",
        run_id=old.run_id,
        snapshot_id=None,
        consumer_id=f"run:{old.run_id}:lead",
    )
    factory_called = asyncio.Event()

    class _Agent:
        async def astream(self, *_args, **_kwargs):
            yield {"messages": []}

    def factory(**_kwargs):
        factory_called.set()
        return _Agent()

    bridge = SimpleNamespace(
        publish=AsyncMock(),
        publish_end=AsyncMock(),
        cleanup=AsyncMock(),
    )
    monkeypatch.setattr(
        "deerflow.runtime.runs.worker._materialize_accepted_skill_projection",
        AsyncMock(return_value=("sandbox:replacement", None)),
    )
    worker = asyncio.create_task(
        run_agent(
            bridge,
            manager,
            replacement,
            ctx=RunContext(checkpointer=None, tenant=_TEST_TENANT),
            agent_factory=factory,
            graph_input={},
            config={"context": {"user_id": "user-1"}},
        )
    )
    try:
        await asyncio.sleep(0.1)
        assert not factory_called.is_set()
        assert replacement.status is RunStatus.pending

        clear = coordinator.release(old_token)
        assert clear is not None
        assert coordinator.finalize_release(clear)
        await asyncio.wait_for(worker, timeout=2)

        assert factory_called.is_set()
        assert replacement.status is RunStatus.success
    finally:
        if not worker.done():
            worker.cancel()
            await worker
        clear = coordinator.release(old_token)
        if clear is not None:
            coordinator.finalize_release(clear)
        coordinator.release_unactivated_run(
            user_id="user-1",
            thread_id="thread-1",
            run_id=replacement.run_id,
        )


@pytest.mark.asyncio
async def test_superseded_worker_cancels_projection_wait_without_claiming() -> None:
    material = ResolvedAgentMaterialV1(
        agent_id="lead-agent",
        storage_source="test",
        storage_version="1",
        agent_config=None,
        soul="",
        model_profile={},
    )
    accepted = _accepted(ResolvedAgentRevision.from_material(material))
    manager = RunManager(tenant=_TEST_TENANT)
    old = await manager.create_or_reject(
        "thread-1",
        user_id="user-1",
        accepted_invocation=accepted,
    )
    await manager.set_status(old.run_id, RunStatus.running)
    waiting = await manager.create_or_reject(
        "thread-1",
        user_id="user-1",
        accepted_invocation=accepted,
        multitask_strategy="interrupt",
    )
    from deerflow.runtime.skill_projection import (
        SkillProjectionEvidence,
        get_skill_projection_coordinator,
    )

    coordinator = get_skill_projection_coordinator()
    reservation = coordinator.reserve_admission(
        user_id="user-1",
        thread_id="thread-1",
        reservation_id="old:superseded-worker",
        snapshot_id=None,
        evidence=SkillProjectionEvidence.from_snapshot(None),
    )
    coordinator.promote_admission(reservation, run_id=old.run_id)
    old_token = coordinator.activate(
        user_id="user-1",
        thread_id="thread-1",
        sandbox_id="sandbox:superseded-worker",
        run_id=old.run_id,
        snapshot_id=None,
        consumer_id=f"run:{old.run_id}:lead",
    )
    factory_called = asyncio.Event()

    def factory(**_kwargs):
        factory_called.set()
        raise AssertionError("superseded worker must not build a graph")

    bridge = SimpleNamespace(
        publish=AsyncMock(),
        publish_end=AsyncMock(),
        cleanup=AsyncMock(),
    )
    worker = asyncio.create_task(
        run_agent(
            bridge,
            manager,
            waiting,
            ctx=RunContext(checkpointer=None, tenant=_TEST_TENANT),
            agent_factory=factory,
            graph_input={},
            config={"context": {"user_id": "user-1"}},
        )
    )
    waiting.task = worker
    try:
        await asyncio.sleep(0.1)
        assert not factory_called.is_set()

        later = await manager.create_or_reject(
            "thread-1",
            user_id="user-1",
            accepted_invocation=accepted,
            multitask_strategy="interrupt",
        )
        await asyncio.wait_for(worker, timeout=2)

        assert waiting.status is RunStatus.interrupted
        assert not factory_called.is_set()
        assert (
            coordinator.current_token(
                user_id="user-1",
                thread_id="thread-1",
            )
            == old_token
        )
        assert (
            coordinator.token_for_consumer(
                user_id="user-1",
                thread_id="thread-1",
                run_id=waiting.run_id,
                consumer_id=f"run:{waiting.run_id}:lead",
            )
            is None
        )
        assert later.status is RunStatus.pending
    finally:
        if not worker.done():
            worker.cancel()
            await worker
        clear = coordinator.release(old_token)
        if clear is not None:
            coordinator.finalize_release(clear)


@pytest.mark.asyncio
async def test_terminal_worker_clears_explicit_empty_view_before_later_binding(
    monkeypatch,
    tmp_path: Path,
    snapshot_paths: Paths,
) -> None:
    import deerflow.config as config_module
    from deerflow.config import paths as paths_module
    from deerflow.runtime import agent_revision as revision_module
    from deerflow.sandbox.local.local_sandbox_provider import LocalSandboxProvider
    from deerflow.sandbox.sandbox_provider import (
        AcceptedSkillSandboxBindingV1,
        reset_sandbox_provider,
        set_sandbox_provider,
    )

    projection = SimpleNamespace(
        public=tmp_path / "view" / "public",
        custom=tmp_path / "view" / "custom",
        legacy=tmp_path / "view" / "legacy",
        integrations=tmp_path / "view" / "integrations",
    )
    for path in vars(projection).values():
        path.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(paths_module, "get_paths", lambda: snapshot_paths)
    monkeypatch.setattr(
        config_module,
        "get_app_config",
        lambda: SimpleNamespace(
            skills=SimpleNamespace(container_path="/mnt/skills"),
            sandbox=SimpleNamespace(mounts=[]),
        ),
    )
    monkeypatch.setattr(
        LocalSandboxProvider,
        "_ensure_skills_projection",
        staticmethod(lambda *_args, **_kwargs: projection),
    )
    monkeypatch.setattr(
        revision_module,
        "_skills",
        lambda _app_config, *, user_id: ((), ()),
    )
    monkeypatch.setattr(
        revision_module,
        "load_agent_soul",
        lambda *_args, **_kwargs: "",
    )
    revision = resolve_agent_revision(
        {"configurable": {}},
        app_config=AppConfig(sandbox=SandboxConfig(use="test")),
        user_id="user-1",
    )
    assert revision.material is not None
    assert revision.material.skill_snapshot is None

    provider = LocalSandboxProvider()
    set_sandbox_provider(provider)
    manager = RunManager(tenant=_TEST_TENANT)
    record = await manager.create_or_reject(
        "thread-worker",
        user_id="user-1",
        accepted_invocation=_accepted(revision),
    )
    from deerflow.runtime.skill_projection import (
        SkillProjectionEvidence,
        get_skill_projection_coordinator,
    )

    coordinator = get_skill_projection_coordinator()
    coordinator.claim_committed_run(
        user_id="user-1",
        thread_id="thread-worker",
        run_id=record.run_id,
        snapshot_id=None,
        evidence=SkillProjectionEvidence.from_snapshot(None),
    )
    sandbox_id = provider.acquire_accepted_skills(
        "thread-worker",
        user_id="user-1",
    )
    token = coordinator.activate(
        user_id="user-1",
        thread_id="thread-worker",
        sandbox_id=sandbox_id,
        run_id=record.run_id,
        snapshot_id=None,
        consumer_id=f"run:{record.run_id}:lead",
    )
    provider.bind_accepted_skill_snapshot(
        sandbox_id,
        thread_id="thread-worker",
        user_id="user-1",
        binding=AcceptedSkillSandboxBindingV1.from_consumer_token(token),
    )

    class _Agent:
        async def astream(self, *_args, **_kwargs):
            yield {"messages": []}

    bridge = SimpleNamespace(
        publish=AsyncMock(),
        publish_end=AsyncMock(),
        cleanup=AsyncMock(),
    )
    try:
        await run_agent(
            bridge,
            manager,
            record,
            ctx=RunContext(checkpointer=None, tenant=_TEST_TENANT),
            agent_factory=lambda **_kwargs: _Agent(),
            graph_input={},
            config={"context": {"user_id": "user-1"}},
        )

        later_file = _write_skill(
            tmp_path / "later",
            body="LATER ACCEPTED RESOURCE",
            name="later-skill",
        )
        later = snapshot_effective_skills(
            (_parsed_skill(later_file),),
            user_id="user-1",
        )
        assert later is not None
        provider.bind_accepted_skill_snapshot(
            provider.acquire_accepted_skills(
                "thread-worker",
                user_id="user-1",
            ),
            thread_id="thread-worker",
            user_id="user-1",
            binding=AcceptedSkillSandboxBindingV1(
                snapshot_id=later.snapshot_id,
                evidence=SkillProjectionEvidence.from_snapshot(later),
            ),
        )
        sandbox = provider.get(sandbox_id)
        assert sandbox is not None
        assert "LATER ACCEPTED RESOURCE" in sandbox.read_file(f"/mnt/skills/.accepted/{later.snapshot_id}/custom/later-skill/SKILL.md")
        later.release()
    finally:
        reset_sandbox_provider()


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
    manager = RunManager(tenant=_TEST_TENANT)
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
        ctx=RunContext(checkpointer=None, tenant=_TEST_TENANT),
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
    manager = RunManager(tenant=_TEST_TENANT)
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
        ctx=RunContext(checkpointer=None, tenant=_TEST_TENANT),
        agent_factory=factory,
        graph_input={},
        config={},
    )

    assert factory_calls == 0
    assert record.status is RunStatus.error
    assert record.stop_reason == "agent_revision_drift"
    assert not material.skill_snapshot.root.exists()


@pytest.mark.asyncio
async def test_remote_materialization_failure_precedes_running_and_graph(
    monkeypatch,
    tmp_path: Path,
    snapshot_paths: Paths,
) -> None:
    skill_file = _write_skill(tmp_path, body="Pinned remote material")
    revision = _resolve_revision(monkeypatch, _parsed_skill(skill_file))
    store = MemoryRunStore()
    manager = RunManager(store=store, tenant=_TEST_TENANT)
    record = await manager.create_or_reject(
        "thread-worker",
        accepted_invocation=_accepted(revision),
    )
    factory_calls = 0

    async def fail_materialization(*_args, **_kwargs):
        raise RuntimeError("accepted_skill_snapshot_materialization_failed")

    monkeypatch.setattr(
        "deerflow.runtime.runs.worker._materialize_accepted_skill_projection",
        fail_materialization,
    )

    def factory(*, config):
        del config
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
        ctx=RunContext(checkpointer=None, tenant=_TEST_TENANT),
        agent_factory=factory,
        graph_input={},
        config={},
    )

    assert factory_calls == 0
    assert record.status is RunStatus.error
    events = await store.list_lifecycle_events(run_id=record.run_id)
    assert [event["lifecycle_type"] for event in events] == ["accepted", "failed"]


@pytest.mark.asyncio
async def test_lead_policy_denial_happens_before_provider_resolution(
    monkeypatch,
    tmp_path: Path,
    snapshot_paths: Paths,
) -> None:
    from deerflow.runtime.runs.worker import (
        _materialize_accepted_skill_projection,
    )
    from deerflow.runtime.skill_projection import (
        get_skill_projection_coordinator,
    )
    from deerflow.sandbox.exceptions import SandboxAuthorizationError
    from deerflow.sandbox.sandbox_provider import (
        AcceptedSkillSandboxBindingError,
    )
    from deerflow.subagents.batch_acceptance import (
        PARENT_BATCH_ACCEPTANCE_CONTEXT_KEY,
    )

    skill_file = _write_skill(tmp_path, body="Policy-denied material")
    revision = _resolve_revision(monkeypatch, _parsed_skill(skill_file))
    material = revision.material
    assert material is not None and material.skill_snapshot is not None
    accepted = _accepted(revision)
    runtime = SimpleNamespace(
        context={
            "thread_id": "thread-1",
            "run_id": "run-policy-denied",
            "app_config": AppConfig(sandbox=SandboxConfig(use="test")),
            RESOLVED_AGENT_MATERIAL_CONTEXT_KEY: material,
            TENANT_REFERENCE_CONTEXT_KEY: _TEST_TENANT,
            PARENT_BATCH_ACCEPTANCE_CONTEXT_KEY: accepted,
            "accepted_agent_revision_digest": revision.digest,
        },
    )
    provider_resolutions = 0
    claim_validations = 0

    def resolve_provider():
        nonlocal provider_resolutions
        provider_resolutions += 1
        return object()

    async def deny(**_kwargs):
        raise SandboxAuthorizationError("denied")

    async def validate_claim(_claim):
        nonlocal claim_validations
        claim_validations += 1
        return True

    monkeypatch.setattr("deerflow.sandbox.get_sandbox_provider", resolve_provider)
    monkeypatch.setattr(
        "deerflow.authz.sandbox_authz.authorize_sandbox_execution_async",
        deny,
    )

    try:
        with pytest.raises(
            AcceptedSkillSandboxBindingError,
            match="accepted_skill_snapshot_materialization_failed",
        ):
            await _materialize_accepted_skill_projection(
                runtime,
                user_id="user-1",
                record=SimpleNamespace(
                    owner_worker_id="worker-pending",
                    state_version=3,
                    execution_takeover=False,
                    execution_evidence_json=None,
                    recovery_policy=None,
                ),
                claim_validator=validate_claim,
            )
    finally:
        get_skill_projection_coordinator().release_unactivated_run(
            user_id="user-1",
            thread_id="thread-1",
            run_id="run-policy-denied",
        )
        material.release_process_material()

    assert provider_resolutions == 0
    assert claim_validations == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "cancel_after_acquire",
    [False, True],
    ids=["success", "cancelled-post-acquire-fence"],
)
async def test_qualified_aio_worker_materialization_uses_neutral_evidence(
    monkeypatch,
    tmp_path: Path,
    snapshot_paths: Paths,
    cancel_after_acquire: bool,
) -> None:
    from deerflow.community.aio_sandbox.aio_sandbox_provider import (
        AioSandboxProvider,
    )
    from deerflow.runtime.runs.worker import (
        _materialize_accepted_skill_projection,
    )
    from deerflow.runtime.skill_projection import (
        SKILL_PROJECTION_TOKEN_CONTEXT_KEY,
        get_skill_projection_coordinator,
    )
    from deerflow.subagents.batch_acceptance import (
        PARENT_BATCH_ACCEPTANCE_CONTEXT_KEY,
    )
    from deerflow.tool_plane.contracts import EffectiveToolPlaneRevisionV1

    skill_file = _write_skill(tmp_path, body="Neutral adapter worker path")
    revision = _resolve_revision(monkeypatch, _parsed_skill(skill_file))
    material = revision.material
    assert material is not None and material.skill_snapshot is not None
    tool_plane = EffectiveToolPlaneRevisionV1(
        base_revision_digest="1" * 64,
        user_overlay_digest="2" * 64,
        base_generation=1,
        overlay_generation=2,
        projection_digest="3" * 64,
    )
    accepted = _accepted(revision, tool_plane_revision=tool_plane.to_json())

    fence_events: list[str] = []
    sql_fence_held = False

    class QualifiedAioProvider(AioSandboxProvider):
        def __init__(self) -> None:
            self.sandbox = SimpleNamespace(id="sandbox-neutral")
            self.evidence = None
            self.acquired_scope = None
            self.execution_claim = None
            self.destroyed = []
            self._config = {
                "accepted_material_lease_duration_seconds": 300,
            }

        def provider_neutral_accepted_materialization_enabled(self) -> bool:
            return True

        async def accepted_material_runtime_image_digest_async(self) -> str:
            return "5" * 64

        async def _accepted_sandbox_qualification(
            self,
            *,
            profile: AcceptedSandboxCapabilityProfileV1,
            runtime_image_digest: str,
        ) -> AcceptedSandboxQualificationV1:
            assert runtime_image_digest == "5" * 64
            now = datetime.now(UTC)
            return AcceptedSandboxQualificationV1.build(
                capability_profile_digest=profile.digest,
                qualification_scope="contract_test_only",
                artifact_digest="d" * 64,
                topology_digest="e" * 64,
                verified_at=now - timedelta(minutes=1),
                expires_at=now + timedelta(days=1),
            )

        async def acquire_bound_accepted_skills_async(
            self,
            thread_id,
            *,
            user_id,
            binding,
            execution_claim=None,
        ) -> str:
            assert sql_fence_held is False
            fence_events.append("provider_acquire")
            self.acquired_scope = (thread_id, user_id)
            self.execution_claim = execution_claim
            wire = {
                "profile": "rwx_verified_copy_v2",
                "attempt_id": "provider-attempt",
                "snapshot_id": binding.snapshot_id,
                "run_id": binding.run_id,
                "generation": binding.generation,
                "pod_uid": "pod-uid",
                "pod_isolation_digest": "6" * 64,
                "lease_uid": "lease-uid",
                "network_policy_uid": "network-policy-uid",
                "network_policy_spec_digest": "7" * 64,
                "evidence_secret_uid": "evidence-secret-uid",
                "evidence_secret_digest": "8" * 64,
                "capability_secret_uid": "capability-secret-uid",
                "capability_secret_digest": "9" * 64,
                "sandbox_image_digest": "5" * 64,
                "accepted_skill_runtime_image_digest": "a" * 64,
                "runtime_image_ids_digest": "b" * 64,
                "verifier_receipt_digest": "c" * 64,
            }
            materialization_wire = {
                "version": 2,
                **wire,
                "content_digest": binding.snapshot_id,
            }
            wire["materialization_evidence_digest"] = hashlib.sha256(
                json.dumps(
                    materialization_wire,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode(),
            ).hexdigest()
            self.evidence = AcceptedSkillExecutionEvidenceV2(**wire)
            return self.sandbox.id

        def get(self, sandbox_id):
            return self.sandbox if sandbox_id == self.sandbox.id else None

        def accepted_skill_execution_evidence(self, sandbox_id):
            return self.evidence if sandbox_id == self.sandbox.id else None

        def has_accepted_skill_isolation(self, sandbox_id):
            return sandbox_id == self.sandbox.id

        def accepted_skill_material_capability(self, sandbox_id):
            assert sandbox_id == self.sandbox.id
            return AcceptedMaterialCapability.IMMUTABLE_READ_ONLY

        async def bind_accepted_skill_snapshot_async(self, *_args, **_kwargs):
            return None

        async def validate_accepted_skill_execution_async(
            self,
            sandbox_id,
            evidence,
        ):
            return sandbox_id == self.sandbox.id and evidence == self.evidence

        async def renew_accepted_skill_execution_async(
            self,
            sandbox_id,
            evidence,
        ):
            return sandbox_id == self.sandbox.id and evidence == self.evidence

        def destroy(self, sandbox_id):
            self.destroyed.append(sandbox_id)

    provider = QualifiedAioProvider()
    monkeypatch.setattr("deerflow.sandbox.get_sandbox_provider", lambda: provider)
    runtime = SimpleNamespace(
        context={
            "thread_id": "thread-1",
            "run_id": "run-neutral",
            RESOLVED_AGENT_MATERIAL_CONTEXT_KEY: material,
            TENANT_REFERENCE_CONTEXT_KEY: _TEST_TENANT,
            PARENT_BATCH_ACCEPTANCE_CONTEXT_KEY: accepted,
            "accepted_agent_revision_digest": revision.digest,
        },
    )

    claim_validations = 0
    cancellation = asyncio.CancelledError("cancelled after provider acquire")

    async def validate_claim(_claim) -> bool:
        nonlocal claim_validations
        nonlocal sql_fence_held
        claim_validations += 1
        assert sql_fence_held is False
        sql_fence_held = True
        fence_events.append("sql_fence_enter")
        await asyncio.sleep(0)
        fence_events.append("sql_fence_exit")
        sql_fence_held = False
        if cancel_after_acquire and claim_validations == 2:
            raise cancellation
        return True

    result = None
    try:
        if cancel_after_acquire:
            with pytest.raises(asyncio.CancelledError) as caught:
                await _materialize_accepted_skill_projection(
                    runtime,
                    user_id="user-1",
                    record=SimpleNamespace(
                        owner_worker_id="worker-pending",
                        state_version=3,
                        execution_takeover=False,
                        execution_evidence_json=None,
                        recovery_policy=None,
                    ),
                    claim_validator=validate_claim,
                )
            assert caught.value is cancellation
            assert provider.destroyed == ["sandbox-neutral"]
            return
        result = await _materialize_accepted_skill_projection(
            runtime,
            user_id="user-1",
            record=SimpleNamespace(
                owner_worker_id="worker-pending",
                state_version=3,
                execution_takeover=False,
                execution_evidence_json=None,
                recovery_policy=None,
            ),
            claim_validator=validate_claim,
        )
        assert provider.acquired_scope == ("thread-1", "user-1")
        assert provider.execution_claim is not None
        assert provider.execution_claim.owner_worker_id == "worker-pending"
        assert provider.execution_claim.state_version == 3
        assert fence_events == [
            "sql_fence_enter",
            "sql_fence_exit",
            "provider_acquire",
            "sql_fence_enter",
            "sql_fence_exit",
        ]
        assert isinstance(result.request, AcceptedMaterialRequestV2)
        assert isinstance(result.evidence, AcceptedExecutionEvidenceV2)
        assert result.evidence.accepted_invocation_digest == (accepted.runtime_identity_digest)
        assert result.evidence.tool_plane_effective_digest == (tool_plane.effective_digest)
        assert "sandbox-neutral" not in json.dumps(
            result.evidence.to_persisted(),
            sort_keys=True,
        )
        assert result.evidence.attempt_id != provider.evidence.attempt_id
        assert await result.validate()
    finally:
        if result is not None:
            await result.release()
        token = runtime.context.get(SKILL_PROJECTION_TOKEN_CONTEXT_KEY)
        if token is not None:
            coordinator = get_skill_projection_coordinator()
            clear = coordinator.release(token)
            if clear is not None:
                coordinator.finalize_release(clear)
        material.release_process_material()

    assert provider.destroyed == ["sandbox-neutral"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    (
        "cancel_lifecycle_publication",
        "lose_authority_before_terminal_commit",
        "cancel_terminal_validation",
    ),
    [
        (None, False, False),
        ("pre_close", False, False),
        ("post_close", False, False),
        (None, True, False),
        (None, False, True),
    ],
    ids=[
        "normal",
        "pre-close-publication-cancelled",
        "post-close-publication-cancelled",
        "late-authority-loss",
        "terminal-validation-cancelled",
    ],
)
async def test_durable_worker_installs_running_claim_sandbox_session(
    monkeypatch,
    tmp_path: Path,
    snapshot_paths: Paths,
    cancel_lifecycle_publication: str | None,
    lose_authority_before_terminal_commit: bool,
    cancel_terminal_validation: bool,
) -> None:
    skill_file = _write_skill(tmp_path, body="Session-gated material")
    revision = _resolve_revision(monkeypatch, _parsed_skill(skill_file))
    material = revision.material
    assert material is not None and material.skill_snapshot is not None
    revision = ResolvedAgentRevision.from_material(
        replace(
            material,
            model_profile={**material.model_profile, "name": "default"},
        )
    )
    accepted = _accepted(revision)
    store = MemoryRunStore()
    manager = RunManager(
        store=store,
        tenant=_TEST_TENANT,
        run_ownership_config=RunOwnershipConfig(heartbeat_enabled=True),
    )
    record = await manager.create_or_reject(
        "thread-worker",
        accepted_invocation=accepted,
    )
    expires_at = datetime.now(UTC) + timedelta(minutes=5)
    request = AcceptedMaterialRequestV1.build(
        run_id=record.run_id,
        attempt_id="attempt-session",
        tenant=_TEST_TENANT,
        user_ref="user-ref",
        thread_ref="thread-ref",
        agent_revision_digest=revision.digest,
        skill_snapshot_digest=material.skill_snapshot.snapshot_id,
        skill_scope_digest=material.skill_scopes.digest,
        file_manifest=(),
        runtime_image_digest="5" * 64,
        lease_expires_at=expires_at,
    )
    lease = AcceptedMaterialLeaseV1(
        version=1,
        provider_kind="test",
        provider_instance_ref="private-provider-resource",
        ownership_epoch=4,
        lease_expires_at=expires_at,
        opaque_renewal_handle=object(),
    )
    evidence = AcceptedExecutionEvidenceV1.build(
        run_id=record.run_id,
        attempt_id=request.attempt_id,
        tenant=_TEST_TENANT,
        provider_kind=lease.provider_kind,
        provider_instance_ref=lease.provider_instance_ref,
        ownership_epoch=lease.ownership_epoch,
        runtime_image_digest=request.runtime_image_digest,
        skill_snapshot_digest=request.skill_snapshot_digest,
        skill_scope_digest=request.skill_scope_digest,
        materialization_digest=request.digest,
        verifier_image_digest="6" * 64,
        verifier_contract_version="test_v1",
        read_only_proof_digest="7" * 64,
        qualification_scope="contract_test_only",
    )
    terminal_validation_interrupt = asyncio.CancelledError(
        "terminal validation interrupted",
    )

    class RawSandbox(Sandbox):
        persistent_shell_sessions = False

        def __init__(self) -> None:
            super().__init__(lease.provider_instance_ref)
            self.commands: list[str] = []

        def execute_command(self, command, env=None, timeout=None):
            del env, timeout
            self.commands.append(command)
            return "gated"

        def read_file(self, path, start_line=None, end_line=None):
            raise AssertionError("unexpected read")

        def download_file(self, path):
            raise AssertionError("unexpected download")

        def list_dir(self, path, max_depth=2):
            raise AssertionError("unexpected list")

        def write_file(self, path, content, append=False):
            raise AssertionError("unexpected write")

        def glob(self, path, pattern, *, include_dirs=False, max_results=200):
            raise AssertionError("unexpected glob")

        def grep(
            self,
            path,
            pattern,
            *,
            glob=None,
            literal=False,
            case_sensitive=False,
            max_results=100,
        ):
            raise AssertionError("unexpected grep")

        def update_file(self, path, content):
            raise AssertionError("unexpected update")

    class Materializer:
        def __init__(self) -> None:
            self.validated = 0
            self.released = 0
            self.validated_tuples = []

        def capability(self):
            return AcceptedMaterialCapability.IMMUTABLE_READ_ONLY

        async def validate(self, checked_lease, checked_evidence):
            self.validated += 1
            self.validated_tuples.append((checked_lease, checked_evidence))
            if cancel_terminal_validation and record.status is RunStatus.success:
                raise terminal_validation_interrupt
            return not (lose_authority_before_terminal_commit and record.status is RunStatus.success)

        async def renew(self, checked_lease):
            return checked_lease

        async def release(self, checked_lease):
            del checked_lease
            self.released += 1

    raw_sandbox = RawSandbox()
    materializer = Materializer()
    result = _AcceptedMaterializationResult(
        sandbox_id=raw_sandbox.id,
        evidence=evidence,
        provider=None,
        materializer=materializer,
        lease=lease,
        sandbox=raw_sandbox,
        request=request,
    )
    monkeypatch.setattr(
        "deerflow.runtime.runs.worker._materialize_accepted_skill_projection",
        AsyncMock(return_value=result),
    )

    observed_facades = []
    observed_results = []
    observed_declarations = []

    class Agent:
        async def astream(self, *_args, **kwargs):
            from deerflow.sandbox.session import current_sandbox_session, declared_sandbox

            assert "__deerflow_accepted_sandbox_session_v1" not in kwargs["config"]["context"]
            facade = declared_sandbox()
            observed_facades.append(facade)
            observed_declarations.append(current_sandbox_session())
            if facade is not None:
                observed_results.append(
                    await asyncio.to_thread(
                        facade.execute_command,
                        "echo gated",
                    )
                )
            yield {"messages": []}

    bridge = SimpleNamespace(
        publish=AsyncMock(),
        publish_end=AsyncMock(),
        cleanup=AsyncMock(),
    )
    lifecycle_publication_started = asyncio.Event()
    lifecycle_publication_interrupt: BaseException | None = None
    completion_observer_entered = asyncio.Event()

    async def blocking_completion_observer(_record) -> None:
        completion_observer_entered.set()
        await asyncio.Event().wait()

    if cancel_lifecycle_publication is not None:
        from deerflow.runtime.runs import worker as worker_module
        from deerflow.sandbox.accepted_material import (
            AcceptedSandboxLifecycleKind,
        )

        publish_lifecycle = worker_module._publish_accepted_sandbox_lifecycle

        async def block_lifecycle_publication() -> None:
            nonlocal lifecycle_publication_interrupt
            lifecycle_publication_started.set()
            try:
                await asyncio.Event().wait()
            except BaseException as exc:
                lifecycle_publication_interrupt = exc
                raise

        async def interruptible_lifecycle_publication(
            event_appender,
            session,
            *,
            start_index,
        ):
            observations = session.lifecycle_observations
            before_close = start_index == len(observations)
            after_close = any(observation.kind is AcceptedSandboxLifecycleKind.RELEASED for observation in observations[start_index:])
            if cancel_lifecycle_publication == "pre_close" and before_close:
                await block_lifecycle_publication()
            if cancel_lifecycle_publication == "post_close" and after_close:
                await block_lifecycle_publication()
            return await publish_lifecycle(
                event_appender,
                session,
                start_index=start_index,
            )

        monkeypatch.setattr(
            worker_module,
            "_publish_accepted_sandbox_lifecycle",
            interruptible_lifecycle_publication,
        )
    event_store = MemoryRunEventStore(
        run_store=store,
        tenant=_TEST_TENANT,
    )
    run_task = asyncio.create_task(
        run_agent(
            bridge,
            manager,
            record,
            ctx=RunContext(
                checkpointer=None,
                event_store=event_store,
                tenant=_TEST_TENANT,
                on_run_completed=(blocking_completion_observer if cancel_lifecycle_publication == "pre_close" else None),
            ),
            agent_factory=lambda **_kwargs: _assembled_agent_for_revision(
                revision,
                Agent(),
            ),
            graph_input={},
            config={},
        )
    )
    if cancel_lifecycle_publication is not None:
        await asyncio.wait_for(lifecycle_publication_started.wait(), timeout=5)
        run_task.cancel()
        with pytest.raises(asyncio.CancelledError) as deferred_interrupt:
            await run_task
        assert isinstance(lifecycle_publication_interrupt, asyncio.CancelledError)
        assert isinstance(deferred_interrupt.value, asyncio.CancelledError)
        if cancel_lifecycle_publication == "pre_close":
            assert not completion_observer_entered.is_set()
    elif cancel_terminal_validation:
        with pytest.raises(asyncio.CancelledError):
            await run_task
    else:
        await run_task

    assert record.status is RunStatus.success, record.error
    assert len(observed_facades) == 1
    assert observed_facades[0] is not raw_sandbox
    assert raw_sandbox.id not in observed_facades[0].id
    assert observed_results == ["gated"]
    from deerflow.sandbox.session import current_sandbox_session, get_sandbox_session_registry

    assert observed_declarations[0] is not None
    assert observed_declarations[0].handle is observed_facades[0]
    assert observed_declarations[0].mount_scope == ("user-1", record.thread_id)
    assert get_sandbox_session_registry().lookup(observed_facades[0].id) is None
    assert current_sandbox_session() is None
    assert raw_sandbox.commands == ["echo gated"]
    assert materializer.validated >= 2
    assert all(pair == (lease, evidence) for pair in materializer.validated_tuples)
    assert materializer.released == 1
    persisted = await store.get(record.run_id)
    if lose_authority_before_terminal_commit or cancel_terminal_validation:
        assert record.ownership_lost is True
        assert persisted["status"] == RunStatus.running.value
        bridge.publish_end.assert_not_awaited()
    else:
        assert record.ownership_lost is False
        assert persisted["status"] == RunStatus.success.value
        bridge.publish_end.assert_awaited_once_with(record.run_id)
    bridge.cleanup.assert_called_once_with(record.run_id, delay=60)
    lifecycle_events = await event_store.list_events(
        record.thread_id,
        record.run_id,
        event_types=["sandbox.lifecycle.v1"],
    )
    assert lifecycle_events
    assert lifecycle_events[0]["content"]["kind"] == "acquired"
    assert raw_sandbox.id not in json.dumps(lifecycle_events, sort_keys=True)


@pytest.mark.asyncio
async def test_remote_materialization_replacement_fails_before_graph_construction(
    monkeypatch,
    tmp_path: Path,
    snapshot_paths: Paths,
) -> None:
    skill_file = _write_skill(tmp_path, body="Pinned remote material")
    revision = _resolve_revision(monkeypatch, _parsed_skill(skill_file))
    store = MemoryRunStore()
    manager = RunManager(
        store=store,
        tenant=_TEST_TENANT,
        run_ownership_config=RunOwnershipConfig(heartbeat_enabled=True),
    )
    record = await manager.create_or_reject(
        "thread-worker",
        accepted_invocation=_accepted(revision),
    )
    evidence = AcceptedSkillExecutionEvidenceV1(
        profile="rwx_verified_copy_v1",
        attempt_id="sandbox-attempt",
        snapshot_id=revision.material.skill_snapshot.snapshot_id,
        run_id=record.run_id,
        generation=0,
        pod_uid="pod-1",
        lease_uid="lease-1",
        runtime_image_ids_digest="a" * 64,
        verifier_receipt_digest="b" * 64,
        materialization_evidence_digest="c" * 64,
    )
    provider = SimpleNamespace(
        validate_accepted_skill_execution_async=AsyncMock(return_value=False),
        renew_accepted_skill_execution_async=AsyncMock(return_value=True),
    )
    monkeypatch.setattr(
        "deerflow.runtime.runs.worker._materialize_accepted_skill_projection",
        AsyncMock(return_value=("sandbox-1", evidence)),
    )
    monkeypatch.setattr("deerflow.sandbox.get_sandbox_provider", lambda: provider)
    factory = AsyncMock(side_effect=AssertionError("graph construction must not run"))
    bridge = SimpleNamespace(
        publish=AsyncMock(),
        publish_end=AsyncMock(),
        cleanup=AsyncMock(),
    )

    await run_agent(
        bridge,
        manager,
        record,
        ctx=RunContext(checkpointer=None, tenant=_TEST_TENANT),
        agent_factory=factory,
        graph_input={},
        config={},
    )

    factory.assert_not_called()
    assert record.status is RunStatus.error
    assert record.stop_reason == "accepted_skill_execution_fence_failed"


@pytest.mark.asyncio
async def test_remote_materialization_is_refenced_immediately_before_astream(
    monkeypatch,
    tmp_path: Path,
    snapshot_paths: Paths,
) -> None:
    skill_file = _write_skill(tmp_path, body="Pinned remote material")
    revision = _resolve_revision(monkeypatch, _parsed_skill(skill_file))
    assert revision.material is not None
    revision = ResolvedAgentRevision.from_material(
        replace(
            revision.material,
            model_profile={**revision.material.model_profile, "name": "default"},
        )
    )
    store = MemoryRunStore()
    manager = RunManager(
        store=store,
        tenant=_TEST_TENANT,
        run_ownership_config=RunOwnershipConfig(heartbeat_enabled=True),
    )
    record = await manager.create_or_reject(
        "thread-worker",
        accepted_invocation=_accepted(revision),
    )
    evidence = AcceptedSkillExecutionEvidenceV1(
        profile="rwx_verified_copy_v1",
        attempt_id="sandbox-attempt",
        snapshot_id=revision.material.skill_snapshot.snapshot_id,
        run_id=record.run_id,
        generation=0,
        pod_uid="pod-1",
        lease_uid="lease-1",
        runtime_image_ids_digest="a" * 64,
        verifier_receipt_digest="b" * 64,
        materialization_evidence_digest="c" * 64,
    )
    attempt_valid = True

    async def validate_attempt(*_args, **_kwargs) -> bool:
        return attempt_valid

    provider = SimpleNamespace(
        validate_accepted_skill_execution_async=AsyncMock(
            side_effect=validate_attempt,
        ),
        renew_accepted_skill_execution_async=AsyncMock(return_value=True),
    )
    monkeypatch.setattr(
        "deerflow.runtime.runs.worker._materialize_accepted_skill_projection",
        AsyncMock(return_value=("sandbox-1", evidence)),
    )
    monkeypatch.setattr("deerflow.sandbox.get_sandbox_provider", lambda: provider)

    async def qualification_barrier(counter_name, *_args, **_kwargs) -> None:
        nonlocal attempt_valid
        if counter_name == "checkpoint_preflight_starts":
            attempt_valid = False

    monkeypatch.setattr(
        "deerflow.runtime.kubernetes_qualification.qualification_counter",
        qualification_barrier,
    )
    astream_calls = 0

    class Agent:
        async def astream(self, *_args, **_kwargs):
            nonlocal astream_calls
            astream_calls += 1
            yield {"messages": []}

    bridge = SimpleNamespace(
        publish=AsyncMock(),
        publish_end=AsyncMock(),
        cleanup=AsyncMock(),
    )

    await run_agent(
        bridge,
        manager,
        record,
        ctx=RunContext(
            checkpointer=None,
            event_store=MemoryRunEventStore(
                run_store=store,
                tenant=_TEST_TENANT,
            ),
            tenant=_TEST_TENANT,
        ),
        agent_factory=lambda **_kwargs: _assembled_agent_for_revision(revision, Agent()),
        graph_input={},
        config={},
    )

    assert astream_calls == 0
    assert provider.validate_accepted_skill_execution_async.await_count == 2
    assert record.status is RunStatus.error
    assert record.stop_reason == "accepted_skill_execution_fence_failed"


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
