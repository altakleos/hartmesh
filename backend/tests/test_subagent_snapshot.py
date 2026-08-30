"""Immutable managed-subagent catalog behavior at durable admission."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from deerflow.config.agents_config import AgentConfig
from deerflow.config.app_config import AppConfig
from deerflow.config.sandbox_config import SandboxConfig
from deerflow.config.subagents_config import (
    CustomSubagentConfig,
    SubagentsAppConfig,
)
from deerflow.persistence.managed_subagents import ManagedSubagentDefinition
from deerflow.runtime.subagent_snapshot import (
    MAX_SUBAGENT_PROMPT_BYTES,
    ResolvedSkillScopesV1,
    ResolvedSubagentCatalogV1,
    SubagentCatalogError,
    assert_subagent_projection_complete,
    resolved_subagent_definition,
    snapshot_effective_subagents,
)
from deerflow.skills.parser import parse_skill_file
from deerflow.skills.types import SkillCategory
from deerflow.subagents import registry


def _definition(name: str, *, prompt: str):
    return resolved_subagent_definition(
        name=name,
        source_kind="managed",
        source_version=f"source-{name}",
        description=f"Delegate {name} work",
        system_prompt=prompt,
        model=None,
        model_settings={},
        tool_names=("read_file",),
        skill_names=(),
        max_turns=12,
        timeout_seconds=30,
    )


def test_catalog_digest_and_persistence_are_independent_of_resolution_order() -> None:
    alpha = _definition("alpha", prompt="Accepted alpha prompt")
    beta = _definition("beta", prompt="Accepted beta prompt")

    first = ResolvedSubagentCatalogV1.from_entries(
        (beta, alpha),
        allowed_names=("beta", "alpha"),
    )
    second = ResolvedSubagentCatalogV1.from_entries(
        (alpha, beta),
        allowed_names=("alpha", "beta"),
    )

    assert first == second
    assert first.allowed_names == ("alpha", "beta")
    assert tuple(entry.name for entry in first.entries) == ("alpha", "beta")
    assert ResolvedSubagentCatalogV1.from_persisted_json(first.to_persisted_json()) == first


def _app_config() -> AppConfig:
    return AppConfig(sandbox=SandboxConfig(use="test"))


def test_snapshot_resolves_managed_definition_once_and_survives_live_edit(monkeypatch) -> None:
    live = [
        ManagedSubagentDefinition(
            name="planner",
            description="Plan bounded work",
            system_prompt="Accepted planner prompt",
            tools=["read_file"],
            skills=[],
            max_turns=9,
            timeout_seconds=45,
        )
    ]
    monkeypatch.setattr(registry, "_managed_definitions", lambda **_: tuple(live))

    catalog = snapshot_effective_subagents(
        app_config=_app_config(),
        agent_config=AgentConfig(name="lead", allowed_subagents=["planner"]),
        user_id="user-1",
        is_bootstrap=False,
    )
    live[0] = live[0].model_copy(update={"system_prompt": "Edited after acceptance"})

    frozen = catalog.get("planner")
    assert frozen is not None
    assert frozen.source_kind == "managed"
    assert frozen.system_prompt == "Accepted planner prompt"
    assert frozen.tool_names == ("read_file",)
    assert frozen.skill_names == ()

    next_catalog = snapshot_effective_subagents(
        app_config=_app_config(),
        agent_config=AgentConfig(name="lead", allowed_subagents=["planner"]),
        user_id="user-1",
        is_bootstrap=False,
        available_skill_names=(),
    )
    assert next_catalog.digest != catalog.digest
    assert next_catalog.get("planner").system_prompt == "Edited after acceptance"


def test_inherited_tools_are_expanded_to_an_immutable_allowlist(
    monkeypatch,
) -> None:
    managed = ManagedSubagentDefinition(
        name="planner",
        description="Plan bounded work",
        system_prompt="Accepted planner prompt",
        tools=None,
        skills=[],
    )
    monkeypatch.setattr(registry, "_managed_definitions", lambda **_: (managed,))
    live_tools = [SimpleNamespace(name="read_file")]
    monkeypatch.setattr(
        "deerflow.tools.get_available_tools",
        lambda **_kwargs: list(live_tools),
    )

    catalog = snapshot_effective_subagents(
        app_config=_app_config(),
        agent_config=AgentConfig(name="lead", allowed_subagents=["planner"]),
        user_id="user-1",
        is_bootstrap=False,
        available_skill_names=(),
    )
    accepted = catalog.get("planner")
    assert accepted is not None
    assert accepted.inherits_tools is True
    assert accepted.tool_names == ("read_file",)
    assert accepted.to_subagent_config().tools == ["read_file"]

    # A provider/config edit can make another tool live, but reconstructing the
    # accepted executor config keeps the original authorization ceiling.
    live_tools.append(SimpleNamespace(name="write_file"))
    assert accepted.to_subagent_config().tools == ["read_file"]


def test_admission_bypasses_registry_ttl_and_reads_managed_store_once(
    monkeypatch,
) -> None:
    live = [
        ManagedSubagentDefinition(
            name="planner",
            description="Plan bounded work",
            system_prompt="Before edit",
            skills=[],
        )
    ]

    class Store:
        list_calls = 0

        def cache_identity(self):
            return ("admission-coherence-test", id(self))

        def signature(self):
            return "stable-during-ttl"

        def list(self):
            self.list_calls += 1
            return list(live)

    store = Store()
    monkeypatch.setattr(
        registry,
        "get_managed_subagent_store",
        lambda _config: store,
    )
    registry._clear_managed_definitions_cache()
    try:
        # Prime the ordinary one-second registry cache, then edit without
        # advancing its monotonic clock or signature.
        assert registry._managed_definitions(app_config=_app_config())[0].system_prompt == "Before edit"
        live[0] = live[0].model_copy(update={"system_prompt": "After edit"})

        catalog = snapshot_effective_subagents(
            app_config=_app_config(),
            agent_config=AgentConfig(
                name="lead",
                allowed_subagents=["planner"],
            ),
            user_id="user-1",
            is_bootstrap=False,
            available_skill_names=(),
        )

        assert catalog.get("planner").system_prompt == "After edit"
        assert store.list_calls == 2
    finally:
        registry._clear_managed_definitions_cache()


def test_descriptive_managed_display_name_does_not_change_execution_digest(
    monkeypatch,
) -> None:
    live = [
        ManagedSubagentDefinition(
            name="planner",
            display_name="Planning specialist",
            description="Plan bounded work",
            system_prompt="Accepted planner prompt",
            skills=[],
        )
    ]
    monkeypatch.setattr(registry, "_managed_definitions", lambda **_: tuple(live))
    app_config = _app_config()
    agent_config = AgentConfig(name="lead", allowed_subagents=["planner"])

    before = snapshot_effective_subagents(
        app_config=app_config,
        agent_config=agent_config,
        user_id="user-1",
        is_bootstrap=False,
        available_skill_names=(),
    )
    live[0] = live[0].model_copy(update={"display_name": "New display label"})
    after = snapshot_effective_subagents(
        app_config=app_config,
        agent_config=agent_config,
        user_id="user-1",
        is_bootstrap=False,
        available_skill_names=(),
    )

    assert after == before


def test_snapshot_preserves_registry_precedence(monkeypatch) -> None:
    managed = (
        ManagedSubagentDefinition(
            name="general-purpose",
            description="Managed conflict",
            system_prompt="Managed must lose to the builtin",
            skills=[],
        ),
        ManagedSubagentDefinition(
            name="planner",
            description="Managed conflict",
            system_prompt="Managed must lose to config",
            skills=[],
        ),
        ManagedSubagentDefinition(
            name="worker",
            description="Managed winner",
            system_prompt="Managed worker prompt",
            skills=[],
        ),
    )
    monkeypatch.setattr(registry, "_managed_definitions", lambda **_: managed)
    app_config = AppConfig(
        sandbox=SandboxConfig(use="test"),
        subagents=SubagentsAppConfig(
            custom_agents={
                "general-purpose": CustomSubagentConfig(
                    description="Config conflict",
                    system_prompt="Config must lose to the builtin",
                    skills=[],
                ),
                "planner": CustomSubagentConfig(
                    description="Config winner",
                    system_prompt="Configured planner prompt",
                    skills=[],
                ),
            }
        ),
    )

    catalog = snapshot_effective_subagents(
        app_config=app_config,
        agent_config=AgentConfig(
            name="lead",
            allowed_subagents=["general-purpose", "planner", "worker"],
        ),
        user_id="user-1",
        is_bootstrap=False,
        available_skill_names=(),
    )

    assert {entry.name: (entry.source_kind, entry.system_prompt) for entry in catalog.entries} == {
        "general-purpose": (
            "builtin",
            registry.BUILTIN_SUBAGENTS["general-purpose"].system_prompt,
        ),
        "planner": ("config", "Configured planner prompt"),
        "worker": ("managed", "Managed worker prompt"),
    }


def test_file_and_sql_managed_stores_produce_identical_catalogs(
    monkeypatch,
    tmp_path: Path,
) -> None:
    from sqlalchemy import create_engine

    from deerflow.persistence.base import Base
    from deerflow.persistence.managed_subagents.file import (
        FileManagedSubagentStore,
    )
    from deerflow.persistence.managed_subagents.model import ManagedSubagentRow
    from deerflow.persistence.managed_subagents.sql import SqlManagedSubagentStore

    definition = ManagedSubagentDefinition(
        name="planner",
        description="Plan bounded work",
        system_prompt="Accepted planner prompt",
        tools=["read_file"],
        skills=[],
        max_turns=9,
        timeout_seconds=45,
    )
    monkeypatch.setenv("DEER_FLOW_HOME", str(tmp_path / "file-home"))
    monkeypatch.setattr("deerflow.config.paths._paths", None)
    file_store = FileManagedSubagentStore()
    file_store.create(definition)

    sql_url = f"sqlite:///{tmp_path / 'managed.db'}"
    engine = create_engine(sql_url)
    Base.metadata.create_all(engine, tables=[ManagedSubagentRow.__table__])
    engine.dispose()
    sql_store = SqlManagedSubagentStore(sql_url)
    sql_store.create(definition)

    app_config = _app_config()
    agent_config = AgentConfig(name="lead", allowed_subagents=["planner"])
    monkeypatch.setattr(
        registry,
        "_managed_definitions",
        lambda **_: tuple(file_store.list()),
    )
    from_file = snapshot_effective_subagents(
        app_config=app_config,
        agent_config=agent_config,
        user_id="user-1",
        is_bootstrap=False,
        available_skill_names=(),
    )
    monkeypatch.setattr(
        registry,
        "_managed_definitions",
        lambda **_: tuple(sql_store.list()),
    )
    from_sql = snapshot_effective_subagents(
        app_config=app_config,
        agent_config=agent_config,
        user_id="user-1",
        is_bootstrap=False,
        available_skill_names=(),
    )

    assert from_sql == from_file


def test_named_missing_definition_fails_admission_with_safe_code(monkeypatch) -> None:
    monkeypatch.setattr(registry, "_managed_definitions", lambda **_: ())

    with pytest.raises(SubagentCatalogError) as raised:
        snapshot_effective_subagents(
            app_config=_app_config(),
            agent_config=AgentConfig(name="lead", allowed_subagents=["missing"]),
            user_id="user-1",
            is_bootstrap=False,
        )

    assert raised.value.code == "subagent_definition_missing"


def test_named_but_sandbox_unavailable_definition_is_intersected_not_missing(
    monkeypatch,
) -> None:
    monkeypatch.setattr(registry, "is_host_bash_allowed", lambda *_args, **_kwargs: False)

    catalog = snapshot_effective_subagents(
        app_config=_app_config(),
        agent_config=AgentConfig(name="lead", allowed_subagents=["bash"]),
        user_id="user-1",
        is_bootstrap=False,
        available_skill_names=(),
    )

    assert catalog == ResolvedSubagentCatalogV1.empty()


def test_operator_catalog_cap_can_only_reduce_the_hard_limit() -> None:
    assert SubagentsAppConfig(max_catalog_entries=8).max_catalog_entries == 8
    with pytest.raises(ValueError):
        SubagentsAppConfig(max_catalog_entries=65)


def test_snapshot_field_classification_covers_every_live_model() -> None:
    assert_subagent_projection_complete()


def test_explicit_empty_allowlist_is_a_canonical_empty_catalog() -> None:
    catalog = snapshot_effective_subagents(
        app_config=_app_config(),
        agent_config=AgentConfig(name="lead", allowed_subagents=[]),
        user_id="user-1",
        is_bootstrap=False,
    )

    assert catalog == ResolvedSubagentCatalogV1.empty()


def test_catalog_rejects_duplicate_entries_bad_digest_and_oversize_prompt() -> None:
    planner = _definition("planner", prompt="Accepted prompt")
    with pytest.raises(SubagentCatalogError):
        ResolvedSubagentCatalogV1.from_entries(
            (planner, planner),
            allowed_names=("planner",),
        )

    persisted = planner.to_persisted_json()
    persisted["definition_digest"] = "0" * 64
    with pytest.raises(SubagentCatalogError):
        type(planner).from_persisted_json(persisted)

    huge = _definition("huge", prompt="x" * MAX_SUBAGENT_PROMPT_BYTES)
    with pytest.raises(SubagentCatalogError):
        ResolvedSubagentCatalogV1.from_entries(
            (huge,),
            allowed_names=("huge",),
        )


def test_typed_catalog_and_scope_constructors_redact_malformed_container_errors() -> None:
    with pytest.raises(SubagentCatalogError) as bad_catalog:
        ResolvedSubagentCatalogV1(
            version=1,
            entries=({},),
            allowed_names=(),
            digest="0" * 64,
        )
    assert bad_catalog.value.code == "subagent_catalog_invalid"

    with pytest.raises(SubagentCatalogError) as bad_scopes:
        ResolvedSkillScopesV1(
            version=1,
            scopes={"lead": (), 1: ()},
            digest="0" * 64,
        )
    assert bad_scopes.value.code == "subagent_catalog_invalid"


def test_every_execution_field_change_changes_definition_digest() -> None:
    base = {
        "name": "planner",
        "source_kind": "managed",
        "source_version": "source-v1",
        "description": "Accepted planner",
        "system_prompt": "Accepted prompt",
        "model": None,
        "model_settings": {},
        "tool_names": ("read_file",),
        "skill_names": ("planning",),
        "max_turns": 7,
        "timeout_seconds": 30,
        "inherits_tools": False,
        "disallowed_tool_names": ("task",),
        "policy_settings": {"token_budget": {"enabled": True}},
    }
    changes = {
        "source_version": "source-v2",
        "description": "Changed description",
        "system_prompt": "Changed prompt",
        "model": "worker-model",
        "model_settings": {"temperature": 0.5},
        "tool_names": ("write_file",),
        "skill_names": ("reviewing",),
        "max_turns": 8,
        "timeout_seconds": 31,
        "inherits_tools": True,
        "disallowed_tool_names": ("bash",),
        "policy_settings": {"token_budget": {"enabled": False}},
    }
    original = resolved_subagent_definition(**base)

    changed_digests = {resolved_subagent_definition(**{**base, field: value}).definition_digest for field, value in changes.items()}

    assert original.definition_digest not in changed_digests
    assert len(changed_digests) == len(changes)


def test_model_snapshot_recursively_excludes_secret_like_values(monkeypatch) -> None:
    from deerflow.config.model_config import ModelConfig

    app_config = AppConfig(
        sandbox=SandboxConfig(use="test"),
        models=[
            ModelConfig(
                name="worker-model",
                use="provider.Client",
                model="worker",
                api_key="do-not-persist",
                default_headers={
                    "Authorization": "Bearer do-not-persist",
                    "X-Safe": "safe-value",
                },
            )
        ],
    )
    managed = ManagedSubagentDefinition(
        name="planner",
        description="Plan work",
        system_prompt="Accepted prompt",
        model="worker-model",
        skills=[],
    )
    monkeypatch.setattr(registry, "_managed_definitions", lambda **_: (managed,))

    catalog = snapshot_effective_subagents(
        app_config=app_config,
        agent_config=AgentConfig(name="lead", allowed_subagents=["planner"]),
        user_id="user-1",
        is_bootstrap=False,
        available_skill_names=(),
    )
    persisted = str(catalog.to_persisted_json())

    assert "do-not-persist" not in persisted
    assert "safe-value" in persisted


def test_accepted_model_and_policy_settings_fail_closed_on_drift(
    monkeypatch,
) -> None:
    from deerflow.config.model_config import ModelConfig

    def config(*, temperature: float) -> AppConfig:
        return AppConfig(
            sandbox=SandboxConfig(use="test"),
            models=[
                ModelConfig(
                    name="worker-model",
                    use="provider.Client",
                    model="worker",
                    temperature=temperature,
                )
            ],
        )

    managed = ManagedSubagentDefinition(
        name="planner",
        description="Plan work",
        system_prompt="Accepted prompt",
        model="worker-model",
        tools=[],
        skills=[],
    )
    monkeypatch.setattr(registry, "_managed_definitions", lambda **_: (managed,))
    accepted_config = config(temperature=0.1)
    catalog = snapshot_effective_subagents(
        app_config=accepted_config,
        agent_config=AgentConfig(name="lead", allowed_subagents=["planner"]),
        user_id="user-1",
        is_bootstrap=False,
        available_skill_names=(),
    )
    accepted = catalog.get("planner")
    assert accepted is not None

    accepted.verify_execution_settings(
        accepted_config,
        parent_model_name="unused-parent",
    )
    with pytest.raises(SubagentCatalogError) as raised:
        accepted.verify_execution_settings(
            config(temperature=0.2),
            parent_model_name="unused-parent",
        )

    assert raised.value.code == "subagent_definition_drift"


def _skill(root: Path, name: str):
    skill_dir = root / name
    skill_dir.mkdir()
    skill_file = skill_dir / "SKILL.md"
    skill_file.write_text(
        f"---\nname: {name}\ndescription: Accepted {name}\n---\n\n{name} body\n",
        encoding="utf-8",
    )
    parsed = parse_skill_file(
        skill_file,
        SkillCategory.CUSTOM,
        relative_path=Path(name),
    )
    assert parsed is not None
    return replace(parsed, enabled=True)


def test_revision_snapshots_transitive_skills_once_with_exact_agent_scopes(
    monkeypatch,
    tmp_path: Path,
) -> None:
    from deerflow.runtime import agent_revision

    lead_skill = _skill(tmp_path, "lead-skill")
    worker_skill = _skill(tmp_path, "worker-skill")
    managed = ManagedSubagentDefinition(
        name="planner",
        description="Plan work",
        system_prompt="Accepted planner prompt",
        skills=["worker-skill"],
    )
    monkeypatch.setattr(registry, "_managed_definitions", lambda **_: (managed,))
    monkeypatch.setattr(
        agent_revision,
        "_skills",
        lambda _app_config, *, user_id: (
            (lead_skill, worker_skill),
            (lead_skill, worker_skill),
        ),
    )
    lead_config = AgentConfig(
        name="lead",
        skills=["lead-skill"],
        allowed_subagents=["planner"],
    )
    monkeypatch.setattr(
        agent_revision,
        "make_agent_store",
        lambda _app_config: SimpleNamespace(
            snapshot=lambda _name, *, user_id: SimpleNamespace(
                config=lead_config,
                soul="",
                source="file",
                version="lead-v1",
            )
        ),
    )

    revision = agent_revision.resolve_agent_revision(
        {
            "configurable": {
                "agent_name": "lead",
                "subagent_enabled": True,
            }
        },
        app_config=_app_config(),
        user_id="user-1",
    )
    material = revision.material
    assert material is not None
    try:
        assert material.subagent_catalog.allowed_names == ("planner",)
        assert [skill.name for skill in material.skill_objects_for_scope("lead")] == ["lead-skill"]
        assert [skill.name for skill in material.skill_objects_for_scope("subagent:planner")] == ["worker-skill"]
        assert material.skill_snapshot is not None
        assert {skill.name for skill in material.skill_snapshot.skills} == {
            "lead-skill",
            "worker-skill",
        }
        persisted = revision.to_json()
        assert persisted["subagent_catalog"]["digest"] == material.subagent_catalog.digest
        assert set(persisted["skill_scopes"]["scopes"]) == {
            "lead",
            "subagent:planner",
        }
    finally:
        material.release_process_material()


def test_revision_inherited_subagent_model_uses_the_lead_fallback_profile(
    monkeypatch,
) -> None:
    from deerflow.config.model_config import ModelConfig
    from deerflow.runtime import agent_revision

    managed = ManagedSubagentDefinition(
        name="planner",
        description="Plan work",
        system_prompt="Accepted planner prompt",
        model="inherit",
        tools=[],
        skills=[],
    )
    monkeypatch.setattr(registry, "_managed_definitions", lambda **_: (managed,))
    monkeypatch.setattr(
        agent_revision,
        "_skills",
        lambda _app_config, *, user_id: ((), ()),
    )
    lead_config = AgentConfig(
        name="lead",
        skills=[],
        allowed_subagents=["planner"],
    )
    monkeypatch.setattr(
        agent_revision,
        "make_agent_store",
        lambda _app_config: SimpleNamespace(
            snapshot=lambda _name, *, user_id: SimpleNamespace(
                config=lead_config,
                soul="",
                source="file",
                version="lead-v1",
            )
        ),
    )
    app_config = AppConfig(
        sandbox=SandboxConfig(use="test"),
        models=[
            ModelConfig(
                name="default-model",
                use="provider.DefaultModel",
                model="provider-default",
            )
        ],
    )

    revision = agent_revision.resolve_agent_revision(
        {
            "configurable": {
                "agent_name": "lead",
                "model_name": "missing-model",
                "subagent_enabled": True,
            }
        },
        app_config=app_config,
        user_id="user-1",
    )
    material = revision.material
    assert material is not None
    planner = material.subagent_catalog.get("planner")
    assert planner is not None
    assert planner.model is None
    assert planner.model_settings["model"] == "provider-default"
    assert material.model_profile["name"] == "default-model"


def _persisted_accepted_row(catalog: ResolvedSubagentCatalogV1) -> dict[str, object]:
    from deerflow.runtime.accepted_invocation import (
        AcceptedInvocation,
        InvocationOrigin,
        PrincipalProjection,
        ResolvedAgentMaterialV1,
        ResolvedAgentRevision,
        canonical_digest,
    )

    scopes = ResolvedSkillScopesV1.from_scopes(
        {
            "lead": (),
            **{f"subagent:{name}": () for name in catalog.allowed_names},
        }
    )
    material = ResolvedAgentMaterialV1(
        agent_id="lead",
        storage_source="file",
        storage_version="lead-v1",
        agent_config={"name": "lead"},
        soul="",
        model_profile={"name": "default"},
        subagent_catalog=catalog,
        skill_scopes=scopes,
    )
    accepted = AcceptedInvocation.seal(
        principal=PrincipalProjection(user_id="user-1"),
        origin=InvocationOrigin(source_kind="http"),
        thread_id="thread-catalog",
        context_references={},
        agent_revision=ResolvedAgentRevision.from_material(material),
        normalized_input={},
        execution_options={},
        extension_generation=1,
        contributor_execution_digest=canonical_digest({"version": 1, "execution": []}),
    )
    return {**accepted.to_persisted(), "thread_id": accepted.thread_id}


def test_persisted_accepted_revision_round_trips_catalog_and_skill_scopes() -> None:
    from deerflow.runtime.accepted_invocation import AcceptedInvocation

    catalog = ResolvedSubagentCatalogV1.from_entries(
        (_definition("planner", prompt="Accepted prompt"),),
        allowed_names=("planner",),
    )

    restored = AcceptedInvocation.from_persisted(_persisted_accepted_row(catalog))

    assert restored is not None
    assert restored.agent_revision.subagent_catalog == catalog
    assert restored.agent_revision.skill_scopes is not None
    assert set(restored.agent_revision.skill_scopes.scopes) == {
        "lead",
        "subagent:planner",
    }
    assert restored.agent_revision.legacy_live_catalog is False


def test_persisted_catalog_tampering_is_rejected() -> None:
    from deerflow.runtime.accepted_invocation import AcceptedInvocation

    catalog = ResolvedSubagentCatalogV1.from_entries(
        (_definition("planner", prompt="Accepted prompt"),),
        allowed_names=("planner",),
    )
    row = _persisted_accepted_row(catalog)
    row["agent_revision_json"]["subagent_catalog"]["entries"][0]["system_prompt"] = "tampered"

    with pytest.raises(SubagentCatalogError):
        AcceptedInvocation.from_persisted(row)


def test_legacy_persisted_revision_is_readable_but_marked_non_executable() -> None:
    from deerflow.runtime.accepted_invocation import AcceptedInvocation

    row = _persisted_accepted_row(ResolvedSubagentCatalogV1.empty())
    row["agent_revision_json"].pop("subagent_catalog")
    row["agent_revision_json"].pop("skill_scopes")

    restored = AcceptedInvocation.from_persisted(row)

    assert restored is not None
    assert restored.agent_revision.subagent_catalog is None
    assert restored.agent_revision.legacy_live_catalog is True


def test_lead_prompt_uses_only_frozen_catalog_descriptions(monkeypatch) -> None:
    from deerflow.agents.lead_agent import prompt as prompt_module

    catalog = ResolvedSubagentCatalogV1.from_entries(
        (_definition("planner", prompt="Accepted prompt"),),
        allowed_names=("planner",),
    )

    def live_read_forbidden(*_args, **_kwargs):
        raise AssertionError("accepted prompt discovery must not read the live registry")

    monkeypatch.setattr(
        prompt_module,
        "get_available_subagent_names",
        live_read_forbidden,
    )
    monkeypatch.setattr(
        registry,
        "get_subagent_config",
        live_read_forbidden,
    )

    section = prompt_module._build_subagent_section(
        3,
        resolved_subagent_catalog=catalog,
    )

    assert "**planner**: Delegate planner work" in section


def test_lead_policy_anchors_frozen_catalog_without_live_reads(monkeypatch) -> None:
    from deerflow.agents.lead_agent import agent as lead_agent_module

    catalog = ResolvedSubagentCatalogV1.from_entries(
        (_definition("planner", prompt="Accepted prompt"),),
        allowed_names=("planner",),
    )

    def live_read_forbidden(*_args, **_kwargs):
        raise AssertionError("accepted policy must not read the live registry")

    monkeypatch.setattr(
        registry,
        "get_available_subagent_names",
        live_read_forbidden,
    )
    monkeypatch.setattr(
        registry,
        "get_subagent_config",
        live_read_forbidden,
    )

    policy = lead_agent_module._subagent_release_policy(
        _app_config(),
        enabled=True,
        max_concurrent=3,
        max_total=6,
        resolved_subagent_catalog=catalog,
    )

    assert policy["catalog_digest"] == catalog.digest
    assert policy["type_allowlist"] == ["planner"]
    assert policy["runtime_limits"] == {"planner": {"max_turns": 12, "timeout_seconds": 30.0}}


def test_revision_recovery_reuses_persisted_catalog_after_live_delete(
    monkeypatch,
) -> None:
    from deerflow.runtime import agent_revision

    live = [
        ManagedSubagentDefinition(
            name="planner",
            description="Accepted planner",
            system_prompt="Accepted prompt",
            skills=[],
        )
    ]
    monkeypatch.setattr(registry, "_managed_definitions", lambda **_: tuple(live))
    monkeypatch.setattr(
        agent_revision,
        "_skills",
        lambda _app_config, *, user_id: ((), ()),
    )
    lead_config = AgentConfig(
        name="lead",
        skills=[],
        allowed_subagents=["planner"],
    )
    monkeypatch.setattr(
        agent_revision,
        "make_agent_store",
        lambda _app_config: SimpleNamespace(
            snapshot=lambda _name, *, user_id: SimpleNamespace(
                config=lead_config,
                soul="",
                source="file",
                version="lead-v1",
            )
        ),
    )
    runtime_config = {
        "configurable": {
            "agent_name": "lead",
            "subagent_enabled": True,
        }
    }
    accepted = agent_revision.resolve_agent_revision(
        runtime_config,
        app_config=_app_config(),
        user_id="user-1",
    )
    assert accepted.material is not None
    live.clear()

    recovered = agent_revision.resolve_agent_revision(
        runtime_config,
        app_config=_app_config(),
        user_id="user-1",
        accepted_subagent_catalog=accepted.material.subagent_catalog,
        accepted_skill_scopes=accepted.material.skill_scopes,
    )

    assert recovered.digest == accepted.digest
    assert recovered.material is not None
    assert recovered.material.subagent_catalog.digest == (accepted.material.subagent_catalog.digest)
