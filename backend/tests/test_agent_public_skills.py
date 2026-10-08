"""Instance skill references resolve only approved public packages."""

from pathlib import Path

import pytest

from deerflow.agent_instances.contract import AgentDenied, DefinitionSnapshot
from deerflow.agent_instances.public_skills import capture_public_skills, public_skill_names
from deerflow.skills.types import Skill, SkillCategory


def skill(tmp_path, name, category=SkillCategory.PUBLIC):
    root = tmp_path / name
    root.mkdir()
    (root / "SKILL.md").write_text("---\nname: " + name + "\ndescription: fixture\n---\nUse public tools.\n", encoding="utf-8")
    (root / "script.py").write_bytes(b"print('fixture')\n")
    return Skill(name, "fixture", None, root, root / "SKILL.md", Path(name), category, enabled=True)


def definition(skills):
    return DefinitionSnapshot.capture(owner_id="creator", config={"name": "analyst", "skills": skills, "mcp_plugins": []}, soul="")


def test_no_private_global_integration_or_legacy_skill_fallback(tmp_path, monkeypatch):
    from deerflow.agents.lead_agent import prompt

    public = skill(tmp_path, "public")
    private = skill(tmp_path, "private", SkillCategory.CUSTOM)
    legacy = skill(tmp_path, "legacy", SkillCategory.LEGACY)
    monkeypatch.setattr(prompt, "get_enabled_skills_for_config", lambda config, user_id=None: [public, private, legacy])
    assert public_skill_names(definition(None), object()) == {"public"}
    assert public_skill_names(definition([]), object()) == set()
    with pytest.raises(AgentDenied):
        public_skill_names(definition(["private"]), object())


def test_public_capture_retains_bytes_and_updates_content_revision(tmp_path, monkeypatch):
    from deerflow.agents.lead_agent import prompt

    public = skill(tmp_path, "public")
    monkeypatch.setattr(prompt, "get_enabled_skills_for_config", lambda config, user_id=None: [public])
    first = capture_public_skills(definition(["public"]), object())
    assert first.names == frozenset({"public"})
    assert ("public/public/script.py", b"print('fixture')\n") in first.files
    (public.skill_dir / "script.py").write_bytes(b"print('changed')\n")
    second = capture_public_skills(definition(["public"]), object())
    assert first.revision != second.revision
    assert first.files != second.files


def test_public_package_link_cannot_import_host_files(tmp_path, monkeypatch):
    from deerflow.agents.lead_agent import prompt

    public = skill(tmp_path, "public")
    outside = tmp_path / "private-data"
    outside.write_bytes(b"never import")
    (public.skill_dir / "linked").symlink_to(outside)
    monkeypatch.setattr(prompt, "get_enabled_skills_for_config", lambda config, user_id=None: [public])
    with pytest.raises(AgentDenied):
        capture_public_skills(definition(["public"]), object())


def test_bound_activation_cannot_read_a_later_same_named_private_package(tmp_path, monkeypatch):
    from types import SimpleNamespace

    from test_agent_execution_runtime import execution

    from deerflow.agent_instances.runtime import execution_scope
    from deerflow.agents.middlewares.skill_activation_middleware import SkillActivationMiddleware
    from deerflow.skills import storage

    private = skill(tmp_path, "public", SkillCategory.LEGACY)
    monkeypatch.setattr(storage, "_default_skill_storage", SimpleNamespace(load_skills=lambda **kwargs: [private], get_skills_root_path=lambda: tmp_path))
    monkeypatch.setattr(storage, "_default_skill_storage_config", None)
    middleware = SkillActivationMiddleware(available_skills={"public"}, user_id=None, slash_source_owner_token="fixture")
    with execution_scope(execution()):
        assert middleware._storage().load_skills(enabled_only=False) == []
        with pytest.raises(AgentDenied):
            storage.get_or_new_user_skill_storage("requester")
