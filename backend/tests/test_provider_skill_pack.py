"""Explicit operator snapshots remain stable beside private customer variants."""

from pathlib import Path

import pytest

from deerflow.config.paths import Paths
from deerflow.skills.storage.user_scoped_skill_storage import UserScopedSkillStorage


def package(root: Path, name: str, content: str):
    folder = root / name
    folder.mkdir(parents=True, exist_ok=True)
    (folder / "SKILL.md").write_text(f"---\nname: {name}\ndescription: Operator workflow\n---\n{content}\n", encoding="utf-8")
    return folder


def test_operator_pack_upgrade_preserves_private_bytes_history_and_baseline_visibility(tmp_path, monkeypatch):
    from deerflow.skills.provider_pack import install_provider_skill_pack

    home = tmp_path / "home"
    monkeypatch.setattr("deerflow.config.paths.get_paths", lambda: Paths(base_dir=home))
    source = tmp_path / "source"
    package(source, "operator-workflow", "Baseline version one.")
    install_provider_skill_pack(source, "office", home=home)
    store = UserScopedSkillStorage("owner", host_path=str(tmp_path / "skills"))
    baseline = next(s for s in store.load_skills() if s.name == "operator-workflow")
    assert baseline.category.value == "integrations"
    store.write_custom_skill("owner-workflow", "SKILL.md", "---\nname: owner-workflow\ndescription: Owner variant\n---\nPrivate edits.\n")
    store.append_history("owner-workflow", {"action": "fixture", "new_content": "Private edits."})
    original = store.read_custom_skill("owner-workflow")
    history = store.read_history("owner-workflow")
    package(source, "operator-workflow", "Baseline version two.")
    with pytest.raises(FileExistsError):
        install_provider_skill_pack(source, "office", home=home)
    install_provider_skill_pack(source, "office", home=home, replace=True)
    reloaded = UserScopedSkillStorage("owner", host_path=str(tmp_path / "skills"))
    baseline = next(s for s in reloaded.load_skills() if s.name == "operator-workflow")
    assert baseline.skill_file.read_text(encoding="utf-8").endswith("Baseline version two.\n")
    assert reloaded.read_custom_skill("owner-workflow") == original
    assert reloaded.read_history("owner-workflow") == history


def test_operator_promotion_does_not_modify_legacy_package_or_existing_histories(tmp_path, monkeypatch):
    from deerflow.skills.provider_pack import install_provider_skill_pack

    home = tmp_path / "home"
    monkeypatch.setattr("deerflow.config.paths.get_paths", lambda: Paths(base_dir=home))
    legacy_root = tmp_path / "skills" / "custom"
    original = package(legacy_root, "legacy-workflow", "Existing legacy content.")
    before = (original / "SKILL.md").read_bytes()
    install_provider_skill_pack(original, "promoted", home=home)
    store = UserScopedSkillStorage("owner", host_path=str(tmp_path / "skills"))
    assert next(s for s in store.load_skills() if s.name == "legacy-workflow").category.value == "integrations"
    assert (original / "SKILL.md").read_bytes() == before


def test_duplicate_names_across_provider_packs_are_refused(tmp_path):
    from deerflow.skills.provider_pack import install_provider_skill_pack

    source = tmp_path / "source"
    package(source, "operator-workflow", "Baseline content.")
    install_provider_skill_pack(source, "first", home=tmp_path / "home")
    with pytest.raises(ValueError, match="another provider pack"):
        install_provider_skill_pack(source, "second", home=tmp_path / "home")


def test_linked_sources_are_refused_without_touching_old_pack(tmp_path):
    from support.symlinks import symlink_or_skip

    from deerflow.skills.provider_pack import install_provider_skill_pack

    source = tmp_path / "source"
    original = package(source, "operator-workflow", "Baseline content.")
    home = tmp_path / "home"
    install_provider_skill_pack(source, "office", home=home)
    old = home / "integrations" / "skills" / "provider" / "office" / "operator-workflow" / "SKILL.md"
    before = old.read_bytes()
    symlink_or_skip(original / "linked.txt", tmp_path / "missing")
    with pytest.raises(ValueError):
        install_provider_skill_pack(source, "office", home=home, replace=True)
    assert old.read_bytes() == before


def test_upgrade_publishes_with_one_atomic_exchange_and_never_retires_target(tmp_path, monkeypatch):
    from deerflow.skills import provider_pack

    source, home = tmp_path / "source", tmp_path / "home"
    package(source, "operator-workflow", "Before upgrade.")
    provider_pack.install_provider_skill_pack(source, "office", home=home)
    target = home / "integrations" / "skills" / "provider" / "office"
    package(source, "operator-workflow", "After upgrade.")
    original_rename = Path.rename

    def guarded_rename(self, destination):
        assert self != target, "Retiring the published directory creates a missing baseline"
        return original_rename(self, destination)

    monkeypatch.setattr(Path, "rename", guarded_rename)
    provider_pack.install_provider_skill_pack(source, "office", home=home, replace=True)
    assert (target / "operator-workflow" / "SKILL.md").read_text(encoding="utf-8").endswith("After upgrade.\n")


def test_failed_atomic_exchange_preserves_existing_pack(tmp_path, monkeypatch):
    from deerflow.skills import provider_pack

    source, home = tmp_path / "source", tmp_path / "home"
    package(source, "operator-workflow", "Before upgrade.")
    provider_pack.install_provider_skill_pack(source, "office", home=home)
    target = home / "integrations" / "skills" / "provider" / "office"
    before = (target / "operator-workflow" / "SKILL.md").read_bytes()
    package(source, "operator-workflow", "After upgrade.")

    def interrupt(*args):
        raise KeyboardInterrupt("Synthetic pre-publication interruption")

    monkeypatch.setattr(provider_pack, "_exchange_directories", interrupt)
    with pytest.raises(KeyboardInterrupt):
        provider_pack.install_provider_skill_pack(source, "office", home=home, replace=True)
    assert (target / "operator-workflow" / "SKILL.md").read_bytes() == before


def test_interruption_after_atomic_exchange_leaves_complete_new_pack(tmp_path, monkeypatch):
    from deerflow.skills import provider_pack

    source, home = tmp_path / "source", tmp_path / "home"
    package(source, "operator-workflow", "Before upgrade.")
    provider_pack.install_provider_skill_pack(source, "office", home=home)
    target = home / "integrations" / "skills" / "provider" / "office"
    package(source, "operator-workflow", "After upgrade.")
    exchange = provider_pack._exchange_directories

    def interrupt_after(staged, published):
        exchange(staged, published)
        raise KeyboardInterrupt("Synthetic post-publication interruption")

    monkeypatch.setattr(provider_pack, "_exchange_directories", interrupt_after)
    with pytest.raises(KeyboardInterrupt):
        provider_pack.install_provider_skill_pack(source, "office", home=home, replace=True)
    assert (target / "operator-workflow" / "SKILL.md").read_text(encoding="utf-8").endswith("After upgrade.\n")
    assert (target / ".provider-pack.json").is_file()
