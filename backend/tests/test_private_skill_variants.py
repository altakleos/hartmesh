"""Private variants retain their source identity and deployment ceiling."""

import json
import zipfile

import pytest

from deerflow.config.app_config import AppConfig
from deerflow.config.paths import Paths
from deerflow.skills.storage.user_scoped_skill_storage import UserScopedSkillStorage


def write_package(root, name="starter", body="Provider baseline."):
    package = root / name
    package.mkdir(parents=True, exist_ok=True)
    (package / "SKILL.md").write_text(f"---\nname: {name}\ndescription: Sample workflow\n---\n{body}\n", encoding="utf-8")
    (package / "references").mkdir(exist_ok=True)
    (package / "references" / "notes.txt").write_text("Reference notes.\n", encoding="utf-8")
    return package


@pytest.fixture
def variant_store(tmp_path, monkeypatch):
    from deerflow.skills.security_scanner import ScanResult

    paths = Paths(base_dir=tmp_path / "home")
    monkeypatch.setattr("deerflow.config.paths.get_paths", lambda: paths)
    config = AppConfig.model_validate({"sandbox": {"use": "test"}, "skills": {"path": str(tmp_path / "skills")}})
    state_file = tmp_path / "extensions.json"
    state_file.write_text('{"mcpServers":{},"skills":{}}', encoding="utf-8")
    monkeypatch.setenv("DEER_FLOW_EXTENSIONS_CONFIG_PATH", str(state_file))
    store = UserScopedSkillStorage("owner-a", app_config=config)
    source = write_package(paths.integration_skills_dir() / "provider" / "sample")
    scanned = []

    async def scan(content, *, location, **kwargs):
        scanned.append((location, content))
        return ScanResult(decision="allow", reason="Offline model decision")

    monkeypatch.setattr("deerflow.skills.installer.scan_skill_content", scan)
    return store, source, state_file, scanned


def test_explicit_provider_pack_wins_over_legacy_before_and_after_first_private_package(variant_store):
    store, _, _, _ = variant_store
    write_package(store.get_skills_root_path() / "custom", body="Legacy global content.")
    before = next(s for s in store.load_skills() if s.name == "starter")
    assert before.category.value == "integrations"
    assert before.skill_file.read_text(encoding="utf-8").endswith("Provider baseline.\n")
    store.write_custom_skill("unrelated-private", "SKILL.md", "---\nname: unrelated-private\ndescription: Owner notes\n---\nPrivate content.\n")
    after = next(s for s in store.load_skills() if s.name == "starter")
    assert after.skill_file == before.skill_file


@pytest.mark.asyncio
async def test_distinct_clone_keeps_provenance_scans_and_source_bytes(variant_store):
    from deerflow.skills.private_variants import clone_provider_skill, provider_skill_sources, source_manifest

    store, source, _, scanned = variant_store
    selected = provider_skill_sources(store)[0]
    original = {p.relative_to(source): p.read_bytes() for p in source.rglob("*") if p.is_file()}
    preview = source_manifest(store, selected["source_id"])
    result = await clone_provider_skill(store, selected["source_id"], expected_revision=preview["revision"])
    assert result["skill_name"] == "starter-private"
    assert scanned
    assert any("name: starter-private" in content for _, content in scanned)
    origin = store.get_skill_origin("starter-private")
    assert origin["source_id"] == selected["source_id"]
    assert origin["source_name"] == "starter"
    assert origin["revision"] == preview["revision"]
    for path, content in original.items():
        assert (source / path).read_bytes() == content
    assert store.read_custom_skill("starter-private").startswith("---\n")
    assert "name: starter-private" in store.read_custom_skill("starter-private")


@pytest.mark.asyncio
async def test_source_disable_ceiling_applies_to_renamed_clone_and_survives_restart(variant_store):
    from deerflow.skills.private_variants import clone_provider_skill, provider_skill_sources, source_manifest

    store, _, state_file, _ = variant_store
    selected = provider_skill_sources(store)[0]
    await clone_provider_skill(store, selected["source_id"], expected_revision=source_manifest(store, selected["source_id"])["revision"])
    state_file.write_text(json.dumps({"mcpServers": {}, "skills": {"starter": {"enabled": False}}}), encoding="utf-8")
    store.set_skill_enabled_state("starter-private", True)
    reloaded = UserScopedSkillStorage("owner-a", app_config=store._app_config)
    clone = next(s for s in reloaded.load_skills() if s.name == "starter-private")
    assert clone.enabled is False
    assert reloaded.get_skill_origin("starter-private")["source_name"] == "starter"
    assert reloaded.read_custom_skill("starter-private")


@pytest.mark.asyncio
async def test_same_name_clone_requires_consent_and_does_not_overwrite_private_package(variant_store):
    from deerflow.skills.installer import SkillAlreadyExistsError
    from deerflow.skills.private_variants import clone_provider_skill, provider_skill_sources, source_manifest

    store, _, _, _ = variant_store
    selected = provider_skill_sources(store)[0]
    revision = source_manifest(store, selected["source_id"])["revision"]
    with pytest.raises(ValueError, match="explicit"):
        await clone_provider_skill(store, selected["source_id"], expected_revision=revision, name="starter")
    await clone_provider_skill(store, selected["source_id"], expected_revision=revision, name="starter", allow_baseline_override=True)
    before = store.read_custom_skill("starter")
    with pytest.raises(SkillAlreadyExistsError):
        await clone_provider_skill(store, selected["source_id"], expected_revision=revision, name="starter", allow_baseline_override=True)
    assert store.read_custom_skill("starter") == before
    # Source inventory still exposes the baseline behind the private winner.
    assert selected["source_id"] in {s["source_id"] for s in provider_skill_sources(store)}


@pytest.mark.asyncio
async def test_clone_rejects_changed_source_and_denied_scanning(variant_store, monkeypatch):
    from deerflow.skills.export import SkillExportError
    from deerflow.skills.private_variants import clone_provider_skill, provider_skill_sources, source_manifest
    from deerflow.skills.security_scanner import ScanResult

    store, source, _, _ = variant_store
    selected = provider_skill_sources(store)[0]
    revision = source_manifest(store, selected["source_id"])["revision"]
    (source / "references" / "notes.txt").write_text("Changed source.\n", encoding="utf-8")
    with pytest.raises(SkillExportError) as error:
        await clone_provider_skill(store, selected["source_id"], expected_revision=revision)
    assert error.value.code == "skill_changed"

    async def deny(*args, **kwargs):
        return ScanResult(decision="block", reason="Rejected fixture")

    monkeypatch.setattr("deerflow.skills.installer.scan_skill_content", deny)
    with pytest.raises(ValueError):
        await clone_provider_skill(store, selected["source_id"], expected_revision=source_manifest(store, selected["source_id"])["revision"])
    assert not store.custom_skill_exists("starter-private")
    assert store.get_skill_origin("starter-private") is None


def test_corrupt_origin_metadata_never_drops_the_disable_ceiling(variant_store):
    store, _, _, _ = variant_store
    store.write_custom_skill("private-notes", "SKILL.md", "---\nname: private-notes\ndescription: Owner notes\n---\nPrivate content.\n")
    store.skill_origins_file.write_text('{"broken":', encoding="utf-8")
    with pytest.raises(ValueError, match="origin"):
        store.load_skills(enabled_only=True)


@pytest.mark.asyncio
async def test_origin_publication_failure_leaves_no_discoverable_clone(variant_store, monkeypatch):
    from deerflow.skills.private_variants import clone_provider_skill, provider_skill_sources, source_manifest

    store, _, _, _ = variant_store
    selected = provider_skill_sources(store)[0]
    original = store._set_skill_origin

    def failed_write(name, origin):
        if origin is not None:
            raise OSError("Synthetic origin write failure")
        return original(name, origin)

    monkeypatch.setattr(store, "_set_skill_origin", failed_write)
    with pytest.raises(OSError):
        await clone_provider_skill(store, selected["source_id"], expected_revision=source_manifest(store, selected["source_id"])["revision"])
    assert not store.custom_skill_exists("starter-private")
    assert all(s.name != "starter-private" for s in store.load_skills())
    assert store.get_skill_origin("starter-private") is None


@pytest.mark.asyncio
async def test_source_upgrade_during_scanning_refuses_stale_clone(variant_store, monkeypatch):
    from deerflow.skills.export import SkillExportError
    from deerflow.skills.private_variants import clone_provider_skill, provider_skill_sources, source_manifest
    from deerflow.skills.security_scanner import ScanResult

    store, source, _, _ = variant_store
    selected = provider_skill_sources(store)[0]
    revision = source_manifest(store, selected["source_id"])["revision"]

    async def scan(*args, **kwargs):
        (source / "references" / "notes.txt").write_text("Operator upgraded the source.\n", encoding="utf-8")
        return ScanResult(decision="allow", reason="Offline model decision")

    monkeypatch.setattr("deerflow.skills.installer.scan_skill_content", scan)
    with pytest.raises(SkillExportError) as error:
        await clone_provider_skill(store, selected["source_id"], expected_revision=revision)
    assert error.value.code == "skill_changed"
    assert not store.custom_skill_exists("starter-private")
    assert store.get_skill_origin("starter-private") is None


@pytest.mark.asyncio
async def test_incoming_package_metadata_cannot_install_host_provenance(variant_store, tmp_path):
    store, _, _, _ = variant_store
    archive = tmp_path / "ordinary.skill"
    with zipfile.ZipFile(archive, "w") as output:
        output.writestr("ordinary/SKILL.md", "---\nname: ordinary\ndescription: Private ordinary package\n---\nNotes.\n")
        output.writestr("ordinary/_skill_origins.json", json.dumps({"source_name": "starter", "private_skill_owner": True}))
    await store.ainstall_skill_from_archive(archive)
    assert store.get_skill_origin("ordinary") is None


@pytest.mark.asyncio
async def test_ordinary_same_name_archive_needs_explicit_override_consent(variant_store, tmp_path):
    store, _, _, _ = variant_store
    archive = tmp_path / "starter.skill"
    with zipfile.ZipFile(archive, "w") as output:
        output.writestr("starter/SKILL.md", "---\nname: starter\ndescription: Private override\n---\nOwner content.\n")
    with pytest.raises(ValueError, match="explicit"):
        await store.ainstall_skill_from_archive(archive)
    await store.ainstall_skill_from_archive(archive, allow_baseline_override=True)
    assert store.read_custom_skill("starter").endswith("Owner content.\n")


@pytest.mark.asyncio
async def test_clone_relocates_its_instructions_without_rewriting_supporting_resources(variant_store):
    from deerflow.skills.private_variants import clone_provider_skill, provider_skill_sources, source_manifest

    store, source, _, _ = variant_store
    old = "/mnt/skills/integrations/provider/sample/starter"
    body = f"---\nname: starter\ndescription: Workflow\n---\nRead {old}/references/notes.txt before working.\n"
    (source / "SKILL.md").write_text(body, encoding="utf-8")
    (source / "references" / "notes.txt").write_text(f"Quoted original path: {old}\n", encoding="utf-8")
    selected = provider_skill_sources(store)[0]
    await clone_provider_skill(store, selected["source_id"], expected_revision=source_manifest(store, selected["source_id"])["revision"])
    assert "/mnt/skills/custom/starter-private/references/notes.txt" in store.read_custom_skill("starter-private")
    assert (store.get_custom_skill_dir("starter-private") / "references" / "notes.txt").read_bytes() == (source / "references" / "notes.txt").read_bytes()
    assert (source / "SKILL.md").read_text(encoding="utf-8") == body


@pytest.mark.asyncio
async def test_cancelled_clone_drains_admitted_scan_and_commits_complete_origin(variant_store, monkeypatch):
    import asyncio

    from deerflow.skills.private_variants import clone_provider_skill, provider_skill_sources, source_manifest
    from deerflow.skills.security_scanner import ScanResult

    store, _, _, _ = variant_store
    started, finish = asyncio.Event(), asyncio.Event()

    async def scan(*args, **kwargs):
        started.set()
        await finish.wait()
        return ScanResult(decision="allow", reason="Offline model decision")

    monkeypatch.setattr("deerflow.skills.installer.scan_skill_content", scan)
    source = provider_skill_sources(store)[0]
    task = asyncio.create_task(clone_provider_skill(store, source["source_id"], expected_revision=source_manifest(store, source["source_id"])["revision"]))
    await asyncio.wait_for(started.wait(), 3)
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    finish.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert store.custom_skill_exists("starter-private")
    assert store.get_skill_origin("starter-private")["source_name"] == "starter"


@pytest.mark.asyncio
async def test_two_owners_keep_variant_content_history_and_toggles_separate(variant_store):
    from deerflow.skills.private_variants import clone_provider_skill, provider_skill_sources, source_manifest

    alice, _, _, _ = variant_store
    bob = UserScopedSkillStorage("owner-b", app_config=alice._app_config)
    source = provider_skill_sources(alice)[0]
    revision = source_manifest(alice, source["source_id"])["revision"]
    for store in (alice, bob):
        await clone_provider_skill(store, source["source_id"], expected_revision=revision)
    before = bob.read_custom_skill("starter-private")
    alice.write_custom_skill("starter-private", "SKILL.md", before + "Alice-only edits.\n")
    alice.append_history("starter-private", {"action": "fixture", "new_content": "Alice-only edits."})
    alice.set_skill_enabled_state("starter-private", False)
    assert bob.read_custom_skill("starter-private") == before
    assert bob.read_history("starter-private") == []
    assert next(s for s in bob.load_skills() if s.name == "starter-private").enabled
    assert not next(s for s in alice.load_skills() if s.name == "starter-private").enabled


@pytest.mark.asyncio
async def test_cloned_resources_preserve_empty_directories_and_executable_modes(variant_store):
    from deerflow.skills.private_variants import clone_provider_skill, provider_skill_sources, source_manifest

    store, source, _, _ = variant_store
    (source / "empty-assets").mkdir()
    script = source / "scripts" / "notes.py"
    script.parent.mkdir()
    script.write_text("print('ordinary notes')\n", encoding="utf-8")
    script.chmod(0o755)
    selected = provider_skill_sources(store)[0]
    await clone_provider_skill(store, selected["source_id"], expected_revision=source_manifest(store, selected["source_id"])["revision"])
    copied = store.get_custom_skill_dir("starter-private")
    assert (copied / "empty-assets").is_dir()
    assert (copied / "scripts" / "notes.py").stat().st_mode & 0o111


@pytest.mark.asyncio
async def test_clone_relocates_configured_container_root(variant_store):
    from deerflow.skills.private_variants import clone_provider_skill, provider_skill_sources, source_manifest

    original, source, _, _ = variant_store
    store = UserScopedSkillStorage("owner-a", app_config=original._app_config, container_path="/mnt/company-skills")
    source_path = "/mnt/company-skills/integrations/provider/sample/starter"
    (source / "SKILL.md").write_text(f"---\nname: starter\ndescription: Workflow\n---\nRead {source_path}/references/notes.txt.\n", encoding="utf-8")
    selected = provider_skill_sources(store)[0]
    await clone_provider_skill(store, selected["source_id"], expected_revision=source_manifest(store, selected["source_id"])["revision"])
    assert "/mnt/company-skills/custom/starter-private/references/notes.txt" in store.read_custom_skill("starter-private")
