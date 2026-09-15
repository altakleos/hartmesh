"""The tenant VM profile ships the public skill library.

``backend/Dockerfile`` copies ``skills/public`` into the image, and at every
start ``deploy/compose/gateway/run.sh`` mirrors it onto the tenant data disk
through ``gateway/seed_skills.sh``. Until then no public skill existed on a
tenant VM: the skills directory was created empty and nothing filled it.
These tests pin the image layer, the seed's contract on a real tree, the
sandbox projection of the seeded set, and its admission by the governed tool
plane under the profile's own policy.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest
import yaml
from _config_singleton_guard import restore_config_singletons  # noqa: F401 -- autouse fixture
from deerflow_extension_api import (
    CredentialEvidenceV1,
    EffectiveSubjectV1,
    InvocationIdentityV1,
    TenantReferenceV1,
    VerifiedActorContextV1,
    effective_authority_digest_v1,
)

from deerflow.config.extensions_config import ExtensionsConfig
from deerflow.config.paths import Paths
from deerflow.sandbox.local.local_sandbox_provider import LocalSandboxProvider
from deerflow.skills.projection import rebuild_skill_projections
from deerflow.skills.review import LocalDirectoryReader, analyze_skill_package
from deerflow.skills.storage.local_skill_storage import LocalSkillStorage
from deerflow.tool_plane import (
    GovernedSkillArtifactStore,
    GovernedToolPlaneValidator,
    InMemoryToolPlaneRevisionRepository,
    LockedFileToolPlaneProjection,
    ToolPlaneRevisionScopeV1,
    ToolPlaneRevisionService,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
PROFILE = REPO_ROOT / "deploy" / "compose"
SEED = PROFILE / "gateway" / "seed_skills.sh"
RUN = PROFILE / "gateway" / "run.sh"
TEMPLATE = PROFILE / "config.yaml"
DOCKERFILE = REPO_ROOT / "backend" / "Dockerfile"
DOCKERIGNORE = REPO_ROOT / ".dockerignore"
PUBLIC = REPO_ROOT / "skills" / "public"
# Where the image carries the library and the one seed line run.sh must hold.
IMAGE_PUBLIC = "/app/skills/public"
# Excluded by the profile's policy (run.sh says why per name: tenant content
# posted to an external service, instructions fetched from a third-party URL,
# flows that cannot work on this profile). None is a review refusal.
EXCLUDED_BY_POLICY = ("chart-visualization", "claude-to-deerflow", "find-skills", "podcast-generation", "web-design-guidelines")
# Excluded because the profile's own skill review refuses them (secret
# assignments in scripts, subprocess use, a sensitive capability declaration):
# a governed base holding any one of them could never be promoted. Each must
# still be refused, or its exclusion is stale (a test below pins that).
EXCLUDED_BY_REVIEW = ("github-deep-research", "image-generation", "music-generation", "skill-creator", "vercel-deploy-claimable", "video-generation")
EXCLUDED = EXCLUDED_BY_POLICY + EXCLUDED_BY_REVIEW
EXCLUSION_LINE = f'EXCLUDED_PUBLIC_SKILLS="{" ".join(sorted(EXCLUDED))}"'  # run.sh keeps the list alphabetical
SEED_LINE = f'sh "$PROFILE/gateway/seed_skills.sh" {IMAGE_PUBLIC} "$DEER_FLOW_HOME/skills" $EXCLUDED_PUBLIC_SKILLS'


def _seed(source: Path, root: Path, *excluded: str) -> subprocess.CompletedProcess[str]:
    env = {"PATH": os.environ["PATH"]}
    return subprocess.run(["sh", str(SEED), str(source), str(root), *excluded], env=env, capture_output=True, text=True, timeout=120, check=False)


_LOCAL_CACHES = {"__pycache__", ".ruff_cache"}


def _tree(root: Path) -> dict[str, bytes]:
    """Every regular file under ``root`` by relative path; symlinks are refused.

    Local caches are skipped: a developer checkout carries them, the Docker
    context never does (``.dockerignore``), so they are not library content."""
    files: dict[str, bytes] = {}
    for path in sorted(root.rglob("*")):
        assert not path.is_symlink(), path
        if _LOCAL_CACHES & set(path.relative_to(root).parts):
            continue
        if path.is_file():
            files[path.relative_to(root).as_posix()] = path.read_bytes()
    return files


def _skill(root: Path, name: str, body: str = "v1", *extra: tuple[str, str]) -> None:
    (root / name).mkdir(parents=True, exist_ok=True)
    (root / name / "SKILL.md").write_text(f"---\nname: {name}\ndescription: {body}\n---\n\n# {name}\n\n{body}\n", encoding="utf-8")
    for relative, content in extra:
        target = root / name / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")


def _seeded_names() -> set[str]:
    """Package directory names the seed places under public/."""
    return {path.name for path in PUBLIC.iterdir() if path.is_dir() and not path.name.startswith(".")} - set(EXCLUDED)


def _declared_names() -> set[str]:
    """The ``name:`` each seeded package declares in its SKILL.md frontmatter,
    which is what the tool plane keys its manifest by (one package directory,
    ``vercel-deploy-claimable``, declares a different name)."""
    names = set()
    for package in _seeded_names():
        text = (PUBLIC / package / "SKILL.md").read_text(encoding="utf-8")
        assert text.startswith("---\n"), package
        names.add(str(yaml.safe_load(text.split("\n---\n", 1)[0][4:])["name"]))
    return names


# ── the image layer ──────────────────────────────────────────────────────────


def test_backend_image_carries_the_public_skill_library() -> None:
    dockerfile = DOCKERFILE.read_text(encoding="utf-8")
    runtime_stage = dockerfile.rindex("FROM python:3.12-slim-bookworm")
    copy = "COPY skills/public ./skills/public"
    assert dockerfile.count(copy) == 1
    assert dockerfile.index(copy) > runtime_stage, "the library ships in the runtime stage"
    assert dockerfile.index(copy) > dockerfile.index("COPY --from=builder /app/contracts ./contracts"), "last, so a skill edit rebuilds only its own layer"
    assert "WORKDIR /app" in dockerfile[runtime_stage:], f"the copy lands at {IMAGE_PUBLIC}"

    ignore = DOCKERIGNORE.read_text(encoding="utf-8").splitlines()
    assert "skills/" in ignore and "!skills/public" in ignore, "the context excludes skills/ and re-admits only the public library"
    assert ignore.index("!skills/public") > ignore.index("skills/")
    assert "!skills/custom" not in ignore and "!skills" not in ignore, "custom/ is operator material, never release content"
    # The re-include wins over every earlier pattern for the subtree, so the
    # blanket exclusions must be restated after it.
    for pattern in ("skills/public/**/__pycache__", "skills/public/**/.ruff_cache", "skills/public/**/.env", "skills/public/**/.env.*", "skills/public/**/.venv", "skills/public/**/node_modules"):
        assert pattern in ignore and ignore.index(pattern) > ignore.index("!skills/public"), pattern


def test_the_public_tree_holds_no_symlinks_and_tracks_no_caches() -> None:
    """The seed copies files; a symlink would be copied as a link into a
    read-only mount or dereferenced past the package boundary, and the
    artifact store refuses either."""
    tree = _tree(PUBLIC)
    assert "business-report/SKILL.md" in tree
    tracked = subprocess.run(["git", "-C", str(REPO_ROOT), "ls-files", "-z", "skills/public"], capture_output=True, text=True, check=True).stdout.split("\0")
    assert not [path for path in tracked if _LOCAL_CACHES & set(path.split("/"))], "a cache committed into the library would ship in the image"
    for excluded in EXCLUDED:
        assert (PUBLIC / excluded / "SKILL.md").is_file(), f"{excluded} is excluded by name; the name must still exist upstream or the exclusion is stale"


# ── run.sh ───────────────────────────────────────────────────────────────────


def test_run_sh_seeds_the_library_after_creating_the_skills_root_and_before_the_gateway_starts() -> None:
    run = RUN.read_text(encoding="utf-8")
    assert run.count(SEED_LINE) == 1, SEED_LINE
    assert run.count(EXCLUSION_LINE) == 1, EXCLUSION_LINE
    assert run.index(EXCLUSION_LINE) < run.index(SEED_LINE)
    assert run.index('mkdir -p "$DEER_FLOW_HOME" "$DEER_FLOW_HOME/skills"') < run.index(SEED_LINE)
    assert run.index(SEED_LINE) < run.index('render_config.py" \\'), "the library is in place before anything reads config.yaml"
    assert run.index(SEED_LINE) < run.index("exec env PYTHONPATH=. uv run --no-sync uvicorn")
    assert "skills/custom" not in run and run.count("skills/public") == 1, "run.sh touches only the public library, through the seed"
    assert not SEED.stat().st_mode & 0o111, "the bundle relies on no exec bit; run.sh invokes it as sh"
    assert SEED.read_text(encoding="utf-8").startswith("#!/bin/sh\n")


# ── the seed's contract, on a synthetic tree ─────────────────────────────────


def test_seed_mirrors_the_image_library_and_leaves_everything_else_alone(tmp_path: Path) -> None:
    source = tmp_path / "image" / "public"
    _skill(source, "alpha", "v1", ("scripts/run.py", "print(1)\n"), ("references/.hidden", "dot\n"))
    _skill(source, "beta")
    _skill(source, "excluded-skill")
    root = tmp_path / "home" / "skills"
    _skill(root / "custom", "mine", "operator", ("notes.txt", "keep\n"))
    (root / "custom" / ".history").mkdir()
    (root / "custom" / ".history" / "mine.jsonl").write_text("{}\n", encoding="utf-8")
    _skill(root / "public", "stale", "left by an earlier image")
    _skill(root / "public", "alpha", "an earlier release", ("old.txt", "gone\n"))
    (root / "public.seed" / "half").mkdir(parents=True)
    (root / "public.seed" / "half" / "SKILL.md").write_text("interrupted copy\n", encoding="utf-8")
    view = tmp_path / "home" / "skills_view" / "public" / "alpha"
    view.mkdir(parents=True)
    (view / "SKILL.md").write_text("the Gateway's projection\n", encoding="utf-8")
    before_custom = _tree(root / "custom")
    before_view = _tree(tmp_path / "home" / "skills_view")

    result = _seed(source, root, "excluded-skill")

    assert result.returncode == 0, result.stderr
    expected = {path: content for path, content in _tree(source).items() if not path.startswith("excluded-skill/")}
    assert _tree(root / "public") == expected, "public/ is release material: the previous set, stale skills and stray files are replaced"
    assert _tree(root / "custom") == before_custom
    assert _tree(tmp_path / "home" / "skills_view") == before_view, "the projection is the Gateway's to rebuild"
    assert not (root / "public.seed").exists(), "an interrupted earlier seed is cleaned up"
    assert not (root / "public.old").exists(), "the previous set is gone once the swap is complete"
    assert sorted(path.name for path in root.iterdir()) == ["custom", "public"]

    # A second start with a changed image: one skill gone, one changed, one new.
    shutil.rmtree(source / "beta")
    _skill(source, "alpha", "v2")
    _skill(source, "gamma")
    again = _seed(source, root, "excluded-skill")
    assert again.returncode == 0, again.stderr
    assert _tree(root / "public") == {path: content for path, content in _tree(source).items() if not path.startswith("excluded-skill/")}
    assert _tree(root / "custom") == before_custom


def test_seed_refuses_an_exclusion_that_is_not_a_directory_name(tmp_path: Path) -> None:
    source = tmp_path / "image" / "public"
    _skill(source, "alpha")
    root = tmp_path / "home" / "skills"
    for bad in ("", ".", "..", ".hidden", "alpha/scripts", "/"):
        result = _seed(source, root, bad)
        assert result.returncode == 2, bad
        assert not root.exists(), bad


def test_seed_skips_an_older_image_and_refuses_an_empty_or_linked_library(tmp_path: Path) -> None:
    root = tmp_path / "home" / "skills"
    _skill(root / "public", "alpha")
    before = _tree(root)

    # No library directory at all: a Gateway image that predates the feature.
    # The profile between cuts carries the previous release's pin, so this must
    # start the Gateway, not crash-loop it.
    missing = _seed(tmp_path / "image" / "public", root)
    assert missing.returncode == 0, missing.stderr
    assert str(tmp_path / "image" / "public") in missing.stderr and "predates" in missing.stderr
    assert _tree(root) == before
    fresh = tmp_path / "fresh" / "skills"
    assert _seed(tmp_path / "image" / "public", fresh).returncode == 0
    assert not (fresh / "public").exists(), "the degrade must not create an empty library"

    empty = tmp_path / "image" / "public"
    empty.mkdir(parents=True)
    result = _seed(empty, root)
    assert result.returncode != 0, "an image without a single skill is the wrong image, not an older one"
    assert _tree(root) == before

    linked = tmp_path / "linked" / "public"
    _skill(linked, "alpha")
    (linked / "alpha" / "escape").symlink_to(tmp_path)
    result = _seed(linked, root)
    assert result.returncode != 0 and "symlink" in result.stderr
    assert _tree(root) == before


def test_seed_refuses_a_relative_or_empty_path(tmp_path: Path) -> None:
    source = tmp_path / "image" / "public"
    _skill(source, "alpha")
    for bad_source, bad_root in (("", tmp_path / "home" / "skills"), ("image/public", tmp_path / "home" / "skills"), (source, ""), (source, "home/skills"), (source, "/")):
        result = subprocess.run(["sh", str(SEED), str(bad_source), str(bad_root)], env={"PATH": os.environ["PATH"]}, cwd=tmp_path, capture_output=True, text=True, timeout=60, check=False)
        assert result.returncode == 2, (bad_source, bad_root, result.stderr)
    assert not (tmp_path / "home").exists()


def test_seed_creates_the_skills_root_and_takes_no_exclusions_by_default(tmp_path: Path) -> None:
    source = tmp_path / "image" / "public"
    _skill(source, "alpha")
    root = tmp_path / "home" / "skills"
    result = _seed(source, root)
    assert result.returncode == 0, result.stderr
    assert _tree(root / "public") == _tree(source)
    # The script pins its umask, so the modes are its own whatever the host's.
    assert (root / "public" / "alpha" / "SKILL.md").stat().st_mode & 0o777 == 0o644
    assert (root / "public" / "alpha").stat().st_mode & 0o777 == 0o755


def test_readme_counts_follow_the_tree() -> None:
    readme = (PROFILE / "README.md").read_text(encoding="utf-8")
    assert f"{len(_seeded_names())} skills are seeded at this release" in readme
    assert f"`EXCLUDED_PUBLIC_SKILLS`, {len(EXCLUDED)} at this release" in readme


# ── the real tree ────────────────────────────────────────────────────────────


def test_seed_copies_the_real_library_byte_for_byte_without_the_excluded_skill(tmp_path: Path) -> None:
    root = tmp_path / "home" / "skills"
    result = _seed(PUBLIC, root, *EXCLUDED)
    assert result.returncode == 0, result.stderr
    expected = {path: content for path, content in _tree(PUBLIC).items() if path.split("/", 1)[0] not in EXCLUDED}
    assert _tree(root / "public") == expected
    assert {path.name for path in (root / "public").iterdir()} == _seeded_names()
    assert "business-report" in _seeded_names()


@pytest.fixture
def projection_env(tmp_path: Path):
    """A seeded tenant skills root under a Gateway home, with the singletons
    the projection reads patched to it."""
    home = tmp_path / "home"
    skills_root = home / "skills"
    assert _seed(PUBLIC, skills_root, *EXCLUDED).returncode == 0
    (skills_root / "custom").mkdir()
    template = yaml.safe_load(TEMPLATE.read_text(encoding="utf-8"))
    paths = Paths(base_dir=home)
    config = SimpleNamespace(
        skills=SimpleNamespace(
            get_skills_path=lambda: skills_root,
            container_path=template["skills"]["container_path"],
            use="deerflow.skills.storage.local_skill_storage:LocalSkillStorage",
        )
    )
    extensions = ExtensionsConfig.model_validate(json.loads((PROFILE / "extensions_config.json").read_text(encoding="utf-8")))
    with (
        patch("deerflow.config.paths.get_paths", return_value=paths),
        patch("deerflow.config.get_app_config", return_value=config),
        patch("deerflow.config.extensions_config.ExtensionsConfig.from_file", return_value=extensions),
        patch("deerflow.config.extensions_config.get_extensions_config", return_value=extensions),
    ):
        yield SimpleNamespace(home=home, skills_root=skills_root, paths=paths, config=config, template=template)


def test_seeded_skills_project_to_the_sandbox_public_mount(projection_env) -> None:
    """Pins the ordinary-sandbox mount; a chat turn is an accepted session and never sees this path."""
    env = projection_env
    storage = LocalSkillStorage(host_path=str(env.skills_root), container_path=env.config.skills.container_path, app_config=env.config)

    projected = rebuild_skill_projections(storage)

    assert projected.public == env.paths.public_skills_view_dir
    assert {path.name for path in projected.public.iterdir()} == _seeded_names(), "the profile ships no disabled state, so every seeded skill is projected"
    assert (projected.public / "business-report" / "SKILL.md").read_bytes() == (PUBLIC / "business-report" / "SKILL.md").read_bytes()
    assert (projected.public / "business-report" / "scripts" / "report.py").is_file()
    assert not (projected.public / "chart-visualization").exists()

    mappings: list = []
    LocalSandboxProvider._append_public_skill_mapping(mappings, projected)
    assert len(mappings) == 1
    assert mappings[0].container_path == f"{env.template['skills']['container_path']}/public" == "/mnt/skills/public"
    assert mappings[0].local_path == str(projected.public)
    assert mappings[0].read_only is True


# ── the governed tool plane ──────────────────────────────────────────────────


def _review_blocks(package: Path) -> set[str]:
    """Error-or-blocker rule ids the profile's skill review raises on a package
    (the tool plane's review step, ``GovernedToolPlaneValidator._review_skill``,
    maps exactly those severities to a failed validation)."""
    facts = analyze_skill_package(LocalDirectoryReader(package).read())
    return {str(finding["rule_id"]) for finding in facts["findings"] if finding["severity"] in {"blocker", "error"}}


@pytest.mark.parametrize("name", EXCLUDED_BY_REVIEW)
def test_every_review_exclusion_is_still_refused_by_the_profiles_review(name: str) -> None:
    assert _review_blocks(PUBLIC / name), f"{name} now passes the review; drop it from EXCLUDED_PUBLIC_SKILLS in run.sh so the tenant gets it"


@pytest.mark.parametrize("name", sorted(EXCLUDED_BY_POLICY))
def test_policy_exclusions_are_not_review_refusals(name: str) -> None:
    assert not _review_blocks(PUBLIC / name), f"{name} is excluded by policy, not by the review; list it under EXCLUDED_BY_REVIEW instead"


_TENANT = TenantReferenceV1(version=1, public_ref="tenant-aaaaaaaaaaaaaaaa", digest="a" * 64)


def _admin() -> VerifiedActorContextV1:
    return VerifiedActorContextV1(
        identity=InvocationIdentityV1(effective_subject=EffectiveSubjectV1(kind="human", subject_id="admin-1", role="admin")),
        credential=CredentialEvidenceV1(
            method="session",
            credential_ref=None,
            effective_authority_digest=effective_authority_digest_v1(("tool_plane:admin",)),
            authority_categories=("tool_plane",),
        ),
        tenant=_TENANT,
    )


@pytest.mark.asyncio
async def test_the_governed_tool_plane_admits_the_seeded_library_and_a_reseed_is_not_drift(projection_env, monkeypatch: pytest.MonkeyPatch) -> None:
    """The profile keeps ``tool_plane.enabled: true`` with
    ``validation_requires_skill_review: true`` under the ``local_development``
    deployment profile: the seeded set is usable at once (this profile never
    fails readiness on governance state), and an administrator who governs it
    captures exactly the seeded bytes. A restart re-seeds the same bytes, which
    is not drift; a release that changes a skill is, and the same capture is
    the repair. The revision repository is substituted (the tenant's is the
    SQL one); the projection, artifact store, review and drift computation
    are the real ones."""
    env = projection_env
    template = env.template
    assert template["deployment"]["profile"] == "local_development"
    policy = template["tool_plane"]
    assert policy["enabled"] is True and policy["validation_requires_skill_review"] is True

    config_path = env.home / "extensions_config.json"
    shutil.copy(PROFILE / "extensions_config.json", config_path)
    monkeypatch.setenv("DEER_FLOW_EXTENSIONS_CONFIG_PATH", str(config_path))
    monkeypatch.setenv("DEER_FLOW_HOME", str(env.home))
    integrations_root = env.home / "integrations" / "skills"
    integrations_root.mkdir(parents=True)
    artifacts = GovernedSkillArtifactStore(env.home / "tool-plane" / "candidates")
    projection = LockedFileToolPlaneProjection(config_path=config_path, skills_root=env.skills_root, integrations_root=integrations_root, artifact_store=artifacts)
    repository = InMemoryToolPlaneRevisionRepository(tenant=_TENANT)
    service = ToolPlaneRevisionService(
        repository=repository,
        projection=projection,
        validator=GovernedToolPlaneValidator(
            policy_digest="b" * 64,
            artifact_store=artifacts,
            durable=False,
            allowed_mcp_transports=tuple(policy["allowed_mcp_transports"]),
            allowed_mcp_stdio_commands=tuple(policy["allowed_mcp_stdio_commands"]),
            allowed_mcp_endpoint_hosts=tuple(policy["allowed_mcp_endpoint_hosts"]),
            allow_private_mcp_endpoints=policy["allow_private_mcp_endpoints"],
            allowed_managed_integration_providers=tuple(policy["allowed_managed_integration_providers"]),
            forbidden_skill_capabilities=tuple(policy["forbidden_skill_capabilities"]),
            maximum_mcp_servers=policy["maximum_mcp_servers"],
            maximum_skills=policy["maximum_skills"],
            require_complete_review=policy["validation_requires_skill_review"],
        ),
        artifact_store=artifacts,
        durable=False,
    )
    admin = _admin()
    base = ToolPlaneRevisionScopeV1(kind="deployment_base")
    # The profile's seeded extensions_config.json is {"mcpServers":{},"skills":{}},
    # so the Gateway computes no pre-governance material to adopt: the status
    # is unmanaged, never bootstrap_required.
    await service.initialize(existing_projection=await projection.has_existing_projection())
    assert (await service.admin_status(base, admin)).governance_state == "unmanaged"
    assert await service.readiness_reason() is None, "local_development: an ungoverned library never blocks readiness"

    staged = await service.stage_current_projection(admin)
    record = await repository.get(staged.revision_id)
    assert record is not None
    captured = {entry["name"]: entry for entry in record.manifest["public_skills"]}
    assert set(captured) == _declared_names()
    assert len(captured) == len(_seeded_names())
    assert all(entry["enabled"] for entry in captured.values())

    report = await service.validate(staged.revision_id, admin)
    assert report.result == "passed", sorted((finding.severity, finding.code, finding.location or "") for finding in report.findings if finding.severity != "warning")
    warnings = {"resource.missing", "resource.unreferenced", "resource.escaping-link", "network-cleartext-http", "network-local-http", "shell-env-dump"}
    assert {finding.code for finding in report.findings} <= warnings, "the upstream library's review warnings; none blocks promotion"
    promoted = await service.promote(staged.revision_id, admin)
    assert promoted.state == "promoted"
    governed = await service.admin_status(base, admin)
    assert governed.governance_state == "governed" and governed.drift is False
    before = _tree(env.skills_root / "public")

    # A restart: run.sh seeds the same image bytes again.
    assert _seed(PUBLIC, env.skills_root, *EXCLUDED).returncode == 0
    assert _tree(env.skills_root / "public") == before
    after_restart = await service.admin_status(base, admin)
    assert after_restart.governance_state == "governed" and after_restart.drift is False
    assert await service.readiness_reason() is None

    # An upgrade to an image whose library changed.
    changed = env.home / "next-image" / "public"
    shutil.copytree(PUBLIC, changed)
    (changed / "business-report" / "SKILL.md").write_text((PUBLIC / "business-report" / "SKILL.md").read_text(encoding="utf-8") + "\nA later release.\n", encoding="utf-8")
    assert _seed(changed, env.skills_root, *EXCLUDED).returncode == 0
    drifted = await service.admin_status(base, admin)
    assert drifted.governance_state == "governed" and drifted.drift is True
    assert await service.readiness_reason() is None, "still usable; the notice is the operator's cue to capture again"

    recaptured = await service.stage_current_projection(admin)
    assert (await service.validate(recaptured.revision_id, admin)).result == "passed"
    assert (await service.promote(recaptured.revision_id, admin)).state == "promoted"
    repaired = await service.admin_status(base, admin)
    assert repaired.governance_state == "governed" and repaired.drift is False
    assert _tree(env.skills_root / "public") == {path: content for path, content in _tree(changed).items() if path.split("/", 1)[0] not in EXCLUDED}
