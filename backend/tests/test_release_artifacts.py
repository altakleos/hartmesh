"""Release adoption verifies candidate bytes before any registry mutation."""

from __future__ import annotations

import importlib.util
import io
import json
import os
import subprocess
import sys
import urllib.error
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "scripts/release_artifacts.py"
VERSION = "2.2.0+hartmesh.39"
IMAGE_TAG = "v2.2.0-hartmesh.39"
REPOSITORY = "altakleos/hartmesh"
DIGEST = "sha256:" + "a" * 64


def git(root: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-c", "gc.auto=0", *args],
        cwd=root,
        check=True,
        text=True,
        capture_output=True,
    ).stdout.strip()


def commit(root: Path, changes: dict[str, str]) -> str:
    for name, content in changes.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
    git(root, "add", ".")
    git(root, "commit", "-qm", "fixture")
    return git(root, "rev-parse", "HEAD")


@pytest.fixture
def checkout(tmp_path: Path) -> Path:
    root = tmp_path / "checkout"
    root.mkdir()
    git(root, "init", "-q")
    git(root, "config", "user.email", "fixture@example.invalid")
    git(root, "config", "user.name", "Fixture")
    changes = {
        "backend/Dockerfile": "COPY backend /app/backend\nCOPY skills/public /app/skills/public\n",
        "backend/app.py": "print('candidate')\n",
        "frontend-hm/Dockerfile": "COPY frontend-hm /app/frontend\n",
        "frontend-hm/src/app.ts": "export const value = 1;\n",
        "docker/provisioner/Dockerfile": "COPY app.py /app/app.py\n",
        "docker/provisioner/app.py": "print('provisioner')\n",
        "docker/sandbox/Dockerfile": "COPY su-shim.sh /bin/su\n",
        "docker/sandbox/su-shim.sh": "#!/bin/sh\nexit 0\n",
        "docker/sandbox-network-proxy/Dockerfile": "COPY backend/packages/harness/deerflow/community/aio_sandbox/network_proxy.py /app.py\n",
        "backend/packages/harness/deerflow/community/aio_sandbox/network_proxy.py": "print('proxy')\n",
        "skills/public/example/SKILL.md": "# A shipped skill\n",
        ".dockerignore": "panel/\n",
        ".github/workflows/container.yaml": (REPO / ".github/workflows/container.yaml").read_text(encoding="utf-8"),
        "scripts/release_tag_spellings.sh": "# fixture\n",
        "scripts/verify_versions.sh": "# fixture\n",
        ".github/workflows/verify-versions.yml": "# fixture\n",
        "deploy/compose/images.txt": "".join(f"ghcr.io/{REPOSITORY}-{component}@{DIGEST}\n" for component in ("backend", "frontend", "sandbox", "sandbox-network-proxy")),
    }
    if SCRIPT.exists():
        changes["scripts/release_artifacts.py"] = SCRIPT.read_text(encoding="utf-8")
    commit(root, changes)
    return root


def run_script(checkout: Path, *args: str, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args],
        cwd=checkout,
        env=env,
        text=True,
        capture_output=True,
    )


def fingerprint(checkout: Path, component: str, revision: str = "HEAD") -> str:
    result = run_script(checkout, "inputs", "--component", component, "--revision", revision)
    assert result.returncode == 0, result.stderr
    return result.stdout.strip()


@pytest.fixture
def registry(tmp_path: Path) -> tuple[dict[str, str], Path]:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    calls = tmp_path / "registry-calls.jsonl"
    double = (
        f"#!{sys.executable}\n"
        + """import json, os, pathlib, sys
with open(os.environ["TEST_CALLS"], "a", encoding="utf-8") as out:
    out.write(json.dumps([pathlib.Path(sys.argv[0]).name, *sys.argv[1:]]) + "\\n")
command = sys.argv[1]
if pathlib.Path(sys.argv[0]).name == "gh":
    sys.exit(int(os.environ.get("TEST_ATTESTATION_EXIT", "0")))
if command == "digest":
    if ":sha-" in sys.argv[-1] and os.environ.get("TEST_FAIL_SHA"):
        sys.exit(1)
    print(os.environ["TEST_DIGEST"])
elif command == "ls":
    print("v2.2.0-hartmesh.39")
elif command == "manifest":
    print(json.dumps({"mediaType": "application/vnd.oci.image.manifest.v1+json"}))
elif command == "config":
    labels = json.loads(os.environ["TEST_LABELS"])
    print(json.dumps({"config": {"Labels": labels.get(sys.argv[-1], labels)}}))
elif command not in {"tag", "auth"}:
    sys.exit(9)
"""
    )
    for command in ("crane", "gh"):
        path = bin_dir / command
        path.write_text(double, encoding="utf-8")
        path.chmod(0o755)
    return {
        **os.environ,
        "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
        "TEST_CALLS": str(calls),
        "TEST_DIGEST": DIGEST,
        "TEST_LABELS": "{}",
    }, calls


def labels(checkout: Path, component: str = "backend") -> dict[str, str]:
    return {
        "org.opencontainers.image.source": f"https://github.com/{REPOSITORY}",
        "org.opencontainers.image.revision": git(checkout, "rev-parse", "HEAD"),
        "io.hartmesh.build-inputs": fingerprint(checkout, component),
        "io.hartmesh.release-version": VERSION,
    }


def adoption(checkout: Path, env: dict[str, str], component: str = "backend") -> subprocess.CompletedProcess[str]:
    workflow = yaml.safe_load((REPO / ".github/workflows/container.yaml").read_text(encoding="utf-8"))
    step = next(step for step in workflow["jobs"]["container"]["steps"] if step.get("id") == "adopt")
    return subprocess.run(
        ["bash", "-c", step["run"]],
        cwd=checkout,
        env={
            **env,
            "GITHUB_EVENT_NAME": "push",
            "GITHUB_REPOSITORY": REPOSITORY,
            "GITHUB_SHA": git(checkout, "rev-parse", "HEAD"),
            "GITHUB_ACTOR": "fixture",
            "GITHUB_OUTPUT": str(checkout / "output"),
            "GITHUB_STEP_SUMMARY": str(checkout / "summary"),
            "GH_TOKEN": "offline-fixture",
            "REGISTRY": "ghcr.io",
            "IMAGE_NAME": f"{REPOSITORY}-{component}",
            "IMAGE_TAG": IMAGE_TAG,
            "SHORT_SHA": git(checkout, "rev-parse", "--short=7", "HEAD"),
            "COMPONENT": component,
            "RELEASE_VERSION": VERSION,
            "CANDIDATES": json.dumps({component: f"ghcr.io/{REPOSITORY}-{component}@{DIGEST}"}),
        },
        capture_output=True,
        text=True,
    )


def recorded(calls: Path) -> list[list[str]]:
    return [json.loads(line) for line in calls.read_text(encoding="utf-8").splitlines()] if calls.exists() else []


def test_tag_adoption_refuses_unverified_candidate_before_any_retag(checkout: Path, registry: tuple[dict[str, str], Path]) -> None:
    env, calls = registry
    result = adoption(checkout, env)
    assert result.returncode != 0, "matching release-tag/pin digests alone cannot prove the candidate's source"
    assert not any(call[:2] == ["crane", "tag"] for call in recorded(calls))


@pytest.mark.parametrize(
    "component",
    ["backend", "frontend", "sandbox", "sandbox-network-proxy", "provisioner"],
)
def test_pin_and_release_notes_commit_can_adopt_an_earlier_verified_candidate(checkout: Path, registry: tuple[dict[str, str], Path], component: str) -> None:
    env, calls = registry
    candidate = labels(checkout, component)
    commit(
        checkout,
        {
            "CHANGELOG.md": "Release notes and schema statement\n",
            "deploy/compose/config.yaml": "sandbox: pinned\n",
            "docs/release.md": "metadata\n",
        },
    )
    result = adoption(checkout, {**env, "TEST_LABELS": json.dumps(candidate)}, component)
    assert result.returncode == 0, result.stderr
    commands = recorded(calls)
    verification = next(i for i, call in enumerate(commands) if call[:3] == ["gh", "attestation", "verify"])
    retags = [i for i, call in enumerate(commands) if call[:2] == ["crane", "tag"]]
    assert len(retags) == 2 and verification < min(retags)
    assert "adopted=true" in (checkout / "output").read_text(encoding="utf-8")
    assert commands[verification][3] == f"oci://ghcr.io/{REPOSITORY}-{component}@{DIGEST}"
    assert commands[verification][commands[verification].index("--source-digest") + 1] == candidate["org.opencontainers.image.revision"]
    assert commands[verification][commands[verification].index("--repo") + 1] == REPOSITORY
    assert commands[verification][commands[verification].index("--signer-workflow") + 1] == f"{REPOSITORY}/.github/workflows/container.yaml"


@pytest.mark.parametrize(
    ("component", "path"),
    [
        ("backend", "backend/app.py"),
        ("backend", "skills/public/example/SKILL.md"),
        ("backend", ".dockerignore"),
        ("backend", "backend/Dockerfile.dockerignore"),
        ("frontend", "frontend-hm/src/app.ts"),
        ("frontend", "frontend-hm/Dockerfile.dockerignore"),
        ("sandbox", "docker/sandbox/su-shim.sh"),
        ("sandbox", "docker/sandbox/.dockerignore"),
        ("provisioner", "docker/provisioner/app.py"),
        (
            "sandbox-network-proxy",
            "backend/packages/harness/deerflow/community/aio_sandbox/network_proxy.py",
        ),
        ("sandbox-network-proxy", "docker/sandbox-network-proxy/Dockerfile"),
        ("frontend", ".github/workflows/container.yaml"),
        ("backend", "scripts/release_artifacts.py"),
    ],
)
def test_changed_build_input_refuses_adoption(checkout: Path, registry: tuple[dict[str, str], Path], component: str, path: str) -> None:
    env, calls = registry
    candidate = labels(checkout, component)
    original = (checkout / path).read_text(encoding="utf-8") if (checkout / path).exists() else ""
    commit(checkout, {path: original + "\n# changed input\n"})
    result = adoption(checkout, {**env, "TEST_LABELS": json.dumps(candidate)}, component)
    assert result.returncode != 0
    assert "build inputs" in result.stderr
    assert not any(call[:2] == ["crane", "tag"] for call in recorded(calls))


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("org.opencontainers.image.source", "https://github.com/another/repo"),
        ("org.opencontainers.image.revision", "not-a-sha"),
        ("org.opencontainers.image.revision", "0" * 40),
        ("io.hartmesh.build-inputs", "b" * 64),
        ("io.hartmesh.release-version", "2.2.0+hartmesh.38"),
    ],
)
def test_wrong_candidate_identity_or_input_label_refuses_adoption(checkout: Path, registry: tuple[dict[str, str], Path], field: str, value: str) -> None:
    env, calls = registry
    candidate = labels(checkout)
    candidate[field] = value
    result = adoption(checkout, {**env, "TEST_LABELS": json.dumps(candidate)})
    assert result.returncode != 0
    assert not any(call[:2] == ["crane", "tag"] for call in recorded(calls))


def test_failed_attestation_refuses_adoption(checkout: Path, registry: tuple[dict[str, str], Path]) -> None:
    env, calls = registry
    result = adoption(
        checkout,
        {
            **env,
            "TEST_LABELS": json.dumps(labels(checkout)),
            "TEST_ATTESTATION_EXIT": "1",
        },
    )
    assert result.returncode != 0
    assert not any(call[:2] == ["crane", "tag"] for call in recorded(calls))


@pytest.mark.parametrize("bad_component", [None, "sandbox-network-proxy"])
def test_shared_preflight_verifies_every_image_and_emits_no_partial_candidates(checkout: Path, registry: tuple[dict[str, str], Path], bad_component: str | None) -> None:
    env, calls = registry
    candidates = {
        f"ghcr.io/{REPOSITORY}-{component}@{DIGEST}": labels(checkout, component)
        for component in (
            "backend",
            "frontend",
            "sandbox",
            "sandbox-network-proxy",
            "provisioner",
        )
    }
    if bad_component:
        candidates[f"ghcr.io/{REPOSITORY}-{bad_component}@{DIGEST}"]["io.hartmesh.build-inputs"] = "b" * 64
    result = run_script(
        checkout,
        "verify-release",
        "--repository",
        REPOSITORY,
        "--version",
        VERSION,
        env={**env, "TEST_LABELS": json.dumps(candidates)},
    )
    assert not any(call[:2] == ["crane", "tag"] for call in recorded(calls))
    if bad_component:
        assert result.returncode != 0 and result.stdout == ""
    else:
        assert result.returncode == 0, result.stderr
        assert set(json.loads(result.stdout).values()) == set(candidates)
        assert len([call for call in recorded(calls) if call[:3] == ["gh", "attestation", "verify"]]) == 5


def test_new_docker_copy_source_requires_fingerprint_coverage(checkout: Path) -> None:
    commit(
        checkout,
        {
            "frontend-hm/Dockerfile": "COPY frontend-hm /app/frontend\nCOPY new-input /app/input\n",
            "new-input/data.json": "{}\n",
        },
    )
    result = run_script(checkout, "inputs", "--component", "frontend")
    assert result.returncode != 0 and "outside the fingerprint policy" in result.stderr


@pytest.mark.parametrize("mount", ["type=bind,source=.,target=/source", "source=.,target=/source", "type=unknown,target=/source"])
def test_run_context_mount_requires_explicit_fingerprint_coverage(checkout: Path, mount: str) -> None:
    commit(checkout, {"frontend-hm/Dockerfile": f"COPY frontend-hm /app/frontend\nRUN --mount={mount} cat /source/new-input/data.json\n"})
    result = run_script(checkout, "inputs", "--component", "frontend")
    assert result.returncode != 0 and "mount" in result.stderr


def test_cache_mount_does_not_add_source_inputs(checkout: Path) -> None:
    commit(checkout, {"backend/Dockerfile": "COPY backend /app/backend\nRUN --mount=type=cache,target=/root/.cache/uv uv sync\n"})
    assert len(fingerprint(checkout, "backend")) == 64


@pytest.mark.parametrize(
    "version",
    [
        "v2.2.0+hartmesh.39",
        "2.2.0-hartmesh.39",
        "2.2.0+hartmesh.039",
        "2.2.0-rc1+hartmesh.39",
        "2.2.0",
    ],
)
def test_dispatch_rejects_noncanonical_versions_before_external_calls(monkeypatch: pytest.MonkeyPatch, version: str) -> None:
    guard = load_guard()

    def unexpected(*_args, **_kwargs):
        pytest.fail("noncanonical versions must fail before any external lookup")

    monkeypatch.setattr(guard.subprocess, "run", unexpected)
    with pytest.raises(guard.ReleaseError, match="canonical"):
        guard.check_unpublished(REPOSITORY, version)


def load_guard():
    spec = importlib.util.spec_from_file_location("release_artifacts", SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize(
    ("tag_status", "http_status", "allowed"),
    [
        (2, 404, True),
        (0, 404, False),
        (2, 200, False),
        (128, 404, False),
        (2, 403, False),
        (2, 500, False),
    ],
)
def test_dispatch_guard_distinguishes_absence_from_published_versions_and_service_errors(monkeypatch: pytest.MonkeyPatch, tag_status: int, http_status: int, allowed: bool) -> None:
    guard = load_guard()
    commands = []

    def run(args, **kwargs):
        commands.append(args)
        return subprocess.CompletedProcess(args, tag_status, stdout="", stderr="offline fixture")

    def urlopen(request, **kwargs):
        assert request.full_url == f"https://api.github.com/repos/{REPOSITORY}/releases/tags/v{VERSION}"
        assert request.get_header("Authorization") == "Bearer offline-fixture"
        if http_status != 200:
            raise urllib.error.HTTPError(request.full_url, http_status, "fixture", {}, None)
        return io.BytesIO(b'{"tag_name":"v2.2.0+hartmesh.39"}')

    monkeypatch.setattr(guard.subprocess, "run", run)
    monkeypatch.setattr(guard.urllib.request, "urlopen", urlopen)
    monkeypatch.setenv("GH_TOKEN", "offline-fixture")
    if allowed:
        guard.check_unpublished(REPOSITORY, VERSION)
    else:
        with pytest.raises(guard.ReleaseError):
            guard.check_unpublished(REPOSITORY, VERSION)
    assert commands[0] == [
        "git",
        "ls-remote",
        "--exit-code",
        "origin",
        f"refs/tags/v{VERSION}",
    ]


def test_workflow_serializes_versions_and_gates_builds_before_publication() -> None:
    workflow = yaml.safe_load((REPO / ".github/workflows/container.yaml").read_text(encoding="utf-8"))
    assert workflow["concurrency"]["cancel-in-progress"] is False
    assert "inputs.version" in workflow["concurrency"]["group"] and "github.ref_name" in workflow["concurrency"]["group"]
    assert "release-policy" in workflow["jobs"]["container"]["needs"]
    policy = workflow["jobs"]["release-policy"]
    assert any("check-unpublished" in step.get("run", "") for step in policy["steps"])
    assert any("verify-release" in step.get("run", "") for step in policy["steps"])
    steps = workflow["jobs"]["container"]["steps"]
    build = next(step for step in steps if step.get("id") == "push")
    assert build["if"] == "github.event_name == 'workflow_dispatch'"
    metadata = next(step for step in steps if step.get("id") == "meta")
    assert "io.hartmesh.build-inputs=" in metadata["with"]["labels"]
    assert "io.hartmesh.release-version=" in metadata["with"]["labels"]
    adopt = next(step for step in steps if step.get("id") == "adopt")
    assert adopt["env"]["CANDIDATES"] == "${{ needs.release-policy.outputs.candidates }}"
    manifest = yaml.safe_load((REPO / ".github/workflows/release-manifest.yaml").read_text(encoding="utf-8"))
    assert manifest["concurrency"]["cancel-in-progress"] is False
    assert manifest["concurrency"]["group"] == "hartmesh-release-${{ github.repository }}-v${{ inputs.version }}"
    manifest_steps = "\n".join(step.get("run", "") for step in manifest["jobs"]["release-manifest"]["steps"])
    assert manifest_steps.index('bash scripts/verify_versions.sh "$VERSION"') < manifest_steps.index("verify-release")
    assert manifest_steps.index("verify-release") < manifest_steps.index("gh release")


def test_workflow_matrix_matches_the_fingerprint_context_policy() -> None:
    guard = load_guard()
    workflow = yaml.safe_load((REPO / ".github/workflows/container.yaml").read_text(encoding="utf-8"))
    matrix = workflow["jobs"]["container"]["strategy"]["matrix"]["include"]
    assert {entry["component"]: (entry["context"], entry["file"]) for entry in matrix} == guard.BUILD_CONTEXTS


@pytest.mark.parametrize("missing_alias", [False, True])
def test_manifest_requires_all_final_commit_aliases(checkout: Path, registry: tuple[dict[str, str], Path], missing_alias: bool) -> None:
    env, _calls = registry
    workflow = yaml.safe_load((REPO / ".github/workflows/release-manifest.yaml").read_text(encoding="utf-8"))
    step = next(step for step in workflow["jobs"]["release-manifest"]["steps"] if step.get("id") == "resolve")
    components = ("backend", "frontend", "provisioner", "sandbox", "sandbox-network-proxy")
    result = subprocess.run(
        ["bash", "-c", step["run"]],
        cwd=checkout,
        env={
            **env,
            "TEST_FAIL_SHA": "1" if missing_alias else "",
            "IMAGE_BASE": f"ghcr.io/{REPOSITORY}",
            "IMAGE_TAG": IMAGE_TAG,
            "SHORT_SHA": "1234567",
            "GITHUB_OUTPUT": str(checkout / "manifest-output"),
            "RUNNER_TEMP": str(checkout),
            "CANDIDATES": json.dumps({component: f"ghcr.io/{REPOSITORY}-{component}@{DIGEST}" for component in components}),
        },
        text=True,
        capture_output=True,
    )
    if missing_alias:
        assert result.returncode != 0
    else:
        assert result.returncode == 0, result.stderr
        output = (checkout / "manifest-output").read_text(encoding="utf-8")
        assert output.count("_revision_check=verified") == 5
