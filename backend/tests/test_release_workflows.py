"""Static contracts for manually dispatched release identity workflows."""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
_CONTAINERS = _ROOT / ".github/workflows/container.yaml"
_MANIFEST = _ROOT / ".github/workflows/release-manifest.yaml"
_MIRROR = _ROOT / ".github/workflows/sandbox-image-mirror.yaml"
_SPELLINGS = _ROOT / "scripts/release_tag_spellings.sh"
_PINNED_ACTION = re.compile(r"uses: [^\s]+@[0-9a-f]{40} # v\d+(?:\.\d+)*")


def _top_level_block(workflow: str, start: str, end: str) -> str:
    return workflow.split(f"{start}:\n", 1)[1].split(f"\n{end}:", 1)[0]


def _input_names(workflow: str) -> set[str]:
    trigger = _top_level_block(workflow, "on", "permissions")
    inputs = trigger.split("    inputs:\n", 1)[1]
    return set(re.findall(r"^      ([a-z_]+):$", inputs, flags=re.MULTILINE))


def _input_body(workflow: str, name: str) -> str:
    trigger = _top_level_block(workflow, "on", "permissions")
    match = re.search(rf"^      {name}:\n(?P<body>(?:        .*\n)+)", trigger, flags=re.MULTILINE)
    assert match is not None
    return match.group("body")


def _assert_actions_are_pinned(workflow: str) -> None:
    action_lines = [line.strip() for line in workflow.splitlines() if "uses:" in line and "uses: ./" not in line]
    assert action_lines
    assert all(_PINNED_ACTION.fullmatch(line) for line in action_lines)


def test_release_builds_and_attests_four_version_gated_images() -> None:
    workflow = _CONTAINERS.read_text(encoding="utf-8")

    for component in ("backend", "frontend", "provisioner", "sandbox"):
        assert f"  {component}-container:" in workflow
        assert f"IMAGE_NAME: ${{{{ github.repository }}}}-{component}" in workflow
    assert workflow.count("needs: verify-versions") == 4
    assert workflow.count("docker/metadata-action@") == 4
    assert workflow.count("docker/build-push-action@") == 4
    assert workflow.count("actions/attest-build-provenance@") == 4
    assert "context: docker/sandbox" in workflow
    assert "file: docker/sandbox/Dockerfile" in workflow
    action_lines = [line.strip() for line in workflow.splitlines() if "uses:" in line and "uses: ./" not in line]
    assert action_lines
    assert all(re.fullmatch(r"uses: [^\s]+@[0-9a-f]{40} # ?v\d+(?:\.\d+)*", line) for line in action_lines)


def test_release_manifest_dispatch_contract_is_manual_and_minimal() -> None:
    workflow = _MANIFEST.read_text(encoding="utf-8")

    trigger = _top_level_block(workflow, "on", "permissions")
    assert _input_names(workflow) == {"version"}
    assert "        required: true\n" in _input_body(workflow, "version")
    assert "        type: string\n" in _input_body(workflow, "version")
    assert trigger.count("workflow_dispatch:") == 1
    assert all(event not in trigger for event in ("push:", "pull_request:", "schedule:", "workflow_call:"))
    assert _top_level_block(workflow, "permissions", "jobs").strip().splitlines() == [
        "contents: write",
        "  packages: read",
    ]
    _assert_actions_are_pinned(workflow)


def test_release_manifest_resolves_and_records_every_published_identity() -> None:
    workflow = _MANIFEST.read_text(encoding="utf-8")

    assert "ref: refs/tags/v${{ inputs.version }}" in workflow
    assert 'SHORT_SHA="${COMMIT:0:7}"' in workflow
    assert '"${IMAGE_REPOSITORY}:sha-${SHORT_SHA}"' in workflow
    assert "TAG_DIGEST" in workflow and "SHA_DIGEST" in workflow
    assert 'if [ "$TAG_DIGEST" != "$SHA_DIGEST" ]; then' in workflow
    assert "helm pull" in workflow and '--version "$VERSION"' in workflow
    assert "release-manifest.json" in workflow
    for field in (
        "schema",
        "version",
        "tag",
        "commit",
        "images",
        "chart",
        "repository",
        "digest",
        "oci_tag",
        "manifest_digest",
        "package_sha256",
    ):
        assert f'"{field}"' in workflow
    for component in ("backend", "frontend", "provisioner", "sandbox"):
        assert f'"{component}"' in workflow
    assert "actions/upload-artifact@" in workflow
    assert "gh release create" in workflow
    assert "gh release upload" in workflow and "--clobber" in workflow
    assert "scripts/release_tag_spellings.sh" in workflow
    assert "ghcr.io/${{ github.repository }}" in workflow
    assert 'OWNER="${REPOSITORY_PATH%%/*}"' in workflow
    assert "github.repository_owner" not in workflow
    assert "GITHUB_REPOSITORY_OWNER" not in workflow


def test_release_manifest_records_revision_check_without_requiring_sha_tag() -> None:
    workflow = _MANIFEST.read_text(encoding="utf-8")

    assert 'TAG_DIGEST="$(crane digest "$TAG_REFERENCE")"' in workflow
    assert 'if SHA_DIGEST="$(crane digest "$SHA_REFERENCE" 2>/dev/null)"; then' in workflow
    assert 'if [ "$TAG_DIGEST" != "$SHA_DIGEST" ]; then' in workflow
    assert 'REVISION_CHECK="verified"' in workflow
    assert 'echo "::warning::${SHA_REFERENCE} was not found' in workflow
    assert 'TAG_LIST_FILE="${RUNNER_TEMP}/${COMPONENT}-tags.txt"' in workflow
    assert 'if ! crane ls "$IMAGE_REPOSITORY" > "$TAG_LIST_FILE"; then' in workflow
    assert 'echo "::error::Unable to confirm whether ${SHA_REFERENCE} exists' in workflow
    assert 'head -50 "$TAG_LIST_FILE"' in workflow
    assert 'if grep -Fxq "sha-${SHORT_SHA}" "$TAG_LIST_FILE"; then' in workflow
    assert 'echo "::error::${SHA_REFERENCE} is listed but its digest could not be resolved"' in workflow
    assert 'REVISION_CHECK="tag-not-found"' in workflow
    assert 'printf \'%s_digest=%s\\n\' "$COMPONENT" "$TAG_DIGEST"' in workflow
    assert 'printf \'%s_revision_check=%s\\n\' "$COMPONENT" "$REVISION_CHECK"' in workflow
    assert '"revision_check": os.environ[f"{name.upper()}_REVISION_CHECK"]' in workflow
    for component in ("backend", "frontend", "provisioner", "sandbox"):
        assert f"{component}_revision_check" in workflow


def test_release_workflows_pass_registry_tokens_over_stdin() -> None:
    expected_login = 'printf \'%s\' "$GH_TOKEN" | crane auth login ghcr.io -u "$GITHUB_ACTOR" --password-stdin'

    for workflow_path in (_MANIFEST, _MIRROR):
        workflow = workflow_path.read_text(encoding="utf-8")
        assert " -p " not in workflow
        assert expected_login in workflow


def test_release_manifest_resolves_sandbox_as_a_built_image() -> None:
    workflow = _MANIFEST.read_text(encoding="utf-8")

    assert "for COMPONENT in backend frontend provisioner sandbox; do" in workflow
    assert 'for name in ("backend", "frontend", "provisioner", "sandbox")' in workflow
    assert "SANDBOX_PRESENT" not in workflow
    assert "inputs.sandbox" not in workflow
    assert 'manifest["sandbox"]' not in workflow
    assert '"sandbox"' in workflow
    assert '"schema": 2' in workflow
    assert "extension_artifact_manifest_digest" in workflow
    assert "extension_api_version" in workflow
    assert "extension_entry_count" in workflow
    assert "provenance_reference" in workflow
    assert 'BACKEND_DIGEST="$TAG_DIGEST"' in workflow
    assert "scripts/verify_release_manifest.py" in workflow


def test_release_manifest_verifies_the_pulled_chart_version() -> None:
    workflow = _MANIFEST.read_text(encoding="utf-8")

    extraction = workflow.index('tar -xzOf "$CHART_PACKAGE" deer-flow/Chart.yaml')
    assert 'grep -Fx "version: ${VERSION}"' in workflow
    assert 'echo "::error::pulled chart $CHART_PACKAGE does not declare requested version $VERSION"' in workflow
    assert extraction < workflow.index('PACKAGE_SHA256="$(sha256sum "$CHART_PACKAGE"')


def test_sandbox_mirror_dispatch_contract_is_manual_and_minimal() -> None:
    workflow = _MIRROR.read_text(encoding="utf-8")

    trigger = _top_level_block(workflow, "on", "permissions")
    assert _input_names(workflow) == {"source", "version"}
    for name in ("source", "version"):
        assert "        required: true\n" in _input_body(workflow, name)
        assert "        type: string\n" in _input_body(workflow, name)
    assert trigger.count("workflow_dispatch:") == 1
    assert all(event not in trigger for event in ("push:", "pull_request:", "schedule:", "workflow_call:"))
    assert _top_level_block(workflow, "permissions", "jobs").strip().splitlines() == [
        "contents: read",
        "  packages: write",
    ]
    _assert_actions_are_pinned(workflow)


def test_sandbox_mirror_rejects_floating_sources_before_network_and_verifies_copy() -> None:
    workflow = _MIRROR.read_text(encoding="utf-8")

    validation = workflow.index('if ! [[ "$SOURCE" =~ ^[^[:space:]@]+@sha256:')
    assert validation < workflow.index("actions/checkout@")
    assert validation < workflow.index("imjasonh/setup-crane@")
    assert "crane copy" in workflow
    assert 'if [ "$MIRROR_DIGEST" != "$SOURCE_DIGEST" ]; then' in workflow
    assert "scripts/release_tag_spellings.sh" in workflow
    assert "MIRROR_REPOSITORY: ghcr.io/${{ github.repository }}-sandbox-base" in workflow
    assert ":latest" not in workflow
    assert "value=latest" not in workflow


def test_only_the_release_container_workflow_publishes_the_sandbox_package() -> None:
    sandbox_publishers = []
    package_declaration = re.compile(
        r"^\s+(?:IMAGE_NAME: \$\{\{ github\.repository \}\}-sandbox|"
        r"MIRROR_REPOSITORY: ghcr\.io/\$\{\{ github\.repository \}\}-sandbox)\s*$",
        flags=re.MULTILINE,
    )

    for workflow_path in sorted((_ROOT / ".github/workflows").glob("*.y*ml")):
        workflow = workflow_path.read_text(encoding="utf-8")
        if package_declaration.search(workflow):
            sandbox_publishers.append(workflow_path.name)

    assert sandbox_publishers == ["container.yaml"]


def test_shared_release_spelling_script_is_the_only_substitution_implementation() -> None:
    result = subprocess.run(
        ["bash", str(_SPELLINGS), "2.1.0+hartmesh.7"],
        capture_output=True,
        text=True,
        check=False,
    )
    injected = subprocess.run(
        ["bash", str(_SPELLINGS), "2.1.0+hartmesh.7\nimage_tag=untrusted"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.splitlines() == [
        "image_tag=v2.1.0-hartmesh.7",
        "chart_oci_tag=2.1.0_hartmesh.7",
    ]
    assert injected.returncode == 1
    for workflow_path in (_MANIFEST, _MIRROR):
        workflow = workflow_path.read_text(encoding="utf-8")
        assert "//+" not in workflow
