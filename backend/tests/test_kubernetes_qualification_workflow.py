"""Contract for the opt-in live Kubernetes qualification workflow."""

from __future__ import annotations

from pathlib import Path

_WORKFLOW = Path(__file__).resolve().parents[2] / ".github/workflows/kubernetes-qualification.yml"
_HARDENED_SCOPE = "durable_one_replica_rwx_verified_copy_v2_nonempty_skill"


def test_live_workflow_selects_the_hardened_sandbox_qualification() -> None:
    workflow = _WORKFLOW.read_text(encoding="utf-8")

    for name in (
        "provisioner_image_repository",
        "provisioner_image_digest",
        "sandbox_image_repository",
        "sandbox_image_digest",
        "rwx_storage_class",
    ):
        assert f"      {name}:" in workflow
        assert f"${{{{ inputs.{name} }}}}" in workflow

    assert f'DEERFLOW_TEST_KUBERNETES_SCOPE: "{_HARDENED_SCOPE}"' in workflow
    assert "DEERFLOW_TEST_VERIFIER_IMAGE_REPOSITORY: ${{ inputs.provisioner_image_repository }}" in workflow
    assert "DEERFLOW_TEST_VERIFIER_IMAGE_DIGEST: ${{ inputs.provisioner_image_digest }}" in workflow
    assert "PYTHONPATH=. uv run pytest -m kubernetes_contract -v -s" in workflow
