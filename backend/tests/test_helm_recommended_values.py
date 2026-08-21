"""Render contract for the recommended Kubernetes values in the chart README."""

from __future__ import annotations

from pathlib import Path

from support.helm import container_env, find_rendered_object, lint_chart, render_chart

_REPO_ROOT = Path(__file__).resolve().parents[2]
_README = _REPO_ROOT / "deploy" / "helm" / "deer-flow" / "README.md"
_START = "<!-- recommended-kubernetes-values:start -->"
_END = "<!-- recommended-kubernetes-values:end -->"


def _recommended_values(tmp_path: Path) -> Path:
    section = _README.read_text(encoding="utf-8").split(_START, 1)[1].split(_END, 1)[0].strip()
    assert section.startswith("```yaml\n") and section.endswith("```")
    path = tmp_path / "recommended-values.yaml"
    path.write_text(section.removeprefix("```yaml\n").removesuffix("```").strip() + "\n", encoding="utf-8")
    return path


def test_readme_recommended_values_render_and_lint(tmp_path: Path) -> None:
    values = _recommended_values(tmp_path)
    documents = render_chart("--values", str(values))
    lint_chart("--values", str(values))

    provisioner = find_rendered_object(documents, "Deployment", component="provisioner")
    environment = container_env(provisioner)
    claim_names = {name for name in ("USERDATA_PVC_NAME", "SKILLS_PVC_NAME") if name in environment}
    assert (environment.get("SANDBOX_VOLUME_MODE") == "pvc" and claim_names == {"USERDATA_PVC_NAME", "SKILLS_PVC_NAME"}) or ("SANDBOX_VOLUME_MODE" not in environment and not claim_names)
