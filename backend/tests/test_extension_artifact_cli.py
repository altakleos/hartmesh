from __future__ import annotations

import json
from pathlib import Path

from deerflow.extensions.artifacts import (
    ExtensionSourceLockV1,
    build_installed_artifact_manifest,
    canonical_platform_tag,
    write_artifact_manifest,
    write_source_lock,
)
from deerflow.extensions.cli import main


def _project_with_empty_manifest(tmp_path: Path, monkeypatch) -> tuple[Path, Path]:
    backend = tmp_path / "backend"
    backend.mkdir()
    (backend / "pyproject.toml").write_text(
        '[project]\nname = "fixture"\n\n[dependency-groups]\nextensions = []\n',
        encoding="utf-8",
    )
    (backend / "uv.lock").write_text("version = 1\n", encoding="utf-8")
    lock = ExtensionSourceLockV1.create(
        extension_api_version="0.13.0",
        entries=(),
    )
    source_lock_path = backend / "extensions.lock.json"
    write_source_lock(source_lock_path, lock)
    artifact_path = tmp_path / "hartmesh" / "extension-artifacts.json"
    write_artifact_manifest(
        artifact_path,
        build_installed_artifact_manifest(
            lock,
            platform_tag=canonical_platform_tag(),
        ),
    )
    monkeypatch.setenv("DEER_FLOW_PROJECT_ROOT", str(tmp_path))
    return source_lock_path, artifact_path


def test_verify_and_manifest_commands_are_read_only_and_non_secret(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    source_lock_path, artifact_path = _project_with_empty_manifest(tmp_path, monkeypatch)
    before = (source_lock_path.read_bytes(), artifact_path.read_bytes())

    assert main(["verify"]) == 0
    assert main(["manifest", "--json"]) == 0

    output = capsys.readouterr().out
    document = json.loads(output[output.index("{") :])
    assert document["version"] == 1
    assert document["entries"] == []
    assert (source_lock_path.read_bytes(), artifact_path.read_bytes()) == before


def test_config_digest_never_prints_secret_values(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    _project_with_empty_manifest(tmp_path, monkeypatch)
    config = tmp_path / "deployment.yaml"
    config.write_text(
        """\
plugins:
  - name: policy
    package: acme-policy
    use: acme_policy:install
    required: true
    config:
      api_key: super-secret-value
      endpoint: https://audit.example
""",
        encoding="utf-8",
    )

    assert main(["config-digest", "--config", str(config)]) == 0

    output = capsys.readouterr().out.strip()
    assert output.startswith("sha256:")
    assert "super-secret-value" not in output


def test_verify_failure_uses_only_a_stable_safe_code(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    _, artifact_path = _project_with_empty_manifest(tmp_path, monkeypatch)
    artifact_path.write_text('{"secret":"do-not-print"}\n', encoding="utf-8")

    assert main(["verify"]) == 1

    output = capsys.readouterr().err
    assert "extension_artifact_manifest_invalid" in output
    assert "do-not-print" not in output
    assert str(tmp_path) not in output


def test_manifest_reports_missing_local_artifact_and_json_falls_back_to_source_lock(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    source_lock_path, artifact_path = _project_with_empty_manifest(tmp_path, monkeypatch)
    artifact_path.unlink()
    expected_source_lock = json.loads(source_lock_path.read_text(encoding="utf-8"))

    assert main(["manifest"]) == 0
    status = capsys.readouterr().out
    assert "extension_artifact_manifest_missing" in status
    assert str(tmp_path) not in status

    assert main(["manifest", "--json"]) == 0
    assert json.loads(capsys.readouterr().out) == expected_source_lock
