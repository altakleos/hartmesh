from __future__ import annotations

from pathlib import Path

import pytest

from hartmesh_governance_extension.artifact_fixture import (
    build_template_artifact_manifest,
    verify_template_artifact_manifest,
)


def test_template_artifact_is_deterministic_and_detects_one_byte_tamper(
    tmp_path: Path,
) -> None:
    package = tmp_path / "package"
    package.mkdir()
    (package / "policy.py").write_bytes(b"POLICY = 1\n")
    (package / "audit.json").write_bytes(b"{}\n")
    files = ("policy.py", "audit.json")

    first = build_template_artifact_manifest(package, files)
    second = build_template_artifact_manifest(package, tuple(reversed(files)))

    assert first == second
    verify_template_artifact_manifest(package, first)
    (package / "policy.py").write_bytes(b"POLICY = 2\n")
    with pytest.raises(ValueError, match="digest mismatch"):
        verify_template_artifact_manifest(package, first)
