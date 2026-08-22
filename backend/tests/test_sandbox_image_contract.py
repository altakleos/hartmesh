"""Contracts for the repository-owned restricted sandbox image."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SU_SHIM = REPO_ROOT / "docker/sandbox/su-shim.sh"
SANDBOX_DOCKERFILE = REPO_ROOT / "docker/sandbox/Dockerfile"


def test_sandbox_dockerfile_pins_the_verified_base_and_non_root_user() -> None:
    dockerfile = SANDBOX_DOCKERFILE.read_text(encoding="utf-8")

    assert (
        "ARG BASE_IMAGE=enterprise-public-cn-beijing.cr.volces.com/vefaas-public/"
        "all-in-one-sandbox@sha256:"
        "6328d7fd2f0ff0b4c147c3d05b3df1ce331f4a482eb6e550ecd64ed1fcf906e7"
        in dockerfile
    )
    assert "FROM ${BASE_IMAGE}" in dockerfile
    assert "COPY --chmod=0755 su-shim.sh /usr/local/bin/su" in dockerfile
    assert "ENV BROWSER_NO_SANDBOX=--no-sandbox" in dockerfile
    assert dockerfile.rstrip().endswith("USER 1000:1000")


def test_su_shim_preserves_login_environment_and_stdin_for_same_uid(
    tmp_path: Path,
) -> None:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_getent = fake_bin / "getent"
    fake_getent.write_text(
        "#!/bin/sh\n"
        "[ \"$1\" = passwd ] || exit 2\n"
        "printf 'gem:x:%s:%s::%s:/bin/bash\\n' \"$TEST_UID\" \"$TEST_GID\" \"$TEST_HOME\"\n",
        encoding="utf-8",
    )
    fake_getent.chmod(0o755)

    home = tmp_path / "home"
    home.mkdir()
    env = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ['PATH']}",
        "TEST_UID": str(os.getuid()),
        "TEST_GID": str(os.getgid()),
        "TEST_HOME": str(home),
    }

    result = subprocess.run(
        [
            "bash",
            str(SU_SHIM),
            "-",
            "gem",
            "-c",
            'read -r value; printf "%s|%s|%s|%s" "$value" "$HOME" "$USER" "$PWD"',
        ],
        input="stdin-preserved\n",
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout == f"stdin-preserved|{home}|gem|{home}"
