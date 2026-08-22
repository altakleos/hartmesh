"""Contracts for the repository-owned restricted sandbox image."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SU_SHIM = REPO_ROOT / "docker/sandbox/su-shim.sh"


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

