"""Contracts for the repository-owned restricted sandbox image."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SU_SHIM = REPO_ROOT / "docker/sandbox/su-shim.sh"
SANDBOX_DOCKERFILE = REPO_ROOT / "docker/sandbox/Dockerfile"
SANDBOX_SMOKE_WORKFLOW = REPO_ROOT / ".github/workflows/sandbox-image-smoke.yml"


def test_sandbox_dockerfile_pins_the_verified_base_and_non_root_user() -> None:
    dockerfile = SANDBOX_DOCKERFILE.read_text(encoding="utf-8")

    assert "ARG BASE_IMAGE=enterprise-public-cn-beijing.cr.volces.com/vefaas-public/all-in-one-sandbox@sha256:6328d7fd2f0ff0b4c147c3d05b3df1ce331f4a482eb6e550ecd64ed1fcf906e7" in dockerfile
    assert "FROM ${BASE_IMAGE}" in dockerfile
    assert "COPY --chmod=0755 su-shim.sh /usr/local/bin/su" in dockerfile
    assert "ENV BROWSER_NO_SANDBOX=--no-sandbox" in dockerfile
    assert dockerfile.rstrip().endswith("USER 1000:1000")


def test_every_vendor_substitution_is_asserted_before_it_runs() -> None:
    dockerfile = SANDBOX_DOCKERFILE.read_text(encoding="utf-8")
    asserted_substitutions = (
        (
            'grep -q \'chown "root:${USER}" "${RUN_DIR}"\'      $G',
            'sed -i \'s|chown "root:${USER}" "${RUN_DIR}"|chown "${USER}:${USER}" "${RUN_DIR}"|\'       $G',
        ),
        (
            'grep -q \'chown "root:${USER}" "${tmp_config}"\'   $G',
            'sed -i \'s|chown "root:${USER}" "${tmp_config}"|chown "${USER}:${USER}" "${tmp_config}"|\' $G',
        ),
        (
            "grep -q '^chown nobody /var/lib/nginx'           $G",
            "sed -i 's|^chown nobody /var/lib/nginx|chown $USER:$USER /var/lib/nginx|'                $G",
        ),
        (
            "grep -q 'chown nobody:root \"${NGINX_RUNTIME_DIR}\"' $G",
            'sed -i \'s|chown nobody:root "${NGINX_RUNTIME_DIR}"|chown $USER:$USER "${NGINX_RUNTIME_DIR}"|\' $G',
        ),
        (
            "grep -q 'chown nobody:root \"${NGINX_RUNTIME_DIR}/${temp_dir}\"' $G",
            'sed -i \'s|chown nobody:root "${NGINX_RUNTIME_DIR}/${temp_dir}"|chown $USER:$USER "${NGINX_RUNTIME_DIR}/${temp_dir}"|\' $G',
        ),
        (
            "grep -q '^user=root'                             $S",
            "sed -i 's|^user=root|user=%(ENV_USER)s|'                                                 $S",
        ),
        (
            "grep -q '^chown=root:%(ENV_USER)s'               $S",
            "sed -i 's|^chown=root:%(ENV_USER)s|chown=%(ENV_USER)s:%(ENV_USER)s|'                     $S",
        ),
        (
            "grep -q '^user=root'                             $N",
            "sed -i 's|^user=root|user=%(ENV_USER)s|'                                                 $N",
        ),
        (
            "grep -q 'os.chown(tmp_path, 0, gid)'             $B",
            "sed -i 's|os.chown(tmp_path, 0, gid)|os.chown(tmp_path, os.getuid(), gid)|'              $B",
        ),
        (
            "grep -q 'os.initgroups(username, gid)'           $B",
            "sed -i 's|os.initgroups(username, gid)|(os.initgroups(username, gid) if os.getuid() == 0 else None)|' $B",
        ),
    )

    for assertion, substitution in asserted_substitutions:
        assert dockerfile.index(assertion) < dockerfile.index(substitution)

    assert dockerfile.count("grep -q ") == len(asserted_substitutions)
    assert 'test "$(grep -cE \'chown "?(nobody|root)\' $G)" = 1' in dockerfile


def test_sandbox_smoke_runs_the_image_with_the_restricted_profile() -> None:
    workflow = SANDBOX_SMOKE_WORKFLOW.read_text(encoding="utf-8")

    assert "actions/checkout@df4cb1c069e1874edd31b4311f1884172cec0e10 # v6.0.3" in workflow
    assert "docker build --tag deer-flow-sandbox-smoke docker/sandbox" in workflow
    assert "--user 1000:1000" in workflow
    assert "--cap-drop=ALL" in workflow
    assert "--security-opt=no-new-privileges" in workflow
    assert "/v1/sandbox" in workflow
    assert "/v1/shell/exec" in workflow
    assert "/v1/bash/exec" in workflow
    assert "id -un" in workflow
    assert "jq --raw-output '.data.output'" in workflow
    assert "jq --raw-output '.data.stdout'" in workflow
    assert 'test "$shell_user" = "gem"' in workflow
    assert "permission denied|operation not permitted" in workflow


def test_su_shim_preserves_login_environment_and_stdin_for_same_uid(
    tmp_path: Path,
) -> None:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_getent = fake_bin / "getent"
    fake_getent.write_text(
        '#!/bin/sh\n[ "$1" = passwd ] || exit 2\nprintf \'gem:x:%s:%s::%s:/bin/bash\\n\' "$TEST_UID" "$TEST_GID" "$TEST_HOME"\n',
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
