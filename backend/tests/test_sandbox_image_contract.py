"""Contracts for the repository-owned restricted sandbox image."""

from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SU_SHIM = REPO_ROOT / "docker/sandbox/su-shim.sh"
SANDBOX_DOCKERFILE = REPO_ROOT / "docker/sandbox/Dockerfile"
SANDBOX_SMOKE_WORKFLOW = REPO_ROOT / ".github/workflows/sandbox-image-smoke.yml"
TENANT_TEMPLATE = REPO_ROOT / "deploy/compose/config.yaml"
TENANT_COMPOSE = REPO_ROOT / "deploy/compose/compose.yaml"


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


DOCUMENT_LIBRARY_LAYER = (
    "RUN set -eux; \\\n    pip install --no-cache-dir --no-deps --disable-pip-version-check \\\n        python-docx==1.2.0 duckdb==1.5.5; \\\n    python3 -c 'import docx, duckdb, xlrd, weasyprint, xlsxwriter, matplotlib, pandas, openpyxl'"
)


def test_sandbox_dockerfile_ships_the_document_libraries_pinned() -> None:
    dockerfile = SANDBOX_DOCKERFILE.read_text(encoding="utf-8")

    assert DOCUMENT_LIBRARY_LAYER in dockerfile
    assert dockerfile.count("pip install") == 1
    assert dockerfile.index("USER 0") < dockerfile.index(DOCUMENT_LIBRARY_LAYER) < dockerfile.index("USER 1000:1000")


def test_sandbox_smoke_imports_the_document_libraries_as_the_runtime_user() -> None:
    dockerfile = SANDBOX_DOCKERFILE.read_text(encoding="utf-8")
    workflow = SANDBOX_SMOKE_WORKFLOW.read_text(encoding="utf-8")
    duckdb_pin = re.search(r"duckdb==([0-9.]+)", dockerfile).group(1)
    import_payload = "import docx, duckdb, xlrd, weasyprint, xlsxwriter, matplotlib, pandas, openpyxl; print(duckdb.__version__)"

    assert import_payload in workflow
    assert workflow.index("/v1/bash/exec", workflow.index(import_payload)) < workflow.index("library_version=")
    assert f'[ "$library_version" != "{duckdb_pin}" ]' in workflow
    # The endpoint answers 200 whatever the command's exit code.
    # library import, data-analysis run, font cache, business-report run
    assert workflow.count("jq -e '.data.exit_code == 0'") == 6
    assert "printf '%s\\n' \"$library_response\"" in workflow


def test_sandbox_smoke_runs_the_data_analysis_script_on_the_built_image() -> None:
    workflow = SANDBOX_SMOKE_WORKFLOW.read_text(encoding="utf-8")

    # Bind-mounted at start rather than copied in later: a gVisor sandbox does not see files copied into a running container.
    assert '--volume "$PWD/skills/public/data-analysis/scripts/analyze.py:/mnt/smoke/analyze.py:ro"' in workflow
    assert '--volume "$PWD/backend/tests/skills/data_analysis/fixtures/example_orders.xls:/mnt/smoke/example_orders.xls:ro"' in workflow
    assert "python3 /mnt/smoke/analyze.py --files /mnt/smoke/example_orders.xls --action inspect" in workflow
    assert "grep -q 'Rows: 12'" in workflow
    assert (REPO_ROOT / "backend/tests/skills/data_analysis/fixtures/example_orders.xls").is_file()


FONT_CACHE_LAYER = (
    'ENV MPLCONFIGDIR=/opt/aio/matplotlib\nRUN set -eux; \\\n    mkdir -p "$MPLCONFIGDIR"; \\\n    cp /opt/gem/matplotlibrc "$MPLCONFIGDIR/matplotlibrc"; \\\n'
    '    python3 -c \'import matplotlib.pyplot\'; \\\n    chown -R 1000:1000 "$MPLCONFIGDIR"; \\\n    test -n "$(ls "$MPLCONFIGDIR"/fontlist-*.json)"'
)


def test_sandbox_dockerfile_prebuilds_the_font_cache_for_the_runtime_user() -> None:
    dockerfile = SANDBOX_DOCKERFILE.read_text(encoding="utf-8")

    assert FONT_CACHE_LAYER in dockerfile
    # After the libraries it caches, before the switch to the runtime user that owns the result.
    assert dockerfile.index(DOCUMENT_LIBRARY_LAYER) < dockerfile.index(FONT_CACHE_LAYER) < dockerfile.index("USER 1000:1000")


def test_sandbox_smoke_checks_the_font_cache_as_the_runtime_user() -> None:
    workflow = SANDBOX_SMOKE_WORKFLOW.read_text(encoding="utf-8")

    # The vendor entrypoint wipes ~/.cache/matplotlib at start; the cache must be found at MPLCONFIGDIR instead.
    assert 'ls \\"$MPLCONFIGDIR\\"/fontlist-*.json && python3 -c' in workflow
    # The cache directory in use is asserted (a read-only MPLCONFIGDIR makes matplotlib fall back to a temp dir silently).
    assert "assert matplotlib.get_cachedir() == os.environ[" in workflow
    assert "grep -Eqi 'building the font cache|temporary cache directory'" in workflow
    assert "printf '%s\\n' \"$font_response\"" in workflow


def test_sandbox_smoke_builds_and_renders_a_business_report_on_the_built_image() -> None:
    workflow = SANDBOX_SMOKE_WORKFLOW.read_text(encoding="utf-8")

    assert '--volume "$PWD/skills/public/business-report:/mnt/smoke/business-report:ro"' in workflow
    assert '--volume "$PWD/backend/tests/skills/business_report/fixtures/example_services_export_small.xls:/mnt/smoke/example_services_export_small.xls:ro"' in workflow
    assert '--volume "$PWD/backend/tests/skills/business_report/check_pdf_on_image.py:/mnt/smoke/check_pdf_on_image.py:ro"' in workflow
    # The command SKILL.md asks for, verbatim: one build that renders all three
    # formats. A smoke test that issues a shape the doc no longer teaches proves
    # the image agrees with a path the tenant does not take.
    assert "python3 $R build /mnt/smoke/example_services_export_small.xls --period 2026-08 --out /tmp/smoke-report --render pdf,docx,xlsx" in workflow
    assert "python3 /mnt/smoke/check_pdf_on_image.py /tmp/smoke-report/2026-08-business-review.pdf" in workflow
    assert "grep -q 'Checks: Totals match your file'" in workflow
    assert "printf '%s\\n' \"$report_response\"" in workflow
    assert (REPO_ROOT / "backend/tests/skills/business_report/fixtures/example_services_export_small.xls").is_file()
    assert (REPO_ROOT / "backend/tests/skills/business_report/check_pdf_on_image.py").is_file()


def test_sandbox_smoke_runs_when_a_public_skill_script_changes() -> None:
    workflow = SANDBOX_SMOKE_WORKFLOW.read_text(encoding="utf-8")

    # Both the push and the pull_request trigger list the skill paths the job exercises.
    for path in ('"skills/public/data-analysis/**"', '"skills/public/business-report/**"', '"backend/tests/skills/**"'):
        assert workflow.count(path) == 2, path


def test_sandbox_smoke_runs_the_slim_services_profile_at_the_tenant_limits() -> None:
    """The tenant profile ships every sandbox with the image's DISABLE_* switches
    and half the memory the full profile needed. Nothing in the tree owns the
    vendor entrypoint that reads those switches, so the smoke job must prove
    the built image still honours them at exactly the shipped limits: ready,
    none of the switched-off services running, the report path rendering, no
    OOM kill. A base-image bump that ignored one switch would otherwise put
    the full profile inside a 512 MiB cgroup on every tenant."""
    import yaml

    workflow = SANDBOX_SMOKE_WORKFLOW.read_text(encoding="utf-8")
    switches = yaml.safe_load(TENANT_TEMPLATE.read_text(encoding="utf-8"))["sandbox"]["environment"]
    gateway = yaml.safe_load(TENANT_COMPOSE.read_text(encoding="utf-8"))["services"]["gateway"]["environment"]
    assert len(switches) == 6 and all(value == "true" for value in switches.values())
    slim = workflow[workflow.index("Exercise the slim services profile") :]
    for key, value in switches.items():
        assert f"--env {key}={value}" in slim, key
    memory = gateway["DEER_FLOW_SANDBOX_MEMORY"]
    assert f"--memory {memory} --memory-swap {memory}" in slim, "the limit the tenant profile ships"
    assert f"--pids-limit {gateway['DEER_FLOW_SANDBOX_PIDS_LIMIT']}" in slim
    for flag in ("--user 1000:1000", "--cap-drop=ALL", "--security-opt=no-new-privileges"):
        assert flag in slim, flag
    assert "ps -eo comm= | grep -Ei " in slim and "chrom|jupyter|code-server|tigervnc|websocat|openbox" in slim, "the switched-off services must not be running (matched on process names)"
    # One run that builds and renders all three formats, which is what the skill
    # now asks for: one interpreter start inside a 512 MiB slot, not four.
    assert "python3 $R build /mnt/smoke/example_services_export_small.xls" in slim and "--render pdf,docx,xlsx" in slim, "the report path builds and renders inside the slim limits, in one run"
    assert "docker inspect --format '{{.State.OOMKilled}}'" in slim and 'test "$oom_killed" = false' in slim
    assert "::error::slim sandbox" in slim
