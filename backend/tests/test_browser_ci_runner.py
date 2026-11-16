"""Host browser CI installs the locked runtime and verifies it before tests."""

import os
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
BROWSER_JOBS = [
    ("e2e-tests.yml", "e2e-tests"),
    ("replay-e2e.yml", "fullstack-replay-render"),
]


def _browser_job(workflow, job):
    config = yaml.safe_load((ROOT / ".github/workflows" / workflow).read_text(encoding="utf-8"))
    return config["jobs"][job]


def _chromium_probe(steps):
    return next(step for step in steps if step.get("id") == "chromium")


@pytest.mark.parametrize(("workflow", "job"), BROWSER_JOBS)
def test_browser_ci_installs_locked_product_playwright(workflow, job):
    browser_job = _browser_job(workflow, job)
    assert browser_job["runs-on"] == "ubuntu-26.04"
    assert "container" not in browser_job
    lock = yaml.safe_load((ROOT / "frontend-hm/pnpm-lock.yaml").read_text(encoding="utf-8"))
    assert lock["importers"]["."]["devDependencies"]["@playwright/test"]["version"]
    steps = browser_job["steps"]
    deps = next(i for i, step in enumerate(steps) if step.get("run") == "pnpm install --frozen-lockfile")
    install = next(i for i, step in enumerate(steps) if step.get("run") == "pnpm exec playwright install chromium --with-deps")
    assert deps < install
    assert steps[deps]["working-directory"] == steps[install]["working-directory"] == "frontend-hm"


@pytest.mark.parametrize(("workflow", "job"), BROWSER_JOBS)
def test_browser_ci_verifies_chromium_before_running_tests(workflow, job):
    steps = _browser_job(workflow, job)["steps"]
    probe = _chromium_probe(steps)
    assert probe["working-directory"] == "frontend-hm"
    assert "await chromium.launch()" in probe["run"]
    assert "await browser.close()" in probe["run"]
    tests = [i for i, step in enumerate(steps) if "pnpm exec playwright test" in step.get("run", "")]
    assert tests, "The browser job must execute tests"
    install = next(i for i, step in enumerate(steps) if "playwright install chromium" in step.get("run", ""))
    assert install < steps.index(probe) < min(tests)


@pytest.mark.parametrize(("workflow", "job"), BROWSER_JOBS)
def test_auth_recovery_requires_a_working_browser(workflow, job):
    steps = _browser_job(workflow, job)["steps"]
    probe = _chromium_probe(steps)
    auth = next(
        step
        for step in steps
        if any(
            name in step.get("run", "")
            for name in (
                "playwright.auth.config.ts",
                "playwright.gateway-auth.config.ts",
            )
        )
    )
    assert f"steps.{probe['id']}.outcome == 'success'" in auth["if"]
    assert "!cancelled()" in auth["if"]
    if workflow == "e2e-tests.yml":
        assert "matrix.shard == 1" in auth["if"]


@pytest.mark.parametrize(("workflow", "job"), BROWSER_JOBS)
def test_browser_ci_uses_and_verifies_host_python(workflow, job):
    steps = _browser_job(workflow, job)["steps"]
    setup = next(i for i, step in enumerate(steps) if step.get("uses", "").startswith("actions/setup-python@"))
    assert steps[setup]["with"]["python-version"] == "3.12"
    sync = next(i for i, step in enumerate(steps) if "uv sync" in step.get("run", ""))
    verify = next(i for i, step in enumerate(steps) if 'uv run --no-sync python -c "import ssl, sqlite3"' == step.get("run"))
    assert setup < sync < verify < steps.index(_chromium_probe(steps))


@pytest.mark.parametrize(("workflow", "job"), BROWSER_JOBS)
@pytest.mark.parametrize("failure", [None, "launch", "close"])
def test_committed_chromium_probe_propagates_browser_failures(workflow, job, failure, tmp_path):
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node is unavailable; static browser workflow checks still apply")
    program = _chromium_probe(_browser_job(workflow, job)["steps"])["run"]
    assert program.startswith("node <<'NODE'\n") and program.endswith("NODE\n")
    javascript = program.removeprefix("node <<'NODE'\n").removesuffix("NODE\n")
    package = tmp_path / "node_modules/@playwright/test"
    package.mkdir(parents=True)
    package.joinpath("index.js").write_text(
        """
exports.chromium = { async launch() {
  console.log('launch');
  if (process.env.PROBE_FAILURE === 'launch') throw new Error('launch refused');
  return { async close() {
    console.log('close');
    if (process.env.PROBE_FAILURE === 'close') throw new Error('close refused');
  }};
}};
""",
        encoding="utf-8",
    )
    env = {**os.environ, "PROBE_FAILURE": failure or ""}
    result = subprocess.run(
        [node, "-"],
        input=javascript,
        cwd=tmp_path,
        env=env,
        text=True,
        capture_output=True,
        timeout=10,
        check=False,
    )
    assert result.stdout.splitlines() == (["launch"] if failure == "launch" else ["launch", "close"])
    assert (result.returncode == 0) is (failure is None)
    if failure:
        assert f"{failure} refused" in result.stderr
