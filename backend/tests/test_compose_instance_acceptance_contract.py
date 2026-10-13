"""Inspect CI qualification contracts without importing or invoking the live suite."""

import ast
import subprocess
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
STORAGE_TESTS = (
    "test_pool_verifies_inside_the_gateway_container_without_host_image_path",
    "test_ordinary_chat_works_with_storage_spaces_on",
    "test_my_files_shared_and_projects_work_with_storage_spaces_on",
    "test_chat_without_files_mounts_nothing_of_the_user",
    "test_chat_without_shared_mounts_the_users_space_alone",
    "test_seat_is_taken_at_the_three_creation_paths_and_refused_past_the_count",
    "test_release_seat_frees_the_seat_and_empties_the_users_space",
    "test_company_grant_is_written_for_an_account_that_had_none",
    "test_publication_records_the_member_and_by_host",
    "test_full_user_space_refuses_upload_and_new_conversation_with_files_full",
)


def test_cleanup_bootstrap_preserves_helper_argv_and_drops_privileges():
    tree = ast.parse((ROOT / "backend/tests/test_compose_instance_acceptance.py").read_text(encoding="utf-8"))
    assignment = next(node for node in tree.body if isinstance(node, ast.Assign) and any(isinstance(target, ast.Name) and target.id == "CLEANUP_BOOTSTRAP" for target in node.targets))
    bootstrap = ast.literal_eval(assignment.value)
    assert 'stat -c %g "$SOCKET"' in bootstrap
    assert '--reuid=1000 --regid=1000 --groups="$docker_gid"' in bootstrap
    assert "cd /app/backend" in bootstrap and "export PYTHONPATH=." in bootstrap
    assert "exec setpriv" in bootstrap and '"$@"' in bootstrap
    assert subprocess.run(["sh", "-n"], input=bootstrap, text=True, capture_output=True).returncode == 0
    bundle = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "Bundle")
    cleanup = next(node for node in bundle.body if isinstance(node, ast.FunctionDef) and node.name == "cleanup_orphan")
    constants = {node.value for node in ast.walk(cleanup) if isinstance(node, ast.Constant) and isinstance(node.value, str)}
    assert {"--entrypoint", "sh", "--name", "--label", "/app/backend/.venv/bin/python", "com.docker.compose.oneoff", "hartmesh.acceptance.cleanup"} <= constants
    assert any(isinstance(node, ast.Try) and node.finalbody for node in ast.walk(cleanup))


def test_helpers_use_the_image_environment_and_fault_case_has_an_assignment_barrier():
    source = (ROOT / "backend/tests/test_compose_instance_acceptance.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    bundle = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "Bundle")
    for name in ("gateway", "accounts"):
        helper = next(node for node in bundle.body if isinstance(node, ast.FunctionDef) and node.name == name)
        constants = {node.value for node in ast.walk(helper) if isinstance(node, ast.Constant) and isinstance(node.value, str)}
        assert {"/app/backend", "PYTHONPATH=.", "/app/backend/.venv/bin/python"} <= constants
    fault = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == STORAGE_TESTS[3])
    barrier = next(node for node in ast.walk(fault) if isinstance(node, ast.With) and any(isinstance(item.context_expr, ast.Name) and item.context_expr.id == "barrier" for item in node.items))
    assert any(isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "sandbox" for node in ast.walk(barrier))
    assert "pg_advisory_lock(hashtext('hartmesh-space-assignment'))" in source
    released = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == STORAGE_TESTS[6])
    enable = next(
        node for node in ast.walk(released) if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "accounts" and node.args and isinstance(node.args[0], ast.Constant) and node.args[0].value == "enable"
    )
    assert any(keyword.arg == "account_id" for keyword in enable.keywords)


def test_gateway_restart_requires_confirmed_container_and_pg_session_drainage():
    source = (ROOT / "backend/tests/test_compose_instance_acceptance.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    fault = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == STORAGE_TESTS[3])
    restarts = [node for node in ast.walk(fault) if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "run" and node.args and isinstance(node.args[0], ast.Constant) and node.args[0].value == "up"]
    assert len(restarts) == 1
    guards = [node for node in ast.walk(fault) if isinstance(node, ast.If) and restarts[0] in list(ast.walk(ast.Module(body=node.body, type_ignores=[])))]
    assert any("cleanup_drained" in ast.unparse(node.test) for node in guards)
    assert "assert barrier.drained" in source
    assert "PGAPPNAME=" in source and "pg_terminate_backend" in source
    assert "backend_start" in source and "self.identity" in source


def test_backend_runtime_provisions_and_checks_bundle_host_tools():
    # Builder packages do not survive the independent final Python slim stage.
    runtime = (ROOT / "backend/Dockerfile").read_text(encoding="utf-8").rsplit("\nFROM ", 1)[1]
    instructions = runtime.replace("\\\n", " ")
    assert any(line.startswith("RUN ") and "apt-get install" in line and "util-linux" in line for line in instructions.splitlines())
    assert "ARG APT_MIRROR" in runtime
    assert 'if [ -n "${APT_MIRROR}" ]' in runtime
    assert "/etc/apt/sources.list.d/debian.sources" in runtime and "/etc/apt/sources.list" in runtime
    document = yaml.safe_load((ROOT / ".github/workflows/compose-instances.yml").read_text(encoding="utf-8"))
    probe = next(step["run"] for step in document["jobs"]["bundle-instances"]["steps"] if step.get("name") == "7 Give the image a digest reference")
    assert "command -v nsenter; command -v setpriv" in probe
    assert "wc -l" not in probe


def test_bundle_acceptance_is_opt_in_and_keeps_the_prescribed_file_order():
    source = (ROOT / "backend/tests/test_compose_instance_acceptance.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    names = tuple(node.name for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name.startswith("test_"))
    assert names == STORAGE_TESTS
    assert "HARTMESH_REQUIRE_BUNDLE_INSTANCES" in source
    assert "allow_module_level=True" in source
    assert "pytest.mark.live" in source
    for name in ("HARTMESH_BUNDLE_URL", "HARTMESH_BUNDLE_PROJECT_DIR", "HARTMESH_BUNDLE_ENV_FILE", "HARTMESH_BUNDLE_ADMIN_EMAIL", "HARTMESH_BUNDLE_ADMIN_PASSWORD", "HARTMESH_BUNDLE_RUNTIME"):
        assert name in source
    helper = next(node for node in tree.body if isinstance(node, ast.Assign) and any(isinstance(target, ast.Name) and target.id == "ISOLATED_RELEASE" for target in node.targets))
    # Compile embedded Python as data; never import or run the opt-in suite.
    cleanup = ast.parse(ast.literal_eval(helper.value))
    assert any(isinstance(node, ast.AsyncFunctionDef) and node.name == "prove_absent" for node in ast.walk(cleanup))


def test_bundle_gate_requires_disk_backed_fixtures_and_exact_named_cases():
    document = yaml.safe_load((ROOT / ".github/workflows/compose-instances.yml").read_text(encoding="utf-8"))
    job = document["jobs"]["bundle-instances"]
    assert job["runs-on"] == "ubuntu-latest"
    # Runner context is unavailable in job-level env; use the step environment.
    assert "TMPDIR" not in job["env"]
    steps = {step.get("name"): step.get("run", "") for step in job["steps"]}
    preflight = steps["2 Free disk space and check the data directory"]
    assert 'TMPDIR="${RUNNER_TEMP}/hartmesh-bundle-${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}"' in preflight
    assert 'echo "TMPDIR=$TMPDIR" >> "$GITHUB_ENV"' in preflight
    assert 'findmnt -no FSTYPE --target "$TMPDIR"' in preflight
    assert "tmpfs|ramfs)" in preflight and "exit 1" in preflight
    acceptance = steps["13 Run the acceptance tests"]
    assert "HARTMESH_REQUIRE_BUNDLE_INSTANCES=1" in acceptance
    assert '--basetemp="$TMPDIR/fixtures"' in acceptance
    assert "-p no:randomly" in acceptance
    for name in STORAGE_TESTS:
        assert repr(name) in acceptance
    assert 'tree.findall(".//skipped")' in acceptance
    assert 'tree.findall(".//failure")' in acceptance
    assert 'tree.findall(".//error")' in acceptance
    shutdown = steps["14 Check that the stack stops cleanly"]
    assert shutdown.index("compose-before-down.log") < shutdown.index("$C down")
    assert "storage-init quiesce" in shutdown
