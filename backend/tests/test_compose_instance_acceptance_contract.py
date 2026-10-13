"""Inspect CI qualification contracts without importing or invoking the live suite."""

import ast
import inspect
import shlex
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


def test_startup_failures_keep_readable_evidence_and_report_service_logs():
    document = yaml.safe_load((ROOT / ".github/workflows/compose-instances.yml").read_text(encoding="utf-8"))
    steps = {step.get("name"): step.get("run", "") for step in document["jobs"]["bundle-instances"]["steps"]}
    startup = steps["11 Start the bundle, whose first start prepares every Space"]
    assert "trap '" in startup and "gateway storage-init >&2" in startup and " ERR" in startup
    assert startup.index("trap '") < startup.index("$C up -d --wait")
    collection = steps["Collect logs"]
    assert 'sudo cat /srv/hartmesh/spaces/inventory.v2.json > "$EVIDENCE/inventory.v2.json"' in collection
    assert 'sudo cat /etc/fstab > "$EVIDENCE/fstab.txt"' in collection
    assert "sudo cp" not in collection


def test_bundle_prefetches_every_pinned_image_before_the_gateway_starts():
    document = yaml.safe_load((ROOT / ".github/workflows/compose-instances.yml").read_text(encoding="utf-8"))
    steps = document["jobs"]["bundle-instances"]["steps"]
    by_name = {step.get("name"): step.get("run", "") for step in steps}
    prefetch = by_name["Prefetch every pinned bundle image"]
    assert 'docker pull "$image"' in prefetch and "/opt/hartmesh/images.txt" in prefetch
    names = [step.get("name") for step in steps]
    assert names.index("Prefetch every pinned bundle image") < names.index("11 Start the bundle, whose first start prepares every Space")
    install = by_name["8 Install the bundle at /opt/hartmesh with the built image"]
    assert "/opt/hartmesh/compose.yaml /opt/hartmesh/images.txt" in install
    assert 'docker image ls --digests --no-trunc > "$EVIDENCE/images.txt"' in by_name["Collect logs"]


def test_bundle_login_uses_the_public_form_contract():
    tree = ast.parse((ROOT / "backend/tests/test_compose_instance_acceptance.py").read_text(encoding="utf-8"))
    fixture = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "session")
    login = next(node for node in ast.walk(fixture) if isinstance(node, ast.Call) and any(isinstance(argument, ast.Constant) and argument.value == "/api/v1/auth/login/local" for argument in node.args))
    assert not any(keyword.arg == "json" for keyword in login.keywords)
    form = next(keyword.value for keyword in login.keywords if keyword.arg == "data")
    assert {ast.literal_eval(key) for key in form.keys} == {"username", "password"}


def test_chat_mount_proof_includes_managed_and_configured_binds():
    tree = ast.parse((ROOT / "backend/tests/test_compose_instance_acceptance.py").read_text(encoding="utf-8"))
    chat = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == STORAGE_TESTS[1])
    assignments = {target.id: node.value for node in ast.walk(chat) if isinstance(node, ast.Assign) for target in node.targets if isinstance(target, ast.Name)}
    assert ast.literal_eval(assignments["managed"]) == {"/mnt/user-data/workspace", "/mnt/user-data/uploads", "/mnt/user-data/outputs", "/mnt/user-data/files", "/mnt/user-data/shared", "/mnt/acp-workspace"}
    assert "configured" in assignments
    source = ast.unparse(chat)
    assert "read_only" in source and "container_path" in source and "host_path" in source
    assert "len(binds) == 7" not in source
    assert "skills" in assignments and "integrations" in assignments
    assert all(category in source for category in ("public", "custom", "legacy", "integrations"))
    assert "chat-runtime" in source and "operational_root" in assignments


def test_synthetic_run_evidence_precedes_terminal_success_assertions():
    tree = ast.parse((ROOT / "backend/tests/test_compose_instance_acceptance.py").read_text(encoding="utf-8"))
    person = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "Person")
    run = next(node for node in person.body if isinstance(node, ast.FunctionDef) and node.name == "run")
    source = ast.unparse(run)
    assert source.index(".sse") < source.index("eventually(terminal")
    assert source.index(".json") < source.index("if success:")
    assert "TRUNCATED" in source and "1048576" in source
    assert "response.headers)" not in source and "self.client.cookies)" not in source


def test_maintenance_proof_waits_for_logs_after_sql_commit():
    tree = ast.parse((ROOT / "backend/tests/test_compose_instance_acceptance.py").read_text(encoding="utf-8"))
    for name in (STORAGE_TESTS[3], STORAGE_TESTS[6], STORAGE_TESTS[7]):
        function = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == name)
        assert any(
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "eventually"
            and any(isinstance(child, ast.Call) and isinstance(child.func, ast.Attribute) and child.func.attr == "logs" for child in ast.walk(node))
            for node in ast.walk(function)
        )


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


def test_scripted_model_host_port_stays_outside_the_container_port_allocator_range():
    from deerflow.utils.network import PortAllocator

    document = yaml.safe_load((ROOT / ".github/workflows/compose-instances.yml").read_text(encoding="utf-8"))
    steps = {step.get("name"): step.get("run", "") for step in document["jobs"]["bundle-instances"]["steps"]}
    start = steps["5 Start the scripted model"]
    tokens = shlex.split(next(line for line in start.splitlines() if line.startswith("nohup ")))
    assert tokens[:3] == ["nohup", "python3", "-c"], "Use the existing Handler at a separate runner-host port"
    tree = ast.parse(tokens[3])
    server = next(node for node in ast.walk(tree) if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "ThreadingHTTPServer")
    host, port = ast.literal_eval(server.args[0])
    assert host == "0.0.0.0"
    handler = server.args[1]
    assert isinstance(handler, ast.Subscript) and ast.literal_eval(handler.slice) == "Handler"
    assert isinstance(handler.value, ast.Call) and isinstance(handler.value.func, ast.Name) and handler.value.func.id == "run_path"
    assert ast.literal_eval(handler.value.args[0]) == "docker/acceptance/provider.py"
    profile = yaml.safe_load((ROOT / "deploy/compose/config.yaml").read_text(encoding="utf-8"))
    first = profile["sandbox"]["port"]
    count = inspect.signature(PortAllocator.allocate).parameters["max_range"].default
    assert port not in range(first, first + count), "Gateway-container socket checks cannot observe runner-host listeners"
    assert start.count(f"http://127.0.0.1:{port}/health") == 2
    assert f"base_url: http://host.docker.internal:{port}/v1" in steps["9 Write the data directory, the model file and the .env contract"]


def test_failure_evidence_captures_physical_image_facts_and_role_bindings():
    document = yaml.safe_load((ROOT / ".github/workflows/compose-instances.yml").read_text(encoding="utf-8"))
    step = next(step for step in document["jobs"]["bundle-instances"]["steps"] if step.get("name") == "Collect logs")
    assert step["if"] == "always()"
    source = step["run"]
    assert "sudo python3 - <<'PY' > \"$EVIDENCE/storage-facts.json\"" in source
    diagnostic = source.split("sudo python3 - <<'PY'", 1)[1].split("\n", 1)[1].split("\nPY\n", 1)[0]
    tree = ast.parse(diagnostic)
    compile(tree, "<CI storage facts>", "exec")
    constants = {node.value for node in ast.walk(tree) if isinstance(node, ast.Constant) and isinstance(node.value, str)}
    assert {"st_mode", "st_dev", "st_nlink", "st_size", "st_blocks", "st_uid", "st_gid", "data", "control"} <= constants
    assert "256" in diagnostic and "islice" in diagnostic and "follow_symlinks=False" in diagnostic
    assert "storage_space_backings" in source and "hm_storage_feature_links" in source
    assert "timeout 30 $C exec -T postgres psql" in source
