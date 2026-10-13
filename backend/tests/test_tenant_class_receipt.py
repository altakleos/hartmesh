"""Synthetic receipt fixtures do not qualify a real tenant host."""

import copy
import hashlib
import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
VERSION = "2.2.0+hartmesh.46"
DIGEST = "sha256:" + "a" * 64
ACCEPTANCE_NAMES = (
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
    "test_bundle_runs_an_agent_and_its_space_survives_gateway_restart_and_workspace_loss",
    "test_runtime_alias_is_on_the_workspace_and_the_chat_sandbox_and_not_on_the_relay_and_the_probe",
    "test_work_activation_on_a_real_workspace_settles_succeeded_and_submits_the_outcome",
    "test_attempt_in_flight_leaves_a_slot_for_an_ordinary_chat",
    "test_graceful_gateway_restart_leaves_no_unresolved_attempt",
    "test_killed_gateway_settles_the_attempt_failed_with_an_attempt_contained_event",
    "test_new_activation_is_admitted_after_a_contained_attempt",
    "test_file_given_while_the_workspace_runs_is_read_by_the_running_agent",
    "test_file_given_while_no_workspace_runs_is_present_at_the_next_run",
    "test_forty_mebibyte_file_arrives_with_the_same_sha256",
    "test_stop_ends_a_held_attempt_and_leaves_the_assignment_open",
    "test_creation_past_the_count_is_refused_for_each_kind",
    "test_deleted_agents_slot_is_reused_by_the_same_kind_at_once",
    "test_assignment_that_meets_a_blocked_host_finishes_and_records_the_denial",
    "test_a_question_in_an_assignment_blocks_it_and_reaches_the_supervisor",
    "test_closing_the_workspace_under_a_held_attempt_ends_its_run_with_the_reason_operator",
    "test_a_killed_workspace_ends_its_run_with_the_reason_container_absent",
)
STEPS = ("fresh_install", "cold_start", "enospc", "egress_denial", "transfer_64_mib", "two_runs_one_workspace", "reboot", "restore")
SECONDS = ("prepare", "cold_start", "pending", "fence_pending", "idle_eviction", "transfer_64_mib")


def junit(names):
    return ("<testsuites><testsuite>" + "".join(f'<testcase classname="qualification" name="{name}" />' for name in names) + "</testsuite></testsuites>").encode()


@pytest.fixture
def receipt_inputs(tmp_path):
    record = {"schema": 1, "version": VERSION, "run_id": 1234, "run_attempt": 1, "artifacts": {"sqlite": DIGEST, "postgres-redis": DIGEST}}
    contents = {
        "record": json.dumps(record).encode(),
        "native-junit": junit(["test_disk_limit", "test_inode_limit"]),
        "acceptance-junit": junit(ACCEPTANCE_NAMES),
        "steps": json.dumps({**dict.fromkeys(STEPS, "passed"), "seconds": dict.fromkeys(SECONDS, 1.25)}).encode(),
        "guest": json.dumps({"vcpus": 4, "memory_mib": 8192, "data_disk_gib": 100, "kernel": "6.8", "docker_engine": "28.3", "compose": "2.38", "runsc": "20260901"}).encode(),
        "images": f"ghcr.io/altakleos/hartmesh-backend@{DIGEST}\nghcr.io/altakleos/hartmesh-sandbox@{DIGEST}\n".encode(),
    }
    paths = {}
    for name, content in contents.items():
        paths[name] = tmp_path / name
        paths[name].write_bytes(content)
    return record, paths


def invoke(paths, version=VERSION):
    command = [sys.executable, str(ROOT / "scripts/tenant_class_receipt.py"), "--version", version]
    for name, path in paths.items():
        command.extend(["--" + name, str(path)])
    return subprocess.run(command, capture_output=True, text=True)


def test_receipt_keeps_the_five_recorded_keys_and_the_size_limit(receipt_inputs):
    record, paths = receipt_inputs
    result = invoke(paths)
    assert result.returncode == 0, result.stderr
    encoded = paths["record"].read_bytes()
    assert len(encoded) < 8192
    value = json.loads(encoded)
    assert {key: value[key] for key in record} == record
    receipt = value["tenant_class"]
    assert set(receipt) == {"schema", "version", "recorded_at", "guest", "images", "native", "acceptance", "steps", "seconds"}
    assert receipt["version"] == VERSION
    assert receipt["recorded_at"].endswith("Z")
    assert receipt["native"] == {"cases": 2, "junit_sha256": "sha256:" + hashlib.sha256(paths["native-junit"].read_bytes()).hexdigest()}
    assert receipt["acceptance"] == {"cases": 27, "junit_sha256": "sha256:" + hashlib.sha256(paths["acceptance-junit"].read_bytes()).hexdigest()}
    assert receipt["steps"]["runtime"] == receipt["steps"]["rprivate"] == "passed"
    assert set(receipt["steps"]) == set(STEPS) | {"runtime", "rprivate"}
    assert receipt["images"] == {"backend": DIGEST, "sandbox": DIGEST}
    assert invoke(paths, VERSION + "0").returncode == 1
    assert paths["record"].read_bytes() == encoded


@pytest.mark.parametrize(
    "mutation",
    [
        "failure",
        "error",
        "skipped",
        "missing",
        "duplicate",
        "empty-native",
        "missing-step",
        "failed-step",
        "supplied-runtime",
        "boolean-guest",
        "nonfinite-seconds",
        "unknown-guest",
        "wrong-images",
        "oversized",
        "root-failure",
        "root-error",
        "root-skipped",
        "count-mismatch",
        "nested-count-mismatch",
        "utf16",
    ],
)
def test_receipt_is_written_only_when_every_step_passed_and_no_case_was_skipped(receipt_inputs, mutation):
    _record, paths = receipt_inputs
    if mutation in {"failure", "error", "skipped"}:
        paths["native-junit"].write_bytes(b'<testsuites><testsuite><testcase name="test_disk_limit"><' + mutation.encode() + b"/></testcase></testsuite></testsuites>")
    elif mutation == "missing":
        paths["acceptance-junit"].write_bytes(junit(ACCEPTANCE_NAMES[:-1]))
    elif mutation == "duplicate":
        paths["acceptance-junit"].write_bytes(junit(ACCEPTANCE_NAMES + (ACCEPTANCE_NAMES[0],)))
    elif mutation == "empty-native":
        paths["native-junit"].write_bytes(junit([]))
    elif mutation.startswith("root-"):
        key = {"root-failure": "failures", "root-error": "errors", "root-skipped": "skipped"}[mutation]
        paths["native-junit"].write_bytes(junit(["test_disk_limit"]).replace(b"<testsuites>", f'<testsuites {key}="1">'.encode()))
    elif mutation in {"count-mismatch", "nested-count-mismatch"}:
        tag = b"testsuite" if mutation == "nested-count-mismatch" else b"testsuites"
        paths["native-junit"].write_bytes(junit(["test_disk_limit"]).replace(b"<" + tag + b">", b"<" + tag + b' tests="2">'))
    elif mutation == "utf16":
        paths["native-junit"].write_bytes(junit(["test_disk_limit"]).decode().encode("utf-16"))
    elif mutation in {"missing-step", "failed-step", "supplied-runtime", "nonfinite-seconds"}:
        value = json.loads(paths["steps"].read_bytes())
        if mutation == "missing-step":
            del value["restore"]
        if mutation == "failed-step":
            value["restore"] = "failed"
        if mutation == "supplied-runtime":
            value["runtime"] = "passed"
        if mutation == "nonfinite-seconds":
            value["seconds"]["pending"] = float("inf")
        paths["steps"].write_text(json.dumps(value), encoding="utf-8")
    elif mutation in {"boolean-guest", "unknown-guest"}:
        value = json.loads(paths["guest"].read_bytes())
        value["vcpus" if mutation == "boolean-guest" else "unknown"] = True
        paths["guest"].write_text(json.dumps(value), encoding="utf-8")
    elif mutation == "wrong-images":
        paths["images"].write_text("backend:latest\nsandbox:latest\n", encoding="utf-8")
    elif mutation == "oversized":
        paths["record"].write_bytes(paths["record"].read_bytes() + b" " * 8192)
    before = paths["record"].read_bytes()
    result = invoke(paths)
    assert result.returncode == 1, result.stderr
    assert paths["record"].read_bytes() == before


def test_record_validation_rejects_unknown_keys_nonfinite_timing_and_bad_utc(receipt_inputs):
    spec = importlib.util.spec_from_file_location("receipt_guard", ROOT / "scripts/release_acceptance.py")
    guard = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(guard)
    _record, paths = receipt_inputs
    assert invoke(paths).returncode == 0
    receipt = json.loads(paths["record"].read_bytes())["tenant_class"]
    for field, bad in [("recorded_at", "2026-99-99T00:00:00Z"), ("recorded_at", "2026-01-01T00:00:00+00:00"), ("schema", True), ("version", "other")]:
        value = copy.deepcopy(receipt)
        value[field] = bad
        with pytest.raises(guard.AcceptanceError):
            guard.validate_tenant_class(value, VERSION)
    for section in (None, "guest", "images", "native", "acceptance", "steps", "seconds"):
        value = copy.deepcopy(receipt)
        (value if section is None else value[section])["unknown"] = "passed"
        with pytest.raises(guard.AcceptanceError):
            guard.validate_tenant_class(value, VERSION)
    for bad in (True, -1, float("inf")):
        value = copy.deepcopy(receipt)
        value["seconds"]["pending"] = bad
        with pytest.raises(guard.AcceptanceError):
            guard.validate_tenant_class(value, VERSION)
