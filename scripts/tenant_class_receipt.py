#!/usr/bin/env python3
"""Add a human-performed tenant qualification to committed image admission evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import stat
import sys
import tempfile
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

from release_acceptance import (
    DIGEST,
    REFERENCE,
    TENANT_SECONDS,
    TENANT_STEPS,
    AcceptanceError,
    exact_keys,
    positive,
    read_json,
    validate_tenant_class,
)

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
DERIVED_STEPS = {
    "runtime": "test_runtime_alias_is_on_the_workspace_and_the_chat_sandbox_and_not_on_the_relay_and_the_probe",
    "rprivate": "test_bundle_runs_an_agent_and_its_space_survives_gateway_restart_and_workspace_loss",
}
MAX_RECORD = 8192
MAX_INPUT = 2 * 1024 * 1024


def bounded(path: Path, limit: int) -> bytes:
    with path.open("rb") as source:
        data = source.read(limit + 1)
    if len(data) > limit:
        raise AcceptanceError("tenant-class input exceeds its size limit")
    return data


def junit(data: bytes, *, acceptance: bool) -> dict:
    try:
        text = data.decode("utf-8")
    except UnicodeError as error:
        raise AcceptanceError("tenant-class JUnit must be UTF-8") from error
    if "\x00" in text or "<!DOCTYPE" in text or "<!ENTITY" in text:
        raise AcceptanceError("tenant-class JUnit declarations are not admitted")
    try:
        root = ET.fromstring(text)
    except (ET.ParseError, ValueError) as error:
        raise AcceptanceError("tenant-class JUnit is invalid") from error
    if root.tag not in {"testsuites", "testsuite"}:
        raise AcceptanceError("tenant-class JUnit has no test suite")
    cases = list(root.iter("testcase"))
    if not cases or len(cases) > 10000:
        raise AcceptanceError("tenant-class JUnit has no bounded test cases")
    if any(list(root.iter(tag)) for tag in ("failure", "error", "skipped")):
        raise AcceptanceError("tenant-class JUnit contains a failed or skipped case")
    for suite in root.iter():
        if suite.tag not in {"testsuite", "testsuites"}:
            continue
        for key in ("failures", "errors", "skipped", "tests"):
            declared = suite.get(key)
            if declared is None:
                continue
            if not re.fullmatch(r"[0-9]{1,6}", declared):
                raise AcceptanceError("tenant-class JUnit aggregate is invalid")
            expected = len(list(suite.iter("testcase"))) if key == "tests" else 0
            if int(declared) != expected:
                raise AcceptanceError(
                    "tenant-class JUnit aggregate differs from the actual cases"
                )
    identities = [(case.get("classname", ""), case.get("name")) for case in cases]
    if any(not name for _classname, name in identities) or len(set(identities)) != len(
        cases
    ):
        raise AcceptanceError("tenant-class JUnit contains unnamed or duplicate cases")
    if acceptance:
        names = [name for _classname, name in identities]
        if len(names) != len(ACCEPTANCE_NAMES) or set(names) != set(ACCEPTANCE_NAMES):
            raise AcceptanceError(
                "tenant-class receipt requires all 27 named acceptance cases"
            )
    return {
        "cases": len(cases),
        "junit_sha256": "sha256:" + hashlib.sha256(data).hexdigest(),
    }


def image_digests(data: bytes) -> dict:
    images = {}
    try:
        lines = data.decode("utf-8").splitlines()
    except UnicodeError as error:
        raise AcceptanceError("tenant-class images list is not UTF-8") from error
    for line in lines:
        if not REFERENCE.fullmatch(line):
            raise AcceptanceError("tenant-class images must be pinned by digest")
        repository, digest = line.rsplit("@", 1)
        for name in ("backend", "sandbox"):
            if repository.endswith("/hartmesh-" + name):
                if name in images:
                    raise AcceptanceError(
                        "tenant-class images list has duplicate component pins"
                    )
                images[name] = digest
    exact_keys(images, ("backend", "sandbox"), "images")
    return images


def write_receipt(args) -> None:
    path = Path(args.record)
    mode = path.lstat().st_mode
    if not stat.S_ISREG(mode):
        raise AcceptanceError("tenant-class record must be a regular file")
    original = bounded(path, MAX_RECORD)
    record = read_json(original)
    fields = {"schema", "version", "run_id", "run_attempt", "artifacts"}
    if (
        set(record) not in (fields, fields | {"tenant_class"})
        or type(record.get("schema")) is not int
        or record["schema"] != 1
        or record.get("version") != args.version
        or not positive(record.get("run_id"))
        or not positive(record.get("run_attempt"))
    ):
        raise AcceptanceError("tenant-class record version or shape is invalid")
    exact_keys(record["artifacts"], ("sqlite", "postgres-redis"), "record artifacts")
    if any(
        not isinstance(value, str) or not DIGEST.fullmatch(value)
        for value in record["artifacts"].values()
    ):
        raise AcceptanceError("tenant-class record artifact digest is invalid")
    steps = read_json(bounded(Path(args.steps), MAX_RECORD))
    external_steps = TENANT_STEPS - DERIVED_STEPS.keys()
    exact_keys(steps, external_steps | {"seconds"}, "input steps")
    if any(steps[key] != "passed" for key in external_steps):
        raise AcceptanceError("tenant-class required step did not pass")
    guest = read_json(bounded(Path(args.guest), MAX_RECORD))
    images = image_digests(bounded(Path(args.images), MAX_RECORD))
    acceptance_data = bounded(Path(args.acceptance_junit), MAX_INPUT)
    acceptance = junit(acceptance_data, acceptance=True)
    native = junit(bounded(Path(args.native_junit), MAX_INPUT), acceptance=False)
    # The two steps are derived exclusively from the required passed cases,
    # never from a caller-supplied success assertion.
    receipt = {
        "schema": 1,
        "version": args.version,
        "recorded_at": datetime.now(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z"),
        "guest": guest,
        "images": images,
        "native": native,
        "acceptance": acceptance,
        "steps": {
            **{key: steps[key] for key in sorted(external_steps)},
            **dict.fromkeys(DERIVED_STEPS, "passed"),
        },
        "seconds": steps["seconds"],
    }
    exact_keys(receipt["seconds"], TENANT_SECONDS, "seconds")
    validate_tenant_class(receipt, args.version)
    encoded = (
        json.dumps({**record, "tenant_class": receipt}, indent=2, allow_nan=False)
        + "\n"
    ).encode("utf-8")
    if len(encoded) >= MAX_RECORD:
        raise AcceptanceError("tenant-class record would reach the 8192-byte limit")
    temporary = None
    try:
        descriptor, temporary = tempfile.mkstemp(
            prefix=".tenant-receipt-", dir=path.parent
        )
        with os.fdopen(descriptor, "wb") as output:
            output.write(encoded)
            output.flush()
            os.fsync(output.fileno())
            os.fchmod(output.fileno(), stat.S_IMODE(mode))
        if bounded(path, MAX_RECORD) != original:
            raise AcceptanceError("tenant-class record changed during qualification")
        os.replace(temporary, path)
        temporary = None
    finally:
        if temporary is not None:
            Path(temporary).unlink(missing_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in (
        "record",
        "version",
        "native-junit",
        "acceptance-junit",
        "steps",
        "guest",
        "images",
    ):
        parser.add_argument("--" + name, required=True)
    args = parser.parse_args()
    try:
        if not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+\+hartmesh\.[0-9]+", args.version):
            raise AcceptanceError("tenant-class release version is not canonical")
        write_receipt(args)
    except (
        AcceptanceError,
        OSError,
        ValueError,
        OverflowError,
        RecursionError,
    ) as error:
        print(f"tenant-class receipt refused: {error}", file=sys.stderr)
        return 1
    print("Tenant-class receipt recorded; commit the acceptance record before tagging.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
