#!/usr/bin/env python3
"""Real storage-init memory qualification on a disposable Linux Docker CI host.

This runs privileged production preparation against its host and is intentionally
unavailable on developer machines. It never changes the host's THP setting.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
import re
import subprocess
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MEMORY = re.compile(r"memory peak .*\(peak_bytes=(\d+) limit_bytes=(\d+)\)")
IMAGE = re.compile(r"[a-zA-Z0-9./:_-]+@sha256:[0-9a-f]{64}\Z")
EXPECTED_LIMIT = 128 << 20
COUNTS = {"user": 10, "assistant": 10, "employee": 10, "company": 1}


def command(*args, env=None):
    try:
        return subprocess.check_output(
            args, text=True, stderr=subprocess.PIPE, env=env
        ).strip()
    except subprocess.CalledProcessError as exc:
        # The fixture contains only synthetic configuration. Preserve the actual
        # Docker/tool failure instead of hiding it behind a Python traceback.
        if exc.stderr:
            print(
                exc.stderr,
                file=sys.stderr,
                end="" if exc.stderr.endswith("\n") else "\n",
                flush=True,
            )
        raise


def validate_sample(sample, *, require_telemetry=True, require_margin=True):
    """No process-exit-only success: admission needs the actual preparation result."""
    if (
        sample["exit_code"] != 0
        or sample["oom_killed"]
        or sample["memory_events"].get("oom_kill", 0)
    ):
        raise ValueError("storage preparation exited unsuccessfully or was OOM-killed")
    doc = sample["document"]
    if (
        doc.get("command") != "prepare"
        or doc.get("returncode") != 0
        or doc.get("verdict") not in {"complete", "nothing-to-do"}
    ):
        raise ValueError("storage preparation did not complete")
    if (
        doc.get("prepared") != COUNTS
        or len(doc.get("spaces", [])) != 31
        or any(not row.get("mounted") for row in doc["spaces"])
    ):
        raise ValueError("all 31 spaces must be prepared and mounted")
    limit, peak = sample["limit_bytes"], sample["peak_bytes"]
    if (
        type(limit) is not int
        or limit != EXPECTED_LIMIT
        or type(peak) is not int
        or peak <= 0
    ):
        raise ValueError("missing cgroup memory observation")
    if require_telemetry:
        reported = sample["reported_memory"]
        if (
            not reported
            or reported["limit_bytes"] != limit
            or reported["peak_bytes"] < sample["observed_peak_bytes"]
        ):
            raise ValueError(
                "reported memory does not cover the external cgroup observation"
            )
    if require_margin and peak * 4 > limit * 3:
        raise ValueError(
            "storage preparation exceeds 75 percent of its declared memory limit"
        )


def profile_service(profile, image, env_file, project):
    frozen = dict(
        line.split("=", 1)
        for line in env_file.read_text(encoding="utf-8").splitlines()
        if line
    )
    rendered = json.loads(
        command(
            "docker",
            "compose",
            "-f",
            str(profile),
            "--env-file",
            str(env_file),
            "config",
            "--format",
            "json",
            env={**os.environ, **frozen},
        )
    )
    service = rendered["services"]["storage-init"]
    service["image"] = image
    # Preserve the actual service, including its entrypoint, limits and privilege.
    if (
        not service["privileged"]
        or service["pid"] != "host"
        or service["network_mode"] != "none"
        or service["pids_limit"] != 64
        or service["restart"] != "no"
    ):
        raise ValueError("production storage service containment differs")
    if (
        service["command"] != ["prepare", "--at-start"]
        or int(service["mem_limit"]) != EXPECTED_LIMIT
        or int(service["memswap_limit"]) != EXPECTED_LIMIT
    ):
        raise ValueError("production startup command or memory limits differ")
    return {"name": project, "services": {"storage-init": service}}


def measure(compose_file, directory, name, *, recreate):
    compose = ("docker", "compose", "-f", str(compose_file))
    if recreate:
        command(*compose, "create", "--force-recreate", "storage-init")
    identity = command(*compose, "ps", "-aq", "storage-init")
    before = json.loads(command("docker", "inspect", identity))[0]
    observed = {"peak": 0, "limit": None, "events": {}, "error": None}
    stop = threading.Event()

    def watch():
        try:
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline and not stop.is_set():
                facts = json.loads(command("docker", "inspect", identity))[0]
                if facts["State"]["Running"] and facts["State"]["Pid"]:
                    pid = facts["State"]["Pid"]
                    group = next(
                        line.split(":", 2)[2]
                        for line in Path(f"/proc/{pid}/cgroup").read_text().splitlines()
                        if line.startswith("0::")
                    )
                    if not group.startswith("/") or ".." in Path(group).parts:
                        raise ValueError("invalid host cgroup path")
                    cg = Path("/sys/fs/cgroup") / group.lstrip("/")
                    break
                time.sleep(0.005)
            else:
                raise ValueError("could not observe running container cgroup")
            while not stop.is_set():
                try:
                    observed["peak"] = max(
                        observed["peak"], int((cg / "memory.peak").read_text())
                    )
                    observed["limit"] = int((cg / "memory.max").read_text())
                    observed["events"] = {
                        key: int(value)
                        for key, value in (
                            line.split()
                            for line in (cg / "memory.events").read_text().splitlines()
                        )
                    }
                except FileNotFoundError:
                    break
                time.sleep(0.005)
        except Exception as exc:
            observed["error"] = f"{type(exc).__name__}: {exc}"

    watcher = threading.Thread(target=watch)
    started_wall = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    expected_image_id = command(
        "docker", "image", "inspect", before["Config"]["Image"], "--format", "{{.Id}}"
    )
    started = time.monotonic()
    # --since scopes logs to this restart rather than accumulated container logs.
    command("docker", "start", identity)
    watcher.start()
    try:
        command("docker", "wait", identity)
    finally:
        stop.set()
        watcher.join()
    facts = json.loads(command("docker", "inspect", identity))[0]
    if facts["Image"] != expected_image_id:
        raise ValueError("running container image differs from the selected digest")
    kernel_log = command(
        "sudo", "dmesg", "--time-format", "iso", "--since", started_wall
    )
    (directory / f"{name}-kernel.log").write_text(kernel_log, encoding="utf-8")
    log = subprocess.check_output(
        ["docker", "logs", "--since", facts["State"]["StartedAt"], identity],
        text=True,
        stderr=subprocess.STDOUT,
    )
    (directory / f"{name}.log").write_text(log, encoding="utf-8")
    docs = []
    for line in log.splitlines():
        try:
            value = json.loads(line)
        except ValueError:
            continue
        if isinstance(value, dict) and value.get("command") == "prepare":
            docs.append(value)
    matches = list(MEMORY.finditer(log))
    reported = (
        dict(zip(("peak_bytes", "limit_bytes"), map(int, matches[-1].groups())))
        if matches
        else None
    )
    sample = dict(
        name=name,
        container_id=identity,
        recreated=recreate,
        image=facts["Config"]["Image"],
        image_id=facts["Image"],
        invocation=facts["Config"]["Entrypoint"] + facts["Config"]["Cmd"],
        started_at=facts["State"]["StartedAt"],
        finished_at=facts["State"]["FinishedAt"],
        seconds=time.monotonic() - started,
        exit_code=facts["State"]["ExitCode"],
        oom_killed=facts["State"]["OOMKilled"],
        limit_bytes=facts["HostConfig"]["Memory"],
        observed_limit_bytes=observed["limit"],
        observed_peak_bytes=observed["peak"],
        peak_bytes=max(observed["peak"], reported["peak_bytes"] if reported else 0),
        reported_memory=reported,
        memory_events=observed["events"],
        observer_error=observed["error"],
        document=docs[-1] if len(docs) == 1 else None,
    )
    (directory / f"{name}.json").write_text(
        json.dumps(sample, indent=2) + "\n", encoding="utf-8"
    )
    if (
        sample["observer_error"]
        or not observed["peak"]
        or observed["limit"] != sample["limit_bytes"]
    ):
        raise ValueError(
            f"external memory observation incomplete: {sample['observer_error']}"
        )
    if not recreate and identity != before["Id"]:
        raise ValueError("restart changed container identity")
    return sample


def verify_mounts(data):
    inventory = json.loads(
        command("sudo", "cat", str(data / "spaces/inventory.v2.json"))
    )
    slots = inventory["volumes"]
    if len(slots) != 31:
        raise ValueError("inventory does not have 31 spaces")
    for slot in slots:
        # Inventory v2 records a list of canonical backing descriptions.
        target = slot["mount_path"]
        found = json.loads(
            command(
                "sudo",
                "findmnt",
                "--json",
                "--mountpoint",
                target,
                "--output",
                "TARGET,FSTYPE,UUID",
            )
        )["filesystems"]
        if (
            len(found) != 1
            or found[0]["fstype"] != "ext4"
            or found[0]["uuid"] != slot["filesystem_uuid"]
        ):
            raise ValueError("host filesystem does not match prepared inventory")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True)
    parser.add_argument("--baseline-image", required=True)
    parser.add_argument("--baseline-ref", default="v2.2.0+hartmesh.55")
    parser.add_argument("--artifacts", required=True, type=Path)
    args = parser.parse_args(argv)
    if (
        os.environ.get("GITHUB_ACTIONS") != "true"
        or os.environ.get("RUNNER_ENVIRONMENT") != "github-hosted"
    ):
        parser.error(
            "requires a disposable GitHub-hosted Docker runner; never run on an existing deployment"
        )
    if not IMAGE.fullmatch(args.image) or not IMAGE.fullmatch(args.baseline_image):
        parser.error("both images must use immutable SHA-256 digest references")
    directory = args.artifacts.resolve()
    directory.mkdir(parents=True, exist_ok=False)
    data = Path("/srv/hartmesh-memory")
    if data.exists():
        parser.error("qualification data directory already exists")
    command(
        "sudo",
        "install",
        "-d",
        "-o",
        str(os.getuid()),
        "-g",
        str(os.getgid()),
        "-m",
        "0755",
        str(data),
    )
    # Compose's normalized JSON omits create_host_path's default. Provision the
    # empty bind source explicitly, as the production long-form bind does.
    command(
        "sudo",
        "install",
        "-d",
        "-o",
        "0",
        "-g",
        "1000",
        "-m",
        "0750",
        str(data / "spaces"),
    )
    env_file = directory / "fixture.env"
    env_file.write_text(
        "\n".join(
            [
                f"HARTMESH_DATA_DIR={data}",
                "HARTMESH_TENANT=memory-ci",
                "HARTMESH_USERS=10",
                "HARTMESH_AI_ASSISTANTS_PER_USER=1",
                "HARTMESH_AI_EMPLOYEES=10",
                "HARTMESH_USER_STORAGE_GIB=1",
                "HARTMESH_AI_ASSISTANT_STORAGE_GIB=1",
                "HARTMESH_AI_EMPLOYEE_STORAGE_GIB=1",
                "HARTMESH_COMPANY_STORAGE_GIB=1",
                "HARTMESH_PUBLIC_HOST=localhost",
                "HARTMESH_TRUSTED_PROXIES=127.0.0.1",
                "HARTMESH_LISTEN=127.0.0.1:2026",
                "SANDBOX_RUNTIME=runc",
                "POSTGRES_PASSWORD=memory-ci-postgres",
                "REDIS_PASSWORD=memory-ci-redis",
                "AUTH_JWT_SECRET=synthetic-memory-qualification-secret",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    old_profile = directory / "baseline-compose.yaml"
    old_profile.write_text(
        command("git", "show", f"{args.baseline_ref}:deploy/compose/compose.yaml"),
        encoding="utf-8",
    )
    rows = []
    result = dict(
        schema=1,
        source=command("git", "rev-parse", "HEAD"),
        image=args.image,
        baseline_image=args.baseline_image,
        kernel=command("uname", "-r"),
        thp=Path("/sys/kernel/mm/transparent_hugepage/enabled").read_text().strip(),
        samples=rows,
    )
    try:
        for label, profile, image in (
            ("candidate", ROOT / "deploy/compose/compose.yaml", args.image),
            ("baseline", old_profile, args.baseline_image),
        ):
            project = profile_service(
                profile, image, env_file, f"storage-memory-{label}"
            )
            compose_file = directory / f"{label}-service.json"
            compose_file.write_text(json.dumps(project, indent=2), encoding="utf-8")
            negative = []
            modes = [("empty-pool", True)] if label == "candidate" else []
            modes += [(f"fresh-{i}", True) for i in range(1, 6)]
            if label == "candidate":
                modes += [(f"restart-{i}", False) for i in range(1, 6)]
            for mode, recreate in modes:
                sample = measure(
                    compose_file, directory, f"{label}-{mode}", recreate=recreate
                )
                rows.append(sample)
                if label == "candidate":
                    validate_sample(sample)
                    verify_mounts(data)
                else:
                    # A failed negative control must be the memory margin, not an unrelated preparation refusal.
                    if not sample["oom_killed"] and not sample["memory_events"].get(
                        "oom_kill", 0
                    ):
                        validate_sample(
                            sample, require_telemetry=False, require_margin=False
                        )
                    negative.append(
                        sample["peak_bytes"] * 4 > sample["limit_bytes"] * 3
                        or sample["oom_killed"]
                        or sample["memory_events"].get("oom_kill", 0) > 0
                    )
            if label == "baseline" and not all(negative):
                raise ValueError(
                    "baseline did not fail the memory-margin gate in every fresh container"
                )
        result["passed"] = True
    finally:
        result.setdefault("passed", False)
        (directory / "result.json").write_text(
            json.dumps(result, indent=2) + "\n", encoding="utf-8"
        )


if __name__ == "__main__":
    main()
