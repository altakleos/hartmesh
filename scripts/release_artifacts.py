#!/usr/bin/env python3
"""Check release admission and bind candidate images to their build inputs.

Verification reads Git, GitHub and the registry. Record creation writes the
verified acceptance pointer; retagging belongs to the admitted workflow.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shlex
import subprocess
import sys
import tempfile
import urllib.error
import urllib.request
from pathlib import Path

from release_acceptance import (
    RECORD,
    AcceptanceError,
    record_acceptance,
    verify_acceptance,
)

COMPONENT_INPUTS = {
    "backend": ("backend", "skills/public", ".dockerignore"),
    "frontend": ("frontend-hm", ".dockerignore"),
    "provisioner": ("docker/provisioner",),
    "sandbox": ("docker/sandbox",),
    "sandbox-network-proxy": (
        "docker/sandbox-network-proxy",
        "backend/packages/harness/deerflow/community/aio_sandbox/network_proxy.py",
        ".dockerignore",
    ),
}
BUILD_CONTEXTS = {
    "backend": (".", "backend/Dockerfile"),
    "frontend": (".", "frontend-hm/Dockerfile"),
    "provisioner": ("docker/provisioner", "docker/provisioner/Dockerfile"),
    "sandbox": ("docker/sandbox", "docker/sandbox/Dockerfile"),
    "sandbox-network-proxy": (".", "docker/sandbox-network-proxy/Dockerfile"),
}
COMMON_INPUTS = (
    ".github/workflows/container.yaml",
    ".github/workflows/verify-versions.yml",
    "scripts/release_artifacts.py",
    "scripts/release_acceptance.py",
    "scripts/verify_versions.sh",
    "scripts/release_tag_spellings.sh",
)
SHA = re.compile(r"[0-9a-f]{40}")
DIGEST = re.compile(r"sha256:[0-9a-f]{64}")


class ReleaseError(Exception):
    """A release condition could not be established."""


def run(*args: str) -> str:
    result = subprocess.run(args, capture_output=True, text=True, check=False)
    if result.returncode:
        raise ReleaseError(f"{args[0]} {args[1]} failed: {result.stderr.strip()}")
    return result.stdout.strip()


def validate_identity(repository: str, version: str) -> None:
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository):
        raise ReleaseError("Invalid repository; expected owner/repository")
    if not re.fullmatch(
        r"(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\+hartmesh\.[1-9][0-9]*",
        version,
    ):
        raise ReleaseError("Invalid release version; use canonical X.Y.Z+hartmesh.N without a leading v")


def check_unpublished(repository: str, version: str) -> None:
    validate_identity(repository, version)
    tag = f"v{version}"
    result = subprocess.run(
        ["git", "ls-remote", "--exit-code", "origin", f"refs/tags/{tag}"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode == 0:
        raise ReleaseError(f"{tag} already exists; published release versions cannot be rebuilt")
    if result.returncode != 2:
        raise ReleaseError(f"Cannot establish whether {tag} exists: remote tag lookup failed")
    token = os.environ.get("GH_TOKEN")
    if not token:
        raise ReleaseError("GH_TOKEN is required to check for an existing GitHub Release")
    request = urllib.request.Request(
        f"https://api.github.com/repos/{repository}/releases/tags/{tag}",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=30):
            pass
    except urllib.error.HTTPError as error:
        if error.code == 404:
            return
        raise ReleaseError(f"Cannot establish whether a GitHub Release exists: HTTP {error.code}") from error
    except (OSError, urllib.error.URLError) as error:
        raise ReleaseError("Cannot establish whether a GitHub Release exists: request failed") from error
    raise ReleaseError(f"A GitHub Release already exists for {tag}; choose a new version")


def resolve_revision(revision: str) -> str:
    resolved = run("git", "rev-parse", "--verify", f"{revision}^{{commit}}")
    if not SHA.fullmatch(resolved):
        raise ReleaseError("Expected a complete Git commit SHA")
    return resolved


def input_fingerprint(component: str, revision: str) -> str:
    """Conservatively hash tracked inputs, including modes and symlink targets.

    Entire copied trees are included rather than reimplementing Dockerignore.
    In particular, skill Markdown is executable guidance shipped in the image.
    Root release notes, documentation and compose pins are outside these trees.
    """
    revision = resolve_revision(revision)
    verify_copy_coverage(component, revision)
    records = run(
        "git",
        "ls-tree",
        "-r",
        "-z",
        "--full-tree",
        revision,
        "--",
        *COMMON_INPUTS,
        *COMPONENT_INPUTS[component],
    )
    if not records:
        raise ReleaseError(f"No build inputs found for {component}")
    for record in records.split("\0"):
        if record and record.split(" ", 1)[0] == "160000":
            raise ReleaseError("Submodule build inputs require an explicit release policy")
    return hashlib.sha256(f"hartmesh-build-inputs-v1\0{component}\0{records}".encode()).hexdigest()


def verify_copy_coverage(component: str, revision: str) -> None:
    """Refuse new Dockerfile sources until the fingerprint policy covers them."""
    context, dockerfile = BUILD_CONTEXTS[component]
    contents = run("git", "show", f"{revision}:{dockerfile}")
    for line in re.sub(r"\\\r?\n", " ", contents).splitlines():
        if re.match(r"^\s*RUN\s", line, re.IGNORECASE):
            for mount in re.findall(r"(?:^|\s)--mount=([^\s]+)", line):
                options = dict(part.split("=", 1) for part in mount.split(",") if "=" in part)
                if options.get("type") != "cache":
                    raise ReleaseError("RUN mounts other than type=cache require explicit build input coverage")
        instruction = re.match(r"^\s*(COPY|ADD)\s+(.+)$", line, re.IGNORECASE)
        if not instruction:
            continue
        operands = instruction.group(2)
        from_stage = False
        while operands.startswith("--"):
            flag, separator, operands = operands.partition(" ")
            if not separator or "=" not in flag:
                raise ReleaseError(f"Unsupported Dockerfile build input option: {flag}")
            from_stage |= flag.startswith("--from=")
            operands = operands.lstrip()
        if from_stage:
            continue
        try:
            paths = json.loads(operands) if operands.startswith("[") else shlex.split(operands)
            if not isinstance(paths, list) or len(paths) < 2 or not all(isinstance(path, str) for path in paths):
                raise ValueError
        except (ValueError, TypeError) as error:
            raise ReleaseError("Cannot establish Dockerfile build input coverage") from error
        for source in paths[:-1]:
            if source.startswith("/") or "$" in source or "://" in source or ".." in Path(source).parts:
                raise ReleaseError(f"Unsupported Dockerfile build input: {source}")
            path = (Path(context) / source).as_posix().rstrip("/")
            if not any(path == root or path.startswith(f"{root}/") for root in (*COMMON_INPUTS, *COMPONENT_INPUTS[component])):
                raise ReleaseError(f"Dockerfile build input {path} is outside the fingerprint policy; update coverage before building")


def verify_candidate(repository: str, component: str, version: str, image: str, revision: str) -> None:
    validate_identity(repository, version)
    image_repository = f"ghcr.io/{repository.lower()}-{component}"
    if not image.startswith(f"{image_repository}@") or not DIGEST.fullmatch(image.removeprefix(f"{image_repository}@")):
        raise ReleaseError("Candidate image must be this component's immutable sha256 reference")
    try:
        config = json.loads(run("crane", "config", "--platform", "linux/amd64", image))
        labels = config["config"]["Labels"]
        source = labels["org.opencontainers.image.source"]
        candidate = labels["org.opencontainers.image.revision"]
        fingerprint = labels["io.hartmesh.build-inputs"]
        candidate_version = labels["io.hartmesh.release-version"]
    except (ValueError, KeyError, TypeError) as error:
        raise ReleaseError("Candidate image lacks valid source and build-input labels") from error
    if source != f"https://github.com/{repository}" or not isinstance(candidate, str) or not SHA.fullmatch(candidate):
        raise ReleaseError("Candidate image has an unexpected source repository or revision")
    if candidate_version != version:
        raise ReleaseError("Candidate image was built for another release version")
    if not isinstance(fingerprint, str) or not re.fullmatch(r"[0-9a-f]{64}", fingerprint):
        raise ReleaseError("Candidate image lacks a valid build inputs fingerprint")
    # The OCI labels are assertions until the signed provenance binds this
    # exact image digest to this repository, workflow and source commit.
    run(
        "gh",
        "attestation",
        "verify",
        f"oci://{image}",
        "--repo",
        repository,
        "--signer-workflow",
        f"{repository}/.github/workflows/container.yaml",
        "--source-digest",
        candidate,
        "--signer-digest",
        candidate,
        "--deny-self-hosted-runners",
    )
    try:
        resolve_revision(candidate)
    except ReleaseError:
        run("git", "-c", "gc.auto=0", "fetch", "--no-tags", "origin", candidate)
        resolve_revision(candidate)
    if input_fingerprint(component, candidate) != fingerprint:
        raise ReleaseError(f"{component} candidate build inputs do not match its recorded fingerprint")
    if input_fingerprint(component, revision) != fingerprint:
        raise ReleaseError(f"{component} build inputs changed after the candidate; rebuild and repin before tagging")


def _verified_candidates(repository: str, version: str, revision: str) -> dict[str, str]:
    """Shared candidate proof for record creation and release admission."""
    validate_identity(repository, version)
    revision = resolve_revision(revision)
    pins = run("git", "show", f"{revision}:deploy/compose/images.txt").splitlines()
    candidates = {}
    for component in COMPONENT_INPUTS:
        image_repository = f"ghcr.io/{repository.lower()}-{component}"
        digest = run("crane", "digest", f"{image_repository}:v{version.replace('+', '-')}")
        if not DIGEST.fullmatch(digest):
            raise ReleaseError(f"{component} candidate does not resolve to a sha256 digest")
        image = f"{image_repository}@{digest}"
        component_pins = [line for line in pins if line.startswith((f"{image_repository}@", f"{image_repository}:"))]
        if component != "provisioner" and component_pins != [image]:
            raise ReleaseError(f"{component} candidate must exactly match its compose digest pin")
        if component_pins and component_pins != [image]:
            raise ReleaseError(f"{component} has an invalid or mismatched compose pin")
        verify_candidate(repository, component, version, image, revision)
        candidates[component] = image
    return candidates


def verify_release(repository: str, version: str, revision: str) -> dict[str, str]:
    """Verify every candidate and its image acceptance before any publication."""
    candidates = _verified_candidates(repository, version, revision)
    try:
        verify_acceptance(repository, version, resolve_revision(revision), candidates)
    except AcceptanceError as error:
        raise ReleaseError(str(error)) from error
    return candidates


def write_acceptance_record(repository: str, version: str, revision: str, run_id: int) -> None:
    candidates = _verified_candidates(repository, version, revision)
    accepted = record_acceptance(repository, version, resolve_revision(revision), candidates, run_id)
    destination = Path(RECORD)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=destination.parent, delete=False) as output:
            temporary = Path(output.name)
            output.write(json.dumps(accepted, indent=2) + "\n")
        temporary.replace(destination)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    unpublished = commands.add_parser("check-unpublished")
    inputs = commands.add_parser("inputs")
    candidate = commands.add_parser("verify-candidate")
    release = commands.add_parser("verify-release")
    record = commands.add_parser("record-acceptance")
    record.add_argument("--run-id", type=int, required=True)
    for command in (unpublished, candidate, release, record):
        command.add_argument("--repository", required=True)
        command.add_argument("--version", required=True)
    for command in (inputs, candidate):
        command.add_argument("--component", choices=COMPONENT_INPUTS, required=True)
    for command in (inputs, candidate, release, record):
        command.add_argument("--revision", default="HEAD")
    candidate.add_argument("--image", required=True)
    args = parser.parse_args()
    try:
        if args.command == "check-unpublished":
            check_unpublished(args.repository, args.version)
        elif args.command == "inputs":
            print(input_fingerprint(args.component, args.revision))
        elif args.command == "verify-candidate":
            verify_candidate(args.repository, args.component, args.version, args.image, args.revision)
        elif args.command == "record-acceptance":
            write_acceptance_record(args.repository, args.version, args.revision, args.run_id)
        else:
            print(
                json.dumps(
                    verify_release(args.repository, args.version, args.revision),
                    sort_keys=True,
                )
            )
    except (ReleaseError, AcceptanceError, OSError) as error:
        print(f"Release verification failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
