"""Bind release admission to one authenticated, completed image acceptance run."""

from __future__ import annotations

import hashlib
import io
import json
import os
import re
import subprocess
import urllib.error
import urllib.parse
import urllib.request
import zipfile
import zlib

RECORD = "deploy/compose/acceptance.json"
WORKFLOW = ".github/workflows/docker-acceptance.yml"
STORES = ("sqlite", "postgres-redis")
FIXTURES = {
    "provider": "python:3.12-alpine",
    "nginx": "nginx:alpine",
    "postgres": "postgres:16-alpine",
    "redis": "redis:7-alpine",
}
HARNESS_PATHS = (
    WORKFLOW,
    "scripts/docker_acceptance.py",
    "scripts/pnpm.py",
    "docker/nginx/nginx.conf",
    "docker/acceptance/compose.yaml",
    "docker/acceptance/compose.postgres-redis.yaml",
    "docker/acceptance/provider.py",
    "docker/acceptance/acceptance-config.yaml",
    "frontend-hm/playwright.docker-acceptance.config.ts",
    "frontend-hm/tests/e2e-docker-acceptance",
    "frontend-hm/package.json",
    "frontend-hm/pnpm-lock.yaml",
    "frontend-hm/.npmrc",
    "frontend-hm/pnpm-workspace.yaml",
)
MAX_ARCHIVE = 2 * 1024 * 1024
MAX_RESULT = 256 * 1024
MAX_API = 1024 * 1024
DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
SHA = re.compile(r"[0-9a-f]{40}\Z")
REFERENCE = re.compile(r"[a-z0-9][a-z0-9._:/-]*@sha256:[0-9a-f]{64}\Z")


class AcceptanceError(ValueError):
    """Exact-image acceptance could not be established."""


def positive(value: object) -> bool:
    return type(value) is int and value > 0


def read_json(data: bytes) -> dict:
    def reject_constant(_value):
        raise ValueError("nonfinite JSON")

    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise AcceptanceError("acceptance JSON contains duplicate keys")
            result[key] = value
        return result

    try:
        result = json.loads(
            data.decode("utf-8"),
            object_pairs_hook=unique,
            parse_constant=reject_constant,
        )
    except (UnicodeError, ValueError, RecursionError) as error:
        raise AcceptanceError("acceptance JSON is invalid") from error
    if not isinstance(result, dict):
        raise AcceptanceError("acceptance JSON must be an object")
    pending = [(result, 0)]
    while pending:
        value, depth = pending.pop()
        if depth > 32:
            raise AcceptanceError("acceptance JSON exceeds nesting limit")
        if isinstance(value, dict):
            pending.extend((child, depth + 1) for child in value.values())
        elif isinstance(value, list):
            pending.extend((child, depth + 1) for child in value)
    return result


def git(*args: str) -> bytes:
    try:
        result = subprocess.run(
            ["git", *args], capture_output=True, timeout=120, check=True
        )
    except (OSError, subprocess.SubprocessError) as error:
        raise AcceptanceError(
            "acceptance committed evidence is missing or unreadable"
        ) from error
    return result.stdout


def git_file(revision: str, name: str, limit: int) -> bytes:
    tree = git("ls-tree", revision, "--", name).decode().split()
    if len(tree) != 4 or tree[0] not in ("100644", "100755") or tree[3] != name:
        raise AcceptanceError(f"acceptance requires a committed regular file: {name}")
    if int(git("cat-file", "-s", f"{revision}:{name}")) > limit:
        raise AcceptanceError("acceptance committed input exceeds size limit")
    return git("show", f"{revision}:{name}")


def harness_fingerprint(revision: str) -> str:
    records = git(
        "ls-tree", "-rz", "--full-tree", revision, "--", *HARNESS_PATHS
    ).split(b"\0")
    entries = []
    for record in filter(None, records):
        metadata, name = record.decode().split("\t", 1)
        mode, kind, _oid = metadata.split()
        if mode not in ("100644", "100755") or kind != "blob":
            raise AcceptanceError(
                "acceptance harness must contain regular tracked files"
            )
        entries.append((name, mode))
    if any(
        not any(name == path or name.startswith(path + "/") for name, _mode in entries)
        for path in HARNESS_PATHS
    ):
        raise AcceptanceError("acceptance harness input is missing")
    digest = hashlib.sha256(b"hartmesh-docker-acceptance-v2\0")
    for name, mode in sorted(entries):
        digest.update(
            mode.encode()
            + b"\0"
            + name.encode()
            + b"\0"
            + git_file(revision, name, 4 * 1024 * 1024)
            + b"\0"
        )
    return digest.hexdigest()


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *_args, **_kwargs):
        return None


class ActionsClient:
    def __init__(self, repository: str):
        if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository):
            raise AcceptanceError("acceptance repository is invalid")
        token = os.environ.get("GH_TOKEN")
        if not token:
            raise AcceptanceError(
                "GH_TOKEN with Actions read permission is required for acceptance"
            )
        self.base = f"https://api.github.com/repos/{repository}"
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        self.opener = urllib.request.build_opener(NoRedirect())

    @staticmethod
    def read(response, limit):
        data = response.read(limit + 1)
        if len(data) > limit:
            raise AcceptanceError("acceptance response exceeds size limit")
        return data

    def json(self, endpoint: str) -> dict:
        request = urllib.request.Request(self.base + endpoint, headers=self.headers)
        try:
            with self.opener.open(request, timeout=30) as response:
                return read_json(self.read(response, MAX_API))
        except (OSError, urllib.error.URLError) as error:
            raise AcceptanceError("acceptance Actions lookup failed") from error

    def download(self, artifact_id: int) -> bytes:
        if not positive(artifact_id):
            raise AcceptanceError("acceptance artifact ID is invalid")
        request = urllib.request.Request(
            f"{self.base}/actions/artifacts/{artifact_id}/zip", headers=self.headers
        )
        try:
            with self.opener.open(request, timeout=30):
                raise AcceptanceError(
                    "acceptance artifact download did not provide a signed redirect"
                )
        except urllib.error.HTTPError as error:
            location = error.headers.get("Location", "")
            code = error.code
            error.close()
            if code != 302:
                raise AcceptanceError(
                    "acceptance artifact download is unavailable or expired"
                ) from error
        except (OSError, urllib.error.URLError) as error:
            raise AcceptanceError("acceptance artifact download failed") from error
        try:
            parsed = urllib.parse.urlsplit(location)
        except ValueError as error:
            raise AcceptanceError("acceptance artifact redirect is invalid") from error
        if (
            len(location) > 8192
            or parsed.scheme != "https"
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
        ):
            raise AcceptanceError(
                "acceptance artifact redirect must be an unauthenticated HTTPS URL"
            )
        # A separate request deliberately carries no GitHub bearer credentials.
        try:
            with self.opener.open(
                urllib.request.Request(location), timeout=30
            ) as response:
                return self.read(response, MAX_ARCHIVE)
        except (OSError, urllib.error.URLError) as error:
            raise AcceptanceError(
                "acceptance signed artifact download failed"
            ) from error


def read_result(data: bytes) -> dict:
    if len(data) > MAX_ARCHIVE:
        raise AcceptanceError("acceptance archive exceeds size limit")
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            members = archive.infolist()
            if (
                len(members) != 1
                or members[0].filename != "result.json"
                or members[0].file_size > MAX_RESULT
                or members[0].flag_bits & 1
            ):
                raise AcceptanceError(
                    "acceptance archive must contain one bounded result.json"
                )
            with archive.open(members[0]) as result:
                return read_json(ActionsClient.read(result, MAX_RESULT))
    except (
        OSError,
        ValueError,
        RuntimeError,
        zipfile.BadZipFile,
        NotImplementedError,
        zlib.error,
    ) as error:
        raise AcceptanceError("acceptance archive is invalid") from error


def validate_run(run: dict, repository: str, run_id: int, attempt: int | None) -> None:
    workflow = run.get("path", "")
    source, _separator, ref = (
        workflow.partition("@") if isinstance(workflow, str) else ("", "", "")
    )
    origin, head = run.get("repository"), run.get("head_repository")
    if (
        type(run.get("id")) is not int
        or run["id"] != run_id
        or not positive(run.get("run_attempt"))
        or (attempt is not None and run["run_attempt"] != attempt)
        or run.get("status") != "completed"
        or run.get("conclusion") != "success"
        or run.get("event") != "workflow_dispatch"
        or source != WORKFLOW
        or "@" in ref
        or (_separator and not ref)
        or not isinstance(run.get("head_sha"), str)
        or not SHA.fullmatch(run["head_sha"])
        or not isinstance(origin, dict)
        or not isinstance(head, dict)
        or origin.get("full_name") != repository
        or head.get("full_name") != repository
        or not positive(origin.get("id"))
        or type(head.get("id")) is not int
        or origin["id"] != head["id"]
    ):
        raise AcceptanceError(
            "acceptance requires a completed successful same-repository image workflow dispatch and attempt"
        )


def validate_identity(
    identity: object, requested: str, *, configured: str | None = None
) -> None:
    if not isinstance(identity, dict):
        raise AcceptanceError("acceptance image identity is missing")
    image_id = identity.get("image_id")
    digests = identity.get("repo_digests")
    if (
        not isinstance(image_id, str)
        or not DIGEST.fullmatch(image_id)
        or identity.get("running_image_id") != image_id
        or identity.get("requested") != requested
    ):
        raise AcceptanceError(
            "acceptance running image differs from its inspected identity"
        )
    if (
        not isinstance(digests, list)
        or not digests
        or any(
            not isinstance(value, str) or not REFERENCE.fullmatch(value)
            for value in digests
        )
    ):
        raise AcceptanceError(
            "acceptance image repository digests are missing or invalid"
        )
    if configured is None and requested not in digests:
        raise AcceptanceError("acceptance image digest differs from the candidate")
    if configured is not None and identity.get("configured") != configured:
        raise AcceptanceError("acceptance fixture declaration differs from the harness")


def validate_result(
    result: dict,
    stores: str,
    repository: str,
    run: dict,
    fingerprint: str,
    candidates: dict[str, str],
) -> None:
    expected_workflow = {
        "repository": repository,
        "run_id": run["id"],
        "run_attempt": run["run_attempt"],
        "event": "workflow_dispatch",
        "head_sha": run["head_sha"],
    }
    if (
        type(result.get("schema")) is not int
        or result["schema"] != 2
        or result.get("status") != "passed"
        or result.get("mode") != "images"
        or result.get("stores") != stores
        or result.get("source_commit") is not None
    ):
        raise AcceptanceError(
            "acceptance result is not a passed image-mode result for this store"
        )
    if (
        result.get("harness_commit") != run["head_sha"]
        or result.get("harness_sha256") != fingerprint
        or result.get("workflow") != expected_workflow
    ):
        raise AcceptanceError(
            "acceptance result has another harness or workflow identity"
        )
    images = result.get("images")
    if not isinstance(images, dict) or set(images) != {"backend", "frontend"}:
        raise AcceptanceError("acceptance application image evidence is missing")
    for name in images:
        validate_identity(images[name], candidates[name])
    fixtures = result.get("fixture_images")
    expected = (
        FIXTURES
        if stores == "postgres-redis"
        else {name: FIXTURES[name] for name in ("provider", "nginx")}
    )
    if not isinstance(fixtures, dict) or set(fixtures) != set(expected):
        raise AcceptanceError("acceptance fixture identity is missing")
    for name, configured in expected.items():
        identity = fixtures[name]
        if not isinstance(identity, dict):
            raise AcceptanceError("acceptance fixture identity is invalid")
        validate_identity(identity, identity.get("image_id"), configured=configured)


def check(
    repository: str,
    version: str,
    revision: str,
    candidates: dict[str, str],
    run_id: int,
    attempt: int | None,
    hashes: dict | None,
    client,
) -> dict:
    if not positive(run_id):
        raise AcceptanceError("acceptance workflow run ID is invalid")
    for name in ("backend", "frontend"):
        reference = candidates.get(name, "")
        if (
            not isinstance(reference, str)
            or not reference.startswith(f"ghcr.io/{repository.lower()}-{name}@")
            or not REFERENCE.fullmatch(reference)
        ):
            raise AcceptanceError("acceptance candidate image reference is invalid")
    client = client or ActionsClient(repository)
    run = client.json(f"/actions/runs/{run_id}")
    validate_run(run, repository, run_id, attempt)
    try:
        git("rev-parse", "--verify", f"{run['head_sha']}^{{commit}}")
    except AcceptanceError:
        git("-c", "gc.auto=0", "fetch", "--no-tags", "origin", run["head_sha"])
    fingerprint = harness_fingerprint(revision)
    if harness_fingerprint(run["head_sha"]) != fingerprint:
        raise AcceptanceError(
            "acceptance harness changed after the image test; rerun both stores"
        )
    listing = client.json(f"/actions/runs/{run_id}/artifacts?per_page=100")
    artifacts = listing.get("artifacts")
    if (
        not isinstance(artifacts, list)
        or type(listing.get("total_count")) is not int
        or listing["total_count"] != len(artifacts)
        or len(artifacts) > 100
    ):
        raise AcceptanceError("acceptance artifact listing is incomplete")
    verified = {}
    for stores in STORES:
        matches = [
            artifact
            for artifact in artifacts
            if isinstance(artifact, dict)
            and artifact.get("name")
            == f"candidate-acceptance-{stores}-{run['run_attempt']}"
        ]
        if len(matches) != 1:
            raise AcceptanceError(
                "acceptance requires both store artifacts from the same complete attempt; rerun all jobs"
            )
        artifact = matches[0]
        digest = artifact.get("digest")
        expected_source = {
            "id": run_id,
            "repository_id": run["repository"]["id"],
            "head_repository_id": run["head_repository"]["id"],
            "head_sha": run["head_sha"],
        }
        source = artifact.get("workflow_run")
        if (
            artifact.get("expired") is not False
            or not positive(artifact.get("id"))
            or not positive(artifact.get("size_in_bytes"))
            or artifact["size_in_bytes"] > MAX_ARCHIVE
            or not isinstance(digest, str)
            or not DIGEST.fullmatch(digest)
            or not isinstance(source, dict)
            or any(
                type(source.get(key)) is not type(value) or source.get(key) != value
                for key, value in expected_source.items()
            )
        ):
            raise AcceptanceError(
                "acceptance artifact identity or retention is invalid"
            )
        if hashes is not None and hashes[stores] != digest:
            raise AcceptanceError(
                "acceptance artifact digest differs from the committed record"
            )
        data = client.download(artifact["id"])
        if (
            len(data) != artifact["size_in_bytes"]
            or "sha256:" + hashlib.sha256(data).hexdigest() != digest
        ):
            raise AcceptanceError(
                "acceptance archive digest differs from authenticated Actions metadata"
            )
        validate_result(
            read_result(data), stores, repository, run, fingerprint, candidates
        )
        verified[stores] = digest
    latest = client.json(f"/actions/runs/{run_id}")
    validate_run(latest, repository, run_id, run["run_attempt"])
    if any(
        latest.get(key) != run.get(key)
        for key in ("head_sha", "repository", "head_repository", "path")
    ):
        raise AcceptanceError(
            "acceptance workflow identity changed during verification"
        )
    return {
        "schema": 1,
        "version": version,
        "run_id": run_id,
        "run_attempt": run["run_attempt"],
        "artifacts": verified,
    }


def record_acceptance(
    repository: str,
    version: str,
    revision: str,
    candidates: dict[str, str],
    run_id: int,
    *,
    client=None,
) -> dict:
    return check(repository, version, revision, candidates, run_id, None, None, client)


def verify_acceptance(
    repository: str,
    version: str,
    revision: str,
    candidates: dict[str, str],
    *,
    client=None,
) -> dict:
    record = read_json(git_file(revision, RECORD, 8192))
    if (
        set(record) != {"schema", "version", "run_id", "run_attempt", "artifacts"}
        or type(record["schema"]) is not int
        or record["schema"] != 1
        or not positive(record["run_id"])
        or not positive(record["run_attempt"])
    ):
        raise AcceptanceError("acceptance record is invalid")
    if record["version"] != version:
        raise AcceptanceError("acceptance record names another release version")
    hashes = record["artifacts"]
    if (
        not isinstance(hashes, dict)
        or set(hashes) != set(STORES)
        or any(
            not isinstance(value, str) or not DIGEST.fullmatch(value)
            for value in hashes.values()
        )
    ):
        raise AcceptanceError("acceptance record artifact digests are invalid")
    return check(
        repository,
        version,
        revision,
        candidates,
        record["run_id"],
        record["run_attempt"],
        hashes,
        client,
    )
