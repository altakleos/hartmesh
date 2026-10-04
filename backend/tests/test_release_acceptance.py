"""Release admission requires authenticated, exact-image acceptance evidence."""

from __future__ import annotations

import copy
import hashlib
import importlib.util
import io
import json
import subprocess
import urllib.error
import zipfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
REPOSITORY = "altakleos/hartmesh"
VERSION = "2.2.0+hartmesh.41"
RUN_ID = 1234
DIGEST = "sha256:" + "a" * 64
IMAGES = {name: f"ghcr.io/{REPOSITORY}-{name}@{DIGEST}" for name in ("backend", "frontend")}


def load():
    spec = importlib.util.spec_from_file_location("release_acceptance", ROOT / "scripts/release_acceptance.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def git(root, *args):
    return subprocess.check_output(["git", "-c", "gc.auto=0", *args], cwd=root, text=True).strip()


def archive(result):
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as bundled:
        bundled.writestr("result.json", json.dumps(result))
    return output.getvalue()


class Actions:
    def __init__(self, guard, revision, fingerprint):
        self.run = {
            "id": RUN_ID,
            "run_attempt": 2,
            "event": "workflow_dispatch",
            "status": "completed",
            "conclusion": "success",
            "path": ".github/workflows/docker-acceptance.yml@main",
            "head_sha": revision,
            "repository": {"id": 77, "full_name": REPOSITORY},
            "head_repository": {"id": 77, "full_name": REPOSITORY},
        }
        self.results = {}
        self.artifacts = []
        self.downloads = []
        self.reread = None
        self.run_reads = 0
        for index, stores in enumerate(guard.STORES, start=1):
            self.results[index] = {
                "schema": 2,
                "status": "passed",
                "mode": "images",
                "stores": stores,
                "harness_commit": revision,
                "harness_sha256": fingerprint,
                "source_commit": None,
                "workflow": {"repository": REPOSITORY, "run_id": RUN_ID, "run_attempt": 2, "event": "workflow_dispatch", "head_sha": revision},
                "images": {name: self.identity(image) for name, image in IMAGES.items()},
                "fixture_images": {name: {**self.identity("sha256:" + "b" * 64), "configured": configured} for name, configured in guard.FIXTURES.items() if stores == "postgres-redis" or name in ("provider", "nginx")},
            }
            self.artifacts.append(
                {
                    "id": index,
                    "name": f"candidate-acceptance-{stores}-2",
                    "expired": False,
                    "workflow_run": {"id": RUN_ID, "repository_id": 77, "head_repository_id": 77, "head_sha": revision},
                }
            )
        self.refresh()

    @staticmethod
    def identity(requested):
        return {"requested": requested, "image_id": "sha256:" + "b" * 64, "running_image_id": "sha256:" + "b" * 64, "repo_digests": [requested] if "@" in requested else ["fixture@" + DIGEST], "source_revision": "c" * 40}

    def refresh(self):
        self.bundles = {key: archive(result) for key, result in self.results.items()}
        for metadata in self.artifacts:
            data = self.bundles[metadata["id"]]
            metadata.update(size_in_bytes=len(data), digest="sha256:" + hashlib.sha256(data).hexdigest())

    def json(self, endpoint):
        if endpoint == f"/actions/runs/{RUN_ID}":
            self.run_reads += 1
            return copy.deepcopy(self.reread if self.run_reads > 1 and self.reread else self.run)
        assert endpoint == f"/actions/runs/{RUN_ID}/artifacts?per_page=100"
        return {"total_count": len(self.artifacts), "artifacts": copy.deepcopy(self.artifacts)}

    def download(self, artifact_id):
        self.downloads.append(artifact_id)
        return self.bundles[artifact_id]


@pytest.fixture
def evidence(tmp_path, monkeypatch):
    guard = load()
    git(tmp_path, "init", "-q")
    git(tmp_path, "config", "user.email", "fixture@example.invalid")
    git(tmp_path, "config", "user.name", "Fixture")
    for relative in guard.HARNESS_PATHS:
        source = ROOT / relative
        files = [source] if source.is_file() else sorted(source.rglob("*"))
        for file in files:
            if not file.is_file():
                continue
            target = tmp_path / file.relative_to(ROOT)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(file.read_bytes())
            target.chmod(file.stat().st_mode & 0o777)
    git(tmp_path, "add", ".")
    git(tmp_path, "commit", "-qm", "fixture harness")
    monkeypatch.chdir(tmp_path)
    revision = git(tmp_path, "rev-parse", "HEAD")
    actions = Actions(guard, revision, guard.harness_fingerprint(revision))
    return guard, actions, tmp_path


def record(evidence):
    guard, actions, _root = evidence
    return guard.record_acceptance(REPOSITORY, VERSION, "HEAD", IMAGES, RUN_ID, client=actions)


def write_record(evidence, value):
    _guard, _actions, root = evidence
    path = root / "deploy/compose/acceptance.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")
    git(root, "add", "deploy/compose/acceptance.json")
    git(root, "commit", "-qm", "record acceptance")


def test_both_stores_accept_exact_images_and_allow_later_notes_and_pins(evidence):
    guard, actions, root = evidence
    value = record(evidence)
    assert value == {"schema": 1, "version": VERSION, "run_id": RUN_ID, "run_attempt": 2, "artifacts": {stores: artifact["digest"] for stores, artifact in zip(guard.STORES, actions.artifacts, strict=True)}}
    write_record(evidence, value)
    (root / "CHANGELOG.md").write_text("release notes\n", encoding="utf-8")
    git(root, "add", "CHANGELOG.md")
    git(root, "commit", "-qm", "notes only")
    assert guard.verify_acceptance(REPOSITORY, VERSION, "HEAD", IMAGES, client=actions) == value
    assert actions.downloads == [1, 2, 1, 2]


def test_local_uncommitted_success_claim_cannot_qualify_a_release(evidence):
    guard, actions, root = evidence
    path = root / "deploy/compose/acceptance.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(record(evidence)), encoding="utf-8")
    with pytest.raises(guard.AcceptanceError, match="acceptance"):
        guard.verify_acceptance(REPOSITORY, VERSION, "HEAD", IMAGES, client=actions)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("status", "in_progress"),
        ("conclusion", "failure"),
        ("event", "pull_request"),
        ("path", ".github/workflows/other.yml"),
        ("repository", {"id": 77, "full_name": "other/repo"}),
        ("head_repository", {"id": 88, "full_name": REPOSITORY}),
        ("head_sha", "bad"),
        ("run_attempt", True),
    ],
)
def test_untrusted_or_incomplete_workflow_run_is_refused(evidence, field, value):
    guard, actions, _root = evidence
    actions.run[field] = value
    with pytest.raises(guard.AcceptanceError):
        record(evidence)
    assert not actions.downloads


@pytest.mark.parametrize("mutation", ["failed", "source", "other-image", "wrong-running-image", "missing-repo-digest", "wrong-harness", "wrong-store", "other-attempt", "missing-fixture", "wrong-fixture", "unknown-schema"])
def test_passed_json_without_the_exact_execution_contract_is_refused(evidence, mutation):
    guard, actions, _root = evidence
    result = actions.results[1]
    if mutation == "failed":
        result["status"] = "failed"
    elif mutation == "source":
        result["mode"] = "source"
    elif mutation == "other-image":
        result["images"]["backend"]["requested"] = IMAGES["frontend"]
    elif mutation == "wrong-running-image":
        result["images"]["backend"]["running_image_id"] = "sha256:" + "d" * 64
    elif mutation == "missing-repo-digest":
        result["images"]["backend"]["repo_digests"] = []
    elif mutation == "wrong-harness":
        result["harness_sha256"] = "d" * 64
    elif mutation == "wrong-store":
        result["stores"] = "postgres-redis"
    elif mutation == "other-attempt":
        result["workflow"]["run_attempt"] = 1
    elif mutation == "missing-fixture":
        result["fixture_images"].pop("provider")
    elif mutation == "wrong-fixture":
        result["fixture_images"]["provider"]["configured"] = "python:other"
    else:
        result["schema"] = 1
    actions.refresh()
    with pytest.raises(guard.AcceptanceError):
        record(evidence)


@pytest.mark.parametrize("mutation", ["expired", "missing-store", "duplicate", "other-run", "other-source", "missing-digest", "archive-changed", "partial-rerun"])
def test_artifact_identity_retention_and_whole_attempt_are_required(evidence, mutation):
    guard, actions, _root = evidence
    artifact = actions.artifacts[0]
    if mutation == "expired":
        artifact["expired"] = True
    elif mutation == "missing-store":
        actions.artifacts.pop()
    elif mutation == "duplicate":
        actions.artifacts.append(copy.deepcopy(artifact))
    elif mutation == "other-run":
        artifact["workflow_run"]["id"] += 1
    elif mutation == "other-source":
        artifact["workflow_run"]["head_repository_id"] += 1
    elif mutation == "missing-digest":
        artifact.pop("digest")
    elif mutation == "archive-changed":
        actions.bundles[1] += b"changed"
    else:
        actions.artifacts[1]["name"] = "candidate-acceptance-postgres-redis-1"
    with pytest.raises(guard.AcceptanceError):
        record(evidence)


def test_new_rerun_during_verification_invalidates_the_evidence(evidence):
    guard, actions, _root = evidence
    actions.reread = {**actions.run, "run_attempt": 3, "status": "in_progress"}
    with pytest.raises(guard.AcceptanceError):
        record(evidence)


def test_committed_pointer_hash_and_version_are_checked(evidence):
    guard, actions, _root = evidence
    value = record(evidence)
    value["artifacts"]["sqlite"] = "sha256:" + "d" * 64
    write_record(evidence, value)
    with pytest.raises(guard.AcceptanceError, match="digest"):
        guard.verify_acceptance(REPOSITORY, VERSION, "HEAD", IMAGES, client=actions)
    with pytest.raises(guard.AcceptanceError, match="version"):
        guard.verify_acceptance(REPOSITORY, "2.2.0+hartmesh.42", "HEAD", IMAGES, client=actions)


def test_new_harness_file_cannot_escape_fingerprint_coverage(evidence):
    guard, _actions, root = evidence
    before = guard.harness_fingerprint("HEAD")
    extra = root / "frontend-hm/tests/e2e-docker-acceptance/extra.spec.ts"
    extra.write_text("throw new Error('extra test');\n", encoding="utf-8")
    git(root, "add", ".")
    git(root, "commit", "-qm", "new executable harness input")
    assert guard.harness_fingerprint("HEAD") != before
    with pytest.raises(guard.AcceptanceError, match="harness"):
        record(evidence)


def test_git_fingerprint_matches_runner_bytes_and_executable_modes(evidence, monkeypatch):
    guard, _actions, root = evidence
    spec = importlib.util.spec_from_file_location("docker_acceptance", ROOT / "scripts/docker_acceptance.py")
    assert spec and spec.loader
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    monkeypatch.setattr(runner, "ROOT", root)
    assert runner.harness_fingerprint() == guard.harness_fingerprint("HEAD")
    source = root / "scripts/pnpm.py"
    source.write_bytes(source.read_bytes() + b"\n")
    source.chmod(0o755 if not source.stat().st_mode & 0o111 else 0o644)
    assert runner.harness_fingerprint() != guard.harness_fingerprint("HEAD")
    git(root, "add", "scripts/pnpm.py")
    git(root, "commit", "-qm", "harness mode and trailing newline")
    assert runner.harness_fingerprint() == guard.harness_fingerprint("HEAD")


@pytest.mark.parametrize("content", [b'{"status":"passed","status":"passed"}', b'{"nested":' + b"[" * 100 + b"0" + b"]" * 100 + b"}", b"[]", b'{"number":NaN}'])
def test_ambiguous_or_deep_json_is_refused(content):
    guard = load()
    with pytest.raises(guard.AcceptanceError):
        guard.read_json(content)


@pytest.mark.parametrize("names", [["elsewhere/result.json"], ["result.json", "extra.txt"], ["result.json", "result.json"]])
def test_zip_never_extracts_or_accepts_extra_or_duplicate_members(names):
    guard = load()
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as bundled:
        for name in names:
            if name in bundled.namelist():
                with pytest.warns(UserWarning, match="Duplicate name"):
                    bundled.writestr(name, "{}")
            else:
                bundled.writestr(name, "{}")
    with pytest.raises(guard.AcceptanceError):
        guard.read_result(output.getvalue())


def test_oversized_download_and_result_are_refused(monkeypatch):
    guard = load()
    monkeypatch.setattr(guard, "MAX_ARCHIVE", 10)
    with pytest.raises(guard.AcceptanceError):
        guard.read_result(b"x" * 11)
    monkeypatch.setattr(guard, "MAX_ARCHIVE", 1024)
    monkeypatch.setattr(guard, "MAX_RESULT", 10)
    with pytest.raises(guard.AcceptanceError):
        guard.read_result(archive({"message": "x" * 20}))


def test_signed_download_does_not_forward_the_github_token(monkeypatch):
    guard = load()
    monkeypatch.setenv("GH_TOKEN", "synthetic-token")
    client = guard.ActionsClient(REPOSITORY)
    requests = []

    def open_request(request, *, timeout):
        requests.append(request)
        assert timeout == 30
        if len(requests) == 1:
            raise urllib.error.HTTPError(request.full_url, 302, "redirect", {"Location": "https://fixture.example/signed?token=opaque"}, None)
        return io.BytesIO(b"fixture ZIP")

    monkeypatch.setattr(client.opener, "open", open_request)
    assert client.download(9) == b"fixture ZIP"
    assert requests[0].full_url == f"https://api.github.com/repos/{REPOSITORY}/actions/artifacts/9/zip"
    assert requests[0].get_header("Authorization") == "Bearer synthetic-token"
    assert requests[1].get_header("Authorization") is None


@pytest.mark.parametrize("url", ["http://fixture.example/file", "https://user:password@fixture.example/file", "file:///tmp/result.zip"])
def test_download_refuses_non_https_or_credential_urls(monkeypatch, url):
    guard = load()
    monkeypatch.setenv("GH_TOKEN", "synthetic-token")
    client = guard.ActionsClient(REPOSITORY)

    def redirect(request, **_kwargs):
        raise urllib.error.HTTPError(request.full_url, 302, "redirect", {"Location": url}, None)

    monkeypatch.setattr(client.opener, "open", redirect)
    with pytest.raises(guard.AcceptanceError):
        client.download(9)
