#!/usr/bin/env python3
"""Exercise production source builds or immutable images in a disposable Docker stack."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
HARNESS_PATHS = (
    ".github/workflows/docker-acceptance.yml",
    "scripts/docker_acceptance.py",
    "scripts/pnpm.py",
    "docker/nginx/nginx.conf",
    "docker/acceptance/compose.yaml",
    "docker/acceptance/compose.postgres-redis.yaml",
    "docker/acceptance/provider.py",
    "docker/acceptance/acceptance-config.yaml",
    "examples/skills",
    "frontend-hm/playwright.docker-acceptance.config.ts",
    "frontend-hm/tests/e2e-docker-acceptance",
    "frontend-hm/package.json",
    "frontend-hm/pnpm-lock.yaml",
    "frontend-hm/.npmrc",
    "frontend-hm/pnpm-workspace.yaml",
)
TITLE_SEARCH_PATHS = (
    ".github/workflows/title-search-measurement.yml",
    "scripts/release_artifacts.py",
    "scripts/release_acceptance.py",
    "docker/acceptance/title-search",
    "frontend-hm/playwright.title-search-measurement.config.ts",
    "frontend-hm/tests/e2e-title-search-measurement",
)
FILE_BROWSER_PATHS = (
    ".github/workflows/file-browser-qualification.yml",
    "scripts/release_artifacts.py",
    "docker/acceptance/file-browser",
    "frontend-hm/playwright.file-browser.config.ts",
    "frontend-hm/tests/e2e-file-browser-qualification",
)
PREVIEW_PATHS = (
    ".github/workflows/preview-measurement.yml",
    "scripts/release_artifacts.py",
    "scripts/release_acceptance.py",
    "docker/acceptance/preview/compose.yaml",
    "docker/acceptance/preview/plugins.yaml",
    "docker/acceptance/preview/sample_projector.py",
    "frontend-hm/playwright.preview-measurement.config.ts",
    "frontend-hm/tests/e2e-preview-measurement",
    "frontend-hm/tests/fixtures/business-report/2026-08-business-review.report.json",
    "frontend-hm/tests/fixtures/office",
)


def snapshot_tracked(source: Path, target: Path) -> str:
    """Copy current tracked bytes, never walking ignored/operator directories."""
    target.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256()
    names = (
        subprocess.check_output(["git", "-C", str(source), "ls-files", "-z"])
        .decode()
        .split("\0")
    )
    for name in filter(None, names):
        relative = Path(name)
        if (
            relative.is_absolute()
            or ".." in relative.parts
            or relative.parts[0] == "panel"
        ):
            raise ValueError(f"Unsafe tracked build path: {name}")
        original, destination = source / relative, target / relative
        if not original.is_symlink() and not original.exists():
            continue  # A tracked deletion in the current working tree.
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(original, destination, follow_symlinks=False)
        content = (
            os.readlink(destination).encode()
            if destination.is_symlink()
            else destination.read_bytes()
        )
        metadata = json.dumps(
            [name, destination.lstat().st_mode, len(content)], separators=(",", ":")
        ).encode()
        digest.update(metadata + b"\0" + content)
    return digest.hexdigest()


def run(
    command: list[str],
    *,
    env: dict[str, str],
    log: Path,
    timeout: int,
    check: bool = True,
) -> subprocess.CompletedProcess:
    with log.open("a", encoding="utf-8") as output:
        process = subprocess.Popen(
            command,
            cwd=ROOT,
            env=env,
            stdout=output,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        try:
            process.wait(timeout=timeout)
        except BaseException:
            # Stop the command's browser/build children as well as its launcher.
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait(timeout=10)
            raise
    result = subprocess.CompletedProcess(command, process.returncode)
    if check and result.returncode:
        raise RuntimeError(f"Command failed ({result.returncode}); see {log}")
    return result


def interrupted(_signal: int, _frame: object) -> None:
    raise KeyboardInterrupt("acceptance interrupted")


def immutable_image(value: str) -> str:
    if (
        not re.fullmatch(r"[a-z0-9][a-z0-9._:/-]*@sha256:[0-9a-f]{64}", value)
        or "://" in value
    ):
        raise argparse.ArgumentTypeError(
            "Expected a repository@sha256:<64 lowercase hex characters> image reference"
        )
    return value


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    measurements = parser.add_mutually_exclusive_group()
    measurements.add_argument(
        "--measure-files",
        choices=("current", "previous-ui", "previous-gateway"),
        help="Qualify the bounded workspace file browser with current or mixed images",
    )
    parser.add_argument(
        "--file-browser-floor",
        help="Published release tag for the previous component of a mixed file-browser check",
    )
    measurements.add_argument(
        "--measure-titles",
        action="store_true",
        help="Measure title search under the PostgreSQL distribution limits",
    )
    measurements.add_argument(
        "--measure-previews",
        action="store_true",
        help="Collect separate preview measurements on supplied candidate digests",
    )
    parser.add_argument(
        "--artifacts",
        type=Path,
        help="Output logs and browser evidence directory (default: a new temporary directory)",
    )
    parser.add_argument(
        "--backend-image",
        type=immutable_image,
        help="Supplied backend image by digest; requires --frontend-image",
    )
    parser.add_argument(
        "--frontend-image",
        type=immutable_image,
        help="Supplied frontend image by digest; requires --backend-image",
    )
    parser.add_argument(
        "--stores",
        choices=("sqlite", "postgres-redis", "both"),
        default="sqlite",
        help="Disposable persistence fixture; both builds one source pair for both stores",
    )
    args = parser.parse_args(argv)
    if bool(args.backend_image) != bool(args.frontend_image):
        parser.error("--backend-image and --frontend-image must be supplied together")
    mixed_files = args.measure_files in ("previous-ui", "previous-gateway")
    if mixed_files != bool(args.file_browser_floor) or (
        mixed_files and not args.backend_image
    ):
        parser.error(
            "A mixed file-browser profile requires its published floor and both immutable images"
        )
    if args.file_browser_floor and not re.fullmatch(
        r"v[0-9]+\.[0-9]+\.[0-9]+\+hartmesh\.[0-9]+", args.file_browser_floor
    ):
        parser.error("File-browser floor must be a published HartMesh version tag")
    if args.measure_files and args.stores == "both":
        parser.error("Qualify individual file-browser storage profiles")
    if args.measure_titles and args.stores != "postgres-redis":
        parser.error("Title search measurement requires the postgres-redis profile")
    if args.measure_previews and (not args.backend_image or args.stores != "sqlite"):
        parser.error("Preview measurement requires both image digests and the sqlite profile")
    if args.stores == "both" and args.backend_image:
        parser.error(
            "--stores both builds source; qualify supplied digests with named stores"
        )
    if os.name != "posix":
        parser.error(
            "Docker acceptance requires POSIX process-group cleanup (Linux/macOS)."
        )
    return args


def inspect_image(image: str, *, env: dict[str, str]) -> dict[str, object]:
    """Record immutable bytes and declared source without exposing image environment."""
    raw = subprocess.check_output(
        [
            "docker",
            "image",
            "inspect",
            "--format",
            # Engine 29 fails a template on a missing key, so `.Config.Labels` is read here instead.
            "{{json .Id}} {{json .RepoDigests}} {{json .Config}}",
            image,
        ],
        cwd=ROOT,
        env=env,
        text=True,
        timeout=30,
    )
    decoder = json.JSONDecoder()
    values = []
    for _ in range(3):
        value, end = decoder.raw_decode(raw.lstrip())
        values.append(value)
        raw = raw.lstrip()[end:]
    return {
        "requested": image,
        "image_id": values[0],
        "repo_digests": values[1] or [],
        "source_revision": ((values[2] or {}).get("Labels") or {}).get("org.opencontainers.image.revision") or None,
    }


def harness_fingerprint(extra_paths: tuple[str, ...] = ()) -> str:
    digest = hashlib.sha256(b"hartmesh-docker-acceptance-v2\0")
    files = []
    for relative in (*HARNESS_PATHS, *extra_paths):
        path = ROOT / relative
        if path.is_symlink() or not path.exists():
            raise ValueError(f"Acceptance harness requires a regular input: {relative}")
        for file in [path] if path.is_file() else path.rglob("*"):
            if file.is_symlink():
                raise ValueError("Acceptance harness does not support symlink inputs")
            if file.is_file():
                files.append(file)
    for file in sorted(files, key=lambda item: item.relative_to(ROOT).as_posix()):
        name = file.relative_to(ROOT).as_posix()
        mode = "100755" if file.stat().st_mode & 0o111 else "100644"
        digest.update(
            mode.encode() + b"\0" + name.encode() + b"\0" + file.read_bytes() + b"\0"
        )
    return digest.hexdigest()


def workflow_identity() -> dict[str, object] | None:
    try:
        run_id = int(os.environ["GITHUB_RUN_ID"])
        attempt = int(os.environ["GITHUB_RUN_ATTEMPT"])
        repository = os.environ["GITHUB_REPOSITORY"]
        event = os.environ["GITHUB_EVENT_NAME"]
        head = os.environ["GITHUB_SHA"]
    except (KeyError, ValueError):
        return None
    return {
        "repository": repository,
        "run_id": run_id,
        "run_attempt": attempt,
        "event": event,
        "head_sha": head,
    }


def check_preview_image(name: str, image: str, repository: str, revision: str | None, expected: str) -> None:
    if image.split("@", 1)[0] != f"ghcr.io/{repository.lower()}-{name}":
        raise ValueError(f"Measurement requires this repository's {name} image")
    if not re.fullmatch(r"[0-9a-f]{40}", expected) or revision != expected:
        raise ValueError(f"Measurement requires a {name} candidate built from the dispatched commit")


def file_browser_image_revision(
    component: str,
    image: str,
    repository: str,
    pair: str,
    floor: str | None,
    commit: str,
    version: str,
) -> tuple[str, str]:
    if image.split("@", 1)[0] != f"ghcr.io/{repository.lower()}-{component}":
        raise ValueError("Unexpected file-browser candidate repository")
    previous = (pair == "previous-ui" and component == "frontend") or (
        pair == "previous-gateway" and component == "backend"
    )
    if previous:
        if not floor:
            raise ValueError("Mixed file-browser check requires a published floor")
        commit = subprocess.check_output(
            ["git", "rev-parse", f"refs/tags/{floor}^{{commit}}"], cwd=ROOT, text=True
        ).strip()
        version = floor.removeprefix("v")
    pins = subprocess.check_output(
        ["git", "show", f"{commit}:deploy/compose/images.txt"], cwd=ROOT, text=True
    ).splitlines()
    if image not in pins:
        raise ValueError(
            f"{component} does not match the selected release's Compose pin"
        )
    return commit, version


def build_source_images(
    backend: str,
    frontend: str,
    *,
    env: dict[str, str],
    artifacts: Path,
    summary: dict[str, object],
) -> None:
    with tempfile.TemporaryDirectory(prefix="hartmesh-acceptance-build-") as temporary:
        context = Path(temporary)
        summary["source_tree_sha256"] = snapshot_tracked(ROOT, context)
        for component, image, dockerfile in (
            ("backend", backend, "backend/Dockerfile"),
            ("frontend", frontend, "frontend-hm/Dockerfile"),
        ):
            print(
                f"Building production {component} from tracked working-tree source",
                flush=True,
            )
            build_args = (
                ["--build-arg", "UV_EXTRAS=postgres"] if component == "backend" else []
            )
            run(
                [
                    "docker",
                    "build",
                    *build_args,
                    "--tag",
                    image,
                    "--file",
                    str(context / dockerfile),
                    str(context),
                ],
                env=env,
                log=artifacts / f"build-{component}.log",
                timeout=1200,
            )


def run_profile(args: argparse.Namespace, source_build: dict | None = None) -> int:
    artifacts = (
        args.artifacts or Path(tempfile.mkdtemp(prefix="hartmesh-acceptance-evidence-"))
    ).resolve()
    artifacts.mkdir(parents=True, exist_ok=True)
    project = f"hm-acceptance-{uuid.uuid4().hex[:12]}"
    title_measurement = args.measure_titles
    files_profile = args.measure_files
    measurement = title_measurement or args.measure_previews or bool(files_profile)
    supplied = bool(args.backend_image)
    backend, frontend = (
        (source_build["backend"], source_build["frontend"])
        if source_build
        else (args.backend_image, args.frontend_image)
        if supplied
        else (f"{project}-backend:local", f"{project}-frontend:local")
    )
    owned_images = [] if supplied or source_build else [backend, frontend]
    env = {
        **os.environ,
        "ACCEPTANCE_BACKEND_IMAGE": backend,
        "ACCEPTANCE_FRONTEND_IMAGE": frontend,
        "COREPACK_ENABLE_NETWORK": "0",
    }
    compose = [
        "docker",
        "compose",
        "--env-file",
        os.devnull,
        "--project-name",
        project,
        "-f",
        str(ROOT / "docker/acceptance/compose.yaml"),
    ]
    if args.stores == "postgres-redis":
        compose += ["-f", str(ROOT / "docker/acceptance/compose.postgres-redis.yaml")]
    summary: dict[str, object] = {
        "schema": 2,
        "project": project,
        "status": "failed",
        "mode": "file-browser-qualification"
        if files_profile
        else "title-search-measurement"
        if title_measurement
        else "preview-measurement"
        if args.measure_previews
        else "images"
        if supplied
        else "source",
        "stores": args.stores,
        "workflow": workflow_identity(),
    }
    print(f"Docker acceptance evidence: {artifacts}", flush=True)
    started = False
    previous_term = signal.signal(signal.SIGTERM, interrupted)
    previous_int = signal.getsignal(signal.SIGINT)
    try:
        commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
        summary["harness_commit"] = commit
        summary["source_commit"] = None if supplied else commit
        paths = (
            FILE_BROWSER_PATHS
            if files_profile
            else TITLE_SEARCH_PATHS
            if title_measurement
            else PREVIEW_PATHS
            if args.measure_previews
            else ()
        )
        summary["harness_sha256"] = harness_fingerprint(paths)
        if measurement:
            verifier_env = env.copy()
            env.pop("GH_TOKEN", None)
        if measurement and not files_profile:
            fixture = ROOT / "docker/acceptance"
            config = (fixture / "acceptance-config.yaml").read_text(encoding="utf-8")
            if title_measurement:
                overlay = fixture / "title-search"
                extra = overlay / "event-storage.yaml"
                generated = artifacts / "title-search-config.yaml"
                env["ACCEPTANCE_TITLE_CONFIG"] = str(generated)
            else:
                overlay = fixture / "preview"
                extra = overlay / "plugins.yaml"
                generated = artifacts / "preview-config.yaml"
                env["ACCEPTANCE_PREVIEW_CONFIG"] = str(generated)
                config = config.replace("log_level: warning\n", "log_level: info\n", 1)
            generated.write_text(config + "\n" + extra.read_text(encoding="utf-8"), encoding="utf-8")
            compose += ["-f", str(overlay / "compose.yaml")]
        # Verify local prerequisites without downloading or installing anything.
        run(
            ["docker", "info"], env=env, log=artifacts / "prerequisites.log", timeout=30
        )
        run(
            [
                sys.executable,
                str(ROOT / "scripts/pnpm.py"),
                "--project",
                "frontend-hm",
                "--",
                "exec",
                "playwright",
                "--version",
            ],
            env=env,
            log=artifacts / "prerequisites.log",
            timeout=30,
        )
        if supplied:
            for image in (backend, frontend):
                available = run(
                    ["docker", "image", "inspect", "--format", "{{.Id}}", image],
                    env=env,
                    log=artifacts / "images.log",
                    timeout=30,
                    check=False,
                )
                if available.returncode:
                    run(
                        ["docker", "pull", image],
                        env=env,
                        log=artifacts / "images.log",
                        timeout=600,
                    )
        elif source_build:
            summary["source_tree_sha256"] = source_build["source_tree_sha256"]
        else:
            build_source_images(
                backend, frontend, env=env, artifacts=artifacts, summary=summary
            )
        summary["images"] = {
            name: inspect_image(image, env=env)
            for name, image in (("backend", backend), ("frontend", frontend))
        }
        if measurement and supplied:
            identity = summary["workflow"]
            if not identity or identity["event"] != "workflow_dispatch" or identity["head_sha"] != commit:
                raise ValueError("Candidate measurement requires an exact-commit workflow dispatch")
            version = json.loads((ROOT / "frontend-hm/package.json").read_text(encoding="utf-8"))["version"]
            summary["release_version"] = version
            for name, image in summary["images"].items():
                expected_revision, expected_version = commit, version
                if files_profile:
                    expected_revision, expected_version = file_browser_image_revision(
                        name,
                        image["requested"],
                        identity["repository"],
                        files_profile,
                        args.file_browser_floor,
                        commit,
                        version,
                    )
                    if expected_revision != commit:
                        summary["previous_release"] = {
                            "tag": args.file_browser_floor,
                            "commit": expected_revision,
                            "component": name,
                        }
                else:
                    check_preview_image(
                        name,
                        image["requested"],
                        identity["repository"],
                        image["source_revision"],
                        commit,
                    )
                run(
                    [
                        sys.executable,
                        str(ROOT / "scripts/release_artifacts.py"),
                        "verify-candidate",
                        "--repository",
                        identity["repository"],
                        "--component",
                        name,
                        "--version",
                        expected_version,
                        "--image",
                        image["requested"],
                        "--revision",
                        expected_revision,
                    ],
                    env=verifier_env,
                    log=artifacts / "provenance.log",
                    timeout=180,
                )
                image["verified_candidate"] = True
        if args.measure_previews:
            summary["activation_evidence"] = {
                "complete": False,
                "unverified": ["container-wide OOM victim selection", "adversarial document and projector workloads"],
            }
        started = True
        run(
            [*compose, "up", "--detach", "--wait", "--wait-timeout", "180"],
            env=env,
            log=artifacts / "startup.log",
            timeout=240,
        )
        for name, identity in summary["images"].items():
            container = subprocess.check_output(
                [*compose, "ps", "-q", name if name != "backend" else "gateway"],
                cwd=ROOT,
                env=env,
                text=True,
                timeout=30,
            ).strip()
            actual = subprocess.check_output(
                ["docker", "inspect", "--format", "{{.Image}}", container],
                cwd=ROOT,
                env=env,
                text=True,
                timeout=30,
            ).strip()
            if actual != identity["image_id"]:
                raise RuntimeError(
                    f"Running {name} image does not match the recorded image"
                )
            identity["running_image_id"] = actual
            if files_profile and name == "backend":
                env["HARTMESH_FILES_GATEWAY_ID"] = container
            if args.measure_previews and name == "backend":
                env["HARTMESH_PREVIEW_GATEWAY_ID"] = container
            if title_measurement and name == "backend":
                env["HARTMESH_TITLE_GATEWAY_ID"] = container
        fixture_images = {}
        for service in [
            "provider",
            "nginx",
            *(["postgres", "redis"] if args.stores == "postgres-redis" else []),
        ]:
            container = subprocess.check_output(
                [*compose, "ps", "-q", service],
                cwd=ROOT,
                env=env,
                text=True,
                timeout=30,
            ).strip()
            image_id = subprocess.check_output(
                ["docker", "inspect", "--format", "{{.Image}}", container],
                cwd=ROOT,
                env=env,
                text=True,
                timeout=30,
            ).strip()
            configured = subprocess.check_output(
                ["docker", "inspect", "--format", "{{.Config.Image}}", container],
                cwd=ROOT,
                env=env,
                text=True,
                timeout=30,
            ).strip()
            if title_measurement and service == "postgres":
                env["HARTMESH_TITLE_POSTGRES_ID"] = container
            fixture_images[service] = {
                **inspect_image(image_id, env=env),
                "configured": configured,
                "running_image_id": image_id,
            }
        summary["fixture_images"] = fixture_images
        binding = subprocess.check_output(
            [*compose, "port", "nginx", "2026"],
            cwd=ROOT,
            env=env,
            text=True,
            timeout=30,
        ).strip()
        if not binding.startswith("127.0.0.1:"):
            raise RuntimeError(f"Acceptance ingress is not loopback: {binding}")
        # nginx answers a GET on 127.0.0.1 with a 301 to localhost (#6156), so
        # a session signed in on the IP spelling would lose its cookie on the
        # next read. Browse the canonical spelling of the same loopback port.
        url = "http://localhost:" + binding.rsplit(":", 1)[1]
        summary["url"] = url
        with urllib.request.urlopen(f"{url}/health/ready", timeout=5) as response:
            summary["readiness"] = json.load(response)
        deadline = time.monotonic() + 90
        while True:
            try:
                with urllib.request.urlopen(f"{url}/login", timeout=5) as response:
                    if response.status == 200:
                        break
            except (urllib.error.URLError, TimeoutError):
                pass
            if time.monotonic() >= deadline:
                raise RuntimeError("Production frontend did not become ready")
            time.sleep(1)
        print(f"Running browser acceptance against {url}", flush=True)
        env.update(
            HARTMESH_ACCEPTANCE_URL=url,
            HARTMESH_ACCEPTANCE_ARTIFACTS=str(artifacts / "browser"),
            HARTMESH_ACCEPTANCE_PYTHON=sys.executable,
        )
        browser_config = "playwright.docker-acceptance.config.ts"
        if files_profile:
            env["HARTMESH_FILES_PAIR"] = files_profile
            summary["file_browser_pair"] = files_profile
            browser_config = "playwright.file-browser.config.ts"
        elif args.measure_previews:
            browser_config = "playwright.preview-measurement.config.ts"
        elif title_measurement:
            browser_config = "playwright.title-search-measurement.config.ts"
        browser = run(
            [
                sys.executable,
                str(ROOT / "scripts/pnpm.py"),
                "--project",
                "frontend-hm",
                "--",
                "exec",
                "playwright",
                "test",
                "--config",
                browser_config,
            ],
            env=env,
            log=artifacts / "browser.log",
            timeout=360 if measurement else 300,
            check=not args.measure_previews,
        )
        if args.measure_previews:
            # Some operators run the Gateway as root. Exercise the child's
            # privilege drop in the same isolated image with a normal report.
            root_probe = (
                "from threading import Event; from deerflow.extensions.projection_host import _invoke; "
                "from hartmesh_legacy_report.projection import project_report; "
                "raw = b'{\"meta\":{}}'; "
                "assert _invoke('project', project_report, raw, 64, 64, 10, Event()) == raw"
            )
            run(
                [*compose, "exec", "-T", "--user", "0:0", "--workdir", "/app/backend", "gateway",
                 "/app/backend/.venv/bin/python", "-c", root_probe],
                env=env, log=artifacts / "root-worker.log", timeout=30,
            )
            summary["root_worker_report"] = "passed"
            if browser.returncode:
                raise RuntimeError("Browser preview checks failed; see browser.log and preview-observations.json")
        summary["status"] = "passed"
    except (
        OSError,
        RuntimeError,
        ValueError,
        subprocess.SubprocessError,
        KeyboardInterrupt,
    ) as error:
        summary["error"] = str(error)
        print(f"Acceptance failed: {error}", file=sys.stderr)
    finally:
        # Repeated termination cannot interrupt the bounded cleanup sequence.
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        cleanup_failed = False
        if started:
            try:
                run(
                    [*compose, "logs", "--no-color"],
                    env=env,
                    log=artifacts / "services.log",
                    timeout=30,
                    check=False,
                )
            except (OSError, subprocess.SubprocessError) as error:
                summary["log_error"] = str(error)
            try:
                run(
                    [
                        *compose,
                        "down",
                        "--volumes",
                        "--remove-orphans",
                        "--timeout",
                        "10",
                    ],
                    env=env,
                    log=artifacts / "cleanup.log",
                    timeout=60,
                )
            except (OSError, RuntimeError, subprocess.SubprocessError) as error:
                cleanup_failed = True
                summary["cleanup_error"] = str(error)
        # Unique names created by this run only. Missing partial-build images are fine.
        for image in owned_images:
            try:
                inspected = run(
                    ["docker", "image", "inspect", "--format", "{{.Id}}", image],
                    env=env,
                    log=artifacts / "cleanup.log",
                    timeout=15,
                    check=False,
                )
                if inspected.returncode == 0:
                    run(
                        ["docker", "image", "rm", image],
                        env=env,
                        log=artifacts / "cleanup.log",
                        timeout=60,
                    )
            except (OSError, RuntimeError, subprocess.SubprocessError) as error:
                cleanup_failed = True
                summary["image_cleanup_error"] = str(error)
        if cleanup_failed:
            summary["status"] = "failed"
        signal.signal(signal.SIGTERM, previous_term)
        signal.signal(signal.SIGINT, previous_int)
        receipt = "title-search-result.json" if title_measurement else "preview-result.json" if args.measure_previews else "result.json"
        (artifacts / receipt).write_text(
            json.dumps(summary, indent=2) + "\n", encoding="utf-8"
        )
    print(f"Docker acceptance {summary['status']}; evidence: {artifacts}", flush=True)
    return 0 if summary["status"] == "passed" else 1


def run_both_source(args: argparse.Namespace) -> int:
    """Build once, qualify the exact pair twice, and remove it after both runs."""
    artifacts = (
        args.artifacts or Path(tempfile.mkdtemp(prefix="hartmesh-pair-evidence-"))
    ).resolve()
    artifacts.mkdir(parents=True, exist_ok=True)
    project = f"hm-acceptance-pair-{uuid.uuid4().hex[:12]}"
    backend, frontend = f"{project}-backend:local", f"{project}-frontend:local"
    env = {**os.environ, "COREPACK_ENABLE_NETWORK": "0"}
    summary = {"schema": 1, "status": "failed", "harness_sha256": harness_fingerprint()}
    previous_term = signal.signal(signal.SIGTERM, interrupted)
    previous_int = signal.getsignal(signal.SIGINT)
    try:
        run(
            ["docker", "info"], env=env, log=artifacts / "prerequisites.log", timeout=30
        )
        run(
            [
                sys.executable,
                str(ROOT / "scripts/pnpm.py"),
                "--project",
                "frontend-hm",
                "--",
                "exec",
                "playwright",
                "--version",
            ],
            env=env,
            log=artifacts / "prerequisites.log",
            timeout=30,
        )
        build_source_images(
            backend, frontend, env=env, artifacts=artifacts, summary=summary
        )
        summary["images"] = {
            name: inspect_image(image, env=env)
            for name, image in (("backend", backend), ("frontend", frontend))
        }
        source = {
            "backend": backend,
            "frontend": frontend,
            "source_tree_sha256": summary["source_tree_sha256"],
        }
        results = {}
        for store in ("sqlite", "postgres-redis"):
            profile = argparse.Namespace(
                artifacts=artifacts / store,
                stores=store,
                backend_image=None,
                frontend_image=None,
                measure_previews=False,
                measure_titles=False,
            )
            run_profile(profile, source_build=source)
            result = json.loads(
                (profile.artifacts / "result.json").read_text(encoding="utf-8")
            )
            results[store] = result
            if result.get("error") == "acceptance interrupted":
                raise KeyboardInterrupt("acceptance interrupted")
        summary["profiles"] = {
            store: result["status"] for store, result in results.items()
        }
        for result in results.values():
            if (
                result["status"] != "passed"
                or result["harness_sha256"] != summary["harness_sha256"]
                or result["source_tree_sha256"] != summary["source_tree_sha256"]
            ):
                raise ValueError(
                    "A profile failed or its source/harness identity changed"
                )
            for name, identity in summary["images"].items():
                actual = result["images"][name]
                if (
                    actual["image_id"] != identity["image_id"]
                    or actual["running_image_id"] != identity["image_id"]
                ):
                    raise ValueError("Profiles did not run the exact built image pair")
        with tempfile.TemporaryDirectory(prefix="hartmesh-pair-verify-") as temporary:
            if snapshot_tracked(ROOT, Path(temporary)) != summary["source_tree_sha256"]:
                raise ValueError("Tracked source changed during qualification")
        if harness_fingerprint() != summary["harness_sha256"]:
            raise ValueError("Harness changed during qualification")
        summary["status"] = "passed"
    except (
        OSError,
        RuntimeError,
        ValueError,
        KeyError,
        subprocess.SubprocessError,
        KeyboardInterrupt,
    ) as error:
        summary["error"] = str(error)
        print(f"Paired acceptance failed: {error}", file=sys.stderr, flush=True)
    finally:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        for image in (backend, frontend):
            try:
                available = run(
                    ["docker", "image", "inspect", "--format", "{{.Id}}", image],
                    env=env,
                    log=artifacts / "cleanup.log",
                    timeout=15,
                    check=False,
                )
                if available.returncode == 0:
                    run(
                        ["docker", "image", "rm", image],
                        env=env,
                        log=artifacts / "cleanup.log",
                        timeout=60,
                    )
            except (OSError, RuntimeError, subprocess.SubprocessError) as error:
                summary.update(status="failed", image_cleanup_error=str(error))
        signal.signal(signal.SIGTERM, previous_term)
        signal.signal(signal.SIGINT, previous_int)
        (artifacts / "pair-result.json").write_text(
            json.dumps(summary, indent=2) + "\n", encoding="utf-8"
        )
    print(
        f"Paired Docker acceptance {summary['status']}; evidence: {artifacts}",
        flush=True,
    )
    return int(summary["status"] != "passed")


def main() -> int:
    args = parse_args()
    return run_both_source(args) if args.stores == "both" else run_profile(args)


if __name__ == "__main__":
    raise SystemExit(main())
