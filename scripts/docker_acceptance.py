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
        choices=("sqlite", "postgres-redis"),
        default="sqlite",
        help="Disposable persistence fixture (default: sqlite)",
    )
    args = parser.parse_args(argv)
    if bool(args.backend_image) != bool(args.frontend_image):
        parser.error("--backend-image and --frontend-image must be supplied together")
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
            '{{json .Id}} {{json .RepoDigests}} {{if .Config.Labels}}{{json (index .Config.Labels "org.opencontainers.image.revision")}}{{else}}null{{end}}',
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
        "source_revision": values[2] or None,
    }


def harness_fingerprint() -> str:
    digest = hashlib.sha256()
    files = [
        "scripts/docker_acceptance.py",
        "scripts/pnpm.py",
        "docker/nginx/nginx.conf",
        "docker/acceptance/compose.yaml",
        "docker/acceptance/compose.postgres-redis.yaml",
        "docker/acceptance/provider.py",
        "docker/acceptance/acceptance-config.yaml",
        "frontend-hm/playwright.docker-acceptance.config.ts",
        "frontend-hm/tests/e2e-docker-acceptance/journey.spec.ts",
        "frontend-hm/package.json",
        "frontend-hm/pnpm-lock.yaml",
    ]
    for name in sorted(files):
        digest.update(name.encode() + b"\0" + (ROOT / name).read_bytes() + b"\0")
    return digest.hexdigest()


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


def main() -> int:
    args = parse_args()
    artifacts = (
        args.artifacts or Path(tempfile.mkdtemp(prefix="hartmesh-acceptance-evidence-"))
    ).resolve()
    artifacts.mkdir(parents=True, exist_ok=True)
    project = f"hm-acceptance-{uuid.uuid4().hex[:12]}"
    supplied = bool(args.backend_image)
    backend, frontend = (
        (args.backend_image, args.frontend_image)
        if supplied
        else (f"{project}-backend:local", f"{project}-frontend:local")
    )
    owned_images = [] if supplied else [backend, frontend]
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
    commit = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
    ).strip()
    summary: dict[str, object] = {
        "project": project,
        "status": "failed",
        "mode": "images" if supplied else "source",
        "stores": args.stores,
        "harness_commit": commit,
        "harness_sha256": harness_fingerprint(),
        "source_commit": None if supplied else commit,
    }
    print(f"Docker acceptance evidence: {artifacts}", flush=True)
    started = False
    previous_term = signal.signal(signal.SIGTERM, interrupted)
    previous_int = signal.getsignal(signal.SIGINT)
    try:
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
        else:
            build_source_images(
                backend, frontend, env=env, artifacts=artifacts, summary=summary
            )
        summary["images"] = {
            name: inspect_image(image, env=env)
            for name, image in (("backend", backend), ("frontend", frontend))
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
            fixture_images[service] = inspect_image(image_id, env=env)
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
        url = f"http://{binding}"
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
        run(
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
                "playwright.docker-acceptance.config.ts",
            ],
            env=env,
            log=artifacts / "browser.log",
            timeout=300,
        )
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
        (artifacts / "result.json").write_text(
            json.dumps(summary, indent=2) + "\n", encoding="utf-8"
        )
    print(f"Docker acceptance {summary['status']}; evidence: {artifacts}", flush=True)
    return 0 if summary["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
