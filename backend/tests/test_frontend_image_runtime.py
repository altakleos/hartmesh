"""Frontend production-image startup contracts."""

from __future__ import annotations

import json
from pathlib import Path

import yaml

_REPO_ROOT = Path(__file__).resolve().parents[2]
_DOCKERFILE = _REPO_ROOT / "frontend-hm" / "Dockerfile"
_PACKAGE_JSON = _REPO_ROOT / "frontend-hm" / "package.json"


def test_frontend_image_vendors_pinned_pnpm_in_shared_runtime_cache() -> None:
    package_manager = json.loads(_PACKAGE_JSON.read_text(encoding="utf-8"))["packageManager"]
    dockerfile = _DOCKERFILE.read_text(encoding="utf-8")

    assert package_manager == "pnpm@10.26.2"
    assert "FROM node:22-alpine AS pnpm-runtime" in dockerfile
    assert "FROM pnpm-runtime AS base" in dockerfile
    assert "FROM pnpm-runtime AS prod" in dockerfile
    assert dockerfile.count("ENV COREPACK_HOME=/opt/corepack") == 1
    assert dockerfile.count(f"corepack install -g {package_manager}") == 1
    assert dockerfile.count('chmod -R a+rX "${COREPACK_HOME}"') == 1
    assert dockerfile.count("ENV COREPACK_ENABLE_NETWORK=0") == 1


def test_all_product_image_builds_use_hartmesh_source_and_preserve_runtime_paths() -> None:
    dockerfile = _DOCKERFILE.read_text(encoding="utf-8")
    assert "COPY frontend-hm ./frontend\n" in dockerfile
    assert "COPY frontend ./frontend\n" not in dockerfile
    for name in ("docker-compose.yaml", "docker-compose-dev.yaml"):
        compose = yaml.safe_load((_REPO_ROOT / "docker" / name).read_text(encoding="utf-8"))
        frontend = compose["services"]["frontend"]
        assert frontend["build"]["dockerfile"] == "frontend-hm/Dockerfile"
    dev = yaml.safe_load((_REPO_ROOT / "docker/docker-compose-dev.yaml").read_text(encoding="utf-8"))
    assert "../frontend-hm/src:/app/frontend/src" in dev["services"]["frontend"]["volumes"]
    release = yaml.safe_load((_REPO_ROOT / ".github/workflows/container.yaml").read_text(encoding="utf-8"))
    entry = next(item for item in release["jobs"]["container"]["strategy"]["matrix"]["include"] if item["component"] == "frontend")
    assert entry["file"] == "frontend-hm/Dockerfile"


def test_frontend_ci_checks_snapshot_before_install_and_runs_hartmesh() -> None:
    workflow = yaml.safe_load((_REPO_ROOT / ".github/workflows/lint-check.yml").read_text(encoding="utf-8"))
    steps = workflow["jobs"]["lint-frontend"]["steps"]
    assert steps[0]["with"]["fetch-depth"] == 0
    assert steps[1]["run"] == "python3 scripts/verify_frontend_isolation.py --revision HEAD"
    assert any("cd frontend-hm" in step.get("run", "") and "pnpm build" in step["run"] for step in steps)
    replay = (_REPO_ROOT / ".github/workflows/replay-e2e.yml").read_text(encoding="utf-8")
    for trigger in ("frontend-hm/**", "backend/app/gateway/**", "contracts/**"):
        assert replay.count(f'      - "{trigger}"') == 2
    assert "working-directory: frontend\n" not in replay
