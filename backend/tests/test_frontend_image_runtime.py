"""Frontend production-image startup contracts."""

from __future__ import annotations

import json
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
_DOCKERFILE = _REPO_ROOT / "frontend" / "Dockerfile"
_PACKAGE_JSON = _REPO_ROOT / "frontend" / "package.json"


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
