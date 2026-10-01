#!/usr/bin/env python3
"""Initialize Hartmesh's local frontend settings without overwriting them."""

from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path


def ensure_frontend_env(project_root: Path) -> str:
    target = project_root / "frontend-hm" / ".env"
    if target.is_file():
        return "existing"
    legacy = project_root / "frontend" / ".env"
    source = legacy if legacy.is_file() else target.with_name(".env.example")
    # Open the source first, and create the destination exclusively. Another
    # setup process or a dangling symlink must never cause an overwrite.
    with source.open("rb") as source_file:
        try:
            descriptor = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError:
            if target.is_file():
                return "existing"
            raise
        try:
            with os.fdopen(descriptor, "wb") as destination:
                shutil.copyfileobj(source_file, destination)
        except BaseException:
            target.unlink()
            raise
    origin = "frontend/.env" if source == legacy else "frontend-hm/.env.example"
    print(f"Created frontend-hm/.env from {origin}")
    return origin


def main() -> int:
    try:
        ensure_frontend_env(Path(__file__).resolve().parent.parent)
    except OSError as exc:
        print(f"Unable to initialize frontend-hm/.env: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
