"""Build-only entry point for the installed extension artifact manifest."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from pathlib import Path

from deerflow_extension_api import API_VERSION

from deerflow.extensions.artifacts import (
    build_installed_artifact_manifest,
    read_source_lock,
    verify_source_lock_current,
    write_artifact_manifest,
)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Verify installed extensions and write the image artifact manifest.")
    parser.add_argument("--source-lock", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--backend-dir", type=Path, required=True)
    args = parser.parse_args(list(argv) if argv is not None else None)
    source_lock = read_source_lock(args.source_lock)
    verify_source_lock_current(source_lock, args.backend_dir)
    manifest = build_installed_artifact_manifest(
        source_lock,
        backend_dir=args.backend_dir,
        expected_extension_api_version=API_VERSION,
    )
    write_artifact_manifest(args.output, manifest)
    print(manifest.digest)
    return 0


if __name__ == "__main__":  # pragma: no cover - exercised by image builds
    raise SystemExit(main())
