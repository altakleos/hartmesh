#!/usr/bin/env python3
"""Run pnpm directly when available, otherwise run it through Corepack."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
FRONTEND_DIR = PROJECT_ROOT / "frontend"
COREPACK_NOTICE = "Using pnpm via Corepack."


def _absolute(executable: str) -> str:
    """*executable* as an absolute path when it is a file here.

    The command runs from the selected project's directory, so a relative
    entry on PATH would stop resolving there. A path that is not a file on
    this machine (a Windows shim named on another platform) is left as found.
    """
    path = Path(executable)
    return str(path.resolve()) if path.exists() else str(path)


def find_pnpm_command() -> list[str] | None:
    """Return the preferred pnpm-compatible command for this machine."""
    pnpm_names = ("pnpm.cmd", "pnpm") if os.name == "nt" else ("pnpm", "pnpm.cmd")
    for name in pnpm_names:
        if pnpm_path := shutil.which(name):
            return [_absolute(pnpm_path)]

    corepack_names = ("corepack.cmd", "corepack") if os.name == "nt" else ("corepack", "corepack.cmd")
    for name in corepack_names:
        if corepack_path := shutil.which(name):
            return [_absolute(corepack_path), "pnpm"]
    return None


def run_pnpm(arguments: Sequence[str]) -> int:
    """Run pnpm with the supplied arguments and propagate its exit status."""
    arguments = list(arguments)
    project_dir = FRONTEND_DIR
    if arguments and arguments[0] == "--project":
        if len(arguments) < 2 or arguments[1] not in {"frontend", "frontend-hm"}:
            print("Error: --project must be frontend or frontend-hm.", file=sys.stderr)
            return 2
        project_dir = PROJECT_ROOT / arguments[1]
        arguments = arguments[2:]
        if arguments[:1] == ["--"]:
            arguments = arguments[1:]
    if not (project_dir / "package.json").is_file():
        print(f"Error: selected project is missing {project_dir}/package.json.", file=sys.stderr)
        return 2
    command = find_pnpm_command()
    if command is None:
        print(
            "Error: Neither pnpm nor Corepack is available on PATH.",
            file=sys.stderr,
        )
        print(
            "Install pnpm, or install Corepack and ensure 'corepack' is on PATH.",
            file=sys.stderr,
        )
        return 127

    if Path(command[0]).stem.lower() == "corepack":
        print(COREPACK_NOTICE, file=sys.stderr)

    try:
        result = subprocess.run(
            [*command, *arguments],
            check=False,
            shell=False,
            cwd=project_dir,
        )
    except OSError as exc:
        print(f"Error: Failed to run pnpm via {command[0]}: {exc}", file=sys.stderr)
        return 126

    exit_code = 128 - result.returncode if result.returncode < 0 else result.returncode
    if exit_code != 0:
        print(
            f"Error: pnpm command failed with exit status {exit_code}.",
            file=sys.stderr,
        )
    return exit_code


def main(argv: Sequence[str] | None = None) -> int:
    return run_pnpm(sys.argv[1:] if argv is None else argv)


if __name__ == "__main__":
    sys.exit(main())
