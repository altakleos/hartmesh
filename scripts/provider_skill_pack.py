#!/usr/bin/env python3
"""Snapshot trusted skill directories as a stable operator-managed pack."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("name", help="Stable provider pack name")
    parser.add_argument(
        "--home", type=Path, help="Application state root (defaults to DEER_FLOW_HOME)"
    )
    parser.add_argument(
        "--replace", action="store_true", help="Explicitly upgrade an existing pack"
    )
    args = parser.parse_args()
    from deerflow.skills.provider_pack import install_provider_skill_pack

    try:
        result = install_provider_skill_pack(
            args.source, args.name, home=args.home, replace=args.replace
        )
    except (OSError, ValueError) as error:
        parser.exit(1, f"Provider pack snapshot refused: {error}\n")
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
