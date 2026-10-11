#!/usr/bin/env python3
"""Explicit provider preparation of new, fixed Storage Spaces filesystems.

Uses existing Linux tools; never installs dependencies or reads app config.
Format targets are exclusively newly created regular image files. This is a
data-disk helper, not a VM-image builder, ordinary file API or live-writer fence.
"""

from __future__ import annotations

import argparse
import json
import os  # noqa: F401
import shutil  # noqa: F401
import subprocess  # noqa: F401
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend" / "packages" / "harness"))

from deerflow.spaces.backings import PreparedVolumeCatalog  # noqa: E402,F401
from deerflow.spaces.provision import prepare_volumes  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-disk", type=Path, required=True)
    parser.add_argument("--count", type=int, default=1)
    parser.add_argument("--bytes", type=int, required=True, dest="size_bytes")
    parser.add_argument("--inodes", type=int, required=True)
    parser.add_argument("--reserve-bytes", type=int, required=True)
    parser.add_argument("--reserve-inodes", type=int, required=True)
    parser.add_argument("--data-uid", type=int, default=1000)
    values = vars(parser.parse_args())
    specs = prepare_volumes(**values)
    print(
        json.dumps(
            {
                "inventory": str(values["data_disk"] / "inventory.v1.json"),
                "prepared_volumes": [spec.slot_id for spec in specs],
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
