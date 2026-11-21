"""Seed only the ordinary file roots in the disposable, signed-in Docker fixture."""

import json
import os
import sys
from pathlib import Path
from uuid import UUID

from deerflow.config.paths import get_paths


def seed(account, foreign, groups, per_group):
    if (
        os.environ.get("DEER_FLOW_HOME") != "/data"
        or os.environ.get("DEER_FLOW_CONFIG_PATH") != "/fixture/config.yaml"
    ):
        raise ValueError("Expected an isolated acceptance bundle")
    paths = get_paths()
    for root in [
        paths.user_files_dir(account),
        paths.shared_dir(),
        paths.user_files_dir(foreign),
    ]:
        if not root.resolve().is_relative_to(Path("/data")):
            raise ValueError("Unexpected ordinary file root")
    for label, root in [
        ("personal", paths.ensure_user_files_dir(account)),
        ("shared", paths.ensure_shared_dir()),
    ]:
        for group in range(groups):
            folder = root / f"Collection {group:02d}"
            folder.mkdir()
            for item in range(per_group):
                (folder / f"Report {item:04d}.txt").write_text(
                    f"{label} sample\n", encoding="utf-8"
                )
        (root / "visible.txt").write_text("shared fixture\n", encoding="utf-8")
    (paths.ensure_user_files_dir(foreign) / "other-account.txt").write_text(
        "other\n", encoding="utf-8"
    )
    return {
        "per_location": groups * per_group + 1,
        "groups": groups,
        "files_per_group": per_group,
    }


if __name__ == "__main__":
    account, foreign = map(lambda value: str(UUID(value)), sys.argv[1:3])
    if account == foreign or sys.argv[3] not in ("large", "small"):
        raise ValueError("Expected two distinct accounts and a known fixture size")
    groups, per_group = (25, 400) if sys.argv[3] == "large" else (1, 1)
    print(json.dumps(seed(account, foreign, groups, per_group)))
