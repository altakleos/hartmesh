"""Populate known disposable mounted areas with ordinary, uniquely named entries."""

import json
import os
import re
import stat
import sys
from pathlib import Path


def seed(areas):
    counts = {}
    for kind, (slot, data_inode, folder) in areas.items():
        assert kind in {"personal", "shared", "agent"}
        assert re.fullmatch(r"[a-f0-9]{32}", slot)
        assert folder == {"personal": "files", "shared": "shared", "agent": "uploads"}[kind]
        mount = Path("/srv/hartmesh/spaces") / (slot + ".volume")
        data = mount / "data"
        assert os.path.ismount(mount) and data.lstat().st_ino == data_inode
        parent = data / folder
        assert stat.S_ISDIR(parent.lstat().st_mode)
        area = parent / "Browser library"
        area.mkdir(mode=0o755)
        expected = 503 if kind == "agent" else 10001
        available = os.statvfs(area).f_favail
        assert available >= expected + 256, f"{kind} needs {expected + 256} free inodes, found {available}"
        for index in range(expected):
            (area / f"entry-{index:05d}.txt").touch(exist_ok=False)
        counts[kind] = len(tuple(area.iterdir()))
        assert counts[kind] == expected
        if kind == "agent":
            with (parent / "browser-upload.txt").open("xb") as old:
                old.write(b"Earlier file")
    return counts


if __name__ == "__main__":
    print(json.dumps(seed(json.loads(sys.argv[1]))))
