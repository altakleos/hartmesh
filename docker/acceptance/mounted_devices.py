"""Observe the hosted qualification volumes across a Gateway restart and remount."""

import argparse
import json
import os
import subprocess
from pathlib import Path


def command(*args):
    return subprocess.check_output(args, text=True, timeout=15).strip()


def snapshot(manifest):
    rows = {}
    volumes = json.loads(manifest.read_text(encoding="utf-8"))["volumes"]
    for entry in volumes:
        root = Path(entry["mount_path"])
        assert root.parent == manifest.parent and root.name == entry["slot_id"] + ".volume"
        target, filesystem, source = command("findmnt", "-rn", "-o", "TARGET,FSTYPE,SOURCE", "-T", str(root)).split()
        assert Path(target) == root and filesystem == "ext4"
        filesystem_uuid = command("blkid", "-s", "UUID", "-o", "value", source)
        assert filesystem_uuid == entry["filesystem_uuid"]
        data = (root / "data").stat()
        control = (root / "control").stat()
        assert root.stat().st_dev == data.st_dev == control.st_dev
        rows[entry["slot_id"]] = {
            "kind": entry["kind"],
            "filesystem_uuid": filesystem_uuid,
            "device": data.st_dev,
            "major_minor": [os.major(data.st_dev), os.minor(data.st_dev)],
            "data_inode": data.st_ino,
            "control_inode": control.st_ino,
        }
    assert len(rows) == 9 and len({row["device"] for row in rows.values()}) == 9
    return rows


def compare(previous, current, *, remounted):
    assert previous.keys() == current.keys()
    changed = []
    for slot, observed in current.items():
        old = previous[slot]
        assert all(observed[key] == old[key] for key in ("kind", "filesystem_uuid", "data_inode", "control_inode"))
        if observed["device"] != old["device"]:
            changed.append(slot)
    if not remounted:
        assert not changed, "a Gateway-only restart changed a mounted volume device"
    return sorted(changed)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("stage", choices=("before-gateway", "after-gateway", "after-remount"))
    parser.add_argument("--previous", type=Path)
    args = parser.parse_args()
    rows = snapshot(Path("/srv/hartmesh/spaces/inventory.v2.json"))
    result = {"stage": args.stage, "host_reboot_measured": False, "volumes": rows}
    if args.previous:
        baseline = json.loads(args.previous.read_text(encoding="utf-8"))
        result["changed_device_slots"] = compare(baseline["volumes"], rows, remounted=args.stage == "after-remount")
    print(json.dumps(result, indent=2))
