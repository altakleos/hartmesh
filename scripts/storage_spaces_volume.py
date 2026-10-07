#!/usr/bin/env python3
"""Explicit provider preparation of new, fixed Storage Spaces filesystems.

Uses existing Linux tools; never installs dependencies or reads app config.
Format targets are exclusively newly created regular image files. This is a
data-disk helper, not a VM-image builder, ordinary file API or live-writer fence.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import uuid
from pathlib import Path

sys.path.insert(
    0, str(Path(__file__).resolve().parents[1] / "backend" / "packages" / "harness")
)

from deerflow.spaces.backings import FixedVolumeSpec, PreparedVolumeCatalog  # noqa: E402


def prepare_volumes(
    data_disk: Path,
    *,
    count: int,
    size_bytes: int,
    inodes: int,
    reserve_bytes: int,
    reserve_inodes: int,
    data_uid: int,
) -> list[FixedVolumeSpec]:
    if sys.platform != "linux" or os.geteuid() != 0:
        raise PermissionError(
            "Provider preparation requires Linux mount authority; ordinary Gateway APIs do not provision mounts"
        )
    if not data_disk.is_absolute() or data_disk == Path("/") or data_disk.is_symlink():
        raise ValueError("Use an explicit private provider data-disk directory")
    if (
        type(count) is not int
        or not 1 <= count <= 100
        or type(size_bytes) is not int
        or size_bytes < 32 * 1024 * 1024
        or size_bytes % 4096
        or type(inodes) is not int
        or inodes < 128
        or type(data_uid) is not int
        or not 0 <= data_uid <= 2**31 - 1
    ):
        raise ValueError(
            "Invalid count, aligned filesystem size, inode bound or native UID"
        )
    if (
        type(reserve_bytes) is not int
        or reserve_bytes < 1
        or type(reserve_inodes) is not int
        or reserve_inodes < 1
    ):
        raise ValueError("Provider byte/inode reserves must be positive integers")
    tools = {name: shutil.which(name) for name in ("mkfs.ext4", "mount", "umount")}
    if not all(tools.values()):
        raise OSError(
            "The provider needs preinstalled ext4 and mount tools; no automatic installation is performed"
        )
    data_disk.mkdir(mode=0o700, parents=True, exist_ok=True)
    metadata = data_disk.stat()
    if metadata.st_uid != 0 or metadata.st_mode & 0o077:
        raise ValueError("Provider data-disk directory must be root-owned and private")
    capacity = os.statvfs(data_disk)
    if (
        capacity.f_bavail * capacity.f_frsize < count * size_bytes + reserve_bytes
        or capacity.f_favail < count * 4 + reserve_inodes
    ):
        raise ValueError(
            "Preparing these filesystems would consume the provider platform reserve"
        )
    if (data_disk / "inventory.v1.json").exists():
        raise FileExistsError(
            "Existing inventory is preserved; this command only prepares a new provider pool"
        )
    specs = []
    owned: list[dict] = []
    published = False
    publication_attempted = False
    temporary: Path | None = None
    temporary_created = False
    manifest = data_disk / "inventory.v1.json"
    try:
        for _index in range(count):
            slot, identity = uuid.uuid4().hex, str(uuid.uuid4())
            image, mount = data_disk / f"{slot}.img", data_disk / f"{slot}.volume"
            mount.mkdir(mode=0o700)
            owned.append(
                {
                    "image": image,
                    "mount": mount,
                    "mounted": False,
                    "image_created": False,
                }
            )
            fd = os.open(
                image,
                os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
                0o600,
            )
            owned[-1]["image_created"] = True
            try:
                os.posix_fallocate(fd, 0, size_bytes)
                subprocess.run(
                    [
                        tools["mkfs.ext4"],
                        "-q",
                        "-F",
                        "-U",
                        identity,
                        "-N",
                        str(inodes),
                        "-m",
                        "0",
                        "-E",
                        "nodiscard,lazy_itable_init=0,lazy_journal_init=0",
                        str(image),
                    ],
                    check=True,
                    timeout=120,
                )
                # Formatting may otherwise discard extents. Require allocation
                # after formatting as well, before making capacity available.
                os.posix_fallocate(fd, 0, size_bytes)
                os.fsync(fd)
            finally:
                os.close(fd)
            subprocess.run(
                [tools["mount"], "-o", "loop,nodev,nosuid", str(image), str(mount)],
                check=True,
                timeout=30,
            )
            owned[-1]["mounted"] = True
            (mount / "data").mkdir(mode=0o770)
            (mount / "control").mkdir(mode=0o700)
            os.chown(mount / "data", data_uid, data_uid)
            specs.append(
                FixedVolumeSpec(slot, mount, image, identity, size_bytes, inodes)
            )
        catalog = PreparedVolumeCatalog(
            specs,
            data_disk=data_disk,
            reserve_bytes=reserve_bytes,
            reserve_inodes=reserve_inodes,
        )
        for spec in specs:
            catalog.verify(spec.slot_id)
        payload = {
            "schema_version": 1,
            "data_disk": str(data_disk),
            "reserve_bytes": reserve_bytes,
            "reserve_inodes": reserve_inodes,
            "volumes": [
                {
                    "slot_id": spec.slot_id,
                    "mount_path": str(spec.mount_path),
                    "image_path": str(spec.image_path),
                    "filesystem_uuid": spec.filesystem_uuid,
                    "max_bytes": spec.max_bytes,
                    "max_inodes": spec.max_inodes,
                }
                for spec in specs
            ],
        }
        temporary = data_disk / ("inventory-" + uuid.uuid4().hex + ".tmp")
        fd = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
            0o600,
        )
        temporary_created = True
        with os.fdopen(fd, "w", encoding="utf-8") as output:
            json.dump(payload, output, ensure_ascii=False, indent=2)
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())
        publication_attempted = True
        os.link(temporary, manifest)
        published = True
        temporary.unlink()
        directory_fd = os.open(data_disk, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        return specs
    except BaseException:
        if published:
            # Another host may have admitted this published pool. A late
            # durability failure never rolls it back or destroys its images.
            raise
        if publication_attempted:
            try:
                manifest.lstat()
            except FileNotFoundError:
                pass  # Definite absence permits unpublished cleanup.
            except OSError:
                raise  # Unknown outcome preserves all prepared backings.
            else:
                # The link may have succeeded before an interrupt was
                # delivered. Any observed publication prevents destruction;
                # an unrelated concurrent publication is preserved too.
                raise
        if temporary is not None and temporary_created:
            temporary.unlink(missing_ok=True)
        # Only unpublished, brand-new paths owned by this invocation. Never
        # unlink an image whose mount containment could not be confirmed.
        for item in reversed(owned):
            image, mount = item["image"], item["mount"]
            if item["mounted"] or os.path.ismount(mount):
                result = subprocess.run(
                    [tools["umount"], str(mount)], check=False, timeout=30
                )
                if result.returncode or os.path.ismount(mount):
                    continue
            if not os.path.ismount(mount):
                if item["image_created"]:
                    image.unlink(missing_ok=True)
                if mount.exists():
                    mount.rmdir()
        raise


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
