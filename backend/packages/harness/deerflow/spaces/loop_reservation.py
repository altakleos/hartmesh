"""Protect a qualified fixed image from loop discard and zeroout deallocation.

Linux loop can implement a WRITE_ZEROES request by punching the backing file,
even on a filesystem mounted with nodiscard. A zero discard limit also disables
that loop operation; ordinary filesystem zeroout can fall back to zero writes.
Only explicit operator preparation changes the queue. Admission only reads it.
"""

from __future__ import annotations

import os
import re
import stat
from pathlib import Path

from deerflow.spaces.backings import BackingUnavailable, FixedVolumeSpec, _ext4_identity, _mount_record, mounted_ext4_identity, validate_loop_range


def _device_path(device: str) -> Path:
    if not re.fullmatch(r"[0-9]+:[0-9]+", device):
        raise BackingUnavailable("Invalid loop reservation device")
    return Path("/sys/dev/block") / device


def verify_loop_reservation(device: str) -> None:
    """Fresh read-only admission; missing/unsupported controls fail closed."""
    try:
        value = (_device_path(device) / "queue/discard_max_bytes").read_text(encoding="ascii")
    except (OSError, UnicodeError) as exc:
        raise BackingUnavailable("Loop reservation protection cannot be verified") from exc
    if value not in ("0", "0\n"):
        raise BackingUnavailable("Loop reservation protection is not enabled; run operator preparation")


def _snapshot(spec: FixedVolumeSpec, root_fd: int, image_fd: int) -> tuple:
    """Pin and reprove raw identity without depending on protected admission."""
    root, image = os.fstat(root_fd), os.fstat(image_fd)
    if not stat.S_ISDIR(root.st_mode) or not stat.S_ISREG(image.st_mode) or image.st_nlink != 1 or image.st_size != spec.max_bytes or image.st_mode & 0o022:
        raise BackingUnavailable("Loop reservation requires an exclusive fixed provider image/root")
    for path, held in ((spec.mount_path, root), (spec.image_path, image)):
        named = path.lstat()
        if (named.st_dev, named.st_ino) != (held.st_dev, held.st_ino):
            raise BackingUnavailable("Loop reservation provider pathname changed")
    mount = _mount_record(root_fd)
    device = f"{os.major(root.st_dev)}:{os.minor(root.st_dev)}"
    if mount["type"] != "ext4" or mount["root"] != "/" or Path(mount["path"]) != spec.mount_path or mount["device"] != device or not {"rw", "nodev", "nosuid"} <= mount["options"] or "ro" in mount["options"]:
        raise BackingUnavailable("Loop reservation requires the exact writable provider mount")
    if _ext4_identity(image_fd) != spec.filesystem_uuid or mounted_ext4_identity(root_fd) != spec.filesystem_uuid:
        raise BackingUnavailable("Loop reservation filesystem identity changed")
    loop = _device_path(device) / "loop"
    backing = "/" + (loop / "backing_file").read_text(encoding="utf-8").strip().lstrip("/")
    if backing != str(spec.host_image_path or spec.image_path):
        raise BackingUnavailable("Loop reservation is attached to another provider image")
    validate_loop_range({name: int((loop / name).read_text(encoding="ascii").strip()) for name in ("offset", "sizelimit", "autoclear")})
    return device, mount["mount_id"], root.st_dev, root.st_ino, image.st_dev, image.st_ino


def protect_loop_reservation(spec: FixedVolumeSpec, root_fd: int, image_fd: int, *, writer=None) -> None:
    """Operator-only effect, retaining the exact mounted root/image throughout.

    A host-namespace writer must inherit both descriptors: the root pins the
    mounted loop's lifetime even if the supervising operator process is killed.
    The caller retains ownership of the supplied descriptors.
    """
    try:
        before = _snapshot(spec, root_fd, image_fd)
        path = _device_path(before[0]) / "queue/discard_max_bytes"
        if writer is None:
            path.write_text("0\n", encoding="ascii")
        else:
            writer(path, pass_fds=(root_fd, image_fd))
        if _snapshot(spec, root_fd, image_fd) != before:
            raise BackingUnavailable("Loop reservation identity changed during configuration")
        verify_loop_reservation(before[0])
    except (OSError, UnicodeError, ValueError) as exc:
        raise BackingUnavailable("Loop reservation protection could not be configured") from exc
