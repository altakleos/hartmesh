"""Explicit fixed-filesystem preparation and shared whole-plan capacity arithmetic."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import uuid
from dataclasses import dataclass
from pathlib import Path

from deerflow.config.storage_spaces_config import StoragePlan
from deerflow.spaces.backings import RESERVE_INODES, FixedVolumeSpec, PreparedVolumeCatalog

MAX_SPACES = 256
INODE_BYTES = 8192
FIXED_VOLUME_MOUNT_OPTIONS = "loop,nodev,nosuid,nodiscard"
HEADROOM_MIN_BYTES = 4 << 30
HEADROOM_FRACTION = 0.10
SPACE_MIN_FREE_BYTES = 16 << 20
SPACE_MIN_FREE_INODES = 256
HOST_WRITE_MARGIN_BYTES = 1 << 20
HOST_WRITE_MIN_INODES = 2
KINDS = ("company", "user", "assistant", "employee")
LAYOUT = {
    "user": (("files", "projects", "conversations"), ("my-files", "project-staging", "project-conversion")),
    "company": (("shared", "projects"), ("shared",)),
    "assistant": (("uploads", "outputs", ".agent-skill-packages"), ()),
    "employee": (("uploads", "outputs", ".agent-skill-packages"), ()),
}


def plan_counts(plan: StoragePlan) -> dict[str, int]:
    return dict(user=plan.users, assistant=plan.users * plan.ai_assistants_per_user, employee=plan.ai_employees, company=1)


def plan_capacities(plan: StoragePlan) -> dict[str, int]:
    return dict(user=plan.user_gib << 30, assistant=plan.ai_assistant_gib << 30, employee=plan.ai_employee_gib << 30, company=plan.company_gib << 30)


@dataclass(frozen=True)
class StorageFit:
    required: dict[str, int]
    present: dict[str, int]
    missing: dict[str, int]
    missing_bytes: int
    disk_bytes: int
    free_bytes: int
    free_inodes: int
    headroom_bytes: int
    fits: bool


def storage_fit(pool: Path, plan: StoragePlan, inventory: PreparedVolumeCatalog | None, *, capacities: dict[str, int] | None = None, growth_bytes: int | None = None) -> StorageFit:
    required = plan_counts(plan)
    if sum(required.values()) > MAX_SPACES:
        raise ValueError(
            f"the keys call for {sum(required.values())} Spaces ({plan.users} users, {plan.users} x {plan.ai_assistants_per_user} AI assistants, {plan.ai_employees} AI employees, the company); at most {MAX_SPACES} can be prepared on one VM"
        )
    present = dict.fromkeys(KINDS, 0)
    if inventory is not None:
        for spec in inventory.volumes.values():
            if spec.kind in present:
                present[spec.kind] += 1
    missing = {kind: max(0, required[kind] - present[kind]) for kind in KINDS}
    capacities = plan_capacities(plan) if capacities is None else capacities
    missing_bytes = sum(missing[kind] * capacities[kind] for kind in KINDS)
    missing_count = sum(missing.values())
    if growth_bytes is not None:
        if type(growth_bytes) is not int or growth_bytes < 0:
            raise ValueError("Growth must be a nonnegative byte count")
        missing_bytes, missing_count = growth_bytes, 0
    capacity = os.statvfs(pool)
    disk_bytes = capacity.f_blocks * capacity.f_frsize
    free_bytes = capacity.f_bavail * capacity.f_frsize
    headroom = max(HEADROOM_MIN_BYTES, int(disk_bytes * HEADROOM_FRACTION))
    fits = free_bytes - missing_bytes >= headroom and capacity.f_favail - 4 * missing_count >= RESERVE_INODES
    return StorageFit(required, present, missing, missing_bytes, disk_bytes, free_bytes, capacity.f_favail, headroom, fits)


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
        raise PermissionError("Provider preparation requires Linux mount authority; ordinary Gateway APIs do not provision mounts")
    if not data_disk.is_absolute() or data_disk == Path("/") or data_disk.is_symlink():
        raise ValueError("Use an explicit private provider data-disk directory")
    if (
        type(count) is not int
        or not 1 <= count <= MAX_SPACES
        or type(size_bytes) is not int
        or size_bytes < 32 * 1024 * 1024
        or size_bytes % 4096
        or type(inodes) is not int
        or inodes < 128
        or type(data_uid) is not int
        or not 0 <= data_uid <= 2**31 - 1
    ):
        raise ValueError("Invalid count, aligned filesystem size, inode bound or native UID")
    if type(reserve_bytes) is not int or reserve_bytes < 1 or type(reserve_inodes) is not int or reserve_inodes < 1:
        raise ValueError("Provider byte/inode reserves must be positive integers")
    tools = {name: shutil.which(name) for name in ("mkfs.ext4", "mount", "umount")}
    if not all(tools.values()):
        raise OSError("The provider needs preinstalled ext4 and mount tools; no automatic installation is performed")
    data_disk.mkdir(mode=0o700, parents=True, exist_ok=True)
    metadata = data_disk.stat()
    if metadata.st_uid != 0 or metadata.st_mode & 0o027:
        raise ValueError("Provider data-disk directory must be root-owned and private")
    capacity = os.statvfs(data_disk)
    if capacity.f_bavail * capacity.f_frsize < count * size_bytes + reserve_bytes or capacity.f_favail < count * 4 + reserve_inodes:
        raise ValueError("Preparing these filesystems would consume the provider platform reserve")
    if (data_disk / "inventory.v1.json").exists():
        raise FileExistsError("Existing inventory is preserved; this command only prepares a new provider pool")
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
                [tools["mount"], "-o", FIXED_VOLUME_MOUNT_OPTIONS, str(image), str(mount)],
                check=True,
                timeout=30,
            )
            owned[-1]["mounted"] = True
            (mount / "data").mkdir(mode=0o770)
            (mount / "control").mkdir(mode=0o700)
            os.chown(mount / "data", data_uid, data_uid)
            os.chown(mount / "control", data_uid, data_uid)
            specs.append(FixedVolumeSpec(slot, mount, image, identity, size_bytes, inodes))
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
                result = subprocess.run([tools["umount"], str(mount)], check=False, timeout=30)
                if result.returncode or os.path.ismount(mount):
                    continue
            if not os.path.ismount(mount):
                if item["image_created"]:
                    image.unlink(missing_ok=True)
                if mount.exists():
                    mount.rmdir()
        raise
