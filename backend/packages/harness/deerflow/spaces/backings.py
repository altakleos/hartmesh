"""A provider-prepared, fixed ext4 filesystem is a hard-bound backing.

Ordinary file APIs never mount, format or resize a disk. The operator supplies
fully allocated images on a private data disk, mounted as separate filesystems.
Visible data and private temporary/recovery data share the same fixed capacity.
Native attachments must expose only data, without images/devices/mount control.
There is no ordinary-directory fallback or size-scan quota claim.
"""

from __future__ import annotations

import json
import os
import re
import stat
import struct
import sys
import uuid
from dataclasses import dataclass
from pathlib import Path

from deerflow.spaces.filesystem import ConfinedFilesystem, FilesystemUnavailable


class BackingUnavailable(FilesystemUnavailable):
    """A backing cannot establish its promised enforcement/capacity facts."""


def _absolute(path: Path) -> Path:
    if not isinstance(path, Path) or not path.is_absolute():
        raise ValueError("Provider backing paths must be absolute host paths")
    return path


def _positive(value: int, name: str) -> int:
    if type(value) is not int or not 1 <= value <= 2**63 - 1:
        raise ValueError(f"{name} must be a positive bounded integer")
    return value


@dataclass(frozen=True)
class FixedVolumeSpec:
    slot_id: str
    mount_path: Path
    image_path: Path
    filesystem_uuid: str
    max_bytes: int
    max_inodes: int
    # Inside a Gateway container the same image may have a different pathname.
    # This operator-supplied host name must match the kernel loop device fact.
    host_image_path: Path | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.slot_id, str) or not re.fullmatch(r"[0-9a-f]{32}", self.slot_id):
            raise ValueError("Invalid provider volume identity")
        _absolute(self.mount_path)
        _absolute(self.image_path)
        if self.host_image_path is not None:
            _absolute(self.host_image_path)
        if not isinstance(self.filesystem_uuid, str) or str(uuid.UUID(self.filesystem_uuid)) != self.filesystem_uuid:
            raise ValueError("Provider filesystem UUID must be canonical")
        _positive(self.max_bytes, "Volume bytes")
        _positive(self.max_inodes, "Volume inodes")


@dataclass(frozen=True)
class VerifiedVolume:
    spec: FixedVolumeSpec
    mount_id: int
    device: int
    root_inode: int
    control_inode: int
    filesystem_bytes: int
    filesystem_inodes: int
    available_bytes: int
    available_inodes: int
    image_device: int
    image_inode: int

    @property
    def data_path(self) -> Path:
        return self.spec.mount_path / "data"

    @property
    def control_path(self) -> Path:
        return self.spec.mount_path / "control"

    def filesystem(self) -> ConfinedFilesystem:
        return ConfinedFilesystem(self.data_path, self.control_path, expected_data=(self.device, self.root_inode), expected_control=(self.device, self.control_inode))


def read_ext4_identity(image_path: Path) -> str:
    fd = os.open(image_path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        return _ext4_identity(fd)
    finally:
        os.close(fd)


def _ext4_identity(fd: int) -> str:
    if not stat.S_ISREG(os.fstat(fd).st_mode):
        raise BackingUnavailable("Backing image is not a regular provider file")
    # ext4 superblock at byte1024: magic at56, UUID at104. Use the already
    # verified image descriptor, without reopening its mutable pathname.
    header = os.pread(fd, 1024, 1024)
    if len(header) != 1024 or header[56:58] != b"\x53\xef":
        raise BackingUnavailable("Backing image does not have an ext4 identity")
    return str(uuid.UUID(bytes=header[104:120]))


def _unescape_mount(value: str) -> str:
    return re.sub(r"\\([0-7]{3})", lambda match: chr(int(match[1], 8)), value)


def _mount_record(fd: int) -> dict:
    descriptor = Path(f"/proc/self/fdinfo/{fd}").read_text(encoding="utf-8")
    match = re.search(r"^mnt_id:\s+(\d+)$", descriptor, re.MULTILINE)
    if match is None:
        raise BackingUnavailable("Kernel did not report the backing mount identity")
    mount_id = int(match[1])
    for line in Path("/proc/self/mountinfo").read_text(encoding="utf-8").splitlines():
        before, _, after = line.partition(" - ")
        fields, filesystem = before.split(), after.split()
        if fields and int(fields[0]) == mount_id:
            if len(fields) < 6 or len(filesystem) < 3:
                break
            return {"mount_id": mount_id, "device": fields[2], "root": _unescape_mount(fields[3]), "path": _unescape_mount(fields[4]), "options": set(fields[5].split(",")), "type": filesystem[0]}
    raise BackingUnavailable("Kernel mount facts are unavailable")


def verify_image_extents(fd: int, size: int) -> None:
    """Prove allocated, exclusive coverage; aggregate st_blocks is insufficient."""
    import fcntl

    offset = 0
    observed = 0
    # Linux FIEMAP, UAPI32-byte header and56-byte extent entries. Only plain
    # allocated or reserved-unwritten extents on the supported ext4 host qualify.
    while offset < size:
        buffer = bytearray(32 + 128 * 56)
        struct.pack_into("=QQIIII", buffer, 0, offset, size - offset, 1, 0, 128, 0)
        try:
            fcntl.ioctl(fd, 0xC020660B, buffer, True)
        except OSError as exc:
            raise BackingUnavailable("Image allocation coverage cannot be verified") from exc
        mapped = struct.unpack_from("=QQIIII", buffer)[3]
        if not 0 < mapped <= 128:
            raise BackingUnavailable("Backing image contains unallocated coverage")
        for index in range(mapped):
            logical, physical, length, _r0, _r1, flags, _r2, _r3, _r4 = struct.unpack_from("=QQQQQIIII", buffer, 32 + index * 56)
            if logical > offset or logical + length <= offset or physical == 0 or flags & ~(0x01 | 0x800):
                raise BackingUnavailable("Backing image has holes, shared or unsupported extents")
            offset = logical + length
            observed += 1
            if observed > 262144:
                raise BackingUnavailable("Backing extent map exceeds the qualified inspection bound")
        if flags & 1 and offset < size:
            raise BackingUnavailable("Backing image allocation ends before its declared capacity")


def validate_loop_range(facts: dict) -> None:
    if facts != {"offset": 0, "sizelimit": 0, "autoclear": 1}:
        raise BackingUnavailable("Loop device must cover the complete provider image with automatic retirement")


def mounted_ext4_identity(fd: int) -> str:
    import fcntl

    # EXT4_IOC_GETFSUUID: UAPI fsuuid header8bytes then16UUIDbytes.
    buffer = bytearray(struct.pack("=II", 16, 0) + bytes(16))
    try:
        fcntl.ioctl(fd, 0x8008662C, buffer, True)
    except OSError as exc:
        raise BackingUnavailable("Mounted ext4 filesystem identity cannot be verified") from exc
    if struct.unpack_from("=II", buffer) != (16, 0):
        raise BackingUnavailable("Mounted filesystem identity has an unsupported shape")
    return str(uuid.UUID(bytes=bytes(buffer[8:24])))


class PreparedVolumeCatalog:
    """Startup-pinned operator inventory; no browser-writable registration."""

    def __init__(self, volumes: list[FixedVolumeSpec], *, data_disk: Path, reserve_bytes: int, reserve_inodes: int) -> None:
        self.data_disk = _absolute(data_disk)
        self.reserve_bytes = _positive(reserve_bytes, "Platform byte reserve")
        self.reserve_inodes = _positive(reserve_inodes, "Platform inode reserve")
        self.volumes: dict[str, FixedVolumeSpec] = {}
        uuids, images, mounts = set(), set(), []
        for spec in volumes:
            if not isinstance(spec, FixedVolumeSpec):
                raise ValueError("Invalid provider volume definition")
            image, mount = spec.image_path.resolve(), spec.mount_path.resolve()
            if spec.slot_id in self.volumes or spec.filesystem_uuid in uuids or image in images:
                raise ValueError("Provider volume identities and images must be exclusive")
            if image == mount or image.is_relative_to(mount) or any(mount == other or mount.is_relative_to(other) or other.is_relative_to(mount) for other in mounts):
                raise ValueError("Provider backing roots cannot overlap")
            if not image.is_relative_to(self.data_disk.resolve()) or not mount.is_relative_to(self.data_disk.resolve()):
                raise ValueError("Prepared volume and image must belong to the declared data disk")
            self.volumes[spec.slot_id] = spec
            images.add(image)
            mounts.append(mount)
            uuids.add(spec.filesystem_uuid)
        if any(image == mount or image.is_relative_to(mount) for image in images for mount in mounts):
            raise ValueError("Provider images cannot be inside any visible resource filesystem")

    @classmethod
    def from_manifest(cls, path: Path) -> PreparedVolumeCatalog:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
        try:
            metadata = os.fstat(fd)
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_mode & 0o022 or (os.geteuid() != 0 and metadata.st_uid != os.geteuid()):
                raise ValueError("Volume inventory must be an operator-controlled regular file")
            raw = os.read(fd, 256 * 1024 + 1)
            if len(raw) > 256 * 1024:
                raise ValueError("Provider inventory exceeds its bound")
        finally:
            os.close(fd)
        value = json.loads(raw.decode("utf-8"))
        if (
            not isinstance(value, dict)
            or set(value) != {"schema_version", "data_disk", "reserve_bytes", "reserve_inodes", "volumes"}
            or type(value["schema_version"]) is not int
            or value["schema_version"] != 1
            or not isinstance(value["volumes"], list)
        ):
            raise ValueError("Unsupported provider inventory shape")
        specs = []
        for entry in value["volumes"]:
            required = {"slot_id", "mount_path", "image_path", "filesystem_uuid", "max_bytes", "max_inodes"}
            if not isinstance(entry, dict) or not required <= set(entry) or set(entry) - required - {"host_image_path"}:
                raise ValueError("Invalid provider volume entry")
            entry = dict(entry)
            for key in ("mount_path", "image_path", "host_image_path"):
                if key in entry:
                    entry[key] = Path(entry[key])
            specs.append(FixedVolumeSpec(**entry))
        return cls(specs, data_disk=Path(value["data_disk"]), reserve_bytes=value["reserve_bytes"], reserve_inodes=value["reserve_inodes"])

    def verify_reserve(self) -> None:
        capacity = os.statvfs(self.data_disk)
        if capacity.f_bavail * capacity.f_frsize < self.reserve_bytes or capacity.f_favail < self.reserve_inodes:
            raise BackingUnavailable("The provider data disk cannot preserve its platform reserve")

    def verify(self, slot_id: str, *, previous: VerifiedVolume | None = None) -> VerifiedVolume:
        if sys.platform != "linux":
            raise BackingUnavailable("Fixed filesystem backings require qualified Linux")
        spec = self.volumes.get(slot_id)
        if spec is None:
            raise BackingUnavailable("The provider backing is unavailable")
        self.verify_reserve()
        root_fd = image_fd = data_fd = control_fd = disk_fd = None
        try:
            disk_fd = os.open(self.data_disk, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
            if _mount_record(disk_fd)["type"] != "ext4":
                raise BackingUnavailable("Backing images require the qualified ext4 hosting data disk")
            root_fd = os.open(spec.mount_path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
            mount = _mount_record(root_fd)
            if mount["type"] != "ext4" or mount["root"] != "/" or Path(mount["path"]) != spec.mount_path or not {"rw", "nodev", "nosuid"} <= mount["options"]:
                raise BackingUnavailable("Backing must be an independent provider-mounted ext4 filesystem with nodev/nosuid")
            image_fd = os.open(spec.image_path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
            image = os.fstat(image_fd)
            disk = os.stat(self.data_disk)
            if not stat.S_ISREG(image.st_mode) or image.st_dev != disk.st_dev or image.st_nlink != 1 or image.st_size != spec.max_bytes or image.st_blocks * 512 < image.st_size or image.st_mode & 0o022:
                raise BackingUnavailable("Backing image is not exclusive, fixed and fully physically allocated")
            verify_image_extents(image_fd, image.st_size)
            if _ext4_identity(image_fd) != spec.filesystem_uuid:
                raise BackingUnavailable("Backing filesystem identity changed")
            if mounted_ext4_identity(root_fd) != spec.filesystem_uuid:
                raise BackingUnavailable("Mounted filesystem UUID does not match the provider image")
            root = os.fstat(root_fd)
            device = f"{os.major(root.st_dev)}:{os.minor(root.st_dev)}"
            if mount["device"] != device:
                raise BackingUnavailable("Backing mount device does not match its descriptor")
            loop_file = Path(f"/sys/dev/block/{device}/loop/backing_file")
            kernel_image = "/" + loop_file.read_text(encoding="utf-8").strip().lstrip("/")
            if kernel_image != str(spec.host_image_path or spec.image_path):
                raise BackingUnavailable("Backing mount is not attached to its provider image")
            current_image = os.stat(spec.image_path, follow_symlinks=False)
            if (current_image.st_dev, current_image.st_ino) != (image.st_dev, image.st_ino):
                raise BackingUnavailable("Provider image pathname changed during verification")
            loop = loop_file.parent
            validate_loop_range({name: int((loop / name).read_text(encoding="utf-8").strip()) for name in ("offset", "sizelimit", "autoclear")})
            data_fd = os.open("data", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=root_fd)
            control_fd = os.open("control", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=root_fd)
            data, control = os.fstat(data_fd), os.fstat(control_fd)
            if data.st_dev != root.st_dev or control.st_dev != root.st_dev or control.st_mode & 0o077 or (os.geteuid() != 0 and control.st_uid != os.geteuid()):
                raise BackingUnavailable("Visible and private data must share the provider filesystem; control remains host-private")
            capacity = os.fstatvfs(root_fd)
            total_bytes = capacity.f_blocks * capacity.f_frsize
            if not 0 < total_bytes <= spec.max_bytes or not 0 < capacity.f_files <= spec.max_inodes:
                raise BackingUnavailable("Filesystem exceeds its promised hard byte/inode bound")
            result = VerifiedVolume(spec, mount["mount_id"], root.st_dev, data.st_ino, control.st_ino, total_bytes, capacity.f_files, capacity.f_bavail * capacity.f_frsize, capacity.f_favail, image.st_dev, image.st_ino)
            if previous is not None and (
                result.spec != previous.spec
                or (result.mount_id, result.device, result.root_inode, result.control_inode, result.image_device, result.image_inode)
                != (previous.mount_id, previous.device, previous.root_inode, previous.control_inode, previous.image_device, previous.image_inode)
            ):
                raise BackingUnavailable("Backing mount/root incarnation changed; provider containment and recovery are required")
            # Probe actual link-confinement support before claiming file capability.
            with result.filesystem():
                pass
            return result
        except OSError as exc:
            raise BackingUnavailable("Provider backing facts cannot be verified") from exc
        finally:
            for fd in (control_fd, data_fd, image_fd, root_fd, disk_fd):
                if fd is not None:
                    os.close(fd)
