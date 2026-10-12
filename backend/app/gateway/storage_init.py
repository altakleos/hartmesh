"""Environment-only, privileged fixed-filesystem operations for Compose.

Run in the storage-init container, with the host PID namespace and a shared
pool bind. No Gateway configuration, database, or container lifecycle is read.
"""

from __future__ import annotations

import argparse
import contextlib
import fcntl
import json
import os
import re
import signal
import stat
import subprocess
import sys
import tempfile
import time
import uuid
from dataclasses import asdict, replace
from pathlib import Path

from deerflow.config.storage_spaces_config import StoragePlan
from deerflow.spaces.backings import (
    RESERVE_BYTES,
    RESERVE_INODES,
    BackingUnavailable,
    FixedVolumeSpec,
    PreparedVolumeCatalog,
    _ext4_identity,
    mounted_ext4_identity,
    read_ext4_identity,
    validate_loop_range,
    verify_image_extents,
)
from deerflow.spaces.provision import INODE_BYTES, KINDS, MAX_SPACES, plan_capacities, plan_counts, storage_fit

ABSENT_MEANS_DEFAULT = False
AT_START_LIMIT_SECONDS = 600
LOCK_WAIT_SECONDS = 30
USERS_RANGE = (1, 100)
ASSISTANTS_PER_USER_RANGE = (0, 10)
EMPLOYEES_RANGE = (0, 100)
CAPACITY_RANGE = (1, 1024)
HOST_TOOLS = ("mkfs.ext4", "e2fsck", "resize2fs", "losetup", "mount", "umount", "findmnt", "fallocate")
KEYS = {
    "HARTMESH_USERS": ("users", USERS_RANGE, 10),
    "HARTMESH_USER_STORAGE_GIB": ("user_gib", CAPACITY_RANGE, 1),
    "HARTMESH_AI_ASSISTANTS_PER_USER": ("ai_assistants_per_user", ASSISTANTS_PER_USER_RANGE, 1),
    "HARTMESH_AI_ASSISTANT_STORAGE_GIB": ("ai_assistant_gib", CAPACITY_RANGE, 1),
    "HARTMESH_AI_EMPLOYEES": ("ai_employees", EMPLOYEES_RANGE, 10),
    "HARTMESH_AI_EMPLOYEE_STORAGE_GIB": ("ai_employee_gib", CAPACITY_RANGE, 1),
    "HARTMESH_COMPANY_STORAGE_GIB": ("company_gib", CAPACITY_RANGE, 1),
}
BEGIN = "# hartmesh-storage begin"
END = "# hartmesh-storage end"
SLOT = re.compile(r"[0-9a-f]{32}\Z")
CONTAINER = re.compile(r"(?<![0-9a-f])[0-9a-f]{64}(?![0-9a-f])")


class Refused(ValueError):
    def __init__(self, text: str, **fields):
        super().__init__(text)
        self.fields = fields


def refusal(command: str | None, error: str, **fields) -> dict:
    return {"command": command, "returncode": 1, "error": error, "note": "refused; nothing was changed", **fields}


def parse_plan(environ) -> StoragePlan | None:
    values = {}
    for key, (field, bounds, default) in KEYS.items():
        raw = environ.get(key, "").strip()
        unit = " of GiB" if field.endswith("gib") else ""
        error = f"{key} must be a whole number{unit} from {bounds[0]} to {bounds[1]} (or absent)"
        if key == "HARTMESH_AI_EMPLOYEES":
            error += "; on and off are not values of this key"
        if raw and (not re.fullmatch(r"[0-9]+", raw) or len(raw) > 6 or not bounds[0] <= int(raw) <= bounds[1]):
            raise Refused(error)
        if not raw and not ABSENT_MEANS_DEFAULT and field in ("ai_assistants_per_user", "ai_employees"):
            default = 0
        values[field] = int(raw) if raw else default
    if not environ.get("HARTMESH_USERS", "").strip() and not ABSENT_MEANS_DEFAULT:
        return None
    plan = StoragePlan(**values)
    count = sum(plan_counts(plan).values())
    if count > MAX_SPACES:
        raise Refused(f"the keys call for {count} Spaces ({plan.users} users, {plan.users} x {plan.ai_assistants_per_user} AI assistants, {plan.ai_employees} AI employees, the company); at most {MAX_SPACES} can be prepared on one VM")
    return plan


def no_symlinks(path: Path) -> None:
    for part in (*reversed(path.parents), path):
        try:
            meta = part.lstat()
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(meta.st_mode):
            raise Refused(f"refusing symbolic link in provider path {part}")


def fstab_text(current: str, entries: list[str]) -> tuple[str, int, int]:
    lines = current.splitlines(keepends=True)
    begins = [i for i, line in enumerate(lines) if line.rstrip("\r\n") == BEGIN]
    ends = [i for i, line in enumerate(lines) if line.rstrip("\r\n") == END]
    if len(begins) > 1 or len(ends) > 1:
        raise Refused("/etc/fstab has more than one hartmesh-storage block; nothing was written")
    if bool(begins) != bool(ends) or (begins and begins[0] >= ends[0]):
        raise Refused("/etc/fstab has a hartmesh-storage begin marker without its end marker; nothing was written")
    old = [line.rstrip("\r\n") for line in lines[begins[0] + 1 : ends[0]]] if begins else []
    block = BEGIN + "\n" + "".join(line + "\n" for line in entries) + END + "\n"
    if begins:
        result = "".join(lines[: begins[0]]) + block + "".join(lines[ends[0] + 1 :])
    else:
        result = current + ("\n" if current and not current.endswith("\n") else "") + block
    return result, len(set(entries) - set(old)), len(set(old) - set(entries))


def container_holders(loops: dict[str, str | None], *, proc_root: Path = Path("/proc")) -> list[dict]:
    own_match = CONTAINER.search((proc_root / "self/cgroup").read_text(encoding="utf-8"))
    own = own_match.group() if own_match else None
    holders = []
    for proc in sorted(proc_root.iterdir()):
        if not proc.name.isdigit():
            continue
        try:
            match = CONTAINER.search((proc / "cgroup").read_text(encoding="utf-8"))
            if not match or match.group() == own:
                continue
            sources = set()
            for line in (proc / "mountinfo").read_text(encoding="utf-8").splitlines():
                _, sep, after = line.partition(" - ")
                fields = after.split()
                if not sep or len(fields) < 3:
                    raise Refused(f"cannot read mount facts for process {proc.name}")
                sources.add(fields[1])
            for slot, loop in loops.items():
                if loop and loop in sources:
                    holders.append(dict(pid=int(proc.name), container_id=match.group()[:12], slot_id=slot))
        except FileNotFoundError:
            if proc.exists():
                raise Refused(f"cannot read container mount facts for process {proc.name}") from None
        except OSError as exc:
            raise Refused(f"cannot read container mount facts for process {proc.name}: {exc}") from exc
    return holders


class Host:
    def __init__(self):
        self.lock_fd: int | None = None

    def run(self, *args: str, input: str | None = None, timeout: int = 30, accepted=(0,)) -> str:
        try:
            result = subprocess.run(["nsenter", "-m", "-t", "1", "--", *map(str, args)], input=input, capture_output=True, text=True, timeout=timeout, check=False, pass_fds=() if self.lock_fd is None else (self.lock_fd,))
        except OSError as exc:
            raise Refused(f"cannot enter the host mount namespace (nsenter -m -t 1: {exc}); the service needs privileged: true and pid: host") from exc
        if result.returncode not in accepted:
            raise Refused((result.stderr or result.stdout or f"{args[0]} exited {result.returncode}").strip())
        return result.stdout


class StorageInitializer:
    def __init__(self, data_dir: Path, plan: StoragePlan | None, *, capacities: dict[str, int] | None = None, fstab_path: Path = Path("/etc/fstab")):
        if not data_dir.is_absolute() or data_dir == Path("/") or ".." in data_dir.parts:
            raise Refused("HARTMESH_DATA_DIR must be an absolute private data-disk directory")
        no_symlinks(data_dir)
        self.data_dir, self.pool, self.plan = data_dir, data_dir / "spaces", plan
        self.manifest = self.pool / "inventory.v2.json"
        self.capacities = capacities if capacities is not None else (plan_capacities(plan) if plan else dict.fromkeys(KINDS, 1 << 30))
        if set(self.capacities) != set(KINDS) or any(type(v) is not int or v < 32 << 20 or v % 4096 for v in self.capacities.values()):
            raise Refused("Invalid aligned capacities for provider preparation")
        self.fstab_path, self.host = fstab_path, Host()

    def setup(self, *, repair=False):
        if sys.platform != "linux" or os.geteuid() != 0:
            raise Refused("storage-init requires Linux root mount authority")
        no_symlinks(self.pool)
        existed = self.pool.exists()
        self.pool.mkdir(parents=True, exist_ok=True, mode=0o750)
        fd = os.open(self.pool, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
        try:
            if repair or not existed:
                os.fchown(fd, 0, 1000)
                os.fchmod(fd, 0o750)
        finally:
            os.close(fd)

    @contextlib.contextmanager
    def locked(self, *, repair_pool=False):
        self.setup(repair=repair_pool)
        fd = os.open(self.pool / ".storage-init.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
        try:
            meta = os.fstat(fd)
            if not stat.S_ISREG(meta.st_mode) or meta.st_nlink != 1:
                raise Refused("storage-init lock must be an exclusive regular file")
            os.fchown(fd, 0, 0)
            os.fchmod(fd, 0o600)
            deadline = time.monotonic() + LOCK_WAIT_SECONDS
            while True:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if time.monotonic() >= deadline:
                        raise Refused(f"another storage-init holds {self.pool}/.storage-init.lock and did not finish within 30 s") from None
                    time.sleep(min(1, max(0, deadline - time.monotonic())))
            previous_fd = self.host.lock_fd
            self.host.lock_fd = fd
            try:
                yield
            finally:
                self.host.lock_fd = previous_fd
        finally:
            # Close rather than unlock: a surviving host tool must keep the
            # shared flock until its inherited descriptor is also closed.
            os.close(fd)

    def preflight(self):
        self.host.run("true")
        for tool in HOST_TOOLS:
            try:
                self.host.run("sh", "-c", 'command -v "$1" >/dev/null', "storage-init", tool)
            except Refused:
                package = "e2fsprogs" if tool in ("mkfs.ext4", "e2fsck", "resize2fs") else "util-linux"
                raise Refused(f"{tool} is not installed on the host (package {package})") from None
        fs = self.host.run("findmnt", "-no", "FSTYPE", "--target", str(self.pool)).strip()
        if fs != "ext4":
            raise Refused(f"{self.data_dir} is not ext4 (found {fs}); Spaces need an ext4 data disk")
        try:
            self.host.run("test", "-e", "/dev/loop-control")
        except Refused:
            raise Refused("loop devices are unavailable on this host (/dev/loop-control is missing)") from None

    def load(self, *, optional: bool = False) -> PreparedVolumeCatalog | None:
        if not self.manifest.exists() and not self.manifest.is_symlink():
            if optional:
                return None
            raise Refused(f"no inventory at {self.manifest}; run prepare")
        try:
            # One pinned read, followed by validation before any privileged effect.
            fd = os.open(self.manifest, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
            try:
                metadata = os.fstat(fd)
                if not stat.S_ISREG(metadata.st_mode) or metadata.st_mode & 0o022 or metadata.st_nlink != 1:
                    raise ValueError("inventory must be an exclusive operator-controlled regular file")
                raw = os.read(fd, 256 * 1024 + 1)
                if len(raw) > 256 * 1024:
                    raise ValueError("inventory exceeds its bound")
            finally:
                os.close(fd)
            value = json.loads(raw)
            if value.get("schema_version") != 2 or any("kind" not in e for e in value.get("volumes", [])):
                raise Refused(f"the inventory {self.manifest} has schema_version 1; an entry has no kind")
            if set(value) != {"schema_version", "data_disk", "reserve_bytes", "reserve_inodes", "volumes"} or type(value["schema_version"]) is not int or not isinstance(value["volumes"], list) or len(value["volumes"]) > MAX_SPACES:
                raise ValueError("Unsupported provider inventory shape")
            if value["data_disk"] != str(self.pool):
                raise Refused(f"the inventory names a path outside {self.pool}")
            specs = []
            for entry in value["volumes"]:
                if set(entry) != {"slot_id", "mount_path", "image_path", "filesystem_uuid", "max_bytes", "max_inodes", "kind"} or entry["kind"] not in KINDS:
                    raise ValueError("Invalid provider volume entry")
                slot = entry["slot_id"]
                if not isinstance(slot, str) or not SLOT.fullmatch(slot) or entry["image_path"] != str(self.pool / f"{slot}.img") or entry["mount_path"] != str(self.pool / f"{slot}.volume"):
                    raise Refused(f"the inventory names a path outside {self.pool}")
                for key in ("image_path", "mount_path"):
                    no_symlinks(Path(entry[key]))
                specs.append(FixedVolumeSpec(**{**entry, "mount_path": Path(entry["mount_path"]), "image_path": Path(entry["image_path"])}))
            return PreparedVolumeCatalog(specs, data_disk=self.pool, reserve_bytes=value["reserve_bytes"], reserve_inodes=value["reserve_inodes"])
        except Refused:
            raise
        except (OSError, ValueError, TypeError, AttributeError) as exc:
            raise Refused(f"the inventory {self.manifest} cannot be read: {exc}") from exc

    def publish(self, inventory: PreparedVolumeCatalog):
        value = dict(schema_version=2, data_disk=str(self.pool), reserve_bytes=RESERVE_BYTES, reserve_inodes=RESERVE_INODES, volumes=[])
        for spec in inventory.volumes.values():
            entry = asdict(spec)
            del entry["host_image_path"]
            entry.update(mount_path=str(spec.mount_path), image_path=str(spec.image_path))
            value["volumes"].append(entry)
        fd, name = tempfile.mkstemp(prefix=".inventory-", dir=self.pool)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as stream:
                json.dump(value, stream, sort_keys=True)
                stream.write("\n")
                stream.flush()
                os.fchown(stream.fileno(), 1000, 1000)
                os.fchmod(stream.fileno(), 0o440)
                os.fsync(stream.fileno())
            os.replace(name, self.manifest)
            self.sync_directory()
        finally:
            Path(name).unlink(missing_ok=True)

    def sync_directory(self):
        fd = os.open(self.pool, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def document(self, command, note, returncode=0, **fields):
        return dict(command=command, returncode=returncode, note=note, pool=str(self.pool), problems=self.unlisted(), **fields)

    def unlisted(self):
        inventory = self.load(optional=True)
        listed = set(inventory.volumes) if inventory else set()
        return [f"image {p.name} is not listed in the inventory; remove it with: forget {p.stem}" for p in sorted(self.pool.glob("*.img")) if p.stem not in listed]

    def loop(self, image: Path) -> str | None:
        no_symlinks(image)
        lines = self.host.run("losetup", "-j", str(image)).splitlines()
        if not lines:
            return None
        if len(lines) != 1 or not re.fullmatch(r"/dev/loop[0-9]+", lines[0].partition(":")[0]):
            raise Refused(f"cannot establish the loop device of {image}")
        return lines[0].partition(":")[0]

    def mount(self, spec):
        no_symlinks(spec.image_path)
        no_symlinks(spec.mount_path)
        self.host.run("mount", "-o", "loop,nodev,nosuid", str(spec.image_path), str(spec.mount_path))
        if not os.path.ismount(spec.mount_path):
            raise Refused(f"slot {spec.slot_id} was mounted on the host and is not visible here; the mount of {self.data_dir} is not shared (findmnt -no PROPAGATION --target {self.data_dir})")

    def mount_all(self):
        inventory = self.load()
        mounted, failed, already = [], [], 0
        for spec in inventory.volumes.values():
            try:
                if os.path.ismount(spec.mount_path):
                    already += 1
                else:
                    self.mount(spec)
                    mounted.append(spec.slot_id)
                with self._qualified_parent(spec) as (root, _image):
                    self._set_mode(root, 0, 1000, 0o770)
            except (Refused, OSError, BackingUnavailable, subprocess.TimeoutExpired) as exc:
                failed.append(dict(slot_id=spec.slot_id, error=str(exc)))
        return self.document("mount-all", f"{len(inventory.volumes)} spaces: {len(mounted)} mounted now, {already} already mounted, {len(failed)} failed", 2 if failed else 0, mounted=mounted, already=already, failed=failed)

    @staticmethod
    def fstab_line(spec):
        def escape(path):
            return str(path).replace("\\", "\\134").replace(" ", "\\040").replace("\t", "\\011").replace("\n", "\\012")

        return f"{escape(spec.image_path)} {escape(spec.mount_path)} ext4 loop,nodev,nosuid,nofail,x-systemd.requires-mounts-for={escape(spec.image_path.parent.parent)},x-systemd.before=docker.service 0 0"

    def fstab_sync(self):
        inventory = self.load()
        entries = [self.fstab_line(s) for s in inventory.volumes.values()]
        try:
            current = self.host.run("cat", str(self.fstab_path))
            updated, added, removed = fstab_text(current, entries)
            changed = current != updated
            if changed:
                # Constant shell program; pathname and complete content are separate
                # arguments/stdin. Resolve symlinks rather than replacing the link.
                script = """set -eu
p=$(readlink -f -- "$1")
t=$(mktemp "${p}.hartmesh.XXXXXX")
trap 'rm -f -- "$t"' EXIT
cat > "$t"
chmod --reference="$p" "$t"
chown --reference="$p" "$t"
sync -f "$t"
mv -f -- "$t" "$p"
sync -f "$(dirname -- "$p")"
"""
                self.host.run("sh", "-c", script, "storage-init", str(self.fstab_path), input=updated)
            return self.document("fstab-sync", f"/etc/fstab block {'rewritten' if changed else 'in sync'}: {len(entries)} entries, {added} added, {removed} removed", entries=len(entries), added=added, removed=removed, changed=changed)
        except Refused as exc:
            if "marker" in str(exc) or "block; nothing" in str(exc):
                raise
            raise Refused(f"/etc/fstab cannot be written: {exc}; until it is, the Gateway mounts the Spaces at its own start and nothing mounts them at boot") from exc

    def pool_plan(self, inventory):
        fit = storage_fit(self.pool, self.plan, inventory, capacities=self.capacities)
        fields = dict(
            plan={k: dict(count=fit.required[k], capacity_bytes=self.capacities[k]) for k in KINDS},
            prepared=fit.present,
            missing=fit.missing,
            required_bytes=sum(fit.required[k] * self.capacities[k] for k in KINDS),
            reserved_bytes=sum(s.max_bytes for s in inventory.volumes.values()) if inventory else 0,
            headroom_bytes=fit.headroom_bytes,
            free_bytes=fit.free_bytes,
            free_inodes=fit.free_inodes,
            fits=fit.fits,
            data_disk=dict(fstype="ext4", size_bytes=fit.disk_bytes, free_bytes=fit.free_bytes),
        )
        return fit, fields

    def layout(self, spec):
        with self._qualified_parent(spec) as (root, image):
            self._set_mode(image, 0, 1000, 0o640)
            self._set_mode(root, 0, 1000, 0o770)
            for name, mode in (("data", 0o770), ("control", 0o700)):
                try:
                    os.mkdir(name, mode=mode, dir_fd=root)
                except FileExistsError:
                    pass
                fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_DIRECTORY, dir_fd=root)
                try:
                    self._set_mode(fd, 1000, 1000, mode)
                finally:
                    os.close(fd)
            os.fsync(root)

    @staticmethod
    def _set_mode(fd, uid, gid, mode):
        before = os.fstat(fd)
        if (before.st_uid, before.st_gid, stat.S_IMODE(before.st_mode)) != (uid, gid, mode):
            os.fchown(fd, uid, gid)
            os.fchmod(fd, mode)
            os.fsync(fd)

    @contextlib.contextmanager
    def _qualified_parent(self, spec):
        """Retain the exact image and mounted root through permission repair."""
        image = root = None
        try:
            no_symlinks(spec.image_path)
            no_symlinks(spec.mount_path)
            image = os.open(spec.image_path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC)
            metadata = os.fstat(image)
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1 or metadata.st_size != spec.max_bytes:
                raise Refused(f"slot {spec.slot_id} has an unqualified image")
            verify_image_extents(image, spec.max_bytes)
            identity = _ext4_identity(image)
            if identity != spec.filesystem_uuid:
                raise Refused(f"slot {spec.slot_id} has another image UUID")
            root = os.open(spec.mount_path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
            facts = self._mounted_root_facts(spec, root, image_uuid=identity)
            if not {"rw", "nodev", "nosuid"} <= facts["options"] or "ro" in facts["options"]:
                raise Refused(f"slot {spec.slot_id} is not a qualified writable mount")
            if mounted_ext4_identity(root) != identity:
                raise Refused(f"slot {spec.slot_id} has another mounted UUID")
            self._current_parent_names(spec, root, image)
            yield root, image
            self._current_parent_names(spec, root, image)
        finally:
            try:
                if root is not None:
                    os.close(root)
            finally:
                if image is not None:
                    os.close(image)

    @staticmethod
    def _current_parent_names(spec, root, image):
        for path, fd in ((spec.mount_path, root), (spec.image_path, image)):
            current, held = path.lstat(), os.fstat(fd)
            if (current.st_dev, current.st_ino) != (held.st_dev, held.st_ino):
                raise Refused(f"slot {spec.slot_id} changed during parent qualification")

    def prepare_slot(self, kind):
        slot = uuid.uuid4().hex
        spec = FixedVolumeSpec(slot, self.pool / f"{slot}.volume", self.pool / f"{slot}.img", str(uuid.uuid4()), self.capacities[kind], self.capacities[kind] // INODE_BYTES, kind=kind)
        spec.mount_path.mkdir(mode=0o750)
        os.chown(spec.mount_path, 0, 1000)
        fd = os.open(spec.image_path, os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o640)
        try:
            os.fchown(fd, 0, 1000)
            os.posix_fallocate(fd, 0, spec.max_bytes)
            self.host.run("mkfs.ext4", "-q", "-F", "-U", spec.filesystem_uuid, "-N", str(spec.max_inodes), "-m", "0", "-E", "nodiscard,lazy_itable_init=0,lazy_journal_init=0", str(spec.image_path), timeout=120)
            os.posix_fallocate(fd, 0, spec.max_bytes)
            os.fsync(fd)
        finally:
            os.close(fd)
        self.mount(spec)
        self.layout(spec)
        return spec

    def prepare(self):
        if self.plan is None:
            return self.off("prepare")
        self.preflight()
        inventory = self.load(optional=True)
        if inventory is None:
            inventory = PreparedVolumeCatalog([], data_disk=self.pool, reserve_bytes=RESERVE_BYTES, reserve_inodes=RESERVE_INODES)
            self.publish(inventory)
        mounted = self.mount_all()
        fit, fields = self.pool_plan(inventory)
        if len(inventory.volumes) + sum(fit.missing.values()) > MAX_SPACES:
            raise Refused(f"the existing pool plus missing slots would exceed {MAX_SPACES} Spaces; remove unused surplus slots with forget, or lower the counts, before preparing more")
        count_before, now = len(inventory.volumes), dict.fromkeys(KINDS, 0)
        previous_slots = set(inventory.volumes)
        new_slots = set()
        problems = [f"slot {e['slot_id']} did not mount: {e['error']}" for e in mounted["failed"]]
        verdict, started = "nothing-to-do", time.monotonic()
        if sum(fit.missing.values()) and not fit.fits:
            verdict = "disk"
        elif sum(fit.missing.values()):
            verdict = "complete"
            for kind in KINDS:
                for index in range(fit.missing[kind]):
                    slot_started = time.monotonic()
                    try:
                        spec = self.prepare_slot(kind)
                        inventory.volumes[spec.slot_id] = spec
                        self.publish(inventory)
                        now[kind] += 1
                        new_slots.add(spec.slot_id)
                        print(f"storage-init: prepared {kind} space {index + 1} of {fit.missing[kind]} in {time.monotonic() - slot_started:.1f} s", file=sys.stderr, flush=True)
                    except (OSError, Refused, subprocess.TimeoutExpired) as exc:
                        problems.append(f"{kind} slot could not be prepared: {exc}")
                        verdict = "incomplete"
                        break
                if verdict == "incomplete":
                    break
        # Reflect the durable visible inventory after a late publication error,
        # rather than reporting an unpublished in-memory slot as prepared.
        inventory = self.load()
        new_slots = set(inventory.volumes) - previous_slots
        now = {kind: sum(inventory.volumes[slot].kind == kind for slot in new_slots) for kind in KINDS}
        probe = self.pool / ".probe"
        no_symlinks(probe)
        probe.mkdir(mode=0o700, exist_ok=True)
        fd = os.open(probe, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
        try:
            os.fchown(fd, 1000, 1000)
            os.fchmod(fd, 0o700)
        finally:
            os.close(fd)
        fstab = dict(entries=len(inventory.volumes), changed=False)
        try:
            fstab.update({k: v for k, v in self.fstab_sync().items() if k in ("entries", "changed")})
        except (Refused, OSError) as exc:
            problems.append(str(exc))
        for spec in inventory.volumes.values():
            try:
                inventory.verify(spec.slot_id)
            except (ValueError, OSError, BackingUnavailable) as exc:
                problems.append(f"slot {spec.slot_id} is unverified: {exc}")
        capacity = os.statvfs(self.pool)
        prepared = {k: sum(s.kind == k for s in inventory.volumes.values()) for k in KINDS}
        fields.update(
            prepared=prepared,
            missing={k: max(0, fit.required[k] - prepared[k]) for k in KINDS},
            reserved_bytes=sum(s.max_bytes for s in inventory.volumes.values()),
            free_bytes=capacity.f_bavail * capacity.f_frsize,
            free_inodes=capacity.f_favail,
        )
        fields["data_disk"]["free_bytes"] = fields["free_bytes"]
        seconds = time.monotonic() - started
        if verdict == "disk":
            nouns = dict(user="user", assistant="AI assistant", employee="AI employee", company="company")
            parts = [f"{fit.missing[k]} {nouns[k]} {'space' if fit.missing[k] == 1 else 'spaces'} of {self.capacities[k] / (1 << 30):g} GiB" for k in ("user", "assistant", "employee", "company") if fit.missing[k]]
            prefix = "nothing prepared" if not count_before else f"{count_before} spaces ({sum(os.path.ismount(s.mount_path) for s in inventory.volumes.values())} mounted), nothing new prepared"
            note = (
                f"pool {self.pool}: {prefix}. The keys call for {fit.missing_bytes / (1 << 30):.1f} GiB{' more' if count_before else ''} ({', '.join(parts)}) "
                f"and {fit.headroom_bytes / (1 << 30):.1f} GiB must stay free (the larger of 4 GiB and 10% of the {fit.disk_bytes / (1 << 30):.1f} GiB data disk): "
                f"{(fit.missing_bytes + fit.headroom_bytes) / (1 << 30):.1f} GiB needed, {fit.free_bytes / (1 << 30):.1f} GiB free. "
                + ("Existing spaces keep working. " if count_before else "")
                + f"Grow the data disk, or lower the counts or capacities in {self.data_dir}/.env, then run docker compose up -d --wait."
            )
        else:
            note = (
                f"pool {self.pool}: {len(inventory.volumes)} spaces ({sum(os.path.ismount(s.mount_path) for s in inventory.volumes.values())} mounted), "
                f"{sum(now.values())} prepared in {seconds:.0f} s, fstab {fstab['entries']} entries; {fields['reserved_bytes'] / (1 << 30):.1f} GiB reserved; "
                f"{fields['free_bytes'] / (1 << 30):.1f} GiB free"
            )
        if problems and verdict != "disk":
            verdict = "incomplete"
        result = self.document(
            "prepare",
            note,
            2 if verdict in ("disk", "incomplete") else 0,
            prepared_now=now,
            fstab=fstab,
            seconds=seconds,
            verdict=verdict,
            spaces=[
                dict(slot_id=s.slot_id, kind=s.kind, max_bytes=s.max_bytes, max_inodes=s.max_inodes, mounted=os.path.ismount(s.mount_path), mounted_now=s.slot_id in mounted["mounted"] or s.slot_id in new_slots)
                for s in inventory.volumes.values()
            ],
            **fields,
        )
        result["problems"].extend(problems)
        return result

    def selected(self, slots=(), kind=None, *, optional=False):
        inventory = self.load(optional=optional)
        values = list(inventory.volumes.values()) if inventory else []
        if kind:
            values = [s for s in values if s.kind == kind]
            if not values:
                raise Refused(f"no slot of kind {kind} in the inventory")
        if slots:
            missing = [s for s in slots if not inventory or s not in inventory.volumes]
            if missing:
                raise Refused(f"no slot {missing[0]} in the inventory")
            values = [inventory.volumes[s] for s in slots]
        return inventory, values

    def require_unheld(self, specs, *, quiesce=False):
        loops = {s.slot_id: self.loop(s.image_path) for s in specs}
        holders = container_holders(loops)
        if holders:
            first = holders[0]
            end = (
                "stop the stack and remove sandboxes and workspace containers first"
                if quiesce
                else f"remove every container of the slot (docker ps --filter label=hartmesh.storage.space.{first['slot_id']}) and stop the Gateway (docker compose stop gateway) first"
            )
            raise Refused(f"process {first['pid']} of container {first['container_id']} still has slot {first['slot_id']} mounted; {end}", holders=holders)

    def mounted_image_facts(self, spec, *, full=True):
        """Prove the mounted filesystem belongs to this confined image.

        Permission repair and emptiness checks use this before trusting any
        mounted directory. It also works for unlisted slots without a UUID claim.
        """

        no_symlinks(spec.mount_path)
        no_symlinks(spec.image_path)
        root = os.open(spec.mount_path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
        try:
            return self._mounted_root_facts(spec, root, full=full)
        finally:
            os.close(root)

    def _mounted_root_facts(self, spec, root, *, full=True, image_uuid=None):
        from deerflow.spaces.backings import _mount_record

        try:
            facts = _mount_record(root)
            meta = os.fstat(root)
            device = f"{os.major(meta.st_dev)}:{os.minor(meta.st_dev)}"
            if facts["type"] != "ext4" or facts["root"] != "/" or facts["path"] != str(spec.mount_path) or facts["device"] != device:
                raise Refused(f"slot {spec.slot_id} has a different mount")
            loop = Path(f"/sys/dev/block/{device}/loop")
            kernel_image = "/" + (loop / "backing_file").read_text(encoding="utf-8").strip().lstrip("/")
            if kernel_image != str(spec.image_path):
                raise Refused(f"slot {spec.slot_id} is mounted from another image")
            if full:
                validate_loop_range({name: int((loop / name).read_text(encoding="utf-8").strip()) for name in ("offset", "sizelimit", "autoclear")})
            if full and (image_uuid is not None or spec.image_path.exists()) and mounted_ext4_identity(root) != (image_uuid if image_uuid is not None else read_ext4_identity(spec.image_path)):
                raise Refused(f"slot {spec.slot_id} has another mounted filesystem UUID")
            return facts
        except (OSError, BackingUnavailable, ValueError) as exc:
            raise Refused(f"cannot verify mount identity of slot {spec.slot_id}: {exc}") from exc

    def detach(self, spec):
        if os.path.ismount(spec.mount_path):
            self.mounted_image_facts(spec, full=False)
            self.host.run("umount", str(spec.mount_path))
        if os.path.ismount(spec.mount_path):
            raise Refused(f"slot {spec.slot_id} remains mounted")
        return self.loop(spec.image_path) is None

    def all_slots(self):
        inventory = self.load(optional=True)
        specs = dict(inventory.volumes) if inventory else {}
        for mount in self.pool.glob("*.volume"):
            if SLOT.fullmatch(mount.stem) and mount.stem not in specs:
                no_symlinks(mount)
                image = self.pool / f"{mount.stem}.img"
                no_symlinks(image)
                # Only the image/mount names matter to holder and loop checks.
                specs[mount.stem] = FixedVolumeSpec(mount.stem, mount, image, str(uuid.UUID(int=0)), 1, 1)
        return inventory, list(specs.values())

    def quiesce(self):
        inventory, specs = self.all_slots()
        self.require_unheld(specs, quiesce=True)
        unmounted, busy, already = [], [], 0
        problems = []
        for spec in specs:
            was_mounted = os.path.ismount(spec.mount_path)
            try:
                free = self.detach(spec)
                if was_mounted:
                    unmounted.append(spec.slot_id)
                elif inventory and spec.slot_id in inventory.volumes:
                    already += 1
                if not free:
                    busy.append(spec.slot_id)
            except (OSError, Refused) as exc:
                problems.append(f"slot {spec.slot_id} could not be unmounted: {exc}")
        note = f"{len(unmounted)} spaces unmounted; up -d mounts them again" if specs else "no spaces to unmount"
        if busy:
            note = f"{len(busy)} spaces were unmounted and their loop devices are still attached"
        doc = self.document("quiesce", note, 2 if busy or problems else 0, unmounted=unmounted, already=already, busy=busy, holders=[])
        doc["problems"].extend(problems)
        return doc

    def empty(self, spec):
        if not os.path.ismount(spec.mount_path):
            return None
        self.mounted_image_facts(spec)
        for name in ("data", "control"):
            path = spec.mount_path / name
            no_symlinks(path)
            fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
            try:
                if os.listdir(fd):
                    return False
            finally:
                os.close(fd)
        return True

    def forget(self, slot, *, discard=False):
        if not SLOT.fullmatch(slot):
            raise Refused("give a 32-hex slot id")
        inventory, specs = self.all_slots()
        spec = next((s for s in specs if s.slot_id == slot), None)
        image, mount = self.pool / f"{slot}.img", self.pool / f"{slot}.volume"
        if spec is None:
            if not image.exists():
                raise Refused(f"no slot and no image named {slot} in {self.pool}")
            no_symlinks(image)
            spec = FixedVolumeSpec(slot, mount, image, str(uuid.UUID(int=0)), 1, 1)
        listed = bool(inventory and slot in inventory.volumes)
        if image.exists():
            no_symlinks(image)
            meta = image.lstat()
            if not stat.S_ISREG(meta.st_mode) or meta.st_nlink != 1:
                raise Refused("refusing to delete a nonexclusive provider image")
        self.require_unheld([spec])
        if image.exists() and not os.path.ismount(mount) and not discard:
            if not mount.exists():
                mount.mkdir(mode=0o750)
                os.chown(mount, 0, 1000)
            self.mount(spec)
        # An explicit discard can reclaim even an image interrupted during mkfs.
        # For an unmounted corrupt image, absence of files cannot be established.
        try:
            had_data = self.empty(spec) is not True if os.path.ismount(mount) else image.exists()
        except (OSError, Refused):
            if not discard:
                raise
            had_data = True
        if had_data and not discard:
            raise Refused(f"slot {slot} holds files; have its agent deleted or its seat released, which frees the slot, or pass --discard to destroy them")
        fields = dict(slot_id=slot, kind=spec.kind, listed=listed, had_data=had_data, image_deleted=False, holders=[], busy=[], verdict="forgotten")
        if not self.detach(spec):
            return self.document("forget", f"slot {slot} was unmounted and its loop device is still attached", 2, **{**fields, "busy": [slot]})
        # Remove durable references first. An interruption then leaves an
        # unlisted image, which a repeated forget explicitly reclaims.
        if listed:
            del inventory.volumes[slot]
            self.publish(inventory)
        if inventory:
            self.fstab_sync()
        if image.exists():
            no_symlinks(image)
            meta = image.lstat()
            if not stat.S_ISREG(meta.st_mode) or meta.st_nlink != 1:
                raise Refused("refusing to delete a nonexclusive provider image")
            image.unlink()
            fields["image_deleted"] = True
        if mount.exists():
            no_symlinks(mount)
            mount.rmdir()
        self.sync_directory()
        return self.document("forget", f"slot {slot} forgotten: inventory entry and fstab line removed, image deleted", **fields)

    def resize(self, size, *, slot=None, kind=None, capacity_bytes=None):
        if not re.fullmatch(r"[0-9]+", str(size)) or len(str(size)) > 4 or not 1 <= int(size) <= 1024:
            raise Refused("give the new size as a whole number of GiB from 1 to 1024")
        if bool(slot) == bool(kind):
            raise Refused("give a slot id or --kind user, assistant, employee or company")
        inventory, specs = self.selected([slot] if slot else (), kind)
        target = int(size) << 30 if capacity_bytes is None else capacity_bytes
        if type(target) is not int or target < 32 << 20 or target % 4096:
            raise Refused("Invalid aligned growth capacity")
        unchanged = [s.slot_id for s in specs if s.max_bytes >= target]
        if slot and unchanged:
            raise Refused(f"resize only grows (slot {slot} is {specs[0].max_bytes / (1 << 30):g} GiB)")
        specs = [s for s in specs if s.max_bytes < target]
        growth = 0
        for spec in specs:
            no_symlinks(spec.image_path)
            meta = spec.image_path.lstat()
            if not stat.S_ISREG(meta.st_mode) or meta.st_nlink != 1:
                raise Refused("Backing image must be an exclusive regular file")
            if meta.st_size > target:
                raise Refused(f"the image of slot {spec.slot_id} is already {meta.st_size / (1 << 30):g} GiB; give that size or a larger one")
            if read_ext4_identity(spec.image_path) != spec.filesystem_uuid:
                raise Refused(f"slot {spec.slot_id} has another filesystem UUID")
            growth += max(0, target - meta.st_size)
        if specs:
            fit = storage_fit(self.pool, self.plan, inventory, growth_bytes=growth)
            if not fit.fits:
                raise Refused(
                    f"growing {len(specs)} {kind or specs[0].kind} spaces by {growth / (1 << 30):.1f} GiB would leave {(fit.free_bytes - growth) / (1 << 30):.1f} GiB free on the data disk; "
                    f"{fit.headroom_bytes / (1 << 30):.1f} GiB must stay free"
                )
            self.require_unheld(specs)
        grown, busy, problems = [], [], []
        for spec in specs:
            if not self.detach(spec):
                busy.append(spec.slot_id)
                break
            fd = os.open(spec.image_path, os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC)
            try:
                # No truncate: interrupted growth is either continued or refused.
                os.posix_fallocate(fd, 0, target)
                os.fsync(fd)
                self.host.run("e2fsck", "-f", "-p", str(spec.image_path), timeout=120, accepted=(0, 1))
                self.host.run("resize2fs", str(spec.image_path), timeout=120)
                os.posix_fallocate(fd, 0, target)
                os.fsync(fd)
                larger = replace(spec, max_bytes=target, max_inodes=target // INODE_BYTES)
                inventory.volumes[spec.slot_id] = larger
                self.publish(inventory)
                self.mount(larger)
                grown.append(dict(slot_id=spec.slot_id, from_bytes=spec.max_bytes, max_inodes=larger.max_inodes))
            except (OSError, Refused, subprocess.TimeoutExpired) as exc:
                problems.append(f"the image of slot {spec.slot_id} was grown and resize2fs failed: {exc}; the space is unverified until resize is run again")
                break
            finally:
                os.close(fd)
        doc = self.document(
            "resize",
            f"{len(grown)} spaces grown; restart the Gateway to accept them",
            2 if busy or problems else 0,
            kind=kind,
            to_bytes=target,
            grown=grown,
            unchanged=unchanged,
            holders=[],
            busy=busy,
            verdict="grown" if specs else "already-that-size",
        )
        doc["problems"].extend(problems)
        return doc

    def slot_state(self, spec, *, repair=False):
        row = dict(slot_id=spec.slot_id, kind=spec.kind, state="ok", loop=None, filesystem_uuid=None, image_bytes=None, used_bytes=None, fstab=False, detail=None)
        try:
            row["fstab"] = self.fstab_line(spec) in self.host.run("cat", str(self.fstab_path)).splitlines()
            if not spec.image_path.exists():
                row["state"] = "image-missing"
                return row
            fd = os.open(spec.image_path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
            try:
                image = os.fstat(fd)
                row["image_bytes"] = image.st_size
                if not stat.S_ISREG(image.st_mode) or image.st_nlink != 1 or image.st_size != spec.max_bytes:
                    row["state"] = "wrong-size"
                    return row
                try:
                    verify_image_extents(fd, spec.max_bytes)
                except (OSError, BackingUnavailable) as exc:
                    row.update(state="not-allocated", detail=str(exc))
                    return row
            finally:
                os.close(fd)
            row["filesystem_uuid"] = read_ext4_identity(spec.image_path)
            if row["filesystem_uuid"] != spec.filesystem_uuid:
                row["state"] = "uuid-mismatch"
                return row
            row["loop"] = self.loop(spec.image_path)
            repaired = False
            if not os.path.ismount(spec.mount_path):
                if not repair:
                    row["state"] = "not-mounted"
                    return row
                try:
                    self.mount(spec)
                except Refused:
                    # Never fsck while any process still has the loop filesystem.
                    if self.loop(spec.image_path):
                        row["state"] = "needs-fsck"
                        return row
                    self.host.run("e2fsck", "-p", str(spec.image_path), timeout=120, accepted=(0, 1))
                    self.mount(spec)
                repaired = True
            try:
                facts = self.mounted_image_facts(spec)
            except Refused as exc:
                row.update(state="wrong-mount", detail=str(exc))
                return row
            root = os.open(spec.mount_path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
            try:
                cap = os.fstatvfs(root)
                row["used_bytes"] = (cap.f_blocks - cap.f_bfree) * cap.f_frsize
            finally:
                os.close(root)
            if facts["type"] != "ext4" or facts["root"] != "/" or facts["path"] != str(spec.mount_path) or not {"nodev", "nosuid"} <= facts["options"]:
                row["state"] = "wrong-mount"
                return row
            if "ro" in facts["options"]:
                row["state"] = "read-only"
                return row
            expected = (
                (self.pool, 0, 1000, 0o750),
                (self.manifest, 1000, 1000, 0o440),
                (spec.image_path, 0, 1000, 0o640),
                (spec.mount_path, 0, 1000, 0o770),
                (spec.mount_path / "data", 1000, 1000, 0o770),
                (spec.mount_path / "control", 1000, 1000, 0o700),
            )
            wrong = any((p.lstat().st_uid, p.lstat().st_gid, stat.S_IMODE(p.lstat().st_mode)) != (uid, gid, mode) for p, uid, gid, mode in expected)
            if wrong or not row["fstab"]:
                if not repair:
                    row["state"] = "wrong-modes"
                    return row
                self.layout(spec)
                self.fstab_sync()
                row["fstab"], repaired = True, True
            catalog = PreparedVolumeCatalog([spec], data_disk=self.pool, reserve_bytes=RESERVE_BYTES, reserve_inodes=RESERVE_INODES)
            try:
                catalog.verify(spec.slot_id)
            except (OSError, BackingUnavailable) as exc:
                row.update(state="wrong-mount", detail=str(exc))
                return row
            row["state"] = "repaired" if repaired else "ok"
        except (OSError, BackingUnavailable, Refused, subprocess.TimeoutExpired) as exc:
            row.update(state="needs-fsck", detail=str(exc))
        return row

    def verify(self, slots=(), *, repair=False):
        if repair:
            self.setup(repair=True)
        if repair and self.manifest.exists():
            fd = os.open(self.manifest, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
            try:
                if not stat.S_ISREG(os.fstat(fd).st_mode) or os.fstat(fd).st_nlink != 1:
                    raise Refused("inventory must be an exclusive regular file")
                os.fchown(fd, 1000, 1000)
                os.fchmod(fd, 0o440)
            finally:
                os.close(fd)
        inventory, specs = self.selected(slots)
        rows = [self.slot_state(spec, repair=repair) for spec in specs]
        ok = sum(row["state"] in ("ok", "repaired") for row in rows)
        return self.document("verify", f"{ok} of {len(rows)} spaces ok", 0 if ok == len(rows) else 2, repair=repair, spaces=rows, ok=ok, total=len(rows))

    def off(self, command):
        return dict(command=command, returncode=0, note="Storage Spaces are off (HARTMESH_USERS absent); nothing to do", pool=str(self.pool), problems=[], verdict="nothing-to-do")

    def status(self):
        inventory = self.load(optional=True)
        if self.plan is None:
            return self.off("status")
        fit, fields = self.pool_plan(inventory)
        rows = []
        current = self.host.run("cat", str(self.fstab_path))
        entries = [self.fstab_line(s) for s in inventory.volumes.values()] if inventory else []
        for spec in inventory.volumes.values() if inventory else []:
            mounted = os.path.ismount(spec.mount_path)
            capacity = os.statvfs(spec.mount_path) if mounted else None
            rows.append(
                dict(
                    slot_id=spec.slot_id,
                    kind=spec.kind,
                    image_bytes=spec.image_path.lstat().st_size if spec.image_path.exists() else None,
                    max_inodes=spec.max_inodes,
                    mounted=mounted,
                    loop=self.loop(spec.image_path),
                    fstab=self.fstab_line(spec) in current.splitlines(),
                    used_bytes=(capacity.f_blocks - capacity.f_bfree) * capacity.f_frsize if capacity else None,
                    empty=self.empty(spec),
                )
            )
        fields["data_disk"].update(fstype=self.host.run("findmnt", "-no", "FSTYPE", "--target", str(self.pool)).strip(), propagation=self.host.run("findmnt", "-no", "PROPAGATION", "--target", str(self.pool)).strip())
        fields["fits"] = fit.fits if sum(fit.missing.values()) else True
        note = f"pool {self.pool}: {len(rows)} spaces; {fields['reserved_bytes'] / (1 << 30):.1f} GiB reserved; {fields['free_bytes'] / (1 << 30):.1f} GiB free; {'fits' if fields['fits'] else 'does not fit'}"
        if inventory is None:
            note = (
                f"no inventory, nothing prepared; the keys call for {fields['required_bytes'] / (1 << 30):.1f} GiB and {fit.headroom_bytes / (1 << 30):.1f} GiB kept free, "
                f"{fit.free_bytes / (1 << 30):.1f} GiB free: {'fits' if fit.fits else 'does not fit'}"
            )
        return self.document("status", note, inventory=inventory is not None, spaces=rows, fstab_in_sync=fstab_text(current, entries)[0] == current, **fields)


class Parser(argparse.ArgumentParser):
    def error(self, message):
        raise Refused(message)


def parser():
    p = Parser(description=__doc__)
    verbs = p.add_subparsers(dest="command", required=True)
    prepare = verbs.add_parser("prepare")
    prepare.add_argument("--at-start", action="store_true")
    for name in ("mount-all", "fstab-sync", "quiesce", "status"):
        verbs.add_parser(name)
    verify = verbs.add_parser("verify")
    verify.add_argument("slots", nargs="*")
    verify.add_argument("--repair", action="store_true")
    forget = verbs.add_parser("forget")
    forget.add_argument("slot")
    forget.add_argument("--discard", action="store_true")
    resize = verbs.add_parser("resize")
    resize.add_argument("values", nargs="+")
    resize.add_argument("--kind", choices=KINDS)
    return p


def end_owned_group(child) -> bool:
    # The group, not completion of its Python leader, owns host-tool execution.
    try:
        os.killpg(child.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    try:
        child.communicate(timeout=5)
    except subprocess.TimeoutExpired:
        return False
    deadline = time.monotonic() + 5
    while True:
        live = False
        for proc in Path("/proc").iterdir():
            if not proc.name.isdigit():
                continue
            try:
                fields = (proc / "stat").read_text(encoding="utf-8").rsplit(")", 1)[1].split()
                if int(fields[2]) == child.pid and fields[0] not in ("Z", "X"):
                    live = True
                    break
            except FileNotFoundError:
                continue
            except (OSError, ValueError, IndexError):
                return False
        if not live:
            return True
        if time.monotonic() >= deadline:
            return False
        time.sleep(0.02)


def at_start():
    child = None
    previous = signal.getsignal(signal.SIGTERM)

    def interrupted(signum, frame):
        raise InterruptedError("prepare supervisor was interrupted")

    signal.signal(signal.SIGTERM, interrupted)
    try:
        child = subprocess.Popen([sys.executable, "-m", "app.gateway.storage_init", "prepare"], stdout=subprocess.PIPE, text=True, start_new_session=True)
        try:
            stdout, _ = child.communicate(timeout=AT_START_LIMIT_SECONDS)
            doc = json.loads(stdout)
            if not isinstance(doc, dict) or doc.get("command") != "prepare" or doc.get("returncode") not in (0, 1, 2):
                raise ValueError("prepare child returned an invalid document")
            if not end_owned_group(child):
                doc.update(returncode=2, verdict="incomplete", note="prepare ended; child containment could not be confirmed")
                doc.setdefault("problems", []).append(doc["note"])
            return doc
        except (subprocess.TimeoutExpired, InterruptedError, KeyboardInterrupt) as exc:
            confirmed = end_owned_group(child)
            cause = "prepare did not finish within 600 s" if isinstance(exc, subprocess.TimeoutExpired) else "prepare supervisor was interrupted"
            note = cause + ("; its children were ended" if confirmed else "; child containment could not be confirmed")
            data = os.environ.get("HARTMESH_DATA_DIR", "")
            doc = dict(
                command="prepare",
                returncode=2,
                note=note,
                verdict="incomplete",
                pool=str(Path(data) / "spaces"),
                problems=[note],
                plan=None,
                prepared=None,
                missing=None,
                prepared_now=None,
                required_bytes=None,
                reserved_bytes=None,
                headroom_bytes=None,
                free_bytes=None,
                free_inodes=None,
                fits=None,
                data_disk=None,
                spaces=[],
                fstab=dict(entries=None, changed=None),
                seconds=AT_START_LIMIT_SECONDS,
            )
            try:
                tool = StorageInitializer(Path(data), parse_plan(os.environ))
                inventory = tool.load(optional=True)
                if inventory:
                    doc["spaces"] = [dict(slot_id=s.slot_id, kind=s.kind, max_bytes=s.max_bytes, max_inodes=s.max_inodes, mounted=os.path.ismount(s.mount_path), mounted_now=None) for s in inventory.volumes.values()]
                    doc["prepared"] = {kind: sum(s.kind == kind for s in inventory.volumes.values()) for kind in KINDS}
                    doc["reserved_bytes"] = sum(s.max_bytes for s in inventory.volumes.values())
                if tool.plan:
                    _, fields = tool.pool_plan(inventory)
                    # Current pool facts are available; which slots this run made,
                    # its initial fit and its fstab publication remain unknown.
                    fields["fits"] = None
                    doc.update(fields)
            except Exception as state_error:
                doc["problems"].append(f"persisted preparation state could not be read: {state_error}")
            return doc
    finally:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        try:
            if child is not None:
                end_owned_group(child)
        finally:
            signal.signal(signal.SIGTERM, previous)


def main(argv=None) -> int:
    args, command, is_start = None, None, False
    try:
        args = parser().parse_args(argv)
        command = args.command
        is_start = bool(getattr(args, "at_start", False))
        if is_start:
            doc = at_start()
        else:
            raw = os.environ.get("HARTMESH_DATA_DIR", "").strip()
            if not raw:
                raise Refused("HARTMESH_DATA_DIR is not set")
            tool = StorageInitializer(Path(raw), None)
            with tool.locked(repair_pool=command == "prepare" or bool(getattr(args, "repair", False))):
                # The pool exists even when the operator's keys are invalid.
                tool.plan = parse_plan(os.environ)
                tool.capacities = plan_capacities(tool.plan) if tool.plan else tool.capacities
                if tool.plan is None and command not in ("status", "quiesce"):
                    doc = tool.off(command)
                else:
                    if command != "prepare":
                        tool.preflight()
                    if command == "verify":
                        doc = tool.verify(args.slots, repair=args.repair)
                    elif command == "forget":
                        doc = tool.forget(args.slot, discard=args.discard)
                    elif command == "resize":
                        if (args.kind and len(args.values) != 1) or (not args.kind and len(args.values) != 2):
                            raise Refused("give a slot id or --kind user, assistant, employee or company")
                        doc = tool.resize(args.values[-1], kind=args.kind, slot=None if args.kind else args.values[0])
                    else:
                        doc = getattr(tool, command.replace("-", "_"))()
    except Refused as exc:
        doc = refusal(command, str(exc), **exc.fields)
    except Exception as exc:
        doc = refusal(command, f"{type(exc).__name__}: {exc}", note="failed; run the verb again")
    print(json.dumps(doc, sort_keys=True), flush=True)
    return 0 if is_start else doc["returncode"]


if __name__ == "__main__":
    raise SystemExit(main())
