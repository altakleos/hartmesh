"""Exact-container containment on one Linux Docker daemon.

Preparation is an operator-owned callback using existing provider primitives.
It creates, never starts, the environment. This adapter adds no scheduling,
leases, agent identity, image management or host-path API.
"""

import json
import os
import re
import secrets
import socket
import stat
import struct
import subprocess
from pathlib import Path

from deerflow.spaces.filesystem import FilesystemUnavailable

ATTACHMENT_LABEL = "hartmesh.storage.attachment"
HOST_LABEL = "hartmesh.storage.host"


class StorageAdapterUnsupported(FilesystemUnavailable):
    pass


def _decode_socket_vfs(data, kernel_inode, sequence):
    if len(data) < 32:
        raise StorageAdapterUnsupported("Socket diagnostic reply is truncated")
    length, kind, flags, returned_sequence, _port = struct.unpack_from("=IHHII", data)
    if length < 32 or length > len(data) or kind != 20 or flags & 2 or returned_sequence != sequence:
        raise StorageAdapterUnsupported("Socket diagnostic reply is invalid")
    family, socket_type, state, _pad, inode, _cookie_a, _cookie_b = struct.unpack_from("=BBBBIII", data, 16)
    if (family, socket_type, state, inode) != (socket.AF_UNIX, socket.SOCK_STREAM, 10, kernel_inode):
        raise StorageAdapterUnsupported("Socket diagnostic reply identifies another listener")
    result = None
    position = 32
    while position < length:
        if position + 4 > length:
            raise StorageAdapterUnsupported("Socket diagnostic attribute is truncated")
        size, attribute = struct.unpack_from("=HH", data, position)
        if size < 4 or position + size > length:
            raise StorageAdapterUnsupported("Socket diagnostic attribute is invalid")
        # Linux UAPI unix_diag.h: UNIX_DIAG_VFS=1, two native u32 fields.
        if attribute == 1:
            if result is not None or size != 12:
                raise StorageAdapterUnsupported("Socket VFS identity is ambiguous")
            result = struct.unpack_from("=II", data, position + 4)
        position += (size + 3) & ~3
    if result is None:
        raise StorageAdapterUnsupported("Kernel cannot prove this listener's VFS identity")
    return result


def _socket_vfs_identity(kernel_inode):
    """Exact bounded UNIX_DIAG_VFS lookup, not a pathname-based socket guess.

    Linux net/unix/diag.c reports d_backing_inode and the kernel dev_t of the
    actual listener. This also handles systemd's inherited listening socket.
    """
    if type(kernel_inode) is not int or not 1 <= kernel_inode <= 2**32 - 1:
        raise StorageAdapterUnsupported("Kernel listener identity exceeds the qualified ABI")
    try:
        with socket.socket(socket.AF_NETLINK, socket.SOCK_RAW, 4) as diagnostic:
            diagnostic.settimeout(5)
            diagnostic.bind((0, 0))
            diagnostic.connect((0, 0))
            sequence = secrets.randbits(32)
            request = struct.pack("=BBHIIIII", socket.AF_UNIX, 0, 0, 0xFFFFFFFF, kernel_inode, 2, 0xFFFFFFFF, 0xFFFFFFFF)
            diagnostic.send(struct.pack("=IHHII", 16 + len(request), 20, 1, sequence, diagnostic.getsockname()[0]) + request)
            data, _ancillary, flags, sender = diagnostic.recvmsg(65536)
            if sender[0] != 0 or flags & socket.MSG_TRUNC:
                raise StorageAdapterUnsupported("Socket diagnostic reply is not a complete kernel response")
            return _decode_socket_vfs(data, kernel_inode, sequence)
    except OSError as exc:
        raise StorageAdapterUnsupported("Kernel socket identity qualification is unavailable") from exc


def _direct_host(contexts):
    """Bind-source strings are meaningful only in the daemon's mount namespace.

    This deliberately rejects DooD, remote engines and private/remapped Gateway
    namespaces. Those require a separately qualified daemon-side identity API.
    """
    try:
        override = os.environ.get("DOCKER_HOST")
        endpoints = {"unix:///run/docker.sock", "unix:///var/run/docker.sock"}
        if override and override not in endpoints:
            raise ValueError("Unqualified Docker endpoint")
        if not isinstance(contexts, list) or len(contexts) != 1 or contexts[0]["Endpoints"]["docker"]["Host"] not in endpoints:
            raise ValueError("Unqualified Docker context")
        socket_path = Path("/run/docker.sock")
        socket_before = socket_path.lstat()
        if not stat.S_ISSOCK(socket_before.st_mode) or socket_before.st_uid != 0:
            raise ValueError("Untrusted Docker socket")
        selected = Path((override or contexts[0]["Endpoints"]["docker"]["Host"])[7:]).lstat()
        if (selected.st_dev, selected.st_ino) != (socket_before.st_dev, socket_before.st_ino):
            raise ValueError("Docker socket alias identifies another socket")
        if socket_before.st_ino > 2**32 - 1:
            raise ValueError("Socket filesystem inode exceeds the qualified kernel ABI")
        fd = os.open("/run/docker.pid", os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o022:
                raise ValueError("Untrusted daemon PID file")
            value = os.read(fd, 32).decode("ascii").strip()
        finally:
            os.close(fd)
        if not re.fullmatch(r"[1-9][0-9]{0,9}", value):
            raise ValueError("Invalid daemon process identity")
        process = Path("/proc") / value
        if (process / "exe").resolve().name != "dockerd":
            raise ValueError("Daemon PID is not dockerd")
        ours, daemon = os.stat("/proc/self/ns/mnt"), os.stat(process / "ns/mnt")
        if (ours.st_dev, ours.st_ino) != (daemon.st_dev, daemon.st_ino):
            raise ValueError("Gateway and daemon have different mount namespaces")
        # The PID file and selected endpoint must identify the same listener.
        # A default PID plus a different/proxied Unix socket is no backing proof.
        listeners = set()
        for line in (process / "net/unix").read_text(encoding="utf-8", errors="surrogateescape").splitlines()[1:]:
            fields = line.split()
            if len(fields) == 8 and fields[3:6] == ["00010000", "0001", "01"] and fields[7] in ("/run/docker.sock", "/var/run/docker.sock"):
                listeners.add(int(fields[6]))
        if len(listeners) != 1:
            raise ValueError("Canonical Docker listener identity is absent or ambiguous")
        listener = next(iter(listeners))
        kernel_device = (os.major(socket_before.st_dev) << 20) | os.minor(socket_before.st_dev)
        if _socket_vfs_identity(listener) != (socket_before.st_ino, kernel_device):
            raise ValueError("Docker pathname was replaced after its verified listener was bound")
        held = False
        for fd in (process / "fd").iterdir():
            try:
                held = os.readlink(fd) == "socket:[" + str(listener) + "]"
            except FileNotFoundError:
                continue  # a closing client FD is not the qualified listener
            if held:
                break
        if not listeners or not held:
            raise ValueError("Selected listener is not held by the verified daemon")
        socket_after = socket_path.lstat()
        if (socket_before.st_dev, socket_before.st_ino) != (socket_after.st_dev, socket_after.st_ino):
            raise ValueError("Docker socket identity changed")
    except (OSError, ValueError, KeyError, TypeError) as exc:
        raise StorageAdapterUnsupported("Native attachment requires a verified direct host daemon namespace; remote/remapped adapters are unsupported") from exc


class DockerStorageAdapter:
    def __init__(self, *, prepare, runner=subprocess.run, host_verifier=None):
        self._prepare, self._runner = prepare, runner
        self._host_verifier = host_verifier or (lambda: _direct_host(self._json("context", "inspect")))
        self._host_verifier()
        facts = self._json("info", "--format", "{{json .}}")
        if not isinstance(facts, dict) or facts.get("OSType") != "linux" or "docker desktop" in str(facts.get("OperatingSystem", "")).lower():
            raise StorageAdapterUnsupported("Native storage requires one qualified Linux Docker host")
        host_id = facts.get("ID")
        if not isinstance(host_id, str) or not host_id or len(host_id) > 128:
            raise StorageAdapterUnsupported("Docker daemon identity is unavailable")
        self.host_id = host_id

    def _call(self, *arguments):
        try:
            return self._runner(["docker", *arguments], capture_output=True, text=True, timeout=30)
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise StorageAdapterUnsupported("Docker operation outcome is unconfirmed") from exc

    def _json(self, *arguments):
        result = self._call(*arguments)
        if result.returncode:
            raise StorageAdapterUnsupported("Docker could not confirm the requested facts")
        try:
            return json.loads(result.stdout)
        except (ValueError, TypeError) as exc:
            raise StorageAdapterUnsupported("Docker returned invalid inspection facts") from exc

    def _same_host(self):
        self._host_verifier()
        facts = self._json("info", "--format", "{{json .}}")
        if facts.get("ID") != self.host_id or facts.get("OSType") != "linux":
            raise StorageAdapterUnsupported("Docker host identity changed; cross-host fencing is unsupported")

    def _inspect(self, container):
        if not isinstance(container, str) or not re.fullmatch(r"[0-9a-f]{64}", container):
            raise StorageAdapterUnsupported("Only a complete immutable Docker container ID is admitted")
        result = self._call("inspect", container)
        if result.returncode:
            # A daemon outage, permissions error or malformed reply is not
            # authoritative absence. Bind even the absence reply to this ID.
            if result.stderr.strip() in ("Error: No such object: " + container, "Error response from daemon: No such container: " + container):
                return None
            raise StorageAdapterUnsupported("Docker cannot establish container containment")
        try:
            entries = json.loads(result.stdout)
            if not isinstance(entries, list) or len(entries) != 1 or entries[0]["Id"] != container:
                raise ValueError("Wrong immutable container identity")
            return entries[0]
        except (KeyError, ValueError, TypeError) as exc:
            raise StorageAdapterUnsupported("Docker inspection identity is invalid") from exc

    def _owned(self, entry, attachment_id):
        labels = (entry.get("Config") or {}).get("Labels") or {}
        if labels.get(ATTACHMENT_LABEL) != attachment_id or labels.get(HOST_LABEL) != self.host_id:
            raise StorageAdapterUnsupported("Container does not belong to this host attachment")

    def _validate(self, container, plan, *, active=False):
        self._same_host()
        if plan.host_id != self.host_id:
            raise StorageAdapterUnsupported("Attachment was prepared for another host")
        entry = self._inspect(container)
        if entry is None:
            raise StorageAdapterUnsupported("Prepared container is absent")
        self._owned(entry, plan.id)
        host = entry.get("HostConfig") or {}
        capabilities = {str(c).upper().removeprefix("CAP_") for c in host.get("CapAdd") or []}
        permitted = {"CHOWN", "FOWNER", "SETUID", "SETGID", "DAC_OVERRIDE"}
        if (
            host.get("Privileged") is not False
            or "ALL" not in {str(c).upper() for c in host.get("CapDrop") or []}
            or capabilities - permitted
            or not any(str(value) in ("no-new-privileges", "no-new-privileges:true") for value in host.get("SecurityOpt") or [])
            or host.get("PidMode") not in ("", "private")
            or host.get("IpcMode") not in ("", "private")
            or host.get("NetworkMode") == "host"
            or host.get("Devices")
            or host.get("DeviceRequests")
            or host.get("DeviceCgroupRules")
            or (host.get("RestartPolicy") or {}).get("Name") not in ("", "no")
        ):
            raise StorageAdapterUnsupported("Prepared environment cannot establish storage confinement")
        mounts = entry.get("Mounts")
        expected = {(v.source, v.destination, v.writable) for v in plan.views}
        if not isinstance(mounts, list) or len(mounts) != len(expected):
            raise StorageAdapterUnsupported("Environment has undeclared storage roots")
        actual = set()
        for mount in mounts:
            if mount.get("Type") != "bind" or mount.get("Propagation") != "rprivate" or type(mount.get("RW")) is not bool:
                raise StorageAdapterUnsupported("Environment mount mode or propagation is unsupported")
            actual.add((mount.get("Source"), mount.get("Destination"), mount["RW"]))
        if actual != expected:
            raise StorageAdapterUnsupported("Environment roots differ from admitted resource views")
        status = "running" if active else "created"
        if (entry.get("State") or {}).get("Status") != status or (entry.get("State") or {}).get("Running") is not active:
            raise StorageAdapterUnsupported("Attachment preparation must leave its environment stopped")
        return entry

    def verify_active(self, container, plan):
        return self._validate(container, plan, active=True)

    def prepare(self, plan):
        self._same_host()
        container = self._prepare(plan)
        if not isinstance(container, str) or not re.fullmatch(r"[0-9a-f]{64}", container):
            raise StorageAdapterUnsupported("Preparation did not return an exact container identity")
        # The service commits this identity before validation. Even an unsafe
        # or incorrectly labelled callback result must remain recoverable.
        return container

    def start(self, container, plan):
        self._validate(container, plan)
        result = self._call("start", container)
        if result.returncode:
            raise StorageAdapterUnsupported("Environment start outcome is unconfirmed")
        entry = self._inspect(container)
        if entry is None or (entry.get("State") or {}).get("Running") is not True:
            raise StorageAdapterUnsupported("Environment activation is unconfirmed")
        self._owned(entry, plan.id)

    def discover(self, attachment_id):
        if not isinstance(attachment_id, str) or not re.fullmatch(r"[0-9a-f]{32}", attachment_id):
            raise ValueError("Invalid attachment identity")
        self._same_host()
        result = self._call("ps", "--all", "--no-trunc", "--quiet", "--filter", "label=" + ATTACHMENT_LABEL + "=" + attachment_id)
        if result.returncode:
            raise StorageAdapterUnsupported("Attachment environment discovery is unconfirmed")
        containers = result.stdout.split()
        for container in containers:
            entry = self._inspect(container)
            if entry is not None:
                self._owned(entry, attachment_id)
        return containers

    def fence(self, container, attachment_id):
        self._same_host()
        entry = self._inspect(container)
        if entry is None:
            return
        self._owned(entry, attachment_id)
        result = self._call("rm", "--force", container)
        # Verify absence even after a nonzero response; a lost reply may have
        # removed it. Never accept a merely stopped, restartable container.
        if self._inspect(container) is not None:
            raise StorageAdapterUnsupported("Container removal has not fenced its storage handles")
        if result.returncode and self.discover(attachment_id):
            raise StorageAdapterUnsupported("Attachment containment remains unconfirmed")

    @classmethod
    def from_local_backend(cls, backend):
        """Add storage admission to the existing provider's container builder."""
        if backend.runtime != "docker" or backend._config_mounts or backend.network_mode != "open":
            raise StorageAdapterUnsupported("Native views require Docker with no unrelated configured mounts")

        def prepare(plan):
            return backend._start_container(
                "hartmesh-space-" + plan.id,
                0,
                [(view.source, view.destination, not view.writable) for view in plan.views],
                publish_port=False,
                labels={ATTACHMENT_LABEL: plan.id, HOST_LABEL: plan.host_id},
                start=False,
            )

        return cls(prepare=prepare)
