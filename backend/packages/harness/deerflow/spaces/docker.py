"""Exact-container containment on one Linux Docker daemon.

Preparation is an operator-owned callback using existing provider primitives.
It creates, never starts, the environment. This adapter adds no scheduling,
leases, agent identity, image management or host-path API.
"""

import errno
import json
import os
import re
import secrets
import stat
import subprocess
import threading
from contextlib import contextmanager
from pathlib import Path

from deerflow.spaces.filesystem import FilesystemUnavailable

ATTACHMENT_LABEL = "hartmesh.storage.attachment"
HOST_LABEL = "hartmesh.storage.host"


class StorageAdapterUnsupported(FilesystemUnavailable):
    pass


class StorageHostUnqualified(StorageAdapterUnsupported):
    """Static host/provider configuration cannot support native attachments."""


_PROBE_LOCKS = {}
_PROBE_LOCKS_GUARD = threading.Lock()


@contextmanager
def _probe_lock(path):
    # One process owns admission; serializing this private directory prevents
    # another adapter from cleaning an in-flight nonce or removing its folder.
    key = str(Path(path).absolute())
    with _PROBE_LOCKS_GUARD:
        lock = _PROBE_LOCKS.setdefault(key, threading.RLock())
    with lock:
        yield


def _local_endpoint(contexts):
    """Admit the local root-owned Unix endpoint; bind-path proof is separate."""
    try:
        endpoints = {"unix:///run/docker.sock", "unix:///var/run/docker.sock"}
        override = os.environ.get("DOCKER_HOST")
        if override and override not in endpoints:
            raise ValueError("Unqualified Docker endpoint")
        if not isinstance(contexts, list) or len(contexts) != 1:
            raise ValueError("Unqualified Docker context")
        endpoint = contexts[0]["Endpoints"]["docker"]["Host"]
        if endpoint not in endpoints:
            raise ValueError("Unqualified Docker context")
        socket_path = Path(endpoint[7:])
        before = socket_path.lstat()
        if not stat.S_ISSOCK(before.st_mode) or before.st_uid != 0:
            raise ValueError("Untrusted Docker socket")
        for alias in endpoints:
            path = Path(alias[7:])
            try:
                info = path.lstat()
            except FileNotFoundError:
                if alias == override:
                    raise
                continue
            if (info.st_dev, info.st_ino) != (before.st_dev, before.st_ino):
                raise ValueError("Docker socket alias identifies another socket")
        after = socket_path.lstat()
        if (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino):
            raise ValueError("Docker socket identity changed")
    except (OSError, ValueError, KeyError, TypeError) as exc:
        raise StorageHostUnqualified("Native storage requires one root-owned local Docker Unix endpoint") from exc


def _probe_directory(path):
    """Pin a private directory without following its final path component."""
    try:
        path.mkdir(mode=0o700, exist_ok=True)
        return os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    except OSError as exc:
        raise StorageAdapterUnsupported("Storage probe directory is unavailable") from exc


def _clear_probe(path):
    fd = _probe_directory(path)
    try:
        for name in os.listdir(fd):
            info = os.stat(name, dir_fd=fd, follow_symlinks=False)
            if stat.S_ISDIR(info.st_mode):
                raise StorageAdapterUnsupported("Unexpected directory in storage probe state")
            os.unlink(name, dir_fd=fd)
        os.fsync(fd)
    finally:
        os.close(fd)


class DockerStorageAdapter:
    def __init__(self, *, prepare, runner=subprocess.run, host_verifier=None, probe_image=None, pool=None, clean_probe=True):
        self._prepare, self._runner = prepare, runner
        self._probe_image = probe_image
        self._execution_guard = None
        if host_verifier is None and (not probe_image or pool is None):
            raise StorageHostUnqualified("Native storage requires a probe image and pool")
        self._host_verifier = host_verifier or (lambda: _local_endpoint(self._json("context", "inspect")))
        self._host_verifier()
        facts = self._json("info", "--format", "{{json .}}")
        if not isinstance(facts, dict) or facts.get("OSType") != "linux" or "docker desktop" in str(facts.get("OperatingSystem", "")).lower():
            raise StorageHostUnqualified("Native storage requires one qualified Linux Docker host")
        host_id = facts.get("ID")
        if not isinstance(host_id, str) or not host_id or len(host_id) > 128:
            raise StorageHostUnqualified("Docker daemon identity is unavailable")
        self.host_id = host_id
        if pool is not None:
            if not probe_image:
                raise StorageHostUnqualified("Native storage requires a probe image")
            directory = Path(pool) / ".probe"
            with _probe_lock(directory):
                if clean_probe:
                    _clear_probe(directory)
                self.probe(directory, label="pool")

    def probe(self, directory: Path, *, label: str) -> None:
        with _probe_lock(directory):
            self._probe_file(directory, label=label)

    def _probe_file(self, directory: Path, *, label: str) -> None:
        directory = Path(directory)
        nonce, content = secrets.token_hex(16), secrets.token_hex(32)
        fd = _probe_directory(directory)
        created = False
        try:
            file_fd = os.open(nonce, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o644, dir_fd=fd)
            created = True
            try:
                os.fchmod(file_fd, 0o644)
                with os.fdopen(file_fd, "w", encoding="utf-8", closefd=False) as stream:
                    stream.write(content)
                    stream.flush()
                    os.fsync(file_fd)
            finally:
                os.close(file_fd)
            result = self._call(
                "run",
                "--rm",
                "--network",
                "none",
                "--cap-drop=ALL",
                "--security-opt",
                "no-new-privileges",
                "--user",
                "1000:1000",
                "--read-only",
                "--name",
                f"hartmesh-probe-{nonce}",
                "--label",
                f"hartmesh.storage.probe={label}",
                "--mount",
                f"type=bind,src={directory / nonce},dst=/probe,readonly",
                "--entrypoint",
                "cat",
                self._probe_image,
                "/probe",
            )
            if result.returncode != 0 or result.stdout != content:
                raise StorageAdapterUnsupported("The Docker daemon does not resolve the storage path to the same files")
        finally:
            try:
                if created:
                    os.unlink(nonce, dir_fd=fd)
                    os.fsync(fd)
                if label != "pool":
                    try:
                        directory.rmdir()
                    except OSError as exc:
                        if exc.errno not in (errno.ENOTEMPTY, errno.EEXIST):
                            raise
            except OSError as exc:
                raise StorageAdapterUnsupported("Storage probe cleanup could not be completed") from exc
            finally:
                os.close(fd)

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
        if self._execution_guard is not None:
            self._execution_guard()
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
        if self._execution_guard is not None:
            self._execution_guard()
        if self._probe_image is not None:
            for view in plan.views:
                directory = Path(view.source).parent / "control" / "probe"
                self.probe(directory, label=Path(view.source).parent.name.removesuffix(".volume"))
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
    def from_local_backend(cls, backend, *, pool: Path | None = None, clean_probe: bool = True):
        """Add storage admission to the existing provider's container builder."""
        if backend.runtime != "docker" or any(not mount.read_only for mount in backend._config_mounts):
            raise StorageHostUnqualified("Native views require Docker with read-only configured mounts")

        def topology():
            if backend._config_mounts or backend.network_mode != "open":
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

        adapter = cls(prepare=prepare, probe_image=backend._image, pool=pool, clean_probe=clean_probe)
        # Storage can be qualified before this provider supports its configured
        # workspace topology. Preparation and resume retain the same guard.
        adapter._execution_guard = topology
        return adapter
