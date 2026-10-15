"""Exact-container containment on one Linux Docker daemon.

Preparation is an operator-owned callback using existing provider primitives.
It creates, never starts, the environment. This adapter adds no scheduling,
leases, agent identity, image management or host-path API.
"""

import errno
import json
import logging
import os
import re
import secrets
import stat
import subprocess
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from deerflow.spaces.filesystem import FilesystemUnavailable

ATTACHMENT_LABEL = "hartmesh.storage.attachment"
HOST_LABEL = "hartmesh.storage.host"
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ResidentPrimary:
    container_id: str
    name: str
    created_at: str
    attachment_id: str
    host_id: str
    slots: tuple[str, ...]
    mounts: tuple[tuple[str, str, bool], ...]
    resources: tuple = ()


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
        self._cleanup = None
        self._backend = None
        self._configured_mounts = ()
        self._network_of = None
        self._readonly_roots = None
        self._pool = Path(pool) if pool is not None else None
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
        if not isinstance(facts, dict) or facts.get("ID") != self.host_id or facts.get("OSType") != "linux":
            raise StorageAdapterUnsupported("Docker host identity changed; cross-host fencing is unsupported")

    def _inspect(self, container):
        if not isinstance(container, str) or not re.fullmatch(r"[0-9a-f]{64}", container):
            raise StorageAdapterUnsupported("Only a complete immutable Docker container ID is admitted")
        result = self._call("inspect", container)
        if result.returncode:
            # A daemon outage, permissions error or malformed reply is not
            # authoritative absence. Bind even the absence reply to this ID.
            absent = ("Error: No such object: " + container, "error: no such object: " + container, "Error response from daemon: No such container: " + container)
            if result.returncode == 1 and result.stdout.strip() in ("", "[]") and result.stderr.strip() in absent:
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

    async def readonly_roots(self):
        if self._readonly_roots is None:
            if self._backend is not None and getattr(self._backend, "_managed_native_required", False):
                raise StorageHostUnqualified("Resident preparation requires current host read-only roots")
            return ()
        return tuple(await self._readonly_roots())

    def _resident_projection(self, plan):
        mounts = [(view.source, view.destination, not view.writable) for view in plan.views]
        labels = {ATTACHMENT_LABEL: plan.id, HOST_LABEL: plan.host_id}
        for view in plan.views:
            if not isinstance(view.slot_id, str) or not re.fullmatch(r"[0-9a-f]{32}", view.slot_id):
                raise StorageHostUnqualified("Resident views require qualified physical slot identities")
            labels["hartmesh.storage.space." + view.slot_id] = "1"
        for root in plan.readonly_roots:
            root.validate()
            mounts.append((root.source, root.destination, True))
            labels["hartmesh.storage.space." + root.slot_id] = "1"
            labels[root.label] = root.revision
        combined = (*mounts, *self._configured_mounts)
        destinations = [PurePosixPath(destination) for _source, destination, _readonly in combined]
        if any(not path.is_absolute() or ".." in path.parts for path in destinations) or any(a.is_relative_to(b) or b.is_relative_to(a) for index, a in enumerate(destinations) for b in destinations[index + 1 :]):
            raise StorageHostUnqualified("Resident mount destinations overlap or are invalid")
        if self._pool is not None:
            pool = self._pool.resolve()
            for source, _destination, readonly in self._configured_mounts:
                path = Path(source).resolve()
                if readonly is not True or path.is_relative_to(pool) or pool.is_relative_to(path):
                    raise StorageHostUnqualified("Configured mounts expose unadmitted storage roots")
        return mounts, labels

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
        if self._network_of is not None:
            declared, observed = self._network_of(plan), host.get("NetworkMode")
            default_bridge = self._backend is not None and self._backend.network_mode == "open" and declared == "bridge" and observed == "default"
            if observed != declared and not default_bridge:
                raise StorageAdapterUnsupported("Environment differs from the declared resident network")
        if self._backend is not None and self._backend.network_mode != "open":
            name = self._network_of(plan)
            networks = (entry.get("NetworkSettings") or {}).get("Networks")
            if not isinstance(networks, dict) or set(networks) != {name} or not isinstance(networks[name], dict):
                raise StorageAdapterUnsupported("Resident has undeclared actual network membership")
            network = self._paired_resource("network", name)
            expected_network = self._backend._restricted_labels(plan.id, "network")
            options = (network or {}).get("Options") or {}
            state = entry.get("State") or {}
            # Docker create records an unconnected endpoint. Only that exact
            # stopped state may omit its ID; active verification never may.
            unconnected = not active and state.get("Status") == "created" and state.get("Running") is False and networks[name].get("NetworkID") == ""
            if (
                network is None
                or (not unconnected and networks[name].get("NetworkID") != network["Id"])
                or network.get("Driver") != "bridge"
                or network.get("Internal") is not True
                or any((network.get("Labels") or {}).get(key) != value for key, value in expected_network.items())
                or any(options.get("com.docker.network.bridge.gateway_mode_" + family) != "isolated" for family in ("ipv4", "ipv6"))
            ):
                raise StorageAdapterUnsupported("Resident internal network identity or isolation is unqualified")
        labels = (entry.get("Config") or {}).get("Labels") or {}
        for root in plan.readonly_roots:
            root.validate()
            if labels.get(root.label) != root.revision:
                raise StorageAdapterUnsupported("Environment has an obsolete read-only root binding")
        if self._backend is not None:
            _mounts, expected_labels = self._resident_projection(plan)
            expected_labels["deerflow.role"] = "resident"
            if self._backend.network_mode != "open":
                expected_labels = {**self._backend._restricted_labels(plan.id, "resident"), **expected_labels}
            if any(labels.get(key) != value for key, value in expected_labels.items()):
                raise StorageAdapterUnsupported("Environment differs from its admitted resident labels")
            for prefix in ("hartmesh.storage.space.", "hartmesh.storage.readonly."):
                if {key for key in labels if key.startswith(prefix)} != {key for key in expected_labels if key.startswith(prefix)}:
                    raise StorageAdapterUnsupported("Environment carries an undeclared storage identity")
            if any(key in labels for key in ("deerflow.owner_user_id", "deerflow.thread_id", "hartmesh.chat.storage")):
                raise StorageAdapterUnsupported("Resident environment carries a chat identity")
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
        expected.update((root.source, root.destination, False) for root in plan.readonly_roots)
        expected.update((source, destination, False) for source, destination, readonly in self._configured_mounts if readonly is True)
        if any(readonly is not True for _source, _destination, readonly in self._configured_mounts):
            raise StorageAdapterUnsupported("Writable configured workspace roots are unsupported")
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

    def _require_resident_record(self, container, plan):
        if self._backend is None or not getattr(self._backend, "_managed_native_required", False):
            return None
        from deerflow.community.aio_sandbox.native_resource_set import NativeResourceSet
        from deerflow.community.aio_sandbox.resident_preparation import _network_target
        from deerflow.spaces.native_resident_evidence import NativeResidentRoot

        bridge = self._backend._native_creation
        bridge.assert_current()
        record = bridge.residents.for_attachment(plan.id)
        roots = tuple(view.native_root for view in plan.views) + tuple(NativeResidentRoot.from_readonly(root) for root in plan.readonly_roots)
        mode = self._backend.network_mode
        target = _network_target(self._backend, plan.id)
        configured = tuple((source, destination, False) for source, destination, readonly in self._configured_mounts if readonly is True)
        if (
            record is None
            or record.phase != "prepared"
            or (record.container_id, record.daemon_id, record.incarnation, record.roots, record.configured_mounts, record.network_mode, record.network_target, record.policy_digest)
            != (container, plan.host_id, plan.incarnation, roots, configured, mode, target, None if mode == "open" else self._backend._network_policy_digest())
            or len(configured) != len(self._configured_mounts)
        ):
            raise StorageHostUnqualified("Resident activation lacks its exact prepared original graph")
        entries = NativeResourceSet(self._backend, guard=bridge.assert_current).qualify_resident_record(record)
        if any(entry is None for entry in entries.values()):
            raise StorageHostUnqualified("Resident activation lacks a complete original resource graph")
        return record

    @contextmanager
    def _resident_activation(self, container, plan):
        record = self._require_resident_record(container, plan)
        if record is None:
            yield
            return
        factory = getattr(self, "_containment_scope", None)
        if factory is None or factory.provider._backend is not self._backend:
            raise StorageHostUnqualified("Resident activation lost its exact owning lifecycle")
        gate = factory.provider._owner_admission_gate()
        reference = gate.enter("resident:" + plan.id)
        try:
            gate.register_slots(reference, record.slots)
            self._require_resident_record(container, plan)
            yield
        finally:
            gate.end(reference)

    def verify_active(self, container, plan):
        with self._resident_activation(container, plan):
            result = self._validate(container, plan, active=True)
            self._require_resident_record(container, plan)
            return result

    def prepare(self, plan):
        self._same_host()
        if self._execution_guard is not None:
            self._execution_guard()
        for root in plan.readonly_roots:
            root.validate()
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
        with self._resident_activation(container, plan):
            self._validate(container, plan)
            self._require_resident_record(container, plan)
            result = self._call("start", container)
            if result.returncode:
                raise StorageAdapterUnsupported("Environment start outcome is unconfirmed")
            self._validate(container, plan, active=True)
            self._require_resident_record(container, plan)

    def bootstrap_residents(self, *, known_slots, paired=None):
        """Count strict original residents; this grants no adoption or cleanup."""
        from deerflow.community.aio_sandbox.native_resource_set import NativeResourceSet
        from deerflow.spaces.native_teardown_evidence import _timestamp

        if self._backend is None or not isinstance(known_slots, (tuple, list, set, frozenset)) or len(known_slots) > 4096 or any(not isinstance(slot, str) or re.fullmatch(r"[0-9a-f]{32}", slot) is None for slot in known_slots):
            raise StorageHostUnqualified("Resident census requires its exact qualified backend and pool")
        self._same_host()
        reader = NativeResourceSet(self._backend, guard=self._same_host)
        paired = reader.inventory_paired() if paired is None else paired
        identifiers = set()
        selectors = ("label=deerflow.role=resident", "label=" + ATTACHMENT_LABEL, "label=" + HOST_LABEL + "=" + self.host_id, "name=^/hartmesh-space-")
        for selector in selectors:
            remaining = reader.deadline - time.monotonic()
            if remaining <= 0:
                raise StorageHostUnqualified("Resident candidate census exhausted its budget")
            reply = self._backend._run(["docker", "ps", "--all", "--no-trunc", "--filter", selector, "--format", "{{.ID}}"], capture_output=True, text=True, timeout=min(15, remaining))
            candidates = reply.stdout.splitlines()
            if reply.returncode or len(reply.stdout) > 65536 or len(candidates) > 256 or len(set(candidates)) != len(candidates) or any(re.fullmatch(r"[0-9a-f]{64}", identifier) is None for identifier in candidates):
                raise StorageHostUnqualified("Resident primary census is incomplete or unbounded")
            identifiers.update(candidates)
            if len(identifiers) > 256:
                raise StorageHostUnqualified("Resident candidate union is unbounded")
        result, attachments = [], set()
        for identifier in sorted(identifiers):
            entry = reader.inspect("container", identifier)
            labels = (entry or {}).get("Config", {}).get("Labels")
            if not isinstance(labels, dict) or any(not isinstance(key, str) or not isinstance(value, str) for key, value in labels.items()):
                raise StorageHostUnqualified("Resident census has unknown labels")
            attachment_id = labels.get(ATTACHMENT_LABEL)
            slots = tuple(sorted(key.removeprefix("hartmesh.storage.space.") for key in labels if key.startswith("hartmesh.storage.space.")))
            if (
                not isinstance(attachment_id, str)
                or re.fullmatch(r"[0-9a-f]{32}", attachment_id) is None
                or attachment_id in attachments
                or labels.get(HOST_LABEL) != self.host_id
                or labels.get("deerflow.role") != "resident"
                or entry.get("Name") != "/hartmesh-space-" + attachment_id
                or not _timestamp(entry.get("Created"))
                or not 1 <= len(slots) <= 128
                or not set(slots).issubset(known_slots)
                or any(labels["hartmesh.storage.space." + slot] != "1" for slot in slots)
                or any(key in labels for key in ("deerflow.owner_user_id", "deerflow.thread_id", "hartmesh.chat.storage"))
            ):
                raise StorageHostUnqualified("Resident primary has unknown or ambiguous correlation")
            reader._state(entry)
            mounts = entry.get("Mounts")
            if (
                not isinstance(mounts, list)
                or len(mounts) > 128
                or any(
                    not isinstance(mount, dict)
                    or mount.get("Type") != "bind"
                    or mount.get("Propagation") != "rprivate"
                    or type(mount.get("RW")) is not bool
                    or not isinstance(mount.get("Source"), str)
                    or not isinstance(mount.get("Destination"), str)
                    for mount in mounts
                )
            ):
                raise StorageHostUnqualified("Resident primary has unknown mount facts")
            mode, digest = labels.get("deerflow.network_mode", "open"), labels.get("deerflow.network_policy_digest")
            members = tuple(item.fact for item in paired if item.sandbox_id == attachment_id)
            if mode == "open":
                if members or digest is not None:
                    raise StorageHostUnqualified("Open resident has unexplained paired resources")
            elif mode in {"blocked", "allowlist"}:
                if (
                    labels.get("deerflow.sandbox_id") != attachment_id
                    or not isinstance(digest, str)
                    or re.fullmatch(r"[0-9a-f]{64}", digest) is None
                    or len(members) != 3
                    or {member.role for member in members} != {"network-proxy", "network", "egress-network"}
                ):
                    raise StorageHostUnqualified("Resident paired graph is incomplete")
            else:
                raise StorageHostUnqualified("Resident network policy is unknown")
            reader.qualify_resident_graph(entry, attachment_id=attachment_id, network_mode=mode, policy_digest=digest, resources=members)
            actual_mounts = tuple(sorted((mount["Source"], mount["Destination"], mount["RW"]) for mount in mounts))
            if len(set(actual_mounts)) != len(actual_mounts):
                raise StorageHostUnqualified("Resident mount projection is ambiguous")
            result.append(ResidentPrimary(identifier, entry["Name"][1:], entry["Created"], attachment_id, self.host_id, slots, actual_mounts, members))
            attachments.add(attachment_id)
        self._same_host()
        return tuple(result)

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

    def container_state(self, container, attachment_id):
        """Fresh read-only liveness, qualified on both sides of inspection."""
        try:
            self._same_host()
            entry = self._inspect(container)
            self._same_host()
            if entry is None:
                return "gone"
            self._owned(entry, attachment_id)
            state = entry.get("State")
            if not isinstance(state, dict) or type(state.get("Running")) is not bool:
                return "unknown"
            return "running" if state["Running"] else "gone"
        except Exception:
            return "unknown"

    def cleanup(self, attachment_id):
        """Optional trusted paired-resource cleanup, wired by the provider."""
        if self._cleanup is not None:
            self._cleanup(attachment_id)

    def _paired_resource(self, kind, identity):
        result = self._call(*(["network"] if kind == "network" else []), "inspect", identity)
        if result.returncode:
            absent = (
                ("Error: No such object: " + identity, "error: no such object: " + identity, "Error response from daemon: No such container: " + identity)
                if kind == "container"
                else ("Error: No such network: " + identity, "Error response from daemon: network " + identity + " not found")
            )
            if result.returncode == 1 and result.stdout.strip() in ("", "[]") and result.stderr.strip() in absent:
                return None
            raise StorageAdapterUnsupported("Paired resource absence is unconfirmed")
        try:
            entries = json.loads(result.stdout)
            if not isinstance(entries, list) or len(entries) != 1 or not isinstance(entries[0], dict):
                raise ValueError("Invalid paired inspection")
            entry = entries[0]
            if not isinstance(entry.get("Id"), str) or not re.fullmatch(r"[0-9a-f]{64}", entry["Id"]):
                raise ValueError("Invalid paired immutable identity")
            if re.fullmatch(r"[0-9a-f]{64}", identity):
                exact = entry["Id"] == identity
            else:
                exact = str(entry.get("Name", "")).removeprefix("/") == identity
            if not exact:
                raise ValueError("Wrong paired resource")
            return entry
        except (ValueError, TypeError, KeyError) as exc:
            raise StorageAdapterUnsupported("Paired resource identity is unqualified") from exc

    def _cleanup_resident(self, attachment_id):
        """Best effort only after primary fencing; never remove guessed names."""
        try:
            if not isinstance(attachment_id, str) or not re.fullmatch(r"[0-9a-f]{32}", attachment_id):
                raise StorageAdapterUnsupported("Invalid paired attachment identity")
            self._same_host()
            if self._paired_resource("container", "hartmesh-space-" + attachment_id) is not None:
                raise StorageAdapterUnsupported("Resident primary containment is unconfirmed")
            proxy_name, network_name = self._backend._resource_names(attachment_id)
            egress_name = self._backend._egress_network_name(attachment_id)
            proxy = self._paired_resource("container", proxy_name)
            networks = [(name, role, self._paired_resource("network", name)) for name, role in ((network_name, "network"), (egress_name, "egress-network"))]
            for _name, role, entry in networks:
                if entry is None:
                    if proxy is not None:
                        raise StorageAdapterUnsupported("Relay network set is incomplete")
                    continue
                labels = entry.get("Labels") or {}
                if any(labels.get(key) != value for key, value in self._backend._restricted_labels(attachment_id, role).items()) or any(key.startswith("hartmesh.storage.") for key in labels):
                    raise StorageAdapterUnsupported("Paired network attribution differs")
                if entry.get("Driver") != "bridge" or entry.get("Internal") is not (role == "network") or not isinstance(entry.get("Containers"), dict):
                    raise StorageAdapterUnsupported("Paired network topology is unqualified")
                if set(entry["Containers"]) - ({proxy["Id"]} if proxy is not None else set()):
                    raise StorageAdapterUnsupported("Paired network retains an unknown holder")
            if proxy is not None:
                labels = (proxy.get("Config") or {}).get("Labels") or {}
                expected = self._backend._restricted_labels(attachment_id, "network-proxy")
                links = (proxy.get("NetworkSettings") or {}).get("Networks")
                mounts = proxy.get("Mounts")
                ephemeral = isinstance(mounts, list) and all(isinstance(mount, dict) and mount.get("Type") == "tmpfs" and mount.get("Destination") == "/tmp" and mount.get("Source") in (None, "") for mount in mounts)
                if any(labels.get(key) != value for key, value in expected.items()) or any(key.startswith("hartmesh.storage.") for key in labels) or not ephemeral:
                    raise StorageAdapterUnsupported("Paired relay attribution or mounts differ")
                if not isinstance(links, dict) or set(links) != {network_name, egress_name} or any((links[name] or {}).get("NetworkID") != entry["Id"] for name, _role, entry in networks):
                    raise StorageAdapterUnsupported("Paired relay names another network set")
                self._same_host()
                if self._paired_resource("container", proxy["Id"]) != proxy:
                    raise StorageAdapterUnsupported("Paired relay changed before cleanup")
                self._call("rm", "--force", proxy["Id"])
                if self._paired_resource("container", proxy["Id"]) is not None:
                    raise StorageAdapterUnsupported("Paired relay removal is unconfirmed")
            for _name, _role, entry in networks:
                if entry is None:
                    continue
                self._same_host()
                current = self._paired_resource("network", entry["Id"])
                if current is None:
                    continue
                if any(current.get(key) != entry.get(key) for key in ("Name", "Labels", "Driver", "Internal")) or current.get("Containers") != {}:
                    raise StorageAdapterUnsupported("Paired network retains a holder or changed identity")
                removed = self._call("network", "rm", entry["Id"])
                # A lost/nonzero reply is successful only with exact-ID absence.
                if self._paired_resource("network", entry["Id"]) is not None:
                    raise StorageAdapterUnsupported("Paired network removal is unconfirmed" if removed.returncode == 0 else "Paired network removal failed and its identity remains")
            self._same_host()
        except Exception as exc:
            logger.warning("Resident paired cleanup remains incomplete for %s: %s", attachment_id, exc)

    def fence(self, container, attachment_id):
        self._same_host()
        entry = self._inspect(container)
        if entry is None:
            self._same_host()
            return
        self._owned(entry, attachment_id)
        result = self._call("rm", "--force", container)
        # Verify absence even after a nonzero response; a lost reply may have
        # removed it. Never accept a merely stopped, restartable container.
        if self._inspect(container) is not None:
            raise StorageAdapterUnsupported("Container removal has not fenced its storage handles")
        if result.returncode and self.discover(attachment_id):
            raise StorageAdapterUnsupported("Attachment containment remains unconfirmed")
        self._same_host()

    @classmethod
    def from_local_backend(cls, backend, *, pool: Path | None = None, clean_probe: bool = True):
        """Add storage admission to the existing provider's container builder."""
        if backend.runtime != "docker" or any(not mount.read_only for mount in backend._config_mounts):
            raise StorageHostUnqualified("Native views require Docker with read-only configured mounts")

        configured = tuple((str(m.host_path), str(m.container_path), m.read_only) for m in backend._config_mounts)
        mode = backend.network_mode

        def topology():
            current = tuple((str(m.host_path), str(m.container_path), m.read_only) for m in backend._config_mounts)
            if current != configured or backend.network_mode != mode:
                raise StorageAdapterUnsupported("Resident provider configuration changed after qualification")

        def prepare(plan):
            mounts, labels = adapter._resident_projection(plan)
            return backend.prepare_resident(plan.id, "hartmesh-space-" + plan.id, mounts, labels)

        adapter = cls(prepare=prepare, runner=backend._run, probe_image=backend._image, pool=pool, clean_probe=clean_probe)
        # Storage can be qualified before this provider supports its configured
        # workspace topology. Preparation and resume retain the same guard.
        adapter._execution_guard = topology
        adapter._backend = backend
        adapter._configured_mounts = configured

        def network(plan):
            if mode != "open":
                return backend._resource_names(plan.id)[1]
            from deerflow.community.aio_sandbox.local_backend import _effective_docker_network_target

            target = _effective_docker_network_target(os.environ.get("DEER_FLOW_SANDBOX_NETWORK", "").strip())
            return "bridge" if target in ("", "default") else target

        adapter._network_of = network
        if mode != "open":
            adapter._cleanup = adapter._cleanup_resident
        return adapter
