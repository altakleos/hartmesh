"""Bounded actual Docker graph correlation; no admission or destructive commands."""

import json
import logging
import re
import time
from dataclasses import dataclass
from types import SimpleNamespace

from deerflow.spaces.docker import StorageHostUnqualified
from deerflow.spaces.native_create_evidence import _unique_object
from deerflow.spaces.native_teardown_evidence import NativeMountlessTeardownRecord, NativeTeardownRecord, NativeTeardownResource

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PairedResource:
    fact: NativeTeardownResource
    sandbox_id: str
    network_mode: str
    policy_digest: str | None


def _reply_shape(output):
    if not output.strip():
        return "empty"
    if len(output) > 65536:
        return "oversized"
    try:
        value = json.loads(output, object_pairs_hook=_unique_object)
    except (ValueError, RecursionError, StorageHostUnqualified):
        return "invalid-json"
    if isinstance(value, list):
        return "empty-list" if not value else "nonempty-list"
    return "object" if isinstance(value, dict) else "scalar"


def _hex(value):
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def _name(value):
    return isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,254}", value) is not None


class NativeResourceSet:
    """The caller retains its daemon and canonical lifecycle fences throughout IO."""

    def __init__(self, backend, *, guard=None):
        self.backend = backend
        self.guard = guard or (lambda: None)
        self.deadline = time.monotonic() + 30

    def inspect(self, kind, reference):
        if kind not in ("container", "network") or not _name(reference):
            raise StorageHostUnqualified("Native resource inspection has invalid correlation")
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise StorageHostUnqualified("Native resource graph exhausted its overall inspection budget")
        try:
            self.guard()
            result = self.backend._run(["docker", *(["network"] if kind == "network" else []), "inspect", reference], capture_output=True, text=True, timeout=min(15, remaining))
            self.guard()
            if time.monotonic() >= self.deadline:
                raise ValueError("Late graph inspection")
            if result.returncode:
                absent = (
                    ("Error: No such object: " + reference, "error: no such object: " + reference, "Error response from daemon: No such container: " + reference)
                    if kind == "container"
                    else ("Error: No such network: " + reference, "Error response from daemon: network " + reference + " not found")
                )
                if result.returncode == 1 and result.stdout.strip() in ("", "[]") and result.stderr.strip() in absent:
                    return None
                logger.warning(
                    "Native graph reply unqualified: kind=%s reference=%s returncode=%d stdout_shape=%s stderr_template=%s stdout_bytes=%d stderr_bytes=%d",
                    kind,
                    "id" if _hex(reference) else "name",
                    result.returncode,
                    _reply_shape(result.stdout),
                    "known-absence" if result.stderr.strip() in absent else "unknown",
                    len(result.stdout.encode("utf-8")),
                    len(result.stderr.encode("utf-8")),
                )
                raise ValueError("Unqualified resource absence")
            entries = json.loads(result.stdout, object_pairs_hook=_unique_object)
            if not isinstance(entries, list) or len(entries) != 1 or not isinstance(entries[0], dict):
                raise ValueError("Ambiguous graph inspection")
            entry = entries[0]
            identity, name = entry.get("Id"), entry.get("Name")
            if kind == "container":
                if not isinstance(name, str) or not name.startswith("/"):
                    raise ValueError("Malformed container name")
                name = name[1:]
            if not _hex(identity) or not _name(name) or (identity != reference if _hex(reference) else name != reference):
                raise ValueError("Changed graph identity")
            return entry
        except Exception as exc:
            raise StorageHostUnqualified("Native graph inspection is unconfirmed") from exc

    def _member(self, kind, identity, name, created):
        entry, named = self.inspect(kind, identity), self.inspect(kind, name)
        if entry is None and named is None:
            return None
        if entry is None or named is None or entry.get("Id") != named.get("Id") or entry.get("Name") != named.get("Name") or entry.get("Created") != created or named.get("Created") != created:
            raise StorageHostUnqualified("Native graph member identity or original name changed")
        expected_name = "/" + name if kind == "container" else name
        if entry.get("Name") != expected_name:
            raise StorageHostUnqualified("Native graph member no longer has its original name")
        return entry

    def inventory_paired(self):
        """All-state immutable facts, including secondaries without a primary."""
        from deerflow.spaces.native_teardown_evidence import _timestamp

        result = []
        for kind in ("container", "network"):
            selectors = (
                ("label=deerflow.sandbox_id", "label=deerflow.role=network-proxy", "name=^/deer-flow-netproxy-")
                if kind == "container"
                else ("label=deerflow.sandbox_id", "label=deerflow.role=network", "label=deerflow.role=egress-network", "name=^deer-flow-sandbox-net-", "name=^deer-flow-sandbox-egress-")
            )
            identifiers = set()
            for selector in selectors:
                remaining = self.deadline - time.monotonic()
                if remaining <= 0:
                    raise StorageHostUnqualified("Paired resource census exhausted its budget")
                command = ["docker", *(["ps", "--all", "--no-trunc"] if kind == "container" else ["network", "ls", "--no-trunc"]), "--filter", selector, "--format", "{{.ID}}"]
                self.guard()
                reply = self.backend._run(command, capture_output=True, text=True, timeout=min(15, remaining))
                self.guard()
                candidates = reply.stdout.splitlines()
                if reply.returncode or len(reply.stdout) > 65536 or len(candidates) > 512 or len(set(candidates)) != len(candidates) or any(not _hex(identifier) for identifier in candidates):
                    raise StorageHostUnqualified("Paired resource census is incomplete or unbounded")
                identifiers.update(candidates)
                if len(identifiers) > 512:
                    raise StorageHostUnqualified("Paired candidate union is unbounded")
            for identifier in sorted(identifiers):
                entry = self.inspect(kind, identifier)
                if entry is None:
                    raise StorageHostUnqualified("A listed paired resource disappeared during census")
                labels = entry.get("Config", {}).get("Labels") if kind == "container" else entry.get("Labels")
                if not isinstance(labels, dict) or any(not isinstance(key, str) or not isinstance(value, str) for key, value in labels.items()):
                    raise StorageHostUnqualified("Paired census has invalid labels")
                role = labels.get("deerflow.role")
                if kind == "container" and role in {"sandbox", "resident"}:
                    continue
                if role not in ({"network-proxy"} if kind == "container" else {"network", "egress-network"}):
                    raise StorageHostUnqualified("Paired census contains an unexplained resource role")
                sandbox_id, mode, digest = (labels.get(key) for key in ("deerflow.sandbox_id", "deerflow.network_mode", "deerflow.network_policy_digest"))
                if not isinstance(sandbox_id, str) or not 1 <= len(sandbox_id) <= 256 or any(ord(char) < 32 for char in sandbox_id) or mode not in {"blocked", "allowlist"} or not _hex(digest) or not _timestamp(entry.get("Created")):
                    raise StorageHostUnqualified("Paired census has unknown original policy or creation")
                fact = NativeTeardownResource(kind, role, identifier, entry["Name"].removeprefix("/"), entry["Created"])
                result.append(PairedResource(fact, sandbox_id, mode, digest))
        if time.monotonic() >= self.deadline:
            raise StorageHostUnqualified("Paired resource census arrived after its budget")
        return tuple(result)

    def qualify_resident_graph(self, primary, *, attachment_id, network_mode, policy_digest, resources):
        """Shared graph rules without inventing a chat owner or teardown permit."""
        record = SimpleNamespace(container_id=primary["Id"], sandbox_id=attachment_id, network_mode=network_mode, policy_digest=policy_digest, resources=resources)
        entries = {("container", primary["Id"]): self._member("container", primary["Id"], primary["Name"][1:], primary["Created"])}
        if entries[("container", primary["Id"])] != primary:
            raise StorageHostUnqualified("Resident primary changed during graph qualification")
        for resource in resources:
            entries[(resource.kind, resource.identity)] = self._member(resource.kind, resource.identity, resource.name, resource.created_at)
            if entries[(resource.kind, resource.identity)] is None:
                raise StorageHostUnqualified("Resident paired graph is incomplete")
        self._paired_graph(record, entries, primary)

    @staticmethod
    def _labels(entry, record, role, *, container=False):
        labels = entry.get("Config", {}).get("Labels") if container else entry.get("Labels")
        expected = {"deerflow.sandbox_id": record.sandbox_id, "deerflow.role": role, "deerflow.network_mode": record.network_mode}
        if record.policy_digest is not None:
            expected["deerflow.network_policy_digest"] = record.policy_digest
        if not isinstance(labels, dict) or any(not isinstance(key, str) or not isinstance(value, str) for key, value in labels.items()) or any(labels.get(key) != value for key, value in expected.items()):
            raise StorageHostUnqualified("Native graph attribution differs from its original policy")
        if role != "sandbox" and any(key.startswith("hartmesh.storage.space.") or key in ("hartmesh.chat.storage", "deerflow.owner_user_id", "deerflow.thread_id") for key in labels):
            raise StorageHostUnqualified("Paired resource unexpectedly carries storage writer attribution")
        return labels

    @staticmethod
    def _state(entry):
        state = entry.get("State")
        if not isinstance(state, dict) or type(state.get("Running")) is not bool or state.get("Status") not in ("created", "running", "exited") or state["Running"] != (state["Status"] == "running"):
            raise StorageHostUnqualified("Native graph has an unknown container state")
        return state["Running"]

    @staticmethod
    def _endpoints(entry):
        values = entry.get("Containers")
        if not isinstance(values, dict) or len(values) > 256 or any(not _hex(identity) or not isinstance(value, dict) or not _name(value.get("Name")) or not _hex(value.get("EndpointID")) for identity, value in values.items()):
            raise StorageHostUnqualified("Native network has unqualified endpoints")
        return values

    def _links(self, entry, expected, networks):
        running = self._state(entry)
        links = entry.get("NetworkSettings", {}).get("Networks")
        if not isinstance(links, dict) or set(links) != set(expected) or entry.get("HostConfig", {}).get("NetworkMode") not in {*expected, *expected.values()}:
            raise StorageHostUnqualified("Native container has undeclared network membership")
        for name, identity in expected.items():
            network = networks.get(identity)
            link = links[name]
            if network is None or not isinstance(link, dict) or link.get("NetworkID") not in ((identity,) if running else (identity, "")):
                raise StorageHostUnqualified("Native endpoint has no exact original network identity")
            endpoint = self._endpoints(network).get(entry["Id"])
            if endpoint is None:
                if running or link.get("EndpointID") not in (None, ""):
                    raise StorageHostUnqualified("Native container is missing its actual network endpoint")
            elif endpoint.get("Name") != entry["Name"][1:] or not _hex(link.get("EndpointID")) or endpoint.get("EndpointID") != link["EndpointID"]:
                raise StorageHostUnqualified("Native endpoint identity or name changed")

    def _graph(self, record, entries):
        primary = entries[("container", record.container_id)]
        if primary is not None:
            labels = self._labels(primary, record, "sandbox", container=True)
            prefix = "hartmesh.storage.space."
            slots = {key[len(prefix) :] for key in labels if key.startswith(prefix)}
            if (
                labels.get("deerflow.owner_user_id") != record.owner_id
                or labels.get("deerflow.thread_id") != record.thread_id
                or labels.get("hartmesh.chat.storage") != "on"
                or slots != set(record.slots)
                or any(labels[prefix + slot] != "1" for slot in record.slots)
            ):
                raise StorageHostUnqualified("Native primary writer correlation changed")
            self._state(primary)
        self._paired_graph(record, entries, primary)

    def _paired_graph(self, record, entries, primary):
        if record.network_mode == "open":
            return
        resources = {item.role: item for item in record.resources}
        proxy_fact, internal_fact, egress_fact = (resources[role] for role in ("network-proxy", "network", "egress-network"))
        proxy = entries[("container", proxy_fact.identity)]
        networks = {item.identity: entries[("network", item.identity)] for item in (internal_fact, egress_fact)}
        for fact in (internal_fact, egress_fact):
            network = networks[fact.identity]
            if network is None:
                continue
            self._labels(network, record, fact.role)
            internal = fact.role == "network"
            options = network.get("Options")
            if network.get("Driver") != "bridge" or type(network.get("Internal")) is not bool or network["Internal"] is not internal or not isinstance(options, dict):
                raise StorageHostUnqualified("Native network isolation changed")
            required = {"com.docker.network.bridge.gateway_mode_ipv4": "isolated", "com.docker.network.bridge.gateway_mode_ipv6": "isolated"} if internal else {"com.docker.network.bridge.enable_icc": "false"}
            if any(options.get(key) != value for key, value in required.items()):
                raise StorageHostUnqualified("Native network isolation options changed")
            allowed = {record.container_id} if internal and primary is not None else set()
            if proxy is not None:
                allowed.add(proxy_fact.identity)
            if set(self._endpoints(network)) - allowed:
                raise StorageHostUnqualified("Native network contains a foreign or absent member")
        if primary is not None:
            self._links(primary, {internal_fact.name: internal_fact.identity}, networks)
        if proxy is not None:
            self._labels(proxy, record, "network-proxy", container=True)
            mounts = proxy.get("Mounts")
            if not isinstance(mounts, list) or any(not isinstance(mount, dict) or mount.get("Type") != "tmpfs" or mount.get("Source") not in (None, "") for mount in mounts):
                raise StorageHostUnqualified("Native relay has a host storage mount")
            self._links(proxy, {internal_fact.name: internal_fact.identity, egress_fact.name: egress_fact.identity}, networks)

    def capture(self, primary, actual):
        """Fresh complete graph only; never derive members from current configuration."""
        try:
            links = primary["NetworkSettings"]["Networks"]
            if not isinstance(links, dict) or len(links) != 1:
                raise ValueError("Primary has no single restricted network")
            internal_name = next(iter(links))
            internal = self.inspect("network", internal_name)
            if internal is None:
                raise ValueError("Missing actual internal network")
            candidates = set(self._endpoints(internal)) - {actual.container_id}
            if len(candidates) != 1:
                raise ValueError("Missing or ambiguous actual relay")
            proxy = self.inspect("container", next(iter(candidates)))
            proxy_links = proxy["NetworkSettings"]["Networks"]
            if not isinstance(proxy_links, dict) or len(proxy_links) != 2 or internal_name not in proxy_links:
                raise ValueError("Relay has no exact paired membership")
            egress_name = next(iter(set(proxy_links) - {internal_name}))
            egress = self.inspect("network", egress_name)
            resources = tuple(
                NativeTeardownResource(kind, role, entry["Id"], entry["Name"].removeprefix("/"), entry["Created"])
                for kind, role, entry in (("container", "network-proxy", proxy), ("network", "network", internal), ("network", "egress-network", egress))
            )
            digest = primary["Config"]["Labels"].get("deerflow.network_policy_digest")
            record_type = NativeTeardownRecord if actual.slots else NativeMountlessTeardownRecord
            record = record_type("0" * 32, actual.daemon_id, *actual.owner, actual.sandbox_id, actual.slots, None, actual.container_id, actual.container_name, actual.created_at, actual.network_mode, digest, resources)
            if any(entry is None for entry in self.qualify(record).values()):
                raise StorageHostUnqualified("Fresh native capture requires every actual graph member")
            return digest, resources
        except Exception as exc:
            raise StorageHostUnqualified("Native restricted resource set is incomplete or unqualified") from exc

    def qualify(self, record):
        if not isinstance(record, NativeTeardownRecord) or NativeTeardownRecord.parse(record.document(), record.id) != record:
            raise StorageHostUnqualified("Native graph lacks exact original teardown correlation")
        entries = {("container", record.container_id): self._member("container", record.container_id, record.name, record.created_at)}
        for member in record.resources:
            entries[(member.kind, member.identity)] = self._member(member.kind, member.identity, member.name, member.created_at)
        self._graph(record, entries)
        return entries

    def prove_absent(self, record):
        # Inspect every original ID AND name, even when the primary is absent.
        entries = self.qualify(record)
        return all(entry is None for entry in entries.values())
