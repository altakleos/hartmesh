"""Actual immutable graph proof with an offline Docker command double."""

import json
import subprocess
import time
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace

import pytest
from test_native_slot_inventory import CID, SLOT, entry
from test_native_teardown_evidence import STAMP

from deerflow.community.aio_sandbox.local_backend import _native_inspection
from deerflow.community.aio_sandbox.native_resource_set import NativeResourceSet, PairedResource
from deerflow.community.aio_sandbox.native_teardown_execution import remove_leftovers
from deerflow.spaces.docker import StorageHostUnqualified
from deerflow.spaces.native_teardown_evidence import NativeTeardownRecord, NativeTeardownResource

PROXY, INTERNAL, EGRESS, DIGEST = "f" * 64, "1" * 64, "2" * 64, "8" * 64


def topology(*, running=True):
    primary = entry(running=running)
    primary["Created"] = STAMP
    primary["State"]["Status"] = "running" if running else "created"
    primary["Config"]["Labels"].update({"deerflow.network_mode": "allowlist", "deerflow.network_policy_digest": DIGEST})
    primary["HostConfig"]["NetworkMode"] = "retained-internal"
    primary["NetworkSettings"] = {"Networks": {"retained-internal": {"NetworkID": INTERNAL if running else "", "EndpointID": "9" * 64 if running else ""}}}
    labels = {"deerflow.sandbox_id": "native", "deerflow.network_mode": "allowlist", "deerflow.network_policy_digest": DIGEST}
    proxy = {
        "Id": PROXY,
        "Name": "/retained-proxy",
        "Created": STAMP,
        "Config": {"Labels": {**labels, "deerflow.role": "network-proxy"}},
        "HostConfig": {"NetworkMode": "retained-egress", "AutoRemove": True, "Privileged": False, "CapDrop": ["ALL"], "SecurityOpt": ["no-new-privileges"], "PidMode": "", "IpcMode": "private", "RestartPolicy": {"Name": "no"}},
        "State": {"Running": True, "Status": "running"},
        "Mounts": [],
        "NetworkSettings": {
            "Networks": {
                "retained-internal": {"NetworkID": INTERNAL, "EndpointID": "4" * 64},
                "retained-egress": {"NetworkID": EGRESS, "EndpointID": "5" * 64},
            }
        },
    }
    internal = {
        "Id": INTERNAL,
        "Name": "retained-internal",
        "Created": STAMP,
        "Driver": "bridge",
        "Internal": True,
        "Labels": {**labels, "deerflow.role": "network"},
        "Options": {"com.docker.network.bridge.gateway_mode_ipv4": "isolated", "com.docker.network.bridge.gateway_mode_ipv6": "isolated"},
        "Containers": {PROXY: {"Name": "retained-proxy", "EndpointID": "4" * 64}},
    }
    if running:
        internal["Containers"][CID] = {"Name": primary["Name"][1:], "EndpointID": "9" * 64}
    egress = {
        "Id": EGRESS,
        "Name": "retained-egress",
        "Created": STAMP,
        "Driver": "bridge",
        "Internal": False,
        "Labels": {**labels, "deerflow.role": "egress-network"},
        "Options": {"com.docker.network.bridge.enable_icc": "false"},
        "Containers": {PROXY: {"Name": "retained-proxy", "EndpointID": "5" * 64}},
    }
    objects = {("container", CID): primary, ("container", PROXY): proxy, ("network", INTERNAL): internal, ("network", EGRESS): egress}
    calls = []

    def run(command, **kwargs):
        calls.append(command)
        network = command[1] == "network"
        assert command[2 if network else 1] == "inspect"
        kind, reference = "network" if network else "container", command[-1]
        selected = [obj for (obj_kind, identity), obj in objects.items() if kind == obj_kind and (identity == reference or obj["Name"].removeprefix("/") == reference)]
        if selected:
            return SimpleNamespace(returncode=0, stdout=json.dumps(selected), stderr="")
        error = "Error: No such object: " + reference if kind == "container" else "Error: No such network: " + reference
        return SimpleNamespace(returncode=1, stdout="[]", stderr=error)

    actual = _native_inspection(primary, CID, "daemon-A", slot_id=SLOT)
    return NativeResourceSet(SimpleNamespace(_run=run)), primary, actual, objects, calls


def capture(reader, primary, actual):
    digest, resources = reader.capture(primary, actual)
    return NativeTeardownRecord("e" * 32, actual.daemon_id, *actual.owner, actual.sandbox_id, actual.slots, SLOT, actual.container_id, actual.container_name, actual.created_at, actual.network_mode, digest, resources)


def leftover(objects, *, sandbox_id="native"):
    """The set as a failed start leaves it: no primary, and its relay and networks as they stood."""
    objects.pop(("container", CID))
    for key in (("network", INTERNAL), ("network", EGRESS)):
        objects[key]["Containers"].pop(CID, None)
    facts = (("container", "network-proxy", PROXY, "retained-proxy"), ("network", "network", INTERNAL, "retained-internal"), ("network", "egress-network", EGRESS, "retained-egress"))
    return tuple(PairedResource(NativeTeardownResource(kind, role, identity, name, STAMP), sandbox_id, "allowlist", DIGEST) for kind, role, identity, name in facts)


def test_a_set_with_no_primary_can_be_proved_to_hold_no_writer():
    reader, _primary, _actual, objects, calls = topology()
    reader.require_no_writer(leftover(objects))
    assert all("inspect" in command for command in calls)


def test_a_part_of_the_set_already_gone_proves_nothing_against_it():
    reader, _primary, _actual, objects, _calls = topology()
    items = leftover(objects)
    del objects[("container", PROXY)]
    for key in (("network", INTERNAL), ("network", EGRESS)):
        objects[key]["Containers"].clear()
    reader.require_no_writer(items)


@pytest.mark.parametrize(
    "fault,message",
    [
        ("host-mount", "host storage mount"),
        ("volume", "host storage mount"),
        ("storage-label", "storage writer attribution"),
        ("owner-label", "storage writer attribution"),
        ("other-sandbox", "attribution differs"),
        ("network-storage-label", "storage writer attribution"),
        ("foreign-member", "foreign or absent member"),
        ("foreign-egress-member", "foreign or absent member"),
        ("another-sets-relay", "foreign or absent member"),
        ("privileged", "not confined"),
        ("capability", "not confined"),
        ("device", "not confined"),
        ("host-pid", "not confined"),
        ("host-ipc", "not confined"),
        ("no-host-config", "not confined"),
        ("capabilities-kept", "not confined"),
        ("privileges-can-grow", "not confined"),
        ("device-request", "not confined"),
        ("device-rule", "not confined"),
        ("restarts-itself", "not confined"),
    ],
)
def test_a_set_that_could_hold_a_writer_is_refused(fault, message):
    reader, _primary, _actual, objects, _calls = topology()
    items = leftover(objects)
    proxy, internal = objects[("container", PROXY)], objects[("network", INTERNAL)]
    host = proxy["HostConfig"]
    if fault == "privileged":
        host["Privileged"] = True
    elif fault == "capabilities-kept":
        host["CapDrop"] = []
    elif fault == "privileges-can-grow":
        host["SecurityOpt"] = []
    elif fault == "device-request":
        host["DeviceRequests"] = [{"Driver": "nvidia"}]
    elif fault == "device-rule":
        host["DeviceCgroupRules"] = ["b 8:* rwm"]
    elif fault == "restarts-itself":
        host["RestartPolicy"] = {"Name": "always"}
    elif fault == "capability":
        host["CapAdd"] = ["SYS_ADMIN"]
    elif fault == "device":
        host["Devices"] = [{"PathOnHost": "/dev/sda", "PathInContainer": "/dev/sda", "CgroupPermissions": "rwm"}]
    elif fault == "host-pid":
        host["PidMode"] = "host"
    elif fault == "host-ipc":
        host["IpcMode"] = "host"
    elif fault == "no-host-config":
        del proxy["HostConfig"]
    elif fault == "network-storage-label":
        internal["Labels"]["hartmesh.storage.space." + SLOT] = "1"
    elif fault == "foreign-egress-member":
        objects[("network", EGRESS)]["Containers"]["a" * 64] = {"Name": "writer", "EndpointID": "6" * 64}
    elif fault == "host-mount":
        proxy["Mounts"] = [{"Type": "bind", "Source": "/pool/slot", "Destination": "/data", "RW": True}]
    elif fault == "volume":
        proxy["Mounts"] = [{"Type": "volume", "Name": "data", "Destination": "/data"}]
    elif fault == "storage-label":
        proxy["Config"]["Labels"]["hartmesh.storage.space." + SLOT] = "1"
    elif fault == "owner-label":
        proxy["Config"]["Labels"]["deerflow.owner_user_id"] = "alice"
    elif fault == "other-sandbox":
        proxy["Config"]["Labels"]["deerflow.sandbox_id"] = "other"
    elif fault == "foreign-member":
        internal["Containers"]["a" * 64] = {"Name": "writer", "EndpointID": "6" * 64}
    else:
        proxy["Config"]["Labels"]["deerflow.sandbox_id"] = "other"
        items = (replace(items[0], sandbox_id="other"), *items[1:])
    with pytest.raises(StorageHostUnqualified, match=message):
        reader.require_no_writer(items)


def removing(reader, replies=None):
    """Answer removals from ``replies`` by immutable ID, keeping each command in order."""
    commands, original = [], reader.backend._run

    def run(command, **kwargs):
        if "rm" not in command:
            return original(command, **kwargs)
        commands.append(command)
        reply = (replies or {}).get(command[-1], 0)
        if isinstance(reply, Exception):
            raise reply
        return SimpleNamespace(returncode=reply, stdout="", stderr="")

    reader.backend._run = run
    return commands


def test_a_leftover_goes_by_immutable_id_with_its_relay_first():
    reader, _primary, _actual, objects, _calls = topology()
    items = leftover(objects)
    commands = removing(reader)
    assert set(remove_leftovers(reader, tuple(reversed(items)))) == set(items)
    assert commands[0] == ["docker", "rm", "--force", PROXY]
    assert sorted(commands[1:]) == [["docker", "network", "rm", INTERNAL], ["docker", "network", "rm", EGRESS]]


@pytest.mark.parametrize("reply", [1, subprocess.TimeoutExpired("docker", 15), OSError("Docker went away")], ids=["refused", "timed-out", "unreachable"])
def test_a_member_that_will_not_go_leaves_the_rest_to_go(reply):
    reader, _primary, _actual, objects, _calls = topology()
    items = leftover(objects)
    commands = removing(reader, {PROXY: reply})
    assert set(remove_leftovers(reader, items)) == set(items[1:])
    assert len(commands) == 3


def test_removal_keeps_to_the_readers_budget():
    reader, _primary, _actual, objects, _calls = topology()
    items = leftover(objects)
    commands = removing(reader)
    reader.deadline = time.monotonic() - 1
    assert remove_leftovers(reader, items) == ()
    assert commands == []


def test_a_removal_gets_no_more_than_the_budget_left():
    reader, _primary, _actual, objects, _calls = topology()
    timeouts = []
    reader.backend._run = lambda _command, **kwargs: timeouts.append(kwargs["timeout"]) or SimpleNamespace(returncode=0, stdout="", stderr="")
    reader.deadline = time.monotonic() + 5
    remove_leftovers(reader, leftover(objects))
    assert timeouts and max(timeouts) <= 5


def test_a_runtime_that_stops_being_current_stops_the_removal():
    reader, _primary, _actual, objects, _calls = topology()
    items = leftover(objects)
    commands = removing(reader)
    checks = iter((None,))

    def guard():
        if next(checks, False) is False:
            raise StorageHostUnqualified("Runtime closed")

    reader.guard = guard
    with pytest.raises(StorageHostUnqualified, match="Runtime closed"):
        remove_leftovers(reader, items)
    assert commands == [["docker", "rm", "--force", PROXY]]


@pytest.mark.parametrize("kind,identity", [("container", PROXY), ("network", INTERNAL), ("network", EGRESS)])
def test_paired_role_without_sandbox_label_cannot_disappear_from_census(kind, identity):
    reader, _primary, _actual, objects, calls = topology()
    candidate = objects[(kind, identity)]
    labels = candidate["Config"]["Labels"] if kind == "container" else candidate["Labels"]
    labels.pop("deerflow.sandbox_id")
    original = reader.backend._run

    def run(command, **kwargs):
        if command[1] == "ps" or command[1:3] == ["network", "ls"]:
            calls.append(command)
            selector = command[command.index("--filter") + 1]
            selected = selector == "label=deerflow.role=" + labels["deerflow.role"] and (command[1] == "ps") == (kind == "container")
            return SimpleNamespace(returncode=0, stdout=identity if selected else "", stderr="")
        return original(command, **kwargs)

    reader.backend._run = run
    with pytest.raises(StorageHostUnqualified, match="unknown original"):
        reader.inventory_paired()
    assert all("rm" not in command and "start" not in command for command in calls)


@pytest.mark.parametrize("running", [False, True])
def test_complete_graph_uses_actual_ids_names_and_persisted_policy(running):
    reader, primary, actual, _objects, calls = topology(running=running)
    saved = capture(reader, primary, actual)
    assert saved.policy_digest == DIGEST
    assert {item.identity for item in saved.resources} == {PROXY, INTERNAL, EGRESS}
    assert all(item.created_at == STAMP for item in saved.resources)
    assert {item.name for item in saved.resources} == {"retained-proxy", "retained-internal", "retained-egress"}
    assert calls and all("inspect" in command for command in calls)


@pytest.mark.parametrize("running", [False, True])
def test_mountless_primary_capture_preserves_complete_original_restricted_graph(running):
    from deerflow.spaces.native_teardown_evidence import NativeMountlessTeardownRecord

    reader, primary, _actual, _objects, calls = topology(running=running)
    primary["Config"]["Labels"].pop("hartmesh.storage.space." + SLOT)
    actual = _native_inspection(primary, CID, "daemon-A", allow_mountless=True)
    digest, resources = reader.capture(primary, actual)
    saved = NativeMountlessTeardownRecord("e" * 32, actual.daemon_id, *actual.owner, actual.sandbox_id, (), None, actual.container_id, actual.container_name, actual.created_at, actual.network_mode, digest, resources)
    assert NativeTeardownRecord.parse(saved.document(), saved.id) == saved
    assert {item.identity for item in resources} == {PROXY, INTERNAL, EGRESS}
    assert len(reader.qualify(saved)) == 4
    assert all("inspect" in command for command in calls)


@pytest.mark.parametrize("change", ["internal-foreign", "egress-foreign", "proxy-host-bind", "proxy-policy", "internal-isolation", "egress-icc", "primary-network-id", "proxy-endpoint"])
def test_incomplete_or_foreign_graph_refuses_before_effects(change):
    reader, primary, actual, objects, calls = topology()
    proxy, internal, egress = objects[("container", PROXY)], objects[("network", INTERNAL)], objects[("network", EGRESS)]
    if change.endswith("foreign"):
        (internal if change.startswith("internal") else egress)["Containers"]["7" * 64] = {"Name": "foreign", "EndpointID": "6" * 64}
    elif change == "proxy-host-bind":
        proxy["Mounts"] = [{"Type": "bind", "Source": "/operator/pool", "Destination": "/data", "RW": True}]
    elif change == "proxy-policy":
        proxy["Config"]["Labels"]["deerflow.network_policy_digest"] = "7" * 64
    elif change == "internal-isolation":
        internal["Options"]["com.docker.network.bridge.gateway_mode_ipv6"] = "nat"
    elif change == "egress-icc":
        egress["Options"]["com.docker.network.bridge.enable_icc"] = "true"
    elif change == "primary-network-id":
        primary["NetworkSettings"]["Networks"]["retained-internal"]["NetworkID"] = "7" * 64
    else:
        internal["Containers"][PROXY]["EndpointID"] = "7" * 64
    with pytest.raises(StorageHostUnqualified):
        capture(reader, primary, actual)
    assert all("inspect" in command for command in calls)


def test_recovery_qualifies_original_survivors_after_primary_disappears():
    reader, primary, actual, objects, _calls = topology()
    saved = capture(reader, primary, actual)
    del objects[("container", CID)]
    del objects[("network", INTERNAL)]["Containers"][CID]
    survivors = reader.qualify(saved)
    assert survivors[("container", CID)] is None
    assert survivors[("container", PROXY)]["Id"] == PROXY
    assert not reader.prove_absent(saved)
    objects.clear()
    assert reader.prove_absent(saved)


@pytest.mark.parametrize("member", ["container", "network"])
def test_reused_original_name_is_not_absence_or_cleanup_permission(member):
    reader, primary, actual, objects, _calls = topology()
    saved = capture(reader, primary, actual)
    identity = PROXY if member == "container" else EGRESS
    replacement = deepcopy(objects.pop((member, identity)))
    replacement["Id"] = "7" * 64
    objects[(member, replacement["Id"])] = replacement
    with pytest.raises(StorageHostUnqualified):
        reader.qualify(saved)
    with pytest.raises(StorageHostUnqualified):
        reader.prove_absent(saved)


def test_fresh_capture_refuses_primary_disappearance_before_final_graph_proof():
    reader, primary, actual, objects, _calls = topology()
    inspect = reader.inspect
    removed = []

    def disappear(kind, reference):
        result = inspect(kind, reference)
        if kind == "network" and reference == "retained-egress" and not removed:
            objects.pop(("container", CID))
            objects[("network", INTERNAL)]["Containers"].pop(CID)
            removed.append(True)
        return result

    reader.inspect = disappear
    with pytest.raises(StorageHostUnqualified):
        capture(reader, primary, actual)
    assert removed


@pytest.mark.parametrize("reference", [CID, "retained-primary"])
def test_docker_29_exact_lowercase_object_absence(reference):
    response = SimpleNamespace(returncode=1, stdout="[]\n", stderr="error: no such object: " + reference + "\n")
    reader = NativeResourceSet(SimpleNamespace(_run=lambda *_args, **_kwargs: response))
    assert reader.inspect("container", reference) is None


@pytest.mark.parametrize("change", ["wrong-reference", "extra-error", "nonempty-json", "wrong-returncode", "wrong-kind", "case-guess"])
def test_lowercase_absence_does_not_relax_exact_reply_proof(change):
    response = SimpleNamespace(returncode=1, stdout="[]\n", stderr="error: no such object: " + CID)
    kind = "container"
    if change == "wrong-reference":
        response.stderr = "error: no such object: " + PROXY
    elif change == "extra-error":
        response.stderr += "\npermission denied"
    elif change == "nonempty-json":
        response.stdout = '[{"unexpected": true}]'
    elif change == "wrong-returncode":
        response.returncode = 125
    elif change == "wrong-kind":
        kind = "network"
    else:
        response.stderr = "ERROR: NO SUCH OBJECT: " + CID
    reader = NativeResourceSet(SimpleNamespace(_run=lambda *_args, **_kwargs: response))
    with pytest.raises(StorageHostUnqualified):
        reader.inspect(kind, CID)


def test_unknown_reply_diagnostics_exclude_reference_and_payload(caplog):
    reference = "private-tenant-reference"
    response = SimpleNamespace(returncode=1, stdout='{"secret": "private-output"}', stderr="private-error-and-token")
    reader = NativeResourceSet(SimpleNamespace(_run=lambda *_args, **_kwargs: response))
    with pytest.raises(StorageHostUnqualified):
        reader.inspect("container", reference)
    assert "Native graph reply unqualified" in caplog.text
    assert "kind=container reference=name returncode=1" in caplog.text
    assert "stdout_shape=object stderr_template=unknown" in caplog.text
    assert all(value not in caplog.text for value in (reference, "private-output", "private-error-and-token", "secret"))


def test_duplicate_key_reply_still_logs_only_structural_diagnostics(caplog):
    response = SimpleNamespace(returncode=1, stdout='{"private-key":"private-value","private-key":"private-other"}', stderr="private-token")
    reader = NativeResourceSet(SimpleNamespace(_run=lambda *_args, **_kwargs: response))
    with pytest.raises(StorageHostUnqualified):
        reader.inspect("container", CID)
    assert "stdout_shape=invalid-json stderr_template=unknown" in caplog.text
    assert all(value not in caplog.text for value in (CID, "private-key", "private-value", "private-other", "private-token"))


def member_reader(kind, observations, *, guard=None):
    """Script independent ID/name reads without a Docker daemon."""
    calls = []
    replies = iter(observations)

    def run(command, **kwargs):
        calls.append(command[-1])
        value = next(replies)
        if value is None:
            error = "Error: No such object: " if kind == "container" else "Error: No such network: "
            return SimpleNamespace(returncode=1, stdout="[]", stderr=error + command[-1])
        return SimpleNamespace(returncode=0, stdout=json.dumps([value]), stderr="")

    return NativeResourceSet(SimpleNamespace(_run=run), guard=guard), calls


@pytest.mark.parametrize("kind", ["container", "network"])
@pytest.mark.parametrize("present_first", [True, False])
@pytest.mark.parametrize("recovery", ["absent", "original"])
def test_split_member_observation_rechecks_both_original_references(kind, present_first, recovery):
    original = {"Id": CID, "Name": "/original" if kind == "container" else "original", "Created": STAMP}
    first_pair = [original, None] if present_first else [None, original]
    recovered = None if recovery == "absent" else original
    guards = []
    reader, calls = member_reader(kind, [*first_pair, recovered, recovered], guard=lambda: guards.append(True))
    deadline = reader.deadline
    assert reader._member(kind, CID, "original", STAMP) == recovered
    assert calls == [CID, "original", CID, "original"]
    assert len(guards) == 8 and reader.deadline == deadline


@pytest.mark.parametrize("present_first", [True, False])
@pytest.mark.parametrize("field,value", [("Id", "7" * 64), ("Name", "/changed"), ("Created", "2026-01-01T00:00:00Z")])
def test_split_observation_never_retries_a_changed_surviving_identity(present_first, field, value):
    original = {"Id": CID, "Name": "/original", "Created": STAMP}
    changed = {**original, field: value}
    pair = [changed, None] if present_first else [None, changed]
    reader, calls = member_reader("container", [*pair, original, original])
    with pytest.raises(StorageHostUnqualified):
        reader._member("container", CID, "original", STAMP)
    assert len(calls) <= 2


@pytest.mark.parametrize("recovery", ["split", "reused-name", "changed-created", "malformed"])
def test_member_recheck_refuses_unresolved_or_replaced_original(recovery):
    original = {"Id": CID, "Name": "/original", "Created": STAMP}
    second_pair = {
        "split": [original, None],
        "reused-name": [None, {**original, "Id": "7" * 64}],
        "changed-created": [{**original, "Created": "2026-01-01T00:00:00Z"}, original],
        "malformed": [{}, None],
    }[recovery]
    reader, calls = member_reader("container", [original, None, *second_pair])
    with pytest.raises(StorageHostUnqualified):
        reader._member("container", CID, "original", STAMP)
    assert len(calls) <= 4


@pytest.mark.parametrize("lost", ["deadline", "guard"])
def test_member_recheck_keeps_original_deadline_and_lifecycle_guard(monkeypatch, lost):
    from deerflow.community.aio_sandbox import native_resource_set

    original = {"Id": CID, "Name": "/original", "Created": STAMP}
    now = [0]
    monkeypatch.setattr(native_resource_set, "time", SimpleNamespace(monotonic=lambda: now[0]))
    guarded = []

    def guard():
        guarded.append(True)
        if len(guarded) == 4 and lost == "deadline":
            now[0] = 31
        if len(guarded) == 5 and lost == "guard":
            raise StorageHostUnqualified("Lifecycle proof lost")

    reader, calls = member_reader("container", [original, None, None, None], guard=guard)
    with pytest.raises(StorageHostUnqualified):
        reader._member("container", CID, "original", STAMP)
    assert calls == [CID, "original"]
    assert reader.deadline == 30
    assert len(guarded) == (4 if lost == "deadline" else 5)
