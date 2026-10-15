"""Actual immutable graph proof with an offline Docker command double."""

import json
from copy import deepcopy
from types import SimpleNamespace

import pytest
from test_native_slot_inventory import CID, SLOT, entry
from test_native_teardown_evidence import STAMP

from deerflow.community.aio_sandbox.local_backend import _native_inspection
from deerflow.community.aio_sandbox.native_resource_set import NativeResourceSet
from deerflow.spaces.docker import StorageHostUnqualified
from deerflow.spaces.native_teardown_evidence import NativeTeardownRecord

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
        "HostConfig": {"NetworkMode": "retained-egress", "AutoRemove": True},
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
