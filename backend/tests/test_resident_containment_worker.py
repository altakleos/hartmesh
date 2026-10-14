"""Original actorless removal and durability with offline Docker and disk journal."""

import os
import time
from dataclasses import replace
from threading import Lock
from types import SimpleNamespace

import pytest
from test_native_create_bridge import fixture as fixture
from test_native_resident_evidence import record
from test_native_resource_set import topology

from deerflow.community.aio_sandbox.owner_admission import OwnerAdmissions
from deerflow.community.aio_sandbox.resident_containment import ResidentContainmentWorker
from deerflow.spaces.docker import StorageHostUnqualified
from deerflow.spaces.native_teardown_evidence import NativeTeardownResource


def worker(fixture, *, lost_reply=False):
    journal, _handoffs, bridge, _owner = fixture
    reader, primary, _actual, objects, calls = topology(running=True)
    base = record(container_id=primary["Id"])
    primary["Name"] = "/" + base.name
    primary["Mounts"] = [{"Type": "bind", "Source": source, "Destination": destination, "RW": writable, "Propagation": "rprivate"} for source, destination, writable in base.mounts]
    primary["HostConfig"]["PortBindings"] = {}
    primary["NetworkSettings"]["Ports"] = {}
    primary["Config"]["Labels"] = {
        "deerflow.role": "resident",
        "deerflow.sandbox_id": base.attachment_id,
        "deerflow.network_mode": "allowlist",
        "deerflow.network_policy_digest": base.policy_digest,
        "hartmesh.storage.attachment": base.attachment_id,
        "hartmesh.storage.host": base.daemon_id,
        **{"hartmesh.storage.space." + slot: "1" for slot in base.slots},
    }
    from hashlib import sha256

    for key in list(primary["Config"]["Labels"]):
        if key.startswith("hartmesh.storage.readonly."):
            del primary["Config"]["Labels"][key]
    for root in base.roots:
        if root.revision is not None:
            primary["Config"]["Labels"]["hartmesh.storage.readonly." + sha256(root.destination.encode()).hexdigest()[:16]] = root.revision
    resources = []
    for (kind, identity), entry in objects.items():
        if identity == primary["Id"]:
            continue
        labels = entry["Config"]["Labels"] if kind == "container" else entry["Labels"]
        labels["deerflow.sandbox_id"] = base.attachment_id
        if kind == "network" and primary["Id"] in entry["Containers"]:
            entry["Containers"][primary["Id"]]["Name"] = base.name
        resources.append(NativeTeardownResource(kind, labels["deerflow.role"], identity, entry["Name"].removeprefix("/"), entry["Created"]))
    # The existing fake topology's policy digest is authoritative here.
    digest = next(entry["Labels"]["deerflow.network_policy_digest"] for (kind, _id), entry in objects.items() if kind == "network")
    primary["Config"]["Labels"]["deerflow.network_policy_digest"] = digest
    saved = journal.begin_resident(replace(base, policy_digest=digest, resources=tuple(resources)))
    bridge.qualify()
    original = reader.backend._run

    def engine(command, **kwargs):
        if "inspect" in command:
            return original(command, **kwargs)
        calls.append(command)
        identity = command[-1]
        kind = "network" if command[1] == "network" else "container"
        objects.pop((kind, identity), None)
        for entry in objects.values():
            entry.get("Containers", {}).pop(identity, None)
        if lost_reply:
            raise OSError("Effect response was lost")
        return SimpleNamespace(returncode=0, stdout=identity, stderr="")

    reader.backend._run = engine
    gate = OwnerAdmissions(lock=Lock())
    token = gate.reserve_resident_batch((("resident:" + saved.attachment_id, tuple(root.slot_id for root in saved.roots if root.writable), tuple(root.slot_id for root in saved.roots if not root.writable)),))
    provider = SimpleNamespace(_native_creation=bridge, _backend=reader.backend)
    capability = ResidentContainmentWorker(provider=provider, adapter=SimpleNamespace(_backend=reader.backend), gate=gate, reservation=token, records=(saved,), roots_guard=lambda _record: None)
    return capability, gate, token, saved, objects, calls


def test_original_graph_stays_durable_until_caller_finalizes_after_fenced_commit(fixture):
    capability, gate, token, saved, objects, calls = worker(fixture)
    try:
        capability.contain(saved.attachment_id)
        retained = capability.bridge.evidence.snapshot().residents
        assert len(retained) == 1 and retained[0].phase == "retiring" and not objects
        assert capability.bridge.residents.records() == retained
        assert all(command[-1] in {saved.container_id, *(member.identity for member in saved.resources)} for command in calls if "rm" in command or "stop" in command)
        capability.finalize(saved.attachment_id)
        assert capability.bridge.evidence.snapshot().residents == () and capability.bridge.residents.records() == ()
    finally:
        capability.close()
        gate.end_resident_batch(token)


def test_lost_effect_response_preserves_original_graph_and_charge(fixture):
    capability, gate, token, saved, objects, _calls = worker(fixture, lost_reply=True)
    try:
        with pytest.raises(OSError):
            capability.contain(saved.attachment_id)
        retained = capability.bridge.evidence.snapshot().residents
        assert len(retained) == 1 and retained[0].phase == "retiring" and objects
        assert capability.bridge.residents.records() == retained
    finally:
        capability.close()
        gate.end_resident_batch(token)


def test_expired_scope_cannot_mutate_even_an_original_matching_graph(fixture):
    capability, gate, token, saved, objects, calls = worker(fixture)
    before = tuple(objects)
    capability.close()
    try:
        with pytest.raises(StorageHostUnqualified):
            capability.contain(saved.attachment_id)
        assert tuple(objects) == before and not any("stop" in command or "rm" in command for command in calls)
    finally:
        gate.end_resident_batch(token)


def test_fenced_sync_retry_reproves_absence_without_retirement_publication(fixture, monkeypatch):
    capability, gate, token, saved, objects, calls = worker(fixture)
    capability.contain(saved.attachment_id)
    journal = capability.bridge.evidence
    fsync = os.fsync

    def interrupted(fd):
        if fd == journal._directory_fd:
            raise OSError("Terminal directory sync was lost")
        return fsync(fd)

    monkeypatch.setattr(os, "fsync", interrupted)
    try:
        with pytest.raises(OSError):
            capability.finalize(saved.attachment_id)
        assert not objects and len(journal.snapshot().residents) == 1
        monkeypatch.setattr(os, "fsync", fsync)
        before = [command for command in calls if "stop" in command or "rm" in command]
        capability.confirm_absent(saved.attachment_id)
        capability.finalize(saved.attachment_id)
        assert journal.snapshot().residents == ()
        assert [command for command in calls if "stop" in command or "rm" in command] == before
    finally:
        capability.close()
        gate.end_resident_batch(token)


@pytest.mark.parametrize("closing", [False, True])
def test_retirement_publication_reply_loss_restores_charge_for_a_fresh_worker(fixture, monkeypatch, closing):
    capability, gate, token, saved, objects, calls = worker(fixture)
    journal = capability.bridge.evidence
    publish = journal.retire_resident

    def interrupted(record):
        publish(record)
        raise OSError("Retirement publication reply was lost")

    monkeypatch.setattr(journal, "retire_resident", interrupted)
    try:
        fixture[3].live = not closing
        with pytest.raises(OSError, match="publication reply"):
            capability.contain(saved.attachment_id)
        retained = capability.bridge.residents.for_attachment(saved.attachment_id)
        assert retained.phase == "retiring" and objects
        assert not any("stop" in command or "rm" in command for command in calls)
        monkeypatch.setattr(journal, "retire_resident", publish)
        capability.close()
        fresh = ResidentContainmentWorker(provider=capability.provider, adapter=capability.adapter, gate=gate, reservation=token, records=(retained,), roots_guard=lambda _record: None)
        try:
            fresh.contain(saved.attachment_id)
            fresh.finalize(saved.attachment_id)
            assert not objects and journal.snapshot().residents == ()
        finally:
            fresh.close()
    finally:
        capability.close()
        gate.end_resident_batch(token)


def test_expired_inherited_recovery_budget_issues_no_effect_and_keeps_charge(fixture):
    capability, gate, token, saved, objects, calls = worker(fixture)
    try:
        bounded = ResidentContainmentWorker(provider=capability.provider, adapter=capability.adapter, gate=gate, reservation=token, records=(saved,), roots_guard=lambda _record: None, deadline=time.monotonic() - 1)
        try:
            with pytest.raises(StorageHostUnqualified, match="deadline"):
                bounded.contain(saved.attachment_id)
            assert objects and capability.bridge.residents.for_attachment(saved.attachment_id) == saved
            assert not any("stop" in command or "rm" in command for command in calls)
        finally:
            bounded.close()
    finally:
        capability.close()
        gate.end_resident_batch(token)


@pytest.mark.parametrize("interruption", ["missing-primary", "network-proxy", "network", "egress-network"])
def test_original_recovery_resumes_surviving_graph_after_each_lost_effect_reply(fixture, interruption):
    capability, gate, token, saved, objects, calls = worker(fixture)
    engine = capability.backend._run
    failed = []
    if interruption == "missing-primary":
        objects.pop(("container", saved.container_id))
        for entry in objects.values():
            entry.get("Containers", {}).pop(saved.container_id, None)
    else:
        member = next(member for member in saved.resources if member.role == interruption)

        def interrupted(command, **kwargs):
            reply = engine(command, **kwargs)
            if command[-1] == member.identity and "inspect" not in command and not failed:
                failed.append(True)
                raise OSError("Original graph effect reply lost")
            return reply

        capability.backend._run = interrupted
    try:
        if interruption != "missing-primary":
            with pytest.raises(OSError, match="effect reply"):
                capability.recover(saved.attachment_id)
            retained = capability.bridge.residents.for_attachment(saved.attachment_id)
            assert retained.phase == "retiring" and failed
            capability.close()
            capability.backend._run = engine
            capability = ResidentContainmentWorker(provider=capability.provider, adapter=capability.adapter, gate=gate, reservation=token, records=(retained,), roots_guard=lambda _record: None)
        capability.recover(saved.attachment_id)
        assert not objects
        capability.finalize(saved.attachment_id)
        assert capability.bridge.residents.records() == ()
        assert all(command[-1] in {saved.container_id, *(member.identity for member in saved.resources)} for command in calls if "rm" in command or "stop" in command)
    finally:
        capability.close()
        gate.end_resident_batch(token)
