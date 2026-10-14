"""Private original-graph worker; the host retains lifecycle and SQL authority."""

import time

from deerflow.spaces.docker import StorageHostUnqualified

from .native_resource_set import NativeResourceSet


class ResidentContainmentWorker:
    """No SQL, SDK, human principal or name-derived deletion authority."""

    def __init__(self, *, provider, adapter, gate, reservation, records, roots_guard):
        self.provider, self.adapter, self.gate, self.reservation = provider, adapter, gate, reservation
        self.bridge, self.backend = provider._native_creation, provider._backend
        self.records = {record.attachment_id: record for record in records}
        self.roots_guard = roots_guard
        self.active = True
        self.contained = set()

    def assert_current(self):
        if not self.active or self.provider._native_creation is not self.bridge or self.provider._backend is not self.backend or self.adapter._backend is not self.backend or not self.gate.current_resident_batch(self.reservation):
            raise StorageHostUnqualified("Resident containment lost its original runtime or lifecycle scope")
        self.bridge.assert_current(teardown=True)

    def _current_record(self, attachment_id):
        self.assert_current()
        record = self.records.get(attachment_id)
        if record is None or record.daemon_id != self.bridge.evidence.daemon_id or self.bridge.residents.for_attachment(attachment_id) != record:
            raise StorageHostUnqualified("Resident containment lost its exact retained original graph")
        return record

    def contain(self, attachment_id):
        """Retain the original journal through physical completion and SQL commit."""
        record = self._current_record(attachment_id)
        self.roots_guard(record)
        reader = NativeResourceSet(self.backend, guard=self.assert_current)
        reader.deadline = time.monotonic() + 60
        # Qualify before publication, including a primary already removed.
        reader.qualify_resident_record(record)
        try:
            retiring = self.bridge.evidence.retire_resident(record)
            self.bridge.qualify(teardown=True)
        except BaseException:
            self.bridge._check_failure_evidence(teardown=True)
            raise
        self.records[attachment_id] = retiring
        record = self._current_record(attachment_id)
        primary = ("container", record.container_id)
        members = [primary, *((member.kind, member.identity) for role in ("network-proxy", "network", "egress-network") for member in record.resources if member.role == role)]
        for kind, identity in members:
            self._current_record(attachment_id)
            self.roots_guard(record)
            entry = reader.qualify_resident_record(record)[(kind, identity)]
            if entry is None:
                continue
            if kind == "container" and reader._state(entry):
                self._command(reader, ["docker", "stop", "--time", "5", identity])
                self._current_record(attachment_id)
                self.roots_guard(record)
                if reader.qualify_resident_record(record)[(kind, identity)] is None:
                    continue
            self._command(reader, ["docker", *(["rm", "--force"] if kind == "container" else ["network", "rm"]), identity])
        self._current_record(attachment_id)
        self.roots_guard(record)
        if not reader.prove_resident_absent(record):
            raise StorageHostUnqualified("Resident original resource absence remains unconfirmed")
        self.contained.add(attachment_id)

    def _command(self, reader, command):
        self.assert_current()
        remaining = reader.deadline - time.monotonic()
        if remaining <= 0:
            raise StorageHostUnqualified("Resident containment exhausted its overall deadline")
        reply = self.backend._run(command, capture_output=True, text=True, timeout=min(30, remaining))
        self.assert_current()
        if reply.returncode:
            raise StorageHostUnqualified("Resident original resource effect remains unconfirmed")

    def finalize(self, attachment_id):
        """Host caller confirmed fenced SQL commit; retry fresh absence only."""
        record = self._current_record(attachment_id)
        if record.phase != "retiring":
            raise StorageHostUnqualified("Resident finalization lacks its retiring original record")
        reader = NativeResourceSet(self.backend, guard=self.assert_current)
        if not reader.prove_resident_absent(record):
            raise StorageHostUnqualified("Resident finalization retains surviving original resources")
        self.assert_current()
        try:
            self.bridge.evidence.finish_resident(record)
            self.bridge.residents.remove(record)
            self.bridge.qualify(teardown=True)
        except BaseException:
            self.bridge._check_failure_evidence(teardown=True)
            raise
        self.records.pop(attachment_id)

    def confirm_absent(self, attachment_id):
        """Already-fenced retry proves absence without replaying effects or intent."""
        record = self._current_record(attachment_id)
        if record.phase != "retiring":
            raise StorageHostUnqualified("Resident absence-only retry lacks its original retiring record")
        reader = NativeResourceSet(self.backend, guard=self.assert_current)
        if not reader.prove_resident_absent(record):
            raise StorageHostUnqualified("Resident fenced retry retains original resources")
        self.contained.add(attachment_id)

    def close(self):
        """Caller already drained every worker before expiring its capability."""
        self.active = False
