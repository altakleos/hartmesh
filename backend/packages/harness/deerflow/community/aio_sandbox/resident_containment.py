"""Private original-graph worker; the host retains lifecycle and SQL authority."""

import time

from deerflow.spaces.docker import StorageHostUnqualified

from .native_resource_set import NativeResourceSet


class ResidentContainmentWorker:
    """No SQL, SDK, human principal or name-derived deletion authority."""

    def __init__(self, *, provider, adapter, gate, reservation, records, roots_guard, deadline=None, recovery=False, eligibility_guard=None):
        self.provider, self.adapter, self.gate, self.reservation = provider, adapter, gate, reservation
        self.bridge, self.backend = provider._native_creation, provider._backend
        self.records = {record.attachment_id: record for record in records}
        self.roots_guard = roots_guard
        self.active = True
        self.contained = set()
        self.deadline = deadline
        self.recovery = recovery
        self.eligibility_guard = eligibility_guard

    def _check_deadline(self):
        if self.deadline is not None and time.monotonic() >= self.deadline:
            raise StorageHostUnqualified("Resident recovery exhausted its inherited deadline")

    def _assert_live(self):
        self._check_deadline()
        if not self.active or self.provider._native_creation is not self.bridge or self.provider._backend is not self.backend or self.adapter._backend is not self.backend or not self.gate.current_resident_batch(self.reservation):
            raise StorageHostUnqualified("Resident containment lost its original runtime or lifecycle scope")

    def assert_current(self):
        self._assert_live()
        pending = self.bridge.assert_current(teardown=True)
        if self.recovery:
            self._guard_creates(pending)
        self._check_deadline()

    def _guard_creates(self, pending):
        names = {record.name for record in self.records.values()}
        cids = {record.container_id for record in self.records.values()}
        writers = {root.slot_id for record in self.records.values() for root in record.roots if root.writable}

        def overlaps(attempt):
            return attempt.name in names or attempt.owner_id in self.reservation.owners or bool(writers.intersection(attempt.slots))

        if any(overlaps(attempt) for attempt in pending):
            raise StorageHostUnqualified("Resident recovery retains an original create request")
        with self.bridge.handoffs.lock:
            if any(overlaps(handoff.attempt) or (handoff.verified is not None and handoff.verified.container_id in cids) for handoff in self.bridge.handoffs._items.values()):
                raise StorageHostUnqualified("Resident recovery retains an untransferred create handoff")

    def _reader(self, *, budget=30):
        reader = NativeResourceSet(self.backend, guard=self.assert_current)
        reader.deadline = min(time.monotonic() + budget, self.deadline) if self.deadline is not None else time.monotonic() + budget
        return reader

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
        reader = self._reader(budget=60)
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
        if self.eligibility_guard is not None:
            self.eligibility_guard()
            self._assert_live()
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
        reader = self._reader()
        if not reader.prove_resident_absent(record):
            raise StorageHostUnqualified("Resident finalization retains surviving original resources")
        self.assert_current()
        if self.eligibility_guard is not None:
            self.eligibility_guard()
            self._assert_live()
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
        reader = self._reader()
        if not reader.prove_resident_absent(record):
            raise StorageHostUnqualified("Resident fenced retry retains original resources")
        self.contained.add(attachment_id)

    def recover(self, attachment_id):
        """Explicit host recovery may remove survivors; tombstone retries only observe."""
        record = self._current_record(attachment_id)
        self.roots_guard(record)
        if record.phase == "retiring" and self._reader().prove_resident_absent(record):
            self.contained.add(attachment_id)
            return
        self.contain(attachment_id)

    def close(self):
        """Caller already drained every worker before expiring its capability."""
        self.active = False
