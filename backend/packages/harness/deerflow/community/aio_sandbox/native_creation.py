"""Private memory-only handoff for managed native creation identity/capacity.

Journal and Docker operations belong outside the provider metadata lock. A
verified result stays retained until ordinary tracking/quarantine has taken it,
including when the awaiting caller is cancelled after its worker completes.
"""

import json
import re
from contextlib import nullcontext
from contextvars import ContextVar
from dataclasses import dataclass, field

from deerflow.spaces.docker import StorageHostUnqualified
from deerflow.spaces.native_create_evidence import ATTEMPT_LABEL, MAX_RECORDS, NativeContainerIdentity, NativeCreateAttempt, _unique_object
from deerflow.spaces.runtime_owner import StorageRuntimeUnavailable

MAX_RESTORE_UPDATES = MAX_RECORDS * 2
NATIVE_CREATE_RESERVATION = ContextVar("native_create_reservation", default=None)
NATIVE_CREATE_HANDOFF = ContextVar("native_create_handoff", default=None)
_SLOT_PREFIX = "hartmesh.storage.space."


def _facts(record):
    return record.id, record.daemon_id, record.owner_id, record.thread_id, record.name, record.slots


@dataclass(eq=False)
class NativeCreateHandoff:
    attempt: NativeCreateAttempt
    sandbox_id: str | None = None
    reservation: object | None = field(default=None, repr=False)
    verified: NativeContainerIdentity | None = None
    acknowledged: bool = False
    submitted: bool = False


@dataclass(eq=False)
class _RestoreToken:
    touched: dict[str, NativeCreateAttempt] = field(default_factory=dict)
    overflowed: bool = False


class NativeCreateHandoffs:
    """All mutations share the provider's metadata lock; none performs IO."""

    def __init__(self, *, daemon_id, lock):
        self.daemon_id, self.lock = daemon_id, lock
        self._items = {}
        self._restore = None
        self._installed = False

    @property
    def ready(self):
        return self._installed

    def _touch(self, record):
        if self._restore is not None:
            updates = self._restore.touched
            if record.id in updates or len(updates) < MAX_RESTORE_UPDATES:
                updates[record.id] = record
            else:
                self._restore.overflowed = True
                self._installed = False

    def begin_restore(self):
        with self.lock:
            if self._restore is not None:
                raise StorageHostUnqualified("Native pending restoration is already owned")
            self._restore = _RestoreToken()
            return self._restore

    def finish_restore(self, token, records):
        with self.lock:
            if not isinstance(token, _RestoreToken) or token is not self._restore or token.overflowed:
                self._installed = False
                raise StorageHostUnqualified("Native pending snapshot changed beyond its bounded restoration window")
            additions = []
            for record in records:
                if not isinstance(record, NativeCreateAttempt):
                    self._installed = False
                    raise StorageHostUnqualified("Native pending restoration lacks validated journal facts")
                current = self._items.get(record.id)
                expected = current.attempt if current is not None else token.touched.get(record.id)
                if expected is not None and _facts(expected) != _facts(record):
                    self._installed = False
                    raise StorageHostUnqualified("Native pending snapshot changed immutable attempt facts")
                if record.id not in token.touched and current is None:
                    additions.append(NativeCreateHandoff(record))
            for handoff in additions:
                self._items[handoff.attempt.id] = handoff
            self._restore = None
            # Completed installation includes the restored accounting, not
            # merely a helper with open file descriptors.
            self._installed = True

    def abort_restore(self, token):
        with self.lock:
            if self._restore is token:
                self._restore = None
                self._installed = False

    def record_pending(self, record, *, sandbox_id, reservation=None):
        with self.lock:
            if not isinstance(record, NativeCreateAttempt) or record.daemon_id != self.daemon_id or record.phase != "pending":
                raise StorageHostUnqualified("Native submission requires its current daemon pending attempt")
            handoff = self._items.get(record.id)
            if handoff is None:
                handoff = self._items[record.id] = NativeCreateHandoff(record, sandbox_id, reservation)
            elif _facts(handoff.attempt) != _facts(record) or handoff.sandbox_id not in (None, sandbox_id) or (handoff.reservation is not None and handoff.reservation is not reservation) or handoff.acknowledged:
                raise StorageHostUnqualified("Native submission handoff changed its immutable correlation")
            else:
                handoff.sandbox_id = sandbox_id
                handoff.reservation = reservation
            self._touch(record)
            return handoff

    def _current(self, handoff):
        if not isinstance(handoff, NativeCreateHandoff) or self._items.get(handoff.attempt.id) is not handoff:
            raise StorageHostUnqualified("Native result no longer owns its exact handoff")

    def record_verified(self, handoff, identity):
        with self.lock:
            self._current(handoff)
            if (
                not isinstance(identity, NativeContainerIdentity)
                or handoff.attempt.daemon_id != self.daemon_id
                or (identity.attempt_id, identity.daemon_id, identity.owner_id, identity.thread_id, identity.name, identity.slots) != _facts(handoff.attempt)
                or not isinstance(identity.container_id, str)
                or re.fullmatch(r"[0-9a-f]{64}", identity.container_id) is None
                or (handoff.verified is not None and handoff.verified != identity)
            ):
                raise StorageHostUnqualified("Native returned identity does not match its actual pending request")
            handoff.verified = identity
            self._touch(handoff.attempt)

    def record_submission(self, handoff):
        with self.lock:
            self._current(handoff)
            if handoff.submitted or handoff.verified is not None or handoff.acknowledged:
                raise StorageHostUnqualified("Native request has already attempted its one primary submission")
            handoff.submitted = True
            self._touch(handoff.attempt)

    def record_acknowledged(self, handoff):
        with self.lock:
            self._current(handoff)
            if handoff.verified is None:
                raise StorageHostUnqualified("An unverified native result cannot be acknowledged")
            handoff.acknowledged = True
            self._touch(handoff.attempt)

    def transferred_locked(self, handoff):
        """Caller holds the provider lock after exact tracking/quarantine insert."""
        self._current(handoff)
        if handoff.verified is None:
            raise StorageHostUnqualified("Unknown native creation cannot be discarded by transfer")
        if handoff.acknowledged:
            self._items.pop(handoff.attempt.id)
        self._touch(handoff.attempt)

    def extra_slots_locked(self, *, starting, known_container_ids):
        """Caller counts active/warm/starting in the same provider critical section.

        A name alone cannot deduplicate an unknown request against another
        container incarnation. Only this worker's starting reservation or the
        positively verified immutable CID can account for the same capacity.
        """
        return sum(
            1
            for handoff in self._items.values()
            if not (handoff.reservation is not None and starting.get(handoff.sandbox_id) is handoff.reservation) and not (handoff.verified is not None and handoff.verified.container_id in known_container_ids)
        )

    def pending_for(self, *, owner_id=None, name=None, slot_id=None):
        with self.lock:
            return any(
                not handoff.acknowledged and (owner_id is None or handoff.attempt.owner_id == owner_id) and (name is None or handoff.attempt.name == name) and (slot_id is None or slot_id in handoff.attempt.slots)
                for handoff in self._items.values()
            )

    def retained_for_retirement(self, *, owner_id, slot_id):
        """An ACK still retained before tracking transfer consumes its owner/slot."""
        with self.lock:
            return any(handoff.attempt.owner_id == owner_id or slot_id in handoff.attempt.slots for handoff in self._items.values())

    def for_reservation(self, sandbox_id, reservation):
        with self.lock:
            return tuple(handoff for handoff in self._items.values() if handoff.sandbox_id == sandbox_id and handoff.reservation is reservation)

    def known_returned(self, info):
        with self.lock:
            return any(handoff.verified is not None and handoff.sandbox_id == info.sandbox_id and handoff.verified.container_id == info.container_id and handoff.verified.name == info.container_name for handoff in self._items.values())

    def confirmed_absent(self, info):
        """Canonical fenced cleanup proved this exact returned CID absent.

        Absence never settles an unacknowledged request. Only a completed
        request's live result can stop consuming capacity here.
        """
        with self.lock:
            for handoff in tuple(self._items.values()):
                if handoff.acknowledged and handoff.verified is not None and handoff.sandbox_id == info.sandbox_id and handoff.verified.container_id == info.container_id and handoff.verified.name == info.container_name:
                    self._items.pop(handoff.attempt.id)
                    self._touch(handoff.attempt)


class NativeCreateUnconfirmed(StorageHostUnqualified):
    """Retain the request and only positively inspected immutable identity."""

    def __init__(self, handoff=None):
        super().__init__("Native creation is unconfirmed; retain its evidence and capacity")
        self.handoff = handoff


class NativeCreateBridge:
    """Owned blocking journal/Docker protocol; never called under metadata locks."""

    def __init__(self, *, evidence, handoffs, check_owner, owner_accepts, check_daemon, admit_native=None):
        self.evidence, self.handoffs = evidence, handoffs
        self._check_owner, self._owner_accepts, self._check_daemon = check_owner, owner_accepts, check_daemon
        self._admit_native = admit_native or (lambda _owner, _slots: nullcontext())
        self._installed = False
        self._ever_installed = False

    @property
    def ready(self):
        return self._installed and self.evidence.ready and self.handoffs.ready and self._owner_accepts()

    def invalidate(self):
        """Memory-only admission closure; owned identity/teardown is retained."""
        self._installed = False

    def qualify(self):
        token = None
        try:
            self._check_owner()
            self._check_daemon()
            token = self.handoffs.begin_restore()
            pending = self.evidence.pending()
            self._check_owner()
            self._check_daemon()
            self.handoffs.finish_restore(token, pending)
            self._installed = True
            self._check_owner()
            self._ever_installed = True
        except BaseException:
            self._installed = False
            if token is not None:
                self.handoffs.abort_restore(token)
            raise

    def assert_current(self, *, teardown=False):
        installed = self._ever_installed if teardown else self._installed and self.handoffs.ready and self._owner_accepts()
        if not installed or not self.evidence.ready:
            raise StorageRuntimeUnavailable("Native creation bridge has not completed its owning runtime installation")
        try:
            self._check_owner(teardown=teardown)
        except BaseException:
            # Closing retains the binding for owned teardown. A positively
            # observed physical ownership failure must prevent publication.
            if self._owner_accepts():
                self._installed = False
            raise
        try:
            self._check_daemon()
            pending = self.evidence.pending()
        except BaseException:
            self._installed = False
            raise
        return pending

    def assert_candidate_current(self, info, *, teardown=False, known_live=False):
        pending = self.assert_current(teardown=teardown)
        name = info.container_name
        unresolved = any(name is None or record.name == name for record in pending)
        unresolved = unresolved or (name is not None and self.handoffs.pending_for(name=name))
        if unresolved and not (known_live or self.handoffs.known_returned(info)):
            raise StorageHostUnqualified("An unreturned native request cannot be adopted, replaced or removed by its name")

    def _check_failure_evidence(self):
        # begin may publish a record before its fsync/final proof raises, so
        # a scan alone cannot permit admission with stale memory accounting.
        # Merge retained evidence using the same bounded restore protocol.
        try:
            self.qualify()
        except Exception:
            self._installed = False

    def require_admissible(self, *, name, owner_id):
        self.assert_current()
        self.evidence.require_admissible(owner_id=owner_id, name=name)
        if self.handoffs.pending_for(owner_id=owner_id) or self.handoffs.pending_for(name=name):
            raise StorageHostUnqualified("Native creation remains unresolved for this owner or name")

    @staticmethod
    def _requested(labels):
        if not isinstance(labels, dict) or any(not isinstance(key, str) or not isinstance(value, str) for key, value in labels.items()):
            raise StorageHostUnqualified("Native creation requires exact host labels")
        slots = tuple(sorted(key[len(_SLOT_PREFIX) :] for key in labels if key.startswith(_SLOT_PREFIX)))
        owner, thread, sandbox = (labels.get(key) for key in ("deerflow.owner_user_id", "deerflow.thread_id", "deerflow.sandbox_id"))
        if (
            not slots
            or any(re.fullmatch(r"[0-9a-f]{32}", slot) is None or labels[_SLOT_PREFIX + slot] != "1" for slot in slots)
            or labels.get("hartmesh.chat.storage") != "on"
            or labels.get("deerflow.role") != "sandbox"
            or not owner
            or not thread
            or not sandbox
        ):
            raise StorageHostUnqualified("Native creation lacks its exact slot and execution correlation")
        return owner, thread, sandbox, slots

    def begin(self, *, name, labels):
        try:
            owner, _thread, _sandbox, slots = self._requested(labels)
            with self._admit_native(owner, slots):
                return self._begin_admitted(name=name, labels=labels)
        except NativeCreateUnconfirmed:
            raise
        except Exception as exc:
            raise NativeCreateUnconfirmed() from exc

    def _begin_admitted(self, *, name, labels):
        handoff = None
        try:
            owner, thread, sandbox, slots = self._requested(labels)
            self.require_admissible(name=name, owner_id=owner)
            attempt = self.evidence.begin(owner_id=owner, thread_id=thread, name=name, slots=slots)
            handoff = self.handoffs.record_pending(attempt, sandbox_id=sandbox, reservation=NATIVE_CREATE_RESERVATION.get())
            self.assert_current()
            return handoff
        except Exception as exc:
            self._check_failure_evidence()
            raise NativeCreateUnconfirmed(handoff) from exc

    def submit(self, *, name, labels, command, runner, handoff=None):
        try:
            owner, _thread, _sandbox, slots = self._requested(labels)
            with self._admit_native(owner, slots):
                return self._submit_admitted(name=name, labels=labels, command=command, runner=runner, handoff=handoff)
        except NativeCreateUnconfirmed:
            raise
        except Exception as exc:
            raise NativeCreateUnconfirmed(handoff) from exc

    def _submit_admitted(self, *, name, labels, command, runner, handoff=None):
        try:
            if not isinstance(command, list) or len(command) < 4 or command[0] != "docker" or command[1] not in ("run", "create") or command.count("--name") != 1 or command[command.index("--name") + 1] != name:
                raise StorageHostUnqualified("Native submission requires the exact existing primary Docker builder")
            owner, thread, sandbox, slots = self._requested(labels)
            if handoff is None:
                handoff = self.begin(name=name, labels=labels)
            with self.handoffs.lock:
                self.handoffs._current(handoff)
                if handoff.verified is not None or handoff.acknowledged or (handoff.attempt.name, handoff.attempt.owner_id, handoff.attempt.thread_id, handoff.sandbox_id, handoff.attempt.slots) != (name, owner, thread, sandbox, slots):
                    raise StorageHostUnqualified("Native primary submission does not own its exact unreturned request")
            self.assert_current()
            self.handoffs.record_submission(handoff)
            self.assert_current()
            actual_command = [*command[:2], "--label", ATTEMPT_LABEL + "=" + handoff.attempt.id, *command[2:]]
            result = runner(actual_command, capture_output=True, text=True, check=True, **({"timeout": 30} if command[1] == "create" else {}))
            container_id = result.stdout.strip()
            if result.returncode != 0 or re.fullmatch(r"[0-9a-f]{64}", container_id) is None:
                raise StorageHostUnqualified("Native submission did not return a complete immutable container identity")
            self.assert_current()
            inspected = runner(["docker", "inspect", container_id], capture_output=True, text=True, check=True, timeout=15)
            if inspected.returncode != 0:
                raise StorageHostUnqualified("Native creation identity inspection is unavailable")
            payload = json.loads(inspected.stdout, object_pairs_hook=_unique_object)
            if not isinstance(payload, list) or len(payload) != 1 or not isinstance(payload[0], dict):
                raise StorageHostUnqualified("Native creation identity inspection is ambiguous")
            entry = payload[0]
            config = entry.get("Config")
            actual_labels = config.get("Labels") if isinstance(config, dict) else None
            expected_labels = {**labels, ATTEMPT_LABEL: handoff.attempt.id}
            if (
                entry.get("Id") != container_id
                or entry.get("Name") != "/" + name
                or not isinstance(actual_labels, dict)
                or any(actual_labels.get(key) != value for key, value in expected_labels.items())
                or {key: value for key, value in actual_labels.items() if key.startswith(_SLOT_PREFIX)} != {key: value for key, value in labels.items() if key.startswith(_SLOT_PREFIX)}
            ):
                raise StorageHostUnqualified("Native returned container differs from its exact request labels or identity")
            self.assert_current()
            identity = NativeContainerIdentity(handoff.attempt.id, self.evidence.daemon_id, owner, thread, name, slots, container_id)
            # Retain identity before durable acknowledgment can disappear and
            # before an awaiting coroutine can discard its worker's result.
            self.handoffs.record_verified(handoff, identity)
            self.evidence.acknowledge(handoff.attempt, identity)
            self.handoffs.record_acknowledged(handoff)
            return container_id
        except Exception as exc:
            self._check_failure_evidence()
            raise NativeCreateUnconfirmed(handoff) from exc
