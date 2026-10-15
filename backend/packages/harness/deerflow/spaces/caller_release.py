"""Explicit granted release uses the same drained physical engine as the host."""

from contextlib import asynccontextmanager
from types import SimpleNamespace

from sqlalchemy import select

from deerflow.persistence.spaces.files import SpaceBackingRow, SpaceFileOperationRow
from deerflow.spaces.contract import InvalidPrincipal, SpaceConflict, SpaceDenied
from deerflow.spaces.host_release import _BINDING, _HostReleasePolicy, _release, _release_ledgers, _ReleaseComplete, _validate_intent
from deerflow.spaces.release_filesystem import _digest
from deerflow.spaces.service import SpaceOperationPending


class _GrantedReleasePolicy(_HostReleasePolicy):
    def __init__(self, recovery, actor, space_id, generation, operation_id):
        super().__init__(recovery, space_id, admission=None, lock_admission=None)
        self.files, self.actor, self.generation, self.operation_id = recovery.files, actor, generation, operation_id

    @asynccontextmanager
    async def locked(self):
        async with self.recovery.registry.admitted_deleted(actor=self.actor, space_id=self.space_id, operation_id=(self.space_id, self.operation_id)) as (session, row, _grant):
            if row.mode != "native":
                raise SpaceDenied("A mediated resource requires its bound recovery controller")
            yield session, {self.space_id: row}

    def initial(self, row, binding):
        return {
            "schema_version": 1,
            "action": "caller-release",
            "space_id": row.id,
            "generation": self.generation,
            "actor_kind": self.actor.kind,
            "actor_id": self.actor.subject_id,
            "operation_id": self.operation_id,
            "binding": {name: getattr(binding, name) for name in _BINDING} if binding is not None else None,
        }

    @staticmethod
    def envelope(operation):
        result = operation.result
        if not isinstance(result, dict) or set(result) != {"schema_version", "release", "progress", "value"} or type(result["schema_version"]) is not int or result["schema_version"] != 1 or not isinstance(result["release"], dict):
            raise SpaceOperationPending("Release receipt is malformed")
        return result

    async def intent(self, session, space_id):
        operation = await session.get(SpaceFileOperationRow, (space_id, self.operation_id), populate_existing=True)
        if operation is not None:
            self.files._same_operation(operation, self.actor, self.generation, {"action": "release"})
            if operation.phase not in {"pending", "complete"}:
                raise SpaceConflict("The operation ID already identifies a different request")
        pending = await _release_ledgers(session, space_id, pending=True)
        for other in pending:
            if other.operation_id == self.operation_id:
                continue
            raise SpaceOperationPending("A different release owns this Space")
        return operation

    def replay(self, operation, row, binding):
        envelope = self.envelope(operation)
        value, proof = envelope["value"], envelope["release"]
        if not isinstance(value, dict) or set(value) != {"complete", "slot_released"} or value["complete"] is not True or type(value["slot_released"]) is not bool or binding is not None:
            raise SpaceOperationPending("Completed release outcome is inconsistent")
        initial = self.initial(row, None)
        physical = "physical" in proof
        if set(proof) != set(initial) | ({"physical"} if physical else set()) or {key: val for key, val in proof.items() if key not in {"binding", "physical"}} != {key: val for key, val in initial.items() if key != "binding"}:
            raise SpaceOperationPending("Completed release identity changed")
        original = proof["binding"]
        if original is not None and (not isinstance(original, dict) or set(original) != set(_BINDING) or original["backing_handle"] != row.backing_handle or original["space_id"] != row.id):
            raise SpaceOperationPending("Completed release binding changed")
        if physical:
            if not value["slot_released"]:
                raise SpaceOperationPending("Completed physical release has no confirmed outcome")
            _request, checkpoint = _validate_intent(SimpleNamespace(request=proof, result=envelope["progress"]), {key: val for key, val in proof.items() if key != "physical"})
            if checkpoint is None:
                raise SpaceOperationPending("Completed release lacks its physical checkpoint")
        elif envelope["progress"] is not None or (original is None) is not value["slot_released"]:
            raise SpaceOperationPending("Completed effect-free release is inconsistent")
        return dict(value)

    async def snapshot(self, session, row):
        operation = await self.intent(session, row.id)
        if row.generation != self.generation:
            raise SpaceConflict("Stale space generation")
        binding = await session.get(SpaceBackingRow, row.backing_handle)
        if binding is not None and binding.space_id != row.id:
            raise SpaceOperationPending("Deleted resource binding is inconsistent")
        if operation is not None and operation.phase == "complete":
            answer = self.replay(operation, row, binding)
            original = self.envelope(operation)["release"]["binding"]
            if original is not None and await session.scalar(select(SpaceBackingRow.backing_handle).where(SpaceBackingRow.slot_id == original["slot_id"])) is not None:
                original = None
            raise _ReleaseComplete(answer, binding=original)
        return self.initial(row, binding), binding

    async def current(self, session, rows, request):
        initial, binding = await self.snapshot(session, rows[self.space_id])
        if binding is None or _digest(initial) != _digest({key: value for key, value in request.items() if key != "physical"}):
            raise SpaceOperationPending("Release authority or binding changed; retain recovery evidence")
        return binding

    def validate(self, operation, initial):
        envelope = self.envelope(operation)
        if operation.phase != "pending" or envelope["value"] is not None:
            raise SpaceOperationPending("Release intent has an unexpected outcome")
        return _validate_intent(SimpleNamespace(request=envelope["release"], result=envelope["progress"]), initial)

    def new_id(self):
        return self.operation_id

    def record(self, session, *, release, progress, value, phase):
        operation = SpaceFileOperationRow(
            space_id=self.space_id,
            operation_id=self.operation_id,
            actor_kind=self.actor.kind,
            actor_id=self.actor.subject_id,
            generation=self.generation,
            phase=phase,
            request={"action": "release"},
            result={"schema_version": 1, "release": release, "progress": progress, "value": value},
        )
        session.add(operation)

    def create(self, session, space_id, operation_id, initial, request):
        self.record(session, release=request, progress={"schema_version": 1, "stage": "intent"}, value=None, phase="pending")

    def checkpoint(self, operation, checkpoint):
        operation.result = {**self.envelope(operation), "progress": checkpoint}

    def finish(self, operation, checkpoint):
        operation.result = {**self.envelope(operation), "progress": checkpoint, "value": {"complete": True, "slot_released": True}}
        operation.phase = "complete"

    @staticmethod
    def outcome(initial, *, released, newly):
        return {"complete": True, "slot_released": released}

    async def absent(self, session, initial):
        result = self.outcome(initial, released=True, newly=False)
        self.record(session, release=initial, progress=None, value=result, phase="complete")
        return result

    def forgotten(self, session, initial):
        self.record(session, release=initial, progress=None, value=self.outcome(initial, released=False, newly=False), phase="complete")

    def own_workflow(self, owner):
        if owner.workflow is None:
            return None
        from deerflow.spaces.workflows import _owner

        previous = _owner.get()
        owned = dict(previous[1]) if previous is not None and previous[0] is self.files else {}
        owned[self.space_id] = owner
        return _owner.set((self.files, owned))

    @staticmethod
    def raise_error(error):
        if isinstance(error, SpaceConflict | SpaceDenied | InvalidPrincipal):
            raise error
        raise SpaceOperationPending("Release is incomplete; repeat it") from error


async def release(recovery, *, actor, space_id, expected_generation, operation_id):
    policy = _GrantedReleasePolicy(recovery, actor, space_id, expected_generation, operation_id)
    return await _release(recovery, space_id, policy)
