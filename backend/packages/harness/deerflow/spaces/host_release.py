"""Actorless reclamation, authorized by immutable deletion and durable progress."""

import logging
from copy import deepcopy
from types import SimpleNamespace
from uuid import uuid4

from sqlalchemy import select

from deerflow.persistence.spaces.files import SpaceBackingRow, SpaceFileOperationRow
from deerflow.persistence.spaces.model import SpaceEventRow, SpaceRow
from deerflow.spaces.attachments import SpaceAttachments
from deerflow.spaces.backings import BackingUnavailable
from deerflow.spaces.release_filesystem import ReleaseFilesystem, ReleaseParentUnavailable, _digest
from deerflow.spaces.service import SpaceOperationPending
from deerflow.utils.file_io import await_drained, run_file_io

_BINDING = ("backing_handle", "space_id", "slot_id", "filesystem_uuid", "max_bytes", "max_inodes", "root_inode", "control_inode")
_WRITER = "host:space-release"
_REFUSAL = {"complete": False, "slot_released": False}
logger = logging.getLogger(__name__)


async def _snapshot(session, row, admission):
    if row is None or row.status != "deleted" or row.mode != "native":
        return None
    event = await session.scalar(select(SpaceEventRow).where(SpaceEventRow.space_id == row.id, SpaceEventRow.action == "delete").order_by(SpaceEventRow.generation.desc()).limit(1))
    if event is None or not isinstance(event.details, dict) or event.details.get("retained_backups") is not False:
        return None
    if await SpaceAttachments._current(session, [row.id], lock=True):
        return None
    if admission is not None:
        await admission(session)
    binding = await session.get(SpaceBackingRow, row.backing_handle)
    if binding is not None and binding.space_id != row.id:
        raise SpaceOperationPending("Deleted resource binding is inconsistent")
    request = {
        "schema_version": 1,
        "action": "host-release",
        "space_id": row.id,
        "generation": row.generation,
        "binding": {name: getattr(binding, name) for name in _BINDING} if binding is not None else None,
        "delete": {"generation": event.generation, "actor_kind": event.actor_kind, "actor_id": event.actor_id, "details": deepcopy(event.details)},
    }
    return request, binding


async def _release_ledgers(session, space_id, *, pending=False):
    """Retain recognizable progress even when its request field is damaged."""
    query = select(SpaceFileOperationRow).where(SpaceFileOperationRow.space_id == space_id)
    if pending:
        query = query.where(SpaceFileOperationRow.phase == "pending")
    rows = (await session.scalars(query)).all()
    selected = []
    for row in rows:
        request = row.request if isinstance(row.request, dict) else {}
        result = row.result if isinstance(row.result, dict) else {}
        if request.get("action") in {"host-release", "release"} or (row.actor_kind, row.actor_id) == ("nonhuman", _WRITER) or "release" in result:
            selected.append(row)
    return selected


async def _intent(session, space_id):
    rows = await _release_ledgers(session, space_id)
    if len(rows) > 1:
        raise SpaceOperationPending("Conflicting host release intents require inspection")
    if not rows:
        return None
    row = rows[0]
    if (row.actor_kind, row.actor_id, row.phase) != ("nonhuman", _WRITER, "pending"):
        raise SpaceOperationPending("Unexpected host release outcome requires inspection")
    return row


async def _current(session, rows, request, admission):
    snapshot = await _snapshot(session, rows.get(request["space_id"]), admission)
    if snapshot is None:
        raise SpaceOperationPending("Release eligibility changed; retain the binding")
    current, binding = snapshot
    expected = {name: value for name, value in request.items() if name != "physical"}
    if _digest(current) != _digest(expected) or binding is None:
        raise SpaceOperationPending("Release authority or binding changed; retain recovery evidence")
    return binding


def _validate_intent(operation, snapshot):
    request = operation.request
    if not isinstance(request, dict) or set(request) != set(snapshot) | {"physical"} or type(request.get("schema_version")) is not int or request.get("schema_version") != 1:
        raise SpaceOperationPending("Release intent is malformed")
    if _digest({name: value for name, value in request.items() if name != "physical"}) != _digest(snapshot):
        raise SpaceOperationPending("Release intent no longer matches its authority")
    physical = request["physical"]
    if not isinstance(physical, dict) or set(physical) != {"parent", "roots"} or not isinstance(physical["roots"], dict) or set(physical["roots"]) != {"data", "control"}:
        raise SpaceOperationPending("Release physical snapshot is malformed")
    for facts in (physical["parent"], *physical["roots"].values()):
        if not isinstance(facts, dict) or set(facts) != {"device", "inode", "uid", "gid", "mode"} or any(type(value) is not int or value < 0 for value in facts.values()) or facts["inode"] <= 0:
            raise SpaceOperationPending("Release physical identity is malformed")
    for name, key in (("data", "root_inode"), ("control", "control_inode")):
        if physical["roots"][name]["inode"] != snapshot["binding"][key] or physical["roots"][name]["device"] != physical["parent"]["device"]:
            raise SpaceOperationPending("Release physical identity differs from its binding")
    result = operation.result
    if not isinstance(result, dict) or type(result.get("schema_version")) is not int or result.get("schema_version") != 1 or result.get("stage") not in ("intent", "filesystem-complete"):
        raise SpaceOperationPending("Release checkpoint is malformed")
    if result["stage"] == "intent":
        if set(result) != {"schema_version", "stage"}:
            raise SpaceOperationPending("Initial release checkpoint is malformed")
        return request, None
    if (
        set(result) != {"schema_version", "stage", "parent", "roots", "journal", "request_sha256"}
        or result["parent"] != physical["parent"]
        or result["request_sha256"] != _digest(request)
        or not isinstance(result["roots"], dict)
        or set(result["roots"]) != {"data", "control"}
    ):
        raise SpaceOperationPending("Completed release checkpoint is malformed")
    for name, facts in result["roots"].items():
        old = physical["roots"][name]
        if (
            not isinstance(facts, dict)
            or set(facts) != set(old)
            or any(type(value) is not int for value in facts.values())
            or facts["inode"] <= 0
            or facts["inode"] == old["inode"]
            or any(facts[key] != old[key] for key in ("device", "uid", "gid", "mode"))
        ):
            raise SpaceOperationPending("Completed replacement identity is malformed")
    journal = result["journal"]
    if (
        not isinstance(journal, dict)
        or set(journal) != set(physical["parent"])
        or any(type(value) is not int for value in journal.values())
        or journal["inode"] <= 0
        or journal["device"] != physical["parent"]["device"]
        or journal["mode"] != 0o700
    ):
        raise SpaceOperationPending("Completed journal identity is malformed")
    return request, result


async def _discard(session, space_id, operation_id=None):
    operations = (await session.scalars(select(SpaceFileOperationRow).where(SpaceFileOperationRow.space_id == space_id, SpaceFileOperationRow.phase == "pending"))).all()
    for operation in operations:
        if operation.operation_id != operation_id:
            operation.phase = "failed"
            operation.result = {"resolution": "host-discarded"}


class _ReleaseComplete(Exception):
    def __init__(self, answer, *, binding=None):
        self.answer, self.binding = answer, binding


class _HostReleasePolicy:
    def __init__(self, recovery, space_id, *, admission, lock_admission):
        self.recovery, self.space_id = recovery, space_id
        self.admission, self.lock_admission = admission, lock_admission

    def locked(self):
        return self.recovery.registry.host_locked([self.space_id], _before_lock=self.lock_admission)

    async def snapshot(self, session, row):
        return await _snapshot(session, row, self.admission)

    async def current(self, session, rows, request):
        return await _current(session, rows, request, self.admission)

    async def intent(self, session, space_id):
        return await _intent(session, space_id)

    @staticmethod
    def validate(operation, initial):
        return _validate_intent(operation, initial)

    @staticmethod
    def new_id():
        return uuid4().hex

    @staticmethod
    def create(session, space_id, operation_id, initial, request):
        session.add(
            SpaceFileOperationRow(space_id=space_id, operation_id=operation_id, actor_kind="nonhuman", actor_id=_WRITER, generation=initial["generation"], phase="pending", request=request, result={"schema_version": 1, "stage": "intent"})
        )

    @staticmethod
    def checkpoint(operation, checkpoint):
        operation.result = checkpoint

    @staticmethod
    def finish(operation, checkpoint):
        operation.phase = "complete"
        operation.result = {**checkpoint, "outcome": "reclaimed"}

    @staticmethod
    def outcome(initial, *, released, newly):
        return {"complete": True, "slot_released": released, "newly_reclaimed": newly, "cause": initial["delete"]["details"].get("cause")}

    async def absent(self, session, initial):
        answer = self.outcome(initial, released=True, newly=False)
        ledgers = await _release_ledgers(session, self.space_id)
        if len(ledgers) != 1:
            return answer
        operation = ledgers[0]
        if (operation.actor_kind, operation.actor_id, operation.phase) != ("nonhuman", _WRITER, "complete"):
            return answer
        if operation.generation != initial["generation"]:
            raise SpaceOperationPending("Completed host release generation changed")
        self.recovery.files._operation_id(operation.operation_id)
        request, result = operation.request, operation.result
        if not isinstance(request, dict) or not isinstance(result, dict) or result.get("outcome") != "reclaimed":
            raise SpaceOperationPending("Completed host release proof is malformed")
        binding = request.get("binding")
        row = await session.get(SpaceRow, self.space_id)
        if not isinstance(binding, dict) or set(binding) != set(_BINDING) or binding["space_id"] != row.id or binding["backing_handle"] != row.backing_handle:
            raise SpaceOperationPending("Completed host release binding changed")
        _request, checkpoint = _validate_intent(
            SimpleNamespace(request=request, result={key: value for key, value in result.items() if key != "outcome"}),
            {**initial, "binding": binding},
        )
        if checkpoint is None:
            raise SpaceOperationPending("Completed host release lacks a physical checkpoint")
        if await session.scalar(select(SpaceBackingRow.backing_handle).where(SpaceBackingRow.slot_id == binding["slot_id"])) is None:
            raise _ReleaseComplete(answer, binding=binding)
        return answer

    def forgotten(self, session, initial):
        pass

    def own_workflow(self, owner):
        return None

    @staticmethod
    def reset_workflow(token):
        if token is not None:
            from deerflow.spaces.workflows import _owner

            _owner.reset(token)

    @staticmethod
    def raise_error(error):
        raise SpaceOperationPending("Release is incomplete; repeat it") from error


async def host_release(recovery, space_id, *, admission=None, publication=None, lock_admission=None, retain_unverified=False, before_publication=None):
    policy = _HostReleasePolicy(recovery, space_id, admission=admission, lock_admission=lock_admission)
    return await _release(recovery, space_id, policy, publication=publication, retain_unverified=retain_unverified, before_publication=before_publication)


async def _release(recovery, space_id, policy, *, publication=None, retain_unverified=False, before_publication=None):
    """One drained physical engine; each policy supplies fresh SQL authority."""
    files = recovery.files

    async def perform():
        claims = files._claim_snapshot()
        owner = None
        workflow = None
        try:
            async with policy.locked() as (session, rows):
                snapshot = await policy.snapshot(session, rows.get(space_id))
                if snapshot is None:
                    return dict(_REFUSAL)
                initial, binding = snapshot
                if binding is None:
                    return await policy.absent(session, initial)
                operation = await policy.intent(session, space_id)
                if operation is not None:
                    request, checkpoint = policy.validate(operation, initial)
                    operation_id = operation.operation_id
                    files._operation_id(operation_id)
                else:
                    request = checkpoint = operation_id = None
            slot = binding.slot_id
            expected = files._binding_claim(claims, slot)
            try:
                files._binding_spec(binding)
                # After rotation only exact journal/checkpoint root placement is
                # relevant; ordinary bound admission must continue to reject it.
                volume = await run_file_io(files.catalog.verify, slot)
                owner = await run_file_io(ReleaseFilesystem, volume)
                if request is None:
                    if await run_file_io(owner.foreign_journals):
                        raise SpaceOperationPending("Unresolved sibling release evidence requires inspection")
                    files._check_incarnation(binding, volume)
                    physical = await run_file_io(owner.original_facts, binding.backing_handle)
                    if (physical["roots"]["data"]["inode"], physical["roots"]["control"]["inode"]) != (binding.root_inode, binding.control_inode):
                        raise BackingUnavailable("Original roots changed before release intent")
            except ReleaseParentUnavailable as exc:
                raise SpaceOperationPending("Provider parent must be repaired before release") from exc
            except (BackingUnavailable, OSError):
                if request is not None:
                    raise SpaceOperationPending("Release physical proof is unavailable; retain its intent") from None
                if retain_unverified:
                    return dict(_REFUSAL)
                # Effect-free branch is selected before creating any release
                # intent. The physical slot stays uncertain and is not counted.
                async with policy.locked() as (session, rows):
                    current = await policy.current(session, rows, initial)
                    if await policy.intent(session, space_id) is not None:
                        raise SpaceOperationPending("Release progress appeared during verification")
                    if publication is not None:
                        await publication(session)
                    await _discard(session, space_id)
                    policy.forgotten(session, initial)
                    await session.delete(current)
                kind = getattr(files.catalog.volumes.get(slot), "kind", None)
                logger.warning("storage: space %s (%s) released without emptying (unverified); run storage-init forget %s --discard to reclaim it", slot, kind, slot)
                if expected is not None:
                    files._publish_claim(slot, expected, binding.backing_handle, "unclaimed")
                return policy.outcome(initial, released=False, newly=False)
            if request is None:
                async with policy.locked() as (session, rows):
                    await policy.current(session, rows, initial)
                    operation = await policy.intent(session, space_id)
                    if operation is not None:
                        request, checkpoint = policy.validate(operation, initial)
                        operation_id = operation.operation_id
                    else:
                        operation_id = policy.new_id()
                        request = {**initial, "physical": physical}
                        policy.create(session, space_id, operation_id, initial, request)
            files._operation_id(operation_id)
            await run_file_io(owner.configure, request, operation_id)
            if await run_file_io(owner.foreign_journals):
                raise SpaceOperationPending("Conflicting sibling release evidence requires inspection")
            if checkpoint is None:
                await run_file_io(owner.acquire_existing)
                workflow = policy.own_workflow(owner)
                async with policy.locked() as (session, rows):
                    await policy.current(session, rows, request)
                    operation = await policy.intent(session, space_id)
                    policy.validate(operation, initial)
                    # Full-slot bootstrap holds SQL ownership until durable
                    # journal ownership exists; independent retries cannot race.
                    await run_file_io(owner.initialize)
                    if workflow is None:
                        workflow = policy.own_workflow(owner)
                for name in ("data", "control"):
                    async with policy.locked() as (session, rows):
                        await policy.current(session, rows, request)
                        await run_file_io(owner.rotate, name)
                for name in ("data", "control"):
                    async with policy.locked() as (session, rows):
                        await policy.current(session, rows, request)
                        await run_file_io(owner.purge_retired, name)
                async with policy.locked() as (session, rows):
                    await policy.current(session, rows, request)
                    operation = await policy.intent(session, space_id)
                    policy.validate(operation, initial)
                    checkpoint = await run_file_io(owner.checkpoint)
                    policy.checkpoint(operation, checkpoint)
                # Confirmed checkpoint precedes every journal-removal effect.
            if before_publication is not None:
                # Includes checkpoint retries. External containment proof must
                # finish before borrowing the final SQL writer/parent locks.
                await before_publication()
            async with policy.locked() as (session, rows):
                current = await policy.current(session, rows, request)
                operation = await policy.intent(session, space_id)
                _request, saved = policy.validate(operation, initial)
                if saved != checkpoint:
                    raise SpaceOperationPending("Release checkpoint changed before publication")
                await run_file_io(owner.finish, checkpoint)
                if publication is not None:
                    await publication(session)
                await _discard(session, space_id, operation_id)
                await session.delete(current)
                policy.finish(operation, checkpoint)
            if expected is not None:
                files._publish_claim(slot, expected, None, "free")
            return policy.outcome(initial, released=True, newly=True)
        except _ReleaseComplete as completed:
            if completed.binding is not None:
                binding = completed.binding
                slot = binding["slot_id"]
                expected = files._binding_claim(claims, slot)
                if expected is not None and (expected.handle, expected.phase) == (binding["backing_handle"], "bound"):
                    released = completed.answer["slot_released"]
                    files._publish_claim(slot, expected, None if released else binding["backing_handle"], "free" if released else "unclaimed")
            return completed.answer
        except SpaceOperationPending:
            raise
        except Exception as exc:
            policy.raise_error(exc)
        finally:
            try:
                if owner is not None:
                    await run_file_io(owner.close)
            finally:
                policy.reset_workflow(workflow)

    return await await_drained(perform())
