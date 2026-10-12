"""Actorless reclamation, authorized by immutable deletion and durable progress."""

from copy import deepcopy
from uuid import uuid4

from sqlalchemy import and_, or_, select

from deerflow.persistence.spaces.files import SpaceBackingRow, SpaceFileOperationRow
from deerflow.persistence.spaces.model import SpaceEventRow
from deerflow.spaces.attachments import SpaceAttachments
from deerflow.spaces.backings import BackingUnavailable
from deerflow.spaces.release_filesystem import ReleaseFilesystem, ReleaseParentUnavailable, _digest
from deerflow.spaces.service import SpaceOperationPending
from deerflow.utils.file_io import await_drained, run_file_io

_BINDING = ("backing_handle", "space_id", "slot_id", "filesystem_uuid", "max_bytes", "max_inodes", "root_inode", "control_inode")
_WRITER = "host:space-release"
_REFUSAL = {"complete": False, "slot_released": False}


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


async def _intent(session, space_id):
    rows = (
        await session.scalars(
            select(SpaceFileOperationRow).where(
                SpaceFileOperationRow.space_id == space_id, or_(SpaceFileOperationRow.request["action"].as_string() == "host-release", and_(SpaceFileOperationRow.actor_kind == "nonhuman", SpaceFileOperationRow.actor_id == _WRITER))
            )
        )
    ).all()
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


async def host_release(recovery, space_id, *, admission=None, publication=None, lock_admission=None):
    files, registry = recovery.files, recovery.registry

    async def perform():
        claims = files._claim_snapshot()
        owner = None
        try:
            async with registry.host_locked([space_id], _before_lock=lock_admission) as (session, rows):
                snapshot = await _snapshot(session, rows.get(space_id), admission)
                if snapshot is None:
                    return dict(_REFUSAL)
                initial, binding = snapshot
                cause = initial["delete"]["details"].get("cause")
                if binding is None:
                    return {"complete": True, "slot_released": True, "newly_reclaimed": False, "cause": cause}
                operation = await _intent(session, space_id)
                if operation is not None:
                    request, checkpoint = _validate_intent(operation, initial)
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
                # Effect-free branch is selected before creating any release
                # intent. The physical slot stays uncertain and is not counted.
                async with registry.host_locked([space_id], _before_lock=lock_admission) as (session, rows):
                    current = await _current(session, rows, initial, admission)
                    if await _intent(session, space_id) is not None:
                        raise SpaceOperationPending("Release progress appeared during verification")
                    if publication is not None:
                        await publication(session)
                    await _discard(session, space_id)
                    await session.delete(current)
                if expected is not None:
                    files._publish_claim(slot, expected, binding.backing_handle, "unclaimed")
                return {"complete": True, "slot_released": False, "newly_reclaimed": False, "cause": cause}
            if request is None:
                async with registry.host_locked([space_id], _before_lock=lock_admission) as (session, rows):
                    await _current(session, rows, initial, admission)
                    operation = await _intent(session, space_id)
                    if operation is not None:
                        request, checkpoint = _validate_intent(operation, initial)
                        operation_id = operation.operation_id
                    else:
                        operation_id = uuid4().hex
                        request = {**initial, "physical": physical}
                        session.add(
                            SpaceFileOperationRow(
                                space_id=space_id, operation_id=operation_id, actor_kind="nonhuman", actor_id=_WRITER, generation=initial["generation"], phase="pending", request=request, result={"schema_version": 1, "stage": "intent"}
                            )
                        )
            files._operation_id(operation_id)
            await run_file_io(owner.configure, request, operation_id)
            if await run_file_io(owner.foreign_journals):
                raise SpaceOperationPending("Conflicting sibling release evidence requires inspection")
            if checkpoint is None:
                await run_file_io(owner.acquire_existing)
                async with registry.host_locked([space_id], _before_lock=lock_admission) as (session, rows):
                    await _current(session, rows, request, admission)
                    operation = await _intent(session, space_id)
                    _validate_intent(operation, initial)
                    # Full-slot bootstrap holds SQL ownership until durable
                    # journal ownership exists; independent retries cannot race.
                    await run_file_io(owner.initialize)
                for name in ("data", "control"):
                    async with registry.host_locked([space_id], _before_lock=lock_admission) as (session, rows):
                        await _current(session, rows, request, admission)
                        await run_file_io(owner.rotate, name)
                for name in ("data", "control"):
                    async with registry.host_locked([space_id], _before_lock=lock_admission) as (session, rows):
                        await _current(session, rows, request, admission)
                        await run_file_io(owner.purge_retired, name)
                async with registry.host_locked([space_id], _before_lock=lock_admission) as (session, rows):
                    await _current(session, rows, request, admission)
                    operation = await _intent(session, space_id)
                    _validate_intent(operation, initial)
                    checkpoint = await run_file_io(owner.checkpoint)
                    operation.result = checkpoint
                # Confirmed checkpoint precedes every journal-removal effect.
            async with registry.host_locked([space_id], _before_lock=lock_admission) as (session, rows):
                current = await _current(session, rows, request, admission)
                operation = await _intent(session, space_id)
                _request, saved = _validate_intent(operation, initial)
                if saved != checkpoint:
                    raise SpaceOperationPending("Release checkpoint changed before publication")
                await run_file_io(owner.finish, checkpoint)
                if publication is not None:
                    await publication(session)
                await _discard(session, space_id, operation_id)
                await session.delete(current)
                operation.phase = "complete"
                operation.result = {**checkpoint, "outcome": "reclaimed"}
            if expected is not None:
                files._publish_claim(slot, expected, None, "free")
            return {"complete": True, "slot_released": True, "newly_reclaimed": True, "cause": cause}
        except SpaceOperationPending:
            raise
        except Exception as exc:
            raise SpaceOperationPending("Release is incomplete; repeat it") from exc
        finally:
            if owner is not None:
                await run_file_io(owner.close)

    return await await_drained(perform())
