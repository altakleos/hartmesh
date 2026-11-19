"""Offline compatibility check for reusing Home files and SQL on an older build."""

import fcntl
import os
from contextlib import ExitStack, asynccontextmanager
from pathlib import Path

from sqlalchemy import func, select, text

from app.gateway.agent_operator_contract import CommandRefused
from app.gateway.agent_operator_pending import PAGE_SIZE, pending_count, pending_page
from deerflow.features.resources import StorageFeatureLinkRow
from deerflow.files.store import open_directory_source
from deerflow.persistence.agent_instances.model import AgentInstanceRow
from deerflow.persistence.run.model import RunRow
from deerflow.persistence.spaces.files import SpaceBackingRow, SpaceFileOperationRow
from deerflow.persistence.spaces.lifecycle import SpaceAttachmentRow, SpaceMountRow
from deerflow.persistence.spaces.model import SpaceRow
from deerflow.spaces.attachments import SpaceAttachments
from deerflow.spaces.incarnations import verify_bound
from deerflow.spaces.native_create_evidence import NativeEvidenceReader
from deerflow.spaces.native_resident_evidence import NativeResidentRoot
from deerflow.spaces.runtime_owner import PoolRuntimeOwner
from deerflow.spaces.workflows import WorkflowLockMissing, acquire, close
from deerflow.utils.file_io import await_drained, run_file_io


def _operator_home_current(home, descriptor):
    current = open_directory_source(home)
    try:
        if not os.path.samestat(os.fstat(descriptor), os.fstat(current)):
            raise CommandRefused("The operator Home directory changed during verification")
    finally:
        os.close(current)


@asynccontextmanager
async def operator_exclusion(home, *, exclusive):
    """Mutating one-shot commands share the lock; final verification owns it."""
    descriptor = await run_file_io(open_directory_source, home)
    try:
        try:
            await run_file_io(fcntl.flock, descriptor, (fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH) | fcntl.LOCK_NB)
        except BlockingIOError:
            raise CommandRefused("Another operator command or rollback check is active") from None
        yield
        await run_file_io(_operator_home_current, home, descriptor)
    finally:
        await run_file_io(os.close, descriptor)


async def under_operator_lock(home, action, *, exclusive=False):
    async def execute():
        async with operator_exclusion(home, exclusive=exclusive):
            return await action()

    return await await_drained(execute())


async def _exclude_writers(session):
    if session.bind.dialect.name == "sqlite":
        await session.execute(text("BEGIN IMMEDIATE"))
    elif session.bind.dialect.name == "postgresql":
        # Row locks alone miss operations inserted for previously quiet Homes.
        models = (SpaceRow, SpaceBackingRow, SpaceFileOperationRow, SpaceAttachmentRow, SpaceMountRow, StorageFeatureLinkRow, AgentInstanceRow, RunRow)
        tables = ", ".join(model.__tablename__ for model in models)
        await session.execute(text(f"LOCK TABLE {tables} IN EXCLUSIVE MODE NOWAIT"))
    else:
        raise CommandRefused("The database cannot exclude writers during rollback verification")


def _check_roots(claim, record, bindings, owners, seen_homes, readonly):
    if len(claim.resources) != 1:
        raise CommandRefused(f"Workspace {claim.id} has ambiguous Home mounts")
    home = claim.resources[0]
    binding = bindings.get(home.space_id)
    if home.space_id in seen_homes or claim.actor.kind != "nonhuman" or owners.get(home.space_id) != claim.actor.subject_id or home.alias != "home" or not home.writable or binding is None or binding[0] != home.generation:
        raise CommandRefused(f"Workspace {claim.id} cannot be associated with one active Home and owner")
    seen_homes.add(home.space_id)
    bound = binding[1]
    observed_home = verify_bound(bound.physical, bound.backing_handle, managed_home=True)
    current = (NativeResidentRoot.from_view(home, observed_home), *(NativeResidentRoot.from_readonly(root) for root in readonly))
    if record.roots != current:
        raise CommandRefused(f"Workspace {claim.id} does not match its current storage roots")


def _check_host(files, adapter, pool, claims, bindings, owners, readonly):
    reader = NativeEvidenceReader(pool, daemon_id=adapter.host_id)
    try:
        snapshot = reader.snapshot()
        if snapshot.pending_creates(adapter.host_id):
            raise CommandRefused("The native journal still has unconfirmed workspace starts")
        actuals = {item.attachment_id: item for item in adapter.bootstrap_residents(known_slots=tuple(files.catalog.volumes))}
        records = {record.attachment_id: record for record in snapshot.residents}
        if len(records) != len(snapshot.residents):
            raise CommandRefused("The native journal repeats a workspace attachment ID")
        retained = {claim.id for claim in claims}
        mismatches = sorted((actuals.keys() | records.keys() | retained) - (actuals.keys() & records.keys() & retained))
        if mismatches:
            raise CommandRefused(
                "Docker, the native journal and the database disagree on the retained workspaces. Keep writers paused and reconcile on the current Gateway before repeating the offline check.",
                workspace_mismatch_count=len(mismatches),
                workspace_mismatches=[{"attachment_id": key, "in_docker": key in actuals, "in_native_journal": key in records, "in_retained_sql": key in retained} for key in mismatches[:PAGE_SIZE]],
            )
        seen_homes = set()
        for claim in claims:
            actual, record = actuals[claim.id], records[claim.id]
            resources = tuple(sorted((r.space_id, r.generation, r.alias, r.writable) for r in claim.resources))
            if (
                claim.phase != "active"
                or record.phase != "prepared"
                or record.correlation != (claim.id, claim.container_id, claim.host_id, claim.incarnation, resources)
                or (actual.container_id, actual.host_id, actual.created_at, actual.slots, actual.mounts) != (record.container_id, record.daemon_id, record.created_at, record.slots, record.mounts)
                or actual.name != record.name
                or frozenset(actual.resources) != frozenset(record.resources)
            ):
                raise CommandRefused(f"Workspace {claim.id} has an unresolved identity or closure")
            _check_roots(claim, record, bindings, owners, seen_homes, readonly)
        reader.assert_current()
        adapter._same_host()
    finally:
        reader.close()


def _drain_files(files, bindings, managed_homes, stack):
    from app.gateway.agent_operator_services import inspect_operator_volume

    current = {}
    for binding, generation in bindings:
        volume, _, _ = inspect_operator_volume(files, binding, managed_home=binding.space_id in managed_homes)
        current[binding.space_id] = (generation, volume)
        try:
            descriptors = acquire({binding.space_id: volume}, create=False)
        except WorkflowLockMissing:
            # Creating a workflow lock first requires the excluded SQL writer.
            continue
        stack.callback(close, descriptors)
    return current


async def _scan(command, session):
    checked = pages = blocked = 0
    cursor = None
    blockers = []
    total = await pending_count(session)
    while True:
        page, next_cursor = await pending_page(session, cursor)
        pages += 1
        for row in page:
            key = row["space_id"] + ":" + row["operation_id"]
            if cursor is not None and key <= cursor:
                raise CommandRefused("The pending inventory repeated or reordered an operation")
            cursor = key
            checked += 1
            if row["instance_id"] is not None and row["attachments"]:
                blocked += 1
                if len(blockers) < PAGE_SIZE:
                    blockers.append(row)
        if next_cursor is None:
            break
        if not page or next_cursor != cursor:
            raise CommandRefused("The pending inventory returned an incomplete page")
    if checked != total or total != await pending_count(session):
        raise CommandRefused("The pending inventory count changed or a page is missing")
    note = f"{checked} pending operations checked; {blocked} still have a retained Home workspace"
    return command._answer("check-rollback", note, returncode=2 if blocked else 0, total=total, checked=checked, pages=pages, blocked=blocked, blockers=blockers)


async def check_rollback(command):
    from app.gateway.storage_spaces import storage_readonly_roots

    if command.storage_reason:
        raise CommandRefused(f"Storage inventory is unavailable: {command.storage_reason}")
    await command._require_daemon()
    pool = Path(command.config.storage_spaces.inventory_path).parent
    owner = PoolRuntimeOwner(pool)
    held = ExitStack()
    try:
        await run_file_io(owner.acquire)
        async with command.files.registry._sf.begin() as session:
            await _exclude_writers(session)
            live = await session.scalar(select(func.count()).select_from(RunRow).where(RunRow.status.in_(("pending", "running"))))
            if live:
                raise CommandRefused(f"{live} runs still have an unfinished admission")
            bindings = (await session.execute(select(SpaceBackingRow, SpaceRow.generation).join(SpaceRow, SpaceRow.id == SpaceBackingRow.space_id).order_by(SpaceBackingRow.space_id))).all()
            homes = (await session.execute(select(AgentInstanceRow.home_id, AgentInstanceRow.principal_id, AgentInstanceRow.status).where(AgentInstanceRow.home_id.is_not(None)))).all()
            current = await run_file_io(_drain_files, command.files, bindings, {home for home, _principal, _status in homes}, held)
            claims = await SpaceAttachments(command.files, command.adapter).retained_claims()
            owners = {home: principal for home, principal, status in homes if status == "active"}
            readonly = await storage_readonly_roots(command.files) if claims else ()
            await run_file_io(_check_host, command.files, command.adapter, pool, claims, current, owners, readonly)
            result = await _scan(command, session)
            await run_file_io(owner.acquire)
            return result
    except CommandRefused:
        raise
    except Exception as exc:
        raise CommandRefused(f"Rollback verification is incomplete: {exc}") from exc
    finally:
        try:
            await run_file_io(held.close)
        finally:
            await run_file_io(owner.close)
