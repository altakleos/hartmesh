"""Durable instance creation consuming the host Storage Spaces boundary.

Enrollment, atomic home binding and canonical membership grants are separately
recoverable. The same creation identity resumes a pending intent; it never
allocates another home after a recorded binding or repairs a ready agent's
revoked grants. An instance cannot execute while provisioning is incomplete.
"""

from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError

from deerflow.agent_instances.contract import ALL_AGENT_PERMISSIONS, AgentConflict, AgentDenied, AgentInstance, AgentPermission, DefinitionSnapshot, InstanceIdentity
from deerflow.agent_instances.directory import InstanceDirectory
from deerflow.persistence.agent_instances.model import AgentDefinitionRevisionRow, AgentInstanceGrantRow, AgentInstanceRow
from deerflow.persistence.spaces.model import SpaceGrantRow
from deerflow.spaces.contract import Custody, MutationMode, Permission, PrincipalRef, SpaceConflict, validate_generation, validate_name
from deerflow.spaces.service import SpaceCapacityExhausted, SpaceFiles
from deerflow.utils.file_io import await_drained

HOME_WORKER_ACCESS = Permission.READ | Permission.WRITE | Permission.EXPORT
HOME_MANAGER_ACCESS = HOME_WORKER_ACCESS | Permission.ADMIN


class _BoundHome(Exception):
    """Rollback an unused allocation after another creator bound the home."""


class _ReadyInstance(Exception):
    """Stop stale initialization without changing a ready instance's grants."""

    def __init__(self, instance):
        self.instance = instance


class AgentInstances:
    def __init__(self, files: SpaceFiles, directory: InstanceDirectory):
        self.files = files
        self.directory = directory
        self._sf = directory.session_factory

    async def _human(self, actor):
        if not isinstance(actor, PrincipalRef) or actor.kind != "human":
            raise AgentDenied("Agent management requires a current human identity")
        resolved = await self.directory.human(actor)
        if resolved is None or resolved.reference != actor:
            raise AgentDenied("Agent management requires a current human identity")
        return resolved

    @staticmethod
    def _view(row, permissions):
        if type(permissions) is not int or not 1 <= permissions <= 7:
            raise ValueError("Invalid persisted agent permissions")
        return AgentInstance(**vars(InstanceIdentity.from_row(row)), permissions=AgentPermission(permissions))

    async def _read(self, actor, instance_id, permission):
        await self._human(actor)
        if not isinstance(permission, AgentPermission) or not 1 <= int(permission) <= 7:
            raise ValueError("A valid instance permission is required")
        async with self._sf() as session:
            result = (
                await session.execute(
                    select(AgentInstanceRow, AgentInstanceGrantRow.permissions)
                    .join(AgentInstanceGrantRow, AgentInstanceGrantRow.instance_id == AgentInstanceRow.id)
                    .where(AgentInstanceRow.id == instance_id, AgentInstanceRow.status != "deleted", AgentInstanceGrantRow.user_id == actor.subject_id)
                )
            ).one_or_none()
        if result is None:
            raise AgentDenied("Agent instance is unavailable")
        view = self._view(*result)
        if view.permissions & permission != permission:
            raise AgentDenied("This agent operation is not granted")
        return view

    async def get(self, *, actor, instance_id, permission=AgentPermission.INSPECT):
        return await self._read(actor, instance_id, permission)

    async def list(self, *, actor, limit=100, offset=0):
        await self._human(actor)
        if type(limit) is not int or not 1 <= limit <= 500 or type(offset) is not int or offset < 0:
            raise ValueError("Invalid instance pagination")
        async with self._sf() as session:
            results = (
                await session.execute(
                    select(AgentInstanceRow, AgentInstanceGrantRow.permissions)
                    .join(AgentInstanceGrantRow, AgentInstanceGrantRow.instance_id == AgentInstanceRow.id)
                    .where(AgentInstanceGrantRow.user_id == actor.subject_id, AgentInstanceRow.status != "deleted")
                    .order_by(AgentInstanceRow.created_at, AgentInstanceRow.id)
                    .limit(limit)
                    .offset(offset)
                )
            ).all()
        return [self._view(*value) for value in results]

    async def definition(self, *, actor, instance_id):
        view = await self._read(actor, instance_id, AgentPermission.INSPECT)
        async with self._sf() as session:
            return await self._stored_definition(session, view.definition_revision)

    @staticmethod
    async def _stored_definition(session, revision):
        row = await session.get(AgentDefinitionRevisionRow, revision)
        if row is None:
            raise AgentConflict("The adopted definition is unavailable")
        try:
            snapshot = DefinitionSnapshot.capture(owner_id=row.owner_id, config=row.config, soul=row.soul)
        except ValueError:
            raise AgentConflict("The adopted definition is invalid") from None
        if snapshot.revision != revision:
            raise AgentConflict("The adopted definition no longer matches its revision")
        return snapshot

    @staticmethod
    async def _reserve_writer(session):
        if session.bind.dialect.name == "sqlite":
            await session.execute(text("BEGIN IMMEDIATE"))

    async def creation(self, *, actor, creation_id):
        """Read an owner-scoped retry without rereading a mutable definition."""
        await self._human(actor)
        SpaceFiles._operation_id(creation_id)
        async with self._sf() as session:
            row = (await session.execute(select(AgentInstanceRow).where(AgentInstanceRow.creator_id == actor.subject_id, AgentInstanceRow.creation_id == creation_id))).scalar_one_or_none()
            if row is None:
                return None
            # Returned only to trusted host admission, never as a raw API row.
            return row.creation_request, await self._stored_definition(session, row.definition_revision)

    async def _enroll(self, actor, request, definition, creation_id):
        for attempt in range(3):
            try:
                async with self._sf() as session, session.begin():
                    await self._reserve_writer(session)
                    row = (await session.execute(select(AgentInstanceRow).where(AgentInstanceRow.creator_id == actor.subject_id, AgentInstanceRow.creation_id == creation_id).with_for_update())).scalar_one_or_none()
                    if row is not None:
                        if row.creation_request != request:
                            raise AgentConflict("Creation identity belongs to another agent request")
                        if row.status not in {"provisioning", "active"}:
                            raise AgentConflict("Creation retry cannot reactivate an agent")
                        grant = await session.get(AgentInstanceGrantRow, (row.id, actor.subject_id))
                        if grant is None or not grant.permissions & int(AgentPermission.MANAGE):
                            raise AgentDenied("Creation retry requires current management access")
                        await self._stored_definition(session, row.definition_revision)
                        return self._view(row, grant.permissions)
                    resolved = await self._human(actor)
                    if request["custody"] == "company" and resolved.can_provision_company is not True:
                        raise AgentDenied("Company agents require current host provisioning authority")
                    await self._human(PrincipalRef("human", request["supervisor_id"]))
                    revision = await session.get(AgentDefinitionRevisionRow, definition.revision)
                    if revision is None:
                        session.add(AgentDefinitionRevisionRow(revision=definition.revision, owner_id=definition.owner_id, config=definition.config, soul=definition.soul))
                        await session.flush()
                    else:
                        stored = await self._stored_definition(session, definition.revision)
                        if stored != definition:
                            raise AgentConflict("The adopted definition does not match its captured document")
                    key = uuid4().hex
                    row = AgentInstanceRow(
                        id=key,
                        principal_id="agent:" + key,
                        name=request["name"],
                        custody=request["custody"],
                        owner_id=actor.subject_id if request["custody"] == "personal" else None,
                        creator_id=actor.subject_id,
                        supervisor_id=request["supervisor_id"],
                        definition_revision=definition.revision,
                        status="provisioning",
                        generation=1,
                        creation_id=creation_id,
                        creation_request=request,
                    )
                    session.add(row)
                    await session.flush()
                    for member in sorted({actor.subject_id, row.supervisor_id}):
                        session.add(AgentInstanceGrantRow(instance_id=key, user_id=member, permissions=int(ALL_AGENT_PERMISSIONS)))
                    await session.flush()
                    return self._view(row, int(ALL_AGENT_PERMISSIONS))
            except IntegrityError:
                # Only concurrent enrollment/revision deduplication is retried;
                # a persistent constraint error still fails, preserving intent.
                if attempt == 2:
                    raise

    async def _bind_home(self, actor, instance):
        if instance.home_id is not None:
            return instance
        for attempt in range(len(self.files.catalog.volumes) + 1):
            try:
                async with self.files.provisioning(actor=actor, name=instance.name, custody=Custody.personal(actor) if instance.custody == "personal" else Custody.company(), mode=MutationMode.NATIVE) as (session, home):
                    row = (await session.execute(select(AgentInstanceRow).where(AgentInstanceRow.id == instance.id).with_for_update())).scalar_one()
                    if row.home_id is not None:
                        raise _BoundHome()
                    if row.status != "provisioning" or row.creator_id != actor.subject_id:
                        raise AgentConflict("Agent enrollment changed before home binding")
                    await self._human(actor)
                    row.home_id = home.id
                    row.updated_at = datetime.now(UTC)
                    await session.flush()
                return await self._read(actor, instance.id, AgentPermission.MANAGE)
            except _BoundHome:
                return await self._read(actor, instance.id, AgentPermission.MANAGE)
            except SpaceCapacityExhausted:
                # Definite allocation refusal is effect-free. A peer may have
                # consumed the final slot for this exact intent; re-read its
                # binding after the rolled-back resource transaction closes.
                current = await self._read(actor, instance.id, AgentPermission.MANAGE)
                if current.home_id is not None:
                    return current
                raise
            except IntegrityError:
                if attempt == len(self.files.catalog.volumes):
                    raise

    async def _ensure_home_grant(self, actor, instance, subject, permissions):
        async def admission(session):
            await self._creation_admission(session, actor, instance)

        for _ in range(4):
            home = await self.files.registry.get(actor=actor, space_id=instance.home_id, permission=Permission.ADMIN)
            async with self._sf() as session:
                existing = await session.get(SpaceGrantRow, (home.id, subject.kind, subject.subject_id))
            if existing is not None and existing.permissions == int(permissions):
                return
            try:
                await self.files.registry.set_grant(actor=actor, space_id=home.id, expected_generation=home.generation, subject=subject, permissions=permissions, acknowledge_existing_data=True, admission=admission)
                return
            except SpaceConflict:
                # Never retry unknown filesystem/containment outcomes. Only
                # a fresh metadata read can resolve a stale concurrent grant.
                current = await self.files.registry.get(actor=actor, space_id=home.id, permission=Permission.ADMIN)
                if current.generation == home.generation:
                    raise
        raise AgentConflict("Concurrent home grants require retrying the same creation identity")

    async def _creation_admission(self, session, actor, instance):
        """Caller holds the storage parent; only pending enrollment may grant."""
        row = (await session.execute(select(AgentInstanceRow).where(AgentInstanceRow.id == instance.id).with_for_update())).scalar_one_or_none()
        grant = await session.get(AgentInstanceGrantRow, (instance.id, actor.subject_id))
        if row is None or grant is None or row.creator_id != actor.subject_id or row.home_id != instance.home_id:
            raise AgentDenied("Creation requires current instance management access")
        view = self._view(row, grant.permissions)
        await self._human(actor)
        if not view.permissions & AgentPermission.MANAGE:
            raise AgentDenied("Creation requires current instance management access")
        if row.status == "active":
            raise _ReadyInstance(view)
        if row.status != "provisioning":
            raise AgentConflict("Agent lifecycle changed during creation")
        return row

    async def _finish(self, actor, instance, grants):
        # Storage parent locks precede the instance lock. Verify recorded grants
        # in that one transaction before publishing an executable identity.
        async with self.files.registry.admitted(actor=actor, requests={instance.home_id: (Permission.ADMIN, None)}) as (session, _):
            row = await self._creation_admission(session, actor, instance)
            for subject, permissions in grants:
                await self.files.registry._actor(subject)
                grant = await session.get(SpaceGrantRow, (row.home_id, subject.kind, subject.subject_id))
                if grant is None or grant.permissions != int(permissions):
                    raise AgentConflict("Initial home access has not been confirmed")
            row.status = "active"
            row.generation += 1
            row.updated_at = datetime.now(UTC)
            await session.flush()
            return self._view(row, int(ALL_AGENT_PERMISSIONS))

    async def create(self, *, actor, creation_id, name, custody, supervisor, definition):
        validate_name(name)
        SpaceFiles._operation_id(creation_id)
        if custody not in {"personal", "company"}:
            raise ValueError("Invalid agent custody")
        await self._human(actor)
        await self._human(supervisor)
        if not isinstance(definition, DefinitionSnapshot):
            raise ValueError("A host-authorized definition snapshot is required")
        definition = definition.validated()
        if definition.owner_id != actor.subject_id:
            raise AgentDenied("Adopting a custom definition requires its current owner")
        request = {"name": name, "custody": custody, "supervisor_id": supervisor.subject_id, "definition_name": definition.config["name"]}

        async def perform():
            instance = await self._enroll(actor, request, definition, creation_id)
            if instance.status == "active":
                return instance
            instance = await self._bind_home(actor, instance)
            if instance.status == "active":
                return instance
            grants = [(instance.principal, HOME_WORKER_ACCESS)]
            if supervisor != actor:
                grants.append((supervisor, HOME_MANAGER_ACCESS))
            try:
                with self.directory.provisioning(instance.principal):
                    for subject, permissions in grants:
                        await self._ensure_home_grant(actor, instance, subject, permissions)
                    return await self._finish(actor, instance, grants)
            except _ReadyInstance as ready:
                return ready.instance

        return await await_drained(perform())

    async def rename(self, *, actor, instance_id, expected_generation, name):
        validate_name(name)
        validate_generation(expected_generation)
        await self._human(actor)

        async def perform():
            async with self._sf() as session, session.begin():
                await self._reserve_writer(session)
                row = (await session.execute(select(AgentInstanceRow).where(AgentInstanceRow.id == instance_id).with_for_update())).scalar_one_or_none()
                grant = await session.get(AgentInstanceGrantRow, (instance_id, actor.subject_id))
                if row is None or row.status == "deleted" or grant is None:
                    raise AgentDenied("Agent instance is unavailable")
                view = self._view(row, grant.permissions)
                if not view.permissions & AgentPermission.MANAGE:
                    raise AgentDenied("Managing this agent is not granted")
                await self._human(actor)
                if row.generation != expected_generation or row.generation >= 2**31 - 1 or row.status == "provisioning":
                    raise AgentConflict("Stale generation or incomplete agent provisioning")
                row.name = name
                row.generation += 1
                row.updated_at = datetime.now(UTC)
                await session.flush()
                return self._view(row, grant.permissions)

        return await await_drained(perform())
