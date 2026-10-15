"""Durable instance creation consuming the host Storage Spaces boundary.

Enrollment, atomic home binding and canonical membership grants are separately
recoverable. The same creation identity resumes a pending intent; it never
allocates another home after a recorded binding or repairs a ready agent's
revoked grants. An instance cannot execute while provisioning is incomplete.
"""

from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import delete, func, select, text
from sqlalchemy.exc import IntegrityError

from deerflow.agent_instances.contract import ALL_AGENT_PERMISSIONS, AgentConflict, AgentDenied, AgentInstance, AgentPermission, CompanyProvisioningDenied, DefinitionSnapshot, InstanceIdentity, Refusal
from deerflow.agent_instances.directory import InstanceDirectory
from deerflow.config.agents_api_config import get_agents_api_config
from deerflow.persistence.agent_instances.model import AgentDefinitionRevisionRow, AgentInstanceGrantRow, AgentInstanceRow, AgentLifecycleRow
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
        self.execution_probe = None
        self.allowance_of = None
        self.residents = None
        from deerflow.agent_instances.agent_files import AgentFiles

        self.agent_files = AgentFiles(self)

    async def _human(self, actor, *, session=None):
        if not isinstance(actor, PrincipalRef) or actor.kind != "human":
            raise AgentDenied("Agent management requires a current human identity")
        borrowed = getattr(self.directory.human, "in_session", None)
        resolved = await borrowed(actor, session) if session is not None and callable(borrowed) else await self.directory.human(actor)
        if resolved is None or resolved.reference != actor:
            raise AgentDenied("Agent management requires a current human identity")
        return resolved

    async def _creation_authority(self, session, actor, custody):
        resolved = await self._human(actor, session=session)
        if custody == "company" and resolved.can_provision_company is not True:
            raise CompanyProvisioningDenied("Company agents require current host provisioning authority")

    async def space_owner(self, space_id):
        """Host-only ownership metadata; grants are admitted by the caller."""
        async with self._sf() as session:
            return await session.scalar(select(AgentInstanceRow).where(AgentInstanceRow.home_id == space_id))

    def space_audience_admission(self, home_id, subject):
        """Borrow the Home transaction before granting a human its files."""
        from deerflow.spaces.contract import SpaceDenied

        async def admission(session):
            message = "A grant on the agent's files requires Inspect on the agent"
            try:
                await self._human(subject, session=session)
            except AgentDenied as exc:
                raise SpaceDenied(message) from exc
            row = await session.scalar(select(AgentInstanceRow).where(AgentInstanceRow.home_id == home_id).with_for_update())
            grant = await session.get(AgentInstanceGrantRow, (row.id, subject.subject_id)) if row is not None else None
            if grant is None or not grant.permissions & int(AgentPermission.INSPECT):
                raise SpaceDenied(message)

        return admission

    async def _creation_counts(self, session, actor, custody):
        kind = "assistant" if custody == "personal" else "employee"
        rows = select(func.count()).select_from(AgentInstanceRow).where(AgentInstanceRow.custody == custody, AgentInstanceRow.status.in_(("provisioning", "active", "suspended", "archived")))
        allowance = await self.allowance_of(kind, session=session) if self.allowance_of is not None else None
        in_use = allowance.in_use if allowance is not None else await session.scalar(rows)
        allowed = allowance.allowed if allowance is not None else len(self.files.catalog.volumes)
        extra = {"allowed": allowed, "in_use": in_use}
        if custody == "personal":
            extra = {"per_user": get_agents_api_config().ai_assistants_per_user, "mine": await session.scalar(rows.where(AgentInstanceRow.owner_id == actor.subject_id)), **extra}
        return allowance, extra

    async def _admit_creation_count(self, session, actor, custody):
        allowance, extra = await self._creation_counts(session, actor, custody)
        if custody == "personal":
            if extra["per_user"] == 0 or extra["mine"] >= extra["per_user"]:
                raise Refusal("You already have as many AI assistants as each person can have here.", reason="ai-assistant-limit", **extra)
            if allowance is not None and allowance.free <= 0:
                raise Refusal("No AI assistant can be added right now: all the AI assistants allowed here are in use.", reason="ai-assistant-none-free", **extra)
        elif allowance is not None and (allowance.allowed_key == 0 or allowance.in_use >= allowance.allowed):
            if allowance.allowed_key == 0:
                extra["allowed"] = 0
            raise Refusal("All the AI employees allowed here are in use.", reason="ai-employee-limit", **extra)

    @staticmethod
    def _view(row, permissions):
        if type(permissions) is not int or not 1 <= permissions <= 7:
            raise ValueError("Invalid persisted agent permissions")
        return AgentInstance(**vars(InstanceIdentity.from_row(row)), permissions=AgentPermission(permissions))

    async def _read(self, actor, instance_id, permission, *, include_deleted=False):
        await self._human(actor)
        if not isinstance(permission, AgentPermission) or not 1 <= int(permission) <= 7:
            raise ValueError("A valid instance permission is required")
        async with self._sf() as session:
            result = (
                await session.execute(
                    select(AgentInstanceRow, AgentInstanceGrantRow.permissions)
                    .join(AgentInstanceGrantRow, AgentInstanceGrantRow.instance_id == AgentInstanceRow.id)
                    .where(AgentInstanceRow.id == instance_id, True if include_deleted else AgentInstanceRow.status != "deleted", AgentInstanceGrantRow.user_id == actor.subject_id)
                )
            ).one_or_none()
        if result is None:
            raise AgentDenied("Agent instance is unavailable")
        view = self._view(*result)
        if view.permissions & permission != permission:
            raise AgentDenied("This agent operation is not granted")
        return view

    async def get(self, *, actor, instance_id, permission=AgentPermission.INSPECT, include_deleted=False):
        return await self._read(actor, instance_id, permission, include_deleted=include_deleted)

    async def list(self, *, actor, limit=100, offset=0, include_deleted=False, custody=None, include_provisioning=True):
        await self._human(actor)
        if type(limit) is not int or not 1 <= limit <= 500 or type(offset) is not int or offset < 0 or custody not in {None, "personal", "company"}:
            raise ValueError("Invalid instance pagination")
        order = [AgentInstanceRow.created_at, AgentInstanceRow.id]
        if custody == "personal":
            order.insert(0, (AgentInstanceRow.owner_id == actor.subject_id).desc())
        async with self._sf() as session:
            results = (
                await session.execute(
                    select(AgentInstanceRow, AgentInstanceGrantRow.permissions)
                    .join(AgentInstanceGrantRow, AgentInstanceGrantRow.instance_id == AgentInstanceRow.id)
                    .where(
                        AgentInstanceGrantRow.user_id == actor.subject_id,
                        True if include_deleted else AgentInstanceRow.status != "deleted",
                        True if include_provisioning else AgentInstanceRow.status != "provisioning",
                        True if custody is None else AgentInstanceRow.custody == custody,
                    )
                    .order_by(*order)
                    .limit(limit)
                    .offset(offset)
                )
            ).all()
        return [self._view(*value) for value in results]

    async def unfinished(self, *, actor, custody=None):
        """The creator's incomplete intents, independent of the normal page."""
        await self._human(actor)
        if custody not in {None, "personal", "company"}:
            raise ValueError("Invalid agent custody")
        async with self._sf() as session:
            rows = (
                await session.scalars(
                    select(AgentInstanceRow)
                    .where(AgentInstanceRow.creator_id == actor.subject_id, AgentInstanceRow.status == "provisioning", True if custody is None else AgentInstanceRow.custody == custody)
                    .order_by(AgentInstanceRow.created_at, AgentInstanceRow.id)
                )
            ).all()
        return [{"id": row.id, "name": row.name, "custody": row.custody, "created_at": row.created_at.replace(tzinfo=UTC) if row.created_at.tzinfo is None else row.created_at.astimezone(UTC)} for row in rows]

    async def personal_counts(self, *, actor):
        await self._human(actor)
        async with self._sf() as session, session.begin():
            _allowance, counts = await self._creation_counts(session, actor, "personal")
            return counts

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

    async def abandon_creation(self, instance_id: str, *, on_behalf_of: str | None = None) -> str | None:
        """Host cleanup of an unfinished intent; release alone owns its files."""
        from deerflow.persistence.spaces.model import SpaceRow
        from deerflow.spaces.recovery import SpaceRecovery

        def removable(row):
            if row is None:
                raise AgentDenied("Agent instance is unavailable")
            if row.status != "provisioning":
                raise Refusal("Only an AI assistant or AI employee whose setting up did not finish can be removed this way.", reason="not-removable")

        async def perform():
            for _ in range(3):
                async with self._sf() as session:
                    original = await session.get(AgentInstanceRow, instance_id)
                    if original is None:
                        raise AgentDenied("Agent instance is unavailable")
                    home_id = original.home_id
                if home_id is None:
                    async with self._sf() as session, session.begin():
                        await self._reserve_writer(session)
                        row = (await session.execute(select(AgentInstanceRow).where(AgentInstanceRow.id == instance_id).with_for_update())).scalar_one_or_none()
                        removable(row)
                        if row.home_id is not None:
                            continue
                        await session.execute(delete(AgentInstanceGrantRow).where(AgentInstanceGrantRow.instance_id == instance_id))
                        await session.delete(row)
                        return None
                async with self.files.registry.host_locked([home_id]) as (session, homes):
                    row = (await session.execute(select(AgentInstanceRow).where(AgentInstanceRow.id == instance_id).with_for_update())).scalar_one_or_none()
                    removable(row)
                    if row.home_id != home_id:
                        continue
                    home = homes.get(home_id)
                    if home is None or not isinstance(home, SpaceRow):
                        raise AgentConflict("The unfinished agent's Home is unavailable")
                    if row.generation >= 2**31 - 1:
                        raise AgentConflict("The generation is exhausted; the agent cannot be removed")
                    actor = PrincipalRef("human", on_behalf_of or row.creator_id)
                    SpaceRecovery._advance(session, home, actor, "delete", {"retained_backups": False, "retained_binding": True, "by": "host", "cause": "creation-abandoned"})
                    home.status = row.status = "deleted"
                    row.generation += 1
                    row.updated_at = datetime.now(UTC)
                    return home_id
            raise AgentConflict("The unfinished agent's Home changed; retry the removal")

        return await await_drained(perform())

    async def host_delete(self, instance_id: str, *, host_id: str, dry_run: bool = False) -> str | None:
        from deerflow.agent_instances.host_operations import host_delete

        return await await_drained(host_delete(self, instance_id, host_id=host_id, dry_run=dry_run))

    async def _enroll(self, actor, request, definition, creation_id):
        for attempt in range(3):
            try:
                async with self._sf() as session, session.begin():
                    await self._reserve_writer(session)
                    # Share pool serialization with Home binding and account
                    # release before kind counts or mutable identity snapshots.
                    await self.files._serialize_allocation(session)
                    if session.bind.dialect.name == "postgresql":
                        kind = "assistant" if request["custody"] == "personal" else "employee"
                        await session.execute(text("SELECT pg_advisory_xact_lock(hashtext(:key))"), {"key": "agent-count:" + kind})
                        if request["custody"] == "personal":
                            await session.execute(text("SELECT pg_advisory_xact_lock(hashtext(:key))"), {"key": "agent-personal:" + actor.subject_id})
                    await self._creation_authority(session, actor, request["custody"])
                    source = None
                    if request.get("copy_from"):
                        source = (await session.execute(select(AgentInstanceRow).where(AgentInstanceRow.id == request["copy_from"]).with_for_update())).scalar_one_or_none()
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
                    if request.get("copy_from"):
                        source_grant = await session.get(AgentInstanceGrantRow, (request["copy_from"], actor.subject_id))
                        required = int(AgentPermission.INSPECT | AgentPermission.MANAGE)
                        if source is None or source_grant is None or source.custody != "personal" or source.owner_id != actor.subject_id or source.status in {"deleted", "provisioning"} or source_grant.permissions & required != required:
                            raise AgentDenied("Company copying requires the current personal custodian with Inspect and Manage")
                        if source.generation != request["copy_generation"]:
                            raise AgentConflict("Personal source changed; review it before copying")
                        captured = await self._stored_definition(session, source.definition_revision)
                        if DefinitionSnapshot.capture(owner_id=actor.subject_id, config=captured.config, soul=captured.soul) != definition or request["custody"] != "company":
                            raise AgentConflict("Company copy must use the current source business definition")
                    await self._admit_creation_count(session, actor, request["custody"])
                    await self._human(PrincipalRef("human", request["supervisor_id"]), session=session)
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

                async def admission(session):
                    await self._creation_authority(session, actor, instance.custody)
                    row = await session.get(AgentInstanceRow, instance.id, populate_existing=True)
                    grant = await session.get(AgentInstanceGrantRow, (instance.id, actor.subject_id), populate_existing=True)
                    if row is None or row.status not in {"provisioning", "active"} or row.creator_id != actor.subject_id or grant is None or not grant.permissions & int(AgentPermission.MANAGE):
                        raise AgentDenied("Home binding requires current instance management access")
                    if row.home_id is not None:
                        raise _BoundHome()

                async with self.files.provisioning(
                    actor=actor,
                    name=instance.name,
                    custody=Custody.personal(actor) if instance.custody == "personal" else Custody.company(),
                    mode=MutationMode.NATIVE,
                    kind="assistant" if instance.custody == "personal" else "employee",
                    admission=admission,
                ) as (session, home):
                    row = (await session.execute(select(AgentInstanceRow).where(AgentInstanceRow.id == instance.id).with_for_update())).scalar_one()
                    if row.home_id is not None:
                        raise _BoundHome()
                    if row.status != "provisioning" or row.creator_id != actor.subject_id:
                        raise AgentConflict("Agent enrollment changed before home binding")
                    await self._creation_authority(session, actor, instance.custody)
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
                if self.allowance_of is None:
                    raise
                async with self._sf() as session, session.begin():
                    _allowance, extra = await self._creation_counts(session, actor, instance.custody)
                reason = "ai-assistant-none-free" if instance.custody == "personal" else "ai-employee-limit"
                detail = "No AI assistant can be added right now: all the AI assistants allowed here are in use." if instance.custody == "personal" else "All the AI employees allowed here are in use."
                raise Refusal(detail, reason=reason, **extra) from None
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
        await self._creation_authority(session, actor, row.custody)
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
                await self.files.registry._actor(subject, session=session)
                grant = await session.get(SpaceGrantRow, (row.home_id, subject.kind, subject.subject_id))
                if grant is None or grant.permissions != int(permissions):
                    raise AgentConflict("Initial home access has not been confirmed")
            row.status = "active"
            row.generation += 1
            row.updated_at = datetime.now(UTC)
            await session.flush()
            return self._view(row, int(ALL_AGENT_PERMISSIONS))

    async def create(self, *, actor, creation_id, name, custody, supervisor, definition, copy_from=None, copy_generation=None):
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
        if copy_from is not None:
            SpaceFiles._operation_id(copy_from)
            validate_generation(copy_generation)
            request.update(copy_from=copy_from, copy_generation=copy_generation)
        elif copy_generation is not None:
            raise ValueError("Copy generation requires a source instance")

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
                await self._human(actor, session=session)
                if row.generation != expected_generation or row.generation >= 2**31 - 1 or row.status == "provisioning":
                    raise AgentConflict("Stale generation or incomplete agent provisioning")
                if await session.scalar(select(AgentLifecycleRow.operation_id).where(AgentLifecycleRow.instance_id == instance_id, AgentLifecycleRow.phase == "pending")) is not None:
                    raise AgentConflict("Resolve the pending lifecycle operation before renaming")
                row.name = name
                row.generation += 1
                row.updated_at = datetime.now(UTC)
                await session.flush()
                return self._view(row, grant.permissions)

        return await await_drained(perform())
