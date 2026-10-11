"""Production identity lookup must share a borrowed writer's one connection."""

import asyncio
from datetime import UTC, datetime

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.gateway.auth.models import User
from app.gateway.auth.repositories.sqlite import SQLiteUserRepository
from app.gateway.storage_spaces import human_lookup
from deerflow.agent_instances.directory import InstanceDirectory
from deerflow.config.auth_config import LOCAL_ACCOUNT_ISSUER
from deerflow.persistence.agent_instances.model import AgentDefinitionRevisionRow, AgentInstanceRow
from deerflow.persistence.spaces.model import SpaceEventRow, SpaceGrantRow, SpaceRow
from deerflow.persistence.user.access import DisabledIdentityRow, RoleLimitRow
from deerflow.persistence.user.model import UserRow
from deerflow.spaces.contract import Custody, InvalidPrincipal, MutationMode, Permission, PrincipalRef
from deerflow.spaces.principals import HostPrincipalResolver
from deerflow.spaces.registry import SpaceRegistry


@pytest.mark.asyncio
@pytest.mark.parametrize("revocation", ["released", "disabled"])
async def test_production_borrowed_human_identity_uses_one_connection_and_rechecks_release(tmp_path, revocation):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'identity.db'}", pool_size=1, max_overflow=0, pool_timeout=0.2)
    sf = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with engine.begin() as connection:
            for table in (UserRow.__table__, DisabledIdentityRow.__table__, RoleLimitRow.__table__, SpaceRow.__table__, SpaceGrantRow.__table__, SpaceEventRow.__table__):
                await connection.run_sync(table.create)
        repository = SQLiteUserRepository(sf)
        user = User(email="borrowed@example.com", password_hash="x")
        async with sf() as session, session.begin():
            session.add(repository._user_to_row(user))
        registry = SpaceRegistry(sf, HostPrincipalResolver(human=human_lookup(repository)))
        actor = PrincipalRef("human", str(user.id))
        space = await registry.create(actor=actor, name="Borrowed", custody=Custody.personal(actor), mode=MutationMode.NATIVE)
        async with sf() as session, session.begin():
            async with asyncio.timeout(2):
                rows = await registry.admit_in_session(session, actor=actor, requests={space.id: (Permission.READ, space.generation)})
            assert rows[space.id][0].id == space.id
            if revocation == "released":
                account = await session.get(UserRow, actor.subject_id)
                account.released_at = datetime.now(UTC)
            else:
                session.add(DisabledIdentityRow(issuer=LOCAL_ACCOUNT_ISSUER, subject=actor.subject_id, disabled_at=datetime.now(UTC)))
            await session.flush()
            with pytest.raises(InvalidPrincipal):
                await registry.admit_in_session(session, actor=actor, requests={space.id: (Permission.READ, space.generation)})
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_production_borrowed_instance_lookup_uses_one_connection_and_rechecks_owner(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'instance.db'}", pool_size=1, max_overflow=0, pool_timeout=0.2)
    sf = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with engine.begin() as connection:
            for table in (UserRow.__table__, DisabledIdentityRow.__table__, RoleLimitRow.__table__, SpaceRow.__table__, AgentDefinitionRevisionRow.__table__, AgentInstanceRow.__table__):
                await connection.run_sync(table.create)
        repository = SQLiteUserRepository(sf)
        owner = User(email="owner@example.com", password_hash="x")
        async with sf() as session, session.begin():
            session.add(repository._user_to_row(owner))
            session.add(AgentDefinitionRevisionRow(revision="a" * 64, owner_id=str(owner.id), config={}, soul=""))
            session.add(SpaceRow(id="c" * 32, backing_handle="e" * 32, name="Home", custody_kind="personal", custodian_kind="human", custodian_id=str(owner.id), mode="native"))
            await session.flush()
            session.add(
                AgentInstanceRow(
                    id="b" * 32,
                    principal_id="agent:" + "b" * 32,
                    name="Worker",
                    custody="personal",
                    owner_id=str(owner.id),
                    creator_id=str(owner.id),
                    supervisor_id=str(owner.id),
                    definition_revision="a" * 64,
                    home_id="c" * 32,
                    status="active",
                    generation=1,
                    creation_id="d" * 32,
                    creation_request={},
                )
            )
        directory = InstanceDirectory(sf, human=human_lookup(repository))
        principal = PrincipalRef("nonhuman", "agent:" + "b" * 32)
        resolver = HostPrincipalResolver(human=human_lookup(repository), nonhuman=directory.lookup, nonhuman_in_session=directory.lookup_in_session)
        async with sf() as session, session.begin():
            async with asyncio.timeout(2):
                assert (await resolver.resolve_in_session(principal, session)).reference == principal
            account = await session.get(UserRow, str(owner.id))
            account.released_at = datetime.now(UTC)
            await session.flush()
            with pytest.raises(InvalidPrincipal):
                await resolver.resolve_in_session(principal, session)
    finally:
        await engine.dispose()
