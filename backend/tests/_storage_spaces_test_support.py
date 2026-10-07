"""Injected SQL/files fixtures; never a production quota or mount adapter."""

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass

import pytest
from sqlalchemy import event
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.schema import CreateSchema, DropSchema

from deerflow.persistence.base import Base
from deerflow.persistence.spaces.model import SpaceEventRow, SpaceGrantRow, SpaceRow
from deerflow.spaces.contract import Custody, MutationMode, PrincipalRef, ResolvedPrincipal
from deerflow.spaces.filesystem import ConfinedFilesystem
from deerflow.spaces.principals import HostPrincipalResolver
from deerflow.spaces.registry import SpaceRegistry
from deerflow.spaces.service import SpaceFiles

ALICE = PrincipalRef("human", "alice")
BOB = PrincipalRef("human", "bob")


@dataclass
class FixtureVolume:
    spec: object
    data_path: object
    control_path: object

    @property
    def root_inode(self):
        return self.data_path.stat().st_ino

    @property
    def control_inode(self):
        return self.control_path.stat().st_ino

    def filesystem(self):
        return ConfinedFilesystem(self.data_path, self.control_path)


class FixtureCatalog:
    """An injected host test adapter, never a production quota fallback."""

    def __init__(self, tmp_path):
        from types import SimpleNamespace

        self.volumes = {}
        self.verified = {}
        for i in range(4):
            root = tmp_path / str(i)
            root.mkdir()
            data, control = root / "data", root / "control"
            data.mkdir()
            control.mkdir(mode=0o700)
            spec = SimpleNamespace(slot_id=uuid.uuid4().hex, filesystem_uuid=str(uuid.uuid4()), max_bytes=1 << 20, max_inodes=1024)
            self.volumes[spec.slot_id] = spec
            self.verified[spec.slot_id] = FixtureVolume(spec, data, control)

    def verify(self, slot_id, *, previous=None):
        return self.verified[slot_id]


async def make_storage_fixture(tmp_path, request):
    from deerflow.persistence.spaces.files import SpaceBackingRow, SpaceFileOperationRow

    admin = None
    schema = None
    if request.param == "postgres":
        uri = os.environ.get("TEST_POSTGRES_URI")
        if not uri:
            pytest.skip("TEST_POSTGRES_URI is not configured")
        url = make_url(uri).set(drivername="postgresql+asyncpg")
        ssl = {"ssl": False} if url.query.get("sslmode") == "disable" else {}
        url = url.difference_update_query(["sslmode"])
        admin = create_async_engine(url, connect_args=ssl)
        schema = "storage_files_" + uuid.uuid4().hex
        async with admin.begin() as c:
            await c.execute(CreateSchema(schema))
        engine = create_async_engine(url, connect_args={**ssl, "server_settings": {"search_path": schema}})
    else:
        engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'resources.db'}")

    @event.listens_for(engine.sync_engine, "connect")
    def pragmas(connection, _):
        if engine.dialect.name == "sqlite":
            connection.execute("PRAGMA foreign_keys=ON")
            connection.execute("PRAGMA busy_timeout=30000")

    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all, tables=[row.__table__ for row in (SpaceRow, SpaceGrantRow, SpaceEventRow, SpaceBackingRow, SpaceFileOperationRow)])
    sf = async_sessionmaker(engine, expire_on_commit=False)
    subjects = {ALICE: ResolvedPrincipal(ALICE, True), BOB: ResolvedPrincipal(BOB)}

    async def lookup(ref):
        return subjects.get(ref)

    catalog = FixtureCatalog(tmp_path)
    service = SpaceFiles(SpaceRegistry(sf, HostPrincipalResolver(human=lookup)), catalog)
    try:
        yield service, sf, catalog
    finally:
        await engine.dispose()
        if admin is not None:
            try:
                async with admin.begin() as c:
                    await c.execute(DropSchema(schema, cascade=True))
            finally:
                await admin.dispose()


async def home(service, name="Home", actor=ALICE):
    return await service.create(actor=actor, name=name, custody=Custody.personal(actor), mode=MutationMode.NATIVE)


def operation():
    return uuid.uuid4().hex
