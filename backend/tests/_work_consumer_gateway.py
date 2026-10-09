"""Disposable HTTP Gateway-router process for synthetic qualification data only.

Production Work/Attention routers and services; injected test identity/backing.
This is not production authentication or a native filesystem adapter.
"""

import argparse
import json
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace

import uvicorn
from _storage_spaces_test_support import ALICE, BOB, FixtureCatalog, FixtureVolume
from fastapi import FastAPI
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.gateway.authz import AuthContext
from app.gateway.routers import agent_instances, agent_work, human_input
from deerflow.agent_instances.directory import InstanceDirectory
from deerflow.agent_instances.service import AgentInstances
from deerflow.spaces.contract import ResolvedPrincipal
from deerflow.spaces.principals import HostPrincipalResolver
from deerflow.spaces.registry import SpaceRegistry
from deerflow.spaces.service import SpaceFiles


def app_from_fixture(fixture):
    engine = create_async_engine(fixture["url"], connect_args=fixture["connect_args"])
    sf = async_sessionmaker(engine, expire_on_commit=False)

    async def human(ref):
        return ResolvedPrincipal(ref, ref == ALICE) if ref in (ALICE, BOB) else None

    directory = InstanceDirectory(sf, human=human)
    catalog = object.__new__(FixtureCatalog)
    catalog.volumes, catalog.verified = {}, {}
    for item in fixture["volumes"]:
        spec = SimpleNamespace(**item["spec"])
        catalog.volumes[spec.slot_id] = spec
        catalog.verified[spec.slot_id] = FixtureVolume(spec, Path(item["data"]), Path(item["control"]))
    files = SpaceFiles(SpaceRegistry(sf, HostPrincipalResolver(human=human, nonhuman=directory.lookup)), catalog)

    @asynccontextmanager
    async def lifespan(app):
        yield
        await engine.dispose()

    app = FastAPI(lifespan=lifespan)
    app.state.agent_instances = AgentInstances(files, directory)
    app.state.storage_spaces = files
    agent_instances.get_agents_api_config = lambda: SimpleNamespace(enabled=True)

    @app.middleware("http")
    async def synthetic_human(request, call_next):
        user = SimpleNamespace(id="bob", system_role="user")
        request.state.user = user
        request.state.auth = AuthContext(user, ["agents:read", "agents:write"])
        request.state.auth_source = "session"
        return await call_next(request)

    app.include_router(agent_instances.router)
    app.include_router(agent_work.router)
    app.include_router(human_input.router)
    return app


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", required=True)
    parser.add_argument("--fd", required=True, type=int)
    args = parser.parse_args()
    fixture = json.loads(Path(args.fixture).read_text(encoding="utf-8"))
    uvicorn.run(app_from_fixture(fixture), fd=args.fd, log_level="error", access_log=False)
