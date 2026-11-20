"""Use the production preview lifespan and wait for startup in route tests."""

from contextlib import asynccontextmanager

from app.gateway.routers._artifact_projection import projection_lifespan


@asynccontextmanager
async def ready_projection_host(app):
    async with projection_lifespan(app):
        await app.state.projection_host.wait_ready()
        yield
