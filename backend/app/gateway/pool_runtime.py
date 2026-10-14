"""Gateway lifetime of the private managed-pool runtime capability."""

from contextlib import asynccontextmanager
from pathlib import Path

from deerflow.sandbox.lease import run_sync_lifecycle_operation
from deerflow.spaces.runtime_owner import PoolRuntimeOwner
from deerflow.utils.file_io import await_drained


def mark_pool_runtime_closing(app):
    owner = getattr(app.state, "pool_runtime_owner", None)
    if owner is not None:
        owner.mark_closing()


@asynccontextmanager
async def pool_runtime_shutdown_boundary(app):
    """Signal closure before partial-startup exit-stack callbacks run."""
    try:
        yield
    finally:
        mark_pool_runtime_closing(app)


@asynccontextmanager
async def managed_pool_runtime(app, config):
    """Retain ownership beyond provider drains and SQL disposal.

    Acquisition belongs to native qualification, so a missing/busy pool still
    permits metadata access; native mountless execution still requires the host's
    complete accounting and opened admission. An incomplete provider constructor is
    joined off-loop here, including when early storage closure found no provider.
    """
    owner = PoolRuntimeOwner(Path(config.inventory_path).parent) if config.enabled and config.plan is not None else None
    app.state.pool_runtime_owner = owner
    try:
        yield
    finally:
        if owner is not None:
            owner.mark_closing()

            async def drain():
                from deerflow.sandbox.sandbox_provider import shutdown_sandbox_provider

                provider = await run_sync_lifecycle_operation(owner.drain_provider)
                try:
                    if provider is not None:
                        await run_sync_lifecycle_operation(shutdown_sandbox_provider, expected_provider=provider)
                finally:
                    await run_sync_lifecycle_operation(owner.close)

            await await_drained(drain())
        if getattr(app.state, "pool_runtime_owner", None) is owner:
            app.state.pool_runtime_owner = None
