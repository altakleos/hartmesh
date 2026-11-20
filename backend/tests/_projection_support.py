"""Use the production preview lifespan and wait for startup in route tests."""

import sys
from contextlib import asynccontextmanager, contextmanager
from importlib.util import find_spec
from pathlib import Path

import pytest

from app.gateway.routers._artifact_projection import projection_lifespan


@asynccontextmanager
async def ready_projection_host(app):
    async with projection_lifespan(app):
        await app.state.projection_host.wait_ready()
        yield


@contextmanager
def installed_or_checkout_package(name, checkout):
    """Use the installed extension when available or temporarily expose its sources."""
    if find_spec(name) is not None:
        yield
        return
    # Checkouts without the extensions group exercise the same startup and route
    # via the source package. Its imports must expire with the temporary path.
    before = set(sys.modules)
    with pytest.MonkeyPatch.context() as patch:
        patch.syspath_prepend(str(checkout))
        try:
            yield
        finally:
            for module in set(sys.modules) - before:
                if module == name or module.startswith(name + "."):
                    del sys.modules[module]


@pytest.fixture(scope="module", autouse=True)
def legacy_report_package():
    checkout = Path(__file__).resolve().parents[1] / "extensions/sources/hartmesh-legacy-report"
    with installed_or_checkout_package("hartmesh_legacy_report", checkout):
        yield
