"""Actual optional vendor-input package through the authenticated plugin route."""

import importlib.util
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from _storage_spaces_test_support import BOB, operation
from deerflow_extension_api.auth import EXTENSION_PRINCIPAL_RESOLVER_KEY, ExtensionPrincipal
from test_agent_instances import instances as instances
from test_agent_instances import request_app
from test_agent_work_records import work as work
from test_work_human_input import ask, reply
from test_work_human_input import attention as attention

from deerflow.extensions.registry import ExtensionRegistry

PACKAGE = Path(__file__).resolve().parents[2] / "examples/deerflow-extension-work-input/deerflow_extension_work_input"


def load_example():
    spec = importlib.util.spec_from_file_location("work_input_example", PACKAGE / "__init__.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["enabled", "absent", "disabled", "removed", "incompatible", "failing"])
async def test_optional_package_preserves_canonical_requests(attention, monkeypatch, state):
    from app.gateway.routers import agent_instances as routes
    from app.gateway.routers.plugins import router

    module = load_example()
    service, instance, work, _ = attention
    request = await ask(attention)
    registry = ExtensionRegistry()
    with registry.attributed_to("test:vendor-input"):
        if state in {"enabled", "disabled", "removed", "failing"}:
            contribution = module.contribution(enabled=state != "disabled")
            if state == "failing":

                async def fail(*_):
                    raise RuntimeError("Example validator failed")

                contribution = replace(contribution, backend=(replace(contribution.backend[1], handler=fail),))
            registry.plugin(contribution)
        elif state == "incompatible":
            with pytest.raises(ValueError):
                registry.plugin(replace(module.contribution(), human_input_api_version=999))
    app = request_app(service.agents, actor=BOB)
    app.state.extensions = registry.build() if state != "removed" else ExtensionRegistry().build()
    setattr(app.state, EXTENSION_PRINCIPAL_RESOLVER_KEY, lambda _: ExtensionPrincipal("bob"))
    app.include_router(router)
    monkeypatch.setattr(routes, "get_agents_api_config", lambda: SimpleNamespace(enabled=True))
    body = {
        "request_id": request["id"],
        "operation_id": operation(),
        "expected_request_revision": request["request_revision"],
        "expected_assignment_revision": request["assignment_revision"],
        "supplier": "Northwind",
        "delivery": "2026-11-10",
        "currency": "EUR",
        "quoted_total": "125.50",
    }
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        result = await client.post("/api/plugins/example.work-input/actions/respond", json=body)
        if state == "enabled":
            assert result.status_code == 200, result.text
            assert result.json()["validation"] == "Example checked field format only; commercial facts await AI employee assessment."
            again = await client.post("/api/plugins/example.work-input/actions/respond", json=body)
            assert again.status_code == 200 and again.json() == result.json()
        else:
            assert result.status_code >= 400
            assert (await service.get(actor=BOB, request_id=request["id"]))["state"] == "pending"
            await service.respond(actor=BOB, request_id=request["id"], request=reply(request))
    responses = (await service.responses(actor=BOB, request_id=request["id"]))["responses"]
    assert len(responses) == 1 and responses[0]["actor_id"] == "bob"
    retained = await service.work.get(actor=BOB, instance_id=instance.id, work_id=work["id"])
    assert retained["attempt"] is None and retained["status"] == "blocked"
    assert (await service.history(actor=BOB, request_id=request["id"]))["events"]


@pytest.mark.asyncio
@pytest.mark.parametrize("changes", [{"currency": "usd"}, {"quoted_total": "12"}, {"delivery": "20261110"}, {"delivery": "2026-02-30"}, {"supplier": ""}])
async def test_invalid_form_is_definitely_unsubmitted(changes):
    from unittest.mock import AsyncMock

    module = load_example()
    facade = SimpleNamespace(call=AsyncMock())
    payload = {
        "request_id": operation(),
        "operation_id": operation(),
        "expected_request_revision": 1,
        "expected_assignment_revision": 1,
        "supplier": "Northwind",
        "delivery": "2026-11-10",
        "currency": "EUR",
        "quoted_total": "125.50",
        **changes,
    }
    result = await module.respond(payload, SimpleNamespace(human_input=facade))
    assert result["status"] == "invalid"
    facade.call.assert_not_called()
