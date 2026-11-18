"""Custom model endpoints on deployments whose connections are written by tenants.

With ``DEER_FLOW_TENANT_WRITTEN_ENDPOINTS=on`` the address of a custom model, the
one kind whose address an administrator chooses, must be a public HTTP(S)
address or a provider-listed private endpoint when it is saved, tested,
discovered or imported, and again at every connection. Models of the provider's
own configuration and of the fixed catalog are untouched.

Every test drives the real routes, the real store with its encryption, the
product's own ``get_app_config()`` and ``create_chat_model``, and a real call of
the model over a socket to a loopback stub. None constructs a model class by
hand or replaces an HTTP client, its transport or the address check.
"""

from __future__ import annotations

import asyncio
import socket
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from support.model_endpoint_stubs import ANSWER, MODEL_ID, ModelEndpointStub

from app.gateway.routers import managed_models as managed_models_router
from deerflow.config.app_config import get_app_config, reset_app_config
from deerflow.config.managed_models import ManagedModel, ManagedModelStore
from deerflow.models.factory import create_chat_model

LINE = "DEER_FLOW_TENANT_WRITTEN_ENDPOINTS"
PRIVATE_ENDPOINTS = "DEER_FLOW_PRIVATE_ENDPOINTS"
ENDPOINT_NOT_PUBLIC = {"code": "endpoint_not_public", "message": "Custom model endpoints on this deployment must use a public HTTP(S) address."}
KEY = "typed-model-key"
PUBLIC = "https://8.8.8.8/v1"
MADE_UP_NAME = "models.example.test"


@pytest.fixture(autouse=True)
def deployment(tmp_path, monkeypatch):
    from deerflow.config import managed_models
    from deerflow.config.extensions_config import reset_extensions_config

    home = tmp_path / "home"
    home.mkdir()
    config = tmp_path / "config.yaml"
    config.write_text("sandbox:\n  use: test\nmodels: []\n", encoding="utf-8")
    extensions = tmp_path / "extensions_config.json"
    extensions.write_text('{"mcpServers": {}, "skills": {}}', encoding="utf-8")
    monkeypatch.setenv("DEER_FLOW_HOME", str(home))
    monkeypatch.setenv("DEER_FLOW_CONFIG_PATH", str(config))
    monkeypatch.setenv("DEER_FLOW_EXTENSIONS_CONFIG_PATH", str(extensions))
    # An ordinary client reads proxy settings; loopback stubs must stay direct.
    monkeypatch.setenv("NO_PROXY", "127.0.0.1,localhost")
    monkeypatch.setenv("no_proxy", "127.0.0.1,localhost")
    for name in (LINE, PRIVATE_ENDPOINTS, "OPENAI_BASE_URL", "OPENAI_API_BASE", "OPENAI_PROXY"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(managed_models, "_cache", None)
    reset_app_config()
    reset_extensions_config()
    yield SimpleNamespace(config=config, store=ManagedModelStore())
    reset_app_config()
    reset_extensions_config()


@pytest.fixture
def stubs():
    with ExitStack() as stack:
        yield lambda: stack.enter_context(ModelEndpointStub())


@pytest.fixture
def client():
    app = FastAPI()

    @app.middleware("http")
    async def identity(request, call_next):
        request.state.user = SimpleNamespace(id="administrator-1", system_role=request.headers.get("test-role", "admin"))
        request.state.auth_source = "session"
        return await call_next(request)

    app.include_router(managed_models_router.router)
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def made_up_name(monkeypatch):
    """Answer ``models.example.test`` from a script: each lookup takes the next address, the last one repeating."""
    answers: list[str] = ["127.0.0.1"]
    lookups: list[str] = []
    system_lookup = socket.getaddrinfo

    def lookup(host, port, *args, **kwargs):
        # An HTTP client may hand the name over already encoded.
        if (host.decode("ascii") if isinstance(host, bytes) else host) != MADE_UP_NAME:
            return system_lookup(host, port, *args, **kwargs)
        lookups.append(answers[min(len(lookups), len(answers) - 1)])
        return system_lookup(lookups[-1], port, *args, **kwargs)

    monkeypatch.setattr(socket, "getaddrinfo", lookup)
    return SimpleNamespace(answers=answers, lookups=lookups)


def _tenant_written(monkeypatch, *listed: ModelEndpointStub) -> None:
    monkeypatch.setenv(LINE, "on")
    monkeypatch.setenv(PRIVATE_ENDPOINTS, ",".join(stub.host_port for stub in listed))


def _store(deployment, base_url: str, *, name: str = "records-model") -> ManagedModel:
    """Write a custom model straight into the store, as a save before this rule would have left it."""
    return deployment.store.save(ManagedModel(name=name, model=MODEL_ID, base_url=base_url, api_key=KEY), expected_revision=None)


def _model(name: str = "records-model"):
    return create_chat_model(name, attach_tracing=False, max_retries=0, timeout=5)


def _draft(base_url: str, **fields) -> dict:
    return {"config": {"model": MODEL_ID, "base_url": base_url, "api_key": KEY, **fields}}


def _connection(base_url: str) -> dict:
    return {"provider": "openai-compatible", "base_url": base_url, "api_key": KEY}


def _refused(response) -> bool:
    assert response.status_code == 422, response.text
    assert response.json()["detail"] == ENDPOINT_NOT_PUBLIC
    return True


def _calls(model) -> list:
    """One call each way: plain and asynchronous."""
    return [lambda: model.invoke("hello"), lambda: asyncio.run(model.ainvoke("hello"))]


@pytest.mark.parametrize("address", ["loopback", "private-number", "name-resolving-to-private"])
def test_custom_model_endpoints_must_be_public(address, deployment, client, stubs, made_up_name, monkeypatch):
    service = stubs()
    base_url = {"loopback": service.base_url(), "private-number": f"http://10.0.0.5:{service.port}/v1", "name-resolving-to-private": service.base_url(host=MADE_UP_NAME)}[address]
    _store(deployment, PUBLIC, name="ledger-model")
    before = deployment.store.path.read_bytes()
    _tenant_written(monkeypatch)

    assert _refused(client.put("/api/managed-models", json=_draft(base_url)))
    assert _refused(client.post("/api/managed-models/test", json=_draft(base_url)))
    assert _refused(client.post("/api/managed-models/discover", json=_connection(base_url)))
    assert _refused(client.post("/api/managed-models/import", json={"connection": _connection(base_url), "model": MODEL_ID}))

    assert service.connections == 0
    assert deployment.store.path.read_bytes() == before


def test_a_stored_custom_model_at_a_private_address_is_not_contacted(deployment, stubs, monkeypatch):
    service = stubs()
    _store(deployment, service.base_url())
    _tenant_written(monkeypatch)

    assert get_app_config().get_model_config("records-model") is not None
    for call in _calls(_model()):
        with pytest.raises(Exception) as failure:
            call()
        shown = f"{failure.value!r} {failure.value} {failure.value.__cause__!r}"
        assert "127.0.0.1" not in shown and str(service.port) not in shown and KEY not in shown

    assert service.connections == 0


def test_a_listed_custom_model_answers_on_every_loop_and_thread_and_leaves_nothing_open(deployment, stubs, monkeypatch):
    service = stubs()
    _store(deployment, service.base_url())
    _tenant_written(monkeypatch, service)
    shared = _model()

    async def answered(model) -> tuple[str, int]:
        reply = await model.ainvoke("hello")
        streamed = [chunk async for chunk in model.astream("hello")]
        return reply.content, len(streamed) > 0

    # One model on a first loop, on a second loop after the first has closed, and one built afresh.
    assert asyncio.run(answered(shared)) == (ANSWER, True)
    assert asyncio.run(answered(shared)) == (ANSWER, True)
    assert asyncio.run(answered(_model())) == (ANSWER, True)
    with ThreadPoolExecutor(max_workers=3) as workers:
        plain = [workers.submit(shared.invoke, "hello"), workers.submit(lambda: _model().invoke("hello"))]
        on_a_worker_loop = workers.submit(lambda: asyncio.run(answered(shared)))
        assert [future.result(timeout=30).content for future in plain] == [ANSWER, ANSWER]
        assert on_a_worker_loop.result(timeout=30) == (ANSWER, True)
    assert [chunk for chunk in shared.stream("hello")]
    # Last, a plain answer the endpoint offers to keep the connection open after.
    assert shared.invoke("hello").content == ANSWER

    assert f"Bearer {KEY}" in service.received_text()
    assert service.settled_open_connections() == 0


def test_a_name_that_changes_its_answer_is_not_contacted(deployment, client, stubs, made_up_name, monkeypatch):
    service = stubs()
    base_url = service.base_url(host=MADE_UP_NAME)
    _tenant_written(monkeypatch)
    # A public address when the address is checked at the route, the stub's afterwards.
    made_up_name.answers[:] = ["8.8.8.8", "127.0.0.1"]

    response = client.post("/api/managed-models/discover", json=_connection(base_url))
    assert (response.status_code, response.json()["detail"]["code"]) == (502, "discovery_failed")
    assert made_up_name.lookups[:2] == ["8.8.8.8", "127.0.0.1"]
    assert service.connections == 0

    made_up_name.lookups.clear()
    assert client.put("/api/managed-models", json=_draft(base_url, name="records-model")).status_code == 200
    for call in _calls(_model()):
        with pytest.raises(Exception):
            call()
    assert service.connections == 0


def test_a_custom_model_redirect_is_not_followed(deployment, stubs, monkeypatch):
    service, elsewhere = stubs(), stubs()
    _store(deployment, service.base_url())
    # Both endpoints are listed, so only the redirect rule can keep a request away.
    _tenant_written(monkeypatch, service, elsewhere)
    service.redirect_to = f"{elsewhere.base_url()}/chat/completions"

    for call in _calls(_model()):
        with pytest.raises(Exception):
            call()

    assert service.calls() == [("POST", "/v1/chat/completions")] * 2
    assert elsewhere.connections == 0


def test_provider_models_keep_their_endpoints(deployment, stubs, monkeypatch):
    service = stubs()
    deployment.config.write_text(
        f"sandbox:\n  use: test\nmodels:\n  - name: provider-model\n    use: langchain_openai:ChatOpenAI\n    model: {MODEL_ID}\n    base_url: {service.base_url()}\n    api_key: {KEY}\n",
        encoding="utf-8",
    )
    _tenant_written(monkeypatch)
    reset_app_config()

    assert get_app_config().get_model_config("provider-model").endpoint_policy is None
    model = _model("provider-model")
    assert (model.http_client, model.http_async_client) == (None, None)
    assert [call().content for call in _calls(model)] == [ANSWER, ANSWER]
    # A model of the fixed catalog carries no endpoint policy; a custom one always does.
    assert ManagedModel(provider="openai", model="gpt-4o", api_key=KEY).runtime_config().endpoint_policy is None
    assert ManagedModel(model=MODEL_ID, base_url=PUBLIC).runtime_config().endpoint_policy == "public"


def test_a_listed_private_endpoint_serves_a_custom_model(deployment, client, stubs, monkeypatch):
    listed, other_port = stubs(), stubs()
    _tenant_written(monkeypatch, listed)

    assert client.post("/api/managed-models/test", json=_draft(listed.base_url())).json() == {"ok": True, "message": "success"}
    discovered = client.post("/api/managed-models/discover", json=_connection(listed.base_url()))
    assert discovered.status_code == 200, discovered.text
    assert discovered.json()["models"] == [{"id": MODEL_ID, "configured": False}]
    assert client.put("/api/managed-models", json=_draft(listed.base_url(), name="records-model")).status_code == 200
    assert [call().content for call in _calls(_model())] == [ANSWER, ANSWER]

    assert _refused(client.put("/api/managed-models", json=_draft(other_port.base_url(), name="ledger-model")))
    assert _refused(client.post("/api/managed-models/test", json=_draft(other_port.base_url())))
    assert _refused(client.post("/api/managed-models/discover", json=_connection(other_port.base_url())))
    assert other_port.connections == 0


def test_an_earlier_stored_model_keeps_the_list_and_the_configuration_readable(deployment, client, stubs, monkeypatch):
    service = stubs()
    private = _store(deployment, service.base_url(), name="records-model")
    _store(deployment, PUBLIC, name="ledger-model")
    _tenant_written(monkeypatch)

    listed = client.get("/api/managed-models")
    assert listed.status_code == 200, listed.text
    assert {item["name"]: item["base_url"] for item in listed.json()["models"]} == {"records-model": service.base_url(), "ledger-model": PUBLIC}
    assert {name: get_app_config().get_model_config(name) is not None for name in ("records-model", "ledger-model")} == {"records-model": True, "ledger-model": True}

    def edit(revision: str, **fields):
        return client.put("/api/managed-models", json={"config": {"name": "records-model", "model": MODEL_ID, "base_url": service.base_url(), **fields}, "expected_revision": revision})

    # Kept at its address it can be switched off, not saved switched on; a public address is accepted.
    assert _refused(edit(private.revision))
    switched_off = edit(private.revision, enabled=False)
    assert switched_off.status_code == 200, switched_off.text
    assert _refused(edit(switched_off.json()["revision"]))
    moved = edit(switched_off.json()["revision"], base_url=PUBLIC)
    assert moved.status_code == 200, moved.text
    assert service.connections == 0


def test_a_model_class_that_cannot_take_the_clients_is_not_built(deployment, monkeypatch):
    deployment.config.write_text(
        "sandbox:\n  use: test\nmodels:\n  - name: fixed-model\n    use: langchain_core.language_models.fake_chat_models:FakeListChatModel\n    model: fixed\n    responses: [fixed answer]\n    endpoint_policy: public\n",
        encoding="utf-8",
    )
    reset_app_config()
    assert create_chat_model("fixed-model", attach_tracing=False).invoke("hello").content == "fixed answer"

    _tenant_written(monkeypatch)
    with pytest.raises(ValueError, match="cannot use the deployment's HTTP clients") as refused:
        create_chat_model("fixed-model", attach_tracing=False)
    assert "http" not in str(refused.value).replace("HTTP clients", "")


def test_deployments_without_the_setting_are_unchanged(deployment, client, stubs):
    service = stubs()

    assert client.post("/api/managed-models/test", json=_draft(service.base_url())).json() == {"ok": True, "message": "success"}
    assert client.post("/api/managed-models/discover", json=_connection(service.base_url())).json()["models"] == [{"id": MODEL_ID, "configured": False}]
    assert client.put("/api/managed-models", json=_draft(service.base_url(), name="records-model")).status_code == 200
    imported = client.post("/api/managed-models/import", json={"connection": _connection(service.base_url()), "model": "second-model"})
    assert imported.json()["status"] == "saved", imported.text

    model = _model()
    assert (model.http_client, model.http_async_client) == (None, None)
    assert [call().content for call in _calls(model)] == [ANSWER, ANSWER]
