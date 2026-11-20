"""Custom model endpoints on deployments whose connections are written by tenants.

With ``DEER_FLOW_TENANT_WRITTEN_ENDPOINTS=on`` the address of a custom model, the
one kind whose address an administrator chooses, must be a public HTTP(S)
address or a provider-listed private endpoint when it is saved, tested,
discovered or imported, and again at every connection. Models of the provider's
own configuration and of the fixed catalog are untouched unless their profile
sets the policy field; the text of a build error is the same for every model.

Every test drives the real routes, the real store with its encryption, the
product's own ``get_app_config()`` and ``create_chat_model``, and a real call of
the model over a socket to a loopback stub. None constructs a model class by
hand or replaces an HTTP client, its transport or the address check.
"""

from __future__ import annotations

import asyncio
import logging
import socket
import ssl
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import ValidationError
from support import loopback_only
from support.model_endpoint_stubs import ANSWER, MODEL_ID, ModelEndpointStub

from app.gateway.routers import managed_models as managed_models_router
from deerflow.config.app_config import get_app_config, reset_app_config
from deerflow.config.managed_models import ManagedModel, ManagedModelStore
from deerflow.mcp import personal_network
from deerflow.models.factory import create_chat_model

LINE = "DEER_FLOW_TENANT_WRITTEN_ENDPOINTS"
PRIVATE_ENDPOINTS = "DEER_FLOW_PRIVATE_ENDPOINTS"
# Settings a developer's environment may carry that the code under test reads.
_AMBIENT_SETTINGS = (
    "OPENAI_BASE_URL",
    "OPENAI_API_BASE",
    "OPENAI_PROXY",
    "OPENAI_ORG_ID",
    "OPENAI_ORGANIZATION",
    "OPENAI_PROJECT_ID",
    "OPENAI_WEBHOOK_SECRET",
    "OPENAI_API_KEY",
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
    "LANGCHAIN_OPENAI_TCP_KEEPALIVE",
)
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
    # Nothing here depends on what the developer's own environment sets.
    for name in (LINE, PRIVATE_ENDPOINTS, *_AMBIENT_SETTINGS):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(managed_models, "_cache", None)
    personal_network._supported_model_socket_options.cache_clear()
    reset_app_config()
    reset_extensions_config()
    yield SimpleNamespace(config=config, store=ManagedModelStore())
    reset_app_config()
    reset_extensions_config()
    personal_network._supported_model_socket_options.cache_clear()


@pytest.fixture(autouse=True)
def network(monkeypatch):
    """No test here leaves the loopback interface, whatever the code under test does."""
    guard = loopback_only.install(monkeypatch)
    yield guard
    guard.assert_nothing_blocked()


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


def _streamed_calls(model) -> list:
    async def streamed() -> list:
        return [chunk async for chunk in model.astream("hello")]

    return [lambda: list(model.stream("hello")), lambda: asyncio.run(streamed())]


@pytest.mark.parametrize("address", ["loopback", "private-number", "name-resolving-to-private"])
def test_custom_model_endpoints_must_be_public(address, deployment, client, stubs, network, monkeypatch):
    service = stubs()
    network.names[MADE_UP_NAME] = ["127.0.0.1"]
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
    for call in [*_calls(_model()), *_streamed_calls(_model())]:
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


@pytest.fixture
def socket_settings(monkeypatch):
    """Record the options set on sockets and the server names asked of TLS, calling through."""
    seen = SimpleNamespace(options=[], tls_names=[])
    set_option, wrap_socket, wrap_bio = socket.socket.setsockopt, ssl.SSLContext.wrap_socket, ssl.SSLContext.wrap_bio

    def setsockopt(self, level, option, *value):
        seen.options.append((level, option, *value))
        return set_option(self, level, option, *value)

    def asked_name(kwargs) -> str | None:
        name = kwargs.get("server_hostname")
        return name.decode("ascii") if isinstance(name, bytes) else name

    def recording_wrap_socket(self, *args, **kwargs):
        seen.tls_names.append(asked_name(kwargs))
        return wrap_socket(self, *args, **kwargs)

    def recording_wrap_bio(self, *args, **kwargs):
        seen.tls_names.append(asked_name(kwargs))
        return wrap_bio(self, *args, **kwargs)

    monkeypatch.setattr(socket.socket, "setsockopt", setsockopt)
    monkeypatch.setattr(ssl.SSLContext, "wrap_socket", recording_wrap_socket)
    monkeypatch.setattr(ssl.SSLContext, "wrap_bio", recording_wrap_bio)
    return seen


def test_a_public_custom_model_is_reached_at_the_address_that_was_checked(deployment, stubs, network, socket_settings, monkeypatch):
    """A public address cannot be contacted from a test: a connect to it lands on the stub, over a real socket."""
    service = stubs()
    _store(deployment, f"http://{MADE_UP_NAME}:8443/v1")
    _tenant_written(monkeypatch)
    network.names[MADE_UP_NAME] = ["8.8.8.8"]
    network.routes[("8.8.8.8", 8443)] = ("127.0.0.1", service.port)
    # What the one-time probe of the options set is not what a connection was given.
    personal_network._supported_model_socket_options()
    socket_settings.options.clear()

    assert [call().content for call in _calls(_model())] == [ANSWER, ANSWER]

    assert network.routed == [("8.8.8.8", 8443)] * 2
    # One lookup for each request: connecting by name instead would look it up again.
    assert network.answers(MADE_UP_NAME) == ["8.8.8.8"] * 2
    assert {request.headers["host"] for request in service.requests} == {f"{MADE_UP_NAME}:8443"}
    # A silent peer is noticed as with the clients the model library builds itself, and nothing stays open.
    assert (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1) in socket_settings.options
    assert service.settled_open_connections() == 0


def test_a_public_custom_model_over_tls_keeps_its_name_at_the_checked_address(deployment, stubs, network, socket_settings, monkeypatch):
    """The stub does not speak TLS, so the call ends at the handshake: where it went and for which name is what is shown."""
    service = stubs()
    _store(deployment, f"https://{MADE_UP_NAME}:8443/v1")
    _tenant_written(monkeypatch)
    network.names[MADE_UP_NAME] = ["8.8.8.8"]
    network.routes[("8.8.8.8", 8443)] = ("127.0.0.1", service.port)

    for call in _calls(_model()):
        with pytest.raises(Exception):
            call()

    assert network.routed == [("8.8.8.8", 8443)] * 2
    # One lookup for each request: connecting by name instead would look it up again.
    assert network.answers(MADE_UP_NAME) == ["8.8.8.8"] * 2
    assert socket_settings.tls_names == [MADE_UP_NAME] * 2
    assert service.calls() == []


def test_connection_options_follow_the_library_switch_and_what_the_system_supports(deployment, stubs, socket_settings, monkeypatch):
    service = stubs()
    _store(deployment, service.base_url())
    _tenant_written(monkeypatch, service)
    refused_option = getattr(socket, "TCP_USER_TIMEOUT", None)
    set_option = socket.socket.setsockopt

    def setsockopt(self, level, option, *value):
        # A system that does not know one of the options.
        if option == refused_option and level == socket.IPPROTO_TCP:
            raise OSError("Protocol not available")
        return set_option(self, level, option, *value)

    monkeypatch.setattr(socket.socket, "setsockopt", setsockopt)
    personal_network._supported_model_socket_options.cache_clear()
    assert [call().content for call in _calls(_model())] == [ANSWER, ANSWER]
    assert refused_option not in {option for _level, option, *_value in personal_network._supported_model_socket_options()}

    # The model library's own switch turns the options off here too.
    monkeypatch.setenv("LANGCHAIN_OPENAI_TCP_KEEPALIVE", "0")
    socket_settings.options.clear()
    assert [call().content for call in _calls(_model())] == [ANSWER, ANSWER]
    assert (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1) not in socket_settings.options


@pytest.mark.parametrize("refusal", [OSError("no socket for the probe"), RuntimeError("no socket for the probe")], ids=["system-error", "another-error"])
def test_a_probe_that_could_not_run_is_run_again(refusal, deployment, stubs, socket_settings, monkeypatch):
    service = stubs()
    _store(deployment, service.base_url())
    _tenant_written(monkeypatch, service)
    refusals = [refusal]

    class OneRefusal:
        """The socket module as the clients see it, refusing one socket and then none."""

        def __getattr__(self, name):
            return getattr(socket, name)

        def socket(self, *args, **kwargs):
            if refusals:
                raise refusals.pop()
            return socket.socket(*args, **kwargs)

    monkeypatch.setattr(personal_network, "socket", OneRefusal())
    model = _model()
    # Building a model makes the library try the options on a socket of its own.
    socket_settings.options.clear()

    # The request whose probe could not run goes out without the options; the next one has them.
    assert model.invoke("hello").content == ANSWER
    assert refusals == [] and (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1) not in socket_settings.options
    assert model.invoke("hello").content == ANSWER
    assert (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1) in socket_settings.options


def test_proxy_settings_of_the_environment_are_not_used(deployment, stubs, monkeypatch):
    service, proxy = stubs(), stubs()
    _store(deployment, service.base_url())
    _tenant_written(monkeypatch, service)
    for name in ("NO_PROXY", "no_proxy"):
        monkeypatch.delenv(name)
    for name in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy", "OPENAI_PROXY"):
        monkeypatch.setenv(name, f"http://proxy-user:typed-proxy-word@{proxy.host_port}")

    assert [call().content for call in _calls(_model())] == [ANSWER, ANSWER]

    assert proxy.connections == 0


@pytest.mark.parametrize("policy_field", [[], ["endpoint_policy: public"]], ids=["provider-model", "provider-model-with-the-policy-field"])
def test_a_model_that_cannot_be_built_says_nothing_of_its_settings(policy_field, deployment, stubs, monkeypatch, caplog):
    service = stubs()
    caplog.set_level(logging.INFO, logger="deerflow")
    monkeypatch.setenv("HM_TEST_VALUE", "plain-marker-word")
    # A provider's own profile with a setting its class refuses.
    profile = ["name: provider-model", "use: langchain_openai:ChatOpenAI", f"model: {MODEL_ID}", f"base_url: {service.base_url()}", f"api_key: {KEY}", "max_retries: $HM_TEST_VALUE", *policy_field]
    deployment.config.write_text("sandbox:\n  use: test\nmodels:\n  - " + "\n    ".join(profile) + "\n", encoding="utf-8")
    reset_app_config()

    # Without the setting the class's own error is shown, as it always was.
    with pytest.raises(ValidationError, match="plain-marker-word"):
        create_chat_model("provider-model", attach_tracing=False)

    _tenant_written(monkeypatch, service)
    with pytest.raises(ValueError) as failure:
        create_chat_model("provider-model", attach_tracing=False)

    assert str(failure.value) == "This model could not be built"
    assert failure.value.__cause__ is None and failure.value.__suppress_context__
    # The log has the kind of error and the field that failed, not what the field held.
    factory_records = [(record.levelno, record.getMessage()) for record in caplog.records if record.name == "deerflow.models.factory"]
    assert factory_records == [(logging.ERROR, "Model 'provider-model' could not be built (ValidationError: max_retries)")]
    for value in ("plain-marker-word", KEY, service.base_url()):
        assert value not in caplog.text


def test_a_caller_cannot_put_another_client_in_their_place(deployment, stubs, monkeypatch):
    service = stubs()
    _store(deployment, service.base_url())
    _tenant_written(monkeypatch)

    ready_made = object()
    with httpx.Client(trust_env=False) as plain:
        supplied = {"http_client": plain, "http_async_client": httpx.AsyncClient(trust_env=False), "client": ready_made, "async_client": ready_made, "root_client": ready_made, "root_async_client": ready_made}
        model = create_chat_model("records-model", attach_tracing=False, max_retries=0, timeout=5, **supplied)
        assert model.http_client is not plain
        assert ready_made not in (model.client, model.async_client, model.root_client, model.root_async_client)
        for call in [*_calls(model), *_streamed_calls(model)]:
            with pytest.raises(Exception) as failure:
                call()
            assert str(failure.value.__cause__) == "Custom model endpoints require a public HTTP(S) endpoint"

    assert service.connections == 0


def test_a_probe_keeps_the_policy_of_the_model_it_builds(deployment, client, stubs, network, monkeypatch):
    """The name answers with a public address first and with the loopback address afterwards."""
    service = stubs()
    _tenant_written(monkeypatch)
    network.names[MADE_UP_NAME] = ["8.8.8.8", "127.0.0.1"]

    response = client.post("/api/managed-models/test", json=_draft(service.base_url(host=MADE_UP_NAME)))

    assert response.json() == {"ok": False, "message": "connection_failed"}
    assert network.answers(MADE_UP_NAME)[:2] == ["8.8.8.8", "127.0.0.1"]
    assert service.connections == 0


def test_the_endpoint_policy_is_not_a_model_parameter(deployment, monkeypatch):
    from deerflow.agents.assembly_descriptor import _model_parameters

    _store(deployment, PUBLIC)
    profile = get_app_config().get_model_config("records-model")
    assert profile.endpoint_policy == "public"

    assert "endpoint_policy" not in _model_parameters(profile)
    for setting in (None, "on"):
        if setting:
            _tenant_written(monkeypatch)
        assert "endpoint_policy" not in _model().model_kwargs


def test_a_custom_model_keeps_its_time_limit_and_its_retry(deployment, stubs, monkeypatch):
    service = stubs()
    _store(deployment, service.base_url())
    _tenant_written(monkeypatch, service)

    for call_with in (lambda model: model.invoke("hello"), lambda model: asyncio.run(model.ainvoke("hello"))):
        answered = len(service.calls())
        # The first answer comes later than the model's own time limit; its one retry is answered.
        service.stall_next_seconds = 3.0
        model = create_chat_model("records-model", attach_tracing=False, max_retries=1, timeout=1)
        assert call_with(model).content == ANSWER
        assert len(service.calls()) == answered + 2


def test_the_adapter_of_a_recognised_official_address_uses_the_clients(deployment, network, socket_settings, monkeypatch):
    official = "api.deepseek.com"
    deployment.store.save(ManagedModel(name="records-model", model="deepseek-flash", base_url=f"https://{official}", api_key=KEY), expected_revision=None)
    _tenant_written(monkeypatch)
    profile = get_app_config().get_model_config("records-model")
    assert (profile.use, profile.endpoint_policy) == ("deerflow.models.patched_deepseek:PatchedChatDeepSeek", "public")
    network.names[official] = ["127.0.0.1"]

    # An answer that is not public: the adapter's own calls are refused by the deployment's clients.
    for call in [*_calls(_model()), *_streamed_calls(_model())]:
        with pytest.raises(Exception) as failure:
            call()
        assert str(failure.value.__cause__) == "Custom model endpoints require a public HTTP(S) endpoint"
    assert network.answers(official) == ["127.0.0.1"] * 4

    # A public answer: the call goes to that address for that name (nothing listens there, so it ends at the connect).
    network.lookups.clear()
    network.names[official] = ["8.8.8.8"]
    network.routes[("8.8.8.8", 443)] = ("127.0.0.1", 9)
    for call in _calls(_model()):
        with pytest.raises(Exception):
            call()
    assert network.routed == [("8.8.8.8", 443)] * 2


def test_a_name_that_changes_its_answer_is_not_contacted(deployment, client, stubs, network, monkeypatch):
    service = stubs()
    base_url = service.base_url(host=MADE_UP_NAME)
    _tenant_written(monkeypatch)
    # The name answers with a public address first and with the loopback address afterwards.
    network.names[MADE_UP_NAME] = ["8.8.8.8", "127.0.0.1"]

    response = client.post("/api/managed-models/discover", json=_connection(base_url))
    assert (response.status_code, response.json()["detail"]["code"]) == (502, "discovery_failed")
    assert network.answers(MADE_UP_NAME)[:2] == ["8.8.8.8", "127.0.0.1"]
    assert service.connections == 0

    network.lookups.clear()
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

    # Nor is a redirect that stays on the model's own origin.
    service.redirect_to = f"{service.base_url()}/chat/completions"
    for call in _calls(_model()):
        with pytest.raises(Exception):
            call()
    assert service.calls() == [("POST", "/v1/chat/completions")] * 4


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


def test_a_listed_model_endpoint_is_matched_in_the_host_form_the_client_sends(deployment, client, stubs, network, monkeypatch):
    service = stubs()
    encoded = "xn--bcher-kva.example"
    monkeypatch.setenv(LINE, "on")
    monkeypatch.setenv(PRIVATE_ENDPOINTS, f"{encoded}:{service.port}")
    network.names[encoded] = ["127.0.0.1"]

    for index, typed in enumerate((encoded, "bücher.example")):
        assert client.put("/api/managed-models", json=_draft(service.base_url(host=typed), name=f"records-model-{index}")).status_code == 200
        assert [call().content for call in _calls(_model(f"records-model-{index}"))] == [ANSWER, ANSWER]


@pytest.mark.parametrize("typed", ["models.internal.test ", " models.internal.test", "models.internal.test\t"])
def test_a_model_address_is_judged_in_the_host_form_the_client_connects_to(typed, deployment, client, stubs, network, monkeypatch):
    """A host that an HTTP client would not take for the listed one is not accepted as listed."""
    service = stubs()
    listed = "models.internal.test"
    monkeypatch.setenv(LINE, "on")
    monkeypatch.setenv(PRIVATE_ENDPOINTS, f"{listed}:{service.port}")
    network.names[listed] = ["127.0.0.1"]

    assert _refused(client.put("/api/managed-models", json=_draft(service.base_url(host=typed), name="records-model")))
    assert _refused(client.post("/api/managed-models/test", json=_draft(service.base_url(host=typed))))
    assert _refused(client.post("/api/managed-models/discover", json=_connection(service.base_url(host=typed))))
    assert service.connections == 0
    assert client.put("/api/managed-models", json=_draft(service.base_url(host=listed), name="records-model")).status_code == 200
    assert [call().content for call in _calls(_model())] == [ANSWER, ANSWER]


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


@pytest.fixture(params=["OPENAI_ORG_ID", "OPENAI_ORGANIZATION"])
def library_environment(request, monkeypatch) -> dict[str, str]:
    """Every setting the model library reads from the environment by itself, each with a made-up value.

    The library knows the organization under two names and reads the second
    only when the first is unset, so each is set in a run of its own.
    """
    settings = {
        request.param: "made-up-organization",
        "OPENAI_PROJECT_ID": "made-up-project",
        "OPENAI_WEBHOOK_SECRET": "made-up-webhook-word",
        "OPENAI_API_KEY": "made-up-environment-key",
        "OPENAI_BASE_URL": "http://environment-base.example.test/v1",
        "OPENAI_API_BASE": "http://environment-api-base.example.test/v1",
    }
    for name, value in settings.items():
        monkeypatch.setenv(name, value)
    return settings


@pytest.mark.parametrize("reached", ["listed-endpoint", "public-address"])
def test_a_custom_model_request_carries_no_setting_of_the_environment(reached, library_environment, deployment, client, stubs, network, monkeypatch):
    service = stubs()
    if reached == "listed-endpoint":
        _tenant_written(monkeypatch, service)
        base_url = service.base_url()
    else:
        _tenant_written(monkeypatch)
        network.names[MADE_UP_NAME] = ["8.8.8.8"]
        network.routes[("8.8.8.8", 8443)] = ("127.0.0.1", service.port)
        base_url = f"http://{MADE_UP_NAME}:8443/v1"

    assert client.post("/api/managed-models/test", json=_draft(base_url)).json() == {"ok": True, "message": "success"}
    assert client.post("/api/managed-models/discover", json=_connection(base_url)).status_code == 200
    assert client.put("/api/managed-models", json=_draft(base_url, name="records-model")).status_code == 200
    model = _model()
    for call in [*_calls(model), *_streamed_calls(model)]:
        assert call()

    assert service.calls() == [("POST", "/v1/chat/completions"), ("GET", "/v1/models"), *[("POST", "/v1/chat/completions")] * 4]
    for request in service.requests:
        assert not {"openai-organization", "openai-project"} & set(request.headers)
        # The key that was typed for this model is the one that arrives.
        assert request.headers["authorization"] == f"Bearer {KEY}"
    received = service.received_text()
    assert [value for value in library_environment.values() if value in received] == []


def test_a_custom_model_without_a_key_takes_none_from_the_environment(library_environment, deployment, client, stubs, monkeypatch):
    service = stubs()
    _tenant_written(monkeypatch, service)
    profile = {"model": MODEL_ID, "base_url": service.base_url()}

    assert client.post("/api/managed-models/test", json={"config": profile}).json() == {"ok": True, "message": "success"}
    assert client.post("/api/managed-models/discover", json={"provider": "openai-compatible", "base_url": service.base_url()}).status_code == 200
    assert client.put("/api/managed-models", json={"config": {**profile, "name": "records-model"}}).status_code == 200
    model = _model()
    for call in [*_calls(model), *_streamed_calls(model)]:
        assert call()

    # A model call carries the product's fixed placeholder, the catalog request no key at all.
    assert [(request.method, request.headers.get("authorization")) for request in service.requests] == [("POST", "Bearer not-required"), ("GET", None), *[("POST", "Bearer not-required")] * 4]
    received = service.received_text()
    assert [value for value in library_environment.values() if value in received] == []


@pytest.mark.parametrize("deployment_kind", ["without-the-setting", "provider-model-with-the-setting"])
def test_other_models_keep_the_settings_the_library_reads(deployment_kind, library_environment, deployment, client, stubs, monkeypatch):
    service = stubs()
    if deployment_kind == "without-the-setting":
        assert client.put("/api/managed-models", json=_draft(service.base_url(), name="records-model")).status_code == 200
        model = _model()
    else:
        deployment.config.write_text(
            f"sandbox:\n  use: test\nmodels:\n  - name: provider-model\n    use: langchain_openai:ChatOpenAI\n    model: {MODEL_ID}\n    base_url: {service.base_url()}\n    api_key: {KEY}\n",
            encoding="utf-8",
        )
        _tenant_written(monkeypatch)
        reset_app_config()
        model = _model("provider-model")

    # The library's own asynchronous client stays with the first loop that used it: one call each way.
    assert [call().content for call in _calls(model)] == [ANSWER, ANSWER]

    assert service.calls() == [("POST", "/v1/chat/completions")] * 2
    for request in service.requests:
        assert (request.headers["openai-organization"], request.headers["openai-project"]) == ("made-up-organization", "made-up-project")
        assert request.headers["authorization"] == f"Bearer {KEY}"
