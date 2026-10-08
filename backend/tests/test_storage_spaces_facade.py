"""Neutral extension capability bound to host identity, never a claimed owner."""

from types import SimpleNamespace

import pytest
import pytest_asyncio
from _storage_spaces_test_support import ALICE, BOB, home, make_storage_fixture, operation
from deerflow_extension_api import StorageActor, StorageCapabilities
from deerflow_extension_api.plugins import ActionContext, ToolContext

from deerflow.runtime.user_context import reset_current_user, set_current_user
from deerflow.spaces.contract import InvalidPrincipal, Permission, PrincipalRef, ResolvedPrincipal, SpaceDenied
from deerflow.spaces.facade import HostStorageProvider, storage_actor_scope
from deerflow.spaces.principals import HostPrincipalResolver


@pytest_asyncio.fixture(params=["sqlite", "postgres"])
async def storage(tmp_path, request):
    async for value in make_storage_fixture(tmp_path, request):
        yield value


def test_actor_contract_does_not_invent_a_human_user_and_contexts_are_additive():
    actor = StorageActor("nonhuman", "worker:wiki")
    assert actor.kind == "nonhuman" and not hasattr(actor, "user_id")
    with pytest.raises(ValueError):
        StorageActor("human", "../owner")
    old = ActionContext(SimpleNamespace(user_id="alice"), {})
    tool = ToolContext(old.principal, {}, "conversation")
    assert old.actor is None and old.storage is None and tool.thread_id == "conversation"
    assert not StorageCapabilities().available


@pytest.mark.asyncio
async def test_facade_has_no_absent_identity_or_default_user_fallback(storage):
    files, _, _ = storage
    provider = HostStorageProvider(lambda: files)
    with pytest.raises(InvalidPrincipal):
        await provider.current()
    token = set_current_user(SimpleNamespace(id=ALICE.subject_id))
    try:
        facade = await provider.current()
        resource = await facade.provision(name="personal", custody="personal")
        assert resource.custody == "personal" and resource.custodian == StorageActor("human", "alice")
        await facade.write(space_id=resource.id, generation=1, operation_id=operation(), path="page", content=b"first", create=True)
        assert await facade.read(space_id=resource.id, path="page", max_bytes=100) == b"first"
        with pytest.raises(TypeError):
            await facade.read(space_id=resource.id, path="page", max_bytes=100, actor=BOB)
    finally:
        reset_current_user(token)


@pytest.mark.asyncio
async def test_host_validated_nonhuman_can_work_without_human_or_thread_impersonation(storage):
    files, _, _ = storage
    agent = PrincipalRef("nonhuman", "worker:wiki")

    async def lookup(reference):
        return ResolvedPrincipal(reference) if reference == agent else None

    previous = files.registry._resolver
    files.registry._resolver = HostPrincipalResolver(human=previous.resolve, nonhuman=lookup)
    provider = HostStorageProvider(lambda: files)
    with storage_actor_scope(agent):
        facade = await provider.current()
        resource = await facade.provision(name="wiki", custody="personal")
        assert facade.actor == StorageActor("nonhuman", "worker:wiki")
        assert resource.custodian == facade.actor
        await facade.mkdir(space_id=resource.id, generation=1, operation_id=operation(), path="pages")
        await facade.write(space_id=resource.id, generation=1, operation_id=operation(), path="pages/home.md", content=b"retained", create=True)
        assert await facade.read(space_id=resource.id, path="pages/home.md", max_bytes=100) == b"retained"
    with pytest.raises(InvalidPrincipal):
        await provider.current()


@pytest.mark.asyncio
async def test_retired_identity_and_current_grants_are_checked_for_every_facade_call(storage):
    files, _, _ = storage
    space = await home(files)
    await files.write(actor=ALICE, space_id=space.id, expected_generation=1, operation_id=operation(), path="page", content=b"private", create=True)
    await files.registry.set_grant(actor=ALICE, space_id=space.id, expected_generation=1, subject=BOB, permissions=Permission.READ, acknowledge_existing_data=True)
    with storage_actor_scope(BOB):
        facade = await HostStorageProvider(lambda: files).current()
    with storage_actor_scope(BOB):
        assert await facade.read(space_id=space.id, path="page", max_bytes=100) == b"private"
    await files.registry.set_grant(actor=ALICE, space_id=space.id, expected_generation=2, subject=BOB, permissions=Permission(0))
    with storage_actor_scope(BOB), pytest.raises(SpaceDenied):
        await facade.read(space_id=space.id, path="page", max_bytes=100)

    async def retired(reference):
        return None

    files.registry._resolver = HostPrincipalResolver(human=retired)
    with storage_actor_scope(BOB), pytest.raises(InvalidPrincipal):
        await facade.list()


@pytest.mark.asyncio
async def test_unsupported_storage_is_explicit_and_never_an_empty_success():
    provider = HostStorageProvider(lambda: None)
    assert provider.capabilities.available is False
    with storage_actor_scope(ALICE), pytest.raises(NotImplementedError):
        await provider.current()


def test_plugin_storage_and_nonhuman_support_require_explicit_new_contract():
    from deerflow_extension_api.plugins import BackendAction, PluginContribution
    from deerflow_extension_api.storage import StorageController

    from deerflow.extensions.registry import ExtensionRegistry

    async def action(payload, context):
        return {}

    for version in (1, 2, 3):
        registry = ExtensionRegistry()
        with registry.attributed_to("installed"):
            with pytest.raises(ValueError, match="v4"):
                registry.plugin(PluginContribution(namespace="storage.wiki", title="Wiki", backend=(BackendAction("put", action),), api_version=version, storage_api_version=1))
        assert not registry.build().plugins
    registry = ExtensionRegistry()
    with registry.attributed_to("installed"):
        assert registry.plugin(
            PluginContribution(namespace="storage.wiki", title="Wiki", backend=(BackendAction("put", action),), api_version=4, storage_api_version=1, actor_kinds=("human", "nonhuman"), storage_controller=StorageController("wiki", 1))
        )


@pytest.mark.asyncio
async def test_mediated_facade_requires_bound_current_controller_and_operate_grant(storage):
    from deerflow_extension_api import StorageController

    files, _, _ = storage
    enabled = True

    async def installed():
        return enabled

    provider = HostStorageProvider(lambda: files, namespace="storage.shared", controller=StorageController("publication", 1), controller_admitted=installed)
    with storage_actor_scope(ALICE):
        facade = await provider.current()
        space = await facade.provision(name="Shared", custody="company", mode="mediated")
        with pytest.raises(SpaceDenied):
            await facade.write(space_id=space.id, generation=1, operation_id=operation(), path="publication", content=b"raw", create=True)
        await facade.mediated_write(space_id=space.id, generation=1, operation_id=operation(), path="publication", content=b"admitted", create=True)
        await facade.mediated_mkdir(space_id=space.id, generation=1, operation_id=operation(), path="folder")
        await facade.mediated_rename(space_id=space.id, generation=1, operation_id=operation(), path="publication", destination="folder/publication")
        assert await facade.read(space_id=space.id, path="folder/publication", max_bytes=100) == b"admitted"
        enabled = False
        with pytest.raises(SpaceDenied, match="controller"):
            await facade.mediated_remove(space_id=space.id, generation=1, operation_id=operation(), path="folder/publication")
        assert await facade.read(space_id=space.id, path="folder/publication", max_bytes=100) == b"admitted"
        enabled = True
        wrong = await HostStorageProvider(lambda: files, namespace="storage.other", controller=StorageController("publication", 1), controller_admitted=installed).current()
        with pytest.raises(SpaceDenied, match="controller"):
            await wrong.mediated_remove(space_id=space.id, generation=1, operation_id=operation(), path="folder/publication")


@pytest.mark.asyncio
async def test_controller_loss_between_admissions_retains_intent_and_performs_no_write(storage):
    from deerflow_extension_api import StorageController, StorageOperationPending

    files, _, _ = storage
    checks = 0

    async def enabled():
        nonlocal checks
        checks += 1
        return checks < 3  # provision, first admission, then disabled

    provider = HostStorageProvider(lambda: files, namespace="storage.shared", controller=StorageController("publication"), controller_admitted=enabled)
    with storage_actor_scope(ALICE):
        facade = await provider.current()
        space = await facade.provision(name="Shared", custody="company", mode="mediated")
        with pytest.raises(StorageOperationPending):
            await facade.mediated_write(space_id=space.id, generation=1, operation_id=operation(), path="page", content=b"not-published", create=True)
        status = await facade.recovery_status(space_id=space.id)
        assert status["operations"][0]["phase"] == "pending"


@pytest.mark.asyncio
async def test_public_facade_errors_are_distinct_and_do_not_require_harness_imports(storage):
    from deerflow_extension_api import StorageAccessDenied, StorageConflict, StorageUnsupported

    files, _, _ = storage
    space = await home(files)
    with storage_actor_scope(BOB):
        facade = await HostStorageProvider(lambda: files).current()
        with pytest.raises(StorageAccessDenied):
            await facade.read(space_id=space.id, path="page", max_bytes=100)
    with storage_actor_scope(ALICE):
        facade = await HostStorageProvider(lambda: files).current()
        with pytest.raises(StorageConflict):
            await facade.mkdir(space_id=space.id, generation=2, operation_id=operation(), path="page")
        with pytest.raises(StorageUnsupported):
            await HostStorageProvider(lambda: None).current()


@pytest.mark.asyncio
async def test_controlled_copy_preserves_source_export_and_disclosure_admission(storage):
    from deerflow_extension_api import ResourceReference, StorageAccessDenied, StorageController

    files, _, _ = storage

    async def installed():
        return True

    with storage_actor_scope(ALICE):
        facade = await HostStorageProvider(lambda: files, namespace="storage.shared", controller=StorageController("publication"), controller_admitted=installed).current()
        source = await facade.provision(name="Private", custody="personal")
        destination = await facade.provision(name="Shared", custody="company", mode="mediated")
        await facade.write(space_id=source.id, generation=1, operation_id=operation(), path="page", content=b"private", create=True)
        await facade.grant(space_id=destination.id, generation=1, subject=StorageActor("human", "bob"), permissions=1, acknowledge_existing_data=True)
        params = dict(source=ResourceReference(source.id, "page"), destination=ResourceReference(destination.id, "page"), source_generation=1, destination_generation=2)
        with pytest.raises(StorageAccessDenied):
            await facade.mediated_copy(**params, operation_id=operation())
        await facade.mediated_copy(**params, operation_id=operation(), acknowledge_disclosure=True)
        assert await facade.read(space_id=destination.id, path="page", max_bytes=100) == b"private"


@pytest.mark.asyncio
async def test_captured_facade_cannot_cross_account_context(storage):
    files, _, _ = storage
    with storage_actor_scope(ALICE):
        captured = await HostStorageProvider(lambda: files).current()
    with storage_actor_scope(BOB), pytest.raises(InvalidPrincipal, match="binding changed"):
        await captured.list()


@pytest.mark.asyncio
async def test_service_dependency_is_lazy_and_preserves_existing_service_lifecycle(storage):
    from deerflow.extensions.gateway import start_services
    from deerflow.extensions.registry import ExtensionRegistry

    files, _, _ = storage
    state = SimpleNamespace(files=None)
    provider = HostStorageProvider(lambda: state.files)

    class Service:
        async def start(self, deps):
            self.provider = deps.storage
            assert not self.provider.capabilities.available

        async def stop(self):
            pass

    service = Service()
    registry = ExtensionRegistry()
    with registry.attributed_to("installed"):
        registry.service(service)
    assert await start_services(registry.build(), SimpleNamespace(), None, storage=provider) == []
    state.files = files
    with storage_actor_scope(ALICE):
        assert (await service.provider.current()).actor == StorageActor("human", "alice")


@pytest.mark.asyncio
async def test_real_storage_tool_uses_host_nonhuman_actor_and_rejects_claimed_runtime_user(storage):
    import json

    from deerflow_extension_api.plugins import ModelTool, PluginContribution
    from langchain_core.messages import AIMessage
    from langgraph.graph import END, START, MessagesState, StateGraph
    from langgraph.prebuilt import ToolNode

    from deerflow.extensions.plugin_tools import build_plugin_tools
    from deerflow.extensions.registry import ExtensionRegistry

    files, _, _ = storage
    agent = PrincipalRef("nonhuman", "worker:wiki")
    previous = files.registry._resolver

    async def lookup(reference):
        return ResolvedPrincipal(reference) if reference == agent else None

    files.registry._resolver = HostPrincipalResolver(human=previous.resolve, nonhuman=lookup)
    calls = []

    async def inspect(payload, context):
        calls.append(context)
        return {"kind": context.actor.kind, "id": context.actor.subject_id, "count": len(await context.storage.list())}

    plugin = PluginContribution(
        namespace="storage.test",
        title="Test",
        enabled=True,
        api_version=4,
        storage_api_version=1,
        actor_kinds=("human", "nonhuman"),
        tools=(ModelTool("inspect", "Inspect resources", {"type": "object", "additionalProperties": False}, inspect),),
    )
    registry = ExtensionRegistry()
    with registry.attributed_to("installed"):
        registry.plugin(plugin)
    loaded = registry.build()
    (tool,) = build_plugin_tools(loaded)
    graph = StateGraph(MessagesState)
    graph.add_node("tools", ToolNode([tool]))
    graph.add_edge(START, "tools")
    graph.add_edge("tools", END)
    compiled = graph.compile()
    from deerflow.spaces.facade import STORAGE_PROVIDER_CONTEXT_KEY

    async def invoke():
        return (await compiled.ainvoke({"messages": [AIMessage(content="", tool_calls=[{"id": "call", "name": tool.name, "args": {}}])]}, context={"user_id": "alice", STORAGE_PROVIDER_CONTEXT_KEY: HostStorageProvider(lambda: files)}))[
            "messages"
        ][-1]

    assert (await invoke()).status == "error"
    assert not calls
    with storage_actor_scope(agent):
        result = await invoke()
    assert json.loads(result.content) == {"kind": "nonhuman", "id": "worker:wiki", "count": 0}
    assert calls[0].principal is None and calls[0].actor.subject_id == "worker:wiki"


def test_worker_does_not_accept_payload_as_storage_provider():
    from deerflow.runtime.runs.worker import _build_runtime_context
    from deerflow.spaces.facade import STORAGE_PROVIDER_CONTEXT_KEY

    assert STORAGE_PROVIDER_CONTEXT_KEY not in _build_runtime_context("thread", "run", {STORAGE_PROVIDER_CONTEXT_KEY: {"user_id": "alice"}})


@pytest.mark.asyncio
async def test_http_action_binds_host_storage_and_rechecks_resource_scope(storage, monkeypatch):
    import httpx
    from deerflow_extension_api import StorageController
    from deerflow_extension_api.auth import EXTENSION_PRINCIPAL_RESOLVER_KEY, ExtensionPrincipal
    from deerflow_extension_api.plugins import BackendAction, PluginContribution
    from fastapi import FastAPI

    from app.gateway.routers.plugins import router
    from deerflow.extensions.registry import ExtensionRegistry

    files, _, _ = storage
    resource = await home(files)
    calls = []

    async def inspect(payload, context):
        calls.append(context)
        return {"actor": context.actor.subject_id, "resource": context.resource.id if context.resource else None}

    async def authorize(*args, **kwargs):
        pass

    monkeypatch.setattr("app.gateway.authz.authorize_plugin_action_for_request", authorize)
    plugin = PluginContribution(namespace="storage.test", title="Test", enabled=True, api_version=4, storage_api_version=1, storage_controller=StorageController("publication"), backend=(BackendAction("inspect", inspect),))
    registry = ExtensionRegistry()
    with registry.attributed_to("installed"):
        registry.plugin(plugin)
    app = FastAPI()
    app.state.extensions = registry.build()
    app.state.extension_storage = HostStorageProvider(lambda: files)
    app.include_router(router)
    setattr(app.state, EXTENSION_PRINCIPAL_RESOLVER_KEY, lambda request: ExtensionPrincipal(request.state.user.id))
    caller = {"id": "alice", "source": "session"}

    @app.middleware("http")
    async def identity(request, call_next):
        request.state.user = SimpleNamespace(id=caller["id"])
        request.state.auth_source = caller["source"]
        return await call_next(request)

    url = "/api/plugins/storage.test/actions/inspect"
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        (descriptor,) = (await client.get("/api/plugins")).json()
        assert descriptor["storage_capabilities"]["mediated_mutations"] is True
        files.attachments = object()
        assert app.state.extension_storage.capabilities.native_attachments is False
        response = await client.post(url, json={"actor": "bob"}, headers={"X-Deerflow-Resource": resource.id})
        assert response.status_code == 200, response.text
        assert response.json() == {"actor": "alice", "resource": resource.id}
        assert calls[0].storage.actor == StorageActor("human", "alice")
        assert descriptor["storage_capabilities"]["mediated_mutations"] == calls[0].storage.capabilities.mediated_mutations
        caller["id"] = "bob"
        assert (await client.post(url, json={}, headers={"X-Deerflow-Resource": resource.id})).status_code == 404
        caller["id"], caller["source"] = "alice", "pat"
        assert (await client.post(url, json={})).status_code == 403
        assert len(calls) == 1
