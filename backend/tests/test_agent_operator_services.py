"""One-shot operator services share domain policy without Gateway startup."""

from types import SimpleNamespace

import pytest


def test_service_construction_neither_queries_nor_allocates_nor_starts_runtime(tmp_path):
    from app.gateway.storage_spaces import build_storage_services

    def unexpected_session():
        raise AssertionError("Construction cannot open a SQL session")

    repository = SimpleNamespace()
    catalog = SimpleNamespace(volumes={})
    config = SimpleNamespace(inventory_path=str(tmp_path / "inventory.v2.json"), user_spaces=False)
    files, agents = build_storage_services(config, catalog, unexpected_session, repository)

    assert files.registry._sf is unexpected_session
    assert files.catalog is catalog and files.user_spaces is False
    assert agents.files is files and agents.directory.session_factory is unexpected_session
    assert files.bound_slots == files.unclaimed_slots == {}
    assert files.workflow_settlers == {}
    assert not hasattr(files, "_pool_runtime_owner")
    assert files.attachment_guard is None


@pytest.mark.asyncio
async def test_disabled_storage_refuses_before_sql_or_docker_startup(monkeypatch):
    from app.gateway import agent_operator_services as module
    from app.gateway.agent_operator_contract import CommandRefused

    monkeypatch.setattr("deerflow.config.get_app_config", lambda: SimpleNamespace(storage_spaces=SimpleNamespace(enabled=False)))

    async def unexpected(*_args):
        pytest.fail("Storage-off commands cannot start the persistence engine")

    monkeypatch.setattr("deerflow.persistence.engine.init_engine_from_config", unexpected)
    with pytest.raises(CommandRefused, match="Storage Spaces are off in the rendered configuration"):
        await module.run("list", all=False)


@pytest.mark.asyncio
@pytest.mark.parametrize("command,discard", [("list", False), ("status", False), ("verify", False), ("supervise", False), ("settle", False), ("settle", True)])
async def test_operator_factory_is_observational_and_opens_native_evidence_only_on_containment(tmp_path, monkeypatch, command, discard):
    from app.gateway import agent_operator_services as module

    calls = []

    def unexpected(*_args, **_kwargs):
        pytest.fail("An isolated operator factory must not initialize SQL, native evidence or the Gateway")

    config = SimpleNamespace(storage_spaces=SimpleNamespace(inventory_path=str(tmp_path / "inventory.v2.json"), user_spaces=True))
    catalog = SimpleNamespace(volumes={})
    adapter = SimpleNamespace(host_id="actual-host")

    def qualify(_config, *, probe):
        calls.append(probe)
        return adapter, None

    monkeypatch.setattr("app.gateway.storage_spaces._read_pool", lambda _config: (catalog, [], None, None, None))
    monkeypatch.setattr("app.gateway.storage_spaces.initialize_storage_spaces", unexpected)
    monkeypatch.setattr("deerflow.community.aio_sandbox.aio_sandbox_provider.AioSandboxProvider.__init__", unexpected)
    monkeypatch.setattr("deerflow.spaces.native_create_evidence.NativeEvidenceReader", unexpected)
    monkeypatch.setattr("deerflow.config.paths.get_paths", lambda: SimpleNamespace(base_dir=tmp_path))
    monkeypatch.setattr(module, "_operator_adapter", qualify)
    async with module.operator_services(config, command=command, session_factory=unexpected, discard=discard) as service:
        assert service.files.catalog is catalog and service.files.bound_slots == {}
        assert service.attachments is None
        if command == "supervise":
            await service._attachment_factory()
            assert service.attachments is service.files.attachments
    assert calls == ([] if command == "settle" and not discard else [command in ("status", "verify")])
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("probe,failed", [(False, False), (True, False), (True, True)])
def test_operator_adapter_uses_one_probe_and_retains_host_observation_if_it_fails(tmp_path, monkeypatch, probe, failed):
    from app.gateway.agent_operator_services import _operator_adapter

    calls, baseline, qualified = [], SimpleNamespace(host_id="actual-host"), SimpleNamespace(host_id="actual-host")
    backend = object()
    monkeypatch.setattr("deerflow.community.aio_sandbox.local_backend.LocalContainerBackend", lambda **_kwargs: backend)

    def for_operator(actual, *, pool, probe_pool=False):
        assert actual is backend and pool == tmp_path
        calls.append(probe_pool)
        if probe_pool and failed:
            raise OSError("Probe is unavailable")
        return qualified if probe_pool else baseline

    monkeypatch.setattr("deerflow.spaces.docker.DockerStorageAdapter.for_operator", for_operator)
    sandbox = SimpleNamespace(image=None, port=None, container_prefix=None, mounts=[], environment={}, network=SimpleNamespace(model_dump=lambda: {}))
    config = SimpleNamespace(sandbox=sandbox, storage_spaces=SimpleNamespace(inventory_path=str(tmp_path / "inventory.v2.json")))
    adapter, error = _operator_adapter(config, probe=probe)
    assert calls == ([False, True] if probe else [False])
    assert adapter is (qualified if probe and not failed else baseline)
    assert isinstance(error, OSError) if failed else error is None
