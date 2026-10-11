"""Members publish through the host without receiving company write authority."""

import pytest
from sqlalchemy import select
from test_storage_feature_routes import THREAD
from test_storage_feature_routes import feature_app as feature_app

from deerflow.extensions.loader import load_extensions
from deerflow.features.plugins import FeatureServices, with_default_features
from deerflow.features.resources import StorageFeatureLinkRow
from deerflow.persistence.spaces.files import SpaceFileOperationRow
from deerflow.spaces.contract import Permission, PrincipalRef


def test_shared_plugin_is_host_written_with_no_controller_or_recover_action():
    loaded, diagnostics = load_extensions(with_default_features([]))
    assert not diagnostics
    plugin = next(plugin for _source, plugin in loaded.plugins if plugin.namespace == "hm.shared")
    service = next(service for _source, service in loaded.services if service.namespace == "hm.shared")
    assert service.host_written is True
    assert plugin.storage_controller is None
    assert len(plugin.backend) == 1
    assert not hasattr(service, "recover")


@pytest.mark.asyncio
async def test_share_from_my_files_needs_only_read_export_and_records_host_write(feature_app):
    client, app, files, _catalog, paths, alice, _bob, _current = feature_app
    outputs = paths.sandbox_outputs_dir(THREAD, user_id=str(alice.id))
    outputs.mkdir(parents=True)
    (outputs / "kept.txt").write_bytes(b"publication retained")
    kept = await client.post(f"/api/threads/{THREAD}/files", json={"path": "/mnt/user-data/outputs/kept.txt"})
    assert kept.status_code == 201, kept.text
    sf = files.registry._sf
    async with sf() as session:
        source = (await session.get(StorageFeatureLinkRow, ("hm.user-space", "human:" + str(alice.id)))).space_id
    await files.registry.host_grant(source, PrincipalRef("human", str(alice.id)), Permission.READ | Permission.EXPORT)
    response = await client.post("/api/shared/publish", json={"path": kept.json()["virtual_path"]})
    assert response.status_code == 201, response.text
    assert (await client.get("/api/shared/kept.txt")).content == b"publication retained"
    async with sf() as session:
        rows = (await session.execute(select(SpaceFileOperationRow))).scalars().all()
        shared = [row for row in rows if row.request.get("namespace") == "hm.shared"]
        assert len(shared) == 1 and shared[0].phase == "complete"
        assert (shared[0].actor_kind, shared[0].actor_id, shared[0].request["by"]) == ("human", str(alice.id), "host")
    assert app.state.extensions.app_store.get(FeatureServices).services["hm.shared"].host_written


@pytest.mark.asyncio
@pytest.mark.parametrize("who", ["admin", "member"])
async def test_company_members_cannot_mutate_or_mount_shared_writable(feature_app, who):
    from types import SimpleNamespace

    from app.gateway.routers import spaces as spaces_router
    from deerflow.spaces.attachments import ResourceMount, SpaceAttachments
    from deerflow.spaces.contract import SpaceDenied

    client, app, files, _catalog, _paths, alice, bob, current = feature_app
    app.include_router(spaces_router.router)
    person = alice if who == "admin" else bob
    actor = PrincipalRef("human", str(person.id))
    async with files.registry._sf() as session:
        space_id = (await session.get(StorageFeatureLinkRow, ("hm.company-space", "company"))).space_id
    await files.registry.host_grant(space_id, actor, Permission.READ | Permission.EXPORT)
    current[0] = person
    space = await files.registry.get(actor=actor, space_id=space_id)
    response = await client.put(f"/api/spaces/{space_id}/content", params={"path": "shared/direct.txt", "generation": space.generation, "operation_id": __import__("uuid").uuid4().hex, "create": True}, content=b"direct mutation")
    assert response.status_code == 403, response.text
    provider = SimpleNamespace(host_id="not-reached")
    attachments = SpaceAttachments(files, provider)
    with pytest.raises(SpaceDenied):
        await attachments.attach(actor=actor, incarnation=__import__("uuid").uuid4().hex, resources=[ResourceMount(space.id, space.generation, "shared", True)])


@pytest.mark.asyncio
@pytest.mark.parametrize("admission", [1, 2, 3])
async def test_shared_role_is_rechecked_at_every_write_admission(feature_app, monkeypatch, admission):

    client, _app, files, _catalog, paths, alice, _bob, _current = feature_app
    outputs = paths.sandbox_outputs_dir(THREAD, user_id=str(alice.id))
    outputs.mkdir(parents=True)
    (outputs / "role.txt").write_bytes(b"role must remain assigned")
    original = files.run_workflow
    count = 0

    async def wrapped(**kwargs):
        controller = kwargs["controller_admission"]

        async def changed(session, rows):
            nonlocal count
            count += 1
            if count == admission:
                link = await session.get(StorageFeatureLinkRow, ("hm.company-space", "company"))
                await session.delete(link)
                await session.flush()
            await controller(session, rows)

        return await original(**{**kwargs, "controller_admission": changed})

    monkeypatch.setattr(files, "run_workflow", wrapped)
    response = await client.post("/api/shared/publish", json={"path": "/mnt/user-data/outputs/role.txt", "thread_id": THREAD})
    assert response.status_code == (409 if admission == 3 else 403), response.text
    async with files.registry._sf() as session:
        # The denied admission rolls back its attempted role change.
        link = await session.get(StorageFeatureLinkRow, ("hm.company-space", "company"))
        assert link is not None
        intent = (await session.execute(select(SpaceFileOperationRow))).scalars().all()
        assert len(intent) == (0 if admission == 1 else 1)
        if intent:
            assert intent[0].phase == "pending"


@pytest.mark.asyncio
async def test_shared_removal_keeps_publisher_ownership_checks(feature_app):
    client, _app, files, _catalog, paths, alice, bob, current = feature_app
    outputs = paths.sandbox_outputs_dir(THREAD, user_id=str(alice.id))
    outputs.mkdir(parents=True)
    (outputs / "owned.txt").write_bytes(b"owner")
    published = await client.post("/api/shared/publish", json={"path": "/mnt/user-data/outputs/owned.txt", "thread_id": THREAD})
    assert published.status_code == 201, published.text
    async with files.registry._sf() as session:
        space_id = (await session.get(StorageFeatureLinkRow, ("hm.company-space", "company"))).space_id
    await files.registry.host_grant(space_id, PrincipalRef("human", str(bob.id)), Permission.READ | Permission.EXPORT)
    current[0] = bob
    denied = await client.delete("/api/shared/owned.txt", params={"expected_publication_id": published.json()["publication_id"]})
    assert denied.status_code == 403, denied.text
    assert (await client.get("/api/shared/owned.txt")).content == b"owner"
