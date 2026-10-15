"""Product-owned storage refuses generic administrative mutations."""

import httpx
import pytest
from _storage_spaces_test_support import ALICE, BOB, home, operation
from sqlalchemy import select
from test_storage_spaces_routes import request_app
from test_storage_spaces_routes import space_file_storage as space_file_storage

from deerflow.features.resources import StorageFeatureLinkRow
from deerflow.persistence.spaces.files import SpaceFileOperationRow
from deerflow.persistence.spaces.model import SpaceEventRow, SpaceGrantRow, SpaceRow
from deerflow.spaces.contract import Custody, MutationMode, Permission

MOUNTS_DETAIL = "This storage is mounted by chat sandboxes that are not recorded, so it cannot be quiesced; restore the data disk from a snapshot instead."
OWNED_DETAIL = "This storage belongs to a person, an AI assistant, an AI employee or the company; who may use it, and whether it is kept, is decided in the product"


@pytest.mark.asyncio
@pytest.mark.parametrize("role", ["none", "user", "company"])
async def test_explicit_deleted_space_release_dispatches_before_chat_lifecycle_guards(space_file_storage, role):
    from test_storage_spaces_release import deleted

    from deerflow.persistence.spaces.files import SpaceBackingRow

    files, sf, _catalog = space_file_storage
    space = await resource(files, sf, role)
    await deleted(sf, space, retained=True)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=request_app(files)), base_url="http://test") as client:
        response = await client.post(f"/api/spaces/{space.id}/lifecycle", json={"generation": 2, "operation_id": operation(), "action": "release"})
        assert response.status_code == 200 and response.json() == {"complete": True, "slot_released": True}, response.text
    async with sf() as session:
        assert await session.get(SpaceBackingRow, space.backing_handle) is None


async def resource(files, sf, role):
    if role == "company":
        space = await files.create(actor=ALICE, name="Company", custody=Custody.company(), mode=MutationMode.NATIVE)
    else:
        space = await home(files)
    if role != "none":
        namespace, key = {
            "user": ("hm.user-space", "human:alice"),
            "company": ("hm.company-space", "company"),
            "custom": ("example.owner", "one"),
        }[role]
        async with sf() as session, session.begin():
            session.add(StorageFeatureLinkRow(namespace=namespace, key=key, space_id=space.id))
    return space


async def metadata(sf, space):
    async with sf() as session:
        row = await session.get(SpaceRow, space.id)
        grants = sorted((g.principal_kind, g.subject_id, g.permissions) for g in await session.scalars(select(SpaceGrantRow).where(SpaceGrantRow.space_id == space.id)))
        events = sorted((e.generation, e.action) for e in await session.scalars(select(SpaceEventRow).where(SpaceEventRow.space_id == space.id)))
        operations = list(await session.scalars(select(SpaceFileOperationRow).where(SpaceFileOperationRow.space_id == space.id)))
        return row.name, row.status, row.generation, grants, events, len(operations)


@pytest.mark.asyncio
@pytest.mark.parametrize("role", ["user", "company"])
@pytest.mark.parametrize("action", ["backup", "restore", "archive", "delete", "recovery"])
async def test_chat_roles_refuse_destructive_generic_routes_without_side_effects(space_file_storage, role, action):
    files, sf, catalog = space_file_storage
    space = await resource(files, sf, role)
    before = await metadata(sf, space)
    async with sf() as session:
        from deerflow.persistence.spaces.files import SpaceBackingRow

        slot = (await session.get(SpaceBackingRow, space.backing_handle)).slot_id
    document = catalog.verified[slot].data_path / "untouched"
    document.write_bytes(b"chat files")
    body = {"generation": 1, "operation_id": operation()}
    if action == "recovery":
        suffix = "recovery"
        body["acknowledge_uncertain_outcome"] = True
    else:
        suffix = "lifecycle"
        body["action"] = action
        if action == "restore":
            body["backup_id"] = operation()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=request_app(files)), base_url="http://test") as client:
        response = await client.post(f"/api/spaces/{space.id}/{suffix}", json=body)
        assert response.status_code == 409
        assert response.json() == {"detail": MOUNTS_DETAIL, "reason": "mounts-unrecorded"}
        status = await client.get(f"/api/spaces/{space.id}/recovery")
        assert status.status_code == 200 and status.json()["operations"] == []
    assert await metadata(sf, space) == before
    assert document.read_bytes() == b"chat files"


@pytest.mark.asyncio
@pytest.mark.parametrize("role", ["user", "company", "custom", "none"])
@pytest.mark.parametrize("change", ["new", "change", "remove"])
async def test_managed_grants_refuse_every_resource_and_every_mutation(space_file_storage, role, change):
    files, sf, _catalog = space_file_storage
    space = await resource(files, sf, role)
    if change != "new":
        space = await files.registry.set_grant(actor=ALICE, space_id=space.id, expected_generation=1, subject=BOB, permissions=Permission.READ, acknowledge_existing_data=True)
    before = await metadata(sf, space)
    files.user_spaces = False
    permissions = {"new": 1, "change": int(Permission.READ | Permission.WRITE), "remove": 0}[change]
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=request_app(files)), base_url="http://test") as client:
        response = await client.put(f"/api/spaces/{space.id}/grants", json={"subject_kind": BOB.kind, "subject_id": BOB.subject_id, "permissions": permissions, "generation": space.generation, "acknowledge_existing_data": True})
    assert response.status_code == 409 and response.json() == {"detail": OWNED_DETAIL, "reason": "owned-space"}
    assert await metadata(sf, space) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["new", "change", "remove"])
async def test_generic_mode_keeps_grant_mutations(space_file_storage, change):
    files, sf, _catalog = space_file_storage
    space = await home(files)
    if change != "new":
        space = await files.registry.set_grant(actor=ALICE, space_id=space.id, expected_generation=1, subject=BOB, permissions=Permission.READ, acknowledge_existing_data=True)
    permissions = {"new": int(Permission.READ), "change": int(Permission.READ | Permission.WRITE), "remove": 0}[change]
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=request_app(files)), base_url="http://test") as client:
        response = await client.put(f"/api/spaces/{space.id}/grants", json={"subject_kind": BOB.kind, "subject_id": BOB.subject_id, "permissions": permissions, "generation": space.generation, "acknowledge_existing_data": True})
    assert response.status_code == 200
    async with sf() as session:
        grant = await session.get(SpaceGrantRow, (space.id, BOB.kind, BOB.subject_id))
        assert grant is None if not permissions else grant.permissions == permissions
        assert (await session.get(SpaceRow, space.id)).generation == space.generation + 1


@pytest.mark.asyncio
@pytest.mark.parametrize("role", ["none", "custom"])
async def test_non_chat_resources_keep_generic_lifecycle(space_file_storage, role):
    files, sf, _catalog = space_file_storage
    space = await resource(files, sf, role)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=request_app(files)), base_url="http://test") as client:
        response = await client.post(f"/api/spaces/{space.id}/lifecycle", json={"action": "archive", "generation": 1, "operation_id": operation()})
    assert response.status_code == 200 and response.json()["status"] == "archived"


@pytest.mark.asyncio
@pytest.mark.parametrize("endpoint", ["grants", "lifecycle", "recovery"])
async def test_managed_refusal_never_bypasses_caller_identity_or_admin_grant(space_file_storage, endpoint):
    files, sf, _catalog = space_file_storage
    space = await resource(files, sf, "user")
    files.user_spaces = False
    body = {"generation": 1, "operation_id": operation()}
    if endpoint == "grants":
        body = {"generation": 1, "subject_kind": BOB.kind, "subject_id": BOB.subject_id, "permissions": 0}
        method = "put"
    else:
        method = "post"
        body.update({"action": "delete"} if endpoint == "lifecycle" else {"acknowledge_uncertain_outcome": True})
    for actor, source, status in [(BOB, "session", 404), (ALICE, "pat", 403), (None, "session", 401)]:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=request_app(files, actor, source)), base_url="http://test") as client:
            response = await getattr(client, method)(f"/api/spaces/{space.id}/{endpoint}", json=body)
        assert response.status_code == status
    await files.registry.set_grant(actor=ALICE, space_id=space.id, expected_generation=1, subject=BOB, permissions=Permission.READ, acknowledge_existing_data=True)
    body["generation"] = 2
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=request_app(files, BOB)), base_url="http://test") as client:
        response = await getattr(client, method)(f"/api/spaces/{space.id}/{endpoint}", json=body)
    assert response.status_code == 403


@pytest.mark.asyncio
@pytest.mark.parametrize("endpoint", ["grants", "lifecycle", "recovery"])
async def test_managed_refusal_keeps_stale_generation_and_works_without_volume_proof(space_file_storage, endpoint):
    from deerflow.persistence.spaces.files import SpaceBackingRow

    files, sf, catalog = space_file_storage
    space = await resource(files, sf, "user")
    files.user_spaces = False
    async with sf() as session, session.begin():
        bound = await session.get(SpaceBackingRow, space.backing_handle)
        catalog.verified[bound.slot_id].control_path.joinpath(".space-binding").unlink()
        session.add(SpaceFileOperationRow(space_id=space.id, operation_id=operation(), actor_kind=ALICE.kind, actor_id=ALICE.subject_id, generation=1, phase="pending", request={"action": "write"}))
    before = await metadata(sf, space)
    body = {"generation": 2, "operation_id": operation()}
    if endpoint == "grants":
        method = "put"
        body = {"generation": 2, "subject_kind": BOB.kind, "subject_id": BOB.subject_id, "permissions": 0}
    else:
        method = "post"
        body.update({"action": "delete"} if endpoint == "lifecycle" else {"acknowledge_uncertain_outcome": True})
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=request_app(files)), base_url="http://test") as client:
        stale = await getattr(client, method)(f"/api/spaces/{space.id}/{endpoint}", json=body)
        assert stale.status_code == 409 and stale.json() == {"detail": "Stale space generation"}
        body["generation"] = 1
        response = await getattr(client, method)(f"/api/spaces/{space.id}/{endpoint}", json=body)
        assert response.status_code == 409
        assert response.json()["reason"] == ("owned-space" if endpoint == "grants" else "mounts-unrecorded")
        status = await client.get(f"/api/spaces/{space.id}/recovery")
        assert status.status_code == 200 and len(status.json()["operations"]) == 1
    assert await metadata(sf, space) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("namespace,key", [("hm.user-space", "wrong-owner"), ("hm.company-space", "wrong-key")])
async def test_malformed_chat_role_claim_still_protects_generic_lifecycle(space_file_storage, namespace, key):
    files, sf, _catalog = space_file_storage
    space = await home(files)
    async with sf() as session, session.begin():
        session.add(StorageFeatureLinkRow(namespace=namespace, key=key, space_id=space.id))
    before = await metadata(sf, space)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=request_app(files)), base_url="http://test") as client:
        response = await client.post(f"/api/spaces/{space.id}/lifecycle", json={"action": "delete", "generation": 1, "operation_id": operation()})
    assert response.status_code == 409 and response.json()["reason"] == "mounts-unrecorded"
    assert await metadata(sf, space) == before
