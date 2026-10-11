import pytest
from test_storage_feature_routes import feature_app as feature_app

from deerflow.features.resources import StorageFeatureLinkRow


@pytest.mark.asyncio
@pytest.mark.parametrize("endpoint", ["/api/files", "/api/shared", "/api/projects"])
async def test_missing_role_answers_structured_unassigned_without_allocation(feature_app, endpoint):
    from sqlalchemy import delete, select

    from deerflow.persistence.spaces.files import SpaceBackingRow

    client, app, files, _, _, _, _, _ = feature_app
    async with files.registry._sf() as session:
        await session.execute(delete(StorageFeatureLinkRow))
        await session.commit()
        before = len((await session.execute(select(SpaceBackingRow))).scalars().all())
    response = await client.get(endpoint)
    assert response.status_code == 503, response.text
    detail = response.json()["detail"]
    assert detail["code"] == "storage-unavailable" and detail["reason"] == "space-not-assigned"
    assert isinstance(detail["message"], str) and detail["message"]
    async with files.registry._sf() as session:
        assert not (await session.execute(select(StorageFeatureLinkRow))).first()
        assert len((await session.execute(select(SpaceBackingRow))).scalars().all()) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("installed", [True, False])
@pytest.mark.parametrize("reason", ["disk-too-small", "storage-pool-missing", "inventory-unreadable", "host-unqualified", "admission-failed"])
async def test_storage_unavailability_has_a_bounded_public_reason(feature_app, installed, reason):
    from types import SimpleNamespace

    client, app, files, _, _, _, _, _ = feature_app
    app.state.storage_spaces_enabled = True
    app.state.agent_execution = SimpleNamespace(storage_reason=reason, storage_text="/private/host-path diagnostic")
    if not installed:
        app.state.storage_spaces = None
    response = await client.get("/api/files")
    assert response.status_code == 503, response.text
    detail = response.json()["detail"]
    assert detail["code"] == "storage-unavailable" and detail["reason"] == reason
    assert "/private/" not in response.text and detail["message"]


@pytest.mark.asyncio
async def test_unverified_assigned_files_answer_503_without_exposing_host_path(feature_app, monkeypatch):
    from deerflow.spaces.backings import BackingUnavailable

    client, _, files, catalog, _, _, _, _ = feature_app

    def unavailable(*args, **kwargs):
        raise BackingUnavailable("/private/mount changed")

    monkeypatch.setattr(catalog, "verify", unavailable)
    response = await client.get("/api/files")
    assert response.status_code == 503, response.text
    assert response.json()["detail"]["reason"] == "space-unverified"
    assert "/private/" not in response.text
