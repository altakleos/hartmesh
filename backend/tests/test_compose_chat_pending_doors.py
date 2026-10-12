"""Native file integrations refuse NoUser before touching legacy directories."""

from unittest.mock import AsyncMock

import pytest
from test_storage_feature_routes import THREAD
from test_storage_feature_routes import feature_app as feature_app


@pytest.mark.asyncio
@pytest.mark.parametrize("door", ["browser", "archive", "skill"])
async def test_native_file_doors_without_user_role_refuse_before_legacy_effect(feature_app, monkeypatch, door):
    from sqlalchemy import delete
    from test_compose_native_file_sources import native_request

    from app.gateway.routers import browser, skills, thread_runs
    from deerflow.features.resources import StorageFeatureLinkRow
    from deerflow.runtime.customer_administration import CustomerAdministrationPolicy

    client, app, files, _, base, alice, _, _ = feature_app
    native_request(app)
    app.state.customer_administration_policy = CustomerAdministrationPolicy(local_skill_management=True)
    app.state.thread_store.get = AsyncMock(return_value={"thread_id": THREAD, "user_id": str(alice.id)})
    app.include_router(browser.router)
    app.include_router(skills.router)
    app.include_router(thread_runs.router)
    monkeypatch.setattr(browser, "_browser_tools_enabled", lambda: True)
    async with files.registry._sf() as session, session.begin():
        await session.execute(delete(StorageFeatureLinkRow).where(StorageFeatureLinkRow.namespace == "hm.user-space", StorageFeatureLinkRow.key == "human:" + str(alice.id)))
    if door == "browser":
        response = await client.post(f"/api/threads/{THREAD}/browser/navigate", json={"url": "https://example.com/"})
    elif door == "archive":
        response = await client.post(f"/api/threads/{THREAD}/runs/run-id/artifacts/archive")
    else:
        response = await client.post("/api/skills/install", json={"thread_id": THREAD, "path": "/mnt/user-data/outputs/file.skill"})
    assert response.status_code == 503, response.text
    assert response.json()["detail"]["reason"] == "space-not-assigned"
    assert not base.sandbox_user_data_dir(THREAD, user_id=str(alice.id)).exists()
