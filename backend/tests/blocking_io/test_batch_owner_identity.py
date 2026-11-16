"""Dispatch revalidation reads current account access without blocking Gateway."""

import asyncio
import json
from datetime import UTC, datetime

import pytest
from sqlalchemy import update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.gateway.auth.models import User
from app.gateway.auth.repositories.sqlite import SQLiteUserRepository
from app.subagent_batches.identity import resolve_batch_principal
from deerflow.config.app_config import get_app_config, reset_app_config
from deerflow.config.auth_config import LOCAL_ACCOUNT_ISSUER
from deerflow.config.subagent_batches_config import SubagentBatchesConfig
from deerflow.config.subagent_runtime_config import SubagentRuntimeConfig
from deerflow.persistence.user.access import DisabledIdentityRow, RoleLimitRow
from deerflow.persistence.user.model import UserRow
from deerflow.subagents.batch_service import SubagentBatchService


@pytest.mark.asyncio
@pytest.mark.parametrize("revocation", ["disabled", "released", "local_auth_off"])
async def test_batch_owner_lookup_does_not_block_the_host_loop(tmp_path, monkeypatch, revocation):
    monkeypatch.setenv("DEER_FLOW_AUTH_DISABLED", "0")
    config_path = tmp_path / "config.yaml"
    monkeypatch.setenv("DEER_FLOW_CONFIG_PATH", str(config_path))
    config = {"sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"}, "auth": {"local": {"enabled": True}}}
    await asyncio.to_thread(config_path.write_text, json.dumps(config), encoding="utf-8")
    reset_app_config()
    await asyncio.to_thread(get_app_config)
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/users.db")
    try:
        async with engine.begin() as connection:
            await connection.run_sync(lambda sync: UserRow.metadata.create_all(sync, tables=[UserRow.__table__, DisabledIdentityRow.__table__, RoleLimitRow.__table__]))
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        repo = SQLiteUserRepository(sessions)
        user = await repo.create_user(User(email="owner@example.com"))
        service = SubagentBatchService(
            repository=None,
            config=SubagentBatchesConfig(),
            runtime_config=SubagentRuntimeConfig(),
            resolve_principal=lambda batch: resolve_batch_principal(batch, load_user=repo.get_user_by_id),
        )
        resolver = service._bind_principal_resolver({"user_id": str(user.id), "execution_spec": {"user_role": "user"}})
        assert (await resolver()).role == "user"
        if revocation == "disabled":
            await repo.disable_identity(LOCAL_ACCOUNT_ISSUER, str(user.id))
        elif revocation == "released":
            async with sessions() as session:
                await session.execute(update(UserRow).where(UserRow.id == str(user.id)).values(released_at=datetime.now(UTC)))
                await session.commit()
        else:
            config["auth"]["local"]["enabled"] = False
            await asyncio.to_thread(config_path.write_text, json.dumps(config), encoding="utf-8")
        with pytest.raises(PermissionError):
            await resolver()
    finally:
        await engine.dispose()
        reset_app_config()
