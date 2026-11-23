"""Upgrade populated predecessor snapshots without rewriting the adopted document."""

import asyncio
from importlib import import_module

import pytest
from alembic import command
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import inspect, text
from test_migration_0047_mcp_oauth_tokens import _engine

from deerflow.agent_instances.contract import DefinitionSnapshot
from deerflow.persistence import bootstrap
from deerflow.persistence.agent_instances.proposals import AgentProposalRow


@pytest.mark.asyncio
@pytest.mark.parametrize("backend", ["sqlite", "postgres"])
@pytest.mark.parametrize("predecessor", ["0056_upstream_adoption", "0057_run_cancel_actor_kind"])
async def test_proposal_schema_freezes_old_documents_and_retains_used_audit(tmp_path, backend, predecessor):
    engine, schema = _engine(tmp_path, backend)
    config = bootstrap._get_alembic_config(engine, postgres_schema=schema or "")
    module = import_module("deerflow.persistence.migrations.versions.0058_agent_proposals")
    base = DefinitionSnapshot.capture(owner_id="user", config={"name": "old", "skills": [], "work_policy": None}, soul="Old prompt\n")
    try:
        if schema:
            async with engine.begin() as conn:
                await conn.execute(text(f'CREATE SCHEMA "{schema}"'))
        await asyncio.to_thread(command.upgrade, config, predecessor)
        async with engine.begin() as conn:
            await conn.execute(text("INSERT INTO agent_definition_revisions (revision, owner_id, config, soul) VALUES (:revision, 'user', :config, :soul)"), {"revision": base.revision, "config": base.config_json, "soul": base.soul})
        await asyncio.to_thread(command.upgrade, config, "0058_agent_proposals")
        async with engine.begin() as conn:

            def check(sync):
                with Operations.context(MigrationContext.configure(sync)):
                    module.upgrade()
                inspector = inspect(sync)
                assert {c["name"] for c in inspector.get_columns("agent_configuration_proposals")} == set(AgentProposalRow.__table__.columns.keys())

            await conn.run_sync(check)
            row = (await conn.execute(text("SELECT revision, soul FROM agent_definition_revisions"))).one()
            assert row == (base.revision, base.soul)
        await asyncio.to_thread(command.downgrade, config, module.down_revision)
        await asyncio.to_thread(command.upgrade, config, module.revision)
        async with engine.begin() as conn:
            await conn.execute(
                text(
                    "INSERT INTO agent_instances (id, principal_id, name, custody, creator_id, supervisor_id, "
                    "definition_revision, status, generation, creation_id, creation_request, created_at, updated_at) "
                    "VALUES ('employee', 'agent:employee', 'Old employee', 'company', 'user', 'user', :revision, 'provisioning', 1, 'creation', '{}', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
                ),
                {"revision": base.revision},
            )
            await conn.execute(
                text(
                    "INSERT INTO agent_configuration_proposals (id,instance_id,author_id,requester_id,thread_id,thread_incarnation,run_id,issuer_id,call_id,"
                    "base_generation,base_revision,field,proposed_value,candidate_digest,created_at) "
                    "VALUES ('proposal','employee','agent:employee','user','thread','incarnation','run','issuer','call',1,:revision,'work_policy.enabled',true,'digest',CURRENT_TIMESTAMP)"
                ),
                {"revision": base.revision},
            )
            await conn.execute(text("UPDATE agent_configuration_proposals SET decided_at=CURRENT_TIMESTAMP, decided_by='user', operation_id='unknown'"))
        with pytest.raises(RuntimeError, match="Resolve linked"):
            await asyncio.to_thread(command.downgrade, config, module.down_revision)
        async with engine.begin() as conn:
            await conn.execute(text("UPDATE agent_configuration_proposals SET operation_id=NULL"))
        await asyncio.to_thread(command.downgrade, config, module.down_revision)
        async with engine.connect() as conn:
            assert (await conn.execute(text("SELECT id FROM agent_configuration_proposals"))).scalar_one() == "proposal"
            assert (await conn.execute(text("SELECT version_num FROM alembic_version"))).scalar_one() == module.down_revision
        await asyncio.to_thread(command.upgrade, config, module.revision)
    finally:
        if schema:
            async with engine.begin() as conn:
                await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await engine.dispose()
