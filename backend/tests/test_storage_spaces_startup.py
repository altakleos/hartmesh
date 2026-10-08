"""No filesystem fallback or unvalidated human projection at startup."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from test_agent_instances import instances as instances

from deerflow.spaces.contract import PrincipalRef


@pytest.mark.asyncio
async def test_human_directory_rechecks_disabled_and_role_limits():
    from app.gateway.storage_spaces import human_lookup

    ref = PrincipalRef("human", "person")
    user = SimpleNamespace(id="person", system_role="admin", disabled_at=None, role_limit=None, needs_setup=False, oauth_provider=None)
    repository = SimpleNamespace(get_user_by_id=AsyncMock(return_value=user))
    lookup = human_lookup(repository)
    assert (await lookup(ref)).can_provision_company
    user.role_limit = "user"
    assert not (await lookup(ref)).can_provision_company
    user.disabled_at = "retired"
    assert await lookup(ref) is None
    assert repository.get_user_by_id.await_count == 3


@pytest.mark.asyncio
async def test_memory_backend_cannot_start_managed_storage():
    from app.gateway.storage_spaces import initialize_storage_spaces
    from deerflow.config.storage_spaces_config import StorageSpacesConfig

    app = SimpleNamespace(state=SimpleNamespace())
    config = StorageSpacesConfig(enabled=True, inventory_path="/provider/inventory.v1.json")
    with pytest.raises(RuntimeError, match="durable database"):
        await initialize_storage_spaces(app, config, session_factory=None)
    assert getattr(app.state, "storage_spaces", None) is None


def test_enabled_inventory_must_be_explicit_absolute_and_startup_only():
    from pydantic import ValidationError

    from deerflow.config.reload_boundary import is_startup_only_field
    from deerflow.config.storage_spaces_config import StorageSpacesConfig

    with pytest.raises(ValidationError):
        StorageSpacesConfig(enabled=True)
    with pytest.raises(ValidationError):
        StorageSpacesConfig(enabled=True, inventory_path="relative/inventory")
    assert StorageSpacesConfig().enabled is False
    assert is_startup_only_field("storage_spaces")


@pytest.mark.asyncio
async def test_disabled_storage_startup_installs_tombstone_authority(instances):
    from _storage_spaces_test_support import ALICE, operation
    from test_agent_conversations import conversations
    from test_agent_instances import create

    from app.gateway.storage_spaces import initialize_storage_spaces
    from deerflow.config.storage_spaces_config import StorageSpacesConfig

    agents, authority, threads, sf = await conversations(instances)
    agent = await create(agents)
    chat = await authority.create(actor=ALICE, instance_id=agent.id, creation_id=operation())
    app = SimpleNamespace(state=SimpleNamespace(thread_store=threads))
    await initialize_storage_spaces(app, StorageSpacesConfig(), session_factory=sf)
    assert app.state.agent_instances is None and threads.instance_authority is app.state.agent_conversations
    assert await threads.get(chat["thread_id"], user_id="alice") is None
