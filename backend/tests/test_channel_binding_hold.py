"""A channel binding held because its owner was turned off routes nothing until it is restored or its owner connects again."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from deerflow.persistence.channel_connections import ChannelConnectionRepository, ChannelCredentialCipher
from deerflow.persistence.channel_connections.sql import HELD_STATUS


@pytest.fixture
async def repo(tmp_path):
    from deerflow.persistence.engine import close_engine, get_session_factory, init_engine

    await init_engine("sqlite", url=f"sqlite+aiosqlite:///{tmp_path / 'channels.db'}", sqlite_dir=str(tmp_path))
    try:
        yield ChannelConnectionRepository(get_session_factory(), cipher=ChannelCredentialCipher.from_key("test-encryption-key"))
    finally:
        await close_engine()


async def _bind(repo: ChannelConnectionRepository, owner: str = "alice", account: str = "U-alice") -> dict:
    return await repo.upsert_connection(owner_user_id=owner, provider="slack", external_account_id=account, workspace_id="T1")


@pytest.mark.anyio
async def test_a_held_binding_no_longer_routes_its_owners_messages(repo):
    binding = await _bind(repo)

    assert await repo.hold_connection(binding["id"], owner_user_id="alice") is True

    assert await repo.find_connection_by_external_identity(provider="slack", external_account_id="U-alice", workspace_id="T1") is None
    [listed] = await repo.list_connections("alice")
    assert listed["status"] == HELD_STATUS


@pytest.mark.anyio
async def test_hold_leaves_a_revoked_binding_and_another_owners_binding_alone(repo):
    revoked = await _bind(repo, account="U-old")
    await repo.disconnect_connection(connection_id=revoked["id"], owner_user_id="alice")
    theirs = await _bind(repo, owner="bob", account="U-bob")

    assert await repo.hold_connection(revoked["id"], owner_user_id="alice") is False
    assert await repo.hold_connection(theirs["id"], owner_user_id="alice") is False
    assert await repo.hold_connection("missing", owner_user_id="alice") is False
    assert (await repo.find_connection_by_external_identity(provider="slack", external_account_id="U-bob", workspace_id="T1"))["id"] == theirs["id"]


@pytest.mark.anyio
async def test_restoring_a_held_binding_routes_it_again(repo):
    binding = await _bind(repo)
    await repo.hold_connection(binding["id"], owner_user_id="alice")

    assert await repo.restore_held_connection(binding["id"], owner_user_id="alice") == "restored"

    assert (await repo.find_connection_by_external_identity(provider="slack", external_account_id="U-alice", workspace_id="T1"))["id"] == binding["id"]


@pytest.mark.anyio
async def test_restoring_leaves_a_binding_that_changed_since(repo):
    binding = await _bind(repo)
    await repo.hold_connection(binding["id"], owner_user_id="alice")
    await repo.disconnect_connection(connection_id=binding["id"], owner_user_id="alice")

    assert await repo.restore_held_connection(binding["id"], owner_user_id="alice") == "changed_since"
    assert await repo.restore_held_connection(binding["id"], owner_user_id="bob") == "gone"
    assert await repo.restore_held_connection("missing", owner_user_id="alice") == "gone"
    assert await repo.find_connection_by_external_identity(provider="slack", external_account_id="U-alice", workspace_id="T1") is None


@pytest.mark.anyio
async def test_the_owner_connecting_again_turns_a_held_binding_on(repo):
    binding = await _bind(repo)
    await repo.hold_connection(binding["id"], owner_user_id="alice")

    again = await _bind(repo)

    assert again["id"] == binding["id"]
    assert again["status"] == "connected"


@pytest.mark.anyio
async def test_another_person_binding_the_same_account_takes_it_from_a_held_binding(repo):
    binding = await _bind(repo)
    await repo.hold_connection(binding["id"], owner_user_id="alice")

    taken = await _bind(repo, owner="bob")

    assert (await repo.find_connection_by_external_identity(provider="slack", external_account_id="U-alice", workspace_id="T1"))["id"] == taken["id"]
    assert await repo.restore_held_connection(binding["id"], owner_user_id="alice") == "changed_since"


@pytest.mark.anyio
async def test_a_turned_off_owners_unused_connect_codes_are_forgotten(repo):
    """A code minted before the refusal would otherwise bind, or turn a held binding back on, once used in the channel."""
    expires = datetime.now(UTC) + timedelta(minutes=10)
    await repo.create_oauth_state(owner_user_id="alice", provider="slack", state="code-alice", expires_at=expires)
    await repo.create_oauth_state(owner_user_id="bob", provider="slack", state="code-bob", expires_at=expires)

    assert await repo.delete_oauth_states_for_owners(["alice"]) == 1

    assert await repo.consume_oauth_state(provider="slack", state="code-alice") is None
    assert await repo.consume_oauth_state(provider="slack", state="code-bob") is not None
    assert await repo.delete_oauth_states_for_owners([]) == 0
