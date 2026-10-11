"""Local accounts keep account-scoped access facts independently of their email."""

import pytest
from test_compose_account_seats import seated_accounts as seated_accounts

from app.gateway.auth.accounts import AccountsCommand
from app.gateway.auth.mode import ACCOUNT_DISABLED, account_refusal
from app.gateway.auth.models import User


@pytest.mark.asyncio
async def test_operator_can_disable_and_enable_local_account_without_fabricated_provider(seated_accounts):
    repo, _sf, *_ = seated_accounts
    local = await repo.create_user(User(email="local@example.com", password_hash="fixture-hash"))
    command = AccountsCommand(repo, tokens=None, schedules=None)
    disabled = await command.run("disable", email=local.email)
    current = await repo.get_user_by_id(str(local.id))
    assert current.disabled_at is not None
    assert account_refusal(current) == ACCOUNT_DISABLED
    assert str(local.id) in await repo.list_refused_user_ids()
    assert current.oauth_provider is current.oauth_id is current.oauth_issuer is None
    assert disabled["identity"]["kind"] == "local"
    enabled = await command.run("enable", email=local.email)
    assert enabled["identity"]["kind"] == "local"
    assert (await repo.get_user_by_id(str(local.id))).disabled_at is None
    assert str(local.id) not in await repo.list_refused_user_ids()


@pytest.mark.asyncio
@pytest.mark.parametrize("padding", ["\t", "\n", "\u2003"])
async def test_enable_preserves_python_issuer_normalization_and_other_issuers(seated_accounts, padding):
    from datetime import UTC, datetime

    from app.gateway.auth.repositories.sqlite import ReleasedAccount
    from deerflow.persistence.user.model import UserRow

    repo, sf, *_ = seated_accounts
    issuer = "https://identity.example.com"
    person = await repo.create_user(User(email="person@example.com", oauth_provider="one", oauth_id="same", oauth_issuer=padding + issuer + "/" + padding))
    other = await repo.create_user(User(email="other@example.com", oauth_provider="two", oauth_id="same", oauth_issuer="https://other.example.com"))
    await repo.disable_identity(issuer, "same")
    async with sf() as session, session.begin():
        (await session.get(UserRow, str(person.id))).released_at = datetime.now(UTC)
    with pytest.raises(ReleasedAccount):
        await repo.enable_identity(issuer, "same", account_id=str(person.id))
    assert (await repo.get_user_by_id(str(person.id))).disabled_at is not None
    assert (await repo.get_user_by_id(str(other.id))).disabled_at is None


@pytest.mark.asyncio
@pytest.mark.parametrize("restore", [False, True])
async def test_enabling_sibling_of_released_account_preserves_identity_and_holds(seated_accounts, restore):
    from datetime import UTC, datetime

    from deerflow.persistence.user.model import UserRow

    repo, sf, *_ = seated_accounts
    issuer = "https://identity.example.com"
    released = await repo.create_user(User(email="released@example.com", oauth_provider="one", oauth_id="same", oauth_issuer=issuer))
    sibling = await repo.create_user(User(email="sibling@example.com", oauth_provider="two", oauth_id="same", oauth_issuer=issuer))
    await repo.disable_identity(issuer, "same")
    holds = [("schedule", "held-schedule", str(released.id))]
    await repo.record_holds(issuer, "same", holds)
    async with sf() as session, session.begin():
        (await session.get(UserRow, str(released.id))).released_at = datetime.now(UTC)
    result = await AccountsCommand(repo, tokens=None, schedules=None).run("enable", account_id=str(sibling.id), restore_held=restore)
    assert result["error"] == "released" and result["returncode"] == 1
    assert await repo.list_holds(issuer, "same") == holds
    assert (await repo.get_user_by_id(str(sibling.id))).disabled_at is not None


@pytest.mark.asyncio
async def test_local_disable_cannot_shadow_issuerless_provider_with_same_subject(seated_accounts):
    repo, _sf, *_ = seated_accounts
    local = await repo.create_user(User(email="local@example.com", password_hash="fixture-hash"))
    provider = await repo.create_user(User(email="provider@example.com", oauth_provider="sso", oauth_id=str(local.id), oauth_issuer=None))
    await AccountsCommand(repo, tokens=None, schedules=None).run("disable", email=local.email)
    assert (await repo.get_user_by_id(str(local.id))).disabled_at is not None
    assert (await repo.get_user_by_id(str(provider.id))).disabled_at is None
    assert await repo.list_refused_user_ids() == {str(local.id)}
    listed = await AccountsCommand(repo, tokens=None, schedules=None).run("list")
    assert listed["disabled_without_account"] == []
    assert "urn:hartmesh:local-account" not in __import__("json").dumps(listed)


@pytest.mark.asyncio
async def test_local_disable_stays_on_account_after_email_changes_or_is_reused(seated_accounts):
    repo, _sf, *_ = seated_accounts
    local = await repo.create_user(User(email="old@example.com", password_hash="fixture-hash"))
    command = AccountsCommand(repo, tokens=None, schedules=None)
    await command.run("disable", email=local.email)
    local.email = "changed@example.com"
    await repo.update_user(local)
    replacement = await repo.create_user(User(email="old@example.com", password_hash="another-fixture-hash"))
    assert (await repo.get_user_by_id(str(local.id))).disabled_at is not None
    assert (await repo.get_user_by_id(str(replacement.id))).disabled_at is None
    await command.run("enable", email="changed@example.com")
    assert (await repo.get_user_by_id(str(local.id))).disabled_at is None


@pytest.mark.asyncio
async def test_local_identity_namespace_cannot_be_supplied_as_provider_selector(seated_accounts):
    from app.gateway.auth.accounts import CommandError
    from deerflow.config.auth_config import LOCAL_ACCOUNT_ISSUER

    repo, _sf, *_ = seated_accounts
    local = await repo.create_user(User(email="local@example.com", password_hash="fixture-hash"))
    with pytest.raises(CommandError, match="Local accounts"):
        await AccountsCommand(repo, tokens=None, schedules=None).run("disable", issuer=LOCAL_ACCOUNT_ISSUER, subject=str(local.id))
    assert (await repo.get_user_by_id(str(local.id))).disabled_at is None
