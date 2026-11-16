"""Gateway batch identity resolves against the current account repository."""

from datetime import UTC, datetime
from traceback import format_exception

import pytest
import pytest_asyncio
from sqlalchemy import delete, update
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.gateway.auth.models import User
from app.gateway.auth.repositories.sqlite import SQLiteUserRepository
from app.subagent_batches.identity import resolve_batch_principal
from deerflow.persistence.user.access import DisabledIdentityRow, RoleLimitRow
from deerflow.persistence.user.model import UserRow


@pytest_asyncio.fixture
async def accounts(tmp_path, monkeypatch):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/users.db")
    async with engine.begin() as connection:
        await connection.run_sync(lambda sync: UserRow.metadata.create_all(sync, tables=[UserRow.__table__, DisabledIdentityRow.__table__, RoleLimitRow.__table__]))
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    repo = SQLiteUserRepository(sessions)
    owner = await repo.create_user(User(email="owner@example.com", system_role="admin", oauth_provider="oidc", oauth_issuer="https://identity.example.test", oauth_id="current"))
    await repo.create_user(User(email="root@example.com", system_role="admin"))
    monkeypatch.setenv("DEER_FLOW_AUTH_DISABLED", "0")
    monkeypatch.setattr("app.gateway.auth.mode.sign_on_only", lambda: False)
    try:
        yield repo, owner, sessions
    finally:
        await engine.dispose()


def _batch(owner, **spec):
    return {"id": "batch-1", "user_id": owner, "execution_spec": {"oauth_provider": "oidc", "oauth_id": "current", **spec}}


@pytest.mark.asyncio
async def test_current_profile_confirms_saved_identity_and_discards_stored_attributes(accounts):
    repo, owner, _ = accounts
    principal = await resolve_batch_principal(_batch(str(owner.id), user_role="admin", oauth_provider="oidc", oauth_id="current", authz_attributes={"admin": True}, is_internal=True, channel_user_id="im-user"), load_user=repo.get_user_by_id)
    assert (principal.user_id, principal.role, principal.oauth_provider, principal.oauth_id) == (str(owner.id), "admin", "oidc", "current")
    assert principal.is_internal and principal.channel_user_id == "im-user"
    assert principal.attributes == {}


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["disabled", "demoted", "deleted", "released"])
@pytest.mark.parametrize("internal", [False, True])
async def test_revoked_local_owner_cannot_fall_back_to_internal(accounts, change, internal):
    repo, owner, sessions = accounts
    batch = _batch(str(owner.id), user_role="admin", is_internal=internal)
    if change == "disabled":
        await repo.disable_identity(owner.oauth_issuer, owner.oauth_id)
    elif change == "demoted":
        await repo.limit_role(owner.oauth_issuer, owner.oauth_id, "user")
    elif change == "released":
        async with sessions() as session:
            await session.execute(update(UserRow).where(UserRow.id == str(owner.id)).values(released_at=datetime.now(UTC)))
            await session.commit()
    else:
        async with sessions() as session:
            await session.execute(delete(UserRow).where(UserRow.id == str(owner.id)))
            await session.commit()
    with pytest.raises(PermissionError):
        await resolve_batch_principal(batch, load_user=repo.get_user_by_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("original,current", [("admin", "user"), ("reviewer", "operator"), (None, "admin")])
async def test_role_changes_do_not_invent_a_role_hierarchy(accounts, original, current):
    repo, owner, _ = accounts
    await repo.record_sign_in(str(owner.id), system_role=current, oauth_issuer=owner.oauth_issuer, last_sign_in_at=datetime.now(UTC))
    with pytest.raises(PermissionError):
        await resolve_batch_principal(_batch(str(owner.id), user_role=original), load_user=repo.get_user_by_id)


@pytest.mark.asyncio
async def test_unmapped_internal_owner_keeps_default_role_without_stale_claims(accounts):
    repo, _, _ = accounts
    batch = _batch("external-owner", is_internal=True, channel_user_id="external-owner", oauth_provider=None, oauth_id=None, authz_attributes={"admin": True})
    principal = await resolve_batch_principal(batch, load_user=repo.get_user_by_id)
    assert principal.user_id == "external-owner" and principal.is_internal
    assert principal.role is None and principal.oauth_provider is None and principal.oauth_id is None
    assert principal.attributes == {}
    batch["execution_spec"]["is_internal"] = False
    with pytest.raises(PermissionError):
        await resolve_batch_principal(batch, load_user=repo.get_user_by_id)


@pytest.mark.asyncio
async def test_auth_disabled_only_preserves_explicit_synthetic_owner(accounts, monkeypatch):
    repo, owner, _ = accounts
    monkeypatch.setenv("DEER_FLOW_AUTH_DISABLED", "1")
    monkeypatch.delenv("DEER_FLOW_ENV", raising=False)
    monkeypatch.delenv("ENVIRONMENT", raising=False)
    batch = _batch("default", user_role="admin", oauth_provider=None, oauth_id=None)
    principal = await resolve_batch_principal(batch, load_user=repo.get_user_by_id)
    assert principal.user_id == "default" and principal.role == "admin"
    await repo.disable_identity(owner.oauth_issuer, owner.oauth_id)
    with pytest.raises(PermissionError):
        await resolve_batch_principal(_batch(str(owner.id), user_role="admin"), load_user=repo.get_user_by_id)
    monkeypatch.setenv("DEER_FLOW_AUTH_DISABLED", "0")
    with pytest.raises(PermissionError):
        await resolve_batch_principal(batch, load_user=repo.get_user_by_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["missing_loader", "exception", "mismatch"])
async def test_unavailable_identity_fails_closed(accounts, failure, caplog):
    _, owner, _ = accounts
    batch = _batch(str(owner.id), user_role="admin", is_internal=True)
    fake_password = "fixture-db-password"
    statement = "SELECT id FROM users WHERE id = :id"

    async def load_user(user_id):
        if failure == "exception":
            raise OperationalError(statement, {"id": user_id}, RuntimeError(f"postgresql://test:{fake_password}@test-db/deerflow"))
        return User(email="different@example.com", system_role="admin")

    with pytest.raises(PermissionError) as exc:
        await resolve_batch_principal(batch, load_user=None if failure == "missing_loader" else load_user)
    assert fake_password not in str(exc.value)
    if failure == "exception":
        records = [record for record in caplog.records if record.name == "app.subagent_batches.identity"]
        assert len(records) == 1
        record = records[0]
        assert batch["id"] in record.getMessage()
        assert "OperationalError" in record.getMessage()
        assert record.exc_info is None
        diagnostics = caplog.text + "".join(format_exception(exc.value))
        assert fake_password not in diagnostics
        assert str(owner.id) not in diagnostics
        assert statement not in diagnostics


@pytest.mark.asyncio
async def test_local_owner_becomes_inert_when_password_access_is_disabled(accounts, monkeypatch):
    repo, _, _ = accounts
    owner = await repo.create_user(User(email="local-owner@example.com"))
    batch = _batch(str(owner.id), user_role="user", oauth_provider=None, oauth_id=None)
    assert (await resolve_batch_principal(batch, load_user=repo.get_user_by_id)).user_id == str(owner.id)
    monkeypatch.setattr("app.gateway.auth.mode.sign_on_only", lambda: True)
    with pytest.raises(PermissionError):
        await resolve_batch_principal(batch, load_user=repo.get_user_by_id)


@pytest.mark.asyncio
async def test_password_reset_does_not_revoke_admitted_automation(accounts):
    repo, owner, _ = accounts
    batch = _batch(str(owner.id), user_role="admin")
    reset = await repo.replace_password(str(owner.id), "synthetic-hash", needs_setup=True)
    assert reset.needs_setup and reset.token_version > owner.token_version
    principal = await resolve_batch_principal(batch, load_user=repo.get_user_by_id)
    assert principal.user_id == str(owner.id) and principal.role == "admin"


@pytest.mark.asyncio
async def test_unreadable_account_policy_fails_closed_without_private_diagnostics(accounts, monkeypatch, caplog):
    repo, owner, _ = accounts
    secret = "synthetic-policy-secret"

    def unavailable(_user):
        raise ValueError(secret)

    monkeypatch.setattr("app.subagent_batches.identity.account_refusal", unavailable)
    with pytest.raises(PermissionError) as caught:
        await resolve_batch_principal(_batch(str(owner.id), user_role="admin"), load_user=repo.get_user_by_id)
    assert secret not in str(caught.value) + caplog.text
    assert all(record.exc_info is None for record in caplog.records)


@pytest.mark.asyncio
async def test_builtin_promotion_retains_the_original_batch_role(accounts):
    from deerflow.config.subagent_batches_config import SubagentBatchesConfig
    from deerflow.config.subagent_runtime_config import SubagentRuntimeConfig
    from deerflow.subagents.batch_service import SubagentBatchService

    repo, owner, _ = accounts
    # The current account is admin, but this batch was admitted as user.
    batch = _batch(str(owner.id), user_role="user")
    service = SubagentBatchService(repository=None, config=SubagentBatchesConfig(), runtime_config=SubagentRuntimeConfig(), resolve_principal=lambda candidate: resolve_batch_principal(candidate, load_user=repo.get_user_by_id))
    resolver = service._bind_principal_resolver(batch)
    assert (await repo.get_user_by_id(str(owner.id))).system_role == "admin"
    assert (await resolver()).role == "user"
    # Native execution calls this same resolver after acquiring its slot.
    assert (await resolver()).role == "user"


@pytest.mark.asyncio
@pytest.mark.parametrize("saved", [{"oauth_provider": "other"}, {"oauth_id": "other"}, {"oauth_provider": None, "oauth_id": None}])
async def test_changed_or_missing_saved_oauth_identity_is_not_a_new_grant(accounts, saved):
    repo, owner, _ = accounts
    with pytest.raises(PermissionError):
        await resolve_batch_principal(_batch(str(owner.id), user_role="admin", **saved), load_user=repo.get_user_by_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("saved", [{"oauth_provider": "old"}, {"oauth_id": "old"}, {"channel_user_id": "different"}, {"channel_user_id": None}])
async def test_unmapped_internal_owner_requires_the_original_unbound_shape(accounts, saved):
    repo, _, _ = accounts
    spec = {"is_internal": True, "channel_user_id": "external-owner", "oauth_provider": None, "oauth_id": None, **saved}
    with pytest.raises(PermissionError):
        await resolve_batch_principal(_batch("external-owner", **spec), load_user=repo.get_user_by_id)
