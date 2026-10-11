"""Operator release preserves rows and atomically marks only the owned user Space."""

import pytest
from sqlalchemy import select
from test_compose_user_assignment import account_storage as account_storage
from test_compose_user_assignment import seated_accounts as seated_accounts

from app.gateway import storage_spaces as ss
from app.gateway.auth.accounts import AccountsCommand, released_email_for
from app.gateway.auth.models import User
from deerflow.features.resources import StorageFeatureLinkRow
from deerflow.persistence.spaces.model import SpaceEventRow, SpaceRow


@pytest.mark.asyncio
async def test_release_requires_disabled_account_and_changes_nothing_otherwise(account_storage):
    app, repo, sf, _files, _catalog, _company = account_storage
    person = await repo.create_user(User(email="person@example.com", password_hash="fixture-hash"))
    await ss.assign_user_space(app, person)
    result = await AccountsCommand(repo, tokens=None, schedules=None).run("release-seat", email=person.email)
    assert result["error"] == "not_disabled" and result["returncode"] == 1
    assert (await repo.get_user_by_id(str(person.id))).released_at is None
    async with sf() as session:
        space = (await session.get(StorageFeatureLinkRow, ("hm.user-space", "human:" + str(person.id)))).space_id
        assert (await session.get(SpaceRow, space)).status == "active"
        assert not (await session.execute(select(SpaceEventRow).where(SpaceEventRow.space_id == space, SpaceEventRow.action == "delete"))).first()


@pytest.mark.asyncio
async def test_release_is_atomic_retryable_and_keeps_account_provenance(account_storage):
    app, repo, sf, _files, _catalog, _company = account_storage
    person = await repo.create_user(User(email="person@example.com", password_hash="fixture-hash"))
    await ss.assign_user_space(app, person)
    command = AccountsCommand(repo, tokens=None, schedules=None)
    await command.run("disable", email=person.email)
    result = await command.run("release-seat", email=person.email)
    assert result["released"] == str(person.id) and result["returncode"] == 0
    stored = await repo.get_user_by_id(str(person.id))
    assert stored.released_at is not None and stored.email == released_email_for(str(person.id))
    assert stored.email_released_from == "person@example.com"
    assert await repo.count_users() == 1 and await repo.count_seated_users() == 0
    async with sf() as session:
        link = await session.get(StorageFeatureLinkRow, ("hm.user-space", "human:" + str(person.id)))
        space = await session.get(SpaceRow, link.space_id)
        assert space.status == "deleted"
        events = (await session.execute(select(SpaceEventRow).where(SpaceEventRow.space_id == space.id, SpaceEventRow.action == "delete"))).scalars().all()
        assert len(events) == 1
        assert (events[0].actor_kind, events[0].actor_id) == ("human", str(person.id))
        assert events[0].details == {"retained_backups": False, "retained_binding": True, "by": "host", "cause": "seat-released"}
    again = await command.run("release-seat", account_id=str(person.id))
    assert again["released"] == str(person.id)
    current = await repo.get_user_by_id(str(person.id))
    assert current.released_at == stored.released_at and current.email_released_from == stored.email_released_from
    async with sf() as session:
        assert len((await session.execute(select(SpaceEventRow).where(SpaceEventRow.space_id == space.id, SpaceEventRow.action == "delete"))).scalars().all()) == 1
    refused = await command.run("enable", account_id=str(person.id))
    assert refused["error"] == "released" and refused["returncode"] == 1
    assert (await repo.get_user_by_id(str(person.id))).disabled_at is not None


@pytest.mark.asyncio
async def test_release_rolls_back_account_marker_and_email_when_space_event_cannot_advance(account_storage):
    app, repo, sf, _files, _catalog, _company = account_storage
    person = await repo.create_user(User(email="person@example.com", password_hash="fixture-hash"))
    await ss.assign_user_space(app, person)
    command = AccountsCommand(repo, tokens=None, schedules=None)
    await command.run("disable", email=person.email)
    async with sf() as session, session.begin():
        link = await session.get(StorageFeatureLinkRow, ("hm.user-space", "human:" + str(person.id)))
        (await session.get(SpaceRow, link.space_id)).generation = 2**31 - 1
    from deerflow.spaces.contract import SpaceConflict

    with pytest.raises(SpaceConflict, match="generation"):
        await command.run("release-seat", email=person.email)
    current = await repo.get_user_by_id(str(person.id))
    assert current.released_at is None and current.email == person.email and current.email_released_from is None
    assert await repo.count_seated_users() == 1
    async with sf() as session:
        assert (await session.get(SpaceRow, link.space_id)).status == "active"


@pytest.mark.asyncio
async def test_release_refuses_a_user_role_pointing_at_company_space(account_storage):
    _app, repo, sf, _files, _catalog, company = account_storage
    person = await repo.create_user(User(email="person@example.com", password_hash="fixture-hash"))
    # Replace the company role instead of adding an impossible second link for its Space.
    async with sf() as session, session.begin():
        await session.delete(await session.get(StorageFeatureLinkRow, ("hm.company-space", "company")))
        await session.flush()
        session.add(StorageFeatureLinkRow(namespace="hm.user-space", key="human:" + str(person.id), space_id=company))
    command = AccountsCommand(repo, tokens=None, schedules=None)
    await command.run("disable", email=person.email)
    from deerflow.spaces.contract import SpaceConflict

    with pytest.raises(SpaceConflict, match="owned user Space"):
        await command.run("release-seat", email=person.email)
    assert (await repo.get_user_by_id(str(person.id))).released_at is None
    async with sf() as session:
        assert (await session.get(SpaceRow, company)).status == "active"


@pytest.mark.asyncio
async def test_stale_provider_sign_in_cannot_reclaim_email_after_release(account_storage):
    from datetime import UTC, datetime

    _app, repo, _sf, _files, _catalog, _company = account_storage
    person = await repo.create_user(User(email="person@example.com", oauth_provider="sso", oauth_id="person", oauth_issuer="https://identity.example.com"))
    command = AccountsCommand(repo, tokens=None, schedules=None)
    await command.run("disable", email=person.email)
    await command.run("release-seat", email=person.email)
    from app.gateway.auth.repositories.sqlite import ReleasedAccount

    with pytest.raises(ReleasedAccount):
        await repo.record_sign_in(str(person.id), system_role="user", oauth_issuer="https://identity.example.com", last_sign_in_at=datetime.now(UTC), email="person@example.com")
    current = await repo.get_user_by_id(str(person.id))
    assert current.email == released_email_for(str(person.id)) and current.email_released_from == "person@example.com"


@pytest.mark.asyncio
@pytest.mark.parametrize("write", ["update_user", "replace_password"])
async def test_stale_credential_write_cannot_reclaim_released_email(account_storage, write):
    from app.gateway.auth.repositories.sqlite import ReleasedAccount

    _app, repo, _sf, _files, _catalog, _company = account_storage
    person = await repo.create_user(User(email="person@example.com", password_hash="fixture-hash"))
    command = AccountsCommand(repo, tokens=None, schedules=None)
    await command.run("disable", email=person.email)
    await command.run("release-seat", email=person.email)
    if write == "update_user":
        with pytest.raises(ReleasedAccount):
            await repo.update_user(person)
    else:
        assert await repo.replace_password(str(person.id), "new-hash", new_email=person.email) is None
    current = await repo.get_user_by_id(str(person.id))
    assert current.email == released_email_for(str(person.id))
    assert current.email_released_from == person.email and current.password_hash == "fixture-hash"


@pytest.mark.asyncio
async def test_failed_postcommit_read_reports_durable_release_and_retry(account_storage, monkeypatch):
    _app, repo, _sf, _files, _catalog, _company = account_storage
    person = await repo.create_user(User(email="person@example.com", password_hash="fixture-hash"))
    command = AccountsCommand(repo, tokens=None, schedules=None)
    await command.run("disable", email=person.email)
    identity, account = await command._resolve(email=person.email, issuer=None, subject=None, allow_local=True)
    original = repo.get_user_by_id

    async def unavailable(_id):
        raise OSError("injected read failure")

    monkeypatch.setattr(repo, "get_user_by_id", unavailable)
    result = await command.release_seat(identity, account)
    assert result["released"] == str(person.id) and result["returncode"] == 2
    assert result["error"] == "cleanup_unconfirmed" and result["cleanup_pending"]
    monkeypatch.setattr(repo, "get_user_by_id", original)
    assert (await repo.get_user_by_id(str(person.id))).released_at is not None
    retried = await command.run("release-seat", account_id=str(person.id))
    assert retried["returncode"] == 0 and not retried["cleanup_pending"]


@pytest.mark.asyncio
async def test_sign_in_retry_rechecks_release_after_email_collision(account_storage, monkeypatch):
    from datetime import UTC, datetime

    from sqlalchemy.ext.asyncio import AsyncSession

    from app.gateway.auth.repositories.sqlite import ReleasedAccount

    _app, repo, _sf, _files, _catalog, _company = account_storage
    person = await repo.create_user(User(email="person@example.com", oauth_provider="sso", oauth_id="person", oauth_issuer="https://identity.example.com"))
    await repo.create_user(User(email="taken@example.com", password_hash="fixture-hash"))
    command = AccountsCommand(repo, tokens=None, schedules=None)
    await command.run("disable", email=person.email)
    identity, account = await command._resolve(email=person.email, issuer=None, subject=None)
    original = AsyncSession.rollback
    released = False

    async def release_during_retry(session):
        nonlocal released
        await original(session)
        if not released:
            released = True
            assert (await command.release_seat(identity, account))["returncode"] == 0

    monkeypatch.setattr(AsyncSession, "rollback", release_during_retry)
    with pytest.raises(ReleasedAccount):
        await repo.record_sign_in(str(person.id), system_role="admin", oauth_issuer=person.oauth_issuer, last_sign_in_at=datetime.now(UTC), email="taken@example.com")
    current = await repo.get_user_by_id(str(person.id))
    assert released and current.email == released_email_for(str(person.id))
    assert current.system_role == "user" and current.last_sign_in_at is None


@pytest.mark.asyncio
async def test_assignment_already_admitted_cannot_survive_concurrent_release(account_storage, monkeypatch):
    import asyncio

    app, repo, sf, _files, _catalog, _company = account_storage
    person = await repo.create_user(User(email="person@example.com", password_hash="fixture-hash"))
    entered, resume = asyncio.Event(), asyncio.Event()
    original = ss._user_assignment_admission

    async def paused(*args):
        await original(*args)
        entered.set()
        await resume.wait()

    monkeypatch.setattr(ss, "_user_assignment_admission", paused)
    assigning = asyncio.create_task(ss.assign_user_space(app, person))
    command = AccountsCommand(repo, tokens=None, schedules=None)

    async def disable_and_release():
        await command.run("disable", email=person.email)
        return await command.run("release-seat", account_id=str(person.id))

    try:
        await asyncio.wait_for(entered.wait(), timeout=10)
        releasing = asyncio.create_task(disable_and_release())
        await asyncio.sleep(0)
    finally:
        resume.set()
    assigned, result = await asyncio.wait_for(asyncio.gather(assigning, releasing), timeout=30)
    assert assigned is None and result["returncode"] == 0
    async with sf() as session:
        link = await session.get(StorageFeatureLinkRow, ("hm.user-space", "human:" + str(person.id)))
        assert link is not None and (await session.get(SpaceRow, link.space_id)).status == "deleted"
    await ss.assign_user_space(app, person)
    async with sf() as session:
        assert (await session.get(SpaceRow, link.space_id)).status == "deleted"


@pytest.mark.asyncio
@pytest.mark.parametrize("rival", ["enable", "update_user", "replace_password", "company_grant"])
async def test_release_serializes_concurrent_account_writes(account_storage, rival):
    import asyncio

    app, repo, sf, _files, _catalog, company = account_storage
    person = await repo.create_user(User(email="person@example.com", password_hash="fixture-hash"))
    await ss.assign_user_space(app, person)
    command = AccountsCommand(repo, tokens=None, schedules=None)
    await command.run("disable", email=person.email)

    async def competing():
        if rival == "enable":
            return await command.run("enable", account_id=str(person.id))
        if rival == "update_user":
            return await repo.update_user(person)
        if rival == "replace_password":
            return await repo.replace_password(str(person.id), "replacement", new_email=person.email)
        return await ss.grant_company_space(app, str(person.id))

    released, other = await asyncio.wait_for(asyncio.gather(command.run("release-seat", account_id=str(person.id)), competing(), return_exceptions=True), timeout=30)
    assert not isinstance(released, BaseException), released
    current = await repo.get_user_by_id(str(person.id))
    if released.get("error") == "not_disabled":
        assert rival == "enable" and other["returncode"] == 0
        assert current.released_at is None and current.disabled_at is None
    else:
        assert released["returncode"] == 0 and current.released_at is not None
        assert current.email == released_email_for(str(person.id))
        assert current.email_released_from == "person@example.com" and current.disabled_at is not None
        refused = await command.run("enable", account_id=str(person.id))
        assert refused["error"] == "released"
        from deerflow.spaces.contract import InvalidPrincipal, Permission, PrincipalRef

        with pytest.raises(InvalidPrincipal):
            async with app.state.storage_spaces.registry.admitted(actor=PrincipalRef("human", str(person.id)), requests={company: (Permission.READ, None)}):
                raise AssertionError("Released accounts must not enter storage admission")
