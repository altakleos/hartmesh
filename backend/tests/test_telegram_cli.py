"""Offline operator JSON, privacy and existing-database behavior."""

import asyncio
import json
import os
import shutil
import subprocess
import sys
import threading
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
import pytest_asyncio
from _telegram_repository_test_support import add_link, rows
from _telegram_repository_test_support import db as db
from sqlalchemy import event, select, text, update

from app.telegram_bots.cipher import token_cipher, wrap_token
from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow, TelegramDeploymentRow, TelegramOutboxRow
from deerflow.persistence.user.access import DisabledIdentityRow, IdentityHoldRow, RoleLimitRow
from deerflow.persistence.user.model import UserRow


def app_config(**values):
    from deerflow.config.app_config import AppConfig

    return AppConfig.model_validate({"sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"}, **values})


@pytest.fixture
def cli():
    from app.telegram_bots import cli

    return cli


@pytest_asyncio.fixture(autouse=True)
async def isolated_engine():
    from deerflow.persistence import engine

    previous = engine._engine, engine._session_factory
    close = engine.close_engine
    dependencies = sys.modules.get("app.gateway.deps")
    previous_accounts = (dependencies._cached_repo, dependencies._cached_local_provider) if dependencies else (None, None)
    engine._engine = engine._session_factory = None
    try:
        yield
    finally:
        try:
            await close()
        finally:
            engine._engine, engine._session_factory = previous
            dependencies = sys.modules.get("app.gateway.deps")
            if dependencies is not None:
                dependencies._cached_repo, dependencies._cached_local_provider = previous_accounts


async def copy_sqlite_file(db, target):
    # The fixture runs in WAL, so the main file alone can lack committed rows.
    async with db.engine.connect() as connection:
        await connection.exec_driver_sql("PRAGMA wal_checkpoint(TRUNCATE)")
    await asyncio.to_thread(shutil.copyfile, db.engine.url.database, target)


async def configured_database(db):
    if db.dialect == "sqlite":
        config = app_config(database={"backend": "sqlite", "sqlite_dir": str(Path(db.engine.url.database).parent), "echo_sql": True})
        await copy_sqlite_file(db, config.database.sqlite_path)
        return config
    async with db.engine.connect() as connection:
        schema = await connection.scalar(text("SELECT current_schema()"))
    return app_config(database={"backend": "postgres", "postgres_url": db.engine.url.set(drivername="postgresql").render_as_string(hide_password=False), "postgres_schema": schema, "echo_sql": True})


@pytest_asyncio.fixture
async def accounts(db):
    async with db.engine.begin() as connection:
        await connection.run_sync(lambda sync: UserRow.metadata.create_all(sync, tables=[UserRow.__table__, DisabledIdentityRow.__table__, IdentityHoldRow.__table__, RoleLimitRow.__table__]))
    owner = str(uuid4())
    async with db.sessions.begin() as session:
        session.add(UserRow(id=owner, email="fixture@example.com", password_hash="unusable-fixture", system_role="user"))
    return owner


@pytest.mark.parametrize(
    "arguments,command",
    [([], None), (["--help"], None), (["unknown"], "unknown"), (["status", "--owner"], "status"), (["remove"], "remove"), (["remove", "--owner", "a", "--local-account", "b"], "remove"), (["adopt", "--owner", "a"], "adopt")],
)
def test_parser_always_one_sorted_refusal(cli, capsys, monkeypatch, arguments, command):
    async def forbidden(*args, **kwargs):
        pytest.fail("invalid arguments opened config or persistence")

    monkeypatch.setattr(cli, "run_command", forbidden)
    assert cli.main(arguments) == 1
    captured = capsys.readouterr()
    document = json.loads(captured.out)
    assert captured.out == json.dumps(document, sort_keys=True) + "\n"
    assert document == {"command": command, "returncode": 1, "error": document["error"], "note": "refused; nothing was changed"}
    assert not captured.err


def test_unexpected_error_has_safe_single_document(cli, capsys, monkeypatch):
    token = "9000000001:" + "A" * 35

    async def broken(*args, **kwargs):
        raise RuntimeError("fixture failure " + token + " chat=7000000001 group=-1001234567890 @private_person")

    monkeypatch.setattr(cli, "run_command", broken)
    assert cli.main(["resume"]) == 1
    captured = capsys.readouterr()
    document = json.loads(captured.out)
    assert document["note"] == "failed; run the verb again"
    assert document["error"].startswith("RuntimeError: fixture failure")
    assert token not in captured.out + captured.err
    assert "7000000001" not in captured.out + captured.err
    assert "1001234567890" not in captured.out + captured.err
    assert "private_person" not in captured.out + captured.err


@pytest.mark.asyncio
async def test_status_projects_only_safe_current_generation_facts(cli, db, accounts, monkeypatch):
    monkeypatch.setenv("HARTMESH_PROVIDER_KEYS_SECRET", "current-fixture-secret-" + "x" * 32)
    monkeypatch.delenv("HARTMESH_PROVIDER_KEYS_SECRET_PREVIOUS", raising=False)
    owner = accounts
    link = await add_link(db, owner=owner)
    now = datetime.now(UTC)
    cipher = token_cipher(os.environ)
    secret_token = f"{link.bot_id}:" + "A" * 35
    async with db.sessions.begin() as session:
        await session.execute(
            update(TelegramBotLinkRow)
            .where(TelegramBotLinkRow.id == link.id)
            .values(
                encrypted_token=wrap_token(cipher, link.bot_id, secret_token),
                health="conflict",
                webhook_host="private-webhook.example",
                chat_id=7000000001,
                telegram_user_id=7000000001,
                connected_name="Private Person",
                last_error="Conflict: private details 7000000001",
                binding_generation=2,
            )
        )
        for generation, state, degraded in [(2, "pending", True), (2, "sending", False), (2, "failed", False), (1, "failed", False), (1, "pending", True)]:
            session.add(
                TelegramOutboxRow(
                    id=uuid4().hex,
                    link_id=link.id,
                    owner_user_id=owner,
                    dedupe_key=uuid4().hex,
                    class_="service",
                    binding_generation=generation,
                    payload={},
                    parts=[],
                    state=state,
                    not_before=now,
                    created_at=now,
                    degraded_at=now if degraded else None,
                )
            )
    before = [(row.id, row.updated_at, row.encrypted_token, row.notices) for row in await rows(db, TelegramBotLinkRow)]
    from deerflow.config.app_config import AppConfig

    config = AppConfig.model_validate({"sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"}, "telegram_bots": {}, "channel_connections": {"enabled": True, "require_bound_identity": True}})
    guard = cli.CopyGuard(db.sessions, host_reader=lambda: "fixture-host")
    result = await cli.status(db.sessions, config=config, guard=guard)
    assert result["links"] == 1 and result["people"] == 1
    assert result["tokens"] == {"current": 1, "previous": 0, "unreadable": 0}
    assert result["by_health"]["conflict"] == 1 and result["by_state"]["held"] == 0
    item = result["attention"][0]
    assert item["delivery_pending"] == 2 and item["delivery_failed"] == 1 and item["delivery_degraded"] is True
    assert item["last_error"] == "Conflict" and item["webhook"] is True
    assert result["host_id"] is None and result["this_host_id"] == "fixture-host"
    serialized = json.dumps(result)
    for private in (secret_token, "private-webhook.example", "7000000001", "Private Person", "private details", link.bot_username, "wrapped-fixture"):
        assert private not in serialized
    assert [(row.id, row.updated_at, row.encrypted_token, row.notices) for row in await rows(db, TelegramBotLinkRow)] == before
    assert len(await rows(db, TelegramDeploymentRow)) == 1  # add_link's existing capacity singleton
    selected = await cli.status(db.sessions, config=config, guard=guard, owner=owner)
    assert len(selected["links"]) == 1 and selected["attention"] == []


@pytest.mark.asyncio
async def test_remove_refuses_live_account_and_local_form_is_repeatable(cli, db, accounts):
    from deerflow.config.app_config import AppConfig

    config = AppConfig(sandbox={"use": "deerflow.sandbox.local:LocalSandboxProvider"})
    link = await add_link(db, owner=accounts)
    with pytest.raises(cli.CommandRefused, match="is not turned off"):
        await cli.remove(db.sessions, config=config, owner=accounts)
    assert await db.links.get(link.id) is not None
    result = await cli.remove(db.sessions, config=config, local_account="FIXTURE@example.com")
    assert result["owner"] == accounts and result["removed"] == 1 and result["local_account"] is True
    assert result["note"].startswith("the account itself stays usable for whoever holds its password; ")
    assert set(result["bots"][0]) == {"link", "bot_id", "agent_kind", "agent_ref", "origin"}
    repeated = await cli.remove(db.sessions, config=config, local_account="fixture@example.com")
    assert repeated["removed"] == 0 and repeated["note"].startswith("the account itself stays usable")
    async with db.sessions() as session:
        assert await session.scalar(select(UserRow.id).where(UserRow.id == accounts)) == accounts


@pytest.mark.asyncio
@pytest.mark.parametrize("current,previous,secret", [(None, "bad", "absent"), ("bad", None, "invalid"), ("x" * 40, "bad", "invalid")])
async def test_status_secret_precedence_and_no_singleton_write(cli, db, monkeypatch, current, previous, secret):
    for name, value in [("HARTMESH_PROVIDER_KEYS_SECRET", current), ("HARTMESH_PROVIDER_KEYS_SECRET_PREVIOUS", previous)]:
        if value is None:
            monkeypatch.delenv(name, raising=False)
        else:
            monkeypatch.setenv(name, value)
    monkeypatch.delenv("DEER_FLOW_AUTH_DISABLED", raising=False)
    config = app_config(telegram_bots={}, channel_connections={"enabled": True, "require_bound_identity": True})
    result = await cli.status(db.sessions, config=config, guard=cli.CopyGuard(db.sessions, host_reader=lambda: None))
    assert result["secret"] == secret
    assert result["reason"] == ("secret-missing" if secret == "absent" else "secret-invalid")
    assert result["tokens"] == {key: None for key in ("current", "previous", "unreadable")}
    assert result["host_id"] is None and result["this_host_id"] is None
    assert result["links"] == 0 and result["people"] == 0
    assert not await rows(db, TelegramDeploymentRow)


@pytest.mark.asyncio
async def test_status_current_previous_and_unreadable_never_rewrap(cli, db, monkeypatch):
    old, new = "old-fixture-" + "x" * 40, "new-fixture-" + "y" * 40
    old_cipher = token_cipher({"HARTMESH_PROVIDER_KEYS_SECRET": old})
    new_cipher = token_cipher({"HARTMESH_PROVIDER_KEYS_SECRET": new})
    originals = []
    for number, cipher in enumerate((new_cipher, old_cipher, None), start=1):
        link = await add_link(db, bot=9000000000 + number)
        encrypted = wrap_token(cipher, link.bot_id, f"{link.bot_id}:" + "B" * 35) if cipher else "broken-fixture-envelope"
        async with db.sessions.begin() as session:
            await session.execute(update(TelegramBotLinkRow).where(TelegramBotLinkRow.id == link.id).values(encrypted_token=encrypted))
        originals.append((link.id, encrypted))
    monkeypatch.setenv("HARTMESH_PROVIDER_KEYS_SECRET", new)
    monkeypatch.setenv("HARTMESH_PROVIDER_KEYS_SECRET_PREVIOUS", old)
    result = await cli.status(db.sessions, config=app_config(), guard=cli.CopyGuard(db.sessions, host_reader=lambda: None))
    assert result["tokens"] == {"current": 1, "previous": 1, "unreadable": 1}
    assert sorted((row.id, row.encrypted_token) for row in await rows(db, TelegramBotLinkRow)) == sorted(originals)


@pytest.mark.asyncio
async def test_missing_owner_refusal_creates_nothing(cli, db, accounts):
    missing = str(uuid4())
    with pytest.raises(cli.CommandRefused, match="no account has id"):
        await cli.status(db.sessions, config=app_config(), guard=cli.CopyGuard(db.sessions, host_reader=lambda: None), owner=missing)
    with pytest.raises(cli.CommandRefused, match="no account has id"):
        await cli.remove(db.sessions, config=app_config(), owner=missing)
    assert not await rows(db, TelegramDeploymentRow)


@pytest.mark.asyncio
@pytest.mark.parametrize("refusal", ["disabled", "released", "provider", "inert"])
async def test_removal_uses_current_derived_access_and_provider_identity(cli, db, accounts, refusal):
    from deerflow.config.auth_config import LOCAL_ACCOUNT_ISSUER

    link = await add_link(db, owner=accounts)
    now = datetime.now(UTC)
    async with db.sessions.begin() as session:
        if refusal == "disabled":
            session.add(DisabledIdentityRow(issuer=LOCAL_ACCOUNT_ISSUER, subject=accounts, disabled_at=now))
        elif refusal == "released":
            await session.execute(update(UserRow).where(UserRow.id == accounts).values(released_at=now, email_released_from="fixture@example.com"))
        elif refusal == "provider":
            await session.execute(update(UserRow).where(UserRow.id == accounts).values(oauth_provider="fixture-idp", oauth_id="fixture-subject"))
    config = app_config()
    if refusal == "inert":
        # Exercise the prepared derived-mode check; production config validation
        # separately requires an enabled OIDC provider for sign-on-only mode.
        config = config.model_copy(update={"auth": config.auth.model_copy(update={"local": config.auth.local.model_copy(update={"enabled": False})})})
    if refusal in ("disabled", "released"):
        result = await cli.remove(db.sessions, config=config, owner=accounts)
        assert result["removed"] == 1 and result["local_account"] is False
    elif refusal == "provider":
        with pytest.raises(cli.CommandRefused, match="not-local-account"):
            await cli.remove(db.sessions, config=config, local_account="fixture@example.com")
        assert await db.links.get(link.id) is not None
    else:
        with pytest.raises(cli.CommandRefused, match="is not turned off"):
            await cli.remove(db.sessions, config=config, owner=accounts)
        assert await db.links.get(link.id) is not None


@pytest.mark.asyncio
async def test_status_uses_one_actual_snapshot(cli, db, accounts):
    # WAL allows the independent SQLite writer to commit while status retains
    # its established read snapshot. PostgreSQL needs REPEATABLE READ as well.
    if db.dialect == "sqlite":
        async with db.engine.connect() as connection:
            await connection.execute(text("PRAGMA journal_mode=WAL"))
    link = await add_link(db, owner=accounts)
    now = datetime.now(UTC)

    class ConcurrentGuard(cli.CopyGuard):
        async def peek(self, *, session=None):
            result = await super().peek(session=session)
            async with db.sessions.begin() as writer:
                await writer.execute(update(TelegramBotLinkRow).where(TelegramBotLinkRow.id == link.id).values(binding_generation=1, health="stopped"))
                writer.add(TelegramOutboxRow(id=uuid4().hex, link_id=link.id, owner_user_id=accounts, dedupe_key=uuid4().hex, class_="service", binding_generation=1, payload={}, parts=[], state="failed", not_before=now, created_at=now))
            return result

    result = await cli.status(db.sessions, config=app_config(), guard=ConcurrentGuard(db.sessions, host_reader=lambda: None), owner=accounts)
    assert result["links"][0]["health"] == "ok"
    assert result["links"][0]["delivery_failed"] == 0
    assert (await db.links.get(link.id)).health == "stopped"


@pytest.mark.asyncio
async def test_status_caps_attention_and_retains_total_counts(cli, db, accounts):
    now = datetime.now(UTC)
    async with db.sessions.begin() as session:
        for number in range(203):
            session.add(
                TelegramBotLinkRow(
                    id=f"{number:032x}",
                    owner_user_id=accounts,
                    agent_kind="custom_agent",
                    agent_ref=f"target-{number}",
                    bot_id=9000000000 + number,
                    bot_username="private_fixture_bot",
                    encrypted_token="broken",
                    state="blocked",
                    health="ok",
                    created_at=now,
                    updated_at=now,
                )
            )
    result = await cli.status(db.sessions, config=app_config(), guard=cli.CopyGuard(db.sessions, host_reader=lambda: None))
    assert result["links"] == 203 and result["by_state"]["blocked"] == 203
    assert len(result["attention"]) == 200
    assert [item["link"] for item in result["attention"]] == [f"{number:032x}" for number in range(200)]
    assert not await rows(db, TelegramDeploymentRow)


@pytest.mark.asyncio
async def test_existing_database_lifecycle_and_missing_tables(cli, db, accounts, monkeypatch):
    from deerflow.persistence.engine import get_engine

    if db.dialect == "sqlite":
        database = {"backend": "sqlite", "sqlite_dir": str(Path(db.engine.url.database).parent), "echo_sql": True}
        # DatabaseConfig's application filename is fixed; our isolated fixture
        # is named differently. Prepare that one existing file explicitly.
        config = app_config(database=database)
        target = Path(config.database.sqlite_path)
        await copy_sqlite_file(db, target)
    else:
        async with db.engine.connect() as connection:
            schema = await connection.scalar(text("SELECT current_schema()"))
        database = {"backend": "postgres", "postgres_url": db.engine.url.set(drivername="postgresql").render_as_string(hide_password=False), "postgres_schema": schema, "echo_sql": True}
        config = app_config(database=database)
    monkeypatch.setattr(cli, "CopyGuard", lambda sessions: __import__("app.telegram_bots.deployment", fromlist=["CopyGuard"]).CopyGuard(sessions, host_reader=lambda: None))
    result = await cli.run_command("status", app_config=config)
    assert result["returncode"] == 0 and get_engine() is None
    async with db.engine.begin() as connection:
        await connection.execute(text("DROP TABLE telegram_deployment"))
    if db.dialect == "sqlite":
        await copy_sqlite_file(db, target)
    with pytest.raises(cli.CommandRefused, match="has no Telegram tables"):
        await cli.run_command("status", app_config=config)
    assert get_engine() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("before_close", [False, True])
async def test_committed_result_survives_cleanup_failure(cli, db, accounts, monkeypatch, capsys, before_close):
    from deerflow.persistence import engine

    await add_link(db, owner=accounts)
    config = await configured_database(db)
    close = engine.close_engine
    attempts = []

    async def failing_cleanup():
        attempts.append(True)
        if before_close and len(attempts) == 1:
            raise RuntimeError("cleanup fixture failure chat=-1001234567890")
        await close()
        if not before_close:
            raise RuntimeError("cleanup fixture failure chat=7000000001")

    monkeypatch.setattr(engine, "close_engine", failing_cleanup)
    result = await cli.run_command("remove", app_config=config, local_account="fixture@example.com")
    assert result["removed"] == 1 and result["returncode"] == 0
    monkeypatch.setattr(engine, "close_engine", close)
    assert engine.get_engine() is None
    assert len(attempts) == (2 if before_close else 1)
    repeated = await cli.run_command("remove", app_config=config, local_account="fixture@example.com")
    assert repeated["removed"] == 0
    captured = capsys.readouterr()
    assert not captured.out
    assert "cleanup" in captured.err and "7000000001" not in captured.err
    assert "1001234567890" not in captured.err


@pytest.mark.asyncio
async def test_original_refusal_survives_cleanup_failure(cli, db, accounts, monkeypatch, capsys):
    from deerflow.persistence import engine

    config = await configured_database(db)
    close = engine.close_engine

    async def failing_cleanup():
        await close()
        raise RuntimeError("cleanup fixture failure")

    monkeypatch.setattr(engine, "close_engine", failing_cleanup)
    try:
        with pytest.raises(cli.CommandRefused, match="no account has the email"):
            await cli.run_command("remove", app_config=config, local_account="unknown@example.com")
    finally:
        monkeypatch.setattr(engine, "close_engine", close)
    assert "cleanup" in capsys.readouterr().err


@pytest.mark.asyncio
async def test_operator_guard_verbs_use_real_persistence_without_tokens(cli, db, accounts, monkeypatch):
    from deerflow.persistence import engine

    link = await add_link(db, owner=accounts)
    config = await configured_database(db)
    real_guard = cli.CopyGuard
    observed = []
    current = ["host-a"]

    def read_host():
        observed.append(current[0])
        return current[0]

    monkeypatch.setattr(cli, "CopyGuard", lambda sessions: real_guard(sessions, host_reader=read_host))
    monkeypatch.setenv("HARTMESH_PROVIDER_KEYS_SECRET", "invalid")
    adopted = await cli.run_command("adopt", app_config=config)
    assert adopted["from"] is None and adopted["host_id"] == "host-a" and adopted["links"] == 1 and adopted["verdict"] == "adopted"
    again = await cli.run_command("adopt", app_config=config)
    assert again["verdict"] == "already_adopted"
    before_resume = len(observed)
    assert (await cli.run_command("resume", app_config=config))["verdict"] == "already_running"
    assert len(observed) == before_resume
    current[0] = "host-b"
    assert (await cli.run_command("status", app_config=config))["polling_state"] == "host_changed"
    assert (await cli.run_command("resume", app_config=config))["verdict"] == "already_running"
    moved = await cli.run_command("adopt", app_config=config)
    assert moved["from"] == "host-a" and moved["host_id"] == "host-b"
    await engine.init_engine_from_config(config.database, existing_only=True)
    try:
        sessions = engine.get_session_factory()
        await real_guard(sessions, host_reader=read_host).stop_for_broad_conflict()
        async with sessions() as session:
            paused = await session.get(TelegramBotLinkRow, link.id)
            assert paused.health == "ok" and paused.acts_paused_since is None
    finally:
        await engine.close_engine()
    with pytest.raises(cli.CommandRefused, match="another deployment polls"):
        await cli.run_command("adopt", app_config=config)
    reads = len(observed)
    resumed = await cli.run_command("resume", app_config=config)
    assert resumed["verdict"] == "resumed" and resumed["links"] == 1
    assert len(observed) == reads
    current[0] = None
    with pytest.raises(cli.CommandRefused, match="Docker daemon did not answer"):
        await cli.run_command("adopt", app_config=config)


@pytest.mark.asyncio
async def test_local_removal_rechecks_preflight_identity_inside_writer(cli, db, accounts, monkeypatch):
    from app.gateway.deps import bind_user_repo

    link = await add_link(db, owner=accounts)
    users = bind_user_repo(db.sessions)
    original = users.get_user_by_email

    async def changed_email(email):
        user = await original(email)
        async with db.sessions.begin() as writer:
            await writer.execute(update(UserRow).where(UserRow.id == accounts).values(email="moved@example.com"))
        return user

    monkeypatch.setattr(users, "get_user_by_email", changed_email)
    with pytest.raises(cli.CommandRefused, match="no account has the email"):
        await cli.remove(db.sessions, config=app_config(), local_account="fixture@example.com")
    assert await db.links.get(link.id) is not None


@pytest.mark.asyncio
async def test_host_removal_and_web_admission_share_global_capacity_lock(cli, db, accounts, monkeypatch):
    from app.gateway.deps import bind_user_repo
    from app.telegram_bots.config import TelegramBotsConfig
    from app.telegram_bots.links import BotLinkRepository

    original = await add_link(db, owner=accounts)
    other = str(uuid4())
    await db.settings.ensure(other, timezone="UTC", locale="en-US")
    users = bind_user_repo(db.sessions)
    read = users.get_user_by_id
    entered, release, admission_issued = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def blocked_account(owner, *, session=None):
        assert session is not None and session.in_transaction()
        user = await read(owner, session=session)
        entered.set()
        await release.wait()
        return user

    monkeypatch.setattr(users, "get_user_by_id", blocked_account)
    config = app_config(telegram_bots={"max_bots": 1})
    web = BotLinkRepository(db.sessions, db.connections, TelegramBotsConfig(max_bots=1))

    def observe(connection, cursor, statement, parameters, context, many):
        if statement.upper().startswith("BEGIN IMMEDIATE") or "telegram_deployment" in statement:
            admission_issued.set()

    removing = asyncio.create_task(cli.remove(db.sessions, config=config, local_account="fixture@example.com"))
    adding = None
    try:
        await asyncio.wait_for(entered.wait(), 3)
        event.listen(db.engine.sync_engine, "before_cursor_execute", observe)
        adding = asyncio.create_task(web.add(owner_user_id=other, agent_kind="custom_agent", agent_ref="other-target", bot_id=9000000002, bot_username="fixture_other_bot", encrypted_token="broken-fixture-envelope", actor_id=other))
        await asyncio.wait_for(admission_issued.wait(), 3)
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(asyncio.shield(adding), 0.15)
        release.set()
        removed, admitted = await asyncio.wait_for(asyncio.gather(removing, adding), 5)
        assert removed["removed"] == 1 and removed["bots"][0]["link"] == original.id
        assert admitted.owner_user_id == other and await db.links.get(original.id) is None
        assert [row.id for row in await rows(db, TelegramBotLinkRow)] == [admitted.id]
    finally:
        release.set()
        if event.contains(db.engine.sync_engine, "before_cursor_execute", observe):
            event.remove(db.engine.sync_engine, "before_cursor_execute", observe)
        await asyncio.gather(*(task for task in (removing, adding) if task is not None), return_exceptions=True)


@pytest.mark.parametrize("failure", ["yaml", "validation", "sql"])
def test_low_level_diagnostics_never_dump_inputs(cli, capsys, monkeypatch, failure):
    from pydantic import BaseModel, ValidationError
    from sqlalchemy.exc import StatementError
    from yaml import YAMLError

    private = "private-payload-" + uuid4().hex

    async def broken(*args, **kwargs):
        if failure == "sql":
            raise StatementError("fixture " + private, "SELECT private", {"private": private}, ValueError(private))
        if failure == "yaml":
            raise YAMLError(private)

        class Validated(BaseModel):
            count: int

        raise ValidationError.from_exception_data("Validated", [{"type": "int_parsing", "loc": ("count",), "input": private}])

    monkeypatch.setattr(cli, "run_command", broken)
    assert cli.main(["status"]) == 1
    captured = capsys.readouterr()
    assert private not in captured.out + captured.err
    document = json.loads(captured.out)
    assert document["note"] == "failed; run the verb again"


@pytest.mark.asyncio
async def test_invalid_rendered_section_is_a_safe_configuration_refusal(cli):
    config = app_config(telegram_bots={"enabled": "private-invalid-input"})
    with pytest.raises(cli.CommandRefused, match="configuration .* cannot be loaded: ValidationError") as refusal:
        await cli.run_command("status", app_config=config)
    assert "private-invalid-input" not in str(refusal.value)


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", [False, True])
async def test_entry_loads_rendered_configuration_off_loop_before_opening_database(cli, monkeypatch, missing):
    from deerflow.persistence import engine

    loop_thread = threading.get_ident()
    observed = []

    def rendered(cls):
        observed.append(threading.get_ident())
        assert observed[-1] != loop_thread
        if missing:
            raise FileNotFoundError(2, "fixture configuration is missing", "private-config-name")
        return app_config()

    async def forbidden(*args, **kwargs):
        pytest.fail("invalid or memory configuration opened persistence")

    monkeypatch.setattr(cli.AppConfig, "from_file", classmethod(rendered))
    monkeypatch.setattr(engine, "init_engine_from_config", forbidden)
    pattern = "configuration /srv/hartmesh/home/config.yaml cannot be loaded" if missing else "database cannot be reached: the memory backend"
    with pytest.raises(cli.CommandRefused, match=pattern) as refusal:
        await cli.run_command("status")
    assert len(observed) == 1 and "private-config-name" not in str(refusal.value)


@pytest.mark.asyncio
async def test_missing_database_refusal_does_not_create_directory(cli, tmp_path):
    missing = tmp_path / "not-created"
    with pytest.raises(cli.CommandRefused, match="database cannot be reached"):
        await cli.run_command("status", app_config=app_config(database={"backend": "sqlite", "sqlite_dir": str(missing)}))
    assert not missing.exists()


@pytest.mark.asyncio
async def test_command_never_replaces_or_closes_an_existing_engine(cli, monkeypatch, tmp_path):
    from deerflow.persistence import engine

    existing = object()
    monkeypatch.setattr(engine, "get_engine", lambda: existing)

    async def forbidden(*args, **kwargs):
        pytest.fail("operator command replaced or closed another lifecycle's engine")

    monkeypatch.setattr(engine, "init_engine_from_config", forbidden)
    monkeypatch.setattr(engine, "close_engine", forbidden)
    with pytest.raises(cli.CommandRefused, match="already initialized"):
        await cli.run_command("status", app_config=app_config(database={"backend": "sqlite", "sqlite_dir": str(tmp_path)}))


@pytest.mark.parametrize("command", ["status", "adopt", "resume", "remove", "remove-unknown"])
def test_fresh_cli_process_never_imports_transport_or_runtime(tmp_path, command):
    program = """
import importlib.abc, json, os, socket, sys
class NoRuntime(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        blocked = ('app.telegram_bots.adapter', 'app.telegram_bots.producer', 'app.telegram_bots.polling',
                   'app.telegram_bots.supervisor', 'app.gateway.app', 'deerflow.config.managed_models')
        if fullname == 'telegram' or fullname.startswith('telegram.') or fullname in blocked:
            raise AssertionError('CLI imported forbidden runtime ' + fullname)
sys.meta_path.insert(0, NoRuntime())
def no_network(*args, **kwargs):
    raise AssertionError('operator CLI attempted network')
socket.create_connection = no_network
socket.socket.connect = no_network
socket.socket.connect_ex = no_network
socket.getaddrinfo = no_network
from app.telegram_bots import cli
from deerflow.config.app_config import AppConfig
from deerflow.persistence.engine import init_engine_from_config, get_session_factory, close_engine
config = AppConfig.model_validate({'sandbox':{'use':'deerflow.sandbox.local:LocalSandboxProvider'},
    'database': {'backend':'sqlite','sqlite_dir':os.environ['FIXTURE_SQLITE_DIR'],'echo_sql':True}})
async def prepare():
    await init_engine_from_config(config.database.model_copy(update={'echo_sql':False}))
    if os.environ['FIXTURE_COMMAND']=='remove':
        from uuid import uuid4
        from app.telegram_bots.config import TelegramBotsConfig
        from app.telegram_bots.links import BotLinkRepository
        from deerflow.persistence.channel_connections import ChannelConnectionRepository
        from deerflow.persistence.user.model import UserRow
        owner=str(uuid4())
        sessions=get_session_factory()
        async with sessions.begin() as writer:
            writer.add(UserRow(id=owner,email='fixture@example.com',password_hash='unusable-fixture',system_role='user'))
        links=BotLinkRepository(sessions,ChannelConnectionRepository(sessions),TelegramBotsConfig())
        await links.settings.ensure(owner,timezone='UTC',locale='en-US')
        await links.add(owner_user_id=owner,agent_kind='lead_agent',agent_ref='lead_agent',bot_id=9000000001,
            bot_username='fixture_private_bot',encrypted_token='broken-fixture-envelope',actor_id=owner)
    await close_engine()
import asyncio
asyncio.run(prepare())
real_guard = cli.CopyGuard
cli.CopyGuard = lambda sessions: real_guard(sessions, host_reader=lambda:'fixture-host')
async def run(command, **options):
    return await original(command, app_config=config, **options)
original=cli.run_command
cli.run_command=run
arguments=[os.environ['FIXTURE_COMMAND']]
if arguments[0]=='remove': arguments += ['--local-account', 'fixture@example.com']
elif arguments[0]=='remove-unknown': arguments=['remove','--local-account','unknown@example.com']
raise SystemExit(cli.main(arguments))
"""
    env = {**os.environ, "FIXTURE_SQLITE_DIR": str(tmp_path), "FIXTURE_COMMAND": command}
    completed = subprocess.run([sys.executable, "-c", program], capture_output=True, text=True, timeout=30, env=env)
    document = json.loads(completed.stdout)
    assert document["command"] == ("remove" if command == "remove-unknown" else command)
    assert document["returncode"] == completed.returncode == (1 if command == "remove-unknown" else 0), completed.stderr + completed.stdout
    if command == "remove":
        assert document["removed"] == 1 and len(document["bots"]) == 1
    assert completed.stdout == json.dumps(document, sort_keys=True) + "\n"
    assert "forbidden" not in completed.stderr
