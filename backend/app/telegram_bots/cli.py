"""Local database/Docker operator commands; never starts a Gateway or bot client."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
from collections import Counter
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta

from pydantic import ValidationError
from sqlalchemy import case, func, inspect, select, text
from sqlalchemy.exc import SQLAlchemyError
from yaml import YAMLError

from app.gateway.auth.mode import ACCOUNT_DISABLED, account_refusal, is_provider_account
from app.gateway.provider_keys.cipher import UnreadableProviderKey, WrappingKeyInvalid
from app.telegram_bots.cipher import token_cipher, unwrap_token
from app.telegram_bots.config import load_config
from app.telegram_bots.deployment import CopyGuard, GuardRefused, feature_reason, static_cause
from deerflow.config.app_config import AppConfig
from deerflow.persistence.telegram_bots import model
from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow, TelegramDeploymentRow, TelegramInboundMarkRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained

COMMANDS = ("status", "adopt", "resume", "remove")
POLLING_STALE_AFTER = timedelta(minutes=5)
_STATES = ("waiting_connect", "connected", "held", "blocked")
_HEALTH = ("ok", "token_rejected", "conflict", "unreachable", "secret_unreadable", "agent_unavailable", "stopped", "policy_off")
_KINDS = ("lead_agent", "custom_agent", "instance")
_ERRORS = frozenset(("BadRequest", "Conflict", "Forbidden", "InvalidToken", "NetworkError", "RetryAfter", "TimedOut", "TelegramError", "UnreadableProviderKey"))
_REMOVE_FLAGS = "give exactly one of --owner <account id> and --local-account <email>"


class CommandRefused(Exception):
    """A refused invocation rolls back its unit before reaching the JSON envelope."""


def _safe_text(value: object) -> str:
    """Keep diagnostics useful without printing transport credentials or identities."""
    result = str(value)
    for name, secret in os.environ.items():
        if any(word in name.upper() for word in ("SECRET", "TOKEN", "PASSWORD", "API_KEY")) and len(secret) >= 8:
            result = result.replace(secret, "[redacted]")
    result = re.sub(r"(?:bot)?[0-9]{5,16}:[A-Za-z0-9_-]{20,}", "[redacted]", result)
    result = re.sub(r"https?://\S+", "[redacted-url]", result)
    result = re.sub(r"(?<![\w.-])@[\w]{3,}", "[redacted-name]", result)
    result = re.sub(r"(?<![\w-])-?[0-9]{7,}(?![\w-])", "[redacted-id]", result)
    result = re.sub(r"(?:fernet:v1:)?[A-Za-z0-9_=-]{80,}", "[redacted]", result)
    return " ".join(result.split())[:400]


def _failure_text(error: Exception) -> str:
    # These libraries include input values, SQL parameters, URLs and local paths
    # in their string representation. Their class remains a useful diagnostic.
    if isinstance(error, (ValidationError, YAMLError)):
        return f"{type(error).__name__}: configuration validation failed"
    if isinstance(error, SQLAlchemyError):
        return f"{type(error).__name__}: database operation failed"
    if isinstance(error, OSError):
        return f"{type(error).__name__}: {_safe_text(error.strerror or 'operating system operation failed')}"
    return f"{type(error).__name__}: {_safe_text(error)}"


def _success(command: str, note: str, **fields) -> dict:
    return {"command": command, "returncode": 0, "note": note, **fields}


def _iso(value: datetime | None) -> str | None:
    return None if value is None else (value.replace(tzinfo=UTC) if value.tzinfo is None else value).isoformat()


@asynccontextmanager
async def _snapshot(session_factory):
    async with session_factory() as session:
        if session.get_bind().dialect.name == "postgresql":
            await session.connection(execution_options={"isolation_level": "REPEATABLE READ"})
            await session.execute(text("SET TRANSACTION READ ONLY"))
        else:
            # sqlite3's legacy mode does not start a transaction for SELECT.
            await session.execute(text("BEGIN"))
        try:
            yield session
        finally:
            await session.rollback()


def _cipher():
    if not os.environ.get("HARTMESH_PROVIDER_KEYS_SECRET", "").strip():
        return "absent", None
    try:
        return "set", token_cipher(os.environ)
    except WrappingKeyInvalid:
        return "invalid", None


def _classification(cipher, row) -> str | None:
    if cipher is None:
        return None
    try:
        opened = unwrap_token(cipher, row.bot_id, row.encrypted_token)
    except UnreadableProviderKey:
        return "unreadable"
    try:
        return "previous" if opened.under_previous else "current"
    finally:
        del opened


def _polling_stale(row, now: datetime) -> bool:
    """A poll stamps `last_ok_at` once a minute, so an older stamp means no poller is working."""
    stamp = row.last_ok_at
    if row.state != "connected" or row.health != "ok" or stamp is None:
        return False
    return now - (stamp.replace(tzinfo=UTC) if stamp.tzinfo is None else stamp) > POLLING_STALE_AFTER


def _link_document(row, token, delivery, inputs, *, polling_stale=False) -> dict:
    pending, degraded, failed = delivery
    open_inputs, oldest_input = inputs
    # Explicit projection: do not serialize an ORM row or a bot-link dataclass.
    error = (row.last_error or "").split(":", 1)[0].strip()
    return {
        "link": row.id,
        "bot_id": row.bot_id,
        "owner": row.owner_user_id,
        "agent_kind": row.agent_kind,
        "agent_ref": row.agent_ref,
        "origin": row.origin,
        "state": row.state,
        "health": row.health,
        "conflict_since": _iso(row.conflict_since),
        "probe_until": _iso(row.probe_until),
        "webhook": bool(row.webhook_host),
        "acts_paused_since": _iso(row.acts_paused_since),
        "limited_since": _iso(row.limited_since),
        "last_ok_at": _iso(row.last_ok_at),
        "last_error": error if error in _ERRORS else None,
        "token": token,
        "delivery_pending": pending,
        "delivery_degraded": degraded,
        "delivery_failed": failed,
        "open_inputs": open_inputs,
        "oldest_open_input_at": _iso(oldest_input),
        "polling_stale": polling_stale,
    }


async def status(session_factory, *, config: AppConfig, guard: CopyGuard, owner: str | None = None) -> dict:
    """One read-only snapshot; opened tokens never enter a result or a write."""
    from app.telegram_bots.voice import resolve_transcriber

    telegram = load_config(config)
    secret, cipher = _cipher()
    cause = static_cause(telegram, config, os.environ)
    async with _snapshot(session_factory) as session:
        polling_state = await guard.peek(session=session)
        deployment = await session.get(TelegramDeploymentRow, 1)
        if owner is not None:
            from app.gateway.deps import bind_user_repo

            if await bind_user_repo(session_factory).get_user_by_id(owner, session=session) is None:
                raise CommandRefused(f"no account has id {owner}")
        query = select(TelegramBotLinkRow).order_by(TelegramBotLinkRow.updated_at, TelegramBotLinkRow.id)
        if owner is not None:
            query = query.where(TelegramBotLinkRow.owner_user_id == owner)
        links = list(await session.scalars(query))
        outbox = TelegramOutboxRow
        aggregates = select(
            outbox.link_id,
            func.sum(case((outbox.state.in_(("pending", "sending")), 1), else_=0)),
            func.max(case((outbox.state.in_(("pending", "sending")) & outbox.degraded_at.is_not(None), 1), else_=0)),
            func.sum(case((outbox.state == "failed", 1), else_=0)),
        ).join(TelegramBotLinkRow, (TelegramBotLinkRow.id == outbox.link_id) & (TelegramBotLinkRow.binding_generation == outbox.binding_generation))
        if owner is not None:
            aggregates = aggregates.where(TelegramBotLinkRow.owner_user_id == owner)
        delivery = {key: (int(pending), bool(degraded), int(failed)) for key, pending, degraded, failed in await session.execute(aggregates.group_by(outbox.link_id))}
        mark = TelegramInboundMarkRow
        open_marks = (
            select(mark.link_id, func.count(), func.min(mark.accepted_at)).join(TelegramBotLinkRow, (TelegramBotLinkRow.id == mark.link_id) & (TelegramBotLinkRow.binding_generation == mark.binding_generation)).where(mark.state != "done")
        )
        if owner is not None:
            open_marks = open_marks.where(TelegramBotLinkRow.owner_user_id == owner)
        inputs = {key: (int(count), oldest) for key, count, oldest in await session.execute(open_marks.group_by(mark.link_id))}
        classifications = [_classification(cipher, row) for row in links]
        now = datetime.now(UTC)
        documents = [
            _link_document(row, token, delivery.get(row.id, (0, False, 0)), inputs.get(row.id, (0, None)), polling_stale=cause is None and polling_state == "running" and _polling_stale(row, now))
            for row, token in zip(links, classifications, strict=True)
        ]
        host_id = deployment.host_id if deployment else None
        totals = {
            "people": len({row.owner_user_id for row in links}),
            "by_state": {key: sum(row.state == key for row in links) for key in _STATES},
            "by_health": {key: sum(row.health == key for row in links) for key in _HEALTH},
            "by_kind": {key: sum(row.agent_kind == key for row in links) for key in _KINDS},
            "acts_paused": sum(row.acts_paused_since is not None for row in links),
            "limited": sum(row.limited_since is not None for row in links),
        }
    if cause is None and polling_state != "running":
        cause = polling_state.replace("_", "-")
    reason = feature_reason(cause)
    attention = [item for item in documents if item["health"] != "ok" or item["state"] == "blocked" or item["acts_paused_since"] or item["limited_since"] or item["delivery_degraded"] or item["delivery_failed"] or item["polling_stale"]]
    transcriber = resolve_transcriber(telegram, models=config.models)
    voice = transcriber.provider if transcriber else ("off" if telegram.voice.provider == "off" else "unavailable")
    counts = Counter(classifications)
    note = f"telegram {'on' if telegram.enabled else 'off'}: {len(documents)} bot links of {totals['people']} people; {len(attention)} needs attention; the next start would find polling {polling_state}"
    return _success(
        "status",
        note,
        enabled=telegram.enabled,
        bots="ready" if cause is None else "unavailable",
        reason=reason,
        polling_state=polling_state,
        host_id=host_id,
        this_host_id=guard.this_host_id,
        secret=secret,
        public_base_url=telegram.public_base_url,
        voice=voice,
        limits={"deployment": telegram.max_bots, "per_person": telegram.max_bots_per_person},
        links=len(documents) if owner is None else documents,
        **totals,
        tokens={key: counts[key] if cipher is not None else None for key in ("current", "previous", "unreadable")},
        attention=attention[:200] if owner is None else [],
    )


async def remove(session_factory, *, config: AppConfig, owner: str | None = None, local_account: str | None = None) -> dict:
    from app.gateway.deps import bind_user_repo
    from app.telegram_bots.links import BotLinkRepository
    from deerflow.persistence.channel_connections import ChannelConnectionRepository

    if (owner is None) == (local_account is None):
        raise CommandRefused(_REMOVE_FLAGS)
    users = bind_user_repo(session_factory)
    if local_account is not None:
        preflight = await users.get_user_by_email(local_account)
        if preflight is None:
            raise CommandRefused(f"no account has the email {local_account}")
        owner = str(preflight.id)
    links = BotLinkRepository(session_factory, ChannelConnectionRepository(session_factory), load_config(config))
    async with links.capacity_writer(owner) as writer:
        user = await users.get_user_by_id(owner, session=writer)
        if local_account is not None:
            if user is None or user.email.lower() != local_account.lower():
                raise CommandRefused(f"no account has the email {local_account}")
            if is_provider_account(user):
                raise CommandRefused("not-local-account: This account signs in through an identity provider; turn its access off with `accounts disable` first")
        elif user is None:
            raise CommandRefused(f"no account has id {owner}")
        elif account_refusal(user, sign_on_only_mode=not config.auth.local.enabled) != ACCOUNT_DISABLED:
            raise CommandRefused(
                f"the access of account {owner} is not turned off; removing bots here is for a person who has left, so turn the account off first with accounts disable, or let the person remove their own bots in the web app"
            )
        removed = await links.remove_all_for_owner(owner, actor_id="host", detail={"by": "operator"}, session=writer)
    note = f"{len(removed)} bot links of account {owner} removed; the bots stay in that person's Telegram account and their tokens were not revoked"
    if local_account is not None:
        note = "the account itself stays usable for whoever holds its password; " + note
    return _success(
        "remove",
        note,
        owner=owner,
        local_account=local_account is not None,
        removed=len(removed),
        bots=[{"link": link.id, "bot_id": link.bot_id, "agent_kind": link.agent_kind, "agent_ref": link.agent_ref, "origin": link.origin} for link in removed],
    )


async def _require_tables(engine, *, schema: str | None):
    tables = {row.__tablename__ for row in vars(model).values() if isinstance(row, type) and row.__module__ == model.__name__ and hasattr(row, "__table__")}
    async with engine.connect() as connection:
        present = await connection.run_sync(lambda sync: set(inspect(sync).get_table_names(schema=schema)))
    if not tables <= present:
        raise CommandRefused("the database has no Telegram tables; this release has not started on it yet")


async def _execute(command, sessions, *, app_config, owner, local_account):
    guard = CopyGuard(sessions)
    if command == "status":
        return await status(sessions, config=app_config, guard=guard, owner=owner)
    if command == "remove":
        return await remove(sessions, config=app_config, owner=owner, local_account=local_account)
    # No fallible database work follows a successful mutating guard unit.
    async with sessions() as session:
        count = await session.scalar(select(func.count()).select_from(TelegramBotLinkRow))
    try:
        result = await (guard.adopt() if command == "adopt" else guard.resume())
    except GuardRefused as error:
        reasons = {
            "host-unreadable": "the Docker daemon did not answer, so this host cannot be named: host-unreadable",
            "polled-elsewhere": "polling is paused because another deployment polls the same bots; stop that deployment's polling, then run resume",
            "host-changed": "polling is paused because the Docker host changed; run adopt",
        }
        raise CommandRefused(reasons[error.reason]) from None
    note = "polling is allowed again; restart the Gateway to start polling"
    if command == "adopt":
        if result["verdict"] == "already_adopted":
            note = f"Docker host {result['host_id']} is already adopted for {count} bot links; restart the Gateway to start polling"
        else:
            note = f"Docker host {result['from']} replaced by {result['host_id']} for {count} bot links; restart the Gateway to start polling"
    return _success(command, note, links=count, **result)


async def run_command(command: str, *, owner: str | None = None, local_account: str | None = None, app_config: AppConfig | None = None) -> dict:
    from deerflow.persistence.engine import close_engine, get_engine, get_session_factory, init_engine_from_config

    if command not in COMMANDS:
        raise CommandRefused("unknown command")
    if command == "remove" and (owner is None) == (local_account is None):
        raise CommandRefused(_REMOVE_FLAGS)
    try:
        if app_config is None:
            app_config = await await_drained(asyncio.to_thread(AppConfig.from_file))
        load_config(app_config)
    except Exception as error:
        raise CommandRefused(f"the configuration /srv/hartmesh/home/config.yaml cannot be loaded: {_failure_text(error)}") from None
    if app_config.database.backend == "memory":
        raise CommandRefused("the database cannot be reached: the memory backend has no persistent database")
    if get_engine() is not None:
        raise CommandRefused("the database cannot be reached: an engine is already initialized in this process")
    document = None
    try:
        try:
            await init_engine_from_config(app_config.database.model_copy(update={"echo_sql": False}), existing_only=True, read_only=command == "status")
            engine, sessions = get_engine(), get_session_factory()
            if engine is None or sessions is None:
                raise RuntimeError("persistence engine unavailable")
            await _require_tables(engine, schema=(app_config.database.postgres_schema or None) if app_config.database.backend == "postgres" else None)
        except CommandRefused:
            raise
        except Exception as error:
            raise CommandRefused(f"the database cannot be reached: {_failure_text(error)}") from None
        document = await _execute(command, sessions, app_config=app_config, owner=owner, local_account=local_account)
        return document
    finally:
        original_error = sys.exc_info()[1]
        try:
            await await_drained(close_engine())
        except Exception as cleanup_error:
            # A close failure cannot undo a committed removal/adoption. Keep the
            # known outcome (or original refusal) and diagnose cleanup separately.
            print(f"telegram command cleanup: {_failure_text(cleanup_error)}", file=sys.stderr)
            if get_engine() is not None:
                try:
                    await await_drained(close_engine())
                except Exception as retry_error:
                    print(f"telegram command cleanup retry: {_failure_text(retry_error)}", file=sys.stderr)
            if document is None and original_error is None:
                raise


class _Parser(argparse.ArgumentParser):
    def error(self, message):
        raise CommandRefused(message)


def _parser():
    parser = _Parser(prog="python -m app.telegram_bots.cli", add_help=False)
    verbs = parser.add_subparsers(dest="command", required=True)
    for command in COMMANDS:
        verb = verbs.add_parser(command, add_help=False)
        if command in ("status", "remove"):
            verb.add_argument("--owner")
        if command == "remove":
            verb.add_argument("--local-account")
    return parser


def _command_word(arguments):
    words = iter(arguments)
    for word in words:
        if word in ("--owner", "--local-account"):
            next(words, None)
        elif not word.startswith("-"):
            return _safe_text(word)
    return None


def main(argv=None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    command = _command_word(arguments)
    try:
        options = vars(_parser().parse_args(arguments))
        command = options.pop("command")
        if command == "remove" and (options["owner"] is None) == (options["local_account"] is None):
            raise CommandRefused(_REMOVE_FLAGS)
        document = asyncio.run(run_command(command, **options))
    except CommandRefused as error:
        document = {"command": command, "returncode": 1, "error": _safe_text(error), "note": "refused; nothing was changed"}
    except Exception as error:
        document = {"command": command, "returncode": 1, "error": _failure_text(error), "note": "failed; run the verb again"}
    print(json.dumps(document, sort_keys=True))
    return document["returncode"]


if __name__ == "__main__":
    raise SystemExit(main())
