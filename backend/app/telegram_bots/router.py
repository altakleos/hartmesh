"""The browser's Telegram routes. Every route acts on the signed-in person's rows.

Routes 2 and 3 read their own bodies: a framework validation error would repeat
what was sent, and what was sent holds a token. No response, refusal or log
carries a token or a connect code other than the caller's own Connect address.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
from datetime import UTC, datetime
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute
from pydantic import BaseModel, ConfigDict
from sqlalchemy import func, select
from telegram.error import InvalidToken, TelegramError

from app.gateway.auth_disabled import AUTH_SOURCE_SESSION
from app.gateway.authz import get_auth_context, resolve_route_permissions_for_request
from app.gateway.deps import get_current_user_from_request, require_admin_user
from app.telegram_bots.config import load_config, section_present
from app.telegram_bots.links import CONNECT_CODES_MAX, AgentLinked, BotLinked, BotLinkRepository, LimitDeployment, LimitPerson, TooManyCodes
from app.telegram_bots.token_pattern import TOKEN_IN_TEXT
from app.telegram_bots.voice import resolve_transcriber
from deerflow.config import get_app_config
from deerflow.persistence.telegram_bots.model import TelegramOutboxRow
from deerflow.utils.file_io import await_drained

logger = logging.getLogger(__name__)
_BODY_MAX_BYTES = 16384
_TOKEN = re.compile(TOKEN_IN_TEXT)
_OPERATION = re.compile(r"[0-9a-f]{32}\Z")
_TIME = re.compile(r"([01][0-9]|2[0-3]):[0-5][0-9]\Z")
_REFUSALS = {
    "session-required": (403, "Telegram bots are managed from a signed-in browser session."),
    "ceiling": (403, "This account is not permitted to do that."),
    "telegram-off": (409, "Telegram is switched off for this deployment."),
    "secret-missing": (409, "HARTMESH_PROVIDER_KEYS_SECRET is not set, so a bot token cannot be stored."),
    "secret-invalid": (409, "HARTMESH_PROVIDER_KEYS_SECRET must be at least 32 characters."),
    "host-changed": (409, "Telegram is paused because this deployment runs on a different host than before."),
    "polled-elsewhere": (409, "Telegram is paused because another process is polling this deployment's bots."),
    "misconfigured": (409, "Telegram did not start on this deployment. The Gateway's log says why."),
    "timezone-required": (400, "A time zone is required with a person's first bot."),
    "invalid-timezone": (400, "The time zone is not an IANA time zone name."),
    "invalid-locale": (400, "The language must be en-US or zh-CN."),
    "invalid-time": (400, "Times are written as HH:MM, and quiet hours need two different times."),
    "invalid-notify": (400, "The choice must be all, questions or none."),
    "token-invalid": (400, "Telegram does not accept this token."),
    "invalid-request": (400, "The request is not valid."),
    "telegram-unreachable": (502, "Telegram could not be reached. Nothing was saved."),
    "webhook-set": (409, "This bot is connected to another service by a webhook."),
    "bot-linked": (409, "This bot is already connected."),
    "agent-linked": (409, "You already have a bot for this chat, custom agent, AI assistant or AI employee."),
    "replacement-gone": (410, "The replacement completed, and that bot was later removed."),
    "operation-mismatch": (409, "That retry does not match the original token replacement."),
    "token-same": (409, "This is the token this bot already has."),
    "limit-person": (409, "You have as many Telegram bots as one person may have here."),
    "limit-deployment": (409, "This deployment has as many Telegram bots as it allows."),
    "agent-not-found": (404, "That custom agent, AI assistant or AI employee is not available to you."),
    "not-found": (404, "That bot is not one of yours."),
    "already-connected": (409, "This bot is already connected to a Telegram account."),
    "not-connected": (409, "This bot is not connected to a Telegram account."),
    "too-many-codes": (429, "Five of your bots are waiting for Start."),
    "no-conflict": (409, "This bot has nothing to acknowledge."),
}


class Refusal(Exception):
    def __init__(self, reason: str, **extra) -> None:
        super().__init__(reason)
        self.reason, self.extra = reason, extra


class _RefusingRoute(APIRoute):
    """Every refusal is ``{detail, reason}`` with only the keys its row names."""

    def get_route_handler(self):
        original = super().get_route_handler()

        async def handler(request: Request) -> Response:
            try:
                return await original(request)
            except RequestValidationError:
                refusal = Refusal("invalid-request")
            except Refusal as raised:
                refusal = raised
            status, detail = _REFUSALS[refusal.reason]
            return JSONResponse({"detail": detail, "reason": refusal.reason, **refusal.extra}, status_code=status)

        return handler


router = APIRouter(prefix="/api/telegram-bots", tags=["telegram-bots"], route_class=_RefusingRoute)


def _supervisor(request: Request):
    return getattr(request.app.state, "telegram_bots", None)


def telegram_feature(app, config):
    """The features key: null without the section, closed defaults on any failure."""
    from app.gateway.routers.features import TelegramFeature

    if not section_present(config):
        return None
    try:
        settings = load_config(config)
        transcriber = resolve_transcriber(settings, models=config.models)
        provider = getattr(transcriber, "provider", None)
        reason = app.state.telegram_bots.reason()
        return TelegramFeature(enabled=settings.enabled, bots="ready" if reason is None else "unavailable", reason=reason, voice="available" if provider else "unavailable", voice_provider=provider)
    except Exception as error:
        logger.warning("telegram_bots feature_unavailable error=%s", type(error).__name__)
        return TelegramFeature()


async def _caller(request: Request) -> str:
    user = await get_current_user_from_request(request)
    if getattr(request.state, "auth_source", None) != AUTH_SOURCE_SESSION:
        raise Refusal("session-required")
    return str(user.id)


async def _require(request: Request, *permissions: str) -> None:
    context = get_auth_context(request)
    if context is not None:
        allowed = all(context.has_permission(*permission.split(":")) for permission in permissions)
    else:
        granted = await resolve_route_permissions_for_request(request, await get_current_user_from_request(request))
        allowed = all(permission in granted for permission in permissions)
    if not allowed:
        raise Refusal("ceiling")


def _config(request: Request):
    supervisor = _supervisor(request)
    return supervisor.config if supervisor is not None else load_config(get_app_config())


def _links(request: Request) -> BotLinkRepository:
    """A deployment that refused to start Telegram still serves a person's rows."""
    supervisor = _supervisor(request)
    if supervisor is not None and supervisor.links is not None:
        return supervisor.links
    from deerflow.persistence.channel_connections.sql import ChannelConnectionRepository
    from deerflow.persistence.engine import get_session_factory

    sessions = (supervisor.session_factory if supervisor is not None else None) or get_session_factory()
    if sessions is None:
        raise Refusal("telegram-off")
    return BotLinkRepository(sessions, ChannelConnectionRepository(sessions), _config(request))


def _live(request: Request):
    """Routes that need Telegram refuse with the deployment's reason."""
    supervisor = _supervisor(request)
    reason = "telegram-off" if supervisor is None else supervisor.reason()
    if reason is not None:
        raise Refusal("telegram-off" if reason == "off" else reason)
    return supervisor


async def _own(request: Request, link_id: str, owner: str):
    link = await _links(request).get(link_id)
    if link is None or link.owner_user_id != owner:
        raise Refusal("not-found")
    return link


async def _agent_name(request: Request, link) -> str | None:
    from deerflow.config.agents_config import load_agent_config

    try:
        if link.agent_kind == "lead_agent":
            supervisor = _supervisor(request)
            return supervisor.product_name if supervisor is not None else None
        target = await await_drained(asyncio.to_thread(load_agent_config, link.agent_ref, user_id=link.owner_user_id))
        return target.display_name or target.name
    except Exception:
        return None


async def _bot(request: Request, link) -> dict:
    links = _links(request)
    now = datetime.now(UTC)
    delivery = (TelegramOutboxRow.link_id == link.id, TelegramOutboxRow.binding_generation == link.binding_generation)
    async with links.session_factory() as session:
        waiting = await session.scalar(select(func.count()).select_from(TelegramOutboxRow).where(*delivery, TelegramOutboxRow.state.in_(("pending", "sending"))))
        degraded = await session.scalar(select(func.count()).select_from(TelegramOutboxRow).where(*delivery, TelegramOutboxRow.state.in_(("pending", "sending")), TelegramOutboxRow.degraded_at.is_not(None)))
        failed = await session.scalar(select(func.count()).select_from(TelegramOutboxRow).where(*delivery, TelegramOutboxRow.state == "failed"))
    return {
        "id": link.id,
        "bot_id": link.bot_id,
        "agent_kind": link.agent_kind,
        "agent_ref": link.agent_ref,
        "agent_name": await _agent_name(request, link),
        "bot_username": link.bot_username,
        "bot_name": link.bot_name,
        "url": f"https://t.me/{link.bot_username}",
        "origin": link.origin,
        "managed_by": link.manager_link_id,
        "can_manage_bots": link.can_manage_bots,
        "bind_mode": link.bind_mode,
        "state": link.state,
        "health": link.health,
        "connected_as": link.connected_name,
        "connect_expires_at": link.connect_expires_at,
        "profile_synced": not link.profile_pending,
        "acts_paused_since": link.acts_paused_since,
        "probing_ended": link.health == "conflict" and link.webhook_host is None and (link.probe_until is None or link.probe_until <= now),
        "limited_since": link.limited_since,
        "delivery_pending": waiting,
        "delivery_degraded": bool(degraded),
        "delivery_failed": failed,
        "webhook_host": link.webhook_host,
        "topics_enabled": link.topics_enabled,
        "policy": None,
        "last_update_at": link.last_update_at,
        "created_at": link.created_at,
    }


def _connect(connect) -> dict:
    return {"url": connect.url, "expires_at": connect.expires_at}


def _time(minute: int) -> str:
    return f"{minute // 60:02d}:{minute % 60:02d}"


def _minute(value) -> int:
    if type(value) is not str or _TIME.match(value) is None:
        raise Refusal("invalid-time")
    return int(value[:2]) * 60 + int(value[3:])


def _settings(row) -> dict | None:
    if row is None:
        return None
    return {
        "timezone": row.timezone,
        "locale": row.locale,
        "quiet_enabled": row.quiet_enabled,
        "quiet_start": _time(row.quiet_start_minute),
        "quiet_end": _time(row.quiet_end_minute),
        "notify": row.notify,
        "summary_enabled": row.summary_enabled,
        "summary_time": _time(row.summary_minute),
    }


async def _zone(value) -> str:
    try:
        if type(value) is not str or not value:
            raise ValueError
        await await_drained(asyncio.to_thread(ZoneInfo, value))
    except Exception:
        raise Refusal("invalid-timezone") from None
    return value


def _locale(value) -> str:
    if value not in ("en-US", "zh-CN"):
        raise Refusal("invalid-locale")
    return value


async def _body(request: Request, *, required: set[str], optional: set[str]) -> dict:
    """Read a body that holds a token, without echoing any of it back."""
    raw = bytearray()
    async for chunk in request.stream():
        raw += chunk
        if len(raw) > _BODY_MAX_BYTES:
            raise Refusal("token-invalid")
    try:
        body = json.loads(raw)
    except (ValueError, RecursionError):
        raise Refusal("token-invalid") from None
    if type(body) is not dict or type(body.get("token")) is not str:
        raise Refusal("token-invalid")
    if not required <= set(body) or set(body) - required - optional:
        raise Refusal("invalid-request")
    takeover = body.get("take_over_webhook", False)
    if type(takeover) is not bool:
        raise Refusal("invalid-request")
    return body


async def _settled(operation) -> None:
    """After commit the change stands: a failure here is the link's health, not a refusal."""
    try:
        await operation
    except Exception as error:
        logger.warning("telegram_bots after_commit_failed error=%s", type(error).__name__)


def _bot_linked(refusal: BotLinked, name: str | None) -> Refusal:
    return Refusal("bot-linked", own=refusal.own, **({"agent_name": name} if refusal.own else {}))


async def _linked_name(request: Request, bot_id: int) -> str | None:
    existing = await _links(request).get_by_bot(bot_id)
    return None if existing is None else await _agent_name(request, existing)


async def _inspect(supervisor, sent: str, *, takeover: bool):
    """The token's shape, then Telegram's answer. Nothing is stored here."""
    match = _TOKEN.search(sent)
    if match is None:
        raise Refusal("token-invalid")
    token = match.group(0)
    try:
        bot_id, username, webhook_host = await supervisor.inspect_token(token)
    except InvalidToken:
        raise Refusal("token-invalid") from None
    except Exception as error:
        logger.warning("telegram_bots token_check_failed error=%s", type(error).__name__)
        raise Refusal("telegram-unreachable") from None
    if webhook_host is not None and not takeover:
        raise Refusal("webhook-set", webhook_host=webhook_host)
    return token, bot_id, username


async def _target(request: Request, owner: str, kind, ref) -> None:
    from deerflow.config.agents_config import load_agent_config

    if type(kind) is not str or type(ref) is not str or not ref or kind not in {"lead_agent", "custom_agent", "instance"}:
        raise Refusal("invalid-request")
    if kind == "lead_agent":
        if ref != "lead_agent":
            raise Refusal("agent-not-found")
        return
    if kind == "instance":
        raise Refusal("agent-not-found")  # AI assistants and AI employees arrive in a later stage.
    try:
        await await_drained(asyncio.to_thread(load_agent_config, ref, user_id=owner))
    except Exception:
        raise Refusal("agent-not-found") from None


@router.get("")
async def list_bots(request: Request, owner: str = Depends(_caller)) -> dict:
    config = _config(request)
    try:
        links = _links(request)
    except Refusal:
        links = None  # No database: nothing is kept, and the answer says so.
    rows = await links.list_for_owner(owner) if links is not None else []
    return {
        "bots": [await _bot(request, row) for row in rows],
        "limits": {"per_person": config.max_bots_per_person, "used": len(rows)},
        "settings": _settings(await links.settings.get(owner)) if links is not None else None,
        "quick_setup": {"available": False, "manager_link_id": None, "manager_username": None},
        "pending_creation": None,
        "removed_bots": [],
        "policies": [],
        "read_by_others": [],
    }


@router.post("/disconnect-all")
async def disconnect_all(request: Request, owner: str = Depends(_caller)) -> dict:
    count = await _links(request).disconnect_all(owner, actor_id=owner)
    supervisor = _supervisor(request)
    if supervisor is not None:
        supervisor.reconcile_soon()
    return {"disconnected": count}


@router.get("/settings")
async def read_settings(request: Request, owner: str = Depends(_caller)) -> dict | None:
    return _settings(await _links(request).settings.get(owner))


class _SettingsChange(BaseModel):
    model_config = ConfigDict(extra="forbid")

    timezone: object = None
    locale: object = None
    quiet_enabled: object = None
    quiet_start: object = None
    quiet_end: object = None
    notify: object = None
    summary_enabled: object = None
    summary_time: object = None


@router.put("/settings")
async def write_settings(request: Request, change: _SettingsChange, owner: str = Depends(_caller)) -> dict | None:
    links = _links(request)
    current = await links.settings.get(owner)
    if current is None:
        raise Refusal("not-found")
    sent, columns = change.model_dump(exclude_unset=True), {}
    if "timezone" in sent:
        columns["timezone"] = await _zone(sent["timezone"])
    if "locale" in sent:
        columns["locale"] = _locale(sent["locale"])
    if "notify" in sent:
        if sent["notify"] not in ("all", "questions", "none"):
            raise Refusal("invalid-notify")
        columns["notify"] = sent["notify"]
    for name, column in (("quiet_start", "quiet_start_minute"), ("quiet_end", "quiet_end_minute"), ("summary_time", "summary_minute")):
        if name in sent:
            columns[column] = _minute(sent[name])
    for name in ("quiet_enabled", "summary_enabled"):
        if name in sent:
            if type(sent[name]) is not bool:
                raise Refusal("invalid-request")
            columns[name] = sent[name]
    if columns.get("quiet_start_minute", current.quiet_start_minute) == columns.get("quiet_end_minute", current.quiet_end_minute):
        raise Refusal("invalid-time")
    changed = columns.get("locale", current.locale) != current.locale
    saved = await links.save_settings(owner, columns, language_changed=changed)
    supervisor = _supervisor(request)
    if changed and supervisor is not None and supervisor.profile_sync is not None:
        supervisor.push_profiles(owner)
    return _settings(saved)


@router.get("/admin/summary")
async def admin_summary(request: Request, _owner: str = Depends(_caller)) -> dict:
    await require_admin_user(request, detail="Telegram counts are for administrators.")
    supervisor = _supervisor(request)
    config = await await_drained(asyncio.to_thread(get_app_config))
    settings = load_config(config)
    feature = telegram_feature(request.app, config)
    if supervisor is not None and supervisor.reason() is None:
        counts = supervisor.status()
    else:
        rows = await _links(request).list_all()
        held = sum(row.state == "held" for row in rows)
        failing = sum(row.state != "held" and row.health != "ok" for row in rows)
        counts = {"bots": len(rows), "people": len({row.owner_user_id for row in rows}), "polling": 0, "failing": failing, "held": held}
    return {
        "enabled": settings.enabled,
        "reason": None if feature is None else feature.reason,
        **{name: counts[name] for name in ("bots", "people", "polling", "failing", "held")},
        "limits": {"per_person": settings.max_bots_per_person, "deployment": settings.max_bots},
        "voice": "unavailable" if feature is None else feature.voice,
        "voice_provider": None if feature is None else feature.voice_provider,
    }


@router.post("", status_code=201)
async def add_bot(request: Request, owner: str = Depends(_caller)) -> dict:
    body = await _body(request, required={"token", "agent_kind", "agent_ref"}, optional={"timezone", "locale", "take_over_webhook"})
    kind, ref, takeover = body["agent_kind"], body["agent_ref"], body.get("take_over_webhook", False)
    if type(kind) is not str or type(ref) is not str:
        raise Refusal("invalid-request")
    await _require(request, "threads:write", "runs:create", *(("agents:read",) if kind == "instance" else ()))
    supervisor = _live(request)
    await _target(request, owner, kind, ref)
    timezone, locale = None, None
    if await supervisor.settings.get(owner) is None:
        if "timezone" not in body:
            raise Refusal("timezone-required")
        timezone = await _zone(body["timezone"])
        locale = _locale(body.get("locale", "en-US"))
    token, bot_id, username = await _inspect(supervisor, body["token"], takeover=takeover)
    encrypted = await supervisor.encrypt_token(bot_id, token)
    links = supervisor.links
    try:
        async with links.capacity_writer(owner, timezone=timezone, locale=locale) as session:
            link = await links.add(owner_user_id=owner, agent_kind=kind, agent_ref=ref, bot_id=bot_id, bot_username=username, encrypted_token=encrypted, actor_id=owner, session=session)
            connect = await links.mint_code(link, session=session)
    except BotLinked as refusal:
        raise _bot_linked(refusal, await _linked_name(request, bot_id)) from None
    except AgentLinked:
        existing = next((row for row in await links.list_for_owner(owner) if (row.agent_kind, row.agent_ref) == (kind, ref)), None)
        raise Refusal("agent-linked", link_id=existing.id if existing is not None else None) from None
    except LimitPerson:
        raise Refusal("limit-person") from None
    except LimitDeployment:
        raise Refusal("limit-deployment") from None
    except TooManyCodes:
        raise Refusal("too-many-codes") from None
    if takeover:
        await _settled(supervisor.clear_webhook(token))
    await _settled(supervisor.add(link, token))
    return {"bot": await _bot(request, await links.get(link.id) or link), "connect": _connect(connect)}


@router.post("/{link_id}/token")
async def replace_token(link_id: str, request: Request, owner: str = Depends(_caller)) -> dict:
    body = await _body(request, required={"token", "operation_id"}, optional={"take_over_webhook"})
    operation, takeover = body["operation_id"], body.get("take_over_webhook", False)
    if type(operation) is not str or _OPERATION.match(operation) is None:
        raise Refusal("invalid-request")
    await _require(request, "threads:write", "runs:create")
    supervisor = _live(request)
    match = _TOKEN.search(body["token"])
    if match is None:
        raise Refusal("token-invalid")
    digest = hashlib.sha256(f"{link_id}:{match.group(0)}".encode()).hexdigest()
    links = supervisor.links
    async with supervisor.token_operation(link_id):
        try:
            receipt = await links.replacement_receipt(owner, operation, digest)
        except ValueError:
            raise Refusal("operation-mismatch") from None
        token = match.group(0)
        if receipt is None:
            current = await _own(request, link_id, owner)
            await _target(request, owner, current.agent_kind, current.agent_ref)
            token, bot_id, username = await _inspect(supervisor, body["token"], takeover=takeover)
            comparison = {}
            if bot_id == current.bot_id:
                same = await supervisor.token_matches(current, token)
                if same is True and current.health not in {"token_rejected", "secret_unreadable"}:
                    raise Refusal("token-same")
                comparison = {"token_changed": same is False, "expected_token_revision": current.token_revision} if same is not None else {}
            encrypted = await supervisor.encrypt_token(bot_id, token)
            try:
                receipt = await links.replace_from_token(
                    owner_user_id=owner,
                    old_link_id=link_id,
                    operation_id=operation,
                    request_digest=digest,
                    validated_bot={"id": bot_id, "username": username, "take_over_webhook": takeover, **comparison},
                    encrypted_token=encrypted,
                )
            except BotLinked as refusal:
                raise _bot_linked(refusal, await _linked_name(request, bot_id)) from None
            except TooManyCodes:
                raise Refusal("too-many-codes") from None
            except ValueError as error:
                if str(error) == "link-not-found":
                    raise Refusal("not-found") from None
                raise
        successor = await links.get(receipt["new_link_id"])
        if successor is None or successor.owner_user_id != owner:
            raise Refusal("replacement-gone", replacement_committed=True, new_link_id=receipt["new_link_id"])
        if successor.token_revision != receipt["token_revision"]:
            return {"bot": await _bot(request, successor), "managed_replaced": receipt["managed_replaced"], "replacement_status": "superseded"}
        if not supervisor.is_running(successor):
            if receipt.get("takeover", {}).get("owed"):
                await _settled(supervisor.clear_webhook(token))
            await _settled(supervisor.adopt_token(successor, token, old_link_id=receipt["old_link_id"]))
        answer = {"bot": await _bot(request, await links.get(successor.id) or successor), "managed_replaced": receipt["managed_replaced"], "replacement_status": "committed"}
        if successor.id != receipt["old_link_id"] and successor.state == "waiting_connect":
            try:
                answer["connect"] = _connect(await links.mint_code(successor))
            except (TooManyCodes, ValueError):
                pass  # The replacement stands; route 5 obtains its address.
        return answer


@router.post("/{link_id}/connect")
async def connect(link_id: str, request: Request, owner: str = Depends(_caller)) -> dict:
    supervisor = _live(request)
    link = await _own(request, link_id, owner)
    links = supervisor.links
    if link.state == "held":
        if await links.codes_waiting(owner, except_link_id=link.id) >= CONNECT_CODES_MAX:
            raise Refusal("too-many-codes")
        link = await links.unbind(link, actor_id=owner, action="release")
        await supervisor.resume_held(link.id)
        link = await links.get(link.id)
    elif link.state != "waiting_connect" or link.bind_mode != "code":
        raise Refusal("already-connected")
    try:
        return _connect(await links.mint_code(link))
    except TooManyCodes:
        raise Refusal("too-many-codes") from None


@router.post("/{link_id}/disconnect")
async def disconnect(link_id: str, request: Request, owner: str = Depends(_caller)) -> dict:
    link = await _own(request, link_id, owner)
    if link.state not in {"connected", "blocked"}:
        raise Refusal("not-connected")
    links = _links(request)
    if await links.codes_waiting(owner, except_link_id=link.id) >= CONNECT_CODES_MAX:
        raise Refusal("too-many-codes")
    link = await links.unbind(link, actor_id=owner)
    fresh = await links.mint_code(link)
    supervisor = _supervisor(request)
    if supervisor is not None:
        supervisor.reconcile_soon()
    return {"bot": await _bot(request, await links.get(link.id) or link), "connect": _connect(fresh)}


@router.post("/{link_id}/acknowledge-conflict")
async def acknowledge_conflict(link_id: str, request: Request, owner: str = Depends(_caller)) -> dict:
    link = await _own(request, link_id, owner)
    if link.acts_paused_since is None:
        raise Refusal("no-conflict")
    links = _links(request)
    saved = await links.update(link.id, acts_paused_since=None)
    await links.event(saved, "conflict_acknowledged", actor_id=owner)
    return {"bot": await _bot(request, saved)}


class _Retry(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    take_over_webhook: bool = False


@router.post("/{link_id}/retry")
async def retry(link_id: str, request: Request, change: _Retry | None = None, owner: str = Depends(_caller)) -> dict:
    supervisor = _live(request)
    link = await _own(request, link_id, owner)
    try:
        saved = await supervisor.retry(link.id, take_over_webhook=change is not None and change.take_over_webhook)
    except TelegramError:
        raise Refusal("telegram-unreachable") from None
    await supervisor.links.event(saved, "retry", actor_id=owner)
    return {"bot": await _bot(request, saved)}


@router.post("/{link_id}/profile")
async def apply_profile(link_id: str, request: Request, owner: str = Depends(_caller)) -> dict:
    supervisor = _live(request)
    link = await _own(request, link_id, owner)
    if not await supervisor.profile_sync.push(link):
        raise Refusal("telegram-unreachable")
    return {"bot": await _bot(request, await supervisor.links.get(link.id) or link)}


@router.delete("/{link_id}", status_code=204)
async def remove_bot(link_id: str, request: Request, owner: str = Depends(_caller)) -> None:
    link = await _own(request, link_id, owner)
    supervisor = _supervisor(request)
    if supervisor is not None and supervisor.links is not None:
        await supervisor.remove(link.id, actor_id=owner)
    else:
        await _links(request).remove(link.id, actor_id=owner)
