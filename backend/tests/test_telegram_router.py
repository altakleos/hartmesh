"""The browser's Telegram routes over real repositories, with Telegram itself scripted."""

import json
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
import pytest_asyncio
from _telegram_repository_test_support import add_link, code_of
from _telegram_repository_test_support import db as db
from fastapi import FastAPI
from sqlalchemy import select
from telegram.error import InvalidToken, NetworkError

from app.gateway.auth_disabled import AUTH_SOURCE_SESSION
from app.telegram_bots import router as module
from app.telegram_bots.links import CONNECT_CODES_MAX
from deerflow.persistence.telegram_bots.model import TelegramBotLinkEventRow, TelegramBotLinkRow, TelegramPersonSettingsRow

OWNER = "owner-a"
TOKEN = ":".join(("123456789", "A" * 35))
SECOND = ":".join(("987654321", "B" * 35))
BOT_FIELDS = {
    "id", "bot_id", "agent_kind", "agent_ref", "agent_name", "bot_username", "bot_name", "url", "origin", "managed_by", "can_manage_bots", "bind_mode", "state", "health", "connected_as", "connect_expires_at", "profile_synced",
    "acts_paused_since", "probing_ended", "limited_since", "delivery_pending", "delivery_degraded", "delivery_failed", "webhook_host", "topics_enabled", "policy", "last_update_at", "created_at",
}  # fmt: skip


@pytest_asyncio.fixture
async def api(db, monkeypatch):
    app = FastAPI()
    app.include_router(module.router)
    state = SimpleNamespace(source=AUTH_SOURCE_SESSION, allowed=True, reason=None, identity=(123456789, "fixture_bot", None), owner=OWNER)

    @app.middleware("http")
    async def session(request, call_next):
        request.state.auth_source = state.source
        return await call_next(request)

    async def inspect(token):
        if isinstance(state.identity, Exception):
            raise state.identity
        return state.identity

    async def encrypt(bot_id, token):
        return f"wrapped-{bot_id}"

    async def current_user(request):
        return SimpleNamespace(id=state.owner)

    async def retry(link_id, *, take_over_webhook=False):
        return await db.links.get(link_id)

    class Operation:
        async def __aenter__(self):
            return None

        async def __aexit__(self, *args):
            return False

    monkeypatch.setattr(module, "get_current_user_from_request", current_user)
    monkeypatch.setattr(module, "get_auth_context", lambda request: SimpleNamespace(has_permission=lambda resource, action: state.allowed))
    order = []
    supervisor = SimpleNamespace(
        order=order,
        session_factory=db.sessions,
        links=db.links,
        settings=db.settings,
        config=db.config,
        product_name="HartMesh",
        reason=lambda: state.reason,
        inspect_token=AsyncMock(side_effect=inspect),
        encrypt_token=AsyncMock(side_effect=encrypt),
        token_matches=AsyncMock(return_value=False),
        token_operation=lambda link_id: Operation(),
        add=AsyncMock(side_effect=lambda *args: order.append("add")),
        clear_webhook=AsyncMock(side_effect=lambda *args: order.append("clear_webhook")),
        is_running=lambda link: False,
        adopt_token=AsyncMock(),
        retry=AsyncMock(side_effect=retry),
        resume_held=AsyncMock(),
        reconcile_soon=lambda: order.append("reconcile"),
        remove=AsyncMock(),
        push_profiles=lambda owner: None,
        profile_sync=SimpleNamespace(push=AsyncMock(return_value=True)),
    )
    app.state.telegram_bots = supervisor
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        yield SimpleNamespace(client=client, state=state, supervisor=supervisor, db=db, app=app)


async def bound(api, link):
    """Press Start with a fresh code; the welcome is the ordinary chat's."""
    code = code_of(await api.db.links.mint_code(link))
    hello = [{"source_ref": {"key": "hello.chat", "values": {"product": "HartMesh", "email": "fixture@example.test"}}}, {"source_ref": {"key": "hello.privacy", "values": {}}, "silent": True}]
    return await api.db.links.bind(
        link, telegram_user_id=7_000_000_001, chat_id=7_000_000_001, first_name="Fixture", username=None, code=code, start_identity={"update_id": 201, "message_id": 101, "chat_id": 7_000_000_001}, hello_parts=hello
    )


def added(**changes):
    return {"agent_kind": "lead_agent", "agent_ref": "lead_agent", "token": TOKEN, "timezone": "America/New_York", "locale": "en-US", **changes}


async def add(api, **changes):
    response = await api.client.post("/api/telegram-bots", json=added(**changes))
    assert response.status_code == 201, response.text
    return response.json()


async def stored(api):
    async with api.db.sessions() as session:
        return list(await session.scalars(select(TelegramBotLinkRow))), list(await session.scalars(select(TelegramPersonSettingsRow)))


async def replace(api, link_id, *, token=SECOND, operation="a" * 32, **changes):
    return await api.client.post(f"/api/telegram-bots/{link_id}/token", json={"token": token, "operation_id": operation, **changes})


@pytest.mark.asyncio
async def test_add_creates_the_link_and_its_connect_address(api):
    body = await add(api)
    assert set(body["bot"]) == BOT_FIELDS and body["bot"]["agent_name"] == "HartMesh" and body["bot"]["url"] == "https://t.me/fixture_bot"
    assert body["connect"]["url"].startswith("https://t.me/fixture_bot?start=")
    api.supervisor.add.assert_awaited_once()
    assert TOKEN not in json.dumps(body)


@pytest.mark.asyncio
@pytest.mark.parametrize("reason", ["off", "secret-missing", "secret-invalid", "host-changed", "polled-elsewhere", "misconfigured"])
async def test_add_refused_for_the_deployment_stores_nothing(api, reason):
    api.state.reason = reason
    response = await api.client.post("/api/telegram-bots", json=added())
    assert response.status_code == 409 and response.json()["reason"] == ("telegram-off" if reason == "off" else reason)
    assert await stored(api) == ([], [])


@pytest.mark.asyncio
async def test_first_bot_refused_without_timezone(api):
    body = added()
    body.pop("timezone")
    missing = await api.client.post("/api/telegram-bots", json=body)
    unknown = await api.client.post("/api/telegram-bots", json=added(timezone="Mars/Olympus"))
    language = await api.client.post("/api/telegram-bots", json=added(locale="fr-FR"))
    assert [(item.status_code, item.json()["reason"]) for item in (missing, unknown, language)] == [(400, "timezone-required"), (400, "invalid-timezone"), (400, "invalid-locale")]
    assert await stored(api) == ([], [])


@pytest.mark.asyncio
async def test_add_refused_when_webhook_set_unless_taken_over(api):
    api.state.identity = (123456789, "fixture_bot", "elsewhere.example")
    refused = await api.client.post("/api/telegram-bots", json=added())
    assert refused.status_code == 409 and refused.json() == {"detail": "This bot is connected to another service by a webhook.", "reason": "webhook-set", "webhook_host": "elsewhere.example"}
    assert (await stored(api))[0] == []
    await add(api, take_over_webhook=True)
    assert api.supervisor.order == ["clear_webhook", "add"], "The webhook goes before anything polls, or the first poll reads as a conflict"


@pytest.mark.asyncio
@pytest.mark.parametrize(("identity", "status", "reason"), [(InvalidToken("Unauthorized"), 400, "token-invalid"), (NetworkError("down"), 502, "telegram-unreachable")])
async def test_a_refused_add_leaves_nothing_stored(api, identity, status, reason):
    api.state.identity = identity
    response = await api.client.post("/api/telegram-bots", json=added())
    assert (response.status_code, response.json()["reason"]) == (status, reason)
    assert await stored(api) == ([], [])


@pytest.mark.asyncio
async def test_a_second_bot_for_one_target_and_a_taken_bot_are_refused(api):
    first = await add(api)
    api.state.identity = (987654321, "other_bot", None)
    response = await api.client.post("/api/telegram-bots", json=added(token=SECOND))
    assert (response.status_code, response.json()["reason"], response.json()["link_id"]) == (409, "agent-linked", first["bot"]["id"])
    api.state.owner = "owner-b"
    api.state.identity = (123456789, "fixture_bot", None)
    taken = await api.client.post("/api/telegram-bots", json=added())
    assert taken.status_code == 409 and taken.json() == {"detail": "This bot is already connected.", "reason": "bot-linked", "own": False}


@pytest.mark.asyncio
async def test_the_refused_person_limit_rolls_the_settings_back(api, monkeypatch):
    monkeypatch.setattr(api.supervisor.config, "max_bots_per_person", 0, raising=False)
    response = await api.client.post("/api/telegram-bots", json=added())
    assert response.status_code == 409 and response.json()["reason"] == "limit-person"
    assert await stored(api) == ([], [])


@pytest.mark.asyncio
async def test_a_custom_agent_must_be_the_callers_and_ai_employees_are_not_offered_yet(api, monkeypatch):
    from deerflow.config import agents_config

    def missing(name, *, user_id):
        raise FileNotFoundError(name)

    monkeypatch.setattr(agents_config, "load_agent_config", missing)
    for kind in ("custom_agent", "instance"):
        response = await api.client.post("/api/telegram-bots", json=added(agent_kind=kind, agent_ref="other"))
        assert response.status_code == 404 and response.json()["reason"] == "agent-not-found"


@pytest.mark.asyncio
async def test_a_missing_ceiling_is_the_routers_own_refusal(api):
    api.state.allowed = False
    response = await api.client.post("/api/telegram-bots", json=added())
    assert response.status_code == 403 and response.json() == {"detail": "This account is not permitted to do that.", "reason": "ceiling"}


@pytest.mark.asyncio
async def test_no_route_or_status_returns_a_token(api):
    for body in ({"token": TOKEN}, {"token": TOKEN, "agent_kind": 7, "agent_ref": "lead_agent"}):
        response = await api.client.post("/api/telegram-bots", json=body)
        assert response.status_code == 400 and response.json()["reason"] == "invalid-request" and TOKEN not in response.text
    for content in (b"[]", b"not json", json.dumps({"token": 5}).encode()):
        response = await api.client.post("/api/telegram-bots", content=content)
        assert response.status_code == 400 and response.json()["reason"] == "token-invalid"
    await add(api)
    assert TOKEN not in (await api.client.get("/api/telegram-bots")).text


@pytest.mark.asyncio
async def test_internal_token_answers_session_required(api):
    api.state.source = "internal"
    for method, path in (("get", ""), ("post", "/disconnect-all"), ("get", "/settings"), ("get", "/admin/summary")):
        response = await getattr(api.client, method)("/api/telegram-bots" + path)
        assert response.status_code == 403 and response.json()["reason"] == "session-required"


@pytest.mark.asyncio
async def test_list_answer_has_every_key_from_stage_1(api):
    await add(api)
    for reason in (None, "off", "misconfigured"):
        api.state.reason = reason
        response = await api.client.get("/api/telegram-bots")
        assert response.status_code == 200
    body = response.json()
    assert set(body) == {"bots", "limits", "settings", "quick_setup", "pending_creation", "removed_bots", "policies", "read_by_others"}
    assert body["removed_bots"] == body["policies"] == body["read_by_others"] == [] and body["pending_creation"] is None
    assert body["quick_setup"] == {"available": False, "manager_link_id": None, "manager_username": None}
    assert body["limits"]["used"] == 1 and body["settings"]["timezone"] == "America/New_York"


@pytest.mark.asyncio
async def test_replace_with_the_same_bot_keeps_the_binding_and_replays_safely(api):
    bot = (await add(api))["bot"]
    await bound(api, await api.db.links.get(bot["id"]))
    api.state.identity = (123456789, "fixture_bot", None)
    first = await replace(api, bot["id"], token=TOKEN)
    assert first.status_code == 200 and first.json()["replacement_status"] == "committed" and "connect" not in first.json()
    saved = await api.db.links.get(bot["id"])
    assert saved.token_revision == 2 and saved.state == "connected"
    assert (await replace(api, bot["id"], token=TOKEN)).json()["replacement_status"] == "committed"
    assert (await api.db.links.get(bot["id"])).token_revision == 2, "A replay never swaps twice"


@pytest.mark.asyncio
async def test_same_token_answers_token_same_and_changes_nothing(api):
    bot = (await add(api))["bot"]
    api.supervisor.token_matches.return_value = True
    response = await replace(api, bot["id"], token=TOKEN)
    assert response.status_code == 409 and response.json()["reason"] == "token-same"
    assert (await api.db.links.get(bot["id"])).token_revision == 1


@pytest.mark.asyncio
async def test_replace_bot_requires_new_connect(api):
    bot = (await add(api))["bot"]
    api.state.identity = (987654321, "other_bot", None)
    response = await replace(api, bot["id"])
    assert response.status_code == 200 and response.json()["bot"]["bot_id"] == 987654321 and response.json()["connect"]["url"].startswith("https://t.me/other_bot?start=")
    assert await api.db.links.get(bot["id"]) is None
    api.supervisor.adopt_token.assert_awaited_once()


@pytest.mark.asyncio
async def test_replaying_a_removed_successor_answers_replacement_gone(api):
    bot = (await add(api))["bot"]
    api.state.identity = (987654321, "other_bot", None)
    successor = (await replace(api, bot["id"])).json()["bot"]["id"]
    await api.db.links.remove(successor, actor_id=OWNER)
    response = await replace(api, bot["id"])
    assert response.status_code == 410 and response.json() == {"detail": "The replacement completed, and that bot was later removed.", "reason": "replacement-gone", "replacement_committed": True, "new_link_id": successor}


@pytest.mark.asyncio
async def test_a_reused_operation_with_another_token_is_refused(api):
    bot = (await add(api))["bot"]
    api.state.identity = (123456789, "fixture_bot", None)
    assert (await replace(api, bot["id"], token=TOKEN)).status_code == 200
    response = await replace(api, bot["id"], token=SECOND)
    assert response.status_code == 409 and response.json()["reason"] == "operation-mismatch"


@pytest.mark.asyncio
async def test_a_superseded_receipt_returns_the_current_bot_without_connect(api):
    bot = (await add(api))["bot"]
    api.state.identity = (123456789, "fixture_bot", None)
    await replace(api, bot["id"], token=TOKEN)
    await replace(api, bot["id"], token=TOKEN, operation="b" * 32)
    answer = (await replace(api, bot["id"], token=TOKEN)).json()
    assert answer["replacement_status"] == "superseded" and "connect" not in answer


@pytest.mark.asyncio
async def test_a_person_cannot_touch_another_persons_bot(api):
    bot = (await add(api))["bot"]
    api.state.owner = "owner-b"
    for method, path in (("post", "/connect"), ("post", "/disconnect"), ("post", "/acknowledge-conflict"), ("post", "/retry"), ("post", "/profile"), ("delete", "")):
        response = await getattr(api.client, method)(f"/api/telegram-bots/{bot['id']}{path}")
        assert response.status_code == 404 and response.json() == {"detail": "That bot is not one of yours.", "reason": "not-found"}


@pytest.mark.asyncio
async def test_connect_serves_a_waiting_and_a_held_link_and_refuses_a_connected_one(api):
    bot = (await add(api))["bot"]
    assert (await api.client.post(f"/api/telegram-bots/{bot['id']}/connect")).status_code == 200
    link = await bound(api, await api.db.links.get(bot["id"]))
    connected = await api.client.post(f"/api/telegram-bots/{bot['id']}/connect")
    assert connected.status_code == 409 and connected.json()["reason"] == "already-connected"
    await api.db.links.hold_if_current(link.id, token_revision=link.token_revision, binding_generation=link.binding_generation)
    held = await api.client.post(f"/api/telegram-bots/{bot['id']}/connect")
    assert held.status_code == 200 and (await api.db.links.get(bot["id"])).state == "waiting_connect"
    api.supervisor.resume_held.assert_awaited_once()


@pytest.mark.asyncio
async def test_disconnect_keeps_bot_and_returns_new_link(api):
    bot = (await add(api))["bot"]
    refused = await api.client.post(f"/api/telegram-bots/{bot['id']}/disconnect")
    assert refused.status_code == 409 and refused.json()["reason"] == "not-connected"
    await bound(api, await api.db.links.get(bot["id"]))
    response = await api.client.post(f"/api/telegram-bots/{bot['id']}/disconnect")
    assert response.status_code == 200 and response.json()["bot"]["state"] == "waiting_connect" and "connect" in response.json()


@pytest.mark.asyncio
async def test_disconnect_all_is_served_with_telegram_off(api):
    bot = (await add(api))["bot"]
    await bound(api, await api.db.links.get(bot["id"]))
    api.state.reason = "off"
    response = await api.client.post("/api/telegram-bots/disconnect-all")
    assert response.status_code == 200 and response.json() == {"disconnected": 1}


@pytest.mark.asyncio
async def test_acknowledge_conflict_clears_the_pause(api):
    bot = (await add(api))["bot"]
    nothing = await api.client.post(f"/api/telegram-bots/{bot['id']}/acknowledge-conflict")
    assert nothing.status_code == 409 and nothing.json()["reason"] == "no-conflict"
    await api.db.links.update(bot["id"], acts_paused_since=datetime.now(UTC))
    assert (await api.client.post(f"/api/telegram-bots/{bot['id']}/acknowledge-conflict")).json()["bot"]["acts_paused_since"] is None
    async with api.db.sessions() as session:
        assert "conflict_acknowledged" in list(await session.scalars(select(TelegramBotLinkEventRow.action)))


@pytest.mark.asyncio
async def test_retry_passes_the_explicit_webhook_choice_and_writes_its_event(api):
    bot = (await add(api))["bot"]
    assert (await api.client.post(f"/api/telegram-bots/{bot['id']}/retry", json={"take_over_webhook": True})).status_code == 200
    assert api.supervisor.retry.await_args.kwargs == {"take_over_webhook": True}
    async with api.db.sessions() as session:
        assert "retry" in list(await session.scalars(select(TelegramBotLinkEventRow.action)))


@pytest.mark.asyncio
async def test_apply_profile_answers_when_telegram_cannot_be_reached(api):
    bot = (await add(api))["bot"]
    assert (await api.client.post(f"/api/telegram-bots/{bot['id']}/profile")).status_code == 200
    api.supervisor.profile_sync.push.return_value = False
    refused = await api.client.post(f"/api/telegram-bots/{bot['id']}/profile")
    assert refused.status_code == 502 and refused.json()["reason"] == "telegram-unreachable"


@pytest.mark.asyncio
async def test_remove_is_served_with_telegram_off(api):
    bot = (await add(api))["bot"]
    api.state.reason = "off"
    assert (await api.client.delete(f"/api/telegram-bots/{bot['id']}")).status_code == 204
    api.supervisor.remove.assert_awaited_once()


@pytest.mark.asyncio
async def test_too_many_codes_refuses_before_a_disconnect_changes_anything(api, monkeypatch):
    bot = (await add(api))["bot"]
    await bound(api, await api.db.links.get(bot["id"]))
    monkeypatch.setattr(api.db.links, "codes_waiting", AsyncMock(return_value=CONNECT_CODES_MAX))
    response = await api.client.post(f"/api/telegram-bots/{bot['id']}/disconnect")
    assert response.status_code == 429 and response.json()["reason"] == "too-many-codes"
    assert (await api.db.links.get(bot["id"])).state == "connected"


@pytest.mark.asyncio
async def test_settings_are_null_before_the_first_bot(api):
    assert (await api.client.get("/api/telegram-bots/settings")).json() is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("change", "reason"),
    [
        ({"quiet_start": "25:00"}, "invalid-time"),
        ({"quiet_start": "21:00", "quiet_end": "21:00"}, "invalid-time"),
        ({"notify": "loud"}, "invalid-notify"),
        ({"locale": "fr-FR"}, "invalid-locale"),
        ({"timezone": "Mars/Olympus"}, "invalid-timezone"),
    ],
)
async def test_quiet_hours_with_equal_ends_are_refused(api, change, reason):
    await add(api)
    response = await api.client.put("/api/telegram-bots/settings", json=change)
    assert response.status_code == 400 and response.json()["reason"] == reason


@pytest.mark.asyncio
async def test_a_language_change_marks_the_persons_bots_owed(api):
    bot = (await add(api))["bot"]
    await api.db.links.finish_profile(bot["id"], token_revision=1, version=1, bot_name="HartMesh", locale="en-US", at=datetime.now(UTC))
    assert (await api.db.links.get(bot["id"])).profile_pending is False
    pushed = []
    api.supervisor.push_profiles = pushed.append
    response = await api.client.put("/api/telegram-bots/settings", json={"locale": "zh-CN", "quiet_start": "22:15"})
    assert response.status_code == 200 and response.json()["locale"] == "zh-CN" and response.json()["quiet_start"] == "22:15"
    assert (await api.db.links.get(bot["id"])).profile_pending is True and pushed == [OWNER]


@pytest.mark.asyncio
async def test_admin_summary_never_names_a_bot(api, monkeypatch):
    await add(api)
    monkeypatch.setattr(module, "require_admin_user", AsyncMock())
    monkeypatch.setattr(module, "get_app_config", lambda: SimpleNamespace(models=[], model_extra={}))
    monkeypatch.setattr(module, "load_config", lambda config: api.supervisor.config)
    api.supervisor.status = lambda: {"bots": 1, "people": 1, "polling": 1, "failing": 0, "held": 0}
    response = await api.client.get("/api/telegram-bots/admin/summary")
    assert response.status_code == 200 and response.json()["bots"] == 1 and "fixture_bot" not in response.text
    assert set(response.json()) == {"enabled", "reason", "bots", "people", "polling", "failing", "held", "limits", "voice", "voice_provider"}


@pytest.mark.asyncio
async def test_inspect_token_reads_telegram_through_a_disposable_client(db, monkeypatch):
    from test_telegram_supervisor import build

    e = build(db, monkeypatch)
    e.updates.enqueue_get_me(bot_id=123456789, username="fixture_bot")
    e.updates.enqueue("getWebhookInfo", result={"url": "https://elsewhere.example/hook", "has_custom_certificate": False, "pending_update_count": 0})
    assert await e.supervisor.inspect_token(TOKEN) == (123456789, "fixture_bot", "elsewhere.example")
    assert e.updates.closed


@pytest.mark.asyncio
async def test_a_failure_after_commit_never_reads_as_a_failed_add(api):
    api.supervisor.add.side_effect = RuntimeError("start failed")
    body = await add(api)
    assert body["connect"]["url"].startswith("https://t.me/fixture_bot?start=") and len((await stored(api))[0]) == 1


@pytest.mark.asyncio
async def test_a_failure_after_commit_never_reads_as_a_failed_swap(api):
    bot = (await add(api))["bot"]
    api.state.identity = (987654321, "other_bot", None)
    api.supervisor.adopt_token.side_effect = RuntimeError("start failed")
    response = await replace(api, bot["id"])
    assert response.status_code == 200 and response.json()["replacement_status"] == "committed" and "connect" in response.json()


@pytest.mark.asyncio
async def test_a_replay_leaves_a_running_client_alone(api):
    bot = (await add(api))["bot"]
    api.state.identity = (123456789, "fixture_bot", None)
    await replace(api, bot["id"], token=TOKEN, take_over_webhook=True)
    assert api.supervisor.order.count("clear_webhook") == 1 and api.supervisor.adopt_token.await_count == 1
    api.supervisor.is_running = lambda link: True
    await replace(api, bot["id"], token=TOKEN, take_over_webhook=True)
    assert api.supervisor.order.count("clear_webhook") == 1 and api.supervisor.adopt_token.await_count == 1


@pytest.mark.asyncio
async def test_a_held_link_keeps_its_binding_when_too_many_codes_wait(api, monkeypatch):
    bot = (await add(api))["bot"]
    link = await bound(api, await api.db.links.get(bot["id"]))
    await api.db.links.hold_if_current(link.id, token_revision=link.token_revision, binding_generation=link.binding_generation)
    monkeypatch.setattr(api.db.links, "codes_waiting", AsyncMock(return_value=CONNECT_CODES_MAX))
    response = await api.client.post(f"/api/telegram-bots/{bot['id']}/connect")
    assert response.status_code == 429 and response.json()["reason"] == "too-many-codes"
    assert (await api.db.links.get(bot["id"])).state == "held"


@pytest.mark.asyncio
async def test_a_bot_the_person_already_has_is_named(api, monkeypatch):
    from deerflow.config import agents_config

    monkeypatch.setattr(agents_config, "load_agent_config", lambda name, *, user_id: SimpleNamespace(name=name, display_name="Reader"))
    await add(api)
    response = await api.client.post("/api/telegram-bots", json=added(agent_kind="custom_agent", agent_ref="reader"))
    assert response.status_code == 409 and response.json() == {"detail": "This bot is already connected.", "reason": "bot-linked", "own": True, "agent_name": "HartMesh"}


@pytest.mark.asyncio
async def test_a_wrong_field_is_the_routers_own_refusal(api):
    await add(api)
    response = await api.client.put("/api/telegram-bots/settings", json={"unknown": 1})
    assert response.status_code == 400 and response.json() == {"detail": "The request is not valid.", "reason": "invalid-request"}


@pytest.mark.asyncio
async def test_a_wrong_retry_choice_is_the_routers_own_refusal(api):
    bot = (await add(api))["bot"]
    response = await api.client.post(f"/api/telegram-bots/{bot['id']}/retry", json={"take_over_webhook": "yes"})
    assert response.status_code == 400 and response.json()["reason"] == "invalid-request"


@pytest.mark.asyncio
async def test_a_telegram_failure_on_retry_is_unreachable_not_a_crash(api):
    bot = (await add(api))["bot"]
    api.supervisor.retry.side_effect = NetworkError("down")
    response = await api.client.post(f"/api/telegram-bots/{bot['id']}/retry")
    assert response.status_code == 502 and response.json()["reason"] == "telegram-unreachable"


@pytest_asyncio.fixture
async def refused(db, monkeypatch):
    """A real supervisor that refused to start, with the sessions production gives it."""
    from test_telegram_supervisor import build

    from app.telegram_bots import supervisor as supervisor_module
    from deerflow.persistence import engine

    e = build(db, monkeypatch)
    monkeypatch.setattr(supervisor_module, "static_cause", lambda *args: "off")
    monkeypatch.setattr(engine, "get_session_factory", lambda: db.sessions)
    e.supervisor.session_factory = e.supervisor.links = e.supervisor.settings = e.supervisor.outbox = None
    await e.supervisor.check()
    assert e.supervisor.reason() == "off" and e.supervisor.links is None and e.supervisor.profile_sync is None
    app = FastAPI()
    app.include_router(module.router)

    @app.middleware("http")
    async def session(request, call_next):
        request.state.auth_source = AUTH_SOURCE_SESSION
        return await call_next(request)

    async def current_user(request):
        return SimpleNamespace(id=OWNER)

    monkeypatch.setattr(module, "get_current_user_from_request", current_user)
    app.state.telegram_bots = e.supervisor
    link = await add_link(db, owner=OWNER)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        yield SimpleNamespace(client=client, link=link, db=db)


@pytest.mark.asyncio
async def test_a_deployment_with_telegram_off_still_serves_a_persons_rows(refused):
    root = "/api/telegram-bots"
    listed = await refused.client.get(root)
    assert listed.status_code == 200 and [bot["id"] for bot in listed.json()["bots"]] == [refused.link.id]
    assert (await refused.client.get(root + "/settings")).status_code == 200
    assert (await refused.client.put(root + "/settings", json={"quiet_start": "22:00"})).status_code == 200
    assert (await refused.client.post(root + "/disconnect-all")).json() == {"disconnected": 0}, "This link had nothing waiting to end"
    assert (await refused.client.post(f"{root}/{refused.link.id}/disconnect")).json()["reason"] == "not-connected"
    assert (await refused.client.post(f"{root}/{refused.link.id}/acknowledge-conflict")).json()["reason"] == "no-conflict"
    assert (await refused.client.delete(f"{root}/{refused.link.id}")).status_code == 204
    assert await refused.db.links.get(refused.link.id) is None


@pytest.mark.asyncio
async def test_the_routes_that_need_telegram_say_why_they_cannot_run(refused):
    for path in ("connect", "retry", "profile"):
        response = await refused.client.post(f"/api/telegram-bots/{refused.link.id}/{path}")
        assert response.status_code == 409 and response.json()["reason"] == "telegram-off"


@pytest.mark.asyncio
async def test_inspect_token_builds_its_own_client_without_an_injected_factory(db, monkeypatch):
    from test_telegram_supervisor import build

    from app.telegram_bots import supervisor as supervisor_module

    e = build(db, monkeypatch)
    del e.supervisor.request_factory
    made = []

    def factory(**kwargs):
        made.append(kwargs)
        return e.updates

    monkeypatch.setattr(supervisor_module, "ManagedHTTPXRequest", factory)
    e.updates.enqueue_get_me(bot_id=123456789, username="fixture_bot")
    e.updates.enqueue("getWebhookInfo", result={"url": "", "has_custom_certificate": False, "pending_update_count": 0})
    assert await e.supervisor.inspect_token(TOKEN) == (123456789, "fixture_bot", None)
    assert made[0]["connection_pool_size"] == 1 and e.supervisor.tls_context is not None
