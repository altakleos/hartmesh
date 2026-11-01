"""A bot's public profile through the actual SDK client, and its drawn picture."""

import asyncio
import hashlib
import io
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from _telegram_repository_test_support import db as db
from PIL import Image
from sqlalchemy import update
from test_telegram_bot_adapter import fail
from test_telegram_bot_handlers import ingress
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging

from app.telegram_bots import avatar
from app.telegram_bots.avatar import AVATAR_PIXELS, CHAT_COLOR, color_of, initials_of, render_avatar
from app.telegram_bots.profile import PROFILE_VERSION, ProfileSync, display_name_of, suggested_username
from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow

ROWS = [
    ("Maya", "0123456789abcdef0123456789abcdef", "#1D4ED8", "M"),
    ("Reporting analyst", "ffffffffffffffffffffffffffffffff", "#15803D", "RA"),
    ("Élodie Müller-Schmidt", "00000000000000000000000000000000", "#1D4ED8", "ÉM"),
    ("财务助理", "9f86d081884c7d659a2feaa0c55ad015", "#A21CAF", "财"),
    ("3D helper", "a3f1c2d4e5b60718293a4b5c6d7e8f90", "#1D4ED8", "3H"),
    ("research-helper", "research-helper", "#0F766E", "RH"),
    ("QA2", "QA2", "#C2410C", "Q"),
]
CALLS = ("setMyShortDescription", "setMyDescription", "setMyCommands", "setChatMenuButton", "setMyProfilePhoto", "setMyName")


async def environment(db, monkeypatch, *, lead):
    e = await ingress(db, monkeypatch=monkeypatch, lead=lead)
    e.services.channels = lambda: (e.channel,)
    e.sync = e.services.profile_sync = ProfileSync(e.services)
    return e


def script(e):
    e.request.enqueue_get_me(bot_id=e.link.bot_id, can_manage_bots=True, has_topics_enabled=True, allows_users_to_create_topics=False)
    for endpoint in CALLS:
        e.request.enqueue(endpoint, result=True)


def sent(e, endpoint):
    return next(call for call in e.request.calls if call.endpoint == endpoint).parameters


def count(e, endpoint):
    return sum(call.endpoint == endpoint for call in e.request.calls)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("lead", "locale", "name", "short", "description"),
    [(True, "en-US", "HartMesh", "HartMesh. Private:", "This is a private HartMesh bot."), (False, "en-US", "Fixture target", "Custom agent. Private:", "This is a private custom agent."), (True, "zh-CN", "HartMesh", None, None)],
)
async def test_profile_texts_per_kind_and_locale(db, monkeypatch, lead, locale, name, short, description):
    from app.telegram_bots.render import render_plain

    e = await environment(db, monkeypatch, lead=lead)
    await db.settings.update(e.link.owner_user_id, locale=locale)
    script(e)
    try:
        assert await e.sync.push(e.link)
        kind = "chat" if lead else "custom_agent"
        assert sent(e, "setMyName")["name"] == name
        assert sent(e, "setMyShortDescription")["short_description"] == render_plain(f"profile.short.{kind}", locale, product="HartMesh")
        assert sent(e, "setMyDescription")["description"] == render_plain(f"profile.description.{kind}", locale, product="HartMesh")
        if short is not None:
            assert sent(e, "setMyShortDescription")["short_description"].startswith(short) and sent(e, "setMyDescription")["description"].startswith(description)
        saved = await db.links.get(e.link.id)
        assert (saved.bot_name, saved.profile_pending, saved.profile_version, saved.can_manage_bots) == (name, False, PROFILE_VERSION, True) and saved.profile_synced_at is not None
    finally:
        await e.channel.stop()


def test_a_name_too_long_for_telegram_is_cut_at_a_word():
    assert display_name_of("x" * 64) == "x" * 64
    assert display_name_of("Quarterly financial reporting assistant for the regional sales and operations teams") == "Quarterly financial reporting assistant for the regional…"
    assert display_name_of("a" * 70) == "a" * 62 + "…"
    assert display_name_of("😀" * 70) == "😀" * 62 + "…"


@pytest.mark.asyncio
@pytest.mark.parametrize("lead", [True, False])
async def test_profile_names_no_role_person_or_company(db, monkeypatch, lead):
    e = await environment(db, monkeypatch, lead=lead)

    async def values(link):
        return {"product": "HartMesh", "name": "Fixture target", "agent": "Fixture target", "memory_url": None, "description": "A private company description", "role": "Accountant", "email": "owner@example.test"}

    e.services.prepare_ingress_values = values
    script(e)
    try:
        assert await e.sync.push(e.link)
        pushed = [sent(e, "setMyName")["name"], sent(e, "setMyShortDescription")["short_description"], sent(e, "setMyDescription")["description"], str(sent(e, "setMyCommands")["commands"])]
        assert not re.search("company|Accountant|owner@", " ".join(pushed))
        assert ("HartMesh" in " ".join(pushed[:3])) is lead, "Only the ordinary chat's bot carries the product's name"
        assert "HartMesh" not in pushed[3]
    finally:
        await e.channel.stop()


@pytest.mark.parametrize(("name", "slug"), [("Reporting analyst", "reportinganalyst"), ("3D helper", "ai_3dhelper"), ("A" * 40, "a" * 20)])
def test_username_suggestion_rules(name, slug):
    username = suggested_username(name)
    assert re.fullmatch(re.escape(slug) + r"_[a-hj-km-np-z2-9]{4}_bot", username)
    assert 10 <= len(username) <= 32 and username.endswith("_bot")


def test_a_name_with_no_slug_and_the_ordinary_chat_have_their_own_forms():
    assert re.fullmatch(r"ai_[a-hj-km-np-z2-9]{8}_bot", suggested_username("财务"))
    assert re.fullmatch(r"chat_[a-hj-km-np-z2-9]{6}_bot", suggested_username("", chat=True))


def test_avatar_is_a_640_pixel_square_jpeg():
    picture = Image.open(io.BytesIO(render_avatar("RA", "#15803D", "custom_agent")))
    assert picture.format == "JPEG" and picture.size == (AVATAR_PIXELS, AVATAR_PIXELS) == (640, 640)


@pytest.mark.parametrize(("name", "key", "color", "initials"), ROWS)
def test_picture_matches_the_seven_rows(name, key, color, initials):
    assert (initials_of(name), color_of(key)) == (initials, color)


def _outside_the_circle(picture_bytes, background):
    picture = Image.open(io.BytesIO(picture_bytes)).convert("RGB")
    expected = tuple(int(background[index : index + 2], 16) for index in (1, 3, 5))
    return [(x, y) for x in range(0, AVATAR_PIXELS, 4) for y in range(0, AVATAR_PIXELS, 4) if (x - 320) ** 2 + (y - 320) ** 2 > 320**2 and max(abs(a - b) for a, b in zip(picture.getpixel((x, y)), expected)) > 40]


@pytest.mark.parametrize(("initials", "kind"), [("WW", "custom_agent"), ("M", "custom_agent"), ("财务", "custom_agent"), ("WM", "instance"), ("财", "instance"), ("", "instance"), ("", "lead_agent")])
def test_picture_content_lies_inside_the_circle(initials, kind):
    assert _outside_the_circle(render_avatar(initials, CHAT_COLOR, kind), CHAT_COLOR) == []


def test_avatar_fonts_are_the_pinned_files():
    fonts = Path(avatar.__file__).parent / "fonts"
    for name, size, digest in (("DejaVuSans-Bold.ttf", 708920, "0d977336a6d5fba34eab8e3199eb218327161b5143749f802982c2bc34df0c96"), ("DroidSansFallbackFull.ttf", 4033420, "acb6440a713d880a13a21b468ba7cd43f5a2b2934972e51be791c880730777b8")):
        data = (fonts / name).read_bytes()
        assert len(data) == size and hashlib.sha256(data).hexdigest() == digest
        assert (fonts / name).with_suffix(".LICENSE").read_text(encoding="utf-8").strip()


def _white_pixels(picture_bytes):
    picture = Image.open(io.BytesIO(picture_bytes)).convert("L")
    return sum(value > 200 for value in picture.getdata())


@pytest.mark.parametrize("initials", ["ÉM", "财"])
def test_initials_drawn_with_accents_and_in_chinese(initials):
    assert render_avatar(initials, "#1D4ED8", "custom_agent") != render_avatar("", "#1D4ED8", "custom_agent")
    assert _white_pixels(render_avatar(initials, "#1D4ED8", "custom_agent")) > 2000


def test_initials_left_out_when_no_shipped_font_has_the_character():
    assert render_avatar("\U000131d1", "#1D4ED8", "custom_agent") == render_avatar("", "#1D4ED8", "custom_agent"), "A custom agent's picture falls back to the chat mark"
    assert render_avatar("\U000131d1", "#1D4ED8", "instance") == render_avatar("", "#1D4ED8", "instance"), "An agent's picture carries the pill alone"


def test_initials_left_out_for_right_to_left_letters():
    assert render_avatar("م", "#1D4ED8", "custom_agent") == render_avatar("", "#1D4ED8", "custom_agent")
    assert render_avatar("א", "#1D4ED8", "instance") == render_avatar("", "#1D4ED8", "instance")


@pytest.mark.asyncio
async def test_custom_agent_menu_is_new_stop_help(db, monkeypatch):
    e = await environment(db, monkeypatch, lead=False)
    script(e)
    try:
        assert await e.sync.push(e.link)
        assert [command["command"] for command in sent(e, "setMyCommands")["commands"]] == ["new", "stop", "help"]
    finally:
        await e.channel.stop()


@pytest.mark.asyncio
async def test_chat_menu_in_stage_1_is_new_stop_settings_help(db, monkeypatch):
    e = await environment(db, monkeypatch, lead=True)
    script(e)
    try:
        assert await e.sync.push(e.link)
        menu = sent(e, "setMyCommands")["commands"]
        assert [command["command"] for command in menu] == ["new", "stop", "settings", "help"]
        assert menu[2]["description"] == "Language"
    finally:
        await e.channel.stop()


@pytest.mark.asyncio
async def test_set_my_commands_passes_no_language_code(db, monkeypatch):
    e = await environment(db, monkeypatch, lead=True)
    script(e)
    try:
        assert await e.sync.push(e.link)
        parameters = sent(e, "setMyCommands")
        assert parameters["scope"]["type"] == "all_private_chats" and "language_code" not in parameters
        assert sent(e, "setChatMenuButton")["menu_button"]["type"] == "commands" and "chat_id" not in sent(e, "setChatMenuButton")
    finally:
        await e.channel.stop()


@pytest.mark.asyncio
async def test_profile_retried_for_24_hours(db, monkeypatch):
    e = await environment(db, monkeypatch, lead=True)
    e.request.enqueue_get_me(bot_id=e.link.bot_id)
    fail(e.request, "setMyShortDescription", code=400, description="Bad Request: invalid")
    try:
        await e.sync.push_owed()
        assert (await db.links.get(e.link.id)).profile_pending is True and e.link.id in e.sync._failing_since
        first = e.sync._failing_since[e.link.id]
        script(e)
        e.sync._failing_since[e.link.id] = datetime.now(UTC) - timedelta(hours=24, seconds=1)
        calls = len(e.request.calls)
        await e.sync.push_owed()
        assert len(e.request.calls) == calls and (await db.links.get(e.link.id)).profile_pending is True, "Left pending until the next start"
        e.sync._failing_since[e.link.id] = first
        await e.sync.push_owed()
        assert (await db.links.get(e.link.id)).profile_pending is False and e.link.id not in e.sync._failing_since, "Inside the window it is tried again"
    finally:
        await e.channel.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize(("version", "pushed"), [(PROFILE_VERSION - 1, True), (PROFILE_VERSION, False)])
async def test_profile_pushed_again_when_profile_version_is_lower(db, monkeypatch, version, pushed):
    e = await environment(db, monkeypatch, lead=True)
    async with db.sessions.begin() as session:
        await session.execute(update(TelegramBotLinkRow).values(profile_pending=False, profile_version=version))
    if pushed:
        script(e)
    try:
        await e.sync.push_owed()
        assert count(e, "setMyName") == int(pushed) and (await db.links.get(e.link.id)).profile_version == PROFILE_VERSION
    finally:
        await e.channel.stop()


@pytest.mark.asyncio
async def test_a_client_replaced_during_a_push_leaves_the_push_owed(db, monkeypatch):
    e = await environment(db, monkeypatch, lead=True)
    e.request.enqueue_get_me(bot_id=e.link.bot_id)
    e.request.enqueue("setMyShortDescription", result=True)
    channel = e.services.channel
    calls = []

    async def retire(*args, **kwargs):
        calls.append(True)
        e.services.channel = lambda link_id: None
        return await actual(*args, **kwargs)

    actual = e.channel.bot.set_my_short_description
    monkeypatch.setattr(type(e.channel.bot), "set_my_short_description", lambda self, *args, **kwargs: retire(*args, **kwargs))
    try:
        assert await e.sync.push(e.link) is False
        assert calls and (await db.links.get(e.link.id)).profile_pending is True and e.link.id not in e.sync._failing_since, "A replacement is no failure of this bot"
        assert count(e, "setMyDescription") == 0
    finally:
        e.services.channel = channel
        await e.channel.stop()


@pytest.mark.asyncio
async def test_a_language_chosen_during_a_push_is_not_lost(db, monkeypatch):
    e = await environment(db, monkeypatch, lead=False)
    held = asyncio.Event()
    e.request.enqueue_get_me(bot_id=e.link.bot_id)
    for endpoint in CALLS[:-1]:
        e.request.enqueue(endpoint, result=True)
    last = e.request.enqueue("setMyName", result=True, release=held)
    push = asyncio.create_task(e.sync.push(e.link))
    try:
        await asyncio.wait_for(last.started.wait(), 10)
        await db.settings.update(e.link.owner_user_id, locale="zh-CN")
        held.set()
        assert await push is False
        assert (await db.links.get(e.link.id)).profile_pending is True, "The English push cannot settle a Chinese person's profile"
        script(e)
        await e.sync.push_owed()
        saved = await db.links.get(e.link.id)
        assert saved.profile_pending is False and count(e, "setMyName") == 2
    finally:
        held.set()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_overlapping_passes_push_one_link_once(db, monkeypatch):
    e = await environment(db, monkeypatch, lead=False)
    held = asyncio.Event()
    e.request.enqueue_get_me(bot_id=e.link.bot_id)
    for endpoint in CALLS[:-1]:
        e.request.enqueue(endpoint, result=True)
    last = e.request.enqueue("setMyName", result=True, release=held)
    first = asyncio.create_task(e.sync.push_owed())
    try:
        await asyncio.wait_for(last.started.wait(), 10)
        second = asyncio.create_task(e.sync.push_owed())
        await asyncio.sleep(0.05)
        held.set()
        await asyncio.gather(first, second)
        assert count(e, "setMyName") == 1, "The waiter rereads the link and finds nothing owed"
    finally:
        held.set()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_a_refused_name_does_not_strand_the_rest_of_the_profile(db, monkeypatch):
    e = await environment(db, monkeypatch, lead=False)
    e.request.enqueue_get_me(bot_id=e.link.bot_id)
    for endpoint in CALLS[:-1]:
        e.request.enqueue(endpoint, result=True)
    fail(e.request, "setMyName", code=400, description="Bad Request: too many requests")
    try:
        assert await e.sync.push(e.link) is False
        assert count(e, "setMyDescription") == 1 and count(e, "setMyCommands") == 1 and count(e, "setMyProfilePhoto") == 1
        assert (await db.links.get(e.link.id)).profile_pending is True
    finally:
        await e.channel.stop()


@pytest.mark.asyncio
async def test_a_persons_language_change_reaches_only_their_bots_and_reopens_retries(db, monkeypatch):
    e = await environment(db, monkeypatch, lead=False)
    e.sync._failing_since[e.link.id] = datetime.now(UTC) - timedelta(hours=48)
    try:
        await e.sync.push_owed()
        await e.sync.push_owed(owner_user_id="another-person")
        assert count(e, "setMyName") == 0
        script(e)
        await e.sync.push_owed(owner_user_id=e.link.owner_user_id)
        assert count(e, "setMyName") == 1 and (await db.links.get(e.link.id)).profile_pending is False
    finally:
        await e.channel.stop()


def test_a_blank_letter_is_not_drawn_as_initials():
    assert initials_of("\u3164 name")[0] == "\u3164"
    assert render_avatar("\u3164", "#1D4ED8", "custom_agent") == render_avatar("", "#1D4ED8", "custom_agent")


def test_an_initial_stays_one_character_in_upper_case():
    assert initials_of("ßa ßb") == "SS"


def test_a_font_licence_keeps_every_notice_the_font_carries():
    fonts = Path(avatar.__file__).parent / "fonts"
    assert "Arev" in (fonts / "DejaVuSans-Bold.LICENSE").read_text(encoding="utf-8") and "Apache License" in (fonts / "DroidSansFallbackFull.LICENSE").read_text(encoding="utf-8")


def test_an_unknown_kind_of_bot_owes_nothing_yet():
    sync = ProfileSync(SimpleNamespace())
    assert not sync._owed(SimpleNamespace(agent_kind="instance", profile_pending=True, profile_version=0))


@pytest.mark.asyncio
async def test_a_running_supervisor_pushes_an_owed_profile_at_start(db, monkeypatch):
    from test_telegram_chat_turns import until
    from test_telegram_supervisor import running_one

    def prepare(e, link):
        e.normal.enqueue_get_me(bot_id=link.bot_id)
        for endpoint in CALLS:
            e.normal.enqueue(endpoint, result=True)
        e.supervisor.prepare_ingress_values = AsyncMock(return_value={"product": "HartMesh", "name": "Fixture target", "agent": "Fixture target", "memory_url": None})

    e, link, held = await running_one(db, monkeypatch, profiles=True, prepare=prepare)
    try:
        await until(lambda: any(call.endpoint == "setMyProfilePhoto" for call in e.normal.calls))
        await until(lambda: not e.supervisor.profile_sync._failing_since)
        saved = await db.links.get(link.id)
        for _ in range(100):
            saved = await db.links.get(link.id)
            if not saved.profile_pending:
                break
            await asyncio.sleep(0.01)
        assert (saved.profile_pending, saved.profile_version, saved.bot_name) == (False, PROFILE_VERSION, "Fixture target")
    finally:
        held.release.set()
        await e.supervisor.stop()
