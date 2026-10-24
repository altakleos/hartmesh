"""Pin library behavior used by the Telegram adapters; requests stay offline."""

from __future__ import annotations

import asyncio
import inspect
from importlib.metadata import version

import pytest
import telegram
from _telegram_bot_test_support import RecordingRequest, offline_bot
from telegram import Bot, Update, User
from telegram.error import BadRequest, Conflict, Forbidden, InvalidToken, NetworkError, RetryAfter, TimedOut
from telegram.ext import AIORateLimiter, Application, TypeHandler

from app.channels.telegram import TelegramChannel


def test_library_version_is_22_8():
    assert version("python-telegram-bot") == "22.8"


def test_bot_api_version_is_10_0():
    assert telegram.constants.BOT_API_VERSION == "10.0"


def test_rate_limiter_extra_is_installed():
    assert isinstance(AIORateLimiter(), AIORateLimiter)


def test_native_methods_exist():
    for name in (
        "send_message_draft",
        "get_managed_bot_token",
        "replace_managed_bot_token",
        "set_managed_bot_access_settings",
        "set_my_profile_photo",
        "create_forum_topic",
        "get_webhook_info",
        "delete_webhook",
        "set_message_reaction",
        "pin_chat_message",
        "unpin_chat_message",
    ):
        assert callable(getattr(Bot, name, None)), name
    for name in ("LinkPreviewOptions", "ReplyParameters", "InaccessibleMessage"):
        assert inspect.isclass(getattr(telegram, name, None)), name


@pytest.mark.asyncio
@pytest.mark.parametrize("status,error", [(401, InvalidToken), (404, InvalidToken), (403, Forbidden), (400, BadRequest), (409, Conflict), (429, RetryAfter), (502, NetworkError)])
async def test_http_statuses_map_to_the_error_classes(status, error):
    request = RecordingRequest()
    request.enqueue_get_me()
    description = "Scripted Telegram failure"
    body = {"ok": False, "error_code": status, "description": description}
    if status == 429:
        body["parameters"] = {"retry_after": 7}
    request.enqueue("getMe", status=status, body=body)
    async with offline_bot(request) as bot:
        with pytest.raises(error) as caught:
            await bot.get_me()
    assert type(caught.value) is error
    if status == 429:
        assert caught.value.retry_after == 7
    else:
        assert str(caught.value) == description
    assert issubclass(BadRequest, NetworkError)
    assert issubclass(TimedOut, NetworkError)


@pytest.mark.asyncio
async def test_unknown_update_field_and_parameter_pass_through_api_kwargs():
    request = RecordingRequest()
    request.enqueue_get_me()
    request.enqueue("sendMessageDraft")
    async with offline_bot(request) as bot:
        stopped = {"chat": {"id": 42, "type": "private"}, "draft_id": 9}
        update = Update.de_json({"update_id": 1, "stopped_message_generation": stopped}, bot)
        assert update.api_kwargs["stopped_message_generation"] == stopped
        assert await bot.send_message_draft(chat_id=42, draft_id=9, text="Hello", api_kwargs={"can_stop": True, "keep_on_stop": True})
    assert request.calls[-1].endpoint == "sendMessageDraft"
    assert request.calls[-1].parameters == {"chat_id": 42, "draft_id": 9, "text": "Hello", "can_stop": True, "keep_on_stop": True}
    assert request.initialized and request.closed


def test_user_has_can_manage_bots_and_topic_fields():
    fields = {"can_manage_bots": True, "has_topics_enabled": True, "allows_users_to_create_topics": True}
    user = User.de_json({"id": 123, "is_bot": True, "first_name": "Bot", **fields}, None)
    for name, value in fields.items():
        assert getattr(user, name, None) is value, name
        assert name not in user.api_kwargs


def test_managed_bot_updated_handler_exists():
    from telegram.ext import ManagedBotUpdatedHandler

    async def callback(update, context):
        pass

    handler = ManagedBotUpdatedHandler(callback, user_id=123, username="offline_test_bot", block=True)
    assert handler.callback is callback
    assert "managed_bot" in inspect.signature(Update).parameters


@pytest.mark.asyncio
async def test_application_builds_with_updater_none():
    request = RecordingRequest()
    updates_request = RecordingRequest()
    request.enqueue_get_me()
    app = Application.builder().bot(offline_bot(request, updates_request=updates_request)).updater(None).build()
    processed = asyncio.Event()
    seen = []

    async def callback(update, context):
        seen.append(update.update_id)
        processed.set()

    app.add_handler(TypeHandler(Update, callback))
    assert app.updater is None
    async with app:
        await app.start()
        try:
            await app.update_queue.put(Update(update_id=17))
            await asyncio.wait_for(processed.wait(), timeout=2)
            await asyncio.wait_for(app.update_queue.join(), timeout=2)
        finally:
            await app.stop()
    assert seen == [17]
    assert not app.running
    assert request.closed and updates_request.closed
    assert [call.endpoint for call in request.calls] == ["getMe"]
    assert updates_request.calls == []


def test_upstream_adapter_members_used_by_subclass_exist():
    # Bind explicit call contracts, allowing compatible optional additions while
    # rejecting removed/renamed parameters and new required arguments.
    contracts = {
        "start": ((), {}),
        "stop": ((), {}),
        "send": ((), {"msg": object(), "_max_retries": 3}),
        "_run_on_telegram_loop": ((), {"coroutine": object()}),
        "_get_download_bot": ((), {}),
    }
    for name, (args, kwargs) in contracts.items():
        member = getattr(TelegramChannel, name)
        assert inspect.iscoroutinefunction(member), name
        inspect.signature(member).bind(object(), *args, **kwargs)
    helper_args = {
        "_strip_bot_username_from_leading_command": ("/new@offline_test_bot", "offline_test_bot"),
        "_extract_inbound_files": (object(),),
        "_safe_inbound_filename": ("file.txt", "fallback.txt"),
        "_file_size": (123,),
        "_parse_message_id": ("17",),
        "_telegram_display_name": (object(),),
    }
    for name, args in helper_args.items():
        member = getattr(TelegramChannel, name)
        assert not inspect.iscoroutinefunction(member), name
        inspect.signature(member).bind(*args)


@pytest.mark.asyncio
async def test_recording_request_controls_independent_completion_and_cancellation():
    request = RecordingRequest()
    request.enqueue_get_me()
    release = asyncio.Event()
    held = request.enqueue("deleteWebhook", release=release)
    request.enqueue("getWebhookInfo", result={"url": "", "has_custom_certificate": False, "pending_update_count": 0})
    async with offline_bot(request) as bot:
        task = asyncio.create_task(bot.delete_webhook())
        try:
            await asyncio.wait_for(held.started.wait(), timeout=2)
            assert not task.done()
            assert (await bot.get_webhook_info()).pending_update_count == 0
            release.set()
            assert await asyncio.wait_for(task, timeout=2)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        cancelled = request.enqueue("deleteWebhook", release=asyncio.Event())
        task = asyncio.create_task(bot.delete_webhook())
        try:
            await asyncio.wait_for(cancelled.started.wait(), timeout=2)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        assert cancelled.cancelled.is_set()


@pytest.mark.asyncio
async def test_recording_request_rejects_unscripted_calls():
    request = RecordingRequest()
    with pytest.raises(NetworkError, match="Unscripted Bot API endpoint: getMe"):
        async with offline_bot(request):
            pass
