"""Redaction reaches application/library and non-propagating logger handlers."""

import io
import logging
import secrets
import sys
from uuid import uuid4

import pytest
from telegram.error import InvalidToken

from app.telegram_bots.log_filter import LOG_TOKEN_MARK, BotTokenLogFilter, install_token_log_filter


def token():
    return str(secrets.randbelow(900000) + 100000) + ":" + secrets.token_urlsafe(30)


def record(name, message, args=(), exc_info=None):
    return logging.LogRecord(name, logging.ERROR, __file__, 1, message, args, exc_info)


@pytest.fixture
def restore_filters():
    loggers = [logging.getLogger(), *(entry for entry in logging.root.manager.loggerDict.values() if isinstance(entry, logging.Logger))]
    logger_filters = {item: list(item.filters) for item in loggers}
    handlers = {handler for item in loggers for handler in item.handlers}
    handler_filters = {item: list(item.filters) for item in handlers}
    yield
    current = [logging.getLogger(), *(entry for entry in logging.root.manager.loggerDict.values() if isinstance(entry, logging.Logger))]
    for item in current:
        item.filters[:] = logger_filters.get(item, [f for f in item.filters if not isinstance(f, BotTokenLogFilter)])
        for handler in item.handlers:
            handler.filters[:] = handler_filters.get(handler, [f for f in handler.filters if not isinstance(f, BotTokenLogFilter)])


@pytest.mark.parametrize("name", ["telegram", "telegram.Bot", "httpx", "httpcore.connection", "app.channels.base", "app.channels.message_bus", "app.telegram_bots.polling"])
def test_filter_replaces_tokens_in_formatted_messages_for_any_logger(name):
    secret = token()
    value = record(name, "request %s and %s", (secret, "9" + secret))
    assert BotTokenLogFilter().filter(value) is True
    assert secret not in value.getMessage() and value.getMessage().count(LOG_TOKEN_MARK) == 2
    assert value.args is None and secret not in str(value.msg)


def test_filter_sanitizes_chained_exception_and_cached_exception_text():
    secret = token()
    try:
        try:
            raise RuntimeError("inner " + secret)
        except RuntimeError as cause:
            raise ValueError("outer " + secret) from cause
    except ValueError:
        value = record("app.channels.base", "failed %s", (secret,), sys.exc_info())
    assert BotTokenLogFilter().filter(value)
    assert value.exc_info is None and value.exc_text.count(LOG_TOKEN_MARK) == 2
    assert secret not in logging.Formatter().format(value)
    cached = record("uvicorn.error", "already formatted")
    cached.exc_text = "cached failure " + secret
    assert BotTokenLogFilter().filter(cached) and cached.exc_info is None
    assert cached.exc_text == "cached failure " + LOG_TOKEN_MARK


@pytest.mark.parametrize("name", ["telegram", "telegram.Bot", "telegram.ext.Application"])
def test_filter_drops_unparseable_raw_updates_only_from_telegram_family(name):
    assert BotTokenLogFilter().filter(record(name, "Error while parsing updates: private content")) is False
    assert BotTokenLogFilter().filter(record("app.telegram_bots.polling", "Error while parsing updates: a product diagnostic")) is True


def test_record_without_token_or_raw_updates_passes_unchanged():
    value = record("app.channels.base", "normal %s", ("message",))
    original = value.__dict__.copy()
    assert BotTokenLogFilter().filter(value) is True
    assert value.__dict__ == original


def test_actual_library_invalid_token_exception_is_sanitized():
    secret = token()
    error = InvalidToken("The token `" + secret + "` was rejected by the server")
    value = record("telegram.Bot", "library request failed", exc_info=(type(error), error, None))
    assert BotTokenLogFilter().filter(value) is True
    assert secret not in logging.Formatter().format(value) and value.exc_info is None
    assert LOG_TOKEN_MARK == "<bot token>"


def test_exception_without_token_passes_unchanged():
    error = RuntimeError("safe diagnostic")
    value = record("app.channels.base", "normal %s", ("message",), (type(error), error, None))
    original = value.__dict__.copy()
    assert BotTokenLogFilter().filter(value) is True
    assert value.__dict__ == original


@pytest.mark.parametrize(
    "name,message",
    [("telegram.Bot", "InvalidToken raised for %s"), ("telegram.Bot", "Making request to base address %s"), ("httpx", 'HTTP Request: POST "https://api.telegram.org/bot%s/getUpdates"')],
)
def test_library_request_and_error_records_remain_visible_without_the_token(name, message):
    secret = token()
    value = record(name, message, (secret,))
    assert BotTokenLogFilter().filter(value) is True
    assert secret not in value.getMessage() and LOG_TOKEN_MARK in value.getMessage()


def test_installer_reaches_root_and_nonpropagating_handlers_idempotently(restore_filters):
    root = logging.getLogger()
    isolated = logging.getLogger("telegram-filter-isolated-" + uuid4().hex)
    root_output, isolated_output = io.StringIO(), io.StringIO()
    root_handler, isolated_handler = logging.StreamHandler(root_output), logging.StreamHandler(isolated_output)
    root.addHandler(root_handler)
    isolated.addHandler(isolated_handler)
    isolated.propagate = False
    try:
        install_token_log_filter()
        install_token_log_filter()
        for name in ("telegram", "telegram.ext", "httpx", "httpcore"):
            assert sum(isinstance(f, BotTokenLogFilter) for f in logging.getLogger(name).filters) == 1
        assert sum(isinstance(f, BotTokenLogFilter) for f in root_handler.filters) == 1
        assert sum(isinstance(f, BotTokenLogFilter) for f in isolated_handler.filters) == 1
        secret = token()
        for logger in (logging.getLogger("app.channels.base"), logging.getLogger("app.channels.message_bus"), logging.getLogger("app.telegram_bots.polling"), isolated):
            try:
                raise RuntimeError("failure " + secret)
            except RuntimeError:
                logger.error("safe class %s", secret, exc_info=True)
        assert secret not in root_output.getvalue() and root_output.getvalue().count(LOG_TOKEN_MARK) == 6
        assert secret not in isolated_output.getvalue() and isolated_output.getvalue().count(LOG_TOKEN_MARK) == 2
    finally:
        root.removeHandler(root_handler)
        isolated.removeHandler(isolated_handler)
        root_handler.close()
        isolated_handler.close()
