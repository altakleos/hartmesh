"""Sanitize managed-bot credentials without suppressing transport diagnostics."""

import logging
import re

from app.telegram_bots.adapter import TOKEN_BODY

LOG_TOKEN_MARK = "<bot token>"
_TOKEN = re.compile(TOKEN_BODY)


class BotTokenLogFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        if (record.name == "telegram" or record.name.startswith("telegram.")) and message.startswith("Error while parsing updates"):
            return False
        sanitized = _TOKEN.sub(LOG_TOKEN_MARK, message)
        if sanitized != message:
            record.msg, record.args = sanitized, None
        exception = record.exc_text
        if record.exc_info:
            rendered = logging.Formatter().formatException(record.exc_info)
            if _TOKEN.search(rendered):
                exception = rendered
        if exception and _TOKEN.search(exception):
            record.exc_text = _TOKEN.sub(LOG_TOKEN_MARK, exception)
            record.exc_info = None
        return True


def install_token_log_filter() -> None:
    """Install on actual emission paths, including non-propagating services."""
    sanitizer = BotTokenLogFilter()

    def attach(target):
        if not any(isinstance(existing, BotTokenLogFilter) for existing in target.filters):
            target.addFilter(sanitizer)

    for name in ("telegram", "telegram.ext", "httpx", "httpcore"):
        attach(logging.getLogger(name))
    for handler in logging.getLogger().handlers:
        attach(handler)
    for logger in list(logging.root.manager.loggerDict.values()):
        if isinstance(logger, logging.Logger) and not logger.propagate:
            for handler in logger.handlers:
                attach(handler)
