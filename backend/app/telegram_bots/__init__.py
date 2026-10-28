"""Managed Telegram bot application services."""

from app.channels.manager import register_inbound_file_reader, register_inbound_file_write_failed
from app.telegram_bots.files import read_inbound, write_failed

register_inbound_file_reader("telegram", read_inbound)
register_inbound_file_write_failed("telegram", write_failed, prepare_failure_filter=lambda file: type(file.get("telegram_bot_id")) is int and type(file.get("message_id")) is int)
