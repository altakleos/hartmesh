"""Managed Telegram bot application services."""

from app.channels.manager import register_inbound_file_reader, register_inbound_file_write_failed
from app.telegram_bots.files import read_inbound, write_failed

register_inbound_file_reader("telegram", read_inbound)
register_inbound_file_write_failed("telegram", write_failed)
