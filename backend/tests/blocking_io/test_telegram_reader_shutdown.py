"""Reader SQL cleanup remains owned under strict event-loop checks."""

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_reader_shutdown import test_shutdown_waits_for_reader_close as reader_shutdown
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging


@pytest.mark.asyncio
@pytest.mark.parametrize("reader", ["sender", "snapshot", "links", "settings", "observation"])
@pytest.mark.parametrize("phase", ["body", "close"])
async def test_reader_cleanup(db, monkeypatch, reader, phase):
    await reader_shutdown(db, monkeypatch, reader, phase)
