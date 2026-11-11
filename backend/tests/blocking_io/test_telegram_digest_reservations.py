"""Real digest SQL ownership stays off blocking event-loop paths."""

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_digest_reservations import test_ambiguous_digest_retry_and_restart_keep_members_and_controls_fixed as retry_digest


@pytest.mark.asyncio
async def test_digest_retry_ownership(db):
    await retry_digest(db)
