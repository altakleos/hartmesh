"""Pending publication metadata retains its whole keyboard through ordinary GC."""

from datetime import timedelta

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_callbacks import callbacks_environment as callbacks_environment
from test_telegram_publication_custody import publication_environment as publication_environment

from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow


@pytest.mark.asyncio
@pytest.mark.parametrize("pending_owner", [True, False])
async def test_sweep_retains_all_siblings_of_pending_or_malformed_owner(callbacks_environment, pending_owner):
    e = callbacks_environment
    await e.plan()
    async with e.sessions() as session, session.begin():
        controls = list(await session.scalars(select(TelegramCallbackRow)))
        identities = {row.id for row in controls}
        assert len(controls) == 2
        if pending_owner:
            # Malformed server metadata cannot qualify completion or an action;
            # it must also not discard the sole receipt and its sibling layout.
            controls[0].payload = {**controls[0].payload, "publication_owner": {}}
        for parent in await session.scalars(select(TelegramOutboxRow)):
            parent.state = "sent"
    removed = await e.callbacks.sweep(e.clock[0] + timedelta(days=181))
    async with e.sessions() as session:
        remaining = {row.id for row in await session.scalars(select(TelegramCallbackRow))}
    assert removed == (0 if pending_owner else 2)
    assert remaining == (identities if pending_owner else set())
