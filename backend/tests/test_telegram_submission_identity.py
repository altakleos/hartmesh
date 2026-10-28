"""Submission receipts retain actual human records and immutable request scope."""

from copy import deepcopy

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_callback_publication import retry_input_environment as retry_input_environment
from test_telegram_publication_authority import attempt_environment as attempt_environment
from test_telegram_publication_coordinator import coordinator_environment as coordinator_environment

from app.telegram_bots import inbox


@pytest.mark.asyncio
async def test_input_snapshot_receipt_keeps_actual_rows_and_rejects_changed_text(retry_input_environment):
    e = retry_input_environment
    await e.stage()
    record = await e.inbox.reserve_callback_publication(e.link, e.callback_id, thread_id=e.thread_id, endpoint_id=e.http.endpoint_id, guard=e.guard_factory())
    async with e.db.sessions.begin() as writer:
        receipt = await e.inbox.snapshot_publication_inputs(record, session=writer)
    restored = inbox.validate_publication_inputs(record, receipt)
    assert [row.message_id for row in restored] == [701, 702]
    assert receipt["submission"] is None and receipt["version"] == 1
    assert record.input_sha256 == inbox._publication_input(restored)
    changed = deepcopy(receipt)
    changed["inputs"][0]["sanitized_input"]["text"] = "A different answer"
    with pytest.raises(ValueError):
        inbox.validate_publication_inputs(record, changed)
