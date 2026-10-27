"""Durable prompt scope/closed guards; no future prompt service is activated."""

from datetime import timedelta

import pytest
from _telegram_repository_test_support import db as db  # noqa: F401
from test_telegram_callbacks import api  # noqa: F401
from test_telegram_callbacks import callbacks_environment as callbacks_environment
from test_telegram_publication_custody import publication_environment as publication_environment  # noqa: F401


@pytest.mark.asyncio
async def test_prompt_scope_and_expiry_preserve_original_closed_identity(callbacks_environment):
    e = callbacks_environment
    prompts = api().Prompts(e.callbacks)
    identity = await prompts.open(e.link, "clarify_other", {"thread_id": "thread", "request_id": "request"}, chat_id=e.link.chat_id, message_thread_id=None, message_id=700)
    assert (await prompts.current(e.link, e.link.chat_id, None)).id == identity
    assert await prompts.current(e.link, e.link.chat_id, 99) is None
    e.clock[0] += timedelta(seconds=601)
    guard = await prompts.current(e.link, e.link.chat_id, None)
    assert guard.id == identity and guard.closed_reason == "expired" and guard.consumed_at is not None


@pytest.mark.asyncio
async def test_replacement_and_deliberate_navigation_are_scoped(callbacks_environment):
    e = callbacks_environment
    prompts = api().Prompts(e.callbacks)
    first = await prompts.open(e.link, "clarify_other", {"thread_id": "thread", "request_id": "one"}, chat_id=e.link.chat_id, message_thread_id=None, message_id=700)
    other = await prompts.open(e.link, "clarify_other", {"thread_id": "thread", "request_id": "topic"}, chat_id=e.link.chat_id, message_thread_id=99, message_id=701)
    second = await prompts.open(e.link, "clarify_other", {"thread_id": "thread", "request_id": "two"}, chat_id=e.link.chat_id, message_thread_id=None, message_id=702)
    assert (await e.callbacks.get(first)).closed_reason == "replaced"
    assert (await prompts.current(e.link, e.link.chat_id, None)).id == second
    await prompts.close(e.link, e.link.chat_id, None, reason="command")
    assert await prompts.current(e.link, e.link.chat_id, None) is None
    assert (await prompts.current(e.link, e.link.chat_id, 99)).id == other
    recreated = api().Prompts(api().CallbackRepository(e.sessions, config=e.db.config, clock=lambda: e.clock[0]))
    assert (await recreated.current(e.link, e.link.chat_id, 99)).id == other
