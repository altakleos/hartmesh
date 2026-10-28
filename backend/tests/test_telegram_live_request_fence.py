"""A limiter's delayed callback must recheck the original live presentation."""

import importlib

import pytest


@pytest.mark.asyncio
async def test_limiter_rechecks_after_wait_and_before_each_retry():
    module = importlib.import_module("app.telegram_bots.adapter")
    active = [True]
    now = [0.0]
    calls = []

    async def guard():
        calls.append("guard")
        if not active[0]:
            raise ValueError("retired")

    async def sleep(_seconds):
        active[0] = False
        now[0] += _seconds

    async def callback():
        calls.append("network")
        from telegram.error import RetryAfter

        raise RetryAfter(1)

    limiter = module.BoundedRateLimiter(sleep=sleep, monotonic=lambda: now[0])
    await limiter.initialize()
    try:
        with pytest.raises(ValueError, match="retired"):
            await limiter.process_request(callback, (), {}, "sendMessageDraft", {"chat_id": 71}, module.LiveRequestFence(guard))
        assert calls == ["guard", "network", "guard"]
    finally:
        await limiter.shutdown()
