"""Bounded Space-conflict retry timing; no transport or source authority stubbed."""

import pytest

from app.telegram_bots.outbound import retry_export
from deerflow.spaces.contract import SpaceConflict, SpaceDenied, SpaceNotFound
from deerflow.spaces.filesystem import FilesystemUnavailable, FileTooLarge

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize("succeed", [False, True])
async def test_only_conflict_repeats_at_the_configured_interval_until_deadline(succeed):
    now, attempts, checks = [0], [], []

    async def current():
        checks.append(now[0])

    async def sleep(seconds):
        assert seconds == 3
        now[0] += seconds

    async def read():
        attempts.append(now[0])
        if succeed and len(attempts) == 3:
            return b"original"
        raise SpaceConflict("File maintenance")

    if succeed:
        assert await retry_export(read, current=current, retry_seconds=3, retry_max_seconds=6, clock=lambda: now[0], sleep=sleep) == b"original"
    else:
        with pytest.raises(SpaceConflict):
            await retry_export(read, current=current, retry_seconds=3, retry_max_seconds=6, clock=lambda: now[0], sleep=sleep)
    assert attempts == checks == [0, 3, 6]


@pytest.mark.parametrize("error", [SpaceDenied, SpaceNotFound, FileTooLarge, FileNotFoundError, FilesystemUnavailable, RuntimeError])
async def test_other_export_failures_never_repeat(error):
    attempts = []

    async def current():
        pass

    async def read():
        attempts.append(True)
        raise error("Unavailable")

    async def sleep(seconds):
        raise AssertionError("Only SpaceConflict may wait")

    with pytest.raises(error):
        await retry_export(read, current=current, retry_seconds=3, retry_max_seconds=120, sleep=sleep)
    assert attempts == [True]


async def test_retired_source_after_conflict_cannot_read_again():
    checks, reads = [], []

    async def current():
        checks.append(True)
        if len(checks) == 2:
            raise ValueError("retired")

    async def read():
        reads.append(True)
        raise SpaceConflict("Busy")

    async def sleep(seconds):
        pass

    with pytest.raises(ValueError, match="retired"):
        await retry_export(read, current=current, retry_seconds=3, retry_max_seconds=120, sleep=sleep)
    assert reads == [True]
