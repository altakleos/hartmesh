"""Brief live references may drain; unknown effects and cancelled waits never retry."""

import asyncio
from contextlib import asynccontextmanager
from threading import Lock

import pytest

from app.gateway import storage_reclamation as module
from deerflow.community.aio_sandbox.owner_admission import NativeRetirementBusy, OwnerAdmissions
from deerflow.spaces.docker import StorageHostUnqualified

SLOT = "a" * 32


@pytest.mark.asyncio
async def test_permanently_live_reference_keeps_retirement_bounded_and_effect_free(monkeypatch):
    monkeypatch.setattr(module, "RETIREMENT_WAIT_SECONDS", 0.03)
    gate, effects = OwnerAdmissions(lock=Lock()), []
    reference = gate.enter("owner")

    @asynccontextmanager
    async def retire(owner, slot):
        token = gate.reserve(owner, slot)
        try:
            effects.append("entered")
            yield token
        finally:
            gate.end_retirement(token)

    try:
        with pytest.raises(NativeRetirementBusy):
            async with module._retirement_scope(retire, "owner", SLOT):
                pytest.fail("A live owner cannot grant retirement")
        assert not effects and not gate._owners and not gate._slots
        with gate.lock:
            assert gate.admission_current_locked(reference)
    finally:
        gate.end(reference)


@pytest.mark.asyncio
async def test_cancelled_busy_wait_cannot_reserve_after_reference_drains():
    gate, attempted, effects = OwnerAdmissions(lock=Lock()), asyncio.Event(), []
    reference = gate.enter("owner")

    @asynccontextmanager
    async def retire(owner, slot):
        attempted.set()
        token = gate.reserve(owner, slot)
        try:
            effects.append("entered")
            yield token
        finally:
            gate.end_retirement(token)

    async def waiting():
        async with module._retirement_scope(retire, "owner", SLOT):
            pytest.fail("Cancelled waiting must never enter")

    task = asyncio.create_task(waiting())
    try:
        await attempted.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        gate.end(reference)
        await asyncio.sleep(0.02)
        assert not effects and not gate._owners and not gate._slots
    finally:
        gate.end(reference)
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_unknown_entry_failure_is_not_retried():
    calls = []

    @asynccontextmanager
    async def retire(_owner, _slot):
        calls.append(True)
        raise StorageHostUnqualified("Original physical graph is unconfirmed")
        yield

    with pytest.raises(StorageHostUnqualified, match="unconfirmed"):
        async with module._retirement_scope(retire, "owner", SLOT):
            pytest.fail("Unconfirmed entry must refuse")
    assert calls == [True]


@pytest.mark.asyncio
async def test_busy_error_after_entry_does_not_repeat_the_effect():
    effects = []

    @asynccontextmanager
    async def retire(_owner, _slot):
        effects.append("entered")
        try:
            yield
        finally:
            effects.append("closed")

    with pytest.raises(NativeRetirementBusy):
        async with module._retirement_scope(retire, "owner", SLOT):
            raise NativeRetirementBusy("A later nested operation is busy")
    assert effects == ["entered", "closed"]
