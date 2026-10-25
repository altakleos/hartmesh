"""Terminal reads, commits and retries do not share one revocable admission."""

import asyncio

import pytest
from _run_output_store_test_support import RAW_OWNER, capture_run
from _run_output_store_test_support import output_store as output_store
from test_run_output_store import receipt

from deerflow.runtime.runs.capture_io import CaptureIOClosed, capture_io_scope
from deerflow.runtime.runs.schemas import RunStatus
from deerflow.runtime.runs.terminal import TerminalRecoveryPolicy


@pytest.mark.asyncio
@pytest.mark.parametrize("retire_at", ["read", "committed", "retry", "open"])
async def test_retirement_between_terminal_leaves_preserves_only_confirmed_evidence(output_store, monkeypatch, retire_at):
    manager, record = await capture_run(output_store)
    owner = manager.get_capture_io_owner(record)
    original_get, original_finalize = output_store.repository.get, output_store.repository.finalize_owned
    reads, writes = [], []
    retry_started, release_retry = asyncio.Event(), asyncio.Event()

    async def observed_get(*args, **kwargs):
        result = await original_get(*args, **kwargs)
        reads.append(result)
        if retire_at == "read":
            owner.retire()
        return result

    async def observed_finalize(*args, **kwargs):
        writes.append(kwargs)
        if retire_at == "retry":
            owner.retire()
            retry_started.set()
            raise OSError("Controlled pre-delegate failure")
        result = await original_finalize(*args, **kwargs)
        if retire_at == "committed":
            owner.retire()
        return result

    monkeypatch.setattr(output_store.repository, "get", observed_get)
    monkeypatch.setattr(output_store.repository, "finalize_owned", observed_finalize)
    manager._terminal_recovery_policy = TerminalRecoveryPolicy(max_attempts=3, attempt_timeout=2, retry_delay=10)
    result, error = None, None
    try:
        with capture_io_scope(owner):
            try:
                result = await asyncio.wait_for(manager.finalize_output(record.run_id, RunStatus.success, output_receipt=receipt(record)), 3)
            except CaptureIOClosed as exc:
                error = exc
        stored = await original_get(record.run_id, user_id=RAW_OWNER)
        if retire_at in {"read", "retry"}:
            assert isinstance(error, CaptureIOClosed) and result is None
            assert stored["status"] == "running" and "output_evidence" not in stored["kwargs"]
            assert len(reads) == 1 and len(writes) == (0 if retire_at == "read" else 1)
        else:
            assert result.finalized and error is None
            assert stored["status"] == "success" and stored["kwargs"]["output_evidence"]["kind"] == "text"
            assert len(reads) == len(writes) == 1
    finally:
        release_retry.set()
        owner.retire()
        await owner.drain()
        await manager.shutdown(timeout=0)
