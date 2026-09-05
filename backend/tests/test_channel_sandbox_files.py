"""Channel attachment copies skip, and record, when an accepted run holds the thread."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.channels import sandbox_files
from deerflow.sandbox.session import SandboxSessionConflict

PUBLIC_REF = "accepted-execution-" + "b" * 64


def test_a_refused_attachment_copy_is_a_recorded_skip(monkeypatch: pytest.MonkeyPatch) -> None:
    observed: list[tuple[str, str, dict]] = []
    registry = SimpleNamespace(observe=lambda public_ref, kind, *, facts: observed.append((public_ref, kind, dict(facts))) or True)
    monkeypatch.setattr("deerflow.sandbox.session.get_sandbox_session_registry", lambda: registry)
    monkeypatch.setattr(sandbox_files, "acquire_sandbox_client_lease", AsyncMock(side_effect=SandboxSessionConflict("held", public_ref=PUBLIC_REF)))
    provider = MagicMock()
    provider.uses_thread_data_mounts = False

    synced = asyncio.run(
        sandbox_files.sync_file_to_thread_sandbox(
            provider,
            thread_id="thread-1",
            user_id="user-1",
            virtual_path="/mnt/user-data/uploads/a.txt",
            content=b"hello",
            owner_prefix="feishu:attachment",
        )
    )

    assert synced is False
    assert observed == [(PUBLIC_REF, "session.refused", {"requester": "feishu:attachment", "reason": "sandbox_session_conflict"})]


def test_an_admitted_attachment_copy_still_syncs_and_releases(monkeypatch: pytest.MonkeyPatch) -> None:
    sandbox = MagicMock()
    lease = SimpleNamespace(sandbox_id="sbx-1", sandbox=sandbox, release=AsyncMock())

    async def run_sync(func, *args):
        return func(*args)

    lease.run_sync = run_sync
    monkeypatch.setattr(sandbox_files, "acquire_sandbox_client_lease", AsyncMock(return_value=lease))
    provider = MagicMock()
    provider.uses_thread_data_mounts = False

    synced = asyncio.run(
        sandbox_files.sync_file_to_thread_sandbox(
            provider,
            thread_id="thread-1",
            user_id="user-1",
            virtual_path="/mnt/user-data/uploads/a.txt",
            content=b"hello",
            owner_prefix="feishu:attachment",
        )
    )

    assert synced is True
    sandbox.update_file.assert_called_once_with("/mnt/user-data/uploads/a.txt", b"hello")
    lease.release.assert_awaited_once()
