"""Parity tests for duplicate durable run identities.

The candidate UUID is an immutable primary identity, independently of thread,
owner, external key, or replacement strategy.  Both the in-memory and SQL
stores must reject a duplicate before mutating any predecessor or secondary
index.
"""

from __future__ import annotations

import copy
from collections.abc import Awaitable, Callable
from typing import Any

import pytest
from sqlalchemy.exc import IntegrityError

from deerflow.persistence.run import RunRepository
from deerflow.runtime import RunManager
from deerflow.runtime.runs.manager import AcceptedEvidenceIntegrityError
from deerflow.runtime.runs.store.base import (
    AdmissionOutcome,
    DuplicateRunIdentityError,
    RunStore,
)
from deerflow.runtime.runs.store.memory import MemoryRunStore


def test_asyncpg_wrapped_runs_primary_key_is_classified_exactly() -> None:
    """Adapter wrapping cannot turn duplicate run identity into thread busy."""

    from deerflow.persistence.run.sql import _is_run_primary_key_violation

    class _NativeViolation(Exception):
        constraint_name = "runs_pkey"

    class _AdaptedViolation(Exception):
        pass

    adapted = _AdaptedViolation("provider text must not be parsed")
    adapted.__cause__ = _NativeViolation("native provider text")
    duplicate = IntegrityError("INSERT", {}, adapted)
    assert _is_run_primary_key_violation(duplicate) is True

    other = _AdaptedViolation("duplicate-looking provider text")
    other.__cause__ = _NativeViolation("native provider text")
    other.__cause__.constraint_name = "uq_runs_external_identity"
    unrelated = IntegrityError("INSERT", {}, other)
    assert _is_run_primary_key_violation(unrelated) is False


async def _make_sqlite_store(tmp_path) -> RunRepository:
    from deerflow.persistence.engine import get_session_factory, init_engine

    url = f"sqlite+aiosqlite:///{tmp_path / 'duplicate-run-identity.db'}"
    await init_engine("sqlite", url=url, sqlite_dir=str(tmp_path))
    return RunRepository(get_session_factory())


async def _close_sqlite_store() -> None:
    from deerflow.persistence.engine import close_engine

    await close_engine()


async def _capture_store_evidence(store: RunStore) -> dict[str, Any]:
    return {
        "original": copy.deepcopy(await store.get("candidate-run", user_id=None)),
        "predecessor": copy.deepcopy(await store.get("target-predecessor", user_id=None)),
        "original_thread": copy.deepcopy(await store.list_by_thread("thread-original", user_id=None)),
        "target_thread": copy.deepcopy(await store.list_by_thread("thread-target", user_id=None)),
        "events": copy.deepcopy(await store.list_lifecycle_events()),
        "original_external": copy.deepcopy(await store.get_by_external_identity("scope-original", "key-original")),
        "new_external": copy.deepcopy(await store.get_by_external_identity("scope-new", "key-new")),
    }


async def _seed_original_and_predecessor(
    store: RunStore,
    *,
    keyed: bool,
) -> dict[str, Any]:
    common = {
        "thread_id": "thread-original",
        "owner_worker_id": "worker-owner",
        "lease_expires_at": None,
        "user_id": "owner-original",
    }
    if keyed:
        result = await store.ensure_run_atomic(
            "candidate-run",
            external_scope="scope-original",
            external_key="key-original",
            request_digest="a" * 64,
            request_digest_version="sha256-canonical-json-v1",
            caller_intent_json={"version": 1, "input": "original"},
            caller_intent_digest="b" * 64,
            caller_intent_digest_version="caller-intent-canonical-json-v1",
            **common,
        )
        assert result.outcome is AdmissionOutcome.created
    else:
        await store.create_thread_operation_atomic(
            "candidate-run",
            **common,
        )

    await store.create_thread_operation_atomic(
        "target-predecessor",
        thread_id="thread-target",
        owner_worker_id="worker-owner",
        lease_expires_at=None,
        user_id="owner-target",
    )
    return await _capture_store_evidence(store)


async def _assert_duplicate_is_non_mutating(
    store: RunStore,
    *,
    keyed: bool,
) -> None:
    before = await _seed_original_and_predecessor(store, keyed=keyed)

    duplicate_create: Callable[[], Awaitable[Any]]
    if keyed:

        async def duplicate_create() -> Any:
            return await store.ensure_run_atomic(
                "candidate-run",
                thread_id="thread-target",
                owner_worker_id="worker-owner",
                lease_expires_at=None,
                user_id="owner-new",
                external_scope="scope-new",
                external_key="key-new",
                request_digest="c" * 64,
                request_digest_version="sha256-canonical-json-v1",
                caller_intent_json={"version": 1, "input": "replacement"},
                caller_intent_digest="d" * 64,
                caller_intent_digest_version="caller-intent-canonical-json-v1",
                multitask_strategy="interrupt",
            )

    else:

        async def duplicate_create() -> Any:
            return await store.create_thread_operation_atomic(
                "candidate-run",
                thread_id="thread-target",
                owner_worker_id="worker-owner",
                lease_expires_at=None,
                user_id="owner-new",
                multitask_strategy="interrupt",
            )

    with pytest.raises(DuplicateRunIdentityError, match="candidate-run"):
        await duplicate_create()

    # A duplicate primary identity loses before replacement processing.  The
    # original row, both thread indexes, the keyed identity index, and the
    # complete lifecycle journal remain byte-for-byte equivalent.
    assert await _capture_store_evidence(store) == before
    assert before["original"]["thread_id"] == "thread-original"
    assert before["original"]["user_id"] == "owner-original"
    assert before["predecessor"]["status"] == "pending"
    assert len([event for event in before["events"] if event["run_id"] == "candidate-run"]) == 1


@pytest.mark.anyio
@pytest.mark.parametrize("keyed", [False, True], ids=["unkeyed", "keyed"])
async def test_memory_store_rejects_duplicate_run_identity_without_mutation(
    keyed: bool,
) -> None:
    await _assert_duplicate_is_non_mutating(MemoryRunStore(), keyed=keyed)


@pytest.mark.anyio
@pytest.mark.parametrize("keyed", [False, True], ids=["unkeyed", "keyed"])
async def test_sqlite_store_rejects_duplicate_run_identity_without_mutation(
    tmp_path,
    keyed: bool,
) -> None:
    store = await _make_sqlite_store(tmp_path)
    try:
        await _assert_duplicate_is_non_mutating(store, keyed=keyed)
    finally:
        await _close_sqlite_store()


@pytest.mark.anyio
@pytest.mark.parametrize("keyed", [False, True], ids=["unkeyed", "keyed"])
async def test_manager_reports_duplicate_candidate_as_bounded_integrity_failure(
    keyed: bool,
) -> None:
    store = MemoryRunStore()
    manager = RunManager(store=store, worker_id="worker-new")
    run_id = "00000000-0000-0000-0000-000000000123"
    await store.create_thread_operation_atomic(
        run_id,
        thread_id="thread-original",
        owner_worker_id="worker-original",
        lease_expires_at=None,
        user_id="owner-original",
    )

    with pytest.raises(
        AcceptedEvidenceIntegrityError,
        match="accepted_evidence_invalid",
    ):
        if keyed:
            await manager.ensure_or_reject(
                "thread-new",
                candidate_run_id=run_id,
                user_id="owner-new",
                external_scope="scope-new",
                external_key="key-new",
                request_digest="c" * 64,
                request_digest_version="sha256-canonical-json-v1",
                caller_intent_json={"version": 1, "input": "replacement"},
                caller_intent_digest="d" * 64,
                caller_intent_digest_version=("caller-intent-canonical-json-v1"),
            )
        else:
            await manager.create_or_reject(
                "thread-new",
                candidate_run_id=run_id,
                user_id="owner-new",
            )

    original = await store.authoritative_get(run_id)
    assert original is not None
    assert original["thread_id"] == "thread-original"
    assert original["user_id"] == "owner-original"
    assert manager._runs == {}
