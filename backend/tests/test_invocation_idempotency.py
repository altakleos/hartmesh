"""Atomic idempotent admission across HTTP, Scheduled Tasks, and channels."""

from __future__ import annotations

import asyncio
import os
import uuid
from contextlib import asynccontextmanager, contextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from support.postgres import postgres_async_url

from app.gateway.auth_disabled import AUTH_DISABLED_USER_ID, AUTH_SOURCE_AUTH_DISABLED, AUTH_SOURCE_INTERNAL, AUTH_SOURCE_SESSION
from app.gateway.authz import _ALL_PERMISSIONS
from app.gateway.credential_evidence import build_boundary_credential_evidence
from app.gateway.run_models import RunCreateRequest
from app.gateway.services import _GatewayLaunchNormalizer, build_channel_invocation_runtime, start_run
from app.runtime.idempotency import (
    REQUEST_DIGEST_VERSION,
    CanonicalCallerIntent,
    canonical_request_digest,
    normalize_external_key,
    scope_for_channel,
    scope_for_http,
    scope_for_scheduler,
)
from app.runtime.invocation import (
    DurableAdmission,
    InternalAdmissionIdentity,
    InternalLaunchIntent,
    InternalNativeChannelFacts,
    InternalSourceKind,
    InvocationRuntime,
    PreparedLaunch,
)
from app.runtime.native_binding import (
    InternalVerifiedNativeBinding,
    InternalVerifiedNativeBindingKind,
)
from deerflow.config.app_config import AppConfig, reset_app_config, set_app_config
from deerflow.persistence.base import Base
from deerflow.persistence.credential_audit import InMemoryCredentialAuditRepository
from deerflow.persistence.run.model import RunRow
from deerflow.persistence.run.sql import RunRepository
from deerflow.persistence.thread_meta.memory import MemoryThreadMetaStore
from deerflow.runtime import DisconnectMode, RunRecord, RunStatus
from deerflow.runtime.accepted_invocation import ResolvedAgentMaterialV1, ResolvedAgentRevision
from deerflow.runtime.events.store.memory import MemoryRunEventStore
from deerflow.runtime.runs.manager import ConflictError, IdempotencyConflictError, RunManager
from deerflow.runtime.runs.store.base import AdmissionOutcome
from deerflow.runtime.runs.store.memory import MemoryRunStore
from deerflow.runtime.tenant_identity import (
    TenantIdentityV1,
    tenant_admission_scope,
)

_POSTGRES_URL = os.environ.get("DEERFLOW_TEST_POSTGRES_URL")
_TEST_TENANT_IDENTITY = TenantIdentityV1.from_canonical_id("local")
_TEST_TENANT = _TEST_TENANT_IDENTITY.to_persisted_reference()


class _ReadyAdmissionFence:
    async def ready_for_admission(self) -> bool:
        return True


def _caller_intent_fields(value: dict[str, object]) -> dict[str, object]:
    intent = CanonicalCallerIntent(value)
    return {
        "caller_intent_json": intent.to_persisted(),
        "caller_intent_digest": intent.digest,
        "caller_intent_digest_version": intent.digest_version,
    }


def test_external_key_normalization_is_unambiguous_and_utf8_bounded() -> None:
    literal_digest = "sha256:utf8:" + "a" * 64
    assert normalize_external_key(literal_digest) == f"raw:{literal_digest}"
    assert normalize_external_key("é" * 127) == "raw:" + "é" * 127
    assert normalize_external_key("family\u200dkey") == "raw:family\u200dkey"

    normalized_long = normalize_external_key("é" * 128)
    assert normalized_long.startswith("sha256:utf8:")
    assert len(normalized_long) == len("sha256:utf8:") + 64
    assert normalized_long != normalize_external_key(literal_digest)


@pytest.mark.parametrize("value", ["", "event\n2", "event\x002", 42, None])
def test_external_key_rejects_empty_control_or_non_string_values(value: object) -> None:
    with pytest.raises(ValueError):
        normalize_external_key(value)  # type: ignore[arg-type]


def test_domain_tagged_scopes_do_not_have_delimiter_or_unicode_collisions() -> None:
    assert scope_for_http("user", "a:b") != scope_for_http("user:a", "b")
    assert scope_for_channel("slack", "c", "a:b", "chat") != scope_for_channel("slack", "c:a", "b", "chat")
    assert scope_for_channel("slack", "c", "", "聊:天") != scope_for_channel("slack", "c", "聊", ":天")
    assert scope_for_scheduler("__deerflow_system__", "task") != scope_for_scheduler("system", "task")
    assert scope_for_http("user", "u1").startswith("http:v1:sha256:")
    long_provider_scope = scope_for_channel("p" * 10_000, "c", "", "chat")
    assert len(long_provider_scope) <= 96
    assert long_provider_scope != scope_for_channel("p" * 9_999, "pc", "", "chat")


def test_canonical_request_digest_is_versioned_order_stable_and_array_sensitive() -> None:
    left = canonical_request_digest({"object": {"b": 2, "a": 1}, "array": [1, 2]})
    right = canonical_request_digest({"array": [1, 2], "object": {"a": 1, "b": 2}})
    reordered = canonical_request_digest({"object": {"a": 1, "b": 2}, "array": [2, 1]})

    assert REQUEST_DIGEST_VERSION == "sha256-canonical-json-v1"
    assert left == right
    assert left != reordered
    assert len(left) == 64


def test_canonical_request_digest_rejects_non_finite_numbers() -> None:
    with pytest.raises(ValueError):
        canonical_request_digest({"temperature": float("nan")})


async def _assert_store_ensure_contract(store) -> None:
    scope = scope_for_http("user", "owner-1")
    key = normalize_external_key("request-1")
    digest = canonical_request_digest({"input": "hello"})

    created = await store.ensure_run_atomic(
        "run-created",
        thread_id="thread-1",
        owner_worker_id="worker-1",
        lease_expires_at=None,
        external_scope=scope,
        external_key=key,
        request_digest=digest,
        request_digest_version=REQUEST_DIGEST_VERSION,
        **_caller_intent_fields({"input": "hello"}),
        user_id="owner-1",
    )
    assert created.outcome is AdmissionOutcome.created
    assert created.row["run_id"] == "run-created"

    same = await store.ensure_run_atomic(
        "run-lost-response",
        thread_id="thread-1",
        owner_worker_id="worker-2",
        lease_expires_at=None,
        external_scope=scope,
        external_key=key,
        request_digest=digest,
        request_digest_version=REQUEST_DIGEST_VERSION,
        **_caller_intent_fields({"input": "hello"}),
        user_id="owner-1",
    )
    assert same.outcome is AdmissionOutcome.known_same
    assert same.row["run_id"] == "run-created"

    conflict = await store.ensure_run_atomic(
        "run-conflict",
        thread_id="thread-1",
        owner_worker_id="worker-2",
        lease_expires_at=None,
        external_scope=scope,
        external_key=key,
        request_digest=canonical_request_digest({"input": "changed"}),
        request_digest_version=REQUEST_DIGEST_VERSION,
        **_caller_intent_fields({"input": "changed"}),
        user_id="owner-1",
    )
    assert conflict.outcome is AdmissionOutcome.key_conflict
    assert conflict.row["run_id"] == "run-created"

    await store.update_status("run-created", "success")
    terminal_replay = await store.ensure_run_atomic(
        "run-after-success",
        thread_id="thread-1",
        owner_worker_id="worker-2",
        lease_expires_at=None,
        external_scope=scope,
        external_key=key,
        request_digest=digest,
        request_digest_version=REQUEST_DIGEST_VERSION,
        **_caller_intent_fields({"input": "hello"}),
        user_id="owner-1",
    )
    assert terminal_replay.outcome is AdmissionOutcome.known_same
    assert terminal_replay.row["status"] == "success"


@pytest.mark.anyio
async def test_memory_store_ensure_is_idempotent_across_response_loss_and_terminal_state() -> None:
    await _assert_store_ensure_contract(MemoryRunStore())


@pytest.mark.anyio
async def test_sql_store_ensure_is_idempotent_across_response_loss_and_terminal_state(tmp_path) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'runs.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    try:
        await _assert_store_ensure_contract(RunRepository(async_sessionmaker(engine, expire_on_commit=False)))
    finally:
        await engine.dispose()


@pytest.mark.anyio
@pytest.mark.postgres_contract
@pytest.mark.skipif(
    not _POSTGRES_URL,
    reason="requires DEERFLOW_TEST_POSTGRES_URL for the independent-session race gate",
)
async def test_postgres_two_independent_sessions_race_to_one_run_row() -> None:
    assert _POSTGRES_URL is not None
    engine = create_async_engine(postgres_async_url(_POSTGRES_URL))
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    repository_a = RunRepository(session_factory)
    repository_b = RunRepository(session_factory)
    unique = uuid.uuid4().hex
    thread_id = f"idempotency-thread-{unique}"
    scope = scope_for_http("user", f"owner-{unique}")
    key = normalize_external_key(f"request-{unique}")
    digest = canonical_request_digest({"input": "race"})
    common = {
        "thread_id": thread_id,
        "owner_worker_id": "worker-a",
        "lease_expires_at": None,
        "external_scope": scope,
        "external_key": key,
        "request_digest": digest,
        "request_digest_version": REQUEST_DIGEST_VERSION,
        **_caller_intent_fields({"input": "race"}),
        "user_id": f"owner-{unique}",
    }
    try:
        left, right = await asyncio.gather(
            repository_a.ensure_run_atomic(f"run-a-{unique}", **common),
            repository_b.ensure_run_atomic(
                f"run-b-{unique}",
                **{**common, "owner_worker_id": "worker-b"},
            ),
        )
        assert {left.outcome, right.outcome} == {
            AdmissionOutcome.created,
            AdmissionOutcome.known_same,
        }
        assert left.row["run_id"] == right.row["run_id"]
    finally:
        async with session_factory() as session:
            await session.execute(delete(RunRow).where(RunRow.thread_id == thread_id))
            await session.commit()
        await engine.dispose()


@pytest.mark.anyio
@pytest.mark.postgres_contract
@pytest.mark.skipif(
    not _POSTGRES_URL,
    reason="requires DEERFLOW_TEST_POSTGRES_URL for the independent-session race gate",
)
async def test_postgres_concurrent_unequal_caller_intents_have_one_winner_and_one_conflict() -> None:
    assert _POSTGRES_URL is not None
    engine = create_async_engine(postgres_async_url(_POSTGRES_URL))
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    unique = uuid.uuid4().hex
    thread_id = f"unequal-intent-thread-{unique}"
    common = {
        "thread_id": thread_id,
        "lease_expires_at": None,
        "external_scope": scope_for_http("user", f"owner-{unique}"),
        "external_key": normalize_external_key(f"request-{unique}"),
        "request_digest_version": REQUEST_DIGEST_VERSION,
        "user_id": f"owner-{unique}",
    }
    left_intent = _caller_intent_fields({"input": "left"})
    right_intent = _caller_intent_fields({"input": "right"})
    try:
        left, right = await asyncio.gather(
            RunRepository(session_factory).ensure_run_atomic(
                f"run-left-{unique}",
                owner_worker_id="worker-left",
                request_digest=canonical_request_digest({"effective": "left"}),
                **left_intent,
                **common,
            ),
            RunRepository(session_factory).ensure_run_atomic(
                f"run-right-{unique}",
                owner_worker_id="worker-right",
                request_digest=canonical_request_digest({"effective": "right"}),
                **right_intent,
                **common,
            ),
        )
        assert {left.outcome, right.outcome} == {
            AdmissionOutcome.created,
            AdmissionOutcome.key_conflict,
        }
        assert left.row["run_id"] == right.row["run_id"]
    finally:
        async with session_factory() as session:
            await session.execute(delete(RunRow).where(RunRow.thread_id == thread_id))
            await session.commit()
        await engine.dispose()


@pytest.mark.anyio
@pytest.mark.parametrize("supersession", ["interrupt", "rollback"])
async def test_different_key_retains_thread_busy_and_supersession_semantics(
    supersession: str,
) -> None:
    store = MemoryRunStore()
    common = {
        "thread_id": "thread-busy",
        "owner_worker_id": "worker-1",
        "lease_expires_at": None,
        "request_digest": canonical_request_digest({"input": "same"}),
        "request_digest_version": REQUEST_DIGEST_VERSION,
        **_caller_intent_fields({"input": "same"}),
        "user_id": "owner-1",
    }
    await store.ensure_run_atomic(
        "run-1",
        external_scope=scope_for_http("user", "owner-1"),
        external_key=normalize_external_key("key-1"),
        **common,
    )

    with pytest.raises(ConflictError, match="active run"):
        await store.ensure_run_atomic(
            "run-2",
            external_scope=scope_for_http("user", "owner-1"),
            external_key=normalize_external_key("key-2"),
            **common,
        )

    replaced = await store.ensure_run_atomic(
        "run-3",
        external_scope=scope_for_http("user", "owner-1"),
        external_key=normalize_external_key("key-3"),
        multitask_strategy=supersession,
        **common,
    )
    assert replaced.outcome is AdmissionOutcome.created
    assert [row["run_id"] for row in replaced.claimed] == ["run-1"]


@pytest.mark.anyio
@pytest.mark.parametrize("strategy", ["reject", "interrupt", "rollback"])
async def test_different_key_thread_races_keep_current_multitask_semantics(
    strategy: str,
) -> None:
    store = MemoryRunStore()
    common = {
        "thread_id": f"thread-race-{strategy}",
        "owner_worker_id": "worker-1",
        "lease_expires_at": None,
        "request_digest": canonical_request_digest({"input": "same"}),
        "request_digest_version": REQUEST_DIGEST_VERSION,
        **_caller_intent_fields({"input": "same"}),
        "user_id": "owner-1",
        "multitask_strategy": strategy,
    }

    async def admit(suffix: str):
        return await store.ensure_run_atomic(
            f"run-{suffix}",
            external_scope=scope_for_http("user", "owner-1"),
            external_key=normalize_external_key(f"key-{suffix}"),
            **common,
        )

    left, right = await asyncio.gather(
        admit("left"),
        admit("right"),
        return_exceptions=True,
    )
    if strategy == "reject":
        assert sum(isinstance(result, ConflictError) for result in (left, right)) == 1
        assert sum(getattr(result, "outcome", None) is AdmissionOutcome.created for result in (left, right)) == 1
    else:
        assert all(getattr(result, "outcome", None) is AdmissionOutcome.created for result in (left, right))
        active = await store.list_inflight()
        assert len(active) == 1


def _runtime_record(run_id: str = "run-existing") -> RunRecord:
    now = datetime.now(UTC).isoformat()
    return RunRecord(
        run_id=run_id,
        thread_id="thread-runtime",
        assistant_id="lead_agent",
        status=RunStatus.pending,
        on_disconnect=DisconnectMode.cancel,
        created_at=now,
        updated_at=now,
    )


class _KeyedNormalizer:
    def __init__(self) -> None:
        self.normalize_calls = 0
        self.replay_calls = 0
        self.worker_calls = 0

    @contextmanager
    def scope(self, _intent):
        yield

    async def identify(self, intent):
        return InternalAdmissionIdentity(
            external_scope=scope_for_http("user", "owner-1"),
            external_key=normalize_external_key("request-1"),
            principal_digest="a" * 64,
            base_origin_digest="b" * 64,
            thread_id=intent.thread_id,
            requested_agent_id="default",
        )

    async def validate_replay(self, _intent, _identity, _record):
        self.replay_calls += 1

    async def normalize(self, intent):
        self.normalize_calls += 1

        async def worker(_record):
            self.worker_calls += 1

        return PreparedLaunch(
            thread_id=intent.thread_id,
            assistant_id="lead_agent",
            on_disconnect=DisconnectMode.cancel,
            metadata={},
            kwargs={},
            multitask_strategy="reject",
            model_name=None,
            user_id="owner-1",
            worker=worker,
            external_scope=scope_for_http("user", "owner-1"),
            external_key=normalize_external_key("request-1"),
            request_digest=canonical_request_digest({"input": "hello"}),
            request_digest_version=REQUEST_DIGEST_VERSION,
        )


class _KeyedRuns:
    def __init__(self, *, existing: RunRecord | None, admission: DurableAdmission | None = None) -> None:
        self.existing = existing
        self.admission = admission

    async def find_by_external_identity(self, _identity):
        return self.existing

    @asynccontextmanager
    async def admission_scope(self, _thread_id):
        yield

    async def prepare_admission(self, _launch):
        return None

    async def admit(self, _launch, *, candidate_run_id):
        assert self.admission is not None
        return self.admission

    async def attach_worker(self, record, worker, task_factory):
        record.task = task_factory(worker)
        return record.task

    async def fail_start(self, *_args):
        raise AssertionError("worker attachment must not fail")

    async def observe(self, run_id, _principal):
        if self.admission is not None and self.admission.record.run_id == run_id:
            return self.admission.record
        return self.existing if self.existing is not None and self.existing.run_id == run_id else None


class _TwoCallerPreflightBarrierStore(MemoryRunStore):
    """Force two independent managers past the optimistic lookup together."""

    def __init__(self) -> None:
        super().__init__()
        self._preflight_count = 0
        self._preflight_ready = asyncio.Event()

    async def get_by_external_identity(self, external_scope: str, external_key: str):
        self._preflight_count += 1
        if self._preflight_count <= 2:
            if self._preflight_count == 2:
                self._preflight_ready.set()
            await self._preflight_ready.wait()
        return await super().get_by_external_identity(external_scope, external_key)


class _LookupFailureRuns:
    async def find_by_external_identity(self, _identity):
        raise RuntimeError("lookup failed")


@pytest.mark.anyio
async def test_known_invocation_skips_normalization_and_worker_attachment() -> None:
    normalizer = _KeyedNormalizer()
    existing = _runtime_record()

    class RejectNewAdmission:
        calls = 0

        async def ready_for_admission(self) -> bool:
            self.calls += 1
            return False

    fence = RejectNewAdmission()
    runtime = InvocationRuntime(
        normalizer=normalizer,
        runs=_KeyedRuns(existing=existing),
        admission_fence=fence,
    )

    receipt = await runtime.launch(InternalLaunchIntent(thread_id="thread-runtime"))

    assert receipt.record is existing
    assert receipt.created is False
    assert normalizer.replay_calls == 1
    assert normalizer.normalize_calls == 0
    assert normalizer.worker_calls == 0
    assert existing.task is None
    assert fence.calls == 0, "an accepted replay must not be silently re-authorized"


@pytest.mark.anyio
async def test_race_loser_known_admission_never_attaches_worker() -> None:
    normalizer = _KeyedNormalizer()
    existing = _runtime_record()
    # The durable store cannot account for a server-generated stateless thread
    # until it returns the winning row, so the race loser may initially look
    # like a digest conflict. The runtime must re-project against that row.
    admission = DurableAdmission(record=existing, outcome=AdmissionOutcome.key_conflict)
    runtime = InvocationRuntime(normalizer=normalizer, runs=_KeyedRuns(existing=None, admission=admission))

    receipt = await runtime.launch(InternalLaunchIntent(thread_id="thread-runtime"))

    assert receipt.record is existing
    assert receipt.created is False
    assert normalizer.normalize_calls == 1
    assert normalizer.replay_calls == 1
    assert normalizer.worker_calls == 0
    assert existing.task is None


@pytest.mark.anyio
@pytest.mark.parametrize("terminal", [RunStatus.success, RunStatus.error, RunStatus.timeout, RunStatus.interrupted])
async def test_run_manager_replays_same_row_active_and_after_every_terminal_status(terminal: RunStatus) -> None:
    manager = RunManager(store=MemoryRunStore())
    common = {
        "external_scope": scope_for_http("user", "owner-1"),
        "external_key": normalize_external_key("request-1"),
        "request_digest": canonical_request_digest({"input": "hello"}),
        "request_digest_version": REQUEST_DIGEST_VERSION,
        **_caller_intent_fields({"input": "hello"}),
        "user_id": "owner-1",
    }

    created = await manager.ensure_or_reject("thread-manager", **common)
    active_replay = await manager.ensure_or_reject("thread-manager", **common)
    assert created.outcome is AdmissionOutcome.created
    assert active_replay.outcome is AdmissionOutcome.known_same
    assert active_replay.record is created.record

    await manager.set_status(created.record.run_id, terminal)
    terminal_replay = await manager.ensure_or_reject("thread-manager", **common)
    assert terminal_replay.outcome is AdmissionOutcome.known_same
    assert terminal_replay.record.run_id == created.record.run_id
    assert terminal_replay.record.status is terminal


@pytest.mark.anyio
async def test_run_manager_distinguishes_key_conflict_from_thread_busy() -> None:
    manager = RunManager(store=MemoryRunStore())
    scope = scope_for_http("user", "owner-1")
    created = await manager.ensure_or_reject(
        "thread-manager",
        external_scope=scope,
        external_key=normalize_external_key("key-1"),
        request_digest=canonical_request_digest({"input": "hello"}),
        request_digest_version=REQUEST_DIGEST_VERSION,
        **_caller_intent_fields({"input": "hello"}),
        user_id="owner-1",
    )

    conflict = await manager.ensure_or_reject(
        "thread-manager",
        external_scope=scope,
        external_key=normalize_external_key("key-1"),
        request_digest=canonical_request_digest({"input": "changed"}),
        request_digest_version=REQUEST_DIGEST_VERSION,
        **_caller_intent_fields({"input": "changed"}),
        user_id="owner-1",
    )
    assert conflict.outcome is AdmissionOutcome.key_conflict

    with pytest.raises(ConflictError) as exc_info:
        await manager.ensure_or_reject(
            "thread-manager",
            external_scope=scope,
            external_key=normalize_external_key("key-2"),
            request_digest=canonical_request_digest({"input": "hello"}),
            request_digest_version=REQUEST_DIGEST_VERSION,
            **_caller_intent_fields({"input": "hello"}),
            user_id="owner-1",
        )
    assert not isinstance(exc_info.value, IdempotencyConflictError)
    assert exc_info.value.active_run_id == created.record.run_id


@pytest.mark.anyio
async def test_http_identity_uses_authenticated_subject_and_auth_disabled_default() -> None:
    session_request = SimpleNamespace(
        state=SimpleNamespace(
            user=SimpleNamespace(id="owner-1", system_role="user", oauth_provider=None, oauth_id=None),
            auth_source=AUTH_SOURCE_SESSION,
        ),
        app=SimpleNamespace(state=SimpleNamespace(tenant_identity=_TEST_TENANT_IDENTITY)),
    )
    session_identity = await _GatewayLaunchNormalizer(session_request).identify(InternalLaunchIntent(thread_id="thread-1", external_key="request-1"))
    assert session_identity is not None
    assert session_identity.external_scope == tenant_admission_scope(
        _TEST_TENANT_IDENTITY.to_persisted_reference(),
        scope_for_http("user", "owner-1"),
    )

    disabled_request = SimpleNamespace(
        state=SimpleNamespace(user=None, auth_source=AUTH_SOURCE_AUTH_DISABLED),
        app=SimpleNamespace(state=SimpleNamespace(tenant_identity=_TEST_TENANT_IDENTITY)),
    )
    disabled_identity = await _GatewayLaunchNormalizer(disabled_request).identify(InternalLaunchIntent(thread_id="thread-1", external_key="request-1"))
    assert disabled_identity is not None
    assert disabled_identity.external_scope == tenant_admission_scope(
        _TEST_TENANT_IDENTITY.to_persisted_reference(),
        scope_for_http("default-user", AUTH_DISABLED_USER_ID),
    )


@pytest.mark.anyio
async def test_admission_scope_includes_the_server_owned_tenant() -> None:
    async def identify(tenant_id: str):
        tenant = TenantIdentityV1.from_canonical_id(tenant_id)
        request = SimpleNamespace(
            state=SimpleNamespace(
                user=SimpleNamespace(
                    id="owner-1",
                    system_role="user",
                    oauth_provider=None,
                    oauth_id=None,
                ),
                auth_source=AUTH_SOURCE_SESSION,
            ),
            app=SimpleNamespace(
                state=SimpleNamespace(tenant_identity=tenant),
            ),
        )
        return await _GatewayLaunchNormalizer(request).identify(
            InternalLaunchIntent(
                thread_id="shared-thread",
                external_key="shared-request",
            )
        )

    tenant_a = await identify("tenant-a")
    tenant_b = await identify("tenant-b")

    assert tenant_a is not None
    assert tenant_b is not None
    assert tenant_a.external_scope == tenant_admission_scope(
        TenantIdentityV1.from_canonical_id("tenant-a").to_persisted_reference(),
        scope_for_http("user", "owner-1"),
    )
    assert tenant_a.external_scope != tenant_b.external_scope


@pytest.mark.anyio
async def test_internal_source_mappings_use_only_trusted_scope_facts(monkeypatch) -> None:
    async def owner_user(*_args, **_kwargs):
        return SimpleNamespace(id="owner-1", system_role="user", oauth_provider=None, oauth_id=None)

    monkeypatch.setattr(
        "app.gateway.services.resolve_trusted_internal_owner_for_attribution",
        owner_user,
    )
    request = SimpleNamespace(
        state=SimpleNamespace(
            user=SimpleNamespace(id="internal", system_role="internal"),
            auth_source=AUTH_SOURCE_INTERNAL,
        ),
        app=SimpleNamespace(state=SimpleNamespace(tenant_identity=_TEST_TENANT_IDENTITY)),
    )
    normalizer = _GatewayLaunchNormalizer(request, trust_internal_launch_facts=True)
    channel = await normalizer.identify(
        InternalLaunchIntent(
            thread_id="thread-channel",
            assistant_id="lead_agent",
            context={"channel_user_id": "sender-1", "channel_name": "slack", "agent_name": None},
            source_kind=InternalSourceKind.native_channel,
            owner_user_id="owner-1",
            external_key="event-1",
            native_channel=InternalNativeChannelFacts(
                provider="slack",
                connection_id="connection-1",
                workspace_id="workspace-1",
                chat_id="chat-1",
                topic_id=None,
                provider_message_id="event-1",
                channel_user_id="sender-1",
                resolved_assistant_id="lead_agent",
                resolved_agent_name=None,
                verified_binding=InternalVerifiedNativeBinding(
                    kind=InternalVerifiedNativeBindingKind.connection,
                    reference="connection-1",
                ),
            ),
        )
    )
    assert channel is not None
    assert channel.external_scope == tenant_admission_scope(
        _TEST_TENANT_IDENTITY.to_persisted_reference(),
        scope_for_channel("slack", "connection-1", "workspace-1", "chat-1"),
    )

    scheduled = await normalizer.identify(
        InternalLaunchIntent(
            thread_id="thread-task",
            source_kind=InternalSourceKind.scheduled_task,
            trusted_task_id="task-1",
            task_run_id="task-run-1",
            scheduled_trigger="scheduled",
            owner_user_id="owner-1",
            external_key="task-run-1",
        )
    )
    assert scheduled is not None
    assert scheduled.external_scope == tenant_admission_scope(
        _TEST_TENANT_IDENTITY.to_persisted_reference(),
        scope_for_scheduler("owner-1", "task-1"),
    )


@pytest.mark.anyio
async def test_system_task_scope_requires_explicit_system_ownership() -> None:
    request = SimpleNamespace(
        state=SimpleNamespace(
            user=SimpleNamespace(id="internal", system_role="internal"),
            auth_source=AUTH_SOURCE_INTERNAL,
        ),
        app=SimpleNamespace(state=SimpleNamespace(tenant_identity=_TEST_TENANT_IDENTITY)),
    )
    normalizer = _GatewayLaunchNormalizer(request, trust_internal_launch_facts=True)
    common = dict(
        thread_id="thread-task",
        source_kind=InternalSourceKind.scheduled_task,
        trusted_task_id="task-1",
        task_run_id="task-run-1",
        scheduled_trigger="scheduled",
        external_key="task-run-1",
    )
    with pytest.raises(ValueError, match="persisted owner"):
        await normalizer.identify(InternalLaunchIntent(**common))

    identity = await normalizer.identify(InternalLaunchIntent(**common, scheduled_system_owned=True))
    assert identity is not None
    assert identity.external_scope == tenant_admission_scope(
        _TEST_TENANT_IDENTITY.to_persisted_reference(),
        scope_for_scheduler("__deerflow_system__", "task-1"),
    )


@pytest.mark.anyio
@pytest.mark.parametrize(
    "request_body",
    [
        RunCreateRequest(metadata={"arbitrary": "value"}),
        RunCreateRequest(config={"unknown": True}),
        RunCreateRequest(config={"configurable": {"unknown": True}}),
        RunCreateRequest(config={"context": {"unknown": True}}),
        RunCreateRequest(context={"unknown": True}),
        RunCreateRequest(config={"configurable": {"non_interactive": True}}),
        RunCreateRequest(config={"context": {"non_interactive": True}}),
        RunCreateRequest(config={"context": {"disable_clarification": True}}),
        RunCreateRequest(context={"non_interactive": True}),
        RunCreateRequest(context={"disable_clarification": True}),
        RunCreateRequest(command={"goto": "agent"}),
    ],
)
async def test_http_key_rejects_every_unclassified_request_container(request_body: RunCreateRequest) -> None:
    request = SimpleNamespace(
        headers={"Idempotency-Key": "request-1"},
        state=SimpleNamespace(
            auth_source=AUTH_SOURCE_SESSION,
            user=SimpleNamespace(id="owner-1", system_role="user", oauth_provider=None, oauth_id=None),
        ),
        app=SimpleNamespace(
            state=SimpleNamespace(
                runtime_readiness=_ReadyAdmissionFence(),
                tenant_identity=_TEST_TENANT_IDENTITY,
            ),
        ),
    )
    request_body = request_body.model_copy(
        update={"input": {"messages": [{"role": "user", "content": "hello"}]}},
    )
    with pytest.raises(Exception) as exc_info:
        await start_run(request_body, "thread-1", request)
    assert getattr(exc_info.value, "status_code", None) == 422


@pytest.mark.anyio
async def test_empty_http_idempotency_key_is_422_before_admission() -> None:
    request = SimpleNamespace(
        headers={"Idempotency-Key": ""},
        state=SimpleNamespace(),
        app=SimpleNamespace(
            state=SimpleNamespace(
                runtime_readiness=_ReadyAdmissionFence(),
                tenant_identity=_TEST_TENANT_IDENTITY,
            ),
        ),
    )
    with pytest.raises(Exception) as exc_info:
        await start_run(RunCreateRequest(), "thread-1", request)
    assert getattr(exc_info.value, "status_code", None) == 422


@pytest.mark.anyio
async def test_failed_normalization_discards_cached_admission_identity() -> None:
    request = SimpleNamespace(
        headers={"Idempotency-Key": "request-1"},
        state=SimpleNamespace(
            auth_source=AUTH_SOURCE_SESSION,
            user=SimpleNamespace(id="owner-1", system_role="user", oauth_provider=None, oauth_id=None),
        ),
        app=SimpleNamespace(state=SimpleNamespace(tenant_identity=_TEST_TENANT_IDENTITY)),
    )
    normalizer = _GatewayLaunchNormalizer(request)
    intent = InternalLaunchIntent(thread_id="thread-1", external_key="request-1")
    assert await normalizer.identify(intent) is not None

    with pytest.raises(Exception):
        await normalizer.normalize(intent)

    assert normalizer._identified == {}


@pytest.mark.anyio
async def test_failed_preflight_lookup_discards_cached_admission_identity() -> None:
    request = SimpleNamespace(
        state=SimpleNamespace(
            auth_source=AUTH_SOURCE_SESSION,
            user=SimpleNamespace(id="owner-1", system_role="user", oauth_provider=None, oauth_id=None),
        ),
        app=SimpleNamespace(state=SimpleNamespace(tenant_identity=_TEST_TENANT_IDENTITY)),
    )
    normalizer = _GatewayLaunchNormalizer(request)
    runtime = InvocationRuntime(normalizer=normalizer, runs=_LookupFailureRuns())

    with pytest.raises(RuntimeError, match="lookup failed"):
        await runtime.launch(InternalLaunchIntent(thread_id="thread-1", external_key="request-1"))

    assert normalizer._identified == {}


@pytest.mark.anyio
async def test_http_replay_conflicts_when_retry_omits_original_thinking_option() -> None:
    from langgraph.checkpoint.memory import InMemorySaver
    from langgraph.store.memory import InMemoryStore

    store = InMemoryStore()
    run_manager = RunManager(
        store=MemoryRunStore(),
        tenant=_TEST_TENANT,
    )
    request = SimpleNamespace(
        headers={"Idempotency-Key": "request-removes-thinking"},
        state=SimpleNamespace(
            auth_source=AUTH_SOURCE_SESSION,
            user=SimpleNamespace(id="owner-1", system_role="user", oauth_provider=None, oauth_id=None),
        ),
        app=SimpleNamespace(
            state=SimpleNamespace(
                runtime_readiness=_ReadyAdmissionFence(),
                stream_bridge=SimpleNamespace(),
                run_manager=run_manager,
                checkpointer=InMemorySaver(),
                store=store,
                run_event_store=MemoryRunEventStore(),
                run_events_config=None,
                thread_store=MemoryThreadMetaStore(store),
                tenant_identity=_TEST_TENANT_IDENTITY,
            )
        ),
    )
    revision = ResolvedAgentRevision.from_material(
        ResolvedAgentMaterialV1(
            agent_id="default",
            storage_source="builtin",
            storage_version="v1",
            agent_config=None,
            soul="test",
            model_profile={},
        )
    )
    set_app_config(AppConfig.model_validate({"sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"}}))
    try:
        with (
            patch("app.gateway.services.resolve_agent_revision", return_value=revision),
            patch("app.gateway.services.resolve_agent_factory", return_value=object()),
            patch("app.gateway.services.run_agent", new_callable=AsyncMock),
        ):
            original = RunCreateRequest(
                input={"messages": [{"role": "user", "content": "hello"}]},
                context={"thinking_enabled": False},
            )
            first = await start_run(original, "thread-removes-thinking", request)
            assert first.task is not None
            await first.task

            retry_without_option = RunCreateRequest(input=original.input)
            with pytest.raises(Exception) as conflict:
                await start_run(retry_without_option, "thread-removes-thinking", request)

            assert getattr(conflict.value, "status_code", None) == 409
    finally:
        reset_app_config()


@pytest.mark.anyio
async def test_http_replay_returns_one_run_and_attaches_exactly_one_worker() -> None:
    from langgraph.checkpoint.memory import InMemorySaver
    from langgraph.store.memory import InMemoryStore

    store = InMemoryStore()
    run_store = MemoryRunStore()
    run_manager = RunManager(store=run_store, tenant=_TEST_TENANT)
    request = SimpleNamespace(
        headers={"Idempotency-Key": "request-1"},
        state=SimpleNamespace(
            auth_source=AUTH_SOURCE_SESSION,
            credential_evidence=build_boundary_credential_evidence(
                auth_source=AUTH_SOURCE_SESSION,
                permissions=_ALL_PERMISSIONS,
            ),
            user=SimpleNamespace(id="owner-1", system_role="user", oauth_provider=None, oauth_id=None),
        ),
        app=SimpleNamespace(
            state=SimpleNamespace(
                runtime_readiness=_ReadyAdmissionFence(),
                stream_bridge=SimpleNamespace(),
                run_manager=run_manager,
                checkpointer=InMemorySaver(),
                store=store,
                run_event_store=MemoryRunEventStore(),
                run_events_config=None,
                thread_store=MemoryThreadMetaStore(store),
                tenant_identity=_TEST_TENANT_IDENTITY,
                credential_audit_repo=InMemoryCredentialAuditRepository(
                    tenant=_TEST_TENANT,
                ),
            )
        ),
    )
    revision = ResolvedAgentRevision.from_material(
        ResolvedAgentMaterialV1(
            agent_id="default",
            storage_source="builtin",
            storage_version="v1",
            agent_config=None,
            soul="test",
            model_profile={},
        )
    )
    set_app_config(AppConfig.model_validate({"sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"}}))
    try:
        with (
            patch("app.gateway.services.resolve_agent_revision", return_value=revision) as resolve_revision,
            patch("app.gateway.services.resolve_agent_factory", return_value=object()),
            patch("app.gateway.services.run_agent", new_callable=AsyncMock) as run_agent,
        ):
            body = RunCreateRequest(input={"messages": [{"role": "user", "content": "hello"}]})
            first = await start_run(body, "thread-1", request)
            assert first.task is not None
            await first.task
            request.app.state.extensions = SimpleNamespace(generation=999)
            request.app.state.contributor_host = SimpleNamespace(
                contribute_origin=AsyncMock(side_effect=AssertionError("contributors reran")),
                contribute_run_context=AsyncMock(side_effect=AssertionError("contributors reran")),
            )
            replay = await start_run(
                body.model_copy(update={"stream_mode": "values", "on_disconnect": "continue"}),
                "thread-1",
                request,
            )

            assert replay is first
            assert run_agent.await_count == 1
            assert resolve_revision.call_count == 1
            rows = await run_store.list_by_thread("thread-1", user_id="owner-1")
            assert [row["run_id"] for row in rows] == [first.run_id]
            projection = first.kwargs["__accepted_request_projection_v1"]
            assert projection["runtime_identity_digest"] == first.accepted_invocation.runtime_identity_digest
            assert projection["contributor_execution_digest"] == first.accepted_invocation.contributor_execution_digest
            from app.gateway.routers.thread_runs import _record_to_response

            assert "__accepted_request_projection_v1" not in _record_to_response(first).kwargs

            with pytest.raises(Exception) as explicit_thread_removed:
                await start_run(
                    body,
                    "newly-generated-thread-id",
                    request,
                    thread_id_explicit=False,
                )
            assert getattr(explicit_thread_removed.value, "status_code", None) == 409

            with pytest.raises(Exception) as exc_info:
                await start_run(
                    body.model_copy(update={"input": {"messages": [{"role": "user", "content": "changed"}]}}),
                    "thread-1",
                    request,
                )
            assert getattr(exc_info.value, "status_code", None) == 409

            conflicting_requests = [
                (body.model_copy(update={"context": {"thinking_enabled": False}}), "thread-1"),
                (body.model_copy(update={"multitask_strategy": "interrupt"}), "thread-1"),
                (body.model_copy(update={"interrupt_before": ["agent"]}), "thread-1"),
                (body.model_copy(update={"checkpoint_id": "different-checkpoint"}), "thread-1"),
                (body.model_copy(update={"assistant_id": "different-agent"}), "thread-1"),
                (body, "different-thread"),
            ]
            for conflicting_body, conflicting_thread in conflicting_requests:
                with pytest.raises(Exception) as conflict:
                    await start_run(
                        conflicting_body,
                        conflicting_thread,
                        request,
                    )
                assert getattr(conflict.value, "status_code", None) == 409
    finally:
        reset_app_config()


@pytest.mark.anyio
async def test_simultaneous_stateless_http_retries_converge_after_thread_binding() -> None:
    from langgraph.checkpoint.memory import InMemorySaver
    from langgraph.store.memory import InMemoryStore

    run_store = _TwoCallerPreflightBarrierStore()
    graph_store = InMemoryStore()

    def request_for(manager: RunManager):
        return SimpleNamespace(
            headers={"Idempotency-Key": "simultaneous-request"},
            state=SimpleNamespace(
                auth_source=AUTH_SOURCE_SESSION,
                user=SimpleNamespace(id="owner-1", system_role="user", oauth_provider=None, oauth_id=None),
            ),
            app=SimpleNamespace(
                state=SimpleNamespace(
                    runtime_readiness=_ReadyAdmissionFence(),
                    stream_bridge=SimpleNamespace(),
                    run_manager=manager,
                    checkpointer=InMemorySaver(),
                    store=graph_store,
                    run_event_store=MemoryRunEventStore(),
                    run_events_config=None,
                    thread_store=MemoryThreadMetaStore(graph_store),
                    tenant_identity=_TEST_TENANT_IDENTITY,
                )
            ),
        )

    revision = ResolvedAgentRevision.from_material(
        ResolvedAgentMaterialV1(
            agent_id="default",
            storage_source="builtin",
            storage_version="v1",
            agent_config=None,
            soul="test",
            model_profile={},
        )
    )
    set_app_config(AppConfig.model_validate({"sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"}}))
    try:
        with (
            patch("app.gateway.services.resolve_agent_revision", return_value=revision),
            patch("app.gateway.services.resolve_agent_factory", return_value=object()),
            patch("app.gateway.services.run_agent", new_callable=AsyncMock) as run_agent,
        ):
            body = RunCreateRequest(input={"messages": [{"role": "user", "content": "hello"}]})
            left, right = await asyncio.gather(
                start_run(
                    body,
                    "generated-thread-left",
                    request_for(
                        RunManager(store=run_store, tenant=_TEST_TENANT),
                    ),
                    thread_id_explicit=False,
                ),
                start_run(
                    body,
                    "generated-thread-right",
                    request_for(
                        RunManager(store=run_store, tenant=_TEST_TENANT),
                    ),
                    thread_id_explicit=False,
                ),
            )

            assert left.run_id == right.run_id
            attached = [record for record in (left, right) if record.task is not None]
            assert len(attached) == 1
            await attached[0].task
            assert run_agent.await_count == 1
    finally:
        reset_app_config()


@pytest.mark.anyio
async def test_channel_redelivery_bypasses_ttl_and_converges_in_sql_store(
    tmp_path,
    monkeypatch,
) -> None:
    from langgraph.checkpoint.memory import InMemorySaver
    from langgraph.store.memory import InMemoryStore

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'channel-runs.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    run_store = RunRepository(async_sessionmaker(engine, expire_on_commit=False))
    run_manager = RunManager(store=run_store, tenant=_TEST_TENANT)
    store = InMemoryStore()
    app = SimpleNamespace(
        state=SimpleNamespace(
            runtime_readiness=_ReadyAdmissionFence(),
            stream_bridge=SimpleNamespace(),
            run_manager=run_manager,
            checkpointer=InMemorySaver(),
            store=store,
            run_event_store=MemoryRunEventStore(),
            run_events_config=None,
            thread_store=MemoryThreadMetaStore(store),
            tenant_identity=_TEST_TENANT_IDENTITY,
        )
    )

    async def owner_user(*_args, **_kwargs):
        return SimpleNamespace(id="owner-1", system_role="user", oauth_provider=None, oauth_id=None)

    monkeypatch.setattr(
        "app.gateway.services.resolve_trusted_internal_owner_for_attribution",
        owner_user,
    )
    revision = ResolvedAgentRevision.from_material(
        ResolvedAgentMaterialV1(
            agent_id="default",
            storage_source="builtin",
            storage_version="v1",
            agent_config=None,
            soul="test",
            model_profile={},
        )
    )
    set_app_config(AppConfig.model_validate({"sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"}}))
    facts = InternalNativeChannelFacts(
        provider="slack",
        connection_id="connection-1",
        workspace_id="workspace-1",
        chat_id="chat-1",
        topic_id="topic-1",
        provider_message_id="event-1",
        channel_user_id="sender-1",
        resolved_assistant_id="lead_agent",
        resolved_agent_name=None,
        verified_binding=InternalVerifiedNativeBinding(
            kind=InternalVerifiedNativeBindingKind.connection,
            reference="connection-1",
        ),
    )

    def intent(
        *,
        channel_facts: InternalNativeChannelFacts = facts,
        temporary_url: str = "https://temporary.invalid/one",
    ) -> InternalLaunchIntent:
        return InternalLaunchIntent(
            thread_id="thread-channel",
            assistant_id="lead_agent",
            input={
                "messages": [
                    {
                        "role": "user",
                        "content": "inspect file",
                        "additional_kwargs": {
                            "files": [
                                {
                                    "filename": "report.pdf",
                                    "size": 12,
                                    "mime_type": "application/pdf",
                                    "url": temporary_url,
                                }
                            ]
                        },
                    }
                ]
            },
            config={"configurable": {"thread_id": "thread-channel", "checkpoint_ns": ""}},
            context={
                "channel_name": "slack",
                "channel_user_id": channel_facts.channel_user_id,
                "user_id": "owner-1",
                "agent_name": None,
            },
            source_kind=InternalSourceKind.native_channel,
            owner_user_id="owner-1",
            native_channel=channel_facts,
            external_key="event-1",
        )

    try:
        with (
            patch("app.gateway.services.resolve_agent_revision", return_value=revision),
            patch("app.gateway.services.resolve_agent_factory", return_value=object()),
            patch("app.gateway.services.run_agent", new_callable=AsyncMock) as run_agent,
        ):
            runtime = build_channel_invocation_runtime(app)
            first = await runtime.launch(intent())
            assert first.record.task is not None
            await first.record.task
            replay = await runtime.launch(intent(temporary_url="https://temporary.invalid/refreshed"))
            assert replay.record.run_id == first.record.run_id
            assert replay.created is False
            assert run_agent.await_count == 1

            contradictory = InternalNativeChannelFacts(
                **{
                    **facts.__dict__,
                    "channel_user_id": "forged-sender",
                }
            )
            with pytest.raises(IdempotencyConflictError, match="authenticated .* evidence"):
                await runtime.launch(intent(channel_facts=contradictory))
    finally:
        reset_app_config()
        await engine.dispose()
