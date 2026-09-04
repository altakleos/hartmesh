import hashlib
import inspect
from datetime import UTC, datetime, timedelta

import pytest
import pytest_asyncio
from deerflow_extension_api import (
    EffectiveSubjectV1,
    InvocationIdentityV1,
)
from sqlalchemy import event, null, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from deerflow.config.database_config import DatabaseConfig
from deerflow.mcp.tasks.lineage import (
    McpTaskLineageBinder,
    TrustedMcpSubmissionContext,
)
from deerflow.persistence.engine import close_engine, get_session_factory, init_engine_from_config
from deerflow.persistence.mcp_tasks import (
    DuplicateMcpRemoteTaskError,
    DuplicateMcpTaskIdError,
    McpTaskRepository,
    McpTaskRepositoryError,
)
from deerflow.persistence.mcp_tasks.model import McpTaskRow
from deerflow.runtime.tenant_identity import TenantIdentityV1
from deerflow.runtime.tool_evidence import build_request_projection


@pytest_asyncio.fixture(autouse=True)
async def _close_persistence_engine():
    yield
    await close_engine()


async def _make_repo(tmp_path) -> McpTaskRepository:
    await init_engine_from_config(DatabaseConfig(backend="sqlite", sqlite_dir=str(tmp_path)))
    session_factory = get_session_factory()
    assert session_factory is not None
    return McpTaskRepository(
        session_factory,
        tenant=TenantIdentityV1.from_canonical_id("test").to_persisted_reference(),
    )


def test_repository_operations_require_explicit_tenant_digest():
    operations = (
        "create",
        "get",
        "get_by_lineage_digest",
        "list_by_thread",
        "list_by_parent_run",
        "claim_due_tasks",
        "apply_snapshot",
        "release_claim",
        "request_cancel",
        "claim_cancel_requests",
        "apply_cancel_snapshot",
        "release_cancel_claim",
        "claim_notification_work",
        "mark_notification_dispatched",
        "release_notification_claim",
        "finish_notification_run",
        "release_notification_lease",
        "dead_letter_notification",
        "defer_dispatched_notification",
    )

    for operation in operations:
        parameter = inspect.signature(getattr(McpTaskRepository, operation)).parameters["tenant_digest"]
        assert parameter.default is inspect.Parameter.empty, operation


async def _create_working_task(
    repo: McpTaskRepository,
    *,
    task_id: str,
    now: datetime,
    user_id: str = "user-1",
    remote_task_id: str | None = None,
    lineage=None,
    tenant_digest: str | None = None,
) -> dict:
    resolved_remote_task_id = remote_task_id or f"remote-{task_id}"
    if lineage is None:
        lineage = McpTaskLineageBinder().for_standalone_api(
            tenant=repo.tenant,
            principal_identity=InvocationIdentityV1(
                effective_subject=EffectiveSubjectV1(
                    kind="human",
                    subject_id=user_id,
                    role="member",
                )
            ),
            extension_generation=1,
            extension_manifest_digest="a" * 64,
            accepted_origin_digest="b" * 64,
            server_name="reports",
            tool_name="submit_report",
            safe_request_projection=build_request_projection(
                "submit_report",
                {
                    "task_id": task_id,
                    "remote_task_id": resolved_remote_task_id,
                },
                evidence_safe_fields=frozenset({"task_id", "remote_task_id"}),
            ),
            credential_selector=None,
        )
    return await repo.create(
        task_id=task_id,
        user_id=user_id,
        thread_id="thread-1",
        lineage=lineage,
        tenant_digest=tenant_digest or repo.tenant.digest,
        driver_name="fake",
        remote_task_id=resolved_remote_task_id,
        task_name="Generate report",
        request_commitment_version=1,
        request_commitment_key_id="test-v1",
        request_commitment_digest=hashlib.sha256(f"commitment:{task_id}".encode()).hexdigest(),
        status="working",
        result=None,
        result_preview=None,
        result_truncated=False,
        result_artifact=None,
        error=None,
        input_required=None,
        next_poll_after_seconds=0,
        driver_data={"status_tool": "status"},
    )


def _agent_lineage(repo: McpTaskRepository, *, token: str):
    digest = hashlib.sha256(token.encode()).hexdigest()
    identity = InvocationIdentityV1(
        effective_subject=EffectiveSubjectV1(
            kind="human",
            subject_id="user-1",
            role="member",
        )
    )
    return McpTaskLineageBinder().for_agent_tool(
        trusted_runtime=TrustedMcpSubmissionContext(
            tenant=repo.tenant,
            principal_identity=identity,
            parent_run_id="run-parent",
            parent_execution_task_id="run-parent",
            parent_execution_kind="lead",
            parent_subagent_name=None,
            parent_tool_receipt_id=f"tr_{digest}",
            agent_revision_digest="a" * 64,
            assembly_fingerprint="b" * 64,
            subagent_catalog_digest="c" * 64,
            subagent_definition_digest=None,
            extension_generation=2,
            extension_manifest_digest="d" * 64,
            accepted_origin_digest="e" * 64,
            artifact_manifest_digest="sha256:" + "f" * 64,
            extension_configuration_digest="sha256:" + "0" * 64,
        ),
        server_name="reports",
        tool_name="submit_report",
        safe_request_projection=build_request_projection(
            "submit_report",
            {"token": token},
            evidence_safe_fields=frozenset({"token"}),
        ),
        credential_selector=None,
    )


@pytest.mark.asyncio
async def test_remote_task_id_is_unique_per_user_and_server(tmp_path):
    repo = await _make_repo(tmp_path)
    now = datetime.now(UTC)
    await _create_working_task(
        repo,
        task_id="task-remote-1",
        now=now,
        remote_task_id="shared-remote-id",
    )

    with pytest.raises(DuplicateMcpRemoteTaskError, match="already tracked"):
        await _create_working_task(
            repo,
            task_id="task-remote-2",
            now=now,
            remote_task_id="shared-remote-id",
        )

    other_user = await _create_working_task(
        repo,
        task_id="task-remote-3",
        now=now,
        user_id="user-2",
        remote_task_id="shared-remote-id",
    )
    assert other_user["remote_task_id"] == "shared-remote-id"


@pytest.mark.asyncio
async def test_duplicate_local_task_id_has_a_distinct_race_signal(tmp_path):
    repo = await _make_repo(tmp_path)
    now = datetime.now(UTC)
    await _create_working_task(repo, task_id="shared-local-id", now=now)

    with pytest.raises(DuplicateMcpTaskIdError):
        await _create_working_task(
            repo,
            task_id="shared-local-id",
            now=now,
            remote_task_id="different-remote-id",
        )


@pytest.mark.asyncio
async def test_task_and_lineage_insert_roll_back_together_on_commit_failure(tmp_path):
    repo = await _make_repo(tmp_path)

    def fail_target_insert(session, _flush_context, _instances):
        if any(isinstance(item, McpTaskRow) and item.id == "atomic-failure" for item in session.new):
            raise RuntimeError("injected commit failure")

    event.listen(Session, "before_flush", fail_target_insert)
    try:
        with pytest.raises(RuntimeError, match="injected commit failure"):
            await _create_working_task(
                repo,
                task_id="atomic-failure",
                now=datetime.now(UTC),
            )
    finally:
        event.remove(Session, "before_flush", fail_target_insert)

    async with repo._sf() as session:
        persisted = (await session.execute(select(McpTaskRow).where(McpTaskRow.id == "atomic-failure"))).scalar_one_or_none()
    assert persisted is None


@pytest.mark.asyncio
async def test_parent_lineage_query_is_bounded_scoped_and_cursor_checked(tmp_path):
    repo = await _make_repo(tmp_path)
    now = datetime.now(UTC)
    for index in range(3):
        await _create_working_task(
            repo,
            task_id=f"child-{index}",
            now=now,
            lineage=_agent_lineage(repo, token=f"child-{index}"),
        )

    first = await repo.list_by_parent_run(
        "run-parent",
        user_id="user-1",
        limit=2,
        tenant_digest=repo.tenant.digest,
    )
    assert len(first["items"]) == 2
    assert first["next_cursor"] is not None
    assert all(item["receipt_id"].startswith("tr_") for item in first["items"])
    assert all(item["server_name"] == "reports" for item in first["items"])
    assert all("request_commitment_version" not in item for item in first["items"])
    assert all("request_commitment_state" not in item for item in first["items"])
    assert all("remote_task_id" not in item for item in first["items"])
    assert all("request_commitment_digest" not in item for item in first["items"])
    assert all("request_commitment_key_id" not in item for item in first["items"])
    assert all("evidence_anchors" not in item for item in first["items"])

    evidence_page = await repo.list_by_parent_run(
        "run-parent",
        user_id="user-1",
        limit=3,
        tenant_digest=repo.tenant.digest,
        include_evidence_anchors=True,
    )
    assert len(evidence_page["items"]) == 3
    assert all(item["request_commitment_version"] == 1 and item["request_commitment_state"] == "present" for item in evidence_page["items"])
    assert all(
        item["evidence_anchors"]
        == {
            "lineage_version": 2,
            "lineage_kind": "agent_tool",
            "tenant_ref": repo.tenant.public_ref,
            "tenant_digest": repo.tenant.digest,
            "parent_run_id": "run-parent",
            "parent_execution_task_id": "run-parent",
            "parent_execution_kind": "lead",
            "parent_subagent_name": None,
            "agent_revision_digest": "a" * 64,
            "assembly_fingerprint": "b" * 64,
            "subagent_catalog_digest": "c" * 64,
            "subagent_definition_digest": None,
            "extension_generation": 2,
            "extension_manifest_digest": "d" * 64,
            "accepted_origin_digest": "e" * 64,
            "artifact_manifest_digest": "sha256:" + "f" * 64,
            "extension_configuration_digest": "sha256:" + "0" * 64,
        }
        for item in evidence_page["items"]
    )

    second = await repo.list_by_parent_run(
        "run-parent",
        user_id="user-1",
        limit=2,
        cursor=first["next_cursor"],
        tenant_digest=repo.tenant.digest,
    )
    assert len(second["items"]) == 1
    assert second["next_cursor"] is None
    assert {item["task_id"] for item in (*first["items"], *second["items"])} == {"child-0", "child-1", "child-2"}

    with pytest.raises(McpTaskRepositoryError) as exc_info:
        await repo.list_by_parent_run(
            "another-run",
            user_id="user-1",
            cursor=first["next_cursor"],
            tenant_digest=repo.tenant.digest,
        )
    assert exc_info.value.code == "mcp_task_cursor_invalid"


@pytest.mark.asyncio
async def test_repository_rejects_stale_tenant_before_any_mutation(tmp_path):
    repo = await _make_repo(tmp_path)
    other = TenantIdentityV1.from_canonical_id("other").to_persisted_reference()

    with pytest.raises(McpTaskRepositoryError) as create_exc:
        await _create_working_task(
            repo,
            task_id="stale-create",
            now=datetime.now(UTC),
            tenant_digest=other.digest,
        )
    assert create_exc.value.code == "mcp_task_tenant_mismatch"

    with pytest.raises(McpTaskRepositoryError) as exc_info:
        await repo.claim_due_tasks(
            now=datetime.now(UTC),
            lease_owner="stale-worker",
            lease_seconds=30,
            limit=1,
            tenant_digest=other.digest,
        )

    assert exc_info.value.code == "mcp_task_tenant_mismatch"

    with pytest.raises(McpTaskRepositoryError) as cancel_exc:
        await repo.request_cancel(
            "task-unknown",
            user_id="user-1",
            thread_id="thread-1",
            requested_at=datetime.now(UTC),
            actor_ref="a" * 64,
            reason_code="user_api",
            tenant_digest=other.digest,
        )
    assert cancel_exc.value.code == "mcp_task_tenant_mismatch"

    with pytest.raises(McpTaskRepositoryError) as result_exc:
        await repo.apply_snapshot(
            "task-unknown",
            lease_owner="stale-worker",
            status="completed",
            result=None,
            result_preview=None,
            result_truncated=False,
            result_artifact=None,
            error=None,
            input_required=None,
            next_poll_after_seconds=None,
            polled_at=datetime.now(UTC),
            tenant_digest=other.digest,
        )
    assert result_exc.value.code == "mcp_task_tenant_mismatch"

    with pytest.raises(McpTaskRepositoryError) as notify_exc:
        await repo.claim_notification_work(
            now=datetime.now(UTC),
            lease_owner="stale-worker",
            lease_seconds=30,
            limit=1,
            tracking_degraded_after_errors=3,
            tenant_digest=other.digest,
        )
    assert notify_exc.value.code == "mcp_task_tenant_mismatch"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("actor_ref", "reason_code"),
    [
        ("a" * 63, "user_api"),
        ("A" * 64, "user_api"),
        ("a" * 64, "untrusted_client_value"),
    ],
)
async def test_repository_rejects_invalid_cancel_attribution(
    tmp_path,
    actor_ref,
    reason_code,
):
    repo = await _make_repo(tmp_path)

    with pytest.raises(McpTaskRepositoryError) as exc_info:
        await repo.request_cancel(
            "task-unknown",
            user_id="user-1",
            thread_id="thread-1",
            requested_at=datetime.now(UTC),
            actor_ref=actor_ref,
            reason_code=reason_code,
            tenant_digest=repo.tenant.digest,
        )

    assert exc_info.value.code == "mcp_task_cancel_intent_invalid"


@pytest.mark.asyncio
async def test_repository_refuses_newer_schema_writer_at_startup(tmp_path):
    repo = await _make_repo(tmp_path)
    await _create_working_task(
        repo,
        task_id="future-writer",
        now=datetime.now(UTC),
    )
    async with repo._sf() as session:
        await session.execute(update(McpTaskRow).where(McpTaskRow.id == "future-writer").values(schema_writer_version=4))
        await session.commit()

    with pytest.raises(McpTaskRepositoryError) as exc_info:
        await repo.verify_schema_writer_compatibility()

    assert exc_info.value.code == "mcp_task_schema_writer_unsupported"


@pytest.mark.asyncio
async def test_schema_writer_v2_cannot_commit_without_tenant_lineage(tmp_path):
    repo = await _make_repo(tmp_path)
    await _create_working_task(
        repo,
        task_id="required-lineage",
        now=datetime.now(UTC),
    )

    with pytest.raises(IntegrityError):
        async with repo._sf() as session:
            await session.execute(
                update(McpTaskRow)
                .where(McpTaskRow.id == "required-lineage")
                .values(
                    schema_writer_version=2,
                    tenant_ref=None,
                    tenant_digest=None,
                    lineage_json=null(),
                    lineage_digest=None,
                )
            )
            await session.commit()


@pytest.mark.asyncio
async def test_legacy_terminal_and_nonterminal_rows_never_fabricate_lineage(tmp_path):
    repo = await _make_repo(tmp_path)
    now = datetime.now(UTC)
    await _create_working_task(repo, task_id="legacy-terminal", now=now)
    await _create_working_task(repo, task_id="legacy-working", now=now)
    async with repo._sf() as session:
        await session.execute(
            update(McpTaskRow)
            .where(McpTaskRow.id.in_(("legacy-terminal", "legacy-working")))
            .values(
                schema_writer_version=1,
                tenant_ref=None,
                tenant_digest=None,
                lineage_json=null(),
                lineage_digest=None,
                parent_run_id=None,
                parent_tool_receipt_id=None,
            )
        )
        await session.execute(
            update(McpTaskRow)
            .where(McpTaskRow.id == "legacy-terminal")
            .values(
                status="completed",
                next_poll_at=None,
                completed_at=now,
            )
        )
        await session.commit()

    terminal = await repo.get(
        "legacy-terminal",
        user_id="user-1",
        tenant_digest=repo.tenant.digest,
    )
    assert terminal is not None
    assert terminal["lineage_status"] == "legacy_unavailable"
    assert terminal["lineage"] is None
    assert terminal["parent_run_id"] is None
    assert terminal["parent_tool_receipt_id"] is None

    claimed = await repo.claim_due_tasks(
        now=now,
        lease_owner="legacy-worker",
        lease_seconds=60,
        limit=10,
        tenant_digest=repo.tenant.digest,
    )
    assert [item["id"] for item in claimed] == ["legacy-working"]
    assert claimed[0]["lineage_status"] == "legacy_unavailable"
    assert await repo.apply_snapshot(
        "legacy-working",
        lease_owner="legacy-worker",
        status="completed",
        result={"done": True},
        result_preview=None,
        result_truncated=False,
        result_artifact=None,
        error=None,
        input_required=None,
        next_poll_after_seconds=None,
        polled_at=now,
        tenant_digest=repo.tenant.digest,
    )


@pytest.mark.asyncio
async def test_two_tenants_with_same_owner_and_remote_handle_are_disjoint(tmp_path):
    repo_a = await _make_repo(tmp_path)
    session_factory = get_session_factory()
    assert session_factory is not None
    repo_b = McpTaskRepository(
        session_factory,
        tenant=TenantIdentityV1.from_canonical_id("other").to_persisted_reference(),
    )
    now = datetime.now(UTC)
    task_a = await _create_working_task(
        repo_a,
        task_id="tenant-a-task",
        now=now,
        remote_task_id="shared-remote",
    )
    task_b = await _create_working_task(
        repo_b,
        task_id="tenant-b-task",
        now=now,
        remote_task_id="shared-remote",
    )

    assert task_a["lineage_digest"] != task_b["lineage_digest"]
    assert (
        await repo_a.get(
            "tenant-b-task",
            user_id="user-1",
            tenant_digest=repo_a.tenant.digest,
        )
        is None
    )
    assert (
        await repo_b.get(
            "tenant-a-task",
            user_id="user-1",
            tenant_digest=repo_b.tenant.digest,
        )
        is None
    )


@pytest.mark.asyncio
async def test_claim_due_tasks_skips_live_leases_and_reclaims_expired_ones(tmp_path):
    repo = await _make_repo(tmp_path)
    now = datetime.now(UTC)
    await _create_working_task(repo, task_id="task-1", now=now)

    first = await repo.claim_due_tasks(
        now=now,
        lease_owner="worker-1",
        lease_seconds=60,
        limit=10,
        tenant_digest=repo.tenant.digest,
    )
    assert [task["id"] for task in first] == ["task-1"]

    while_live = await repo.claim_due_tasks(
        now=now + timedelta(seconds=10),
        lease_owner="worker-2",
        lease_seconds=60,
        limit=10,
        tenant_digest=repo.tenant.digest,
    )
    assert while_live == []

    async with repo._sf() as session:
        await session.execute(update(McpTaskRow).where(McpTaskRow.id == "task-1").values(lease_expires_at=now - timedelta(seconds=1)))
        await session.commit()

    reclaimed = await repo.claim_due_tasks(
        now=now + timedelta(seconds=61),
        lease_owner="worker-2",
        lease_seconds=60,
        limit=10,
        tenant_digest=repo.tenant.digest,
    )
    assert [task["id"] for task in reclaimed] == ["task-1"]
    assert reclaimed[0]["lease_owner"] == "worker-2"


@pytest.mark.asyncio
async def test_poll_lease_and_completion_use_database_clock_under_pod_skew(tmp_path):
    repo = await _make_repo(tmp_path)
    database_window_start = datetime.now(UTC)
    await _create_working_task(
        repo,
        task_id="task-db-clock-poll",
        now=database_window_start,
    )
    fast_pod = database_window_start + timedelta(days=1)

    claimed = await repo.claim_due_tasks(
        now=fast_pod,
        lease_owner="fast-pod",
        lease_seconds=60,
        limit=1,
        tenant_digest=repo.tenant.digest,
    )

    assert [item["id"] for item in claimed] == ["task-db-clock-poll"]
    lease_expires_at = datetime.fromisoformat(claimed[0]["lease_expires_at"])
    assert database_window_start + timedelta(seconds=55) <= lease_expires_at
    assert lease_expires_at <= datetime.now(UTC) + timedelta(seconds=65)
    assert await repo.apply_snapshot(
        "task-db-clock-poll",
        lease_owner="fast-pod",
        status="completed",
        result={"done": True},
        result_preview=None,
        result_truncated=False,
        result_artifact=None,
        error=None,
        input_required=None,
        next_poll_after_seconds=None,
        polled_at=fast_pod,
        tenant_digest=repo.tenant.digest,
    )


@pytest.mark.asyncio
async def test_slow_pod_clock_cannot_hide_database_due_poll_work(tmp_path):
    repo = await _make_repo(tmp_path)
    database_now = datetime.now(UTC)
    await _create_working_task(
        repo,
        task_id="task-db-clock-slow",
        now=database_now,
    )

    claimed = await repo.claim_due_tasks(
        now=database_now - timedelta(days=1),
        lease_owner="slow-pod",
        lease_seconds=60,
        limit=1,
        tenant_digest=repo.tenant.digest,
    )

    assert [item["id"] for item in claimed] == ["task-db-clock-slow"]


@pytest.mark.asyncio
async def test_cancel_and_notification_leases_use_database_clock_under_pod_skew(
    tmp_path,
):
    repo = await _make_repo(tmp_path)
    database_window_start = datetime.now(UTC)
    fast_pod = database_window_start + timedelta(days=1)
    await _create_working_task(
        repo,
        task_id="task-db-clock-cancel",
        now=database_window_start,
    )
    assert await repo.request_cancel(
        "task-db-clock-cancel",
        user_id="user-1",
        thread_id="thread-1",
        requested_at=database_window_start,
        actor_ref="a" * 64,
        reason_code="user_api",
        tenant_digest=repo.tenant.digest,
    )
    cancelled = await repo.claim_cancel_requests(
        now=fast_pod,
        lease_owner="fast-canceller",
        lease_seconds=60,
        limit=1,
        tenant_digest=repo.tenant.digest,
    )
    cancel_expiry = datetime.fromisoformat(cancelled[0]["lease_expires_at"])
    assert cancel_expiry <= datetime.now(UTC) + timedelta(seconds=65)
    assert await repo.apply_cancel_snapshot(
        "task-db-clock-cancel",
        lease_owner="fast-canceller",
        status="cancelled",
        result=None,
        result_preview=None,
        result_truncated=False,
        result_artifact=None,
        error=None,
        input_required=None,
        completed_at=fast_pod,
        tenant_digest=repo.tenant.digest,
    )

    notification = await repo.claim_notification_work(
        now=fast_pod,
        lease_owner="fast-notifier",
        lease_seconds=60,
        limit=1,
        tracking_degraded_after_errors=3,
        tenant_digest=repo.tenant.digest,
    )
    assert [item["id"] for item in notification] == ["task-db-clock-cancel"]
    notification_expiry = datetime.fromisoformat(
        notification[0]["notification_lease_expires_at"],
    )
    assert notification_expiry <= datetime.now(UTC) + timedelta(seconds=65)


@pytest.mark.asyncio
async def test_expired_poll_owner_cannot_release_or_schedule_work(tmp_path):
    repo = await _make_repo(tmp_path)
    database_now = datetime.now(UTC)
    await _create_working_task(
        repo,
        task_id="task-expired-poll-release",
        now=database_now,
    )
    await repo.claim_due_tasks(
        now=database_now,
        lease_owner="stale-poller",
        lease_seconds=60,
        limit=1,
        tenant_digest=repo.tenant.digest,
    )
    async with repo._sf() as session:
        await session.execute(update(McpTaskRow).where(McpTaskRow.id == "task-expired-poll-release").values(lease_expires_at=database_now - timedelta(seconds=1)))
        await session.commit()

    assert not await repo.release_claim(
        "task-expired-poll-release",
        lease_owner="stale-poller",
        retry_after_seconds=86_400,
        error="late remote failure",
        tenant_digest=repo.tenant.digest,
    )

    reclaimed = await repo.claim_due_tasks(
        now=database_now + timedelta(days=1),
        lease_owner="replacement-poller",
        lease_seconds=60,
        limit=1,
        tenant_digest=repo.tenant.digest,
    )
    assert [item["id"] for item in reclaimed] == [
        "task-expired-poll-release",
    ]


@pytest.mark.asyncio
async def test_expired_cancel_owner_cannot_release_or_schedule_work(tmp_path):
    repo = await _make_repo(tmp_path)
    database_now = datetime.now(UTC)
    await _create_working_task(
        repo,
        task_id="task-expired-cancel-release",
        now=database_now,
    )
    assert await repo.request_cancel(
        "task-expired-cancel-release",
        user_id="user-1",
        thread_id="thread-1",
        requested_at=database_now + timedelta(days=1),
        actor_ref="a" * 64,
        reason_code="user_api",
        tenant_digest=repo.tenant.digest,
    )
    claimed = await repo.claim_cancel_requests(
        now=database_now - timedelta(days=1),
        lease_owner="stale-canceller",
        lease_seconds=60,
        limit=1,
        tenant_digest=repo.tenant.digest,
    )
    assert [item["id"] for item in claimed] == [
        "task-expired-cancel-release",
    ]
    requested_at = datetime.fromisoformat(claimed[0]["cancel_requested_at"])
    assert requested_at <= datetime.now(UTC) + timedelta(seconds=5)
    async with repo._sf() as session:
        await session.execute(update(McpTaskRow).where(McpTaskRow.id == "task-expired-cancel-release").values(lease_expires_at=database_now - timedelta(seconds=1)))
        await session.commit()

    assert not await repo.release_cancel_claim(
        "task-expired-cancel-release",
        lease_owner="stale-canceller",
        retry_after_seconds=86_400,
        error="late cancellation failure",
        tenant_digest=repo.tenant.digest,
    )

    reclaimed = await repo.claim_cancel_requests(
        now=database_now + timedelta(days=1),
        lease_owner="replacement-canceller",
        lease_seconds=60,
        limit=1,
        tenant_digest=repo.tenant.digest,
    )
    assert [item["id"] for item in reclaimed] == [
        "task-expired-cancel-release",
    ]


@pytest.mark.asyncio
async def test_expired_notification_owner_cannot_release_or_schedule_work(
    tmp_path,
):
    repo = await _make_repo(tmp_path)
    database_now = datetime.now(UTC)
    await _create_working_task(
        repo,
        task_id="task-expired-notification-release",
        now=database_now,
    )
    await repo.claim_due_tasks(
        now=database_now,
        lease_owner="poller",
        lease_seconds=60,
        limit=1,
        tenant_digest=repo.tenant.digest,
    )
    await repo.apply_snapshot(
        "task-expired-notification-release",
        lease_owner="poller",
        status="completed",
        result={"done": True},
        result_preview=None,
        result_truncated=False,
        result_artifact=None,
        error=None,
        input_required=None,
        next_poll_after_seconds=None,
        polled_at=database_now,
        tenant_digest=repo.tenant.digest,
    )
    claimed = await repo.claim_notification_work(
        now=database_now,
        lease_owner="stale-notifier",
        lease_seconds=60,
        limit=1,
        tracking_degraded_after_errors=3,
        tenant_digest=repo.tenant.digest,
    )
    assert [item["id"] for item in claimed] == [
        "task-expired-notification-release",
    ]
    async with repo._sf() as session:
        await session.execute(update(McpTaskRow).where(McpTaskRow.id == "task-expired-notification-release").values(notification_lease_expires_at=(database_now - timedelta(seconds=1))))
        await session.commit()

    assert not await repo.release_notification_claim(
        "task-expired-notification-release",
        lease_owner="stale-notifier",
        retry_after_seconds=86_400,
        error="late notification failure",
        replace_with_latest=True,
        tenant_digest=repo.tenant.digest,
    )

    reclaimed = await repo.claim_notification_work(
        now=database_now + timedelta(days=1),
        lease_owner="replacement-notifier",
        lease_seconds=60,
        limit=1,
        tracking_degraded_after_errors=3,
        tenant_digest=repo.tenant.digest,
    )
    assert [item["id"] for item in reclaimed] == [
        "task-expired-notification-release",
    ]


@pytest.mark.asyncio
async def test_apply_snapshot_requires_current_lease_owner_and_terminalizes_task(tmp_path):
    repo = await _make_repo(tmp_path)
    now = datetime.now(UTC)
    await _create_working_task(repo, task_id="task-2", now=now)
    await repo.claim_due_tasks(
        now=now,
        lease_owner="worker-new",
        lease_seconds=60,
        limit=10,
        tenant_digest=repo.tenant.digest,
    )

    stale_applied = await repo.apply_snapshot(
        "task-2",
        lease_owner="worker-old",
        status="failed",
        result=None,
        result_preview=None,
        result_truncated=False,
        result_artifact=None,
        error="stale result",
        input_required=None,
        next_poll_after_seconds=None,
        polled_at=now,
        tenant_digest=repo.tenant.digest,
    )
    assert stale_applied is False

    applied = await repo.apply_snapshot(
        "task-2",
        lease_owner="worker-new",
        status="completed",
        result={"report": "ready"},
        result_preview=None,
        result_truncated=False,
        result_artifact={"uri": "s3://reports/2.json", "mime_type": "application/json"},
        error=None,
        input_required=None,
        next_poll_after_seconds=None,
        polled_at=now,
        tenant_digest=repo.tenant.digest,
    )
    assert applied is True

    stored = await repo.get(
        "task-2",
        user_id="user-1",
        tenant_digest=repo.tenant.digest,
    )
    assert stored is not None
    assert stored["status"] == "completed"
    assert stored["result"] == {"report": "ready"}
    assert stored["result_artifact"] == {
        "uri": "s3://reports/2.json",
        "mime_type": "application/json",
    }
    assert stored["notification_status"] == "pending"
    assert stored["lease_owner"] is None

    assert (
        await repo.claim_due_tasks(
            now=now + timedelta(hours=1),
            lease_owner="worker-3",
            lease_seconds=60,
            limit=10,
            tenant_digest=repo.tenant.digest,
        )
        == []
    )


@pytest.mark.asyncio
async def test_apply_snapshot_rejects_result_after_same_workers_lease_expires(tmp_path):
    repo = await _make_repo(tmp_path)
    now = datetime.now(UTC)
    await _create_working_task(repo, task_id="task-expired", now=now)
    await repo.claim_due_tasks(
        now=now,
        lease_owner="worker-1",
        lease_seconds=60,
        limit=10,
        tenant_digest=repo.tenant.digest,
    )
    async with repo._sf() as session:
        await session.execute(update(McpTaskRow).where(McpTaskRow.id == "task-expired").values(lease_expires_at=now - timedelta(seconds=1)))
        await session.commit()

    applied = await repo.apply_snapshot(
        "task-expired",
        lease_owner="worker-1",
        status="completed",
        result={"report": "stale"},
        result_preview=None,
        result_truncated=False,
        result_artifact=None,
        error=None,
        input_required=None,
        next_poll_after_seconds=None,
        polled_at=now + timedelta(seconds=61),
        tenant_digest=repo.tenant.digest,
    )

    assert applied is False
    stored = await repo.get(
        "task-expired",
        user_id="user-1",
        tenant_digest=repo.tenant.digest,
    )
    assert stored is not None
    assert stored["status"] == "working"
    assert stored["result"] is None


@pytest.mark.asyncio
async def test_input_required_is_persisted_and_remains_scheduled_for_slow_polling(tmp_path):
    repo = await _make_repo(tmp_path)
    now = datetime.now(UTC)
    await _create_working_task(repo, task_id="task-3", now=now)
    await repo.claim_due_tasks(
        now=now,
        lease_owner="worker-1",
        lease_seconds=60,
        limit=10,
        tenant_digest=repo.tenant.digest,
    )

    applied = await repo.apply_snapshot(
        "task-3",
        lease_owner="worker-1",
        status="input_required",
        result=None,
        result_preview=None,
        result_truncated=False,
        result_artifact=None,
        error=None,
        input_required={"prompt": "Approve deployment?"},
        next_poll_after_seconds=60,
        polled_at=now,
        tenant_digest=repo.tenant.digest,
    )
    assert applied is True

    stored = await repo.get(
        "task-3",
        user_id="user-1",
        tenant_digest=repo.tenant.digest,
    )
    assert stored is not None
    assert stored["input_required"] == {"prompt": "Approve deployment?"}
    assert stored["notification_status"] == "pending"
    next_poll_at = datetime.fromisoformat(stored["next_poll_at"])
    assert now + timedelta(seconds=59) <= next_poll_at
    assert next_poll_at <= datetime.now(UTC) + timedelta(seconds=61)


@pytest.mark.asyncio
async def test_release_claim_retries_transient_poll_failure(tmp_path):
    repo = await _make_repo(tmp_path)
    now = datetime.now(UTC)
    await _create_working_task(repo, task_id="task-4", now=now)
    await repo.claim_due_tasks(
        now=now,
        lease_owner="worker-1",
        lease_seconds=60,
        limit=10,
        tenant_digest=repo.tenant.digest,
    )
    released = await repo.release_claim(
        "task-4",
        lease_owner="worker-1",
        retry_after_seconds=30,
        error="temporary network failure",
        tenant_digest=repo.tenant.digest,
    )
    assert released is True

    stored = await repo.get(
        "task-4",
        user_id="user-1",
        tenant_digest=repo.tenant.digest,
    )
    assert stored is not None
    assert stored["status"] == "working"
    assert stored["last_poll_error"] == "temporary network failure"
    next_poll_at = datetime.fromisoformat(stored["next_poll_at"])
    assert now + timedelta(seconds=29) <= next_poll_at
    assert next_poll_at <= datetime.now(UTC) + timedelta(seconds=31)
    assert stored["lease_owner"] is None


@pytest.mark.asyncio
async def test_consecutive_poll_error_count_increments_and_resets_on_success(tmp_path):
    repo = await _make_repo(tmp_path)
    now = datetime.now(UTC)
    await _create_working_task(repo, task_id="task-6", now=now)

    for expected_errors in (1, 2):
        await repo.claim_due_tasks(
            now=now,
            lease_owner="worker-1",
            lease_seconds=60,
            limit=10,
            tenant_digest=repo.tenant.digest,
        )
        await repo.release_claim(
            "task-6",
            lease_owner="worker-1",
            retry_after_seconds=0,
            error="temporary network failure",
            tenant_digest=repo.tenant.digest,
        )
        stored = await repo.get(
            "task-6",
            user_id="user-1",
            tenant_digest=repo.tenant.digest,
        )
        assert stored is not None
        assert stored["consecutive_poll_error_count"] == expected_errors

    await repo.claim_due_tasks(
        now=now,
        lease_owner="worker-1",
        lease_seconds=60,
        limit=10,
        tenant_digest=repo.tenant.digest,
    )
    applied = await repo.apply_snapshot(
        "task-6",
        lease_owner="worker-1",
        status="working",
        result=None,
        result_preview=None,
        result_truncated=False,
        result_artifact=None,
        error=None,
        input_required=None,
        next_poll_after_seconds=5,
        polled_at=now,
        tenant_digest=repo.tenant.digest,
    )
    assert applied is True

    stored = await repo.get(
        "task-6",
        user_id="user-1",
        tenant_digest=repo.tenant.digest,
    )
    assert stored is not None
    assert stored["consecutive_poll_error_count"] == 0


@pytest.mark.asyncio
async def test_notification_snapshot_is_versioned_and_not_overwritten_in_flight(tmp_path):
    repo = await _make_repo(tmp_path)
    now = datetime.now(UTC)
    await _create_working_task(repo, task_id="task-notify", now=now)
    await repo.claim_due_tasks(
        now=now,
        lease_owner="poller",
        lease_seconds=60,
        limit=1,
        tenant_digest=repo.tenant.digest,
    )
    await repo.apply_snapshot(
        "task-notify",
        lease_owner="poller",
        status="input_required",
        result=None,
        result_preview=None,
        result_truncated=False,
        result_artifact=None,
        error=None,
        input_required={"prompt": "Approve?"},
        next_poll_after_seconds=0,
        polled_at=now,
        tenant_digest=repo.tenant.digest,
    )

    first = await repo.claim_notification_work(
        now=now,
        lease_owner="notifier",
        lease_seconds=60,
        limit=1,
        tracking_degraded_after_errors=3,
        tenant_digest=repo.tenant.digest,
    )
    assert first[0]["dispatch_version"] == 1
    assert first[0]["dispatch_event"]["input_required"] == {"prompt": "Approve?"}

    await repo.claim_due_tasks(
        now=now,
        lease_owner="poller",
        lease_seconds=60,
        limit=1,
        tenant_digest=repo.tenant.digest,
    )
    await repo.apply_snapshot(
        "task-notify",
        lease_owner="poller",
        status="completed",
        result={"done": True},
        result_preview=None,
        result_truncated=False,
        result_artifact=None,
        error=None,
        input_required=None,
        next_poll_after_seconds=None,
        polled_at=now,
        tenant_digest=repo.tenant.digest,
    )
    changed = await repo.get(
        "task-notify",
        user_id="user-1",
        tenant_digest=repo.tenant.digest,
    )
    assert changed is not None
    assert changed["event_version"] == 2
    assert changed["dispatch_version"] == 1
    assert changed["dispatch_event"]["status"] == "input_required"

    await repo.mark_notification_dispatched(
        "task-notify",
        lease_owner="notifier",
        dispatch_version=1,
        run_id="notify-run-1",
        now=now,
        tenant_digest=repo.tenant.digest,
    )
    await repo.claim_notification_work(
        now=now,
        lease_owner="notifier",
        lease_seconds=60,
        limit=1,
        tracking_degraded_after_errors=3,
        tenant_digest=repo.tenant.digest,
    )
    await repo.finish_notification_run(
        "task-notify",
        lease_owner="notifier",
        dispatch_version=1,
        delivered=True,
        retry_after_seconds=None,
        error=None,
        now=now,
        tenant_digest=repo.tenant.digest,
    )
    delivered = await repo.get(
        "task-notify",
        user_id="user-1",
        tenant_digest=repo.tenant.digest,
    )
    assert delivered is not None
    assert delivered["notification_run_id"] == "notify-run-1"
    second = await repo.claim_notification_work(
        now=now,
        lease_owner="notifier",
        lease_seconds=60,
        limit=1,
        tracking_degraded_after_errors=3,
        tenant_digest=repo.tenant.digest,
    )
    assert second[0]["dispatch_version"] == 2
    assert second[0]["dispatch_event"]["status"] == "completed"


@pytest.mark.asyncio
async def test_notification_retry_rebuilds_a_newer_event_and_resets_its_budget(tmp_path):
    repo = await _make_repo(tmp_path)
    now = datetime.now(UTC)
    await _create_working_task(repo, task_id="task-retry-latest", now=now)
    await repo.claim_due_tasks(
        now=now,
        lease_owner="poller",
        lease_seconds=60,
        limit=1,
        tenant_digest=repo.tenant.digest,
    )
    await repo.apply_snapshot(
        "task-retry-latest",
        lease_owner="poller",
        status="input_required",
        result=None,
        result_preview=None,
        result_truncated=False,
        result_artifact=None,
        error=None,
        input_required={"prompt": "Approve?"},
        next_poll_after_seconds=0,
        polled_at=now,
        tenant_digest=repo.tenant.digest,
    )
    first = await repo.claim_notification_work(
        now=now,
        lease_owner="notifier",
        lease_seconds=60,
        limit=1,
        tracking_degraded_after_errors=3,
        tenant_digest=repo.tenant.digest,
    )
    await repo.mark_notification_dispatched(
        "task-retry-latest",
        lease_owner="notifier",
        dispatch_version=first[0]["dispatch_version"],
        run_id="notify-run-1",
        now=now,
        tenant_digest=repo.tenant.digest,
    )
    await repo.claim_notification_work(
        now=now,
        lease_owner="notifier",
        lease_seconds=60,
        limit=1,
        tracking_degraded_after_errors=3,
        tenant_digest=repo.tenant.digest,
    )
    await repo.finish_notification_run(
        "task-retry-latest",
        lease_owner="notifier",
        dispatch_version=first[0]["dispatch_version"],
        delivered=False,
        retry_after_seconds=5,
        error="Agent run failed",
        now=now,
        tenant_digest=repo.tenant.digest,
    )
    failed = await repo.get(
        "task-retry-latest",
        user_id="user-1",
        tenant_digest=repo.tenant.digest,
    )
    assert failed is not None
    assert failed["notification_status"] == "retry"
    assert failed["dispatch_attempt"] == 1
    assert failed["notification_attempt_count"] == 1

    await repo.claim_due_tasks(
        now=now,
        lease_owner="poller",
        lease_seconds=60,
        limit=1,
        tenant_digest=repo.tenant.digest,
    )
    await repo.apply_snapshot(
        "task-retry-latest",
        lease_owner="poller",
        status="completed",
        result={"done": True},
        result_preview=None,
        result_truncated=False,
        result_artifact=None,
        error=None,
        input_required=None,
        next_poll_after_seconds=None,
        polled_at=now,
        tenant_digest=repo.tenant.digest,
    )
    async with repo._sf() as session:
        await session.execute(update(McpTaskRow).where(McpTaskRow.id == "task-retry-latest").values(next_notification_at=now - timedelta(seconds=1)))
        await session.commit()
    latest = await repo.claim_notification_work(
        now=now + timedelta(days=1),
        lease_owner="notifier",
        lease_seconds=60,
        limit=1,
        tracking_degraded_after_errors=3,
        tenant_digest=repo.tenant.digest,
    )

    assert latest[0]["dispatch_version"] == first[0]["dispatch_version"] + 1
    assert latest[0]["dispatch_event"]["status"] == "completed"
    assert latest[0]["dispatch_attempt"] == 0
    assert latest[0]["notification_attempt_count"] == 0


@pytest.mark.asyncio
async def test_unexpected_notification_failure_releases_lease_without_changing_phase(tmp_path):
    repo = await _make_repo(tmp_path)
    now = datetime.now(UTC)
    await _create_working_task(repo, task_id="task-notify-release", now=now)
    await repo.claim_due_tasks(
        now=now,
        lease_owner="poller",
        lease_seconds=60,
        limit=1,
        tenant_digest=repo.tenant.digest,
    )
    await repo.apply_snapshot(
        "task-notify-release",
        lease_owner="poller",
        status="input_required",
        result=None,
        result_preview=None,
        result_truncated=False,
        result_artifact=None,
        error=None,
        input_required={"prompt": "Approve?"},
        next_poll_after_seconds=0,
        polled_at=now,
        tenant_digest=repo.tenant.digest,
    )
    await repo.claim_notification_work(
        now=now,
        lease_owner="notifier",
        lease_seconds=60,
        limit=1,
        tracking_degraded_after_errors=3,
        tenant_digest=repo.tenant.digest,
    )
    assert await repo.release_notification_lease(
        "task-notify-release",
        lease_owner="notifier",
        retry_after_seconds=5,
        error="run store unavailable",
        tenant_digest=repo.tenant.digest,
    )

    stored = await repo.get(
        "task-notify-release",
        user_id="user-1",
        tenant_digest=repo.tenant.digest,
    )
    assert stored is not None
    assert stored["notification_status"] == "claimed"
    assert stored["notification_lease_owner"] is None
    assert stored["notification_error"] == "run store unavailable"
    next_notification_at = datetime.fromisoformat(
        stored["next_notification_at"],
    )
    assert now + timedelta(seconds=4) <= next_notification_at
    assert next_notification_at <= datetime.now(UTC) + timedelta(seconds=6)


@pytest.mark.asyncio
async def test_notification_launch_failure_counts_and_reclaims_latest_snapshot(tmp_path):
    repo = await _make_repo(tmp_path)
    now = datetime.now(UTC)
    await _create_working_task(repo, task_id="task-launch-retry", now=now)
    await repo.claim_due_tasks(
        now=now,
        lease_owner="poller",
        lease_seconds=60,
        limit=1,
        tenant_digest=repo.tenant.digest,
    )
    await repo.apply_snapshot(
        "task-launch-retry",
        lease_owner="poller",
        status="input_required",
        result=None,
        result_preview=None,
        result_truncated=False,
        result_artifact=None,
        error=None,
        input_required={"prompt": "Approve?"},
        next_poll_after_seconds=0,
        polled_at=now,
        tenant_digest=repo.tenant.digest,
    )
    first = await repo.claim_notification_work(
        now=now,
        lease_owner="notifier",
        lease_seconds=60,
        limit=1,
        tracking_degraded_after_errors=3,
        tenant_digest=repo.tenant.digest,
    )
    assert await repo.release_notification_claim(
        "task-launch-retry",
        lease_owner="notifier",
        retry_after_seconds=5,
        error="run store unavailable",
        replace_with_latest=True,
        count_failure=True,
        tenant_digest=repo.tenant.digest,
    )

    stored = await repo.get(
        "task-launch-retry",
        user_id="user-1",
        tenant_digest=repo.tenant.digest,
    )
    assert stored is not None
    assert stored["notification_status"] == "pending"
    assert stored["notification_attempt_count"] == 1
    assert stored["dispatch_version"] == first[0]["dispatch_version"]
    async with repo._sf() as session:
        await session.execute(update(McpTaskRow).where(McpTaskRow.id == "task-launch-retry").values(next_notification_at=now - timedelta(seconds=1)))
        await session.commit()
    reclaimed = await repo.claim_notification_work(
        now=now + timedelta(days=1),
        lease_owner="notifier",
        lease_seconds=60,
        limit=1,
        tracking_degraded_after_errors=3,
        tenant_digest=repo.tenant.digest,
    )
    assert reclaimed[0]["notification_attempt_count"] == 1
    assert reclaimed[0]["dispatch_version"] == first[0]["dispatch_version"]
    assert reclaimed[0]["dispatch_event"] == first[0]["dispatch_event"]


@pytest.mark.asyncio
async def test_permanent_notification_failure_is_not_reclaimed(tmp_path):
    repo = await _make_repo(tmp_path)
    now = datetime.now(UTC)
    await _create_working_task(repo, task_id="task-dead-letter", now=now)
    await repo.claim_due_tasks(
        now=now,
        lease_owner="poller",
        lease_seconds=60,
        limit=1,
        tenant_digest=repo.tenant.digest,
    )
    await repo.apply_snapshot(
        "task-dead-letter",
        lease_owner="poller",
        status="completed",
        result={"done": True},
        result_preview=None,
        result_truncated=False,
        result_artifact=None,
        error=None,
        input_required=None,
        next_poll_after_seconds=None,
        polled_at=now,
        tenant_digest=repo.tenant.digest,
    )
    claimed = await repo.claim_notification_work(
        now=now,
        lease_owner="notifier",
        lease_seconds=60,
        limit=1,
        tracking_degraded_after_errors=3,
        tenant_digest=repo.tenant.digest,
    )

    assert await repo.dead_letter_notification(
        "task-dead-letter",
        lease_owner="notifier",
        dispatch_version=claimed[0]["dispatch_version"],
        error="Thread deleted-thread not found",
        count_failure=True,
        now=now,
        tenant_digest=repo.tenant.digest,
    )

    stored = await repo.get(
        "task-dead-letter",
        user_id="user-1",
        tenant_digest=repo.tenant.digest,
    )
    assert stored is not None
    assert stored["notification_status"] == "dead_letter"
    assert stored["notification_attempt_count"] == 1
    assert stored["notification_error"] == "Thread deleted-thread not found"
    assert (
        await repo.claim_notification_work(
            now=now + timedelta(days=1),
            lease_owner="other",
            lease_seconds=60,
            limit=1,
            tracking_degraded_after_errors=3,
            tenant_digest=repo.tenant.digest,
        )
        == []
    )


@pytest.mark.asyncio
async def test_dispatched_notification_can_be_dead_lettered_after_retry_budget(tmp_path):
    repo = await _make_repo(tmp_path)
    now = datetime.now(UTC)
    await _create_working_task(repo, task_id="task-dispatched-budget", now=now)
    await repo.claim_due_tasks(
        now=now,
        lease_owner="poller",
        lease_seconds=60,
        limit=1,
        tenant_digest=repo.tenant.digest,
    )
    await repo.apply_snapshot(
        "task-dispatched-budget",
        lease_owner="poller",
        status="completed",
        result={"done": True},
        result_preview=None,
        result_truncated=False,
        result_artifact=None,
        error=None,
        input_required=None,
        next_poll_after_seconds=None,
        polled_at=now,
        tenant_digest=repo.tenant.digest,
    )
    first = await repo.claim_notification_work(
        now=now,
        lease_owner="notifier",
        lease_seconds=60,
        limit=1,
        tracking_degraded_after_errors=3,
        tenant_digest=repo.tenant.digest,
    )
    dispatch_version = first[0]["dispatch_version"]
    assert await repo.mark_notification_dispatched(
        "task-dispatched-budget",
        lease_owner="notifier",
        dispatch_version=dispatch_version,
        run_id="notify-run-1",
        now=now,
        tenant_digest=repo.tenant.digest,
    )

    claimed = await repo.claim_notification_work(
        now=now,
        lease_owner="budget-checker",
        lease_seconds=60,
        limit=1,
        tracking_degraded_after_errors=3,
        tenant_digest=repo.tenant.digest,
    )
    assert claimed[0]["notification_status"] == "dispatched"
    assert await repo.dead_letter_notification(
        "task-dispatched-budget",
        lease_owner="budget-checker",
        dispatch_version=dispatch_version,
        error="Notification delivery stopped after 5 failed attempts",
        count_failure=False,
        now=now,
        tenant_digest=repo.tenant.digest,
    )

    stored = await repo.get(
        "task-dispatched-budget",
        user_id="user-1",
        tenant_digest=repo.tenant.digest,
    )
    assert stored is not None
    assert stored["notification_status"] == "dead_letter"
    assert stored["notification_run_id"] == "notify-run-1"


@pytest.mark.asyncio
async def test_dead_lettering_dispatched_snapshot_preserves_newer_event(tmp_path):
    repo = await _make_repo(tmp_path)
    now = datetime.now(UTC)
    await _create_working_task(repo, task_id="task-dispatched-latest", now=now)
    await repo.claim_due_tasks(
        now=now,
        lease_owner="poller",
        lease_seconds=60,
        limit=1,
        tenant_digest=repo.tenant.digest,
    )
    await repo.apply_snapshot(
        "task-dispatched-latest",
        lease_owner="poller",
        status="input_required",
        result=None,
        result_preview=None,
        result_truncated=False,
        result_artifact=None,
        error=None,
        input_required={"prompt": "Approve?"},
        next_poll_after_seconds=0,
        polled_at=now,
        tenant_digest=repo.tenant.digest,
    )
    first = await repo.claim_notification_work(
        now=now,
        lease_owner="notifier",
        lease_seconds=60,
        limit=1,
        tracking_degraded_after_errors=3,
        tenant_digest=repo.tenant.digest,
    )
    dispatch_version = first[0]["dispatch_version"]
    assert await repo.mark_notification_dispatched(
        "task-dispatched-latest",
        lease_owner="notifier",
        dispatch_version=dispatch_version,
        run_id="notify-run-1",
        now=now,
        tenant_digest=repo.tenant.digest,
    )

    await repo.claim_due_tasks(
        now=now,
        lease_owner="poller",
        lease_seconds=60,
        limit=1,
        tenant_digest=repo.tenant.digest,
    )
    await repo.apply_snapshot(
        "task-dispatched-latest",
        lease_owner="poller",
        status="completed",
        result={"done": True},
        result_preview=None,
        result_truncated=False,
        result_artifact=None,
        error=None,
        input_required=None,
        next_poll_after_seconds=None,
        polled_at=now,
        tenant_digest=repo.tenant.digest,
    )
    claimed = await repo.claim_notification_work(
        now=now,
        lease_owner="budget-checker",
        lease_seconds=60,
        limit=1,
        tracking_degraded_after_errors=3,
        tenant_digest=repo.tenant.digest,
    )
    assert claimed[0]["dispatch_version"] == dispatch_version
    assert await repo.dead_letter_notification(
        "task-dispatched-latest",
        lease_owner="budget-checker",
        dispatch_version=dispatch_version,
        error="old snapshot exhausted its retry budget",
        count_failure=False,
        now=now,
        tenant_digest=repo.tenant.digest,
    )

    stored = await repo.get(
        "task-dispatched-latest",
        user_id="user-1",
        tenant_digest=repo.tenant.digest,
    )
    assert stored is not None
    assert stored["notification_status"] == "pending"
    assert stored["notification_attempt_count"] == 0
    assert stored["notification_error"] is None
    latest = await repo.claim_notification_work(
        now=now,
        lease_owner="latest-notifier",
        lease_seconds=60,
        limit=1,
        tracking_degraded_after_errors=3,
        tenant_digest=repo.tenant.digest,
    )
    assert latest[0]["dispatch_version"] > dispatch_version
    assert latest[0]["dispatch_event"]["status"] == "completed"


@pytest.mark.asyncio
async def test_cancel_request_stops_polling_and_rejects_stale_poll_result(tmp_path):
    repo = await _make_repo(tmp_path)
    now = datetime.now(UTC)
    created = await _create_working_task(repo, task_id="task-cancel", now=now)
    await repo.claim_due_tasks(
        now=now,
        lease_owner="stale-poller",
        lease_seconds=60,
        limit=1,
        tenant_digest=repo.tenant.digest,
    )

    requested = await repo.request_cancel(
        "task-cancel",
        user_id="user-1",
        thread_id="thread-1",
        requested_at=now,
        actor_ref="a" * 64,
        reason_code="user_api",
        tenant_digest=repo.tenant.digest,
    )
    assert requested is not None
    cancel_requested_at = datetime.fromisoformat(
        requested["cancel_requested_at"],
    )
    assert now <= cancel_requested_at <= datetime.now(UTC)
    assert requested["cancel_actor_ref"] == "a" * 64
    assert requested["cancel_reason_code"] == "user_api"
    assert requested["lineage_digest"] == created["lineage_digest"]
    assert (
        await repo.claim_due_tasks(
            now=now,
            lease_owner="new-poller",
            lease_seconds=60,
            limit=1,
            tenant_digest=repo.tenant.digest,
        )
        == []
    )
    assert (
        await repo.apply_snapshot(
            "task-cancel",
            lease_owner="stale-poller",
            status="completed",
            result={"stale": True},
            result_preview=None,
            result_truncated=False,
            result_artifact=None,
            error=None,
            input_required=None,
            next_poll_after_seconds=None,
            polled_at=now,
            tenant_digest=repo.tenant.digest,
        )
        is False
    )

    claimed = await repo.claim_cancel_requests(
        now=now,
        lease_owner="canceller",
        lease_seconds=60,
        limit=1,
        tenant_digest=repo.tenant.digest,
    )
    assert [row["id"] for row in claimed] == ["task-cancel"]

    repeated = await repo.request_cancel(
        "task-cancel",
        user_id="user-1",
        thread_id="thread-1",
        requested_at=now + timedelta(seconds=1),
        actor_ref="b" * 64,
        reason_code="agent_tool",
        tenant_digest=repo.tenant.digest,
    )
    assert repeated is not None
    assert repeated["cancel_requested_at"] == requested["cancel_requested_at"]
    assert repeated["cancel_actor_ref"] == "a" * 64
    assert repeated["cancel_reason_code"] == "user_api"
    assert repeated["lineage_digest"] == created["lineage_digest"]
    assert repeated["lease_owner"] == "canceller"
    assert repeated["cancel_attempt_count"] == 1
    assert (
        await repo.claim_cancel_requests(
            now=now,
            lease_owner="other",
            lease_seconds=60,
            limit=1,
            tenant_digest=repo.tenant.digest,
        )
        == []
    )
    assert await repo.apply_cancel_snapshot(
        "task-cancel",
        lease_owner="canceller",
        status="cancelled",
        result=None,
        result_preview=None,
        result_truncated=False,
        result_artifact=None,
        error=None,
        input_required=None,
        completed_at=now,
        tenant_digest=repo.tenant.digest,
    )
    stored = await repo.get(
        "task-cancel",
        user_id="user-1",
        tenant_digest=repo.tenant.digest,
    )
    assert stored is not None
    assert stored["status"] == "cancelled"
    assert stored["notification_status"] == "pending"
