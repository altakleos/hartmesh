from __future__ import annotations

import asyncio
import threading
import time
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from deerflow_extension_api import TenantReferenceV1
from langchain.tools import ToolRuntime

from deerflow.sandbox.accepted_material import (
    AcceptedExecutionEvidenceV1,
    AcceptedMaterialCapability,
    AcceptedMaterialError,
    AcceptedMaterialExecutionClaimV1,
    AcceptedMaterialLeaseV1,
    AcceptedMaterialRequestV1,
    AcceptedSandboxAuthorityLostError,
    AcceptedSandboxLifecycleKind,
    AcceptedSandboxOperationV1,
    AcceptedSandboxSession,
    AcceptedSandboxSessionBridge,
    accepted_execution_evidence_reference,
    accepted_sandbox_from_runtime_context,
    install_accepted_sandbox_session,
)
from deerflow.sandbox.sandbox import Sandbox
from deerflow.sandbox.sandbox_provider import (
    SandboxProvider,
    reset_sandbox_provider,
    set_sandbox_provider,
)
from deerflow.sandbox.tools import bash_tool, ensure_sandbox_initialized_async


class RecordingSandbox(Sandbox):
    persistent_shell_sessions = False

    def __init__(self) -> None:
        super().__init__("raw-provider-resource")
        self.calls: list[tuple[str, tuple[object, ...], dict[str, object]]] = []

    def _record(self, name: str, *args: object, **kwargs: object) -> str:
        self.calls.append((name, args, kwargs))
        return name

    def execute_command(self, command, env=None, timeout=None):
        return self._record("execute_command", command, env=env, timeout=timeout)

    def read_file(self, path, start_line=None, end_line=None):
        return self._record(
            "read_file",
            path,
            start_line=start_line,
            end_line=end_line,
        )

    def download_file(self, path):
        self._record("download_file", path)
        return b"download_file"

    def list_dir(self, path, max_depth=2):
        self._record("list_dir", path, max_depth=max_depth)
        return ["list_dir"]

    def write_file(self, path, content, append=False):
        self._record("write_file", path, content, append=append)

    def glob(self, path, pattern, *, include_dirs=False, max_results=200):
        self._record(
            "glob",
            path,
            pattern,
            include_dirs=include_dirs,
            max_results=max_results,
        )
        return (["glob"], False)

    def grep(
        self,
        path,
        pattern,
        *,
        glob=None,
        literal=False,
        case_sensitive=False,
        max_results=100,
    ):
        self._record(
            "grep",
            path,
            pattern,
            glob=glob,
            literal=literal,
            case_sensitive=case_sensitive,
            max_results=max_results,
        )
        return ([], False)

    def update_file(self, path, content):
        self._record("update_file", path, content)


class RecordingMaterializer:
    def __init__(self, *, valid: bool = True) -> None:
        self.valid = valid
        self.validate_calls = 0
        self.renew_calls = 0
        self.release_calls = 0
        self.fail_renew = False

    def capability(self):
        return AcceptedMaterialCapability.IMMUTABLE_READ_ONLY

    async def validate(self, lease, evidence):
        del lease, evidence
        self.validate_calls += 1
        return self.valid

    async def renew(self, lease):
        self.renew_calls += 1
        if self.fail_renew:
            raise AcceptedMaterialError("accepted_material_lease_lost")
        return AcceptedMaterialLeaseV1(
            version=1,
            provider_kind=lease.provider_kind,
            provider_instance_ref=lease.provider_instance_ref,
            ownership_epoch=lease.ownership_epoch,
            lease_expires_at=lease.lease_expires_at + timedelta(minutes=1),
            opaque_renewal_handle=lease.opaque_renewal_handle,
        )

    async def release(self, lease):
        del lease
        self.release_calls += 1


def _session_tuple():
    tenant = TenantReferenceV1(
        version=1,
        public_ref="tenant-1111111111111111",
        digest="1" * 64,
    )
    expires_at = datetime(2030, 1, 1, tzinfo=UTC)
    request = AcceptedMaterialRequestV1.build(
        run_id="run-1",
        attempt_id="attempt-1",
        tenant=tenant,
        user_ref="user-ref",
        thread_ref="thread-ref",
        agent_revision_digest="2" * 64,
        skill_snapshot_digest="3" * 64,
        skill_scope_digest="4" * 64,
        file_manifest=(),
        runtime_image_digest="5" * 64,
        lease_expires_at=expires_at,
    )
    lease = AcceptedMaterialLeaseV1(
        version=1,
        provider_kind="test",
        provider_instance_ref="raw-provider-resource",
        ownership_epoch=7,
        lease_expires_at=expires_at,
        opaque_renewal_handle=object(),
    )
    evidence = AcceptedExecutionEvidenceV1.build(
        run_id=request.run_id,
        attempt_id=request.attempt_id,
        tenant=tenant,
        provider_kind=lease.provider_kind,
        provider_instance_ref=lease.provider_instance_ref,
        ownership_epoch=lease.ownership_epoch,
        runtime_image_digest=request.runtime_image_digest,
        skill_snapshot_digest=request.skill_snapshot_digest,
        skill_scope_digest=request.skill_scope_digest,
        materialization_digest=request.digest,
        verifier_image_digest="6" * 64,
        verifier_contract_version="test_v1",
        read_only_proof_digest="7" * 64,
        qualification_scope="contract_test_only",
    )
    claim = AcceptedMaterialExecutionClaimV1(
        version=1,
        tenant_digest=tenant.digest,
        run_id=request.run_id,
        owner_worker_id="worker-1",
        state_version=8,
        execution_takeover=False,
    )
    return lease, evidence, claim


def _session(*, run_current: bool = True, material_current: bool = True):
    lease, evidence, claim = _session_tuple()
    sandbox = RecordingSandbox()
    materializer = RecordingMaterializer(valid=material_current)
    authority_calls: list[AcceptedMaterialExecutionClaimV1] = []

    async def validate_run(checked_claim):
        authority_calls.append(checked_claim)
        return run_current

    session = AcceptedSandboxSession(
        sandbox=sandbox,
        materializer=materializer,
        lease=lease,
        evidence=evidence,
        execution_claim=claim,
        run_fence_validator=validate_run,
    )
    return session, sandbox, materializer, authority_calls


@pytest.mark.asyncio
async def test_session_delegates_only_after_both_authorities_validate() -> None:
    session, sandbox, materializer, authority_calls = _session()

    result = await session.execute(
        AcceptedSandboxOperationV1.execute_command(
            "echo bounded",
            env={"SAFE": "1"},
            timeout=2,
        ),
    )

    assert result == "execute_command"
    assert len(authority_calls) == 1
    assert materializer.validate_calls == 1
    assert sandbox.calls == [
        (
            "execute_command",
            ("echo bounded",),
            {"env": {"SAFE": "1"}, "timeout": 2},
        ),
    ]


@pytest.mark.asyncio
async def test_bridge_publishes_only_safe_evidence_and_operation_references() -> None:
    session, _sandbox, _materializer, _authority_calls = _session()
    _lease, evidence, _claim = _session_tuple()
    bridge = AcceptedSandboxSessionBridge(
        session,
        owner_loop=asyncio.get_running_loop(),
    )
    operation = AcceptedSandboxOperationV1.read_file("/safe")

    assert bridge.execution_evidence_reference == (accepted_execution_evidence_reference(evidence))
    assert bridge.execution_evidence_reference.startswith("accepted-execution-")
    assert operation.operation_ref.startswith("accepted-operation-")
    assert "raw-provider-resource" not in bridge.execution_evidence_reference


@pytest.mark.asyncio
async def test_materializer_validation_loss_prevents_provider_delegation() -> None:
    session, sandbox, materializer, authority_calls = _session(
        material_current=False,
    )

    with pytest.raises(
        AcceptedSandboxAuthorityLostError,
        match="accepted_sandbox_material_lease_lost",
    ):
        await session.execute(AcceptedSandboxOperationV1.read_file("/safe"))

    assert len(authority_calls) == 1
    assert materializer.validate_calls == 1
    assert sandbox.calls == []


@pytest.mark.asyncio
async def test_renewal_loss_permanently_invalidates_the_session() -> None:
    session, sandbox, materializer, authority_calls = _session()
    materializer.fail_renew = True

    with pytest.raises(
        AcceptedSandboxAuthorityLostError,
        match="accepted_sandbox_material_lease_lost",
    ):
        await session.renew()
    with pytest.raises(AcceptedSandboxAuthorityLostError):
        await session.execute(AcceptedSandboxOperationV1.read_file("/safe"))

    assert len(authority_calls) == 1
    assert sandbox.calls == []
    assert [observation.kind for observation in session.lifecycle_observations] == [
        AcceptedSandboxLifecycleKind.ACQUIRED,
        AcceptedSandboxLifecycleKind.AUTHORITY_LOST,
    ]


@pytest.mark.asyncio
async def test_cancellation_during_validation_permanently_invalidates_session() -> None:
    lease, evidence, claim = _session_tuple()
    sandbox = RecordingSandbox()
    materializer = RecordingMaterializer()
    validation_started = asyncio.Event()

    async def validate_run(_claim):
        validation_started.set()
        await asyncio.Event().wait()
        return True

    session = AcceptedSandboxSession(
        sandbox=sandbox,
        materializer=materializer,
        lease=lease,
        evidence=evidence,
        execution_claim=claim,
        run_fence_validator=validate_run,
    )
    operation = asyncio.create_task(
        session.execute(AcceptedSandboxOperationV1.read_file("/safe")),
    )
    await validation_started.wait()
    operation.cancel()

    with pytest.raises(asyncio.CancelledError):
        await operation
    with pytest.raises(
        AcceptedSandboxAuthorityLostError,
        match="accepted_sandbox_session_not_open",
    ):
        await session.execute(AcceptedSandboxOperationV1.read_file("/later"))

    assert sandbox.calls == []
    assert session.lifecycle_observations[-1].reason_code == ("accepted_sandbox_session_cancelled")


@pytest.mark.asyncio
async def test_close_invalidates_before_awaiting_provider_release() -> None:
    session, sandbox, materializer, _authority_calls = _session()
    release_started = asyncio.Event()
    permit_release = asyncio.Event()

    async def blocked_release(lease):
        del lease
        release_started.set()
        await permit_release.wait()
        materializer.release_calls += 1

    materializer.release = blocked_release
    close_task = asyncio.create_task(session.close())
    await release_started.wait()

    with pytest.raises(AcceptedSandboxAuthorityLostError):
        await session.execute(AcceptedSandboxOperationV1.read_file("/safe"))

    permit_release.set()
    await close_task
    await session.close()
    assert materializer.release_calls == 1
    assert sandbox.calls == []
    assert session.lifecycle_observations[-1].kind is (AcceptedSandboxLifecycleKind.RELEASED)


@pytest.mark.asyncio
async def test_renewal_is_not_blocked_by_an_already_delegated_operation() -> None:
    """A long provider call must not starve the existing lease heartbeat."""

    session, sandbox, materializer, _authority_calls = _session()
    operation_started = threading.Event()
    permit_operation = threading.Event()

    def blocked_command(command, env=None, timeout=None):
        del command, env, timeout
        operation_started.set()
        assert permit_operation.wait(timeout=2)
        sandbox.calls.append(("execute_command", (), {}))
        return "completed"

    sandbox.execute_command = blocked_command
    operation = asyncio.create_task(
        session.execute(AcceptedSandboxOperationV1.execute_command("long")),
    )
    assert await asyncio.to_thread(operation_started.wait, 2)

    started_at = time.monotonic()
    await asyncio.wait_for(session.renew(), timeout=0.5)
    assert time.monotonic() - started_at < 0.5
    assert materializer.renew_calls == 1

    permit_operation.set()
    assert await operation == "completed"


@pytest.mark.asyncio
async def test_close_waits_for_an_already_delegated_operation_before_release() -> None:
    """Closing refuses new calls but does not release under an in-flight call."""

    session, sandbox, materializer, _authority_calls = _session()
    operation_started = threading.Event()
    permit_operation = threading.Event()

    def blocked_command(command, env=None, timeout=None):
        del command, env, timeout
        operation_started.set()
        assert permit_operation.wait(timeout=2)
        return "completed"

    sandbox.execute_command = blocked_command
    operation = asyncio.create_task(
        session.execute(AcceptedSandboxOperationV1.execute_command("long")),
    )
    assert await asyncio.to_thread(operation_started.wait, 2)

    close_task = asyncio.create_task(session.close())
    await asyncio.sleep(0)
    assert materializer.release_calls == 0
    with pytest.raises(AcceptedSandboxAuthorityLostError):
        await session.execute(AcceptedSandboxOperationV1.read_file("/later"))

    permit_operation.set()
    assert await operation == "completed"
    await close_task
    assert materializer.release_calls == 1


@pytest.mark.asyncio
async def test_check_then_call_profile_allows_one_gap_race_but_no_later_call() -> None:
    """Document the deliberately narrower baseline provider guarantee."""

    lease, evidence, claim = _session_tuple()
    sandbox = RecordingSandbox()
    materializer = RecordingMaterializer()
    validation_reached = asyncio.Event()
    release_validation = asyncio.Event()
    run_current = True

    async def validate_run(_claim):
        return run_current

    async def pause_after_both_validations():
        validation_reached.set()
        await release_validation.wait()

    session = AcceptedSandboxSession(
        sandbox=sandbox,
        materializer=materializer,
        lease=lease,
        evidence=evidence,
        execution_claim=claim,
        run_fence_validator=validate_run,
        before_delegate=pause_after_both_validations,
    )
    raced = asyncio.create_task(
        session.execute(AcceptedSandboxOperationV1.execute_command("raced")),
    )
    await validation_reached.wait()
    run_current = False
    release_validation.set()

    # This call was already past the SQL sample when takeover won. Providers
    # declaring atomic_provider_operation_fencing=False may accept it.
    assert await raced == "execute_command"
    with pytest.raises(
        AcceptedSandboxAuthorityLostError,
        match="accepted_sandbox_run_fence_lost",
    ):
        await session.execute(
            AcceptedSandboxOperationV1.execute_command("after-loss"),
        )

    assert [call[1][0] for call in sandbox.calls] == ["raced"]


@pytest.mark.parametrize(
    ("operation", "expected"),
    [
        (AcceptedSandboxOperationV1.execute_command("cmd"), "execute_command"),
        (AcceptedSandboxOperationV1.read_file("/file"), "read_file"),
        (AcceptedSandboxOperationV1.download_file("/file"), b"download_file"),
        (AcceptedSandboxOperationV1.list_dir("/dir"), ["list_dir"]),
        (AcceptedSandboxOperationV1.write_file("/file", "text"), None),
        (AcceptedSandboxOperationV1.glob("/dir", "*.md"), (["glob"], False)),
        (AcceptedSandboxOperationV1.grep("/dir", "needle"), ([], False)),
        (AcceptedSandboxOperationV1.update_file("/file", b"bytes"), None),
    ],
)
@pytest.mark.asyncio
async def test_session_covers_every_sandbox_operation(operation, expected) -> None:
    session, sandbox, materializer, authority_calls = _session()

    assert await session.execute(operation) == expected
    assert len(sandbox.calls) == 1
    assert materializer.validate_calls == 1
    assert len(authority_calls) == 1


@pytest.mark.asyncio
async def test_session_bridge_returns_only_a_safe_sandbox_facade() -> None:
    session, sandbox, materializer, authority_calls = _session()
    context: dict[str, object] = {}

    bridge = install_accepted_sandbox_session(context, session)
    facade = accepted_sandbox_from_runtime_context(context)

    assert isinstance(bridge, AcceptedSandboxSessionBridge)
    assert facade is bridge.sandbox
    assert facade is not sandbox
    assert "raw-provider-resource" not in facade.id
    assert await asyncio.to_thread(facade.execute_command, "echo bridged") == ("execute_command")
    assert materializer.validate_calls == 1
    assert len(authority_calls) == 1
    assert sandbox.calls[0][0] == "execute_command"


@pytest.mark.asyncio
async def test_tool_sandbox_resolution_cannot_bypass_an_installed_session(
    monkeypatch,
) -> None:
    session, raw_sandbox, _materializer, _authority_calls = _session()
    context: dict[str, object] = {"thread_id": "thread-1"}
    bridge = install_accepted_sandbox_session(context, session)

    class PoisonProvider(SandboxProvider):
        def acquire(self, thread_id=None, *, user_id=None):
            raise AssertionError("accepted session must not acquire a second sandbox")

        def get(self, sandbox_id):
            raise AssertionError("accepted session must not expose the raw sandbox")

        def release(self, sandbox_id):
            raise AssertionError("accepted session owns release")

    async def authorize(**kwargs):
        del kwargs

    monkeypatch.setattr(
        "deerflow.sandbox.tools.authorize_sandbox_execution_async",
        authorize,
    )
    runtime = ToolRuntime(
        state={"sandbox": {"sandbox_id": raw_sandbox.id}},
        context=context,
        config={"configurable": {}},
        stream_writer=lambda _: None,
        tools=[],
        tool_call_id="call-1",
        store=None,
    )
    set_sandbox_provider(PoisonProvider())
    try:
        resolved = await ensure_sandbox_initialized_async(runtime)
    finally:
        reset_sandbox_provider()

    assert resolved is bridge.sandbox


@pytest.mark.asyncio
async def test_public_tool_does_not_convert_authority_loss_to_model_text(
    monkeypatch,
) -> None:
    session, raw_sandbox, materializer, _authority_calls = _session(
        run_current=False,
    )
    context: dict[str, object] = {"thread_id": "thread-1"}
    install_accepted_sandbox_session(context, session)

    async def authorize(**kwargs):
        del kwargs

    monkeypatch.setattr(
        "deerflow.sandbox.tools.authorize_sandbox_execution_async",
        authorize,
    )
    runtime = ToolRuntime(
        state={"sandbox": {"sandbox_id": raw_sandbox.id}},
        context=context,
        config={"configurable": {}},
        stream_writer=lambda _: None,
        tools=[],
        tool_call_id="call-1",
        store=None,
    )

    with pytest.raises(
        AcceptedSandboxAuthorityLostError,
        match="accepted_sandbox_run_fence_lost",
    ):
        await bash_tool.coroutine(runtime, "echo forbidden", "authority test")

    assert materializer.validate_calls == 0
    assert raw_sandbox.calls == []


@pytest.mark.asyncio
async def test_tool_output_externalization_resolves_the_gated_facade(
    monkeypatch,
) -> None:
    from deerflow.agents.middlewares.tool_output_budget_middleware import (
        _resolve_sandbox,
    )

    session, raw_sandbox, _materializer, _authority_calls = _session()
    context: dict[str, object] = {}
    bridge = install_accepted_sandbox_session(context, session)

    def forbidden_provider():
        raise AssertionError("externalization must not look up the raw provider")

    monkeypatch.setattr(
        "deerflow.agents.middlewares.tool_output_budget_middleware.get_sandbox_provider",
        forbidden_provider,
    )
    runtime = SimpleNamespace(
        context=context,
        state={"sandbox": {"sandbox_id": raw_sandbox.id}},
    )

    assert _resolve_sandbox(SimpleNamespace(runtime=runtime)) is bridge.sandbox


@pytest.mark.asyncio
async def test_tool_output_externalization_never_reopens_the_raw_provider(
    monkeypatch,
    tmp_path,
) -> None:
    from deerflow.agents.middlewares.tool_output_budget_middleware import (
        _budget_content,
    )
    from deerflow.config.tool_output_config import ToolOutputConfig

    session, raw_sandbox, materializer, _authority_calls = _session()
    context: dict[str, object] = {}
    bridge = install_accepted_sandbox_session(context, session)
    provider_calls = 0

    def forbidden_provider():
        nonlocal provider_calls
        provider_calls += 1
        raise AssertionError("accepted externalization must stay on the facade")

    def command(command, env=None, timeout=None):
        del env, timeout
        raw_sandbox.calls.append(("execute_command", (command,), {}))
        return "OK" if command.startswith("test -s") else ""

    raw_sandbox.execute_command = command
    monkeypatch.setattr(
        "deerflow.agents.middlewares.tool_output_budget_middleware.get_sandbox_provider",
        forbidden_provider,
    )

    result = await asyncio.to_thread(
        _budget_content,
        "x" * 500,
        tool_name="remote_executor",
        tool_call_id="tool-call-1",
        outputs_path=str(tmp_path),
        config=ToolOutputConfig(
            externalize_min_chars=50,
            preview_head_chars=20,
            preview_tail_chars=10,
        ),
        sandbox=bridge.sandbox,
    )

    assert result is not None and result[1] == "externalized"
    assert provider_calls == 0
    assert materializer.validate_calls == 3


@pytest.mark.asyncio
async def test_sandbox_middleware_uses_session_without_provider_lifecycle(
    monkeypatch,
) -> None:
    from deerflow.sandbox.middleware import SandboxMiddleware

    session, raw_sandbox, _materializer, _authority_calls = _session()
    context: dict[str, object] = {
        "thread_id": "thread-1",
        "user_id": "user-1",
    }
    bridge = install_accepted_sandbox_session(context, session)

    async def authorize(**kwargs):
        del kwargs

    monkeypatch.setattr(
        "deerflow.sandbox.middleware.authorize_sandbox_execution_async",
        authorize,
    )
    monkeypatch.setattr(
        "deerflow.sandbox.middleware.safe_app_config_async",
        lambda: asyncio.sleep(0, result=object()),
    )

    class PoisonProvider(SandboxProvider):
        def acquire(self, thread_id=None, *, user_id=None):
            raise AssertionError((thread_id, user_id))

        def get(self, sandbox_id):
            raise AssertionError(sandbox_id)

        def release(self, sandbox_id):
            raise AssertionError(sandbox_id)

    runtime = SimpleNamespace(context=context)
    middleware = SandboxMiddleware(lazy_init=True)
    set_sandbox_provider(PoisonProvider())
    try:
        update = await middleware.abefore_agent({}, runtime)
        assert update == {"sandbox": {"sandbox_id": bridge.safe_reference}}
        assert await middleware.aafter_agent(update, runtime) is None
    finally:
        reset_sandbox_provider()

    assert raw_sandbox.calls == []
