from __future__ import annotations

import pytest
from langgraph.runtime import Runtime

from deerflow.runtime.accepted_invocation import ResolvedAgentMaterialV1
from deerflow.runtime.agent_revision import RESOLVED_AGENT_MATERIAL_CONTEXT_KEY
from deerflow.runtime.skill_projection import (
    SKILL_PROJECTION_TOKEN_CONTEXT_KEY,
    get_skill_projection_coordinator,
)
from deerflow.sandbox.accepted_material import (
    AcceptedSkillSandboxBindingError,
    AcceptedSkillSandboxBindingV1,
)
from deerflow.sandbox.accepted_projection import (
    bind_runtime_accepted_skill_projection_async,
    release_accepted_skill_consumer,
)
from deerflow.sandbox.capabilities import AcceptedSkillProjection
from deerflow.sandbox.sandbox import Sandbox
from deerflow.sandbox.sandbox_provider import (
    SandboxProvider,
    get_sandbox_provider,
    reset_sandbox_provider,
    set_sandbox_provider,
    shutdown_sandbox_provider,
)
from deerflow.sandbox.session import unwrap_sandbox_provider


class _BindingProvider(SandboxProvider, AcceptedSkillProjection):
    def __init__(self) -> None:
        self.fail_next_bind = True
        self.bindings: list[AcceptedSkillSandboxBindingV1] = []
        self.refuse_teardown = False

    def acquire(self, thread_id: str | None = None, *, user_id: str | None = None) -> str:
        del thread_id, user_id
        return "sandbox-binding-recovery"

    def get(self, sandbox_id: str) -> Sandbox | None:
        del sandbox_id
        return None

    def has_accepted_skill_isolation(self, sandbox_id: str) -> bool:
        return sandbox_id == "sandbox-binding-recovery"

    def release(self, sandbox_id: str) -> None:
        del sandbox_id

    def bind_accepted_skill_snapshot(
        self,
        sandbox_id: str,
        *,
        thread_id: str,
        user_id: str,
        binding: AcceptedSkillSandboxBindingV1,
    ) -> None:
        del sandbox_id, thread_id, user_id
        if self.fail_next_bind:
            self.fail_next_bind = False
            raise RuntimeError("projection bind failed")
        self.bindings.append(binding)

    def clear_accepted_skill_snapshot(self, clear) -> bool:
        del clear
        return True

    def _check_teardown(self) -> None:
        if self.refuse_teardown:
            raise AcceptedSkillSandboxBindingError(
                "accepted_skill_snapshot_projection_in_use",
            )

    def reset(self) -> None:
        self._check_teardown()

    def shutdown(self) -> None:
        self._check_teardown()


class _RetryingCleanupProvider(_BindingProvider):
    def __init__(self, *, first_clear: str = "false", fail_release: bool = False) -> None:
        super().__init__()
        self.first_clear = first_clear
        self.fail_release = fail_release
        self.clear_attempts = 0
        self.release_attempts = 0

    def clear_accepted_skill_snapshot(self, clear) -> bool:
        del clear
        self.clear_attempts += 1
        if self.clear_attempts == 1 and self.first_clear == "false":
            return False
        if self.clear_attempts == 1 and self.first_clear == "exception":
            raise RuntimeError("transient clear failure")
        return True

    def release(self, sandbox_id: str) -> None:
        del sandbox_id
        self.release_attempts += 1
        if self.fail_release:
            raise RuntimeError("resource release failure")


@pytest.mark.asyncio
async def test_failed_bind_invalidates_runtime_token_before_retry() -> None:
    provider = _BindingProvider()
    material = ResolvedAgentMaterialV1(
        agent_id="lead-agent",
        storage_source="test",
        storage_version="1",
        agent_config=None,
        soul="",
        model_profile={},
    )
    runtime = Runtime(
        context={
            "thread_id": "thread-binding-recovery",
            "run_id": "run-binding-recovery",
            "user_id": "owner-binding-recovery",
            RESOLVED_AGENT_MATERIAL_CONTEXT_KEY: material,
        }
    )
    coordinator = get_skill_projection_coordinator()
    set_sandbox_provider(provider)
    try:
        with pytest.raises(RuntimeError, match="projection bind failed"):
            await bind_runtime_accepted_skill_projection_async(
                provider,
                runtime,
                sandbox_id="sandbox-binding-recovery",
                user_id="owner-binding-recovery",
            )

        assert SKILL_PROJECTION_TOKEN_CONTEXT_KEY not in runtime.context

        assert await bind_runtime_accepted_skill_projection_async(
            provider,
            runtime,
            sandbox_id="sandbox-binding-recovery",
            user_id="owner-binding-recovery",
        )
        replacement = runtime.context[SKILL_PROJECTION_TOKEN_CONTEXT_KEY]
        assert coordinator.owns(replacement)
        assert len(provider.bindings) == 1
    finally:
        token = runtime.context.pop(SKILL_PROJECTION_TOKEN_CONTEXT_KEY, None)
        if token is not None:
            release_accepted_skill_consumer(token)
        reset_sandbox_provider()


@pytest.mark.parametrize("first_clear", ["false", "exception"])
def test_failed_clear_retains_exact_cleanup_proof_for_retry(first_clear: str) -> None:
    provider = _RetryingCleanupProvider(first_clear=first_clear)
    coordinator = get_skill_projection_coordinator()
    thread_id = f"thread-clear-retry-{first_clear}"
    run_id = f"run-clear-retry-{first_clear}"
    coordinator.claim_committed_run(
        user_id="owner-clear-retry",
        thread_id=thread_id,
        run_id=run_id,
        snapshot_id=None,
    )
    token = coordinator.activate(
        user_id="owner-clear-retry",
        thread_id=thread_id,
        sandbox_id=f"sandbox-clear-retry-{first_clear}",
        run_id=run_id,
        snapshot_id=None,
        consumer_id=f"run:{run_id}:lead",
    )
    set_sandbox_provider(provider)
    try:
        if first_clear == "exception":
            with pytest.raises(RuntimeError, match="transient clear failure"):
                release_accepted_skill_consumer(token)
        else:
            assert release_accepted_skill_consumer(token) is False

        assert coordinator.is_busy(
            user_id="owner-clear-retry",
            thread_id=thread_id,
        )
        assert not coordinator.try_claim_committed_run(
            user_id="owner-clear-retry",
            thread_id=thread_id,
            run_id="replacement-before-clear",
            snapshot_id=None,
        )

        assert release_accepted_skill_consumer(token) is True
        assert provider.clear_attempts == 2
        assert not coordinator.is_busy(
            user_id="owner-clear-retry",
            thread_id=thread_id,
        )
        assert coordinator.try_claim_committed_run(
            user_id="owner-clear-retry",
            thread_id=thread_id,
            run_id="replacement-after-clear",
            snapshot_id=None,
        )
    finally:
        coordinator.release_unactivated_run(
            user_id="owner-clear-retry",
            thread_id=thread_id,
            run_id="replacement-after-clear",
        )
        reset_sandbox_provider()


def test_successful_clear_finalizes_even_when_resource_release_fails() -> None:
    provider = _RetryingCleanupProvider(first_clear="success", fail_release=True)
    coordinator = get_skill_projection_coordinator()
    coordinator.claim_committed_run(
        user_id="owner-release-failure",
        thread_id="thread-release-failure",
        run_id="run-release-failure",
        snapshot_id=None,
    )
    token = coordinator.activate(
        user_id="owner-release-failure",
        thread_id="thread-release-failure",
        sandbox_id="sandbox-release-failure",
        run_id="run-release-failure",
        snapshot_id=None,
        consumer_id="run:run-release-failure:lead",
    )
    set_sandbox_provider(provider)
    try:
        with pytest.raises(RuntimeError, match="resource release failure"):
            release_accepted_skill_consumer(token)

        assert provider.clear_attempts == 1
        assert not coordinator.is_busy(
            user_id="owner-release-failure",
            thread_id="thread-release-failure",
        )
        assert coordinator.try_claim_committed_run(
            user_id="owner-release-failure",
            thread_id="thread-release-failure",
            run_id="replacement-after-release-failure",
            snapshot_id=None,
        )
    finally:
        coordinator.release_unactivated_run(
            user_id="owner-release-failure",
            thread_id="thread-release-failure",
            run_id="replacement-after-release-failure",
        )
        provider.fail_release = False
        reset_sandbox_provider()


@pytest.mark.parametrize(
    "teardown",
    [reset_sandbox_provider, shutdown_sandbox_provider],
)
def test_refused_provider_teardown_preserves_installed_provider(teardown) -> None:
    provider = _BindingProvider()
    provider.refuse_teardown = True
    set_sandbox_provider(provider)
    try:
        with pytest.raises(
            AcceptedSkillSandboxBindingError,
            match="accepted_skill_snapshot_projection_in_use",
        ):
            teardown()

        assert unwrap_sandbox_provider(get_sandbox_provider()) is provider
    finally:
        provider.refuse_teardown = False
        teardown()
