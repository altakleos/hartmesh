"""The host owner retains finite calls and resources, not arbitrary graph tasks.

Before the new module exists, the availability case is an API scaffold. The
remaining cases are the intended new-owner contract, not claims of old defects.
Actual SQL/saver/Store lifetime boundaries have separate integration tests.
"""

import asyncio
import importlib
import importlib.util
import traceback
from contextvars import ContextVar

import pytest

MODULE = "deerflow.runtime.runs.capture_io"
EXPORTS = (
    "CaptureIOOwner",
    "CaptureIOResource",
    "CaptureIOClosed",
    "CaptureIOUnsupported",
    "CaptureIOCleanupError",
    "current_capture_io_owner",
    "capture_io_scope",
    "run_capture_io",
)


def test_capture_io_owner_api_contract_is_available():
    assert importlib.util.find_spec(MODULE) is not None, "New CaptureIOOwner API scaffold is unavailable; this is not an existing behavioral regression"
    module = importlib.import_module(MODULE)
    assert all(hasattr(module, name) for name in EXPORTS)


@pytest.fixture
def api():
    return importlib.import_module(MODULE)


class RetainedResource:
    """A protocol resource with an explicit close boundary and owned release."""

    def __init__(self, *, error=None):
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.closed = asyncio.Event()
        self.error = error
        self.retire_calls = 0
        self.close_attempts = 0
        self.task = None

    async def close(self):
        self.close_attempts += 1
        self.entered.set()
        await self.release.wait()
        if self.error is not None:
            raise self.error
        self.closed.set()

    def retire(self):
        self.retire_calls += 1
        if self.task is None:
            self.task = asyncio.create_task(self.close())

    async def drain(self):
        if self.task is not None:
            await self.task


@pytest.mark.asyncio
async def test_owner_binds_current_loop_and_plain_helper_preserves_ordinary_result(api):
    assert api.current_capture_io_owner() is None
    owner = api.CaptureIOOwner()
    assert owner.loop is asyncio.get_running_loop()
    assert owner.retired is False
    task = asyncio.current_task()

    async def operation():
        assert api.current_capture_io_owner() is None
        assert asyncio.current_task() is task
        return {"value": 37}

    assert await api.run_capture_io(operation) == {"value": 37}
    owner.retire()
    await owner.drain()


@pytest.mark.asyncio
async def test_owner_retirement_is_irreversible_and_refuses_an_uninvoked_factory(api):
    owner = api.CaptureIOOwner()
    calls = []

    async def completed():
        return 37

    def operation():
        calls.append("delegate")
        return completed()

    owner.retire()
    owner.retire()
    await owner.wait_retired()
    with pytest.raises(api.CaptureIOClosed) as first:
        owner.check_open()
    with pytest.raises(api.CaptureIOClosed) as second:
        await owner.run(operation)
    assert str(first.value) == str(second.value)
    assert calls == [] and owner.retired is True
    await owner.drain()
    await owner.drain()


@pytest.mark.asyncio
async def test_prepared_owner_task_rechecks_retirement_before_factory_invocation(api):
    owner = api.CaptureIOOwner()
    invoked = []

    async def operation():
        return 37

    def factory():
        invoked.append("factory-called")
        return operation()

    pending = asyncio.create_task(owner.run(factory))
    # One scheduling turn lets run register its retained child. The caller's
    # continuation is already queued before that child can invoke the factory.
    await asyncio.sleep(0)
    owner.retire()
    try:
        with pytest.raises(api.CaptureIOClosed):
            await pending
        assert invoked == []
    finally:
        owner.retire()
        await asyncio.gather(pending, return_exceptions=True)
        await owner.drain()


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel", ["caller", "drain", "both"])
async def test_started_unit_and_cleanup_remain_owned_through_repeated_cancellation(api, cancel):
    owner = api.CaptureIOOwner()
    entered, release, finished, delegate_cancelled = (asyncio.Event() for _ in range(4))

    async def operation():
        entered.set()
        try:
            await release.wait()
            return 37
        except asyncio.CancelledError:
            delegate_cancelled.set()
            raise
        finally:
            finished.set()

    running = asyncio.create_task(owner.run(operation))
    draining = None
    try:
        await asyncio.wait_for(entered.wait(), 3)
        owner.retire()
        draining = asyncio.create_task(owner.drain())
        await asyncio.sleep(0)
        if cancel in {"caller", "both"}:
            running.cancel()
            await asyncio.sleep(0)
            running.cancel()
        if cancel in {"drain", "both"}:
            draining.cancel()
            await asyncio.sleep(0)
            draining.cancel()
        done, _ = await asyncio.wait({running, draining}, timeout=0.01)
        assert not done and not finished.is_set() and not delegate_cancelled.is_set()
        release.set()
        result, drained = await asyncio.gather(running, draining, return_exceptions=True)
        assert finished.is_set() and not delegate_cancelled.is_set()
        assert isinstance(result, asyncio.CancelledError) if cancel in {"caller", "both"} else result == 37
        assert isinstance(drained, asyncio.CancelledError) if cancel in {"drain", "both"} else drained is None
        await owner.drain()
    finally:
        release.set()
        owner.retire()
        await asyncio.gather(*(task for task in (running, draining) if task is not None), return_exceptions=True)
        await owner.drain()


@pytest.mark.asyncio
async def test_atomic_delegate_steps_can_finish_but_nested_public_call_is_fresh_admission(api):
    owner = api.CaptureIOOwner()
    entered, release = asyncio.Event(), asyncio.Event()
    steps = []

    async def forbidden():
        steps.append("new-call")

    async def transaction():
        steps.append("begin")
        entered.set()
        await release.wait()
        steps.append("commit-and-close")
        with pytest.raises(api.CaptureIOClosed):
            await owner.run(forbidden)
        return 37

    running = asyncio.create_task(owner.run(transaction))
    try:
        await asyncio.wait_for(entered.wait(), 3)
        owner.retire()
        release.set()
        assert await running == 37
        assert steps == ["begin", "commit-and-close"]
        await owner.drain()
    finally:
        release.set()
        owner.retire()
        await asyncio.gather(running, return_exceptions=True)
        await owner.drain()


@pytest.mark.asyncio
async def test_original_task_scope_restores_nested_context_and_children_inherit_owner(api):
    first, second = api.CaptureIOOwner(), api.CaptureIOOwner()
    role = ContextVar("capture_owner_unit_scope", default="parent")
    original = asyncio.current_task()

    async def child():
        assert api.current_capture_io_owner() is first
        assert role.get() == "host"
        return 37

    token = role.set("host")
    try:
        with api.capture_io_scope(first):
            assert asyncio.current_task() is original
            assert api.current_capture_io_owner() is first
            assert await asyncio.create_task(child()) == 37
            with pytest.raises(ValueError, match="scope control"):
                with api.capture_io_scope(second):
                    assert asyncio.current_task() is original
                    assert api.current_capture_io_owner() is second
                    raise ValueError("scope control")
            assert api.current_capture_io_owner() is first
        assert api.current_capture_io_owner() is None
    finally:
        role.reset(token)
        first.retire()
        second.retire()
        await asyncio.gather(first.drain(), second.drain())
    assert role.get() == "parent" and api.current_capture_io_owner() is None


@pytest.mark.asyncio
async def test_helper_uses_original_owner_and_cannot_bypass_its_retirement(api):
    owner = api.CaptureIOOwner()
    calls = []

    async def operation():
        calls.append(api.current_capture_io_owner())
        return 37

    try:
        with api.capture_io_scope(owner):
            assert await api.run_capture_io(operation) == 37
            owner.retire()
            with pytest.raises(api.CaptureIOClosed):
                await api.run_capture_io(operation)
        assert calls == [owner]
        assert api.current_capture_io_owner() is None
    finally:
        owner.retire()
        await owner.drain()


@pytest.mark.asyncio
@pytest.mark.parametrize("ambient", ["none", "other"])
async def test_explicit_owner_binds_its_child_without_rebinding_the_parent_context(api, ambient):
    owner, other = api.CaptureIOOwner(), api.CaptureIOOwner()
    inherited = ContextVar("capture_explicit_owner_payload", default=None)
    payload = object()
    token = inherited.set(payload)
    parent = asyncio.current_task()
    ambient_owner = other if ambient == "other" else None

    async def sdk_child():
        assert api.current_capture_io_owner() is owner
        assert inherited.get() is payload
        return 37

    async def operation():
        assert asyncio.current_task() is not parent
        assert api.current_capture_io_owner() is owner
        return await asyncio.create_task(sdk_child())

    try:
        with api.capture_io_scope(ambient_owner):
            assert await owner.run(operation) == 37
            assert api.current_capture_io_owner() is ambient_owner
            assert inherited.get() is payload
        assert api.current_capture_io_owner() is None
    finally:
        inherited.reset(token)
        owner.retire()
        other.retire()
        await asyncio.gather(owner.drain(), other.drain())


@pytest.mark.asyncio
async def test_idle_retained_resource_starts_one_close_and_drain_owns_it(api):
    owner, resource = api.CaptureIOOwner(), RetainedResource()
    owner.register_resource(resource)
    owner.retire()
    owner.retire()
    draining = asyncio.create_task(owner.drain())
    try:
        await asyncio.wait_for(resource.entered.wait(), 3)
        assert resource.retire_calls == 1 and resource.close_attempts == 1
        draining.cancel()
        await asyncio.sleep(0)
        draining.cancel()
        done, _ = await asyncio.wait({draining}, timeout=0.01)
        assert not done and not resource.closed.is_set()
        resource.release.set()
        with pytest.raises(asyncio.CancelledError):
            await draining
        assert resource.closed.is_set()
        owner.unregister_resource(resource)
        await owner.drain()
        assert resource.close_attempts == 1
    finally:
        resource.release.set()
        owner.retire()
        await asyncio.gather(draining, return_exceptions=True)
        await owner.drain()


@pytest.mark.asyncio
async def test_late_resource_registration_refuses_without_starting_it(api):
    owner, resource = api.CaptureIOOwner(), RetainedResource()
    owner.retire()
    with pytest.raises(api.CaptureIOClosed):
        owner.register_resource(resource)
    assert resource.task is None and resource.retire_calls == 0
    await owner.drain()


@pytest.mark.asyncio
async def test_open_drain_cannot_cache_success_across_later_resource_retirement(api):
    owner, resource = api.CaptureIOOwner(), RetainedResource()
    owner.register_resource(resource)
    await owner.drain()
    assert resource.task is None, "Observing current work does not close an open resource"
    owner.retire()
    draining = asyncio.create_task(owner.drain())
    try:
        await asyncio.wait_for(resource.entered.wait(), 3)
        done, _ = await asyncio.wait({draining}, timeout=0.01)
        assert not done, "An earlier open observation is not a retired cleanup result"
        resource.release.set()
        await draining
        assert resource.closed.is_set() and resource.close_attempts == 1
    finally:
        resource.release.set()
        owner.retire()
        await asyncio.gather(draining, resource.drain(), return_exceptions=True)
        await owner.drain()


@pytest.mark.asyncio
async def test_normally_closed_unregistered_resource_is_not_retired_again(api):
    owner, resource = api.CaptureIOOwner(), RetainedResource()
    owner.register_resource(resource)
    resource.retire()
    resource.release.set()
    await resource.drain()
    owner.unregister_resource(resource)
    owner.retire()
    await owner.drain()
    assert resource.closed.is_set() and resource.retire_calls == 1 and resource.close_attempts == 1


@pytest.mark.asyncio
async def test_close_failure_is_fixed_and_reported_only_after_all_other_work_settles(api):
    owner = api.CaptureIOOwner()
    failed = RetainedResource(error=ValueError("private-close-payload"))
    healthy = RetainedResource()
    owner.register_resource(failed)
    owner.register_resource(healthy)
    owner.retire()
    draining = asyncio.create_task(owner.drain())
    try:
        await asyncio.wait_for(asyncio.gather(failed.entered.wait(), healthy.entered.wait()), 3)
        failed.release.set()
        done, _ = await asyncio.wait({draining}, timeout=0.01)
        assert not done, "A failed close cannot detach another already-started close"
        healthy.release.set()
        with pytest.raises(api.CaptureIOCleanupError, match="^Capture I/O cleanup failed$") as error:
            await draining
        rendered = "".join(traceback.format_exception(error.value))
        assert "private-close-payload" not in rendered
        assert healthy.closed.is_set() and not failed.closed.is_set()
        with pytest.raises(api.CaptureIOCleanupError, match="^Capture I/O cleanup failed$"):
            await owner.drain()
        assert failed.close_attempts == 1 and healthy.close_attempts == 1
    finally:
        failed.release.set()
        healthy.release.set()
        owner.retire()
        await asyncio.gather(draining, return_exceptions=True)
        await asyncio.gather(owner.drain(), return_exceptions=True)


@pytest.mark.asyncio
async def test_delegate_error_keeps_its_original_caller_result_without_an_unhandled_task(api):
    owner = api.CaptureIOOwner()
    error = ValueError("original delegate failure")

    async def operation():
        raise error

    with pytest.raises(ValueError) as caught:
        await owner.run(operation)
    assert caught.value is error
    owner.retire()
    await owner.drain()


@pytest.mark.asyncio
async def test_cancelled_retirement_waiter_does_not_retire_owner_or_other_waiters(api):
    owner = api.CaptureIOOwner()
    first = asyncio.create_task(owner.wait_retired())
    second = asyncio.create_task(owner.wait_retired())
    try:
        await asyncio.sleep(0)
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        assert not owner.retired and not second.done()
        owner.retire()
        await second
    finally:
        owner.retire()
        await asyncio.gather(first, second, return_exceptions=True)
        await owner.drain()


def test_unsupported_profile_refusal_has_fixed_separate_type(api):
    error = api.CaptureIOUnsupported()
    assert str(error) == "Capture I/O profile is unsupported"
    assert not isinstance(error, api.CaptureIOClosed)


@pytest.mark.asyncio
@pytest.mark.parametrize("entry", ["check", "run", "scope", "unregister"])
async def test_fixed_host_refusal_does_not_chain_currently_handled_private_failure(api, entry):
    owner = api.CaptureIOOwner()
    resource = RetainedResource(error=ValueError("separate close failure"))
    if entry == "unregister":
        owner.register_resource(resource)
        owner.retire()
        resource.release.set()
        await asyncio.gather(owner.drain(), return_exceptions=True)
    elif entry in {"check", "run"}:
        owner.retire()
    expected = api.CaptureIOUnsupported if entry == "scope" else api.CaptureIOCleanupError if entry == "unregister" else api.CaptureIOClosed
    private_marker = "private-backend-value-must-not-appear"

    async def operation():
        raise AssertionError("Retired factory must not be invoked")

    try:
        try:
            raise ValueError(private_marker)
        except ValueError:
            with pytest.raises(expected) as caught:
                if entry == "check":
                    owner.check_open()
                elif entry == "run":
                    await owner.run(operation)
                elif entry == "scope":
                    with api.capture_io_scope(object()):
                        raise AssertionError("An invalid host scope must not enter")
                else:
                    owner.unregister_resource(resource)
        assert private_marker not in "".join(traceback.format_exception(caught.value))
    finally:
        resource.release.set()
        owner.retire()
        await asyncio.gather(owner.drain(), return_exceptions=True)


@pytest.mark.asyncio
async def test_deferred_release_continuation_follows_all_started_operation_units(api):
    owner = api.CaptureIOOwner()
    entered, release = asyncio.Event(), asyncio.Event()
    order = []

    class ReleaseContinuation:
        def retire(self):
            pass  # It releases a known reservation, not an idle native cursor.

        async def drain(self):
            order.append("reservation-release")

    async def operation():
        entered.set()
        await release.wait()
        order.append("operation-settled")

    owner.register_resource(ReleaseContinuation())
    running = asyncio.create_task(owner.run(operation))
    draining = None
    try:
        await asyncio.wait_for(entered.wait(), 3)
        owner.retire()
        draining = asyncio.create_task(owner.drain())
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        release.set()
        await asyncio.gather(running, draining)
        assert order == ["operation-settled", "reservation-release"]
    finally:
        release.set()
        owner.retire()
        await asyncio.gather(*(task for task in (running, draining) if task is not None), return_exceptions=True)
        await owner.drain()


@pytest.mark.asyncio
async def test_retirement_triggered_idle_close_can_unblock_an_admitted_unit(api):
    owner = api.CaptureIOOwner()
    entered, unlocked = asyncio.Event(), asyncio.Event()

    class IdleCursor(RetainedResource):
        async def close(self):
            await super().close()
            unlocked.set()

    resource = IdleCursor()
    resource.release.set()
    owner.register_resource(resource)

    async def operation():
        entered.set()
        await unlocked.wait()
        return 37

    running = asyncio.create_task(owner.run(operation))
    draining = None
    try:
        await asyncio.wait_for(entered.wait(), 3)
        owner.retire()
        draining = asyncio.create_task(owner.drain())
        done, _ = await asyncio.wait({running, draining}, timeout=1)
        assert done == {running, draining}, "Waiting for operations must not delay the independent idle-cursor close"
        assert await running == 37
        await draining
        assert resource.closed.is_set() and resource.close_attempts == 1
    finally:
        unlocked.set()
        resource.release.set()
        owner.retire()
        await asyncio.gather(*(task for task in (running, draining) if task is not None), return_exceptions=True)
        await owner.drain()
