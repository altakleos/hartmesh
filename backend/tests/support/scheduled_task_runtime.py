"""Test adapter for legacy scheduler callback fakes."""

from __future__ import annotations

from types import SimpleNamespace

from app.runtime.invocation import InternalLaunchReceipt


class CallbackInvocationRuntime:
    """Expose a callback fake through the scheduler's InvocationRuntime seam."""

    def __init__(self, callback) -> None:
        self._callback = callback

    async def launch(self, intent):
        message = intent.input["messages"][0]
        result = await self._callback(
            thread_id=intent.thread_id,
            assistant_id=intent.assistant_id,
            prompt=message["content"],
            owner_user_id=intent.owner_user_id,
            metadata={
                "scheduled_task_id": intent.trusted_task_id,
                "scheduled_task_run_id": intent.task_run_id,
                "scheduled_trigger": intent.scheduled_trigger,
            },
            context=intent.context,
            on_disconnect=intent.on_disconnect,
            multitask_strategy=intent.multitask_strategy,
        )
        record = SimpleNamespace(thread_id=result.get("thread_id"))
        if "run_id" in result:
            record.run_id = result["run_id"]
        return InternalLaunchReceipt(record=record)


class NeverLaunchInvocationRuntime:
    async def launch(self, _intent):
        raise AssertionError("InvocationRuntime launch was not expected")
