"""Finite read/cancel recovery for an actual original publication, never creation."""

from app.channels.execution_client import ChannelExecutionFacade
from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.publication import channel_execution_scope
from app.telegram_bots.turns import _StopAdmissionGuard
from app.telegram_bots.work_command_stop_answers import AnswerCancelAdmission


class ColdCancelAdmission:
    def __init__(self, original, check, current):
        self.original, self.check, self.current_guard = original, check, current

    def current(self, operation):
        self.current_guard()
        return self.original.current(operation)

    async def authorize(self, operation):
        self.current_guard()
        if operation == "run_cancel":
            await self.check()
        await self.original.authorize(operation)
        if operation == "run_cancel":
            await self.check()
        self.current_guard()

    async def begin_cancel(self, run_id):
        await self.check.enter(run_id)
        self.current_guard()


async def observe_historical(answers, link, origin, basis, record, *, guard):
    """A delayed choice may observe its old run, never cancel a successor."""
    host = answers.owner.host
    attempt = await host.publication_authority.capture(link, thread_id=record.scope.thread_id, endpoint_id=record.scope.endpoint_id)
    facade = ChannelExecutionFacade(host.thread_client, scope=channel_execution_scope(record), admission=attempt.admission)
    run = await facade.lookup_run()
    guard.current()
    if run is None or run.row["status"] not in {"success", "error", "timeout", "interrupted"} or record.run_id not in (None, run.row["run_id"]):
        raise ProductSourceRetired()
    return await answers.notice(link, origin, basis, record, reason="ended", guard=guard)


async def recover_published(answers, link, origin, basis, record, *, guard):
    owner, host = answers.owner, answers.owner.host
    stop_id = f"command:{origin.chat_id}:{origin.message_id}"

    check = AnswerCancelAdmission(answers, link, origin, basis, guard)
    attempt = await host.publication_authority.capture(link, thread_id=record.scope.thread_id, endpoint_id=record.scope.endpoint_id)
    if attempt.cancel_guard is None:
        raise ProductSourceRetired()
    async with owner.callbacks.writer(link, guard) as reader:
        await check.receipt(session=reader)
        marker = await answers.stop_marker(reader, basis)
    if marker == stop_id:
        # A committed barrier may outlive either process or damaged admission
        # metadata. Native terminal reads settle it without fresh cancel consent.
        readonly = ChannelExecutionFacade(host.thread_client, scope=channel_execution_scope(record), admission=attempt.admission, capture_output=True, capture_inventory=True, inventory_document_version=2)
        original = await readonly.lookup_run()
        guard.current()
        if original is not None and record.run_id not in (None, original.row["run_id"]):
            raise ProductSourceRetired()
        if original is not None and original.row["status"] in {"success", "error", "timeout", "interrupted"}:
            if (original.row["status"], original.row.get("stop_reason")) != ("interrupted", None):
                return await answers.notice(link, origin, basis, record, reason="ended", guard=guard)
            output = await readonly.get_run_output(original.row["run_id"])
            if output is None or output.output.state != "unavailable" or output.output.reason != "cancelled":
                return
            receipt_guard = _StopAdmissionGuard(attempt.read_guard, check.receipt)
            record = await owner.outbox.inbox.bind_publication_run(record, run_id=original.row["run_id"], guard=receipt_guard)
            await owner.outbox.plan_publication_outcome(
                record, run=original, cancelled_output=output, stop_id=stop_id, source_ref={"key": "stop.answer", "values": {"written": False}}, keyboard=(), callbacks=owner.callbacks, guard=receipt_guard
            )
            return await answers.complete_receipt(link, origin, guard=guard)
    try:
        await check.admit()
    except ProductSourceRetired:
        if not check.expired_unentered:
            raise
        if marker == stop_id:
            await owner.outbox.inbox.clear_publication_stop(record, stop_id=stop_id, guard=_StopAdmissionGuard(attempt.cancel_guard, check.no_call))
        return await answers.notice(link, origin, basis, record, reason="expired", guard=guard)
    source_guard = _StopAdmissionGuard(attempt.cancel_guard, check)
    fresh = await owner.outbox.inbox.request_publication_stop(record, stop_id=stop_id, guard=source_guard)
    if not fresh.dispatch_started:
        await owner.outbox.plan_cancelled_before_dispatch(fresh, stop_id=stop_id, guard=source_guard, callbacks=owner.callbacks)
        return await answers.complete_receipt(link, origin, guard=guard)
    located = await host.publications.locate_run(fresh, link, max_pages=8)
    guard.current()
    if located.state != "matched":
        return
    fresh, run = located.record, located.run
    facade = ChannelExecutionFacade(host.thread_client, scope=channel_execution_scope(fresh), admission=ColdCancelAdmission(attempt.admission, check, guard.current), capture_output=True, capture_inventory=True, inventory_document_version=2)
    result = await facade.cancel_run(run.row["run_id"])
    if result == "refused":
        await owner.outbox.inbox.clear_publication_stop(fresh, stop_id=stop_id, guard=source_guard)
        return await answers.notice(link, origin, basis, fresh, reason="refused", guard=guard)
    if result == "terminal":
        return await answers.notice(link, origin, basis, fresh, reason="ended", guard=guard)
    # A cancellation request does not prove settlement. The source endpoint
    # supplies the exact native terminal output; it grants no preview ranges.
    output = await facade.get_run_output(run.row["run_id"])
    if output is None or output.output.state != "unavailable" or output.output.reason != "cancelled":
        return
    settled = await facade.lookup_run()
    if settled is None or (settled.row["status"], settled.row.get("stop_reason")) != ("interrupted", None):
        return
    await owner.outbox.plan_publication_outcome(
        fresh,
        run=settled,
        cancelled_output=output,
        stop_id=stop_id,
        source_ref={"key": "stop.answer", "values": {"written": False}},
        keyboard=(),
        callbacks=owner.callbacks,
        guard=source_guard,
    )
    await answers.complete_receipt(link, origin, guard=guard)
