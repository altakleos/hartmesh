"""Fixed cold Stop receipts borrow native run/output and persisted Stop proof."""

from types import MappingProxyType

from app.channels.execution_client import ValidatedRun
from app.gateway.channel_execution import receipt_from_run_kwargs
from app.gateway.run_output import RunOutputProjectionV2
from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.work_actions import same_body


def cancelled_stop_proof(record, run, output, stop_id, source):
    proof = {"kind": "cancelled_stop", "row": dict(run.row), "output": output.model_dump(mode="json"), "stop_id": stop_id}
    read_cancelled_stop(record, proof, source)
    return proof


def read_cancelled_stop(record, proof, source):
    if type(proof) is not dict or set(proof) != {"kind", "row", "output", "stop_id"} or proof["kind"] != "cancelled_stop" or not same_body(source, {"key": "stop.answer", "values": {"written": False}}):
        raise ProductSourceRetired()
    run = ValidatedRun(MappingProxyType(proof["row"]), receipt_from_run_kwargs(proof["row"].get("kwargs")))
    output = RunOutputProjectionV2.model_validate(proof["output"]).output
    scope, channel = record.scope, output.channel_execution
    if (
        type(proof["stop_id"]) is not str
        or not proof["stop_id"]
        or len(proof["stop_id"]) > 128
        or not proof["stop_id"].isprintable()
        or (run.row["status"], run.row.get("stop_reason")) != ("interrupted", None)
        or run.receipt is None
        or (run.receipt.publication_id, run.receipt.owner_user_id, run.receipt.thread_id, run.receipt.assistant_id, run.receipt.agent_name)
        != (record.publication_id, scope.owner_user_id, scope.thread_id, scope.assistant_id, scope.agent_name)
        or (run.row["run_id"], run.row["thread_id"]) != (record.run_id, scope.thread_id)
        or (output.state, output.reason, output.owner_user_id, output.thread_id, output.run_id) != ("unavailable", "cancelled", scope.owner_user_id, scope.thread_id, record.run_id)
        or (channel.publication_id, channel.owner_user_id, channel.thread_id, channel.assistant_id, channel.agent_name) != (record.publication_id, scope.owner_user_id, scope.thread_id, scope.assistant_id, scope.agent_name)
    ):
        raise ProductSourceRetired()
