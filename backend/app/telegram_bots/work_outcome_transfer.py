"""Durable presentation ownership, distinct from an acknowledged outcome."""

from collections.abc import Mapping

from sqlalchemy import or_, select

from app.telegram_bots.producer import ProductSourceRetired
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow


def transfer_receipt(value):
    if not isinstance(value, Mapping) or value.get("version") != 2:
        return None
    if "request_activation" in value:
        from app.telegram_bots.work_request_continuations import request_activation_value

        request_activation_value(value)
        value = {"version": value["version"], "work_outcome_transfer": value.get("work_outcome_transfer")}
    if type(value["version"]) is not int or set(value) != {"version", "work_outcome_transfer"}:
        raise ValueError("invalid-work-outcome-transfer")
    saved = value["work_outcome_transfer"]
    if not isinstance(saved, Mapping) or set(saved) != {"phase", "outbox_id"}:
        raise ValueError("invalid-work-outcome-transfer")
    phase, identity = saved["phase"], saved["outbox_id"]
    if (
        not isinstance(phase, (list, tuple))
        or len(phase) != 3
        or phase[0] not in {"conflict", "refused"}
        or phase[1] is not None
        and type(phase[1]) is not str
        or type(phase[2]) is not int
        or phase[2] < 0
        or type(identity) is not str
        or len(identity) != 32
        or any(char not in "0123456789abcdef" for char in identity)
    ):
        raise ValueError("invalid-work-outcome-transfer")
    return saved


def transferred(act):
    saved = transfer_receipt(act.continuation)
    return saved is not None and list(saved["phase"]) == [act.state, act.reason, act.attempts]


async def transfer_outcome(writer, link, act_id, outbox_id, *, outbox, input_origin=None):
    """Called only in the source-qualified writer that publishes its successor."""
    act = await writer.get(TelegramActRow, act_id, with_for_update=True, populate_existing=True)
    if act is None or act.state not in {"conflict", "refused"}:
        return
    parent = await writer.get(TelegramOutboxRow, outbox_id, populate_existing=True)
    work_id = parent.work_id if parent is not None else None
    if parent is not None and work_id is None and input_origin is not None:
        basis = parent.payload.get("product_basis", {})
        if basis.get("origins") != [[input_origin.link_id, input_origin.message_id, input_origin.chat_id]]:
            raise ProductSourceRetired()
        work_id = input_origin.route_basis["work_input"]["work_id"]
    if (
        parent is None
        or (act.link_id, act.owner_user_id, act.binding_generation, act.chat_id, act.instance_id, act.work_id) != (link.id, link.owner_user_id, link.binding_generation, link.chat_id, link.agent_ref, work_id)
        or (parent.link_id, parent.owner_user_id, parent.binding_generation, parent.chat_id, parent.instance_id)
        != (link.id, link.owner_user_id, link.binding_generation, link.chat_id, link.agent_ref if parent.instance_id is not None else None)
    ):
        raise ProductSourceRetired()
    from app.telegram_bots.work_outcome_delivery import retire_outcome

    previous = list(
        await writer.scalars(
            select(TelegramOutboxRow)
            .where(
                TelegramOutboxRow.link_id == link.id,
                TelegramOutboxRow.binding_generation == link.binding_generation,
                TelegramOutboxRow.payload["work_outcome_basis"]["context"]["act"]["id"].as_string() == act.id,
            )
            .order_by(TelegramOutboxRow.id)
            .with_for_update()
        )
    )
    for old in previous:
        await retire_outcome(outbox, writer, link, old)
    await outbox.inbox.settle_deliveries(link_id=link.id, session=writer)
    saved = {key: value for key, value in (act.continuation or {}).items() if key in {"request_activation", "activation_continuation"}}
    act.continuation = {**saved, "version": 2, "work_outcome_transfer": {"phase": [act.state, act.reason, act.attempts], "outbox_id": outbox_id}}
    await writer.flush()


async def transfer_input_outcomes(writer, link, origin, outbox_id, *, outbox):
    """Keep direct and confirmation-owned refusals behind this actual input."""
    callback, parent, act = TelegramCallbackRow, TelegramOutboxRow, TelegramActRow
    operation = origin.operation_ids[0]
    confirmations = (
        select(callback.id)
        .join(parent, parent.id == callback.payload["anchor_outbox_id"].as_string())
        .where(
            callback.link_id == link.id,
            callback.chat_id == link.chat_id,
            callback.binding_generation == link.binding_generation,
            parent.link_id == link.id,
            parent.chat_id == link.chat_id,
            parent.binding_generation == link.binding_generation,
            parent.payload["work_request_basis"]["input"]["operation_id"].as_string() == operation,
            callback.payload["work_action_id"].as_string() == callback.id,
        )
    )
    identities = list(
        await writer.scalars(
            select(act.id).where(
                act.link_id == link.id,
                act.binding_generation == link.binding_generation,
                act.chat_id == link.chat_id,
                act.work_id == origin.route_basis["work_input"]["work_id"],
                act.state.in_(("conflict", "refused")),
                or_(act.id == operation, act.id.in_(confirmations)),
            )
        )
    )
    for identity in identities:
        await transfer_outcome(writer, link, identity, outbox_id, outbox=outbox, input_origin=origin)
