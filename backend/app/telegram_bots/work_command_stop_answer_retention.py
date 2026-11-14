"""Historical answer evidence pins carry no live cancellation authority."""

from types import SimpleNamespace

from app.telegram_bots.inbox import _callback_owner
from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.work_command_stop_answers import answer_receipt_delivery, read_answer, selected_answer
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow


def scope(row, mark, basis, *, delivery=False):
    value = (row.link_id, row.chat_id, row.binding_generation, row.message_thread_id) == (mark.link_id, mark.chat_id, mark.binding_generation, mark.message_thread_id)
    return value and (not delivery or row.owner_user_id == basis["owner_user_id"] and row.instance_id in (None, basis["instance_id"]))


def belongs(row, answer):
    if type(row.payload) is not dict:
        return False
    expected = {(value["link_id"], value["message_id"], value["chat_id"]) for value in answer["origins"]}
    for key in ("reply_basis", "turn_outcome_basis"):
        data = row.payload.get(key)
        publication = data.get("publication") if type(data) is dict else None
        if type(publication) is dict:
            origins = publication.get("origins")
            if type(origins) is list and {(value["link_id"], value["message_id"], value["chat_id"]) for value in origins} == expected and publication.get("owner_callback_id") == answer["callback_id"]:
                return True
    product = row.payload.get("product_basis")
    if answer["callback_id"] is not None and row.payload.get("input_owner_callback_id") == answer["callback_id"]:
        return type(product) is dict and product.get("origins") == [] and product.get("finish_route") is False and product.get("parts") == [{"kind": "text", "source_ref": {"key": "busy.cancelled", "values": {}}}]
    return type(product) is dict and {tuple(value) for value in product.get("origins", [])} == expected


async def receipt(session, mark, basis):
    if not selected_answer(basis):
        return None
    # This detached historical scope renders/verifies stored recipes only.
    # It is never passed to an admission, SDK or native service.
    historical = SimpleNamespace(id=mark.link_id, owner_user_id=basis["owner_user_id"], binding_generation=mark.binding_generation, chat_id=mark.chat_id, agent_ref=basis["instance_id"])
    return await answer_receipt_delivery(session, historical, basis, mark)


async def pin(session, result, mark, basis):
    try:
        answer = read_answer(basis["answer"], incoming=basis["input"])
        if answer is None:
            return
        for expected in answer["origins"]:
            row = await session.get(TelegramInboundMarkRow, (expected["link_id"], expected["message_id"], expected["chat_id"]), populate_existing=True)
            if row is None or not scope(row, mark, basis) or row.update_id != expected["update_id"]:
                continue
            result.inputs.add((row.link_id, row.message_id, row.chat_id))
            for identity in set(row.delivery_ids) | ({row.outbox_id} if row.outbox_id is not None else set()):
                delivery = await session.get(TelegramOutboxRow, identity)
                if delivery is not None and scope(delivery, mark, basis, delivery=True) and belongs(delivery, answer):
                    result.deliveries.add(delivery.id)
        if answer["callback_id"] is not None:
            control = await session.get(TelegramCallbackRow, answer["callback_id"])
            if control is not None and scope(control, mark, basis):
                _data, stage = _callback_owner(control.payload, control.id)
                if [(row.link_id, row.message_id, row.chat_id) for row in stage.origins] != [(row["link_id"], row["message_id"], row["chat_id"]) for row in answer["origins"]]:
                    raise ProductSourceRetired()
                result.callbacks.add(control.id)
                for identity in _data["delivery_ids"]:
                    delivery = await session.get(TelegramOutboxRow, identity)
                    if delivery is not None and scope(delivery, mark, basis, delivery=True) and belongs(delivery, answer):
                        result.deliveries.add(delivery.id)
        saved = mark.route_basis.get("stop_answer_receipt")
        if type(saved) is dict and type(saved.get("delivery_id")) is str:
            delivery = await session.get(TelegramOutboxRow, saved["delivery_id"])
            if delivery is not None and scope(delivery, mark, basis, delivery=True):
                result.deliveries.add(delivery.id)
    except (ProductSourceRetired, ValueError, TypeError, KeyError, AttributeError):
        return
