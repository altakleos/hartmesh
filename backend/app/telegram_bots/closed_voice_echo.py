"""A previously acknowledged transcript remains evidence, never a second echo."""

from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_outcome_delivery import acknowledged_parts
from app.telegram_bots.work_result_identity import digest


def echo_identity(mark):
    value = mark.route_basis.get("closed_voice_echo")
    if value is None:
        return None
    if (
        type(value) is not dict
        or set(value) != {"version", "outbox_id", "signature"}
        or type(value["version"]) is not int
        or value["version"] != 1
        or any(type(value[key]) is not str or len(value[key]) != size or any(char not in "0123456789abcdef" for char in value[key]) for key, size in (("outbox_id", 32), ("signature", 64)))
    ):
        raise ProductSourceRetired()
    return value["outbox_id"]


def echo_recipe(mark):
    from app.telegram_bots.voice import _echo

    return {"kind": "text", "reply_to_message_id": mark.message_id, "source_ref": {"key": "voice.heard", "values": {"text": _echo(mark.sanitized_input["text"])}}}


def qualify_echo(link, mark, echo):
    if (
        echo is None
        or "voice_note" not in mark.route_basis
        or (echo.link_id, echo.owner_user_id, echo.binding_generation, echo.chat_id, echo.message_thread_id, echo.instance_id, echo.work_id, echo.request_id, echo.event_id, echo.class_)
        != (link.id, link.owner_user_id, link.binding_generation, mark.chat_id, mark.message_thread_id, None, None, None, None, "reply")
        or echo.dedupe_key != f"notice:voice-heard:{mark.chat_id}:{mark.message_id}"
        or echo.state != "sent"
        or echo.payload.get("product_basis", {}).get("origins") != [[link.id, mark.message_id, mark.chat_id]]
        or not same_body(echo.payload.get("product_basis", {}).get("parts"), [echo_recipe(mark)])
        or type(echo.message_id) is not int
        or echo.message_id <= 0
        or len(echo.parts) != 1
        or type(echo.parts[0].get("protect_content")) is not bool
        or echo.parts[0].get("message_ids") != [echo.message_id]
    ):
        raise ProductSourceRetired()
    acknowledged_parts(echo, [{**echo_recipe(mark), "protect_content": echo.parts[0]["protect_content"]}])
    value = {"version": 1, "outbox_id": echo.id, "signature": digest([echo.id, echo.payload.get("product_basis"), echo.parts])}
    if echo_identity(mark) is not None and not same_body(value, mark.route_basis["closed_voice_echo"]):
        raise ProductSourceRetired()
    return value
