"""Closed metadata variants for saved-work editor custody."""

from app.telegram_bots.producer import ProductSourceRetired


def hex_identity(value, size=32):
    return type(value) is str and len(value) == size and all(ch in "0123456789abcdef" for ch in value)


def read_origin(value):
    if (
        type(value) is not dict
        or set(value) != {"callback_id", "anchor_outbox_id", "presentation_version", "signature"}
        or not hex_identity(value["callback_id"])
        or not hex_identity(value["anchor_outbox_id"])
        or not hex_identity(value["signature"], 64)
        or type(value["presentation_version"]) is not int
        or value["presentation_version"] < 0
    ):
        raise ProductSourceRetired()
    return value


def read_editor(value):
    if value is None:
        return None
    if type(value) is not dict or set(value) != {"version", "prompt_id", "draft_id", "origin"} or type(value["version"]) is not int or value["version"] != 1 or not hex_identity(value["prompt_id"]) or not hex_identity(value["draft_id"]):
        raise ProductSourceRetired()
    read_origin(value["origin"])
    return value


def read_input(value):
    if value is None:
        return None
    if (
        type(value) is not dict
        or set(value) != {"version", "input_id", "phase", "claim_id"}
        or type(value["version"]) is not int
        or value["version"] != 1
        or not hex_identity(value["input_id"])
        or type(value["phase"]) is not str
        or value["phase"] not in {"captured", "extracting", "failed", "completed"}
        or (value["claim_id"] is not None if value["phase"] == "captured" else not hex_identity(value["claim_id"]))
    ):
        raise ProductSourceRetired()
    return value


def read_incoming(value):
    if (
        type(value) is not dict
        or set(value) != {"link_id", "chat_id", "message_id", "update_id", "binding_generation", "message_thread_id", "input_signature"}
        or not hex_identity(value["link_id"])
        or not hex_identity(value["input_signature"], 64)
        or type(value["chat_id"]) is not int
        or value["chat_id"] == 0
        or any(type(value[key]) is not int or value[key] < minimum for key, minimum in (("message_id", 1), ("update_id", 0), ("binding_generation", 0)))
        or value["message_thread_id"] is not None
        and (type(value["message_thread_id"]) is not int or value["message_thread_id"] < 1)
    ):
        raise ProductSourceRetired()
    return value
