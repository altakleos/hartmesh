"""Closed full-detail carriers; callers must qualify their actual source owner."""

from app.telegram_bots.producer import ProductSourceRetired

DETAIL_INTENTS = frozenset({"work_view_intent", "work_management_intent", "work_execution_intent"})


def detail_card_owner(item, owners):
    found = DETAIL_INTENTS.intersection(item.payload)
    if len(found) != 1 or not found.issubset(owners):
        raise ProductSourceRetired()
    return owners[next(iter(found))]


def full_detail(source):
    """A canonical frozen recipe, not an arbitrary dict, proves this content."""
    key, data = (source["key"], source["work"]) if type(source) is dict else (source.key, source.work)
    if not (
        key == "view.detail"
        or key in {"management.edit", "management.withdraw"}
        and data.get("phase") in {"saved", "changed", "back"}
        or key in {"execution.activate", "execution.stop"}
        and data.get("notice", {}).get("preview") is True
        or key == "assignment.card"
        and data.get("phase") == "saved"
        or key == "capacity.offer"
    ):
        raise ProductSourceRetired()
    detail = data.get("detail")
    if type(detail) is not dict or type(detail.get("record")) is not dict:
        raise ProductSourceRetired()
    return detail
