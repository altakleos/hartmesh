"""Optional vendor response form. Domain fields never enter a core schema."""

import datetime
import re
from pathlib import Path

from deerflow_extension_api import extension
from deerflow_extension_api.plugins import BackendAction, BrowserAssets, PluginContribution


async def get_request(payload, context):
    if set(payload) != {"request_id"}:
        raise ValueError("Expected a request ID only")
    if context.human_input is None:
        raise NotImplementedError("Specialized validation unavailable; use Attention")
    return await context.human_input.call("get", payload)


def validate_quote(payload):
    fields = {"request_id", "operation_id", "expected_request_revision", "expected_assignment_revision", "supplier", "delivery", "currency", "quoted_total"}
    if set(payload) != fields:
        raise ValueError("Invalid quote response fields")
    for name, maximum in (("supplier", 120), ("delivery", 10), ("currency", 3), ("quoted_total", 16)):
        value = payload[name]
        if not isinstance(value, str) or not value.strip() or len(value) > maximum or any(ord(c) < 32 or ord(c) == 127 for c in value):
            raise ValueError("Invalid " + name)
    if not re.fullmatch(r"[A-Z]{3}", payload["currency"]) or not re.fullmatch(r"[0-9]{1,10}\.[0-9]{2}", payload["quoted_total"]):
        raise ValueError("Supply uppercase currency and a nonnegative total with two decimal places")
    if not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", payload["delivery"]):
        raise ValueError("Use delivery date YYYY-MM-DD")
    datetime.date.fromisoformat(payload["delivery"])


async def respond(payload, context):
    try:
        validate_quote(payload)
    except ValueError as error:
        return {"status": "invalid", "message": str(error)[:160]}
    if context.human_input is None:
        raise NotImplementedError("Specialized validation unavailable; use Attention")
    # Reload current source through the host before validation and canonical append.
    request = await context.human_input.call("get", {"request_id": payload["request_id"]})
    if request["purpose"] != "information":
        return {"status": "invalid", "message": "This form supplies facts only; use Attention for decisions and reviews"}
    text = f"Supplier: {payload['supplier']}\nDelivery: {payload['delivery']}\nQuoted total: {payload['currency']} {payload['quoted_total']}\nExample quote form checked field format only; commercial facts await AI employee assessment."
    receipt = await context.human_input.call("respond", {"request_id": payload["request_id"], "body": {key: payload[key] for key in ("operation_id", "expected_request_revision", "expected_assignment_revision")} | {"text": text}})
    return {"status": "supplied", "receipt": receipt, "validation": "Example checked field format only; commercial facts await AI employee assessment."}


def contribution(*, enabled=True):
    return PluginContribution(
        namespace="example.work-input",
        title="Quote response example",
        enabled=enabled,
        api_version=5,
        human_input_api_version=1,
        frontend=BrowserAssets("work-input.v1", Path(__file__).parent),
        backend=(BackendAction("get", get_request), BackendAction("respond", respond)),
    )


@extension(api="0.2.7", name="work-input")
def install(registry, config):
    if not registry.plugin(contribution(enabled=config.get("enabled", True))):
        raise RuntimeError("This host does not support the required human-input facade")
