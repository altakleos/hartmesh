"""Retire the mounted rehearsal's ordinary chat through the authenticated Gateway."""

import json
import time

# Read only effective numeric timings and the store type inside the running image.
# The rendered config, Redis address and credentials do not leave the container.
POLICY = """
import json
from app.gateway.storage_spaces import RECHECK_SECONDS, STARTUP_BUDGET_SECONDS
from deerflow.community.aio_sandbox.ownership.factory import compute_lease_ttl, resolve_ownership_config
from deerflow.community.warm_pool_lifecycle import DEFAULT_IDLE_TIMEOUT
from deerflow.config import get_app_config
config = get_app_config()
policy = resolve_ownership_config(config.sandbox.ownership, stream_bridge=config.stream_bridge)
idle = getattr(config.sandbox, "idle_timeout", None)
print(json.dumps({"store": policy.type, "lease_seconds": compute_lease_ttl(policy), "recheck_seconds": RECHECK_SECONDS,
                  "startup_seconds": STARTUP_BUDGET_SECONDS, "idle_seconds": DEFAULT_IDLE_TIMEOUT if idle is None else idle}))
"""


def retire_reader(compose, session, reader, container, evidence, *, wait_ready):
    policy = json.loads(compose.gateway("python", "-c", POLICY))
    assert policy["store"] in ("memory", "redis")
    # A crashed Redis owner can keep its lease for one TTL. After expiry the
    # next scan must observe absence for another full TTL before claiming it.
    grace = 2 * policy["lease_seconds"] if policy["store"] == "redis" else 0
    budget = grace + 2 * policy["recheck_seconds"] + policy["startup_seconds"]
    receipt = evidence["reader_retirement"] = {"policy": policy, "eligibility_budget_seconds": budget}
    started = time.monotonic()
    try:
        wait_ready(session, timeout=budget)
    finally:
        receipt["eligibility_wait_seconds"] = time.monotonic() - started

    def present():
        return container in compose.invoke(["docker", "ps", "-aq", "--no-trunc"]).stdout.splitlines()

    receipt["container_present_before_delete"] = present()
    started = time.monotonic()
    try:
        assert session.request("DELETE", "/api/threads/" + reader).json()["success"] is True
    finally:
        receipt["authenticated_delete_seconds"] = time.monotonic() - started
    session.request("GET", "/api/langgraph/threads/" + reader, expected=404)
    receipt["container_absent"] = not present()
    assert receipt["container_absent"], "the deleted reader's original sandbox is still present"
    receipt["execution_after"] = wait_ready(session)["execution"]
