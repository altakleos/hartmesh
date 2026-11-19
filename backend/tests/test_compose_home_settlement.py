"""Measure interrupted Home writes on the hosted, mounted PostgreSQL bundle."""

import hashlib
import json
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

if os.environ.get("GITHUB_ACTIONS") != "true" or os.environ.get("HARTMESH_REQUIRE_BUNDLE_INSTANCES") != "1":
    pytest.skip("requires the hosted mounted bundle", allow_module_level=True)

from test_compose_instance_acceptance import MARKER, artifact, await_attempt, await_execution_ready, container_absent, eventually, held_attempt, new_conversation, read_workspace_attachment, required, sql, work_path, workspace
from test_compose_instance_acceptance import compose as compose
from test_compose_instance_acceptance import session as session

pytestmark = pytest.mark.live


def interrupt_write(compose, session, space, generation, container):
    """Refuse only this writer's final SQL update, after the real file step."""
    operation = uuid.uuid4().hex
    trigger = "acceptance_write_" + operation
    path = "outputs/" + operation + ".txt"
    predicate = f"space_id={sql(space)} AND operation_id={sql(operation)}"
    compose.psql(
        f"CREATE FUNCTION {trigger}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic final commit refusal'; END $$; "
        f"CREATE TRIGGER {trigger} BEFORE UPDATE OF phase ON storage_space_file_operations FOR EACH ROW "
        f"WHEN (OLD.space_id={sql(space)} AND OLD.operation_id={sql(operation)} AND NEW.phase='complete' AND NEW.result->>'resolution' IS NULL) EXECUTE FUNCTION {trigger}()"
    )
    try:
        response = session.request(
            "PUT",
            f"/api/spaces/{space}/content",
            params={"operation_id": operation, "generation": generation, "path": path, "create": True},
            content=MARKER,
            expected=409,
        )
        cutoff = time.monotonic()
        assert response.json()["detail"]["code"] == "storage-busy"
        assert compose.psql("SELECT phase FROM storage_space_file_operations WHERE " + predicate) == "pending"
        assert compose.invoke(["docker", "exec", container, "cat", "/mnt/spaces/home/" + path]).stdout.encode() == MARKER
    finally:
        compose.psql(f"DROP TRIGGER {trigger} ON storage_space_file_operations; DROP FUNCTION {trigger}()")
    return operation, predicate, cutoff


@pytest.mark.parametrize("workload", ["idle", "assignment", "restart"])
def test_idle_workspace_releases_interrupted_write(compose, session, workload):
    await_execution_ready(session)
    agent = json.loads(compose.psql(f"SELECT json_build_object('id',id,'home_id',home_id) FROM agent_instances WHERE status='active' AND custody='personal' AND owner_id={sql(session.user['id'])} AND name='Qualification AI assistant'"))
    context = new_conversation(session, {"instance": agent})
    session.run(context["thread_id"])
    identity, _ = workspace(compose, context)
    held = None
    if workload == "assignment":
        held, active = held_attempt(compose, session, context)
        assert active == identity
    space = agent["home_id"]
    generation = session.request("GET", f"/api/spaces/{space}").json()["generation"]
    operation, predicate, cutoff = interrupt_write(compose, session, space, generation, identity["container_id"])
    eligible = cutoff
    if workload == "restart":
        compose.run("kill", "-s", "KILL", "gateway")
        assert workspace(compose, context)[0] == identity
        compose.run("up", "-d", "--wait", "--no-deps", "gateway", timeout=240)
    if held:
        # Let two ordinary maintenance ticks pass with the assignment active.
        time.sleep(125)
        assert workspace(compose, context)[0] == identity
        assert compose.psql("SELECT phase FROM storage_space_file_operations WHERE " + predicate) == "pending"
        stopped = session.request(
            "POST",
            work_path(context, held["id"]) + "/commands",
            json={"operation_id": uuid.uuid4().hex, "expected_revision": held["revision"], "expected_assignment_revision": held["assignment_revision"], "action": "stop", "attempt_id": held["attempt"]["id"]},
        ).json()
        assert stopped["attempt"]["id"] == held["attempt"]["id"]
        await_attempt(session, context, held["id"], {"cancelled"})
        eligible = time.monotonic()
    expected_digest = hashlib.sha256(MARKER).hexdigest()

    def settled():
        return compose.psql("SELECT phase,result->>'resolution',result->>'value' FROM storage_space_file_operations WHERE " + predicate) == "complete|host-settled|" + expected_digest

    eventually(settled, timeout=max(1, eligible + 150 - time.monotonic()), interval=1)
    confirmed = time.monotonic()
    assert confirmed - eligible < 150
    original = read_workspace_attachment(compose, identity["attachment_id"], timeout=10)
    assert original["phase"] == "fenced" and original["fence_reason"] == "outcome-uncertain"
    assert container_absent(compose, identity["container_id"])
    await_execution_ready(session)
    thread = new_conversation(session, context)["thread_id"]

    def fresh_turn():
        submitted = time.monotonic()
        turn = session.run(thread)
        return submitted, time.monotonic(), turn

    def fresh_workspace():
        identifier = compose.psql("SELECT id FROM storage_space_attachments WHERE actor_kind='nonhuman' AND actor_id=" + sql("agent:" + agent["id"]) + " AND phase='active'")
        return workspace(compose, context)[0] if identifier else None

    with ThreadPoolExecutor(max_workers=1) as client:
        future = client.submit(fresh_turn)
        fresh = eventually(fresh_workspace, timeout=120, interval=0.25)
        ready = time.monotonic()
        submitted, answered, run = future.result(timeout=120)
    artifact(session, thread)
    assert fresh["attachment_id"] != identity["attachment_id"]
    receipt = {
        "backend": required("BACKEND_REF"),
        "database": "postgresql",
        "workload": workload,
        "operation": operation,
        "original": identity,
        "fresh": fresh,
        "run_id": run["run_id"],
        "cutoff_to_settlement_seconds": confirmed - cutoff,
        "eligible_to_settlement_seconds": confirmed - eligible,
        "submission_to_observed_fresh_workspace_seconds": ready - submitted,
        "settlement_to_completed_answer_seconds": answered - confirmed,
    }
    (Path(required("EVIDENCE")) / f"workspace-settlement-{workload}.json").write_text(json.dumps(receipt, indent=2), encoding="utf-8")
