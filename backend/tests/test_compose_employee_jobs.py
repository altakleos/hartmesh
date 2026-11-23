"""Exercise overlapping conversation jobs on the installed disposable bundle."""

import json
import os
import time
import uuid
from pathlib import Path

import pytest

if os.environ.get("GITHUB_ACTIONS") != "true" or os.environ.get("HARTMESH_VERIFY_AGENT_JOBS") != "1":
    pytest.skip("requires the mounted employee job rehearsal", allow_module_level=True)

from _compose_browser_session import verify_image
from _compose_connection_evidence import confirmed_browser_notice
from _compose_gateway_restart import start_gateway, stop_gateway
from _compose_shared_reader import POLICY
from test_compose_agent_connections import REGISTRY, create_employees, features, notice_services, replace_option, server, set_grant
from test_compose_instance_acceptance import Person, await_execution_ready, eventually, required, sql
from test_compose_instance_acceptance import compose as compose
from test_compose_instance_acceptance import member as member
from test_compose_instance_acceptance import session as session

pytestmark = pytest.mark.live


def start_job(person, thread, kind, token):
    source = person.run(thread, helpers=True, message=f"acceptance:job:{kind}:submit:{token}")
    rows = person.request("GET", f"/api/threads/{thread}/subagent-batches").json()
    (row,) = [row for row in rows if row["title"] == "Review sample " + kind + " " + token]
    return {"id": row["id"], "thread": thread, "source_run": source["run_id"], "kind": kind}


def terminal(bundle, person, job, token, *, interrupted=False):
    path = f"/api/threads/{job['thread']}/subagent-batches/{job['id']}"

    def check():
        row = person.request("GET", path).json()
        run = row.get("continuation_run_id")
        if not run:
            return None
        if job["kind"] != "personal":
            outcome = row.get("follow_up") or {}
            if outcome.get("state") != ("ended" if interrupted else "done"):
                return None
            if interrupted:
                assert outcome["reason"] == "interrupted"
                return run
        run_path = f"/api/langgraph/threads/{job['thread']}/runs/{run}"
        run_state = person.request("GET", run_path).json()
        if job["kind"] == "replacement" and run == job["original_run"]:
            return None
        assert run_state["status"] not in ("error", "timeout", "interrupted"), run_state
        if run_state["status"] != "success":
            return None
        messages = person.request("GET", f"/api/langgraph/threads/{job['thread']}/state").json()["values"]["messages"]
        events = person.request("GET", run_path + "/messages").json()["data"]
        expected = "Completed " + job["kind"] + " checks " + token
        message_id = confirmed_browser_notice(job["thread"], run, expected, messages, events)
        if not message_id:
            return None
        job["final_message_id"] = message_id
        return run

    job["followup_run"] = eventually(check, timeout=300, interval=2)
    rows = person.request("GET", path + "/items").json()
    expected = ["Checked sample " + token]
    attempts = [1]
    if job["kind"] == "replacement":
        expected.append("Checked remaining " + token)
        attempts.append(2)
    assert len(rows) == len(expected)
    job["items"] = []
    for row, report, attempt in zip(rows, expected, attempts, strict=True):
        item = person.request("GET", path + "/items/" + str(row["position"])).json()
        assert row["id"] == item["id"] and item["attempt"] == attempt and item["status"] == "succeeded" and item["report"] == report
        job["items"].append({k: row[k] for k in ("id", "attempt", "model_name", "token_usage", "status")})
        assert sum((row["token_usage"] or {}).values()) > 0
    if interrupted or job["kind"] == "replacement":
        keys = "native-batch-followup:" + job["id"]
        observed = bundle.psql(f"SELECT run_id FROM runs WHERE thread_id={sql(job['thread'])} AND idempotency_key IN ({sql(keys)},{sql(keys + ':replacement')})")
        expected_ids = {job["followup_run"]} if interrupted else {job["original_run"], job["followup_run"]}
        assert set(observed.splitlines()) == expected_ids
        assert rows[0]["id"] == job["retained_item_id"]
    job["turn_usage"] = {}
    for phase in ("source_run", "followup_run"):
        observed = bundle.psql("SELECT total_input_tokens,total_output_tokens FROM runs WHERE run_id=" + sql(job[phase]))
        job["turn_usage"][phase] = list(map(int, observed.split("|")))
        if phase == "source_run" or not interrupted:
            assert sum(job["turn_usage"][phase]) > 0
    if job["kind"] != "personal":
        events = person.request("GET", f"/api/threads/{job['thread']}/runs/{job['followup_run']}/events", params={"event_types": "middleware:connection_call"}).json()
        facts = [event["content"]["changes"] for event in events]
        assert [x["outcome"] for x in facts] == ([] if job["kind"] == "off" else ["sending", "made"])
        assert all(x["origin"] == "batch_followup" for x in facts)
        job["connection_outcomes"] = [x["outcome"] for x in facts]


def overlap(bundle, jobs):
    observations = {"at": time.time()}
    for kind in ("employee", "personal"):
        job = jobs[kind]
        state = bundle.psql("SELECT b.status,i.status,i.attempt FROM subagent_batches b JOIN subagent_batch_items i ON i.batch_id=b.id WHERE b.id=" + sql(job["id"]))
        assert state == "running|running|1", state
        observations[kind] = state
    return observations


def wait_for_restored_residents(compose, session, evidence):
    policy = json.loads(compose.gateway("python", "-c", POLICY))
    assert policy["store"] == "redis"
    budget = 2 * policy["lease_seconds"] + 2 * policy["recheck_seconds"] + policy["startup_seconds"]
    start = time.monotonic()
    await_execution_ready(session, timeout=budget)
    evidence.setdefault("recoveries", []).append({"ownership": policy, "budget_seconds": budget, "observed_seconds": time.monotonic() - start})


def limits(bundle):
    current, peak, maximum = map(int, bundle.run("exec", "-T", "gateway", "cat", "/sys/fs/cgroup/memory.current", "/sys/fs/cgroup/memory.peak", "/sys/fs/cgroup/memory.max").stdout.split())
    assert 0 < current <= peak <= maximum == 1088 * 1024 * 1024
    settings = json.loads(
        bundle.gateway(
            "python",
            "-c",
            "import json; from deerflow.config import get_app_config; c=get_app_config(); print(json.dumps([c.sandbox.replicas,c.subagent_batches.enabled,c.subagent_batches.agent_max_items,c.subagent_batches.agent_max_work_seconds]))",
        )
    )
    assert settings == [2, True, 50, 900]
    return {"current_bytes": current, "peak_bytes": peak, "limit_bytes": maximum, "sandbox_slots": settings[0], "work_seconds": settings[3]}


def test_installed_conversation_jobs(compose, session, member):
    folder = Path(required("EVIDENCE"))
    token = uuid.uuid4().hex
    armed = [folder / ("armed-" + token + "-" + suffix) for suffix in ("employee", "personal", "final", "replacement")]
    evidence = {"complete": False, "mode": "signed" if required("BACKEND_REF").startswith("ghcr.io/") else "source", "revision": required("FILES_BROWSER_REVISION"), "jobs": {}}
    third = Person(compose)
    try:
        evidence["images"] = {k: verify_image(compose, k, required(v)) for k, v in (("gateway", "BACKEND_REF"), ("frontend", "HARTMESH_BROWSER_FRONTEND"))}
        evidence["limits_before"] = limits(compose)
        third.request("POST", "/api/v1/auth/register", json={"email": "third-worker@example.com", "password": "Acceptance-Third-2026!"}, expected=201)
        third.user = third.request("GET", "/api/v1/auth/me").json()
        assert len({p.user["id"] for p in (third, session, member)}) == 3
        await_execution_ready(session)
        with notice_services(compose) as counts:
            agent, _ = create_employees(session)
            session.request("POST", REGISTRY + "/servers", json={"mcp_servers": {"notices": server("notices-a")}}, expected=(200, 201))
            set_grant(session, agent, "notices", True)
            for marker in armed:
                marker.touch()
            for kind, person, thread in (("employee", session, agent["thread_id"]), ("personal", member, member.thread())):
                evidence["jobs"][kind] = start_job(person, thread, kind, token)
            for kind in ("employee", "personal"):
                eventually((folder / ("waiting-" + token + "-" + kind)).is_file, timeout=150, interval=1)
            assert counts()["notices-a"]["calls"] == 0
            evidence["overlap_before_third"] = overlap(compose, evidence["jobs"])
            tid = third.thread()
            run = third.run(tid, message=f"acceptance:job:third:submit:{token}")
            assert third.request("GET", f"/api/langgraph/threads/{tid}/state").json()["values"]["messages"][-1]["content"] == "The third chat is ready."
            evidence["third"] = {"thread": tid, "run": run["run_id"]}
            evidence["overlap_after_third"] = overlap(compose, evidence["jobs"])
            evidence["limits_overlap"] = limits(compose)
            for marker in armed[:2]:
                marker.unlink()
            for kind, person in (("employee", session), ("personal", member)):
                terminal(compose, person, evidence["jobs"][kind], token)
            assert counts()["notices-a"]["calls"] == 1
            for owner, thread in ((member, evidence["jobs"]["personal"]["thread"]), (third, tid)):
                assert owner.request("DELETE", "/api/threads/" + thread).json()["success"] is True
                owner.request("GET", "/api/langgraph/threads/" + thread, expected=404)
            # A separate job loses its process after the service response while
            # the model's final answer is still held outside the Gateway.
            cut = evidence["jobs"]["cut"] = start_job(session, agent["thread_id"], "cut", token)
            eventually((folder / ("waiting-" + token + "-final")).is_file, timeout=180, interval=1)
            assert counts()["notices-a"]["calls"] == 2
            cut["retained_item_id"] = compose.psql("SELECT id FROM subagent_batch_items WHERE status='succeeded' AND batch_id=" + sql(cut["id"]))
            assert cut["retained_item_id"]
            compose.run("kill", "-s", "SIGKILL", "gateway")
            armed[2].unlink()
            start_gateway(compose, session)
            terminal(compose, session, cut, token, interrupted=True)
            assert counts()["notices-a"]["calls"] == 2
            wait_for_restored_residents(compose, session, evidence)
            replacement = evidence["jobs"]["replacement"] = start_job(session, agent["thread_id"], "replacement", token)
            eventually((folder / ("waiting-" + token + "-replacement")).is_file, timeout=180, interval=1)
            replacement["original_run"] = compose.psql("SELECT continuation_run_id FROM subagent_batches WHERE id=" + sql(replacement["id"]))
            replacement["retained_item_id"] = compose.psql("SELECT id FROM subagent_batch_items WHERE position=0 AND status='succeeded' AND batch_id=" + sql(replacement["id"]))
            assert replacement["original_run"] and replacement["retained_item_id"]
            assert counts()["notices-b"]["calls"] == 0
            session.request("POST", REGISTRY + "/servers", json={"mcp_servers": {"updated": server("notices-b")}}, expected=(200, 201))
            set_grant(session, agent, "notices", False)
            set_grant(session, agent, "updated", True)
            compose.run("kill", "-s", "SIGKILL", "gateway")
            (folder / ("released-" + token + "-replacement")).touch()
            armed[3].unlink()
            start_gateway(compose, session)
            wait_for_restored_residents(compose, session, evidence)
            terminal(compose, session, replacement, token)
            assert counts()["notices-a"]["calls"] == 2 and counts()["notices-b"]["calls"] == 1
            stop_gateway(compose)
            replace_option(compose, "off")
            start_gateway(compose, session)
            assert features(session)["connections"] is False
            await_execution_ready(session)
            off = evidence["jobs"]["off"] = start_job(session, agent["thread_id"], "off", token)
            terminal(compose, session, off, token)
            evidence["counts"] = counts()
            assert evidence["counts"]["notices-a"]["calls"] == 2
            assert evidence["counts"]["notices-b"]["calls"] == 1 and evidence["counts"]["notices-unlisted"]["calls"] == 0
            evidence["limits_after"] = limits(compose)
            images_after = {service: verify_image(compose, service, identity["ref"]) for service, identity in evidence["images"].items()}
            assert images_after == evidence["images"]
            evidence["complete"] = True
    finally:
        for marker in armed:
            marker.unlink(missing_ok=True)
        third.client.close()
        (folder / "employee-jobs.json").write_text(json.dumps(evidence, indent=2) + "\n")
