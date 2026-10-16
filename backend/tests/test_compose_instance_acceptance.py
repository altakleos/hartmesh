"""CI-only qualification of the shipped bundle; never a local fixture stack.

File order is deliberate. Case 4 has an isolated generic cleanup phase because
released-account maintenance cannot reclaim an active account's orphaned old
Space. Case 9 proves Undo sharing and explicit republication, not removal Undo.
"""

import hashlib
import json
import os
import random
import re
import selectors
import subprocess
import threading
import time
import uuid
from pathlib import Path
from urllib.parse import quote

import httpx
import pytest

if os.environ.get("HARTMESH_REQUIRE_BUNDLE_INSTANCES") != "1":
    pytest.skip("whole-bundle qualification requires its explicit CI/tenant opt-in", allow_module_level=True)

pytestmark = pytest.mark.live
MARKER = b"UPLOAD-ACCEPTANCE-726"
ARTIFACT = b"Synthetic acceptance artifact.\nUploaded text: UPLOAD-ACCEPTANCE-726\n"
CLEANUP_BOOTSTRAP = r"""set -eu
SOCKET=/var/run/docker.sock
test -S "$SOCKET"
test "$(id -u)" = 0
docker_gid="$(stat -c %g "$SOCKET")"
test "$docker_gid" != 0
cd /app/backend
export PYTHONPATH=.
exec setpriv --reuid=1000 --regid=1000 --groups="$docker_gid" --inh-caps=-all --no-new-privs "$@"
"""


def required(name):
    value = os.environ.get(name, "").strip()
    assert value, f"required bundle precondition is missing: {name}"
    return value


def sql(value):
    assert isinstance(value, str) and "\x00" not in value
    return "'" + value.replace("'", "''") + "'"


def eventually(check, *, timeout=90, interval=5):
    end = time.monotonic() + timeout
    while True:
        value = check()
        if value:
            return value
        assert time.monotonic() < end, "bundle state did not converge before the qualification deadline"
        time.sleep(interval)


class Bundle:
    def __init__(self):
        self.command = ["docker", "compose", "--project-directory", required("HARTMESH_BUNDLE_PROJECT_DIR"), "--env-file", required("HARTMESH_BUNDLE_ENV_FILE")]
        self.url = required("HARTMESH_BUNDLE_URL")
        self.runtime = required("HARTMESH_BUNDLE_RUNTIME")
        self.state = {}

    @staticmethod
    def invoke(command, *, check=True, timeout=180):
        result = subprocess.run(command, capture_output=True, text=True, timeout=timeout)
        if check:
            assert result.returncode == 0, f"qualification command failed: {command[:5]}\n{result.stderr[-4000:]}"
        return result

    def run(self, *args, **options):
        return self.invoke([*self.command, *args], **options)

    def psql(self, statement, *, timeout=180):
        return self.run("exec", "-T", "postgres", "psql", "-U", "deerflow", "-d", "deerflow", "-v", "ON_ERROR_STOP=1", "-Atc", statement, timeout=timeout).stdout.strip()

    def gateway(self, *args):
        assert args and args[0] == "python"
        return self.run("exec", "-T", "--user", "1000:1000", "--workdir", "/app/backend", "--env", "PYTHONPATH=.", "gateway", "/app/backend/.venv/bin/python", *args[1:]).stdout.strip()

    def accounts(self, verb, email=None, *, account_id=None):
        assert (email is None) != (account_id is None)
        selector = ["--id", account_id] if account_id is not None else ["--email", email]
        result = self.run("exec", "-T", "--user", "1000:1000", "--workdir", "/app/backend", "--env", "PYTHONPATH=.", "gateway", "/app/backend/.venv/bin/python", "-m", "app.gateway.auth.accounts", verb, *selector, check=False)
        return result.returncode, json.loads(result.stdout.splitlines()[-1])

    def ai(self, *args, timeout=180):
        bootstrap = (
            'cd /app/backend && exec setpriv --reuid=1000 --regid=1000 --groups="$(stat -c %g /var/run/docker.sock)" '
            "--inh-caps=-all --no-new-privs env HOME=/tmp/gateway-home DOCKER_CONFIG=/tmp/gateway-home/.docker PYTHONPATH=. "
            '/app/backend/.venv/bin/python -m app.gateway.agent_instances "$@"'
        )
        result = self.run("exec", "-T", "gateway", "sh", "-c", bootstrap, "agent_instances", *args, check=False, timeout=timeout)
        document = json.loads(result.stdout.splitlines()[-1])
        assert document["command"] == args[0] and document["returncode"] == result.returncode == 0, document
        return document

    def cleanup_orphan(self, facts):
        token = uuid.uuid4().hex
        name = "hartmesh-acceptance-cleanup-" + token
        self.state["cleanup_drained"] = False
        try:
            return self.run(
                "run",
                "--rm",
                "--no-deps",
                "-T",
                "--name",
                name,
                "--label",
                "hartmesh.acceptance.cleanup=" + token,
                "--entrypoint",
                "sh",
                "gateway",
                "-c",
                CLEANUP_BOOTSTRAP,
                "cleanup-bootstrap",
                "/app/backend/.venv/bin/python",
                "-c",
                ISOLATED_RELEASE,
                json.dumps(facts),
                timeout=240,
            )
        finally:
            # A timed-out Compose client can leave its container running. Drain
            # only this positively identified one-off before restarting Gateway.
            entries = [entry for entry in self.containers() if entry["Name"] == "/" + name]
            assert len(entries) <= 1
            for entry in entries:
                labels = entry["Config"].get("Labels") or {}
                assert labels.get("hartmesh.acceptance.cleanup") == token
                assert labels.get("com.docker.compose.service") == "gateway" and labels.get("com.docker.compose.oneoff", "").lower() == "true"
                self.invoke(["docker", "rm", "--force", entry["Id"]])
            assert not any(entry["Name"] == "/" + name for entry in self.containers())
            self.state["cleanup_drained"] = True

    def containers(self):
        ids = self.invoke(["docker", "ps", "-aq", "--no-trunc"]).stdout.split()
        assert len(ids) <= 1000 and all(re.fullmatch(r"[0-9a-f]{64}", identity) for identity in ids)
        if not ids:
            return []
        entries = json.loads(self.invoke(["docker", "inspect", *ids]).stdout)
        assert len(entries) == len(ids) and {entry["Id"] for entry in entries} == set(ids)
        return entries

    def sandbox(self, thread_id, *, before=None):
        def find():
            entries = [entry for entry in self.containers() if (entry.get("Config", {}).get("Labels") or {}).get("deerflow.role") == "sandbox"]
            matches = [entry for entry in entries if any(f"/conversations/{thread_id}/" in mount.get("Source", "") for mount in entry.get("Mounts", []))]
            if before is not None and not matches:
                matches = [entry for entry in entries if entry["Id"] not in before]
            assert len(matches) <= 1, "conversation sandbox identity is ambiguous"
            return matches[0] if matches else None

        return eventually(find)

    def role(self, account):
        value = self.psql(
            "SELECT json_build_object('space', s.id, 'handle', b.backing_handle, 'slot', b.slot_id, "
            "'filesystem_uuid', b.filesystem_uuid, 'generation', s.generation, 'max_bytes', b.max_bytes, "
            "'max_inodes', b.max_inodes, 'root_inode', b.root_inode, 'control_inode', b.control_inode, "
            "'custodian_id', s.custodian_id, 'custodian_kind', s.custodian_kind, 'custody_kind', s.custody_kind, "
            "'mode', s.mode, 'status', s.status)::text FROM hm_storage_feature_links l "
            "JOIN storage_spaces s ON s.id=l.space_id JOIN storage_space_backings b ON b.backing_handle=s.backing_handle "
            "WHERE l.namespace='hm.user-space' AND l.key=" + sql("human:" + account)
        )
        return json.loads(value) if value else None

    def bindings(self):
        return json.loads(self.psql("SELECT COALESCE(json_object_agg(slot_id, row_to_json(b)), '{}')::text FROM storage_space_backings b"))

    def logs(self):
        return self.run("logs", "--no-color", "gateway").stdout


class AllocationBarrier:
    """Hold the production allocation lock on a separate test-owned PG session."""

    def __init__(self, bundle):
        self.bundle = bundle
        self.process = None
        self.application_name = "hartmesh-acceptance-barrier-" + uuid.uuid4().hex
        self.identity = None
        self.drained = False

    def __enter__(self):
        self.process = subprocess.Popen(
            [
                *self.bundle.command,
                "exec",
                "-T",
                "--env",
                "PGAPPNAME=" + self.application_name,
                "--env",
                "PGOPTIONS=-c statement_timeout=15000 -c idle_session_timeout=600000",
                "postgres",
                "psql",
                "-U",
                "deerflow",
                "-d",
                "deerflow",
                "-v",
                "ON_ERROR_STOP=1",
                "-At",
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            self.process.stdin.write(
                b"SELECT json_build_object('pid', pid, 'started', backend_start::text)::text FROM pg_stat_activity WHERE pid=pg_backend_pid(); "
                b"SELECT pg_advisory_lock(hashtext('hartmesh-space-assignment')); SELECT 'allocation-barrier-held';\n"
            )
            self.process.stdin.flush()
            deadline = time.monotonic() + 20
            output = b""
            with selectors.DefaultSelector() as selector:
                selector.register(self.process.stdout, selectors.EVENT_READ)
                while b"allocation-barrier-held\n" not in output:
                    assert time.monotonic() < deadline, "could not acquire the qualification allocation barrier"
                    assert selector.select(timeout=max(0, deadline - time.monotonic())), "allocation barrier timed out"
                    chunk = os.read(self.process.stdout.fileno(), 4096)
                    assert chunk, "allocation barrier connection ended before acquiring its lock"
                    output += chunk
                    assert len(output) <= 8192
                    for line in output.split(b"\n")[:-1]:
                        if line.startswith(b"{"):
                            self.identity = json.loads(line)
            assert self.identity is not None and type(self.identity["pid"]) is int
            return self
        except BaseException:
            self.close()
            raise

    def close(self):
        if self.process is None:
            return
        try:
            try:
                _, stderr = self.process.communicate(input=b"\\q\n", timeout=30)
                assert self.process.returncode == 0, "allocation barrier failed: " + stderr.decode(errors="replace")[-2000:]
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.communicate(timeout=10)
                raise
        finally:
            # Docker client exit alone does not prove remote psql released its
            # session lock. Fence termination to our unique token and exact
            # backend identity, and confirm absence before any maintenance poll.
            try:
                predicate = "application_name=" + sql(self.application_name) + " AND datname='deerflow' AND usename='deerflow' AND backend_type='client backend'"
                entries = json.loads(self.bundle.psql("SELECT COALESCE(json_agg(json_build_object('pid', pid, 'started', backend_start::text)), '[]')::text FROM pg_stat_activity WHERE " + predicate, timeout=20))
                assert len(entries) <= 1, "qualification barrier session identity is ambiguous"
                if entries:
                    identity = entries[0]
                    assert self.identity is None or self.identity == identity
                    assert type(identity["pid"]) is int
                    exact = predicate + " AND pid=" + str(identity["pid"]) + " AND backend_start=" + sql(identity["started"]) + "::timestamptz"
                    assert self.bundle.psql("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE " + exact, timeout=20) in ("", "t")
                eventually(lambda: self.bundle.psql("SELECT count(*) FROM pg_stat_activity WHERE " + predicate, timeout=20) == "0", timeout=15, interval=0.2)
                self.drained = True
            finally:
                self.process = None

    def __exit__(self, *_error):
        self.close()


class Person:
    def __init__(self, bundle):
        self.bundle = bundle
        self.client = httpx.Client(base_url=bundle.url, trust_env=False, timeout=httpx.Timeout(180, connect=10))
        self.user = None

    def request(self, method, path, *, expected=200, **kwargs):
        token = self.client.cookies.get("csrf_token")
        if token:
            kwargs["headers"] = {"X-CSRF-Token": token, **kwargs.get("headers", {})}
        response = self.client.request(method, path, **kwargs)
        statuses = expected if isinstance(expected, tuple) else (expected,)
        assert response.status_code in statuses, f"{method} {path}: {response.status_code} {response.text[:1000]}"
        return response

    def thread(self):
        return self.request("POST", "/api/langgraph/threads", json={"thread_id": str(uuid.uuid4())}).json()["thread_id"]

    def upload(self, thread_id, content=MARKER, *, name="input.txt", expected=200):
        return self.request("POST", f"/api/threads/{thread_id}/uploads", files={"files": (name, content, "text/plain")}, expected=expected)

    def run(self, thread_id, *, success=True, files=None):
        response = self.request(
            "POST",
            f"/api/langgraph/threads/{thread_id}/runs/stream",
            json={
                "assistant_id": "lead_agent",
                "input": {"messages": [{"type": "human", "content": "acceptance:file", **({"additional_kwargs": {"files": files}} if files is not None else {})}]},
                "context": {"mode": "flash", "model_name": "acceptance-model", "thinking_enabled": False, "subagent_enabled": False},
                "stream_mode": ["values", "messages-tuple", "updates"],
            },
        )
        run_id = response.headers["Content-Location"].rstrip("/").rsplit("/", 1)[-1]
        assert re.fullmatch(r"[0-9a-f-]{36}", run_id)
        evidence = Path(os.environ.get("EVIDENCE", os.environ["TMPDIR"]))
        evidence.mkdir(parents=True, exist_ok=True)
        prefix = evidence / ("run-" + run_id)
        payload = response.content
        (prefix.with_suffix(".sse")).write_bytes(payload[:1048576] + (b"\nTRUNCATED: synthetic SSE evidence exceeded 1048576 bytes\n" if len(payload) > 1048576 else b""))
        (prefix.with_suffix(".identity.json")).write_text(json.dumps({"thread_id": thread_id, "run_id": run_id}), encoding="utf-8")

        def terminal():
            value = self.request("GET", f"/api/langgraph/threads/{thread_id}/runs/{run_id}").json()
            return value if value["status"] in ("success", "error", "timeout", "interrupted") else None

        result = eventually(terminal, timeout=120, interval=2)
        payload = json.dumps(result, indent=2).encode("utf-8")
        (prefix.with_suffix(".json")).write_bytes(payload[:1048576] + (b"\nTRUNCATED: synthetic run evidence exceeded 1048576 bytes\n" if len(payload) > 1048576 else b""))
        if success:
            assert result["status"] == "success", result
        return result


@pytest.fixture(scope="module")
def compose():
    bundle = Bundle()
    assert bundle.invoke(["docker", "info", "--format", "{{.OSType}}"]).stdout.strip() == "linux"
    return bundle


@pytest.fixture(scope="module")
def session(compose):
    person = Person(compose)
    email, password = required("HARTMESH_BUNDLE_ADMIN_EMAIL"), required("HARTMESH_BUNDLE_ADMIN_PASSWORD")
    person.request("POST", "/api/v1/auth/initialize", json={"email": email, "password": password}, expected=(201, 409))
    person.request("POST", "/api/v1/auth/login/local", data={"username": email, "password": password})
    person.user = person.request("GET", "/api/v1/auth/me").json()
    yield person
    person.client.close()


@pytest.fixture(scope="module")
def member(compose):
    person = Person(compose)
    person.request("POST", "/api/v1/auth/register", json={"email": "bundle-member@example.com", "password": "Acceptance-Member-2026!"}, expected=201)
    person.user = person.request("GET", "/api/v1/auth/me").json()
    yield person
    person.client.close()


@pytest.fixture(scope="module")
def employee(compose, session):
    """Share one personal AI assistant and its adopted Work-enabled definition."""
    definition = session.request(
        "POST",
        "/api/agents",
        json={"name": "bundle-worker", "model": "acceptance-model", "skills": [], "mcp_plugins": [], "allowed_subagents": [], "work_policy": {"enabled": True}, "soul": "Complete the assigned work using the offered tools."},
        expected=201,
    ).json()
    instance = session.request("POST", "/api/agent-instances", json={"creation_id": uuid.uuid4().hex, "name": "Qualification AI assistant", "definition_name": definition["name"], "custody": "personal"}, expected=201).json()
    thread = session.request("POST", f"/api/agent-instances/{instance['id']}/conversations", json={"creation_id": uuid.uuid4().hex}, expected=201).json()
    slot = compose.psql("SELECT b.slot_id FROM storage_spaces s JOIN storage_space_backings b ON b.backing_handle=s.backing_handle WHERE s.id=" + sql(instance["home_id"]))
    assert compose.state["inventory"][slot]["kind"] == "assistant"
    root = compose.state["inventory"][slot]["mount_path"] + "/data"
    # The pre-run Space WRITE route is available now; composer transfers are
    # stage-four qualification and must not be claimed by this setup.
    space = session.request("GET", f"/api/spaces/{instance['home_id']}").json()
    session.request("PUT", f"/api/spaces/{instance['home_id']}/content", params={"path": "uploads/input.txt", "generation": space["generation"], "operation_id": uuid.uuid4().hex, "create": True}, content=MARKER)
    return {"instance": instance, "thread_id": thread["thread_id"], "slot": slot, "root": root, "definition": definition["name"]}


def workspace(compose, employee):
    value = compose.psql(
        "SELECT json_build_object('attachment_id',id,'container_id',container_id)::text FROM storage_space_attachments WHERE actor_kind='nonhuman' AND actor_id=" + sql("agent:" + employee["instance"]["id"]) + " AND phase='active'"
    )
    assert value, "AI assistant has no active attachment"
    identity = json.loads(value)
    (entry,) = json.loads(compose.invoke(["docker", "inspect", "hartmesh-space-" + identity["attachment_id"]]).stdout)
    assert entry["Id"] == identity["container_id"] and entry["State"]["Running"]
    return identity, entry


def container_absent(compose, identity):
    result = compose.invoke(["docker", "inspect", identity], check=False)
    if result.returncode == 0:
        return False
    assert result.returncode == 1 and result.stdout.strip() in ("", "[]")
    assert result.stderr.strip() in ("Error: No such object: " + identity, "error: no such object: " + identity, "Error response from daemon: No such container: " + identity)
    return True


def work_path(employee, work_id=None):
    base = f"/api/agent-instances/{employee['instance']['id']}/work"
    return base + ("/" + work_id if work_id else "")


def delegate(session, employee, marker):
    return session.request("POST", work_path(employee), json={"operation_id": uuid.uuid4().hex, "objective": marker, "success_criteria": "Complete the deterministic acceptance task.", "review_required": True}, expected=201).json()


def activate(session, employee, work, *, expected=200):
    return session.request(
        "POST",
        work_path(employee, work["id"]) + "/activate",
        json={"operation_id": uuid.uuid4().hex, "expected_revision": work["revision"], "expected_assignment_revision": work["assignment_revision"], "thread_id": employee["thread_id"]},
        expected=expected,
    ).json()


def await_attempt(session, employee, work_id, statuses, *, timeout=120):
    def check():
        work = session.request("GET", work_path(employee, work_id)).json()
        return work if work["attempt"] and work["attempt"]["status"] in statuses else None

    return eventually(check, timeout=timeout, interval=2)


def held_attempt(compose, session, employee):
    work = delegate(session, employee, "acceptance:work:hold")
    activate(session, employee, work)

    def holding():
        value = session.request("GET", work_path(employee, work["id"])).json()
        if not value["attempt"] or value["attempt"]["status"] != "running" or value["attempt"]["workspace"] != "ready":
            return None
        identity, _entry = workspace(compose, employee)
        attempt_attachment = compose.psql("SELECT attachment_id FROM agent_work_attempts WHERE id=" + sql(value["attempt"]["id"]))
        assert attempt_attachment == identity["attachment_id"]
        processes = compose.invoke(["docker", "exec", identity["container_id"], "ps", "-eo", "args"]).stdout.splitlines()
        return (value, identity) if any(line.strip() == "sleep 300" for line in processes) else None

    return eventually(holding, timeout=120, interval=2)


class ProbeObserver:
    """Observe actual probe creates during preparation, draining our reader."""

    def __init__(self, compose):
        self.compose, self.facts, self.errors = compose, [], []
        self.stop = threading.Event()

    def __enter__(self):
        self.process = subprocess.Popen(
            ["docker", "events", "--since", str(int(time.time())), "--filter", "type=container", "--filter", "event=create", "--filter", "label=hartmesh.storage.probe", "--format", "{{json .}}"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        self.reader = threading.Thread(target=self.observe, name="qualification-probe-observer")
        self.reader.start()
        return self

    def observe(self):
        try:
            with selectors.DefaultSelector() as selector:
                selector.register(self.process.stdout, selectors.EVENT_READ)
                while not self.stop.is_set():
                    if not selector.select(0.2):
                        continue
                    line = self.process.stdout.readline()
                    if not line:
                        if self.stop.is_set():
                            return
                        raise AssertionError("Docker probe observer ended before drainage")
                    event = json.loads(line)
                    identity = event["Actor"]["ID"]
                    result = self.compose.invoke(["docker", "inspect", identity], check=False, timeout=10)
                    if result.returncode == 0:
                        (entry,) = json.loads(result.stdout)
                        assert entry["Id"] == identity and "hartmesh.storage.probe" in entry["Config"]["Labels"]
                        self.facts.append(entry)
        except BaseException as error:
            self.errors.append(error)

    def __exit__(self, *_args):
        self.stop.set()
        self.process.terminate()
        try:
            self.process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=15)
        self.reader.join(timeout=15)
        self.process.stdout.close()
        self.process.stderr.close()
        assert not self.reader.is_alive(), "Docker observer reader did not drain"
        assert not self.errors, self.errors


def company(compose):
    return json.loads(
        compose.psql(
            "SELECT json_build_object('space', s.id, 'handle', b.backing_handle, 'slot', b.slot_id, "
            "'filesystem_uuid', b.filesystem_uuid)::text FROM hm_storage_feature_links l JOIN storage_spaces s ON s.id=l.space_id "
            "JOIN storage_space_backings b ON b.backing_handle=s.backing_handle WHERE l.namespace='hm.company-space' AND l.key='company'"
        )
    )


def mounted(entry, destination):
    matches = [mount for mount in entry["Mounts"] if mount["Destination"] == destination]
    assert len(matches) == 1, f"expected one bind at {destination}"
    assert matches[0]["Type"] == "bind"
    return matches[0]


def user_mounts(compose, entry, thread_id, role, *, shared=True):
    inventory = compose.state["inventory"]
    assert inventory[role["slot"]]["kind"] == "user"
    root = inventory[role["slot"]]["mount_path"] + "/data"
    for leaf in ("workspace", "uploads", "outputs"):
        bind = mounted(entry, "/mnt/user-data/" + leaf)
        assert bind["RW"] is True
        assert bind["Source"] == f"{root}/conversations/{thread_id}/user-data/{leaf}"
    files = mounted(entry, "/mnt/user-data/files")
    assert files["Source"] == root + "/files" and files["RW"] is True
    acp = mounted(entry, "/mnt/acp-workspace")
    assert acp["Source"] == f"{root}/conversations/{thread_id}/acp-workspace" and acp["RW"] is False
    labels = entry["Config"]["Labels"]
    assert labels["hartmesh.storage.space." + role["slot"]] == "1"
    if shared:
        shared_role = company(compose)
        assert inventory[shared_role["slot"]]["kind"] == "company"
        bind = mounted(entry, "/mnt/user-data/shared")
        assert bind["Source"] == inventory[shared_role["slot"]]["mount_path"] + "/data/shared" and bind["RW"] is False
        assert labels["hartmesh.storage.space." + shared_role["slot"]] == "1"
    else:
        assert not any(mount["Destination"] == "/mnt/user-data/shared" for mount in entry["Mounts"])


def artifact(person, thread_id):
    response = person.request("GET", f"/api/threads/{thread_id}/artifacts/mnt/user-data/outputs/acceptance.txt")
    assert response.content == ARTIFACT


ISOLATED_RELEASE = r"""import asyncio, json, os, re, sys
from pathlib import Path
from datetime import datetime, timezone
from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from deerflow.features.resources import StorageFeatureLinkRow
from deerflow.persistence.spaces.files import SpaceBackingRow
from deerflow.persistence.spaces.model import SpaceRow, SpaceEventRow
from deerflow.spaces.backings import PreparedVolumeCatalog
from deerflow.spaces.principals import HostPrincipalResolver
from deerflow.spaces.registry import SpaceRegistry
from deerflow.spaces.recovery import SpaceRecovery
from deerflow.spaces.runtime_owner import PoolRuntimeOwner
from deerflow.spaces.service import SpaceFiles
from deerflow.utils.file_io import run_file_io
assert os.geteuid() == os.getegid() == 1000
facts = json.loads(sys.argv[1])
old, new = facts['old'], facts['new']
pool = Path(os.environ['HARTMESH_DATA_DIR']) / 'spaces'
owner = PoolRuntimeOwner(pool)
owner.acquire()
engine = create_async_engine(os.environ['DATABASE_URL'].replace('postgresql://', 'postgresql+psycopg://', 1))
sf = async_sessionmaker(engine, expire_on_commit=False)
def docker(*arguments):
    result = owner.run(['docker', *arguments], capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, 'Docker containment could not be established: ' + result.stderr
    return result.stdout.strip()
def inventory():
    assert docker('info', '--format', '{{.ID}}') == facts['daemon']
    ids = docker('ps', '-aq', '--no-trunc').split()
    assert len(ids) <= 1000 and all(re.fullmatch('[0-9a-f]{64}', identity) for identity in ids)
    entries = json.loads(docker('inspect', *ids)) if ids else []
    assert len(entries) == len(ids) and {entry['Id'] for entry in entries} == set(ids)
    assert docker('info', '--format', '{{.ID}}') == facts['daemon']
    return entries
catalog = PreparedVolumeCatalog.from_manifest(pool / 'inventory.v2.json')
root = catalog.volumes[old['slot']].mount_path
self_id = json.loads(docker('inspect', os.environ['HOSTNAME']))[0]['Id']
def holder(entry):
    if entry['Id'] == self_id:
        assert entry['Config']['Labels']['com.docker.compose.service'] == 'gateway'
        assert entry['Config']['Labels']['com.docker.compose.oneoff'].lower() == 'true'
        return False
    if (entry['Config'].get('Labels') or {}).get('hartmesh.storage.space.' + old['slot']) == '1':
        return True
    for mount in entry.get('Mounts', []):
        source = Path(mount['Source'])
        if mount['Type'] == 'bind' and (source == root or source.is_relative_to(root)):
            return True
        if mount['Type'] == 'bind' and root.is_relative_to(source):
            return True
    return False
for entry in inventory():
    if holder(entry):
        docker('rm', '--force', entry['Id'])
def absent():
    assert not any(holder(entry) for entry in inventory()), 'old slot still has a container holder'
def same_binding(binding, expected):
    assert binding is not None
    assert (binding.space_id, binding.backing_handle, binding.slot_id) == (expected['space'], expected['handle'], expected['slot'])
    for key in ('filesystem_uuid', 'max_bytes', 'max_inodes', 'root_inode', 'control_inode'):
        assert getattr(binding, key) == expected[key]
async def human(_reference):
    return None
async def main():
    files = SpaceFiles(SpaceRegistry(sf, HostPrincipalResolver(human=human)), catalog, inventory_path=pool / 'inventory.v2.json')
    await files.load_bound_slots()
    async with sf.begin() as session:
        row = await session.get(SpaceRow, old['space'], with_for_update=True)
        assert row.generation == old['generation'] and row.status == 'active' and row.custodian_id == facts['account']
        same_binding(await session.get(SpaceBackingRow, old['handle']), old)
        row.status = 'deleted'
        row.generation += 1
        row.updated_at = datetime.now(timezone.utc)
        details = {'retained_backups': False, 'retained_binding': True, 'by': 'host', 'cause': 'fixture-cleanup'}
        session.add(SpaceEventRow(space_id=row.id, generation=row.generation, action='delete', actor_kind='human', actor_id=facts['account'], details=details))
    async def admission(session):
        row = await session.get(SpaceRow, old['space'])
        assert row.status == 'deleted' and row.generation == old['generation'] + 1 and row.custodian_id == facts['account']
        event = await session.get(SpaceEventRow, (row.id, row.generation))
        assert event.action == 'delete' and event.actor_id == facts['account'] and event.details == {'retained_backups': False, 'retained_binding': True, 'by': 'host', 'cause': 'fixture-cleanup'}
        same_binding(await session.get(SpaceBackingRow, old['handle']), old)
        link = await session.get(StorageFeatureLinkRow, ('hm.user-space', 'human:' + facts['account']))
        assert link.space_id == new['space']
        assert await session.scalar(select(StorageFeatureLinkRow).where(StorageFeatureLinkRow.space_id == old['space'])) is None
        new_row = await session.get(SpaceRow, new['space'])
        assert new_row.status == 'active' and new_row.generation == new['generation'] and new_row.custodian_id == facts['account']
        same_binding(await session.get(SpaceBackingRow, new['handle']), new)
    absent()
    async def prove_absent():
        await run_file_io(absent)
    result = await SpaceRecovery(files).host_release(old['space'], admission=admission, retain_unverified=True, before_publication=prove_absent)
    assert result['complete'] is True and result['slot_released'] is True
    print(json.dumps(result))
async def run():
    try:
        await main()
    finally:
        await engine.dispose()
try:
    asyncio.run(run())
finally:
    owner.close()
"""


def test_pool_verifies_inside_the_gateway_container_without_host_image_path(compose):
    script = """import json, os
from pathlib import Path
from deerflow.spaces.backings import PreparedVolumeCatalog
assert os.geteuid() == os.getegid() == 1000
path = Path(os.environ['HARTMESH_DATA_DIR']) / 'spaces/inventory.v2.json'
raw = json.loads(path.read_text(encoding='utf-8'))
assert raw['schema_version'] == 2
assert all(entry['kind'] in ('user', 'assistant', 'employee', 'company') and 'host_image_path' not in entry for entry in raw['volumes'])
catalog = PreparedVolumeCatalog.from_manifest(path)
for slot in catalog.volumes:
    catalog.verify(slot)
print(json.dumps(raw))
"""
    raw = json.loads(compose.gateway("python", "-c", script))
    compose.state["inventory"] = {entry["slot_id"]: entry for entry in raw["volumes"]}


def test_ordinary_chat_works_with_storage_spaces_on(compose, session):
    thread_id = session.thread()
    session.upload(thread_id, MARKER + b" administrator")
    session.run(thread_id)
    artifact(session, thread_id)
    entry = compose.sandbox(thread_id)
    role = eventually(lambda: compose.role(session.user["id"]))
    user_mounts(compose, entry, thread_id, role)
    binds = [mount for mount in entry["Mounts"] if mount["Type"] == "bind"]
    managed = {"/mnt/user-data/workspace", "/mnt/user-data/uploads", "/mnt/user-data/outputs", "/mnt/user-data/files", "/mnt/user-data/shared", "/mnt/acp-workspace"}
    settings = json.loads(
        compose.gateway(
            "python",
            "-c",
            "import json, os, sys, yaml; from pathlib import Path; "
            "print(json.dumps({'configured': yaml.safe_load(Path(os.environ['DEER_FLOW_CONFIG_PATH']).read_text(encoding='utf-8'))['sandbox'].get('mounts', []), "
            "'home': os.environ['DEER_FLOW_HOST_BASE_DIR'], 'projection': (Path(os.environ['DEER_FLOW_HOME'])/'chat-runtime'/sys.argv[1]/sys.argv[2]/'skills_view').is_dir(), "
            "'lark_runtime': (Path(os.environ['DEER_FLOW_HOME'])/'integrations/lark-cli/sandbox-cli').is_dir()}))",
            session.user["id"],
            thread_id,
        )
    )
    configured = settings["configured"]
    categories = ("public", "custom", "legacy", "integrations")
    operational_root = settings["home"] + "/chat-runtime/" + session.user["id"] + "/" + thread_id
    if settings["projection"]:
        skills = {"/mnt/skills/" + category: (operational_root + "/skills_view/" + category, False) for category in categories}
    else:
        skills = {"/mnt/skills/" + category: (settings["home"] + "/users/" + session.user["id"] + "/skills_view/" + category, False) for category in categories}
        skills["/mnt/skills/public"] = (settings["home"] + "/skills_view/public", False)
    lark_root = settings["home"] + "/users/" + session.user["id"] + "/integrations/lark-cli"
    integrations = {"/mnt/integrations/lark-cli/config": (lark_root + "/config", False), "/mnt/integrations/lark-cli/config/locks": (lark_root + "/config/locks", True), "/mnt/integrations/lark-cli/data": (lark_root + "/data", True)}
    if settings["lark_runtime"]:
        integrations["/mnt/integrations/lark-cli/runtime"] = (settings["home"] + "/integrations/lark-cli/sandbox-cli", False)
    destinations = {mount["Destination"] for mount in binds}
    assert destinations == managed | set(skills) | set(integrations) | {mount["container_path"] for mount in configured}
    assert len(binds) == len(destinations)
    for configuration in configured:
        assert configuration["read_only"] is True and configuration["container_path"] not in managed | set(skills) | set(integrations)
        bind = mounted(entry, configuration["container_path"])
        assert bind["Source"] == configuration["host_path"] and bind["RW"] is False
    for destination, (source, writable) in {**skills, **integrations}.items():
        bind = mounted(entry, destination)
        assert bind["Source"] == source and bind["RW"] is writable
    compose.state["admin_thread"] = thread_id


def test_my_files_shared_and_projects_work_with_storage_spaces_on(compose, session):
    for path in ("/api/files", "/api/shared", "/api/projects"):
        session.request("GET", path)
    thread_id = compose.state["admin_thread"]
    kept = session.request("POST", f"/api/threads/{thread_id}/files", json={"path": "/mnt/user-data/outputs/acceptance.txt"}, expected=201).json()
    listing = session.request("GET", "/api/files").json()["files"]
    assert any(item["path"] == kept["path"] for item in listing)
    role = compose.role(session.user["id"])
    root = compose.state["inventory"][role["slot"]]["mount_path"]
    assert compose.gateway("python", "-c", "import sys; from pathlib import Path; sys.stdout.write(Path(sys.argv[1]).read_text(encoding='utf-8'))", root + "/data/files/" + kept["path"]).encode() == ARTIFACT.rstrip(b"\n")
    entry = compose.sandbox(thread_id)
    assert compose.invoke(["docker", "exec", entry["Id"], "cat", "/mnt/user-data/files/" + kept["path"]]).stdout.encode() == ARTIFACT
    user_mounts(compose, entry, thread_id, role)


def test_chat_without_files_mounts_nothing_of_the_user(compose, member):
    account = member.user["id"]
    old = eventually(lambda: compose.role(account))
    before_bindings = compose.bindings()
    new = None
    removed = False
    barrier = AllocationBarrier(compose)
    try:
        with barrier:
            compose.psql("DELETE FROM hm_storage_feature_links WHERE namespace='hm.user-space' AND key=" + sql("human:" + account))
            removed = True
            refusal = member.request("GET", "/api/files", expected=503).json()["detail"]
            assert refusal["code"] == "storage-unavailable" and refusal["reason"] == "space-not-assigned"
            before = {entry["Id"] for entry in compose.containers()}
            thread_id = member.thread()
            member.run(thread_id, success=False)
            entry = compose.sandbox(thread_id, before=before)
            assert not any(mount["Destination"].startswith("/mnt/user-data") or mount["Destination"] == "/mnt/acp-workspace" for mount in entry["Mounts"])
        new = eventually(lambda: compose.role(account))
        assert new["space"] != old["space"] and new["slot"] not in before_bindings
        eventually(lambda: f"storage: user space of {account} assigned ({new['slot']})" in compose.logs())
        member.request("GET", "/api/files")
        ready_thread = member.thread()
        member.upload(ready_thread)
        member.run(ready_thread)
        user_mounts(compose, compose.sandbox(ready_thread), ready_thread, new)
    finally:
        if removed:
            if not barrier.drained:
                compose.run("stop", "--timeout", "30", "gateway")
            assert barrier.drained, "qualification barrier drainage unconfirmed; Gateway left stopped"
            # Let maintenance complete even if an earlier assertion failed;
            # preserve cleanup errors as test failures rather than orphan slots.
            new = new or eventually(lambda: compose.role(account))
            assert new["space"] != old["space"]
            daemon = compose.invoke(["docker", "info", "--format", "{{.ID}}"]).stdout.strip()
            evidence = Path(os.environ.get("EVIDENCE", os.environ["TMPDIR"]))
            evidence.mkdir(parents=True, exist_ok=True)
            (evidence / "case4-before-cleanup.log").write_text(compose.logs(), encoding="utf-8")
            compose.run("stop", "--timeout", "30", "gateway")
            try:
                facts = {"old": old, "new": new, "account": account, "daemon": daemon}
                # Test-owned bootstrap mirrors the service's privilege drop
                # and executes the helper with the image's Python environment.
                compose.cleanup_orphan(facts)
            finally:
                if compose.state.get("cleanup_drained") is True:
                    compose.run("up", "-d", "--wait", timeout=240)
            current = compose.bindings()
            expected = {key: value for key, value in before_bindings.items() if key != old["slot"]}
            expected[new["slot"]] = current[new["slot"]]
            assert current == expected and current[new["slot"]]["space_id"] == new["space"]
            assert compose.role(account) == new
            path = compose.state["inventory"][old["slot"]]["mount_path"]
            assert compose.gateway("python", "-c", "from pathlib import Path; import sys; root=Path(sys.argv[1]); assert not any((root/'data').iterdir()) and not any((root/'control').iterdir())", path) == ""
            member.request("GET", "/api/files")


def test_chat_without_shared_mounts_the_users_space_alone(compose, session):
    original = company(compose)
    before = compose.bindings()
    try:
        compose.psql("UPDATE storage_space_backings SET filesystem_uuid=" + sql(str(uuid.uuid4())) + " WHERE backing_handle=" + sql(original["handle"]))
        thread_id = session.thread()
        session.upload(thread_id)
        session.run(thread_id)
        artifact(session, thread_id)
        user_mounts(compose, compose.sandbox(thread_id), thread_id, compose.role(session.user["id"]), shared=False)
        refusal = session.request("GET", "/api/shared", expected=503).json()["detail"]
        assert refusal["code"] == "storage-unavailable" and refusal["reason"] == "space-unverified"
    finally:
        compose.psql("UPDATE storage_space_backings SET filesystem_uuid=" + sql(original["filesystem_uuid"]) + " WHERE backing_handle=" + sql(original["handle"]))
        assert compose.bindings() == before
    session.request("GET", "/api/shared")
    thread_id = session.thread()
    session.upload(thread_id)
    session.run(thread_id)
    user_mounts(compose, compose.sandbox(thread_id), thread_id, compose.role(session.user["id"]))


def test_seat_is_taken_at_the_three_creation_paths_and_refused_past_the_count(compose, session, member):
    seats = session.request("GET", "/api/features").json()["accounts"]["seats"]
    assert type(seats["allowed"]) is int and seats["allowed"] >= 3
    added = []
    while seats["in_use"] < seats["allowed"]:
        email = f"bundle-seated-{len(added)}@example.com"
        added.append(session.request("POST", "/api/v1/auth/users", json={"email": email}, expected=201).json())
        seats = session.request("GET", "/api/features").json()["accounts"]["seats"]
    assert added and seats["in_use"] == seats["allowed"]
    shared = company(compose)
    for account in [session.user, member.user, *added]:
        role = eventually(lambda: compose.role(account["id"]))
        assert compose.state["inventory"][role["slot"]]["kind"] == "user"
        grant = compose.psql("SELECT permissions FROM storage_space_grants WHERE space_id=" + sql(shared["space"]) + " AND principal_kind='human' AND subject_id=" + sql(account["id"]))
        assert grant == "17"
    for endpoint, status, email in [("/api/v1/auth/users", 409, "bundle-refused-admin@example.com"), ("/api/v1/auth/register", 403, "bundle-refused-registration@example.com")]:
        payload = {"email": email}
        if endpoint.endswith("register"):
            payload["password"] = "Acceptance-Refused-2026!"
        refusal = session.request("POST", endpoint, json=payload, expected=status).json()["detail"]
        assert refusal["code"] == "no_seats_left"
        assert compose.psql("SELECT count(*) FROM users WHERE email=" + sql(email)) == "0"
    assert session.request("GET", "/api/features").json()["accounts"]["seats"] == seats
    compose.state["release_account"] = added[-1]


def test_release_seat_frees_the_seat_and_empties_the_users_space(compose, session):
    account = compose.state["release_account"]
    old = compose.role(account["id"])
    before = session.request("GET", "/api/features").json()["accounts"]["seats"]
    person = Person(compose)
    paired_resources = []
    try:
        person.request("POST", "/api/v1/auth/login/local", data={"username": account["email"], "password": account["one_time_password"]})
        person.request("POST", "/api/v1/auth/change-password", json={"current_password": account["one_time_password"], "new_password": "Acceptance-Released-2026!", "new_email": account["email"]})
        person.user = person.request("GET", "/api/v1/auth/me").json()
        assert person.user["id"] == account["id"] and person.user["needs_setup"] is False
        for _index in range(2):
            thread_id = person.thread()
            person.upload(thread_id)
            person.run(thread_id)
            artifact(person, thread_id)
            primary = compose.sandbox(thread_id)
            user_mounts(compose, primary, thread_id, old)
            labels = primary["Config"]["Labels"]
            assert labels["deerflow.network_mode"] == "allowlist" and labels["deerflow.owner_user_id"] == account["id"]
            networks = primary["NetworkSettings"]["Networks"]
            assert len(networks) == 1
            internal_name = next(iter(networks))
            (internal,) = json.loads(compose.invoke(["docker", "network", "inspect", internal_name]).stdout)
            assert internal["Id"] == networks[internal_name]["NetworkID"]
            candidates = set(internal["Containers"]) - {primary["Id"]}
            assert len(candidates) == 1
            (proxy,) = json.loads(compose.invoke(["docker", "inspect", next(iter(candidates))]).stdout)
            assert proxy["Config"]["Labels"]["deerflow.role"] == "network-proxy"
            assert proxy["Config"]["Labels"]["deerflow.sandbox_id"] == labels["deerflow.sandbox_id"]
            proxy_networks = proxy["NetworkSettings"]["Networks"]
            assert len(proxy_networks) == 2 and internal_name in proxy_networks
            egress_name = next(iter(set(proxy_networks) - {internal_name}))
            (egress,) = json.loads(compose.invoke(["docker", "network", "inspect", egress_name]).stdout)
            assert egress["Id"] == proxy_networks[egress_name]["NetworkID"]
            paired_resources.extend((kind, entry["Id"], entry["Name"].removeprefix("/")) for kind, entry in (("container", primary), ("container", proxy), ("network", internal), ("network", egress)))
        assert len({(kind, identity) for kind, identity, _name in paired_resources}) == 8
        compose.state["resource_members"] = paired_resources
    finally:
        person.client.close()

    code, result = compose.accounts("release-seat", account["email"])
    assert code == 1 and result["error"] == "not_disabled"
    # Both complete sets must still exist at the containment boundary.
    for kind, identity, name in paired_resources:
        (current,) = json.loads(compose.invoke(["docker", *(["network"] if kind == "network" else []), "inspect", identity]).stdout)
        assert current["Id"] == identity and current["Name"].removeprefix("/") == name
    code, result = compose.accounts("disable", account["email"])
    assert code == 0 and result["returncode"] == 0
    code, result = compose.accounts("release-seat", account["email"])
    assert code == 0 and result["returncode"] == 0
    eventually(lambda: old["slot"] not in compose.bindings())
    eventually(lambda: f"storage: user space reclaimed ({old['slot']})" in compose.logs())

    def paired_absent():
        for kind, identity, name in paired_resources:
            for reference in (identity, name):
                result = compose.invoke(["docker", *(["network"] if kind == "network" else []), "inspect", reference], check=False)
                absent = (
                    ("Error: No such object: " + reference, "Error response from daemon: No such container: " + reference)
                    if kind == "container"
                    else ("Error: No such network: " + reference, "Error response from daemon: network " + reference + " not found")
                )
                if result.returncode == 0:
                    return False
                assert result.returncode == 1 and result.stdout.strip() in ("", "[]") and result.stderr.strip() in absent
        assert not any((entry.get("Config", {}).get("Labels") or {}).get("hartmesh.storage.space." + old["slot"]) == "1" for entry in compose.containers())
        return True

    assert eventually(paired_absent)
    assert session.request("GET", "/api/features").json()["accounts"]["seats"] == {"allowed": before["allowed"], "in_use": before["allowed"] - 1}
    path = compose.state["inventory"][old["slot"]]["mount_path"]
    compose.gateway("python", "-c", "from pathlib import Path; import sys; p=Path(sys.argv[1]); assert not any((p/'data').iterdir()) and not any((p/'control').iterdir())", path)
    code, result = compose.accounts("enable", account_id=account["id"])
    assert code == 1 and result["error"] == "released"
    replacement = session.request("POST", "/api/v1/auth/users", json={"email": "bundle-replacement@example.com"}, expected=201).json()
    assert eventually(lambda: compose.role(replacement["id"]))["slot"] == old["slot"]


def test_company_grant_is_written_for_an_account_that_had_none(compose, member):
    shared = company(compose)
    account = member.user["id"]
    line = f"storage: company grant written for {account}"
    before = compose.logs().count(line)
    compose.psql("DELETE FROM storage_space_grants WHERE space_id=" + sql(shared["space"]) + " AND principal_kind='human' AND subject_id=" + sql(account))
    try:
        eventually(lambda: compose.psql("SELECT permissions FROM storage_space_grants WHERE space_id=" + sql(shared["space"]) + " AND principal_kind='human' AND subject_id=" + sql(account)) == "17")
        eventually(lambda: compose.logs().count(line) == before + 1)
        member.request("GET", "/api/shared")
    finally:
        # A failed repair remains a failed test, while restoring its fixture
        # authority before any later test inspects Shared.
        compose.psql(
            "INSERT INTO storage_space_grants (space_id,principal_kind,subject_id,permissions) VALUES ("
            + sql(shared["space"])
            + ",'human',"
            + sql(account)
            + ",17) ON CONFLICT (space_id,principal_kind,subject_id) DO UPDATE SET permissions=17"
        )


def test_publication_records_the_member_and_by_host(compose, session, member):
    thread_id = member.thread()
    content = MARKER + b" member publication"
    member.upload(thread_id, content)
    member.run(thread_id)
    request = {"thread_id": thread_id, "path": "/mnt/user-data/uploads/input.txt"}
    first = member.request("POST", "/api/shared/publish", json=request, expected=201).json()
    duplicate = member.request("POST", "/api/shared/publish", json=request).json()
    assert duplicate["publication_id"] == first["publication_id"] and duplicate["path"] == first["path"]
    collision = session.request("POST", "/api/shared/publish", json={"thread_id": compose.state["admin_thread"], "path": "/mnt/user-data/uploads/input.txt"}, expected=201).json()
    assert collision["path"] != first["path"]
    assert member.request("GET", "/api/shared/" + quote(first["path"], safe="/")).content == content
    shared = company(compose)
    operation = json.loads(
        compose.psql(
            "SELECT json_build_object('actor_kind',actor_kind,'actor_id',actor_id,'request',request)::text FROM storage_space_file_operations WHERE space_id="
            + sql(shared["space"])
            + " AND actor_id="
            + sql(member.user["id"])
            + " ORDER BY created_at DESC LIMIT 1"
        )
    )
    assert operation["actor_kind"] == "human" and operation["actor_id"] == member.user["id"] and operation["request"]["by"] == "host"
    listed = member.request("GET", "/api/shared").json()["files"]
    assert next(item for item in listed if item["publication_id"] == first["publication_id"])["published_by"] == member.user["email"]
    sandbox = compose.sandbox(thread_id)
    assert compose.invoke(["docker", "exec", sandbox["Id"], "cat", "/mnt/user-data/shared/" + first["path"]]).stdout.encode() == content
    denied = compose.invoke(["docker", "exec", sandbox["Id"], "touch", "/mnt/user-data/shared/qualification-write"], check=False)
    assert denied.returncode != 0 and "Read-only file system" in denied.stderr
    # The actual Share toast's Undo sends exact-publication DELETE. Shared
    # removal has no Undo; republication from the retained source is explicit.
    member.request("DELETE", "/api/shared/" + quote(first["path"], safe="/"), params={"expected_publication_id": first["publication_id"]})
    assert not any(item["publication_id"] == first["publication_id"] for item in member.request("GET", "/api/shared").json()["files"])
    restored = member.request("POST", "/api/shared/publish", json=request, expected=201).json()
    assert restored["publication_id"] != first["publication_id"]
    assert member.request("GET", "/api/shared/" + quote(restored["path"], safe="/")).content == content
    compose.state["member_thread"] = thread_id


def test_full_user_space_refuses_upload_and_new_conversation_with_files_full(compose, member, tmp_path):
    thread_id = compose.state["member_thread"]
    # Create before filling: the current Gateway also rejects thread creation
    # itself at capacity. This separate empty thread proves run admission too.
    empty_thread = member.thread()
    entry = compose.sandbox(thread_id)
    before = {container["Id"] for container in compose.containers() if (container["Config"].get("Labels") or {}).get("deerflow.role") == "sandbox"}
    payload = tmp_path / "nine-mebibytes.txt"
    with payload.open("wb") as output:
        for _ in range(9):
            output.write(b"x" * (1024 * 1024))
    try:
        filled = compose.invoke(["docker", "exec", entry["Id"], "sh", "-c", "dd if=/dev/zero of=/mnt/user-data/workspace/qualification-fill bs=1M"], check=False)
        assert filled.returncode != 0 and "No space left on device" in filled.stderr
        compose.invoke(["docker", "exec", entry["Id"], "truncate", "-s", "-8M", "/mnt/user-data/workspace/qualification-fill"])
        with payload.open("rb") as source:
            refusal = member.upload(thread_id, source, name="large.txt", expected=507).json()
        assert refusal["reason"] == "files-full"
        denied = member.request("POST", f"/api/langgraph/threads/{empty_thread}/runs/stream", json={"assistant_id": "lead_agent", "input": {"messages": [{"type": "human", "content": "acceptance:file"}]}}, expected=507).json()
        assert denied["reason"] == "files-full"
        denied_create = member.request("POST", "/api/langgraph/threads", json={"thread_id": str(uuid.uuid4())}, expected=507).json()
        assert denied_create["reason"] == "files-full"
        after = {container["Id"] for container in compose.containers() if (container["Config"].get("Labels") or {}).get("deerflow.role") == "sandbox"}
        assert after == before
        member.request("POST", "/api/langgraph/threads/search", json={})
        member.request("GET", f"/api/langgraph/threads/{thread_id}/state")
    finally:
        compose.invoke(["docker", "exec", entry["Id"], "rm", "-f", "/mnt/user-data/workspace/qualification-fill"])
    with payload.open("rb") as source:
        member.upload(thread_id, source, name="large.txt")
    fresh = member.thread()
    member.upload(fresh)
    member.run(fresh)
    artifact(member, fresh)


def test_bundle_runs_an_agent_and_its_space_survives_gateway_restart_and_workspace_loss(compose, session, employee):
    thread_id = employee["thread_id"]
    session.run(thread_id)
    artifact(session, thread_id)
    first, entry = workspace(compose, employee)
    home = mounted(entry, "/mnt/spaces/home")
    assert home["Source"] == employee["root"] and home["RW"] is True and home["Propagation"] == "rprivate"
    assert not any(mount["Destination"] == "/mnt/user-data/files" for mount in entry["Mounts"])
    shared = company(compose)
    shared_mount = mounted(entry, "/mnt/user-data/shared")
    assert shared_mount["Source"] == compose.state["inventory"][shared["slot"]]["mount_path"] + "/data/shared" and shared_mount["RW"] is False
    labels = entry["Config"]["Labels"]
    assert labels["deerflow.role"] == "resident"
    assert not entry["HostConfig"]["PortBindings"] and not any(entry["NetworkSettings"]["Ports"].values())
    assert len(entry["NetworkSettings"]["Networks"]) == 1
    network_name = next(iter(entry["NetworkSettings"]["Networks"]))
    (network,) = json.loads(compose.invoke(["docker", "network", "inspect", network_name]).stdout)
    assert network["Internal"] is True
    tenant = mounted(entry, "/mnt/tenant")
    assert tenant["RW"] is False
    for slot in (employee["slot"], shared["slot"]):
        assert labels["hartmesh.storage.space." + slot] == "1"
    assert compose.invoke(["docker", "exec", entry["Id"], "cat", "/mnt/user-data/outputs/acceptance.txt"]).stdout.encode() == ARTIFACT
    compose.run("restart", "gateway", timeout=180)
    compose.run("up", "-d", "--wait", timeout=240)
    artifact(session, thread_id)
    session.run(thread_id)
    second, _entry = workspace(compose, employee)
    assert second["container_id"] == first["container_id"]
    compose.invoke(["docker", "rm", "-f", second["container_id"]])
    assert container_absent(compose, second["container_id"])
    session.run(thread_id)
    third, _entry = workspace(compose, employee)
    assert third["container_id"] != second["container_id"]
    artifact(session, thread_id)
    assert compose.gateway("python", "-c", "from pathlib import Path; import sys; print((Path(sys.argv[1])/'outputs/acceptance.txt').read_bytes().hex())", employee["root"]) == ARTIFACT.hex()


def test_runtime_alias_is_on_the_workspace_and_the_chat_sandbox_and_not_on_the_relay_and_the_probe(compose, session, employee):
    daemon = json.loads(compose.invoke(["docker", "info", "--format", "{{json .}} "]).stdout)
    default_runtime = daemon["DefaultRuntime"]
    chat_thread = session.thread()
    session.upload(chat_thread)
    session.run(chat_thread)
    chat = compose.sandbox(chat_thread)
    assert chat["HostConfig"]["Runtime"] == compose.runtime
    sandbox_id = chat["Config"]["Labels"]["deerflow.sandbox_id"]
    relays = [entry for entry in compose.containers() if (entry["Config"].get("Labels") or {}).get("deerflow.role") == "network-proxy" and entry["Config"]["Labels"].get("deerflow.sandbox_id") == sandbox_id]
    assert len(relays) == 1 and relays[0]["HostConfig"]["Runtime"] == default_runtime
    _identity, resident = workspace(compose, employee)
    resident_sid = resident["Config"]["Labels"]["deerflow.sandbox_id"]
    resident_relays = [entry for entry in compose.containers() if (entry["Config"].get("Labels") or {}).get("deerflow.role") == "network-proxy" and entry["Config"]["Labels"].get("deerflow.sandbox_id") == resident_sid]
    assert len(resident_relays) == 1 and resident_relays[0]["HostConfig"]["Runtime"] == default_runtime
    with ProbeObserver(compose) as observer:
        for _retry in range(3):
            identity, _entry = workspace(compose, employee)
            compose.invoke(["docker", "rm", "-f", identity["container_id"]])
            session.run(employee["thread_id"])
            if observer.facts:
                break
        assert observer.facts, "No actual probe inspect survived three bounded preparations"
        assert all(entry["HostConfig"]["Runtime"] == default_runtime for entry in observer.facts)
        evidence = Path(os.environ.get("EVIDENCE", os.environ["TMPDIR"]))
        (evidence / "probe-runtime.json").write_text(
            json.dumps({"default_runtime": default_runtime, "probes": [{"id": entry["Id"], "runtime": entry["HostConfig"]["Runtime"], "labels": entry["Config"]["Labels"]} for entry in observer.facts]}), encoding="utf-8"
        )
    _identity, current = workspace(compose, employee)
    assert current["HostConfig"]["Runtime"] == compose.runtime


def test_work_activation_on_a_real_workspace_settles_succeeded_and_submits_the_outcome(compose, session, employee):
    work = delegate(session, employee, "acceptance:work:outcome")
    activated = activate(session, employee, work)
    settled = await_attempt(session, employee, work["id"], {"succeeded"})
    assert settled["attempt"]["id"] == activated["activation_receipt"]["attempt_id"]
    assert settled["attempt"]["contained"] is None
    assert settled["status"] == "submitted" and settled["outcome"]["attempt_id"] == settled["attempt"]["id"]
    assert settled["outcome"]["statement"] == "Acceptance outcome: outputs/proof.txt written."
    proof = session.request("GET", f"/api/threads/{employee['thread_id']}/artifacts/mnt/user-data/outputs/proof.txt")
    assert proof.content == b"WORK-ACCEPTANCE-PROOF\n"
    _identity, entry = workspace(compose, employee)
    assert compose.invoke(["docker", "exec", entry["Id"], "cat", "/mnt/user-data/outputs/proof.txt"]).stdout == "WORK-ACCEPTANCE-PROOF\n"


def test_attempt_in_flight_leaves_a_slot_for_an_ordinary_chat(compose, session, employee):
    held, identity = held_attempt(compose, session, employee)
    compose.state["graceful_work"] = held
    compose.state["graceful_identity"] = identity
    capacity = session.request("GET", work_path(employee)).json()["work_capacity"]
    replicas = json.loads(compose.gateway("python", "-c", "import os, json, yaml; from pathlib import Path; print(json.dumps(yaml.safe_load(Path(os.environ['DEER_FLOW_CONFIG_PATH']).read_text(encoding='utf-8'))['sandbox']['replicas']))"))
    assert capacity["max"] == max(1, replicas - 1) and capacity["in_use"] == 1 and len(capacity["running"]) == 1
    assert capacity["running"][0]["work_id"] == held["id"]
    thread = session.thread()
    session.upload(thread)
    session.run(thread)
    artifact(session, thread)
    assert compose.sandbox(thread)["State"]["Running"]
    another = delegate(session, employee, "acceptance:work:outcome")
    refusal = activate(session, employee, another, expected=409)
    assert refusal["reason"] == "work-limit"
    assert session.request("GET", work_path(employee, another["id"])).json()["attempt"] is None
    assert workspace(compose, employee)[0] == identity


def test_graceful_gateway_restart_leaves_no_unresolved_attempt(compose, session, employee):
    held = compose.state["graceful_work"]
    compose.run("restart", "gateway", timeout=180)
    compose.run("up", "-d", "--wait", timeout=240)
    settled = await_attempt(session, employee, held["id"], {"cancelled", "failed"})
    assert settled["attempt"]["id"] == held["attempt"]["id"]
    unresolved = compose.psql("SELECT count(*) FROM agent_work_attempts WHERE status IN ('starting','running','stopping','uncertain')")
    assert unresolved == "0"


def test_killed_gateway_settles_the_attempt_failed_with_an_attempt_contained_event(compose, session, employee):
    held, identity = held_attempt(compose, session, employee)
    startup = compose.logs().count("agents: ready (")
    compose.run("kill", "-s", "KILL", "gateway")
    compose.run("up", "-d", "--wait", timeout=240)
    settled = await_attempt(session, employee, held["id"], {"failed"})
    assert settled["attempt"]["id"] == held["attempt"]["id"] and settled["attempt"]["contained"]["reason"] == "run-orphaned"
    events = session.request("GET", work_path(employee, held["id"]) + "/history").json()["events"]
    contained = [event for event in events if event["action"] == "attempt_contained"]
    assert len(contained) == 1
    event = contained[0]
    assert event["actor_kind"] == "host"
    assert event["report"]["attempt_id"] == held["attempt"]["id"] and event["report"]["attachment_id"] == identity["attachment_id"] and event["report"]["reason"] == "run-orphaned"
    assert eventually(lambda: container_absent(compose, identity["container_id"]))
    assert eventually(lambda: container_absent(compose, "hartmesh-space-" + identity["attachment_id"]))

    def startup_settled():
        lines = [line for line in compose.logs().splitlines() if "agents: ready (" in line]
        return len(lines) > startup and "1 attachment fenced at start; 1 work attempt settled" in lines[-1]

    assert eventually(startup_settled)
    compose.state["contained_work"] = settled


def test_new_activation_is_admitted_after_a_contained_attempt(compose, session, employee):
    prior = compose.state["contained_work"]
    fresh = session.request("GET", work_path(employee, prior["id"])).json()
    edited = session.request(
        "POST",
        work_path(employee, prior["id"]) + "/commands",
        json={"operation_id": uuid.uuid4().hex, "expected_revision": fresh["revision"], "expected_assignment_revision": fresh["assignment_revision"], "action": "edit", "objective": "acceptance:work:outcome"},
    ).json()
    activated = activate(session, employee, edited)
    assert activated["activation_receipt"]["attempt_id"] != prior["attempt"]["id"]
    settled = await_attempt(session, employee, prior["id"], {"succeeded"})
    assert settled["status"] == "submitted" and settled["outcome"]["attempt_id"] == activated["activation_receipt"]["attempt_id"]
    _identity, entry = workspace(compose, employee)
    assert compose.invoke(["docker", "exec", entry["Id"], "cat", "/mnt/user-data/outputs/proof.txt"]).stdout == "WORK-ACCEPTANCE-PROOF\n"


def give_file(session, employee, content):
    instance_id = employee["instance"]["id"]
    session.request("POST", f"/api/agent-instances/{instance_id}/home/files", json={"operation_id": uuid.uuid4().hex, "action": "remove", "path": "uploads/input.txt"})
    saved = session.request(
        "POST",
        f"/api/agent-instances/{instance_id}/conversations/{employee['thread_id']}/files",
        params={"operation_id": uuid.uuid4().hex},
        files={"file": ("input.txt", content, "text/plain")},
        expected=201,
    ).json()
    assert saved["path"] == "uploads/input.txt" and saved["sha256"] == hashlib.sha256(content).hexdigest()
    assert saved["size"] == len(content) and saved["sandbox_path"] == "/mnt/user-data/uploads/input.txt"
    return [{"filename": "input.txt", "path": saved["sandbox_path"], "size": saved["size"], "status": "uploaded"}]


def test_file_given_while_the_workspace_runs_is_read_by_the_running_agent(compose, session, employee):
    identity, _entry = workspace(compose, employee)
    content = MARKER + b"-while-running"
    files = give_file(session, employee, content)
    answered = time.monotonic()
    assert compose.invoke(["docker", "exec", identity["container_id"], "cat", "/mnt/user-data/uploads/input.txt"], timeout=2).stdout.encode() == content
    assert time.monotonic() - answered < 2
    assert workspace(compose, employee)[0] == identity
    session.run(employee["thread_id"], files=files)
    assert workspace(compose, employee)[0] == identity


def test_file_given_while_no_workspace_runs_is_present_at_the_next_run(compose, session, employee):
    previous, _entry = workspace(compose, employee)
    assert compose.ai("close-workspace", employee["instance"]["id"])["verdict"] == "closed"
    files = give_file(session, employee, MARKER + b"-while-closed")
    assert not any(entry["Name"].startswith("/hartmesh-space-") for entry in compose.containers())
    session.run(employee["thread_id"], files=files)
    assert workspace(compose, employee)[0]["container_id"] != previous["container_id"]


def test_forty_mebibyte_file_arrives_with_the_same_sha256(compose, session, employee, tmp_path):
    filesystem = compose.invoke(["stat", "-f", "-c", "%T", str(tmp_path)]).stdout.strip()
    assert filesystem not in ("tmpfs", "ramfs"), "large fixtures must be disk-backed"
    payload = tmp_path / "forty-mebibytes.bin"
    random_bytes, digest = random.Random(726), hashlib.sha256()
    with payload.open("wb") as stream:
        for _ in range(640):
            chunk = random_bytes.randbytes(65536)
            stream.write(chunk)
            digest.update(chunk)
    assert payload.stat().st_size == 40 * 1024 * 1024
    identity, _entry = workspace(compose, employee)
    with payload.open("rb") as stream:
        saved = session.request(
            "PUT",
            f"/api/agent-instances/{employee['instance']['id']}/home/content",
            params={"path": "uploads/forty-mebibytes.bin", "operation_id": uuid.uuid4().hex},
            headers={"Content-Length": str(payload.stat().st_size), "Content-Type": "application/octet-stream"},
            content=iter(lambda: stream.read(65536), b""),
        ).json()
    assert saved["sha256"] == digest.hexdigest() and saved["size"] == payload.stat().st_size
    assert compose.invoke(["docker", "exec", identity["container_id"], "sha256sum", "/mnt/user-data/uploads/forty-mebibytes.bin"]).stdout.split()[0] == digest.hexdigest()
    assert workspace(compose, employee)[0] == identity


def test_stop_ends_a_held_attempt_and_leaves_the_assignment_open(compose, session, employee):
    held, identity = held_attempt(compose, session, employee)
    stopped = session.request(
        "POST",
        work_path(employee, held["id"]) + "/commands",
        json={"operation_id": uuid.uuid4().hex, "expected_revision": held["revision"], "expected_assignment_revision": held["assignment_revision"], "action": "stop", "attempt_id": held["attempt"]["id"]},
    ).json()
    assert stopped["attempt"]["status"] == "stopping" and stopped["status"] == held["status"] == "open"
    settled = await_attempt(session, employee, held["id"], {"cancelled"}, timeout=60)
    assert settled["status"] == "open" and settled["attempt"]["id"] == held["attempt"]["id"]
    assert workspace(compose, employee)[0] == identity
    edited = session.request(
        "POST",
        work_path(employee, held["id"]) + "/commands",
        json={"operation_id": uuid.uuid4().hex, "expected_revision": settled["revision"], "expected_assignment_revision": settled["assignment_revision"], "action": "edit", "objective": "acceptance:work:outcome"},
    ).json()
    activated = activate(session, employee, edited)
    assert activated["activation_receipt"]["attempt_id"] != held["attempt"]["id"]
    assert await_attempt(session, employee, held["id"], {"succeeded"})["status"] == "submitted"


def assistant_slots(compose):
    return sum(slot["use"] == "assistant" for slot in compose.ai("verify")["spaces"])


def create_agent(session, employee, custody, name, *, expected=201):
    return session.request("POST", "/api/agent-instances", json={"creation_id": uuid.uuid4().hex, "name": name, "definition_name": employee["definition"], "custody": custody}, expected=expected).json()


def listed_agent(compose, instance_id, *, all=False):
    document = compose.ai("list", *(["--all"] if all else []))
    return next(instance for instance in document["instances"] if instance["id"] == instance_id)


def test_creation_past_the_count_is_refused_for_each_kind(compose, session, employee):
    counts = session.request("GET", "/api/features").json()["agent_instances"]
    personal = counts["ai_assistants"]
    assert set(personal) == {"per_user", "mine", "allowed", "in_use"}
    remaining = personal["per_user"] - personal["mine"]
    assert 0 <= remaining <= 10
    for _ in range(remaining):
        previous_mine = personal["mine"]
        create_agent(session, employee, "personal", "Counted AI assistant " + uuid.uuid4().hex)
        personal = session.request("GET", "/api/features").json()["agent_instances"]["ai_assistants"]
        assert personal["mine"] == previous_mine + 1
    before = compose.psql("SELECT count(*) FROM agent_instances")
    refused = create_agent(session, employee, "personal", "Refused AI assistant", expected=409)
    assert refused["reason"] == "ai-assistant-limit"
    assert {key: refused[key] for key in personal} == personal and personal["mine"] == personal["per_user"]
    assert compose.psql("SELECT count(*) FROM agent_instances") == before
    occupied = assistant_slots(compose)
    company_count = session.request("GET", "/api/features").json()["agent_instances"]["ai_employees"]
    assert set(company_count) == {"allowed", "in_use"} and company_count["allowed"] > company_count["in_use"]
    last = None
    remaining = company_count["allowed"] - company_count["in_use"]
    assert 1 <= remaining <= 100
    for _ in range(remaining):
        previous_in_use = company_count["in_use"]
        last = create_agent(session, employee, "company", "Counted AI employee " + uuid.uuid4().hex)
        listed = listed_agent(compose, last["id"])
        assert listed["kind"] == "employee" and compose.state["inventory"][listed["space"]["slot_id"]]["kind"] == "employee"
        company_count = session.request("GET", "/api/features").json()["agent_instances"]["ai_employees"]
        assert company_count["in_use"] == previous_in_use + 1
    assert last is not None
    before = compose.psql("SELECT count(*) FROM agent_instances")
    refused = create_agent(session, employee, "company", "Refused AI employee", expected=409)
    assert refused["reason"] == "ai-employee-limit" and refused["allowed"] == refused["in_use"] == company_count["allowed"]
    assert compose.psql("SELECT count(*) FROM agent_instances") == before
    assert session.request("GET", "/api/agent-instances", params={"include_provisioning": "true"}).json()["unfinished"] == []
    assert assistant_slots(compose) == occupied
    compose.state["last_company_employee"] = last
    compose.state["assistant_slots_in_use"] = occupied


def test_deleted_agents_slot_is_reused_by_the_same_kind_at_once(compose, session, employee):
    previous = compose.state["last_company_employee"]
    slot = listed_agent(compose, previous["id"])["space"]["slot_id"]
    removed = session.request(
        "POST",
        f"/api/agent-instances/{previous['id']}/lifecycle",
        json={"operation_id": uuid.uuid4().hex, "generation": previous["generation"], "action": "delete"},
    ).json()
    assert removed["complete"] is True and type(removed["space_released"]) is bool
    if not removed["space_released"]:
        assert eventually(lambda: listed_agent(compose, previous["id"], all=True)["space"]["release"] == "done", timeout=180, interval=15)
    replacement = create_agent(session, employee, "company", "Replacement AI employee")
    listed = listed_agent(compose, replacement["id"])
    assert listed["space"]["slot_id"] == slot and listed["kind"] == "employee"
    assert compose.state["inventory"][slot]["kind"] == "employee"
    assert assistant_slots(compose) == compose.state["assistant_slots_in_use"]
    session.request("POST", f"/api/agent-instances/{previous['id']}/lifecycle", json={"operation_id": uuid.uuid4().hex, "generation": removed["instance"]["generation"], "action": "restore"}, expected=404)


def new_conversation(session, employee):
    thread = session.request("POST", f"/api/agent-instances/{employee['instance']['id']}/conversations", json={"creation_id": uuid.uuid4().hex}, expected=201).json()
    return {**employee, "thread_id": thread["thread_id"]}


def messages(session, employee):
    result = session.request("GET", f"/api/langgraph/threads/{employee['thread_id']}/state").json()["values"]["messages"]
    assert result
    return result


def test_assignment_that_meets_a_blocked_host_finishes_and_records_the_denial(compose, session, employee):
    context = new_conversation(session, employee)
    work = delegate(session, context, "acceptance:work:denied")
    activate(session, context, work)
    settled = await_attempt(session, context, work["id"], {"succeeded"})
    assert settled["status"] == "submitted"
    stored = messages(session, context)
    assert any(message.get("type") == "tool" and message.get("name") == "bash" and (message.get("artifact") or {}).get("sandbox_network_denied") == {"host": "example.org", "port": 443} for message in stored)
    assert not any((message.get("artifact") or {}).get("human_input", {}).get("source") == "sandbox_network" for message in stored)


def test_a_question_in_an_assignment_blocks_it_and_reaches_the_supervisor(compose, session, employee):
    context = new_conversation(session, employee)
    work = delegate(session, context, "acceptance:work:ask")
    activate(session, context, work)
    settled = await_attempt(session, context, work["id"], {"succeeded"})
    assert settled["status"] == "blocked" and settled["human_input_request_id"]
    request = session.request("GET", "/api/human-input/" + settled["human_input_request_id"]).json()
    assert request["question"] == "Which input should I use for this assignment?"
    assert employee["instance"]["supervisor"]["kind"] == "human"
    assert request["recipient_id"] == employee["instance"]["supervisor"]["subject_id"] == session.user["id"] and not request["needs_routing"]
    stored = messages(session, context)
    assert not any(
        message.get("name") == "ask_clarification"
        or (message.get("artifact") or {}).get("human_input")
        or any(call.get("name") == "ask_clarification" or call.get("function", {}).get("name") == "ask_clarification" for call in message.get("tool_calls", []))
        for message in stored
    )


def contained_run(session, employee, held, reason, *, timeout):
    def terminal():
        work = session.request("GET", work_path(employee, held["id"])).json()
        run = session.request("GET", f"/api/langgraph/threads/{employee['thread_id']}/runs/{held['attempt']['run_id']}").json()
        if work["attempt"]["status"] != "failed" or run["status"] != "error":
            return None
        assert work["attempt"]["id"] == held["attempt"]["id"] and work["attempt"]["contained"]["reason"] == reason
        return work

    return eventually(terminal, timeout=max(0.01, timeout), interval=2)


def test_closing_the_workspace_under_a_held_attempt_ends_its_run_with_the_reason_operator(compose, session, employee):
    held, _identity = held_attempt(compose, session, employee)
    started = time.monotonic()
    closed = compose.ai("close-workspace", employee["instance"]["id"], "--end-running-work", "--wait-seconds", "60", timeout=75)
    assert closed["verdict"] == "closed" and closed["runs_found"] == closed["runs_ended"] == 1
    contained_run(session, employee, held, "operator", timeout=60 - (time.monotonic() - started))
    assert time.monotonic() - started <= 60


def test_a_killed_workspace_ends_its_run_with_the_reason_container_absent(compose, session, employee):
    held, identity = held_attempt(compose, session, employee)
    started = time.monotonic()
    compose.invoke(["docker", "kill", identity["container_id"]])
    contained_run(session, employee, held, "container-absent", timeout=60 - (time.monotonic() - started))
    assert container_absent(compose, identity["container_id"])
    assert container_absent(compose, "hartmesh-space-" + identity["attachment_id"])
    assert time.monotonic() - started <= 60
