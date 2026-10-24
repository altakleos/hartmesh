"""Offline helper tests and AST-only checks of privileged qualification wiring."""

import ast
import importlib
import sys
import traceback
from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

_LOSS_CASES = (
    "test_bundle_runs_an_agent_and_its_space_survives_gateway_restart_and_workspace_loss",
    "test_runtime_alias_is_on_the_workspace_and_the_chat_sandbox_and_not_on_the_relay_and_the_probe",
)


def _calls(node, name):
    return [call for call in ast.walk(node) if isinstance(call, ast.Call) and isinstance(call.func, ast.Name) and call.func.id == name]


def _run_calls(node):
    return [call for call in ast.walk(node) if isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute) and isinstance(call.func.value, ast.Name) and call.func.value.id == "session" and call.func.attr == "run"]


@pytest.mark.parametrize("case_name", _LOSS_CASES)
def test_loss_recovery_awaits_captured_workspace_retirement(case_name):
    # Parse source only. Never import, compile or execute the acceptance module.
    source = Path(__file__).with_name("test_compose_instance_acceptance.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    case = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == case_name)
    _assert_loss_order(case)


def _assert_loss_order(case):
    kills = [
        call
        for call in ast.walk(case)
        if isinstance(call, ast.Call)
        and isinstance(call.func, ast.Attribute)
        and call.func.attr == "invoke"
        and call.args
        and isinstance(call.args[0], ast.List)
        and [getattr(value, "value", None) for value in call.args[0].elts[:3]] == ["docker", "rm", "-f"]
    ]
    assert len(kills) == 1
    kill = kills[0]
    body = next(node.body for node in ast.walk(case) if isinstance(getattr(node, "body", None), list) and any(isinstance(stmt, ast.Expr) and stmt.value is kill for stmt in node.body))
    index = next(index for index, statement in enumerate(body) if isinstance(statement, ast.Expr) and statement.value is kill)
    runs = [(index + 1 + offset, call) for offset, statement in enumerate(body[index + 1 :]) for call in _run_calls(statement)]
    assert len(runs) == 1, "Each deliberate loss has one later fresh run, never a POST retry loop"
    run_index, run = runs[0]
    barriers = [
        statement.value
        for statement in body[index + 1 : run_index]
        if isinstance(statement, ast.Expr) and isinstance(statement.value, ast.Call) and isinstance(statement.value.func, ast.Name) and statement.value.func.id == "await_workspace_retirement"
    ]
    assert len(barriers) == 1, f"{case.name}: native removal at line {kill.lineno} reaches model POST at line {run.lineno} without observing captured retirement and fresh qualification"
    captured = barriers[0].args[-1]
    assert isinstance(captured, ast.Name)
    captures = [
        statement
        for statement in body[:index]
        if isinstance(statement, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == captured.id for target in statement.targets)
        and isinstance(statement.value, ast.Call)
        and isinstance(statement.value.func, ast.Name)
        and statement.value.func.id == "capture_workspace_retirement"
    ]
    assert len(captures) == 1, "Capture the current scope within this exact function/loop body before its kill"
    assert ast.unparse(kill.args[0].elts[-1]) == captured.id + ".container_id"


def test_conditional_dead_barrier_cannot_satisfy_the_source_order_guard():
    case = ast.parse("""
def example():
    captured = capture_workspace_retirement(compose, employee)
    compose.invoke(["docker", "rm", "-f", captured.container_id])
    if False:
        await_workspace_retirement(compose, session, captured)
    session.run(thread)
""").body[0]
    with pytest.raises(AssertionError, match="without observing"):
        _assert_loss_order(case)


def test_intentional_inflight_kill_keeps_its_existing_run_and_containment_semantics():
    source = Path(__file__).with_name("test_compose_instance_acceptance.py").read_text(encoding="utf-8")
    case = next(node for node in ast.parse(source).body if isinstance(node, ast.FunctionDef) and node.name == "test_a_killed_workspace_ends_its_run_with_the_reason_container_absent")
    (held,) = _calls(case, "held_attempt")
    (contained,) = _calls(case, "contained_run")
    (kill,) = [call for call in ast.walk(case) if isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute) and call.func.attr == "invoke"]
    assert held.lineno < kill.lineno < contained.lineno
    assert [getattr(value, "value", None) for value in kill.args[0].elts[:2]] == ["docker", "kill"]
    assert ast.unparse(kill.args[0].elts[2]) == "identity['container_id']"
    assert any(isinstance(value, ast.Constant) and value.value == "container-absent" for value in contained.args)
    assert ast.unparse(contained.args[2]) == "held"
    absences = _calls(case, "container_absent")
    assert [ast.unparse(call.args[1]) for call in absences] == ["identity['container_id']", "'hartmesh-space-' + identity['attachment_id']"]
    assert not _calls(case, "capture_workspace_retirement") and not _calls(case, "await_workspace_retirement")
    assert not _run_calls(case), "This case observes the already-held run, never submits a replacement"


@pytest.fixture
def api():
    return importlib.import_module("_compose_workspace_barrier")


def captured(api, **changes):
    values = dict(
        instance_id="1" * 32,
        owner_id="fixture-owner",
        instance_generation=1,
        home_id="2" * 32,
        home_generation=3,
        attachment_id="3" * 32,
        incarnation="4" * 32,
        host_id="fixture-host",
        container_id="5" * 64,
        container_name="/hartmesh-space-fixture",
        relay_id="6" * 64,
        relay_name="/deer-flow-netproxy-fixture",
    )
    return api.CapturedWorkspace(**(values | changes))


def observation(c, phase="fenced"):
    return dict(
        attachment_id=c.attachment_id,
        incarnation=c.incarnation,
        actor_kind="nonhuman",
        actor_id="agent:" + c.instance_id,
        host_id=c.host_id,
        container_id=c.container_id,
        instance_id=c.instance_id,
        instance_owner_id=c.owner_id,
        instance_generation=c.instance_generation,
        instance_status="active",
        instance_custody="personal",
        home_id=c.home_id,
        home_generation=c.home_generation,
        home_status="active",
        home_custody_kind="personal",
        home_custodian_kind="human",
        home_custodian_id=c.owner_id,
        mount_space_id=c.home_id,
        mount_generation=c.home_generation,
        mount_alias="home",
        mount_writable=1,
        phase=phase,
        fenced_by_kind="host" if phase != "active" else None,
        fenced_by_id=c.host_id if phase != "active" else None,
        fence_reason="container-absent" if phase != "active" else None,
        fenced_at="2026-10-24T00:00:00+00:00" if phase == "fenced" else None,
    )


class ManualClock:
    def __init__(self):
        self.now, self.sleeps = 0.0, []

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        assert 0 < seconds <= 1
        self.sleeps.append(seconds)
        self.now += seconds


class Reads:
    def __init__(self, c):
        self.c, self.clock, self.calls = c, ManualClock(), []
        self.rows = [observation(c)]
        self.executions = [dict(enabled=True, execution="ready", reason=None)]
        self.absent = {}

    def record(self, kind, timeout, reference=None):
        assert 0 < timeout <= 10
        self.calls.append((kind, self.clock.now, timeout, reference))

    def read_attachment(self, timeout):
        self.record("attachment", timeout)
        value = self.rows.pop(0) if len(self.rows) > 1 else self.rows[0]
        return dict(value) if isinstance(value, dict) else value

    def is_absent(self, reference, timeout):
        self.record("absence", timeout, reference)
        return self.absent.get(reference, True)

    def read_execution(self, timeout):
        self.record("execution", timeout)
        return self.executions.pop(0) if len(self.executions) > 1 else self.executions[0]

    def wait(self, api, *, timeout=3):
        return api.wait_for_workspace_retirement(self.c, read_attachment=self.read_attachment, is_absent=self.is_absent, read_execution=self.read_execution, timeout=timeout, clock=self.clock, sleep=self.clock.sleep)


def test_capture_validates_real_personal_home_and_nonhuman_actor_without_mutation(api):
    c = captured(api)
    row = observation(c, "active")
    original = dict(row)
    assert api.validate_workspace_capture(c, row) is None
    assert row == original
    with pytest.raises(FrozenInstanceError):
        c.home_generation = 99


@pytest.mark.parametrize(
    "field,value",
    [
        ("instance_generation", True),
        ("home_generation", 0),
        ("home_generation", 1.5),
        ("attachment_id", "bad"),
        ("container_id", "short"),
        ("container_name", "no-leading-slash"),
        ("relay_name", "//double"),
        ("relay_name", "/a/b"),
        ("owner_id", "bad\nowner"),
        ("host_id", ""),
        ("relay_id", "5" * 64),
        ("relay_name", "/hartmesh-space-fixture"),
        ("container_name", "/" + "5" * 64),
        ("relay_name", "/" + "5" * 64),
    ],
)
def test_capture_rejects_malformed_or_duplicate_primitive_identity(api, field, value):
    with pytest.raises(api.WorkspaceBarrierError):
        captured(api, **{field: value})


@pytest.mark.parametrize(
    "field,value",
    [
        ("instance_id", "9" * 32),
        ("instance_owner_id", "other"),
        ("instance_generation", True),
        ("instance_status", "suspended"),
        ("instance_custody", "company"),
        ("home_id", "9" * 32),
        ("home_generation", True),
        ("home_status", "deleted"),
        ("home_custodian_kind", "nonhuman"),
        ("home_custodian_id", "agent:" + "1" * 32),
        ("home_custody_kind", "company"),
        ("mount_space_id", "9" * 32),
        ("mount_generation", True),
        ("mount_alias", "other"),
        ("mount_writable", True),
        ("actor_kind", "human"),
        ("actor_id", "other"),
        ("host_id", "other"),
        ("container_id", "9" * 64),
        ("incarnation", "9" * 32),
        ("attachment_id", "9" * 32),
    ],
)
def test_initial_capture_refuses_changed_closed_scope(api, field, value):
    c = captured(api)
    row = observation(c, "active") | {field: value}
    with pytest.raises(api.WorkspaceBarrierError):
        api.validate_workspace_capture(c, row)


@pytest.mark.parametrize("row_kind", [None, "missing-key", "fenced", "false-closure"])
def test_capture_requires_complete_active_unfenced_row(api, row_kind):
    c = captured(api)
    row = observation(c, "active")
    if row_kind is None:
        row = None
    elif row_kind == "missing-key":
        row.pop("fenced_at")
    elif row_kind == "fenced":
        row = observation(c)
    else:
        row["fenced_by_kind"] = "host"
    with pytest.raises(api.WorkspaceBarrierError):
        api.validate_workspace_capture(c, row)


def test_global_ready_and_fenced_sql_alone_never_release_until_four_absences_and_fresh_ready(api):
    c = captured(api)
    reads = Reads(c)
    reads.rows = [observation(c, "active"), observation(c, "fence_pending"), observation(c), observation(c), observation(c)]
    reads.executions = [dict(enabled=True, execution="unavailable", reason="storage-unavailable"), dict(enabled=True, execution="ready", reason=None)]
    actual_absent = reads.is_absent
    physical_polls = 0

    def absent(reference, timeout):
        nonlocal physical_polls
        result = actual_absent(reference, timeout)
        if reference == c.relay_name[1:]:
            physical_polls += 1
            return physical_polls > 1
        return result

    reads.is_absent = absent
    api.wait_for_workspace_retirement(c, read_attachment=reads.read_attachment, is_absent=reads.is_absent, read_execution=reads.read_execution, clock=reads.clock, sleep=reads.clock.sleep)
    assert [kind for kind, *_ in reads.calls].count("attachment") == 5
    assert [kind for kind, *_ in reads.calls].count("execution") == 2
    assert [reference for kind, _, _, reference in reads.calls if kind == "absence"] == [c.container_id, c.container_name[1:], c.relay_id, c.relay_name[1:]] * 3


@pytest.mark.parametrize("member", ["container_id", "container_name", "relay_id", "relay_name"])
@pytest.mark.parametrize("reply", [False, None, 1, "true"])
def test_every_exact_absence_is_required_and_unknown_is_not_truthy_success(api, member, reply):
    reads = Reads(captured(api))
    reference = getattr(reads.c, member).removeprefix("/")
    reads.absent[reference] = reply
    with pytest.raises(api.WorkspaceBarrierError):
        reads.wait(api)
    assert not any(call[0] == "execution" for call in reads.calls)


@pytest.mark.parametrize(
    "field,value",
    [("fenced_by_kind", "human"), ("fenced_by_id", "other-host"), ("fence_reason", "operator"), ("fenced_at", None), ("fenced_at", "not-a-date"), ("fenced_at", "2026-10-24T00:00:00"), ("phase", "closed"), ("phase", "contained")],
)
def test_retirement_requires_actual_fault_specific_closure(api, field, value):
    reads = Reads(captured(api))
    reads.rows = [observation(reads.c) | {field: value}]
    with pytest.raises(api.WorkspaceBarrierError):
        reads.wait(api)
    assert not any(call[0] != "attachment" for call in reads.calls)


@pytest.mark.parametrize("change", ["missing", "scope", "regressed"])
@pytest.mark.parametrize("waiting_on", ["physical", "readiness"])
def test_fenced_scope_is_reread_while_physical_or_readiness_observation_waits(api, change, waiting_on):
    reads = Reads(captured(api))
    if change == "missing":
        second = None
    elif change == "scope":
        second = observation(reads.c) | {"actor_id": "other"}
    else:
        second = observation(reads.c, "active")
    reads.rows = [observation(reads.c), second]
    if waiting_on == "physical":
        reads.absent[reads.c.container_id] = False
    else:
        reads.executions = [dict(enabled=True, execution="unavailable", reason="storage-unavailable")]
    with pytest.raises(api.WorkspaceBarrierError):
        reads.wait(api)
    assert [call[0] for call in reads.calls].count("attachment") == 2
    assert [call[0] for call in reads.calls].count("execution") == (waiting_on == "readiness")


@pytest.mark.parametrize("execution", [dict(enabled=1, execution="ready", reason=None), dict(enabled=True, execution="ready"), None, dict(enabled=True, execution="ready", reason="not-qualified")])
def test_malformed_or_unqualified_features_never_release(api, execution):
    reads = Reads(captured(api))
    reads.executions = [execution]
    with pytest.raises(api.WorkspaceBarrierError):
        reads.wait(api)


@pytest.mark.parametrize("stage", ["attachment", "absence", "execution"])
def test_read_completion_at_or_after_shared_deadline_is_rejected(api, stage):
    reads = Reads(captured(api))
    method = {"attachment": "read_attachment", "absence": "is_absent", "execution": "read_execution"}[stage]
    actual = getattr(reads, method)

    def delayed(*args):
        result = actual(*args)
        reads.clock.now = 3
        return result

    setattr(reads, method, delayed)
    with pytest.raises(api.WorkspaceBarrierError):
        reads.wait(api)
    assert reads.calls[-1][0] == stage


def test_all_stages_share_one_budget_and_each_read_gets_only_remaining_time(api):
    reads = Reads(captured(api))
    reads.executions = [dict(enabled=True, execution="unavailable", reason="storage-unavailable")]
    with pytest.raises(api.WorkspaceBarrierError):
        reads.wait(api)
    assert reads.clock.now == 3
    assert reads.clock.sleeps and sum(reads.clock.sleeps) == 3
    assert all(0 < budget <= min(10, 3 - started) for _, started, budget, _ in reads.calls)


def test_each_completed_read_reduces_the_next_read_budget_without_a_sleep(api):
    reads = Reads(captured(api))
    for method in ("read_attachment", "is_absent", "read_execution"):
        actual = getattr(reads, method)

        def elapsed(*args, actual=actual):
            result = actual(*args)
            reads.clock.now += 0.4
            return result

        setattr(reads, method, elapsed)
    reads.wait(api)
    assert [call[0] for call in reads.calls] == ["attachment", "absence", "absence", "absence", "absence", "execution"]
    assert [call[2] for call in reads.calls] == pytest.approx([3, 2.6, 2.2, 1.8, 1.4, 1])
    assert reads.clock.now == pytest.approx(2.4)
    assert not reads.clock.sleeps


@pytest.mark.parametrize("timeout", [0, -1, True, float("nan"), float("inf"), 91])
def test_invalid_or_extended_deadline_is_refused_before_reads(api, timeout):
    reads = Reads(captured(api))
    with pytest.raises(api.WorkspaceBarrierError):
        reads.wait(api, timeout=timeout)
    assert not reads.calls


@pytest.mark.parametrize("stage", ["read_attachment", "is_absent", "read_execution"])
def test_callback_failures_have_fixed_diagnostics_without_reply_contents(api, stage):
    reads = Reads(captured(api))
    secret = "synthetic-private-response-do-not-print"

    def broken(*_args):
        raise RuntimeError(secret)

    setattr(reads, stage, broken)
    with pytest.raises(api.WorkspaceBarrierError) as failure:
        reads.wait(api)
    assert secret not in "".join(traceback.format_exception(failure.value))


def test_helper_imports_only_standard_library_and_has_no_effect_capabilities(api):
    tree = ast.parse(Path(api.__file__).read_text(encoding="utf-8"))
    imports = [name.name.split(".")[0] for node in tree.body if isinstance(node, ast.Import) for name in node.names]
    imports += [node.module.split(".")[0] for node in tree.body if isinstance(node, ast.ImportFrom)]
    assert set(imports) <= sys.stdlib_module_names
    assert not set(imports) & {"subprocess", "socket", "os"}
