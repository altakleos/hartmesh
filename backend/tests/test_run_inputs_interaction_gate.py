"""Queue and Steer (#7729) open only for an interactive chat a person drives.

Upstream opens a Run's input control whenever an agent-runs host and an input
store exist and ``non_interactive`` is unset. In HartMesh an AI employee's Work
attempt runs with ``interaction_mode="autonomous"`` but without that legacy
flag, and an agent-instance run or an output-captured run holds one admitted
input as its evidence. A run owner steering any of those mid-flight would break
that evidence and the Work Attention model (plan §5.2(g)). Without a Run's input
state, the Gateway refuses a Queue or Steer submission with 409.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from deerflow.runtime.runs.worker import _run_accepts_inputs

_HOST = object()
_STORE = object()


def _ctx(*, agent_runs=_HOST, agent_execution=None):
    return SimpleNamespace(agent_runs=agent_runs, agent_execution=agent_execution)


def _manager(*, input_store=_STORE):
    return SimpleNamespace(input_store=input_store)


def test_an_interactive_web_chat_accepts_inputs():
    assert _run_accepts_inputs(_ctx(), _manager(), {"configurable": {}}, {}, capture_requested=False) is True


@pytest.mark.parametrize(
    ("config", "runtime_ctx"),
    [
        ({"configurable": {}}, {"interaction_mode": "autonomous"}),
        ({"configurable": {"interaction_mode": "autonomous"}}, {}),
        ({"configurable": {}}, {"interaction_mode": "scheduled"}),
        ({"configurable": {}}, {"interaction_mode": "webhook"}),
        ({"configurable": {}}, {"non_interactive": True}),
        ({"configurable": {}}, {"disable_clarification": True}),
        ({"configurable": {}}, {"channel_name": "github"}),
    ],
    ids=["autonomous-context", "autonomous-configurable", "scheduled", "webhook", "non-interactive", "clarification-disabled", "github-channel"],
)
def test_a_run_without_a_synchronous_person_refuses_inputs(config, runtime_ctx):
    assert _run_accepts_inputs(_ctx(), _manager(), config, runtime_ctx, capture_requested=False) is False


def test_an_agent_instance_run_refuses_inputs_even_when_interactive():
    assert _run_accepts_inputs(_ctx(agent_execution=object()), _manager(), {"configurable": {}}, {}, capture_requested=False) is False


def test_an_output_captured_run_refuses_inputs():
    assert _run_accepts_inputs(_ctx(), _manager(), {"configurable": {}}, {}, capture_requested=True) is False


@pytest.mark.parametrize("missing", ["agent_runs", "input_store"])
def test_upstreams_own_preconditions_still_apply(missing):
    ctx = _ctx(agent_runs=None) if missing == "agent_runs" else _ctx()
    manager = _manager(input_store=None) if missing == "input_store" else _manager()

    assert _run_accepts_inputs(ctx, manager, {"configurable": {}}, {}, capture_requested=False) is False
