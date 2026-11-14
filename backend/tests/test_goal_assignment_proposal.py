"""Checkpointed proposals wait for consent without advancing an active goal."""

from copy import deepcopy

import pytest
from langchain_core.messages import AIMessage, HumanMessage
from langgraph.checkpoint.memory import InMemorySaver
from test_agent_chat_tools import context
from test_assignment_proposal_runtime import build
from test_goal_worker import _CollectingBridge, _full_accessor, _seed_goal_thread

from deerflow.runtime.goal import GoalEvaluation, read_thread_goal
from deerflow.runtime.runs import worker


def proposed():
    graph, model, _effects, incoming = build({"shows_cards": ["assignment_card"]})
    messages = graph.invoke(incoming, context=context("company"))["messages"]
    assert model.calls == 1
    return messages


async def decide(monkeypatch, messages, *, stop_reason=None, pending=False):
    saver = InMemorySaver()
    await _seed_goal_thread(saver, thread_id="proposal-goal", goal_text="Review the guide", messages=messages)
    if pending:
        latest = await saver.aget_tuple({"configurable": {"thread_id": "proposal-goal", "checkpoint_ns": ""}})
        await saver.aput_writes(latest.config, [("messages", AIMessage(content="Pending write"))], "unfinished-task")
    evaluations = []

    async def evaluate(*args, **kwargs):
        evaluations.append(args)
        return GoalEvaluation(satisfied=False, blocker="goal_not_met_yet", reason="More work remains", evidence_summary="")

    monkeypatch.setattr(worker, "evaluate_goal_completion", evaluate)
    continuation = await worker._prepare_goal_continuation_input(
        accessor=_full_accessor(saver),
        bridge=_CollectingBridge(),
        checkpointer=saver,
        thread_id="proposal-goal",
        run_id="proposal-run",
        model_name=None,
        app_config=None,
        run_stop_reason=stop_reason,
    )
    goal = await read_thread_goal(saver, "proposal-goal")
    return continuation, goal, evaluations


@pytest.mark.asyncio
@pytest.mark.parametrize("hidden", [False, True])
async def test_real_proposal_stands_down_without_evaluator_or_continuation(monkeypatch, hidden):
    messages = proposed()
    if hidden:
        messages.append(HumanMessage(content="Hidden reminder", additional_kwargs={"hide_from_ui": True}))
    continuation, goal, evaluations = await decide(monkeypatch, messages)
    assert continuation is None and evaluations == []
    assert goal["status"] == "active" and goal["continuation_count"] == 0
    assert goal["last_evaluation"]["blocker"] == "needs_user_input"
    assert goal["last_evaluation"]["stand_down_reason"] == "blocked:needs_user_input"


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["issuer", "call", "draft", "version", "new-human", "answer-here", "later-ai"])
async def test_stale_or_invalid_proposal_does_not_acquire_waiting_authority(monkeypatch, damage):
    messages = deepcopy(proposed())
    if damage == "issuer":
        messages[-1].id = "copied-result"
    elif damage == "call":
        messages[-1].tool_call_id = "foreign-call"
    elif damage == "draft":
        messages[-1].artifact["assignment_card"]["draft"]["objective"] = "Changed objective"
    elif damage == "version":
        messages[-1].artifact["assignment_card"]["version"] = True
    elif damage == "new-human":
        messages.append(HumanMessage(content="Continue here", additional_kwargs={"shows_cards": ["assignment_card"]}))
    elif damage == "answer-here":
        messages[0].additional_kwargs["assignment_card_response"] = {"choice": "answer_here"}
    else:
        messages.append(AIMessage(content="Work remains"))
    _continuation, goal, _evaluations = await decide(monkeypatch, messages)
    assert goal["last_evaluation"]["blocker"] != "needs_user_input"


@pytest.mark.asyncio
async def test_just_answer_turn_resumes_goal_evaluation(monkeypatch):
    messages = proposed() + [
        HumanMessage(content="Do it here", additional_kwargs={"assignment_card_response": {"version": 1, "kind": "assignment_card_response", "tool_call_id": "reused-provider-call", "choice": "answer_here"}}),
        AIMessage(content="I reviewed the guide; validation remains."),
    ]
    continuation, goal, evaluations = await decide(monkeypatch, messages)
    assert continuation is not None and len(evaluations) == 1
    assert goal["continuation_count"] == 1


@pytest.mark.asyncio
async def test_step_limit_remains_a_failure_even_with_a_proposal(monkeypatch):
    continuation, goal, evaluations = await decide(monkeypatch, proposed(), stop_reason="recursion_capped")
    assert continuation is None and evaluations == []
    assert goal["last_evaluation"]["blocker"] == "run_failed"


@pytest.mark.asyncio
async def test_pending_checkpoint_cannot_establish_a_durable_proposal_wait(monkeypatch):
    continuation, goal, evaluations = await decide(monkeypatch, proposed(), pending=True)
    assert continuation is None and evaluations == []
    assert goal["last_evaluation"]["blocker"] == "run_failed"
