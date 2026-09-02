"""Deterministic contracts for the live recovery qualification probes."""

from __future__ import annotations

import json

import pytest
import support.multi_gateway_qualification as qualification_module
from support.kubernetes_qualification import (
    QualificationCommandError,
    QualificationTimeout,
)
from support.multi_gateway_qualification import (
    KubernetesMultiGatewayQualificationDriverV1,
)


def _driver() -> KubernetesMultiGatewayQualificationDriverV1:
    return object.__new__(KubernetesMultiGatewayQualificationDriverV1)


def _receipt_rows() -> str:
    return json.dumps(
        {
            "starts": 1,
            "receipt_ids": ["tr_" + ("a" * 64)],
        }
    )


def test_accepted_commitment_is_canonical_and_requires_exact_two_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    driver = _driver()
    accepted = {
        "recovery_policy": "exact_two_takeover_v1",
        "caller_intent_json": {"messages": [], "version": 1},
        "agent_revision_digest": "a" * 64,
    }
    monkeypatch.setattr(
        driver,
        "_postgres",
        lambda _query: json.dumps(accepted),
    )
    first = driver._accepted_run_commitment("run-1")
    monkeypatch.setattr(
        driver,
        "_postgres",
        lambda _query: json.dumps(dict(reversed(tuple(accepted.items())))),
    )

    assert driver._accepted_run_commitment("run-1") == first
    assert first[0] == "exact_two_takeover_v1"
    assert first[1].startswith("sha256:")

    monkeypatch.setattr(
        driver,
        "_postgres",
        lambda _query: json.dumps({**accepted, "recovery_policy": "terminalize_v1"}),
    )
    with pytest.raises(
        QualificationCommandError,
        match="omitted its immutable recovery policy",
    ):
        driver._accepted_run_commitment("run-1")


def test_reconciled_probe_requires_one_receipt_and_one_external_body(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    driver = _driver()
    commands: list[tuple[str, ...]] = []
    monkeypatch.setattr(driver, "_postgres", lambda _query: _receipt_rows())

    def kubectl(*arguments: str, **_kwargs) -> str:
        commands.append(arguments)
        path = arguments[-1]
        if path.endswith("/execution-count"):
            return "1"
        if path.endswith("/result"):
            return "qualification-complete"
        raise AssertionError(f"unexpected qualification probe: {arguments!r}")

    monkeypatch.setattr(driver, "_kubectl", kubectl)

    facts = driver._reconciled_tool_operation_facts(
        "run-1",
        sandbox_pod_name="sandbox-1",
        require_result=True,
    )

    assert facts == {
        "tool_receipt_id": "tr_" + ("a" * 64),
        "external_tool_executions": 1,
        "reconciled_result_digest": ("sha256:dd742cccc9252bbc1b85035d2f86ece250ec9a5b7d7a6b9a698b027a65d4e381"),
    }
    assert len(commands) == 2


def test_opted_in_recovery_probe_fails_when_sandbox_evidence_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    driver = _driver()
    monkeypatch.setattr(driver, "_postgres", lambda _query: _receipt_rows())
    monkeypatch.setattr(
        driver,
        "_kubectl",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(QualificationCommandError("sandbox unavailable")),
    )

    def fail_fast(predicate, *, description: str, **_kwargs) -> None:
        assert description == "one receipt-keyed sandbox execution body"
        if not predicate():
            raise QualificationTimeout("missing recovery infrastructure")

    monkeypatch.setattr(qualification_module, "wait_until", fail_fast)

    with pytest.raises(
        QualificationTimeout,
        match="missing recovery infrastructure",
    ):
        driver._reconciled_tool_operation_facts(
            "run-1",
            sandbox_pod_name="sandbox-1",
            require_result=False,
        )
