"""Fail-closed orchestration of every exact multi-Gateway live scenario."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from test_multi_gateway_qualification_evidence import _artifact

from deerflow.multi_gateway_qualification import (
    MULTI_GATEWAY_QUALIFICATION_SCENARIOS,
    MULTI_GATEWAY_SCENARIO_RESULTS,
    MultiGatewayQualificationSubjectsV1,
    MultiGatewayScenarioObservationV1,
    run_multi_gateway_qualification,
)


class _Driver:
    def __init__(self, *, wrong_scenario: bool = False) -> None:
        artifact = _artifact()
        self.subjects = MultiGatewayQualificationSubjectsV1.from_evidence(artifact)
        self.calls: list[str] = []
        self.closed = False
        self.wrong_scenario = wrong_scenario

    async def prepare(self) -> MultiGatewayQualificationSubjectsV1:
        return self.subjects

    async def run_scenario(
        self,
        scenario_id: str,
    ) -> MultiGatewayScenarioObservationV1:
        self.calls.append(scenario_id)
        actual_id = "topology_identity" if self.wrong_scenario and scenario_id == "concurrent_admission" else scenario_id
        proof = next(item for item in _artifact().scenarios if item.scenario_id == scenario_id)
        return MultiGatewayScenarioObservationV1(
            scenario_id=actual_id,
            input_facts={"case": scenario_id},
            evidence_facts={"observed": MULTI_GATEWAY_SCENARIO_RESULTS[scenario_id]},
            authoritative_count=proof.authoritative_count,
            duplicate_count=proof.duplicate_count,
            stale_write_rejections=proof.stale_write_rejections,
            takeover_count=proof.takeover_count,
            pod_deletion_count=proof.pod_deletion_count,
            pod_restart_count=proof.pod_restart_count,
            lease_epoch_before=proof.lease_epoch_before,
            lease_epoch_after=proof.lease_epoch_after,
            dependency_interruption_count=proof.dependency_interruption_count,
            duration_millis=100,
            verified_case_count=proof.verified_case_count,
            cleanup_count=proof.cleanup_count,
            retryable_failure_count=proof.retryable_failure_count,
        )

    async def close(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_runner_executes_all_scenarios_in_canonical_order() -> None:
    driver = _Driver()
    now = datetime(2026, 9, 1, 12, 0, tzinfo=UTC)

    evidence = await run_multi_gateway_qualification(
        driver,
        qualification_id="qualification-09",
        clock=lambda: now,
    )

    assert tuple(driver.calls) == MULTI_GATEWAY_QUALIFICATION_SCENARIOS
    assert tuple(item.scenario_id for item in evidence.scenarios) == (MULTI_GATEWAY_QUALIFICATION_SCENARIOS)
    assert driver.closed is True


@pytest.mark.asyncio
async def test_runner_rejects_driver_scenario_substitution_and_closes() -> None:
    driver = _Driver(wrong_scenario=True)

    with pytest.raises(ValueError, match="unexpected scenario"):
        await run_multi_gateway_qualification(
            driver,
            qualification_id="qualification-09",
        )

    assert driver.closed is True
