"""Instance event delegation retains the synchronous native leaf off loop."""

import pytest
import test_agent_network_policy as policy


@pytest.mark.asyncio
async def test_instance_denial_claim_and_decision_drain_under_cancellation():
    await policy.test_async_agent_denial_runs_off_loop_and_drains_repeated_cancellation()
