"""Final effect authority uses one borrowed writer without event-loop I/O."""

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_work_actions import actions as actions
from test_telegram_work_effect_authority import test_route_effect_rolls_back_changes_during_final_provider_await as revoke


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["recipient_disabled", "binding", "cancelled"])
async def test_final_effect_revocation(actions, monkeypatch, change):
    await revoke(actions, monkeypatch, change)
