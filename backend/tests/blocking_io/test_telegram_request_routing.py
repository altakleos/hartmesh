"""Routing shares real SQL and HTTP custody without blocking the event loop."""

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_request_routing import test_route_is_receipted_once_works_during_pause_and_grants_nothing as route_once
from test_telegram_work_actions import actions as actions


@pytest.mark.asyncio
async def test_route_receipt_and_recipient_read(actions, monkeypatch):
    await route_once(actions)
    # route_once changed the request revision, so this wrapper exercises the
    # read separately without submitting the fixture's original reply basis.
    from app.gateway.routers import agent_instances
    from app.telegram_bots.work_routing import WorkRoutingReader
    from deerflow.config import agents_api_config

    e = actions
    monkeypatch.setattr(agents_api_config, "_agents_api_config", agents_api_config.AgentsApiConfig(enabled=True))
    e.http_app.include_router(agent_instances.router)
    reader = WorkRoutingReader(work=e.human.work, authority=e.live.authority, links=e.db.links, client=e.http_client)
    assert (await reader.read_routing(e.link, e.request["id"])).recipients[0]["user_id"] == e.actor.subject_id
