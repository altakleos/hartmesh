"""What a chat app shows a person names the product the deployment configured, or nothing at all."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

from app.channels import manager
from app.channels.service import ChannelService
from deerflow.config.ui_config import UiConfig


def _started_config(monkeypatch, app_config) -> dict:
    captured: dict = {}

    class StubChannel:
        def __init__(self, bus, config):
            captured.update(config)
            self.is_running = True

        async def start(self):
            pass

    monkeypatch.setattr("deerflow.reflection.resolve_class", lambda path, base_class=None: StubChannel)
    service = ChannelService(channels_config={}, app_config=app_config)
    assert asyncio.run(service._start_channel("dingtalk", {"client_id": "x", "client_secret": "y"}))
    return captured


def test_each_channel_is_handed_the_configured_product_name(monkeypatch) -> None:
    assert _started_config(monkeypatch, SimpleNamespace(ui=UiConfig(product_name="Acme Assist")))["product_name"] == "Acme Assist"


def test_a_service_without_a_ui_section_hands_the_default(monkeypatch) -> None:
    assert _started_config(monkeypatch, None)["product_name"] == "HartMesh"


def test_the_binding_replies_name_no_product() -> None:
    # Sent into a chat by whichever deployment runs the bridge: they point at
    # the web app's Settings and the administrator, not at a framework's name.
    for message in (manager.BOUND_IDENTITY_REQUIRED_MESSAGE, manager.BOUND_IDENTITY_UNAVAILABLE_MESSAGE):
        assert "DeerFlow" not in message
    assert "Settings" in manager.BOUND_IDENTITY_REQUIRED_MESSAGE
    assert "administrator" in manager.BOUND_IDENTITY_UNAVAILABLE_MESSAGE
