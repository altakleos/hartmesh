"""Conversation navigation belongs to the canonical outcome's exact attempt."""

from types import SimpleNamespace

from app.telegram_bots.config import load_config
from app.telegram_bots.pages import page_address
from app.telegram_bots.producer import ProductSourceRetired
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow


def _shape(value):
    if type(value) is not dict or set(value) != {"version", "agent_ref", "attempt_id", "thread_id", "public_base_url"} or type(value["version"]) is not int or value["version"] != 1:
        raise ValueError("invalid-work-result-navigation")
    if any(type(value[key]) is not str or not value[key] for key in ("agent_ref", "attempt_id")) or any(value[key] is not None and type(value[key]) is not str for key in ("thread_id", "public_base_url")):
        raise ValueError("invalid-work-result-navigation")


def qualify_navigation(value, link, source):
    _shape(value)
    if link.agent_kind != "instance" or value["agent_ref"] != link.agent_ref or value["attempt_id"] != source.work["outcome"]["attempt_id"]:
        raise ValueError("invalid-work-result-navigation")


def require_navigation(value, attempt, work_id):
    _shape(value)
    if attempt is None or (attempt.id, attempt.work_id, attempt.thread_id) != (value["attempt_id"], work_id, value["thread_id"]):
        raise ValueError("changed-work-result-navigation")


class NavigationOrigin:
    """Retain the already prepared configuration across provider awaits."""

    def __init__(self):
        from deerflow.config.app_config import peek_loaded_app_config

        self.config = peek_loaded_app_config()
        if self.config is None:
            raise ProductSourceRetired()
        self.base = load_config(self.config).public_base_url

    def check(self, value):
        from deerflow.config.app_config import peek_loaded_app_config

        if peek_loaded_app_config() is not self.config or load_config(self.config).public_base_url != self.base or value["public_base_url"] != self.base:
            raise ProductSourceRetired()


async def capture_navigation(writer, link, source):
    origin = NavigationOrigin()
    attempt = await writer.get(AgentWorkAttemptRow, source.work["outcome"]["attempt_id"], populate_existing=True)
    if attempt is None or attempt.work_id != source.work["id"]:
        raise ValueError("missing-work-result-attempt")
    value = {"version": 1, "agent_ref": link.agent_ref, "attempt_id": attempt.id, "thread_id": attempt.thread_id, "public_base_url": origin.base}
    qualify_navigation(value, link, source)
    origin.check(value)
    return value


def navigation_url(value):
    if value is None:
        return None
    _shape(value)
    return page_address("web.conversation", SimpleNamespace(agent_kind="instance", agent_ref=value["agent_ref"]), thread_id=value["thread_id"], public_base_url=value["public_base_url"])
