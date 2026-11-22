"""An operator-selected search projection over visible PostgreSQL message history."""

from deerflow_extension_api import extension

from .index import MessageSearch


@extension(api="0.2.13", name="message-search")
def install(registry, config):
    search = MessageSearch()
    if not registry.workspace_search(search):
        raise RuntimeError("Workspace search is unavailable")
    registry.service(search)
