"""Only the actual component view may authorize its transient placeholder."""

from types import SimpleNamespace


def own_view(view, *, execute_callback=None):
    async def retire(channel):
        if channel is view.channel:
            await view.close()

    owner = SimpleNamespace(
        preview_allowed=lambda channel, item: channel is view.channel and view.allows_placeholder(item),
        message_sent=lambda channel, **kwargs: view.message_sent(**kwargs) if channel is view.channel else None,
        retire=retire,
    )
    if execute_callback is not None:
        owner.execute_callback = execute_callback
    view.channel._services.turns = owner
    return owner
