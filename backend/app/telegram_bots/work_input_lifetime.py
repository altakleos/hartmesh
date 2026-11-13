"""Carry an accepted input's original lifetime through service and file awaits."""


def input_current(channel, link, guard):
    try:
        current = getattr(guard, "current", None)
        if current is not None:
            current()
    except Exception:
        return False
    return channel._callback_current(link) is True
