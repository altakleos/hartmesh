"""Notify a mounted snapshot service at the start of its own Uvicorn drain."""

import signal
import threading
from contextlib import contextmanager

from uvicorn import Server


def _serves(server, app):
    current = getattr(server.config, "loaded_app", None)
    seen = set()
    while current is not None and id(current) not in seen:
        if current is app:
            return True
        seen.add(id(current))
        current = getattr(current, "app", None)
    return False


@contextmanager
def snapshot_lifetime(app):
    stopping = threading.Event()
    app.state.snapshot_stopping = stopping
    installed = {}
    try:
        if threading.current_thread() is threading.main_thread():
            for sig in (signal.SIGINT, signal.SIGTERM):
                previous = signal.getsignal(sig)
                server = getattr(previous, "__self__", None)
                if not isinstance(server, Server) or not _serves(server, app):
                    continue

                def stop(signum, frame, previous=previous):
                    stopping.set()
                    return previous(signum, frame)

                signal.signal(sig, stop)
                installed[sig] = (stop, previous)
        yield
    finally:
        stopping.set()
        for sig, (ours, previous) in installed.items():
            if signal.getsignal(sig) is ours:
                signal.signal(sig, previous)
