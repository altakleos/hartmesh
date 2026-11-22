import signal

from fastapi import FastAPI
from uvicorn import Config, Server

from app.gateway.snapshot_shutdown import snapshot_lifetime


def test_a_real_server_notifies_only_its_own_snapshot_service_before_lifespan_teardown(monkeypatch):
    own, other = FastAPI(), FastAPI()
    config = Config(own, log_config=None)
    config.load()
    server = Server(config)
    previous = server.handle_exit
    handlers = {signal.SIGTERM: previous, signal.SIGINT: previous}
    monkeypatch.setattr(signal, "getsignal", handlers.get)
    monkeypatch.setattr(signal, "signal", lambda sig, value: handlers.__setitem__(sig, value))
    with snapshot_lifetime(other), snapshot_lifetime(own):
        assert not own.state.snapshot_stopping.is_set()
        handlers[signal.SIGTERM](signal.SIGTERM, None)
        assert own.state.snapshot_stopping.is_set() and server.should_exit
        assert not other.state.snapshot_stopping.is_set()
    assert all(handler is previous for handler in handlers.values())
    assert other.state.snapshot_stopping.is_set()
