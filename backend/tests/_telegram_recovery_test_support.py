"""Real opaque ticket issuer, with an explicitly synthetic host startup phase.

SQL/account/custody collaborators are real. The startup phase in this helper is
controlled; separate host tests exercise the actual startup ordering.
"""

import asyncio
from types import SimpleNamespace

from test_telegram_supervisor import build


def issue_restart_epoch(environment, monkeypatch, records, *, deadline=None):
    from app.telegram_bots import supervisor as supervisor_module
    from app.telegram_bots.recovery_authority import _issue_startup_epoch

    previous = supervisor_module.current()
    monkeypatch.setattr(supervisor_module, "_current", previous)
    boot = build(environment.db, monkeypatch)
    host = boot.supervisor
    host.authority = environment.live.authority
    host.thread_client = SimpleNamespace(endpoint_id=environment.endpoint_id)
    host._ready = host._cohort_complete = True
    host._copy_decision = False
    channel = SimpleNamespace(link=environment.link, transport_revision=environment.link.token_revision, application=SimpleNamespace(running=True))
    host._channels[environment.link.id] = channel
    host._copy_task = asyncio.current_task()
    phase = object()
    host._recovery_phase = phase
    host._recovery_epoch = None
    end = asyncio.get_running_loop().time() + 30 if deadline is None else deadline
    epoch = _issue_startup_epoch(host, tuple(records), deadline=end, phase=phase)

    async def aclose():
        # This controlled host never started: no transport/client/task resource
        # exists to stop. Retire its authority and restore the singleton only.
        epoch.retire()
        host._recovery_phase = None
        if supervisor_module.current() is host:
            supervisor_module._current = previous

    return SimpleNamespace(epoch=epoch, host=host, channel=channel, boot=boot, phase=phase, aclose=aclose)
