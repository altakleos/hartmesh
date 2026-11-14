"""Bounded original-run source capture through the publication's HTTP owner."""

import math
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass

MAX_SOURCE_MESSAGES = 4096
MAX_SOURCE_PAGES = 32


def plain_source(value):
    if isinstance(value, Mapping):
        if any(type(key) is not str for key in value):
            raise ValueError("terminal-source-conflict")
        return {key: plain_source(child) for key, child in value.items()}
    if type(value) in {list, tuple}:
        return [plain_source(child) for child in value]
    if value is None or type(value) in {str, bool, int} or type(value) is float and math.isfinite(value):
        return value
    raise ValueError("terminal-source-conflict")


@dataclass(frozen=True)
class CapturedTerminalSource:
    record: object
    projection: object
    messages: list
    guard: object
    current: object


async def capture_terminal_source(owner, record, link, *, guard, accept):
    """Capture stable output/messages; a client-specific validator grants no rights."""
    from app.channels.execution_client import ChannelExecutionFacade
    from app.telegram_bots.links import prepare_publication_guard
    from app.telegram_bots.publication import channel_execution_scope

    def refuse():
        raise ValueError("terminal-source-conflict") from None

    record, link = deepcopy(record), deepcopy(link)
    if guard is not None:
        if not callable(getattr(guard, "current", None)) or guard.current() is not True:
            refuse()
        await prepare_publication_guard(guard)
        if guard.current() is not True:
            refuse()
        # Recovery cannot allocate a lookup operation beyond its original guard.
        if record.run_id is None:
            return None
    if record.run_id is None:
        observed = await owner.coordinator.locate_run(record, link, max_pages=1)
        if observed.state != "matched":
            return None
        record = observed.record
    attempt = await owner.capture_attempt(link, thread_id=record.scope.thread_id, endpoint_id=record.scope.endpoint_id)

    class Admission:
        def current(self, operation):
            return attempt.admission.current(operation) is True and (guard is None or guard.current() is True)

        async def authorize(self, operation):
            if self.current(operation) is not True:
                refuse()
            await attempt.admission.authorize(operation)
            if guard is not None:
                await prepare_publication_guard(guard)
            if self.current(operation) is not True:
                refuse()

    admission = Admission()
    facade = ChannelExecutionFacade(owner.http_owner, scope=channel_execution_scope(record), admission=admission, capture_output=True, capture_inventory=True, inventory_document_version=2)

    async def output():
        return await facade.get_run_output(record.run_id) if guard is not None else await owner.coordinator.read_output(record, link)

    first = await output()
    if first is None or accept(first) is not True:
        return None
    rows, after = [], None
    for _ in range(MAX_SOURCE_PAGES):
        page = await facade.list_run_messages(record.run_id, limit=200, after_seq=after)
        rows.extend(plain_source(page.data))
        if len(rows) > MAX_SOURCE_MESSAGES:
            refuse()
        if not page.has_more:
            break
        if page.next_after_seq is None or after is not None and page.next_after_seq <= after:
            refuse()
        after = page.next_after_seq
    else:
        refuse()
    fresh = await output()
    if fresh is None or fresh.model_dump(mode="json") != first.model_dump(mode="json"):
        refuse()
    await admission.authorize("source_read")

    class SourceGuard:
        captured_link = attempt.read_guard.captured_link

        async def prepare(self):
            await admission.authorize("source_read")

        def current(self):
            return admission.current("source_read") is True

        async def check(self, writer):
            if self.current() is not True:
                refuse()
            result = await attempt.read_guard.check(writer)
            if guard is not None:
                result = await guard.check(writer)
            if self.current() is not True:
                refuse()
            return result

    return CapturedTerminalSource(record, fresh, rows, SourceGuard() if guard is not None else attempt.read_guard, lambda: admission.current("source_read"))
