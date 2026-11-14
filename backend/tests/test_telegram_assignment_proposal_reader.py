"""Actual retained HTTP owner and fences; remote proposal JSON is scripted."""

import asyncio
from dataclasses import replace
from types import SimpleNamespace

import httpx
import pytest
import pytest_asyncio
from test_telegram_assignment_proposal_source import proposal_basis
from test_telegram_publication_observation import ACK

from app.gateway.run_output import RunOutputProjectionV2
from app.telegram_bots.assignment_proposal_source import AssignmentProposalSource, check_qualified_proposal
from app.telegram_bots.gateway_client import GatewayClientOwner
from app.telegram_bots.inbox import publication_identity


@pytest_asyncio.fixture
async def reader_environment():
    record, basis, _draft = proposal_basis()
    e = SimpleNamespace(record=record, basis=basis, current=True, requests=[], authorizations=[], calls=0, responder=None)

    async def respond(request):
        e.requests.append(request)
        if e.responder is not None:
            return await e.responder(request)
        if request.url.path.endswith("/output"):
            return httpx.Response(200, headers=ACK, json=e.basis["projection"])
        return httpx.Response(200, headers=ACK, json={"data": e.basis["messages"], "has_more": False})

    e.http = GatewayClientOwner(gateway_url="https://gateway.example", langgraph_url="https://runtime.example/api", transport=httpx.MockTransport(respond))
    e.record = replace(record, scope=replace(record.scope, endpoint_id=e.http.endpoint_id))
    e.basis["publication"] = publication_identity(e.record)
    e.link = SimpleNamespace(id=e.record.scope.link_id)

    class Admission:
        def current(self, operation):
            return e.current

        async def authorize(self, operation):
            if not e.current:
                raise ValueError("retired-source")
            e.authorizations.append(operation)

    class Guard:
        captured_link = e.link

        def current(self):
            return e.current

        async def prepare(self):
            if not e.current:
                raise ValueError("retired-source")

        async def check(self, writer):
            if not e.current:
                raise ValueError("retired-source")
            return e.link

    async def capture(link, *, thread_id, endpoint_id):
        assert link.id == e.record.scope.link_id and (thread_id, endpoint_id) == (e.record.scope.thread_id, e.http.endpoint_id)
        return SimpleNamespace(admission=Admission(), read_guard=Guard())

    async def output(record, link):
        e.calls += 1
        return RunOutputProjectionV2.model_validate(e.basis["projection"])

    e.guard = Guard()
    e.coordinator = SimpleNamespace(read_output=output)
    e.reader = AssignmentProposalSource(e.coordinator, e.http, capture)
    try:
        yield e
    finally:
        await e.http.stop()


@pytest.mark.asyncio
async def test_reader_freezes_stable_source_and_preserves_live_original_attempt(reader_environment):
    e = reader_environment
    result = await e.reader.read_proposal(e.record, e.link)
    assert e.calls == 2 and len(e.requests) == 1
    assert check_qualified_proposal(result, e.record) == e.basis
    assert result.draft.objective == "Review the guide"
    copied = result.basis
    copied["messages"].clear()
    assert result.basis["messages"]
    e.current = False
    with pytest.raises(ValueError):
        check_qualified_proposal(result, e.record)
    with pytest.raises(ValueError):
        await result._guard.check(None)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["retire", "output-change", "nonadvancing", "missing-next"])
async def test_pagination_or_source_drift_cannot_issue_an_adoption_capability(reader_environment, damage):
    e = reader_environment

    async def respond(request):
        if damage == "retire":
            e.current = False
            return httpx.Response(200, headers=ACK, json={"data": e.basis["messages"], "has_more": False})
        if damage == "output-change":
            e.basis["projection"]["output"]["output_evidence"]["input_message_id"] = "changed"
            return httpx.Response(200, headers=ACK, json={"data": e.basis["messages"], "has_more": False})
        return httpx.Response(200, headers=ACK, json={"data": [], "has_more": True, **({"next_after_seq": 1} if damage == "nonadvancing" else {})})

    e.responder = respond
    with pytest.raises(Exception):
        await e.reader.read_proposal(e.record, e.link)
    assert len(e.requests) <= 2


@pytest.mark.asyncio
async def test_guarded_source_does_not_allocate_a_fresh_run_lookup(reader_environment):
    e = reader_environment
    assert await e.reader.read_proposal(replace(e.record, run_id=None), e.link, guard=e.guard) is None
    assert e.requests == [] and e.calls == 0
    e.current = False
    with pytest.raises(ValueError):
        await e.reader.read_proposal(e.record, e.link, guard=e.guard)
    assert e.requests == [] and e.calls == 0


@pytest.mark.asyncio
async def test_guarded_read_retains_the_original_attempt_for_every_http_operation(reader_environment):
    e = reader_environment
    source = await e.reader.read_proposal(e.record, e.link, guard=e.guard)
    assert e.calls == 0 and len(e.requests) == 3
    assert sum(request.url.path.endswith("/output") for request in e.requests) == 2
    assert check_qualified_proposal(source, e.record) == e.basis
    await source._guard.prepare()
    assert await source._guard.check(None) is e.link
    e.current = False
    with pytest.raises(ValueError):
        await source._guard.prepare()


@pytest.mark.asyncio
async def test_source_is_fenced_after_actual_off_thread_validation(reader_environment, monkeypatch):
    import threading

    from app.telegram_bots import assignment_proposal_source

    e = reader_environment
    entered, release = threading.Event(), threading.Event()
    validate = assignment_proposal_source.validate_proposal_basis

    def held(*args):
        value = validate(*args)
        entered.set()
        assert release.wait(5)
        return value

    monkeypatch.setattr(assignment_proposal_source, "validate_proposal_basis", held)
    task = asyncio.create_task(e.reader.read_proposal(e.record, e.link))
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        e.current = False
        release.set()
        with pytest.raises(ValueError):
            await task
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
