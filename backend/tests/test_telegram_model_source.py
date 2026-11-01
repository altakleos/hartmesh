"""Synthetic strict source data exercises source binding, not SDK provenance."""

import asyncio
import importlib
import threading
from copy import deepcopy
from dataclasses import replace

import pytest
from _telegram_repository_test_support import db as db  # noqa: F401
from test_telegram_publication_custody import publication_environment as publication_environment
from test_telegram_publication_primary import _managed_turn
from test_telegram_verified_reply import projection, synthetic_item

from app.telegram_bots.producer import DeliveryClaimRetired, SendingPart, SourceGone, SourceNotRead


def source_api():
    return importlib.import_module("app.telegram_bots.model_source")


def captured(e, record, output):
    item = synthetic_item(record, output)
    return SendingPart(e.link, item, deepcopy(item.parts[0]), "process", "c" * 32, e.link.token_revision)


@pytest.mark.asyncio
async def test_source_borrows_exact_original_record_and_returns_frozen_part(publication_environment):
    e = publication_environment
    record = await _managed_turn(e)
    output = projection(record)
    part = captured(e, record, output)
    calls = []

    async def read(original, link):
        calls.append((original, link))
        return output

    ready = await source_api().ModelSource(read_output=read).read_part(part)
    assert ready.html == part.part["html"] and ready.plain_text == part.part["visible_text"]
    assert calls[0][0].scope == record.scope and calls[0][0].run_id == output.output.run_id
    assert calls[0][1] == e.link and len(calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["missing", "changed", "inventory-only", "incomplete", "private-error"])
async def test_source_uncertainty_retires_without_cached_or_fixed_substitution(publication_environment, change):
    e = publication_environment
    record = await _managed_turn(e)
    output = projection(record)

    async def read(*args):
        if change == "private-error":
            raise ValueError("private-diagnostic-marker")
        if change == "inventory-only":
            from app.gateway.run_output import RunOutputProjectionV2
            from deerflow.runtime.runs.output_inventory import inventory_document_digest

            data = output.model_dump(mode="json")
            data["inventory"]["document"]["root_epoch"] = "a3a78d6c-038c-4bf8-b420-05a993b42934"
            data["inventory"]["evidence"]["inventory_sha256"] = inventory_document_digest(data["inventory"]["document"])
            return RunOutputProjectionV2.model_validate(data)
        return None if change == "missing" else projection(record, text="changed") if change == "changed" else projection(record, coverage="incomplete")

    with pytest.raises(DeliveryClaimRetired) as error:
        await source_api().ModelSource(read_output=read).read_part(captured(e, record, output))
    import traceback

    assert "private-diagnostic-marker" not in "".join(traceback.format_exception(error.value))
    assert type(error.value) is (SourceNotRead if change == "private-error" else SourceGone), "A failed read may pass. A changed answer will not."


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["html", "part-source", "scope", "legacy-version", "inventory", "repair", "missing-part", "duplicate-part", "bool-version"])
async def test_corrupt_saved_basis_refuses_before_source_read(publication_environment, change):
    e = publication_environment
    record = await _managed_turn(e)
    output = projection(record)
    part = captured(e, record, output)
    item = deepcopy(part.item)
    selected = deepcopy(part.part)
    if change == "html":
        item.payload["reply_basis"]["presentation"]["chunks"][0]["html"] = "unrelated"
    elif change == "part-source":
        selected["source_ref"]["run_id"] = "different"
    elif change == "scope":
        item.payload["reply_basis"]["publication"]["scope"]["owner_user_id"] = "different"
    elif change == "legacy-version":
        item.payload["reply_basis"]["version"] = 1
    elif change == "inventory":
        item.payload["reply_basis"]["outcome"]["projection"]["inventory"]["document"]["items"] = [False]
    elif change == "missing-part":
        item.parts.clear()
    elif change == "duplicate-part":
        item.parts.append(deepcopy(item.parts[0]))
    elif change == "bool-version":
        item.parts[0]["presentation_version"] = False
        selected["presentation_version"] = False
    else:
        item.payload["delivery_kind"] = "status_notice"
    calls = []

    async def read(*args):
        calls.append(True)
        return output

    with pytest.raises(DeliveryClaimRetired):
        await source_api().ModelSource(read_output=read).read_part(replace(part, item=item, part=selected))
    assert calls == []


@pytest.mark.asyncio
async def test_caller_mutation_while_read_waits_cannot_change_private_part(publication_environment):
    e = publication_environment
    record = await _managed_turn(e)
    output = projection(record)
    part = captured(e, record, output)
    entered, release = asyncio.Event(), asyncio.Event()

    async def read(*args):
        entered.set()
        await release.wait()
        return output

    task = asyncio.create_task(source_api().ModelSource(read_output=read).read_part(part))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        part.part["html"] = "caller changed"
        part.item.payload.clear()
    finally:
        release.set()
    assert (await task).html == "<b>synthetic answer</b>"


@pytest.mark.asyncio
async def test_cold_preparation_is_drained_before_cancel_without_calling_reader(publication_environment, monkeypatch):
    e = publication_environment
    record = await _managed_turn(e)
    api = source_api()
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()
    original = api._prepare_part
    calls = []

    def held(value):
        result = original(value)
        entered.set()
        try:
            assert release.wait(5)
            return result
        finally:
            finished.set()

    async def read(*args):
        calls.append(True)
        return projection(record)

    monkeypatch.setattr(api, "_prepare_part", held)
    task = asyncio.create_task(api.ModelSource(read_output=read).read_part(captured(e, record, projection(record))))
    try:
        assert await asyncio.wait_for(asyncio.to_thread(entered.wait, 5), 6)
        task.cancel()
        assert not (await asyncio.wait({task}, timeout=0.03))[0]
        task.cancel()
        assert not (await asyncio.wait({task}, timeout=0.03))[0]
    finally:
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert finished.is_set() and calls == []
