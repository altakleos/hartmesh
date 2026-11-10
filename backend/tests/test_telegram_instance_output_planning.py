"""Unknown-size plans; synthetic projections test recipes, not worker provenance."""

from dataclasses import replace
from types import SimpleNamespace

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_file_source import file_output, record
from test_telegram_instance_publication import publication as publication
from test_telegram_output_planning import source

from app.telegram_bots.files import plan_output_files, repartition_output_part


def instance_record():
    original = record()
    return replace(original, scope=replace(original.scope, agent_kind="instance", agent_ref="a" * 32))


def instance_output(saved, *, paths=("/mnt/user-data/outputs/report.txt",)):
    return file_output(saved, paths=paths)


@pytest.mark.parametrize("count,capacity,groups", [(5, 200_000_000, [4, 1]), (10, 200_000_000, [4, 4, 2]), (2, 50_000_000, [1, 1])])
def test_unknown_size_photos_reserve_each_members_full_ceiling(count, capacity, groups):
    descriptors = [source(i, size=None, state="export") for i in range(count)]
    plan = plan_output_files(descriptors, answer_visible="Answer", capacity=capacity, max_bytes=50_000_000)
    assert not plan.caption
    assert [len(part["files"]) for part in plan.parts] == groups
    assert [member for part in plan.parts for member in part["files"]] == descriptors
    assert all(len(part["files"]) * 50_000_000 <= capacity for part in plan.parts)


def test_unknown_single_file_can_keep_caption_and_reduced_budget_splits_saved_album():
    descriptors = [source(i, size=None, state="export") for i in range(4)]
    single = plan_output_files(descriptors[:1], answer_visible="Answer", capacity=50_000_000, max_bytes=50_000_000)
    assert single.caption and single.parts[0]["kind"] == "document"
    initial = plan_output_files(descriptors, answer_visible="Answer", capacity=200_000_000, max_bytes=50_000_000)
    result = repartition_output_part(initial.parts[0], capacity=50_000_000, max_bytes=50_000_000)
    assert [part["kind"] for part in result] == ["photo"] * 4
    assert [member for part in result for member in part["files"]] == descriptors


def test_ordinary_sources_cannot_adopt_unknown_instance_export_descriptors():
    from app.telegram_bots.file_source import prepare_file_reply

    saved = record()
    output = file_output(saved, paths=("/mnt/user-data/outputs/image-0.png",))
    with pytest.raises(ValueError):
        prepare_file_reply(saved, output, files=[source(0, size=None, state="export")], capacity=50_000_000, max_bytes=50_000_000)


@pytest.mark.asyncio
async def test_instance_planning_never_heads_or_exports_and_seals_saved_descriptors(publication):
    from app.telegram_bots.model_source import ModelSource, validate_model_reply_basis

    e = publication
    saved = await e.coordinator.dispatch(e.link, [e.origin], thread_id=e.thread_id)
    saved = await e.inbox.bind_publication_run(saved, run_id="managed-primary-run", guard=(await e.host.capture(e.link, thread_id=e.thread_id, endpoint_id=e.http_client.endpoint_id)).read_guard)
    output = instance_output(saved, paths=tuple(source(i)["path"] for i in range(5)))
    reads = []

    async def forbidden(*args, **kwargs):
        reads.append(True)
        raise AssertionError("Instance planning must read no file bytes or artifact metadata")

    async def reader(*args):
        return output

    box = e.channel._services.outbox
    owner = SimpleNamespace(describe_artifact=forbidden, read_artifact_capped=forbidden)
    planner = ModelSource(read_output=reader, outbox=box, http_owner=owner, file_capacity=200_000_000)
    guard = (await e.host.capture(e.link, thread_id=e.thread_id, endpoint_id=e.http_client.endpoint_id)).read_guard
    identity = await planner.plan_file_reply(saved, e.link, output, guard=guard)
    item = await box.get(e.link, identity)
    assert validate_model_reply_basis(item).kind == "file_outputs"
    assert not reads
    media = [part for part in item.parts if part["kind"] in {"photo", "album", "document"}]
    assert [len(part["files"]) for part in media] == [4, 1]
    assert all(member["size"] is None and member["state"] == "export" for part in media for member in part["files"])


@pytest.mark.parametrize("size", [0, 4, True])
def test_export_descriptors_cannot_claim_a_known_size(size):
    with pytest.raises(ValueError):
        plan_output_files([source(0, size=size, state="export")], answer_visible="", capacity=50_000_000, max_bytes=50_000_000)


@pytest.mark.asyncio
async def test_explicit_output_retry_only_plans_unknown_metadata(publication):
    from unittest.mock import AsyncMock

    from app.telegram_bots.model_source import ModelSource

    e = publication
    saved = await e.coordinator.dispatch(e.link, [e.origin], thread_id=e.thread_id)
    file = source(0, size=None, state="export")
    # Protocol stub only for the already-qualified retry lookup/write. Real
    # callback ownership and preserved receipts have separate SQL regressions.
    box = SimpleNamespace(read_file_retry=AsyncMock(return_value=(saved, file)), retry_file_callback=AsyncMock(return_value=True))
    head = AsyncMock(side_effect=AssertionError("An instance retry cannot HEAD a legacy artifact"))
    planner = ModelSource(read_output=AsyncMock(), outbox=box, http_owner=SimpleNamespace(describe_artifact=head))
    guard = (await e.host.capture(e.link, thread_id=e.thread_id, endpoint_id=e.http_client.endpoint_id)).read_guard
    assert await planner.retry_file_callback(e.link, SimpleNamespace(id="callback"), callbacks=object(), guard=guard)
    head.assert_not_awaited()
    assert box.retry_file_callback.await_args.kwargs["metadata"] == file
